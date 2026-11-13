"""Four-node package fan-out and one-hertz progress soak verification."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time

import requests


ROOT = Path(__file__).resolve().parents[1]
NODE_COUNT = 4


def free_port():
    with socket.socket() as stream:
        stream.bind(('127.0.0.1', 0))
        return stream.getsockname()[1]


def wait(check, seconds):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            value = check()
            if value:
                return value
        except (OSError, requests.RequestException, KeyError, ValueError):
            pass
        time.sleep(0.1)
    raise RuntimeError('verification wait timed out')


def browser_client(url):
    client = requests.Session()
    client.trust_env = False
    response = client.get(url + '/api/session', timeout=5)
    response.raise_for_status()
    client.headers['X-CSRF-Token'] = response.json()['csrf']
    return client


def profile(path):
    digest = hashlib.sha256()
    rows = blank = size = 0
    with path.open('rb') as source:
        for line in source:
            digest.update(line)
            size += len(line)
            rows += 1
            blank += int(not line.strip())
    return {'name': path.name, 'bytes': size, 'rows': rows,
            'blank_rows': blank, 'sha256': digest.hexdigest()}


def stream_digest(response):
    digest = hashlib.sha256()
    size = 0
    for block in response.iter_content(1024 * 1024):
        digest.update(block)
        size += len(block)
    return size, digest.hexdigest()


def main(arguments):
    source = arguments.input.resolve()
    input_info = profile(source)
    processes = []
    logs = []
    clients = []
    started_wall = time.time()
    with tempfile.TemporaryDirectory(prefix='cluster-four-node-soak-') as temporary_name:
        temporary = Path(temporary_name)
        ports = []
        while len(ports) < NODE_COUNT + 1:
            port = free_port()
            if port not in ports:
                ports.append(port)
        urls = [f'http://127.0.0.1:{port}' for port in ports]
        try:
            for index, (port, url) in enumerate(zip(ports, urls)):
                name = 'controller' if index == 0 else f'node-{index:02d}'
                command = [sys.executable, str(ROOT / 'app.py'),
                           'controller' if index == 0 else 'node', '--port', str(port),
                           '--data-dir', str(temporary / name)]
                if index:
                    command += ['--max-processes', str(arguments.processes),
                                '--max-threads', str(arguments.threads)]
                environment = {key: value for key, value in os.environ.items()
                               if key != 'CLUSTER_LAB_ADMIN_PASSWORD'}
                environment['CLUSTER_LAB_SIMULATE_DELAY'] = str(arguments.simulate_delay)
                log = (temporary / f'{name}.log').open('wb')
                logs.append(log)
                processes.append(subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=log,
                                                   start_new_session=True, env=environment))
                wait(lambda url=url: requests.get(url + '/healthz', timeout=1).ok, 30)
                clients.append(browser_client(url))

            master, master_url = clients[0], urls[0]
            for index, (node_client, node_url) in enumerate(zip(clients[1:], urls[1:]), 1):
                response = master.post(master_url + '/api/nodes', json={
                    'name': f'本地压测节点 {index:02d}', 'processes': arguments.processes,
                    'threads': arguments.threads}, timeout=5)
                response.raise_for_status()
                identity = response.json()
                response = node_client.post(node_url + '/api/connect', json={
                    'url': master_url, 'token': identity['token']}, timeout=5)
                response.raise_for_status()

            def state():
                response = master.get(master_url + '/api/state', timeout=15)
                response.raise_for_status()
                return response.json()

            online = wait(lambda: state() if state()['totals']['online'] == NODE_COUNT else None, 45)
            assert all(node['effective_processes'] == arguments.processes and
                       node['effective_threads'] == arguments.threads for node in online['nodes'])

            query = requests.compat.urlencode({
                'name': source.name, 'unit_rows': arguments.unit_rows, 'mode': 'simulate',
                'enqueue': 'false', 'timeout': 20, 'retries': 0})
            upload_started = time.monotonic()
            with source.open('rb') as upload:
                response = master.post(master_url + '/api/jobs/stream?' + query, data=upload,
                                       headers={'Content-Type': 'application/octet-stream',
                                                'Content-Length': str(input_info['bytes'])},
                                       timeout=900)
            response.raise_for_status()
            job_id = response.json()['id']
            upload_seconds = time.monotonic() - upload_started

            def job_state():
                current = state()
                return next(job for job in current['jobs'] if job['id'] == job_id), current

            prepare_started = time.monotonic()
            job, _ = wait(lambda: job_state() if job_state()[0]['status'] == 'ready' else None, 300)
            prepare_seconds = time.monotonic() - prepare_started
            expected_units = math.ceil(input_info['rows'] / arguments.unit_rows)
            assert (job['rows'], job['units']) == (input_info['rows'], expected_units)

            database_path = temporary / 'controller/controller.sqlite3'
            with sqlite3.connect(database_path) as database:
                indexed = database.execute(
                    'SELECT COUNT(*),COUNT(DISTINCT path),SUM(byte_end-byte_start),MIN(rows),MAX(rows) '
                    'FROM units WHERE job_id=?', (job_id,)).fetchone()
            assert indexed[:3] == (expected_units, 1, input_info['bytes'])
            assert indexed[3] > 0 and indexed[4] <= arguments.unit_rows
            assert not list((temporary / 'controller/units' / job_id).glob('*.part'))

            response = master.post(master_url + f'/api/jobs/{job_id}/start', json={}, timeout=5)
            response.raise_for_status()
            run_started = time.monotonic()
            samples = []
            next_sample = run_started
            completed_at = None
            while time.monotonic() - run_started < arguments.duration:
                now = time.monotonic()
                if now < next_sample:
                    time.sleep(next_sample - now)
                job, current = job_state()
                elapsed = time.monotonic() - run_started
                workers = [int((node.get('telemetry') or {}).get('worker_processes') or 0)
                           for node in current['nodes']]
                samples.append({'second': round(elapsed, 3), 'processed': job['processed'],
                                'confirmed': job['confirmed'],
                                'active_packages': sum(node['active'] for node in current['nodes']),
                                'workers': workers,
                                'rate': current['totals']['rate'], 'status': job['status']})
                if job['status'] == 'completed' and completed_at is None:
                    completed_at = elapsed
                next_sample += 1.0

            job, current = job_state()
            if job['status'] != 'completed':
                job, current = wait(lambda: job_state() if job_state()[0]['status'] == 'completed' else None,
                                    arguments.completion_grace)
                completed_at = time.monotonic() - run_started

            processed_changes = [sample['second'] for previous, sample in zip(samples, samples[1:])
                                 if sample['processed'] > previous['processed']]
            assert processed_changes, 'no one-hertz progress changes observed'
            progress_gap = max((right - left for left, right in zip(processed_changes, processed_changes[1:])),
                               default=0.0)
            assert progress_gap <= 3.2, f'progress stalled for {progress_gap:.2f}s while sampling'
            assert max(sum(sample['workers']) for sample in samples) == NODE_COUNT * arguments.processes
            assert max(sample['active_packages'] for sample in samples) == NODE_COUNT
            assert job['confirmed'] == input_info['rows']

            def reconciled():
                values = [node_client.get(node_url + '/api/state', timeout=10).json()
                          for node_client, node_url in zip(clients[1:], urls[1:])]
                return values if (sum(value['confirmed'] for value in values) == input_info['rows']
                                  and all(value['outbox'] == 0 for value in values)) else None

            node_states = wait(reconciled, 120)
            with master.get(master_url + f'/api/jobs/{job_id}/results', stream=True,
                            timeout=(10, 900)) as download:
                download.raise_for_status()
                downloaded_bytes, downloaded_sha = stream_digest(download)
            assert (downloaded_bytes, downloaded_sha) == (input_info['bytes'], input_info['sha256'])

            with sqlite3.connect(database_path) as database:
                distribution = [dict(zip(('node_id', 'packages', 'rows'), row)) for row in
                                database.execute('SELECT node_id,COUNT(*),SUM(rows) FROM units '
                                                 'WHERE job_id=? AND status="completed" '
                                                 'GROUP BY node_id ORDER BY node_id', (job_id,))]
            assert len(distribution) == NODE_COUNT
            report = {
                'input': input_info,
                'configuration': {'nodes': NODE_COUNT,
                                  'processes_per_node': arguments.processes,
                                  'threads_per_process': arguments.threads,
                                  'package_rows': arguments.unit_rows,
                                  'report_interval_seconds': 1,
                                  'soak_seconds': arguments.duration,
                                  'mode': 'local_simulate_no_external_auth'},
                'preparation': {'upload_seconds': round(upload_seconds, 3),
                                'index_seconds': round(prepare_seconds, 3),
                                'packages': expected_units, 'source_copies': 1,
                                'indexed_bytes': indexed[2]},
                'result': {'job_id': job_id, 'status': job['status'],
                           'confirmed_rows': job['confirmed'],
                           'completed_seconds': round(completed_at or 0, 3),
                           'samples': len(samples),
                           'max_progress_update_gap_seconds': round(progress_gap, 3),
                           'max_active_packages': max(sample['active_packages'] for sample in samples),
                           'max_worker_processes': max(sum(sample['workers']) for sample in samples),
                           'download_matches_input': True,
                           'node_confirmed_rows': sum(value['confirmed'] for value in node_states),
                           'node_outbox': sum(value['outbox'] for value in node_states)},
                'distribution': distribution,
                'sample_head': samples[:5], 'sample_tail': samples[-5:],
                'wall_seconds': round(time.time() - started_wall, 3),
            }
            arguments.report.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            arguments.report.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n', 'utf-8')
            print(json.dumps(report, ensure_ascii=False, indent=2))
        except Exception:
            for log in logs:
                log.flush()
            for path in sorted(temporary.glob('*.log')):
                tail = path.read_text('utf-8', errors='replace').splitlines()[-80:]
                print(f'--- {path.name} tail ---', file=sys.stderr)
                print('\n'.join(tail), file=sys.stderr)
            raise
        finally:
            for process in reversed(processes):
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait(timeout=5)
            for client in clients:
                client.close()
            for log in logs:
                log.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True, type=Path)
    parser.add_argument('--report', required=True, type=Path)
    parser.add_argument('--duration', type=int, default=600)
    parser.add_argument('--completion-grace', type=int, default=180)
    parser.add_argument('--processes', type=int, default=10)
    parser.add_argument('--threads', type=int, default=10)
    parser.add_argument('--unit-rows', type=int, default=10000)
    parser.add_argument('--simulate-delay', type=float, default=0.04)
    main(parser.parse_args())
