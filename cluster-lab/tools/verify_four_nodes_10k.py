"""Run a real local HTTP round trip with four nodes and a caller-provided TXT.

The executor mode is ``simulate``: it hashes every input row locally and never
sends account contents to an external authentication service.
"""
import argparse
import hashlib
import json
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
PROCESSES = 10
THREADS = 20
UNIT_ROWS = 250


def free_port():
    with socket.socket() as stream:
        stream.bind(('127.0.0.1', 0))
        return stream.getsockname()[1]


def wait(check, seconds=180):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            value = check()
            if value:
                return value
        except (OSError, requests.RequestException, KeyError, ValueError):
            pass
        time.sleep(.05)
    raise RuntimeError('local four-node verification timed out')


def client(url):
    value = requests.Session()
    value.trust_env = False
    response = value.get(url + '/api/session', timeout=5)
    response.raise_for_status()
    value.headers['X-CSRF-Token'] = response.json()['csrf']
    return value


def main(input_path, report_path):
    source = Path(input_path).resolve()
    payload = source.read_bytes()
    lines = payload.splitlines(keepends=True)
    if not payload or not lines:
        raise ValueError('input file is empty')
    processes, logs, clients = [], [], []
    with tempfile.TemporaryDirectory(prefix='cluster-four-nodes-10k-') as temporary:
        temporary = Path(temporary)
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
                           'controller' if index == 0 else 'node',
                           '--port', str(port), '--data-dir', str(temporary / name)]
                if index:
                    command += ['--max-processes', str(PROCESSES),
                                '--max-threads', str(THREADS)]
                log = (temporary / f'{name}.log').open('wb')
                logs.append(log)
                processes.append(subprocess.Popen(command, cwd=ROOT, stdout=log, stderr=log,
                                                   start_new_session=True,
                                                   env={k: v for k, v in os.environ.items()
                                                        if k != 'CLUSTER_LAB_ADMIN_PASSWORD'}))
                wait(lambda url=url: requests.get(url + '/healthz', timeout=1).ok, 30)
                clients.append(client(url))

            master, master_url = clients[0], urls[0]
            identities = []
            for index, (node_client, node_url) in enumerate(zip(clients[1:], urls[1:]), 1):
                response = master.post(master_url + '/api/nodes', json={
                    'name': f'本地验收节点 {index:02d}',
                    'processes': PROCESSES, 'threads': THREADS}, timeout=5)
                response.raise_for_status()
                identity = response.json()
                response = node_client.post(node_url + '/api/connect', json={
                    'url': master_url, 'token': identity['token']}, timeout=5)
                response.raise_for_status()
                identities.append(identity)

            def state():
                response = master.get(master_url + '/api/state', timeout=10)
                response.raise_for_status()
                return response.json()

            online = wait(lambda: state() if state()['totals']['online'] == NODE_COUNT else None, 45)
            assert all(node['effective_processes'] == PROCESSES and
                       node['effective_threads'] == THREADS for node in online['nodes'])

            query = requests.compat.urlencode({
                'name': source.name, 'unit_rows': UNIT_ROWS, 'mode': 'simulate',
                'enqueue': 'false', 'timeout': 20, 'retries': 0})
            with source.open('rb') as stream:
                response = master.post(master_url + '/api/jobs/stream?' + query, data=stream,
                                       headers={'Content-Type': 'application/octet-stream',
                                                'Content-Length': str(source.stat().st_size)},
                                       timeout=120)
            response.raise_for_status()
            job_id = response.json()['id']

            def job_state():
                current = state()
                return next(item for item in current['jobs'] if item['id'] == job_id), current

            job, _ = wait(lambda: job_state() if job_state()[0]['status'] == 'ready' else None, 45)
            expected_units = (len(lines) + UNIT_ROWS - 1) // UNIT_ROWS
            assert job['rows'] == len(lines) and job['units'] == expected_units
            response = master.post(master_url + f'/api/jobs/{job_id}/start', json={}, timeout=5)
            response.raise_for_status()

            started = time.monotonic()
            samples = []
            while True:
                job, current = job_state()
                samples.append({'seconds': round(time.monotonic() - started, 3),
                                'processed': job['processed'], 'confirmed': job['confirmed'],
                                'active_units': sum(node['active'] for node in current['nodes'])})
                if job['status'] == 'completed':
                    break
                if time.monotonic() - started > 180:
                    raise RuntimeError('job did not complete')
                time.sleep(.05)

            response = master.get(master_url + f'/api/jobs/{job_id}/results', timeout=30)
            if not response.ok:
                print(f'RESULT_DOWNLOAD_ERROR status={response.status_code} body={response.text[:500]}',
                      file=sys.stderr, flush=True)
            response.raise_for_status()
            assert response.content == payload

            database = sqlite3.connect(temporary / 'controller/controller.sqlite3')
            database.row_factory = sqlite3.Row
            units = list(database.execute(
                'SELECT seq,first_row,rows,node_id,result_path FROM units '
                'WHERE job_id=? ORDER BY seq', (job_id,)))
            distribution = [dict(row) for row in database.execute(
                'SELECT node_id,COUNT(*) AS units,SUM(rows) AS rows FROM units '
                'WHERE job_id=? AND status="completed" GROUP BY node_id ORDER BY node_id',
                (job_id,))]
            database.close()
            assert len(units) == expected_units
            assert len(distribution) == NODE_COUNT
            assert sum(row['units'] for row in distribution) == expected_units
            assert sum(row['rows'] for row in distribution) == len(lines)

            observed = set()
            for unit in units:
                records = [json.loads(line) for line in Path(unit['result_path']).read_text('utf-8').splitlines()]
                assert len(records) == unit['rows']
                for offset, record in enumerate(records):
                    row = unit['first_row'] + offset
                    raw = lines[row - 1].rstrip(b'\r\n')
                    assert record['row'] == row
                    assert record['sha256'] == hashlib.sha256(raw).hexdigest()
                    assert record['bytes'] == len(raw)
                    assert row not in observed
                    observed.add(row)
            assert observed == set(range(1, len(lines) + 1))

            def nodes_reconciled():
                values = [node_client.get(node_url + '/api/state', timeout=5).json()
                          for node_client, node_url in zip(clients[1:], urls[1:])]
                if (sum(value['confirmed'] for value in values) == len(lines)
                        and all(value['outbox'] == 0 for value in values)):
                    return values
                return None

            node_states = wait(nodes_reconciled, 45)
            report = {
                'input': {'name': source.name, 'bytes': len(payload), 'rows': len(lines),
                          'blank_rows': sum(not line.strip() for line in lines),
                          'sha256': hashlib.sha256(payload).hexdigest()},
                'configuration': {'nodes': NODE_COUNT, 'processes_per_node': PROCESSES,
                                  'threads_per_process': THREADS, 'unit_rows': UNIT_ROWS,
                                  'mode': 'local_simulate_no_external_auth'},
                'result': {'job_id': job_id, 'units': len(units),
                           'confirmed_rows': job['confirmed'],
                           'unique_result_rows': len(observed),
                           'download_matches_input': True,
                           'all_row_digests_match': True,
                           'node_confirmed_rows': sum(value['confirmed'] for value in node_states),
                           'node_outbox': sum(value['outbox'] for value in node_states),
                           'max_active_units': max(sample['active_units'] for sample in samples),
                           'elapsed_seconds': round(time.monotonic() - started, 3)},
                'distribution': distribution,
            }
            report_path = Path(report_path)
            report_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n', 'utf-8')
            print(json.dumps(report, ensure_ascii=False, indent=2))
        except Exception:
            for log in logs:
                log.flush()
            for path in sorted(temporary.glob('*.log')):
                tail = path.read_text('utf-8', errors='replace').splitlines()[-40:]
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
            for value in clients:
                value.close()
            for log in logs:
                log.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True, type=Path)
    parser.add_argument('--report', type=Path,
                        default=ROOT / 'runtime/four-nodes-10k/report.json')
    arguments = parser.parse_args()
    main(arguments.input, arguments.report)
