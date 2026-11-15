"""Adversarial local HTTP verification for the two-node primary-instance lock.

Runs three 10,002-row simulated jobs through two real node services.  While
both nodes are processing 5,001 rows, extra Node instances open the same data
directories and briefly start their agent threads.  No external authentication
or proxy request is made.
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
sys.path.insert(0, str(ROOT))

from clusterlab import VERSION  # noqa: E402
from clusterlab.node import Node  # noqa: E402


NODE_COUNT = 2
ROWS = 10_002


def free_port():
    with socket.socket() as stream:
        stream.bind(('127.0.0.1', 0))
        return stream.getsockname()[1]


def wait(check, seconds=90):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            last = check()
            if last:
                return last
        except (OSError, requests.RequestException, KeyError, ValueError):
            pass
        time.sleep(.05)
    raise RuntimeError(f'等待本地双节点状态超时；last={last!r}')


def client(url):
    value = requests.Session()
    value.trust_env = False
    response = value.get(url + '/api/session', timeout=5)
    response.raise_for_status()
    value.headers['X-CSRF-Token'] = response.json()['csrf']
    return value


def main(report_path, process_count=2, threads=10, rounds_count=3, simulate_delay=.02):
    payload = b''.join(
        f'local-{row:05d}@example.invalid----LOCK-TEST-{row:05d}\n'.encode()
        for row in range(1, ROWS + 1)
    )
    child_processes, logs, clients = [], [], []
    with tempfile.TemporaryDirectory(prefix='cluster-two-node-lock-') as directory:
        root = Path(directory)
        ports = []
        while len(ports) < NODE_COUNT + 1:
            port = free_port()
            if port not in ports:
                ports.append(port)
        urls = [f'http://127.0.0.1:{port}' for port in ports]
        try:
            for index, (port, url) in enumerate(zip(ports, urls)):
                name = 'controller' if index == 0 else f'node-{index}'
                command = [sys.executable, str(ROOT / 'app.py'),
                           'controller' if index == 0 else 'node',
                           '--port', str(port), '--data-dir', str(root / name)]
                if index:
                    command += ['--max-processes', str(process_count),
                                '--max-threads', str(threads)]
                log = (root / f'{name}.log').open('wb')
                logs.append(log)
                environment = {key: value for key, value in os.environ.items()
                               if key != 'CLUSTER_LAB_ADMIN_PASSWORD'}
                environment['CLUSTER_LAB_SIMULATE_DELAY'] = str(simulate_delay)
                child_processes.append(subprocess.Popen(
                    command, cwd=ROOT, stdout=log, stderr=log,
                    start_new_session=True, env=environment))
                wait(lambda url=url: requests.get(url + '/healthz', timeout=1).ok, 30)
                clients.append(client(url))

            master, master_url = clients[0], urls[0]
            identities = []
            for index, (node_client, node_url) in enumerate(zip(clients[1:], urls[1:]), 1):
                response = master.post(master_url + '/api/nodes', json={
                    'name': f'本地锁验证节点 {index:02d}',
                    'processes': process_count, 'threads': threads}, timeout=5)
                response.raise_for_status()
                identity = response.json()
                identities.append(identity)
                response = node_client.post(node_url + '/api/connect', json={
                    'url': master_url, 'token': identity['token']}, timeout=5)
                response.raise_for_status()

            controller_db = root / 'controller/controller.sqlite3'
            with sqlite3.connect(controller_db) as database:
                database.execute('UPDATE nodes SET enabled=1,device_request=0')

            def state():
                response = master.get(master_url + '/api/state', timeout=10)
                response.raise_for_status()
                return response.json()

            wait(lambda: state() if all(node['status'] in ('idle', 'busy') and node['enabled']
                                        for node in state()['nodes']) else None, 30)
            rounds = []
            for round_number in range(1, rounds_count + 1):
                query = requests.compat.urlencode({
                    'name': f'two-node-lock-{round_number}.txt',
                    'unit_rows': 10_000, 'mode': 'simulate',
                    'enqueue': 'false', 'timeout': 20, 'retries': 0})
                response = master.post(
                    master_url + '/api/jobs/stream?' + query, data=payload,
                    headers={'Content-Type': 'application/octet-stream',
                             'Content-Length': str(len(payload))}, timeout=120)
                response.raise_for_status()
                job_id = response.json()['id']

                def job_state():
                    current = state()
                    job = next(item for item in current['jobs'] if item['id'] == job_id)
                    return job, current

                wait(lambda: job_state() if job_state()[0]['status'] == 'ready' else None, 30)
                response = master.post(master_url + f'/api/jobs/{job_id}/start', json={}, timeout=5)
                response.raise_for_status()
                started = time.monotonic()
                wait(lambda: job_state() if sum(node['active'] for node in job_state()[1]['nodes']) == 2
                     else None, 30)

                adversarial = []
                for node_index in range(1, NODE_COUNT + 1):
                    node_root = root / f'node-{node_index}'
                    secondary = Node(node_root, process_count, threads, allow_http=True, interval=.1)
                    assert not secondary.primary_instance
                    secondary.start()
                    time.sleep(.35)
                    secondary.close()
                    for _ in range(4):
                        probe = Node(node_root, process_count, threads, allow_http=True)
                        assert not probe.primary_instance
                        probe.close()
                    with sqlite3.connect(node_root / 'node.sqlite3') as database:
                        rows = database.execute(
                            'SELECT status,processed,error FROM tasks WHERE job_id=?',
                            (job_id,)).fetchall()
                    assert rows and all(row[0] not in ('interrupted', 'rejected') for row in rows), rows
                    adversarial.append({'node': node_index, 'extra_instances': 5,
                                        'statuses_after_injection': rows})

                job, _ = wait(lambda: job_state() if job_state()[0]['status'] == 'completed'
                              else None, 90)
                result = master.get(master_url + f'/api/jobs/{job_id}/results', timeout=30)
                result.raise_for_status()
                assert result.content == payload
                with sqlite3.connect(controller_db) as database:
                    distribution = database.execute(
                        'SELECT node_id,seq,rows,status FROM units WHERE job_id=? ORDER BY seq',
                        (job_id,)).fetchall()
                    interrupted = database.execute(
                        "SELECT COUNT(*) FROM events WHERE job_id=? AND code='node_interrupted'",
                        (job_id,)).fetchone()[0]
                assert len(distribution) == 2, distribution
                assert [row[2] for row in distribution] == [5001, 5001], distribution
                assert all(row[3] == 'completed' for row in distribution), distribution
                assert len({row[0] for row in distribution}) == 2, distribution
                assert interrupted == 0
                node_tasks = []
                for node_index in range(1, NODE_COUNT + 1):
                    with sqlite3.connect(root / f'node-{node_index}/node.sqlite3') as database:
                        node_tasks.extend(database.execute(
                            'SELECT status,processed,error FROM tasks WHERE job_id=?',
                            (job_id,)).fetchall())
                assert len(node_tasks) == 2 and all(row[0] == 'confirmed' and row[1] == 5001
                                                    and row[2] is None for row in node_tasks), node_tasks
                rounds.append({
                    'round': round_number, 'job_id': job_id,
                    'confirmed': job['confirmed'], 'remaining': job['rows'] - job['confirmed'],
                    'unit_rows': [row[2] for row in distribution],
                    'unit_statuses': [row[3] for row in distribution],
                    'nodes_used': len({row[0] for row in distribution}),
                    'node_tasks': node_tasks, 'node_interrupted_events': interrupted,
                    'adversarial': adversarial,
                    'elapsed_seconds': round(time.monotonic() - started, 3),
                })

            report = {
                'version': VERSION,
                'mode': 'local_simulate_no_external_requests',
                'input': {'rows': ROWS, 'bytes': len(payload),
                          'sha256': hashlib.sha256(payload).hexdigest()},
                'configuration': {'nodes': NODE_COUNT, 'processes_per_node': process_count,
                                  'threads_per_process': threads, 'rounds': rounds_count,
                                  'simulate_delay': simulate_delay,
                                  'extra_node_instances_per_node_per_round': 5},
                'result': {'rounds_passed': len(rounds),
                           'confirmed_rows_total': sum(item['confirmed'] for item in rounds),
                           'node_interrupted_events': sum(item['node_interrupted_events']
                                                          for item in rounds),
                           'all_units_completed': all(
                               status == 'completed' for item in rounds
                               for status in item['unit_statuses'])},
                'rounds': rounds,
            }
            report_path = Path(report_path).resolve()
            report_path.parent.mkdir(parents=True, exist_ok=True)
            report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n', 'utf-8')
            print(json.dumps(report, ensure_ascii=False, indent=2))
        except Exception:
            for log in logs:
                log.flush()
            for path in sorted(root.glob('*.log')):
                print(f'--- {path.name} tail ---', file=sys.stderr)
                print('\n'.join(path.read_text('utf-8', errors='replace').splitlines()[-60:]),
                      file=sys.stderr)
            raise
        finally:
            for process in reversed(child_processes):
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
    parser.add_argument('--report', required=True, type=Path)
    parser.add_argument('--processes', type=int, default=2)
    parser.add_argument('--threads', type=int, default=10)
    parser.add_argument('--rounds', type=int, default=3)
    parser.add_argument('--delay', type=float, default=.02)
    arguments = parser.parse_args()
    main(arguments.report, arguments.processes, arguments.threads,
         arguments.rounds, arguments.delay)
