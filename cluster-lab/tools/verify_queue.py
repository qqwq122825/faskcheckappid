"""Real local HTTP FIFO check: two 100,000-record tasks and one offline node."""
import io
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
import requests

ROOT = Path(__file__).resolve().parents[1]


def wait(check, seconds=120):
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        try:
            value = check()
            if value:
                return value
        except requests.ConnectionError:
            pass
        time.sleep(.1)
    raise RuntimeError('Queue verification timed out')


def main():
    processes, logs, urls = [], [], []
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix='cluster-queue-') as temporary:
        try:
            for role in ('controller', 'node'):
                with socket.socket() as sock:
                    sock.bind(('127.0.0.1', 0))
                    port = sock.getsockname()[1]
                url = f'http://127.0.0.1:{port}'
                urls.append(url)
                log = (Path(temporary) / (role + '.log')).open('wb')
                logs.append(log)
                command = [sys.executable, str(ROOT / (role + '.py')), '--port', str(port), '--data-dir', str(Path(temporary) / role)]
                if role == 'node':
                    command += ['--max-processes', '2', '--max-threads', '4']
                environment = dict(os.environ)
                environment.pop('CLUSTER_LAB_ADMIN_PASSWORD', None)
                processes.append(subprocess.Popen(command, cwd=ROOT, env=environment, stdout=log, stderr=log, start_new_session=True))
                wait(lambda: requests.get(url + '/healthz', timeout=1).ok, 15)
            master_url, node_url = urls
            master, node = requests.Session(), requests.Session()
            for client, url in ((master, master_url), (node, node_url)):
                client.trust_env = False
                client.headers['X-CSRF-Token'] = client.get(url + '/api/session', timeout=5).json()['csrf']
            response = master.post(master_url + '/api/nodes', json={'name':'queue-test','processes':2,'threads':4}, timeout=5)
            response.raise_for_status()
            identity = response.json()
            sources, ids = [], []
            for name in ('A', 'B'):
                source = b''.join(f'offline-{name}-{number:08d}\n'.encode() for number in range(1, 100001))
                response = master.post(master_url + '/api/jobs', data={'name':'Queue '+name,'enqueue':'true','unit_rows':'10000'}, files={'file':(name+'.txt',io.BytesIO(source),'text/plain')}, timeout=15)
                response.raise_for_status()
                sources.append(source)
                ids.append(response.json()['id'])
            def snapshot():
                response = master.get(master_url + '/api/state', timeout=10)
                response.raise_for_status()
                return response.json()
            def prepared():
                state = snapshot()
                return state if all(job['status'] != 'preparing' for job in state['jobs']) else None
            state = wait(prepared)
            jobs = {job['id']:job for job in state['jobs']}
            assert jobs[ids[0]]['status'] == 'running' and jobs[ids[1]]['status'] == 'queued'
            assert jobs[ids[1]]['queue_position'] == 1
            response = node.post(node_url + '/api/connect', json={'url':master_url,'token':identity['token']}, timeout=5)
            response.raise_for_status()
            def finished():
                state = snapshot()
                jobs = {job['id']:job for job in state['jobs']}
                assert sum(job['status'] == 'running' for job in jobs.values()) <= 1
                if jobs[ids[1]]['processed']:
                    assert jobs[ids[0]]['status'] == 'completed', 'Second task overtook first task'
                return state if all(jobs[job]['status'] == 'completed' for job in ids) else None
            final = wait(finished)
            for job, source in zip(ids, sources):
                response = master.get(master_url + f'/api/jobs/{job}/results', timeout=15)
                response.raise_for_status()
                assert response.content == source and response.headers['Content-Type'].startswith('text/plain')
            receipt = dict(task_count=2, rows_per_task=100000, shards_per_task=10, confirmed_total=final['totals']['confirmed'], fifo_verified=True, both_txt_exports_match=True, elapsed_seconds=round(time.monotonic()-started,2))
            assert receipt['confirmed_total'] == 200000
            (ROOT/'runtime').mkdir(exist_ok=True)
            (ROOT/'runtime/queue-verification.json').write_text(json.dumps(receipt,ensure_ascii=False,indent=2))
            print(json.dumps(receipt,ensure_ascii=False),flush=True)
        finally:
            for process in reversed(processes):
                if process.poll() is None:
                    os.killpg(process.pid,signal.SIGTERM)
                    process.wait(timeout=10)
            for log in logs:
                log.close()


if __name__ == '__main__':
    main()
