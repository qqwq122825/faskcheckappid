"""Verify retry propagation, 10x100 leasing and live categories over HTTP."""
import argparse
import hashlib
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


def free_port():
    with socket.socket() as stream:
        stream.bind(('127.0.0.1', 0))
        return stream.getsockname()[1]


def wait(check, seconds=30):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            value = check()
            if value:
                return value
        except (OSError, requests.RequestException, KeyError, ValueError):
            pass
        time.sleep(0.05)
    raise RuntimeError('live feedback verification timed out')


def main(report_path):
    port = free_port()
    url = f'http://127.0.0.1:{port}'
    process = None
    browser = requests.Session()
    agent = requests.Session()
    browser.trust_env = agent.trust_env = False
    with tempfile.TemporaryDirectory(prefix='cluster-live-feedback-') as temporary:
        log_path = Path(temporary) / 'controller.log'
        with log_path.open('wb') as log:
            try:
                process = subprocess.Popen(
                    [sys.executable, str(ROOT / 'app.py'), 'controller', '--port', str(port),
                     '--data-dir', str(Path(temporary) / 'controller')],
                    cwd=ROOT, stdout=log, stderr=log, start_new_session=True,
                    env={key: value for key, value in os.environ.items()
                         if key != 'CLUSTER_LAB_ADMIN_PASSWORD'})
                wait(lambda: requests.get(url + '/healthz', timeout=1).ok)
                session = browser.get(url + '/api/session', timeout=5).json()
                browser.headers['X-CSRF-Token'] = session['csrf']
                identity = browser.post(url + '/api/nodes', json={
                    'name': 'live-feedback-node', 'processes': 10, 'threads': 32}, timeout=5).json()
                agent.headers['Authorization'] = 'Bearer ' + identity['token']
                heartbeat = {'session': 'feedback-session', 'hostname': 'local-test',
                             'version': '0.3.5', 'max_processes': 10, 'max_threads': 100,
                             'active': [], 'interrupted': []}
                first = agent.post(url + '/agent/heartbeat', json=heartbeat, timeout=5)
                first.raise_for_status()
                assert (first.json()['processes'], first.json()['threads']) == (10, 100)

                payload = b'hello\n' * 50
                query = requests.compat.urlencode({
                    'name': 'live-feedback.txt', 'unit_rows': 50, 'mode': 'auth',
                    'enqueue': 'false', 'timeout': 35, 'retries': 4})
                response = browser.post(url + '/api/jobs/stream?' + query, data=payload,
                                        headers={'Content-Type': 'application/octet-stream',
                                                 'Content-Length': str(len(payload))}, timeout=10)
                response.raise_for_status()
                job_id = response.json()['id']

                def state():
                    response = browser.get(url + '/api/state', timeout=5)
                    response.raise_for_status()
                    return response.json()

                wait(lambda: state() if next(job for job in state()['jobs']
                                              if job['id'] == job_id)['status'] == 'ready' else None)
                browser.post(url + f'/api/jobs/{job_id}/start', json={}, timeout=5).raise_for_status()
                lease = agent.post(url + '/agent/lease', json={'session': 'feedback-session'},
                                   timeout=5).json()['unit']
                assert (lease['processes'], lease['threads'], lease['timeout'], lease['retries']) == (10, 100, 35, 4)

                samples = []
                for processed, counts in (
                        (10, {'wrong_password': 3, 'locked': 2, 'undetermined': 5}),
                        (20, {'wrong_password': 6, 'locked': 4, 'undetermined': 10})):
                    tick = time.monotonic()
                    heartbeat['version'] = '0.3.6'
                    heartbeat['active'] = [{'id': lease['id'], 'token': lease['token'],
                                            'processed': processed, 'counts': counts}]
                    response = agent.post(url + '/agent/heartbeat', json=heartbeat, timeout=5)
                    response.raise_for_status()
                    current = next(job for job in state()['jobs'] if job['id'] == job_id)
                    samples.append({'elapsed': round(time.monotonic() - tick, 3),
                                    'processed': current['processed'],
                                    'categorized': current['categorized']})
                    assert current['processed'] == processed
                    assert current['categorized']['wrong_password'] == counts['wrong_password']
                    assert current['categorized']['locked'] == counts['locked']
                    assert current['categorized']['unknown'] == counts['undetermined']
                    time.sleep(1.0)

                statuses = (['wrong_password'] * 6 + ['locked'] * 4 +
                            ['undetermined'] * 10 + ['valid'] * 30)
                result = b''.join((json.dumps({
                    'row': index + 1, 'status': status,
                    'sha256': hashlib.sha256(b'hello').hexdigest(), 'bytes': 5},
                    separators=(',', ':')) + '\n').encode()
                    for index, status in enumerate(statuses))
                response = agent.post(url + f'/agent/units/{lease["id"]}/result',
                                      data={'lease_token': lease['token'],
                                            'checksum': hashlib.sha256(result).hexdigest()},
                                      files={'result': ('result.jsonl', io.BytesIO(result),
                                                        'application/x-ndjson')}, timeout=10)
                response.raise_for_status()
                final = next(job for job in state()['jobs'] if job['id'] == job_id)
                assert final['status'] == 'completed'
                assert final['categorized'] == {
                    'valid': 30, 'wrong_password': 6, 'need_2fa': 0,
                    'locked': 4, 'timeout': 0, 'unknown': 10}
                report = {'version': '0.3.6', 'lease': {
                    'processes': lease['processes'], 'threads': lease['threads'],
                    'timeout': lease['timeout'], 'retries': lease['retries']},
                    'samples': samples, 'final': final['categorized'],
                    'result': 'live categories updated before package completion; final counts did not double'}
                report_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n', 'utf-8')
                print(json.dumps(report, ensure_ascii=False, indent=2))
            finally:
                if process and process.poll() is None:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait(timeout=5)
                browser.close()
                agent.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--report', required=True, type=Path)
    main(parser.parse_args().report)
