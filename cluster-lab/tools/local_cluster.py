#!/usr/bin/env python3
"""Run an isolated controller and node that import the production core.

This file only orchestrates the two real ``app.py`` roles.  It deliberately
contains no executor, proxy-pool, device-pool, timeout or retry implementation.
"""
from __future__ import annotations

import argparse
import json
import signal
import subprocess
import sys
import time
import webbrowser
from pathlib import Path

import requests


ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / 'app.py'


def wait_health(url: str, process: subprocess.Popen, timeout: float = 30) -> dict:
    deadline = time.monotonic() + timeout
    last_error = ''
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f'{url} 进程提前退出，状态 {process.returncode}')
        try:
            response = requests.get(url + '/healthz', timeout=(1, 2))
            if response.ok:
                return response.json()
            last_error = f'HTTP {response.status_code}'
        except requests.RequestException as exc:
            last_error = type(exc).__name__
        time.sleep(.2)
    raise RuntimeError(f'{url} 未就绪：{last_error}')


def api_session(url: str) -> tuple[requests.Session, str]:
    session = requests.Session()
    session.trust_env = False
    response = session.get(url + '/api/session', timeout=(2, 5))
    response.raise_for_status()
    return session, response.json()['csrf']


def ensure_connected(controller_url: str, node_url: str) -> dict:
    node_session, node_csrf = api_session(node_url)
    state = node_session.get(node_url + '/api/state', timeout=(2, 5)).json()
    config = state.get('config') or {}
    if config.get('url') == controller_url and config.get('token_saved'):
        return {'reused': True, 'connected': bool(state.get('connected'))}

    controller_session, controller_csrf = api_session(controller_url)
    created = controller_session.post(
        controller_url + '/api/nodes',
        json={'name': '本地测试节点', 'processes': 2, 'threads': 500},
        headers={'X-CSRF-Token': controller_csrf}, timeout=(2, 5))
    created.raise_for_status()
    node = created.json()
    configured = controller_session.post(
        controller_url + f"/api/nodes/{node['id']}/config",
        json={'processes': 2, 'threads': 500, 'enabled': True},
        headers={'X-CSRF-Token': controller_csrf}, timeout=(2, 5))
    configured.raise_for_status()
    connected = node_session.post(
        node_url + '/api/connect',
        json={'url': controller_url, 'token': node['token']},
        headers={'X-CSRF-Token': node_csrf}, timeout=(2, 5))
    connected.raise_for_status()
    return {'reused': False, 'connected': True, 'node_id': node['id']}


def stop_process(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(8)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(3)


def main() -> int:
    parser = argparse.ArgumentParser(description='启动同核心本地总控与节点')
    parser.add_argument('--controller-port', type=int, default=9000)
    parser.add_argument('--node-port', type=int, default=9001)
    parser.add_argument('--no-browser', action='store_true')
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    controller_url = f'http://127.0.0.1:{args.controller_port}'
    node_url = f'http://127.0.0.1:{args.node_port}'
    description = {
        'controller': controller_url, 'node': node_url,
        'executor': 'clusterlab.executor.execute_unit_group',
        'processes': 2, 'threads': 500,
        'core': str((ROOT / 'clusterlab').resolve()),
    }
    if args.check:
        print(json.dumps(description, ensure_ascii=False, sort_keys=True))
        return 0

    runtime = ROOT / 'runtime' / 'local-cluster'
    runtime.mkdir(parents=True, exist_ok=True)
    common = [sys.executable, str(APP)]
    controller = subprocess.Popen(
        common + ['controller', '--host', '127.0.0.1', '--port', str(args.controller_port),
                  '--data-dir', str(runtime / 'controller')], cwd=ROOT)
    node = subprocess.Popen(
        common + ['node', '--host', '127.0.0.1', '--port', str(args.node_port),
                  '--data-dir', str(runtime / 'node'), '--max-processes', '2',
                  '--max-threads', '500'], cwd=ROOT)
    processes = (node, controller)

    def stop_all(*_args):
        for child in processes:
            stop_process(child)

    signal.signal(signal.SIGTERM, stop_all)
    signal.signal(signal.SIGINT, stop_all)
    try:
        controller_health = wait_health(controller_url, controller)
        node_health = wait_health(node_url, node)
        enrollment = ensure_connected(controller_url, node_url)
        description.update(controller_health=controller_health,
                           node_health=node_health, enrollment=enrollment)
        print(json.dumps(description, ensure_ascii=False, sort_keys=True), flush=True)
        if not args.no_browser:
            webbrowser.open(controller_url)
        while all(process.poll() is None for process in processes):
            time.sleep(.5)
        return next((process.returncode for process in processes
                     if process.returncode not in (None, 0)), 0)
    finally:
        stop_all()


if __name__ == '__main__':
    raise SystemExit(main())
