"""One init-only comparison using a known account and an existing identity."""
from datetime import datetime
import argparse
import getpass
import hashlib
import ipaddress
import multiprocessing as mp
import os
from pathlib import Path
import plistlib
import queue
import sys
import time
import warnings
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import requests
from fastcheck.diagnostics import SafeDiagnostics, TracedClient
from fastcheck.proxy_pool import ApiFetcher
from archive.legacy.gsa_batch import EventLog, proxy_fingerprint
from fastcheck.devices import DevicePool, save_json
from fastcheck.protocols import gsa_status as gsa

ORIGIN = 'https://new.ani.sidestore.io'
CONTROL = 'control@example.invalid'


def exit_ip(proxy=None):
    with requests.Session() as session:
        session.trust_env = False
        if proxy:
            session.proxies = {'http': proxy, 'https': proxy}
        with session.get('https://api.ipify.org?format=json', timeout=(5, 7),
                         allow_redirects=False) as response:
            response.raise_for_status()
            return str(ipaddress.ip_address(response.json()['ip']))


def worker(proxy, output, pool_root=None, slot=1):
    diagnostics = SafeDiagnostics(output, role='known_account_proxy_init',
                                  proxy_fingerprint=proxy_fingerprint(proxy) if proxy else None)
    pool = DevicePool(pool_root or ROOT / 'config/batch-device-pool', ORIGIN, max_devices=10,
                      client_factory=TracedClient)
    before = pool.inspect(slot).get('device_id')
    result = {'stage': 'proxy_probe', 'authentication_requests': 0}
    try:
        result['exit_before'] = exit_ip(proxy)
        with pool.operation(slot):
            info = pool.info(12, proxy=proxy, provider_direct=True, diagnostics=diagnostics)
            headers = pool.fetch_headers(slot, info, 12, proxy=proxy, provider_direct=True,
                                         diagnostics=diagnostics)
            cpd = gsa._build_cpd(headers, CONTROL)
            # A is independent of the password; no complete/M1 request is made.
            srp = gsa.SRPClient(CONTROL, 'unused-init-only')
            body = plistlib.dumps({'Header': {'Version': '1.0.1'}, 'Request': {
                'cpd': cpd, 'u': CONTROL, 'A2k': srp.A_bytes,
                'ps': ['s2k', 's2k_fo'], 'o': 'init'}})
            result.update(stage='init', authentication_requests=1)
            started = time.perf_counter()
            gsa.close_session()
            outcome = gsa._session_for(proxy).post(body, cpd, 10, diagnostics=diagnostics,
                request_phase='http_init', client_info=gsa._client_info(headers),
                user_agent=headers.get('User-Agent'))
            result.update(http=outcome.http_status, apple_ec=outcome.apple_ec,
                          kind=outcome.kind, init_ms=round((time.perf_counter()-started)*1000, 2))
            if outcome.payload is not None:
                response = outcome.payload.get('Response') or {}
                status = response.get('Status') or {}
                result['apple_ec'] = status.get('ec')
                result['init_accepted'] = status.get('ec') == 0
                result['challenge_present'] = all(key in response for key in ('c', 's', 'B', 'i', 'sp'))
            else:
                result['init_accepted'] = False
        result['exit_after'] = exit_ip(proxy)
    except Exception as exc:
        result.update(error_type=type(exc).__name__)
    finally:
        gsa.close_session()
        result.update(identity_preserved=pool.inspect(slot).get('device_id') == before,
                      device_fingerprint=hashlib.sha256(before.encode()).hexdigest()[:12],
                      password_proof_submitted=False, automatic_retries=0)
        output.put({'result': result})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--direct', action='store_true')
    parser.add_argument('--local-device', type=int, choices=range(1, 6))
    args = parser.parse_args()
    api_url = ''
    if not args.direct:
        with warnings.catch_warnings():
            warnings.simplefilter('error', getpass.GetPassWarning)
            api_url = getpass.getpass('代理 API 地址（隐藏输入，不保存）：').strip()
    context = mp.get_context('spawn')
    stop = context.Event()
    stamp = datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y%m%d-%H%M%S')
    directory = ROOT / 'logs' / f'known-account-proxy-{stamp}-{os.getpid()}'
    log = EventLog(directory, debug=True)
    direct_exit = exit_ip()
    lease_start = time.monotonic()
    proxy = None if args.direct else ApiFetcher(context, stop, debug_log=log.debug)(api_url, 1)[0]
    api_url = ''
    output = context.Queue(maxsize=2048)
    pool_root = ROOT/'config/device-pool' if args.local_device else ROOT/'config/batch-device-pool'
    slot = args.local_device or 1
    process = context.Process(target=worker, args=(proxy, output, str(pool_root), slot), name='known-account-proxy-init')
    log('proxy_control_start', route='explicit_proxy' if proxy else 'explicit_direct',
        device=slot, max_authentication_requests=1)
    process.start()
    result = None
    try:
        deadline = min(time.monotonic()+45, lease_start+58)
        while time.monotonic() < deadline:
            try:
                message = output.get(timeout=.1)
            except queue.Empty:
                if not process.is_alive():
                    break
                continue
            if '_debug' in message:
                log.debug(message['_debug'])
            else:
                result = message['result']
                break
    finally:
        if result is None and process.is_alive():
            process.terminate()
        process.join(.5)
        if process.is_alive():
            process.kill()
            process.join(2)
        process.close()
        output.cancel_join_thread()
        output.close()
        stop.set()
    report = {'checked_at': datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),
              'direct_exit': direct_exit, 'proxy_fingerprint': proxy_fingerprint(proxy) if proxy else None,
              'existing_device': f'{pool_root.name}/{slot}', 'registered_new_devices': False,
              'result': result or {'kind': 'timeout', 'stage': 'worker'},
              'limitations': ['A fresh proxy cannot reconstruct the expired historical proxy lease.',
                              'IP probes observe the probe endpoint, not Apple server-side telemetry.',
                              'Init-only result does not validate a password or complete a login.']}
    save_json(directory / 'report.json', report)
    save_json(directory / 'debug-summary.json', log.debug_summary())
    log('proxy_control_done', **report['result'])
    print('代理初始化对照报告：' + str(directory / 'report.json'))


if __name__ == '__main__':
    main()
