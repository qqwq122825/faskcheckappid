"""At most two explicit diagnostic attempts; existing identities, no retries."""
from datetime import datetime
import hashlib
import multiprocessing as mp
import os
from pathlib import Path
import queue
import secrets
import sys
import time
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import requests
from fastcheck.diagnostics import SafeDiagnostics, TracedClient
from archive.legacy.gsa_batch import EventLog, read_accounts
from fastcheck.devices import DevicePool, RetryPolicy, save_json

CONTROL = 'control@example.invalid'
ORIGIN = 'https://new.ani.sidestore.io'


def worker(pool_root, slot, email, password, label, output):
    diagnostics = SafeDiagnostics(output, role='account_control', label=label)
    pool = DevicePool(pool_root, ORIGIN, max_devices=10, client_factory=TracedClient,
                      policy=RetryPolicy(attempts=1))
    before = pool.inspect(slot).get('device_id')
    try:
        result = pool.authenticate(slot, email, password, timeout=10, prepare_timeout=15,
                                   proxy=None, provider_direct=True, diagnostics=diagnostics)
        safe = {key: result.get(key) for key in
                ('status', 'kind', 'stage', 'apple_ec', 'total_ms', 'attempts', 'attempt_history')}
        safe['identity_preserved'] = pool.inspect(slot).get('device_id') == before
        output.put({'result': safe})
    except Exception as exc:
        output.put({'result': {'status': 'error', 'kind': 'diagnostic',
                               'error_type': type(exc).__name__}})
    finally:
        password = ''


def run(context, log, pool_root, slot, email, password, label):
    pool = DevicePool(pool_root, ORIGIN, max_devices=10)
    fingerprint = hashlib.sha256(pool.inspect(slot)['device_id'].encode()).hexdigest()[:12]
    output = context.Queue(maxsize=2048)
    process = context.Process(target=worker, args=(str(pool_root), slot, email, password, label, output))
    log('account_control_start', label=label, device=slot, device_fingerprint=fingerprint,
        route='explicit_direct', max_attempts=1)
    process.start()
    result = None
    try:
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            try:
                message = output.get(timeout=.1)
            except queue.Empty:
                if not process.is_alive():
                    break
                continue
            if '_debug' in message:
                log.debug(message['_debug'])
            elif 'result' in message:
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
    result = result or {'status': 'error', 'kind': 'timeout', 'stage': 'worker'}
    result.update(label=label, device_fingerprint=fingerprint, route='explicit_direct')
    log('account_control_done', **result)
    return result


def main():
    context = mp.get_context('spawn')
    stamp = datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y%m%d-%H%M%S')
    directory = ROOT / 'logs' / f'account-control-{stamp}-{os.getpid()}'
    log = EventLog(directory, debug=True)
    with requests.Session() as session:
        session.trust_env = False
        response = session.get('https://api.ipify.org?format=json', timeout=(5, 8))
        response.raise_for_status()
        exit_ip = response.json()['ip']
    wrong_password = secrets.token_urlsafe(32)
    results = [run(context, log, ROOT / 'config/batch-device-pool', 1,
                   CONTROL, wrong_password, 'known_account_previously_refused_device')]
    first = results[0]
    if first.get('stage') == 'complete' and first.get('status') == 'wrong_password':
        accounts = read_accounts(ROOT / '账号.txt', max_accounts=20)
        account = accounts[0]
        for other in accounts[1:]:
            other.password = ''
        try:
            results.append(run(context, log, ROOT / 'config/batch-device-pool', 1,
                               account.email, account.password, 'file_row_1_same_device_and_route'))
        finally:
            account.password = ''
    elif first.get('stage') == 'init' and first.get('apple_ec') == -20970:
        results.append(run(context, log, ROOT / 'config/device-pool', 3,
                           CONTROL, wrong_password, 'known_account_previously_successful_device'))
    wrong_password = ''
    report = {'checked_at': datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),
              'exit_ip': exit_ip, 'route': 'explicit_direct', 'registered_new_devices': False,
              'automatic_retries': 0, 'max_auth_calls': 2,
              'control_password': 'random_deliberately_incorrect_not_saved', 'results': results,
              'limitations': ['No current successful-password positive control.',
                              'The previous batch used proxies; this comparison intentionally uses one direct route.',
                              'Separate connections can still reach different Apple backend instances.']}
    save_json(directory / 'report.json', report)
    save_json(directory / 'debug-summary.json', log.debug_summary())
    print('账号对照报告：' + str(directory / 'report.json'))


if __name__ == '__main__':
    main()
