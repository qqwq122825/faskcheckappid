"""Bounded malformed-init controls for the user's one known account.

Six calls at most: accepted baseline, four one-field changes, final baseline.
No password proof, registration, automatic retry, proxy rotation or guessing.
"""
import base64
from copy import deepcopy
from datetime import datetime
import hashlib
import multiprocessing as mp
import os
from pathlib import Path
import plistlib
import queue
import signal
import sys
import time
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from fastcheck.diagnostics import SafeDiagnostics, TracedClient
from archive.legacy.gsa_batch import EventLog
from fastcheck.devices import DevicePool, save_json
from fastcheck.protocols import gsa_status as gsa
from tools.check_known_account_proxy import exit_ip

ORIGIN = 'https://new.ani.sidestore.io'
CONTROL = 'control@example.invalid'
CASES = ('baseline_before', 'u_empty', 'cpd_md_corrupt', 'A2k_zero', 'ps_unsupported', 'baseline_after')


def mutate(request, case):
    value = deepcopy(request)
    if case == 'u_empty':
        value['u'] = ''
    elif case == 'cpd_md_corrupt':
        raw = bytearray(base64.b64decode(value['cpd']['X-Apple-I-MD'], validate=True))
        if not raw:
            raise ValueError('Missing original device OTP')
        raw[0] ^= 1
        value['cpd']['X-Apple-I-MD'] = base64.b64encode(raw).decode()
    elif case == 'A2k_zero':
        value['A2k'] = b'\0'
    elif case == 'ps_unsupported':
        value['ps'] = ['unsupported-test-only']
    elif case not in ('baseline_before', 'baseline_after'):
        raise ValueError('Unknown fixed diagnostic case')
    return value


def worker(case, output):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    diagnostic = SafeDiagnostics(output, role='init_field_control', case=case)
    pool = DevicePool(ROOT/'config/device-pool', ORIGIN, max_devices=5, client_factory=TracedClient)
    before = pool.inspect(5).get('device_id')
    result = {'case': case, 'route': 'direct', 'device': 5, 'stage': 'prepare',
              'password_proof_submitted': False, 'automatic_retries': 0, 'requests': 0}
    try:
        with pool.operation(5):
            info = pool.info(10, diagnostics=diagnostic, provider_direct=True)
            headers = pool.fetch_headers(5, info, 10, diagnostics=diagnostic, provider_direct=True)
            cpd = gsa._build_cpd(headers, CONTROL)
            srp = gsa.SRPClient(CONTROL, 'unused-init-only')
            original = {'cpd': cpd, 'u': CONTROL, 'A2k': srp.A_bytes,
                        'ps': ['s2k', 's2k_fo'], 'o': 'init'}
            request = mutate(original, case)
            result['deliberately_changed_fields'] = [key for key in original if original[key] != request[key]]
            body = plistlib.dumps({'Header': {'Version': '1.0.1'}, 'Request': request})
            gsa.close_session()
            mark = time.perf_counter()
            result.update(stage='init', requests=1)
            outcome = gsa._session_for(None).post(body, request['cpd'], 10, diagnostics=diagnostic,
                request_phase='http_init', client_info=gsa._client_info(headers), user_agent=headers.get('User-Agent'))
            result.update(http=outcome.http_status, apple_ec=outcome.apple_ec, kind=outcome.kind,
                          content_type=outcome.content_type, init_ms=round((time.perf_counter()-mark)*1000,2))
            if outcome.payload is not None:
                response = outcome.payload.get('Response') or {}
                status = response.get('Status') or {}
                result.update(apple_ec=status.get('ec'), init_accepted=status.get('ec') == 0,
                              challenge_present=all(k in response for k in ('c','s','B','i','sp')))
    except Exception as exc:
        result.update(error_type=type(exc).__name__)
        http = getattr(exc, 'http', 0)
        if http:
            result['http'] = http
    finally:
        gsa.close_session()
        result['identity_preserved'] = before is not None and pool.inspect(5).get('device_id') == before
        result['device_fingerprint'] = hashlib.sha256(before.encode()).hexdigest()[:12] if before else None
        output.put({'result': result})


def run_case(context, log, case):
    output = context.Queue(maxsize=2048)
    process = context.Process(target=worker, args=(case, output), name='init-field-control')
    log('init_field_control_start', case=case)
    process.start()
    result = None
    try:
        deadline = time.monotonic()+30
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
    result = result or {'case': case, 'stage': 'worker', 'kind': 'timeout_or_exit'}
    log.result(result)
    log('init_field_control_done', **result)
    return result


def main():
    if gsa.mock_enabled():
        raise RuntimeError('Mock mode not permitted')
    context = mp.get_context('spawn')
    stamp = datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y%m%d-%H%M%S')
    directory = ROOT/'logs'/f'init-fields-{stamp}-{os.getpid()}'
    log = EventLog(directory, debug=True)
    report = {'account': 'user_designated_known_account', 'device': 'local/5', 'route': 'direct',
              'max_init_requests': 6, 'automatic_retries': 0, 'password_proof_submitted': False,
              'new_devices_registered': False, 'results': [],
              'limitations': ['Each case refreshes valid ephemeral device data and generates a fresh SRP key.',
                              'Only one named field is intentionally malformed; other normal ephemeral values also vary.',
                              'Single observations do not establish universal error-code mappings.']}
    try:
        report['exit_before'] = exit_ip()
        for case in CASES:
            result = run_case(context, log, case)
            report['results'].append(result)
            save_json(directory/'report.json', report)
            if result.get('http') in (403,429) or result.get('apple_ec') in (-20209,-20751,-20752,-22413,-22014):
                report['stop_reason'] = 'rate_limit_or_account_restriction'
                break
            if case == 'baseline_before' and not (result.get('http') == 200 and result.get('init_accepted') and result.get('challenge_present')):
                report['stop_reason'] = 'normal_baseline_not_accepted'
                break
            if case != CASES[-1]:
                time.sleep(1)
        report['exit_after'] = exit_ip()
    finally:
        save_json(directory/'report.json', report)
        save_json(directory/'debug-summary.json', log.debug_summary())
        print('参数对照报告：'+str(directory/'report.json'))


if __name__ == '__main__':
    main()
