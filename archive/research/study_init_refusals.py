"""Fixed own-account init comparison: proxy once, direct once only for -20970.

Never reads passwords into child processes, registers identities, or completes
authentication. Existing production leases/cooldowns remain in force. Findings
are kept in a separate study ledger rather than treating a refusal as a proven
device fault.
"""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import getpass
import hashlib
import multiprocessing as mp
import os
from pathlib import Path
import plistlib
import queue
import secrets
import signal
import sys
import threading
import time
import warnings
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from fastcheck.diagnostics import SafeDiagnostics, TracedClient
from fastcheck.proxy_pool import ApiFetcher
from archive.legacy.gsa_batch import EventLog, proxy_fingerprint, read_accounts
from fastcheck.devices import DevicePool, save_json
from fastcheck.protocols import gsa_status as gsa
from tools.check_known_account_proxy import exit_ip

ORIGIN = 'https://new.ani.sidestore.io'


def init_worker(payload, output):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    started = time.perf_counter()
    diagnostic = SafeDiagnostics(output, role='init_refusal_study', row=payload['row'],
                                 route=payload['route'], device=payload['slot'])
    pool = DevicePool(payload['root'], ORIGIN, max_devices=10, client_factory=TracedClient)
    before = pool.inspect(payload['slot']).get('device_id')
    result = {key: payload[key] for key in ('row', 'route', 'slot')}
    result.update(stage='egress_probe', authentication_requests=0,
                  password_proof_submitted=False, automatic_retries=0)
    try:
        result['exit_ip'] = exit_ip(payload['proxy'])
        with pool.operation(payload['slot']):
            info = pool.info(10, proxy=payload['proxy'], diagnostics=diagnostic, provider_direct=True)
            headers = pool.fetch_headers(payload['slot'], info, 10, proxy=payload['proxy'],
                                         diagnostics=diagnostic, provider_direct=True)
            cpd = gsa._build_cpd(headers, payload['email'])
            srp = gsa.SRPClient(payload['email'], 'unused-init-only')
            body = plistlib.dumps({'Header': {'Version': '1.0.1'}, 'Request': {
                'cpd': cpd, 'u': payload['email'], 'A2k': srp.A_bytes,
                'ps': ['s2k', 's2k_fo'], 'o': 'init'}})
            result.update(stage='init', authentication_requests=1)
            mark = time.perf_counter()
            gsa.close_session()
            outcome = gsa._session_for(payload['proxy']).post(body, cpd, 10,
                diagnostics=diagnostic, request_phase='http_init',
                client_info=gsa._client_info(headers), user_agent=headers.get('User-Agent'))
            result.update(http=outcome.http_status, apple_ec=outcome.apple_ec,
                          kind=outcome.kind, init_ms=round((time.perf_counter()-mark)*1000, 2))
            if outcome.payload is not None:
                response = outcome.payload.get('Response') or {}
                status = response.get('Status') or {}
                ec = status.get('ec')
                result.update(apple_ec=ec, init_accepted=ec == 0,
                              challenge_present=all(k in response for k in ('c', 's', 'B', 'i', 'sp')))
                classification, _ = gsa.classify(ec, status.get('em', '') or '')
                result['classification'] = 'init_accepted' if ec == 0 else classification
                if classification == 'wrong_password':
                    result['classification'] = 'init_rejected_password_not_checked'
    except Exception as exc:
        http = getattr(exc, 'http', 0) or getattr(getattr(exc, 'response', None), 'status_code', 0)
        result.update(error_type=type(exc).__name__)
        if http:
            result['http'] = http
        if http == 429:
            result['kind'] = 'ratelimit'
    finally:
        gsa.close_session()
        after = pool.inspect(payload['slot']).get('device_id')
        result.update(identity_preserved=before is not None and before == after,
                      device_fingerprint=hashlib.sha256(before.encode()).hexdigest()[:12] if before else None,
                      total_ms=round((time.perf_counter()-started)*1000, 2))
        output.put({'result': result})


class Study:
    def __init__(self, api_url, directory):
        self.context = mp.get_context('spawn')
        self.stop = self.context.Event()
        self.log = EventLog(directory, debug=True)
        self.directory = directory
        self.api_url = api_url
        self.fetch_lock = threading.Lock()
        self.local_lock = threading.Lock()
        self.local_free = set(range(1, 6))
        self.primary_free = queue.Queue()
        for slot in (1, 2):
            self.primary_free.put(slot)
        self.findings_lock = threading.Lock()
        self.findings = []
        self.fetcher = ApiFetcher(self.context, self.stop, debug_log=self.log.debug)

    def execute(self, row, email, root, slot, route, proxy=None, expires=None):
        if self.stop.is_set():
            return {'row': row, 'route': route, 'stage': 'not_started', 'kind': 'study_stopped'}
        payload = dict(row=row, email=email, root=str(root), slot=slot, route=route, proxy=proxy)
        output = self.context.Queue(maxsize=2048)
        process = self.context.Process(target=init_worker, args=(payload, output),
                                       name=f'init-study-row-{row}-{route}')
        self.log('init_comparison_start', row=row, route=route, device=slot)
        process.start()
        result = None
        try:
            deadline = min(time.monotonic()+35, expires-2 if expires else float('inf'))
            while time.monotonic() < deadline and not self.stop.is_set():
                try:
                    message = output.get(timeout=.1)
                except queue.Empty:
                    if not process.is_alive():
                        break
                    continue
                if '_debug' in message:
                    self.log.debug(message['_debug'])
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
        result = result or {'row': row, 'route': route, 'slot': slot, 'stage': 'worker',
                            'kind': 'stopped' if self.stop.is_set() else 'timeout_or_child_exit'}
        self.log.result(result)
        self.log('init_comparison_done', **result)
        if result.get('http') == 429 or result.get('kind') == 'ratelimit':
            self.stop.set()
        return result

    def account(self, row, email):
        slot = self.primary_free.get()
        try:
            if self.stop.is_set():
                return {'row': row, 'skipped': 'study_stopped'}
            with self.fetch_lock:
                if self.stop.is_set():
                    return {'row': row, 'skipped': 'study_stopped'}
                lease_started = time.monotonic()
                proxy = self.fetcher(self.api_url, 1)[0]
            primary = self.execute(row, email, ROOT/'config/batch-device-pool', slot,
                                   'proxy', proxy, expires=lease_started+60)
            primary['proxy_fingerprint'] = proxy_fingerprint(proxy)
            record = {'row': row, 'primary': primary}
            if primary.get('stage') == 'init' and primary.get('apple_ec') == -20970:
                with self.findings_lock:
                    self.findings.append({'row': row, 'primary': primary, 'direct_check': 'pending'})
                    save_json(self.directory/'refused-accounts.json', self.findings)
                self.log('init_20970_recorded', row=row, device=slot)
                if not self.stop.is_set():
                    with self.local_lock:
                        direct_slot = secrets.choice(sorted(self.local_free))
                        self.local_free.remove(direct_slot)
                    try:
                        record['direct'] = self.execute(row, email, ROOT/'config/device-pool',
                                                        direct_slot, 'direct')
                    finally:
                        with self.local_lock:
                            self.local_free.add(direct_slot)
                    with self.findings_lock:
                        for item in self.findings:
                            if item['row'] == row:
                                item['direct_check'] = record['direct']
                        save_json(self.directory/'refused-accounts.json', self.findings)
            return record
        except Exception as exc:
            self.stop.set()
            return {'row': row, 'error_type': type(exc).__name__, 'stage': 'supervisor'}
        finally:
            self.primary_free.put(slot)


def main():
    accounts = read_accounts(ROOT/'账号.txt', max_accounts=20)
    # Previously observed -20209: do not keep challenging a security-locked account.
    candidates = [(a.row, a.email) for a in accounts if a.row != 1]
    for account in accounts:
        account.password = ''
    for root, slots in ((ROOT/'config/device-pool', range(1, 6)),
                        (ROOT/'config/batch-device-pool', (1, 2))):
        pool = DevicePool(root, ORIGIN, max_devices=10)
        for slot in slots:
            if pool.inspect(slot)['state'] != 'ready':
                raise RuntimeError('Required existing device missing; registration disabled')
            pool.allowed(slot)
    with warnings.catch_warnings():
        warnings.simplefilter('error', getpass.GetPassWarning)
        api_url = getpass.getpass('代理 API 地址（隐藏输入，不保存）：').strip()
    stamp = datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y%m%d-%H%M%S')
    directory = ROOT/'logs'/f'init-refusal-study-{stamp}-{os.getpid()}'
    study = Study(api_url, directory)
    api_url = ''
    save_json(directory/'refused-accounts.json', [])
    study.log('init_study_start', accounts_in_file=len(accounts), candidates=len(candidates),
              skipped_security_lock_row=1, concurrency=2, max_init_requests_per_account=2,
              password_proof_submitted=False, registered_new_devices=False)
    records = []
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(study.account, row, email) for row, email in candidates]
            try:
                for future in as_completed(futures):
                    records.append(future.result())
                    save_json(directory/'account-results.json', sorted(records, key=lambda v:v['row']))
            except KeyboardInterrupt:
                study.stop.set()
                raise
    except KeyboardInterrupt:
        study.stop.set()
        raise
    finally:
        study.api_url = ''
        save_json(directory/'debug-summary.json', study.log.debug_summary())
        summary = {'accounts_in_file': len(accounts), 'skipped_known_security_lock_rows': [1],
                   'records': sorted(records, key=lambda v:v['row']),
                   'proxy_20970_rows': sorted(v['row'] for v in study.findings),
                   'proxy_ec_counts': dict(Counter(str(v.get('primary',{}).get('apple_ec')) for v in records)),
                   'direct_ec_counts': dict(Counter(str(v['direct'].get('apple_ec')) for v in records if 'direct' in v)),
                   'stopped': study.stop.is_set(), 'password_proof_submitted': False,
                   'limitations': ['Direct comparison changes both device and route.',
                                   'Init acceptance does not validate a password.',
                                   'Transient timing and backend differences remain possible.']}
        save_json(directory/'summary.json', summary)
        study.log('init_study_done', processed=len(records), proxy_20970_rows=summary['proxy_20970_rows'],
                  proxy_ec_counts=summary['proxy_ec_counts'], direct_ec_counts=summary['direct_ec_counts'])
        print('初始化对照报告：'+str(directory/'summary.json'))


if __name__ == '__main__':
    main()
