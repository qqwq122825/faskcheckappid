"""Two isolated auth processes, one proxy supplier, one registration supervisor."""
from __future__ import annotations

from collections import Counter, deque
from dataclasses import dataclass, field
from datetime import datetime
import getpass
import hashlib
import heapq
import json
import multiprocessing as mp
import os
from pathlib import Path
import queue
import random
import signal
import sys
import threading
import time
import warnings
from zoneinfo import ZoneInfo
from urllib.parse import urlsplit

from fastcheck.proxy_pool import ApiFetcher, ProxyPool
from fastcheck.devices import DevicePool, PoolError, RetryPolicy, file_lease, private_directory, save_json
from fastcheck.single_login import _dependencies
from fastcheck.protocols import gsa_status as gsa
from fastcheck.protocols.anisette_v3 import AnisetteError
from fastcheck.diagnostics import SafeDiagnostics, TracedClient


@dataclass(repr=False)
class Account:
    row: int
    email: str
    password: str = field(repr=False)
    queued_at: float = 0


def read_accounts(path, *, max_accounts=10):
    records, seen = [], set()
    for line_number, line in enumerate(Path(path).expanduser().read_text(encoding='utf-8-sig').splitlines(), 1):
        if not line.strip():
            continue
        email, separator, password = line.partition('----')
        email = email.strip()
        if not separator or not password or '@' not in email or email.endswith('.invalid'):
            raise ValueError(f'第 {line_number} 行格式错误或仍为示例：应为 邮箱----密码。')
        if len(email) > 4096 or len(password) > 4096 or any(ord(c) < 32 for c in email + password):
            raise ValueError(f'第 {line_number} 行包含无效字段。')
        if email.casefold() in seen:
            raise ValueError(f'第 {line_number} 行账号重复，请每个账号仅保留一行。')
        seen.add(email.casefold())
        records.append(Account(len(records) + 1, email, password))
    if not records or (max_accounts is not None and len(records)>max_accounts):
        raise ValueError(f'此测试入口接收 1–{max_accounts} 个不同的自有账号。' if max_accounts else '账号文件不能为空。')
    return records


class EventLog:
    labels = {
        'batch_start':'批次启动', 'proxy_pool_start':'代理池启动', 'proxy_fetch_start':'代理 API 开始补充',
        'proxy_fetch_done':'代理 API 补充完成', 'proxy_fetch_failed':'代理 API 提取失败',
        'proxy_expired':'丢弃过期代理', 'proxy_too_old':'代理剩余有效期不足',
        'proxy_supplier_stopped':'代理供给线程停止', 'devices_restored':'读取已注册设备',
        'proxy_lease_acquired':'领取代理租约', 'proxy_lease_released':'释放代理租约',
        'device_register_start':'设备注册线程领取 IP，开始注册', 'device_register_done':'设备注册成功入池',
        'device_register_failed':'设备注册失败', 'device_supplier_stopped':'设备注册线程停止',
        'device_register_retry':'同一设备身份更换故障租约，重试注册',
        'workers_ready':'设备达到启动水位，认证开始', 'account_dispatch':'账号领取设备和 IP，启动认证进程',
        'account_attempt_done':'本轮认证返回', 'account_retry':'同设备任务进入重试等待',
        'account_done':'账号处理结束', 'child_stopped':'停止并回收子进程',
        'batch_stop':'批次停止分发', 'batch_done':'批次完成',
    }

    def __init__(self, directory, *, debug=False, console=True):
        self.directory = private_directory(Path(directory))
        self.lock = threading.Lock()
        self.path = self.directory / 'events.jsonl'
        self.results_path = self.directory / 'results.jsonl'
        self.debug_enabled = debug
        self.console = console
        self.debug_dropped = 0
        self.debug_spans = {}
        if debug:
            descriptor = os.open(self.directory/'debug.jsonl', os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o600)
            os.close(descriptor)

    def __call__(self, event, **fields):
        # Call sites pass only row numbers, lease labels, class names and timings.
        # URLs, account names, passwords, device material and raw messages are excluded.
        forbidden = {'email', 'password', 'url', 'api_url', 'headers', 'identity', 'detail', 'apple_message'}
        if forbidden.intersection(fields):
            raise ValueError('Sensitive field rejected by event logger')
        stamp = datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(timespec='milliseconds')
        record = dict(fields, event=event, at=stamp, pid=os.getpid(), thread=threading.current_thread().name)
        with self.lock:
            descriptor = os.open(self.path, os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o600)
            with os.fdopen(descriptor, 'a', encoding='utf-8') as handle:
                handle.write(json.dumps(record, ensure_ascii=False, separators=(',', ':')) + '\n')
            if self.console:
                display = ' '.join(f'{key}={value}' for key, value in fields.items())
                print(f'[{stamp[11:23]}] {self.labels.get(event, event)} {display}', flush=True)

    def result(self, value):
        with self.lock:
            descriptor = os.open(self.results_path, os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o600)
            with os.fdopen(descriptor, 'a', encoding='utf-8') as handle:
                handle.write(json.dumps(value, ensure_ascii=False, separators=(',', ':')) + '\n')

    def debug(self, value):
        if not self.debug_enabled:
            return
        try:
            stamp = datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(timespec='milliseconds')
            with self.lock:
                path = self.directory / 'debug.jsonl'
                descriptor = os.open(path, os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o600)
                with os.fdopen(descriptor, 'a', encoding='utf-8') as handle:
                    handle.write(json.dumps(dict(value, logged_at=stamp), ensure_ascii=False,
                                            separators=(',', ':')) + '\n')
                if value.get('event') == 'phase_end' and isinstance(value.get('ms'), (int,float)):
                    key = (value.get('role','parent'), value.get('phase','unknown'), value.get('request_phase',''))
                    self.debug_spans.setdefault(key, []).append(value['ms'])
        except OSError:
            self.debug_dropped += 1

    def debug_summary(self):
        rows = []
        for (role, phase, request_phase), values in sorted(self.debug_spans.items()):
            values = sorted(values)
            rows.append({'role':role,'phase':phase,'request_phase':request_phase,'count':len(values),
                         'mean_ms':round(sum(values)/len(values),3),'max_ms':max(values),
                         'p95_ms':values[round((len(values)-1)*.95)]})
        return {'phases':rows,'log_write_failures':self.debug_dropped,
                'note':'Nested spans overlap; do not add all phase durations together.'}


def proxy_fingerprint(url):
    parsed = urlsplit(url)
    return hashlib.sha256(f'{parsed.scheme}|{parsed.hostname}|{parsed.port}'.encode()).hexdigest()[:12]


def _child_signals():
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def register_process(payload, output):
    _child_signals()
    started = time.perf_counter()
    diagnostic = SafeDiagnostics(output, role='registration', device=payload['slot'],
        proxy_fingerprint=proxy_fingerprint(payload['proxy'])) if payload.get('debug') else None
    if diagnostic:
        diagnostic({'event':'phase_end','phase':'process_bootstrap',
                    'ms':round((time.perf_counter()-payload.get('spawn_at_perf',time.perf_counter()))*1000,3)})
    try:
        kwargs = {'client_factory':TracedClient} if diagnostic else {}
        pool = DevicePool(payload['root'], payload['origin'], max_devices=10, **kwargs)
        slot = payload['slot']
        with file_lease(pool.slot_directory(slot) / '.lease.lock'):
            pool.allowed(slot)
            if diagnostic:
                pool.store(slot).load()
                device_id = pool.inspect(slot).get('device_id','')
                diagnostic.context['device_fingerprint'] = hashlib.sha256(device_id.encode()).hexdigest()[:12]
            info = pool.info(payload['prepare_timeout'], proxy=payload['proxy'], diagnostics=diagnostic,
                             provider_direct=payload.get('provider_direct',False))
            pool.fetch_headers(slot, info, payload['prepare_timeout'], registered=False, proxy=payload['proxy'],
                               diagnostics=diagnostic, provider_direct=payload.get('provider_direct',False))
        output.put({'ok':True, 'ms':round((time.perf_counter() - started) * 1000, 2),
                    'dropped_debug_events':diagnostic.dropped if diagnostic else 0})
    except Exception as exc:
        output.put({'ok':False, 'error_type':type(exc).__name__, 'http':getattr(exc, 'http', 0),
                    'apple_ec':getattr(exc, 'code', None), 'stage':getattr(exc, 'stage', 'registration')})


def authenticate_process(payload, output, stop):
    _child_signals()
    diagnostic = SafeDiagnostics(output, role='authentication', row=payload['row'], device=payload['slot'],
        attempt=payload['attempt'], proxy_fingerprint=proxy_fingerprint(payload['proxy']),
        device_fingerprint=payload.get('device_fingerprint')) if payload.get('debug') else None
    if diagnostic:
        diagnostic({'event':'phase_end','phase':'process_bootstrap',
                    'ms':round((time.perf_counter()-payload.get('spawn_at_perf',time.perf_counter()))*1000,3)})
    try:
        kwargs = {'client_factory':TracedClient} if diagnostic else {}
        pool = DevicePool(payload['root'], payload['origin'], max_devices=10, policy=RetryPolicy(attempts=1), **kwargs)
        result = pool.authenticate(payload['slot'], payload['email'], payload['password'],
            timeout=payload['timeout'], prepare_timeout=payload['prepare_timeout'],
            proxy=payload['proxy'], stop_event=stop, diagnostics=diagnostic,
            provider_direct=payload.get('provider_direct',False))
        safe = {key:result.get(key) for key in
            ('status','kind','stage','apple_ec','password_verified','two_factor_status','total_ms','attempt_history')}
        safe['dropped_debug_events'] = diagnostic.dropped if diagnostic else 0
        output.put(safe)
    except PoolError:
        output.put({'status':'deferred','kind':'pool','stage':'queue'})
    except Exception as exc:
        output.put({'status':'error','kind':'internal','stage':'worker','error_type':type(exc).__name__})
    finally:
        gsa.close_session()


@dataclass
class Device:
    slot: int
    busy: bool = False
    lease: object = field(default=None, repr=False)
    uses: int = 0


@dataclass(repr=False)
class Job:
    account: Account
    device: Device
    attempt: int = 0
    started: float = field(default_factory=time.monotonic)
    history: list = field(default_factory=list)


class BatchRunner:
    def __init__(self, accounts, api_url, root, output, *, ttl=60, proxy_auth='',
                 origin=None, timeout=15, prepare_timeout=40, attempts=2,
                 startup_timeout=150, proxy_fetcher=None, auth_target=authenticate_process,
                 registration_target=register_process, registration_gap=3, job_budget=45,
                 min_proxy_life=20, debug=False, provider_direct=True):
        self.accounts, self.root = accounts, str(root)
        self.origin = origin or next(u for u in gsa.ANISETTE_SERVERS if u.startswith('https://')).rstrip('/')
        self.timeout, self.prepare_timeout, self.attempts = timeout, prepare_timeout, attempts
        self.startup_timeout, self.registration_gap = startup_timeout, registration_gap
        self.job_budget, self.min_proxy_life = job_budget, min_proxy_life
        self.ctx = mp.get_context('spawn')
        self.stop = self.ctx.Event()
        self.debug = debug
        self.provider_direct = provider_direct
        self.log = EventLog(output, debug=debug)
        self.pool = DevicePool(root, self.origin, max_devices=10)
        self.stop_reason, self.reg_done = None, False
        self.condition = threading.Condition()
        self.devices = {}
        self.active, self.retries, self.results = {}, [], {}
        self.pending = deque(accounts)
        self.retry_serial = 0
        fetcher = proxy_fetcher or ApiFetcher(self.ctx, self.stop, proxy_auth,
                                            debug_log=self.log.debug if debug else None)
        self.proxies = ProxyPool(api_url, self.stop, self.log, ttl=ttl, target=10,
                                 fetcher=fetcher, on_limit=self.request_stop)
        self.auth_target, self.registration_target = auth_target, registration_target
        self.reg_thread = threading.Thread(target=self._register_devices, daemon=True, name='device-registrar')

    def request_stop(self, reason):
        with self.condition:
            if self.stop_reason is None:
                self.stop_reason = reason
                self.log('batch_stop', reason=reason)
            self.stop.set()
            self.condition.notify_all()
        self.proxies.wake()

    def _reap(self, process, output, *, terminate=False):
        started = time.perf_counter()
        child_pid = process.pid
        if terminate and process.is_alive():
            process.terminate()
        process.join(.5)
        if process.is_alive():
            process.kill()
            process.join(2)
        output.cancel_join_thread()
        output.close()
        process.close()
        self.log.debug({'event':'phase_end','phase':'parent_process_reap','child_pid':child_pid,
                        'ms':round((time.perf_counter()-started)*1000,3),'forced':terminate})

    def _register_devices(self):
        try:
            slot, registration_attempt = 1, 0
            while slot <= 10:
                if self.stop.is_set():
                    break
                with self.condition:
                    if slot in self.devices:
                        slot += 1
                        registration_attempt = 0
                        continue
                if self.pool.inspect(slot)['state'] == 'invalid':
                    self.log('device_register_failed', device=slot, error_type='InvalidSavedState')
                    break
                lease = None
                while not self.stop.is_set() and lease is None:
                    lease = self.proxies.take(timeout=1, min_remaining=max(25, self.min_proxy_life))
                if lease is None:
                    break
                registration_attempt += 1
                self.log('device_register_start', device=slot, proxy=lease.label,
                         proxy_fingerprint=proxy_fingerprint(lease.url), attempt=registration_attempt)
                output = self.ctx.Queue(maxsize=2048 if self.debug else 1)
                payload = {'root':self.root,'origin':self.origin,'slot':slot,'proxy':lease.url,
                           'prepare_timeout':self.prepare_timeout,'debug':self.debug,
                           'spawn_at_perf':time.perf_counter(),'provider_direct':self.provider_direct}
                process = self.ctx.Process(target=self.registration_target, args=(payload, output),
                                           name=f'device-register-{slot}')
                started_child, result = False, None
                try:
                    process.start()
                    started_child = True
                    deadline = min(time.monotonic() + self.job_budget, lease.expires_at - 2)
                    while not self.stop.is_set() and time.monotonic() < deadline:
                        try:
                            message = output.get(timeout=.1)
                            if '_debug' in message:
                                self.log.debug(message['_debug'])
                                continue
                            result = message
                            break
                        except queue.Empty:
                            if not process.is_alive():
                                try:
                                    message = output.get(timeout=.1)
                                    if '_debug' in message:
                                        self.log.debug(message['_debug'])
                                        continue
                                    result = message
                                except queue.Empty:
                                    pass
                                break
                finally:
                    if started_child:
                        self._reap(process, output, terminate=result is None)
                    else:
                        output.close()
                        process.close()
                if result and result.get('ok') and self.pool.inspect(slot)['state'] == 'ready':
                    with self.condition:
                        self.devices[slot] = Device(slot, lease=lease)
                        ready = len(self.devices)
                        self.condition.notify_all()
                    self.log('device_register_done', device=slot, proxy=lease.label,
                             ready_devices=ready, ms=result.get('ms', 0),
                             device_fingerprint=self._device_fingerprint(slot),
                             dropped_debug_events=result.get('dropped_debug_events',0))
                    slot += 1
                    registration_attempt = 0
                else:
                    self.proxies.release(lease, valid=False)
                    if not self.stop.is_set():
                        self.log('device_register_failed', device=slot,
                                 error_type=(result or {}).get('error_type', 'RegistrationTimeout'),
                                 http=(result or {}).get('http', 0),
                                 apple_ec=(result or {}).get('apple_ec'),
                                 stage=(result or {}).get('stage', 'registration'))
                        if (result or {}).get('http') == 429:
                            self.request_stop('registration_ratelimit')
                        transient = (result or {}).get('error_type') in (
                            'ReadTimeout','ConnectTimeout','ConnectionError','ProxyError','SSLError',
                            'TimeoutError','RegistrationTimeout') or (result or {}).get('http',0) >= 500
                        if transient and registration_attempt < 2 and not self.stop.is_set():
                            self.log('device_register_retry',device=slot,next_attempt=registration_attempt+1)
                            if self.stop.wait(2):
                                break
                            continue
                    break
                if self.stop.wait(self.registration_gap):
                    break
        except Exception as exc:
            self.log('device_register_failed', error_type=type(exc).__name__)
        finally:
            with self.condition:
                self.reg_done = True
                self.condition.notify_all()
            self.log('device_supplier_stopped')

    def _choose_device(self):
        with self.condition:
            # Iteration order gives the first released identities immediate reuse;
            # registrations need not finish before account 3 can start.
            for device in self.devices.values():
                if device.busy:
                    continue
                try:
                    self.pool.allowed(device.slot)
                except PoolError:
                    continue
                device.busy = True
                return device
        return None

    def _lease_for(self, device):
        lease = device.lease
        if lease is not None and lease.expires_at - time.monotonic() < self.min_proxy_life:
            self.proxies.release(lease, valid=False)
            device.lease = None
        if device.lease is None:
            device.lease = self.proxies.take(min_remaining=self.min_proxy_life)
        return device.lease

    def _device_fingerprint(self, slot):
        identifier = self.pool.inspect(slot).get('device_id','')
        return hashlib.sha256(identifier.encode()).hexdigest()[:12]

    def _dispatch(self, job):
        lease = self._lease_for(job.device)
        if lease is None:
            return False
        job.attempt += 1
        reused = job.device.uses > 0
        if job.attempt == 1:
            job.device.uses += 1
        output = self.ctx.Queue(maxsize=2048 if self.debug else 1)
        payload = {'root':self.root,'origin':self.origin,'slot':job.device.slot,
                   'proxy':lease.url,'email':job.account.email,'password':job.account.password,
                   'timeout':self.timeout,'prepare_timeout':self.prepare_timeout,
                   'row':job.account.row,'attempt':job.attempt,'debug':self.debug,
                   'device_fingerprint':self._device_fingerprint(job.device.slot),
                   'spawn_at_perf':time.perf_counter(),'provider_direct':self.provider_direct}
        process = self.ctx.Process(target=self.auth_target, args=(payload, output, self.stop),
                                   name=f'auth-row-{job.account.row}-try-{job.attempt}')
        process.start()
        deadline = min(time.monotonic() + self.job_budget, lease.expires_at - 2)
        self.active[job.account.row] = (job, process, output, deadline)
        self.log('account_dispatch', row=job.account.row, device=job.device.slot, proxy=lease.label,
                 attempt=job.attempt, worker_pid=process.pid, active_auth=len(self.active),
                 ready_devices=len(self.devices), device_reused=reused,
                 ip_remaining_seconds=round(lease.expires_at - time.monotonic(), 1),
                 proxy_fingerprint=proxy_fingerprint(lease.url),
                 device_fingerprint=payload['device_fingerprint'],
                 account_wait_ms=round((time.monotonic()-job.account.queued_at)*1000,2))
        return True

    def _finish_job(self, job, result):
        selected = {key:result.get(key) for key in ('status','kind','stage','apple_ec','password_verified')}
        selected.update(row=job.account.row, device=job.device.slot, attempts=job.attempt,
                        elapsed_ms=round((time.monotonic() - job.started) * 1000, 2), history=job.history)
        self.results[job.account.row] = selected
        self.log.result(selected)
        self.log('account_done', row=job.account.row, device=job.device.slot,
                 status=result.get('status'), attempts=job.attempt)
        with self.condition:
            job.device.busy = False
            self.condition.notify_all()
        job.account.password = ''

    def _handle(self, job, result):
        result = dict(result)
        sample = {key:result.get(key) for key in
                  ('status','kind','stage','apple_ec','total_ms','attempt_history','dropped_debug_events')}
        sample.update(attempt=job.attempt, proxy=job.device.lease.label if job.device.lease else None)
        job.history.append(sample)
        self.log('account_attempt_done', row=job.account.row, device=job.device.slot,
                 attempt=job.attempt, status=result.get('status'), stage=result.get('stage'),
                 apple_ec=result.get('apple_ec'), ms=result.get('total_ms', 0))
        if result.get('kind') == 'ratelimit':
            self._finish_job(job, result)
            self.request_stop('authentication_ratelimit')
            return
        health = self.pool.health()
        if health.get('provider_until', 0) > time.time():
            self._finish_job(job, result)
            self.request_stop('provider_circuit_open')
            return
        refused = result.get('stage') == 'init' and result.get('apple_ec') == -20970
        transient = result.get('status') == 'error' and result.get('kind') in ('network','tls','server','timeout')
        can_retry = (refused or transient) and job.attempt < self.attempts and not self.stop.is_set()
        try:
            self.pool.allowed(job.device.slot)
        except PoolError:
            can_retry = False
        if can_retry:
            # Protocol rejection keeps its valid exit; broken transport drops it.
            if transient:
                self.proxies.release(job.device.lease, valid=False)
                job.device.lease = None
            delay = 2 ** job.attempt + random.uniform(0, .5)
            self.retry_serial += 1
            heapq.heappush(self.retries, (time.monotonic() + delay, self.retry_serial, job))
            self.log('account_retry', row=job.account.row, device=job.device.slot,
                     next_attempt=job.attempt + 1, delay_seconds=delay)
        else:
            self._finish_job(job, result)

    def _collect(self):
        for row, (job, process, output, deadline) in list(self.active.items()):
            forced = False
            result = None
            for _ in range(512):
                try:
                    message = output.get_nowait()
                except queue.Empty:
                    break
                if '_debug' in message:
                    self.log.debug(message['_debug'])
                    continue
                result = message
                break
            if result is None:
                if process.is_alive() and time.monotonic() < deadline:
                    continue
                if not process.is_alive():
                    try:
                        message = output.get(timeout=.1)
                        if '_debug' in message:
                            self.log.debug(message['_debug'])
                            continue
                        result = message
                        self._reap(process, output)
                        del self.active[row]
                        self._handle(job, result)
                        continue
                    except queue.Empty:
                        pass
                # Stop the old process before publishing an unknown outcome or
                # scheduling another attempt; no two attempts may overlap.
                timed_out = time.monotonic() >= deadline
                self.log('child_stopped', row=row, worker_pid=process.pid,
                         reason='deadline' if timed_out else 'worker_exit')
                result = {'status':'error','kind':'timeout' if timed_out else 'internal','stage':'worker'}
                forced = True
            self._reap(process, output, terminate=forced)
            del self.active[row]
            self._handle(job, result)

    def run(self):
        started, last_progress = time.monotonic(), time.monotonic()
        for account in self.accounts:
            account.queued_at = started
        for slot in range(1, 11):
            if self.pool.inspect(slot)['state'] == 'ready':
                self.devices[slot] = Device(slot)
        self.log('batch_start', accounts=len(self.accounts), concurrency=2, device_capacity=10,
                 attempts=self.attempts, proxy_ttl_seconds=self.proxies.ttl, debug=self.debug,
                 provider_route='direct' if self.provider_direct else 'leased_proxy', apple_route='leased_proxy')
        self.log('devices_restored', count=len(self.devices))
        if self.pool.health().get('provider_until',0) > time.time():
            self.request_stop('provider_circuit_open')
        if not self.stop.is_set():
            self.proxies.start()
            self.reg_thread.start()
        gate = False
        try:
            while (self.pending or self.active or self.retries) and not self.stop.is_set():
                self._collect()
                if self.stop.is_set():
                    break
                with self.condition:
                    ready = len(self.devices)
                    reg_done = self.reg_done
                if not gate:
                    if ready >= 2:
                        gate = True
                        self.log('workers_ready', ready_devices=ready, concurrency=2)
                    elif reg_done or time.monotonic() - started > self.startup_timeout:
                        self.request_stop('device_warmup_failed')
                        break
                    else:
                        self.stop.wait(.05)
                        continue
                with self.condition:
                    for device in self.devices.values():
                        if not device.busy and device.lease and device.lease.expires_at <= time.monotonic():
                            self.proxies.release(device.lease, valid=False)
                            device.lease = None
                dispatched = False
                while len(self.active) < 2 and not self.stop.is_set():
                    retry = bool(self.retries and self.retries[0][0] <= time.monotonic())
                    if retry:
                        _, serial, job = heapq.heappop(self.retries)
                    elif self.pending:
                        device = self._choose_device()
                        if device is None:
                            break
                        job = Job(self.pending.popleft(), device)
                    else:
                        break
                    if not self._dispatch(job):
                        self.retry_serial += 1
                        heapq.heappush(self.retries, (time.monotonic() + .2, self.retry_serial, job))
                        break
                    dispatched = True
                    last_progress = time.monotonic()
                if not self.active and time.monotonic() - last_progress > self.startup_timeout:
                    self.request_stop('resource_wait_timeout')
                    break
                if not dispatched:
                    self.stop.wait(.05)
        except KeyboardInterrupt:
            self.request_stop('user_cancelled')
        finally:
            self.stop.set()
            self.proxies.wake()
            for row, (job, process, output, _) in list(self.active.items()):
                self._reap(process, output, terminate=True)
                job.history.append({'attempt':job.attempt,'status':'stopped','kind':'cancelled','stage':'worker'})
                self._finish_job(job, {'status':'stopped','kind':'cancelled','stage':'worker'})
                del self.active[row]
            while self.retries:
                _, _, job = heapq.heappop(self.retries)
                if job.account.row in self.results:
                    continue
                # A dispatched account waiting for retry has already started.
                # Preserve that attempt history instead of labeling it untouched.
                self._finish_job(job, {'status':'stopped' if job.attempt else 'not_started',
                    'kind':'cancelled','stage':'backoff' if job.attempt else 'queue'})
            if self.reg_thread.ident is not None:
                self.reg_thread.join(5)
            if self.proxies.thread.ident is not None:
                self.proxies.thread.join(16)
            for account in self.accounts:
                if account.row not in self.results:
                    value = {'row':account.row,'status':'not_started','reason':self.stop_reason or 'stopped'}
                    self.results[account.row] = value
                    self.log.result(value)
                account.password = ''
            counts = dict(Counter(item['status'] for item in self.results.values()))
            summary = {'accounts':len(self.accounts),'concurrency':2,'device_capacity':10,
                       'registered_devices':sum(self.pool.inspect(n)['state']=='ready' for n in range(1,11)),
                       'results':counts,'stop_reason':self.stop_reason,
                       'elapsed_seconds':round(time.monotonic() - started, 2),
                       'debug':self.debug,'debug_log_write_failures':self.log.debug_dropped}
            save_json(self.log.directory / 'summary.json', summary)
            if self.debug:
                save_json(self.log.directory / 'debug-summary.json', self.log.debug_summary())
            self.log('batch_done', **summary)
        return summary


def run_account_batch(accounts, api_url, root, output, *, debug=False, proxy_auth=''):
    """One task queue, two auth slots, up to ten reusable devices."""
    root = Path(root)
    print(f'开始单队列测试，共 {len(accounts)} 个账号任务，并发 2。', flush=True)
    runner = BatchRunner(accounts, api_url, root/'config/batch-device-pool', output,
                         ttl=60, debug=debug, proxy_auth=proxy_auth)
    return runner.run()


def main():
    try:
        _dependencies()
        if gsa.mock_enabled():
            raise ValueError('请清除 FASTCHECK_MOCK，批量入口不会运行模拟认证。')
        if not sys.stdin.isatty():
            raise ValueError('请在本机终端交互运行。')
        path = input('账号文件路径（邮箱----密码，每行一个，最多 20 个；同一任务队列）：').strip().strip('"\'')
        accounts = read_accounts(path, max_accounts=20)
        debug = input('是否开启调试模式？[y/N]：').strip().lower() in ('y','yes','是','开启')
        with warnings.catch_warnings():
            warnings.simplefilter('error', getpass.GetPassWarning)
            api_url = getpass.getpass('代理 API 地址（隐藏输入，不保存）：').strip()
            proxy_auth = getpass.getpass('代理连接认证 user:pass（没有则回车，勿填 API 密钥）：').strip()
        root = Path(__file__).resolve().parent
        stamp = datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y%m%d-%H%M%S')
        output = root / 'logs' / f'batch-{stamp}-{os.getpid()}'
        print('配置：并发 2，代理库存目标 10，IP 有效期 60 秒，设备池上限 10，2 台就绪后开跑。')
        print('路由：公共配置/参数服务直连；Apple 设备注册及账号认证始终使用租约代理。')
        print('密码保留分隔符后的原始字符；结果用文件内的账号序号关联。')
        summary = run_account_batch(accounts, api_url, root, output, debug=debug, proxy_auth=proxy_auth)
        api_url = proxy_auth = ''
        print(f'日志目录：{output}')
        print('结果：' + json.dumps(summary['results'], ensure_ascii=False))
        return 0 if not summary['stop_reason'] else 1
    except (KeyboardInterrupt, EOFError):
        print('\n已取消。')
        return 130
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except Exception as exc:
        print(f'批次未完成：{type(exc).__name__}。', file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
