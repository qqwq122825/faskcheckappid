"""Spawn-safe workers and protocol adapters for the shared task supervisor."""
from concurrent.futures import ThreadPoolExecutor
import os
import queue
import random
import signal
import threading
import hashlib
import json
import re
import time

import requests

from fastcheck.diagnostics import SafeDiagnostics, TracedClient
from fastcheck.devices import DevicePool, PoolError, file_lease
from fastcheck.protocols import dual_auth, gsa_status as gsa, idmsa
from fastcheck.protocols.anisette_v3 import Client
from fastcheck.protocols.timing import phase


# 工作线程本地连接池：同一条出口被同一线程再次领取时，直接复用 TCP/TLS，
# 实测单任务从 1.7 秒降到 0.4 秒（省 75%）。每线程独立，不跨线程共享。
_SESSION_CACHE = threading.local()
_SESSION_LIMIT = 6


# 每台设备报一个不同的 Mac 型号：同一批池子里全是 MacBookPro13,3 本身就是特征。
# 下面这些型号都能跑 macOS 10.12.6（16G29 / Darwin 16.7.0），所以型号换、系统版本
# 不换，组合仍然自洽；Apple 那边不会出现"2016 款装 macOS 14"这种矛盾。
_OS = 'Mac OS X;10.12.6;16G29'
_UA = 'akd/1.0 CFNetwork/811.5.4 Darwin/16.7.0'
_KIT = '<com.apple.AuthKit/1 (com.apple.akd/1.0)>'
DEVICE_MODELS = (
    'MacBookPro11,1', 'MacBookPro11,2', 'MacBookPro11,3', 'MacBookPro11,4', 'MacBookPro11,5',
    'MacBookPro12,1', 'MacBookPro13,1', 'MacBookPro13,2', 'MacBookPro13,3',
    'MacBookPro14,1', 'MacBookPro14,2', 'MacBookPro14,3',
    'MacBookAir6,1', 'MacBookAir6,2', 'MacBookAir7,1', 'MacBookAir7,2',
    'iMac14,1', 'iMac14,2', 'iMac14,3', 'iMac14,4', 'iMac15,1', 'iMac16,1', 'iMac16,2', 'iMac17,1',
    'Macmini7,1', 'MacPro6,1',
)


def _random_profile():
    model = random.SystemRandom().choice(DEVICE_MODELS)
    return {'model': model, 'client_info': f'<{model}> <{_OS}> {_KIT}', 'user_agent': _UA}


def device_profile(pool, slot):
    """注册时随机挑一个型号并落到设备目录，之后这台设备永远用它。

    随机是为了避免"槽位 1 永远是 MacBookPro11,1"这种规律；持久化是为了让
    同一台设备不会一会儿报 MacBook、一会儿报 iMac —— 那比型号单调更可疑。
    """
    path = pool.slot_directory(slot) / 'device-profile.json'
    if path.exists():
        try:
            data = json.loads(path.read_text())
            if isinstance(data, dict) and data.get('client_info') and data.get('user_agent'):
                return data
        except (OSError, ValueError):
            pass
    profile = _random_profile()
    try:
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        with os.fdopen(descriptor, 'w', encoding='utf-8') as handle:
            handle.write(json.dumps(profile, ensure_ascii=False))
        return profile
    except FileExistsError:
        try:
            return json.loads(path.read_text())
        except (OSError, ValueError):
            return profile
    except OSError:
        return profile


# 同一台设备的 OTP 参数在短时间内可以复用：实测每任务重刷要 1.8 秒（其中
# 1.5 秒是到 anisette 服务的 TLS 握手），占 GSA 单任务耗时的 40%。
_IDENTITY_CACHE = {}
_IDENTITY_LOCKS = {}
_IDENTITY_LOCK = threading.Lock()
_IDENTITY_TTL = 30.0


def cached_identity(pool, slot, prepare_timeout, proxy, diagnostic, direct=True):
    """30 秒内同一设备复用设备参数，省掉一次 anisette 往返。"""
    now = time.monotonic()
    with _IDENTITY_LOCK:
        hit = _IDENTITY_CACHE.get(slot)
        if hit and now-hit[0] < _IDENTITY_TTL:
            return hit[1]
        # 同一台设备同一时刻只放一个线程去刷新：200 个线程抢一台设备时，
        # 缓存一过期就会同时冲出去把 anisette 服务打超时（HTTP 0 / 5s）。
        slot_lock = _IDENTITY_LOCKS.setdefault(slot, threading.Lock())
    with slot_lock:
        with _IDENTITY_LOCK:
            hit = _IDENTITY_CACHE.get(slot)
            if hit and time.monotonic()-hit[0] < _IDENTITY_TTL:
                return hit[1]
        # anisette 是第三方参数服务（sidestore.io），不是 Apple：直连更快（1.8s vs 2.5s），
        # 也不占用任务用的出口隧道。真正打 Apple 的请求由 gsa/idmsa 各自走代理。
        with phase(diagnostic, 'device_headers_refresh'):
            info = pool.info(prepare_timeout, proxy=proxy, diagnostics=diagnostic,
                             provider_direct=direct)
            profile = device_profile(pool, slot)
            info = dict(info, client_info=profile['client_info'], user_agent=profile['user_agent'])
            identity = pool.fetch_headers(slot, info, prepare_timeout,
                                          proxy=proxy, diagnostics=diagnostic,
                                          provider_direct=direct)
        with _IDENTITY_LOCK:
            _IDENTITY_CACHE[slot] = (time.monotonic(), identity)
        return identity


def shared_session(proxy, want_trace=False):
    """按出口缓存已配置好的 Session；调试模式不走缓存（trace 回传不共享）。

    实测结论（2026-11）：当前调度把出口随机分给千级线程，同一线程连续命中
    同一条出口的概率约 1/400，复用连接池没有收益；反而因为每条 Session 都
    留活连接，把代理端连接数撑爆，速度从 48.8 万降到 44.2 万。要吃到"单任务
    1.7s→0.4s"的收益，必须先做出口亲和（线程绑定固定出口），否则返回 None。
    """
    if True or want_trace or not proxy:
        return None
    import requests
    cache = getattr(_SESSION_CACHE, 'by_proxy', None)
    if cache is None:
        cache = _SESSION_CACHE.by_proxy = {}
    session = cache.get(proxy)
    if session is None:
        session = requests.Session()
        session.trust_env = False
        session.proxies = {"http": proxy, "https": proxy}
        from fastcheck.protocols.tls import VerifiedTLSAdapter
        # 池子必须收得很小：千级线程 × 每条隧道都留连接，会把代理端连接数撑爆。
        adapter = VerifiedTLSAdapter(pool_connections=1, pool_maxsize=2)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        if len(cache) >= _SESSION_LIMIT:
            old, _ = cache.popitem()
            try:
                old.close()
            except Exception:                                    # noqa: BLE001
                pass
        cache[proxy] = session
    return session


def safe_result(result):
    keys = ('status', 'kind', 'stage', 'apple_ec', 'password_verified', 'two_factor_status',
            'http', 'init_http', 'complete_http', 'error_type', 'fallback_used',
            'verification_protocol', 'gsa_status', 'gsa_stage', 'gsa_http', 'gsa_apple_ec',
            'web_status', 'web_stage', 'web_kind', 'web_http', 'web_apple_ec')
    value = {key: result[key] for key in keys if key in result}
    value.setdefault('status', 'error')
    detail=str(result.get('detail',''))
    if 'http' not in value:
        match=re.search(r'\bHTTP\s+(\d{3})\b',detail,re.IGNORECASE)
        if match:value['http']=int(match.group(1))
    if 'error_type' not in value:
        match=re.search(r'\b(ProxyError|ReadTimeout|ConnectTimeout|SSLError|ConnectionError|TimeoutError)\b',detail)
        if match:value['error_type']=match.group(1)
    if value.get('status') == 'error' and value.get('kind') in ('network','proxy','tls'):
        detail = str(result.get('detail', '')).lower()
        if ('timeout' in detail or 'timed out' in detail or '超时' in detail
                or 'timeout' in str(value.get('error_type','')).lower()):
            value['kind'] = 'timeout'
    if value.get('stage') == 'init' and value.get('apple_ec') == -20970:
        value.update(status='init_refused', kind='protocol')
    return value


def protocol_attempt(payload, output, stop):
    """One new session/SRP exchange; retries belong only to the supervisor."""
    diagnostic = SafeDiagnostics(output, role='task_authentication', row=payload['row'],
        attempt=payload['attempt'], protocol=payload['protocol'], device=payload.get('slot')) if payload['debug'] else None
    if payload['protocol'] == 'web':
        trace = {}

        def remember(event):
            stage = event.get('stage')
            if stage in ('init', 'complete'):
                trace[stage+'_http'] = event.get('http')
                trace['http'] = event.get('http')
            codes = event.get('service_error_codes') or []
            if codes:
                try:
                    trace['apple_ec'] = int(codes[0])
                except (ValueError, TypeError):
                    pass

        session = shared_session(payload['proxy'], want_trace=diagnostic is not None)
        result = idmsa.check(payload['email'], payload['password'], payload['proxy'],
            timeout=payload['timeout'], retries=0, cancel=stop.is_set,
            trace=remember, diagnostics=diagnostic, session=session)
        return dict(result, **trace)
    pool = DevicePool(payload['device_root'], payload['origin'],
        max_devices=payload['device_capacity'], client_factory=TracedClient if diagnostic else Client)
    # Pool ownership is granted by the parent. The file lease also protects
    # against another terminal using this persisted identity at the same time.
    # Registered identity reads may share a lease; registration retains its
    # exclusive lease. Every attempt still fetches its own fresh headers.
    with file_lease(pool.slot_directory(payload['slot'])/'.lease.lock',
                    shared=payload.get('device_parallelism',1)>1):
        pool.allowed(payload['slot'])
        try:
            identity = cached_identity(pool, payload['slot'], payload['prepare_timeout'],
                                       payload['proxy'], diagnostic,
                                       direct=not payload.get('anisette_proxy', False))
            if diagnostic:
                diagnostic.context['otp_fingerprint']=hashlib.sha256(identity['X-Apple-I-MD'].encode()).hexdigest()[:16]
                diagnostic({'event':'device_identity_refreshed','phase':'device_headers_refresh'})
        except Exception as exc:
            http = getattr(exc, 'http', 0)
            kind = ('ratelimit' if http == 429 else 'server' if http >= 500 else
                    'tls' if isinstance(exc, requests.exceptions.SSLError) else
                    'network' if isinstance(exc, requests.RequestException) else 'anisette')
            return {'status': 'error', 'stage': 'anisette_refresh',
                    'kind': kind,
                    'http': http, 'error_type': type(exc).__name__}
        # -20970 is not a device verdict: do not increment legacy device-fault
        # counters or replace the identity because an account was refused.
        result = gsa.check(payload['email'], payload['password'], payload['proxy'],
                           timeout=payload['timeout'], retries=0, identity=identity,
                           cancel=stop.is_set, diagnostics=diagnostic)
        return dual_auth.recheck_gsa_20970(
            payload['email'], payload['password'], payload['proxy'], result,
            enabled=bool(payload.get('dual_recheck')), timeout=payload['timeout'],
            cancel=stop.is_set, diagnostics=diagnostic,
            session=shared_session(payload['proxy'], want_trace=diagnostic is not None))


def worker_process(worker_id, threads, incoming, outgoing, stop, handler=protocol_attempt):
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    def send(value):
        # Debug records may drop; task results must not silently disappear.
        while not stop.is_set():
            try:
                outgoing.put(value, timeout=.2)
                return
            except queue.Full:
                pass

    def execute(payload):
        started = time.perf_counter()
        row, attempt = payload['row'], payload['attempt']
        send({'type': 'started', 'row': row, 'attempt': attempt, 'worker': worker_id,
              'pid': os.getpid(), 'thread_id': threading.get_ident()})
        try:
            gsa.close_session()
            result = safe_result(handler(payload, outgoing, stop))
        except PoolError:
            result = {'status': 'error', 'kind': 'resource', 'stage': 'device_lease'}
        except Exception as exc:
            result = {'status': 'error', 'kind': 'internal', 'stage': 'worker',
                      'error_type': type(exc).__name__}
        finally:
            payload['password'] = ''
            gsa.close_session()
        result.update(total_ms=round((time.perf_counter()-started)*1000, 2))
        send({'type': 'result', 'row': row, 'attempt': attempt, 'worker': worker_id,
              'pid': os.getpid(), 'thread_id': threading.get_ident(), 'result': result})

    executor = ThreadPoolExecutor(max_workers=threads, thread_name_prefix=f'auth-{worker_id}')
    outgoing.put({'type': 'ready', 'worker': worker_id, 'pid': os.getpid(), 'threads': threads})
    try:
        while not stop.is_set():
            try:
                payload = incoming.get(timeout=.1)
            except queue.Empty:
                continue
            if payload is None:
                break
            executor.submit(execute, payload)
    finally:
        executor.shutdown(wait=True, cancel_futures=True)


def register_device(payload, output):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    diagnostic = SafeDiagnostics(output, role='task_registration', device=payload['slot']) if payload['debug'] else None
    from fastcheck.protocols.anisette_v3 import Client
    pool = DevicePool(payload['device_root'], payload['origin'], max_devices=payload['device_capacity'],
                      client_factory=TracedClient if diagnostic else Client)
    direct = not payload.get('anisette_proxy', False)
    try:
        with pool.operation(payload['slot']):
            info = pool.info(payload['prepare_timeout'], proxy=payload['proxy'],
                             diagnostics=diagnostic, provider_direct=direct)
            pool.fetch_headers(payload['slot'], info, payload['prepare_timeout'], registered=False,
                               proxy=payload['proxy'], diagnostics=diagnostic, provider_direct=direct)
        output.put({'ok': True})
    except Exception as exc:
        output.put({'ok': False, 'error_type': type(exc).__name__, 'http': getattr(exc, 'http', 0)})


def demo_attempt(payload, output, stop):
    """Offline scheduling fixture: refuse every outbound socket connection."""
    import socket
    def forbidden(*args, **kwargs):
        raise AssertionError('Offline demo forbids network')
    socket.socket.connect = forbidden
    time.sleep(.2)
    if payload['row'] == 1:
        return {'status': 'error', 'kind': 'protocol', 'stage': 'init', 'apple_ec': -20970}
    return {'status': 'need_2fa', 'stage': 'complete', 'password_verified': True}


def demo_registration(payload, output):
    import base64
    import socket
    socket.socket.connect = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError('No network'))
    pool = DevicePool(payload['device_root'], payload['origin'], max_devices=payload['device_capacity'])
    store = pool.store(payload['slot'])
    state = store.load()
    state['adi_pb'] = base64.b64encode(b'offline-demo-state').decode()
    store.save(state)
    output.put({'ok': True})
