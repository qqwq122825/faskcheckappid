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
PROXY_MICROBATCH_WINDOW_SECONDS = 0.2
PROXY_MICROBATCH_MAX_SIZE = 1000
MIN_STALL_TIMEOUT_SECONDS = 180.0
PROXY_DEBUG_ENV = 'CLUSTER_PROXY_DEBUG'
PROXY_EXIT_IP_URL = 'https://api.ipify.org?format=json'
PROXY_EXIT_IP_TIMEOUT = 4.0
PROXY_EXIT_IP_MAX = 2048


class _OnDemandProxyBatcher:
    """Merge only currently waiting proxy requests into short API batches.

    This deliberately is not a proxy pool: returned endpoints are handed
    directly to the waiters in the current batch. Nothing is retained for a
    later request, and there is no TTL, low-water mark, recycle or refill
    state.
    """

    def __init__(self, fetch, window=PROXY_MICROBATCH_WINDOW_SECONDS,
                 max_batch=PROXY_MICROBATCH_MAX_SIZE):
        self._fetch = fetch
        self._window = max(0.0, float(window))
        self._max_batch = max(1, int(max_batch))
        self._condition = threading.Condition()
        self._pending = deque()
        self._coordinator_running = False

    def request(self, stop=None):
        request = {'event': threading.Event(), 'proxy': None, 'cancelled': False}
        with self._condition:
            self._pending.append(request)
            self._condition.notify_all()
            if not self._coordinator_running:
                self._coordinator_running = True
                threading.Thread(target=self._run, name='proxy-microbatch',
                                 daemon=True).start()
        while not request['event'].wait(0.1):
            if stop is not None and stop.is_set():
                request['cancelled'] = True
                return None
        return request['proxy']

    def _run(self):
        while True:
            with self._condition:
                if not self._pending:
                    self._coordinator_running = False
                    return
                deadline = time.monotonic() + self._window
                while len(self._pending) < self._max_batch:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    self._condition.wait(remaining)
                batch = [self._pending.popleft()
                         for _ in range(min(self._max_batch, len(self._pending)))]
            try:
                values = list(self._fetch(len(batch)) or ())
            except Exception:                                      # noqa: BLE001
                values = []
            # Assign the exact provider-returned endpoint to one waiter only.
            # Extra provider values are intentionally discarded, not stocked.
            for index, request in enumerate(batch):
                if not request['cancelled'] and index < len(values):
                    request['proxy'] = values[index]
                request['event'].set()


class _CrossProcessProxyRequester:
    """Let worker threads wait on a node-level microbatch broker."""

    def __init__(self, request_queue, response_queue, client_id):
        self._request_queue = request_queue
        self._response_queue = response_queue
        self._client_id = int(client_id)
        self._lock = threading.Lock()
        self._next_id = 0
        self._pending = {}
        threading.Thread(target=self._listen, name='proxy-microbatch-listener',
                         daemon=True).start()

    def request(self, stop=None):
        with self._lock:
            request_id = self._next_id
            self._next_id += 1
            request = {'event': threading.Event(), 'proxy': None}
            self._pending[request_id] = request
        self._request_queue.put((self._client_id, request_id))
        while not request['event'].wait(0.1):
            if stop is not None and stop.is_set():
                with self._lock:
                    self._pending.pop(request_id, None)
                return None
        return request['proxy']

    def _listen(self):
        while True:
            response = self._response_queue.get()
            if response is None:
                return
            request_id, proxy = response
            with self._lock:
                request = self._pending.pop(int(request_id), None)
            if request is not None:
                request['proxy'] = proxy
                request['event'].set()


class _NodeProxyMicrobatchBroker:
    """Batch current proxy demand across every auth process on one node."""

    def __init__(self, request_queue, response_queues, fetch,
                 window=PROXY_MICROBATCH_WINDOW_SECONDS,
                 max_batch=PROXY_MICROBATCH_MAX_SIZE):
        self._request_queue = request_queue
        self._response_queues = response_queues
        self._fetch = fetch
        self._window = max(0.0, float(window))
        self._max_batch = max(1, int(max_batch))
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run,
                                        name='node-proxy-microbatch', daemon=True)

    def start(self):
        self._thread.start()

    def close(self):
        self._stop.set()
        self._request_queue.put(None)
        self._thread.join(2)
        for response_queue in self._response_queues:
            response_queue.put(None)

    def _run(self):
        while not self._stop.is_set():
            try:
                first = self._request_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            if first is None:
                return
            batch = [first]
            deadline = time.monotonic() + self._window
            while len(batch) < self._max_batch:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    item = self._request_queue.get(timeout=remaining)
                except queue.Empty:
                    break
                if item is None:
                    self._stop.set()
                    break
                batch.append(item)
            try:
                values = list(self._fetch(len(batch)) or ())
            except Exception:                                      # noqa: BLE001
                values = []
            for index, (client_id, request_id) in enumerate(batch):
                proxy = values[index] if index < len(values) else None
                self._response_queues[int(client_id)].put((request_id, proxy))


def stall_timeout_seconds(descriptor, minimum=MIN_STALL_TIMEOUT_SECONDS,
                          padding=60.0):
    """Return a no-progress budget larger than every configured retry."""
    timeout = max(0.1, float(descriptor.get('timeout') or 20))
    attempts = max(0, int(descriptor.get('retries') or 0)) + 1
    return max(float(minimum), timeout * attempts + float(padding))


def _partition_device_slots(slot_ids, process_count, process_index):
    """Give every process a disjoint share of the full device pool."""
    slots = [int(value) for value in slot_ids]
    count = max(1, int(process_count))
    index = max(0, int(process_index))
    return slots[index::count] or [index + 1]


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


def _group_child(directory, descriptor, stop, proxy_pool=None,
                 proxy_microbatch_channels=None):
    # 认证子进程属于吞吐型后台工作。降低它相对节点主进程的调度优先级，
    # 避免 SRP/PBKDF2/TLS 高负载时心跳线程拿不到 CPU 而误丢租约。
    worker_nice = 0
    if mp.parent_process() is not None:
        try:
            worker_nice = os.nice(5)
        except (AttributeError, OSError):
            worker_nice = 0
    descriptor['_worker_nice'] = worker_nice
    if proxy_pool is not None:
        # Proxy stock is expendable; result files are independently flushed.
        # A child must not wait at exit for its reusable-queue feeder to drain
        # into a pipe nobody reads while the coordinator waits for this child.
        for channel in (proxy_pool.fresh, proxy_pool.reusable):
            channel.cancel_join_thread()
    runner = execute_auth_unit if descriptor.get('mode') == 'auth' else execute_unit
    if runner is execute_auth_unit:
        proxy_requester = None
        if proxy_microbatch_channels is not None:
            request_queue, response_queue, client_id = proxy_microbatch_channels
            proxy_requester = _CrossProcessProxyRequester(
                request_queue, response_queue, client_id)
        runner(directory, descriptor, stop, proxy_pool=proxy_pool,
               proxy_requester=proxy_requester)
    else:
        runner(directory, descriptor, stop)


def _auth_worker_limit(configured, configured_processes=1, active_processes=None):
    """Preserve total network concurrency across CPU-sized auth processes."""
    configured = max(1, int(configured or 1))
    configured_processes = max(1, int(configured_processes or 1))
    active_processes = max(1, int(active_processes or configured_processes))
    total_concurrency = configured_processes * configured
    return (total_concurrency + active_processes - 1) // active_processes


def _auth_process_limit(configured, rows, cpu_count=None):
    """Use one GMP compute lane per logical CPU."""
    configured = max(1, int(configured or 1))
    rows = max(1, int(rows or 1))
    logical_cpus = max(1, int(cpu_count or os.cpu_count() or 1))
    return min(configured, rows, logical_cpus)


def execute_unit_group(directory, descriptor, stop):
    """Use one controller package as a complete node-sized work batch.

    The controller leases at most one package to a node.  This coordinator
    splits that package locally into contiguous ranges, starts the configured
    process count, aggregates their progress, and merges ordered results.  The
    controller therefore keeps only 10,000-row package metadata while a node
    still uses all of its process and thread capacity.
    """
    directory = Path(directory)
    descriptor = dict(descriptor)
    debug_path = None
    if _proxy_debug_enabled(descriptor):
        debug_path = directory / 'proxy-debug.jsonl'
        descriptor['_proxy_debug'] = True
        descriptor['_proxy_debug_path'] = str(debug_path)
        _write_proxy_debug(
            debug_path, 'debug_start', job_id=str(descriptor.get('job_id') or ''),
            unit_id=str(descriptor.get('id') or ''), rows=int(descriptor.get('rows') or 0),
            processes=int(descriptor.get('processes') or 1),
            threads=int(descriptor.get('threads') or 1),
            retries=int(descriptor.get('retries') or 0),
            timeout=float(descriptor.get('timeout') or 20),
            mode='timeout_full_trace', credentials_recorded=False)
    started = time.time()
    rows = max(0, int(descriptor.get('rows') or 0))
    process_limit = max(1, int(descriptor.get('processes') or 1))
    node_cpu_count = max(1, int(os.cpu_count() or 1))
    process_count = min(process_limit, max(1, rows))
    if descriptor.get('mode') == 'auth':
        process_count = _auth_process_limit(process_limit, rows, node_cpu_count)
    descriptor['_node_process_count'] = process_count
    descriptor['_node_configured_process_count'] = process_limit
    descriptor['_node_cpu_count'] = node_cpu_count
    active_threads_per_process = (
        _auth_worker_limit(descriptor.get('threads'), process_limit, process_count)
        if descriptor.get('mode') == 'auth'
        else max(1, int(descriptor.get('threads') or 1)))
    descriptor['_active_threads_per_process'] = active_threads_per_process
    context = mp.get_context('spawn')
    # Production deliberately has no IP inventory. Waiting auth threads are
    # collected for at most 200 ms and one provider request returns exactly
    # their current demand (up to 1000); retrying threads submit new demand.
    # The old injected pool path remains only for compatibility tests.
    proxy_pool = None
    if process_count == 1:
        _group_child(directory, descriptor, stop, proxy_pool)
        progress = read_progress = {}
        try:
            read_progress = json.loads((directory / 'progress.json').read_text('utf-8'))
        except (OSError, ValueError):
            pass
        read_progress.update(workers=0, worker_limit=process_limit,
                             active_process_limit=process_count,
                             threads_per_process=int(descriptor.get('threads') or 1),
                             active_threads_per_process=active_threads_per_process)
        atomic_json(directory / 'progress.json', read_progress)
        done = json.loads((directory / 'done.json').read_text('utf-8'))
        atomic_json(directory / 'done.json', done)
        return

    workers_root = directory / 'workers'
    workers_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    counts = [rows // process_count + (index < rows % process_count)
              for index in range(process_count)]
    child_descriptors = []
    current_row = int(descriptor.get('first_row') or 1)
    with (directory / 'input.part').open('rb') as source:
        for index, count in enumerate(counts):
            child_dir = workers_root / f'{index + 1:03d}'
            child_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
            for name in ('done.json', 'progress.json', 'results.jsonl'):
                (child_dir / name).unlink(missing_ok=True)
            child_input = child_dir / 'input.part'
            with child_input.open('wb') as output:
                child_input.chmod(0o600)
                for _ in range(count):
                    line = source.readline(65537)
                    if not line:
                        raise ValueError('input ended before local fan-out completed')
                    if len(line) > 65536:
                        raise ValueError('input record too long')
                    output.write(line)
            child = dict(descriptor, first_row=current_row, rows=count, processes=1)
            if descriptor.get('mode') == 'auth':
                # 每个进程分到一组不重叠槽位；30 台 / 10 进程时每进程 3 台，
                # 避免整个子包的账号都累计到同一台设备。
                assigned = descriptor.get('device_slots') or []
                child['device_slots'] = _partition_device_slots(
                    assigned or range(1, process_count + 1), process_count, index)
                child['device_pool_capacity'] = int(
                    descriptor.get('device_pool_capacity') or max(assigned))
            atomic_json(child_dir / 'manifest.json', child)
            child_descriptors.append((child_dir, child))
            current_row += count
        if source.read(1):
            raise ValueError('input contains more rows than package descriptor')

    children = []
    proxy_broker = None
    proxy_request_queue = None
    proxy_response_queues = []
    try:
        if (descriptor.get('mode') == 'auth' and descriptor.get('proxy_api')
                and process_count > 1):
            _load_fastcheck()
            from fastcheck.proxy_pool import fetch_proxies
            scheme = descriptor.get('proxy_scheme') or 'socks5h'

            def fetch_proxy_batch(count):
                return fetch_proxies(
                    descriptor['proxy_api'], count,
                    scheme=None if scheme == 'http' else scheme)

            proxy_request_queue = context.Queue()
            proxy_response_queues = [context.Queue() for _ in child_descriptors]
            proxy_broker = _NodeProxyMicrobatchBroker(
                proxy_request_queue, proxy_response_queues, fetch_proxy_batch)
            proxy_broker.start()
        for index, (child_dir, child) in enumerate(child_descriptors):
            child_stop = context.Event()
            microbatch_channels = (
                (proxy_request_queue, proxy_response_queues[index], index)
                if proxy_broker is not None else None)
            process = context.Process(target=_group_child,
                                      args=(str(child_dir), child, child_stop, proxy_pool,
                                            microbatch_channels),
                                      name=f'package-worker-{index + 1:02d}')
            process.start()
            children.append((process, child_stop, child_dir))

        last_write = 0.0
        last_processed = -1
        last_progress_at = time.monotonic()
        stall_timeout = stall_timeout_seconds(descriptor)
        while any(process.is_alive() for process, _, _ in children):
            if stop.is_set():
                for _, child_stop, _ in children:
                    child_stop.set()
            processed = 0
            active = 0
            live_counts = {}
            for process, _, child_dir in children:
                active += int(process.is_alive())
                try:
                    child_progress = json.loads((child_dir / 'progress.json').read_text('utf-8'))
                    processed += int(child_progress.get('processed') or 0)
                    for key, value in (child_progress.get('counts') or {}).items():
                        live_counts[key] = live_counts.get(key, 0) + int(value)
                except (OSError, ValueError):
                    pass
            now = time.monotonic()
            if processed != last_processed:
                last_processed = processed
                last_progress_at = now
            elif not stop.is_set() and now - last_progress_at >= stall_timeout:
                raise RuntimeError(
                    f'工作包无进度超过 {int(stall_timeout)} 秒，已中止并交回总控')
            if now - last_write >= 0.25:
                atomic_json(directory / 'progress.json',
                            {'processed': processed, 'started': started,
                             'workers': active, 'worker_limit': process_count,
                             'configured_process_limit': process_limit,
                             'threads_per_process': int(descriptor.get('threads') or 1),
                             'active_threads_per_process': active_threads_per_process,
                             'counts': live_counts})
                last_write = now
            time.sleep(0.05)

        failures = []
        for process, _, child_dir in children:
            process.join()
            done = {}
            try:
                done = json.loads((child_dir / 'done.json').read_text('utf-8'))
            except (OSError, ValueError):
                pass
            if process.exitcode != 0 or not done.get('ok'):
                failures.append(str(done.get('error') or f'worker_exit_{process.exitcode}'))
        if failures:
            raise RuntimeError(failures[0])

        output_digest = hashlib.sha256()
        processed = 0
        merged_counts = {}
        merged_output = directory / 'results.jsonl'
        with merged_output.open('wb') as output:
            merged_output.chmod(0o600)
            for _, _, child_dir in children:
                done = json.loads((child_dir / 'done.json').read_text('utf-8'))
                processed += int(done['processed'])
                for key, value in (done.get('counts') or {}).items():
                    merged_counts[key] = merged_counts.get(key, 0) + int(value)
                with (child_dir / 'results.jsonl').open('rb') as source:
                    for block in iter(lambda: source.read(1024 * 1024), b''):
                        output.write(block)
                        output_digest.update(block)
            output.flush()
            os.fsync(output.fileno())
        if processed != rows:
            raise ValueError('local fan-out result count mismatch')
        atomic_json(directory / 'progress.json',
                    {'processed': processed, 'started': started, 'workers': 0,
                     'worker_limit': process_count,
                     'configured_process_limit': process_limit,
                     'threads_per_process': int(descriptor.get('threads') or 1),
                     'active_threads_per_process': active_threads_per_process,
                     'counts': merged_counts})
        atomic_json(directory / 'done.json',
                    {'ok': True, 'processed': processed, 'counts': merged_counts,
                     'checksum': output_digest.hexdigest(),
                     'workers': process_count, 'elapsed': time.time() - started,
                     'configured_process_limit': process_limit,
                     'active_threads_per_process': active_threads_per_process})
    except Exception as exc:                                      # noqa: BLE001
        for process, child_stop, _ in children:
            child_stop.set()
            process.join(2)
            if process.is_alive():
                process.terminate()
                process.join(2)
        processed = 0
        live_counts = {}
        for _, _, child_dir in children:
            try:
                child_progress = json.loads((child_dir / 'progress.json').read_text('utf-8'))
                processed += int(child_progress.get('processed') or 0)
                for key, value in (child_progress.get('counts') or {}).items():
                    live_counts[key] = live_counts.get(key, 0) + int(value)
            except (OSError, ValueError):
                pass
        atomic_json(directory / 'progress.json',
                    {'processed': processed, 'started': started, 'workers': 0,
                     'worker_limit': process_count,
                     'configured_process_limit': process_limit,
                     'threads_per_process': int(descriptor.get('threads') or 1),
                     'active_threads_per_process': active_threads_per_process,
                     'counts': live_counts})
        atomic_json(directory / 'done.json',
                    {'ok': False, 'processed': processed,
                     'error': f'{type(exc).__name__}: {exc}'[:300]})
    finally:
        if proxy_broker is not None:
            proxy_broker.close()
        for process, _, _ in children:
            process.close()


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


def _bump_usage(device_root, counts):
    """每个工作包合并写一次设备计数，并用进程锁防止覆盖。"""
    if not device_root or not counts:
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
            for slot, count in counts.items():
                key = str(int(slot))
                data[key] = int(data.get(key) or 0) + int(count)
            atomic_json(path, data)
    except (OSError, portalocker.exceptions.LockException):
        pass


def record_device_registration(device_root, slot, proxy, model=''):
    """Persist the first observed registration facts for one device slot.

    Each slot owns a separate file, so worker processes cannot overwrite one
    another's history. O_EXCL also preserves the original time and proxy when
    the same device is reused by later tasks.
    """
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


def execute_auth_unit(directory, descriptor, stop, proxy_pool=None,
                      proxy_requester=None):
    """真实认证执行入口：任何异常都落成 done.json，避免节点只看到"进程退出"。"""
    try:
        _execute_auth_unit(directory, descriptor, stop, proxy_pool=proxy_pool,
                           proxy_requester=proxy_requester)
    except Exception as exc:                                       # noqa: BLE001
        atomic_json(Path(directory) / 'done.json',
                    {'ok': False, 'processed': 0,
                     'error': f'{type(exc).__name__}: {exc}'[:300]})


def _execute_auth_unit(directory, descriptor, stop, proxy_pool=None,
                       proxy_requester=None):
    from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
    from collections import Counter
    import heapq
    import sys

    directory = Path(directory)
    started = time.time()
    output_path = directory / 'results.jsonl'
    DevicePool, gsa_check, result_bucket = _load_fastcheck()
    proxy = descriptor.get('proxy')
    proxy_batcher = proxy_requester
    if (descriptor.get('proxy_api') and proxy_pool is None
            and proxy_batcher is None):
        from fastcheck.proxy_pool import fetch_proxies
        scheme = descriptor.get('proxy_scheme') or 'socks5h'

        def fetch_proxy_batch(count):
            return fetch_proxies(
                descriptor['proxy_api'], count,
                scheme=None if scheme == 'http' else scheme)

        proxy_batcher = _OnDemandProxyBatcher(fetch_proxy_batch)
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
    timeout = float(descriptor.get('timeout') or 20)
    retries = max(0, int(descriptor.get('retries') or 0))
    retry_delay = max(0.0, float(descriptor.get('_retry_delay',
                                                PROXY_RETRY_DELAY_SECONDS)))
    configured_workers = max(1, int(descriptor.get('threads') or 1))
    workers = max(1, int(descriptor.get('_active_threads_per_process') or
                         _auth_worker_limit(
                             configured_workers,
                             descriptor.get('_node_configured_process_count') or 1,
                             descriptor.get('_node_process_count') or 1)))
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
                           slots=slot_ids, credentials_recorded=False)

    # 一个包内把账号摊到所有设备上：一台设备只承担 rows/capacity 条，
    # 比固定用 1 号设备更分散，明显降低单设备被风控的概率。
    identities, infos, device_labels = {}, {}, {}
    # Compatibility only: an explicitly injected legacy pool keeps the old
    # bootstrap path. Production execute_unit_group never creates such a pool.
    for index in slot_ids if proxy_pool is not None else ():
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
            identities[index] = pool.fetch_headers(index, infos[index], max(timeout, 40),
                                                   registered=ready, proxy=bootstrap_proxy,
                                                   provider_direct=True)
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
        state = {'slot': slot_id, 'proxy': None, 'identity': None,
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
        if proxy_batcher is not None and (force or not state['proxy']):
            state['proxy'] = proxy_batcher.request(stop)
        elif proxy_batcher is None and not state['proxy']:
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

    def one(row, index, line, start_attempt=0, account_started=None,
            identity_override=identity_default, previous_proxy=None,
            force_proxy=False):
        if stop.is_set():
            return row, line, {'status': 'unknown', 'kind': 'stopped'}
        account, password = _split_account(line)
        account_started = time.time() if account_started is None else account_started
        worker_state = state_for_thread() if proxy_pool is None else None
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
                    identity = pool.fetch_headers(slot_id, info, max(timeout, 40),
                                                  registered=ready, proxy=account_proxy,
                                                  provider_direct=True)
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
                # 每次协议请求使用当前线程自己的出口。
                result = gsa_check(account, password, account_proxy, timeout=timeout,
                                   retries=0 if (proxy_pool is not None or
                                                 proxy_batcher is not None) else retries,
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
                        if proxy_batcher is not None:
                            account_proxy, lease = proxy_for(
                                index + attempt + web_attempt, force=True)
                            current_proxy = (_proxy_identity(account_proxy)
                                             if debug_enabled else current_proxy)
                            return account_proxy, account_proxy is not None
                        return account_proxy, False

                    if proxy_pool is not None or proxy_batcher is not None:
                        result = _refine_repair_auth_with_rotation(
                            result, account, password, account_proxy, timeout,
                            stop.is_set, retries, rotate_web)
                    else:
                        result = _refine_repair_auth(
                            result, account, password, account_proxy, timeout,
                            stop.is_set, retries=retries)
            except Exception as exc:                               # noqa: BLE001
                result = {'status': 'error', 'kind': type(exc).__name__, 'http': 0,
                          'stage': 'init', 'detail': str(exc)[:180]}
            request_ms = (round((time.monotonic() - request_started) * 1000, 2)
                          if debug_enabled else None)
            transport_failed = retryable_transport(result)
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
            if not transport_failed or attempt + 1 >= attempt_limit:
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
    usage_counts = Counter()
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
                usage_counts[device] += 1
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
