"""Bounded, expiring proxy leases supplied by one API thread."""
from collections import deque
from contextlib import nullcontext
from dataclasses import dataclass, field
import queue
import random
import signal
import threading
import time
from urllib.parse import urlsplit

import requests

from fastcheck.protocols.proxy_api import (ALLOWED_SCHEMES, apply_scheme, batch_limit as url_batch_limit,
                                           demand_url, parse)
from fastcheck.protocols.timing import phase


class ProxyFetchError(Exception):
    def __init__(self, code=0):
        self.code = code
        super().__init__(f"proxy_api_http_{code}" if code else "proxy_api_invalid_response")


def fetch_proxies(api_url, count, diagnostics=None, scheme=None):
    url = demand_url(api_url, count)
    with requests.Session() as session:
        session.trust_env = False
        trace = None
        if diagnostics is not None:
            from fastcheck.protocols.network_timing import NetworkTrace, TimingAdapter
            trace = NetworkTrace(diagnostics)
            for scheme_name in ('http://','https://'):
                session.mount(scheme_name, TimingAdapter(trace))
        with trace.request('proxy_api') if trace else nullcontext():
            with phase(diagnostics, 'proxy_api_fetch') as timing:
                with session.get(url, timeout=(5, 10), allow_redirects=False, stream=True) as response:
                    timing['http'] = response.status_code
                    if response.status_code != 200:
                        raise ProxyFetchError(response.status_code)
                    body = bytearray()
                    with phase(diagnostics, 'proxy_api_body'):
                        for block in response.iter_content(16384):
                            body.extend(block)
                            if len(body) > 1024 * 1024:
                                raise ProxyFetchError()
                    addresses = parse(body.decode('utf-8-sig'))
    addresses = [value for value in addresses if urlsplit(value).scheme in ALLOWED_SCHEMES]
    # 供应商普遍只返回裸 ip:port；传输协议由任务配置决定，必须在这里统一套上，
    # 否则 SOCKS5 隧道会被当成 HTTP 代理使用（表现为整批超时）。
    if scheme is not None:
        addresses = apply_scheme(addresses, scheme)
    if not addresses:
        raise ProxyFetchError()
    return addresses[:count]


def api_process(api_url, count, output, debug=False, scheme=None):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    try:
        from fastcheck.diagnostics import SafeDiagnostics
        diagnostic = SafeDiagnostics(output, role='proxy_api') if debug else None
        output.put({'addresses':fetch_proxies(api_url, count, diagnostic, scheme=scheme),
                    'dropped_debug_events':diagnostic.dropped if diagnostic else 0})
    except Exception as exc:
        output.put({'error_type':type(exc).__name__, 'http':getattr(exc, 'code', 0)})


class ApiFetcher:
    """Give the supplier thread a hard deadline, including DNS and slow reads."""
    def __init__(self, context, stop, proxy_auth='', *, target=api_process, deadline=15,
                 debug_log=None, scheme=None):
        self.context, self.stop, self.proxy_auth = context, stop, proxy_auth
        self.target, self.deadline = target, deadline
        self.debug_log = debug_log
        self.scheme = scheme

    def __call__(self, api_url, count):
        from fastcheck.protocols.proxy_api import inject_auth
        output = self.context.Queue(maxsize=2048 if self.debug_log else 1)
        kwargs = {}
        if self.target is api_process:
            kwargs['debug'] = bool(self.debug_log)
            if self.scheme is not None:
                kwargs['scheme'] = self.scheme
        process = self.context.Process(target=self.target, args=(api_url, count, output),
                                       kwargs=kwargs, name='proxy-api-fetch')
        started, result = False, None
        try:
            process.start()
            started = True
            until = time.monotonic() + self.deadline
            while not self.stop.is_set() and time.monotonic() < until:
                try:
                    message = output.get(timeout=.1)
                    if '_debug' in message:
                        if self.debug_log:
                            self.debug_log(message['_debug'])
                        continue
                    result = message
                    break
                except queue.Empty:
                    if not process.is_alive():
                        try:
                            message = output.get(timeout=.1)
                            if '_debug' in message:
                                if self.debug_log:
                                    self.debug_log(message['_debug'])
                                continue
                            result = message
                        except queue.Empty:
                            pass
                        break
            if not result or 'addresses' not in result:
                raise ProxyFetchError((result or {}).get('http', 0))
            if self.debug_log and result.get('dropped_debug_events',0):
                self.debug_log({'event':'diagnostic_drops','role':'proxy_api',
                                'count':result['dropped_debug_events']})
            return inject_auth(result['addresses'], self.proxy_auth)
        finally:
            if started:
                if process.is_alive():
                    process.terminate()
                process.join(.5)
                if process.is_alive():
                    process.kill()
                    process.join(2)
            output.cancel_join_thread()
            output.close()
            process.close()


@dataclass(repr=False)
class ProxyLease:
    label: str
    url: str = field(repr=False)
    expires_at: float = 0
    # 借用计数：池子补不满时同一条出口可以同时服务多个任务，不再让线程空等。
    refs: int = 0
    # 连续失败次数。单次抖动就判死会让池子反复萎缩、供应商补货跟不上，
    # 线程于是在"池子满-池子空"之间来回等；连续 fail_limit 次才真正淘汰。
    fails: int = 0
    bad: bool = False


# 连续多少次传输层失败才淘汰一条出口。
LEASE_FAIL_LIMIT = 3


class ProxyPool:
    def __init__(self, api_url, stop, log, *, ttl=60, target=10, fetcher=fetch_proxies,
                 clock=time.monotonic, on_limit=None, batch_limit=None):
        # 支持多个提取链接（多账号并联）：用空白分隔即可，供应商按链接各自的
        # 配额返回，池子合并成一份库存。轮询取用，避免单个账号被反复榨取。
        urls = [value for value in str(api_url or '').split() if value]
        if not urls:
            raise ValueError('代理 API 地址格式错误。')
        for value in urls:
            parsed = urlsplit(value)
            if parsed.scheme not in ('http', 'https') or not parsed.hostname or parsed.fragment:
                raise ValueError('代理 API 地址格式错误。')
        if batch_limit is None:
            # 单次提取上限直接取链接里的 num：用户在链接里写 num=2000，
            # 就是允许一次要 2000 条；没写 num 时退回 200 一路补。
            limits = [url_batch_limit(value) for value in urls]
            batch_limit = max([value for value in limits if value] or [200])
        if type(target) is not int or target < 0 or type(batch_limit) is not int or not 1 <= batch_limit <= 10000:
            raise ValueError('代理库存目标必须非负，单次提取上限必须在 1–10000 之间。')
        for value in urls:
            demand_url(value, max(1, min(target, batch_limit)))
        self.api_urls = urls
        self.api_url = urls[0]
        self.fetch_serial = 0
        self.stop, self.log = stop, log
        self.ttl, self.target, self.fetcher, self.clock = ttl, target, fetcher, clock
        self.batch_limit = batch_limit
        self.on_limit = on_limit
        self.condition = threading.Condition()
        self.available, self.busy, self.known = deque(), {}, {}
        self.serial, self.waiters = 0, 0
        self.prefill = True
        self.thread = threading.Thread(target=self._supply, daemon=True, name='proxy-supplier')

    def start(self):
        self.log('proxy_pool_start', target=self.target, ttl_seconds=self.ttl,
                 batch_limit=self.batch_limit, api_count=len(self.api_urls))
        self.thread.start()

    def set_target(self, target):
        if type(target) is not int or target < 0:
            raise ValueError('代理库存目标必须为非负整数。')
        with self.condition:
            if self.target != target:
                self.target = target
                self.condition.notify_all()

    def _prune(self):
        now = self.clock()
        before = len(self.available)
        self.available = deque(item for item in self.available if item.expires_at > now)
        active = {item.url for item in self.busy.values()}
        self.known = {url: until for url, until in self.known.items() if until > now or url in active}
        expired = before - len(self.available)
        if expired:
            self.log('proxy_expired', count=expired)

    def _supply(self):
        failures = 0
        while not self.stop.is_set():
            with self.condition:
                self._prune()
                # 库存要按"持有量"衡量（空闲 + 借用中）。只按空闲算，借用中的
                # 租约会被当成缺口，供应商就会反复补货，把隧道额度浪费在重复
                # 提取上；这也是高并发下提取次数暴涨的根因。
                held = len(self.available)+len(self.busy)
                if held>=self.target:
                    self.prefill=False
                low = self.prefill or len(self.available) < max(1, self.target // 3)
                if not low and not self.waiters:
                    self.condition.wait(.25)
                    continue
                count = min(self.batch_limit,self.target - held)
                if count <= 0:
                    self.condition.wait(.25)
                    continue
            self.log('proxy_fetch_start', requested=count)
            # 多链接轮询：每个账号的提取配额独立，轮流取能同时用满两边。
            api = self.api_urls[self.fetch_serial % len(self.api_urls)]
            self.fetch_serial += 1
            # TTL starts before fetching: response latency cannot extend a lease.
            fetched_at = self.clock()
            try:
                addresses = self.fetcher(api, count)
                accepted = 0
                with self.condition:
                    self._prune()
                    for address in addresses[:count]:
                        if address in self.known or fetched_at + self.ttl <= self.clock():
                            continue
                        self.serial += 1
                        lease = ProxyLease(f'IP{self.serial:04d}', address, fetched_at + self.ttl)
                        self.known[address] = lease.expires_at
                        self.available.append(lease)
                        accepted += 1
                    remaining = len(self.available)
                    self.condition.notify_all()
                self.log('proxy_fetch_done', received=accepted, available=remaining)
                failures = 0
                if accepted == 0:
                    self.stop.wait(2)
            except Exception as exc:
                failures += 1
                self.log('proxy_fetch_failed', error_type=type(exc).__name__,
                         http=getattr(exc, 'code', 0), failure_count=failures)
                if getattr(exc, 'code', 0) == 429:
                    if self.on_limit:
                        self.on_limit('proxy_api_ratelimit')
                    break
                self.stop.wait(min(2 ** failures, 10))
        with self.condition:
            self.condition.notify_all()
        self.log('proxy_supplier_stopped')

    def _claim(self, minimum):
        """取一条剩余寿命不少于 minimum 的租约，不够的放回池子等淘汰。

        放回而不是直接丢弃，是为了让更短 TTL 的备用租约还能被 fallback 用上。
        """
        held, chosen = [], None
        while self.available:
            lease = self.available.popleft()
            if lease.expires_at - self.clock() >= minimum:
                chosen = lease
                break
            held.append(lease)
        if held:
            self.available.extendleft(reversed(held))
        return chosen

    def _share(self, minimum):
        """池子已满额但全部在借用中时，共用一条出口，而不是让线程空等。

        返回 (lease, refs)。优先级：借用次数最少、剩余寿命最长。
        """
        if not self.busy:
            return None, 0
        now = self.clock()
        floor = max(3.0, minimum/4)
        usable = [item for item in self.busy.values()
                  if not item.bad and item.expires_at-now >= floor]
        if not usable:
            return None, 0
        # 大池子必须随机挑：只在前 24 条里找最小 refs，会让其余出口长期闲置，
        # 少数出口被反复压榨——池子越大越明显，表现为"加隧道反而更慢"。
        if len(usable) > 24:
            lease = random.choice(usable)
        else:
            lease = min(usable, key=lambda item:(item.refs,-item.expires_at))
        lease.refs += 1
        return lease, lease.refs

    def take(self, *, timeout=0, min_remaining=20, target_after_take=None, fallback_remaining=None,
             share=False):
        if target_after_take is not None and (type(target_after_take) is not int or target_after_take < 0):
            raise ValueError('领取后的代理库存目标必须为非负整数。')
        if fallback_remaining is not None and (type(fallback_remaining) is not int or fallback_remaining < 0):
            raise ValueError('备用寿命阈值必须为非负整数。')
        started = self.clock()
        until = self.clock() + timeout
        with self.condition:
            self.waiters += 1
            self.condition.notify_all()
            try:
                while not self.stop.is_set():
                    self._prune()
                    lease = self._claim(min_remaining)
                    short_lived = False
                    # 池子补不满（供应商给得慢或配额见底）时，用剩余寿命较短但
                    # 仍足够本轮的租约顶上：宁可复用一个旧出口，也不要让线程空等。
                    if lease is None and fallback_remaining is not None:
                        lease = self._claim(max(1, min(fallback_remaining, min_remaining)))
                        short_lived = lease is not None
                    if lease is not None:
                        lease.refs = 1
                        self.busy[lease.label] = lease
                        # Atomically shrink demand with allocation. The supplier
                        # must not see a last-task vacancy as demand for more IPs.
                        if target_after_take is not None:
                            self.target = target_after_take
                            self.condition.notify_all()
                        self.log('proxy_lease_acquired', proxy=lease.label,
                                 wait_ms=round((self.clock()-started)*1000,2),
                                 remaining_seconds=round(lease.expires_at-self.clock(),1),
                                 short_lived=short_lived)
                        return lease
                    # 只有"库存已经达到目标、全部都在借用中"才共享出口；库存没
                    # 达标说明还有补货空间，此时共享会掩盖供应商配额不足。
                    if share and len(self.available)+len(self.busy) >= self.target:
                        shared, refs = self._share(min_remaining)
                        if shared is not None:
                            if target_after_take is not None:
                                self.target = target_after_take
                                self.condition.notify_all()
                            self.log('proxy_lease_shared', proxy=shared.label, refs=refs,
                                     wait_ms=round((self.clock()-started)*1000,2),
                                     remaining_seconds=round(shared.expires_at-self.clock(),1))
                            return shared
                    remaining = until - self.clock()
                    if remaining <= 0:
                        return None
                    self.condition.wait(min(.25, remaining))
                return None
            finally:
                self.waiters -= 1

    def release(self, lease, *, valid=True):
        if lease is None:
            return
        with self.condition:
            owned = self.busy.get(lease.label)
            if owned is None:
                return
            if valid:
                owned.fails = 0
            else:
                owned.fails += 1
                if owned.fails >= LEASE_FAIL_LIMIT:
                    owned.bad = True
            owned.refs = max(0, owned.refs-1)
            if owned.refs:
                # 还有别的任务在用这条出口：只落坏标记，等最后一个借用者回收。
                self.condition.notify_all()
                return
            self.busy.pop(lease.label, None)
            # valid 只用于累加失败次数；只要没到淘汰阈值，出口仍回池复用。
            reusable = not owned.bad and owned.expires_at > self.clock()
            if reusable:
                self.available.append(owned)
            self.log('proxy_lease_released', proxy=owned.label, reusable=reusable,
                     fails=owned.fails)
            self.condition.notify_all()

    def wake(self):
        with self.condition:
            self.condition.notify_all()
