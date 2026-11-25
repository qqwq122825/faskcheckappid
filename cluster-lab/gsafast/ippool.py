"""每进程独立 IP 池：启动有上限，运行时按低水位小批补货。

设计目标（按优先级）：
  1. 每个进程只维护自己的池和 supplier，进程之间不共享补货状态；
  2. 启动预热最多 1000 条，避免多进程同时向代理 API 发起超大批请求；
  3. 可用库存降到低水位 200 时，单次只补 200 条，最高保留 1000 条；
  4. acquire 仍走本地 deque / SimpleQueue，池空时唤醒 supplier，不阻塞事件循环；
  5. _seen 只负责 TTL 去重，水位只统计队列里的真实可用库存。
"""
from __future__ import annotations

import collections
import math
import queue
import threading
import time
import urllib.request
from urllib.parse import urlsplit


class IpPool:
    def __init__(self, api, *, threads=1000, ttl=50.0, per_ip=9,
                 batch=1000, refill_interval=2.0, log=None,
                 initial_max=1000, refill_batch=None, low_water=None,
                 lease_margin=8.0, gateway_limit=0,
                 rotate=False):
        self.api = api
        self.threads = max(1, int(threads))
        self.ttl = max(5.0, float(ttl))
        self.per_ip = max(1, int(per_ip))
        self.batch = max(1, int(batch))
        self.interval = max(0.5, float(refill_interval))
        self.initial_max = max(1, int(initial_max))
        # A fixed 200-item refill cannot sustain 1000 workers once retries
        # start rotating bad exits: the supplier empties faster than it can
        # replenish and all workers sleep in acquire().  Scale both watermarks
        # with concurrency while retaining small batches for small jobs.
        if refill_batch is None:
            refill_batch = min(self.initial_max, max(200, self.threads))
        if low_water is None:
            low_water = min(self.initial_max - 1, max(50, self.threads // 2))
        self.refill_batch = max(1, min(self.batch, int(refill_batch)))
        self.low_water = max(0, min(self.initial_max - 1, int(low_water)))
        self.high_water = min(self.initial_max,
                              self.low_water + self.refill_batch)
        self.lease_margin = max(1.0, min(self.ttl - 1.0, float(lease_margin)))
        # 实测：同一入口网关（如 137.184.119.209 的 400 个端口）同时建 400 连接
        # 成功率 100%，且每个端口背后是独立出口 IP。所以网关并发不是瓶颈，
        # 0 表示不限制；需要限流时显式传正数。
        self.gateway_limit = max(0, int(gateway_limit))
        # rotate=True：还回去的 IP 走共享队列，下次 acquire 从共享队头拿一条不同的，
        # 实现「每个任务结束就换新 IP」。关闭时保留每线程本地队列以复用长连接。
        self.rotate = bool(rotate)
        self.log = log or (lambda *a, **k: None)

        # 每线程一个本地队列：只有一个线程读写，天然无锁
        self._local = {tid: collections.deque() for tid in range(self.threads)}
        self._shared = queue.SimpleQueue()      # C 实现，正常路径几乎不用
        self._seen = {}                         # proxy -> expires_at，仅用于去重/观测
        self._seen_lock = threading.Lock()
        self._gateway_active = collections.Counter()
        self._gateway_lock = threading.Lock()
        self._refill_lock = threading.Lock()
        self._refill_wakeup = threading.Event()
        self.cpu_supplier = 0.0
        self.cpu_snapshot = 0.0
        self._cv = threading.Condition()
        self._stop = threading.Event()
        self._thread = None
        self.stats = {'warmed': 0, 'api_calls': 0, 'api_error': 0, 'fetched': 0,
                      'duplicate': 0, 'expired': 0, 'discarded': 0,
                      'local_hit': 0, 'shared_hit': 0, 'blocked': 0,
                      'refill_calls': 0, 'low_water_hits': 0,
                      'near_expiry': 0, 'gateway_limited': 0,
                      'blocked_seconds': 0.0, 'last_fetch_ms': 0.0}

    # ---------------------------------------------------------------- 拉取
    def _fetch(self, num):
        num = max(1, min(self.batch, int(num)))
        url = self.api.format(num=num)
        started = time.monotonic()
        error = None
        try:
            with urllib.request.urlopen(url, timeout=25) as resp:
                body = resp.read().decode('utf-8', 'replace')
        except Exception as exc:                                    # noqa: BLE001
            self.stats['api_error'] += 1
            error = exc
            return []
        finally:
            elapsed_ms = round((time.monotonic() - started) * 1000, 1)
            self.stats['api_calls'] += 1
            self.stats['last_fetch_ms'] = elapsed_ms
            if error is not None:
                self.log('ip_api_error', requested=num,
                         elapsed_ms=elapsed_ms,
                         error=type(error).__name__,
                         detail=str(error)[:160])
        values = []
        for line in body.splitlines():
            line = line.strip()
            if not line or line.startswith('{') or ':' not in line:
                continue
            values.append(line if '://' in line else f'socks5h://{line}')
        self.stats['fetched'] += len(values)
        self.log('ip_api_fetch', requested=num, got=len(values),
                 elapsed_ms=round((time.monotonic() - started) * 1000, 1))
        return values

    def _accept(self, values, now=None):
        """去重后带 expires_at 存进 _seen；返回 (proxy, expires_at) 列表。"""
        now = time.monotonic() if now is None else now
        expires = now + self.ttl
        fresh = []
        with self._seen_lock:
            for proxy in values:
                if proxy in self._seen and self._seen[proxy] > now:
                    self.stats['duplicate'] += 1
                    continue
                self._seen[proxy] = expires
                fresh.append((proxy, expires))
        return fresh

    # ---------------------------------------------------------------- 预热
    def warm(self, extra=0):
        """启动预热：默认按 threads × 1.2 备料，上限 initial_max。

        实测每个入口端口背后是独立出口 IP，且同一网关 400 并发 100% 通过，
        所以多备料是有意义的（换 IP 时不必等补货）；但备太多会在队列里
        放到快过期，1.2 倍是折中。
        """
        requested = int(self.threads * (1 + max(0, extra)))
        target = min(self.initial_max, requested if extra else int(self.threads * 1.2))
        fresh = []
        rounds = 0
        while len(fresh) < target and rounds < 8 and not self._stop.is_set():
            want = min(self.batch, target - len(fresh))
            values = self._fetch(want)
            rounds += 1
            if not values:
                break
            fresh.extend(self._accept(values))
        fresh = fresh[:target]
        with self._cv:
            for index, item in enumerate(fresh):
                if (not self.rotate and index < self.threads
                        and not self._local[index]):
                    self._local[index].append(item)
                else:
                    self._shared.put(item)
            self.stats['warmed'] = len(fresh)
            self._cv.notify_all()
        self.log('ip_warmed', requested=requested, target=target,
                 fresh=len(fresh), rounds=rounds)
        return len(fresh)

    # ---------------------------------------------------------------- 补货
    def start(self):
        self._thread = threading.Thread(target=self._supply, name='ip-supplier', daemon=True)
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()
        self._refill_wakeup.set()
        with self._cv:
            self._cv.notify_all()
        if self._thread:
            self._thread.join(3)

    def _live_local(self):
        now = time.monotonic()
        return sum(1 for bucket in self._local.values()
                   for _, exp in bucket if exp - now > self.lease_margin)

    def _prune_shared(self, limit=4000):
        """清掉共享队列里已过期的，避免队头堆满死货。"""
        now = time.monotonic()
        kept = []
        dropped = 0
        for _ in range(min(limit, self._shared.qsize())):
            try:
                item = self._shared.get_nowait()
            except queue.Empty:
                break
            if item[1] - now > self.lease_margin:
                kept.append(item)
            else:
                dropped += 1
        for item in kept:
            self._shared.put(item)
        self.stats['expired'] += dropped
        return dropped

    def _available_live(self):
        """本地队列与共享队列中真正可立即取用的库存。"""
        return self._live_local() + self._shared.qsize()

    @staticmethod
    def _gateway(proxy):
        """代理入口主机；不同端口仍属于同一个供应商网关。"""
        try:
            return urlsplit(proxy).hostname or proxy
        except ValueError:
            return proxy

    def _reserve_gateway(self, proxy):
        if not self.gateway_limit:
            return True                      # 不限制
        gateway = self._gateway(proxy)
        with self._gateway_lock:
            if self._gateway_active[gateway] >= self.gateway_limit:
                self.stats['gateway_limited'] += 1
                return False
            self._gateway_active[gateway] += 1
        return True

    def _release_gateway(self, proxy):
        if not self.gateway_limit:
            return
        gateway = self._gateway(proxy)
        with self._gateway_lock:
            if self._gateway_active[gateway] > 1:
                self._gateway_active[gateway] -= 1
            else:
                self._gateway_active.pop(gateway, None)

    def _supply(self):
        """本进程唯一 API 调用方；按真实可用库存执行水位补货。"""
        sup0 = time.thread_time()
        try:
            self._supply_loop()
        finally:
            self.cpu_supplier = time.thread_time() - sup0

    def _supply_loop(self):
        # Empty-pool waiters may set _refill_wakeup thousands of times per
        # second. Wakeups can request an earlier inventory check, but must not
        # bypass the supplier API rate limit.
        next_fetch_at = 0.0
        while not self._stop.is_set():
            try:
                self._prune_shared()
                available = self._available_live()
                if available <= self.low_water:
                    self.stats['low_water_hits'] += 1
                    delay = next_fetch_at - time.monotonic()
                    if delay > 0:
                        self._stop.wait(delay)
                        continue
                    request = min(self.refill_batch,
                                  self.initial_max - available)
                    if request > 0:
                        values = self._fetch(request)
                        next_fetch_at = time.monotonic() + self.interval
                        fresh = self._accept(values)
                        with self._cv:
                            for item in fresh:
                                self._shared.put(item)
                            self._cv.notify_all()
                        self.stats['refill_calls'] += 1
                        self.log('ip_refilled', requested=request,
                                 got=len(fresh), available_before=available,
                                 available_after=available + len(fresh),
                                 low_water=self.low_water,
                                 high_water=self.high_water)
            except Exception as exc:                                # noqa: BLE001
                self.log('ip_supply_error', error=type(exc).__name__)
            self._refill_wakeup.wait(self.interval)
            self._refill_wakeup.clear()

    # ---------------------------------------------------------------- 取还
    def acquire_nowait(self, tid):
        """异步调度器用的非阻塞取用；池空时不睡眠也不计一次 blocked。"""
        bucket = None if self.rotate else self._local.get(tid)
        now = time.monotonic()
        if bucket:
            while bucket:
                proxy, exp = bucket.popleft()
                if exp - now > self.lease_margin:
                    if not self._reserve_gateway(proxy):
                        bucket.appendleft((proxy, exp))
                        return None, 0.0
                    self.stats['local_hit'] += 1
                    return proxy, exp
                self.stats['near_expiry'] += 1
                self.stats['expired'] += 1
        while True:
            try:
                proxy, exp = self._shared.get_nowait()
            except queue.Empty:
                self._refill_wakeup.set()
                return None, 0.0
            if exp - time.monotonic() > self.lease_margin:
                if not self._reserve_gateway(proxy):
                    self._shared.put((proxy, exp))
                    return None, 0.0
                self.stats['shared_hit'] += 1
                return proxy, exp
            self.stats['near_expiry'] += 1
            self.stats['expired'] += 1

    def acquire(self, tid, wait=30.0):
        """返回 (proxy, expires_at)；取用不排队，只有池真的空了才会短暂等待。"""
        t0 = time.monotonic()
        proxy, exp = self.acquire_nowait(tid)
        if proxy:
            return proxy, exp
        # 这里只等 supplier 补货，工作线程绝不自己发 API
        deadline = t0 + max(0.0, float(wait))
        while time.monotonic() < deadline and not self._stop.is_set():
            proxy, exp = self.acquire_nowait(tid)
            if proxy:
                self.stats['blocked'] += 1
                self.stats['blocked_seconds'] += time.monotonic() - t0
                return proxy, exp
            time.sleep(0.05)
        self.stats['blocked'] += 1
        self.stats['blocked_seconds'] += time.monotonic() - t0
        return None, 0.0

    def release(self, tid, proxy, expires_at, *, healthy=True):
        """还回去：未过期且健康，优先放回本地队列，其次共享。"""
        if not proxy:
            return 'dropped'
        now = time.monotonic()
        self._release_gateway(proxy)
        if not healthy or expires_at - now <= self.lease_margin:
            self.stats['discarded'] += 1
            with self._seen_lock:
                # Failure means immediate discard, not quarantine.  If the
                # supplier later returns the same endpoint it is eligible
                # again like any other fresh item.
                self._seen.pop(proxy, None)
            return 'discarded'
        bucket = None if self.rotate else self._local.get(tid)
        if bucket is not None:
            bucket.append((proxy, expires_at))
        else:
            self._shared.put((proxy, expires_at))
        with self._cv:
            self._cv.notify()
        return 'pooled'

    # ---------------------------------------------------------------- 观测
    def snapshot(self):
        now = time.monotonic()
        with self._seen_lock:
            tracked = sum(1 for exp in self._seen.values() if exp > now)
        with self._gateway_lock:
            active_gateways = dict(self._gateway_active)
        local = self._live_local()
        shared = self._shared.qsize()
        return {'threads': self.threads, 'ttl': self.ttl, 'per_ip': self.per_ip,
                'rotate': self.rotate,
                'tracked': tracked, 'local_live': local, 'shared': shared,
                'available': local + shared,
                'initial_max': self.initial_max,
                'low_water': self.low_water, 'high_water': self.high_water,
                'refill_batch': self.refill_batch,
                'lease_margin': self.lease_margin,
                'gateway_limit': self.gateway_limit,
                'gateway_active': active_gateways,
                'supplier_alive': bool(self._thread and self._thread.is_alive()),
                **self.stats}
