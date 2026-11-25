import collections
import hashlib
import io
import tempfile
import threading
import time
import unittest

from clusterlab.threadworker import (Context, DeviceSlot, ProxySource, ThreadDevicePool,
                                     Worker, _commit_ready_rows,
                                     _is_proxy_quality_failure)


class FakeDevicePool:
    def __init__(self):
        self.header_calls = 0

    def fetch_headers(self, *_args, **_kwargs):
        self.header_calls += 1
        return {'otp': self.header_calls}


class ThreadWorkerPoolTests(unittest.TestCase):
    def test_live_counts_only_include_contiguous_committed_rows(self):
        pending = {2: (b'two\n', 'locked')}
        output = io.BytesIO()
        digest = hashlib.sha256()
        counts = collections.Counter()

        next_row, committed = _commit_ready_rows(
            pending, 1, output, digest, counts)
        self.assertEqual((1, 0), (next_row, committed))
        self.assertEqual({}, dict(counts))

        pending[1] = (b'one\n', 'wrong_password')
        next_row, committed = _commit_ready_rows(
            pending, next_row, output, digest, counts)
        self.assertEqual((3, 2), (next_row, committed))
        self.assertEqual(b'one\ntwo\n', output.getvalue())
        self.assertEqual({'wrong_password': 1, 'locked': 1}, dict(counts))

    def test_proxy_refill_scales_with_process_concurrency(self):
        large = ProxySource('http://invalid/{num}', threads=1000, target=1000)
        self.assertEqual(1000, large.refill_batch)
        self.assertEqual(500, large.low_water)

        small = ProxySource('http://invalid/{num}', threads=10, target=10)
        self.assertEqual(10, small.refill_batch)
        self.assertEqual(9, small.low_water)

        explicit = ProxySource('http://invalid/{num}', threads=1000, target=1000,
                               refill_batch=80, low_water=40)
        self.assertEqual(80, explicit.refill_batch)
        self.assertEqual(40, explicit.low_water)

        requested = []
        def fetch_once(count):
            requested.append(count)
            large._stop.set()
            return []
        large._fetch = fetch_once
        large._supply_loop()
        self.assertEqual([1000], requested)

    def test_proxy_is_reused_by_same_thread_and_bad_exit_is_quarantined(self):
        pool = ProxySource('http://invalid/{num}', threads=1, target=10,
                           low_water=2, ttl=50)
        values = pool._accept(['socks5h://gw:1001', 'socks5h://gw:1002'])
        with pool._cv:
            pool._local[0].append(values[0])
            pool._free.append(values[1])
        first, first_exp = pool.acquire(0, wait=0)
        pool.release(0, first, first_exp, healthy=True)
        second, second_exp = pool.acquire(0, wait=0)
        self.assertEqual(first, second)
        pool.release(0, second, second_exp, healthy=False)
        self.assertEqual([], pool._accept([second]))

    def test_every_http_zero_triggers_same_ip_google_probe(self):
        self.assertTrue(_is_proxy_quality_failure(
            {'http': 0, 'kind': 'proxy', 'detail': '代理池未返回结果'}))
        self.assertTrue(_is_proxy_quality_failure({'http': '0', 'detail': ''}))
        self.assertFalse(_is_proxy_quality_failure({'http': 503, 'detail': 'timeout'}))
        self.assertFalse(_is_proxy_quality_failure({'http': 200, 'detail': 'timeout'}))

    def test_near_expiry_exit_is_not_leased(self):
        pool = ProxySource('http://invalid/{num}', threads=1, ttl=50)
        with pool._cv:
            pool._local[0].append(('socks5h://gw:1001', time.monotonic() + 2))
        self.assertEqual((None, 0.0), pool.acquire(0, wait=0))

    def test_worker_borrows_and_returns_shared_device_for_each_batch(self):
        device = DeviceSlot(1, 'ready', 'id', 'label', {
            'model': 'fixture', 'client_info': 'fixture', 'user_agent': 'fixture'})

        class Devices:
            use_limit = 0
            def __init__(inner):
                inner.acquired = 0
                inner.released = 0
            def acquire(inner, wait=0):
                inner.acquired += 1
                return device
            def release(inner, value, retire=False):
                self.assertIs(value, device)
                self.assertFalse(retire)
                inner.released += 1
            def headers(inner, *_args, **_kwargs):
                return {'fixture': True}

        devices = Devices()
        results = []
        ctx = Context(
            cfg={'accounts_per_ip': 2, 'retries': 0, 'timeout': 1,
                 'otp_reuse': 15, 'web_secondary_auth': False},
            gsa_check=lambda *_args, **_kwargs: {'status': 'valid', 'http': 200},
            result_bucket=lambda result: result['status'], devices=devices,
            proxies=None, tasks=collections.deque([
                (1, 'a----p'), (2, 'b----p'), (3, 'c----p')]),
            stop=threading.Event(),
            sink=lambda row, line, leased, result: results.append((row, leased.slot)),
            sink_lock=threading.Lock(), counters=collections.Counter(),
            telemetry=None, debug=False)
        Worker(0, ctx).run()
        self.assertEqual(results, [(1, 1), (2, 1), (3, 1)])
        self.assertEqual((devices.acquired, devices.released), (2, 2))

    def test_zero_device_limit_and_fifteen_account_header_reuse(self):
        raw = FakeDevicePool()
        with tempfile.TemporaryDirectory() as root:
            devices = ThreadDevicePool(raw, [1], root, use_limit=0)
            device = DeviceSlot(1, 'ready', 'id', 'label', {
                'model': 'fixture', 'client_info': 'fixture', 'user_agent': 'fixture'})
            values = [devices.headers(device, None, 1, reuse=15) for _ in range(15)]
            self.assertEqual(0, devices.use_limit)
            self.assertEqual(1, raw.header_calls)
            self.assertTrue(all(value == values[0] for value in values))
            devices.headers(device, None, 1, reuse=15)
            self.assertEqual(2, raw.header_calls)


if __name__ == '__main__':
    unittest.main()
