"""代理协议（scheme）接线与短租约回退的回归测试。"""
import threading
import unittest
from unittest import mock

import requests

from fastcheck.proxy_pool import ProxyLease, ProxyPool, fetch_proxies
from fastcheck.protocols.proxy_api import apply_scheme
from fastcheck.scheduler import Settings


class _Response:
    """最小化的 requests 响应替身：只要 status_code 与 iter_content。"""

    def __init__(self, body):
        self.status_code = 200
        self._body = body

    def __enter__(self):return self

    def __exit__(self, *exc):return False

    def iter_content(self, size):
        yield self._body


class FetchSchemeTests(unittest.TestCase):
    def fetch(self, body, scheme):
        with mock.patch.object(requests.Session, 'get', return_value=_Response(body)):
            return fetch_proxies('http://api.example.invalid/?num=10', 10, scheme=scheme)

    def test_bare_ip_port_inherits_configured_socks5h(self):
        self.assertEqual(self.fetch(b'1.2.3.4:8080\n5.6.7.8:3128\n', 'socks5h'),
                         ['socks5h://1.2.3.4:8080', 'socks5h://5.6.7.8:3128'])

    def test_default_stays_http_when_scheme_is_not_configured(self):
        self.assertEqual(self.fetch(b'1.2.3.4:8080\n', None), ['http://1.2.3.4:8080'])
        self.assertEqual(self.fetch(b'1.2.3.4:8080\n', 'http'), ['http://1.2.3.4:8080'])

    def test_explicit_scheme_overrides_supplier_prefix_and_keeps_credentials(self):
        body = b'socks5://9.9.9.9:1080\nu:p@8.8.8.8:8080\n'
        # 带凭据的行由解析器先收集，顺序沿用既有行为；重点是 scheme 被统一改写。
        self.assertEqual(self.fetch(body, 'socks5h'),
                         ['socks5h://u:p@8.8.8.8:8080', 'socks5h://9.9.9.9:1080'])

    def test_unparsable_body_still_raises_instead_of_returning_empty(self):
        from fastcheck.proxy_pool import ProxyFetchError
        with self.assertRaises(ProxyFetchError):
            self.fetch(b'no proxy here', 'socks5h')

    def test_apply_scheme_rejects_unknown_protocol(self):
        with self.assertRaises(ValueError):
            apply_scheme(['http://1.2.3.4:1'], 'https')


class SettingsSchemeTests(unittest.TestCase):
    def test_socks5h_and_http_are_accepted(self):
        self.assertEqual(Settings(proxy_scheme='socks5h').proxy_scheme, 'socks5h')
        self.assertEqual(Settings(proxy_scheme='http').proxy_scheme, 'http')

    def test_unknown_protocol_is_rejected(self):
        with self.assertRaises(ValueError):
            Settings(proxy_scheme='socks4')


class ShortLeaseFallbackTests(unittest.TestCase):
    def pool(self, now):
        return ProxyPool('https://api.example.invalid', threading.Event(),
                         lambda *a, **k: None, clock=lambda: now[0])

    def test_short_lease_is_used_when_pool_cannot_refill(self):
        now = [100.]
        pool = self.pool(now)
        lease = ProxyLease('IP1', 'socks5h://127.0.0.1:1080', 120.)   # 只剩 20 秒
        pool.available.append(lease)
        pool.known[lease.url] = 120.
        self.assertIsNone(pool.take(min_remaining=28))
        self.assertIs(pool.take(min_remaining=28, fallback_remaining=15), lease)
        self.assertIn('IP1', pool.busy)

    def test_fallback_does_not_accept_an_expired_lease(self):
        now = [100.]
        pool = self.pool(now)
        lease = ProxyLease('IP1', 'socks5h://127.0.0.1:1080', 105.)
        pool.available.append(lease)
        pool.known[lease.url] = 105.
        self.assertIsNone(pool.take(min_remaining=28, fallback_remaining=15))

    def test_without_fallback_behaviour_is_unchanged(self):
        now = [100.]
        pool = self.pool(now)
        lease = ProxyLease('IP1', 'socks5h://127.0.0.1:1080', 120.)
        pool.available.append(lease)
        pool.known[lease.url] = 120.
        self.assertIsNone(pool.take(min_remaining=28))

    def test_negative_fallback_is_rejected(self):
        with self.assertRaises(ValueError):
            self.pool([100.]).take(fallback_remaining=-1)


if __name__ == '__main__':
    unittest.main()


class SharedLeaseTests(unittest.TestCase):
    """池子达到目标且全部在借用中时，允许共用出口而不是让线程空等。"""

    def pool(self, now, target=1):
        return ProxyPool('https://api.example.invalid', threading.Event(),
                         lambda *a, **k: None, clock=lambda: now[0], target=target)

    def test_share_returns_busy_lease_once_pool_is_full(self):
        now = [100.]
        pool = self.pool(now)
        lease = ProxyLease('IP1', 'socks5h://127.0.0.1:1080', 600.)
        pool.available.append(lease)
        pool.known[lease.url] = 600.
        self.assertIs(pool.take(min_remaining=28), lease)
        self.assertIs(pool.take(min_remaining=28, share=True), lease)
        self.assertEqual(lease.refs, 2)

    def test_share_is_off_by_default_and_still_returns_none(self):
        now = [100.]
        pool = self.pool(now)
        lease = ProxyLease('IP1', 'socks5h://127.0.0.1:1080', 600.)
        pool.available.append(lease)
        pool.known[lease.url] = 600.
        pool.take(min_remaining=28)
        self.assertIsNone(pool.take(min_remaining=28))

    def test_shared_lease_is_only_freed_after_every_borrower_returns(self):
        now = [100.]
        pool = self.pool(now)
        lease = ProxyLease('IP1', 'socks5h://127.0.0.1:1080', 600.)
        pool.available.append(lease)
        pool.known[lease.url] = 600.
        first = pool.take(min_remaining=28)
        second = pool.take(min_remaining=28, share=True)
        self.assertIs(first, second)
        pool.release(first)
        self.assertIn('IP1', pool.busy)
        self.assertEqual(lease.refs, 1)
        pool.release(second)
        self.assertNotIn('IP1', pool.busy)
        self.assertEqual(len(pool.available), 1)

    def test_single_failure_keeps_the_lease_reusable(self):
        """单次传输抖动不该废掉出口，否则池子萎缩、线程反而要等补货。"""
        now = [100.]
        pool = self.pool(now)
        lease = ProxyLease('IP1', 'socks5h://127.0.0.1:1080', 600.)
        pool.available.append(lease)
        pool.known[lease.url] = 600.
        first = pool.take(min_remaining=28)
        pool.release(first, valid=False)
        self.assertNotIn('IP1', pool.busy)
        self.assertEqual(len(pool.available), 1)

    def test_repeated_failures_retire_the_lease(self):
        now = [100.]
        pool = self.pool(now)
        lease = ProxyLease('IP1', 'socks5h://127.0.0.1:1080', 600.)
        pool.available.append(lease)
        pool.known[lease.url] = 600.
        for _ in range(3):
            taken = pool.take(min_remaining=28)
            self.assertIsNotNone(taken)
            pool.release(taken, valid=False)
        self.assertNotIn('IP1', pool.busy)
        self.assertEqual(len(pool.available), 0)
        self.assertTrue(lease.bad)

    def test_success_resets_the_failure_counter(self):
        now = [100.]
        pool = self.pool(now)
        lease = ProxyLease('IP1', 'socks5h://127.0.0.1:1080', 600.)
        pool.available.append(lease)
        pool.known[lease.url] = 600.
        for _ in range(2):
            pool.release(pool.take(min_remaining=28), valid=False)
        pool.release(pool.take(min_remaining=28), valid=True)
        self.assertEqual(lease.fails, 0)
        self.assertFalse(lease.bad)

    def test_share_refuses_leases_that_are_about_to_expire(self):
        now = [100.]
        pool = self.pool(now)
        lease = ProxyLease('IP1', 'socks5h://127.0.0.1:1080', 600.)
        pool.available.append(lease)
        pool.known[lease.url] = 600.
        pool.take(min_remaining=28)
        now[0] = 595.
        self.assertIsNone(pool.take(min_remaining=28, share=True))
