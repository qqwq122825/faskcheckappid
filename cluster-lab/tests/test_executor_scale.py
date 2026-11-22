import hashlib
import json
import multiprocessing as mp
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from clusterlab.executor import (DeviceAuthLease, NodeProxyPool, _auth_process_limit,
                                 _auth_worker_limit, _consume_thread_pool_device,
                                 _device_id_label, _execute_auth_unit,
                                 _partition_device_slots, _usage_snapshot,
                                 _apple_reason_for_record, _proxy_debug_enabled, _slot_for_account, atomic_json,
                                 execute_unit_group, proxy_supplier_loop,
                                 record_device_registration, stall_timeout_seconds)


class FakePool:
    def __init__(self, root, *_args, **_kwargs):
        self.root = Path(root)

    def slot_directory(self, slot):
        return self.root / f'{int(slot):02d}'

    def info(self, *_args, **_kwargs):
        return {}

    def inspect(self, _slot):
        return {'state': 'ready'}

    def fetch_headers(self, *_args, **_kwargs):
        return {}


class ScriptedProxyPool:
    def __init__(self, proxies):
        now = time.monotonic()
        self.leases = [(proxy, now + 60, 0) for proxy in proxies]
        self.releases = []
        self.discards = []

    def acquire(self, wait=1.0):
        del wait
        return self.leases.pop(0) if self.leases else None

    def release(self, lease, healthy):
        self.releases.append((lease[0] if lease else None, healthy))

    def discard(self, lease):
        self.discards.append(lease[0] if lease else None)


class BusyThenReusableProxyPool(ScriptedProxyPool):
    """Expose one busy interval before a healthy lease is reusable."""

    def __init__(self):
        super().__init__(['socks5h://bootstrap'])
        self.account_acquires = 0

    def acquire(self, wait=1.0):
        if self.leases:
            return super().acquire(wait)
        self.account_acquires += 1
        if self.account_acquires == 1:
            return None
        now = time.monotonic()
        return ('socks5h://reused-after-busy', now + 60, 1, 0)

    def snapshot(self):
        # The first account acquisition observes an exit leased by another
        # worker.  The executor must wait for it instead of recording a row.
        return {'available': 0, 'in_use': 1, 'known': 1}


class RecordingStop:
    def __init__(self):
        self.waits = []

    def is_set(self):
        return False

    def wait(self, seconds):
        self.waits.append(float(seconds))
        return False


def use_shared_proxy_pool(pool, output):
    lease = pool.acquire(wait=1)
    output.put(lease[0] if lease else None)
    pool.release(lease, healthy=lease is not None)


class ExecutorScaleTests(unittest.TestCase):
    def setUp(self):
        # Direct executor fixtures predate node manifests. Production node
        # manifests explicitly carry the required three-second cooldown.
        cooldown = patch('clusterlab.executor.DEVICE_AUTH_COOLDOWN_SECONDS', 0.0)
        cooldown.start()
        self.addCleanup(cooldown.stop)

    def test_device_lease_is_exclusive_and_enforces_cooldown(self):
        class Pool:
            def __init__(inner, root):
                inner.root = Path(root)

            def slot_directory(inner, slot):
                return inner.root / f'{int(slot):02d}'

        with tempfile.TemporaryDirectory() as temporary:
            pool = Pool(temporary)
            stop = threading.Event()
            first = DeviceAuthLease.acquire(pool, 1, stop, cooldown=.15)
            marker = json.loads((pool.slot_directory(1) / '.auth-cooldown.json').read_text())
            self.assertEqual(marker['state'], 'in_use')
            acquired = []

            def second_user():
                lease = DeviceAuthLease.acquire(pool, 1, stop, cooldown=.15)
                acquired.append(time.monotonic())
                lease.release()

            thread = threading.Thread(target=second_user)
            thread.start()
            time.sleep(.05)
            self.assertEqual(acquired, [])
            released = time.monotonic()
            first.release()
            thread.join(1)
            self.assertFalse(thread.is_alive())
            self.assertGreaterEqual(acquired[0] - released, .13)
            marker = json.loads((pool.slot_directory(1) / '.auth-cooldown.json').read_text())
            self.assertEqual(marker['state'], 'cooldown')
            self.assertEqual(marker['slot'], 1)
            self.assertIn('last_used_at', marker)
            self.assertIn('available_at', marker)

    def test_thread_mode_prefers_ready_pool_then_uses_registration_gap(self):
        class Pool:
            def __init__(inner, root):
                inner.root = Path(root)
                inner.base = inner.root

            def slot_directory(inner, slot):
                return inner.root / f'{int(slot):02d}'

        with tempfile.TemporaryDirectory() as temporary:
            pool = Pool(temporary)
            stop = threading.Event()
            first = DeviceAuthLease.acquire_any(
                pool, [1, 2], stop, cooldown=0,
                ready_slots=[1], overflow_slots=[2])
            self.assertEqual(first.slot, 1)
            second = DeviceAuthLease.acquire_any(
                pool, [1, 2], stop, cooldown=0,
                ready_slots=[1], overflow_slots=[2])
            self.assertEqual(second.slot, 2)
            second.release()
            first.release()

    def test_thread_mode_registers_new_slot_instead_of_waiting_for_busy_device(self):
        class Pool:
            def __init__(inner, root):
                inner.root = Path(root)

            def slot_directory(inner, slot):
                return inner.root / f'{int(slot):02d}'

            def inspect(inner, slot):
                return {'state': 'ready' if int(slot) == 1 else 'missing'}

        with tempfile.TemporaryDirectory() as temporary:
            pool = Pool(temporary)
            stop = threading.Event()
            busy = DeviceAuthLease.acquire(pool, 1, stop, cooldown=0)
            started = time.monotonic()
            extension = DeviceAuthLease.acquire_any(pool, [1, 2], stop, cooldown=0)
            elapsed = time.monotonic() - started
            self.assertEqual(extension.slot, 2)
            self.assertLess(elapsed, .1)
            extension.release()
            busy.release()

    def test_ninth_use_is_verified_then_tenth_use_retires_device(self):
        class Pool:
            def __init__(inner, root):
                inner.root = Path(root)
                inner.base = inner.root

            def slot_directory(inner, slot):
                return inner.root / f'{int(slot):02d}'

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            pool = Pool(root)
            slot = pool.slot_directory(1)
            slot.mkdir(parents=True)
            (slot / 'registered').write_text('identity', 'utf-8')
            (root / 'usage.json').write_text('{"1":9}', 'utf-8')
            registrations = root / 'registrations'
            registrations.mkdir()
            (registrations / '001.json').write_text('{"slot":1}', 'utf-8')

            count, retired, before, after, used_at = (
                _consume_thread_pool_device(pool, root, 1, 10))

            self.assertEqual((count, retired, before, after), (10, True, 1, 0))
            self.assertTrue(used_at)
            self.assertFalse((slot / 'registered').exists())
            self.assertFalse((registrations / '001.json').exists())
            # The hot path is per slot; the legacy global file is left intact
            # and the zero tombstone overrides it until a replacement is born.
            self.assertEqual(json.loads((root / 'usage.json').read_text()), {'1': 9})
            counter = json.loads((root / 'usage-slots' / '001.json').read_text())
            self.assertEqual((counter['usage'], counter['retired']), (0, True))
            self.assertEqual(_usage_snapshot(root)[1], 0)

    def test_raw_apple_reason_is_packaged_only_when_debug_is_enabled(self):
        result = {'apple_message': 'Your account information was entered incorrectly.'}
        self.assertEqual(_apple_reason_for_record(result, 'wrong_password'), '')
        self.assertEqual(_apple_reason_for_record(result, 'wrong_password', True),
                         'Your account information was entered incorrectly.')

    def test_stall_budget_covers_all_retry_windows(self):
        self.assertEqual(stall_timeout_seconds({'timeout': 20, 'retries': 4}), 180)
        self.assertEqual(stall_timeout_seconds({'timeout': 60, 'retries': 4}), 360)

    def test_native_srp_preserves_total_concurrency_with_cpu_sized_process_pool(self):
        self.assertEqual(_auth_process_limit(10, rows=1000, cpu_count=4), 4)
        self.assertEqual(_auth_process_limit(10, rows=1000, cpu_count=16), 10)
        self.assertEqual(_auth_worker_limit(100, configured_processes=10,
                                            active_processes=4), 250)
        self.assertEqual(_auth_worker_limit(100, configured_processes=10,
                                            active_processes=2), 500)
        self.assertEqual(4 * _auth_worker_limit(100, 10, 4), 10 * 100)

    def test_device_id_uses_short_base64_label(self):
        self.assertEqual(_device_id_label('device-1'), 'B64:ZGV2aWNl')
        self.assertEqual(_device_id_label(''), '')

    def test_full_device_pool_is_partitioned_without_overlap(self):
        groups = [_partition_device_slots(range(1, 31), 10, index)
                  for index in range(10)]
        self.assertEqual(groups[0], [1, 11, 21])
        self.assertEqual(groups[9], [10, 20, 30])
        self.assertEqual(sorted(slot for group in groups for slot in group),
                         list(range(1, 31)))

    def test_account_hash_balances_and_exposes_base64_prefix(self):
        slots = list(range(1, 31))
        assignments = [_slot_for_account(f'user-{index:05d}@example.com', slots)
                       for index in range(3000)]
        counts = {slot: 0 for slot in slots}
        for slot, prefix in assignments:
            counts[slot] += 1
            self.assertRegex(prefix, r'^B64:[A-Za-z0-9_-]{8}$')
        self.assertLess(max(counts.values()) - min(counts.values()), 50)

    def test_auth_unit_does_not_bootstrap_or_request_without_proxy(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'input.part').write_text('account----password\n', encoding='utf-8')
            descriptor = {'rows': 1, 'first_row': 1, 'threads': 1,
                          'device_capacity': 1, 'device_root': str(root / 'devices'),
                          'origin': 'http://127.0.0.1:1', 'timeout': 5, 'retries': 0,
                          'proxy_api': 'http://proxy.invalid/list'}
            shared = ScriptedProxyPool([])
            with patch('clusterlab.executor.PROXY_EMPTY_GRACE_SECONDS', 0), \
                 patch('clusterlab.executor._load_fastcheck', return_value=(
                    FakePool, lambda *_args, **_kwargs: self.fail('request used direct connection'),
                    lambda result: result['status'])), \
                 patch('fastcheck.workers.device_profile',
                       return_value={'client_info': 'test', 'user_agent': 'test'}), \
                 patch('clusterlab.executor._bump_usage'):
                _execute_auth_unit(root, descriptor, threading.Event(), proxy_pool=shared)
            record = json.loads((root / 'results.jsonl').read_text())
            self.assertEqual(record['status'], 'timeout')
            self.assertEqual(record['device_key'][:4], 'B64:')
            self.assertFalse((root / 'devices' / 'registrations').exists())

    def test_auth_unit_waits_for_busy_exit_then_reuses_it(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'input.part').write_text('account----password\n', encoding='utf-8')
            descriptor = {'rows': 1, 'first_row': 1, 'threads': 1,
                          'device_capacity': 1, 'device_root': str(root / 'devices'),
                          'origin': 'http://127.0.0.1:1', 'timeout': 5, 'retries': 0,
                          'proxy_api': 'http://proxy.invalid/list'}
            shared = BusyThenReusableProxyPool()
            checked = []

            def check(_account, _password, proxy, **_kwargs):
                checked.append(proxy)
                return {'status': 'valid', 'kind': 'valid', 'http': 200}

            with patch('clusterlab.executor._load_fastcheck', return_value=(
                    FakePool, check, lambda result: result['status'])), \
                 patch('fastcheck.workers.device_profile',
                       return_value={'client_info': 'test', 'user_agent': 'test'}), \
                 patch('clusterlab.executor._bump_usage'):
                _execute_auth_unit(root, descriptor, threading.Event(), proxy_pool=shared)

            record = json.loads((root / 'results.jsonl').read_text())
            self.assertEqual(checked, ['socks5h://reused-after-busy'])
            self.assertEqual(record['status'], 'valid')
            self.assertEqual(record['attempts'], 0)
            self.assertEqual(shared.account_acquires, 2)

    def test_explicit_debug_setting_overrides_legacy_node_environment(self):
        with patch.dict(os.environ, {'CLUSTER_PROXY_DEBUG': '1'}):
            self.assertFalse(_proxy_debug_enabled({'_proxy_debug': False}))
            self.assertTrue(_proxy_debug_enabled({'_proxy_debug': True}))
            self.assertTrue(_proxy_debug_enabled({}))

    def test_node_proxy_pool_is_shared_across_spawned_processes(self):
        context = mp.get_context('spawn')
        pool = NodeProxyPool(context, concurrency=2, ttl=30)
        output = context.Queue()
        children = []
        try:
            pool.add_fresh(['socks5h://process-1', 'socks5h://process-2'])
            children = [context.Process(target=use_shared_proxy_pool, args=(pool, output))
                        for _index in range(2)]
            for child in children:
                child.start()
            for child in children:
                child.join(5)
            self.assertEqual([child.exitcode for child in children], [0, 0])
            self.assertEqual({output.get(timeout=1), output.get(timeout=1)},
                             {'socks5h://process-1', 'socks5h://process-2'})
            self.assertEqual(pool.snapshot()['reusable'], 2)
        finally:
            for child in children:
                if child.is_alive():
                    child.terminate()
                    child.join(1)
                child.close()
            output.cancel_join_thread()
            output.close()
            pool.close()

    def test_node_proxy_pool_prefers_fresh_then_reuses_recent_success(self):
        context = mp.get_context('spawn')
        pool = NodeProxyPool(context, concurrency=4, ttl=2)
        try:
            self.assertEqual(pool.add_fresh(['socks5h://fresh-1', 'socks5h://fresh-2']), 2)
            first = pool.acquire(wait=.5)
            self.assertEqual(first[0], 'socks5h://fresh-1')
            pool.release(first, healthy=True)
            second = pool.acquire(wait=.5)
            self.assertEqual(second[0], 'socks5h://fresh-2')
            reused = pool.acquire(wait=.5)
            self.assertEqual(reused[0], 'socks5h://fresh-1')
            self.assertEqual(reused[2], 1)
            self.assertEqual(pool.snapshot()['reused'], 1)
            pool.release(second, healthy=False)
            self.assertEqual(pool.snapshot()['discarded'], 0)
            self.assertEqual(pool.snapshot()['soft_failures'], 1)
            pool.release(reused, healthy=True)
        finally:
            pool.close()

    def test_node_proxy_pool_counts_busy_inventory_and_retires_after_three_failures(self):
        context = mp.get_context('spawn')
        pool = NodeProxyPool(context, concurrency=1, ttl=30, target=1)
        try:
            pool.add_fresh(['socks5h://unstable'])
            for failure in range(1, 4):
                lease = pool.acquire(wait=.5)
                self.assertIsNotNone(lease)
                busy = pool.snapshot()
                self.assertEqual(busy['available'], 1)
                self.assertEqual(busy['in_use'], 1)
                self.assertEqual(busy['known'], 1)
                pool.release(lease, healthy=False)
                state = pool.snapshot()
                self.assertEqual(state['soft_failures'], failure)
                self.assertEqual(state['discarded'], int(failure == 3))
            self.assertEqual(pool.snapshot()['known'], 0)
        finally:
            pool.close()

    def test_node_proxy_pool_immediate_discard_removes_exit_and_requests_refill(self):
        context = mp.get_context('spawn')
        pool = NodeProxyPool(context, concurrency=1, ttl=50, target=1)
        try:
            pool.add_fresh(['socks5h://http-error'])
            lease = pool.acquire(wait=.5)
            pool.refill.clear()
            pool.discard(lease)
            state = pool.snapshot()
            self.assertEqual(state['known'], 0)
            self.assertEqual(state['discarded'], 1)
            self.assertTrue(pool.refill.is_set())
            # The deleted endpoint remains quarantined for its original TTL,
            # while a different available endpoint can fill the empty slot.
            self.assertEqual(pool.add_fresh(['socks5h://http-error']), 0)
            self.assertEqual(pool.add_fresh(['socks5h://replacement']), 1)
        finally:
            pool.close()

    def test_node_proxy_pool_allows_concurrent_ip_reuse(self):
        context = mp.get_context('spawn')
        pool = NodeProxyPool(context, concurrency=2, ttl=50, target=2)
        try:
            pool.add_fresh(['socks5h://one', 'socks5h://two'])
            first = pool.acquire(wait=.5)
            second = pool.acquire(wait=.5)
            self.assertEqual({first[0], second[0]},
                             {'socks5h://one', 'socks5h://two'})
            third = pool.acquire(wait=.05)
            self.assertIn(third[0], {'socks5h://one', 'socks5h://two'})
            self.assertEqual(pool.snapshot()['known'], 2)
            self.assertEqual(pool.snapshot()['in_use'], 3)
            pool.release(first, healthy=True)
            pool.release(second, healthy=True)
            pool.release(third, healthy=True)
            self.assertEqual(pool.snapshot()['available'], 2)
        finally:
            pool.close()

    def test_twenty_ips_can_feed_one_thousand_inflight_requests(self):
        context = mp.get_context('spawn')
        pool = NodeProxyPool(context, concurrency=1000, ttl=50, target=1000)
        leases = []
        try:
            endpoints = [f'socks5h://shared-{index}' for index in range(20)]
            self.assertEqual(pool.add_fresh(endpoints), 20)
            for _ in range(1000):
                lease = pool.acquire(wait=.05)
                self.assertIsNotNone(lease)
                leases.append(lease)
            self.assertEqual(pool.snapshot()['available'], 20)
            self.assertEqual(pool.snapshot()['known'], 20)
            self.assertEqual(pool.snapshot()['in_use'], 1000)
            self.assertGreaterEqual(pool.snapshot()['reused'], 980)
            for lease in leases:
                pool.release(lease, healthy=True)
            self.assertEqual(pool.snapshot()['in_use'], 0)
        finally:
            pool.close()

    def test_node_proxy_pool_never_exceeds_configured_logical_capacity(self):
        context = mp.get_context('spawn')
        pool = NodeProxyPool(context, concurrency=2, ttl=30, target=2)
        try:
            pool.add_fresh(['socks5h://one', 'socks5h://two'])
            first = pool.acquire(wait=.5)
            second = pool.acquire(wait=.5)
            # IPs are shared, so an idle queue token remains rotatable even
            # while requests using the old endpoint are still in flight.
            self.assertEqual(pool.add_fresh(['socks5h://three']), 1)
            self.assertEqual(pool.snapshot()['known'], 2)
            pool.release(first, healthy=True)
            pool.release(second, healthy=True)
            self.assertEqual(pool.add_fresh(['socks5h://four']), 1)
            self.assertEqual(pool.snapshot()['known'], 2)
            self.assertEqual(pool.snapshot()['refreshed'], 2)
        finally:
            pool.close()

    def test_node_proxy_pool_discards_expired_exit(self):
        context = mp.get_context('spawn')
        pool = NodeProxyPool(context, concurrency=2, ttl=1)
        try:
            pool.add_fresh(['socks5h://stale'], fetched_at=time.monotonic() - 2)
            self.assertIsNone(pool.acquire(wait=.05))
            self.assertEqual(pool.snapshot()['discarded'], 1)
        finally:
            pool.close()

    def test_proxy_supplier_prefills_pool_and_reports_fetch_metrics(self):
        context = mp.get_context('spawn')
        pool = NodeProxyPool(context, concurrency=4, ttl=30)
        stop = threading.Event()
        calls = []

        def fetcher(_url, count, scheme=None):
            calls.append((count, scheme))
            return [f'{scheme}://127.0.0.{index}:9000' for index in range(1, count + 1)]

        thread = threading.Thread(target=proxy_supplier_loop,
                                  args=(pool, 'http://proxy.test/list', 'socks5h', stop, fetcher))
        thread.start()
        until = time.monotonic() + 2
        while pool.snapshot()['fresh'] < pool.target and time.monotonic() < until:
            time.sleep(.01)
        stop.set()
        pool.refill.set()
        thread.join(1)
        try:
            metrics = pool.snapshot()
            self.assertEqual(metrics['fresh'], pool.target)
            self.assertEqual(metrics['fetched'], pool.target)
            self.assertEqual(metrics['fetch_calls'], 1)
            self.assertEqual(calls, [(pool.target, 'socks5h')])
        finally:
            pool.close()

    def test_partial_proxy_batch_marks_pool_ready_for_reuse(self):
        context = mp.get_context('spawn')
        pool = NodeProxyPool(context, concurrency=1000, ttl=50, target=1000)
        try:
            self.assertEqual(pool.add_fresh([
                f'socks5h://partial-{index}' for index in range(200)
            ]), 200)
            self.assertTrue(pool.ready.is_set())
            self.assertEqual(pool.snapshot()['available'], 200)
            self.assertEqual(pool.snapshot()['target'], 1000)
        finally:
            pool.close()

    def test_ready_shared_pool_refills_in_small_batches(self):
        context = mp.get_context('spawn')
        pool = NodeProxyPool(context, concurrency=1000, ttl=50, target=1000)
        stop = threading.Event()
        calls = []

        def fetcher(_url, count, scheme=None):
            calls.append(count)
            stop.set()
            return [f'{scheme}://rolling-{index}' for index in range(count)]

        try:
            pool.add_fresh(['socks5h://existing'])
            proxy_supplier_loop(pool, 'http://proxy.test/list', 'socks5h',
                                stop, fetcher)
            self.assertEqual(calls, [999])
        finally:
            pool.close()

    def test_two_nodes_each_request_1000_then_reuse_sparse_provider_stock(self):
        context = mp.get_context('spawn')
        observed = []
        for node in (1, 2):
            pool = NodeProxyPool(context, concurrency=1000, ttl=50, target=1000)
            stop = threading.Event()
            calls = []

            def fetcher(_url, count, scheme=None, node=node):
                calls.append(count)
                return [f'{scheme}://node-{node}-only-exit:9000']

            thread = threading.Thread(
                target=proxy_supplier_loop,
                args=(pool, f'http://proxy.test/node-{node}', 'socks5h', stop, fetcher))
            thread.start()
            self.assertTrue(pool.ready.wait(1))
            stop.set()
            pool.refill.set()
            thread.join(1)
            try:
                first = pool.acquire(wait=.5)
                pool.release(first, healthy=True)
                reused = pool.acquire(wait=.5)
                observed.append((calls[0], first[0], reused[0], reused[2]))
                pool.release(reused, healthy=True)
            finally:
                pool.close()

        self.assertEqual(observed, [
            (1000, 'socks5h://node-1-only-exit:9000',
             'socks5h://node-1-only-exit:9000', 1),
            (1000, 'socks5h://node-2-only-exit:9000',
             'socks5h://node-2-only-exit:9000', 1),
        ])

    def test_retry_delay_queue_frees_single_worker_for_next_account(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'input.part').write_text(
                'first----password\nsecond----password\n', encoding='utf-8')
            descriptor = {'rows': 2, 'first_row': 1, 'threads': 1,
                          'device_capacity': 1, 'device_root': str(root / 'devices'),
                          'origin': 'http://127.0.0.1:1', 'timeout': 5, 'retries': 1,
                          'proxy_api': 'http://proxy.invalid/list', '_retry_delay': .05}
            shared = ScriptedProxyPool([
                'socks5h://bootstrap', 'socks5h://bad-first',
                'socks5h://second-account', 'socks5h://first-retry'])
            checked = []
            first_calls = 0

            def check(account, _password, proxy, **_kwargs):
                nonlocal first_calls
                checked.append((account, proxy))
                if account == 'first' and first_calls == 0:
                    first_calls += 1
                    return {'status': 'error', 'kind': 'network', 'http': 0}
                return {'status': 'valid', 'kind': 'valid', 'http': 200}

            with patch('clusterlab.executor._load_fastcheck', return_value=(
                    FakePool, check, lambda result: result['status'])), \
                 patch('fastcheck.workers.device_profile',
                       return_value={'client_info': 'test', 'user_agent': 'test'}), \
                 patch('clusterlab.executor._bump_usage'):
                _execute_auth_unit(root, descriptor, threading.Event(), proxy_pool=shared)

            records = [json.loads(line) for line in
                       (root / 'results.jsonl').read_text().splitlines()]
            by_account = {record['account']: record for record in records}
            self.assertEqual(checked, [
                ('first', 'socks5h://bad-first'),
                ('second', 'socks5h://second-account'),
                ('first', 'socks5h://first-retry'),
            ])
            self.assertEqual(by_account['first']['attempts'], 1)
            self.assertEqual(by_account['second']['attempts'], 0)
            self.assertEqual(shared.discards, ['socks5h://bad-first'])

    def test_one_time_mode_reuses_thread_pool_then_retires_at_limit(self):
        class OneTimePool(FakePool):
            registered_flags = []

            @property
            def base(self):
                return self.root

            def inspect(self, slot):
                marker = self.slot_directory(slot) / 'registered'
                return ({'state': 'ready', 'device_id':
                         '00000000-0000-0000-0000-000000000001'}
                        if marker.exists() else {'state': 'missing'})

            def fetch_headers(self, slot, *_args, **kwargs):
                type(self).registered_flags.append(bool(kwargs.get('registered')))
                directory = self.slot_directory(slot)
                directory.mkdir(parents=True, exist_ok=True)
                (directory / 'registered').write_text('kept', 'utf-8')
                return {'identity': 'thread-pool'}

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            device_root = root / 'devices'
            OneTimePool.registered_flags = []
            records = []
            releases = []
            first_registration = None

            def check(_account, _password, _proxy, **_kwargs):
                return {'status': 'valid', 'kind': 'valid', 'http': 200}

            for number in (1, 2):
                unit = root / f'unit-{number}'
                unit.mkdir()
                (unit / 'input.part').write_text(
                    f'account-{number}----password\n', encoding='utf-8')
                descriptor = {
                    'id': f'unit-{number}', 'rows': 1, 'first_row': number,
                    'threads': 1, 'device_pool_capacity': 1, 'device_slots': [1],
                    'device_root': str(device_root), 'origin': 'http://127.0.0.1:1',
                    'timeout': 5, 'retries': 0, '_device_mode': 'one_time',
                    '_retry_delay': 0, '_one_time_min_retries': 0,
                    'device_usage_limit': 2,
                }
                shared = ScriptedProxyPool([f'socks5h://exit-{number}'])
                with patch('clusterlab.executor._load_fastcheck', return_value=(
                        OneTimePool, check, lambda result: result['status'])), \
                     patch('fastcheck.workers.device_profile',
                           return_value={'client_info': 'test', 'user_agent': 'test',
                                         'model': 'ThreadPool'}):
                    _execute_auth_unit(unit, descriptor, threading.Event(),
                                       proxy_pool=shared)
                records.append(json.loads((unit / 'results.jsonl').read_text()))
                releases.extend(shared.releases)
                if number == 1:
                    first_registration = json.loads(
                        (device_root / 'registrations' / '001.json').read_text())

            pooled = device_root
            self.assertEqual(OneTimePool.registered_flags, [False, True])
            self.assertEqual([record['device_usage'] for record in records], [1, 2])
            self.assertEqual([record['device_remaining_before'] for record in records],
                             [2, 1])
            self.assertEqual([record['device_remaining'] for record in records], [1, 0])
            self.assertEqual([record['device_retired'] for record in records],
                             [False, True])
            self.assertTrue(records[0]['device_used_at'])
            self.assertEqual(first_registration['usage'], 1)
            self.assertEqual(first_registration['remaining'], 1)
            self.assertEqual(first_registration['last_used_at'],
                             records[0]['device_used_at'])
            self.assertFalse((pooled / '01' / 'registered').exists())
            self.assertFalse((pooled / 'registrations' / '001.json').exists())
            self.assertEqual(releases, [
                ('socks5h://exit-1', True), ('socks5h://exit-2', True)])

    def test_device_accounting_failure_still_unlocks_device_and_settles_proxy(self):
        class AccountingPool(FakePool):
            @property
            def base(self):
                return self.root

            def inspect(self, slot):
                marker = self.slot_directory(slot) / 'registered'
                return {'state': 'ready', 'device_id': str(slot)} if marker.exists() \
                    else {'state': 'missing'}

            def fetch_headers(self, slot, *_args, **_kwargs):
                directory = self.slot_directory(slot)
                directory.mkdir(parents=True, exist_ok=True)
                (directory / 'registered').write_text('ready', 'utf-8')
                return {'identity': 'accounting-test'}

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            device_root = root / 'devices'
            (root / 'input.part').write_text('account----password\n', encoding='utf-8')
            descriptor = {
                'rows': 1, 'first_row': 1, 'threads': 1,
                'device_pool_capacity': 1, 'device_slots': [1],
                'device_root': str(device_root), 'origin': 'http://127.0.0.1:1',
                'timeout': 5, 'retries': 0, '_device_mode': 'one_time',
                '_retry_delay': 0, '_one_time_min_retries': 0,
                'device_usage_limit': 10,
                'device_cooldown_seconds': 0,
            }
            shared = ScriptedProxyPool(['socks5h://one', 'socks5h://two'])
            with patch('clusterlab.executor._load_fastcheck', return_value=(
                    AccountingPool,
                    lambda *_args, **_kwargs: {
                        'status': 'valid', 'kind': 'valid', 'http': 200},
                    lambda result: result['status'])), \
                 patch('fastcheck.workers.device_profile', return_value={
                     'client_info': 'test', 'user_agent': 'test', 'model': 'test'}), \
                 patch('clusterlab.executor._consume_thread_pool_device',
                       side_effect=TimeoutError('simulated usage contention')):
                _execute_auth_unit(root, descriptor, threading.Event(), proxy_pool=shared)

            done = json.loads((root / 'done.json').read_text())
            self.assertEqual((done['ok'], done['processed']), (True, 1))
            self.assertEqual(shared.discards, ['socks5h://one', 'socks5h://two'])
            # The bookkeeping exception must not strand the device lock.
            lease = DeviceAuthLease.try_acquire(AccountingPool(device_root), 1, cooldown=0)
            self.assertIsNotNone(lease)
            lease.release()

    def test_thread_mode_uses_device_profile_without_pool_info_request(self):
        class InfoPool(FakePool):
            info_calls = 0

            @property
            def base(self):
                return self.root

            def info(self, *_args, **_kwargs):
                type(self).info_calls += 1
                time.sleep(.02)
                return {'client_info': 'cached', 'user_agent': 'cached'}

            def inspect(self, slot):
                marker = self.slot_directory(slot) / 'registered'
                return {'state': 'ready', 'device_id': str(slot)} if marker.exists() \
                    else {'state': 'missing'}

            def fetch_headers(self, *_args, **_kwargs):
                return {'identity': 'shared-info'}

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            device_root = root / 'devices'
            for slot in (1, 2):
                directory = device_root / f'{slot:02d}'
                directory.mkdir(parents=True)
                (directory / 'registered').write_text('ready', 'utf-8')
            (root / 'input.part').write_text(
                'one----password\ntwo----password\n', encoding='utf-8')
            descriptor = {
                'rows': 2, 'first_row': 1, 'threads': 2,
                'device_pool_capacity': 2, 'device_slots': [1, 2],
                'device_root': str(device_root), 'origin': 'http://127.0.0.1:1',
                'timeout': 5, 'retries': 0, '_device_mode': 'one_time',
                '_one_time_min_retries': 0, 'device_usage_limit': 10,
                'device_cooldown_seconds': 0,
            }
            InfoPool.info_calls = 0
            shared = ScriptedProxyPool(['socks5h://one', 'socks5h://two'])
            with patch('clusterlab.executor._load_fastcheck', return_value=(
                    InfoPool,
                    lambda *_args, **_kwargs: {
                        'status': 'valid', 'kind': 'valid', 'http': 200},
                    lambda result: result['status'])), \
                 patch('fastcheck.workers.device_profile',
                       return_value={'client_info': 'test', 'user_agent': 'test',
                                     'model': 'SharedInfo'}):
                _execute_auth_unit(root, descriptor, threading.Event(), proxy_pool=shared)
            self.assertEqual(InfoPool.info_calls, 0)
            self.assertEqual(len((root / 'results.jsonl').read_text().splitlines()), 2)

    def test_auth_unit_rotates_proxy_after_http_zero_and_reuses_http_200(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'input.part').write_text('account----password\n', encoding='utf-8')
            descriptor = {'rows': 1, 'first_row': 1, 'threads': 1,
                          'device_capacity': 1, 'device_root': str(root / 'devices'),
                          'origin': 'http://127.0.0.1:1', 'timeout': 5, 'retries': 1,
                          '_retry_delay': 0}
            shared = ScriptedProxyPool([
                'socks5h://bootstrap', 'socks5h://dead-exit', 'socks5h://good-exit'])
            stop = RecordingStop()
            checked = []

            def check(_account, _password, proxy, **_kwargs):
                checked.append(proxy)
                if proxy == 'socks5h://dead-exit':
                    return {'status': 'error', 'kind': 'network', 'http': 0}
                return {'status': 'valid', 'kind': 'valid', 'http': 200}

            with patch('clusterlab.executor._load_fastcheck', return_value=(
                    FakePool, check, lambda result: result['status'])), \
                 patch('fastcheck.workers.device_profile',
                       return_value={'client_info': 'test', 'user_agent': 'test'}), \
                 patch('clusterlab.executor._bump_usage'):
                _execute_auth_unit(root, descriptor, stop, proxy_pool=shared)

            record = json.loads((root / 'results.jsonl').read_text())
            self.assertEqual(checked, ['socks5h://dead-exit', 'socks5h://good-exit'])
            self.assertEqual(record['status'], 'valid')
            self.assertEqual(record['attempts'], 1)
            self.assertEqual(shared.releases, [
                ('socks5h://bootstrap', True),
                ('socks5h://good-exit', True),
            ])
            self.assertEqual(shared.discards, ['socks5h://dead-exit'])
            self.assertEqual(stop.waits, [])

    def test_auth_unit_discards_http_503_waits_then_rotates_proxy(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'input.part').write_text('account----password\n', encoding='utf-8')
            descriptor = {'rows': 1, 'first_row': 1, 'threads': 1,
                          'device_capacity': 1, 'device_root': str(root / 'devices'),
                          'origin': 'http://127.0.0.1:1', 'timeout': 5, 'retries': 1,
                          'proxy_api': 'http://proxy.invalid/list', '_retry_delay': 0}
            shared = ScriptedProxyPool([
                'socks5h://bootstrap', 'socks5h://service-busy', 'socks5h://good-exit'])
            stop = RecordingStop()
            checked = []

            def check(_account, _password, proxy, **_kwargs):
                checked.append(proxy)
                if proxy == 'socks5h://service-busy':
                    return {'status': 'error', 'kind': 'service', 'http': 503}
                return {'status': 'invalid', 'kind': 'invalid', 'http': 200}

            with patch('clusterlab.executor._load_fastcheck', return_value=(
                    FakePool, check,
                    lambda result: result['status'])), \
                 patch('fastcheck.workers.device_profile',
                       return_value={'client_info': 'test', 'user_agent': 'test'}), \
                 patch('clusterlab.executor._bump_usage'):
                _execute_auth_unit(root, descriptor, stop, proxy_pool=shared)

            record = json.loads((root / 'results.jsonl').read_text())
            self.assertEqual(checked, ['socks5h://service-busy', 'socks5h://good-exit'])
            self.assertEqual(record['status'], 'invalid')
            self.assertEqual(record['attempts'], 1)
            self.assertEqual(shared.releases, [
                ('socks5h://bootstrap', True),
                ('socks5h://good-exit', True),
            ])
            self.assertEqual(shared.discards, ['socks5h://service-busy'])
            self.assertEqual(stop.waits, [])

    def test_auth_unit_retries_22421_with_fresh_identity_and_keeps_apple_reason(self):
        class RefreshingPool(FakePool):
            fetch_count = 0

            def fetch_headers(self, *_args, **_kwargs):
                type(self).fetch_count += 1
                return {'generation': type(self).fetch_count}

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'input.part').write_text('account----password\n', encoding='utf-8')
            descriptor = {'rows': 1, 'first_row': 1, 'threads': 1,
                          'device_capacity': 1, 'device_root': str(root / 'devices'),
                          'origin': 'http://127.0.0.1:1', 'timeout': 5, 'retries': 1,
                          '_retry_delay': 0, '_proxy_debug': True}
            shared = ScriptedProxyPool([
                'socks5h://bootstrap', 'socks5h://first-exit', 'socks5h://second-exit'])
            stop = RecordingStop()
            checked = []

            def check(_account, _password, proxy, **kwargs):
                checked.append((proxy, kwargs['identity']['generation']))
                if proxy == 'socks5h://first-exit':
                    return {'status': 'error', 'kind': 'protocol', 'stage': 'init',
                            'http': 200, 'apple_ec': -22421,
                            'detail': 'ec=-22421',
                            'apple_message': 'This action could not be completed. Try again.'}
                return {'status': 'need_2fa', 'kind': 'valid', 'stage': 'complete',
                        'http': 200, 'detail': '密码证明通过，服务要求二次验证',
                        'auth_type': 'trustedDeviceSecondaryAuth'}

            with patch('clusterlab.executor._load_fastcheck', return_value=(
                    RefreshingPool, check,
                    lambda result: ('timeout' if result.get('apple_ec') == -22421
                                    else result['status']))), \
                 patch('fastcheck.workers.device_profile',
                       return_value={'client_info': 'test', 'user_agent': 'test'}), \
                 patch('clusterlab.executor._bump_usage'):
                _execute_auth_unit(root, descriptor, stop, proxy_pool=shared)

            record = json.loads((root / 'results.jsonl').read_text())
            self.assertEqual(checked, [
                ('socks5h://first-exit', 1), ('socks5h://second-exit', 2)])
            self.assertEqual((record['status'], record['attempts']), ('need_2fa', 1))
            self.assertEqual(record['reason'], 'au=trustedDeviceSecondaryAuth')
            self.assertEqual(shared.releases, [
                ('socks5h://bootstrap', True),
                ('socks5h://second-exit', True),
            ])
            self.assertEqual(shared.discards, ['socks5h://first-exit'])
            self.assertEqual(stop.waits, [])

    def test_debug_sample_records_phase_and_device_timings_without_credentials(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            # Row 20 is the deterministic five-percent performance sample.
            (root / 'input.part').write_text(
                'account----password\n', encoding='utf-8')
            descriptor = {'rows': 1, 'first_row': 20, 'threads': 1,
                          'device_capacity': 1,
                          'device_root': str(root / 'devices'),
                          'origin': 'http://127.0.0.1:1',
                          'timeout': 5, 'retries': 0,
                          '_proxy_debug': True}
            shared = ScriptedProxyPool(
                ['socks5h://bootstrap', 'socks5h://account-exit'])
            stop = RecordingStop()

            def check(_account, _password, _proxy, **kwargs):
                diagnostics = kwargs.get('diagnostics')
                self.assertIsNotNone(diagnostics)
                diagnostics({'event': 'phase_end', 'phase': 'srp_challenge',
                             'ms': 12.5, 'cpu_ms': 7.25})
                diagnostics({'event': 'phase_end', 'phase': 'http_complete',
                             'ms': 45.0, 'cpu_ms': 1.0})
                return {'status': 'wrong_password', 'stage': 'complete',
                        'http': 200, 'apple_ec': -22406,
                        'detail': 'synthetic wrong password'}

            with patch('clusterlab.executor._load_fastcheck', return_value=(
                    FakePool, check, lambda result: result['status'])), \
                 patch('fastcheck.workers.device_profile',
                       return_value={'client_info': 'test', 'user_agent': 'test'}), \
                 patch('clusterlab.executor._bump_usage'):
                _execute_auth_unit(root, descriptor, stop, proxy_pool=shared)

            events = [json.loads(line) for line in
                      (root / 'proxy-debug.jsonl').read_text().splitlines()]
            timing = next(item for item in events
                          if item.get('event') == 'auth_timing')
            self.assertTrue(timing['sampled'])
            self.assertEqual(timing['phase_ms'], {
                'http_complete': 45.0, 'srp_challenge': 12.5})
            self.assertEqual(timing['phase_cpu_ms'], {
                'http_complete': 1.0, 'srp_challenge': 7.25})
            self.assertIn('device_lease_ms', timing)
            self.assertIn('device_identity_ms', timing)
            serialized = json.dumps(timing)
            self.assertNotIn('account----password', serialized)
            self.assertNotIn('socks5h://account-exit', serialized)

    def test_auth_unit_uses_configured_two_retries_for_apple_try_again_later(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'input.part').write_text('account----password\n', encoding='utf-8')
            descriptor = {'rows': 1, 'first_row': 1, 'threads': 1,
                          'device_capacity': 1, 'device_root': str(root / 'devices'),
                          'origin': 'http://127.0.0.1:1', 'timeout': 5, 'retries': 2,
                          '_retry_delay': 0, '_proxy_debug': True}
            shared = ScriptedProxyPool([
                'socks5h://bootstrap', 'socks5h://first-exit',
                'socks5h://second-exit', 'socks5h://third-exit'])
            stop = RecordingStop()
            checked = []
            reason = 'Unable to sign you in to your Apple Account. Try again later.'

            def check(_account, _password, proxy, **_kwargs):
                checked.append(proxy)
                return {'status': 'error', 'kind': 'protocol', 'stage': 'complete',
                        'http': 200, 'apple_ec': -1, 'detail': f'ec=-1 {reason}',
                        'apple_message': reason}

            with patch('clusterlab.executor._load_fastcheck', return_value=(
                    FakePool, check,
                    lambda result: ('timeout' if reason.casefold() in
                                    str(result.get('apple_message') or '').casefold()
                                    else 'undetermined'))), \
                 patch('fastcheck.workers.device_profile',
                       return_value={'client_info': 'test', 'user_agent': 'test'}), \
                 patch('clusterlab.executor._bump_usage'):
                _execute_auth_unit(root, descriptor, stop, proxy_pool=shared)

            record = json.loads((root / 'results.jsonl').read_text())
            self.assertEqual(checked, [
                'socks5h://first-exit', 'socks5h://second-exit', 'socks5h://third-exit'])
            self.assertEqual((record['status'], record['attempts']), ('timeout', 2))
            self.assertEqual(record['reason'], reason)
            self.assertEqual(stop.waits, [])
            self.assertEqual(shared.discards, [
                'socks5h://first-exit', 'socks5h://second-exit', 'socks5h://third-exit'])

    def test_auth_unit_retries_transient_apple_codes_with_new_ip_and_identity(self):
        class RefreshingPool(FakePool):
            fetch_count = 0

            def fetch_headers(self, *_args, **_kwargs):
                type(self).fetch_count += 1
                return {'generation': type(self).fetch_count}

        for code in (-332000, -33200, -3000303, -22410):
            with self.subTest(code=code), tempfile.TemporaryDirectory() as temporary:
                RefreshingPool.fetch_count = 0
                root = Path(temporary)
                (root / 'input.part').write_text('account----password\n', encoding='utf-8')
                descriptor = {'rows': 1, 'first_row': 1, 'threads': 1,
                              'device_capacity': 1, 'device_root': str(root / 'devices'),
                              'origin': 'http://127.0.0.1:1', 'timeout': 5, 'retries': 1,
                              '_retry_delay': 0}
                shared = ScriptedProxyPool([
                    'socks5h://bootstrap', 'socks5h://first-exit', 'socks5h://second-exit'])
                stop = RecordingStop()
                checked = []

                def check(_account, _password, proxy, **kwargs):
                    checked.append((proxy, kwargs['identity']['generation']))
                    if proxy == 'socks5h://first-exit':
                        return {'status': 'error', 'kind': 'protocol', 'stage': 'init',
                                'http': 200, 'apple_ec': code,
                                'apple_message': 'Error Description not available'}
                    return {'status': 'need_2fa', 'kind': 'valid', 'stage': 'complete',
                            'http': 200, 'auth_type': 'trustedDeviceSecondaryAuth'}

                with patch('clusterlab.executor._load_fastcheck', return_value=(
                        RefreshingPool, check,
                        lambda result: ('timeout' if result.get('apple_ec') in
                                        (-332000, -33200, -3000303, -22410)
                                        else result['status']))), \
                     patch('fastcheck.workers.device_profile',
                           return_value={'client_info': 'test', 'user_agent': 'test'}), \
                     patch('clusterlab.executor._bump_usage'):
                    _execute_auth_unit(root, descriptor, stop, proxy_pool=shared)

                record = json.loads((root / 'results.jsonl').read_text())
                self.assertEqual(checked, [
                    ('socks5h://first-exit', 1), ('socks5h://second-exit', 2)])
                self.assertEqual((record['status'], record['attempts']), ('need_2fa', 1))
                self.assertEqual(shared.discards, ['socks5h://first-exit'])
                self.assertEqual(shared.releases, [
                    ('socks5h://bootstrap', True), ('socks5h://second-exit', True)])
                self.assertEqual(stop.waits, [])

    def test_auth_unit_retries_apple_server_error_reasons_with_new_ip(self):
        reasons = (
            'This request could not be completed because of a server error.',
            'This Action Cannot Be Completed',
            'This action could not be completed. Try again.',
            'The request cannot be processed at this time. Please try again later.',
            'We cannot process your request, please try again later.',
            'Your account information was entered incorrectly.',
        )
        for reason in reasons:
            with self.subTest(reason=reason), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                (root / 'input.part').write_text('account----password\n', encoding='utf-8')
                descriptor = {'rows': 1, 'first_row': 1, 'threads': 1,
                              'device_capacity': 1, 'device_root': str(root / 'devices'),
                              'origin': 'http://127.0.0.1:1', 'timeout': 5, 'retries': 1,
                              '_retry_delay': 0, '_proxy_debug': True}
                shared = ScriptedProxyPool([
                    'socks5h://bootstrap', 'socks5h://first-exit', 'socks5h://second-exit'])
                stop = RecordingStop()
                checked = []

                def check(_account, _password, proxy, **_kwargs):
                    checked.append(proxy)
                    return {'status': 'error', 'kind': 'protocol', 'stage': 'complete',
                            'http': 200, 'apple_ec': -1, 'detail': reason,
                            'apple_message': reason}

                with patch('clusterlab.executor._load_fastcheck', return_value=(
                        FakePool, check, lambda _result: 'timeout')), \
                     patch('fastcheck.workers.device_profile',
                           return_value={'client_info': 'test', 'user_agent': 'test'}), \
                     patch('clusterlab.executor._bump_usage'):
                    _execute_auth_unit(root, descriptor, stop, proxy_pool=shared)

                record = json.loads((root / 'results.jsonl').read_text())
                self.assertEqual(checked, ['socks5h://first-exit', 'socks5h://second-exit'])
                expected = ('wrong_password'
                            if reason == 'Your account information was entered incorrectly.'
                            else 'timeout')
                self.assertEqual((record['status'], record['attempts']), (expected, 1))
                self.assertEqual(record['reason'], reason)
                self.assertEqual(stop.waits, [])
                self.assertEqual(shared.discards,
                                 ['socks5h://first-exit', 'socks5h://second-exit'])

    def test_registration_history_keeps_the_first_time_and_proxy_per_slot(self):
        with tempfile.TemporaryDirectory() as temporary:
            self.assertTrue(record_device_registration(temporary, 3, 'socks5h://first:9000',
                                                       'MacBookPro14,3'))
            self.assertFalse(record_device_registration(temporary, 3, 'socks5h://later:9001',
                                                        'iMac17,1'))
            value = json.loads((Path(temporary) / 'registrations' / '003.json').read_text())
            self.assertEqual(value['proxy'], 'socks5h://first:9000')
            self.assertEqual(value['model'], 'MacBookPro14,3')
            self.assertRegex(value['at'], r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$')

    def test_one_package_fans_out_to_processes_and_merges_in_row_order(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            lines = [f'row-{row:04d}' for row in range(123)]
            (root / 'input.part').write_text('\n'.join(lines) + '\n', encoding='utf-8')
            descriptor = {'rows': len(lines), 'first_row': 7001, 'processes': 4,
                          'threads': 3, 'mode': 'digest'}
            stop = mp.get_context('spawn').Event()
            execute_unit_group(root, descriptor, stop)
            done = json.loads((root / 'done.json').read_text('utf-8'))
            progress = json.loads((root / 'progress.json').read_text('utf-8'))
            records = [json.loads(line) for line in
                       (root / 'results.jsonl').read_text('utf-8').splitlines()]
            self.assertTrue(done['ok'], done)
            self.assertEqual(done['workers'], 4)
            self.assertEqual(progress['worker_limit'], 4)
            self.assertEqual(progress['counts'], {'valid': 123})
            self.assertEqual([record['row'] for record in records], list(range(7001, 7124)))
            self.assertEqual([record['account'] for record in records], lines)
            self.assertEqual(done['checksum'], hashlib.sha256(
                (root / 'results.jsonl').read_bytes()).hexdigest())

    def test_auth_unit_bounds_futures_hashes_incrementally_and_batches_usage_write(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            lines = [f'account-{row:05d}----password-{row:05d}' for row in range(500)]
            (root / 'input.part').write_text('\n'.join(lines) + '\n', encoding='utf-8')
            usage_calls = []

            def check(*_args, **_kwargs):
                return {'status': 'valid'}

            descriptor = {'rows': len(lines), 'first_row': 1, 'threads': 4,
                          'device_capacity': 2, 'device_root': str(root / 'devices'),
                          'origin': 'http://127.0.0.1:1', 'timeout': 5}
            with patch('clusterlab.executor._load_fastcheck',
                       return_value=(FakePool, check, lambda result: result['status'])), \
                 patch('fastcheck.workers.device_profile',
                       return_value={'client_info': 'test', 'user_agent': 'test'}), \
                 patch('clusterlab.executor._bump_usage',
                       side_effect=lambda _root, counts: usage_calls.append(dict(counts))), \
                 patch.object(Path, 'read_bytes', side_effect=AssertionError('must hash while writing')):
                _execute_auth_unit(root, descriptor, threading.Event())

            done = json.loads((root / 'done.json').read_text('utf-8'))
            payload = (root / 'results.jsonl').read_bytes()
            self.assertEqual(done['processed'], 500)
            self.assertEqual(json.loads((root / 'progress.json').read_text('utf-8'))['counts'],
                             {'valid': 500})
            self.assertEqual(done['checksum'], hashlib.sha256(payload).hexdigest())
            self.assertEqual(sum(usage_calls[0].values()), 500)
            self.assertLess(abs(usage_calls[0][1] - usage_calls[0][2]), 50)
            self.assertEqual(len(payload.splitlines()), 500)

    def test_auth_progress_is_time_based_instead_of_waiting_for_ten_results(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'input.part').write_text('account----password\n', encoding='utf-8')
            descriptor = {'rows': 1, 'first_row': 1, 'threads': 1,
                          'device_capacity': 1, 'device_root': str(root / 'devices'),
                          'origin': 'http://127.0.0.1:1', 'timeout': 5}
            with patch('clusterlab.executor._load_fastcheck', return_value=(
                    FakePool, lambda *_args, **_kwargs: {'status': 'wrong_password'},
                    lambda result: result['status'])), \
                 patch('fastcheck.workers.device_profile',
                       return_value={'client_info': 'test', 'user_agent': 'test'}), \
                 patch('clusterlab.executor._bump_usage'), \
                 patch('clusterlab.executor.time.monotonic', side_effect=[10.0, 10.0, 10.3]), \
                 patch('clusterlab.executor.atomic_json', wraps=atomic_json) as writes:
                _execute_auth_unit(root, descriptor, threading.Event())

            progress = [call.args[1] for call in writes.call_args_list
                        if call.args[0].name == 'progress.json']
            self.assertEqual([item['processed'] for item in progress], [1, 1])
            self.assertEqual(progress[0]['counts'], {'wrong_password': 1})

    def test_auth_result_records_device_usage_at_that_account(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'input.part').write_text(
                'one@example.com----a\ntwo@example.com----b\nthree@example.com----c\n',
                encoding='utf-8')
            devices = root / 'devices'
            devices.mkdir()
            (devices / 'usage.json').write_text('{"1":7}', encoding='utf-8')
            descriptor = {'rows': 3, 'first_row': 1, 'threads': 1,
                          'device_capacity': 1, 'device_root': str(devices),
                          'origin': 'http://127.0.0.1:1', 'timeout': 5}
            with patch('clusterlab.executor._load_fastcheck', return_value=(
                    FakePool, lambda *_args, **_kwargs: {'status': 'valid'},
                    lambda result: result['status'])), \
                 patch('fastcheck.workers.device_profile',
                       return_value={'client_info': 'test', 'user_agent': 'test'}), \
                 patch('clusterlab.executor._bump_usage'):
                _execute_auth_unit(root, descriptor, threading.Event())

            records = [json.loads(line) for line in
                       (root / 'results.jsonl').read_text('utf-8').splitlines()]
            self.assertEqual([record['device_usage'] for record in records], [8, 9, 10])

    def test_auth_unit_classifies_gsa_20970_as_invalid_without_web_recheck(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'input.part').write_text('account----password\n', encoding='utf-8')
            descriptor = {'rows': 1, 'first_row': 1, 'threads': 1,
                          'device_capacity': 1, 'device_root': str(root / 'devices'),
                          'origin': 'http://127.0.0.1:1', 'timeout': 5,
                          'dual_recheck': True, '_proxy_debug': True}

            with patch('clusterlab.executor._load_fastcheck', return_value=(
                    FakePool,
                    lambda *_args, **_kwargs: {'status': 'error', 'kind': 'protocol',
                                               'stage': 'init', 'http': 200,
                                               'apple_ec': -20970, 'ms': 5,
                                               'apple_message': 'Apple init refused this request'},
                    lambda result: 'invalid' if result.get('apple_ec') == -20970 else 'undetermined')), \
                 patch('fastcheck.workers.device_profile',
                       return_value={'client_info': 'test', 'user_agent': 'test'}), \
                 patch('clusterlab.executor._bump_usage'):
                _execute_auth_unit(root, descriptor, threading.Event())

            record = json.loads((root / 'results.jsonl').read_text())
            self.assertEqual(record['status'], 'invalid')
            self.assertEqual(record['protocol'], 'GSA')
            self.assertEqual((record['http'], record['code']), (200, '-20970'))
            self.assertEqual(record['reason'], 'Apple init refused this request')
            self.assertNotIn('gsa_code', record)
            self.assertNotIn('web_code', record)
            done = json.loads((root / 'done.json').read_text())
            self.assertEqual(done['counts'], {'invalid': 1})


if __name__ == '__main__':
    unittest.main()
