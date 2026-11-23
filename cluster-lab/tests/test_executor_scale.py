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

from clusterlab.executor import (DeviceAuthLease, DeviceLeaseAllocator, NodeProxyPool, _auth_process_limit, _auth_worker_limit, _cached_device_identity, _consume_device_use, _device_id_label, _effective_auth_workers, _execute_auth_unit, _local_queue_refill_count, _partition_device_slots, _reset_incomplete_device_slots,
                                 _apple_reason_for_record, _proxy_debug_enabled, _slot_for_account, atomic_json,
                                 execute_unit_group, proxy_supplier_loop,
                                 record_device_registration, stall_timeout_seconds)


class FakePool:
    def __init__(self, root, *_args, **_kwargs):
        self.root = Path(root)

    def slot_directory(self, slot):
        return self.root / 'fake-devices' / f'{int(slot):03d}'

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
    def test_new_task_resets_only_incomplete_assigned_slots(self):
        class MixedPool(FakePool):
            states = {1: 'ready', 2: 'pending', 3: 'invalid', 4: 'missing'}

            def inspect(self, slot):
                return {'state': self.states[int(slot)]}

        with tempfile.TemporaryDirectory() as temporary:
            pool = MixedPool(temporary)
            for slot in range(1, 5):
                directory = pool.slot_directory(slot)
                directory.mkdir(parents=True)
                (directory / 'identity.json').write_text(f'slot-{slot}')
                (directory / '.lease-state.json').write_text('{}')
            reset = _reset_incomplete_device_slots(pool, [1, 2, 3, 4])
            self.assertEqual(reset, [2, 3])
            self.assertTrue((pool.slot_directory(1) / 'identity.json').exists())
            self.assertFalse((pool.slot_directory(2) / 'identity.json').exists())
            self.assertFalse((pool.slot_directory(3) / '.lease-state.json').exists())
            self.assertTrue((pool.slot_directory(4) / 'identity.json').exists())

    def test_local_queue_refills_at_low_water_to_three_hundred(self):
        self.assertEqual(_local_queue_refill_count(1000, 1100, 100), 200)
        self.assertEqual(_local_queue_refill_count(1000, 1080, 80), 220)
        self.assertEqual(_local_queue_refill_count(1000, 1260, 260), 0)
        self.assertEqual(_local_queue_refill_count(1000, 0, 0), 1300)

    def test_device_identity_cache_expires_after_thirty_seconds(self):
        identities = {7: {'X-Apple-I-MD': 'cached'}}
        fetched_at = {7: 100.0}
        self.assertIs(_cached_device_identity(identities, fetched_at, 7, 129.999),
                      identities[7])
        self.assertIsNone(_cached_device_identity(identities, fetched_at, 7, 130.0))
        self.assertIsNone(_cached_device_identity(identities, {}, 7, 101.0))

    def test_device_lease_is_exclusive_and_writes_return_timestamp(self):
        with tempfile.TemporaryDirectory() as temporary:
            pool = FakePool(temporary)
            stop = threading.Event()
            first = DeviceAuthLease.acquire_any(pool, [1], stop)
            acquired = []

            def second_user():
                lease = DeviceAuthLease.acquire_any(pool, [1], stop)
                acquired.append(lease.slot)
                lease.release()

            thread = threading.Thread(target=second_user)
            thread.start()
            time.sleep(.05)
            self.assertEqual(acquired, [])
            first.release()
            thread.join(1)
            self.assertEqual(acquired, [1])
            state = json.loads((pool.slot_directory(1) / '.lease-state.json').read_text())
            self.assertEqual(state['state'], 'idle')
            self.assertIsInstance(state['last_used_at'], float)
            self.assertTrue(state['last_used'])

    def test_empty_pool_registers_distinct_devices_inside_worker_threads(self):
        class EmptyPool(FakePool):
            ready = set()
            registered = []
            registration_barrier = None

            def inspect(self, slot):
                return {'state': 'ready' if int(slot) in type(self).ready else 'missing',
                        'device_id': f'device-{slot}' if int(slot) in type(self).ready else ''}

            def fetch_headers(self, slot, *_args, **_kwargs):
                type(self).registration_barrier.wait(1)
                type(self).ready.add(int(slot))
                type(self).registered.append(int(slot))
                return {'slot': int(slot)}

        EmptyPool.ready = set()
        EmptyPool.registered = []
        EmptyPool.registration_barrier = threading.Barrier(2)
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'input.part').write_text(
                'one@example.com----a\ntwo@example.com----b\nthree@example.com----c\n',
                encoding='utf-8')
            descriptor = {'rows': 3, 'first_row': 1, 'threads': 2,
                          'device_capacity': 3, 'device_slots': [1, 2, 3],
                          'device_root': str(root / 'devices'),
                          'origin': 'http://127.0.0.1:1', 'timeout': 5}
            barrier = threading.Barrier(2)
            used = []

            def check(_account, _password, _proxy, **kwargs):
                used.append(kwargs['identity']['slot'])
                barrier.wait(1)
                return {'status': 'valid', 'http': 200, 'stage': 'complete'}

            with patch('clusterlab.executor._load_fastcheck', return_value=(
                    EmptyPool, check, lambda result: result['status'])), \
                 patch('fastcheck.workers.device_profile',
                       return_value={'client_info': 'test', 'user_agent': 'test'}), \
                 patch('clusterlab.executor.record_device_registration'), \
                 patch('clusterlab.executor._bump_usage'):
                _execute_auth_unit(root, descriptor, threading.Event())

            self.assertEqual(sorted(EmptyPool.registered), [1, 2])
            self.assertEqual(len(used),3)
            self.assertEqual(set(used),{1,2})
            for slot in (1, 2):
                state = json.loads(
                    (EmptyPool(root / 'devices').slot_directory(slot) /
                     '.lease-state.json').read_text())
                self.assertEqual(state['state'], 'idle')
                self.assertTrue(state['last_used_at'])

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

    def test_four_processes_with_thirty_slots_start_only_thirty_threads(self):
        groups = [_partition_device_slots(range(1, 31), 4, index)
                  for index in range(4)]
        self.assertEqual([len(group) for group in groups], [8, 8, 7, 7])
        workers = [_effective_auth_workers(250, group) for group in groups]
        self.assertEqual(workers, [8, 8, 7, 7])
        self.assertEqual(sum(workers), 30)

    def test_four_processes_with_two_hundred_pool_slots_start_two_hundred_threads(self):
        groups = [_partition_device_slots(range(1, 201), 4, index)
                  for index in range(4)]
        workers = [_effective_auth_workers(250, group) for group in groups]
        self.assertEqual(workers, [50, 50, 50, 50])
        self.assertEqual(sum(workers), 200)

    def test_device_use_is_atomic_and_never_exceeds_limit(self):
        with tempfile.TemporaryDirectory() as temporary:
            values = [_consume_device_use(temporary, 3, 20) for _ in range(20)]
            self.assertEqual(values, list(range(1, 21)))
            with self.assertRaisesRegex(RuntimeError, 'reached use limit 20'):
                _consume_device_use(temporary, 3, 20)
            usage = json.loads((Path(temporary) / 'usage.json').read_text())
            self.assertEqual(usage['3'], 20)

    def test_twentieth_request_retires_identity_and_next_request_registers_fresh(self):
        class LifecyclePool(FakePool):
            registrations = 0

            def inspect(self, slot):
                marker = self.slot_directory(slot) / 'identity.json'
                return {'state': 'ready' if marker.exists() else 'missing',
                        'device_id': f'device-{slot}' if marker.exists() else ''}

            def fetch_headers(self, slot, *_args, registered=True, **_kwargs):
                directory = self.slot_directory(slot)
                directory.mkdir(parents=True, exist_ok=True)
                (directory / 'identity.json').write_text('{}')
                if not registered:
                    type(self).registrations += 1
                return {'slot': slot}

        LifecyclePool.registrations = 0
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            device_root = root / 'devices'
            pool = LifecyclePool(device_root)
            pool.slot_directory(1).mkdir(parents=True)
            (pool.slot_directory(1) / 'identity.json').write_text('{}')
            lines = [f'user-{index}@example.com----password' for index in range(21)]
            (root / 'input.part').write_text('\n'.join(lines) + '\n')
            descriptor = {'rows': 21, 'first_row': 1, 'threads': 1,
                          'device_capacity': 1, 'device_slots': [1],
                          'device_use_limit': 20, 'device_root': str(device_root),
                          'origin': 'http://127.0.0.1:1', 'timeout': 5}
            with patch('clusterlab.executor._load_fastcheck', return_value=(
                    LifecyclePool, lambda *_args, **_kwargs: {'status': 'valid'},
                    lambda result: result['status'])), \
                 patch('fastcheck.workers.device_profile',
                       return_value={'client_info': 'test', 'user_agent': 'test'}), \
                 patch('clusterlab.executor.record_device_registration'):
                _execute_auth_unit(root, descriptor, threading.Event())
            records = [json.loads(line) for line in
                       (root / 'results.jsonl').read_text().splitlines()]
            self.assertEqual([item['device_usage'] for item in records],
                             list(range(1, 21)) + [1])
            self.assertEqual(LifecyclePool.registrations, 1)

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
                self.assertEqual(busy['available'], 0)
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

    def test_node_proxy_pool_never_leases_an_exit_that_is_already_in_use(self):
        context = mp.get_context('spawn')
        pool = NodeProxyPool(context, concurrency=2, ttl=50, target=2)
        try:
            pool.add_fresh(['socks5h://one', 'socks5h://two'])
            first = pool.acquire(wait=.5)
            second = pool.acquire(wait=.5)
            self.assertEqual({first[0], second[0]},
                             {'socks5h://one', 'socks5h://two'})
            self.assertIsNone(pool.acquire(wait=.05))
            pool.discard(first)
            # The other worker still owns `second`; it cannot be leased again.
            self.assertIsNone(pool.acquire(wait=.05))
            pool.release(second, healthy=True)
            available = pool.acquire(wait=.5)
            self.assertEqual(available[0], second[0])
            pool.release(available, healthy=True)
        finally:
            pool.close()

    def test_node_proxy_pool_never_exceeds_configured_logical_capacity(self):
        context = mp.get_context('spawn')
        pool = NodeProxyPool(context, concurrency=2, ttl=30, target=2)
        try:
            pool.add_fresh(['socks5h://one', 'socks5h://two'])
            first = pool.acquire(wait=.5)
            second = pool.acquire(wait=.5)
            self.assertEqual(pool.add_fresh(['socks5h://three']), 0)
            self.assertEqual(pool.snapshot()['known'], 2)
            pool.release(first, healthy=True)
            pool.release(second, healthy=True)
            self.assertEqual(pool.add_fresh(['socks5h://three']), 1)
            self.assertEqual(pool.snapshot()['known'], 2)
            self.assertEqual(pool.snapshot()['refreshed'], 1)
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

    def test_proxy_pool_marks_exits_aging_at_45_seconds(self):
        context = mp.get_context('spawn')
        pool = NodeProxyPool(context, concurrency=2, ttl=50, target=2)
        try:
            now = time.monotonic()
            pool.add_fresh(['socks5h://young'], fetched_at=now - 44)
            pool.add_fresh(['socks5h://aging'], fetched_at=now - 46)
            time.sleep(.05)
            self.assertEqual(pool.count_aging(now=now), 1)
            self.assertEqual(pool.snapshot()['aging_45s'], 1)
            self.assertEqual(pool.snapshot()['known'], 2)
        finally:
            pool.close()

    def test_supplier_prefetches_replacements_for_45_second_old_exits(self):
        context = mp.get_context('spawn')
        pool = NodeProxyPool(context, concurrency=2, ttl=50, target=2)
        stop = threading.Event()
        calls = []
        try:
            pool.add_fresh(['socks5h://old-one', 'socks5h://old-two'],
                           fetched_at=time.monotonic() - 46)
            time.sleep(.05)

            def fetcher(_url, count, scheme=None):
                calls.append(count)
                return [f'{scheme}://replacement-{index}' for index in range(count)]

            thread = threading.Thread(
                target=proxy_supplier_loop,
                args=(pool, 'http://proxy.test/list', 'socks5h', stop, fetcher))
            thread.start()
            deadline = time.monotonic() + 1
            while pool.snapshot()['refreshed'] < 2 and time.monotonic() < deadline:
                time.sleep(.01)
            stop.set()
            thread.join(2)
            self.assertFalse(thread.is_alive())
            self.assertEqual(calls, [2])
            self.assertEqual(pool.snapshot()['known'], 2)
            self.assertEqual(pool.snapshot()['refreshed'], 2)
        finally:
            stop.set()
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

    def test_auth_unit_rotates_proxy_after_http_zero_and_reuses_http_200(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'input.part').write_text('account----password\n', encoding='utf-8')
            descriptor = {'rows': 1, 'first_row': 1, 'threads': 1,
                          'device_capacity': 1, 'device_root': str(root / 'devices'),
                          'origin': 'http://127.0.0.1:1', 'timeout': 5, 'retries': 1,
                          '_retry_delay': 0, '_proxy_debug': True}
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
                self.assertEqual((record['status'], record['attempts']), ('timeout', 1))
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

    def test_debug_uses_five_percent_timing_samples_not_full_account_logging(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
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
                diagnostics({'event': 'phase_end',
                             'phase': 'srp_challenge_compute',
                             'ms': 12.5, 'cpu_ms': 7.25})
                diagnostics({'event': 'phase_end',
                             'phase': 'http_complete',
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
            self.assertEqual(timing['trace_sample_every'], 20)
            self.assertEqual(timing['srp']['state'], 'observed')
            self.assertEqual(timing['srp']['compute_cpu_ms'], 7.25)
            serialized = json.dumps(timing)
            self.assertNotIn('account----password', serialized)
            self.assertNotIn('socks5h://account-exit', serialized)

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
                 patch('clusterlab.executor._merge_device_usage',
                       side_effect=lambda _root, counts: usage_calls.append(dict(counts))), \
                 patch.object(Path, 'read_bytes', side_effect=AssertionError('must hash while writing')):
                _execute_auth_unit(root, descriptor, threading.Event())

            done = json.loads((root / 'done.json').read_text('utf-8'))
            payload = (root / 'results.jsonl').read_bytes()
            self.assertEqual(done['processed'], 500)
            self.assertEqual(json.loads((root / 'progress.json').read_text('utf-8'))['counts'],
                             {'valid': 500})
            self.assertEqual(done['checksum'], hashlib.sha256(payload).hexdigest())
            self.assertEqual(len(usage_calls), 1)
            self.assertTrue(all(0 <= value <= 20 for value in usage_calls[0].values()))
            self.assertEqual(len(payload.splitlines()), 500)

    def test_auth_progress_is_time_based_instead_of_waiting_for_ten_results(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'input.part').write_text('account----password\n', encoding='utf-8')
            descriptor = {'rows': 1, 'first_row': 1, 'threads': 1,
                          'device_capacity': 1, 'device_root': str(root / 'devices'),
                          'origin': 'http://127.0.0.1:1', 'timeout': 5}
            flushes = []
            original_flush = DeviceLeaseAllocator.flush_usage
            def record_flush(allocator):
                flushes.append(dict(allocator.usage))
                return original_flush(allocator)
            with patch('clusterlab.executor._load_fastcheck', return_value=(
                    FakePool, lambda *_args, **_kwargs: {'status': 'wrong_password'},
                    lambda result: result['status'])), \
                 patch('fastcheck.workers.device_profile',
                       return_value={'client_info': 'test', 'user_agent': 'test'}), \
                 patch('clusterlab.executor._bump_usage'), \
                 patch('clusterlab.executor.time.monotonic',
                       side_effect=[10.0] * 4 + [10.3] * 8), \
                 patch.object(DeviceLeaseAllocator, 'flush_usage', record_flush), \
                 patch('clusterlab.executor.atomic_json', wraps=atomic_json) as writes:
                _execute_auth_unit(root, descriptor, threading.Event())

            progress = [call.args[1] for call in writes.call_args_list
                        if call.args[0].name == 'progress.json']
            self.assertEqual([item['processed'] for item in progress], [1, 1])
            self.assertEqual(progress[0]['counts'], {'wrong_password': 1})
            self.assertGreaterEqual(len(flushes), 2)
            self.assertEqual(flushes[0][1], 1)

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
