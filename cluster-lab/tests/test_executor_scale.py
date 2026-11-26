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

import clusterlab.executor as executor_module
from clusterlab.executor import (_ProcessDeviceAllocator, _ProcessOwnedDevicePool, _ReferenceIpPoolAdapter, _WatermarkProxyPool, _device_id_label, _execute_auth_unit, _gsa_per_hop_timeout, _proxy_watermarks,
                                 _apple_reason_for_record, _apple_reason_key,
                                 _proxy_debug_enabled, _slot_for_account, atomic_json,
                                 execute_auth_unit, execute_unit_group,
                                 _partition_device_slots, record_device_registration, stall_timeout_seconds)


class FakePool:
    def __init__(self, *_args, **_kwargs):
        pass

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





class ExecutorScaleTests(unittest.TestCase):
    def test_500_thread_proxy_pool_matches_proven_watermarks(self):
        self.assertEqual(_proxy_watermarks(500), (250, 750))

    def test_fastcheck_root_walks_ancestors_for_nested_release_copy(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'fastcheck').mkdir()
            module = root / 'verification-artifacts' / 'release-v1' / 'MODIFIED_PROJECT' / 'clusterlab' / 'executor.py'
            module.parent.mkdir(parents=True)
            module.touch()
            with patch.dict(os.environ, {}, clear=False):
                os.environ.pop('CLUSTER_LAB_FASTCHECK_ROOT', None)
                found, searched = executor_module._find_fastcheck_root(module, [])
            self.assertEqual(found, root.resolve())
            self.assertIn(root.resolve(), searched)

    def test_two_processes_receive_balanced_disjoint_device_files(self):
        slots = list(range(1, 501))
        first = _partition_device_slots(slots, 2, 0)
        second = _partition_device_slots(slots, 2, 1)
        self.assertEqual(first, list(range(1, 251)))
        self.assertEqual(second, list(range(251, 501)))
        self.assertFalse(set(first) & set(second))
        self.assertEqual(sorted(first + second), slots)

    def test_device_checkout_prefers_least_used_and_releases_lock_before_work(self):
        states = {slot: {'slot': slot} for slot in (1, 2, 3)}
        allocator = _ProcessDeviceAllocator(states, {1: 8, 2: 1, 3: 1})
        first = allocator.acquire()
        second = allocator.acquire()
        self.assertEqual([first['slot'], second['slot']], [2, 3])
        acquired = []

        def probe_lock():
            with allocator.condition:
                acquired.append(True)

        thread = threading.Thread(target=probe_lock)
        thread.start(); thread.join(1)
        self.assertEqual(acquired, [True])
        self.assertEqual(allocator.snapshot()['in_use'], 2)
        allocator.release(first)
        allocator.release(second)
        self.assertEqual(allocator.snapshot()['available'], 3)
        self.assertEqual(allocator.acquire()['slot'], 2)

    def test_process_owned_pool_preloads_only_assigned_device_files(self):
        class RecordingPool:
            def __init__(inner):
                inner.inspected = []
            def inspect(inner, slot):
                inner.inspected.append(slot)
                return {'slot': slot, 'state': 'ready', 'device_id': f'device-{slot}'}
            def slot_directory(inner, slot):
                return Path('/tmp') / str(slot)
        delegate = RecordingPool()
        pool = _ProcessOwnedDevicePool(delegate, [251, 252, 253])
        loaded = pool.preload()
        self.assertEqual(delegate.inspected, [251, 252, 253])
        self.assertEqual(set(loaded), {251, 252, 253})
        with self.assertRaisesRegex(ValueError, 'not owned'):
            pool.inspect(250)

    def test_process_owned_pool_reset_falls_back_to_retiring_real_device_directory(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            directory = root / '1'
            directory.mkdir()
            (directory / 'device.json').write_text('{}', encoding='utf-8')

            class RealLikePool:
                def __init__(inner):
                    inner._memory_states = {1: {'cached': True}}
                    inner._memory_states_lock = threading.Lock()
                def slot_directory(inner, slot):
                    self.assertEqual(slot, 1)
                    return directory

            delegate = RealLikePool()
            pool = _ProcessOwnedDevicePool(delegate, [1])
            self.assertTrue(pool.reset(1))
            self.assertFalse(directory.exists())
            self.assertNotIn(1, delegate._memory_states)

    def test_reference_ip_pool_adapter_treats_supplier_timeout_as_empty_refill(self):
        pool = _ReferenceIpPoolAdapter(
            lambda _count: (_ for _ in ()).throw(TimeoutError('supplier timeout')),
            workers=2, ttl=5)
        try:
            snapshot = pool.bootstrap()
            self.assertEqual(snapshot['available'], 0)
            self.assertIsNone(pool.acquire(wait=0.01))
        finally:
            pool.stop()

    def test_watermark_pool_bootstraps_exact_thread_count(self):
        calls = []

        def fetch(count):
            calls.append(count)
            return [f'socks5h://192.0.2.{index + 1}:1080'
                    for index in range(count)]

        pool = _WatermarkProxyPool(fetch, target=300)
        snapshot = pool.bootstrap()
        self.assertEqual(calls, [300])
        self.assertEqual(snapshot['target'], 300)
        self.assertEqual(snapshot['low_water'], 150)
        self.assertEqual(snapshot['fresh'], 300)
        self.assertEqual(snapshot['known'], 300)

    def test_watermark_pool_rotates_evenly_and_prioritizes_refill(self):
        calls = []
        generation = 0

        def fetch(count):
            nonlocal generation
            generation += 1
            calls.append(count)
            return [f'socks5h://10.0.{generation}.{index}:1080'
                    for index in range(1, count + 1)]

        pool = _WatermarkProxyPool(fetch, target=4, low_water=2)
        pool.bootstrap()
        first = [pool.acquire() for _ in range(4)]
        for lease in first:
            pool.release(lease, healthy=True)
        second = [pool.acquire() for _ in range(4)]
        self.assertEqual([lease[0] for lease in second],
                         [lease[0] for lease in first])
        for lease in second:
            pool.release(lease, healthy=True)

        failed = [pool.acquire() for _ in range(3)]
        for lease in failed:
            pool.discard(lease)
        deadline = time.time() + 2
        while pool.snapshot()['refilling'] and time.time() < deadline:
            time.sleep(0.01)
        self.assertEqual(calls, [4, 3])
        replacement = [pool.acquire() for _ in range(3)]
        # The one validated healthy exit is preferred; the discarded three
        # are replaced from the next provider generation.
        self.assertIn('10.0.1.', replacement[0][0])
        self.assertTrue(all('10.0.2.' in lease[0] for lease in replacement[1:]))

    def test_empty_watermark_refills_are_rate_limited(self):
        called = []

        def fetch(_count):
            called.append(time.monotonic())
            return []

        pool = _WatermarkProxyPool(fetch, target=1, refill_interval=.05)
        pool.bootstrap()
        self.assertIsNone(pool.acquire(wait=.01))
        deadline = time.monotonic() + .5
        while pool.snapshot()['refilling'] and time.monotonic() < deadline:
            time.sleep(.005)
        self.assertGreaterEqual(len(called), 2)
        self.assertGreaterEqual(called[1] - called[0], .045)

    def test_watermark_pool_expires_provider_ports_before_reuse(self):
        calls = []

        def fetch(count):
            calls.append(count)
            generation = len(calls)
            return [f'socks5h://10.0.{generation}.1:{8000 + index}'
                    for index in range(count)]

        pool = _WatermarkProxyPool(fetch, target=2, low_water=1,
                                   refill_interval=0, ttl=.05,
                                   lease_margin=.01)
        pool.bootstrap()
        first = pool.acquire()
        pool.release(first)
        time.sleep(.06)
        second = pool.acquire(wait=.5)
        self.assertIsNotNone(second)
        self.assertNotEqual(first[0], second[0])
        self.assertGreaterEqual(len(calls), 2)

    def test_expired_busy_leases_do_not_block_replacement_inventory(self):
        calls = []

        def fetch(count):
            calls.append(count)
            generation = len(calls)
            return [f'socks5h://10.0.{generation}.{index}:1080'
                    for index in range(1, count + 1)]

        pool = _WatermarkProxyPool(fetch, target=2, low_water=1,
                                   refill_interval=0, ttl=.05,
                                   lease_margin=.01)
        pool.bootstrap()
        expired_in_flight = pool.acquire()
        self.assertIsNotNone(expired_in_flight)
        time.sleep(.06)
        replacement = pool.acquire(wait=.5)
        self.assertIsNotNone(replacement)
        self.assertNotEqual(expired_in_flight[0], replacement[0])
        self.assertGreaterEqual(len(calls), 2)

    def test_gsa_two_hops_fit_inside_account_deadline(self):
        self.assertEqual(_gsa_per_hop_timeout(25, 25), 25.0)
        self.assertEqual(_gsa_per_hop_timeout(25, 8), 8.0)
        self.assertEqual(_gsa_per_hop_timeout(1, 25), 1.0)

    def test_raw_apple_reason_is_packaged_only_when_debug_is_enabled(self):
        result = {'apple_message': 'Your account information was entered incorrectly.'}
        self.assertEqual(_apple_reason_for_record(result, 'wrong_password'), '')
        self.assertEqual(_apple_reason_for_record(result, 'wrong_password', True),
                         'Your account information was entered incorrectly.')
        self.assertEqual(_apple_reason_key(result, 'wrong_password'), 'account_info_incorrect')
        unknown = {'apple_message': 'New Apple reason ' + 'x' * 400}
        self.assertEqual(_apple_reason_key(unknown, 'undetermined'), 'unknown')
        self.assertEqual(len(_apple_reason_for_record(unknown, 'undetermined', True)), 240)

    def test_stall_budget_covers_all_retry_windows(self):
        self.assertEqual(stall_timeout_seconds({'timeout': 20, 'retries': 4}), 180)
        self.assertEqual(stall_timeout_seconds({'timeout': 60, 'retries': 4}), 360)
        self.assertEqual(stall_timeout_seconds({'timeout': 25, 'retries': 2,
                                                'item_timeout': 40}), 180)

    def test_device_id_uses_short_base64_label(self):
        self.assertEqual(_device_id_label('device-1'), 'B64:ZGV2aWNl')
        self.assertEqual(_device_id_label(''), '')

    def test_task_bootstraps_high_water_pool_then_reuses_evenly(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'input.part').write_text(
                ''.join(f'user-{index}----password\n' for index in range(8)), 'utf-8')
            proxy_calls = []
            header_calls = []
            checked = []
            guard = threading.Lock()
            first_wave = threading.Barrier(4)
            seen_threads = set()

            class Pool(FakePool):
                def info(inner, *_args, **_kwargs):
                    raise AssertionError('.info.lock hot path must not be used')

                def fetch_headers(inner, slot, *_args, **_kwargs):
                    with guard:
                        header_calls.append((threading.current_thread().name, slot))
                    return {'slot': slot}

            def fetch(_api, count, **_kwargs):
                with guard:
                    proxy_calls.append((threading.current_thread().name, count))
                return [f'socks5h://10.0.0.{index}:1080'
                        for index in range(1, count + 1)]

            def check(_account, _password, proxy, **_kwargs):
                name = threading.current_thread().name
                with guard:
                    first = name not in seen_threads
                    seen_threads.add(name)
                if first:
                    first_wave.wait(2)
                with guard:
                    checked.append((name, proxy))
                return {'status': 'valid', 'http': 200}

            descriptor = {'rows': 8, 'first_row': 1, 'threads': 4,
                          'device_pool_capacity': 4, 'device_slots': [1, 2, 3, 4],
                          'device_root': str(root / 'devices'),
                          'origin': 'http://127.0.0.1:1', 'timeout': 1,
                          'proxy_api': 'http://proxy.invalid/list',
                          'proxy_scheme': 'socks5h'}
            with patch('clusterlab.executor._load_fastcheck',
                       return_value=(Pool, check, lambda result: result['status'])), \
                 patch('fastcheck.proxy_pool.fetch_proxies', side_effect=fetch), \
                 patch('fastcheck.workers.device_profile',
                       return_value={'client_info': 'test', 'user_agent': 'test'}):
                _execute_auth_unit(root, descriptor, threading.Event())

            self.assertEqual(proxy_calls[0], ('MainThread', 8))
            self.assertTrue(all(name in ('MainThread', 'ip-supplier')
                                for name, _count in proxy_calls))
            self.assertEqual(len(header_calls), 4)
            self.assertEqual({slot for _name, slot in header_calls}, {1, 2, 3, 4})
            by_thread = {}
            for name, endpoint in checked:
                by_thread.setdefault(name, set()).add(endpoint)
            self.assertEqual(len(by_thread), 4)
            endpoint_counts = {}
            for _name, endpoint in checked:
                endpoint_counts[endpoint] = endpoint_counts.get(endpoint, 0) + 1
            self.assertEqual(len(endpoint_counts), 4)
            self.assertEqual(sum(endpoint_counts.values()), 8)
            self.assertGreater(max(endpoint_counts.values()), 1)

    def test_threads_share_one_on_demand_public_header_bound_to_owned_device(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'input.part').write_text(
                'first----password\nsecond----password\n', 'utf-8')
            header_calls = []

            class Pool(FakePool):
                def fetch_headers(inner, slot, *_args, **_kwargs):
                    header_calls.append(slot)
                    time.sleep(.03)
                    return {'slot': slot, 'public': 'header'}

            def fetch(_api, count, **_kwargs):
                return [f'socks5h://10.0.0.{index}:1080'
                        for index in range(1, count + 1)]

            descriptor = {'rows': 2, 'first_row': 1, 'threads': 2,
                          'device_pool_capacity': 1, 'device_slots': [1],
                          'device_root': str(root / 'devices'),
                          'origin': 'http://127.0.0.1:1', 'timeout': 1,
                          'proxy_api': 'http://proxy.invalid/list',
                          'proxy_scheme': 'socks5h'}
            with patch('clusterlab.executor._load_fastcheck', return_value=(
                    Pool, lambda *_args, **_kwargs: {'status': 'valid', 'http': 200},
                    lambda result: result['status'])), \
                 patch('fastcheck.proxy_pool.fetch_proxies', side_effect=fetch), \
                 patch('fastcheck.workers.device_profile',
                       return_value={'client_info': 'test', 'user_agent': 'test'}), \
                 patch('clusterlab.executor._bump_usage'):
                _execute_auth_unit(root, descriptor, threading.Event())
            self.assertEqual(header_calls, [1])
            records = [json.loads(line) for line in
                       (root / 'results.jsonl').read_text().splitlines()]
            self.assertEqual(len(records), 2)
            self.assertEqual({record['status'] for record in records}, {'valid'})

    def test_production_coordinator_runs_two_processes_with_process_owned_pools(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'input.part').write_text('user----password\n', 'utf-8')

            (root / 'input.part').write_text(
                'user-1----password\nuser-2----password\n', 'utf-8')
            descriptor = {'rows': 2, 'first_row': 1, 'processes': 2,
                          'threads': 250, 'mode': 'digest'}
            execute_unit_group(root, descriptor, threading.Event())
            done = json.loads((root / 'done.json').read_text('utf-8'))
            manifests = [json.loads(path.read_text('utf-8')) for path in
                         sorted((root / 'workers').glob('*/manifest.json'))]
            self.assertTrue(done['ok'])
            self.assertEqual(done['workers'], 2)
            self.assertEqual(done['configured_process_limit'], 2)
            self.assertEqual(done['active_threads_per_process'], 250)
            self.assertEqual(done['proxy_pool_owner'], 'process')
            self.assertEqual(done['proxy_pool_count'], 2)
            self.assertEqual([item['_proxy_pool_owner'] for item in manifests],
                             ['process', 'process'])
            self.assertEqual([item['_device_owner'] for item in manifests],
                             ['process', 'process'])
            self.assertEqual(done['device_owner'], 'process')
            self.assertEqual(done['device_pool_count'], 2)

    def test_account_hash_balances_and_exposes_base64_prefix(self):
        slots = list(range(1, 31))
        assignments = [_slot_for_account(f'user-{index:05d}@example.com', slots)
                       for index in range(3000)]
        counts = {slot: 0 for slot in slots}
        for slot, prefix in assignments:
            counts[slot] += 1
            self.assertRegex(prefix, r'^B64:[A-Za-z0-9_-]{8}$')
        self.assertLess(max(counts.values()) - min(counts.values()), 50)

    def test_auth_unit_does_not_take_account_before_proxy_and_device_are_ready(self):
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
                execute_auth_unit(root, descriptor, threading.Event(), proxy_pool=shared)
            done = json.loads((root / 'done.json').read_text())
            progress = json.loads((root / 'progress.json').read_text())
            self.assertFalse(done['ok'])
            self.assertEqual(done['processed'], 0)
            self.assertEqual(progress['stage'], 'preparing_devices')
            self.assertEqual(progress['prepared_devices'], 0)
            self.assertFalse((root / 'results.jsonl').exists())
            self.assertFalse((root / 'devices' / 'registrations').exists())

    def test_each_bound_worker_gets_headers_before_its_account_request(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'input.part').write_text(
                'first----password\nsecond----password\n', encoding='utf-8')
            lock = threading.Lock()
            warm = []
            checked = []

            class Pool(FakePool):
                def fetch_headers(inner, slot, *_args, **_kwargs):
                    time.sleep(.02 if slot == 1 else .04)
                    with lock:
                        warm.append(slot)
                    return {'slot': slot, 'public': 'ready'}

            def fetch(_api, count, **_kwargs):
                return [f'socks5h://10.0.0.{index}:1080'
                        for index in range(1, count + 1)]

            def check(account, _password, _proxy, **kwargs):
                with lock:
                    slot = kwargs['identity']['slot']
                    self.assertIn(slot, warm)
                    checked.append((account, kwargs['identity']['public'], slot))
                return {'status': 'valid', 'kind': 'valid', 'http': 200}

            descriptor = {'rows': 2, 'first_row': 1, 'threads': 2,
                          'device_pool_capacity': 2, 'device_slots': [1, 2],
                          'device_root': str(root / 'devices'),
                          'origin': 'http://127.0.0.1:1', 'timeout': 1,
                          'proxy_api': 'http://proxy.invalid/list',
                          'proxy_scheme': 'socks5h'}
            with patch('clusterlab.executor._load_fastcheck', return_value=(
                    Pool, check, lambda result: result['status'])), \
                 patch('fastcheck.proxy_pool.fetch_proxies', side_effect=fetch), \
                 patch('fastcheck.workers.device_profile',
                       return_value={'client_info': 'test', 'user_agent': 'test'}), \
                 patch('clusterlab.executor._bump_usage'):
                _execute_auth_unit(root, descriptor, threading.Event())

            self.assertEqual(set(warm), {1, 2})
            self.assertEqual(len(checked), 2)
            self.assertEqual({value for _account, value, _slot in checked}, {'ready'})
            self.assertTrue({slot for _account, _value, slot in checked} <= {1, 2})

    def test_device_header_failure_stops_before_any_account_result(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'input.part').write_text('account----password\n', encoding='utf-8')
            checked = []

            class Pool(FakePool):
                def inspect(inner, _slot):
                    return {'state': 'new'}

                def fetch_headers(inner, *_args, **_kwargs):
                    raise TimeoutError('warmup fixture')

            descriptor = {'rows': 1, 'first_row': 1, 'threads': 1,
                          'device_capacity': 1, 'device_root': str(root / 'devices'),
                          'origin': 'http://127.0.0.1:1', 'timeout': 1,
                          'retries': 0, '_device_warmup_retry_delay': 0}
            shared = ScriptedProxyPool(['socks5h://warmup'])

            def check(*_args, **_kwargs):
                checked.append(True)
                return {'status': 'valid', 'http': 200}

            with patch('clusterlab.executor._load_fastcheck', return_value=(
                    Pool, check, lambda result: result['status'])), \
                 patch('fastcheck.workers.device_profile',
                       return_value={'client_info': 'test', 'user_agent': 'test'}), \
                 patch('clusterlab.executor._bump_usage'):
                execute_auth_unit(root, descriptor, threading.Event(), proxy_pool=shared)

            done = json.loads((root / 'done.json').read_text())
            self.assertEqual(checked, [])
            self.assertEqual((done['ok'], done['processed']), (False, 0))
            self.assertIn('设备 1 预热失败', done['error'])
            self.assertFalse((root / 'results.jsonl').exists())

    def test_one_device_warmup_failure_does_not_kill_process(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'input.part').write_text(
                'first----password\nsecond----password\n', encoding='utf-8')
            checked = []

            class Pool(FakePool):
                def fetch_headers(inner, slot, *_args, **_kwargs):
                    if slot == 2:
                        raise TimeoutError('one bad device')
                    return {'slot': slot, 'public': 'ready'}

            descriptor = {'rows': 2, 'first_row': 1, 'threads': 2,
                          'device_pool_capacity': 2, 'device_slots': [1, 2],
                          'device_root': str(root / 'devices'),
                          'origin': 'http://127.0.0.1:1', 'timeout': 1,
                          'retries': 0, '_device_warmup_retry_delay': 0}
            shared = ScriptedProxyPool([
                'socks5h://warm-one', 'socks5h://warm-two',
                'socks5h://account-one', 'socks5h://account-two'])

            def check(account, _password, _proxy, **kwargs):
                checked.append((account, kwargs['identity']['slot']))
                return {'status': 'valid', 'kind': 'valid', 'http': 200}

            with patch('clusterlab.executor._load_fastcheck', return_value=(
                    Pool, check, lambda result: result['status'])), \
                 patch('fastcheck.workers.device_profile',
                       return_value={'client_info': 'test', 'user_agent': 'test'}), \
                 patch('clusterlab.executor._bump_usage'):
                execute_auth_unit(root, descriptor, threading.Event(), proxy_pool=shared)

            done = json.loads((root / 'done.json').read_text())
            self.assertEqual((done['ok'], done['processed']), (True, 2))
            self.assertEqual(checked, [('first', 1), ('second', 1)])
            records = [json.loads(line) for line in
                       (root / 'results.jsonl').read_text().splitlines()]
            self.assertEqual([record['device'] for record in records], [1, 1])
            progress = json.loads((root / 'progress.json').read_text())
            self.assertEqual(progress['processed'], 2)

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

    def test_retry_stays_on_same_worker_before_next_account(self):
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
                ('first', 'socks5h://second-account'),
                ('second', 'socks5h://first-retry'),
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

    def test_auth_unit_retries_22421_with_fresh_identity_and_hides_apple_reason(self):
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
                          '_retry_delay': 0}
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
            self.assertEqual(record['reason'], '')
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
                          '_retry_delay': 0}
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
            self.assertEqual(record['reason'], '')
            self.assertEqual(stop.waits, [])
            self.assertEqual(shared.discards, [
                'socks5h://first-exit', 'socks5h://second-exit', 'socks5h://third-exit'])

    def test_each_late_attempt_consumes_one_configured_retry(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'input.part').write_text('account----password\n', encoding='utf-8')
            descriptor = {'rows': 1, 'first_row': 1, 'threads': 1,
                          'device_capacity': 1, 'device_root': str(root / 'devices'),
                          'origin': 'http://127.0.0.1:1', 'timeout': .1,
                          'item_timeout': .2, 'retries': 2, '_retry_delay': 0}
            shared = ScriptedProxyPool([
                'socks5h://bootstrap', 'socks5h://first-exit',
                'socks5h://second-exit', 'socks5h://third-exit'])
            request_timeouts = []

            def check(_account, _password, _proxy, **kwargs):
                request_timeouts.append(kwargs['timeout'])
                time.sleep(.25)
                return {'status': 'error', 'kind': 'network', 'http': 0}

            with patch('clusterlab.executor._load_fastcheck', return_value=(
                    FakePool, check, lambda _result: 'timeout')), \
                 patch('fastcheck.workers.device_profile',
                       return_value={'client_info': 'test', 'user_agent': 'test'}), \
                 patch('clusterlab.executor._bump_usage'):
                _execute_auth_unit(root, descriptor, threading.Event(), proxy_pool=shared)

            record = json.loads((root / 'results.jsonl').read_text())
            self.assertEqual(len(request_timeouts), 3)
            self.assertTrue(all(value <= .1 for value in request_timeouts))
            self.assertEqual((record['status'], record['attempts']), ('timeout', 2))

    def test_late_attempt_retries_on_fresh_exit_without_total_budget(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'input.part').write_text('account----password\n', encoding='utf-8')
            descriptor = {'rows': 1, 'first_row': 1, 'threads': 1,
                          'device_capacity': 1, 'device_root': str(root / 'devices'),
                          'origin': 'http://127.0.0.1:1', 'timeout': .05,
                          'item_timeout': .01, 'retries': 1, '_retry_delay': 0}
            shared = ScriptedProxyPool([
                'socks5h://bootstrap', 'socks5h://late-exit',
                'socks5h://fresh-exit'])
            checked = []

            def check(_account, _password, proxy, **_kwargs):
                checked.append(proxy)
                if len(checked) == 1:
                    time.sleep(.07)
                return {'status': 'valid', 'kind': 'valid', 'http': 200}

            with patch('clusterlab.executor._load_fastcheck', return_value=(
                    FakePool, check, lambda result: result['status'])), \
                 patch('fastcheck.workers.device_profile',
                       return_value={'client_info': 'test', 'user_agent': 'test'}), \
                 patch('clusterlab.executor._bump_usage'):
                _execute_auth_unit(root, descriptor, threading.Event(), proxy_pool=shared)

            record = json.loads((root / 'results.jsonl').read_text())
            self.assertEqual(checked, [
                'socks5h://late-exit', 'socks5h://fresh-exit'])
            self.assertEqual((record['status'], record['attempts']), ('valid', 1))
            self.assertEqual(shared.discards, ['socks5h://late-exit'])
            self.assertEqual(shared.releases, [
                ('socks5h://bootstrap', True),
                ('socks5h://fresh-exit', True),
            ])

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
                              '_retry_delay': 0, '_proxy_debug': True,
                              '_detailed_error_log': True}
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
                self.assertNotEqual(record['reason_key'], 'unknown')
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

    def test_one_package_runs_in_configured_processes_and_keeps_row_order(self):
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
            self.assertEqual(done['proxy_pool_owner'], 'process')
            self.assertEqual(done['proxy_pool_count'], 4)
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
                          'origin': 'http://127.0.0.1:1', 'timeout': 5,
                          # This test exercises bounded futures and batched usage writes,
                          # not device retirement. Keep the threshold above the fixture size.
                          'device_use_limit': len(lines) + 1}
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
            self.assertLessEqual(len(usage_calls[0]), 2)
            self.assertEqual(len(payload.splitlines()), 500)

    def test_public_header_limit_refreshes_and_device_limit_retires_slot(self):
        class CountingPool(FakePool):
            headers = 0
            resets = 0

            def fetch_headers(self, *_args, **_kwargs):
                type(self).headers += 1
                return {'header-generation': type(self).headers}

            def reset(self, _slot):
                type(self).resets += 1

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            device_root = root / 'devices'
            device_root.mkdir()
            (device_root / 'usage.json').write_text('{"1":1}', encoding='utf-8')
            (root / 'input.part').write_text(
                'first----password\nsecond----password\n', encoding='utf-8')
            descriptor = {'rows': 2, 'first_row': 1, 'threads': 1,
                          'device_capacity': 1, 'device_root': str(device_root),
                          'origin': 'http://127.0.0.1:1', 'timeout': 5,
                          'public_header_use_limit': 1, 'device_use_limit': 1}
            with patch('clusterlab.executor._load_fastcheck', return_value=(
                    CountingPool, lambda *_args, **_kwargs: {'status': 'valid'},
                    lambda result: result['status'])), \
                 patch('fastcheck.workers.device_profile',
                       return_value={'client_info': 'test', 'user_agent': 'test',
                                     'model': 'TestModel'}):
                _execute_auth_unit(root, descriptor, threading.Event())
            # One startup header plus two post-retirement/usage-limit refreshes.
            self.assertEqual(CountingPool.headers, 3)
            self.assertEqual(CountingPool.resets, 2)
            self.assertEqual(json.loads((device_root / 'usage.json').read_text())['1'], 1)

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
                 patch('clusterlab.executor.time.monotonic',
                       side_effect=[10.0, 10.0, 10.0, 10.3, 10.3, 10.3]), \
                 patch('clusterlab.executor.atomic_json', wraps=atomic_json) as writes:
                _execute_auth_unit(root, descriptor, threading.Event())

            progress = [call.args[1] for call in writes.call_args_list
                        if call.args[0].name == 'progress.json']
            self.assertEqual([item['processed'] for item in progress],
                             [0, 0, 0, 1, 1])
            self.assertEqual([item.get('stage') for item in progress[:3]],
                             ['preparing_devices', 'preparing_devices', 'ready'])
            self.assertEqual(progress[-2]['counts'], {'wrong_password': 1})

    def test_registered_device_warmup_progress_batches_fsync_writes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'input.part').write_text('account----password\n', encoding='utf-8')
            descriptor = {'rows': 1, 'first_row': 1, 'threads': 50,
                          'device_pool_capacity': 50,
                          'device_slots': list(range(1, 51)),
                          'device_root': str(root / 'devices'),
                          'origin': 'http://127.0.0.1:1', 'timeout': 5}
            with patch('clusterlab.executor._load_fastcheck', return_value=(
                    FakePool, lambda *_args, **_kwargs: {'status': 'wrong_password'},
                    lambda result: result['status'])), \
                 patch('fastcheck.workers.device_profile',
                       return_value={'client_info': 'test', 'user_agent': 'test'}), \
                 patch('clusterlab.executor._bump_usage'), \
                 patch('clusterlab.executor.atomic_json', wraps=atomic_json) as writes:
                _execute_auth_unit(root, descriptor, threading.Event())

            preparing = [call.args[1] for call in writes.call_args_list
                         if call.args[0].name == 'progress.json'
                         and call.args[1].get('stage') == 'preparing_devices']
            self.assertEqual(len(preparing), 3)
            self.assertEqual(preparing[-1]['prepared_devices'], 50)

    def test_auth_unit_keeps_unrecognized_gsa_20970_as_undetermined(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'input.part').write_text('account----password\n', encoding='utf-8')
            descriptor = {'rows': 1, 'first_row': 1, 'threads': 1,
                          'device_capacity': 1, 'device_root': str(root / 'devices'),
                          'origin': 'http://127.0.0.1:1', 'timeout': 5,
                          'dual_recheck': True}

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
            self.assertEqual(record['status'], 'undetermined')
            self.assertEqual(record['protocol'], 'GSA')
            self.assertEqual((record['http'], record['code']), (200, '-20970'))
            self.assertEqual(record['reason'], '')
            self.assertNotIn('gsa_code', record)
            self.assertNotIn('web_code', record)
            done = json.loads((root / 'done.json').read_text())
            self.assertEqual(done['counts'], {'undetermined': 1})

    def test_unlabelled_gsa_20970_consumes_retries_then_finishes_timeout(self):
        cases = {
            'http_zero_then_20970': [
                {'status': 'error', 'kind': 'network', 'stage': 'init', 'http': 0},
                {'status': 'error', 'kind': 'network', 'stage': 'init', 'http': 0},
                {'status': 'error', 'kind': 'protocol', 'stage': 'init',
                 'http': 200, 'apple_ec': -20970, 'detail': 'ec=-20970'},
            ],
            '20970_from_first_attempt': [
                {'status': 'error', 'kind': 'protocol', 'stage': 'init',
                 'http': 200, 'apple_ec': -20970, 'detail': 'ec=-20970'},
            ] * 3,
            '20970_with_unrecognized_reason': [
                {'status': 'error', 'kind': 'protocol', 'stage': 'init',
                 'http': 200, 'apple_ec': -20970,
                 'apple_message': 'A new Apple reason not yet classified.'},
            ] * 3,
        }
        for name, responses in cases.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                (root / 'input.part').write_text('account----password\n', encoding='utf-8')
                descriptor = {'rows': 1, 'first_row': 1, 'threads': 1,
                              'device_capacity': 1, 'device_root': str(root / 'devices'),
                              'origin': 'http://127.0.0.1:1', 'timeout': 5,
                              'retries': 2, '_retry_delay': 0}
                shared = ScriptedProxyPool([
                    'socks5h://bootstrap', 'socks5h://first-exit',
                    'socks5h://second-exit', 'socks5h://third-exit'])
                checked = []

                def check(*_args, **_kwargs):
                    checked.append(1)
                    return dict(responses[len(checked) - 1])

                def bucket(result):
                    if int(result.get('http') or 0) == 0:
                        return 'timeout'
                    return 'invalid' if result.get('apple_ec') == -20970 else 'undetermined'

                with patch('clusterlab.executor._load_fastcheck', return_value=(
                        FakePool, check, bucket)), \
                     patch('fastcheck.workers.device_profile',
                           return_value={'client_info': 'test', 'user_agent': 'test'}), \
                     patch('clusterlab.executor._bump_usage'):
                    _execute_auth_unit(root, descriptor, threading.Event(), proxy_pool=shared)

                record = json.loads((root / 'results.jsonl').read_text())
                self.assertEqual(len(checked), 3)
                expected = ('undetermined' if name == '20970_with_unrecognized_reason'
                            else 'timeout')
                self.assertEqual((record['status'], record['attempts'], record['code']),
                                 (expected, 2, '-20970'))


if __name__ == '__main__':
    unittest.main()
