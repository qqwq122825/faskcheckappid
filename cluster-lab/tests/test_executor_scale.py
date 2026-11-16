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

from clusterlab.executor import (NodeProxyPool, _device_id_label, _execute_auth_unit, _partition_device_slots,
                                 _proxy_debug_enabled, _slot_for_account, atomic_json,
                                 execute_unit_group, proxy_supplier_loop,
                                 record_device_registration)


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
            with patch('clusterlab.executor._load_fastcheck', return_value=(
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

    def test_auth_unit_rotates_proxy_after_http_zero_and_reuses_http_200(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'input.part').write_text('account----password\n', encoding='utf-8')
            descriptor = {'rows': 1, 'first_row': 1, 'threads': 1,
                          'device_capacity': 1, 'device_root': str(root / 'devices'),
                          'origin': 'http://127.0.0.1:1', 'timeout': 5, 'retries': 1}
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
            self.assertEqual(stop.waits, [5.0])

    def test_auth_unit_discards_http_503_waits_then_rotates_proxy(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'input.part').write_text('account----password\n', encoding='utf-8')
            descriptor = {'rows': 1, 'first_row': 1, 'threads': 1,
                          'device_capacity': 1, 'device_root': str(root / 'devices'),
                          'origin': 'http://127.0.0.1:1', 'timeout': 5, 'retries': 1,
                          'proxy_api': 'http://proxy.invalid/list'}
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
            self.assertEqual(stop.waits, [5.0])

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
                 patch('clusterlab.executor.time.monotonic', side_effect=[10.0, 10.3]), \
                 patch('clusterlab.executor.atomic_json', wraps=atomic_json) as writes:
                _execute_auth_unit(root, descriptor, threading.Event())

            progress = [call.args[1] for call in writes.call_args_list
                        if call.args[0].name == 'progress.json']
            self.assertEqual([item['processed'] for item in progress], [1, 1])
            self.assertEqual(progress[0]['counts'], {'wrong_password': 1})

    def test_auth_unit_classifies_gsa_20970_as_invalid_without_web_recheck(self):
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
                                               'apple_ec': -20970, 'ms': 5},
                    lambda result: 'invalid' if result.get('apple_ec') == -20970 else 'undetermined')), \
                 patch('fastcheck.workers.device_profile',
                       return_value={'client_info': 'test', 'user_agent': 'test'}), \
                 patch('clusterlab.executor._bump_usage'):
                _execute_auth_unit(root, descriptor, threading.Event())

            record = json.loads((root / 'results.jsonl').read_text())
            self.assertEqual(record['status'], 'invalid')
            self.assertEqual(record['protocol'], 'GSA')
            self.assertEqual((record['http'], record['code']), (200, '-20970'))
            self.assertNotIn('gsa_code', record)
            self.assertNotIn('web_code', record)
            done = json.loads((root / 'done.json').read_text())
            self.assertEqual(done['counts'], {'invalid': 1})


if __name__ == '__main__':
    unittest.main()
