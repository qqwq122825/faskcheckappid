import hashlib
import io
import json
import multiprocessing as mp
from collections import Counter
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from clusterlab.node import Node
from clusterlab.device_build import REGISTRATION_TIMEOUT, _register
from clusterlab.executor import (_partition_one_time_device_slots,
                                 _replace_device_identity)
from clusterlab.web import create_app


class Response:
    def __init__(self, value):
        self.value = value

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def json(self):
        return self.value


def _construct_node_in_spawned_child(root):
    node = Node(root)
    node.close()


class NodeRecoveryTests(unittest.TestCase):
    def test_device_registration_allows_realistic_provisioning_time(self):
        self.assertGreaterEqual(REGISTRATION_TIMEOUT, 90)

    def test_registration_error_sanitizer_never_masks_original_failure(self):
        class Sink:
            value = None

            def put(inner, value):
                inner.value = value

        output = Sink()
        with patch('clusterlab.device_build._load_fastcheck',
                   side_effect=RuntimeError('password=hunter2 endpoint failed')), \
             patch('signal.signal'):
            _register({}, output)
        self.assertEqual(output.value['error'],
                         'RuntimeError: password=*** endpoint failed')

    def test_pre_registration_uses_device_profile_without_public_info_request(self):
        captured = {}

        class Sink:
            value = None

            def put(inner, value):
                inner.value = value

        class Pool:
            def __init__(inner, *_args, **_kwargs):
                pass

            def operation(inner, _slot):
                return Response(None)

            def info(inner, *_args, **_kwargs):
                raise AssertionError('public info must not be requested')

            def fetch_headers(inner, slot, info, *_args, **kwargs):
                captured.update(slot=slot, info=info, registered=kwargs.get('registered'))
                return {'identity': 'registered'}

        output = Sink()
        payload = {'device_root': '/tmp/probe', 'origin': 'http://127.0.0.1:1',
                   'capacity': 2000, 'slot': 1001, 'timeout': 90,
                   'proxy': 'socks5h://proxy'}
        profile = {'model': 'MacBookPro14,3', 'client_info': '<device-profile>',
                   'user_agent': 'device-user-agent'}
        with patch('clusterlab.device_build._load_fastcheck',
                   return_value=(Pool, None, None)), \
             patch('fastcheck.workers.device_profile', return_value=profile), \
             patch('signal.signal'):
            _register(payload, output)
        self.assertEqual(output.value, {'ok': True, 'model': 'MacBookPro14,3'})
        self.assertEqual(captured, {'slot': 1001,
                                   'info': {'client_info': '<device-profile>',
                                            'user_agent': 'device-user-agent'},
                                   'registered': False})

    def test_spawned_child_cannot_interrupt_parent_live_task(self):
        with tempfile.TemporaryDirectory() as root:
            primary = Node(root)
            unit = 'c' * 32
            with primary.db.transaction() as db:
                db.execute(
                    'INSERT INTO tasks(id,job_id,seq,descriptor,status,processed,created,updated) '
                    'VALUES(?,?,?,?,?,?,?,?)',
                    (unit, 'd' * 32, 1, '{}', 'running', 41, 1, 1))
            try:
                child = mp.get_context('spawn').Process(
                    target=_construct_node_in_spawned_child, args=(root,))
                child.start()
                child.join(10)
                self.assertEqual(child.exitcode, 0)
                with primary.db.connect() as db:
                    task = db.execute('SELECT status,error FROM tasks WHERE id=?',
                                      (unit,)).fetchone()
                self.assertEqual(task['status'], 'running')
                self.assertIsNone(task['error'])
            finally:
                primary.close()

    def test_second_top_level_instance_cannot_interrupt_live_task(self):
        with tempfile.TemporaryDirectory() as root:
            primary = Node(root)
            unit = 'e' * 32
            with primary.db.transaction() as db:
                db.execute(
                    'INSERT INTO tasks(id,job_id,seq,descriptor,status,processed,created,updated) '
                    'VALUES(?,?,?,?,?,?,?,?)',
                    (unit, 'f' * 32, 1, '{}', 'running', 73, 1, 1))
            secondary = Node(root)
            try:
                self.assertTrue(primary.primary_instance)
                self.assertFalse(secondary.primary_instance)
                with primary.db.connect() as db:
                    task = db.execute('SELECT status,error FROM tasks WHERE id=?',
                                      (unit,)).fetchone()
                self.assertEqual(task['status'], 'running')
                self.assertIsNone(task['error'])
            finally:
                secondary.close()
                primary.close()

    def test_real_service_restart_still_recovers_live_task(self):
        with tempfile.TemporaryDirectory() as root:
            primary = Node(root)
            unit = '1' * 32
            with primary.db.transaction() as db:
                db.execute(
                    'INSERT INTO tasks(id,job_id,seq,descriptor,status,processed,created,updated) '
                    'VALUES(?,?,?,?,?,?,?,?)',
                    (unit, '2' * 32, 1, '{}', 'running', 9, 1, 1))
            primary.close()
            restarted = Node(root)
            try:
                with restarted.db.connect() as db:
                    task = db.execute('SELECT status,error FROM tasks WHERE id=?',
                                      (unit,)).fetchone()
                self.assertEqual(task['status'], 'interrupted')
                self.assertEqual(task['error'], '节点重启，等待总控确认')
            finally:
                restarted.close()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.node = Node(self.temp.name)
        self.unit = 'a' * 32
        directory = self.node.spool / self.unit
        directory.mkdir()
        self.result_line = json.dumps({
            'row': 1, 'account': 'user@example.com', 'status': 'valid',
            'sha256': hashlib.sha256(b'hello').hexdigest(), 'bytes': 5},
            separators=(',', ':'))
        (directory / 'results.jsonl').write_text(self.result_line + '\n')
        with self.node.db.transaction() as db:
            db.execute('INSERT INTO tasks(id,job_id,seq,descriptor,status,processed,checksum,created,updated) VALUES(?,?,?,?,?,?,?,?,?)',
                       (self.unit, 'b'*32, 1, json.dumps({'token':'test-lease','rows':1,'threads':1}), 'outbox', 1, 'digest', 1, 1))

    def tearDown(self):
        self.node.close()
        self.temp.cleanup()

    def test_legacy_service_limits_are_migrated_to_four_by_five_hundred(self):
        with tempfile.TemporaryDirectory() as root:
            legacy = Node(root, max_processes=10, max_threads=100)
            try:
                self.assertEqual((legacy.max_processes, legacy.max_threads), (4, 500))
            finally:
                legacy.close()

    def test_public_task_log_returns_bounded_sanitized_json(self):
        directory = self.node.spool / self.unit
        descriptor = {
            'token': 'top-secret-lease', 'rows': 300, 'threads': 250,
            'processes': 4, 'mode': 'auth', 'timeout': 20, 'retries': 1,
            'proxy_api': 'https://user:password@proxy.invalid/list',
            'device_root': '/private/device-pool', 'auth_protocol': 'gsa'}
        descriptor.update(_proxy_debug=True, _proxy_debug_mode='sampled',
                          _proxy_debug_sample_every=37)
        with self.node.db.transaction() as db:
            db.execute('UPDATE tasks SET descriptor=?,status=?,processed=? WHERE id=?',
                       (json.dumps(descriptor), 'running', 120, self.unit))
        (directory / 'manifest.json').write_text(json.dumps(descriptor), 'utf-8')
        (directory / 'progress.json').write_text(json.dumps({
            'processed': 120, 'workers': 3, 'worker_limit': 3,
            'configured_process_limit': 4, 'cpu_reserve': 1,
            'threads_per_process': 250, 'active_threads_per_process': 333,
            'srp_gate': {'limit': 3, 'active': 2, 'peak': 3, 'waiters': 1},
            'counts': {'wrong_password': 110, 'locked': 10},
            'proxy_pool': {'available': 240, 'endpoint': 'socks5://secret'}}), 'utf-8')
        (directory / 'proxy-debug.jsonl').write_text('\n'.join((
            json.dumps({'event': 'worker_start', 'srp_math_backend': 'gmpy2',
                        'proxy': 'socks5://user:pass@host'}),
            json.dumps({'event': 'auth_timing', 'row': 7, 'request_ms': 210.0,
                        'request_thread_cpu_ms': 4.0,
                        'endpoint': 'user:pass@10.0.0.1:1080',
                        'detail': 'password=hunter2',
                        'srp': {'scope': 'sampled_attempt_leaf_phases',
                                'state': 'observed', 'compute_cpu_ms': 8.5,
                                'compute_wall_ms': 9.0, 'lane_wait_ms': 2.0,
                                'local_lane_wait_ms': 0.5,
                                'global_lane_wait_ms': 1.5}}),
        )) + '\n', 'utf-8')
        self.node.telemetry.latest = {'cpu_percent': 81.2, 'memory_percent': 33.0}

        client = create_app('node', self.node, password='not-used-here').test_client()
        response = client.post('/node-api/task-log', json={
            'task': {'id': self.unit, 'job_id': 'b' * 32, 'seq': 1},
            'recent': 10})
        self.assertEqual(response.status_code, 200)
        value = response.get_json()
        self.assertTrue(value['public_endpoint'])
        self.assertEqual(value['progress']['worker_limit'], 3)
        self.assertEqual(value['progress']['cpu_reserve'], 1)
        self.assertEqual(value['progress']['srp_gate'],
                         {'limit': 3, 'active': 2, 'peak': 3, 'waiters': 1})
        self.assertEqual(value['node']['telemetry']['cpu_percent'], 81.2)
        self.assertEqual(value['configuration']['logging'], {
            'mode': 'sampled', 'sample_every': 37,
            'sampling_strategy': 'stable_hash'})
        self.assertEqual(value['debug']['srp']['compute_cpu_ms']['total'], 8.5)
        self.assertEqual(value['debug']['srp']['global_lane_wait_ms']['total'], 1.5)
        self.assertEqual(value['debug']['srp']['math_backends'], {'gmpy2': 1})
        encoded = json.dumps(value)
        for secret in ('top-secret-lease', 'user:password', 'hunter2',
                       '10.0.0.1', '/private/device-pool', 'proxy.invalid'):
            self.assertNotIn(secret, encoded)

    def test_task_log_rejects_path_and_identity_mismatch(self):
        with self.assertRaisesRegex(ValueError, '32 位'):
            self.node.task_log({'task': {'id': '../../etc/passwd'}})
        with self.assertRaisesRegex(ValueError, 'job_id'):
            self.node.task_log({'task': {'id': self.unit, 'job_id': 'c' * 32}})
        with self.assertRaisesRegex(ValueError, 'lease_token'):
            self.node.task_log({'task': {'id': self.unit, 'lease_token': 'wrong'}})

    def test_public_task_log_rejects_oversize_body_before_json_decode(self):
        client = create_app('node', self.node, password='not-used-here').test_client()
        response = client.post('/node-api/task-log', data=b'{' + b'x' * 20000,
                               content_type='application/json')
        self.assertEqual(response.status_code, 400)
        self.assertIn('16 KiB', response.get_json()['error'])

    def test_stalled_coordinator_is_terminated_and_reported(self):
        class WedgedProcess:
            def __init__(inner):
                inner.alive = True
                inner.terminated = False

            def is_alive(inner):
                return inner.alive

            def join(inner, _timeout=None):
                pass

            def terminate(inner):
                inner.terminated = True
                inner.alive = False

            def close(inner):
                pass

        process = WedgedProcess()
        stop = threading.Event()
        directory = self.node.spool / self.unit
        (directory / 'manifest.json').write_text(
            json.dumps({'timeout': 20, 'retries': 0, 'device_slots': []}), 'utf-8')
        (directory / 'progress.json').write_text(json.dumps({'processed': 7}), 'utf-8')
        with self.node.db.transaction() as db:
            db.execute("UPDATE tasks SET status='running',processed=7 WHERE id=?", (self.unit,))
        self.node.active[self.unit] = (process, stop)
        self.node.progress_watch[self.unit] = {'processed': 7, 'changed': 0.0}
        with patch('clusterlab.node.time.monotonic', return_value=1000.0), \
             patch.object(self.node.builder, 'release'):
            self.node._poll_processes()
        with self.node.db.connect() as db:
            task = db.execute('SELECT status,processed,error FROM tasks WHERE id=?',
                              (self.unit,)).fetchone()
        self.assertTrue(process.terminated)
        self.assertTrue(stop.is_set())
        self.assertEqual((task['status'], task['processed']), ('interrupted', 7))
        self.assertIn('无进度', task['error'])

    def test_upload_failure_keeps_outbox_and_retry_confirms(self):
        with patch.object(self.node, '_request', side_effect=ConnectionError('offline')):
            with self.assertRaises(ConnectionError):
                self.node._upload_one()
        self.assertEqual(self.node.snapshot()['outbox'], 1)
        with patch.object(self.node, '_request', return_value=Response({'accepted':True})):
            self.node._upload_one()
        self.assertEqual(self.node.snapshot()['confirmed'], 1)
        self.assertEqual(self.node.snapshot()['outbox'], 0)

    def test_lost_upload_ack_can_be_confirmed_by_heartbeat(self):
        value = {'node_id':'node','name':'node','processes':1,'threads':1,'enabled':True,
                 'rejected':[],'confirmed':[self.unit]}
        with patch.object(self.node, '_request', return_value=Response(value)):
            self.node._heartbeat()
        self.assertEqual(self.node.snapshot()['confirmed'], 1)
        self.assertEqual(self.node.snapshot()['outbox'], 0)

    def test_heartbeat_includes_live_category_counts(self):
        (self.node.spool / self.unit / 'progress.json').write_text(
            json.dumps({'processed': 1, 'counts': {'wrong_password': 1}}), 'utf-8')
        captured = {}
        value = {'node_id': 'node', 'name': 'node', 'processes': 1, 'threads': 1,
                 'enabled': True, 'rejected': [], 'confirmed': []}

        def request(*_args, **kwargs):
            captured.update(kwargs['json'])
            return Response(value)

        with patch.object(self.node, '_request', side_effect=request):
            self.node._heartbeat()
        self.assertEqual(captured['active'][0]['processed'], 1)
        self.assertEqual(captured['active'][0]['counts'], {'wrong_password': 1})

    def test_heartbeat_includes_device_proxy_health(self):
        captured = {}
        value = {'node_id': 'node', 'name': 'node', 'processes': 1, 'threads': 1,
                 'enabled': True, 'rejected': [], 'confirmed': []}
        snapshot = {'running': True, 'capacity': 20, 'ready': 0, 'built': 0,
                    'failed': 3, 'current': 4, 'attempted': 3,
                    'proxy_status': 'ok', 'proxy_ip': '1.2.3.4:80',
                    'message': '正在注册第 4 台', 'slots': []}

        def request(*_args, **kwargs):
            captured.update(kwargs['json'])
            return Response(value)

        with patch.object(self.node.builder, 'snapshot', return_value=snapshot), \
             patch.object(self.node, '_request', side_effect=request):
            self.node._heartbeat()
        self.assertEqual(captured['devices']['proxy_status'], 'ok')
        self.assertEqual(captured['devices']['proxy_ip'], '1.2.3.4:80')
        self.assertEqual(captured['devices']['message'], '正在注册第 4 台')

    def test_heartbeat_reports_update_status_and_schedules_new_generation(self):
        with self.node.db.transaction() as db:
            db.execute("UPDATE tasks SET status='confirmed'")
        self.node.update_generation = 4
        self.node._save_control()
        self.node.update_status_path.write_text(json.dumps({
            'generation': 4, 'state': 'verifying', 'message': '正在验证节点健康状态'}), 'utf-8')
        captured = {}
        value = {'node_id': 'node', 'name': 'node', 'processes': 1, 'threads': 1,
                 'enabled': False, 'rejected': [], 'confirmed': [],
                 'update': {'generation': 5, 'ref': 'abc1234', 'service': 'cluster-node'}}

        def request(*_args, **kwargs):
            captured.update(kwargs['json'])
            return Response(value)

        with patch.object(self.node, '_request', side_effect=request), \
             patch.object(self.node, '_schedule_update', return_value=True) as schedule:
            self.node._heartbeat()
        self.assertEqual(captured['ack_update_generation'], 4)
        self.assertEqual(captured['update_status']['state'], 'verifying')
        schedule.assert_called_once_with(value['update'])

    def test_schedule_update_uses_detached_systemd_unit(self):
        with self.node.db.transaction() as db:
            db.execute("UPDATE tasks SET status='confirmed'")
        with patch('clusterlab.node.shutil.which', return_value='/usr/bin/systemd-run'), \
             patch('clusterlab.node.subprocess.Popen') as popen:
            self.assertTrue(self.node._schedule_update({
                'generation': 2, 'ref': 'abc1234', 'service': 'cluster-node'}))
        command = popen.call_args.args[0]
        self.assertEqual(command[:4], ['/usr/bin/systemd-run', '--unit',
                                      'cluster-node-update-2', '--collect'])
        self.assertIn('--service', command)
        self.assertIn('cluster-node', command)
        self.assertEqual(self.node.update_generation, 2)
        self.assertEqual(json.loads(self.node.update_status_path.read_text())['state'], 'scheduled')

    def test_heartbeat_reports_result_lines_once_per_controller_ack(self):
        captured = []
        value = {'node_id': 'node', 'name': 'node', 'processes': 1, 'threads': 1,
                 'enabled': True, 'rejected': [], 'confirmed': [],
                 'reported': {self.unit: 1}}

        def request(*_args, **kwargs):
            captured.append(kwargs['json'])
            return Response(value)

        with patch.object(self.node, '_request', side_effect=request):
            self.node._heartbeat()
            self.node._heartbeat()
        self.assertEqual(captured[0]['active'][0]['records'], [self.result_line])
        self.assertNotIn('records', captured[1]['active'][0])
        state = json.loads((self.node.spool / self.unit / 'report-state.json').read_text())
        self.assertGreater(state['files']['results.jsonl'], 0)

    def test_pending_results_block_controller_switch(self):
        with self.assertRaisesRegex(ValueError, '结果待上传'):
            self.node.connect('http://127.0.0.1:18772','x'*32)

    def test_auth_task_plans_at_most_one_hundred_accounts_per_device(self):
        descriptor = {'mode': 'auth', 'rows': 10000, 'seq': 2,
                      'processes': 10, 'device_capacity': 30,
                      '_auth_protocol': 'gsa'}
        ready = {'slots': [
            {'slot': 2, 'state': 'ready'}, {'slot': 4, 'state': 'failed'},
            {'slot': 7, 'state': 'ready'}, {'slot': 9, 'state': 'ready'}]}
        with patch.object(self.node.builder, 'snapshot', return_value=ready):
            self.node._prepare_auth_devices(descriptor)
        counts = Counter(descriptor['device_plan'])
        self.assertEqual(len(descriptor['device_plan']), 10000)
        self.assertTrue(counts)
        self.assertLessEqual(max(counts.values()), 100)
        self.assertEqual(len(counts), 100)
        self.assertEqual(descriptor['processes'], 10)
        self.assertIn(4, descriptor['device_reset_slots'])
        self.assertEqual(json.loads((self.node.root / 'device-pool' / 'usage.json').read_text()),
                         {str(slot): count for slot, count in counts.items()})
        self.assertNotIn('device_capacity', descriptor)
        self.assertEqual(self.node.builder.reserved, set(counts))
        self.node.builder.release(descriptor['device_slots'])

    def test_auth_task_with_no_devices_registers_before_verifying(self):
        descriptor = {'mode': 'auth', 'rows': 50, 'processes': 4,
                      '_auth_protocol': 'gsa'}
        with patch.object(self.node.builder, 'snapshot', return_value={'slots': []}):
            self.node._prepare_auth_devices(descriptor)
        self.assertEqual(len(descriptor['device_plan']), 50)
        self.assertEqual(len(set(descriptor['device_plan'])), 4)
        self.assertEqual(descriptor['device_reset_slots'], [])
        self.assertEqual(descriptor['processes'], 4)

    def test_one_time_mode_extends_configured_shared_pool_and_sets_retry(self):
        descriptor = {'mode': 'auth', 'rows': 50, 'processes': 4, 'threads': 25,
                      '_auth_protocol': 'gsa', '_device_mode': 'one_time',
                      'device_registration_workers': 32, 'device_usage_limit': 100}
        with patch.object(self.node.builder, 'snapshot', return_value={
                'capacity': 1000, 'slots': [{'slot': index, 'state': 'ready'}
                                            for index in range(1, 1001)]}) as snapshot, \
             patch.object(self.node.builder, 'reserve_usage') as reserve:
            self.node._prepare_auth_devices(descriptor)
        snapshot.assert_called_once_with()
        reserve.assert_not_called()
        self.assertEqual(descriptor['device_slots'], list(range(1, 1051)))
        self.assertEqual(descriptor['device_ready_slots'], list(range(1, 1001)))
        self.assertEqual(descriptor['device_overflow_slots'], list(range(1001, 1051)))
        self.assertEqual(descriptor['device_base_capacity'], 1000)
        self.assertEqual(descriptor['device_pool_capacity'], 1050)
        self.assertEqual(descriptor['device_usage_limit'], 10)
        self.assertEqual(descriptor['device_cooldown_seconds'], 3.0)
        self.assertNotIn('device_plan', descriptor)
        self.assertEqual(descriptor['_retry_delay'], 10.0)
        self.assertEqual(descriptor['_one_time_min_retries'], 1)

    def test_two_thousand_pool_supports_eight_hundred_thread_extension_model(self):
        descriptor = {'mode': 'auth', 'rows': 800, 'processes': 4, 'threads': 200,
                      '_auth_protocol': 'gsa', '_device_mode': 'one_time',
                      'device_usage_limit': 10}
        with patch.object(self.node.builder, 'snapshot', return_value={
                'capacity': 2000, 'slots': [
                    {'slot': index, 'state': 'ready'} for index in range(1, 2001)]}):
            self.node._prepare_auth_devices(descriptor)
        self.assertEqual(descriptor['device_base_capacity'], 2000)
        self.assertEqual(descriptor['device_pool_capacity'], 2800)
        self.assertEqual(descriptor['device_slots'][0], 1)
        self.assertEqual(descriptor['device_slots'][-1], 2800)
        self.assertEqual(descriptor['device_ready_slots'], list(range(1, 2001)))
        self.assertEqual(descriptor['device_overflow_slots'], list(range(2001, 2801)))
        self.assertEqual(descriptor['device_cooldown_seconds'], 3.0)
        self.assertEqual(descriptor['device_usage_limit'], 10)

    def test_zero_device_pool_gives_every_active_thread_a_registration_slot(self):
        descriptor = {'mode': 'auth', 'rows': 1000, 'processes': 4, 'threads': 250,
                      '_auth_protocol': 'gsa', '_device_mode': 'one_time'}
        with patch.object(self.node.builder, 'snapshot', return_value={
                'capacity': 0, 'slots': []}), \
             patch.object(self.node.builder, 'usage', return_value={}):
            self.node._prepare_auth_devices(descriptor)
        self.assertEqual(descriptor['device_base_capacity'], 0)
        self.assertEqual(descriptor['device_ready_slots'], [])
        self.assertEqual(descriptor['device_overflow_slots'], list(range(1, 1001)))
        self.assertEqual(descriptor['device_usage_limit'], 10)

    def test_two_nodes_with_one_hundred_ready_devices_send_nine_hundred_threads_to_registration(self):
        snapshot = {'capacity': 100, 'slots': [
            {'slot': slot, 'state': 'ready'} for slot in range(1, 101)]}
        observed = []
        node_keys = []
        for node_index in range(2):
            descriptor = {'mode': 'auth', 'rows': 10000, 'processes': 4,
                          'threads': 250, '_auth_protocol': 'gsa',
                          '_device_mode': 'one_time'}
            with patch.object(self.node.builder, 'snapshot', return_value=snapshot), \
                 patch.object(self.node.builder, 'usage', return_value={}):
                self.node._prepare_auth_devices(descriptor)
            ready_taken = registration_taken = waiting = 0
            keys = set()
            for process_index in range(4):
                ready, overflow = _partition_one_time_device_slots(
                    descriptor['device_ready_slots'],
                    descriptor['device_overflow_slots'], 4, process_index)
                ready_now = min(250, len(ready))
                register_now = min(250 - ready_now, len(overflow))
                ready_taken += ready_now
                registration_taken += register_now
                waiting += 250 - ready_now - register_now
                keys.update((node_index, slot) for slot in ready[:ready_now])
                keys.update((node_index, slot) for slot in overflow[:register_now])
            observed.append((ready_taken, registration_taken, waiting,
                             len(descriptor['device_overflow_slots'])))
            node_keys.append(keys)
        self.assertEqual(observed, [(100, 900, 0, 1000),
                                    (100, 900, 0, 1000)])
        self.assertTrue(node_keys[0].isdisjoint(node_keys[1]))

    def test_web_task_does_not_require_or_reserve_devices(self):
        descriptor = {'mode': 'auth', 'rows': 50, 'processes': 4,
                      '_auth_protocol': 'web'}
        with patch.object(self.node.builder, 'snapshot') as snapshot, \
             patch.object(self.node.builder, 'reserve_usage') as reserve:
            self.node._prepare_auth_devices(descriptor)
        snapshot.assert_not_called()
        reserve.assert_not_called()
        self.assertEqual(descriptor['device_slots'], [1])
        self.assertNotIn('device_plan', descriptor)

    def test_usage_reservation_is_hard_and_reset_starts_new_identity_at_zero(self):
        pool = self.node.root / 'device-pool'
        pool.mkdir()
        (pool / 'usage.json').write_text('{"1":99}', 'utf-8')
        self.assertEqual(self.node.builder.reserve_usage([1], 100), {'1': 99})
        with self.assertRaisesRegex(RuntimeError, '额度不足'):
            self.node.builder.reserve_usage([1], 100)
        self.assertEqual(self.node.builder.reserve_usage([1] * 100, 100, [1]),
                         {'1': 0})
        self.assertEqual(json.loads((pool / 'usage.json').read_text()), {'1': 100})

    def test_all_full_devices_are_scheduled_for_replacement_before_auth(self):
        pool = self.node.root / 'device-pool'
        pool.mkdir()
        (pool / 'usage.json').write_text(
            json.dumps({str(slot): 100 for slot in range(1, 5)}), 'utf-8')
        snapshot = {'slots': [
            {'slot': slot, 'state': 'ready'} for slot in range(1, 5)]}
        descriptor = {'mode': 'auth', 'rows': 400, 'processes': 4,
                      '_auth_protocol': 'gsa', 'device_usage_limit': 100}
        with patch.object(self.node.builder, 'snapshot', return_value=snapshot):
            self.node._prepare_auth_devices(descriptor)
        self.assertEqual(descriptor['device_reset_slots'], [1, 2, 3, 4])
        self.assertEqual(Counter(descriptor['device_plan']),
                         Counter({1: 100, 2: 100, 3: 100, 4: 100}))

    def test_two_hundred_fifty_threads_use_distinct_planned_devices_without_thread_registration(self):
        pool = self.node.root / 'device-pool'
        pool.mkdir()
        usage = {str(slot): (0 if slot == 100 else 100) for slot in range(1, 101)}
        (pool / 'usage.json').write_text(json.dumps(usage), 'utf-8')
        snapshot = {'slots': [
            {'slot': slot, 'state': 'ready'} for slot in range(1, 101)]}
        descriptor = {'mode': 'auth', 'rows': 250, 'processes': 4,
                      'threads': 250, '_auth_protocol': 'gsa',
                      'device_usage_limit': 100}
        with patch.object(self.node.builder, 'snapshot', return_value=snapshot):
            self.node._prepare_auth_devices(descriptor)
        counts = Counter(descriptor['device_plan'])
        self.assertEqual(sum(counts.values()), 250)
        self.assertEqual(len(counts), 250)
        self.assertLessEqual(max(counts.values()), 100)
        self.assertEqual(len(descriptor['device_reset_slots']), 99)

    def test_four_by_two_hundred_fifty_uses_one_thousand_prebuilt_devices_evenly(self):
        snapshot = {'slots': [
            {'slot': slot, 'state': 'ready'} for slot in range(1, 1001)]}
        descriptor = {'mode': 'auth', 'rows': 10000, 'processes': 4,
                      'threads': 250, '_auth_protocol': 'gsa',
                      'device_usage_limit': 100}
        with patch.object(self.node.builder, 'snapshot', return_value=snapshot):
            self.node._prepare_auth_devices(descriptor)
        counts = Counter(descriptor['device_plan'])
        self.assertEqual(len(counts), 1000)
        self.assertEqual(set(counts.values()), {10})
        self.assertEqual(descriptor['device_reset_slots'], [])

    def test_four_by_two_hundred_fifty_uses_second_wave_of_two_thousand_devices(self):
        snapshot = {'slots': [
            {'slot': slot, 'state': 'ready'} for slot in range(1, 2001)]}
        descriptor = {'mode': 'auth', 'rows': 20000, 'processes': 4,
                      'threads': 250, '_auth_protocol': 'gsa',
                      'device_usage_limit': 100}
        with patch.object(self.node.builder, 'snapshot', return_value=snapshot):
            self.node._prepare_auth_devices(descriptor)
        counts = Counter(descriptor['device_plan'])
        self.assertEqual(len(counts), 2000)
        self.assertEqual(set(counts.values()), {10})
        self.assertEqual(descriptor['device_cooldown_seconds'], 3.0)
        self.assertEqual(descriptor['device_reset_slots'], [])
        child_rows = len(descriptor['device_plan']) // 4
        for process in range(4):
            child = descriptor['device_plan'][process * child_rows:(process + 1) * child_rows]
            self.assertTrue(set(child[:250]).isdisjoint(child[250:500]))

    def test_quota_replacement_restores_old_device_when_registration_fails(self):
        root = Path(self.temp.name) / 'quota-replace'

        class Pool:
            base = root / 'origin'

            def slot_directory(inner, slot):
                return inner.base / 'devices' / f'{slot:02d}'

            def fetch_headers(inner, *_args, **_kwargs):
                raise RuntimeError('registration failed')

            def inspect(inner, _slot):
                return {'state': 'missing'}

        pool = Pool()
        target = pool.slot_directory(1)
        target.mkdir(parents=True)
        (target / 'old.json').write_text('old', 'utf-8')

        def profile(_pool, _slot):
            target.mkdir(parents=True, exist_ok=True)
            (target / 'new.json').write_text('new', 'utf-8')
            return {'model': 'MacBookPro14,3', 'client_info': 'client',
                    'user_agent': 'agent'}

        with self.assertRaisesRegex(RuntimeError, 'registration failed'):
            _replace_device_identity(pool, root, 1, {}, 20, 'proxy', profile)
        self.assertEqual((target / 'old.json').read_text(), 'old')
        self.assertFalse((target / 'new.json').exists())
        self.assertEqual(json.loads((root / 'usage.json').read_text()), {'1': 100})

    def test_quota_replacement_commits_new_device_before_verification(self):
        root = Path(self.temp.name) / 'quota-replace-ok'

        class Pool:
            base = root / 'origin'

            def slot_directory(inner, slot):
                return inner.base / 'devices' / f'{slot:02d}'

            def fetch_headers(inner, *_args, **_kwargs):
                (inner.slot_directory(1) / 'ready.json').write_text('ready', 'utf-8')
                return {'identity': 'new'}

            def inspect(inner, slot):
                return {'state': ('ready' if (inner.slot_directory(slot) / 'ready.json').exists()
                                  else 'missing'), 'device_id': 'new-device'}

        pool = Pool()
        target = pool.slot_directory(1)
        target.mkdir(parents=True)
        (target / 'old.json').write_text('old', 'utf-8')

        def profile(_pool, _slot):
            target.mkdir(parents=True, exist_ok=True)
            return {'model': 'MacBookPro14,3', 'client_info': 'client',
                    'user_agent': 'agent'}

        identity, _profile = _replace_device_identity(
            pool, root, 1, {}, 20, 'socks5h://proxy', profile)
        self.assertEqual(identity, {'identity': 'new'})
        self.assertFalse((target / 'old.json').exists())
        self.assertTrue((target / 'ready.json').exists())
        registration = json.loads((root / 'registrations' / '001.json').read_text())
        self.assertEqual(registration['model'], 'MacBookPro14,3')

    def test_background_builder_never_registers_without_a_proxy(self):
        class Pool:
            def __init__(inner, *_args, **_kwargs):
                pass

            def inspect(inner, _slot):
                return {'state': 'missing'}

        with patch('clusterlab.device_build._load_fastcheck', return_value=(Pool, None, None)), \
             patch('clusterlab.device_build.mp.get_context', return_value=object()), \
             patch.object(self.node.builder, '_proxy', return_value=None):
            self.node.builder._run(1)
        self.assertEqual(self.node.builder.state['ready'], 0)
        self.assertEqual(self.node.builder.state['failed'], 1)
        self.assertIn('禁止直连注册', self.node.builder.state['message'])
        self.assertEqual(self.node.builder.state['proxy_status'], 'failed')
        self.assertEqual(self.node.builder.state['proxy_ip'], '')
        self.assertEqual(self.node.builder.state['slot_errors']['1'], '代理获取失败')
        self.assertFalse((self.node.root / 'device-pool' / 'registrations').exists())

    def test_idle_builder_registers_thirty_two_devices_in_parallel(self):
        ready, guard = set(), threading.Lock()
        active = {'now': 0, 'max': 0}

        class Pool:
            def __init__(inner, *_args, **_kwargs):
                pass

            def inspect(inner, slot):
                return {'state': 'ready' if slot in ready else 'missing'}

        def provision(payload):
            with guard:
                active['now'] += 1
                active['max'] = max(active['max'], active['now'])
            threading.Event().wait(.04)
            with guard:
                ready.add(payload['slot'])
                active['now'] -= 1
            return {'ok': True, 'model': 'MacBookPro14,3'}

        self.node.builder.state.update(running=True, capacity=32, workers=32)
        with patch('clusterlab.device_build._load_fastcheck',
                   return_value=(Pool, None, None)), \
             patch.object(self.node.builder, '_proxy',
                          side_effect=[f'socks5h://proxy-{slot}' for slot in range(32)]), \
             patch.object(self.node.builder, '_provision', side_effect=provision), \
             patch.object(self.node.builder, '_record', return_value=True):
            self.node.builder._run(32)
        self.assertEqual(active['max'], 32)
        self.assertEqual((self.node.builder.state['built'], self.node.builder.state['ready']),
                         (32, 32))

    def test_snapshot_distinguishes_registering_failed_and_pending_slots(self):
        class Pool:
            def __init__(inner, root, *_args, **_kwargs):
                inner.base = Path(root) / 'pool'

            def inspect(inner, slot):
                return {'state': 'pending', 'device_id': f'device-{slot}'}

        for slot in range(1, 4):
            (self.node.root / 'device-pool' / 'pool' / 'devices' /
             str(slot)).mkdir(parents=True)
        self.node.builder.state.update(running=True, capacity=3, current=2,
                                       slot_errors={'1': 'AnisetteError: HTTP 503'})
        with patch('clusterlab.device_build._load_fastcheck', return_value=(Pool, None, None)):
            snapshot = self.node.builder.snapshot()
        self.assertEqual([row['state'] for row in snapshot['slots']],
                         ['failed', 'registering', 'pending'])
        self.assertEqual(snapshot['slots'][0]['error'], 'AnisetteError: HTTP 503')

    def test_snapshot_skips_unmaterialized_large_pool_slots(self):
        inspected = []

        class Pool:
            key = 'origin-key'

            def __init__(inner, root, *_args, **_kwargs):
                inner.base = Path(root) / 'pool'

            def slot_directory(inner, slot):
                return inner.base / 'devices' / str(slot)

            def inspect(inner, slot):
                inspected.append(slot)
                return {'state': 'ready' if slot in (1, 9) else 'missing',
                        'device_id': f'device-{slot}'}

        for slot in (1, 9):
            (self.node.root / 'device-pool' / 'pool' / 'devices' /
             str(slot)).mkdir(parents=True)
        self.node.builder.state.update(running=True, capacity=2000,
                                       current_slots=[11],
                                       slot_errors={'12': 'registration failed'})
        with patch('clusterlab.device_build._load_fastcheck',
                   return_value=(Pool, None, None)):
            snapshot = self.node.builder.snapshot()
        self.assertEqual(inspected, [1, 9, 11, 12])
        self.assertEqual(snapshot['capacity'], 2000)
        self.assertEqual(snapshot['ready'], 2)
        self.assertEqual([row['slot'] for row in snapshot['slots']], [1, 9, 11, 12])

    def test_idle_snapshot_reuses_structural_scan_for_two_thousand_devices(self):
        inspected = []

        class Pool:
            key = 'origin-key'

            def __init__(inner, root, *_args, **_kwargs):
                inner.base = Path(root) / 'pool'

            def slot_directory(inner, slot):
                return inner.base / 'devices' / str(slot)

            def inspect(inner, slot):
                inspected.append(slot)
                return {'state': 'ready', 'device_id': f'device-{slot}'}

        devices = self.node.root / 'device-pool' / 'pool' / 'devices'
        for slot in range(1, 2001):
            (devices / str(slot)).mkdir(parents=True)
        self.node.builder.state.update(running=False, capacity=2000)
        with patch('clusterlab.device_build._load_fastcheck',
                   return_value=(Pool, None, None)):
            first = self.node.builder.snapshot()
            second = self.node.builder.snapshot()
            (self.node.root / 'device-pool' / 'usage.json').write_text(
                '{"1":7}', 'utf-8')
            third = self.node.builder.snapshot()
        self.assertEqual(len(inspected), 2000)
        self.assertEqual((first['ready'], second['ready'], third['ready']),
                         (2000, 2000, 2000))
        self.assertEqual(third['slots'][0]['used'], 7)

    def test_registration_change_invalidates_snapshot_cache(self):
        inspected = []

        class Pool:
            key = 'origin-key'

            def __init__(inner, root, *_args, **_kwargs):
                inner.base = Path(root) / 'pool'

            def slot_directory(inner, slot):
                return inner.base / 'devices' / str(slot)

            def inspect(inner, slot):
                inspected.append(slot)
                return {'state': 'ready', 'device_id': f'device-{slot}'}

        device = self.node.root / 'device-pool' / 'pool' / 'devices' / '1'
        device.mkdir(parents=True)
        with patch('clusterlab.device_build._load_fastcheck',
                   return_value=(Pool, None, None)):
            self.node.builder.snapshot()
            self.node.builder.snapshot()
            registrations = self.node.root / 'device-pool' / 'registrations'
            registrations.mkdir()
            (registrations / '001.json').write_text('{"slot":1}', 'utf-8')
            self.node.builder.snapshot()
        self.assertEqual(inspected, [1, 1])

    def test_ready_legacy_device_recovers_model_and_registration_time(self):
        class Pool:
            key = 'origin-key'

            def __init__(inner, root, _origin, max_devices):
                inner.base = Path(root) / 'pool'
                inner.max_devices = max_devices

            def slot_directory(inner, slot):
                return inner.base / f'{slot:02d}'

            def inspect(inner, slot):
                return {'state': 'ready', 'device_id': 'device-1'}

        self.node.builder.state['capacity'] = 1
        directory = self.node.root / 'device-pool' / 'pool' / '01'
        directory.mkdir(parents=True)
        (directory / 'device-profile.json').write_text(json.dumps({'model': 'MacBookPro14,3'}))
        state = directory / 'origin-key.json'
        state.write_text('{}')
        (self.node.root / 'device-pool' / 'usage.json').write_text('{"1":123}')
        with patch('clusterlab.device_build._load_fastcheck', return_value=(Pool, None, None)):
            snapshot = self.node.builder.snapshot()
        self.assertEqual(snapshot['attempted'], 1)
        self.assertEqual(snapshot['ready'], 1)
        self.assertEqual(snapshot['slots'][0]['label'], 'B64:ZGV2aWNl')
        self.assertEqual(snapshot['slots'][0]['model'], 'MacBookPro14,3')
        self.assertEqual(snapshot['slots'][0]['proxy'], '历史未记录')
        self.assertRegex(snapshot['slots'][0]['at'], r'^\d{4}-\d{2}-\d{2} ')
        self.assertFalse(snapshot['slots'][0]['recorded'])
        self.assertEqual(snapshot['slots'][0]['used'], 123)

    def test_snapshot_rediscovers_all_persisted_devices_after_restart(self):
        class Pool:
            key = 'origin-key'

            def __init__(inner, root, _origin, max_devices):
                inner.base = Path(root) / 'pool'
                inner.max_devices = max_devices

            def slot_directory(inner, slot):
                return inner.base / 'devices' / f'{slot:02d}'

            def inspect(inner, slot):
                state = 'ready' if slot in (1, 3) else 'pending'
                return {'state': state, 'device_id': f'device-{slot}'}

        for slot in range(1, 4):
            (self.node.root / 'device-pool' / 'pool' / 'devices' /
             f'{slot:02d}').mkdir(parents=True)
        self.node.builder.state['capacity'] = 0
        with patch('clusterlab.device_build._load_fastcheck', return_value=(Pool, None, None)):
            snapshot = self.node.builder.snapshot()
        self.assertEqual(snapshot['capacity'], 3)
        self.assertEqual(snapshot['ready'], 2)
        self.assertEqual([row['slot'] for row in snapshot['slots']], [1, 2, 3])

    def test_completed_manual_device_request_does_not_restart_each_heartbeat(self):
        value = {'node_id': 'node', 'name': 'node', 'processes': 1, 'threads': 1,
                 'enabled': True, 'rejected': [], 'confirmed': [], 'device_request': 1}
        complete = {'running': False, 'capacity': 1, 'ready': 1, 'built': 0,
                    'failed': 0, 'current': 0, 'attempted': 1, 'slots': []}
        with patch.object(self.node, '_request', return_value=Response(value)), \
             patch.object(self.node.builder, 'snapshot', return_value=complete), \
             patch.object(self.node.builder, 'start') as start:
            self.node._heartbeat()
        start.assert_not_called()

    def test_pending_validation_prevents_background_device_refill(self):
        value = {'node_id': 'node', 'name': 'node', 'processes': 1, 'threads': 1,
                 'enabled': True, 'rejected': [], 'confirmed': [],
                 'device_request': 2000, 'work_available': True}
        incomplete = {'running': False, 'capacity': 2000, 'ready': 1200,
                      'built': 0, 'failed': 0, 'current': 0,
                      'attempted': 1200, 'slots': []}
        with patch.object(self.node, '_request', return_value=Response(value)), \
             patch.object(self.node.builder, 'snapshot', return_value=incomplete), \
             patch.object(self.node.builder, 'start') as start:
            self.node._heartbeat()
        start.assert_not_called()

    def test_active_package_prevents_background_device_refill(self):
        value = {'node_id': 'node', 'name': 'node', 'processes': 1, 'threads': 1,
                 'enabled': True, 'rejected': [], 'confirmed': [],
                 'device_request': 2000, 'work_available': False}
        self.node.active['unit'] = (None, threading.Event())
        with patch.object(self.node, '_request', return_value=Response(value)), \
             patch.object(self.node.builder, 'start') as start:
            self.node._heartbeat()
        self.node.active.pop('unit', None)
        start.assert_not_called()

    def test_legacy_auto_rotation_directive_is_ignored(self):
        value = {'node_id': 'node', 'name': 'node', 'processes': 1, 'threads': 1,
                 'enabled': True, 'rejected': [], 'confirmed': [], 'device_request': 0,
                 'device_auto_rotate': True, 'device_rotate_after': 20}
        complete = {'running': False, 'capacity': 30, 'ready': 30, 'built': 0,
                    'failed': 0, 'current': 0, 'attempted': 30, 'slots': []}
        with patch.object(self.node, '_request', return_value=Response(value)), \
             patch.object(self.node.builder, 'snapshot', return_value=complete), \
             patch.object(self.node.builder, 'rotate') as rotate, \
             patch.object(self.node.builder, 'start') as start:
            self.node._heartbeat()
        rotate.assert_not_called()
        start.assert_not_called()

    def test_failed_staged_rotation_keeps_the_live_device(self):
        live = self.node.root / 'device-pool' / 'origin' / 'devices' / '01'
        live.mkdir(parents=True)
        (live / 'old').write_text('old', 'utf-8')

        class Pool:
            def __init__(inner, root, _origin, max_devices):
                inner.base = Path(root) / 'origin'
                inner.max_devices = max_devices

            def slot_directory(inner, slot):
                return inner.base / 'devices' / f'{slot:02d}'

            def inspect(inner, slot):
                return {'state': 'missing'}

        with patch('clusterlab.device_build._load_fastcheck', return_value=(Pool, None, None)), \
             patch.object(self.node.builder, '_proxy', return_value='socks5h://proxy'), \
             patch.object(self.node.builder, '_provision', return_value={'ok': False, 'error': 'failed'}), \
             patch.object(self.node.builder, '_commit_rotation') as commit:
            self.node.builder._run_rotation(1, 1)
        self.assertEqual((live / 'old').read_text('utf-8'), 'old')
        commit.assert_not_called()
        self.assertIn('原设备已保留', self.node.builder.state['message'])

    def test_successful_staged_rotation_swaps_then_removes_the_oldest_device(self):
        class Pool:
            def __init__(inner, root, _origin, max_devices):
                inner.base = Path(root) / 'origin'
                inner.max_devices = max_devices

            def slot_directory(inner, slot):
                return inner.base / 'devices' / f'{slot:02d}'

            def inspect(inner, slot):
                directory = inner.slot_directory(slot)
                return {'state': 'ready' if (directory / 'ready').exists() else 'missing'}

        live_root = self.node.root / 'device-pool'
        staging_root = self.node.root / 'device-pool-rotation'
        old = live_root / 'origin' / 'devices' / '01'
        new = staging_root / 'origin' / 'devices' / '01'
        old.mkdir(parents=True)
        new.mkdir(parents=True)
        (old / 'ready').write_text('old', 'utf-8')
        (old / 'old-only').write_text('old', 'utf-8')
        (new / 'ready').write_text('new', 'utf-8')
        (new / 'new-only').write_text('new', 'utf-8')
        (live_root / 'usage.json').write_text('{"1":20,"2":7}', 'utf-8')
        health = live_root / 'origin' / 'health.json'
        health.write_text('{"devices":{"1":{"failures":5},"2":{}},"events":[[1,1],[2,2]]}', 'utf-8')
        with patch('clusterlab.device_build._load_fastcheck', return_value=(Pool, None, None)):
            changed = self.node.builder._commit_rotation(
                staging_root, 1, 30, 'socks5h://proxy', 'MacBookPro14,3')
        self.assertTrue(changed)
        self.assertEqual((old / 'ready').read_text('utf-8'), 'new')
        self.assertTrue((old / 'new-only').exists())
        self.assertFalse((old / 'old-only').exists())
        self.assertEqual(json.loads((live_root / 'usage.json').read_text()), {'2': 7})
        saved_health = json.loads(health.read_text())
        self.assertNotIn('1', saved_health['devices'])
        self.assertEqual(saved_health['events'], [[2, 2]])
        registration = json.loads(
            (live_root / 'registrations' / '001.json').read_text('utf-8'))
        self.assertEqual(registration['model'], 'MacBookPro14,3')
        self.assertEqual(registration['slot'], 1)
        self.assertFalse(list(old.parent.glob('.rotation-backup-*')))

    def test_rotation_commit_failure_restores_the_original_device(self):
        class Pool:
            def __init__(inner, root, _origin, max_devices):
                inner.base = Path(root) / 'origin'
                inner.max_devices = max_devices

            def slot_directory(inner, slot):
                return inner.base / 'devices' / f'{slot:02d}'

            def inspect(inner, slot):
                directory = inner.slot_directory(slot)
                return {'state': 'ready' if (directory / 'ready').exists() else 'missing'}

        live_root = self.node.root / 'device-pool'
        staging_root = self.node.root / 'device-pool-rotation'
        old = live_root / 'origin' / 'devices' / '01'
        new = staging_root / 'origin' / 'devices' / '01'
        old.mkdir(parents=True)
        new.mkdir(parents=True)
        (old / 'ready').write_text('old', 'utf-8')
        (old / 'old-only').write_text('old', 'utf-8')
        (new / 'ready').write_text('new', 'utf-8')
        with patch('clusterlab.device_build._load_fastcheck', return_value=(Pool, None, None)), \
             patch.object(self.node.builder, '_record', return_value=False):
            with self.assertRaisesRegex(OSError, '注册记录'):
                self.node.builder._commit_rotation(
                    staging_root, 1, 30, 'socks5h://proxy', 'MacBookPro14,3')
        self.assertEqual((old / 'ready').read_text('utf-8'), 'old')
        self.assertTrue((old / 'old-only').exists())
        self.assertFalse(list(old.parent.glob('.rotation-backup-*')))

    def test_oldest_rotation_candidate_uses_birth_time_and_excludes_busy_slots(self):
        registrations = self.node.root / 'device-pool' / 'registrations'
        registrations.mkdir(parents=True)
        for slot, born in ((1, 30), (2, 10), (3, 20)):
            (registrations / f'{slot:03d}.json').write_text(json.dumps({
                'slot': slot, 'ts': born, 'proxy': '', 'model': '', 'at': ''}), 'utf-8')
        (self.node.root / 'device-pool' / 'usage.json').write_text(
            '{"1":20,"2":20,"3":20}', 'utf-8')
        eligible = self.node.builder.retirable(20, exclude={2})
        self.assertEqual(eligible, [1, 3])
        self.assertEqual(self.node.builder.oldest(1, candidates=eligible), [3])

    def test_remote_http_requires_explicit_test_override(self):
        with self.assertRaisesRegex(ValueError, 'HTTPS'):
            self.node.connect('http://example.com','x'*32)

    def test_remote_stop_sets_only_owned_process_events_and_is_persisted(self):
        stop = threading.Event()
        self.node.active['c'*32] = (None, stop)
        value = {'node_id':'node','name':'node','processes':1,'threads':1,'enabled':False,
                 'rejected':[],'confirmed':[],'stop_version':3}
        try:
            with patch.object(self.node, '_request', return_value=Response(value)):
                self.node._heartbeat()
            self.assertTrue(stop.is_set())
            self.assertEqual(self.node.stop_version, 3)
            self.assertEqual(json.loads((self.node.root/'control.json').read_text())['stop_version'], 3)
        finally:
            self.node.active.clear()

    def test_rejected_unit_stops_local_process_and_records_delete_reason(self):
        stop = threading.Event()
        self.node.active[self.unit] = (None, stop)
        value = {'node_id': 'node', 'name': 'node', 'processes': 1, 'threads': 1,
                 'enabled': True, 'rejected': [self.unit], 'confirmed': []}
        try:
            with patch.object(self.node, '_request', return_value=Response(value)):
                self.node._heartbeat()
            self.assertTrue(stop.is_set())
            with self.node.db.connect() as db:
                task = db.execute('SELECT status,error FROM tasks WHERE id=?',
                                  (self.unit,)).fetchone()
            self.assertEqual(task['status'], 'rejected')
            self.assertIn('总控已删除任务或撤销租约', task['error'])
        finally:
            self.node.active.clear()

    def test_clear_device_command_removes_pool_and_persists_ack(self):
        pool = self.node.root / 'device-pool'
        (pool / 'registrations').mkdir(parents=True)
        (pool / 'registrations' / '001.json').write_text('{"slot":1}')
        (pool / 'usage.json').write_text('{"1":99}')
        self.node.builder.state.update(capacity=1, ready=1)
        value = {'node_id': 'node', 'name': 'node', 'processes': 1, 'threads': 1,
                 'enabled': True, 'rejected': [], 'confirmed': [],
                 'device_request': 0, 'device_clear_version': 1}
        with patch.object(self.node, '_request', return_value=Response(value)):
            self.node._heartbeat()
        self.assertEqual(list(pool.iterdir()), [])
        self.assertEqual(self.node.builder.state['capacity'], 0)
        self.assertEqual(self.node.builder.state['ready'], 0)
        self.assertEqual(self.node.device_clear_version, 1)
        control = json.loads((self.node.root / 'control.json').read_text())
        self.assertEqual(control['device_clear_version'], 1)


if __name__ == '__main__':
    unittest.main()
