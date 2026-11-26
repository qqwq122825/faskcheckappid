import hashlib
import io
import json
import multiprocessing as mp
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from clusterlab.node import Node
from clusterlab.device_build import REGISTRATION_TIMEOUT, _register


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

    def test_proxy_check_fetches_one_proxy_and_tests_exit_then_google(self):
        class ProbeResponse:
            def __init__(self, status, payload=None):
                self.status_code, self.payload = status, payload or {}

            def raise_for_status(self):
                if self.status_code >= 400:
                    raise RuntimeError(f'HTTP {self.status_code}')

            def json(self):
                return self.payload

        class ProbeSession:
            requested = []

            def __init__(self):
                self.proxies, self.trust_env = {}, True

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def get(self, url, **kwargs):
                self.requested.append(url)
                return (ProbeResponse(200, {'ip': '203.0.113.9'}) if 'ipify' in url
                        else ProbeResponse(204))

        with tempfile.TemporaryDirectory() as root:
            node = Node(root)
            try:
                node.proxy_api, node.proxy_scheme = 'https://supplier.test/api', 'http'
                with patch('fastcheck.proxy_pool.fetch_proxies',
                           return_value=['http://user:secret@10.0.0.2:8080']) as fetch, \
                     patch('clusterlab.node.requests.Session', ProbeSession):
                    node._run_proxy_check(7)
                check = node._proxy_check_snapshot()
                fetch.assert_called_once_with('https://supplier.test/api', 1, scheme=None)
                self.assertEqual(ProbeSession.requested,
                                 ['https://api.ipify.org?format=json',
                                  'https://www.google.com/generate_204'])
                self.assertEqual(check['status'], 'ok')
                self.assertEqual(check['exit_ip'], '203.0.113.9')
                self.assertEqual(check['google_status'], 204)
                self.assertNotIn('secret', check['proxy'])
            finally:
                node.close()

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

    def test_registration_uses_existing_worker_instead_of_spawning_python(self):
        payload = {'slot': 7}
        expected = {'ok': True, 'model': 'iMac14,1'}
        with patch('clusterlab.device_build._registration_result', return_value=expected) as run:
            result = self.node.builder._provision(payload)
        self.assertEqual(result, expected)
        run.assert_called_once_with(payload)

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

    def test_legacy_ten_by_one_hundred_service_becomes_two_by_two_fifty(self):
        with tempfile.TemporaryDirectory() as root:
            legacy = Node(root, max_processes=10, max_threads=100)
            try:
                self.assertEqual((legacy.max_processes, legacy.max_threads), (2, 250))
            finally:
                legacy.close()

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

    def test_heartbeat_includes_runtime_backend(self):
        captured = {}
        value = {'node_id': 'node', 'name': 'node', 'processes': 1, 'threads': 1,
                 'enabled': True, 'rejected': [], 'confirmed': []}

        def request(*_args, **kwargs):
            captured.update(kwargs['json'])
            return Response(value)

        with patch.object(self.node, '_request', side_effect=request):
            self.node._heartbeat()
        telemetry = captured['telemetry']
        self.assertRegex(telemetry['python_version'], r'^\d+\.\d+\.\d+$')
        self.assertTrue(telemetry['gmpy2_version'])
        self.assertIn(telemetry['srp_math_backend'], ('gmpy2', 'python', 'unavailable'))

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

    def test_auth_task_assigns_and_reserves_one_slot_per_worker_without_prefill(self):
        descriptor = {'mode': 'auth', 'rows': 1000, 'seq': 2,
                      'processes': 2, 'threads': 200, 'device_capacity': 30}
        with patch.object(self.node.builder, 'snapshot',
                          side_effect=AssertionError('must not inspect device pool')):
            self.node._prepare_auth_devices(descriptor)
        self.assertEqual(descriptor['device_pool_capacity'], 400)
        self.assertEqual(descriptor['device_slots'], list(range(1, 401)))
        self.assertEqual(descriptor['processes'], 2)
        self.assertNotIn('device_capacity', descriptor)
        self.assertEqual(self.node.builder.reserved, set(range(1, 401)))

    def test_auth_task_does_not_require_ready_devices(self):
        descriptor = {'mode': 'auth', 'rows': 2, 'processes': 4, 'threads': 10}
        with patch.object(self.node.builder, 'snapshot',
                          side_effect=AssertionError('must not inspect device pool')):
            self.node._prepare_auth_devices(descriptor)
        self.assertEqual(descriptor['processes'], 2)
        self.assertEqual(descriptor['device_slots'], [1, 2])
        self.assertEqual(self.node.builder.reserved, {1, 2})

    def test_device_status_summarizes_total_in_use_and_registering(self):
        self.node.builder.reserved = {1, 2, 3}
        snapshot = {'running': False, 'capacity': 4, 'ready': 2, 'built': 2,
                    'failed': 0, 'current': 0, 'attempted': 3,
                    'proxy_status': 'idle', 'proxy_ip': '', 'message': '',
                    'slots': [{'slot': 1, 'state': 'ready'},
                              {'slot': 2, 'state': 'missing'},
                              {'slot': 3, 'state': 'registering'},
                              {'slot': 4, 'state': 'ready'}]}
        with patch.object(self.node.builder, 'snapshot', return_value=snapshot):
            status = self.node._device_status()
        self.assertEqual(status['ready'], 2)
        self.assertEqual(status['in_use'], 3)
        self.assertEqual(status['registering'], 2)

    def test_background_builder_never_registers_without_a_proxy(self):
        class Pool:
            def __init__(inner, *_args, **_kwargs):
                pass

            def inspect(inner, _slot):
                return {'state': 'missing'}

        with patch('clusterlab.device_build._load_fastcheck', return_value=(Pool, None, None)), \
             patch.object(self.node.builder, '_proxy', return_value=None):
            self.node.builder._run(1)
        self.assertEqual(self.node.builder.state['ready'], 0)
        self.assertEqual(self.node.builder.state['failed'], 1)
        self.assertIn('禁止直连注册', self.node.builder.state['message'])
        self.assertEqual(self.node.builder.state['proxy_status'], 'failed')
        self.assertEqual(self.node.builder.state['proxy_ip'], '')
        self.assertEqual(self.node.builder.state['slot_errors']['1'], '代理获取失败')
        self.assertFalse((self.node.root / 'device-pool' / 'registrations').exists())

    def test_background_builder_registers_missing_slots_concurrently(self):
        class Pool:
            ready = set()
            lock = threading.Lock()

            def __init__(inner, *_args, **_kwargs):
                pass

            def inspect(inner, slot):
                with inner.lock:
                    state = 'ready' if slot in inner.ready else 'missing'
                return {'state': state}

        entered = 0
        maximum = 0
        guard = threading.Lock()
        two_workers = threading.Event()

        def provision(payload):
            nonlocal entered, maximum
            with guard:
                entered += 1
                maximum = max(maximum, entered)
                if entered >= 2:
                    two_workers.set()
            self.assertTrue(two_workers.wait(2), 'registration remained serial')
            with Pool.lock:
                Pool.ready.add(payload['slot'])
            with guard:
                entered -= 1
            return {'ok': True, 'model': 'MacBookPro14,3'}

        with patch('clusterlab.device_build._load_fastcheck',
                   return_value=(Pool, None, None)), \
             patch.object(self.node.builder, '_proxy', return_value='socks5h://proxy'), \
             patch.object(self.node.builder, '_provision', side_effect=provision), \
             patch.object(self.node.builder, '_record', return_value=True):
            self.node.builder._run(4)
        self.assertGreaterEqual(maximum, 2)
        self.assertEqual(self.node.builder.state['built'], 4)
        self.assertEqual(self.node.builder.state['ready'], 4)
        self.assertEqual(self.node.builder.state['failed'], 0)
        self.assertEqual(self.node.builder.state['active_slots'], [])

    def test_snapshot_distinguishes_registering_failed_and_pending_slots(self):
        class Pool:
            def __init__(inner, *_args, **_kwargs):
                pass

            def inspect(inner, slot):
                return {'state': 'pending', 'device_id': f'device-{slot}'}

        self.node.builder.state.update(running=True, capacity=3, current=2,
                                       slot_errors={'1': 'AnisetteError: HTTP 503'})
        with patch('clusterlab.device_build._load_fastcheck', return_value=(Pool, None, None)):
            snapshot = self.node.builder.snapshot()
        self.assertEqual([row['state'] for row in snapshot['slots']],
                         ['failed', 'registering', 'pending'])
        self.assertEqual(snapshot['slots'][0]['error'], 'AnisetteError: HTTP 503')

    def test_empty_snapshot_has_no_synthetic_missing_slot(self):
        class Pool:
            def __init__(inner, *_args, **_kwargs):
                inner.base = self.node.root / 'device-pool' / 'pool'

            def inspect(inner, slot):
                return {'state': 'missing', 'device_id': ''}

        self.node.builder.state.update(running=False, capacity=0)
        with patch('clusterlab.device_build._load_fastcheck', return_value=(Pool, None, None)):
            snapshot = self.node.builder.snapshot()
        self.assertEqual(snapshot['ready'], 0)
        self.assertEqual(snapshot['capacity'], 0)
        self.assertEqual(snapshot['slots'], [])

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

    def test_manual_device_request_forwards_exit_ip_switch(self):
        value = {'node_id': 'node', 'name': 'node', 'processes': 1, 'threads': 1,
                 'enabled': True, 'rejected': [], 'confirmed': [],
                 'device_request': 2, 'device_verify_exit_ip': True}
        incomplete = {'running': False, 'capacity': 0, 'ready': 0, 'built': 0,
                      'failed': 0, 'current': 0, 'attempted': 0, 'slots': []}
        with patch.object(self.node, '_request', return_value=Response(value)), \
             patch.object(self.node.builder, 'snapshot', return_value=incomplete), \
             patch.object(self.node.builder, 'start') as start:
            self.node._heartbeat()
        start.assert_called_once_with(2, verify_exit_ip=True)

    def test_manual_registration_skips_exit_ip_probe_by_default(self):
        with patch('clusterlab.device_build._probe_proxy_exit_ip') as probe:
            self.assertEqual(
                self.node.builder._registration_origin('socks5h://proxy'), '未回查')
        probe.assert_not_called()

    def test_manual_registration_can_explicitly_probe_exit_ip(self):
        with patch('clusterlab.device_build._probe_proxy_exit_ip',
                   return_value='203.0.113.8') as probe:
            self.assertEqual(self.node.builder._registration_origin(
                'socks5h://proxy', True), '203.0.113.8')
        probe.assert_called_once_with('socks5h://proxy')

    def test_auto_rotation_registers_one_replacement_before_removing_any_device(self):
        value = {'node_id': 'node', 'name': 'node', 'processes': 1, 'threads': 1,
                 'enabled': True, 'rejected': [], 'confirmed': [], 'device_request': 0,
                 'device_auto_rotate': True, 'device_rotate_after': 20}
        complete = {'running': False, 'capacity': 30, 'ready': 30, 'built': 0,
                    'failed': 0, 'current': 0, 'attempted': 30, 'slots': []}
        with patch.object(self.node, '_request', return_value=Response(value)), \
             patch.object(self.node.builder, 'snapshot', return_value=complete), \
             patch.object(self.node.builder, 'reserved_slots', return_value=set()), \
             patch.object(self.node.builder, 'retirable', return_value=[7]) as retirable, \
             patch.object(self.node.builder, 'oldest', return_value=[1]) as oldest, \
             patch.object(self.node.builder, 'rotate') as rotate, \
             patch.object(self.node.builder, 'retire') as retire, \
             patch.object(self.node.builder, 'start') as start:
            self.node._heartbeat()
        retirable.assert_not_called()
        oldest.assert_not_called()
        rotate.assert_not_called()
        retire.assert_not_called()
        start.assert_not_called()

    def test_auto_rotation_keeps_all_reserved_devices_untouched(self):
        value = {'node_id': 'node', 'name': 'node', 'processes': 1, 'threads': 1,
                 'enabled': True, 'rejected': [], 'confirmed': [], 'device_request': 0,
                 'device_auto_rotate': True, 'device_rotate_after': 20}
        complete = {'running': False, 'capacity': 30, 'ready': 30, 'built': 0,
                    'failed': 0, 'current': 0, 'attempted': 30, 'slots': []}
        reserved = set(range(1, 31))
        with patch.object(self.node, '_request', return_value=Response(value)), \
             patch.object(self.node.builder, 'snapshot', return_value=complete), \
             patch.object(self.node.builder, 'reserved_slots', return_value=reserved), \
             patch.object(self.node.builder, 'retirable', return_value=[]) as retirable, \
             patch.object(self.node.builder, 'rotate') as rotate, \
             patch.object(self.node.builder, 'start') as start:
            self.node._heartbeat()
        retirable.assert_not_called()
        rotate.assert_not_called()
        start.assert_not_called()

    def test_auto_rotation_fills_an_existing_gap_before_starting_a_replacement(self):
        value = {'node_id': 'node', 'name': 'node', 'processes': 1, 'threads': 1,
                 'enabled': True, 'rejected': [], 'confirmed': [], 'device_request': 0,
                 'device_auto_rotate': True, 'device_rotate_after': 20}
        incomplete = {'running': False, 'capacity': 30, 'ready': 29, 'built': 0,
                      'failed': 0, 'current': 0, 'attempted': 30, 'slots': []}
        with patch.object(self.node, '_request', return_value=Response(value)), \
             patch.object(self.node.builder, 'snapshot', return_value=incomplete), \
             patch.object(self.node.builder, 'retirable') as retirable, \
             patch.object(self.node.builder, 'rotate') as rotate, \
             patch.object(self.node.builder, 'start') as start:
            self.node._heartbeat()
        start.assert_not_called()
        retirable.assert_not_called()
        rotate.assert_not_called()

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
