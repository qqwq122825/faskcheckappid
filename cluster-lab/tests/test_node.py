import hashlib
import io
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from clusterlab.node import Node


class Response:
    def __init__(self, value):
        self.value = value

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def json(self):
        return self.value


class NodeRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.node = Node(self.temp.name)
        self.unit = 'a' * 32
        directory = self.node.spool / self.unit
        directory.mkdir()
        (directory / 'results.jsonl').write_text('{"row":1}\n')
        with self.node.db.transaction() as db:
            db.execute('INSERT INTO tasks(id,job_id,seq,descriptor,status,processed,checksum,created,updated) VALUES(?,?,?,?,?,?,?,?,?)',
                       (self.unit, 'b'*32, 1, json.dumps({'token':'test-lease','rows':1,'threads':1}), 'outbox', 1, 'digest', 1, 1))

    def tearDown(self):
        self.node.close()
        self.temp.cleanup()

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

    def test_pending_results_block_controller_switch(self):
        with self.assertRaisesRegex(ValueError, '结果待上传'):
            self.node.connect('http://127.0.0.1:18772','x'*32)

    def test_auth_task_starts_background_pool_build_and_rotates_slots(self):
        descriptor = {'mode': 'auth', 'rows': 10000, 'seq': 2,
                      'processes': 10, 'device_capacity': 30}
        with patch.object(self.node.builder, 'start') as start:
            self.node._prepare_auth_devices(descriptor)
        self.assertEqual(descriptor['device_pool_capacity'], 30)
        self.assertEqual(descriptor['device_slots'], list(range(11, 21)))
        start.assert_called_once_with(30)
        self.assertTrue(set(range(11, 21)).issubset(self.node.builder.reserved))
        self.node.builder.release(descriptor['device_slots'])

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
        with patch('clusterlab.device_build._load_fastcheck', return_value=(Pool, None, None)):
            snapshot = self.node.builder.snapshot()
        self.assertEqual(snapshot['attempted'], 1)
        self.assertEqual(snapshot['ready'], 1)
        self.assertEqual(snapshot['slots'][0]['model'], 'MacBookPro14,3')
        self.assertEqual(snapshot['slots'][0]['proxy'], '历史未记录')
        self.assertRegex(snapshot['slots'][0]['at'], r'^\d{4}-\d{2}-\d{2} ')
        self.assertFalse(snapshot['slots'][0]['recorded'])

    def test_completed_device_target_does_not_restart_each_heartbeat(self):
        value = {'node_id': 'node', 'name': 'node', 'processes': 1, 'threads': 1,
                 'enabled': True, 'rejected': [], 'confirmed': [], 'device_request': 1}
        complete = {'running': False, 'capacity': 1, 'ready': 1, 'built': 0,
                    'failed': 0, 'current': 0, 'attempted': 1, 'slots': []}
        with patch.object(self.node, '_request', return_value=Response(value)), \
             patch.object(self.node.builder, 'snapshot', return_value=complete), \
             patch.object(self.node.builder, 'start') as start:
            self.node._heartbeat()
        start.assert_not_called()

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


if __name__ == '__main__':
    unittest.main()
