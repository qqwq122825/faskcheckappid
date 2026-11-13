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


if __name__ == '__main__':
    unittest.main()
