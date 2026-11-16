"""Offline coverage for result framing, stopped uploads, and debug visibility."""
import hashlib
import io
import json
from pathlib import Path
import tempfile
import time
import unittest

from clusterlab.controller import Controller
from clusterlab.node import Node


class ResultContractTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.controller = Controller(Path(self.temp.name) / 'controller')
        self.node = Node(Path(self.temp.name) / 'node')
        self.identity = self.controller.add_node('synthetic-node', 1, 1)
        self.heartbeat()
        self.controller.configure_node(self.identity['id'],
                                       {'processes': 1, 'threads': 1, 'enabled': True})

    def tearDown(self):
        self.node.close()
        self.controller.close()
        self.temp.cleanup()

    def heartbeat(self, active=None, **extra):
        return self.controller.heartbeat(self.identity['id'], dict(
            session='synthetic-session', max_processes=1, max_threads=1,
            active=active or [], devices={'running': False, 'capacity': 30, 'ready': 30},
            **extra))

    def package(self):
        job = self.controller.create_job('synthetic-results',
                                        io.BytesIO(b'hello\n' * 50), 50, mode='digest')
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            with self.controller.db.connect() as db:
                status = db.execute('SELECT status FROM jobs WHERE id=?', (job,)).fetchone()[0]
            if status != 'preparing':
                break
            time.sleep(.01)
        self.controller.job_action(job, 'start')
        return job, self.controller.lease(self.identity['id'], 'synthetic-session')

    @staticmethod
    def record(row, reason='x' * 1200, code='SyntheticError: full diagnostic text'):
        return {'row': row, 'sha256': hashlib.sha256(b'hello').hexdigest(), 'bytes': 5,
                'status': 'undetermined', 'reason': reason, 'detail': reason, 'code': code}

    @staticmethod
    def encode(value):
        return (json.dumps(value, separators=(',', ':')) + '\n').encode()

    def test_long_result_upload_settles_stopped_node_without_rewriting_bytes(self):
        job, unit = self.package()
        payload = b''.join(self.encode(self.record(i)) for i in range(1, 51))
        directory = self.node.spool / unit['id']
        directory.mkdir()
        (directory / 'results.jsonl').write_bytes(payload)
        records, _ = self.node._collect_live_records(unit['id'])
        self.assertEqual(len(records), 50)
        self.assertEqual(len(self.controller.clean_live_records(records, unit)), 50)
        self.controller.stop_node(self.identity['id'])
        digest = hashlib.sha256(payload).hexdigest()
        self.assertTrue(self.controller.accept_result(self.identity['id'], unit['id'],
                        unit['token'], io.BytesIO(payload), digest)['accepted'])
        with self.controller.db.connect() as db:
            saved = db.execute('SELECT result_path FROM units WHERE id=?', (unit['id'],)).fetchone()[0]
        self.assertEqual(Path(saved).read_bytes(), payload)
        self.heartbeat(ack_stop_version=1)
        state = self.controller.snapshot()['nodes'][0]
        self.assertEqual((state['status'], state['active']), ('draining', 0))

    def test_oversize_records_are_rejected_by_node_and_both_controller_paths(self):
        _, unit = self.package()
        self.assertEqual(Node.RESULT_RECORD_BYTE_LIMIT, Controller.RESULT_RECORD_BYTE_LIMIT)
        record = self.record(1, reason='x' * Node.RESULT_RECORD_BYTE_LIMIT)
        line = self.encode(record)
        directory = self.node.spool / unit['id']
        directory.mkdir()
        (directory / 'results.jsonl').write_bytes(line)
        with self.assertRaises(ValueError): self.node._collect_live_records(unit['id'])
        with self.assertRaises(ValueError): self.controller.clean_live_records([line[:-1].decode()], unit)
        with self.assertRaises(ValueError):
            self.controller.accept_result(self.identity['id'], unit['id'], unit['token'],
                                          io.BytesIO(line), hashlib.sha256(line).hexdigest())
        self.assertEqual((directory / 'results.jsonl').read_bytes(), line)
        self.assertFalse((directory / 'report-state.json').exists())

    def test_byte_budget_defers_unacknowledged_lines_without_losing_them(self):
        unit = 'f' * 32
        directory = self.node.spool / unit
        directory.mkdir()
        lines = [self.encode(self.record(i, reason='x' * 24000)) for i in range(1, 51)]
        (directory / 'results.jsonl').write_bytes(b''.join(lines))
        records, cursor = self.node._collect_live_records(unit)
        self.assertLess(len(records), 50)
        self.assertLessEqual(sum(len(x.encode()) + 1 for x in records), self.node.LIVE_RESULT_BYTE_LIMIT)
        # Without an ACK the identical prefix is replayed, never skipped.
        self.assertEqual(self.node._collect_live_records(unit)[0], records)
        combined = list(records)
        while len(combined) < 50:
            (directory / 'report-state.json').write_text(json.dumps(cursor))
            tail, cursor = self.node._collect_live_records(unit)
            self.assertTrue(tail)
            combined.extend(tail)
        self.assertEqual([json.loads(x)['row'] for x in combined], list(range(1, 51)))

    def assert_debug_switch(self, job, original):
        for enabled in (False, True, False):
            self.controller.configure_debug({'proxy_debug': enabled})
            rows = self.controller.read_rows(job, limit=2)['rows']
            self.assertEqual(rows[0]['reason'], original)
            self.assertEqual(rows[0]['reason_cn'], '其他错误')
            if enabled:
                self.assertEqual(rows[0]['detail'], original)
                self.assertEqual(rows[0]['code'], 'SyntheticError: full diagnostic text')
            else:
                self.assertNotIn('detail', rows[0])
                self.assertEqual(rows[0]['code'], '')
            self.assertEqual(rows[1]['code'], '-20970')

    def test_debug_switch_controls_completed_result_response_not_stored_file(self):
        job, unit = self.package()
        original = 'SyntheticError: 原文\n' + 'UTF8 diagnostic 原因 ' * 160
        values = [self.record(i, original, '-20970' if i == 2 else 'SyntheticError: full diagnostic text')
                  for i in range(1, 51)]
        payload = b''.join(self.encode(x) for x in values)
        self.controller.accept_result(self.identity['id'], unit['id'], unit['token'],
                                      io.BytesIO(payload), hashlib.sha256(payload).hexdigest())
        self.assert_debug_switch(job, original)
        with self.controller.db.connect() as db:
            path = db.execute('SELECT result_path FROM units WHERE id=?', (unit['id'],)).fetchone()[0]
        self.assertEqual(Path(path).read_bytes(), payload)

    def test_debug_switch_also_controls_live_rows(self):
        job, unit = self.package()
        original = 'SyntheticError: 原文\n' + 'UTF8 diagnostic 原因 ' * 160
        values = [self.record(1, original), self.record(2, original, '-20970')]
        self.heartbeat([{'id': unit['id'], 'token': unit['token'], 'processed': 2,
                         'counts': {'undetermined': 2}, 'records': [json.dumps(x) for x in values]}])
        self.assert_debug_switch(job, original)
