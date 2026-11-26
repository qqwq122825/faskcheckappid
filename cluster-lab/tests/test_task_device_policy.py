"""Task-policy contracts, using isolated temporary controllers and synthetic input."""
import io
from pathlib import Path
import sqlite3
import tempfile
import time
import unittest

from clusterlab.controller import Controller, SCHEMA
from clusterlab.web import create_app


class TaskDevicePolicyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.service = Controller(self.temp.name)
        self.client = create_app('controller', self.service).test_client()
        self.csrf = {'X-CSRF-Token': self.client.get('/api/session').json['csrf']}
        self.source = self.service.inputs / 'fixture.txt'
        self.source.write_bytes(b'fixture----fixture\n' * 50)

    def tearDown(self):
        self.service.close()
        self.temp.cleanup()

    def policy(self, job):
        with self.service.db.connect() as db:
            row = db.execute('SELECT public_header_use_limit,device_use_limit '
                             'FROM jobs WHERE id=?', (job,)).fetchone()
        return tuple(row)

    def job(self, **kwargs):
        job = self.service.create_job('fixture', io.BytesIO(b'fixture\n' * 50),
                                      50, **kwargs)
        self.wait_ready(job)
        return job

    def wait_ready(self, job):
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            with self.service.db.connect() as db:
                status = db.execute('SELECT status FROM jobs WHERE id=?',
                                    (job,)).fetchone()[0]
            if status != 'preparing':
                self.assertEqual(status, 'ready')
                return
            time.sleep(.01)
        self.fail('synthetic fixture preparation timed out')

    def lease(self, job):
        node = self.service.add_node('policy-fixture', 1, 1)
        self.service.heartbeat(node['id'], {'session': 'policy-fixture',
            'max_processes': 1, 'max_threads': 1, 'active': []})
        self.service.configure_node(node['id'], {'processes': 1,
            'threads': 1, 'enabled': True})
        self.service.job_action(job, 'start')
        unit = self.service.lease(node['id'], 'policy-fixture')
        self.assertIsNotNone(unit)
        return unit

    def test_stream_upload_saves_task_policy(self):
        response = self.client.post('/api/jobs/stream?name=fixture.txt&unit_rows=50'
            '&public_header_use_limit=7&device_use_limit=321',
            data=b'fixture\n' * 50, content_type='application/octet-stream',
            headers=self.csrf)
        self.assertEqual(response.status_code, 202, response.json)
        self.assertEqual(self.policy(response.json['id']), (7, 321))

    def test_file_reuse_saves_task_policy_without_copying_source(self):
        response = self.client.post('/api/jobs/from-file', json={
            'source': str(self.source), 'name': 'reuse-fixture', 'unit_rows': 50,
            'enqueue': 'false', 'public_header_use_limit': 11,
            'device_use_limit': 987}, headers=self.csrf)
        self.assertEqual(response.status_code, 202, response.json)
        self.assertEqual(self.policy(response.json['id']), (11, 987))
        with self.service.db.connect() as db:
            source = db.execute('SELECT source FROM jobs WHERE id=?',
                                (response.json['id'],)).fetchone()[0]
        self.assertEqual(source, str(self.source))

    def test_multipart_upload_saves_task_policy(self):
        response = self.client.post('/api/jobs', data={
            'file': (io.BytesIO(b'fixture\n' * 50), 'fixture.txt'),
            'unit_rows': '50', 'public_header_use_limit': '13',
            'device_use_limit': '654'}, headers=self.csrf)
        self.assertEqual(response.status_code, 202, response.json)
        self.assertEqual(self.policy(response.json['id']), (13, 654))

    def test_defaults_are_snapshotted_and_tasks_are_isolated(self):
        self.service.configure_device_policy({'public_header_use_limit': 8,
                                               'device_use_limit': 888})
        default_job = self.job()
        explicit_job = self.job(public_header_use_limit=4, device_use_limit=444)
        self.service.configure_device_policy({'public_header_use_limit': 19,
                                               'device_use_limit': 999})
        self.assertEqual(self.policy(default_job), (8, 888))
        self.assertEqual(self.policy(explicit_job), (4, 444))
        unit = self.lease(default_job)
        self.assertEqual((unit['public_header_use_limit'], unit['device_use_limit']),
                         (8, 888))
        self.service.job_action(default_job, 'stop')
        unit = self.lease(explicit_job)
        self.assertEqual((unit['public_header_use_limit'], unit['device_use_limit']),
                         (4, 444))

    def test_file_reuse_defaults_are_a_new_snapshot(self):
        self.service.configure_device_policy({'public_header_use_limit': 9,
                                               'device_use_limit': 777})
        job = self.service.create_job_from_file(str(self.source), 'reuse', 50)
        self.assertEqual(self.policy(job), (9, 777))

    def test_partial_policy_defaults_other_field_and_accepts_boundaries(self):
        first = self.job(public_header_use_limit=1)
        last = self.job(public_header_use_limit=10000, device_use_limit=1000000)
        self.assertEqual(self.policy(first), (1, 30))
        self.assertEqual(self.policy(last), (10000, 1000000))

    def test_invalid_policies_are_rejected_before_upload_is_saved(self):
        before = set(self.service.inputs.iterdir())
        for field, invalid_values in {
            'public_header_use_limit': (0, -1, 10001, True, 1.5, '5'),
            'device_use_limit': (0, -1, 1000001, False, 1.5, '5'),
        }.items():
            for value in invalid_values:
                with self.subTest(field=field, value=value):
                    with self.assertRaises(ValueError):
                        self.service.create_job('invalid', io.BytesIO(b'fixture\n'),
                                                50, **{field: value})
                    with self.assertRaises(ValueError):
                        self.service.create_job_from_file(str(self.source), 'invalid',
                                                          50, **{field: value})
        self.assertEqual(set(self.service.inputs.iterdir()), before)

    def test_http_entries_reject_invalid_policy_ranges_and_types(self):
        for key, value in [('public_header_use_limit', '0'),
                           ('public_header_use_limit', '10001'),
                           ('device_use_limit', '1000001'),
                           ('device_use_limit', 'not-a-number')]:
            with self.subTest(key=key, value=value):
                response = self.client.post(f'/api/jobs/stream?unit_rows=50&{key}={value}',
                    data=b'fixture\n' * 50, content_type='application/octet-stream',
                    headers=self.csrf)
                self.assertEqual(response.status_code, 400, response.json)
                response = self.client.post('/api/jobs', data={
                    'file': (io.BytesIO(b'fixture\n' * 50), 'fixture.txt'),
                    'unit_rows': '50', key: value}, headers=self.csrf)
                self.assertEqual(response.status_code, 400, response.json)
        for value in (True, 1.5, '5', 0, 10001):
            with self.subTest(json_value=value):
                response = self.client.post('/api/jobs/from-file', json={
                    'source': str(self.source), 'unit_rows': 50,
                    'public_header_use_limit': value}, headers=self.csrf)
                self.assertEqual(response.status_code, 400, response.json)

    def test_task_policy_survives_controller_restart(self):
        job = self.job(public_header_use_limit=6, device_use_limit=456)
        self.service.close()
        self.service = Controller(self.temp.name)
        self.assertEqual(self.policy(job), (6, 456))
        unit = self.lease(job)
        self.assertEqual((unit['public_header_use_limit'], unit['device_use_limit']),
                         (6, 456))

    def test_legacy_null_policy_falls_back_to_global_values(self):
        job = self.job()
        with self.service.db.transaction() as db:
            db.execute('UPDATE jobs SET public_header_use_limit=NULL,'
                       'device_use_limit=NULL WHERE id=?', (job,))
        self.service.configure_device_policy({'public_header_use_limit': 12,
                                               'device_use_limit': 345})
        unit = self.lease(job)
        self.assertEqual((unit['public_header_use_limit'], unit['device_use_limit']),
                         (12, 345))

    def test_legacy_database_gets_nullable_policy_columns(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'controller.sqlite3'
            legacy_schema = SCHEMA.replace(' public_header_use_limit INTEGER,\n', '')
            legacy_schema = legacy_schema.replace(' device_use_limit INTEGER,\n', '')
            with sqlite3.connect(path) as db:
                db.executescript(legacy_schema)
                db.execute('INSERT INTO jobs(id,name,status,created,updated,source,'
                           'unit_rows,mode) VALUES(?,?,?,?,?,?,?,?)',
                           ('legacy', 'legacy fixture', 'ready', 1, 1, '', 50, 'auth'))
            service = Controller(root)
            try:
                with service.db.connect() as db:
                    row = db.execute('SELECT public_header_use_limit,device_use_limit '
                                     'FROM jobs WHERE id=?', ('legacy',)).fetchone()
                self.assertEqual(tuple(row), (None, None))
            finally:
                service.close()


if __name__ == '__main__':
    unittest.main()
