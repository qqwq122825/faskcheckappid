import hashlib
import io
import json
from pathlib import Path
import tempfile
import time
import unittest

from clusterlab.controller import Conflict, Controller
from clusterlab.web import create_app


class ControllerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.service = Controller(self.temp.name)
        self.node = self.service.add_node('test-node', 2, 4)
        self.heartbeat()

    def tearDown(self):
        self.service.close()
        self.temp.cleanup()

    def heartbeat(self, **extra):
        return self.service.heartbeat(self.node['id'], dict(session='test-session', max_processes=1,
                                      max_threads=2, active=[], **extra))

    def job(self, count=101):
        job = self.service.create_job('test', io.BytesIO(b'hello\n' * count), 50)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            state = next(j for j in self.service.snapshot()['jobs'] if j['id'] == job)
            if state['status'] != 'preparing':
                self.assertEqual(state['status'], 'ready')
                return job
            time.sleep(.01)
        self.fail('preparation did not finish')

    def output(self, unit):
        value = b''.join((json.dumps({'row': n, 'sha256': hashlib.sha256(b'hello').hexdigest(), 'bytes': 5},
                                    separators=(',', ':')) + '\n').encode()
                         for n in range(unit['first_row'], unit['first_row'] + unit['rows']))
        return value, hashlib.sha256(value).hexdigest()

    def test_lease_requires_started_job_and_respects_node_capacity(self):
        job = self.job()
        self.assertIsNone(self.service.lease(self.node['id'], 'test-session'))
        self.service.job_action(job, 'start')
        unit = self.service.lease(self.node['id'], 'test-session')
        self.assertEqual(unit['threads'], 2)  # Local maximum wins over desired 4.
        self.assertIsNone(self.service.lease(self.node['id'], 'test-session'))
        self.service.job_action(job, 'pause')
        payload, checksum = self.output(unit)
        self.service.accept_result(self.node['id'], unit['id'], unit['token'], io.BytesIO(payload), checksum)
        self.assertIsNone(self.service.lease(self.node['id'], 'test-session'))

    def test_large_node_capacity_is_accepted(self):
        """P0：单节点可以要 100 进程 / 100 线程，不再被 16/32 上限卡住。"""
        node = self.service.add_node('big', 100, 100)
        self.service.heartbeat(node['id'], dict(session='big-session', max_processes=200,
                                                max_threads=512, active=[], interrupted=[]))
        job = self.job()
        self.service.job_action(job, 'start')
        unit = self.service.lease(node['id'], 'big-session')
        self.assertIsNotNone(unit)
        self.assertEqual(unit['threads'], 100)

    def test_late_result_after_lease_expiry_is_still_accepted(self):
        """P0：租约过期后迟到的结果仍要接受，否则节点白算一遍。"""
        self.service.lease_seconds = 1
        job = self.job()
        self.service.job_action(job, 'start')
        unit = self.service.lease(self.node['id'], 'test-session')
        time.sleep(1.6)                      # 让租约过期，包被标成 unknown
        self.service.job_action(job, 'pause')
        payload, checksum = self.output(unit)
        result = self.service.accept_result(self.node['id'], unit['id'], unit['token'],
                                            io.BytesIO(payload), checksum)
        self.assertTrue(result['accepted'])

    def test_global_dispatch_gate_limits_new_leases(self):
        """P0：全局闸门限制每秒新派发的包数。"""
        self.service.dispatch_per_second = 2
        job = self.job(count=200)
        self.service.job_action(job, 'start')
        got, started = 0, time.monotonic()
        while time.monotonic() - started < 1.0:
            if self.service.lease(self.node['id'], 'test-session'):
                got += 1
            else:
                time.sleep(.02)
        self.assertLessEqual(got, 4)          # 2/秒，1 秒窗口内最多 4 个（含边界）

    def test_result_replay_is_idempotent_and_lost_ack_is_reconciled(self):
        job = self.job(50)
        self.service.job_action(job, 'start')
        unit = self.service.lease(self.node['id'], 'test-session')
        payload, checksum = self.output(unit)
        first = self.service.accept_result(self.node['id'], unit['id'], unit['token'], io.BytesIO(payload), checksum)
        replay = self.service.accept_result(self.node['id'], unit['id'], unit['token'], io.BytesIO(payload), checksum)
        self.assertFalse(first['duplicate'])
        self.assertTrue(replay['duplicate'])
        hb = self.service.heartbeat(self.node['id'], {'session': 'test-session', 'max_processes': 1, 'max_threads': 2,
                                    'active': [{'id': unit['id'], 'token': unit['token'], 'processed': 50}]})
        self.assertEqual(hb['confirmed'], [unit['id']])
        self.assertEqual(self.service.snapshot()['jobs'][0]['confirmed'], 50)
        self.assertEqual(self.service.snapshot()['jobs'][0]['status'], 'completed')

    def test_task_row_statistics_count_partial_final_unit(self):
        job = self.job(101)
        snapshot = self.service.snapshot()['jobs'][0]
        self.assertEqual(snapshot['row_counts'], {'pending': 101})
        self.service.job_action(job, 'start')
        unit = self.service.lease(self.node['id'], 'test-session')
        snapshot = self.service.snapshot()['jobs'][0]
        self.assertEqual(snapshot['row_counts'], {'pending': 51, 'leased': 50})
        payload, checksum = self.output(unit)
        self.service.accept_result(self.node['id'], unit['id'], unit['token'], io.BytesIO(payload), checksum)
        snapshot = self.service.snapshot()['jobs'][0]
        self.assertEqual(snapshot['row_counts'], {'pending': 51, 'completed': 50})
        self.assertEqual(sum(snapshot['row_counts'].values()), snapshot['rows'])

    def test_vue_page_and_hashed_assets_are_served_locally(self):
        client = create_app('controller', self.service).test_client()
        response = client.get('/')
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'<div id="app"></div>', response.data)
        import re
        assets = re.findall(rb'(?:src|href)="(/static/ui/assets/[^\"]+)"', response.data)
        self.assertGreaterEqual(len(assets), 2)
        for asset in assets:
            with client.get(asset.decode()) as result:
                self.assertEqual(result.status_code, 200)
        response.close()

    def test_expired_lease_is_unknown_and_old_result_cannot_overwrite_new_lease(self):
        job = self.job(50)
        self.service.job_action(job, 'start')
        first = self.service.lease(self.node['id'], 'test-session')
        with self.service.db.transaction() as db:
            db.execute('UPDATE units SET lease_until=? WHERE id=?', (time.time() - 1, first['id']))
        self.assertEqual(self.service.snapshot()['jobs'][0]['unit_counts']['unknown'], 1)
        self.assertIsNone(self.service.lease(self.node['id'], 'test-session'))
        self.service.job_action(job, 'retry_unknown')
        second = self.service.lease(self.node['id'], 'test-session')
        self.assertEqual(first['id'], second['id'])
        self.assertNotEqual(first['token'], second['token'])
        payload, checksum = self.output(first)
        with self.assertRaises(Conflict):
            self.service.accept_result(self.node['id'], first['id'], first['token'], io.BytesIO(payload), checksum)

    def test_wrong_output_count_is_not_confirmed(self):
        job = self.job(50)
        self.service.job_action(job, 'start')
        unit = self.service.lease(self.node['id'], 'test-session')
        payload, _ = self.output(unit)
        partial = b'\n'.join(payload.split(b'\n')[:2]) + b'\n'
        with self.assertRaises(ValueError):
            self.service.accept_result(self.node['id'], unit['id'], unit['token'], io.BytesIO(partial), hashlib.sha256(partial).hexdigest())
        self.assertEqual(self.service.snapshot()['jobs'][0]['confirmed'], 0)

    def test_controller_restart_preserves_tasks_and_tokens(self):
        job = self.job(50)
        self.service.job_action(job, 'start')
        unit = self.service.lease(self.node['id'], 'test-session')
        self.service.close()
        self.service = Controller(self.temp.name)
        self.assertEqual(self.service.authenticate(self.node['token']), self.node['id'])
        payload, checksum = self.output(unit)
        self.service.accept_result(self.node['id'], unit['id'], unit['token'], io.BytesIO(payload), checksum)
        self.assertEqual(self.service.snapshot()['jobs'][0]['status'], 'completed')

    def test_long_record_fails_preparation_and_never_enters_queue(self):
        job = self.service.create_job('bad', io.BytesIO(b'x' * 65537), 50)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            state = next(j for j in self.service.snapshot()['jobs'] if j['id'] == job)
            if state['status'] == 'failed':
                break
            time.sleep(.01)
        self.assertEqual(state['status'], 'failed')
        self.assertIsNone(self.service.lease(self.node['id'], 'test-session'))

    def test_agent_auth_and_browser_csrf_are_separate(self):
        client = create_app('controller', self.service).test_client()
        self.assertEqual(client.post('/agent/lease', json={'session':'test-session'}).status_code, 401)
        self.assertEqual(client.post('/api/demo', json={'rows':100}).status_code, 403)
        response = client.get('/api/state')
        self.assertNotIn('token_hash', response.get_data(as_text=True))
        self.assertNotIn(self.node['token'], response.get_data(as_text=True))

    def test_work_packages_have_a_byte_budget_as_well_as_a_row_budget(self):
        job = self.service.create_job('bytes', io.BytesIO((b'x' * 60000 + b'\n') * 80), 100000)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            state = next(j for j in self.service.snapshot()['jobs'] if j['id'] == job)
            if state['status'] != 'preparing':
                break
            time.sleep(.01)
        self.assertEqual(state['status'], 'ready')
        self.assertEqual(state['rows'], 80)
        self.assertEqual(state['units'], 2)
        with self.service.db.connect() as db:
            paths = [Path(r['path']) for r in db.execute('SELECT path FROM units WHERE job_id=?', (job,))]
        self.assertTrue(all(p.stat().st_size <= 4 * 1024 * 1024 for p in paths))

    def test_another_node_cannot_download_a_leased_package(self):
        job = self.job(50)
        self.service.job_action(job, 'start')
        unit = self.service.lease(self.node['id'], 'test-session')
        other = self.service.add_node('another-node')
        client = create_app('controller', self.service).test_client()
        response = client.get(f'/agent/units/{unit["id"]}/input',
                              headers={'Authorization':'Bearer '+other['token'],'X-Lease-Token':unit['token']})
        self.assertEqual(response.status_code, 409)
        response = client.get(f'/agent/units/{unit["id"]}/input',
                              headers={'Authorization':'Bearer '+self.node['token'],'X-Lease-Token':unit['token']})
        self.assertEqual(response.status_code, 200)
        response.close()

    def test_stop_waits_for_node_ack_and_retains_unknown_work(self):
        job = self.job(50)
        self.service.job_action(job, 'start')
        unit = self.service.lease(self.node['id'], 'test-session')
        self.service.stop_node(self.node['id'])
        self.assertEqual(self.service.snapshot()['nodes'][0]['status'], 'stopping')
        with self.assertRaises(Conflict):
            self.service.configure_node(self.node['id'], {'processes':1,'threads':2,'enabled':True})
        response = self.service.heartbeat(self.node['id'], {
            'session':'test-session','max_processes':1,'max_threads':2,'ack_stop_version':0,
            'active':[{'id':unit['id'],'token':unit['token'],'processed':10}]})
        self.assertEqual(response['stop_version'], 1)
        self.assertFalse(response['enabled'])
        self.assertEqual(self.service.unit_for_node(self.node['id'], unit['id'], unit['token'])['status'], 'running')
        response = self.service.heartbeat(self.node['id'], {
            'session':'test-session','max_processes':1,'max_threads':2,'ack_stop_version':1,'active':[],
            'interrupted':[{'id':unit['id'],'token':unit['token'],'processed':10}]})
        self.assertEqual(response['interrupted_ack'], [unit['id']])
        self.assertEqual(self.service.snapshot()['nodes'][0]['status'], 'draining')
        self.assertEqual(self.service.snapshot()['jobs'][0]['unit_counts']['unknown'], 1)
        self.assertIsNone(self.service.lease(self.node['id'], 'test-session'))
        payload, checksum = self.output(unit)
        with self.assertRaises(Conflict):
            self.service.accept_result(self.node['id'], unit['id'], unit['token'], io.BytesIO(payload), checksum)

    def test_host_metrics_are_finite_and_do_not_accept_arbitrary_fields(self):
        value = self.service.clean_telemetry({'cpu_percent':25.2,'memory_percent':float('nan'),
                                             'upload_bps':1024,'download_bps':-1,'worker_processes':2,'secret':'private'})
        self.assertEqual(value['cpu_percent'], 25.2)
        self.assertIsNone(value['memory_percent'])
        self.assertIsNone(value['download_bps'])
        self.assertEqual(value['worker_processes'], 2)
        self.assertNotIn('secret', value)


if __name__ == '__main__':
    unittest.main()
