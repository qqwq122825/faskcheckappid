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

    def queued_job(self, count=10000, unit_rows=1000):
        job = self.service.create_job('queued test', io.BytesIO(b'hello\n' * count), unit_rows, enqueue=True)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            state = next(j for j in self.service.snapshot()['jobs'] if j['id'] == job)
            if state['status'] != 'preparing':
                return job
            time.sleep(.01)
        self.fail('queued preparation did not finish')

    def confirm(self, unit):
        payload, checksum = self.output(unit)
        return self.service.accept_result(self.node['id'], unit['id'], unit['token'], io.BytesIO(payload), checksum)

    def test_fifo_starts_second_task_only_after_final_ack_of_first(self):
        first, second = self.queued_job(10001), self.queued_job(10000)
        snapshot = self.service.snapshot()
        self.assertEqual(snapshot['current_job_id'], first)
        self.assertEqual(snapshot['queue'][0]['id'], second)
        self.assertEqual(snapshot['totals']['queued'], 1)
        units = []
        for _ in range(11):
            unit = self.service.lease(self.node['id'], 'test-session')
            self.assertEqual(unit['job_id'], first)
            units.append(unit)
            self.assertIsNone(self.service.lease(self.node['id'], 'test-session'))
            self.assertEqual(self.service.snapshot()['current_job_id'], first)
            self.confirm(unit)
        self.assertEqual(sum(unit['rows'] for unit in units), 10001)
        self.assertEqual(self.service.snapshot()['current_job_id'], second)
        self.assertTrue(self.confirm(units[-1])['duplicate'])
        unit = self.service.lease(self.node['id'], 'test-session')
        self.assertEqual(unit['job_id'], second)

    def test_pause_releases_slot_and_rejoins_queue_on_resume(self):
        """暂停 = 让出执行位：下一个任务立刻开始，恢复时按原排队时间回到队列。"""
        first, second = self.queued_job(100, 50), self.queued_job(50, 50)
        unit = self.service.lease(self.node['id'], 'test-session')
        self.service.job_action(first, 'pause')
        # 在途工作包仍可确认，确认后队列立刻推进到第二个任务
        self.confirm(unit)
        self.assertEqual(self.service.snapshot()['current_job_id'], second)
        # 暂停的任务不占队列位，只以 paused 状态留在任务列表里
        self.assertEqual(self.service.snapshot()['queue'], [])
        paused = next(j for j in self.service.snapshot()['jobs'] if j['id'] == first)
        self.assertEqual(paused['status'], 'paused')
        unit = self.service.lease(self.node['id'], 'test-session')
        self.assertEqual(unit['job_id'], second)
        # 总控重启后状态保持
        self.service.close()
        self.service = Controller(self.temp.name)
        self.assertEqual(self.service.snapshot()['current_job_id'], second)
        self.assertEqual(self.service.snapshot()['queue'], [])
        # 恢复时回到队列，不会抢占正在跑的任务
        self.service.job_action(first, 'start')
        self.assertEqual([j['id'] for j in self.service.snapshot()['queue']], [first])

    def test_preparing_head_cannot_be_overtaken_and_can_leave_queue(self):
        from unittest.mock import patch
        with patch.object(self.service.preparer, 'submit'):
            first = self.service.create_job('first', io.BytesIO(b'hello\n' * 50), 50, enqueue=True)
            second = self.service.create_job('second', io.BytesIO(b'hello\n' * 50), 50, enqueue=True)
        self.service.prepare(second)
        self.assertIsNone(self.service.snapshot()['current_job_id'])
        self.assertEqual([j['id'] for j in self.service.snapshot()['queue']], [first, second])
        self.service.job_action(first, 'dequeue')
        self.assertEqual(self.service.snapshot()['current_job_id'], second)
        self.service.prepare(first)
        self.assertEqual(next(j['status'] for j in self.service.snapshot()['jobs'] if j['id'] == first), 'ready')
        self.service.job_action(first, 'start')
        self.assertEqual(self.service.snapshot()['queue'][0]['id'], first)

    def test_failed_preparation_advances_queue_and_simultaneous_starts_serialize(self):
        from unittest.mock import patch
        from concurrent.futures import ThreadPoolExecutor
        with patch.object(self.service.preparer, 'submit'):
            failed = self.service.create_job('bad', io.BytesIO(b'\xff\n'), 50, enqueue=True)
            queued = self.service.create_job('next', io.BytesIO(b'hello\n' * 50), 50, enqueue=True)
        self.service.prepare(queued)
        self.service.prepare(failed)
        self.assertEqual(self.service.snapshot()['current_job_id'], queued)
        self.confirm(self.service.lease(self.node['id'], 'test-session'))
        jobs = [self.job(50) for _ in range(3)]
        with ThreadPoolExecutor(max_workers=3) as pool:
            list(pool.map(lambda job: self.service.job_action(job, 'start'), jobs))
        snapshot = self.service.snapshot()
        self.assertEqual(sum(j['status'] == 'running' for j in snapshot['jobs']), 1)
        self.assertEqual(snapshot['totals']['queued'], 2)

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
        # 显式压低字节预算，验证"条数没到上限也会按字节切包"的行为本身
        self.service.settings = lambda: {'max_unit_rows': 100000, 'default_unit_rows': 10000,
                                        'unit_byte_limit': 4 * 1024 * 1024}
        job = self.service.create_job('bytes', io.BytesIO((b'x' * 60000 + b'\n') * 80), 10000)
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

    def test_prepare_does_not_query_settings_per_row(self):
        """解析阶段每行查一次数据库会把 10 万行拖到 30 秒以上（回归防护）。"""
        calls = []
        original = self.service.settings
        self.service.settings = lambda: (calls.append(1), original())[1]
        self.job(count=400)
        self.assertLess(len(calls), 6, f'解析期间调用了 {len(calls)} 次 settings()')

    def test_shard_limit_is_persistent_and_only_changes_new_jobs(self):
        self.assertEqual(self.service.settings()['max_unit_rows'], 100000)
        for value in (49, 100001, True, 1000.5):
            with self.assertRaises(ValueError):
                self.service.configure_settings({'max_unit_rows': value})
        with self.assertRaises(ValueError):
            self.service.create_job('over', io.BytesIO(b'hello\n'), 100001)
        job = self.job(101)
        self.service.configure_settings({'max_unit_rows': 50})
        with self.assertRaises(ValueError):
            self.service.create_job('over configured limit', io.BytesIO(b'hello\n'), 51)
        self.assertEqual(self.service.snapshot()['jobs'][0]['unit_rows'], 50)
        self.service.close()
        self.service = Controller(self.temp.name)
        self.assertEqual(self.service.settings()['max_unit_rows'], 50)
        self.assertEqual(self.service.snapshot()['jobs'][0]['id'], job)
        client = create_app('controller', self.service).test_client()
        self.assertEqual(client.post('/api/settings', json={'max_unit_rows': 10000}).status_code, 403)
        csrf = client.get('/api/session').json['csrf']
        response = client.post('/api/settings', json={'max_unit_rows': 10000}, headers={'X-CSRF-Token': csrf})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json['max_unit_rows'], 10000)

    def test_ten_shards_export_original_txt_without_duplicates_or_missing_rows(self):
        source = ('\ufeff' + '\r\n'.join(f'离线记录-{i:05d}' for i in range(1, 10001))).encode('utf-8')
        job = self.service.create_job('TXT export', io.BytesIO(source), 1000)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            state = next(j for j in self.service.snapshot()['jobs'] if j['id'] == job)
            if state['status'] != 'preparing':
                break
            time.sleep(.01)
        self.assertEqual((state['rows'], state['units']), (10000, 10))
        client = create_app('controller', self.service).test_client()
        self.assertEqual(client.get(f'/api/jobs/{job}/results').status_code, 409)
        self.service.configure_settings({'max_unit_rows': 500})
        self.service.job_action(job, 'start')
        observed = []
        while unit := self.service.lease(self.node['id'], 'test-session'):
            observed.append((unit['first_row'], unit['rows']))
            path = self.service.unit_for_node(self.node['id'], unit['id'], unit['token'])['path']
            payload = b''.join((json.dumps({'row': unit['first_row'] + offset,
                                'sha256': hashlib.sha256(line.rstrip(b'\r\n')).hexdigest(),
                                'bytes': len(line.rstrip(b'\r\n'))}) + '\n').encode()
                               for offset, line in enumerate(Path(path).read_bytes().splitlines(keepends=True)))
            self.service.accept_result(self.node['id'], unit['id'], unit['token'], io.BytesIO(payload), hashlib.sha256(payload).hexdigest())
        self.assertEqual(observed, [(start, 1000) for start in range(1, 10001, 1000)])
        response = client.get(f'/api/jobs/{job}/results')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.mimetype, 'text/plain')
        from urllib.parse import unquote
        disposition = unquote(response.headers['Content-Disposition'])
        self.assertRegex(disposition, r'\d{4}-\d{2}-\d{2}_处理成功_10000条\.txt')
        self.assertEqual(response.data, source)
        self.assertEqual(len(response.data.splitlines()), 10000)
        response.close()

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
