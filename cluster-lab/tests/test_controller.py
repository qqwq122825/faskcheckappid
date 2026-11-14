import hashlib
import io
import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

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

    def test_multiple_proxy_apis_are_round_robin_distributed_to_nodes_and_leases(self):
        first = 'https://proxy.example/one?password=secret&num=200'
        second = 'https://proxy.example/two?token=hidden&num=200'
        settings = self.service.configure_proxy({
            'action': 'add', 'proxy_api': first, 'proxy_scheme': 'socks5h'})
        settings = self.service.configure_proxy({
            'action': 'add', 'proxy_api': second, 'proxy_scheme': 'socks5h'})
        self.assertEqual(settings['proxy_count'], 2)
        self.assertEqual(settings['proxy_apis'], [
            'https://proxy.example/one?password=***&num=200',
            'https://proxy.example/two?token=***&num=200'])

        other = self.service.add_node('second-node', 2, 4)
        self.service.heartbeat(other['id'], dict(session='second-session', max_processes=1,
                               max_threads=2, active=[]))
        with self.service.db.connect() as db:
            nodes = list(db.execute('SELECT * FROM nodes ORDER BY created,id'))
        self.assertEqual(self.service.proxy_for_node(nodes[0])['api'], first)
        self.assertEqual(self.service.proxy_for_node(nodes[1])['api'], second)

        job = self.service.create_job('auth-proxy', io.BytesIO(b'a@example.com----p\n' * 50),
                                      50, mode='auth', enqueue=True)
        deadline = time.monotonic() + 5
        while self.service.snapshot()['current_job_id'] != job and time.monotonic() < deadline:
            time.sleep(.01)
        unit = self.service.lease(self.node['id'], 'test-session')
        self.assertEqual(unit['proxy_api'], first)
        self.assertNotIn(second, unit['proxy_api'])

        settings = self.service.configure_proxy({'action': 'delete', 'index': 0})
        self.assertEqual(settings['proxy_count'], 1)
        self.assertEqual(self.service.proxy_config()['api'], second)

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
        self.assertEqual(unit['processes'], 50)  # 50 rows cannot use more than 50 processes.
        self.assertIsNone(self.service.lease(node['id'], 'big-session'))

    def test_legacy_32_thread_ceiling_upgrades_to_node_100_thread_capacity(self):
        node = self.service.add_node('legacy-32', 10, 32)
        response = self.service.heartbeat(node['id'], {
            'session': 'legacy-session', 'version': '0.3.5',
            'max_processes': 10, 'max_threads': 100, 'active': [], 'interrupted': []})
        self.assertEqual((response['processes'], response['threads']), (10, 100))
        self.assertEqual(response['desired_threads'], 100)
        with self.service.db.connect() as db:
            self.assertEqual(db.execute('SELECT desired_threads FROM nodes WHERE id=?',
                                        (node['id'],)).fetchone()[0], 100)

    def test_lease_carries_job_timeout_and_indexed_package_bytes(self):
        job = self.service.create_job('indexed', io.BytesIO(b'hello\n' * 100), 50,
                                      timeout=37, retries=3)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            state = next(j for j in self.service.snapshot()['jobs'] if j['id'] == job)
            if state['status'] == 'ready':
                break
            time.sleep(.01)
        self.service.job_action(job, 'start')
        unit = self.service.lease(self.node['id'], 'test-session')
        self.assertEqual((unit['timeout'], unit['retries']), (37, 3))
        self.assertEqual(unit['bytes'], 300)
        indexed = self.service.unit_for_node(self.node['id'], unit['id'], unit['token'])
        self.assertEqual(b''.join(self.service.unit_blocks(indexed)), b'hello\n' * 50)
        with self.service.db.connect() as db:
            paths = {row['path'] for row in db.execute('SELECT path FROM units WHERE job_id=?', (job,))}
        self.assertEqual(len(paths), 1, 'all packages should reference the one uploaded source file')

    def test_lease_carries_task_device_pool_target(self):
        job = self.service.create_job('devices', io.BytesIO(b'hello\n' * 50), 50,
                                      enqueue=True, device_capacity=30)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            state = next(item for item in self.service.snapshot()['jobs'] if item['id'] == job)
            if state['status'] == 'running':
                break
            time.sleep(.01)
        unit = self.service.lease(self.node['id'], 'test-session')
        self.assertEqual(unit['device_capacity'], 30)

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

    def test_live_category_feedback_updates_each_heartbeat_without_double_counting(self):
        job = self.job(50)
        self.service.job_action(job, 'start')
        unit = self.service.lease(self.node['id'], 'test-session')
        live = {'wrong_password': 3, 'locked': 2, 'undetermined': 5}
        self.service.heartbeat(self.node['id'], {
            'session': 'test-session', 'version': '0.3.6',
            'max_processes': 1, 'max_threads': 2, 'interrupted': [],
            'active': [{'id': unit['id'], 'token': unit['token'],
                        'processed': 10, 'counts': live}]})
        state = next(value for value in self.service.snapshot()['jobs'] if value['id'] == job)
        self.assertEqual(state['processed'], 10)
        self.assertEqual(state['categorized'], {
            'valid': 0, 'wrong_password': 3, 'need_2fa': 0,
            'locked': 2, 'timeout': 0, 'unknown': 5})

        statuses = (['wrong_password'] * 3 + ['locked'] * 2 +
                    ['undetermined'] * 5 + ['valid'] * 40)
        payload = b''.join((json.dumps({
            'row': unit['first_row'] + offset, 'status': status,
            'sha256': hashlib.sha256(b'hello').hexdigest(), 'bytes': 5},
            separators=(',', ':')) + '\n').encode()
            for offset, status in enumerate(statuses))
        self.service.accept_result(self.node['id'], unit['id'], unit['token'],
                                   io.BytesIO(payload), hashlib.sha256(payload).hexdigest())
        state = next(value for value in self.service.snapshot()['jobs'] if value['id'] == job)
        self.assertEqual(state['categorized'], {
            'valid': 40, 'wrong_password': 3, 'need_2fa': 0,
            'locked': 2, 'timeout': 0, 'unknown': 5})
        with self.service.db.connect() as db:
            self.assertIsNone(db.execute('SELECT live_counts FROM units WHERE id=?',
                                         (unit['id'],)).fetchone()[0])

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
            ranges = [(r['byte_start'], r['byte_end']) for r in
                      db.execute('SELECT byte_start,byte_end FROM units WHERE job_id=?', (job,))]
        self.assertTrue(all(end - start <= 4 * 1024 * 1024 for start, end in ranges))

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
            indexed = self.service.unit_for_node(self.node['id'], unit['id'], unit['token'])
            payload = b''.join((json.dumps({'row': unit['first_row'] + offset,
                                'sha256': hashlib.sha256(line.rstrip(b'\r\n')).hexdigest(),
                                'bytes': len(line.rstrip(b'\r\n'))}) + '\n').encode()
                               for offset, line in enumerate(self.service.unit_lines(indexed)))
            self.service.accept_result(self.node['id'], unit['id'], unit['token'], io.BytesIO(payload), hashlib.sha256(payload).hexdigest())
        self.assertEqual(observed, [(start, 1000) for start in range(1, 10001, 1000)])
        response = client.get(f'/api/jobs/{job}/results')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.mimetype, 'text/plain')
        from urllib.parse import unquote
        response.headers['Content-Disposition'].encode('latin-1')
        self.assertNotIn('全部结果', response.headers['Content-Disposition'])
        disposition = unquote(response.headers['Content-Disposition'])
        self.assertRegex(disposition, r'\d{4}-\d{2}-\d{2}_全部结果\.txt')
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

    def test_10002_records_keep_the_final_two_record_package(self):
        source = b''.join(f'account-{index:05d}\n'.encode() for index in range(10000)) + b'\n\n'
        job = self.service.create_job('10002 records', io.BytesIO(source), 10000)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            state = next(item for item in self.service.snapshot()['jobs'] if item['id'] == job)
            if state['status'] != 'preparing':
                break
            time.sleep(.01)
        self.assertEqual(state['status'], 'ready')
        self.assertEqual((state['rows'], state['units']), (10002, 2))
        with self.service.db.connect() as db:
            units = [dict(row) for row in db.execute(
                'SELECT first_row,rows,path,byte_start,byte_end FROM units WHERE job_id=? ORDER BY seq',
                (job,))]
        self.assertEqual([(unit['first_row'], unit['rows']) for unit in units], [(1, 10000), (10001, 2)])
        self.assertEqual(b''.join(self.service.unit_lines(units[1])), b'\n\n')

        self.service.job_action(job, 'start')
        first = self.service.lease(self.node['id'], 'test-session')
        self.assertEqual(first['rows'], 10000)
        self.assertIsNone(self.service.lease(self.node['id'], 'test-session'))
        self.confirm(first)
        second = self.service.lease(self.node['id'], 'test-session')
        self.assertEqual((second['first_row'], second['rows']), (10001, 2))
        self.confirm(second)
        self.assertEqual(self.service.snapshot()['jobs'][0]['status'], 'completed')

    def test_one_hundred_million_row_high_offset_opens_only_target_shard(self):
        """1 亿条任务翻到末页时，不扫前 9,999 个分片。"""
        job = self.job(50)
        result = Path(self.temp.name) / 'last-10000.jsonl'
        first = 100_000_000 - 10_000 + 1
        with result.open('w', encoding='utf-8') as stream:
            for row in range(first, 100_000_001):
                stream.write(json.dumps({'row': row, 'status': 'valid'}) + '\n')
        now = time.time()
        units = []
        for seq in range(1, 10_001):
            start = (seq - 1) * 10_000 + 1
            units.append((f'u{seq:05d}', job, seq, start, 10_000, str(result), '0' * 64,
                          'completed', 10_000, str(result), '0' * 64,
                          '{"valid":10000}', now))
        with self.service.db.transaction() as db:
            db.execute('DELETE FROM units WHERE job_id=?', (job,))
            db.executemany(
                'INSERT INTO units(id,job_id,seq,first_row,rows,path,checksum,status,processed,'
                'result_path,result_checksum,result_counts,updated) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                units)
            db.execute("UPDATE jobs SET rows=100000000,status='completed' WHERE id=?", (job,))

        opened = []
        original_open = Path.open
        def counting_open(path, *args, **kwargs):
            if path == result:
                opened.append(path)
            return original_open(path, *args, **kwargs)
        with patch.object(Path, 'open', counting_open):
            page = self.service.read_rows(job, 99_999_950, 50)
        self.assertEqual([row['row'] for row in page['rows']], list(range(99_999_951, 100_000_001)))
        self.assertEqual(page['total'], 100_000_000)
        self.assertEqual(len(opened), 1)

    def test_category_page_uses_persisted_shard_counts(self):
        job = self.job(100)
        self.service.job_action(job, 'start')
        while unit := self.service.lease(self.node['id'], 'test-session'):
            self.confirm(unit)
        page = self.service.read_rows(job, 50, 1, 'valid')
        self.assertEqual(page['total'], 100)
        self.assertEqual(page['rows'][0]['row'], 51)
        with self.service.db.connect() as db:
            counts = [json.loads(row['result_counts']) for row in
                      db.execute('SELECT result_counts FROM units WHERE job_id=? ORDER BY seq', (job,))]
        self.assertEqual(counts, [{'valid': 50}, {'valid': 50}])

    def test_raw_stream_upload_avoids_multipart_copy(self):
        client = create_app('controller', self.service).test_client()
        csrf = client.get('/api/session').json['csrf']
        response = client.post('/api/jobs/stream?name=large.txt&unit_rows=50&mode=digest&enqueue=true',
                               data=b'hello\n' * 50,
                               content_type='application/octet-stream',
                               headers={'X-CSRF-Token': csrf, 'Content-Length': '300'})
        self.assertEqual(response.status_code, 202)
        self.assertEqual(response.json['bytes'], 300)

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
