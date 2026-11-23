import io
import json
from pathlib import Path
import tempfile
import time
import unittest

from fastcheck.version import VERSION
from unittest.mock import patch
import zipfile

from fastcheck.web.bootstrap import initialize
from fastcheck.web.app import create_app


class ServerTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        initialize(self.root,'test-console-password')
        self.app=create_app(self.root);self.client=self.app.test_client()
        self.csrf=self.client.get('/api/session').json['csrf']

    def tearDown(self):
        self.app.extensions['jobs'].shutdown();self.temp.cleanup()

    def login(self):
        response=self.client.post('/api/login',json={'username':'admin','password':'test-console-password'},headers={'X-CSRF-Token':self.csrf})
        self.assertEqual(response.status_code,200)
        self.csrf=response.json['csrf']

    def test_authentication_and_csrf_protect_controls(self):
        self.assertEqual(self.client.get('/api/settings').status_code,401)
        self.assertEqual(self.client.post('/api/settings',json={}).status_code,403)
        self.login()
        self.assertEqual(self.client.get('/api/settings').status_code,200)
        self.assertEqual(self.client.get('/healthz').json['version'],VERSION)

    def test_private_api_configuration_is_saved_but_never_returned(self):
        self.login()
        value='https://api.example.invalid/list?password=private-api-token&num=200'
        response=self.client.post('/api/settings',json={'settings':{'processes':8,'threads':10},'api_url':value},headers={'X-CSRF-Token':self.csrf})
        self.assertEqual(response.status_code,200)
        self.assertNotIn('private-api-token',self.client.get('/api/settings').get_data(as_text=True))
        self.assertTrue((self.root/'config/proxy-api.json').exists())

    def test_web_preserves_requested_process_and_thread_counts(self):
        self.login()
        response=self.client.post('/api/settings',json={'settings':{'processes':100,'threads':250}},
                                  headers={'X-CSRF-Token':self.csrf})
        self.assertEqual(response.status_code,200)
        saved=self.client.get('/api/settings').json['settings']
        self.assertEqual((saved['processes'],saved['threads']),(100,250))

    def test_device_settings_use_plain_node_semantics(self):
        response=self.client.get('/');page=response.get_data(as_text=True);response.close()
        self.assertIn('提前注册设备数',page)
        self.assertIn('每台设备最多使用次数',page)
        self.assertIn('设备不休息，用完立即回到设备池',page)
        self.assertNotIn('休息前使用轮数',page)
        self.assertNotIn('每次休息 / 秒',page)
        self.assertIn('当前设备池',page)
        self.assertIn('实时显示节点同核设备池',page)
        self.assertIn('id="device-total"',page)
        self.assertIn('id="device-registering"',page)
        script=(Path(__file__).parents[1]/'fastcheck/web/static/app.js').read_text()
        self.assertIn('if(devicePoolRequest)return devicePoolRequest',script)
        self.assertIn('applyDevicePoolSnapshot(data,revision)',script)

    def test_live_worker_results_are_visible_deduplicated_and_credential_free(self):
        from fastcheck.devices import save_json
        job='b'*32;directory=self.root/'jobs'/job
        save_json(directory/'state.json',{'id':job,'state':'running','accounts':2})
        save_json(directory/'accounts-index.json',[
            {'row':1,'email':'first@example.invalid'},
            {'row':2,'email':'second@example.invalid'},
        ])
        worker=directory/'workers/001';worker.mkdir(parents=True)
        first=json.dumps({
            'row':1,'account':'first@example.invalid','password':'secret-one',
            'status':'wrong_password','protocol':'GSA','code':'-22406',
            'stage':'complete','http':200,'device':7,
            'device_label':'B64:live-one','device_usage':3,
            'elapsed':2.5,'attempts':0,
        },separators=(',',':'))+'\n'
        second=json.dumps({
            'row':2,'account':'second@example.invalid','password':'secret-two',
            'status':'locked','protocol':'GSA','code':'-21669',
            'stage':'complete','http':200,'device':8,
            'device_label':'B64:live-two','device_usage':1,
            'elapsed':3.5,'attempts':0,
        },separators=(',',':'))+'\n'
        path=worker/'results.jsonl';path.write_text(first+second[:12])
        manager=self.app.extensions['jobs']
        live=manager.read_results(job)
        self.assertEqual(live['total'],1)
        self.assertNotIn('password',live['rows'][0])
        self.assertNotIn('account',live['rows'][0])
        self.assertEqual(live['rows'][0]['device_display'],'B64:live-one · 第 3 次')
        with path.open('a') as output:output.write(second[12:])
        self.assertEqual(manager.read_results(job)['total'],2)
        # The final merged log repeats worker records.  Row-keyed ingestion
        # must replace rather than duplicate them.
        (directory/'results.jsonl').write_text(first+second)
        final=manager.read_results(job)
        self.assertEqual(final['total'],2)
        self.assertNotIn('secret-one',json.dumps(final))
        self.assertNotIn('secret-two',json.dumps(final))

    def test_spawned_demo_status_exports_and_credential_free_logs(self):
        self.login()
        options={'protocol':'gsa','processes':2,'threads':2,'device_capacity':2,'device_parallelism':0,'debug':True,'max_attempts':1}
        response=self.client.post('/api/jobs',data={'simulation':'true','settings':json.dumps(options)},headers={'X-CSRF-Token':self.csrf})
        self.assertEqual(response.status_code,202)
        job=response.json['id'];deadline=time.monotonic()+15
        while time.monotonic()<deadline:
            state=self.client.get('/api/jobs/'+job).json
            if state['state'] in ('completed','failed','stopped','unknown'):break
            time.sleep(.1)
        self.assertEqual(state['state'],'completed',state)
        self.assertEqual(state['summary']['accounts'],20)
        self.assertFalse((self.root/'config/batch-device-pool').exists(), '演示身份必须与真实设备池隔离')
        rows=self.client.get('/api/jobs/'+job+'/results').json['rows']
        self.assertEqual(len(rows),20)
        self.assertNotIn('demo-password',json.dumps(rows))
        response=self.client.get('/api/jobs/'+job+'/exports/invalid')
        self.assertIn(b'demo-1@example.invalid----demo-password',response.data)
        zipped=self.client.get('/api/jobs/'+job+'/logs')
        with zipfile.ZipFile(io.BytesIO(zipped.data)) as archive:
            self.assertNotIn('accounts-index.json',archive.namelist())
            content=b'\n'.join(archive.read(name) for name in archive.namelist())
            self.assertNotIn(b'demo-password',content)
            self.assertIn(b'-20970',content)

    def test_real_tasks_reject_inherited_mock_mode(self):
        self.login()
        with patch.dict('os.environ',{'FASTCHECK_MOCK':'1'}):
            response=self.client.post('/api/jobs',data={'accounts_text':'owned@example.invalid----secret'},
                                      headers={'X-CSRF-Token':self.csrf})
        self.assertEqual(response.status_code,400)
        self.assertIn('FASTCHECK_MOCK',response.json['error'])

    def test_real_task_upload_keeps_duplicate_account_rows(self):
        self.login();captured=[]
        def start(accounts,*_args):
            captured.extend((row.row,row.email,row.password) for row in accounts)
            return {'id':'captured-duplicate-rows'}
        with patch('fastcheck.protocols.gsa_status.mock_enabled',return_value=False), \
             patch('fastcheck.web.app.load_api',return_value='https://proxy.example/list'), \
             patch.object(self.app.extensions['jobs'],'start',side_effect=start):
            response=self.client.post('/api/jobs',data={
                'accounts_text':'same@example.com----one\nSAME@example.com----two\nsame@example.com----three\n',
                'settings':json.dumps({'protocol':'gsa'}),
            },headers={'X-CSRF-Token':self.csrf})
        self.assertEqual(response.status_code,202,response.json)
        self.assertEqual(captured,[(1,'same@example.com','one'),
                                   (2,'SAME@example.com','two'),
                                   (3,'same@example.com','three')])

    def test_local_node_backend_reuses_executor_and_preserves_device_label(self):
        from fastcheck.scheduler import Settings
        from fastcheck.task_io import Account
        from fastcheck.devices import save_json
        from fastcheck.web import runtime
        executor=runtime._node_executor()
        self.assertEqual((executor.__module__,executor.__name__),
                         ('clusterlab.executor','execute_unit_group'))
        job='a'*32;directory=self.root/'jobs'/job;directory.mkdir(parents=True)
        accounts=[Account(1,'same@example.com','one'),
                  Account(2,'SAME@example.com','two')]
        save_json(directory/'state.json',{'id':job,'state':'running','accounts':2})
        save_json(directory/'accounts-index.json',[{'row':a.row,'email':a.email}
                                                    for a in accounts])
        captured={}
        def fake_node_executor(target,descriptor,_stop):
            captured.update(descriptor)
            records=[]
            for row,status in ((1,'init_refused'),(2,'need_2fa')):
                records.append(json.dumps({
                    'row':row,'status':status,'protocol':'GSA','code':'-20970',
                    'stage':'init','http':200,'device':row,
                    'device_label':f'B64:device-{row}','device_usage':2,
                    'elapsed':1.25,'attempts':1,
                },separators=(',',':')))
            (Path(target)/'results.jsonl').write_text('\n'.join(records)+'\n')
            save_json(Path(target)/'progress.json',{
                'processed':2,'counts':{'init_refused':1,'need_2fa':1},
                'active_threads_total':2,'active_threads_by_process':[2]})
            save_json(Path(target)/'done.json',{
                'ok':True,'processed':2,'counts':{'init_refused':1,'need_2fa':1},
                'elapsed':2.5,'active_threads_total':2,
                'active_threads_by_process':[2]})
        progress=[]
        settings=Settings(protocol='gsa',processes=4,threads=250,device_use_limit=7,
                          device_capacity=0,warmup_devices=0)
        with patch('fastcheck.web.runtime._node_executor',return_value=fake_node_executor):
            summary=runtime.run_node_auth_job(
                self.root,directory,accounts,settings,'https://proxy.example/list',
                progress.append)
        self.assertEqual((captured['processes'],captured['threads']),(4,250))
        self.assertEqual(captured['device_slots'],[1,2])
        self.assertEqual(captured['device_use_limit'],7)
        self.assertEqual((directory/'input.part').read_text().splitlines(),[
            'same@example.com----one','SAME@example.com----two'])
        self.assertEqual(summary['executor_backend'],'clusterlab.executor')
        rows=self.app.extensions['jobs'].read_results(job)['rows']
        self.assertEqual(rows[0]['category'],'init_refused')
        self.assertEqual(rows[0]['device_display'],'B64:device-1 · 第 2 次')
        self.assertEqual(rows[0]['result_code'],'-20970')
        self.assertEqual(rows[0]['retry_count'],1)

    def test_local_prebuild_adapter_uses_node_device_builder(self):
        from fastcheck.web.app import LocalNodeDeviceBuilder
        builder=LocalNodeDeviceBuilder(self.root,'https://new.ani.sidestore.io')
        self.assertEqual(builder.builder.__class__.__module__,'clusterlab.device_build')
        self.assertEqual(builder.snapshot()['ready'],0)
        pending={'ready':0,'slots':[{'slot':1,'state':'pending','device_id':'','error':''}]}
        with patch.object(builder.builder,'snapshot',return_value=pending):
            self.assertEqual(builder.snapshot()['slots'],[])
            self.assertEqual(builder.snapshot(include_pending=True)['slots'],pending['slots'])

        mixed={'ready':1,'slots':[
            {'slot':1,'state':'ready','device_id':'current','error':''},
            {'slot':2,'state':'pending','device_id':'','error':''},
            {'slot':3,'state':'missing','device_id':'','error':''},
            {'slot':999,'state':'pending','device_id':'','error':''},
        ]}
        with patch.object(builder.builder,'snapshot',return_value=mixed):
            current=builder.snapshot(include_pending=True,slot_ids=[1,2,3])
            self.assertEqual([item['slot'] for item in current['slots']],[1,2,3])
            self.assertEqual(current['ready'],1)

    def test_stop_and_second_task_conflict(self):
        self.login();headers={'X-CSRF-Token':self.csrf}
        data={'simulation':'true','demo_count':'100','settings':json.dumps({'processes':1,'threads':1})}
        response=self.client.post('/api/jobs',data=data,headers=headers);job=response.json['id']
        self.assertEqual(self.client.post('/api/jobs',data=data,headers=headers).status_code,409)
        self.assertEqual(self.client.post('/api/jobs/'+job+'/stop',headers=headers).status_code,202)
        deadline=time.monotonic()+10
        while time.monotonic()<deadline:
            state=self.client.get('/api/jobs/'+job).json
            if state['state'] in ('completed','failed','stopped','unknown'):break
            time.sleep(.1)
        self.assertEqual(state['state'],'stopped')


if __name__=='__main__':unittest.main()
