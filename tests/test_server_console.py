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
        settings=Settings(protocol='gsa',processes=4,threads=250,
                          device_capacity=0,warmup_devices=0)
        with patch('fastcheck.web.runtime._node_executor',return_value=fake_node_executor):
            summary=runtime.run_node_auth_job(
                self.root,directory,accounts,settings,'https://proxy.example/list',
                progress.append)
        self.assertEqual((captured['processes'],captured['threads']),(4,250))
        self.assertEqual(captured['device_slots'],[1,2])
        self.assertEqual((directory/'input.part').read_text().splitlines(),[
            'same@example.com----one','SAME@example.com----two'])
        self.assertEqual(summary['executor_backend'],'clusterlab.executor')
        rows=self.app.extensions['jobs'].read_results(job)['rows']
        self.assertEqual(rows[0]['category'],'init_refused')
        self.assertEqual(rows[0]['device_display'],'B64:device-1 · 第 2 次')
        self.assertEqual(rows[0]['result_code'],'-20970')
        self.assertEqual(rows[0]['retry_count'],1)

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
