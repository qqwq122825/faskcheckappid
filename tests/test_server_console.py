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
        response=self.client.get('/api/jobs/'+job+'/exports/undetermined')
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
