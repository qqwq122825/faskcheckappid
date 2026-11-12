import base64
from concurrent.futures import ThreadPoolExecutor
import contextlib
from dataclasses import replace
import io
import json
import os
from pathlib import Path
import socket
import tempfile
import threading
import unittest
from unittest.mock import patch

from fastcheck.scheduler import Settings, Supervisor
from fastcheck.workers import demo_attempt, demo_registration, protocol_attempt
from batch_fixtures import (supervisor_retry, supervisor_limited, supervisor_hang,
                            supervisor_crash, supervisor_debug,supervisor_retry_priority)
from archive.legacy.gsa_batch import Account
from fastcheck.protocols import idmsa
from fastcheck.protocols import gsa_status as gsa
from fastcheck.devices import DevicePool
from fastcheck.protocols.anisette_v3 import AnisetteError
from test_gsa_login import SyntheticServer


class SupervisorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.serial = 0
        self.guard = patch.object(socket.socket, 'connect', side_effect=AssertionError('No network'))
        self.guard.start()

    def tearDown(self):
        self.guard.stop()
        self.temp.cleanup()

    def fetch(self, url, count):
        result = []
        for _ in range(count):
            self.serial += 1
            result.append(f'http://private-user:private-proxy-secret@127.0.0.1:{10000+self.serial}')
        return result

    def runner(self, count=20, handler=demo_attempt, **options):
        settings = replace(Settings(resource_timeout=15,device_parallelism=1,
                                    device_selection='balanced',max_attempts=2), **options)
        accounts = [Account(n, f'private-account-{n}@example.invalid', f'private-password-{n}')
                    for n in range(1, count+1)]
        return Supervisor(accounts, 'https://api.example.invalid/?num=200&password=private-api-secret',
            self.root, self.root/'logs', settings, handler=handler,
            registration=demo_registration, fetcher=self.fetch)

    def run_quiet(self, runner):
        with contextlib.redirect_stdout(io.StringIO()):
            return runner.run()

    def events(self):
        return [json.loads(v) for v in (self.root/'logs/events.jsonl').read_text().splitlines()]

    def test_two_real_processes_ten_real_threads_each_handle_twenty_tasks(self):
        runner = self.runner()
        summary = self.run_quiet(runner)
        self.assertIsNone(summary['stop_reason'])
        self.assertEqual(summary['max_active'], 20)
        self.assertEqual(summary['results'], {'init_refused':1, 'need_2fa':19})
        events = self.events()
        started = [e for e in events if e['event']=='task_started']
        self.assertEqual(len({e['worker_pid'] for e in started}), 2)
        for worker in (1, 2):
            self.assertEqual(len({e['worker_thread'] for e in started if e['worker']==worker}), 10)
        self.assertTrue(all(e['worker_pid'] != os.getpid() for e in started))
        self.assertTrue(all(e['worker_active'] <= 10 for e in events if e['event']=='task_dispatch'))
        self.assertFalse((self.root/'config').exists())
        self.assertFalse(any(e['event']=='task_retry' for e in events))
        self.assertTrue(all(a.password == '' for a in runner.accounts))

    def test_one_hundred_tasks_prefetch_once_and_do_not_top_up_after_dispatch(self):
        runner=self.runner(count=100,processes=5,threads=10,max_attempts=1)
        # 提取链接里写了 num=200，这个数就是本批允许的上限，100 个账号全部预取。
        self.assertEqual(runner.initial_proxy_target,100)
        summary=self.run_quiet(runner)
        self.assertIsNone(summary['stop_reason'])
        self.assertEqual(sum(summary['results'].values()),100)
        self.assertLessEqual(summary['max_active'],50)
        events=self.events()
        requests=[e['requested'] for e in events if e['event']=='proxy_fetch_start']
        self.assertEqual(requests,[100])
        self.assertEqual(len({e['worker_pid'] for e in events if e['event']=='worker_ready'}),5)
        self.assertEqual(len([e for e in events if e['event']=='task_dispatch']),100)
        self.assertEqual(runner.proxies.target,0)

    def test_gsa_devices_are_exclusive_and_can_be_reused_after_two_ready(self):
        runner = self.runner(protocol='gsa', device_capacity=3)
        summary = self.run_quiet(runner)
        self.assertIsNone(summary['stop_reason'])
        self.assertLessEqual(summary['max_active'], 3)
        busy, uses = set(), {}
        for event in self.events():
            if event['event']=='task_dispatch':
                self.assertNotIn(event['device'], busy)
                busy.add(event['device'])
                uses[event['device']] = uses.get(event['device'],0)+1
            if event['event']=='task_done':
                busy.remove(event['device'])
        self.assertFalse(busy)
        self.assertTrue(any(value>1 for value in uses.values()))
        self.assertEqual(sum(summary['results'].values()),20)
        self.assertFalse(runner.registration_thread.is_alive())
        self.assertFalse(runner.proxies.thread.is_alive())
        self.assertFalse(runner.pool.health()['events'])

    def test_only_transient_init_retries_and_keeps_device(self):
        runner = self.runner(count=4, handler=supervisor_retry, protocol='gsa', device_capacity=2)
        summary = self.run_quiet(runner)
        self.assertIsNone(summary['stop_reason'])
        dispatch = [e for e in self.events() if e['event']=='task_dispatch']
        first = [e for e in dispatch if e['row']==1]
        self.assertEqual(len(first),2)
        self.assertEqual(first[0]['device'],first[1]['device'])
        self.assertEqual(len([e for e in dispatch if e['row']==2]),1)
        self.assertEqual(len([e for e in dispatch if e['row']==3]),1)
        self.assertEqual(runner.results[3]['status'],'init_refused')

    def test_device_rests_preserve_identities_and_do_not_retry_20970(self):
        runner=self.runner(count=20,protocol='gsa',device_capacity=3,
                           device_rest_after=2,device_rest_seconds=.1,device_rest_slots=1)
        original={}
        for slot in (1,2,3):
            store=runner.pool.store(slot)
            state=store.load()
            state['adi_pb']=base64.b64encode(b'offline-existing-state').decode()
            store.save(state)
            original[slot]=runner.pool.inspect(slot)['device_id']
        summary=self.run_quiet(runner)
        self.assertIsNone(summary['stop_reason'])
        self.assertEqual(sum(summary['results'].values()),20)
        events=self.events()
        self.assertTrue(any(e['event']=='device_rest_started' for e in events))
        self.assertTrue(any(e['event']=='device_rest_ended' for e in events))
        self.assertFalse(any(e['event']=='registration_start' for e in events))
        self.assertEqual(len([e for e in events if e['event']=='task_dispatch' and e['row']==1]),1)
        self.assertEqual(runner.results[1]['status'],'init_refused')
        self.assertEqual(sum(v['total_uses'] for v in summary['device_usage'].values()),20)
        self.assertEqual({s:runner.pool.inspect(s)['device_id'] for s in original},original)

    def test_due_retry_does_not_hold_device_behind_unstarted_accounts(self):
        runner=self.runner(count=20,protocol='gsa',device_capacity=2,
                           handler=supervisor_retry_priority,max_attempts=2)
        summary=self.run_quiet(runner)
        self.assertIsNone(summary['stop_reason'])
        dispatch=[v for v in self.events() if v['event']=='task_dispatch']
        retry=next(i for i,v in enumerate(dispatch) if v['row']==1 and v['attempt']==2)
        last=next(i for i,v in enumerate(dispatch) if v['row']==20 and v['attempt']==1)
        self.assertLess(retry,last)

    def test_two_devices_can_fill_eighty_thread_slots_when_sharing_is_enabled(self):
        runner=self.runner(count=82,protocol='gsa',processes=8,threads=10,
                           device_capacity=2,device_parallelism=0,max_attempts=1)
        summary=self.run_quiet(runner)
        self.assertIsNone(summary['stop_reason'])
        self.assertEqual(sum(summary['results'].values()),82)
        self.assertGreater(summary['max_active'],10)
        self.assertLessEqual(summary['max_active'],80)
        self.assertEqual(summary['ready_devices'],2)

    def test_empty_pool_reports_zero_of_target_before_startup_and_two_ready_gate(self):
        runner=self.runner(count=4,protocol='gsa',device_capacity=3,processes=1,threads=2)
        progress=[];runner.progress=progress.append
        summary=self.run_quiet(runner)
        self.assertEqual((progress[0]['devices']['total'],progress[0]['devices']['target']),(0,3))
        self.assertEqual(progress[0]['devices']['restored'],0)
        self.assertEqual(summary['device_pool']['registered_this_run'],summary['ready_devices'])
        ready=0
        for event in self.events():
            if event['event']=='registration_ready':ready=event['ready_devices']
            if event['event']=='task_dispatch':self.assertGreaterEqual(ready,2)

    def test_saved_pool_reports_reuse_instead_of_new_registration(self):
        runner=self.runner(count=4,protocol='gsa',device_capacity=2,processes=1,threads=2)
        for slot in (1,2):
            store=runner.pool.store(slot);state=store.load()
            state['adi_pb']=base64.b64encode(b'offline-existing-state').decode();store.save(state)
        progress=[];runner.progress=progress.append
        summary=self.run_quiet(runner)
        self.assertEqual((progress[0]['devices']['ready'],progress[0]['devices']['target']),(2,2))
        self.assertEqual(progress[0]['devices']['restored'],2)
        self.assertEqual(summary['device_pool']['registered_this_run'],0)
        self.assertFalse(any(event['event']=='registration_start' for event in self.events()))

    def test_429_stops_without_scheduling_rest_or_retrying(self):
        runner = self.runner(count=5, handler=supervisor_limited, processes=1, threads=1)
        summary = self.run_quiet(runner)
        self.assertEqual(summary['stop_reason'],'authentication_ratelimit')
        self.assertEqual(summary['results'],{'error':1,'not_started':4})
        self.assertEqual(summary['display_counts']['processed'],1)
        self.assertEqual(summary['display_counts']['remaining'],4)
        self.assertEqual(len([e for e in self.events() if e['event']=='task_dispatch']),1)

    def test_hung_thread_reaps_process_and_marks_all_inflight_unknown(self):
        runner = self.runner(count=5, handler=supervisor_hang, processes=1, threads=2, attempt_budget=.5)
        summary = self.run_quiet(runner)
        self.assertEqual(summary['stop_reason'],'worker_deadline_unknown_inflight')
        self.assertEqual(summary['results'],{'unknown':2,'not_started':3})
        pids={e['worker_pid'] for e in self.events() if e['event']=='worker_ready'}
        for pid in pids:
            with self.assertRaises(ProcessLookupError):
                os.kill(pid,0)
        self.assertFalse(any(e['event']=='task_retry' for e in self.events()))

    def test_process_crash_does_not_repeat_inflight_credentials(self):
        runner = self.runner(count=4, handler=supervisor_crash, processes=1, threads=2)
        summary = self.run_quiet(runner)
        self.assertEqual(summary['stop_reason'],'worker_exit_unknown_inflight')
        self.assertEqual(summary['results'],{'unknown':2,'not_started':2})

    def test_task_results_and_debug_filter_secrets(self):
        runner = self.runner(count=2, handler=supervisor_debug, debug=True)
        self.run_quiet(runner)
        content='\n'.join(p.read_text() for p in (self.root/'logs').iterdir() if p.is_file())
        for secret in ('private-password', 'private-api-secret', 'private-proxy-secret',
                       'private-account', 'unwanted-diagnostic-secret', 'unwanted-result-secret', 'unwanted-cookie'):
            self.assertNotIn(secret,content)
        self.assertIn('offline_test',content)


class WebAdapterTests(unittest.TestCase):
    def test_concurrent_accounts_keep_independent_cookie_and_session_headers(self):
        barrier = threading.Barrier(10)
        clients = []
        class Client:
            def __init__(self, *args, **kwargs):
                self.headers = {}
                self.token = 'secret-token-'+str(id(self))
                clients.append(self)
            def __enter__(self): return self
            def __exit__(self,*args): pass
            def post(self,url,payload):
                if url==idmsa.IDMSA_INIT:
                    self.email=payload['accountName']
                    barrier.wait(5)
                    body={'salt':base64.b64encode(b'salt').decode(),'b':base64.b64encode(b'b').decode(),
                          'c':self.token,'iteration':100,'protocol':'s2k'}
                    return 200,json.dumps(body).encode(),{'scnt':self.token,'x-apple-id-session-id':self.token},''
                if payload['accountName']!=self.email or payload['c']!=self.token:
                    raise AssertionError('Cross-account SRP state')
                if self.headers.get('scnt')!=self.token or self.headers.get('X-Apple-ID-Session-Id')!=self.token:
                    raise AssertionError('Cross-account session headers')
                if self.email=='account-0@example.invalid':
                    return 401,b'{"serviceErrors":[{"code":"-20283"}]}',{},''
                return 409,b'{"authType":"hsa2"}',{},''
        class SRP:
            A_bytes=b'A'
            M2_expected=b'M2'
            def __init__(self,*args): pass
            def process_challenge(self,*args): return b'M1'
        stop=threading.Event()
        with patch.object(idmsa,'_Client',Client), patch.object(idmsa,'SRPClient',SRP), \
             patch.object(socket.socket,'connect',side_effect=AssertionError('No network')):
            with ThreadPoolExecutor(max_workers=10) as executor:
                payloads=[{'protocol':'web','row':n,'attempt':1,'email':f'account-{n}@example.invalid',
                           'password':'secret','proxy':'http://127.0.0.1:9','timeout':1,'debug':False} for n in range(10)]
                results=list(executor.map(lambda payload:protocol_attempt(payload,None,stop),payloads))
        self.assertEqual(len(clients),10)
        self.assertEqual(len({c.token for c in clients}),10)
        self.assertEqual(results[0]['status'],'credentials_rejected')
        self.assertEqual(results[0]['apple_ec'],-20283)
        self.assertEqual(results[0]['complete_http'],401)
        self.assertTrue(all(r['status']=='need_2fa' and r['complete_http']==409 for r in results[1:]))


class GSAAdapterTests(unittest.TestCase):
    def test_concurrent_srp_server_proofs_remain_bound_to_each_task(self):
        identity={'X-Apple-I-MD':'otp','X-Apple-I-MD-M':'machine','X-Apple-I-MD-LU':'user',
                  'X-Apple-I-MD-RINFO':'17106176','X-Mme-Device-Id':'device',
                  'X-Mme-Client-Info':'client','User-Agent':'agent'}
        with tempfile.TemporaryDirectory() as directory:
            pool=DevicePool(directory,'https://new.ani.sidestore.io',max_devices=10)
            for slot in range(1,11):
                store=pool.store(slot)
                state=store.load()
                state['adi_pb']=base64.b64encode(b'offline-state').decode()
                store.save(state)
            payloads=[{'protocol':'gsa','row':n,'slot':n,'attempt':1,
                       'email':f'account-{n}@example.invalid','password':'synthetic-password',
                       'proxy':'http://127.0.0.1:9','timeout':1,'debug':False,
                       'prepare_timeout':1,'device_root':directory,'origin':pool.origin,
                       'device_capacity':10} for n in range(1,11)]
            with patch.object(socket.socket,'connect',side_effect=AssertionError('No network')), \
                 patch.object(DevicePool,'info',return_value={}), \
                 patch.object(DevicePool,'fetch_headers',return_value=identity), \
                 patch.object(gsa,'_session_for',side_effect=lambda proxy:SyntheticServer(auth='trustedDeviceSecondaryAuth')):
                with ThreadPoolExecutor(max_workers=10) as executor:
                    results=list(executor.map(lambda p:protocol_attempt(p,None,threading.Event()),payloads))
            self.assertTrue(all(r['status']=='need_2fa' and r['password_verified'] for r in results))
            from fastcheck import workers
            workers._IDENTITY_CACHE.clear()   # 设备参数缓存会跳过 fetch_headers，先清空
            with patch.object(DevicePool,'info',return_value={}), \
                 patch.object(DevicePool,'fetch_headers',side_effect=AnisetteError('invalid_state')):
                result=protocol_attempt(payloads[0],None,threading.Event())
            self.assertEqual(result['kind'],'anisette')

    def test_same_registered_device_supports_parallel_isolated_srp_sessions(self):
        identity={'X-Apple-I-MD':'otp','X-Apple-I-MD-M':'machine','X-Apple-I-MD-LU':'user',
                  'X-Apple-I-MD-RINFO':'17106176','X-Mme-Device-Id':'device',
                  'X-Mme-Client-Info':'client','User-Agent':'agent'}
        barrier=threading.Barrier(4)
        class Server(SyntheticServer):
            def post(self,body,*args,**kwargs):
                import plistlib
                if plistlib.loads(body)['Request']['o']=='init':barrier.wait(5)
                return super().post(body,*args,**kwargs)
        with tempfile.TemporaryDirectory() as directory:
            pool=DevicePool(directory,'https://new.ani.sidestore.io')
            store=pool.store(1);state=store.load();state['adi_pb']=base64.b64encode(b'existing').decode();store.save(state)
            payloads=[{'protocol':'gsa','row':n,'slot':1,'device_parallelism':4,'attempt':1,
                       'email':f'account-{n}@example.invalid','password':'synthetic-password',
                       'proxy':None,'timeout':1,'debug':False,'prepare_timeout':1,
                       'device_root':directory,'origin':pool.origin,'device_capacity':2} for n in range(4)]
            with patch.object(socket.socket,'connect',side_effect=AssertionError('No network')), \
                 patch.object(DevicePool,'info',return_value={}), \
                 patch.object(DevicePool,'fetch_headers',return_value=identity), \
                 patch.object(gsa,'_session_for',side_effect=lambda proxy:Server(auth='trustedDeviceSecondaryAuth')):
                with ThreadPoolExecutor(max_workers=4) as executor:
                    results=list(executor.map(lambda p:protocol_attempt(p,None,threading.Event()),payloads))
            self.assertTrue(all(r['password_verified'] for r in results))


if __name__=='__main__':
    unittest.main()
