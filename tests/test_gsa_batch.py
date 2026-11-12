import base64
import contextlib
import io
import json
import multiprocessing as mp
import os
from pathlib import Path
import plistlib
import socket
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from fastcheck.proxy_pool import ApiFetcher, ProxyFetchError, ProxyLease, ProxyPool
from archive.legacy.gsa_batch import Account, BatchRunner, read_accounts
from fastcheck.devices import DevicePool
from fastcheck.protocols.anisette_v3 import Client, StateStore
from fastcheck.protocols.proxy_api import demand_url, parse
from batch_fixtures import auth, register, limited, hang, finish, api_ready, api_hang, debug_auth, retry_then_limit


class BatchTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.counter = 0
        self.guard = patch.object(socket.socket, 'connect', side_effect=AssertionError('Network forbidden'))
        self.guard.start()

    def tearDown(self):
        self.guard.stop()
        self.temp.cleanup()

    def fetch(self, url, count):
        values = []
        for _ in range(count):
            self.counter += 1
            values.append(f'http://proxy-user:proxy-secret@127.0.0.1:{20000 + self.counter}')
        return values

    def runner(self, count=10, **kwargs):
        accounts = [Account(i, f'owned{i}@example.invalid', f'password-secret-{i}') for i in range(1, count + 1)]
        return BatchRunner(accounts, 'https://api.example.invalid/?password=api-secret&num=200',
            self.root/'devices', self.root/'logs', proxy_fetcher=self.fetch,
            auth_target=kwargs.pop('auth_target', auth), registration_target=register,
            registration_gap=0, startup_timeout=15, job_budget=8, min_proxy_life=1, **kwargs)

    def seed(self, runner):
        for slot in (1, 2):
            store = runner.pool.store(slot)
            state = store.load()
            state['adi_pb'] = base64.b64encode(b'offline-state').decode()
            store.save(state)

    def events(self):
        return [json.loads(line) for line in (self.root/'logs/events.jsonl').read_text().splitlines()]

    def test_spawned_processes_reuse_two_devices_and_bound_concurrency_and_retries(self):
        runner = self.runner(count=20)
        with contextlib.redirect_stdout(io.StringIO()):
            summary = runner.run()
        self.assertEqual(sum(summary['results'].values()), 20)
        self.assertEqual(summary['results']['need_2fa'], 19)
        self.assertEqual(summary['results']['wrong_password'], 1)
        events = self.events()
        dispatches = [x for x in events if x['event']=='account_dispatch']
        self.assertLessEqual(max(x['active_auth'] for x in dispatches), 2)
        self.assertTrue(all(x['worker_pid'] != os.getpid() for x in dispatches))
        first = [x for x in dispatches if x['row']==1]
        self.assertEqual(len(first), 2)
        self.assertEqual(first[0]['device'], first[1]['device'])
        self.assertEqual(first[0]['proxy'], first[1]['proxy'])
        self.assertNotEqual(first[0]['worker_pid'], first[1]['worker_pid'])
        self.assertEqual(len([x for x in dispatches if x['row']==2]), 1)
        third = next(x for x in dispatches if x['row']==3)
        self.assertEqual(third['ready_devices'], 2)
        self.assertIn(third['device'], (1, 2))
        busy = set()
        for event in events:
            if event['event']=='account_dispatch' and event['attempt']==1:
                self.assertNotIn(event['device'], busy)
                busy.add(event['device'])
            if event['event']=='account_done':
                busy.discard(event['device'])
        self.assertFalse(busy)
        self.assertFalse(runner.reg_thread.is_alive())
        self.assertFalse(runner.proxies.thread.is_alive())
        for path in (self.root/'logs').iterdir():
            body = path.read_text()
            for secret in ('password-secret', 'api-secret', 'proxy-secret', 'owned1@'):
                self.assertNotIn(secret, body)

    def test_429_stops_batch_without_retrying_or_sending_remaining_accounts(self):
        runner = self.runner(auth_target=limited)
        self.seed(runner)
        with contextlib.redirect_stdout(io.StringIO()):
            summary = runner.run()
        self.assertEqual(summary['stop_reason'], 'authentication_ratelimit')
        self.assertGreater(summary['results'].get('not_started', 0), 0)
        self.assertFalse(any(x['event']=='account_retry' for x in self.events()))

    def test_stopping_during_backoff_preserves_started_row_and_attempt_history(self):
        runner = self.runner(count=5,auth_target=retry_then_limit)
        self.seed(runner)
        with contextlib.redirect_stdout(io.StringIO()):
            summary = runner.run()
        results = [json.loads(line) for line in (self.root/'logs/results.jsonl').read_text().splitlines()]
        first = next(row for row in results if row['row']==1)
        self.assertEqual(first['status'],'stopped')
        self.assertEqual(first['stage'],'backoff')
        self.assertEqual(first['attempts'],1)
        self.assertEqual(first['history'][0]['apple_ec'],-20970)
        self.assertEqual(summary['results']['not_started'],3)

    def test_timed_out_process_is_reaped_before_retry(self):
        runner = self.runner(count=1, auth_target=hang, attempts=2)
        runner.job_budget = .7
        self.seed(runner)
        with contextlib.redirect_stdout(io.StringIO()):
            summary = runner.run()
        self.assertEqual(summary['results']['error'], 1)
        events = self.events()
        stopped = [x for x in events if x['event']=='child_stopped' and 'row' in x]
        self.assertEqual(len(stopped), 2)
        for event in stopped:
            with self.assertRaises(ProcessLookupError):
                os.kill(event['worker_pid'], 0)
        result = json.loads((self.root/'logs/results.jsonl').read_text())
        self.assertEqual(result['attempts'], 2)

    def test_debug_mode_drains_child_events_and_filters_sensitive_fields(self):
        runner = self.runner(count=1, auth_target=debug_auth, debug=True)
        self.seed(runner)
        with contextlib.redirect_stdout(io.StringIO()):
            summary = runner.run()
        self.assertEqual(summary['results']['need_2fa'], 1)
        content = (self.root/'logs/debug.jsonl').read_text()
        self.assertNotIn('must-not-log', content)
        self.assertNotIn('password-secret', content)
        events = [json.loads(line) for line in content.splitlines()]
        self.assertEqual(sum(row.get('phase')=='synthetic_phase' for row in events), 100)
        stats = json.loads((self.root/'logs/debug-summary.json').read_text())
        self.assertEqual(next(row for row in stats['phases'] if row['phase']=='synthetic_phase')['count'],100)

    def test_account_parser_preserves_password_and_rejects_duplicates(self):
        path = self.root/'accounts.txt'
        path.write_text('own@example.com---- password----suffix \n')
        accounts = read_accounts(path)
        self.assertEqual(accounts[0].password, ' password----suffix ')
        path.write_text('own@example.com----a\nOWN@example.com----b\n')
        with self.assertRaises(ValueError):
            read_accounts(path)

    def test_proxy_requests_exact_ten_and_parser_preserves_proxy_credentials(self):
        url = 'https://api.example.invalid/?password=a%26b&num=200&region=any'
        result = demand_url(url, 10)
        self.assertIn('num=10', result)
        self.assertIn('password=a%26b', result)
        self.assertNotIn('num=200', result)
        self.assertEqual(parse('1.2.3.4:8080:user:pass')[0], 'http://user:pass@1.2.3.4:8080')

    def test_expired_proxy_and_exclusive_lease(self):
        now = [100.]
        pool = ProxyPool('https://api.example.invalid', threading.Event(), lambda *a, **k:None,
                         clock=lambda:now[0])
        lease = ProxyLease('IP1','http://127.0.0.1:12345',160)
        pool.available.append(lease)
        pool.known[lease.url] = 160
        self.assertIs(pool.take(), lease)
        self.assertIsNone(pool.take())
        pool.release(lease)
        now[0] = 161
        self.assertIsNone(pool.take())

    def test_api_fetch_is_isolated_and_hard_timeout_reaps_child(self):
        context = mp.get_context('spawn')
        stop = context.Event()
        fetcher = ApiFetcher(context, stop, target=api_ready)
        values = fetcher('https://api.example.invalid/?password=api-secret', 2)
        self.assertEqual(len(values), 2)
        fetcher = ApiFetcher(context, stop, target=api_hang, deadline=.3)
        started = time.monotonic()
        with self.assertRaises(ProxyFetchError):
            fetcher('https://api.example.invalid', 1)
        self.assertLess(time.monotonic() - started, 3)
        self.assertFalse(any(p.name=='proxy-api-fetch' for p in mp.active_children()))

    def test_http_and_websocket_registration_receive_the_same_explicit_proxy(self):
        store = StateStore('https://ani.example.invalid', directory=self.root/'state')
        proxy = 'http://user:secret@127.0.0.1:12345'
        client = Client('https://ani.example.invalid', store=store, proxy=proxy,
                        client_info={'client_info':'synthetic','user_agent':'synthetic'})
        self.assertEqual(client.session.proxies, {'http':proxy,'https':proxy})
        seen = {}
        messages = iter([
            {'result':'GiveIdentifier'}, {'result':'GiveStartProvisioningData'},
            {'result':'GiveEndProvisioningData','cpim':'synthetic'},
            {'result':'ProvisioningSuccess','adi_pb':base64.b64encode(b'offline').decode()},
        ])
        class WebSocket:
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def recv(self, **kwargs): return json.dumps(next(messages))
            def send(self, value): pass
        def connect(uri, *, proxy=None, **kwargs):
            seen['proxy'], seen['ssl'] = proxy, kwargs['ssl']
            return WebSocket()
        lookup = plistlib.dumps({'urls':{'midStartProvisioning':'https://gsa.apple.com/start',
                                        'midFinishProvisioning':'https://gsa.apple.com/end'}})
        with patch('websockets.sync.client.connect', connect), \
             patch.object(client, '_request', return_value=lookup), \
             patch.object(client, '_plist', side_effect=[{'spim':'synthetic'},{'ptm':'p','tk':'t'}]):
            client._provision(store.load())
        self.assertEqual(seen['proxy'], proxy)
        self.assertTrue(seen['ssl'].check_hostname)
        client.close()

    def test_provider_direct_keeps_apple_requests_on_explicit_proxy(self):
        from unittest.mock import MagicMock
        store = StateStore('https://ani.example.invalid',directory=self.root/'routes')
        proxy = 'http://127.0.0.1:12345'
        client = Client('https://ani.example.invalid',store=store,proxy=proxy,provider_direct=True)
        response = MagicMock()
        response.status_code = 200
        response.iter_content.return_value = [b'{}']
        response.__enter__.return_value = response
        with patch.object(client.session,'request',return_value=response) as request:
            client._request('GET','https://ani.example.invalid/v3/client_info','client_info')
            self.assertEqual(request.call_args.kwargs['proxies'],{'http':'','https':''})
            client._request('GET','https://gsa.apple.com/grandslam/GsService2/lookup','lookup',apple=True)
            self.assertNotIn('proxies',request.call_args.kwargs)
            self.assertEqual(client.session.proxies,{'http':proxy,'https':proxy})
            self.assertIsNot(request.call_args.kwargs['verify'],False)
        client.close()


if __name__ == '__main__':
    unittest.main()
