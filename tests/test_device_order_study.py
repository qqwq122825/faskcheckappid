import contextlib
import io
import json
from pathlib import Path
import socket
import tempfile
import time
import unittest
from unittest.mock import patch

from archive.legacy.gsa_batch import Account
from archive.legacy.gsa_device_order_study import StudyRunner,retry_eligible
from batch_fixtures import study_order_unit,study_limited_unit


class OrderStudyTests(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory()
        self.root=Path(self.directory.name)
        self.serial=0
        self.fetch_times=[]
        self.network=patch.object(socket.socket,'connect',side_effect=AssertionError('Network forbidden'))
        self.network.start()

    def tearDown(self):
        self.network.stop()
        self.directory.cleanup()

    def fetch(self,url,count):
        self.fetch_times.append(time.time())
        self.serial+=1
        return [f'http://proxy-user:proxy-secret@127.0.0.1:{22000+self.serial}']

    def runner(self,**kwargs):
        accounts=[Account(n,f'owned{n}@example.invalid','password-secret') for n in range(1,21)]
        return StudyRunner(accounts,'https://api.example.invalid/?password=api-secret',
            self.root/'logs',self.root/'devices',worker=kwargs.pop('worker',study_order_unit),
            fetcher=self.fetch,prepare_info=False,**kwargs)

    def test_twenty_accounts_pair_order_and_only_failed_account_gets_fresh_device_retry(self):
        runner=self.runner()
        with contextlib.redirect_stdout(io.StringIO()):summary=runner.run()
        self.assertEqual(summary['accounts_observed'],20)
        self.assertEqual(summary['groups']['paired_first_pass_position_1']['records'],10)
        self.assertEqual(summary['groups']['paired_first_pass_position_2']['records'],10)
        self.assertEqual(summary['groups']['fresh_device_retry_position_1']['records'],1)
        attempts=[r for r in summary['results'] if r['row']==1]
        self.assertEqual(len(attempts),2)
        self.assertEqual(attempts[0]['device'],1)
        self.assertEqual(attempts[1]['device'],11)
        self.assertEqual(attempts[1]['position'],1)
        self.assertEqual(attempts[1]['status'],'need_2fa')
        events=[json.loads(line) for line in (self.root/'logs/events.jsonl').read_text().splitlines()]
        self.assertLessEqual(max(r.get('active_units',0) for r in events),2)
        for path in (self.root/'logs').iterdir():
            for secret in ('password-secret','proxy-secret','api-secret','owned1@'):
                self.assertNotIn(secret,path.read_text())

    def test_rate_limit_stops_experiment_without_fresh_retry(self):
        runner=self.runner(worker=study_limited_unit)
        with contextlib.redirect_stdout(io.StringIO()):summary=runner.run()
        self.assertEqual(summary['stop_reason'],'ratelimit')
        self.assertFalse(any(r['study_phase']=='fresh_device_retry' for r in summary['results']))
        self.assertTrue(any(r['not_started'] for r in summary['account_markers']))
        self.assertFalse(runner.active)

    def test_wait_is_respected_before_proxy_fetch(self):
        target=time.time()+.15
        runner=self.runner(wait_until=target)
        with contextlib.redirect_stdout(io.StringIO()):runner.run()
        self.assertGreaterEqual(min(self.fetch_times),target)

    def test_definitive_account_outcomes_are_not_retried(self):
        for status in ('wrong_password','security_lock','locked','not_active','need_2fa','valid'):
            self.assertFalse(retry_eligible({'status':status,'stage':'complete'}))
        self.assertFalse(retry_eligible({'status':'error','stage':'complete','apple_ec':-20970}))
        self.assertTrue(retry_eligible({'status':'error','stage':'init','apple_ec':-20970}))


if __name__=='__main__':unittest.main()
