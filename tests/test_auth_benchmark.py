import contextlib
import io
import json
from pathlib import Path
import socket
import tempfile
import unittest
from unittest.mock import patch

from archive.legacy.auth_benchmark import compare, distribution
from fastcheck.scheduler import Supervisor
from fastcheck.workers import demo_registration
from batch_fixtures import supervisor_retry, supervisor_limited
from archive.legacy.gsa_batch import Account


class BenchmarkTests(unittest.TestCase):
    def test_one_hundred_tasks_per_channel_keep_zero_retries_and_equal_limits(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            accounts = [Account(n,f'owned-{n}@example.invalid',f'secret-{n}') for n in range(1,101)]
            seen = []
            serial = 0
            def fetch(url,count):
                nonlocal serial
                values=[]
                for _ in range(count):
                    serial+=1
                    values.append(f'http://127.0.0.1:{10000+serial}')
                return values
            def factory(*args,**kwargs):
                settings=args[4]
                seen.append((settings.protocol,settings.processes*settings.threads,settings.max_attempts))
                self.assertEqual(settings.warmup_devices,4)
                return Supervisor(*args,**kwargs,fetcher=fetch,handler=supervisor_retry,
                                  registration=demo_registration)
            with contextlib.redirect_stdout(io.StringIO()), \
                 patch.object(socket.socket,'connect',side_effect=AssertionError('No network')):
                result=compare(accounts,'https://api.example.invalid/?num=200&password=api-secret',
                    root,root/'comparison',processes=2,threads=2,supervisor_factory=factory)
            self.assertEqual(seen,[('web',4,1),('gsa',4,1)])
            for protocol in ('web','gsa'):
                report=result['protocols'][protocol]
                self.assertEqual(report['result_rows'],100)
                self.assertEqual(report['max_attempts_observed'],1)
                self.assertIsNone(report['summary']['stop_reason'])
                self.assertLessEqual(report['summary']['max_active'],4)
                events=[json.loads(v) for v in (root/'comparison'/protocol/'events.jsonl').read_text().splitlines()]
                self.assertEqual(len([v for v in events if v['event']=='task_dispatch']),100)
                self.assertFalse(any(v['event']=='task_retry' for v in events))
            self.assertTrue(all(a.password=='' for a in accounts))
            self.assertNotIn('api-secret',(root/'comparison/comparison.json').read_text())

    def test_rate_limit_does_not_switch_to_other_channel_to_continue(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            accounts=[Account(n,f'owned-{n}@example.invalid','secret') for n in range(1,6)]
            called=[]
            def factory(*args,**kwargs):
                called.append(args[4].protocol)
                return Supervisor(*args,**kwargs,fetcher=lambda url,count:[f'http://127.0.0.1:{10000+n}' for n in range(count)],
                                  handler=supervisor_limited,registration=demo_registration)
            with contextlib.redirect_stdout(io.StringIO()):
                result=compare(accounts,'https://api.example.invalid/?num=200',root,root/'comparison',
                               processes=1,threads=1,supervisor_factory=factory)
            self.assertEqual(called,['web'])
            self.assertEqual(result['stop_reason'],'authentication_ratelimit')
            self.assertTrue(all(a.password=='' for a in accounts))

    def test_p95_uses_nearest_rank_and_empty_samples_remain_unknown(self):
        self.assertEqual(distribution([]),{'samples':0})
        result=distribution(list(range(1,101)))
        self.assertEqual(result['p50_ms'],50.5)
        self.assertEqual(result['p95_ms'],95)


if __name__=='__main__':
    unittest.main()
