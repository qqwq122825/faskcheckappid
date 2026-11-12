import contextlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from fastcheck.workers import safe_result
from archive.legacy.gsa_batch import EventLog
from fastcheck.status import StatusPanel,result_bucket,snapshot


class StatusTests(unittest.TestCase):
    def test_counts_partition_completed_accounts_without_guessing_frozen_or_password(self):
        samples=[{'status':'valid'},{'status':'need_2fa'},{'status':'wrong_password'},
                 {'status':'security_lock'},{'status':'locked'},
                 {'status':'error','kind':'timeout'},
                 {'status':'init_refused','apple_ec':-20970},
                 {'status':'credentials_rejected','apple_ec':-20283},
                 {'status':'error','kind':'server','http':503},
                 {'status':'unknown','kind':'interrupted'},
                 {'status':'not_started','attempts':0}]
        rows={n:dict(row,attempts=row.get('attempts',1)) for n,row in enumerate(samples)}
        counts=snapshot(12,rows,active=1)
        self.assertEqual({k:counts[k] for k in ('processed','remaining','active')},
                         {'processed':10,'remaining':2,'active':1})
        self.assertEqual([counts[k] for k in ('valid','wrong_password','need_2fa','locked','timeout','undetermined')],
                         [1,1,1,2,1,4])
        self.assertEqual(result_bucket({'status':'not_active'}),'undetermined')
        self.assertEqual(result_bucket({'status':'needs_setup','password_verified':True}),'valid')
        self.assertEqual(result_bucket({'status':'needs_setup'}),'undetermined')

    def test_updates_once_per_second_and_forces_final_snapshot(self):
        stream=io.StringIO()
        clock=[0]
        panel=StatusPanel(stream=stream,clock=lambda:clock[0])
        counts=snapshot(20,{})
        self.assertTrue(panel.update(counts,0,'准备'))
        clock[0]=.5
        self.assertFalse(panel.update(counts,.5,'运行'))
        clock[0]=1
        self.assertTrue(panel.update(counts,1,'运行'))
        self.assertTrue(panel.update(counts,1,'已完成',final=True))
        self.assertEqual(len(stream.getvalue().splitlines()),3)
        self.assertNotIn('\x1b',stream.getvalue())

    def test_narrow_tty_wraps_and_replaces_panel_without_losing_columns(self):
        class TTY(io.StringIO):
            def isatty(self): return True
        stream=TTY()
        panel=StatusPanel(stream=stream)
        with patch('fastcheck.status.shutil.get_terminal_size',return_value=os.terminal_size((60,24))):
            panel.update(snapshot(82,{}),0,'准备')
            panel.update(snapshot(82,{}),1,'已停止',final=True)
        for label in ('总数','已运行','剩余','密正','错误','双重','锁定','超时','未判断'):
            self.assertIn(label,stream.getvalue())
        self.assertIn('\x1b[1A',stream.getvalue())
        self.assertEqual(panel.previous_rows,0)

    def test_silent_console_still_writes_events_and_debug_file(self):
        with tempfile.TemporaryDirectory() as directory:
            log=EventLog(directory,debug=True,console=False)
            stream=io.StringIO()
            with contextlib.redirect_stdout(stream):
                log('example',row=1)
            self.assertEqual(stream.getvalue(),'')
            self.assertEqual(json.loads((Path(directory)/'events.jsonl').read_text())['row'],1)
            self.assertTrue((Path(directory)/'debug.jsonl').exists())

    def test_timeout_is_separate_from_server_and_other_network_failures(self):
        value=safe_result({'status':'error','kind':'network','detail':'init ReadTimeout'})
        self.assertEqual(value['kind'],'timeout')
        self.assertNotIn('detail',value)
        self.assertEqual(safe_result({'status':'error','kind':'server','detail':'HTTP 503'})['kind'],'server')
        self.assertEqual(safe_result({'status':'error','kind':'network','detail':'ConnectionError'})['kind'],'network')


if __name__=='__main__':
    unittest.main()
