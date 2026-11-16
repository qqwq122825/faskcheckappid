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
        samples=[{'status':'valid'},{'status':'need_2fa'},
                 {'status':'wrong_password','apple_message':'Enter the correct password for this Apple Account.'},
                 {'status':'security_lock','apple_message':'This Apple Account has been locked for security reasons.'},
                 {'status':'locked'},
                 {'status':'error','kind':'timeout'},
                 {'status':'init_refused','apple_ec':-20970,
                  'apple_message':'This Apple Account is not active.'},
                 {'status':'credentials_rejected','apple_ec':-20283},
                 {'status':'error','kind':'server','http':503},
                 {'status':'unknown','kind':'interrupted'},
                 {'status':'not_started','attempts':0}]
        rows={n:dict(row,attempts=row.get('attempts',1)) for n,row in enumerate(samples)}
        counts=snapshot(12,rows,active=1)
        self.assertEqual({k:counts[k] for k in ('processed','remaining','active')},
                         {'processed':10,'remaining':2,'active':1})
        self.assertEqual([counts[k] for k in ('valid','wrong_password','need_2fa','locked','timeout','invalid','not_found','undetermined')],
                         [1,1,1,1,1,1,0,4])
        self.assertEqual(result_bucket({'status':'init_refused','stage':'init','apple_ec':-20970}), 'undetermined')
        self.assertEqual(result_bucket({'status':'error','stage':'init','apple_ec':'-20209'}), 'undetermined')
        self.assertEqual(result_bucket({'status':'error','stage':'init','http':200,
                                        'apple_ec':'-22421'}), 'timeout')
        self.assertEqual(result_bucket({'status':'error','stage':'init','http':200,
                                        'apple_ec':'-332000'}), 'timeout')
        self.assertEqual(result_bucket({'status':'error','stage':'init','http':200,
                                        'apple_ec':'-33200'}), 'timeout')
        self.assertEqual(result_bucket({'status':'not_active'}),'undetermined')
        self.assertEqual(result_bucket({'status':'needs_setup','password_verified':True}),'valid')
        self.assertEqual(result_bucket({'status':'needs_setup'}),'undetermined')

    def test_english_apple_reasons_override_shared_error_codes(self):
        shared = {'status': 'error', 'stage': 'init', 'apple_ec': -20970}
        samples = {
            'Unable to sign you in to your Apple Account. Try again later.': 'timeout',
            'This request could not be completed because of a server error.': 'timeout',
            'This Action Cannot Be Completed': 'timeout',
            'The request cannot be processed at this time. Please try again later.': 'timeout',
            'We cannot process your request, please try again later.': 'timeout',
            'This Apple Account has been\u00a0locked\u00a0for security reasons. Visit iForgot to reset your account (https://iforgot.apple.com).': 'locked',
            'Enter the correct password for this Apple Account.': 'wrong_password',
            'This Apple Account is not active.': 'invalid',
            'Your account information was entered incorrectly.': 'not_found',
            'This Apple Account has been lockd for security reasons. Visit iForgot to reset your account.': 'locked',
            'This Apple Account has been Locked. Contact your Administrator or Manager to reset your password and unlock your account.': 'locked',
        }
        for reason, expected in samples.items():
            with self.subTest(reason=reason):
                self.assertEqual(result_bucket(dict(shared, apple_message=reason)), expected)
        self.assertEqual(result_bucket(dict(
            shared, apple_message='This Apple Account is ready for use.')), 'undetermined')

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
        for label in ('总数','已运行','剩余','密码正确','错误','双重','锁定','请重试','未激活','不存在','其他错误'):
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

    def test_explicit_http_zero_is_timeout_but_missing_http_is_not(self):
        self.assertEqual(result_bucket({'status':'undetermined','kind':'network','http':0}), 'timeout')
        self.assertEqual(result_bucket({'status':'undetermined','kind':'network'}), 'undetermined')

    def test_safe_result_keeps_raw_apple_reason_and_two_factor_auth_type(self):
        value = safe_result({
            'status': 'need_2fa',
            'apple_message': 'Unable to sign you in to your Apple Account. Try again later.',
            'auth_type': 'trustedDeviceSecondaryAuth',
        })
        self.assertEqual(value['apple_message'],
                         'Unable to sign you in to your Apple Account. Try again later.')
        self.assertEqual(value['auth_type'], 'trustedDeviceSecondaryAuth')


if __name__=='__main__':
    unittest.main()
