import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch

from clusterlab.controller import Controller, result_reason_cn
from clusterlab.executor import _execute_auth_unit, _refine_repair_auth


class FakePool:
    def __init__(self, *_args, **_kwargs):
        pass

    def info(self, *_args, **_kwargs):
        return {}

    def inspect(self, _slot):
        return {'state': 'ready'}

    def fetch_headers(self, *_args, **_kwargs):
        return {}


class RepairAuthProbeTests(unittest.TestCase):
    base = {'status': 'need_2fa', 'auth_type': 'repair', 'detail': 'au=repair'}

    @staticmethod
    def checker(auth_type, status='need_2fa', http=409):
        def run(_email, _password, _proxy, **kwargs):
            kwargs['trace']({'stage': 'complete', 'http': http,
                             'auth_type': auth_type})
            return {'status': status, 'stage': 'complete'}
        return run

    def refine(self, checker, result=None):
        return _refine_repair_auth(
            result or dict(self.base), 'owned@example.invalid', 'secret', None,
            20, threading.Event().is_set, checker=checker)

    def test_hsa2_challenge_is_verification_code(self):
        value = self.refine(self.checker('hsa2'))
        self.assertEqual(value['two_factor_method'], 'verification_code')
        self.assertEqual(value['web_auth_type'], 'hsa2')
        self.assertEqual(value['web_http'], 409)
        self.assertEqual(result_reason_cn(value, 'need_2fa'),
                         '密码已通过，需要手机或 App 验证码')

    def test_security_auth_types_are_repair(self):
        for auth_type in ('sa', 'non-sa', 'dnu'):
            with self.subTest(auth_type=auth_type):
                value = self.refine(self.checker(auth_type, status='valid', http=200))
                self.assertEqual(value['two_factor_method'], 'security_repair')

    def test_http_412_setup_is_not_claimed_as_code_challenge(self):
        value = self.refine(self.checker('hsa2', status='needs_setup', http=412))
        self.assertEqual(value['two_factor_method'], 'security_repair')

    def test_probe_failure_keeps_gsa_result(self):
        def failing(*_args, **_kwargs):
            raise RuntimeError('offline')
        value = self.refine(failing)
        self.assertEqual(value['status'], 'need_2fa')
        self.assertEqual(value['auth_type'], 'repair')
        self.assertEqual(value['two_factor_method'], 'repair_unspecified')

    def test_non_repair_does_not_probe(self):
        def forbidden(*_args, **_kwargs):
            self.fail('supplemental probe must not run')
        source = {'status': 'need_2fa', 'auth_type': 'trustedDeviceSecondaryAuth'}
        self.assertIs(_refine_repair_auth(
            source, 'owned@example.invalid', 'secret', None, 20,
            threading.Event().is_set, checker=forbidden), source)

    def test_executor_persists_supplemental_method_without_changing_gsa_bucket(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'input.part').write_text(
                'owned@example.invalid----secret\n', encoding='utf-8')
            descriptor = {
                'rows': 1, 'first_row': 1, 'threads': 1,
                'device_capacity': 1, 'device_root': str(root / 'devices'),
                'origin': 'http://127.0.0.1:1', 'timeout': 5,
            }

            def web_check(_email, _password, _proxy, **kwargs):
                kwargs['trace']({'stage': 'complete', 'http': 409,
                                 'auth_type': 'hsa2'})
                return {'status': 'need_2fa', 'stage': 'complete'}

            with patch('clusterlab.executor._load_fastcheck', return_value=(
                    FakePool,
                    lambda *_args, **_kwargs: {
                        'status': 'need_2fa', 'auth_type': 'repair',
                        'detail': 'au=repair', 'http': 200,
                    },
                    lambda result: result['status'])), \
                 patch('fastcheck.protocols.idmsa.check', side_effect=web_check), \
                 patch('fastcheck.workers.device_profile', return_value={
                     'client_info': 'test', 'user_agent': 'test'}), \
                 patch('clusterlab.executor._bump_usage'):
                _execute_auth_unit(root, descriptor, threading.Event())

            record = json.loads((root / 'results.jsonl').read_text('utf-8'))
            self.assertEqual(record['status'], 'need_2fa')
            self.assertEqual(record['code'], 'au=repair')
            self.assertEqual(record['two_factor_method'], 'verification_code')
            self.assertEqual(record['web_auth_type'], 'hsa2')
            self.assertEqual(record['web_http'], 409)

    def test_controller_exposes_chinese_method_with_debug_off(self):
        record = Controller._decorate_result_device(
            {'status': 'need_2fa', 'code': 'au=repair', 'device': 3,
             'two_factor_method': 'verification_code'},
            {'node_id': 'node-a'},
            ({'node-a': 1}, {('node-a', 3): 'B64:test'}, False),
        )
        self.assertEqual(record['two_factor_method_cn'], '手机或 App 验证码')
        self.assertEqual(record['reason_cn'], '密码已通过，需要手机或 App 验证码')


if __name__ == '__main__':
    unittest.main()
