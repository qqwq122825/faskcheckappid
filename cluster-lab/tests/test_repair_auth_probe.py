import json
import hashlib
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from fastcheck.protocols import idmsa
from clusterlab.controller import Controller, result_reason_cn
from clusterlab.executor import (
    _execute_auth_unit,
    _refine_repair_auth,
    _refine_repair_auth_with_rotation,
)


class FakePool:
    def __init__(self, root, *_args, **_kwargs):
        self.root = Path(root)

    def slot_directory(self, slot):
        return self.root / 'fake-devices' / f'{int(slot):03d}'

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

    def refine(self, checker, result=None, retries=0):
        return _refine_repair_auth(
            result or dict(self.base), 'owned@example.invalid', 'secret', None,
            20, threading.Event().is_set, retries=retries, checker=checker)

    def test_hsa2_challenge_is_verification_code(self):
        value = self.refine(self.checker('hsa2'))
        self.assertEqual(value['two_factor_method'], 'verification_code')
        self.assertEqual(value['web_auth_type'], 'hsa2')
        self.assertEqual(value['web_http'], 409)
        self.assertEqual(result_reason_cn(value, 'need_2fa'),
                         'Web 二次校验后：需要手机或 App 验证码')

    def test_sa_and_non_sa_move_to_password_correct(self):
        for auth_type in ('sa', 'non-sa'):
            with self.subTest(auth_type=auth_type):
                value = self.refine(self.checker(auth_type, status='valid', http=200))
                self.assertEqual(value['status'], 'valid')
                self.assertEqual(value['two_factor_method'], 'security_repair')

    def test_dnu_stays_in_two_factor_bucket(self):
        value = self.refine(self.checker('dnu', status='error', http=409))
        self.assertEqual(value['status'], 'need_2fa')
        self.assertEqual(value['two_factor_method'], 'security_repair')

    def test_web_transport_retries_follow_task_setting(self):
        seen = {}
        def failing(_email, _password, _proxy, **kwargs):
            seen.update(kwargs)
            kwargs['trace']({
                'stage': 'init', 'http': 503, 'auth_type': None, 'attempt': 4,
                'server': 'Apple', 'via': 'edge-gateway', 'retry_after': '5',
                'content_type': 'application/json', 'response_body_bytes': 23,
                'response_body_sha256': 'a' * 64,
                'response_keys': ['serviceErrors'],
                'service_error_codes': ['-1'],
                'has_session_headers': False, 'has_set_cookie': True,
            })
            return {'status': 'error', 'kind': 'server', 'stage': 'init',
                    'detail': 'HTTP 503（苹果服务暂不可用）'}
        value = self.refine(failing, retries=3)
        self.assertEqual(seen['retries'], 3)
        self.assertEqual(value['status'], 'need_2fa')
        self.assertEqual(value['web_stage'], 'init')
        self.assertEqual(value['web_http'], 503)
        self.assertEqual(value['web_kind'], 'server')
        self.assertEqual(value['web_detail'], 'HTTP 503（苹果服务暂不可用）')
        self.assertEqual(value['web_attempts'], 4)
        self.assertEqual(value['web_server'], 'Apple')
        self.assertEqual(value['web_via'], 'edge-gateway')
        self.assertEqual(value['web_retry_after'], '5')
        self.assertEqual(value['web_response_sha256'], 'a' * 64)
        self.assertEqual(value['web_response_keys'], ['serviceErrors'])
        self.assertEqual(value['web_service_error_codes'], ['-1'])
        self.assertTrue(value['web_has_set_cookie'])

    def test_web_transient_retry_rotates_proxy_without_repeating_gsa(self):
        seen = []

        def checker(_email, _password, proxy, **kwargs):
            seen.append(proxy)
            if proxy == 'proxy-a':
                kwargs['trace']({'stage': 'init', 'http': 503, 'attempt': 1})
                return {'status': 'error', 'kind': 'server', 'stage': 'init'}
            kwargs['trace']({'stage': 'complete', 'http': 412,
                             'auth_type': 'sa', 'attempt': 1})
            return {'status': 'valid', 'stage': 'complete'}

        rotations = []

        def rotate(attempt):
            rotations.append(attempt)
            return 'proxy-b', True

        value = _refine_repair_auth_with_rotation(
            dict(self.base), 'owned@example.invalid', 'secret', 'proxy-a',
            20, threading.Event().is_set, 2, rotate, checker=checker)
        self.assertEqual(seen, ['proxy-a', 'proxy-b'])
        self.assertEqual(rotations, [1])
        self.assertEqual(value['status'], 'valid')
        self.assertEqual(value['web_attempts'], 2)
        self.assertEqual(value['web_proxy_rotations'], 1)
        self.assertNotIn('web_rotation_exhausted', value)

    def test_idmsa_trace_keeps_only_safe_response_metadata(self):
        events = []
        raw = b'{"serviceErrors":[{"code":"-1"}]}'
        idmsa._trace(
            events.append, 'init', 503,
            {'serviceErrors': [{'code': '-1'}]},
            {'Server': 'Apple', 'Via': 'edge-gateway', 'Retry-After': '5',
             'Content-Type': 'application/json', 'Set-Cookie': 'secret-cookie',
             'scnt': 'secret-session'}, raw)
        event = events[0]
        self.assertEqual(event['server'], 'Apple')
        self.assertEqual(event['via'], 'edge-gateway')
        self.assertEqual(event['retry_after'], '5')
        self.assertEqual(event['response_body_bytes'], len(raw))
        self.assertEqual(event['response_body_sha256'], hashlib.sha256(raw).hexdigest())
        self.assertEqual(event['response_keys'], ['serviceErrors'])
        self.assertEqual(event['service_error_codes'], ['-1'])
        self.assertTrue(event['has_session_headers'])
        self.assertTrue(event['has_set_cookie'])
        serialized = json.dumps(event)
        self.assertNotIn('secret-cookie', serialized)
        self.assertNotIn('secret-session', serialized)
        self.assertNotIn(raw.decode(), serialized)

    def test_idmsa_http_zero_and_503_consume_retry_budget(self):
        outcomes = [
            {'status': 'error', 'kind': 'network', 'stage': 'init',
             'detail': 'init ConnectionError'},
            {'status': 'error', 'kind': 'server', 'stage': 'init',
             'detail': 'HTTP 503'},
            {'status': 'need_2fa', 'kind': '', 'stage': 'complete'},
        ]
        with patch.object(idmsa, '_once', side_effect=outcomes) as request:
            value = idmsa.check('owned@example.invalid', 'secret', None, retries=2)
        self.assertEqual(request.call_count, 3)
        self.assertEqual(value['status'], 'need_2fa')

    def test_http_412_setup_is_not_claimed_as_code_challenge(self):
        value = self.refine(self.checker('hsa2', status='needs_setup', http=412))
        self.assertEqual(value['two_factor_method'], 'security_repair')

    def test_probe_failure_keeps_gsa_result(self):
        def failing(*_args, **_kwargs):
            raise RuntimeError('proxy://user:secret@example.invalid')
        value = self.refine(failing)
        self.assertEqual(value['status'], 'need_2fa')
        self.assertEqual(value['auth_type'], 'repair')
        self.assertEqual(value['two_factor_method'], 'repair_unspecified')
        self.assertEqual(value['web_kind'], 'RuntimeError')
        self.assertEqual(value['web_detail'], 'RuntimeError')
        self.assertNotIn('secret', json.dumps(value))

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
                'origin': 'http://127.0.0.1:1', 'timeout': 5, 'retries': 2,
                '_web_secondary_auth': True,
            }

            def web_check(_email, _password, _proxy, **kwargs):
                self.assertEqual(kwargs['retries'], 2)
                kwargs['trace']({'stage': 'complete', 'http': 409,
                                 'auth_type': 'hsa2', 'attempt': 1,
                                 'server': 'Apple', 'via': '', 'retry_after': '',
                                 'content_type': 'application/json',
                                 'response_body_bytes': 18,
                                 'response_body_sha256': 'b' * 64,
                                 'response_keys': ['authType'],
                                 'service_error_codes': [],
                                 'has_session_headers': True,
                                 'has_set_cookie': True})
                return {'status': 'need_2fa', 'stage': 'complete'}

            with patch('clusterlab.executor._load_fastcheck', return_value=(
                    FakePool,
                    lambda *_args, **_kwargs: {
                        'status': 'need_2fa', 'auth_type': 'repair',
                        'detail': 'au=repair', 'http': 200,
                    },
                    lambda result: result['status'])), \
                 patch('fastcheck.protocols.idmsa.check', side_effect=web_check), \
                 patch.dict('sys.modules', {'fastcheck.workers': SimpleNamespace(
                     device_profile=lambda *_args, **_kwargs: {
                         'client_info': 'test', 'user_agent': 'test'})}), \
                 patch('clusterlab.executor._bump_usage'):
                _execute_auth_unit(root, descriptor, threading.Event())

            record = json.loads((root / 'results.jsonl').read_text('utf-8'))
            self.assertEqual(record['status'], 'need_2fa')
            self.assertEqual(record['code'], 'au=repair')
            self.assertEqual(record['two_factor_method'], 'verification_code')
            self.assertEqual(record['web_auth_type'], 'hsa2')
            self.assertEqual(record['web_http'], 409)
            self.assertEqual(record['web_stage'], 'complete')
            self.assertEqual(record['web_attempts'], 1)
            self.assertEqual(record['web_server'], 'Apple')
            self.assertEqual(record['web_response_sha256'], 'b' * 64)
            self.assertEqual(record['web_response_keys'], ['authType'])
            self.assertTrue(record['web_has_session_headers'])
            self.assertTrue(record['web_has_set_cookie'])
            self.assertNotIn('web_kind', record)
            self.assertNotIn('web_detail', record)

    def test_executor_skips_web_probe_when_setting_is_off(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'input.part').write_text(
                'owned@example.invalid----secret\n', encoding='utf-8')
            descriptor = {
                'rows': 1, 'first_row': 1, 'threads': 1,
                'device_capacity': 1, 'device_root': str(root / 'devices'),
                'origin': 'http://127.0.0.1:1', 'timeout': 5, 'retries': 2,
                '_web_secondary_auth': False,
            }
            with patch('clusterlab.executor._load_fastcheck', return_value=(
                    FakePool,
                    lambda *_args, **_kwargs: {
                        'status': 'need_2fa', 'auth_type': 'repair',
                        'detail': 'au=repair', 'http': 200,
                    },
                    lambda result: result['status'])), \
                 patch('fastcheck.protocols.idmsa.check',
                       side_effect=AssertionError('Web probe must stay off')), \
                 patch.dict('sys.modules', {'fastcheck.workers': SimpleNamespace(
                     device_profile=lambda *_args, **_kwargs: {
                         'client_info': 'test', 'user_agent': 'test'})}), \
                 patch('clusterlab.executor._bump_usage'):
                _execute_auth_unit(root, descriptor, threading.Event())
            record = json.loads((root / 'results.jsonl').read_text('utf-8'))
            self.assertEqual(record['status'], 'need_2fa')
            self.assertNotIn('web_status', record)

    def test_executor_discards_failed_web_exit_and_acquires_a_new_one(self):
        class ProxyPool:
            def __init__(self):
                self.leases = [
                    ('proxy-bootstrap', 1, 0),
                    ('proxy-bad', 2, 0),
                    ('proxy-good', 3, 0),
                ]
                self.discards = []
                self.releases = []

            def acquire(self, wait=1.0):
                del wait
                return self.leases.pop(0) if self.leases else None

            def discard(self, lease):
                self.discards.append(lease[0])

            def release(self, lease, healthy):
                self.releases.append((lease[0], healthy))

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'input.part').write_text(
                'owned@example.invalid----secret\n', encoding='utf-8')
            descriptor = {
                'rows': 1, 'first_row': 1, 'threads': 1,
                'device_capacity': 1, 'device_root': str(root / 'devices'),
                'origin': 'http://127.0.0.1:1', 'timeout': 5, 'retries': 2,
                'proxy_api': 'http://proxy.invalid/list',
                '_web_secondary_auth': True,
            }
            pool = ProxyPool()
            gsa_proxies = []
            web_proxies = []

            def gsa_check(_email, _password, proxy, **_kwargs):
                gsa_proxies.append(proxy)
                return {'status': 'need_2fa', 'auth_type': 'repair',
                        'detail': 'au=repair', 'http': 200}

            def web_check(_email, _password, proxy, **kwargs):
                web_proxies.append(proxy)
                if proxy == 'proxy-bad':
                    kwargs['trace']({'stage': 'init', 'http': 503, 'attempt': 1})
                    return {'status': 'error', 'kind': 'server', 'stage': 'init'}
                kwargs['trace']({'stage': 'complete', 'http': 412,
                                 'auth_type': 'sa', 'attempt': 1})
                return {'status': 'valid', 'stage': 'complete'}

            with patch('clusterlab.executor._load_fastcheck', return_value=(
                    FakePool, gsa_check, lambda result: result['status'])), \
                 patch('fastcheck.protocols.idmsa.check', side_effect=web_check), \
                 patch.dict('sys.modules', {'fastcheck.workers': SimpleNamespace(
                     device_profile=lambda *_args, **_kwargs: {
                         'client_info': 'test', 'user_agent': 'test'})}), \
                 patch('clusterlab.executor._bump_usage'):
                _execute_auth_unit(root, descriptor, threading.Event(), proxy_pool=pool)

            record = json.loads((root / 'results.jsonl').read_text('utf-8'))
            self.assertEqual(gsa_proxies, ['proxy-bad'])
            self.assertEqual(web_proxies, ['proxy-bad', 'proxy-good'])
            self.assertEqual(pool.discards, ['proxy-bad'])
            self.assertIn(('proxy-good', True), pool.releases)
            self.assertEqual(record['status'], 'valid')
            self.assertEqual(record['attempts'], 1)
            self.assertEqual(record['web_attempts'], 2)
            self.assertEqual(record['web_proxy_rotations'], 1)

    def test_controller_exposes_chinese_method_with_debug_off(self):
        record = Controller._decorate_result_device(
            {'status': 'need_2fa', 'code': 'au=repair', 'device': 3,
             'two_factor_method': 'verification_code'},
            {'node_id': 'node-a'},
            ({'node-a': 1}, {('node-a', 3): 'B64:test'}, False),
        )
        self.assertEqual(record['two_factor_method_cn'], '手机或 App 验证码')
        self.assertEqual(record['reason_cn'], '密码已通过，需要手机或 App 验证码')

    def test_controller_reason_names_web_secondary_result(self):
        success = Controller._decorate_result_device(
            {'status': 'valid', 'code': 'au=repair', 'device': 5,
             'web_status': 'valid', 'web_stage': 'complete',
             'web_http': 412, 'web_auth_type': 'sa',
             'web_detail': '密码验证通过（authType=sa）；隐私或安全设置待确认（HTTP 412）'},
            {'node_id': 'node-a'},
            ({'node-a': 1}, {('node-a', 5): 'B64:test'}, False),
        )
        self.assertEqual(
            success['reason_cn'],
            'Web 二次校验后：密码验证通过（authType=sa）；隐私或安全设置待确认（HTTP 412）')
        failure = Controller._decorate_result_device(
            {'status': 'need_2fa', 'code': 'au=repair', 'device': 5,
             'web_status': 'error', 'web_stage': 'init', 'web_http': 503,
             'web_kind': 'server', 'web_detail': 'HTTP 503（苹果服务暂不可用）'},
            {'node_id': 'node-a'},
            ({'node-a': 1}, {('node-a', 5): 'B64:test'}, False),
        )
        self.assertEqual(
            failure['reason_cn'],
            'Web 二次校验失败：HTTP 503（苹果服务暂不可用）；保留 GSA 结果')

    def test_web_reason_wins_over_gsa_repair_in_debug_mode(self):
        record = Controller._decorate_result_device(
            {'status': 'need_2fa', 'code': 'au=repair', 'reason': 'au=repair',
             'device': 8, 'web_status': 'error', 'web_stage': 'complete',
             'web_http': 503, 'web_kind': 'server',
             'web_detail': 'HTTP 503（苹果服务或网络链路暂不可用，无法判断账号密码）'},
            {'node_id': 'node-a'},
            ({'node-a': 1}, {('node-a', 8): 'B64:test'}, True),
        )
        self.assertEqual(
            record['reason_cn'],
            'Web 二次校验失败：HTTP 503（苹果服务或网络链路暂不可用，无法判断账号密码）；保留 GSA 结果')


    def test_repair_auth_web_502_rotates_and_retries(self):
        """A Web-only 502 consumes retry budget and switches proxy exits."""
        from clusterlab.executor import _refine_repair_auth_with_rotation

        proxies = []
        rotations = []

        def checker(email, password, proxy, timeout, retries, cancel, trace):
            proxies.append(proxy)
            http = 502 if len(proxies) == 1 else 409
            trace({'stage': 'complete', 'http': http, 'attempt': 1,
                   'auth_type': '' if http == 502 else 'hsa2', 'server': 'Apple'})
            if http == 502:
                return {'status': 'error', 'stage': 'complete', 'kind': 'protocol',
                        'detail': 'HTTP 502'}
            return {'status': 'need_2fa', 'stage': 'complete'}

        def rotate(number):
            rotations.append(number)
            return 'proxy-b', True

        result = _refine_repair_auth_with_rotation(
            {'status': 'need_2fa', 'auth_type': 'repair', 'password_verified': True},
            'account@example.com', 'secret', 'proxy-a', 20, None, 2, rotate, checker)

        self.assertEqual(proxies, ['proxy-a', 'proxy-b'])
        self.assertEqual(rotations, [1])
        self.assertEqual(result['web_http'], 409)
        self.assertEqual(result['web_attempts'], 2)
        self.assertEqual(result['web_proxy_rotations'], 1)


if __name__ == '__main__':
    unittest.main()
