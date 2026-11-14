import unittest

from fastcheck.protocols.dual_auth import recheck_gsa_20970


PRIMARY = {'status': 'error', 'kind': 'protocol', 'stage': 'init',
           'http': 200, 'apple_ec': -20970, 'ms': 600}


class DualProtocolTests(unittest.TestCase):
    def checker(self, result, *, complete_http, apple_ec=None):
        def check(_email, _password, _proxy, **kwargs):
            trace = kwargs['trace']
            trace({'stage': 'init', 'http': 200, 'service_error_codes': []})
            codes = [] if apple_ec is None else [str(apple_ec)]
            trace({'stage': 'complete', 'http': complete_http,
                   'service_error_codes': codes})
            return dict(result)
        return check

    def test_disabled_task_keeps_original_gsa_result(self):
        result = recheck_gsa_20970('a', 'b', None, PRIMARY, enabled=False,
                                   timeout=20, checker=lambda *_a, **_k: self.fail('called'))
        self.assertIs(result, PRIMARY)

    def test_web_20209_becomes_security_lock(self):
        result = recheck_gsa_20970(
            'a', 'b', None, PRIMARY, enabled=True, timeout=20,
            checker=self.checker({'status': 'security_lock', 'stage': 'complete',
                                  'kind': '', 'detail': 'locked', 'ms': 800},
                                 complete_http=403, apple_ec=-20209))
        self.assertEqual(result['status'], 'security_lock')
        self.assertEqual(result['apple_ec'], -20209)
        self.assertEqual(result['http'], 403)
        self.assertEqual(result['gsa_apple_ec'], -20970)
        self.assertEqual(result['verification_protocol'], 'web')
        self.assertEqual(result['ms'], 1400)

    def test_web_503_is_retryable_server_error_not_account_verdict(self):
        result = recheck_gsa_20970(
            'a', 'b', None, PRIMARY, enabled=True, timeout=20,
            checker=self.checker({'status': 'error', 'stage': 'complete',
                                  'kind': 'server', 'detail': 'HTTP 503', 'ms': 900},
                                 complete_http=503))
        self.assertEqual((result['status'], result['kind'], result['http']),
                         ('error', 'server', 503))
        self.assertNotIn('apple_ec', result)
        self.assertEqual(result['gsa_apple_ec'], -20970)

    def test_web_password_acceptance_is_marked_verified(self):
        result = recheck_gsa_20970(
            'a', 'b', None, PRIMARY, enabled=True, timeout=20,
            checker=self.checker({'status': 'need_2fa', 'stage': 'complete',
                                  'kind': '', 'detail': '', 'ms': 700},
                                 complete_http=409))
        self.assertTrue(result['password_verified'])


if __name__ == '__main__':
    unittest.main()
