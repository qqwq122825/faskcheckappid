from pathlib import Path
import stat
import tempfile
import unittest
from archive.legacy.gsa_batch import Account
from fastcheck.exports import ClassifiedExports
from fastcheck.workers import safe_result


class ExportTests(unittest.TestCase):
    def test_password_accepted_and_invalid_exports_keep_original_format(self):
        with tempfile.TemporaryDirectory() as directory:
            export=ClassifiedExports(Path(directory)/'exports')
            account=Account(1,'owned@example.invalid','private-password')
            export.write(account,{'row':1,'protocol':'gsa','status':'need_2fa','attempts':1})
            export.write(account,{'row':2,'protocol':'gsa','status':'init_refused','stage':'init','apple_ec':-20970,'attempts':1})
            self.assertEqual((export.directory/'accepted_passwords.txt').read_text(),'owned@example.invalid----private-password\n')
            self.assertIn('private-password',(export.directory/'invalid.txt').read_text())
            details=(export.directory/'undetermined-details.jsonl').read_text()
            self.assertIn('-20970',details)
            self.assertNotIn('private-password',details)
            self.assertNotIn('owned@example.invalid',details)
            self.assertEqual(stat.S_IMODE((export.directory/'invalid.txt').stat().st_mode),0o600)

    def test_gsa_http_503_and_timeout_type_survive_redaction(self):
        result=safe_result({'status':'error','stage':'init','kind':'server','detail':'init HTTP 503; Content-Type=text/html'})
        self.assertEqual(result['http'],503)
        self.assertNotIn('detail',result)

    def test_parameter_refresh_read_timeout_is_timeout_without_raw_detail(self):
        result=safe_result({'status':'error','stage':'anisette_refresh','kind':'network',
                            'error_type':'ReadTimeout','http':0})
        self.assertEqual(result['kind'],'timeout')
        self.assertEqual(result['stage'],'anisette_refresh')
        self.assertEqual(result['http'],0)
