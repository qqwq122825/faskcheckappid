import contextlib
import io
from pathlib import Path
import stat
import tempfile
import unittest

from fastcheck.settings_store import load_api,prompt_api,save_api,settings_path


class ProxySettingsTests(unittest.TestCase):
    def test_round_trip_preserves_api_query_and_private_permissions(self):
        with tempfile.TemporaryDirectory() as directory:
            self.assertIsNone(load_api(directory))
            value='https://api.example.invalid/list?password=secret%2Btoken&num=200&custom=123'
            save_api(directory,value)
            self.assertEqual(load_api(directory),value)
            self.assertEqual(stat.S_IMODE(settings_path(directory).stat().st_mode),0o600)
            self.assertEqual(stat.S_IMODE(settings_path(directory).parent.stat().st_mode),0o700)

    def test_blank_reuses_and_new_input_replaces_without_echoing_secrets(self):
        with tempfile.TemporaryDirectory() as directory:
            first='https://api.example.invalid/list?password=first-secret&num=200'
            second='https://api.example.invalid/list?password=second-secret&num=100'
            save_api(directory,first)
            display=io.StringIO()
            with contextlib.redirect_stdout(display):
                self.assertEqual(prompt_api(directory,reader=lambda prompt:''),first)
                self.assertEqual(prompt_api(directory,reader=lambda prompt:second),second)
            self.assertEqual(load_api(directory),second)
            self.assertNotIn('first-secret',display.getvalue())
            self.assertNotIn('second-secret',display.getvalue())

    def test_invalid_replacement_does_not_overwrite_working_setting(self):
        with tempfile.TemporaryDirectory() as directory:
            value='https://api.example.invalid/list?password=secret&num=200'
            save_api(directory,value)
            for invalid in ('','https://api.example.invalid/?num=0&password=hidden-secret',
                            'https://api.example.invalid/?num=2&num=3','http://bad:invalid/?password=hidden-secret',
                            'https://api.example.invalid/#hidden-secret'):
                with self.assertRaises(ValueError) as raised:
                    save_api(directory,invalid)
                self.assertNotIn('hidden-secret',str(raised.exception))
                self.assertEqual(load_api(directory),value)

    def test_corrupt_saved_file_can_be_replaced_without_printing_contents(self):
        with tempfile.TemporaryDirectory() as directory:
            save_api(directory,'https://api.example.invalid/?num=20')
            settings_path(directory).write_text('invalid-json-secret')
            with self.assertRaises(ValueError) as raised:
                load_api(directory)
            self.assertNotIn('invalid-json-secret',str(raised.exception))
            value='https://api.example.invalid/?num=10&password=new-secret'
            display=io.StringIO()
            with contextlib.redirect_stdout(display):
                self.assertEqual(prompt_api(directory,reader=lambda prompt:value),value)
            self.assertNotIn('invalid-json-secret',display.getvalue())
            self.assertEqual(load_api(directory),value)


if __name__=='__main__':
    unittest.main()
