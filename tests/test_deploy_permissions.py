from pathlib import Path
import os
import shutil
import stat
import subprocess
import tempfile
import unittest


class DeployPermissionHookTests(unittest.TestCase):
    def test_normalizer_repairs_root_umask_style_source_modes(self):
        root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory)
            subprocess.run(['git', 'init', '-q'], cwd=target, check=True)
            (target / 'deploy').mkdir()
            (target / 'fastcheck').mkdir()
            shutil.copy2(root / 'deploy' / 'normalize-code-permissions.sh',
                         target / 'deploy' / 'normalize-code-permissions.sh')
            source = target / 'fastcheck' / 'status.py'
            source.write_text('VALUE = 1\n', encoding='utf-8')
            source.chmod(0o600)

            subprocess.run([str(target / 'deploy' / 'normalize-code-permissions.sh')],
                           cwd=target, check=True, capture_output=True, text=True)

            self.assertTrue(stat.S_IMODE(source.stat().st_mode) & stat.S_IROTH)
            self.assertTrue(os.access(source, os.R_OK))


if __name__ == '__main__':
    unittest.main()
