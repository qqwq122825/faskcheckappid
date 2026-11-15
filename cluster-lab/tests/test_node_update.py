import subprocess
import unittest
from unittest.mock import patch

from tools.node_update import resolve_commit


class NodeUpdateTests(unittest.TestCase):
    def test_plain_branch_prefers_fresh_remote_over_stale_local_branch(self):
        calls = []

        def fake_run(command, *_args, **_kwargs):
            ref = command[-1]
            calls.append(ref)
            return {'origin/main^{commit}': 'new', 'main^{commit}': 'old'}[ref]

        with patch('tools.node_update.run', side_effect=fake_run):
            self.assertEqual(resolve_commit('/repo', 'main'), 'new')
        self.assertEqual(calls, ['origin/main^{commit}'])

    def test_commit_falls_back_when_no_remote_named_ref_exists(self):
        def fake_run(command, *_args, **_kwargs):
            if command[-1].startswith('origin/'):
                raise subprocess.CalledProcessError(1, command)
            return 'exact-commit'

        with patch('tools.node_update.run', side_effect=fake_run):
            self.assertEqual(resolve_commit('/repo', 'abc123'), 'exact-commit')


if __name__ == '__main__':
    unittest.main()
