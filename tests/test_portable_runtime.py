from pathlib import Path
from types import SimpleNamespace
import json
import tempfile
import unittest
from unittest.mock import MagicMock,patch

from fastcheck.scheduler import Settings,Supervisor
from fastcheck.devices import file_lease,PoolError,save_json
from fastcheck.process_control import isolate_process,terminate_tree
from fastcheck.web.bootstrap import initialize
from fastcheck.web.runtime import JobManager
from fastcheck.web.launcher import main


class PortableRuntimeTests(unittest.TestCase):
    def test_requested_hundred_workers_and_thread_capacity_are_not_truncated(self):
        settings=Settings(processes=100,threads=100)
        runner=object.__new__(Supervisor)
        runner.settings=settings;runner.context=MagicMock();runner.stop=object()
        runner.handler=object();runner.workers={}
        runner.start_workers()
        self.assertEqual(len(runner.workers),100)
        self.assertEqual(runner.context.Process.call_count,100)
        for call in runner.context.Process.call_args_list:
            self.assertEqual(call.kwargs['args'][1],100)
        self.assertEqual(Settings(processes=1000,threads=1000).processes,1000)

    def test_shared_leases_can_overlap_but_exclusive_writer_cannot(self):
        with tempfile.TemporaryDirectory() as temporary:
            path=Path(temporary)/'shared.lock'
            with file_lease(path,shared=True),file_lease(path,shared=True):
                with self.assertRaises(PoolError):
                    with file_lease(path):pass
            with file_lease(path):pass

    def test_atomic_state_write_retries_transient_windows_reader_contention(self):
        import os
        with tempfile.TemporaryDirectory() as temporary:
            path=Path(temporary)/'state.json';replace=os.replace;calls=[]
            def contend(source,target):
                calls.append(1)
                if len(calls)<3:raise PermissionError('reader holds file')
                return replace(source,target)
            with patch('fastcheck.devices.os.replace',side_effect=contend):save_json(path,{'ready':True})
            self.assertEqual(json.loads(path.read_text()),{'ready':True})
            self.assertEqual(len(calls),3)

    def test_windows_process_cleanup_uses_tree_termination_without_posix_calls(self):
        with patch('fastcheck.process_control.os.name','nt'),patch('fastcheck.process_control.subprocess.run') as run:
            isolate_process();terminate_tree(SimpleNamespace(pid=1234))
            self.assertEqual(run.call_args.args[0],['taskkill','/PID','1234','/T','/F'])

    def test_incremental_results_keep_partial_record_until_complete_and_do_not_duplicate(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);manager=JobManager(root);job='a'*32
            directory=root/'jobs'/job
            save_json(directory/'state.json',{'state':'completed'})
            save_json(directory/'accounts-index.json',[{'row':1,'email':'first@example.invalid'},{'row':2,'email':'second@example.invalid'}])
            first=json.dumps({'row':1,'status':'init_refused','stage':'init',
                              'apple_ec':-20970,'attempts':1})+'\n'
            second=json.dumps({'row':2,'status':'need_2fa','attempts':1})+'\n'
            path=directory/'results.jsonl';path.write_text(first+second[:10])
            self.assertEqual(manager.read_results(job)['total'],1)
            with path.open('a') as handle:handle.write(second[10:])
            self.assertEqual(manager.read_results(job)['total'],2)
            self.assertEqual(manager.read_results(job)['total'],2)
            self.assertEqual(manager.read_results(job,'need_2fa')['total'],1)
            invalid=manager.read_results(job,'invalid')
            self.assertEqual(invalid['total'],1)
            self.assertEqual(invalid['rows'][0]['category'],'invalid')
            manager.shutdown()

    def test_local_profile_initializes_requested_password_and_binds_to_loopback(self):
        from werkzeug.security import check_password_hash
        with tempfile.TemporaryDirectory() as temporary:
            with patch.dict('os.environ',{'GSA_DATA_DIR':temporary}),patch('waitress.serve') as serve:
                main(['--local','--web-threads','16'])
            auth=json.loads((Path(temporary)/'config/web-admin.json').read_text())
            self.assertTrue(check_password_hash(auth['password_hash'],'admin123'))
            self.assertEqual(serve.call_args.kwargs['port'],18088)
            self.assertEqual(serve.call_args.kwargs['threads'],16)


if __name__=='__main__':unittest.main()
