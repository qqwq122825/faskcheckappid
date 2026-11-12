import contextlib
import io
import threading
from types import SimpleNamespace
import unittest

from fastcheck.cli import choose_run_settings,read_integer
from fastcheck.scheduler import Settings
from fastcheck.proxy_pool import ProxyPool


class RunOptionsTests(unittest.TestCase):
    def test_user_selects_five_processes_ten_threads_and_one_hundred_ips(self):
        answers=iter(('5','10','100'))
        args=SimpleNamespace(processes=None,threads=None,proxy_target=None)
        values=choose_run_settings({},args,reader=lambda prompt:next(answers))
        settings=Settings(**values)
        self.assertEqual((settings.processes,settings.threads,settings.proxy_target),(5,10,100))

    def test_enter_uses_config_defaults_and_zero_means_task_count(self):
        args=SimpleNamespace(processes=None,threads=None,proxy_target=None)
        values=choose_run_settings({'processes':3,'threads':4,'proxy_target':0},args,reader=lambda prompt:'')
        self.assertEqual((values['processes'],values['threads'],values['proxy_target']),(3,4,0))

    def test_explicit_flags_skip_interactive_fields(self):
        args=SimpleNamespace(processes=5,threads=10,proxy_target=100)
        def forbidden(prompt):raise AssertionError('Unexpected prompt')
        values=choose_run_settings({},args,reader=forbidden)
        self.assertEqual((values['processes'],values['threads'],values['proxy_target']),(5,10,100))

    def test_invalid_input_can_be_corrected(self):
        values=iter(('not-a-number','0','9','5'))
        with contextlib.redirect_stdout(io.StringIO()):
            result=read_integer('进程数',2,1,8,reader=lambda prompt:next(values))
        self.assertEqual(result,5)

    def test_proxy_api_never_requests_more_than_two_hundred(self):
        stop=threading.Event()
        requested=[]
        def fetch(url,count):
            requested.append(count)
            stop.set()
            return []
        pool=ProxyPool('https://api.example.invalid/?num=200',stop,lambda *a,**k:None,
                       target=500,fetcher=fetch)
        pool._supply()
        self.assertEqual(requested,[200])

    def test_gsa_options_include_capacity_and_rest_without_waiting_for_entire_pool(self):
        args=SimpleNamespace(processes=None,threads=None,proxy_target=None)
        answers=iter(('2','10','0','10','0','20','30'))
        values=choose_run_settings({'protocol':'gsa'},args,reader=lambda prompt:next(answers))
        settings=Settings(**values)
        self.assertEqual(settings.warmup_devices,2)
        self.assertEqual((settings.device_capacity,settings.device_rest_after,settings.device_rest_seconds),(10,20,30))


if __name__=='__main__':
    unittest.main()
