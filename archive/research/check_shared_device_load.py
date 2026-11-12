"""One authorized 82-account zero-retry run with two shared identities."""
from datetime import datetime
from pathlib import Path
import sys
import threading
from zoneinfo import ZoneInfo
import os
import json

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from fastcheck.scheduler import Settings,Supervisor
from archive.legacy.gsa_batch import read_accounts,proxy_fingerprint
from fastcheck.devices import save_json
from fastcheck.settings_store import load_api
from tools.check_known_account_proxy import exit_ip


class ProbedSupervisor(Supervisor):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.probe_thread=None
        self.probe={}
    def dispatch(self):
        super().dispatch()
        if self.probe_thread is None and self.active:
            item=next(iter(self.active.values()))
            proxy=item['lease'].url
            self.probe={'row':item['job'].account.row,'proxy_fingerprint':proxy_fingerprint(proxy)}
            def probe():
                try:
                    self.probe['direct_exit']=exit_ip()
                    self.probe['proxy_exit']=exit_ip(proxy)
                    self.probe['different_from_direct']=self.probe['direct_exit']!=self.probe['proxy_exit']
                except Exception as exc:
                    self.probe['error_type']=type(exc).__name__
            self.probe_thread=threading.Thread(target=probe,daemon=True)
            self.probe_thread.start()


def main():
    accounts=read_accounts(ROOT/'账号.txt',max_accounts=82)
    api=load_api(ROOT)
    if not api:raise RuntimeError('Saved API required')
    directory=ROOT/'logs'/('optimized-gsa-'+datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y%m%d-%H%M%S')+'-'+str(os.getpid()))
    settings=Settings(protocol='gsa',processes=8,threads=10,device_capacity=2,warmup_devices=2,
                      device_parallelism=0,device_selection='prefer_new',device_rest_after=0,
                      max_attempts=1,debug=True)
    runner=ProbedSupervisor(accounts,api,ROOT,directory,settings)
    summary=runner.run()
    if runner.probe_thread:runner.probe_thread.join(20)
    save_json(directory/'egress-probe.json',runner.probe)
    print('优化实测报告：'+str(directory))
    print(json.dumps(summary,ensure_ascii=False))


if __name__=='__main__':main()
