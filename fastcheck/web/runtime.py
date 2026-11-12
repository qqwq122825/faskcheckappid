import atexit
from collections import OrderedDict,defaultdict
from dataclasses import asdict
from datetime import datetime
import json
import multiprocessing as mp
import os
from pathlib import Path
import re
import signal
import tempfile
import threading
import time
import uuid
from zoneinfo import ZoneInfo

from fastcheck.scheduler import Settings,Supervisor
from fastcheck.workers import demo_attempt,demo_registration
from fastcheck.devices import file_lease,PoolError,private_directory,save_json
from fastcheck.settings_store import load_api
from fastcheck.task_io import Account
from fastcheck.process_control import isolate_process,terminate_tree
from fastcheck.status import result_bucket

TERMINAL={'completed','stopped','failed','unknown'}


def timestamp():return datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(timespec='seconds')


def run_job(root,job_id,accounts,settings_values,proxy_auth,simulation):
    isolate_process()
    root=Path(root);directory=root/'jobs'/job_id;state_path=directory/'state.json'
    state=json.loads(state_path.read_text())
    state.update(state='running',pid=os.getpid(),started_at=timestamp())
    save_json(state_path,state)
    finished=threading.Event()
    temporary=None
    try:
        with file_lease(root/'.job.lock'):
            settings=Settings(**settings_values)
            options={}
            runtime_root=root
            api=None
            if simulation:
                temporary=tempfile.TemporaryDirectory(prefix='console-demo-')
                runtime_root=Path(temporary.name)
                serial=[10000]
                def fetch(url,count):
                    serial[0]+=count
                    return [f'http://127.0.0.1:{serial[0]+n}' for n in range(count)]
                api='https://api.example.invalid/?num=200'
                options.update(fetcher=fetch,handler=demo_attempt,registration=demo_registration)
            else:
                api=load_api(root)
            if not api:raise ValueError('请先保存代理 API。')
            def progress(value):
                state['progress']=value
                save_json(state_path,state)
            runner=Supervisor(accounts,api,runtime_root,directory,settings,proxy_auth=proxy_auth,
                              progress=progress,console=False,**options)
            def monitor():
                while not finished.wait(.15):
                    if (directory/'stop').exists():
                        runner.request_stop('user_cancelled');return
            thread=threading.Thread(target=monitor,daemon=True);thread.start()
            summary=runner.run()
            state.update(state='stopped' if summary['stop_reason'] else 'completed',summary=summary,
                         completed_at=timestamp())
    except Exception as exc:
        state.update(state='failed',error_type=type(exc).__name__,completed_at=timestamp())
    finally:
        finished.set()
        for account in accounts:account.password=''
        if temporary:temporary.cleanup()
        save_json(state_path,state)


class JobManager:
    def __init__(self,root):
        self.root=private_directory(Path(root));private_directory(self.root/'jobs')
        self.context=mp.get_context('spawn');self.lock=threading.RLock();self.owned={}
        self.result_lock=threading.Lock();self.result_cache=OrderedDict()
        atexit.register(self.shutdown)

    def directory(self,job_id):
        if not re.fullmatch('[0-9a-f]{32}',job_id):raise ValueError('任务编号无效。')
        path=self.root/'jobs'/job_id
        if not (path/'state.json').exists():raise FileNotFoundError
        return path

    def get(self,job_id):
        with self.lock:
            process=self.owned.get(job_id)
            if process is not None and not process.is_alive():
                process.join();process.close();del self.owned[job_id]
                path=self.directory(job_id)/'state.json';state=json.loads(path.read_text())
                if state['state'] not in TERMINAL:
                    state.update(state='unknown',completed_at=timestamp(),error_type='WorkerExited')
                    save_json(path,state)
            return json.loads((self.directory(job_id)/'state.json').read_text())

    def history(self):
        values=[]
        for path in sorted((self.root/'jobs').glob('*/state.json'),reverse=True):
            try:values.append(self.get(path.parent.name))
            except (OSError,ValueError):continue
        return sorted(values,key=lambda v:v['created_at'],reverse=True)[:100]

    def read_results(self,job_id,category='all',offset=0,limit=100):
        """Read only appended complete records; never reread the whole log each poll."""
        directory=self.directory(job_id)
        with self.result_lock:
            cached=self.result_cache.get(job_id)
            if cached is None:
                index={v['row']:v['email'] for v in json.loads((directory/'accounts-index.json').read_text())}
                cached={'index':index,'position':0,'rows':defaultdict(list)}
                self.result_cache[job_id]=cached
            self.result_cache.move_to_end(job_id)
            while len(self.result_cache)>4:self.result_cache.popitem(last=False)
            path=directory/'results.jsonl'
            if path.exists():
                with path.open('rb') as stream:
                    stream.seek(cached['position'])
                    while True:
                        line=stream.readline()
                        if not line or not line.endswith(b'\n'):break
                        cached['position']=stream.tell()
                        try:value=json.loads(line)
                        except ValueError:continue
                        value['email']=cached['index'].get(value['row'],'')
                        value['category']='not_started' if not value.get('attempts') else result_bucket(value)
                        cached['rows']['all'].append(value)
                        cached['rows'][value['category']].append(value)
            rows=cached['rows'][category]
            return {'total':len(rows),'rows':rows[offset:offset+limit]}

    def start(self,accounts,settings,proxy_auth='',simulation=False):
        with self.lock:
            for job_id in list(self.owned):
                if self.get(job_id)['state'] not in TERMINAL:raise RuntimeError('已有任务运行，请先等待或停止。')
            try:
                with file_lease(self.root/'.job.lock'):pass
            except PoolError:
                raise RuntimeError('已有总控占用设备，请等待当前任务结束。') from None
            job_id=uuid.uuid4().hex
            directory=private_directory(self.root/'jobs'/job_id)
            state={'id':job_id,'state':'queued','created_at':timestamp(),'protocol':settings.protocol,
                   'simulation':simulation,'accounts':len(accounts),'settings':asdict(settings)}
            save_json(directory/'state.json',state)
            save_json(directory/'accounts-index.json',[{'row':a.row,'email':a.email} for a in accounts])
            process=self.context.Process(target=run_job,args=(str(self.root),job_id,accounts,asdict(settings),proxy_auth,simulation),name='web-task-supervisor')
            process.start();self.owned[job_id]=process
            return state

    def stop(self,job_id):
        state=self.get(job_id)
        if state['state'] not in TERMINAL:
            fd=os.open(self.directory(job_id)/'stop',os.O_CREAT|os.O_WRONLY,0o600);os.close(fd)
        return state

    def shutdown(self):
        with self.lock:
            for job_id,process in list(self.owned.items()):
                if process.is_alive():
                    fd=os.open(self.directory(job_id)/'stop',os.O_CREAT|os.O_WRONLY,0o600);os.close(fd)
                process.join(5)
                if process.is_alive():
                    terminate_tree(process)
                    process.join(2)
                if process.is_alive():
                    terminate_tree(process,force=True)
                    process.join(1)
                process.close();self.owned.pop(job_id,None)
