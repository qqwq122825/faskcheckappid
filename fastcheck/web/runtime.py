import atexit
from collections import OrderedDict,defaultdict
from dataclasses import asdict
from datetime import datetime
import json
import multiprocessing as mp
import os
from pathlib import Path
import re
import shutil
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


def _node_executor():
    """Import the exact executor used by deployed cluster nodes."""
    import sys
    root=Path(__file__).resolve().parents[2]
    cluster=root/'cluster-lab'
    if str(cluster) not in sys.path:sys.path.insert(0,str(cluster))
    from clusterlab.executor import execute_unit_group
    return execute_unit_group


def _node_progress(value,total,started):
    """Adapt node progress to the local console without changing execution."""
    processed=max(0,min(total,int(value.get('processed') or 0)))
    counts=dict(value.get('counts') or {})
    counts.update(total=total,processed=processed,remaining=max(0,total-processed),
                  active=max(0,int(value.get('active_threads_total') or 0))
                  if processed<total else 0)
    return {'counts':counts,'phase':'节点执行器本地模式',
            'elapsed_seconds':round(max(0,time.time()-started),3),
            'executor_backend':'clusterlab.executor',
            'workers':int(value.get('workers') or 0),
            'active_threads_by_process':value.get('active_threads_by_process') or [],
            'active_threads_total':int(value.get('active_threads_total') or 0),
            **({'proxy_pool':value['proxy_pool']} if value.get('proxy_pool') else {})}


def run_node_auth_job(root,directory,accounts,settings,api,progress):
    """Run a local GSA task through the deployed node's executor code path."""
    directory=Path(directory);started=time.time()
    input_path=directory/'input.part'
    with input_path.open('w',encoding='utf-8',newline='') as output:
        input_path.chmod(0o600)
        for account in accounts:
            output.write(f'{account.email}----{account.password}\n')
    rows=len(accounts)
    capacity=max(1,min(rows,settings.processes*settings.threads))
    descriptor={
        'id':directory.name,'job_id':directory.name,'mode':'auth',
        'first_row':1,'rows':rows,'processes':settings.processes,
        'threads':settings.threads,'timeout':settings.timeout,
        'retries':max(0,settings.max_attempts-1),'proxy_api':api,
        'proxy_scheme':settings.proxy_scheme,'proxy_ttl':settings.proxy_ttl,
        'proxy_pool_size':1000,
        'origin':settings.origin,'device_root':str(private_directory(Path(root)/'device-pool')),
        'device_pool_capacity':capacity,'device_slots':list(range(1,capacity+1)),
        'device_use_limit':20,'_proxy_debug':bool(settings.debug),
        '_web_secondary_auth':True,
    }
    save_json(directory/'manifest.json',descriptor)
    context=mp.get_context('spawn');stop=context.Event();finished=threading.Event()
    def monitor():
        while not finished.wait(.15):
            if (directory/'stop').exists():stop.set()
            try:value=json.loads((directory/'progress.json').read_text())
            except (OSError,ValueError):value={}
            progress(_node_progress(value,rows,started))
    thread=threading.Thread(target=monitor,name='local-node-monitor',daemon=True)
    thread.start()
    try:_node_executor()(directory,descriptor,stop)
    finally:
        finished.set();thread.join()
    done=json.loads((directory/'done.json').read_text())
    if not done.get('ok'):raise RuntimeError(str(done.get('error') or 'node executor failed'))
    try:value=json.loads((directory/'progress.json').read_text())
    except (OSError,ValueError):value={}
    current=_node_progress(value,rows,started);progress(current)
    results=dict(done.get('counts') or {})
    return {'accounts':rows,'results':results,'display_counts':current['counts'],
            'elapsed_seconds':round(float(done.get('elapsed') or time.time()-started),3),
            'max_active':int(done.get('active_threads_total') or
                             current.get('active_threads_total') or 0),
            'active_threads_by_process':done.get('active_threads_by_process') or
                                        current.get('active_threads_by_process') or [],
            'executor_backend':'clusterlab.executor','stop_reason':
            'user_cancelled' if stop.is_set() else None}


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
            use_node=(not simulation and settings.protocol=='gsa' and
                      os.environ.get('FASTCHECK_LOCAL_NODE_EXECUTOR')=='1')
            if use_node:
                summary=run_node_auth_job(root,directory,accounts,settings,api,progress)
                state.update(state='stopped' if summary['stop_reason'] else 'completed',
                             summary=summary,executor_backend='clusterlab.executor',
                             completed_at=timestamp())
                return
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

    def clear_history(self):
        """删除全部历史任务目录；有非终态任务时拒绝执行。"""
        with self.lock:
            for job_id in list(self.owned):
                if self.get(job_id)['state'] not in TERMINAL:
                    raise RuntimeError('有任务正在运行，先停止再清空。')
            removed = 0
            for path in (self.root/'jobs').glob('*/state.json'):
                try:
                    shutil.rmtree(path.parent)
                    removed += 1
                except OSError:
                    continue
            with self.result_lock:
                self.result_cache.clear()
            return removed

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
                        if value.get('protocol')=='GSA' and 'code' in value:
                            # Node records already contain their final bucket.  Only
                            # decorate them for this page; never reclassify them with
                            # the legacy local-result rules.
                            value['category']=value.get('status') or 'undetermined'
                            value['result_code']=value.get('code')
                            value['total_ms']=round(float(value.get('elapsed') or 0)*1000)
                            value['retry_count']=max(0,int(value.get('attempts') or 0))
                            label=str(value.get('device_label') or '').strip()
                            slot=value.get('device')
                            device=label or (f'#{slot}' if slot else '—')
                            usage=max(0,int(value.get('device_usage') or 0))
                            value['device_display']=device+(f' · 第 {usage} 次' if usage else '')
                        else:
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
            if (not simulation and settings.protocol=='gsa' and
                    os.environ.get('FASTCHECK_LOCAL_NODE_EXECUTOR')=='1'):
                state['executor_backend']='clusterlab.executor'
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
