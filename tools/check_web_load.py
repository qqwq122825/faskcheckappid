"""Exercise the real HTTP server with synthetic tasks and concurrent polling only."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import requests

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from fastcheck.process_control import terminate_tree


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--port',type=int,default=18089)
    parser.add_argument('--accounts',type=int,default=1000)
    parser.add_argument('--processes',type=int,default=8)
    parser.add_argument('--threads',type=int,default=10)
    parser.add_argument('--output',default=str(ROOT/'data/validation/web-load.json'))
    args=parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='fastcheck-http-load-') as temporary:
        data=Path(temporary)/'data';env=os.environ.copy();env.pop('FASTCHECK_MOCK',None)
        with (Path(temporary)/'http.log').open('w') as log:
            process=subprocess.Popen([sys.executable,str(ROOT/'app.py'),'--local','--data-dir',str(data),'--port',str(args.port)],
                                     cwd=ROOT,env=env,stdout=log,stderr=log,start_new_session=os.name!='nt')
            try:
                base=f'http://127.0.0.1:{args.port}';client=requests.Session();client.trust_env=False
                deadline=time.monotonic()+10
                while True:
                    try:
                        client.get(base+'/healthz',timeout=1).raise_for_status();break
                    except requests.RequestException:
                        if time.monotonic()>deadline:raise
                        time.sleep(.1)
                csrf=client.get(base+'/api/session').json()['csrf']
                response=client.post(base+'/api/login',json={'username':'admin','password':'admin123'},headers={'X-CSRF-Token':csrf})
                response.raise_for_status();csrf=response.json()['csrf']
                settings={'protocol':'gsa','processes':args.processes,'threads':args.threads,'device_capacity':2,
                          'warmup_devices':2,'device_parallelism':0,'device_rest_after':0,'debug':True,'max_attempts':1}
                started=time.perf_counter()
                response=client.post(base+'/api/jobs',data={'simulation':'true','demo_count':str(args.accounts),'settings':json.dumps(settings)},headers={'X-CSRF-Token':csrf})
                response.raise_for_status();job=response.json()['id'];stop=threading.Event();latencies=[];failures=[];lock=threading.Lock()
                def poll(number):
                    session=requests.Session();session.trust_env=False;session.cookies.update(client.cookies)
                    while not stop.is_set():
                        for endpoint in (f'/api/jobs/{job}',f'/api/jobs/{job}/results?limit=100'):
                            tick=time.perf_counter()
                            try:session.get(base+endpoint,timeout=5).raise_for_status()
                            except requests.RequestException as exc:
                                with lock:failures.append(type(exc).__name__)
                            else:
                                with lock:latencies.append((time.perf_counter()-tick)*1000)
                        stop.wait(.02)
                    session.close()
                with ThreadPoolExecutor(max_workers=8) as executor:
                    futures=[executor.submit(poll,n) for n in range(8)]
                    deadline=time.monotonic()+90
                    while True:
                        state=client.get(base+'/api/jobs/'+job,timeout=5).json()
                        if state['state'] in ('completed','failed','stopped','unknown'):break
                        if time.monotonic()>deadline:raise TimeoutError('Offline load test exceeded budget')
                        time.sleep(.05)
                    stop.set()
                    for future in futures:future.result()
                wall=time.perf_counter()-started
                assert state['state']=='completed',state
                rows=[]
                for offset in range(0,args.accounts,200):rows+=client.get(base+f'/api/jobs/{job}/results?offset={offset}&limit=200').json()['rows']
                assert len(rows)==args.accounts==len({row['row'] for row in rows})
                assert not failures,failures
                events=[json.loads(line) for line in (data/'jobs'/job/'events.jsonl').read_text().splitlines()]
                workers={event['worker_pid'] for event in events if event['event']=='worker_ready'}
                assert len(workers)==args.processes
                ordered=sorted(latencies)
                result={'version':client.get(base+'/healthz').json()['version'],'execution':'offline synthetic, loopback HTTP only',
                        'accounts':args.accounts,'processes':args.processes,'threads_per_process':args.threads,'actual_worker_processes':len(workers),
                        'maximum_in_flight':state['summary']['max_active'],'wall_seconds':round(wall,3),
                        'concurrent_pollers':8,'http_samples':len(ordered),'http_failures':len(failures),
                        'http_median_ms':round(statistics.median(ordered),3),'http_p95_ms':round(ordered[int((len(ordered)-1)*.95)],3),
                        'http_max_ms':round(max(ordered),3),'duplicate_or_missing_results':0,
                        'real_device_pool_untouched':not (data/'config/batch-device-pool').exists(),
                        'note':'Synthetic scheduling and HTTP responsiveness only; no measurement of Apple service throughput.'}
                output=Path(args.output);output.parent.mkdir(parents=True,exist_ok=True);output.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
                print(json.dumps(result,ensure_ascii=False))
            finally:
                terminate_tree(process)
                try:process.wait(12)
                except subprocess.TimeoutExpired:terminate_tree(process,force=True);process.wait()


if __name__=='__main__':main()
