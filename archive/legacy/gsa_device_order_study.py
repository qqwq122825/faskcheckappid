"""Bounded first/second-account experiment and one fresh-device retry cohort."""
from collections import Counter, deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import getpass
import hashlib
import ipaddress
import json
import multiprocessing as mp
import os
from pathlib import Path
import queue
import signal
import sys
import time
import warnings
from zoneinfo import ZoneInfo

import requests

from fastcheck.diagnostics import SafeDiagnostics, TracedClient
from fastcheck.proxy_pool import ApiFetcher
from archive.legacy.gsa_batch import EventLog, proxy_fingerprint, read_accounts
from fastcheck.devices import DevicePool, RetryPolicy, save_json
from fastcheck.protocols import gsa_status as gsa

ROOT=Path(__file__).resolve().parent
ORIGIN=next(value for value in gsa.ANISETTE_SERVERS if value.startswith('https://')).rstrip('/')


def retry_eligible(result):
    if result.get('stage')=='init' and result.get('apple_ec')==-20970:
        return True
    return result.get('status') in ('error','not_tested') and result.get('kind') in (
        'network','tls','server','timeout','lease_expiring','registration_network')


def exit_ip(proxy):
    with requests.Session() as session:
        session.trust_env=False
        session.proxies={'http':proxy,'https':proxy}
        with session.get('https://api.ipify.org?format=json',timeout=(4,6),
                         allow_redirects=False,stream=True) as response:
            response.raise_for_status()
            data=bytearray()
            for block in response.iter_content(512):
                data.extend(block)
                if len(data)>2048:raise ValueError('Unexpected IP-check response')
            return str(ipaddress.ip_address(json.loads(data)['ip']))


def study_unit(payload,output,stop):
    signal.signal(signal.SIGINT,signal.SIG_IGN)
    diagnostic=SafeDiagnostics(output,role='device_order_study',study_phase=payload['phase'],
        device=payload['slot'],proxy_fingerprint=proxy_fingerprint(payload['proxy']))
    current_stage='proxy_probe'
    returned=set()
    try:
        public_ip=exit_ip(payload['proxy'])
        # Isolated fixed-size experiment: each identity sees at most two
        # first-pass accounts, plus no implicit retries. Production health and
        # its 3-device circuit are kept intact in their original directory.
        policy=RetryPolicy(attempts=1,provider_device_limit=1000)
        pool=DevicePool(payload['root'],ORIGIN,max_devices=30,client_factory=TracedClient,policy=policy)
        current_stage='registration'
        info=pool.info(12,diagnostics=diagnostic,provider_direct=True)
        started=time.perf_counter()
        pool.fetch_headers(payload['slot'],info,25,registered=False,proxy=payload['proxy'],
                           provider_direct=True,diagnostics=diagnostic)
        fingerprint=hashlib.sha256(pool.inspect(payload['slot'])['device_id'].encode()).hexdigest()[:12]
        diagnostic.context['device_fingerprint']=fingerprint
        output.put({'_event':{'event':'study_device_ready','device':payload['slot'],
            'study_phase':payload['phase'],'exit_ip':public_ip,'device_fingerprint':fingerprint,
            'proxy_fingerprint':proxy_fingerprint(payload['proxy']),
            'registration_ms':round((time.perf_counter()-started)*1000,2)}})
        for position,account in enumerate(payload['accounts'],1):
            if stop.is_set():break
            diagnostic.context.update(row=account['row'],position=position)
            remaining=payload['expires_at']-time.monotonic()
            if position>1 and remaining>=14:
                current_stage='proxy_probe'
                public_ip=exit_ip(payload['proxy'])
                remaining=payload['expires_at']-time.monotonic()
            if remaining<14:
                result={'status':'not_tested','kind':'lease_expiring','stage':'lease','attempted':False}
            else:
                current_stage='authentication'
                output.put({'_event':{'event':'study_auth_start','row':account['row'],
                    'device':payload['slot'],'position':position,'study_phase':payload['phase'],
                    'exit_ip':public_ip,'proxy_fingerprint':proxy_fingerprint(payload['proxy'])}})
                started=time.perf_counter()
                result=pool.authenticate(payload['slot'],account['email'],account['password'],
                    timeout=10,prepare_timeout=12,proxy=payload['proxy'],provider_direct=True,
                    diagnostics=diagnostic,stop_event=stop)
                result={key:result.get(key) for key in
                    ('status','kind','stage','apple_ec','password_verified','two_factor_status','attempt_history')}
                result.update(attempted=True,elapsed_ms=round((time.perf_counter()-started)*1000,2))
            result.update(row=account['row'],device=payload['slot'],position=position,
                study_phase=payload['phase'],exit_ip=public_ip,device_fingerprint=fingerprint,
                proxy_fingerprint=proxy_fingerprint(payload['proxy']))
            output.put({'_result':result})
            returned.add(account['row'])
            if result.get('kind')=='ratelimit':
                stop.set()
                break
            if position<len(payload['accounts']) and stop.wait(1):break
    except Exception as exc:
        http=getattr(exc,'http',0) or getattr(getattr(exc,'response',None),'status_code',0)
        kind='ratelimit' if http==429 else 'registration_network' if current_stage=='registration' else 'network'
        for position,account in enumerate(payload['accounts'],1):
            if account['row'] not in returned:
                output.put({'_result':{'row':account['row'],'device':payload['slot'],
                    'position':position,'study_phase':payload['phase'],'status':'not_tested','kind':kind,
                    'stage':current_stage,'error_type':type(exc).__name__,'http':http,'attempted':False,
                    'proxy_fingerprint':proxy_fingerprint(payload['proxy'])}})
        if http==429:stop.set()
    finally:
        gsa.close_session()
        output.put({'done':True,'dropped_debug_events':diagnostic.dropped})


class StudyRunner:
    def __init__(self,accounts,api_url,directory,device_root,*,worker=study_unit,fetcher=None,
                 wait_until=0,unit_budget=55,prepare_info=True):
        self.accounts=accounts
        self.directory=Path(directory)
        self.device_root=str(device_root)
        self.context=mp.get_context('spawn')
        self.stop=self.context.Event()
        self.log=EventLog(directory,debug=True)
        self.worker=worker
        self.fetcher=fetcher or ApiFetcher(self.context,self.stop,debug_log=self.log.debug)
        self.api_url=api_url
        self.wait_until,self.unit_budget,self.prepare_info=wait_until,unit_budget,prepare_info
        self.results=[]
        self.active={}
        self.serial=0
        self.stop_reason=None

    def _fetch(self):
        started=time.monotonic()
        return self.fetcher(self.api_url,1)[0],started+60

    def _reap(self,process,output,terminate=False):
        if terminate and process.is_alive():process.terminate()
        process.join(.5)
        if process.is_alive():process.kill();process.join(2)
        output.cancel_join_thread();output.close();process.close()

    def _accept(self,unit,result):
        if result['row'] in unit['returned']:return
        unit['returned'].add(result['row'])
        self.results.append(result)
        self.log.result(result)
        self.log('study_account_result',row=result['row'],device=result['device'],
            position=result['position'],study_phase=result['study_phase'],status=result.get('status'),
            stage=result.get('stage'),apple_ec=result.get('apple_ec'),attempted=result.get('attempted'))
        if result.get('kind')=='ratelimit':
            self.stop_reason='ratelimit'
            self.stop.set()

    def _drain(self):
        for slot,unit in list(self.active.items()):
            process,output=unit['process'],unit['output']
            done=False
            for _ in range(1024):
                try:message=output.get_nowait()
                except queue.Empty:break
                if '_debug' in message:self.log.debug(message['_debug'])
                elif '_event' in message:
                    event=dict(message['_event'])
                    name=event.pop('event')
                    if name=='study_auth_start':unit['auth_started'].add(event['row'])
                    self.log(name,**event)
                elif '_result' in message:self._accept(unit,message['_result'])
                elif message.get('done'):
                    done=True
                    break
            expired=time.monotonic()>=unit['deadline']
            if not done and process.is_alive() and not expired:continue
            if not done and not process.is_alive():
                # A normal child flushes all messages before exiting. Give a
                # just-arrived queue item another scheduler pass to be read.
                try:message=output.get(timeout=.05)
                except queue.Empty:pass
                else:
                    if '_debug' in message:self.log.debug(message['_debug'])
                    elif '_event' in message:
                        event=dict(message['_event']);name=event.pop('event')
                        if name=='study_auth_start':unit['auth_started'].add(event['row'])
                        self.log(name,**event)
                    elif '_result' in message:self._accept(unit,message['_result'])
                    elif message.get('done'):done=True
                    if not done:continue
            self._reap(process,output,terminate=expired and not done)
            for position,account in enumerate(unit['accounts'],1):
                if account.row not in unit['returned']:
                    self._accept(unit,{'row':account.row,'device':slot,'position':position,
                        'study_phase':unit['phase'],'status':'error','kind':'timeout','stage':'worker',
                        'attempted':account.row in unit['auth_started']})
            del self.active[slot]

    def _cohort(self,phase,groups):
        pending=deque(groups)
        future=None
        waiting=None
        with ThreadPoolExecutor(max_workers=1,thread_name_prefix='study-proxy-supply') as supply:
            try:
                while (pending or future or self.active) and not self.stop.is_set():
                    self._drain()
                    if self.stop.is_set():break
                    if future is None and pending and len(self.active)<2:
                        waiting=pending.popleft()
                        self.log('study_proxy_fetch',study_phase=phase,count=1)
                        future=supply.submit(self._fetch)
                    if future is not None and future.done():
                        try:proxy,expires=future.result()
                        except Exception as exc:
                            self.stop_reason='proxy_api_failure'
                            self.log('study_stopped',reason=self.stop_reason,error_type=type(exc).__name__)
                            self.stop.set()
                            break
                        self.serial+=1
                        slot=self.serial
                        output=self.context.Queue(maxsize=2048)
                        payload={'slot':slot,'root':self.device_root,'phase':phase,
                            'proxy':proxy,'expires_at':expires,
                            'accounts':[{'row':a.row,'email':a.email,'password':a.password} for a in waiting]}
                        process=self.context.Process(target=self.worker,args=(payload,output,self.stop),
                                                     name=f'study-device-{slot}')
                        process.start()
                        self.active[slot]={'process':process,'output':output,'accounts':waiting,'phase':phase,
                            'returned':set(),'auth_started':set(),
                            'deadline':min(time.monotonic()+self.unit_budget,expires-2)}
                        self.log('study_unit_start',device=slot,study_phase=phase,rows=[a.row for a in waiting],
                            active_units=len(self.active),proxy_fingerprint=proxy_fingerprint(proxy))
                        future=None
                        waiting=None
                    self.stop.wait(.02)
            except BaseException:
                self.stop.set()
                raise
            finally:
                if self.stop.is_set():
                    time.sleep(.05)
                    self._drain()
                    if self.stop_reason is None:self.stop_reason='stop_requested'
                    if future:future.cancel()
                    for slot,unit in list(self.active.items()):
                        self._reap(unit['process'],unit['output'],terminate=True)
                        for position,account in enumerate(unit['accounts'],1):
                            if account.row not in unit['returned']:
                                self._accept(unit,{'row':account.row,'device':slot,'position':position,
                                    'study_phase':phase,'status':'stopped','kind':'cancelled','stage':'worker',
                                    'attempted':account.row in unit['auth_started']})
                        del self.active[slot]

    def run(self):
        self.log('study_plan',accounts=len(self.accounts),concurrency=2,
                 max_auth_calls=len(self.accounts)*2,max_new_devices=(len(self.accounts)+1)//2+len(self.accounts),
                 production_circuit_unchanged=True)
        if self.wait_until>time.time():
            self.log('study_wait',until=datetime.fromtimestamp(self.wait_until,ZoneInfo('Asia/Shanghai')).isoformat())
        try:
            while time.time()<self.wait_until:
                if self.stop.wait(min(1,self.wait_until-time.time())):break
            if self.prepare_info and not self.stop.is_set():
                # One public configuration fetch; it doesn't register a device.
                DevicePool(self.device_root,ORIGIN,max_devices=30).info(12)
            if not self.stop.is_set():
                groups=[self.accounts[index:index+2] for index in range(0,len(self.accounts),2)]
                self._cohort('paired_first_pass',groups)
            failures={r['row']:r for r in self.results if r['study_phase']=='paired_first_pass' and retry_eligible(r)}
            save_json(self.directory/'failed-queue.json',{'rows':sorted(failures),
                'reason':'init/-20970 or transient/unknown transport; definitive account outcomes excluded'})
            if failures and not self.stop.is_set():
                self.log('study_fresh_retry_start',rows=sorted(failures),max_attempts_per_row=1)
                self._cohort('fresh_device_retry',[[a] for a in self.accounts if a.row in failures])
        except KeyboardInterrupt:
            self.stop_reason='user_cancelled';self.stop.set()
        except Exception as exc:
            self.stop_reason='setup_or_runtime_error';self.stop.set()
            self.log('study_stopped',reason=self.stop_reason,error_type=type(exc).__name__)
        finally:
            self.stop.set()
            for account in self.accounts:account.password=''
        summary=self.summarize()
        save_json(self.directory/'summary.json',summary)
        save_json(self.directory/'debug-summary.json',self.log.debug_summary())
        self.log('study_done',accounts_observed=summary['accounts_observed'],stop_reason=self.stop_reason)
        return summary

    def summarize(self):
        groups={}
        for phase,position in (('paired_first_pass',1),('paired_first_pass',2),('fresh_device_retry',1)):
            records=[r for r in self.results if r['study_phase']==phase and r['position']==position]
            groups[f'{phase}_position_{position}']={'records':len(records),
                'auth_calls':sum(bool(r.get('attempted')) for r in records),
                'init_20970':sum(r.get('stage')=='init' and r.get('apple_ec')==-20970 for r in records),
                'reached_complete':sum(r.get('stage')=='complete' for r in records),
                'password_verified':sum(bool(r.get('password_verified')) for r in records),
                'statuses':dict(Counter(r['status'] for r in records))}
        return {'accounts':len(self.accounts),'accounts_observed':len({r['row'] for r in self.results}),
            'groups':groups,'new_device_units':self.serial,'stop_reason':self.stop_reason,
            'account_markers':[{'row':a.row,
                'first_pass':[r for r in self.results if r['row']==a.row and r['study_phase']=='paired_first_pass'],
                'fresh_retry':[r for r in self.results if r['row']==a.row and r['study_phase']=='fresh_device_retry'],
                'not_started':not any(r['row']==a.row for r in self.results)} for a in self.accounts],
            'results':self.results,'interpretation':'Position association is observational. Definitive password outcomes require server M2. No server-side restriction threshold is inferred.'}


def main():
    path=input('账号文件路径（回车使用当前目录的账号.txt）：').strip().strip('"\'')
    accounts=read_accounts(path or ROOT/'账号.txt',max_accounts=20)
    with warnings.catch_warnings():
        warnings.simplefilter('error',getpass.GetPassWarning)
        api_url=getpass.getpass('代理 API 地址（隐藏输入，不保存）：').strip()
    stamp=datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y%m%d-%H%M%S')
    directory=ROOT/'logs'/f'order-study-{stamp}-{os.getpid()}'
    old_pool=DevicePool(ROOT/'config/batch-device-pool',ORIGIN,max_devices=10)
    wait_until=max(time.time(),old_pool.health().get('provider_until',0))+5
    runner=StudyRunner(accounts,api_url,directory,ROOT/'config/device-order-studies'/f'{stamp}-{os.getpid()}',
                       wait_until=wait_until)
    summary=runner.run()
    print('设备顺序对照报告：'+str(directory/'summary.json'))
    print(json.dumps(summary['groups'],ensure_ascii=False))


if __name__=='__main__':
    main()
