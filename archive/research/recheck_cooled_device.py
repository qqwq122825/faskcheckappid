"""One bounded recovery check per original device after local cooldown."""
from datetime import datetime
import getpass
import ipaddress
import json
import multiprocessing as mp
import os
from pathlib import Path
import queue
import sys
import time
import warnings
from zoneinfo import ZoneInfo

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

import requests
from fastcheck.proxy_pool import ApiFetcher
from archive.legacy.gsa_batch import EventLog, authenticate_process, proxy_fingerprint, read_accounts
from fastcheck.devices import DevicePool, PoolError, save_json


def public_exit(proxy):
    with requests.Session() as session:
        session.trust_env=False
        session.proxies={'http':proxy,'https':proxy}
        with session.get('https://api.ipify.org?format=json',timeout=(6,8),allow_redirects=False) as response:
            response.raise_for_status()
            return str(ipaddress.ip_address(response.json()['ip']))


def main():
    with warnings.catch_warnings():
        warnings.simplefilter('error',getpass.GetPassWarning)
        api_url=getpass.getpass('代理 API 地址（隐藏输入，不保存）：').strip()
    context=mp.get_context('spawn')
    stop=context.Event()
    stamp=datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y%m%d-%H%M%S')
    directory=ROOT/'logs'/f'recovery-{stamp}-{os.getpid()}'
    log=EventLog(directory,debug=True)
    pool=DevicePool(ROOT/'config/batch-device-pool','https://new.ani.sidestore.io',max_devices=10)
    accounts=read_accounts(ROOT/'账号.txt',max_accounts=20)
    originals={slot:pool.inspect(slot).get('device_id') for slot in (1,2,3)}
    results=[]
    try:
        health=pool.health()
        target=max(time.time(),health.get('provider_until',0))+5
        log('recovery_wait',until=datetime.fromtimestamp(target,ZoneInfo('Asia/Shanghai')).isoformat(),
            wait_seconds=round(max(0,target-time.time()),1))
        while time.time()<target:
            time.sleep(min(1,target-time.time()))
        # Original pairs from the first live run: row 1/device 1,
        # row 2/device 2 and row 5/device 3. No identity rotation or registration.
        for slot,row in ((1,1),(2,2),(3,5)):
            try:
                pool.allowed(slot)
            except PoolError:
                log('recovery_blocked',device=slot,reason='cooldown_active')
                break
            if pool.inspect(slot)['state']!='ready' or not originals[slot]:
                log('recovery_blocked',device=slot,reason='original_state_not_ready')
                break
            lease_start=time.monotonic()
            proxy=ApiFetcher(context,stop,debug_log=log.debug)(api_url,1)[0]
            try:
                exit_ip=public_exit(proxy)
            except Exception as exc:
                log('recovery_proxy_failed',device=slot,error_type=type(exc).__name__)
                results.append({'device':slot,'row':row,'status':'proxy_probe_failed',
                                'error_type':type(exc).__name__})
                break
            log('recovery_probe_start',device=slot,row=row,exit_ip=exit_ip,
                proxy_fingerprint=proxy_fingerprint(proxy),max_attempts=1)
            output=context.Queue(maxsize=2048)
            account=accounts[row-1]
            payload={'root':str(ROOT/'config/batch-device-pool'),'origin':pool.origin,'slot':slot,
                     'email':account.email,'password':account.password,'row':row,'attempt':1,
                     'proxy':proxy,'timeout':15,'prepare_timeout':30,'provider_direct':True,'debug':True,
                     'device_fingerprint':__import__('hashlib').sha256(originals[slot].encode()).hexdigest()[:12],
                     'spawn_at_perf':time.perf_counter()}
            process=context.Process(target=authenticate_process,args=(payload,output,stop),name=f'recovery-device-{slot}')
            process.start()
            deadline=min(time.monotonic()+40,lease_start+58)
            result=None
            try:
                while time.monotonic()<deadline:
                    try:message=output.get(timeout=.1)
                    except queue.Empty:
                        if not process.is_alive():break
                        continue
                    if '_debug' in message:
                        log.debug(message['_debug'])
                        continue
                    result=message
                    break
            finally:
                if result is None and process.is_alive():process.terminate()
                process.join(.5)
                if process.is_alive():process.kill();process.join(2)
                output.cancel_join_thread();output.close();process.close()
                account.password=''
            result=result or {'status':'error','kind':'timeout','stage':'worker'}
            result.update(device=slot,row=row,exit_ip=exit_ip,
                          original_identity_preserved=pool.inspect(slot).get('device_id')==originals[slot])
            results.append(result)
            log('recovery_probe_done',device=slot,row=row,status=result.get('status'),stage=result.get('stage'),
                apple_ec=result.get('apple_ec'),ms=result.get('total_ms',0),
                original_identity_preserved=result['original_identity_preserved'])
            if result.get('kind')=='ratelimit' or pool.health().get('provider_until',0)>time.time():
                log('recovery_stopped',reason='rate_limit_or_circuit_open')
                break
        report={'checked_at':datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),
                'device_registration_performed':False,'max_attempts_per_device':1,'results':results,
                'provider_cooldown_until':datetime.fromtimestamp(pool.health().get('provider_until',0),
                                                               ZoneInfo('Asia/Shanghai')).isoformat()}
        save_json(directory/'report.json',report)
        save_json(directory/'debug-summary.json',log.debug_summary())
        print('冷却复查报告：'+str(directory/'report.json'))
    finally:
        stop.set()
        for account in accounts:account.password=''


if __name__=='__main__':
    main()
