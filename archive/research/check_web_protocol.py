"""One protocol-one comparison, one account, one fresh proxy, zero retries."""
from datetime import datetime
import getpass
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

from fastcheck.diagnostics import SafeDiagnostics
from fastcheck.proxy_pool import ApiFetcher
from archive.legacy.gsa_batch import EventLog,read_accounts,proxy_fingerprint
from fastcheck.devices import save_json
from fastcheck.protocols import idmsa


def web_worker(account,proxy,output):
    diagnostics=SafeDiagnostics(output,role='web_authentication',row=account.row,
                                proxy_fingerprint=proxy_fingerprint(proxy))
    def trace(value):
        safe={key:value.get(key) for key in
              ('stage','http','auth_type','service_error_codes','has_session_headers','has_set_cookie')}
        output.put({'_trace':safe})
    try:
        result=idmsa.check(account.email,account.password,proxy,timeout=12,retries=0,
                           diagnostics=diagnostics,trace=trace)
        output.put({'result':{key:result.get(key) for key in ('status','kind','stage','ms')},
                    'dropped_debug_events':diagnostics.dropped})
    except Exception as exc:
        output.put({'result':{'status':'error','kind':'internal','stage':'worker','error_type':type(exc).__name__}})
    finally:
        account.password=''


def main():
    with warnings.catch_warnings():
        warnings.simplefilter('error',getpass.GetPassWarning)
        api_url=getpass.getpass('代理 API 地址（隐藏输入，不保存）：').strip()
    context=mp.get_context('spawn')
    stop=context.Event()
    stamp=datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y%m%d-%H%M%S')
    directory=ROOT/'logs'/f'web-protocol-{stamp}-{os.getpid()}'
    log=EventLog(directory,debug=True)
    accounts=read_accounts(ROOT/'账号.txt',max_accounts=20)
    account=accounts[0]
    for other in accounts[1:]:other.password=''
    log('web_comparison_start',row=account.row,backend='web',max_attempts=1,device_registration=False)
    lease_started=time.monotonic()
    proxy=ApiFetcher(context,stop,debug_log=log.debug)(api_url,1)[0]
    api_url=''
    output=context.Queue(maxsize=2048)
    process=context.Process(target=web_worker,args=(account,proxy,output),name='web-protocol-comparison')
    process.start()
    result=None
    traces=[]
    try:
        deadline=min(time.monotonic()+35,lease_started+58)
        while time.monotonic()<deadline:
            try:message=output.get(timeout=.1)
            except queue.Empty:
                if not process.is_alive():break
                continue
            if '_debug' in message:log.debug(message['_debug'])
            elif '_trace' in message:
                traces.append(message['_trace'])
                log('web_http_result',row=account.row,**message['_trace'])
            else:
                result=message
                break
    finally:
        if result is None and process.is_alive():process.terminate()
        process.join(.5)
        if process.is_alive():process.kill();process.join(2)
        output.cancel_join_thread();output.close();process.close()
        stop.set();account.password=''
    report={'backend':'web','ui_name':'协议一','row':1,'proxy_fingerprint':proxy_fingerprint(proxy),
            'device_registration_performed':False,'max_attempts':1,'http_responses':traces,
            'outcome':result or {'result':{'status':'error','kind':'timeout','stage':'worker'}},
            'endpoint':idmsa.IDMSA_INIT}
    save_json(directory/'report.json',report)
    save_json(directory/'debug-summary.json',log.debug_summary())
    print('协议一对照报告：'+str(directory/'report.json'))
    print(report['outcome'])


if __name__=='__main__':main()
