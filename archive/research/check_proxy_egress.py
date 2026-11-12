"""Compare direct and fresh proxy exits using the same Session as GSA.

No account file is read and no Apple authentication request is sent.
"""
from datetime import datetime
import argparse
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
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

from fastcheck.diagnostics import SafeDiagnostics
from fastcheck.proxy_pool import ApiFetcher
from archive.legacy.gsa_batch import EventLog, proxy_fingerprint
from fastcheck.devices import save_json
from fastcheck.protocols import gsa_status as gsa
from fastcheck.protocols.network_timing import NetworkTrace, TimingAdapter

ENDPOINTS = [('ipify','https://api.ipify.org?format=json'),
             ('aws','https://checkip.amazonaws.com/'),
             ('ipify_repeat','https://api.ipify.org?format=json')]


def probe_worker(label, proxy, output):
    diagnostics = SafeDiagnostics(output, role='egress', route=label,
        proxy_fingerprint=proxy_fingerprint(proxy) if proxy else 'direct')
    samples=[]
    for endpoint, url in ENDPOINTS:
        started=time.perf_counter()
        session=gsa._Session(proxy)
        trace=NetworkTrace(diagnostics)
        adapter=TimingAdapter(trace,pool_connections=1,pool_maxsize=1,max_retries=0)
        session._impl.mount('https://',adapter)
        try:
            with trace.request(endpoint):
                # Only this generic IP-check request uses the normal public CA
                # bundle. GSA's own Apple CA configuration is not modified.
                with session._impl.get(url,verify=True,timeout=(6,8),allow_redirects=False,stream=True) as response:
                    if response.status_code!=200:
                        samples.append({'endpoint':endpoint,'ok':False,'http':response.status_code})
                        continue
                    body=bytearray()
                    for block in response.iter_content(512):
                        body.extend(block)
                        if len(body)>2048:
                            raise ValueError('Response too large')
                    text=body.decode().strip()
                    candidate=json.loads(text)['ip'] if endpoint.startswith('ipify') else text
                    address=ipaddress.ip_address(candidate)
                    samples.append({'endpoint':endpoint,'ok':True,'exit_ip':str(address),
                                    'ip_version':address.version,'ms':round((time.perf_counter()-started)*1000,2)})
        except Exception as exc:
            samples.append({'endpoint':endpoint,'ok':False,'error_type':type(exc).__name__,
                            'error_detail':gsa._transport_detail(exc,proxy),
                            'ms':round((time.perf_counter()-started)*1000,2)})
        finally:
            session._impl.close()
    output.put({'route':label,'samples':samples,'dropped_debug_events':diagnostics.dropped})


def main():
    parser=argparse.ArgumentParser(description='Check exit routing without submitting account data')
    parser.add_argument('--compare-https',action='store_true',help='Also test TLS-to-proxy on the same endpoint')
    parser.add_argument('--count',type=int,choices=(1,2,3),default=3)
    options=parser.parse_args()
    with warnings.catch_warnings():
        warnings.simplefilter('error',getpass.GetPassWarning)
        api_url=getpass.getpass('代理 API 地址（隐藏输入，不保存）：').strip()
    context=mp.get_context('spawn')
    stop=context.Event()
    stamp=datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y%m%d-%H%M%S')
    directory=ROOT/'logs'/f'egress-{stamp}-{os.getpid()}'
    log=EventLog(directory,debug=True)
    log('egress_start',proxy_count=options.count,accounts_submitted=0,compare_https=options.compare_https)
    fetched_at=time.monotonic()
    addresses=ApiFetcher(context,stop,debug_log=log.debug)(api_url,options.count)
    api_url=''
    routes=[('direct',None),('invalid_proxy','http://127.0.0.1:1')]
    routes.extend((f'proxy_{number}',address) for number,address in enumerate(addresses,1))
    if options.compare_https:
        routes.extend((f'https_proxy_{number}',urlsplit(address)._replace(scheme='https').geturl())
                      for number,address in enumerate(addresses,1))
    active={}
    try:
        for label,address in routes:
            output=context.Queue(maxsize=2048)
            process=context.Process(target=probe_worker,args=(label,address,output),name=f'egress-{label}')
            process.start()
            active[label]=(process,output)
        results={}
        while active:
            for label,(process,output) in list(active.items()):
                result=None
                for _ in range(512):
                    try:message=output.get_nowait()
                    except queue.Empty:break
                    if '_debug' in message:log.debug(message['_debug'])
                    else:
                        result=message
                        break
                if result is None and time.monotonic()-fetched_at<50 and process.is_alive():
                    continue
                if result is None and not process.is_alive():
                    try:
                        message=output.get(timeout=.1)
                        if '_debug' in message:
                            log.debug(message['_debug'])
                            continue
                        result=message
                    except queue.Empty:pass
                if result is None:
                    result={'route':label,'samples':[],'error_type':'ProcessTimeoutOrExit'}
                    if process.is_alive():process.terminate()
                process.join(.5)
                if process.is_alive():
                    process.kill();process.join(2)
                output.cancel_join_thread();output.close();process.close()
                results[label]=result
                del active[label]
                log('egress_route_done',route=label,samples=result['samples'])
            time.sleep(.02)
        direct={row['exit_ip'] for row in results['direct']['samples'] if row.get('ok')}
        comparisons=[]
        for number,address in enumerate(addresses,1):
            label=f'proxy_{number}'
            samples=results[label]['samples']
            exits={row['exit_ip'] for row in samples if row.get('ok')}
            parsed=urlsplit(address)
            comparisons.append({'route':label,'proxy_endpoint':f'{parsed.hostname}:{parsed.port}',
                'proxy_fingerprint':proxy_fingerprint(address),'exit_ips':sorted(exits),
                'different_from_direct':bool(exits and direct and exits.isdisjoint(direct)),
                'same_ip_across_samples':len(exits)==1 and sum(bool(row.get('ok')) for row in samples)>=2})
        report={'measured_at':datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(),
            'scope':'Fresh leases; HTTPS exit probes via GSA Session transport; no Apple auth or account data.',
            'direct_exit_ips':sorted(direct),'comparisons':comparisons,'routes':results,
            'invalid_proxy_failed_closed':not any(row.get('ok') for row in results['invalid_proxy']['samples']),
            'note':'Previous batch leases have expired; these measurements do not retrospectively establish those exits.'}
        save_json(directory/'report.json',report)
        save_json(directory/'debug-summary.json',log.debug_summary())
        print('出口检测报告：'+str(directory/'report.json'))
        return 0
    finally:
        stop.set()
        for process,output in active.values():
            if process.is_alive():process.terminate()
            process.join(.5)
            if process.is_alive():process.kill();process.join(2)
            output.cancel_join_thread();output.close();process.close()


if __name__=='__main__':
    raise SystemExit(main())
