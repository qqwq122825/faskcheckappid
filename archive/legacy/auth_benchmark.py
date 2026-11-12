"""One attempt per owned account and protocol, matched concurrency, no retry."""
import argparse
from collections import Counter
from dataclasses import replace
from datetime import datetime
import getpass
import json
import math
import os
from pathlib import Path
import statistics
import sys
import warnings
from zoneinfo import ZoneInfo

from fastcheck.scheduler import Settings, Supervisor
from archive.legacy.gsa_batch import Account, read_accounts
from fastcheck.devices import save_json
from fastcheck.protocols import gsa_status as gsa
from fastcheck.settings_store import prompt_api
from fastcheck.cli import read_integer

ROOT = Path(__file__).resolve().parent


def distribution(values):
    values = sorted(v for v in values if isinstance(v, (int, float)))
    if not values:
        return {'samples': 0}
    return {'samples': len(values), 'mean_ms': round(statistics.mean(values), 2),
            'p50_ms': round(statistics.median(values), 2),
            'p95_ms': round(values[math.ceil(len(values)*.95)-1], 2),
            'max_ms': round(values[-1], 2)}


def summarize(directory, summary):
    results = [json.loads(v) for v in (directory/'results.jsonl').read_text().splitlines()]
    events = [json.loads(v) for v in (directory/'events.jsonl').read_text().splitlines()]
    debug_path = directory/'debug.jsonl'
    debug = [json.loads(v) for v in debug_path.read_text().splitlines()] if debug_path.exists() else []
    start = next(datetime.fromisoformat(v['at']) for v in events if v['event']=='supervisor_start')
    dispatches = [datetime.fromisoformat(v['at']) for v in events if v['event']=='task_dispatch']
    completions = [datetime.fromisoformat(v['at']) for v in events if v['event']=='task_done' and v['attempts']>0]
    span = (max(completions)-min(dispatches)).total_seconds() if dispatches and completions else None
    init = [v['ms'] for v in debug if v.get('role')=='task_authentication'
            and v.get('event')=='phase_end' and v.get('phase')=='http_init']
    complete = [v['ms'] for v in debug if v.get('role')=='task_authentication'
                and v.get('event')=='phase_end' and v.get('phase')=='http_complete']
    http_by_row = {}
    http_by_phase = {'http_init': Counter(), 'http_complete': Counter()}
    response_latencies = {'http_init': [], 'http_complete': []}
    for event in debug:
        phase = event.get('phase')
        if event.get('role') != 'task_authentication' or event.get('event') != 'phase_end' or phase not in http_by_phase:
            continue
        code = event.get('http', 0)
        http_by_row.setdefault(event['row'], {})[phase] = code
        http_by_phase[phase][str(code)] += 1
        if isinstance(code, int) and code > 0:
            response_latencies[phase].append(event['ms'])
    outcomes = Counter()
    final_response_latencies = []
    for row in results:
        phase = 'http_'+str(row.get('stage'))
        code = row.get('http', http_by_row.get(row['row'], {}).get(phase))
        outcomes[f"{row.get('stage')}/ec={row.get('apple_ec')}/http={code}"] += 1
        if isinstance(code, int) and code > 0 and isinstance(row.get('total_ms'), (int, float)):
            final_response_latencies.append(row['total_ms'])
    return {'summary': summary,
            'pre_first_dispatch_seconds': round((min(dispatches)-start).total_seconds(),3) if dispatches else None,
            'first_dispatch_to_last_completion_seconds': round(span,3) if span is not None else None,
            'worker_attempt_latency': distribution([r.get('total_ms') for r in results]),
            'worker_latency_with_final_http_response': distribution(final_response_latencies),
            'init_latency': distribution(init), 'complete_latency': distribution(complete),
            'init_http_response_latency': distribution(response_latencies['http_init']),
            'complete_http_response_latency': distribution(response_latencies['http_complete']),
            'http_counts_by_phase': {phase: dict(counts) for phase, counts in http_by_phase.items()},
            'responses_by_stage_ec_http': dict(outcomes),
            'max_attempts_observed': max((r.get('attempts',0) for r in results),default=0),
            'result_rows': len(results)}


def rebuild(directory):
    """Reanalyze persisted safe metadata; performs no authentication or network."""
    directory = Path(directory)
    result = json.loads((directory/'comparison.json').read_text())
    rowsets = {}
    for protocol in result['protocols']:
        output = directory/protocol
        summary = json.loads((output/'summary.json').read_text())
        result['protocols'][protocol] = summarize(output, summary)
        rowsets[protocol] = {row['row']:row for row in map(json.loads,(output/'results.jsonl').read_text().splitlines())}
    if set(rowsets) == {'web','gsa'}:
        shared = sorted(set(rowsets['web']) & set(rowsets['gsa']))
        paired = Counter()
        for row in shared:
            web, gsa_row = rowsets['web'][row], rowsets['gsa'][row]
            paired[f"gsa_ec={gsa_row.get('apple_ec')};web_http={web.get('http')};web_ec={web.get('apple_ec')}"] += 1
        result['paired_rows'] = len(shared)
        result['paired_outcomes'] = dict(paired)
    save_json(directory/'comparison.json', result)
    return result


def compare(accounts, api_url, root, directory, *, processes=2, threads=5,
            supervisor_factory=Supervisor, proxy_auth=''):
    concurrency = processes*threads
    if concurrency > 20:
        raise ValueError('公平对照并发最多 20，与现有设备池容量上限一致。')
    settings = Settings(processes=processes, threads=threads, device_capacity=concurrency,
                        warmup_devices=concurrency, max_attempts=1, debug=True)
    result = {'accounts': len(accounts), 'actual_concurrency_limit': concurrency,
              'processes': processes, 'threads_per_process': threads,
              'automatic_retries': 0, 'order': ['web','gsa'], 'protocols': {},
              'limitations': ['Sequential runs use different live proxy leases and time windows.',
                              'A faster refusal is not proof of faster successful authentication.',
                              'Preparation includes worker/IP readiness; GSA also waits for all comparison devices.']}
    try:
        for protocol in result['order']:
            copies = [Account(a.row, a.email, a.password) for a in accounts]
            output = directory/protocol
            try:
                supervisor = supervisor_factory(copies, api_url, root, output,
                    replace(settings, protocol=protocol), proxy_auth=proxy_auth)
                summary = supervisor.run()
                result['protocols'][protocol] = summarize(output, summary)
                save_json(directory/'comparison.json',result)
                if summary['stop_reason']:
                    result['stopped_after'] = protocol
                    result['stop_reason'] = summary['stop_reason']
                    break
            finally:
                for account in copies:
                    account.password = ''
    finally:
        for account in accounts:
            account.password = ''
        save_json(directory/'comparison.json',result)
    return result


def main():
    parser = argparse.ArgumentParser(description='网页/GSA 零重试耗时对照')
    parser.add_argument('--accounts')
    parser.add_argument('--processes', type=int)
    parser.add_argument('--threads', type=int)
    args = parser.parse_args()
    accounts = []
    try:
        if gsa.mock_enabled():
            raise ValueError('请清除 FASTCHECK_MOCK；测速入口不接受模拟认证。')
        account_path = args.accounts or input(f'账号文件路径 [{ROOT/"账号.txt"}]：').strip().strip('"\'') or str(ROOT/'账号.txt')
        accounts = read_accounts(account_path,max_accounts=100)
        print(f'已读取 {len(accounts)} 个账号。公平对照要求两边相同实际并发，当前最多 20。')
        if args.processes is None:
            args.processes = read_integer('工作进程数',2,1,8)
        if args.threads is None:
            args.threads = read_integer('每个进程的线程数',5,1,32)
        # Validate concurrency before accepting API credentials or opening any session.
        if args.processes*args.threads>20:
            raise ValueError('公平对照并发最多 20。')
        Settings(processes=args.processes,threads=args.threads)
        with warnings.catch_warnings():
            warnings.simplefilter('error',getpass.GetPassWarning)
            api_url = prompt_api(ROOT)
        stamp = datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y%m%d-%H%M%S')
        directory = ROOT/'logs'/f'benchmark-{stamp}-{os.getpid()}'
        print(f'读取 {len(accounts)} 个自有账号；每通道一次；零重试；共同并发 {args.processes*args.threads}。',flush=True)
        result = compare(accounts,api_url,ROOT,directory,processes=args.processes,threads=args.threads)
        api_url = ''
        print('对照报告：'+str(directory/'comparison.json'))
        for protocol,value in result['protocols'].items():
            print(f'{protocol}：总耗时 {value["summary"]["elapsed_seconds"]} 秒；'
                  f'处理区间 {value["first_dispatch_to_last_completion_seconds"]} 秒；'
                  f'单账号中位数 {value["worker_attempt_latency"].get("p50_ms", "未知")} 毫秒。')
        return 1 if result.get('stop_reason') else 0
    except (KeyboardInterrupt,EOFError):
        print('\n已取消。')
        return 130
    except ValueError as exc:
        print(str(exc),file=sys.stderr)
        return 2
    except Exception as exc:
        print('测速未完成：'+type(exc).__name__,file=sys.stderr)
        return 2
    finally:
        for account in accounts:
            account.password = ''


if __name__=='__main__':
    raise SystemExit(main())
