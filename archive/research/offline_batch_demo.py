"""Exercise the real spawn scheduler with ten synthetic accounts; no network."""
from datetime import datetime
import json
from pathlib import Path
import sys
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT/'tests'))

from archive.legacy.gsa_batch import Account, BatchRunner
from fastcheck.devices import save_json
from batch_fixtures import auth, register, block_network


def main():
    block_network()
    stamp = datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y%m%d-%H%M%S')
    directory = ROOT/'logs'/f'offline-demo-{stamp}'
    directory.mkdir(parents=True, mode=0o700, exist_ok=False)
    serial = [0]
    def fetch(url, count):
        result = []
        for _ in range(count):
            serial[0] += 1
            result.append(f'http://127.0.0.1:{20000+serial[0]}')
        return result
    accounts = [Account(n, f'offline{n}@example.invalid', 'synthetic-password') for n in range(1,11)]
    print('完全离线演示：十个合成账号，代理、设备和认证均使用替身，不访问 Apple。')
    runner = BatchRunner(accounts, 'https://supplier.example.invalid/?num=200', directory/'devices',
        directory/'events', proxy_fetcher=fetch, auth_target=auth, registration_target=register,
        registration_gap=0, startup_timeout=15, job_budget=8, min_proxy_life=1, debug=True)
    summary = runner.run()
    events = [json.loads(line) for line in runner.log.path.read_text().splitlines()]
    dispatches = [event for event in events if event['event']=='account_dispatch']
    third = next(event for event in dispatches if event['row']==3)
    first = [event for event in dispatches if event['row']==1]
    checks = {'offline':True, 'real_authentication_tested':False,
              'accounts_completed':sum(summary['results'].values()),
              'max_auth_concurrency':max(event['active_auth'] for event in dispatches),
              'third_account_device':third['device'], 'ready_devices_for_third_account':third['ready_devices'],
              'retry_keeps_device':len(first)==2 and first[0]['device']==first[1]['device'],
              'retry_keeps_valid_proxy':len(first)==2 and first[0]['proxy']==first[1]['proxy'],
              'retry_uses_new_process':len(first)==2 and first[0]['worker_pid']!=first[1]['worker_pid']}
    assert checks['accounts_completed']==10 and checks['max_auth_concurrency']==2
    assert checks['ready_devices_for_third_account']==2
    assert all(checks[key] for key in ('retry_keeps_device','retry_keeps_valid_proxy','retry_uses_new_process'))
    save_json(directory/'verification.json', checks)
    print('离线验证结果：'+str(directory/'verification.json'))


if __name__=='__main__':
    main()
