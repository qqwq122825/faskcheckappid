"""Exercise real HTTP, one local node and 10,000 synthetic records.

Run after starting controller :18770 and node :18771; outputs contain no tokens.
"""
import hashlib
import json
from pathlib import Path
import time

import requests


def session(url):
    client = requests.Session()
    client.trust_env = False
    response = client.get(url + '/api/session', timeout=5)
    response.raise_for_status()
    client.headers['X-CSRF-Token'] = response.json()['csrf']
    return client


def main():
    controller_url, node_url = 'http://127.0.0.1:18770', 'http://127.0.0.1:18771'
    master, worker = session(controller_url), session(node_url)
    response = master.post(controller_url + '/api/nodes', json={'name':'本地验证节点 01','processes':2,'threads':2}, timeout=5)
    response.raise_for_status()
    identity = response.json()
    response = worker.post(node_url + '/api/connect', json={'url':controller_url,'token':identity['token']}, timeout=5)
    response.raise_for_status()
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        state = master.get(controller_url + '/api/state', timeout=5).json()
        if state['totals']['online']:
            break
        time.sleep(.25)
    else:
        raise RuntimeError('node did not connect')
    response = master.post(controller_url + '/api/demo', json={'rows':10000,'unit_rows':500}, timeout=5)
    response.raise_for_status()
    job_id = response.json()['id']
    for _ in range(100):
        state = master.get(controller_url + '/api/state', timeout=5).json()
        job = next(j for j in state['jobs'] if j['id'] == job_id)
        if job['status'] == 'ready':
            break
        time.sleep(.1)
    response = master.post(controller_url + f'/api/jobs/{job_id}/start', json={}, timeout=5)
    response.raise_for_status()
    samples = []
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        state = master.get(controller_url + '/api/state', timeout=5).json()
        job = next(j for j in state['jobs'] if j['id'] == job_id)
        samples.append({'at':time.time(),'processed':job['processed'],'confirmed':job['confirmed'],
                        'active':sum(n['active'] for n in state['nodes']),'rate':state['totals']['rate']})
        if job['status'] == 'completed':
            break
        time.sleep(.5)
    else:
        raise RuntimeError('job did not complete')
    response = master.get(controller_url + f'/api/jobs/{job_id}/results', timeout=20)
    response.raise_for_status()
    records = [json.loads(line) for line in response.iter_lines()]
    assert len(records) == 10000
    assert [r['row'] for r in records] == list(range(1,10001))
    for row in records:
        raw = f'demo-{row["row"]:08d}@example.invalid----SYNTHETIC-ONLY-{row["row"]:08d}'.encode()
        assert row['sha256'] == hashlib.sha256(raw).hexdigest()
        assert row['bytes'] == len(raw)
    for _ in range(40):
        node = worker.get(node_url + '/api/state', timeout=5).json()
        if node['confirmed'] == 10000:
            break
        time.sleep(.25)
    assert node['confirmed'] == 10000 and node['outbox'] == 0
    receipt = {'job_id':job_id,'synthetic_rows':10000,'confirmed_rows':job['confirmed'],
               'unit_count':job['units'],'node_confirmed':node['confirmed'],
               'node_outbox':node['outbox'],'all_digests_verified':True,'unique_rows':len({r['row'] for r in records}),
               'elapsed_seconds':round(samples[-1]['at']-samples[0]['at'],2),'samples':samples}
    target = Path(__file__).resolve().parents[1] / 'runtime' / 'verification.json'
    target.write_text(json.dumps(receipt,ensure_ascii=False,indent=2))
    print(json.dumps({k:v for k,v in receipt.items() if k!='samples'},ensure_ascii=False,indent=2),flush=True)


if __name__ == '__main__':
    main()
