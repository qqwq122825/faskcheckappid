"""Real offline file upload and dynamic distribution, 10,000 rows / 1,000 per unit.

Requires a controller :18770 and nodes :18771 / :18772. No external authentication.
"""
import hashlib
import json
from pathlib import Path
import sqlite3
import time

from verify_live import session

ROOT = Path(__file__).resolve().parents[1]
MASTER_URL = 'http://127.0.0.1:18770'
NODE_URLS = ['http://127.0.0.1:18771', 'http://127.0.0.1:18772']


def post(client, url, **kwargs):
    response = client.post(url, timeout=15, **kwargs)
    response.raise_for_status()
    return response.json()


def wait(predicate, timeout=30):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        value = predicate()
        if value:
            return value
        time.sleep(.2)
    raise RuntimeError('等待条件超时')


def main():
    master = session(MASTER_URL)
    nodes = [session(url) for url in NODE_URLS]
    identities = []
    for index, (client, url) in enumerate(zip(nodes, NODE_URLS), 1):
        state = client.get(url+'/api/state',timeout=5).json()
        if state['connected'] and state['config']['url'] == MASTER_URL:
            identities.append({'id':state['settings']['node_id'], 'name':state['settings']['name']})
        else:
            identity = post(master, MASTER_URL+'/api/nodes', json={'name':f'本地验证节点 {index:02d}', 'processes':2,'threads':2})
            post(client, url+'/api/connect', json={'url':MASTER_URL,'token':identity['token']})
            identities.append({'id':identity['id'],'name':identity['name']})
    wait(lambda: all(c.get(u+'/api/state',timeout=5).json()['connected'] for c,u in zip(nodes,NODE_URLS)))
    source = ROOT/'runtime'/'synthetic-10000.txt'
    with source.open('w',encoding='utf-8') as output:
        for n in range(1,10001):
            output.write(f'demo-{n:08d}@example.invalid----SYNTHETIC-ONLY-{n:08d}\n')
    receipt = {'rows':10000,'rows_per_unit':1000,'expected_units':10,'input_bytes':source.stat().st_size,
               'input_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),
               'executor':'offline-sha256','nodes':identities,'profiles':[]}
    for count in (1,2):
        for index, identity in enumerate(identities):
            post(master, MASTER_URL+f'/api/nodes/{identity["id"]}/config',
                 json={'processes':2,'threads':2,'enabled':index<count})
        wait(lambda: all(c.get(u+'/api/state',timeout=5).json()['settings']['enabled'] == (i<count)
                         for i,(c,u) in enumerate(zip(nodes,NODE_URLS))))
        begin = time.monotonic()
        with source.open('rb') as stream:
            job_id = post(master,MASTER_URL+'/api/jobs',data={'name':f'{count} 节点分片验收 · 10,000 / 1,000','unit_rows':'1000'},
                          files={'file':('synthetic-10000.txt',stream,'text/plain')})['id']
        def job_state():
            state = master.get(MASTER_URL+'/api/state',timeout=5).json()
            return next(j for j in state['jobs'] if j['id']==job_id),state
        wait(lambda: job_state()[0]['status']=='ready')
        prepared = time.monotonic()
        post(master,MASTER_URL+f'/api/jobs/{job_id}/start',json={})
        samples = []
        end = time.monotonic()+120
        while time.monotonic()<end:
            job,state = job_state()
            samples.append({'seconds':round(time.monotonic()-prepared,3),'processed':job['processed'],
                            'confirmed':job['confirmed'],'active':sum(n['active'] for n in state['nodes'])})
            if job['status']=='completed':
                break
            time.sleep(.2)
        else:
            raise RuntimeError('离线验收任务未完成')
        elapsed = time.monotonic()-prepared
        response = master.get(MASTER_URL+f'/api/jobs/{job_id}/results',timeout=15)
        response.raise_for_status()
        records = [json.loads(line) for line in response.iter_lines()]
        assert len(records)==10000
        assert [r['row'] for r in records]==list(range(1,10001))
        for record in records:
            n=record['row']
            raw=f'demo-{n:08d}@example.invalid----SYNTHETIC-ONLY-{n:08d}'.encode()
            assert record['sha256']==hashlib.sha256(raw).hexdigest()
        db=sqlite3.connect(ROOT/'runtime'/'controller'/'controller.sqlite3')
        distribution=[{'node_id':r[0],'units':r[1],'confirmed':r[2]} for r in db.execute(
            "SELECT node_id,COUNT(*),SUM(rows) FROM units WHERE job_id=? AND status='completed' GROUP BY node_id",(job_id,))]
        db.close()
        assert len(distribution)==count
        assert sum(x['units'] for x in distribution)==10
        def reconciled():
            total=0
            for client,url in zip(nodes,NODE_URLS):
                value=client.get(url+'/api/state',timeout=5).json()
                total+=sum(t['processed'] for t in value['tasks'] if t['job_id']==job_id and t['status']=='confirmed')
            return total==10000
        wait(reconciled)
        profile={'node_count':count,'job_id':job_id,'prepare_seconds':round(prepared-begin,3),
                 'execution_and_reporting_seconds':round(elapsed,3), 'confirmed':job['confirmed'],
                 'all_digests_verified':True,'unique_rows':len({r['row'] for r in records}),
                 'distribution':distribution,'samples':samples}
        receipt['profiles'].append(profile)
        (ROOT/'runtime'/'benchmark-two-nodes.json').write_text(json.dumps(receipt,ensure_ascii=False,indent=2))
        print(json.dumps({k:v for k,v in profile.items() if k!='samples'},ensure_ascii=False),flush=True)
    print('Two-node offline verification passed. No external service requests.',flush=True)


if __name__=='__main__':
    main()
