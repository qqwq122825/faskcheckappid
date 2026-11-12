"""Review the current parser with public documentation addresses; no connections."""
import json
from pathlib import Path
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from fastcheck.protocols.proxy_api import normalize,parse


def review():
    address='192.0.2.10:8217';url='socks5h://'+address
    cases=[('normalize explicit scheme',normalize,url,url),
           ('bare defaults to HTTP',parse,address,['http://'+address]),
           ('plain text preserves explicit scheme',parse,url,[url]),
           ('JSON strings preserve explicit scheme',parse,json.dumps([url]),[url]),
           ('JSON server URL preserves explicit scheme',parse,json.dumps([{'server':url}]),[url])]
    rows=[]
    for name,method,value,expected in cases:
        actual=method(value)
        rows.append({'case':name,'expected':expected,'actual':actual,'passed':actual==expected})
    return {'scope':'offline format review only; production parser unchanged','network_requests':0,'cases':rows,
            'passed':sum(row['passed'] for row in rows),'failed':sum(not row['passed'] for row in rows)}


if __name__=='__main__':
    result=review();path=ROOT/'data/validation/proxy-format-review.json'
    path.parent.mkdir(parents=True,exist_ok=True);path.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(result,ensure_ascii=False))
    raise SystemExit(bool(result['failed']))
