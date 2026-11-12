"""Short-lived DNS snapshots; slow public lookups can fall back to verified DoH.

The HTTPS URL and TLS hostname remain unchanged. No Apple address is pinned.
Custom/private hostnames use system DNS only.
"""
import ipaddress
import json
import queue
import socket
import threading
import time

HOST = "gsa.apple.com"
PORT = 443
MAX_AGE = 60
PUBLIC_PROVIDER_HOSTS={'new.ani.sidestore.io','mac-ani.jkcoxson.com','ani.iloader.app',
                       'ani3.stikstore.app','win.ani.sidestore.io'}
DOH_ENDPOINTS=('https://1.1.1.1/dns-query','https://1.0.0.1/dns-query')


def resolve_target(https_fallback=True,timeout=5,fallback_after=.3):
    return resolve_host(HOST,PORT,https_fallback=https_fallback,timeout=timeout,fallback_after=fallback_after)


def _system_snapshot(host,port,lookup):
    answers = lookup(host, port, 0, socket.SOCK_STREAM)
    if not answers:
        raise socket.gaierror("No GSA addresses")
    return {"host": host, "port": port, "answers": answers, "resolved_at": time.time(),'resolver':'system'}


def _https_snapshot(host,port,endpoint,timeout):
    import requests
    with requests.Session() as session:
        session.trust_env=False
        # IP-literal DoH bootstrap avoids waiting on the same slow DNS server.
        # Cloudflare's certificate is still checked against this IP SAN.
        with session.get(endpoint,params={'name':host,'type':'A'},headers={'Accept':'application/dns-json'},
                         timeout=timeout,verify=True,allow_redirects=False,stream=True) as response:
            if response.status_code!=200:
                raise OSError('HTTPS DNS lookup failed')
            raw=bytearray()
            for block in response.iter_content(4096):
                raw.extend(block)
                if len(raw)>65536:
                    raise OSError('HTTPS DNS response exceeds limit')
        payload=json.loads(raw)
    if not isinstance(payload,dict) or type(payload.get('Status')) is not int or payload['Status']!=0 or payload.get('TC'):
        raise OSError('Invalid HTTPS DNS response')
    questions=payload.get('Question',[])
    if not any(isinstance(q,dict) and q.get('name','').rstrip('.').lower()==host.lower() and q.get('type')==1 for q in questions):
        raise OSError('HTTPS DNS question mismatch')
    answers=[];ttls=[]
    for entry in payload.get('Answer',[])[:128]:
        if not isinstance(entry,dict) or entry.get('type')!=1:
            continue
        try:
            address=ipaddress.ip_address(entry.get('data',''))
        except ValueError:
            continue
        ttl=entry.get('TTL')
        if address.version!=4 or not address.is_global or type(ttl)is not int or ttl<=0:
            continue
        answers.append((socket.AF_INET,socket.SOCK_STREAM,socket.IPPROTO_TCP,'',(str(address),port)))
        ttls.append(ttl)
    if not answers:
        raise OSError('No public HTTPS DNS addresses')
    return {'host':host,'port':port,'answers':answers,'resolved_at':time.time(),
            'max_age':min(MAX_AGE,min(ttls)),'resolver':'https_cloudflare'}


def resolve_host(host,port=443,*,https_fallback=False,timeout=5,fallback_after=.3):
    lookup=socket.getaddrinfo
    if not https_fallback or host not in ({HOST}|PUBLIC_PROVIDER_HOSTS):
        return _system_snapshot(host,port,lookup)
    results=queue.Queue()
    def run(function,*args):
        try: results.put((True,function(*args)))
        except Exception as exc: results.put((False,exc))
    # Native getaddrinfo has no Python socket deadline. Daemon threads are
    # confined to the already killable preparation process and cannot hold exit.
    threading.Thread(target=run,args=(_system_snapshot,host,port,lookup),daemon=True).start()
    until=time.monotonic()+timeout
    failures=0
    try:
        ok,value=results.get(timeout=min(fallback_after,timeout))
        if ok:
            value['https_fallback']=True
            return value
        failures+=1
    except queue.Empty:
        pass
    for endpoint in DOH_ENDPOINTS:
        threading.Thread(target=run,args=(_https_snapshot,host,port,endpoint,min(3,timeout)),daemon=True).start()
    while time.monotonic()<until:
        try: ok,value=results.get(timeout=max(.001,until-time.monotonic()))
        except queue.Empty: break
        if ok:
            value['https_fallback']=True
            return value
        failures+=1
        if failures==3:
            raise OSError('GSA DNS lookup failed')
    raise TimeoutError('GSA DNS lookup deadline')


class CachedResolver:
    def __init__(self, snapshot):
        self.snapshot = snapshot

    def __call__(self, host, port, family=0, type=0, proto=0, flags=0):
        snapshot = self.snapshot
        age = time.time() - snapshot["resolved_at"]
        if (snapshot.get('https_fallback') and age>=min(MAX_AGE,snapshot.get('max_age',MAX_AGE))
                and host==snapshot['host'] and str(port)==str(snapshot['port']) and flags==0):
            snapshot=resolve_host(host,port,https_fallback=True)
            self.snapshot=snapshot
            age=time.time()-snapshot['resolved_at']
        if (host == snapshot["host"] and str(port) == str(snapshot["port"])
                and 0 <= age < min(MAX_AGE,snapshot.get('max_age',MAX_AGE)) and flags == 0):
            matching = [answer for answer in snapshot["answers"]
                        if (not family or answer[0] == family)
                        and (not type or answer[1] == type)
                        and (not proto or answer[2] == proto)]
            if matching:
                return matching
        return socket.getaddrinfo(host, port, family, type, proto, flags)
