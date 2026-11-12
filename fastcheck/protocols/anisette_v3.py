"""Standard Anisette v3 client, with one persistent identity per provider.

Only device provisioning data goes to the provider. Apple credentials never
enter this module. All TLS and hostname checks remain enabled. On Windows,
client provisioning state is encrypted for the current user with DPAPI.
On POSIX, state is stored in a private directory with owner-only file access.
Protocol reference: SideStore/apple-private-apis, remote_anisette_v3.rs.
"""
import base64
from contextlib import contextmanager
import ctypes
from ctypes import wintypes
from datetime import datetime, timezone
import hashlib
import inspect
import json
import logging
import os
from pathlib import Path
import plistlib
import secrets
import ssl
import tempfile
import time
from urllib.parse import urlsplit, urlunsplit
import uuid

import requests

from .apple_trust import gsa_ca_file
from .timing import phase

LIMIT = 1024 * 1024
_IS_WINDOWS = os.name == 'nt'


class AnisetteError(Exception):
    def __init__(self, stage, http=0, code=None):
        self.stage, self.http, self.code = stage, http, code
        super().__init__(f"Anisette v3 {stage}; HTTP={http}; code={code}")


def _b64(data):
    return base64.b64encode(data).decode('ascii')


def _unb64(data):
    if not isinstance(data,str) or not 0 < len(data) <= LIMIT:
        raise AnisetteError('invalid_state')
    try:
        return base64.b64decode(data,validate=True)
    except (ValueError,TypeError):
        raise AnisetteError('invalid_state') from None


def _dpapi(data, decrypt=False):
    if os.name!='nt':
        raise AnisetteError('windows_state_store_required')
    class Blob(ctypes.Structure):
        _fields_=[('size',wintypes.DWORD),('data',ctypes.c_void_p)]
    buffer=ctypes.create_string_buffer(data)
    source=Blob(len(data),ctypes.cast(buffer,ctypes.c_void_p))
    output=Blob()
    library=ctypes.WinDLL('crypt32',use_last_error=True)
    function=library.CryptUnprotectData if decrypt else library.CryptProtectData
    function.argtypes=[ctypes.POINTER(Blob),ctypes.c_void_p if decrypt else ctypes.c_wchar_p,
                       ctypes.c_void_p,ctypes.c_void_p,ctypes.c_void_p,wintypes.DWORD,ctypes.POINTER(Blob)]
    function.restype=wintypes.BOOL
    description=None if decrypt else 'FastCheck GSA client provisioning state'
    if not function(ctypes.byref(source),description,None,None,None,1,ctypes.byref(output)):
        raise AnisetteError('state_decryption' if decrypt else 'state_encryption')
    kernel=ctypes.WinDLL('kernel32',use_last_error=True)
    kernel.LocalFree.argtypes=[ctypes.c_void_p]
    kernel.LocalFree.restype=ctypes.c_void_p
    try:
        return ctypes.string_at(output.data,output.size)
    finally:
        kernel.LocalFree(output.data)


class StateStore:
    def __init__(self, origin, directory=None):
        self.origin=origin
        default=(Path(os.environ.get('LOCALAPPDATA',Path.home()))/'FastCheckGSA'/'anisette-v3'
                 if _IS_WINDOWS else Path(__file__).resolve().parents[2]/'data'/'config'/'anisette-v3')
        base=Path(directory or os.environ.get('FASTCHECK_ANISETTE_STATE_DIR') or default)
        base.mkdir(mode=0o700,parents=True,exist_ok=True)
        if not _IS_WINDOWS:
            base.chmod(0o700)
        self.path=base/(hashlib.sha256(origin.encode()).hexdigest()[:24]+'.json')

    @contextmanager
    def lock(self, timeout):
        if _IS_WINDOWS:
            import msvcrt
        else:
            import fcntl
        path=self.path.with_suffix('.lock')
        descriptor=os.open(path,os.O_RDWR|os.O_CREAT|os.O_APPEND,0o600)
        with os.fdopen(descriptor,'a+b') as handle:
            if handle.tell()==0:
                handle.write(b'\0');handle.flush()
            until=time.monotonic()+timeout
            while True:
                try:
                    handle.seek(0)
                    if _IS_WINDOWS:
                        msvcrt.locking(handle.fileno(),msvcrt.LK_NBLCK,1)
                    else:
                        fcntl.flock(handle.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
                    break
                except OSError:
                    if time.monotonic()>=until:
                        raise AnisetteError('state_busy') from None
                    time.sleep(.05)
            try:
                yield
            finally:
                handle.seek(0)
                if _IS_WINDOWS:
                    msvcrt.locking(handle.fileno(),msvcrt.LK_UNLCK,1)
                else:
                    fcntl.flock(handle.fileno(),fcntl.LOCK_UN)

    def load(self):
        if not self.path.exists():
            state={'identifier':_b64(secrets.token_bytes(16)),'adi_pb':None}
            self.save(state)
            return state
        data=json.loads(self.path.read_text('utf-8'))
        if data.get('origin')!=self.origin or data.get('schema')!=(1 if _IS_WINDOWS else 2):
            raise AnisetteError('state_origin')
        if _IS_WINDOWS:
            state=json.loads(_dpapi(_unb64(data['protected_state']),decrypt=True))
        else:
            self.path.chmod(0o600)
            state=data['state']
        if len(_unb64(state['identifier']))!=16:
            raise AnisetteError('invalid_identifier')
        if state.get('adi_pb') is not None:
            _unb64(state['adi_pb'])
        return state

    def save(self, state):
        if _IS_WINDOWS:
            encrypted=_dpapi(json.dumps(state,separators=(',',':')).encode())
            data={'schema':1,'origin':self.origin,'protected_state':_b64(encrypted)}
        else:
            data={'schema':2,'origin':self.origin,'state':state}
        temporary=None
        try:
            with tempfile.NamedTemporaryFile(mode='w',encoding='utf-8',dir=self.path.parent,
                                             prefix='.anisette-',suffix='.tmp',delete=False) as handle:
                temporary=Path(handle.name)
                json.dump(data,handle,separators=(',',':'))
                handle.flush();os.fsync(handle.fileno())
            os.replace(temporary,self.path)
            if not _IS_WINDOWS:
                self.path.chmod(0o600)
        finally:
            if temporary is not None and temporary.exists():
                temporary.unlink()


class Client:
    def __init__(self, origin, timeout=20, store=None,resolver=None,client_info=None,proxy=None,diagnostics=None,provider_direct=False):
        parsed=urlsplit(origin)
        if parsed.username or parsed.password or parsed.query or parsed.fragment or not parsed.hostname:
            raise AnisetteError('invalid_origin')
        if parsed.scheme!='https' and not (parsed.scheme=='http' and parsed.hostname in ('127.0.0.1','localhost','::1')):
            raise AnisetteError('invalid_origin')
        self.origin=origin.rstrip('/')
        self.timeout=timeout
        self.deadline=time.monotonic()+timeout
        self.session=requests.Session()
        self.session.trust_env=False
        self.proxy=proxy
        self.diagnostics=diagnostics
        self.provider_direct=provider_direct
        if proxy is not None:
            self.session.proxies={'http':proxy,'https':proxy}
        self.store=store or StateStore(self.origin)
        self.info=client_info
        if resolver is not None:
            from .network_timing import NetworkTrace,TimingAdapter
            self.session.mount('https://',TimingAdapter(NetworkTrace(None,resolver=resolver)))
            self.session.mount('http://',TimingAdapter(NetworkTrace(None,resolver=resolver)))

    def close(self):
        self.session.close()

    def remaining(self):
        left=self.deadline-time.monotonic()
        if left<=0:
            raise AnisetteError('deadline')
        return max(.1,left)

    def _request(self, method, url, stage, apple=False, **kwargs):
        if apple:
            parsed=urlsplit(url)
            if parsed.scheme!='https' or not parsed.hostname or not parsed.hostname.endswith('.apple.com') or parsed.username or parsed.password:
                raise AnisetteError('invalid_apple_origin')
        if self.provider_direct and not apple:
            kwargs['proxies']={'http':'','https':''}
        with self.session.request(method,url,timeout=self.remaining(),allow_redirects=False,
                                  verify=gsa_ca_file() if apple else True,stream=True,**kwargs) as response:
            if response.status_code!=200:
                raise AnisetteError(stage,http=response.status_code)
            body=bytearray()
            for block in response.iter_content(16384):
                body.extend(block)
                if len(body)>LIMIT:
                    raise AnisetteError('response_size')
                self.remaining()
            return bytes(body)

    def client_info(self):
        if self.info is None:
            data=json.loads(self._request('GET',self.origin+'/v3/client_info','client_info'))
            if not isinstance(data,dict) or not all(isinstance(data.get(k),str) and 0<len(data[k])<=4096 and '\r' not in data[k] and '\n' not in data[k]
                       for k in ('client_info','user_agent')):
                raise AnisetteError('client_info_format')
            self.info=data
        return self.info

    def _apple_headers(self, state):
        info=self.client_info()
        identifier=_unb64(state['identifier'])
        return {'X-Mme-Client-Info':info['client_info'],'User-Agent':info['user_agent'],
                'Content-Type':'text/x-xml-plist','X-Apple-I-MD-LU':hashlib.sha256(identifier).hexdigest(),
                'X-Mme-Device-Id':str(uuid.UUID(bytes=identifier)),
                'X-Apple-I-Client-Time':datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
                'X-Apple-I-TimeZone':'UTC','X-Apple-Locale':'en_US'}

    def _plist(self, url, state, fields):
        raw=self._request('POST',url,'provisioning',apple=True,headers=self._apple_headers(state),
                          data=plistlib.dumps({'Header':{},'Request':fields}))
        data=plistlib.loads(raw)
        response=data.get('Response') if isinstance(data,dict) else None
        if not isinstance(response,dict):
            raise AnisetteError('provisioning_format')
        status=response.get('Status',{})
        code=status.get('ec') if isinstance(status,dict) else None
        if code is not None and code!=0:
            raise AnisetteError('provisioning',code=code if type(code)is int else None)
        return response

    def _provision(self, state):
        from websockets.sync.client import connect
        raw=self._request('GET','https://gsa.apple.com/grandslam/GsService2/lookup',
                          'lookup',apple=True,headers=self._apple_headers(state))
        data=plistlib.loads(raw)
        urls=data.get('urls') if isinstance(data,dict) else None
        if not isinstance(urls,dict):
            raise AnisetteError('lookup_format')
        parsed=urlsplit(self.origin)
        ws_url=urlunsplit((('wss' if parsed.scheme=='https' else 'ws'),parsed.netloc,
                           parsed.path.rstrip('/')+'/v3/provisioning_session','',''))
        logger=logging.Logger('fastcheck.anisette.websocket')
        logger.disabled=True  # WebSocket debug frames would contain provisioning data.
        options={'open_timeout':self.remaining(),'close_timeout':1,'max_size':LIMIT,
                 'compression':None,'logger':logger}
        if 'proxy' in inspect.signature(connect).parameters:
            options['proxy']=None if self.provider_direct else self.proxy
        elif self.proxy is not None and not self.provider_direct:
            raise AnisetteError('websocket_proxy_unsupported')
        if parsed.scheme=='https':
            options['ssl']=ssl.create_default_context()
        with phase(self.diagnostics,'anisette_ws_connect'):
            connection=connect(ws_url,**options)
        with connection as websocket:
            for _ in range(8):
                with phase(self.diagnostics,'anisette_ws_receive'):
                    payload=json.loads(websocket.recv(timeout=self.remaining()))
                result=payload.get('result')
                if result=='GiveIdentifier':
                    reply={'identifier':state['identifier']}
                elif result=='GiveStartProvisioningData':
                    response=self._plist(urls['midStartProvisioning'],state,{})
                    reply={'spim':response['spim']}
                elif result=='GiveEndProvisioningData':
                    response=self._plist(urls['midFinishProvisioning'],state,{'cpim':payload['cpim']})
                    reply={k:response[k] for k in ('ptm','tk')}
                elif result=='ProvisioningSuccess':
                    _unb64(payload['adi_pb'])
                    state['adi_pb']=payload['adi_pb']
                    # Persist success before asking for headers; a normal socket
                    # close cannot rotate or discard this working identity.
                    with phase(self.diagnostics,'device_state_save'):
                        self.store.save(state)
                    return
                else:
                    raise AnisetteError('provisioning_message')
                with phase(self.diagnostics,'anisette_ws_send'):
                    websocket.send(json.dumps(reply,separators=(',',':')))
        raise AnisetteError('provisioning_incomplete')

    def headers(self):
        self.client_info()
        with self.store.lock(self.remaining()):
            with phase(self.diagnostics,'device_state_load'):
                state=self.store.load()
            if state.get('adi_pb') is None:
                self._provision(state)
        raw=self._request('POST',self.origin+'/v3/get_headers','get_headers',
                          json={'identifier':state['identifier'],'adi_pb':state['adi_pb']})
        result=json.loads(raw)
        keys=('X-Apple-I-MD','X-Apple-I-MD-M','X-Apple-I-MD-RINFO')
        if not isinstance(result,dict) or result.get('result')!='Headers' or not all(isinstance(result.get(k),str) and result[k] for k in keys):
            raise AnisetteError('get_headers_format')
        headers=self._apple_headers(state)
        headers.update({k:result[k] for k in keys})
        headers.update({'X-Apple-I-SRL-NO':'0','_fastcheck_v3_origin':self.origin})
        return headers
