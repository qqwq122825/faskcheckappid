"""Spawn-safe offline substitutes. They never contact Apple or a proxy."""
import base64
import socket
import time

from fastcheck.devices import DevicePool


def block_network():
    def forbidden(*args, **kwargs):
        raise AssertionError('Network forbidden in offline child')
    socket.socket.connect = forbidden


def register(payload, output):
    block_network()
    pool = DevicePool(payload['root'], payload['origin'], max_devices=10)
    slot = payload['slot']
    time.sleep(.03 if slot <= 2 else 2)
    store = pool.store(slot)
    state = store.load()
    state['adi_pb'] = base64.b64encode(b'offline-device-state').decode()
    store.save(state)
    output.put({'ok':True,'ms':30})


def auth(payload, output, stop):
    block_network()
    time.sleep(.1)
    pool = DevicePool(payload['root'], payload['origin'], max_devices=10)
    if payload['row'] == 1 and payload['attempt'] == 1:
        result = {'status':'error','kind':'protocol','stage':'init','apple_ec':-20970}
    elif payload['row'] == 2:
        result = {'status':'wrong_password','stage':'complete','apple_ec':-22406}
    else:
        result = {'status':'need_2fa','stage':'complete','password_verified':True}
    pool.observe(payload['slot'], result)
    output.put(dict(result, total_ms=100))


def limited(payload, output, stop):
    block_network()
    output.put({'status':'error','kind':'ratelimit','stage':'init'})


def hang(payload, output, stop):
    block_network()
    time.sleep(20)


def finish(payload, output, stop):
    block_network()
    output.put({'status':'need_2fa','stage':'complete','password_verified':True})


def api_ready(url, count, output):
    block_network()
    output.put({'addresses':[f'http://127.0.0.1:{10000+n}' for n in range(count)]})


def api_hang(url, count, output):
    block_network()
    time.sleep(20)


def supervisor_retry(payload, output, stop):
    block_network()
    if payload['row'] == 1 and payload['attempt'] == 1:
        return {'status':'error','kind':'network','stage':'init'}
    if payload['row'] == 2:
        return {'status':'error','kind':'network','stage':'complete'}
    if payload['row'] == 3:
        return {'status':'error','kind':'protocol','stage':'init','apple_ec':-20970}
    return {'status':'need_2fa','stage':'complete','password_verified':True}


def supervisor_retry_priority(payload,output,stop):
    block_network()
    if payload['row']==1 and payload['attempt']==1:
        return {'status':'error','kind':'network','stage':'init'}
    time.sleep(.18)
    return {'status':'need_2fa','stage':'complete','password_verified':True}


def supervisor_limited(payload, output, stop):
    block_network()
    return {'status':'error','kind':'ratelimit','stage':'init','http':429}


def supervisor_hang(payload, output, stop):
    block_network()
    time.sleep(100)
    return {'status':'error'}


def supervisor_crash(payload, output, stop):
    block_network()
    __import__('os')._exit(7)


def supervisor_debug(payload, output, stop):
    block_network()
    from fastcheck.diagnostics import SafeDiagnostics
    diagnostic=SafeDiagnostics(output,role='authentication',row=payload['row'])
    diagnostic({'event':'phase_end','phase':'offline_test','ms':1,
                'password':'unwanted-diagnostic-secret','headers':{'cookie':'unwanted-cookie'}})
    return {'status':'wrong_password','stage':'complete','apple_ec':-22406,
            'password':payload['password'],'email':payload['email'],
            'detail':'unwanted-result-secret','headers':{'cookie':'unwanted-cookie'}}


def debug_auth(payload, output, stop):
    block_network()
    from fastcheck.diagnostics import SafeDiagnostics
    diagnostics = SafeDiagnostics(output, role='authentication', row=payload['row'], device=payload['slot'])
    for n in range(100):
        diagnostics({'event':'phase_end','phase':'synthetic_phase','ms':float(n),
                     'password':'must-not-log','headers':{'secret':'must-not-log'}})
    output.put({'status':'need_2fa','stage':'complete','password_verified':True,
                'dropped_debug_events':diagnostics.dropped})


def retry_then_limit(payload, output, stop):
    block_network()
    if payload['row']==1:
        output.put({'status':'error','kind':'protocol','stage':'init','apple_ec':-20970})
    else:
        time.sleep(.4)
        output.put({'status':'error','kind':'ratelimit','stage':'init'})


def study_order_unit(payload,output,stop):
    block_network()
    for position,account in enumerate(payload['accounts'],1):
        output.put({'_event':{'event':'study_auth_start','row':account['row'],
                             'device':payload['slot'],'position':position,'study_phase':payload['phase']}})
        time.sleep(.03)
        refused=account['row']==1 and payload['phase']=='paired_first_pass'
        output.put({'_result':{'row':account['row'],'device':payload['slot'],'position':position,
            'study_phase':payload['phase'],'attempted':True,
            'status':'error' if refused else 'need_2fa','stage':'init' if refused else 'complete',
            'apple_ec':-20970 if refused else 0,'password_verified':not refused}})
    output.put({'done':True})


def study_limited_unit(payload,output,stop):
    block_network()
    account=payload['accounts'][0]
    output.put({'_result':{'row':account['row'],'device':payload['slot'],'position':1,
        'study_phase':payload['phase'],'attempted':True,'status':'error','kind':'ratelimit','stage':'init'}})
    stop.set()
    output.put({'done':True})
