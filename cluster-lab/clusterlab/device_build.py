"""节点侧身份池初始化：不跑任务也能把设备注册好。"""
from __future__ import annotations

import base64
import json
import multiprocessing as mp
import os
from pathlib import Path
import queue
import re
import shutil
import threading
import time

from .common import atomic_json, read_json
from .executor import (_load_fastcheck, _reset_incomplete_device_slots,
                       record_device_registration)

REGISTRATION_TIMEOUT = 20
REGISTRATION_PROXY_ATTEMPTS = 2


def device_label(device_id):
    """Return a short, non-secret Base64 prefix for one persisted device."""
    raw = str(device_id or '').encode('utf-8')
    if not raw:
        return ''
    return 'B64:' + base64.urlsafe_b64encode(raw).decode('ascii').rstrip('=')[:8]


def proxy_endpoint(value):
    """Return a credential-free proxy endpoint suitable for status displays."""
    text = str(value or '').strip()
    if '://' in text:
        text = text.split('://', 1)[1]
    if '@' in text:
        text = text.rsplit('@', 1)[1]
    return text[:96]


class DeviceBuilder:
    def __init__(self, node):
        self.node = node
        self.lock = threading.Lock()
        self.thread = None
        self.provision_process = None
        self.stop = threading.Event()
        self.reserved = set()
        self.state = {'running': False, 'capacity': 0, 'ready': 0, 'built': 0,
                      'failed': 0, 'current': 0, 'message': '', 'started_at': None,
                      'proxy_status': 'idle', 'proxy_ip': '', 'slot_errors': {}}

    def slot_limit(self):
        """Initialization may fill every slot allowed by node concurrency."""
        return max(1, int(self.node.max_processes) * int(self.node.max_threads))

    def snapshot(self):
        root = self.node.root / 'device-pool'
        ready = 0
        slots = []
        usage = self.usage()
        with self.lock:
            data = dict(self.state)
            data['slot_errors'] = dict(data.get('slot_errors') or {})
        persisted_capacity = 0
        configured_capacity = 0
        try:
            configured_capacity = max(
                0, int(read_json(root / 'target.json', {}).get('capacity') or 0))
        except (TypeError, ValueError):
            configured_capacity = 0
        try:
            DevicePool, _, _ = _load_fastcheck()
            # The in-memory builder state resets on a service restart. Discover
            # the persisted slots so a task can still use the node's complete
            # device pool before any new initialization request is made.
            pool = DevicePool(str(root), self.node.origin,
                              max_devices=max(self.slot_limit(), configured_capacity, 1))
            device_root = Path(getattr(pool, 'base', root)) / 'devices'
            existing = [int(path.name) for path in device_root.iterdir()
                        if path.is_dir() and path.name.isdigit()
                        and 1 <= int(path.name) <= self.slot_limit()
                        ] if device_root.is_dir() else []
            persisted_capacity = max(existing, default=0)
            limit = max(1, int(data.get('capacity') or 0), persisted_capacity,
                        configured_capacity)
            for slot in range(1, limit + 1):
                inspected = pool.inspect(slot)
                state = inspected['state']
                lease = self._active_task_lease(pool, slot)
                if state == 'ready':
                    ready += 1
                    if lease:
                        state = 'in_use'
                elif state in ('missing', 'pending', 'invalid') and lease:
                    # Work threads register missing identities under the same
                    # exclusive slot lease used for requests.  The standalone
                    # builder is not running in that path, so infer the live
                    # registration from its process-safe lease marker.
                    state = 'registering'
                elif data.get('running') and slot == data.get('current'):
                    state = 'registering'
                elif str(slot) in data['slot_errors']:
                    state = 'failed'
                current_identity = state in ('ready', 'idle', 'in_use')
                used = usage.get(str(slot), 0) if current_identity else 0
                slots.append({'slot': slot, 'state': state,
                              'device_id': inspected.get('device_id', ''),
                              'label': device_label(inspected.get('device_id')),
                              'error': data['slot_errors'].get(str(slot), ''),
                              'used': used if type(used) is int and used >= 0 else 0})
        except Exception:                                          # noqa: BLE001
            pass
        registry = self.registry()
        for entry in slots:
            if entry['state'] not in ('ready', 'idle', 'in_use'):
                # A pending/failed slot belongs to the next identity generation.
                # Never combine it with the retired identity's usage or history.
                entry.update(proxy='', model='', at='', recorded=False)
                continue
            info = registry.get(str(entry['slot'])) or {}
            recovered = self._recover_slot(pool, entry['slot']) if entry['state'] == 'ready' else {}
            entry['proxy'] = info.get('proxy', '') or ('历史未记录' if entry['state'] == 'ready' else '')
            entry['model'] = info.get('model', '') or recovered.get('model', '')
            entry['at'] = info.get('at', '') or recovered.get('at', '')
            entry['recorded'] = bool(info)
        attempted = sum(entry['state'] != 'missing' for entry in slots)
        registering = sum(entry['state'] == 'registering' for entry in slots)
        in_use = sum(entry['state'] == 'in_use' for entry in slots)
        pending = sum(entry['state'] in ('missing', 'pending') for entry in slots)
        failed_slots = sum(entry['state'] == 'failed' for entry in slots)
        data.update(ready=ready, attempted=attempted, registering=registering,
                    in_use=in_use, pending=pending, failed_slots=failed_slots, slots=slots,
                    capacity=max(int(data.get('capacity') or 0), persisted_capacity,
                                 configured_capacity, ready),
                    target_capacity=configured_capacity)
        return data

    @staticmethod
    def _active_task_lease(pool, slot):
        """Return true only for a live worker that still owns this slot."""
        try:
            lease = read_json(pool.slot_directory(int(slot)) / '.lease-state.json', {})
            if lease.get('state') != 'in_use':
                return False
            pid = lease.get('pid')
            if type(pid) is not int or pid <= 0:
                return False
            os.kill(pid, 0)
            return True
        except (AttributeError, OSError, TypeError, ValueError):
            return False

    @staticmethod
    def _recover_slot(pool, slot):
        """Recover display facts for ready devices created before history existed."""
        directory = pool.slot_directory(int(slot))
        model = ''
        try:
            profile = json.loads((directory / 'device-profile.json').read_text('utf-8'))
            model = str(profile.get('model') or '') if isinstance(profile, dict) else ''
        except (OSError, ValueError, TypeError):
            pass
        try:
            timestamp = (directory / f'{pool.key}.json').stat().st_mtime
            at = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(timestamp))
        except (OSError, ValueError, TypeError):
            at = ''
        return {'model': model, 'at': at}

    def start(self, capacity):
        with self.lock:
            if self.state['running']:
                raise RuntimeError('正在初始化设备，请等待本轮结束')
            if not 1 <= int(capacity) <= self.slot_limit():
                raise ValueError(f'设备数量应为 1–{self.slot_limit()}')
            # 设备注册会连 Apple；没有代理就变成用机房 IP 注册，出生地不可改，
            # 宁可拒绝也不留这种设备。
            if not (self.node.proxy_api or '').strip():
                raise ValueError('总控未下发代理 API，拒绝直连注册设备')
            self.stop.clear()
            self.state.update(running=True, capacity=int(capacity), ready=0, built=0,
                              failed=0, current=0, message='准备中',
                              proxy_status='checking', proxy_ip='',
                              slot_errors={},
                              started_at=time.strftime('%H:%M:%S'))
            root = self.node.root / 'device-pool'
            root.mkdir(mode=0o700, parents=True, exist_ok=True)
            atomic_json(root / 'target.json', {'capacity': int(capacity)})
            self.thread = threading.Thread(target=self._run, args=(int(capacity),),
                                           name='device-builder', daemon=True)
            self.thread.start()

    def cancel(self):
        self.stop.set()
        with self.lock:
            process = self.provision_process
            if self.state.get('running'):
                self.state['message'] = '正在停止'
        # 不只设置标志：当前注册在独立子进程中，必须立即终止，
        # 否则 UI 会显示“已请求停止”但仍等待完整注册超时。
        if process is not None:
            self._stop_process(process)

    def clear(self):
        """清空节点本地设备身份、注册记录和使用计数。"""
        with self.lock:
            if self.state['running']:
                raise RuntimeError('设备正在注册，请先等待停止')
            root = self.node.root / 'device-pool'
            shutil.rmtree(root, ignore_errors=True)
            root.mkdir(mode=0o700, parents=True, exist_ok=True)
            self.reserved.clear()
            self.state.update(running=False, capacity=0, ready=0, built=0,
                              failed=0, current=0, message='设备池已清空',
                              started_at=None, proxy_status='idle', proxy_ip='',
                              slot_errors={})
        return True

    def reserve(self, slots):
        """任务进程占用的槽位由执行器注册，后台构建设备时主动避开。"""
        with self.lock:
            self.reserved.update(int(slot) for slot in slots if int(slot) > 0)

    def release(self, slots):
        with self.lock:
            self.reserved.difference_update(int(slot) for slot in slots if int(slot) > 0)

    def _reserved(self, slot):
        with self.lock:
            return int(slot) in self.reserved

    def reserved_slots(self):
        with self.lock:
            return set(self.reserved)

    def _update(self, **fields):
        with self.lock:
            self.state.update(fields)

    @staticmethod
    def _stop_process(process):
        try:
            if process.is_alive():
                process.terminate()
                process.join(2)
            if process.is_alive():
                process.kill()
                process.join(1)
        except (AttributeError, OSError, ValueError):
            pass

    def _provision(self, payload):
        """Run one registration in a spawned process and return its safe result."""
        if self.stop.is_set():
            return {'ok': False, 'cancelled': True, 'error': '已取消'}
        context = mp.get_context('spawn')
        output = context.Queue(maxsize=8)
        process = context.Process(target=_register, args=(payload, output),
                                  name=f'device-{payload["slot"]}')
        result = None
        started = False
        try:
            process.start()
            started = True
            with self.lock:
                self.provision_process = process
            # One complete registration attempt has a hard 20-second budget.
            # The child is reaped at the deadline even if a dependency ignores
            # its own socket timeout.
            deadline = time.monotonic() + payload['timeout']
            while time.monotonic() < deadline:
                if self.stop.is_set():
                    result = {'ok': False, 'cancelled': True, 'error': '已取消'}
                    break
                try:
                    result = output.get(timeout=.2)
                    break
                except queue.Empty:
                    if not process.is_alive():
                        break
        finally:
            if started:
                self._stop_process(process)
            with self.lock:
                if self.provision_process is process:
                    self.provision_process = None
            output.cancel_join_thread(); output.close()
            if started:
                try:
                    process.close()
                except (OSError, ValueError):
                    pass
        return result

    def _run(self, capacity):
        root = self.node.root / 'device-pool'
        root.mkdir(parents=True, exist_ok=True)
        try:
            DevicePool, _, _ = _load_fastcheck()
            pool = DevicePool(str(root), self.node.origin, max_devices=capacity)
            for slot in range(1, capacity + 1):
                if self.stop.is_set():
                    self._update(message='已取消')
                    break
                if self._reserved(slot):
                    self._update(message=f'第 {slot} 台由任务进程初始化，后台继续补充')
                    continue
                if pool.inspect(slot)['state'] == 'ready':
                    self._update(ready=self.state['ready'] + 1)
                    continue
                self._update(current=slot, message=f'正在注册第 {slot} 台')
                ok = False
                reason = '代理获取失败'
                for proxy_attempt in range(REGISTRATION_PROXY_ATTEMPTS):
                    proxy = self._proxy()
                    if not proxy:
                        reason = '代理获取失败'
                        self._update(proxy_status='failed', proxy_ip='',
                                     message=f'第 {slot} 台代理获取失败，正在换出口')
                    else:
                        self._update(proxy_status='ok', proxy_ip=proxy_endpoint(proxy),
                                     message=f'正在注册第 {slot} 台（出口 {proxy_attempt + 1}）')
                        payload = {'slot': slot, 'capacity': capacity, 'proxy': proxy,
                                   'device_root': str(root), 'origin': self.node.origin,
                                   'timeout': REGISTRATION_TIMEOUT}
                        result = self._provision(payload)
                        if self.stop.is_set() or (result or {}).get('cancelled'):
                            self._update(message='已取消')
                            break
                        ok = (bool(result and result.get('ok'))
                              and pool.inspect(slot)['state'] == 'ready')
                        if ok:
                            self._record(slot, proxy, (result or {}).get('model'))
                            self._clear_slot_error(slot)
                            self._update(built=self.state['built'] + 1,
                                         ready=self.state['ready'] + 1,
                                         message=f'第 {slot} 台已就绪')
                            break
                        reason = (result or {}).get('error') or '20秒超时'
                    _reset_incomplete_device_slots(pool, [slot])
                    if proxy_attempt + 1 < REGISTRATION_PROXY_ATTEMPTS:
                        self._update(message=f'第 {slot} 台失败（{reason}），正在换出口重试')
                if self.stop.is_set():
                    break
                if not ok:
                    self._fail_slot(slot, reason)
                    self._update(failed=self.state['failed'] + 1,
                                 message=f'第 {slot} 台失败（{reason}）')
            else:
                if not self.state['failed']:
                    self._update(message='全部完成')
        except Exception as exc:                                   # noqa: BLE001
            self._update(message=f'异常：{type(exc).__name__}')
        finally:
            self._update(running=False, current=0)

    def _registry_path(self):
        return self.node.root / 'device-pool' / 'registry.json'

    def _fail_slot(self, slot, reason):
        with self.lock:
            errors = dict(self.state.get('slot_errors') or {})
            errors[str(int(slot))] = str(reason or '注册失败')[:120]
            self.state['slot_errors'] = errors

    def _clear_slot_error(self, slot):
        with self.lock:
            errors = dict(self.state.get('slot_errors') or {})
            errors.pop(str(int(slot)), None)
            self.state['slot_errors'] = errors

    def _record(self, slot, proxy, model=''):
        """记下每台设备注册时用的出口，供界面核对设备"出生地"。"""
        return record_device_registration(self.node.root / 'device-pool', slot, proxy, model)

    def registry(self):
        data = {}
        try:
            legacy = json.loads(self._registry_path().read_text('utf-8'))
            if isinstance(legacy, dict):
                data.update(legacy)
        except (OSError, ValueError):
            pass
        directory = self.node.root / 'device-pool' / 'registrations'
        for path in directory.glob('*.json') if directory.exists() else ():
            try:
                value = json.loads(path.read_text('utf-8'))
                slot = int(value.get('slot') or path.stem)
                if isinstance(value, dict):
                    data[str(slot)] = value
            except (OSError, ValueError, TypeError):
                continue
        return data

    def usage(self):
        path = self.node.root / 'device-pool' / 'usage.json'
        try:
            return json.loads(path.read_text('utf-8'))
        except (OSError, ValueError):
            return {}

    def _proxy(self):
        """设备注册借用本机出口；配置了代理就用一条，否则直连。"""
        try:
            from fastcheck.proxy_pool import fetch_proxies
            picked = fetch_proxies(self.node.proxy_api, 1,
                                   scheme=None if self.node.proxy_scheme == 'http'
                                   else self.node.proxy_scheme)
            return picked[0] if picked else None
        except Exception:                                          # noqa: BLE001
            return None


def _register(payload, output):
    """子进程：给一个槽位做 provisioning。"""
    import signal
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    try:
        DevicePool, _, _ = _load_fastcheck()
        # 注册时就定下型号：设备身份一旦写死，之后执行必须报同一个型号，
        # 否则同一台设备"注册时是 MacBookPro13,3、执行时变 iMac"会被判异常。
        from fastcheck.workers import device_profile
        pool = DevicePool(payload['device_root'], payload['origin'],
                          max_devices=payload['capacity'])
        with pool.operation(payload['slot']):
            info = pool.info(payload['timeout'], proxy=payload['proxy'], provider_direct=True)
            profile = device_profile(pool, payload['slot'])
            info = dict(info, client_info=profile['client_info'], user_agent=profile['user_agent'])
            pool.fetch_headers(payload['slot'], info, payload['timeout'], registered=False,
                               proxy=payload['proxy'], provider_direct=True)
        output.put({'ok': True, 'model': profile['model']})
    except Exception as exc:                                       # noqa: BLE001
        detail = re.sub(r'(?i)\b(?:https?|socks5h?)://\S+', '[endpoint]', str(exc))
        detail = re.sub(r'(?i)(token|password|passwd|secret|key)=\S+', r'\1=***', detail)
        error = type(exc).__name__ + (f': {detail[:96]}' if detail else '')
        output.put({'ok': False, 'error': error})
