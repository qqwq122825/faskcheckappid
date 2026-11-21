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
from concurrent.futures import ThreadPoolExecutor, as_completed

from .common import atomic_json, read_json
from .executor import _load_fastcheck, record_device_registration

REGISTRATION_TIMEOUT = 90
IDLE_BUILD_WORKERS = 4
BUSY_BUILD_WORKERS = 2


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
        self.stop = threading.Event()
        self.reserved = set()
        self.state = {'running': False, 'capacity': 0, 'ready': 0, 'built': 0,
                      'failed': 0, 'current': 0, 'message': '', 'started_at': None,
                      'proxy_status': 'idle', 'proxy_ip': '', 'slot_errors': {},
                      'workers': 0, 'current_slots': []}

    def snapshot(self):
        root = self.node.root / 'device-pool'
        ready = 0
        slots = []
        usage = self.usage()
        with self.lock:
            data = dict(self.state)
            data['slot_errors'] = dict(data.get('slot_errors') or {})
        persisted_capacity = 0
        try:
            DevicePool, _, _ = _load_fastcheck()
            # The in-memory builder state resets on a service restart. Discover
            # the persisted slots so a task can still use the node's complete
            # device pool before any new initialization request is made.
            pool = DevicePool(str(root), self.node.origin, max_devices=200)
            device_root = Path(getattr(pool, 'base', root)) / 'devices'
            existing = [int(path.name) for path in device_root.iterdir()
                        if path.is_dir() and path.name.isdigit()
                        and 1 <= int(path.name) <= 200] if device_root.is_dir() else []
            persisted_capacity = max(existing, default=0)
            limit = max(1, int(data.get('capacity') or 0), persisted_capacity)
            active_slots = set(data.get('current_slots') or [])
            if not active_slots and data.get('current'):
                active_slots.add(int(data['current']))
            for slot in range(1, limit + 1):
                inspected = pool.inspect(slot)
                state = inspected['state']
                if state == 'ready':
                    ready += 1
                elif data.get('running') and slot in active_slots:
                    state = 'registering'
                elif str(slot) in data['slot_errors']:
                    state = 'failed'
                used = usage.get(str(slot), 0)
                slots.append({'slot': slot, 'state': state,
                              'device_id': inspected.get('device_id', ''),
                              'label': device_label(inspected.get('device_id')),
                              'error': data['slot_errors'].get(str(slot), ''),
                              'used': used if type(used) is int and used >= 0 else 0})
        except Exception:                                          # noqa: BLE001
            pass
        registry = self.registry()
        for entry in slots:
            info = registry.get(str(entry['slot'])) or {}
            recovered = self._recover_slot(pool, entry['slot']) if entry['state'] == 'ready' else {}
            entry['proxy'] = info.get('proxy', '') or ('历史未记录' if entry['state'] == 'ready' else '')
            entry['model'] = info.get('model', '') or recovered.get('model', '')
            entry['at'] = info.get('at', '') or recovered.get('at', '')
            entry['recorded'] = bool(info)
        attempted = sum(entry['state'] != 'missing' for entry in slots)
        data.update(ready=ready, attempted=attempted, slots=slots,
                    capacity=max(int(data.get('capacity') or 0), persisted_capacity, ready))
        return data

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
            if not 1 <= int(capacity) <= 200:
                raise ValueError('设备数量应为 1–200')
            # 设备注册会连 Apple；没有代理就变成用机房 IP 注册，出生地不可改，
            # 宁可拒绝也不留这种设备。
            if not (self.node.proxy_api or '').strip():
                raise ValueError('总控未下发代理 API，拒绝直连注册设备')
            self.stop.clear()
            workers = min(int(capacity), BUSY_BUILD_WORKERS if self.node._active_worker_count()
                          else IDLE_BUILD_WORKERS)
            self.state.update(running=True, capacity=int(capacity), ready=0, built=0,
                              failed=0, current=0, message='准备中',
                              proxy_status='checking', proxy_ip='',
                              slot_errors={}, workers=workers, current_slots=[],
                              started_at=time.strftime('%H:%M:%S'))
            self.thread = threading.Thread(target=self._run, args=(int(capacity),),
                                           name='device-builder', daemon=True)
            self.thread.start()

    def cancel(self):
        self.stop.set()

    def clear(self):
        """清空节点本地设备身份、注册记录和使用计数。"""
        with self.lock:
            if self.state['running']:
                raise RuntimeError('设备正在注册，请先等待停止')
            root = self.node.root / 'device-pool'
            shutil.rmtree(root, ignore_errors=True)
            shutil.rmtree(self.node.root / 'device-pool-rotation', ignore_errors=True)
            root.mkdir(mode=0o700, parents=True, exist_ok=True)
            self.reserved.clear()
            self.state.update(running=False, capacity=0, ready=0, built=0,
                              failed=0, current=0, message='设备池已清空',
                              started_at=None, proxy_status='idle', proxy_ip='',
                              slot_errors={}, workers=0, current_slots=[])
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

    def _increment(self, **fields):
        with self.lock:
            for key, amount in fields.items():
                self.state[key] = int(self.state.get(key) or 0) + int(amount)

    def _active_slot(self, slot, active):
        with self.lock:
            slots = set(self.state.get('current_slots') or [])
            slots.add(int(slot)) if active else slots.discard(int(slot))
            ordered = sorted(slots)
            self.state['current_slots'] = ordered
            self.state['current'] = ordered[0] if ordered else 0
            return len(ordered)

    def rotate(self, capacity, slot):
        """Register a replacement first; swap it into one idle old slot later."""
        capacity, slot = int(capacity), int(slot)
        with self.lock:
            if self.state['running']:
                raise RuntimeError('正在初始化设备，请等待本轮结束')
            if not 1 <= capacity <= 200 or not 1 <= slot <= capacity:
                raise ValueError('轮换设备编号无效')
            if slot in self.reserved:
                raise RuntimeError('设备正在执行任务，本轮不轮换')
            if not (self.node.proxy_api or '').strip():
                raise ValueError('总控未下发代理 API，拒绝直连注册设备')
            self.stop.clear()
            self.state.update(running=True, capacity=capacity, built=0, failed=0,
                              current=slot, message=f'正在注册第 {slot} 台的替换设备',
                              proxy_status='checking', proxy_ip='',
                              started_at=time.strftime('%H:%M:%S'))
            self.thread = threading.Thread(
                target=self._run_rotation, args=(capacity, slot),
                name='device-rotation', daemon=True)
            self.thread.start()

    @staticmethod
    def _provision(payload):
        """Run one registration in a spawned process and return its safe result."""
        context = mp.get_context('spawn')
        output = context.Queue(maxsize=8)
        process = context.Process(target=_register, args=(payload, output),
                                  name=f'device-{payload["slot"]}')
        result = None
        started = False
        try:
            process.start()
            started = True
            # Residential exits can need 40–60 seconds for the first complete
            # Apple provisioning exchange. Keep the existing safety margin.
            deadline = time.monotonic() + payload['timeout'] + 30
            while time.monotonic() < deadline:
                try:
                    result = output.get(timeout=.2)
                    break
                except queue.Empty:
                    if not process.is_alive():
                        break
        finally:
            if started and process.is_alive():
                process.terminate(); process.join(2)
                if process.is_alive():
                    process.kill(); process.join(1)
            output.cancel_join_thread(); output.close()
            if started:
                process.close()
        return result

    def _run(self, capacity):
        root = self.node.root / 'device-pool'
        root.mkdir(parents=True, exist_ok=True)
        try:
            DevicePool, _, _ = _load_fastcheck()
            pool = DevicePool(str(root), self.node.origin, max_devices=capacity)
            missing = []
            for slot in range(1, capacity + 1):
                if self._reserved(slot):
                    self._update(message=f'第 {slot} 台由任务进程初始化，后台继续补充')
                    continue
                if pool.inspect(slot)['state'] == 'ready':
                    self._increment(ready=1)
                    continue
                missing.append(slot)
            workers = max(1, min(len(missing) or 1, int(self.state.get('workers') or 1)))
            with ThreadPoolExecutor(max_workers=workers,
                                    thread_name_prefix='device-register') as executor:
                futures = [executor.submit(self._build_slot, pool, root, capacity, slot)
                           for slot in missing]
                for future in as_completed(futures):
                    future.result()
            if self.stop.is_set():
                self._update(message='已取消')
            elif not self.state['failed']:
                self._update(message='全部完成')
        except Exception as exc:                                   # noqa: BLE001
            self._update(message=f'异常：{type(exc).__name__}')
        finally:
            self._update(running=False, current=0, current_slots=[], workers=0)

    def _build_slot(self, pool, root, capacity, slot):
        if self.stop.is_set():
            return
        active = self._active_slot(slot, True)
        self._update(message=f'正在并行注册 {active} 台设备')
        try:
            proxy = self._proxy()
            if not proxy:
                self._fail_slot(slot, '代理获取失败')
                self._increment(failed=1)
                self._update(proxy_status='failed', proxy_ip='',
                             message=f'第 {slot} 台代理获取失败，已禁止直连注册')
                return
            self._update(proxy_status='ok', proxy_ip=proxy_endpoint(proxy))
            payload = {'slot': slot, 'capacity': capacity, 'proxy': proxy,
                       'device_root': str(root), 'origin': self.node.origin,
                       'timeout': REGISTRATION_TIMEOUT}
            result = self._provision(payload)
            ok = bool(result and result.get('ok')) and pool.inspect(slot)['state'] == 'ready'
            if ok:
                self._record(slot, proxy, (result or {}).get('model'))
                self._clear_slot_error(slot)
                self._increment(built=1, ready=1)
            else:
                reason = (result or {}).get('error') or '超时'
                self._fail_slot(slot, reason)
                self._increment(failed=1)
        finally:
            self._active_slot(slot, False)

    def _run_rotation(self, capacity, slot):
        """Build outside the live pool, then replace exactly one idle old slot."""
        staging_root = self.node.root / 'device-pool-rotation'
        shutil.rmtree(staging_root, ignore_errors=True)
        try:
            DevicePool, _, _ = _load_fastcheck()
            proxy = self._proxy()
            if not proxy:
                self._update(failed=1, proxy_status='failed', proxy_ip='',
                             message='替换设备代理获取失败，原设备已保留')
                return
            self._update(proxy_status='ok', proxy_ip=proxy_endpoint(proxy))
            payload = {'slot': 1, 'capacity': 1, 'proxy': proxy,
                       'device_root': str(staging_root), 'origin': self.node.origin,
                       'timeout': REGISTRATION_TIMEOUT}
            result = self._provision(payload)
            staged = DevicePool(str(staging_root), self.node.origin, max_devices=1)
            ok = bool(result and result.get('ok')) and staged.inspect(1)['state'] == 'ready'
            if not ok:
                reason = (result or {}).get('error') or '超时'
                self._update(failed=1,
                             message=f'替换设备注册失败（{reason}），原设备已保留')
                return
            if not self._commit_rotation(staging_root, slot, capacity, proxy,
                                         (result or {}).get('model', '')):
                self._update(message='旧设备正在执行任务，本轮未替换；原设备已保留')
                return
            self._clear_slot_error(slot)
            self._update(built=1, ready=capacity,
                         message=f'第 {slot} 台替换成功，原设备已移除')
        except Exception as exc:                                  # noqa: BLE001
            self._update(failed=1,
                         message=f'替换失败（{type(exc).__name__}），原设备已保留')
        finally:
            shutil.rmtree(staging_root, ignore_errors=True)
            self._update(running=False, current=0)

    @staticmethod
    def _restore_file(path, value):
        if value is None:
            path.unlink(missing_ok=True)
        else:
            data, mode = value
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            path.chmod(mode)

    def _commit_rotation(self, staging_root, slot, capacity, proxy, model=''):
        """Atomically swap a verified staged identity into an unused live slot.

        The old directory is retained as a backup until identity, registration,
        usage and health metadata for the replacement have all been persisted.
        """
        DevicePool, _, _ = _load_fastcheck()
        live_root = self.node.root / 'device-pool'
        live = DevicePool(str(live_root), self.node.origin, max_devices=capacity)
        staged = DevicePool(str(staging_root), self.node.origin, max_devices=1)
        source = staged.slot_directory(1)
        target = live.slot_directory(int(slot))
        if staged.inspect(1)['state'] != 'ready' or not target.is_dir():
            return False
        registration = live_root / 'registrations' / f'{int(slot):03d}.json'
        registry_path = self._registry_path()
        usage_path = live_root / 'usage.json'
        health_path = live.base / 'health.json'
        metadata = {path: (path.read_bytes(), path.stat().st_mode & 0o777)
                    if path.exists() else None
                    for path in (registration, registry_path, usage_path, health_path)}
        backup = target.parent / f'.rotation-backup-{int(slot)}-{time.time_ns()}'
        with self.lock:
            if int(slot) in self.reserved:
                return False
            os.replace(target, backup)
            try:
                os.replace(source, target)
                registration.unlink(missing_ok=True)
                if not self._record(slot, proxy, model):
                    raise OSError('无法保存替换设备注册记录')
                usage = read_json(usage_path, {})
                if usage_path.exists() and isinstance(usage, dict):
                    usage.pop(str(int(slot)), None)
                    atomic_json(usage_path, usage)
                legacy = read_json(registry_path, {})
                if (registry_path.exists() and isinstance(legacy, dict)
                        and str(int(slot)) in legacy):
                    legacy.pop(str(int(slot)), None)
                    atomic_json(registry_path, legacy)
                health = read_json(health_path, {})
                if health_path.exists() and isinstance(health, dict):
                    devices = health.get('devices')
                    if isinstance(devices, dict):
                        devices.pop(str(int(slot)), None)
                    events = health.get('events')
                    if isinstance(events, list):
                        health['events'] = [item for item in events
                                            if not (isinstance(item, list) and len(item) > 1
                                                    and item[1] == int(slot))]
                    atomic_json(health_path, health)
            except Exception:
                shutil.rmtree(target, ignore_errors=True)
                os.replace(backup, target)
                for path, value in metadata.items():
                    self._restore_file(path, value)
                raise
        shutil.rmtree(backup, ignore_errors=True)
        return True

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

    def retirable(self, limit, exclude=()):
        """返回使用次数已达上限、可以换新的槽位。"""
        if not limit:
            return []
        excluded = {int(slot) for slot in exclude}
        counts = self.usage()
        return sorted(int(slot) for slot, used in counts.items()
                      if isinstance(used, int) and used >= limit
                      and int(slot) not in excluded)

    def oldest(self, count=1, candidates=None):
        """Return the oldest birth record among eligible candidate slots."""
        registry = self.registry()
        allowed = ({int(slot) for slot in candidates}
                   if candidates is not None else None)
        entries = []
        keys = allowed if allowed is not None else registry.keys()
        for key in keys:
            try:
                slot = int(key)
                info = registry.get(str(slot)) or {}
                entries.append((slot, float(info.get('ts') or 0)))
            except (TypeError, ValueError):
                continue
        entries.sort(key=lambda item: item[1])
        return [slot for slot, _ in entries[:max(0, int(count))]]

    def retire(self, slots):
        """删掉这些槽位的设备身份，下次执行会自动重新 provisioning。"""
        removed = []
        pool_dir = self.node.root / 'device-pool'
        for slot in slots:
            if self._reserved(slot):
                continue
            try:
                DevicePool, _, _ = _load_fastcheck()
                pool = DevicePool(str(pool_dir), self.node.origin, max_devices=max(1, int(slot)))
                directory = pool.slot_directory(int(slot))
            except Exception:                                      # noqa: BLE001
                continue
            for name in ('device-profile.json',):
                (directory / name).unlink(missing_ok=True)
            for path in directory.glob('*.json'):
                path.unlink(missing_ok=True)
            for path in directory.glob('*.lock'):
                path.unlink(missing_ok=True)
            removed.append(int(slot))
        counts = self.usage()
        registry = self.registry()
        for slot in removed:
            counts.pop(str(slot), None)
            registry.pop(str(slot), None)     # 出生记录一起清，否则 oldest 还会选中它
            (pool_dir / 'registrations' / f'{int(slot):03d}.json').unlink(missing_ok=True)
        try:
            (pool_dir / 'usage.json').write_text(json.dumps(counts), 'utf-8')
            self._registry_path().write_text(json.dumps(registry), 'utf-8')
        except OSError:
            pass
        self._update(message=f'已轮换 {len(removed)} 台设备')
        return removed

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
