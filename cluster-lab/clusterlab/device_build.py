"""节点侧身份池初始化：不跑任务也能把设备注册好。"""
from __future__ import annotations

import json
import multiprocessing as mp
from pathlib import Path
import queue
import shutil
import threading
import time

from .executor import _load_fastcheck, record_device_registration


class DeviceBuilder:
    def __init__(self, node):
        self.node = node
        self.lock = threading.Lock()
        self.thread = None
        self.stop = threading.Event()
        self.reserved = set()
        self.state = {'running': False, 'capacity': 0, 'ready': 0, 'built': 0,
                      'failed': 0, 'current': 0, 'message': '', 'started_at': None}

    def snapshot(self):
        root = self.node.root / 'device-pool'
        ready = 0
        slots = []
        try:
            DevicePool, _, _ = _load_fastcheck()
            pool = DevicePool(str(root), self.node.origin, max_devices=max(1, self.state['capacity'] or 1))
            limit = self.state['capacity'] or 1
            for slot in range(1, limit + 1):
                inspected = pool.inspect(slot)
                state = inspected['state']
                if state == 'ready':
                    ready += 1
                slots.append({'slot': slot, 'state': state,
                              'device_id': inspected.get('device_id', '')})
        except Exception:                                          # noqa: BLE001
            pass
        with self.lock:
            data = dict(self.state)
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
                    capacity=max(data['capacity'], ready))
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
            self.state.update(running=True, capacity=int(capacity), ready=0, built=0,
                              failed=0, current=0, message='准备中',
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
            root.mkdir(mode=0o700, parents=True, exist_ok=True)
            self.reserved.clear()
            self.state.update(running=False, capacity=0, ready=0, built=0,
                              failed=0, current=0, message='设备池已清空',
                              started_at=None)
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

    def _run(self, capacity):
        context = mp.get_context('spawn')
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
                proxy = self._proxy()
                payload = {'slot': slot, 'capacity': capacity, 'proxy': proxy,
                           'device_root': str(root), 'origin': self.node.origin,
                           'timeout': 40}
                output = context.Queue(maxsize=8)
                process = context.Process(target=_register, args=(payload, output),
                                          name=f'device-{slot}')
                result = None
                try:
                    process.start()
                    deadline = time.monotonic() + 60
                    while time.monotonic() < deadline:
                        try:
                            result = output.get(timeout=.2)
                            break
                        except queue.Empty:
                            if not process.is_alive():
                                break
                finally:
                    if process.is_alive():
                        process.terminate(); process.join(2)
                        if process.is_alive():
                            process.kill(); process.join(1)
                    output.cancel_join_thread(); output.close(); process.close()
                ok = bool(result and result.get('ok')) and pool.inspect(slot)['state'] == 'ready'
                if ok:
                    self._record(slot, proxy, (result or {}).get('model'))
                    self._update(built=self.state['built'] + 1,
                                 ready=self.state['ready'] + 1,
                                 message=f'第 {slot} 台已就绪')
                else:
                    self._update(failed=self.state['failed'] + 1,
                                 message=f'第 {slot} 台失败（{(result or {}).get("error") or "超时"}）')
            else:
                self._update(message='全部完成')
        except Exception as exc:                                   # noqa: BLE001
            self._update(message=f'异常：{type(exc).__name__}')
        finally:
            self._update(running=False, current=0)

    def _registry_path(self):
        return self.node.root / 'device-pool' / 'registry.json'

    def _record(self, slot, proxy, model=''):
        """记下每台设备注册时用的出口，供界面核对设备"出生地"。"""
        record_device_registration(self.node.root / 'device-pool', slot, proxy, model)

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

    def retirable(self, limit):
        """返回使用次数已达上限、可以换新的槽位。"""
        if not limit:
            return []
        counts = self.usage()
        return sorted(int(slot) for slot, used in counts.items()
                      if isinstance(used, int) and used >= limit)

    def oldest(self, count=1):
        """按注册时间取最老的槽位：轮换时先淘汰设备 1，再 2、3……"""
        registry = self.registry()
        entries = []
        for key, info in registry.items():
            try:
                entries.append((int(key), float(info.get('ts') or 0)))
            except (TypeError, ValueError):
                continue
        entries.sort(key=lambda item: item[1])
        return [slot for slot, _ in entries[:max(0, int(count))]]

    def retire(self, slots):
        """删掉这些槽位的设备身份，下次执行会自动重新 provisioning。"""
        removed = []
        pool_dir = self.node.root / 'device-pool'
        for slot in slots:
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
        output.put({'ok': False, 'error': type(exc).__name__})
