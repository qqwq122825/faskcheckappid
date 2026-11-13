"""节点侧身份池初始化：不跑任务也能把设备注册好。"""
from __future__ import annotations

import json
import multiprocessing as mp
from pathlib import Path
import queue
import threading
import time

from .executor import _load_fastcheck


class DeviceBuilder:
    def __init__(self, node):
        self.node = node
        self.lock = threading.Lock()
        self.thread = None
        self.stop = threading.Event()
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
                state = pool.inspect(slot)['state']
                if state == 'ready':
                    ready += 1
                slots.append({'slot': slot, 'state': state})
        except Exception:                                          # noqa: BLE001
            pass
        with self.lock:
            data = dict(self.state)
        data.update(ready=ready, slots=slots,
                    capacity=max(data['capacity'], ready))
        return data

    def start(self, capacity):
        with self.lock:
            if self.state['running']:
                raise RuntimeError('正在初始化设备，请等待本轮结束')
            if not 1 <= int(capacity) <= 200:
                raise ValueError('设备数量应为 1–200')
            self.stop.clear()
            self.state.update(running=True, capacity=int(capacity), ready=0, built=0,
                              failed=0, current=0, message='准备中',
                              started_at=time.strftime('%H:%M:%S'))
            self.thread = threading.Thread(target=self._run, args=(int(capacity),),
                                           name='device-builder', daemon=True)
            self.thread.start()

    def cancel(self):
        self.stop.set()

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
        for slot in removed:
            counts.pop(str(slot), None)
        try:
            (pool_dir / 'usage.json').write_text(json.dumps(counts), 'utf-8')
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
        pool = DevicePool(payload['device_root'], payload['origin'],
                          max_devices=payload['capacity'])
        with pool.operation(payload['slot']):
            info = pool.info(payload['timeout'], proxy=payload['proxy'], provider_direct=True)
            pool.fetch_headers(payload['slot'], info, payload['timeout'], registered=False,
                               proxy=payload['proxy'], provider_direct=True)
        output.put({'ok': True})
    except Exception as exc:                                       # noqa: BLE001
        output.put({'ok': False, 'error': type(exc).__name__})
