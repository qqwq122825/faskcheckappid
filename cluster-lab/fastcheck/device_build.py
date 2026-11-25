"""独立补身份：不跑任务也能把空槽位注册满。"""
from __future__ import annotations

import multiprocessing as mp
from pathlib import Path
import queue
import threading
import time

from fastcheck.devices import DevicePool, PoolError
from fastcheck.proxy_pool import fetch_proxies
from fastcheck.workers import register_device


class DeviceBuilder:
    """后台补齐身份池；状态写在内存里，前端轮询 /api/devices/build。"""

    def __init__(self, root, origin, device_root):
        self.root = Path(root)
        self.origin = origin
        self.device_root = Path(device_root)
        self.lock = threading.Lock()
        self.thread = None
        self.stop = threading.Event()
        self.state = {'running': False, 'total': 0, 'ready': 0, 'built': 0,
                      'failed': 0, 'current': 0, 'message': '', 'started_at': None}

    def pool(self, capacity):
        return DevicePool(self.device_root, self.origin, max_devices=capacity)

    def snapshot(self, capacity):
        try:
            pool = self.pool(capacity)
            states = [pool.inspect(slot)['state'] for slot in range(1, capacity + 1)]
            ready = states.count('ready')
        except (PoolError, OSError, ValueError):
            ready = 0
        with self.lock:
            data = dict(self.state)
        data.update(capacity=capacity, ready_now=ready,
                    missing=max(0, capacity - ready))
        return data

    def start(self, capacity, api_url, scheme='socks5h', proxy_auth=''):
        with self.lock:
            if self.state['running']:
                raise RuntimeError('正在建立身份，请等待本轮结束。')
            self.stop.clear()
            self.state.update(running=True, total=capacity, ready=0, built=0,
                              failed=0, current=0, message='准备中',
                              started_at=time.strftime('%H:%M:%S'))
            self.thread = threading.Thread(target=self._run,
                                           args=(capacity, api_url, scheme, proxy_auth),
                                           name='device-builder', daemon=True)
            self.thread.start()

    def cancel(self):
        self.stop.set()

    def _update(self, **fields):
        with self.lock:
            self.state.update(fields)

    def _run(self, capacity, api_url, scheme, proxy_auth):
        context = mp.get_context('spawn')
        try:
            pool = self.pool(capacity)
            for slot in range(1, capacity + 1):
                if self.stop.is_set():
                    self._update(message='已取消')
                    break
                try:
                    state = pool.inspect(slot)['state']
                except (PoolError, OSError, ValueError):
                    state = 'invalid'
                if state == 'ready':
                    self._update(ready=self.state['ready'] + 1)
                    continue
                if state == 'invalid':
                    self._update(message=f'槽位 {slot} 状态损坏，停止', running=False)
                    return
                self._update(current=slot, message=f'正在注册第 {slot} 台')
                lease = self._lease(api_url, scheme, proxy_auth)
                if not lease:
                    self._update(message='代理提取失败，停止', running=False)
                    return
                payload = {'slot': slot, 'proxy': lease, 'device_root': str(self.device_root),
                           'origin': self.origin, 'prepare_timeout': 37,
                           'device_capacity': capacity, 'debug': False,
                           'anisette_proxy': False}
                output = context.Queue(maxsize=8)
                process = context.Process(target=register_device, args=(payload, output),
                                          name=f'build-{slot}')
                result = None
                try:
                    process.start()
                    deadline = time.monotonic() + 50
                    while time.monotonic() < deadline:
                        try:
                            result = output.get(timeout=.2)
                            break
                        except queue.Empty:
                            if not process.is_alive():
                                break
                finally:
                    if process.is_alive():
                        process.terminate()
                        process.join(2)
                        if process.is_alive():
                            process.kill()
                            process.join(1)
                    output.cancel_join_thread()
                    output.close()
                    process.close()
                ok = bool(result and result.get('ok')) and \
                    pool.inspect(slot)['state'] == 'ready'
                if ok:
                    self._update(built=self.state['built'] + 1,
                                 ready=self.state['ready'] + 1,
                                 message=f'第 {slot} 台已注册')
                else:
                    self._update(failed=self.state['failed'] + 1,
                                 message=f'第 {slot} 台失败'
                                         f"（{(result or {}).get('error_type', 'timeout')}）")
            else:
                self._update(message='全部完成')
        except Exception as exc:                                  # noqa: BLE001
            self._update(message=f'异常：{type(exc).__name__}', failed=self.state['failed'] + 1)
        finally:
            self._update(running=False, current=0)

    @staticmethod
    def _lease(api_url, scheme, proxy_auth):
        try:
            proxies = fetch_proxies(api_url, 1,
                                    scheme=None if scheme == 'http' else scheme)
        except Exception:                                         # noqa: BLE001
            return None
        if not proxies:
            return None
        proxy = proxies[0]
        if proxy_auth and ':' in proxy_auth and '@' not in proxy:
            head, tail = proxy.split('://', 1)
            proxy = f'{head}://{proxy_auth}@{tail}'
        return proxy
