import atexit
import hashlib
import json
import multiprocessing as mp
from pathlib import Path
import socket
import threading
import time
from urllib.parse import urlsplit
import uuid

import requests

from . import VERSION
from .common import Database, atomic_json, file_digest, integer, private_dir, read_json
from .executor import execute_auth_unit, execute_unit
from .telemetry import HostTelemetry

SCHEMA = '''
CREATE TABLE IF NOT EXISTS tasks (
 id TEXT PRIMARY KEY, job_id TEXT NOT NULL, seq INTEGER NOT NULL,
 descriptor TEXT NOT NULL, status TEXT NOT NULL, processed INTEGER NOT NULL DEFAULT 0,
 created REAL NOT NULL, updated REAL NOT NULL, error TEXT, checksum TEXT
);
'''


class Node:
    # 必须 >= 总控的 unit_byte_limit（controller.settings 里是 32 MiB）。
    # 两处不一致时总控切得出、节点收不下，大包会一直重试到租约过期。
    UNIT_BYTE_LIMIT = 34 * 1024 * 1024

    def __init__(self, root, max_processes=2, max_threads=8, allow_http=False, interval=1.0):
        self.root = private_dir(root)
        self.spool = private_dir(self.root / 'spool')
        self.db = Database(self.root / 'node.sqlite3', SCHEMA)
        self.max_processes = integer(max_processes, '本机进程上限', 1, 200)
        self.max_threads = integer(max_threads, '本机线程上限', 1, 512)
        self.allow_http, self.interval = allow_http, interval
        self.instance = uuid.uuid4().hex
        self.context = mp.get_context('spawn')
        self.stop = threading.Event()
        self.lock = threading.RLock()
        self.active = {}
        self.connected = False
        self.error = ''
        self.last_success = None
        self.settings = {'processes': 1, 'threads': 1, 'enabled': False, 'name': '未连接节点'}
        self.config = read_json(self.root / 'connection.json', {})
        self.client = requests.Session()
        self.client.trust_env = False
        self.thread = None
        self.generation = 0
        self.stop_version = read_json(self.root / 'control.json', {}).get('stop_version', 0)
        self.telemetry = HostTelemetry()
        self.proxy_api, self.proxy_scheme = '', 'socks5h'
        self.origin = 'https://new.ani.sidestore.io'
        from .device_build import DeviceBuilder
        self.builder = DeviceBuilder(self)
        # Unfinished tasks never get silently executed again after restart.
        with self.db.transaction() as db:
            for task in db.execute("SELECT id FROM tasks WHERE status IN ('downloading','running')").fetchall():
                done = read_json(self.spool / task['id'] / 'done.json')
                if done and done.get('ok'):
                    db.execute("UPDATE tasks SET status='outbox',processed=?,checksum=? WHERE id=?",
                               (done['processed'], done['checksum'], task['id']))
                else:
                    db.execute("UPDATE tasks SET status='interrupted',error='节点重启，等待总控确认' WHERE id=?", (task['id'],))

    def close(self):
        if self.stop.is_set():
            return
        self.stop.set()
        if self.thread and self.thread is not threading.current_thread():
            self.thread.join(7)
        with self.lock:
            active = list(self.active.values())
        for process, stop in active:
            stop.set()
            process.join(2)
            if process.is_alive():
                process.terminate()
                process.join(2)
        self.client.close()

    def connect(self, url, token):
        url = str(url).strip().rstrip('/')
        parsed = urlsplit(url)
        if (parsed.scheme not in ('http', 'https') or not parsed.hostname or parsed.username
                or parsed.password or parsed.query or parsed.fragment or parsed.path not in ('', '/')
                or any(ord(c) < 32 for c in url)):
            raise ValueError('请填写总控根地址，例如 http://127.0.0.1:18770')
        if parsed.scheme == 'http' and parsed.hostname not in ('127.0.0.1', 'localhost', '::1') and not self.allow_http:
            raise ValueError('远程总控需要 HTTPS；本机回环地址可用 HTTP')
        with self.lock:
            if self.active:
                raise ValueError('本机仍有工作包在执行，请先停止接单并等待完成')
            with self.db.connect() as db:
                if db.execute("SELECT COUNT(*) FROM tasks WHERE status='outbox'").fetchone()[0]:
                    raise ValueError('仍有结果待上传，不能更换总控')
            secret = str(token).strip() or self.config.get('token', '')
            if len(secret) < 24:
                raise ValueError('请填写总控生成的节点接入凭证')
            if self.config.get('url') and url != self.config['url'] and not str(token).strip():
                raise ValueError('更换总控时需填写新的节点凭证')
            if self.config.get('url') != url or self.config.get('token') != secret:
                self.stop_version = 0
                atomic_json(self.root / 'control.json', {'stop_version': 0})
            self.config = {'url': url, 'token': secret}
            self.generation += 1
            atomic_json(self.root / 'connection.json', self.config)
            self.connected = False
            self.error = ''

    def _request(self, method, path, **kwargs):
        with self.lock:
            config = dict(self.config)
        headers = dict(kwargs.pop('headers', {}))
        headers['Authorization'] = 'Bearer ' + config['token']
        response = self.client.request(method, config['url'] + path, headers=headers,
                                       timeout=(3, 8), allow_redirects=False, **kwargs)
        if response.status_code >= 400:
            try:
                message = response.json().get('error', '总控拒绝请求')
            except ValueError:
                message = f'总控 HTTP {response.status_code}'
            status = response.status_code
            response.close()
            raise ConnectionError(f'{message}（{status}）')
        if response.status_code >= 300:
            response.close()
            raise ConnectionError('总控地址发生重定向，请填写最终地址')
        return response

    def _poll_processes(self):
        with self.lock:
            active = list(self.active.items())
        for unit_id, (process, stop) in active:
            progress = read_json(self.spool / unit_id / 'progress.json', {})
            with self.db.transaction() as db:
                db.execute('UPDATE tasks SET processed=?,updated=? WHERE id=?', (progress.get('processed', 0), time.time(), unit_id))
            if not process.is_alive():
                process.join()
                done = read_json(self.spool / unit_id / 'done.json', {})
                with self.db.transaction() as db:
                    db.execute('UPDATE tasks SET status=?,processed=?,checksum=?,error=?,updated=? WHERE id=?',
                               ('outbox' if done.get('ok') else 'interrupted', done.get('processed', 0),
                                done.get('checksum'), done.get('error', '执行器意外退出') if not done.get('ok') else None,
                                time.time(), unit_id))
                process.close()
                with self.lock:
                    self.active.pop(unit_id, None)

    def apply_config(self, config):
        """总控下发的出口配置；只在内存里保存，不落盘。

        兼容两种键名：proxy_for_node 返回的是 api/scheme，老版本是 proxy_api。
        """
        if isinstance(config, dict):
            self.proxy_api = str(config.get('api') or config.get('proxy_api') or '')
            self.proxy_scheme = str(config.get('scheme') or config.get('proxy_scheme') or 'socks5h')
            self.origin = str(config.get('origin') or self.origin)

    def _heartbeat(self):
        with self.db.connect() as db:
            values, interrupted = [], []
            for task in db.execute("SELECT * FROM tasks WHERE status IN ('running','outbox','interrupted')"):
                descriptor = json.loads(task['descriptor'])
                collection = interrupted if task['status'] == 'interrupted' else values
                collection.append({'id': task['id'], 'token': descriptor['token'], 'processed': task['processed']})
        with self._request('POST', '/agent/heartbeat', json={'session': self.instance, 'hostname': socket.gethostname(),
                           'version': VERSION, 'max_processes': self.max_processes, 'max_threads': self.max_threads,
                           'active': values, 'interrupted': interrupted, 'ack_stop_version': self.stop_version,
                           'telemetry': dict(self.telemetry.latest, worker_processes=len(self.active)),
                           'devices': {k: self.builder.state[k] for k in ('running', 'capacity', 'ready', 'built', 'failed')},
                           'error': ''}) as response:
            value = response.json()
        with self.lock:
            self.settings = value
            self.connected, self.error, self.last_success = True, '', time.time()
            if value.get('stop_version', 0) > self.stop_version:
                for process, stop in self.active.values():
                    stop.set()
                self.stop_version = value['stop_version']
                atomic_json(self.root / 'control.json', {'stop_version': self.stop_version})
            for unit_id in value.get('interrupted_ack', []):
                with self.db.transaction() as db:
                    db.execute("UPDATE tasks SET status='interrupted_reported',error='已按总控指令中止；等待总控重新安排',updated=? WHERE id=? AND status='interrupted'",
                               (time.time(), unit_id))
            for unit_id in value.get('confirmed', []):
                with self.db.transaction() as db:
                    db.execute("UPDATE tasks SET status='confirmed',updated=? WHERE id=? AND status='outbox'", (time.time(), unit_id))
            # 总控下发的出口配置与设备初始化请求都在心跳响应里
            if 'config' in value:
                self.apply_config(value['config'])
            requested = value.get('device_request') or 0
            if requested and not self.builder.state['running']:
                try:
                    self.builder.start(int(requested))
                except (RuntimeError, ValueError) as exc:
                    # 不能静默吞：面板上看不到原因会以为节点没响应
                    self.builder._update(message=str(exc))
            # 运行中轮换：用够次数的设备删身份重注册，池子始终是新的
            limit = int(value.get('device_rotate_after') or 0)
            if limit and not self.builder.state['running']:
                stale = self.builder.retirable(limit)
                if stale:
                    self.builder.retire(stale[:max(1, len(stale) // 2)])
            for unit_id in value['rejected']:
                if unit_id in self.active:
                    self.active[unit_id][1].set()
                with self.db.transaction() as db:
                    db.execute("UPDATE tasks SET status='rejected',error='总控已拒绝过期租约；本地结果保留' WHERE id=?", (unit_id,))
        return value

    def _upload_one(self):
        with self.db.connect() as db:
            task = db.execute("SELECT * FROM tasks WHERE status='outbox' ORDER BY created LIMIT 1").fetchone()
        if not task:
            return
        descriptor = json.loads(task['descriptor'])
        with (self.spool / task['id'] / 'results.jsonl').open('rb') as stream:
            with self._request('POST', f'/agent/units/{task["id"]}/result',
                               data={'lease_token': descriptor['token'], 'checksum': task['checksum']},
                               files={'result': ('result.jsonl', stream, 'application/x-ndjson')}) as response:
                if not response.json().get('accepted'):
                    raise ConnectionError('总控尚未确认结果')
        with self.db.transaction() as db:
            db.execute("UPDATE tasks SET status='confirmed',updated=? WHERE id=?", (time.time(), task['id']))

    def _take_one(self):
        with self._request('POST', '/agent/lease', json={'session': self.instance}) as response:
            descriptor = response.json().get('unit')
        if not descriptor:
            return False
        unit_id = descriptor['id']
        if not isinstance(unit_id, str) or len(unit_id) != 32 or any(c not in '0123456789abcdef' for c in unit_id):
            raise ValueError('工作包标识无效')
        directory = private_dir(self.spool / unit_id)
        with self.db.transaction() as db:
            db.execute('INSERT OR REPLACE INTO tasks(id,job_id,seq,descriptor,status,created,updated) VALUES(?,?,?,?,?,?,?)',
                       (unit_id, descriptor['job_id'], descriptor['seq'], json.dumps(descriptor), 'downloading', time.time(), time.time()))
        with self._request('GET', f'/agent/units/{unit_id}/input',
                           headers={'X-Lease-Token': descriptor['token']}, stream=True) as response:
            path = directory / 'input.part'
            total = 0
            with path.open('wb') as output:
                path.chmod(0o600)
                for block in response.iter_content(64 * 1024):
                    total += len(block)
                    if total > self.UNIT_BYTE_LIMIT:
                        raise ValueError('工作包超过允许大小')
                    output.write(block)
        if file_digest(path) != descriptor['checksum']:
            raise ValueError('工作包校验失败')
        # A retried unit gets a new token; old completion markers must not be reused.
        (directory / 'done.json').unlink(missing_ok=True)
        (directory / 'progress.json').unlink(missing_ok=True)
        # 认证模式需要本地设备目录；节点自己决定路径，不接受总控指定。
        if descriptor.get('mode') == 'auth':
            descriptor.setdefault('device_root', str(private_dir(self.root / 'device-pool')))
            descriptor.setdefault('device_capacity', self.max_processes)
            descriptor.setdefault('timeout', 15)
        atomic_json(directory / 'manifest.json', descriptor)
        stop = self.context.Event()
        runner = execute_auth_unit if descriptor.get('mode') == 'auth' else execute_unit
        process = self.context.Process(target=runner, args=(str(directory), descriptor, stop),
                                       name='unit-worker')
        with self.db.transaction() as db:
            db.execute("UPDATE tasks SET status='running',processed=0,error=NULL WHERE id=?", (unit_id,))
        process.start()
        with self.lock:
            self.active[unit_id] = (process, stop)
        return True

    def start(self):
        self.thread = threading.Thread(target=self.loop, daemon=True, name='node-agent')
        self.thread.start()
        # 心跳与上传各自独立：上传一个几十 MB 的结果文件会阻塞几十秒，
        # 放在主循环里会让总控把节点判离线、把工作包标成待确认。
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True,
                                                 name='node-heartbeat')
        self.heartbeat_thread.start()
        self.upload_thread = threading.Thread(target=self._upload_loop, daemon=True,
                                              name='node-upload')
        self.upload_thread.start()
        atexit.register(self.close)

    def _heartbeat_loop(self):
        while not self.stop.is_set():
            with self.lock:
                configured = bool(self.config)
            if configured:
                try:
                    self._heartbeat()
                except Exception as exc:                          # noqa: BLE001
                    message = str(exc) if isinstance(exc, (ConnectionError, ValueError)) else type(exc).__name__
                    with self.lock:
                        self.connected = False
                        self.error = message[:240]
            self.stop.wait(self.interval)

    def _upload_loop(self):
        while not self.stop.is_set():
            with self.lock:
                configured = bool(self.config)
            if configured:
                try:
                    self._upload_one()
                    self.stop.wait(0.2)
                    continue
                except Exception as exc:                          # noqa: BLE001
                    message = str(exc) if isinstance(exc, (ConnectionError, ValueError)) else type(exc).__name__
                    with self.lock:
                        self.error = message[:240]
            self.stop.wait(2.0)

    def loop(self):
        """主循环只做进程回收与领取，心跳和上传在各自线程里跑。"""
        while not self.stop.is_set():
            self._poll_processes()
            self.telemetry.sample()
            with self.lock:
                configured = bool(self.config)
                generation = self.generation
                settings = self.settings
            if not configured:
                self.stop.wait(self.interval)
                continue
            try:
                if settings['enabled']:
                    with self.db.connect() as db:
                        backlog = db.execute("SELECT COUNT(*) FROM tasks WHERE status='outbox'").fetchone()[0]
                    with self.lock:
                        capacity = settings['processes'] - len(self.active) if backlog < settings['processes'] * 2 else 0
                    for _ in range(max(0, capacity)):
                        with self.lock:
                            if generation != self.generation:
                                break
                        if not self._take_one():
                            break
            except Exception as exc:
                # Error text from controlled endpoints only; never include request URLs or headers.
                message = str(exc) if isinstance(exc, (ConnectionError, ValueError)) else type(exc).__name__
                with self.lock:
                    self.connected = False
                    self.error = message[:240]
                with self.db.transaction() as db:
                    db.execute("UPDATE tasks SET status='interrupted',error=?,updated=? WHERE status='downloading'",
                               (message[:240], time.time()))
            self.stop.wait(self.interval)

    def snapshot(self):
        with self.lock:
            config = {'url': self.config.get('url', ''), 'token_saved': bool(self.config.get('token'))}
            settings = dict(self.settings)
            connected = self.connected and self.last_success and time.time() - self.last_success < 10
            error, last_success = self.error, self.last_success
            active_processes = len(self.active)
        with self.db.connect() as db:
            tasks = []
            for task in db.execute('SELECT * FROM tasks ORDER BY created DESC LIMIT 60'):
                descriptor = json.loads(task['descriptor'])
                progress = read_json(self.spool / task['id'] / 'progress.json', {})
                tasks.append({'id': task['id'], 'job_id': task['job_id'], 'seq': task['seq'],
                              'rows': descriptor['rows'], 'threads': descriptor['threads'],
                              'status': task['status'], 'processed': progress.get('processed', task['processed']),
                              'created': task['created'], 'updated': task['updated'], 'error': task['error']})
            confirmed, outbox, processed = db.execute("SELECT COALESCE(SUM(CASE WHEN status='confirmed' THEN processed ELSE 0 END),0),COALESCE(SUM(CASE WHEN status='outbox' THEN processed ELSE 0 END),0),COALESCE(SUM(processed),0) FROM tasks").fetchone()
        return {'connected': bool(connected), 'error': error, 'last_success': last_success,
                'config': config, 'settings': settings, 'tasks': tasks, 'max_processes': self.max_processes,
                'max_threads': self.max_threads, 'active_processes': active_processes,
                'confirmed': confirmed, 'outbox': outbox, 'processed': processed, 'telemetry': dict(self.telemetry.latest),
                'hostname': socket.gethostname(), 'server_time': time.time()}
