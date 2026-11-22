import atexit
import hashlib
import json
import multiprocessing as mp
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
from urllib.parse import urlsplit
import uuid

import portalocker
import requests

from . import VERSION
from .common import Database, atomic_json, file_digest, integer, private_dir, read_json
from .executor import execute_unit_group, stall_timeout_seconds
from .telemetry import HostTelemetry

SCHEMA = '''
CREATE TABLE IF NOT EXISTS tasks (
 id TEXT PRIMARY KEY, job_id TEXT NOT NULL, seq INTEGER NOT NULL,
 descriptor TEXT NOT NULL, status TEXT NOT NULL, processed INTEGER NOT NULL DEFAULT 0,
 created REAL NOT NULL, updated REAL NOT NULL, error TEXT, checksum TEXT
);
'''

DEVICE_AUTH_LIMIT = 100
DEVICE_POOL_LIMIT = 10000
DEVICE_AUTH_COOLDOWN_SECONDS = 3.0


def build_device_plan(rows, processes, slots, usage, limit=DEVICE_AUTH_LIMIT,
                      max_slots=DEVICE_POOL_LIMIT, desired_devices=None):
    """Allocate every row without letting one device identity exceed ``limit``.

    Slots are exclusive to one child process for the package.  A consumed or
    invalid slot is returned in ``reset_slots`` so that child replaces its
    identity before starting any account checks.
    """
    rows, processes = max(1, int(rows)), max(1, min(int(processes), int(rows)))
    states = {int(item['slot']): str(item.get('state') or '')
              for item in slots if isinstance(item, dict) and item.get('slot')}
    used = {}
    for key, value in (usage or {}).items():
        try:
            used[int(key)] = max(0, int(value or 0))
        except (TypeError, ValueError):
            continue
    available = [(slot, int(limit) - used.get(slot, 0))
                 for slot, state in states.items()
                 if state == 'ready' and used.get(slot, 0) < int(limit)]
    available.sort(key=lambda item: (-item[1], item[0]))
    fresh = [(slot, int(limit)) for slot, state in sorted(states.items())
             if state == 'missing' and used.get(slot, 0) == 0]
    reset = [slot for slot, state in sorted(states.items())
             if state not in ('missing',) and not (
                 state == 'ready' and used.get(slot, 0) < int(limit))]
    next_slot = max(states, default=0) + 1
    reset_slots, plan = set(), []
    child_counts = [rows // processes + (index < rows % processes)
                    for index in range(processes)]
    desired_devices = min(rows, int(max_slots), max(
        processes, int(desired_devices or processes)))
    desired_counts = [desired_devices // processes
                      + (index < desired_devices % processes)
                      for index in range(processes)]

    def candidate():
        nonlocal next_slot
        if available:
            return (*available.pop(0), False)
        if fresh:
            return (*fresh.pop(0), False)
        if reset:
            slot = reset.pop(0)
            return slot, int(limit), True
        if next_slot <= int(max_slots):
            slot, next_slot = next_slot, next_slot + 1
            return slot, int(limit), False
        raise RuntimeError(
            f'设备额度不足：本包 {rows} 个账号无法在 {max_slots} 个槽位内满足每台最多 {limit} 个')

    for process_index, count in enumerate(child_counts):
        selected = []
        target = max((count + int(limit) - 1) // int(limit),
                     desired_counts[process_index])
        while len(selected) < target or sum(item[1] for item in selected) < count:
            selected.append(candidate())
        child, remaining = [], [int(item[1]) for item in selected]
        while len(child) < count:
            progressed = False
            for offset, (slot, _capacity, must_reset) in enumerate(selected):
                if remaining[offset] <= 0 or len(child) >= count:
                    continue
                child.append(slot)
                remaining[offset] -= 1
                progressed = True
                if must_reset:
                    reset_slots.add(slot)
            if not progressed:
                raise RuntimeError('设备额度规划失败')
        plan.extend(child)
    return plan, sorted(reset_slots)


class Node:
    # 必须 >= 总控的 unit_byte_limit（controller.settings 里是 32 MiB）。
    # 两处不一致时总控切得出、节点收不下，大包会一直重试到租约过期。
    UNIT_BYTE_LIMIT = 34 * 1024 * 1024
    LIVE_RESULT_LIMIT = 1000
    RESULT_RECORD_BYTE_LIMIT = 64 * 1024
    LIVE_RESULT_BYTE_LIMIT = 1024 * 1024
    # Coordinator has its own watchdog; this outer guard also handles a wedged
    # coordinator event loop.  The effective budget is raised for configured
    # request/retry windows below.
    TASK_STALL_SECONDS = 210.0

    def __init__(self, root, max_processes=2, max_threads=8, allow_http=False, interval=1.0):
        self.root = private_dir(root)
        self.spool = private_dir(self.root / 'spool')
        self.db = Database(self.root / 'node.sqlite3', SCHEMA)
        self.instance_lock_file = (self.root / 'node-service.lock').open('a+', encoding='utf-8')
        self.primary_instance = False
        try:
            portalocker.lock(
                self.instance_lock_file,
                portalocker.LockFlags.EXCLUSIVE | portalocker.LockFlags.NON_BLOCKING,
            )
        except portalocker.exceptions.LockException:
            self.instance_lock_file.close()
            self.instance_lock_file = None
        else:
            self.primary_instance = True
        self.max_processes = integer(max_processes, '本机进程上限', 1, 200)
        self.max_threads = integer(max_threads, '本机线程上限', 1, 512)
        # 已部署的 systemd 仍传入旧参数 10×100。新版将它等并发
        # 迁移为 4×250，并把节点可调上限公告为 4×500。
        if (self.max_processes, self.max_threads) == (10, 100):
            self.max_processes, self.max_threads = 4, 500
        self.allow_http, self.interval = allow_http, interval
        self.instance = uuid.uuid4().hex
        self.context = mp.get_context('spawn')
        self.stop = threading.Event()
        self.lock = threading.RLock()
        self.active = {}
        self.progress_watch = {}
        self.connected = False
        self.error = ''
        self.last_success = None
        self.settings = {'processes': 1, 'threads': 1, 'enabled': False, 'name': '未连接节点'}
        self.config = read_json(self.root / 'connection.json', {})
        self.client = requests.Session()
        self.client.trust_env = False
        self.thread = None
        self.generation = 0
        control = read_json(self.root / 'control.json', {})
        self.stop_version = control.get('stop_version', 0)
        self.device_clear_version = control.get('device_clear_version', 0)
        self.update_generation = control.get('update_generation', 0)
        self.update_status_path = self.root / 'update-status.json'
        self.telemetry = HostTelemetry()
        self.proxy_api, self.proxy_scheme = '', 'socks5h'
        self.proxy_ttl = 50
        self.proxy_pool_size = 1000
        self.origin = 'https://new.ani.sidestore.io'
        from .device_build import DeviceBuilder
        self.builder = DeviceBuilder(self)
        # Spawned coordinator/worker processes may import the node entrypoint.
        # Only the top-level service process is allowed to perform restart
        # recovery; a child must never turn the parent's live task into an
        # interrupted package merely because it constructed a Node object.
        if self.primary_instance and mp.parent_process() is None:
            self._recover_unfinished_tasks()

    def _recover_unfinished_tasks(self):
        """Reconcile local work only when the real node service starts."""
        with self.db.transaction() as db:
            tasks = db.execute(
                "SELECT id FROM tasks WHERE status IN ('downloading','running')"
            ).fetchall()
            for task in tasks:
                done = read_json(self.spool / task['id'] / 'done.json')
                if done and done.get('ok'):
                    db.execute("UPDATE tasks SET status='outbox',processed=?,checksum=? WHERE id=?",
                               (done['processed'], done['checksum'], task['id']))
                else:
                    db.execute(
                        "UPDATE tasks SET status='interrupted',"
                        "error='节点重启，等待总控确认' WHERE id=?", (task['id'],))

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
        if self.instance_lock_file is not None:
            portalocker.unlock(self.instance_lock_file)
            self.instance_lock_file.close()
            self.instance_lock_file = None

    def _save_control(self):
        atomic_json(self.root / 'control.json',
                    {'stop_version': self.stop_version,
                     'device_clear_version': self.device_clear_version,
                     'update_generation': self.update_generation})

    def _update_status(self):
        value = read_json(self.update_status_path, {})
        return value if isinstance(value, dict) else {}

    def _schedule_update(self, directive):
        """在独立 systemd transient unit 中拉取代码并重启。

        更新器不能留在 cluster-node.service 的 cgroup 里，否则服务
        重启时它也会被 systemd 一起终止。
        """
        if not isinstance(directive, dict):
            return False
        generation = directive.get('generation')
        target_ref = str(directive.get('ref') or '').strip()
        service = str(directive.get('service') or 'cluster-node').strip()
        if (type(generation) is not int or generation <= self.update_generation
                or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._/-]{0,119}', target_ref)
                or '..' in target_ref or not re.fullmatch(r'[A-Za-z0-9_.@-]{1,80}', service)):
            return False
        with self.lock:
            if self.active:
                return False
        with self.db.connect() as db:
            pending = db.execute("SELECT COUNT(*) FROM tasks WHERE status IN ('downloading','running','outbox')").fetchone()[0]
        if pending:
            return False
        repo = Path(__file__).resolve().parents[2]
        updater = repo / 'cluster-lab' / 'tools' / 'node_update.py'
        requirements = repo / 'cluster-lab' / 'requirements.txt'
        systemd_run = shutil.which('systemd-run')
        if not systemd_run or not updater.is_file() or not (repo / '.git').exists():
            atomic_json(self.update_status_path, {'generation': generation, 'state': 'failed',
                        'message': '节点未使用标准 Git + systemd 部署，无法自动更新'})
            self.update_generation = generation
            self._save_control()
            return False
        self.update_generation = generation
        self._save_control()
        atomic_json(self.update_status_path, {'generation': generation, 'state': 'scheduled',
                    'message': f'已排队更新到 {target_ref}'})
        unit = f'cluster-node-update-{generation}'
        command = [systemd_run, '--unit', unit, '--collect', '--property=Type=oneshot',
                   sys.executable, str(updater), '--repo', str(repo), '--ref', target_ref,
                   '--service', service, '--python', sys.executable,
                   '--requirements', str(requirements), '--status', str(self.update_status_path),
                   '--generation', str(generation)]
        log_path = self.root / 'node-update.log'
        try:
            with log_path.open('ab', buffering=0) as log:
                subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=log,
                                 stderr=subprocess.STDOUT, start_new_session=True)
        except OSError as exc:
            atomic_json(self.update_status_path, {'generation': generation, 'state': 'failed',
                        'message': f'启动更新器失败：{type(exc).__name__}'})
            return False
        return True

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
                self.device_clear_version = 0
                self.update_generation = 0
                self._save_control()
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
            processed = int(progress.get('processed') or 0)
            now = time.monotonic()
            watch = self.progress_watch.setdefault(
                unit_id, {'processed': processed, 'changed': now})
            if processed != int(watch['processed']):
                watch.update(processed=processed, changed=now)
            descriptor = read_json(self.spool / unit_id / 'manifest.json', {})
            stall_budget = stall_timeout_seconds(
                descriptor, minimum=self.TASK_STALL_SECONDS, padding=90.0)
            if process.is_alive() and now - float(watch['changed']) >= stall_budget:
                stop.set()
                process.join(2)
                if process.is_alive():
                    process.terminate()
                    process.join(2)
                error = f'执行进程无进度超过 {int(stall_budget)} 秒，已中止并等待总控重排'
                with self.db.transaction() as db:
                    db.execute('UPDATE tasks SET status=?,processed=?,error=?,updated=? WHERE id=?',
                               ('interrupted', processed, error, time.time(), unit_id))
                self.builder.release(descriptor.get('device_slots') or [])
                with self.lock:
                    self.active.pop(unit_id, None)
                self.progress_watch.pop(unit_id, None)
                process.close()
                continue
            with self.db.transaction() as db:
                db.execute('UPDATE tasks SET processed=?,updated=? WHERE id=?', (processed, time.time(), unit_id))
            if not process.is_alive():
                process.join()
                done = read_json(self.spool / unit_id / 'done.json', {})
                descriptor = read_json(self.spool / unit_id / 'manifest.json', {})
                with self.db.transaction() as db:
                    db.execute('UPDATE tasks SET status=?,processed=?,checksum=?,error=?,updated=? WHERE id=?',
                               ('outbox' if done.get('ok') else 'interrupted', done.get('processed', 0),
                                done.get('checksum'), done.get('error', '执行器意外退出') if not done.get('ok') else None,
                                time.time(), unit_id))
                process.close()
                self.builder.release(descriptor.get('device_slots') or [])
                with self.lock:
                    self.active.pop(unit_id, None)
                self.progress_watch.pop(unit_id, None)

    def _refresh_progress(self):
        """Persist coordinator counters immediately before a heartbeat."""
        with self.lock:
            unit_ids = list(self.active)
        now = time.time()
        with self.db.transaction() as db:
            for unit_id in unit_ids:
                progress = read_json(self.spool / unit_id / 'progress.json', {})
                db.execute('UPDATE tasks SET processed=MAX(processed,?),updated=? WHERE id=?',
                           (int(progress.get('processed') or 0), now, unit_id))

    def _active_worker_count(self):
        with self.lock:
            unit_ids = list(self.active)
        workers = 0
        for unit_id in unit_ids:
            progress = read_json(self.spool / unit_id / 'progress.json', {})
            workers += int(progress['workers'] if 'workers' in progress
                           else progress.get('worker_limit') or 0)
        return workers

    def _device_slots(self):
        """设备池明细：编号、状态、注册出口。心跳带上，总控页面直接展示。"""
        try:
            snapshot = self.builder.snapshot()
        except Exception:                                          # noqa: BLE001
            return []
        return snapshot.get('slots', [])[:DEVICE_POOL_LIMIT]

    def _device_status(self):
        """Use one snapshot so summary counts and table rows always agree."""
        try:
            snapshot = self.builder.snapshot()
        except Exception:                                          # noqa: BLE001
            snapshot = dict(self.builder.state)
            snapshot['slots'] = []
        return {key: snapshot.get(key) for key in
                ('running', 'capacity', 'ready', 'built', 'failed',
                 'current', 'attempted', 'proxy_status', 'proxy_ip',
                 'message', 'slots')}

    def _live_result_paths(self, unit_id):
        """Return worker outputs, never the later merged duplicate file."""
        directory = self.spool / unit_id
        workers = directory / 'workers'
        if workers.is_dir():
            return sorted(workers.glob('*/results.jsonl'))
        result = directory / 'results.jsonl'
        return [result] if result.exists() else []

    def _collect_live_records(self, unit_id):
        """Read newly flushed complete JSONL lines for the next 1 Hz heartbeat.

        The cursor advances only after the controller explicitly acknowledges
        the batch.  A lost response therefore causes an idempotent replay,
        while a partial line remains for the next heartbeat.
        """
        directory = self.spool / unit_id
        state_path = directory / 'report-state.json'
        state = read_json(state_path, {'files': {}})
        offsets = dict(state.get('files') or {})
        records = []
        record_bytes = 0
        batch_full = False
        for path in self._live_result_paths(unit_id):
            key = str(path.relative_to(directory))
            offset = max(0, int(offsets.get(key) or 0))
            try:
                with path.open('rb') as stream:
                    if offset > path.stat().st_size:
                        offset = 0
                    stream.seek(offset)
                    while len(records) < self.LIVE_RESULT_LIMIT:
                        start = stream.tell()
                        line = stream.readline(self.RESULT_RECORD_BYTE_LIMIT + 1)
                        if not line:
                            break
                        if len(line) > self.RESULT_RECORD_BYTE_LIMIT:
                            raise ValueError('实时结果记录过长')
                        if not line.endswith(b'\n'):
                            stream.seek(start)
                            break
                        if record_bytes + len(line) > self.LIVE_RESULT_BYTE_LIMIT:
                            stream.seek(start)
                            batch_full = True
                            break
                        try:
                            record = json.loads(line)
                        except ValueError as exc:
                            raise ValueError('实时结果 JSON 无效') from exc
                        if not isinstance(record, dict):
                            raise ValueError('实时结果记录无效')
                        # Send the exact JSON text.  The controller can then
                        # promote the streamed rows into the authoritative file
                        # with the same checksum as the later full-file replay.
                        records.append(line[:-1].decode('utf-8'))
                        record_bytes += len(line)
                        offset = stream.tell()
            except FileNotFoundError:
                continue
            offsets[key] = offset
            if len(records) >= self.LIVE_RESULT_LIMIT or batch_full:
                break
        return records, {'files': offsets, 'updated': time.time()}

    def apply_config(self, config):
        """总控下发的出口配置；只在内存里保存，不落盘。

        兼容两种键名：proxy_for_node 返回的是 api/scheme，老版本是 proxy_api。
        """
        if isinstance(config, dict):
            self.proxy_api = str(config.get('api') or config.get('proxy_api') or '')
            self.proxy_scheme = str(config.get('scheme') or config.get('proxy_scheme') or 'socks5h')
            self.origin = str(config.get('origin') or self.origin)

    def _heartbeat(self):
        self._refresh_progress()
        report_states = {}
        report_errors = []
        with self.db.connect() as db:
            values, interrupted = [], []
            for task in db.execute("SELECT * FROM tasks WHERE status IN ('running','outbox','interrupted')"):
                descriptor = json.loads(task['descriptor'])
                collection = interrupted if task['status'] == 'interrupted' else values
                progress = read_json(self.spool / task['id'] / 'progress.json', {})
                item = {'id': task['id'], 'token': descriptor['token'],
                        'processed': max(int(task['processed'] or 0),
                                         int(progress.get('processed') or 0)),
                        'counts': progress.get('counts') or {}}
                # 失败原因一并上报，否则总控只能看到"待确认"，看不到为什么
                if task['error']:
                    item['error'] = str(task['error'])[:180]
                try:
                    records, next_state = self._collect_live_records(task['id'])
                except (OSError, ValueError):
                    # Result validation must not suppress liveness or control ACKs.
                    # Keep both the source and cursor intact for diagnosis/retry.
                    item['error'] = '实时结果上报失败，原文件已保留'
                    report_errors.append(item['error'])
                    records, next_state = [], None
                if records:
                    item['records'] = records
                    report_states[task['id']] = next_state
                collection.append(item)
        with self._request('POST', '/agent/heartbeat', json={'session': self.instance, 'hostname': socket.gethostname(),
                           'version': VERSION, 'max_processes': self.max_processes, 'max_threads': self.max_threads,
                           'active': values, 'interrupted': interrupted,
                           'ack_stop_version': self.stop_version,
                           'ack_device_clear_version': self.device_clear_version,
                           'ack_update_generation': self.update_generation,
                           'update_status': self._update_status(),
                           'telemetry': dict(self.telemetry.latest,
                                             worker_processes=self._active_worker_count()),
                           'devices': self._device_status(),
                           'error': '; '.join(report_errors)[:200]}) as response:
            value = response.json()
        reported = value.get('reported') if isinstance(value.get('reported'), dict) else {}
        for unit_id, next_state in report_states.items():
            if unit_id in reported:
                atomic_json(self.spool / unit_id / 'report-state.json', next_state)
        with self.lock:
            self.settings = value
            self.connected, self.error, self.last_success = True, '; '.join(report_errors)[:240], time.time()
            if value.get('stop_version', 0) > self.stop_version:
                for process, stop in self.active.values():
                    stop.set()
                self.stop_version = value['stop_version']
                self._save_control()
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
            try:
                self.proxy_ttl = max(5, min(3600, int(value.get('proxy_ttl') or 50)))
            except (TypeError, ValueError):
                self.proxy_ttl = 50
            try:
                self.proxy_pool_size = max(1, min(10000, int(
                    value.get('proxy_pool_size') or 1000)))
            except (TypeError, ValueError):
                self.proxy_pool_size = 1000
            clear_version = int(value.get('device_clear_version') or 0)
            if clear_version > self.device_clear_version:
                self.builder.cancel()
                if not self.builder.state['running']:
                    self.builder.clear()
                    self.device_clear_version = clear_version
                    self._save_control()
            requested = value.get('device_request') or 0
            if requested and not self.builder.state['running']:
                try:
                    requested = int(requested)
                    snapshot = self.builder.snapshot()
                    # device_request is a persistent desired size. Do not restart a
                    # completed build on every heartbeat; only fill missing slots.
                    if int(snapshot.get('ready') or 0) < requested:
                        self.builder.start(requested)
                except (RuntimeError, ValueError) as exc:
                    # 不能静默吞：面板上看不到原因会以为节点没响应
                    self.builder._update(message=str(exc))
            # 自动补充：先补缺口；满池轮换时先在隔离目录注册新设备，
            # 成功后才替换一台最老且未被任务占用的旧设备。
            auto = bool(value.get('device_auto_rotate'))
            limit = int(value.get('device_rotate_after') or 0)
            if auto and not self.builder.state['running']:
                snapshot = self.builder.snapshot()
                capacity = int(snapshot.get('capacity') or self.builder.state['capacity'] or 0)
                if capacity:
                    try:
                        if int(snapshot.get('ready') or 0) < capacity:
                            # 先补既有缺口，不再额外淘汰。
                            self.builder.start(capacity)
                        elif limit:
                            reserved = self.builder.reserved_slots()
                            stale = self.builder.retirable(limit, exclude=reserved)
                            if stale:
                                candidates = [int(item['slot'])
                                              for item in snapshot.get('slots') or []
                                              if item.get('state') == 'ready'
                                              and int(item['slot']) not in reserved]
                                if not candidates:
                                    candidates = [slot for slot in range(1, capacity + 1)
                                                  if slot not in reserved]
                                oldest = self.builder.oldest(1, candidates=candidates)
                                if oldest:
                                    self.builder.rotate(capacity, oldest[0])
                    except (RuntimeError, ValueError):
                        pass
            for unit_id in value['rejected']:
                if unit_id in self.active:
                    self.active[unit_id][1].set()
                with self.db.transaction() as db:
                    db.execute("UPDATE tasks SET status='rejected',error='总控已删除任务或撤销租约；节点已停止执行' WHERE id=?", (unit_id,))
        self._schedule_update(value.get('update'))
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
        (directory / 'report-state.json').unlink(missing_ok=True)
        # 认证模式需要本地设备目录；节点自己决定路径，不接受总控指定。
        if descriptor.get('mode') == 'auth':
            descriptor.setdefault('device_root', str(private_dir(self.root / 'device-pool')))
            descriptor.setdefault('timeout', 20)
            descriptor.setdefault('retries', 0)
            descriptor.setdefault('proxy_ttl', self.proxy_ttl)
            descriptor.setdefault('proxy_pool_size', self.proxy_pool_size)
            self._prepare_auth_devices(descriptor)
        atomic_json(directory / 'manifest.json', descriptor)
        stop = self.context.Event()
        process = self.context.Process(target=execute_unit_group,
                                       args=(str(directory), descriptor, stop),
                                       name='package-coordinator')
        with self.db.transaction() as db:
            db.execute("UPDATE tasks SET status='running',processed=0,error=NULL WHERE id=?", (unit_id,))
        process.start()
        with self.lock:
            self.active[unit_id] = (process, stop)
        self.progress_watch[unit_id] = {'processed': 0, 'changed': time.monotonic()}
        return True

    def _prepare_auth_devices(self, descriptor):
        """Reserve a hard per-identity budget before any account reaches Apple."""
        rows = max(1, int(descriptor.get('rows') or 1))
        processes = max(1, min(int(descriptor.get('processes') or 1), rows))
        if str(descriptor.get('_auth_protocol') or 'gsa').lower() == 'web':
            descriptor.pop('device_capacity', None)
            descriptor['processes'] = processes
            descriptor['device_pool_capacity'] = 1
            descriptor['device_slots'] = [1]
            descriptor.pop('device_plan', None)
            descriptor.pop('device_reset_slots', None)
            return
        if str(descriptor.get('_device_mode') or 'pool').lower() == 'one_time':
            # 线程模式与预注册共用同一个持久池。预注册容量可由用户调成
            # 1000/2000 或更高；线程总是在它后面预留本包并发量作为即时扩容槽位。
            snapshot = self.builder.snapshot()
            entries = list(snapshot.get('slots') or [])
            base_capacity = max(
                0, int(snapshot.get('capacity') or 0),
                max((int(item.get('slot') or 0) for item in entries
                     if isinstance(item, dict)), default=0))
            active_devices = min(rows, processes * max(
                1, int(descriptor.get('threads') or 1)))
            pool_size = base_capacity + active_devices
            limit = max(1, int(descriptor.get('device_usage_limit') or DEVICE_AUTH_LIMIT))
            descriptor.pop('device_capacity', None)
            descriptor['processes'] = processes
            descriptor['device_pool_capacity'] = pool_size
            descriptor['device_slots'] = list(range(1, pool_size + 1))
            descriptor['device_base_capacity'] = base_capacity
            descriptor['device_usage_limit'] = limit
            descriptor.pop('device_plan', None)
            descriptor.pop('device_reset_slots', None)
            descriptor.pop('device_usage_base', None)
            descriptor.pop('device_usage_reserved', None)
            descriptor['device_cooldown_seconds'] = DEVICE_AUTH_COOLDOWN_SECONDS
            descriptor['_retry_delay'] = 10.0
            descriptor['_one_time_min_retries'] = 1
            return
        snapshot = self.builder.snapshot()
        entries = list(snapshot.get('slots') or [])
        limit = max(1, int(descriptor.get('device_usage_limit') or DEVICE_AUTH_LIMIT))
        active_devices = processes * max(1, int(descriptor.get('threads') or 1))
        ready_devices = sum(1 for item in entries
                            if isinstance(item, dict) and item.get('state') == 'ready')
        # Keep the first 1,000 requests fully parallel, then let those workers
        # consume the second 1,000 prepared identities while the first wave is
        # cooling down. A smaller prepared pool still uses safe backpressure.
        desired_devices = min(rows, DEVICE_POOL_LIMIT,
                              max(active_devices, ready_devices))
        plan, reset_slots = build_device_plan(
            rows, processes, entries, self.builder.usage(), limit=limit,
            desired_devices=desired_devices)
        slots = sorted(set(plan))
        base = self.builder.reserve_usage(plan, limit, reset_slots)
        capacity = max(slots)
        descriptor.pop('device_capacity', None)
        descriptor['processes'] = processes
        descriptor['device_pool_capacity'] = capacity
        descriptor['device_slots'] = slots
        descriptor['device_plan'] = plan
        descriptor['device_reset_slots'] = reset_slots
        descriptor['device_usage_limit'] = limit
        descriptor['device_cooldown_seconds'] = DEVICE_AUTH_COOLDOWN_SECONDS
        descriptor['device_usage_base'] = base
        descriptor['device_usage_reserved'] = True
        self.builder.reserve(slots)

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
                # 初始化/轮换期间不领新包；已在运行的包不受影响。
                if settings['enabled'] and not self.builder.state['running']:
                    with self.db.connect() as db:
                        backlog = db.execute("SELECT COUNT(*) FROM tasks WHERE status='outbox'").fetchone()[0]
                    with self.lock:
                        # One package consumes this node's complete process
                        # budget; execute_unit_group performs local fan-out.
                        capacity = 1 - len(self.active) if backlog < 2 else 0
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
            active_processes = self._active_worker_count()
        with self.db.connect() as db:
            tasks = []
            for task in db.execute('SELECT * FROM tasks ORDER BY created DESC LIMIT 60'):
                descriptor = json.loads(task['descriptor'])
                progress = read_json(self.spool / task['id'] / 'progress.json', {})
                tasks.append({'id': task['id'], 'job_id': task['job_id'], 'seq': task['seq'],
                              'rows': descriptor['rows'], 'processes': descriptor.get('processes', 1),
                              'threads': descriptor['threads'],
                              'status': task['status'], 'processed': progress.get('processed', task['processed']),
                              'created': task['created'], 'updated': task['updated'], 'error': task['error']})
            confirmed, outbox, processed = db.execute("SELECT COALESCE(SUM(CASE WHEN status='confirmed' THEN processed ELSE 0 END),0),COALESCE(SUM(CASE WHEN status='outbox' THEN processed ELSE 0 END),0),COALESCE(SUM(processed),0) FROM tasks").fetchone()
        return {'connected': bool(connected), 'error': error, 'last_success': last_success,
                'config': config, 'settings': settings, 'tasks': tasks, 'max_processes': self.max_processes,
                'max_threads': self.max_threads, 'active_processes': active_processes,
                'confirmed': confirmed, 'outbox': outbox, 'processed': processed, 'telemetry': dict(self.telemetry.latest),
                'hostname': socket.gethostname(), 'server_time': time.time()}
