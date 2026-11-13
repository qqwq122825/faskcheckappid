from collections import deque
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import math
import os
from pathlib import Path
import re
import secrets
import shutil
import threading
import time
import uuid

from .common import Database, event, file_digest, integer, private_dir

SCHEMA = '''
CREATE TABLE IF NOT EXISTS jobs (
 id TEXT PRIMARY KEY, name TEXT NOT NULL, status TEXT NOT NULL,
 created REAL NOT NULL, updated REAL NOT NULL, source TEXT NOT NULL,
 bytes INTEGER NOT NULL DEFAULT 0, rows INTEGER NOT NULL DEFAULT 0,
 unit_rows INTEGER NOT NULL, mode TEXT NOT NULL, error TEXT,
 c_valid INTEGER NOT NULL DEFAULT 0, c_wrong INTEGER NOT NULL DEFAULT 0,
 c_2fa INTEGER NOT NULL DEFAULT 0, c_locked INTEGER NOT NULL DEFAULT 0,
 c_timeout INTEGER NOT NULL DEFAULT 0, c_unknown INTEGER NOT NULL DEFAULT 0,
 reported_rows INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS nodes (
 id TEXT PRIMARY KEY, name TEXT NOT NULL, token_hash TEXT UNIQUE NOT NULL,
 created REAL NOT NULL, last_seen REAL, session TEXT, hostname TEXT,
 enabled INTEGER NOT NULL DEFAULT 1, desired_processes INTEGER NOT NULL DEFAULT 10,
 desired_threads INTEGER NOT NULL DEFAULT 100, max_processes INTEGER,
 max_threads INTEGER, version TEXT, last_total INTEGER NOT NULL DEFAULT 0,
 rate REAL NOT NULL DEFAULT 0, last_error TEXT
);
CREATE TABLE IF NOT EXISTS units (
 id TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES jobs(id), seq INTEGER NOT NULL,
 first_row INTEGER NOT NULL, rows INTEGER NOT NULL, path TEXT NOT NULL,
 checksum TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', node_id TEXT,
 lease_token TEXT, lease_until REAL, attempt INTEGER NOT NULL DEFAULT 0,
 processed INTEGER NOT NULL DEFAULT 0, result_path TEXT, result_checksum TEXT,
 updated REAL NOT NULL, UNIQUE(job_id,seq)
);
CREATE INDEX IF NOT EXISTS units_queue ON units(status,job_id,seq);
CREATE INDEX IF NOT EXISTS units_node ON units(node_id,status);
CREATE TABLE IF NOT EXISTS events (
 id INTEGER PRIMARY KEY AUTOINCREMENT, at REAL NOT NULL, code TEXT NOT NULL,
 message TEXT NOT NULL, job_id TEXT, node_id TEXT
);
CREATE TABLE IF NOT EXISTS controller_settings (
 key TEXT PRIMARY KEY, value INTEGER NOT NULL
);
INSERT OR IGNORE INTO controller_settings(key,value) VALUES('max_unit_rows',100000);
INSERT OR IGNORE INTO controller_settings(key,value) VALUES('proxy_api','');
INSERT OR IGNORE INTO controller_settings(key,value) VALUES('proxy_scheme','socks5h');
INSERT OR IGNORE INTO controller_settings(key,value) VALUES('origin','https://new.ani.sidestore.io');
INSERT OR IGNORE INTO controller_settings(key,value) VALUES('device_rotate_after','0');
'''


class Conflict(ValueError):
    pass


def mask_api(value):
    """回显时隐去 password/token，避免代理凭证出现在页面和接口响应里。"""
    if not value:
        return ''
    import re as _re
    return _re.sub(r'(?i)(password|passwd|pwd|token|key)=[^&\s]+', r'\1=***', str(value))


class Controller:
    MAX_PROCESSES = 200
    MAX_THREADS = 512
    MAX_ACTIVE_UNITS = 512

    def __init__(self, root, lease_seconds=30, offline_seconds=12, dispatch_per_second=0):
        self.root = private_dir(root)
        self.inputs = private_dir(self.root / 'inputs')
        self.units_dir = private_dir(self.root / 'units')
        self.results = private_dir(self.root / 'results')
        self.db = Database(self.root / 'controller.sqlite3', SCHEMA)
        with self.db.transaction() as db:
            job_columns = {row['name'] for row in db.execute('PRAGMA table_info(jobs)')}
            if 'queued_at' not in job_columns:
                db.execute('ALTER TABLE jobs ADD COLUMN queued_at REAL')
                db.execute("UPDATE jobs SET queued_at=created WHERE status IN ('running','paused')")
            columns = {row['name'] for row in db.execute('PRAGMA table_info(nodes)')}
            job_columns = {row['name'] for row in db.execute('PRAGMA table_info(jobs)')}
            for name in ('c_valid', 'c_wrong', 'c_2fa', 'c_locked', 'c_timeout',
                         'c_unknown', 'reported_rows'):
                if name not in job_columns:
                    db.execute(f'ALTER TABLE jobs ADD COLUMN {name} INTEGER NOT NULL DEFAULT 0')
            for name, definition in {'telemetry_json': "TEXT NOT NULL DEFAULT '{}'",
                                     'device_request': 'INTEGER NOT NULL DEFAULT 0',
             'stop_version': 'INTEGER NOT NULL DEFAULT 0',
                                     'ack_stop_version': 'INTEGER NOT NULL DEFAULT 0'}.items():
                if name not in columns:
                    db.execute(f'ALTER TABLE nodes ADD COLUMN {name} {definition}')
        self.lease_seconds, self.offline_seconds = lease_seconds, offline_seconds
        # 全局派发闸门：整个集群每秒最多放多少个新工作包。0 表示不限。
        self.dispatch_per_second = max(0, int(dispatch_per_second or 0))
        self.rate_lock = threading.Lock()
        self.dispatch_times = deque()
        self.preparer = ThreadPoolExecutor(max_workers=1, thread_name_prefix='file-preparation')
        self.closing = threading.Event()
        with self.db.transaction() as db:
            # Retain the oldest active task when upgrading an older installation.
            running = list(db.execute("SELECT id FROM jobs WHERE status='running' ORDER BY queued_at,created,id"))
            for row in running[1:]:
                db.execute("UPDATE jobs SET status='queued',queued_at=COALESCE(queued_at,created) WHERE id=?", (row['id'],))
            self._advance_queue(db)
            preparing = [r['id'] for r in db.execute("SELECT id FROM jobs WHERE status='preparing' ORDER BY created,id")]
        for job_id in preparing:
            self.preparer.submit(self.prepare, job_id)

    def close(self):
        self.closing.set()
        self.preparer.shutdown(wait=True, cancel_futures=True)

    def add_node(self, name, processes=10, threads=100):
        name = str(name).strip()[:80]
        if not name:
            raise ValueError('请填写节点名称')
        integer(processes, '进程数', 1, self.MAX_PROCESSES)
        integer(threads, '线程数', 1, self.MAX_THREADS)
        node_id, token = uuid.uuid4().hex, secrets.token_urlsafe(32)
        with self.db.transaction() as db:
            db.execute('INSERT INTO nodes(id,name,token_hash,created,desired_processes,desired_threads) VALUES(?,?,?,?,?,?)',
                       (node_id, name, hashlib.sha256(token.encode()).hexdigest(), time.time(), processes, threads))
            event(db, 'node_added', f'已添加节点：{name}', node_id=node_id)
        return {'id': node_id, 'name': name, 'token': token}

    def authenticate(self, token):
        if not token:
            return None
        with self.db.connect() as db:
            row = db.execute('SELECT id FROM nodes WHERE token_hash=?',
                             (hashlib.sha256(token.encode()).hexdigest(),)).fetchone()
            return row['id'] if row else None

    def configure_node(self, node_id, data):
        processes = integer(data.get('processes'), '进程数', 1, self.MAX_PROCESSES)
        threads = integer(data.get('threads'), '线程数', 1, self.MAX_THREADS)
        if type(data.get('enabled')) is not bool:
            raise ValueError('节点接单状态无效')
        with self.db.transaction() as db:
            node = db.execute('SELECT * FROM nodes WHERE id=?', (node_id,)).fetchone()
            if not node:
                raise ValueError('节点不存在')
            active = db.execute("SELECT COUNT(*) FROM units WHERE node_id=? AND status IN ('leased','running')", (node_id,)).fetchone()[0]
            if data['enabled'] and (node['stop_version'] > node['ack_stop_version'] or
                                    not node['enabled'] and node['stop_version'] > 0 and active):
                raise Conflict('节点尚未确认停止，暂不能恢复接单')
            db.execute('UPDATE nodes SET desired_processes=?,desired_threads=?,enabled=? WHERE id=?',
                       (processes, threads, int(data['enabled']), node_id))
            event(db, 'node_config', '节点执行配置已更新，下一次心跳下发；在途工作包保持原配置', node_id=node_id)

    def create_job(self, name, stream, unit_rows=500, mode='auth', enqueue=False):
        integer(unit_rows, '每包记录数', 50, self.settings()['max_unit_rows'])
        if type(enqueue) is not bool:
            raise ValueError('加入队列选项无效')
        if mode not in ('digest', 'simulate', 'auth'):
            raise ValueError('执行模式应为 digest / simulate / auth')
        job_id = uuid.uuid4().hex
        target = self.inputs / (job_id + '.txt')
        with target.open('xb') as output:
            target.chmod(0o600)
            shutil.copyfileobj(stream, output, 1024 * 1024)
        if not target.stat().st_size:
            target.unlink()
            raise ValueError('文件为空')
        now = time.time()
        with self.db.transaction() as db:
            db.execute('INSERT INTO jobs(id,name,status,created,updated,source,bytes,unit_rows,mode,queued_at) VALUES(?,?,?,?,?,?,?,?,?,?)',
                       (job_id, str(name).strip()[:120] or '未命名任务', 'preparing', now, now,
                        str(target), target.stat().st_size, unit_rows, mode, now if enqueue else None))
            event(db, 'job_uploaded', '输入文件已保存，正在后台准备工作包', job_id=job_id)
        self.preparer.submit(self.prepare, job_id)
        return job_id

    def settings(self):
        with self.db.connect() as db:
            values = {row['key']: row['value'] for row in db.execute('SELECT key,value FROM controller_settings')}
        maximum = int(values.get('max_unit_rows', 100000))
        return {'max_unit_rows': maximum, 'default_unit_rows': min(10000, maximum),
                'unit_byte_limit': 32 * 1024 * 1024,
                'proxy_api': mask_api(values.get('proxy_api', '')),
                'proxy_configured': bool(values.get('proxy_api', '')),
                'proxy_scheme': values.get('proxy_scheme', 'socks5h'),
                'device_rotate_after': int(values.get('device_rotate_after', 0) or 0)}

    def proxy_for_node(self, node):
        """节点专属出口：按创建顺序把链接分给各节点，避免所有节点抢同一条配额。"""
        config = self.proxy_config()
        parts = [part for part in (config.get('api') or '').split() if part]
        if not parts:
            return config
        with self.db.connect() as db:
            index = db.execute('SELECT COUNT(*) FROM nodes WHERE created < ?',
                               (node['created'],)).fetchone()[0]
        return {'api': parts[index % len(parts)], 'scheme': config.get('scheme', 'socks5h'),
                'shared': len(parts) < index + 1, 'index': index % len(parts)}

    def proxy_config(self):
        """下发节点用的原文配置；只在服务端内部调用，不进任何接口响应。"""
        with self.db.connect() as db:
            values = {row['key']: row['value'] for row in db.execute('SELECT key,value FROM controller_settings')}
        return {'api': values.get('proxy_api', ''), 'scheme': values.get('proxy_scheme', 'socks5h')}

    def configure_settings(self, data):
        maximum = integer(data.get('max_unit_rows'), '单个分片最大条数', 50, 100000)
        with self.db.transaction() as db:
            db.execute("UPDATE controller_settings SET value=? WHERE key='max_unit_rows'", (maximum,))
            event(db, 'controller_config', f'新任务单个分片最大条数已设置为 {maximum:,} 条；已有任务保持原配置')
        return self.settings()

    def configure_rotation(self, data):
        limit = integer(int(data.get('device_rotate_after') or 0), '设备轮换阈值', 0, 100000)
        with self.db.transaction() as db:
            db.execute("UPDATE controller_settings SET value=? WHERE key='device_rotate_after'", (limit,))
            event(db, 'controller_config',
                  f'设备轮换阈值设为 {limit} 条' if limit else '设备轮换已关闭')
        return self.settings()

    def configure_proxy(self, data):
        api = str(data.get('proxy_api', '')).strip()
        scheme = str(data.get('proxy_scheme', 'socks5h')).strip()
        if scheme not in ('socks5h', 'socks5', 'http', 'https'):
            raise ValueError('代理协议应为 socks5h / socks5 / http / https')
        if api:
            from urllib.parse import urlsplit
            parts = [p for p in api.split() if p]
            if len(parts) > 16:
                raise ValueError('最多支持 16 个代理 API 链接')
            for part in parts:
                parsed = urlsplit(part)
                if parsed.scheme not in ('http', 'https') or not parsed.hostname:
                    raise ValueError('代理 API 需要是 http(s) 地址')
        with self.db.transaction() as db:
            db.execute("UPDATE controller_settings SET value=? WHERE key='proxy_api'", (api,))
            db.execute("UPDATE controller_settings SET value=? WHERE key='proxy_scheme'", (scheme,))
            event(db, 'controller_config', '代理 API 配置已更新' if api else '代理 API 配置已清空')
        return self.settings()

    def request_devices(self, node_id, capacity):
        """把设备初始化意图挂到节点上，节点下次心跳取走执行。"""
        capacity = integer(int(capacity), '设备数量', 0, 200)
        with self.db.transaction() as db:
            node = db.execute('SELECT * FROM nodes WHERE id=?', (node_id,)).fetchone()
            if not node:
                raise ValueError('节点不存在')
            if not node['last_seen'] or time.time() - node['last_seen'] > self.offline_seconds:
                raise Conflict('节点离线，无法初始化设备')
            db.execute('UPDATE nodes SET device_request=? WHERE id=?', (capacity, node_id))
            event(db, 'device_build_requested', f'已请求节点初始化 {capacity} 台设备', node_id=node_id)
        return {'requested': capacity}

    def stop_node(self, node_id):
        with self.db.transaction() as db:
            changed = db.execute('UPDATE nodes SET enabled=0,stop_version=stop_version+1 WHERE id=?', (node_id,)).rowcount
            if not changed:
                raise ValueError('节点不存在')
            event(db, 'node_stop_requested', '已请求停止节点当前工作并暂停接单，等待节点确认', node_id=node_id)

    @staticmethod
    def clean_telemetry(value):
        if not isinstance(value, dict):
            return {}
        result = {}
        for key, maximum in {'cpu_percent': 100, 'memory_percent': 100,
                             'upload_bps': 10**15, 'download_bps': 10**15}.items():
            number = value.get(key)
            result[key] = round(float(number), 1) if type(number) in (int, float) and math.isfinite(number) and 0 <= number <= maximum else None
        result.update(received_at=time.time(), scope='host_non_loopback')
        processes = value.get('worker_processes')
        result['worker_processes'] = processes if type(processes) is int and 0 <= processes <= 200 else None
        pool = value.get('device_pool')
        if isinstance(pool, dict):
            result['device_pool'] = {key: pool.get(key) for key in
                                     ('running', 'capacity', 'ready', 'built', 'failed', 'current')}
        return result

    def prepare(self, job_id):
        directory = private_dir(self.units_dir / job_id)
        try:
            with self.db.transaction() as db:
                job = db.execute('SELECT * FROM jobs WHERE id=?', (job_id,)).fetchone()
                if job['status'] != 'preparing':
                    return
                db.execute('DELETE FROM units WHERE job_id=?', (job_id,))
                db.execute('UPDATE jobs SET rows=0 WHERE id=?', (job_id,))
            # Only unstarted preparation is rebuilt after a controller restart.
            for old in directory.glob('*.part'):
                old.unlink()
            # 字节预算必须提到循环外：每行调一次 settings() 就是每行一次数据库查询，
            # 10 万行会被这种写法拖到 30 秒以上。
            byte_limit = self.settings()['unit_byte_limit']
            seq, rows, first_row, buffer, buffered_bytes = 0, 0, 1, [], 0
            with Path(job['source']).open('rb') as source:
                while True:
                    if self.closing.is_set():
                        return
                    line = source.readline(65537)
                    if not line:
                        break
                    if len(line) > 65536:
                        raise ValueError('单行超过 64 KiB；请将超长记录拆分后重试')
                    line.decode('utf-8-sig' if rows == 0 else 'utf-8')
                    line = line if line.endswith(b'\n') else line + b'\n'
                    if buffer and buffered_bytes + len(line) > byte_limit:
                        seq += 1
                        self._save_unit(job_id, seq, first_row, buffer, directory, rows)
                        first_row, buffer, buffered_bytes = rows + 1, [], 0
                    buffer.append(line)
                    buffered_bytes += len(buffer[-1])
                    rows += 1
                    if len(buffer) == job['unit_rows'] or buffered_bytes >= byte_limit:
                        seq += 1
                        self._save_unit(job_id, seq, first_row, buffer, directory, rows)
                        first_row, buffer, buffered_bytes = rows + 1, [], 0
                if buffer:
                    self._save_unit(job_id, seq + 1, first_row, buffer, directory, rows)
            with self.db.transaction() as db:
                db.execute("UPDATE jobs SET status=CASE WHEN queued_at IS NOT NULL THEN 'queued' ELSE 'ready' END,rows=?,updated=? WHERE id=?", (rows, time.time(), job_id))
                event(db, 'job_ready', f'文件准备完成，共 {rows:,} 条记录', job_id=job_id)
                self._advance_queue(db)
        except Exception as exc:
            message = str(exc) if isinstance(exc, (ValueError, UnicodeError)) else type(exc).__name__
            with self.db.transaction() as db:
                db.execute("UPDATE jobs SET status='failed',queued_at=NULL,error=?,updated=? WHERE id=?", (message, time.time(), job_id))
                event(db, 'prepare_failed', message, job_id=job_id)
                self._advance_queue(db)

    def _advance_queue(self, db):
        if db.execute("SELECT 1 FROM jobs WHERE status IN ('running','paused') LIMIT 1").fetchone():
            return
        # Older versions could have leased work belonging to another task.
        if db.execute("SELECT 1 FROM units WHERE status IN ('leased','running') LIMIT 1").fetchone():
            return
        head = db.execute("SELECT id,status FROM jobs WHERE status='queued' OR (status='preparing' AND queued_at IS NOT NULL) ORDER BY queued_at,id LIMIT 1").fetchone()
        if head and head['status'] == 'queued':
            db.execute("UPDATE jobs SET status='running',updated=? WHERE id=?", (time.time(), head['id']))
            event(db, 'job_started', '轮到本任务，自动开始派发', job_id=head['id'])

    def _save_unit(self, job_id, seq, first_row, lines, directory, rows):
        path = directory / f'{seq:08d}.part'
        payload = b''.join(lines)
        with path.open('wb') as output:
            path.chmod(0o600)
            output.write(payload)
        with self.db.transaction() as db:
            db.execute('INSERT INTO units(id,job_id,seq,first_row,rows,path,checksum,updated) VALUES(?,?,?,?,?,?,?,?)',
                       (uuid.uuid4().hex, job_id, seq, first_row, len(lines), str(path),
                        hashlib.sha256(payload).hexdigest(), time.time()))
            db.execute('UPDATE jobs SET rows=?,updated=? WHERE id=?', (rows, time.time(), job_id))

    def job_action(self, job_id, action):
        with self.db.transaction() as db:
            job = db.execute('SELECT * FROM jobs WHERE id=?', (job_id,)).fetchone()
            if not job:
                raise ValueError('任务不存在')
            if action == 'start' and job['status'] == 'ready':
                db.execute("UPDATE jobs SET status='queued',queued_at=?,updated=? WHERE id=?", (time.time(), time.time(), job_id))
                event(db, 'job_queued', '任务已加入执行队列', job_id=job_id)
                self._advance_queue(db)
            elif action == 'start' and job['status'] == 'paused':
                db.execute("UPDATE jobs SET status='running',updated=? WHERE id=?", (time.time(), job_id))
                event(db, 'job_started', '继续当前任务，恢复派发', job_id=job_id)
            elif action == 'start' and job['status'] in ('queued', 'running'):
                pass
            elif action == 'dequeue' and (job['status'] == 'queued' or job['status'] == 'preparing' and job['queued_at'] is not None):
                db.execute("UPDATE jobs SET status=CASE WHEN status='queued' THEN 'ready' ELSE status END,queued_at=NULL,updated=? WHERE id=?", (time.time(), job_id))
                event(db, 'job_dequeued', '任务已移出队列，可稍后重新加入', job_id=job_id)
                self._advance_queue(db)
            elif action == 'pause' and job['status'] == 'running':
                db.execute("UPDATE jobs SET status='paused',updated=? WHERE id=?", (time.time(), job_id))
                event(db, 'job_paused', '已暂停派发，在途工作包继续完成', job_id=job_id)
            elif action == 'stop' and job['status'] in ('queued', 'running', 'paused', 'ready'):
                db.execute("UPDATE jobs SET status='stopped',queued_at=NULL,updated=? WHERE id=?",
                           (time.time(), job_id))
                db.execute("UPDATE units SET status='pending',node_id=NULL,lease_token=NULL,"
                           "lease_until=NULL,processed=0,updated=? WHERE job_id=? "
                           "AND status IN ('leased','running','unknown')", (time.time(), job_id))
                event(db, 'job_stopped', '任务已停止，在途工作包退回待分配', job_id=job_id)
                self._advance_queue(db)
            elif action == 'retry_unknown' and job['status'] in ('running', 'paused', 'ready'):
                count = db.execute("UPDATE units SET status='pending',node_id=NULL,lease_token=NULL,lease_until=NULL,processed=0,updated=? WHERE job_id=? AND status='unknown'",
                                   (time.time(), job_id)).rowcount
                event(db, 'units_requeued', f'人工重新排队 {count} 个待确认工作包（仅离线可重复任务）', job_id=job_id)
            else:
                raise Conflict('当前任务状态不支持此操作')

    def _expire(self, db):
        now = time.time()
        expired = list(db.execute("SELECT id,job_id,node_id FROM units WHERE status IN ('leased','running') AND lease_until<?", (now,)))
        for unit in expired:
            # 只置为待确认，保留 lease_token：节点可能只是算得慢，结果上传时还要靠它
            # 认领；一旦这一包被重新派发给别人，token 会被覆盖，旧结果自然被拒。
            db.execute("UPDATE units SET status='unknown',lease_until=NULL,updated=? WHERE id=?", (now, unit['id']))
            event(db, 'lease_expired', '工作包租约过期，已转为待确认；未自动重跑', unit['job_id'], unit['node_id'])

    def heartbeat(self, node_id, data):
        now = time.time()
        session = str(data.get('session', ''))[:64]
        if not session:
            raise ValueError('缺少节点会话')
        max_p = integer(data.get('max_processes'), '节点进程上限', 1, self.MAX_PROCESSES)
        max_t = integer(data.get('max_threads'), '节点线程上限', 1, self.MAX_THREADS)
        active = data.get('active', [])
        interrupted = data.get('interrupted', [])
        if not isinstance(active, list) or len(active) > self.MAX_ACTIVE_UNITS:
            raise ValueError('节点工作包列表无效')
        if not isinstance(interrupted, list) or len(interrupted) > self.MAX_ACTIVE_UNITS:
            raise ValueError('节点中断工作包列表无效')
        with self.db.transaction() as db:
            self._expire(db)
            node = db.execute('SELECT * FROM nodes WHERE id=?', (node_id,)).fetchone()
            if (node['session'] and node['session'] != session and node['last_seen']
                    and now - node['last_seen'] < self.offline_seconds):
                raise Conflict('同一节点身份已有另一个实例在线；请等待旧实例离线或使用独立接入凭证')
            reject, confirmed, interrupted_ack = [], [], []
            for value in interrupted:
                unit = db.execute('SELECT * FROM units WHERE id=? AND node_id=?', (value.get('id'), node_id)).fetchone()
                if not unit or unit['lease_token'] != value.get('token') or unit['status'] not in ('leased', 'running'):
                    reject.append(value.get('id'))
                    continue
                count = integer(value.get('processed', 0), '中断记录数', 0, unit['rows'])
                db.execute("UPDATE units SET status='unknown',processed=?,lease_token=NULL,lease_until=NULL,updated=? WHERE id=?",
                           (count, now, unit['id']))
                interrupted_ack.append(unit['id'])
                event(db, 'unit_interrupted', f'节点已中止工作包 #{unit["seq"]}，未完成包进入待确认', unit['job_id'], node_id)
            for value in active:
                unit = db.execute('SELECT * FROM units WHERE id=? AND node_id=?', (value.get('id'), node_id)).fetchone()
                if unit and unit['lease_token'] == value.get('token') and unit['status'] == 'completed':
                    confirmed.append(unit['id'])
                    continue
                if not unit or unit['lease_token'] != value.get('token') or unit['status'] not in ('leased', 'running'):
                    reject.append(value.get('id'))
                    continue
                count = integer(value.get('processed', 0), '已处理数量', 0, unit['rows'])
                db.execute("UPDATE units SET status='running',processed=MAX(processed,?),lease_until=?,updated=? WHERE id=?",
                           (count, now + self.lease_seconds, now, unit['id']))
            total = db.execute("SELECT COALESCE(SUM(processed),0) FROM units WHERE node_id=? AND status IN ('leased','running','completed')", (node_id,)).fetchone()[0]
            elapsed = now - (node['last_seen'] or now)
            instant = max(0, total - node['last_total']) / elapsed if elapsed > 0.1 else 0
            rate = instant if not node['last_seen'] or elapsed > self.offline_seconds else 0.55 * node['rate'] + 0.45 * instant
            db.execute('UPDATE nodes SET last_seen=?,session=?,hostname=?,version=?,max_processes=?,max_threads=?,last_total=?,rate=?,last_error=? WHERE id=?',
                       (now, session, str(data.get('hostname', ''))[:120], str(data.get('version', ''))[:30],
                        max_p, max_t, total, rate, str(data.get('error', ''))[:200], node_id))
            ack_stop = data.get('ack_stop_version', 0)
            ack_stop = max(0, min(ack_stop, node['stop_version'])) if type(ack_stop) is int else 0
            merged = dict(data.get('telemetry') or {})
            if isinstance(data.get('devices'), dict):
                merged['device_pool'] = data['devices']
            db.execute('UPDATE nodes SET telemetry_json=?,ack_stop_version=? WHERE id=?',
                       (json.dumps(self.clean_telemetry(merged)), ack_stop, node_id))
            if not node['last_seen'] or now - node['last_seen'] > self.offline_seconds:
                event(db, 'node_online', f'节点已连接：{node["name"]}', node_id=node_id)
            return {'node_id': node_id, 'name': node['name'], 'enabled': bool(node['enabled']),
                    'processes': min(node['desired_processes'], max_p), 'threads': min(node['desired_threads'], max_t),
                    'desired_processes': node['desired_processes'], 'desired_threads': node['desired_threads'],
                    'rejected': reject, 'confirmed': confirmed, 'interrupted_ack': interrupted_ack,
                    'device_request': node['device_request'],
                    'device_rotate_after': self.settings().get('device_rotate_after', 0),
                    'stop_version': node['stop_version'], 'lease_seconds': self.lease_seconds,
                    'server_time': now,
                    'config': {**self.proxy_for_node(node),
                               'origin': self.settings().get('origin') or 'https://new.ani.sidestore.io'}}

    def _rate_allows(self):
        """整个集群每秒最多派发 dispatch_per_second 个包；0 表示不限。"""
        if not self.dispatch_per_second:
            return True
        now = time.monotonic()
        with self.rate_lock:
            while self.dispatch_times and now - self.dispatch_times[0] >= 1.0:
                self.dispatch_times.popleft()
            return len(self.dispatch_times) < self.dispatch_per_second

    def _rate_record(self):
        if self.dispatch_per_second:
            with self.rate_lock:
                self.dispatch_times.append(time.monotonic())

    def lease(self, node_id, session):
        if not self._rate_allows():
            return None
        with self.db.transaction() as db:
            self._expire(db)
            node = db.execute('SELECT * FROM nodes WHERE id=?', (node_id,)).fetchone()
            if node['session'] != session or not node['last_seen'] or time.time() - node['last_seen'] > self.offline_seconds:
                raise Conflict('请先完成有效心跳')
            active = db.execute("SELECT COUNT(*) FROM units WHERE node_id=? AND status IN ('leased','running')", (node_id,)).fetchone()[0]
            if not node['enabled'] or active >= min(node['desired_processes'], node['max_processes'] or 1):
                return None
            unit = db.execute("SELECT u.*,j.mode FROM units u JOIN jobs j ON j.id=u.job_id WHERE u.status='pending' AND j.id=(SELECT id FROM jobs WHERE status='running' ORDER BY queued_at,created,id LIMIT 1) ORDER BY u.seq LIMIT 1").fetchone()
            if not unit:
                return None
            token = secrets.token_urlsafe(24)
            db.execute("UPDATE units SET status='leased',node_id=?,lease_token=?,lease_until=?,attempt=attempt+1,processed=0,updated=? WHERE id=?",
                       (node_id, token, time.time() + self.lease_seconds, time.time(), unit['id']))
            event(db, 'unit_leased', f'工作包 #{unit["seq"]} 已分配（{unit["rows"]:,} 条）', unit['job_id'], node_id)
            self._rate_record()
            payload = {'id': unit['id'], 'job_id': unit['job_id'], 'seq': unit['seq'],
                       'first_row': unit['first_row'], 'rows': unit['rows'],
                       'checksum': unit['checksum'], 'token': token, 'mode': unit['mode'],
                       'threads': min(node['desired_threads'], node['max_threads'] or 1),
                       'lease_seconds': self.lease_seconds}
            if unit['mode'] == 'auth':
                # 认证任务额外带设备与出口配置；代理地址取服务端保存的原文。
                config = self.proxy_config()
                payload.update(proxy_api=config['api'], proxy_scheme=config['scheme'],
                               origin=self.settings().get('origin', 'https://new.ani.sidestore.io'))
            return payload

    def unit_for_node(self, node_id, unit_id, token):
        with self.db.transaction() as db:
            self._expire(db)
            unit = db.execute('SELECT * FROM units WHERE id=? AND node_id=?', (unit_id, node_id)).fetchone()
            if (not unit or not secrets.compare_digest(unit['lease_token'] or '', token or '')
                    or unit['status'] not in ('leased', 'running', 'completed', 'unknown')):
                raise Conflict('工作包租约已失效')
            return dict(unit)

    def accept_result(self, node_id, unit_id, token, stream, expected_digest):
        # 先把租约推到当前时刻：结果文件可能十几 MB，读完再校验时租约容易刚好过期，
        # 那会把节点辛苦算完的结果当成失效包丢掉。
        with self.db.transaction() as db:
            db.execute("UPDATE units SET lease_until=? WHERE id=? AND node_id=? AND lease_token=? "
                       "AND status IN ('leased','running','unknown')",
                       (time.time() + self.lease_seconds, unit_id, node_id, token))
        unit = self.unit_for_node(node_id, unit_id, token)
        temporary = self.results / (secrets.token_hex(16) + '.upload')
        digest, count = hashlib.sha256(), 0
        from collections import Counter as _Counter
        buckets = _Counter()
        try:
            with temporary.open('xb') as output:
                temporary.chmod(0o600)
                while True:
                    line = stream.readline(1025)
                    if not line:
                        break
                    if len(line) > 1024 or not line.endswith(b'\n'):
                        raise ValueError('结果记录格式无效')
                    value = json.loads(line)
                    # 必需字段固定，允许执行器附加 account/status 等业务字段
                    if (not {'row', 'sha256', 'bytes'} <= set(value)
                            or type(value['row']) is not int
                            or value['row'] != unit['first_row'] + count
                            or not isinstance(value['sha256'], str) or not re.fullmatch('[0-9a-f]{64}', value['sha256'])
                            or type(value['bytes']) is not int or value['bytes'] < 0):
                        raise ValueError('结果编号或摘要格式无效')
                    bucket = value.get('status') or 'valid'
                    if bucket not in ('valid', 'wrong_password', 'need_2fa',
                                      'locked', 'timeout', 'undetermined', 'unknown'):
                        bucket = 'unknown'
                    buckets[bucket] += 1
                    count += 1
                    if count > unit['rows']:
                        raise ValueError('结果条数超过工作包记录数')
                    digest.update(line)
                    output.write(line)
                output.flush()
                os.fsync(output.fileno())
            if count != unit['rows'] or digest.hexdigest() != expected_digest:
                raise ValueError('结果条数或文件校验值不一致')
            with self.db.transaction() as db:
                self._expire(db)
                current = db.execute('SELECT * FROM units WHERE id=?', (unit_id,)).fetchone()
                if current['lease_token'] != token or current['node_id'] != node_id:
                    raise Conflict('迟到的结果不覆盖当前任务状态')
                if current['status'] == 'completed':
                    if current['result_checksum'] != expected_digest:
                        raise Conflict('重复结果内容不一致')
                    return {'accepted': True, 'duplicate': True}
                # 允许 unknown：租约刚好过期时节点仍在算，结果本身是有效的；
                # 如果这一包已经被重新派给别人，lease_token 就会变，前面那道校验会拦住。
                if current['status'] not in ('leased', 'running', 'unknown'):
                    raise Conflict('当前工作包不接受结果')
                final = self.results / (unit_id + '.' + hashlib.sha256(token.encode()).hexdigest()[:12] + '.jsonl')
                os.replace(temporary, final)
                # 六类计数与分片确认在同一个事务里累加；重复提交走上面的 completed
                # 分支直接返回，不会二次累加。分类取自结果行里的 status。
                job_mode = db.execute('SELECT mode FROM jobs WHERE id=?',
                                      (unit['job_id'],)).fetchone()['mode']
                if job_mode == 'auth':
                    # 只有账号校验才产生业务分类；摘要任务记成"密正"会误导
                    db.execute(
                        "UPDATE jobs SET c_valid=c_valid+?,c_wrong=c_wrong+?,c_2fa=c_2fa+?,"
                        "c_locked=c_locked+?,c_timeout=c_timeout+?,c_unknown=c_unknown+?,updated=? WHERE id=?",
                        (buckets['valid'], buckets['wrong_password'], buckets['need_2fa'],
                         buckets['locked'], buckets['timeout'],
                         buckets['undetermined'] + buckets['unknown'], time.time(), unit['job_id']))
                else:
                    db.execute("UPDATE jobs SET updated=? WHERE id=?", (time.time(), unit['job_id']))
                db.execute("UPDATE units SET status='completed',processed=rows,result_path=?,result_checksum=?,lease_until=NULL,updated=? WHERE id=?",
                           (str(final), expected_digest, time.time(), unit_id))
                event(db, 'result_confirmed', f'工作包 #{unit["seq"]} 结果已确认（{count:,} 条）', unit['job_id'], node_id)
                unfinished = db.execute("SELECT COUNT(*) FROM units WHERE job_id=? AND status!='completed'", (unit['job_id'],)).fetchone()[0]
                if not unfinished:
                    db.execute("UPDATE jobs SET status='completed',updated=? WHERE id=?", (time.time(), unit['job_id']))
                    event(db, 'job_completed', '全部工作包已确认，任务完成', unit['job_id'])
                    self._advance_queue(db)
                return {'accepted': True, 'duplicate': False}
        finally:
            temporary.unlink(missing_ok=True)

    def snapshot(self):
        now = time.time()
        with self.db.transaction() as db:
            self._expire(db)
            jobs = []
            queue_filter = "(status='queued' OR (status='preparing' AND queued_at IS NOT NULL))"
            waiting_count = db.execute(f'SELECT COUNT(*) FROM jobs WHERE {queue_filter}').fetchone()[0]
            queue = [dict(row) for row in db.execute(f'SELECT id,name,status,rows FROM jobs WHERE {queue_filter} ORDER BY queued_at,id LIMIT 10')]
            for position, entry in enumerate(queue, 1):
                entry['position'] = position
            current = db.execute("SELECT id FROM jobs WHERE status IN ('running','paused') ORDER BY queued_at,created,id LIMIT 1").fetchone()
            for row in db.execute("SELECT * FROM jobs ORDER BY CASE WHEN status IN ('running','paused') THEN 0 WHEN status='queued' OR (status='preparing' AND queued_at IS NOT NULL) THEN 1 ELSE 2 END,CASE WHEN status IN ('queued','running','paused') OR (status='preparing' AND queued_at IS NOT NULL) THEN queued_at END,created DESC,id LIMIT 50"):
                value = {k: row[k] for k in ('id', 'name', 'status', 'created', 'updated', 'bytes', 'rows', 'unit_rows', 'mode', 'error', 'queued_at')}
                value['queue_position'] = (db.execute(f'SELECT COUNT(*) FROM jobs WHERE {queue_filter} AND (queued_at<? OR (queued_at=? AND id<=?))', (row['queued_at'], row['queued_at'], row['id'])).fetchone()[0]
                                           if row['status'] in ('queued', 'preparing') and row['queued_at'] is not None else None)
                groups = db.execute('SELECT status,COUNT(*) AS n,SUM(rows) AS rows FROM units WHERE job_id=? GROUP BY status', (row['id'],)).fetchall()
                counts = {r['status']: r['n'] for r in groups}
                row_counts = {r['status']: r['rows'] for r in groups}
                sums = db.execute("SELECT COALESCE(SUM(CASE WHEN status='completed' THEN rows ELSE 0 END),0), COALESCE(SUM(CASE WHEN status IN ('running','leased','completed') THEN processed ELSE 0 END),0) FROM units WHERE job_id=?", (row['id'],)).fetchone()
                value.update(units=sum(counts.values()), unit_counts=counts, row_counts=row_counts,
                             confirmed=sums[0], processed=sums[1],
                             categorized={'valid': row['c_valid'], 'wrong_password': row['c_wrong'],
                                          'need_2fa': row['c_2fa'], 'locked': row['c_locked'],
                                          'timeout': row['c_timeout'], 'unknown': row['c_unknown']},
                             reported=row['reported_rows'])
                jobs.append(value)
            nodes = []
            for row in db.execute('SELECT * FROM nodes ORDER BY created'):
                value = {k: row[k] for k in ('id', 'name', 'last_seen', 'hostname', 'enabled', 'desired_processes', 'desired_threads', 'max_processes', 'max_threads', 'version', 'last_error')}
                online = bool(row['last_seen'] and now - row['last_seen'] < self.offline_seconds)
                stats = db.execute("SELECT COALESCE(SUM(CASE WHEN status IN ('leased','running') THEN 1 ELSE 0 END),0), COALESCE(SUM(CASE WHEN status='completed' THEN rows ELSE 0 END),0), COALESCE(SUM(CASE WHEN status IN ('leased','running') THEN processed ELSE 0 END),0) FROM units WHERE node_id=?", (row['id'],)).fetchone()
                value.update(status='offline' if not online else 'draining' if not row['enabled'] else 'busy' if stats[0] else 'idle',
                             active=stats[0], confirmed=stats[1], in_progress=stats[2], rate=round(row['rate'], 1) if online and stats[0] else 0)
                if online and (row['stop_version'] > row['ack_stop_version'] or
                               not row['enabled'] and row['stop_version'] > 0 and stats[0]):
                    value['status'] = 'stopping'
                value['telemetry'] = json.loads(row['telemetry_json']) if online else {}
                value['effective_processes'] = min(row['desired_processes'], row['max_processes']) if row['max_processes'] else None
                value['effective_threads'] = min(row['desired_threads'], row['max_threads']) if row['max_threads'] else None
                nodes.append(value)
            events = [dict(r) for r in db.execute('SELECT * FROM events ORDER BY id DESC LIMIT 40')]
            aggregate = db.execute('SELECT COALESCE(SUM(rows),0),COUNT(*) FROM jobs').fetchone()
            counts = db.execute("SELECT COALESCE(SUM(CASE WHEN status='completed' THEN rows ELSE 0 END),0),COALESCE(SUM(CASE WHEN status IN ('running','leased','completed') THEN processed ELSE 0 END),0) FROM units").fetchone()
            maximum = db.execute("SELECT value FROM controller_settings WHERE key='max_unit_rows'").fetchone()[0]
            return {'jobs': jobs, 'nodes': nodes, 'events': events, 'server_time': now,
                    'current_job_id': current['id'] if current else None, 'queue': queue,
                    'settings': self.settings(),
                    'totals': {'rows': aggregate[0], 'jobs': aggregate[1], 'confirmed': counts[0],
                               'processed': counts[1], 'online': sum(n['status'] != 'offline' for n in nodes),
                               'nodes': len(nodes), 'busy': sum(n['status'] != 'offline' and n['active'] > 0 for n in nodes),
                               'queued': waiting_count, 'rate': round(sum(n['rate'] for n in nodes), 1)}}

    def read_rows(self, job_id, offset=0, limit=100, category='all'):
        """按行号窗口读取结果，供页面表格分页。

        只读已确认分片的结果文件，不加载整个任务；单个分片 10 万行读一遍约 100 ms，
        1 亿条任务翻页也不会随规模变慢。
        """
        limit = max(1, min(int(limit), 500))
        offset = max(0, int(offset))
        with self.db.connect() as db:
            job = db.execute('SELECT rows,rows AS total FROM jobs WHERE id=?', (job_id,)).fetchone()
            if not job:
                raise ValueError('任务不存在')
            units = list(db.execute(
                "SELECT seq,first_row,rows,result_path FROM units WHERE job_id=? AND status='completed' "
                "ORDER BY seq", (job_id,)))
        rows, skipped = [], 0
        for unit in units:
            span_start, span_end = unit['first_row'], unit['first_row'] + unit['rows']
            path = unit['result_path']
            if not path or not Path(path).exists():
                continue
            try:
                with Path(path).open('r', encoding='utf-8') as stream:
                    for line in stream:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            record = json.loads(line)
                        except ValueError:
                            continue
                        if category != 'all' and record.get('status') != category:
                            continue
                        if skipped < offset:
                            skipped += 1
                            continue
                        rows.append(record)
                        if len(rows) >= limit:
                            return {'rows': rows, 'total': job['total'], 'offset': offset}
            except OSError:
                continue
        return {'rows': rows, 'total': job['total'], 'offset': offset}

    def result_paths(self, job_id):
        with self.db.connect() as db:
            job = db.execute('SELECT status FROM jobs WHERE id=?', (job_id,)).fetchone()
            if not job or job['status'] != 'completed':
                raise Conflict('全部工作包确认后才可下载完整结果')
            return [r['result_path'] for r in db.execute('SELECT result_path FROM units WHERE job_id=? ORDER BY seq', (job_id,))]

    def text_export(self, job_id):
        with self.db.connect() as db:
            job = db.execute('SELECT status,source,rows FROM jobs WHERE id=?', (job_id,)).fetchone()
            confirmed = db.execute("SELECT COALESCE(SUM(CASE WHEN status='completed' THEN rows ELSE 0 END),0) FROM units WHERE job_id=?", (job_id,)).fetchone()[0]
            if not job or job['status'] != 'completed' or confirmed != job['rows']:
                raise Conflict('全部工作包确认后才可下载完整结果')
            # All records passed the offline executor. Stream the original TXT to
            # preserve ordering, duplicate records, encoding and line endings.
            return Path(job['source']), job['rows']
