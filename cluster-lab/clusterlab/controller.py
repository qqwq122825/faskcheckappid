from collections import Counter, OrderedDict, deque
from concurrent.futures import ThreadPoolExecutor
from difflib import SequenceMatcher
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
import unicodedata
import uuid

from .common import Database, event, file_digest, integer, private_dir

DEVICE_DETAIL_LIMIT = 2000

SCHEMA = '''
CREATE TABLE IF NOT EXISTS jobs (
 id TEXT PRIMARY KEY, name TEXT NOT NULL, status TEXT NOT NULL,
 created REAL NOT NULL, updated REAL NOT NULL, source TEXT NOT NULL,
 bytes INTEGER NOT NULL DEFAULT 0, rows INTEGER NOT NULL DEFAULT 0,
 unit_rows INTEGER NOT NULL, mode TEXT NOT NULL, error TEXT,
 c_valid INTEGER NOT NULL DEFAULT 0, c_wrong INTEGER NOT NULL DEFAULT 0,
 c_2fa INTEGER NOT NULL DEFAULT 0, c_locked INTEGER NOT NULL DEFAULT 0,
 c_timeout INTEGER NOT NULL DEFAULT 0, c_invalid INTEGER NOT NULL DEFAULT 0,
 c_not_found INTEGER NOT NULL DEFAULT 0,
 c_unknown INTEGER NOT NULL DEFAULT 0, c_web_recheck INTEGER NOT NULL DEFAULT 0,
 reported_rows INTEGER NOT NULL DEFAULT 0,
 traffic_bytes INTEGER NOT NULL DEFAULT 0,
 req_timeout REAL NOT NULL DEFAULT 20, req_retries INTEGER NOT NULL DEFAULT 0,
 device_capacity INTEGER NOT NULL DEFAULT 30,
 dual_recheck INTEGER NOT NULL DEFAULT 0,
 auth_protocol TEXT NOT NULL DEFAULT 'gsa',
 device_mode TEXT NOT NULL DEFAULT 'pool'
);
CREATE TABLE IF NOT EXISTS nodes (
 id TEXT PRIMARY KEY, name TEXT NOT NULL, token_hash TEXT UNIQUE NOT NULL,
 created REAL NOT NULL, last_seen REAL, session TEXT, hostname TEXT,
 enabled INTEGER NOT NULL DEFAULT 1, desired_processes INTEGER NOT NULL DEFAULT 4,
 desired_threads INTEGER NOT NULL DEFAULT 250, max_processes INTEGER,
 max_threads INTEGER, version TEXT, last_total INTEGER NOT NULL DEFAULT 0,
 rate REAL NOT NULL DEFAULT 0, last_error TEXT,
 update_generation INTEGER NOT NULL DEFAULT 0,
 ack_update_generation INTEGER NOT NULL DEFAULT 0,
 update_ref TEXT NOT NULL DEFAULT '', update_state TEXT NOT NULL DEFAULT '',
 update_message TEXT NOT NULL DEFAULT '', update_resume_enabled INTEGER NOT NULL DEFAULT 0,
 update_started REAL, update_finished REAL
);
CREATE TABLE IF NOT EXISTS units (
 id TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES jobs(id), seq INTEGER NOT NULL,
 first_row INTEGER NOT NULL, rows INTEGER NOT NULL, path TEXT NOT NULL,
 byte_start INTEGER NOT NULL DEFAULT 0, byte_end INTEGER NOT NULL DEFAULT 0,
 checksum TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', node_id TEXT,
 lease_token TEXT, lease_until REAL, attempt INTEGER NOT NULL DEFAULT 0,
 processed INTEGER NOT NULL DEFAULT 0, result_path TEXT, result_checksum TEXT,
 result_counts TEXT,
 classification_version INTEGER NOT NULL DEFAULT 0,
 live_counts TEXT,
 reported_rows INTEGER NOT NULL DEFAULT 0,
 updated REAL NOT NULL, UNIQUE(job_id,seq)
);
CREATE INDEX IF NOT EXISTS units_queue ON units(status,job_id,seq);
CREATE INDEX IF NOT EXISTS units_node ON units(node_id,status);
CREATE INDEX IF NOT EXISTS units_rows ON units(job_id,status,first_row);
CREATE TABLE IF NOT EXISTS live_result_rows (
 unit_id TEXT NOT NULL, job_id TEXT NOT NULL, row INTEGER NOT NULL,
 category TEXT NOT NULL, payload TEXT NOT NULL,
 PRIMARY KEY(unit_id,row)
);
CREATE INDEX IF NOT EXISTS live_result_job ON live_result_rows(job_id,unit_id,row);
CREATE INDEX IF NOT EXISTS live_result_category ON live_result_rows(job_id,category,row);
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
INSERT OR IGNORE INTO controller_settings(key,value) VALUES('device_auto_rotate','0');
INSERT OR IGNORE INTO controller_settings(key,value) VALUES('proxy_ttl','50');
INSERT OR IGNORE INTO controller_settings(key,value) VALUES('proxy_pool_size','1000');
INSERT OR IGNORE INTO controller_settings(key,value) VALUES('proxy_debug','0');
INSERT OR IGNORE INTO controller_settings(key,value) VALUES('web_secondary_auth','1');
INSERT OR IGNORE INTO controller_settings(key,value) VALUES('auth_protocol','gsa');
INSERT OR IGNORE INTO controller_settings(key,value) VALUES('device_mode','one_time');
INSERT OR IGNORE INTO controller_settings(key,value) VALUES('update_parallel','2');
'''


class Conflict(ValueError):
    pass


def mask_proxy(value):
    """只保留出口 host:port，去掉查询串和认证信息。"""
    if not value:
        return ''
    text = str(value)
    if '@' in text:
        text = text.split('@', 1)[1]
    for prefix in ('socks5h://', 'socks5://', 'http://', 'https://'):
        if text.startswith(prefix):
            text = text[len(prefix):]
    return text.split('?', 1)[0][:64]


def mask_api(value):
    """回显时隐去 password/token，避免代理凭证出现在页面和接口响应里。"""
    if not value:
        return ''
    import re as _re
    return _re.sub(r'(?i)(password|passwd|pwd|token|key)=[^&\s]+', r'\1=***', str(value))


_APPLE_REASON_BUCKETS = (
    ('unable to sign you in to your apple account. try again later', 'timeout'),
    ('this request could not be completed because of a server error', 'timeout'),
    ('this action cannot be completed', 'timeout'),
    ('this action could not be completed. try again', 'timeout'),
    ('the request cannot be processed at this time. please try again later', 'timeout'),
    ('we cannot process your request, please try again later', 'timeout'),
    ('this apple account has been locked for security reasons', 'locked'),
    ('this apple account has been locked', 'locked'),
    ('enter the correct password for this apple account', 'wrong_password'),
    ('this apple account is not active', 'locked'),
    ('your account information was entered incorrectly', 'wrong_password'),
    ('the account does not exist', 'not_found'),
)


def normalized_apple_reason(*values):
    """Collapse Unicode whitespace for matching while preserving stored raw text."""
    text = ' '.join(str(value).strip() for value in values if value)
    return ' '.join(unicodedata.normalize('NFKC', text).split()).casefold()


def apple_reason_matches(text, phrase, threshold=0.90):
    if phrase in text:
        return True
    candidate = text[:len(phrase)]
    if not candidate:
        return False
    # SequenceMatcher's best possible ratio is bounded by the two lengths.
    # Reject impossible >=90% matches before constructing its edit graph; this
    # keeps short unknown reasons from dominating a large category scan.
    shorter, combined = min(len(phrase), len(candidate)), len(phrase) + len(candidate)
    if 2 * shorter < threshold * combined:
        return False
    return SequenceMatcher(None, phrase, candidate, autojunk=False).ratio() >= threshold


def apple_reason_bucket(value):
    """Return a business bucket only for explicit English Apple reasons."""
    fields = (value.get('reason'), value.get('apple_message'), value.get('detail'))
    texts = [normalized_apple_reason(field) for field in fields if field]
    # Almost every production response contains the canonical English phrase.
    # Check all cheap substring matches before invoking SequenceMatcher; doing a
    # fuzzy comparison for every earlier, unrelated phrase made large result
    # pages unnecessarily CPU-heavy on the controller.
    for phrase, bucket in _APPLE_REASON_BUCKETS:
        if any(phrase in text for text in texts):
            return bucket
    for phrase, bucket in _APPLE_REASON_BUCKETS:
        if any(apple_reason_matches(text, phrase) for text in texts):
            return bucket
    return None


def result_record_bucket(value):
    """Classify account results using explicit reasons or Web response triples."""
    try:
        explicit_http_zero = ('http' in value and value.get('http') is not None
                              and int(value['http']) == 0)
    except (TypeError, ValueError):
        explicit_http_zero = False
    if explicit_http_zero:
        return 'timeout'
    reason_bucket = apple_reason_bucket(value)
    if reason_bucket:
        return reason_bucket
    # Web/IDMSA 与 GSA 的错误码语义并不相同。仅接受线上同批账号已核对过的
    # 完成阶段三元组，用于修复旧 Web 结果；不能把这些码泛化到 GSA 或 init。
    protocol = str(value.get('protocol') or '').strip().casefold()
    stage = str(value.get('stage') or '').strip().casefold()
    try:
        web_pair = (int(value.get('http')), int(value.get('code')))
    except (TypeError, ValueError):
        web_pair = None
    if protocol == 'web' and stage == 'complete':
        web_bucket = {(401, -20101): 'wrong_password',
                      (403, -20209): 'locked',
                      (403, -20751): 'locked'}.get(web_pair)
        if web_bucket:
            return web_bucket
    codes = []
    for key in ('code', 'apple_ec', 'gsa_code'):
        try:
            codes.append(int(value.get(key)))
        except (TypeError, ValueError):
            continue
    if any(code in (-22421, -22410, -332000, -33200, -3000303)
           for code in codes):
        return 'timeout'
    bucket = value.get('status') or 'unknown'
    # The node has already completed the protocol-specific classification.
    # Preserve every public result bucket here so live-row indexing uses the
    # same category as the counters reported by that node.
    if bucket in ('valid', 'wrong_password', 'need_2fa', 'locked',
                  'not_found', 'timeout', 'undetermined', 'unknown'):
        return bucket
    return 'unknown'


def result_reason_cn(value, bucket=None):
    """Translate the node's shortcut category/raw Apple reason for display."""
    raw = str(value.get('reason') or value.get('apple_message') or '').strip()
    protocol = str(value.get('protocol') or '').strip().casefold()
    if protocol == 'web' and raw:
        # Web 原文由节点从 serviceErrors[].message 单独提取；不保存响应体。
        return 'Apple 原文：' + raw[:2048]
    low = normalized_apple_reason(raw)
    translations = (
        ('unable to sign you in to your apple account. try again later',
         '暂时无法登录，已按任务设置重试'),
        ('this request could not be completed because of a server error',
         'Apple 服务器错误，已按任务设置重试'),
        ('this action cannot be completed',
         'Apple 操作暂时无法完成，已按任务设置重试'),
        ('this action could not be completed. try again',
         'Apple 操作暂时无法完成，已按任务设置重试'),
        ('the request cannot be processed at this time. please try again later',
         'Apple 当前无法处理请求，已按任务设置重试'),
        ('we cannot process your request, please try again later',
         'Apple 当前无法处理请求，已按任务设置重试'),
        ('this apple account has been locked for security reasons',
         '账号因安全原因被锁定'),
        ('this apple account has been locked', '账号已被锁定'),
        ('enter the correct password for this apple account', '密码错误'),
        ('this apple account is not active', '账号已被锁定'),
        ('your account information was entered incorrectly', '密码错误'),
        ('the account does not exist', '账号不存在'),
    )
    for phrase, translated in translations:
        if phrase in low:
            return translated
    for phrase, translated in translations:
        if apple_reason_matches(low, phrase):
            return translated
    stage = str(value.get('stage') or '').strip().casefold()
    try:
        web_pair = (int(value.get('http')), int(value.get('code')))
    except (TypeError, ValueError):
        web_pair = None
    if protocol == 'web' and stage == 'complete':
        web_reason = {(401, -20101): '密码错误',
                      (403, -20209): '账号因安全原因被锁定',
                      (403, -20751): '账号未激活或已停用'}.get(web_pair)
        if web_reason:
            return web_reason
    try:
        web_http = int(value.get('http'))
    except (TypeError, ValueError):
        web_http = None
    if protocol == 'web' and stage == 'init':
        if web_pair == (400, -900007):
            return ('该记录未保存 Apple 文字说明；Web 初始化失败，尚未校验密码'
                    '（原始码 -900007）')
        if web_http == 403:
            return ('该记录未保存 Apple 协议错误码或文字说明；Web 初始化被 HTTP 403 拒绝，'
                    '尚未校验密码')
    web_status = str(value.get('web_status') or '').strip().casefold()
    web_detail = str(value.get('web_detail') or '').strip()[:256]
    if web_status:
        if web_status in ('valid', 'needs_setup', 'need_2fa'):
            return 'Web 二次校验后：' + (web_detail or {
                'valid': '密码正确',
                'needs_setup': '需要确认隐私或安全设置',
                'need_2fa': '需要手机或 App 验证码',
            }[web_status])
        if web_status == 'error':
            return ('Web 二次校验失败：' + web_detail + '；保留 GSA 结果'
                    if web_detail else 'Web 二次校验失败；保留 GSA 结果')
    resolved = bucket or result_record_bucket(value)
    if resolved == 'need_2fa':
        method = str(value.get('two_factor_method') or '').strip()
        if method == 'verification_code':
            return '密码已通过，需要手机或 App 验证码'
        if method == 'security_repair':
            return '密码已通过，需要密保或安全修复'
        if method == 'repair_unspecified':
            return '密码已通过，需要修复流程（未细分）'
        if low.startswith('au='):
            return f'密码已通过，需要双重验证（{raw}）'
        return '密码已通过，需要双重验证'
    if resolved == 'valid':
        return '密码正确'
    if resolved == 'wrong_password':
        return '密码错误'
    if resolved == 'locked':
        return '账号已锁定'
    if resolved == 'not_found':
        return '账号不存在'
    if resolved == 'timeout':
        try:
            if int(value.get('code')) in (-22410, -332000, -33200, -3000303):
                return 'Apple 当前无法处理请求，已按任务设置重试'
        except (TypeError, ValueError):
            pass
        return '请求超时或服务暂时不可用'
    # 未绑定到内置分类的 Apple 原因不翻译、不覆盖，直接展示服务器
    # 返回的英文。旧记录若从未保存 reason，则只能保留“其他错误”。
    return raw[:2048] if raw else '其他错误'


class Controller:
    MAX_PROCESSES = 4
    MAX_THREADS = 500
    MAX_ACTIVE_UNITS = 512
    RESULT_RECORD_BYTE_LIMIT = 64 * 1024
    RESULT_BUCKETS = ('valid', 'wrong_password', 'need_2fa', 'locked', 'not_found',
                      'timeout', 'undetermined', 'unknown')
    RESULT_CLASSIFICATION_VERSION = 15
    DEFAULT_DEVICE_REQUEST = 30
    UPDATE_ACTIVE_STATES = ('dispatched', 'scheduled', 'fetching', 'installing', 'restarting', 'verifying')
    UPDATE_FINAL_STATES = ('success', 'failed', 'rolled_back')
    UPDATE_REF_PATTERN = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._/-]{0,119}$')

    def __init__(self, root, lease_seconds=120, offline_seconds=12, dispatch_per_second=0):
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
            for name in ('c_valid', 'c_wrong', 'c_2fa', 'c_locked', 'c_timeout', 'c_invalid', 'c_not_found',
                         'c_unknown', 'c_web_recheck', 'reported_rows', 'traffic_bytes',
                         'req_timeout', 'req_retries', 'device_capacity', 'dual_recheck'):
                if name not in job_columns:
                    default = '20' if name == 'req_timeout' else ('30' if name == 'device_capacity' else '0')
                    column_type = 'REAL' if name == 'req_timeout' else 'INTEGER'
                    db.execute(f'ALTER TABLE jobs ADD COLUMN {name} {column_type} NOT NULL DEFAULT {default}')
            if 'auth_protocol' not in job_columns:
                db.execute("ALTER TABLE jobs ADD COLUMN auth_protocol TEXT NOT NULL DEFAULT 'gsa'")
            if 'device_mode' not in job_columns:
                db.execute("ALTER TABLE jobs ADD COLUMN device_mode TEXT NOT NULL DEFAULT 'pool'")
            for name, definition in {'telemetry_json': "TEXT NOT NULL DEFAULT '{}'",
                                     'device_request': 'INTEGER NOT NULL DEFAULT 0',
                                     'stop_version': 'INTEGER NOT NULL DEFAULT 0',
                                     'ack_stop_version': 'INTEGER NOT NULL DEFAULT 0',
                                     'device_clear_version': 'INTEGER NOT NULL DEFAULT 0',
                                     'ack_device_clear_version': 'INTEGER NOT NULL DEFAULT 0',
                                     'update_generation': 'INTEGER NOT NULL DEFAULT 0',
                                     'ack_update_generation': 'INTEGER NOT NULL DEFAULT 0',
                                     'update_ref': "TEXT NOT NULL DEFAULT ''",
                                     'update_state': "TEXT NOT NULL DEFAULT ''",
                                     'update_message': "TEXT NOT NULL DEFAULT ''",
                                     'update_resume_enabled': 'INTEGER NOT NULL DEFAULT 0',
                                     'update_started': 'REAL',
                                     'update_finished': 'REAL'}.items():
                if name not in columns:
                    db.execute(f'ALTER TABLE nodes ADD COLUMN {name} {definition}')
            db.execute("INSERT OR IGNORE INTO controller_settings(key,value) VALUES('update_parallel','2')")
            db.execute("INSERT OR IGNORE INTO controller_settings(key,value) VALUES('device_mode','one_time')")
            unit_columns = {row['name'] for row in db.execute('PRAGMA table_info(units)')}
            if 'result_counts' not in unit_columns:
                db.execute('ALTER TABLE units ADD COLUMN result_counts TEXT')
            if 'byte_start' not in unit_columns:
                db.execute('ALTER TABLE units ADD COLUMN byte_start INTEGER NOT NULL DEFAULT 0')
            if 'byte_end' not in unit_columns:
                db.execute('ALTER TABLE units ADD COLUMN byte_end INTEGER NOT NULL DEFAULT 0')
            if 'live_counts' not in unit_columns:
                db.execute('ALTER TABLE units ADD COLUMN live_counts TEXT')
            if 'classification_version' not in unit_columns:
                db.execute('ALTER TABLE units ADD COLUMN classification_version INTEGER NOT NULL DEFAULT 0')
            if 'reported_rows' not in unit_columns:
                db.execute('ALTER TABLE units ADD COLUMN reported_rows INTEGER NOT NULL DEFAULT 0')
        self.lease_seconds, self.offline_seconds = lease_seconds, offline_seconds
        # 全局派发闸门：整个集群每秒最多放多少个新工作包。0 表示不限。
        self.dispatch_per_second = max(0, int(dispatch_per_second or 0))
        self.rate_lock = threading.Lock()
        self.dispatch_times = deque()
        # Stable result pages are reused until the job update timestamp changes.
        # Serializing misses also prevents identical browser requests from
        # scanning the same result shards concurrently.
        self.result_query_lock = threading.Lock()
        self.result_page_cache = OrderedDict()
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
        # 旧结果中的 HTTP=0 曾被算进“其他未验证”。后台从最新结果开始修复，
        # 不阻塞服务启动；每个工作包只扫描一次，适合已有大任务的数据目录。
        self.classifier = threading.Thread(target=self._reclassify_legacy_results,
                                           name='result-reclassification', daemon=True)
        self.classifier.start()

    def close(self):
        self.closing.set()
        self.preparer.shutdown(wait=True, cancel_futures=True)
        if getattr(self, 'classifier', None) and self.classifier is not threading.current_thread():
            self.classifier.join()

    def _classify_result_file(self, path):
        counts = Counter()
        try:
            with Path(path).open('r', encoding='utf-8') as stream:
                for index, line in enumerate(stream, 1):
                    if index % 1000 == 0 and self.closing.is_set():
                        return None
                    try:
                        record = json.loads(line)
                    except (TypeError, ValueError):
                        return None
                    counts[result_record_bucket(record)] += 1
        except OSError:
            return None
        return counts

    def _reclassify_legacy_results(self):
        """One-pass repair for completed result files created before HTTP=0 meant timeout."""
        with self.db.connect() as db:
            units = [dict(row) for row in db.execute(
                "SELECT u.id,u.job_id,u.rows,u.result_path,u.result_counts,j.mode "
                "FROM units u JOIN jobs j ON j.id=u.job_id "
                "WHERE u.status='completed' AND COALESCE(u.classification_version,0)<? "
                "ORDER BY u.updated DESC", (self.RESULT_CLASSIFICATION_VERSION,))]
        for unit in units:
            if self.closing.is_set():
                return
            if unit['mode'] != 'auth':
                with self.db.transaction() as db:
                    db.execute('UPDATE units SET classification_version=? WHERE id=?',
                               (self.RESULT_CLASSIFICATION_VERSION, unit['id']))
                continue
            counts = self._classify_result_file(unit['result_path'])
            if counts is None or sum(counts.values()) != unit['rows']:
                continue
            try:
                previous = json.loads(unit['result_counts'] or '{}')
            except (TypeError, ValueError):
                previous = {}
            columns = {'valid': 'c_valid', 'wrong_password': 'c_wrong',
                       'need_2fa': 'c_2fa', 'locked': 'c_locked',
                       'timeout': 'c_timeout', 'invalid': 'c_invalid',
                       'not_found': 'c_not_found'}
            deltas = {key: int(counts.get(key, 0)) - int(previous.get(key, 0) or 0)
                      for key in columns}
            old_unknown = int(previous.get('undetermined', 0) or 0) + int(previous.get('unknown', 0) or 0)
            new_unknown = int(counts.get('undetermined', 0)) + int(counts.get('unknown', 0))
            with self.db.transaction() as db:
                current = db.execute('SELECT status,classification_version FROM units WHERE id=?',
                                     (unit['id'],)).fetchone()
                if not current or current['status'] != 'completed' or current['classification_version'] >= self.RESULT_CLASSIFICATION_VERSION:
                    continue
                db.execute(
                    "UPDATE jobs SET c_valid=MAX(0,c_valid+?),c_wrong=MAX(0,c_wrong+?),"
                    "c_2fa=MAX(0,c_2fa+?),c_locked=MAX(0,c_locked+?),"
                    "c_timeout=MAX(0,c_timeout+?),c_invalid=MAX(0,c_invalid+?),"
                    "c_not_found=MAX(0,c_not_found+?),"
                    "c_unknown=MAX(0,c_unknown+?),updated=? WHERE id=?",
                    (deltas['valid'], deltas['wrong_password'], deltas['need_2fa'],
                     deltas['locked'], deltas['timeout'], deltas['invalid'],
                     deltas['not_found'],
                     new_unknown - old_unknown, time.time(), unit['job_id']))
                db.execute('UPDATE units SET result_counts=?,classification_version=? WHERE id=?',
                           (json.dumps(dict(counts), separators=(',', ':')),
                            self.RESULT_CLASSIFICATION_VERSION, unit['id']))

    def add_node(self, name, processes=4, threads=250):
        name = str(name).strip()[:80]
        if not name:
            raise ValueError('请填写节点名称')
        integer(processes, '进程数', 1, self.MAX_PROCESSES)
        integer(threads, '线程数', 1, self.MAX_THREADS)
        node_id, token = uuid.uuid4().hex, secrets.token_urlsafe(32)
        with self.db.transaction() as db:
            db.execute('INSERT INTO nodes(id,name,token_hash,created,enabled,desired_processes,desired_threads,device_request) '
                       'VALUES(?,?,?,?,?,?,?,?)',
                       (node_id, name, hashlib.sha256(token.encode()).hexdigest(), time.time(),
                        0, processes, threads, self.DEFAULT_DEVICE_REQUEST))
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
        if 'enabled' in data and type(data.get('enabled')) is not bool:
            raise ValueError('节点接单状态无效')
        with self.db.transaction() as db:
            node = db.execute('SELECT * FROM nodes WHERE id=?', (node_id,)).fetchone()
            if not node:
                raise ValueError('节点不存在')
            initialization = self._device_initialization(node)
            enabled = bool(node['enabled']) if 'enabled' not in data else data['enabled']
            if enabled and not node['enabled'] and not initialization['complete']:
                raise Conflict(f'设备初始化完成后才能开始接单（'
                               f'{initialization["ready"]}/{initialization["requested"]}）')
            active = db.execute("SELECT COUNT(*) FROM units WHERE node_id=? AND status IN ('leased','running')", (node_id,)).fetchone()[0]
            if enabled and (node['stop_version'] > node['ack_stop_version'] or
                                    not node['enabled'] and node['stop_version'] > 0 and active):
                raise Conflict('节点尚未确认停止，暂不能恢复接单')
            db.execute('UPDATE nodes SET desired_processes=?,desired_threads=?,enabled=? WHERE id=?',
                       (processes, threads, int(enabled), node_id))
            event(db, 'node_config', '节点执行配置已更新，下一次心跳下发；在途工作包保持原配置', node_id=node_id)

    def request_updates(self, node_ids, target_ref, parallel=2):
        """将节点放入滚动更新队列：先停止接新包，在途包完成后再下发更新指令。"""
        target_ref = str(target_ref or '').strip()
        if not self.UPDATE_REF_PATTERN.fullmatch(target_ref) or '..' in target_ref:
            raise ValueError('请填写合法的 Git commit、tag 或分支')
        parallel = integer(int(parallel or 2), '并行更新节点数', 1, 5)
        if node_ids is None:
            node_ids = []
        if not isinstance(node_ids, list) or len(node_ids) > 200:
            raise ValueError('节点列表无效')
        node_ids = list(dict.fromkeys(str(value) for value in node_ids if value))
        now = time.time()
        with self.db.transaction() as db:
            rows = list(db.execute(
                'SELECT * FROM nodes' +
                (f" WHERE id IN ({','.join('?' for _ in node_ids)})" if node_ids else '') +
                ' ORDER BY created', node_ids))
            if not rows:
                raise ValueError('没有可更新的节点')
            if node_ids and len(rows) != len(node_ids):
                raise ValueError('部分节点不存在')
            offline = [row['name'] for row in rows
                       if not row['last_seen'] or now - row['last_seen'] >= self.offline_seconds]
            if offline:
                raise Conflict('节点离线，暂不能更新：' + '、'.join(offline[:5]))
            db.execute("INSERT INTO controller_settings(key,value) VALUES('update_parallel',?) "
                       "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(parallel),))
            for row in rows:
                resume = int(bool(row['enabled']) or bool(row['update_resume_enabled']))
                db.execute(
                    "UPDATE nodes SET enabled=0,update_generation=update_generation+1,"
                    "update_ref=?,update_state='queued',update_message=?,update_resume_enabled=?,"
                    "update_started=NULL,update_finished=NULL WHERE id=?",
                    (target_ref, '已暂停接新包，等待在途工作完成', resume, row['id']))
                event(db, 'node_update_queued',
                      f'已加入更新队列：{target_ref}', node_id=row['id'])
            self._advance_updates(db, now)
        return {'queued': len(rows), 'ref': target_ref, 'parallel': parallel}

    def _advance_updates(self, db, now=None):
        """按并发上限将已排空在途工作的节点从 queued 推进为 dispatched。"""
        now = now or time.time()
        saved = db.execute("SELECT value FROM controller_settings WHERE key='update_parallel'").fetchone()
        parallel = max(1, min(5, int(saved[0] if saved else 2)))
        placeholders = ','.join('?' for _ in self.UPDATE_ACTIVE_STATES)
        active = db.execute(f"SELECT COUNT(*) FROM nodes WHERE update_state IN ({placeholders})",
                            self.UPDATE_ACTIVE_STATES).fetchone()[0]
        for row in db.execute("SELECT * FROM nodes WHERE update_state='queued' ORDER BY created").fetchall():
            if active >= parallel:
                break
            if not row['last_seen'] or now - row['last_seen'] >= self.offline_seconds:
                continue
            working = db.execute("SELECT COUNT(*) FROM units WHERE node_id=? "
                                 "AND status IN ('leased','running','unknown')", (row['id'],)).fetchone()[0]
            if working:
                db.execute("UPDATE nodes SET update_message=? WHERE id=?",
                           (f'等待 {working} 个在途工作包完成', row['id']))
                continue
            db.execute("UPDATE nodes SET update_state='dispatched',update_message=?,update_started=? WHERE id=?",
                       ('已下发更新指令', now, row['id']))
            event(db, 'node_update_dispatched',
                  f'已下发更新：{row["update_ref"]}', node_id=row['id'])
            active += 1

    @staticmethod
    def _device_initialization(node):
        requested = max(0, int(node['device_request'] or 0))
        try:
            telemetry = json.loads(node['telemetry_json'] or '{}')
        except (TypeError, ValueError):
            telemetry = {}
        pool = telemetry.get('device_pool') if isinstance(telemetry, dict) else {}
        pool = pool if isinstance(pool, dict) else {}
        capacity = max(0, int(pool.get('capacity') or 0))
        ready = max(0, int(pool.get('ready') or 0))
        running = bool(pool.get('running'))
        return {'requested': requested, 'capacity': capacity, 'ready': ready,
                'running': running,
                # Existing nodes created before mandatory pre-registration keep
                # device_request=0.  Zero means "no minimum", not "forever
                # incomplete": thread mode must be able to start with an empty
                # pool and register missing identities on demand.
                'complete': bool(requested == 0 or (capacity >= requested
                                 and ready >= requested and not running))}

    def _source_path(self, name):
        """输入文件沿用用户给的名字；重名时加 -2、-3，避免覆盖已上传的文件。"""
        stem = Path(str(name or 'upload')).stem.strip() or 'upload'
        stem = re.sub(r'[\\/:*?"<>|\x00-\x1f]+', '_', stem)[:60] or 'upload'
        suffix = Path(str(name or '')).suffix or '.txt'
        if suffix.lower() != '.txt':
            suffix = '.txt'
        candidate = self.inputs / f'{stem}{suffix}'
        serial = 2
        while candidate.exists():
            candidate = self.inputs / f'{stem}-{serial}{suffix}'
            serial += 1
        return candidate

    def _check_exec(self, timeout, retries):
        value = float(timeout)
        if not 5 <= value <= 120:
            raise ValueError('单请求超时应为 5–120 秒')
        count = int(retries)
        if not 0 <= count <= 5:
            raise ValueError('重试次数应为 0–5 次')
        return value, count

    def create_job(self, name, stream, unit_rows=500, mode='auth', enqueue=False,
                   timeout=20.0, retries=0, device_capacity=30, dual_recheck=False):
        settings = self.settings()
        integer(unit_rows, '每包记录数', 50, settings['max_unit_rows'])
        if type(enqueue) is not bool:
            raise ValueError('加入队列选项无效')
        if mode not in ('digest', 'simulate', 'auth'):
            raise ValueError('执行模式应为 digest / simulate / auth')
        timeout, retries = self._check_exec(timeout, retries)
        # 兼容旧调用参数，但任务不再决定节点设备数量。
        _ = device_capacity
        device_capacity = 0
        # 兼容旧调用签名；主协议由系统设置在任务创建时固化。
        _ = dual_recheck
        auth_protocol = ((settings.get('auth_protocol') or 'gsa')
                         if mode == 'auth' else 'gsa')
        device_mode = ((settings.get('device_mode') or 'one_time')
                       if mode == 'auth' and auth_protocol == 'gsa' else 'pool')
        job_id = uuid.uuid4().hex
        target = self._source_path(name).resolve()
        with target.open('xb') as output:
            target.chmod(0o600)
            shutil.copyfileobj(stream, output, 1024 * 1024)
        if not target.stat().st_size:
            target.unlink()
            raise ValueError('文件为空')
        now = time.time()
        with self.db.transaction() as db:
            db.execute('INSERT INTO jobs(id,name,status,created,updated,source,bytes,unit_rows,mode,queued_at,req_timeout,req_retries,device_capacity,dual_recheck,auth_protocol,device_mode) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                       (job_id, str(name).strip()[:120] or '未命名任务', 'preparing', now, now,
                        str(target), target.stat().st_size, unit_rows, mode, now if enqueue else None,
                        timeout, retries, device_capacity, 0, auth_protocol, device_mode))
            event(db, 'job_uploaded', '输入文件已保存，正在后台准备工作包', job_id=job_id)
        self.preparer.submit(self.prepare, job_id)
        return job_id

    def settings(self):
        with self.db.connect() as db:
            values = {row['key']: row['value'] for row in db.execute('SELECT key,value FROM controller_settings')}
        maximum = int(values.get('max_unit_rows', 100000))
        proxy_parts = [part for part in str(values.get('proxy_api', '')).split() if part]
        return {'max_unit_rows': maximum, 'default_unit_rows': min(10000, maximum),
                'unit_byte_limit': 32 * 1024 * 1024,
                'proxy_api': mask_api(values.get('proxy_api', '')),
                'proxy_apis': [mask_api(part) for part in proxy_parts],
                'proxy_count': len(proxy_parts),
                'proxy_configured': bool(proxy_parts),
                'proxy_scheme': values.get('proxy_scheme', 'socks5h'),
                'device_rotate_after': int(values.get('device_rotate_after', 0) or 0),
                # column 声明是 INTEGER，存 '1' 会读回整数 1，比较时统一转字符串
                'device_auto_rotate': str(values.get('device_auto_rotate', '0')) in ('1', 'True', 'true'),
                'proxy_ttl': int(values.get('proxy_ttl', 50) or 50),
                'proxy_pool_size': int(values.get('proxy_pool_size', 1000) or 1000),
                'proxy_debug': str(values.get('proxy_debug', '0')) in ('1', 'True', 'true'),
                'web_secondary_auth': str(values.get('web_secondary_auth', '1')) in
                                      ('1', 'True', 'true'),
                'auth_protocol': (str(values.get('auth_protocol', 'gsa')).lower()
                                  if str(values.get('auth_protocol', 'gsa')).lower()
                                  in ('gsa', 'web') else 'gsa'),
                'device_mode': (str(values.get('device_mode', 'one_time')).lower()
                                if str(values.get('device_mode', 'one_time')).lower()
                                in ('pool', 'one_time') else 'one_time')}

    def proxy_for_node(self, node):
        """节点专属出口：按创建顺序把链接分给各节点，避免所有节点抢同一条配额。"""
        config = self.proxy_config()
        parts = [part for part in (config.get('api') or '').split() if part]
        if not parts:
            return config
        with self.db.connect() as db:
            index = db.execute(
                'SELECT COUNT(*) FROM nodes WHERE created < ? OR (created = ? AND id < ?)',
                (node['created'], node['created'], node['id'])).fetchone()[0]
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

    def configure_debug(self, data):
        enabled = data.get('proxy_debug')
        if type(enabled) is not bool:
            raise ValueError('代理调试开关应为布尔值')
        value = '1' if enabled else '0'
        with self.db.transaction() as db:
            db.execute("INSERT INTO controller_settings(key,value) VALUES(?,?) "
                       "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                       ('proxy_debug', value))
            event(db, 'controller_config',
                  '代理调试日志已开启；新工作包将记录超时出口明细' if enabled else
                  '代理调试日志已关闭；新工作包不再写入调试日志')
        return self.settings()

    def configure_web_secondary_auth(self, data):
        enabled = data.get('web_secondary_auth')
        if type(enabled) is not bool:
            raise ValueError('Web 二次验证开关应为布尔值')
        value = '1' if enabled else '0'
        with self.db.transaction() as db:
            db.execute("INSERT INTO controller_settings(key,value) VALUES(?,?) "
                       "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                       ('web_secondary_auth', value))
            event(db, 'controller_config',
                  'Web 二次验证已开启；仅对 GSA au=repair 的新工作包执行 Web 校验'
                  if enabled else
                  'Web 二次验证已关闭；新工作包只使用 GSA 结果')
        return self.settings()

    def configure_auth_protocol(self, data):
        protocol = str(data.get('auth_protocol') or '').strip().lower()
        if protocol not in ('gsa', 'web'):
            raise ValueError('认证协议应为 GSA 或 Web')
        with self.db.transaction() as db:
            db.execute("INSERT INTO controller_settings(key,value) VALUES(?,?) "
                       "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                       ('auth_protocol', protocol))
            event(db, 'controller_config',
                  f'账号校验协议已切换为 {protocol.upper()}；仅对新建任务生效')
        return self.settings()

    def configure_device_mode(self, data):
        mode = str(data.get('device_mode') or '').strip().lower()
        if mode not in ('pool', 'one_time'):
            raise ValueError('设备模式应为共享设备或一次性设备')
        with self.db.transaction() as db:
            db.execute("INSERT INTO controller_settings(key,value) VALUES(?,?) "
                       "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                       ('device_mode', mode))
            event(db, 'controller_config',
                  '设备模式已切换为一次性设备；每个账号由当前线程自行注册，失败 10 秒后换 IP 重试'
                  if mode == 'one_time' else
                  '设备模式已切换为共享设备池')
        return self.settings()

    def configure_rotation(self, data):
        with self.db.transaction() as db:
            current = {row['key']: row['value'] for row in db.execute(
                "SELECT key,value FROM controller_settings "
                "WHERE key IN ('device_rotate_after','device_auto_rotate')")}
            # This endpoint accepts partial updates. Saving a threshold must
            # not implicitly disable the separately saved refill switch.
            limit_value = data.get('device_rotate_after', current.get('device_rotate_after', 0))
            limit = integer(int(limit_value or 0), '设备轮换阈值', 0, 100000)
            if 'device_auto_rotate' in data:
                if type(data['device_auto_rotate']) is not bool:
                    raise ValueError('设备自动补充开关应为布尔值')
                auto = '1' if data['device_auto_rotate'] else '0'
            else:
                auto = '1' if str(current.get('device_auto_rotate', '0')) in ('1', 'True', 'true') else '0'
            for key_name, key_value in (('device_rotate_after', str(limit)),
                                        ('device_auto_rotate', auto)):
                db.execute("INSERT INTO controller_settings(key,value) VALUES(?,?) "
                           "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                           (key_name, key_value))
            if auto == '1':
                event(db, 'controller_config',
                      f'设备自动补充已开启：每台用满 {limit or 20} 条后换新并补齐' if limit
                      else '设备自动补充已开启（轮换阈值 0 时只补缺不换新）')
            else:
                event(db, 'controller_config', '设备自动补充已关闭')
        return self.settings()

    def configure_proxy(self, data):
        action = str(data.get('action') or 'replace').strip()
        if action not in ('add', 'delete', 'clear', 'replace', 'resize'):
            raise ValueError('代理配置操作无效')
        with self.db.connect() as db:
            current = {row['key']: row['value'] for row in db.execute(
                "SELECT key,value FROM controller_settings "
                "WHERE key IN ('proxy_api','proxy_scheme','proxy_pool_size')")}
        scheme = str(data.get('proxy_scheme') or current.get('proxy_scheme') or 'socks5h').strip()
        if scheme not in ('socks5h', 'socks5', 'http', 'https'):
            raise ValueError('代理协议应为 socks5h / socks5 / http / https')
        try:
            pool_value = int(data.get('proxy_pool_size') or
                             current.get('proxy_pool_size') or 1000)
        except (TypeError, ValueError):
            raise ValueError('单节点代理池容量应为 1–10000 的整数') from None
        pool_size = integer(pool_value, '单节点代理池容量', 1, 10000)
        existing = [part for part in str(current.get('proxy_api') or '').split() if part]
        submitted = [part for part in str(data.get('proxy_api') or '').split() if part]
        if action == 'add':
            if not submitted:
                raise ValueError('请填写要添加的代理 API 地址')
            parts = list(dict.fromkeys(existing + submitted))
        elif action == 'delete':
            try:
                index = int(data.get('index'))
            except (TypeError, ValueError):
                raise ValueError('代理配置编号无效') from None
            if not 0 <= index < len(existing):
                raise ValueError('代理配置不存在')
            parts = existing[:index] + existing[index + 1:]
        elif action == 'clear':
            parts = []
        elif action == 'resize':
            parts = existing
        else:
            parts = list(dict.fromkeys(submitted))
        if len(parts) > 16:
            raise ValueError('最多支持 16 个代理 API 链接')
        from urllib.parse import urlsplit
        for part in parts:
            parsed = urlsplit(part)
            if parsed.scheme not in ('http', 'https') or not parsed.hostname:
                raise ValueError('代理 API 需要是 http(s) 地址')
        api = '\n'.join(parts)
        with self.db.transaction() as db:
            db.execute("UPDATE controller_settings SET value=? WHERE key='proxy_api'", (api,))
            db.execute("UPDATE controller_settings SET value=? WHERE key='proxy_scheme'", (scheme,))
            db.execute("INSERT INTO controller_settings(key,value) VALUES('proxy_pool_size',?) "
                       "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(pool_size),))
            if action == 'resize':
                event(db, 'controller_config',
                      f'单节点代理池容量已设置为 {pool_size:,}；每次最多提取 1,000 条并在节点本地补齐')
            else:
                event(db, 'controller_config',
                      f'代理 API 配置已更新，当前 {len(parts)} 个，按节点轮询分配'
                      if parts else '代理 API 配置已清空')
        return self.settings()

    def request_devices(self, node_id, capacity):
        """把设备初始化意图挂到节点上，节点下次心跳取走执行。"""
        capacity = integer(int(capacity), '设备数量', 0, 10000)
        with self.db.transaction() as db:
            node = db.execute('SELECT * FROM nodes WHERE id=?', (node_id,)).fetchone()
            if not node:
                raise ValueError('节点不存在')
            if not node['last_seen'] or time.time() - node['last_seen'] > self.offline_seconds:
                raise Conflict('节点离线，无法初始化设备')
            db.execute('UPDATE nodes SET device_request=? WHERE id=?', (capacity, node_id))
            event(db, 'device_build_requested', f'已请求节点初始化 {capacity} 台设备', node_id=node_id)
        return {'requested': capacity}

    def clear_devices(self, node_id):
        """下发持久化清空指令；节点离线后重连也会执行。"""
        with self.db.transaction() as db:
            node = db.execute('SELECT * FROM nodes WHERE id=?', (node_id,)).fetchone()
            if not node:
                raise ValueError('节点不存在')
            active = db.execute("SELECT COUNT(*) FROM units WHERE node_id=? "
                                "AND status IN ('leased','running','unknown')", (node_id,)).fetchone()[0]
            if active:
                raise Conflict(f'节点仍有 {active} 个工作包在执行，请先停止任务')
            version = int(node['device_clear_version'] or 0) + 1
            db.execute('UPDATE nodes SET device_request=0,device_clear_version=? WHERE id=?',
                       (version, node_id))
            event(db, 'device_pool_clear_requested', '已请求节点清空全部设备',
                  node_id=node_id)
        return {'requested': True, 'version': version}

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
        network_bytes = value.get('network_bytes')
        result['network_bytes'] = (network_bytes if type(network_bytes) is int
                                   and 0 <= network_bytes <= 10**18 else None)
        result.update(received_at=time.time(), scope='host_non_loopback')
        processes = value.get('worker_processes')
        result['worker_processes'] = processes if type(processes) is int and 0 <= processes <= 200 else None
        pool = value.get('device_pool')
        if isinstance(pool, dict):
            result['device_pool'] = {key: pool.get(key) for key in
                                     ('running', 'capacity', 'ready', 'built', 'failed',
                                      'current', 'attempted')}
            proxy_status = str(pool.get('proxy_status') or 'idle')
            result['device_pool']['proxy_status'] = (proxy_status if proxy_status in
                                                      ('idle', 'checking', 'ok', 'failed') else 'idle')
            result['device_pool']['proxy_ip'] = mask_proxy(str(pool.get('proxy_ip') or ''))[:96]
            result['device_pool']['message'] = str(pool.get('message') or '')[:160]
            slots = pool.get('slots')
            if isinstance(slots, list):
                result['device_pool']['slots'] = [
                    {'slot': int(item.get('slot') or 0),
                     'state': str(item.get('state') or '')[:20],
                     'label': str(item.get('label') or '')[:20],
                     'error': str(item.get('error') or '')[:120],
                     'model': str(item.get('model') or '')[:32],
                     'proxy': mask_proxy(str(item.get('proxy') or '')),
                     'at': str(item.get('at') or '')[:19],
                     'used': (item.get('used') if type(item.get('used')) is int
                              and 0 <= item.get('used') <= 10**12 else 0),
                     'recorded': bool(item.get('recorded'))}
                    for item in slots[:DEVICE_DETAIL_LIMIT] if isinstance(item, dict)]
        return result

    @classmethod
    def clean_live_counts(cls, value, processed, rows):
        if not isinstance(value, dict):
            return {}
        result = {}
        # Fold legacy nodes' former "invalid" counter into locked while a
        # rolling update is in progress; the public category no longer exists.
        legacy_invalid = value.get('invalid', 0)
        if type(legacy_invalid) is not int or legacy_invalid < 0 or legacy_invalid > rows:
            raise ValueError('实时分类计数无效')
        for key in cls.RESULT_BUCKETS:
            count = value.get(key, 0)
            if type(count) is not int or count < 0 or count > rows:
                raise ValueError('实时分类计数无效')
            if count:
                result[key] = count
        if legacy_invalid:
            result['locked'] = result.get('locked', 0) + legacy_invalid
        if sum(result.get(key, 0) for key in cls.RESULT_BUCKETS) > processed:
            raise ValueError('实时分类数超过已处理数')
        return result

    @classmethod
    def clean_live_records(cls, value, unit):
        """Validate one node-level heartbeat batch without trusting workers.

        Workers only append local JSONL files.  The node agent reads those files
        once per heartbeat and sends at most 1,000 complete lines here.  Keeping
        this validation identical to the final-result path means a live row can
        never bypass the package row range or the bounded result-record byte limit.
        """
        if value is None:
            return []
        if not isinstance(value, list) or len(value) > 1000:
            raise ValueError('实时结果批次无效')
        first = int(unit['first_row'])
        last = first + int(unit['rows'])
        seen, result = set(), []
        for item in value:
            if isinstance(item, str):
                encoded = item
                try:
                    record = json.loads(encoded)
                except ValueError as exc:
                    raise ValueError('实时结果 JSON 无效') from exc
            elif isinstance(item, dict):
                # Backward-compatible with an early development node build.
                record = item
                encoded = json.dumps(record, separators=(',', ':'))
            else:
                raise ValueError('实时结果记录无效')
            if not isinstance(record, dict):
                raise ValueError('实时结果记录无效')
            row = record.get('row')
            size = record.get('bytes')
            digest = record.get('sha256')
            if (type(row) is not int or row < first or row >= last or row in seen
                    or type(size) is not int or size < 0
                    or not isinstance(digest, str)
                    or not re.fullmatch('[0-9a-f]{64}', digest)):
                raise ValueError('实时结果编号或摘要无效')
            if len(encoded.encode('utf-8')) + 1 > cls.RESULT_RECORD_BYTE_LIMIT:
                raise ValueError('实时结果记录过长')
            seen.add(row)
            result.append((row, result_record_bucket(record), encoded))
        return result

    def _accept_live_records(self, db, unit, records):
        """Persist an idempotent heartbeat batch and return the unit ACK count."""
        inserted = 0
        for row, category, payload in self.clean_live_records(records, unit):
            existing = db.execute(
                'SELECT payload FROM live_result_rows WHERE unit_id=? AND row=?',
                (unit['id'], row)).fetchone()
            if existing:
                if existing['payload'] != payload:
                    raise Conflict('同一记录的实时结果不一致')
                continue
            db.execute(
                'INSERT INTO live_result_rows(unit_id,job_id,row,category,payload) VALUES(?,?,?,?,?)',
                (unit['id'], unit['job_id'], row, category, payload))
            inserted += 1
        if inserted:
            db.execute('UPDATE units SET reported_rows=reported_rows+? WHERE id=?',
                       (inserted, unit['id']))
            db.execute('UPDATE jobs SET reported_rows=reported_rows+?,updated=? WHERE id=?',
                       (inserted, time.time(), unit['job_id']))
        current = db.execute('SELECT reported_rows FROM units WHERE id=?',
                             (unit['id'],)).fetchone()
        return int(current['reported_rows'] or 0)

    def _complete_live_unit(self, db, unit):
        """Promote a fully streamed package without waiting for a second upload."""
        records = db.execute(
            'SELECT row,category,payload FROM live_result_rows WHERE unit_id=? ORDER BY row',
            (unit['id'],)).fetchall()
        if len(records) != int(unit['rows']):
            return False
        temporary = self.results / (secrets.token_hex(16) + '.live')
        digest, buckets = hashlib.sha256(), Counter()
        expected = int(unit['first_row'])
        try:
            with temporary.open('xb') as output:
                temporary.chmod(0o600)
                for record in records:
                    if int(record['row']) != expected:
                        return False
                    line = (record['payload'] + '\n').encode('utf-8')
                    output.write(line)
                    digest.update(line)
                    buckets[record['category']] += 1
                    expected += 1
                output.flush()
                os.fsync(output.fileno())
            final = self.results / (unit['id'] + '.' + hashlib.sha256(
                str(unit['lease_token']).encode()).hexdigest()[:12] + '.jsonl')
            os.replace(temporary, final)
            job_mode = db.execute('SELECT mode FROM jobs WHERE id=?',
                                  (unit['job_id'],)).fetchone()['mode']
            self._clear_live_unit(db, unit['id'])
            if job_mode == 'auth':
                db.execute(
                    "UPDATE jobs SET c_valid=c_valid+?,c_wrong=c_wrong+?,c_2fa=c_2fa+?,"
                    "c_locked=c_locked+?,c_timeout=c_timeout+?,c_invalid=c_invalid+?,c_not_found=c_not_found+?,"
                    "c_unknown=c_unknown+?,updated=? WHERE id=?",
                    (buckets['valid'], buckets['wrong_password'], buckets['need_2fa'],
                     buckets['locked'], buckets['timeout'], buckets['invalid'],
                     buckets['not_found'],
                     buckets['undetermined'] + buckets['unknown'], time.time(), unit['job_id']))
            else:
                db.execute('UPDATE jobs SET updated=? WHERE id=?',
                           (time.time(), unit['job_id']))
            db.execute(
                "UPDATE units SET status='completed',processed=rows,result_path=?,result_checksum=?,"
                "result_counts=?,classification_version=?,live_counts=NULL,reported_rows=0,"
                "lease_until=NULL,updated=? WHERE id=?",
                (str(final), digest.hexdigest(),
                 json.dumps(dict(buckets), separators=(',', ':')),
                 self.RESULT_CLASSIFICATION_VERSION, time.time(), unit['id']))
            event(db, 'result_stream_confirmed',
                  f'工作包 #{unit["seq"]} 已通过节点心跳实时确认（{int(unit["rows"]):,} 条）',
                  unit['job_id'], unit['node_id'])
            unfinished = db.execute(
                "SELECT COUNT(*) FROM units WHERE job_id=? AND status!='completed'",
                (unit['job_id'],)).fetchone()[0]
            if not unfinished:
                db.execute("UPDATE jobs SET status='completed',updated=? WHERE id=?",
                           (time.time(), unit['job_id']))
                event(db, 'job_completed', '全部工作包已确认，任务完成', unit['job_id'])
            self._advance_queue(db)
            return True
        finally:
            temporary.unlink(missing_ok=True)

    @staticmethod
    def _clear_live_unit(db, unit_id):
        unit = db.execute('SELECT job_id,reported_rows FROM units WHERE id=?',
                          (unit_id,)).fetchone()
        if not unit:
            return 0
        count = int(unit['reported_rows'] or 0)
        db.execute('DELETE FROM live_result_rows WHERE unit_id=?', (unit_id,))
        db.execute('UPDATE units SET reported_rows=0 WHERE id=?', (unit_id,))
        if count:
            db.execute('UPDATE jobs SET reported_rows=MAX(0,reported_rows-?) WHERE id=?',
                       (count, unit['job_id']))
        return count

    def files(self):
        """列出上传过的源文件，供新建任务时复用，避免重复上传大文件。"""
        with self.db.connect() as db:
            rows = db.execute("""
                SELECT source, MAX(created) AS created, MAX(bytes) AS bytes, MAX(rows) AS rows,
                       COUNT(*) AS jobs, MAX(name) AS name
                FROM jobs WHERE source != '' GROUP BY source ORDER BY created DESC LIMIT 100
            """).fetchall()
        result = []
        for row in rows:
            if not Path(row['source']).exists():
                continue
            result.append({'id': row['source'], 'name': Path(row['source']).name,
                           'bytes': row['bytes'], 'rows': row['rows'],
                           'jobs': row['jobs'], 'created': row['created']})
        return result

    def create_job_from_file(self, source, name, unit_rows=10000, mode='auth', enqueue=False,
                             timeout=20.0, retries=0, device_capacity=30,
                             dual_recheck=False):
        """复用已上传的源文件创建任务，不再接收文件内容。"""
        settings = self.settings()
        integer(unit_rows, '每包记录数', 50, settings['max_unit_rows'])
        if mode not in ('digest', 'simulate', 'auth'):
            raise ValueError('执行模式应为 digest / simulate / auth')
        timeout, retries = self._check_exec(timeout, retries)
        # 兼容旧调用参数，但任务不再决定节点设备数量。
        _ = device_capacity
        device_capacity = 0
        # 兼容旧调用签名；主协议由系统设置在任务创建时固化。
        _ = dual_recheck
        auth_protocol = ((settings.get('auth_protocol') or 'gsa')
                         if mode == 'auth' else 'gsa')
        device_mode = ((settings.get('device_mode') or 'one_time')
                       if mode == 'auth' and auth_protocol == 'gsa' else 'pool')
        path = Path(str(source))
        if not path.exists():
            raise ValueError('源文件不存在或不可复用')
        path = path.resolve()
        if path.parent != self.inputs.resolve():
            raise ValueError('源文件不存在或不可复用')
        job_id = uuid.uuid4().hex
        now = time.time()
        with self.db.transaction() as db:
            db.execute('INSERT INTO jobs(id,name,status,created,updated,source,bytes,unit_rows,mode,queued_at,req_timeout,req_retries,device_capacity,dual_recheck,auth_protocol,device_mode) '
                       'VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                       (job_id, str(name).strip()[:120] or path.name, 'preparing', now, now,
                        str(path), path.stat().st_size, unit_rows, mode, now if enqueue else None,
                        timeout, retries, device_capacity, 0, auth_protocol, device_mode))
            event(db, 'job_uploaded', '复用已有输入文件，正在后台准备工作包', job_id=job_id)
        self.preparer.submit(self.prepare, job_id)
        return job_id

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
            # First respect the configured maximum.  If that creates lopsided
            # work (for example 10,000 + 2), rebuild the byte index once with a
            # balanced row limit derived from the currently accepting nodes.
            built = self._build_units(job, int(job['unit_rows']), byte_limit)
            if built is None:
                return
            rows, unit_count = built
            with self.db.connect() as db:
                accepting = int(db.execute(
                    "SELECT COUNT(*) FROM nodes WHERE enabled=1 AND last_seen>? "
                    "AND update_state NOT IN ('dispatched','scheduled','fetching',"
                    "'installing','restarting','verifying')",
                    (time.time() - self.offline_seconds,)).fetchone()[0])
            target_units = max(1, unit_count, accepting)
            balanced_limit = max(50, math.ceil(rows / target_units))
            if balanced_limit < int(job['unit_rows']):
                with self.db.transaction() as db:
                    db.execute('DELETE FROM units WHERE job_id=?', (job_id,))
                    db.execute('UPDATE jobs SET rows=0 WHERE id=?', (job_id,))
                rebuilt = self._build_units(job, balanced_limit, byte_limit)
                if rebuilt is None:
                    return
                rows, unit_count = rebuilt
            with self.db.transaction() as db:
                db.execute("UPDATE jobs SET status=CASE WHEN queued_at IS NOT NULL THEN 'queued' ELSE 'ready' END,rows=?,updated=? WHERE id=?", (rows, time.time(), job_id))
                event(db, 'job_ready',
                      f'文件准备完成，共 {rows:,} 条记录，均衡为 {unit_count} 个工作包',
                      job_id=job_id)
                self._advance_queue(db)
        except Exception as exc:
            message = str(exc) if isinstance(exc, (ValueError, UnicodeError)) else type(exc).__name__
            with self.db.transaction() as db:
                db.execute("UPDATE jobs SET status='failed',queued_at=NULL,error=?,updated=? WHERE id=?", (message, time.time(), job_id))
                event(db, 'prepare_failed', message, job_id=job_id)
                self._advance_queue(db)

    def _build_units(self, job, row_limit, byte_limit):
        """Index one source file into bounded, nearly equal byte ranges."""
        seq, rows, first_row = 0, 0, 1
        unit_start, unit_rows, unit_bytes = 0, 0, 0
        unit_digest = hashlib.sha256()
        with Path(job['source']).open('rb') as source:
            while True:
                if self.closing.is_set():
                    return None
                line_start = source.tell()
                line = source.readline(65537)
                if not line:
                    break
                if len(line) > 65536:
                    raise ValueError('单行超过 64 KiB；请将超长记录拆分后重试')
                line.decode('utf-8-sig' if rows == 0 else 'utf-8')
                # Every source line remains a record, including blank lines.
                if unit_rows and unit_bytes + len(line) > byte_limit:
                    seq += 1
                    self._save_unit(job['id'], seq, first_row, unit_rows, job['source'],
                                    unit_start, line_start, unit_digest.hexdigest(), rows)
                    first_row, unit_start = rows + 1, line_start
                    unit_rows, unit_bytes = 0, 0
                    unit_digest = hashlib.sha256()
                unit_digest.update(line)
                unit_rows += 1
                unit_bytes += len(line)
                rows += 1
                if unit_rows == row_limit or unit_bytes >= byte_limit:
                    seq += 1
                    self._save_unit(job['id'], seq, first_row, unit_rows, job['source'],
                                    unit_start, source.tell(), unit_digest.hexdigest(), rows)
                    first_row, unit_start = rows + 1, source.tell()
                    unit_rows, unit_bytes = 0, 0
                    unit_digest = hashlib.sha256()
            if unit_rows:
                seq += 1
                self._save_unit(job['id'], seq, first_row, unit_rows, job['source'],
                                unit_start, source.tell(), unit_digest.hexdigest(), rows)
        return rows, seq

    def _advance_queue(self, db):
        # paused 不挡队列：暂停等于让出执行位，下一个任务立刻开始；
        # 恢复时按原来记录的排队时间回到队列，位置不会丢。
        if db.execute("SELECT 1 FROM jobs WHERE status='running' LIMIT 1").fetchone():
            return
        # Older versions could have leased work belonging to another task.
        if db.execute("SELECT 1 FROM units WHERE status IN ('leased','running') LIMIT 1").fetchone():
            return
        head = db.execute("SELECT id,status FROM jobs WHERE status='queued' OR (status='preparing' AND queued_at IS NOT NULL) ORDER BY queued_at,id LIMIT 1").fetchone()
        if head and head['status'] == 'queued':
            db.execute("UPDATE jobs SET status='running',updated=? WHERE id=?", (time.time(), head['id']))
            event(db, 'job_started', '轮到本任务，自动开始派发', job_id=head['id'])

    def _save_unit(self, job_id, seq, first_row, unit_rows, source_path,
                   byte_start, byte_end, checksum, rows):
        with self.db.transaction() as db:
            db.execute('INSERT INTO units(id,job_id,seq,first_row,rows,path,byte_start,byte_end,checksum,updated) '
                       'VALUES(?,?,?,?,?,?,?,?,?,?)',
                       (uuid.uuid4().hex, job_id, seq, first_row, unit_rows,
                        str(source_path), byte_start, byte_end, checksum, time.time()))
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
                db.execute("UPDATE jobs SET status='queued',queued_at=COALESCE(queued_at,?),updated=? WHERE id=?",
                           (time.time(), time.time(), job_id))
                event(db, 'job_queued', '任务恢复，按原排队顺序回到队列', job_id=job_id)
                self._advance_queue(db)
            elif action == 'start' and job['status'] in ('queued', 'running'):
                pass
            elif action == 'dequeue' and (job['status'] == 'queued' or job['status'] == 'preparing' and job['queued_at'] is not None):
                db.execute("UPDATE jobs SET status=CASE WHEN status='queued' THEN 'ready' ELSE status END,queued_at=NULL,updated=? WHERE id=?", (time.time(), job_id))
                event(db, 'job_dequeued', '任务已移出队列，可稍后重新加入', job_id=job_id)
                self._advance_queue(db)
            elif action == 'pause' and job['status'] == 'running':
                db.execute("UPDATE jobs SET status='paused',queued_at=COALESCE(queued_at,?),updated=? WHERE id=?",
                           (job['created'], time.time(), job_id))
                event(db, 'job_paused', '已暂停派发，在途工作包继续完成；下一个排队任务开始', job_id=job_id)
                self._advance_queue(db)
            elif action == 'stop' and job['status'] in ('queued', 'running', 'paused', 'ready'):
                db.execute("UPDATE jobs SET status='stopped',queued_at=NULL,updated=? WHERE id=?",
                           (time.time(), job_id))
                for unit in db.execute(
                        "SELECT id FROM units WHERE job_id=? AND status IN ('leased','running','unknown')",
                        (job_id,)).fetchall():
                    self._clear_live_unit(db, unit['id'])
                db.execute("UPDATE units SET status='pending',node_id=NULL,lease_token=NULL,"
                           "lease_until=NULL,processed=0,live_counts=NULL,reported_rows=0,updated=? WHERE job_id=? "
                           "AND status IN ('leased','running','unknown')", (time.time(), job_id))
                event(db, 'job_stopped', '任务已停止，在途工作包退回待分配', job_id=job_id)
                self._advance_queue(db)
            elif action == 'retry_unknown' and job['status'] in ('running', 'paused', 'ready'):
                unknown = db.execute("SELECT id FROM units WHERE job_id=? AND status='unknown'",
                                     (job_id,)).fetchall()
                for unit in unknown:
                    self._clear_live_unit(db, unit['id'])
                count = db.execute("UPDATE units SET status='pending',node_id=NULL,lease_token=NULL,lease_until=NULL,processed=0,live_counts=NULL,reported_rows=0,updated=? WHERE job_id=? AND status='unknown'",
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
        # 滚动更新期间允许旧节点继续上报 10×100；新的可配置上限仍是 4×500。
        max_p = integer(data.get('max_processes'), '节点进程上限', 1, 200)
        max_t = integer(data.get('max_threads'), '节点线程上限', 1, 512)
        active = data.get('active', [])
        interrupted = data.get('interrupted', [])
        if not isinstance(active, list) or len(active) > self.MAX_ACTIVE_UNITS:
            raise ValueError('节点工作包列表无效')
        if not isinstance(interrupted, list) or len(interrupted) > self.MAX_ACTIVE_UNITS:
            raise ValueError('节点中断工作包列表无效')
        record_total = 0
        for value in active:
            records = value.get('records', []) if isinstance(value, dict) else None
            if not isinstance(records, list) or len(records) > 1000:
                raise ValueError('节点实时结果批次无效')
            record_total += len(records)
        if record_total > 2000:
            raise ValueError('单次心跳实时结果过多')
        with self.db.transaction() as db:
            self._expire(db)
            node = db.execute('SELECT * FROM nodes WHERE id=?', (node_id,)).fetchone()
            was_online = bool(node['last_seen'] and now - node['last_seen'] < self.offline_seconds)
            if (node['session'] and node['session'] != session and node['last_seen']
                    and now - node['last_seen'] < self.offline_seconds):
                raise Conflict('同一节点身份已有另一个实例在线；请等待旧实例离线或使用独立接入凭证')
            desired_processes = node['desired_processes']
            desired_threads = node['desired_threads']
            # 0.3.66 把 4 核节点从 10×100 改为 4×250，保持总并发 1000。
            if desired_processes == 10 and desired_threads == 100 and max_p >= 4 and max_t >= 250:
                desired_processes, desired_threads = 4, 250
                db.execute('UPDATE nodes SET desired_processes=?,desired_threads=? WHERE id=?',
                           (desired_processes, desired_threads, node_id))
                event(db, 'node_config', '已将旧版 10×100 等并发迁移为 4×250', node_id=node_id)
            # 0.3.5 前端把线程数上限错误限制为 32。
            if desired_threads == 32 and max_t >= 100 and str(node['version'] or '') != '0.3.6':
                desired_threads = 250
                desired_processes = min(desired_processes, 4)
                db.execute('UPDATE nodes SET desired_processes=?,desired_threads=? WHERE id=?',
                           (desired_processes, desired_threads, node_id))
                event(db, 'node_config', '已将旧版线程配置升级为 4 核节点配置',
                      node_id=node_id)
            reject, confirmed, interrupted_ack, reported_ack = [], [], [], {}
            traffic_jobs = set()
            for value in interrupted:
                unit = db.execute('SELECT * FROM units WHERE id=? AND node_id=?', (value.get('id'), node_id)).fetchone()
                if (not unit or unit['lease_token'] != value.get('token')
                        or unit['status'] not in ('leased', 'running', 'unknown')):
                    reject.append(value.get('id'))
                    continue
                count = integer(value.get('processed', 0), '中断记录数', 0, unit['rows'])
                self._clear_live_unit(db, unit['id'])
                interrupted_ack.append(unit['id'])
                reason = str(value.get('error') or '')[:120]
                if '无进度' in reason and int(unit['attempt'] or 0) < 3:
                    # Watchdog failures are safe to retry: the previous lease
                    # token is revoked and live rows are cleared before this
                    # exact package is offered again.  Manual stops and node
                    # shutdowns deliberately keep the old "unknown" behavior.
                    db.execute(
                        "UPDATE units SET status='pending',node_id=NULL,processed=0,"
                        "live_counts=NULL,reported_rows=0,lease_token=NULL,lease_until=NULL,updated=? WHERE id=?",
                        (now, unit['id']))
                    event(db, 'unit_auto_requeued',
                          f'工作包 #{unit["seq"]} 无进度，已自动重排（第 {int(unit["attempt"] or 0)} 次）',
                          unit['job_id'], node_id)
                else:
                    db.execute("UPDATE units SET status='unknown',processed=?,live_counts=NULL,lease_token=NULL,lease_until=NULL,updated=? WHERE id=?",
                               (count, now, unit['id']))
                    event(db, 'unit_interrupted',
                          f'节点已中止工作包 #{unit["seq"]}，未完成包进入待确认'
                          + (f'（{reason}）' if reason else ''), unit['job_id'], node_id)
                    if '无进度' in reason:
                        db.execute("UPDATE jobs SET status='paused',error=?,updated=? WHERE id=? AND status='running'",
                                   ('工作包连续 3 次无进度，已自动暂停以免阻塞后续任务', now,
                                    unit['job_id']))
                        self._advance_queue(db)
            for value in active:
                unit = db.execute('SELECT * FROM units WHERE id=? AND node_id=?', (value.get('id'), node_id)).fetchone()
                if unit and unit['lease_token'] == value.get('token') and unit['status'] == 'completed':
                    traffic_jobs.add(unit['job_id'])
                    confirmed.append(unit['id'])
                    continue
                if (not unit or unit['lease_token'] != value.get('token')
                        or unit['status'] not in ('leased', 'running', 'unknown')):
                    reject.append(value.get('id'))
                    continue
                traffic_jobs.add(unit['job_id'])
                count = integer(value.get('processed', 0), '已处理数量', 0, unit['rows'])
                live_counts = self.clean_live_counts(value.get('counts'), count, unit['rows'])
                reported_ack[unit['id']] = self._accept_live_records(
                    db, unit, value.get('records', []))
                if (reported_ack[unit['id']] == int(unit['rows'])
                        and count == int(unit['rows'])
                        and self._complete_live_unit(db, unit)):
                    confirmed.append(unit['id'])
                elif count >= unit['processed']:
                    db.execute("UPDATE units SET status='running',processed=?,live_counts=?,lease_until=?,updated=? WHERE id=?",
                               (count, json.dumps(live_counts, separators=(',', ':')),
                                now + self.lease_seconds, now, unit['id']))
                else:
                    db.execute("UPDATE units SET status='running',lease_until=?,updated=? WHERE id=?",
                               (now + self.lease_seconds, now, unit['id']))
            total = db.execute("SELECT COALESCE(SUM(processed),0) FROM units WHERE node_id=? AND status IN ('leased','running','completed')", (node_id,)).fetchone()[0]
            elapsed = now - (node['last_seen'] or now)
            instant = max(0, total - node['last_total']) / elapsed if elapsed > 0.1 else 0
            rate = instant if not node['last_seen'] or elapsed > self.offline_seconds else 0.55 * node['rate'] + 0.45 * instant
            db.execute('UPDATE nodes SET last_seen=?,session=?,hostname=?,version=?,max_processes=?,max_threads=?,last_total=?,rate=?,last_error=? WHERE id=?',
                       (now, session, str(data.get('hostname', ''))[:120], str(data.get('version', ''))[:30],
                        max_p, max_t, total, rate, str(data.get('error', ''))[:200], node_id))
            ack_stop = data.get('ack_stop_version', 0)
            ack_stop = max(0, min(ack_stop, node['stop_version'])) if type(ack_stop) is int else 0
            ack_device_clear = data.get('ack_device_clear_version', 0)
            ack_device_clear = (max(0, min(ack_device_clear, node['device_clear_version']))
                                if type(ack_device_clear) is int else 0)
            ack_update = data.get('ack_update_generation', 0)
            ack_update = (max(0, min(ack_update, node['update_generation']))
                          if type(ack_update) is int else 0)
            update_status = data.get('update_status')
            if not isinstance(update_status, dict):
                update_status = {}
            update_state = str(update_status.get('state') or '')[:24]
            update_message = str(update_status.get('message') or '')[:240]
            status_generation = update_status.get('generation', 0)
            if type(status_generation) is not int:
                status_generation = 0
            merged = dict(data.get('telemetry') or {})
            if isinstance(data.get('devices'), dict):
                merged['device_pool'] = data['devices']
            cleaned_telemetry = self.clean_telemetry(merged)
            try:
                previous_telemetry = json.loads(node['telemetry_json'] or '{}')
            except (TypeError, ValueError):
                previous_telemetry = {}
            current_bytes = cleaned_telemetry.get('network_bytes')
            previous_bytes = previous_telemetry.get('network_bytes')
            current_job_id = next(iter(traffic_jobs)) if len(traffic_jobs) == 1 else None
            # Keep the last active task for one trailing heartbeat so result-upload
            # bytes are included even when the node confirms the package between ticks.
            traffic_job_id = current_job_id or (previous_telemetry.get('_traffic_job_id')
                                                if not traffic_jobs else None)
            if (traffic_job_id and type(current_bytes) is int and type(previous_bytes) is int
                    and current_bytes >= previous_bytes):
                db.execute('UPDATE jobs SET traffic_bytes=traffic_bytes+? WHERE id=?',
                           (current_bytes - previous_bytes, traffic_job_id))
            cleaned_telemetry['_traffic_job_id'] = current_job_id
            db.execute('UPDATE nodes SET telemetry_json=?,ack_stop_version=?,ack_device_clear_version=?,ack_update_generation=? WHERE id=?',
                       (json.dumps(cleaned_telemetry), ack_stop,
                        ack_device_clear, ack_update, node_id))
            if (status_generation == node['update_generation'] and update_state
                    and update_state != node['update_state']):
                allowed = {'scheduled', 'fetching', 'installing', 'restarting',
                           'verifying', 'success', 'failed', 'rolled_back'}
                if update_state in allowed:
                    final = update_state in self.UPDATE_FINAL_STATES
                    db.execute('UPDATE nodes SET update_state=?,update_message=?,update_finished=? WHERE id=?',
                               (update_state, update_message,
                                now if final else None, node_id))
                    if update_state == 'success':
                        current = db.execute('SELECT * FROM nodes WHERE id=?', (node_id,)).fetchone()
                        if current['update_resume_enabled'] and self._device_initialization(current)['complete']:
                            db.execute('UPDATE nodes SET enabled=1 WHERE id=?', (node_id,))
                        event(db, 'node_update_success',
                              f'节点更新成功：{current["update_ref"]}', node_id=node_id)
                    elif update_state in ('failed', 'rolled_back'):
                        event(db, 'node_update_failed',
                              update_message or '节点更新失败', node_id=node_id)
            self._advance_updates(db, now)
            node = db.execute('SELECT * FROM nodes WHERE id=?', (node_id,)).fetchone()
            if not was_online:
                event(db, 'node_online', f'节点已连接：{node["name"]}', node_id=node_id)
            update = None
            if (node['update_state'] == 'dispatched'
                    and int(node['ack_update_generation'] or 0) < int(node['update_generation'] or 0)):
                update = {'generation': int(node['update_generation']),
                          'ref': node['update_ref'], 'service': 'cluster-node'}
            return {'node_id': node_id, 'name': node['name'], 'enabled': bool(node['enabled']),
                    'processes': min(desired_processes, max_p), 'threads': min(desired_threads, max_t),
                    'desired_processes': desired_processes, 'desired_threads': desired_threads,
                    'rejected': reject, 'confirmed': confirmed, 'interrupted_ack': interrupted_ack,
                    'reported': reported_ack,
                    'device_request': node['device_request'],
                    'device_clear_version': node['device_clear_version'],
                    'device_rotate_after': self.settings().get('device_rotate_after', 0),
                    'device_auto_rotate': bool(self.settings().get('device_auto_rotate')),
                    'proxy_ttl': int(self.settings().get('proxy_ttl') or 50),
                    'proxy_pool_size': int(self.settings().get('proxy_pool_size') or 1000),
                    'stop_version': node['stop_version'], 'lease_seconds': self.lease_seconds,
                    'update': update,
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
            # A package is node-sized: the node fans it out across all local
            # processes.  Leasing a second package would double the configured
            # process/thread ceiling and recreate the old overload bug.
            if not node['enabled'] or active >= 1:
                return None
            unit = db.execute("SELECT u.*,j.mode,j.req_timeout,j.req_retries,j.auth_protocol,j.device_mode FROM units u JOIN jobs j ON j.id=u.job_id WHERE u.status='pending' AND j.id=(SELECT id FROM jobs WHERE status='running' ORDER BY queued_at,created,id LIMIT 1) ORDER BY u.seq LIMIT 1").fetchone()
            if not unit:
                return None
            # A manually retried package must not inherit rows accepted under
            # its previous lease token.
            self._clear_live_unit(db, unit['id'])
            token = secrets.token_urlsafe(24)
            db.execute("UPDATE units SET status='leased',node_id=?,lease_token=?,lease_until=?,attempt=attempt+1,processed=0,live_counts=NULL,reported_rows=0,updated=? WHERE id=?",
                       (node_id, token, time.time() + self.lease_seconds, time.time(), unit['id']))
            event(db, 'unit_leased', f'工作包 #{unit["seq"]} 已分配（{unit["rows"]:,} 条）', unit['job_id'], node_id)
            self._rate_record()
            payload = {'id': unit['id'], 'job_id': unit['job_id'], 'seq': unit['seq'],
                       'timeout': unit['req_timeout'] if 'req_timeout' in unit.keys() else 20,
                       'retries': unit['req_retries'] if 'req_retries' in unit.keys() else 0,
                       'first_row': unit['first_row'], 'rows': unit['rows'],
                       'checksum': unit['checksum'], 'token': token, 'mode': unit['mode'],
                       'bytes': max(0, int(unit['byte_end']) - int(unit['byte_start'])),
                       'processes': min(node['desired_processes'], node['max_processes'] or 1,
                                        unit['rows']),
                       'threads': min(node['desired_threads'], node['max_threads'] or 1),
                       'lease_seconds': self.lease_seconds}
            if unit['mode'] == 'auth':
                # 认证任务额外带设备与出口配置；代理地址取服务端保存的原文。
                # 必须和心跳下发使用同一个节点专属配置，不能把全部 API
                # 串成一个地址交给执行器。
                config = self.proxy_for_node(node)
                settings = self.settings()
                payload.update(proxy_api=config['api'], proxy_scheme=config['scheme'],
                               proxy_pool_size=int(settings.get('proxy_pool_size') or 1000),
                               device_registration_workers=32,
                               device_usage_limit=max(1, int(
                                   settings.get('device_rotate_after') or 100)),
                               origin=settings.get('origin', 'https://new.ani.sidestore.io'),
                               _proxy_debug=bool(settings.get('proxy_debug')),
                               _auth_protocol=(unit['auth_protocol'] or 'gsa').lower(),
                               _device_mode=(unit['device_mode'] or 'one_time').lower(),
                               _web_secondary_auth=(
                                   (unit['auth_protocol'] or 'gsa').lower() == 'gsa'
                                   and bool(settings.get('web_secondary_auth'))))
            return payload

    def unit_for_node(self, node_id, unit_id, token):
        with self.db.transaction() as db:
            self._expire(db)
            unit = db.execute('SELECT * FROM units WHERE id=? AND node_id=?', (unit_id, node_id)).fetchone()
            if (not unit or not secrets.compare_digest(unit['lease_token'] or '', token or '')
                    or unit['status'] not in ('leased', 'running', 'completed', 'unknown')):
                raise Conflict('工作包租约已失效')
            return dict(unit)

    @staticmethod
    def unit_blocks(unit, block_size=64 * 1024):
        """Yield exactly one indexed package without copying the source file."""
        path = Path(unit['path'])
        start = int(unit.get('byte_start') or 0)
        end = int(unit.get('byte_end') or 0)
        if end <= start:  # compatibility with packages prepared before 0.3.5
            end = path.stat().st_size
        remaining = end - start
        with path.open('rb') as source:
            source.seek(start)
            while remaining:
                block = source.read(min(block_size, remaining))
                if not block:
                    raise OSError('source file ended inside indexed package')
                remaining -= len(block)
                yield block

    @staticmethod
    def unit_lines(unit):
        """Yield original lines inside a byte-indexed package."""
        path = Path(unit['path'])
        start = int(unit.get('byte_start') or 0)
        end = int(unit.get('byte_end') or 0)
        if end <= start:
            end = path.stat().st_size
        remaining = end - start
        with path.open('rb') as source:
            source.seek(start)
            while remaining:
                line = source.readline(min(65537, remaining))
                if not line:
                    raise OSError('source file ended inside indexed package')
                remaining -= len(line)
                yield line

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
                    line = stream.readline(self.RESULT_RECORD_BYTE_LIMIT + 1)
                    if not line:
                        break
                    if len(line) > self.RESULT_RECORD_BYTE_LIMIT or not line.endswith(b'\n'):
                        raise ValueError('结果记录格式无效')
                    value = json.loads(line)
                    # 必需字段固定，允许执行器附加 account/status 等业务字段
                    if (not {'row', 'sha256', 'bytes'} <= set(value)
                            or type(value['row']) is not int
                            or value['row'] != unit['first_row'] + count
                            or not isinstance(value['sha256'], str) or not re.fullmatch('[0-9a-f]{64}', value['sha256'])
                            or type(value['bytes']) is not int or value['bytes'] < 0):
                        raise ValueError('结果编号或摘要格式无效')
                    bucket = result_record_bucket(value)
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
                # 业务分类计数与分片确认在同一个事务里累加；重复提交走上面的 completed
                # 分支直接返回，不会二次累加。分类取自结果行里的 status。
                job_mode = db.execute('SELECT mode FROM jobs WHERE id=?',
                                      (unit['job_id'],)).fetchone()['mode']
                # Live rows were already accepted one heartbeat at a time.
                # Replace that temporary index with the authoritative complete
                # package in the same transaction so confirmed totals never
                # jump backwards or count a row twice.
                self._clear_live_unit(db, unit_id)
                if job_mode == 'auth':
                    # 只有账号校验才产生业务分类；摘要任务记成"密正"会误导
                    db.execute(
                        "UPDATE jobs SET c_valid=c_valid+?,c_wrong=c_wrong+?,c_2fa=c_2fa+?,"
                        "c_locked=c_locked+?,c_timeout=c_timeout+?,c_invalid=c_invalid+?,c_not_found=c_not_found+?,"
                        "c_unknown=c_unknown+?,updated=? WHERE id=?",
                        (buckets['valid'], buckets['wrong_password'], buckets['need_2fa'],
                         buckets['locked'], buckets['timeout'], buckets['invalid'],
                         buckets['not_found'],
                         buckets['undetermined'] + buckets['unknown'], time.time(), unit['job_id']))
                else:
                    db.execute("UPDATE jobs SET updated=? WHERE id=?", (time.time(), unit['job_id']))
                db.execute("UPDATE units SET status='completed',processed=rows,result_path=?,result_checksum=?,"
                           "result_counts=?,classification_version=?,live_counts=NULL,reported_rows=0,lease_until=NULL,updated=? WHERE id=?",
                           (str(final), expected_digest,
                            json.dumps(dict(buckets), separators=(',', ':')),
                            self.RESULT_CLASSIFICATION_VERSION, time.time(), unit_id))
                event(db, 'result_confirmed', f'工作包 #{unit["seq"]} 结果已确认（{count:,} 条）', unit['job_id'], node_id)
                unfinished = db.execute("SELECT COUNT(*) FROM units WHERE job_id=? AND status!='completed'", (unit['job_id'],)).fetchone()[0]
                if not unfinished:
                    db.execute("UPDATE jobs SET status='completed',updated=? WHERE id=?", (time.time(), unit['job_id']))
                    event(db, 'job_completed', '全部工作包已确认，任务完成', unit['job_id'])
                # 在途包一确认就要推进队列：暂停/完成的任务可能正卡着下一个
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
            # running 优先：暂停的任务已经让出执行位，不该再占据"当前任务"位置
            current = db.execute(
                "SELECT id FROM jobs WHERE status IN ('running','paused') "
                "ORDER BY CASE WHEN status='running' THEN 0 ELSE 1 END, queued_at, created, id LIMIT 1").fetchone()
            for row in db.execute("SELECT * FROM jobs ORDER BY CASE WHEN status IN ('running','paused') THEN 0 WHEN status='queued' OR (status='preparing' AND queued_at IS NOT NULL) THEN 1 ELSE 2 END,CASE WHEN status IN ('queued','running','paused') OR (status='preparing' AND queued_at IS NOT NULL) THEN queued_at END,created DESC,id LIMIT 50"):
                value = {k: row[k] for k in ('id', 'name', 'status', 'created', 'updated', 'bytes', 'rows', 'unit_rows', 'mode', 'error', 'queued_at', 'traffic_bytes', 'auth_protocol', 'device_mode')}
                value['queue_position'] = (db.execute(f'SELECT COUNT(*) FROM jobs WHERE {queue_filter} AND (queued_at<? OR (queued_at=? AND id<=?))', (row['queued_at'], row['queued_at'], row['id'])).fetchone()[0]
                                           if row['status'] in ('queued', 'preparing') and row['queued_at'] is not None else None)
                groups = db.execute('SELECT status,COUNT(*) AS n,SUM(rows) AS rows FROM units WHERE job_id=? GROUP BY status', (row['id'],)).fetchall()
                counts = {r['status']: r['n'] for r in groups}
                row_counts = {r['status']: r['rows'] for r in groups}
                sums = db.execute("SELECT COALESCE(SUM(CASE WHEN status='completed' THEN rows ELSE 0 END),0), COALESCE(SUM(CASE WHEN status IN ('running','leased','completed') THEN processed ELSE 0 END),0) FROM units WHERE job_id=?", (row['id'],)).fetchone()
                live = {key: 0 for key in self.RESULT_BUCKETS}
                if row['mode'] == 'auth':
                    for unit in db.execute("SELECT live_counts FROM units WHERE job_id=? AND status IN ('leased','running') AND live_counts IS NOT NULL",
                                           (row['id'],)):
                        try:
                            unit_counts = json.loads(unit['live_counts'])
                        except (TypeError, ValueError):
                            continue
                        for key in self.RESULT_BUCKETS:
                            live[key] += int(unit_counts.get(key, 0) or 0)
                value.update(units=sum(counts.values()), unit_counts=counts, row_counts=row_counts,
                             confirmed=min(int(row['rows']), int(sums[0]) + int(row['reported_rows'] or 0)),
                             processed=sums[1],
                             categorized={'valid': row['c_valid'] + live['valid'],
                                          'wrong_password': row['c_wrong'] + live['wrong_password'],
                                          'need_2fa': row['c_2fa'] + live['need_2fa'],
                                          'locked': row['c_locked'] + row['c_invalid'] + live['locked'],
                                          'timeout': row['c_timeout'] + live['timeout'],
                                          'not_found': row['c_not_found'] + live['not_found'],
                                          'unknown': row['c_unknown'] + live['undetermined'] + live['unknown']},
                             reported=row['reported_rows'])
                jobs.append(value)
            nodes = []
            for row in db.execute('SELECT * FROM nodes ORDER BY created'):
                value = {k: row[k] for k in ('id', 'name', 'last_seen', 'hostname', 'enabled',
                                             'desired_processes', 'desired_threads', 'max_processes',
                                             'max_threads', 'version', 'last_error', 'update_generation',
                                             'update_ref', 'update_state', 'update_message',
                                             'update_started', 'update_finished')}
                online = bool(row['last_seen'] and now - row['last_seen'] < self.offline_seconds)
                stats = db.execute("SELECT COALESCE(SUM(CASE WHEN status IN ('leased','running') THEN 1 ELSE 0 END),0), COALESCE(SUM(CASE WHEN status='completed' THEN rows ELSE reported_rows END),0), COALESCE(SUM(CASE WHEN status IN ('leased','running') THEN processed ELSE 0 END),0) FROM units WHERE node_id=?", (row['id'],)).fetchone()
                value.update(status='offline' if not online else 'draining' if not row['enabled'] else 'busy' if stats[0] else 'idle',
                             active=stats[0], confirmed=stats[1], in_progress=stats[2], rate=round(row['rate'], 1) if online and stats[0] else 0)
                if online and (row['stop_version'] > row['ack_stop_version'] or
                               not row['enabled'] and row['stop_version'] > 0 and stats[0]):
                    value['status'] = 'stopping'
                value['telemetry'] = json.loads(row['telemetry_json']) if online else {}
                initialization = self._device_initialization(row)
                value.update(device_request=initialization['requested'],
                             device_capacity=initialization['capacity'],
                             device_ready=initialization['ready'],
                             device_initialization_running=initialization['running'],
                             device_initialization_complete=initialization['complete'])
                value['effective_processes'] = min(row['desired_processes'], row['max_processes']) if row['max_processes'] else None
                value['effective_threads'] = min(row['desired_threads'], row['max_threads']) if row['max_threads'] else None
                nodes.append(value)
            events = [dict(r) for r in db.execute('SELECT * FROM events ORDER BY id DESC LIMIT 40')]
            aggregate = db.execute('SELECT COALESCE(SUM(rows),0),COUNT(*) FROM jobs').fetchone()
            counts = db.execute("SELECT COALESCE(SUM(CASE WHEN status='completed' THEN rows ELSE reported_rows END),0),COALESCE(SUM(CASE WHEN status IN ('running','leased','completed') THEN processed ELSE 0 END),0) FROM units").fetchone()
            maximum = db.execute("SELECT value FROM controller_settings WHERE key='max_unit_rows'").fetchone()[0]
            return {'jobs': jobs, 'nodes': nodes, 'events': events, 'server_time': now,
                    'current_job_id': current['id'] if current else None, 'queue': queue,
                    'settings': self.settings(),
                    'totals': {'rows': aggregate[0], 'jobs': aggregate[1], 'confirmed': counts[0],
                               'processed': counts[1], 'online': sum(n['status'] != 'offline' for n in nodes),
                               'nodes': len(nodes), 'busy': sum(n['status'] != 'offline' and n['active'] > 0 for n in nodes),
                               'queued': waiting_count, 'rate': round(sum(n['rate'] for n in nodes), 1)}}

    @staticmethod
    def _result_device_context(db):
        """Map node/slot pairs to stable short labels for result-table display."""
        numbers, labels = {}, {}
        for number, node in enumerate(db.execute(
                'SELECT id,telemetry_json FROM nodes ORDER BY created,id'), 1):
            numbers[node['id']] = number
            try:
                telemetry = json.loads(node['telemetry_json'] or '{}')
            except (TypeError, ValueError):
                telemetry = {}
            pool = telemetry.get('device_pool') if isinstance(telemetry, dict) else {}
            for item in (pool.get('slots') if isinstance(pool, dict) else []) or []:
                try:
                    slot = int(item.get('slot'))
                except (AttributeError, TypeError, ValueError):
                    continue
                label = str(item.get('label') or '').strip()
                if label:
                    labels[(node['id'], slot)] = label[:32]
        setting = db.execute("SELECT value FROM controller_settings WHERE key='proxy_debug'").fetchone()
        show_errors = bool(setting and str(setting[0]) in ('1', 'True', 'true'))
        return numbers, labels, show_errors

    @staticmethod
    def _decorate_result_device(record, unit, context):
        numbers, labels, show_errors = context
        node_id = unit['node_id']
        number = numbers.get(node_id)
        try:
            slot = int(record.get('device'))
        except (TypeError, ValueError):
            slot = 0
        label = str(record.get('device_label') or '').strip()
        if not label and node_id and slot:
            label = labels.get((node_id, slot), '')
        node_text = f'节点{number:02d}' if number else '节点—'
        device_text = label[:32] if label else (f'#{slot}' if slot else '—')
        try:
            usage = max(0, int(record.get('device_usage') or 0))
        except (TypeError, ValueError):
            usage = 0
        usage_text = f' · 第 {usage} 次' if usage else ''
        record['device_display'] = f'{node_text} · {device_text}{usage_text}'
        method_labels = {
            'verification_code': '手机或 App 验证码',
            'security_repair': '密保或安全修复',
            'repair_unspecified': '修复流程（未细分）',
        }
        record['two_factor_method_cn'] = method_labels.get(
            str(record.get('two_factor_method') or ''), '')
        if record.get('reason') in ('密码证明通过，服务要求二次验证',
                                    '密码证明通过；未确认业务服务登录状态'):
            # 旧记录里这两句是本地生成的说明，不是 Apple 原始返回。
            record['reason'] = ''
        raw_reason = str(record.get('reason') or record.get('apple_message') or '').strip()
        if record.get('web_status'):
            # A supplemental Web probe is more specific than GSA's umbrella
            # au=repair reason.  Keep that result visible even when raw Apple
            # reason debugging is enabled.
            public_record = dict(record)
            public_record.pop('reason', None)
            public_record.pop('apple_message', None)
            record['reason_cn'] = result_reason_cn(public_record, record.get('status'))
        elif show_errors and raw_reason:
            # 调试模式直接显示并返回 Apple 的原始英文，不再用中文覆盖。
            record['reason_cn'] = raw_reason[:2048]
        else:
            # 普通模式只按快捷分类显示中文；历史记录中的原始原因也不回传。
            public_record = dict(record)
            public_record.pop('reason', None)
            public_record.pop('apple_message', None)
            record['reason_cn'] = result_reason_cn(public_record, record.get('status'))
        if not show_errors:
            # Filter the response only; never rewrite authoritative result bytes.
            for field in ('reason', 'detail', 'apple_message', 'error', 'traceback', 'raw_response'):
                record.pop(field, None)
            code = str(record.get('code') or '')
            # Older workers used the first 180 characters of an exception as code.
            # Keep numeric/machine codes, but do not leak that fallback when off.
            if code and not re.fullmatch(r'-?\d+|[A-Za-z][A-Za-z0-9_.:-]{0,63}', code):
                record['code'] = ''
        return record

    def _read_rows_with_live(self, db, job_id, offset, limit, category, device_context):
        """Merge completed package files with heartbeat-confirmed live rows.

        Only active packages occupy ``live_result_rows`` (normally one per
        node, at most 10,000 rows each).  Completed packages keep using the
        scalable file/index path.  Iterating package metadata preserves row
        order without retaining an unbounded per-account database table.
        """
        wanted = (('undetermined', 'unknown') if category == 'undetermined'
                  else (category,))
        units = list(db.execute(
            "SELECT id,seq,status,first_row,rows,result_path,result_counts,reported_rows,node_id "
            "FROM units WHERE job_id=? AND (status='completed' OR reported_rows>0) ORDER BY seq",
            (job_id,)))
        result, skipped, total = [], 0, 0
        for unit in units:
            count = 0
            counts = None
            if unit['status'] == 'completed':
                if category == 'all':
                    count = int(unit['rows'])
                else:
                    try:
                        counts = json.loads(unit['result_counts'] or '{}')
                    except (TypeError, ValueError):
                        counts = None
                    if counts is not None:
                        count = sum(int(counts.get(key, 0) or 0) for key in wanted)
                    else:
                        # Legacy files without counts are uncommon and small;
                        # let the scan below determine their contribution.
                        count = 0
            else:
                if category == 'all':
                    count = int(unit['reported_rows'] or 0)
                else:
                    placeholders = ','.join('?' for _ in wanted)
                    count = int(db.execute(
                        f'SELECT COUNT(*) FROM live_result_rows WHERE unit_id=? '
                        f'AND category IN ({placeholders})',
                        (unit['id'], *wanted)).fetchone()[0])
            total += count
            if count and skipped + count <= offset:
                skipped += count
                continue
            if len(result) >= limit:
                continue

            if unit['status'] == 'completed':
                path = unit['result_path']
                if not path or not Path(path).exists():
                    continue
                try:
                    with Path(path).open('r', encoding='utf-8') as stream:
                        for line in stream:
                            try:
                                record = json.loads(line)
                            except ValueError:
                                continue
                            record['status'] = result_record_bucket(record)
                            self._decorate_result_device(record, unit, device_context)
                            if category != 'all' and record['status'] not in wanted:
                                continue
                            if counts is None and category != 'all':
                                total += 1
                            if skipped < offset:
                                skipped += 1
                                continue
                            if len(result) < limit:
                                result.append(record)
                            if (len(result) >= limit
                                    and (category == 'all' or counts is not None)):
                                break
                except OSError:
                    continue
            else:
                placeholders = ','.join('?' for _ in wanted)
                sql = ('SELECT payload FROM live_result_rows WHERE unit_id=? '
                       + ('' if category == 'all'
                          else f'AND category IN ({placeholders}) ')
                       + 'ORDER BY row')
                params = ((unit['id'],) if category == 'all'
                          else (unit['id'], *wanted))
                for live in db.execute(sql, params):
                    try:
                        record = json.loads(live['payload'])
                    except (TypeError, ValueError):
                        continue
                    record['status'] = result_record_bucket(record)
                    self._decorate_result_device(record, unit, device_context)
                    if skipped < offset:
                        skipped += 1
                        continue
                    if len(result) < limit:
                        result.append(record)
        return {'rows': result, 'total': total, 'offset': offset}

    def _search_rows_uncached(self, db, job_id, offset, limit, category, wanted,
                              device_context, search):
        """Search confirmed accounts across completed shards and current live rows."""
        needle = str(search or '').strip().casefold()
        rows, total = [], 0
        units = db.execute(
            "SELECT id,status,result_path,node_id FROM units WHERE job_id=? "
            "AND status IN ('leased','running','completed') ORDER BY seq", (job_id,))
        for unit in units:
            if unit['status'] == 'completed':
                path = unit['result_path']
                if not path or not Path(path).exists():
                    continue
                try:
                    stream = Path(path).open('r', encoding='utf-8')
                except OSError:
                    continue
                with stream:
                    payloads = stream
                    for payload in payloads:
                        try:
                            record = json.loads(payload)
                        except (TypeError, ValueError):
                            continue
                        record['status'] = result_record_bucket(record)
                        if category != 'all' and record['status'] not in wanted:
                            continue
                        if needle not in str(record.get('account') or '').casefold():
                            continue
                        total += 1
                        if total <= offset or len(rows) >= limit:
                            continue
                        self._decorate_result_device(record, unit, device_context)
                        rows.append(record)
            else:
                for live in db.execute(
                        'SELECT payload FROM live_result_rows WHERE unit_id=? ORDER BY row',
                        (unit['id'],)):
                    try:
                        record = json.loads(live['payload'])
                    except (TypeError, ValueError):
                        continue
                    record['status'] = result_record_bucket(record)
                    if category != 'all' and record['status'] not in wanted:
                        continue
                    if needle not in str(record.get('account') or '').casefold():
                        continue
                    total += 1
                    if total <= offset or len(rows) >= limit:
                        continue
                    self._decorate_result_device(record, unit, device_context)
                    rows.append(record)
        return {'rows': rows, 'total': total, 'offset': offset, 'search': search}

    def _read_rows_uncached(self, job_id, offset=0, limit=100, category='all', search=''):
        """按行号窗口读取结果，供页面表格分页。

        全部结果按 first_row 直接定位覆盖页面的分片；分类结果利用每片
        result_counts 跳过不需要的整片。100,000,000 条任务的高位页不再从第 1 条扫描。
        """
        limit = max(1, min(int(limit), 500))
        offset = max(0, int(offset))
        categories = ('all', 'valid', 'wrong_password', 'need_2fa', 'locked',
                      'not_found', 'timeout', 'undetermined', 'unknown')
        if category not in categories:
            raise ValueError('结果分类无效')
        wanted = (('undetermined', 'unknown') if category == 'undetermined'
                  else (category,))
        with self.db.connect() as db:
            device_context = self._result_device_context(db)
            job = db.execute(
                "SELECT rows,(SELECT COALESCE(SUM(rows),0) FROM units "
                "WHERE job_id=? AND status='completed') AS total FROM jobs WHERE id=?",
                (job_id, job_id)).fetchone()
            if not job:
                raise ValueError('任务不存在')
            if search:
                return self._search_rows_uncached(
                    db, job_id, offset, limit, category, wanted, device_context, search)
            if db.execute('SELECT 1 FROM live_result_rows WHERE job_id=? LIMIT 1',
                          (job_id,)).fetchone():
                return self._read_rows_with_live(db, job_id, offset, limit, category,
                                                 device_context)
            if category == 'all':
                # offset 是全局零基行号，利用分片的连续行号范围直接定位。
                first, last = offset + 1, offset + limit
                units = list(db.execute(
                    "SELECT seq,first_row,rows,result_path,result_counts,node_id FROM units "
                    "WHERE job_id=? AND status='completed' AND first_row<=? "
                    "AND first_row+rows>? ORDER BY seq", (job_id, last, first)))
            else:
                # 分类偏移不能由全局行号推导，但可用分片计数跳过整个结果文件。
                units = list(db.execute(
                    "SELECT seq,first_row,rows,result_path,result_counts,node_id FROM units "
                    "WHERE job_id=? AND status='completed' ORDER BY seq", (job_id,)))

            rows, skipped, total = [], 0, 0
            for unit in units:
                counts = None
                if unit['result_counts']:
                    try:
                        counts = json.loads(unit['result_counts'])
                    except (TypeError, ValueError):
                        counts = None
                if category != 'all' and counts is not None:
                    count = sum(int(counts.get(key, 0) or 0) for key in wanted)
                    total += count
                    if skipped + count <= offset:
                        skipped += count
                        continue
                    if len(rows) >= limit:
                        continue

                path = unit['result_path']
                if not path or not Path(path).exists():
                    continue
                try:
                    with Path(path).open('r', encoding='utf-8') as stream:
                        local_index = 0
                        for line in stream:
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                record = json.loads(line)
                            except ValueError:
                                continue
                            record['status'] = result_record_bucket(record)
                            self._decorate_result_device(record, unit, device_context)
                            if category != 'all' and record['status'] not in wanted:
                                continue
                            if category == 'all':
                                absolute = unit['first_row'] + local_index
                                local_index += 1
                                if absolute <= offset:
                                    continue
                            elif counts is None:
                                total += 1
                            if category != 'all' and skipped < offset:
                                skipped += 1
                                continue
                            if len(rows) < limit:
                                rows.append(record)
                            if (len(rows) >= limit
                                    and (category == 'all' or counts is not None)):
                                break
                except OSError:
                    continue
            if category != 'all':
                # Older shards may carry counts produced by an earlier rule set.
                # A complete short-page scan must never display fewer total rows
                # than it actually returned.
                total = max(total, offset + len(rows))
            return {'rows': rows, 'total': job['total'] if category == 'all' else total,
                    'offset': offset}

    @staticmethod
    def _copy_result_page(page):
        return {'rows': [dict(row) for row in page['rows']],
                'total': page['total'], 'offset': page['offset']}

    def read_rows(self, job_id, offset=0, limit=100, category='all', search=''):
        """Read a page once and reuse it while a paused/completed job is stable."""
        limit = max(1, min(int(limit), 500))
        offset = max(0, int(offset))
        search = str(search or '').strip()[:256]
        with self.result_query_lock:
            with self.db.connect() as db:
                job = db.execute('SELECT status,updated FROM jobs WHERE id=?',
                                 (job_id,)).fetchone()
                debug_row = db.execute(
                    "SELECT value FROM controller_settings WHERE key='proxy_debug'").fetchone()
            if not job:
                raise ValueError('任务不存在')
            stable = job['status'] in ('paused', 'completed')
            key = (job_id, offset, limit, category, search, float(job['updated']),
                   str(debug_row['value']) if debug_row else '0')
            if stable and key in self.result_page_cache:
                page = self.result_page_cache.pop(key)
                self.result_page_cache[key] = page
                return self._copy_result_page(page)

            page = self._read_rows_uncached(job_id, offset, limit, category, search)
            if stable:
                self.result_page_cache[key] = self._copy_result_page(page)
                while len(self.result_page_cache) > 32:
                    self.result_page_cache.popitem(last=False)
            return self._copy_result_page(page)

    def delete_job(self, job_id):
        """删除任务并撤销已下发工作包；节点下次心跳会立即中止。"""
        with self.db.transaction() as db:
            job = db.execute('SELECT * FROM jobs WHERE id=?', (job_id,)).fetchone()
            if not job:
                raise ValueError('任务不存在')
            if job['status'] == 'preparing':
                raise Conflict('文件正在准备工作包，准备完成后再删除')
            active = db.execute("SELECT COUNT(*) FROM units WHERE job_id=? "
                                "AND status IN ('leased','running','unknown')",
                                (job_id,)).fetchone()[0]
            db.execute('DELETE FROM live_result_rows WHERE job_id=?', (job_id,))
            db.execute('DELETE FROM units WHERE job_id=?', (job_id,))
            db.execute('DELETE FROM jobs WHERE id=?', (job_id,))
            event(db, 'job_deleted',
                  f'任务已删除：{job["name"]}；已撤销 {active} 个在途工作包', job_id=None)
            self._advance_queue(db)
            remaining = db.execute('SELECT COUNT(*) FROM jobs WHERE source=?',
                                   (job['source'],)).fetchone()[0]
        shutil.rmtree(self.units_dir / job_id, ignore_errors=True)
        shutil.rmtree(self.results / job_id, ignore_errors=True)
        source = Path(job['source']) if job['source'] else None
        if source and remaining == 0 and source.exists() and source.parent == self.inputs:
            source.unlink(missing_ok=True)
        return {'deleted': job_id, 'source_kept': bool(remaining),
                'cancelled_units': int(active)}

    def export_results(self, job_id, category='all'):
        """按分类流式导出，内容保持上传时的原始记录（`账号----密码`）。

        结果文件和输入分包行数一一对应，所以两个文件同步读取即可：
        既能按 status 过滤，又不用把整包读进内存。
        """
        with self.db.connect() as db:
            job = db.execute('SELECT id,status FROM jobs WHERE id=?', (job_id,)).fetchone()
            if not job:
                raise ValueError('任务不存在')
            if job['status'] not in ('paused', 'completed'):
                raise Conflict('任务暂停或全部完成后才可导出已确认结果')
            units = [dict(row) for row in db.execute(
                "SELECT id,seq,status,first_row,path,byte_start,byte_end,result_path "
                "FROM units WHERE job_id=? AND (status='completed' OR reported_rows>0) ORDER BY seq",
                (job_id,))]
            for unit in units:
                if unit['status'] != 'completed':
                    unit['live_categories'] = {
                        int(row['row']): row['category'] for row in db.execute(
                            'SELECT row,category FROM live_result_rows WHERE unit_id=?',
                            (unit['id'],))}

        def rows():
            for unit in units:
                source, result = Path(unit['path']), unit['result_path']
                if unit['status'] != 'completed':
                    if not source.exists():
                        continue
                    live = unit.get('live_categories') or {}
                    try:
                        for row_number, original in enumerate(
                                self.unit_lines(unit), int(unit['first_row'])):
                            bucket = live.get(row_number)
                            if bucket is None or category != 'all' and bucket != category:
                                continue
                            yield original
                    except OSError:
                        pass
                    continue
                if not result or not Path(result).exists() or not source.exists():
                    continue
                try:
                    # 二进制模式：保留原始换行符（\r\n 不能被文本模式转成 \n）
                    with Path(result).open('rb') as res:
                        for original, line in zip(self.unit_lines(unit), res):
                            line = line.strip()
                            if not line:
                                continue
                            if category != 'all':
                                try:
                                    if result_record_bucket(json.loads(line)) != category:
                                        continue
                                except ValueError:
                                    continue
                            yield original      # 严格保持原字节，包括末行没有换行的情况
                except OSError:
                    continue
        return rows()

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
