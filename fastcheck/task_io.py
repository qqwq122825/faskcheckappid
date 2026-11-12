"""Task inputs and credential-free event logging shared by CLI and Web."""
from dataclasses import dataclass,field
from datetime import datetime
import hashlib,json,os,threading
from pathlib import Path
from zoneinfo import ZoneInfo
from urllib.parse import urlsplit
from .devices import private_directory

@dataclass(repr=False)
class Account:
    row: int
    email: str
    password: str = field(repr=False)
    queued_at: float = 0


def read_accounts(path, *, max_accounts=None):
    records, seen = [], set()
    for line_number, line in enumerate(Path(path).expanduser().read_text(encoding='utf-8-sig').splitlines(), 1):
        if not line.strip():
            continue
        email, separator, password = line.partition('----')
        email = email.strip()
        if not separator or not password or '@' not in email or email.endswith('.invalid'):
            raise ValueError(f'第 {line_number} 行格式错误或仍为示例：应为 邮箱----密码。')
        if len(email) > 4096 or len(password) > 4096 or any(ord(c) < 32 for c in email + password):
            raise ValueError(f'第 {line_number} 行包含无效字段。')
        if email.casefold() in seen:
            raise ValueError(f'第 {line_number} 行账号重复，请每个账号仅保留一行。')
        seen.add(email.casefold())
        records.append(Account(len(records) + 1, email, password))
    if not records or (max_accounts is not None and len(records)>max_accounts):
        raise ValueError(f'此测试入口接收 1–{max_accounts} 个不同的自有账号。' if max_accounts else '账号文件不能为空。')
    return records


class EventLog:
    labels = {
        'batch_start':'批次启动', 'proxy_pool_start':'代理池启动', 'proxy_fetch_start':'代理 API 开始补充',
        'proxy_fetch_done':'代理 API 补充完成', 'proxy_fetch_failed':'代理 API 提取失败',
        'proxy_expired':'丢弃过期代理', 'proxy_too_old':'代理剩余有效期不足',
        'proxy_supplier_stopped':'代理供给线程停止', 'devices_restored':'读取已注册设备',
        'proxy_lease_acquired':'领取代理租约', 'proxy_lease_released':'释放代理租约',
        'device_register_start':'设备注册线程领取 IP，开始注册', 'device_register_done':'设备注册成功入池',
        'device_register_failed':'设备注册失败', 'device_supplier_stopped':'设备注册线程停止',
        'device_register_retry':'同一设备身份更换故障租约，重试注册',
        'workers_ready':'设备达到启动水位，认证开始', 'account_dispatch':'账号领取设备和 IP，启动认证进程',
        'account_attempt_done':'本轮认证返回', 'account_retry':'同设备任务进入重试等待',
        'account_done':'账号处理结束', 'child_stopped':'停止并回收子进程',
        'batch_stop':'批次停止分发', 'batch_done':'批次完成',
    }

    def __init__(self, directory, *, debug=False, console=True):
        self.directory = private_directory(Path(directory))
        self.lock = threading.Lock()
        self.path = self.directory / 'events.jsonl'
        self.results_path = self.directory / 'results.jsonl'
        self.debug_enabled = debug
        self.console = console
        self.debug_dropped = 0
        self.debug_spans = {}
        if debug:
            descriptor = os.open(self.directory/'debug.jsonl', os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o600)
            os.close(descriptor)

    def __call__(self, event, **fields):
        # Call sites pass only row numbers, lease labels, class names and timings.
        # URLs, account names, passwords, device material and raw messages are excluded.
        forbidden = {'email', 'password', 'url', 'api_url', 'headers', 'identity', 'detail', 'apple_message'}
        if forbidden.intersection(fields):
            raise ValueError('Sensitive field rejected by event logger')
        stamp = datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(timespec='milliseconds')
        record = dict(fields, event=event, at=stamp, pid=os.getpid(), thread=threading.current_thread().name)
        with self.lock:
            descriptor = os.open(self.path, os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o600)
            with os.fdopen(descriptor, 'a', encoding='utf-8') as handle:
                handle.write(json.dumps(record, ensure_ascii=False, separators=(',', ':')) + '\n')
            if self.console:
                display = ' '.join(f'{key}={value}' for key, value in fields.items())
                print(f'[{stamp[11:23]}] {self.labels.get(event, event)} {display}', flush=True)

    def result(self, value):
        with self.lock:
            descriptor = os.open(self.results_path, os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o600)
            with os.fdopen(descriptor, 'a', encoding='utf-8') as handle:
                handle.write(json.dumps(value, ensure_ascii=False, separators=(',', ':')) + '\n')

    def debug(self, value):
        if not self.debug_enabled:
            return
        try:
            stamp = datetime.now(ZoneInfo('Asia/Shanghai')).isoformat(timespec='milliseconds')
            with self.lock:
                path = self.directory / 'debug.jsonl'
                descriptor = os.open(path, os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o600)
                with os.fdopen(descriptor, 'a', encoding='utf-8') as handle:
                    handle.write(json.dumps(dict(value, logged_at=stamp), ensure_ascii=False,
                                            separators=(',', ':')) + '\n')
                if value.get('event') == 'phase_end' and isinstance(value.get('ms'), (int,float)):
                    key = (value.get('role','parent'), value.get('phase','unknown'), value.get('request_phase',''))
                    self.debug_spans.setdefault(key, []).append(value['ms'])
        except OSError:
            self.debug_dropped += 1

    def debug_summary(self):
        rows = []
        for (role, phase, request_phase), values in sorted(self.debug_spans.items()):
            values = sorted(values)
            rows.append({'role':role,'phase':phase,'request_phase':request_phase,'count':len(values),
                         'mean_ms':round(sum(values)/len(values),3),'max_ms':max(values),
                         'p95_ms':values[round((len(values)-1)*.95)]})
        return {'phases':rows,'log_write_failures':self.debug_dropped,
                'note':'Nested spans overlap; do not add all phase durations together.'}


def proxy_fingerprint(url):
    parsed = urlsplit(url)
    return hashlib.sha256(f'{parsed.scheme}|{parsed.hostname}|{parsed.port}'.encode()).hexdigest()[:12]


