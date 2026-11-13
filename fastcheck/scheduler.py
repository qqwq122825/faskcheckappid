"""One owner for task scheduling, expiring IP/device leases and result files."""
from collections import Counter, deque
from dataclasses import dataclass
import multiprocessing as mp
from pathlib import Path
import queue
import threading
import time

from fastcheck.workers import protocol_attempt, register_device, worker_process
from fastcheck.proxy_pool import ApiFetcher, ProxyPool
from fastcheck.protocols.proxy_api import batch_limit as url_batch_limit
from fastcheck.task_io import EventLog, proxy_fingerprint
from fastcheck.devices import DevicePool, save_json
from fastcheck.status import StatusPanel, snapshot
from fastcheck.device_scheduler import DeviceScheduler
from fastcheck.exports import ClassifiedExports


@dataclass(frozen=True)
class Settings:
    protocol: str = 'web'
    processes: int = 2
    threads: int = 10
    device_capacity: int = 10
    device_parallelism: int = 0
    device_selection: str = 'prefer_new'
    warmup_devices: int = 2
    device_rest_after: int = 20
    device_rest_seconds: float = 30
    device_rest_slots: int = 1
    proxy_target: int = 0
    proxy_ttl: float = 60
    proxy_scheme: str = 'http'
    # True=anisette（sidestore.io）也走代理。服务器直连境外 anisette 常超时，
    # 打开后注册/刷新都借任务隧道，代价是每台设备注册慢约 1 秒。
    anisette_proxy: bool = False
    timeout: float = 10
    prepare_timeout: float = 12
    attempt_budget: float = 35
    registration_budget: float = 40
    resource_timeout: float = 120
    max_attempts: int = 1
    debug: bool = False
    origin: str = 'https://new.ani.sidestore.io'

    def __post_init__(self):
        if self.protocol not in ('web', 'gsa'):
            raise ValueError('认证通道必须是 web 或 gsa。')
        if self.device_selection not in ('balanced','prefer_new'):
            raise ValueError('设备选择必须是 balanced 或 prefer_new。')
        if self.proxy_scheme not in ('http','socks5','socks5h'):
            raise ValueError('代理协议必须是 http、socks5 或 socks5h。')
        if type(self.anisette_proxy) is not bool:
            raise ValueError('anisette_proxy 必须是布尔值。')
        for name, low, high in (('processes', 1, None), ('threads', 1, None),
                                ('device_capacity', 1, None), ('proxy_target', 0, None),
                                ('device_parallelism',0,None),
                                ('device_rest_after', 0, None), ('device_rest_slots', 1, None),
                                ('warmup_devices', 1, self.device_capacity), ('max_attempts', 1, 6)):
            value = getattr(self, name)
            if type(value) is not int or value < low or (high is not None and value > high):
                raise ValueError(f'{name} 必须不少于 {low}'+(f' 且不超过 {high}' if high is not None else '')+'。')
        for name in ('timeout', 'prepare_timeout', 'resource_timeout', 'attempt_budget',
                     'registration_budget'):
            if not 0 < getattr(self, name) <= 600:
                raise ValueError(f'{name} 必须大于 0 且不超过 600 秒。')
        # 隧道式住宅代理的端口可以长时间复用，有效期不必再被 10 分钟上限卡住；
        # TTL 越长，供应商侧的重复提取次数越少，也不需要为了补货等线程。
        if not 0 < self.proxy_ttl <= 3600:
            raise ValueError('proxy_ttl 必须大于 0 且不超过 3600 秒。')
        if not 0 <= self.device_rest_seconds <= 3600:
            raise ValueError('device_rest_seconds 必须在 0–3600 秒之间。')
        if max(self.attempt_budget, self.registration_budget) + 4 > self.proxy_ttl:
            raise ValueError('任务/注册预算必须比 IP 有效期至少少 4 秒。')


@dataclass(repr=False)
class Task:
    account: object
    attempt: int = 0
    slot: int | None = None
    ready_at: float = 0
    history: list = None

    def __post_init__(self):
        self.history = []


# 身份池调度策略（供维护参考，不在界面上展示）：
#   · device_parallelism 固定 0：单身份并发跟随总体线程容量，设备之间按在途数
#     均匀分配，不做额外限流；这个参数不再暴露到界面上；
#   · warmup_devices 默认 2，够 2 台就绪就派发任务，注册线程在后台继续补齐；
#   · device_rest_after / device_rest_seconds 为 0 时关闭休息策略。
class Supervisor:
    def __init__(self, accounts, api_url, root, directory, settings, *, proxy_auth='',
                 handler=protocol_attempt, registration=register_device, fetcher=None,progress=None,console=True):
        self.settings = settings
        self.root = Path(root)
        self.log = EventLog(directory, debug=settings.debug, console=False)
        self.exports=ClassifiedExports(self.log.directory/'exports')
        self.export_failures=[]
        self.progress,self.console=progress,console
        self.context = mp.get_context('spawn')
        self.stop = self.context.Event()
        self.stop_reason = None
        self.stop_at = None
        self.reason_lock = threading.Lock()
        self.resource_lock = threading.Lock()
        self.pending = deque(Task(a) for a in accounts)
        self.accounts = accounts
        self.active, self.results, self.workers = {}, {}, {}
        self.devices = set()
        self.restored_devices = 0
        self.device_parallelism = settings.device_parallelism or settings.processes*settings.threads
        self.device_scheduler = DeviceScheduler(rest_after=settings.device_rest_after,
            rest_seconds=settings.device_rest_seconds, rest_slots=settings.device_rest_slots,
            minimum_ready=min(settings.warmup_devices,len(accounts)), concurrency=self.device_parallelism,
            selection=settings.device_selection,log=self.log)
        self.handler, self.registration = handler, registration
        self.registration_done = threading.Event()
        self.registration_thread = None
        self.max_active = 0
        self.worker_serial = 0
        self.next_retry_at = float('inf')
        self.device_root = self.root/'config/batch-device-pool'
        self.pool = DevicePool(self.device_root, settings.origin, max_devices=settings.device_capacity) if settings.protocol == 'gsa' else None
        fetch = fetcher or ApiFetcher(self.context, self.stop, proxy_auth,
                                     debug_log=self.log.debug if settings.debug else None,
                                     scheme=None if settings.proxy_scheme=='http' else settings.proxy_scheme)
        # 预取来源优先级：显式 proxy_target → 链接里的 num 上限 → 并发容量 ×1.5。
        # 供应商给的链接里写 num=2000，就表示这一批允许一次要 2000 条。
        capacity = settings.processes*settings.threads
        api_limits = [url_batch_limit(value) for value in str(api_url or '').split() if value]
        api_ceiling = max([value for value in api_limits if value] or [0])
        supply = settings.proxy_target or api_ceiling or (capacity+capacity//2)
        self.initial_proxy_target = min(supply, len(accounts))
        # 单次提取上限交给 ProxyPool 按链接里的 num 决定，这里不再写死。
        self.proxies = ProxyPool(api_url, self.stop, self.log, ttl=settings.proxy_ttl,
            target=self.initial_proxy_target, fetcher=fetch, on_limit=self.request_stop)

    def request_stop(self, reason):
        with self.reason_lock:
            if self.stop_reason is None:
                self.stop_reason = reason
                self.stop_at = time.monotonic()
                self.log('supervisor_stop', reason=reason)
        self.stop.set()
        self.proxies.wake()

    @staticmethod
    def reap(process, output):
        if process.is_alive():
            process.terminate()
        process.join(1)
        if process.is_alive():
            process.kill()
            process.join(2)
        output.cancel_join_thread()
        output.close()
        process.close()

    def prepare_devices(self):
        s = self.settings
        # 单台注册失败不该终止整轮：隧道路由抖一次就把后面几十个空槽位全部放弃，
        # 后面的任务会因为凑不够启动门槛直接判死。连续失败到上限才收手。
        max_consecutive_failures = 3
        failures = 0
        try:
            for slot in range(1, s.device_capacity+1):
                if self.stop.is_set():
                    break
                if slot in self.devices:
                    continue
                if self.pool.inspect(slot)['state'] == 'invalid':
                    self.log('registration_failed', device=slot, error_type='InvalidSavedState')
                    failures += 1
                    if failures >= max_consecutive_failures:
                        break
                    continue
                lease = None
                until = time.monotonic()+s.resource_timeout
                while lease is None and not self.stop.is_set() and time.monotonic() < until:
                    # 注册也要出口，但绝不能因此把任务饿死：库存满额时共用一条即可。
                    lease = self.proxies.take(timeout=.2, min_remaining=s.registration_budget+3,
                                              share=True)
                if lease is None:
                    failures += 1
                    if failures >= max_consecutive_failures:
                        break
                    continue
                output = self.context.Queue(maxsize=2048)
                payload = {'slot': slot, 'proxy': lease.url, 'device_root': str(self.device_root),
                           'origin': s.origin, 'prepare_timeout': max(.1, s.registration_budget-3),
                           'device_capacity': s.device_capacity, 'debug': s.debug,
                           'anisette_proxy': s.anisette_proxy}
                process = self.context.Process(target=self.registration, args=(payload, output),
                                               name=f'device-register-{slot}')
                result = None
                try:
                    self.log('registration_start', device=slot, proxy=lease.label)
                    process.start()
                    deadline = min(time.monotonic()+s.registration_budget, lease.expires_at-2)
                    while not self.stop.is_set() and time.monotonic() < deadline:
                        try:
                            value = output.get(timeout=.1)
                        except queue.Empty:
                            if not process.is_alive():
                                break
                            continue
                        if '_debug' in value:
                            self.log.debug(value['_debug'])
                        else:
                            result = value
                            break
                finally:
                    self.reap(process, output)
                    self.proxies.release(lease, valid=bool(result and result.get('ok')))
                if not result or not result.get('ok'):
                    self.log('registration_failed', device=slot, **(result or {'kind':'timeout'}))
                    if result and result.get('http') == 429:
                        self.request_stop('registration_ratelimit')
                        break
                    failures += 1
                    if failures >= max_consecutive_failures:
                        break
                    continue
                if self.pool.inspect(slot)['state'] != 'ready':
                    failures += 1
                    if failures >= max_consecutive_failures:
                        break
                    continue
                failures = 0
                with self.resource_lock:
                    self.devices.add(slot)
                    self.device_scheduler.add(slot)
                self.log('registration_ready', device=slot, ready_devices=len(self.devices))
                if self.stop.wait(.2):
                    break
        except Exception as exc:
            self.log('registration_failed', error_type=type(exc).__name__)
        finally:
            self.registration_done.set()

    def start_workers(self):
        for worker in range(1, self.settings.processes+1):
            incoming = self.context.Queue(maxsize=self.settings.threads)
            outgoing = self.context.Queue(maxsize=8192)
            process = self.context.Process(target=worker_process,
                args=(worker, self.settings.threads, incoming, outgoing, self.stop, self.handler),
                name=f'{self.settings.protocol}-worker-{worker}')
            process.start()
            self.workers[worker] = {'process': process, 'in': incoming, 'out': outgoing,
                                    'ready': False, 'rows': set(), 'dispatch_serial': 0}

    def available_worker(self):
        choices = [(len(w['rows']), w['dispatch_serial'], n) for n, w in self.workers.items()
                   if w['ready'] and len(w['rows']) < self.settings.threads]
        return min(choices)[2] if choices else None

    def choose_device(self):
        health = self.pool.health()
        now = time.time()
        if health.get('provider_until',0) > now:
            allowed = set()
        else:
            with self.resource_lock:
                allowed = {slot for slot in self.devices if
                           health['devices'].get(str(slot),{}).get('until',0) <= now}
        return self.device_scheduler.reserve(allowed)

    def proxy_demand(self):
        # 在途任务仍持有 IP，必须计入需求：只看 pending 会在派发高峰后立刻收缩
        # 库存目标，供应商随即再补一次货，把隧道额度反复消耗在无谓的重新提取上。
        registration_need = int(self.pool is not None and not self.registration_done.is_set())
        remaining = len(self.pending)+len(self.active)
        return min(self.initial_proxy_target,max(remaining,registration_need))

    def dispatch(self):
        s = self.settings
        # Due retries already own devices. Running fresh jobs ahead of them can
        # strand every reserved device behind jobs that need a free device.
        # 只在有重试到期时才重排队列：零重试批次（默认）整数次扫描都不做，
        # 否则每秒三十多轮全量遍历会让大批次的主进程空转。
        now = time.monotonic()
        if self.next_retry_at <= now:
            due = [job for job in self.pending if job.attempt and job.ready_at <= now]
            other = [job for job in self.pending if not (job.attempt and job.ready_at <= now)]
            self.pending = deque(due+other)
            upcoming = [job.ready_at for job in self.pending if job.attempt]
            self.next_retry_at = min(upcoming) if upcoming else float('inf')
        # Only the parent gives out resources. No child waits with an IP/device
        # held while requesting a second resource from another child.
        for _ in range(len(self.pending)):
            worker = self.available_worker()
            if worker is None or self.stop.is_set():
                break
            job = self.pending.popleft()
            if job.ready_at > now:
                self.pending.append(job)
                continue
            if self.pool and job.slot is None:
                job.slot = self.choose_device()
                if job.slot is None:
                    self.pending.appendleft(job)
                    break
            # 池子已达目标但全部在借用中时共用出口，保证线程不空转。
            lease = self.proxies.take(min_remaining=s.attempt_budget+3,
                                      target_after_take=self.proxy_demand(),
                                      fallback_remaining=max(15, int(s.attempt_budget//2)),
                                      share=True)
            if lease is None:
                self.pending.appendleft(job)
                break
            job.attempt += 1
            payload = {'row': job.account.row, 'email': job.account.email, 'password': job.account.password,
                       'attempt': job.attempt, 'slot': job.slot, 'proxy': lease.url, 'protocol': s.protocol,
                       'timeout': s.timeout, 'prepare_timeout': s.prepare_timeout, 'debug': s.debug,
                       'device_root': str(self.device_root), 'device_capacity': s.device_capacity, 'origin': s.origin}
            payload['device_parallelism']=self.device_parallelism
            payload['anisette_proxy']=s.anisette_proxy
            deadline = min(time.monotonic()+s.attempt_budget, lease.expires_at-2)
            row = job.account.row
            self.active[row] = {'job': job, 'worker': worker, 'lease': lease, 'deadline': deadline}
            w = self.workers[worker]
            w['rows'].add(row)
            self.worker_serial += 1
            w['dispatch_serial'] = self.worker_serial
            w['in'].put_nowait(payload)
            device_fields = self.device_scheduler.dispatched(job.slot) if job.slot is not None else {}
            self.max_active = max(self.max_active, len(self.active))
            self.log('task_dispatch', row=row, attempt=job.attempt, worker=worker,
                     device=job.slot, proxy=lease.label, active=len(self.active),
                     worker_active=len(w['rows']), proxy_fingerprint=proxy_fingerprint(lease.url), **device_fields)

    def finish(self, job, result):
        row = job.account.row
        value = dict(result, row=row, protocol=self.settings.protocol, device=job.slot,
                     attempts=job.attempt, history=list(job.history))
        self.results[row] = value
        self.log.result(value)
        self.log('task_done', row=row, device=job.slot, status=value.get('status'), stage=value.get('stage'),
                 apple_ec=value.get('apple_ec'), attempts=job.attempt)
        try:self.exports.write(job.account,value)
        except OSError as exc:
            self.export_failures.append(row)
            self.log('export_failed',row=row,error_type=type(exc).__name__)
        if job.slot is not None:
            self.device_scheduler.release(job.slot,allow_rest=not self.stop.is_set() and bool(self.pending or self.active))
        job.account.password = ''

    def accept(self, message):
        row = message['row']
        item = self.active.get(row)
        if not item or item['job'].attempt != message['attempt'] or item['worker'] != message['worker']:
            return
        if message['type'] == 'started':
            self.log('task_started', row=row, worker=message['worker'], worker_pid=message['pid'],
                     worker_thread=message['thread_id'], attempt=message['attempt'])
            return
        del self.active[row]
        self.workers[item['worker']]['rows'].discard(row)
        job, result = item['job'], message['result']
        job.history.append(dict(result, attempt=job.attempt, worker=item['worker']))
        # 只有传输层失败才说明出口本身不可用。HTTP 5xx/超时是目标站或链路抖动，
        # 跟着丢弃整条隧道会让供应商重复提取同一批出口，白烧额度、白等补货。
        # 两件事要分开：出口坏没坏（决定是否回收隧道）和这次值不值得重试。
        # 超时/5xx 是链路抖动，重试有意义；但把它们当成"出口坏了"去丢弃，
        # 会让供应商反复重新提取同一批出口。
        lease_failed = result.get('status') == 'error' and result.get('kind') in ('network', 'tls')
        self.proxies.release(item['lease'], valid=not lease_failed)
        retryable = result.get('status') == 'error' and result.get('kind') in (
            'network', 'tls', 'timeout', 'server')
        if result.get('kind') == 'ratelimit' or result.get('http') == 429:
            self.finish(job, result)
            self.request_stop('authentication_ratelimit')
        elif (retryable and result.get('stage') in ('init', 'anisette_refresh')
              and job.attempt < self.settings.max_attempts and not self.stop.is_set()):
            job.ready_at = time.monotonic()+1
            self.pending.append(job)
            self.next_retry_at = min(self.next_retry_at, job.ready_at)
            self.log('task_retry', row=row, next_attempt=job.attempt+1, device=job.slot,
                     reason='transient_before_password_proof')
        else:
            self.finish(job, result)

    def collect(self):
        for number, worker in self.workers.items():
            # empty() 有竞态但只用于快速跳过空闲队列；漏读的记录下一轮仍会被取到。
            if not worker['out'].empty():
                for _ in range(4096):
                    try:
                        value = worker['out'].get_nowait()
                    except queue.Empty:
                        break
                    if '_debug' in value:
                        self.log.debug(value['_debug'])
                    elif value['type'] == 'ready':
                        worker['ready'] = True
                        self.log('worker_ready', worker=number, worker_pid=value['pid'], threads=value['threads'])
                    else:
                        self.accept(value)
            if not worker['process'].is_alive() and not self.stop.is_set():
                self.request_stop('worker_exit_unknown_inflight')
        if any(v['deadline'] <= time.monotonic() for v in self.active.values()):
            self.request_stop('worker_deadline_unknown_inflight')

    def run(self):
        started = last_progress = time.monotonic()
        panel = StatusPanel()
        last_publish=[None]
        def publish(phase,final=False):
            now=time.monotonic()
            if not final and last_publish[0] is not None and now-last_publish[0]<1:return
            last_publish[0]=now
            counts=snapshot(len(self.accounts),self.results,len(self.active))
            devices=self.device_scheduler.snapshot() if self.pool else None
            if devices is not None:
                devices.update(target=self.settings.device_capacity,warmup=self.settings.warmup_devices,
                               restored=self.restored_devices,
                               registered_this_run=max(0,devices['total']-self.restored_devices))
            if self.console:panel.update(counts,now-started,phase,devices=devices,final=final)
            if self.progress:
                try:self.progress({'counts':counts,'devices':devices,
                                   'device_usage':self.device_scheduler.report() if self.pool else None,
                                   'elapsed_seconds':round(now-started,2),
                                   'phase':phase,'exports':self.exports.summary()})
                except Exception as exc:self.log('progress_error',error_type=type(exc).__name__)
        s = self.settings
        self.log('supervisor_start', protocol=s.protocol, accounts=len(self.accounts),
                 processes=s.processes, threads_per_process=s.threads,
                 thread_capacity=s.processes*s.threads, device_capacity=s.device_capacity if self.pool else 0,
                 proxy_ttl=s.proxy_ttl, debug=s.debug)
        try:
            if self.pool:
                for slot in range(1, s.device_capacity+1):
                    if self.pool.inspect(slot)['state'] == 'ready':
                        self.devices.add(slot)
                        self.device_scheduler.add(slot)
                self.restored_devices=len(self.devices)
                self.log('devices_restored',restored=self.restored_devices,target=s.device_capacity,
                         warmup=s.warmup_devices)
                if self.pool.health().get('provider_until', 0) > time.time():
                    self.device_scheduler.set_allowed(set())
                    self.request_stop('existing_provider_cooldown')
                publish('准备')
            if not self.stop.is_set():
                self.start_workers()
                self.proxies.start()
                if self.pool:
                    self.registration_thread = threading.Thread(target=self.prepare_devices,
                        name='device-supplier', daemon=True)
                    self.registration_thread.start()
                else:
                    self.registration_done.set()
            while (self.pending or self.active) and not self.stop.is_set():
                before = (len(self.results), len(self.active))
                self.collect()
                if self.stop.is_set():
                    break
                with self.resource_lock:
                    gate = not self.pool or len(self.devices) >= min(s.warmup_devices, len(self.accounts))
                if gate:
                    self.dispatch()
                elif self.registration_done.is_set():
                    self.request_stop('device_warmup_failed')
                self.proxies.set_target(self.proxy_demand())
                if before != (len(self.results), len(self.active)):
                    last_progress = time.monotonic()
                if time.monotonic()-last_progress > s.resource_timeout:
                    self.request_stop('resource_wait_timeout')
                publish('运行' if gate else '准备')
                # 30ms 是实测的平衡点：再降到 5ms 会让主进程被 dispatch 全量扫描
                # 拖满（实测速度从 48.8 万掉到 35.3 万），再高则会有派发空档。
                self.stop.wait(.03)
        except KeyboardInterrupt:
            self.request_stop('user_cancelled')
        except Exception as exc:
            self.log('supervisor_error', error_type=type(exc).__name__)
            self.request_stop('supervisor_error')
        finally:
            self.stop.set()
            self.proxies.wake()
            # A thread cannot be forcefully stopped on its own. Reap the process
            # before publishing unknown outcomes; never resubmit those tasks.
            for worker in self.workers.values():
                worker['process'].join(.5)
                self.reap(worker['process'], worker['out'])
                worker['in'].cancel_join_thread()
                worker['in'].close()
            for row, item in list(self.active.items()):
                job = item['job']
                expired = self.stop_at is not None and item['deadline'] <= self.stop_at
                unknown = {'status': 'unknown', 'kind': 'timeout' if expired else 'interrupted', 'stage': 'worker'}
                job.history.append(dict(unknown, attempt=job.attempt, worker=item['worker']))
                self.proxies.release(item['lease'], valid=False)
                self.finish(job, unknown)
            self.active.clear()
            while self.pending:
                job = self.pending.popleft()
                self.finish(job, {'status': 'stopped' if job.attempt else 'not_started',
                                  'kind': 'cancelled', 'stage': 'backoff' if job.attempt else 'queue'})
            if self.registration_thread:
                self.registration_thread.join(5)
            if self.proxies.thread.ident is not None:
                self.proxies.thread.join(16)
            for account in self.accounts:
                account.password = ''
            summary = {'protocol': s.protocol, 'accounts': len(self.accounts), 'processes': s.processes,
                       'threads_per_process': s.threads, 'thread_capacity': s.processes*s.threads,
                       'max_active': self.max_active, 'ready_devices': len(self.devices),
                       'initial_proxy_target': self.initial_proxy_target,
                       'device_usage': self.device_scheduler.report() if self.pool else {},
                       'device_pool': {'target':s.device_capacity,'warmup':s.warmup_devices,
                                       'restored':self.restored_devices,
                                       'registered_this_run':max(0,len(self.devices)-self.restored_devices)} if self.pool else None,
                       'exports':self.exports.summary(),'export_failed_rows':self.export_failures,
                       'results': dict(Counter(r['status'] for r in self.results.values())),
                       'display_counts': snapshot(len(self.accounts),self.results),
                       'stop_reason': self.stop_reason, 'elapsed_seconds': round(time.monotonic()-started, 2)}
            save_json(self.log.directory/'summary.json', summary)
            if s.debug:
                save_json(self.log.directory/'debug-summary.json', self.log.debug_summary())
            self.log('supervisor_done', **summary)
            publish('已停止' if self.stop_reason else '已完成',final=True)
        return summary
