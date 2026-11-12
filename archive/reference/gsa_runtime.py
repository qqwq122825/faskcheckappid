"""GSA account jobs isolated in killable processes with a total time budget.

The parent alone publishes final results. A timed-out child is stopped before
its unknown result is published, so authentication does not continue unseen.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import queue
import signal
import threading
import time

from .runtime import EngineSession


def _ignore_console_signals():
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, signal.SIG_IGN)
    from .engine import _watch_parent
    threading.Thread(target=_watch_parent, daemon=True, name="gsa-parent-watch").start()


def _prepare_identity(pipe, selected_url, timeout, require_dns=False, anisette_version=3,https_dns=True):
    _ignore_console_signals()
    from . import gsa_status as gsa
    if anisette_version==3:
        from .anisette_v3 import Client,AnisetteError
        from .timing import exception_metadata
        from .gsa_dns import resolve_host,CachedResolver
        from urllib.parse import urlsplit
        origin=selected_url or next(u for u in gsa.ANISETTE_SERVERS if u.startswith('https://'))
        client=None
        stage='provider_dns'
        try:
            parsed=urlsplit(origin)
            provider_dns=resolve_host(parsed.hostname,parsed.port or (443 if parsed.scheme=='https' else 80),https_fallback=https_dns)
            stage='identity'
            client=Client(origin,timeout=timeout,resolver=CachedResolver(provider_dns))
            identity=client.headers()
            identity['_fastcheck_provider_dns']=provider_dns
            gsa._build_cpd(identity,'')
            pipe.send({'progress':'identity_ready','provider':0})
            snapshot=None
            if require_dns:
                stage='dns'
                from .gsa_dns import resolve_target
                snapshot=resolve_target(https_fallback=https_dns)
                pipe.send({'progress':'dns_ready'})
            pipe.send({'identity':identity,'dns_snapshot':snapshot,'provider':0,'state':'ready'})
        except Exception as exc:
            failure={'progress':'failed','provider':-1 if stage=='dns' else 0,**exception_metadata(exc)}
            if isinstance(exc,AnisetteError):
                failure.update(http=exc.http,code=exc.code,stage=exc.stage)
            pipe.send(failure)
            state='ratelimit' if failure.get('http')==429 else 'dns_unavailable' if stage=='dns' else 'unavailable'
            pipe.send({'identity':None,'state':state})
        finally:
            if client is not None:
                client.close()
            pipe.close()
        return
    urls = [selected_url] if selected_url else [u for u in gsa.ANISETTE_SERVERS if u.startswith("https://")]
    pool = ThreadPoolExecutor(max_workers=min(4, len(urls)) + int(require_dns))
    try:
        futures = {pool.submit(gsa._fetch_anisette, url, timeout): index for index,url in enumerate(urls)}
        dns_job = None
        if require_dns:
            from .gsa_dns import resolve_target
            dns_job = pool.submit(resolve_target,https_fallback=https_dns)
            futures[dns_job] = -1
        selected = snapshot = None
        selected_provider = None
        for future in as_completed(futures):
            try:
                if future is dns_job:
                    snapshot = future.result()
                    pipe.send({"progress": "dns_ready"})
                else:
                    identity = future.result()
                    if not isinstance(identity, dict) or not all(isinstance(identity.get(k), str) and identity[k]
                            for k in ("X-Apple-I-MD", "X-Apple-I-MD-M")):
                        raise ValueError("Incomplete identity")
                    candidate = {k: v[:4096] for k, v in identity.items()
                                 if isinstance(k,str) and k.startswith("X-") and isinstance(v,str)}
                    if type(identity.get("X-Apple-I-MD-RINFO")) is int:
                        candidate["X-Apple-I-MD-RINFO"] = str(identity["X-Apple-I-MD-RINFO"])
                    gsa._build_cpd(candidate, "")
                    if selected is None:
                        selected = candidate
                        selected_provider = futures[future]
                    pipe.send({"progress": "identity_ready", "provider": futures[future]})
                if selected is not None and (snapshot is not None or not require_dns):
                    pipe.send({"identity": selected, "dns_snapshot": snapshot, "provider": selected_provider, "state": "ready"})
                    return
            except Exception as exc:
                from .timing import exception_metadata
                pipe.send({"progress": "failed", "provider": futures[future], **exception_metadata(exc)})
                continue
        state = "dns_unavailable" if selected is not None and require_dns and snapshot is None else "unavailable"
        pipe.send({"identity": None, "state": state})
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
        pipe.close()


def _gsa_job(pipe, job, identity, proxy_q, static_proxy, timeout, diagnostics, dns_snapshot=None, refresh_identity=False):
    _ignore_console_signals()
    from . import gsa_status as gsa
    if dns_snapshot:
        from .gsa_dns import CachedResolver
        gsa._TLS.resolver = CachedResolver(dns_snapshot)
    started = time.monotonic()
    wait_ms = request_ms = 0
    attempts = 0
    def event(value):
        pipe.send({"event": "diagnostic", "diagnostic": dict(value, attempt=1, pid=os.getpid())})
    try:
        if refresh_identity and identity and identity.get('_fastcheck_v3_origin'):
            from .anisette_v3 import Client
            from .timing import phase
            pipe.send({'event':'progress','result':{'status':'checking','stage':'anisette_refresh'}})
            from .gsa_dns import CachedResolver
            provider_dns=identity.get('_fastcheck_provider_dns')
            info={'client_info':gsa._client_info(identity),'user_agent':identity['User-Agent']}
            client=Client(identity['_fastcheck_v3_origin'],timeout=timeout,
                          resolver=CachedResolver(provider_dns) if provider_dns else None,client_info=info)
            try:
                with phase(event if diagnostics else None,'anisette_refresh'):
                    identity=client.headers()
            finally:
                client.close()
        with gsa._ANI_LOCK:
            gsa._ANI_LIVE[:] = [identity] if identity else []
        address = static_proxy
        if proxy_q is not None:
            pipe.send({"event": "progress", "result": {"status": "checking", "stage": "proxy"}})
            wait_started = time.monotonic()
            try:
                address = proxy_q.get(timeout=timeout)
            except queue.Empty:
                address = None
            wait_ms = int((time.monotonic() - wait_started) * 1000)
            if not address:
                raise TimeoutError()
        attempts = 1
        pipe.send({"event": "progress", "result": {"status": "checking", "stage": "check"}})
        request_started = time.monotonic()
        result = gsa.check(job[1], job[2], address, timeout, 0,
                           diagnostics=event if diagnostics else None)
        request_ms = int((time.monotonic() - request_started) * 1000)
    except TimeoutError:
        result = {"status": "error", "kind": "timeout", "stage": "proxy", "detail": "等待代理超时，未验证账号密码"}
    except Exception as exc:
        from .anisette_v3 import AnisetteError
        if isinstance(exc,AnisetteError):
            result={"status":"error","kind":"ratelimit" if exc.http==429 else "anisette",
                    "stage":"anisette_refresh","detail":str(exc)}
        else:
            result = {"status": "error", "kind": "internal", "stage": "check", "detail": type(exc).__name__}
    result.update(attempts=attempts, retry_count=0, max_retries=0, proxy_wait_ms=wait_ms,
                  request_ms=request_ms, backoff_ms=0, attempt_history=[])
    try:
        pipe.send({"event": "result", "result": result})
    finally:
        pipe.close()


class GsaSession(EngineSession):
    supports_retries = False

    def __init__(self, *args, anisette_url=None, prepare_timeout=20, anisette_version=3,system_dns=False,prepare_dns=False, **kwargs):
        super().__init__(*args, **kwargs)
        if self.retries != 0 and not self.supports_retries:
            raise ValueError("GSA bounded jobs do not retry")
        self.anisette_url = anisette_url
        self.anisette_version = anisette_version
        self.https_dns = not system_dns
        self.anisette_provider_index = None
        self.prepare_timeout = prepare_timeout
        self.prepare_state = "pending"
        self.prepare_failures = []
        self.dns_snapshot = None
        self.require_dns = (bool(self.jobs) or prepare_dns) and self.proxy is None and self.proxy_q is None
        self.rate_limited = False
        self.children = []

    @staticmethod
    def _halt(process):
        if process.is_alive():
            process.terminate()
        process.join(0.25)
        if process.is_alive():
            process.kill()
            process.join(0.25)
        if process.is_alive():
            raise RuntimeError("GSA worker cleanup failed")

    def _prepare(self):
        receive, send = self.ctx.Pipe(duplex=False)
        process = self.ctx.Process(target=_prepare_identity,
                                   args=(send, self.anisette_url, self.prepare_timeout, self.require_dns,self.anisette_version,self.https_dns), daemon=True)
        self.processes.append(process)
        until = time.monotonic() + self.prepare_timeout
        self.prepare_state = "timeout"
        try:
            process.start()
            send.close()
            while time.monotonic() < until and not self.stop_requested.is_set():
                if receive.poll(min(0.05, max(0, until - time.monotonic()))):
                    response = receive.recv()
                    if response.get("progress"):
                        if response["progress"] == "failed":
                            self.prepare_failures.append(response)
                        elif response["progress"] == "identity_ready" and self.require_dns:
                            self.prepare_state = "target_dns_timeout"
                        continue
                    self.prepare_state = response["state"]
                    self.anisette_provider_index = response.get("provider")
                    self.dns_snapshot = response.get("dns_snapshot")
                    return response["identity"]
                if not process.is_alive():
                    self.prepare_state = "worker_exit"
                    break
        except (EOFError, OSError):
            self.prepare_state = "worker_exit"
        finally:
            if process.pid is not None:
                self._halt(process)
            receive.close()
            send.close()
        return None

    def _run(self):
        from . import gsa_status as gsa
        started = time.monotonic()
        jobs = list(self.jobs)
        self.jobs = []
        active = []
        try:
            mocked = gsa.mock_enabled()
            identity = None if mocked else self._prepare()
            if mocked:
                self.prepare_state = "mock"
            self.startup_ms = int((time.monotonic() - started) * 1000)
            self.ready.set()
            if not mocked and identity is None:
                dns_failed = self.prepare_state in ("dns_unavailable", "target_dns_timeout")
                for row, _, _ in jobs:
                    self.out_q.put({"event": "result", "row": row, "result": {
                        "status": "error", "kind": "ratelimit" if self.prepare_state=='ratelimit' else "network" if dns_failed else "anisette",
                        "stage": "prepare_dns" if dns_failed else "prepare", "ms": 0,
                        "detail": ("设备参数服务返回 HTTP 429，未提交账号验证" if self.prepare_state=='ratelimit' else
                                   "目标域名 DNS 未在准备预算内就绪，未提交账号验证" if dns_failed else
                                   f"Anisette 认证参数准备超时（{self.prepare_timeout} 秒），未提交账号验证"
                                   if self.prepare_state == "timeout" else "Anisette 认证参数不可用，未提交账号验证"),
                        "prepare_state": self.prepare_state, "prepare_ms": self.startup_ms,
                        "prepare_failures": list(self.prepare_failures), "attempts": 0,
                        "retry_count": 0, "max_retries": 0}})
                jobs.clear()
            capacity = max(1, self.count)
            launched = 0
            while (jobs or active) and not self.stop_requested.is_set():
                while jobs and len(active) < capacity and not self.stop_requested.is_set():
                    job = jobs.pop(0)
                    launch_at = time.monotonic()
                    active.append({"job": job, "process": None, "pipe": None,
                                   "started": launch_at, "stage": "proxy" if self.proxy_q is not None else "startup",
                                   "proxy_wait_ms": 0})
                    self.out_q.put({"event": "progress", "row": job[0], "result": {
                        "status": "checking", "stage": "startup", "detail": "GSA 独立校验进程启动中"}})
                for task in list(active):
                    done = None
                    if task["process"] is None and time.monotonic() - task["started"] < self.timeout:
                        address = self.proxy
                        proxy_ready = self.proxy_q is None
                        if self.proxy_q is not None:
                            # Only the persistent parent reads this shared queue.
                            # Killing an account worker must not strand its lock.
                            try:
                                address = self.proxy_q.get_nowait()
                                proxy_ready = True
                            except queue.Empty:
                                pass
                        if proxy_ready:
                            if self.proxy_q is not None:
                                task["proxy_wait_ms"] = int((time.monotonic() - task["started"]) * 1000)
                            receive, send = self.ctx.Pipe(duplex=False)
                            process = self.ctx.Process(target=_gsa_job, args=(send, task["job"], identity, None,
                                address, self.timeout, self.diagnostics, self.dns_snapshot,launched>0), daemon=True)
                            task.update(process=process, pipe=receive, stage="startup")
                            process.start()
                            launched += 1
                            self.processes.append(process)
                            send.close()
                    channel = task["pipe"]
                    for _ in range(100):
                        if channel is None or not channel.poll():
                            break
                        try:
                            message = channel.recv()
                        except (EOFError, OSError):
                            break
                        if message["event"] == "result":
                            done = message["result"]
                            break
                        if message["event"] == "progress":
                            task["stage"] = message["result"]["stage"]
                        elif message["event"] == "diagnostic" and message["diagnostic"]["event"] == "phase_start":
                            task["stage"] = message["diagnostic"]["phase"]
                        self.out_q.put(dict(message, row=task["job"][0]))
                    elapsed = time.monotonic() - task["started"]
                    if done is None and elapsed >= self.timeout:
                        done = {"status": "error", "kind": "timeout", "stage": task["stage"],
                                "detail": f"超过 {self.timeout} 秒总预算，未验证账号密码", "attempts": 1,
                                "retry_count": 0, "max_retries": 0, "deadline_exceeded": True}
                    if done is None and task["process"] is not None and not task["process"].is_alive():
                        done = {"status": "error", "kind": "internal", "stage": task["stage"],
                                "detail": "GSA 子进程未返回有效结果", "attempts": 1,
                                "retry_count": 0, "max_retries": 0}
                    if done is not None:
                        if task["process"] is not None:
                            self._halt(task["process"])
                        if channel is not None:
                            channel.close()
                        done["ms"] = int((time.monotonic() - task["started"]) * 1000)
                        done["proxy_wait_ms"] = task["proxy_wait_ms"] if task["process"] is not None else done["ms"]
                        self.out_q.put({"event": "result", "row": task["job"][0], "result": done})
                        active.remove(task)
                        if done.get('kind')=='ratelimit':
                            self.rate_limited=True
                            jobs.clear()
                            for pending in active:
                                if pending['process'] is not None:
                                    self._halt(pending['process'])
                                if pending['pipe'] is not None:
                                    pending['pipe'].close()
                            active.clear()
                            self.out_q.put({'event':'rate_limit_stop','row':task['job'][0]})
                            break
                self.stop_requested.wait(0.01)
            self.stop_requested.wait()
        except Exception as exc:
            self.error = type(exc).__name__
        finally:
            self.ready.set()
            try:
                for task in active:
                    if task["process"] is not None and task["process"].pid is not None:
                        self._halt(task["process"])
                    if task["pipe"] is not None:
                        task["pipe"].close()
                for process in self.processes:
                    if process.pid is not None and process.is_alive():
                        self._halt(process)
            finally:
                for channel in (self.in_q, self.out_q, self.proxy_q):
                    if channel is not None:
                        channel.cancel_join_thread()
                        channel.close()
                self.closed.set()


def _cli_check(email, password, proxy, timeout, retries, cancel=None, diagnostics=None,
               *, identity=None, dns_snapshot=None):
    """Refresh metadata per attempt without sharing mutable identities between threads."""
    from . import gsa_status as gsa
    from .anisette_v3 import AnisetteError, Client
    from .gsa_dns import CachedResolver
    from .timing import phase

    if cancel and cancel():
        return {"status": "stopped", "stage": "check", "detail": ""}
    gsa._TLS.resolver = CachedResolver(dns_snapshot) if dns_snapshot else None
    current = identity
    if not gsa.mock_enabled() and identity and identity.get("_fastcheck_v3_origin"):
        provider_dns = identity.get("_fastcheck_provider_dns")
        client = Client(identity["_fastcheck_v3_origin"], timeout=timeout,
                        resolver=CachedResolver(provider_dns) if provider_dns else None,
                        client_info={"client_info": gsa._client_info(identity),
                                     "user_agent": identity["User-Agent"]})
        try:
            with phase(diagnostics, "anisette_refresh"):
                current = client.headers()
        except AnisetteError as exc:
            return {"status": "error", "kind": "ratelimit" if exc.http == 429 else "anisette",
                    "stage": "anisette_refresh", "detail": str(exc)}
        finally:
            client.close()
    try:
        return gsa.check(email, password, proxy, timeout, retries, cancel=cancel,
                         diagnostics=diagnostics, identity=current)
    finally:
        gsa.close_session()


def _cli_engine_main(in_q, out_q, stop_ev, pause_ev, proxy, timeout, retries,
                     threads, proxy_q, diagnostics, identity, dns_snapshot, record_proxy=False,
                     observe_egress=False, notify_proxy_requests=False):
    from functools import partial
    from .engine import engine_main

    engine_main(in_q, out_q, stop_ev, pause_ev, proxy, timeout, retries, threads,
                "gsa_cli", proxy_q, diagnostics,
                partial(_cli_check, identity=identity, dns_snapshot=dns_snapshot), record_proxy, observe_egress,
                notify_proxy_requests)


class GsaCliSession(GsaSession):
    """GSA preparation with the same process/thread and retry model as the web CLI."""
    supports_retries = True
    _run = EngineSession._run

    def _prepare_workers(self):
        from . import gsa_status as gsa

        mocked = gsa.mock_enabled()
        self.identity = None if mocked else self._prepare()
        if mocked:
            self.prepare_state = "mock"
        if mocked or self.identity is not None or self.stop_requested.is_set():
            return
        dns_failed = self.prepare_state in ("dns_unavailable", "target_dns_timeout")
        limited = self.prepare_state == "ratelimit"
        for row, _, _ in self.jobs:
            self.out_q.put({"event": "result", "row": row, "result": {
                "status": "error", "kind": "ratelimit" if limited else "network" if dns_failed else "anisette",
                "stage": "prepare_dns" if dns_failed else "prepare", "ms": 0,
                "detail": f"GSA 认证环境准备失败（{self.prepare_state}），未提交账号验证",
                "prepare_state": self.prepare_state, "prepare_failures": list(self.prepare_failures),
                "attempts": 0, "retry_count": 0, "max_retries": self.retries}})
        self.jobs = []
        self.count = 0

    def _create_process(self, index, started):
        return self.ctx.Process(target=_cli_engine_main,
            args=(self.in_q, self.out_q, self.stop_event, self.pause_event,
                  self.proxy, self.timeout, self.retries, self.threads[index], self.proxy_q,
                  {"run_started": started, "performance_debug": self.performance_debug} if self.diagnostics else None,
                  self.identity, self.dns_snapshot),
            kwargs={key: True for key in ("record_proxy", "observe_egress", "notify_proxy_requests") if getattr(self, key)}, daemon=True)
