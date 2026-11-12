"""Small persistent device pool and bounded authentication retries.

Each slot owns a separate upstream StateStore. No account credentials are
persisted. The circuit-breaker thresholds are local policy, not Apple rules.
"""
from __future__ import annotations

import base64
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
import portalocker
import hashlib
import json
import os
from pathlib import Path
import random
import tempfile
import time
from urllib.parse import urlsplit
import uuid

import requests

from fastcheck.protocols import gsa_status as gsa
from fastcheck.protocols.anisette_v3 import AnisetteError, Client, StateStore


class PoolError(Exception):
    """Safe, credential-free error for display."""


def private_directory(path):
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    path.chmod(0o700)
    return path


def read_json(path, default=None):
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path, data):
    private_directory(path.parent)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                         prefix=".pool-", delete=False) as handle:
            temporary = Path(handle.name)
            json.dump(data, handle, ensure_ascii=False, separators=(",", ":"))
            handle.flush()
            os.fsync(handle.fileno())
        # Windows readers can briefly prevent replacing an open state file.
        for attempt in range(8):
            try:
                os.replace(temporary, path)
                break
            except PermissionError:
                if attempt == 7:raise
                time.sleep(min(.01 * (2 ** attempt), .1))
        path.chmod(0o600)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


@contextmanager
def file_lease(path, wait=0, *, shared=False):
    """Independent process-safe lease; never reuse upstream's state lock."""
    private_directory(path.parent)
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    with os.fdopen(descriptor, "a+") as handle:
        deadline = time.monotonic() + wait
        while True:
            try:
                portalocker.lock(handle, (portalocker.LOCK_SH if shared else portalocker.LOCK_EX) | portalocker.LOCK_NB)
                break
            except portalocker.exceptions.LockException:
                if time.monotonic() >= deadline:
                    raise PoolError("资源正在使用，请等待当前操作完成。") from None
                time.sleep(.05)
        try:
            yield
        finally:
            portalocker.unlock(handle)


class RegisteredStore(StateStore):
    """Authentication must never silently register a replacement identity."""

    def load(self):
        if not self.path.exists():
            raise PoolError("设备状态缺失，请先运行 prepare。")
        state = super().load()
        if not state.get("adi_pb"):
            raise PoolError("设备尚未完成注册，请先运行 prepare。")
        return state


@dataclass(frozen=True)
class RetryPolicy:
    attempts: int = 2
    device_failure_limit: int = 5
    device_cooldown: float = 600
    provider_device_limit: int = 3
    provider_window: float = 60
    provider_cooldown: float = 600
    backoff: float = 2


class DevicePool:
    def __init__(self, root, origin, *, client_factory=Client, policy=None,
                 clock=time.time, sleeper=time.sleep, max_devices=5):
        parsed = urlsplit(origin)
        if (not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment
                or any(ord(c) < 32 for c in origin)
                or (parsed.scheme != "https" and not (parsed.scheme == "http" and parsed.hostname in
                                                        ("127.0.0.1", "localhost", "::1")))):
            raise PoolError("设备服务地址必须是 HTTPS，或本机 HTTP，且不能包含凭据或查询参数。")
        self.origin = origin.rstrip("/")
        key = hashlib.sha256(self.origin.encode()).hexdigest()[:24]
        self.base = private_directory(private_directory(Path(root)) / key)
        self.client_factory = client_factory
        self.policy = policy or RetryPolicy()
        if not 1 <= self.policy.attempts <= 3:
            raise PoolError("认证最多允许 1–3 轮尝试。")
        self.clock, self.sleeper = clock, sleeper
        self.key = key
        self.timing_error = None
        self.max_devices = max_devices

    def slot_directory(self, slot):
        if type(slot) is not int or not 1 <= slot <= self.max_devices:
            raise PoolError(f"设备编号必须是 1–{self.max_devices}。")
        return self.base / "devices" / f"{slot:02d}"

    def store(self, slot, *, registered=False):
        factory = RegisteredStore if registered else StateStore
        return factory(self.origin, directory=self.slot_directory(slot))

    def inspect(self, slot):
        path = self.slot_directory(slot) / f"{self.key}.json"
        if not path.exists():
            return {"slot": slot, "state": "missing"}
        try:
            state = self.store(slot).load()
            identifier = base64.b64decode(state["identifier"], validate=True)
            return {"slot": slot, "state": "ready" if state.get("adi_pb") else "pending",
                    "device_id": str(uuid.UUID(bytes=identifier))}
        except (OSError, ValueError, KeyError, TypeError, AnisetteError):
            return {"slot": slot, "state": "invalid"}

    def health(self):
        with file_lease(self.base / ".health.lock", wait=2):
            return read_json(self.base / "health.json", {"devices": {}, "events": [], "provider_until": 0})

    def record_timing(self, operation, values):
        # Only callers' explicitly selected numeric/status fields are logged.
        try:
            with file_lease(self.base / ".measurements.lock", wait=2):
                path = self.base / "measurements.jsonl"
                descriptor = os.open(path, os.O_CREAT | os.O_WRONLY | os.O_APPEND, 0o600)
                with os.fdopen(descriptor, "a", encoding="utf-8") as handle:
                    handle.write(json.dumps(dict(values, operation=operation, at=self.clock()),
                                            ensure_ascii=False, separators=(",", ":")) + "\n")
        except (OSError, PoolError) as exc:
            # Timing is optional; a disk problem must not hide an auth verdict.
            self.timing_error = type(exc).__name__

    def allowed(self, slot):
        health = self.health()
        now = self.clock()
        provider_left = health.get("provider_until", 0) - now
        device = health["devices"].get(str(slot), {})
        device_left = device.get("until", 0) - now
        if provider_left > 0:
            raise PoolError(f"服务处于冷却期，剩余 {provider_left:.0f} 秒。")
        if device_left > 0:
            raise PoolError(f"设备 {slot} 处于冷却期，剩余 {device_left:.0f} 秒。")

    @contextmanager
    def operation(self, slot):
        with ExitStack() as stack:
            stack.enter_context(file_lease(self.slot_directory(slot) / ".lease.lock"))
            self.allowed(slot)
            until = self.health().get("provider_until", 0)
            if until and until <= self.clock():
                # One operation across all slots may test provider recovery.
                stack.enter_context(file_lease(self.base / ".provider-recovery.lock"))
                self.allowed(slot)
            yield

    def info(self, timeout=60, *, refresh=False, proxy=None, diagnostics=None, provider_direct=False):
        with file_lease(self.base / ".info.lock", wait=timeout):
            cached = read_json(self.base / "client-info.json")
            if not refresh and cached and cached.get("origin") == self.origin:
                info = cached.get("info")
                if self.valid_info(info):
                    return dict(info)
            # This creates a directory only; client_info() doesn't register a device.
            kwargs = {'proxy':proxy} if proxy is not None else {}
            if diagnostics is not None:
                kwargs['diagnostics'] = diagnostics
            if provider_direct:
                kwargs['provider_direct'] = True
            client = self.client_factory(self.origin, timeout=timeout,
                                         store=StateStore(self.origin, directory=self.base / "metadata"), **kwargs)
            try:
                info = client.client_info()
                if not self.valid_info(info):
                    raise PoolError("设备服务返回的客户端配置不完整。")
                info = {key: info[key] for key in ("client_info", "user_agent")}
                save_json(self.base / "client-info.json", {"origin": self.origin, "info": info})
                return info
            finally:
                client.close()

    @staticmethod
    def valid_info(info):
        return isinstance(info, dict) and all(isinstance(info.get(key), str)
            and 0 < len(info[key]) <= 4096 and "\r" not in info[key] and "\n" not in info[key]
            for key in ("client_info", "user_agent"))

    def fetch_headers(self, slot, info, timeout, *, registered=True, proxy=None, diagnostics=None, provider_direct=False):
        kwargs = {'proxy':proxy} if proxy is not None else {}
        if diagnostics is not None:
            kwargs['diagnostics'] = diagnostics
        if provider_direct:
            kwargs['provider_direct'] = True
        client = self.client_factory(self.origin, timeout=timeout,
            store=self.store(slot, registered=registered), client_info=dict(info), **kwargs)
        try:
            headers = client.headers()
            gsa._build_cpd(headers, "")
            return headers
        finally:
            client.close()

    def prepare(self, count=5, *, timeout=60, gap=3, refresh_info=False, event=None):
        self.slot_directory(count)
        records, info = [], None
        if refresh_info:
            info = self.info(timeout, refresh=True)
        registered = 0
        for slot in range(1, count + 1):
            with file_lease(self.slot_directory(slot) / ".lease.lock"):
                self.allowed(slot)
                current = self.inspect(slot)
                if current["state"] == "ready":
                    record = {"slot": slot, "action": "reused", "ms": 0}
                else:
                    if current["state"] == "invalid":
                        raise PoolError(f"设备 {slot} 状态损坏，已保留原文件，需人工排查。")
                    if registered:
                        self.sleeper(gap)
                    if info is None:
                        info = self.info(timeout)
                    if event:
                        event({"slot": slot, "action": "registering"})
                    started = time.perf_counter()
                    self.fetch_headers(slot, info, timeout, registered=False)
                    record = {"slot": slot, "action": "registered",
                              "ms": round((time.perf_counter() - started) * 1000, 2)}
                    registered += 1
                records.append(record)
                self.record_timing("prepare", record)
                if event:
                    event(record)
        return records

    def refresh(self, slot, *, timeout=60):
        with self.operation(slot):
            if self.inspect(slot)["state"] != "ready":
                raise PoolError(f"设备 {slot} 尚未就绪，请先运行 prepare。")
            started = time.perf_counter()
            info = self.info(timeout)
            local_ms = (time.perf_counter() - started) * 1000
            started = time.perf_counter()
            self.fetch_headers(slot, info, timeout)
            result = {"slot": slot, "load_ms": round(local_ms, 2),
                      "refresh_ms": round((time.perf_counter() - started) * 1000, 2)}
            self.record_timing("refresh", result)
            return result

    def observe(self, slot, result):
        """Track only init/-20970. Other outcomes are not proof of a bad device."""
        with file_lease(self.base / ".health.lock", wait=2):
            path = self.base / "health.json"
            health = read_json(path, {"devices": {}, "events": [], "provider_until": 0})
            now, policy = self.clock(), self.policy
            device = health["devices"].setdefault(str(slot), {"failures": 0, "until": 0})
            events = [item for item in health["events"] if now - policy.provider_window <= item[0] <= now]
            if result.get("stage") == "init" and result.get("apple_ec") == -20970:
                device["failures"] += 1
                events.append([now, slot])
                if device["failures"] >= policy.device_failure_limit:
                    device["until"] = now + policy.device_cooldown
                if (len({item[1] for item in events}) >= policy.provider_device_limit
                        or 0 < health.get("provider_until", 0) <= now):
                    health["provider_until"] = now + policy.provider_cooldown
            elif result.get("stage") == "complete":
                # Reaching complete establishes that init was accepted. A bad
                # password/2FA outcome is handled separately by the caller.
                device.update(failures=0, until=0)
                events = [item for item in events if item[1] != slot]
                if health.get("provider_until", 0) <= now:
                    health["provider_until"] = 0
            health["events"] = events
            save_json(path, health)

    def authenticate(self, slot, email, password, *, timeout=25, prepare_timeout=60, event=None,
                     proxy=None, stop_event=None, diagnostics=None, provider_direct=False):
        if gsa.mock_enabled():
            raise PoolError("FASTCHECK_MOCK=1；已拒绝执行真实认证入口。")
        started_total = time.perf_counter()
        history, waits_ms = [], 0.0
        with self.operation(slot):
            if self.inspect(slot)["state"] != "ready":
                raise PoolError(f"设备 {slot} 尚未就绪，请先运行 prepare。")
            started = time.perf_counter()
            info = self.info(prepare_timeout, proxy=proxy, diagnostics=diagnostics, provider_direct=provider_direct)
            load_ms = (time.perf_counter() - started) * 1000
            health = self.health()["devices"].get(str(slot), {})
            # After an expired device cooldown, allow just one recovery attempt.
            recovering = health.get("failures", 0) >= self.policy.device_failure_limit
            attempts = 1 if recovering else self.policy.attempts
            for attempt in range(1, attempts + 1):
                if stop_event is not None and stop_event.is_set():
                    raise PoolError("任务已停止。")
                try:
                    self.allowed(slot)
                except PoolError as exc:
                    if not history:
                        raise
                    result = dict(result, retry_stopped_reason=str(exc))
                    break
                if event:
                    event({"event": "attempt", "attempt": attempt})
                refresh_ms = auth_ms = 0.0
                phase = "refresh"
                mark = time.perf_counter()
                gsa.close_session()
                try:
                    headers = self.fetch_headers(slot, info, prepare_timeout, proxy=proxy, diagnostics=diagnostics,
                                                 provider_direct=provider_direct)
                    refresh_ms = (time.perf_counter() - mark) * 1000
                    self.allowed(slot)
                    phase, mark = "auth", time.perf_counter()
                    kwargs = {'cancel':stop_event.is_set} if stop_event is not None else {}
                    if diagnostics is not None:
                        kwargs['diagnostics'] = diagnostics
                    result = gsa.check(email, password, proxy=proxy, timeout=timeout,
                                       retries=0, identity=headers, **kwargs)
                    auth_ms = (time.perf_counter() - mark) * 1000
                except AnisetteError as exc:
                    kind = "ratelimit" if exc.http == 429 else "server" if exc.http >= 500 else "anisette"
                    result = {"status": "error", "kind": kind, "stage": "anisette_refresh",
                              "detail": str(exc)}
                except PoolError as exc:
                    result = {"status": "error", "kind": "pool", "stage": "queue", "detail": str(exc)}
                except requests.RequestException as exc:
                    result = {"status": "error", "kind": "network", "stage": "anisette_refresh",
                              "detail": type(exc).__name__}
                finally:
                    elapsed = (time.perf_counter() - mark) * 1000
                    if phase == "refresh":
                        refresh_ms = elapsed
                    else:
                        auth_ms = elapsed
                    gsa.close_session()
                self.observe(slot, result)
                sample = {"attempt": attempt, "refresh_ms": round(refresh_ms, 2),
                          "auth_ms": round(auth_ms, 2), "stage": result.get("stage"),
                          "status": result.get("status"), "apple_ec": result.get("apple_ec")}
                history.append(sample)
                if event:
                    event(dict(sample, event="attempt_result"))
                init_refused = result.get("stage") == "init" and result.get("apple_ec") == -20970
                transient = result.get("status") == "error" and result.get("kind") in ("network", "server", "tls")
                if attempt >= attempts or not (init_refused or transient):
                    break
                try:
                    self.allowed(slot)
                except PoolError as exc:
                    result = dict(result, retry_stopped_reason=str(exc))
                    break
                delay = self.policy.backoff * 2 ** (attempt - 1) + random.uniform(0, .5)
                if event:
                    event({"event": "backoff", "seconds": round(delay, 2)})
                mark_wait = time.perf_counter()
                self.sleeper(delay)
                waits_ms += (time.perf_counter() - mark_wait) * 1000
        result = dict(result, attempts=len(history), device=slot, attempt_history=history,
                      load_ms=round(load_ms, 2), backoff_ms=round(waits_ms, 2),
                      total_ms=round((time.perf_counter() - started_total) * 1000, 2))
        self.record_timing("auth", {key: result[key] for key in
            ("device", "attempts", "attempt_history", "load_ms", "backoff_ms", "total_ms", "status")})
        return result
