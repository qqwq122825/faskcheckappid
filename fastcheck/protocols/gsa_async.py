"""Async GSA transport.

The SRP/plist verdict logic is identical to :mod:`gsa_status`, but sockets are
driven by aiohttp instead of dedicating one OS thread to every in-flight
account.  CPU-heavy SRP operations are sent to asyncio's small bounded executor.
"""
from __future__ import annotations

import asyncio
import plistlib
import time
from dataclasses import dataclass

from .apple_trust import gsa_context
from .srp_auth import GrandSlamSRPClient as SRPClient
from . import gsa_status as sync

try:
    import aiohttp
    from aiohttp_socks import ProxyConnector
except ImportError as exc:  # pragma: no cover - exercised by deployment check
    aiohttp = None
    ProxyConnector = None
    _IMPORT_ERROR = exc
else:
    _IMPORT_ERROR = None


GSA_URL = sync.GSA_URL
HEADERS = dict(sync._HEADERS)


def available() -> bool:
    return aiohttp is not None and ProxyConnector is not None


def require_available() -> None:
    if not available():
        raise RuntimeError(
            "async engine requires aiohttp and aiohttp-socks; "
            "run: pip install -r requirements.txt"
        ) from _IMPORT_ERROR


def _proxy_url(value: str) -> tuple[str, bool]:
    """aiohttp-socks spells remote-DNS SOCKS as socks5 + rdns=True."""
    if value.startswith("socks5h://"):
        return "socks5://" + value[len("socks5h://"):], True
    return value, value.startswith("socks")


@dataclass
class AsyncOutcome:
    payload: dict | None
    error: str = ""
    kind: str = ""
    http_status: int = 0


class AsyncGsaSession:
    """One coroutine-owned keep-alive pool for one short-lived proxy exit."""

    def __init__(self, proxy: str | None):
        require_available()
        self.proxy = proxy
        self._session = None

    async def open(self) -> "AsyncGsaSession":
        if self._session is not None:
            return self
        kwargs = dict(ssl=gsa_context(), limit=2, limit_per_host=2,
                      keepalive_timeout=20)
        if self.proxy:
            url, rdns = _proxy_url(self.proxy)
            connector = ProxyConnector.from_url(url, rdns=rdns, **kwargs)
        else:
            connector = aiohttp.TCPConnector(**kwargs)
        self._session = aiohttp.ClientSession(
            connector=connector, trust_env=False, raise_for_status=False)
        return self

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def __aenter__(self):
        return await self.open()

    async def __aexit__(self, *_):
        await self.close()

    async def post(self, body: bytes, cpd: dict, timeout: float,
                   *, client_info: str = "", user_agent: str | None = None) -> AsyncOutcome:
        await self.open()
        headers = dict(HEADERS)
        headers["X-MMe-Client-Info"] = client_info
        if user_agent:
            if len(user_agent) > 4096 or "\r" in user_agent or "\n" in user_agent:
                return AsyncOutcome(None, "Invalid provider user agent", "anisette")
            headers["User-Agent"] = user_agent
        total = max(1.0, float(timeout))
        client_timeout = aiohttp.ClientTimeout(
            total=total, connect=min(sync._CONNECT_TIMEOUT_CAP, total),
            sock_connect=min(sync._CONNECT_TIMEOUT_CAP, total), sock_read=total)
        try:
            async with self._session.post(
                    GSA_URL, data=body, headers=headers, timeout=client_timeout,
                    allow_redirects=False) as response:
                code = response.status
                # 边读边累计，超过 2 MiB 立即中断：先 read() 再判断挡不住内存
                limit = 2 * 1024 * 1024
                chunks, size = [], 0
                async for chunk in response.content.iter_chunked(64 * 1024):
                    size += len(chunk)
                    if size > limit:
                        return AsyncOutcome(None, "response exceeds 2 MiB",
                                            "protocol", code)
                    chunks.append(chunk)
                raw = b''.join(chunks)
        except Exception as exc:  # noqa: BLE001
            kind, detail = sync._classify_transport(exc, self.proxy, redactions=[
                cpd.get(key) for key in ("X-Apple-I-MD", "X-Apple-I-MD-M",
                                         "X-Apple-I-MD-LU", "X-Mme-Device-Id")])
            return AsyncOutcome(None, detail, kind)
        try:
            payload = plistlib.loads(raw)
        except Exception as exc:  # noqa: BLE001
            return AsyncOutcome(None, f"plist: {type(exc).__name__}", "protocol", code)
        if not isinstance(payload, dict):
            return AsyncOutcome(None, "plist root is not a dictionary", "protocol", code)
        if code < 200 or code >= 300:
            return AsyncOutcome(None, f"HTTP {code}", sync._error_kind(f"HTTP {code}"), code)
        return AsyncOutcome(payload, http_status=code)


def _result(status: str, detail: str, started: float, *, kind: str = "",
            stage: str = "", http: int | None = None) -> dict:
    result = {
        "status": status, "detail": detail,
        "ms": int((time.time() - started) * 1000),
        "kind": kind or ("" if status != "error" else sync._error_kind(detail)),
        "stage": stage,
    }
    # HTTP 0 is meaningful: the proxy/transport failed before any response.
    # Keeping it lets result_bucket classify the row as a timeout instead of
    # leaking it into the unrelated "undetermined/other" bucket.
    if http is not None:
        result["http"] = http
    return result


async def check(email: str, password: str, proxy: str | None,
                timeout: float = 15.0, retries: int = 0, *, identity: dict,
                session: AsyncGsaSession | None = None) -> dict:
    """Async equivalent of ``gsa_status.check`` using two non-blocking POSTs."""
    if sync.mock_enabled():
        await asyncio.sleep(0.01)
        value = sync.MOCK_RESULTS[sync._seed(email, "mock") % len(sync.MOCK_RESULTS)]
        return _result(value, "", time.time(), stage="mock")
    started = time.time()
    owned = session is None
    transport = session or AsyncGsaSession(proxy)
    await transport.open()
    last = None
    try:
        for attempt in range(max(0, retries) + 1):
            if attempt and last is not None and not sync.is_transient(
                    last.get("detail", ""), last.get("kind", "")):
                return last
            last = await _once(email, password, timeout, started, identity, transport)
            if last.get("status") != "error" or not sync.is_transient(
                    last.get("detail", ""), last.get("kind", "")):
                return last
        return last or _result("error", "no attempt made", started)
    finally:
        if owned:
            await transport.close()


async def _once(email: str, password: str, timeout: float, started: float,
                identity: dict, transport: AsyncGsaSession) -> dict:
    try:
        cpd = sync._build_cpd(identity, email)
        srp = await asyncio.to_thread(SRPClient, email, password)
    except Exception as exc:  # noqa: BLE001
        return _result("error", f"srp init: {type(exc).__name__}", started,
                       kind="protocol", stage="init")
    init_body = plistlib.dumps({
        "Header": {"Version": "1.0.1"},
        "Request": {"cpd": cpd, "u": email, "A2k": srp.A_bytes,
                    "ps": ["s2k", "s2k_fo"], "o": "init"},
    })
    init = await transport.post(
        init_body, cpd, timeout, client_info=sync._client_info(identity),
        user_agent=identity.get("User-Agent"))
    if init.payload is None:
        return _result("error", f"init {init.error}", started, kind=init.kind,
                       stage="init", http=init.http_status)
    response = init.payload.get("Response") or {}
    status = response.get("Status") or {}
    if status.get("ec") != 0:
        verdict, detail = sync.classify(status.get("ec"), status.get("em", "") or "")
        if verdict == "wrong_password":
            result = _result("error", "init did not accept password proof", started,
                             kind="protocol", stage="init", http=init.http_status)
        else:
            result = _result(verdict, detail, started, stage="init", http=init.http_status)
        return sync._with_apple_message(result, status, email, password,
                                        transport.proxy, identity)
    try:
        if "c" not in response:
            raise KeyError("c")
        m1 = await asyncio.to_thread(
            srp.process_challenge, response["s"], response["B"],
            response["i"], response["sp"])
    except Exception as exc:  # noqa: BLE001
        return _result("error", f"srp: {type(exc).__name__}", started,
                       kind="protocol", stage="init", http=init.http_status)
    done_body = plistlib.dumps({
        "Header": {"Version": "1.0.1"},
        "Request": {"cpd": cpd, "u": email, "M1": m1,
                    "c": response["c"], "o": "complete"},
    })
    done = await transport.post(
        done_body, cpd, timeout, client_info=sync._client_info(identity),
        user_agent=identity.get("User-Agent"))
    if done.payload is None:
        return _result("error", f"complete {done.error}", started, kind=done.kind,
                       stage="complete", http=done.http_status)
    final_response = done.payload.get("Response") or {}
    status2 = final_response.get("Status") or {}
    if status2.get("ec") in (-22025, -22420):
        result = _result("error", "authentication did not complete", started,
                         kind="protocol", stage="complete", http=done.http_status)
        return sync._with_apple_message(result, status2, email, password,
                                        transport.proxy, identity)
    verdict, detail = sync.classify(status2.get("ec"), status2.get("em", "") or "")
    if verdict == "valid":
        proof = final_response.get("M2")
        valid_proof = isinstance(proof, bytes) and await asyncio.to_thread(srp.verify_M2, proof)
        if not valid_proof:
            return _result("error", "missing or invalid GSA server proof", started,
                           kind="protocol", stage="complete", http=done.http_status)
        raw_auth_type = status2.get("au")
        auth_type = raw_auth_type.strip() if isinstance(raw_auth_type, str) else ""
        if auth_type:
            verdict, detail = "need_2fa", f"au={auth_type[:80]}"
        result = _result(verdict, detail, started, stage="complete", http=done.http_status)
        result.update(password_verified=True,
                      two_factor_status="required" if verdict == "need_2fa" else "unknown",
                      auth_type=auth_type[:80] if auth_type else "none")
        return result
    result = _result(verdict, detail, started, stage="complete", http=done.http_status)
    return sync._with_apple_message(result, status2, email, password,
                                    transport.proxy, identity)
