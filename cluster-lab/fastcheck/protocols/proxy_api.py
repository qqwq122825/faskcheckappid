"""代理提取 API：拉取、解析、归一化。

代理商的"提取链接"返回格式五花八门，这里统一成 `http://[用户:密码@]主机:端口`。
支持（实测覆盖常见几家）：

    纯文本     1.2.3.4:8080            每行一个
               1.2.3.4:8080:user:pass   带凭据的冒号格式
               user:pass@1.2.3.4:8080
    JSON 列表  [{"ip":"1.2.3.4","port":8080,"username":"u","password":"p"}]
    JSON 包装  {"data":[...]} / {"proxies":[...]} / {"result":{"proxy_list":[...]}}
    混合       [{"proxy":"1.2.3.4:8080"}] / {"server":"1.2.3.4:8080"}
"""
from __future__ import annotations

import ipaddress
import json
import queue
import re
import ssl
import time
import threading
from concurrent.futures import Future, wait, FIRST_COMPLETED
from urllib.parse import urlsplit, urlunsplit
import urllib.error
import urllib.request

_IP_PORT = re.compile(r"(?<![\d.])(\d{1,3}(?:\.\d{1,3}){3}):(\d{2,5})(?![\d])")
_CRED_HOST_PORT = re.compile(r"([^:\s@]+):([^:\s@]+)@(\d{1,3}(?:\.\d{1,3}){3}):(\d{2,5})")
_HOST_PORT_CRED = re.compile(r"(\d{1,3}(?:\.\d{1,3}){3}):(\d{2,5}):([^:\s]+):([^:\s]+)")
_HOSTNAME_PORT = re.compile(r"(?<![\w.-])([a-zA-Z0-9][a-zA-Z0-9.\-]{3,}):(\d{2,5})(?![\d])")

_SCHEME_DEFAULT = "http"
# 提取链接返回 ip:port 时允许保留的 scheme；socks5/socks5h 由任务配置的
# proxy_scheme 显式指定，不靠供应商返回。
ALLOWED_SCHEMES = ("http", "https", "socks4", "socks5", "socks5h")


def _batch_parts(url: str):
    parts = urlsplit(url)
    query = parts.query.split("&")
    indices = [index for index, item in enumerate(query) if item.partition("=")[0] == "num"]
    if not indices:
        return parts, query, None, None
    if len(indices) != 1:
        raise ValueError("代理 API 的 num 参数不能重复")
    index = indices[0]
    value = query[index].partition("=")[2]
    if not value.isascii() or not value.isdecimal() or int(value) < 1:
        raise ValueError("代理 API 的 num 必须为正整数")
    return parts, query, index, int(value)


def batch_limit(url: str) -> int | None:
    return _batch_parts(url)[3]


def demand_url(url: str, count: int) -> str:
    """Set num to the exact free-slot demand, preserving other query bytes."""
    if type(count) is not int or count < 1:
        raise ValueError("代理提取数量必须为正整数")
    parts, query, index, _ = _batch_parts(url)
    if index is None:
        query = [item for item in query if item]
        query.append(f"num={count}")
    else:
        query[index] = f"num={count}"
    return urlunsplit(parts._replace(query="&".join(query)))


def batch_url(url: str, remaining: int, available: int, target_size=None) -> str:
    """Treat num as a supplier limit; optionally cap inventory to current demand."""
    parts, query, index, limit = _batch_parts(url)
    if index is None:
        return url
    target = remaining if target_size is None else min(remaining, target_size)
    needed = max(1, target - max(0, available))
    query[index] = "num=" + str(min(limit, needed))
    return urlunsplit(parts._replace(query="&".join(query)))


def _valid_ip(text: str) -> bool:
    try:
        ipaddress.ip_address(text)
        return True
    except ValueError:
        return False


def normalize(raw: str) -> str | None:
    """任意形态 -> scheme://[user:pass@]host:port；无法识别返回 None。"""
    text = (raw or "").strip()
    if not text:
        return None
    if "://" not in text:
        text = f"{_SCHEME_DEFAULT}://{text}"
    scheme, rest = text.split("://", 1)
    rest = rest.lstrip("/")                       # 处理 "http:////u:p@1.2.3.4:1"
    if scheme not in ("http", "https", "socks4", "socks5", "socks5h"):
        scheme = _SCHEME_DEFAULT
    auth = ""
    if "@" in rest:
        auth, rest = rest.rsplit("@", 1)
        auth = auth + "@"
    if ":" not in rest:
        return None
    host, port = rest.rsplit(":", 1)
    host = host.strip("[]")
    if not port.isdigit() or not (0 < int(port) < 65536):
        return None
    if not (_valid_ip(host) or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.\-]+\.[A-Za-z]{2,}", host)):
        return None
    return f"{scheme}://{auth}{host}:{int(port)}"


def _from_mapping(obj: dict) -> list[str]:
    out: list[str] = []
    ip = obj.get("ip") or obj.get("host") or obj.get("hostname") or obj.get("server")
    port = obj.get("port") or obj.get("server_port")
    user = obj.get("username") or obj.get("user") or obj.get("account")
    password = obj.get("password") or obj.get("pass") or obj.get("pwd")
    if ip and port:
        auth = f"{user}:{password}@" if user and password else ""
        candidate = normalize(f"{auth}{str(ip).strip()}:{int(port)}")
        if candidate:
            out.append(candidate)
    elif isinstance(ip, str) and ":" in ip:
        candidate = normalize(ip)
        if candidate:
            out.append(candidate)
    return out


def _walk(node, out: list[str], depth: int = 0) -> None:
    if depth > 8:
        return
    if isinstance(node, dict):
        found = _from_mapping(node)
        if found:
            out.extend(found)
            return
        for value in node.values():
            _walk(value, out, depth + 1)
    elif isinstance(node, list):
        for item in node:
            _walk(item, out, depth + 1)
    elif isinstance(node, str):
        out.extend(extract_from_text(node))


def extract_from_text(text: str) -> list[str]:
    """从任意文本里抠出代理（覆盖纯文本接口与夹在日志里的情况）。

    同一个 主机:端口 只保留一条；带凭据的版本优先于不带凭据的。
    """
    out: list[str] = []
    keys: dict[str, int] = {}

    def add(candidate: str | None) -> None:
        if not candidate:
            return
        body = candidate.split("://", 1)[-1].rsplit("@", 1)[-1]
        has_auth = "@" in candidate
        if body in keys:
            if has_auth and "@" not in out[keys[body]]:
                out[keys[body]] = candidate
            return
        keys[body] = len(out)
        out.append(candidate)

    for match in _CRED_HOST_PORT.finditer(text):
        user, pwd, ip, port = match.groups()
        add(normalize(f"{user}:{pwd}@{ip}:{port}"))
    for match in _HOST_PORT_CRED.finditer(text):
        ip, port, user, pwd = match.groups()
        add(normalize(f"{user}:{pwd}@{ip}:{port}"))
    for line in text.replace(",", "\n").splitlines():
        stripped = line.strip().strip('"\'')
        if not stripped:
            continue
        for match in _IP_PORT.finditer(stripped):
            add(normalize(match.group(0)))
    return out


def parse(payload: str) -> list[str]:
    """解析 API 返回体：先按 JSON 试，失败再按文本抠。去重保序。"""
    text = (payload or "").strip()
    if not text:
        return []
    candidates: list[str] = []
    try:
        data = json.loads(text)
    except Exception:
        data = None
    if data is not None:
        _walk(data, candidates)
    if not candidates:
        candidates = extract_from_text(text)
    seen, out = set(), []
    for item in candidates:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def inject_auth(proxies: list[str], auth: str | None) -> list[str]:
    """给纯 ip:port 的代理补上认证信息（user:pass）。

    有些商家提取接口只返回 ip:port，真正的代理认证是另外一套账号密码，
    需要在请求代理时用 Basic 认证带上，这里统一拼进 URL。
    """
    credential = (auth or "").strip()
    if not credential or ":" not in credential:
        return proxies
    out = []
    for proxy in proxies:
        if "@" in proxy:
            out.append(proxy)
            continue
        scheme, rest = proxy.split("://", 1)
        out.append(f"{scheme}://{credential}@{rest}")
    return out


def apply_scheme(proxies: list[str], scheme: str) -> list[str]:
    """提取接口返回 ip:port 时，以用户选择的传输协议连接代理。"""
    if scheme not in ("http", "socks5", "socks5h"):
        raise ValueError("不支持的代理协议")
    return [scheme + "://" + proxy.split("://", 1)[-1] for proxy in proxies]


def _describe_empty(text: str) -> str:
    """Say what the supplier actually replied instead of just "unparsable".

    Suppliers answer 200 with a plain-text notice ("白名单不存在1.2.3.4",
    "余额不足", "频率过高"); that text is the whole diagnosis.
    """
    body = (text or "").strip()
    if not body:
        return "代理 API 返回空内容"
    head = body[:80]
    if all(ch.isprintable() or ch in "\r\n\t" for ch in head):
        return f"代理 API 返回：{head}"
    return f"返回体里没解析出代理（前 80 字：{head!r}）"


def _fetch_once(url: str, timeout: float = 15.0, auth: str | None = None,
                scheme: str | None = None, trace=None, route="direct") -> tuple[list[str], str]:
    """请求提取链接。返回 (代理列表, 错误说明)。

    auth：可选的代理认证 "用户:密码"，会拼到每条代理上；提取链路由 route 决定。
    """
    if not url:
        return [], "未填写提取链接"
    started = time.monotonic()
    timing = {"route": "system_proxy" if route == "system" else "without_system_proxy"}
    def report(**extra):
        if trace is not None:
            try:
                trace(dict(timing, total_ms=int((time.monotonic() - started) * 1000), **extra))
            except Exception:
                pass
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    opener = urllib.request.build_opener(
        urllib.request.HTTPSHandler(context=ctx),
        urllib.request.ProxyHandler() if route == "system" else urllib.request.ProxyHandler({}))
    req = urllib.request.Request(url, headers={
        "User-Agent": "FastCheckPro/3.0 (proxy-fetch)",
        "Accept": "*/*",
    })
    request_started = time.monotonic()
    timing["setup_ms"] = int((request_started - started) * 1000)
    try:
        with opener.open(req, timeout=timeout) as resp:
            headers_received = time.monotonic()
            timing["response_headers_ms"] = int((headers_received - request_started) * 1000)
            raw = resp.read(65536)
            timing.update(received_body_bytes=len(raw), http=getattr(resp, "status", 200),
                          response_complete=len(raw) < 65536)
            timing["body_ms"] = int((time.monotonic() - headers_received) * 1000)
            charset = resp.headers.get_content_charset() or "utf-8"
            text = raw.decode(charset, "replace")
    except urllib.error.HTTPError as exc:
        body = ""
        try:
            raw = exc.read(200)
            timing.update(received_body_bytes=len(raw), response_complete=len(raw) < 200)
            body = raw.decode("utf-8", "replace")
        except Exception:
            pass
        report(http=exc.code)
        return [], f"HTTP {exc.code} {body[:80]}"
    except Exception as exc:                       # noqa: BLE001
        report(error_type=type(exc).__name__)
        return [], f"{type(exc).__name__}: {str(exc)[:110]}"
    proxies = parse(text)
    proxies = inject_auth(proxies, auth)
    if scheme is not None:
        proxies = apply_scheme(proxies, scheme)
    if not proxies:
        report(count=0)
        return [], _describe_empty(text)
    report(count=len(proxies))
    return proxies, ""


class _FetchPool:
    """Bounded daemon workers: slow extraction must not hold desktop exit up.

    Sized for the refill lanes (default two concurrent extraction GETs) plus the
    route-hedging used by the pool mode, where one call can occupy two workers.
    """
    THREADS = 8

    def __init__(self):
        self.tasks = queue.Queue()
        self.lock = threading.Lock()
        self.started = False

    def submit(self, function, *args):
        with self.lock:
            if not self.started:
                for index in range(self.THREADS):
                    threading.Thread(target=self._run, daemon=True,
                                     name=f"proxy-extract-{index}").start()
                self.started = True
        future = Future()
        self.tasks.put((future, function, args))
        return future

    def _run(self):
        while True:
            future, function, args = self.tasks.get()
            try:
                if future.set_running_or_notify_cancel():
                    try:
                        future.set_result(function(*args))
                    except BaseException as exc:
                        future.set_exception(exc)
            finally:
                self.tasks.task_done()


_FETCH_POOL = _FetchPool()
_ROUTE_LOCK = threading.Lock()
_FAST_ROUTES = {}
_HEDGE_DELAY = 0.8
_ROUTE_TIMEOUT = 4.0


def fetch(url: str, timeout: float = 15.0, auth: str | None = None,
          scheme: str | None = None, trace=None, *, single_request=False) -> tuple[list[str], str]:
    """Use the browser's proxy route, hedging a stalled extraction after 0.8s.

    Only the extraction GET is eligible; Apple requests keep their explicit
    account proxy. A slow round may issue a second extraction GET.
    """
    if not url:
        return [], "未填写提取链接"
    started = time.monotonic()
    system = urllib.request.getproxies()
    routes = ["system", "direct"] if system.get(urlsplit(url).scheme) else ["direct"]
    host = urlsplit(url).netloc
    with _ROUTE_LOCK:
        preferred = _FAST_ROUTES.get(host)
    if preferred and preferred[0] in routes and started - preferred[1] < 60:
        routes.remove(preferred[0])
        routes.insert(0, preferred[0])
    if single_request:
        # Extraction can consume supplier quota: demand mode issues one GET,
        # without the speculative second request used by the legacy pool.
        return _fetch_once(url, timeout, auth, scheme, trace=trace, route=routes[0])
    route_timeout = max(0.1, min(float(timeout), _ROUTE_TIMEOUT))
    deadline = started + route_timeout + _HEDGE_DELAY + 0.2

    def request(route):
        timing = {}
        try:
            proxies, error = _fetch_once(url, route_timeout, auth, scheme,
                                         trace=timing.update, route=route)
            return proxies, error, timing, route
        except Exception as exc:
            return [], type(exc).__name__, timing, route

    futures = {_FETCH_POOL.submit(request, routes[0])}
    hedge_at = started + _HEDGE_DELAY
    fallback_started = False
    error = "代理提取超时，请稍后重试"
    while futures:
        now = time.monotonic()
        if now >= deadline:
            break
        if len(routes) > 1 and not fallback_started:
            delay = max(0, min(hedge_at, deadline) - now)
        else:
            delay = max(0, deadline - now)
        done, pending = wait(futures, timeout=delay, return_when=FIRST_COMPLETED)
        futures = set(pending)
        for future in done:
            proxies, failure, timing, route = future.result()
            if proxies:
                with _ROUTE_LOCK:
                    _FAST_ROUTES[host] = (route, time.monotonic())
                for pending_future in futures:
                    pending_future.cancel()
                if trace is not None:
                    try:
                        trace(dict(timing, total_ms=int((time.monotonic() - started) * 1000),
                                   fallback_started=fallback_started))
                    except Exception:
                        pass
                return proxies, ""
            error = failure or error
            hedge_at = time.monotonic()  # Failed primary: try the other route immediately.
        if len(routes) > 1 and not fallback_started and time.monotonic() >= hedge_at:
            futures.add(_FETCH_POOL.submit(request, routes[1]))
            fallback_started = True
    for future in futures:
        future.cancel()
    if trace is not None:
        try:
            trace({"total_ms": int((time.monotonic() - started) * 1000),
                   "fallback_started": fallback_started, "count": 0})
        except Exception:
            pass
    return [], error


def mask(proxy: str) -> str:
    """打码展示：http://u:***@1.2.3.4:8080"""
    if "@" not in proxy:
        return proxy
    head, tail = proxy.rsplit("@", 1)
    if "://" in head:
        scheme, cred = head.split("://", 1)
        user = cred.split(":", 1)[0]
        return f"{scheme}://{user}:***@{tail}"
    return f"***@{tail}"


# --------------------------------------------------------------------------- #
# 自检：不联网，只验证解析
# --------------------------------------------------------------------------- #
_SAMPLES = [
    ("纯文本 ip:port", "1.2.3.4:8080\n5.6.7.8:3128"),
    ("带凭据冒号格式", "1.2.3.4:8080:user:p@ss"),
    ("user:pass@host:port", "user:pass@1.2.3.4:8080"),
    ("JSON 数组", '[{"ip":"1.2.3.4","port":8080,"username":"u","password":"p"}]'),
    ("JSON data 包装", '{"data":[{"ip":"9.9.9.9","port":1080},{"host":"8.8.8.8","port":8888}]}'),
    ("JSON proxy 字段", '{"result":{"proxy_list":["1.1.1.1:1234","2.2.2.2:4321"]}}'),
    ("混合文本", 'ok: http://u:p@3.3.3.3:9999 available'),
]

if __name__ == "__main__":
    for label, sample in _SAMPLES:
        got = parse(sample)
        print(f"{label:22s} -> {len(got)} 个: {[mask(g) for g in got]}")
