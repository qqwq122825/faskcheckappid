"""网页版（idmsa）登录接口后端。

与 gsa_status 并列的网页 SRP 校验通道：

    POST https://idmsa.apple.com/appleauth/auth/signin/init
         {"accountName": ..., "a": ..., "protocols": ["s2k", "s2k_fo"]}
         -> salt/b/c/iteration/protocol + scnt 会话令牌
    POST .../signin/complete?isRememberMeEnabled=true
         {"accountName": ..., "m1": ..., "m2": ..., "c": ..., "rememberMe": true}

初始化失败或缺少挑战时停止，不降级提交明文密码。这里只做登录状态校验，
不会继续完成双重验证，也不会保存可供其他业务使用的登录会话。

对外返回的 dict 与 gsa_status.check() 完全一致（status/detail/ms/kind/stage），
所以引擎和界面不需要区分后端。
"""
from __future__ import annotations

import base64
from contextlib import nullcontext
import http.cookiejar
import json
import ssl
import time
import urllib.error
import urllib.request

from .gsa_status import _error_kind, classify, mock_enabled, _seed
from .srp_auth import N_BYTES, SRPClient
from .timing import phase, emit, exception_metadata

try:                       # SOCKS5 住宅代理需要它；没有就退回 urllib
    import requests
    _HAS_REQUESTS = True
except Exception:          # pragma: no cover
    requests = None        # type: ignore
    _HAS_REQUESTS = False

IDMSA_INIT = "https://idmsa.apple.com/appleauth/auth/signin/init"
IDMSA_COMPLETE = "https://idmsa.apple.com/appleauth/auth/signin/complete?isRememberMeEnabled=true"
# iCloud 网页客户端的公开标识，不是账号密钥。
WIDGET_KEY = "d39ba9916b7251055b22c7f910e2ea796ee65e98b2ddecea8f5dde8d9d1a815d"

_BASE_HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Content-Type": "application/json",
    "X-Requested-With": "XMLHttpRequest",
    "X-Apple-OAuth-Client-Type": "firstPartyAuth",
    "X-Apple-Widget-Key": WIDGET_KEY,
    "X-Apple-OAuth-Client-Id": WIDGET_KEY,
    "X-Apple-OAuth-Redirect-URI": "https://www.icloud.com",
    "X-Apple-OAuth-Require-Grant-Code": "true",
    "X-Apple-OAuth-Response-Type": "code",
    "X-Apple-OAuth-Response-Mode": "web_message",
    "X-Apple-Trusted-Domain": "true",
    "X-Apple-Locale": "zh_CN",
    "Origin": "https://idmsa.apple.com",
    "Referer": "https://idmsa.apple.com/",
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/156.0.0.0 Safari/537.36"),
}

MOCK_RESULTS = ("valid", "wrong_password", "wrong_password", "not_active",
                "security_lock", "locked", "need_2fa", "error")
MOCK_CN = {"valid": "有效", "wrong_password": "密码错误", "not_active": "未激活",
           "security_lock": "安全锁定", "locked": "已锁定", "need_2fa": "需要二次验证",
           "error": "请求失败"}


# --------------------------------------------------------------------------- #
# 传输层：每个账号一套 cookie 会话（idmsa 靠 cookie + scnt 维持状态）
# --------------------------------------------------------------------------- #
class _Client:
    """优先用 requests（支持 SOCKS5 住宅代理），没有就退回 urllib。"""

    def __init__(self, proxy: str | None, timeout: float, diagnostics=None, session=None):
        self.diagnostics = diagnostics
        self.timeout = timeout
        self.proxy = proxy
        self.jar = http.cookiejar.CookieJar()
        self.headers = dict(_BASE_HEADERS)
        self._sess = None
        self._network_trace = None
        self._borrowed = session is not None
        if _HAS_REQUESTS:
            if session is not None:
                # 借来的连接池：adapter/proxies 已由工作线程配好，重新 mount 会
                # 把连接池丢掉（等于白付一次 TLS 握手）。这里只清 cookie。
                session.cookies.clear()
                self._sess = session
                return
            sess = requests.Session()
            sess.trust_env = False          # 不吃系统代理（Clash 全局时会套娃）
            if proxy:
                sess.proxies = {"http": proxy, "https": proxy}
            if diagnostics is not None:
                from .network_timing import NetworkTrace, TimingAdapter
                self._network_trace = NetworkTrace(diagnostics)
                adapter = TimingAdapter(self._network_trace)
            else:
                from .tls import VerifiedTLSAdapter
                adapter = VerifiedTLSAdapter()
            sess.mount("http://", adapter)
            sess.mount("https://", adapter)
            self._sess = sess
            return
        with phase(diagnostics, "tls_setup"):
            ctx = ssl.create_default_context()
        handlers = [urllib.request.HTTPSHandler(context=ctx),
                    urllib.request.HTTPCookieProcessor(self.jar)]
        if proxy:
            handlers.append(urllib.request.ProxyHandler({"http": proxy, "https": proxy}))
        else:
            handlers.append(urllib.request.ProxyHandler({}))
        self.opener = urllib.request.build_opener(*handlers)

    def post(self, url: str, payload: dict) -> tuple[int, bytes, dict, str]:
        """返回 (状态码, 响应体, 响应头, 传输错误)。"""
        if self._sess is not None:
            try:
                trace = self._network_trace
                context = trace.request("http_init" if url == IDMSA_INIT else "http_complete") if trace else nullcontext()
                with context:
                    kwargs = {"stream": True} if trace else {}
                    resp = self._sess.post(url, json=payload, headers=self.headers,
                                           timeout=self.timeout, allow_redirects=False, **kwargs)
                    try:
                        hdrs = {k.lower(): v for k, v in resp.headers.items()}
                        with phase(trace.record if trace else None, "response_body"):
                            raw = resp.content[:8192]
                        return resp.status_code, raw, hdrs, ""
                    finally:
                        resp.close()
            except Exception as exc:                        # noqa: BLE001
                if self.diagnostics is not None:
                    emit(self.diagnostics, {"event": "transport_error",
                         "phase": "http_init" if url == IDMSA_INIT else "http_complete",
                         **exception_metadata(exc)})
                return 0, b"", {}, type(exc).__name__
        body = json.dumps(payload).encode()
        req = urllib.request.Request(url, data=body, headers=self.headers, method="POST")
        try:
            with self.opener.open(req, timeout=self.timeout) as resp:
                hdrs = {k.lower(): v for k, v in resp.headers.items()}
                return resp.getcode(), resp.read(8192), hdrs, ""
        except urllib.error.HTTPError as exc:
            hdrs = {k.lower(): v for k, v in (exc.headers or {}).items()}
            try:
                return exc.code, exc.read(8192), hdrs, ""
            except Exception:
                return exc.code, b"", hdrs, ""
        except Exception as exc:                            # noqa: BLE001
            if self.diagnostics is not None:
                emit(self.diagnostics, {"event": "transport_error",
                     "phase": "http_init" if url == IDMSA_INIT else "http_complete",
                     **exception_metadata(exc)})
            return 0, b"", {}, type(exc).__name__

    def close(self) -> None:
        # 借来的连接池由调用方（工作线程）负责回收，这里不能关掉。
        if self._sess is not None and not self._borrowed:
            self._sess.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def _remember_session(client: _Client, hdrs: dict) -> dict:
    """把 init 拿到的会话令牌带进后续请求。"""
    extra = {}
    hdrs = {k.lower(): v for k, v in hdrs.items()}
    scnt = hdrs.get("scnt")
    session = hdrs.get("x-apple-id-session-id")
    if scnt:
        extra["scnt"] = scnt
        client.headers["scnt"] = scnt
    if session:
        client.headers["X-Apple-ID-Session-Id"] = session
    attributes = hdrs.get("x-apple-auth-attributes")
    if attributes:
        client.headers["X-Apple-Auth-Attributes"] = attributes
    return extra


def _result(status: str, detail: str, started: float, kind: str = "",
            stage: str = "") -> dict:
    return {"status": status, "detail": detail,
            "ms": int((time.time() - started) * 1000),
            "kind": kind or ("" if status != "error" else _error_kind(detail)),
            "stage": stage}


def _json(raw: bytes) -> dict:
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _verdict(code: int, data: dict, stage: str) -> tuple[str, str, str]:
    """把响应翻译成 (状态, 详情, 错误类别)。"""
    # HTTP 错误优先；不能把服务不可用或初始化响应当成密码结论。
    if code == 429:
        return "error", "HTTP 429（请求过多，请稍后再试）", "ratelimit"
    if 500 <= code < 600:
        return "error", f"HTTP {code}（苹果服务或网络链路暂不可用，无法判断账号密码）", "server"
    if code == 407:
        return "error", "HTTP 407（代理认证失败）", "proxy_auth"
    errors = data.get("serviceErrors")
    if errors is None:
        errors = []
    if not isinstance(errors, list):
        return "error", "serviceErrors 格式异常", "protocol"
    first = errors[0] if errors and isinstance(errors[0], dict) else {}
    ec = first.get("code")
    em = first.get("message") or ""
    if stage == "complete" and code in (200, 409, 412) and not errors:
        auth = data.get("authType")
        auth = auth.strip() if isinstance(auth, str) else None
        # 此处的 valid 表示账号密码通过，不宣称业务会话或所有设置步骤已完成。
        # 仅对完成 SRP 后的明确 sa/non-sa 响应作此判定，未知错误仍保留为错误。
        if code in (200, 412) and auth in ("sa", "non-sa"):
            if code == 412:
                return "valid", f"密码验证通过（authType={auth}）；隐私或安全设置待确认（HTTP 412）", ""
            return "valid", f"账号密码验证通过（authType={auth}）", ""
        if code == 412 and auth in ("sa", "non-sa", "hsa", "hsa2"):
            return "needs_setup", "需确认隐私或安全设置（HTTP 412）；不能据此判断已开启双重验证", ""
        if code in (200, 409) and auth in ("hsa2", "hsa"):
            return "need_2fa", f"需要二次验证（authType={auth}）；当前工具未完成验证码登录", ""
        if code == 409 and auth == "sa":
            return "needs_setup", "需确认账号安全设置（authType=sa）；未获得明确的验证码挑战", ""
        if auth == "dnu":
            return "error", "账号需要在苹果官方页面完成后续操作（authType=dnu）", "protocol"
    if ec is not None:
        try:
            ec_int = int(ec)
        except (TypeError, ValueError):
            ec_int = None
        if stage == "init" and ec_int == -900007:
            return "error", "初始化未完成（ec=-900007），尚未校验密码", "init_incomplete"
        if stage == "complete" and code == 401 and ec_int == -20283:
            return "credentials_rejected", "苹果拒绝了本次账号信息（ec=-20283）；请核对账号和密码，若官方可登录则需检查登录环境或请求兼容性", ""
        if ec_int in (-22406, -22025, -22420):
            return "error", f"ec={ec}（认证未完成，可能需要官方页面验证，不能确定密码错误）", "protocol"
        verdict, detail = classify(ec_int, em)
        if verdict == "valid":
            return "error", f"HTTP {code}（错误列表中含 ec={ec}，不能确认登录成功）", "protocol"
        if verdict == "error":
            return "error", f"ec={ec} {em}".strip(), "protocol"
        if verdict == "wrong_password":
            return verdict, f"苹果拒绝了账号或密码（ec={ec}）", ""
        return verdict, detail or f"ec={ec}", ""
    if code in (401, 403):
        return "error", f"HTTP {code}（被拒，可能触发了风控）", "ratelimit" if code == 403 else "endpoint"
    if code == 0:
        return "error", "网络不通", "network"
    return "error", f"HTTP {code}（未识别的响应）", "protocol"


# --------------------------------------------------------------------------- #
# 对外接口（与 gsa_status 同签名）
# --------------------------------------------------------------------------- #
def should_retry(result: dict) -> bool:
    """Retry only transient transport/server failures, never account verdicts."""
    if result.get("stage") in ("probe", "check"):
        return False  # Connection probes and synthetic mock results aren't login attempts.
    return (result.get("status") == "error"
            and result.get("kind") in {"network", "proxy", "tls", "timeout", "server"})


def check(email: str, password: str, proxy: str | None, timeout: float = 20.0,
          retries: int = 1, cancel=None, trace=None, diagnostics=None, session=None) -> dict:
    started = time.time()
    if mock_enabled():
        status = MOCK_RESULTS[_seed(email, "mock") % len(MOCK_RESULTS)]
        with phase(diagnostics, "mock_check"):
            time.sleep(0.05)
        return _result(status, "" if status == "valid" else f"模拟结果：{MOCK_CN[status]}",
                       started, stage="check")

    last: dict | None = None
    for attempt in range(max(0, retries) + 1):
        if cancel and cancel():
            return _result("stopped", "", started, stage="check")
        if attempt and last and not should_retry(last):
            return last
        result = _once(email, password, proxy, timeout, cancel, started, trace, diagnostics,
                       session=session)
        if result["status"] in ("stopped",):
            return result
        if not should_retry(result):
            return result
        last = result
    return last or _result("error", "未发起请求", started, stage="check")


def _once(email: str, password: str, proxy: str | None, timeout: float,
          cancel, started: float, trace=None, diagnostics=None, session=None) -> dict:
    with phase(diagnostics, "client_setup"):
        client = _Client(proxy, timeout, diagnostics=diagnostics, session=session)
    with client:
        return _authenticate(client, email, password, cancel, started, trace, diagnostics)


def _trace(callback, stage, code, data, headers):
    """Only protocol metadata; never passwords, proofs, cookies or header values."""
    if callback is None:
        return
    auth = data.get("authType")
    errors = data.get("serviceErrors") or []
    codes = [str(item.get("code", ""))[:24] for item in errors if isinstance(item, dict)] if isinstance(errors, list) else []
    entry = {"stage": stage, "http": code,
             "auth_type": auth if auth in ("sa", "non-sa", "hsa", "hsa2", "dnu") else None,
             "response_keys": sorted(str(key)[:64] for key in data), "service_error_codes": codes,
             "has_session_headers": any(key.lower() in ("scnt", "x-apple-id-session-id") for key in headers),
             "has_set_cookie": any(key.lower() == "set-cookie" for key in headers)}
    try:
        callback(entry)
    except Exception:
        pass


def _init_payload(email: str, srp: SRPClient) -> dict:
    return {"accountName": email, "a": base64.b64encode(srp.A_bytes).decode(),
            "protocols": ["s2k", "s2k_fo"]}


def _challenge_error(data: dict) -> str:
    missing = [key for key in ("salt", "b", "c", "iteration", "protocol")
               if not data.get(key)]
    if missing:
        return "init 缺少 SRP 挑战字段：" + ", ".join(missing)
    if data["protocol"] not in ("s2k", "s2k_fo"):
        return "init 返回不支持的 SRP 协议"
    iterations = data["iteration"]
    if type(iterations) is not int or not 1 <= iterations <= 1_000_000:
        return "init 返回异常的密码派生迭代次数"
    if not isinstance(data["c"], str):
        return "init 返回异常的会话挑战"
    try:
        salt = base64.b64decode(data["salt"], validate=True)
        server_b = base64.b64decode(data["b"], validate=True)
        if not salt or not server_b or len(server_b) > N_BYTES:
            return "init 返回异常的 SRP 挑战长度"
    except (ValueError, TypeError):
        return "init 返回无效的 SRP 挑战编码"
    return ""


def _transport_result(err: str, started: float, stage: str) -> dict:
    low = err.lower()
    kind = ("tls" if any(token in low for token in ("ssl", "certificate")) else
            "proxy" if "proxy" in low else "network")
    return _result("error", f"{stage} {err}", started, kind=kind, stage=stage)


def _authenticate(client: _Client, email: str, password: str, cancel,
                  started: float, trace=None, diagnostics=None) -> dict:
    # A 必须先送到 init，后续证明使用同一个 SRP 实例的私钥。
    with phase(diagnostics, "srp_init"):
        srp = SRPClient(email, password)
    with phase(diagnostics, "http_init") as timing:
        code, raw, hdrs, err = client.post(IDMSA_INIT, _init_payload(email, srp))
        timing.update(http=code, error_type=err)
    data = _json(raw)
    _trace(trace, "init", code, data, hdrs)
    if cancel and cancel():
        return _result("stopped", "", started, stage="init")
    if code == 0:
        return _transport_result(err, started, "init")
    _remember_session(client, hdrs)
    if code != 200 or data.get("serviceErrors"):
        verdict, detail, kind = _verdict(code, data, "init")
        # init 尚未提交密码，不能由此断言密码错误。
        if verdict in ("valid", "wrong_password", "need_2fa"):
            verdict, kind = "error", "protocol"
            detail = "init 未完成，尚未校验密码"
        return _result(verdict, detail, started, kind=kind, stage="init")
    error = _challenge_error(data)
    if error:
        return _result("error", error, started, kind="protocol", stage="init")
    try:
        with phase(diagnostics, "srp_challenge") as timing:
            timing["iterations"] = data["iteration"]
            m1 = srp.process_challenge(base64.b64decode(data["salt"]),
                                       base64.b64decode(data["b"]),
                                       data["iteration"], data["protocol"])
    except Exception as exc:                        # noqa: BLE001
        return _result("error", f"SRP 计算失败：{type(exc).__name__}", started,
                       kind="protocol", stage="srp")
    if cancel and cancel():
        return _result("stopped", "", started, stage="complete")
    payload = {"accountName": email, "m1": base64.b64encode(m1).decode(),
               "m2": base64.b64encode(srp.M2_expected).decode(),
               "c": data["c"], "rememberMe": True, "trustTokens": []}
    with phase(diagnostics, "http_complete") as timing:
        code, raw, hdrs, err = client.post(IDMSA_COMPLETE, payload)
        timing.update(http=code, error_type=err)
    data = _json(raw)
    _trace(trace, "complete", code, data, hdrs)
    if code == 0:
        return _transport_result(err, started, "complete")
    _remember_session(client, hdrs)
    if cancel and cancel():
        return _result("stopped", "", started, stage="complete")
    verdict, detail, kind = _verdict(code, data, "complete")
    return _result(verdict, detail, started, kind=kind, stage="complete")


def probe(email: str, proxy: str | None, timeout: float = 20.0, cancel=None) -> dict:
    """只发 init 测试挑战是否可获取；不能证明账号存在或密码正确。"""
    started = time.time()
    if mock_enabled():
        status = MOCK_RESULTS[_seed(email, "mock") % len(MOCK_RESULTS)]
        if status in ("valid", "wrong_password", "need_2fa"):
            status = "alive"
        return _result(status, "" if status == "alive" else f"模拟结果：{MOCK_CN.get(status, status)}",
                       started, stage="probe")
    if cancel and cancel():
        return _result("stopped", "", started, stage="probe")
    with _Client(proxy, timeout) as client:
        srp = SRPClient(email, "")
        code, raw, hdrs, err = client.post(IDMSA_INIT, _init_payload(email, srp))
    if cancel and cancel():
        return _result("stopped", "", started, stage="probe")
    if code == 0:
        return _transport_result(err, started, "probe")
    data = _json(raw)
    if code == 200 and not data.get("serviceErrors"):
        error = _challenge_error(data)
        if error:
            return _result("error", error, started, kind="protocol", stage="probe")
        return _result("alive", "接口已返回 SRP 挑战，尚未验证账号或密码", started, stage="probe")
    verdict, detail, kind = _verdict(code, data, "probe")
    return _result("error" if verdict in ("valid", "wrong_password", "need_2fa") else verdict, detail, started,
                   kind=kind, stage="probe")
