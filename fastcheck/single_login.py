"""Interactive single-account GSA authentication using extracted FastCheck code."""
from __future__ import annotations

import argparse
import getpass
import os
from pathlib import Path
import sys
import time
import warnings


def arguments(argv=None):
    parser = argparse.ArgumentParser(description="Apple GSA 单账号认证（不会保存账号密码）")
    parser.add_argument("--anisette-url", help="Anisette v3 地址；默认沿用原项目首个 HTTPS 服务")
    parser.add_argument("--timeout", type=float, default=25, help="每次 GSA 请求超时秒数（默认 25）")
    parser.add_argument("--prepare-timeout", type=float, default=60, help="设备准备超时秒数（默认 60）")
    parser.add_argument("--prepare-only", action="store_true", help="只准备设备认证，不读取或提交账号")
    parser.add_argument("--self-check", action="store_true", help="离线检查依赖、证书和模块导入")
    options = parser.parse_args(argv)
    for name in ("timeout", "prepare_timeout"):
        value = getattr(options, name)
        if not 1 <= value <= 300:
            parser.error(f"--{name.replace('_', '-')} 必须在 1–300 秒之间")
    return options


def _dependencies():
    import requests
    from websockets.sync.client import connect
    from fastcheck.protocols import gsa_status
    from fastcheck.protocols.anisette_v3 import Client
    from fastcheck.protocols.apple_trust import gsa_ca_file, gsa_context
    # Import optional upstream components without creating state or connections.
    from fastcheck.protocols import gsa_dns, network_timing

    gsa_ca_file()
    gsa_context()
    return gsa_status, Client


def render_result(result):
    labels = {
        "valid": "密码认证通过；业务服务登录状态未确认。",
        "need_2fa": "密码认证通过，需要二次验证。此项目尚未实现验证码提交。",
        "needs_setup": "密码认证通过，Apple 要求进一步验证。",
        "wrong_password": "密码认证失败。",
        "security_lock": "账号安全锁定，已停止。",
        "locked": "账号已锁定或停用，已停止。",
        "not_active": "账号未激活。",
        "stopped": "认证已停止。",
        "error": "未取得认证结论。",
    }
    status = result.get("status", "error")
    print(labels.get(status, "未取得认证结论。"))
    if result.get("apple_message"):
        print("Apple：" + result["apple_message"])
    elif result.get("detail"):
        print(result["detail"])
    context = [str(result[key]) for key in ("stage", "kind") if result.get(key)]
    if "apple_ec" in result:
        context.append(f"ec={result['apple_ec']}")
    if context:
        print("阶段：" + " / ".join(context))
    if status == "need_2fa" and result.get("password_verified"):
        return 3
    if status == "valid" and result.get("password_verified"):
        return 0
    return 1


def fetch_identity(Client, origin, timeout, *, client_info=None):
    client = Client(origin, timeout=timeout, client_info=client_info)
    try:
        return client.headers()
    finally:
        client.close()


def main(argv=None):
    options = arguments(argv)
    try:
        gsa, Client = _dependencies()
    except (ImportError, OSError, ValueError) as exc:
        print(f"本地依赖检查失败：{type(exc).__name__}。", file=sys.stderr)
        print("请使用 .venv/bin/python，并安装 requirements.txt。", file=sys.stderr)
        return 2
    if options.self_check:
        print("离线检查通过：SRP、Anisette v3、HTTP/DNS 模块和 Apple 根证书已就绪。")
        print("未联网，未读取账号。")
        return 0
    if gsa.mock_enabled():
        print("FASTCHECK_MOCK=1 会生成模拟结果；请清除此环境变量后再认证。", file=sys.stderr)
        return 2
    if not options.prepare_only and not sys.stdin.isatty():
        print("请在本机终端交互运行，账号密码不会从管道或文件读取。", file=sys.stderr)
        return 2
    origin = options.anisette_url or next(url for url in gsa.ANISETTE_SERVERS if url.startswith("https://"))
    # Keep device state local to this workspace, independent of the caller's cwd.
    state_directory = Path(__file__).resolve().parents[1] / "data" / "config" / "anisette-v3"
    os.environ.setdefault("FASTCHECK_ANISETTE_STATE_DIR", str(state_directory))
    print(f"设备认证服务：{origin}", flush=True)
    print("正在准备设备认证参数；首次运行会注册并保存本地设备身份。", flush=True)
    prepare_started = time.perf_counter()
    try:
        identity = fetch_identity(Client, origin, options.prepare_timeout)
        gsa._build_cpd(identity, "")
    except KeyboardInterrupt:
        print("\n已取消设备准备。")
        return 130
    except Exception as exc:
        # Upstream transport exceptions may include provisioning values. Print
        # only error classes plus the numeric fields of its typed error.
        from fastcheck.protocols.anisette_v3 import AnisetteError
        detail = str(exc) if isinstance(exc, AnisetteError) else type(exc).__name__
        print(f"设备准备失败：{detail}；尚未提交账号认证。", file=sys.stderr)
        return 2
    print(f"设备认证参数已就绪（{time.perf_counter() - prepare_started:.2f} 秒）。", flush=True)
    if options.prepare_only:
        print("准备完成，未读取或提交账号。")
        return 0
    password = None
    try:
        email = input("Apple 账号：").strip()
        if not email or len(email) > 4096:
            print("账号不能为空，且不能超过 4096 字符。", file=sys.stderr)
            return 2
        # getpass otherwise falls back to echoing the password on unsupported
        # terminals; convert its warning to an error before accepting input.
        with warnings.catch_warnings():
            warnings.simplefilter("error", getpass.GetPassWarning)
            password = getpass.getpass("密码（输入时不显示）：")
        if not password or len(password) > 4096:
            print("密码不能为空，且不能超过 4096 字符。", file=sys.stderr)
            return 2
        # User input can take minutes. Refresh OTP after input, keeping the same
        # provisioned identity and its matching client metadata.
        print("正在刷新设备认证参数……", flush=True)
        refresh_started = time.perf_counter()
        identity = fetch_identity(Client, origin, options.prepare_timeout, client_info={
            "client_info": gsa._client_info(identity), "user_agent": identity["User-Agent"],
        })
        print(f"参数刷新耗时：{time.perf_counter() - refresh_started:.2f} 秒。", flush=True)
        print("正在提交 GSA 认证（单次尝试）……", flush=True)
        result = gsa.check(email, password, proxy=None, timeout=options.timeout,
                           retries=0, identity=identity)
        if "ms" in result:
            print(f"GSA 认证耗时：{result['ms'] / 1000:.2f} 秒。")
        return render_result(result)
    except (EOFError, KeyboardInterrupt):
        print("\n已取消认证。")
        return 130
    except getpass.GetPassWarning:
        print("当前终端无法隐藏密码，请在系统终端运行。", file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"认证未完成：{type(exc).__name__}。", file=sys.stderr)
        return 1
    finally:
        password = None
        gsa.close_session()


if __name__ == "__main__":
    raise SystemExit(main())
