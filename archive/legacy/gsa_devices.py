"""Prepare five identities and select a persistent device for timed testing."""
from __future__ import annotations

import argparse
import getpass
from pathlib import Path
import sys
import time
import warnings

from fastcheck.single_login import _dependencies, render_result
from fastcheck.devices import DevicePool, PoolError, RetryPolicy
from fastcheck.protocols import gsa_status as gsa
from fastcheck.protocols.anisette_v3 import AnisetteError


def arguments(argv=None):
    parser = argparse.ArgumentParser(description="5 套持久设备身份：注册、选择、计时、有限重试")
    parser.add_argument("--anisette-url", default=next(u for u in gsa.ANISETTE_SERVERS if u.startswith("https://")))
    parser.add_argument("--pool-dir", type=Path, default=Path(__file__).resolve().parent / "config" / "device-pool")
    parser.add_argument("--timeout", type=float, default=25)
    parser.add_argument("--prepare-timeout", type=float, default=60)
    parser.add_argument("--attempts", type=int, choices=(1, 2, 3), default=2,
                        help="包含首次在内的最大尝试轮数，默认 2")
    commands = parser.add_subparsers(dest="command")
    prepare = commands.add_parser("prepare", help="准备设备；已注册的编号直接复用")
    prepare.add_argument("--count", type=int, choices=range(1, 6), default=5)
    prepare.add_argument("--refresh-info", action="store_true", help="重新同步服务的客户端配置")
    commands.add_parser("list", help="列出设备状态；不联网")
    for name in ("refresh", "login"):
        command = commands.add_parser(name, help="仅测参数刷新" if name == "refresh" else "选择设备认证")
        command.add_argument("--device", type=int, choices=range(1, 6), required=True)
    options = parser.parse_args(argv)
    if not all(1 <= value <= 300 for value in (options.timeout, options.prepare_timeout)):
        parser.error("超时秒数必须为 1–300。")
    return options


def show_devices(pool):
    started = time.perf_counter()
    health = pool.health()
    now = pool.clock()
    print("编号  设备状态        初始化拒绝次数  设备标识（末 8 位）")
    names = {"ready": "可用", "missing": "未注册", "pending": "注册未完成", "invalid": "文件异常"}
    for slot in range(1, 6):
        item = pool.inspect(slot)
        device = health["devices"].get(str(slot), {})
        left = device.get("until", 0) - now
        status = f"冷却 {left:.0f}s" if left > 0 else names[item["state"]]
        print(f" {slot}    {status:<12} {device.get('failures', 0):<16} {item.get('device_id', '--------')[-8:]}")
    left = health.get("provider_until", 0) - now
    if left > 0:
        print(f"服务暂停分发，冷却剩余 {left:.0f} 秒。")
    print(f"本地列表加载：{(time.perf_counter() - started) * 1000:.1f} 毫秒。")


def show_event(value):
    if value.get("action") == "registering":
        print(f"设备 {value['slot']}：正在注册……", flush=True)
    elif value.get("action") == "registered":
        print(f"设备 {value['slot']}：注册并取得参数成功，耗时 {value['ms'] / 1000:.2f} 秒。", flush=True)
    elif value.get("action") == "reused":
        print(f"设备 {value['slot']}：已注册，直接复用。", flush=True)
    elif value.get("event") == "attempt":
        print(f"第 {value['attempt']} 轮：刷新参数并创建新的认证会话……", flush=True)
    elif value.get("event") == "attempt_result":
        code = f" / ec={value['apple_ec']}" if value.get("apple_ec") is not None else ""
        print(f"刷新 {value['refresh_ms'] / 1000:.2f}s，GSA {value['auth_ms'] / 1000:.2f}s；"
              f"{value.get('stage')} / {value.get('status')}{code}", flush=True)
    elif value.get("event") == "backoff":
        print(f"等待 {value['seconds']:.2f} 秒后，使用同一设备重试。", flush=True)


def timing_notice(pool):
    if pool.timing_error:
        print(f"耗时记录未能写盘：{pool.timing_error}；请以本次终端输出为准。")


def prepare(pool, options):
    if gsa.mock_enabled():
        raise PoolError("请清除 FASTCHECK_MOCK 后再准备真实设备。")
    print("按顺序准备设备，新增设备之间间隔 3 秒；失败时停止，已成功状态会保留。", flush=True)
    started = time.perf_counter()
    pool.prepare(getattr(options, "count", 5), timeout=options.prepare_timeout,
                 refresh_info=getattr(options, "refresh_info", False), event=show_event)
    timing_notice(pool)
    print(f"准备完成，共耗时 {time.perf_counter() - started:.2f} 秒。后续选择设备时无需重复注册。")
    return 0


def refresh(pool, slot, options):
    if gsa.mock_enabled():
        raise PoolError("请清除 FASTCHECK_MOCK 后再测试真实设备。")
    print(f"设备 {slot}：仅测试参数刷新，不提交账号。", flush=True)
    timings = pool.refresh(slot, timeout=options.prepare_timeout)
    timing_notice(pool)
    print(f"配置加载 {timings['load_ms']:.1f}ms，参数刷新 {timings['refresh_ms'] / 1000:.2f}s。")
    return 0


def login(pool, slot, options):
    if not sys.stdin.isatty():
        raise PoolError("请在本机终端交互输入账号和密码。")
    if gsa.mock_enabled():
        raise PoolError("请清除 FASTCHECK_MOCK 后再执行真实认证。")
    pool.allowed(slot)
    if pool.inspect(slot)["state"] != "ready":
        raise PoolError(f"设备 {slot} 尚未注册，请先运行 prepare。")
    # Synchronize/cache only metadata before input; no OTP is prefetched.
    started = time.perf_counter()
    pool.info(options.prepare_timeout)
    print(f"设备 {slot} 已选中；配置加载 {(time.perf_counter() - started) * 1000:.1f}ms。")
    password = None
    try:
        email = input("Apple 账号：").strip()
        if not email or len(email) > 4096:
            raise PoolError("账号不能为空，且不能超过 4096 字符。")
        with warnings.catch_warnings():
            warnings.simplefilter("error", getpass.GetPassWarning)
            password = getpass.getpass("密码（输入时不显示）：")
        if not password or len(password) > 4096:
            raise PoolError("密码不能为空，且不能超过 4096 字符。")
        result = pool.authenticate(slot, email, password, timeout=options.timeout,
                                  prepare_timeout=options.prepare_timeout, event=show_event)
        timing_notice(pool)
        print(f"本次执行 {result['attempts']} 轮，总耗时 {result['total_ms'] / 1000:.2f}s"
              f"（不含账号密码输入时间，包含刷新、认证及退避）。")
        if result.get("retry_stopped_reason"):
            print(result["retry_stopped_reason"])
        health = pool.health()["devices"].get(str(slot), {})
        print(f"该设备未恢复的 init/-20970 计数：{health.get('failures', 0)}。")
        return render_result(result)
    finally:
        password = None
        gsa.close_session()


def menu(pool, options):
    if not sys.stdin.isatty():
        raise PoolError("请在本机终端运行，或指定 prepare / list / refresh 子命令。")
    while True:
        show_devices(pool)
        print("输入设备编号 1–5 进行认证；p 准备 5 套设备；r 只测参数刷新；q 退出。")
        choice = input("选择：").strip().lower()
        if choice == "q":
            return 0
        try:
            if choice == "p":
                prepare(pool, options)
            elif choice == "r":
                slot = int(input("设备编号（1–5）：").strip())
                refresh(pool, slot, options)
            elif choice in ("1", "2", "3", "4", "5"):
                login(pool, int(choice), options)
            else:
                print("请输入 1–5、p、r 或 q。")
        except (PoolError, AnisetteError) as exc:
            print(str(exc))
        except ValueError:
            print("请输入有效设备编号。")
        except Exception as exc:
            print(f"操作未完成：{type(exc).__name__}。设备状态已保留。")
        print()


def main(argv=None):
    options = arguments(argv)
    try:
        _dependencies()
        pool = DevicePool(options.pool_dir, options.anisette_url,
                          policy=RetryPolicy(attempts=options.attempts))
        if options.command == "prepare":
            return prepare(pool, options)
        if options.command == "list":
            show_devices(pool)
            return 0
        if options.command == "refresh":
            return refresh(pool, options.device, options)
        if options.command == "login":
            return login(pool, options.device, options)
        return menu(pool, options)
    except (KeyboardInterrupt, EOFError):
        print("\n操作已取消；已保存的设备状态保留。")
        return 130
    except getpass.GetPassWarning:
        print("当前终端不能隐藏密码，请在系统终端运行。", file=sys.stderr)
        return 2
    except (PoolError, AnisetteError) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except Exception as exc:
        print(f"操作未完成：{type(exc).__name__}。未导出凭据或响应正文。", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
