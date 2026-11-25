"""Shared interactive entry point for web and GSA process/thread workers."""
import argparse
from dataclasses import fields
from datetime import datetime
import getpass
import json
import os
from pathlib import Path
import sys
import tempfile
import warnings
from zoneinfo import ZoneInfo

from fastcheck.scheduler import Settings, Supervisor
from fastcheck.workers import demo_attempt, demo_registration
from fastcheck.task_io import Account, read_accounts
from fastcheck.protocols import gsa_status as gsa
from fastcheck.settings_store import prompt_api
from fastcheck.version import VERSION
from fastcheck.devices import file_lease,PoolError

ROOT = Path(__file__).resolve().parents[1]


def configuration(path, args):
    values = json.loads(Path(path).read_text()) if path else {}
    if not isinstance(values, dict) or set(values)-{f.name for f in fields(Settings)}:
        raise ValueError('配置只支持 Settings 中的调度字段，请参考 auth_settings.example.json。')
    for key in ('protocol', 'processes', 'threads', 'proxy_target', 'debug', 'proxy_scheme'):
        if getattr(args, key, None) is not None:
            values[key] = getattr(args, key)
    return values


def read_integer(label, default, low, high, reader=None):
    reader = reader or input
    while True:
        entered = reader(f'{label} [{default}]：').strip()
        if not entered:
            return default
        try:
            value = int(entered)
        except ValueError:
            value = None
        if value is not None and value >= low and (high is None or value <= high):
            return value
        print(f'请输入不少于 {low} 的整数。' if high is None else f'请输入 {low}–{high} 之间的整数。')


def choose_run_settings(values, args, reader=None):
    selected = dict(values)
    for name,label,default,low,high in (
        ('processes','工作进程数',2,1,None),
        ('threads','每个进程的线程数',10,1,None),
        ('proxy_target','首批 IP 数量（0=按线程容量 ×1.5 预取并复用；每次 API 最多 200）',0,0,None)):
        if getattr(args,name) is not None:
            selected[name] = getattr(args,name)
        else:
            selected[name] = read_integer(label,selected.get(name,default),low,high,reader)
    if selected.get('protocol') == 'gsa':
        selected['device_capacity'] = read_integer('设备池容量',selected.get('device_capacity',10),1,None,reader)
        selected['warmup_devices'] = min(selected.get('warmup_devices',2),selected['device_capacity'])
        selected['device_parallelism'] = read_integer('单台设备允许并发（0=自动随总线程容量）',selected.get('device_parallelism',0),0,None,reader)
        selected['device_rest_after'] = read_integer('每台设备使用多少轮后安排休息（0=关闭）',
                                                    selected.get('device_rest_after',20),0,None,reader)
        selected['device_rest_seconds'] = read_integer('设备每次休息秒数（0=关闭）',
                                                      selected.get('device_rest_seconds',30),0,3600,reader)
    return selected


def main(argv=None):
    parser = argparse.ArgumentParser(description='网页/GSA 共用任务调度入口')
    parser.add_argument('--version',action='version',version=VERSION)
    parser.add_argument('--protocol', choices=('web', 'gsa'))
    parser.add_argument('--config', help='JSON 调度配置；代理 API 另存于本机 config/proxy-api.json')
    parser.add_argument('--processes', type=int)
    parser.add_argument('--threads', type=int)
    parser.add_argument('--proxy-target', type=int, help='首批 IP 数量；0=按线程容量 ×1.5 预取，API 每次最多 200')
    parser.add_argument('--scheme', dest='proxy_scheme', choices=('http','socks5','socks5h'),
                        help='代理传输协议；供应商只返回 ip:port 时必填，隧道式住宅代理用 socks5h')
    parser.add_argument('--retries',type=int,choices=range(0,6),help='初始化阶段的网络失败/超时重试次数（0-5）')
    parser.add_argument('--accounts', help='本机账号文件；邮箱----密码，每行一个')
    parser.add_argument('--debug', action='store_true', default=None)
    parser.add_argument('--demo', action='store_true', help='20 个合成任务，禁止网络，不读取账号文件')
    parser.add_argument('--demo-count',type=int,default=20,help='离线演示任务数，正整数')
    args = parser.parse_args(argv)
    if args.demo_count<1:parser.error('演示任务数必须为正整数。')
    print('FastCheck 控制台 v'+VERSION)
    temp = None
    accounts = []
    try:
        default_config = ROOT/'data/config/task-settings.json'
        values = configuration(args.config or (default_config if default_config.exists() else ROOT/'examples/auth_settings.json'), args)
        if not args.demo and not sys.stdin.isatty():
            raise ValueError('真实任务请在本机终端交互启动。')
        if not args.demo and gsa.mock_enabled():
            raise ValueError('请清除 FASTCHECK_MOCK；真实入口不接受模拟认证。')
        if not args.demo and 'protocol' not in values:
            selected = input('选择通道：1 网页；2 GSA [1]：').strip() or '1'
            if selected not in ('1', '2'):
                raise ValueError('请选择 1 或 2。')
            values['protocol'] = 'web' if selected == '1' else 'gsa'
        root = ROOT/'data'
        if args.demo:
            accounts = [Account(i, f'owned-{i}@example.invalid', 'offline-password') for i in range(1,args.demo_count+1)]
            settings = Settings(**values)
            temp = tempfile.TemporaryDirectory(prefix='auth-task-demo-')
            root = Path(temp.name)
            api_url = 'https://api.example.invalid/?num=200'
            proxy_auth = ''
            serial = 0
            def offline_fetch(url, count):
                nonlocal serial
                values = []
                for _ in range(count):
                    serial += 1
                    values.append(f'http://127.0.0.1:{20000+serial}')
                return values
            kwargs = {'fetcher': offline_fetch, 'handler': demo_attempt, 'registration': demo_registration}
        else:
            account_path = args.accounts or input(f'账号文件路径 [{ROOT/"data/accounts/账号.txt"}]：').strip().strip('"\'') or str(ROOT/'data/accounts/账号.txt')
            accounts = read_accounts(account_path, max_accounts=None)
            print(f'已读取 {len(accounts)} 个不同账号。')
            values = choose_run_settings(values,args)
            values['max_attempts']=1+(args.retries if args.retries is not None else
                read_integer('网络失败重试次数（0=零重试）',values.get('max_attempts',1)-1,0,1))
            if args.debug is None:
                default_debug = values.get('debug', False)
                selected = input(f'是否开启调试模式？[{"Y/n" if default_debug else "y/N"}]：').strip().lower()
                values['debug'] = default_debug if not selected else selected in ('y', 'yes', '是')
            settings = Settings(**values)
            with warnings.catch_warnings():
                warnings.simplefilter('error', getpass.GetPassWarning)
                api_url = prompt_api(root)
                proxy_auth = getpass.getpass('代理连接 user:pass（没有则回车）：').strip()
            kwargs = {}
        stamp = datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y%m%d-%H%M%S')
        directory = ROOT/'data/cli-logs'/f'tasks-{"demo-" if args.demo else ""}{settings.protocol}-{stamp}-{os.getpid()}'
        print(f'通道 {settings.protocol}；{len(accounts)} 个任务；{settings.processes} 个进程 × 每进程 {settings.threads} 个线程。')
        print(f'线程容量 {settings.processes*settings.threads}；首批 IP 目标 {min(settings.proxy_target or len(accounts),len(accounts))}，API 单次上限 200，有效期 {settings.proxy_ttl:g} 秒。')
        if settings.protocol == 'gsa':
            per_device=settings.device_parallelism or settings.processes*settings.threads
            print(f'设备池容量 {settings.device_capacity}；{settings.warmup_devices} 台就绪后启动；单设备并发 {per_device}，总线程容量 {settings.processes*settings.threads}。')
            print(f'设备按使用量和空闲时间分配；每 {settings.device_rest_after} 轮可安排休息 {settings.device_rest_seconds:g} 秒，最多同时休息 {settings.device_rest_slots} 台；0 表示关闭休息。')
        print('主进程统一分配 IP/设备、监督超时并保存结果；20970 和账号拒绝不自动重试。')
        if args.demo:
            print('当前为离线演示：合成账号、代理和设备，不读取真实账号文件，不请求 Apple 或代理 API。')
        supervisor = Supervisor(accounts, api_url, root, directory, settings, proxy_auth=proxy_auth, **kwargs)
        api_url = proxy_auth = ''
        with file_lease(root/'.job.lock'):
            summary = supervisor.run()
        if summary['stop_reason']:
            print('停止原因：'+summary['stop_reason'])
        print('日志目录：'+str(directory))
        print('分类账号导出（含输入密码）：'+str(directory/'exports'))
        return 0 if not summary['stop_reason'] else 1
    except (KeyboardInterrupt, EOFError):
        print('\n已取消。')
        return 130
    except (ValueError,PoolError) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except Exception as exc:
        print(f'任务未启动或未完成：{type(exc).__name__}。', file=sys.stderr)
        return 2
    finally:
        for account in accounts:
            account.password = ''
        if temp:
            temp.cleanup()


if __name__ == '__main__':
    raise SystemExit(main())
