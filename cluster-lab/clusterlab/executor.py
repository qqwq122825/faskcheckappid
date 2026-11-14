"""The only executor: offline line hashing, with optional synthetic delay.

No plugin loading, account authentication, shell commands or target URLs.
"""
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import multiprocessing as mp
import os
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
            # The override is used by the local soak verifier to keep a large
            # test running long enough to observe dispatch and 1 Hz reports.
            delay = float(os.environ.get('CLUSTER_LAB_SIMULATE_DELAY', '0.004'))
            time.sleep(max(0.0, min(delay, 10.0)))
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
                        atomic_json(directory / 'progress.json',
                                    {'processed': processed, 'started': started,
                                     'counts': {'valid': processed}})
                        last_progress = time.monotonic()
            output.flush()
        if processed != descriptor['rows']:
            raise ValueError('输入条数和工作包描述不一致')
        atomic_json(directory / 'progress.json',
                    {'processed': processed, 'started': started,
                     'counts': {'valid': processed}})
        atomic_json(directory / 'done.json', {'ok': True, 'processed': processed,
                    'counts': {'valid': processed},
                    'checksum': output_digest.hexdigest(), 'elapsed': time.time() - started})
    except Exception as exc:
        atomic_json(directory / 'progress.json',
                    {'processed': processed, 'started': started,
                     'counts': {'valid': processed}})
        atomic_json(directory / 'done.json', {'ok': False, 'processed': processed, 'error': type(exc).__name__})


def _group_child(directory, descriptor, stop):
    runner = execute_auth_unit if descriptor.get('mode') == 'auth' else execute_unit
    runner(directory, descriptor, stop)


def execute_unit_group(directory, descriptor, stop):
    """Use one controller package as a complete node-sized work batch.

    The controller leases at most one package to a node.  This coordinator
    splits that package locally into contiguous ranges, starts the configured
    process count, aggregates their progress, and merges ordered results.  The
    controller therefore keeps only 10,000-row package metadata while a node
    still uses all of its process and thread capacity.
    """
    directory = Path(directory)
    started = time.time()
    rows = max(0, int(descriptor.get('rows') or 0))
    process_limit = max(1, int(descriptor.get('processes') or 1))
    process_count = min(process_limit, max(1, rows))
    if process_count == 1:
        _group_child(directory, descriptor, stop)
        progress = read_progress = {}
        try:
            read_progress = json.loads((directory / 'progress.json').read_text('utf-8'))
        except (OSError, ValueError):
            pass
        read_progress.update(workers=0, worker_limit=process_limit,
                             threads_per_process=int(descriptor.get('threads') or 1))
        atomic_json(directory / 'progress.json', read_progress)
        return

    workers_root = directory / 'workers'
    workers_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    counts = [rows // process_count + (index < rows % process_count)
              for index in range(process_count)]
    child_descriptors = []
    current_row = int(descriptor.get('first_row') or 1)
    with (directory / 'input.part').open('rb') as source:
        for index, count in enumerate(counts):
            child_dir = workers_root / f'{index + 1:03d}'
            child_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
            for name in ('done.json', 'progress.json', 'results.jsonl'):
                (child_dir / name).unlink(missing_ok=True)
            child_input = child_dir / 'input.part'
            with child_input.open('wb') as output:
                child_input.chmod(0o600)
                for _ in range(count):
                    line = source.readline(65537)
                    if not line:
                        raise ValueError('input ended before local fan-out completed')
                    if len(line) > 65536:
                        raise ValueError('input record too long')
                    output.write(line)
            child = dict(descriptor, first_row=current_row, rows=count, processes=1)
            if descriptor.get('mode') == 'auth':
                # 每个进程稳定占用一个槽位。节点可以把连续工作包轮流分配到
                # 30 台等更大的池子，后台注册器同时补齐其余未占用槽位。
                assigned = descriptor.get('device_slots') or []
                child['device_slots'] = [int(assigned[index]) if index < len(assigned)
                                         else index + 1]
                child['device_pool_capacity'] = max(
                    process_count, int(descriptor.get('device_pool_capacity') or
                                       descriptor.get('device_capacity') or process_count))
            atomic_json(child_dir / 'manifest.json', child)
            child_descriptors.append((child_dir, child))
            current_row += count
        if source.read(1):
            raise ValueError('input contains more rows than package descriptor')

    context = mp.get_context('spawn')
    children = []
    try:
        for index, (child_dir, child) in enumerate(child_descriptors):
            child_stop = context.Event()
            process = context.Process(target=_group_child,
                                      args=(str(child_dir), child, child_stop),
                                      name=f'package-worker-{index + 1:02d}')
            process.start()
            children.append((process, child_stop, child_dir))

        last_write = 0.0
        while any(process.is_alive() for process, _, _ in children):
            if stop.is_set():
                for _, child_stop, _ in children:
                    child_stop.set()
            processed = 0
            active = 0
            live_counts = {}
            for process, _, child_dir in children:
                active += int(process.is_alive())
                try:
                    child_progress = json.loads((child_dir / 'progress.json').read_text('utf-8'))
                    processed += int(child_progress.get('processed') or 0)
                    for key, value in (child_progress.get('counts') or {}).items():
                        live_counts[key] = live_counts.get(key, 0) + int(value)
                except (OSError, ValueError):
                    pass
            now = time.monotonic()
            if now - last_write >= 0.25:
                atomic_json(directory / 'progress.json',
                            {'processed': processed, 'started': started,
                             'workers': active, 'worker_limit': process_count,
                             'threads_per_process': int(descriptor.get('threads') or 1),
                             'counts': live_counts})
                last_write = now
            time.sleep(0.05)

        failures = []
        for process, _, child_dir in children:
            process.join()
            done = {}
            try:
                done = json.loads((child_dir / 'done.json').read_text('utf-8'))
            except (OSError, ValueError):
                pass
            if process.exitcode != 0 or not done.get('ok'):
                failures.append(str(done.get('error') or f'worker_exit_{process.exitcode}'))
        if failures:
            raise RuntimeError(failures[0])

        output_digest = hashlib.sha256()
        processed = 0
        merged_counts = {}
        merged_output = directory / 'results.jsonl'
        with merged_output.open('wb') as output:
            merged_output.chmod(0o600)
            for _, _, child_dir in children:
                done = json.loads((child_dir / 'done.json').read_text('utf-8'))
                processed += int(done['processed'])
                for key, value in (done.get('counts') or {}).items():
                    merged_counts[key] = merged_counts.get(key, 0) + int(value)
                with (child_dir / 'results.jsonl').open('rb') as source:
                    for block in iter(lambda: source.read(1024 * 1024), b''):
                        output.write(block)
                        output_digest.update(block)
            output.flush()
            os.fsync(output.fileno())
        if processed != rows:
            raise ValueError('local fan-out result count mismatch')
        atomic_json(directory / 'progress.json',
                    {'processed': processed, 'started': started, 'workers': 0,
                     'worker_limit': process_count,
                     'threads_per_process': int(descriptor.get('threads') or 1),
                     'counts': merged_counts})
        atomic_json(directory / 'done.json',
                    {'ok': True, 'processed': processed, 'counts': merged_counts,
                     'checksum': output_digest.hexdigest(),
                     'workers': process_count, 'elapsed': time.time() - started})
    except Exception as exc:                                      # noqa: BLE001
        for process, child_stop, _ in children:
            child_stop.set()
            process.join(2)
            if process.is_alive():
                process.terminate()
                process.join(2)
        processed = 0
        live_counts = {}
        for _, _, child_dir in children:
            try:
                child_progress = json.loads((child_dir / 'progress.json').read_text('utf-8'))
                processed += int(child_progress.get('processed') or 0)
                for key, value in (child_progress.get('counts') or {}).items():
                    live_counts[key] = live_counts.get(key, 0) + int(value)
            except (OSError, ValueError):
                pass
        atomic_json(directory / 'progress.json',
                    {'processed': processed, 'started': started, 'workers': 0,
                     'worker_limit': process_count,
                     'threads_per_process': int(descriptor.get('threads') or 1),
                     'counts': live_counts})
        atomic_json(directory / 'done.json',
                    {'ok': False, 'processed': processed,
                     'error': f'{type(exc).__name__}: {exc}'[:300]})
    finally:
        for process, _, _ in children:
            process.close()


def _load_fastcheck():
    """cluster-lab 与 fastcheck 在同一仓库的兄弟目录，运行时按需挂载。"""
    import sys
    from pathlib import Path
    # cluster-lab 目录下的 fastcheck 软链接优先：父目录通常只有 x 权限，
    # Python 的 FileFinder 要用 os.listdir 才能发现包，会 ImportError。
    here = Path(__file__).resolve().parents[1]
    candidates = [here, Path(__file__).resolve().parents[2]]
    root = next((path for path in candidates if (path / 'fastcheck').exists()), candidates[-1])
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


def _bump_usage(device_root, counts):
    """每个工作包合并写一次设备计数，并用进程锁防止覆盖。"""
    if not device_root or not counts:
        return
    import portalocker
    path = Path(device_root) / 'usage.json'
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    lock = path.with_suffix('.lock')
    try:
        with portalocker.Lock(str(lock), mode='a+', timeout=10):
            try:
                data = json.loads(path.read_text('utf-8')) if path.exists() else {}
            except (OSError, ValueError):
                data = {}
            for slot, count in counts.items():
                key = str(int(slot))
                data[key] = int(data.get(key) or 0) + int(count)
            atomic_json(path, data)
    except (OSError, portalocker.exceptions.LockException):
        pass


def record_device_registration(device_root, slot, proxy, model=''):
    """Persist the first observed registration facts for one device slot.

    Each slot owns a separate file, so worker processes cannot overwrite one
    another's history. O_EXCL also preserves the original time and proxy when
    the same device is reused by later tasks.
    """
    if not device_root:
        return False
    directory = Path(device_root) / 'registrations'
    path = directory / f'{int(slot):03d}.json'
    try:
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        value = {'slot': int(slot), 'proxy': str(proxy or ''), 'model': str(model or ''),
                 'at': time.strftime('%Y-%m-%d %H:%M:%S'), 'ts': time.time()}
        with os.fdopen(descriptor, 'w', encoding='utf-8') as handle:
            json.dump(value, handle, ensure_ascii=False, separators=(',', ':'))
        return True
    except FileExistsError:
        return False
    except (OSError, TypeError, ValueError):
        return False


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
    capacity = int(descriptor.get('device_pool_capacity') or
                   descriptor.get('device_capacity') or 8)
    configured_slots = descriptor.get('device_slots')
    if isinstance(configured_slots, list) and configured_slots:
        slot_ids = [max(1, min(capacity, int(value))) for value in configured_slots]
    else:
        slot_ids = list(range(1, capacity + 1))
    timeout = float(descriptor.get('timeout') or 20)
    retries = max(0, int(descriptor.get('retries') or 0))
    workers = max(1, int(descriptor.get('threads') or 1))
    counters = Counter()
    processed = 0
    last_progress = time.monotonic()

    pool = DevicePool(device_root, origin, max_devices=capacity)
    from fastcheck.workers import device_profile

    # 一个包内把账号摊到所有设备上：一台设备只承担 rows/capacity 条，
    # 比固定用 1 号设备更分散，明显降低单设备被风控的概率。
    identities, infos = {}, {}
    for index in slot_ids:
        try:
            base_info = pool.info(timeout, proxy=proxy, provider_direct=True)
            profile = device_profile(pool, index)
            infos[index] = dict(base_info, client_info=profile['client_info'],
                                user_agent=profile['user_agent'])
            ready = pool.inspect(index)['state'] == 'ready'
            identities[index] = pool.fetch_headers(index, infos[index], max(timeout, 40),
                                                   registered=ready, proxy=proxy,
                                                   provider_direct=True)
            if not ready and pool.inspect(index)['state'] == 'ready':
                record_device_registration(device_root, index, proxy, profile.get('model', ''))
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
        slot_id = slot_ids[index % len(slot_ids)]
        # 空白行仍是输入文件中的一条记录，但不应触发真实认证请求。
        # 作为未判断结果落盘，保证小包按描述的 rows 正常完成。
        if not line.strip():
            return row, line, {'status': 'undetermined', 'kind': 'blank_record',
                               'slot': slot_id, 'elapsed': 0}
        account_proxy = proxy_for(index)
        identity = identities.get(slot_id)
        try:
            if identity is None:
                # 预取失败的槽位在这里补一次
                info = pool.info(timeout, proxy=account_proxy, provider_direct=True)
                ready = pool.inspect(slot_id)['state'] == 'ready'
                profile = device_profile(pool, slot_id)
                info = dict(info, client_info=profile['client_info'],
                            user_agent=profile['user_agent'])
                identity = pool.fetch_headers(slot_id, info, max(timeout, 40),
                                              registered=ready, proxy=account_proxy,
                                              provider_direct=True)
                if not ready and pool.inspect(slot_id)['state'] == 'ready':
                    record_device_registration(device_root, slot_id, account_proxy,
                                               profile.get('model', ''))
            result = gsa_check(account, password, account_proxy, timeout=timeout,
                               retries=retries,
                               cancel=stop.is_set, identity=identity)
        except Exception as exc:                                   # noqa: BLE001
            return row, line, {'status': 'error', 'kind': type(exc).__name__,
                               'detail': str(exc)[:180], 'slot': slot_id,
                               'elapsed': round(time.time() - account_started, 2)}
        result['elapsed'] = round(time.time() - account_started, 2)
        result['slot'] = slot_id
        return row, line, result

    stop_flag = stop
    output_digest = hashlib.sha256()
    usage_counts = Counter()
    with (directory / 'input.part').open('r', encoding='utf-8', errors='replace') as source, output_path.open('wb') as output:
        output_path.chmod(0o600)
        first_row = int(descriptor.get('first_row') or 1)
        with ThreadPoolExecutor(max_workers=workers) as workers_pool:
            # 只保留 2×线程数的在途 Future；分片上限调到 10 万时也不会
            # 同时构造 10 万个 Future 和 10 万份账号对象。
            pending = deque()
            index = 0
            exhausted = False
            while not exhausted or pending:
                if stop_flag.is_set():
                    raise InterruptedError('租约失效或节点关闭')
                while not exhausted and len(pending) < workers * 2:
                    line = source.readline()
                    if line == '':
                        exhausted = True
                        break
                    line = line.rstrip('\r\n')
                    pending.append(workers_pool.submit(one, first_row + index, index, line))
                    index += 1
                if not pending:
                    continue
                future = pending.popleft()
                row, line, result = future.result()
                status = result_bucket(result)
                counters[status] += 1
                processed += 1
                # sha256/bytes 是总控的必需字段（结果完整性校验），认证模式同样补齐
                raw_line = line.encode('utf-8')
                account, password = _split_account(line)
                device = result.get('slot') or slot_ids[(row - first_row) % len(slot_ids)]
                record = {'row': row, 'account': account, 'password': password, 'status': status,
                          'sha256': hashlib.sha256(raw_line).hexdigest(), 'bytes': len(raw_line),
                          'stage': result.get('stage') or '', 'http': result.get('http'),
                          'code': (result.get('detail') or result.get('kind') or '')[:180],
                          'device': device,
                          'elapsed': result.get('elapsed'), 'attempts': 0}
                encoded = (json.dumps(record, separators=(',', ':')) + '\n').encode()
                output.write(encoded)
                output_digest.update(encoded)
                usage_counts[device] += 1
                now = time.monotonic()
                if now - last_progress >= 0.25:
                    atomic_json(directory / 'progress.json',
                                {'processed': processed, 'started': started,
                                 'counts': dict(counters)})
                    last_progress = now

    if processed != int(descriptor.get('rows') or 0):
        raise ValueError('输入条数和工作包描述不一致')
    _bump_usage(device_root, usage_counts)
    atomic_json(directory / 'progress.json',
                {'processed': processed, 'started': started,
                 'counts': dict(counters)})
    atomic_json(directory / 'done.json',
                {'ok': True, 'processed': processed, 'counts': dict(counters),
                 'checksum': output_digest.hexdigest(),
                 'elapsed': time.time() - started})
