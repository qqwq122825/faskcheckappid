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
        return (json.dumps({'row': row, 'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw)},
                           separators=(',', ':')) + '\n').encode()

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
