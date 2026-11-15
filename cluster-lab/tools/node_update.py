#!/usr/bin/env python3
"""Atomic-ish Git updater executed outside cluster-node.service's cgroup."""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import time
from urllib.request import urlopen


def write_status(path, generation, state, message, **extra):
    path = Path(path)
    value = {'generation': generation, 'state': state, 'message': str(message)[:240],
             'updated': time.time(), **extra}
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.parent.mkdir(parents=True, exist_ok=True)
    temporary.write_text(json.dumps(value, ensure_ascii=False, separators=(',', ':')), 'utf-8')
    os.replace(temporary, path)


def run(command, cwd=None, timeout=300):
    return subprocess.run(command, cwd=cwd, text=True, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, timeout=timeout, check=True).stdout.strip()


def resolve_commit(repo, target_ref):
    # After fetch, a plain branch name must prefer origin/<branch>.  Nodes are
    # normally detached, so their stale local "main" must never win over the
    # freshly fetched remote branch.
    candidates = ([target_ref] if target_ref.startswith('origin/')
                  else ['origin/' + target_ref, target_ref])
    for candidate in candidates:
        try:
            return run(['git', 'rev-parse', '--verify', candidate + '^{commit}'], repo, 30)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            continue
    raise RuntimeError('目标 commit / tag / 分支不存在')


def wait_health(timeout=45):
    deadline = time.monotonic() + timeout
    last = 'healthz 未就绪'
    while time.monotonic() < deadline:
        try:
            with urlopen('http://127.0.0.1:18771/healthz', timeout=3) as response:
                value = json.loads(response.read(2048))
            if response.status == 200 and value.get('role') == 'node':
                return value
            last = '健康检查返回内容无效'
        except Exception as exc:  # noqa: BLE001 - recorded as a bounded local error
            last = type(exc).__name__
        time.sleep(1)
    raise RuntimeError('重启后健康检查超时：' + last)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--repo', required=True)
    parser.add_argument('--ref', required=True)
    parser.add_argument('--service', required=True)
    parser.add_argument('--python', required=True)
    parser.add_argument('--requirements', required=True)
    parser.add_argument('--status', required=True)
    parser.add_argument('--generation', required=True, type=int)
    args = parser.parse_args()
    repo = Path(args.repo).resolve()
    status = Path(args.status).resolve()
    if (not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._/-]{0,119}', args.ref)
            or '..' in args.ref or not re.fullmatch(r'[A-Za-z0-9_.@-]{1,80}', args.service)):
        raise SystemExit('invalid update arguments')
    old_commit = ''
    changed = False
    try:
        if not (repo / '.git').is_dir():
            raise RuntimeError('部署目录不是 Git 仓库')
        if run(['git', 'status', '--porcelain', '--untracked-files=no'], repo, 30):
            raise RuntimeError('服务器存在未提交的跟踪文件，已拒绝覆盖')
        old_commit = run(['git', 'rev-parse', 'HEAD'], repo, 30)
        write_status(status, args.generation, 'fetching', '正在从 origin 拉取目标版本',
                     old_commit=old_commit)
        run(['git', 'fetch', '--prune', 'origin'], repo, 180)
        target = resolve_commit(repo, args.ref)
        write_status(status, args.generation, 'installing', '正在切换代码并同步依赖',
                     old_commit=old_commit, target_commit=target)
        run(['git', 'checkout', '--detach', '--force', target], repo, 60)
        changed = target != old_commit
        run([args.python, '-m', 'pip', 'install', '-r', args.requirements], repo, 600)
        write_status(status, args.generation, 'restarting', '正在重启节点服务',
                     old_commit=old_commit, target_commit=target)
        run(['systemctl', 'restart', args.service], timeout=60)
        write_status(status, args.generation, 'verifying', '正在验证节点健康状态',
                     old_commit=old_commit, target_commit=target)
        health = wait_health()
        write_status(status, args.generation, 'success', '更新完成，节点已恢复心跳',
                     old_commit=old_commit, target_commit=target,
                     version=health.get('version'))
    except Exception as exc:  # noqa: BLE001 - rollback must handle every updater failure
        message = f'{type(exc).__name__}: {exc}'
        rollback_error = ''
        if old_commit and changed:
            try:
                run(['git', 'checkout', '--detach', '--force', old_commit], repo, 60)
                run([args.python, '-m', 'pip', 'install', '-r', args.requirements], repo, 600)
                run(['systemctl', 'restart', args.service], timeout=60)
                wait_health()
                write_status(status, args.generation, 'rolled_back',
                             '更新失败，已自动回滚：' + message,
                             old_commit=old_commit)
                return
            except Exception as rollback_exc:  # noqa: BLE001
                rollback_error = f'；回滚失败 {type(rollback_exc).__name__}: {rollback_exc}'
        write_status(status, args.generation, 'failed', '更新失败：' + message + rollback_error,
                     old_commit=old_commit)
        raise


if __name__ == '__main__':
    main()
