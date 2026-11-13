#!/usr/bin/env bash
# Run from either role's full Git checkout. Service restart stays in BaoTa.
set -euo pipefail

cluster_source_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$cluster_source_dir/.."
test -d .git || { echo '需要完整 Git 仓库；ZIP 解压目录不能执行 git pull。' >&2; exit 1; }
test "$(git symbolic-ref --short HEAD)" = main || { echo '请先切换到 main 分支。' >&2; exit 1; }
test "$(git config --get remote.origin.url)" = 'https://github.com/qqwq122825/faskcheckappid.git' || {
    echo 'origin 与项目仓库不一致，已停止更新。' >&2; exit 1;
}
test -z "$(git status --porcelain --untracked-files=normal)" || {
    echo '工作区存在未提交文件，已停止更新；请先检查，不会自动覆盖。' >&2; exit 1;
}
git pull --ff-only origin main
if [ "$#" -gt 0 ]; then
    "$1" -m pip install -r "$cluster_source_dir/requirements.txt"
fi
printf '已更新到 %s。请在宝塔重启本机对应的 Python 项目。\n' "$(git rev-parse --short HEAD)"
