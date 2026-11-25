#!/bin/sh
set -eu
cd "$(dirname "$0")"

# CPython 3.11 on macOS collapses under the proven 2×500 socket workload
# (the exact same worker loop measured <2万/hour versus >50万/hour on
# CPython 3.15).  Never silently fall back to the slow runtime.
RUNTIME="${CLUSTER_LAB_PYTHON:-}"
if [ -z "$RUNTIME" ] && [ -x .venv315/bin/python ]; then
  RUNTIME=.venv315/bin/python
fi
if [ -z "$RUNTIME" ]; then
  PY315="$(command -v python3.15 || true)"
  if [ -z "$PY315" ]; then
    echo "需要 Python 3.15：请先安装 python3.15，或用 CLUSTER_LAB_PYTHON 指定运行时。" >&2
    exit 1
  fi
  "$PY315" -m venv .venv315
  .venv315/bin/python -m pip install -r requirements.txt
  RUNTIME=.venv315/bin/python
fi

"$RUNTIME" -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 15) else 1)' || {
  echo "2×500 高并发模式需要 Python 3.15 或更高版本。" >&2
  exit 1
}
exec "$RUNTIME" tools/local_cluster.py
