#!/usr/bin/env bash
# Deploy controller first, verify it, then queue the exact commit on online nodes.
set -Eeuo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 COMMIT [REPO] [PYTHON] [SERVICE] [RUNTIME] [PARALLEL]" >&2
  exit 64
fi

target=$1
repo=${2:-/www/wwwroot/faskcheckappid}
python=${3:-/www/server/pyporject_evn/cluster-controller-env/bin/python}
service=${4:-cluster-controller}
runtime=${5:-/www/wwwroot/cluster-controller}
parallel=${6:-2}

[[ $target =~ ^[0-9a-fA-F]{7,40}$ ]] || { echo "target must be a commit SHA" >&2; exit 64; }
[[ $parallel =~ ^[1-5]$ ]] || { echo "parallel must be 1..5" >&2; exit 64; }
cd "$repo"
[[ -d .git ]] || { echo "not a Git repository: $repo" >&2; exit 66; }
[[ -x $python ]] || { echo "python is not executable: $python" >&2; exit 66; }
[[ -z $(git status --porcelain --untracked-files=no) ]] || {
  echo "tracked files are dirty; refusing deployment" >&2
  exit 73
}

old=$(git rev-parse HEAD)
changed=0
rollback() {
  code=$?
  if (( changed )); then
    echo "controller deployment failed; rolling back to $old" >&2
    git checkout --detach --force "$old" || true
    "$python" -m pip install -r cluster-lab/requirements.txt || true
    systemctl restart "$service" || true
  fi
  exit "$code"
}
trap rollback ERR

git fetch --prune origin
git cat-file -e "$target^{commit}"
git checkout --detach --force "$target"
changed=1
"$python" -m pip install -r cluster-lab/requirements.txt
systemctl restart "$service"

for _ in $(seq 1 45); do
  if curl --fail --silent --show-error --max-time 3 http://127.0.0.1:18770/healthz \
      | "$python" -c 'import json,sys; value=json.load(sys.stdin); raise SystemExit(0 if value.get("role")=="controller" else 1)'; then
    healthy=1
    break
  fi
  sleep 1
done
[[ ${healthy:-0} == 1 ]] || { echo "controller health check timed out" >&2; false; }

trap - ERR
if ! "$python" cluster-lab/tools/queue_node_updates.py \
    --root "$runtime" --ref "$target" --parallel "$parallel"; then
  echo "controller is healthy at $target, but queuing node updates failed" >&2
  exit 75
fi
echo "controller=$target nodes=queued"
