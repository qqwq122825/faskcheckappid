#!/usr/bin/env bash
set -euo pipefail
ROOT="${1:-$(pwd)}"
PATCH_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$ROOT"
git apply --reverse --binary "$PATCH_DIR/DIFF_FILE"
printf '%s\n' 'ROLLBACK_RESULT=restored 10-column narrow layout and VERSION 0.3.14'
