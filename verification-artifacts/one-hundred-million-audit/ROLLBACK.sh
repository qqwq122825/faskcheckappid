#!/bin/sh
set -eu
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
TARGET=${1:-$(CDPATH= cd -- "$SCRIPT_DIR/../.." && pwd)}
PATCH_FILE="$SCRIPT_DIR/DIFF_FILE"
cd "$TARGET"
git apply -R --check "$PATCH_FILE"
git apply -R "$PATCH_FILE"
printf '%s\n' 'ROLLBACK_RESULT=restored pre-change sources, tests, README, and built UI assets'
