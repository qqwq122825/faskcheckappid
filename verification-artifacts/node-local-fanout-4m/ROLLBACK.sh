#!/bin/sh
set -eu
TARGET=${1:-"$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)"}
PATCH=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)/DIFF_FILE
cd "$TARGET"
git apply --reverse --check "$PATCH"
git apply --reverse "$PATCH"
printf '%s\n' 'ROLLBACK_RESULT=restored indexed packages to materialized package files, one-package-per-process execution, 1.5-second UI polling, VERSION=0.3.4'
