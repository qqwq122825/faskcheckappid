#!/bin/sh
set -eu
TARGET=${1:?"usage: ROLLBACK.sh /absolute/path/to/project-copy"}
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$TARGET"
git apply --reverse --binary "$SCRIPT_DIR/DIFF_FILE"
printf '%s\n' 'ROLLBACK_RESULT=restored pre-change source, built UI, version, retry UI, node limits, and final-only category behavior'
