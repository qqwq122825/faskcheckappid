#!/bin/sh
set -eu
TARGET=${1:?"usage: ROLLBACK.sh /absolute/path/to/project-copy"}
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$TARGET"
git apply --reverse --binary "$SCRIPT_DIR/DIFF_FILE"
printf '%s\n' 'ROLLBACK_RESULT=restored 22px clipped sidebar brand CSS, previous built UI and VERSION 0.3.8'
