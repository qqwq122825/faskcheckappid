#!/bin/sh
set -eu
TARGET=${1:?"usage: ROLLBACK.sh /absolute/path/to/project-copy"}
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$TARGET"
git apply --reverse --binary "$SCRIPT_DIR/DIFF_FILE"
printf '%s\n' 'ROLLBACK_RESULT=restored pre-0.3.7 task creation, lease, node device builder, executor slot assignment, UI and version'
