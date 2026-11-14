#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

git config --local core.hooksPath .githooks
"$repo_root/deploy/normalize-code-permissions.sh"

printf 'Installed repository hooks: %s/.githooks\n' "$repo_root"
