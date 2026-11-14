#!/usr/bin/env bash
set -euo pipefail

# BaoTa commonly runs Python projects as www while git pull is executed as root.
# A restrictive root umask can leave newly replaced source files at mode 0600,
# which only fails later when multiprocessing starts a fresh www worker.  Keep
# runtime/data files untouched and normalize only versioned program assets.
repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

source_roots=(
  fastcheck
  cluster-lab/clusterlab
  cluster-lab/tools
  tools
  scripts
  deploy
)

for path in "${source_roots[@]}"; do
  [[ -e "$path" ]] || continue
  find "$path" -type d -exec chmod a+rx {} +
  find "$path" -type f \
    \( -name '*.py' -o -name '*.js' -o -name '*.css' -o -name '*.html' \
       -o -name '*.json' -o -name '*.mjs' -o -name '*.sh' \) \
    -exec chmod a+r {} +
done

find . -maxdepth 1 -type f -name '*.py' -exec chmod a+r {} +
find .githooks deploy scripts -type f -name '*.sh' -exec chmod a+rx {} + 2>/dev/null || true

unreadable="$({
  for path in "${source_roots[@]}"; do
    [[ -e "$path" ]] || continue
    find "$path" -type f \
      \( -name '*.py' -o -name '*.js' -o -name '*.css' -o -name '*.html' \
         -o -name '*.json' -o -name '*.mjs' -o -name '*.sh' \) \
      ! -perm -004 -print
  done
} | head -20)"

if [[ -n "$unreadable" ]]; then
  printf 'Unreadable program files remain:\n%s\n' "$unreadable" >&2
  exit 1
fi

printf 'Program file permissions normalized for service workers.\n'
