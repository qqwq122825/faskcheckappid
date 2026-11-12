#!/usr/bin/env bash
set -euo pipefail
umask 022
cd -- "$(dirname -- "$0")"
chmod +x start.sh
python_bin="${1:-python3.11}"
"$python_bin" -c 'import sys; assert sys.version_info >= (3,11), "需要 Python 3.11 或更新版本"'
"$python_bin" -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python app.py --check
.venv/bin/python -m fastcheck.web.bootstrap
if [[ "$(id -u)" == "0" ]] && id www >/dev/null 2>&1; then
  chown -R www:www data
fi
printf '%s\n' '安装完成。宝塔自定义启动：项目 .venv/bin/python app.py --port 8088。'
