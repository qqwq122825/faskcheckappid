#!/bin/zsh
cd -- "${0:A:h}" || exit 1
if [[ ! -x .venv/bin/python ]]; then
  print '缺少本地 Python 环境，请先按 gsa_protocol/README.md 安装依赖。'
else
  .venv/bin/python gsa_login.py
fi
print
read -r '?按回车关闭窗口……'
