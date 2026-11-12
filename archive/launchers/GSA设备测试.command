#!/bin/zsh
cd -- "${0:A:h}" || exit 1
if [[ ! -x .venv/bin/python ]]; then
  print '缺少本地 Python 环境，请先安装项目依赖。'
else
  .venv/bin/python gsa_devices.py
fi
print
read -r '?按回车关闭窗口……'
