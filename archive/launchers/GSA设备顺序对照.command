#!/bin/zsh
cd -- "${0:A:h}" || exit 1
if [[ -x .venv/bin/python ]]; then
  .venv/bin/python gsa_device_order_study.py
else
  print '缺少项目 Python 环境。'
fi
print
read -r '?按回车关闭窗口……'
