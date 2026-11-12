#!/bin/zsh
cd -- "$(dirname -- "$0")/../.."
.venv/bin/python cli.py --protocol web
read '?按回车关闭窗口……'
