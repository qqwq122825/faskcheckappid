# Windows Web 与 CLI

需要 64 位 Python 3.11+。本轮验证了 Windows Python 3.11 x64 依赖包和跨平台代码分支，没有真实 Windows 主机运行记录。

双击项目根目录“启动Web.cmd”，首次创建虚拟环境、安装依赖，再启动本地 Web。打开 http://127.0.0.1:18088/ ，首次登录 admin / admin123。安装可能需要几分钟，后续启动不会重复安装。

CLI 双击 `scripts\windows\CLI.cmd`，也可在项目根目录手动执行：

```bat
py -3.11 -m venv .venv
.venv\Scripts\python.exe -m pip install -r requirements.txt
.venv\Scripts\python.exe app.py --local

rem 需要 CLI 时在另一个终端执行；先结束当前认证批次
.venv\Scripts\python.exe cli.py --protocol gsa
.venv\Scripts\python.exe cli.py --protocol web

rem 合成任务，不读取真实账号、不请求 Apple
.venv\Scripts\python.exe cli.py --protocol gsa --demo --processes 2 --threads 10
```

启动脚本默认调用 `py -3.11`；如果只安装其他受支持版本，使用对应解释器运行手动命令。不要把 macOS/Linux 的 `.venv` 复制到 Windows。

Windows 使用 spawn 创建工作进程，入口包含 freeze_support。Web 使用 Waitress，不导入 Gunicorn。Portalocker 与 pywin32 提供共享设备租约，taskkill /T 回收进程树，tzdata 提供 IANA 时区。

Windows 设备文件按现有实现使用当前 Windows 用户 DPAPI 保护，不能把该加密状态直接交给另一用户。Unix 的 600/700 权限位不等同于 Windows ACL，请把 data/ 放在自己的用户目录。

进程数和每进程线程数不按系统静默下调，线程按需创建，设置值是容量。资源不足会报告失败，不把填写的 100 自动改成 8。
