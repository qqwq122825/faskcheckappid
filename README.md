# FastCheck 控制台 · 4.1.0-preview.4

以 Web 为主，CLI 共用同一套多进程调度器。Python 3.11+，支持本地 macOS / Windows 与 Linux 宝塔原生部署，无需 Docker。

## 本地 Web

当前电脑直接打开 **http://127.0.0.1:18088/**，用户名 **admin**，本地测试密码 **admin123**。

下次启动双击根目录 `启动Web.command`（macOS）或 `启动Web.cmd`（Windows）。手动启动：

```bash
python3.11 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python app.py --local
```

Windows 对应解释器为 `.venv\Scripts\python.exe`。保持服务窗口打开，Ctrl+C 可结束服务。首次本地模式自动初始化测试登录，以后沿用已有配置。先点“离线演示 20 个任务”可检查统计、分类导出和调度，不提交真实账号。

## 进程与线程

进程数、每进程线程数只检查正整数，没有 8、32、100 等人工上限，不按 CPU 数或账号数偷偷缩小。填写 100 个工作进程会尝试创建 100 个认证工作进程；创建失败时报错，不改成较小值继续。Web、总控和资源供给会有辅助进程。

线程数为每个进程的线程池容量，8 × 10 最多 80 个在途任务。线程按实际任务懒创建。实际在途还受待办任务、代理供给及你设置的设备共享上限影响。单设备并发 0 跟随总线程容量，2 台就绪即可共享全部工作线程；1 为独占。

设备容量、就绪数量、休息、超时和重试在页面明确设置，无效组合报错。代理 API 单次最多 200 条，较大库存分批提取；60 秒有效期来自代理商。系统资源限制和服务端响应会影响实际运行。

## 源码目录

```text
app.py                 Web 启动入口
cli.py                 共用 CLI
fastcheck/             调度、资源池、协议、Web 页面与服务
  web/                 Web 后端及 static/ 页面
  protocols/           网页和 GSA 协议
  assets/              应用证书
data/                  本机私有数据，不打包
  accounts/账号.txt     原账号文件的新位置
  config/              已保存代理、设备身份、管理登录、CLI 默认配置
  jobs/                Web 任务、脱敏日志及分类导出
  cli-logs/            原有本地日志，历史已保留
scripts/               macOS / Windows CLI 快捷入口
examples/              示例账号及默认调度配置
deploy/                Gunicorn、Nginx 配置
docs/                  部署、CLI 和协议说明
tests/                 离线回归测试
tools/                 打包与本地 HTTP 合成任务检查
archive/               历史脚本、实验及旧文档，不是正式入口
dist/                  发布包
```

原账号、配置、设备身份、日志已搬入 `data/`，没有重注册设备或更改账号密码。Web 和 CLI 共用代理 API 与设备池；同一数据目录同时只运行一个批次。分类导出保留原 `邮箱----密码`，与脱敏日志分开，Web 日志 ZIP 不含分类账号文件。

## CLI 与服务器

单元测试命令用于完整源码目录；发布包可用页面离线演示检查运行。

```bash
.venv/bin/python cli.py --protocol gsa
.venv/bin/python cli.py --protocol web
.venv/bin/python cli.py --protocol gsa --demo --demo-count 100 --processes 5 --threads 10
.venv/bin/python app.py --check
.venv/bin/python -m unittest discover -s tests -v
```

[Windows](docs/WINDOWS.md) · [CLI](docs/TASKS.md) · [宝塔部署](docs/DEPLOY_BT.md) · [协议与分类](docs/PROTOCOLS.md)。服务器入口 `python app.py --port 8088`，先执行 `python -m fastcheck.web.bootstrap` 设置管理密码。本地测试密码不替代服务器初始化流程。

## 验证范围

94 项离线测试通过。真实本地 HTTP 服务运行 1000 个合成任务、8 进程 × 10 线程、2 台共享演示设备，同时 8 路轮询：最大在途 80，结果无重复/遗漏，2266 次请求无失败，响应 P95 3.588ms。报告在 `data/validation/web-load.json`。合成任务不测 Apple 吞吐，不读取真实账号。

Windows 入口、文件锁、时区和进程树回收已适配，Python 3.11 x64 依赖 wheel 可下载，并通过跨平台分支测试。本轮实际系统为 macOS，尚未在真实 Windows 和 Linux 宝塔主机上实跑。

实现参考：[Waitress 平台支持](https://docs.pylonsproject.org/projects/waitress/en/latest/)、[Portalocker 平台行为](https://portalocker.readthedocs.io/en/latest/platforms.html)。

本版修正：未完成时不四舍五入显示 100%；显式 ReadTimeout 参数刷新失败计入超时；设备面板明确区分可用数量与结束快照，并可展开每台设备的派发轮数和在途任务。已有历史任务文件保持原样，修正后的分类用于新任务。

设备数量显示为“可用/目标容量”，同时显示已注册、沿用、本批新增和启动门槛。空池先报告 0/目标，达到启动门槛后开始派发；已有身份直接恢复，启动不会清空或重新注册它们。停止后的剩余账号包含未开始项，反馈速度包含错误和中断，不代表登录成功速度。
