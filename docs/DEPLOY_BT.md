# FastCheck 控制台 v4.1.0-preview.4 · 宝塔原生部署

本包运行在 Python 3.11+，不需要 Docker。界面沿用左侧任务设置、右侧统计/进度/结果表的布局。网页认证和 GSA 使用同一个总控；默认零重试，单设备可共享并发，设备 2 台就绪即可开始。

这是独立预览版本，不覆盖旧 FastCheck Web v4.0.21。推荐放到新目录 `/www/wwwroot/gsa-console`。

## 1. 上传与安装

将 ZIP 上传至宝塔文件管理的 `/www/wwwroot/` 并解压，得到 `gsa-console/`。确认这一层包含 `app.py`、`install.sh`、`fastcheck/web/`。

在宝塔“Python 环境管理”准备 Python 3.11，然后在终端执行：

```bash
cd /www/wwwroot/gsa-console
bash install.sh python3.11
```

如果命令行找不到 `python3.11`，把第二个参数改成宝塔 Python 环境管理中显示的解释器完整路径。脚本创建项目自己的 `.venv`、安装锁定依赖、离线检查模块及证书，最后让你设置管理密码（至少 12 个字符）。只保存密码哈希。运行用户采用 www 时，root 执行的安装脚本会将 `data/` 交给 www 所有。

已安装环境也可手动操作：

```bash
python3.11 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python app.py --check
.venv/bin/python -m fastcheck.web.bootstrap
```

## 2. 宝塔添加 Python 项目

| 项目项 | 填写内容 |
| --- | --- |
| 项目名 | gsa-console |
| 根目录 | /www/wwwroot/gsa-console |
| Python | 项目 `.venv`，或用下面的绝对路径命令启动 |
| 启动方式 | 自定义命令 / command |
| 启动命令 | `/www/wwwroot/gsa-console/.venv/bin/python /www/wwwroot/gsa-console/app.py --port 8088` |
| 监听端口 | 8088 |
| 运行用户 | www（需对 data/ 有读写权限） |

也可以用 `/www/wwwroot/gsa-console/start.sh` 作为自定义启动脚本。若界面要求“启动文件”，填写 `app.py`。不同宝塔版本字段名略有区别，关键是用本项目 `.venv` 执行该文件。

默认监听 `127.0.0.1:8088`，通过宝塔绑定域名、反向代理到该地址并配置 SSL；示例片段见 `deploy/nginx.example.conf`。不要把 `data/` 作为静态站点目录。没有域名时可通过 SSH 隧道查看。需要直接监听服务器网卡时，可在启动命令加 `--host 0.0.0.0`，并自行配置访问入口及 HTTPS。

不要把 Web 服务的 Gunicorn worker 数改成 8：它固定为 1 个管理 worker，默认 8 个 Web 请求线程（可用 `--web-threads` 调整），认证进程/线程数在页面内设置。任务总控另外启动，页面请求不会执行认证工作。每个服务实例同时管理一个批次，刷新页面可继续查看。

## 3. 登录与测试

用浏览器打开宝塔配置的域名，用户名 **admin**，密码为安装时设置的值。

先点击 **离线演示 20 个任务**，可以验证页面统计、后台多进程、结果表、分类导出和日志下载；演示使用合成账号和代理，不请求 Apple。页面明确标记“离线演示”。

真实任务：上传 UTF-8 文本或粘贴账号，每行 `邮箱----密码`；选择协议、工作进程数、每进程线程数、代理与设备参数，保存代理 API 后启动。API 密钥不回显，后续留空沿用。默认网络重试次数为 0。`20970`、503 等保留为未判断，不直接记成冻结或密码错误。

单设备并发 0 表示跟随总线程容量，例如 8 进程 × 10 线程，可以由 2 台已注册设备共享 80 个在途任务。每个任务新建会话和 SRP 状态，并各自刷新设备参数；服务可能在同一时段返回相同设备参数值，不承诺设备码每次唯一。设置单设备并发为 1，可使用独占模式。新设备加入后优先参与负载分配。

进程、线程、设备数量不再使用原来的 8/32/20 人工上限；请按服务器资源设置。API 单次最多 200 条仍是供应商的限制，库存目标较大时分批补齐。IP 仍按 60 秒有效期淘汰，不能延长供应商给定寿命。首次在 Linux 上使用时没有本地设备身份，需要注册；2 台就绪即开始，后续运行恢复已有身份。

## 4. 导出与数据目录

右侧导出菜单可下载：密码已通过（含双重验证）、仅密正、双重验证、密码错误、锁定、未判断、超时、未开始账号。文件保持 `邮箱----密码` 格式，仅登录后可下载。

- `data/config/`：管理密码哈希、会话密钥、代理 API、设备注册身份。
- `data/jobs/任务ID/`：任务状态、脱敏日志及结果。
- `data/jobs/任务ID/exports/`：含输入密码的分类文件，权限 600；`undetermined-details.jsonl` 另存编号、错误码、阶段、耗时等脱敏明细，含超时。
- 日志 ZIP 不包含账号索引、分类账号文件、代理密钥或设备身份。

网页结果表只显示邮箱及安全结果字段，不显示密码。上传原文件解析后删除；分类导出按用户要求保留原账号密码。导出内容与过程日志分开保存。`GSA_DATA_DIR` 可以指定其他数据目录；不设置时为项目下的 `data/`。

## 5. 常用维护

```bash
# 查看版本或离线自检
.venv/bin/python app.py --version
.venv/bin/python app.py --check

# 手动前台启动（已被宝塔启动时不要重复执行）
./start.sh --port 8088

# 重设管理密码
.venv/bin/python -m fastcheck.web.bootstrap --reset
```

root 重设后，若服务以 www 运行，应恢复 `data/` 的所有权：`chown -R www:www data`。不要同时运行多份 Web 总控指向同一数据目录。正常停止项目会请求结束当前批次；未知在途结果不会自动重跑。

可选环境变量：`GSA_PORT`、`GSA_BIND`、`GSA_DATA_DIR`；HTTPS 入口可设置 `GSA_COOKIE_SECURE=1`。默认上传请求大小为 8MB，可用 `GSA_MAX_UPLOAD_MB` 调整；账号条数默认没有独立硬上限，可用 `GSA_MAX_ACCOUNTS` 自行设置，0 表示不限制。

## 6. 本版验证范围

当前本地 82 个用户自有冻结样本，8 进程 × 10 线程、2 台共享设备、零重试：总耗时 13.88 秒，最大 80 个在途；7 秒约完成 77 个，慢代理/TLS 等使最后几个超过 10 秒。实际抽查代理出口与直连不同。

这测的是这批样本的响应速度，不是正常登录成功率，也不承诺换一台服务器或一批代理必定同样耗时。缩短超时可以更早结束等待，但可能增加未判断，不能把它算作验证更准确。

服务包已在本机用 Gunicorn 完成 HTTP 登录、离线 GSA 任务、停止、分类下载、脱敏日志及浏览器界面检查；Linux 宝塔现场仍需按上面的离线演示进行首次验收。依赖版本和源文件校验值见包内清单。包中没有本地真实账号、代理 API、设备身份和任务历史。

协议及错误分类详见 `docs/PROTOCOLS.md`。`-20970` 不等同于 `-20209`；这两个码保留独立子类。

部署参考：[宝塔 Python 项目说明](https://docs.bt.cn/ai-ops/mcp/tools-reference/)、[Flask Gunicorn 部署](https://flask.palletsprojects.com/en/stable/deploying/gunicorn/)。

## 7. 本地与 Windows

本地测试运行 `python app.py --local`，监听 `http://127.0.0.1:18088/`；首次自动创建 admin/admin123。macOS 可双击项目根目录“启动Web.command”，Windows 双击“启动Web.cmd”。默认测试密码只由本地模式初始化，宝塔部署仍执行安装时设置密码的流程。Windows CLI 见 [WINDOWS.md](WINDOWS.md)。

本版源码集中在 `fastcheck/`，设备、API、账号和历史已归入 `data/`，常规入口不再依赖归档目录。Windows 与本地 Web 使用 Waitress；Linux 默认 Gunicorn，仍不需要 Docker。
