# 宝塔原生部署

总控和节点分别使用独立目录，例如：

- 总控：`/www/wwwroot/cluster-controller`
- 节点：`/www/wwwroot/cluster-node`

0.3 版 ZIP 内已包含构建好的 Vue 界面，服务器无需安装 Node.js 或执行 npm。

上传对应 ZIP 并解压，在宝塔 Python 项目管理中为每个角色选择独立的 Python 3.11+ 虚拟环境，安装包内 `requirements.txt`。运行用户需要拥有各自数据目录的读写权限。

## 总控项目

- 根目录：`/www/wwwroot/cluster-controller`
- 启动命令：`python controller.py --host 127.0.0.1 --port 18770`
- 环境变量：`CLUSTER_LAB_ADMIN_PASSWORD`，自行设置至少 8 位管理密码，建议使用 12 位以上随机密码。
- 管理用户名：`admin`。
- 宝塔 Nginx 通过 HTTPS 域名反向代理到 `127.0.0.1:18770`。
- Nginx 与应用都需要允许上传体大小；应用首版上限是 2 GiB。代理 `/agent/` 和 `/api/`，不对外提供源码目录静态访问。
- 使用包内启动入口；不要启动多个总控进程共享同一 SQLite 数据目录。

总控需要向节点提供可访问的 HTTPS 接口。节点主动发起连接，并不代表总控可以完全不开放接口。

## 节点项目

- 根目录：`/www/wwwroot/cluster-node`
- 启动命令：`python node.py --host 127.0.0.1 --port 18771 --max-processes 2 --max-threads 8`
- 环境变量：独立的 `CLUSTER_LAB_ADMIN_PASSWORD`。
- 如需远程查看节点页面，为该管理页配置独立 HTTPS 入口。总控不需要主动连接节点管理页。
- 到总控添加节点，把总控 HTTPS 地址和节点凭证填入节点页面。
- 节点只向配置的总控下载工作包、发送心跳和上传摘要结果。

远程节点默认拒绝明文 HTTP 总控。仅受信内网测试可在节点命令中显式加 `--allow-http`，本机 `127.0.0.1` 通信不需要此参数。

## 数据目录与维护

可用 `--data-dir /srv/cluster-controller` 或 `--data-dir /srv/cluster-node-01` 指定数据目录。每个节点使用自己的数据库和 spool，不共用目录。SQLite 备份请使用在线备份接口或停服后完整备份，不能只复制一个正在写入的主库文件而忽略 WAL。

停止节点接单后等待在途与待回传归零，再重启或迁移节点。节点意外中断后，总控会将过期工作包标为待确认，不能把“节点离线”直接当作“任务没执行”。本版本不自动清理历史输入和结果。

安装包是离线摘要任务平台，不包含原单机软件及其运行配置。
