# 宝塔原生部署与 Git 更新

## 一个仓库，按启动入口选择角色

总控、节点拉取同一个完整仓库，推荐每台机器统一放在 `/www/wwwroot/faskcheckappid`。不要在 `cluster-lab` 内再创建 Git 仓库。

```bash
cd /www/wwwroot
git clone https://github.com/qqwq122825/faskcheckappid.git faskcheckappid
```

目录结构：

```text
/www/wwwroot/faskcheckappid/          ← 在这里 git pull
├── .git/
├── cluster-lab/                     ← 宝塔集群 Python 项目根目录
│   ├── controller.py                ← 总控启动入口
│   ├── node.py                      ← 节点启动入口
│   ├── requirements.txt             ← 集群依赖
│   ├── clusterlab/static/ui/        ← 已构建的界面
│   └── runtime/                     ← 本机数据，不进入 Git
└── …                               ← 原单机项目，独立部署与启动
```

两台服务器都选择 `/www/wwwroot/faskcheckappid/cluster-lab` 为 Python 项目根目录。每个服务使用自己的 Python 3.11+ 虚拟环境，安装这里的 `requirements.txt`，不使用仓库根目录的单机版入口或依赖。服务器无需 Node.js，Git 中已经包含构建好的 Vue 界面。

## 总控项目

- 宝塔项目名可以继续叫 `cluster-controller`；项目名不需要与目录名一致。
- 项目根目录：`/www/wwwroot/faskcheckappid/cluster-lab`。
- 当前总控启动命令：`python controller.py --host 0.0.0.0 --port 8766`。
- 默认数据：`runtime/controller/`，相对于上述集群代码目录。
- 环境变量：`CLUSTER_LAB_ADMIN_PASSWORD`，至少 8 位，管理用户名 `admin`。密码放在宝塔环境变量中，不写进 Git。
- 推荐正式部署用 HTTPS 反向代理；仅监听本机可改为 `--host 127.0.0.1`。总控接口必须能被节点访问。
- Nginx 与应用都需要允许上传体大小；应用上限为 2 GiB。代理 `/agent/` 和 `/api/`，不把源码目录作为静态站点发布。
- 不启动多个总控进程共享同一个 SQLite 数据目录。

## 节点项目

- 宝塔项目名可以继续叫 `cluster-node`。
- 项目根目录：`/www/wwwroot/faskcheckappid/cluster-lab`。
- 当前节点启动命令：`python node.py --host 0.0.0.0 --port 18771 --max-processes 10 --max-threads 100 --allow-http`。
- 默认数据：`runtime/node/`，每台节点保存自己的连接配置、数据库和待回传文件。
- 环境变量：本机独立的 `CLUSTER_LAB_ADMIN_PASSWORD`。
- 节点连接总控一次后，更新代码不需要重新添加节点或重新生成凭证。
- `--allow-http` 是现有明确允许的 HTTP 联调配置；使用 HTTPS 总控时去掉该参数。远程节点默认拒绝明文 HTTP。
- 总控不需要主动连接节点管理页；节点主动领取工作包、发送心跳和回传离线摘要。

同机启动多个节点时，使用不同端口，并分别指定 `--data-dir /独立目录`。

## 日常更新

开发机改完 Python 后提交推送；改前端时，先在 `cluster-lab/frontend` 执行 `npm ci && npm run build`，把源码和 `clusterlab/static/ui` 构建产物一起提交推送。

服务器在仓库根目录更新，然后在宝塔重启对应项目：

```bash
cd /www/wwwroot/faskcheckappid
git pull --ff-only origin main
```

也可以执行带工作区检查的脚本：

```bash
bash /www/wwwroot/faskcheckappid/cluster-lab/tools/update.sh
```

若依赖有变化，可把宝塔对应虚拟环境的 Python **绝对路径**作为唯一参数传给脚本，更新后会安装集群依赖。脚本不会自动重启项目，不执行 `reset --hard` 或删除本地文件；工作区有未提交文件会停止更新。

在宝塔“计划任务”里可保存以上命令作为**停用状态的手动维护任务**，需要更新时点“执行”，检查日志成功后到 Python 项目点“重启”。不要设置自动定时更新。用 root 更新后，需要确保服务用户 `www` 能读取代码、读写本机 `runtime/`，并确认项目使用的环境变量文件仍可被 `www` 读取。

只改总控页面时更新总控；改节点执行代码时更新节点；通信协议变化应按版本说明更新两端。`git pull` 更新磁盘文件，正在运行的 Python 服务需要重启才会加载新代码。

## 已部署服务迁移

旧总控代码在 `/www/wwwroot/cluster-controller/cluster-lab`，旧节点代码在 `/www/wwwroot/cluster-node`。旧节点是 ZIP 解压目录，不能直接 `git pull`。

迁移时先暂停节点接单，等在途工作和待回传归零。停止服务后，保留旧目录作回退，在新完整仓库的 `cluster-lab/runtime/` 中复制原服务的完整 runtime 数据（包括数据库、WAL 和 spool），再修改宝塔项目根目录，保留原虚拟环境、端口、启动参数及环境变量。先确认新服务恢复、旧任务与节点身份均保留，再考虑清理旧代码目录；不要同时运行新旧两份服务。

备份运行中的 SQLite 应使用在线备份接口；文件复制备份应停服后执行。不要只复制正在写入的主库文件。

## ZIP 备用部署

独立 ZIP 仍可用于不使用 Git 的机器。包内根目录就是 Python 项目目录，运行入口与上面相同；这类目录没有 `.git`，后续通过更新 ZIP 维护。

集群程序是独立离线摘要任务平台，没有接入原单机版的账号认证、设备初始化或代理服务。原单机服务 `gsa-console` 不因以上集群部署而改变。
