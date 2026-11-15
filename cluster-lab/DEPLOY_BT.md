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

## 新节点快速上线（宝塔）

下面的清单用于把一台全新的 Linux 服务器接入现有总控。示例总控地址是
`http://104.160.36.118:8766`；新节点的 IP、宝塔入口、用户名、密码和节点接入凭证
都只保存在密码管理器或部署记录中，**不要提交到 Git**。

### 1. 安装宝塔

以 `root` 登录新服务器，运行约定的安装器：

```bash
url='https://download.miku.ci/install/install_panel.sh'
if [ -f /usr/bin/curl ]; then
  curl -sSO "$url"
else
  wget -O install_panel.sh "$url"
fi
bash install_panel.sh
```

安装结束后立即保存安装器输出的面板地址、安全入口、用户名和密码，并确认云防火墙与
本机防火墙允许该面板端口。首次登录后修改随机密码；不要把登录资料写进本文档。

### 2. 拉取统一仓库

在宝塔终端执行：

```bash
if command -v dnf >/dev/null 2>&1; then
  dnf install -y git
elif command -v yum >/dev/null 2>&1; then
  yum install -y git
else
  apt-get update
  apt-get install -y git
fi
mkdir -p /www/wwwroot
cd /www/wwwroot
if [ -d faskcheckappid/.git ]; then
  git -C faskcheckappid pull --ff-only origin main
else
  git clone https://github.com/qqwq122825/faskcheckappid.git faskcheckappid
fi
git -C faskcheckappid rev-parse HEAD
```

若最后一条命令不是当前已发布提交，先停止配置项目并检查拉取错误。不要用
`reset --hard` 掩盖服务器工作区问题。

### 3. 创建 Python 节点项目

1. 在宝塔“软件商店”安装 Python 项目管理器。
2. 在“Python 环境管理”创建 Python 3.11 或更高版本环境。
3. 添加 Python 项目，项目名填 `cluster-node`。
4. 项目路径填 `/www/wwwroot/faskcheckappid/cluster-lab`。
5. 使用该项目的虚拟环境安装 `requirements.txt`。
6. 启动用户选 `www`，并保证 `www` 可读代码、可写 `cluster-lab/runtime/`。
7. 启动命令固定为：

   ```bash
   python node.py --host 0.0.0.0 --port 18771 --max-processes 10 --max-threads 100 --allow-http
   ```

8. 环境变量至少设置本机独立的 `CLUSTER_LAB_ADMIN_PASSWORD`（不少于 8 位）。
   代理调试由总控“系统设置”控制；通常不需要在节点上设置
   `CLUSTER_PROXY_DEBUG`。
9. 在宝塔与云防火墙开放 TCP `18771`；节点还需要能主动访问总控和代理服务。
10. 启动项目，确认宝塔显示“运行中”。

总控改为 HTTPS 后，应删除启动参数 `--allow-http`，并在节点连接页填写 HTTPS 地址。

### 4. 创建接入凭证并连接

1. 打开总控“节点配置”，点“添加节点”。
2. 节点名使用能识别机器的名称；默认设置 `10` 进程、`100` 线程。
3. 创建后立刻复制一次性节点接入凭证并安全保存。
4. 打开 `http://<NEW_NODE_IP>:18771`，使用该节点自己的管理员密码登录。
5. 在“连接总控”填写 `http://104.160.36.118:8766` 和刚生成的凭证，点“保存并连接”。
6. 返回总控，确认节点显示在线、可接单，且最近心跳持续更新。

### 5. 上线验收

在新节点执行：

```bash
cd /www/wwwroot/faskcheckappid
git status --short
git rev-parse HEAD
curl -fsS http://127.0.0.1:18771/api/session
```

同时检查：

- `git status --short` 没有输出；
- `/api/session` 返回节点角色和当前版本；
- 宝塔项目状态为“运行中”；
- 总控显示节点在线、可接单，心跳时间持续变化；
- 用一个小任务验证领取、执行、回传和结果确认完整闭环；
- 调试开关关闭时，新工作包不创建 `proxy-debug.jsonl`。

### 6. 后续更新

0.3.38 起节点必须作为 `cluster-node.service` 运行。总控下发目标提交后会自动排空
在途工作、拉取代码、安装依赖、用 systemd 重启并执行健康检查；失败会自动退回旧提交，
不再需要到宝塔页面逐台点“重启”。首次启用自动更新前仍需人工把旧的宝塔进程迁移为
systemd 服务，并确认代码目录是完整 Git 仓库。

### 7. 五分钟复用清单

- [ ] root 登录成功，记录新节点 IP 与 SSH 端口
- [ ] 安装宝塔并保存面板安全入口
- [ ] 克隆统一仓库并核对提交号
- [ ] 创建 Python 3.11+ 环境并安装依赖
- [ ] 创建 `cluster-node`，使用固定启动命令
- [ ] 开放节点管理端口 `18771`
- [ ] 在总控创建 10×100 节点并保存一次性凭证
- [ ] 节点连接总控，心跳与可接单状态正常
- [ ] 小任务闭环通过，调试日志开关符合预期

## 日常更新

开发机改完 Python 后提交推送；改前端时，先在 `cluster-lab/frontend` 执行 `npm ci && npm run build`，把源码和 `clusterlab/static/ui` 构建产物一起提交推送。

### 推送后自动发布

仓库包含 `.github/workflows/deploy-cluster.yml`。完成下面的一次性配置后，向 `main`
推送涉及 `cluster-lab` 的提交即可：GitHub Actions 先通过 SSH 更新总控；总控健康检查
通过后，再把**同一个 commit SHA**按并发上限通知在线节点。离线节点会跳过并在输出中
列出，更新失败的节点保持暂停且自动回滚。

GitHub Environment `production` 需要配置：

- Secrets：`CONTROLLER_HOST`、`CONTROLLER_USER`、`CONTROLLER_SSH_KEY`、
  `CONTROLLER_KNOWN_HOSTS`（预先核对过的服务器主机公钥行，禁止在流水线里临时信任）；
- Variables（可省略并使用默认值）：`CONTROLLER_REPO`、`CONTROLLER_PYTHON`、
  `CONTROLLER_SERVICE`、`CONTROLLER_RUNTIME`、`NODE_UPDATE_PARALLEL`。

默认值分别是 `/www/wwwroot/faskcheckappid`、
`/www/server/pyporject_evn/cluster-controller-env/bin/python`、`cluster-controller`、
`/www/wwwroot/cluster-controller` 和 `2`。总控和节点服务账户必须能读取仓库、写入各自
runtime；用于部署的 SSH 账户必须能执行对应 `systemctl restart`。

### 手动备用发布

若 Actions 不可用，可在总控运行同一发布脚本；它同样会先验证总控，再通知节点：

```bash
cd /www/wwwroot/faskcheckappid
bash cluster-lab/tools/deploy_controller.sh <完整提交SHA>
```

也可以执行带工作区检查的脚本：

```bash
bash /www/wwwroot/faskcheckappid/cluster-lab/tools/update.sh
```

若依赖有变化，可把宝塔对应虚拟环境的 Python **绝对路径**作为唯一参数传给脚本，更新后会安装集群依赖。脚本不会自动重启项目，不执行 `reset --hard` 或删除本地文件；工作区有未提交文件会停止更新。

不要再为每台节点建立定时 `git pull`，也不要让节点自行跟随 `main`；总控只下发本次已
验证的精确提交号。用 root 更新后，需要确保服务用户 `www` 能读取代码、读写本机
`runtime/`，并确认项目使用的环境变量文件仍可被 `www` 读取。

只改总控页面时也会完成一次无害的节点版本对齐；通信协议变化由“总控先更新、节点后
滚动”的顺序保证兼容窗口。节点使用 `systemctl restart cluster-node` 加载新代码，不依赖
宝塔图形界面。

## 已部署服务迁移

旧总控代码在 `/www/wwwroot/cluster-controller/cluster-lab`，旧节点代码在 `/www/wwwroot/cluster-node`。旧节点是 ZIP 解压目录，不能直接 `git pull`。

迁移时先暂停节点接单，等在途工作和待回传归零。停止服务后，保留旧目录作回退，在新完整仓库的 `cluster-lab/runtime/` 中复制原服务的完整 runtime 数据（包括数据库、WAL 和 spool），再修改宝塔项目根目录，保留原虚拟环境、端口、启动参数及环境变量。先确认新服务恢复、旧任务与节点身份均保留，再考虑清理旧代码目录；不要同时运行新旧两份服务。

备份运行中的 SQLite 应使用在线备份接口；文件复制备份应停服后执行。不要只复制正在写入的主库文件。

## ZIP 备用部署

独立 ZIP 仍可用于不使用 Git 的机器。包内根目录就是 Python 项目目录，运行入口与上面相同；这类目录没有 `.git`，后续通过更新 ZIP 维护。

集群程序是独立离线摘要任务平台，没有接入原单机版的账号认证、设备初始化或代理服务。原单机服务 `gsa-console` 不因以上集群部署而改变。
