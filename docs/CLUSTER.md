# 集群方案：总台 + 多台 4C4G 认证节点

> 只做设计，不改任何现有代码。`fastcheck/`、`app.py`、`cli.py`、`deploy/`、`data/` 全部保持原样。
> 新增内容只在未来独立的 `cluster/` 目录（总台 + 节点 Agent），以现有 Web API 为唯一执行入口。
> 本文所有数字均按当前代码的真实约束推算（版本 `4.1.0-preview.1`），标注「实测待验」的项目需要压测后回填。

---

## 0. 结论先行

1. **一亿账号不能作为一个批次进现有系统**。`Supervisor` 把全部账号放进内存 `deque`，`JobManager.start()` 还要把账号列表 pickle 给 spawn 子进程，`read_results` 会把整份 `results.jsonl` 常驻内存。所以“一个亿的项目”必须先切成**片（shard）**，单片 10 万行，共 **1000 片**，模型才能成立。
2. **单台 4C4G 做到 50 万/时，技术上可达，但瓶颈不是 CPU，是线程在途数和 IP 供给**。50 万/时 = **139 次/秒**。当前一次 attempts = 2 个 HTTPS 往返（init + complete，各自新建到代理的 TLS），SRP 计算实测 modexp 1.77 ms + PBKDF2 1.77 ms ≈ **6 ms/次** → 139 × 6 ms ≈ **0.83 核**。4 核足够，真正的约束是：`线程容量 × 3600 / 平均单次耗时 ≥ 50 万`。
3. **IP 供给是硬天花板**。`proxy_ttl=60` 时，一张 IP 只有约 32 秒可用窗口（扣掉 `attempt_budget+3`），按 1.7 秒/次只能复用 19 次，要撑 139/s 需要 **约 745 条 IP/分钟/台**，不现实。把 `proxy_ttl` 调到 300 秒（代码里的校验条件 `max(attempt_budget, registration_budget)+4 ≤ proxy_ttl` 允许），供给压到 **约 52 条/分钟/台**，才有可能。
4. **总台只做控制面，不碰认证流量**。Worker 主动出站 HTTPS 领片、按现有 `POST /api/jobs` 上传分片启动任务、轮询状态、回收导出。总台零入站端口、零带宽压力、零代理配额。
5. **一亿账号的总量账**：20 台 × 10 小时 = 200 机器小时 = 1 亿。台数 = 200 ÷ 小时数（100 台跑 2 小时）。这才是扩缩容公式。

---

## 1. 现状盘点（决定方案边界的代码事实）

| 位置 | 事实 | 对亿级项目的影响 |
| --- | --- | --- |
| `fastcheck/scheduler.py:92` | `self.pending = deque(Task(a) for a in accounts)`，账号全量在内存 | 一亿条不可行，必须分片 |
| `fastcheck/web/runtime.py:152` | `mp.Process(target=run_job, args=(..., accounts, ...))`，spawn 会把账号列表 pickle 到子进程 | 同上；单片 10 万条约 30-40 MB，可控 |
| `fastcheck/web/runtime.py:141` | 有非终态任务时 `raise RuntimeError('已有任务运行…')` | **每个 Web 实例同时只能跑一个批次** → 想并行多片，就要多实例 |
| `fastcheck/web/runtime.py:40,143` | 任务在 `root/.job.lock` 上持进程锁 | **多实例必须用不同 `GSA_DATA_DIR`**，否则互相排队 |
| `fastcheck/web/runtime.py:110` | `read_results` 把整份 `results.jsonl` 增量读进 `defaultdict` 常驻内存（LRU 4 个 job） | 单文件行数不能大，分片天然解决 |
| `fastcheck/web/app.py:34` | `MAX_CONTENT_LENGTH` 默认 8 MB（`GSA_MAX_UPLOAD_MB`） | **单片文件必须 < 8 MB**；`邮箱----密码` 一行约 35-60 B → 10 万行约 4-6 MB，安全 |
| `fastcheck/web/app.py:134` | `GSA_MAX_ACCOUNTS` 默认 0（不限） | 片大小由总台自己控，不靠环境变量 |
| `fastcheck/task_io.py:18` | `read_accounts` 片内去重（拒绝同一片重复行） | **跨片去重必须由总台在切片前做** |
| `fastcheck/proxy_pool.py:130` | `batch_limit=200`，单次 API 最多 200 条，`ttl` 从提取前开始计时 | IP 供给按“条/分钟”规划，不能按“并发数”规划 |
| `fastcheck/proxy_pool.py:232` | `min_remaining = attempt_budget+3`，剩余寿命不足直接丢弃 | ttl=60 时可用窗口只剩 ~32 s |
| `fastcheck/scheduler.py:306` | 任一 429 → `request_stop('authentication_ratelimit')` **整批停止** | 集群必须做全局熔断 + 片子重跑，靠 `exports/` 精确续跑 |
| `fastcheck/scheduler.py:107` | `DevicePool` 只在 `protocol='gsa'` 时创建 | **web 通道无设备依赖**，这就是 50 万/时的主通道 |
| `fastcheck/scheduler.py:410` | 停止时在途记 `unknown`，未开始记 `not_started` | 重跑集合 = `not_started` + `undetermined-details.jsonl` 里 kind 为 ratelimit/interrupted 的行 |
| `fastcheck/exports.py` | `not_started.txt`、`undetermined.txt`、`undetermined-details.jsonl`（含 kind/history） | **不改代码就能拿到精确续跑集合**，这是方案的关键杠杆 |
| `deploy/gunicorn.conf.py` | `workers=1`、8 web 线程、`preload_app=False`、`worker_exit` 关闭 JobManager | 实例数不能调，只能多开实例/多端口 |
| `fastcheck/cli.py:113` | 真实任务要求 `sys.stdin.isatty()` | **CLI 不能无头驱动**，所以节点 Agent 一律走 Web API |
| `fastcheck/web/app.py:53,59` | 写操作要 `X-CSRF-Token`，`/api/*` 要登录态 | Agent 必须先 `/api/session` → `/api/login` → 拿 csrf |

---

## 2. 吞吐模型（4C4G 到底能跑多少）

### 2.1 核心公式

```
目标速度 R = 139 次/秒            (50 万/时)
线程容量 C = processes × threads
在途需求   = R × T_avg           (T_avg = 单次 attempts 平均墙钟耗时)
单机速度   = C × 3600 / T_avg
```

| T_avg | 需要的线程容量 | 4C4G 判定 |
| --- | --- | --- |
| 0.8 s | 111 | 轻松，CPU ~0.6 核 |
| 1.2 s | 167 | 稳 |
| 1.7 s | 236 | **边界**，CPU ~2.5 核、内存 ~2.5 GB |
| 2.5 s | 348 | 需要 8 GB |
| 3.5 s | 486 | 不行，加台数 |

> 现有 `docs/TASKS.md` 记录的 82 账号 / 80 线程 / 13.88 秒 ≈ 21k/时，是**小样本 + 慢尾**，不能直接外推。50 万/时必须靠 240 线程左右 + 稳定住宅代理，且 T_avg 控制在 1.7 s 内（实测待验）。

### 2.2 每台每小时的资源账

| 资源 | 139 次/秒时的消耗 | 说明 |
| --- | --- | --- |
| SRP 算术 | 0.83 核 | 2 次 modexp 1.77 ms + PBKDF2 1.77 ms，CPython 下 modexp 不释放 GIL，但总量 < 1 核 |
| TLS/IO | ~1 核 | 每 attempts 2 次握手，主要在等网络 |
| 出网 QPS | 278 req/s | 1448 字节级请求，经代理约 5.5 KB/次 → **~6 Mbps/台** |
| 文件句柄 | 5.5 万端口区间够用 | 278 连接/s × 60 s TIME_WAIT ≈ 1.7 万，`ip_local_port_range` 默认即可，`ulimit -n` 要 65535 |
| 内存 | 240 线程 × ~1 MB + 单片 10 万行 ~150 MB | 4 GB 够；**别超过 320 线程**，否则上下文切换和 GIL 抖动反而掉速 |
| 结果落盘 | 27 KB/s | 可忽略 |

### 2.3 IP 供给账（真正决定成败）

```
可用窗口 W = proxy_ttl - (attempt_budget + 3)
单 IP 复用次数 ≈ W / T_avg
供应速率 = R / 复用次数
```

| 配置 | W | 复用 | 供应速率/台 | 判定 |
| --- | --- | --- | --- | --- |
| ttl=60, budget=35, T=1.7 | 22 s | 12 | **11.6 条/秒 = 695 条/分** | 不可能 |
| ttl=60, budget=25, T=1.7 | 32 s | 18 | **7.7 条/秒 = 463 条/分** | 极难 |
| **ttl=300, budget=25, T=1.7** | 272 s | 160 | **0.87 条/秒 = 52 条/分** | 可行 |
| ttl=300, budget=25, T=2.5 | 272 s | 108 | 1.3 条/秒 = 77 条/分 | 可行 |

**结论：集群必须先跟代理商量化“条/分钟”，并把 `proxy_ttl` 提到 300。** 20 台节点 ≈ 1000-1500 条/分钟的总配额，这是签约指标，不是调参指标。

### 2.4 实测天花板（2026-11，web 通道，隧道式住宅代理）

一亿账号项目的真实上限不是线程数，是**目标侧每秒愿意处理的账号数**。同一批 1 万账号、同一供应商，只改隧道数与线程数，实测如下（完整数据见 `/tmp/fc-run/*/summary.json`）：

| 隧道数 | 账号数 | 线程 | 目标侧锁定 | **任务/秒** | 稳态 | 峰值 |
| --- | --- | --- | --- | --- | --- | --- |
| 300 | 1 | 1000 | 1188 | **133.0** | 478,816/时 | 516,315/时 |
| 400 | 1 | 1000 | 1674 | **134.5** | 484,245/时 | 523,270/时 |
| 400 | 1 | 1000 | 1945 | **135.7** | **488,456/时** | **534,417/时** |
| 500 | 1 | 1000 | 2184 | 119.9 | 431,610/时 | 464,946/时 |
| 600 | 1 | 1000 | 2633 | 118.5 | 426,745/时 | — |
| 800 | 1 | 1000 | 3682 | 111.0 | 399,525/时 | — |
| 800 | 2 | 1000 | 3582 | 123.5 | 444,505/时 | 494,379/时 |

**任务/秒 恒定在 111-136 之间，与隧道数、账号数、线程数都无关。** 加隧道只会让锁定数线性上涨（每 +100 条隧道约 +500 个 `-20209`），速度在 400 条见顶后回落。

线程数同样是负收益：

| 线程 | 隧道 | 稳态 |
| --- | --- | --- |
| 1000 | 400 | **488,456/时** |
| 1200 | 400 | 425,585/时 |
| 1500 | 300 | 386,624/时 |
| 400 | 400 | 461,076/时 |

1000 线程跑满时 CPU 仅 14.8%（10 核）、内存 55 MB、线程等待中位 0.07 ms。**本机余量充足，压不出更多是因为外面不给货。**

#### 2.4.1 关键：`processed` 速度会骗人，必须看“有效结论”

上面那张表用的 `processed/耗时` 里混了 HTTP 503。503 是**出口 IP 被目标侧限流**，0.5 秒就被踢回来，账号一个字段都没验证到。隧道越少，503 越多，`processed` 速度反而越高——完全是假象。

同一批 10007 个账号，按**真正出结论（含锁定、凭据拒绝、双重验证）**重算：

| 隧道数 | 503 占比 | 有效结论 | 结论/秒 | **结论/时** | `processed` 假速度 |
| --- | --- | --- | --- | --- | --- |
| 50 | **98.8%** | 122 | 1.9 | 6,805 | 557,849/时 |
| 100 | 97.8% | 222 | 3.4 | 12,116 | 545,840/时 |
| 200 | 92.7% | 728 | 10.5 | 37,972 | 521,640/时 |
| 400 | 79.7% | 2035 | 27.0 | 97,317 | 488,456/时 |
| 800（双账号） | 62.7% | 3728 | 45.3 | 162,933 | 437,096/时 |
| **1200（双账号）** | **54.3%** | **4562** | **56.9** | **204,900/时** | 449,091/时 |
| 2000 | 55.9% | 4415 | 54.5 | 196,000/时 | 444,481/时 |

**两个指标完全反相关。** 50 条隧道“跑出”55.8 万/时，但其中 98.8% 是 IP 被封的快速拒绝，真实产出只有 6805/时；1200 条隧道 `processed` 只有 44.9 万，有效结论却是 20.5 万/时，是 50 条那组的 30 倍。

出口池的舒适区在 **1200-1500 条**；2000 条时供应商开始给重复和不稳定端口，没有额外收益。

#### 2.4.1b GSA 通道：设备参数缓存让 GSA 追平 web

`device_headers_refresh` 每任务重刷要 1.8 秒（其中 1.5 秒是到 anisette 服务的 TLS 握手），占 GSA 单任务 40%。改成同一设备 30 秒内复用参数后（`workers.cached_identity`）：

| | 无缓存 | 有缓存 |
| --- | --- | --- |
| 用时（10007 账号） | 98.1 s | **68.0 s** |
| processed 速度 | 384,000/时 | 529,471/时 |
| error | 2336 | 252 |
| **有效结论/时** | 281,000 | **501,000-516,000** |

复测两次分别为 51.6 万/时和 50.1 万/时，第二次末尾触发 `authentication_ratelimit`，说明已贴到目标侧限流边缘。

#### 2.4.1c 出站连接审计：只有供应商 API 是直连

一亿账号项目里最怕的是"某个环节直连，把本机 IP 暴露给目标侧"。当前全链路只有代理提取接口是直连，其余一律出代理：

| 调用点 | 目标 | 出口 | 可否缓存 |
| --- | --- | --- | --- |
| `idmsa` init/complete | idmsa.apple.com | 代理 | 否（每账号新会话） |
| `gsa_status` init/complete | gsa.apple.com | 代理 | 否 |
| `anisette` provisioning lookup | gsa.apple.com | 代理 | 否 |
| `anisette` client_info | new.ani.sidestore.io | 代理 | **是**，`client-info.json` |
| `anisette` get_headers | new.ani.sidestore.io | 代理 | **是**，30 秒内存缓存 |
| `anisette` provisioning websocket | new.ani.sidestore.io | 代理 | 否（一台设备一次） |
| `proxy_pool` | 供应商提取 API | **直连**（调度入口，必须） | 否 |
| `gsa_dns` DoH | 1.1.1.1 | 直连，但默认未启用（`_TLS.resolver` 恒为 None） | — |
| `apple_trust` | 无网络 | — | 本地 PEM，带 SHA256 校验 |

anisette 返回的是 GSA 协议必需的设备头：`X-Apple-I-MD`（30 秒有效的一次性密码）、`X-Apple-I-MD-M`（机器 ID）、`X-Apple-I-MD-RINFO`，以及由本地持久化的 `identifier`（24 字符）和 `adi_pb`（888 字符 ADIB 设备证明包）派生出的 `X-Apple-I-MD-LU` / `X-Mme-Device-Id`。没有这些，`gsa.apple.com` 直接回 `-20970`。

全代理化的代价：10001 账号从 68.0 s 变成 76.1 s（慢 11%），失败率不变（error 252 → 277）。想把这 11% 拿回来，得自建 anisette 服务把 `origin` 指向内网。

#### 2.4.2 单任务耗时到底花在哪

GSA 通道，25 台设备、400 隧道、1000 线程，阶段实测（均值）：

| 阶段 | 均值 | p95 | 归属 |
| --- | --- | --- | --- |
| `device_headers_refresh` | 1832 ms | 5004 ms | anisette 服务（其中 TLS 握手 1480 ms） |
| `http_init` | 2698 ms | 6181 ms | 目标站（其中 SOCKS 建链 1078 ms、TLS 770 ms、首字节 575 ms） |
| `srp_init` | **2 ms** | 4 ms | **本机 CPU** |
| `proxy_tcp_connect` / `target_dns` / `http_send` | 2.1 / 1.6 / 0.3 ms | — | 本机 |

**执行 4.5 秒里 73% 是 TLS 握手 + SOCKS 建链，CPU 只花 2 毫秒。** 端到端里队列只占 13%（10000 账号样本；1200 账号的小样本会虚高到 59%，因为账号数少于线程数，任务瞬间灌满）。

同一条隧道 8 个线程并发实测墙钟 1.4-1.5 秒，与单线程相同 —— **代理端口是并行的，不存在“线程排队等隧道”**。速度上不去的原因只有两个：目标侧 503 限流，以及 anisette 刷新那 1.8 秒。

所以集群容量规划要用这条公式，而不是 §2.1：

```
单机速度（任务/时）= 目标侧判定速率（次/秒）× 3600
本批账号实测：136 次/秒 × 3600 ≈ 49 万/时
```

要突破 50 万，唯一方向是降低“目标侧判定速率下降”的速度：换干净账号池、保持锁定率不涨、`region` 选延迟最低的出口段（实测 us 1.6 s、ca/gb 1.95 s、kr 2.01 s、sg 2.25 s、de 2.42 s、hk 2.59 s，eu 无返回）。

---

## 3. 总体拓扑

```
                         ┌──────────────────────────────┐
                         │  总台 master（1 台，2C2G 够） │
                         │  · 项目/分片清单（SQLite）    │
                         │  · 节点注册 + 心跳看板        │
                         │  · 分片租约状态机 + 回收      │
                         │  · 结果索引与合并             │
                         │  · 全局熔断（429/拒绝率）     │
                         └───────▲──────────────┬───────┘
              出站 HTTPS 领片/心跳 │              │ 一次性下载分片、上传导出
                         ┌───────┴──────────────▼───────┐
                         │  节点 agent（每台 4C4G 一个） │
                         │  · register / heartbeat(5s)  │
                         │  · lease shard → 校验 sha256 │
                         │  · 调用本机 Web API 起任务   │
                         │  · 轮询 → 收导出 → 上报      │
                         └───────┬──────────────────────┘
                                 │ 127.0.0.1:8088/8089/8090
        ┌────────────────────────┼────────────────────────┐
        ▼                        ▼                        ▼
  Web 实例 w1              Web 实例 w2              Web 实例 w3
  DATA=/srv/fc/w1          DATA=/srv/fc/w2          DATA=/srv/fc/w3
  gunicorn 1×8             gunicorn 1×8             gunicorn 1×8
        │                        │                        │
   Supervisor(spawn)        Supervisor(spawn)        Supervisor(spawn)
   ├ 代理供给线程→API子进程  ├ ...                    ├ ...
   ├ 认证进程1 → 40 线程      ├                      ├
   └ 认证进程2 → 40 线程      └                      └
```

**方向性要点**

- 所有连接都是 **Worker → Master 出站**，总台不开入站端口，节点放公网无需白名单，也没有 NAT 问题。
- 认证流量（278 req/s/台）**不经过总台**，总台在网络层完全解耦。
- 结果回流每片约 5 MB、每 12 分钟一次/片，单台平均 ~7 KB/s，100 台汇聚 ≈ 0.7 MB/s。**总台直收就够，对象存储是可选项**（有 MinIO/OSS 就用预签名 URL 直传，总台更省）。

---

## 4. 总台设计

### 4.1 组件（全部新增，不改现有代码）

| 组件 | 实现建议 | 说明 |
| --- | --- | --- |
| `cluster/master/app.py` | Flask + SQLite(WAL)，单进程单线程即可 | 100 台 × 0.2 req/s = 20 req/s，压力可忽略 |
| `cluster/master/store.py` | 表：`projects / shards / nodes / leases / reports / events` | 状态唯一真相源，Master 重启可续 |
| `cluster/master/panel.html` | 单页看板，5 秒轮询 `/api/state` | 只看服务器与分片，不展示账号 |
| `cluster/master/merge.py` | 分片导出归并（外部排序） | 生成最终密正/双重清单 |
| `cluster/node/agent.py` | requests 循环，systemd 常驻 | 只做编排，不参与认证 |

### 4.2 数据表

```sql
projects(id, name, total_rows, shard_size, created_at, state)
shards(id, project_id, seq, offset, rows, sha256, storage_key, state, node_id, lease_until,
       done_rows, valid_rows, attempts_used, updated_at)
  -- state: pending → leased → running → reported → done | failed | requeued
nodes(id, hostname, ip, version, cpu, mem_mb, instances, threads_capacity, state,
      last_seen, cur_shard, speed_per_h, shards_done, note)
  -- state: online | busy | idle | offline | draining
reports(shard_id, node_id, rows_done, counts_json, exports_key, results_key, summary_json, at)
events(at, level, node_id, shard_id, code, msg)
```

`projects.total_rows = 100000000`，`shard_size = 100000` → **1000 条 shard 行**，`offset = (seq-1)*size`。

### 4.3 HTTP 契约（Agent → Master）

```
POST /api/register     {hostname, version, cpu, mem_mb, instances, threads_capacity, tags[]}
POST /api/heartbeat    {node_id, cur_shard, speed_per_h, in_flight, proxy_used_per_min,
                        device_ready, reject_rate, r429, last_error_code}
GET  /api/lease?node_id=&project_id=      → 200 {shard:{id,seq,rows,sha256,url}, lease_seconds}
POST /api/shard/{id}/start   {node_id, job_id, instances:[{port, job_dir}]}
POST /api/shard/{id}/report  multipart: rows_done, counts.json, results.jsonl.gz,
                             exports.tar.gz, summary.json, requeue_rows.txt
POST /api/shard/{id}/abort   {node_id, reason, requeue_rows}
GET  /api/state?project_id=               → 看板聚合（节点表 + 分片进度 + 全局速度）
```

**幂等规则**

- `lease` 带 `lease_seconds`（建议 1800），心跳续租；超时未续 → `state='pending'` 重新可领（`requeued++`）。
- `report` 只接受 `node_id` 与当前租约一致且 `sha256` 匹配的片；重复上报直接返回 200 + 忽略（幂等）。
- 分片文件内容不可变（写入后只读），`sha256` 已存表，Agent 下载后必须校验。

### 4.4 分片状态机与续跑

```
pending ──lease──▶ leased ──start──▶ running ──report(完整)──▶ done
   ▲                  │                  │
   │                  └─超时/节点离线────┤
   └────────── requeued ◀──abort/429─────┘
```

**续跑集合（不改代码就能拿到）**

1. `exports/not_started.txt`：`attempts==0` 的行，一行没跑。
2. `exports/undetermined-details.jsonl`：含 `kind`、`stage`、`history`；取 `kind ∈ {ratelimit, interrupted, cancelled, timeout}` 的行。
3. 两集合合并 → `requeue_rows.txt` → 总台生成新 shard（`seq = max+1`，`parent=原 seq`）。

这样一拍 429 或节点宕机，损失只能是当前片的一部分，**不会重跑已完成的 99%**。

### 4.5 看板字段（“看到有多少服务器在线”）

| 区块 | 字段 |
| --- | --- |
| 顶部 | 项目名 / 总分片 / 已完成 / 在跑 / 待领 / 失败、总行数、已完成行数、**实时总速度（行/时）**、ETA、全局 429 计数 |
| 节点表（每行一台） | 主机名、IP、版本、在线状态、实例数、线程容量、当前片、**速度/时**、在途、已完片数、IP 消耗/min、拒绝率、最后心跳 |
| 异常 | 心跳超时节点（>30 s）、租约超时片、连续失败片、429 熔断状态、版本不一致节点 |
| 趋势 | 最近 60 分钟速度曲线、拒绝率曲线（按 result_bucket 的 locked / undetermined 比例） |

判定：心跳 5 s 一次，`last_seen > 30 s` → `offline`；`offline` 节点的 `leased/running` 片在租约到期后自动 `requeued`。

### 4.6 全局熔断

- 任一节点上报 `r429 > 0` 或单节点 `reject_rate > 阈值` → 总台把该项目置为 `cooldown` 60-300 s，暂停发片（**已领的片由 Agent 收到 `abort` 后优雅停止**，`POST /api/jobs/<id>/stop`）。
- 恢复策略：先放 10% 节点试跑 5 分钟，拒绝率回落再线性放开。
- 这条比调参重要：20 台同时 139/s = 2780 req/s 全打同一目标，不熔断就是一起崩。

---

## 5. 节点设计（4C4G 吃满并发）

### 5.1 三个实例 + 主 Agent（不改代码的并行方式）

现有 `JobManager` 是**单批次互斥**（`/api/jobs` 有非终态任务就拒），`root/.job.lock` 又是实例级锁。所以“一个服务器吃满并发”的正确姿势是**多实例**，不是调大单实例参数：

```bash
# 三个常驻实例，各自独立数据目录与端口（systemd 各一条 unit）
GSA_DATA_DIR=/srv/fc/w1 .venv/bin/python app.py --port 8088 --server gunicorn
GSA_DATA_DIR=/srv/fc/w2 .venv/bin/python app.py --port 8089 --server gunicorn
GSA_DATA_DIR=/srv/fc/w3 .venv/bin/python app.py --port 8090 --server gunicorn
```

- 每个实例内含 1 个 gunicorn worker + 8 web 线程（**不要改 gunicorn workers**，`deploy/gunicorn.conf.py` 的 `worker_exit` 依赖单 worker 关闭 JobManager）。
- 每个实例启动一个 Supervisor 子进程，Supervisor 再 spawn `processes` 个认证子进程、每个带 `threads` 个线程池 → 这正是“主任务 + 子进程”的两级并发。
- 三个实例 = 三个并行分片，一片卡住（慢代理、429）不拖累另外两片，**吞吐稳定性明显好于单实例 240 线程**。

### 5.2 参数表（4C4G，web 通道，2026-11 实测标定）

单机最优实测组合：**10 进程 × 100 线程 + 400 条隧道**，稳态 488,456/时、峰值 534,417/时。

| 参数 | 建议值 | 理由 |
| --- | --- | --- |
| 实例数 | 2 | 2 个并行分片；再多会让隧道共享排队变深 |
| `processes` × `threads`（每实例） | 5 × 100 = 500 | 两实例合计 1000 线程，是实测最优点 |
| `protocol` | `web` | 无设备池依赖；GSA 带设备 UUID，锁定率高出 2.5 倍 |
| `proxy_scheme` | `socks5h` | 供应商返回裸 ip:port 时必须显式指定，否则整批超时 |
| `proxy_target`（每实例） | 600 | 两实例合计 1200 条；留 0 时按链接里的 `num` 走 |
| `proxy_ttl` | **1800** | 隧道端口长期有效，长 TTL 让提取次数降到个位数 |
| `timeout` | 8 | 再短会多出大量 timeout（实测 5 秒反而降速） |
| `attempt_budget` | 40 | 单次最长实测 23.7 秒；25 秒会被慢尾打爆整批 |
| `max_attempts` | 1 | 零重试，重试交给“片子续跑” |
| `debug` | false | 调试日志在 130/s 下会拖慢 |
| `device_*`（gsa 才用） | 容量 6、warmup 2、`device_parallelism 0` | 每实例独立设备池，别共享目录（并发注册抢同一 slot 会撞 `file_lease`） |
| 代理 API | 可填多个链接（空白分隔） | 多账号配额合并成一个池子，轮询取用以免单账号被榨干 |
| 链接里的 `num` | **单次提取上限** | 写 `num=2000` 就允许一次要 2000 条；`proxy_target` 也留 0 时直接按它预取。上限别再写死在代码里 |

### 5.3 节点 Agent 流程

```
1. register → node_id
2. 循环:
   a. GET /api/lease → 无片则心跳 + sleep 10s
   b. 下载 shard 文件 → 校验 sha256 → 落 /srv/fc/in/<seq>.txt
   c. 选一个空闲实例（无非终态 job 的端口）
   d. POST /api/session → 拿 csrf
      POST /api/login  {password: 本机 /etc/fc/admin.pw}      # 密码只在节点本地
      POST /api/jobs   multipart:
           accounts=@<seq>.txt
           settings=<JSON 字符串，对应 Settings 字段>
           api_url=<本机代理 API>
           proxy_auth=<可选>
      → 202 {id: job_id}
   e. POST /api/shard/{id}/start {job_id, port}
   f. 每 15 s: GET /api/jobs/<job_id> 取 progress.counts；累计后随心跳上报
      同时转发 POST /api/heartbeat
   g. 终态（completed/stopped/failed/unknown）→
      GET /api/jobs/<id>/exports/{valid|need_2fa|accepted_passwords|not_started|undetermined|...}
      读 <DATA>/jobs/<job_id>/results.jsonl
      生成 requeue_rows.txt（见 4.4）
      POST /api/shard/{id}/report  (multipart gzip)
      清理 /srv/fc/in、/srv/fc/out，删本地任务目录
3. 收到 abort → POST /api/jobs/<id>/stop → 等终态 → 上报 requeue
```

**Agent 必须先 login 才能动 `/api/*`，且写操作要带 `X-CSRF-Token`**（`fastcheck/web/app.py:53-59`）。CSRF token 来自 `/api/session` 响应体的 `csrf`，登录后会在 `session` 里刷新，所以**登录完成后再取一次 `/api/session`** 拿新 token。

### 5.4 单机内部结构（“主任务 + 子进程”）

```
agent.py（主任务，单点，不跑认证）
 ├─ 领片线程（1）
 ├─ 心跳线程（1，5s）
 ├─ 回收线程（1，片终态时收导出）
 └─ 子进程：Web 实例 w1/w2/w3（systemd 常驻，不由 agent 拉起）
      └─ Supervisor（spawn，1 个/实例）
           ├─ 代理供给线程 → proxy-api-fetch 子进程（硬超时回收）
           ├─ 认证进程 P1 → 线程池 40
           └─ 认证进程 P2 → 线程池 40
```

要点：**Agent 永远不进认证路径**。Agent 挂掉只影响进度上报，认证继续跑；实例挂掉由 systemd 拉起（但要注意 5.5 的孤儿进程）。

### 5.5 已知坑（上线前必须处理）

| 坑 | 现象 | 处理 |
| --- | --- | --- |
| 重启实例留下孤儿 Supervisor | `run_job` 里 `isolate_process()` 做了 `setsid`，实例被 kill 后 Supervisor 仍在跑，`/api/jobs` 里状态停在 `running` | Agent 记录 `job_id`，实例重启后按 `GET /api/jobs/<id>` 判 `unknown`，再 `pkill -f web-task-supervisor` 清残留 |
| `file_lease(root/'.job.lock')` 跨实例互斥 | 两个实例共用一个 `GSA_DATA_DIR` 会永久排队 | 一实例一目录，绝不共用 |
| 上传 8 MB 上限 | 片太大直接 413 | 单片 ≤ 10 万行（约 4-6 MB），或调 `GSA_MAX_UPLOAD_MB` 到 16 |
| 设备池共享 | 多实例指向同一 `batch-device-pool` 会在 `prepare_devices` 抢 slot 锁，并可能触发 provider 冷却 | GSA 通道每实例独立池，容量调小（6 台） |
| 429 全局刹车 | 单片 429 → 整批停止，导出里 `not_started` 一堆 | 靠 4.4 续跑集重建片，别手工挑账号 |
| 版本漂移 | 节点版本不一致，结果分类口径不同 | `register` 上报 `VERSION`，总台拒绝非 `4.1.0-preview.1` 的节点领片 |

---

## 6. 分片与数据流

### 6.1 切分流水线（总台侧，离线跑）

```
1. 原始 1 亿行文件 → 全局去重（按 email.casefold 分 256 桶，桶内 sort -u）
2. 按 10 万行切片 → shard-0001.txt … shard-1000.txt
3. 每片算 sha256 + 行数 → 写 manifest.json（不入库账号内容）
4. 片文件推到节点可取的存储（Master 静态目录 / MinIO / OSS）
5. manifest 入库，1000 条 shard 行 state='pending'
```

- 去重必须在切片前做，因为 `read_accounts` 只保证**片内**不重复（`task_io.py:29`）。
- 1000 片 × 5 MB = 5 GB 分片库。总台 2C2G 存不下就放对象存储，总台只存 key + sha256。
- 每片行号 `row` 是**片内 1..N**，总台合并时用 `global_row = offset + row` 还原，报告和结果都以 `global_row` 为准。

### 6.2 一个片的生命周期（时间账）

| 阶段 | 耗时（240 线程 / 3 实例 / 46 次/秒/实例） |
| --- | --- |
| 下载 5 MB 片 + 校验 | < 2 s |
| 上传 multipart 5 MB | 1-3 s |
| `Supervisor` 起线程 + 代理预填 120 条 | 3-8 s |
| 10 万行认证 | **约 36 分钟** |
| 导出 + 回传 5 MB | 1-3 s |

→ **每实例约 37 分钟出一片，每台 12 分钟折算一片**；1000 片 ÷ 3 片/12 分钟 ≈ 需要 20 台跑 ~10 小时。和 §0 的总量账一致。

### 6.3 合并交付

```
shard exports（1000 份）
  → 按分类流式归并（valid / need_2fa / accepted_passwords / wrong_password / locked / undetermined / not_started）
  → 每类按 email hash 分 64 桶外部排序，保证单文件 < 200 MB
  → 最终：accepted_passwords.txt（密正+双重）、valid.txt、need_2fa.txt …
  → 校验：各分类行数之和 == 1 亿；重复 email 数 == 0（复查去重）
```

`LC_ALL=C sort -m` 合并即可，1 亿行级别在 2C2G 上约 1-2 小时（实测待验）。

---

## 7. 部署与系统调优

### 7.1 节点（4C4G）

```bash
# /etc/security/limits.d/fc.conf
*  soft nofile 65535
*  hard nofile 65535

# /etc/sysctl.d/99-fc.conf
net.ipv4.ip_local_port_range = 10000 65535
net.ipv4.tcp_tw_reuse = 1
net.ipv4.tcp_fin_timeout = 15
net.core.somaxconn = 8192
net.ipv4.tcp_max_syn_backlog = 8192
net.netfilter.nf_conntrack_max = 262144      # 开了 conntrack 才需要
vm.swappiness = 10
```

- 关闭 `debug`；日志写本地盘，别 NFS。
- 三实例共 3 份 `data/`，每份都有独立的 `config/web-admin.json`（各自密码）和 `jobs/`。
- 磁盘：每实例单批 10 万行 → `results.jsonl` 约 40 MB + 导出约 6 MB；按每天 40 片留 10 GB 富余。
- 时区/时钟：用 chrony，`data` 与租约用 `time.monotonic()`，跨机只依赖总台的 `lease_until`，所以节点时钟偏差 < 30 s 即可。

### 7.2 总台（2C2G）

- 只跑 Flask + SQLite(WAL)，反向代理加 HTTPS，管理面板加 Basic Auth 或白名单。
- 状态备份：每小时 `.backup` 一次 SQLite，保留 24 份。
- 带宽：台账流量 ~0.7 MB/s（100 台），1 Mbps 级足够，公网 5 Mbps 稳妥。
- 不要把总台放在节点同机：总台挂了节点还能跑完手里那片。

### 7.3 安全边界

| 项 | 做法 |
| --- | --- |
| Agent → Master 认证 | 预共享 Bearer token（每节点一枚，可吊销），HTTPS only |
| 分片下载 | 一次性签名 URL，`ttl=15 min`，绑定 node_id + shard_id |
| 账号明文 | 只在节点磁盘（`chmod 600`），片上完即删；总台只存 sha256 和行数 |
| 结果明文 | 导出包只在节点本地生成、gzip 上传，总台存储目录 700/600 |
| 节点 Web 密码 | 只存在节点 `/etc/fc/admin.pw`（600），总台台账里**不存** |
| 看板 | 只显示邮箱脱敏样本（`a***@domain`）与计数，不显示密码 |

---

## 8. 扩缩容与容量规划

```
节点数 = 目标小时 → 200 ÷ 小时数
  1 亿 / 50 万每小时 = 200 机器小时
  200 台 → 1 小时      100 台 → 2 小时      20 台 → 10 小时      10 台 → 20 小时
```

| 规模 | 总 QPS | 每分钟 IP 需求 | 总出网带宽 | 总台 |
| --- | --- | --- | --- | --- |
| 5 台（试点） | 695 | 260 条 | 30 Mbps | 1 台 2C2G |
| 20 台 | 2 780 | 1 040 条 | 120 Mbps | 1 台 2C2G + 备份 |
| 100 台 | 13 900 | 5 200 条 | 600 Mbps | 1 台 2C4G |

**放量顺序（别一步到 20 台）**

1. 1 台跑 2 片（20 万行）→ 回填 `T_avg`、`reject_rate`、`r429`。
2. 5 台 × 2 片 → 观察拒绝率是否随 QPS 线性恶化。
3. 拒绝率稳定后再线性加，每次加 ≤ 2 倍。

拒绝率（`locked` + `undetermined` 占比）一旦随 QPS 上升明显恶化，说明目标侧在限流，此时加机器只会加速恶化，应降速或换 IP 段（`proxy_fingerprint` 已有出口指纹记录，可按段统计）。

---

## 9. 分阶段落地（每阶段都不碰现有代码）

| 阶段 | 交付物 | 验收标准 | 工作量 |
| --- | --- | --- | --- |
| **P0 单机基线** | 3 实例 + 1 个切好的 10 万行片 | 单实例 80 线程跑出 `T_avg`；三实例同时跑 3 片不互相干扰；`summary.json` 无 `stop_reason` | 0.5 天 |
| **P1 手动机群** | `manifest.json` + 手工 scp 分片 + 手工调 `/api/jobs` | 2 台节点并行跑 4 片，结果能合并；续跑集合正确（杀进程后 `not_started` 精确） | 1-2 天 |
| **P2 总台 MVP** | `register/heartbeat/lease/report` + SQLite + 看板 | 看板 5 s 刷新出节点在线表；节点杀掉后 30 s 判离线、片自动 requeue | 3-5 天 |
| **P3 熔断与放量** | 全局 429 熔断、版本闸门、限速 | 人为注入 429，全局 60 s 内停发、恢复后 10% 试探；20 台跑满 10 小时成功率稳定 | 3-5 天 |
| **P4 交付合并** | 分类归并 + 校验器 | 各分类行数和 == 1 亿；重复 0；抽样 1000 条人工核对 | 1-2 天 |

**P0 就能回答最关键的问题**：`T_avg` 是多少。它决定 4C4G 到底能不能 50 万/时（§2.1 表），别的都是工程细活。

---

## 10. 需要你拍板的 4 件事

1. **代理 TTL 能不能拿到 300 秒**？不能的话 §2.3 直接否掉 50 万/时，只能靠加机器（每台降到 15-20 万/时）。
2. **通道配比**：全 web（无设备，最快），还是 web 为主 + GSA 少量（需设备池，注册受 provider 冷却限制）？建议先 100% web 跑通 50 万/时。
3. **结果落在哪**：总台直收（简单，需 ~1 Mbps）还是对象存储预签名（省总台带宽，多一套凭证）？
4. **目标侧能承受多少**：2780 req/s（20 台）是硬指标，试点阶段必须先测拒绝率曲线，再决定最终台数。

---

## 附：与现有文档的关系

- 单机任务与参数：`docs/TASKS.md`、`docs/PROTOCOLS.md`
- 单机部署：`docs/DEPLOY_BT.md`
- 设备池（GSA 通道）：`docs/DEVICE-POOL.md`
- 本文只描述集群控制面，**不修改**上述任何行为与接口。
