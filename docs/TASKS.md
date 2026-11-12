# CLI 任务入口

项目根目录运行 `python cli.py --protocol gsa` 或 `python cli.py --protocol web`。macOS 快捷入口在 scripts/macos/，Windows 在 scripts/windows/CLI.cmd。

按提示选择账号文件（默认 data/accounts/账号.txt）、进程、每进程线程、IP 库存目标、设备参数、重试、调试和代理 API。账号每行 `邮箱----密码`，保留四个连字符后的原密码，重复邮箱报错。常规入口没有账号条数硬上限。

进程、线程只接受正整数，无人工上限。共享队列动态领取任务，完成较快的工作进程继续接任务。配置的进程数是认证工作进程数，另有总控和供给进程。线程按需创建，填写值是容量。

CLI 默认来自 data/config/task-settings.json；发布包没有该文件时使用 examples/auth_settings.json。`--processes`、`--threads`、`--proxy-target`、`--retries 0|1`、`--accounts` 可覆盖相应提示，`--config` 可选另一份 JSON。本轮选择不会覆盖默认文件。Web 保存配置为 data/config/web-settings.json。

Web 与 CLI 共用 data/config/proxy-api.json 和 data/config/batch-device-pool/。API 留空沿用，新地址覆盖保存；密钥不回显。单独代理连接 user:pass 仅本轮使用。同一数据目录同时只运行一个批次。

首批 IP 目标 0 按任务数，超过 200 分批提取；后续按剩余任务收缩目标。租约同一时刻只交给一个操作，默认 60 秒淘汰。同一次 init/complete 固定出口，不退回直连。公共 Anisette 参数服务直连，Apple 注册和认证走租约代理。

GSA 默认 2 台设备就绪开始，后台补至配置容量。单设备并发 0 随总线程容量，1 为独占，其他正整数为自定义上限。每轮会话/SRP 独立，并刷新参数；同一身份参数值可能相同。可选新设备优先或按负载均衡。休息时先停止新派发，待在途释放后计时，不换身份。

默认零重试。选一次网络重试只处理符合条件的初始化前/初始化阶段临时网络失败，保留设备，退避到期后优先派发。init/-20970、账号拒绝、锁定和 complete 未知不重试；HTTP429 停止批次。20970 不触发设备封禁判定或身份更换。见 [PROTOCOLS.md](PROTOCOLS.md)。

日志在 data/cli-logs/tasks-*，终端每秒显示统计，详细事件写文件，调试模式另记网络/协议阶段耗时。exports/ 分类文件按用户要求包含原账号密码，与脱敏过程日志分开。

```bash
python cli.py --protocol gsa --demo --demo-count 100 --processes 5 --threads 10 --debug
python cli.py --protocol web --accounts /path/to/owned-accounts.txt --processes 2 --threads 10 --retries 0
```

演示使用临时设备目录及合成账号/代理，禁止外连。进程异常或任务超过硬期限时停止本批并回收子进程，在途保留未知，未开始项另列，避免自动重复提交。
