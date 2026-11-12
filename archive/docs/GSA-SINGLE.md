# 网页和 GSA 协议

统一的协议流程、HTTP/业务码区分、20970 与 20209 的边界、实测证据和面板分类规则，见 [协议及结果分类说明](PROTOCOLS.md)。当前共用总控入口见上一级 [TASKS.md](../TASKS.md)。以下保留 GSA 单账号入口的说明。

新增的 5 台设备选择、持久化和有限重试工具，见上一级 `DEVICE-POOL.md`；双击上一级的 `GSA设备测试.command` 即可使用。并发 2 的批量入口及可选调试模式，见上一级 `BATCH.md` 和 `GSA批量测试.command`。

已从 `/Users/xxx/Downloads/core/` 提取协议和所需依赖。`anisette_v3.py` 已增加显式 HTTP/WebSocket 代理和可选的无正文计时钩子；`network_timing.py` 修正 HTTP 代理的目标 DNS 模式标注。其余提取源码保持原样，原始及本地 SHA-256 见 `source_manifest.json`。配套 Apple 根证书从服务器同一项目下载，运行时按原项目指纹校验。原项目和 `gsa_concurrency.py` 未修改。

## 使用

双击上一级的 `GSA登录.command`，或在终端执行：

```sh
cd /Users/xxx/Documents/并发
.venv/bin/python gsa_login.py
```

程序在本机交互读取 Apple 账号和密码。密码输入不显示，不写入配置或结果文件。每次只认证一个账号、尝试一轮，使用直连，不读取系统代理。准备阶段不提交账号。

```sh
# 离线检查依赖、模块导入及证书，不联网
.venv/bin/python gsa_login.py --self-check

# 在线准备设备参数，不读取或提交账号
.venv/bin/python gsa_login.py --prepare-only --prepare-timeout 60

# 指定自己的 Anisette v3 服务
.venv/bin/python gsa_login.py --anisette-url https://your-service.example

# 离线协议测试，禁止网络连接
.venv/bin/python -m unittest discover -s tests -v
```

默认设备服务沿用原项目首个 HTTPS 地址 `https://new.ani.sidestore.io/`，不会自动尝试其他供应商。设备服务接收设备注册数据，不接收 Apple 账号密码。设备身份默认以目录权限 700、文件权限 600 保存在当前目录的 `config/anisette-v3/`。可通过 `FASTCHECK_ANISETTE_STATE_DIR` 指定其他目录。

## 认证流程与耗时

1. 加载本地模块，验证 Apple 根证书指纹。
2. 向设备服务获取配套的 client-info 和 User-Agent。首次使用该服务时注册设备并保存设备状态；已有状态时直接复用。
3. 获取新鲜的 Anisette 参数。准备完成后，在本机输入账号和密码。
4. 输入完毕后再次刷新设备参数，防止输入等待导致 OTP 过期；身份和配套元数据保持一致。
5. 在同一个 HTTP 会话内向 Apple 发送 `init`，取得 salt、服务器公钥 B、迭代次数、协议和上下文 c。
6. 在本地执行 s2k / s2k_fo 密码派生和 SRP 运算，产生 M1。
7. 发送 `complete`，必须验证服务器 M2 后才报告密码认证通过。
8. 显示认证结果，然后关闭 HTTP 会话。

准备工作不需要每个账号都重新注册设备，但每次尝试都应刷新参数。准备耗时、用户输入时间、参数刷新耗时和账号认证耗时应分开测量；离线测试或设备准备成功不能作为真实账号的认证耗时。

`--prepare-timeout` 默认为 60 秒，约束一次设备准备/刷新流程。`--timeout` 默认为 25 秒，分别用于 GSA 的每次网络请求；requests 路径另将连接超时上限设为 10 秒。这些并非整个账号的严格总时限，DNS、分段读取和两个请求的耗时可能叠加。发生网络失败时，只能报告未取得结论。

## 提取内容

| 文件 | 用途 |
| --- | --- |
| `gsa_status.py` | XML plist、HTTP 会话、init/complete、M2 校验和结果分类 |
| `srp_auth.py` | Apple SRP、s2k / s2k_fo 派生、M1/M2 |
| `anisette_v3.py` | 持久设备身份、注册、认证参数刷新 |
| `apple_trust.py` | 应用内 Apple 根证书及指纹校验 |
| `timing.py`、`network_timing.py`、`tls.py`、`gsa_dns.py` | 原协议依赖的传输、解析及诊断组件 |
| `reference/gsa_runtime.py` | 原项目调度与调用参考，不作为当前包入口 |

单账号入口是上一级的 `gsa_login.py`。参考文件依赖的原并发调度层没有迁入，也不影响单账号入口。

协议地址：`https://gsa.apple.com/grandslam/GsService2`。请求 Header.Version 为 `1.0.1`。init 字段包含 cpd、u、A2k、ps=[s2k,s2k_fo]、o=init；complete 包含 cpd、u、M1、c、o=complete。两轮使用同一份设备参数。

## 当前能力

本项目确认 GSA 密码认证结果。HTTP 200 或 init 成功不能代替 M2 校验；服务器明确要求 trustedDeviceSecondaryAuth / secondaryAuth 时显示需要二次验证。原项目没有提交二次验证码、解密 SPD、导出会话令牌或完成业务服务登录的代码，这些功能尚未实现。valid 仅代表密码证明通过。

退出码：0 表示 M2 校验后密码认证通过；3 表示密码认证通过但需要二次验证；1 表示失败或未完成；2 表示参数、环境或设备准备错误；130 表示用户取消。

## 验证

已通过离线模块/证书检查，以及 7 项测试：独立合成服务器验证 s2k/s2k_fo 的真实 SRP 运算，拒绝缺失或篡改的 M2，二次验证分类，初始化错误不判为密码错误，单次尝试，输入后刷新参数及会话清理。离线测试禁止网络连接。

早期设备准备样本见 `prepare_timing.json`，不能代表账号认证耗时。此后已经完成用户自有账号的双通道实测与正常账号参数变体对照，记录及结论见 [协议说明的本机证据部分](PROTOCOLS.md)。

Python 环境为 3.11，本次依赖版本记录在 `requirements.lock.txt`。重建方式：

```sh
python3.11 -m venv .venv
.venv/bin/python -m pip install -r requirements.lock.txt
```
