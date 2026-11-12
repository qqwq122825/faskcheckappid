# -*- coding: utf-8 -*-
"""
GSA 登录并发方案 —— 供给器 + 设备池 + IP 池 + Worker 池 骨架
================================================================

架构总览
--------
                ┌──────────── Provisioner(供给器 / 生产者,后台低速)
                │   借IP → uuid + client-info → provision → adi.pb
                ▼
          设备池 IdentityPool(常备 N 套 ready 身份)        IP 池 ProxyPool
                │  取(消费)                                   │ 借 / 还 / 换
                ▼                                              ▼
          Worker 池(并发度 = M)──► 本地 OTP ──► GSA init/complete
                │
                └─► 回报 Master,按错误分流:
                       -20970 → 销毁该身份,供给器补一套新的
                       503/429 → 换 IP + 退避,重入队
                       -20209 → 账号锁定,出队停手(不重试)
                       成功    → 标记完成

核心原则(务必守住)
--------------------
1. 设备身份绑【账号】,不绑进程/线程。进程线程只是搬运工。
2. 并发度 = 同时在跑的 worker 数,和身份数量无关。别用"进程×线程"乘出意外并发。
3. 一套身份同一时刻只被一个 worker 持有(出队即独占)。
4. IP 粘住绑定,坏了才换;不要同一设备每次随机换 IP(漂移=风控信号)。
5. device-id 本地随机一次后固定;client-info 从合法集合里【选】不是编;
   adi.pb 必须 provisioning 联网生成,不能本地造。
6. 跑之前先 NTP 对时(OTP / provision 都和时间强相关)。

错误码速查
----------
  -20970  init 被拒 → anisette/client-info/provision 问题 → 换设备身份
  503/429 → IP/限流 → 换代理 + 退避
  -20209  账号被锁 → 去 iForgot,和密码对错无关 → 停手
"""

from __future__ import annotations

import queue
import random
import threading
import time
import uuid
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(threadName)s] %(message)s")
log = logging.getLogger("gsa")


# =============================================================================
# 配置
# =============================================================================
@dataclass
class Config:
    concurrency: int = 10            # 真实并发度 = worker 数(建议从 2~5 起步,看报错率再加)
    pool_target: int = 20            # 设备池常备水位
    pool_min: int = 8                # 低于此值,供给器加速补货
    provisioner_count: int = 2       # 供给器数量(低速后台,别开太多)
    provision_interval: float = 3.0  # 每次 provision 之间的最小间隔秒(控频)
    max_retry: int = 3               # 单账号最大重试次数(熔断)
    backoff_base: float = 2.0        # 退避基数:2s, 4s, 8s ...


# =============================================================================
# 合法 client-info 集合 —— 从已验证可用的串里选,不要自己编
#   可从 pyprovision / omnisette 里取现成的、被验证能过的串填进来
# =============================================================================
CLIENT_INFO_POOL = [
    # "<MacBookPro15,1> <macOS;13.x;...> <com.apple.AOSKit/...>",
    # "<iMac20,1> <macOS;13.x;...> <com.apple.AOSKit/...>",
    # TODO: 填入真实合法 client-info 串
]


# =============================================================================
# 数据结构
# =============================================================================
class Result(Enum):
    OK = "ok"
    ERR_20970 = "err_20970"   # 设备身份问题 → 销毁重建
    ERR_503 = "err_503"       # IP/限流 → 换IP退避
    ERR_20209 = "err_20209"   # 账号锁定 → 停手
    ERR_OTHER = "err_other"


@dataclass
class Proxy:
    """socks5 代理。url 形如 socks5://user:pass@host:port"""
    url: str
    cooldown_until: float = 0.0      # 冷却到期时间戳(被限流后冷却)

    def available(self) -> bool:
        return time.time() >= self.cooldown_until


@dataclass
class Identity:
    """一套设备身份 = 苹果眼里的一台设备。provision 产出,落盘复用。"""
    device_id: str                    # X-Mme-Device-Id,本地随机一次后固定
    local_user_uuid: str              # X-Apple-I-MD-LU
    client_info: str                  # 从合法集合里选
    adi_state_dir: str                # 独立的 adi.pb 状态目录(各身份隔离!)
    proxy: Optional[Proxy] = None     # 绑定的出口(粘住,坏了才换)
    bound_account: Optional[str] = None
    ready: bool = False


@dataclass
class Account:
    username: str
    password: str
    retries: int = 0
    done: bool = False
    locked: bool = False              # -20209 后置真,永久停手


# =============================================================================
# IP 池
# =============================================================================
class ProxyPool:
    def __init__(self, proxies: list[str]):
        self._proxies = [Proxy(url=u) for u in proxies]
        self._lock = threading.Lock()

    def borrow(self) -> Optional[Proxy]:
        """借一个当前可用(不在冷却)的代理。"""
        with self._lock:
            candidates = [p for p in self._proxies if p.available()]
            if not candidates:
                return None
            return random.choice(candidates)

    def cooldown(self, proxy: Proxy, seconds: float = 120.0):
        """把被限流的代理冷却一段时间。"""
        with self._lock:
            proxy.cooldown_until = time.time() + seconds
            log.info("代理冷却 %.0fs: %s", seconds, proxy.url)


# =============================================================================
# 设备池 + 供给器
# =============================================================================
class IdentityPool:
    def __init__(self, cfg: Config, proxy_pool: ProxyPool):
        self.cfg = cfg
        self.proxy_pool = proxy_pool
        self._ready: "queue.Queue[Identity]" = queue.Queue()
        self._stop = threading.Event()

    # ---- 消费侧 ----
    def acquire(self, timeout: float = 30.0) -> Optional[Identity]:
        """worker 取一套 ready 身份(独占)。"""
        try:
            return self._ready.get(timeout=timeout)
        except queue.Empty:
            return None

    def destroy(self, identity: Identity):
        """20970:销毁坏身份,供给器会自动补。"""
        log.info("销毁身份 device_id=%s", identity.device_id[:8])
        # TODO: 清理 identity.adi_state_dir 目录
        # 不放回池;水位下降触发供给器补货

    def recycle(self, identity: Identity):
        """成功/可复用:放回池。"""
        self._ready.put(identity)

    def size(self) -> int:
        return self._ready.qsize()

    # ---- 生产侧 ----
    def start_provisioners(self):
        for i in range(self.cfg.provisioner_count):
            t = threading.Thread(target=self._provision_loop, name=f"prov-{i}", daemon=True)
            t.start()

    def stop(self):
        self._stop.set()

    def _provision_loop(self):
        while not self._stop.is_set():
            if self.size() >= self.cfg.pool_target:
                time.sleep(1.0)
                continue

            proxy = self.proxy_pool.borrow()
            if proxy is None:
                time.sleep(2.0)
                continue

            try:
                identity = self._provision_one(proxy)
                identity.ready = True
                self._ready.put(identity)
                log.info("新身份入池 device_id=%s 池深=%d",
                         identity.device_id[:8], self.size())
            except Exception as e:
                log.warning("provision 失败: %s", e)

            # 控频:供给器必须慢,避免密集 provision 触发风控
            time.sleep(self.cfg.provision_interval)

    def _provision_one(self, proxy: Proxy) -> Identity:
        """
        生成一套设备身份。顺序:
          A1 本地随机 device-id / local-user-uuid
          A2 选 client-info
          A3 初始化 ADI 库 + 独立状态目录
          A4 provisioning 联网拿 adi.pb(走 proxy)
        """
        device_id = str(uuid.uuid4()).upper()
        local_user_uuid = str(uuid.uuid4()).upper()
        client_info = random.choice(CLIENT_INFO_POOL) if CLIENT_INFO_POOL else "TODO_CLIENT_INFO"
        state_dir = f"./adi_states/{device_id}"   # 每套身份独立目录,绝不共享

        # TODO: 真正的 provisioning
        #   1) 初始化 ADI 库,设置 device_id、state_dir
        #   2) ADIProvisioningStart -> start 数据
        #   3) POST 到苹果 provision 端点(走 proxy),拿返回
        #   4) ADIProvisioningEnd -> 写出 adi.pb 到 state_dir
        adi_provision(device_id, client_info, state_dir, proxy)

        return Identity(
            device_id=device_id,
            local_user_uuid=local_user_uuid,
            client_info=client_info,
            adi_state_dir=state_dir,
            proxy=proxy,
        )


# =============================================================================
# 待填的外部接口(接你的 libCoreADI 封装 + GSA 实现)
# =============================================================================
def adi_provision(device_id: str, client_info: str, state_dir: str, proxy: Proxy) -> None:
    """A4:联网 provisioning,把 adi.pb 写进 state_dir。失败抛异常。"""
    raise NotImplementedError


def adi_otp(identity: Identity) -> dict:
    """
    B2:本地算 anisette(毫秒级)。返回要塞进 cpd 的头字典:
      X-Apple-I-MD        (OTP)
      X-Apple-I-MD-M      (M 值)
      X-Apple-I-MD-RINFO
      X-Mme-Device-Id     = identity.device_id
      X-Apple-I-MD-LU     = identity.local_user_uuid
      X-Mme-Client-Info   = identity.client_info
      X-Apple-I-Client-Time = 当前时间
    """
    raise NotImplementedError


def gsa_init(account: Account, identity: Identity, cpd: dict) -> dict:
    """B3:init 往返。返回 {salt, B, iter, proto} 或抛出带错误码的异常。"""
    raise NotImplementedError


def gsa_complete(account: Account, identity: Identity, init_resp: dict) -> dict:
    """B4+B5:本地算 M1 → complete 往返。返回 {adsid, token, ...}。"""
    raise NotImplementedError


# =============================================================================
# 登录一次 + 错误分类
# =============================================================================
def login_once(account: Account, identity: Identity) -> tuple[Result, Optional[dict]]:
    try:
        cpd = adi_otp(identity)                       # B2 本地,快
        init_resp = gsa_init(account, identity, cpd)  # B3 往返1
        spd = gsa_complete(account, identity, init_resp)  # B4+B5 往返2
        return Result.OK, spd
    except Exception as e:
        code = _extract_code(e)                       # 从异常里解析出 ec / http 状态
        if code == -20970:
            return Result.ERR_20970, None
        if code == -20209:
            return Result.ERR_20209, None
        if code in (503, 429):
            return Result.ERR_503, None
        return Result.ERR_OTHER, None


def _extract_code(exc: Exception) -> Optional[int]:
    """TODO:从你的异常/响应里解析出 GSA 的 ec 或 HTTP 状态码。"""
    return getattr(exc, "code", None)


# =============================================================================
# Master:分发 + 调度 + 错误路由
# =============================================================================
class Master:
    def __init__(self, cfg: Config, accounts: list[Account],
                 identity_pool: IdentityPool, proxy_pool: ProxyPool):
        self.cfg = cfg
        self.identity_pool = identity_pool
        self.proxy_pool = proxy_pool
        self._tasks: "queue.Queue[Account]" = queue.Queue()
        for a in accounts:
            self._tasks.put(a)
        self._results: list[tuple[str, Result]] = []
        self._results_lock = threading.Lock()

    def run(self):
        # 等池子先攒够一点再开跑,避免 worker 空转
        while self.identity_pool.size() < min(self.cfg.pool_min, self.cfg.concurrency):
            log.info("等待设备池预热... 当前 %d", self.identity_pool.size())
            time.sleep(1.0)

        workers = [threading.Thread(target=self._worker_loop, name=f"worker-{i}", daemon=True)
                   for i in range(self.cfg.concurrency)]
        for w in workers:
            w.start()
        for w in workers:
            w.join()

        self._report()

    def _worker_loop(self):
        while True:
            try:
                account = self._tasks.get(timeout=5.0)
            except queue.Empty:
                return  # 没任务了,收工

            if account.done or account.locked:
                continue

            identity = self.identity_pool.acquire()
            if identity is None:
                # 池空,稍等重入队
                self._tasks.put(account)
                time.sleep(1.0)
                continue

            identity.bound_account = account.username
            result, _spd = login_once(account, identity)
            self._handle(account, identity, result)

    def _handle(self, account: Account, identity: Identity, result: Result):
        if result == Result.OK:
            account.done = True
            self.identity_pool.recycle(identity)   # 身份可复用
            self._record(account.username, result)
            log.info("成功 %s", account.username)
            return

        if result == Result.ERR_20970:
            # 设备身份坏 → 销毁,供给器补新的;账号重入队重试
            self.identity_pool.destroy(identity)
            self._retry_or_fail(account, result)
            return

        if result == Result.ERR_503:
            # IP/限流 → 冷却该 IP,身份留着,退避后重入队
            if identity.proxy:
                self.proxy_pool.cooldown(identity.proxy)
            identity.proxy = self.proxy_pool.borrow()  # 换一个
            self.identity_pool.recycle(identity)
            self._backoff(account)
            self._retry_or_fail(account, result)
            return

        if result == Result.ERR_20209:
            # 账号锁定 → 停手,不重试;身份别销毁(没用)
            account.locked = True
            self.identity_pool.recycle(identity)
            self._record(account.username, result)
            log.warning("账号锁定,停手 %s", account.username)
            return

        # 其他错误
        self.identity_pool.recycle(identity)
        self._retry_or_fail(account, result)

    def _retry_or_fail(self, account: Account, result: Result):
        account.retries += 1
        if account.retries >= self.cfg.max_retry:
            self._record(account.username, result)
            log.warning("熔断,放弃 %s (%s)", account.username, result.value)
        else:
            self._tasks.put(account)  # 重入队

    def _backoff(self, account: Account):
        delay = self.cfg.backoff_base ** account.retries
        time.sleep(min(delay, 30.0))

    def _record(self, username: str, result: Result):
        with self._results_lock:
            self._results.append((username, result))

    def _report(self):
        with self._results_lock:
            ok = sum(1 for _, r in self._results if r == Result.OK)
            log.info("==== 完成 成功 %d / 总 %d ====", ok, len(self._results))
            for u, r in self._results:
                log.info("  %s -> %s", u, r.value)


# =============================================================================
# 入口
# =============================================================================
def main():
    cfg = Config(concurrency=5, pool_target=20, provisioner_count=2)

    proxies = [
        # "socks5://user:pass@host1:port",
        # ... 从你的 socks5 API 拉一批填进来
    ]
    accounts = [
        # Account("user1@xx.com", "pw1"),
        # ... 100 个你自己的账号
    ]

    proxy_pool = ProxyPool(proxies)
    identity_pool = IdentityPool(cfg, proxy_pool)
    identity_pool.start_provisioners()   # 后台开始补货

    master = Master(cfg, accounts, identity_pool, proxy_pool)
    master.run()

    identity_pool.stop()


if __name__ == "__main__":
    main()
