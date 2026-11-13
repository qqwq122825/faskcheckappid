"""部署自检：代理 API、提取、设备池、anisette、Apple 连通性。"""
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def line(ok, text, detail=''):
    print(f"{'✅' if ok else '❌'} {text}" + (f"  {detail}" if detail else ''))
    return ok


def main():
    print('=== 1. 配置 ===')
    api_path = ROOT/'data/config/proxy-api.json'
    api = None
    if api_path.exists():
        try:
            raw = json.loads(api_path.read_text())
            api = raw.get('api_url') or ''
            parts = [p for p in api.split() if p]
            hosts = []
            from urllib.parse import urlsplit
            for p in parts:
                u = urlsplit(p)
                hosts.append(f"{u.scheme}://{u.hostname}{'?' + u.query.split('&')[0] if u.query else ''}")
            line(bool(api), '代理 API 已配置', f'{len(parts)} 个链接，{len(api)} 字符')
        except (OSError, ValueError) as exc:
            line(False, '代理 API 文件损坏', type(exc).__name__)
    else:
        line(False, '代理 API 未配置', str(api_path))

    print('\n=== 2. 提取测试 ===')
    if api:
        try:
            from fastcheck.proxy_pool import fetch_proxies
            started = time.monotonic()
            got = fetch_proxies(api, 5, scheme='socks5h')
            line(bool(got), '提取成功', f'{len(got)} 条 / {(time.monotonic()-started)*1000:.0f} ms')
            if got:
                print(f'   样例: {got[0][:48]}')
        except Exception as exc:
            line(False, '提取失败', f'{type(exc).__name__}: {exc}')

    print('\n=== 3. 设备池 ===')
    try:
        from fastcheck.devices import DevicePool
        from fastcheck.scheduler import Settings
        settings = Settings()
        pool = DevicePool(ROOT/'data/config/batch-device-pool', settings.origin,
                          max_devices=settings.device_capacity)
        states = {}
        for slot in range(1, settings.device_capacity+1):
            st = pool.inspect(slot)['state']
            states[st] = states.get(st, 0) + 1
        ready = states.get('ready', 0)
        line(ready > 0, '身份池就绪', f"ready={ready} pending={states.get('pending',0)} "
                                      f"missing={states.get('missing',0)} invalid={states.get('invalid',0)}")
        if ready == 0:
            print(f'   → 需要注册：启动任务后每台 6-8 秒，共 {settings.device_capacity} 台')
        health = pool.health()
        cooldown = health.get('provider_until', 0) - time.time()
        line(cooldown <= 0, 'provider 无冷却', f'剩余 {cooldown:.0f}s' if cooldown > 0 else '')
    except Exception as exc:
        line(False, '身份池读取失败', f'{type(exc).__name__}: {exc}')

    print('\n=== 4. anisette 服务 ===')
    try:
        import requests
        started = time.monotonic()
        r = requests.get(settings.origin.rstrip('/') + '/v3/client_info', timeout=20)
        info = r.json()
        line(r.status_code == 200 and 'client_info' in info, 'anisette 可达',
             f'{settings.origin}  {(time.monotonic()-started)*1000:.0f} ms')
        print(f"   型号: {info.get('client_info','')[:60]}")
    except Exception as exc:
        line(False, 'anisette 不可达', f'{type(exc).__name__}')

    print('\n=== 5. 代理 → Apple ===')
    if api:
        try:
            from fastcheck.proxy_pool import fetch_proxies
            import requests
            got = fetch_proxies(api, 1, scheme='socks5h')
            proxy = got[0]
            started = time.monotonic()
            r = requests.post('https://idmsa.apple.com/appleauth/auth/signin/init',
                              json={}, timeout=25, allow_redirects=False,
                              headers={'User-Agent': 'Mozilla/5.0', 'Content-Type': 'application/json'},
                              proxies={'http': proxy, 'https': proxy})
            line(r.status_code in (200, 400, 401, 403, 503), '隧道可达 Apple',
                 f'HTTP {r.status_code}  {(time.monotonic()-started)*1000:.0f} ms')
        except Exception as exc:
            line(False, '隧道不通', f'{type(exc).__name__}')
    else:
        line(False, '跳过（无代理配置）')

    print('\n=== 6. 最近任务 ===')
    jobs = sorted((ROOT/'data/jobs').glob('*/state.json'), reverse=True)[:3]
    for path in jobs:
        try:
            st = json.loads(path.read_text())
            print(f"   {st.get('created_at','?')[:19]}  {st.get('state'):10s} "
                  f"{st.get('protocol','?'):4s} {st.get('accounts','?')} 个  {path.parent.name[:8]}")
        except (OSError, ValueError):
            continue


if __name__ == '__main__':
    main()
