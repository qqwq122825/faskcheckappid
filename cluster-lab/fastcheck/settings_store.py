"""Private, shared proxy API setting; never echoes the credential-bearing URL."""
import getpass
import json
from pathlib import Path
from urllib.parse import urlsplit

from fastcheck.devices import save_json
from fastcheck.protocols.proxy_api import demand_url


def settings_path(root):
    return Path(root)/'config/proxy-api.json'


def validate_api(value):
    try:
        if not isinstance(value, str) or not value or len(value)>16384:
            raise ValueError
        # 允许同时填多个账号的提取链接（空白分隔），各账号的配额合并成一个池子。
        parts=value.split()
        if not parts or len(parts)>8:
            raise ValueError
        for item in parts:
            if any(ord(c)<33 for c in item):
                raise ValueError
            parsed=urlsplit(item)
            if parsed.scheme not in ('http','https') or not parsed.hostname or parsed.fragment:
                raise ValueError
            if parsed.port is not None and not 1<=parsed.port<=65535:
                raise ValueError
            demand_url(item,1)
    except (ValueError,TypeError):
        raise ValueError('代理 API 地址无效：请使用有效的 HTTP/HTTPS 提取链接（可填多个，空白分隔），且 num 为单个正整数。') from None
    return value


def save_api(root, value):
    value=validate_api(value)
    save_json(settings_path(root),{'schema':1,'api_url':value})
    return value


def load_api(root):
    path=settings_path(root)
    if not path.exists():
        return None
    try:
        if path.is_symlink():
            raise ValueError
        path.chmod(0o600)
        value=json.loads(path.read_text(encoding='utf-8'))
        if not isinstance(value,dict) or value.get('schema')!=1:
            raise ValueError
        return validate_api(value.get('api_url'))
    except (ValueError,TypeError,OSError):
        raise ValueError('本地代理 API 配置不可用，请重新输入。') from None


def prompt_api(root, reader=None):
    reader=reader or getpass.getpass
    try:
        saved=load_api(root)
    except ValueError:
        print('本地代理 API 配置不可用，请重新输入。')
        saved=None
    prompt=('代理 API 地址（已保存；回车沿用，输入新地址覆盖；隐藏输入）：' if saved else
            '代理 API 地址（隐藏输入，自动保存到本机）：')
    entered=reader(prompt).strip()
    if not entered and saved:
        print('已沿用本地代理 API 配置。')
        return saved
    value=save_api(root,entered)
    print('代理 API 已保存到本机配置。')
    return value
