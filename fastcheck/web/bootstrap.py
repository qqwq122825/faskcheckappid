import argparse
import getpass
import os
from pathlib import Path
import secrets
import warnings

from werkzeug.security import generate_password_hash
from fastcheck.devices import save_json

ROOT=Path(__file__).resolve().parents[2]


def data_root():
    return Path(os.environ.get('GSA_DATA_DIR',str(ROOT/'data'))).resolve()


def initialize(root,password,*,minimum=12):
    if not isinstance(password,str) or len(password)<minimum:
        raise ValueError(f'管理密码至少需要 {minimum} 个字符。')
    save_json(Path(root)/'config/web-admin.json',{'schema':1,'password_hash':generate_password_hash(password),
                                               'session_secret':secrets.token_hex(32)})


def main():
    parser=argparse.ArgumentParser(description='设置 Web 管理登录密码')
    parser.add_argument('--reset',action='store_true')
    args=parser.parse_args()
    if (data_root()/'config/web-admin.json').exists() and not args.reset:
        print('已有管理配置；如需重设密码，请加 --reset。');return
    with warnings.catch_warnings():
        warnings.simplefilter('error',getpass.GetPassWarning)
        first=getpass.getpass('设置管理密码（至少 12 个字符）：')
        second=getpass.getpass('再次输入：')
    if first!=second:raise ValueError('两次密码不一致。')
    initialize(data_root(),first)
    print('管理密码已设置；只保存密码哈希。登录用户名为 admin。')


if __name__=='__main__':main()
