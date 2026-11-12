import argparse
import os
from pathlib import Path
import sys
from fastcheck.version import VERSION

ROOT=Path(__file__).resolve().parents[2]


def main(argv=None):
    parser=argparse.ArgumentParser(description='FastCheck Web · 本地与服务器共用入口')
    parser.add_argument('--version',action='version',version=VERSION)
    parser.add_argument('--local',action='store_true',help='本地测试，默认 18088，首次使用 admin/admin123')
    parser.add_argument('--host',default=os.environ.get('GSA_BIND','127.0.0.1'))
    parser.add_argument('--port',type=int)
    parser.add_argument('--data-dir')
    parser.add_argument('--server',choices=('auto','waitress','gunicorn'),default='auto')
    parser.add_argument('--web-threads',type=int,default=8,help='处理页面/API 的线程，与认证线程分别设置')
    parser.add_argument('--check',action='store_true')
    args=parser.parse_args(argv)
    if args.check:
        from fastcheck.single_login import _dependencies
        import flask,portalocker,waitress
        _dependencies()
        print(f'v{VERSION}：Web/CLI/锁/协议/证书离线检查通过。');return
    port=args.port if args.port is not None else int(os.environ.get('GSA_PORT','18088' if args.local else '8088'))
    if not 1<=port<=65535:parser.error('端口必须在 1–65535。')
    if args.web_threads<1:parser.error('Web 请求线程数必须为正整数。')
    if args.data_dir:os.environ['GSA_DATA_DIR']=str(Path(args.data_dir).resolve())
    from .bootstrap import data_root,initialize
    if args.local and args.host not in ('127.0.0.1','localhost','::1'):
        parser.error('--local 使用本机回环地址。服务器部署请去掉 --local。')
    if not (data_root()/'config/web-admin.json').exists():
        if args.local:initialize(data_root(),'admin123',minimum=8)
        else:
            print('请先运行：python -m fastcheck.web.bootstrap',file=sys.stderr);raise SystemExit(2)
    chosen=args.server
    if chosen=='auto':chosen='waitress' if args.local or os.name=='nt' else 'gunicorn'
    if chosen=='gunicorn':
        if os.name=='nt':parser.error('Windows 请使用 Waitress。')
        os.environ.update(GSA_BIND=args.host,GSA_PORT=str(port),GSA_WEB_THREADS=str(args.web_threads))
        os.chdir(ROOT)
        os.execv(sys.executable,[sys.executable,'-m','gunicorn','-c',str(ROOT/'deploy/gunicorn.conf.py'),'fastcheck.web.app:create_app()'])
    from .app import create_app
    from waitress import serve
    application=create_app()
    print(f'FastCheck v{VERSION} · http://{args.host}:{port}/',flush=True)
    if args.local:print('本地测试登录：admin / admin123（首次初始化）',flush=True)
    try:
        serve(application,host=args.host,port=port,threads=args.web_threads)
    finally:application.extensions['jobs'].shutdown()
