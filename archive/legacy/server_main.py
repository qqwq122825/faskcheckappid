"""Native Linux/BT entry point, using one web worker and independent task workers."""
import argparse
import os
from pathlib import Path
import sys
from fastcheck.version import VERSION

ROOT=Path(__file__).resolve().parent


def main():
    parser=argparse.ArgumentParser(description='FastCheck 宝塔 Web 服务')
    parser.add_argument('--version',action='version',version=VERSION)
    parser.add_argument('--host',default=os.environ.get('GSA_BIND','127.0.0.1'))
    parser.add_argument('--port',type=int,default=int(os.environ.get('GSA_PORT','8088')))
    parser.add_argument('--data-dir')
    parser.add_argument('--check',action='store_true')
    args=parser.parse_args()
    if args.check:
        from fastcheck.single_login import _dependencies
        import flask,gunicorn
        _dependencies()
        print('v'+VERSION+'：Python/Flask/Gunicorn/SRP/证书离线检查通过。');return
    if not 1<=args.port<=65535:parser.error('端口必须在 1–65535。')
    if args.data_dir:os.environ['GSA_DATA_DIR']=str(Path(args.data_dir).resolve())
    from fastcheck.web.bootstrap import data_root
    if not (data_root()/'config/web-admin.json').exists():
        print('请先运行：.venv/bin/python -m server.bootstrap',file=sys.stderr);raise SystemExit(2)
    os.environ.update(GSA_BIND=args.host,GSA_PORT=str(args.port))
    os.chdir(ROOT)
    os.execv(sys.executable,[sys.executable,'-m','gunicorn','-c',str(ROOT/'gunicorn.conf.py'),'fastcheck.web.app:create_app()'])


if __name__=='__main__':main()
