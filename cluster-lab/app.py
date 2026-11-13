"""Run controller or node as an independent service."""
import argparse
import multiprocessing
import os
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description='苹果id校验集群系统 · 总控与节点')
    parser.add_argument('role', choices=('controller', 'node'))
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int)
    parser.add_argument('--data-dir', type=Path)
    parser.add_argument('--max-processes', type=int, default=10)
    parser.add_argument('--max-threads', type=int, default=100)
    parser.add_argument('--allow-http', action='store_true', help='允许节点连接非回环 HTTP 总控，仅受信内网测试使用')
    args = parser.parse_args()
    password = os.environ.get('CLUSTER_LAB_ADMIN_PASSWORD')
    if args.host not in ('127.0.0.1', 'localhost', '::1') and (not password or len(password) < 8):
        parser.error('非回环监听需要通过 CLUSTER_LAB_ADMIN_PASSWORD 设置至少 8 位管理密码')
    port = args.port or (18770 if args.role == 'controller' else 18771)
    if not 1 <= port <= 65535:
        parser.error('端口应为 1–65535')
    from clusterlab.controller import Controller
    from clusterlab.node import Node
    from clusterlab.web import create_app
    from waitress import serve
    root = args.data_dir or Path(__file__).resolve().parent / 'runtime' / args.role
    dispatch_qps = int(os.environ.get('CLUSTER_DISPATCH_QPS', '0'))
    service = (Controller(root, dispatch_per_second=dispatch_qps) if args.role == 'controller'
               else Node(root, args.max_processes, args.max_threads, args.allow_http))
    application = create_app(args.role, service, password)
    if args.role == 'node':
        service.start()
    print(f'苹果id校验集群系统 {args.role} · http://{args.host}:{port}/', flush=True)
    print(f'数据目录：{root.resolve()}', flush=True)
    try:
        # waitress 的 max_request_body_size 默认只有 1 GiB，Flask 那边的
        # MAX_CONTENT_LENGTH 调多大都没用——超过 1 GiB 会直接 413。
        serve(application, host=args.host, port=port, threads=8,
              max_request_body_size=8 * 1024 * 1024 * 1024)
    except KeyboardInterrupt:
        pass
    finally:
        service.close()


if __name__ == '__main__':
    multiprocessing.freeze_support()
    main()
