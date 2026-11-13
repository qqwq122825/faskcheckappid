import hashlib
from datetime import datetime
import io
import os
from pathlib import Path
import secrets
import tempfile
from zoneinfo import ZoneInfo

from flask import Flask, jsonify, request, send_file, session
from werkzeug.exceptions import HTTPException
from werkzeug.security import check_password_hash, generate_password_hash

from . import VERSION
from .common import atomic_json, integer, read_json
from .controller import Conflict


def create_app(role, service, password=None):
    static = Path(__file__).resolve().parent / 'static'
    app = Flask(__name__, static_folder=str(static), static_url_path='/static')
    state = read_json(service.root / 'web-session.json')
    if not state:
        state = {'secret': secrets.token_hex(32)}
        atomic_json(service.root / 'web-session.json', state)
    password_hash = generate_password_hash(password) if password else None
    app.secret_key = state['secret']
    app.config.update(MAX_CONTENT_LENGTH=8 * 1024 * 1024 * 1024, MAX_FORM_MEMORY_SIZE=512 * 1024,
                      SESSION_COOKIE_NAME='clusterlab_' + hashlib.sha256(str(service.root).encode()).hexdigest()[:10],
                      SESSION_COOKIE_HTTPONLY=True, SESSION_COOKIE_SAMESITE='Strict')
    app.extensions['service'] = service

    @app.before_request
    def protect():
        if request.path.startswith('/agent/'):
            if role != 'controller':
                return jsonify(error='当前服务不是总控'), 404
            authorization = request.headers.get('Authorization', '')
            token = authorization[7:] if authorization.startswith('Bearer ') else ''
            node_id = service.authenticate(token)
            if not node_id:
                return jsonify(error='节点接入凭证无效'), 401
            request.node_id = node_id
            return None
        if request.path.startswith('/api/'):
            if request.path not in ('/api/session', '/api/login') and password_hash and not session.get('authenticated'):
                return jsonify(error='请先登录'), 401
            if request.method != 'GET':
                supplied = request.headers.get('X-CSRF-Token', '')
                if not supplied or not secrets.compare_digest(supplied, session.get('csrf', '')):
                    return jsonify(error='页面会话已失效，请刷新'), 403

    @app.after_request
    def headers(response):
        response.headers['Cache-Control'] = 'no-store'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['X-Frame-Options'] = 'DENY'
        response.headers['Content-Security-Policy'] = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'; object-src 'none'"
        return response

    @app.errorhandler(Exception)
    def error(exc):
        if isinstance(exc, HTTPException):
            return jsonify(error=exc.description), exc.code
        if isinstance(exc, Conflict):
            return jsonify(error=str(exc)), 409
        if isinstance(exc, (ValueError, KeyError, TypeError)):
            return jsonify(error=str(exc) if isinstance(exc, ValueError) else '请求字段不完整或格式无效'), 400
        app.logger.exception('Request failed')
        return jsonify(error='操作失败，请查看服务日志'), 500

    @app.get('/')
    def index():
        return send_file(static / 'ui' / 'index.html')

    @app.get('/healthz')
    def health():
        return jsonify(ok=True, role=role, version=VERSION)

    @app.get('/api/session')
    def current_session():
        session.setdefault('csrf', secrets.token_urlsafe(24))
        return jsonify(role=role, version=VERSION, authenticated=not password_hash or bool(session.get('authenticated')),
                       password_required=bool(password_hash), csrf=session['csrf'])

    @app.post('/api/login')
    def login():
        data = request.get_json(silent=True) or {}
        if password_hash and (data.get('username') != 'admin' or not check_password_hash(password_hash, str(data.get('password', '')))):
            return jsonify(error='用户名或密码错误'), 401
        session.clear()
        session.update(authenticated=True, csrf=secrets.token_urlsafe(24))
        return jsonify(csrf=session['csrf'])

    @app.post('/api/logout')
    def logout():
        session.clear()
        return jsonify(ok=True)

    @app.get('/api/state')
    def snapshot():
        return jsonify(service.snapshot())

    if role == 'controller':
        @app.post('/api/settings/proxy')
        def configure_proxy():
            return jsonify(service.configure_proxy(request.get_json() or {}))

        @app.post('/api/settings')
        def configure_settings():
            return jsonify(service.configure_settings(request.get_json() or {}))

        @app.post('/api/nodes')
        def add_node():
            data = request.get_json() or {}
            return jsonify(service.add_node(data.get('name'), data.get('processes', 10), data.get('threads', 100))), 201

        @app.post('/api/nodes/<node_id>/config')
        def configure_node(node_id):
            service.configure_node(node_id, request.get_json() or {})
            return jsonify(ok=True)

        @app.post('/api/nodes/<node_id>/stop')
        def stop_node(node_id):
            service.stop_node(node_id)
            return jsonify(ok=True, message='停止请求已发送，等待节点确认')

        @app.post('/api/jobs/stream')
        def upload_stream():
            """裸流上传：请求体就是文件内容，不经 multipart 解析，不落临时文件。

            参数走 query，避免读取 request.form 触发 werkzeug 把整个文件先解到
            临时目录（那样 4 GB 文件会占双倍磁盘，还多一次全量拷贝）。
            """
            name = request.args.get('name', '')
            unit_rows = int(request.args.get('unit_rows', str(service.settings()['default_unit_rows'])))
            enqueue = request.args.get('enqueue', 'false')
            if enqueue not in ('true', 'false'):
                raise ValueError('加入队列选项无效')
            if not request.content_length:
                raise ValueError('缺少 Content-Length，无法确认上传完整性')
            job_id = service.create_job(name or '未命名任务', request.stream, unit_rows,
                                        'digest', enqueue == 'true')
            return jsonify(id=job_id, bytes=request.content_length), 202

        @app.post('/api/jobs')
        def upload():
            uploaded = request.files.get('file')
            if not uploaded:
                raise ValueError('请选择 UTF-8 文本文件')
            unit_rows = int(request.form.get('unit_rows', str(service.settings()['default_unit_rows'])))
            enqueue = request.form.get('enqueue', 'false')
            if enqueue not in ('true', 'false'):
                raise ValueError('加入队列选项无效')
            job_id = service.create_job(request.form.get('name') or uploaded.filename, uploaded.stream, unit_rows, 'digest', enqueue == 'true')
            return jsonify(id=job_id), 202

        @app.post('/api/demo')
        def demo():
            data = request.get_json(silent=True) or {}
            count = integer(data.get('rows', 10000), '演示记录数', 1, 100000)
            settings = service.settings()
            unit_rows = integer(data.get('unit_rows', settings['default_unit_rows']), '每包记录数', 50, settings['max_unit_rows'])
            with tempfile.SpooledTemporaryFile(max_size=1024 * 1024) as stream:
                for number in range(1, count + 1):
                    stream.write(f'demo-{number:08d}@example.invalid----SYNTHETIC-ONLY-{number:08d}\n'.encode())
                stream.seek(0)
                job_id = service.create_job(f'通信验收 · {count:,} 条合成记录', stream, unit_rows, 'simulate')
            return jsonify(id=job_id), 202

        @app.post('/api/jobs/<job_id>/<action>')
        def job_action(job_id, action):
            service.job_action(job_id, action)
            return jsonify(ok=True)

        @app.get('/api/jobs/<job_id>/rows')
        def job_rows(job_id):
            offset = int(request.args.get('offset', 0))
            limit = int(request.args.get('limit', 100))
            category = request.args.get('category', 'all')
            return jsonify(service.read_rows(job_id, offset, limit, category))

        @app.get('/api/jobs/<job_id>/results')
        def download_results(job_id):
            path, rows = service.text_export(job_id)
            date = datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%Y-%m-%d')
            return send_file(path, mimetype='text/plain', as_attachment=True,
                             download_name=f'{date}_处理成功_{rows}条.txt', conditional=False)

        @app.post('/agent/heartbeat')
        def heartbeat():
            return jsonify(service.heartbeat(request.node_id, request.get_json() or {}))

        @app.post('/agent/lease')
        def lease():
            return jsonify(unit=service.lease(request.node_id, (request.get_json() or {}).get('session')))

        @app.get('/agent/units/<unit_id>/input')
        def unit_input(unit_id):
            unit = service.unit_for_node(request.node_id, unit_id, request.headers.get('X-Lease-Token'))
            return send_file(unit['path'], mimetype='application/octet-stream', as_attachment=True, download_name=unit_id + '.part')

        @app.post('/agent/units/<unit_id>/result')
        def unit_result(unit_id):
            result = request.files.get('result')
            if not result:
                raise ValueError('缺少结果文件')
            return jsonify(service.accept_result(request.node_id, unit_id, request.form.get('lease_token'),
                                                 result.stream, request.form.get('checksum')))
    else:
        @app.post('/api/connect')
        def connect_node():
            data = request.get_json() or {}
            service.connect(data.get('url'), data.get('token', ''))
            return jsonify(ok=True)

    return app
