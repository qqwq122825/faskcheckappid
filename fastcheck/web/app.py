from dataclasses import asdict,fields
from datetime import timedelta
import io
import json
import os
from pathlib import Path
import secrets
import tempfile
import threading
import time
import zipfile

from flask import Flask,jsonify,request,session,send_file
from werkzeug.security import check_password_hash

from fastcheck.scheduler import Settings
from fastcheck.exports import FILES,LABELS
from fastcheck.task_io import Account,read_accounts
from fastcheck.devices import save_json,private_directory
from fastcheck.settings_store import load_api,save_api
from fastcheck.status import result_bucket
from fastcheck.version import VERSION
from .bootstrap import ROOT,data_root
from .runtime import JobManager
from fastcheck.device_build import DeviceBuilder


def create_app(data_dir=None):
    root=private_directory(Path(data_dir) if data_dir else data_root())
    auth_path=root/'config/web-admin.json'
    if not auth_path.exists():raise RuntimeError('请先运行 .venv/bin/python -m server.bootstrap 设置管理密码。')
    auth=json.loads(auth_path.read_text());auth_path.chmod(0o600)
    app=Flask(__name__,static_folder='static',static_url_path='/static')
    app.secret_key=auth['session_secret'];app.json.ensure_ascii=False
    app.config.update(MAX_CONTENT_LENGTH=int(os.environ.get('GSA_MAX_UPLOAD_MB','8'))*1024*1024,
                      SESSION_COOKIE_HTTPONLY=True,SESSION_COOKIE_SAMESITE='Lax',
                      SESSION_COOKIE_SECURE=os.environ.get('GSA_COOKIE_SECURE')=='1',
                      PERMANENT_SESSION_LIFETIME=timedelta(hours=8))
    manager=JobManager(root);app.extensions['jobs']=manager
    builder=DeviceBuilder(root,Settings().origin,root/'config/batch-device-pool')
    app.extensions['builder']=builder
    login_attempts={};login_lock=threading.Lock()

    def defaults():
        path=root/'config/web-settings.json'
        if path.exists():return json.loads(path.read_text())
        path=ROOT/'examples/auth_settings.json'
        return json.loads(path.read_text()) if path.exists() else asdict(Settings(max_attempts=1,device_parallelism=0,device_selection='prefer_new'))

    def options(data):
        if not isinstance(data,dict):raise ValueError('任务配置字段无效。')
        extra=set(data)-{f.name for f in fields(Settings)}
        if extra:raise ValueError('配置里有当前版本不认识的字段：'+'、'.join(sorted(extra))+'。请刷新页面或更新服务端。')
        values=defaults();values.update(data)
        if type(values.get('debug')) is not bool:raise ValueError('调试模式必须为布尔值。')
        return Settings(**values)

    @app.before_request
    def protect():
        if request.method in ('POST','PUT','DELETE'):
            token=request.headers.get('X-CSRF-Token','')
            if not token or not secrets.compare_digest(token,session.get('csrf','')):
                return jsonify(error='页面会话已失效，请刷新。'),403
        if request.path.startswith('/api/') and request.path not in ('/api/session','/api/login') and not session.get('authenticated'):
            return jsonify(error='请先登录。'),401

    @app.after_request
    def headers(response):
        response.headers['Cache-Control']='no-store'
        response.headers['X-Content-Type-Options']='nosniff'
        response.headers['X-Frame-Options']='DENY'
        response.headers['Content-Security-Policy']="default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'"
        return response

    @app.get('/')
    def index():return app.send_static_file('index.html')

    @app.get('/healthz')
    def health():return jsonify(ok=True,version=VERSION)

    @app.get('/api/session')
    def current_session():
        session.setdefault('csrf',secrets.token_urlsafe(32))
        return jsonify(authenticated=bool(session.get('authenticated')),csrf=session['csrf'],version=VERSION)

    @app.post('/api/login')
    def login():
        data=request.get_json(silent=True) or {};password=data.get('password','')
        ip=request.remote_addr or 'local';now=time.monotonic()
        with login_lock:
            values=[v for v in login_attempts.get(ip,[]) if now-v<60]
            if len(values)>=10:return jsonify(error='登录尝试过多，请稍后再试。'),429
            values.append(now);login_attempts[ip]=values
        if not isinstance(password,str) or data.get('username','admin')!='admin' or not check_password_hash(auth['password_hash'],password):
            return jsonify(error='用户名或密码错误。'),401
        session.clear();session.update(authenticated=True,csrf=secrets.token_urlsafe(32));session.permanent=True
        return jsonify(authenticated=True,csrf=session['csrf'])

    @app.post('/api/logout')
    def logout():session.clear();return jsonify(ok=True)

    @app.get('/api/settings')
    def settings_get():return jsonify(settings=defaults(),proxy_saved=bool(load_api(root)))

    @app.post('/api/settings')
    def settings_save():
        data=request.get_json(silent=True) or {};selected=options(data.get('settings',{}))
        api=data.get('api_url','').strip()
        if api:save_api(root,api)
        save_json(root/'config/web-settings.json',asdict(selected))
        return jsonify(ok=True,proxy_saved=bool(load_api(root)))

    @app.get('/api/jobs')
    def jobs():return jsonify(jobs=manager.history())

    @app.post('/api/jobs')
    def start():
        accounts=[]
        try:
            selected=options(json.loads(request.form.get('settings','{}')))
            simulation=request.form.get('simulation')=='true'
            api=request.form.get('api_url','').strip()
            if api:save_api(root,api)
            if simulation:
                count=int(request.form.get('demo_count','20'))
                if count<1:raise ValueError('演示任务数必须为正整数。')
                accounts=[Account(n,f'demo-{n}@example.invalid','demo-password') for n in range(1,count+1)]
            else:
                from fastcheck.protocols import gsa_status
                if gsa_status.mock_enabled():raise ValueError('请清除 FASTCHECK_MOCK；真实任务不接受模拟认证。')
                if not load_api(root):raise ValueError('请先填写代理 API。')
                upload=request.files.get('accounts')
                raw=upload.read() if upload else request.form.get('accounts_text','').encode('utf-8')
                if not raw:raise ValueError('请选择账号文件或粘贴账号。')
                directory=private_directory(root/'uploads')
                with tempfile.NamedTemporaryFile(dir=directory,delete=False) as handle:
                    path=Path(handle.name);os.chmod(path,0o600);handle.write(raw)
                try:
                    limit=int(os.environ.get('GSA_MAX_ACCOUNTS','0'))
                    accounts=read_accounts(path,max_accounts=limit or None)
                finally:path.unlink(missing_ok=True)
            state=manager.start(accounts,selected,request.form.get('proxy_auth',''),simulation)
            return jsonify(state),202
        finally:
            for account in accounts:account.password=''

    @app.post('/api/proxy/check')
    def proxy_check():
        """取一条出口，实际打一次目标站，回报出口 IP 与延迟。"""
        import time as _time
        import requests as _requests
        from fastcheck.proxy_pool import fetch_proxies
        from fastcheck.protocols.proxy_api import inject_auth, mask
        data = request.get_json(silent=True) or {}
        selected = options(data.get('settings', {}))
        api = (data.get('api_url') or '').strip() or load_api(root)
        if not api:
            raise ValueError('请先填写或保存代理 API 地址。')
        scheme = None if selected.proxy_scheme == 'http' else selected.proxy_scheme
        started = _time.monotonic()
        try:
            proxies = fetch_proxies(api, 1, scheme=scheme)
        except Exception as exc:
            raise ValueError(f'提取失败：{type(exc).__name__}') from None
        if not proxies:
            raise ValueError('提取返回空，供应商没给出口。')
        auth = (data.get('proxy_auth') or '').strip()
        proxy = inject_auth(proxies, auth)[0] if auth else proxies[0]
        fetch_ms = round((_time.monotonic()-started)*1000)
        started = _time.monotonic()
        try:
            response = _requests.get('https://api.ipify.org',
                                     proxies={'http': proxy, 'https': proxy},
                                     timeout=min(20, selected.timeout*2))
        except Exception as exc:
            raise ValueError(f'出口不可用：{type(exc).__name__}') from None
        return jsonify(ok=True, proxy=mask(proxy), exit_ip=response.text.strip()[:64],
                       fetch_ms=fetch_ms, request_ms=round((_time.monotonic()-started)*1000))

    @app.post('/api/devices/build')
    def devices_build_start():
        data = request.get_json(silent=True) or {}
        selected = options(data.get('settings', {}))
        api = (data.get('api_url') or '').strip() or load_api(root)
        if not api:
            raise ValueError('请先填写或保存代理 API 地址。')
        builder.start(selected.device_capacity, api, selected.proxy_scheme,
                      (data.get('proxy_auth') or '').strip())
        return jsonify(builder.snapshot(selected.device_capacity)), 202

    @app.get('/api/devices/build')
    def devices_build_status():
        selected = options({})
        return jsonify(builder.snapshot(selected.device_capacity))

    @app.post('/api/devices/build/cancel')
    def devices_build_cancel():
        builder.cancel()
        return jsonify(ok=True)

    @app.post('/api/jobs/clear')
    def clear_jobs():return jsonify(removed=manager.clear_history(),jobs=manager.history())

    @app.get('/api/jobs/<job_id>')
    def status(job_id):return jsonify(manager.get(job_id))

    @app.post('/api/jobs/<job_id>/stop')
    def stop(job_id):manager.stop(job_id);return jsonify(ok=True),202

    @app.get('/api/jobs/<job_id>/results')
    def results(job_id):
        category=request.args.get('category','all')
        offset=max(0,int(request.args.get('offset',0)));limit=min(200,max(1,int(request.args.get('limit',100))))
        return jsonify(manager.read_results(job_id,category,offset,limit))

    @app.get('/api/jobs/<job_id>/exports/<category>')
    def export(job_id,category):
        if category not in FILES:raise FileNotFoundError
        path=manager.directory(job_id)/'exports'/FILES[category]
        if not path.exists():raise FileNotFoundError
        return send_file(path,as_attachment=True,download_name=LABELS[category]+'.txt')

    @app.get('/api/jobs/<job_id>/logs')
    def logs(job_id):
        directory=manager.directory(job_id);buffer=io.BytesIO()
        names=('events.jsonl','results.jsonl','summary.json','debug.jsonl','debug-summary.json','state.json')
        if sum((directory/name).stat().st_size for name in names if (directory/name).exists())>64*1024*1024:
            return jsonify(error='日志较大，请从服务器任务目录获取。'),413
        with zipfile.ZipFile(buffer,'w',zipfile.ZIP_DEFLATED) as archive:
            for name in names:
                if (directory/name).exists():archive.write(directory/name,name)
            details=directory/'exports/undetermined-details.jsonl'
            if details.exists():archive.write(details,'undetermined-details.jsonl')
        buffer.seek(0)
        return send_file(buffer,as_attachment=True,download_name='logs-'+job_id+'.zip')

    @app.errorhandler(ValueError)
    def invalid(error):return jsonify(error=str(error)),400
    @app.errorhandler(TypeError)
    def wrong_type(error):return jsonify(error='配置字段类型无效。'),400
    @app.errorhandler(RuntimeError)
    def conflict(error):return jsonify(error=str(error)),409
    @app.errorhandler(FileNotFoundError)
    def missing(error):return jsonify(error='记录尚未生成或不存在。'),404
    @app.errorhandler(413)
    def large(error):return jsonify(error='上传文件超过服务器设置的大小。'),413
    return app
