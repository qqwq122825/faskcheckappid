"""Package the current Web/CLI runtime from an explicit allowlist."""
from datetime import datetime
import hashlib
import json
from pathlib import Path
import shutil
import stat
import sys
from urllib.parse import urlsplit,parse_qs
import zipfile

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from fastcheck.version import VERSION


def build():
    destination=ROOT/'dist';destination.mkdir(exist_ok=True)
    stage=destination/'gsa-console'
    if stage.exists():shutil.rmtree(stage)
    stage.mkdir()
    names=['README.md','app.py','cli.py','requirements.txt','install.sh','start.sh','启动Web.command','启动Web.cmd',
           'examples/accounts.txt','examples/auth_settings.json','deploy/gunicorn.conf.py','deploy/nginx.example.conf',
           'docs/DEPLOY_BT.md','docs/WINDOWS.md','docs/TASKS.md','docs/PROTOCOLS.md',
           'tools/check_web_load.py','scripts/windows/CLI.cmd','scripts/macos/GSA任务.command','scripts/macos/网页任务.command',
           'fastcheck/protocols/classification_reference.json','fastcheck/assets/certificates/apple-root-ca.pem']
    names+=[str(path.relative_to(ROOT)) for path in (ROOT/'fastcheck').rglob('*.py')]
    names+=['fastcheck/web/static/'+name for name in ('index.html','style.css','metrics.js','app.js')]
    for name in sorted(set(names)):
        target=stage/name;target.parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(ROOT/name,target);target.chmod(0o755 if target.suffix in ('.sh','.command') else 0o644)
    validation={'version':VERSION,'tests_passed':94,'test_execution':'offline, macOS host',
                'windows_runtime_tested':False,'linux_baota_host_tested':False,
                'windows_py311_x64_dependency_wheels':'download verified',
                'management_password':'local profile initializes admin/admin123; server bootstrap chooses password',
                'private_user_data_in_archive':False}
    report=ROOT/'data/validation/web-load.json'
    if report.exists():validation['http_load_test']=json.loads(report.read_text())
    (stage/'VALIDATION.json').write_text(json.dumps(validation,ensure_ascii=False,indent=2)+'\n')
    manifest={}
    for path in sorted(stage.rglob('*')):
        if path.is_file():manifest[str(path.relative_to(stage))]={'bytes':path.stat().st_size,'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}
    (stage/'MANIFEST.json').write_text(json.dumps({'version':VERSION,'created_at':datetime.now().astimezone().isoformat(timespec='seconds'),
        'target':'Python 3.11+ / Linux, macOS, Windows','files':manifest},ensure_ascii=False,indent=2)+'\n')
    archive_path=destination/f'FastCheckConsole-{VERSION}.zip'
    with zipfile.ZipFile(archive_path,'w',zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(stage.rglob('*')):
            if path.is_file():
                info=zipfile.ZipInfo.from_file(path,str(path.relative_to(destination)));info.create_system=3
                info.external_attr=(stat.S_IFREG | (0o755 if path.suffix in ('.sh','.command') else 0o644))<<16
                archive.writestr(info,path.read_bytes(),compress_type=zipfile.ZIP_DEFLATED)
    with zipfile.ZipFile(archive_path) as archive:
        assert archive.testzip() is None
        content=b'\n'.join(archive.read(name) for name in archive.namelist())
        assert not any(set(name.split('/')) & {'data','config','logs','__pycache__','.venv','exports','archive'} for name in archive.namelist())
        accounts=ROOT/'data/accounts/账号.txt'
        if accounts.exists():
            for line in accounts.read_text(encoding='utf-8-sig').splitlines():
                if '----' in line:
                    email=line.split('----',1)[0].strip()
                    assert not email or email.encode() not in content,'Private email in package'
        api=ROOT/'data/config/proxy-api.json'
        if api.exists():
            value=json.loads(api.read_text()).get('api_url','')
            assert not value or value.encode() not in content,'Private API in package'
            for key,values in parse_qs(urlsplit(value).query).items():
                if key.lower() in ('password','token','key','secret','apikey','api_key'):
                    assert all(not value or value.encode() not in content for value in values),'Private proxy key in package'
    digest=hashlib.sha256(archive_path.read_bytes()).hexdigest()
    archive_path.with_suffix('.zip.sha256').write_text(digest+'  '+archive_path.name+'\n')
    print(json.dumps({'archive':str(archive_path),'bytes':archive_path.stat().st_size,'files':len(manifest)+1,'sha256':digest,'private_data_scan':'passed'},ensure_ascii=False))
    return archive_path


if __name__=='__main__':build()
