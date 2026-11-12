import os
from pathlib import Path
chdir=str(Path(__file__).resolve().parents[1])
bind=os.environ.get('GSA_BIND','127.0.0.1')+':'+os.environ.get('GSA_PORT','8088')
workers=1
threads=int(os.environ.get('GSA_WEB_THREADS','8'))
worker_class='gthread'
timeout=60
graceful_timeout=20
accesslog=None
errorlog='-'
capture_output=True
umask=0o077
preload_app=False


def worker_exit(server,worker):
    application=getattr(worker,'wsgi',None)
    manager=getattr(application,'extensions',{}).get('jobs')
    if manager:manager.shutdown()
