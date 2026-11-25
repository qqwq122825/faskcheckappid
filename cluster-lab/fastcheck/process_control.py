"""Stop an owned supervisor and its descendants on supported platforms."""
import os
import signal
import subprocess


def isolate_process():
    if os.name != 'nt':os.setsid()


def terminate_tree(process, *, force=False):
    if os.name == 'nt':
        subprocess.run(['taskkill','/PID',str(process.pid),'/T','/F'],
                       capture_output=True,timeout=10,check=False)
    else:
        try:
            if os.getpgid(process.pid) == process.pid:
                os.killpg(process.pid,signal.SIGKILL if force else signal.SIGTERM)
            elif force:process.kill()
            else:process.terminate()
        except ProcessLookupError:pass
