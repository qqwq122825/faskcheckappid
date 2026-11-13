"""Read-only host measurements. No packet inspection or traffic shaping."""
import time

try:
    import psutil
except ImportError:
    psutil = None


class HostTelemetry:
    def __init__(self):
        self.previous = None
        self.primed = False
        self.latest = {}

    def sample(self):
        if psutil is None:
            return {}
        try:
            now = time.monotonic()
            counters = psutil.net_io_counters(pernic=True)
            links = psutil.net_if_stats()
            active = [value for name, value in counters.items()
                      if not name.lower().startswith(('lo', 'loopback'))
                      and name in links and links[name].isup]
            sent = sum(value.bytes_sent for value in active)
            received = sum(value.bytes_recv for value in active)
            upload, download = None, None
            if self.previous:
                elapsed = max(.01, now - self.previous[0])
                upload = max(0, sent - self.previous[1]) / elapsed
                download = max(0, received - self.previous[2]) / elapsed
            cpu = psutil.cpu_percent(interval=None)
            self.latest = {'cpu_percent': round(cpu, 1) if self.primed else None,
                           'memory_percent': round(psutil.virtual_memory().percent, 1),
                           'upload_bps': round(upload, 1) if upload is not None else None,
                           'download_bps': round(download, 1) if download is not None else None,
                           'sampled_at': time.time(), 'scope': 'host_non_loopback'}
            self.primed = True
            self.previous = (now, sent, received)
        except (OSError, psutil.Error):
            self.latest = {}
        return dict(self.latest)
