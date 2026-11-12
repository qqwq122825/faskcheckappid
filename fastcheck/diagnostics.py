"""Allowlisted diagnostics. Values from requests, headers and frames never log."""
import os
import queue

from fastcheck.protocols.anisette_v3 import Client, AnisetteError
from fastcheck.protocols.network_timing import NetworkTrace, TimingAdapter
from fastcheck.protocols.timing import phase


class SafeDiagnostics:
    traffic_enabled = True
    allowed = {'event','phase','phase_id','thread_id','clock','at','started_at','ended_at','ms','cpu_ms',
        'http','apple_ec','iterations','error_type','error_chain','os_errors','request_id','connection_id',
        'request_phase','reused','target_dns_mode','owner_thread_id','auth_session_id','http_session_generation',
        'sent_http_bytes','received_http_header_bytes','received_body_bytes','received_http_bytes',
        'send_complete','response_complete'}

    def __init__(self, output, **context):
        self.output, self.context, self.dropped = output, context, 0

    def __call__(self, value):
        safe = {key:item for key,item in value.items() if key in self.allowed}
        safe.update(self.context, child_pid=os.getpid())
        try:
            self.output.put_nowait({'_debug':safe})
        except queue.Full:
            self.dropped += 1


class TracedClient(Client):
    def __init__(self, *args, diagnostics=None, **kwargs):
        super().__init__(*args, diagnostics=diagnostics, **kwargs)
        self.trace = NetworkTrace(diagnostics)
        for scheme in ('http://','https://'):
            old = self.session.get_adapter(scheme)
            self.session.mount(scheme, TimingAdapter(self.trace))
            old.close()

    def _request(self, method, url, stage, **kwargs):
        with self.trace.request('anisette_' + stage):
            with phase(self.diagnostics, 'anisette_' + stage) as timing:
                try:
                    result = super()._request(method, url, stage, **kwargs)
                    timing['http'] = 200
                    return result
                except AnisetteError as exc:
                    if exc.http:
                        timing['http'] = exc.http
                    raise

    def _plist(self, url, state, fields):
        name = 'provisioning_finish' if 'cpim' in fields else 'provisioning_start'
        with phase(self.diagnostics, name):
            return super()._plist(url, state, fields)

    def _provision(self, state):
        with phase(self.diagnostics, 'device_provision_total'):
            return super()._provision(state)

    def headers(self):
        with phase(self.diagnostics, 'anisette_headers_total'):
            return super().headers()
