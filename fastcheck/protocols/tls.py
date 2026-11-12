"""Reuse the default verified CA context within each worker process.

Only default verification without a client certificate uses the cache. Custom
trust bundles, client certificates and verify=False retain requests' own pools.
Cookies, HTTP sessions and authenticated connections are never cached here.
"""
import ssl
import threading
from contextvars import ContextVar

from requests.adapters import DEFAULT_CA_BUNDLE_PATH, HTTPAdapter
from requests.utils import select_proxy
from urllib3.util.ssl_ import ALPN_PROTOCOLS, create_urllib3_context

from .timing import phase

_context = None
_lock = threading.Lock()
_cache_allowed = ContextVar("default_tls_cache_allowed", default=True)


def default_context(diagnostics=None):
    global _context
    if _context is None:
        with phase(diagnostics, "tls_cache_wait"):
            with _lock:
                if _context is None:
                    with phase(diagnostics, "tls_ca_load"):
                        context = create_urllib3_context(cert_reqs=ssl.CERT_REQUIRED)
                        context.load_verify_locations(DEFAULT_CA_BUNDLE_PATH)
                        context.set_alpn_protocols(ALPN_PROTOCOLS)
                        # Publish only after all security settings and CAs load.
                        _context = context
    return _context


class VerifiedTLSAdapter(HTTPAdapter):
    def __init__(self, diagnostics=None, **kwargs):
        self.tls_diagnostics = diagnostics
        super().__init__(**kwargs)

    def build_connection_pool_key_attributes(self, request, verify, cert=None):
        host, options = super().build_connection_pool_key_attributes(request, verify, cert)
        if host["scheme"] == "https" and verify is True and not cert and _cache_allowed.get():
            options["ssl_context"] = default_context(self.tls_diagnostics)
            options.pop("ca_certs", None)
            options.pop("ca_cert_dir", None)
        return host, options

    def get_connection_with_tls_context(self, request, verify, proxies=None, cert=None):
        proxy = select_proxy(request.url, proxies)
        # HTTPS proxies have an independent outer TLS trust configuration.
        # Keep requests' complete handling for that less common transport.
        token = _cache_allowed.set(not (proxy and proxy.lower().startswith("https://")))
        try:
            return super().get_connection_with_tls_context(request, verify, proxies, cert)
        finally:
            _cache_allowed.reset(token)

    def cert_verify(self, conn, url, verify, cert):
        super().cert_verify(conn, url, verify, cert)
        # urllib3 otherwise loads these files again even with an SSLContext.
        # Clear them only for the exact, fully loaded default verified context.
        if (verify is True and not cert and _context is not None
                and conn.conn_kw.get("ssl_context") is _context):
            conn.ca_certs = None
            conn.ca_cert_dir = None
