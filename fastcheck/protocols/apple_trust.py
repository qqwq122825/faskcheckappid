"""Apple TLS contexts shared by the local runner and Linux node executor.

The public Apple chain is valid in the operating-system trust store, but the
Python.org macOS runtime uses a separate OpenSSL/Certifi store.  Using only the
bundled legacy Apple root made that runtime reject the current GSA chain while
the system ``curl`` accepted it.  ``truststore`` keeps certificate validation
and hostname checks enabled while delegating platform verification to the same
native store used by the host.  The bundled, fingerprint-checked Apple root is
also loaded so minimal Linux images do not depend on an incidental CA package.
"""
import hashlib
from pathlib import Path
import ssl

from requests.adapters import HTTPAdapter


# Apple PKI: https://www.apple.com/appleca/AppleIncRootCertificate.cer
APPLE_ROOT_SHA256 = "b0b1730ecbc7ff4505142c49f1295e6eda6bcaed7e2c68c5be91b5a11001f024"
APPLE_ROOT = Path(__file__).resolve().parents[1] / "assets" / "certificates" / "apple-root-ca.pem"


def gsa_ca_file():
    der = ssl.PEM_cert_to_DER_cert(APPLE_ROOT.read_text(encoding="ascii"))
    if hashlib.sha256(der).hexdigest() != APPLE_ROOT_SHA256:
        raise ssl.SSLError("Bundled Apple root certificate fingerprint does not match")
    return str(APPLE_ROOT)


def gsa_context():
    try:
        import truststore
    except ImportError:  # dependency installation errors remain diagnosable
        context = ssl.create_default_context()
    else:
        context = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.load_verify_locations(cafile=gsa_ca_file())
    context.check_hostname = True
    context.verify_mode = ssl.CERT_REQUIRED
    return context


class AppleTrustAdapter(HTTPAdapter):
    """Give urllib3 pools and SOCKS proxy pools the native Apple TLS context."""

    def __init__(self, *args, ssl_context=None, **kwargs):
        self._apple_ssl_context = ssl_context or gsa_context()
        super().__init__(*args, **kwargs)

    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        pool_kwargs["ssl_context"] = self._apple_ssl_context
        return super().init_poolmanager(connections, maxsize, block, **pool_kwargs)

    def proxy_manager_for(self, proxy, **proxy_kwargs):
        # SOCKSProxyManager is created lazily, so init_poolmanager alone does
        # not affect proxied Apple traffic.
        proxy_kwargs["ssl_context"] = self._apple_ssl_context
        return super().proxy_manager_for(proxy, **proxy_kwargs)
