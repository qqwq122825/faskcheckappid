"""App-local Apple trust for the GSA service; never changes the OS trust store."""
import hashlib
from pathlib import Path
import ssl


# Apple PKI: https://www.apple.com/appleca/AppleIncRootCertificate.cer
APPLE_ROOT_SHA256 = "b0b1730ecbc7ff4505142c49f1295e6eda6bcaed7e2c68c5be91b5a11001f024"
APPLE_ROOT = Path(__file__).resolve().parents[1] / "assets" / "certificates" / "apple-root-ca.pem"


def gsa_ca_file():
    der = ssl.PEM_cert_to_DER_cert(APPLE_ROOT.read_text(encoding="ascii"))
    if hashlib.sha256(der).hexdigest() != APPLE_ROOT_SHA256:
        raise ssl.SSLError("Bundled Apple root certificate fingerprint does not match")
    return str(APPLE_ROOT)


def gsa_context():
    context = ssl.create_default_context()
    context.load_verify_locations(cafile=gsa_ca_file())
    return context
