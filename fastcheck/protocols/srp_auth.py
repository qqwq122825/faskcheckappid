"""Apple SRP-6a with s2k / s2k_fo password derivation.

Protocol logic is unchanged from the shipped FastCheckV2 build (verified against
its bytecode): Apple's GSA does NOT use textbook SRP-6a - it derives x from
PBKDF2(SHA256(password)) and offers either the raw digest (`s2k`) or its hex
string (`s2k_fo`) as the PBKDF2 secret.
"""
from __future__ import annotations

import hashlib
import hmac
import secrets

# Apple's 2048-bit SRP group (RFC 5054 group-14 prime, Apple's own generator g=2)
N_HEX = (
    "AC6BDB41324A9A9BF166DE5E1389582FAF72B6651987EE07FC3192943DB56050A37329CBB4"
    "A099ED8193E0757767A13DD52312AB4B03310DCD7F48A9DA04FD50E8083969EDB767B0CF"
    "6095179A163AB3661A05FBD5FAAAE82918A9962F0B93B855F97993EC975EEAA80D740ADB"
    "F4FF747359D041D5C33EA71D281E446B14773BCA97B43A23FB801676BD207A436C6481F1"
    "D2B9078717461A5B9D32E688F87748544523B524B0D57D5EA77A2775D2ECFA032CFBDBF5"
    "2FB3786160279004E57AE6AF874E7303CE53299CCC041C7BC308D82A5698F3A8D0C38271"
    "AE35F8E9DBFBB694B5C803D89F7AE435DE236D525F54759B65E372FCD68EF20FA7111F9E"
    "4AFF73"
)
N = int(N_HEX, 16)
g = 2
N_BYTES = (N.bit_length() + 7) // 8


def H(*chunks: bytes) -> bytes:
    h = hashlib.sha256()
    for chunk in chunks:
        h.update(chunk)
    return h.digest()


def i2b(value: int, length: int | None = None) -> bytes:
    if length is None:
        length = (value.bit_length() + 7) // 8 or 1
    return value.to_bytes(length, "big")


def b2i(raw: bytes) -> int:
    return int.from_bytes(raw, "big")


def pad(x, n_bytes: int = N_BYTES) -> bytes:
    if isinstance(x, int):
        return i2b(x, n_bytes)
    return x.rjust(n_bytes, b"\x00")


def s2k(password: str, salt: bytes, iters: int) -> bytes:
    """PBKDF2(sha256(password), salt, iters) - 32 bytes."""
    p = hashlib.sha256(password.encode("utf-8")).digest()
    return hashlib.pbkdf2_hmac("sha256", p, salt, iters, 32)


def s2k_fo(password: str, salt: bytes, iters: int) -> bytes:
    """PBKDF2(hex(sha256(password)) as ascii, salt, iters) - 32 bytes."""
    p = hashlib.sha256(password.encode("utf-8")).hexdigest().encode("utf-8")
    return hashlib.pbkdf2_hmac("sha256", p, salt, iters, 32)


class SRPClient:
    """Client half of Apple's GSA SRP handshake (one instance per account)."""

    __slots__ = ("I", "P", "a", "A", "A_bytes", "k", "K", "M1", "M2_expected")

    def __init__(self, username: str, password: str):
        self.I = username.encode("utf-8")
        self.P = password
        self.a = b2i(secrets.token_bytes(32))
        self.A = pow(g, self.a, N)
        self.A_bytes = pad(self.A)
        self.k = b2i(H(i2b(N), pad(g)))
        self.K = b""
        self.M1 = b""
        self.M2_expected = b""

    def process_challenge(self, salt: bytes, B_bytes: bytes, iters: int, ps: str) -> bytes:
        """Consume the server challenge, return M1 (the proof we send back)."""
        B = b2i(B_bytes)
        if B % N == 0:
            raise ValueError("invalid server B (B % N == 0)")
        u = b2i(H(self.A_bytes, B_bytes))
        if u == 0:
            raise ValueError("invalid server u == 0")
        P_key = s2k_fo(self.P, salt, iters) if ps == "s2k_fo" else s2k(self.P, salt, iters)
        inner = H(b":", P_key)
        x = b2i(H(salt, inner))
        S = pow((B - self.k * pow(g, x, N)) % N, self.a + u * x, N)
        self.K = H(i2b(S))
        hN_xor_hg = bytes(a ^ b for a, b in zip(H(i2b(N)), H(pad(g))))
        self.M1 = H(hN_xor_hg, H(self.I), salt, self.A_bytes, B_bytes, self.K)
        self.M2_expected = H(self.A_bytes, self.M1, self.K)
        return self.M1

    def verify_M2(self, server_M2: bytes) -> bool:
        return len(self.M2_expected) == 32 and len(server_M2) == 32 and hmac.compare_digest(server_M2, self.M2_expected)


class GrandSlamSRPClient(SRPClient):
    """GSA hashes minimal big-endian A/B; the web client's padding is separate."""
    __slots__ = ()

    def __init__(self, username: str, password: str):
        if len(username) > 4096 or len(password) > 4096:
            raise ValueError("GSA credential exceeds limit")
        super().__init__(username, password)
        if not self.a:
            raise ValueError("Invalid SRP private ephemeral")
        self.A_bytes = i2b(self.A)

    def process_challenge(self, salt: bytes, B_bytes: bytes, iters: int, ps: str) -> bytes:
        if not isinstance(salt, bytes) or not 1 <= len(salt) <= 1024:
            raise ValueError("Invalid GSA salt")
        if not isinstance(B_bytes, bytes) or not 1 <= len(B_bytes) <= N_BYTES + 1:
            raise ValueError("Invalid GSA public key")
        if type(iters) is not int or not 1 <= iters <= 1_000_000:
            raise ValueError("Invalid GSA iteration count")
        if ps not in ("s2k", "s2k_fo"):
            raise ValueError("Unsupported GSA password protocol")
        return super().process_challenge(salt, i2b(b2i(B_bytes)), iters, ps)
