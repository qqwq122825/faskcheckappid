"""Offline protocol validation with an independent synthetic SRP server."""
import contextlib
import hashlib
import io
import os
import plistlib
import socket
import unittest
from unittest.mock import patch

import fastcheck.single_login as gsa_login
from fastcheck.protocols import gsa_status as gsa
from fastcheck.protocols import srp_auth


def digest(*values):
    return hashlib.sha256(b"".join(values)).digest()


def integer(value):
    return value.to_bytes((value.bit_length() + 7) // 8 or 1, "big")


class SyntheticServer:
    """No network. Computes server-side K and proofs from the SRP equations."""

    def __init__(self, protocol="s2k", *, proof="valid", init_ec=0, complete_ec=0, auth=None):
        self.protocol, self.proof = protocol, proof
        self.init_ec, self.complete_ec, self.auth = init_ec, complete_ec, auth
        self.requests = []

    def post(self, body, cpd, timeout, **kwargs):
        request = plistlib.loads(body)["Request"]
        self.requests.append(request)
        if request["o"] == "init":
            if self.init_ec:
                return gsa.HttpOutcome({"Response": {"Status": {"ec": self.init_ec}}})
            self.A = request["A2k"]
            n, g = srp_auth.N, 2
            salt = bytes.fromhex("91a2c3d4e5f60718293a4b5c6d7e8f90")
            rounds = 1000
            secret = hashlib.sha256("synthetic-password".encode()).digest()
            if self.protocol == "s2k_fo":
                secret = secret.hex().encode()
            derived = hashlib.pbkdf2_hmac("sha256", secret, salt, rounds, 32)
            x = int.from_bytes(digest(salt, digest(b":", derived)), "big")
            verifier = pow(g, x, n)
            k = int.from_bytes(digest(integer(n), g.to_bytes(256, "big")), "big")
            b = int.from_bytes(b"independent synthetic server key", "big")
            B = integer((k * verifier + pow(g, b, n)) % n)
            u = int.from_bytes(digest(self.A, B), "big")
            shared = pow(int.from_bytes(self.A, "big") * pow(verifier, u, n) % n, b, n)
            key = digest(integer(shared))
            xor = bytes(a ^ b for a, b in zip(digest(integer(n)), digest(g.to_bytes(256, "big"))))
            self.M1 = digest(xor, digest(request["u"].encode()), salt, self.A, B, key)
            self.M2 = digest(self.A, self.M1, key)
            return gsa.HttpOutcome({"Response": {"Status": {"ec": 0}, "s": salt,
                "B": B, "i": rounds, "sp": self.protocol, "c": "synthetic-context"}})
        if request["o"] != "complete" or request["c"] != "synthetic-context":
            raise AssertionError("Unexpected authentication context")
        if request["cpd"] != self.requests[0]["cpd"] or request["M1"] != self.M1:
            raise AssertionError("Identity changed or client proof does not match server")
        response = {"Status": {"ec": self.complete_ec}}
        if self.auth:
            response["Status"]["au"] = self.auth
        if self.proof != "missing":
            response["M2"] = self.M2 if self.proof == "valid" else b"!" * 32
        return gsa.HttpOutcome({"Response": response})


class ProtocolTests(unittest.TestCase):
    def setUp(self):
        self.network_guard = patch.object(socket.socket, "connect", side_effect=AssertionError("Network forbidden"))
        self.network_guard.start()
        self.mock_mode = patch.dict(os.environ, {"FASTCHECK_MOCK": ""})
        self.mock_mode.start()
        self.identity = {
            "X-Apple-I-MD": "synthetic-otp", "X-Apple-I-MD-M": "synthetic-machine",
            "X-Apple-I-MD-LU": "synthetic-user", "X-Apple-I-MD-RINFO": "17106176",
            "X-Mme-Device-Id": "synthetic-device", "X-Mme-Client-Info": "synthetic-client",
            "User-Agent": "synthetic-agent",
        }

    def tearDown(self):
        gsa.close_session()
        self.mock_mode.stop()
        self.network_guard.stop()

    def authenticate(self, server):
        with patch.object(gsa, "_session_for", return_value=server):
            return gsa.check("owned@example.invalid", "synthetic-password", None, retries=0, identity=self.identity)

    def test_both_password_protocols_against_server_proof(self):
        for protocol in ("s2k", "s2k_fo"):
            with self.subTest(protocol=protocol):
                server = SyntheticServer(protocol)
                result = self.authenticate(server)
                self.assertEqual(result["status"], "valid")
                self.assertTrue(result["password_verified"])
                self.assertEqual(result["two_factor_status"], "unknown")
                self.assertEqual(len(server.requests), 2)

    def test_missing_and_tampered_server_proofs_are_rejected(self):
        for proof in ("missing", "tampered"):
            with self.subTest(proof=proof):
                result = self.authenticate(SyntheticServer(proof=proof))
                self.assertEqual(result["status"], "error")
                self.assertFalse(result.get("password_verified", False))

    def test_two_factor_requires_valid_server_proof(self):
        result = self.authenticate(SyntheticServer(auth="trustedDeviceSecondaryAuth"))
        self.assertEqual(result["status"], "need_2fa")
        self.assertEqual(result["two_factor_status"], "required")
        bad = self.authenticate(SyntheticServer(auth="trustedDeviceSecondaryAuth", proof="tampered"))
        self.assertEqual(bad["status"], "error")

    def test_any_post_password_auth_challenge_is_two_factor(self):
        for auth_type in ("smsSecondaryAuth", "phoneVerification", " regionalChallenge "):
            with self.subTest(auth_type=auth_type):
                result = self.authenticate(SyntheticServer(auth=auth_type))
                self.assertEqual(result["status"], "need_2fa")
                self.assertEqual(result["two_factor_status"], "required")
                self.assertEqual(result["auth_type"], auth_type.strip())

        result = self.authenticate(SyntheticServer(auth="   "))
        self.assertEqual(result["status"], "valid")
        self.assertEqual(result["auth_type"], "none")

    def test_init_error_is_not_a_password_conclusion(self):
        server = SyntheticServer(init_ec=-22406)
        result = self.authenticate(server)
        self.assertEqual(result["status"], "error")
        self.assertEqual(len(server.requests), 1)

    def test_complete_error_stops_after_one_attempt(self):
        server = SyntheticServer(complete_ec=-22406)
        result = self.authenticate(server)
        self.assertEqual(result["status"], "wrong_password")
        self.assertEqual(len(server.requests), 2)

    def test_actual_http_status_is_recorded_and_does_not_leak_to_next_account(self):
        server = SyntheticServer(init_ec=-20970)
        original = server.post
        def with_http(*args, **kwargs):
            outcome = original(*args, **kwargs)
            outcome.http_status = 200
            return outcome
        server.post = with_http
        self.assertEqual(self.authenticate(server)['http'], 200)
        # A subsequent request without any response must not inherit HTTP 200.
        with patch.object(gsa, '_session_for') as factory:
            factory.return_value.post.return_value = gsa.HttpOutcome(None, error='timeout', kind='network')
            result = gsa.check('other@example.invalid', 'synthetic-password', None,
                               retries=0, identity=self.identity)
        self.assertNotEqual(result.get('http'), 200)

    def test_server_rejects_invalid_public_value(self):
        client = srp_auth.GrandSlamSRPClient("owned@example.invalid", "synthetic-password")
        with self.assertRaises(ValueError):
            client.process_challenge(b"salt", b"\x00", 1000, "s2k")

    def test_cli_refreshes_after_password_input_and_closes_session(self):
        events = []
        class FakeClient:
            def __init__(inner, origin, **kwargs):
                events.append("open")
            def headers(inner):
                events.append("headers")
                return dict(self.identity, **{"X-Apple-I-MD": str(len(events))})
            def close(inner):
                events.append("close")
        def get_password(*args):
            events.append("password")
            return "synthetic-password"
        def check(*args, **kwargs):
            events.append("check")
            self.assertEqual(kwargs["retries"], 0)
            self.assertEqual(kwargs["identity"]["X-Apple-I-MD"], "6")
            return {"status": "need_2fa", "password_verified": True}
        with patch.object(gsa_login, "_dependencies", return_value=(gsa, FakeClient)), \
             patch.object(gsa_login.sys.stdin, "isatty", return_value=True), \
             patch("builtins.input", return_value="owned@example.invalid"), \
             patch.object(gsa_login.getpass, "getpass", side_effect=get_password), \
             patch.object(gsa, "check", side_effect=check), \
             patch.object(gsa, "close_session") as close, \
             patch.dict(os.environ, {}, clear=False), contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(gsa_login.main([]), 3)
            close.assert_called_once()
        self.assertEqual(events, ["open", "headers", "close", "password", "open", "headers", "close", "check"])


if __name__ == "__main__":
    unittest.main()
