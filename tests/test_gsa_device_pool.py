import base64
import hashlib
import json
from pathlib import Path
import socket
import stat
import tempfile
import unittest
from unittest.mock import patch
import uuid

from fastcheck.devices import DevicePool, PoolError, RetryPolicy
from fastcheck.protocols import gsa_status as gsa


REFUSED = {"status": "error", "kind": "protocol", "stage": "init", "apple_ec": -20970}
PASSED = {"status": "valid", "stage": "complete", "password_verified": True}


class FakeClient:
    infos = 0
    registrations = 0
    instances = []
    refreshes = []

    def __init__(self, origin, *, store, client_info=None, **kwargs):
        self.store, self.info, self.closed = store, client_info, False
        self.instances.append(self)

    def client_info(self):
        type(self).infos += 1
        return {"client_info": "synthetic-client", "user_agent": "synthetic-agent"}

    def headers(self):
        state = self.store.load()
        if not state.get("adi_pb"):
            type(self).registrations += 1
            state["adi_pb"] = base64.b64encode(b"synthetic-state").decode()
            self.store.save(state)
        identifier = base64.b64decode(state["identifier"])
        type(self).refreshes.append(identifier)
        return {"X-Apple-I-MD": "synthetic-otp", "X-Apple-I-MD-M": "synthetic-machine",
                "X-Apple-I-MD-LU": hashlib.sha256(identifier).hexdigest(),
                "X-Apple-I-MD-RINFO": "17106176", "X-Mme-Device-Id": str(uuid.UUID(bytes=identifier)),
                "X-Mme-Client-Info": self.info["client_info"], "User-Agent": self.info["user_agent"]}

    def close(self):
        self.closed = True


class DevicePoolTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.now = 1000
        FakeClient.infos = FakeClient.registrations = 0
        FakeClient.instances, FakeClient.refreshes = [], []
        self.network = patch.object(socket.socket, "connect", side_effect=AssertionError("Network forbidden"))
        self.network.start()
        self.mock_mode = patch.dict("os.environ", {"FASTCHECK_MOCK": ""})
        self.mock_mode.start()
        self.pool = self.new_pool()

    def tearDown(self):
        self.mock_mode.stop()
        self.network.stop()
        self.temporary.cleanup()

    def new_pool(self, origin="https://pool.example.invalid/", policy=None):
        return DevicePool(self.root, origin, client_factory=FakeClient,
                          policy=policy or RetryPolicy(backoff=0), clock=lambda: self.now,
                          sleeper=lambda seconds: None)

    def authenticate(self, slot=1, outcomes=None):
        with patch.object(gsa, "check", side_effect=outcomes or [dict(PASSED)]):
            return self.pool.authenticate(slot, "owned@example.invalid", "synthetic-password")

    def test_five_independent_states_survive_restart_without_reprovisioning(self):
        self.pool.prepare()
        self.assertEqual(FakeClient.registrations, 5)
        ids = {self.pool.inspect(slot)["device_id"] for slot in range(1, 6)}
        self.assertEqual(len(ids), 5)
        self.assertEqual(FakeClient.infos, 1)
        restored = self.new_pool()
        records = restored.prepare()
        self.assertTrue(all(item["action"] == "reused" for item in records))
        restored.refresh(3)
        self.assertEqual(FakeClient.registrations, 5)
        self.assertEqual(FakeClient.infos, 1)
        self.assertTrue(all(client.closed for client in FakeClient.instances))

    def test_different_providers_have_independent_state_and_metadata(self):
        self.pool.prepare(1)
        other = self.new_pool("https://other.example.invalid")
        self.assertEqual(other.inspect(1)["state"], "missing")
        other.prepare(1)
        self.assertNotEqual(self.pool.inspect(1)["device_id"], other.inspect(1)["device_id"])
        self.assertEqual(FakeClient.infos, 2)

    def test_selected_device_is_used_for_same_identity_bounded_retry(self):
        self.pool.prepare()
        FakeClient.refreshes = []
        result = self.authenticate(3, [dict(REFUSED), dict(PASSED)])
        self.assertEqual(result["attempts"], 2)
        self.assertEqual(result["device"], 3)
        self.assertEqual(len(FakeClient.refreshes), 2)
        self.assertEqual(FakeClient.refreshes[0], FakeClient.refreshes[1])
        self.assertEqual(str(uuid.UUID(bytes=FakeClient.refreshes[0])), self.pool.inspect(3)["device_id"])
        self.assertEqual(self.pool.health()["devices"]["3"]["failures"], 0)

    def test_five_init_refusals_trigger_persistent_cooldown_and_one_recovery_attempt(self):
        self.pool = self.new_pool(policy=RetryPolicy(attempts=3, backoff=0))
        self.pool.prepare(1)
        self.assertEqual(self.authenticate(outcomes=[dict(REFUSED)] * 3)["attempts"], 3)
        result = self.authenticate(outcomes=[dict(REFUSED)] * 3)
        self.assertEqual(result["attempts"], 2)
        self.assertIn("冷却", result["retry_stopped_reason"])
        self.assertEqual(self.new_pool().health()["devices"]["1"]["failures"], 5)
        with self.assertRaises(PoolError):
            self.authenticate()
        self.now += 601
        self.assertEqual(self.authenticate(outcomes=[dict(REFUSED)] * 3)["attempts"], 1)
        self.now += 601
        self.assertEqual(self.authenticate()["status"], "valid")
        self.assertEqual(self.pool.health()["devices"]["1"]["until"], 0)

    def test_three_distinct_devices_open_provider_circuit(self):
        self.pool = self.new_pool(policy=RetryPolicy(attempts=1, backoff=0))
        self.pool.prepare()
        for slot in (1, 2, 3):
            self.authenticate(slot, [dict(REFUSED)])
        with self.assertRaises(PoolError):
            self.authenticate(4)
        self.now += 601
        # Only one selected device may probe provider recovery across processes.
        with self.pool.operation(1):
            with self.assertRaises(PoolError):
                with self.pool.operation(2):
                    self.fail("Parallel provider recovery must be blocked")
        self.authenticate(1, [dict(REFUSED)])
        self.assertGreater(self.pool.health()["provider_until"], self.now)

    def test_old_refusals_do_not_open_provider_circuit(self):
        self.pool.prepare()
        self.pool.observe(1, dict(REFUSED))
        self.now += 61
        self.pool.observe(2, dict(REFUSED))
        self.pool.observe(3, dict(REFUSED))
        self.assertEqual(self.pool.health()["provider_until"], 0)

    def test_429_password_error_account_lock_and_complete_refusal_do_not_retry(self):
        self.pool.prepare(1)
        cases = [
            {"status": "error", "kind": "ratelimit", "stage": "init"},
            {"status": "wrong_password", "stage": "complete"},
            {"status": "security_lock", "stage": "init", "apple_ec": -20209},
            {"status": "need_2fa", "stage": "complete", "password_verified": True},
            {"status": "error", "stage": "complete", "apple_ec": -20970},
        ]
        for value in cases:
            with self.subTest(value=value):
                result = self.authenticate(outcomes=[value])
                self.assertEqual(result["attempts"], 1)
                self.assertEqual(self.pool.health()["devices"]["1"]["failures"], 0)

    def test_transport_retry_does_not_increment_device_refusal_count(self):
        self.pool.prepare(1)
        result = self.authenticate(outcomes=[{"status": "error", "kind": "network", "stage": "init"}, dict(PASSED)])
        self.assertEqual(result["attempts"], 2)
        self.assertEqual(self.pool.health()["devices"]["1"]["failures"], 0)

    def test_missing_device_never_implicitly_registers_during_refresh_or_login(self):
        with self.assertRaises(PoolError):
            self.pool.refresh(1)
        with self.assertRaises(PoolError):
            self.authenticate()
        self.assertEqual(FakeClient.registrations, 0)
        self.assertEqual(FakeClient.infos, 0)

    def test_exclusive_device_lease_blocks_simultaneous_use(self):
        self.pool.prepare(1)
        with self.pool.operation(1):
            with self.assertRaises(PoolError):
                self.pool.refresh(1)

    def test_state_permissions_and_health_files_do_not_include_credentials_or_otp(self):
        self.pool.prepare(1)
        self.authenticate(outcomes=[dict(REFUSED), dict(REFUSED)])
        health = (self.pool.base / "health.json").read_text()
        self.assertNotIn("owned@example.invalid", health)
        self.assertNotIn("synthetic-password", health)
        self.assertNotIn("synthetic-otp", health)
        measurements = (self.pool.base / "measurements.jsonl").read_text()
        for private in ("owned@example.invalid", "synthetic-password", "synthetic-otp", "synthetic-state"):
            self.assertNotIn(private, measurements)
        for path in self.pool.base.rglob("*.json"):
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        for slot in range(1, 2):
            self.assertEqual(stat.S_IMODE(self.pool.slot_directory(slot).stat().st_mode), 0o700)

    def test_corrupt_state_is_preserved(self):
        self.pool.prepare(1)
        path = self.pool.store(1).path
        path.write_text("not-json")
        with self.assertRaises(PoolError):
            self.pool.prepare(1)
        self.assertEqual(path.read_text(), "not-json")

    def test_timing_write_failure_does_not_hide_authentication_result(self):
        self.pool.prepare(1)
        path = self.pool.base / "measurements.jsonl"
        path.unlink()
        path.mkdir()
        result = self.authenticate()
        self.assertEqual(result["status"], "valid")
        self.assertEqual(self.pool.timing_error, "IsADirectoryError")

    def test_provider_trip_during_refresh_stops_before_apple_request(self):
        self.pool.prepare()
        original = self.pool.fetch_headers
        def refresh_then_trip(*args, **kwargs):
            headers = original(*args, **kwargs)
            for slot in (2, 3, 4):
                self.pool.observe(slot, dict(REFUSED))
            return headers
        with patch.object(self.pool, "fetch_headers", side_effect=refresh_then_trip), \
                patch.object(gsa, "check") as check:
            result = self.pool.authenticate(1, "owned@example.invalid", "synthetic-password")
        check.assert_not_called()
        self.assertEqual(result["kind"], "pool")


if __name__ == "__main__":
    unittest.main()
