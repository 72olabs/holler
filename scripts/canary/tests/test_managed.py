from __future__ import annotations

import copy
from contextlib import closing
import io
import json
import os
from pathlib import Path
import sqlite3
import struct
import sys
import tempfile
import unittest
from unittest.mock import patch

SCRIPT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPT_DIR))

from catalog import CatalogError, load_catalog, validate_scenario
from handler_contract import load_handler
from managed import exchange, receive_exact
from worker import BudgetedInteractiveSession, CanaryFailure, PtyProcess, Worker
from budget import BudgetExceeded, BudgetLedger


class FrameTests(unittest.TestCase):
    def test_graceful_exit_then_close_is_idempotent(self):
        process = object.__new__(PtyProcess)
        read_fd, write_fd = os.pipe()
        self.addCleanup(os.close, write_fd)
        process.master = read_fd
        process.process = type("Child", (), {"poll": lambda _self: 0})()
        process.selector = type("Selector", (), {"close": lambda _self: None})()
        process._sweep_process_group = lambda: None
        process.graceful_claude_exit()
        process.close()
        process.graceful_claude_exit()
        self.assertEqual(process.master, -1)

    def test_wake_reserves_budget_before_trigger_and_sends_no_input(self):
        session = object.__new__(BudgetedInteractiveSession)
        session.client = "claude"
        session.marker_suffix = "SUFFIX"
        session.ledger = BudgetLedger({"claude_usd": 1, "codex_reported_tokens": 100,
                                       "model_turns": 1, "wall_seconds": 60})
        observed = []
        session.process = type("Process", (), {
            "checkpoint": lambda _self: 17,
            "wait_for": lambda _self, marker, timeout, after: observed.append((marker, after)),
        })()
        self.assertEqual(session.wake(lambda: "sent", "DONE_SUFFIX"), "sent")
        self.assertEqual(observed, [("DONE_SUFFIX", 17)])
        self.assertEqual(session.ledger.model_turns, 1)
        with self.assertRaises(BudgetExceeded):
            session.wake(lambda: self.fail("over-budget trigger executed"), "DONE_SUFFIX")

    def test_bounded_frames_and_correlation(self):
        class Connection:
            def __init__(self, data):
                self.data = io.BytesIO(data)
            def recv(self, n):
                return self.data.read(min(n, 2))
            def sendall(self, _raw):
                pass
        raw = json.dumps({"id": 2, "ok": True, "result": []}).encode()
        self.assertTrue(exchange(Connection(struct.pack(">I", len(raw)) + raw), 2, "ping", {})["ok"])
        with self.assertRaisesRegex(ValueError, "correlation"):
            exchange(Connection(struct.pack(">I", len(raw)) + raw), 3, "ping", {})
        with self.assertRaisesRegex(ValueError, "limit"):
            receive_exact(Connection(b""), (2 << 20) + 1)
        with self.assertRaisesRegex(ValueError, "truncated"):
            receive_exact(Connection(b"a"), 2)

    def test_daemon_configuration_is_closed_and_hash_bound(self):
        scenario = load_catalog()["C9"]
        validate_scenario(scenario, Path("C9.json"))
        for change in ({"conversations": False}, {"address": "0.0.0.0"}):
            altered = copy.deepcopy(scenario)
            altered["daemon"].update(change)
            with self.assertRaises(CatalogError):
                validate_scenario(altered, Path("C9.json"))
        altered = copy.deepcopy(scenario)
        altered["test_environment"]["human"] = "real-human"
        with self.assertRaises(CatalogError):
            validate_scenario(altered, Path("C9.json"))


class ManagedDaemonTests(unittest.TestCase):
    """No hand-written schema: all tables/data are created by the real daemon/API."""

    def setUp(self):
        root = SCRIPT_DIR.parents[1]
        holler = root / ".build" / "holler"
        hollerd = root / ".build" / "hollerd"
        if not holler.is_file() or not hollerd.is_file():
            if os.environ.get("HOLLER_REQUIRE_MANAGED_FIXTURE_TEST") == "1":
                self.fail("real holler/hollerd binaries required; run scripts/build.sh first")
            self.skipTest("real binaries unavailable; run scripts/build.sh (required by CI)")
        # Short Unix paths also work with macOS's 104-byte socket limit.
        self.temp = tempfile.TemporaryDirectory(prefix="hmc-", dir="/tmp")
        self.addCleanup(self.temp.cleanup)
        w = object.__new__(Worker)
        w.root = Path(self.temp.name)
        w.fixture = w.root
        w.runtime = w.root / "runtime"
        w.socket = w.runtime / "h.sock"
        w.database = w.runtime / "db.sqlite3"
        w.holler, w.hollerd = holler, hollerd
        w.env = os.environ.copy()
        w.daemon = None
        w.managed_config = load_catalog()["C9"]["daemon"]
        w.human_url = ""
        w.human_credentials = w.runtime / "human" / "credentials.json"
        self.worker = w
        self.addCleanup(w.stop_daemon)
        w.start_daemon()
        self.fixture = w.managed_fixture()

    def test_c9_full_protocol_and_synthetic_http_workflow(self):
        context = type("Context", (), {})()
        context.managed = lambda: self.fixture
        checks = []
        context.check = checks.append
        result = load_handler(SCRIPT_DIR / "handlers" / "C9.py")(context)
        self.assertEqual(result, load_catalog()["C9"]["checks"])
        self.assertIn("c9-view-restart", checks)

    def test_real_schema_projection_and_public_terminal_state(self):
        f = self.fixture
        a, b, controller = "canary-claude", "canary-codex", "canary-controller"
        for actor in (a, b, controller):
            f.api(actor, "channel.list")
        channel = f.create(controller, [controller, a, b], "projection")
        message = f.post(controller, channel["channel_id"], "projection-message", attention=[a, b])["message"]
        mid = message["message_id"]
        claim = f.api(a, "channel.claim", {"message_id": mid})
        f.api(a, "channel.delivery", {"message_id": mid, "lease_token": claim["lease_token"], "action": "ack"})
        rows = {row["recipient_actor"]: row for row in f.terminal([mid])}
        self.assertEqual((rows[a]["state"], rows[a]["attempt"], rows[a]["claims"], rows[a]["acks"]), ("acked", 1, 1, 1))
        self.assertEqual((rows[b]["state"], rows[b]["attempt"]), ("queued", 0))
        self.assertEqual(set(rows[a]), {"message_id", "recipient_actor", "state", "attempt", "claims", "acks", "attention_attempts", "attention_adapters"})
        self.assertEqual(f.inbox(a), [])
        self.assertEqual(f.inbox(b)[0]["message"]["message_id"], mid)
        f.api(a, "channel.claim", {"message_id": mid}, error="no_message")
        self.assertEqual(f.api(b, "channel.message", {"message_id": mid})["channel_id"], channel["channel_id"])

    def test_projection_rejects_live_clients_before_stopping(self):
        with patch.object(self.worker, "actor_directory", return_value={"actors": [{"sessions": [{"state": "live"}]}]}):
            with self.assertRaisesRegex(CanaryFailure, "sessions ended"):
                self.worker.managed_snapshot(["msg_test"])
        self.assertIsNone(self.worker.daemon.poll())

    def test_non_attended_member_is_delivered_but_never_notified(self):
        f = self.fixture
        a, b, controller = "canary-claude", "canary-codex", "canary-controller"
        for actor in (a, b, controller):
            f.api(actor, "channel.list")
        channel = f.create(controller, [controller, a, b], "attention-separation")
        mid = f.post(controller, channel["channel_id"], "attention-one", attention=[a])["message"]["message_id"]
        pending = f.inbox(b)
        self.assertEqual([(d["message"]["message_id"], d["state"], d["attempt"]) for d in pending], [(mid, "queued", 0)])
        states = {d["recipient_actor"]: d for d in f.terminal([mid])}
        self.assertEqual(states[b], {"message_id": mid, "recipient_actor": b, "state": "queued", "attempt": 0,
                                     "claims": 0, "acks": 0, "attention_attempts": 0, "attention_adapters": 0})

    def test_projection_rejects_running_daemon_and_remaining_socket(self):
        with patch.object(self.worker, "stop_daemon", return_value=None):
            with self.assertRaisesRegex(CanaryFailure, "stopped daemon"):
                self.worker.managed_snapshot(["msg_test"])

    def test_projection_schema_pin_and_restart_on_failure(self):
        real_stop = self.worker.stop_daemon
        def stop_and_change_version():
            real_stop()
            with closing(sqlite3.connect(self.worker.database)) as connection:
                connection.execute("DELETE FROM schema_migrations WHERE version=16")
                connection.commit()
        with patch.object(self.worker, "stop_daemon", side_effect=stop_and_change_version):
            with self.assertRaisesRegex(CanaryFailure, "schema 16"):
                self.worker.managed_snapshot(["msg_test"])
        self.assertIsNone(self.worker.daemon.poll())

    def test_fixture_does_not_enable_attention_and_rejects_escape(self):
        f = self.fixture
        f.api("canary-claude", "channel.list")
        with self.assertRaises(CanaryFailure):
            f.api("production-agent", "channel.list")
        with self.assertRaises(CanaryFailure):
            f.api("canary-claude", "archive.execute")
        self.worker.stop_daemon()
        with closing(sqlite3.connect(self.worker.database.as_uri() + "?mode=ro", uri=True)) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM channel_attention_clients").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
