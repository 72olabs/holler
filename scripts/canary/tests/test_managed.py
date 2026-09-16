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
from handler_contract import HandlerContractError, load_handler, validate_write_contract
from managed import exchange, receive_exact
from worker import (BudgetedInteractiveSession, CanaryFailure, PtyProcess, Worker,
                    approved_fixture_policy, assert_fixture_policy_baseline, fixture_write_policy,
                    codex_tool_counts, make_failure_evidence, terminal_wait_diagnostic,
                    terminal_marker_seen, marker_instruction)
from clients import client_policy
from types import SimpleNamespace
from budget import BudgetExceeded, BudgetLedger


class FrameTests(unittest.TestCase):
    def test_terminal_diagnostic_exports_only_fixed_signals_not_transcript(self):
        output = b"SECRET Please run /login API Error: Do you want to proceed? DONE_\x1b[0mSUFFIX\n$"
        diag = terminal_wait_diagnostic(output, "DONE_SUFFIX", client_running=True)
        self.assertFalse(diag["raw_marker_seen"])
        self.assertTrue(diag["normalized_marker_seen"])
        self.assertTrue(diag["screen_reader_prompt_at_end"])
        self.assertEqual(diag["signals"], ["api-error", "auth-error", "permission-prompt"])
        error = CanaryFailure("SECRET", code="terminal-marker-timeout")
        error.terminal_diagnostic = diag
        evidence = make_failure_evidence(
            {"request_hash": "hash", "source": {}, "tier": "core", "budget": {}},
            results=[], usage={}, scenario="C11", check="c11-claude-arm", error=error)
        self.assertEqual(evidence["terminal_diagnostic"], diag)
        self.assertNotIn("SECRET", json.dumps(evidence))
        self.assertNotIn("Please run", json.dumps(evidence))

    def test_terminal_wait_distinguishes_exit_from_timeout(self):
        process = object.__new__(PtyProcess)
        process.buffer = bytearray(b"SECRET no expected marker")
        process._read_available = lambda *args: None
        for exited in (True, False):
            with self.subTest(exited=exited):
                process.process = SimpleNamespace(poll=lambda: 1 if exited else None)
                with self.assertRaises(CanaryFailure) as caught:
                    process.wait_for("DONE_SUFFIX", 0.001)
                self.assertEqual(caught.exception.code, "terminal-client-exited" if exited else "terminal-marker-timeout")
                self.assertEqual(caught.exception.terminal_diagnostic["client_running"], not exited)
                self.assertFalse(caught.exception.terminal_diagnostic["normalized_marker_seen"])
                self.assertGreaterEqual(caught.exception.terminal_diagnostic["elapsed_seconds"], 0)
        process.buffer = bytearray(b"DONE_SUFFIX")
        process.wait_for("DONE_SUFFIX", 0.001)

    def test_terminal_marker_handles_styling_wrapping_and_excludes_prompt_echo(self):
        marker = "C11_CLAUDE_ARMED_123456ABCDEF"
        wrapped = b"C11_CLAUDE_\x1b[0mAR\r\nMED_123456ABCDEF"
        self.assertNotIn(marker.encode(), wrapped)
        self.assertTrue(terminal_marker_seen(wrapped, marker))
        self.assertFalse(terminal_marker_seen(marker_instruction(marker).encode(), marker))
        self.assertFalse(terminal_marker_seen(b"C11_CLAUDE_ARMED_DIFFERENT", marker))
        process = object.__new__(PtyProcess)
        process.buffer = bytearray(marker.encode() + wrapped)
        process.wait_for(marker, 0.001, after=len(marker))
        with self.assertRaisesRegex(CanaryFailure, "contains its expected"):
            process.submit(wrapped.decode(), marker=marker, timeout=1)

    def interactive_session(self, client, events, registration=None):
        ledger = BudgetLedger({"claude_usd": 1, "codex_reported_tokens": 100,
                               "model_turns": 4, "wall_seconds": 60})
        def register(*args):
            events.append(("registration", ledger.model_turns))
            if registration:
                registration()
        return BudgetedInteractiveSession(
            client=client, actor="canary-" + client, run_id="run", extra_args=(),
            config=client_policy()[client], launcher=lambda *args: [],
            wait_for_registration=register, fixture=Path("/tmp"), env={}, ledger=ledger,
            marker_suffix="SUFFIX", phase=lambda name: events.append(("phase", name)))

    def fake_process(self, events):
        return SimpleNamespace(
            wait_until_ready=lambda *args, **kwargs: events.append(("ready",)),
            wait_until_quiet=lambda *args, **kwargs: None,
            normalized_output=lambda: "", submit=lambda *args, **kwargs: events.append(("submit",)),
            close=lambda: events.append(("close",)), graceful_claude_exit=lambda: None)

    def test_interactive_client_registration_order_and_first_turn_only(self):
        for client in ("claude", "codex"):
            with self.subTest(client=client):
                events = []
                session = self.interactive_session(client, events)
                with patch("worker.PtyProcess", return_value=self.fake_process(events)):
                    with session:
                        self.assertEqual(session.registered, client == "claude")
                        session.turn("instruction", "ARMED_SUFFIX")
                        session.turn("instruction", "SECOND_SUFFIX")
                ordered = [e for e in events if e[0] in {"registration", "submit"}]
                self.assertEqual(ordered, [("registration", 0), ("submit",), ("submit",)] if client == "claude"
                                 else [("submit",), ("registration", 1), ("submit",)])
                self.assertEqual(session.ledger.model_turns, 2)

    def test_registration_timeout_keeps_completed_turn_charge_and_failure_phase(self):
        events = []
        def fail():
            raise CanaryFailure("private detail", code="registration-timeout")
        session = self.interactive_session("codex", events, fail)
        with patch("worker.PtyProcess", return_value=self.fake_process(events)):
            with self.assertRaises(CanaryFailure) as caught:
                with session:
                    session.turn("instruction", "ARMED_SUFFIX")
        self.assertEqual(caught.exception.code, "registration-timeout")
        self.assertEqual(session.ledger.model_turns, 1)
        self.assertEqual([e for e in events if e[0] == "phase"][-1], ("phase", "registration"))
        self.assertIsNone(session.process)

    def test_failed_first_submit_does_not_wait_for_registration(self):
        events = []
        session = self.interactive_session("codex", events)
        process = self.fake_process(events)
        def fail(*args, **kwargs):
            raise CanaryFailure("missing marker")
        process.submit = fail
        with patch("worker.PtyProcess", return_value=process):
            with self.assertRaises(CanaryFailure):
                with session:
                    session.turn("instruction", "ARMED_SUFFIX")
        self.assertFalse(any(e[0] == "registration" for e in events))
        self.assertEqual(session.ledger.model_turns, 0)

    def test_wake_records_sent_id_before_marker_failure(self):
        events = []
        session = self.interactive_session("codex", events)
        session.record_wake = lambda result: events.append(("sent", result["message"]["message_id"]))
        def fail(*args, **kwargs):
            raise CanaryFailure("private marker error")
        session.process = SimpleNamespace(checkpoint=lambda: 0, wait_for=fail)
        with self.assertRaises(CanaryFailure):
            session.wake(lambda: {"message": {"message_id": "msg_test"}}, "WOKE_SUFFIX")
        self.assertEqual(events, [("phase", "wake-trigger"), ("sent", "msg_test"), ("phase", "wake")])
        self.assertEqual(session.ledger.model_turns, 0)

    def test_baseline_rejects_stale_approval_and_symlinks_without_writing(self):
        original = (SCRIPT_DIR.parents[1] / "connectors/policies/codex-live-review.toml").read_bytes()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "holler.config.toml"
            assert_fixture_policy_baseline(path, missing_ok=True)
            with self.assertRaises(CanaryFailure):
                assert_fixture_policy_baseline(path)
            path.write_bytes(original)
            assert_fixture_policy_baseline(path)
            stale = approved_fixture_policy(original)
            path.write_bytes(stale)
            with self.assertRaises(CanaryFailure):
                assert_fixture_policy_baseline(path, missing_ok=True)
            self.assertEqual(path.read_bytes(), stale)
            link = Path(directory) / "link"
            link.symlink_to(path)
            with self.assertRaises(CanaryFailure):
                assert_fixture_policy_baseline(link, missing_ok=True)

    def test_write_contract_requires_declaration_and_explicit_method(self):
        catalog = load_catalog()
        for sid in ("C9", "C10", "C11"):
            validate_write_contract(SCRIPT_DIR / "handlers" / (sid + ".py"), catalog[sid])
        bad = copy.deepcopy(catalog["C10"])
        bad["test_environment"]["write_policy"] = "generated-default"
        with self.assertRaises(HandlerContractError):
            validate_write_contract(SCRIPT_DIR / "handlers" / "C10.py", bad)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "C10.py"
            original = (SCRIPT_DIR / "handlers" / "C10.py").read_text()
            path.write_text(original.replace("run_codex_write", "run_codex"))
            with self.assertRaises(HandlerContractError):
                validate_write_contract(path, bad)

    def test_codex_write_approval_is_temporary_and_fail_closed(self):
        w = object.__new__(Worker)
        w.request = {"clients": client_policy(), "scenarios": [load_catalog()["C10"]]}
        w.active_scenario = "C10"
        w.managed_config = load_catalog()["C10"]["daemon"]
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        policy_path = Path(temporary.name) / "holler.config.toml"
        original = (SCRIPT_DIR.parents[1] / "connectors/policies/codex-live-review.toml").read_bytes()
        policy_path.write_bytes(original)
        w.fixture, w.env, w.tool_counts = Path("/tmp"), {"CODEX_HOME": temporary.name}, []
        w.policy_audits = []
        w.ledger = BudgetLedger({"claude_usd": 1, "codex_reported_tokens": 100,
                                "model_turns": 8, "wall_seconds": 60})
        w.launcher = lambda harness, actor, run_id, args: args
        def process(*args, **kwargs):
            self.assertEqual(policy_path.read_bytes(), approved_fixture_policy(original))
            return SimpleNamespace(stdout="")
        with patch("worker.run_command", side_effect=process) as run:
            w.run_codex("canary-codex", "c10-create", "prompt", fixture_write=True)
            self.assertEqual(policy_path.read_bytes(), original)
            self.assertTrue(w.policy_audits[0]["restored"])
            run.side_effect = lambda *args, **kwargs: SimpleNamespace(stdout="")
            w.run_codex("canary-codex", "plain", "prompt")
            self.assertEqual(len(w.policy_audits), 1)
            for actor, scenario in (("canary-claude", "C10"), ("canary-codex", "C11")):
                w.active_scenario = scenario
                before = w.ledger.model_turns
                with self.assertRaises(CanaryFailure) as error:
                    w.run_codex(actor, "bad", "prompt", fixture_write=True)
                self.assertEqual(error.exception.code, "write-policy-mismatch")
                self.assertEqual(w.ledger.model_turns, before)
            self.assertEqual(run.call_count, 2)
        self.assertEqual(w.env, {"CODEX_HOME": temporary.name})

    def test_policy_restore_on_exception_and_reject_unexpected_changes(self):
        original = (SCRIPT_DIR.parents[1] / "connectors/policies/codex-live-review.toml").read_bytes()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "holler.config.toml"
            path.write_bytes(original)
            path.chmod(0o640)
            audits = []
            with self.assertRaisesRegex(RuntimeError, "synthetic failure"):
                with fixture_write_policy(path, audits):
                    self.assertEqual(path.read_bytes(), approved_fixture_policy(original))
                    raise RuntimeError("synthetic failure")
            self.assertEqual(path.read_bytes(), original)
            self.assertEqual(path.stat().st_mode & 0o777, 0o640)
            self.assertEqual(audits[0]["original_sha256"], audits[0]["restored_sha256"])
            self.assertTrue(audits[0]["restored"])
            with self.assertRaises(CanaryFailure) as error:
                with fixture_write_policy(path, audits):
                    path.write_bytes(b"unexpected concurrent edit")
            self.assertEqual(error.exception.code, "policy-restore-failed")
            self.assertEqual(path.read_bytes(), b"unexpected concurrent edit")
            self.assertFalse(audits[-1]["restored"])
            with self.assertRaises(CanaryFailure) as error:
                with fixture_write_policy(path, audits):
                    self.fail("invalid baseline launched a client")
            self.assertEqual(error.exception.code, "policy-invalid")
            with self.assertRaises(CanaryFailure):
                approved_fixture_policy(approved_fixture_policy(original))

    def test_tool_telemetry_and_failure_export_no_bodies(self):
        events = [
            {"type": "item.completed", "item": {"type": "mcp_tool_call", "tool": "holler_write",
             "status": "failed", "arguments": "SECRET", "error": "SECRET", "result": "SECRET"}},
            {"type": "item.completed", "item": {"type": "mcp_tool_call", "tool": "SECRET",
             "status": "completed"}},
            {"type": "item.completed", "item": {"type": "agent_message", "text": "SECRET"}},
            [], {"type": "item.completed", "item": None},
        ]
        counts = codex_tool_counts("\n".join(json.dumps(e) for e in events))
        self.assertEqual(counts, [{"tool": "holler_write", "status": "failed", "count": 1}])
        evidence = make_failure_evidence(
            {"request_hash": "hash", "source": {}, "tier": "core", "budget": {},
             "scenarios": [load_catalog()["C10"]]}, results=[], usage={}, scenario="C10",
            check="c10-codex-turn-marker", error=CanaryFailure("SECRET", code="marker-missing"),
            tool_counts=counts)
        self.assertEqual(evidence["failure"]["code"], "marker-missing")
        self.assertEqual(evidence["failure"]["test_environment"]["write_policy"], "fixture-approved-codex-write")
        self.assertNotIn("SECRET", json.dumps(evidence))
        with self.assertRaises(ValueError):
            CanaryFailure("ignored", code="SECRET")

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
        session.phase = lambda name: None
        session.record_wake = lambda result: None
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

    def test_c10_post_turn_oracles_with_protocol_stand_ins_not_models(self):
        """Zero-model regression of C10 oracles; never real-client evidence."""
        f = self.fixture
        controller, a, b = "canary-controller", "canary-claude", "canary-codex"
        def channel():
            return next(c for c in f.api(controller, "channel.list") if c["title"] == "c10-real")
        def consume(actor, run_id, prompt, marker):
            target = next(d for d in f.inbox(actor) if d["message"].get("response_request_id"))
            mid = target["message"]["message_id"]
            claim = f.api(actor, "channel.claim", {"message_id": mid})
            if actor == b:
                c = channel()
                response = f.api(b, "channel.responses", {"channel_id": c["channel_id"]})[0]
                f.api(b, "channel.post", {"channel_id": c["channel_id"], "expected_policy_revision": c["policy_revision"],
                    "idempotency_key": "local-answer", "body": {"text": "synthetic"},
                    "response_to": response["request_id"], "expected_response_revision": response["revision"]}, run_id=run_id)
            f.api(actor, "channel.delivery", {"message_id": mid, "lease_token": claim["lease_token"], "action": "ack"})
        def codex(actor, run_id, prompt, marker):
            if run_id != "c10-create":
                return consume(actor, run_id, prompt, marker)
            c = f.api(b, "channel.create", {"project_id": "canary", "kind": "named", "title": "c10-real",
                "participants": [controller, a, b], "idempotency_key": "local-create"}, run_id=run_id)
            f.api(b, "channel.post", {"channel_id": c["channel_id"], "expected_policy_revision": c["policy_revision"],
                "idempotency_key": "local-opening", "body": {"text": "synthetic"}}, run_id=run_id)
        checks = []
        context = SimpleNamespace(managed=lambda: f, check=checks.append, marker=lambda name: name,
                                  marker_instruction=lambda name: name, run_codex_write=codex,
                                  run_claude=consume, wait_for_session_end=lambda *args: None)
        result = load_handler(SCRIPT_DIR / "handlers" / "C10.py")(context)
        self.assertEqual(result, load_catalog()["C10"]["checks"])
        self.assertIn("c10-opening-correlation", checks)
        with self.assertRaises(CanaryFailure):
            f.api(a, "channel.list", run_id="c10-create")
        with self.assertRaises(CanaryFailure):
            f.api(b, "channel.list", run_id="arbitrary")

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

    def test_failure_diagnostic_preserves_projection_guards_and_primary_check(self):
        w = self.worker
        w.active_scenario, w.active_check, w.managed_wake_ids = "C11", "c11-codex-wake", ["msg_test"]
        with patch.object(w, "actor_directory", return_value={"actors": [{"sessions": [{"state": "live"}]}]}):
            self.assertEqual(w.managed_wake_failure_diagnostic(),
                             {"status": "omitted", "reason": "projection-clients-live"})
        self.assertIsNone(w.daemon.poll())
        with patch.object(w, "stop_daemon", return_value=None):
            self.assertEqual(w.managed_wake_failure_diagnostic(),
                             {"status": "omitted", "reason": "projection-daemon-live"})
        self.assertEqual(w.active_check, "c11-codex-wake")
        self.assertEqual(w.managed_wake_failure_diagnostic()["status"], "captured")
        w.active_check = "c11-codex-registration"
        self.assertIsNone(w.managed_wake_failure_diagnostic())

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
