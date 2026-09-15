from __future__ import annotations

import json
import io
import os
import signal
import sys
from pathlib import Path
import tempfile
import tarfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

SCRIPT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPT_DIR))

from daytona_controller import make_runtime_bundle
from budget import BudgetLedger
from handler_contract import HandlerContractError, load_handler
from worker import (
    actor_delivery_counts,
    actor_for_run,
    alias_collision_visible,
    CanaryFailure,
    BudgetedInteractiveSession,
    HandlerContext,
    PtyProcess,
    claude_fixture_ready,
    claude_cost,
    codex_reported_tokens,
    codex_config_with_trusted_fixture,
    codex_hook_trust_ready,
    delivery_event_attempts,
    delivery_was_queued,
    delivery_was_acked,
    doctor_command,
    lifecycle_evidence_complete,
    make_failure_evidence,
    marker_instruction,
    sent_message_id,
    minted_actors,
    parse_version,
    run_with_timeout,
    terminal_query_responses,
    Worker,
    safe_extract,
)


class WorkerTests(unittest.TestCase):
    def test_graceful_exit_sweeps_hook_monitor_process_group(self) -> None:
        process = object.__new__(PtyProcess)
        observed: list[signal.Signals] = []
        process._signal = observed.append
        with patch("worker.time.sleep"):
            process._sweep_process_group()
        self.assertEqual(observed, [signal.SIGTERM, signal.SIGKILL])

    def test_wait_for_no_live_registration_accepts_ended_session(self) -> None:
        worker = SimpleNamespace(
            actor_directory=lambda: {
                "actors": [
                    {
                        "actor": "canary-claude",
                        "sessions": [
                            {
                                "run_id": "c2-claude",
                                "harness": "claude",
                                "state": "ended",
                            }
                        ],
                    }
                ]
            }
        )
        Worker.wait_for_no_live_registration(
            worker,
            "canary-claude",
            "c2-claude",
            harness="claude",
            timeout=0.01,
        )

    def test_wait_for_no_live_registration_rejects_wrong_actor(self) -> None:
        worker = SimpleNamespace(
            actor_directory=lambda: {
                "actors": [
                    {
                        "actor": "unexpected-actor",
                        "sessions": [
                            {
                                "run_id": "c2-claude",
                                "harness": "claude",
                                "state": "live",
                            }
                        ],
                    }
                ]
            }
        )
        with self.assertRaisesRegex(CanaryFailure, "unexpected actor"):
            Worker.wait_for_no_live_registration(
                worker,
                "canary-claude",
                "c2-claude",
                harness="claude",
                timeout=0.01,
            )

    def test_sent_message_id_correlates_body_free_durable_event(self) -> None:
        events = [
            {
                "kind": "message.sent",
                "message_id": "msg-1",
                "actor_id": "sender",
                "payload": {"from_run": "run-1", "recipients": ["recipient"]},
            },
            {
                "kind": "message.sent",
                "message_id": "msg-other",
                "actor_id": "sender",
                "payload": {"from_run": "other-run", "recipients": ["recipient"]},
            },
        ]
        self.assertEqual(
            sent_message_id(
                events,
                from_actor="sender",
                from_run="run-1",
                recipient_actor="recipient",
            ),
            "msg-1",
        )

    def test_sent_message_id_rejects_missing_or_duplicate_correlations(self) -> None:
        with self.assertRaisesRegex(CanaryFailure, "exactly one sent message"):
            sent_message_id(
                [], from_actor="sender", from_run="run-1", recipient_actor="recipient"
            )
        duplicate = {
            "kind": "message.sent",
            "message_id": "msg-1",
            "actor_id": "sender",
            "payload": {"from_run": "run-1", "recipients": ["recipient"]},
        }
        with self.assertRaisesRegex(CanaryFailure, "exactly one sent message"):
            sent_message_id(
                [duplicate, {**duplicate, "message_id": "msg-2"}],
                from_actor="sender",
                from_run="run-1",
                recipient_actor="recipient",
            )

    def test_delivery_was_queued_requires_same_message_and_actor(self) -> None:
        events = [
            {"kind": "delivery.queued", "message_id": "msg-1", "actor_id": "recipient"},
            {"kind": "delivery.queued", "message_id": "msg-2", "actor_id": "other"},
        ]
        self.assertTrue(delivery_was_queued(events, message_id="msg-1", actor="recipient"))
        self.assertFalse(delivery_was_queued(events, message_id="msg-1", actor="other"))

    def test_handler_context_has_no_raw_worker_reference(self) -> None:
        worker = SimpleNamespace(
            fixture=Path("/tmp/fixture"),
            request={
                "request_hash": "sha256:" + "a" * 64,
                "clients": {"claude": {}, "codex": {}},
            },
            run_claude=lambda *args: "",
            run_codex=lambda *args: "",
            wait_for_live_registration=lambda *args: None,
            handler_query=lambda *args, **kwargs: {},
            launcher=lambda *args: [],
            env={},
            ledger=BudgetLedger({
                "claude_usd": 1.0,
                "codex_reported_tokens": 100,
                "model_turns": 2,
                "wall_seconds": 60,
            }),
            active_check="initialization",
        )
        context = HandlerContext(worker)
        self.assertFalse(hasattr(context, "worker"))
        self.assertFalse(hasattr(context, "__dict__"))
        self.assertEqual(context.marker("C9_DONE"), "C9_DONE_AAAAAAAAAAAA")

    def test_interactive_turn_charges_only_after_expected_marker(self) -> None:
        limits = {
            "claude_usd": 1.0,
            "codex_reported_tokens": 100,
            "model_turns": 2,
            "wall_seconds": 60,
        }
        ledger = BudgetLedger(limits)
        session = BudgetedInteractiveSession(
            client="codex",
            actor="c9-codex",
            run_id="c9-run",
            extra_args=(),
            config={},
            launcher=lambda *args: [],
            wait_for_registration=lambda *args: None,
            fixture=Path("/tmp/fixture"),
            env={},
            ledger=ledger,
            marker_suffix="AAAAAAAAAAAA",
        )
        session.process = SimpleNamespace(submit=lambda *args, **kwargs: None)
        session.turn("encoded completion instruction", "C9_DONE_AAAAAAAAAAAA", timeout=1)
        self.assertEqual(ledger.model_turns, 1)

        failing = BudgetedInteractiveSession(
            client="codex",
            actor="c9-codex",
            run_id="c9-fail",
            extra_args=(),
            config={},
            launcher=lambda *args: [],
            wait_for_registration=lambda *args: None,
            fixture=Path("/tmp/fixture"),
            env={},
            ledger=ledger,
            marker_suffix="AAAAAAAAAAAA",
        )
        failing.process = SimpleNamespace(
            submit=lambda *args, **kwargs: (_ for _ in ()).throw(CanaryFailure("missing marker"))
        )
        with self.assertRaisesRegex(CanaryFailure, "missing marker"):
            failing.turn("encoded completion instruction", "C9_FAIL_AAAAAAAAAAAA", timeout=1)
        self.assertEqual(ledger.model_turns, 1)

    def test_safe_extract_accepts_internal_binary_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source" / "bundle"
            target = source / "node_modules" / "package" / "cli.js"
            target.parent.mkdir(parents=True)
            target.write_text("#!/usr/bin/env node\n", encoding="utf-8")
            binary = source / "node_modules" / ".bin" / "client"
            binary.parent.mkdir()
            binary.symlink_to("../package/cli.js")
            archive = root / "bundle.tar.gz"
            with tarfile.open(archive, "w:gz") as bundle:
                bundle.add(source, arcname="bundle")
            extracted = safe_extract(archive, root / "output")
            self.assertEqual((extracted / "node_modules" / ".bin" / "client").read_text(), "#!/usr/bin/env node\n")

    def test_safe_extract_accepts_internal_hardlink_and_rejects_escape(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archive = root / "bundle.tar.gz"
            payload = b"binary"
            with tarfile.open(archive, "w:gz") as bundle:
                target = tarfile.TarInfo("bundle/package/binary")
                target.size = len(payload)
                bundle.addfile(target, io.BytesIO(payload))
                link = tarfile.TarInfo("bundle/platform/binary")
                link.type = tarfile.LNKTYPE
                link.linkname = "bundle/package/binary"
                bundle.addfile(link)
            extracted = safe_extract(archive, root / "output")
            self.assertEqual((extracted / "platform" / "binary").read_bytes(), payload)

            unsafe = root / "unsafe.tar.gz"
            with tarfile.open(unsafe, "w:gz") as bundle:
                link = tarfile.TarInfo("bundle/binary")
                link.type = tarfile.LNKTYPE
                link.linkname = "../outside"
                bundle.addfile(link)
            with self.assertRaisesRegex(CanaryFailure, "unsafe archive hardlink"):
                safe_extract(unsafe, root / "unsafe-output")

    def test_c7_budget_cutoffs_and_teardown_are_zero_token(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            worker = Worker.__new__(Worker)
            worker.fixture = Path(directory)
            worker.env = os.environ.copy()
            worker.active_check = "initialization"
            self.assertEqual(
                worker.scenario_c7(),
                [
                    "turn-limit",
                    "claude-dollar-limit",
                    "codex-reported-token-limit",
                    "wall-clock-limit",
                    "clean-teardown",
                ],
            )

    def test_committed_custom_handler_runs_and_reports_declared_checks(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            handlers = root / "handlers"
            handlers.mkdir()
            (handlers / "C9.py").write_text(
                'def run(context):\n    return ["contributor-check"]\n',
                encoding="utf-8",
            )
            worker = Worker.__new__(Worker)
            worker.request = {
                "request_hash": "sha256:request",
                "source": {"commit": "abc"},
                "tier": "core",
                "scenarios": [
                    {
                        "id": "C9",
                        "name": "Contributor scenario",
                        "estimated_model_turns": 0,
                        "timeout_seconds": 180,
                        "checks": ["contributor-check"],
                    }
                ],
                "budget": {
                    "claude_usd": 0.5,
                    "codex_reported_tokens": 250_000,
                    "model_turns": 8,
                    "wall_seconds": 1800,
                },
            }
            worker.results = []
            worker.fixture = root
            worker.ledger = SimpleNamespace(model_turns=0, as_dict=lambda: {"model_turns": 0})
            worker.prepare = lambda: None
            with patch("worker.SCRIPT_DIR", root):
                evidence = worker.run()
            self.assertEqual(evidence["results"][0]["assertions"], [
                {"name": "contributor-check", "status": "PASS"}
            ])
            self.assertEqual(evidence["results"][0]["estimated_model_turns"], 0)
            self.assertEqual(evidence["results"][0]["observed_model_turns"], 0)

    def test_custom_handler_assertions_must_match_definition(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            handlers = root / "handlers"
            handlers.mkdir()
            (handlers / "C9.py").write_text(
                'def run(context):\n    return ["undeclared-check"]\n',
                encoding="utf-8",
            )
            worker = Worker.__new__(Worker)
            worker.request = {
                "request_hash": "sha256:request",
                "source": {"commit": "abc"},
                "tier": "core",
                "scenarios": [{
                    "id": "C9",
                    "name": "Contributor scenario",
                    "estimated_model_turns": 0,
                    "timeout_seconds": 180,
                    "checks": ["declared-check"],
                }],
                "budget": {
                    "claude_usd": 0.5,
                    "codex_reported_tokens": 250_000,
                    "model_turns": 8,
                    "wall_seconds": 1800,
                },
            }
            worker.results = []
            worker.fixture = root
            worker.ledger = SimpleNamespace(model_turns=0, as_dict=lambda: {"model_turns": 0})
            worker.prepare = lambda: None
            with patch("worker.SCRIPT_DIR", root), self.assertRaisesRegex(
                CanaryFailure,
                "assertions do not match",
            ):
                worker.run()

    def test_custom_handler_requires_callable_run(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "C9.py"
            path.write_text("VALUE = 1\n", encoding="utf-8")
            with self.assertRaisesRegex(HandlerContractError, "must define callable run"):
                load_handler(path)

    def test_custom_handler_import_failure_reports_only_error_type(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "C9.py"
            path.write_text('raise RuntimeError("private detail")\n', encoding="utf-8")
            with self.assertRaises(HandlerContractError) as raised:
                load_handler(path)
            self.assertIn("RuntimeError", str(raised.exception))
            self.assertNotIn("private detail", str(raised.exception))

    def test_custom_handler_static_tripwire_rejects_process_escape(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "C9.py"
            path.write_text("import subprocess\ndef run(context): return []\n", encoding="utf-8")
            with self.assertRaisesRegex(HandlerContractError, "static tripwire"):
                load_handler(path)

    def test_scenario_timeout_is_enforced(self) -> None:
        def expire() -> list[str]:
            try:
                signal.pause()
            except Exception:
                return []

        with self.assertRaisesRegex(CanaryFailure, "C9 exceeded its 1-second timeout"):
            run_with_timeout(expire, 1, "C9")

    def test_usage_parsers(self) -> None:
        events = "\n".join(
            [
                json.dumps({"type": "turn.completed", "usage": {"input_tokens": 12, "output_tokens": 3}}),
                json.dumps({"type": "turn.completed", "usage": {"total_tokens": 25}}),
            ]
        )
        self.assertEqual(codex_reported_tokens(events), 25)
        self.assertEqual(claude_cost(json.dumps({"total_cost_usd": 0.0123})), 0.0123)

    def test_version_parser(self) -> None:
        self.assertEqual(parse_version("codex-cli 0.151.0\n"), "0.151.0")
        self.assertEqual(parse_version("2.1.259 (Claude Code)\n"), "2.1.259")

    def test_marker_instruction_never_contains_expected_literal(self) -> None:
        marker = "C2_CLAUDE_ARMED"
        instruction = marker_instruction(marker)
        self.assertNotIn(marker, instruction)
        self.assertIn("'C2', 'CLAUDE', 'ARMED'", instruction)

    def test_pty_submit_requires_post_submission_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            process = PtyProcess(
                ["/bin/sh", "-c", "IFS= read -r value; printf 'REAL_RESULT_MARKER\\n'"],
                cwd=Path(directory),
                env={},
            )
            try:
                process.submit("input without the expected value", marker="REAL_RESULT_MARKER", timeout=2)
            finally:
                process.close()

    def test_pty_ready_waits_for_real_input_footer(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            process = PtyProcess(
                ["/bin/sh", "-c", "printf 'loading'; sleep 0.2; printf '? for shortcuts'; sleep 2"],
                cwd=Path(directory),
                env={},
            )
            try:
                process.wait_until_ready("? for shortcuts", 2)
                self.assertIn(b"? for shortcuts", process.buffer)
            finally:
                process.close()

    def test_pty_ready_accepts_stable_screen_reader_prompt_suffix(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            process = PtyProcess(
                ["/bin/sh", "-c", "printf 'Claude ready\\r\\n$'; sleep 2"],
                cwd=Path(directory),
                env={},
            )
            try:
                process.wait_until_ready("$", 2, suffix=True)
            finally:
                process.close()

    def test_pty_child_exit_is_reported_as_canary_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            process = PtyProcess(
                ["/bin/sh", "-c", "exit 0"],
                cwd=Path(directory),
                env={},
            )
            try:
                with self.assertRaisesRegex(CanaryFailure, "exited before"):
                    process.wait_until_ready("never rendered", 2)
            finally:
                process.close()

    def test_terminal_query_responses_cover_codex_startup_probes(self) -> None:
        queries = b"\x1b[6n\x1b]10;?\x1b\\\x1b]11;?\x1b\\\x1b[?u\x1b[c"
        replies = terminal_query_responses(queries)
        self.assertIn(b"\x1b[1;1R", replies)
        self.assertIn(b"\x1b]10;rgb:ffff/ffff/ffff\x1b\\", replies)
        self.assertIn(b"\x1b]11;rgb:0000/0000/0000\x1b\\", replies)
        self.assertIn(b"\x1b[?0u", replies)
        self.assertIn(b"\x1b[?1;2c", replies)

    def test_pty_submit_rejects_marker_in_prompt(self) -> None:
        process = PtyProcess.__new__(PtyProcess)
        with self.assertRaisesRegex(CanaryFailure, "contains its expected output marker"):
            process.submit("echo BAD_MARKER", marker="BAD_MARKER", timeout=0)

    def test_pty_submit_sends_enter_as_a_separate_terminal_event(self) -> None:
        process = PtyProcess.__new__(PtyProcess)
        sent = []
        process.checkpoint = lambda: 0
        process.send = sent.append
        process.wait_for = lambda marker, timeout, after=0: None
        process.submit("do the work", marker="DONE_MARKER", timeout=1)
        self.assertEqual(sent, ["do the work", "\r"])

    def test_lifecycle_evidence_requires_correlated_registration_and_hydration(self) -> None:
        events = [
            {
                "kind": "session.registered",
                "actor_id": "canary-claude",
                "payload": {"run_id": "run-1", "harness": "claude"},
            },
            {
                "kind": "startup.hydrated",
                "actor_id": "canary-claude",
                "payload": {"run_id": "run-1", "harness": "claude"},
            },
        ]
        self.assertTrue(
            lifecycle_evidence_complete(events, actor="canary-claude", run_id="run-1")
        )
        self.assertFalse(
            lifecycle_evidence_complete(events[:1], actor="canary-claude", run_id="run-1")
        )
        self.assertFalse(
            lifecycle_evidence_complete(events, actor="canary-claude", run_id="another-run")
        )

    def test_lifecycle_evidence_rejects_invalid_shape(self) -> None:
        with self.assertRaisesRegex(CanaryFailure, "not a list"):
            lifecycle_evidence_complete({}, actor="canary-claude", run_id="run-1")

    def test_c4_metadata_helpers_require_same_message_and_actor(self) -> None:
        message_id = "msg-1"
        directory = {
            "actors": [
                {"actor": "canary-claude", "unclaimed_messages": 0, "active_claims": 1},
                {"actor": "another", "unclaimed_messages": 2, "active_claims": 0},
            ]
        }
        self.assertEqual(actor_delivery_counts(directory, actor="canary-claude"), (0, 1))
        self.assertEqual(actor_delivery_counts(directory, actor="missing"), (0, 0))

        events = [
            {
                "kind": "delivery.claimed",
                "message_id": message_id,
                "actor_id": "canary-claude",
                "payload": {"attempt": 1},
            },
            {
                "kind": "delivery.claimed",
                "message_id": message_id,
                "actor_id": "another-actor",
                "payload": {"attempt": 99},
            },
            {
                "kind": "delivery.claimed",
                "message_id": message_id,
                "actor_id": "canary-claude",
                "payload": {"attempt": 2},
            },
            {
                "kind": "delivery.acked",
                "message_id": message_id,
                "actor_id": "canary-claude",
                "payload": {},
            },
        ]
        self.assertEqual(
            delivery_event_attempts(events, message_id=message_id, actor="canary-claude"),
            [1, 2],
        )
        self.assertTrue(delivery_was_acked(events, message_id=message_id, actor="canary-claude"))

    def test_c4_metadata_helpers_reject_invalid_directory(self) -> None:
        with self.assertRaisesRegex(CanaryFailure, "invalid shape"):
            actor_delivery_counts({}, actor="canary-claude")
        with self.assertRaisesRegex(CanaryFailure, "invalid delivery counts"):
            actor_delivery_counts(
                {"actors": [{"actor": "canary-claude", "unclaimed_messages": "one"}]},
                actor="canary-claude",
            )

    def test_c5_directory_and_condition_helpers(self) -> None:
        directory = {
            "actors": [
                {
                    "actor": "c5-claude-a1b2c3",
                    "sessions": [
                        {"run_id": "c5-a", "harness": "claude", "state": "live"},
                        {"run_id": "old", "harness": "claude", "state": "ended"},
                    ],
                }
            ]
        }
        self.assertEqual(
            actor_for_run(directory, run_id="c5-a", harness="claude"),
            "c5-claude-a1b2c3",
        )
        self.assertIsNone(actor_for_run(directory, run_id="old", harness="claude"))
        self.assertEqual(
            actor_for_run(directory, run_id="old", harness="claude", live_only=False),
            "c5-claude-a1b2c3",
        )
        conditions = [
            {
                "kind": "alias_collision",
                "subject": "c5-claude",
                "state": "active_visible",
            }
        ]
        self.assertTrue(alias_collision_visible(conditions, alias="c5-claude"))
        self.assertFalse(alias_collision_visible(conditions, alias="another-alias"))

    def test_c5_directory_helper_rejects_run_under_multiple_actors(self) -> None:
        session = {"run_id": "c5-a", "harness": "claude", "state": "live"}
        directory = {
            "actors": [
                {"actor": "actor-a", "sessions": [session]},
                {"actor": "actor-b", "sessions": [session]},
            ]
        }
        with self.assertRaisesRegex(CanaryFailure, "multiple actors"):
            actor_for_run(directory, run_id="c5-a", harness="claude")

    def test_c5_minted_actor_helper_uses_only_durable_mint_events(self) -> None:
        events = [
            {"kind": "actor.minted", "actor_id": "actor-a"},
            {"kind": "delivery.claimed", "actor_id": "actor-a"},
            {"kind": "actor.minted", "actor_id": "actor-b"},
        ]
        self.assertEqual(minted_actors(events), ["actor-a", "actor-b"])

    def test_claude_fixture_requires_onboarding_version_and_project_trust(self) -> None:
        fixture = Path("/home/daytona/.holler-canary-workspace")
        config = {
            "theme": "dark",
            "hasCompletedOnboarding": True,
            "lastOnboardingVersion": "2.1.259",
            "projects": {str(fixture): {"hasTrustDialogAccepted": True}},
        }
        self.assertTrue(claude_fixture_ready(config, fixture=fixture, version="2.1.259"))
        config["projects"][str(fixture)]["hasTrustDialogAccepted"] = False
        self.assertFalse(claude_fixture_ready(config, fixture=fixture, version="2.1.259"))

    def test_codex_fixture_trust_merge_preserves_generated_policy(self) -> None:
        fixture = Path("/home/daytona/.holler-canary-workspace")
        original = '[profiles.holler]\nsandbox_mode = "read-only"\n'
        updated = codex_config_with_trusted_fixture(original, fixture)
        self.assertIn(original, updated)
        self.assertIn('[projects."/home/daytona/.holler-canary-workspace"]', updated)
        self.assertIn('trust_level = "trusted"', updated)
        self.assertEqual(codex_config_with_trusted_fixture(updated, fixture), updated)

    def test_codex_hook_trust_requires_both_packaged_hooks(self) -> None:
        config = {
            "hooks": {
                "state": {
                    "holler@holler:hooks/hooks.json:session_start:0:0": {
                        "trusted_hash": "sha256:" + "a" * 64,
                    },
                    "holler@holler:hooks/hooks.json:session_end:0:0": {
                        "trusted_hash": "sha256:" + "b" * 64,
                    },
                }
            }
        }
        self.assertTrue(codex_hook_trust_ready(config))
        del config["hooks"]["state"]["holler@holler:hooks/hooks.json:session_end:0:0"]
        self.assertFalse(codex_hook_trust_ready(config))

    def test_runtime_bundle_contains_worker_without_tests(self) -> None:
        import tarfile

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.tar.gz"
            make_runtime_bundle(path)
            with tarfile.open(path, "r:gz") as bundle:
                names = set(bundle.getnames())
        self.assertIn("holler-canary/worker.py", names)
        self.assertIn("holler-canary/handler_contract.py", names)
        self.assertIn("holler-canary/scenarios/C0.json", names)
        self.assertFalse(any("tests" in name for name in names))

    def test_codex_doctor_uses_generated_least_privilege_policy(self) -> None:
        common = {
            "actor": "canary",
            "attention": "native-queue",
            "project": Path("/tmp/fixture"),
            "socket": Path("/tmp/holler.sock"),
            "env": {"CODEX_HOME": "/auth/codex"},
        }
        codex = doctor_command(Path("/bin/holler"), harness="codex", **common)
        claude = doctor_command(Path("/bin/holler"), harness="claude", **common)
        self.assertIn("/auth/codex/holler.config.toml", codex)
        self.assertNotIn("--policy", claude)

    def test_failure_evidence_does_not_include_exception_message(self) -> None:
        evidence = make_failure_evidence(
            {
                "request_hash": "sha256:request",
                "source": {"commit": "abc"},
                "tier": "core",
                "budget": {"model_turns": 8},
            },
            results=[],
            usage={"model_turns": 1},
            scenario="C1",
            check="c1-claim",
            error=RuntimeError("sensitive prompt or peer message"),
        )
        self.assertEqual(
            evidence["failure"],
            {"scenario": "C1", "check": "c1-claim", "type": "RuntimeError"},
        )
        self.assertNotIn("sensitive", json.dumps(evidence))
        self.assertFalse(evidence["message_bodies_included"])


if __name__ == "__main__":
    unittest.main()
