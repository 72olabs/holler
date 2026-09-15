from __future__ import annotations

import json
import io
import os
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
from worker import (
    actor_delivery_counts,
    actor_for_run,
    alias_collision_visible,
    CanaryFailure,
    PtyProcess,
    claude_fixture_ready,
    claude_cost,
    codex_reported_tokens,
    codex_config_with_trusted_fixture,
    codex_hook_trust_ready,
    delivery_event_attempts,
    delivery_was_acked,
    doctor_command,
    lifecycle_evidence_complete,
    make_failure_evidence,
    marker_instruction,
    minted_actors,
    parse_version,
    terminal_query_responses,
    Worker,
    safe_extract,
)


class WorkerTests(unittest.TestCase):
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
                'def run(worker):\n    return ["contributor-check"]\n',
                encoding="utf-8",
            )
            worker = Worker.__new__(Worker)
            worker.request = {
                "request_hash": "sha256:request",
                "source": {"commit": "abc"},
                "tier": "core",
                "scenarios": [
                    {"id": "C9", "name": "Contributor scenario", "checks": ["contributor-check"]}
                ],
                "budget": {
                    "claude_usd": 0.5,
                    "codex_reported_tokens": 250_000,
                    "model_turns": 8,
                    "wall_seconds": 1800,
                },
            }
            worker.results = []
            worker.ledger = SimpleNamespace(as_dict=lambda: {"model_turns": 0})
            worker.prepare = lambda: None
            with patch("worker.SCRIPT_DIR", root):
                evidence = worker.run()
            self.assertEqual(evidence["results"][0]["assertions"], [
                {"name": "contributor-check", "status": "PASS"}
            ])

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
