from __future__ import annotations

import json
import sys
from pathlib import Path
import tempfile
import unittest

SCRIPT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPT_DIR))

from daytona_controller import make_runtime_bundle
from worker import (
    CanaryFailure,
    PtyProcess,
    claude_fixture_ready,
    claude_cost,
    codex_reported_tokens,
    doctor_command,
    lifecycle_evidence_complete,
    make_failure_evidence,
    marker_instruction,
    parse_version,
)


class WorkerTests(unittest.TestCase):
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

    def test_pty_submit_rejects_marker_in_prompt(self) -> None:
        process = PtyProcess.__new__(PtyProcess)
        with self.assertRaisesRegex(CanaryFailure, "contains its expected output marker"):
            process.submit("echo BAD_MARKER", marker="BAD_MARKER", timeout=0)

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
            error=RuntimeError("sensitive prompt or peer message"),
        )
        self.assertEqual(evidence["failure"], {"scenario": "C1", "type": "RuntimeError"})
        self.assertNotIn("sensitive", json.dumps(evidence))
        self.assertFalse(evidence["message_bodies_included"])


if __name__ == "__main__":
    unittest.main()
