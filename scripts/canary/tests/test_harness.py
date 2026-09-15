from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest


SCRIPT_DIR = Path(__file__).resolve().parents[1]
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from harness import (  # noqa: E402
    checkpoint_directory,
    command_for_checkpoint,
    forwarded_arguments,
    reusable_artifact,
    runtime_python,
)


class HarnessTests(unittest.TestCase):
    def test_checkpoint_directory_is_commit_and_tier_scoped(self) -> None:
        root = checkpoint_directory(Path("/tmp/repo"), "1234567890abcdef", "core")
        self.assertEqual(
            root,
            Path("/tmp/repo").resolve() / ".runs/canary/checkpoints/1234567890ab-core",
        )

    def test_checkpoint_command_requires_exact_hash_for_second_phase(self) -> None:
        command = command_for_checkpoint(tier="core", approval="sha256:abc")
        self.assertEqual(
            command,
            "python3 scripts/canary/harness.py checkpoint --tier core --execute --approve sha256:abc",
        )

    def test_runtime_path_stays_under_gitignored_runs(self) -> None:
        path = runtime_python(Path("/tmp/repo"))
        self.assertIn("/.runs/canary/venv/", str(path))

    def test_nondefault_options_are_forwarded_to_approval_command(self) -> None:
        class Args:
            repo = Path("/tmp/custom")
            ref = "candidate"
            runner_name = "runner-02"
            snapshot = None
            upgrade_from = Path("/tmp/v0.7.1.tar.gz")
            client_bundle = None
            output_dir = None
            claude_version = None
            codex_version = None
            claude_model = None
            codex_model = None
            allow_model_override = False

        command = command_for_checkpoint(
            tier="release",
            approval="sha256:abc",
            forwarded=forwarded_arguments(Args()),
        )
        self.assertIn("--repo /tmp/custom", command)
        self.assertIn("--ref candidate", command)
        self.assertIn("--runner-name runner-02", command)
        self.assertIn("--upgrade-from /tmp/v0.7.1.tar.gz", command)
        self.assertTrue(command.endswith("--approve sha256:abc"))

    def test_reusable_artifact_rejects_non_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            artifact = root / "holler.tar.gz"
            artifact.write_bytes(b"artifact")
            request = root / "request.json"
            request.write_text(json.dumps({"not": "a request"}), encoding="utf-8")
            self.assertFalse(
                reusable_artifact(
                    paths={"artifact": artifact, "request": request},
                    expected_commit="abc",
                    tier="core",
                )
            )


if __name__ == "__main__":
    unittest.main()
