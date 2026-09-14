from __future__ import annotations

import json
import sys
from pathlib import Path
import tempfile
import unittest

SCRIPT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPT_DIR))

from daytona_controller import make_runtime_bundle
from worker import claude_cost, codex_reported_tokens, parse_version


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


if __name__ == "__main__":
    unittest.main()
