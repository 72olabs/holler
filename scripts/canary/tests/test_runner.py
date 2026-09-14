from __future__ import annotations

import sys
from pathlib import Path
import unittest

SCRIPT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPT_DIR))

from clients import client_policy
from daytona import execution_plan
from manifest import create_request
from run import run_fake


REPO = SCRIPT_DIR.parent.parent


class RunnerTests(unittest.TestCase):
    def test_fake_release_run_is_body_free_and_within_budget(self) -> None:
        request = create_request(REPO, ref="HEAD", tier="release", clients=client_policy())
        evidence = run_fake(request)
        self.assertEqual(evidence["status"], "PASS")
        self.assertFalse(evidence["message_bodies_included"])
        self.assertLessEqual(evidence["usage"]["model_turns"], evidence["limits"]["model_turns"])
        self.assertNotIn("prompt", str(evidence).lower())

    def test_daytona_plan_separates_source_and_credentials(self) -> None:
        request = create_request(REPO, ref="HEAD", tier="core", clients=client_policy())
        plan = execution_plan(request)
        self.assertEqual(plan["resource_policy"]["builder"]["credentials"], [])
        self.assertFalse(plan["resource_policy"]["canary"]["source_checkout"])
        self.assertEqual(plan["models"]["claude"], "haiku")
        self.assertEqual(plan["models"]["codex"], "gpt-5.6-luna")


if __name__ == "__main__":
    unittest.main()
