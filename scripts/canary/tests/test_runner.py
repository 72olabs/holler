from __future__ import annotations

import json
import sys
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

SCRIPT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPT_DIR))

from clients import client_policy
from daytona_controller import (
    BUILDER_DOMAINS,
    CREDENTIAL_DOMAINS,
    claude_fixture_state_source,
    execution_plan,
    validate_runner,
)
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
        self.assertTrue(plan["resource_policy"]["canary"]["persistent"])
        self.assertEqual(plan["resource_policy"]["canary"]["runner"], "holler-canary-runner")
        self.assertEqual(
            plan["resource_policy"]["canary"]["fixture"],
            "/home/daytona/.holler-canary-workspace",
        )
        self.assertEqual(plan["models"]["claude"], "haiku")
        self.assertEqual(plan["models"]["codex"], "gpt-5.6-luna")
        self.assertIn("go-1-26-0", plan["resource_policy"]["canary"]["snapshot"])
        self.assertIn("*.claude.com", plan["network_policy"]["canary"])
        self.assertEqual(plan["network_policy"]["builder"], BUILDER_DOMAINS)
        self.assertIn("proxy.golang.org", plan["network_policy"]["builder"])

    def test_persistent_runner_policy_is_verified(self) -> None:
        request = create_request(REPO, ref="HEAD", tier="core", clients=client_policy())
        execution = request["execution"]
        runner = SimpleNamespace(
            name="holler-canary-runner",
            labels={"purpose": "holler-canary-persistent-runner"},
            snapshot=execution["snapshot"],
            auto_delete_interval=-1,
            auto_stop_interval=15,
            domain_allow_list=",".join(CREDENTIAL_DOMAINS),
            env={
                "CLAUDE_CONFIG_DIR": "/home/daytona/.holler-canary-auth/claude",
                "CODEX_HOME": "/home/daytona/.holler-canary-auth/codex",
            },
            volumes=[],
        )
        validate_runner(runner, execution)
        runner.volumes = ["unexpected"]
        with self.assertRaisesRegex(RuntimeError, "must not mount FUSE volumes"):
            validate_runner(runner, execution)

    def test_claude_fixture_state_preserves_existing_account_data(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".claude.json"
            path.write_text(json.dumps({"oauthAccount": {"accountUuid": "keep-me"}}))
            source = claude_fixture_state_source(
                config_path=str(path),
                fixture="/cleanroom",
                version="2.1.259",
            )
            exec(source, {})
            config = json.loads(path.read_text())
        self.assertEqual(config["oauthAccount"], {"accountUuid": "keep-me"})
        self.assertTrue(config["hasCompletedOnboarding"])
        self.assertTrue(config["projects"]["/cleanroom"]["hasTrustDialogAccepted"])


if __name__ == "__main__":
    unittest.main()
