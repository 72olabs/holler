from __future__ import annotations

import json
import sys
from pathlib import Path
import tempfile
from types import ModuleType
from types import SimpleNamespace
import unittest
from unittest.mock import patch

SCRIPT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPT_DIR))

from clients import client_policy
from daytona_controller import (
    BUILDER_DOMAINS,
    CREDENTIAL_DOMAINS,
    NETWORK_POLICY_LABEL,
    NETWORK_POLICY_ORGANIZATION,
    claude_fixture_state_source,
    create_with_network_policy,
    execution_plan,
    inspect_auth_runner,
    sandbox_state,
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
        self.assertIn("*.claude.com", plan["network_policy"]["requested_canary_domains"])
        self.assertEqual(plan["network_policy"]["requested_builder_domains"], BUILDER_DOMAINS)
        self.assertIn("proxy.golang.org", plan["network_policy"]["requested_builder_domains"])

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

        runner.volumes = []
        runner.labels[NETWORK_POLICY_LABEL] = NETWORK_POLICY_ORGANIZATION
        runner.domain_allow_list = None
        validate_runner(runner, execution)

    def test_network_policy_falls_back_only_for_tier_rejection(self) -> None:
        class BadRequest(Exception):
            pass

        class Params:
            def __init__(self, **values: object) -> None:
                self.values = values

        class Daytona:
            def __init__(self) -> None:
                self.calls = []

            def create(self, params: Params, timeout: int) -> SimpleNamespace:
                self.calls.append(params.values)
                if len(self.calls) == 1:
                    raise BadRequest(
                        "Network access is restricted and cannot be overridden at the sandbox level"
                    )
                return SimpleNamespace(id="sandbox")

        daytona = Daytona()
        sandbox, policy = create_with_network_policy(
            daytona,
            Params,
            params={"labels": {"purpose": "test"}},
            domains=["example.com"],
            timeout=120,
            bad_request_type=BadRequest,
        )
        self.assertEqual(sandbox.id, "sandbox")
        self.assertEqual(policy, NETWORK_POLICY_ORGANIZATION)
        self.assertEqual(daytona.calls[0]["domain_allow_list"], "example.com")
        self.assertNotIn("domain_allow_list", daytona.calls[1])
        self.assertEqual(daytona.calls[1]["labels"][NETWORK_POLICY_LABEL], NETWORK_POLICY_ORGANIZATION)

    def test_sandbox_state_normalizes_sdk_enum_or_string(self) -> None:
        self.assertEqual(sandbox_state(SimpleNamespace(state="STARTED")), "started")
        self.assertEqual(
            sandbox_state(SimpleNamespace(state=SimpleNamespace(value="STOPPED"))),
            "stopped",
        )

    def test_runner_doctor_checks_auth_and_restores_stopped_runner(self) -> None:
        request = create_request(REPO, ref="HEAD", tier="core", clients=client_policy())

        class Process:
            def exec(self, command: str, timeout: int) -> SimpleNamespace:
                self.last_timeout = timeout
                return SimpleNamespace(exit_code=0)

        class Sandbox:
            def __init__(self) -> None:
                self.state = "stopped"
                self.process = Process()
                self.stop_calls = 0

            def start(self, timeout: int) -> None:
                self.state = "started"

            def stop(self, timeout: int) -> None:
                self.state = "stopped"
                self.stop_calls += 1

        sandbox = Sandbox()
        daytona_module = ModuleType("daytona")
        daytona_module.Daytona = lambda: SimpleNamespace(get=lambda name: sandbox)  # type: ignore[attr-defined]
        errors_module = ModuleType("daytona.common.errors")
        errors_module.DaytonaNotFoundError = RuntimeError  # type: ignore[attr-defined]
        with (
            patch.dict(
                sys.modules,
                {"daytona": daytona_module, "daytona.common.errors": errors_module},
            ),
            patch("daytona_controller.require_committed_controller"),
            patch("daytona_controller.require_daytona_key"),
            patch("daytona_controller.validate_runner"),
        ):
            result = inspect_auth_runner(request)

        self.assertEqual(result["status"], "READY")
        self.assertEqual(result["oauth"], {"claude": "READY", "codex": "READY"})
        self.assertEqual(sandbox.state, "stopped")
        self.assertEqual(sandbox.stop_calls, 1)

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
