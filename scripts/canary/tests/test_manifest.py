from __future__ import annotations

import copy
import sys
from pathlib import Path
import unittest

SCRIPT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPT_DIR))

from clients import client_policy
from manifest import ManifestError, create_request, request_hash, validate_request


REPO = SCRIPT_DIR.parent.parent


class ManifestTests(unittest.TestCase):
    def test_request_binds_commit_tree_scenarios_and_models(self) -> None:
        request = create_request(REPO, ref="HEAD", tier="core", clients=client_policy())
        self.assertEqual(request["request_hash"], request_hash(request))
        self.assertEqual(len(request["source"]["commit"]), 40)
        self.assertEqual(len(request["source"]["tree"]), 40)
        self.assertEqual(request["clients"]["claude"]["model"], "haiku")
        self.assertEqual(request["clients"]["codex"]["model"], "gpt-5.6-luna")
        self.assertEqual(request["execution"]["go_toolchain"]["version"], "1.26.0")
        self.assertIn("go-1-26-0", request["execution"]["snapshot"])
        self.assertEqual(request["execution"]["runner_name"], "holler-canary-runner")
        self.assertTrue(request["execution"]["runner_persistent"])
        self.assertEqual(
            request["execution"]["runner_fixture"],
            "/home/daytona/.holler-canary-workspace",
        )
        validate_request(request)

    def test_tampering_is_rejected(self) -> None:
        request = create_request(REPO, ref="HEAD", tier="core", clients=client_policy())
        changed = copy.deepcopy(request)
        changed["budget"]["model_turns"] += 1
        with self.assertRaises(ManifestError):
            validate_request(changed)

    def test_unapproved_go_toolchain_is_rejected(self) -> None:
        request = create_request(REPO, ref="HEAD", tier="core", clients=client_policy())
        request["execution"]["go_toolchain"]["version"] = "1.26.1"
        request["request_hash"] = request_hash(request)
        with self.assertRaisesRegex(ManifestError, "unsupported Daytona Go toolchain"):
            validate_request(request)

    def test_ephemeral_credentialed_runner_is_rejected(self) -> None:
        request = create_request(REPO, ref="HEAD", tier="core", clients=client_policy())
        request["execution"]["runner_persistent"] = False
        request["request_hash"] = request_hash(request)
        with self.assertRaisesRegex(ManifestError, "must be persistent"):
            validate_request(request)

    def test_runner_fixture_tampering_is_rejected(self) -> None:
        request = create_request(REPO, ref="HEAD", tier="preflight", clients=client_policy())
        request["execution"]["runner_fixture"] = "/tmp/untrusted-fixture"
        request["request_hash"] = request_hash(request)
        with self.assertRaisesRegex(ManifestError, "dedicated stable fixture"):
            validate_request(request)

    def test_secret_like_fields_are_rejected(self) -> None:
        request = create_request(REPO, ref="HEAD", tier="core", clients=client_policy())
        request["api_key"] = "must-not-appear"
        request["request_hash"] = request_hash(request)
        with self.assertRaisesRegex(ManifestError, "secret-like"):
            validate_request(request)


if __name__ == "__main__":
    unittest.main()
