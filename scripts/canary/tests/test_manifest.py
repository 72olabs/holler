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
        validate_request(request)

    def test_tampering_is_rejected(self) -> None:
        request = create_request(REPO, ref="HEAD", tier="core", clients=client_policy())
        changed = copy.deepcopy(request)
        changed["budget"]["model_turns"] += 1
        with self.assertRaises(ManifestError):
            validate_request(changed)

    def test_secret_like_fields_are_rejected(self) -> None:
        request = create_request(REPO, ref="HEAD", tier="core", clients=client_policy())
        request["api_key"] = "must-not-appear"
        request["request_hash"] = request_hash(request)
        with self.assertRaisesRegex(ManifestError, "secret-like"):
            validate_request(request)


if __name__ == "__main__":
    unittest.main()
