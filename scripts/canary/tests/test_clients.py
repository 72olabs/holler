from __future__ import annotations

import sys
from pathlib import Path
import unittest

SCRIPT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPT_DIR))

from clients import (
    assert_low_cost_defaults,
    claude_print_command,
    client_policy,
    codex_exec_command,
)


class ClientPolicyTests(unittest.TestCase):
    def test_defaults_are_low_cost_and_bounded(self) -> None:
        policy = client_policy()
        assert_low_cost_defaults(policy)
        claude = claude_print_command(policy["claude"], 0.05)
        codex = codex_exec_command(policy["codex"])
        self.assertIn("haiku", claude)
        self.assertIn("--max-budget-usd", claude)
        self.assertIn("gpt-5.6-luna", codex)
        self.assertIn('model_reasoning_effort="low"', codex)
        self.assertNotIn("fast", " ".join(codex))

    def test_expensive_override_requires_explicit_allowance(self) -> None:
        with self.assertRaises(ValueError):
            assert_low_cost_defaults(client_policy(claude_model="opus"))


if __name__ == "__main__":
    unittest.main()
