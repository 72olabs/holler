from __future__ import annotations

import sys
from pathlib import Path
import unittest

SCRIPT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPT_DIR))

from budget import BudgetExceeded, BudgetLedger, budget_for_tier


class BudgetTests(unittest.TestCase):
    def test_charge_tracks_each_client(self) -> None:
        ledger = BudgetLedger(budget_for_tier("core"))
        ledger.charge(client="claude", cost_usd=0.01, turns=1)
        ledger.charge(client="codex", reported_tokens=1000, turns=1)
        self.assertEqual(
            ledger.as_dict(),
            {"claude_usd": 0.01, "codex_reported_tokens": 1000, "model_turns": 2, "wall_seconds": 0.0},
        )

    def test_overage_is_rejected_before_mutation(self) -> None:
        ledger = BudgetLedger(budget_for_tier("preflight"))
        with self.assertRaises(BudgetExceeded):
            ledger.charge(client="codex", reported_tokens=1, turns=1)
        self.assertEqual(ledger.model_turns, 0)
        self.assertEqual(ledger.codex_reported_tokens, 0)

    def test_capacity_check_does_not_record_unconfirmed_turn(self) -> None:
        ledger = BudgetLedger(budget_for_tier("core"))
        ledger.ensure_capacity(client="claude", turns=1)
        self.assertEqual(ledger.model_turns, 0)
        ledger.charge(client="claude", turns=1, cost_usd=0.01)
        self.assertEqual(ledger.model_turns, 1)


if __name__ == "__main__":
    unittest.main()
