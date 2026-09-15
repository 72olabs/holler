from __future__ import annotations

import sys
from pathlib import Path
import unittest

SCRIPT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPT_DIR))

from budget import budget_for_tier, validate_estimate
from catalog import TIER_SCENARIOS, load_catalog, scenarios_for_tier


class CatalogTests(unittest.TestCase):
    def test_every_tier_fits_its_turn_budget(self) -> None:
        catalog = load_catalog()
        for tier in TIER_SCENARIOS:
            with self.subTest(tier=tier):
                validate_estimate(scenarios_for_tier(tier, catalog), budget_for_tier(tier))

    def test_release_tier_is_twelve_turns(self) -> None:
        scenarios = scenarios_for_tier("release", load_catalog())
        self.assertEqual(sum(item["estimated_model_turns"] for item in scenarios), 12)


if __name__ == "__main__":
    unittest.main()
