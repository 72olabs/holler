from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path
import tempfile
import unittest

SCRIPT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPT_DIR))

from budget import budget_for_tier, validate_estimate
from catalog import (
    CatalogError,
    TIER_SCENARIOS,
    load_catalog,
    scenarios_for_ids,
    scenarios_for_tier,
    validate_custom_handlers,
)


class CatalogTests(unittest.TestCase):
    def test_every_tier_fits_its_turn_budget(self) -> None:
        catalog = load_catalog()
        for tier in TIER_SCENARIOS:
            with self.subTest(tier=tier):
                validate_estimate(scenarios_for_tier(tier, catalog), budget_for_tier(tier))

    def test_release_tier_is_eleven_turns(self) -> None:
        scenarios = scenarios_for_tier("release", load_catalog())
        self.assertEqual(sum(item["estimated_model_turns"] for item in scenarios), 11)

    def test_explicit_selection_prepends_c0_and_preserves_order(self) -> None:
        scenarios = scenarios_for_ids(["C3", "C1"], load_catalog())
        self.assertEqual([item["id"] for item in scenarios], ["C0", "C3", "C1"])

    def test_explicit_selection_rejects_duplicates_and_unknown_ids(self) -> None:
        catalog = load_catalog()
        with self.assertRaisesRegex(CatalogError, "duplicate scenario ids"):
            scenarios_for_ids(["C1", "C1"], catalog)
        with self.assertRaisesRegex(CatalogError, "unknown scenario ids"):
            scenarios_for_ids(["C99"], catalog)

    def test_catalog_accepts_a_well_formed_custom_scenario(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            scenario_dir = Path(directory)
            for source in (SCRIPT_DIR / "scenarios").glob("C*.json"):
                if int(source.stem[1:]) < 9:
                    shutil.copy2(source, scenario_dir / source.name)
            custom = {
                "schema_version": 1,
                "id": "C9",
                "name": "Contributor scenario",
                "description": "Exercises a contributor-defined behavior.",
                "required_clients": ["claude", "codex"],
                "estimated_model_turns": 2,
                "timeout_seconds": 180,
                "checks": ["contributor-check"],
            }
            (scenario_dir / "C9.json").write_text(json.dumps(custom), encoding="utf-8")
            catalog = load_catalog(scenario_dir)
            self.assertIn("C9", catalog)
            handler_dir = scenario_dir / "handlers"
            handler_dir.mkdir()
            with self.assertRaisesRegex(CatalogError, r"missing=\['C9'\]"):
                validate_custom_handlers(catalog, handler_dir)
            (handler_dir / "C9.py").write_text("def run(worker): return []\n", encoding="utf-8")
            validate_custom_handlers(catalog, handler_dir)


if __name__ == "__main__":
    unittest.main()
