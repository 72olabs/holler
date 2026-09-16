#!/usr/bin/env python3
"""Load and validate the committed real-client scenario catalog."""

from __future__ import annotations

import json
from pathlib import Path
import re
from typing import Any


SCENARIO_DIR = Path(__file__).with_name("scenarios")
HANDLER_DIR = Path(__file__).with_name("handlers")
TIER_SCENARIOS = {
    "preflight": ("C0",),
    "core": ("C0", "C1", "C2", "C3"),
    "release": ("C0", "C1", "C2", "C3", "C4", "C6"),
    "extended": ("C0", "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8"),
}
BUILTIN_SCENARIOS = frozenset(
    scenario_id for values in TIER_SCENARIOS.values() for scenario_id in values
)
SCENARIO_ID_PATTERN = re.compile(r"^C(?:0|[1-9][0-9]*)$")


class CatalogError(ValueError):
    """The committed scenario catalog is invalid."""


def load_catalog(directory: Path = SCENARIO_DIR) -> dict[str, dict[str, Any]]:
    catalog: dict[str, dict[str, Any]] = {}
    for path in sorted(directory.glob("C*.json")):
        try:
            scenario = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise CatalogError(f"cannot load {path}: {error}") from error
        validate_scenario(scenario, path)
        scenario_id = scenario["id"]
        if path.stem != scenario_id:
            raise CatalogError(f"{path} filename must match scenario id {scenario_id}")
        if scenario_id in catalog:
            raise CatalogError(f"duplicate scenario id {scenario_id}")
        catalog[scenario_id] = scenario
    missing = BUILTIN_SCENARIOS - set(catalog)
    if missing:
        raise CatalogError(f"scenario catalog is missing required built-ins: {sorted(missing)}")
    return catalog


def validate_scenario(scenario: object, path: Path) -> None:
    if not isinstance(scenario, dict):
        raise CatalogError(f"{path} must contain a JSON object")
    required = {
        "schema_version",
        "id",
        "name",
        "description",
        "required_clients",
        "estimated_model_turns",
        "timeout_seconds",
        "checks",
    }
    missing = required - set(scenario)
    if missing:
        raise CatalogError(f"{path} is missing {sorted(missing)}")
    if scenario["schema_version"] != 1:
        raise CatalogError(f"{path} has unsupported schema_version")
    if "daemon" in scenario and (scenario["daemon"] != {
        "conversations": True,
        "human_gateway": {"human": "human:canary", "scope": "observe+admin"},
    } or scenario["daemon"].get("conversations") is not True):
        raise CatalogError(f"{path} has unsupported daemon fixture configuration")
    if "daemon" in scenario and scenario.get("test_environment") != {
        "human": "synthetic-http", "write_policy": "generated-default",
        "terminal_oracle": "stopped-daemon-fixed-projection-and-public-api",
    }:
        raise CatalogError(f"{path} must declare the managed fixture evidence boundary")
    if not isinstance(scenario["id"], str) or not SCENARIO_ID_PATTERN.fullmatch(scenario["id"]):
        raise CatalogError(f"{path} has an invalid id")
    for key in ("required_clients", "checks"):
        if not isinstance(scenario[key], list) or not all(
            isinstance(value, str) and value for value in scenario[key]
        ):
            raise CatalogError(f"{path} field {key} must be a non-empty string list")
    if not isinstance(scenario["estimated_model_turns"], int) or scenario["estimated_model_turns"] < 0:
        raise CatalogError(f"{path} has invalid estimated_model_turns")
    if not isinstance(scenario["timeout_seconds"], int) or scenario["timeout_seconds"] <= 0:
        raise CatalogError(f"{path} has invalid timeout_seconds")


def scenarios_for_tier(tier: str, catalog: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    try:
        scenario_ids = TIER_SCENARIOS[tier]
    except KeyError as error:
        raise CatalogError(f"unknown tier {tier!r}; expected {', '.join(TIER_SCENARIOS)}") from error
    return [catalog[scenario_id] for scenario_id in scenario_ids]


def scenarios_for_ids(
    scenario_ids: list[str] | tuple[str, ...],
    catalog: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    """Select explicit scenarios, always placing the credential-free C0 gate first."""
    if not scenario_ids:
        raise CatalogError("explicit scenario selection cannot be empty")
    duplicates = sorted({item for item in scenario_ids if scenario_ids.count(item) > 1})
    if duplicates:
        raise CatalogError(f"duplicate scenario ids: {duplicates}")
    unknown = sorted(set(scenario_ids) - set(catalog))
    if unknown:
        raise CatalogError(f"unknown scenario ids: {unknown}")
    ordered = ["C0", *(item for item in scenario_ids if item != "C0")]
    return [catalog[scenario_id] for scenario_id in ordered]


def validate_custom_handlers(
    catalog: dict[str, dict[str, Any]],
    handler_directory: Path = HANDLER_DIR,
) -> None:
    custom_ids = set(catalog) - BUILTIN_SCENARIOS
    handler_ids = {path.stem for path in handler_directory.glob("C*.py")}
    missing = sorted(custom_ids - handler_ids)
    unexpected = sorted(handler_ids - custom_ids)
    if missing or unexpected:
        raise CatalogError(
            f"custom handler mismatch: missing={missing}, unexpected={unexpected}"
        )
