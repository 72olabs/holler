#!/usr/bin/env python3
"""Validate a request and exercise its orchestration without vendor credentials."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from budget import BudgetExceeded, BudgetLedger  # noqa: E402
from manifest import ManifestError, canonical_json, load_request, sha256_bytes  # noqa: E402


def fake_scenario(scenario: dict[str, Any], ledger: BudgetLedger) -> dict[str, Any]:
    started = time.monotonic()
    turns = scenario["estimated_model_turns"]
    claude_turns = (turns + 1) // 2
    codex_turns = turns // 2
    if claude_turns:
        ledger.charge(client="claude", turns=claude_turns, cost_usd=0.002 * claude_turns)
    if codex_turns:
        ledger.charge(client="codex", turns=codex_turns, reported_tokens=1_500 * codex_turns)
    assertions = [{"name": check, "status": "PASS"} for check in scenario["checks"]]
    if scenario["id"] == "C7":
        isolated = BudgetLedger({"claude_usd": 0.0, "codex_reported_tokens": 0, "model_turns": 0, "wall_seconds": 0})
        try:
            isolated.charge(client="codex", turns=1, reported_tokens=1)
        except BudgetExceeded:
            pass
        else:
            assertions.append({"name": "budget-controller-rejected-overage", "status": "FAIL"})
    return {
        "id": scenario["id"],
        "name": scenario["name"],
        "status": "PASS" if all(item["status"] == "PASS" for item in assertions) else "FAIL",
        "duration_seconds": round(time.monotonic() - started, 6),
        "assertions": assertions,
    }


def run_fake(request: dict[str, Any]) -> dict[str, Any]:
    ledger = BudgetLedger(request["budget"])
    results = []
    status = "PASS"
    reason = ""
    try:
        for scenario in request["scenarios"]:
            result = fake_scenario(scenario, ledger)
            results.append(result)
            if result["status"] != "PASS":
                status = "FAIL"
    except BudgetExceeded as error:
        status, reason = "BUDGET_EXCEEDED", str(error)
    evidence: dict[str, Any] = {
        "schema_version": 1,
        "kind": "holler-canary-evidence",
        "driver": "fake",
        "request_hash": request["request_hash"],
        "source": request["source"],
        "tier": request["tier"],
        "status": status,
        "reason": reason,
        "results": results,
        "usage": ledger.as_dict(),
        "limits": request["budget"],
        "message_bodies_included": False,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    evidence["evidence_hash"] = sha256_bytes(canonical_json(evidence))
    return evidence


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("request", type=Path)
    parser.add_argument("--driver", choices=("fake",), default="fake")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--allow-model-override", action="store_true")
    args = parser.parse_args()
    request = load_request(args.request, allow_model_override=args.allow_model_override)
    evidence = run_fake(request)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "status": evidence["status"]}, sort_keys=True))
    if evidence["status"] != "PASS":
        raise SystemExit(1)


if __name__ == "__main__":
    try:
        main()
    except ManifestError as error:
        raise SystemExit(str(error)) from error
