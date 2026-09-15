#!/usr/bin/env python3
"""Prepare an immutable, human-reviewable real-client canary request."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from catalog import TIER_SCENARIOS  # noqa: E402
from clients import assert_low_cost_defaults, client_policy  # noqa: E402
from manifest import ManifestError, create_request  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=SCRIPT_DIR.parent.parent)
    parser.add_argument("--ref", default="HEAD", help="committed Git ref to test; no PR is required")
    parser.add_argument("--tier", choices=TIER_SCENARIOS, default="core")
    parser.add_argument("--artifact", type=Path, help="optional already-built release archive")
    parser.add_argument("--upgrade-from", type=Path, help="checksum-bind the v0.7.1 upgrade fixture")
    parser.add_argument("--client-bundle", type=Path, help="checksum-bind the minimum-client bundle")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--claude-version")
    parser.add_argument("--codex-version")
    parser.add_argument("--claude-model")
    parser.add_argument("--codex-model")
    parser.add_argument("--allow-model-override", action="store_true")
    parser.add_argument("--snapshot", help="defaults to a name derived from both pinned client versions")
    parser.add_argument("--runner-name", default="holler-canary-runner")
    args = parser.parse_args()
    policy = client_policy(
        claude_version=args.claude_version,
        codex_version=args.codex_version,
        claude_model=args.claude_model,
        codex_model=args.codex_model,
    )
    if not args.allow_model_override:
        assert_low_cost_defaults(policy)
    request = create_request(
        args.repo,
        ref=args.ref,
        tier=args.tier,
        clients=policy,
        artifact=args.artifact,
        upgrade_from=args.upgrade_from,
        client_bundle=args.client_bundle,
        snapshot=args.snapshot,
        runner_name=args.runner_name,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(request, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(args.output), "request_hash": request["request_hash"]}, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (ManifestError, ValueError) as error:
        raise SystemExit(str(error)) from error
