#!/usr/bin/env python3
"""Stateful fake Claude/Codex plugin CLI for credential-free product setup tests."""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys


def save(path: Path, state: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(state, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


def main() -> int:
    harness = Path(sys.argv[0]).name
    if harness not in {"claude", "codex"}:
        print(f"unsupported fake harness name: {harness}", file=sys.stderr)
        return 2
    state_root = os.environ.get("HOLLER_TEST_FAKE_CLIENT_STATE")
    if not state_root:
        print("HOLLER_TEST_FAKE_CLIENT_STATE is required", file=sys.stderr)
        return 2
    state_path = Path(state_root) / f"{harness}.json"
    if state_path.exists():
        state = json.loads(state_path.read_text(encoding="utf-8"))
    else:
        state = {"marketplaces": {}, "plugins": {}}
    args = sys.argv[1:]
    if args == ["plugin", "marketplace", "list", "--json"]:
        entries = [
            {"name": name, "path": source, "root": source, "marketplaceSource": {"source": source}}
            for name, source in sorted(state["marketplaces"].items())
        ]
        print(json.dumps(entries if harness == "claude" else {"marketplaces": entries}))
        return 0
    if args == ["plugin", "list", "--json"]:
        entries = [
            {"id": plugin_id, "enabled": enabled}
            for plugin_id, enabled in sorted(state["plugins"].items())
        ]
        print(json.dumps(entries))
        return 0
    if len(args) >= 4 and args[:3] == ["plugin", "marketplace", "add"]:
        source = args[3]
        state["marketplaces"]["holler"] = source
        save(state_path, state)
        return 0
    if len(args) >= 4 and args[:3] == ["plugin", "marketplace", "remove"]:
        state["marketplaces"].pop(args[3], None)
        save(state_path, state)
        return 0
    if harness == "claude" and len(args) >= 3 and args[:2] == ["plugin", "install"]:
        state["plugins"][args[2]] = True
        save(state_path, state)
        return 0
    if harness == "claude" and len(args) >= 3 and args[:2] in (["plugin", "update"], ["plugin", "enable"]):
        state["plugins"][args[2]] = True
        save(state_path, state)
        return 0
    if harness == "claude" and len(args) >= 3 and args[:2] == ["plugin", "uninstall"]:
        state["plugins"].pop(args[2], None)
        save(state_path, state)
        return 0
    if harness == "codex" and len(args) == 3 and args[:2] == ["plugin", "add"]:
        state["plugins"][args[2]] = True
        save(state_path, state)
        return 0
    if harness == "codex" and len(args) == 3 and args[:2] == ["plugin", "remove"]:
        if args[2] not in state["plugins"]:
            print("plugin not installed", file=sys.stderr)
            return 1
        state["plugins"].pop(args[2], None)
        save(state_path, state)
        return 0
    print(f"unsupported fake {harness} invocation: {args}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
