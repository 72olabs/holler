#!/usr/bin/env python3
"""Validate and load committed contributor canary handlers."""

from __future__ import annotations

import argparse
import ast
import importlib.util
import inspect
import json
from pathlib import Path
import re
from typing import Any, Callable


SCENARIO_ID_PATTERN = re.compile(r"^C(?:0|[1-9][0-9]*)$")
FORBIDDEN_IMPORTS = {"pty", "signal", "socket", "subprocess", "worker"}
FORBIDDEN_OS_CALLS = {
    "execl",
    "execle",
    "execlp",
    "execlpe",
    "execv",
    "execve",
    "execvp",
    "execvpe",
    "popen",
    "posix_spawn",
    "posix_spawnp",
    "spawnl",
    "spawnle",
    "spawnlp",
    "spawnlpe",
    "spawnv",
    "spawnve",
    "spawnvp",
    "spawnvpe",
    "system",
}


class HandlerContractError(ValueError):
    """A committed custom handler violates the contributor contract."""


def handler_path(handler_directory: Path, scenario_id: str) -> Path:
    if not SCENARIO_ID_PATTERN.fullmatch(scenario_id):
        raise HandlerContractError(f"invalid scenario id {scenario_id!r}")
    path = handler_directory / f"{scenario_id}.py"
    if not path.is_file():
        raise HandlerContractError(f"missing committed handler {path.name}")
    return path


def lint_handler(path: Path) -> None:
    try:
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=path.name)
    except (OSError, SyntaxError) as error:
        raise HandlerContractError(
            f"cannot parse committed handler {path.name}: {type(error).__name__}"
        ) from error

    os_aliases: set[str] = set()
    violations: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for imported in node.names:
                root = imported.name.split(".", 1)[0]
                if root in FORBIDDEN_IMPORTS:
                    violations.add(f"import {root}")
                if imported.name == "os":
                    os_aliases.add(imported.asname or "os")
        elif isinstance(node, ast.ImportFrom):
            root = (node.module or "").split(".", 1)[0]
            if root in FORBIDDEN_IMPORTS:
                violations.add(f"from {root} import")
            if node.module == "os" and any(item.name in FORBIDDEN_OS_CALLS for item in node.names):
                violations.add("process-launching import from os")
        elif isinstance(node, ast.Name) and node.id == "PtyProcess":
            violations.add("PtyProcess")
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        if (
            isinstance(node.func.value, ast.Name)
            and node.func.value.id in os_aliases
            and node.func.attr in FORBIDDEN_OS_CALLS
        ):
            violations.add(f"{node.func.value.id}.{node.func.attr}")
    if violations:
        raise HandlerContractError(
            f"committed handler {path.name} violates the static tripwire: {sorted(violations)}"
        )


def load_handler(path: Path) -> Callable[[Any], list[str]]:
    lint_handler(path)
    try:
        spec = importlib.util.spec_from_file_location(
            f"holler_canary_handler_{path.stem.lower()}",
            path,
        )
        if spec is None or spec.loader is None:
            raise HandlerContractError(f"cannot load committed handler {path.name}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        run = getattr(module, "run", None)
        if not callable(run):
            raise HandlerContractError(f"committed handler {path.name} must define callable run(context)")
        parameters = list(inspect.signature(run).parameters.values())
        if (
            len(parameters) != 1
            or parameters[0].kind
            not in {inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD}
        ):
            raise HandlerContractError(
                f"committed handler {path.name} run must accept exactly one positional context"
            )
        return run
    except HandlerContractError:
        raise
    except Exception as error:
        raise HandlerContractError(
            f"cannot import committed handler {path.name}: {type(error).__name__}"
        ) from error


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("handler_directory", type=Path)
    parser.add_argument("scenario_id")
    args = parser.parse_args()
    path = handler_path(args.handler_directory, args.scenario_id)
    load_handler(path)
    print(json.dumps({"handler": args.scenario_id, "status": "VALID"}, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except HandlerContractError as error:
        raise SystemExit(str(error)) from error
