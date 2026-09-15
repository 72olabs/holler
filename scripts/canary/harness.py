#!/usr/bin/env python3
"""Agent-facing entrypoint for repeatable Holler checkpoint canaries."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import venv
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from catalog import BUILTIN_SCENARIOS, HANDLER_DIR, TIER_SCENARIOS  # noqa: E402
from clients import assert_low_cost_defaults, client_policy  # noqa: E402
from daytona_controller import (  # noqa: E402
    build_client_bundle,
    build_daytona,
    execution_plan,
    inspect_auth_runner,
    run_daytona,
)
from manifest import ManifestError, create_request, git, load_request, sha256_file  # noqa: E402
from run import run_fake  # noqa: E402


RUNTIME_MARKER = "HOLLER_CANARY_MANAGED_RUNTIME"


def emit_event(phase: str, status: str, **fields: object) -> None:
    """Emit body-free progress on stderr while reserving stdout for the result."""
    print(
        json.dumps(
            {"kind": "holler-canary-progress", "phase": phase, "status": status, **fields},
            sort_keys=True,
        ),
        file=sys.stderr,
        flush=True,
    )


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def validate_selected_handlers(request: dict[str, Any]) -> list[str]:
    """Import selected custom handlers in a short-lived credential-free process."""
    validated: list[str] = []
    environment = {
        "PATH": os.environ.get("PATH", ""),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONIOENCODING": "utf-8",
    }
    validator = SCRIPT_DIR / "handler_contract.py"
    for scenario in request["scenarios"]:
        scenario_id = scenario["id"]
        if scenario_id in BUILTIN_SCENARIOS:
            continue
        try:
            result = subprocess.run(
                [sys.executable, str(validator), str(HANDLER_DIR), scenario_id],
                cwd=REPO_ROOT,
                env=environment,
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
        except subprocess.TimeoutExpired as error:
            raise RuntimeError(f"custom handler {scenario_id} validation timed out") from error
        if result.returncode != 0:
            detail = (result.stderr or result.stdout).strip()[-1000:]
            raise RuntimeError(f"custom handler {scenario_id} validation failed: {detail}")
        validated.append(scenario_id)
    return validated


def checkpoint_directory(
    repo: Path,
    commit: str,
    tier: str,
    scenario_ids: list[str] | tuple[str, ...] | None = None,
) -> Path:
    namespace = tier
    if scenario_ids:
        joined = "-".join(scenario_ids)
        if len(joined) > 48:
            joined = "custom-" + hashlib.sha256(joined.encode("utf-8")).hexdigest()[:12]
        namespace = f"{tier}-{joined}"
    return repo.resolve() / ".runs" / "canary" / "checkpoints" / f"{commit[:12]}-{namespace}"


def command_for_checkpoint(
    *,
    tier: str,
    approval: str | None = None,
    forwarded: list[str] | None = None,
) -> str:
    command = ["python3", "scripts/canary/harness.py", "checkpoint", "--tier", tier]
    command.extend(forwarded or [])
    command.append("--execute")
    if approval is not None:
        command.extend(["--approve", approval])
    return shlex.join(command)


def runtime_python(repo: Path) -> Path:
    executable = "python.exe" if os.name == "nt" else "python"
    directory = "Scripts" if os.name == "nt" else "bin"
    return repo.resolve() / ".runs" / "canary" / "venv" / directory / executable


def runtime_ready(python: Path) -> bool:
    if not python.is_file():
        return False
    result = subprocess.run(
        [str(python), "-c", "import daytona"],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    return result.returncode == 0


def ensure_managed_runtime(repo: Path) -> None:
    """Install the pinned Daytona SDK in gitignored state and re-exec once."""
    try:
        import daytona  # noqa: F401
    except ImportError:
        pass
    else:
        return
    if os.environ.get(RUNTIME_MARKER) == "1":
        raise RuntimeError("managed canary runtime does not contain the pinned Daytona SDK")
    python = runtime_python(repo)
    if not runtime_ready(python):
        emit_event("managed-runtime", "STARTED")
        if not python.exists():
            venv.EnvBuilder(with_pip=True).create(python.parent.parent)
        requirements = SCRIPT_DIR / "requirements-daytona.txt"
        subprocess.run(
            [
                str(python),
                "-m",
                "pip",
                "install",
                "--disable-pip-version-check",
                "--requirement",
                str(requirements),
            ],
            check=True,
            timeout=600,
        )
        emit_event("managed-runtime", "PASS")
    environment = dict(os.environ)
    environment[RUNTIME_MARKER] = "1"
    os.execve(str(python), [str(python), str(Path(__file__).resolve()), *sys.argv[1:]], environment)


def selected_clients(args: argparse.Namespace) -> dict[str, dict[str, Any]]:
    policy = client_policy(
        claude_version=args.claude_version,
        codex_version=args.codex_version,
        claude_model=args.claude_model,
        codex_model=args.codex_model,
    )
    if not args.allow_model_override:
        assert_low_cost_defaults(policy)
    return policy


def forwarded_arguments(args: argparse.Namespace) -> list[str]:
    values: list[str] = []
    for name, default in (
        ("repo", REPO_ROOT),
        ("ref", "HEAD"),
        ("runner_name", "holler-canary-runner"),
    ):
        value = getattr(args, name)
        if str(value) != str(default):
            values.extend(["--" + name.replace("_", "-"), str(value)])
    for name in (
        "snapshot",
        "upgrade_from",
        "client_bundle",
        "output_dir",
        "claude_version",
        "codex_version",
        "claude_model",
        "codex_model",
    ):
        value = getattr(args, name)
        if value is not None:
            values.extend(["--" + name.replace("_", "-"), str(value)])
    if args.allow_model_override:
        values.append("--allow-model-override")
    for scenario_id in getattr(args, "scenario_ids", None) or []:
        values.extend(["--scenario", scenario_id])
    return values


def make_request(
    args: argparse.Namespace,
    *,
    artifact: Path | None = None,
    client_bundle: Path | None = None,
) -> dict[str, Any]:
    return create_request(
        args.repo,
        ref=args.ref,
        tier=args.tier,
        scenario_ids=args.scenario_ids,
        clients=selected_clients(args),
        artifact=artifact,
        upgrade_from=args.upgrade_from,
        client_bundle=client_bundle,
        snapshot=args.snapshot,
        runner_name=args.runner_name,
    )


def paths_for(args: argparse.Namespace, request: dict[str, Any]) -> dict[str, Path]:
    selected = [item["id"] for item in request["scenarios"]] if args.scenario_ids else None
    root = args.output_dir or checkpoint_directory(
        args.repo,
        request["source"]["commit"],
        args.tier,
        selected,
    )
    version = request["source"]["connector_version"]
    return {
        "root": root,
        "draft_request": root / "draft-request.json",
        "artifact": root / f"holler-{version}-linux-amd64.tar.gz",
        "client_bundle": root / "minimum-client-bundle.tar.gz",
        "request": root / "request.json",
        "fake_evidence": root / "fake-evidence.json",
        "plan": root / "plan.json",
        "evidence": root / "evidence.json",
    }


def write_local_contract(request: dict[str, Any], paths: dict[str, Path]) -> None:
    write_json(paths["request"], request)
    write_json(paths["fake_evidence"], run_fake(request))
    write_json(paths["plan"], execution_plan(request))


def reusable_artifact(
    *,
    paths: dict[str, Path],
    expected_commit: str,
    tier: str,
    scenario_ids: list[str] | None = None,
) -> bool:
    if not paths["artifact"].is_file() or not paths["request"].is_file():
        return False
    try:
        request = load_request(paths["request"])
    except ManifestError:
        return False
    return bool(
        request["source"]["commit"] == expected_commit
        and request["tier"] == tier
        and (scenario_ids is None or [item["id"] for item in request["scenarios"]] == scenario_ids)
        and request["artifact"].get("sha256") == sha256_file(paths["artifact"])
    )


def doctor(args: argparse.Namespace) -> None:
    repo = args.repo.resolve()
    python = runtime_python(repo)
    changed = git(repo, "status", "--porcelain", "--untracked-files=all", "--", "scripts/canary")
    ready = runtime_ready(python)
    local_ready = bool(os.environ.get("DAYTONA_API_KEY")) and not changed
    result: dict[str, Any] = {
        "status": "LOCAL_READY" if local_ready else "ACTION_REQUIRED",
        "repository": str(repo),
        "commit": git(repo, "rev-parse", "HEAD"),
        "harness_committed": not bool(changed),
        "daytona_api_key": "configured" if os.environ.get("DAYTONA_API_KEY") else "missing",
        "managed_runtime": "ready" if ready else "will be installed on first --execute",
        "agent_workflow": [
            command_for_checkpoint(tier="core"),
            "rerun the printed command with the exact --approve sha256:... value",
        ],
    }
    if args.execute:
        if not os.environ.get("DAYTONA_API_KEY"):
            result["remote"] = {
                "status": "ACTION_REQUIRED",
                "operator_action": "expose DAYTONA_API_KEY to the agent process",
            }
        else:
            ensure_managed_runtime(repo)
            emit_event("runner-doctor", "STARTED")
            remote = inspect_auth_runner(make_request(args))
            emit_event("runner-doctor", remote["status"])
            result["remote"] = remote
            result["status"] = "READY" if local_ready and remote["status"] == "READY" else "ACTION_REQUIRED"
    print(json.dumps(result, indent=2, sort_keys=True))


def check(args: argparse.Namespace) -> None:
    request = make_request(args)
    validated_handlers = validate_selected_handlers(request)
    paths = paths_for(args, request)
    write_json(paths["draft_request"], request)
    write_json(paths["fake_evidence"], run_fake(request))
    write_json(paths["plan"], execution_plan(request))
    print(
        json.dumps(
            {
                "status": "READY_TO_BUILD",
                "commit": request["source"]["commit"],
                "tier": request["tier"],
                "scenarios": [item["id"] for item in request["scenarios"]],
                "validated_custom_handlers": validated_handlers,
                "estimated_model_turns": sum(
                    item["estimated_model_turns"] for item in request["scenarios"]
                ),
                "draft_request": str(paths["draft_request"]),
                "fake_evidence": str(paths["fake_evidence"]),
                "plan": str(paths["plan"]),
                "next_command": command_for_checkpoint(
                    tier=args.tier,
                    forwarded=forwarded_arguments(args),
                ),
            },
            indent=2,
            sort_keys=True,
        )
    )


def checkpoint(args: argparse.Namespace) -> None:
    if not args.execute:
        raise RuntimeError("checkpoint is non-mutating unless --execute is supplied")
    if not os.environ.get("DAYTONA_API_KEY"):
        raise RuntimeError("DAYTONA_API_KEY is not set; an operator must expose it to the agent process")
    ensure_managed_runtime(args.repo)
    emit_event("contract", "STARTED", tier=args.tier)
    draft = make_request(args)
    validate_selected_handlers(draft)
    paths = paths_for(args, draft)
    paths["root"].mkdir(parents=True, exist_ok=True)
    write_json(paths["draft_request"], draft)
    emit_event("contract", "PASS", commit=draft["source"]["commit"])
    scenario_ids = {item["id"] for item in draft["scenarios"]}
    if "C6" in scenario_ids and args.upgrade_from is None:
        raise RuntimeError(
            "this tier requires --upgrade-from pointing to the checksum-verified v0.7.1 Linux archive"
        )
    reusable = reusable_artifact(
        paths=paths,
        expected_commit=draft["source"]["commit"],
        tier=args.tier,
        scenario_ids=[item["id"] for item in draft["scenarios"]],
    )
    client_bundle = args.client_bundle
    if "C8" in scenario_ids and client_bundle is None:
        client_bundle = paths["client_bundle"]
        if not client_bundle.is_file():
            emit_event("minimum-client-bundle", "STARTED")
            build_client_bundle(draft, output=client_bundle)
            emit_event("minimum-client-bundle", "PASS", path=str(client_bundle))
    if not reusable:
        emit_event("artifact-build", "STARTED")
        build_daytona(draft, repo=args.repo, output=paths["artifact"])
        emit_event("artifact-build", "PASS", path=str(paths["artifact"]))
    else:
        emit_event("artifact-build", "REUSED", path=str(paths["artifact"]))
    request = make_request(args, artifact=paths["artifact"], client_bundle=client_bundle)
    write_local_contract(request, paths)
    approved = args.approve == request["request_hash"]
    if not approved:
        emit_event("approval", "REQUIRED", request_hash=request["request_hash"])
        print(
            json.dumps(
                {
                    "status": "APPROVAL_REQUIRED",
                    "commit": request["source"]["commit"],
                    "tier": request["tier"],
                    "artifact": str(paths["artifact"]),
                    "artifact_sha256": request["artifact"]["sha256"],
                    "request": str(paths["request"]),
                    "request_hash": request["request_hash"],
                    "budget": request["budget"],
                    "plan": str(paths["plan"]),
                    "next_command": command_for_checkpoint(
                        tier=args.tier,
                        approval=request["request_hash"],
                        forwarded=forwarded_arguments(args),
                    ),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return
    emit_event("approval", "PASS", request_hash=request["request_hash"])
    emit_event("credentialed-canary", "STARTED", tier=request["tier"])
    result = run_daytona(
        request,
        archive=paths["artifact"],
        output=paths["evidence"],
        keep_on_failure=args.keep_on_failure,
        upgrade_from=args.upgrade_from,
        client_bundle=client_bundle,
    )
    evidence = json.loads(paths["evidence"].read_text(encoding="utf-8"))
    emit_event("credentialed-canary", "PASS", evidence_hash=evidence["evidence_hash"])
    print(
        json.dumps(
            {
                **result,
                "request_hash": request["request_hash"],
                "evidence_hash": evidence["evidence_hash"],
                "usage": evidence["usage"],
            },
            indent=2,
            sort_keys=True,
        )
    )


def add_request_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--repo", type=Path, default=REPO_ROOT)
    parser.add_argument("--ref", default="HEAD")
    parser.add_argument("--tier", choices=TIER_SCENARIOS, default="core")
    parser.add_argument(
        "--scenario",
        dest="scenario_ids",
        action="append",
        help="run this scenario instead of the tier's default set; repeat for more (C0 is automatic)",
    )
    parser.add_argument("--runner-name", default="holler-canary-runner")
    parser.add_argument("--snapshot")
    parser.add_argument("--upgrade-from", type=Path)
    parser.add_argument("--client-bundle", type=Path)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--claude-version")
    parser.add_argument("--codex-version")
    parser.add_argument("--claude-model")
    parser.add_argument("--codex-model")
    parser.add_argument("--allow-model-override", action="store_true")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    doctor_parser = subparsers.add_parser("doctor", help="show local or real-runner prerequisites")
    add_request_arguments(doctor_parser)
    doctor_parser.add_argument(
        "--execute",
        action="store_true",
        help="start the existing runner if needed, verify both logins, then restore its power state",
    )
    check_parser = subparsers.add_parser("check", help="write the zero-cost local contract and plan")
    add_request_arguments(check_parser)
    checkpoint_parser = subparsers.add_parser(
        "checkpoint",
        help="build once, stop for hash approval, then run the real canary",
    )
    add_request_arguments(checkpoint_parser)
    checkpoint_parser.add_argument("--execute", action="store_true")
    checkpoint_parser.add_argument("--approve", help="exact request hash printed by the build phase")
    checkpoint_parser.add_argument("--keep-on-failure", action="store_true")
    args = parser.parse_args()
    if args.command == "doctor":
        doctor(args)
    elif args.command == "check":
        check(args)
    else:
        checkpoint(args)


if __name__ == "__main__":
    try:
        main()
    except (ManifestError, RuntimeError, OSError, subprocess.SubprocessError, ValueError) as error:
        raise SystemExit(str(error)) from error
