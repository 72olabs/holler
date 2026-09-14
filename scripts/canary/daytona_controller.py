#!/usr/bin/env python3
"""Render or execute the Daytona control-plane portion of a Holler canary."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tarfile
import tempfile
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from manifest import ManifestError, git, load_request, sha256_file  # noqa: E402


def execution_plan(request: dict[str, Any]) -> dict[str, Any]:
    execution = request["execution"]
    return {
        "schema_version": 1,
        "kind": "holler-daytona-plan",
        "request_hash": request["request_hash"],
        "resource_policy": {
            "builder": {
                "credentials": [],
                "source": request["source"],
                "purpose": "build and verify the exact committed tree",
                "delete_after_artifact_download": True,
            },
            "canary": {
                "credentials": ["claude-subscription-oauth", "codex-subscription-oauth"],
                "source_checkout": False,
                "artifact_sha256": request["artifact"].get("sha256", "produced-by-builder"),
                "auth_volume": execution["auth_volume"],
                "snapshot": execution["snapshot"],
                "ephemeral": True,
                "auto_delete_minutes": execution["auto_delete_minutes"],
            },
        },
        "network_policy": {
            "credentialed_sandbox_enforced": True,
            "builder": ["source-host", "go-modules"],
            "canary": [
                "api.anthropic.com", "*.anthropic.com", "claude.ai", "*.claude.ai",
                "api.openai.com", "*.openai.com", "chatgpt.com", "*.chatgpt.com",
            ],
        },
        "budget": request["budget"],
        "models": {
            "claude": request["clients"]["claude"]["model"],
            "codex": request["clients"]["codex"]["model"],
        },
        "approval_boundary": "An operator approves request_hash before any credentialed sandbox starts.",
        "cleanup": [
            "stop both clients",
            "stop hollerd",
            "download body-free evidence",
            "delete credentialed sandbox",
            "delete uncredentialed builder sandbox",
        ],
    }


def probe_daytona(*, keep: bool) -> dict[str, Any]:
    if not os.environ.get("DAYTONA_API_KEY"):
        raise RuntimeError("DAYTONA_API_KEY is not set")
    try:
        from daytona import CreateSandboxFromSnapshotParams, Daytona
    except ImportError as error:
        raise RuntimeError(
            "Daytona SDK is missing; install scripts/canary/requirements-daytona.txt in an isolated venv"
        ) from error
    daytona = Daytona()
    sandbox = daytona.create(
        CreateSandboxFromSnapshotParams(
            language="python",
            ephemeral=not keep,
            auto_delete_interval=60,
        )
    )
    result: dict[str, Any] = {"sandbox_id": sandbox.id, "kept": keep}
    try:
        response = sandbox.process.exec(
            "python3 -c 'import os,platform; print(platform.system(), platform.machine(), os.getuid())'",
            timeout=60,
        )
        result.update({"exit_code": response.exit_code, "output": response.result.strip()})
        if response.exit_code != 0:
            raise RuntimeError(f"Daytona probe failed with exit code {response.exit_code}")
    finally:
        if not keep:
            sandbox.delete()
            result["deleted"] = True
    return result


def make_runtime_bundle(output: Path) -> None:
    included = [
        "__init__.py",
        "budget.py",
        "catalog.py",
        "clients.py",
        "manifest.py",
        "worker.py",
    ]
    with tarfile.open(output, "w:gz") as bundle:
        for name in included:
            bundle.add(SCRIPT_DIR / name, arcname=f"holler-canary/{name}")
        for scenario in sorted((SCRIPT_DIR / "scenarios").glob("C*.json")):
            bundle.add(scenario, arcname=f"holler-canary/scenarios/{scenario.name}")


def require_daytona_key() -> None:
    if not os.environ.get("DAYTONA_API_KEY"):
        raise RuntimeError("DAYTONA_API_KEY is not set")


def require_committed_controller(request: dict[str, Any], repo: Path) -> None:
    repo = repo.resolve()
    if git(repo, "rev-parse", "HEAD") != request["source"]["commit"]:
        raise RuntimeError("controller HEAD does not match the commit in the approved request")
    changed = git(repo, "status", "--porcelain", "--untracked-files=all", "--", "scripts/canary")
    if changed:
        raise RuntimeError("scripts/canary has uncommitted changes; commit and prepare a new request")


def build_daytona(request: dict[str, Any], *, repo: Path, output: Path) -> dict[str, Any]:
    require_committed_controller(request, repo)
    require_daytona_key()
    try:
        from daytona import CreateSandboxFromSnapshotParams, Daytona
    except ImportError as error:
        raise RuntimeError(
            "Daytona SDK is missing; install scripts/canary/requirements-daytona.txt in an isolated venv"
        ) from error
    execution = request["execution"]
    source = request["source"]
    with tempfile.TemporaryDirectory(prefix="holler-canary-source-") as directory:
        source_archive = Path(directory) / "source.tar"
        try:
            subprocess.run(
                [
                    "git", "-C", str(repo.resolve()), "archive", "--format=tar",
                    "--prefix=holler-source/", "-o", str(source_archive), source["commit"],
                ],
                check=True,
                capture_output=True,
                text=True,
                timeout=60,
            )
        except (OSError, subprocess.SubprocessError) as error:
            raise RuntimeError(f"cannot archive committed source: {error}") from error
        daytona = Daytona()
        sandbox = daytona.create(
            CreateSandboxFromSnapshotParams(
                language="python",
                snapshot=execution["snapshot"],
                ephemeral=True,
                ttl_minutes=60,
                labels={"purpose": "holler-canary-builder", "commit": source["commit"][:12]},
            ),
            timeout=120,
        )
        try:
            sandbox.fs.upload_file(str(source_archive), "/tmp/holler-source.tar")
            command = " && ".join(
                [
                    "mkdir -p /tmp/holler-build",
                    "tar -xf /tmp/holler-source.tar -C /tmp/holler-build",
                    "mkdir -p /tmp/holler-dist",
                    "cd /tmp/holler-build/holler-source",
                    f"HOLLER_COMMIT={shlex.quote(source['commit'])} HOLLER_DIRTY=false "
                    f"HOLLER_VERSION={shlex.quote(source['connector_version'])} "
                    "ARTIFACT_ROOT=/tmp/holler-dist HOLLER_CI_HOME_AUDIT=off ./scripts/ci/run.sh",
                ]
            )
            response = sandbox.process.exec(command, timeout=2400)
            if response.exit_code != 0:
                raise RuntimeError(f"uncredentialed Daytona builder exited {response.exit_code}")
            platform_result = sandbox.process.exec("go env GOOS GOARCH", timeout=30)
            values = platform_result.result.split()
            if platform_result.exit_code != 0 or len(values) != 2:
                raise RuntimeError("cannot determine builder platform")
            filename = f"holler-{source['connector_version']}-{values[0]}-{values[1]}.tar.gz"
            output.parent.mkdir(parents=True, exist_ok=True)
            sandbox.fs.download_file(f"/tmp/holler-dist/{filename}", str(output))
            remote_sidecar = Path(directory) / "remote.sha256"
            sandbox.fs.download_file(f"/tmp/holler-dist/{filename}.sha256", str(remote_sidecar))
            digest = sha256_file(output).removeprefix("sha256:")
            fields = remote_sidecar.read_text(encoding="utf-8").split()
            if not fields or fields[0].lower() != digest:
                raise RuntimeError("builder checksum sidecar does not match downloaded archive")
            Path(str(output) + ".sha256").write_text(f"{digest}  {output.name}\n", encoding="utf-8")
        finally:
            sandbox.delete()
    return {"status": "PASS", "artifact": str(output), "sha256": sha256_file(output)}


def bootstrap_daytona(request: dict[str, Any]) -> dict[str, Any]:
    require_committed_controller(request, SCRIPT_DIR.parent.parent)
    require_daytona_key()
    try:
        from daytona import CreateSandboxFromSnapshotParams, Daytona
    except ImportError as error:
        raise RuntimeError(
            "Daytona SDK is missing; install scripts/canary/requirements-daytona.txt in an isolated venv"
        ) from error
    execution = request["execution"]
    clients = request["clients"]
    go_toolchain = execution["go_toolchain"]
    daytona = Daytona()
    auth_volume = daytona.volume.get(execution["auth_volume"], create=True)
    sandbox = daytona.create(
        CreateSandboxFromSnapshotParams(
            language="python",
            snapshot="daytona-medium",
            auto_delete_interval=0,
            ttl_minutes=60,
            labels={"purpose": "holler-canary-snapshot-builder"},
        ),
        timeout=120,
    )
    try:
        packages = [
            f"@anthropic-ai/claude-code@{clients['claude']['version']}",
            f"@openai/codex@{clients['codex']['version']}",
        ]
        install = "npm install --global " + " ".join(shlex.quote(value) for value in packages)
        go_archive = f"/tmp/{go_toolchain['filename']}"
        command = " && ".join(
            [
                "command -v git",
                "command -v node",
                "command -v npm",
                "command -v curl",
                f"curl --fail --silent --show-error --location --output {shlex.quote(go_archive)} "
                f"https://go.dev/dl/{shlex.quote(go_toolchain['filename'])}",
                f"printf '%s  %s\\n' {shlex.quote(go_toolchain['sha256'])} "
                f"{shlex.quote(go_archive)} | sha256sum --check --strict",
                "sudo rm -rf /usr/local/go",
                f"sudo tar -C /usr/local -xzf {shlex.quote(go_archive)}",
                "sudo ln -sfn /usr/local/go/bin/go /usr/local/bin/go",
                "sudo ln -sfn /usr/local/go/bin/gofmt /usr/local/bin/gofmt",
                f"test \"$(go version | sed -E 's/^go version go([^ ]+).*/\\1/')\" = "
                f"{shlex.quote(go_toolchain['version'])}",
                install,
                f"test \"$(claude --version | sed -E 's/[^0-9]*([0-9]+\\.[0-9]+\\.[0-9]+).*/\\1/')\" = {shlex.quote(clients['claude']['version'])}",
                f"test \"$(codex --version | sed -E 's/[^0-9]*([0-9]+\\.[0-9]+\\.[0-9]+).*/\\1/')\" = {shlex.quote(clients['codex']['version'])}",
            ]
        )
        response = sandbox.process.exec(command, timeout=900)
        if response.exit_code != 0:
            detail = (response.result or "").strip()[-4000:]
            raise RuntimeError(
                f"client snapshot bootstrap exited {response.exit_code}"
                + (f":\n{detail}" if detail else "")
            )
        sandbox.create_snapshot(execution["snapshot"], timeout=600)
    finally:
        sandbox.delete()
    return {
        "status": "PASS",
        "snapshot": execution["snapshot"],
        "auth_volume": auth_volume.name,
        "credentials_in_snapshot": False,
    }


def create_auth_sandbox(request: dict[str, Any]) -> dict[str, Any]:
    require_committed_controller(request, SCRIPT_DIR.parent.parent)
    require_daytona_key()
    try:
        from daytona import CreateSandboxFromSnapshotParams, Daytona, VolumeMount
    except ImportError as error:
        raise RuntimeError(
            "Daytona SDK is missing; install scripts/canary/requirements-daytona.txt in an isolated venv"
        ) from error
    execution = request["execution"]
    daytona = Daytona()
    auth_volume = daytona.volume.get(execution["auth_volume"], create=False)
    mount = "/home/daytona/.holler-canary-auth"
    sandbox = daytona.create(
        CreateSandboxFromSnapshotParams(
            language="python",
            snapshot=execution["snapshot"],
            ttl_minutes=120,
            auto_delete_interval=0,
            labels={"purpose": "holler-canary-auth-bootstrap"},
            domain_allow_list=(
                "api.anthropic.com,*.anthropic.com,claude.ai,*.claude.ai,"
                "api.openai.com,*.openai.com,chatgpt.com,*.chatgpt.com"
            ),
            env_vars={"CLAUDE_CONFIG_DIR": f"{mount}/claude", "CODEX_HOME": f"{mount}/codex"},
            volumes=[VolumeMount(volume_id=auth_volume.id, mount_path=mount)],
        ),
        timeout=120,
    )
    response = sandbox.process.exec(
        f"mkdir -p {mount}/claude {mount}/codex && chmod 700 {mount}/claude {mount}/codex",
        timeout=30,
    )
    if response.exit_code != 0:
        sandbox.delete()
        raise RuntimeError("could not initialize OAuth volume directories")
    return {
        "status": "READY_FOR_INTERACTIVE_LOGIN",
        "sandbox_id": sandbox.id,
        "expires_after_minutes": 120,
        "commands": ["claude auth login", "codex login"],
        "note": "Run the commands in this sandbox's terminal, then delete the sandbox; the auth volume persists.",
    }


def run_daytona(
    request: dict[str, Any],
    *,
    archive: Path,
    output: Path,
    keep_on_failure: bool,
) -> dict[str, Any]:
    require_committed_controller(request, SCRIPT_DIR.parent.parent)
    if request["tier"] != "core":
        raise RuntimeError("the credentialed worker currently accepts only the core tier")
    approved_artifact = request["artifact"]
    if not approved_artifact.get("sha256"):
        raise RuntimeError("the approved request must include an artifact checksum")
    if sha256_file(archive) != approved_artifact["sha256"]:
        raise RuntimeError("local archive checksum does not match the approved request")
    if not os.environ.get("DAYTONA_API_KEY"):
        raise RuntimeError("DAYTONA_API_KEY is not set")
    try:
        from daytona import CreateSandboxFromSnapshotParams, Daytona, VolumeMount
    except ImportError as error:
        raise RuntimeError(
            "Daytona SDK is missing; install scripts/canary/requirements-daytona.txt in an isolated venv"
        ) from error
    execution = request["execution"]
    daytona = Daytona()
    auth_volume = daytona.volume.get(execution["auth_volume"], create=False)
    params = CreateSandboxFromSnapshotParams(
        language="python",
        snapshot=execution["snapshot"],
        ephemeral=True,
        ttl_minutes=max(1, (request["budget"]["wall_seconds"] + 59) // 60 + 10),
        labels={"purpose": "holler-canary", "request": request["request_hash"][-12:]},
        domain_allow_list=(
            "api.anthropic.com,*.anthropic.com,claude.ai,*.claude.ai,"
            "api.openai.com,*.openai.com,chatgpt.com,*.chatgpt.com"
        ),
        env_vars={
            "CLAUDE_CONFIG_DIR": "/home/daytona/.holler-canary-auth/claude",
            "CODEX_HOME": "/home/daytona/.holler-canary-auth/codex",
        },
        volumes=[
            VolumeMount(
                volume_id=auth_volume.id,
                mount_path="/home/daytona/.holler-canary-auth",
            )
        ],
    )
    sandbox = daytona.create(params, timeout=120)
    succeeded = False
    try:
        with tempfile.TemporaryDirectory(prefix="holler-canary-controller-") as directory:
            temporary = Path(directory)
            request_path = temporary / "request.json"
            request_path.write_text(json.dumps(request, sort_keys=True) + "\n", encoding="utf-8")
            runtime_path = temporary / "runtime.tar.gz"
            make_runtime_bundle(runtime_path)
            remote_archive = f"/tmp/{archive.name}"
            sandbox.fs.upload_file(str(archive), remote_archive)
            sandbox.fs.upload_file(str(request_path), "/tmp/holler-canary-request.json")
            sandbox.fs.upload_file(str(runtime_path), "/tmp/holler-canary-runtime.tar.gz")
            command = " && ".join(
                [
                    "mkdir -p /tmp/holler-canary-runtime",
                    "tar -xzf /tmp/holler-canary-runtime.tar.gz -C /tmp/holler-canary-runtime",
                    "python3 /tmp/holler-canary-runtime/holler-canary/worker.py "
                    "/tmp/holler-canary-request.json "
                    f"{shlex.quote(remote_archive)} --output /tmp/holler-canary-evidence.json",
                ]
            )
            response = sandbox.process.exec(command, timeout=request["budget"]["wall_seconds"])
            if response.exit_code != 0:
                raise RuntimeError(f"credentialed canary worker exited {response.exit_code}")
            output.parent.mkdir(parents=True, exist_ok=True)
            sandbox.fs.download_file("/tmp/holler-canary-evidence.json", str(output))
            evidence = json.loads(output.read_text(encoding="utf-8"))
            if evidence.get("request_hash") != request["request_hash"] or evidence.get("status") != "PASS":
                raise RuntimeError("downloaded evidence is not a passing result for the approved request")
            succeeded = True
            return {"sandbox_id": sandbox.id, "evidence": str(output), "status": "PASS"}
    finally:
        if succeeded or not keep_on_failure:
            sandbox.delete()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    plan_parser = subparsers.add_parser("plan", help="render a zero-mutation execution plan")
    plan_parser.add_argument("request", type=Path)
    plan_parser.add_argument("--allow-model-override", action="store_true")

    probe_parser = subparsers.add_parser("probe", help="create a minimal sandbox and prove command execution")
    probe_parser.add_argument("--execute", action="store_true", help="required because this creates a billable resource")
    probe_parser.add_argument("--keep", action="store_true", help="leave the probe sandbox running")

    run_parser = subparsers.add_parser("run", help="execute an approved core request in Daytona")
    run_parser.add_argument("request", type=Path)
    run_parser.add_argument("--artifact", type=Path, required=True)
    run_parser.add_argument("--output", type=Path, required=True)
    run_parser.add_argument("--execute", action="store_true", help="required because this spends sandbox and model quota")
    run_parser.add_argument("--keep-on-failure", action="store_true")

    build_parser = subparsers.add_parser("build", help="build the committed tree in an uncredentialed sandbox")
    build_parser.add_argument("request", type=Path)
    build_parser.add_argument("--repo", type=Path, default=SCRIPT_DIR.parent.parent)
    build_parser.add_argument("--output", type=Path, required=True)
    build_parser.add_argument("--execute", action="store_true")

    bootstrap_parser = subparsers.add_parser("bootstrap", help="create the pinned client snapshot and empty auth volume")
    bootstrap_parser.add_argument("request", type=Path)
    bootstrap_parser.add_argument("--execute", action="store_true")

    auth_parser = subparsers.add_parser("auth-sandbox", help="create a temporary sandbox for interactive OAuth login")
    auth_parser.add_argument("request", type=Path)
    auth_parser.add_argument("--execute", action="store_true")

    args = parser.parse_args()
    if args.command == "plan":
        request = load_request(args.request, allow_model_override=args.allow_model_override)
        print(json.dumps(execution_plan(request), indent=2, sort_keys=True))
        return
    if args.command == "run":
        if not args.execute:
            raise SystemExit("run is non-mutating unless --execute is supplied")
        request = load_request(args.request)
        print(
            json.dumps(
                run_daytona(
                    request,
                    archive=args.artifact.resolve(),
                    output=args.output,
                    keep_on_failure=args.keep_on_failure,
                ),
                indent=2,
                sort_keys=True,
            )
        )
        return
    if args.command == "build":
        if not args.execute:
            raise SystemExit("build is non-mutating unless --execute is supplied")
        request = load_request(args.request)
        print(json.dumps(build_daytona(request, repo=args.repo, output=args.output), indent=2, sort_keys=True))
        return
    if args.command == "bootstrap":
        if not args.execute:
            raise SystemExit("bootstrap is non-mutating unless --execute is supplied")
        print(json.dumps(bootstrap_daytona(load_request(args.request)), indent=2, sort_keys=True))
        return
    if args.command == "auth-sandbox":
        if not args.execute:
            raise SystemExit("auth-sandbox is non-mutating unless --execute is supplied")
        print(json.dumps(create_auth_sandbox(load_request(args.request)), indent=2, sort_keys=True))
        return
    if not args.execute:
        raise SystemExit("probe is non-mutating unless --execute is supplied")
    print(json.dumps(probe_daytona(keep=args.keep), indent=2, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (ManifestError, RuntimeError) as error:
        raise SystemExit(str(error)) from error
