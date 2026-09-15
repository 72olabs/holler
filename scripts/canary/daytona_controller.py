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


CREDENTIAL_DOMAINS = [
    "api.anthropic.com",
    "*.anthropic.com",
    "claude.ai",
    "*.claude.ai",
    "claude.com",
    "*.claude.com",
    "api.openai.com",
    "*.openai.com",
    "chatgpt.com",
    "*.chatgpt.com",
]
AUTH_ROOT = "/home/daytona/.holler-canary-auth"
RUNNER_PURPOSE = "holler-canary-persistent-runner"


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
                "runner": execution["runner_name"],
                "snapshot": execution["snapshot"],
                "persistent": True,
                "auto_stop_minutes": execution["runner_auto_stop_minutes"],
            },
        },
        "network_policy": {
            "credentialed_sandbox_enforced": True,
            "builder": ["source-host", "go-modules"],
            "canary": CREDENTIAL_DOMAINS,
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
            "remove per-run files from the credentialed runner",
            "stop the credentialed runner while retaining its OAuth filesystem",
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
        from daytona.common.errors import DaytonaNotFoundError
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
        try:
            credentialed_runner = daytona.get(execution["runner_name"])
        except DaytonaNotFoundError:
            credentialed_runner = None
        if credentialed_runner is not None:
            validate_runner(credentialed_runner, execution)
            if sandbox_state(credentialed_runner) == "started":
                credentialed_runner.stop(timeout=120)
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
                    "git init -q",
                    "git remote add origin https://github.com/72olabs/holler.git",
                    "git add -A",
                    "git -c user.name='Holler Canary Builder' "
                    "-c user.email=canary-builder@invalid commit -qm imported-source",
                    f"HOLLER_COMMIT={shlex.quote(source['commit'])} HOLLER_DIRTY=false "
                    f"HOLLER_VERSION={shlex.quote(source['connector_version'])} "
                    "ARTIFACT_ROOT=/tmp/holler-dist HOLLER_CI_HOME_AUDIT=off ./scripts/ci/run.sh",
                ]
            )
            response = sandbox.process.exec(command, timeout=2400)
            if response.exit_code != 0:
                detail = (response.result or "").strip()[-4000:]
                raise RuntimeError(
                    f"uncredentialed Daytona builder exited {response.exit_code}"
                    + (f":\n{detail}" if detail else "")
                )
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
        client_prefix = "/home/daytona/.holler-canary-tools"
        install = (
            f"npm install --global --prefix {shlex.quote(client_prefix)} "
            + " ".join(shlex.quote(value) for value in packages)
        )
        go_archive = f"/tmp/{go_toolchain['filename']}"
        command = "set -x && " + " && ".join(
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
                "mkdir -p /home/daytona/.local/bin",
                f"ln -sfn {shlex.quote(client_prefix + '/bin/claude')} "
                "/home/daytona/.local/bin/claude",
                f"ln -sfn {shlex.quote(client_prefix + '/bin/codex')} "
                "/home/daytona/.local/bin/codex",
                f"sudo ln -sfn {shlex.quote(client_prefix + '/bin/claude')} /usr/local/bin/claude",
                f"sudo ln -sfn {shlex.quote(client_prefix + '/bin/codex')} /usr/local/bin/codex",
                f"sudo ln -sfn {shlex.quote(client_prefix + '/bin/claude')} "
                "/usr/local/share/nvm/current/bin/claude",
                f"sudo ln -sfn {shlex.quote(client_prefix + '/bin/codex')} "
                "/usr/local/share/nvm/current/bin/codex",
                "test \"$(command -v claude)\" = /usr/local/share/nvm/current/bin/claude",
                "test \"$(command -v codex)\" = /usr/local/share/nvm/current/bin/codex",
                f"test \"$(claude --version | awk '{{print $1}}')\" = "
                f"{shlex.quote(clients['claude']['version'])}",
                f"test \"$(codex --version | awk '{{print $NF}}')\" = "
                f"{shlex.quote(clients['codex']['version'])}",
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
        "credentials_in_snapshot": False,
    }


def sandbox_state(sandbox: Any) -> str:
    state = getattr(sandbox, "state", "")
    return str(getattr(state, "value", state)).lower()


def validate_runner(sandbox: Any, execution: dict[str, Any]) -> None:
    if sandbox.name != execution["runner_name"]:
        raise RuntimeError("Daytona returned the wrong credentialed runner")
    if sandbox.labels.get("purpose") != RUNNER_PURPOSE:
        raise RuntimeError("refusing to use a runner without the Holler canary purpose label")
    if sandbox.snapshot != execution["snapshot"]:
        raise RuntimeError("credentialed runner snapshot does not match the approved request")
    if sandbox.auto_delete_interval != -1:
        raise RuntimeError("credentialed runner must have auto-delete disabled")
    if sandbox.auto_stop_interval != execution["runner_auto_stop_minutes"]:
        raise RuntimeError("credentialed runner auto-stop does not match the approved request")
    if sandbox.domain_allow_list != ",".join(CREDENTIAL_DOMAINS):
        raise RuntimeError("credentialed runner domain allowlist does not match the controller policy")
    expected_env = {
        "CLAUDE_CONFIG_DIR": f"{AUTH_ROOT}/claude",
        "CODEX_HOME": f"{AUTH_ROOT}/codex",
    }
    if sandbox.env != expected_env:
        raise RuntimeError("credentialed runner OAuth paths do not match the controller policy")
    if sandbox.volumes:
        raise RuntimeError("credentialed runner must not mount FUSE volumes")


def start_runner(sandbox: Any) -> None:
    if sandbox_state(sandbox) != "started":
        sandbox.start(timeout=120)


def create_auth_runner(request: dict[str, Any]) -> dict[str, Any]:
    require_committed_controller(request, SCRIPT_DIR.parent.parent)
    require_daytona_key()
    try:
        from daytona import CreateSandboxFromSnapshotParams, Daytona
        from daytona.common.errors import DaytonaNotFoundError
    except ImportError as error:
        raise RuntimeError(
            "Daytona SDK is missing; install scripts/canary/requirements-daytona.txt in an isolated venv"
        ) from error
    execution = request["execution"]
    daytona = Daytona()
    reused = True
    try:
        sandbox = daytona.get(execution["runner_name"])
    except DaytonaNotFoundError:
        reused = False
        sandbox = daytona.create(
            CreateSandboxFromSnapshotParams(
                name=execution["runner_name"],
                language="python",
                snapshot=execution["snapshot"],
                auto_stop_interval=execution["runner_auto_stop_minutes"],
                labels={"purpose": RUNNER_PURPOSE},
                domain_allow_list=",".join(CREDENTIAL_DOMAINS),
                env_vars={
                    "CLAUDE_CONFIG_DIR": f"{AUTH_ROOT}/claude",
                    "CODEX_HOME": f"{AUTH_ROOT}/codex",
                },
            ),
            timeout=120,
        )
    validate_runner(sandbox, execution)
    start_runner(sandbox)
    response = sandbox.process.exec(
        f"umask 077 && mkdir -p {AUTH_ROOT}/claude {AUTH_ROOT}/codex && "
        f"chmod 700 {AUTH_ROOT} {AUTH_ROOT}/claude {AUTH_ROOT}/codex && "
        f"test -w {AUTH_ROOT}/claude && test -w {AUTH_ROOT}/codex",
        timeout=30,
    )
    if response.exit_code != 0:
        detail = (response.result or "").strip()[-4000:]
        raise RuntimeError(
            "could not initialize writable OAuth directories on the persistent runner"
            + (f":\n{detail}" if detail else "")
        )
    return {
        "status": "READY_FOR_INTERACTIVE_LOGIN",
        "sandbox_id": sandbox.id,
        "sandbox_name": sandbox.name,
        "persistent_filesystem": True,
        "reused": reused,
        "auto_stop_minutes": execution["runner_auto_stop_minutes"],
        "commands": ["claude auth login", "codex login"],
        "note": "Log in once, then keep this runner; stop/start preserves OAuth state.",
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
        from daytona import Daytona
        from daytona.common.errors import DaytonaNotFoundError
    except ImportError as error:
        raise RuntimeError(
            "Daytona SDK is missing; install scripts/canary/requirements-daytona.txt in an isolated venv"
        ) from error
    execution = request["execution"]
    daytona = Daytona()
    try:
        sandbox = daytona.get(execution["runner_name"])
    except DaytonaNotFoundError as error:
        raise RuntimeError("persistent Daytona runner does not exist; run the runner command first") from error
    validate_runner(sandbox, execution)
    if sandbox_state(sandbox) == "started":
        sandbox.stop(timeout=120)
    start_runner(sandbox)
    succeeded = False
    run_root = f"/tmp/holler-canary-{request['request_hash'][-16:]}"
    try:
        for client, command in (
            ("Claude", "claude auth status >/dev/null 2>&1"),
            ("Codex", "codex login status >/dev/null 2>&1"),
        ):
            preflight = sandbox.process.exec(command, timeout=60)
            if preflight.exit_code != 0:
                raise RuntimeError(f"{client} is not authenticated in the persistent runner")
        with tempfile.TemporaryDirectory(prefix="holler-canary-controller-") as directory:
            temporary = Path(directory)
            request_path = temporary / "request.json"
            request_path.write_text(json.dumps(request, sort_keys=True) + "\n", encoding="utf-8")
            runtime_path = temporary / "runtime.tar.gz"
            make_runtime_bundle(runtime_path)
            prepare = sandbox.process.exec(
                f"umask 077 && mkdir {shlex.quote(run_root)} && chmod 700 {shlex.quote(run_root)}",
                timeout=30,
            )
            if prepare.exit_code != 0:
                raise RuntimeError("could not create an isolated per-run directory")
            remote_archive = f"{run_root}/{archive.name}"
            sandbox.fs.upload_file(str(archive), remote_archive)
            sandbox.fs.upload_file(str(request_path), f"{run_root}/request.json")
            sandbox.fs.upload_file(str(runtime_path), f"{run_root}/runtime.tar.gz")
            command = " && ".join(
                [
                    f"mkdir {shlex.quote(run_root + '/runtime')}",
                    f"tar -xzf {shlex.quote(run_root + '/runtime.tar.gz')} "
                    f"-C {shlex.quote(run_root + '/runtime')}",
                    f"CLAUDE_CONFIG_DIR={shlex.quote(AUTH_ROOT + '/claude')} "
                    f"CODEX_HOME={shlex.quote(AUTH_ROOT + '/codex')} "
                    f"python3 {shlex.quote(run_root + '/runtime/holler-canary/worker.py')} "
                    f"{shlex.quote(run_root + '/request.json')} {shlex.quote(remote_archive)} "
                    f"--output {shlex.quote(run_root + '/evidence.json')}",
                ]
            )
            response = sandbox.process.exec(command, timeout=request["budget"]["wall_seconds"])
            if response.exit_code != 0:
                raise RuntimeError(f"credentialed canary worker exited {response.exit_code}")
            output.parent.mkdir(parents=True, exist_ok=True)
            sandbox.fs.download_file(f"{run_root}/evidence.json", str(output))
            evidence = json.loads(output.read_text(encoding="utf-8"))
            if evidence.get("request_hash") != request["request_hash"] or evidence.get("status") != "PASS":
                raise RuntimeError("downloaded evidence is not a passing result for the approved request")
            succeeded = True
            return {
                "runner_id": sandbox.id,
                "runner_name": sandbox.name,
                "evidence": str(output),
                "status": "PASS",
            }
    finally:
        if succeeded or not keep_on_failure:
            sandbox.process.exec(f"rm -rf -- {shlex.quote(run_root)}", timeout=60)
            sandbox.stop(timeout=120)


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

    bootstrap_parser = subparsers.add_parser("bootstrap", help="create the pinned credential-free client snapshot")
    bootstrap_parser.add_argument("request", type=Path)
    bootstrap_parser.add_argument("--execute", action="store_true")

    runner_parser = subparsers.add_parser("runner", help="create or verify the persistent OAuth runner")
    runner_parser.add_argument("request", type=Path)
    runner_parser.add_argument("--execute", action="store_true")

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
    if args.command == "runner":
        if not args.execute:
            raise SystemExit("runner is non-mutating unless --execute is supplied")
        print(json.dumps(create_auth_runner(load_request(args.request)), indent=2, sort_keys=True))
        return
    if not args.execute:
        raise SystemExit("probe is non-mutating unless --execute is supplied")
    print(json.dumps(probe_daytona(keep=args.keep), indent=2, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (ManifestError, RuntimeError) as error:
        raise SystemExit(str(error)) from error
