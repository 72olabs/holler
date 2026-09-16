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

from clients import MINIMUM_CLIENTS  # noqa: E402
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
BUILDER_DOMAINS = [
    "github.com",
    "*.github.com",
    "*.githubusercontent.com",
    "go.dev",
    "*.go.dev",
    "proxy.golang.org",
    "sum.golang.org",
    "storage.googleapis.com",
    "registry.npmjs.org",
]
AUTH_ROOT = "/home/daytona/.holler-canary-auth"
RUNNER_PURPOSE = "holler-canary-persistent-runner"
NETWORK_POLICY_LABEL = "holler-network-policy"
NETWORK_POLICY_SANDBOX = "sandbox-allowlist"
NETWORK_POLICY_ORGANIZATION = "organization-tier"
NETWORK_OVERRIDE_REJECTION = "Network access is restricted and cannot be overridden at the sandbox level"


def claude_fixture_state_source(*, config_path: str, fixture: str, version: str) -> str:
    """Build the credential-preserving mutation used inside the runner."""
    return "\n".join(
        [
            "import json, os, tempfile",
            "from pathlib import Path",
            f"path = Path({config_path!r})",
            f"fixture = {fixture!r}",
            "data = json.loads(path.read_text()) if path.exists() else {}",
            "data['theme'] = 'dark'",
            "data['hasCompletedOnboarding'] = True",
            f"data['lastOnboardingVersion'] = {version!r}",
            "data.setdefault('projects', {}).setdefault(fixture, {})['hasTrustDialogAccepted'] = True",
            "fd, temporary = tempfile.mkstemp(prefix='.claude.json.', dir=path.parent)",
            "try:",
            "    with os.fdopen(fd, 'w') as stream:",
            "        json.dump(data, stream, separators=(',', ':'))",
            "        stream.write('\\n')",
            "        stream.flush()",
            "        os.fsync(stream.fileno())",
            "    os.chmod(temporary, 0o600)",
            "    os.replace(temporary, path)",
            "finally:",
            "    if os.path.exists(temporary): os.unlink(temporary)",
        ]
    )


def install_claude_fixture_state(sandbox: Any, execution: dict[str, Any], version: str) -> None:
    """Seed only non-secret Claude UI state for the dedicated cleanroom fixture."""
    source = claude_fixture_state_source(
        config_path=AUTH_ROOT + "/claude/.claude.json",
        fixture=execution["runner_fixture"],
        version=version,
    )
    response = sandbox.process.exec(f"python3 -c {shlex.quote(source)}", timeout=30)
    if response.exit_code != 0:
        raise RuntimeError("could not install Claude's non-secret cleanroom fixture state")


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
                "fixture": execution["runner_fixture"],
                "persistent": True,
                "auto_stop_minutes": execution["runner_auto_stop_minutes"],
            },
        },
        "network_policy": {
            "credentialed_sandbox_enforced": True,
            "requested_builder_domains": BUILDER_DOMAINS,
            "requested_canary_domains": CREDENTIAL_DOMAINS,
            "enforcement": (
                "sandbox allowlist where supported; otherwise Daytona's mandatory organization-tier restriction"
            ),
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


def make_runtime_bundle(output: Path, *, scenario_ids: set[str] | None = None) -> None:
    included = [
        "__init__.py",
        "budget.py",
        "catalog.py",
        "clients.py",
        "handler_contract.py",
        "manifest.py",
        "managed.py",
        "worker.py",
    ]
    with tarfile.open(output, "w:gz") as bundle:
        for name in included:
            bundle.add(SCRIPT_DIR / name, arcname=f"holler-canary/{name}")
        for scenario in sorted((SCRIPT_DIR / "scenarios").glob("C*.json")):
            bundle.add(scenario, arcname=f"holler-canary/scenarios/{scenario.name}")
        for handler in sorted((SCRIPT_DIR / "handlers").glob("C*.py")):
            if scenario_ids is None or handler.stem in scenario_ids:
                bundle.add(handler, arcname=f"holler-canary/handlers/{handler.name}")


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


def create_with_network_policy(
    daytona: Any,
    params_type: Any,
    *,
    params: dict[str, Any],
    domains: list[str],
    timeout: int,
    bad_request_type: type[Exception],
) -> tuple[Any, str]:
    """Use a sandbox allowlist when supported, or mandatory tier policy otherwise."""
    sandbox_params = dict(params)
    sandbox_labels = dict(sandbox_params.get("labels", {}))
    sandbox_labels[NETWORK_POLICY_LABEL] = NETWORK_POLICY_SANDBOX
    sandbox_params["labels"] = sandbox_labels
    sandbox_params["domain_allow_list"] = ",".join(domains)
    try:
        return daytona.create(params_type(**sandbox_params), timeout=timeout), NETWORK_POLICY_SANDBOX
    except bad_request_type as error:
        if NETWORK_OVERRIDE_REJECTION not in str(error):
            raise RuntimeError("Daytona rejected the requested sandbox network policy") from error
    organization_params = dict(params)
    organization_labels = dict(organization_params.get("labels", {}))
    organization_labels[NETWORK_POLICY_LABEL] = NETWORK_POLICY_ORGANIZATION
    organization_params["labels"] = organization_labels
    return (
        daytona.create(params_type(**organization_params), timeout=timeout),
        NETWORK_POLICY_ORGANIZATION,
    )


def build_daytona(request: dict[str, Any], *, repo: Path, output: Path) -> dict[str, Any]:
    require_committed_controller(request, repo)
    require_daytona_key()
    try:
        from daytona import CreateSandboxFromSnapshotParams, Daytona
        from daytona.common.errors import DaytonaBadRequestError, DaytonaNotFoundError
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
        sandbox, network_policy = create_with_network_policy(
            daytona,
            CreateSandboxFromSnapshotParams,
            params={
                "language": "python",
                "snapshot": execution["snapshot"],
                "ephemeral": True,
                "ttl_minutes": 60,
                "labels": {"purpose": "holler-canary-builder", "commit": source["commit"][:12]},
            },
            domains=BUILDER_DOMAINS,
            timeout=120,
            bad_request_type=DaytonaBadRequestError,
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
                    "if test -x ./scripts/ci/run.sh; then "
                    f"HOLLER_COMMIT={shlex.quote(source['commit'])} HOLLER_DIRTY=false "
                    f"HOLLER_VERSION={shlex.quote(source['connector_version'])} "
                    "ARTIFACT_ROOT=/tmp/holler-dist HOLLER_CI_HOME_AUDIT=off ./scripts/ci/run.sh; "
                    "elif test -x ./scripts/package-release.sh; then "
                    f"HOLLER_COMMIT={shlex.quote(source['commit'])} HOLLER_DIRTY=false "
                    f"HOLLER_VERSION={shlex.quote(source['connector_version'])} "
                    "ARTIFACT_ROOT=/tmp/holler-dist ./scripts/package-release.sh; "
                    "else echo 'no supported release build entrypoint' >&2; exit 127; fi",
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
    return {
        "status": "PASS",
        "artifact": str(output),
        "sha256": sha256_file(output),
        "network_policy": network_policy,
    }


def build_client_bundle(request: dict[str, Any], *, output: Path) -> dict[str, Any]:
    """Build the minimum-version client fixture without exposing OAuth state."""
    require_committed_controller(request, SCRIPT_DIR.parent.parent)
    require_daytona_key()
    try:
        from daytona import CreateSandboxFromSnapshotParams, Daytona
        from daytona.common.errors import DaytonaBadRequestError, DaytonaNotFoundError
    except ImportError as error:
        raise RuntimeError(
            "Daytona SDK is missing; install scripts/canary/requirements-daytona.txt in an isolated venv"
        ) from error
    execution = request["execution"]
    daytona = Daytona()
    try:
        runner = daytona.get(execution["runner_name"])
    except DaytonaNotFoundError:
        runner = None
    if runner is not None:
        validate_runner(runner, execution)
        if sandbox_state(runner) == "started":
            runner.stop(timeout=120)
    sandbox, network_policy = create_with_network_policy(
        daytona,
        CreateSandboxFromSnapshotParams,
        params={
            "language": "python",
            "snapshot": execution["snapshot"],
            "ephemeral": True,
            "ttl_minutes": 30,
            "labels": {"purpose": "holler-canary-client-bundle"},
        },
        domains=BUILDER_DOMAINS,
        timeout=120,
        bad_request_type=DaytonaBadRequestError,
    )
    try:
        claude = MINIMUM_CLIENTS["claude"]
        codex = MINIMUM_CLIENTS["codex"]
        command = " && ".join(
            [
                "umask 022",
                "mkdir -p /tmp/holler-client-matrix/claude /tmp/holler-client-matrix/codex",
                f"npm install --prefix /tmp/holler-client-matrix/claude "
                f"@anthropic-ai/claude-code@{shlex.quote(claude['version'])}",
                f"npm install --prefix /tmp/holler-client-matrix/codex "
                f"@openai/codex@{shlex.quote(codex['version'])}",
                f"test \"$(/tmp/holler-client-matrix/{claude['relative_binary']} --version | awk '{{print $1}}')\" "
                f"= {shlex.quote(claude['version'])}",
                f"test \"$(/tmp/holler-client-matrix/{codex['relative_binary']} --version | awk '{{print $NF}}')\" "
                f"= {shlex.quote(codex['version'])}",
                "tar -czf /tmp/holler-client-matrix.tar.gz -C /tmp holler-client-matrix",
            ]
        )
        response = sandbox.process.exec(command, timeout=600)
        if response.exit_code != 0:
            detail = (response.result or "").strip()[-4000:]
            raise RuntimeError(
                f"minimum-client bundle builder exited {response.exit_code}"
                + (f":\n{detail}" if detail else "")
            )
        output.parent.mkdir(parents=True, exist_ok=True)
        sandbox.fs.download_file("/tmp/holler-client-matrix.tar.gz", str(output))
    finally:
        sandbox.delete()
    return {
        "status": "PASS",
        "artifact": str(output),
        "sha256": sha256_file(output),
        "network_policy": network_policy,
    }


def validate_fixture(request: dict[str, Any], name: str, path: Path | None) -> Path | None:
    approved = request["fixtures"][name]
    if approved.get("required") and path is None:
        raise RuntimeError(f"the approved request requires --{name.replace('_', '-')}")
    if path is None:
        return None
    path = path.resolve()
    if not approved.get("sha256"):
        raise RuntimeError(f"the approved request must include a {name} checksum")
    if path.name != approved.get("filename") or path.stat().st_size != approved.get("bytes"):
        raise RuntimeError(f"local {name} metadata does not match the approved request")
    if sha256_file(path) != approved["sha256"]:
        raise RuntimeError(f"local {name} checksum does not match the approved request")
    return path


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
    policy = sandbox.labels.get(NETWORK_POLICY_LABEL, "")
    if policy == NETWORK_POLICY_ORGANIZATION:
        if sandbox.domain_allow_list not in (None, ""):
            raise RuntimeError("organization-tier runner unexpectedly has a sandbox domain allowlist")
    elif sandbox.domain_allow_list != ",".join(CREDENTIAL_DOMAINS):
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
        from daytona.common.errors import DaytonaBadRequestError, DaytonaNotFoundError
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
        sandbox, _ = create_with_network_policy(
            daytona,
            CreateSandboxFromSnapshotParams,
            params={
                "name": execution["runner_name"],
                "language": "python",
                "snapshot": execution["snapshot"],
                "auto_stop_interval": execution["runner_auto_stop_minutes"],
                "labels": {"purpose": RUNNER_PURPOSE},
                "env_vars": {
                    "CLAUDE_CONFIG_DIR": f"{AUTH_ROOT}/claude",
                    "CODEX_HOME": f"{AUTH_ROOT}/codex",
                },
            },
            domains=CREDENTIAL_DOMAINS,
            timeout=120,
            bad_request_type=DaytonaBadRequestError,
        )
    validate_runner(sandbox, execution)
    start_runner(sandbox)
    fixture = shlex.quote(execution["runner_fixture"])
    response = sandbox.process.exec(
        f"umask 077 && mkdir -p {AUTH_ROOT}/claude {AUTH_ROOT}/codex && "
        f"chmod 700 {AUTH_ROOT} {AUTH_ROOT}/claude {AUTH_ROOT}/codex && "
        f"test -w {AUTH_ROOT}/claude && test -w {AUTH_ROOT}/codex && "
        f"mkdir -p {fixture} && chmod 700 {fixture} && "
        f"if ! test -d {fixture}/.git; then "
        f"git -C {fixture} init -q && "
        f"git -C {fixture} -c user.name='Holler Canary' -c user.email=canary@invalid "
        f"commit --allow-empty -qm fixture; fi && "
        f"test -d {fixture}/.git",
        timeout=30,
    )
    if response.exit_code != 0:
        detail = (response.result or "").strip()[-4000:]
        raise RuntimeError(
            "could not initialize writable OAuth directories on the persistent runner"
            + (f":\n{detail}" if detail else "")
        )
    install_claude_fixture_state(sandbox, execution, request["clients"]["claude"]["version"])
    return {
        "status": "READY_FOR_INTERACTIVE_LOGIN",
        "sandbox_id": sandbox.id,
        "sandbox_name": sandbox.name,
        "persistent_filesystem": True,
        "reused": reused,
        "auto_stop_minutes": execution["runner_auto_stop_minutes"],
        "commands": [
            "claude auth login",
            "codex login",
        ],
        "note": (
            "Log in once. The runner idempotently installs only Claude's non-secret theme and "
            "project-trust state for the dedicated empty fixture; stop/start preserves OAuth."
        ),
    }


def inspect_auth_runner(request: dict[str, Any]) -> dict[str, Any]:
    """Verify runner policy and both OAuth sessions without exposing account data."""
    require_committed_controller(request, SCRIPT_DIR.parent.parent)
    require_daytona_key()
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
    except DaytonaNotFoundError:
        return {
            "status": "ACTION_REQUIRED",
            "runner": "missing",
            "operator_action": "create the persistent runner and complete both interactive logins",
        }
    validate_runner(sandbox, execution)
    originally_started = sandbox_state(sandbox) == "started"
    start_runner(sandbox)
    try:
        auth = {}
        for client, command in (
            ("claude", "claude auth status >/dev/null 2>&1"),
            ("codex", "codex login status >/dev/null 2>&1"),
        ):
            response = sandbox.process.exec(command, timeout=60)
            auth[client] = "READY" if response.exit_code == 0 else "LOGIN_REQUIRED"
    finally:
        if not originally_started:
            sandbox.stop(timeout=120)
    ready = all(value == "READY" for value in auth.values())
    result: dict[str, Any] = {
        "status": "READY" if ready else "ACTION_REQUIRED",
        "runner": execution["runner_name"],
        "runner_policy": "PASS",
        "oauth": auth,
        "restored_initial_power_state": True,
    }
    if not ready:
        result["operator_action"] = "open the persistent runner and log in the clients marked LOGIN_REQUIRED"
    return result


def run_daytona(
    request: dict[str, Any],
    *,
    archive: Path,
    output: Path,
    keep_on_failure: bool,
    upgrade_from: Path | None = None,
    client_bundle: Path | None = None,
) -> dict[str, Any]:
    require_committed_controller(request, SCRIPT_DIR.parent.parent)
    approved_artifact = request["artifact"]
    if not approved_artifact.get("sha256"):
        raise RuntimeError("the approved request must include an artifact checksum")
    if sha256_file(archive) != approved_artifact["sha256"]:
        raise RuntimeError("local archive checksum does not match the approved request")
    upgrade_from = validate_fixture(request, "upgrade_from", upgrade_from)
    client_bundle = validate_fixture(request, "client_bundle", client_bundle)
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
        install_claude_fixture_state(sandbox, execution, request["clients"]["claude"]["version"])
        with tempfile.TemporaryDirectory(prefix="holler-canary-controller-") as directory:
            temporary = Path(directory)
            request_path = temporary / "request.json"
            request_path.write_text(json.dumps(request, sort_keys=True) + "\n", encoding="utf-8")
            runtime_path = temporary / "runtime.tar.gz"
            make_runtime_bundle(
                runtime_path,
                scenario_ids={item["id"] for item in request["scenarios"]},
            )
            prepare = sandbox.process.exec(
                f"umask 077 && mkdir {shlex.quote(run_root)} && chmod 700 {shlex.quote(run_root)}",
                timeout=30,
            )
            if prepare.exit_code != 0:
                raise RuntimeError("could not create an isolated per-run directory")
            remote_archive = f"{run_root}/{archive.name}"
            sandbox.fs.upload_file(str(archive), remote_archive)
            worker_args = ""
            if upgrade_from is not None:
                remote_upgrade = f"{run_root}/{upgrade_from.name}"
                sandbox.fs.upload_file(str(upgrade_from), remote_upgrade)
                worker_args += f" --upgrade-from {shlex.quote(remote_upgrade)}"
            if client_bundle is not None:
                remote_clients = f"{run_root}/{client_bundle.name}"
                sandbox.fs.upload_file(str(client_bundle), remote_clients)
                worker_args += f" --client-bundle {shlex.quote(remote_clients)}"
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
                    f"--output {shlex.quote(run_root + '/evidence.json')}{worker_args}",
                ]
            )
            output.parent.mkdir(parents=True, exist_ok=True)
            output.unlink(missing_ok=True)
            response = sandbox.process.exec(command, timeout=request["budget"]["wall_seconds"])
            if response.exit_code != 0:
                failure_location = ""
                try:
                    sandbox.fs.download_file(f"{run_root}/evidence.json", str(output))
                    failure_evidence = json.loads(output.read_text(encoding="utf-8"))
                    allowed_scenarios = {"initialization", *(item["id"] for item in request["scenarios"])}
                    failed_scenario = failure_evidence.get("failure", {}).get("scenario")
                    if (
                        failure_evidence.get("request_hash") == request["request_hash"]
                        and failure_evidence.get("status") == "FAIL"
                        and failed_scenario in allowed_scenarios
                    ):
                        failure_location = f" in {failed_scenario}; body-free evidence: {output}"
                except Exception:
                    output.unlink(missing_ok=True)
                raise RuntimeError(
                    f"credentialed canary worker exited {response.exit_code}{failure_location}"
                )
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
    run_parser.add_argument("--upgrade-from", type=Path)
    run_parser.add_argument("--client-bundle", type=Path)

    build_parser = subparsers.add_parser("build", help="build the committed tree in an uncredentialed sandbox")
    build_parser.add_argument("request", type=Path)
    build_parser.add_argument("--repo", type=Path, default=SCRIPT_DIR.parent.parent)
    build_parser.add_argument("--output", type=Path, required=True)
    build_parser.add_argument("--execute", action="store_true")

    bundle_parser = subparsers.add_parser(
        "client-bundle", help="build the minimum-version client fixture in an uncredentialed sandbox"
    )
    bundle_parser.add_argument("request", type=Path)
    bundle_parser.add_argument("--output", type=Path, required=True)
    bundle_parser.add_argument("--execute", action="store_true")

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
                    upgrade_from=args.upgrade_from,
                    client_bundle=args.client_bundle,
                ),
                indent=2,
                sort_keys=True,
            )
        )
        return
    if args.command == "client-bundle":
        if not args.execute:
            raise SystemExit("client-bundle is non-mutating unless --execute is supplied")
        request = load_request(args.request)
        print(json.dumps(build_client_bundle(request, output=args.output), indent=2, sort_keys=True))
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
