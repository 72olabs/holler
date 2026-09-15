#!/usr/bin/env python3
"""Immutable canary request construction and validation."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import subprocess
from typing import Any

from budget import budget_for_tier, validate_estimate
from catalog import load_catalog, scenarios_for_tier
from clients import MINIMUM_CLIENTS, assert_low_cost_defaults, client_policy


SCHEMA_VERSION = 2
HASH_PREFIX = "sha256:"
GO_TOOLCHAINS = {
    "1.26": {
        "version": "1.26.0",
        "filename": "go1.26.0.linux-amd64.tar.gz",
        "sha256": "aac1b08a0fb0c4e0a7c1555beb7b59180b05dfc5a3d62e40e9de90cd42f88235",
    },
}
SECRET_KEY_PATTERN = re.compile(
    r"(^|_)(secret|password|api_?key|access_token|refresh_token|credential_value|authorization)($|_)",
    re.I,
)


class ManifestError(ValueError):
    """A canary request is invalid or has been changed after approval."""


def canonical_json(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode(
        "utf-8"
    )


def sha256_bytes(value: bytes) -> str:
    return HASH_PREFIX + hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return HASH_PREFIX + digest.hexdigest()


def git(repo: Path, *args: str) -> str:
    try:
        result = subprocess.run(
            ["git", "-C", str(repo), *args],
            check=True,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise ManifestError(f"git {' '.join(args)} failed: {error}") from error
    return result.stdout.strip()


def repository_name(remote: str) -> str:
    cleaned = remote.rstrip("/")
    if ":" in cleaned and "/" not in cleaned.split(":", 1)[0]:
        cleaned = cleaned.split(":", 1)[1]
    name = cleaned.rsplit("/", 1)[-1]
    return name[:-4] if name.endswith(".git") else name


def connector_version(repo: Path, ref: str) -> str:
    raw = git(repo, "show", f"{ref}:internal/connector/manifest.go")
    match = re.search(r'const ConnectorVersion = "([^"]+)"', raw)
    if not match:
        raise ManifestError("cannot determine ConnectorVersion at requested ref")
    return match.group(1)


def go_toolchain(repo: Path, ref: str) -> dict[str, str]:
    raw = git(repo, "show", f"{ref}:go.mod")
    match = re.search(r"^go\s+([^\s]+)\s*$", raw, re.MULTILINE)
    if not match:
        raise ManifestError("cannot determine Go version at requested ref")
    requested = match.group(1)
    try:
        return dict(GO_TOOLCHAINS[requested])
    except KeyError as error:
        raise ManifestError(f"unsupported Daytona Go toolchain: {requested}") from error


def create_request(
    repo: Path,
    *,
    ref: str,
    tier: str,
    clients: dict[str, dict[str, Any]] | None = None,
    artifact: Path | None = None,
    upgrade_from: Path | None = None,
    client_bundle: Path | None = None,
    snapshot: str | None = None,
    runner_name: str = "holler-canary-runner",
) -> dict[str, Any]:
    repo = repo.resolve()
    commit = git(repo, "rev-parse", f"{ref}^{{commit}}")
    tree = git(repo, "rev-parse", f"{commit}^{{tree}}")
    remote = git(repo, "remote", "get-url", "origin")
    catalog = load_catalog()
    scenarios = scenarios_for_tier(tier, catalog)
    limits = budget_for_tier(tier)
    validate_estimate(scenarios, limits)
    selected_clients = clients or client_policy()
    selected_go = go_toolchain(repo, commit)
    if snapshot is None:
        go_pin = re.sub(r"[^a-zA-Z0-9]+", "-", selected_go["version"]).strip("-")
        claude_pin = re.sub(r"[^a-zA-Z0-9]+", "-", str(selected_clients["claude"]["version"])).strip("-")
        codex_pin = re.sub(r"[^a-zA-Z0-9]+", "-", str(selected_clients["codex"]["version"])).strip("-")
        snapshot = f"holler-canary-go-{go_pin}-claude-{claude_pin}-codex-{codex_pin}"
    artifact_record = file_record(artifact, required=tier != "preflight")
    scenario_ids = {scenario["id"] for scenario in scenarios}
    fixtures = {
        "upgrade_from": {
            **file_record(upgrade_from, required="C6" in scenario_ids),
            "connector_version": "0.7.1",
            "schema_version": 14,
        },
        "client_bundle": {
            **file_record(client_bundle, required="C8" in scenario_ids),
            "clients": MINIMUM_CLIENTS,
        },
    }
    scenario_records = []
    for scenario in scenarios:
        record = dict(scenario)
        record["definition_hash"] = sha256_bytes(canonical_json(scenario))
        scenario_records.append(record)
    request: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "kind": "holler-real-client-canary",
        "source": {
            "repository": repository_name(remote),
            "commit": commit,
            "tree": tree,
            "connector_version": connector_version(repo, commit),
        },
        "tier": tier,
        "scenarios": scenario_records,
        "clients": selected_clients,
        "budget": limits,
        "artifact": artifact_record,
        "fixtures": fixtures,
        "execution": {
            "provider": "daytona",
            "snapshot": snapshot,
            "go_toolchain": selected_go,
            "runner_name": runner_name,
            "runner_persistent": True,
            "runner_auto_stop_minutes": 15,
            "runner_fixture": "/home/daytona/.holler-canary-workspace",
            "source_checkout_in_credential_sandbox": False,
            "evidence_contains_message_bodies": False,
        },
    }
    request["request_hash"] = request_hash(request)
    validate_request(request, allow_model_override=True)
    return request


def request_hash(request: dict[str, Any]) -> str:
    unsigned = {key: value for key, value in request.items() if key != "request_hash"}
    return sha256_bytes(canonical_json(unsigned))


def validate_request(request: object, *, allow_model_override: bool = False) -> dict[str, Any]:
    if not isinstance(request, dict):
        raise ManifestError("request must be a JSON object")
    if request.get("schema_version") != SCHEMA_VERSION or request.get("kind") != "holler-real-client-canary":
        raise ManifestError("unsupported canary request")
    supplied_hash = request.get("request_hash")
    if not isinstance(supplied_hash, str) or supplied_hash != request_hash(request):
        raise ManifestError("request hash does not match the request contents")
    for required in ("source", "tier", "scenarios", "clients", "budget", "artifact", "fixtures", "execution"):
        if required not in request:
            raise ManifestError(f"request is missing {required}")
    _reject_embedded_secrets(request)
    scenarios = request["scenarios"]
    if not isinstance(scenarios, list) or not scenarios:
        raise ManifestError("request has no scenarios")
    for scenario in scenarios:
        if not isinstance(scenario, dict):
            raise ManifestError("scenario records must be objects")
        expected = scenario.get("definition_hash")
        unsigned = {key: value for key, value in scenario.items() if key != "definition_hash"}
        if expected != sha256_bytes(canonical_json(unsigned)):
            raise ManifestError(f"scenario {scenario.get('id', '?')} definition hash does not match")
    validate_estimate(scenarios, request["budget"])
    if not allow_model_override:
        try:
            assert_low_cost_defaults(request["clients"])
        except ValueError as error:
            raise ManifestError(str(error)) from error
    execution = request["execution"]
    if execution.get("go_toolchain") not in GO_TOOLCHAINS.values():
        raise ManifestError("request uses an unsupported Daytona Go toolchain")
    if not isinstance(execution.get("runner_name"), str) or not execution["runner_name"].strip():
        raise ManifestError("request must name a persistent Daytona runner")
    if execution.get("runner_persistent") is not True:
        raise ManifestError("credentialed Daytona runner must be persistent")
    if execution.get("runner_fixture") != "/home/daytona/.holler-canary-workspace":
        raise ManifestError("credentialed Daytona runner must use the dedicated stable fixture")
    if execution.get("source_checkout_in_credential_sandbox") is not False:
        raise ManifestError("credentialed canaries must not receive a source checkout")
    if execution.get("evidence_contains_message_bodies") is not False:
        raise ManifestError("canary evidence must remain body-free")
    fixtures = request["fixtures"]
    if not isinstance(fixtures, dict):
        raise ManifestError("request fixtures must be an object")
    scenario_ids = {scenario["id"] for scenario in scenarios}
    for name, scenario_id in (("upgrade_from", "C6"), ("client_bundle", "C8")):
        fixture = fixtures.get(name)
        if not isinstance(fixture, dict):
            raise ManifestError(f"request fixture {name} must be an object")
        if fixture.get("required") is not (scenario_id in scenario_ids):
            raise ManifestError(f"request fixture {name} requirement does not match scenarios")
    return request


def file_record(path: Path | None, *, required: bool) -> dict[str, Any]:
    record: dict[str, Any] = {"required": required}
    if path is None:
        return record
    path = path.resolve()
    if not path.is_file():
        raise ManifestError(f"fixture does not exist: {path}")
    record.update({"filename": path.name, "bytes": path.stat().st_size, "sha256": sha256_file(path)})
    return record


def _reject_embedded_secrets(value: object, path: str = "request") -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}"
            if SECRET_KEY_PATTERN.search(str(key)):
                raise ManifestError(f"secret-like field is forbidden in manifests: {child_path}")
            _reject_embedded_secrets(child, child_path)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _reject_embedded_secrets(child, f"{path}[{index}]")


def load_request(path: Path, *, allow_model_override: bool = False) -> dict[str, Any]:
    try:
        request = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ManifestError(f"cannot read request {path}: {error}") from error
    return validate_request(request, allow_model_override=allow_model_override)
