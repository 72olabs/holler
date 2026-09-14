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
from clients import assert_low_cost_defaults, client_policy


SCHEMA_VERSION = 1
HASH_PREFIX = "sha256:"
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


def create_request(
    repo: Path,
    *,
    ref: str,
    tier: str,
    clients: dict[str, dict[str, Any]] | None = None,
    artifact: Path | None = None,
    snapshot: str | None = None,
    auth_volume: str = "holler-canary-auth",
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
    if snapshot is None:
        claude_pin = re.sub(r"[^a-zA-Z0-9]+", "-", str(selected_clients["claude"]["version"])).strip("-")
        codex_pin = re.sub(r"[^a-zA-Z0-9]+", "-", str(selected_clients["codex"]["version"])).strip("-")
        snapshot = f"holler-canary-claude-{claude_pin}-codex-{codex_pin}"
    artifact_record: dict[str, Any] = {"required": tier != "preflight"}
    if artifact is not None:
        artifact = artifact.resolve()
        if not artifact.is_file():
            raise ManifestError(f"artifact does not exist: {artifact}")
        artifact_record.update(
            {"filename": artifact.name, "bytes": artifact.stat().st_size, "sha256": sha256_file(artifact)}
        )
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
        "execution": {
            "provider": "daytona",
            "snapshot": snapshot,
            "auth_volume": auth_volume,
            "ephemeral": True,
            "auto_delete_minutes": 60,
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
    for required in ("source", "tier", "scenarios", "clients", "budget", "artifact", "execution"):
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
    if execution.get("source_checkout_in_credential_sandbox") is not False:
        raise ManifestError("credentialed canaries must not receive a source checkout")
    if execution.get("evidence_contains_message_bodies") is not False:
        raise ManifestError("canary evidence must remain body-free")
    return request


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
