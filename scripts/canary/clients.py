#!/usr/bin/env python3
"""Pinned low-cost client policy and command construction."""

from __future__ import annotations

from typing import Any


DEFAULT_CLIENTS = {
    "claude": {
        "binary": "claude",
        "version": "2.1.259",
        "model": "haiku",
        "model_family": "claude-haiku-4.5",
        "effort": "model-default",
        "pricing_guard": "max-budget-usd-print-mode-and-controller-turn-limit",
    },
    "codex": {
        "binary": "codex",
        "version": "0.151.0",
        "model": "gpt-5.6-luna",
        "reasoning_effort": "low",
        "fast_mode": False,
        "pricing_guard": "reported-token-and-controller-turn-limit",
    },
}

MINIMUM_CLIENTS = {
    "claude": {
        "version": "2.1.247",
        "relative_binary": "claude/node_modules/.bin/claude",
    },
    "codex": {
        "version": "0.149.1",
        "relative_binary": "codex/node_modules/.bin/codex",
    },
}

# Daytona's credentialed runner proxy rejects the Codex Responses WebSocket.
# Keep the pinned subscription login but use the supported custom-provider
# settings to select the HTTP/SSE transport for canary traffic.
CODEX_HTTP_PROVIDER_ARGS = [
    "--config",
    'model_provider="openai-http"',
    "--config",
    'model_providers.openai-http.name="OpenAI HTTP"',
    "--config",
    'model_providers.openai-http.base_url="https://chatgpt.com/backend-api/codex"',
    "--config",
    "model_providers.openai-http.requires_openai_auth=true",
    "--config",
    "model_providers.openai-http.supports_websockets=false",
]


def client_policy(
    *,
    claude_version: str | None = None,
    codex_version: str | None = None,
    claude_model: str | None = None,
    codex_model: str | None = None,
) -> dict[str, dict[str, Any]]:
    policy = {name: dict(values) for name, values in DEFAULT_CLIENTS.items()}
    overrides = {
        ("claude", "version"): claude_version,
        ("codex", "version"): codex_version,
        ("claude", "model"): claude_model,
        ("codex", "model"): codex_model,
    }
    for (client, key), value in overrides.items():
        if value is not None:
            cleaned = value.strip()
            if not cleaned:
                raise ValueError(f"{client} {key} cannot be empty")
            policy[client][key] = cleaned
    return policy


def claude_print_command(config: dict[str, Any], max_budget_usd: float) -> list[str]:
    """Build a bounded one-turn command; the prompt is supplied on stdin."""
    if max_budget_usd <= 0:
        raise ValueError("Claude print calls require a positive dollar limit")
    return [
        str(config["binary"]),
        "--print",
        "--output-format",
        "json",
        "--no-session-persistence",
        "--permission-prompts",
        "none",
        "--model",
        str(config["model"]),
        "--max-turns",
        "8",
        "--max-budget-usd",
        f"{max_budget_usd:.4f}",
    ]


def claude_live_command(config: dict[str, Any]) -> list[str]:
    """Build the interactive command used for live-attention scenarios."""
    return [str(config["binary"]), "--model", str(config["model"]), "--ax-screen-reader"]


def codex_exec_command(config: dict[str, Any]) -> list[str]:
    """Build a bounded noninteractive command; the prompt is supplied on stdin."""
    return [
        str(config["binary"]),
        "exec",
        "--json",
        "--ephemeral",
        "--model",
        str(config["model"]),
        "--sandbox",
        "read-only",
        "--config",
        f'model_reasoning_effort="{config["reasoning_effort"]}"',
        *CODEX_HTTP_PROVIDER_ARGS,
        "-",
    ]


def codex_live_command(config: dict[str, Any]) -> list[str]:
    """Build the interactive command used for native-queue scenarios."""
    return [
        str(config["binary"]),
        "--model",
        str(config["model"]),
        "--sandbox",
        "read-only",
        "--config",
        f'model_reasoning_effort="{config["reasoning_effort"]}"',
        *CODEX_HTTP_PROVIDER_ARGS,
    ]


def assert_low_cost_defaults(policy: dict[str, dict[str, Any]]) -> None:
    """Catch accidental drift to expensive defaults; explicit overrides remain visible."""
    claude = policy.get("claude", {})
    codex = policy.get("codex", {})
    if claude.get("model") != "haiku":
        raise ValueError("non-Haiku Claude models require --allow-model-override")
    if codex.get("model") != "gpt-5.6-luna":
        raise ValueError("non-Luna Codex models require --allow-model-override")
    if codex.get("reasoning_effort") != "low" or codex.get("fast_mode") is not False:
        raise ValueError("Codex canaries must default to low effort with fast mode disabled")
