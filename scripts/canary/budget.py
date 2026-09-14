#!/usr/bin/env python3
"""Budget policy and accounting for token-consuming canaries."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


TIER_BUDGETS = {
    "preflight": {
        "claude_usd": 0.0,
        "codex_reported_tokens": 0,
        "model_turns": 0,
        "wall_seconds": 1200,
    },
    "core": {
        "claude_usd": 0.50,
        "codex_reported_tokens": 250_000,
        "model_turns": 8,
        "wall_seconds": 1800,
    },
    "release": {
        "claude_usd": 1.00,
        "codex_reported_tokens": 500_000,
        "model_turns": 12,
        "wall_seconds": 2700,
    },
    "extended": {
        "claude_usd": 2.00,
        "codex_reported_tokens": 1_000_000,
        "model_turns": 24,
        "wall_seconds": 5400,
    },
}


class BudgetExceeded(RuntimeError):
    """Raised before another model call when a configured limit is exhausted."""


@dataclass
class BudgetLedger:
    limits: dict[str, Any]
    claude_usd: float = 0.0
    codex_reported_tokens: int = 0
    model_turns: int = 0
    wall_seconds: float = 0.0

    def charge(
        self,
        *,
        client: str,
        reported_tokens: int = 0,
        cost_usd: float = 0.0,
        turns: int = 1,
        wall_seconds: float = 0.0,
    ) -> None:
        if client not in {"claude", "codex", "controller"}:
            raise ValueError(f"unsupported client {client!r}")
        if min(reported_tokens, cost_usd, turns, wall_seconds) < 0:
            raise ValueError("budget charges cannot be negative")
        next_claude = self.claude_usd + (cost_usd if client == "claude" else 0.0)
        next_codex = self.codex_reported_tokens + (reported_tokens if client == "codex" else 0)
        next_turns = self.model_turns + turns
        next_wall = self.wall_seconds + wall_seconds
        checks = (
            ("claude_usd", next_claude),
            ("codex_reported_tokens", next_codex),
            ("model_turns", next_turns),
            ("wall_seconds", next_wall),
        )
        for name, value in checks:
            if value > self.limits[name]:
                raise BudgetExceeded(f"{name} would be {value}, limit is {self.limits[name]}")
        self.claude_usd = next_claude
        self.codex_reported_tokens = next_codex
        self.model_turns = next_turns
        self.wall_seconds = next_wall

    def as_dict(self) -> dict[str, Any]:
        return {
            "claude_usd": round(self.claude_usd, 6),
            "codex_reported_tokens": self.codex_reported_tokens,
            "model_turns": self.model_turns,
            "wall_seconds": round(self.wall_seconds, 3),
        }


def budget_for_tier(tier: str) -> dict[str, Any]:
    try:
        return dict(TIER_BUDGETS[tier])
    except KeyError as error:
        raise ValueError(f"unknown tier {tier!r}") from error


def validate_estimate(scenarios: list[dict[str, Any]], limits: dict[str, Any]) -> None:
    estimated_turns = sum(scenario["estimated_model_turns"] for scenario in scenarios)
    if estimated_turns > limits["model_turns"]:
        raise ValueError(
            f"scenario estimate is {estimated_turns} model turns but the limit is {limits['model_turns']}"
        )
