---
name: holler-setup
description: Installs, configures, diagnoses, or changes the Codex Holler connector and its native-queue or startup-only attention mode. Use when a user asks to set up Holler, make Codex talkable, change wake behavior, or troubleshoot connector readiness.
---

# Holler Setup

For ordinary first-time setup, use the opinionated product command:

```sh
holler setup codex
```

It defaults to actor `codex`, peer `claude`, project `default`, and live `native-queue` attention. It discovers the installed Holler marketplace, installs or refreshes the plugin, preserves and extends Codex user configuration with only the frozen Holler MCP allowlist, records absolute executable paths and connector identity, installs the per-user `hollerd` service, starts it, and verifies the socket. Existing connector identity wins on a rerun. The command previews exact paths and actions and asks for confirmation; use `--dry-run` for a non-mutating plan or `--yes` only when the user explicitly authorizes unattended application.

Codex owns executable-hook trust. On the first turn after installation or a package update, have the user inspect and trust the exact packaged `SessionStart` and `SessionEnd` commands and content hash when Codex prompts. Do not manipulate an internal trust store or recommend `--dangerously-bypass-hook-trust` for a normal session. Once trusted, normal `codex` launches reuse the connector automatically. Codex runs `SessionStart` on the first submitted turn, so native-queue addressability begins after that turn rather than when an untouched TUI opens.

Rerun setup after upgrading Holler. Before uninstalling the package, use `holler setup codex --remove`; the command strips only the managed Codex policy and connector state and leaves the durable database intact.

Use advanced connector setup when the user requests a custom attention mode, identity, project, policy destination, marketplace, or preview/apply split. Do not edit Codex configuration from memory:

```sh
holler connector setup --harness codex --attention <native-queue|startup-only> \
  --actor <actor> --peer <peer> --project <project> --marketplace <path-or-source> --apply
```

Advanced setup defaults to preview-only and writes a dedicated `$CODEX_HOME/<profile>.config.toml`; apply only after the user authorizes the reported changes. The product command may merge its exact allowlist into general Codex config because the user invoked and confirmed product setup. It preserves unrelated TOML byte-for-byte, creates a backup before a change, is idempotent, and refuses a conflicting pre-existing Holler policy instead of broadening it.

Setup records absolute runtime and Codex executable paths so a background daemon or plugin process never depends on an interactive shell `PATH`. Once the plugin hooks are trusted and its MCP policy is admitted, starting plain `codex` loads the persisted actor, routing, socket, and attention selection. Plugin processes from one Codex process derive the same immutable run automatically.

Use the connector launcher when you want the dedicated least-privilege profile, an explicit actor override, or a separately addressable concurrent session:

```sh
holler connector launch --harness codex --actor <actor> -- [additional codex arguments]
```

The launcher supplies a fresh random run, the reviewed named profile, working tree, connector identity, and attention selection. Explicit launcher values always override persisted plain-session defaults.

Run the doctor command reported by setup when diagnosis or certification is requested. `CONFIGURED` proves static setup only. A `live-review` connector is `READY` only after a real post-registration native-queue wake is fetched, claimed, and acknowledged. `startup-only` can satisfy only `async-peer`.

Treat actor identity as an inbox identity, not a display name. Concurrent sessions using one actor are competing consumers; use distinct actors when each session must be independently addressable.
