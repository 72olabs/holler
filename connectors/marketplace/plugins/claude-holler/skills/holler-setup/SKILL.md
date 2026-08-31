---
name: holler-setup
description: Installs, configures, diagnoses, or changes the Claude Holler connector and its hook-long-poll or startup-only attention mode. Use when a user asks to set up Holler, make Claude talkable, change wake behavior, or troubleshoot connector readiness.
---

# Holler Setup

For ordinary first-time setup, use the opinionated product command:

```sh
holler setup claude
```

It defaults to actor `claude`, peer `codex`, project `default`, and live `hook-long-poll` attention. It discovers the installed Holler marketplace, installs or refreshes the plugin, merges only the frozen Holler MCP allowlist and plugin options into Claude user settings, records the absolute Holler executable and connector identity, installs the per-user `hollerd` service, starts it, and verifies the socket. Existing connector identity wins on a rerun. The command previews exact paths and actions and asks for confirmation; use `--dry-run` for a non-mutating plan or `--yes` only when the user explicitly authorizes unattended application.

Rerun setup after upgrading Holler. Before uninstalling the package, use `holler setup claude --remove`; the command removes only Holler-managed Claude state and leaves the durable database intact.

After setup, normal `claude` launches load the connector automatically; the user does not need the connector launcher. Use advanced connector setup when the user requests a custom attention mode, identity, project, scope, marketplace, or preview/apply split. Do not edit Claude settings from memory:

```sh
holler connector setup --harness claude --attention <hook-long-poll|startup-only> \
  --actor <actor> --peer <peer> --project <project> --marketplace <path-or-source> --apply
```

Advanced setup defaults to preview-only. Show the resulting plan and apply only after the user authorizes the reported changes. Run the reported doctor command when diagnosis or certification is requested. `CONFIGURED` proves static setup only; use a real wake/claim/ack canary before describing the connector as `READY`.

The native Claude Channels adapter remains research-only and is not included in this package. Never instruct users to add `--channels` for Holler or modify organization channel policy.

Setup records the absolute Holler executable and connector binding. Starting plain `claude` therefore loads the persisted actor, routing, socket, and attention selection even when the plugin process has a minimal `PATH`. Plugin processes from one Claude process derive the same immutable run automatically.

Use the connector launcher for an explicit actor override or a separately addressable concurrent session:

```sh
holler connector launch --harness claude --actor <actor> -- [additional claude arguments]
```

The launcher creates a fresh random run identity and supplies the selected adapter. Explicit launcher values always override persisted plain-session defaults.

Naming is opt-in so existing installations retain their current behavior. Use `--name-mode exact` when one durable actor must refuse a second live run; add launcher-only `--takeover` to supersede that run deliberately. Use `--name-mode allocate` when concurrent sessions should receive `actor`, `actor-2`, and so on. Add `--launch-tag <stable-tag>` when a supervisor needs a restarted process to reclaim its previous allocation. Never generate takeover from an agent tool call.

Treat actor identity as an inbox identity, not a display name. In legacy mode concurrent sessions using one actor are competing consumers and the first successful claim owns each message. Prefer allocate mode when the user expects each session to be independently addressable.
