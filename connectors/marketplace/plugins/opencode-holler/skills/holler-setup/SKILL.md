---
name: holler-setup
description: Installs, configures, diagnoses, or changes the OpenCode Holler connector and its native-prompt or startup-only attention mode. Use when a user asks to set up Holler, make OpenCode talkable, change wake behavior, or troubleshoot connector readiness.
---

# Holler Setup

Use `holler connector setup --harness opencode`; do not edit OpenCode configuration from memory. `native-prompt` submits a reference-only asynchronous prompt to the active session. `startup-only` provides durable hydration without live wakeups.

Run setup without `--apply` first and show the plan. Apply only after authorization:

```sh
holler connector setup --harness opencode --attention <native-prompt|startup-only> \
  --actor <actor> --peer <peer> --project <project> --package-source <source-package> --apply
```

Setup installs a connector-owned OpenCode config directory rather than rewriting the user's general config. It adds only the Holler MCP server, its eleven explicit tool permissions, lifecycle plugin, and two skills. Existing connector-owned files receive backups.

Launch configured sessions through:

```sh
holler connector launch --harness opencode --actor <actor> -- [additional opencode arguments]
```

The launcher supplies a fresh run identity, loopback server binding, per-run HTTP credentials, project root, MCP profile, plugin directory, and selected attention adapter. Starting plain `opencode` does not guarantee those bindings.

Naming is opt-in. Use `--name-mode exact` to refuse another live run unless the operator also supplies `--takeover`. Use `--name-mode allocate` for independently addressable parallel actors and `--launch-tag <stable-tag>` when a supervisor should reclaim an allocation after restart.

Run the doctor command reported by setup. `CONFIGURED` proves static setup only. `live-review` is `READY` only after an installed OpenCode client registers and processes a real native-prompt wake through claim and acknowledgement. `startup-only` can satisfy only `async-peer`.

Treat actor identity as an inbox identity. In legacy mode concurrent sessions using one actor are competing consumers; prefer allocate mode when each session must be independently addressable.
