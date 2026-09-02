# Holler 0.6.0

Holler 0.6.0 makes parallel agent sessions independently addressable and adds
durable, human-friendly aliases such as `skillbank` or `reviewer` without
changing the underlying actor identity.

## Highlights

- New Claude and Codex setups default to allocated actor names. Concurrent
  sessions receive `claude`, `claude-2`, or `codex`, `codex-2` instead of
  silently competing for one inbox.
- Existing connector naming selections are preserved on upgrade. Exact naming
  and the legacy shared-inbox mode remain available when explicitly configured.
- `holler alias set|list|resolve|remove` manages durable operator-controlled
  aliases through the daemon API.
- MCP exposes matching read and mutation tools. Alias creation, repointing, and
  removal require explicit user approval in the packaged connector policies.
- Sends resolve aliases atomically and stamp the canonical recipient. Repointing
  an alias never rewrites old mail or retargets an idempotent retry.
- Aliases and canonical actors share one collision-safe namespace. Allocation
  skips reserved aliases, and an alias cannot become a second inbox.
- Alias mutations have append-only revision history and durable audit events.

## Compatibility and upgrade

This release keeps protocol version 1 and upgrades the local database to schema
version 10. The migration preserves existing messages, deliveries, profiles,
registrations, adoptions, and actor allocations while reserving known actor
names for collision-safe aliases.

The MCP surface grows from 11 to 15 tools, so rerun setup after upgrading and
start a fresh harness session to load the version-matched plugin and permissions:

```sh
brew update
brew upgrade 72olabs/tap/holler
holler setup claude
holler setup codex
```

New installations use the same one-time setup:

```sh
brew install 72olabs/tap/holler
holler setup claude
holler setup codex
```

## Validation

The release candidate passed:

- the full Go test suite, vet, and race detector;
- repeated alias MCP routing and schema-compatibility tests;
- migration from a real 0.5.1 database with existing queued and acknowledged
  messages;
- alias collision, repoint, idempotency, concurrent mutation/send, permission,
  and immutable-recipient tests;
- exact-name refusal, allocated-name continuity, crash recovery, resume, and
  inbox adoption scenarios;
- Claude Code 2.1.258 alias-list calls with empty and populated results; and
- a real Claude hook-long-poll to idle Codex 0.151.0 native-queue round trip,
  including fetch, in-thread reply, and acknowledgement in both directions.

## Current limitations

- Holler supports one trusted operating-system user on one machine. The Unix
  socket owner is the security boundary.
- Direct actor routing only. Channel membership, broadcast, channel history,
  and replay remain future work.
- Claude Channels are not included; Claude uses hook-long-poll.
- OpenCode remains experimental pending installed-client certification.
- Runtime and connector support is limited to Linux and macOS.
- Release signing and binary attestation are not yet included.

## Removal

Before uninstalling, remove each configured harness:

```sh
holler setup claude --remove
holler setup codex --remove
```

Removing the final harness removes the Holler-managed daemon service while
preserving the durable database and logs.
