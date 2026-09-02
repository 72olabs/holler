# Holler 0.7.0

Holler 0.7.0 makes session routing explicit and failure states visible while
preserving the protocol-v1 socket and 0.6.1 capability bridge.

## Highlights

- Typed alias, actor, and immutable reply routes retain requested-route
  provenance; alias repoints cannot redirect old replies or idempotent retries.
- Daemon-proven harness-instance binding reconciles MCP, hooks, and monitors.
  Unproven connectors keep durable messaging but cannot claim live wake.
- Concurrent resume never steals a live predecessor. Holler gives the successor
  a usable identity and records a pending-takeover condition for an explicit
  operator decision.
- Per-recipient send receipts separate durable commit, control presence,
  attention capability, attachment, reason, and required sender action.
- Durable coalesced operator conditions appear in `holler status`, startup
  hydration, the CLI, MCP status, and the restart-free read bridge.
- Alias preflight exposes repoint and whole-actor impact before approval.
- Guarded reversible actor archival preserves names and unread mail, blocks on
  aliases/presence/claims, and fences late acknowledgements after an explicit
  crash-grace lease revocation.
- `holler migrate bare-harnesses` produces a read-only plan for legacy `claude`,
  `codex`, and `opencode` identities; it changes nothing automatically.

## Compatibility

The wire protocol remains version 1 and the database migrates forward to schema
14. A 0.6.1 MCP child can discover typed send, alias preflight, conditions, and
archive preflight through its existing capability bridge. Fresh connectors add
daemon-attested harness-instance binding and the new startup behavior.

## Validation

Release-candidate validation is in progress on the local release branch. The
release will not be pushed until the full unit, vet, race, sandbox-lab, packaged
artifact, upgrade, and live connector checks pass.

---

# Holler 0.6.1

Holler 0.6.1 makes 0.6.0's MCP upgrade boundary a one-time event. It adds a
fixed capability bridge whose implementation and catalog live in `hollerd`, so
a running 0.6.1 MCP child can use capabilities shipped by a later daemon.

## Highlights

- `holler_capabilities` returns the current daemon-owned typed catalog.
- `holler_read` invokes only catalog entries enforced as read-only by the
  daemon.
- `holler_write` invokes only write entries and remains `prompt`/`ask` gated in
  every packaged connector policy.
- Approving the generic `holler_write` bridge may also authorize write
  capabilities introduced by later daemon versions. This broader approval is
  the tradeoff that permits restart-free capability additions.
- The local API adds `list_capabilities`, `invoke_read_capability`, and
  `invoke_write_capability` without changing protocol version 1.
- Generic capability writes are not replayed after an ambiguous transport
  failure; each operation retains responsibility for its own idempotency key.

## Upgrade boundary

A process that is already running Holler 0.6.0 cannot execute code introduced
by 0.6.1. Upgrade the package, rerun setup, and reconnect that existing harness
once so it loads the three bridge tools:

```sh
brew update
brew upgrade 72olabs/tap/holler
holler setup claude
holler setup codex
```

After that bootstrap, later daemon upgrades do not require the MCP child to be
replaced for capabilities exposed through the bridge. The reference client
reconnects to a restarted daemon, refreshes the catalog, and invokes the new
operation through the same fixed tool surface.

## Validation

- An unchanged MCP server and API client were connected to one daemon, then the
  daemon was replaced with a build exposing a new `future.echo` capability.
  The same MCP server discovered and invoked it successfully.
- The daemon rejected a read capability sent through `holler_write` and a write
  capability sent through `holler_read`.
- Connector manifest tests verify the frozen eighteen-tool surface and require
  explicit approval for the generic write bridge.

---

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
