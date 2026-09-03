# Holler 0.7.1

Holler 0.7.1 hardens Claude's hook-long-poll adapter against duplicate wake
acceptance and recursive Stop continuations.

## Fixes

- A Claude turn started by an `asyncRewake` notice no longer receives the same
  unread reference again when its Stop hook rearms. The continuation monitor
  stays parked and still wakes for later message IDs.
- A parked attention waiter is now consumed atomically with its first accepted
  notice. A racing second notification remains retryable until the monitor has
  actually rearmed instead of being recorded as accepted into an abandoned
  waiter channel.

## Validation

- Full Go tests, `go vet`, the executable OpenCode plugin behavior tests, and a
  source build pass.
- A real Claude Code 2.1.259 MCP startup connected, reconciled its allocated
  identity, and called `bus_status` successfully.
- The minimum supported Claude Code 2.1.247 build emitted
  `stop_hook_active` and passed the same two-wake unavailable-MCP probe.
- The isolated hard-crash and resume probe delivered only to each successor,
  preserved the resumed Claude session ID, and left zero orphan monitors.
- With the Claude MCP intentionally unavailable, two distinct wakes each
  produced exactly one Stop continuation; after each one Claude remained alive
  with exactly one hook-long-poll monitor parked.

---

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
- `holler who` reports unread count and age, active claims and earliest lease
  expiry, and the current stale-unread condition state for each actor.
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

The local release candidate passed:

- the full Go test suite, `go vet`, and the race detector;
- the required executable OpenCode plugin test and a Linux amd64 cross-build;
- schema-11 to schema-14 migration with queued-mail preservation;
- all seven isolated certification-lab scenarios, including daemon restart,
  alias resume, immutable reply routing, and a three-agent/two-thread handoff,
  with zero orphan processes;
- checksum, clean build identity, extracted-layout setup discovery, and the same
  seven scenarios from the packaged release artifact; and
- packaged CLI smoke coverage for alias preflight, conditions, guarded archival
  with unread mail, archived discovery, and the read-only bare-harness migration
  plan;
- a packaged-artifact Codex CLI 0.151.0 canary covering native-queue wake,
  claim, acknowledgement, and MCP reply with a `READY` certificate;
- a packaged-artifact Claude Code 2.1.258 interactive canary covering an idle
  hook-long-poll wake, automatic follow-up turn, claim, acknowledgement, and
  MCP reply with a `READY` certificate and zero orphan release processes; and
- an independent release-gate review that migrated a copy of the live database
  without losing any of its 23 actors, 3 aliases, 243 messages, or unacknowledged
  deliveries, then behaviorally verified routing, identity, lifecycle, and
  operator-condition invariants.

The final release artifact must be built from the exact `v0.7.0` tag so every
binary and connector reports the public release version.

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
