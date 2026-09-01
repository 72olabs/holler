# Holler 1.5.2

Holler is a durable local communication layer for terminal agents. This
release makes multiple same-harness agents discoverable and independently
addressable, preserves their identities across restart, and adds an explicit
recovery path for an inactive actor's inbox.

## Highlights

- One-time `holler setup claude` and `holler setup codex`; afterward,
  start both harnesses normally.
- Reliable in-place macOS upgrades, including bounded retry when launchd is
  still retiring the previous daemon service.
- Durable direct inboxes backed by a single-writer local daemon.
- Provider-neutral API shared by the CLI, MCP tools, and harness hooks.
- Idempotent sends, non-consuming inbox checks, leased claims, lease renewal,
  acknowledgement, retryable/final nack, and dead-letter handling.
- Claude hook-long-poll and Codex native-queue attention with startup hydration
  as the durable fallback.
- Explicit session handoff and abrupt-crash recovery without stale monitors
  continuing to accept wakes.
- Reference-only wake notifications: message bodies remain behind an explicit
  inbox claim and are treated as untrusted peer input.
- `holler who` and `holler_who` discovery with advisory role profiles, recent
  sessions, liveness, and orphaned inbox counts.
- Explicit `exact` and `allocate` naming: exact names reject accidental live
  collisions, while allocated workers receive stable suffixed identities.
- Continuity binding by harness session or supervisor launch tag, including
  restart-safe fencing after an explicit takeover.
- Human-authorized, one-winner inbox adoption with preserved original-recipient
  provenance, retired-source fencing, and idempotent retries.
- Offline `holler version`/`holler --version` output and hardened, rerunnable
  release packaging.

## Functionally tested

The release validation suite has demonstrated:

- Claude-to-Codex and Codex-to-Claude send, fetch, reply, and acknowledgement;
- two simultaneous direct-message threads across three durable actors;
- exact-artifact review, revision, and approval without cross-thread leakage;
- a reviewer handoff to a new session under the same actor identity;
- Claude hard-crash replacement and resume with no stale accepted wake;
- daemon reconnects and fake-clock outages both shorter and longer than the
  five-minute presence lease;
- zero Holler-owned orphan Claude monitors after shutdown;
- Go tests, vet, race tests, macOS builds, and Linux cross-builds.

The 2026-08-28 macOS release rehearsal used Claude Code 2.1.251 and Codex CLI
0.150.1. Both two-agent role directions passed all nine behavioral assertions;
the three-agent handoff passed all 11 infrastructure and 12 functionality
assertions. These end-to-end runs predate repository extraction and identify
build `0.1.0-alpha.1@2cc800b`; that commit is not present in this repository's
history. They demonstrate the listed behavior but do not certify the current
commit. Release-candidate certification includes a packaged Claude-to-Codex
canary built from this repository.
Release certification against the current and immediately previous supported
client versions remains part of the release-candidate gate.

## Installation

On macOS:

```sh
brew install 72olabs/tap/holler
holler setup claude
holler setup codex
claude
codex
```

Release archives provide the package-manager-independent path and contain this
layout:

```text
holler-<version>-<os>-<arch>/
  bin/holler
  bin/hollerd
  share/holler/marketplace/
```

The hosted release workflow currently publishes `linux-amd64` and
`darwin-arm64` archives. Homebrew builds from source for the host architecture.

Keep `bin/` and `share/` under the same extracted prefix, then run:

```sh
./bin/holler setup claude
./bin/holler setup codex
claude
codex
```

The tap formula builds from the immutable tagged source archive and installs
the version-matched connector marketplace under Homebrew's stable prefix.

## Current limitations

- One local operating-system user and one machine only. The Unix socket owner
  is the security boundary; signed actor authentication is not implemented.
- Direct actor routing only. `channel_id` labels messages, but membership,
  broadcast, channel history, and channel replay are V2 work.
- Claude Channels are excluded from the release; Claude uses hook-long-poll.
- The OpenCode package is included for advanced testing, but remains
  experimental until it passes a live installed-client canary.
- Linux builds and deterministic tests are covered by CI, but Linux setup and
  user-service lifecycle support must pass a clean-machine rehearsal before it
  is advertised for this release.
- Codex registration and native attention begin when the first turn triggers
  its lifecycle hook, not merely when an idle TUI is opened.
- A wake already accepted by a harness queue cannot be retracted if another
  poll acknowledges the message first. The resulting empty wake does not
  duplicate durable processing, but can be visible as harmless notification
  noise.
- Release signing and attestation are not part of this alpha.

## Upgrade and removal

Rerun setup after every Holler upgrade so the daemon and cached connector
package stay version-matched:

```sh
holler setup claude
holler setup codex
```

Before uninstalling, remove each configured harness:

```sh
holler setup claude --remove
holler setup codex --remove
```

Removing the final harness removes the Holler-managed daemon service but
preserves the durable database and logs.
