# Holler

Durable local messaging for terminal agents.

Holler lets a Claude Code session ask a Codex session a question, lets Codex
reply on the same thread, and keeps the exchange recoverable if either agent or
the daemon restarts. It is a provider-independent message bus, not another
agent framework: agents keep running in their normal terminals and communicate
through a shared local service.

> **Public alpha:** the current release is for one user on one machine. It is
> not a remote service, a multi-user security boundary, or a production task
> orchestrator.

## What works today

- direct actor-to-actor messaging with durable inboxes;
- threaded replies, idempotent sends, leased claims, acknowledgement, retry,
  and dead-letter handling;
- ordinary Claude Code and Codex launches after one-time connector setup;
- live attention through Claude hook-long-poll and the Codex native queue;
- startup hydration when an agent was offline;
- daemon restart, abrupt Claude exit, and same-actor session handoff recovery;
- a universal framed API used by the CLI, MCP tools, and harness connectors;
- two concurrent threads and a three-agent review workflow, tested end to end.

Real channel membership, broadcast, channel history, and replay are not in the
alpha. A `channel_id` is currently a label on a direct message. Those features
remain future work.

## Quick start

On macOS, install from the 72o Labs tap, then configure each installed harness
once:

```sh
brew install 72olabs/tap/holler
holler setup claude
holler setup codex
```

The same formula works with an explicit tap first if your Homebrew policy
requires it:

```sh
brew tap 72olabs/tap
brew install holler
```

Release archives are the package-manager-independent option. Each archive
contains `holler`, `hollerd`, and the version-matched Claude and Codex
connector marketplace. Unpack it without separating `bin/` from `share/`,
change into the extracted directory, and run:

```sh
./bin/holler setup claude
./bin/holler setup codex
```

Setup previews its changes and asks for confirmation. It installs or refreshes
the harness plugin, records a stable actor identity, admits only the frozen
Holler MCP tools, starts the per-user daemon service, and verifies its socket.
Run `holler status` after setup to inspect the client, daemon, protocol, and
socket identities.

Then start both harnesses normally:

```sh
claude
codex
```

The default actors are `claude` and `codex`, configured as peers. Natural
requests such as “holler at Claude,” “ask Codex,” or “tell the reviewer” invoke
the Holler participation skill and its MCP workflow. Codex intentionally asks
the user to review and trust the packaged lifecycle hooks on the first turn
after installation or update.

For a source-tree development install:

```sh
./scripts/build.sh
./.build/holler setup claude
./.build/holler setup codex
```

Rerun the same setup commands after upgrading Holler so the harness cache and daemon build move together. To remove a connector safely before uninstalling the package:

```sh
holler setup claude --remove
holler setup codex --remove
```

Removing the last configured harness stops and removes the setup-owned daemon service while preserving the durable database and logs.

The release workflow produces host-native macOS and Linux archives. Homebrew
builds from the tagged source archive and is the intended macOS package path;
additional prebuilt OS/architecture pairs are not claimed until they are added
to the release matrix and clean-machine certification.

## Alpha boundaries

Holler trusts the local operating-system account that owns its mode-`0600`
Unix socket. It does not yet use signed actor challenge-response, and it must
not be exposed across users or machines. Peer message bodies are untrusted
input: attention notifications reveal only a server-generated message ID, and
agents fetch the body explicitly before deciding what to do.

The release rehearsal passed against Claude Code 2.1.251 and Codex CLI 0.150.1;
the packaged connector manifests retain the lower compatibility baselines used
by deterministic certification. OpenCode has a built connector package but has
not passed the installed-client canary, so it is not claimed as supported in
this alpha. See
[SECURITY.md](SECURITY.md) and [RELEASE-NOTES.md](RELEASE-NOTES.md) before
deploying or contributing.

## Product boundary

Holler owns identity binding, typed envelopes, direct routing, inboxes,
threads, delivery leases, acknowledgement, retry/redelivery, and event
cursors. It answers *who is talking to whom, and did the message arrive?*

Task ownership, obligations, reviews, and decisions belong in a separate work
registry such as Linear, Jira, or GitHub. Organizational policy—when to ask,
whom to ask, escalation, and termination—also sits above the bus. Testing found
that richer task state can help some workflows, but coupling
it to transport made simple agent conversations slower and more expensive.

## Documentation

- [API.md](API.md) documents the implemented framed Unix-socket API.
- [connectors/README.md](connectors/README.md) covers connector setup,
  manifests, diagnostics, certification, and supported attention modes.
- [SECURITY.md](SECURITY.md) defines the alpha trust boundary and reporting
  process.
- [CONTRIBUTING.md](CONTRIBUTING.md) describes development and validation.
- [RELEASE-NOTES.md](RELEASE-NOTES.md) lists tested functionality and current
  limitations.
- [ROADMAP.md](ROADMAP.md) defines the sequenced V2 scope, acceptance gates,
  and explicit non-goals.

The current Go implementation provides the
SQLite message and per-recipient delivery lifecycle plus the single-writer `hollerd` boundary:
idempotent send, non-consuming inbox checks, leased claims, acknowledgement, retry/dead-letter nack,
lease renewal, crash recovery, renewable registrations, durable asynchronous notification delivery,
and per-partition durable and operational event positions.

Only the separate `hollerd` executable opens SQLite. CLI commands, the MCP shim, and lifecycle hooks all use
the same versioned framed-JSON API over a mode-`0600` Unix socket. The connection handshake binds an
actor and immutable run, and the daemon stamps that identity onto sends instead of accepting a
model-controlled sender. The client self-reports its build identity and the daemon stamps its own into
operational evidence; within the current same-user trust boundary, certification rejects unknown or dirty
identities. This slice does not yet implement the protocol's Ed25519 challenge-response;
until credentials land, the local socket and its owning OS user are the trust boundary.

```sh
go test ./...
./scripts/build.sh
./.build/hollerd --db /tmp/holler.sqlite3 --socket /tmp/holler.sock

# In another terminal:
./.build/holler send --socket /tmp/holler.sock \
  --actor implementer --run run-1 --to reviewer --idempotency-key demo-1 \
  --type QUESTION --body '{"text":"Which retry policy applies?"}'
./.build/holler inbox --socket /tmp/holler.sock --actor reviewer --run run-2

# Diagnose static integration, then certify evidence from a bounded real-client run.
go run ./cmd/holler connector doctor --harness codex --project "$PWD" --policy /path/to/policy.toml
go run ./cmd/holler connector certify --harness codex --project demo --actor codex --run run-1
```
