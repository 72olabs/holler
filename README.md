# Holler

[![CI](https://github.com/72olabs/holler/actions/workflows/ci.yml/badge.svg)](https://github.com/72olabs/holler/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/72olabs/holler?include_prereleases)](https://github.com/72olabs/holler/releases)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

Durable local messaging for terminal agents.

Your Claude and Codex sessions live in separate terminals. Holler lets them ask
questions, reply in a thread, and recover the conversation after a session or
daemon restart. You stop carrying messages between agents by hand.

Ask Codex to “holler at Claude.” Holler stores the message, wakes Claude when
the client supports it, and keeps the delivery available until Claude processes
it. Both agents continue running in their normal terminals.

```text
You     → Codex:  Holler at Claude and ask for a second opinion on this retry policy.
Codex   → Claude: QUESTION  Which failures should be retried?
Claude  → Codex:  ANSWER    Retry transport failures; surface policy denials.
Codex   → You:     Claude agrees on transport retries and says policy failures should stop.
```

Holler is a public alpha for one user on one machine.

## Install

On macOS:

```sh
brew install 72olabs/tap/holler
holler setup claude
holler setup codex
```

Setup shows every plugin, config, permission, and service change before asking
for confirmation. Run the same command after an upgrade to refresh the daemon
and version-matched connector package.

Start the agents normally:

```sh
claude
codex
```

No Holler launcher is required. The default actors are `claude` and `codex`,
configured as peers.

## Try it

Give Claude one turn, then leave it idle:

```text
You → Claude: You are the reviewer. Stay available for questions from Codex.
```

In Codex:

```text
You → Codex: Holler at Claude. Ask it to review our current approach, discuss
             any disagreement with it, then bring the conclusion back to me.
```

The packaged participation skill recognizes “holler at,” “ask,” “tell,” and
similar requests. Agents can also call the frozen Holler MCP tools directly.

Check the installation at any time:

```sh
holler status
```

The result identifies the client, daemon, protocol, and socket. Connector
diagnostics can then distinguish a daemon problem from plugin discovery, MCP
permission, project discovery, or attention failure.

Agents can describe themselves and discover specialized peers without the user
memorizing actor IDs:

```sh
holler profile --actor codex-reviewer --run reviewer-run --role "Reviews coupon correctness" --accepts REVIEW_REQUEST
holler who
```

The participation skill uses the same `holler_profile` and `holler_who` MCP
tools when the user assigns a role or says “holler at the coupon reviewer.”
Profiles are untrusted descriptive hints, never permissions.

When an allocated actor ends without a continuity tag, a user can explicitly
hand its inbox to one live replacement:

```sh
holler adopt --actor codex-reviewer-3 --run replacement-run \
  --from codex-reviewer-2 --project coupon \
  --idempotency-key recover-reviewer-2
```

Holler refuses a live source, a replacement run without its own live presence,
or an active source claim. The decision is durable, actor-global, and
one-winner; `--project` selects the audit-event partition rather than limiting
routing. Old and future mail addressed to the source reaches the replacement
while still reporting the original recipient. The source actor name is
permanently retired: an old session continuity handle receives a fresh suffixed
identity instead of silently reclaiming the transferred inbox, and a stale
connection cannot renew presence or author new messages or profile metadata
under the retired name. Plain protocol connections retain only read-only
diagnostics and session cleanup. Reusing the adopter's own name inherits its
adopted inboxes. Adoption is never automatic and does not support chains.

## What works today

- Claude Code and Codex talk in either direction after one-time setup.
- Messages sent while the recipient is offline remain in its durable inbox.
- Replies retain their thread and `in_reply_to` relationship.
- Claims use leases, so a crash before acknowledgement can be redelivered.
- Idempotency keys prevent a retry from creating a second durable message.
- Claude uses supervised hook-long-poll attention; Codex uses its native queue.
- Startup hydration recovers unread messages when live attention is unavailable.
- The daemon, CLI, MCP shim, and hooks share one versioned local API.
- Sender identity is bound to the connector connection rather than accepted on
  each model-controlled send.
- Agents can publish advisory role profiles and discover live, ended, or lapsed
  peers, their recent sessions, and orphaned inbox counts.
- Optional `exact` naming refuses accidental duplicate live actors; optional
  `allocate` naming creates parallel `actor`, `actor-2`, ... identities and
  reclaims them after restart from a session or supervisor launch tag.
- An explicitly authorized live actor can adopt one inactive actor's orphaned
  inbox without rewriting message recipients or losing provenance.

The naming, continuity, and adoption behaviors were validated separately in an
isolated two-Codex lab at commit `71611fb` with Codex CLI 0.151.0. That lab did
not include Claude. A later packaged `0.2.0` release-candidate canary used
Claude Code 2.1.252 to exercise the native `holler_adopt` confirmation prompt,
transfer one inactive inbox with original-recipient provenance intact, and
claim and acknowledge the message. A fresh idle Claude session in the same
isolated lab also accepted a real `hook-long-poll` wake, claimed and
acknowledged it, exited normally, and left no artifact monitor behind.

The 2026-08-28 pre-extraction release suite exercised both Claude-to-Codex and
Codex-to-Claude conversations, two concurrent threads, a three-agent review
handoff, daemon restart, abrupt Claude exit, lease recovery, and zero orphan
Holler monitors.
It used Claude Code 2.1.251 and Codex CLI 0.150.1. Those behavioral artifacts
identify build `0.1.0-alpha.1@2cc800b`, whose commit is not present in this
repository's post-extraction history; they are behavioral evidence, not
certification of the current commit.

## How it works

```text
┌──────────────────────┐                    ┌──────────────────────┐
│ Claude Code          │                    │ Codex CLI            │
│ skill · MCP · hooks  │                    │ skill · MCP · hooks  │
└──────────┬───────────┘                    └──────────┬───────────┘
           │                framed API                 │
           └──────────────┐  over UDS  ┌──────────────┘
                          ▼            ▼
                    ┌──────────────────────┐
                    │       hollerd        │
                    │ routing · leases     │
                    │ attention · events   │
                    └──────────┬───────────┘
                               │ only database owner
                               ▼
                    ┌──────────────────────┐
                    │ SQLite              │
                    │ messages · delivery │
                    │ outbox · presence   │
                    └──────────────────────┘
```

`hollerd` is the only process that opens SQLite. Every CLI command, MCP call,
and lifecycle hook connects through a mode-`0600` Unix socket.

A wake notification contains only a generated message reference. The recipient
must fetch and claim the body through its local connection, apply its own
permission rules, and acknowledge the delivery after processing. Peer messages
provide context, not authority.

## Delivery path

1. The sender commits a typed message, recipient delivery, and notification
   outbox entry in one transaction.
2. `hollerd` attempts live attention through the recipient's connector.
3. The recipient fetches and claims the message with a fenced lease.
4. It replies on the same thread and acknowledges the claim.
5. A crash or expired lease returns the delivery to the inbox for recovery.

An accepted wake is not treated as proof that the model processed the message.
The claim and acknowledgement are the durable evidence.

## Client support

| Client | Release status | Attention path | Setup |
| --- | --- | --- | --- |
| Claude Code | Supported alpha | `hook-long-poll`, with startup hydration fallback | `holler setup claude` |
| Codex CLI | Supported alpha | `native-queue`, with startup hydration fallback | `holler setup codex` |
| OpenCode | Package available; live certification pending | `native-prompt` or startup-only | Advanced connector setup only |
| Other agents | Use the CLI or local protocol; no bundled connector yet | Connector-defined | See the API and connector docs |

The experimental OpenCode package is included in release archives and Homebrew
installs, but is not yet a supported connector. Advanced testers must provide
its installed package path explicitly. For Homebrew:

```sh
holler connector setup --harness opencode --actor opencode \
  --package-source "$(brew --prefix holler)/share/holler/marketplace/plugins/opencode-holler" \
  --apply
```

For an extracted release archive, use
`./share/holler/marketplace/plugins/opencode-holler` as `--package-source`.

Holler exposes the same message semantics through MCP, CLI, and its framed
local protocol. A client does not need MCP if it can invoke the CLI or use a
future SDK.

Existing installations retain legacy actor behavior. For independently
addressable parallel sessions, opt in during setup:

```sh
holler setup codex --name-mode allocate
holler setup claude --name-mode allocate
```

Supervisors can launch with `--launch-tag <stable-tag>` so a replacement
process reclaims its allocation. Use `--name-mode exact` when duplicates must
be rejected and add launcher-only `--takeover` only for a deliberate handoff.

## Current boundaries

- One trusted operating-system user and one machine.
- The owning OS account and mode-`0600` socket are the trust boundary.
- Direct actor routing only. A `channel_id` is currently a label, not a real
  membership-enforced channel.
- No broadcast, channel history, channel replay, or channel membership yet.
- No bundled task manager or workflow authority.
- OpenCode is not called supported until its installed-client canary passes.
- Peer message bodies are untrusted input and never grant tool, filesystem,
  credential, spend, or release authority.

Real multi-party channels are the core of the [V2 roadmap](ROADMAP.md).
Multi-user and multi-node security remain later work.

## Product boundary

Holler answers two questions: who is talking to whom, and did the message
arrive? It owns identity binding, typed envelopes, routing, inboxes, threads,
delivery leases, acknowledgement, redelivery, and event cursors.

Task ownership, obligations, reviews, and decisions belong in GitHub, Linear,
Jira, or a separate work registry. Testing found that shared task state can
catch false completion, but imposing it on every short conversation consumed
substantially more time and model context.

## Other installation paths

Release archives contain `holler`, `hollerd`, and the matching Claude and Codex
connector marketplace. Download one from [GitHub Releases](https://github.com/72olabs/holler/releases),
keep `bin/` and `share/` under the extracted prefix, then run:

```sh
./bin/holler setup claude
./bin/holler setup codex
```

For source development:

```sh
./scripts/build.sh
./.build/holler setup claude
./.build/holler setup codex
go test ./...
```

Before uninstalling, remove each configured connector:

```sh
holler setup claude --remove
holler setup codex --remove
```

Removing the final connector stops the Holler-managed daemon service. The
durable database and logs are preserved.

## Documentation

- [Local API](API.md): framing, handshake, operations, and client surfaces.
- [Connector integration](connectors/README.md): packages, permissions,
  diagnostics, certification, and attention modes.
- [Security](SECURITY.md): current trust boundary and vulnerability reporting.
- [V2 roadmap](ROADMAP.md): sequenced scope, acceptance gates, and non-goals.
- [Release notes](RELEASE-NOTES.md): tested functionality and known limits.
- [Contributing](CONTRIBUTING.md): development and validation workflow.

## License

Apache-2.0. See [LICENSE](LICENSE).
