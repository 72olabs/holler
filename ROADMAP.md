# Holler V2 roadmap

Status: proposed execution plan.

V2 is the next product milestone, not necessarily the wire-protocol version.
Protocol changes must be negotiated independently so V1 clients can continue
direct messaging during an upgrade.

## North star

Move Holler from durable local direct messages to trustworthy local rooms where
three or more agents—and eventually a human—can coordinate, recover, and audit
a conversation without a person relaying messages between terminals.

V2.0 is complete when real channels work end to end on one machine. V2.1 adds
the human chat and administration surface. Multi-node transport is a gated
follow-on, not part of the V2.0 release contract.

## Product boundaries

Holler V2 owns:

- connector-bound actor identity and session presence;
- channels, membership, threads, broadcast, history, and replay;
- per-recipient delivery, read positions, attention, and recovery;
- a provider-neutral protocol plus CLI, MCP, SDK, and connector surfaces;
- storage lifecycle, export, diagnostics, and bus-health observability.

Holler V2 does not become:

- a task manager, workflow engine, or source of release authority;
- an agent runtime that decides when an agent must ask, stop, or escalate;
- a filesystem, tool, credential, spend, or sandbox authorization system;
- a remote multi-tenant service disguised as a local daemon.

Work items, obligations, reviews, and decisions may be referenced through
opaque `scope_refs` or `authority_ref` values. Their authoritative state stays
in GitHub, Linear, Jira, or a separate work registry. The task-management
experiment showed that shared task state can catch false completion, but made
short conversations materially slower and more token-expensive. It should be
an optional integration, not a tax on every message.

Claude Channels and Holler channels are different concepts. Claude Channels is
a client-specific wake transport. Holler channels are durable, provider-
neutral communication containers. One must never be required for the other.

## Release slices

| Slice | Promise | Required for |
| --- | --- | --- |
| V2.0 | Membership-enforced multi-party channels on one trusted local node | The core V2 release |
| V2.1 | Local human chat, supervision, and administration over HTTP/WebSocket | “Slack for agents” human experience |
| V2.x research | Supervised client runtimes and experimental multi-node transport | Evidence for a later distributed release |

## Impact and complexity ranking

The tiers are independent. Impact measures how much a capability advances the
product or removes a release-threatening risk. Complexity measures engineering
difficulty and cross-system failure risk—not implementation value.

| Tier | Impact | Complexity |
| --- | --- | --- |
| S | Defines the product promise or is required to trust it | Cross-cutting architecture with difficult consistency or lifecycle failure modes |
| A | Materially improves adoption, safety, or operability | Multiple subsystems, migrations, or client integrations |
| B | Valuable expansion with a narrower audience or dependency | Bounded work with known patterns and interfaces |
| C | Speculative, deferrable, or outside the current product boundary | Localized change with low coupling and straightforward verification |

Ranked by product impact, then by critical-path urgency:

| Rank | Capability | Impact | Complexity | Target | Rationale |
| ---: | --- | :---: | :---: | --- | --- |
| 1 | Membership-enforced channels, atomic fan-out, and threads | S | S | V2.0 | This is the V2 product promise and the foundation for every multi-agent experience. |
| 2 | Attention, connector lifecycle, crash recovery, and handoff | S | A | V2.0 | A durable message is not useful if the right live agent is not reliably awakened or resumed. Harness integration was the riskiest V1 subsystem. |
| 3 | Ordered history, replay, read positions, and resumable subscriptions | S | S | V2.0 | Makes channel state trustworthy across restarts, gaps, compaction, and same-actor handoff. |
| 4 | Connector-bound actor identity and membership enforcement | S | A | V2.0 | Prevents a model from impersonating another configured actor while preserving bring-your-own identity. |
| 5 | Released-artifact install, upgrade, rollback, and client canaries | S | A | V2.0 | Protects the working V1 baseline and proves the product outside a source checkout. |
| 6 | Crash injection, conformance, adversarial tests, and release integrity | S | A | V2.0 | Converts protocol claims into evidence and protects the trust boundary at release time. |
| 7 | Protocol negotiation and crash-safe schema migration | A | A | V2.0 | Allows old and new clients to coexist without making every product release a coordinated upgrade. |
| 8 | Retention, compaction, export, backup, and restore | A | S | V2.0 | Keeps fan-out storage bounded without silently destroying delivery or replay state. |
| 9 | Diagnostics, health, metrics, and stale-state visibility | A | B | V2.0 | Shortens setup and production debugging, especially across daemon, connector, permission, and wake layers. |
| 10 | Versioned CLI, MCP, Go SDK, and connector conformance surfaces | A | A | V2.0 | Makes channel behavior portable across harnesses without allowing adapters to become alternate protocols. |
| 11 | Human chat and administration UI | A | A | V2.1 | Delivers the clearest “Slack for agents” experience, but should sit on a stable channel and replay contract. |
| 12 | TypeScript and Python SDK parity | B | B | V2.x | Broadens integration beyond MCP and CLI after the Go contract proves the SDK shape. |
| 13 | OpenCode certification and generic harness guidance | B | B | V2.0 | Expands reach and validates portability, but does not define the core channel model. |
| 14 | Native wake and supervised-runtime adapters | B | A | Research | May improve attention reliability, but depends on unstable or vendor-specific harness capabilities. |
| 15 | Optional external work-registry integration | C | A | Post-V2.0 | Can improve long-running coordination, but adds latency and token cost and must remain outside Holler's core state. |
| 16 | Multi-node routing, replication, and remote wake | C | S | Post-V2 | Large distributed-systems cost before local channels, identity binding, replay, and retention are proven. |

The V2.0 cut line is ranks 1–10 plus OpenCode certification at rank 13. Rank 11
is the V2.1 experience layer. Ranks 12 and 14–16 must not delay the V2.0 money
shot unless experiments produce evidence that they unblock a release gate.

## Execution order

### 0. Close the alpha feedback loop

Treat current direct messaging as the compatibility baseline while V2 is built.

Build:

- clean-machine macOS install, upgrade, rollback, and removal tests;
- a clean-machine Linux service-lifecycle rehearsal before advertising Linux;
- current and previous client-version canaries for Claude Code and Codex;
- an installed-client OpenCode canary before calling OpenCode supported;
- private vulnerability reporting, release provenance, signing, and checksum
  verification;
- structured upgrade diagnostics that separate daemon, plugin, permission,
  project-discovery, and attention failures.

Exit gate:

- no known P0 data-loss, identity, setup, or orphan-process bug;
- every supported client passes send, wake, claim, reply, and acknowledge from
  a released artifact;
- a failed setup is retryable and leaves an inspectable, recoverable state.

### 1. Add actor binding and protocol evolution

Holler accepts an opaque actor identity supplied by the user, client, or an
external identity system. It binds that identity to a connector connection and
run, but does not become the actor registry or credential issuer. The owning OS
user remains the V2 trust boundary.

Build:

- opaque actor IDs with optional identity-provider and issuer metadata that
  Holler stores without interpreting;
- stable actor, configuration, run, and client-session identities kept
  distinct;
- connector-bound sender identity that cannot be changed on an individual
  model-controlled send;
- explicit diagnostics for cloned bindings and concurrent runs sharing one
  actor;
- per-partition and per-channel operation scopes;
- immutable administrative events for binding and scope changes;
- protocol capability negotiation and typed errors for old/new client mixes;
- forward-only, crash-safe database migrations with preflight and backup;
- an external verifier interface reserved for later multi-user or multi-node
  deployments, without implementing a Holler credential system in V2.0.

Compatibility rule:

- V1 direct-message clients keep working through a documented transition
  window. V2-only channel operations require negotiated channel capabilities.

Exit gate:

- a model cannot override the actor bound by its connector on send, claim,
  acknowledgement, membership, or history operations;
- actor continuity survives restart and handoff while concurrent same-actor
  runs remain visible;
- channel membership is enforced for configured actors within the documented
  single-user trust boundary;
- mixed V1/V2 daemon-client upgrade tests pass in both upgrade orders.

### 2. Build the real channel vertical slice

Build:

- create, inspect, list, close, join, and leave operations;
- durable membership with explicit read, post, and membership-management
  permissions;
- channel posts that atomically fan out to one independent delivery lifecycle
  per member;
- threads rooted in a channel and replies linked to the correct root;
- a monotonic `channel_seq` assigned at commit;
- membership snapshots and immutable membership-change events;
- direct messages and existing per-recipient delivery semantics preserved.

Migration rule:

- existing `channel_id` strings are legacy labels, not implicit channel
  records. V2 must not silently infer membership or expose old messages. New
  channels receive explicit identities; importing legacy conversations is a
  deliberate operation.

Money-shot acceptance test:

1. Claude, Codex, and OpenCode join one channel.
2. One threaded post fans out to three independent deliveries.
3. A fourth, non-member actor cannot read or post.
4. One member crashes after claim; the other members continue unaffected.
5. The crashed member resumes or hands off and receives only its own redelivery.
6. Every delivery is acknowledged exactly once at the durable layer.

Exit gate:

- the scenario passes in every supported three-client combination and after a
  daemon restart at each transaction boundary.

### 3. Make history, replay, and continuity trustworthy

Build:

- ordered, paginated channel history using `channel_seq` as the authority;
- thread replay as a filtered view of channel order;
- per-member read positions distinct from delivery acknowledgement;
- resumable subscriptions with positions, checkpoints, leases, and explicit
  gap errors;
- deterministic reconstruction of channel, membership, thread, and read state;
- correction/tombstone events instead of destructive history rewriting;
- role/session handoff that preserves channel membership and read position
  without changing message provenance.

Exit gate:

- a channel and each thread reconstruct byte-for-byte equivalently after
  restart, compaction boundary, and same-actor handoff;
- no member's claim, read, or acknowledgement mutates another member's state;
- a released or compacted cursor fails explicitly with the recoverable lower
  bound instead of silently skipping history.

### 4. Bound storage and make operations inspectable

Channels multiply message and delivery volume, so bounded storage is a V2
correctness feature rather than a later optimization.

Build:

- retention classes with partition/channel overrides;
- periodic sweep and ad-hoc compaction with mandatory dry-run output;
- checkpoints and retention events identifying every discarded range;
- JSONL archive-before-delete, with a stable import/export contract;
- bounded abandonment for dead recipients and stalled subscriptions;
- protection for unacknowledged delivery, required membership history, and
  live subscriber watermarks;
- `hollerd stats --json`, structured logs, queue/lease/wake latency, WAL and
  storage growth, dead letters, stale presence, and retention blockers;
- backup, restore, integrity-check, and disaster-recovery commands.

Exit gate:

- a soak test with sustained channel fan-out remains within its configured disk
  budget;
- dry-run predicts the rows and bytes removed, and the executed sweep matches;
- export plus restore reconstructs the retained channel state;
- stalled subscribers are visible and never silently force-advanced.

### 5. Productize every client surface

Build:

- CLI commands for channel lifecycle, membership, posting, history, threads,
  read positions, export, and diagnostics;
- a frozen, versioned MCP tool surface with an explicit reapproval path when
  schemas or write capabilities change;
- a public Go SDK for V2.0, followed by thin TypeScript and Python SDKs after
  the channel contract is proven unless a supported harness makes one
  release-critical;
- a connector SDK containing lifecycle registration, hydration, attention,
  actor binding, readiness states, and conformance fixtures;
- generic CLI/SDK connector guidance for Kimi and other harnesses;
- OpenCode live certification, then inclusion in normal setup and release
  claims;
- attention coalescing, `requires_reply`, and terminal/no-reply semantics to
  prevent acknowledgement loops and channel notification storms;
- adapter-specific capability reporting without pretending every client has
  native wake.

Research separately:

- a supervised `codex app-server` adapter for reliably addressable workers;
- the Claude native-channel adapter when the vendor feature leaves research;
- other native wake APIs as replaceable adapters, never protocol dependencies.

Exit gate:

- the conformance suite passes through CLI, MCP, and at least one SDK;
- Claude, Codex, and OpenCode each pass lifecycle, offline hydration, live
  attention, crash, compaction, and upgrade canaries;
- unsupported attention degrades visibly to durable startup/polling behavior.

### 6. Add the human chat and administration surface (V2.1)

Build only after the channel API and replay contract stabilize.

Build:

- an opt-in loopback HTTP and WebSocket server embedded in `hollerd`;
- push updates from the existing notification/event path, with no periodic full
  state polling;
- channel list, threaded history, unread/read state, search, compose, reply,
  and human acknowledgement;
- presence that distinguishes online, listening, busy, and stale;
- delivery failures, dead letters, connector readiness, storage, retention,
  and security events in an admin view;
- explicit confirmation and immutable events for destructive administration;
- a documented human actor-binding decision before humans can participate with
  durable identity.

Security defaults:

- bind to loopback only;
- enforce Host validation, session authentication, and actor scopes server-side;
- never expose the UI remotely through a flag alone;
- keep message bodies out of client wake channels.

Exit gate:

- a human can join a channel, answer an agent in-thread, and survive browser and
  daemon restart without identity or read-position loss;
- an idle browser generates no polling load;
- a non-member and a forged browser session cannot read channel history.

### 7. Stabilize and ship V2

Required release gates:

- migration tests from every public alpha schema and fixture;
- crash injection around send/fan-out, join/leave, claim/ack, checkpoint, and
  compaction commits;
- property tests for independent recipient state and deterministic replay;
- fuzzing for frames, envelopes, signatures, and migration inputs;
- adversarial peer-message tests for prompt injection and authority confusion;
- three-, five-, and twenty-actor concurrency/soak tests;
- clean install and upgrade tests from the current public release on macOS and
  Linux;
- signed artifacts, SBOM/provenance, checksums, rollback instructions, and a
  compatibility matrix;
- API, threat-model, connector, operator, migration, and recovery documentation.

V2 ships only when the money-shot channel scenario passes from a released
artifact, not merely from a source checkout.

## Success measures

| Area | V2 release threshold |
| --- | --- |
| Delivery | No lost committed messages; retries never create a second durable message or recipient obligation |
| Isolation | Unauthorized channel read, post, claim, membership, and admin operations are rejected in every adversarial case |
| Replay | Channel and thread state reconstruct identically after restart and supported compaction |
| Recovery | Crash at every claim/ack and membership boundary recovers without cross-member state corruption |
| Attention | At least 95% of wake-requested messages are processed within 20 seconds after the recipient can take a turn; unsupported wake is explicit |
| Efficiency | No acknowledgement-of-ack loops; terminal/FYI messages do not wake every member; fan-out work grows linearly and predictably |
| Operability | One-command setup and upgrade work from release artifacts; failures identify the broken layer and remediation |
| Storage | Configured retention bounds sustained channel traffic without deleting protected state |

## Experiments to run before broadening scope

1. **Channel money shot:** the three-member/non-member/crash scenario above.
2. **Chatter calibration:** useful broadcast versus unnecessary wakes across
   three and five agents; compare `requires_reply`, mentions, and coalescing.
3. **Actor continuity:** renamed or rebound actors, cloned config, handoff, and
   same-actor concurrent sessions.
4. **Human participation:** measure whether a small chat surface reduces routing
   friction before building a full dashboard.
5. **Optional work registry:** multiple open obligations, stale artifact review,
   and crash/handoff—conditions where the earlier experiment says task state may
   become worth its cost.
6. **Supervised runtimes:** prove that app-server/native adapters materially
   improve attention reliability before replacing current connector paths.

Each expensive behavioral experiment starts only after deterministic protocol,
permission, lifecycle, and state-changing connector canaries pass.

## Explicitly deferred beyond V2.0

- multi-node routing, replication, and remote wake;
- cross-OS-user or hosted multi-tenancy;
- a bundled task/work/review registry;
- autonomous organizational policy or release authority;
- voice/video, file storage, and general-purpose team-chat features;
- quotas until measured actor-level resource contention justifies them.

Multi-node research may begin after an external authenticated identity provider,
partitioned replay, export/restore, and bounded retention are proven locally.
Its entry experiment must test one partition moving between two nodes,
authenticated remote wake, network partition recovery, and preserved per-
partition ordering. It must not be implemented by sharing SQLite or exposing
the local Unix-socket protocol over TCP.

## Recommended issue structure

Create one epic per numbered milestone and label issues by workstream:
`protocol`, `identity`, `channels`, `storage`, `sdk`, `connector`, `web`,
`security`, `release`, and `experiment`. Every issue should name its observable
acceptance test, migration impact, protocol compatibility, and whether it
changes a frozen connector tool surface.

The critical path is:

```text
alpha hardening
  -> actor binding and protocol negotiation
  -> channel membership and atomic fan-out
  -> replay and read positions
  -> retention and observability
  -> client/connector conformance
  -> released-artifact channel experiment
  -> V2.0
  -> human web surface
  -> V2.1
```
