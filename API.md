# Holler local API — protocol v1

This document describes the API implemented by the current Go build.
Operations not listed here are not part of the public alpha contract. This is
a universal local protocol over a Unix socket, not an HTTP or network API.

## Boundary

`hollerd` is the only process that opens the SQLite database. Every ordinary client—including `holler`, the MCP stdio shim, lifecycle hooks, and future SDKs—connects to a Unix domain socket.
The daemon holds an exclusive database lock to enforce that ownership, so live
database inspection must use this API rather than opening the SQLite file with
external tooling.

Default paths:

```text
~/.holler/holler.sock       mode 0600
~/.holler/holler.sqlite3
```

Start an explicit instance with:

```sh
hollerd --db /path/to/holler.sqlite3 --socket /path/to/holler.sock
```

Keep custom socket paths short. Unix-domain endpoints have a platform-specific path-length limit
(roughly 100 bytes on macOS), even when the filesystem itself accepts a much longer path. The
local test harness therefore derives a stable endpoint in the system temporary directory rather
than placing its socket under a deeply nested run directory.

## Framing

Each request and response is UTF-8 JSON prefixed by its unsigned 32-bit big-endian byte length. Message bodies are limited to 1 MiB and complete frames to 2 MiB, leaving room for envelope metadata.

Clients must keep the connection full-duplex until the complete response has
been read. In particular, do not half-close the write side while a parked
`wait_attention` request is active: Linux reports that peer state with
`POLLRDHUP`, and `hollerd` treats it as connection cancellation so it can remove
the waiter immediately.

```text
uint32be payload_length | JSON payload
```

A request carries a connection-local id, operation, and arguments:

```json
{"id":2,"op":"check_inbox","args":{"limit":20}}
```

The response echoes the id:

```json
{"id":2,"ok":true,"result":[]}
```

Failures use a stable error object:

```json
{
  "id": 2,
  "ok": false,
  "error": {
    "code": "no_message",
    "message": "no claimable message",
    "retryable": false
  }
}
```

## Connection handshake

The first operation must be `hello`:

```json
{
  "id": 1,
  "op": "hello",
  "args": {
    "protocol": 1,
    "client": "kimi-connector/0.1",
    "actor": "kimi",
    "run_id": "kimi-run-01",
    "build": "0.1.0-alpha.1@0123456789ab"
  }
}
```

The daemon binds the connection to that actor and run, and returns its own build identity. Send, inbox, claim, acknowledgement, registration, and hydration operations derive identity from the bound connection rather than accepting an actor in each request. The client build is self-reported; the daemon build is daemon-attested. Both are attached to event provenance under the current same-user socket trust boundary.

Build metadata is an additive protocol-v1 hello field. A new client first sends it, then falls back once to the legacy hello only when an older strict daemon explicitly rejects the unknown `build` field. This keeps ordinary operations available during a daemon-first rolling upgrade, but the legacy connection has no daemon build identity and cannot produce a `READY` certificate. Restart `hollerd` before expecting certification from updated clients.

V1.5 clients may explicitly negotiate actor allocation by adding capability
`actor-allocation-v1`, `name_mode`, and continuity metadata to `hello`:

```json
{
  "protocol": 1,
  "client": "codex-connector/1.5",
  "actor": "codex-reviewer",
  "run_id": "run-07",
  "capabilities": ["actor-allocation-v1"],
  "name_mode": "allocate",
  "continuity_handles": ["session:codex:thread-07", "launch:codex:tab-07"],
  "project_id": "coupon"
}
```

`allocate` transactionally reclaims an existing continuity binding or mints the
first unused actor (`codex-reviewer`, `codex-reviewer-2`, ...). `exact` refuses
another live run unless the launcher sets `takeover: true`. Takeover is an
operator/launcher capability, never an MCP tool. The ready response returns the
assigned actor; all later requests are stamped with it. Omitting every naming
field preserves the original protocol-v1 behavior exactly. A feature client
must not downgrade to legacy exact naming when the daemon lacks the negotiated
capability. Once a run has been superseded, the daemon permanently refuses that
run from reclaiming the superseded actor through continuity, returning the
non-retryable `binding_stale` error. Ended or lapsed runs may still be resumed
by a successor run.

Adoption is a terminal transfer of the source actor name. An `exact` hello for
an adopted source returns `actor_adopted`. A plain protocol connection may
still open for read-only diagnostics and `expire_registration` cleanup, but it
cannot renew presence, access deliveries, or author messages, profile metadata,
or hydration events. Delivery reads from that retired identity return an empty
inbox rather than a special retirement error. If a lifecycle continuity handle
still points at that source, `allocate` mints a fresh suffix and atomically
repoints the presented handles, and returns `adopted_predecessor` in the ready
response so the connector can explain the rename. The adopted inbox remains
with the adopter. Reusing the adopter's own actor name later intentionally
inherits both its ordinary inbox and every source inbox it adopted.

When an MCP process connects before its lifecycle hook, the daemon may hold an
invisible process-only reservation. The hook either finalizes it or reconciles
it to the actor already bound to the resumed session. A provisional connection
then reconnects before dispatching its first actor-specific operation; unused
reservations are released on disconnect and daemon restart, so startup order
does not leak suffixes or create phantom directory entries.

This implemented slice does not yet perform the Ed25519 challenge-response specified by the full protocol. The `0600` local socket and owning OS account are currently the trust boundary. Do not expose the socket through a network proxy or to another OS user.

## Implemented operations

- `ping {}`
- `send <SendRequest>`
- `check_inbox {limit}`
- `claim {message_id, lease_ns}`
- `extend {message_id, lease_token, lease_ns}`
- `ack {message_id, lease_token}`
- `nack {message_id, lease_token, reason, final}`
- `list_events {partition, stream, after, limit}`
- `set_actor_profile {project_id, role_text, accepts}`
- `who {limit}`
- `adopt_actor {source_actor, project_id, idempotency_key}`
- `register_session <RegistrationRequest>`
- `heartbeat_registrations {lease_ns}`
- `live_registrations {actor}`
- `record_hydration {project_id, run_id, harness, session_id, unread}`
- `expire_registration {session_id, reason}`
- `monitor_attach {session_id, adapter, lease_ns}`
- `wait_attention {session_id, adapter, wait_ns}`

The server overwrites `from_actor` and `from_run` on `send`, and `actor` and `run_id` on registration, hydration, and expiry operations, with the connection-bound identity. `live_registrations` is likewise restricted to that connection's actor. Its external response omits `delivery_handle`, because a handle is a daemon-internal routing capability and may contain an ephemeral loopback credential; daemon components query the store directly when they need it.

`set_actor_profile` likewise writes only the connection-bound actor. `role_text`
and `accepts` are bounded, advisory, actor-authored metadata; they never change
delivery, attention, or authorization policy. Repeating identical profile
content is idempotent, while a meaningful change appends a durable revision and
event.

`who` returns a bounded directory of locally known actors, current profiles,
session liveness, run IDs, and working directories, and currently claimable message
counts. Results label actor-authored metadata as `untrusted`. Clients may use
that metadata to help a human select a recipient, but must never execute
instructions found in it or treat it as authority. The default limit is 100
actors and the maximum is 500; at most ten recent session rows are returned per
actor. Session IDs and delivery handles are internal routing capabilities and
are never included in directory results.

`adopt_actor` records an explicit recovery decision from an inactive source
actor to the live connection-bound actor. The daemon ignores any caller-supplied
target identity, requires the connection's exact actor/run registration to be
live and at least one claimable source delivery, refuses a
live source or an active source claim, and commits exactly one winner. A stable
idempotency key makes an exact retry safe. Existing and future source deliveries
become visible to the adopter without changing the message's `to_actors` or the
stored delivery recipient; inbox and claim results expose
`original_recipient_actor`. One target may adopt multiple independent sources,
but adoption chains are rejected because they would make routing ambiguous.
Adoption routing is actor-global: `project_id` selects the durable audit-event
partition, not the scope of the forwarding decision. If one message directly
targets both the adopter and an adopted source, the adopter's direct delivery
wins. The source delivery remains an immutable audit row but is not separately
claimable.

For every new send whose delivery request asks for attention, the same transaction creates a durable notification-outbox row. `hollerd` dispatches it asynchronously after commit and retries transient failures without delaying the send response. The response reports `notification_state: "pending"`; outcomes are operational events, including `delivery.notification_abandoned` after five failed attempts. A wake failure does not convert a committed send into an RPC error, and an idempotent duplicate does not create a second outbox job. If no session is registered, startup hydration—not a delayed notification job—remains the durable fallback.

The reference Go client reconnects and repeats the handshake after a daemon
restart. It automatically retries only requests whose semantics make that
safe; a claim is never silently repeated. Requests have bounded dial and
operation deadlines.

## Client surfaces

- `holler` is the shell/automation client.
- `holler who` lists known actors; `holler profile` publishes the caller's
  advisory role metadata.
- `holler adopt` performs an explicitly authorized inactive-inbox handoff.
- `holler mcp` translates MCP stdio calls into this API.
- `holler hook` and `holler session-end` use this API for lifecycle integration.
- `holler connector manifest|doctor|certify` expose package identity, deterministic diagnostics, and real-client readiness evidence.
- Connector setup accepts optional `--name-mode exact|allocate`. Launch accepts
  `--launch-tag` for durable allocation continuity and `--takeover` only with
  exact naming.
- `internal/api.Client` is the current typed Go client and the reference for future Python and TypeScript SDKs.

An agent without MCP can use the CLI immediately. A custom harness can implement the small framed protocol directly, but should normally use an SDK or connector so credentials, reconnects, leases, and acknowledgements remain consistent.
