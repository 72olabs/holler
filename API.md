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

For every new send whose delivery request asks for attention, the same transaction creates a durable notification-outbox row. `hollerd` dispatches it asynchronously after commit and retries transient failures without delaying the send response. The response reports `notification_state: "pending"`; outcomes are operational events, including `delivery.notification_abandoned` after five failed attempts. A wake failure does not convert a committed send into an RPC error, and an idempotent duplicate does not create a second outbox job. If no session is registered, startup hydration—not a delayed notification job—remains the durable fallback.

The reference Go client reconnects and repeats the handshake after a daemon
restart. It automatically retries only requests whose semantics make that
safe; a claim is never silently repeated. Requests have bounded dial and
operation deadlines.

## Client surfaces

- `holler` is the shell/automation client.
- `holler who` lists known actors; `holler profile` publishes the caller's
  advisory role metadata.
- `holler mcp` translates MCP stdio calls into this API.
- `holler hook` and `holler session-end` use this API for lifecycle integration.
- `holler connector manifest|doctor|certify` expose package identity, deterministic diagnostics, and real-client readiness evidence.
- `internal/api.Client` is the current typed Go client and the reference for future Python and TypeScript SDKs.

An agent without MCP can use the CLI immediately. A custom harness can implement the small framed protocol directly, but should normally use an SDK or connector so credentials, reconnects, leases, and acknowledgements remain consistent.
