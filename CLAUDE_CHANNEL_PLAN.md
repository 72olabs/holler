# Claude Channel Attention Plan

Status: implementation branch (`feature/claude-channel`)

## Objective

Add Claude Code Channels as an explicit Holler attention transport so SDK-style
Claude sessions can wake while idle without the hook monitor that interactive
CLI sessions use today.

This is an attention-path change, not a change to Holler's delivery contract.
The durable inbox, claim lease, processing, reply, and acknowledgement remain
authoritative. A successfully written Channel notification is only a wake hint;
it is not proof that Claude processed the message.

Claude Channels and Holler channels are separate concepts. The former is a
client-specific wake transport. The latter is the membership-enforced messaging
work planned for Holler V2.

## End-to-end contract

```text
hollerd durable outbox
  -> exact actor/run/session Channel attachment
  -> Holler MCP server
  -> notifications/claude/channel (message ID only)
  -> Claude synthetic turn
  -> bus_inbox claim
  -> agent processes and optionally replies
  -> bus_ack with lease token
```

The supported Claude attention modes will be:

- `hook-long-poll`: interactive Claude CLI sessions supervised by Holler's
  existing hook monitor.
- `claude-channel`: hosts using the Claude Agent SDK, or another host that
  explicitly enables Holler's Channel-capable MCP server.
- `startup-only`: durable hydration with no live wake transport.

No runtime may advertise `claude-channel` readiness merely because the Holler
MCP server supports the protocol. Readiness requires the host to enable the
specific trusted Holler server and the daemon to observe the resulting live
attachment.

## Safety invariants

- Never place a Holler message body in a Channel notification. Send a
  server-generated durable message ID plus fixed fetch instructions only.
- Treat Channel content and metadata as untrusted input, never as a human
  instruction or an authorization decision.
- Enable only the configured Holler MCP server; never auto-enable arbitrary
  third-party Channel servers.
- Route notifications to an exact actor, run, and session attachment. An alias
  may resolve the recipient before dispatch but must not identify a live
  transport by itself.
- Preserve Holler's claim/ack semantics. Claude Code currently does not
  acknowledge Channel notification delivery.
- A failed, unavailable, or policy-blocked Channel activation must be visible
  and must fall back truthfully to `startup-only`; it must not report `READY`.
- Existing hook-long-poll and startup-only behavior must remain unchanged.

## Implementation work

### 1. Holler MCP foundation

- Advertise `capabilities.experimental["claude/channel"]` only when the
  connector explicitly enables Channel mode.
- Return fixed Channel-specific instructions in the MCP initialize response.
- Serialize every JSON-RPC response and asynchronous Channel notification
  through one writer so concurrent output can never corrupt the stdio stream.
- Add an internal notification API whose payload is generated from durable
  server state and contains only the message ID.

The first branch slice implements conditional capability advertisement and the
serialized writer without enabling Channels in normal installs.

### 2. Daemon attachment and dispatch

- Add `claude-channel` to connector, API, and store validation.
- Associate each live Channel attachment with the exact actor/run/session that
  owns the MCP transport.
- Subscribe the MCP process to committed deliveries for that identity and
  emit one Channel notification per eligible durable message.
- Define reconnect and cancellation behavior for MCP exit, daemon restart,
  Claude/T3 restart, and stale session expiry.
- Make repeated notifications idempotent from the agent's perspective. The
  inbox claim remains the arbiter if the transport retries.

Before launch, decide whether the current attention broker is sufficient for
the preview or whether Channel dispatch needs a leased/two-phase acceptance
record. In either design, transport acceptance must remain distinct from
message acknowledgement.

### 3. Registration, setup, and diagnostics

- Record `claude-channel` only when the host deliberately opts in.
- Teach setup, manifests, package metadata, doctor, status, and certification
  about the new mode.
- Distinguish these states in user-facing output:
  `CAPABLE`, `POLICY_BLOCKED`, `ENABLED_NO_ATTACHMENT`, `READY`, and
  `STARTUP_ONLY`.
- Keep `hook-long-poll` the default for plain interactive Claude Code until the
  Channel path is supported and released.
- Update connector skills and public documentation only after packaged
  real-client certification passes.

### 4. T3 launcher integration

A local proof of concept may use Claude launch arguments to enable the trusted
Holler development Channel. Public T3 support requires a small code change:

- After creating the Agent SDK query, wait for initialization and inspect MCP
  server status.
- Resolve the exact configured Holler MCP server and verify that it advertises
  the Channel capability.
- Call the SDK's Channel activation method for that server before T3 reports
  Holler live-wake readiness. The T3-bundled SDK runtime exposes
  `enableChannel(serverName)` even though the currently published TypeScript
  declaration may require a narrow local augmentation.
- If activation is rejected or blocked by policy, surface the reason and use a
  truthful startup-only registration (or fail the explicitly requested launch).
- Preserve Channel origin metadata in T3's synthetic turn and render it as an
  agent/system event, not as a human-authored message.
- Do not enable every discovered Channel server.

The proof of concept should use the development-channel flag only in an
isolated test profile. Release builds should use Anthropic's supported
allowlist/policy path.

## Delivery sequence

1. **Protocol-safe foundation**: this branch adds the design, opt-in MCP
   capability, serialized output, and unit tests. No user-visible mode exists.
2. **Holler Channel vertical slice**: add the live attachment, ID-only
   notification, daemon dispatch, mode validation, and integration tests behind
   an experimental switch.
3. **T3 proof of concept**: enable the exact Holler server in a local T3 build
   and prove an idle SDK session starts a synthetic turn.
4. **Durability and readiness hardening**: exercise restart/retry paths and make
   diagnostics reflect actual attachment state.
5. **Packaged real-client certification**: test the exact Holler artifact,
   Claude Code/Agent SDK version, and T3 build intended for release.
6. **Policy, packaging, and docs**: secure Channel allowlisting, update setup
   and connector packages, and publish limitations for the research-preview
   feature.

Holler and T3 should land as separate pull requests linked to the same protocol
contract. The Holler experimental vertical slice lands first; T3 consumes it;
the Holler mode is made generally selectable only after the combined canary
passes.

## Test plan

### Unit and protocol tests

- Channel capability and instructions are absent by default and present only
  when explicitly enabled.
- Concurrent responses and notifications remain valid, non-interleaved JSONL.
- Channel payloads contain a durable message ID and fixed instructions, never
  the message body.
- Connector/API/store validation accepts only the three documented modes.
- Exact actor/run/session routing rejects stale or mismatched attachments.
- Repeated notification attempts cannot create duplicate durable messages.

### Holler integration tests

- Idle attached recipient receives one wake and can claim/ack the message.
- A busy recipient queues Channel events and later processes them in order.
- Two rapid messages produce no loss, cross-thread routing, or duplicate
  processing.
- Alias resolution targets only the current actor and its exact attachment.
- Daemon restart before and after dispatch preserves committed delivery.
- MCP transport restart and lease expiry recover without message loss.
- T3/Claude restart hydrates unread work once and reattaches cleanly.
- Policy denial and unsupported clients register startup-only and explain why.
- Existing hook-long-poll certification remains green.

### Security tests

- Hostile message bodies never appear in Channel notification frames.
- Forged/malformed Channel metadata cannot select an actor, run, or session.
- Channel-originated turns are never labeled as human input.
- A second Channel-capable MCP server is not activated automatically.
- Session/alias collisions cannot wake or expose another inbox.
- Logs, diagnostics, and protocol traces do not reveal message bodies or
  secrets beyond the durable inbox's existing access boundary.

### Launch-blocker real-client canary

Use a fresh isolated Git repository, daemon database, T3 profile, and Claude
session assigned the `reviewer-holler` alias.

1. Verify the exact T3, Agent SDK, Claude Code, Holler, connector, and daemon
   build identities.
2. Start Claude through T3, verify Channel activation for the exact Holler MCP
   server, and leave the session idle.
3. From `coder-holler`, send message A. Claude must start exactly one synthetic
   turn, claim A, acknowledge it, and reply without human input.
4. Leave Claude idle and send message B. It must wake once with no replay of A.
5. While Claude is busy, send messages C and D. Both must remain durable and be
   processed once in the documented order.
6. Repeat across a daemon restart, an MCP reconnect, and a complete T3/Claude
   restart.
7. Exercise policy-blocked activation and prove the UI/status says startup-only
   rather than ready.
8. Finish with empty inboxes, no active claims, zero orphan monitor processes,
   no lost/duplicated/misrouted messages, and retained provenance for every
   reply.

## Release gates

- Zero message loss, duplicate processing, cross-session wake, or misrouting in
  the canary and automated suite.
- `READY` means a live attachment was observed, not just static capability.
- Busy, restart, and policy-blocked paths have explicit passing tests.
- Minimum and latest supported Claude/T3 versions pass.
- Hook-long-poll and startup-only regression suites pass unchanged.
- The exact packaged artifact passes the real-client canary.
- Anthropic preview/allowlist requirements and T3 compatibility are documented
  as release constraints.

## External dependencies and risks

- Claude Channels is a research-preview surface and its allowlist/policy rules
  may change.
- The SDK runtime activation method and its public TypeScript declarations are
  not currently aligned in every version.
- A successful stdio notification write is not a client acknowledgement.
- Public readiness depends on both a Holler release and a compatible T3 release;
  configuration alone is suitable only for the local proof of concept.

