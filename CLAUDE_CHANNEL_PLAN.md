# Claude Channel Attention Plan

Status: implementation branch (`feature/claude-channel`)

## Objective

Evaluate Claude Code Channels as an experimental Holler attention transport for
SDK-style sessions while first testing whether the SDK's existing
`asyncRewake` hook support can provide the public live-wake path with a smaller,
safer integration.

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

The internal Claude attention adapters will be:

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
- Never declare `claude/channel/permission`. Holler is an agent-message
  transport, not a remote tool-approval authority.
- Enable only the configured Holler MCP server; never auto-enable arbitrary
  third-party Channel servers.
- Route notifications to an exact actor, run, and session attachment. An alias
  may resolve the recipient before dispatch but must not identify a live
  transport by itself.
- Preserve Holler's claim/ack semantics. Claude Code currently does not
  acknowledge Channel notification delivery.
- A failed, unavailable, policy-blocked, or unverifiable Channel activation
  must fall back truthfully to `startup-only`; Holler cannot detect policy
  denial from its stdout write and must not report `READY` without host evidence.
- Existing hook-long-poll and startup-only behavior must remain unchanged.

## Decision and implementation order

The public launch path and the Claude Channel experiment are separate tracks.
The launch path should use the smallest supported transport that passes the
real T3 lifecycle matrix. Claude Channel remains experimental while Anthropic's
research-preview allowlist excludes third-party plugins for ordinary Pro and
Max sessions.

### 1. Fix MCP identity rebinding first

The MCP process starts without Claude's `session_id`. In allocate mode it can
therefore hold a provisional actor/run binding until SessionStart supplies the
daemon-proven harness instance and session continuity. The API client follows
that reconciliation, but an MCP tool can read the old bound actor immediately
before another goroutine changes the client identity. Its following
actor-validated call then fails with `actor: does not match the authenticated
API session`.

- Treat the MCP identity lookup as a snapshot, not an atomic transaction with
  the daemon call. Retry only the typed pre-operation identity-rebound error
  emitted by the local API guard after fetching the new binding. Never retry an
  operation whose commit status is ambiguous or a daemon-returned lookalike.
- Keep the provisional reservation invisible and do not attach attention until
  SessionStart has finalized the canonical actor/run/session registration.
- Add deterministic tests that reconcile between identity lookup and inbox,
  claim, ack, profile, heartbeat, and future attention attachment calls.
- Require the first `bus_inbox` after SessionStart reconciliation to succeed.

This race affects both hook and Channel transports, so no wake-path experiment
is meaningful until it is fixed.

Follow-up hardening after the launch-path experiment:

- Restart the complete `bus_inbox` operation once if its check and claim phases
  straddle an identity rebind; never return a misleading empty result for the
  new actor after silently skipping the old actor's message IDs.
- Translate ack, extend, and nack failures caused by a post-claim rebind into an
  explicit `identity changed since claim` diagnostic rather than exposing a
  bare lease mismatch.

### 2. SDK async-rewake guard-bypass experiment: rejected

The Claude Agent SDK bundled by current T3 supports `asyncRewake` hooks. The
existing Holler `hooks.json` already uses that contract. Holler's wrapper exits
early for all `sdk-cli`, `sdk-ts`, and `sdk-py` entrypoints to protect one-shot
queries from being held open by the parked monitor.

The real Agent SDK canary rejected the proposed `HOLLER_CLAUDE_LIVE_WAKE=1`
host opt-in:

- Bypassing the SDK guard for the SessionStart monitor produced neither SDK
  `init` nor a model `result` in 120 seconds. Calling `query.close()` then ended
  the registration and monitor in about two seconds with no orphan.
- Restricting the bypass to the Stop hook restored prompt results (5.2 seconds
  in the query-close canary), but the hook result channel closed with the turn.
  No monitor remained attached while the query was idle, and a wake-requested
  message stayed durable and unclaimed.
- SDK hook-event traces showed Stop starts before the result and completes as
  the result channel closes. Diagnostic output from the hook triggered an
  immediate synthetic continuation, proving `asyncRewake` reacts to output but
  does not keep a silent long poll alive across idle turns.
- The identical control query with the shipping guard completed normally in
  3.7 seconds. Every canary ended its registration and left zero orphan monitor
  processes.

Keep the SDK guard unchanged. A T3 environment-only change cannot provide the
required live wake. Continue with the Claude Channel vertical slice; do not ship
the experimental opt-in.

### 3. Keep Claude Channel behind a development flag

The protocol-safe foundation on this branch remains useful, but it is not a
public setup option:

- Advertise `capabilities.experimental["claude/channel"]` only under an
  explicit development switch.
- Never declare `claude/channel/permission`; Holler peers cannot approve local
  tool prompts.
- Negotiate MCP down to a protocol revision Holler actually implements rather
  than echoing an unsupported client revision.
- Serialize every JSON-RPC response and asynchronous notification through one
  writer.
- Emit required fixed `content` that tells Claude to fetch its durable inbox,
  with only `meta: {message_id}`. Never put a message body or peer-authored
  metadata in the Channel frame.

Custom channels must use Anthropic's dangerous development flag during the
preview unless an organization explicitly places Holler in its managed
`allowedChannelPlugins`. They are unavailable on Bedrock, Google Cloud, and
Foundry. Do not expose `claude-channel` in `holler setup` while those constraints
remain.

### 4. Reuse the attention broker for Channel dispatch

Do not build a second delivery subscription. After SessionStart finalizes the
registration, the Channel-enabled MCP process should resolve that exact
daemon-proven harness instance and canonical run, then:

1. attach with adapter `claude-channel`;
2. call the existing `wait_attention` loop in a goroutine;
3. emit an ID-only Channel notification through the serialized writer; and
4. immediately park the next wait.

Extend API/store adapter validation without weakening exact actor/run/session
matching. A broker acceptance proves only that Holler wrote to MCP stdout.
Claude Code does not acknowledge Channel notifications and may silently drop
them when the server or policy is inactive. Keep an `accepted-but-unclaimed`
timer, raise a visible condition, and allow at most one bounded re-notification
for the same still-unread message and unchanged attachment. Inbox claims remain
the only processing authority.

### 5. Make readiness host-attested and user-simple

Holler cannot infer Channel readiness from a successful stdout write. T3 must
report successful activation of the exact configured Holler server and Holler
must observe the matching live attachment. If the SDK cannot distinguish policy
denial from activation, report wake as unverified/off until a canary event is
claimed; never infer `READY`.

Keep detailed states internal. Present one user concept with one remediation:

- `Live wake: ready (hook monitor)` or `Live wake: ready (Claude Channel)`.
- `Live wake: off. Messages will arrive at next session start. Fix: <action>`.

The host selects the transport. Users do not choose `hook-long-poll` versus
`claude-channel`, and one session must never activate both.

## Delivery sequence

1. **Identity correctness:** fix and regression-test MCP-first SessionStart
   reconciliation.
2. **Launch-path experiment (complete, rejected):** the SDK async-rewake hook
   cannot retain a parked monitor after a T3 turn result.
3. **Experimental Channel vertical slice:** finish ID-only broker dispatch,
   host-attested readiness, bounded unclaimed recovery, and protocol/security
   tests behind the development flag.
4. **Revisit public Channels later:** only after Anthropic offers a viable
   third-party distribution path and the combined packaged canary passes.

Holler and T3 changes should remain separate commits or pull requests linked to
this contract. Identity reconciliation and the rejected SDK experiment are now
complete. The next code slice is daemon Channel dispatch behind the development
flag, followed by explicit T3 Channel activation and readiness evidence.

## Test plan

### Observed v0.7.1 SDK baseline

The `reviewer-holler` canary established the failure state this work must fix:

- An idle `sdk-ts` Claude session did not receive a live wake. The durable
  message remained unclaimed for about 49 minutes and was recovered only when
  a new session reported one unread message during startup hydration.
- The daemon raised `stale_unread` with
  `wake_requested_unclaimed_threshold`, which correctly detected the missing
  processing but did not provide an SDK wake transport.
- No `holler monitor` process existed under the shipping SDK configuration.
  Local source confirms that the plugin wrapper exits immediately for
  `sdk-cli`, `sdk-ts`, and `sdk-py` before invoking the monitor. This establishes
  a missing host integration, not a limitation of the monitor itself.
- A follow-up manual canary launched `holler monitor` as a background task with
  pipe-backed stdout/stderr. It immediately surfaced an approximately
  88-minute-old durable reply, exited with the expected async-rewake status,
  and Claude Code's own background-hook handling caused the SDK session to run
  again. This did not prove that T3 can manage a monitor or inject a turn; it
  proved that the SDK's native `asyncRewake` path works when Holler's wrapper
  does not suppress the monitor.
- The first `bus_inbox` call after startup failed because the actor did not
  match the authenticated API session. `bus_status` observed the new run and a
  retry succeeded without claiming the message twice. Track this MCP/run
  rebinding race separately and require the first inbox call to succeed in the
  selected-path canary.
- No duplicate delivery or recursive Stop continuation was observed.

This baseline means v0.7.1 preserves durable recovery and contains a working
monitor primitive, but the shipping SDK guard leaves T3 without live wake. The
guard-bypass experiment above determines whether native SDK `asyncRewake` is the
small public fix; T3 process management is not assumed.

### Unit and protocol tests

- Channel capability and instructions are absent by default and present only
  when explicitly enabled.
- Unsupported client protocol revisions negotiate down to Holler's implemented
  revision instead of being echoed.
- Channel capability output never includes `claude/channel/permission`.
- Concurrent responses and notifications remain valid, non-interleaved JSONL.
- Channel payloads contain required fixed `content` and only
  `meta: {message_id}`, never the message body.
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

### SDK async-rewake experiment (completed)

- The control `sdk-ts` query completed normally with the shipping guard.
- A SessionStart long poll blocked SDK initialization and result delivery.
- A Stop-only long poll allowed the result but did not survive the idle
  boundary; a wake-requested message remained durable and unclaimed.
- `query.close()` consistently expired the registration and left no orphan
  monitor, including after the 120-second blocked-start timeout.
- Hook output triggered an immediate continuation, so using heartbeat or
  diagnostic output to hold the channel open would create a wake loop rather
  than a stable attachment.
- One-shot `sdk-cli`, `sdk-ts`, and `sdk-py` commands retain the current guard
  and are never kept open by Holler.
- The SDK hook path is rejected for T3 live wake. No
  `HOLLER_CLAUDE_LIVE_WAKE` option is shipped.

### Security tests

- Hostile message bodies never appear in Channel notification frames.
- Holler never advertises `claude/channel/permission` and never receives or
  returns permission decisions.
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
2. Start Claude through T3 with exactly one selected live-wake transport, verify
   its attachment/readiness evidence unambiguously, and leave the session idle.
3. From `coder-holler`, send message A. Claude must start exactly one synthetic
   turn, claim A, acknowledge it, and reply without human input.
4. Leave Claude idle and send message B. It must wake once with no replay of A.
5. While Claude is busy, send messages C and D. Both must remain durable and be
   processed once in the documented order.
6. Repeat across a daemon restart, an MCP reconnect, and a complete T3/Claude
   restart.
7. Exercise missing host opt-in and, for Channel, policy-blocked activation;
   prove the UI/status says startup-only rather than ready.
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
- Custom third-party channels require the dangerous development flag for Pro
  and Max during the preview; managed organizations can explicitly allow their
  own plugin, but this is not a general public distribution path.
- The SDK runtime activation method and its public TypeScript declarations are
  not currently aligned in every version.
- A successful stdio notification write is not a client acknowledgement.
- Public readiness depends on both a Holler release and a compatible T3 release;
  configuration alone is suitable only for the local proof of concept.

## References

- [Claude Code Channels](https://code.claude.com/docs/en/channels)
- [Claude Code Channels reference](https://code.claude.com/docs/en/channels-reference)
