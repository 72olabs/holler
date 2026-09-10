# Claude Channel Attention Plan

Status: implementation branch (`feature/claude-channel`)

## Objective

Deliver a public Holler attention path for SDK-style sessions while evaluating
Claude Code Channels as an experimental transport. The SDK `asyncRewake` hook
path has been tested and rejected; the next public-path candidate is an
ID-only, host-injected wake owned by T3.

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
  -> exact actor/run/session attention attachment
  -> T3 in-process waiter -> fixed data-free synthetic SDK input (public path)
     or Holler MCP -> notifications/claude/channel, ID only     (experimental)
  -> Claude synthetic turn
  -> bus_inbox claim
  -> agent processes and optionally replies
  -> bus_ack with lease token
```

The internal Claude attention adapters will be:

- `hook-long-poll`: interactive Claude CLI sessions supervised by Holler's
  existing hook monitor.
- `host-injected`: SDK hosts such as T3 that own both the streaming query and
  an exact Holler attention waiter.
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
- Host-injected wake text is Holler-generated, fixed, contains no message data,
  and is explicitly
  labeled synthetic/agent-originated in both the SDK input and T3 UI. It must
  never be rendered or persisted as human-authored input.

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
  3.7 seconds. Controlled query-close canaries ended cleanly, but a separate
  manual shell workaround left a monitor orphan because its reparented shell
  kept the output pipe open after Claude exited. That monitor renewed a phantom
  live registration until it was explicitly terminated.

Keep the SDK guard unchanged. A T3 environment-only change cannot provide the
required live wake. Do not ship the experimental opt-in.

### 3. Prove host-injected wake as the public SDK path

There is no protocol blocker to a host-injected wake, provided the host owns
the SDK query and the attention wait, and the injected frame contains no
peer-authored data. The proof of concept should use the existing broker and
exact attachment checks rather than a new delivery subscription.

Holler changes:

- Add a small host-facing attention wait API/CLI that attaches to one exact
  actor, run, and session, blocks for an attention notice, and emits structured
  JSON containing only `{message_id}`. This is a strict projection of the
  broker's internal `AttentionNotice`: thread, sender, type, delivery request,
  and all other peer-controlled fields must not cross the host API. The waiter
  must never claim messages, inject content, or manufacture a registration
  after startup grace.
- Treat the launch handle as correlation only, never as authority. It is present
  in Claude's environment and can therefore be read by model-spawned processes.
  When a trusted lifecycle hook or MCP attachment binds the harness instance,
  record the daemon-verified Claude PID and process start time, plus Claude's
  direct-parent PID and start time, with the exact actor/run/session
  registration.
- On host attach, use the handle only to find the candidate registration, then
  verify the connecting peer's PID and start time through the daemon. They must
  exactly equal the recorded direct-parent PID and start time, and PID 1 is
  never eligible. A grandparent, sibling, Claude itself, or descendant such as
  model-spawned Bash must be rejected even when it presents the correct handle.
  Comparing start times makes PID reuse fail closed.
- A later opaque attachment token may be minted only after this host ancestry
  proof succeeds. It must be bound to the connection and must never be placed
  in Claude's environment.
- Make cancellation authoritative: when T3 closes the query or waiter pipe, the
  host attachment detaches immediately. A host attachment never creates,
  renews, or extends registration presence; the MCP heartbeat remains the
  registration lease authority.

T3 changes:

- Connect the T3 server itself to the framed host-attention protocol over the
  Unix socket. Do not spawn `holler attention wait` for the product integration:
  that CLI would be Claude's sibling and cannot satisfy direct-parent identity.
  Keep the CLI only as a lab/test client.
- Start one in-process waiter with the long-lived Claude query, cancel it before
  or with `query.close()`, and restart it only after the exact session reattaches.
- On a notice, enqueue the fixed input `Holler has unread messages; call
  bus_inbox`. Keep the message ID only in host state for deduplication; do not
  put it or any body, sender, type, or thread in the SDK input.
- Construct the SDK input with `isSynthetic: true`,
  `origin: {kind: "peer", from: "holler"}`, no `fromMode`,
  `priority: "later"`, and `shouldQuery: true`. The fixed `from` value names
  the integration, not the actual sending actor. Preserve this provenance
  after query resume and render only a system event in T3, never a user message.
- Maintain at most one pending synthetic wake per session. Coalesce additional
  notices while it is pending, and inject at most once per message ID for the
  lifetime of one attachment as a loop guard. The later inbox drain, not the
  number of injected frames, determines the work to process.
- While Claude is busy, queue and coalesce wake hints without claiming or
  dropping durable messages. A subsequent `bus_inbox` call remains the only
  authority for what Claude processes.

Reject this public path if the tested SDK cannot preserve synthetic provenance,
if injection interrupts or impersonates the user, or if waiter cancellation
can leave a live attachment. Claude Channel remains the experimental fallback.

### 4. Harden hook-monitor process liveness

The output pipe is not sufficient proof that Claude is alive. A shell or `cat`
process can inherit the descriptor, become reparented to launchd, and keep a
monitor renewing a phantom registration after the harness exits.

- Bind the monitor and host attachment to the verified Claude process identity
  recorded during API attachment, expose that identity without trusting
  caller-supplied metadata, and cancel them when that process exits. Use one
  shared process-liveness primitive: on macOS prefer a kqueue `NOTE_EXIT`
  watch; retain a bounded portable polling fallback.
- Do not let a standalone monitor create a replacement registration after
  startup grace unless daemon-verified lifecycle-hook provenance authorizes the
  fallback. A host attention waiter never self-registers.
- On ancestor exit, detach the waiter and expire the exact registration rather
  than waiting for passive lease timeout.
- Add a regression lab that kills Claude while a descendant deliberately holds
  the result pipe open. The monitor and descendants must exit, and status must
  stop reporting live presence within the teardown bound.

### 5. Keep Claude Channel behind a development flag

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

### 6. Reuse the attention broker for Channel dispatch

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

### 7. Make readiness host-attested and user-simple

Holler cannot infer Channel readiness from a successful stdout write. T3 must
report successful activation of the exact configured Holler server and Holler
must observe the matching live attachment. For `host-injected`, `READY` requires
the daemon to have admitted a live host connection whose PID and start time
exactly match Claude's recorded direct parent for that registration; knowing a
launch handle or starting a waiter is not readiness evidence. If the SDK cannot
distinguish Channel policy denial from activation, report wake as unverified/off
until a canary event is claimed; never infer `READY`.

Keep detailed states internal. Present one user concept with one remediation:

- `Live wake: ready (hook monitor)`, `Live wake: ready (host injected)`, or
  `Live wake: ready (Claude Channel)`.
- `Live wake: off. Messages will arrive at next session start. Fix: <action>`.

The host selects the transport. Users do not choose `hook-long-poll` versus
`claude-channel`, and one session must never activate both.

## Delivery sequence

1. **Identity correctness:** fix and regression-test MCP-first SessionStart
   reconciliation.
2. **Launch-path experiment (complete, rejected):** the SDK async-rewake hook
   cannot retain a parked monitor after a T3 turn result.
3. **Host-injected proof:** add the exact, cancellable attention-wait API and
   prove fixed, data-free synthetic injection in T3.
4. **Monitor liveness hardening:** tie hook monitor lifetime to its verified
   Claude ancestor and remove unverified self-registration fallback behavior.
5. **Experimental Channel vertical slice:** finish ID-only broker dispatch,
   host-attested readiness, bounded unclaimed recovery, and protocol/security
   tests behind the development flag.
6. **Revisit public Channels later:** only after Anthropic offers a viable
   third-party distribution path and the combined packaged canary passes.

Holler and T3 changes should remain separate commits or pull requests linked to
this contract. Identity reconciliation and the rejected SDK experiment are now
complete. The next code slice is the host attention-wait API plus a T3 proof of
concept. Channel dispatch remains behind the development flag.

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
- Connector/API/store validation accepts only implemented modes; adding
  `host-injected` requires an explicit allowlist and downgrade tests.
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
- Controlled `query.close()` calls expired their registrations. A separate
  shell-backgrounded monitor outlived Claude because a reparented shell kept
  its result pipe open, renewed a phantom registration, and required explicit
  termination. This invalidates the earlier blanket zero-orphan claim.
- Hook output triggered an immediate continuation, so using heartbeat or
  diagnostic output to hold the channel open would create a wake loop rather
  than a stable attachment.
- One-shot `sdk-cli`, `sdk-ts`, and `sdk-py` commands retain the current guard
  and are never kept open by Holler.
- The SDK hook path is rejected for T3 live wake. No
  `HOLLER_CLAUDE_LIVE_WAKE` option is shipped.

### Host-injected wake proof

- The host waiter cannot attach with a stale actor, run, session, launch
  handle, or ended registration.
- A model-spawned Bash process presenting the correct launch handle is rejected.
  So are a waiter CLI spawned by T3, a process spawned by a neighboring
  `codex app-server`, and a grandparent such as the T3 app, a shell, or tmux.
  Only T3's in-process connection, whose peer PID and start time exactly equal
  Claude's recorded direct parent, is admitted; PID 1 and a reused PID with a
  different start time are rejected.
- The launch handle performs lookup only. Any opaque token is minted after the
  ancestry check, remains outside Claude's environment, and expires with the
  host connection.
- The public host notice is exactly `{message_id}`. Broker thread, sender, type,
  delivery-request, and message-body fields are absent even when populated with
  hostile values.
- Closing the T3 query cancels the waiter before readiness can renew; keeping
  stdout open cannot manufacture or extend presence. The host attachment never
  heartbeats or renews the registration lease.
- Kill Claude while the waiter's output pipe remains open. The waiter exits and
  the exact registration becomes non-live through the shared `NOTE_EXIT`
  watcher within the bounded teardown window.
- An idle query receives one fixed, data-free synthetic input, claims and
  acknowledges the corresponding inbox item, and produces one agent turn.
- The SDK frame has `isSynthetic: true`, fixed peer origin `holler`, no
  `fromMode`, `priority: "later"`, and `shouldQuery: true`. Those fields remain
  non-human after query resume, and T3 records a system event rather than a user
  message.
- Busy queries queue multiple notices without interruption; the next synthetic
  turn drains durable inbox state once with no loss, replay, or reordering. At
  most one synthetic wake is pending per session, and each message ID causes at
  most one injection per attachment.
- T3 renders the wake as a system/agent event and the transcript never labels
  it human-authored. Peer body, sender, type, and thread metadata do not appear
  in the injected SDK input.
- Daemon restart, T3 restart, query close/reopen, and waiter reconnect preserve
  durable messages and never leave duplicate waiters.
- All real-client canaries use a temporary socket and database. No experiment
  may register actors or conditions in the operator's production daemon.
  Teardown uses exact existing identities and must not allocate replacement
  actors while attempting cleanup.

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

Use a fresh isolated Git repository, daemon socket/database, T3 profile, and
Claude session assigned the `reviewer-holler` alias. Never point this canary at
the operator's production `~/.holler` state.

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
7. Exercise missing/stale host attachment and, for Channel, policy-blocked
   activation; prove the UI/status says startup-only rather than ready.
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
