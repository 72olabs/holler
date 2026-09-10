# Proposal: Optional Holler Attention for T3 Claude SDK Sessions

Status: discussion draft only

This document proposes an integration boundary for discussion with the T3
maintainers. It is not an implementation plan approved by T3, and Holler will
not open or push a T3 patch unless the maintainers and the Holler project first
agree on the product behavior and extension point.

## Summary

Holler durably delivers agent-to-agent messages to Claude Code sessions hosted
by T3, but an idle Claude Agent SDK query does not currently have a supported
live-wake path. The message remains safe in Holler's inbox and is recovered at
the next user turn or session start. The proposed optional integration would
let T3 translate an ID-only Holler attention notice into a fixed synthetic SDK
input so an idle session can check its inbox promptly.

The durable Holler inbox remains authoritative. The proposed attention channel
does not carry peer message bodies, claim work, acknowledge delivery, approve
tools, or grant authority to another agent.

## Why this crosses the T3 boundary

Configuration alone cannot establish this wake path. T3 owns both pieces of
runtime state needed to do it safely:

- the exact Claude child process spawned for the SDK query; and
- the in-process SDK prompt queue that can resume that query.

Holler can authenticate the host connection and emit a minimal notice, but it
cannot safely rediscover T3's Claude child or inject an SDK message from outside
the host. Any integration therefore needs either a T3-supported extension point
or an explicitly accepted T3 implementation.

## Proposed contract

When explicitly enabled for a Claude SDK session:

1. T3 retains the PID of the exact Claude child it spawned.
2. T3 opens an in-process connection to Holler's local Unix socket and presents
   only that Claude PID.
3. Holler verifies the PID generation, its daemon-bound actor/run/session, and
   that the connecting process is Claude's recorded direct parent.
4. Holler returns attention notices containing only `{message_id}`. The ID is a
   deduplication hint, not message content and not proof of processing.
5. T3 queues at most one fixed synthetic input while a wake is pending:
   `Holler has unread messages; call bus_inbox`.
6. Claude calls `bus_inbox`, processes the claimed messages, and calls
   `bus_ack`. Existing Holler claim and lease rules are unchanged.
7. T3 cancels the attention connection when the query closes or the Claude
   child exits. The connection never creates or renews session presence.

The synthetic input should retain machine provenance through the SDK and T3
UI. The candidate SDK representation is `isSynthetic: true`, peer origin
`holler`, no human `fromMode`, `priority: later`, and `shouldQuery: true`.
Those exact fields are part of the discussion: T3 maintainers should confirm
the supported representation and UI behavior before implementation.

## Non-goals

- No T3 patch or pull request before maintainers agree on the approach.
- No peer-authored body, sender, thread, type, or instruction in the wake.
- No representation of the wake as human-authored input.
- No remote approval of tools or permissions.
- No message claim or acknowledgement by T3.
- No process discovery by command name, launch order, or caller-provided tag.
- No replacement of T3's normal Claude process supervision or diagnostics.
- No requirement that T3 users install or enable Holler.

## Questions for T3 maintainers

1. Is there a supported provider extension point for observing the exact
   spawned Claude PID without replacing T3's normal spawn implementation?
2. Is there a supported API for queueing a synthetic, non-human SDK input while
   a query is idle?
3. Which SDK provenance fields does T3 preserve, display, and persist for such
   an input?
4. Should this appear in the UI as a system event, an integration event, or be
   hidden while still remaining auditable?
5. Where should connection lifecycle and cancellation live so `/clear`, resume,
   query close, child exit, and application shutdown cannot leave stale waiters?
6. How should provider stderr and startup failures remain observable if a spawn
   callback is required?
7. Would T3 prefer a general local-attention provider interface rather than a
   Holler-specific integration?
8. What feature flag, compatibility policy, and test level would be required
   before accepting an experimental implementation?

## Alternatives

### Keep startup-only behavior

This requires no T3 changes and is the safe default. Messages remain durable
but an idle SDK session does not wake until its next interaction or restart.

### T3-supported integration point

This is the preferred live-wake route. Holler implements the authenticated,
ID-only local attention protocol; T3 controls process identity, SDK injection,
provenance, lifecycle, and UI behavior through a supported interface.

### Claude Code Channels

Channels remain experimental and do not currently provide a broadly available,
host-attested distribution path for this use case. They should be evaluated
separately rather than used to bypass T3's ownership of SDK lifecycle and UI.

## Acceptance criteria for any future implementation

- Opt-in and absent when Holler is not configured.
- Exact process-generation and actor/run/session binding; no alias-based live
  routing and no cross-session wake.
- Fixed, data-free synthetic input with non-human provenance.
- One pending wake per session and at most one injection per message ID per
  attachment; inbox draining determines actual work.
- Busy-session delivery queues without interrupting current work.
- `/clear`, resume, daemon restart, query close, child exit, and T3 shutdown
  leave no stale host connection or orphan process.
- Failure degrades visibly to startup-only while durable delivery continues.
- Existing Claude startup, stderr diagnostics, cancellation, and process
  cleanup behavior does not regress.
- Real-client canaries show no loss, duplication, misrouting, or transcript
  misattribution.

## Suggested collaboration sequence

1. Share this proposal as a discussion artifact, without a code patch.
2. Confirm the problem and desired UX with T3 maintainers.
3. Let T3 maintainers select or define the supported extension point.
4. Revise the protocol and threat model together.
5. Only with explicit agreement, build a small opt-in prototype on a separate
   branch and run the lifecycle/security test matrix.
6. Decide independently whether an upstream implementation is appropriate.

Until that sequence reaches step 5, Holler should describe T3 SDK live wake as
unsupported and rely on durable startup/next-turn hydration.
