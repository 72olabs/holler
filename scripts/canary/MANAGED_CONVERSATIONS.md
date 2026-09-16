# Managed conversation canary plan

These are opt-in feature-checkpoint tests, not v0.7.4 release certification.
Use the same committed harness and pinned subscription models as the built-in
canaries. The source must be committed on a local topic branch; no push is
required. Scenario definitions bind the managed daemon, loopback gateway,
synthetic human identity, generated policy and terminal-oracle choice into the
approved request hash. No model override or production permission change is used.
These local checkpoints are unpublished; passing them does not substitute for
GitHub PR/merge checks.

Run separately, in this order, only after review:

| Selection (C0 always prepended) | Model turns | Purpose |
| --- | ---: | --- |
| C9 | 0 | Protocol-client + synthetic HTTP human observation, privacy, private/group reference continuation, response/decision, view restart and revocation |
| C10 | 3 | Fixture-approved real Codex create/post, Claude narrow consumption, independent Codex designated answer/ACK after restart |
| C11 | 4 | One arm and one unsolicited managed-wake turn for each real client; no generic Claude write |

Each selection uses `--tier core`: at most 8 turns, $0.50 reported Claude cost,
250,000 reported Codex tokens and 1,800 seconds. The initial planned sequence is
7 model turns total. Do not silently expand into retries, different models or
extended/release tiers after a failure. Report failed checkpoints and remaining
budget before proposing another live model run. Interactive client turns rely
on turn/wall-clock controls, not complete dollar/token accounting.

```sh
python3 scripts/canary/harness.py doctor
python3 scripts/canary/harness.py check --tier core --scenario C9
python3 scripts/canary/harness.py checkpoint --tier core --scenario C9 --execute
# Inspect exact commit, artifact, scenarios, budgets and printed request hash.
# Then run the printed --approve sha256:... command under operator authorization.
# Repeat for C10 only after C9 passes, and C11 only after C10 passes.
```

## Oracles and privacy

- Protocol setup uses `canary-fixture`, a separate controller run, no managed
  attention negotiation, and fixed synthetic actors. It never becomes a real
  client or claims real-client readiness.
- Human operations use authenticated loopback HTTP. Synthetic gateway bearer
  and session stay inside the worker fixture. Handlers receive no credential,
  process, network destination or database path. HTTP 429 is bounded/backed off.
- C0 verifies actual installed connector versions, generated policies, tool
  surface, onboarding and lifecycle. Claude uses only narrow consumption tools
  and reads. C10 explicitly declares `fixture-approved-codex-write`: only its
  two `run_codex_write` turns temporarily change the runner's generated
  `holler.config.toml` entry for `holler_write` from `prompt` to `approve`.
  Parsed before/after comparison proves only that field changed; atomic writes
  preserve file permissions. A `finally` restores the original bytes and verifies
  the original hash. Evidence records original/approved/restored policy hashes.
  Unexpected concurrent edits are not overwritten: restoration fails closed and
  the controller stops the runner. A hard kill can prevent `finally`; baseline
  checks before setup and before each managed scenario reject a stale approval
  without rewriting it. Inspect the policy before manual recovery. This requires an exclusive
  runner, not a shared developer session. OAuth files are never read or changed.
  This approves the generic write tool within the fixture turn, not just two
  operations; daemon identity/audience checks still apply. Source templates and
  the operator's local install stay unchanged. Normal Codex calls, Claude, C0,
  C9 and C11 retain generated defaults. This tests the
  preapproved write path, not approval UI or unchanged-default write behavior.
- Static check-time validation requires explicit write calls and the declared
  policy to agree; a runtime guard rejects missing declaration before model
  launch. This is a review tripwire, not semantic analysis or a Python sandbox.
- C10 has separate marker, session-end, channel-count, opening-correlation and
  controller-post failure checkpoints. Closed harness reason codes and known
  Codex tool terminal-status counts are exported without arguments, results,
  errors or transcript text. Missing telemetry never proves a denied call.
- C11 separates entry, readiness, hook trust, arm, registration, wake trigger,
  wake, exit and session-ended failure phases. Claude registers on entry; Codex
  registers after its first completed (and charged) submitted turn, because that
  turn triggers SessionStart. Registration timeout has a closed reason code.
  On wake-marker failure only, sent IDs are retained for the same fixed terminal
  projection described below. If lifecycle/daemon guards fail, the diagnostic is
  omitted with a safe reason; it never bypasses a guard or replaces the original
  failure. Failed interactive attempts may incur usage not charged by the ledger.
  Terminal marker failures distinguish client exit from timeout and report only
  byte counts, process/marker booleans and fixed permission/auth/rate/API/tool
  error signal labels. These labels are diagnostic hints, not proof of cause;
  no transcript, marker value, error text or credential is exported. Marker
  matching accepts ANSI styling and whitespace wrapping; the same normalized
  check rejects prompts containing their expected marker, preventing echo-based
  completion. Marker instructions request a standalone line. These are only turn
  checkpoints: the independent API and terminal success oracles remain unchanged.
  On a clean C11 session exit, Codex receives `/quit` and must exit successfully
  within 20 seconds before descendant cleanup. Its actual SessionEnd hook must
  remove the live registration within the existing 30-second wait; the harness
  never invokes `holler session-end` to make that check pass. Timeout reports
  `session-end-timeout`; an unsuccessful process exit reports `session-exit-failed`.
  Exceptions still force process cleanup while preserving the test failure.
- Marker strings are turn-accounting checkpoints, never the success oracle.
  API-visible messages, actor/run/thread correlation, response state, per-actor
  inboxes, negative claims, observer rights and view state are asserted separately.
- There is no public managed-event/delivery audit endpoint. To distinguish ACK
  from dead-letter and count attempts, one fixed helper first verifies every
  client registration ended, stops hollerd, verifies process exit/socket removal,
  opens SQLite read-only, pins schema 16, and selects only IDs, actor, delivery
  state/attempt and event counts. It closes the connection before restarting.
  Evidence labels these assertions `offline-projection-and-public-api` and pairs
  them with public API assertions after restart. No bodies, tokens, reasons or
  event payloads are exported. Tests use a real-daemon-created schema; CI requires
  those tests rather than accepting a mock schema or skipped fixture.
- The non-attended member must retain a queued delivery with zero claims/ACKs
  and zero attention events: audience delivers; attention wakes. Legacy
  inboxes/events must contain no managed message IDs. Human observation must not
  consume agent delivery. Source private continuations must not add source-side
  messages/events; inaccessible references hide the source ID.
- The persistent Daytona runner is exclusive to the sequence. Use the existing
  provider stop/start, per-run directory removal, evidence download and final
  stop. Never export subscription OAuth state or snapshot the authenticated runner.

## Coverage limits

The human is **synthetic HTTP**, not a browser or a real human. Public/mobile,
multi-owner transport, other response modes, large directories, visual UI QA and
real-human usability are not covered. The worker does not modify or restart the
operator's local daemon. C9 is protocol coverage; only C10/C11 prove model-client
behavior. Keep any failure evidence as a failure rather than weakening checks.
