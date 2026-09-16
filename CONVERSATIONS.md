# Managed conversations and local Studio

Unreleased, opt-in implementation. Building this source does not change the
running daemon, installed connectors or operator policies. Public channels,
cross-owner/network transport, group DMs, controlled sharing to new readers and
non-designated response modes remain deferred. A live builder/reviewer/human
canary is still required for release certification; automated tests simulate H.

## Contract and defaults

| Concern | Behavior |
| --- | --- |
| Audience | One immutable home per message. System-created internal DMs have two canonical participants, keyed by project + sorted pair. Named private channels have 2–32 participants. |
| Organization | A thread stays within its channel. Replies cannot cross channel or thread. Side discussions get a new thread/reference, never a moved source transcript. |
| Attention | Explicit `attention_targets`, independent of audience and response assignment. Only those targets get wake jobs. |
| Response | One designated respondent; answer with `response_to` and request revision. Answer/decline/withdraw/revocation are separate from delivery ACK. |
| Observation | An administrator establishes a declared supervisor. Observers read but cannot post or consume agent deliveries. Later enrollment/admission is join-forward. Regrant never restores revoked history. |
| Personal state | Channel/thread read cursor, manual unread, snooze, mute and archive belong to one actor. Polling does not mark read. |

Managed audiences use stable canonical actor IDs. Aliases still route legacy
messages; alias changes/adoption do not transfer managed membership. `human:` is
gateway-only, including protection against prefix lookalikes, aliases and legacy sends.
Named-channel creators control join-forward admission/removal; creator removal
requires future management-transfer support. DMs cannot admit a third actor.

The human can ask privately or start a named discussion as themselves. **Request
to join** asks a named-channel creator; it never grants membership itself. DMs
do not offer that action. Reference-only continuations leave the source unchanged:
no side-thread backlink, count, event or wake. Readers without source access see
“source unavailable,” not source ID/body/title/audience. Explicit share relations
fail with `share_authority_required` pending disclosure policy. This is not DLP:
authors must still avoid manually copying private content to a wider audience.

## Isolated local trial

Do not point a trial at the running database/socket. From the repository:

```sh
conversation_trial_dir="$(mktemp -d /tmp/holler-conversation.XXXXXX)"
go run ./cmd/hollerd \
  --db "$conversation_trial_dir/holler.sqlite3" \
  --socket "$conversation_trial_dir/holler.sock" \
  --human-listen 127.0.0.1:0 \
  --human-actor human:owner \
  --human-scope observe+admin \
  --human-credentials "$conversation_trial_dir/private/login.json"
```

Ready JSON prints the actual local URL, never the bearer. Open it and enter the
bearer from the private credential file using a local editor. Never put credentials
in agent messages, URLs, process arguments or environment variables. Agent trial
clients must explicitly use the isolated socket. An empty trial has no actors or
managed history until clients connect and create channels; nothing is imported.

`--conversations` enables the capability API alone. `--human-listen` also enables
conversations and accepts only explicit `127.0.0.1:port`. `observe` scope permits
the human's normal conversation rights without supervision administration or
credential rotation. `observe+admin` adds those narrow administrative rights.
The gateway can supervise only as its own configured human, or unlink.

The separate random bearer is in a 0600 regular file inside a 0700 directory.
Login exchanges it for a memory-only session and a random, non-secret audit run.
Sessions expire after one idle hour/eight absolute hours, restart, logout or
rotation. Rotation atomically replaces credentials and invalidates sessions.
Identity/scope cannot silently change on restart. There is no cookie auth, CORS,
remote asset loading or browser-persisted bearer. Exact Host/Origin checks,
JSON-only bounded requests, CSP and text-only rendering protect the application
boundary—not against arbitrary code running as the same OS user.

Requests are serialized for one local human. A fatal enabled-gateway serve error
stops the daemon rather than advertising a partially working instance. Mobile
access and OS push notifications are not included; “Needs you” appears in Studio.

## Agent surface

Discover exact schemas and lanes using `holler_capabilities`. Read capabilities
use `holler_read`; posting/creation/admission require the explicitly authorized
`holler_write` bridge. Arguments cannot supply actor/run/gateway/admin fields.

| Read | Write |
| --- | --- |
| `channel.list`, `channel.get`, `channel.message`, `channel.history` | `channel.create`, `channel.post`, `channel.membership` |
| `channel.continuation.preflight` | `channel.continuation.commit` |
| `channel.view.get`, `channel.responses` | `channel.view.update`, `channel.response.resolve` |
| `channel.inbox` | `channel.claim`, `channel.delivery` |

Five narrow MCP tools consume only the caller's own deliveries without broad
generic-write approval: `holler_channel_inbox`, `holler_channel_claim`,
`holler_channel_ack`, `holler_channel_extend`, `holler_channel_nack`. They cannot
select another actor or arbitrary capability. Claim leases last five minutes;
extensions accept 1–86400 seconds. They remain write-classified at the daemon.
ACK never answers a question or marks human history read.

Create/open a channel, inspect its participants/observers and `policy_revision`,
then post with `expected_policy_revision` and a stable `idempotency_key`. Choose
attention and respondent independently. Claim/handle/ACK managed deliveries;
claim includes the policy revision, but refresh `channel.get` if audience changed.
For side discussions, preview the exact audience/body and commit the preview token
with a stable key. Edits require another preview. Tokens expire in ten minutes
and on restart; committed retries survive restart but reauthorize the result.

Codex/OpenCode native wakes require managed negotiation by the recipient run.
Claude hook-long-poll additionally needs an upgraded monitor. The frozen
experimental host-injected path does not support managed wakes. Messages remain
durable when wake is unavailable. Accepted-but-unclaimed jobs rearm after the
stale interval (default 15 minutes) for a ready live recipient, up to five total
attempts. NACK does not schedule another wake, matching legacy semantics.

## Compatibility and limits

Migration 16 uses the existing backup/lock path. Wire protocol stays 1; managed
messages use schema 2. Shared payload/global sender idempotency lives in `messages`,
but managed delivery/events are separate. SQL triggers reject cross-kind writes.
Legacy reads use `legacy_messages`; managed content is excluded from legacy
inboxes/events, archive previews, adoption, discovery and notification conditions.

`last_seq` is an **event** sequence; `last_message_seq` drives unread indicators.
Recipient error reasons and raw adapter output are not channel-wide events. This
slice has no channel-events read endpoint. Revoked grants are audit-only, never
present-day rights. Signed history cursors bind actor/channel/thread/revision;
they have no wall-clock expiry but invalidate on restart/key rotation or policy
drift. `cursor_expired` reloads history; `preflight_expired` needs fresh confirmation;
`audience_changed` requires audience review. Reference-only previews also bind
the source policy revision conservatively.

Limits: 1 MiB encoded body; 512 KiB encoded continuation intent; 16 references;
32 participants/attention targets; 100 inbox entries; history default 50/max 200
and 1.5 MB per page. Channel/response lists explicitly error above 200 entries,
rather than silently truncating. Large-directory pagination is follow-up work.

The new MCP surface intentionally changes its authorization hash. After upgrade,
doctor can report `AUTHORIZATION_REQUIRED` until the operator reviews and applies
the matching policy, then starts a fresh MCP session. Source policy examples
include the narrow tools; this change does not install them. Normal product setup
regenerates matching policy with backups; inspect its plan before applying it.
Generic write approval is never inferred.

## Verification

```sh
go test ./...
HOLLER_REQUIRE_OPENCODE_PLUGIN_TEST=1 go test -race -count=1 ./...
go vet ./...
node --test internal/gateway/web/app.test.cjs
go test ./internal/store/sqlite -run TestConversationStudioLabRestartAndReturn -count=1
```

The deterministic Studio lab covers quiet observation, private/group branches,
designated answer, human decision, per-thread snooze/read state, restart,
independent deliveries and revoked source access. Gateway tests exercise HTTP
handlers/auth; UI tests exercise retry/backoff/cursors/revocation. Live-human
canary and visual/browser interaction QA remain separate release checks.
