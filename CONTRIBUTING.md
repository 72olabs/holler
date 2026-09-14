# Contributing to Holler

Holler is in public-alpha development. Small, test-backed changes that
preserve the protocol and trust boundaries are easiest to review.

## Development setup

Requirements:

- Go 1.26 or newer;
- macOS or Linux;
- Claude Code or Codex only for live connector canaries.

Build and run the deterministic checks:

```sh
./scripts/ci/run.sh
```

This single entrypoint is also used by pull-request and release workflows. It
checks formatting and module tidiness, runs the Go and deterministic lab suites,
builds the release archive, enforces its explicit file allowlist and build
identity, then exercises the extracted product in an isolated home with fake
Claude and Codex clients. The black-box test starts the real packaged daemon but
never uses vendor credentials or model calls.

The lab command starts a real `hollerd` in an isolated home, socket, database,
and harness configuration tree. Its built-in fake Claude and Codex scenarios
exercise direct round trips, allocated names and aliases, resume, offline
delivery, inbox adoption, exact-name collision, daemon replacement, teardown,
and evidence generation. Reports, redacted scenarios, a body-free event ledger,
JUnit output, and logs are written under `.runs/lab/` by default. Pass
`--include-database` only when the full stopped database—including peer message
bodies—is needed for local diagnosis. The command
must leave the sandbox removed, the socket closed, and zero supervised orphan
processes.

These deterministic checks do not require model calls. Fake harnesses certify
Holler protocol and lifecycle logic; they do not certify a vendor plugin,
permission prompt, hook, or wake implementation. Live harness validation is
deliberately separate because it consumes time and tokens and can be affected
by client version or TUI changes.

### Checkpoint canaries

A coding agent can prepare a private Daytona canary for any committed branch
checkpoint without opening a pull request. The request binds the exact commit
and tree, test scenarios, client versions, low-cost models, and spend limits to
one reviewable hash:

```sh
python3 scripts/canary/prepare.py \
  --ref HEAD \
  --tier core \
  --output .runs/canary/request.json
python3 scripts/canary/run.py \
  .runs/canary/request.json \
  --driver fake \
  --output .runs/canary/fake-evidence.json
python3 scripts/canary/daytona.py plan .runs/canary/request.json
```

The committed defaults use Claude Haiku and `gpt-5.6-luna` at low reasoning
effort. Model overrides require an explicit opt-in and become part of the
request hash. See [scripts/canary/README.md](scripts/canary/README.md) for the
test tiers, credential boundary, and approval flow.

## Design constraints

- `hollerd` is the only SQLite owner. CLI, MCP, hooks, and SDKs use the versioned
  API instead of opening the database.
- Actor and run identity are connector-bound, not accepted from message body
  metadata.
- A committed message and an accepted wake are not processing proof. Only a
  successful claim followed by acknowledgement closes delivery.
- Attention notifications contain references, never peer-controlled bodies.
- Hooks fail open when Holler is unavailable; connector failure must not
  prevent the underlying harness from starting or stopping.
- Durable routing and organizational/task policy remain separate concerns.
- Real channels are V2 work. Do not describe a shared `channel_id` label as
  membership or broadcast.

Read [API.md](API.md) and [connectors/README.md](connectors/README.md) before
changing these boundaries.

## Pull requests

Keep generated run output, transcripts, databases, credentials, absolute local
paths, and harness user configuration out of commits. Include:

- the user-visible behavior being changed;
- tests covering success and failure paths;
- compatibility or migration impact;
- any new permissions, hooks, or external processes; and
- live evidence only when deterministic tests cannot establish the behavior.

For connector changes, update the frozen manifest/package hash and include the
deterministic connector tests. Maintainers rerun the private installed-client
release canaries; contributors may include equivalent manual evidence when
available. Do not automate or bypass a user's harness trust prompt in product
code.

Contributions are licensed under Apache-2.0, as described in [LICENSE](LICENSE).
