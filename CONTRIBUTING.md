# Contributing to Holler

Holler is in public-alpha development. Small, test-backed changes that
preserve the protocol and trust boundaries are easiest to review.

## Development setup

Requirements:

- Go 1.19 or newer;
- macOS or Linux;
- Claude Code or Codex only for live connector canaries.

Build and run the deterministic checks:

```sh
go test ./...
go vet ./...
go test -race ./...
./scripts/build.sh
HOLLER_VERSION=0.0.0-dev ./scripts/package-release.sh
```

The tests do not require model calls. Live harness validation is deliberately
separate because it consumes time and tokens and can be affected by client
version or TUI changes.

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
