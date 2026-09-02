# Harness connectors

This directory contains the operator-controlled half of the Codex, Claude, and OpenCode connector contract. The matching relocatable plugin packages live under `connectors/marketplace/plugins/`; the production binary freezes their expected contents in a capability manifest.

The installation artifact must place `holler` and `hollerd` beside each other and copy the version-matched marketplace to `share/holler/marketplace` under the same prefix. Development builds discover `connectors/marketplace` automatically. This lets the product setup command remain independent of a source checkout.

On macOS, `brew install 72olabs/tap/holler` installs this layout under
Homebrew's stable prefix. A release archive provides the same `bin/` and
`share/holler/marketplace/` relationship under its extracted directory.

For the standard setup path, run exactly one command per installed harness:

```sh
holler setup claude
holler setup codex
```

Each command shows the plugin, config, permission, and service changes and asks for confirmation. `--dry-run` emits the non-mutating structured plan; `--yes` is for explicitly authorized unattended installation. Defaults are live attention, reciprocal `claude`/`codex` peers, and the `default` project. Reruns preserve an existing connector identity and repair or update the rest idempotently. Rerun setup after a Holler package upgrade so the plugin cache and daemon build are refreshed together.

Remove a harness connector before uninstalling Holler:

```sh
holler setup claude --remove
holler setup codex --remove
```

Removal deletes only Holler-managed harness configuration. The shared daemon stays running while either harness remains configured; removing the last harness removes its service definition but preserves the durable database and logs. Local state can still be removed if the harness executable was uninstalled first. Config writes preserve symlink chains and existing modes, and the first pre-Holler `.bak` is never overwritten by a later setup rerun.

The connector is deliberately split into three proof levels:

- `connector manifest` states what this binary expects: protocol and connector versions, supported client range, lifecycle coverage, package hash, full MCP tool-schema hash, notification mode, and permission class for every tool.
- `connector doctor` performs deterministic, model-free checks. It can prove discovery, installation, configured policy, project identity, daemon compatibility, and the presence of a notification adapter. Its highest state is `CONFIGURED`; it never claims that file inspection proves a working agent.
- `connector certify` evaluates evidence produced by one bounded real-client run after explicit event cursors. It requires correlated registration and hydration, MCP write, claim plus acknowledgement, clean client and daemon build provenance, and—under `live-review`—an accepted daemon notification for the same session. Only this command can return `READY`.

## Supported profiles

`async-peer` has minimum compatibility baselines of Codex CLI 0.149.1 and
Claude Code 2.1.247. It requires durable send/fetch plus lifecycle hydration
and permits visible polling as the fallback attention path.

`live-review` uses the native `codex queue` adapter. On Claude Code,
`hook-long-poll` is the supervised `asyncRewake` adapter rearmed by
`Stop`/`StopFailure`; `startup-only` deliberately cannot satisfy
`live-review`. Claude Channels are not packaged for this release. A connector
advertises `READY` only when one message ID has accepted wake, MCP claim, and
MCP acknowledgement evidence in the same certified run.

The current packaged test versions are Claude Code 2.1.258 and Codex CLI
0.151.0.

OpenCode support targets the current 1.x plugin and configuration contract. `native-prompt` uses the local OpenCode HTTP server's asynchronous prompt endpoint to submit a message reference—not its body—to the exact registered session. `startup-only` retains durable hydration without a live wake path. The package is deterministically tested but remains marked `pending-live-certification` until it passes the bounded canary against an installed OpenCode client.

## Operator policy

The files under `policies/` are examples to review and merge into a user, profile, managed, or enterprise configuration layer. A repository or plugin must not grant itself authority.

For Codex, use a real `$CODEX_HOME/<name>.config.toml` profile or managed configuration. Codex 0.149.1 passed the least-privilege canary with a profile file but ignored the equivalent repeated dotted `-c` overrides for leased/write MCP calls. The policy therefore names the exact eighteen-tool allowlist and repeats a per-tool mode instead of relying only on the server default. Routine tools and the read capability bridge use `approve`; inbox adoption, alias mutation, and the generic write bridge use `prompt`.

Codex plugin installation does not trust plugin hooks. A human can review the exact definition with `/hooks`, or externally vetted automation can use `--dangerously-bypass-hook-trust` for that one certification invocation. The latter proves hook functionality, not persisted operator trust.

For Claude, pass the reviewed settings file using `--settings` or install equivalent operator/managed permissions. Routine Holler tools and the read capability bridge are pre-approved, while inbox adoption, alias mutation, and the generic write bridge are installed as explicit `ask` rules because they can change routing or other durable state. Project settings are effective only after project trust.

For OpenCode, setup previews and, only after the operator chooses `--apply`, generates a connector-owned `opencode.json` with one local MCP server, exact `allow` entries for routine and read-bridge tools, and explicit `ask` entries for inbox adoption, alias mutation, and the generic write bridge. It does not modify the user's general OpenCode config. The launcher points `OPENCODE_CONFIG` and `OPENCODE_CONFIG_DIR` at that isolated package for the launched process only. This is an operator-authorized installation action, not authority a plugin grants itself at runtime.

## Actor naming lifecycle

New product setup defaults to `--name-mode allocate`; rerunning an existing
setup preserves its prior choice. Advanced setup can omit `--name-mode` to keep
the original shared-inbox behavior. `--name-mode exact` refuses another live run for the
same actor; launcher-only `--takeover` records and supersedes the old presence.
`--name-mode allocate` treats the configured actor as a base and always mints
an opaque canonical identity such as `claude-a7f3c2`. A harness session ID automatically reclaims the
same allocation on resume, while `--launch-tag <stable-tag>` provides the same
continuity to an external supervisor. Separately launched workers in one
working directory remain isolated because continuity never depends on cwd.

Allocation, continuity binding, and the mint event commit in one daemon
transaction. Live and removed aliases share a reserved namespace with live and
retired actors. The first authoritative SessionStart atomically claims the
installed `<project>-<harness>` default alias if absent; a concurrent loser
keeps its exact opaque actor address and receives an actionable route-choice
prompt. The ready handshake returns the assigned actor before hooks or MCP
operations are accepted, and every later operation uses that immutable bound
identity. Naming flags are setup/launcher controls and are not exposed as agent
tools.

## Codex setup and launch

The normal product setup is:

```sh
holler setup codex
```

It registers the marketplace, installs or refreshes the plugin, writes `~/.holler/connectors/codex.json`, merges a marked frozen MCP allowlist into `~/.codex/config.toml` without rewriting unrelated bytes, writes the dedicated `holler.config.toml` profile for explicit launcher use, and installs and verifies the per-user daemon. Changed existing files receive `.bak` backups. A conflicting pre-existing Holler policy stops setup during preflight rather than being broadened.

Codex deliberately retains hook trust. On the first turn after installation or a package update, review and approve the exact `SessionStart` and `SessionEnd` commands and package hash when Codex prompts. Holler does not edit Codex's internal trust store or bypass this check. After that review, ordinary `codex` launches need no Holler flags or launcher.

Setup also records stable Holler and absolute Codex executable paths. The daemon resolves the Codex path again for every native-queue notification, so a daemon started before first setup or a later client relocation does not freeze a broken minimal-`PATH` lookup. Plain `codex` sessions load the persisted connector identity and derive a process-scoped run after hook trust and MCP admission. Codex currently runs `SessionStart` when the first turn is submitted, not when an otherwise-idle TUI opens, so native-queue delivery becomes available after that first turn.

Launch through the connector when the session should use the dedicated least-privilege profile, explicit project root, or an independently addressable actor override:

```sh
holler connector launch --harness codex --actor codex-review -- [additional codex arguments]
```

Choose `native-queue` for live attention or `startup-only` when durable hydration is sufficient. The selected mode is stored on each live registration, so both kinds of Codex session can safely share one daemon. The launcher rejects hook-trust bypass flags and conflicting profile or working-directory flags.

For custom identity, attention, or policy destinations, `scripts/setup-codex.sh` and `holler connector setup --harness codex` retain the advanced preview/`--apply` workflow.

## Claude setup and launch

The normal product setup is:

```sh
holler setup claude
```

It registers the marketplace, installs or updates the plugin, merges only the frozen eighteen-tool allowlist and Holler plugin options into Claude user settings, writes `~/.holler/connectors/claude.json`, and installs and verifies the per-user daemon; changed existing files receive `.bak` backups.

Plain `claude` sessions load the persisted connector binding and setup-recorded Holler executable, so hook-long-poll does not depend on shell-only environment variables. Use the connector launcher when the session needs an explicit identity override or a separately addressable actor:

```sh
holler connector launch --harness claude --actor claude-review -- [additional claude arguments]
```

Choose `hook-long-poll` for live attention or `startup-only` for durable hydration without live wakeups. Claude Channels are intentionally absent from the shipping plugin.

Interactive Claude sessions arm `hook-long-poll` after a turn. One-shot
`claude --print`/SDK sessions still register, hydrate their durable inbox, and
use MCP, but skip the live monitor so the hook cannot keep the client process
open after its response is complete.

For custom identity, attention, scope, or policy destinations, `scripts/setup-claude.sh` and `holler connector setup --harness claude` retain the advanced preview/`--apply` workflow.

An actor is a durable inbox identity, not a session name. Legacy concurrent sessions using the same actor are intentional competing consumers: the first successful `bus_inbox` claim owns the lease. Prefer allocate mode when parallel sessions must each be independently talkable. Directory-discovered opaque handles are candidates, not routing authority; an agent must ask the operator before using one unless the operator supplied the exact handle.

## OpenCode setup and launch

Preview the isolated OpenCode installation:

```sh
scripts/setup-opencode.sh \
  --attention native-prompt \
  --actor opencode-live \
  --peer codex-live \
  --project holler
```

Repeat with `--apply` after review. By default, setup installs the plugin, launcher, and skills under `~/.config/opencode/holler`, writes its dedicated profile there, and writes the selected connector settings to `~/.holler/connectors/opencode.json`. Existing connector-owned files receive `.bak` backups.

Launch through Holler so registration and wakeup share one fresh run identity and one loopback HTTP endpoint:

```sh
holler connector launch --harness opencode --actor opencode-review -- [additional opencode arguments]
```

For `native-prompt`, the launcher passes `--port 0`, so the OS and OpenCode atomically bind an available loopback port, and generates fresh HTTP Basic credentials for every run. OpenCode supplies the actual bound `serverUrl` to the plugin, which places that exact URL in the lifecycle registration handle. The launcher rejects conflicting `--hostname` and `--port` arguments. `startup-only` exposes no HTTP attention endpoint or credentials. Starting plain `opencode` does not load these connector bindings.

## Typical workflow

Start `hollerd`, then inspect without spending model tokens:

```sh
holler connector manifest --harness codex
holler connector doctor \
  --harness codex \
  --profile live-review \
  --project /absolute/project/root \
  --policy /operator/config/holler.config.toml
```

Capture the current durable and operational event positions, run one bounded real-client canary, then certify only the new window:

```sh
holler connector certify \
  --harness codex \
  --profile live-review \
  --project experiment-partition \
  --actor codex \
  --run immutable-run-id \
  --after-durable 12 \
  --after-operational 48
```

## Lifecycle and failure behavior

Plugin paths resolve through `PLUGIN_ROOT` or `CLAUDE_PLUGIN_ROOT`, never through the source checkout. The wrapper resolves the Holler runtime from an explicit environment override, setup's mode-`0600` runtime path record, or `PATH`, in that order. `SessionStart` covers cold start, resume, clear, compaction, and Claude fork. It fills missing identity and routing fields from the connector selection, derives a stable run from the owning harness process, registers directly through the daemon before MCP is assumed ready, and hydrates reference-only unread metadata without claiming it. Launcher-provided bindings retain precedence.

`SessionEnd` expires that exact `(actor, run, session)` registration and records `session.stale`. It is advisory; crash recovery still relies on the registration lease. A failed start hook emits small `DEGRADED` context and exits successfully so Holler cannot prevent the underlying agent session from starting.

Notification dispatch belongs to `hollerd` through a durable outbox created in the message transaction. CLI, MCP, and future SDK/API senders have identical wake semantics; daemon restart cannot lose a committed wake request. Adapter acceptance is not processing proof: the outbox remains visibly `accepted` until the recipient claims the message, but the daemon does not reinject an already-accepted reference. A claim atomically closes the actor-scoped wake job. Retryable adapter failures use bounded backoff and separate operational evidence; they never turn a committed message into a retryable send error. Duplicate idempotent sends do not create a second wake job.

The MCP process renews only the newest registration for its actor/run. A passively expired newest session can recover after daemon downtime, while explicit `SessionEnd` is terminal and prevents an older sibling from being resurrected. Agents can call `bus_extend` before a long task exhausts a message lease, and acknowledgement is idempotent for the same terminal lease token. The API client reconnects after daemon restart without replaying unsafe claims.

Claude attention notices contain only the server-generated message ID and fixed fetch instructions; sender, thread, type, and body stay behind `bus_inbox`. The daemon's durable notification worker offers that reference to an exact `(actor, run, session, attention mode)` waiter; if no waiter is active, the outbox retries and records the failure visibly. Hook-long-poll holds a per-session OS file lock, performs one durable inbox reconciliation whenever it arms, and exits through `asyncRewake` for either an already-durable item or a newly offered notice. It reconnects across daemon restarts. `SessionEnd` expires the registration and releases its parked wait. The connector degrades to startup-only hydration when live attention is unavailable.

OpenCode treats an HTTP `204` from `prompt_async` only as acceptance by the local harness endpoint. It is not processing evidence; certification still requires the same message ID to be claimed and acknowledged through MCP.

For an in-place source upgrade, build from a clean commit with `scripts/build.sh`, then rerun `holler setup claude` and/or `holler setup codex`. Moving from 0.6.0 to 0.6.1 is a one-time reconnect boundary because a live 0.6.0 MCP process cannot gain the new bridge code. Once a session runs the 0.6.1 bridge, the unchanged MCP child can reconnect after later daemon upgrades, query `holler_capabilities`, and invoke new daemon-owned operations through the fixed read/write tools. Package-manager installations keep service and marketplace references on the stable prefix rather than a versioned Cellar directory. Updated clients can fall back to the legacy protocol-v1 hello for ordinary operations, but certification remains unavailable until the daemon reports a clean build identity.

## Remaining production work

- Replace the local socket's OS-user identity boundary with the protocol's signed challenge-response before supporting multiple OS identities or nodes.
- Certify hook-long-poll against each supported Claude release and retain startup-only as the explicit fallback when live attention is unavailable.
- Add release signing/attestation around the existing package and MCP-surface hashes.
- Publish the release tag, render the formula with that tag archive's SHA-256,
  and complete one clean-machine install from `72olabs/homebrew-tap`.
