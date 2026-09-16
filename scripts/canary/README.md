# Real-client canaries

This directory is the committed, credential-free control plane for Holler's
private Claude Code and Codex canaries. It lets a coding agent prepare a test
for any committed Git SHA without opening a pull request. The credentialed run
is a separate, explicitly approved operation.

## Agent quickstart

Coding agents use one front door and do not need the Daytona CLI or provider
API details:

```sh
python3 scripts/canary/harness.py doctor
python3 scripts/canary/harness.py doctor --execute
python3 scripts/canary/harness.py check --tier core
python3 scripts/canary/harness.py checkpoint --tier core --execute
```

The first doctor command is local and non-mutating and reports `LOCAL_READY`
because it does not claim to know remote credential state. With `--execute`, it starts
the existing persistent runner only when needed, verifies its policy and both
OAuth sessions without printing account data, and restores its initial power
state. The checkpoint command installs the pinned Daytona Python SDK into the
gitignored `.runs/canary/venv` when necessary, builds the exact committed
Linux artifact in an uncredentialed sandbox, and then stops with
`APPROVAL_REQUIRED`. Inspect the generated request and plan, then paste the
exact command it prints. That second invocation reuses the checksum-verified
artifact and runs the credentialed canary. It will not accept `yes` or another
generic approval in place of the exact request hash.

Progress events are emitted as body-free JSON lines on stderr so an agent can
report whether it is preparing the contract, installing the managed runtime,
building or reusing an artifact, waiting for approval, or running the
credentialed canary. The final machine-readable result is written to stdout.

An operator must expose `DAYTONA_API_KEY` to the agent process and perform the
one-time runner bootstrap and Claude/Codex interactive logins described below.
Agents never need the key value in a prompt, and the harness never prints it.
The common `core` workflow needs no other provider knowledge. The `release`
and `extended` tiers additionally require the checksum-verified v0.7.1 Linux
archive through `--upgrade-from`; `extended` builds and caches its pinned
minimum-client bundle automatically.

The underlying flow is:

1. Commit the candidate changes on a topic branch.
2. Prepare an immutable request for that commit.
3. Run the fake driver and inspect the Daytona plan without spending tokens.
4. Build the artifact and approve the printed `request_hash`.
5. Rerun the printed command to execute the approved request using dedicated
   test subscriptions.
6. Fix on the branch, create another checkpoint commit, and repeat.
7. Use core-tier and relevant feature canaries for checkpoints and PRs;
   squash-merge after review and deterministic CI. Before tagging a versioned
   release, require a fresh release-tier pass bound to the candidate commit.
   Feature-only scenarios do not substitute for that release gate.

The lower-level zero-cost preparation steps remain available for harness
development and debugging:

```sh
python3 scripts/canary/prepare.py \
  --ref HEAD \
  --tier core \
  --output .runs/canary/request.json

python3 scripts/canary/run.py \
  .runs/canary/request.json \
  --driver fake \
  --output .runs/canary/fake-evidence.json

python3 scripts/canary/daytona_controller.py plan .runs/canary/request.json
```

All generated requests, evidence, transcripts, databases, and provider state
belong under `.runs/`, which is gitignored. Requests contain commit, tree,
scenario, model, and budget hashes, but never credentials or message bodies.

## Test tiers

| Tier | Scenarios | Model turns | Intended use |
|---|---|---:|---|
| `preflight` | C0 | 0 | Packaging and environment only |
| `core` | C0-C3 | 7 | Checkpoint commits and pre-PR canaries |
| `release` | C0-C4, C6 | 11 | Release candidate gate |
| `extended` | C0-C8 | 23 | Scheduled compatibility and failure testing |

The scenario files are data rather than executable prompts. This keeps the
test contract reviewable and gives the local fake driver and the remote worker
the same IDs, timeouts, assertions, and estimated model-turn count.

Spend model turns only on the behavior a scenario is meant to prove. Use the
controller's versioned Holler API for deterministic setup and teardown unless
the scenario explicitly tests a real client's ability to perform that action.

### Adding and selecting a scenario

Contributors can add a test without changing a tier:

1. Choose the next unused numeric ID and add
   `scripts/canary/scenarios/C9.json`.
2. Add `scripts/canary/handlers/C9.py` with a `run(context)` function. Return
   the JSON file's assertion names in exactly the declared order; a mismatch
   fails the canary rather than publishing incomplete evidence.
3. Add deterministic tests for helper or parsing logic under
   `scripts/canary/tests/` and run `./scripts/ci/run.sh`.
4. Commit the scenario, handler, and tests. The credentialed runner refuses an
   uncommitted canary controller.
5. Select the scenario through the normal front door:

```sh
python3 scripts/canary/harness.py check --tier core --scenario C9
python3 scripts/canary/harness.py checkpoint --tier core --scenario C9 --execute
```

Repeat `--scenario` to compose an ad hoc run. Explicit selection replaces the
tier's default scenario list, while `--tier` remains the hard spend and wall
clock envelope. C0 is always prepended, unknown and duplicate IDs are rejected,
and each selection gets its own directory under `.runs/canary/checkpoints/`.
The request hash binds the exact commit, scenario definitions, clients,
artifact, and budget before any model call.

Custom code runs beside subscription credentials, so handlers must be public,
committed, reviewed code. `HandlerContext` prevents accidental unbudgeted use;
it is not a security sandbox. Commit review plus exact-tree approval remains
the credential boundary. The harness deliberately does not accept arbitrary
script paths or load code from gitignored directories. It imports each selected
handler in a short-lived credential-free process before building, enforces the
declared per-scenario timeout and exact model-turn estimate, and applies a static
tripwire against direct process, PTY, socket, signal, or Worker access. See
[`handlers/README.md`](handlers/README.md) for the handler contract and worker
helpers.

## Cost controls

The committed defaults are deliberately the cheapest suitable subscription
models:

- Claude Code: `haiku` (Claude Haiku 4.5 family). One-shot invocations use
  `--max-budget-usd`; interactive live-attention scenarios rely on the
  controller's total-turn and wall-clock kill switches because Claude's dollar
  flag is print-mode only.
- Codex: `gpt-5.6-luna`, low reasoning effort, with fast mode left disabled.
  Codex reports token usage; the controller stops before starting another turn
  once the request's reported-token, total-turn, or wall-clock limit is
  exhausted. Daytona rejects the client's Responses WebSocket, so canaries use
  the same ChatGPT subscription login through a pinned HTTP/SSE custom-provider
  configuration.

Changing either model requires both an explicit command-line override and
`--allow-model-override`. The selected model is included in the approved
request hash, so a worker cannot silently upgrade to a more expensive model.

## Daytona boundary

The Daytona plan intentionally separates two sandboxes:

- The builder receives the committed source tree and no model credentials. It
  runs deterministic CI and produces the verified release archive.
- The persistent canary runner receives only that archive, the canary runtime,
  and a tiny fixture Git repository. Its private filesystem holds only the
  dedicated test accounts' OAuth state; it never receives the Holler source
  checkout.

Both real clients run in the same credentialed sandbox and OS user because
Holler uses a local Unix socket. The named runner auto-stops after 15 idle
minutes, and Daytona preserves its filesystem across stop/start and archive.
Each run begins with a stop/start boundary, uses a private per-run directory,
downloads body-free evidence, removes that directory, and stops the runner.
Evidence contains IDs, hashes, versions, assertions, timings, and usage
totals—not peer message bodies or auth files.

The controller requests explicit sandbox domain allowlists on Daytona Tier 3
and Tier 4. Daytona Tier 1 and Tier 2 reject sandbox overrides because their
organization-level restriction is mandatory; on that exact response, the
controller retries creation without an override and records
`organization-tier` in the sandbox labels and build result. Other network
policy errors fail closed. See [Daytona's network-limit semantics](https://www.daytona.io/docs/en/network-limits/#tier-based-network-restrictions).

Use the provider probe only when you explicitly want to create a billable
sandbox:

```sh
python3 -m venv .runs/canary/venv
.runs/canary/venv/bin/pip install -r scripts/canary/requirements-daytona.txt
DAYTONA_API_KEY=... .runs/canary/venv/bin/python \
  scripts/canary/daytona_controller.py probe --execute
```

Without `--execute`, the provider tool does not create anything. By default a
successful probe is deleted in a `finally` block; `--keep` is an explicit
debugging escape hatch.

Once a verified artifact and authenticated runner exist, the credentialed core
canary is launched explicitly:

```sh
DAYTONA_API_KEY=... .runs/canary/venv/bin/python \
  scripts/canary/daytona_controller.py run .runs/canary/request.json \
  --artifact dist/holler-VERSION-linux-amd64.tar.gz \
  --output .runs/canary/real-evidence.json \
  --execute
```

The `run` command accepts any validated built-in tier or explicit scenario
selection. It uploads only the approved archive, request, and small worker bundle;
downloads body-free
evidence; removes the per-run files; and stops the persistent runner in a
`finally` block. `--keep-on-failure` is available only for deliberate
debugging.

## Credential bootstrap

Do not put OAuth state in this repository, a request manifest, a snapshot, a
FUSE volume, or an environment variable. Create one named persistent Daytona
runner and log each dedicated test account in from its terminal. Claude Code
and Codex use separate mode-`0700` directories on the runner's normal
filesystem. The accounts should have no source-hosting, production,
billing-administration, or unrelated-data access. Treat the Daytona
organization, its API keys, and this dedicated runner as the credential trust
boundary.

The controller creates a credential-free client snapshot first. It installs
the Go version required by `go.mod` from a hash-pinned official archive, then
installs the pinned Claude and Codex clients. Its name is derived from all
three versions, so a toolchain or client upgrade creates a new immutable
environment rather than mutating the previous one:

```sh
DAYTONA_API_KEY=... .runs/canary/venv/bin/python \
  scripts/canary/daytona_controller.py bootstrap .runs/canary/request.json --execute

DAYTONA_API_KEY=... .runs/canary/venv/bin/python \
  scripts/canary/daytona_controller.py runner .runs/canary/request.json --execute
```

The second command creates or verifies `holler-canary-runner` and returns its
ID. Open that runner's terminal in Daytona and run the two printed login
commands yourself. Keep the runner: stopping or archiving it preserves the
OAuth state without capturing credentials in a reusable snapshot.

Build the committed source in an uncredentialed sandbox. `git archive` means a
local checkpoint commit can be tested without a push or PR:

```sh
DAYTONA_API_KEY=... .runs/canary/venv/bin/python \
  scripts/canary/daytona_controller.py build .runs/canary/request.json \
  --output .runs/canary/holler-linux-amd64.tar.gz \
  --execute
```

The builder stops the persistent runner first, preserving its filesystem while
keeping the workflow within Daytona's entry-tier concurrent-memory limit.

Then regenerate the request with `--artifact` so the downloaded archive hash
becomes part of the operator-approved request before running the real canary.

The real worker becomes usable only after the named runner exists, the pinned
clients are present in its base snapshot, both authentication preflights
succeed, and Claude's interactive onboarding state is complete. The controller
idempotently seeds only a dark theme, the pinned completed-onboarding version,
and trust for `/home/daytona/.holler-canary-workspace`; it preserves all other
Claude configuration and never prints or exports credentials. C0 verifies that
non-secret state, then runs `claude --init-only` to require Holler's actual
`SessionStart` registration and hydration without a model call. C0 also opens
Codex's real first-launch review, selects “Trust all” only when exactly Holler's
two lifecycle hooks are pending, and verifies that Codex persisted SHA-256 trust
records for both hooks before any model call.

Interactive scenarios wait for the live Holler registration before submitting
input, send the terminal Enter key rather than a newline, and use expected
markers that never appear literally in prompts or peer message bodies. Usage
is recorded only after the corresponding response marker is observed. Until
these gates pass, the fake driver still tests manifest integrity, tier
accounting, budget enforcement, provider planning, and body-free evidence
generation without model calls.
