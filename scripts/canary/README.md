# Real-client canaries

This directory is the committed, credential-free control plane for Holler's
private Claude Code and Codex canaries. It lets a coding agent prepare a test
for any committed Git SHA without opening a pull request. The credentialed run
is a separate, explicitly approved operation.

The normal flow is:

1. Commit the candidate changes on a topic branch.
2. Prepare an immutable request for that commit.
3. Run the fake driver and inspect the Daytona plan without spending tokens.
4. Approve the printed `request_hash`.
5. Run the approved request in Daytona using dedicated test subscriptions.
6. Fix on the branch, create another checkpoint commit, and repeat.
7. Open the PR only after the release-tier canary passes; squash-merge after
   review.

The zero-cost preparation steps are:

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
| `core` | C0-C3 | 8 | Checkpoint commits and pre-PR canaries |
| `release` | C0-C4, C6 | 12 | Release candidate gate |
| `extended` | C0-C8 | 24 | Scheduled compatibility and failure testing |

The scenario files are data rather than executable prompts. This keeps the
test contract reviewable and gives the local fake driver and the remote worker
the same IDs, timeouts, assertions, and estimated model-turn count.

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
  exhausted.

Changing either model requires both an explicit command-line override and
`--allow-model-override`. The selected model is included in the approved
request hash, so a worker cannot silently upgrade to a more expensive model.

## Daytona boundary

The Daytona plan intentionally separates two sandboxes:

- The builder receives the committed source tree and no model credentials. It
  runs deterministic CI and produces the verified release archive.
- The canary receives only that archive, the canary runtime, and a tiny fixture
  Git repository. It mounts the dedicated OAuth test-account volume but never
  receives the Holler source checkout.

Both real clients run in the same credentialed sandbox and OS user because
Holler uses a local Unix socket. The sandbox is ephemeral and has a 60-minute
stopped-state auto-delete fallback. Evidence is downloaded before deletion and
contains IDs, hashes, versions, assertions, timings, and usage totals—not peer
message bodies or auth files.

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

Once a verified artifact and the OAuth volume exist, the credentialed core
canary is launched explicitly:

```sh
DAYTONA_API_KEY=... .runs/canary/venv/bin/python \
  scripts/canary/daytona_controller.py run .runs/canary/request.json \
  --artifact dist/holler-VERSION-linux-amd64.tar.gz \
  --output .runs/canary/real-evidence.json \
  --execute
```

The `run` command accepts the `core` tier today. It uploads only the approved
archive, request, and small worker bundle; mounts the auth volume; runs C0-C3;
downloads body-free evidence; and deletes the sandbox in a `finally` block.
`--keep-on-failure` is available only for deliberate debugging.

## Credential bootstrap

Do not put OAuth state in this repository, a request manifest, a snapshot, or
an environment variable. Create one Daytona volume named
`holler-canary-auth`, mount it only into the credentialed sandbox, and log each
dedicated test account in from that sandbox. Configure Claude Code and Codex to
use separate subdirectories in the mounted volume. The accounts should have no
source-hosting, production, billing-administration, or unrelated-data access.

The controller makes the client snapshot before it mounts the empty auth
volume. It installs the Go version required by `go.mod` from a hash-pinned
official archive, then installs the pinned Claude and Codex clients. Its name
is derived from all three versions, so a toolchain or client upgrade creates a
new immutable environment rather than mutating the previous one:

```sh
DAYTONA_API_KEY=... .runs/canary/venv/bin/python \
  scripts/canary/daytona_controller.py bootstrap .runs/canary/request.json --execute

DAYTONA_API_KEY=... .runs/canary/venv/bin/python \
  scripts/canary/daytona_controller.py auth-sandbox .runs/canary/request.json --execute
```

The second command returns a sandbox ID. Open that sandbox's terminal in
Daytona, run the two printed login commands yourself, then delete only the
temporary sandbox. The OAuth state remains in `holler-canary-auth`; it is never
captured by the snapshot.

Build the committed source in an uncredentialed sandbox. `git archive` means a
local checkpoint commit can be tested without a push or PR:

```sh
DAYTONA_API_KEY=... .runs/canary/venv/bin/python \
  scripts/canary/daytona_controller.py build .runs/canary/request.json \
  --output .runs/canary/holler-linux-amd64.tar.gz \
  --execute
```

Then regenerate the request with `--artifact` so the downloaded archive hash
becomes part of the operator-approved request before running the real canary.

The real worker becomes usable only after the dedicated OAuth volume exists,
the pinned clients are present in the named snapshot, and both authentication
preflights succeed. Until then, the fake driver tests manifest integrity, tier
accounting, budget enforcement, provider planning, and body-free evidence
generation without model calls.
