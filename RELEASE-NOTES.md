# Holler 0.5.1

Holler 0.5.1 is a focused storage-ownership reliability release. It ensures
that concurrent daemon or connector startup cannot race schema migration or
leave multiple processes believing they own the same SQLite database.

## Highlights

- Serializes schema migration across processes with an advisory lock scoped to
  the exact database path.
- Retains SQLite's exclusive ownership lock for the lifetime of the winning
  daemon.
- Uses short, jittered migration retries so normal daemon replacement succeeds
  without creating a startup thundering herd.
- Destroys losing SQLite connections instead of caching locks that can
  deadlock other contenders.
- Rolls back interrupted migrations with a detached, bounded cleanup context.
- Reports the losing process clearly: `another hollerd already owns this
  database`.
- Keeps `BEGIN IMMEDIATE` as defense in depth against non-Holler SQLite writers
  and filesystems that do not preserve local advisory-lock semantics.

## Validation

The release candidate includes deterministic coverage for:

- concurrent cold migration and concurrent warm reopen;
- exactly one retained database owner when several processes start together;
- successful daemon replacement after the current owner exits;
- databases whose schema is newer than the running binary;
- bounded migration-lock timeout with a typed ownership error;
- interrupted migration rollback and retry behavior; and
- the complete Holler test, race, vet, packaging, and build-identity suites.

The connector protocol and tool surface are unchanged from 0.2.0. Claude
continues to use hook-long-poll, Codex continues to use native queue delivery,
and OpenCode remains experimental.

## Installation

On macOS:

```sh
brew update
brew upgrade 72olabs/tap/holler
holler setup claude
holler setup codex
```

For a first installation:

```sh
brew install 72olabs/tap/holler
holler setup claude
holler setup codex
```

Release archives contain `holler`, `hollerd`, and the version-matched connector
marketplace. The hosted workflow publishes Linux amd64 and macOS arm64
archives; Homebrew builds the tagged source for the host architecture.

## Current limitations

- Holler supports one trusted operating-system user on one machine. The Unix
  socket owner is the security boundary.
- Direct actor routing only. Channel membership, broadcast, channel history,
  and replay remain future work.
- Claude Channels are not included; Claude uses hook-long-poll.
- OpenCode remains experimental pending installed-client certification.
- Runtime and connector support is limited to Linux and macOS. Windows is not
  supported by the current Unix locking and service implementation.
- Release signing and binary attestation are not yet included.

## Upgrade and removal

Rerun setup after upgrading so the daemon and cached connector packages remain
version-matched:

```sh
holler setup claude
holler setup codex
```

Before uninstalling, remove each configured harness:

```sh
holler setup claude --remove
holler setup codex --remove
```

Removing the final harness removes the Holler-managed daemon service while
preserving the durable database and logs.
