# Security policy

## Alpha security boundary

Holler 0.1.0-alpha.2 is designed for one trusted operating-system user on one
machine. `hollerd` listens on a mode-`0600` Unix socket and is the only process
that opens the SQLite database. CLI commands, MCP servers, and harness hooks
connect through that socket.

The local OS account is currently the authentication boundary. The protocol's
signed challenge-response is not implemented, so Holler must not be exposed
to another OS user, forwarded over a network, or used as a multi-tenant
service. Anyone who can execute code as the owning user should be assumed able
to access that user's Holler data and connector configuration.

Harness connectors bind the actor and immutable run during the API handshake;
the model cannot select a different sender on an individual send. This
prevents ordinary prompt content from spoofing the envelope, but it does not
protect a compromised same-user process.

Peer messages are untrusted input. Notifications contain only a generated
message reference and fixed retrieval instructions. Agents must fetch a
message through the inbox, evaluate it under their existing permissions, and
acknowledge it only after processing. Do not place credentials or secrets in
message bodies.

## Reporting a vulnerability

Please do not open a public issue for a suspected vulnerability. Use GitHub's
private vulnerability reporting flow from the repository's **Security** tab
and include:

- affected Holler and harness versions;
- operating system and installation method;
- reproduction steps or a minimal proof of concept;
- expected and observed security impact; and
- whether logs or artifacts contain sensitive material.

The repository owner must enable private vulnerability reporting before the
repository is made public. If that private reporting option is unavailable,
do not publish exploit details; contact the repository owner privately through
their GitHub profile first.

## Supported versions

Until the first stable release, security fixes are made only on the latest
tagged alpha and the default development branch. Older alpha builds may be
asked to upgrade before a report is investigated.

## Out of scope for the alpha

- multi-user, multi-node, or network transport security;
- protection from code already executing as the owning OS user;
- release signing or binary attestation;
- organizational authorization supplied by an external policy or work system;
- vulnerabilities in Claude Code, Codex, OpenCode, or their plugin runtimes
  that do not arise from Holler integration.
