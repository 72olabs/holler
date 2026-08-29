## What changed

Describe the user-visible behavior and why it belongs in Holler.

## Validation

- [ ] `go test ./...`
- [ ] `go vet ./...`
- [ ] `go test -race ./...`
- [ ] `./scripts/build.sh`
- [ ] Connector manifest/package hash updated if packaged assets changed
- [ ] Live canary included only when deterministic tests cannot prove the behavior

## Compatibility and security

- [ ] I described protocol, schema, migration, or harness-version impact.
- [ ] I called out new permissions, hooks, processes, or network/file access.
- [ ] Notifications contain references only; peer-controlled bodies remain behind inbox fetch.
- [ ] No transcripts, databases, credentials, private prompts, or identifying local paths are included.
