#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_dir=$(CDPATH= cd -- "${script_dir}/../.." && pwd)
cd "$repo_dir"

if [ -n "${EXPECTED_UNAME_MACHINE:-}" ]; then
  actual_machine=$(uname -m)
  if [ "$actual_machine" != "$EXPECTED_UNAME_MACHINE" ]; then
    echo "runner architecture mismatch: got ${actual_machine}, want ${EXPECTED_UNAME_MACHINE}" >&2
    exit 1
  fi
fi

unformatted=$(gofmt -l .)
if [ -n "$unformatted" ]; then
  echo "gofmt is required for:" >&2
  printf '%s\n' "$unformatted" >&2
  exit 1
fi

go mod tidy -diff
HOLLER_REQUIRE_OPENCODE_PLUGIN_TEST=1 go test ./...
go vet ./...
HOLLER_REQUIRE_OPENCODE_PLUGIN_TEST=1 go test -race ./...
python3 -m unittest discover -s scripts/canary/tests -p 'test_*.py'
scripts/build.sh

run_key=${GITHUB_RUN_ID:-local-$(date -u +%Y%m%dT%H%M%SZ)-$$}
evidence_root=${HOLLER_CI_EVIDENCE_ROOT:-"${repo_dir}/.runs/ci/${run_key}"}
mkdir -p "$evidence_root"
lab_summary="${evidence_root}/lab-results.json"
if ! ./.build/holler lab run --all --timeout 20s --output "${evidence_root}/lab" >"$lab_summary"; then
  cat "$lab_summary" >&2
  exit 1
fi

version=${HOLLER_VERSION:-}
if [ -z "$version" ]; then
  version=$(sed -n 's/^const ConnectorVersion = "\([^"]*\)"/\1/p' internal/connector/manifest.go)
fi
version=${version#v}
if [ -z "$version" ]; then
  echo "could not determine the connector version" >&2
  exit 1
fi

artifact_root=${ARTIFACT_ROOT:-"${repo_dir}/dist"}
HOLLER_VERSION="$version" ARTIFACT_ROOT="$artifact_root" scripts/package-release.sh
archive="${artifact_root}/holler-${version}-$(go env GOOS)-$(go env GOARCH).tar.gz"
python3 scripts/ci/verify_release_archive.py "$archive" --version "$version"
GOCACHE=${GOCACHE:-$(go env GOCACHE)} \
GOMODCACHE=${GOMODCACHE:-$(go env GOMODCACHE)} \
GOPATH=${GOPATH:-$(go env GOPATH)} \
python3 scripts/ci/packaged_black_box.py "$archive" --version "$version"
