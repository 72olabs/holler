#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
commit=${HOLLER_COMMIT:-}
if [ -z "$commit" ]; then
  commit=$(git -C "$repo_root" rev-parse HEAD 2>/dev/null || true)
fi
if [ -z "$commit" ]; then
  commit=unknown
fi
dirty=${HOLLER_DIRTY:-false}
if git -C "$repo_root" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  if [ -n "$(git -C "$repo_root" status --porcelain)" ]; then
    dirty=true
  fi
fi
built_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
version=${HOLLER_VERSION:-}
if [ -z "$version" ]; then
  version=$(git -C "$repo_root" describe --tags --exact-match 2>/dev/null || true)
fi
version=${version#v}
if [ -z "$version" ]; then
  version=dev
fi
ldflags="-X github.com/72olabs/holler/internal/buildinfo.Version=$version -X github.com/72olabs/holler/internal/buildinfo.Commit=$commit -X github.com/72olabs/holler/internal/buildinfo.Dirty=$dirty -X github.com/72olabs/holler/internal/buildinfo.BuiltAt=$built_at"
build_root=${BUILD_ROOT:-"$repo_root/.build"}

mkdir -p "$build_root"
(
  cd "$repo_root"
  go build -trimpath -ldflags "$ldflags" -o "$build_root/holler" ./cmd/holler
  go build -trimpath -ldflags "$ldflags" -o "$build_root/hollerd" ./cmd/hollerd
)
