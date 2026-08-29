#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_dir=$(CDPATH= cd -- "${script_dir}/.." && pwd)

if [ -n "${HOLLER_BIN:-}" ]; then
  holler_bin=${HOLLER_BIN}
elif [ -x "${repo_dir}/.build/holler" ]; then
  holler_bin=${repo_dir}/.build/holler
elif command -v holler >/dev/null 2>&1; then
  holler_bin=$(command -v holler)
else
  echo "holler executable not found; build it first or set HOLLER_BIN" >&2
  exit 2
fi

exec "${holler_bin}" connector setup \
  --harness opencode \
  --package-source "${repo_dir}/connectors/marketplace/plugins/opencode-holler" \
  --project-root "${repo_dir}" \
  "$@"
