#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_dir=$(CDPATH= cd -- "${script_dir}/.." && pwd)
prefix=${PREFIX:-/usr/local}
destination_root=${DESTDIR:-}
bin_dir=${destination_root}${prefix}/bin
marketplace_dir=${destination_root}${prefix}/share/holler/marketplace

"${repo_dir}/scripts/build.sh"

install -d "${bin_dir}" "${marketplace_dir}/plugins"
install -m 0755 "${repo_dir}/.build/holler" "${bin_dir}/holler"
install -m 0755 "${repo_dir}/.build/hollerd" "${bin_dir}/hollerd"

rm -rf -- \
  "${marketplace_dir}/.agents" \
  "${marketplace_dir}/.claude-plugin" \
  "${marketplace_dir}/plugins/holler" \
  "${marketplace_dir}/plugins/claude-holler"
cp -R "${repo_dir}/connectors/marketplace/.agents" "${marketplace_dir}/"
cp -R "${repo_dir}/connectors/marketplace/.claude-plugin" "${marketplace_dir}/"
cp -R "${repo_dir}/connectors/marketplace/plugins/holler" "${marketplace_dir}/plugins/"
cp -R "${repo_dir}/connectors/marketplace/plugins/claude-holler" "${marketplace_dir}/plugins/"

echo "Installed Holler binaries to ${bin_dir}"
echo "Installed connector marketplace to ${marketplace_dir}"
