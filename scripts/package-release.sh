#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_dir=$(CDPATH= cd -- "${script_dir}/.." && pwd)
version=${HOLLER_VERSION:-}
if [ -z "$version" ]; then
  version=$(git -C "$repo_dir" describe --tags --exact-match 2>/dev/null || true)
fi
version=${version#v}
if [ -z "$version" ]; then
  echo "package-release requires HOLLER_VERSION or an exact v* git tag" >&2
  exit 1
fi

artifact_root=${ARTIFACT_ROOT:-"${repo_dir}/dist"}
platform=$(go env GOOS)-$(go env GOARCH)
stage=$(mktemp -d "${TMPDIR:-/tmp}/holler-release.XXXXXX")
trap 'rm -rf -- "$stage"' EXIT HUP INT TERM
package_root="${stage}/holler-${version}-${platform}"

HOLLER_VERSION="$version" "${script_dir}/build.sh"
mkdir -p "${package_root}/bin" "${package_root}/share/holler/marketplace/plugins" "$artifact_root"
install -m 0755 "${repo_dir}/.build/holler" "${package_root}/bin/holler"
install -m 0755 "${repo_dir}/.build/hollerd" "${package_root}/bin/hollerd"
cp -R "${repo_dir}/connectors/marketplace/.agents" "${package_root}/share/holler/marketplace/"
cp -R "${repo_dir}/connectors/marketplace/.claude-plugin" "${package_root}/share/holler/marketplace/"
cp -R "${repo_dir}/connectors/marketplace/plugins/holler" "${package_root}/share/holler/marketplace/plugins/"
cp -R "${repo_dir}/connectors/marketplace/plugins/claude-holler" "${package_root}/share/holler/marketplace/plugins/"
cp "${repo_dir}/README.md" "${package_root}/"
cp "${repo_dir}/RELEASE-NOTES.md" "${package_root}/"
cp "${repo_dir}/SECURITY.md" "${package_root}/"
cp "${repo_dir}/LICENSE" "${package_root}/"

archive="${artifact_root}/holler-${version}-${platform}.tar.gz"
tar -C "$stage" -czf "$archive" "$(basename "$package_root")"
(cd "$artifact_root" && shasum -a 256 "$(basename "$archive")" > "$(basename "$archive").sha256")
echo "$archive"
