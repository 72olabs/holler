#!/usr/bin/env python3
"""Verify a Holler release archive without trusting or extracting its paths first."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import subprocess
import tarfile
import tempfile


def fail(message: str) -> None:
    raise SystemExit(message)


def load_allowlist(path: Path) -> set[str]:
    values = {
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }
    if not values:
        fail(f"release allowlist is empty: {path}")
    return values


def verify_checksum(archive: Path) -> None:
    sidecar = Path(str(archive) + ".sha256")
    if not sidecar.is_file():
        fail(f"missing checksum sidecar: {sidecar}")
    fields = sidecar.read_text(encoding="utf-8").strip().split()
    if len(fields) != 2 or fields[1].lstrip("*") != archive.name:
        fail(f"invalid checksum sidecar contents: {sidecar}")
    actual = hashlib.sha256(archive.read_bytes()).hexdigest()
    if fields[0].lower() != actual:
        fail(f"checksum mismatch for {archive.name}")


def inspect_archive(archive: Path, version: str, allowlist: set[str]) -> tuple[str, dict[str, tarfile.TarInfo]]:
    roots: set[str] = set()
    files: dict[str, tarfile.TarInfo] = {}
    with tarfile.open(archive, "r:gz") as bundle:
        for member in bundle.getmembers():
            path = PurePosixPath(member.name)
            if path.is_absolute() or ".." in path.parts or len(path.parts) < 1:
                fail(f"unsafe archive path: {member.name}")
            roots.add(path.parts[0])
            if not (member.isdir() or member.isfile()):
                fail(f"release archives may contain only regular files and directories: {member.name}")
            if member.isfile():
                relative = PurePosixPath(*path.parts[1:]).as_posix()
                if not relative or relative in files:
                    fail(f"duplicate or invalid archive file: {member.name}")
                files[relative] = member
    if len(roots) != 1:
        fail(f"archive must have exactly one root directory, found: {sorted(roots)}")
    root = next(iter(roots))
    expected_prefix = f"holler-{version}-"
    if not root.startswith(expected_prefix) or len(root) == len(expected_prefix):
        fail(f"archive root {root!r} does not identify Holler {version}")
    actual = set(files)
    if actual != allowlist:
        unexpected = sorted(actual - allowlist)
        missing = sorted(allowlist - actual)
        fail(f"release archive allowlist mismatch; unexpected={unexpected}, missing={missing}")
    for path in ("bin/holler", "bin/hollerd"):
        if files[path].mode & 0o111 == 0:
            fail(f"packaged binary is not executable: {path}")
    return root, files


def load_json(path: Path) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        fail(f"cannot decode {path}: {error}")


def verify_identity(root: Path, version: str) -> None:
    holler = root / "bin" / "holler"
    try:
        result = subprocess.run(
            [str(holler), "version"], check=True, capture_output=True, text=True, timeout=10
        )
        identity = json.loads(result.stdout)
    except (OSError, subprocess.SubprocessError, json.JSONDecodeError) as error:
        fail(f"cannot read packaged build identity: {error}")
    if identity.get("version") != version or identity.get("dirty") is not False:
        fail(f"packaged build identity does not match {version}: {identity}")
    if not str(identity.get("commit", "")).strip() or identity.get("commit") == "unknown":
        fail(f"packaged build identity has no source commit: {identity}")

    marketplace = root / "share" / "holler" / "marketplace"
    connector_paths = sorted((marketplace / "plugins").glob("*/connector.json"))
    if len(connector_paths) != 3:
        fail(f"expected three connector manifests, found {len(connector_paths)}")
    for path in connector_paths:
        manifest = load_json(path)
        if not isinstance(manifest, dict) or manifest.get("connector_version") != version:
            fail(f"connector version mismatch in {path}")

    plugin_paths = [
        marketplace / "plugins" / "holler" / ".codex-plugin" / "plugin.json",
        marketplace / "plugins" / "claude-holler" / ".claude-plugin" / "plugin.json",
    ]
    for path in plugin_paths:
        plugin = load_json(path)
        if not isinstance(plugin, dict) or plugin.get("version") != version:
            fail(f"plugin version mismatch in {path}")

    claude_marketplace = load_json(marketplace / ".claude-plugin" / "marketplace.json")
    plugins = claude_marketplace.get("plugins", []) if isinstance(claude_marketplace, dict) else []
    if not any(plugin.get("name") == "holler" and plugin.get("version") == version for plugin in plugins):
        fail("Claude marketplace does not advertise the packaged version")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("archive", type=Path)
    parser.add_argument("--version", required=True)
    parser.add_argument(
        "--allowlist",
        type=Path,
        default=Path(__file__).with_name("release-allowlist.txt"),
    )
    args = parser.parse_args()
    archive = args.archive.resolve()
    if not archive.is_file():
        fail(f"release archive does not exist: {archive}")
    verify_checksum(archive)
    allowlist = load_allowlist(args.allowlist)
    root_name, _ = inspect_archive(archive, args.version, allowlist)
    with tempfile.TemporaryDirectory(prefix="holler-archive-verify-") as directory:
        with tarfile.open(archive, "r:gz") as bundle:
            bundle.extractall(directory)
        verify_identity(Path(directory) / root_name, args.version)
    print(f"verified release archive: {archive.name}")


if __name__ == "__main__":
    main()
