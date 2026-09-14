#!/usr/bin/env python3
"""Exercise the extracted Holler product in a disposable, credential-free home."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tarfile
import tempfile
import time


SCRIPT_DIR = Path(__file__).resolve().parent


def fail(message: str) -> None:
    raise RuntimeError(message)


def run(command: list[str], *, env: dict[str, str], cwd: Path, timeout: int = 20) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(command, env=env, cwd=cwd, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        fail(
            f"command failed ({result.returncode}): {' '.join(command)}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def json_run(command: list[str], *, env: dict[str, str], cwd: Path) -> object:
    result = run(command, env=env, cwd=cwd)
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as error:
        fail(f"command did not emit JSON: {' '.join(command)}: {error}\n{result.stdout}")


def hash_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def normalized(path: Path) -> str:
    return os.path.normcase(os.path.abspath(path))


def inside(path: Path, parent: Path) -> bool:
    candidate = normalized(path)
    root = normalized(parent)
    return candidate == root or candidate.startswith(root + os.sep)


def host_snapshot(home: Path, exclusions: list[Path]) -> dict[str, tuple[int, int, int, int]]:
    snapshot: dict[str, tuple[int, int, int, int]] = {}
    if not home.is_dir():
        return snapshot
    excluded_paths = [normalized(path) for path in exclusions if path.exists() and inside(path, home)]

    def excluded(path: Path) -> bool:
        candidate = normalized(path)
        return any(candidate == root or candidate.startswith(root + os.sep) for root in excluded_paths)

    for root, dirs, files in os.walk(home, topdown=True, followlinks=False):
        root_path = Path(root)
        dirs[:] = [name for name in dirs if not excluded(root_path / name)]
        for name in [*dirs, *files]:
            path = root_path / name
            if excluded(path):
                continue
            try:
                stat = path.lstat()
            except FileNotFoundError:
                continue
            snapshot[path.relative_to(home).as_posix()] = (
                stat.st_mode,
                stat.st_size,
                stat.st_mtime_ns,
                stat.st_ino,
            )
    return snapshot


def wait_for_absence(path: Path, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while path.exists() and time.monotonic() < deadline:
        time.sleep(0.05)
    if path.exists():
        fail(f"path remained after teardown: {path}")


def stop_leftover_daemon(pid_path: Path) -> None:
    try:
        pid = int(pid_path.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return


def assert_setup(result: object, harness: str) -> None:
    if not isinstance(result, dict) or result.get("harness") != harness or result.get("applied") is not True:
        fail(f"unexpected {harness} setup result: {result}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("archive", type=Path)
    parser.add_argument("--version", required=True)
    args = parser.parse_args()
    archive = args.archive.resolve()

    original_home = Path.home().resolve()
    exclusions = []
    for name in ("GOCACHE", "GOMODCACHE", "GOPATH"):
        value = os.environ.get(name, "").strip()
        if value:
            exclusions.append(Path(value))
    audit_runner_home = os.environ.get("GITHUB_ACTIONS") == "true"
    before = host_snapshot(original_home, exclusions) if audit_runner_home else {}

    with tempfile.TemporaryDirectory(prefix="holler ci ünicode ") as temporary:
        root = Path(temporary)
        extract_root = root / "installed product"
        extract_root.mkdir()
        with tarfile.open(archive, "r:gz") as bundle:
            bundle.extractall(extract_root)
        package_roots = list(extract_root.iterdir())
        if len(package_roots) != 1 or not package_roots[0].is_dir():
            fail("release archive did not extract to one product directory")
        package = package_roots[0]
        holler = package / "bin" / "holler"
        hollerd = package / "bin" / "hollerd"

        sandbox = root / "isolated user home"
        home = sandbox / "home"
        claude_home = sandbox / "claude config"
        codex_home = sandbox / "codex config"
        # Unix-domain socket limits are short on macOS. Keep the runtime path
        # deliberately compact while exercising spaces and Unicode everywhere
        # users commonly choose paths: installation, home, configs, and project.
        runtime = root / "r"
        fake_bin = sandbox / "fake clients"
        fake_state = sandbox / "fake state"
        non_git = sandbox / "project with spaces π"
        for directory in (home, claude_home, codex_home, runtime, fake_bin, fake_state, non_git):
            directory.mkdir(parents=True, exist_ok=True)

        for name in ("claude", "codex"):
            (fake_bin / name).symlink_to(SCRIPT_DIR / "fake_harness.py")
        for name in ("launchctl", "systemctl"):
            (fake_bin / name).symlink_to(SCRIPT_DIR / "fake_service_manager.py")

        socket = runtime / "holler.sock"
        database = runtime / "holler.sqlite3"
        pid_path = runtime / "service.pid"
        env = os.environ.copy()
        env.update(
            {
                "HOME": str(home),
                "HOLLER_HOME": str(runtime),
                "HOLLER_SOCKET": str(socket),
                "HOLLER_RUNTIME_PATH": str(runtime / "bin-path"),
                "CODEX_HOME": str(codex_home),
                "CLAUDE_CONFIG_DIR": str(claude_home),
                "XDG_CONFIG_HOME": str(sandbox / "xdg config"),
                "XDG_DATA_HOME": str(sandbox / "xdg data"),
                "XDG_STATE_HOME": str(sandbox / "xdg state"),
                "XDG_CACHE_HOME": str(sandbox / "xdg cache"),
                "PATH": str(fake_bin) + os.pathsep + env.get("PATH", ""),
                "HOLLER_TEST_FAKE_CLIENT_STATE": str(fake_state),
                "HOLLER_TEST_DAEMON_BIN": str(hollerd),
                "HOLLER_TEST_SOCKET": str(socket),
                "HOLLER_TEST_DB": str(database),
                "HOLLER_TEST_SERVICE_PID": str(pid_path),
                "HOLLER_TEST_SERVICE_LOG_DIR": str(runtime / "logs"),
            }
        )

        claude_settings = claude_home / "settings.json"
        claude_original = b'{"theme":"dark","permissions":{"allow":["Read"]},"unrelated":{"keep":"exactly"}}\n'
        claude_settings.write_bytes(claude_original)
        codex_config = codex_home / "config.toml"
        codex_original = b'# unrelated user configuration\nmodel = "gpt-test"\ncustom_value = "keep exactly"\n'
        codex_config.write_bytes(codex_original)
        unrelated = home / "unrelated-config.txt"
        unrelated.write_bytes(b"must remain byte-identical\n")
        unrelated_hash = hash_bytes(unrelated.read_bytes())

        common = [
            "--yes",
            "--project",
            "ci-black-box",
            "--project-root",
            str(non_git),
            "--socket",
            str(socket),
            "--runtime-path",
            str(runtime / "bin-path"),
            "--daemon-binary",
            str(hollerd),
        ]

        try:
            claude_command = [
                str(holler),
                "setup",
                "claude",
                *common,
                "--client-binary",
                str(fake_bin / "claude"),
            ]
            first = json_run(claude_command, env=env, cwd=non_git)
            assert_setup(first, "claude")
            claude_after_first = claude_settings.read_bytes()
            first = json_run(claude_command, env=env, cwd=non_git)
            assert_setup(first, "claude")
            if claude_settings.read_bytes() != claude_after_first:
                fail("repeated Claude setup was not byte-idempotent")

            codex_command = [
                str(holler),
                "setup",
                "codex",
                *common,
                "--client-binary",
                str(fake_bin / "codex"),
            ]
            first = json_run(codex_command, env=env, cwd=non_git)
            assert_setup(first, "codex")
            codex_after_first = codex_config.read_bytes()
            first = json_run(codex_command, env=env, cwd=non_git)
            assert_setup(first, "codex")
            if codex_config.read_bytes() != codex_after_first:
                fail("repeated Codex setup was not byte-idempotent")

            if (Path(str(claude_settings) + ".bak")).read_bytes() != claude_original:
                fail("Claude setup backup did not preserve the original settings bytes")
            if (Path(str(codex_config) + ".bak")).read_bytes() != codex_original:
                fail("Codex setup backup did not preserve the original config bytes")
            if hash_bytes(unrelated.read_bytes()) != unrelated_hash:
                fail("setup changed an unrelated user file")

            claude_data = json.loads(claude_settings.read_text(encoding="utf-8"))
            if claude_data.get("theme") != "dark" or claude_data.get("unrelated") != {"keep": "exactly"}:
                fail("Claude setup did not preserve unrelated settings")
            codex_text = codex_config.read_text(encoding="utf-8")
            if not codex_text.startswith(codex_original.decode("utf-8")):
                fail("Codex setup did not preserve the unrelated config prefix byte-for-byte")

            json_run(
                [str(holler), "inbox", "--socket", str(socket), "--actor", "receiver", "--run", "receiver-run"],
                env=env,
                cwd=non_git,
            )
            sent = json_run(
                [
                    str(holler),
                    "send",
                    "--socket",
                    str(socket),
                    "--actor",
                    "sender",
                    "--run",
                    "sender-run",
                    "--to-actor",
                    "receiver",
                    "--idempotency-key",
                    "packaged-black-box-roundtrip",
                    "--body",
                    '{"text":"hello from packaged black box π"}',
                ],
                env=env,
                cwd=non_git,
            )
            sent_message = sent.get("message", {}) if isinstance(sent, dict) else {}
            message_id = sent_message.get("message_id") if isinstance(sent_message, dict) else None
            if not isinstance(message_id, str) or not message_id:
                fail(f"packaged send returned no message id: {sent}")
            claim = json_run(
                [str(holler), "claim", "--socket", str(socket), "--actor", "receiver", "--run", "receiver-run"],
                env=env,
                cwd=non_git,
            )
            claimed_message = claim.get("message", {}) if isinstance(claim, dict) else {}
            claimed_id = claimed_message.get("message_id") if isinstance(claimed_message, dict) else None
            if claimed_id != message_id:
                fail(f"packaged claim did not return the sent message: {claim}")
            token = claim.get("lease_token")
            if not isinstance(token, str) or not token:
                fail(f"packaged claim returned no lease token: {claim}")
            json_run(
                [
                    str(holler),
                    "ack",
                    "--socket",
                    str(socket),
                    "--actor",
                    "receiver",
                    "--run",
                    "receiver-run",
                    "--message",
                    message_id,
                    "--lease-token",
                    token,
                ],
                env=env,
                cwd=non_git,
            )
            inbox = json_run(
                [str(holler), "inbox", "--socket", str(socket), "--actor", "receiver", "--run", "receiver-run"],
                env=env,
                cwd=non_git,
            )
            if inbox != []:
                fail(f"recipient inbox was not empty after acknowledgement: {inbox}")

            assert_setup(
                json_run(claude_command[:3] + ["--remove", *claude_command[3:]], env=env, cwd=non_git),
                "claude",
            )
            if not pid_path.exists():
                fail("removing Claude stopped the daemon while Codex remained configured")
            assert_setup(
                json_run(codex_command[:3] + ["--remove", *codex_command[3:]], env=env, cwd=non_git),
                "codex",
            )
            wait_for_absence(socket)
            if pid_path.exists():
                fail("removing the final connector left the daemon running")

            restored_claude = json.loads(claude_settings.read_text(encoding="utf-8"))
            if restored_claude != json.loads(claude_original):
                fail("Claude removal did not preserve the original unrelated settings")
            if codex_config.read_bytes() != codex_original:
                fail("Codex removal did not restore the unrelated config bytes")
            if hash_bytes(unrelated.read_bytes()) != unrelated_hash:
                fail("removal changed an unrelated user file")
            for selection in (home / ".holler" / "connectors" / "claude.json", home / ".holler" / "connectors" / "codex.json"):
                if selection.exists():
                    fail(f"connector selection remained after removal: {selection}")
        finally:
            stop_leftover_daemon(pid_path)

    if audit_runner_home:
        after = host_snapshot(original_home, exclusions)
        if before != after:
            created = sorted(after.keys() - before.keys())
            removed = sorted(before.keys() - after.keys())
            changed = sorted(path for path in before.keys() & after.keys() if before[path] != after[path])
            fail(
                "packaged black-box wrote outside its isolated prefix; "
                f"created={created[:20]}, removed={removed[:20]}, changed={changed[:20]}"
            )

    print(f"packaged black-box passed for Holler {args.version}")


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, subprocess.SubprocessError, tarfile.TarError) as error:
        print(error, file=sys.stderr)
        raise SystemExit(1)
