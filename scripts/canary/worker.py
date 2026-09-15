#!/usr/bin/env python3
"""Credentialed sandbox worker for the core Holler real-client canary."""

from __future__ import annotations

import argparse
import errno
import fcntl
import json
import os
from pathlib import Path
import pty
import re
import selectors
import signal
import struct
import subprocess
import sys
import tarfile
import tempfile
import termios
import time
import tomllib
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from budget import BudgetExceeded, BudgetLedger  # noqa: E402
from clients import claude_live_command, claude_print_command, codex_exec_command, codex_live_command  # noqa: E402
from manifest import ManifestError, canonical_json, load_request, sha256_bytes, sha256_file  # noqa: E402


SUPPORTED_REAL_SCENARIOS = {"C0", "C1", "C2", "C3", "C4", "C5"}
ANSI_ESCAPE = re.compile(r"\x1b(?:\[[0-?]*[ -/]*[@-~]|\][^\x07]*(?:\x07|\x1b\\))")
TERMINAL_QUERY_RESPONSES = {
    b"\x1b[6n": b"\x1b[1;1R",
    b"\x1b[c": b"\x1b[?1;2c",
    b"\x1b[>c": b"\x1b[>0;0;0c",
    b"\x1b[?u": b"\x1b[?0u",
    b"\x1b]10;?\x07": b"\x1b]10;rgb:ffff/ffff/ffff\x07",
    b"\x1b]10;?\x1b\\": b"\x1b]10;rgb:ffff/ffff/ffff\x1b\\",
    b"\x1b]11;?\x07": b"\x1b]11;rgb:0000/0000/0000\x07",
    b"\x1b]11;?\x1b\\": b"\x1b]11;rgb:0000/0000/0000\x1b\\",
}


def terminal_query_responses(data: bytes, *, previous_tail_length: int = 0) -> bytes:
    """Return standard terminal replies for queries ending in newly read bytes."""
    replies = bytearray()
    for query, response in TERMINAL_QUERY_RESPONSES.items():
        start = 0
        while True:
            index = data.find(query, start)
            if index < 0:
                break
            if index + len(query) > previous_tail_length:
                replies.extend(response)
            start = index + len(query)
    return bytes(replies)


class CanaryFailure(RuntimeError):
    pass


class PtyChild:
    """Small wait/poll adapter for a child created by forkpty."""

    def __init__(self, pid: int, command: list[str]):
        self.pid = pid
        self.command = command
        self.returncode: int | None = None

    def poll(self) -> int | None:
        if self.returncode is not None:
            return self.returncode
        try:
            waited, status = os.waitpid(self.pid, os.WNOHANG)
        except ChildProcessError:
            return self.returncode
        if waited == self.pid:
            self.returncode = os.waitstatus_to_exitcode(status)
        return self.returncode

    def wait(self, timeout: float) -> int:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            result = self.poll()
            if result is not None:
                return result
            time.sleep(0.05)
        raise subprocess.TimeoutExpired(self.command, timeout)


def make_failure_evidence(
    request: dict[str, Any],
    *,
    results: list[dict[str, Any]],
    usage: dict[str, Any],
    scenario: str,
    check: str,
    error: BaseException,
) -> dict[str, Any]:
    evidence: dict[str, Any] = {
        "schema_version": 1,
        "kind": "holler-canary-evidence",
        "driver": "real",
        "request_hash": request["request_hash"],
        "source": request["source"],
        "tier": request["tier"],
        "status": "FAIL",
        "results": results,
        "failure": {"scenario": scenario, "check": check, "type": type(error).__name__},
        "usage": usage,
        "limits": request["budget"],
        "message_bodies_included": False,
    }
    evidence["evidence_hash"] = sha256_bytes(canonical_json(evidence))
    return evidence


def run_command(
    command: list[str],
    *,
    cwd: Path,
    env: dict[str, str],
    timeout: int,
    stdin: str | None = None,
) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            command,
            cwd=cwd,
            env=env,
            input=stdin,
            text=True,
            capture_output=True,
            timeout=timeout,
        )
    except (OSError, subprocess.SubprocessError) as error:
        raise CanaryFailure(f"command failed to run: {command[0]}: {error}") from error
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()[-2000:]
        raise CanaryFailure(f"{command[0]} exited {result.returncode}: {detail}")
    return result


def safe_extract(archive: Path, destination: Path) -> Path:
    with tarfile.open(archive, "r:gz") as bundle:
        members = bundle.getmembers()
        roots = set()
        for member in members:
            path = Path(member.name)
            if path.is_absolute() or ".." in path.parts or not path.parts:
                raise CanaryFailure(f"unsafe archive path: {member.name}")
            if not (member.isdir() or member.isfile()):
                raise CanaryFailure(f"unsupported archive member: {member.name}")
            roots.add(path.parts[0])
        if len(roots) != 1:
            raise CanaryFailure("archive must contain exactly one root directory")
        bundle.extractall(destination)
    return destination / next(iter(roots))


def parse_version(output: str) -> str:
    match = re.search(r"(?<![0-9])([0-9]+\.[0-9]+\.[0-9]+)(?![0-9])", output)
    if not match:
        raise CanaryFailure(f"cannot parse client version from {output.strip()!r}")
    return match.group(1)


def codex_reported_tokens(output: str) -> int:
    last_usage: dict[str, Any] | None = None
    for line in output.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if event.get("type") == "turn.completed" and isinstance(event.get("usage"), dict):
            last_usage = event["usage"]
    if last_usage is None:
        return 0
    direct = last_usage.get("total_tokens")
    if isinstance(direct, int):
        return direct
    return sum(
        value
        for key, value in last_usage.items()
        if isinstance(value, int) and key in {"input_tokens", "output_tokens"}
    )


def claude_cost(output: str) -> float:
    try:
        value = json.loads(output).get("total_cost_usd", 0.0)
    except json.JSONDecodeError:
        return 0.0
    return float(value) if isinstance(value, (int, float)) else 0.0


def marker_instruction(marker: str) -> str:
    """Describe a marker without placing its literal value in model-visible input."""
    parts = marker.split("_")
    if len(parts) < 2 or any(not part for part in parts):
        raise ValueError("markers must contain at least two non-empty underscore-separated tokens")
    quoted = ", ".join(repr(part) for part in parts)
    return f"the marker formed by joining these tokens with underscores: {quoted}"


def lifecycle_evidence_complete(events: object, *, actor: str, run_id: str) -> bool:
    """Require correlated Claude registration and hydration lifecycle events."""
    if not isinstance(events, list):
        raise CanaryFailure("operational lifecycle events are not a list")
    seen: set[str] = set()
    for event in events:
        if not isinstance(event, dict) or event.get("actor_id") != actor:
            continue
        payload = event.get("payload")
        if not isinstance(payload, dict) or payload.get("run_id") != run_id:
            continue
        if payload.get("harness") == "claude":
            seen.add(str(event.get("kind")))
    return {"session.registered", "startup.hydrated"}.issubset(seen)


def actor_delivery_counts(directory: object, *, actor: str) -> tuple[int, int]:
    """Return operator-observed unclaimed and active-claim counts for one actor."""
    if not isinstance(directory, dict) or not isinstance(directory.get("actors"), list):
        raise CanaryFailure("actor directory has an invalid shape")
    for entry in directory["actors"]:
        if not isinstance(entry, dict) or entry.get("actor") != actor:
            continue
        unclaimed = entry.get("unclaimed_messages")
        active = entry.get("active_claims")
        if not isinstance(unclaimed, int) or not isinstance(active, int):
            raise CanaryFailure("actor directory has invalid delivery counts")
        return unclaimed, active
    return 0, 0


def actor_for_run(
    directory: object, *, run_id: str, harness: str, live_only: bool = True
) -> str | None:
    """Resolve a live allocated actor from operator directory metadata."""
    if not isinstance(directory, dict) or not isinstance(directory.get("actors"), list):
        raise CanaryFailure("actor directory has an invalid shape")
    matches: list[str] = []
    for entry in directory["actors"]:
        if not isinstance(entry, dict) or not isinstance(entry.get("sessions"), list):
            continue
        for session in entry["sessions"]:
            if (
                isinstance(session, dict)
                and session.get("run_id") == run_id
                and session.get("harness") == harness
                and (not live_only or session.get("state") == "live")
                and isinstance(entry.get("actor"), str)
            ):
                matches.append(entry["actor"])
    if len(set(matches)) > 1:
        raise CanaryFailure("one run is live under multiple actors")
    return matches[0] if matches else None


def alias_collision_visible(conditions: object, *, alias: str) -> bool:
    if not isinstance(conditions, list):
        raise CanaryFailure("operator conditions are not a list")
    return any(
        isinstance(condition, dict)
        and condition.get("kind") == "alias_collision"
        and condition.get("subject") == alias
        and str(condition.get("state", "")).startswith("active_")
        for condition in conditions
    )


def delivery_event_attempts(events: object, *, message_id: str, actor: str) -> list[int]:
    """Return recorded claim attempts for a message without retaining its body."""
    if not isinstance(events, list):
        raise CanaryFailure("operational lifecycle events are not a list")
    attempts: list[int] = []
    for event in events:
        if not isinstance(event, dict):
            continue
        if (
            event.get("kind") != "delivery.claimed"
            or event.get("message_id") != message_id
            or event.get("actor_id") != actor
        ):
            continue
        payload = event.get("payload")
        attempt = payload.get("attempt") if isinstance(payload, dict) else None
        if isinstance(attempt, int):
            attempts.append(attempt)
    return attempts


def delivery_was_acked(events: object, *, message_id: str, actor: str) -> bool:
    if not isinstance(events, list):
        raise CanaryFailure("operational lifecycle events are not a list")
    return any(
        isinstance(event, dict)
        and event.get("kind") == "delivery.acked"
        and event.get("message_id") == message_id
        and event.get("actor_id") == actor
        for event in events
    )


def minted_actors(events: object) -> list[str]:
    if not isinstance(events, list):
        raise CanaryFailure("durable events are not a list")
    return [
        event["actor_id"]
        for event in events
        if isinstance(event, dict)
        and event.get("kind") == "actor.minted"
        and isinstance(event.get("actor_id"), str)
    ]


def claude_fixture_ready(config: object, *, fixture: Path, version: str) -> bool:
    """Check the non-secret Claude preferences required for an unattended cleanroom TUI."""
    if not isinstance(config, dict):
        return False
    projects = config.get("projects")
    project = projects.get(str(fixture)) if isinstance(projects, dict) else None
    return (
        config.get("hasCompletedOnboarding") is True
        and config.get("lastOnboardingVersion") == version
        and isinstance(project, dict)
        and project.get("hasTrustDialogAccepted") is True
    )


def codex_config_with_trusted_fixture(text: str, fixture: Path) -> str:
    """Merge one trusted cleanroom project into Codex TOML without altering policy."""
    section = f"[projects.{json.dumps(str(fixture))}]"
    pattern = re.compile(rf"(?ms)^{re.escape(section)}\n(?P<body>.*?)(?=^\[|\Z)")
    match = pattern.search(text)
    if match:
        body = match.group("body")
        trust = re.compile(r'(?m)^trust_level[ \t]*=[ \t]*"[^"]*"[ \t]*$')
        if trust.search(body):
            updated = trust.sub('trust_level = "trusted"', body, count=1)
        else:
            updated = 'trust_level = "trusted"\n' + body
        return text[: match.start("body")] + updated + text[match.end("body") :]
    separator = "" if not text or text.endswith("\n\n") else "\n" if text.endswith("\n") else "\n\n"
    return text + separator + section + '\ntrust_level = "trusted"\n'


def codex_hook_trust_ready(config: object) -> bool:
    """Require persisted trust for Holler's SessionStart and SessionEnd hooks."""
    if not isinstance(config, dict):
        return False
    hooks = config.get("hooks")
    state = hooks.get("state") if isinstance(hooks, dict) else None
    if not isinstance(state, dict):
        return False
    trusted_events: set[str] = set()
    for key, value in state.items():
        if not isinstance(key, str) or not key.startswith("holler@holler:hooks/hooks.json:"):
            continue
        trusted_hash = value.get("trusted_hash") if isinstance(value, dict) else None
        if not isinstance(trusted_hash, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", trusted_hash):
            continue
        if ":session_start:" in key:
            trusted_events.add("session_start")
        if ":session_end:" in key:
            trusted_events.add("session_end")
    return trusted_events == {"session_start", "session_end"}


def doctor_command(
    holler: Path,
    *,
    harness: str,
    actor: str,
    attention: str,
    project: Path,
    socket: Path,
    env: dict[str, str],
) -> list[str]:
    command = [
        str(holler), "connector", "doctor", "--harness", harness,
        "--profile", "live-review", "--project", str(project),
        "--attention", attention, "--actor", actor, "--socket", str(socket),
    ]
    if harness == "codex":
        command.extend(["--policy", str(Path(env["CODEX_HOME"]) / "holler.config.toml")])
    return command


class PtyProcess:
    def __init__(self, command: list[str], *, cwd: Path, env: dict[str, str]):
        pid, master = pty.fork()
        if pid == 0:
            try:
                os.chdir(cwd)
                os.execvpe(command[0], command, env)
            except BaseException:
                os._exit(127)
        fcntl.ioctl(master, termios.TIOCSWINSZ, struct.pack("HHHH", 40, 120, 0, 0))
        self.master = master
        self.buffer = bytearray()
        self.query_tail = b""
        self.process = PtyChild(pid, command)
        os.set_blocking(master, False)
        self.selector = selectors.DefaultSelector()
        self.selector.register(master, selectors.EVENT_READ)

    def send(self, text: str) -> None:
        os.write(self.master, text.encode("utf-8"))

    def checkpoint(self) -> int:
        self._read_available(0)
        return len(self.buffer)

    def normalized_output(self, *, after: int = 0) -> str:
        return ANSI_ESCAPE.sub("", self.buffer[after:].decode("utf-8", errors="replace"))

    def wait_until_quiet(self, timeout: float, *, quiet_seconds: float = 0.5) -> None:
        """Wait until an interactive client has rendered output and stopped repainting."""
        deadline = time.monotonic() + timeout
        last_size = len(self.buffer)
        quiet_since = time.monotonic() if last_size else None
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise CanaryFailure("interactive client exited before becoming ready")
            self._read_available(min(0.1, deadline - time.monotonic()))
            size = len(self.buffer)
            if size != last_size:
                last_size = size
                quiet_since = time.monotonic()
            elif quiet_since is not None and time.monotonic() - quiet_since >= quiet_seconds:
                return
        raise CanaryFailure("interactive client did not reach a stable input-ready state")

    def wait_until_ready(self, marker: str, timeout: float, *, suffix: bool = False) -> None:
        """Wait for a stable input marker in normalized terminal output."""
        deadline = time.monotonic() + timeout
        last_size = len(self.buffer)
        ready_since: float | None = None
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise CanaryFailure("interactive client exited before its input prompt was ready")
            self._read_available(min(0.1, deadline - time.monotonic()))
            size = len(self.buffer)
            text = ANSI_ESCAPE.sub("", self.buffer.decode("utf-8", errors="replace"))
            ready = text.rstrip().endswith(marker) if suffix else marker in text
            if not ready or size != last_size:
                ready_since = time.monotonic() if ready else None
                last_size = size
            elif ready_since is not None and time.monotonic() - ready_since >= 0.5:
                return
        raise CanaryFailure("interactive client input prompt did not become ready")

    def submit(self, prompt: str, *, marker: str, timeout: float) -> None:
        if marker in prompt:
            raise CanaryFailure("interactive prompt contains its expected output marker")
        after = self.checkpoint()
        self.send(prompt)
        time.sleep(0.1)
        self.send("\r")
        self.wait_for(marker, timeout, after=after)

    def wait_for(self, marker: str, timeout: float, *, after: int = 0) -> None:
        deadline = time.monotonic() + timeout
        marker_bytes = marker.encode("utf-8")
        while time.monotonic() < deadline:
            if marker_bytes in self.buffer[after:]:
                return
            if self.process.poll() is not None:
                raise CanaryFailure(f"interactive client exited before {marker}")
            self._read_available(min(0.25, deadline - time.monotonic()))
        raise CanaryFailure(f"timed out waiting for expected client marker {marker}")

    def _read_available(self, timeout: float) -> None:
        for key, _ in self.selector.select(timeout=timeout):
            try:
                chunk = os.read(key.fd, 65536)
            except BlockingIOError:
                continue
            except OSError as error:
                if error.errno == errno.EIO:
                    return
                raise
            if chunk:
                self.buffer.extend(chunk)
                combined = self.query_tail + chunk
                response = terminal_query_responses(
                    combined,
                    previous_tail_length=len(self.query_tail),
                )
                if response:
                    os.write(self.master, response)
                maximum = max(len(query) for query in TERMINAL_QUERY_RESPONSES)
                self.query_tail = combined[-(maximum - 1):]

    def close(self, *, abrupt: bool = False) -> None:
        if self.process.poll() is None:
            if abrupt:
                self._signal(signal.SIGKILL)
                self.process.wait(timeout=5)
            else:
                self._signal(signal.SIGTERM)
                try:
                    self.process.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    self._signal(signal.SIGKILL)
                    self.process.wait(timeout=5)
        self.selector.close()
        os.close(self.master)

    def _signal(self, requested: signal.Signals) -> None:
        try:
            os.killpg(self.process.pid, requested)
        except (ProcessLookupError, PermissionError):
            if self.process.poll() is None:
                try:
                    os.kill(self.process.pid, requested)
                except ProcessLookupError:
                    pass


class Worker:
    def __init__(self, request: dict[str, Any], archive: Path, root: Path):
        self.request = request
        self.archive = archive
        self.root = root
        self.home = root / "home"
        self.fixture = Path(request["execution"]["runner_fixture"])
        self.runtime = root / "runtime"
        self.socket = self.runtime / "holler.sock"
        self.database = self.runtime / "holler.sqlite3"
        self.package = safe_extract(archive, root / "package")
        self.holler = self.package / "bin" / "holler"
        self.hollerd = self.package / "bin" / "hollerd"
        self.env = os.environ.copy()
        self.env.update(
            {
                "HOME": str(self.home),
                "HOLLER_HOME": str(self.runtime),
                "HOLLER_SOCKET": str(self.socket),
                "CLAUDE_CONFIG_DIR": os.environ.get(
                    "CLAUDE_CONFIG_DIR", "/home/daytona/.holler-canary-auth/claude"
                ),
                "CODEX_HOME": os.environ.get("CODEX_HOME", "/home/daytona/.holler-canary-auth/codex"),
                "TERM": "xterm-256color",
                "NO_COLOR": "1",
            }
        )
        self.ledger = BudgetLedger(request["budget"])
        self.daemon: subprocess.Popen[str] | None = None
        self.results: list[dict[str, Any]] = []
        self.active_scenario = "initialization"
        self.active_check = "initialization"

    def prepare(self) -> None:
        for directory in (self.home, self.runtime, self.package.parent):
            directory.mkdir(parents=True, exist_ok=True)
        if not (self.fixture / ".git").is_dir():
            raise CanaryFailure("stable canary fixture is not initialized")
        if run_command(
            ["git", "status", "--porcelain", "--untracked-files=all"],
            cwd=self.fixture,
            env=self.env,
            timeout=30,
        ).stdout.strip():
            raise CanaryFailure("stable canary fixture is not clean")
        self.start_daemon()

    def start_daemon(self) -> None:
        self.runtime.mkdir(parents=True, exist_ok=True)
        log = (self.runtime / "hollerd.log").open("a", encoding="utf-8")
        self.daemon = subprocess.Popen(
            [str(self.hollerd), "--db", str(self.database), "--socket", str(self.socket)],
            cwd=self.fixture,
            env=self.env,
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=True,
        )
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if self.daemon.poll() is not None:
                raise CanaryFailure("hollerd exited during startup")
            try:
                run_command(
                    [str(self.holler), "status", "--socket", str(self.socket)],
                    cwd=self.fixture,
                    env=self.env,
                    timeout=2,
                )
                return
            except CanaryFailure:
                time.sleep(0.2)
        raise CanaryFailure("hollerd did not become ready")

    def stop_daemon(self) -> None:
        if self.daemon is None or self.daemon.poll() is not None:
            return
        os.killpg(self.daemon.pid, signal.SIGTERM)
        try:
            self.daemon.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(self.daemon.pid, signal.SIGKILL)
            self.daemon.wait(timeout=5)

    def setup_connectors(self) -> None:
        marketplace = self.package / "share" / "holler" / "marketplace"
        common = ["--project", "canary", "--channel", "direct", "--socket", str(self.socket), "--name-mode", "exact"]
        run_command(
            [
                str(self.holler), "connector", "setup", "--harness", "claude", "--apply",
                "--attention", "hook-long-poll", "--actor", "canary-claude", "--peer", "canary-codex",
                "--marketplace", str(marketplace), "--client-binary", str(self.request["clients"]["claude"]["binary"]),
                *common,
            ],
            cwd=self.fixture,
            env=self.env,
            timeout=120,
        )
        run_command(
            [
                str(self.holler), "connector", "setup", "--harness", "codex", "--apply",
                "--attention", "native-queue", "--actor", "canary-codex", "--peer", "canary-claude",
                "--marketplace", str(marketplace), "--client-binary", str(self.request["clients"]["codex"]["binary"]),
                "--project-root", str(self.fixture), *common,
            ],
            cwd=self.fixture,
            env=self.env,
            timeout=120,
        )
        self.install_codex_fixture_trust()
        self.ensure_codex_hook_trust()

    def install_codex_fixture_trust(self) -> None:
        path = Path(self.env["CODEX_HOME"]) / "config.toml"
        try:
            current = path.read_text(encoding="utf-8") if path.exists() else ""
            updated = codex_config_with_trusted_fixture(current, self.fixture)
            descriptor, temporary = tempfile.mkstemp(prefix=".config.toml.", dir=path.parent)
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                stream.write(updated)
                stream.flush()
                os.fsync(stream.fileno())
            os.chmod(temporary, 0o600)
            os.replace(temporary, path)
        except OSError as error:
            raise CanaryFailure("could not install Codex cleanroom fixture trust") from error

    def ensure_codex_hook_trust(self) -> None:
        """Approve the packaged hooks through Codex's real first-launch review."""
        args = codex_live_command(self.request["clients"]["codex"])[1:]
        process = PtyProcess(
            self.launcher("codex", "canary-codex", "c0-codex-trust", args),
            cwd=self.fixture,
            env=self.env,
        )
        try:
            process.wait_until_quiet(60, quiet_seconds=2)
            tail = process.normalized_output()[-5000:]
            if "Hooks need review" in tail:
                if "2 hooks are new or changed" not in tail:
                    raise CanaryFailure("Codex requested trust for an unexpected number of hooks")
                after = process.checkpoint()
                process.send("\x1b[B")
                process.wait_for("› 2. Trust all and continue", 5, after=after)
                after = process.checkpoint()
                process.send("\r")
                process.wait_for("Ask Codex to do anything", 30, after=after)
                process.wait_until_quiet(30, quiet_seconds=2)
            elif "Ask Codex to do anything" not in tail:
                raise CanaryFailure("Codex hook-trust review did not reach an input-ready state")
        finally:
            process.close()
        path = Path(self.env["CODEX_HOME"]) / "config.toml"
        try:
            config = tomllib.loads(path.read_text(encoding="utf-8"))
        except (OSError, tomllib.TOMLDecodeError) as error:
            raise CanaryFailure("Codex hook-trust state is unavailable") from error
        if not codex_hook_trust_ready(config):
            raise CanaryFailure("Codex did not persist trust for both packaged Holler hooks")

    def launcher(self, harness: str, actor: str, run_id: str, client_args: list[str]) -> list[str]:
        return [
            str(self.holler), "connector", "launch", "--harness", harness,
            "--actor", actor, "--run", run_id, "--", *client_args,
        ]

    def certify(self, harness: str, actor: str, run_id: str, attention: str) -> None:
        result = run_command(
            [
                str(self.holler), "connector", "certify", "--harness", harness,
                "--profile", "live-review", "--project", "canary", "--actor", actor,
                "--run", run_id, "--socket", str(self.socket), "--attention", attention,
                "--after-durable", "0", "--after-operational", "0",
            ],
            cwd=self.fixture,
            env=self.env,
            timeout=60,
        )
        try:
            report = json.loads(result.stdout)
        except json.JSONDecodeError as error:
            raise CanaryFailure(f"cannot decode {harness} certification") from error
        if report.get("ready") is not True or report.get("state") != "READY":
            raise CanaryFailure(f"{harness} did not certify READY")

    def wait_for_live_registration(self, actor: str, run_id: str, timeout: float = 60) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            result = run_command(
                [str(self.holler), "who", "--socket", str(self.socket), "--all", "--limit", "100"],
                cwd=self.fixture,
                env=self.env,
                timeout=15,
            )
            try:
                directory = json.loads(result.stdout)
            except json.JSONDecodeError as error:
                raise CanaryFailure("cannot decode actor directory") from error
            for entry in directory.get("actors", []):
                if entry.get("actor") != actor:
                    continue
                if any(
                    session.get("run_id") == run_id and session.get("state") == "live"
                    for session in entry.get("sessions", [])
                ):
                    return
            time.sleep(0.25)
        raise CanaryFailure(f"{actor} did not create a live registration")

    def has_lifecycle_evidence(self, actor: str, run_id: str) -> bool:
        events = self.operational_events()
        return lifecycle_evidence_complete(events, actor=actor, run_id=run_id)

    def json_command(self, command: list[str], *, timeout: int = 30) -> Any:
        result = run_command(command, cwd=self.fixture, env=self.env, timeout=timeout)
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError as error:
            raise CanaryFailure(f"cannot decode {Path(command[0]).name} JSON output") from error

    def operational_events(self, partition: str = "canary") -> list[dict[str, Any]]:
        return self.events(partition=partition, stream="operational")

    def durable_events(self, partition: str = "canary") -> list[dict[str, Any]]:
        return self.events(partition=partition, stream="durable")

    def events(self, *, partition: str, stream: str) -> list[dict[str, Any]]:
        events = self.json_command(
            [
                str(self.holler), "events", "--socket", str(self.socket), "--partition", partition,
                "--stream", stream, "--after", "0", "--limit", "1000",
            ]
        )
        if not isinstance(events, list):
            raise CanaryFailure("operational lifecycle events are not a list")
        return events

    def actor_directory(self) -> dict[str, Any]:
        directory = self.json_command(
            [
                str(self.holler), "who", "--socket", str(self.socket), "--all", "--limit", "100",
            ]
        )
        if not isinstance(directory, dict):
            raise CanaryFailure("actor directory is not an object")
        return directory

    def wait_for_allocated_actor(self, run_id: str, *, harness: str = "claude") -> str:
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            actor = actor_for_run(self.actor_directory(), run_id=run_id, harness=harness)
            if actor is not None:
                return actor
            time.sleep(0.25)
        raise CanaryFailure(f"allocated {harness} run did not create a live registration")

    def allocated_launcher(
        self,
        harness: str,
        base_actor: str,
        run_id: str,
        launch_tag: str,
        project: str,
        client_args: list[str],
    ) -> list[str]:
        return [
            str(self.holler), "connector", "launch", "--harness", harness,
            "--actor", base_actor, "--run", run_id, "--project", project,
            "--name-mode", "allocate", "--launch-tag", launch_tag,
            "--socket", str(self.socket), "--", *client_args,
        ]

    def run_claude_lifecycle_preflight(self) -> None:
        run_id = "c0-claude-init"
        command = claude_live_command(self.request["clients"]["claude"])[1:] + ["--init-only"]
        run_command(
            self.launcher("claude", "canary-claude", run_id, command),
            cwd=self.fixture,
            env=self.env,
            timeout=60,
        )
        if not self.has_lifecycle_evidence("canary-claude", run_id):
            raise CanaryFailure("Claude init-only did not produce registration and hydration evidence")

    def validate_claude_fixture(self) -> None:
        path = Path(self.env["CLAUDE_CONFIG_DIR"]) / ".claude.json"
        try:
            config = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise CanaryFailure("Claude cleanroom fixture state is unavailable") from error
        if not claude_fixture_ready(
            config,
            fixture=self.fixture,
            version=self.request["clients"]["claude"]["version"],
        ):
            raise CanaryFailure("Claude cleanroom fixture state is incomplete")

    def run_claude(self, actor: str, run_id: str, prompt: str, max_usd: float = 0.10) -> str:
        self.ledger.ensure_capacity(client="claude", turns=1)
        command = claude_print_command(self.request["clients"]["claude"], max_usd)[1:]
        started = time.monotonic()
        result = run_command(
            self.launcher("claude", actor, run_id, command),
            cwd=self.fixture,
            env=self.env,
            timeout=180,
            stdin=prompt,
        )
        self.ledger.charge(
            client="claude", turns=1, cost_usd=claude_cost(result.stdout),
            wall_seconds=time.monotonic() - started,
        )
        return result.stdout

    def run_codex(self, actor: str, run_id: str, prompt: str) -> str:
        self.ledger.ensure_capacity(client="codex", turns=1)
        command = codex_exec_command(self.request["clients"]["codex"])[1:]
        started = time.monotonic()
        result = run_command(
            self.launcher("codex", actor, run_id, command),
            cwd=self.fixture,
            env=self.env,
            timeout=180,
            stdin=prompt,
        )
        self.ledger.charge(
            client="codex", turns=1, reported_tokens=codex_reported_tokens(result.stdout),
            wall_seconds=time.monotonic() - started,
        )
        return result.stdout

    def run_allocated_claude(
        self,
        base_actor: str,
        run_id: str,
        launch_tag: str,
        project: str,
        prompt: str,
        max_usd: float = 0.10,
    ) -> str:
        self.ledger.ensure_capacity(client="claude", turns=1)
        command = claude_print_command(self.request["clients"]["claude"], max_usd)[1:]
        started = time.monotonic()
        result = run_command(
            self.allocated_launcher(
                "claude", base_actor, run_id, launch_tag, project, command
            ),
            cwd=self.fixture,
            env=self.env,
            timeout=180,
            stdin=prompt,
        )
        self.ledger.charge(
            client="claude", turns=1, cost_usd=claude_cost(result.stdout),
            wall_seconds=time.monotonic() - started,
        )
        return result.stdout

    def scenario_c0(self) -> list[str]:
        artifact = self.request["artifact"]
        if artifact.get("sha256") and sha256_file(self.archive) != artifact["sha256"]:
            raise CanaryFailure("release archive checksum does not match the approved request")
        identity = json.loads(
            run_command([str(self.holler), "version"], cwd=self.fixture, env=self.env, timeout=30).stdout
        )
        source = self.request["source"]
        if identity.get("commit") != source["commit"] or identity.get("dirty") is not False:
            raise CanaryFailure(f"packaged identity does not match approved source: {identity}")
        versions = {}
        for name in ("claude", "codex"):
            config = self.request["clients"][name]
            output = run_command(
                [str(config["binary"]), "--version"], cwd=self.fixture, env=self.env, timeout=30
            ).stdout
            versions[name] = parse_version(output)
            if versions[name] != config["version"]:
                raise CanaryFailure(f"{name} version {versions[name]} does not match pin {config['version']}")
        self.validate_claude_fixture()
        self.setup_connectors()
        for name, actor, attention in (
            ("claude", "canary-claude", "hook-long-poll"),
            ("codex", "canary-codex", "native-queue"),
        ):
            run_command(
                doctor_command(
                    self.holler,
                    harness=name,
                    actor=actor,
                    attention=attention,
                    project=self.fixture,
                    socket=self.socket,
                    env=self.env,
                ),
                cwd=self.fixture,
                env=self.env,
                timeout=60,
            )
        self.run_claude_lifecycle_preflight()
        return [
            "archive-checksum", "clean-build-identity", "client-version-pins", "connector-doctor",
            "interactive-onboarding", "codex-hook-trust", "claude-lifecycle-hook",
        ]

    def scenario_c1(self) -> list[str]:
        token = "C1-" + self.request["request_hash"][-12:]
        sent = self.run_codex(
            "canary-codex", "c1-codex",
            f"Use Holler bus_send to send exactly one durable message to actor canary-claude. "
            f"The body must contain token {token}. Use idempotency key {token}. Finish with marker C1_SENT.",
        )
        if "C1_SENT" not in sent:
            raise CanaryFailure("Codex did not report the C1 send marker")
        received = self.run_claude(
            "canary-claude", "c1-claude",
            f"Use bus_inbox to claim the offline Holler message containing {token}, then bus_ack its lease. "
            "Do not echo its full body. Finish with marker C1_ACKED.",
        )
        if "C1_ACKED" not in received:
            raise CanaryFailure("Claude did not report the C1 acknowledgement marker")
        return ["durable-send", "offline-hydration", "mcp-claim", "mcp-ack"]

    def scenario_c2(self) -> list[str]:
        token = "C2-" + self.request["request_hash"][-12:]
        started = time.monotonic()
        claude_armed = "C2_CLAUDE_ARMED"
        codex_sent = "C2_CODEX_SENT"
        claude_replied = "C2_CLAUDE_REPLIED"
        codex_acked = "C2_CODEX_ACKED"
        claude_args = claude_live_command(self.request["clients"]["claude"])[1:]
        codex_args = codex_live_command(self.request["clients"]["codex"])[1:]
        claude = PtyProcess(
            self.launcher("claude", "canary-claude", "c2-claude", claude_args), cwd=self.fixture, env=self.env
        )
        codex: PtyProcess | None = None
        try:
            self.wait_for_live_registration("canary-claude", "c2-claude")
            claude.wait_until_ready("$", 60, suffix=True)
            self.ledger.ensure_capacity(client="claude", turns=1)
            claude.submit(
                "Initialize Holler and wait for Holler attention. If Holler wakes you later, claim and "
                f"acknowledge the message, reply_to its sender with the requested correlation token, and "
                f"finish that awakened turn with {marker_instruction(claude_replied)}. For this initial "
                f"turn, finish with {marker_instruction(claude_armed)}.",
                marker=claude_armed,
                timeout=180,
            )
            self.ledger.charge(client="claude", turns=1)
            claude_wake_after = claude.checkpoint()
            codex = PtyProcess(
                self.launcher("codex", "canary-codex", "c2-codex", codex_args), cwd=self.fixture, env=self.env
            )
            codex.wait_until_ready("Ask Codex to do anything", 60)
            codex.wait_until_quiet(30, quiet_seconds=2)
            if "Hooks need review" in codex.normalized_output()[-5000:]:
                raise CanaryFailure("Codex hook trust was not ready before C2")
            self.ledger.ensure_capacity(client="codex", turns=1)
            codex.submit(
                f"Use Holler bus_send to actor canary-claude with idempotency key {token}. "
                f"The exact message body must be: acknowledge this message and reply_to its sender with "
                f"correlation token {token}-REPLY. Do not include any completion marker in the message. "
                f"If Holler wakes you later, claim and acknowledge the reply, then finish that awakened "
                f"turn with {marker_instruction(codex_acked)}. For this initial turn, finish with "
                f"{marker_instruction(codex_sent)}.",
                marker=codex_sent,
                timeout=180,
            )
            self.ledger.charge(client="codex", turns=1)
            self.wait_for_live_registration("canary-codex", "c2-codex")
            codex_wake_after = codex.checkpoint()
            self.ledger.ensure_capacity(client="claude", turns=1)
            claude.wait_for(claude_replied, 180, after=claude_wake_after)
            self.ledger.charge(client="claude", turns=1)
            self.ledger.ensure_capacity(client="codex", turns=1)
            codex.wait_for(codex_acked, 180, after=codex_wake_after)
            self.ledger.charge(client="codex", turns=1)
        finally:
            if codex is not None:
                codex.close()
            claude.close()
            self.ledger.charge(client="controller", turns=0, wall_seconds=time.monotonic() - started)
        self.certify("claude", "canary-claude", "c2-claude", "hook-long-poll")
        self.certify("codex", "canary-codex", "c2-codex", "native-queue")
        return [
            "claude-live-wake", "codex-live-wake", "bidirectional-routing",
            "exactly-once-processing", "connector-ready",
        ]

    def scenario_c3(self) -> list[str]:
        token = "C3-" + self.request["request_hash"][-12:]
        sent = self.run_codex(
            "canary-codex", "c3-codex",
            f"Use Holler bus_send to actor canary-claude with body token {token} and idempotency key {token}. "
            "Finish with marker C3_SENT.",
        )
        if "C3_SENT" not in sent:
            raise CanaryFailure("Codex did not report the C3 send marker")
        self.stop_daemon()
        self.start_daemon()
        received = self.run_claude(
            "canary-claude", "c3-claude",
            f"Use bus_inbox to claim the Holler message containing {token}, then bus_ack its lease. "
            "Finish with marker C3_ACKED.",
        )
        if "C3_ACKED" not in received:
            raise CanaryFailure("Claude did not report the C3 acknowledgement marker")
        return ["daemon-restart", "client-reconnect", "no-message-loss", "no-duplicate-processing"]

    def scenario_c4(self) -> list[str]:
        self.active_check = "c4-durable-send"
        token = "C4-" + self.request["request_hash"][-12:]
        sent = self.json_command(
            [
                str(self.holler), "send", "--socket", str(self.socket),
                "--actor", "c4-controller", "--run", "c4-controller",
                "--project", "canary", "--channel", "direct",
                "--to-actor", "canary-claude", "--idempotency-key", token,
                "--body", json.dumps({"text": f"claim-crash-redelivery token {token}"}),
            ]
        )
        message = sent.get("message") if isinstance(sent, dict) else None
        message_id = message.get("message_id") if isinstance(message, dict) else None
        if not isinstance(message_id, str) or not message_id:
            raise CanaryFailure("C4 durable send returned no message ID")

        command = claude_live_command(self.request["clients"]["claude"])[1:]
        claude = PtyProcess(
            self.launcher("claude", "canary-claude", "c4-claim-holder", command),
            cwd=self.fixture,
            env=self.env,
        )
        try:
            self.active_check = "c4-live-registration"
            self.wait_for_live_registration("canary-claude", "c4-claim-holder")
            self.active_check = "c4-live-readiness"
            claude.wait_until_ready("$", 60, suffix=True)
            self.active_check = "c4-first-claim"
            self.ledger.ensure_capacity(client="claude", turns=1)
            claude.send(
                f"Use bus_claim to claim message ID {message_id} with lease_seconds 15. "
                "Do not acknowledge, nack, or extend the lease. Return after the claim succeeds."
            )
            time.sleep(0.1)
            claude.send("\r")
            deadline = time.monotonic() + 180
            attempts: list[int] = []
            while time.monotonic() < deadline:
                attempts = delivery_event_attempts(
                    self.operational_events(), message_id=message_id, actor="canary-claude"
                )
                if attempts == [1]:
                    break
                if claude.process.poll() is not None:
                    raise CanaryFailure("C4 claim holder exited before claiming the message")
                time.sleep(0.25)
            self.ledger.charge(client="claude", turns=1)
            self.active_check = "c4-claimed-state"
            if attempts != [1] or actor_delivery_counts(
                self.actor_directory(), actor="canary-claude"
            ) != (0, 1):
                raise CanaryFailure("C4 first claim was not active and unavailable")
        finally:
            claude.close(abrupt=True)

        deadline = time.monotonic() + 30
        expired_counts = (0, 0)
        self.active_check = "c4-lease-expiry"
        while time.monotonic() < deadline:
            expired_counts = actor_delivery_counts(
                self.actor_directory(), actor="canary-claude"
            )
            if expired_counts == (1, 0):
                break
            time.sleep(0.25)
        if expired_counts != (1, 0):
            raise CanaryFailure("C4 lease did not expire into redelivery for the same message")

        self.active_check = "c4-redelivery-claim-ack"
        acknowledged = self.run_claude(
            "canary-claude",
            "c4-claim-holder",
            f"Use bus_claim to reclaim exact message ID {message_id}. Verify the returned attempt is 2, "
            "then bus_ack that lease. Do not echo the body. Finish with marker C4_ACKED.",
        )
        if "C4_ACKED" not in acknowledged:
            raise CanaryFailure("Claude did not report the C4 terminal acknowledgement marker")

        self.active_check = "c4-terminal-state"
        if actor_delivery_counts(self.actor_directory(), actor="canary-claude") != (0, 0):
            raise CanaryFailure("C4 message remained in the inbox after acknowledgement")
        self.active_check = "c4-event-correlation"
        events = self.operational_events()
        if delivery_event_attempts(events, message_id=message_id, actor="canary-claude") != [1, 2]:
            raise CanaryFailure("C4 did not record exactly two ordered claims for the same message")
        if not delivery_was_acked(events, message_id=message_id, actor="canary-claude"):
            raise CanaryFailure("C4 did not record the terminal acknowledgement")
        return ["claim-before-crash", "lease-expiry", "redelivery-same-message-id", "terminal-ack"]

    def scenario_c5(self) -> list[str]:
        project = "c5"
        alias = "c5-claude"
        token_a = "C5-A-" + self.request["request_hash"][-10:]
        token_b = "C5-B-" + self.request["request_hash"][-10:]
        command = claude_live_command(self.request["clients"]["claude"])[1:]
        session_a = PtyProcess(
            self.allocated_launcher("claude", "c5-claude", "c5-a", "slot-a", project, command),
            cwd=self.fixture,
            env=self.env,
        )
        session_b: PtyProcess | None = None
        try:
            self.active_check = "c5-first-allocation"
            candidate_a = self.wait_for_allocated_actor("c5-a")
            session_a.wait_until_ready("$", 60, suffix=True)

            self.active_check = "c5-second-allocation"
            session_b = PtyProcess(
                self.allocated_launcher("claude", "c5-claude", "c5-b", "slot-b", project, command),
                cwd=self.fixture,
                env=self.env,
            )
            candidate_b = self.wait_for_allocated_actor("c5-b")
            session_b.wait_until_ready("$", 60, suffix=True)
            if (
                candidate_a == candidate_b
                or not candidate_a.startswith("c5-claude-")
                or not candidate_b.startswith("c5-claude-")
            ):
                raise CanaryFailure("C5 did not allocate two distinct opaque actors")

            self.active_check = "c5-close-concurrent-sessions"
            session_a.close()
            session_b.close()

            actor_a = candidate_a
            actor_b = candidate_b

            self.active_check = "c5-alias-collision"
            alias_ready = False
            resolved: Any = None
            conditions: Any = []
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                try:
                    resolved = self.json_command(
                        [str(self.holler), "alias", "resolve", "--socket", str(self.socket), alias]
                    )
                except CanaryFailure:
                    resolved = None
                conditions = self.json_command(
                    [
                        str(self.holler), "conditions", "list", "--socket", str(self.socket),
                        "--limit", "100",
                    ]
                )
                if (
                    isinstance(resolved, dict)
                    and resolved.get("actor") in {actor_a, actor_b}
                    and alias_collision_visible(conditions, alias=alias)
                ):
                    alias_ready = True
                    break
                time.sleep(0.25)
            if not alias_ready:
                if not isinstance(resolved, dict):
                    self.active_check = "c5-alias-missing"
                elif resolved.get("actor") not in {actor_a, actor_b}:
                    self.active_check = "c5-alias-owner-mismatch"
                elif not alias_collision_visible(conditions, alias=alias):
                    self.active_check = "c5-alias-condition-missing"
                raise CanaryFailure("C5 alias ownership and collision condition did not converge")

            self.active_check = "c5-send-isolated"
            message_ids: dict[str, str] = {}
            for label, actor, token in (("a", actor_a, token_a), ("b", actor_b, token_b)):
                sent = self.json_command(
                    [
                        str(self.holler), "send", "--socket", str(self.socket),
                        "--actor", "c5-controller", "--run", "c5-controller",
                        "--project", project, "--channel", "direct", "--to-actor", actor,
                        "--idempotency-key", token,
                        "--body", json.dumps({"text": f"identity isolation probe {token}"}),
                    ]
                )
                message = sent.get("message") if isinstance(sent, dict) else None
                message_id = message.get("message_id") if isinstance(message, dict) else None
                if not isinstance(message_id, str) or not message_id:
                    raise CanaryFailure("C5 durable send returned no message ID")
                message_ids[label] = message_id

            self.active_check = "c5-first-resume"
            self.run_allocated_claude(
                "c5-claude",
                "c5-a-resume",
                "slot-a",
                project,
                f"Use bus_claim to claim exact message ID {message_ids['a']}, then bus_ack its lease. "
                "Do not echo the body. Finish with marker C5_A_ACKED.",
            )
            resumed_a = actor_for_run(
                self.actor_directory(), run_id="c5-a-resume", harness="claude", live_only=False
            )
            if resumed_a != actor_a:
                self.active_check = "c5-first-resume-identity-mismatch"
                raise CanaryFailure("C5 first launch tag did not resume its allocated identity")
            self.active_check = "c5-second-resume"
            self.run_allocated_claude(
                "c5-claude",
                "c5-b-resume",
                "slot-b",
                project,
                f"Use bus_claim to claim exact message ID {message_ids['b']}, then bus_ack its lease. "
                "Do not echo the body. Finish with marker C5_B_ACKED.",
            )
            resumed_b = actor_for_run(
                self.actor_directory(), run_id="c5-b-resume", harness="claude", live_only=False
            )
            if resumed_b != actor_b:
                self.active_check = "c5-second-resume-identity-mismatch"
                raise CanaryFailure("C5 second launch tag did not resume its allocated identity")

            self.active_check = "c5-inbox-isolation"
            events = self.operational_events(project)
            for label, expected_actor, other_actor in (
                ("a", actor_a, actor_b),
                ("b", actor_b, actor_a),
            ):
                message_id = message_ids[label]
                if delivery_event_attempts(events, message_id=message_id, actor=expected_actor) != [1]:
                    self.active_check = f"c5-{label}-claim-missing"
                    raise CanaryFailure("C5 expected actor did not claim its isolated message")
                if delivery_event_attempts(events, message_id=message_id, actor=other_actor):
                    self.active_check = f"c5-{label}-cross-inbox-claim"
                    raise CanaryFailure("C5 message crossed allocated inboxes")
                if not delivery_was_acked(events, message_id=message_id, actor=expected_actor):
                    self.active_check = f"c5-{label}-ack-missing"
                    raise CanaryFailure("C5 expected actor did not acknowledge its isolated message")
            self.active_check = "c5-resume-continuity"
            mints = minted_actors(self.durable_events(project))
            if len(mints) < 2:
                self.active_check = "c5-mint-events-missing"
                raise CanaryFailure("C5 initial allocations did not record both actor mints")
            if len(mints) > 2:
                self.active_check = "c5-resume-reminted-actors"
                raise CanaryFailure("C5 resumes minted unexpected actor identities")
            if set(mints) != {actor_a, actor_b}:
                self.active_check = "c5-mint-identity-mismatch"
                raise CanaryFailure("C5 mint events did not match the live allocated actors")
            if actor_delivery_counts(self.actor_directory(), actor=actor_a) != (0, 0) or actor_delivery_counts(
                self.actor_directory(), actor=actor_b
            ) != (0, 0):
                raise CanaryFailure("C5 allocated inboxes were not empty after isolated acknowledgements")
        finally:
            if session_b is not None and session_b.process.poll() is None:
                session_b.close()
            if session_a.process.poll() is None:
                session_a.close()
        return ["allocated-identities", "alias-collision-visible", "resume-continuity", "inbox-isolation"]

    def run(self) -> dict[str, Any]:
        requested = {scenario["id"] for scenario in self.request["scenarios"]}
        unsupported = requested - SUPPORTED_REAL_SCENARIOS
        if unsupported:
            raise CanaryFailure(
                f"real worker currently supports the core tier only; unsupported scenarios: {sorted(unsupported)}"
            )
        self.prepare()
        handlers = {
            "C0": self.scenario_c0,
            "C1": self.scenario_c1,
            "C2": self.scenario_c2,
            "C3": self.scenario_c3,
            "C4": self.scenario_c4,
            "C5": self.scenario_c5,
        }
        for scenario in self.request["scenarios"]:
            self.active_scenario = scenario["id"]
            self.active_check = "scenario-start"
            started = time.monotonic()
            checks = handlers[scenario["id"]]()
            self.results.append(
                {
                    "id": scenario["id"],
                    "name": scenario["name"],
                    "status": "PASS",
                    "duration_seconds": round(time.monotonic() - started, 3),
                    "assertions": [{"name": check, "status": "PASS"} for check in checks],
                }
            )
        evidence: dict[str, Any] = {
            "schema_version": 1,
            "kind": "holler-canary-evidence",
            "driver": "real",
            "request_hash": self.request["request_hash"],
            "source": self.request["source"],
            "tier": self.request["tier"],
            "status": "PASS",
            "results": self.results,
            "usage": self.ledger.as_dict(),
            "limits": self.request["budget"],
            "message_bodies_included": False,
        }
        evidence["evidence_hash"] = sha256_bytes(canonical_json(evidence))
        return evidence


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("request", type=Path)
    parser.add_argument("archive", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    request = load_request(args.request)
    with tempfile.TemporaryDirectory(prefix="holler-canary-") as directory:
        package_dir = Path(directory) / "package"
        package_dir.mkdir()
        worker = Worker(request, args.archive.resolve(), Path(directory))
        try:
            evidence = worker.run()
        except (BudgetExceeded, CanaryFailure, ManifestError) as error:
            evidence = make_failure_evidence(
                request,
                results=worker.results,
                usage=worker.ledger.as_dict(),
                scenario=worker.active_scenario,
                check=worker.active_check,
                error=error,
            )
        finally:
            worker.stop_daemon()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"status": evidence["status"], "output": str(args.output)}, sort_keys=True))
    if evidence["status"] != "PASS":
        raise SystemExit("canary failed; inspect the body-free evidence")


if __name__ == "__main__":
    try:
        main()
    except (BudgetExceeded, CanaryFailure, ManifestError) as error:
        raise SystemExit(str(error)) from error
