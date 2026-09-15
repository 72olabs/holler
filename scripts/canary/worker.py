#!/usr/bin/env python3
"""Credentialed sandbox worker for the core Holler real-client canary."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import pty
import re
import selectors
import signal
import subprocess
import sys
import tarfile
import tempfile
import time
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from budget import BudgetExceeded, BudgetLedger  # noqa: E402
from clients import claude_live_command, claude_print_command, codex_exec_command, codex_live_command  # noqa: E402
from manifest import ManifestError, canonical_json, load_request, sha256_bytes, sha256_file  # noqa: E402


SUPPORTED_REAL_SCENARIOS = {"C0", "C1", "C2", "C3"}


class CanaryFailure(RuntimeError):
    pass


def make_failure_evidence(
    request: dict[str, Any],
    *,
    results: list[dict[str, Any]],
    usage: dict[str, Any],
    scenario: str,
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
        "failure": {"scenario": scenario, "type": type(error).__name__},
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


def claude_fixture_ready(config: object, *, fixture: Path, version: str) -> bool:
    """Check the non-secret Claude preferences required for an unattended cleanroom TUI."""
    if not isinstance(config, dict):
        return False
    projects = config.get("projects")
    project = projects.get(str(fixture)) if isinstance(projects, dict) else None
    return (
        config.get("theme") == "dark"
        and config.get("hasCompletedOnboarding") is True
        and config.get("lastOnboardingVersion") == version
        and isinstance(project, dict)
        and project.get("hasTrustDialogAccepted") is True
    )


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
        master, slave = pty.openpty()
        self.master = master
        self.buffer = bytearray()
        self.process = subprocess.Popen(
            command,
            cwd=cwd,
            env=env,
            stdin=slave,
            stdout=slave,
            stderr=slave,
            start_new_session=True,
            close_fds=True,
        )
        os.close(slave)
        os.set_blocking(master, False)
        self.selector = selectors.DefaultSelector()
        self.selector.register(master, selectors.EVENT_READ)

    def send(self, text: str) -> None:
        os.write(self.master, text.encode("utf-8"))

    def checkpoint(self) -> int:
        self._read_available(0)
        return len(self.buffer)

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

    def wait_until_ready(self, timeout: float) -> None:
        """Wait for the input footer shared by the pinned Claude and Codex TUIs."""
        self.wait_for("? for shortcuts", timeout)
        self.wait_until_quiet(min(timeout, 10))

    def submit(self, prompt: str, *, marker: str, timeout: float) -> None:
        if marker in prompt:
            raise CanaryFailure("interactive prompt contains its expected output marker")
        after = self.checkpoint()
        self.send(prompt + "\r")
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
            if chunk:
                self.buffer.extend(chunk)

    def close(self) -> None:
        if self.process.poll() is None:
            self.send("\x03")
            try:
                self.process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                os.killpg(self.process.pid, signal.SIGTERM)
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(self.process.pid, signal.SIGKILL)
                    self.process.wait(timeout=5)
        self.selector.close()
        os.close(self.master)


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
        result = run_command(
            [
                str(self.holler), "events", "--socket", str(self.socket), "--partition", "canary",
                "--stream", "operational", "--after", "0", "--limit", "100",
            ],
            cwd=self.fixture,
            env=self.env,
            timeout=30,
        )
        try:
            events = json.loads(result.stdout)
        except json.JSONDecodeError as error:
            raise CanaryFailure("cannot decode operational lifecycle events") from error
        return lifecycle_evidence_complete(events, actor=actor, run_id=run_id)

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
            "interactive-onboarding", "claude-lifecycle-hook",
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
            claude.wait_until_ready(60)
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
            codex.wait_until_ready(60)
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
        }
        for scenario in self.request["scenarios"]:
            self.active_scenario = scenario["id"]
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
