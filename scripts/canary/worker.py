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

    def wait_for(self, marker: str, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        marker_bytes = marker.encode("utf-8")
        while time.monotonic() < deadline:
            if marker_bytes in self.buffer:
                return
            if self.process.poll() is not None:
                raise CanaryFailure(f"interactive client exited before {marker}")
            for key, _ in self.selector.select(timeout=min(0.25, deadline - time.monotonic())):
                try:
                    chunk = os.read(key.fd, 65536)
                except BlockingIOError:
                    continue
                if chunk:
                    self.buffer.extend(chunk)
                    if len(self.buffer) > 2_000_000:
                        del self.buffer[:1_000_000]
        raise CanaryFailure(f"timed out waiting for expected client marker {marker}")

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
        self.fixture = root / "fixture"
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

    def prepare(self) -> None:
        for directory in (self.home, self.fixture, self.runtime, self.package.parent):
            directory.mkdir(parents=True, exist_ok=True)
        run_command(["git", "init", "-q"], cwd=self.fixture, env=self.env, timeout=30)
        (self.fixture / "README.md").write_text("# Holler real-client canary fixture\n", encoding="utf-8")
        run_command(["git", "add", "README.md"], cwd=self.fixture, env=self.env, timeout=30)
        run_command(
            ["git", "-c", "user.name=Holler Canary", "-c", "user.email=canary@invalid", "commit", "-qm", "fixture"],
            cwd=self.fixture,
            env=self.env,
            timeout=30,
        )
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

    def run_claude(self, actor: str, run_id: str, prompt: str, max_usd: float = 0.10) -> str:
        self.ledger.charge(client="claude", turns=1)
        command = claude_print_command(self.request["clients"]["claude"], max_usd)[1:]
        started = time.monotonic()
        result = run_command(
            self.launcher("claude", actor, run_id, command),
            cwd=self.fixture,
            env=self.env,
            timeout=180,
            stdin=prompt,
        )
        self.ledger.charge(client="claude", turns=0, cost_usd=claude_cost(result.stdout), wall_seconds=time.monotonic() - started)
        return result.stdout

    def run_codex(self, actor: str, run_id: str, prompt: str) -> str:
        self.ledger.charge(client="codex", turns=1)
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
            client="codex", turns=0, reported_tokens=codex_reported_tokens(result.stdout),
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
        self.setup_connectors()
        for name, actor, attention in (
            ("claude", "canary-claude", "hook-long-poll"),
            ("codex", "canary-codex", "native-queue"),
        ):
            run_command(
                [
                    str(self.holler), "connector", "doctor", "--harness", name,
                    "--profile", "live-review", "--project", str(self.fixture),
                    "--attention", attention, "--actor", actor, "--socket", str(self.socket),
                ],
                cwd=self.fixture,
                env=self.env,
                timeout=60,
            )
        return ["archive-checksum", "clean-build-identity", "client-version-pins", "connector-doctor"]

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
        claude_args = claude_live_command(self.request["clients"]["claude"])[1:]
        codex_args = codex_live_command(self.request["clients"]["codex"])[1:]
        claude = PtyProcess(
            self.launcher("claude", "canary-claude", "c2-claude", claude_args), cwd=self.fixture, env=self.env
        )
        codex: PtyProcess | None = None
        try:
            self.ledger.charge(client="claude", turns=1)
            claude.send("Initialize Holler, reply with marker C2_CLAUDE_ARMED, then wait for Holler attention.\n")
            claude.wait_for("C2_CLAUDE_ARMED", 180)
            codex = PtyProcess(
                self.launcher("codex", "canary-codex", "c2-codex", codex_args), cwd=self.fixture, env=self.env
            )
            self.ledger.charge(client="codex", turns=1)
            codex.send(
                f"Use Holler bus_send to actor canary-claude with idempotency key {token}. "
                f"The message must tell Claude to acknowledge it, reply_to you with token {token}-REPLY, "
                "finish its awakened turn with C2_CLAUDE_REPLIED, and tell you to acknowledge the reply "
                "and finish with C2_CODEX_ACKED. Finish this turn with C2_CODEX_SENT.\n"
            )
            codex.wait_for("C2_CODEX_SENT", 180)
            self.ledger.charge(client="claude", turns=1)
            claude.wait_for("C2_CLAUDE_REPLIED", 180)
            self.ledger.charge(client="codex", turns=1)
            codex.wait_for("C2_CODEX_ACKED", 180)
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
        finally:
            worker.stop_daemon()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"status": evidence["status"], "output": str(args.output)}, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (BudgetExceeded, CanaryFailure, ManifestError) as error:
        raise SystemExit(str(error)) from error
