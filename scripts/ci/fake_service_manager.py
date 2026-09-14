#!/usr/bin/env python3
"""Minimal launchctl/systemctl stand-in that supervises the packaged hollerd."""

from __future__ import annotations

import os
from pathlib import Path
import signal
import subprocess
import sys
import time


def required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        print(f"{name} is required", file=sys.stderr)
        raise SystemExit(2)
    return value


PID_PATH = Path(required("HOLLER_TEST_SERVICE_PID"))


def current_pid() -> int:
    try:
        pid = int(PID_PATH.read_text(encoding="utf-8").strip())
        os.kill(pid, 0)
        return pid
    except (OSError, ValueError):
        return 0


def stop() -> None:
    pid = current_pid()
    if pid:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        for _ in range(100):
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                break
            time.sleep(0.02)
        else:
            try:
                os.kill(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
    PID_PATH.unlink(missing_ok=True)


def start() -> None:
    stop()
    daemon = required("HOLLER_TEST_DAEMON_BIN")
    socket = required("HOLLER_TEST_SOCKET")
    database = required("HOLLER_TEST_DB")
    log_root = Path(required("HOLLER_TEST_SERVICE_LOG_DIR"))
    log_root.mkdir(parents=True, exist_ok=True)
    PID_PATH.parent.mkdir(parents=True, exist_ok=True)
    stdout = (log_root / "hollerd.stdout.log").open("ab")
    stderr = (log_root / "hollerd.stderr.log").open("ab")
    process = subprocess.Popen(
        [daemon, "--socket", socket, "--db", database],
        stdin=subprocess.DEVNULL,
        stdout=stdout,
        stderr=stderr,
        start_new_session=True,
    )
    stdout.close()
    stderr.close()
    PID_PATH.write_text(f"{process.pid}\n", encoding="utf-8")


def systemctl(args: list[str]) -> int:
    filtered = [arg for arg in args if arg != "--user"]
    if filtered[:1] == ["show"]:
        pid = current_pid()
        if not pid:
            return 1
        print(pid)
        return 0
    if filtered == ["daemon-reload"]:
        return 0
    if filtered[:2] == ["enable", "--now"] or filtered[:1] == ["restart"]:
        start()
        return 0
    if filtered[:2] == ["is-active", "--quiet"]:
        return 0 if current_pid() else 1
    if filtered[:2] == ["disable", "--now"]:
        stop()
        return 0
    print(f"unsupported fake systemctl invocation: {args}", file=sys.stderr)
    return 2


def launchctl(args: list[str]) -> int:
    if args[:1] == ["print"]:
        pid = current_pid()
        if not pid:
            return 1
        print(f"pid = {pid};")
        return 0
    if args[:1] in (["bootstrap"], ["kickstart"]):
        start()
        return 0
    if args[:1] == ["bootout"]:
        stop()
        return 0
    print(f"unsupported fake launchctl invocation: {args}", file=sys.stderr)
    return 2


def main() -> int:
    command = Path(sys.argv[0]).name
    if command == "systemctl":
        return systemctl(sys.argv[1:])
    if command == "launchctl":
        return launchctl(sys.argv[1:])
    print(f"unsupported fake service manager name: {command}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
