"""Fixed-endpoint, synthetic-data managed conversation fixture for canaries.

Only the worker constructs this object. Handlers never receive credentials,
socket paths, SQL, process handles, or an arbitrary network destination.
"""

from __future__ import annotations

import http.client
import json
from pathlib import Path
import socket
import struct
import time
from typing import Any


READS = frozenset({
    "channel.list", "channel.get", "channel.message", "channel.history",
    "channel.continuation.preflight", "channel.view.get", "channel.responses",
    "channel.inbox",
})
WRITES = frozenset({
    "channel.create", "channel.post", "channel.membership",
    "channel.continuation.commit", "channel.view.update",
    "channel.response.resolve", "channel.claim", "channel.delivery",
})
ACTORS = frozenset({"canary-claude", "canary-codex", "canary-controller", "canary-stranger"})
HUMAN = "human:canary"
MAX_FRAME = 2 << 20


def receive_exact(connection: Any, size: int) -> bytes:
    if not 0 <= size <= MAX_FRAME:
        raise ValueError("managed API frame exceeds limit")
    output = bytearray()
    while len(output) < size:
        chunk = connection.recv(size - len(output))
        if not chunk:
            raise ValueError("managed API frame truncated")
        output.extend(chunk)
    return bytes(output)


def exchange(connection: Any, number: int, operation: str, arguments: dict) -> dict:
    raw = json.dumps({"id": number, "op": operation, "args": arguments}).encode()
    if len(raw) > MAX_FRAME:
        raise ValueError("managed API request exceeds limit")
    connection.sendall(struct.pack(">I", len(raw)) + raw)
    size = struct.unpack(">I", receive_exact(connection, 4))[0]
    result = json.loads(receive_exact(connection, size))
    if not isinstance(result, dict) or result.get("id") != number:
        raise ValueError("managed API response correlation failed")
    return result


class ManagedFixture:
    def __init__(self, *, socket_path: Path, credentials: Path, endpoint: Any,
                 restart: Any, snapshot: Any, fail: Any):
        self._socket = socket_path
        self._credentials = credentials
        self._endpoint = endpoint
        self._restart = restart
        self._snapshot = snapshot
        self._fail = fail
        self._session = ""

    def require(self, condition: bool, check: str) -> None:
        if not condition:
            self._fail(check)

    def api(self, actor: str, operation: str, arguments: dict | None = None,
            *, error: str | None = None, run_id: str = "managed-controller") -> Any:
        if actor not in ACTORS or operation not in READS | WRITES:
            self._fail("managed fixture operation or identity is not allowed")
        if run_id != "managed-controller" and (actor != "canary-codex" or run_id not in {
            "c10-create", "c10-consume-codex",
        }):
            self._fail("managed fixture run identity is not allowed")
        args = arguments or {}
        if operation == "channel.create" and args.get("project_id") != "canary":
            self._fail("managed fixture project is not canary")
        result = self._raw(actor,
                           "invoke_read_capability" if operation in READS else "invoke_write_capability",
                           {"name": operation, "arguments": args}, run_id=run_id)
        if error is not None:
            self.require(not result.get("ok") and result.get("error", {}).get("code") == error,
                         "managed fixture expected API rejection: " + error)
            return None
        self.require(result.get("ok") is True, "managed fixture API operation failed: " + operation)
        return result.get("result")

    def _raw(self, actor: str, operation: str, arguments: dict, *, run_id: str = "managed-controller") -> dict:
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
                connection.settimeout(10)
                connection.connect(str(self._socket))
                hello = exchange(connection, 1, "hello", {
                    "protocol": 1, "actor": actor, "run_id": run_id,
                    "client": "canary-fixture", "capabilities": ["capability-bridge-v1"],
                })
                if not hello.get("ok"):
                    return hello
                result = exchange(connection, 2, operation, arguments)
        except (OSError, ValueError, KeyError):
            self._fail("managed fixture API transport failed")
        return result

    def assert_legacy_hidden(self, message_ids: list[str]) -> None:
        for actor in ("canary-claude", "canary-codex"):
            response = self._raw(actor, "check_inbox", {"limit": 100})
            self.require(response.get("ok") is True, "legacy inbox probe failed")
            self.require(not any(item in json.dumps(response) for item in message_ids),
                         "managed delivery leaked into legacy inbox")
        for stream in ("durable", "operational"):
            response = self._raw("operator", "list_events", {
                "partition": "canary", "stream": stream, "after": 0, "limit": 1000,
            })
            self.require(response.get("ok") is True, "legacy event probe failed")
            self.require(not any(item in json.dumps(response) for item in message_ids),
                         "managed ID leaked into legacy events")
        denied = self._raw(HUMAN, "ping", {})
        self.require(denied.get("error", {}).get("code") == "capability_required",
                     "human identity accepted on Unix transport")

    def logout(self) -> None:
        status, _ = self._http("/logout", {})
        self.require(status == 200, "managed gateway logout failed")
        self._session = ""

    def _http(self, path: str, payload: dict, *, authenticated: bool = True) -> tuple[int, Any]:
        # Endpoint is selected by hollerd's loopback listener, never by a handler.
        endpoint = self._endpoint()
        if not endpoint.startswith("http://127.0.0.1:"):
            self._fail("managed gateway is not loopback")
        port = int(endpoint.rsplit(":", 1)[1])
        headers = {"Content-Type": "application/json", "Origin": endpoint}
        if authenticated:
            headers["Authorization"] = "Bearer " + self._session
        for attempt in range(3):
            connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
            try:
                connection.request("POST", path, json.dumps(payload), headers)
                response = connection.getresponse()
                raw = response.read(MAX_FRAME + 1)
                if len(raw) > MAX_FRAME:
                    self._fail("managed gateway response exceeds limit")
                status = response.status
                value = json.loads(raw)
            except (OSError, ValueError):
                self._fail("managed gateway transport failed")
            finally:
                connection.close()
            if status != 429 or attempt == 2:
                return status, value
            time.sleep(1)
        raise AssertionError("unreachable")

    def login(self) -> None:
        # This is the synthetic local gateway credential, never subscription OAuth.
        credential = json.loads(self._credentials.read_text())
        status, value = self._http("/login", {"bearer": credential["bearer"]}, authenticated=False)
        self.require(status == 200 and value.get("human") == HUMAN, "managed human login failed")
        self._session = value["session"]

    def human(self, operation: str, arguments: dict | None = None, *, error: str | None = None) -> Any:
        if operation not in READS | WRITES | {"supervision.preflight", "supervision.commit"}:
            self._fail("managed human operation is not allowed")
        if not self._session:
            self.login()
        status, result = self._http("/rpc", {"operation": operation, "arguments": arguments or {}})
        if error is not None:
            self.require(status != 200 and result.get("error") == error,
                         "managed fixture expected human rejection: " + error)
            return None
        self.require(status == 200, "managed human operation failed: " + operation)
        return result

    def supervise(self, agent: str, *, linked: bool, key: str) -> None:
        self.require(agent in ACTORS, "managed supervision actor outside fixture")
        preview = self.human("supervision.preflight", {"agent": agent})
        self.human("supervision.commit", {
            "preview": preview, "human": HUMAN if linked else "", "idempotency_key": key,
        })

    def create(self, actor: str, participants: list[str], key: str, *, kind: str = "named") -> dict:
        return self.api(actor, "channel.create", {
            "project_id": "canary", "kind": kind, "title": key,
            "participants": participants, "idempotency_key": key,
        })

    def post(self, actor: str, channel: str, key: str, *, attention: list[str] | None = None,
             body: dict | None = None, **extra: Any) -> dict:
        current = self.api(actor, "channel.get", {"channel_id": channel})
        return self.api(actor, "channel.post", {
            "channel_id": channel, "expected_policy_revision": current["policy_revision"],
            "idempotency_key": key, "body": body or {"text": "synthetic canary context"},
            "attention_targets": attention or [], **extra,
        })

    def inbox(self, actor: str) -> list:
        return self.api(actor, "channel.inbox", {"limit": 100})

    def restart(self) -> None:
        self._restart()
        self._session = ""

    def terminal(self, message_ids: list[str]) -> list[dict]:
        # Worker stops the daemon first; fixed body-free SQL, no caller SQL.
        result = self._snapshot(message_ids)
        self._session = ""
        return result
