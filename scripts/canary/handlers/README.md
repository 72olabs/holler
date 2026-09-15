# Contributor canary handlers

Each contributor-defined real-client scenario has two committed files with the
same ID:

- `../scenarios/C9.json` declares its name, clients, estimated model turns,
  timeout, and expected assertion names.
- `C9.py` implements `run(worker)` and returns those assertion names in exactly
  the declared order after the checks pass.

Use the next unused numeric ID. Custom handlers are included in the small
runtime bundle and execute in the credentialed canary runner, so they require
the same code review as any other credential-bound change. The controller only
loads `C<number>.py` from this committed directory; it never loads handlers
from `.runs`, `.docs`, an environment variable, or a command-line path.

Minimal shape:

```python
from typing import Any


def run(worker: Any) -> list[str]:
    worker.active_check = "c9-send"
    # Use Worker helpers such as run_claude, run_codex, and json_command.
    # Raise CanaryFailure when an assertion fails.
    return ["message-sent", "message-acknowledged"]
```

One-shot `run_claude` and `run_codex` helpers account for their own model use.
Handlers that directly create interactive client sessions must reserve and
charge `worker.ledger` exactly as the built-in C2 scenario does, and must close
every process in a `finally` block.
