# Contributor canary handlers

Each contributor-defined real-client scenario has two committed files with the
same ID:

- `../scenarios/C9.json` declares its name, clients, estimated model turns,
  timeout, and expected assertion names.
- `C9.py` implements `run(context)` and returns those assertion names in exactly
  the declared order after the checks pass.

Use the next unused numeric ID. Custom handlers are included in the small
runtime bundle and execute in the credentialed canary runner, so they require
the same code review as any other credential-bound change. The controller only
loads `C<number>.py` from this committed directory; it never loads handlers
from `.runs`, `.docs`, an environment variable, or a command-line path.

Minimal shape:

```python
from typing import Any


def run(context: Any) -> list[str]:
    context.check("c9-send")
    done = context.marker("C9_DONE")
    context.run_codex(
        "c9-codex",
        "c9-send",
        "Perform the check and finish with " + context.marker_instruction(done),
        done,
    )
    return ["message-sent", "message-acknowledged"]
```

The context exposes `marker`, `marker_instruction`, budgeted `run_claude` and
`run_codex` calls, `interactive`, `wait_for_live_registration`, bounded
`query`, `check`, `fail`, and a read-only `fixture` reference. Interactive
sessions are context managers; their `turn` method reserves budget before
submitting and charges only after observing the expected marker. Every handler
must consume exactly its declared `estimated_model_turns`.

Managed scenarios additionally use `context.managed()` for the fixed synthetic
protocol/human fixture and `session.wake()` for a budgeted unsolicited turn.
The Python fixture's private attributes are not a security boundary: handlers
must not inspect its bearer/session or export bodies. Evidence records only
reviewed IDs, states, counts and declared oracle labels.

`HandlerContext` is an ergonomics and accidental-spend control, not a Python
security sandbox. Commit review plus exact-tree approval is the credential
security boundary. A static CI tripwire rejects direct process, PTY, socket,
signal, or Worker access, and the harness imports selected handlers in a
credential-free subprocess before building. Do not work around those checks.
