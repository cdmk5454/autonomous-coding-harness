"""Prompt/context diet helpers; stable contract stays in the native session."""

from __future__ import annotations


def durable_continuation_prompt() -> str:
    return (
        "Continue the current managed task from this durable session. "
        "Reconcile the interrupted turn, inspect current workspace state, "
        "finish remaining work, and return the normal completion protocol."
    )


def deduplicate_repeated_lines(text: str) -> tuple[str, int]:
    seen: set[str] = set()
    output: list[str] = []
    removed = 0
    for line in str(text or "").splitlines():
        normalized = " ".join(line.split()).casefold()
        if len(normalized) >= 12 and normalized in seen:
            removed += 1
            continue
        if len(normalized) >= 12:
            seen.add(normalized)
        output.append(line)
    return "\n".join(output), removed

