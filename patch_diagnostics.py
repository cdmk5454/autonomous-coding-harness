"""Evidence-based Worker patch failure classification and artifacts."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping


CONTEXT_MISMATCH = "WORKER_PATCH_CONTEXT_MISMATCH"
CONTEXT_STALE = "WORKER_PATCH_CONTEXT_STALE"


def classify_patch_context(
    text: str,
    *,
    pre_read_sha256: str = "",
    pre_apply_sha256: str = "",
    mutation_provenance: Mapping[str, Any] | None = None,
) -> str:
    folded = (text or "").casefold()
    if not (
        "apply_patch verification failed" in folded
        or "target context not found" in folded
    ):
        return ""
    provenance = dict(mutation_provenance or {})
    if (
        len(pre_read_sha256) == 64
        and len(pre_apply_sha256) == 64
        and pre_read_sha256 != pre_apply_sha256
        and bool(provenance)
    ):
        return CONTEXT_STALE
    return CONTEXT_MISMATCH


def write_patch_diagnostic(root: str | Path, evidence: Mapping[str, Any]) -> dict[str, Any]:
    safe = dict(evidence)
    streams: dict[str, str] = {}
    for field in ("stdout", "stderr"):
        raw = str(safe.pop(field, ""))
        streams[field] = raw
        safe[f"{field}_sha256"] = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    safe.setdefault("created_at", datetime.now().astimezone().isoformat())
    payload = json.dumps(safe, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest()
    base = Path(root)
    target = base / f"PATCH-DIAG-{digest[:24].upper()}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    for field, raw in streams.items():
        stream_path = base / f"PATCH-DIAG-{digest[:24].upper()}.{field}.txt"
        stream_path.write_text(raw, encoding="utf-8")
        safe[f"{field}_artifact"] = str(stream_path)
    payload = json.dumps(safe, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest()
    target = base / f"PATCH-DIAG-{digest[:24].upper()}.json"
    target.write_bytes(payload)
    return {"path": str(target), "sha256": digest, "evidence": safe}
