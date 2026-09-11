"""Startup-frozen Harness runtime identity for read-only health reporting."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


class RuntimeIdentityError(RuntimeError):
    def __init__(self, code: str, detail: str = ""):
        self.code = code
        self.detail = detail[:300]
        super().__init__(code + (f": {self.detail}" if self.detail else ""))


@dataclass(frozen=True)
class RuntimeIdentity:
    harness_version: str
    runtime_build_id: str
    runtime_manifest_sha256: str
    runtime_started_at: str
    runtime_pid: int

    def public(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def load(
        cls,
        harness_root: str | Path,
        *,
        pid: int | None = None,
        started_at: str | None = None,
    ) -> "RuntimeIdentity":
        """Read VERSION and its release manifest once for this process lifetime."""
        root = Path(harness_root).resolve()
        version_path = root / "VERSION"
        try:
            version = version_path.read_text(encoding="utf-8").strip()
        except (OSError, UnicodeError) as exc:
            raise RuntimeIdentityError("RUNTIME_VERSION_UNREADABLE") from exc
        if not version or len(version) > 64:
            raise RuntimeIdentityError("RUNTIME_VERSION_INVALID")
        manifest_path = root / "releases" / version / "SHA256SUMS.json"
        try:
            manifest_bytes = manifest_path.read_bytes()
            manifest = json.loads(manifest_bytes.decode("utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise RuntimeIdentityError("RUNTIME_MANIFEST_UNREADABLE", str(manifest_path)) from exc
        if not isinstance(manifest, dict) or str(manifest.get("version", "")) != version:
            raise RuntimeIdentityError("RUNTIME_MANIFEST_VERSION_MISMATCH")
        manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()
        build_id = str(manifest.get("build_id") or manifest.get("release_id") or "").strip()
        if not build_id:
            build_id = f"{version}+{manifest_sha[:16]}"
        return cls(
            harness_version=version,
            runtime_build_id=build_id,
            runtime_manifest_sha256=manifest_sha,
            runtime_started_at=started_at or datetime.now().astimezone().isoformat(),
            runtime_pid=int(pid if pid is not None else os.getpid()),
        )
