"""Harness-owned disposable workspaces; atomic-write temps stay at their target."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import tempfile as _tempfile
import time
import uuid

_SESSION = uuid.uuid4().hex
_MARKER = ".harness-owner.json"


def temp_root() -> Path:
    return Path(os.environ.get("HARNESS_TEMP_ROOT") or (Path(_tempfile.gettempdir()) / "autonomous-coding-harness")).resolve()


def scratch_root(area: str = "runtime") -> Path:
    if area not in {"test", "operator", "reviewer", "runtime"}:
        raise ValueError("HARNESS_TEMP_AREA_INVALID")
    parent = temp_root() / area / _SESSION[:8]
    parent.mkdir(parents=True, exist_ok=True)
    marker = parent / _MARKER
    if not marker.exists():
        marker.write_text(json.dumps({"owner": "autonomous-coding-harness", "pid": os.getpid(),
                                      "session": _SESSION, "created_at": time.time()}), encoding="utf-8")
    return parent


class TemporaryDirectory(_tempfile.TemporaryDirectory):
    def __init__(self, suffix=None, prefix=None, dir=None, **kwargs):
        # Existing callers' cwd scratch placement must never escape this root.
        super().__init__(suffix=suffix, prefix=prefix, dir=scratch_root("test"), **kwargs)


def cleanup_stale(*, pid_is_active, stale_seconds: float = 86400, now=None):
    """Only remove marked dead-owner directories; uncertain ownership is kept."""
    diagnostics = []
    current = time.time() if now is None else now
    root = temp_root()
    for area in ("test", "operator", "reviewer", "runtime"):
        parent = root / area
        if not parent.is_dir() or parent.is_symlink() or parent.resolve().parent != root:
            continue
        for candidate in parent.iterdir():
            try:
                if not candidate.is_dir() or candidate.is_symlink() or candidate.resolve().parent != parent.resolve():
                    continue
                marker = candidate / _MARKER
                if not marker.is_file() or marker.is_symlink():
                    continue
                record = json.loads(marker.read_text(encoding="utf-8"))
                if record.get("owner") != "autonomous-coding-harness" or record.get("session") == _SESSION:
                    continue
                if current - float(record["created_at"]) < stale_seconds or pid_is_active(int(record["pid"])) is not False:
                    continue
                shutil.rmtree(candidate)
            except Exception as exc:
                diagnostics.append({"path": str(candidate), "code": "TEMP_CLEANUP_FAILED", "error": type(exc).__name__})
    return diagnostics


def pid_active_or_unknown(pid):
    """A denied/uncertain process probe cannot authorize scratch deletion."""
    if pid <= 0:
        return True
    if os.name == 'nt':
        import ctypes
        from ctypes import wintypes
        kernel=ctypes.WinDLL('kernel32',use_last_error=True)
        kernel.OpenProcess.argtypes=(wintypes.DWORD,wintypes.BOOL,wintypes.DWORD)
        kernel.OpenProcess.restype=wintypes.HANDLE
        kernel.CloseHandle.argtypes=(wintypes.HANDLE,)
        handle=kernel.OpenProcess(0x1000,False,pid)
        if not handle:
            return ctypes.get_last_error()!=87  # ERROR_INVALID_PARAMETER: absent PID
        kernel.CloseHandle(handle)
        return True
    try:
        os.kill(pid,0)
        return True
    except ProcessLookupError:
        return False
    except OSError:
        return True


# Compatibility for test utilities using other stdlib tempfile helpers.
# Production atomic writers continue to import stdlib tempfile directly.
def __getattr__(name):
    return getattr(_tempfile, name)
