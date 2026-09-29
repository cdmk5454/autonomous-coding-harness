"""Cross-process canonical ownership, reentrant only on the owning thread.

The lease spans compatibility, apply, integration and rollback; never just write.
No lease expiry permits stealing from an unknown/live canonical writer.
"""
from contextlib import contextmanager
from functools import wraps
from pathlib import Path
import hashlib
import os
import threading

_guard = threading.Lock()
_locks = {}
_local = threading.local()

class CanonicalBusy(RuntimeError):
    code = "CANONICAL_PROMOTION_BUSY"

@contextmanager
def canonical_ownership(workspace):
    workspace = Path(workspace).resolve()
    key = os.path.normcase(str(workspace))
    held = getattr(_local, "held", set())
    if key in held:
        yield
        return
    with _guard:
        guard = _locks.setdefault(key, threading.Lock())
    if not guard.acquire(blocking=False):
        raise CanonicalBusy()
    try:
        from job_queue import FileMutex, QueueError
        root = workspace.parent / ".harness-promotion-locks"
        from control_paths import ensure_safe_state_root
        root = ensure_safe_state_root(root, field="canonical_ownership", create=True)
        path = root / (hashlib.sha256(key.encode()).hexdigest() + ".lock")
        mutex = FileMutex(path, root=root, timeout=0)
        try:
            with mutex.hold():
                _local.held = held | {key}
                try:
                    yield
                finally:
                    _local.held = held
        except QueueError as exc:
            if exc.code == "QUEUE_LOCK_TIMEOUT":
                raise CanonicalBusy() from None
            raise
    finally:
        guard.release()

def owns_canonical(method):
    @wraps(method)
    def wrapped(self, *args, **kwargs):
        from qa_quarantine import CandidateError
        try:
            with canonical_ownership(self.workspace):
                return method(self, *args, **kwargs)
        except CanonicalBusy:
            raise CandidateError("CANONICAL_PROMOTION_BUSY") from None
    return wrapped
