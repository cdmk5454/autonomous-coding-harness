"""Process shutdown barrier and explicit ownership of execution resources."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor  # Register Python's exit hook at startup.
from contextlib import contextmanager
import sys
import threading


class LifecycleError(RuntimeError):
    code = "CONTROL_SHUTTING_DOWN"


def assert_process_running() -> None:
    # Python sets this before joining non-daemon threads, before is_finalizing().
    if sys.is_finalizing() or getattr(threading, "_SHUTTING_DOWN", False):
        raise LifecycleError("CONTROL_SHUTTING_DOWN")


class ShutdownBarrier:
    def __init__(self):
        assert_process_running()
        self._guard = threading.RLock()
        self.state = "ACTIVE"

    @contextmanager
    def dispatch(self):
        with self._guard:
            assert_process_running()
            if self.state != "ACTIVE":
                raise LifecycleError("CONTROL_SHUTTING_DOWN")
            yield

    def close(self):
        # Admitted work drains before its resource owner releases the lease.
        self.state = "SHUTTING_DOWN"
        with self._guard:
            pass

    def restart(self):
        with self._guard:
            assert_process_running()
            self.state = "ACTIVE"


def create_executor(*, max_workers: int, thread_name_prefix: str):
    assert_process_running()
    return ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix=thread_name_prefix)
