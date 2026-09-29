"""Bounded command observation, not a filesystem/network security sandbox.

Linux: an owned process group, bounded pipe capture, explicit timeout/cancel.
Windows: the existing suspended-launch kill-on-close JobObject boundary. Windows
output files are polled and only bounded prefixes are read; this is NOT a disk quota.
Commands must already have passed frozen-plan/executable/environment checks.
"""
from __future__ import annotations
from dataclasses import dataclass
import math
import os
from pathlib import Path
import selectors
import signal
import subprocess
import tempfile
import time

MAX_OUTPUT_BYTES = 2 * 1024 * 1024
RECEIPT_POLICY = 'bounded-command/1'

@dataclass(frozen=True)
class CommandObservation:
    returncode: int
    stdout: bytes
    stderr: bytes
    elapsed_seconds: float
    reason: str
    output_complete: bool
    quiescent: bool
    boundary: str
    pid: int


def _group_live(pgid):
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    if not Path('/proc').is_dir():
        return True  # Cannot independently disambiguate zombies on this host.
    for entry in Path('/proc').iterdir():
        if not entry.name.isdigit():
            continue
        try:
            fields = (entry/'stat').read_text().rsplit(')', 1)[1].split()
            if int(fields[2]) == pgid and fields[0] not in {'Z', 'X'}:
                return True
        except FileNotFoundError:
            continue
        except (PermissionError, OSError, ValueError, IndexError):
            return True
    return False


def _kill_group(process):
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=2)
    except subprocess.TimeoutExpired:
        return False
    deadline = time.monotonic()+2
    while _group_live(process.pid):
        if time.monotonic() >= deadline:
            return False
        time.sleep(.01)
    return True


def _posix(argv, cwd, env, timeout, limit, cancelled):
    start = time.monotonic()
    process = subprocess.Popen(argv, cwd=str(cwd), env=env, stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    buffers = {'stdout': bytearray(), 'stderr': bytearray()}
    reason, complete, quiet = '', False, False
    selector = selectors.DefaultSelector()
    for name in buffers:
        pipe = getattr(process, name); os.set_blocking(pipe.fileno(), False)
        selector.register(pipe, selectors.EVENT_READ, name)
    ended = None
    try:
        while selector.get_map() or process.poll() is None:
            elapsed = time.monotonic()-start
            if cancelled is not None and cancelled():
                reason = 'CANCELLED'; break
            if elapsed >= timeout:
                reason = 'TIMEOUT'; break
            for key, _ in selector.select(min(.02, max(.001, timeout-elapsed))):
                chunk = os.read(key.fileobj.fileno(), 8192)
                if not chunk:
                    selector.unregister(key.fileobj); continue
                remaining = limit-sum(map(len, buffers.values()))
                buffers[key.data].extend(chunk[:max(0, remaining)])
                if len(chunk) > remaining:
                    reason = 'OUTPUT_LIMIT'; break
            if reason:
                break
            if process.poll() is not None:
                ended = ended or time.monotonic()
                if selector.get_map() and time.monotonic()-ended > .15:
                    reason = 'CHILD_PROCESS_NOT_QUIESCENT'; break
        if not reason and _group_live(process.pid):
            reason = 'CHILD_PROCESS_NOT_QUIESCENT'
        if reason:
            quiet = _kill_group(process)
        else:
            quiet, complete = True, True
        code = process.poll()
        if code is None:
            code = -1
        if code < 0 and not reason:
            reason = 'SIGNAL_TERMINATION'
        if not quiet:
            reason = 'QUIESCENCE_NOT_VERIFIED'
        return CommandObservation(code, bytes(buffers['stdout']), bytes(buffers['stderr']),
            time.monotonic()-start, reason, complete, quiet,
            'POSIX_PROCESS_GROUP_NOT_SECURITY_SANDBOX', process.pid)
    finally:
        if process.poll() is None or _group_live(process.pid):
            _kill_group(process)
        selector.close()
        process.stdout.close(); process.stderr.close()


def _windows(argv, cwd, env, timeout, limit, cancelled):
    # No native import/handle creation on portable import paths.
    from hermes_process_boundary import OwnedProcess
    start = time.monotonic()
    with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
        process = OwnedProcess(argv[0], argv, env, str(cwd), out, stderr=err)
        reason, quiet, complete = '', False, False
        try:
            while process.poll() is None:
                if cancelled is not None and cancelled(): reason = 'CANCELLED'; break
                if time.monotonic()-start >= timeout: reason = 'TIMEOUT'; break
                if os.fstat(out.fileno()).st_size + os.fstat(err.fileno()).st_size > limit:
                    reason = 'OUTPUT_LIMIT'; break
                time.sleep(.01)
            if not reason:
                # Job membership can lag the root process exit briefly on Win32.
                # Allow that accounting to settle, but never accept a lingering child.
                settle_deadline = min(start + timeout, time.monotonic() + .1)
                while process.members():
                    if cancelled is not None and cancelled():
                        reason = 'CANCELLED'; break
                    if time.monotonic() >= settle_deadline:
                        reason = 'CHILD_PROCESS_NOT_QUIESCENT'; break
                    time.sleep(.01)
            if os.fstat(out.fileno()).st_size + os.fstat(err.fileno()).st_size > limit:
                reason = 'OUTPUT_LIMIT'
            if reason:
                try: process.terminate_and_wait(timeout=2); quiet = True
                except (OSError, TimeoutError): reason = 'QUIESCENCE_NOT_VERIFIED'
            else: quiet, complete = True, True
            code = process.poll()
            out.seek(0); stdout = out.read(limit)
            err.seek(0); stderr = err.read(max(0, limit-len(stdout)))
            # NT exception exit statuses are infrastructure, not assertion failures.
            if code is not None and code >= 0x80000000 and not reason:
                reason = 'NATIVE_PROCESS_EXCEPTION'
            return CommandObservation(code if code is not None else -1, stdout, stderr,
                time.monotonic()-start, reason, complete, quiet, 'WINDOWS_OWNED_JOB_OBJECT', process.pid)
        finally:
            process.release()


def run(argv, *, cwd, env, timeout, output_limit=MAX_OUTPUT_BYTES, cancelled=None):
    if (type(timeout) not in {int, float} or not math.isfinite(timeout) or timeout <= 0
            or type(output_limit) is not int or not 1 <= output_limit <= MAX_OUTPUT_BYTES
            or not isinstance(argv, (list, tuple)) or not argv
            or any(not isinstance(arg, str) or '\0' in arg for arg in argv)):
        raise ValueError('TEST_COMMAND_LIMITS_INVALID')
    if cancelled is not None and cancelled():
        return CommandObservation(-1, b'', b'', 0.0, 'CANCELLED', False, True, 'NOT_STARTED', 0)
    return (_windows if os.name == 'nt' else _posix)(argv, cwd, env, timeout, output_limit, cancelled)
