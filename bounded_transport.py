"""Small, byte-bounded subprocess transport for host-owned optional tools."""

from __future__ import annotations

import os
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path


class BoundedTransportError(RuntimeError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class BoundedResult:
    returncode: int
    stdout: bytes
    stderr: bytes
    elapsed_seconds: float
    pid: int


def run_bounded(argv, *, input_bytes=b'', cwd, env, timeout=30, output_limit=65536):
    if (not isinstance(argv,(list,tuple)) or not argv or any(not isinstance(x,str) or not x for x in argv)
            or not isinstance(input_bytes,bytes) or len(input_bytes)>262144
            or not isinstance(env,dict) or not isinstance(timeout,(int,float)) or not 0<timeout<=300
            or not isinstance(output_limit,int) or not 0<output_limit<=1048576):
        raise BoundedTransportError('HOST_TRANSPORT_CONTRACT_INVALID')
    directory = Path(cwd)
    if not directory.is_dir():
        raise BoundedTransportError('HOST_TRANSPORT_CWD_INVALID')
    started = time.monotonic()
    try:
        process = subprocess.Popen(list(argv), cwd=str(directory), env=env,
            stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,bufsize=0,
            creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
    except OSError:
        raise BoundedTransportError('HOST_BINARY_UNAVAILABLE') from None
    output = {'stdout':bytearray(),'stderr':bytearray()}
    overflow = threading.Event()
    write_failure = threading.Event()
    def read(pipe, name):
        try:
            while True:
                chunk = os.read(pipe.fileno(),4096)
                if not chunk:
                    return
                output[name].extend(chunk)
                if len(output[name])>output_limit:
                    overflow.set()
                    process.kill()
                    return
        except OSError:
            return
    def write():
        try:
            view = memoryview(input_bytes)
            while view:
                written = os.write(process.stdin.fileno(),view[:4096])
                view = view[written:]
        except (OSError, ValueError):
            write_failure.set()
        finally:
            try: process.stdin.close()
            except (OSError, ValueError): pass
    readers = [threading.Thread(target=read,args=(pipe,name),daemon=True)
               for name,pipe in (('stdout',process.stdout),('stderr',process.stderr))]
    for thread in readers: thread.start()
    writer = threading.Thread(target=write,daemon=True)
    writer.start()
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)
        raise BoundedTransportError('HOST_TRANSPORT_TIMEOUT') from None
    finally:
        writer.join(timeout=1)
        for thread in readers: thread.join(timeout=1)
        for pipe in (process.stdin,process.stdout,process.stderr):
            try: pipe.close()
            except (OSError, ValueError): pass
    if writer.is_alive() or any(thread.is_alive() for thread in readers):
        raise BoundedTransportError('HOST_TRANSPORT_DRAIN_UNKNOWN')
    if overflow.is_set():
        raise BoundedTransportError('HOST_TRANSPORT_OUTPUT_TOO_LARGE')
    if write_failure.is_set() and input_bytes:
        raise BoundedTransportError('HOST_TRANSPORT_WRITE_FAILED')
    return BoundedResult(process.returncode,bytes(output['stdout']),bytes(output['stderr']),
                         time.monotonic()-started,process.pid)
