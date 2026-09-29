"""Host-owned Pi RPC process inside the existing pinned Docker boundary.

Only a disposable candidate view is writable. The provider key enters through
stdin, never Docker argv, inspect metadata, source or a receipt.
"""

from __future__ import annotations

import hashlib
import json
import os
import queue
import re
import stat
import subprocess
import threading
import time
from pathlib import Path

from harness_11.core import BoundaryError, FenceProof, ProcessIdentity, Scope
from opencode_boundary import IMAGE, _mount_source, validate_tree
from opencode_runtime_adapter import OpenCodeAdapterError


PI_PACKAGE = Path(r'D:\hermes\pi-candidate-v0.87.1')
PI_VERSION = '0.87.1'
PI_MODEL = 'opencode-go/deepseek-v4.1-flash'
PI_CLI = '/runtime/pi/node_modules/@earendil-works/pi-coding-agent/dist/bundle/cli.js'
PI_TOOLS = 'read,edit,write,grep,find,ls'


class DockerPiProcessBoundary:
    def __init__(self, *, candidate_dir, context_dir, canonical_root, package_dir=PI_PACKAGE,
                 provider, model, credential_loader, docker='docker', timeout=15):
        self.candidate = Path(candidate_dir).resolve()
        self.context = Path(context_dir).resolve()
        self.canonical = Path(canonical_root).resolve()
        self.package = Path(package_dir).resolve()
        self.provider, self.model = provider, model
        self.credential_loader = credential_loader
        self.docker_command = [docker] if isinstance(docker,str) else list(docker)
        if not self.docker_command or any(not isinstance(part,str) or not part for part in self.docker_command):
            raise BoundaryError('PI_DOCKER_COMMAND_INVALID')
        self.timeout = timeout
        self.cid = ''
        self.client = None
        self.process = None
        self.binding = None
        self._stdout = queue.Queue(maxsize=256)
        self._stderr = queue.Queue(maxsize=256)
        self._counts = {'stdout':0, 'stderr':0}
        self._overflow = False
        self._quiesced = False
        self._receipt = {}

    def _docker(self, *args, timeout=None, check=True):
        from bounded_transport import BoundedTransportError, run_bounded
        try:
            result = run_bounded([*self.docker_command, *args],cwd=self.candidate,
                env=dict(os.environ),timeout=timeout or self.timeout,output_limit=65536)
        except BoundedTransportError:
            raise BoundaryError('PI_DOCKER_UNAVAILABLE') from None
        if check and result.returncode:
            raise BoundaryError('PI_DOCKER_COMMAND_FAILED')
        return result

    def preflight(self):
        for path in (self.candidate, self.context):
            try:
                validate_tree(path)
            except OpenCodeAdapterError as exc:
                raise BoundaryError('PI_SCOPE_UNSAFE') from exc
            if not path.is_dir() or path == self.canonical or path.is_relative_to(self.canonical):
                raise BoundaryError('PI_SCOPE_INVALID')
        if not self.package.is_dir():
            raise BoundaryError('PI_BINARY_UNAVAILABLE')
        for item in (self.package, *self.package.rglob('*')):
            metadata = item.lstat()
            if (stat.S_ISLNK(metadata.st_mode) or getattr(metadata,'st_file_attributes',0) & 0x400
                    or item.is_file() and metadata.st_nlink != 1):
                raise BoundaryError('PI_PACKAGE_LINK_DENIED')
            if item.name.casefold() in {'.env','.ssh','.aws','auth.json'}:
                raise BoundaryError('PI_PACKAGE_SECRET_DENIED')
        if self.canonical.is_relative_to(self.candidate) or self.canonical.is_relative_to(self.context):
            raise BoundaryError('PI_CANONICAL_EXPOSURE')
        if not re.fullmatch(r'[a-z0-9][a-z0-9_.-]{0,63}', self.provider):
            raise BoundaryError('PI_PROVIDER_INVALID')
        if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}', self.model):
            raise BoundaryError('PI_MODEL_INVALID')
        package = self.package/'node_modules/@earendil-works/pi-coding-agent/package.json'
        cli = self.package/'node_modules/@earendil-works/pi-coding-agent/dist/bundle/cli.js'
        if not package.is_file() or not cli.is_file():
            raise BoundaryError('PI_BINARY_UNAVAILABLE')
        if json.loads(package.read_text(encoding='utf-8')).get('version') != PI_VERSION:
            raise BoundaryError('PI_VERSION_DRIFT')
        self._docker('image', 'inspect', IMAGE, '--format', '{{.Id}}')
        return {'runtime':'pi', 'version':PI_VERSION,
                'cli_sha256':hashlib.sha256(cli.read_bytes()).hexdigest(), 'image':IMAGE,
                'tools':PI_TOOLS.split(','), 'canonical_mounted':False}

    def verify_scope(self, binding, scope: Scope) -> bool:
        if (not isinstance(scope,Scope) or scope.candidate_root != str(self.candidate)
                or scope.canonical_root != str(self.canonical)
                or binding.role != 'WORKER'):
            return False
        self.preflight()
        return True

    def _inspect(self):
        result = self._docker('inspect', self.cid)
        try:
            item = json.loads(result.stdout)[0]
        except (ValueError, IndexError, TypeError):
            raise BoundaryError('PI_CONTAINER_INSPECT_INVALID') from None
        mounts = item.get('Mounts') or []
        expected = {'/workspace':(self.candidate,True), '/context':(self.context,False),
                    '/runtime/pi':(self.package,False)}
        if len(mounts) != len(expected):
            raise BoundaryError('PI_MOUNT_DRIFT')
        for mount in mounts:
            target = mount.get('Destination')
            if target not in expected:
                raise BoundaryError('PI_MOUNT_DRIFT')
            source, writable = expected[target]
            if (mount.get('Type') != 'bind' or bool(mount.get('RW')) != writable
                    or _mount_source(mount.get('Source')) != _mount_source(source)):
                raise BoundaryError('PI_MOUNT_DRIFT')
        host = item.get('HostConfig') or {}
        if not host.get('ReadonlyRootfs') or host.get('Privileged') or host.get('CapAdd'):
            raise BoundaryError('PI_CONTAINER_POLICY_DRIFT')
        return item

    def _read_pipe(self, pipe, target, kind):
        try:
            while True:
                chunk = os.read(pipe.fileno(),4096)
                if not chunk:
                    break
                self._counts[kind] += len(chunk)
                if self._counts[kind] > (8_388_608 if kind == 'stdout' else 1_048_576):
                    self._overflow = True
                    self.stop_local_server()
                    break
                try:
                    target.put(chunk, timeout=0.1)
                except queue.Full:
                    self._overflow = True
                    self.stop_local_server()
                    break
        except OSError:
            self._overflow = True

    def start(self, binding, scope, *, timeout):
        if self.cid or self.client or not self.verify_scope(binding,scope):
            raise BoundaryError('PI_BOUNDARY_NOT_ENFORCED')
        self.binding = binding
        name = 'harness-pi-' + hashlib.sha256(str(self.candidate).casefold().encode()).hexdigest()[:32]
        script = ('IFS= read -r OPENCODE_API_KEY; export OPENCODE_API_KEY; '
                  'exec /usr/local/bin/node '+PI_CLI+' --mode rpc --no-session '
                  '--no-extensions --no-skills --no-context-files --no-approve '
                  '--tools '+PI_TOOLS+' --provider "$1" --model "$2"')
        command = ['create','-i','--name',name,'--network','bridge','--read-only',
            '--cap-drop','ALL','--security-opt','no-new-privileges',
            '--pids-limit','128','--memory','1g','--cpus','1',
            '--tmpfs','/tmp:rw,nosuid,size=128m','--tmpfs','/root:rw,nosuid,size=128m',
            '--tmpfs','/opt/data:rw,noexec,nosuid,size=128m',
            '--mount',f'type=bind,src={self.candidate},dst=/workspace',
            '--mount',f'type=bind,src={self.context},dst=/context,readonly',
            '--mount',f'type=bind,src={self.package},dst=/runtime/pi,readonly',
            '--entrypoint','/bin/sh','-w','/workspace',IMAGE,
            '-c',script,'pi',self.provider,self.model]
        for key, value in (('job_id',binding.job_id),('execution_id',binding.execution_id),
                           ('attempt_id',binding.attempt_id),('candidate_id',binding.candidate_id)):
            command[1:1] = ['--label','harness.'+key+'='+value]
        try:
            self.cid = self._docker(*command, timeout=timeout).stdout.decode('ascii').strip()
            if not re.fullmatch(r'[0-9a-f]{64}',self.cid):
                raise BoundaryError('PI_CONTAINER_ID_INVALID')
            self._inspect()
            self.client = subprocess.Popen([*self.docker_command,'start','-ai',self.cid],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
            self.process = ProcessIdentity(self.client.pid,
                f'{self.client.pid}:{time.time_ns()}:{self.cid}', self.cid, self.provider, 'pi')
            threading.Thread(target=self._read_pipe,args=(self.client.stdout,self._stdout,'stdout'),daemon=True).start()
            threading.Thread(target=self._read_pipe,args=(self.client.stderr,self._stderr,'stderr'),daemon=True).start()
            secret = self.credential_loader()
            if not isinstance(secret,str) or not secret or '\n' in secret or '\r' in secret:
                raise BoundaryError('PI_PROVIDER_CREDENTIAL_UNAVAILABLE')
            self.send(secret.encode('utf-8')+b'\n',timeout=timeout)
            self._receipt = {**{k:getattr(binding,k) for k in ('job_id','execution_id','attempt_id','candidate_id','epoch')},
                'boundary':'docker-pi/1','container_id':self.cid,'process_key':self.process.key,
                'package_version':PI_VERSION,'image':IMAGE,'provider':self.provider,'model':self.model,
                'tools':PI_TOOLS.split(','), 'quiesced':False}
            return self.process
        except Exception:
            self.stop_local_server()
            raise

    def send(self, raw: bytes, *, timeout: float) -> None:
        if not isinstance(raw,bytes) or len(raw)>262144 or not self.client or not self.client.stdin:
            raise BoundaryError('PI_SEND_INVALID')
        result = []
        def write():
            try:
                self.client.stdin.write(raw)
                self.client.stdin.flush()
            except (OSError, ValueError) as exc:
                result.append(exc)
        writer = threading.Thread(target=write,daemon=True)
        writer.start(); writer.join(timeout)
        if writer.is_alive() or result:
            self.stop_local_server()
            raise BoundaryError('PI_SEND_TIMEOUT_OR_FAILED')

    def read(self, *, timeout: float, maximum: int) -> tuple[bytes,bytes]:
        if self._overflow:
            raise BoundaryError('PI_TRANSPORT_OUTPUT_TOO_LARGE')
        try:
            stdout = self._stdout.get(timeout=timeout)
        except queue.Empty:
            if self.client is not None and self.client.poll() is None:
                raise TimeoutError()
            stdout = b''
        stderr = bytearray()
        while not self._stderr.empty() and len(stderr)<maximum:
            stderr.extend(self._stderr.get_nowait())
        if len(stdout)>maximum or len(stderr)>maximum or self._overflow:
            raise BoundaryError('PI_TRANSPORT_OUTPUT_TOO_LARGE')
        return stdout,bytes(stderr)

    def poll(self):
        return self.client.poll() if self.client is not None else None

    def fence(self,binding,process,token,*,timeout):
        if binding != self.binding or process != self.process:
            raise BoundaryError('PI_FENCE_BINDING_MISMATCH')
        self.stop_local_server(timeout=timeout)
        if not self._quiesced:
            raise BoundaryError('PI_WRITER_QUIESCENCE_UNKNOWN')
        ref = hashlib.sha256(json.dumps(self._receipt,sort_keys=True).encode()).hexdigest()
        return FenceProof(binding.key,process.key,token,True,True,'DOCKER_INSPECT',ref)

    def stop_local_server(self, timeout=None):
        if self.cid and not self._quiesced:
            self._docker('rm','-f',self.cid,timeout=timeout or self.timeout,check=False)
            absent = self._docker('inspect',self.cid,timeout=timeout or self.timeout,check=False)
            self._quiesced = absent.returncode != 0
        if self.client is not None:
            try:
                self.client.wait(timeout=timeout or self.timeout)
            except subprocess.TimeoutExpired:
                self.client.kill()
                self.client.wait(timeout=5)
            for pipe in (self.client.stdin,self.client.stdout,self.client.stderr):
                if pipe:
                    pipe.close()
        self._receipt['quiesced'] = self._quiesced

    def confirm_quiescence(self):
        return self._quiesced and (self.client is None or self.client.poll() is not None)

    def boundary_receipt(self):
        return dict(self._receipt)
