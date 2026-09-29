"""Per-execution Docker isolation for the fixed OpenCode Go worker.

Docker is a trusted host control plane. Candidate/context trees must be exclusively
owned by the harness while this adapter is active; no other host writer is allowed.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import threading
import time

from opencode_runtime_adapter import OpenCodeRuntimeAdapter, OpenCodeAdapterError
from opencode_provider_relay import MAX_BODY, forward
from runtime_adapter import RuntimeCapabilities, RuntimeDescriptor

IMAGE = "sha256:99641e57ec762c59e54cb44aa6746b7fc68c18b3c5ddb088af54234c613d9294"
BINARY_SHA256 = "87bd160e053af86b5b409daabf71f8dc05bbc3a2a3a5f563f36011cdf706a999"
CATALOG_SHA256 = "4e8a9639e3c05d939c5fea516cf5a7bc9cb2b6e62bf51569306d9ad463b0257d"
MODEL = "opencode-go/deepseek-v4.1-flash"
VERSION = "1.18.30"
SECRET_NAMES = {".git", ".env", ".ssh", ".aws", ".azure", ".codex", "auth.json", "credentials", "id_rsa", "id_ed25519"}


def _fail(code):
    raise OpenCodeAdapterError("OPENCODE_BOUNDARY_" + code)


def validate_tree(path):
    path = Path(path).absolute()
    if not path.exists():
        _fail("PATH_MISSING")
    for item in (*path.parents, path, *(path.rglob("*") if path.is_dir() else ())):
        stat = item.lstat()
        if item.is_symlink() or getattr(stat, "st_file_attributes", 0) & 0x400:
            _fail("LINK_REJECTED")
        if item.is_file() and stat.st_nlink != 1:
            _fail("HARDLINK_REJECTED")
        if item == path or path in item.parents:
            name = item.name.casefold()
            if name in SECRET_NAMES or name.startswith(".env.") or name.endswith((".pem", ".key", ".pfx", ".p12")):
                _fail("SECRET_PATH_REJECTED")
    return path.resolve()


def _mount_source(value):
    value = str(value).replace("\\", "/").rstrip("/").casefold()
    # Docker Desktop translates Windows bind sources in inspect output.
    for prefix in ("/run/desktop/mnt/host/", "/host_mnt/"):
        if value.startswith(prefix):
            suffix = value[len(prefix):]
            return suffix[0] + ":/" + suffix[2:]
    return value


class IsolatedOpenCodeRuntimeAdapter(OpenCodeRuntimeAdapter):
    def __init__(self, *, execution_id, execution_epoch, candidate_id, job_id, attempt_id,
                 candidate_dir, context_dir, runtime_binary, provider_catalog,
                 credential_loader, canonical_root, evidence_dir,
                 docker_executable="docker", readonly_candidate=False,
                 expected_runtime_sha256=BINARY_SHA256, image_digest=IMAGE, timeout=30.0):
        super().__init__(executable="/runtime/opencode", timeout=timeout)
        self.identity = dict(execution_id=execution_id, execution_epoch=execution_epoch,
                             candidate_id=candidate_id, job_id=job_id, attempt_id=attempt_id)
        if any(not isinstance(v, str) or not v.strip() for v in self.identity.values()):
            _fail("IDENTITY_REQUIRED")
        if expected_runtime_sha256 != BINARY_SHA256 or image_digest != IMAGE:
            _fail("RUNTIME_PIN_REJECTED")
        self.candidate = Path(candidate_dir).absolute()
        self.context = Path(context_dir).absolute()
        self.binary = Path(runtime_binary).absolute()
        self.catalog = Path(provider_catalog).absolute()
        self.canonical = Path(canonical_root).resolve()
        self.evidence_dir = Path(evidence_dir).absolute()
        self.docker = str(docker_executable)
        self.credential_loader = credential_loader
        self.readonly_candidate = bool(readonly_candidate)
        self.cid = ""
        self._relay = None
        self._relay_thread = None
        self._started = False
        self._removed = False
        self._receipt = {**self.identity, "boundary": "docker-opencode/1", "image": IMAGE,
                         "runtime_sha256": BINARY_SHA256, "route": MODEL,
                         "candidate_path": str(self.candidate), "readonly_candidate": self.readonly_candidate,
                         "quiesced": False, "provider_requests": []}
        self._validate_paths()

    def _validate_paths(self):
        self._validate_evidence_path()
        for path in (self.candidate, self.context, self.binary, self.catalog):
            validate_tree(path)
            if path == self.canonical or self.canonical in path.parents or path in self.canonical.parents:
                _fail("CANONICAL_EXPOSURE")
        if self.candidate == self.context or self.candidate in self.context.parents or self.context in self.candidate.parents:
            _fail("MOUNT_OVERLAP")
        for path in (self.binary, self.catalog, self.evidence_dir):
            if path == self.candidate or self.candidate in path.parents:
                _fail("CONTROL_PATH_EXPOSED")
        if hashlib.sha256(self.binary.read_bytes()).hexdigest() != BINARY_SHA256:
            _fail("BINARY_DRIFT")
        if hashlib.sha256(self.catalog.read_bytes()).hexdigest() != CATALOG_SHA256:
            _fail("CATALOG_DRIFT")
        if not self.candidate.is_dir() or not self.context.is_dir():
            _fail("DIRECTORY_REQUIRED")

    def _validate_evidence_path(self):
        # Preserve the original spelling until every existing ancestor has
        # been checked, including Windows junctions and dangling symlinks.
        for path in (self.evidence_dir, *self.evidence_dir.parents):
            if path.is_symlink():
                _fail("EVIDENCE_LINK_REJECTED")
            try:
                info = path.lstat()
            except FileNotFoundError:
                continue
            if getattr(info, "st_file_attributes", 0) & 0x400:
                _fail("EVIDENCE_LINK_REJECTED")
            if not path.is_dir():
                _fail("EVIDENCE_DIRECTORY_REQUIRED")
        resolved = self.evidence_dir.resolve()
        if resolved == self.canonical or self.canonical in resolved.parents or resolved in self.canonical.parents:
            _fail("CANONICAL_EXPOSURE")
        candidate = self.candidate.resolve()
        if resolved == candidate or candidate in resolved.parents:
            _fail("CONTROL_PATH_EXPOSED")

    def _docker(self, *args, input=None, timeout=30, check=True):
        try:
            result = subprocess.run([self.docker, *args], input=input, capture_output=True,
                                    text=True, encoding="utf-8", timeout=timeout)
        except (OSError, subprocess.SubprocessError):
            _fail("DOCKER_UNAVAILABLE")
        if check and result.returncode:
            _fail("DOCKER_COMMAND_FAILED")
        return result

    def _inspect(self):
        try:
            return json.loads(self._docker("inspect", self.cid).stdout)[0]
        except (ValueError, IndexError, KeyError):
            _fail("INSPECT_INVALID")

    def _expected_mounts(self):
        return {"/workspace": (self.candidate, not self.readonly_candidate),
                "/context": (self.context, False), "/runtime/opencode": (self.binary, False),
                "/relay.py": (Path(__file__).with_name("opencode_provider_relay.py").resolve(), False)}

    def _verify_ownership(self, item):
        labels = item.get("Config", {}).get("Labels", {})
        if item.get("Id") != self.cid or item.get("Image") != IMAGE or any(labels.get("harness." + k) != v for k, v in self.identity.items()):
            _fail("OWNERSHIP_MISMATCH")

    def verify_inspection(self, item):
        host = item.get("HostConfig", {})
        self._verify_ownership(item)
        if (host.get("NetworkMode") != "none" or host.get("ReadonlyRootfs") is not True
                or set(host.get("CapDrop") or []) != {"ALL"}
                or not any(x in {"no-new-privileges", "no-new-privileges:true"} for x in host.get("SecurityOpt", []))
                or host.get("Privileged") or host.get("Binds") or host.get("Devices")
                or host.get("PidMode") or host.get("IpcMode") == "host"
                or host.get("PortBindings") or host.get("CapAdd")):
            _fail("SECURITY_DRIFT")
        if (host.get("PidsLimit") != 128 or host.get("Memory") != 1073741824
                or host.get("NanoCpus") != 1000000000
                or host.get("Tmpfs") != {"/tmp": "rw,nosuid,size=256m", "/root": "rw,nosuid,size=512m", "/opt/data": "rw,noexec,nosuid,size=256m"}):
            _fail("RESOURCE_DRIFT")
        mounts = item.get("Mounts", [])
        expected = self._expected_mounts()
        if len(mounts) != len(expected) or {m.get("Destination") for m in mounts} != set(expected):
            _fail("MOUNT_DRIFT")
        for mount in mounts:
            target = mount.get("Destination")
            if target not in expected:
                _fail("MOUNT_DRIFT")
            source, writable = expected[target]
            if mount.get("Type") != "bind" or mount.get("RW") != writable or _mount_source(mount.get("Source")) != _mount_source(source):
                _fail("MOUNT_DRIFT")
        return item

    def preflight(self):
        self._validate_paths()
        info = json.loads(self._docker("image", "inspect", IMAGE).stdout)[0]
        if info.get("Id") != IMAGE:
            _fail("IMAGE_DRIFT")
        return RuntimeDescriptor(runtime="opencode", version=VERSION, executable="/runtime/opencode",
            capabilities=RuntimeCapabilities(False, False, True, True, False, True, interrupt=True),
            protocol_revision="opencode-server-http-sse/1", permission_tool_profile="docker-opencode/1",
            session_process_lifetime="EPHEMERAL_EXECUTION_CONTAINER", sandbox_semantics="DOCKER_ENFORCED",
            execution_environment="DOCKER_LINUX", validation_disposition="PASS",
            capability_contract={key: "SUPPORTED" for key in ("session.create", "session.read_query", "turn.submit", "turn.interrupt", "events.terminal_query", "runtime.permission_tool_semantics", "runtime.protocol_revision")},
            detail="PINNED_ARTIFACT_PREFLIGHT; LIVE_SERVER_CHECK_ON_START")

    def _save_receipt(self):
        self._validate_evidence_path()
        self.evidence_dir.mkdir(parents=True, exist_ok=True)
        target = self.evidence_dir / ("opencode-boundary-" + hashlib.sha256(json.dumps(self.identity, sort_keys=True).encode()).hexdigest() + ".json")
        temp = target.with_suffix(".tmp")
        temp.write_text(json.dumps(self._receipt, indent=2), encoding="utf-8")
        temp.replace(target)

    def boundary_receipt(self):
        return json.loads(json.dumps(self._receipt))

    def _forward_loop(self):
        count = 0
        while self._relay and self._relay.stdout:
            line = self._relay.stdout.readline(MAX_BODY * 2)
            if not line:
                break
            try:
                count += 1
                if count > 256 or len(line) >= MAX_BODY * 2:
                    raise ValueError()
                result, receipt = forward(json.loads(line), self.credential_loader)
            except Exception as exc:
                result = {"status": 502, "type": "application/json", "body": base64.b64encode(b'{"error":"BOUNDARY_PROVIDER_REJECTED"}').decode()}
                receipt = {"status": "REJECTED", "error_type": type(exc).__name__}
            self._receipt["provider_requests"].append(receipt)
            try:
                self._relay.stdin.write(json.dumps(result) + "\n")
                self._relay.stdin.flush()
            except (OSError, ValueError):
                break

    def start_local_server(self, *, cwd, port=0, pure=True):
        if Path(cwd).resolve() != self.candidate or self._started or self.cid or port or not pure:
            _fail("START_BINDING_REJECTED")
        self.preflight()
        # Docker's atomic name reservation prevents two adapters mounting the
        # same candidate concurrently, including after host-process crashes.
        candidate_owner = hashlib.sha256(os.path.normcase(str(self.candidate.resolve())).encode()).hexdigest()[:32]
        command = ["create", "--name", "harness-opencode-" + candidate_owner,
                   "--network", "none", "--read-only", "--cap-drop", "ALL",
                   "--security-opt", "no-new-privileges", "--pids-limit", "128", "--memory", "1g", "--cpus", "1",
                   "--tmpfs", "/tmp:rw,nosuid,size=256m", "--tmpfs", "/root:rw,nosuid,size=512m",
                   "--tmpfs", "/opt/data:rw,noexec,nosuid,size=256m", "--entrypoint", "/bin/sh", "-w", "/workspace"]
        for key, value in self.identity.items():
            command += ["--label", "harness." + key + "=" + value]
        for target, (source, writable) in self._expected_mounts().items():
            if "," in str(source):
                _fail("MOUNT_PATH_INVALID")
            command += ["--mount", f"type=bind,src={source},dst={target}" + ("" if writable else ",readonly")]
        command += [IMAGE, "-c", "sleep infinity"]
        self.cid = self._docker(*command).stdout.strip()
        self._started = True
        self._receipt["container_id"] = self.cid
        try:
            self.verify_inspection(self._inspect())
            self._docker("start", self.cid)
            version = self._docker("exec", self.cid, "/runtime/opencode", "--version").stdout.strip()
            if version != VERSION:
                _fail("VERSION_DRIFT")
            config = {"model": MODEL, "small_model": MODEL, "enabled_providers": ["opencode-go"],
                      "provider": {"opencode-go": {"options": {"apiKey": "boundary-relay", "baseURL": "http://127.0.0.1:18699/zen/go/v1"}}}, "permission": {"*": "allow"}}
            self._receipt["config_sha256"] = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()
            setup = "import sys,pathlib,json; d=json.load(sys.stdin); [(pathlib.Path(p).parent.mkdir(parents=True,exist_ok=True),pathlib.Path(p).write_text(v)) for p,v in d.items()]"
            files = {"/root/.config/opencode/opencode.json": json.dumps(config), "/root/.cache/opencode/models.json": self.catalog.read_text(encoding="utf-8")}
            self._docker("exec", "-i", self.cid, "python3", "-c", setup, input=json.dumps(files))
            self._relay = subprocess.Popen([self.docker, "exec", "-i", self.cid, "python3", "-u", "/relay.py"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, encoding="utf-8")
            ready = threading.Event()
            def relay_start():
                if self._relay.stdout.readline(100).strip() == "RELAY_READY":
                    ready.set()
                    self._forward_loop()
            self._relay_thread = threading.Thread(target=relay_start, daemon=True)
            self._relay_thread.start()
            if not ready.wait(self.timeout):
                _fail("RELAY_START_TIMEOUT")
            # No host provider key enters container environment or process argv.
            self.password = "boundary-local-server"
            self._docker("exec", "-d", "-e", "OPENCODE_SERVER_PASSWORD=" + self.password, self.cid,
                         "sh", "-c", "exec /runtime/opencode serve --hostname 127.0.0.1 --port 18700 --pure >/tmp/server.log 2>&1")
            self.base_url = "http://127.0.0.1:18700"
            deadline = time.monotonic() + self.timeout
            while time.monotonic() < deadline:
                try:
                    health = self.request("GET", "/global/health", _request_timeout=2)
                    if not isinstance(health, dict) or health.get("healthy") is not True or health.get("version") != VERSION:
                        _fail("HEALTH_INVALID")
                    self._receipt["mounts_verified"] = True
                    self._save_receipt()
                    return self.base_url
                except OpenCodeAdapterError as exc:
                    self._receipt["last_startup_error"] = exc.code
                    time.sleep(0.1)
            diagnostics = self._docker("exec", self.cid, "cat", "/tmp/server.log", check=False).stdout
            # Retain a bounded hash, never arbitrary runtime logs in receipts.
            self._receipt["startup_log_sha256"] = hashlib.sha256(diagnostics.encode()).hexdigest()
            _fail("SERVER_START_TIMEOUT")
        except Exception:
            self.stop_local_server()
            raise

    def request(self, method, path, body=None, *, _request_timeout=None):
        if not self.cid or self._removed:
            _fail("NOT_RUNNING")
        inspected = self.verify_inspection(self._inspect())
        if not inspected.get("State", {}).get("Running") or not self._relay or self._relay.poll() is not None:
            _fail("PROCESS_LOST")
        allowed_path = re.fullmatch(r"/[A-Za-z0-9_/-]+", path)
        message_query = re.fullmatch(r"/session/[A-Za-z0-9_-]+/message\?limit=([1-9][0-9]{0,2})", path)
        if method not in {"GET", "POST", "DELETE"} or not (allowed_path or (method == "GET" and message_query)):
            _fail("API_ROUTE_REJECTED")
        if body and "model" in body and body["model"] != {"providerID": "opencode-go", "modelID": "deepseek-v4.1-flash"}:
            _fail("MODEL_REJECTED")
        script = """import sys,json,base64,urllib.request,urllib.error
p=json.load(sys.stdin)
r=urllib.request.Request('http://127.0.0.1:18700'+p['path'],data=None if p['body'] is None else json.dumps(p['body']).encode(),method=p['method'],headers=p['headers'])
try:
    with urllib.request.urlopen(r,timeout=p['timeout']) as q:
        data=q.read(32000001)
        if len(data)>32000000:
            raise ValueError('RESPONSE_TOO_LARGE')
        out={'status':q.status,'body':base64.b64encode(data).decode('ascii')}
except urllib.error.HTTPError as exc:
    out={'status':exc.code,'body':''}
except Exception:
    out={'transport_error':True}
print(json.dumps(out))
"""
        request_timeout = self.timeout if _request_timeout is None else _request_timeout
        result = self._docker("exec", "-i", self.cid, "python3", "-c", script,
                              input=json.dumps(dict(method=method, path=path, body=body, headers=self._headers(), timeout=request_timeout)), timeout=request_timeout + 5, check=False)
        if result.returncode:
            raise OpenCodeAdapterError("OPENCODE_TRANSPORT_ERROR")
        try:
            envelope = json.loads(result.stdout)
            if not isinstance(envelope, dict):
                raise ValueError()
            if envelope.get("transport_error") is True:
                raise OpenCodeAdapterError("OPENCODE_TRANSPORT_ERROR")
            status = envelope.get("status")
            if type(status) is not int or not 100 <= status <= 599:
                raise ValueError()
            if not 200 <= status <= 299:
                raise OpenCodeAdapterError("OPENCODE_HTTP_ERROR", str(status))
            encoded = envelope.get("body")
            if not isinstance(encoded, str) or len(encoded) > 42666668:
                raise ValueError()
            raw = base64.b64decode(encoded, validate=True)
            if len(raw) > 32000000:
                raise ValueError()
            return json.loads(raw) if raw else None
        except (ValueError, TypeError):
            _fail("API_RESPONSE_INVALID")

    def create_session(self, prompt, *, cwd, model="", **kwargs):
        self._check_call(cwd, model)
        return super().create_session(prompt, cwd=cwd, model=MODEL, **kwargs)

    def submit_turn(self, session_id, prompt, *, cwd, model="", **kwargs):
        self._check_call(cwd, model)
        return super().submit_turn(session_id, prompt, cwd=cwd, model=MODEL, **kwargs)

    def _check_call(self, cwd, model):
        if Path(cwd).resolve() != self.candidate or model not in {"", MODEL}:
            _fail("CALL_BINDING_REJECTED")

    def confirm_quiescence(self):
        # API idle and a dead docker-exec client do not prove descendant death.
        return bool(self._started and self._removed and self._receipt.get("quiesced"))

    def stop_local_server(self):
        if not self.cid or self._removed:
            return
        # Stop an owned container even if its mounts/security inspection failed.
        self._verify_ownership(self._inspect())
        self._docker("rm", "-f", self.cid)
        # Successful list is essential: an unavailable daemon is not absence.
        remaining = self._docker("ps", "-a", "--no-trunc", "--format", "{{.ID}}").stdout.splitlines()
        if self.cid in remaining:
            _fail("QUIESCENCE_UNCONFIRMED")
        self._removed = True
        self._receipt["quiesced"] = True
        self._receipt["cleanup"] = "CONTAINER_REMOVED_VERIFIED"
        self.base_url = ""
        if self._relay is not None:
            try:
                self._relay.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self._relay.kill()
                self._relay.wait(timeout=5)
            if self._relay_thread is not None:
                self._relay_thread.join(timeout=1)
            # The host forwarder can be inside its bounded HTTPS call; let that
            # daemon thread close its pipes on completion rather than racing it.
            if self._relay_thread is None or not self._relay_thread.is_alive():
                for stream in (self._relay.stdin, self._relay.stdout):
                    if stream:
                        stream.close()
        self._save_receipt()
