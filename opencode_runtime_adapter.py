"""Provider-independent OpenCode server/CLI RuntimeAdapter implementation."""

from __future__ import annotations

import base64
import csv
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import subprocess
import time
from typing import Any, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen
import uuid

from runtime_safety import assert_secret_free_argv
from runtime_adapter import (
    ADAPTER_API_REVISION,
    CapabilitySupport,
    NormalizedRuntimeEvent,
    RuntimeCapabilities,
    RuntimeCapabilityError,
    RuntimeDescriptor,
    SessionQuery,
    SessionState,
    TurnInvocation,
    ValidationDisposition,
    normalize_usage,
)


PERSONAL = "PERSONAL"
MANAGED = "MANAGED"
BOUNDARY_MODES = frozenset({PERSONAL, MANAGED})


class OpenCodeAdapterError(RuntimeError):
    def __init__(self, code: str, detail: str = ""):
        self.code = code
        self.detail = detail[:300]
        super().__init__(code + (f": {self.detail}" if self.detail else ""))


class OpenCodeRuntimeAdapter:
    runtime_name = "opencode"

    def __init__(
        self,
        executable: str = "opencode",
        *,
        base_url: str = "",
        mode: str = MANAGED,
        username: str = "opencode",
        password: str = "",
        timeout: float = 10.0,
    ):
        mode = str(mode).upper()
        if mode not in BOUNDARY_MODES:
            raise OpenCodeAdapterError("OPENCODE_BOUNDARY_MODE_INVALID")
        self.executable = executable
        self.base_url = base_url.rstrip("/")
        self.mode = mode
        self.username = username
        self.password = password
        self.timeout = timeout
        self._server: subprocess.Popen[str] | None = None
        self._server_port = 0
        self._server_pids: set[int] = set()

    @staticmethod
    def _command_id(value: str) -> str:
        return value or "CMD-" + uuid.uuid4().hex.upper()

    @staticmethod
    def _native_message_id(command_id: str) -> str:
        return "msg_" + hashlib.sha256(command_id.encode("utf-8")).hexdigest()

    def preflight(self) -> RuntimeDescriptor:
        executable = shutil.which(self.executable) or ""
        if not executable:
            return RuntimeDescriptor(
                runtime=self.runtime_name, version="", executable="",
                schema_compatible=False,
                validation_disposition=ValidationDisposition.UNAVAILABLE.value,
                detail="OPENCODE_EXECUTABLE_NOT_FOUND",
                capabilities=RuntimeCapabilities(False, False, False, False, False, False),
            )
        try:
            version_result = subprocess.run(
                [executable, "--version"], capture_output=True, text=True,
                encoding="utf-8", errors="replace", timeout=10, check=False,
            )
            serve_help = subprocess.run(
                [executable, "serve", "--help"], capture_output=True, text=True,
                encoding="utf-8", errors="replace", timeout=10, check=False,
            )
            run_help = subprocess.run(
                [executable, "run", "--help"], capture_output=True, text=True,
                encoding="utf-8", errors="replace", timeout=10, check=False,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            return RuntimeDescriptor(
                runtime=self.runtime_name, version="", executable=executable,
                schema_compatible=False,
                validation_disposition=ValidationDisposition.UNAVAILABLE.value,
                detail=type(exc).__name__,
                capabilities=RuntimeCapabilities(False, False, False, False, False, False),
            )
        run_text = run_help.stdout + run_help.stderr
        serve_text = serve_help.stdout + serve_help.stderr
        compatible = (
            version_result.returncode == serve_help.returncode == run_help.returncode == 0
            and "--session" in run_text and "--format" in run_text
            and "--hostname" in serve_text and "--port" in serve_text
        )
        detail = "CLI_AND_SERVER_SCHEMA_PREFLIGHT" if compatible else "OPENCODE_CLI_SCHEMA_UNSUPPORTED"
        if compatible and self.mode == MANAGED and self.base_url and not self.password:
            compatible = False
            detail = "MANAGED_REMOTE_AUTH_REQUIRED"
        supported = CapabilitySupport.SUPPORTED.value
        partial = CapabilitySupport.PARTIAL.value
        unsupported = CapabilitySupport.UNSUPPORTED.value
        return RuntimeDescriptor(
            runtime=self.runtime_name,
            version=version_result.stdout.strip() or version_result.stderr.strip(),
            adapter_revision=ADAPTER_API_REVISION,
            executable=executable,
            capabilities=RuntimeCapabilities(
                durable_session=True,
                explicit_resume=True,
                session_query=True,
                terminal_query=True,
                reconnect=True,
                normalized_events=True,
                interrupt=True,
                pending_interaction=True,
                native_queue=False,
                managed_remote=True,
            ),
            protocol_revision="opencode-server-http-sse/1",
            structured_output_schema_revision="UNSUPPORTED",
            permission_tool_profile="opencode-managed-permissions/1",
            native_queue_maturity="UNSUPPORTED",
            session_process_lifetime="PERSISTED_SERVER_STORAGE",
            sandbox_semantics="PERMISSION_RULES_NOT_OS_SANDBOX",
            execution_environment="WINDOWS_NATIVE" if os.name == "nt" else "CURRENT_NATIVE",
            capability_contract={
                "session.create": supported,
                "session.resume": supported,
                "session.read_query": supported,
                "session.fork": unsupported,
                "turn.submit": supported,
                "turn.steer": unsupported,
                "turn.interrupt": supported,
                "events.subscribe": partial,
                "events.reconnect": partial,
                "events.replay_cursor": unsupported,
                "events.terminal_query": supported,
                # The native server surfaces pending approvals/questions on its
                # SSE stream (permission.asked). This adapter binds no SSE
                # client and /session/status exposes no pending state, so the
                # canonical observation operation is only partially covered.
                # A permission wait is therefore observed as a no-progress
                # technical timeout, never as a product defect.
                "interaction.pending_approval": partial,
                "interaction.pending_question": partial,
                "interaction.permission_response": unsupported,
                "interaction.response_delivery": supported,
                "output.structured": partial,
                "output.schema_revision": unsupported,
                "runtime.permission_tool_semantics": supported,
                "runtime.usage_metadata": partial,
                "runtime.native_queue": unsupported,
                "runtime.protocol_revision": supported,
            },
            schema_compatible=compatible,
            live_model_validated=False,
            validation_disposition=(
                ValidationDisposition.PASS.value if compatible
                else ValidationDisposition.FAIL.value
            ),
            detail=detail,
        )

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if self.password:
            token = base64.b64encode(f"{self.username}:{self.password}".encode()).decode("ascii")
            headers["Authorization"] = "Basic " + token
        return headers

    def request(self, method: str, path: str, body: Mapping[str, Any] | None = None) -> Any:
        if not self.base_url:
            raise OpenCodeAdapterError("OPENCODE_SERVER_URL_REQUIRED")
        if self.mode == MANAGED and not self.password:
            raise OpenCodeAdapterError("MANAGED_REMOTE_AUTH_REQUIRED")
        raw = None if body is None else json.dumps(body, ensure_ascii=False).encode("utf-8")
        request = Request(self.base_url + path, data=raw, method=method, headers=self._headers())
        try:
            with urlopen(request, timeout=self.timeout) as response:
                payload = response.read()
                if not payload:
                    return None
                return json.loads(payload.decode("utf-8"))
        except HTTPError as exc:
            raise OpenCodeAdapterError("OPENCODE_HTTP_ERROR", str(exc.code)) from exc
        except (URLError, OSError, ValueError) as exc:
            raise OpenCodeAdapterError("OPENCODE_TRANSPORT_ERROR", type(exc).__name__) from exc

    @staticmethod
    def _model_body(model: str) -> dict[str, str]:
        if not model:
            return {}
        if "/" not in model or model.startswith("/") or model.endswith("/"):
            raise OpenCodeAdapterError("OPENCODE_MODEL_ID_INVALID")
        provider_id, model_id = model.split("/", 1)
        return {"providerID": provider_id, "modelID": model_id}

    def create_session(self, prompt: str, *, cwd: str | Path, model: str = "",
                       reasoning_effort: str = "medium", command_id: str = "") -> TurnInvocation:
        # Session creation is intentionally provider independent. Prompt delivery
        # is a separate ledger command after the returned session id is bound.
        return TurnInvocation(
            command_id=self._command_id(command_id), transport="HTTP", method="POST",
            url=self.base_url + "/session", body={"title": "Harness managed session"},
        )

    def resume_session(self, session_id: str, prompt: str, *, cwd: str | Path,
                       model: str = "", reasoning_effort: str = "medium",
                       command_id: str = "") -> TurnInvocation:
        return self.submit_turn(
            session_id, prompt, cwd=cwd, model=model,
            reasoning_effort=reasoning_effort, command_id=command_id,
        )

    def submit_turn(self, session_id: str, prompt: str, *, cwd: str | Path,
                    model: str = "", reasoning_effort: str = "medium",
                    command_id: str = "") -> TurnInvocation:
        if not session_id:
            raise OpenCodeAdapterError("NATIVE_SESSION_ID_REQUIRED")
        model_body = self._model_body(model)
        canonical_command_id = self._command_id(command_id)
        body: dict[str, Any] = {
            "messageID": self._native_message_id(canonical_command_id),
            "parts": [{"type": "text", "text": prompt}],
            **({"model": model_body} if model_body else {}),
        }
        return TurnInvocation(
            command_id=canonical_command_id, session_id=session_id, resume=True,
            transport="HTTP", method="POST",
            url=self.base_url + f"/session/{quote(session_id, safe='')}/prompt_async",
            body=body,
        )

    def fork_session(self, session_id: str, prompt: str, *, cwd: str | Path,
                     model: str = "", reasoning_effort: str = "medium",
                     command_id: str = "") -> TurnInvocation:
        raise RuntimeCapabilityError("OPENCODE_SESSION_FORK_UNSUPPORTED")

    def steer_turn(self, session_id: str, turn_id: str, prompt: str) -> bool:
        raise RuntimeCapabilityError("OPENCODE_TURN_STEER_UNSUPPORTED")

    def subscribe_events(self, session_id: str, *, cursor: str = "") -> Any:
        if cursor:
            raise RuntimeCapabilityError("OPENCODE_EVENT_REPLAY_UNSUPPORTED")
        raise RuntimeCapabilityError("OPENCODE_SSE_CLIENT_NOT_BOUND")

    def replay_events(self, session_id: str, *, cursor: str) -> Any:
        raise RuntimeCapabilityError("OPENCODE_EVENT_REPLAY_UNSUPPORTED")

    def respond_interaction(self, session_id: str, interaction_id: str,
                            response: Mapping[str, Any]) -> bool:
        raise RuntimeCapabilityError("OPENCODE_PERMISSION_RESPONSE_NOT_MAPPED")

    def execute_invocation(self, invocation: TurnInvocation) -> Any:
        if invocation.transport != "HTTP" or not invocation.method:
            raise OpenCodeAdapterError("OPENCODE_INVOCATION_INVALID")
        if not invocation.url.startswith(self.base_url + "/"):
            raise OpenCodeAdapterError("OPENCODE_INVOCATION_ORIGIN_MISMATCH")
        path = invocation.url[len(self.base_url):]
        return self.request(invocation.method, path, invocation.body)

    def normalize_event(self, event: Mapping[str, Any]) -> tuple[NormalizedRuntimeEvent, ...]:
        event_type = str(event.get("type") or "")
        props = event.get("properties") if isinstance(event.get("properties"), Mapping) else event
        info = props.get("info") if isinstance(props.get("info"), Mapping) else {}
        part = props.get("part") if isinstance(props.get("part"), Mapping) else {}
        session_id = str(
            props.get("sessionID") or props.get("session_id")
            or info.get("sessionID") or part.get("sessionID") or ""
        )
        turn_id = str(
            props.get("messageID") or props.get("message_id")
            or info.get("id") or part.get("messageID") or ""
        )
        native_id = str(props.get("id") or part.get("id") or turn_id)
        lowered = event_type.casefold()
        pending_kind = (
            "TOOL_PERMISSION" if "permission" in lowered
            else "CLARIFICATION" if "question" in lowered else ""
        )
        terminal = event_type in {"session.idle", "session.error", "message.completed"}
        success = True if event_type in {"session.idle", "message.completed"} else False if event_type == "session.error" else None
        text = str(part.get("text") or props.get("text") or "") if "text" in str(part.get("type") or lowered) else ""
        kind = (
            "SESSION_BOUND" if event_type in {"session.created", "server.connected"} else
            "TURN_STARTED" if event_type in {"session.busy", "message.updated"} else
            "TURN_COMPLETED" if success is True else
            "TURN_FAILED" if success is False else
            "PENDING_INPUT" if pending_kind else "EVENT"
        )
        return (NormalizedRuntimeEvent(
            kind=kind, runtime=self.runtime_name, session_id=session_id,
            turn_id=turn_id, native_event_id=native_id, terminal=terminal,
            success=success, pending_kind=pending_kind, result_text=text,
            error_code="OPENCODE_TURN_FAILED" if success is False else "",
            raw_type=event_type,
            usage=normalize_usage(
                info.get("tokens") if isinstance(info.get("tokens"), Mapping)
                else props.get("usage") if isinstance(props.get("usage"), Mapping) else {}
            ),
        ),)

    def health(self) -> dict[str, Any]:
        value = self.request("GET", "/global/health")
        if not isinstance(value, Mapping) or value.get("healthy") is not True:
            raise OpenCodeAdapterError("OPENCODE_HEALTH_INVALID")
        return dict(value)

    def query_session(self, session_id: str, *, cwd: str | Path) -> SessionQuery:
        if not session_id:
            return SessionQuery("", SessionState.LOST.value, detail="SESSION_ID_MISSING")
        try:
            session = self.request("GET", f"/session/{quote(session_id, safe='')}")
            statuses = self.request("GET", "/session/status") or {}
        except OpenCodeAdapterError as exc:
            if exc.code == "OPENCODE_HTTP_ERROR" and exc.detail == "404":
                return SessionQuery(session_id, SessionState.LOST.value, detail="SESSION_NOT_FOUND")
            return SessionQuery(session_id, SessionState.UNKNOWN.value, detail=exc.code)
        native = statuses.get(session_id, {}) if isinstance(statuses, Mapping) else {}
        status_type = str(native.get("type") or native.get("status") or "idle").casefold()
        state = (
            SessionState.ACTIVE.value if status_type in {"busy", "running", "retry"}
            else SessionState.IDLE.value
        )
        current_turn = str((session or {}).get("messageID") or (session or {}).get("currentMessageID") or "") if isinstance(session, Mapping) else ""
        return SessionQuery(session_id, state, current_turn, terminal=False)

    def query_terminal(self, session_id: str, turn_id: str = "", *, cwd: str | Path) -> SessionQuery:
        return self.query_session(session_id, cwd=cwd)

    def interrupt(self, session_id: str, turn_id: str = "") -> bool:
        value = self.request("POST", f"/session/{quote(session_id, safe='')}/abort", {})
        return bool(value)

    def pending_approval_or_question(self, session_id: str, *, cwd: str | Path) -> SessionQuery:
        # Permission/question requests arrive on the SSE event stream. Session
        # status remains authoritative for whether execution is still active.
        return self.query_session(session_id, cwd=cwd)

    def reconcile(self, session_id: str, *, cwd: str | Path) -> SessionQuery:
        try:
            self.health()
        except OpenCodeAdapterError as exc:
            return SessionQuery(session_id, SessionState.UNKNOWN.value, detail=exc.code)
        return self.query_session(session_id, cwd=cwd)

    @staticmethod
    def _free_loopback_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            return int(sock.getsockname()[1])

    @staticmethod
    def _windows_listener_pids(port: int) -> set[int]:
        result = subprocess.run(
            ["netstat", "-ano", "-p", "tcp"],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
            timeout=10, check=False,
        )
        if result.returncode != 0:
            return set()
        found: set[int] = set()
        for line in result.stdout.splitlines():
            fields = line.split()
            if len(fields) < 5 or fields[0].upper() != "TCP":
                continue
            if fields[-2].upper() != "LISTENING":
                continue
            try:
                local_port = int(fields[1].rsplit(":", 1)[1])
                pid = int(fields[-1])
            except (IndexError, ValueError):
                continue
            if local_port == port and pid > 0:
                found.add(pid)
        return found

    @staticmethod
    def _windows_process_name(pid: int) -> str:
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
            capture_output=True, text=True, encoding="utf-8", errors="replace",
            timeout=10, check=False,
        )
        if result.returncode != 0:
            return ""
        for row in csv.reader(result.stdout.splitlines()):
            if len(row) > 1 and row[1].strip() == str(pid):
                return row[0].strip().casefold()
        return ""

    @staticmethod
    def _expected_windows_process_names(executable: str) -> set[str]:
        path = Path(executable)
        return {
            path.name.casefold(),
            (path.stem + ".exe").casefold(),
        }

    def start_local_server(self, *, cwd: str | Path, port: int = 0, pure: bool = True) -> str:
        if self._server is not None and self._server.poll() is None:
            return self.base_url
        # An explicit credential is mandatory only for a pre-configured
        # (remote) managed endpoint. A harness-owned local server started here
        # has no base_url yet and gets an ephemeral per-instance credential
        # below, matching the documented MANAGED-local auth contract.
        if self.mode == MANAGED and self.base_url and not self.password:
            raise OpenCodeAdapterError("MANAGED_REMOTE_AUTH_REQUIRED")
        executable = shutil.which(self.executable)
        if not executable:
            raise OpenCodeAdapterError("OPENCODE_EXECUTABLE_NOT_FOUND")
        port = int(port or self._free_loopback_port())
        command = [executable, "serve", "--hostname", "127.0.0.1", "--port", str(port)]
        if pure:
            command.append("--pure")
        env = os.environ.copy()
        if not self.password:
            # OpenCode server >=1.18.30 rejects unauthenticated requests even
            # on loopback. A harness-owned local server gets an ephemeral
            # per-instance credential; it is never persisted or logged.
            self.password = secrets.token_urlsafe(24)
            self.username = self.username or "opencode"
        env["OPENCODE_SERVER_PASSWORD"] = self.password
        env["OPENCODE_SERVER_USERNAME"] = self.username
        flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
        assert_secret_free_argv(command, env=env)
        # The server outlives this call and nothing drains its streams while
        # running; PIPE handles would dead-lock the server once the OS pipe
        # buffers fill. Server diagnostics come from the health/status API.
        self._server = subprocess.Popen(
            command, cwd=str(Path(cwd).resolve()), env=env,
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            creationflags=flags,
        )
        self._server_port = port
        self._server_pids = set()
        self.base_url = f"http://127.0.0.1:{port}"
        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            if self._server.poll() is not None:
                raise OpenCodeAdapterError("OPENCODE_SERVER_START_FAILED")
            try:
                self.health()
            except OpenCodeAdapterError:
                time.sleep(0.1)
                continue
            if os.name == "nt":
                expected = self._expected_windows_process_names(executable)
                ownership_deadline = min(deadline, time.monotonic() + 2)
                while time.monotonic() < ownership_deadline:
                    self._server_pids = {
                        pid for pid in self._windows_listener_pids(port)
                        if self._windows_process_name(pid) in expected
                    }
                    if self._server_pids:
                        break
                    time.sleep(0.05)
                if not self._server_pids:
                    self.stop_local_server()
                    raise OpenCodeAdapterError(
                        "OPENCODE_SERVER_PROCESS_OWNERSHIP_UNVERIFIED"
                    )
            return self.base_url
        self.stop_local_server()
        raise OpenCodeAdapterError("OPENCODE_SERVER_START_TIMEOUT")

    def stop_local_server(self) -> None:
        server = self._server
        self._server = None
        port = self._server_port
        tracked_pids = set(self._server_pids)
        self._server_port = 0
        self._server_pids = set()
        if server is None:
            return
        try:
            if os.name == "nt":
                if port:
                    tracked_pids.update(self._windows_listener_pids(port))
                executable = shutil.which(self.executable) or self.executable
                expected = self._expected_windows_process_names(executable)
                if server.poll() is None:
                    subprocess.run(
                        ["taskkill", "/PID", str(server.pid), "/T", "/F"],
                        capture_output=True, text=True, encoding="utf-8",
                        errors="replace", timeout=10, check=False,
                    )
                for pid in sorted(tracked_pids, reverse=True):
                    name = self._windows_process_name(pid)
                    if not name:
                        continue
                    if name not in expected:
                        raise OpenCodeAdapterError(
                            "OPENCODE_SERVER_PROCESS_OWNERSHIP_MISMATCH",
                            f"pid={pid}",
                        )
                    subprocess.run(
                        ["taskkill", "/PID", str(pid), "/T", "/F"],
                        capture_output=True, text=True, encoding="utf-8",
                        errors="replace", timeout=10, check=False,
                    )
                if server.poll() is None:
                    server.wait(timeout=5)
                if port and self._windows_listener_pids(port):
                    raise OpenCodeAdapterError("OPENCODE_SERVER_STOP_FAILED")
            elif server.poll() is None:
                server.terminate()
                try:
                    server.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    server.kill()
                    server.wait(timeout=5)
        finally:
            for stream in (server.stdout, server.stderr):
                if stream is not None:
                    stream.close()
