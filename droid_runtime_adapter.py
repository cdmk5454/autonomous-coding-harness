"""Legacy Droid adapter retained as the pre-OpenCode-live fallback runtime."""

from __future__ import annotations

from pathlib import Path
import shutil
import subprocess
import uuid
from typing import Any, Mapping

from runtime_adapter import (
    ADAPTER_API_REVISION, CapabilitySupport, NormalizedRuntimeEvent,
    RuntimeCapabilities, RuntimeCapabilityError, RuntimeDescriptor, SessionQuery,
    SessionState, TurnInvocation, ValidationDisposition, normalize_usage,
)


class DroidRuntimeAdapter:
    runtime_name = "droid"

    def __init__(self, executable: str = "droid"):
        self.executable = executable

    def preflight(self) -> RuntimeDescriptor:
        executable = shutil.which(self.executable) or ""
        if not executable:
            return RuntimeDescriptor(
                runtime="droid", version="", executable="", schema_compatible=False,
                validation_disposition=ValidationDisposition.UNAVAILABLE.value,
                detail="DROID_EXECUTABLE_NOT_FOUND",
                capabilities=RuntimeCapabilities(False, False, False, False, False, False),
            )
        try:
            result = subprocess.run(
                [executable, "--version"], capture_output=True, text=True,
                encoding="utf-8", errors="replace", timeout=10, check=False,
            )
            exec_help = subprocess.run(
                [executable, "help", "exec"], capture_output=True, text=True,
                encoding="utf-8", errors="replace", timeout=10, check=False,
            )
            resume_help = subprocess.run(
                [executable, "resume", "--help"], capture_output=True, text=True,
                encoding="utf-8", errors="replace", timeout=10, check=False,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            return RuntimeDescriptor(
                runtime="droid", version="", executable=executable,
                schema_compatible=False,
                validation_disposition=ValidationDisposition.UNAVAILABLE.value,
                detail=type(exc).__name__,
                capabilities=RuntimeCapabilities(False, False, False, False, False, False),
            )
        help_text = exec_help.stdout + exec_help.stderr
        resume_text = resume_help.stdout + resume_help.stderr
        resume_supported = "--session-id" in help_text and "sessionId" in resume_text
        fork_supported = "--fork" in help_text and "--fork" in resume_text
        jsonrpc_supported = "stream-jsonrpc" in help_text
        structured_events = "--output-format" in help_text
        compatible = (
            result.returncode == exec_help.returncode == resume_help.returncode == 0
            and resume_supported and structured_events
        )
        supported = CapabilitySupport.SUPPORTED.value
        partial = CapabilitySupport.PARTIAL.value
        unsupported = CapabilitySupport.UNSUPPORTED.value
        return RuntimeDescriptor(
            runtime="droid", version=result.stdout.strip() or result.stderr.strip(),
            adapter_revision=ADAPTER_API_REVISION, executable=executable,
            capabilities=RuntimeCapabilities(
                durable_session=resume_supported,
                explicit_resume=resume_supported,
                session_query=False,
                terminal_query=False,
                reconnect=jsonrpc_supported,
                normalized_events=structured_events,
                interrupt=False,
                pending_interaction=jsonrpc_supported,
                native_queue=False,
                managed_remote=False,
            ),
            protocol_revision="droid-stream-jsonrpc/1" if jsonrpc_supported else "droid-stream-json/1",
            structured_output_schema_revision="droid-sdk-json-schema/1" if jsonrpc_supported else "UNSUPPORTED",
            permission_tool_profile="droid-autonomy-tools/1",
            native_queue_maturity="UNSUPPORTED",
            session_process_lifetime="PERSISTED_BEYOND_SUBMIT_PROCESS",
            sandbox_semantics="AUTONOMY_TOOL_POLICY_NOT_OS_SANDBOX",
            execution_environment="WINDOWS_NATIVE" if __import__("os").name == "nt" else "CURRENT_NATIVE",
            capability_contract={
                "session.create": supported,
                "session.resume": supported if resume_supported else unsupported,
                "session.read_query": unsupported,
                "session.fork": supported if fork_supported else unsupported,
                "turn.submit": supported,
                "turn.steer": partial if jsonrpc_supported else unsupported,
                "turn.interrupt": partial if jsonrpc_supported else unsupported,
                "events.subscribe": partial if jsonrpc_supported else unsupported,
                "events.reconnect": partial if jsonrpc_supported else unsupported,
                "events.replay_cursor": unsupported,
                "events.terminal_query": unsupported,
                "interaction.pending_approval": partial if jsonrpc_supported else unsupported,
                "interaction.pending_question": partial if jsonrpc_supported else unsupported,
                "interaction.permission_response": partial if jsonrpc_supported else unsupported,
                "interaction.response_delivery": supported if structured_events else unsupported,
                "output.structured": supported if structured_events else unsupported,
                "output.schema_revision": partial if jsonrpc_supported else unsupported,
                "runtime.permission_tool_semantics": supported,
                "runtime.usage_metadata": supported if jsonrpc_supported else partial,
                "runtime.native_queue": unsupported,
                "runtime.protocol_revision": supported,
            },
            schema_compatible=compatible, live_model_validated=False,
            validation_disposition=(ValidationDisposition.PASS.value if compatible else ValidationDisposition.FAIL.value),
            detail="NATIVE_CONTINUATION_CLI_SURFACE" if compatible else "DROID_CLI_SCHEMA_UNSUPPORTED",
        )

    def _base(self, *, cwd: str | Path, model: str, reasoning_effort: str) -> list[str]:
        command = [
            self.executable, "exec", "--cwd", str(Path(cwd).resolve()),
            "--output-format", "stream-json",
        ]
        if model:
            command.extend(["--model", model])
        if reasoning_effort in {"low", "medium", "high"}:
            command.extend(["--reasoning-effort", reasoning_effort])
        return command

    def create_session(self, prompt: str, *, cwd: str | Path, model: str = "",
                       reasoning_effort: str = "medium", command_id: str = "") -> TurnInvocation:
        command = self._base(cwd=cwd, model=model, reasoning_effort=reasoning_effort)
        return TurnInvocation(command_id=command_id or "CMD-" + uuid.uuid4().hex.upper(),
                              argv=tuple(command), stdin=prompt)

    def resume_session(self, session_id: str, prompt: str, *, cwd: str | Path,
                       model: str = "", reasoning_effort: str = "medium",
                       command_id: str = "") -> TurnInvocation:
        if not str(session_id).strip():
            raise ValueError("NATIVE_SESSION_ID_REQUIRED")
        command = self._base(cwd=cwd, model=model, reasoning_effort=reasoning_effort)
        command.extend(["--session-id", session_id])
        return TurnInvocation(
            command_id=command_id or "CMD-" + uuid.uuid4().hex.upper(),
            argv=tuple(command), stdin=prompt, session_id=session_id, resume=True,
        )

    def submit_turn(self, session_id: str, prompt: str, **kwargs) -> TurnInvocation:
        return self.resume_session(session_id, prompt, **kwargs)

    def fork_session(self, session_id: str, prompt: str, *, cwd: str | Path,
                     model: str = "", reasoning_effort: str = "medium",
                     command_id: str = "") -> TurnInvocation:
        if not str(session_id).strip():
            raise ValueError("NATIVE_SESSION_ID_REQUIRED")
        command = self._base(cwd=cwd, model=model, reasoning_effort=reasoning_effort)
        command.extend(["--fork", session_id])
        return TurnInvocation(
            command_id=command_id or "CMD-" + uuid.uuid4().hex.upper(),
            argv=tuple(command), stdin=prompt, session_id=session_id,
        )

    def normalize_event(self, event: Mapping[str, Any]) -> tuple[NormalizedRuntimeEvent, ...]:
        event_type = str(event.get("type") or "")
        terminal = event_type in {"result", "error"}
        subtype = str(event.get("subtype") or "")
        is_error = bool(event.get("is_error")) or event_type == "error" or subtype in {"error", "failure"}
        return (NormalizedRuntimeEvent(
            kind="TURN_COMPLETED" if event_type == "result" else "TURN_FAILED" if event_type == "error" else "EVENT",
            runtime="droid",
            session_id=str(event.get("session_id") or event.get("sessionId") or ""),
            turn_id=str(event.get("turn_id") or event.get("turnId") or event.get("message_id") or ""),
            native_event_id=str(event.get("event_id") or event.get("id") or ""),
            terminal=terminal,
            success=(not is_error) if event_type == "result" else False if event_type == "error" else None,
            result_text=str(event.get("result") or event.get("text") or ""),
            error_code="DROID_TURN_FAILED" if is_error else "",
            raw_type=event_type,
            usage=normalize_usage(
                event.get("usage") if isinstance(event.get("usage"), Mapping) else {}
            ),
        ),)

    def query_session(self, session_id: str, *, cwd: str | Path) -> SessionQuery:
        return SessionQuery(session_id, SessionState.UNKNOWN.value, detail="SESSION_QUERY_UNSUPPORTED")

    def query_terminal(self, session_id: str, turn_id: str = "", *, cwd: str | Path) -> SessionQuery:
        return self.query_session(session_id, cwd=cwd)

    def pending_approval_or_question(self, session_id: str, *, cwd: str | Path) -> SessionQuery:
        return self.query_session(session_id, cwd=cwd)

    def reconcile(self, session_id: str, *, cwd: str | Path) -> SessionQuery:
        return self.query_session(session_id, cwd=cwd)

    def interrupt(self, session_id: str, turn_id: str = "") -> bool:
        return False

    def steer_turn(self, session_id: str, turn_id: str, prompt: str) -> bool:
        raise RuntimeCapabilityError("DROID_STEER_REQUIRES_STREAM_JSONRPC_CLIENT")

    def subscribe_events(self, session_id: str, *, cursor: str = "") -> Any:
        if cursor:
            raise RuntimeCapabilityError("DROID_EVENT_REPLAY_UNSUPPORTED")
        raise RuntimeCapabilityError("DROID_SUBSCRIBE_REQUIRES_STREAM_JSONRPC_CLIENT")

    def replay_events(self, session_id: str, *, cursor: str) -> Any:
        raise RuntimeCapabilityError("DROID_EVENT_REPLAY_UNSUPPORTED")

    def respond_interaction(self, session_id: str, interaction_id: str,
                            response: Mapping[str, Any]) -> bool:
        raise RuntimeCapabilityError("DROID_PERMISSION_RESPONSE_REQUIRES_STREAM_JSONRPC_CLIENT")
