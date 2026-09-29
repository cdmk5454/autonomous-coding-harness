"""Codex CLI implementation of the single-agent RuntimeAdapter contract."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
import uuid
from typing import Any, Mapping

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


class CodexRuntimeAdapter:
    runtime_name = "codex"

    def __init__(self, executable: str = "codex", *, sessions_root: str | Path | None = None):
        self.executable = executable
        self.sessions_root = Path(sessions_root) if sessions_root is not None else None

    @staticmethod
    def _command_id(value: str) -> str:
        return value or "CMD-" + uuid.uuid4().hex.upper()

    def preflight(self) -> RuntimeDescriptor:
        executable = shutil.which(self.executable) or ""
        if not executable:
            return RuntimeDescriptor(
                runtime=self.runtime_name,
                version="",
                executable="",
                schema_compatible=False,
                validation_disposition=ValidationDisposition.UNAVAILABLE.value,
                detail="CODEX_EXECUTABLE_NOT_FOUND",
                capabilities=RuntimeCapabilities(False, False, False, False, False, False),
            )
        try:
            version = subprocess.run(
                [executable, "--version"], capture_output=True, text=True,
                encoding="utf-8", errors="replace", timeout=10, check=False,
            )
            help_result = subprocess.run(
                [executable, "exec", "resume", "--help"], capture_output=True,
                text=True, encoding="utf-8", errors="replace", timeout=10, check=False,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            return RuntimeDescriptor(
                runtime=self.runtime_name, version="", executable=executable,
                schema_compatible=False,
                validation_disposition=ValidationDisposition.UNAVAILABLE.value,
                detail=type(exc).__name__,
                capabilities=RuntimeCapabilities(False, False, False, False, False, False),
            )
        help_text = help_result.stdout + help_result.stderr
        compatible = version.returncode == 0 and help_result.returncode == 0 and "SESSION_ID" in help_text
        supported = CapabilitySupport.SUPPORTED.value
        partial = CapabilitySupport.PARTIAL.value
        unsupported = CapabilitySupport.UNSUPPORTED.value
        return RuntimeDescriptor(
            runtime=self.runtime_name,
            version=version.stdout.strip() or version.stderr.strip(),
            adapter_revision=ADAPTER_API_REVISION,
            executable=executable,
            capabilities=RuntimeCapabilities(
                durable_session=True,
                explicit_resume=True,
                session_query=True,
                terminal_query=True,
                reconnect=True,
                normalized_events=True,
                interrupt=False,
                pending_interaction=True,
                native_queue=False,
                managed_remote=False,
            ),
            protocol_revision="codex-exec-jsonl/1",
            structured_output_schema_revision="codex-output-schema/1",
            permission_tool_profile="codex-managed-worker/1",
            native_queue_maturity="UNSUPPORTED",
            session_process_lifetime="PERSISTED_BEYOND_SUBMIT_PROCESS",
            sandbox_semantics="NATIVE_SANDBOX_AND_APPROVAL_PROFILE",
            execution_environment="WINDOWS_NATIVE" if os.name == "nt" else "CURRENT_NATIVE",
            capability_contract={
                "session.create": supported,
                "session.resume": supported,
                "session.read_query": partial,
                "session.fork": unsupported,
                "turn.submit": supported,
                "turn.steer": unsupported,
                "turn.interrupt": unsupported,
                "events.subscribe": partial,
                "events.reconnect": partial,
                "events.replay_cursor": unsupported,
                "events.terminal_query": partial,
                "interaction.pending_approval": partial,
                "interaction.pending_question": partial,
                "interaction.permission_response": unsupported,
                "interaction.response_delivery": supported,
                "output.structured": supported,
                "output.schema_revision": supported,
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
            detail="CLI_SCHEMA_PREFLIGHT" if compatible else "CODEX_CLI_SCHEMA_UNSUPPORTED",
        )

    def _base(self, cwd: str | Path, model: str, reasoning_effort: str) -> list[str]:
        command = [
            self.executable, "exec", "-C", str(Path(cwd).resolve()),
            "--approve-for-me", "--skip-git-repo-check", "--json",
        ]
        if model:
            command.extend(["-m", model])
        command.extend(["-c", f'model_reasoning_effort="{reasoning_effort}"'])
        command.extend(["-c", "project_doc_max_bytes=0"])
        return command

    def create_session(self, prompt: str, *, cwd: str | Path, model: str = "",
                       reasoning_effort: str = "medium", command_id: str = "") -> TurnInvocation:
        return TurnInvocation(
            command_id=self._command_id(command_id),
            argv=tuple(self._base(cwd, model, reasoning_effort)),
            stdin=prompt,
        )

    def resume_session(self, session_id: str, prompt: str, *, cwd: str | Path,
                       model: str = "", reasoning_effort: str = "medium",
                       command_id: str = "") -> TurnInvocation:
        if not str(session_id).strip():
            raise ValueError("NATIVE_SESSION_ID_REQUIRED")
        command = self._base(cwd, model, reasoning_effort)
        command.insert(2, "resume")
        # `resume` inherits the original cwd; -C is not accepted by the subcommand.
        index = command.index("-C")
        del command[index:index + 2]
        # Codex CLI 0.153.4 advertises this inherited flag in resume --help but
        # its resume parser rejects it. The original session retains approval
        # policy; omit the broken flag after schema preflight.
        if "--approve-for-me" in command:
            command.remove("--approve-for-me")
        command.append(session_id)
        command.append("-")
        return TurnInvocation(
            command_id=self._command_id(command_id), argv=tuple(command), stdin=prompt,
            session_id=session_id, resume=True,
        )

    def submit_turn(self, session_id: str, prompt: str, *, cwd: str | Path,
                    model: str = "", reasoning_effort: str = "medium",
                    command_id: str = "") -> TurnInvocation:
        return self.resume_session(
            session_id, prompt, cwd=cwd, model=model,
            reasoning_effort=reasoning_effort, command_id=command_id,
        )

    def fork_session(self, session_id: str, prompt: str, *, cwd: str | Path,
                     model: str = "", reasoning_effort: str = "medium",
                     command_id: str = "") -> TurnInvocation:
        raise RuntimeCapabilityError("CODEX_SESSION_FORK_UNSUPPORTED")

    def steer_turn(self, session_id: str, turn_id: str, prompt: str) -> bool:
        raise RuntimeCapabilityError("CODEX_TURN_STEER_UNSUPPORTED")

    def subscribe_events(self, session_id: str, *, cursor: str = "") -> Any:
        if cursor:
            raise RuntimeCapabilityError("CODEX_EVENT_REPLAY_UNSUPPORTED")
        raise RuntimeCapabilityError("CODEX_LIVE_SUBSCRIBE_UNSUPPORTED")

    def replay_events(self, session_id: str, *, cursor: str) -> Any:
        raise RuntimeCapabilityError("CODEX_EVENT_REPLAY_UNSUPPORTED")

    def respond_interaction(self, session_id: str, interaction_id: str,
                            response: Mapping[str, Any]) -> bool:
        raise RuntimeCapabilityError("CODEX_PERMISSION_RESPONSE_UNSUPPORTED")

    def normalize_event(self, event: Mapping[str, Any]) -> tuple[NormalizedRuntimeEvent, ...]:
        event_type = str(event.get("type") or "")
        session_id = str(event.get("thread_id") or event.get("threadId") or "")
        turn_id = str(event.get("turn_id") or event.get("turnId") or "")
        item = event.get("item") if isinstance(event.get("item"), Mapping) else {}
        native_event_id = str(event.get("id") or item.get("id") or "")
        text = ""
        if item.get("type") == "agent_message":
            text = str(item.get("text") or "")
        pending_kind = ""
        if "approval" in event_type:
            pending_kind = "TOOL_PERMISSION"
        elif "question" in event_type:
            pending_kind = "CLARIFICATION"
        terminal = event_type in {"turn.completed", "turn.failed", "error"}
        success = True if event_type == "turn.completed" else False if terminal else None
        error = str(event.get("error") or event.get("message") or "") if success is False else ""
        normalized = NormalizedRuntimeEvent(
            kind=(
                "SESSION_BOUND" if event_type == "thread.started" else
                "TURN_STARTED" if event_type == "turn.started" else
                "TURN_COMPLETED" if event_type == "turn.completed" else
                "TURN_FAILED" if event_type in {"turn.failed", "error"} else
                "PENDING_INPUT" if pending_kind else "EVENT"
            ),
            runtime=self.runtime_name,
            session_id=session_id,
            turn_id=turn_id,
            native_event_id=native_event_id,
            terminal=terminal,
            success=success,
            pending_kind=pending_kind,
            result_text=text,
            error_code="CODEX_TURN_FAILED" if success is False else "",
            raw_type=event_type,
            usage=normalize_usage(
                event.get("usage") if isinstance(event.get("usage"), Mapping) else {}
            ),
        )
        return (normalized,)

    def _session_file(self, session_id: str) -> Path | None:
        root = self.sessions_root
        if root is None:
            configured = os.environ.get("CODEX_HOME", "").strip()
            try:
                root = (Path(configured) if configured else Path.home() / ".codex") / "sessions"
            except RuntimeError:
                return None
        if not root.is_dir() or not session_id:
            return None
        suffix = session_id + ".jsonl"
        for path in root.rglob("*.jsonl"):
            if path.name.endswith(suffix):
                return path
        return None

    def query_session(self, session_id: str, *, cwd: str | Path) -> SessionQuery:
        path = self._session_file(session_id)
        if path is None:
            return SessionQuery(session_id, SessionState.LOST.value, detail="SESSION_FILE_NOT_FOUND")
        last_type = ""
        last_turn = ""
        pending = ""
        try:
            with path.open("r", encoding="utf-8", errors="replace") as handle:
                for line in handle:
                    try:
                        row = json.loads(line)
                    except ValueError:
                        continue
                    payload = row.get("payload") if isinstance(row.get("payload"), Mapping) else row
                    last_type = str(payload.get("type") or last_type)
                    last_turn = str(payload.get("turn_id") or payload.get("turnId") or last_turn)
                    lowered = last_type.casefold()
                    pending = "TOOL_PERMISSION" if "approval" in lowered else "CLARIFICATION" if "question" in lowered else pending
        except OSError:
            return SessionQuery(session_id, SessionState.UNKNOWN.value, detail="SESSION_FILE_UNREADABLE")
        state = SessionState.WAITING_INPUT.value if pending else SessionState.IDLE.value
        return SessionQuery(session_id, state, last_turn, pending, False, str(path))

    def query_terminal(self, session_id: str, turn_id: str = "", *, cwd: str | Path) -> SessionQuery:
        # Codex persists durable history but has no provider-free authoritative
        # remote terminal endpoint. An existing history is resumable, not terminal.
        return self.query_session(session_id, cwd=cwd)

    def interrupt(self, session_id: str, turn_id: str = "") -> bool:
        return False

    def pending_approval_or_question(self, session_id: str, *, cwd: str | Path) -> SessionQuery:
        return self.query_session(session_id, cwd=cwd)

    def reconcile(self, session_id: str, *, cwd: str | Path) -> SessionQuery:
        return self.query_session(session_id, cwd=cwd)
