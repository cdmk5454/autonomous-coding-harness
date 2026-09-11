"""Runtime-neutral single-agent session, turn, command, and recovery contracts.

The adapter boundary intentionally models a native runtime session separately
from the short-lived OS process used to submit or resume a turn.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import Enum
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping, Protocol, Sequence


ADAPTER_API_REVISION = "runtime-adapter/2"


class CapabilitySupport(str, Enum):
    SUPPORTED = "SUPPORTED"
    PARTIAL = "PARTIAL"
    UNSUPPORTED = "UNSUPPORTED"
    UNKNOWN = "UNKNOWN"


CAPABILITY_KEYS = (
    "session.create",
    "session.resume",
    "session.read_query",
    "session.fork",
    "turn.submit",
    "turn.steer",
    "turn.interrupt",
    "events.subscribe",
    "events.reconnect",
    "events.replay_cursor",
    "events.terminal_query",
    "interaction.pending_approval",
    "interaction.pending_question",
    "interaction.permission_response",
    "interaction.response_delivery",
    "output.structured",
    "output.schema_revision",
    "runtime.permission_tool_semantics",
    "runtime.usage_metadata",
    "runtime.native_queue",
    "runtime.protocol_revision",
)


class RuntimeCapabilityError(RuntimeError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def complete_capability_contract(
    values: Mapping[str, str] | None = None,
) -> dict[str, str]:
    supplied = dict(values or {})
    unknown = set(supplied) - set(CAPABILITY_KEYS)
    if unknown:
        raise ValueError("RUNTIME_CAPABILITY_KEY_INVALID")
    allowed = {item.value for item in CapabilitySupport}
    result = {
        key: str(supplied.get(key, CapabilitySupport.UNSUPPORTED.value))
        for key in CAPABILITY_KEYS
    }
    if any(value not in allowed for value in result.values()):
        raise ValueError("RUNTIME_CAPABILITY_SUPPORT_INVALID")
    return result


class RetryDomain(str, Enum):
    RUNTIME_RECOVERY = "RUNTIME_RECOVERY"
    TECHNICAL_EXECUTION_RETRY = "TECHNICAL_EXECUTION_RETRY"
    PRODUCT_SEMANTIC_RETRY = "PRODUCT_SEMANTIC_RETRY"


class ValidationDisposition(str, Enum):
    PASS = "PASS"
    FAIL = "FAIL"
    NOT_RUN_BY_POLICY = "NOT_RUN_BY_POLICY"
    NOT_RUN = "NOT_RUN"
    UNAVAILABLE = "UNAVAILABLE"


class CommandDelivery(str, Enum):
    PREPARED = "PREPARED"
    SENT = "SENT"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    COMPLETED = "COMPLETED"
    FAILED_BEFORE_SEND = "FAILED_BEFORE_SEND"
    UNKNOWN = "UNKNOWN"


class SessionState(str, Enum):
    CREATED = "CREATED"
    ACTIVE = "ACTIVE"
    IDLE = "IDLE"
    WAITING_INPUT = "WAITING_INPUT"
    TERMINAL = "TERMINAL"
    LOST = "LOST"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class RuntimeCapabilities:
    durable_session: bool
    explicit_resume: bool
    session_query: bool
    terminal_query: bool
    reconnect: bool
    normalized_events: bool
    interrupt: bool = False
    pending_interaction: bool = False
    native_queue: bool = False
    managed_remote: bool = False


@dataclass(frozen=True)
class RuntimeDescriptor:
    runtime: str
    version: str
    adapter_revision: str = ADAPTER_API_REVISION
    executable: str = ""
    capabilities: RuntimeCapabilities = field(
        default_factory=lambda: RuntimeCapabilities(False, False, False, False, False, False)
    )
    protocol_revision: str = "UNSUPPORTED"
    structured_output_schema_revision: str = "UNSUPPORTED"
    permission_tool_profile: str = "UNSPECIFIED"
    native_queue_maturity: str = "UNSUPPORTED"
    session_process_lifetime: str = "UNKNOWN"
    sandbox_semantics: str = "UNSPECIFIED"
    execution_environment: str = "CURRENT_NATIVE"
    capability_contract: Mapping[str, str] = field(default_factory=dict)
    schema_compatible: bool = True
    live_model_validated: bool = False
    validation_disposition: str = ValidationDisposition.NOT_RUN.value
    detail: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "capability_contract",
            complete_capability_contract(self.capability_contract),
        )

    def contract_snapshot(self, *, permission_tool_profile: str = "") -> dict[str, Any]:
        capability_snapshot = dict(self.capability_contract)
        capability_hash = payload_sha256(capability_snapshot)
        contract = {
            "runtime": self.runtime,
            "runtime_version": self.version,
            "adapter_revision": self.adapter_revision,
            "protocol_revision": self.protocol_revision,
            "capability_snapshot": capability_snapshot,
            "capability_snapshot_hash": capability_hash,
            "structured_output_schema_revision": self.structured_output_schema_revision,
            "permission_tool_profile": (
                str(permission_tool_profile) or self.permission_tool_profile
            ),
            "native_queue_maturity": self.native_queue_maturity,
            "session_process_lifetime": self.session_process_lifetime,
            "sandbox_semantics": self.sandbox_semantics,
            "execution_environment": self.execution_environment,
        }
        contract["runtime_contract_hash"] = payload_sha256(contract)
        return contract

    def public(self) -> dict[str, Any]:
        return {**asdict(self), **self.contract_snapshot()}


@dataclass(frozen=True)
class NormalizedRuntimeEvent:
    kind: str
    runtime: str
    session_id: str = ""
    turn_id: str = ""
    native_event_id: str = ""
    terminal: bool = False
    success: bool | None = None
    pending_kind: str = ""
    result_text: str = ""
    error_code: str = ""
    raw_type: str = ""
    usage: Mapping[str, int | str] = field(default_factory=dict)


@dataclass(frozen=True)
class SessionQuery:
    session_id: str
    state: str
    current_turn_id: str = ""
    pending_kind: str = ""
    terminal: bool = False
    detail: str = ""


@dataclass(frozen=True)
class TurnInvocation:
    command_id: str
    argv: tuple[str, ...] = ()
    stdin: str | None = None
    session_id: str = ""
    resume: bool = False
    transport: str = "PROCESS"
    method: str = ""
    url: str = ""
    body: Mapping[str, Any] = field(default_factory=dict)


def payload_sha256(value: Any) -> str:
    raw = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def normalize_usage(value: Mapping[str, Any] | None) -> dict[str, int | str]:
    source = dict(value or {})
    aliases = {
        "input_tokens": ("input_tokens", "inputTokens", "prompt_tokens", "promptTokens", "input"),
        "cached_input_tokens": ("cached_input_tokens", "cachedInputTokens", "cache_read_input_tokens", "cacheRead", "cached"),
        "output_tokens": ("output_tokens", "outputTokens", "completion_tokens", "completionTokens", "output"),
        "context_size": ("context_size", "contextSize", "context_tokens", "contextTokens"),
    }
    result: dict[str, int | str] = {}
    for target, keys in aliases.items():
        found = next((source[key] for key in keys if isinstance(source.get(key), int)), None)
        result[target] = int(found) if found is not None else "NOT_CAPTURED"
    return result


class RuntimeAdapter(Protocol):
    runtime_name: str

    def preflight(self) -> RuntimeDescriptor: ...

    def create_session(self, prompt: str, *, cwd: str | Path, model: str = "",
                       reasoning_effort: str = "medium", command_id: str = "") -> TurnInvocation: ...

    def resume_session(self, session_id: str, prompt: str, *, cwd: str | Path,
                       model: str = "", reasoning_effort: str = "medium",
                       command_id: str = "") -> TurnInvocation: ...

    def submit_turn(self, session_id: str, prompt: str, *, cwd: str | Path,
                    model: str = "", reasoning_effort: str = "medium",
                    command_id: str = "") -> TurnInvocation: ...

    def fork_session(self, session_id: str, prompt: str, *, cwd: str | Path,
                     model: str = "", reasoning_effort: str = "medium",
                     command_id: str = "") -> TurnInvocation: ...

    def steer_turn(self, session_id: str, turn_id: str, prompt: str) -> bool: ...

    def subscribe_events(self, session_id: str, *, cursor: str = "") -> Any: ...

    def replay_events(self, session_id: str, *, cursor: str) -> Any: ...

    def respond_interaction(self, session_id: str, interaction_id: str,
                            response: Mapping[str, Any]) -> bool: ...

    def normalize_event(self, event: Mapping[str, Any]) -> tuple[NormalizedRuntimeEvent, ...]: ...

    def query_session(self, session_id: str, *, cwd: str | Path) -> SessionQuery: ...

    def query_terminal(self, session_id: str, turn_id: str = "", *, cwd: str | Path) -> SessionQuery: ...

    def interrupt(self, session_id: str, turn_id: str = "") -> bool: ...

    def pending_approval_or_question(self, session_id: str, *, cwd: str | Path) -> SessionQuery: ...

    def reconcile(self, session_id: str, *, cwd: str | Path) -> SessionQuery: ...


def normalize_json_lines(lines: str, adapter: RuntimeAdapter) -> list[NormalizedRuntimeEvent]:
    events: list[NormalizedRuntimeEvent] = []
    for line in str(lines or "").splitlines():
        try:
            payload = json.loads(line)
        except (TypeError, ValueError):
            continue
        if isinstance(payload, Mapping):
            events.extend(adapter.normalize_event(payload))
    return events


def last_session_and_turn(events: Iterable[NormalizedRuntimeEvent]) -> tuple[str, str]:
    session_id = ""
    turn_id = ""
    for event in events:
        session_id = event.session_id or session_id
        turn_id = event.turn_id or turn_id
    return session_id, turn_id
