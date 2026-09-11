"""Explicit OpenCode migration/fallback and PERSONAL/MANAGED boundaries."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import os
from pathlib import Path
from typing import Mapping
from urllib.parse import urlparse


class RuntimeSelectionError(RuntimeError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def assert_single_migration_axis(*, previous_runtime: str, next_runtime: str,
                                 previous_model_tier: str, next_model_tier: str) -> str:
    runtime_changed = str(previous_runtime).casefold() != str(next_runtime).casefold()
    model_changed = str(previous_model_tier).casefold() != str(next_model_tier).casefold()
    if runtime_changed and model_changed:
        raise RuntimeSelectionError("RUNTIME_AND_MODEL_MIGRATION_COUPLED")
    return "RUNTIME" if runtime_changed else "MODEL" if model_changed else "NONE"


@dataclass(frozen=True)
class RuntimeSelection:
    requested_runtime: str
    selected_runtime: str
    fallback_runtime: str
    selection_reason: str
    opencode_live_validated: bool
    provider_setup: str
    model: str

    def public(self):
        return asdict(self)


def select_runtime(requested_runtime: str, requested_model: str = "",
                   env: Mapping[str, str] | None = None) -> RuntimeSelection:
    values = env if env is not None else os.environ
    requested = str(requested_runtime or "codex").casefold()
    if requested not in {"codex", "droid", "opencode"}:
        raise RuntimeSelectionError("RUNTIME_NOT_REGISTERED")
    live = str(values.get("KKM_OPENCODE_LIVE_VALIDATED", "0")) == "1"
    fallback = str(values.get("KKM_OPENCODE_FALLBACK_RUNTIME", "droid")).casefold()
    if fallback not in {"droid", "codex", "none"}:
        raise RuntimeSelectionError("OPENCODE_FALLBACK_INVALID")
    if requested != "opencode":
        return RuntimeSelection(
            requested, requested, fallback, "REQUESTED_RUNTIME", live,
            "READY" if live else "DEFERRED_USER_SETUP", requested_model,
        )
    model = requested_model or str(values.get("KKM_OPENCODE_MODEL", "")).strip()
    if live and model:
        return RuntimeSelection(
            requested, requested, fallback, "OPENCODE_LIVE_VALIDATED", True, "READY", model,
        )
    # An explicit OpenCode Job is not silently executed by another writer. The
    # configured fallback is advertised for recovery/migration but activation
    # remains a separate controller decision.
    return RuntimeSelection(
        requested, requested, fallback, "OPENCODE_PROVIDER_SETUP_DEFERRED",
        False, "DEFERRED_USER_SETUP", model,
    )


@dataclass(frozen=True)
class RuntimeBoundary:
    mode: str
    controller: str
    product_writer: bool
    native_ui_mutation: bool
    remote_url: str = ""
    authenticated: bool = False

    @classmethod
    def personal(cls, remote_url: str = "") -> "RuntimeBoundary":
        return cls("PERSONAL", "USER", False, True, remote_url, False)

    @classmethod
    def managed(cls, remote_url: str = "", *, authenticated: bool = False) -> "RuntimeBoundary":
        if remote_url:
            parsed = urlparse(remote_url)
            loopback = parsed.hostname in {"127.0.0.1", "localhost", "::1"}
            if not loopback and parsed.scheme != "https":
                raise RuntimeSelectionError("MANAGED_REMOTE_TLS_REQUIRED")
            if not authenticated:
                raise RuntimeSelectionError("MANAGED_REMOTE_AUTH_REQUIRED")
        return cls("MANAGED", "HARNESS", True, False, remote_url, authenticated)

    def assert_operation(self, operation: str) -> None:
        if self.mode == "PERSONAL" and operation == "PRODUCT_WRITE":
            raise RuntimeSelectionError("PERSONAL_PRODUCT_WRITER_FORBIDDEN")
        if self.mode == "MANAGED" and operation == "NATIVE_UI_MUTATION":
            raise RuntimeSelectionError("MANAGED_NATIVE_UI_MUTATION_FORBIDDEN")
