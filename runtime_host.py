"""RuntimeHost identity; intentionally distinct from process, session, and workspace."""

from __future__ import annotations

import os
import platform
from typing import Any, Mapping

from runtime_adapter import payload_sha256


def runtime_host_manifest(
    *, workspace_identity: str, runtime_contracts: Mapping[str, Any],
    system: str | None = None, machine: str | None = None,
    release: str | None = None, wsl: bool | None = None,
) -> dict[str, Any]:
    detected_system = str(system or platform.system())
    detected_wsl = bool(os.environ.get("WSL_INTEROP")) if wsl is None else bool(wsl)
    environment = "WSL" if detected_wsl else "WINDOWS" if detected_system.casefold() == "windows" else detected_system.upper()
    identity = {
        "schema_revision": "runtime-host/1",
        "platform": detected_system,
        "environment": environment,
        "machine": str(machine or platform.machine()),
        "release": str(release or platform.release()),
        "workspace_identity": str(workspace_identity),
        "runtime_contract_hash": str(runtime_contracts.get("runtime_contract_hash") or ""),
    }
    identity["runtime_host_id"] = "HOST-" + payload_sha256(identity)[:24].upper()
    return identity
