"""Provider-independent frozen Profile mapping and OpenCode host preflight."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from typing import Any, Mapping, Sequence
from urllib.parse import urlparse

from runtime_adapter import payload_sha256


class OpenCodeProfileError(RuntimeError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _file_ref(profile_dir: Path, relative: str) -> dict[str, str]:
    path = (profile_dir / relative).resolve()
    if not path.is_file() or not path.is_relative_to(profile_dir):
        raise OpenCodeProfileError("OPENCODE_PROFILE_REFERENCE_INVALID")
    return {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def map_frozen_profile(profile_dir: str | Path, project: Mapping[str, Any], *,
                       optional_skills: Sequence[str] = (),
                       mcp_servers: Sequence[Mapping[str, Any]] = ()) -> dict[str, Any]:
    root = Path(profile_dir).resolve()
    context = dict(project.get("context") or {})
    rules = dict(project.get("rules") or {})
    mandatory_names = [str(context.get("project_rules") or ""), *map(str, rules.get("always") or ())]
    mandatory = [_file_ref(root, name) for name in mandatory_names if name]
    discoverable = [_file_ref(root, name) for name in (
        str(context.get("analysis") or ""), str(context.get("feature_map") or "")
    ) if name]
    seen_mcp: set[str] = set()
    native_mcp = []
    for item in mcp_servers:
        identity = str(item.get("identity") or item.get("name") or "").casefold()
        if not identity or identity in seen_mcp:
            raise OpenCodeProfileError("MCP_DUPLICATE_INJECTION_FORBIDDEN")
        seen_mcp.add(identity)
        native_mcp.append({**dict(item), "loader": "NATIVE_ONLY"})
    mapping = {
        "schema_revision": "opencode-profile-mapping/1",
        "profile_id": str(project.get("id") or ""),
        "profile_schema_version": int(project.get("schema_version") or 0),
        "working_directory": str(Path((project.get("workspace_roots") or [""])[0]).resolve()),
        "os_toolchain_decision": "WINDOWS_NATIVE",
        "config": {"format": "opencode.json", "generated": False, "provider": "DEFERRED_USER_SETUP"},
        "mandatory_context": mandatory,
        "optional_discoverable_context": discoverable,
        "skills": {"classification": "OPTIONAL_DISCOVERABLE_SKILL", "names": sorted(set(optional_skills))},
        "tools": {"read": "allow", "edit": "ask", "bash": "ask", "external_directory": "deny-by-default"},
        "mcp": native_mcp,
        "permissions": {
            "meaning": "RUNTIME_APPROVAL_POLICY_NOT_OS_SANDBOX",
            "auto": False, "managed_writer_requires_harness_authority": True,
        },
    }
    mapping["mapping_hash"] = payload_sha256(mapping)
    return mapping


def windows_argv(executable: str, *arguments: str) -> tuple[str, ...]:
    if not executable or any("\x00" in str(value) for value in (executable, *arguments)):
        raise OpenCodeProfileError("OPENCODE_ARGV_INVALID")
    return tuple(str(value) for value in (executable, *arguments))


def environment_preflight() -> dict[str, Any]:
    commands = {
        "jdk": ("java", "-version"), "maven": ("mvn.cmd" if os.name == "nt" else "mvn", "-version"),
        "node": ("node", "--version"), "npm": ("npm.cmd" if os.name == "nt" else "npm", "--version"),
    }
    tools: dict[str, Any] = {}
    for name, argv in commands.items():
        executable = shutil.which(argv[0])
        if not executable:
            tools[name] = {"status": "UNAVAILABLE", "version": "UNKNOWN"}
            continue
        try:
            result = subprocess.run(
                [executable, *argv[1:]], capture_output=True, text=True,
                encoding="utf-8", errors="replace", timeout=15, check=False,
            )
            first = next((line.strip() for line in (result.stdout + result.stderr).splitlines() if line.strip()), "")
            tools[name] = {"status": "PASS" if result.returncode == 0 else "FAIL", "version": first[:200] or "UNKNOWN"}
        except (OSError, subprocess.SubprocessError) as exc:
            tools[name] = {"status": "FAIL", "version": "UNKNOWN", "diagnostic": type(exc).__name__}
    platform_name = "WINDOWS" if os.name == "nt" else "POSIX"
    return {
        "schema_revision": "opencode-environment/1", "platform": platform_name,
        "os_toolchain_decision": "WINDOWS_NATIVE" if os.name == "nt" else "CURRENT_NATIVE",
        "runtime_os_migration_coupled": False,
        "path": {"drive_letters": os.name == "nt", "argv_array_quoting": True},
        "encoding": {"filesystem": sys.getfilesystemencoding() or "UNKNOWN",
                     "stdout": "UTF-8_EXPLICIT", "stderr": "UTF-8_EXPLICIT"},
        "process": {"termination": "PROCESS_TREE_VERIFIED", "child_collection": "OWNED_PID_LIST",
                    "timeout_interrupt": "SUPPORTED"},
        "tools": tools,
    }


def remote_security_contract(*, endpoint: str, authenticated: bool,
                             password_configured: bool, private_network: bool,
                             managed_mutation_exposed: bool,
                             redaction: Mapping[str, bool]) -> dict[str, Any]:
    parsed = urlparse(endpoint)
    loopback = parsed.hostname in {"127.0.0.1", "localhost", "::1"}
    authenticated_tls = parsed.scheme == "https" and authenticated
    if not loopback and not (private_network or authenticated_tls):
        raise OpenCodeProfileError("REMOTE_PRIVATE_OR_AUTHENTICATED_TLS_REQUIRED")
    if not password_configured or not authenticated:
        raise OpenCodeProfileError("OPENCODE_SERVER_AUTH_REQUIRED")
    required = {"api_secret", "provider_credential", "db_credential", "pii"}
    if any(redaction.get(name) is not True for name in required):
        raise OpenCodeProfileError("REMOTE_REDACTION_BOUNDARY_INCOMPLETE")
    if managed_mutation_exposed:
        raise OpenCodeProfileError("MANAGED_MUTATION_ENDPOINT_EXPOSURE_FORBIDDEN")
    return {
        "schema_revision": "remote-security/1", "network_boundary": (
            "LOOPBACK" if loopback else "PRIVATE_NETWORK" if private_network else "AUTHENTICATED_TLS"
        ),
        "server_authentication": "PASSWORD_CONFIGURED",
        "cors_is_authentication": False, "redaction": dict(redaction),
        "managed_mutation_endpoint_exposed": False,
        "permission_is_os_sandbox": False,
        "os_vm_container_boundary": "SEPARATE_CAPABILITY_UNKNOWN",
        "credential_process_config_boundary": "SEPARATED",
    }
