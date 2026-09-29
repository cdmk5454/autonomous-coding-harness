"""Native skill/MCP discovery records and measured optional-tool decisions."""

from __future__ import annotations

import json
import getpass
import os
from pathlib import Path
import stat
import subprocess
import tempfile
from typing import Any, Mapping, Sequence

from runtime_adapter import payload_sha256


class McpCredentialError(RuntimeError):
    retry_domain = "TECHNICAL_EXECUTION_RETRY"
    product_retry_delta = 0

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def _harden_mcp_config_permissions(path: str) -> None:
    os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
    if os.name != "nt":
        return
    user = getpass.getuser()
    domain = os.environ.get("USERDOMAIN", "").strip()
    principal = f"{domain}\\{user}" if domain else user
    result = subprocess.run(
        ["icacls", path, "/inheritance:r", "/grant:r", f"{principal}:(F)",
         "*S-1-5-18:(F)", "*S-1-5-32-544:(F)"],
        capture_output=True, text=True, encoding="utf-8", errors="replace",
        timeout=15, check=False,
    )
    if result.returncode != 0:
        raise McpCredentialError("MCP_CREDENTIAL_PERMISSION_HARDENING_FAILED")


def migrate_mcp_secret_argv_config(
    config_path: str | Path,
    server_name: str,
    secret_flag: str,
    environment_name: str,
) -> dict[str, Any]:
    """Atomically move one native MCP credential from argv to server-scoped env."""
    path = Path(config_path).resolve()
    data = json.loads(path.read_text(encoding="utf-8"))
    servers = data.get("mcpServers")
    if not isinstance(servers, dict) or not isinstance(servers.get(server_name), dict):
        raise McpCredentialError("MCP_SERVER_CONFIG_MISSING")
    server = servers[server_name]
    args = list(server.get("args") or ())
    env = dict(server.get("env") or {})
    found: list[tuple[int, int, str]] = []
    prefix = secret_flag + "="
    for index, value in enumerate(args):
        text = str(value)
        if text == secret_flag:
            if index + 1 >= len(args) or not str(args[index + 1]):
                raise McpCredentialError("MCP_CREDENTIAL_MISSING")
            found.append((index, index + 2, str(args[index + 1])))
        elif text.startswith(prefix) and text[len(prefix):]:
            found.append((index, index + 1, text[len(prefix):]))
    if len(found) > 1:
        raise McpCredentialError("MCP_CREDENTIAL_AMBIGUOUS")
    existing = env.get(environment_name)
    if found and existing and existing != found[0][2]:
        raise McpCredentialError("MCP_CREDENTIAL_CONFLICT")
    secret = found[0][2] if found else existing
    if not isinstance(secret, str) or not secret:
        raise McpCredentialError("MCP_CREDENTIAL_MISSING")
    if found:
        start, stop, _ = found[0]
        server["args"] = args[:start] + args[stop:]
    env[environment_name] = secret
    server["env"] = env

    handle, temp_name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(handle, "w", encoding="utf-8", newline="\n") as stream:
            json.dump(data, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        _harden_mcp_config_permissions(temp_name)
        os.replace(temp_name, path)
    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)
    return {
        "server": server_name,
        "credential_transport": "ENVIRONMENT",
        "environment_name": environment_name,
        "secret_in_argv": False,
        "retry_domain": "TECHNICAL_EXECUTION_RETRY",
        "product_retry_delta": 0,
    }


def skill_capability(runtime: str, discovered: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    skills = sorted({str(item.get("name") or "") for item in discovered if item.get("name")})
    return {
        "runtime": str(runtime), "mandatory_context_loader": "HARNESS",
        "optional_discovery": "NATIVE", "discovered": skills,
        "optional_failure_blocks_mandatory_context": False,
        "duplicate_harness_skill_loader_candidate": bool(skills),
    }


def mcp_capability(runtime: str, servers: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    seen: set[str] = set()
    records = []
    for item in servers:
        name = str(item.get("name") or "")
        identity = str(item.get("identity") or name).casefold()
        if not name or not identity or identity in seen:
            raise ValueError("MCP_DUPLICATE_INJECTION_FORBIDDEN")
        seen.add(identity)
        record = {
            "name": name, "identity": identity,
            "availability": str(item.get("availability") or "UNKNOWN"),
            "version": str(item.get("version") or "UNKNOWN"),
            "capability": sorted(map(str, item.get("capability") or ())),
            "required": bool(item.get("required", False)),
            "health": str(item.get("health") or "UNKNOWN"),
            "loader": "NATIVE_ONLY",
        }
        record["failure_retry_domain"] = "TECHNICAL_EXECUTION_RETRY"
        record["product_retry_delta"] = 0
        records.append(record)
    result = {"runtime": str(runtime), "servers": records}
    result["catalog_hash"] = payload_sha256(result)
    return result


def lsp_decisions(measurements: Mapping[str, Mapping[str, Any]]) -> dict[str, str]:
    decisions = {}
    for module, result in sorted(measurements.items()):
        gain = result.get("diagnostic_gain")
        stable = result.get("synchronization") == "PASS"
        latency = result.get("latency_ms")
        memory = result.get("memory_mb")
        decisions[module] = "ON" if (
            isinstance(gain, (int, float)) and gain > 0 and stable
            and isinstance(latency, (int, float)) and isinstance(memory, (int, float))
        ) else "OFF"
    return decisions


def tooling_decisions(*, repeated_investigation_bottleneck: bool) -> dict[str, Any]:
    return {
        "SERENA": "BENCHMARK_RECOMMENDED" if repeated_investigation_bottleneck else "DEFERRED",
        "REPOMIX": "ADOPTED_OPTIONAL",
        "REPOMIX_USAGE": "ONBOARDING_EXPORT_CONTEXT_BUNDLE_ONLY",
        "REPOMIX_PER_ATTEMPT_REPOSITORY_INJECTION": False,
        "UI_UX_CAPABILITY": "ADOPTED_OPTIONAL",
        "UI_UX_RUNTIME_MANDATORY": False,
        "CLAUDE_MEM": "DEFERRED_TO_1_1",
    }
