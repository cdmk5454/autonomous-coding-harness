"""Freeze and compare the small provider-independent runtime execution surface."""

from __future__ import annotations

from functools import lru_cache
from typing import Any, Mapping

from runtime_adapter import RuntimeDescriptor, payload_sha256


PERMISSION_TOOL_PROFILES = {
    ("deterministic-tester", "TESTER"): "FROZEN_TEST_COMMANDS_ONLY",
    ("hermes", "WORKER"): "hermes-candidate-rw-context-ro/1",
    ("hermes", "TESTER"): "hermes-tester-source-candidate-context-ro-scratch-rw/1",
    ("hermes", "REVIEWER"): "hermes-source-candidate-context-ro-scratch-rw/1",
    ("codex", "WORKER"): "codex-managed-workspace-write/1",
    ("codex", "REVIEWER"): "codex-read-only-review/1",
    ("droid", "WORKER"): "droid-managed-auto-policy/1",
    ("opencode", "WORKER"): "opencode-managed-product-writer/1",
    ("pi", "WORKER"): "pi-candidate-rw-context-ro-docker/1",
}


class RuntimeContractError(RuntimeError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def permission_tool_profile(runtime: str, role: str) -> str:
    key = (str(runtime).casefold(), str(role).upper())
    return PERMISSION_TOOL_PROFILES.get(key, f"{key[0]}-{key[1].casefold()}-unsupported/1")


@lru_cache(maxsize=3)
def descriptor_for(runtime: str) -> RuntimeDescriptor:
    name = str(runtime).casefold()
    if name == "hermes":
        from hermes_runtime_adapter import HermesRuntimeAdapter
        return HermesRuntimeAdapter.catalog_descriptor()
    if name == "codex":
        from codex_runtime_adapter import CodexRuntimeAdapter
        return CodexRuntimeAdapter().preflight()
    if name == "droid":
        from droid_runtime_adapter import DroidRuntimeAdapter
        return DroidRuntimeAdapter().preflight()
    if name == "opencode":
        from opencode_runtime_adapter import OpenCodeRuntimeAdapter
        return OpenCodeRuntimeAdapter().preflight()
    if name == "pi":
        from pi_process_boundary import PI_VERSION
        from runtime_adapter import RuntimeCapabilities
        return RuntimeDescriptor(runtime='pi',version=PI_VERSION,
            capabilities=RuntimeCapabilities(False,False,True,False,False,True,interrupt=True),
            protocol_revision='pi-rpc-jsonl-single-attempt/1',
            structured_output_schema_revision='harness11-pi-observation/1',
            permission_tool_profile=permission_tool_profile('pi','WORKER'),
            session_process_lifetime='SINGLE_ATTEMPT_PROCESS_NO_REUSE',
            sandbox_semantics='DOCKER_ENFORCED', execution_environment='PINNED_DOCKER_CANDIDATE',
            capability_contract={'session.create':'PARTIAL','session.read_query':'PARTIAL',
                'turn.submit':'PARTIAL','turn.interrupt':'PARTIAL','events.subscribe':'PARTIAL',
                'output.structured':'PARTIAL','output.schema_revision':'SUPPORTED',
                'runtime.protocol_revision':'PARTIAL','runtime.permission_tool_semantics':'PARTIAL'},
            live_model_validated=False,validation_disposition='NOT_RUN',
            detail='PI_CANDIDATE_HOST_BOUNDARY_REQUIRED')
    raise RuntimeContractError("RUNTIME_NOT_REGISTERED")


def role_contract(descriptor: RuntimeDescriptor, role: str) -> dict[str, Any]:
    return descriptor.contract_snapshot(
        permission_tool_profile=(descriptor.permission_tool_profile if descriptor.sandbox_semantics == "DOCKER_ENFORCED"
                                 else permission_tool_profile(descriptor.runtime, role))
    )


def freeze_runtime_contracts(
    *, worker: RuntimeDescriptor, reviewer: RuntimeDescriptor, tester: RuntimeDescriptor | None = None,
) -> dict[str, Any]:
    contracts = {
        "WORKER": role_contract(worker, "WORKER"),
        "REVIEWER": role_contract(reviewer, "REVIEWER"),
    }
    if tester is not None:
        contracts["TESTER"] = role_contract(tester, "TESTER")
    return {
        "schema_revision": "runtime-contracts/1",
        "roles": contracts,
        "runtime_contract_hash": payload_sha256(contracts),
    }


def assert_role_contract_compatible(
    frozen: Mapping[str, Any] | None,
    descriptor: RuntimeDescriptor,
    role: str,
) -> dict[str, Any]:
    role = str(role).upper()
    expected = dict(dict(frozen or {}).get("roles") or {}).get(role)
    current = role_contract(descriptor, role)
    if not expected:
        return current
    if expected.get("runtime_contract_hash") != current.get("runtime_contract_hash"):
        raise RuntimeContractError("RUNTIME_CONTRACT_DRIFT")
    return current
