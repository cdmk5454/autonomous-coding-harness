"""Canonical verification depth and artifact-first evidence rules."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from runtime_adapter import ValidationDisposition, payload_sha256


DEPTHS = ("STATIC", "TARGETED", "E2E", "HUMAN_CONTROLLED_E2E")
AUTHORITY_ORDER = (
    "JOB_CONTRACT", "APPROVED_BUSINESS_DB_API_AUTH_CONTRACT", "ACCEPTANCE_CRITERIA",
    "APPROVED_DESIGN_UI_INTENT", "QA_RESOLUTION", "SOURCE_IMPLEMENTATION_EVIDENCE",
)


class VerificationContractError(RuntimeError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def derive_verification_contract(request: Mapping[str, Any], test_plan: Mapping[str, Any]) -> dict[str, Any]:
    declared = str(test_plan.get("verification_depth") or "").upper()
    if declared and declared not in DEPTHS:
        raise VerificationContractError("VERIFICATION_DEPTH_INVALID")
    depth = declared or (
        "HUMAN_CONTROLLED_E2E" if "HUMAN" in str(request.get("qa_type") or "").upper()
        else "TARGETED" if (test_plan.get("required_tests") or test_plan.get("command"))
        else "STATIC"
    )
    contract = {
        "schema_revision": "verification-contract/1",
        "depth": depth,
        "mandatory": bool(test_plan.get("mandatory", True)),
        "authority_order": list(AUTHORITY_ORDER),
        "source_is_requirement_authority": False,
        "review_risk_axis": "SEPARATE",
        "human_trace_readiness": depth == "HUMAN_CONTROLLED_E2E",
        "auto_e2e_generation": False,
    }
    contract["verification_contract_hash"] = payload_sha256(contract)
    return contract


def validate_evidence(
    *, contract: Mapping[str, Any], candidate_hash: str, contract_revision: int,
    environment_hash: str, result: str, artifacts: Sequence[Mapping[str, Any]],
    machine_observed: bool, supplied_candidate_hash: str,
    supplied_contract_revision: int, supplied_environment_hash: str,
) -> None:
    disposition = str(result).upper()
    allowed = {item.value for item in ValidationDisposition}
    if disposition not in allowed:
        raise VerificationContractError("VERIFICATION_RESULT_INVALID")
    if (str(supplied_candidate_hash) != str(candidate_hash)
            or int(supplied_contract_revision) != int(contract_revision)
            or str(supplied_environment_hash) != str(environment_hash)):
        raise VerificationContractError("VERIFICATION_EVIDENCE_BINDING_STALE")
    if disposition == ValidationDisposition.PASS.value:
        if not machine_observed or not artifacts or not any(
            isinstance(item, Mapping) and any(item.get(key) for key in ("path", "uri", "ref"))
            for item in artifacts
        ):
            raise VerificationContractError("VERIFICATION_SELF_REPORT_FORBIDDEN")
        depth = str(contract.get("depth") or "STATIC")
        observed_depths = {str(item.get("depth") or "STATIC").upper() for item in artifacts}
        required_index = DEPTHS.index(depth)
        if max((DEPTHS.index(item) for item in observed_depths if item in DEPTHS), default=-1) < required_index:
            raise VerificationContractError("MANDATORY_VERIFICATION_DEPTH_NOT_MET")
