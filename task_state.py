"""
TaskState - 작업 상태를 JSON으로 저장
"""

from __future__ import annotations

import fnmatch
import json
import re
from dataclasses import dataclass, asdict, field
from datetime import datetime
from pathlib import Path
from typing import Any, TYPE_CHECKING, Sequence

from runtime_safety import scrub_secrets
from control_paths import (
    ControlPathError,
    atomic_state_write,
    ensure_safe_state_directory,
    ensure_safe_state_file,
    ensure_safe_state_root,
)

if TYPE_CHECKING:
    from project_profile import ProjectProfile

# ===== 작업 모드 (검증 게이트 기준) =====
# MODIFICATION : 코드 변경 작업. build=PASS + REVIEW_PASS 필수.
# ANALYSIS     : 분석 전용 작업. 빈 diff 허용, build/review 완화.
TASK_MODE_MODIFICATION = "MODIFICATION"
TASK_MODE_ANALYSIS = "ANALYSIS"
TASK_MODES = (TASK_MODE_MODIFICATION, TASK_MODE_ANALYSIS)

# Worker가 "현재 상태에서 이미 충족"을 주장할 때만 사용하는 구조화 표식.
# 이 문구만으로 성공하지 않으며 Reviewer의 현재 파일 검증 PASS가 추가로 필요하다.
ALREADY_SATISFIED_MARKER = "NO_CHANGE_REASON: ALREADY_SATISFIED"

def extract_requirement_paths(
    requirement: str,
    module_names: Sequence[str] | None = None,
) -> list[str]:
    """요구사항의 명시적 profile module 상대 경로를 순서대로 추출한다."""
    names = [name for name in (module_names or []) if name]
    module_pattern = (
        "|".join(re.escape(name) for name in sorted(names, key=len, reverse=True))
        if names else r"[A-Za-z][A-Za-z0-9_-]*"
    )
    path_re = re.compile(
        rf"(?<![A-Za-z0-9_:])(?P<path>(?P<module>{module_pattern})[\\/][^\s`\"'<>|]+)",
        re.IGNORECASE,
    )
    paths: list[str] = []
    for match in path_re.finditer(requirement or ""):
        path = match.group("path").replace("\\", "/").rstrip(".,;:!?)]}")
        module = match.group("module").lower()
        path = module + path[len(match.group("module")):]
        if path not in paths:
            paths.append(path)
    return paths


def extract_requirement_modules(
    requirement: str,
    module_names: Sequence[str] | None = None,
) -> list[str]:
    """명시 파일 경로가 가리키는 모듈을 표준 모듈 순서로 반환한다."""
    paths = extract_requirement_paths(requirement, module_names)
    found = {path.split("/", 1)[0].casefold() for path in paths}
    order = list(module_names or dict.fromkeys(path.split("/", 1)[0] for path in paths))
    return [module for module in order if module.casefold() in found]


def claims_already_satisfied(worker_stdout: str) -> bool:
    """Worker 최종 응답에 정확한 구조화 no-op 표식이 있는지 확인한다."""
    return any(
        line.strip() == ALREADY_SATISFIED_MARKER
        for line in (worker_stdout or "").splitlines()
    )


def can_verify_current_state(
    requirement: str,
    worker_stdout: str = "",
    module_names: Sequence[str] | None = None,
) -> bool:
    """명시 파일 경로나 구조화 표식이 있으면 빈 task delta를 독립 검증할 수 있다."""
    return claims_already_satisfied(worker_stdout) or bool(
        extract_requirement_paths(requirement, module_names)
    )

# ===== 리뷰 상태 =====
# REVIEW_PASS       : 리뷰 통과
# REVIEW_FAIL       : 리뷰 실패 (규칙 위반 / 리뷰어 판정 FAIL)
# REVIEW_UNAVAILABLE: 리뷰어 실행 불가 (미설치/타임아웃/실행 실패)
# REVIEW_ERROR      : 리뷰어 응답 파싱 실패 (형식 오류)
REVIEW_STATUS_PASS = "REVIEW_PASS"
REVIEW_STATUS_FAIL = "REVIEW_FAIL"
REVIEW_STATUS_UNAVAILABLE = "REVIEW_UNAVAILABLE"
REVIEW_STATUS_ERROR = "REVIEW_ERROR"

# ===== 리뷰 리스크 (REVIEW_FAIL 재시도 전략) =====
# LOW : 국소 지적(표시/검증/문구/국소 로직) — 워킹트리 유지, 부분 수정만
# MID : 구조·범위가 요구사항 대비 과다 — 워킹트리 유지, 같은 요구사항 전면 재작성
# HIGH: 하드 제약 위반 등 — 안전 롤백 후 처음부터 재시도 (기존 동작)
# REVIEW_PASS / REVIEW_UNAVAILABLE / REVIEW_ERROR 에서는 등급 미사용("")
REVIEW_RISK_LOW = "LOW"
REVIEW_RISK_MID = "MID"
REVIEW_RISK_HIGH = "HIGH"
REVIEW_RISKS = (REVIEW_RISK_LOW, REVIEW_RISK_MID, REVIEW_RISK_HIGH)

# ===== 재시도 범위 (retry_scope) =====
RETRY_SCOPE_PARTIAL = "partial"   # LOW: 부분 수정만
RETRY_SCOPE_FULL = "full"         # MID: 전면 재작성 (트리 유지)
RETRY_SCOPE_RESTART = "restart"   # HIGH: 롤백 후 재시작
RETRY_SCOPES = (RETRY_SCOPE_PARTIAL, RETRY_SCOPE_FULL, RETRY_SCOPE_RESTART)

# ===== Codex reasoning effort 동적 승격 =====
REASONING_EFFORTS = ("low", "medium", "high")


def _reasoning_baseline(value: str) -> str:
    """사용자/환경 baseline을 보존하되 비정상값은 medium으로 보정한다."""
    return value if value in REASONING_EFFORTS else "medium"


def resolve_worker_reasoning_effort(
    task: "TaskState", attempt: int
) -> tuple[str, str]:
    """현재 attempt의 Codex Worker effort와 승격 사유를 결정한다.

    사용자 baseline은 낮추지 않는다. baseline이 high이면 동적 승격으로
    기록하지 않고 그대로 high를 사용한다.
    """
    baseline = _reasoning_baseline(task.codex_reasoning_effort)
    if baseline == "high":
        return "high", ""

    previous_risk = (task.review_risk or "").upper()
    if previous_risk == REVIEW_RISK_HIGH:
        return "high", "previous_review_high"
    if attempt >= 3:
        return "high", "attempt_three_or_later"
    if attempt >= 2 and previous_risk == REVIEW_RISK_MID:
        return "high", "previous_review_mid_retry"
    if (task.criticality or "").upper() == "CRITICAL":
        return "high", "critical_task"
    return baseline, ""


def resolve_reviewer_reasoning_effort(
    task: "TaskState",
    attempt: int,
    max_attempts: int,
    baseline: str = "medium",
) -> tuple[str, str]:
    """Choose Reviewer effort before invocation from already-known task state."""
    effective = _reasoning_baseline(baseline)
    if effective == "high":
        return "high", ""
    previous_risk = (task.review_risk or "").upper()
    if previous_risk == REVIEW_RISK_HIGH:
        return "high", "previous_review_high"
    if attempt >= 2 and previous_risk == REVIEW_RISK_MID:
        return "high", "previous_review_mid_retry"
    if (task.criticality or "").upper() in ("HIGH", "CRITICAL"):
        return "high", "critical_task"
    if max_attempts > 0 and attempt >= max_attempts:
        return "high", "final_attempt"
    return effective, ""

# mode 미지정 시 보조 추정용 키워드 (task_mode 가 명시되면 사용되지 않음)
CODE_CHANGE_KEYWORDS = [
    "수정", "변경", "추가", "구현", "생성", "삭제", "제거", "고쳐",
    "만들어", "작성", "적용", "수정해", "변경해", "추가해", "구현해",
    "생성해", "삭제해", "제거해", "만들어줘", "작성해", "적용해",
    "이관", "이동", "이름 변경", "리팩터", "리팩토링",
]

# 분석 전용 키워드 (우선순위: 코드 변경 키워드보다 앞선다.
# 예: "수정하지 말고 분석만" → ANALYSIS)
ANALYSIS_ONLY_KEYWORDS = [
    "수정하지 말",
    "파일을 수정하지 말",
    "분석 결과만",
    "보고만 해",
    "변경하지 말",
    "연결 테스트",
    "연결 확인",
    "연결 점검",
    "테스트만",
    "테스트 해줘",
    "대답만",
    "답변만",
    "설명만",
    "조회만",
    "분석만",
    "분석해줘",
    "분석 해줘",
    "분석해라",
    "분석해",
    "분석하",
    "조사해",
    "조사해줘",
    "검토만",
    "검토해줘",
    "점검만",
    "점검해줘",
    "구조 파악",
    "구조만",
    "읽기만",
]


def resolve_task_mode(requirement: str, explicit: str = "") -> str:
    """
    작업 모드 확정.
    - explicit 이 MODIFICATION/ANALYSIS 이면 그것을 우선 (키워드 무시).
    - 미지정("")이면 requirement 키워드로 추정:
        분석 전용 키워드 → ANALYSIS (우선)
        코드 변경 키워드 → MODIFICATION
        어느 쪽도 없으면 → ANALYSIS (기본: 코드 변경 요구 없음)
    """
    exp = (explicit or "").strip().upper()
    if exp in TASK_MODES:
        return exp
    text = requirement or ""
    if any(k in text for k in ANALYSIS_ONLY_KEYWORDS):
        return TASK_MODE_ANALYSIS
    if any(k in text for k in CODE_CHANGE_KEYWORDS):
        return TASK_MODE_MODIFICATION
    return TASK_MODE_ANALYSIS


# ===== 로컬 개발 설정 파일 제외 목록 (게이트/리뷰/롤백 공용 단일 소스) =====
# 개발자가 로컬 개발환경 세팅용으로 직접 수정하는 파일.
# AI 작업 검증(성공 게이트/리뷰)과 롤백에서 "의미 있는 변경"으로 보지 않는다.
# - .env / .env.local / .env.localhost / .env.development* : 로컬 환경변수
# 주의: .env.production 은 운영 비밀 포함 가능성이 있어 제외하지 않는다(리뷰 대상 유지).
# 매칭은 파일명 또는 profile 상대경로 기준으로 적용한다.
def is_local_dev_config(
    path: str,
    profile: "ProjectProfile | None" = None,
    excludes: Sequence[str] | None = None,
) -> bool:
    """경로가 로컬 개발 설정 파일(제외 대상)인지 판정. 파일명 기준 매칭."""
    if not path:
        return False
    if profile is not None:
        return profile.is_local_dev_config(path)
    patterns = tuple(excludes or ())
    normalized = path.replace("\\", "/").rstrip("/")
    name = normalized.rsplit("/", 1)[-1]
    return any(fnmatch.fnmatch(name, pat) or fnmatch.fnmatch(normalized, pat) for pat in patterns)


def split_local_dev_configs(
    files: list[str] | None,
    profile: "ProjectProfile | None" = None,
    excludes: Sequence[str] | None = None,
) -> tuple[list[str], list[str]]:
    """변경 파일 목록을 (게이트/리뷰용 목록, 제외된 로컬 개발 설정 목록)으로 분리."""
    kept: list[str] = []
    excluded: list[str] = []
    for f in files or []:
        (excluded if is_local_dev_config(f, profile, excludes) else kept).append(f)
    return kept, excluded


# ===== 프론트엔드 UI 모듈 판별 =====
# profile이 없는 레거시 호출의 경로 분류 보조 함수다. Build gate는 이 결과로
# SKIPPED를 성공으로 승격하지 않으며, 실제 BuildTestResult.success를 사용한다.
def is_frontend_only_changes(
    files: list[str] | None,
    profile: "ProjectProfile | None" = None,
) -> bool:
    """변경 파일이 전부 profile frontend 모듈 아래인지 판별.
    - 빈 목록 → False (변경 없음은 다른 게이트에서 처리)
    - 프리픽스 없는 경로(모듈 미지정 수집 등) 포함 시 False (fail-closed:
      백엔드/불명확 변경은 build SKIPPED 로 성공 불가)
    """
    if profile is not None:
        return profile.is_frontend_only(files)
    normalized = [path.replace("\\", "/").casefold() for path in (files or [])]
    return bool(normalized) and all(
        "/" in path
        and path.split("/", 1)[0].endswith("ui")
        and path.endswith((".vue", ".js", ".jsx", ".ts", ".tsx"))
        for path in normalized
    )


@dataclass
class TaskState:
    task_id: str
    requirement: str
    status: str = "PENDING"          # PENDING | RUNNING | SUCCESS | FAILED | AWAITING_QA
    stage: str = "INIT"              # INIT | WORKING | REVIEW | RETRY | AWAITING_QA | DONE
    worker: str = "droid"
    droid_model: str = ""                 # droid 워커용 모델 (작업별 지정)
    codex_model: str | None = None        # codex 워커용 모델 (작업별 지정, None=기본값)
    opencode_model: str = ""              # provider/model; user setup required before live use
    codex_reasoning_effort: str = "medium" # codex reasoning: low | medium | high
    worker_effective_reasoning_effort: str = "medium"
    reviewer_effective_reasoning_effort: str = "medium"
    worker_effort_escalated: bool = False
    reviewer_effort_escalated: bool = False
    worker_effort_escalation_reason: str = ""
    reviewer_effort_escalation_reason: str = ""
    working_dir: str = ""
    worker_no: int = 0                    # 병렬 슬롯 식별자(1~max_workers, 0=미지정)

    profile_id: str = ""
    profile_dir: str = ""
    profile_schema_version: int = 0

    importance: int = 50
    risk: int = 20
    complexity: int = 30

    # 선택 profile 기반 프로젝트 범위 필드
    target_module: list[str] = field(default_factory=list)
    # Optional path-prefix scope below a module, for example ``sample-service/sp`` or
    # ``sample-web/src/pages/admin/sp``. Empty keeps the 0.7 module-level contract.
    target_resources: list[str] = field(default_factory=list)
    commit_summary: str = ""
    criticality: str = "NORMAL"           # NORMAL | HIGH | CRITICAL
    # 0.9: validated immutable Job scope is authoritative.  Requirement text
    # inference is retained only for legacy records with no declared scope.
    scope_authority: str = ""              # JOB_CONTRACT | LEGACY_INFERRED_SCOPE
    scope_provenance: dict[str, Any] = field(default_factory=dict)
    policy_refs: list[str] = field(default_factory=list)
    policy_overlays: list[str] = field(default_factory=list)
    effective_policy: dict[str, Any] = field(default_factory=dict)
    policy_changed_after_materialization: bool = False
    materialized_execution: dict[str, Any] = field(default_factory=dict)
    logical_job_id: str = ""
    materialization_id: str = ""
    execution_id: str = ""
    attempt_id: str = ""
    runtime_process_id: str = ""
    native_session_binding_id: str = ""
    native_session_id: str = ""
    native_turn_id: str = ""
    reviewer_attempt_id: str = ""
    reviewer_native_session_binding_id: str = ""
    reviewer_native_session_id: str = ""
    reviewer_session_contract: dict[str, Any] = field(default_factory=dict)
    reviewer_session_reuse_mode: str = ""
    writer_authority: dict[str, Any] = field(default_factory=dict)
    command_id: str = ""
    control_repository_path: str = ""
    runtime_adapter: dict[str, Any] = field(default_factory=dict)
    runtime_recovery_count: int = 0
    technical_execution_retry_count: int = 0
    product_semantic_retry_count: int = 0
    retry_domain: str = ""
    runtime_recovery_history: list[dict[str, Any]] = field(default_factory=list)
    validation_dispositions: list[dict[str, Any]] = field(default_factory=list)
    selective_revalidation: dict[str, Any] = field(default_factory=dict)
    troubleshooting_bundle: dict[str, Any] = field(default_factory=dict)
    product_readiness: str = "FINALIZATION_PENDING"
    user_action_required: bool = False
    open_decision_count: int = 0
    open_decisions: list[dict[str, Any]] = field(default_factory=list)
    test_plan: dict = field(default_factory=dict)
    test_scope_hash: str = ""
    gate_evidence: dict = field(default_factory=dict)
    official_tester_invoked: bool = False
    official_tester_execution: dict = field(default_factory=dict)
    operator_label: str = ""
    developer_change_report: dict = field(default_factory=dict)
    report_diagnostics: list = field(default_factory=list)
    context_refs: list[str] = field(default_factory=list)
    prompt_metrics: dict[str, Any] = field(default_factory=dict)

    # 작업 모드: MODIFICATION(코드 변경) | ANALYSIS(분석 전용) | ""(미지정→키워드 추정)
    task_mode: str = ""

    # 성공 유형: ""(미확정) | CHANGED | ALREADY_SATISFIED(독립 리뷰 검증 완료)
    completion_kind: str = ""

    # Git source delta와 분리된 Job 전용 문서/검증 산출물 경로.
    artifact_files: list[str] = field(default_factory=list)

    changed_files: list[str] = field(default_factory=list)
    # 0.8.1: current Task ownership is explicit. ``changed_files`` remains the
    # backward-compatible alias of task_owned_changed_files.
    task_owned_changed_files: list[str] = field(default_factory=list)
    inherited_batch_delta_files: list[str] = field(default_factory=list)
    unexpected_external_dirty_files: list[str] = field(default_factory=list)
    pre_job_snapshot_id: str = ""
    # 게이트/리뷰에서 제외된 profile 로컬 개발 설정 파일. 기록/표시용.
    excluded_files: list[str] = field(default_factory=list)
    retry_count: int = 0
    # Reviewer infrastructure recovery is bounded independently from Worker
    # attempts so a valid source delta is never mistaken for implementation
    # failure merely because structured review transport was unavailable.
    reviewer_recovery_count: int = 0
    reviewer_recovery_limit: int = 1
    worker_attempt_limit_configured: int = 0
    worker_attempt_limit_effective: int = 0

    # 워커 간 파일 충돌 방지: {"파일경로/디렉토리": "워커_ID"}
    locked_files: dict[str, str] = field(default_factory=dict)
    eol_repairs: list[str] = field(default_factory=list)
    changed_file_sha256: dict[str, str] = field(default_factory=dict)
    commit_eligible: bool = False
    commit_blockers: list[str] = field(default_factory=list)
    commit_blocker_history: list[dict[str, Any]] = field(default_factory=list)
    eol_repair_diagnostics: list[dict[str, Any]] = field(default_factory=list)

    # 실패 추적
    failure_stage: str = ""               # WORKER | GIT | BUILD | REVIEW
    failure_code: str = ""                # stable reason code, e.g. BUILD_NOT_VERIFIED
    failure_reason: str = ""
    failure_origin: str = ""
    control_field: str = ""
    control_recovery_evidence: dict[str, Any] = field(default_factory=dict)
    is_rolled_back: bool = False
    # Current/terminal Job delta disposition. Queue automation must use this
    # attempt-scoped value instead of the legacy boolean, which can otherwise be
    # stale after an earlier retry was rolled back.
    worktree_disposition: str = "UNKNOWN"  # UNKNOWN | NO_DELTA | PRESERVED | CLEAN_ROLLBACK
    failure_type: str = ""
    severity: str = "NONE"
    fix_scope: str = "RETRYABLE"
    verification_status: str = "SKIPPED"
    attempt_history: list[dict[str, Any]] = field(default_factory=list)
    failure_fingerprint: str = ""
    retry_strategy: str = "ORIGINAL"
    original_worker: str = ""
    original_model: str = ""
    retry_worker: str = ""
    retry_model: str = ""
    reroute_reason: str = ""
    fallback_availability_evidence: str = ""
    planning_artifact_errors: list[str] = field(default_factory=list)
    no_diff_gate: dict[str, Any] = field(default_factory=dict)
    scoped_rollback_targets: list[str] = field(default_factory=list)
    rollback_scope_result: dict[str, Any] = field(default_factory=dict)
    preserved_predecessor_delta_files: list[str] = field(default_factory=list)
    remediation_cycle: int = 0
    user_intervention_reason: str = ""
    reviewed_task_files: list[str] = field(default_factory=list)
    inspected_unchanged_target_files: list[str] = field(default_factory=list)
    profile_build_commands: list[dict[str, Any]] = field(default_factory=list)
    build_evidence: dict[str, Any] = field(default_factory=dict)
    queue_generation: int = 0
    active_baseline_id: str = ""
    batch_id: str = ""
    job_id: str = ""
    client_job_id: str = ""
    outer_attempt: int = 0
    attempt_reservation: dict[str, Any] = field(default_factory=dict)
    reservation_id: str = ""
    attempt_kind: str = ""
    execution_attempt: int = 0
    normal_attempt_no: int = 0
    normal_attempt_budget: int = 0
    technical_recovery_attempt_no: int = 0
    technical_recovery_budget: int = 0
    effective_execution_limit: int = 0
    pre_job_snapshot_manifest: str = ""
    external_frozen_integrity: bool = True
    baseline_declaration_integrity: bool = True
    checkpoint_id: str = ""
    checkpoint_status: str = ""
    checkpoint_manifest: str = ""
    checkpoint_event: str = ""
    checkpoint_failure_evidence: dict[str, Any] = field(default_factory=dict)
    restored_checkpoint_id: str = ""
    restored_for_retry: bool = False
    restored_evidence_epoch: str = ""
    requested_worker: str = ""
    requested_model: str = ""
    planned_worker: str = ""
    planned_model: str = ""
    selected_worker: str = ""
    selected_model: str = ""
    selection_reason: str = ""
    actual_worker: str = ""
    actual_model: str = ""
    actual_executable: str = ""
    actual_invocation_id: str = ""
    worker_routing_policy: str = ""
    worker_routing_evidence_sha256: str = ""
    worker_execution_evidence: dict[str, Any] = field(default_factory=dict)
    patch_diagnostics: dict[str, Any] = field(default_factory=dict)
    no_task_delta_evidence: dict[str, Any] = field(default_factory=dict)
    # 0.8.5: latest role invocation links to a redacted ContextManifest.  The
    # prompt body and environment are deliberately never stored in TaskState.
    context_pack_id: str = ""
    context_manifest_path: str = ""
    context_total_chars: int = 0
    context_selected_count: int = 0
    context_warnings: list[str] = field(default_factory=list)
    prompt_template_version: str = ""
    unknown_optional_fields: dict[str, Any] = field(default_factory=dict)

    build: dict[str, Any] = field(default_factory=lambda: {"status": "WAITING"})
    test: dict[str, Any] = field(default_factory=lambda: {"status": "WAITING"})

    # ===== 2단계: Planner / Tester =====
    use_planner: bool = False                 # 플래너 사용 여부 (기본 OFF)
    # planner 가 생성한 브리프: {"worker_brief","tester_brief","reviewer_brief"}
    planner_brief: dict[str, str] = field(default_factory=dict)

    # Tester(실행 전용 에이전트) 결과. 판정은 Reviewer 가 하며,
    # 기본 정책(KKM_TEST_STRICT=0)에서는 성공 판정에 개입하지 않는다.
    test_status: str = "SKIPPED"              # PASS | FAIL | SKIPPED | ERROR
    test_required: bool = False
    test_summary: str = ""
    test_evidence: list[str] = field(default_factory=list)
    test_frontend: str = ""                   # 선택 세부: 프론트 검증 상태
    test_db: str = ""                         # 선택 세부: DB(SELECT) 검증 상태

    git_status: str = ""
    git_diff_stat: str = ""
    git_diff: str = ""

    # 리뷰 결과
    # 신규 값: REVIEW_PASS | REVIEW_FAIL | REVIEW_UNAVAILABLE | REVIEW_ERROR
    # 구버전 JSON 의 "PASS"/"FAIL" 는 그대로 로드됨(호환).
    review_status: str = "PENDING"
    review_result: str = ""               # 전체 의견(텍스트)
    review_violations: list[str] = field(default_factory=list)  # 위반 규칙 리스트
    review_coverage: dict[str, Any] = field(default_factory=dict)
    review_policy: dict[str, Any] = field(default_factory=dict)
    reviewer_invocations: list[dict[str, Any]] = field(default_factory=list)
    # 0.99.1 review evidence reuse: exact-input bound PASS units of the most
    # recent review round (schema review-evidence/1). See review_evidence.py.
    review_evidence: dict[str, Any] = field(default_factory=dict)

    # 등급 기반 재시도: REVIEW_FAIL 시에만 의미. ""|LOW|MID|HIGH
    review_risk: str = ""
    # 재시도 범위: ""|partial(LOW)|full(MID)|restart(HIGH)
    retry_scope: str = ""
    # LOW/MID 재시도에서 워킹트리를 유지했는지(롤백 생략)
    keep_working_tree: bool = False

    worker_stdout: str = ""
    worker_stderr: str = ""
    exit_code: int | None = None

    # 재시도 연속성(handoff): 이전 실패 시도 요약. 다음 재시도 프롬프트에 주입.
    # 첫 시도에는 빈 문자열. 성공 시 초기화. 구버전 JSON 누락 시 빈 문자열로 보정.
    handoff_context: str = ""

    # 병렬 워커 결과(다중 워커 실행 시 각 워커 결과를 리스트로 병합).
    # 각 원소: {"worker_id","target_module","success","exit_code","stdout","stderr"}
    # worker_stdout/stderr/exit_code 는 요약값으로 병행 유지(하위 호환/리포터/리뷰어용).
    worker_results: list[dict[str, Any]] = field(default_factory=list)

    # 0.9 durable progress projection. progress_revision is deliberately
    # independent from Queue/control revision.
    progress_revision: int = 0
    progress_snapshot: dict[str, Any] = field(default_factory=dict)
    progress_events: list[dict[str, Any]] = field(default_factory=list)
    progress_plan_kind: str = "STANDARD"
    progress_attempt_limit: int = 0
    notification_evidence: list[dict[str, Any]] = field(default_factory=list)
    # 0.99.1 timing breakdown: queue-side wait and admission captured from the
    # job history at execution start; -1 means not captured.
    queue_wait_seconds: int = -1
    admission_control_seconds: int = -1

    # 0.9 runtime/system heartbeat.  The current record is a compact projection
    # of the subprocess owned by the existing Worker/Reviewer wrapper; terminal
    # invocations are copied to history.  These fields are additive so legacy
    # TaskState JSON remains readable.
    execution_runtime: dict[str, Any] = field(default_factory=dict)
    execution_runtime_history: list[dict[str, Any]] = field(default_factory=list)

    # 0.9 QA quarantine metadata.  A candidate remains non-canonical evidence
    # until hash-guarded promotion and integration verification complete.
    qa_type: str = ""
    hold_scope: str = ""
    machine_verified: bool = False
    qa_request: dict[str, Any] = field(default_factory=dict)
    candidate_id: str = ""
    candidate_manifest: str = ""
    # Keep coincident Harness/Reviewer failures separate from the primary QA
    # hold so they can never be mistaken for Worker source defects.
    qa_secondary_failures: list[dict[str, Any]] = field(default_factory=list)

    started_at: str = ""
    ended_at: str = ""
    execution_summary: dict[str, Any] = field(default_factory=dict)
    crash_salvage: dict[str, Any] = field(default_factory=dict)
    crash_recovery_handoff: str = ""

    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def touch(self):
        self.updated_at = datetime.now().isoformat()

    @staticmethod
    def task_date_dir(task_id: str, base_dir: str | Path = ".tasks") -> Path:
        """task_id(TASK-YYYYMMDD-HHmmss) → <base>/YYYYMMDD/ 일자 폴더.
        21차: task 파일(json/md/log)을 일자별로 정리. task_id 형식이 아니면 base 그대로.
        """
        base = Path(base_dir)
        m = re.match(r"TASK-(\d{8})-", task_id or "")
        return base / m.group(1) if m else base

    def save(self, base_dir: str | Path = ".tasks"):
        root = ensure_safe_state_root(base_dir, field="task_root", create=True)
        base = ensure_safe_state_directory(
            root,
            self.task_date_dir(self.task_id, root),
            field="task_date_dir",
            create=True,
        )

        path = base / f"{self.task_id}.json"
        self.touch()

        def scrub(value: Any) -> Any:
            if isinstance(value, str):
                return scrub_secrets(value)
            if isinstance(value, list):
                return [scrub(item) for item in value]
            if isinstance(value, dict):
                return {key: scrub(item) for key, item in value.items()}
            return value

        payload = json.dumps(
            scrub(asdict(self)), ensure_ascii=False, indent=2
        ).encode("utf-8")
        return atomic_state_write(path, payload, root=root, field="task_state")

    @classmethod
    def load(cls, task_id: str, base_dir: str | Path = ".tasks") -> "TaskState":
        # 일자 폴더 우선, 없으면 루트(구버전 파일 하위 호환)
        root = ensure_safe_state_root(base_dir, field="task_root")
        dated_dir = cls.task_date_dir(task_id, root)
        path = dated_dir / f"{task_id}.json"
        try:
            ensure_safe_state_directory(
                root, dated_dir, field="task_date_dir", create=False
            )
            ensure_safe_state_file(
                root, path, field="task_state", allow_missing=True
            )
        except ControlPathError as exc:
            if exc.code != "CONTROL_STATE_PATH_MISSING":
                raise
        if not path.exists():
            path = root / f"{task_id}.json"
            ensure_safe_state_file(root, path, field="task_state")
        with open(path, encoding="utf-8") as f:
            data = json.load(f)

        # 하위 호환: target_module 이 구버전(str)이면 list 로 변환
        tm = data.get("target_module")
        if isinstance(tm, str):
            data["target_module"] = [tm] if tm else []
        elif tm is None:
            data["target_module"] = []

        if not isinstance(data.get("target_resources"), list):
            data["target_resources"] = []
        if not isinstance(data.get("commit_summary"), str):
            data["commit_summary"] = ""

        # locked_files 누락 시 기본값 보정
        if "locked_files" not in data:
            data["locked_files"] = {}
        if not isinstance(data.get("eol_repairs"), list):
            data["eol_repairs"] = []
        if not isinstance(data.get("changed_file_sha256"), dict):
            data["changed_file_sha256"] = {}
        if not isinstance(data.get("commit_eligible"), bool):
            data["commit_eligible"] = False
        if not isinstance(data.get("commit_blockers"), list):
            data["commit_blockers"] = []

        # excluded_files 누락 시 기본값 보정 (구버전 JSON 하위 호환)
        if "excluded_files" not in data:
            data["excluded_files"] = []

        # 2단계 필드 누락 시 기본값 보정 (구버전 JSON 하위 호환)
        if "use_planner" not in data:
            data["use_planner"] = False
        if not isinstance(data.get("planner_brief"), dict):
            data["planner_brief"] = {}
        if data.get("test_status") not in ("PASS", "FAIL", "SKIPPED", "ERROR"):
            data["test_status"] = "SKIPPED"
        if not isinstance(data.get("test_evidence"), list):
            data["test_evidence"] = []
        if not isinstance(data.get("test_required"), bool):
            data["test_required"] = False

        # task_mode 누락/비정상값 보정 (구버전 JSON 하위 호환)
        if data.get("task_mode") not in TASK_MODES:
            data["task_mode"] = ""

        if data.get("completion_kind") not in ("CHANGED", "ALREADY_SATISFIED"):
            data["completion_kind"] = ""
        if not isinstance(data.get("artifact_files"), list):
            data["artifact_files"] = []

        # 리뷰 리스크/재시도 범위 누락·비정상값 보정 (구버전 JSON 하위 호환)
        if data.get("review_risk") not in REVIEW_RISKS:
            data["review_risk"] = ""
        if data.get("retry_scope") not in RETRY_SCOPES:
            data["retry_scope"] = ""
        if not isinstance(data.get("keep_working_tree"), bool):
            data["keep_working_tree"] = False
        if data.get("worktree_disposition") not in (
            "UNKNOWN",
            "NO_DELTA",
            "PRESERVED",
            "CLEAN_ROLLBACK",
            "CHECKPOINTED_CLEAN_ROLLBACK",
        ):
            data["worktree_disposition"] = "UNKNOWN"
        if data.get("codex_reasoning_effort") not in REASONING_EFFORTS:
            data["codex_reasoning_effort"] = "medium"

        # reasoning 실행 이력 누락/비정상값 보정 (구버전 JSON 하위 호환).
        # Worker 이력 누락 시 사용자 baseline을 보존해 high를 낮추지 않는다.
        if data.get("worker_effective_reasoning_effort") not in REASONING_EFFORTS:
            data["worker_effective_reasoning_effort"] = data["codex_reasoning_effort"]
        if data.get("reviewer_effective_reasoning_effort") not in REASONING_EFFORTS:
            data["reviewer_effective_reasoning_effort"] = "medium"
        for field_name in ("worker_effort_escalated", "reviewer_effort_escalated"):
            if not isinstance(data.get(field_name), bool):
                data[field_name] = False
        for field_name in (
            "worker_effort_escalation_reason",
            "reviewer_effort_escalation_reason",
        ):
            if not isinstance(data.get(field_name), str):
                data[field_name] = ""

        defaults = {
            "failure_code": "",
            "failure_origin": "",
            "control_field": "",
            "control_recovery_evidence": {},
            "profile_id": "",
            "profile_dir": "",
            "profile_schema_version": 0,
            "failure_type": "",
            "severity": "NONE",
            "fix_scope": "RETRYABLE",
            "verification_status": "SKIPPED",
            "attempt_history": [],
            "commit_blocker_history": [],
            "eol_repair_diagnostics": [],
            "review_coverage": {},
            "review_policy": {},
            "reviewer_invocations": [],
            "review_evidence": {},
            "queue_wait_seconds": -1,
            "admission_control_seconds": -1,
            "task_owned_changed_files": [],
            "inherited_batch_delta_files": [],
            "unexpected_external_dirty_files": [],
            "pre_job_snapshot_id": "",
            "reviewer_recovery_count": 0,
            "reviewer_recovery_limit": 1,
            "worker_attempt_limit_configured": 0,
            "worker_attempt_limit_effective": 0,
            "failure_fingerprint": "",
            "retry_strategy": "ORIGINAL",
            "original_worker": "",
            "original_model": "",
            "retry_worker": "",
            "retry_model": "",
            "reroute_reason": "",
            "fallback_availability_evidence": "",
            "planning_artifact_errors": [],
            "no_diff_gate": {},
            "scoped_rollback_targets": [],
            "rollback_scope_result": {},
            "preserved_predecessor_delta_files": [],
            "remediation_cycle": 0,
            "user_intervention_reason": "",
            "reviewed_task_files": [],
            "inspected_unchanged_target_files": [],
            "profile_build_commands": [],
            "build_evidence": {},
            "queue_generation": 0,
            "active_baseline_id": "",
            "batch_id": "",
            "job_id": "",
            "client_job_id": "",
            "outer_attempt": 0,
            "attempt_reservation": {},
            "reservation_id": "",
            "attempt_kind": "",
            "execution_attempt": 0,
            "normal_attempt_no": 0,
            "normal_attempt_budget": 0,
            "technical_recovery_attempt_no": 0,
            "technical_recovery_budget": 0,
            "effective_execution_limit": 0,
            "pre_job_snapshot_manifest": "",
            "external_frozen_integrity": True,
            "baseline_declaration_integrity": True,
            "checkpoint_id": "",
            "checkpoint_status": "",
            "checkpoint_manifest": "",
            "checkpoint_event": "",
            "checkpoint_failure_evidence": {},
            "restored_checkpoint_id": "",
            "restored_for_retry": False,
            "restored_evidence_epoch": "",
            "requested_worker": "",
            "requested_model": "",
            "planned_worker": "",
            "planned_model": "",
            "selected_worker": "",
            "selected_model": "",
            "selection_reason": "",
            "actual_worker": "",
            "actual_model": "",
            "actual_executable": "",
            "actual_invocation_id": "",
            "worker_routing_policy": "",
            "worker_routing_evidence_sha256": "",
            "worker_execution_evidence": {},
            "patch_diagnostics": {},
            "no_task_delta_evidence": {},
            "context_pack_id": "",
            "context_manifest_path": "",
            "context_total_chars": 0,
            "context_selected_count": 0,
            "context_warnings": [],
            "prompt_template_version": "",
            "unknown_optional_fields": {},
            "scope_authority": "",
            "scope_provenance": {},
            "policy_refs": [],
            "policy_overlays": [],
            "effective_policy": {},
            "context_refs": [],
            "prompt_metrics": {},
            "progress_revision": 0,
            "progress_snapshot": {},
            "progress_events": [],
            "progress_plan_kind": "STANDARD",
            "progress_attempt_limit": 0,
            "notification_evidence": [],
            "execution_runtime": {},
            "execution_runtime_history": [],
            "qa_type": "",
            "hold_scope": "",
            "machine_verified": False,
            "qa_request": {},
            "candidate_id": "",
            "candidate_manifest": "",
            "started_at": "",
            "ended_at": "",
            "execution_summary": {},
            "crash_salvage": {},
            "crash_recovery_handoff": "",
        }
        for field_name, default in defaults.items():
            if field_name not in data or not isinstance(data[field_name], type(default)):
                data[field_name] = default

        # Forward-compatible state loading: a newer producer may add optional
        # evidence fields.  Preserve them without letting an unknown key make an
        # older 0.8.x Task unreadable.
        known = {item.name for item in __import__("dataclasses").fields(cls)}
        unknown = {
            key: value for key, value in data.items()
            if key not in known
        }
        data = {key: value for key, value in data.items() if key in known}
        carried = data.get("unknown_optional_fields")
        if not isinstance(carried, dict):
            carried = {}
        data["unknown_optional_fields"] = {**carried, **unknown}
        return cls(**data)
