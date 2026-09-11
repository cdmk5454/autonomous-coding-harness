"""
Manager - 작업 조율 뼈대 (연속 작업 백그라운드 병렬 실행 개선)
- 사용자가 연속으로 입력한 독립 작업(Job)을 ThreadPoolExecutor(max_workers=3)로
  백그라운드 병렬 실행(멀티태스킹).
- _split_work(모듈 단위 작업 분할) 제거 → 단일 Task 단위 파이프라인.
  병렬성은 "모듈 분할"이 아니라 "독립 작업(Job) 동시 실행"에서 발생.
- 파이프라인: Worker(단일) → Git(모듈별 개별 저장소) → Build/Test → Review
  (재시도/롤백 포함).
- 검증 게이트 (task_mode 기준):
  - MODIFICATION(코드 변경 작업):
      produced_work AND git_ok AND build==PASS AND review==REVIEW_PASS
      (build=SKIPPED/FAIL, REVIEW_UNAVAILABLE/ERROR 는 성공 불가;
       Reviewer 인프라 오류만 있으면 Manager가 제한 재호출 후 수동 리뷰 대기)
  - ANALYSIS(분석 전용 작업):
      빈 diff 허용, build=SKIPPED 허용, REVIEW_UNAVAILABLE/ERROR 는
      '검증 불완전' 경고로만 기록(자동 FAIL 아님). REVIEW_FAIL/build=FAIL은 실패.
  - task_mode 는 TaskState.task_mode 명시값 우선, 미지정 시 키워드 추정.
"""

from __future__ import annotations

import concurrent.futures
from execution_lifecycle import ShutdownBarrier, create_executor
import fnmatch
import hashlib
import json
import os
import re
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Mapping

from worker import DroidWorker, CodexWorker, OpenCodeWorker, WorkerResult, parse_qa_request, CODEX_IPC_FAILURES
from git_collector import GitCollector, GitDiffResult, GitTaskBaseline
from build_test_runner import BuildTestRunner, BuildTestResult
from reviewer import (
    Reviewer,
    ReviewResult,
    REVIEW_PASS,
    REVIEW_FAIL,
    REVIEW_ERROR,
    REVIEW_UNAVAILABLE,
    REVIEW_REASONING,
    DiffBatchPlan,
    DIFF_TOO_LARGE_FOR_AUTOMATED_REVIEW,
    BINARY_DIFF_REQUIRES_MANUAL_REVIEW,
    REVIEW_COVERAGE_PLAN_INVALID,
    STRUCTURED_OUTPUT_INVALID,
    REVIEW_SCHEMA_INVALID,
    REVIEW_EXECUTION_UNAVAILABLE,
    REVIEW_OUTPUT_PARSE_FAILED,
    RUNTIME_CONTRACT_DRIFT,
    deterministic_violations,
)
from task_state import (
    TaskState,
    resolve_task_mode,
    extract_requirement_paths,
    can_verify_current_state,
    resolve_worker_reasoning_effort,
    resolve_reviewer_reasoning_effort,
    TASK_MODE_MODIFICATION,
)
from planner import run_planner
from tester import run_tester, should_run_tester, mark_tester_skipped
import gate_evidence
from project_profile import ProjectProfile, DEFAULT_NON_SOURCE_ARTIFACT_PATTERNS
from runtime_safety import scrub_secrets, safe_print as print, trusted_executable
from job_contract import (
    CODEX_MODELS,
    DROID_DEFAULT_CODING_MODEL,
    DROID_MODELS,
    WORKER_TYPES,
    DROID_GLM_FAIL_CLOSED_MODEL,
    DROID_GLM_FAIL_CLOSED_POLICY,
    JobContractError,
    droid_model_selection_reason,
    resolve_droid_model,
)
from attempt_checkpoint import AttemptCheckpointError, AttemptCheckpointStore
from patch_diagnostics import (
    CONTEXT_MISMATCH as WORKER_PATCH_CONTEXT_MISMATCH,
    classify_patch_context,
    write_patch_diagnostic,
)
from policy_catalog import (
    REVIEWER_CODE_FINDING,
    WORKER_CAUSED,
    classify_failure_origin,
    worker_remediation_allowed,
)
from review_policy import decision_from_task
import review_evidence
from progress_projection import (
    HEARTBEAT_SECONDS,
    meaningful_progress_update,
    project_execution_health,
    render_progress,
)
from notify import send_notify_evidence


@dataclass
class ManagerResult:
    success: bool
    worker_result: WorkerResult | None = None
    git_result: GitDiffResult | None = None
    build_test_result: BuildTestResult | None = None
    review_result: ReviewResult | None = None
    message: str = ""
    secondary_failures: tuple[str, ...] = ()


WORKER_APPROVAL_WAIT_REASON = "워커가 구현 승인 대기 응답으로 종료함 (변경사항 없음)"
BUILD_NOT_VERIFIED = "BUILD_NOT_VERIFIED"
BUILD_FAILED = "BUILD_FAILED"
TEST_FAILED = "TEST_FAILED"
REVIEW_FAILED = "REVIEW_FAILED"
REVIEW_INFRA_STATUSES = (REVIEW_ERROR, REVIEW_UNAVAILABLE)
REVIEW_INFRA_RETRY_LIMIT = 1
DETERMINISTIC_REVIEW_FAILURE_CODES = {
    DIFF_TOO_LARGE_FOR_AUTOMATED_REVIEW,
    BINARY_DIFF_REQUIRES_MANUAL_REVIEW,
    REVIEW_COVERAGE_PLAN_INVALID,
    REVIEW_SCHEMA_INVALID,
    RUNTIME_CONTRACT_DRIFT,
}
MANUAL_REVIEW_FAILURE_CODES = DETERMINISTIC_REVIEW_FAILURE_CODES | {
    STRUCTURED_OUTPUT_INVALID,
    REVIEW_EXECUTION_UNAVAILABLE,
    REVIEW_OUTPUT_PARSE_FAILED,
}
NO_MEANINGFUL_SOURCE_CHANGE = "NO_MEANINGFUL_SOURCE_CHANGE"
WORKER_PATCH_CONFLICT = "WORKER_PATCH_CONFLICT"
WORKER_PATCH_CONTEXT_STALE = "WORKER_PATCH_CONTEXT_STALE"
CHECKPOINT_CAPTURE_FAILED = "CHECKPOINT_CAPTURE_FAILED"
CHECKPOINT_ROLLBACK_VERIFY_FAILED = "CHECKPOINT_ROLLBACK_VERIFY_FAILED"
NO_TASK_DELTA_VERIFICATION_FAILED = "NO_TASK_DELTA_VERIFICATION_FAILED"
PATCH_EXPECTED_LINES_NOT_FOUND = "PATCH_EXPECTED_LINES_NOT_FOUND"
PATCH_MULTIPLE_OPERATIONS_SAME_TARGET = "PATCH_MULTIPLE_OPERATIONS_SAME_TARGET"
PATCH_EOL_MISMATCH = "PATCH_EOL_MISMATCH"
PATCH_ENCODING_MISMATCH = "PATCH_ENCODING_MISMATCH"
TECHNICAL_RECOVERY_CODES = {
    NO_MEANINGFUL_SOURCE_CHANGE,
    WORKER_PATCH_CONFLICT,
    WORKER_PATCH_CONTEXT_STALE,
    WORKER_PATCH_CONTEXT_MISMATCH,
    PATCH_EXPECTED_LINES_NOT_FOUND,
    PATCH_MULTIPLE_OPERATIONS_SAME_TARGET,
    PATCH_EOL_MISMATCH,
    PATCH_ENCODING_MISMATCH,
    REVIEW_COVERAGE_PLAN_INVALID,
    "PROFILE_BUILD_COMMAND_NOT_FOUND",
}
TECHNICAL_RECOVERY_MAX_ATTEMPTS = 4
TECHNICAL_REMEDIATION_MAX_CYCLES = 1


def _review_infrastructure_code(result: ReviewResult) -> str:
    if result.failure_code:
        return result.failure_code
    if result.status == REVIEW_UNAVAILABLE:
        return REVIEW_EXECUTION_UNAVAILABLE
    return REVIEW_OUTPUT_PARSE_FAILED


def _normalized_failure_reason(value: str) -> str:
    text = scrub_secrets(value or "").casefold()
    text = re.sub(r"task-\d{8}-\d{6}(?:-\d+)?", "task-<id>", text)
    text = re.sub(r"[a-f0-9]{16,}", "<digest>", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text[:1000]


def _tool_error_kinds(task: TaskState) -> list[str]:
    text = f"{task.worker_stderr}\n{task.worker_stdout}".casefold()
    kinds: list[str] = []
    for needle, code in (
        ("multiple operations target", WORKER_PATCH_CONFLICT),
        ("apply_patch verification failed", "APPLY_PATCH_VERIFICATION_FAILED"),
        ("target context not found", WORKER_PATCH_CONTEXT_STALE),
        ("file not found", "FILE_NOT_FOUND"),
        ("timed out", "TIMEOUT"),
        ("timeout", "TIMEOUT"),
    ):
        if needle in text and code not in kinds:
            kinds.append(code)
    return kinds


def _task_model(task: TaskState, worker: str) -> str:
    if worker == "codex":
        return str(task.codex_model or "")
    if worker == "opencode":
        return str(task.opencode_model or "")
    return str(task.droid_model or "")


def _worker_patch_failure(task: TaskState) -> tuple[str, str]:
    text = f"{task.worker_stderr}\n{task.worker_stdout}"
    folded = text.casefold()
    if "expected lines" in folded and "not found" in folded:
        code, needle = PATCH_EXPECTED_LINES_NOT_FOUND, "expected lines"
    elif "multiple operations target" in folded:
        code, needle = WORKER_PATCH_CONFLICT, "multiple operations target"
    elif "patch eol mismatch" in folded or "line ending mismatch" in folded:
        code, needle = PATCH_EOL_MISMATCH, "mismatch"
    elif "patch encoding mismatch" in folded or "encoding mismatch" in folded:
        code, needle = PATCH_ENCODING_MISMATCH, "mismatch"
    elif "apply_patch verification failed" in folded or "target context not found" in folded:
        evidence = dict(getattr(task, "worker_execution_evidence", {}) or {})
        code = classify_patch_context(
            text,
            pre_read_sha256=str(evidence.get("pre_read_sha256", "")),
            pre_apply_sha256=str(evidence.get("pre_apply_sha256", "")),
            mutation_provenance=dict(evidence.get("mutation_provenance") or {}),
        )
        needle = "apply_patch verification failed" if "apply_patch verification failed" in folded else "target context not found"
    else:
        return "", ""
    diagnostic = next((line.strip()[:1000] for line in text.splitlines() if needle in line.casefold()), code)
    return code, diagnostic


def failure_fingerprint(task: TaskState) -> tuple[str, str]:
    route_model = task.retry_model or (
        _task_model(task, task.worker)
    ) or "default"
    core = {
        "stage": task.failure_stage,
        "code": task.failure_code,
        "reason": _normalized_failure_reason(task.failure_reason),
        "has_task_delta": bool(task.task_owned_changed_files),
        "tool_errors": _tool_error_kinds(task),
        "coverage_error": str(task.review_coverage.get("failure_code", ""))
        or (task.failure_code if "COVERAGE" in task.failure_code else ""),
        "target_modules": sorted(task.target_module),
    }
    routed = {**core, "worker": task.worker, "model": route_model}
    encoded = json.dumps(routed, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    core_encoded = json.dumps(core, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest(), hashlib.sha256(core_encoded.encode()).hexdigest()


def _is_non_source_artifact(path: str, profile: ProjectProfile | None) -> bool:
    if profile is not None:
        return profile.is_non_source_artifact(path)
    normalized = path.replace("\\", "/").strip("/")
    name = normalized.rsplit("/", 1)[-1]
    return any(
        fnmatch.fnmatchcase(name.casefold(), pattern.casefold())
        or fnmatch.fnmatchcase(normalized.casefold(), pattern.casefold())
        for pattern in DEFAULT_NON_SOURCE_ARTIFACT_PATTERNS
    )


def _artifact_dir_for(task_id: str) -> Path:
    """task_id → 하네스 doc/ 산출물 디렉토리 (worker.artifact_paths 와 동일 규칙)."""
    from worker import artifact_paths
    return Path(artifact_paths(task_id)[0])


def collect_task_artifacts(task: TaskState) -> list[str]:
    """Job 산출물(doc/<일자>/<시간>/) 파일을 source diff와 분리해 기록한다.
    21차: .tasks/<task_id>/ → doc/YYYYMMDD/HHmmss/ (하네스 루트 기준 절대경로).
    워커가 프롬프트로 받은 산출물 경로와 동일 소스(worker.artifact_paths).
    """
    root = _artifact_dir_for(task.task_id)
    artifacts: list[str] = []
    if root.is_dir():
        artifacts = [
            path.as_posix()
            for path in sorted(root.rglob("*"))
            if path.is_file()
        ]
    task.artifact_files = artifacts
    return artifacts


def is_worker_approval_wait_response(text: str) -> bool:
    """비대화형 Worker가 구현 대신 승인 질문으로 끝났는지 보수적으로 감지."""
    body = (text or "").casefold()
    return any(phrase in body for phrase in (
        "구현해도 될까요",
        "수정해도 될까요",
        "진행해도 될까요",
        "shall i proceed",
        "should i proceed",
        "may i proceed",
        "would you like me to proceed",
        "would you like me to implement",
    ))


class TaskScopeCoordinator:
    """Serialize only overlapping module or resource path-prefix scopes."""

    ROOT = "*"

    def __init__(self):
        self._condition = threading.Condition()
        self._active: dict[str, frozenset[str]] = {}

    @staticmethod
    def keys(resources: list[str]) -> frozenset[str]:
        normalized = {
            item.strip().replace("\\", "/").strip("/").casefold()
            for item in resources
            if item and item.strip().strip("/\\")
        }
        return frozenset(normalized) if normalized else frozenset({TaskScopeCoordinator.ROOT})

    @classmethod
    def _overlaps(cls, left: frozenset[str], right: frozenset[str]) -> bool:
        if cls.ROOT in left or cls.ROOT in right:
            return True
        return any(
            a == b or a.startswith(b + "/") or b.startswith(a + "/")
            for a in left
            for b in right
        )

    def acquire(self, job_id: str, resources: list[str]) -> bool:
        wanted = self.keys(resources)
        waited = False
        with self._condition:
            while any(
                holder != job_id and self._overlaps(wanted, held)
                for holder, held in self._active.items()
            ):
                waited = True
                self._condition.wait()
            self._active[job_id] = wanted
        return waited

    def release(self, job_id: str) -> None:
        with self._condition:
            self._active.pop(job_id, None)
            self._condition.notify_all()


class Manager:
    def __init__(
        self,
        working_dir: str,
        model: str,
        max_retry: int = 3,
        timeout: int = 300,
        worker_type: str = "droid",
        codex_model: str | None = None,
        opencode_model: str = "",
        max_workers: int = 3,
        profile: ProjectProfile | None = None,
        task_root: str | Path = ".tasks",
        configuration_guard: Callable[[], None] | None = None,
        policy_root: str | Path | None = None,
        initial_baseline: GitTaskBaseline | None = None,
        checkpoint_store: AttemptCheckpointStore | None = None,
        restore_checkpoint_manifest: str = "",
        restore_checkpoint_source_job_id: str = "",
        restore_checkpoint_source_client_job_id: str = "",
        initial_delta_applier: Callable[[], Mapping[str, Any] | None] | None = None,
    ):
        self._lifecycle = ShutdownBarrier()
        self.working_dir = str(Path(working_dir).resolve())
        self.model = model
        self.max_retry = max_retry
        self.timeout = timeout
        self.worker_type = worker_type
        self.codex_model = codex_model
        self.opencode_model = opencode_model
        self.max_workers = max(1, max_workers)
        self.profile = profile
        self.task_root = Path(task_root)
        self.configuration_guard = configuration_guard
        self.policy_root = Path(policy_root) if policy_root is not None else None
        self._initial_baseline = initial_baseline
        self.checkpoint_store = checkpoint_store
        self.restore_checkpoint_manifest = restore_checkpoint_manifest
        self.restore_checkpoint_source_job_id = restore_checkpoint_source_job_id
        self.restore_checkpoint_source_client_job_id = restore_checkpoint_source_client_job_id
        self.initial_delta_applier = initial_delta_applier

        # 활동 기반 타임아웃 정책 (환경변수에서 로드)
        from worker import TimeoutConfig
        self.timeout_config = TimeoutConfig.from_env()
        print(f"[Manager] 타임아웃 정책: {self.timeout_config.summary}")

        # TaskState 병렬 병합 보호용 락
        self._state_lock = threading.Lock()
        self._scope_coordinator = TaskScopeCoordinator()
        self._task_baselines = {}

        # 백그라운드 작업용 공유 스레드 풀 (연속 작업 병렬 실행, 최대 3개 동시)
        self._executor = create_executor(
            max_workers=self.max_workers, thread_name_prefix="job"
        )

        self.collector = GitCollector(self.working_dir, profile=profile)
        self.build_test_runner = BuildTestRunner(
            self.working_dir, profile=profile, evidence_root=self.task_root
        )
        self.reviewer = Reviewer(
            working_dir=self.working_dir,
            timeout=timeout,
            timeout_config=self.timeout_config,   # 워커와 동일한 활동 기반 타임아웃
            profile=profile,
            policy_root=self.policy_root,
            manifest_root=self.task_root / "context-manifests",
        )

    # ------------------------------------------------------------------
    # 백그라운드 실행 제어
    # ------------------------------------------------------------------
    def submit_task(self, task: TaskState) -> concurrent.futures.Future:
        """백그라운드 스레드 풀에 작업(Job) 제출. Future 반환."""
        with self._lifecycle.dispatch():
            return self._executor.submit(self.run_with_retry, task)

    def shutdown(self):
        """모든 백그라운드 작업 완료 대기 후 풀 종료."""
        self._lifecycle.close()
        self._executor.shutdown(wait=True)

    # ------------------------------------------------------------------
    # 워커 선택 (작업별 task 기반)
    # ------------------------------------------------------------------
    def choose_worker(self, task: TaskState) -> str:
        """
        워커 선택 (작업별).
        - task.worker 가 설정되어 있으면 그것을 사용.
        - 확장 자리: complexity/importance 기반 자동 분기
          예) if task.complexity >= 70: return "codex"
        """
        return task.retry_worker or task.worker or self.worker_type

    def _get_worker(self, task: TaskState) -> tuple[DroidWorker | CodexWorker | OpenCodeWorker, str]:
        """
        작업별 워커 생성(스레드 안전).
        - 매 파이프라인 실행 시 task 설정에서 워커를 새로 만들어 공유 가변 상태(self.model 등)
          에 대한 race 를 원천 차단한다.
        """
        wt = self.choose_worker(task)
        profile = getattr(self, "profile", None)
        policy_root = getattr(self, "policy_root", None)
        task_root = getattr(self, "task_root", None)
        manifest_root = Path(task_root) / "context-manifests" if task_root else None
        if wt == "codex":
            frozen_route = dict(
                dict(task.materialized_execution or {}).get("effective_policy") or {}
            ).get("resolved_worker", {})
            locked_effort = str(frozen_route.get("reasoning_effort") or "")
            if frozen_route.get("reasoning_effort_locked") and locked_effort in {
                "low", "medium", "high"
            }:
                effort, reason = locked_effort, ""
            else:
                effort, reason = resolve_worker_reasoning_effort(
                    task, task.retry_count + 1
                )
            task.worker_effective_reasoning_effort = effort
            task.worker_effort_escalated = bool(reason)
            task.worker_effort_escalation_reason = reason
            return CodexWorker(
                timeout=self.timeout,
                model=task.retry_model or task.codex_model,
                reasoning_effort=effort,
                timeout_config=self.timeout_config,
                profile=profile,
                policy_root=policy_root,
                manifest_root=manifest_root,
            ), "codex"
        if wt == "opencode":
            frozen_route = dict(
                dict(task.materialized_execution or {}).get("effective_policy") or {}
            ).get("resolved_worker", {})
            locked_effort = str(frozen_route.get("reasoning_effort") or "")
            if frozen_route.get("reasoning_effort_locked") and locked_effort in {
                "low", "medium", "high"
            }:
                task.worker_effective_reasoning_effort = locked_effort
            return OpenCodeWorker(
                timeout=self.timeout,
                model=task.retry_model or task.opencode_model or self.opencode_model,
                reasoning_effort=task.worker_effective_reasoning_effort,
                timeout_config=getattr(self, "timeout_config", None),
                profile=profile,
                policy_root=policy_root,
                manifest_root=manifest_root,
            ), "opencode"
        droid_model = (
            task.retry_model or task.droid_model or self.model or DROID_DEFAULT_CODING_MODEL
        )
        try:
            # Immutable legacy contracts may still carry a retired Droid model
            # ID; resolve the declared alias to its canonical execution target.
            # Unknown models are never substituted and fail closed downstream.
            droid_model = resolve_droid_model(droid_model)[0]
        except JobContractError:
            pass
        return DroidWorker(
            timeout=self.timeout,
            model=droid_model,
            timeout_config=self.timeout_config,
            profile=profile,
            policy_root=policy_root,
            manifest_root=manifest_root,
        ), "droid"

    def _fallback_route(self, task: TaskState) -> tuple[str, str, str]:
        if task.materialized_execution:
            return "", "", "frozen execution policy has no alternate Worker route"
        if task.worker_routing_policy == DROID_GLM_FAIL_CLOSED_POLICY:
            return "", "", "fail-closed Droid GLM policy forbids worker/model fallback"
        original = task.original_worker or task.worker or self.worker_type
        fallback = "droid" if original in {"codex", "opencode"} else "codex"
        if fallback not in WORKER_TYPES:
            return "", "", "routing policy does not allow an alternate worker"
        model = DROID_DEFAULT_CODING_MODEL if fallback == "droid" else CODEX_MODELS[0]
        try:
            executable = trusted_executable(fallback, forbidden_root=self.working_dir)
        except FileNotFoundError:
            return "", "", f"{fallback} executable unavailable"
        evidence = f"worker={fallback};model={model};executable={Path(executable).name};policy=job_contract"
        return fallback, model, evidence

    @staticmethod
    def _worker_routing_failure(task: TaskState) -> ManagerResult | None:
        """Reject a fail-closed technical route before any Worker subprocess starts."""
        if task.worker_routing_policy != DROID_GLM_FAIL_CLOSED_POLICY:
            return None
        worker = task.retry_worker or task.worker
        model = task.retry_model or task.droid_model
        if worker == "droid":
            try:
                # The canonical model or its declared legacy alias are the only
                # accepted execution targets for the fail-closed Droid policy.
                canonical_model, _ = resolve_droid_model(model)
            except JobContractError:
                canonical_model = ""
            if canonical_model == DROID_GLM_FAIL_CLOSED_MODEL:
                return None
        task.failure_stage = "CONTROL"
        task.failure_code = "DROID_MODEL_ROUTING_POLICY_VIOLATION"
        task.failure_reason = task.failure_code
        task.failure_type = "infrastructure"
        task.severity = "HIGH"
        task.fix_scope = "NON_RETRYABLE"
        task.verification_status = "FAILED"
        task.worktree_disposition = "NO_DELTA"
        task.status = "AWAITING_QA"
        task.stage = "AWAITING_QA"
        return ManagerResult(success=False, message=task.failure_code)

    def _prepare_reviewer_effort(
        self, task: TaskState, infrastructure_retry: bool = False
    ) -> str:
        """Set the effective Reviewer effort before the one Reviewer invocation."""
        selected = str(dict(task.review_policy or {}).get("reasoning_effort", ""))
        if selected in ("low", "medium", "high"):
            task.reviewer_effective_reasoning_effort = selected
            task.reviewer_effort_escalated = False
            task.reviewer_effort_escalation_reason = ""
            return selected
        policy_attempt = self.max_retry if infrastructure_retry else task.retry_count + 1
        effort, reason = resolve_reviewer_reasoning_effort(
            task, policy_attempt, self.max_retry, REVIEW_REASONING
        )
        task.reviewer_effective_reasoning_effort = effort
        task.reviewer_effort_escalated = bool(reason)
        task.reviewer_effort_escalation_reason = reason
        return effort

    def _configuration_failure(self, task: TaskState) -> ManagerResult | None:
        """Fail closed when the queued profile/policy snapshot is no longer current."""
        configuration_guard = getattr(self, "configuration_guard", None)
        if configuration_guard is None:
            return None
        try:
            configuration_guard()
        except Exception as exc:
            # The guard's underlying exception may include paths or file content.
            # Persist only the stable failure code across every control surface.
            task.failure_stage = "CONTROL"
            task.failure_code = "GLOBAL_STOP" if getattr(exc, "code", "") == "GLOBAL_STOP" else "PROFILE_DRIFT"
            task.failure_reason = task.failure_code
            task.failure_type = "infrastructure"
            task.severity = "HIGH"
            task.fix_scope = "NON_RETRYABLE"
            mutation_possible = bool(
                getattr(task, "_control_external_mutation_possible", False)
            )
            if task.changed_files:
                task.worktree_disposition = "PRESERVED"
            elif mutation_possible:
                # Planner/Worker may have changed the worktree before Git collection.
                # UNKNOWN is deliberately not treated as a safe rollback state.
                task.worktree_disposition = "UNKNOWN"
            else:
                task.worktree_disposition = "NO_DELTA"
            task.verification_status = "PARTIAL" if mutation_possible else "FAILED"
            return ManagerResult(success=False, message=task.failure_code)
        return None

    def _verify_preserved_source_baseline(
        self, task: TaskState, baseline: Any
    ) -> tuple[bool, str] | None:
        """Validate an exact same-Job technical candidate against its old baseline."""
        reservation = dict(task.attempt_reservation or {})
        proof = dict(reservation.get("provenance") or {})
        expected = dict(proof.get("preserved_source_hashes") or {})
        if not (
            reservation.get("attempt_kind") == "TECHNICAL_RECOVERY"
            and proof.get("reuse_source_snapshot") is True
            and expected
        ):
            return None
        delta = self.collector.collect(task, baseline)
        if not delta.success:
            return False, delta.error or "TECHNICAL_RECOVERY_SOURCE_DIFF_INVALID"
        changed = sorted(dict.fromkeys(delta.changed_files))
        if changed != sorted(expected):
            return False, "TECHNICAL_RECOVERY_SOURCE_SCOPE_CHANGED"
        root = Path(self.working_dir).resolve()
        for relative, digest in expected.items():
            target = (root / relative).resolve()
            if (
                not target.is_relative_to(root)
                or not target.is_file()
                or hashlib.sha256(target.read_bytes()).hexdigest() != str(digest)
            ):
                return False, "TECHNICAL_RECOVERY_SOURCE_HASH_CHANGED"
        return True, ""

    def _binding_failure(self, task, *, require_all=False):
        if not task.materialized_execution:
            return None
        failures = gate_evidence.validate(task, gate_evidence.identity(task, self.working_dir), require_all=require_all)
        if not failures:
            return None
        task.failure_stage = "CONTROL"
        task.failure_code = "GATE_EVIDENCE_STALE"
        task.failure_reason = ", ".join(failures)
        task.failure_origin = "HARNESS_CAUSED"
        task.failure_type = "infrastructure"
        task.fix_scope = "NON_RETRYABLE"
        task.verification_status = "PARTIAL"
        task.commit_eligible = False
        task.worktree_disposition = "PRESERVED" if task.changed_files else "NO_DELTA"
        return ManagerResult(success=False, message=task.failure_code)

    @staticmethod
    def _prepare_restored_evidence_epoch(
        task: TaskState, checkpoint_id: str, files: list[str]
    ) -> None:
        """Make checkpoint restoration an explicit, non-success starting state.

        Every execution-derived gate is cleared here so no call path can reuse a
        Build, Test, verification, Review, or commit decision from the failed
        attempt that produced the checkpoint.
        """
        task.restored_checkpoint_id = checkpoint_id
        task.restored_for_retry = True
        task.restored_evidence_epoch = (
            f"{checkpoint_id}:outer-{task.outer_attempt}"
        )
        task.checkpoint_status = "RESTORED_FOR_RETRY"
        task.status = "RUNNING"
        task.stage = "WORKER"
        task.changed_files = list(files)
        task.task_owned_changed_files = list(files)
        task.build = {"status": "WAITING"}
        task.build_evidence = {}
        task.test = {"status": "WAITING"}
        task.test_status = "SKIPPED"
        task.test_summary = ""
        task.test_evidence = []
        task.verification_status = "SKIPPED"
        task.review_status = "PENDING"
        task.review_result = ""
        task.review_violations = []
        task.review_coverage = {}
        # A restored checkpoint is a new execution epoch: exact-input evidence
        # from the failed attempt must not survive as reusable review proof.
        task.review_evidence = {}
        task.commit_eligible = False
        task.commit_blockers = []
        task.failure_stage = ""
        task.failure_code = ""
        task.failure_reason = ""

    @staticmethod
    def _aggregate_review_batches(
        pairs: list[tuple[object, ReviewResult]],
        plan: DiffBatchPlan,
        total_files: list[str],
    ) -> ReviewResult:
        """Aggregate parse-valid semantic verdicts and prove changed-atom coverage."""
        semantic_failures = [result for _, result in pairs if result.status == REVIEW_FAIL]
        infrastructure = [
            result for _, result in pairs if result.status in REVIEW_INFRA_STATUSES
        ]
        semantic_results = [
            (batch, result) for batch, result in pairs
            if result.status in (REVIEW_PASS, REVIEW_FAIL)
        ]
        reviewed_atoms = [
            atom
            for batch, _ in semantic_results
            for atom in getattr(batch, "changed_atoms", ())
        ]
        counts: dict[str, int] = {}
        for atom in reviewed_atoms:
            counts[atom] = counts.get(atom, 0) + 1
        missing_atoms = [atom for atom in plan.expected_atoms if atom not in counts]
        duplicate_atoms = [atom for atom, count in counts.items() if count != 1]
        reviewed_files = list(dict.fromkeys(
            path for _, result in semantic_results for path in result.reviewed_files
        ))
        normalized_total = list(dict.fromkeys(total_files))
        missing_files = [path for path in normalized_total if path not in reviewed_files]
        complete = (
            len(pairs) == len(plan.batches)
            and not infrastructure
            and not missing_atoms
            and not duplicate_atoms
            and all(result.status in (REVIEW_PASS, REVIEW_FAIL) for _, result in pairs)
        )
        expected_count = len(plan.expected_atoms)
        coverage_percent = (
            100 if expected_count == 0 and complete
            else int(100 * (expected_count - len(missing_atoms)) / expected_count)
            if expected_count else 0
        )
        system_codes = list(dict.fromkeys(
            result.failure_code for _, result in pairs if result.failure_code
        ))
        coverage = {
            "total_files": normalized_total,
            "reviewed_files": reviewed_files,
            "missing_files": missing_files,
            "batch_count": len(plan.batches),
            "completed_batch_count": len(pairs),
            "batch_ids": [batch.batch_id for batch, _ in pairs],
            "max_batch_chars": plan.max_chars,
            "max_batches": plan.max_batches,
            "expected_atom_count": expected_count,
            "reviewed_atom_count": len(counts),
            "missing_atom_count": len(missing_atoms),
            "duplicate_atom_count": len(duplicate_atoms),
            "coverage_percent": coverage_percent,
            "truncated": False,
            "failure_codes": system_codes,
            "semantic_failure_count": len(semantic_failures),
        }
        if semantic_failures and complete:
            risk_rank = {"": 0, "LOW": 1, "MID": 2, "HIGH": 3}
            risk = max(
                (result.risk for result in semantic_failures),
                key=lambda value: risk_rank.get(value, 3),
                default="HIGH",
            )
            return ReviewResult(
                passed=False,
                reason="; ".join(dict.fromkeys(
                    result.reason for result in semantic_failures if result.reason
                )) or "semantic review failed",
                details=list(dict.fromkeys(
                    detail for result in semantic_failures for detail in result.details
                )),
                status=REVIEW_FAIL,
                risk=risk,
                violation_codes=list(dict.fromkeys(
                    code for result in semantic_failures for code in result.violation_codes
                )),
                reviewed_files=reviewed_files,
                coverage=coverage,
            )
        if complete and all(result.status == REVIEW_PASS for _, result in pairs):
            return ReviewResult(
                passed=True,
                reason="all review batches passed",
                status=REVIEW_PASS,
                reviewed_files=reviewed_files,
                coverage=coverage,
            )
        first_error = infrastructure[0] if infrastructure else None
        return ReviewResult(
            passed=False,
            reason=(first_error.reason if first_error else "review coverage incomplete"),
            details=list(dict.fromkeys(
                detail
                for result in (semantic_failures + infrastructure)
                for detail in result.details
            )),
            status=(first_error.status if first_error else REVIEW_ERROR),
            failure_code=(
                _review_infrastructure_code(first_error) if first_error
                else REVIEW_COVERAGE_PLAN_INVALID if not complete else ""
            ),
            violation_codes=list(dict.fromkeys(
                code for result in semantic_failures for code in result.violation_codes
            )),
            reviewed_files=reviewed_files,
            coverage=coverage,
        )

    def _plan_review_reuse(
        self,
        task: TaskState,
        plan: Any,
        review_identity: dict[str, str] | None,
    ) -> tuple[Any, Any, tuple[Any, ReviewResult] | None]:
        """Plan evidence-proportional review for a follow-up round.

        Reuses prior PASS units only when their exact proof inputs still match
        and the current review depth is LOW/MID (DIFF_ONLY/LOCAL).  Any other
        case conservatively reviews the full plan.
        """
        if not review_identity or not getattr(task, "retry_count", 0):
            return plan, None, None
        tier = str(dict(task.review_policy or {}).get("context_tier", ""))
        if tier not in review_evidence.REUSE_TIERS:
            return plan, None, None
        contract_revision = int(
            dict(task.materialized_execution or {}).get("contract_revision", 1) or 1
        )
        units = review_evidence.fresh_units(
            getattr(task, "review_evidence", None),
            acceptance_hash=str(review_identity.get("acceptance_hash", "")),
            contract_revision=contract_revision,
            execution_surface_hash=str(review_identity.get("execution_surface_hash", "")),
        )
        if not units:
            return plan, None, None
        try:
            partition = review_evidence.partition(plan, units)
            if not partition.enabled:
                return plan, None, None
            residual = review_evidence.residual_plan(plan, partition)
            if residual is None:
                return plan, None, None
            batch, reason = review_evidence.reused_batch(
                partition, plan.max_chars, plan.max_batches,
            )
        except Exception as exc:
            # Reuse planning is an optimization; any unexpected failure falls
            # back to the conservative full review of the same lossless plan.
            task.report_diagnostics.append({
                "code": "REVIEW_EVIDENCE_REUSE_PLANNING_FAILED",
                "diagnostic": type(exc).__name__,
            })
            return plan, None, None
        carrier = (batch, review_evidence.reused_result(batch, reason))
        merged = DiffBatchPlan(
            batches=((batch,) + tuple(residual.batches)),
            expected_atoms=plan.expected_atoms,
            assigned_atoms=plan.assigned_atoms,
            max_chars=plan.max_chars,
            max_batches=plan.max_batches,
        )
        return merged, partition, carrier

    def _record_review_evidence(
        self,
        task: TaskState,
        pairs: list[tuple[object, ReviewResult]],
        review_identity: dict[str, str] | None,
        partition: Any,
    ) -> None:
        """Persist this round's reusable PASS units (exact-input bound)."""
        if not review_identity or not task.materialized_execution:
            return
        try:
            task.review_evidence = review_evidence.record(
                pairs,
                acceptance_hash=str(review_identity.get("acceptance_hash", "")),
                contract_revision=int(
                    dict(task.materialized_execution or {}).get("contract_revision", 1) or 1
                ),
                execution_surface_hash=str(review_identity.get("execution_surface_hash", "")),
                round_no=int(getattr(task, "retry_count", 0)) + 1,
            )
        except Exception as exc:
            # Evidence reuse is an optimization over the gate, never a gate
            # input: a bookkeeping failure disables reuse and is recorded.
            task.review_evidence = {}
            task.report_diagnostics.append({
                "code": "REVIEW_EVIDENCE_RECORD_FAILED",
                "diagnostic": type(exc).__name__,
            })

    def _deterministic_oracle_review(
        self,
        task: TaskState,
        review_identity: dict[str, str],
    ) -> ReviewResult | None:
        """Evaluate the explicitly approved deterministic review-oracle contract.

        The separate LLM Reviewer is the default for every risk level.  It may
        be skipped only when the immutable Job test plan explicitly declares a
        deterministic oracle AND every typed binding condition holds for the
        current acceptance: effective review risk LOW without hard escalation
        flags, no pending QA hold, a current-scope oracle execution bound to
        this materialized execution surface, and fresh BUILD/TEST gate
        evidence bound to the current candidate/acceptance identity.  LOW risk
        alone, worker self-summaries, stale oracle evidence, or any missing
        condition keeps the normal LLM review.  A skip still leaves a REVIEW
        gate receipt with the bound oracle evidence.
        """
        from review_policy import LOW, REVIEW_MODE_DETERMINISTIC_ORACLE

        plan = dict(task.test_plan or {})
        if str(plan.get("review_oracle", "")).upper() != REVIEW_MODE_DETERMINISTIC_ORACLE:
            return None
        if plan.get("mandatory") is not True:
            return None
        policy = dict(task.review_policy or {})
        if str(policy.get("risk_level", "")) != LOW:
            return None
        if list(policy.get("hard_flags") or []):
            return None
        if not review_identity:
            return None
        if parse_qa_request(task.worker_stdout):
            return None
        if not task.changed_files or not task.git_diff.strip():
            return None
        if task.test_status != "PASS" or not task.official_tester_invoked:
            return None
        materialized = dict(task.materialized_execution or {})
        oracle_execution = dict(task.official_tester_execution or {})
        if (
            str(oracle_execution.get("exit_code", "")) != "0"
            or str(oracle_execution.get("test_scope_hash", ""))
            != str(plan.get("scope_hash", ""))
            or str(oracle_execution.get("execution_surface_hash", ""))
            != str(materialized.get("execution_surface_hash", ""))
        ):
            return None
        build = dict(task.build or {})
        gates = dict(task.gate_evidence or {})
        if (
            build.get("status") != "PASS"
            or dict(gates.get("BUILD") or {}).get("status") != "PASS"
            or dict(gates.get("TEST") or {}).get("status") != "PASS"
        ):
            return None
        stale = gate_evidence.validate(task, review_identity)
        if any(str(item).split(":", 1)[0] in {"BUILD", "TEST"} for item in stale):
            return None
        oracle_evidence = {
            "oracle_contract": "DETERMINISTIC_ORACLE",
            "test_scope_hash": str(plan.get("scope_hash", "")),
            "execution_surface_hash": str(review_identity.get("execution_surface_hash", "")),
            "acceptance_hash": str(review_identity.get("acceptance_hash", "")),
            "diff_hash": str(review_identity.get("diff_hash", "")),
            "build_gate": "PASS",
            "test_gate": "PASS",
            "official_tester_invoked": True,
            "test_status": "PASS",
            "risk_level": LOW,
        }
        oracle_sha256 = hashlib.sha256(json.dumps(
            oracle_evidence, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode("utf-8")).hexdigest()
        task.review_policy.update({
            "reviewer_required": False,
            "review_mode": REVIEW_MODE_DETERMINISTIC_ORACLE,
            "oracle_evidence_sha256": oracle_sha256,
        })
        return ReviewResult(
            passed=True,
            status=REVIEW_PASS,
            reason=(
                "LOW risk with an explicitly approved deterministic review-oracle "
                "contract: required Build/Test PASS on the current frozen test "
                "scope bound to this execution surface; separate LLM Reviewer "
                "skipped with 0 invocations"
            ),
            reviewed_files=list(task.changed_files),
            coverage={
                "review_mode": REVIEW_MODE_DETERMINISTIC_ORACLE,
                "reviewer_required": False,
                "reviewer_invocations": 0,
                "oracle_evidence_ref": oracle_evidence,
                "oracle_evidence_sha256": oracle_sha256,
                "missing_files": [],
                "coverage_percent": 100,
            },
        )

    def run_review_only(self, task: TaskState) -> ManagerResult:
        """Re-run only the isolated Reviewer for a preserved, built Task.

        This operator path intentionally has no Worker, Git collection, Build, or
        source-correction step.  It exists only for a prior Reviewer transport /
        schema failure after the immutable Task delta and structured Build/Test
        evidence were already captured.
        """
        if task.build.get("status") != "PASS" or task.build.get("success") is not True:
            raise RuntimeError("REVIEW_ONLY_BUILD_NOT_PASS")
        if task.test_required and task.test_status != "PASS":
            raise RuntimeError("REVIEW_ONLY_TEST_NOT_PASS")
        if not task.changed_files or not task.git_diff.strip():
            raise RuntimeError("REVIEW_ONLY_DIFF_MISSING")

        task.stage = "REVIEW"
        task.status = "RUNNING"
        task.review_policy = decision_from_task(task).to_dict()
        self._prepare_reviewer_effort(task)
        kwargs = {
            "requirement": task.requirement,
            "worker_success": True,
            "exit_code": 0,
            "changed_files": list(task.changed_files),
            "worker_stdout": task.worker_stdout,
            "git_diff": task.git_diff,
            "task": task,
            "attempt": max(1, task.retry_count + 1),
            "max_attempts": self.max_retry,
        }

        def invoke(**review_kwargs):
            started = datetime.now().astimezone()
            try:
                outcome = self.reviewer.review(**review_kwargs)
            except Exception as exc:
                task.reviewer_invocations.append({
                    "ordinal": len(task.reviewer_invocations) + 1,
                    "kind": "REVIEW_ONLY_OPERATOR",
                    "started_at": started.isoformat(),
                    "ended_at": datetime.now().astimezone().isoformat(),
                    "model": str(task.review_policy.get("model", "")),
                    "reasoning_effort": task.reviewer_effective_reasoning_effort,
                    "context_tier": str(task.review_policy.get("context_tier", "")),
                    "status": "ERROR",
                    "failure_reason": type(exc).__name__,
                    "source_mutation": False,
                })
                raise
            task.reviewer_invocations.append({
                "ordinal": len(task.reviewer_invocations) + 1,
                "kind": "REVIEW_ONLY_OPERATOR",
                "started_at": started.isoformat(),
                "ended_at": datetime.now().astimezone().isoformat(),
                "model": str(task.review_policy.get("model", "")),
                "reasoning_effort": task.reviewer_effective_reasoning_effort,
                "context_tier": str(task.review_policy.get("context_tier", "")),
                "status": str(outcome.status),
                "failure_code": str(outcome.failure_code or ""),
                "source_mutation": False,
            })
            return outcome

        if hasattr(self.reviewer, "plan_diff_batches"):
            plan = self.reviewer.plan_diff_batches(task.git_diff)
            if not plan.valid:
                review_result = self.reviewer.plan_failure_result(
                    plan, list(task.changed_files)
                )
            else:
                pairs: list[tuple[object, ReviewResult]] = []
                for batch in plan.batches:
                    batch_kwargs = dict(kwargs)
                    batch_kwargs.update({
                        "changed_files": list(batch.changed_files),
                        "git_diff": batch.text,
                        "review_batch": batch,
                        "all_changed_files": list(task.changed_files),
                        "batch_index": batch.index,
                        "batch_count": len(plan.batches),
                    })
                    result = invoke(**batch_kwargs)
                    pairs.append((batch, result))
                    if result.status in REVIEW_INFRA_STATUSES:
                        break
                review_result = self._aggregate_review_batches(
                    pairs, plan, list(task.changed_files)
                )
        else:
            review_result = invoke(**kwargs)
        Reviewer._apply_to_state(task, review_result)
        task.reviewed_task_files = list(dict.fromkeys(review_result.reviewed_files))
        task.review_coverage = dict(review_result.coverage or {})

        if review_result.status == REVIEW_PASS:
            baseline = self._initial_baseline
            if baseline is None:
                raise RuntimeError("REVIEW_ONLY_BASELINE_MISSING")
            eligible, blockers, hashes = self.collector.assess_commit_safety(task, baseline)
            task.changed_file_sha256 = hashes
            task.commit_blockers = list(dict.fromkeys(task.commit_blockers + blockers))
            task.commit_eligible = bool(eligible and not task.commit_blockers)
            if not task.commit_eligible:
                raise RuntimeError("REVIEW_ONLY_COMMIT_SAFETY_FAILED")
            task.status = "SUCCESS"
            task.stage = "DONE"
            task.verification_status = "VERIFIED"
            task.failure_stage = ""
            task.failure_code = ""
            task.failure_reason = ""
            task.failure_type = ""
            task.failure_origin = ""
            task.control_field = ""
            task.severity = "NONE"
            task.fix_scope = "RETRYABLE"
            return ManagerResult(
                success=True, review_result=review_result, message="PASS"
            )

        task.status = "AWAITING_QA"
        task.stage = "AWAITING_QA"
        task.failure_stage = "REVIEW"
        if review_result.status in REVIEW_INFRA_STATUSES:
            task.failure_code = _review_infrastructure_code(review_result)
            task.failure_type = "infrastructure"
            task.verification_status = "PARTIAL"
        else:
            task.failure_code = REVIEW_FAILED
            task.failure_type = "review"
            task.verification_status = "FAILED"
        task.failure_reason = scrub_secrets(review_result.reason or review_result.status)
        task.failure_origin = classify_failure_origin(
            task.failure_code,
            failure_stage=task.failure_stage,
            failure_type=task.failure_type,
        )
        task.fix_scope = "NON_RETRYABLE"
        task.worktree_disposition = "PRESERVED"
        return ManagerResult(
            success=False, review_result=review_result, message="FAIL"
        )

    def run_control_recovery(self, task: TaskState) -> ManagerResult:
        """Rebuild and review a preserved delta without Worker or Git regeneration."""
        if not task.changed_files or not task.git_diff.strip():
            raise RuntimeError("CONTROL_RECOVERY_DIFF_MISSING")
        before = {
            relative: hashlib.sha256(
                (Path(self.working_dir) / relative).read_bytes()
            ).hexdigest()
            for relative in sorted(set(task.changed_files))
            if (Path(self.working_dir) / relative).is_file()
        }
        if set(before) != set(task.changed_files):
            raise RuntimeError("CONTROL_RECOVERY_SOURCE_MISSING")
        recorded = dict(task.changed_file_sha256 or {})
        if recorded and recorded != before:
            raise RuntimeError("CONTROL_RECOVERY_SOURCE_HASH_STALE")

        task.status = "RUNNING"
        task.stage = "BUILD"
        self._log_stage(task, "BUILD", "control recovery build (Worker/Git skipped)")
        if self.configuration_guard is not None:
            self.configuration_guard()
        build_context = {
            "task_id": task.task_id,
            "baseline_id": task.active_baseline_id,
            "checkpoint_id": task.checkpoint_id,
            "restored_checkpoint_id": task.restored_checkpoint_id,
            "restored_evidence_epoch": task.restored_evidence_epoch,
        }
        try:
            result = self.build_test_runner.run(
                changed_files=list(task.changed_files),
                target_modules=list(task.target_module),
                evidence_context=build_context,
                authoritative_scope=task.scope_authority == "JOB_CONTRACT",
            )
        except TypeError as exc:
            if not any(
                name in str(exc) for name in ("evidence_context", "authoritative_scope")
            ):
                raise
            result = self.build_test_runner.run(
                changed_files=list(task.changed_files),
                target_modules=list(task.target_module),
            )
        evidence_ok, evidence_error = self._validate_structured_build_evidence(
            task, result
        )
        if result.build_status != "PASS" or result.success is not True or not evidence_ok:
            raise RuntimeError(evidence_error or "CONTROL_RECOVERY_BUILD_NOT_PASS")
        after = {
            relative: hashlib.sha256(
                (Path(self.working_dir) / relative).read_bytes()
            ).hexdigest()
            for relative in sorted(set(task.changed_files))
        }
        if after != before:
            raise RuntimeError("CONTROL_RECOVERY_BUILD_MUTATED_SOURCE")
        task.changed_file_sha256 = dict(before)
        details = {
            **dict(result.details),
            "structured_evidence_valid": True,
            "recovery_stage": "BUILD",
            "worker_skipped": True,
            "git_regeneration_skipped": True,
            "source_sha256": dict(before),
        }
        task.build = {
            "status": result.build_status,
            "success": bool(result.success),
            "output": scrub_secrets(result.build_output or ""),
            "details": details,
        }
        task.build_evidence = {
            "schema_version": 1,
            "status": result.build_status,
            "success": bool(result.success),
            "details": details,
            **build_context,
        }
        task.control_recovery_evidence = {
            "kind": "CONTROL_STATE_WRITE_RECOVERY",
            "source_sha256": dict(before),
            "build_reused": False,
            "build_rebuilt": True,
            "worker_invoked": False,
            "git_regenerated": False,
        }
        if self.configuration_guard is not None:
            self.configuration_guard()
        return self.run_review_only(task)

    def run_post_worker_recovery(self, task: TaskState) -> ManagerResult:
        """Resume Git/Build/Review after a successful Worker QA-sentinel parse fault."""
        evidence = dict(task.worker_execution_evidence or {})
        if (
            evidence.get("exit_code") != 0
            or evidence.get("timed_out") is True
            or parse_qa_request(task.worker_stdout)
        ):
            raise RuntimeError("POST_WORKER_RECOVERY_EVIDENCE_INVALID")
        baseline = self._initial_baseline
        if baseline is None:
            raise RuntimeError("POST_WORKER_RECOVERY_BASELINE_MISSING")
        if self.configuration_guard is not None:
            self.configuration_guard()
        git_result = self.collector.collect(task=task, baseline=baseline)
        if not git_result.success:
            raise RuntimeError("POST_WORKER_RECOVERY_GIT_FAILED")
        raw_delta = list(task.changed_files)
        meaningful = [
            path for path in raw_delta
            if not _is_non_source_artifact(path, self.profile)
        ]
        if not meaningful:
            raise RuntimeError("POST_WORKER_RECOVERY_DIFF_MISSING")
        task.task_owned_changed_files = list(meaningful)
        task.changed_files = list(meaningful)
        task.scoped_rollback_targets = list(raw_delta)
        task.no_diff_gate = {
            "applied": True,
            "passed": True,
            "raw_task_delta_files": list(raw_delta),
            "non_source_artifact_files": [p for p in raw_delta if p not in meaningful],
            "meaningful_source_files": list(meaningful),
        }
        if hasattr(self.collector, "collect_selected"):
            git_result = self.collector.collect_selected(task, baseline, meaningful)
            if not git_result.success:
                raise RuntimeError("POST_WORKER_RECOVERY_GIT_FAILED")
        eol_result = self.collector.repair_eol_to_index(task, baseline)
        task.eol_repairs = list(eol_result.repaired_files)
        task.eol_repair_diagnostics = list(
            getattr(eol_result, "diagnostics", []) or []
        )
        if not eol_result.success:
            raise RuntimeError(eol_result.error or "POST_WORKER_RECOVERY_EOL_FAILED")
        if eol_result.repaired_files:
            git_result = self.collector.collect_selected(task, baseline, meaningful)
            if not git_result.success:
                raise RuntimeError("POST_WORKER_RECOVERY_GIT_FAILED")
        if self.profile is not None:
            codes, _details = deterministic_violations(
                task.changed_files,
                task.git_diff,
                self.profile,
                task.target_module,
                gate_evidence.allowed_scope(task),
            )
            if codes:
                raise RuntimeError("POST_WORKER_RECOVERY_SCOPE_INVALID")
        task.completion_kind = "CHANGED"
        task.qa_request = {}
        task.qa_type = ""
        task.hold_scope = ""
        task.failure_stage = ""
        task.failure_code = ""
        task.failure_reason = ""
        task.failure_origin = ""
        task.failure_type = ""
        task.severity = "NONE"
        task.worktree_disposition = "PRESERVED"
        result = self.run_control_recovery(task)
        task.control_recovery_evidence.update({
            "kind": "POST_WORKER_QA_PARSE_RECOVERY",
            "worker_invoked": False,
            "worker_result_reused": True,
            "git_regenerated": True,
        })
        return result

    def run_integration_recovery(self, task: TaskState) -> ManagerResult:
        """Re-evaluate EOL and commit safety without Worker or gate reruns."""
        if (
            task.build.get("status") != "PASS"
            or (task.test_required and task.test_status != "PASS")
            or task.review_status != REVIEW_PASS
            or task.verification_status != "VERIFIED"
        ):
            raise RuntimeError("INTEGRATION_RECOVERY_EVIDENCE_NOT_FRESH")
        baseline = self._initial_baseline
        if baseline is None:
            raise RuntimeError("INTEGRATION_RECOVERY_BASELINE_MISSING")
        eol = self.collector.inspect_eol_to_index(task, baseline)
        if not eol.get("success"):
            raise RuntimeError("INTEGRATION_RECOVERY_EOL_MISMATCH")
        eligible, blockers, hashes = self.collector.assess_commit_safety(task, baseline)
        if task.changed_file_sha256 and hashes != task.changed_file_sha256:
            raise RuntimeError("INTEGRATION_RECOVERY_SOURCE_HASH_STALE")
        if blockers or not eligible:
            raise RuntimeError("INTEGRATION_RECOVERY_COMMIT_SAFETY_FAILED")
        if task.commit_blockers:
            task.commit_blocker_history.append({
                "attempt": max(1, task.retry_count + 1),
                "blockers": list(task.commit_blockers),
                "eol_repair_diagnostics": list(task.eol_repair_diagnostics),
                "resolution": "WORKER_FREE_INTEGRATION_RECOVERY",
            })
        task.commit_blockers = []
        task.commit_eligible = True
        task.changed_file_sha256 = hashes
        task.status = "SUCCESS"
        task.stage = "DONE"
        task.failure_stage = ""
        task.failure_code = ""
        task.failure_reason = ""
        task.failure_type = ""
        task.failure_origin = ""
        task.user_intervention_reason = ""
        task.control_recovery_evidence = {
            "kind": "WORKER_FREE_INTEGRATION_RECOVERY",
            "worker_invoked": False,
            "source_regenerated": False,
            "build_reused": True,
            "review_reused": True,
            "verification_reused": True,
            "source_sha256": dict(hashes),
            "eol": dict(eol),
        }
        return ManagerResult(success=True, message="PASS")

    def _worker_scope(self, modules: list[str]) -> list[str]:
        """
        단일 작업이 점유할 락 경로 범위.
        - 모듈 리스트가 있으면 working_dir/{module} 디렉토리 각각.
        - 없으면 working_dir 루트 전체.
        동일 모듈을 건드리는 두 백그라운드 작업이 동시에 실행되지 않도록 한다.
        """
        if modules:
            return [str(Path(self.working_dir) / m) for m in modules]
        return [self.working_dir] if self.working_dir else []

    @staticmethod
    def _wtag(task: TaskState) -> str:
        """병렬 슬롯 태그: worker_no>0 이면 '[Worker1] '. 없으면 ''."""
        n = getattr(task, "worker_no", 0) or 0
        return f"[Worker{n}] " if n > 0 else ""

    def _log_stage(self, task: TaskState, stage: str, note: str = "") -> None:
        """단계 전환 한 줄 로그 (task_id / 시도 / 단계). 콘솔 침묵 방지용.
        worker_no 가 있으면 [Worker1] 태그로 병렬 작업을 식별한다."""
        attempt = min(task.retry_count + 1, self.max_retry)
        tag = f"[Worker{task.worker_no}]" if getattr(task, "worker_no", 0) else ""
        line = f"{tag}[{task.task_id}] ({attempt}/{self.max_retry}) ▶ {stage}"
        if note:
            line += f" — {note}"
        snapshot = meaningful_progress_update(
            task,
            stage=stage,
            event_type="STAGE_CHANGED",
            summary=note or stage,
        )
        task.save(self.task_root)
        print(line, flush=True)
        task.notification_evidence.append(self._notify_progress(task, snapshot))

    def _notify_progress(self, task, snapshot, *, heartbeat=False):
        try:
            if task.job_id and str(task.status) != "RUNNING":
                return {"terminal_projection_owned_by_supervisor": True, "delivered": False}
            if heartbeat and (str(task.status) != "RUNNING" or
                    dict(snapshot.get("execution_health") or {}).get("runtime_health") != "VALID"):
                return {"heartbeat_suppressed": True, "delivered": False}
            title = "Harness heartbeat" if heartbeat else "Harness progress"
            message = render_progress(snapshot, "HEARTBEAT" if heartbeat else "TELEGRAM")
            database = self.task_root.parent / ".control" / "control.sqlite3"
            if database.is_file():
                from control_repository import ControlRepository
                from operator_notifications import deliver_semantic, progress_fields
                projection = {**progress_fields(snapshot), "message": message}
                return deliver_semantic(projection, ControlRepository(database),
                    lambda text: send_notify_evidence(text, title=title), heartbeat=heartbeat)
            return send_notify_evidence(message, title=title)
        except Exception as exc:
            return {"delivered": False, "failure_origin": "NOTIFICATION",
                    "failure_code": "NOTIFICATION_PROJECTION_ERROR", "diagnostic": type(exc).__name__}

    def _runtime_event_callback(self, task: TaskState) -> Callable[[dict], None]:
        """Persist the existing subprocess owner's lease and progress evidence."""

        def record(event: dict) -> None:
            delivery_needed = False
            snapshot_for_delivery: dict = {}
            with self._state_lock:
                event_type = str(event.get("event", ""))
                execution_id = str(event.get("execution_id", ""))
                if task.control_repository_path and task.attempt_id:
                    from control_repository import ControlRepository
                    repository = ControlRepository(task.control_repository_path)
                    event_attempt_id = str(event.get("attempt_id") or task.attempt_id)
                    if event_type == "STARTED":
                        event_role = str(event.get("execution_role") or "WORKER").upper()
                        process = repository.record_runtime_process(
                            event_attempt_id,
                            runtime=str(event.get("runtime") or task.actual_worker or task.worker),
                            pid=int(event.get("pid", 0) or 0),
                            process_identity=(
                                f"{event.get('pid', 0)}:{event.get('started_at', '')}:"
                                f"{task.command_id or event.get('execution_id', '')}"
                            ),
                            binding_id=(
                                task.reviewer_native_session_binding_id
                                if event_role == "REVIEWER"
                                else task.native_session_binding_id
                            ),
                        )
                        task.runtime_process_id = process["process_id"]
                    elif event_type == "ENDED" and task.runtime_process_id:
                        state = "EXITED" if event.get("termination_reason") in {"EXITED", "NONZERO_EXIT"} else "KILLED"
                        repository.finish_runtime_process(
                            task.runtime_process_id,
                            state=state,
                            exit_code=event.get("exit_code"),
                        )
                current = dict(task.execution_runtime or {})
                if event_type == "STARTED" or current.get("execution_id") != execution_id:
                    current = {}
                current.update({
                    key: value for key, value in event.items()
                    if key in {
                        "execution_id", "execution_owner", "execution_role", "pid",
                        "started_at", "last_runtime_heartbeat_at", "lease_expires_at",
                        "last_progress_at", "ended_at", "exit_code",
                        "termination_reason", "process_alive",
                        "progress_timeout_seconds",
                    }
                })
                if event_type == "STARTED":
                    for key in ("ended_at", "exit_code", "termination_reason"):
                        current.pop(key, None)
                current["last_event"] = event_type
                current["last_event_at"] = str(event.get("observed_at", ""))
                task.execution_runtime = current

                if event_type == "ENDED":
                    history = list(task.execution_runtime_history or [])
                    history = [
                        item for item in history
                        if str(item.get("execution_id", "")) != execution_id
                    ]
                    history.append(dict(current))
                    task.execution_runtime_history = history

                snapshot = dict(task.progress_snapshot or {})
                previous_health = dict(snapshot.get("execution_health") or {})
                current_health = project_execution_health(task)
                snapshot["execution_health"] = current_health
                semantic_keys = (
                    "runtime_health", "progress_health", "failure_code",
                    "reconciliation_required",
                )
                health_changed = any(
                    previous_health.get(key) != current_health.get(key)
                    for key in semantic_keys
                )
                if health_changed:
                    task.progress_revision = int(task.progress_revision) + 1
                    snapshot["progress_revision"] = task.progress_revision
                    now_text = str(event.get("observed_at", ""))
                    snapshot["last_meaningful_event"] = {
                        "type": "EXECUTION_HEALTH_CHANGED",
                        "at": now_text,
                        "summary": (
                            f"runtime={current_health.get('runtime_health', '')};"
                            f"progress={current_health.get('progress_health', '')}"
                        ),
                    }
                    task.progress_events = (
                        list(task.progress_events or [])
                        + [{
                            "progress_revision": task.progress_revision,
                            **snapshot["last_meaningful_event"],
                        }]
                    )
                task.progress_snapshot = snapshot

                last_delivery = ""
                if task.notification_evidence:
                    last_delivery = str(
                        dict(task.notification_evidence[-1]).get("attempted_at", "")
                    )
                heartbeat_due = False
                if last_delivery:
                    try:
                        heartbeat_due = (
                            datetime.now().astimezone()
                            - datetime.fromisoformat(last_delivery)
                        ).total_seconds() >= HEARTBEAT_SECONDS
                    except ValueError:
                        heartbeat_due = False
                delivery_needed = health_changed or heartbeat_due
                snapshot_for_delivery = dict(snapshot)
                task.save(self.task_root)

            if delivery_needed:
                evidence = self._notify_progress(task, snapshot_for_delivery,
                    heartbeat=str(event.get("event", "")) == "HEARTBEAT")
                with self._state_lock:
                    task.notification_evidence.append(evidence)

        return record

    # ------------------------------------------------------------------
    # 파이프라인
    # ------------------------------------------------------------------
    def _run_pipeline(self, task: TaskState) -> ManagerResult:
        """Worker(단일) → Git(모듈별) → Build/Test → Review 실제 파이프라인 (단일 Task)."""

        # 0. 작업 모드 확정: task.task_mode 명시 우선, 미지정 시 키워드 추정.
        #    이후 모든 게이트(diff/git/build/review)는 task_mode 기준으로 동작한다.
        task.task_mode = resolve_task_mode(task.requirement, task.task_mode)
        task._runtime_event_callback = self._runtime_event_callback(task)
        is_modification = task.task_mode == TASK_MODE_MODIFICATION
        if not task.original_worker:
            task.original_worker = task.worker or self.worker_type
        if not task.original_model:
            task.original_model = (
                _task_model(task, task.original_worker)
            ) or (self.codex_model if task.original_worker == "codex" else self.opencode_model if task.original_worker == "opencode" else self.model) or "default"
        if self.profile is not None and hasattr(self.profile, "modules"):
            task.profile_build_commands = [
                {
                    "module": module.name,
                    "cwd": module.build_cwd,
                    "argv": list(module.build_argv),
                }
                for module in self.profile.modules
                if module.name in task.target_module and module.build_argv
            ]
        task.preserved_predecessor_delta_files = list(task.inherited_batch_delta_files)
        print(f"[{task.task_id}] 작업 모드: {task.task_mode}")

        configuration_failure = self._configuration_failure(task)
        if configuration_failure is not None:
            return configuration_failure
        routing_failure = self._worker_routing_failure(task)
        if routing_failure is not None:
            return routing_failure

        if task.task_id not in self._task_baselines:
            baseline = self._initial_baseline
            self._initial_baseline = None
            error = ""
            if baseline is not None:
                preserved = self._verify_preserved_source_baseline(task, baseline)
                if preserved is None:
                    verified, error = self.collector.verify_baseline_current(baseline)
                else:
                    verified, error = preserved
                if not verified:
                    task.failure_stage = "GIT"
                    task.failure_code = "PRE_JOB_SNAPSHOT_HASH_MISMATCH"
                    task.failure_reason = error
                    task.failure_type = "infrastructure"
                    task.fix_scope = "NON_RETRYABLE"
                    task.verification_status = "PARTIAL"
                    task.status = "AWAITING_QA"
                    task.stage = "AWAITING_QA"
                    return ManagerResult(success=False, message=error)
            else:
                baseline, error = self.collector.capture_baseline(task)
            if baseline is None:
                task.failure_stage = "GIT"
                task.failure_reason = error
                return ManagerResult(success=False, message=error)
            self._task_baselines[task.task_id] = baseline

        # A resolved QA candidate is intentional input for this Task.  Its base
        # must be verified first, then the candidate must be applied before the
        # Worker runs so Git/Build/Review see the complete Task-owned delta.
        if self.initial_delta_applier is not None:
            applier = self.initial_delta_applier
            self.initial_delta_applier = None
            try:
                applier()
            except Exception as exc:
                task.failure_stage = "CHECKPOINT"
                task.failure_code = "QA_CANDIDATE_APPLY_FAILED"
                task.failure_reason = type(exc).__name__
                task.failure_origin = "HARNESS_CAUSED"
                task.failure_type = "infrastructure"
                task.fix_scope = "NON_RETRYABLE"
                task.verification_status = "PARTIAL"
                task.status, task.stage = "AWAITING_QA", "AWAITING_QA"
                task.worktree_disposition = "PRESERVED"
                return ManagerResult(False, message=task.failure_code)

        if self.restore_checkpoint_manifest and not task.restored_for_retry:
            if self.checkpoint_store is None:
                task.failure_code = "CHECKPOINT_STORE_UNAVAILABLE"
                task.status, task.stage = "AWAITING_QA", "AWAITING_QA"
                task.failure_type, task.fix_scope = "infrastructure", "NON_RETRYABLE"
                return ManagerResult(False, message=task.failure_code)
            try:
                restored = self.checkpoint_store.restore_for_retry(
                    task,
                    self.restore_checkpoint_manifest,
                    source_job_id=self.restore_checkpoint_source_job_id,
                    source_client_job_id=self.restore_checkpoint_source_client_job_id,
                )
            except AttemptCheckpointError as exc:
                task.failure_stage = "CHECKPOINT"
                task.failure_code = exc.code
                task.failure_reason = exc.code
                task.failure_type = "infrastructure"
                task.fix_scope = "NON_RETRYABLE"
                task.verification_status = "PARTIAL"
                task.status, task.stage = "AWAITING_QA", "AWAITING_QA"
                task.worktree_disposition = "NO_DELTA"
                task.save(self.task_root)
                return ManagerResult(False, message=exc.code)
            self._prepare_restored_evidence_epoch(
                task, restored["checkpoint_id"], list(restored["files"])
            )
            findings = str(restored.get("review_findings", "")).strip()
            task.handoff_context = (
                "[RESTORED_FOR_RETRY] 이전 checkpoint의 구현을 시작점으로 사용한다. "
                "전체 구현을 다시 쓰지 말고 이전 Review finding만 최소 수정한다.\n"
                f"복원 파일: {', '.join(restored['files'])}\n"
                + (f"이전 Review findings:\n{findings[:3000]}" if findings else "")
            )
            self.restore_checkpoint_manifest = ""
            task.save(self.task_root)

        configuration_failure = self._configuration_failure(task)
        if configuration_failure is not None:
            return configuration_failure

        # 0.5 플래너 (선택): Worker 실행 전 3종 브리프 생성.
        #     실패해도 작업을 계속한다(원 요구사항 그대로 진행).
        if task.use_planner:
            print(f"[{task.task_id}] 플래너 사용: 브리프 생성 시도")
            configuration_failure = self._configuration_failure(task)
            if configuration_failure is not None:
                return configuration_failure
            task._control_external_mutation_possible = True
            try:
                task_root = getattr(self, "task_root", None)
                if task_root is not None:
                    task._context_manifest_root = Path(task_root) / "context-manifests"
                run_planner(
                    task,
                    self.working_dir,
                    profile=self.profile,
                    policy_root=self.policy_root,
                )
            except Exception as e:
                print(f"[{task.task_id}][Planner] 예외(무시): {e}")
            configuration_failure = self._configuration_failure(task)
            if configuration_failure is not None:
                return configuration_failure
        else:
            print(f"[{task.task_id}] 플래너 미사용")

        # 1. 단일 워커 실행. Job 범위 락은 run_with_retry 전체에서 유지된다.
        task.stage = "WORKING"
        task.requested_worker = task.requested_worker or task.original_worker or task.worker
        task.requested_model = task.requested_model or task.original_model or (
            _task_model(task, task.requested_worker)
        ) or "default"
        task.planned_worker = self.choose_worker(task)
        task.planned_model = task.retry_model or (
            _task_model(task, task.planned_worker)
        ) or "default"
        task.worker = task.planned_worker
        task.selected_worker = task.planned_worker
        task.selected_model = task.planned_model
        task.selection_reason = (
            task.retry_strategy
            if task.retry_worker
            else droid_model_selection_reason(task.requested_model, task.planned_model)
        )
        task.worker_results = []   # 결과 초기화
        routing_failure = self._worker_routing_failure(task)
        if routing_failure is not None:
            return routing_failure
        task.save(self.task_root)
        self._log_stage(
            task, "WORKING",
            f"워커 {task.worker} 실행 "
            f"(모듈: {', '.join(task.target_module) if task.target_module else '전체'})",
        )

        job_id = f"{task.worker}-{task.task_id}"

        worker_results: list[WorkerResult] = []
        worker, actual_worker = self._get_worker(task)
        task.actual_worker = actual_worker
        task.actual_model = task.retry_model or (
            _task_model(task, actual_worker)
        ) or "default"
        routing_failure = self._worker_routing_failure(task)
        if routing_failure is not None:
            return routing_failure
        configuration_failure = self._configuration_failure(task)
        if configuration_failure is not None:
            return configuration_failure
        task._control_external_mutation_possible = True
        task._control_worker_started = True
        wr = worker.execute(task.requirement, self.working_dir, task=task)
        worker_command = list(getattr(wr, "command", []) or [])
        task.actual_executable = getattr(wr, "actual_executable", "") or (
            str(worker_command[0]) if worker_command else ""
        )
        task.actual_invocation_id = getattr(wr, "actual_invocation_id", "")
        configuration_failure = self._configuration_failure(task)
        if configuration_failure is not None:
            return configuration_failure

        with self._state_lock:
            task.worker_results.append({
                "worker_id": job_id,
                "target_module": ", ".join(task.target_module) if task.target_module else "-",
                "success": wr.success,
                "exit_code": wr.exit_code,
                "stdout": scrub_secrets(wr.stdout or "")[:4000],
                "stderr": scrub_secrets(wr.stderr or "")[:1000],
                "requested_worker": task.requested_worker,
                "requested_model": task.requested_model,
                "planned_worker": task.planned_worker,
                "planned_model": task.planned_model,
                "selected_worker": task.selected_worker,
                "selected_model": task.selected_model,
                "selection_reason": task.selection_reason,
                "actual_worker": task.actual_worker,
                "actual_model": task.actual_model,
                "actual_executable": task.actual_executable,
                "actual_invocation_id": task.actual_invocation_id,
            })
        worker_results.append(wr)

        if getattr(wr, "execution_failure_code", "") in CODEX_IPC_FAILURES:
            task.failure_code = wr.execution_failure_code
            task.failure_reason = wr.execution_failure_code
            task.failure_stage = "WORKER"
            task.failure_origin = "HARNESS_CAUSED"
            task.failure_type = "infrastructure"
            task.fix_scope = "NON_RETRYABLE"
            task.verification_status = "PARTIAL"
            task.status, task.stage = "AWAITING_QA", "AWAITING_QA"
            task.worktree_disposition = "PRESERVED"
            task.keep_working_tree = True
            task.commit_eligible = False
            task.retry_domain = (
                "TECHNICAL_EXECUTION_RETRY"
                if wr.execution_failure_code == "NATIVE_SESSION_RESUME_UNAVAILABLE"
                else "RUNTIME_RECOVERY"
            )
            if task.retry_domain == "TECHNICAL_EXECUTION_RETRY":
                task.technical_execution_retry_count += 1
            from selective_revalidation import plan_revalidation
            task.selective_revalidation = plan_revalidation(
                {"runtime_process_id": task.runtime_process_id, "runtime_health": "FAILED"},
                {"runtime_process_id": "RECOVERY", "runtime_health": "RECONCILE"},
            ).public()
            return ManagerResult(False, worker_result=wr, message=task.failure_code)

        worker_success = wr.success
        with self._state_lock:
            task.exit_code = wr.exit_code
            task.worker_stdout = scrub_secrets(wr.stdout or "")
            task.worker_stderr = scrub_secrets(wr.stderr or "")
            task.qa_request = dict(getattr(wr, "qa_request", {}) or {})
            if task.qa_request.get("required") is True:
                task.qa_type = str(task.qa_request.get("qa_type", ""))
                task.hold_scope = str(task.qa_request.get("hold_scope", ""))
            collect_task_artifacts(task)
        if task.artifact_files:
            print(
                f"[{task.task_id}][Artifact] Job 산출물: "
                f"{', '.join(task.artifact_files)}"
            )
        # 타임아웃 발생 안내 (부분 변경사항이 있으면 Review까지 진행)
        if getattr(wr, "timed_out", False):
            print(f"[{task.task_id}] ⏱ 워커 타임아웃 — 부분 변경사항이 있으면 Review 진행")
        # 주의: failure_stage 는 git 수집 후에 확정한다.
        # exit_code != 0 이더라도 실제로 파일을 변경했으면 "의미 있는 작업" 으로 간주하기 때문.

        # 1.5 워커 종료 중간 요약 (GIT 수행 전 — 첫 결과까지의 침묵 방지)
        self._log_stage(
            task, "WORKER-DONE",
            note=(
                f"exit_code={wr.exit_code}, "
                f"timed_out={getattr(wr, 'timed_out', False)}, "
                f"변경 파일 수(GIT 수집 전)={len(task.changed_files or [])}"
                f" → 다음 단계: GIT → BUILD → REVIEW"
            ),
        )

        # 2. Git Diff (모듈별 개별 저장소 수집, 반드시 워커 종료 후)
        task.stage = "GIT"
        self._log_stage(task, "GIT", "모듈별 diff 수집")
        configuration_failure = self._configuration_failure(task)
        if configuration_failure is not None:
            return configuration_failure
        git_result = self.collector.collect(
            task=task, baseline=self._task_baselines[task.task_id]
        )
        raw_task_delta = list(task.changed_files)
        meaningful_source = [
            path for path in raw_task_delta
            if not _is_non_source_artifact(path, self.profile)
        ]
        planning_only = [
            path for path in raw_task_delta if path not in meaningful_source
        ]
        task.task_owned_changed_files = list(meaningful_source)
        task.changed_files = list(meaningful_source)
        from qa_policy_resolution import exempt_qa_signal
        import skeleton_policy
        if exempt_qa_signal(task) and not skeleton_policy.enabled(task):
            task.qa_request = {}
            task.qa_type = ''
            task.hold_scope = ''
            if not meaningful_source:
                # A Worker stopping for a forbidden gate is a policy execution
                # error, not product no-delta and never a product retry.
                task.failure_stage = 'CONTROL'
                task.failure_code = 'POLICY_EXEMPT_GATE_STOPPED_EXECUTION'
                task.failure_reason = 'DB_CONNECT/E2E are NOT_RUN_BY_POLICY; implementation remains unverified'
                task.failure_type = 'infrastructure'
                task.failure_origin = 'HARNESS_CAUSED'
                task.fix_scope = 'NON_RETRYABLE'
                task.verification_status = 'PARTIAL'
                return ManagerResult(success=False, worker_result=wr, git_result=git_result,
                                     message=task.failure_code)
        if (
            task.qa_request.get("required") is True
            and task.qa_type in {"BUSINESS_CONTRACT", "API_CONTRACT", "DB_CONTRACT"}
            and meaningful_source
            and task.qa_request.get("decision_independent_work_complete") is not True
        ):
            task.failure_stage = "QA_SIGNAL"
            task.failure_code = "QA_DECISION_DEPENDENT_MUTATION_FORBIDDEN"
            task.failure_reason = str(task.qa_request.get("reason", ""))
            task.failure_type = "infrastructure"
            task.failure_origin = "HARNESS_CAUSED"
            task.fix_scope = "NON_RETRYABLE"
            task.qa_type = "SAFETY_INTEGRITY"
            task.hold_scope = "BATCH"
            task.verification_status = "PARTIAL"
            return ManagerResult(success=False, worker_result=wr, git_result=git_result,
                                 message=task.failure_code)
        task.scoped_rollback_targets = list(raw_task_delta)
        task.no_diff_gate = {
            "applied": bool(is_modification),
            "passed": bool(meaningful_source) if is_modification else True,
            "raw_task_delta_files": raw_task_delta,
            "non_source_artifact_files": planning_only,
            "meaningful_source_files": meaningful_source,
        }
        if git_result.success and not raw_task_delta and hasattr(
            self.collector, "prove_no_task_delta"
        ):
            task.no_task_delta_evidence = dict(
                self.collector.prove_no_task_delta(
                    self._task_baselines.get(task.task_id)
                )
            )
        elif raw_task_delta:
            task.no_task_delta_evidence = {
                "schema_version": 1,
                "kind": "VERIFIED_NO_TASK_DELTA",
                "passed": False,
                "failure_code": "TASK_DELTA_PRESENT",
                "raw_task_delta_files": list(raw_task_delta),
            }
        if (task.qa_request.get('required') is True
                and task.qa_type in {'BUSINESS_CONTRACT', 'API_CONTRACT', 'DB_CONTRACT', 'HUMAN_ACCEPTANCE'}
                and not skeleton_policy.deferred_signal(task)):
            task.failure_stage = 'QA_SIGNAL'
            task.failure_code = 'CONTRACT_UNRESOLVED'
            task.failure_reason = str(task.qa_request.get('reason', ''))
            task.failure_origin = 'USER_QA'
            task.failure_type = 'contract'
            task.fix_scope = 'NON_RETRYABLE'
            task.status = task.stage = 'AWAITING_QA'
            task.verification_status = 'PARTIAL'
            task.worktree_disposition = 'PRESERVED' if meaningful_source else 'NO_DELTA'
            task.keep_working_tree = bool(meaningful_source)
            task.commit_eligible = False
            return ManagerResult(success=False, worker_result=wr, git_result=git_result,
                                 message=task.failure_code)
        patch_failure_code, diagnostic = _worker_patch_failure(task)
        if patch_failure_code:
            if diagnostic not in task.planning_artifact_errors:
                task.planning_artifact_errors.append(diagnostic)
            evidence = dict(task.worker_execution_evidence or {})
            diagnostic_record = write_patch_diagnostic(
                Path(self.task_root) / "patch-diagnostics" / task.task_id,
                {
                    "invocation_id": task.actual_invocation_id,
                    "worker": task.actual_worker or task.worker,
                    "model": task.actual_model or task.retry_model or task.codex_model or task.droid_model,
                    "task_id": task.task_id,
                    "execution_attempt": task.execution_attempt or task.outer_attempt,
                    "internal_retry": task.retry_count,
                    "target_path": str(evidence.get("target_path", "")),
                    "stdout": task.worker_stdout,
                    "stderr": task.worker_stderr,
                    "patch_payload_sha256": str(evidence.get("patch_payload_sha256", "")),
                    "pre_read_sha256": str(evidence.get("pre_read_sha256", "")),
                    "pre_apply_sha256": str(evidence.get("pre_apply_sha256", "")),
                    "post_failure_sha256": str(evidence.get("post_failure_sha256", "")),
                    "snapshot_id": task.pre_job_snapshot_id,
                    "baseline_id": task.active_baseline_id,
                    "failure_code": patch_failure_code,
                    "failure_fingerprint": task.failure_fingerprint,
                },
            )
            task.patch_diagnostics = diagnostic_record
        if (
            is_modification
            and self.profile is not None
            and git_result.success
            and not meaningful_source
            and not is_worker_approval_wait_response(task.worker_stdout)
            and not (skeleton_policy.deferred_signal(task)
                and task.no_task_delta_evidence.get('passed') is True
                and can_verify_current_state(task.requirement, task.worker_stdout,
                    self.profile.module_names if self.profile else None))
        ):
            task.git_status = ""
            task.git_diff_stat = ""
            task.git_diff = ""
            task.failure_stage = "SOURCE_DELTA_GATE"
            task.failure_code = patch_failure_code or NO_MEANINGFUL_SOURCE_CHANGE
            task.failure_reason = (
                f"{task.failure_code}: {diagnostic or 'application source delta is empty'}"
                + (f"; non-source artifacts={','.join(planning_only)}" if planning_only else "")
            )
            task.failure_type = "technical"
            task.severity = "MID"
            task.fix_scope = "RETRYABLE"
            task.verification_status = "FAILED"
            task.build = {"status": "SKIPPED", "success": False, "output": "source delta gate"}
            task.test = {"status": "SKIPPED", "output": "source delta gate"}
            task.test_status = "SKIPPED"
            task.review_status = "SKIPPED"
            task.review_result = "Build/Review not invoked: source delta gate failed"
            task.review_coverage = {
                "current_task_changed_files": [],
                "reviewed_task_files": [],
                "inspected_unchanged_target_files": [],
                "inherited_batch_delta_files": list(task.inherited_batch_delta_files),
                "missing_files": [],
                "failure_code": task.failure_code,
                "secondary_gate": NO_MEANINGFUL_SOURCE_CHANGE,
            }
            task.no_diff_gate["secondary_gate"] = NO_MEANINGFUL_SOURCE_CHANGE
            return ManagerResult(
                success=False,
                worker_result=wr,
                git_result=git_result,
                message=task.failure_code,
            )
        if git_result.success and hasattr(self.collector, "collect_selected"):
            git_result = self.collector.collect_selected(
                task, self._task_baselines[task.task_id], meaningful_source
            )
        elif git_result.success:
            git_result.changed_files = list(meaningful_source)
            task.changed_files = list(meaningful_source)
            task.task_owned_changed_files = list(meaningful_source)
        if patch_failure_code:
            task.failure_stage = "WORKER_TOOL"
            task.failure_code = patch_failure_code
            task.failure_reason = diagnostic or patch_failure_code
            task.failure_type = "technical"
            task.severity = "MID"
            task.fix_scope = "RETRYABLE"
            task.verification_status = "FAILED"
            task.no_diff_gate["secondary_gate"] = (
                "SOURCE_DELTA_PRESENT" if meaningful_source else NO_MEANINGFUL_SOURCE_CHANGE
            )
            return ManagerResult(
                success=False, worker_result=wr, git_result=git_result,
                message=patch_failure_code,
            )
        if git_result.success and task.changed_files:
            eol_result = self.collector.repair_eol_to_index(
                task, self._task_baselines[task.task_id]
            )
            task.eol_repairs = list(eol_result.repaired_files)
            task.eol_repair_diagnostics = list(
                getattr(eol_result, "diagnostics", []) or []
            )
            if eol_result.repaired_files:
                print(
                    f"[{task.task_id}][Git] index EOL 자동 복구: "
                    f"{', '.join(eol_result.repaired_files)}"
                )
                # Rebuild the exact Job delta after byte-only normalization so
                # Build and Reviewer consume the repaired representation.
                git_result = self.collector.collect(
                    task=task, baseline=self._task_baselines[task.task_id]
                )
                if hasattr(self.collector, "collect_selected"):
                    git_result = self.collector.collect_selected(
                        task, self._task_baselines[task.task_id], meaningful_source
                    )
            if not eol_result.success:
                task.commit_blockers.append(eol_result.error or "EOL_REPAIR_FAILED")
        configuration_failure = self._configuration_failure(task)
        if configuration_failure is not None:
            return configuration_failure

        # profile의 로컬 개발 설정은 게이트/리뷰 입력에서 이미 제외됨.
        if task.excluded_files:
            print(f"[{task.task_id}][Git] 로컬 개발 설정 제외(게이트/리뷰 무시): "
                  f"{', '.join(task.excluded_files)}")

        # git 게이트: MODIFICATION 은 git 수집 필수. ANALYSIS 는 허용(기존 동작).
        if not git_result.success:
            if is_modification:
                task.failure_stage = "GIT"
                task.failure_reason = git_result.error or "git diff 수집 실패"

        # 실제 source delta와 검증 완료는 별개의 사실이다. 이후 build/review가
        # 실패해도 변경이 있었다는 기록은 보존한다.
        if is_modification and task.changed_files:
            task.completion_kind = "CHANGED"

        if git_result.success and self.profile is not None:
            policy_codes, policy_details = deterministic_violations(
                task.changed_files,
                task.git_diff,
                self.profile,
                task.target_module,
                gate_evidence.allowed_scope(task),
            )
            if policy_codes:
                policy_review = ReviewResult(
                    passed=False,
                    reason="deterministic policy violation",
                    details=policy_details,
                    status=REVIEW_FAIL,
                    risk="HIGH",
                    violation_codes=policy_codes,
                    reviewed_files=list(dict.fromkeys(task.changed_files)),
                    coverage={
                        "total_files": list(dict.fromkeys(task.changed_files)),
                        "reviewed_files": list(dict.fromkeys(task.changed_files)),
                        "missing_files": [],
                        "batch_count": 0,
                        "truncated": False,
                    },
                    deterministic=True,
                )
                Reviewer._apply_to_state(task, policy_review)
                task.failure_stage = "REVIEW"
                task.failure_reason = policy_review.reason
                return ManagerResult(
                    success=False,
                    git_result=git_result,
                    review_result=policy_review,
                    message="deterministic policy violation",
                )

        approval_wait = (
            is_modification
            and wr.success
            and not task.changed_files
            and is_worker_approval_wait_response(task.worker_stdout)
        )
        if approval_wait:
            print(
                f"[{task.task_id}][Worker] 승인 대기 응답 감지 — "
                "비대화형 Job에서는 사용자 응답을 받을 수 없음"
            )

        current_state_candidate = (
            is_modification
            and wr.success
            and not task.changed_files
            and can_verify_current_state(
                task.requirement,
                task.worker_stdout,
                self.profile.module_names if self.profile else None,
            )
        )
        verification_files = (
            extract_requirement_paths(
                task.requirement,
                self.profile.module_names if self.profile else None,
            )
            if current_state_candidate else []
        )
        if current_state_candidate and not verification_files:
            verification_files = [f"{module}/" for module in task.target_module]
        if current_state_candidate:
            print(
                f"[{task.task_id}][Worker] source delta 없음 + 검증 대상 확인 — "
                "명시 경로 빌드 및 Reviewer 현재 파일 검증 진행"
            )

        # worker_produced_work:
        # - MODIFICATION: exit 0 이거나, exit != 0 이더라도 실제로 파일을 변경했으면 True.
        # - ANALYSIS    : 변경사항이 없어도 True (리뷰가 exit!=0 실패를 별도 판정).
        worker_produced_work = (
            (wr.success and not approval_wait)
            or len(task.changed_files) > 0
            or not is_modification
        )
        if not worker_produced_work and git_result.success:
            # git 은 성공했지만 변경 파일이 없음 → 진짜 워커 실패
            task.failure_stage = "WORKER"
            task.failure_reason = (
                f"워커 실패 (exit={wr.exit_code}, 변경사항 없음)"
            )

        # 3. Build / Test
        #    - MODIFICATION: 실제 실행 성공(PASS + success=True)만 통과.
        #      SKIPPED/UNAVAILABLE/ERROR 및 status=PASS라도 success=False는
        #      build 미확인으로 완전 성공이 될 수 없다.
        #    - ANALYSIS: 기존 build 정책을 유지한다.
        task.stage = "BUILD"
        self._log_stage(task, "BUILD", "빌드/테스트 실행")
        build_files = verification_files if current_state_candidate else task.changed_files
        configuration_failure = self._configuration_failure(task)
        if configuration_failure is not None:
            return configuration_failure
        build_context = {
            "task_id": task.task_id,
            "baseline_id": task.active_baseline_id,
            "checkpoint_id": task.checkpoint_id,
            "restored_checkpoint_id": task.restored_checkpoint_id,
            "restored_evidence_epoch": task.restored_evidence_epoch,
        }
        if task.materialized_execution:
            task.gate_evidence = {}
            task.official_tester_invoked = False
            build_identity = gate_evidence.identity(task, self.working_dir)
            try:
                from developer_change_report import capture
                capture(task,self._task_baselines[task.task_id],self.collector,self.working_dir,self.task_root)
            except Exception as exc:
                task.report_diagnostics.append({"code":"DEVELOPER_REPORT_CAPTURE_FAILED","diagnostic":type(exc).__name__})
        try:
            bt_result = self.build_test_runner.run(
                changed_files=build_files,
                target_modules=list(task.target_module),
                evidence_context=build_context,
                authoritative_scope=task.scope_authority == "JOB_CONTRACT",
            )
        except TypeError as exc:
            # Compatibility for injected 0.8.2/0.8.3 test runners. The
            # operational BuildTestRunner supports structured evidence.
            if not any(name in str(exc) for name in ("evidence_context", "authoritative_scope")):
                raise
            bt_result = self.build_test_runner.run(
                changed_files=build_files,
                target_modules=list(task.target_module),
            )
        evidence_ok, evidence_error = self._validate_structured_build_evidence(
            task, bt_result
        )
        if bt_result.build_status == "PASS" and not evidence_ok:
            bt_result.success = False
            bt_result.build_status = "PARTIAL"
            bt_result.error = evidence_error
            bt_result.details = {
                **dict(bt_result.details),
                "reason": evidence_error,
                "structured_evidence_valid": False,
            }
        else:
            bt_result.details = {
                **dict(bt_result.details),
                "structured_evidence_valid": bool(evidence_ok),
            }
        configuration_failure = self._configuration_failure(task)
        if configuration_failure is not None:
            return configuration_failure
        task.build = {
            "status": bt_result.build_status,
            "success": bt_result.success,
            "output": scrub_secrets(bt_result.build_output or ""),
            "details": dict(bt_result.details),
        }
        task.build_evidence = {
            "schema_version": 1,
            "status": bt_result.build_status,
            "success": bool(bt_result.success),
            "details": dict(bt_result.details),
            **build_context,
        }
        task.test = {
            "status": bt_result.test_status,
            "output": scrub_secrets(bt_result.test_output or ""),
        }
        if task.materialized_execution:
            gate_evidence.bind(task, "BUILD", build_identity, bt_result.build_status)
            stale = self._binding_failure(task)
            if stale is not None:
                return stale
        build_ok: bool
        if bt_result.build_status == "FAIL":
            build_ok = False
            task.failure_stage = "BUILD"
            task.failure_code = BUILD_FAILED
            task.failure_reason = scrub_secrets(bt_result.error or (
                f"build={bt_result.build_status}, test={bt_result.test_status}"
            ))
        elif is_modification:
            if bt_result.build_status == "PASS" and bt_result.success:
                build_ok = True
            else:
                build_ok = False
                task.failure_stage = "BUILD"
                reason_detail = bt_result.details.get("reason", "")
                task.failure_code = BUILD_NOT_VERIFIED
                task.failure_reason = (
                    f"{BUILD_NOT_VERIFIED}: MODIFICATION 작업은 "
                    "실행 성공한 build=PASS 필요 "
                    f"(현재: status={bt_result.build_status}, success={bt_result.success}"
                    + (f", {reason_detail}" if reason_detail else "")
                    + ")"
                )
                if bt_result.build_status == "SKIPPED":
                    task.failure_type = "build"
                    task.severity = "MID"
                    task.fix_scope = "NON_RETRYABLE"
                    task.verification_status = "PARTIAL"
        else:
            # ANALYSIS: SKIPPED 허용
            build_ok = True

        # 3.5 Tester (실행 전용, 코드 수정 금지): 증거 수집 후 Reviewer 에 전달.
        #     기본 정책(KKM_TEST_STRICT=0)에서는 결과가 성공 판정에 개입하지 않는다.
        task.stage = "TEST"
        self._log_stage(task, "TEST", "Tester (기본 OFF)")
        run_test, test_reason = should_run_tester(task)
        task.test_required = run_test
        if task.materialized_execution:
            test_identity = gate_evidence.identity(task, self.working_dir)
        if run_test:
            configuration_failure = self._configuration_failure(task)
            if configuration_failure is not None:
                return configuration_failure
            try:
                recorder = getattr(task, "_record_execution_attempt", None)
                if callable(recorder):
                    recorder("TESTER", task.task_id + "-TESTER-" + str(task.retry_count + 1),
                             task.effective_policy["resolved_tester"]["model"])
                task_root = getattr(self, "task_root", None)
                if task_root is not None:
                    task._context_manifest_root = Path(task_root) / "context-manifests"
                if self.profile is not None:
                    run_tester(
                        task,
                        self.working_dir,
                        profile=self.profile,
                        policy_root=self.policy_root,
                    )
                else:
                    run_tester(task, self.working_dir)
            except Exception as e:
                # 예외조차 파이프라인을 깨지 않는다 (ERROR 로 기록)
                mark_tester_skipped(
                    task, f"tester 예외(ERROR 취급): {e}"
                )
                task.test_status = "ERROR"
            configuration_failure = self._configuration_failure(task)
            if configuration_failure is not None:
                return configuration_failure
        else:
            mark_tester_skipped(task, test_reason)
            print(f"[{task.task_id}][Tester] 스킵: {test_reason}")

        strict_mode = os.environ.get("KKM_TEST_STRICT", "0") == "1"
        strict_test_failed = ((strict_mode and task.test_status == "FAIL") or
            (task.test_plan.get("mandatory") is True and
             (task.test_status != "PASS" or not task.official_tester_invoked)))
        review_identity: dict[str, str] = {}
        if task.materialized_execution:
            gate_evidence.bind(task, "TEST", test_identity,
                task.test_status if task.official_tester_invoked else "NOT_INVOKED")
            stale = self._binding_failure(task)
            if stale is not None:
                return stale
            review_identity = gate_evidence.identity(task, self.working_dir)

        if (skeleton_policy.enabled(task) and task.test_status == "ERROR"
                and task.test_summary == "tester 응답 파싱 실패(TEST_RESULT 없음)"):
            task.failure_stage = "TEST"
            task.failure_code = STRUCTURED_OUTPUT_INVALID
            task.failure_reason = task.test_summary
            task.failure_origin = "TOOL_INFRA"
            task.failure_type = "infrastructure"
            task.fix_scope = "NON_RETRYABLE"
            task.status = task.stage = "AWAITING_QA"
            task.verification_status = "PARTIAL"
            task.commit_eligible = False
            task.keep_working_tree = True
            task.worktree_disposition = "PRESERVED" if task.changed_files else "NO_DELTA"
            return ManagerResult(success=False, worker_result=wr, git_result=git_result,
                build_test_result=bt_result, message=task.failure_code)

        # 4. Review (파일명 목록 + diff 본문 + 워커 요약 전달)
        task.stage = "REVIEW"
        self._log_stage(task, "REVIEW", "codex 리뷰 (fail-closed)")
        configuration_failure = self._configuration_failure(task)
        if configuration_failure is not None:
            return configuration_failure
        task.reviewer_recovery_count = 0
        task.reviewer_recovery_limit = REVIEW_INFRA_RETRY_LIMIT
        task.review_policy = decision_from_task(task).to_dict()
        self._prepare_reviewer_effort(task)
        review_kwargs = {
            "requirement": task.requirement,
            "worker_success": worker_success,
            "exit_code": task.exit_code,
            "changed_files": list(task.changed_files),
            "worker_stdout": task.worker_stdout,
            "git_diff": task.git_diff,
            "task": task,
            "attempt": task.retry_count + 1,
            "max_attempts": self.max_retry,
        }
        review_retry_allowed = (
            is_modification
            and worker_produced_work
            and git_result.success
            and build_ok
            and not strict_test_failed
        )
        def invoke_reviewer(**kwargs):
            started = datetime.now().astimezone()
            recorder = getattr(task, "_record_execution_attempt", None)
            if callable(recorder):
                attempt_record = recorder(
                    "REVIEWER",
                    task.task_id + "-REVIEWER-" + __import__("uuid").uuid4().hex[:12],
                    task.review_policy["model"],
                )
                task.reviewer_attempt_id = str(attempt_record.get("attempt_id", ""))
            def finish_reviewer_attempt(status):
                if not task.control_repository_path or not task.reviewer_attempt_id:
                    return
                from control_repository import ControlRepository
                from runtime_adapter import RetryDomain
                ControlRepository(task.control_repository_path).finish_attempt(
                    task.reviewer_attempt_id,
                    status=status,
                    retry_domain=(
                        "" if status == "SUCCEEDED"
                        else RetryDomain.TECHNICAL_EXECUTION_RETRY.value
                    ),
                    failure_code=("" if status == "SUCCEEDED" else "REVIEWER_TECHNICAL_FAILURE"),
                )
            try:
                outcome = self.reviewer.review(**kwargs)
            except Exception as exc:
                try:
                    finish_reviewer_attempt("FAILED")
                except Exception as finish_exc:
                    task.report_diagnostics.append({
                        "code": "REVIEWER_ATTEMPT_OUTCOME_PERSIST_FAILED",
                        "diagnostic": type(finish_exc).__name__,
                    })
                task.reviewer_invocations.append({
                    "ordinal": len(task.reviewer_invocations) + 1,
                    "started_at": started.isoformat(),
                    "ended_at": datetime.now().astimezone().isoformat(),
                    "model": str(task.review_policy.get("model", "")),
                    "reasoning_effort": task.reviewer_effective_reasoning_effort,
                    "context_tier": str(task.review_policy.get("context_tier", "")),
                    "status": "ERROR",
                    "failure_reason": type(exc).__name__,
                    "source_mutation": False,
                })
                raise
            try:
                finish_reviewer_attempt(
                    "SUCCEEDED" if outcome.status in {REVIEW_PASS, REVIEW_FAIL} else "FAILED"
                )
            except Exception as exc:
                task.report_diagnostics.append({
                    "code": "REVIEWER_ATTEMPT_OUTCOME_PERSIST_FAILED",
                    "diagnostic": type(exc).__name__,
                })
            task.reviewer_invocations.append({
                "ordinal": len(task.reviewer_invocations) + 1,
                "started_at": started.isoformat(),
                "ended_at": datetime.now().astimezone().isoformat(),
                "model": str(task.review_policy.get("model", "")),
                "reasoning_effort": task.reviewer_effective_reasoning_effort,
                "context_tier": str(task.review_policy.get("context_tier", "")),
                "status": str(outcome.status),
                "failure_code": str(outcome.failure_code or ""),
                "native_session_id": task.reviewer_native_session_id,
                "session_reuse_mode": task.reviewer_session_reuse_mode,
                "source_mutation": False,
            })
            return outcome
        reuse_partition = None
        oracle_review = self._deterministic_oracle_review(task, review_identity)
        if oracle_review is not None:
            # Explicit deterministic-oracle contract covers this acceptance;
            # the separate LLM Reviewer is skipped with a bound REVIEW receipt.
            print(f"[{task.task_id}][Review] deterministic oracle PASS — LLM Reviewer 생략(0 invocations)")
            review_result = Reviewer._apply_to_state(task, oracle_review)
        elif task.git_diff.strip() and hasattr(self.reviewer, "plan_diff_batches"):
            plan = self.reviewer.plan_diff_batches(task.git_diff)
            if not plan.valid:
                review_result = Reviewer._apply_to_state(
                    task, self.reviewer.plan_failure_result(plan, list(task.changed_files))
                )
            else:
                review_plan, reuse_partition, reused_carrier = self._plan_review_reuse(
                    task, plan, review_identity,
                )
                pairs: list[tuple[object, ReviewResult]] = []
                retry_budget = REVIEW_INFRA_RETRY_LIMIT
                for batch in review_plan.batches:
                    if reused_carrier is not None and batch.batch_id == reused_carrier[0].batch_id:
                        print(f"[{task.task_id}][Review] {reused_carrier[1].reason}")
                        pairs.append(reused_carrier)
                        continue
                    print(f"[{task.task_id}][Review] batch {batch.index}/{len(review_plan.batches)}")
                    batch_kwargs = dict(review_kwargs)
                    batch_kwargs.update({
                        "changed_files": list(batch.changed_files),
                        "git_diff": batch.text,
                        "review_batch": batch,
                        "all_changed_files": list(task.changed_files),
                        "batch_index": batch.index,
                        "batch_count": len(review_plan.batches),
                    })
                    configuration_failure = self._configuration_failure(task)
                    if configuration_failure is not None:
                        return configuration_failure
                    batch_result = invoke_reviewer(**batch_kwargs)
                    configuration_failure = self._configuration_failure(task)
                    if configuration_failure is not None:
                        return configuration_failure
                    retryable = (
                        batch_result.status in REVIEW_INFRA_STATUSES
                        and batch_result.failure_code not in DETERMINISTIC_REVIEW_FAILURE_CODES
                    )
                    if retryable and review_retry_allowed and retry_budget:
                        retry_budget -= 1
                        task.reviewer_recovery_count += 1
                        self._prepare_reviewer_effort(task, infrastructure_retry=True)
                        print(
                            f"[{task.task_id}][Review] 인프라 오류 {batch_result.status} — "
                            f"Manager 제한 재호출 {REVIEW_INFRA_RETRY_LIMIT - retry_budget}/"
                            f"{REVIEW_INFRA_RETRY_LIMIT} (batch {batch.index})"
                        )
                        configuration_failure = self._configuration_failure(task)
                        if configuration_failure is not None:
                            return configuration_failure
                        batch_result = invoke_reviewer(**batch_kwargs)
                        configuration_failure = self._configuration_failure(task)
                        if configuration_failure is not None:
                            return configuration_failure
                    pairs.append((batch, batch_result))
                    if batch_result.status in REVIEW_INFRA_STATUSES:
                        break
                review_result = Reviewer._apply_to_state(
                    task,
                    self._aggregate_review_batches(pairs, review_plan, list(task.changed_files)),
                )
                self._record_review_evidence(task, pairs, review_identity, reuse_partition)
        else:
            configuration_failure = self._configuration_failure(task)
            if configuration_failure is not None:
                return configuration_failure
            review_result = invoke_reviewer(**review_kwargs)
            configuration_failure = self._configuration_failure(task)
            if configuration_failure is not None:
                return configuration_failure
            for review_retry in range(REVIEW_INFRA_RETRY_LIMIT):
                if review_result.status not in REVIEW_INFRA_STATUSES or not review_retry_allowed:
                    break
                if review_result.failure_code in DETERMINISTIC_REVIEW_FAILURE_CODES:
                    break
                task.reviewer_recovery_count += 1
                self._prepare_reviewer_effort(task, infrastructure_retry=True)
                print(
                    f"[{task.task_id}][Review] 인프라 오류 {review_result.status} — "
                    f"Manager 제한 재호출 {review_retry + 1}/{REVIEW_INFRA_RETRY_LIMIT}"
                )
                configuration_failure = self._configuration_failure(task)
                if configuration_failure is not None:
                    return configuration_failure
                review_result = invoke_reviewer(**review_kwargs)
                configuration_failure = self._configuration_failure(task)
                if configuration_failure is not None:
                    return configuration_failure
        if self._review_only_disputes_valid_build(task, review_result):
            review_result = Reviewer._apply_to_state(
                task,
                ReviewResult(
                    passed=True,
                    status=REVIEW_PASS,
                    reason="structured Build PASS accepted; no semantic finding remains",
                    details=list(review_result.details) + [
                        "Reviewer build-evidence prose was superseded by verified execution evidence"
                    ],
                    reviewed_files=list(review_result.reviewed_files),
                    coverage=dict(review_result.coverage or {}),
                ),
            )
        task.reviewed_task_files = list(dict.fromkeys(review_result.reviewed_files))
        coverage = dict(review_result.coverage or {})
        coverage.update({
            "current_task_changed_files": list(task.task_owned_changed_files),
            "reviewed_task_files": list(task.reviewed_task_files),
            "inspected_unchanged_target_files": list(task.inspected_unchanged_target_files),
            "inherited_batch_delta_files": list(task.inherited_batch_delta_files),
            "pre_job_snapshot_id": task.pre_job_snapshot_id,
            "failure_fingerprint": task.failure_fingerprint,
            "worker_tool_errors": _tool_error_kinds(task),
            "profile_build_commands": list(task.profile_build_commands),
            "reviewer_recovery_count": task.reviewer_recovery_count,
            "reviewer_recovery_limit": task.reviewer_recovery_limit,
        })
        if reuse_partition is not None and reuse_partition.enabled:
            coverage["review_evidence_reuse"] = {
                "disposition_files_reused_valid_evidence": list(reuse_partition.reused_files),
                "disposition_files_stale_reviewed": list(reuse_partition.stale_files),
                "reused_chunk_count": len(reuse_partition.reused_chunks),
                "prior_review_round": reuse_partition.prior_round,
            }
        review_result.coverage = coverage
        task.review_coverage = coverage
        # 리뷰 게이트:
        # - REVIEW_FAIL                     : 모든 모드에서 실패
        # - REVIEW_UNAVAILABLE / REVIEW_ERROR:
        #     MODIFICATION → 실패 (fail-closed: 검증 불완전을 PASS 로 만들지 않음)
        #     ANALYSIS     → 경고로만 기록 (자동 FAIL 하지 않음)
        review_ok: bool
        if is_modification:
            review_ok = review_result.status == REVIEW_PASS
            if not review_ok:
                task.failure_stage = "REVIEW"
                task.failure_reason = (
                    f"review={review_result.status} — {review_result.reason or 'review 실패'}"
                )
        else:
            review_ok = review_result.status != REVIEW_FAIL
            if review_result.status not in (REVIEW_PASS, REVIEW_FAIL):
                print(f"[{task.task_id}][Review] ⚠ 검증 불완전 "
                      f"({review_result.status}) — ANALYSIS 작업은 경고로만 기록")
            if review_result.status == REVIEW_FAIL:
                task.failure_stage = "REVIEW"
                task.failure_reason = review_result.reason or "review FAIL"

        # 검증 게이트 (task_mode 기준):
        #   MODIFICATION: produced_work AND git_ok AND build==PASS AND review==REVIEW_PASS
        #   ANALYSIS    : produced_work AND (git 관대) AND (build FAIL만 차단) AND (REVIEW_FAIL만 차단)
        self._log_stage(task, "VERIFY", "검증 게이트 집계")
        if task.materialized_execution:
            gate_evidence.bind(task, "REVIEW", review_identity, "PASS" if review_ok else "FAIL")
            stale = self._binding_failure(task)
            if stale is not None:
                return stale
        git_ok = git_result.success or not is_modification
        success = (
            worker_produced_work
            and git_ok
            and build_ok
            and review_ok
            and not strict_test_failed
        )

        if success:
            task.completion_kind = (
                "ALREADY_SATISFIED" if current_state_candidate else "CHANGED"
            )
            task.verification_status = (
                "VERIFIED"
                if (not task.test_required or task.test_status == "PASS")
                else "PARTIAL"
            )
            eligible, blockers, hashes = self.collector.assess_commit_safety(
                task, self._task_baselines[task.task_id]
            )
            task.changed_file_sha256 = hashes
            task.commit_blockers = list(dict.fromkeys(task.commit_blockers + blockers))
            task.commit_eligible = bool(eligible and not task.commit_blockers)
            if task.materialized_execution:
                gate_evidence.bind(task, "VERIFICATION", gate_evidence.identity(task, self.working_dir),
                                   "PASS" if task.verification_status == "VERIFIED" else "PARTIAL")
                stale = self._binding_failure(task, require_all=True)
                if stale is not None:
                    return stale

        # Reviewer의 일반 무변경 판정보다 실제 원인(승인 대기)을 우선 보존한다.
        if approval_wait:
            task.failure_stage = "WORKER"
            task.failure_reason = WORKER_APPROVAL_WAIT_REASON

        # 3.6 STRICT 모드(KKM_TEST_STRICT=1): 테스트 FAIL 을 전체 실패로 승격.
        #     기본(0)에서는 테스트 결과가 성공 판정/롤백에 개입하지 않는다.
        if strict_test_failed:
            success = False
            print(f"[{task.task_id}][Test] STRICT 모드 — tester FAIL 로 전체 실패 승격")

        secondary_failures: list[str] = []
        if success:
            # 같은 시도 안의 Reviewer 인프라 재호출이 PASS한 경우 첫 오류가
            # 남아 성공 상태를 오염시키지 않게 현재 실패 메타데이터를 정리한다.
            task.failure_stage = ""
            task.failure_code = ""
            task.failure_reason = ""
            task.failure_type = ""
            task.severity = "NONE"
            task.fix_scope = "RETRYABLE"
        elif approval_wait:
            task.failure_stage = "WORKER"
            task.failure_code = ""
            task.failure_reason = WORKER_APPROVAL_WAIT_REASON
            task.failure_type = "infrastructure"
            task.severity = "MID"
            task.fix_scope = "RETRYABLE"
            task.verification_status = "FAILED"
        elif not worker_produced_work:
            task.failure_stage = "WORKER"
            task.failure_type = "timeout" if getattr(wr, "timed_out", False) else "infrastructure"
            task.severity = "MID"
            task.fix_scope = "RETRYABLE"
            task.verification_status = "FAILED"
        elif not git_result.success and is_modification:
            task.failure_stage = "GIT"
            task.failure_type = "infrastructure"
            task.severity = "MID"
            task.fix_scope = "RETRYABLE"
            task.verification_status = "FAILED"
        elif bt_result.build_status == "FAIL":
            task.failure_stage = "BUILD"
            task.failure_code = BUILD_FAILED
            task.failure_reason = scrub_secrets(bt_result.error or (
                f"build={bt_result.build_status}, test={bt_result.test_status}"
            ))
            task.failure_type = "build"
            task.severity = "MID"
            task.fix_scope = "RETRYABLE"
            task.verification_status = "FAILED"
            if review_result.status != REVIEW_PASS:
                secondary_failures.append(review_result.failure_code or review_result.status)
        elif bt_result.test_status == "FAIL" or strict_test_failed:
            task.failure_stage = "TEST"
            task.failure_code = TEST_FAILED
            task.failure_reason = scrub_secrets(
                bt_result.test_output
                if bt_result.test_status == "FAIL"
                else f"tester FAIL (STRICT 모드): {task.test_summary or '증거 확인 필요'}"
            )
            task.failure_type = "test"
            task.severity = "MID"
            task.fix_scope = "RETRYABLE"
            task.verification_status = "FAILED"
            if review_result.status != REVIEW_PASS:
                secondary_failures.append(review_result.failure_code or review_result.status)
        elif not build_ok:
            # BUILD_NOT_VERIFIED 는 기존 fail-closed/manual QA 계약을 유지한다.
            task.failure_stage = "BUILD"
            task.failure_code = BUILD_NOT_VERIFIED
            reason_detail = bt_result.details.get("reason", "")
            task.failure_reason = scrub_secrets(
                f"{BUILD_NOT_VERIFIED}: MODIFICATION 작업은 실행 성공한 build=PASS 필요 "
                f"(현재: status={bt_result.build_status}, success={bt_result.success}"
                + (f", {reason_detail}" if reason_detail else "")
                + ")"
            )
            task.failure_type = "build"
            task.severity = "MID"
            task.fix_scope = "NON_RETRYABLE"
            task.verification_status = "PARTIAL"
            if review_result.status != REVIEW_PASS:
                secondary_failures.append(review_result.failure_code or review_result.status)
        elif review_result.status == REVIEW_FAIL:
            task.failure_stage = "REVIEW"
            task.failure_code = REVIEW_FAILED
            task.failure_reason = scrub_secrets(review_result.reason or "review FAIL")
            task.failure_type = "review"
            task.severity = review_result.risk if review_result.risk in ("LOW", "MID", "HIGH") else "HIGH"
            task.fix_scope = "RETRYABLE"
            task.verification_status = "FAILED"
        elif review_result.status in REVIEW_INFRA_STATUSES:
            task.failure_stage = "REVIEW"
            task.failure_code = _review_infrastructure_code(review_result)
            task.failure_reason = scrub_secrets(
                f"review={review_result.status} — {review_result.reason or 'review 검증 불완전'}"
            )
            task.failure_type = "infrastructure"
            task.severity = "MID"
            task.fix_scope = "NON_RETRYABLE"
            task.verification_status = "PARTIAL"

        configuration_failure = self._configuration_failure(task)
        if configuration_failure is not None:
            return configuration_failure

        task.failure_origin = "" if success else classify_failure_origin(
            task.failure_code,
            failure_stage=task.failure_stage,
            failure_type=task.failure_type,
            user_qa_required=task.status == "AWAITING_QA",
        )
        if success and skeleton_policy.enabled(task):
            # Local gate success is not final contract/DB/E2E acceptance.
            if (not task.commit_eligible or task.commit_blockers
                    or not task.external_frozen_integrity or not task.baseline_declaration_integrity
                    or task.unexpected_external_dirty_files):
                task.failure_code = 'CUMULATIVE_VERIFICATION_CONTRACT_INVALID'
                task.status = task.stage = 'AWAITING_QA'
            elif not skeleton_policy.deferred_signal(task):
                task.failure_code = 'DEFERRED_CONTRACT_RECORD_REQUIRED'
                task.status = task.stage = 'AWAITING_QA'
            else:
                task.failure_code = 'DEFERRED_CONTRACT'
                task.status = skeleton_policy.SKELETON_READY
                task.stage = 'DONE'
            task.verification_status = 'PARTIAL'
            task.commit_eligible = False
            task.failure_origin = 'USER_QA'
            task.failure_type = 'contract'
            task.fix_scope = 'NON_RETRYABLE'
            task.keep_working_tree = True
            if task.failure_code == 'CUMULATIVE_VERIFICATION_CONTRACT_INVALID':
                task.failure_origin = 'HARNESS_CAUSED'
                task.failure_type = 'infrastructure'
            task.worktree_disposition = 'PRESERVED' if task.changed_files else 'NO_DELTA'
            gate_evidence.bind(task, 'VERIFICATION', gate_evidence.identity(task, self.working_dir), 'PARTIAL')
            return ManagerResult(success=False, worker_result=wr, git_result=git_result,
                build_test_result=bt_result, review_result=review_result,
                message=task.status)
        if not success and not worker_remediation_allowed(
            task.failure_origin, task.failure_code
        ):
            task.fix_scope = "NON_RETRYABLE"

        return ManagerResult(
            success=success,
            worker_result=worker_results[0] if worker_results else None,
            git_result=git_result,
            build_test_result=bt_result,
            review_result=review_result,
            message="PASS" if success else "FAIL",
            secondary_failures=tuple(dict.fromkeys(secondary_failures)),
        )

    @staticmethod
    def _decide_retry_scope(task: TaskState, result: "ManagerResult") -> str:
        """실패 시도의 재시도 범위 결정 (단일 결정 지점).
        반환: "partial"(LOW) | "full"(MID) | "restart"(HIGH)
        - REVIEW_FAIL + review_risk LOW/MID → partial/full (워킹트리 유지)
        - Build/Test FAIL → full (변경 유지 후 Worker가 실행 증거를 수정)
        - 그 외 실패(변경 없음, WORKER/GIT 단계 등) → restart
        성공 시 호출되지 않는다.
        """
        from task_state import REVIEW_STATUS_FAIL

        if not worker_remediation_allowed(task.failure_origin, task.failure_code):
            return "restart"

        # 1) actionable Build/Test 증거는 변경을 유지해 Worker handoff에 사용한다.
        if (
            task.failure_code in (BUILD_FAILED, TEST_FAILED)
            or (
                result.build_test_result is not None
                and (
                    result.build_test_result.build_status == "FAIL"
                    or result.build_test_result.test_status == "FAIL"
                )
            )
        ):
            return "full"

        # Patch tooling failures keep any safe partial task delta and re-read only
        # the failed target on the one corrected retry.
        if task.failure_code in (WORKER_PATCH_CONFLICT, WORKER_PATCH_CONTEXT_STALE):
            return "partial" if task.task_owned_changed_files else "restart"

        # 2) 의미 있는 변경 없음 → 롤백해도 잃을 게 없음 + 클린 시작
        if task.failure_stage in ("WORKER", "WORKER_TOOL", "GIT"):
            return "restart"

        # 3) REVIEW_FAIL + 등급: LOW/MID만 트리 유지
        if (
            task.review_status == REVIEW_STATUS_FAIL
            and task.review_risk in ("LOW", "MID")
        ):
            return "partial" if task.review_risk == "LOW" else "full"

        # 4) 나머지 재시도 실패는 기존 restart 정책을 유지한다.
        return "restart"

    @staticmethod
    def _build_executions(details: dict) -> list[dict]:
        executions: list[dict] = []
        for item in details.get("executions") or []:
            if isinstance(item, dict):
                executions.append(dict(item))
        for result in details.get("results") or []:
            if isinstance(result, dict):
                executions.extend(Manager._build_executions(result))
        return executions

    @staticmethod
    def _review_only_disputes_valid_build(
        task: TaskState, result: ReviewResult
    ) -> bool:
        if result.status != REVIEW_FAIL:
            return False
        details = dict(task.build_evidence.get("details") or {})
        if (
            task.build_evidence.get("status") != "PASS"
            or task.build_evidence.get("success") is not True
            or details.get("structured_evidence_valid") is not True
        ):
            return False
        codes = {str(code).strip().upper() for code in result.violation_codes if str(code).strip()}
        build_only = {
            "BUILD_EVIDENCE_MISSING",
            "BUILD_NOT_EXECUTED",
            "BUILD_COMMAND_NOT_RUN",
        }
        return bool(codes) and codes.issubset(build_only)

    def _validate_structured_build_evidence(
        self, task: TaskState, result: BuildTestResult
    ) -> tuple[bool, str]:
        """Validate authoritative evidence from the operational Build runner.

        Legacy/injected runners remain compatible; real BATCH_FINAL_COMMIT runs
        must prove that every requested module command actually executed for the
        current Task/baseline/restore epoch and that its persisted log matches.
        """
        if not isinstance(self.build_test_runner, BuildTestRunner):
            return result.build_status == "PASS" and bool(result.success), ""
        if result.build_status != "PASS" or not result.success:
            return False, "BUILD_EXECUTION_NOT_PASS"
        executions = self._build_executions(dict(result.details))
        if not executions:
            return False, "BUILD_EXECUTION_EVIDENCE_MISSING"
        expected_modules = set(task.target_module or [])
        actual_modules: set[str] = set()
        required = {
            "module", "cwd", "argv", "command_source", "started_at",
            "finished_at", "exit_code", "status", "log_sha256", "log_path",
            "task_id", "baseline_id", "checkpoint_id",
            "restored_checkpoint_id", "restored_evidence_epoch",
        }
        for execution in executions:
            if not required.issubset(execution):
                return False, "BUILD_EXECUTION_EVIDENCE_INCOMPLETE"
            module = str(execution.get("module", ""))
            actual_modules.add(module)
            if (
                execution.get("status") != "PASS"
                or int(execution.get("exit_code", -1)) != 0
                or not execution.get("argv")
                or not execution.get("cwd")
            ):
                return False, "BUILD_COMMAND_NOT_SUCCESSFUL"
            identities = {
                "task_id": task.task_id,
                "baseline_id": task.active_baseline_id,
                "checkpoint_id": task.checkpoint_id,
                "restored_checkpoint_id": task.restored_checkpoint_id,
                "restored_evidence_epoch": task.restored_evidence_epoch,
            }
            if any(str(execution.get(key, "")) != str(value) for key, value in identities.items()):
                return False, "STALE_BUILD_EVIDENCE"
            log_path = Path(str(execution.get("log_path", "")))
            try:
                payload = log_path.read_bytes()
            except OSError:
                return False, "BUILD_LOG_MISSING"
            if hashlib.sha256(payload).hexdigest() != execution.get("log_sha256"):
                return False, "BUILD_LOG_HASH_MISMATCH"
        if expected_modules and not expected_modules.issubset(actual_modules):
            return False, "BUILD_MODULE_COVERAGE_PARTIAL"
        return True, ""

    def _rollback_for_outer_retry(self, task: TaskState, label: str) -> bool:
        """Close an exhausted inner attempt without carrying its delta forward.

        A new Task captures a new Git baseline.  Requeueing a PRESERVED delta
        would therefore turn the failed edit into that baseline and could omit it
        from later review/commit.  Only a proven rollback may hand control to the
        outer Job retry.  An uncertain rollback remains AWAITING_QA.
        """
        task.retry_scope = "restart"
        task.keep_working_tree = False
        checkpointed = False
        checkpoint_policy_enabled = getattr(self, "checkpoint_store", None) is not None
        no_delta_proof: dict = {}
        if checkpoint_policy_enabled and hasattr(self.collector, "prove_no_task_delta"):
            try:
                no_delta_proof = dict(
                    self.collector.prove_no_task_delta(
                        self._task_baselines.get(task.task_id)
                    )
                )
            except Exception as exc:
                no_delta_proof = {
                    "schema_version": 1,
                    "kind": "VERIFIED_NO_TASK_DELTA",
                    "passed": False,
                    "failure_code": "NO_TASK_DELTA_PROOF_ERROR",
                    "error_type": type(exc).__name__,
                }
            task.no_task_delta_evidence = no_delta_proof
        verified_no_delta = bool(no_delta_proof.get("passed"))
        if checkpoint_policy_enabled and not verified_no_delta:
            try:
                record = self.checkpoint_store.capture(
                    task, self._task_baselines.get(task.task_id), label
                )
                if not record:
                    raise AttemptCheckpointError(
                        NO_TASK_DELTA_VERIFICATION_FAILED,
                        str(no_delta_proof.get("failure_code", "")),
                    )
                if hasattr(self.checkpoint_store, "load"):
                    self.checkpoint_store.load(record["manifest_path"])
                checkpointed = True
                task.checkpoint_id = record["checkpoint_id"]
                task.checkpoint_manifest = record["manifest_path"]
                task.checkpoint_status = "CAPTURED_AND_VERIFIED"
                task.checkpoint_event = "PRE_ROLLBACK_CHECKPOINT_CAPTURED"
                if task.attempt_history:
                    task.attempt_history[-1].update({
                        "recovery_scope": "restart",
                        "recovery_action": "ARTIFACT_CAPTURE_THEN_SCOPED_ROLLBACK",
                        "checkpoint_id": task.checkpoint_id,
                        "checkpoint_manifest": task.checkpoint_manifest,
                        "artifact_first": True,
                    })
            except Exception as exc:
                prior_code = task.failure_code
                task.failure_stage = "CHECKPOINT"
                task.failure_code = CHECKPOINT_CAPTURE_FAILED
                detail_code = getattr(exc, "code", type(exc).__name__)
                task.failure_reason = f"{CHECKPOINT_CAPTURE_FAILED}:{detail_code}"
                task.failure_type = "infrastructure"
                task.fix_scope = "NON_RETRYABLE"
                task.verification_status = "PARTIAL"
                task.status = "AWAITING_QA"
                task.stage = "AWAITING_QA"
                task.worktree_disposition = "PRESERVED"
                task.user_intervention_reason = CHECKPOINT_CAPTURE_FAILED
                task.checkpoint_status = "CAPTURE_OR_VERIFICATION_FAILED"
                task.checkpoint_failure_evidence = {
                    "failure_code": detail_code,
                    "primary_failure_before_checkpoint": prior_code,
                    "no_task_delta_evidence": no_delta_proof,
                    "rollback_performed": False,
                    "current_task_delta_preserved": True,
                    "predecessor_delta_preserved": True,
                }
                task.save(self.task_root)
                print(f"[{task.task_id}] checkpoint capture failed: {getattr(exc, 'code', type(exc).__name__)}")
                return False
        elif checkpoint_policy_enabled:
            task.checkpoint_status = "VERIFIED_NO_TASK_DELTA"
            task.checkpoint_event = "ROLLBACK_WITHOUT_CHECKPOINT_NO_TASK_DELTA"
        ok, message = self.collector.rollback(
            task=task, baseline=self._task_baselines.get(task.task_id)
        )
        if ok and checkpoint_policy_enabled and hasattr(self.collector, "prove_no_task_delta"):
            post = dict(
                self.collector.prove_no_task_delta(
                    self._task_baselines.get(task.task_id)
                )
            )
            if not post.get("passed"):
                ok = False
                message = CHECKPOINT_ROLLBACK_VERIFY_FAILED
                task.failure_stage = "CHECKPOINT"
                task.failure_code = CHECKPOINT_ROLLBACK_VERIFY_FAILED
                task.failure_type = "infrastructure"
                task.fix_scope = "NON_RETRYABLE"
                task.verification_status = "PARTIAL"
                task.user_intervention_reason = CHECKPOINT_ROLLBACK_VERIFY_FAILED
        task.worktree_disposition = (
            "CHECKPOINTED_CLEAN_ROLLBACK" if ok and checkpointed
            else "CLEAN_ROLLBACK" if ok else "UNKNOWN"
        )
        if task.attempt_history:
            task.attempt_history[-1].update({
                "worktree_disposition": task.worktree_disposition,
                "recovery_scope": "restart",
                "recovery_action": (
                    "ARTIFACT_CAPTURE_THEN_SCOPED_ROLLBACK"
                    if checkpointed else "VERIFIED_NO_DELTA_SCOPED_ROLLBACK"
                ),
                "checkpoint_id": task.checkpoint_id,
                "checkpoint_manifest": task.checkpoint_manifest,
                "artifact_first": bool(checkpointed),
            })
        print(
            f"[{task.task_id}] {label} 롤백 결과: {message} "
            f"| is_rolled_back={task.is_rolled_back}"
        )
        if ok:
            task.status = "FAILED"
            task.stage = "DONE"
        else:
            task.failure_reason = (
                f"{task.failure_reason}; rollback failed: {scrub_secrets(message)}"
            ).strip("; ")
            task.failure_type = "infrastructure"
            task.fix_scope = "NON_RETRYABLE"
            task.verification_status = "PARTIAL"
            task.status = "AWAITING_QA"
            task.stage = "AWAITING_QA"
        task.save(self.task_root)
        return ok

    def run_with_retry(self, task: TaskState) -> ManagerResult:
        """공유 모듈 범위를 점유한 채 전체 재시도 파이프라인을 실행한다."""
        job_id = f"job-{task.task_id}"
        scope_resources = list(task.target_resources or task.target_module)
        print(f"[{task.task_id}][Scope] 대기: {sorted(TaskScopeCoordinator.keys(scope_resources))}")
        waited = self._scope_coordinator.acquire(job_id, scope_resources)
        print(f"[{task.task_id}][Scope] 획득" + (" (대기 후)" if waited else ""))
        try:
            return self._run_with_retry_locked(task)
        finally:
            self._task_baselines.pop(task.task_id, None)
            self._scope_coordinator.release(job_id)
            print(f"[{task.task_id}][Scope] 해제")

    def _run_with_retry_locked(self, task: TaskState) -> ManagerResult:
        """재시도 포함 실제 실행. 호출 시 Job 범위 락이 이미 잡혀 있다."""
        last_result: ManagerResult | None = None
        attempt_limit = self.max_retry
        task.worker_attempt_limit_configured = int(self.max_retry)
        task.worker_attempt_limit_effective = int(attempt_limit)
        task.progress_attempt_limit = int(attempt_limit)
        fingerprint_counts: dict[str, int] = {}
        core_counts: dict[str, int] = {}
        attempt = 0

        while attempt < attempt_limit:
            attempt += 1
            wtag = self._wtag(task)
            print("\n" + "=" * 60)
            print(f"{wtag}[{task.task_id}] 시도 {attempt}/{attempt_limit}")
            print("=" * 60)

            task.retry_count = attempt - 1
            # Current eligibility is attempt-scoped; keep prior failures only as
            # historical evidence before recomputing this attempt.
            if task.commit_blockers:
                task.commit_blocker_history.append({
                    "attempt": max(1, attempt - 1),
                    "blockers": list(task.commit_blockers),
                    "eol_repair_diagnostics": list(task.eol_repair_diagnostics),
                })
            task.commit_blockers = []
            task.eol_repair_diagnostics = []
            task.commit_eligible = False
            # Attempt-scoped worktree facts must never leak from an earlier retry.
            # In particular a HIGH review may roll back attempt 1, while attempt 2
            # later preserves a LOW/MID diff.
            task.is_rolled_back = False
            task.keep_working_tree = False
            task.worktree_disposition = "UNKNOWN"
            task.retry_scope = ""
            task.stage = "WORKING"
            task.save(self.task_root)

            result = self._run_pipeline(task)
            post_pipeline_configuration_failure = self._configuration_failure(task)
            if post_pipeline_configuration_failure is not None:
                result = post_pipeline_configuration_failure
            last_result = result
            if task.status == 'SKELETON_READY' and task.failure_code == 'DEFERRED_CONTRACT':
                task.save(self.task_root)
                return result
            if (task.failure_stage == 'TEST' and task.failure_code == STRUCTURED_OUTPUT_INVALID
                    and task.status == 'AWAITING_QA' and task.failure_type == 'infrastructure'):
                task.save(self.task_root)
                return result
            if task.failure_code == 'CONTRACT_UNRESOLVED' and task.status == 'AWAITING_QA':
                # A missing human contract is neither a product fingerprint nor
                # a retry/rollback request. Preserve the explicit QA evidence.
                task.save(self.task_root)
                return result
            if task.failure_code in CODEX_IPC_FAILURES:
                # Transport failure is not a product attempt/fingerprint/retry.
                # Preserve any unknown partial tool effects for technical recovery.
                task.save(self.task_root)
                return result
            # Older/injected pipeline adapters may not yet populate the 0.9
            # origin field.  Derive it from structured gate evidence so legacy
            # LOW/MID review and Build/Test remediation semantics stay intact.
            if not result.success and not task.failure_origin:
                if (
                    result.review_result is not None
                    and result.review_result.status == REVIEW_FAIL
                ):
                    task.failure_origin = REVIEWER_CODE_FINDING
                elif (
                    result.build_test_result is not None
                    and (
                        result.build_test_result.build_status == "FAIL"
                        or result.build_test_result.test_status == "FAIL"
                    )
                ):
                    task.failure_origin = WORKER_CAUSED
                else:
                    task.failure_origin = classify_failure_origin(
                        task.failure_code,
                        failure_stage=task.failure_stage,
                        failure_type=task.failure_type,
                    )
            review_infrastructure_failure = bool(
                not result.success
                and task.failure_stage == "REVIEW"
                and result.review_result is not None
                and result.review_result.status in REVIEW_INFRA_STATUSES
            )
            if not result.success and not review_infrastructure_failure:
                fingerprint, core_fingerprint = failure_fingerprint(task)
                task.failure_fingerprint = fingerprint
                fingerprint_counts[fingerprint] = fingerprint_counts.get(fingerprint, 0) + 1
                core_counts[core_fingerprint] = core_counts.get(core_fingerprint, 0) + 1
            elif review_infrastructure_failure:
                # Reviewer transport/schema failures are not source failure
                # fingerprints and must not consume Worker recovery budget.
                task.failure_fingerprint = ""
            if result.git_result is not None and result.git_result.success:
                task.worktree_disposition = (
                    "PRESERVED"
                    if (result.git_result.changed_files or task.changed_files)
                    else "NO_DELTA"
                )
            from selective_revalidation import plan_revalidation
            current_revalidation_identity = {
                "contract_revision": int(task.materialized_execution.get("contract_revision", 0)),
                "profile_revision": str(task.materialized_execution.get("profile_revision", "")),
                "policy_revision": str(task.materialized_execution.get("policy_revision", "")),
                "execution_surface_hash": str(task.materialized_execution.get("execution_surface_hash", "")),
                "candidate_hash": hashlib.sha256(json.dumps(
                    task.changed_file_sha256, sort_keys=True
                ).encode("utf-8")).hexdigest(),
                "test_scope_hash": task.test_scope_hash,
                "test_evidence_hash": hashlib.sha256(json.dumps(
                    task.test_evidence, sort_keys=True, default=str
                ).encode("utf-8")).hexdigest(),
                "review_evidence_hash": hashlib.sha256(str(task.review_result).encode("utf-8")).hexdigest(),
                "runtime_process_id": task.runtime_process_id,
                "runtime_health": dict(task.execution_runtime or {}).get("last_event", ""),
            }
            previous_revalidation_identity = dict(
                task.attempt_history[-1].get("revalidation_identity", {})
            ) if task.attempt_history else {}
            task.selective_revalidation = plan_revalidation(
                previous_revalidation_identity, current_revalidation_identity
            ).public()
            task.attempt_history.append({
                "attempt": attempt,
                "success": result.success,
                "failure_code": task.failure_code,
                "failure_type": task.failure_type,
                "severity": task.severity,
                "verification_status": task.verification_status,
                "review_status": task.review_status,
                "worktree_disposition": task.worktree_disposition,
                "secondary_failures": list(result.secondary_failures),
                "failure_fingerprint": task.failure_fingerprint,
                "retry_strategy": task.retry_strategy,
                "requested_worker": task.requested_worker,
                "requested_model": task.requested_model,
                "planned_worker": task.planned_worker,
                "planned_model": task.planned_model,
                "actual_worker": task.actual_worker,
                "actual_model": task.actual_model,
                "selected_worker": task.selected_worker,
                "selected_model": task.selected_model,
                "selection_reason": task.selection_reason,
                "actual_executable": task.actual_executable,
                "actual_invocation_id": task.actual_invocation_id,
                "worker_execution_evidence": dict(task.worker_execution_evidence),
                "checkpoint_id": task.checkpoint_id,
                "restored_checkpoint_id": task.restored_checkpoint_id,
                "restored_evidence_epoch": task.restored_evidence_epoch,
                "worker_attempt_limit_configured": int(task.worker_attempt_limit_configured),
                "worker_attempt_limit_effective": int(task.worker_attempt_limit_effective),
                "reviewer_infra_retry_limit": int(task.reviewer_recovery_limit),
                "commit_blockers": list(task.commit_blockers),
                "eol_repair_diagnostics": list(task.eol_repair_diagnostics),
                "revalidation_identity": current_revalidation_identity,
                "selective_revalidation": dict(task.selective_revalidation),
            })

            # 로그
            if result.git_result and result.git_result.success:
                print(f"[{task.task_id}][Git] changed_files: {result.git_result.changed_files}")
            else:
                gi = result.git_result
                print(f"[{task.task_id}][Git] 수집 실패 또는 변경 없음: "
                      f"{gi.error if gi else 'N/A'}")

            if result.build_test_result:
                print(f"[{task.task_id}][Build/Test] "
                      f"build={result.build_test_result.build_status}, "
                      f"test={result.build_test_result.test_status}")

            if result.review_result:
                risk = getattr(result.review_result, "risk", "")
                risk_label = f", risk={risk}" if risk else ""
                print(f"[{task.task_id}][Review] "
                      f"status={result.review_result.status}{risk_label}, "
                      f"passed={result.review_result.passed}, "
                      f"reason={result.review_result.reason}")
                for d in result.review_result.details:
                    print(f"  - {d}")

            if result.success:
                print(f"\n✅ {wtag}[{task.task_id}] 시도 {attempt} 성공")
                task.status = (
                    "SUCCESS" if task.verification_status == "VERIFIED" else "AWAITING_QA"
                )
                task.stage = "DONE" if task.status == "SUCCESS" else "AWAITING_QA"
                task.save(self.task_root)
                return result

            print(f"\n❌ {wtag}[{task.task_id}] 시도 {attempt} 실패")

            if task.failure_code == "PROFILE_DRIFT":
                uncertain_delta = task.worktree_disposition in {"PRESERVED", "UNKNOWN"}
                task.keep_working_tree = uncertain_delta
                task.status = "AWAITING_QA" if uncertain_delta else "FAILED"
                task.stage = "AWAITING_QA" if uncertain_delta else "DONE"
                task.verification_status = "PARTIAL" if uncertain_delta else "FAILED"
                task.save(self.task_root)
                print(
                    f"{wtag}[{task.task_id}] PROFILE_DRIFT — 자동 재시도/rollback 중단"
                )
                return result

            reviewer_recovery_exhausted = (
                task.failure_stage == "REVIEW"
                and result.review_result is not None
                and result.review_result.status in REVIEW_INFRA_STATUSES
            )
            build_unverified_wait = (
                task.failure_code == BUILD_NOT_VERIFIED
                and task.build.get("status") == "SKIPPED"
                and task.review_status in (REVIEW_PASS, REVIEW_ERROR, REVIEW_UNAVAILABLE)
            )
            technical_recovery = task.failure_code in TECHNICAL_RECOVERY_CODES
            if reviewer_recovery_exhausted:
                try:
                    import handoff
                    handoff.attach_handoff(task)
                except Exception as e:
                    print(f"[{task.task_id}][Handoff] 스킵: {e}")
                # Review was already retried in-place without invoking Worker.
                # Exhausting that independent infrastructure budget is a
                # human gate, not proof that the task-owned delta is invalid.
                # Preserve it for inspection; an outer retry must not silently
                # discard source that has not received a semantic verdict.
                task.failure_type = "infrastructure"
                task.fix_scope = "NON_RETRYABLE"
                task.user_intervention_reason = "REVIEWER_RECOVERY_EXHAUSTED"
                task.verification_status = "PARTIAL"
                task.keep_working_tree = bool(task.changed_files)
                task.worktree_disposition = (
                    "PRESERVED" if task.changed_files else "NO_DELTA"
                )
                task.status = "AWAITING_QA"
                task.stage = "AWAITING_QA"
                task.save(self.task_root)
                return result

            if build_unverified_wait and not technical_recovery:
                try:
                    import handoff
                    handoff.attach_handoff(task)
                except Exception as e:
                    print(f"[{task.task_id}][Handoff] 스킵: {e}")
                task.failure_type = "build"
                task.fix_scope = "NON_RETRYABLE"
                task.user_intervention_reason = BUILD_NOT_VERIFIED
                task.verification_status = "PARTIAL"
                task.keep_working_tree = bool(task.changed_files)
                task.worktree_disposition = (
                    "PRESERVED" if task.changed_files else "NO_DELTA"
                )
                task.status = "AWAITING_QA"
                task.stage = "AWAITING_QA"
                task.save(self.task_root)
                return result

            if task.fix_scope == "NON_RETRYABLE":
                if technical_recovery:
                    task.fix_scope = "RETRYABLE"
                    task.failure_type = "technical"
                else:
                    rollback_ok = self._rollback_for_outer_retry(
                        task, "non-retryable failure"
                    )
                    rollback_message = task.worktree_disposition
                    task.attempt_history[-1]["worktree_disposition"] = task.worktree_disposition
                    print(
                        f"[{task.task_id}] 비재시도 실패 롤백 결과: {rollback_message} "
                        f"| is_rolled_back={task.is_rolled_back}"
                    )
                    if task.failure_code == CHECKPOINT_CAPTURE_FAILED:
                        return result
                    if not rollback_ok:
                        task.failure_reason = (
                            f"{task.failure_reason}; rollback failed: "
                            f"{scrub_secrets(rollback_message)}"
                        ).strip("; ")
                        task.failure_type = "infrastructure"
                        task.verification_status = "FAILED"
                    print(
                        f"{wtag}[{task.task_id}] 재시도 중단: "
                        f"failure_type={task.failure_type}, fix_scope={task.fix_scope}"
                    )
                    task.status = "FAILED"
                    task.stage = "DONE"
                    task.save(self.task_root)
                    return result

            if (
                task.failure_stage == "WORKER"
                and task.failure_reason == WORKER_APPROVAL_WAIT_REASON
            ):
                print(
                    f"{wtag}[{task.task_id}] 승인 대기 응답 — "
                    "비대화형 Worker에서 해결 불가, 동일 재시도 중단"
                )
                task.status = "FAILED"
                task.stage = "DONE"
                task.save(self.task_root)
                return result

            # 등급 기반 재시도 전략: HIGH만 롤백, LOW/MID와 Build/Test는 워킹트리 유지.
            # - 리뷰어가 LOW/MID를 줘도 하드 제약 위반이면 reviewer 가 HIGH로 승격 완료.
            retry_scope = self._decide_retry_scope(task, result)

            if technical_recovery:
                attempt_limit = max(attempt_limit, TECHNICAL_RECOVERY_MAX_ATTEMPTS)
                current_count = fingerprint_counts.get(task.failure_fingerprint, 0)
                core_count = core_counts.get(core_fingerprint, 0)
                if (
                    task.failure_code in (WORKER_PATCH_CONFLICT, WORKER_PATCH_CONTEXT_STALE)
                    and current_count == 1
                    and not task.retry_worker
                ):
                    task.retry_strategy = "PATCH_CORRECTION_SAME_WORKER"
                elif current_count >= 2 and not task.retry_worker:
                    fallback_worker, fallback_model, evidence = self._fallback_route(task)
                    task.fallback_availability_evidence = evidence
                    if not fallback_worker:
                        task.user_intervention_reason = "SAFE_FALLBACK_UNAVAILABLE"
                        self._rollback_for_outer_retry(task, "fallback unavailable")
                        return result
                    task.retry_worker = fallback_worker
                    task.retry_model = fallback_model
                    task.reroute_reason = "REPEATED_FAILURE_FINGERPRINT"
                    task.retry_strategy = "ALTERNATE_WORKER_MODEL"
                    attempt_limit = max(attempt_limit, attempt + 1)
                elif task.retry_worker and core_count >= 3 and (
                    task.remediation_cycle < TECHNICAL_REMEDIATION_MAX_CYCLES
                ):
                    task.remediation_cycle += 1
                    task.retry_strategy = "BOUNDED_REMEDIATION"
                    attempt_limit = max(attempt_limit, attempt + 1)
                elif attempt >= attempt_limit:
                    task.user_intervention_reason = "TECHNICAL_RECOVERY_EXHAUSTED"
                    self._rollback_for_outer_retry(task, "technical recovery exhausted")
                    return result
                else:
                    task.retry_strategy = "DIRECT_IMPLEMENTATION"
                task.worker_attempt_limit_effective = int(attempt_limit)
                task.progress_attempt_limit = int(attempt_limit)
                task.attempt_history[-1]["worker_attempt_limit_effective"] = int(
                    attempt_limit
                )

            # Every retryable final failure must return to the original Job
            # baseline before an outer Task can be created.  This covers review
            # failures as well as build/test failures.
            if attempt == attempt_limit:
                self._rollback_for_outer_retry(task, "내부 재시도 소진")
                return result

            task.retry_scope = retry_scope
            task.keep_working_tree = retry_scope in ("partial", "full")
            task.attempt_history[-1].update({
                "recovery_scope": retry_scope,
                "recovery_action": (
                    "FOCUSED_CORRECTION_ON_PRESERVED_CANDIDATE"
                    if retry_scope == "partial"
                    else "EVIDENCE_GUIDED_CORRECTION_ON_PRESERVED_CANDIDATE"
                    if retry_scope == "full"
                    else "ARTIFACT_CAPTURE_THEN_SCOPED_ROLLBACK"
                ),
                "artifact_first": bool(
                    retry_scope in {"partial", "full"}
                    or task.checkpoint_manifest
                ),
                "affected_evidence": {
                    "failure_code": task.failure_code,
                    "failure_fingerprint": task.failure_fingerprint,
                    "review_violation_codes": list(
                        getattr(result.review_result, "violation_codes", ()) or ()
                    ) if result.review_result is not None else [],
                    "task_owned_changed_files": list(task.task_owned_changed_files),
                },
            })
            task.save(self.task_root)

            if retry_scope == "restart":
                reason = (
                    f"실패 단계: {task.failure_stage or 'UNKNOWN'}"
                    + (f" / review_risk: {task.review_risk}" if task.review_risk else "")
                )
                print(f"{wtag}[{task.task_id}] 재시도 전략: 전체 롤백(restart) — {reason}")
                ok = self._rollback_for_outer_retry(task, "inner restart")
                msg = task.worktree_disposition
                task.attempt_history[-1]["worktree_disposition"] = task.worktree_disposition
                print(f"[{task.task_id}] 롤백 결과: {msg} | is_rolled_back={task.is_rolled_back}")
                if task.failure_code == CHECKPOINT_CAPTURE_FAILED:
                    return result
                if not ok:
                    task.failure_reason = (
                        f"{task.failure_reason}; rollback failed: {scrub_secrets(msg)}"
                    ).strip("; ")
                    task.failure_type = "infrastructure"
                    task.fix_scope = "NON_RETRYABLE"
                    task.verification_status = "FAILED"
                    task.status = "FAILED"
                    task.stage = "DONE"
                    task.attempt_history[-1].update({
                        "failure_type": task.failure_type,
                        "verification_status": task.verification_status,
                    })
                    task.save(self.task_root)
                    return result
            else:
                task.worktree_disposition = (
                    "PRESERVED" if task.changed_files else task.worktree_disposition
                )
                scope_label = (
                    "LOW — 워킹트리 유지, 부분 수정만 (지적 외 변경 금지)"
                    if retry_scope == "partial"
                    else "MID — 워킹트리 유지, 같은 요구사항 전면 재작성"
                )
                print(f"{wtag}[{task.task_id}] 재시도 전략: {scope_label}")
                print(f"[{task.task_id}] 롤백 생략 — 워킹트리 유지 "
                      f"(is_rolled_back={task.is_rolled_back})")

            # Handoff: 다음 재시도 프롬프트에 주입할 이전 실패 요약(얕은 훅)
            try:
                import handoff
                summary = handoff.attach_handoff(task)
                if summary:
                    print(f"[{task.task_id}][Handoff] 다음 시도에 피드백 주입 "
                          f"({len(summary)}자)")
            except Exception as e:
                print(f"[{task.task_id}][Handoff] 스킵: {e}")

            task.status = "FAILED" if attempt == attempt_limit else "RUNNING"
            task.stage = "RETRY"
            task.save(self.task_root)

        print(f"\n[{task.task_id}] 최대 재시도 횟수 초과")
        return last_result or ManagerResult(success=False, message="no result")
