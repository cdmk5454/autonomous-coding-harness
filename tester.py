"""
Tester - 실행 전용 검증 에이전트 (코드 수정 금지, 판정 불개입)
- Worker 이후, Reviewer 이전에 실행되어 "증거"를 수집한다.
- droid exec 를 tester 모드(--auto 는 KKM_DROID_AUTO_ANALYSIS 공용, 기본 medium)로 1회 호출:
    * Playwright Test / Playwright MCP: 프론트 UI smoke (진입 경로 → 확인할 UI)
    * DB 도구: 읽기 전용 조회만 허용 (DML/DDL 시도 금지)
    * 0.99.2.1: generic Chrome MCP (Chrome DevTools MCP) capability 제거.
      동적 브라우저 QA는 Momentic(primary)/Stagehand(fallback) QA provider가
      담당하며 Tester 프롬프트는 여기서 참조하지 않는다.
- 결과 스키마 → TaskState:
    test_status  : PASS | FAIL | SKIPPED | ERROR
    test_summary : 짧은 요약
    test_evidence: 단계별 결과 리스트
- 기본 정책(KKM_TEST_STRICT=0): 결과가 성공 판정을 바꾸지 않는다.
- 모든 실패/예외는 ERROR/SKIPPED 로 기록되고 파이프라인을 깨지 않는다.
"""

from __future__ import annotations

import os
import re
import json
import hashlib
import time
import subprocess
import tempfile
from harness_temp import scratch_root
from dataclasses import dataclass, field
from pathlib import Path

from task_state import TaskState
from job_contract import DROID_DEFAULT_CODING_MODEL
from worker import droid_auto_level
from project_profile import ProjectProfile
from context_foundation import build_and_record_context
from runtime_safety import (
    assert_secret_free_argv,
    isolated_subprocess_env,
    scrub_secrets,
    safe_print as print,
    trusted_executable,
)

# 테스터용 droid 모델 (기본: canonical registry primary coding model.
# 은퇴한 custom:GLM-5-Turbo-[Z.AI-Coding]-0 ID는 0.9.0.6 registry에서 제거됨)
TESTER_MODEL = os.environ.get(
    "KKM_TESTER_MODEL", DROID_DEFAULT_CODING_MODEL
)
TESTER_TIMEOUT = int(os.environ.get("KKM_TESTER_TIMEOUT", "600"))


@dataclass
class TesterResult:
    status: str                    # PASS | FAIL | SKIPPED | ERROR
    summary: str = ""
    evidence: list[str] = field(default_factory=list)
    frontend: str = ""             # 선택 세부 (프론트 검증 상태)
    db: str = ""                   # 선택 세부 (DB SELECT 검증 상태)
    receipts: list["TestExecutionReceipt"] = field(default_factory=list)


@dataclass
class TestExecutionReceipt:
    """Harness-observed execution of the frozen test plan command.

    The model cannot create or self-assert a receipt: Harness derives it from
    an actual host subprocess execution of the authoritative frozen test
    command (same trust boundary as BuildTestRunner; see the F06 boundary
    note in build_test_runner.py).
    """
    job_id: str
    execution_id: str
    tester_attempt_id: str
    candidate_id: str
    test_scope_hash: str
    command: str
    required_tests: tuple[str, ...]
    exit_code: int
    status: str
    tool_identity: str
    workspace: str
    started_at: float
    finished_at: float
    assertion_fingerprint: str = ""
    assertions_passed: bool | None = None
    stdout_sha256: str = ""
    stderr_sha256: str = ""
    observation_version: str = ""
    termination_reason: str = ""
    output_complete: bool | None = None
    quiescent: bool | None = None
    process_boundary: str = ""
    observed_process_id: int = 0

    def as_dict(self) -> dict:
        return {
            "job_id": self.job_id, "execution_id": self.execution_id,
            "tester_attempt_id": self.tester_attempt_id,
            "candidate_id": self.candidate_id,
            "test_scope_hash": self.test_scope_hash,
            "command": scrub_secrets(self.command),
            "required_tests": list(self.required_tests),
            "exit_code": self.exit_code, "status": self.status,
            "tool_identity": self.tool_identity, "workspace": self.workspace,
            "elapsed_seconds": round(self.finished_at - self.started_at, 3),
            "assertion_fingerprint": self.assertion_fingerprint,
            "assertions_passed": self.assertions_passed,
            "stdout_sha256": self.stdout_sha256, "stderr_sha256": self.stderr_sha256,
            "observation_version": self.observation_version,
            "termination_reason": self.termination_reason,
            "output_complete": self.output_complete, "quiescent": self.quiescent,
            "process_boundary": self.process_boundary,
            "observed_process_id": self.observed_process_id,
        }


def _authorized_test_actions(task: TaskState) -> list[dict[str, Any]]:
    """Authorized per-test execution specification (F12 §13).

    Coverage is derived from the FROZEN test plan's own command binding, never
    copied from the required_tests declaration:
      * ``test_commands`` maps each required test to its authorized command;
      * without that mapping, the single frozen command can only claim the
        required tests it actually references (deterministic text binding);
      * a required test with no authorized command is NOT covered — a no-op
        command can never claim an unrelated required test.
    """
    if (getattr(task, "materialized_execution", {}) or {}).get("role") == "BATCH_VERIFICATION":
        if task.test_plan.get("evaluation") is None:
            from batch_verification import authorized_batch_test_actions
            return authorized_batch_test_actions(task)
    plan = dict(task.test_plan or {})
    if plan.get("evaluation") is not None:
        return [{"test": test, "command": plan.get("test_commands", {}).get(test, ""),
                 "source": "FROZEN_DETERMINISTIC_COMMAND"} for test in plan.get("required_tests", [])]
    required = [str(item).strip().replace("\\", "/") for item in
                (plan.get("required_tests") or []) if str(item).strip()]
    actions: list[dict[str, Any]] = []
    mapping = plan.get("test_commands")
    if isinstance(mapping, dict) and mapping:
        for test in required:
            command = str(mapping.get(test, "") or "").strip()
            if command:
                actions.append({"test": test, "command": command, "source": "TEST_COMMANDS"})
            else:
                actions.append({"test": test, "command": "", "source": "UNMAPPED"})
        return actions
    command = str(plan.get("command") or "").strip()
    if not command:
        return [{"test": test, "command": "", "source": "NO_COMMAND"} for test in required]
    normalized = command.replace("\\", "/")
    for test in required:
        # Deterministic binding: the authorized command itself references the
        # required test path (argument/token boundary), so executing the
        # command observably executes that test.
        bound = test in normalized.split() or f" {test} " in f" {normalized} " \
            or normalized.endswith(f" {test}") or normalized.endswith(f"/{test}") \
            or f" {test}" in normalized and normalized.split()[-1] == test
        actions.append({"test": test, "command": command if bound else "",
                        "source": "COMMAND_REFERENCE" if bound else "UNBOUND"})
    return actions


def execute_test_receipts(task: TaskState, working_dir: str,
                          timeout: int = TESTER_TIMEOUT) -> list[TestExecutionReceipt]:
    """Execute the frozen authorized test commands under Harness observation.

    F13: this is the approved HOST test boundary — the same trust framework as
    the Build boundary: isolated allowlisted child environment (no ambient
    provider credentials), secret-free argv validation, trusted-executable
    resolution, exact-cwd execution, bounded timeout, observed exit/result.
    The Hermes/droid Tester performs semantic exploratory validation; THIS
    boundary is the authoritative deterministic receipt source (Option A).
    """
    import shlex
    if task.test_plan.get("evaluation") is not None:
        return _execute_deterministic_receipts(task, working_dir, timeout)
    actions = _authorized_test_actions(task)
    workspace = Path(working_dir).resolve()
    receipts: list[TestExecutionReceipt] = []
    attempt_id = str(getattr(task, "tester_attempt_id", "") or "")
    for action in actions:
        command = str(action.get("command") or "")
        started = time.time()
        if not command:
            receipts.append(TestExecutionReceipt(
                task.job_id, task.execution_id, attempt_id,
                str(task.candidate_id or ""), str(task.test_scope_hash or ""),
                "", (str(action.get("test", "")),), -1, "NOT_EXECUTED",
                "authorized-command-missing", str(workspace), started, time.time()))
            continue
        try:
            argv = shlex.split(command)
            if not argv:
                raise ValueError("EMPTY_TEST_COMMAND")
            argv[0] = trusted_executable(argv[0], forbidden_root=workspace)
            child_env = isolated_subprocess_env("tester", task.profile_dir)
            assert_secret_free_argv(argv, env=child_env)
        except Exception as exc:
            receipts.append(TestExecutionReceipt(
                task.job_id, task.execution_id, attempt_id,
                str(task.candidate_id or ""), str(task.test_scope_hash or ""),
                command, (str(action.get("test", "")),), -1, "ERROR",
                f"host-test-boundary:{type(exc).__name__}", str(workspace),
                started, time.time()))
            continue
        tool_identity = str(argv[0])
        try:
            tool_path = Path(argv[0])
            if tool_path.is_file():
                tool_identity = f"{argv[0]}:{__import__('hashlib').sha256(tool_path.read_bytes()).hexdigest()[:16]}"
        except OSError:
            pass
        try:
            completed = subprocess.run(
                argv, capture_output=True, text=True, encoding="utf-8",
                errors="replace", timeout=timeout, cwd=str(workspace),
                env=child_env)
            exit_code = int(completed.returncode)
            status = "PASS" if exit_code == 0 else "FAIL"
        except subprocess.TimeoutExpired:
            exit_code, status = -1, "ERROR"
        except (FileNotFoundError, OSError):
            exit_code, status = -1, "ERROR"
        receipts.append(TestExecutionReceipt(
            task.job_id, task.execution_id, attempt_id,
            str(task.candidate_id or ""), str(task.test_scope_hash or ""),
            command, (str(action.get("test", "")),), exit_code, status,
            tool_identity, str(workspace), started, time.time()))
    return receipts


def _receipt_valid_for(task: TaskState, receipt: TestExecutionReceipt) -> bool:
    """Identity binding (F03/F12 §14): exact match on every required field.

    No conditional tolerance: for the frozen mandatory test contract a missing
    identity field (candidate, command, workspace, tool, exit/result) makes
    the receipt invalid rather than wildcard-matching.
    """
    import math
    if (not isinstance(receipt, TestExecutionReceipt)
            or any(not isinstance(getattr(receipt, name), str) for name in (
                "job_id", "execution_id", "tester_attempt_id", "candidate_id", "test_scope_hash",
                "command", "tool_identity", "workspace", "stdout_sha256", "stderr_sha256"))
            or type(receipt.exit_code) is not int
            or not isinstance(receipt.required_tests, tuple) or not receipt.required_tests
            or any(not isinstance(test, str) or not test for test in receipt.required_tests)
            or len(set(receipt.required_tests)) != len(receipt.required_tests)
            or any(type(value) not in {int, float} or not math.isfinite(value) or value < 0
                   for value in (receipt.started_at, receipt.finished_at))
            or receipt.finished_at < receipt.started_at):
        return False
    current_attempt = str(getattr(task, "tester_attempt_id", "") or "")
    if not current_attempt or receipt.tester_attempt_id != current_attempt:
        return False
    if receipt.job_id != task.job_id:
        return False
    if task.execution_id and receipt.execution_id != task.execution_id:
        return False
    if str(task.test_scope_hash or "") and receipt.test_scope_hash != task.test_scope_hash:
        return False
    if not receipt.candidate_id or receipt.candidate_id != str(task.candidate_id or ""):
        return False
    if not receipt.command.strip() or not receipt.tool_identity.strip():
        return False
    if not receipt.workspace.strip():
        return False
    # The receipt must be bound to THIS candidate workspace (F12 §14).
    try:
        if Path(receipt.workspace).resolve() != Path(str(task.working_dir)).resolve():
            return False
    except OSError:
        return False
    if task.test_plan.get("evaluation") is not None:
        from deterministic_test_policy import assertions_for, fingerprint, POLICY_VERSION
        if task.test_plan["evaluation"].get("policy_version") == POLICY_VERSION:
            from bounded_test_command import RECEIPT_POLICY
            if (receipt.observation_version != RECEIPT_POLICY or receipt.output_complete is not True
                    or receipt.quiescent is not True or receipt.termination_reason
                    or receipt.process_boundary not in {"POSIX_PROCESS_GROUP_NOT_SECURITY_SANDBOX", "WINDOWS_OWNED_JOB_OBJECT"}
                    or type(receipt.observed_process_id) is not int or receipt.observed_process_id <= 0
                    or receipt.exit_code < 0):
                return False
        try:
            expected = fingerprint(assertions_for(task.test_plan, receipt.required_tests))
        except (KeyError, TypeError):
            return False
        if (receipt.assertion_fingerprint != expected
                or type(receipt.assertions_passed) is not bool
                or not re.fullmatch(r"[0-9a-f]{64}", receipt.stdout_sha256)
                or not re.fullmatch(r"[0-9a-f]{64}", receipt.stderr_sha256)):
            return False
        return ((receipt.status == "PASS" and receipt.exit_code == 0 and receipt.assertions_passed)
                or (receipt.status == "FAIL" and
                    (receipt.exit_code != 0 or not receipt.assertions_passed)))
    return ((receipt.status == "PASS" and receipt.exit_code == 0)
            or (receipt.status == "FAIL" and receipt.exit_code != 0))


def arbitrate_tester_result(task: TaskState, tr: TesterResult,
                            receipts: list[TestExecutionReceipt]) -> TesterResult:
    """F03/F12 arbitration: observed proof, complete coverage, aggregate veto.

    PASS requires: identity-bound receipts covering EVERY authorized required
    test, all observed PASS, and no contradiction with the structured result.
    Aggregate veto: ANY valid current receipt with an observed FAIL makes the
    outcome semantic FAIL regardless of a model PASS.  Missing/malformed/
    contradictory proof stays ERROR (technical) and never consumes product
    semantic retry.
    """
    plan = dict(task.test_plan or {})
    tr.receipts = list(receipts)
    if not plan.get("mandatory"):
        return tr  # optional Tester keeps existing semantics
    if plan.get("evaluation") is not None:
        # A current ERROR/malformed observation cannot be filtered away behind
        # a second PASS receipt. Stale unrelated attempts remain inadmissible.
        if any(not _receipt_valid_for(task, r) for r in receipts):
            return TesterResult("ERROR", "TESTER_DETERMINISTIC_RECEIPT_INVALID",
                                evidence=tr.evidence, receipts=tr.receipts)
    valid = [r for r in receipts if _receipt_valid_for(task, r)]
    # Authorized command binding (F12 §14): a receipt may only claim required
    # tests whose AUTHORIZED command it actually executed — a wrong command
    # cannot claim an unrelated test label.
    authorized_commands = {str(action["test"]): str(action["command"])
                           for action in _authorized_test_actions(task)}
    valid = [r for r in valid
             if r.required_tests and all(
                 authorized_commands.get(test, "") == r.command
                 for test in r.required_tests)]
    # Duplicate identity = the full execution identity, not the command alone
    # (F12 §16): same command under a different Attempt/candidate/plan is a
    # different receipt.  Conflicting re-delivery of one identity fails closed.
    seen: dict[tuple, TestExecutionReceipt] = {}
    for receipt in valid:
        key = (receipt.command, receipt.tester_attempt_id, receipt.candidate_id,
               receipt.test_scope_hash, receipt.execution_id, receipt.required_tests)
        prior = seen.get(key)
        if prior is not None and (prior.exit_code, prior.status, prior.assertions_passed,
                                  prior.assertion_fingerprint, prior.stdout_sha256,
                                  prior.stderr_sha256) != (receipt.exit_code, receipt.status,
                                  receipt.assertions_passed, receipt.assertion_fingerprint,
                                  receipt.stdout_sha256, receipt.stderr_sha256):
            return TesterResult(
                status="ERROR", summary="TESTER_RECEIPT_CONFLICT",
                evidence=tr.evidence, receipts=tr.receipts)
        if prior is None:
            seen[key] = receipt
    valid = list(seen.values())
    authorized = _authorized_test_actions(task)
    covered = {test for receipt in valid for test in receipt.required_tests}
    required = {str(action["test"]) for action in authorized}
    coverage_complete = not required or required <= covered
    observed_fail = any(r.status == "FAIL" for r in valid)
    observed_pass_all = bool(valid) and all(
        r.exit_code == 0 and r.status == "PASS" for r in valid)
    if tr.status == "PASS":
        if not valid:
            return TesterResult(
                status="ERROR", summary="TESTER_RECEIPTS_REQUIRED",
                evidence=tr.evidence, receipts=tr.receipts)
        if not coverage_complete:
            return TesterResult(
                status="ERROR", summary="TESTER_RECEIPT_COVERAGE_INCOMPLETE",
                evidence=tr.evidence, receipts=tr.receipts)
        if observed_fail:
            # Aggregate failure veto (§15): a valid observed required-test
            # failure vetoes the model PASS as a semantic FAIL.
            return TesterResult(
                status="FAIL", summary="TESTER_OBSERVED_REQUIRED_FAILURE",
                evidence=tr.evidence, receipts=tr.receipts)
        if not observed_pass_all:
            return TesterResult(
                status="ERROR", summary="TESTER_RECEIPT_PROOF_INVALID",
                evidence=tr.evidence, receipts=tr.receipts)
        return tr
    if tr.status == "FAIL":
        # A genuine observed failure is semantic; an uncorroborated model FAIL
        # cannot be verified either way and stays technical.
        if observed_fail:
            return tr
        if valid:
            return TesterResult(
                status="ERROR", summary="TESTER_RECEIPT_CONTRADICTION",
                evidence=tr.evidence, receipts=tr.receipts)
        return TesterResult(
            status="ERROR", summary="TESTER_RECEIPTS_REQUIRED",
            evidence=tr.evidence, receipts=tr.receipts)
    return tr


def should_run_tester(task: TaskState) -> tuple[bool, str]:
    """
    Tester 실행 조건:
    - ANALYSIS → 항상 스킵
    - MODIFICATION + 변경 파일 있음 + (planner tester_brief 존재
      또는 KKM_TEST_FRONTEND=1 또는 KKM_TEST_DB=1) → 실행
    반환: (실행여부, 사유)
    """
    if task.test_plan.get("mandatory") is True:
        return True, "MANDATORY_TEST_PLAN"
    if task.task_mode != "MODIFICATION":
        return False, f"task_mode={task.task_mode} → 기본 스킵"
    if not task.changed_files:
        return False, "변경 파일 없음 → 스킵"
    has_brief = bool((task.planner_brief or {}).get("tester_brief"))
    flag_on = (
        os.environ.get("KKM_TEST_FRONTEND", "0") == "1"
        or os.environ.get("KKM_TEST_DB", "0") == "1"
    )
    if has_brief or flag_on:
        return True, "tester_brief/플래그 충족"
    return False, "tester_brief 없음 + KKM_TEST_FRONTEND/DB 플래그 OFF → 스킵"


def _build_tester_prompt(
    task: TaskState,
    policy_root: Path | None = None,
    profile: ProjectProfile | None = None,
    manifest_root: Path | None = None,
) -> str:
    if manifest_root is None:
        manifest_root = getattr(task, "_context_manifest_root", None)
    tester_brief = (task.planner_brief or {}).get("tester_brief", "")
    if task.test_plan:
        tester_brief = ("Frozen official TestPlan (Worker-local claims cannot substitute for execution):\n"
                        + json.dumps(task.test_plan, ensure_ascii=False, sort_keys=True) + "\n"
                        + "Execute every required test and applicable derived test. Report commands, exit codes and evidence. "
                        + "Missing capability or missing mandatory test is ERROR, never PASS/SKIPPED.\n" + tester_brief)
    mods = ", ".join(task.target_module) if task.target_module else "(미지정)"
    files = "\n".join(task.changed_files[:30])
    brief_block = f"[Tester 브리프(플래너)]\n{tester_brief}\n\n" if tester_brief else ""
    policy_block = ""
    if policy_root is not None and profile is None:
        blocks: list[str] = []
        required = (
            policy_root / "AGENTS.md",
            policy_root / "hooks" / "validation.md",
            policy_root / "skills" / "project-context" / "SKILL.md",
        )
        optional = (policy_root / "skills" / "testing" / "SKILL.md",)
        try:
            for path in required:
                text = path.read_text(encoding="utf-8").strip()
                if not text:
                    raise OSError("empty policy")
                blocks.append(text)
            for path in optional:
                if path.is_file():
                    text = path.read_text(encoding="utf-8").strip()
                    if text:
                        blocks.append(text)
        except Exception as exc:
            raise RuntimeError("tester policy snapshot unreadable") from exc
        policy_block = "[공통 하네스 규칙]\n" + "\n\n---\n\n".join(blocks) + "\n\n"
    profile_block = ""
    if profile is not None:
        pack = build_and_record_context(
            task=task,
            role="TESTER",
            profile=profile,
            workspace=task.working_dir or profile.workspace_roots[0],
            policy_root=policy_root or Path(os.environ.get("AGENTS_DIR", r"D:\agents")),
            manifest_root=manifest_root,
            changed_files=task.changed_files,
            task_evidence={
                "changed_files": list(task.changed_files),
                "task_owned_changed_files": list(task.task_owned_changed_files),
                "build_evidence": dict(task.build_evidence),
                "test_status": task.test_status,
                "test_summary": task.test_summary,
            },
        )
        profile_block = pack.prompt + "\n\n"

    return (
        "너는 실행 전용 테스터다. 아래 절대 규칙을 준수해라.\n"
        "절대 규칙:\n"
        "1. 코드/파일을 절대 수정하지 마라. 실행·조회·확인만 한다.\n"
        "2. DB 도구는 읽기 전용 조회만 허용한다. INSERT/UPDATE/DELETE/DDL 시도 금지.\n"
        "3. UI 확인은 Playwright Test / Playwright MCP 를 사용한다(브라우저 열기→진입→요소 확인).\n"
        "4. 계정/비밀번호는 MCP/환경 설정에서만 사용하고 출력하지 마라.\n\n"
        f"{policy_block}"
        f"{profile_block}"
        f"{brief_block}"
        f"[작업 정보]\n"
        f"- 요구사항: {task.requirement}\n"
        f"- 타겟 모듈: {mods}\n"
        f"- 변경 파일:\n{files}\n\n"
        "[수행 지침]\n"
        "- UI 진입 경로가 확인되면 실제로 열어 요소를 확인하고 결과를 기록해라.\n"
        "- 확인 불가(서버 미기동 등)면 그 이유를 기록해라(억지로 PASS 내지 마라).\n"
        "- DB 검증 포인트가 있으면 SELECT 로 실제 데이터를 확인해라.\n\n"
        "[출력 형식] (오직 아래 형태만)\n"
        "TEST_RESULT: PASS 또는 FAIL 또는 ERROR\n"
        "SUMMARY: <한 줄 요약>\n"
        "STEPS:\n"
        "- <단계 결과 1>\n"
        "- <단계 결과 2>\n"
    )


def _parse_tester_output(out: str) -> TesterResult:
    """tester 출력 파싱. 파싱 실패 시 status=ERROR (판정 불개입, 증거만 보존)."""
    m = re.search(r"TEST_RESULT:\s*(PASS|FAIL|ERROR)", out, re.IGNORECASE)
    if not m:
        return TesterResult(
            status="ERROR", summary="tester 응답 파싱 실패(TEST_RESULT 없음)"
        )
    status = m.group(1).upper()
    sm = re.search(r"SUMMARY:\s*(.+)", out)
    summary = sm.group(1).strip()[:300] if sm else ""
    steps_m = re.search(r"STEPS:\s*\n?([\s\S]*)\Z", out)
    evidence: list[str] = []
    if steps_m:
        for ln in steps_m.group(1).splitlines():
            ln = ln.strip().lstrip("-").strip()
            if ln:
                evidence.append(ln[:300])
    evidence = evidence[:20]
    frontend = "UI 확인됨" if any("ui" in e.lower() or "페이지" in e for e in evidence) else ""
    db = "SELECT 확인됨" if any("select" in e.lower() for e in evidence) else ""
    return TesterResult(
        status=status, summary=summary, evidence=evidence,
        frontend=frontend, db=db,
    )


def run_tester(
    task: TaskState,
    working_dir: str,
    profile: ProjectProfile | None = None,
    policy_root: Path | None = None,
    manifest_root: Path | None = None,
) -> TesterResult:
    """
    Tester 실행 → TesterResult 반환 + TaskState 매핑.
    모든 예외는 ERROR 로 기록되며 파이프라인을 깨지 않는다.
    """
    resolved = task.effective_policy.get("resolved_tester", {})
    if (task.test_plan.get("evaluation") is not None
            or resolved.get("runtime") == "deterministic"):
        return _run_deterministic_tester(task, working_dir)
    if task.effective_policy.get('resolved_tester',{}).get('runtime') == 'hermes':
        return _run_hermes_tester(task,working_dir)
    if getattr(task,'worker','') == 'hermes':
        return _apply_to_state(task,TesterResult('ERROR','HERMES_TESTER_POLICY_REQUIRED'))
    work_path = Path(working_dir).resolve()
    prompt = _build_tester_prompt(task, policy_root, profile, manifest_root)
    if getattr(task, '_tester_rearm_no_build', False):
        prompt += ('\nHARNESS TESTER RECOVERY: Build already has accepted immutable receipts. '
            'Do not run Maven clean/install/package or npm build; do not rerun Build. '
            'Execute only the frozen source/unit test plan and focused non-DB checks. '
            'No DB connections or SQL. Report required TEST_RESULT promptly; '
            'unavailable DB-backed checks remain deferred under the approved contract.\n')

    with tempfile.NamedTemporaryFile(
        dir=scratch_root(),
        mode="w", suffix=".txt", delete=False, encoding="utf-8"
    ) as f:
        f.write(prompt)
        prompt_file = f.name

    auto_level = droid_auto_level("ANALYSIS")
    resolved = task.effective_policy.get("resolved_tester", {})
    selected_model = resolved.get("model", TESTER_MODEL)
    selected_timeout = resolved.get("timeout", TESTER_TIMEOUT)
    cmd = [
        "droid", "exec",
        "--auto", auto_level,
        "-m", selected_model,
        "--cwd", str(work_path),
        "-f", prompt_file,
    ]
    print(f"[Tester] 실행: droid exec (model={TESTER_MODEL}, "
          f"timeout={TESTER_TIMEOUT}s, auto={auto_level})")
    try:
        cmd[0] = trusted_executable(cmd[0], forbidden_root=work_path)
        profile_dir = profile.profile_dir if profile else task.profile_dir
        if not profile_dir:
            return _apply_to_state(task, TesterResult(
                status="ERROR", summary="validated profile directory unavailable"
            ))
        task.official_tester_invoked = True
        task.official_tester_execution = {"model": selected_model, "started_at": time.time(),
            "execution_surface_hash": task.materialized_execution.get("execution_surface_hash", ""),
            "test_scope_hash": task.test_scope_hash, "status": "RUNNING"}
        child_env = isolated_subprocess_env("tester", profile_dir)
        assert_secret_free_argv(cmd, env=child_env)
        if task.materialized_execution.get('role') in {'CANONICAL_REVALIDATION', 'BATCH_VERIFICATION'}:
            from worker import TimeoutConfig
            from owned_role_transport import run as run_owned_role
            from dataclasses import replace
            callback = getattr(task, '_runtime_event_callback', None)
            if not callable(callback):
                raise RuntimeError('REVALIDATION_PROCESS_CUSTODY_REQUIRED')
            cfg = replace(TimeoutConfig.from_env(), analysis_max=selected_timeout,
                          modification_max=selected_timeout, hard_limit=selected_timeout)
            code, stdout, stderr, timeout_reason = run_owned_role(
                cmd=cmd, stdin_data='', cwd=str(work_path), cfg=cfg,
                requirement=task.requirement, task_mode='ANALYSIS', env=child_env,
                runtime_event=callback, execution_id='INV-TEST-'+__import__('uuid').uuid4().hex,
                execution_role='TESTER', attempt_id=task.tester_attempt_id, runtime_name='droid')
            if timeout_reason:
                raise subprocess.TimeoutExpired(cmd, selected_timeout, stdout, stderr)
            result = subprocess.CompletedProcess(cmd, code, stdout, stderr)
        else:
            result = subprocess.run(
                cmd, capture_output=True, text=True, encoding="utf-8", errors="replace",
                timeout=selected_timeout, cwd=str(work_path), env=child_env)
    except FileNotFoundError:
        tr = TesterResult(status="ERROR", summary="LEGACY_RUNTIME_UNAVAILABLE: droid")
        return _apply_to_state(task, tr)
    except subprocess.TimeoutExpired as exc:
        for label, payload in (('stdout', exc.stdout), ('stderr', exc.stderr)):
            if isinstance(payload, bytes):
                payload = payload.decode('utf-8', errors='replace')
            safe = scrub_secrets(str(payload or ''))
            task.official_tester_execution.update({label:safe[:65536],
                label+'_sha256':hashlib.sha256(safe.encode('utf-8')).hexdigest(),
                label+'_truncated':len(safe)>65536})
        task.official_tester_execution['failure_code'] = 'TESTER_WALL_CLOCK_TIMEOUT'
        tr = TesterResult(status="ERROR", summary=f"tester 타임아웃({TESTER_TIMEOUT}s)")
        return _apply_to_state(task, tr)
    except Exception as e:
        tr = TesterResult(status="ERROR", summary=scrub_secrets(f"tester 예외: {e}"))
        return _apply_to_state(task, tr)
    finally:
        if task.official_tester_execution:
            task.official_tester_execution.update({"finished_at": time.time(), "status": "ENDED"})
        try:
            Path(prompt_file).unlink(missing_ok=True)
        except Exception:
            pass

    raw_stdout = getattr(result, "stdout", "") or ""
    raw_stderr = getattr(result, "stderr", "") or ""
    if isinstance(raw_stdout, bytes):
        raw_stdout = raw_stdout.decode("utf-8", errors="replace")
    if isinstance(raw_stderr, bytes):
        raw_stderr = raw_stderr.decode("utf-8", errors="replace")
    stdout = scrub_secrets(str(raw_stdout))
    stderr = scrub_secrets(str(raw_stderr))
    stdout_bytes = stdout.encode("utf-8")
    stderr_bytes = stderr.encode("utf-8")
    task.official_tester_execution.update({
        "exit_code": int(result.returncode),
        "stdout_sha256": hashlib.sha256(stdout_bytes).hexdigest(),
        "stderr_sha256": hashlib.sha256(stderr_bytes).hexdigest(),
        "stdout": stdout[:65536],
        "stderr": stderr[:65536],
        "stdout_truncated": len(stdout) > 65536,
        "stderr_truncated": len(stderr) > 65536,
    })
    if result.returncode != 0:
        err = stderr[:300]
        tr = TesterResult(
            status="ERROR", summary=f"tester 실행 실패(exit={result.returncode}): {err}"
        )
        return _apply_to_state(task, tr)

    tr = _parse_tester_output(stdout)
    if task.test_plan.get("mandatory") and tr.status == "PASS" and not tr.evidence:
        tr = TesterResult(status="ERROR", summary="MANDATORY_TEST_EVIDENCE_MISSING")
    # F03: a mandatory PASS/FAIL is arbitrated against Harness-observed
    # execution receipts of the frozen test-plan command; prose alone can no
    # longer establish a test PASS.
    if task.test_plan.get("mandatory"):
        receipts = execute_test_receipts(task, working_dir)
        tr = arbitrate_tester_result(task, tr, receipts)
    return _apply_to_state(task, tr)


def mark_tester_skipped(task: TaskState, reason: str) -> TesterResult:
    """실행 조건 미충족 시 SKIPPED 기록."""
    return _apply_to_state(
        task, TesterResult(status="SKIPPED", summary=reason)
    )


def _apply_to_state(task: TaskState, tr: TesterResult) -> TesterResult:
    """TesterResult → TaskState 매핑 (test_status/summary/evidence/...)."""
    task.test_status = tr.status
    task.test_summary = scrub_secrets(tr.summary)
    task.test_evidence = [scrub_secrets(item) for item in tr.evidence]
    task.test_frontend = scrub_secrets(tr.frontend)
    task.test_db = scrub_secrets(tr.db)
    task.test_receipts = [receipt.as_dict() for receipt in getattr(tr, "receipts", [])]
    print(f"[Tester] 결과: status={tr.status} summary={task.test_summary[:120]}")
    return tr


def _run_hermes_tester(task,working_dir):
    factory=getattr(task,'_hermes_tester_factory',None)
    if not callable(factory):
        return _apply_to_state(task,TesterResult('ERROR','HERMES_TESTER_OWNER_REQUIRED'))
    task.official_tester_invoked=False
    task.official_tester_execution={'runtime':'hermes','model':task.effective_policy['resolved_tester']['model'],
        'attempt_id':task.tester_attempt_id,'started_at':time.time(),'status':'RUNNING',
        'execution_surface_hash':task.materialized_execution.get('execution_surface_hash',''),
        'test_scope_hash':task.test_scope_hash}
    try:
        from runtime_contract import assert_role_contract_compatible
        from hermes_runtime_adapter import HermesRuntimeAdapter
        contracts=task.materialized_execution.get('runtime_contracts',{})
        if not contracts.get('roles',{}).get('TESTER'):
            return _apply_to_state(task,TesterResult('ERROR','HERMES_TESTER_CONTRACT_REQUIRED'))
        assert_role_contract_compatible(contracts,HermesRuntimeAdapter.catalog_descriptor(),'TESTER')
        runner=factory(task)
        task.official_tester_invoked=True
        tr=runner.test(working_dir,task)
        # F03: same receipt arbitration on the Hermes tester path.
        if task.test_plan.get('mandatory'):
            receipts=execute_test_receipts(task,working_dir)
            tr=arbitrate_tester_result(task,tr,receipts)
        task.official_tester_execution.update({'binding_id':runner.adapter.binding.binding_id,
            'epoch':runner.adapter.binding.execution_epoch,'native_session_id':runner.adapter.session_id,
            'run_id':runner.adapter.run_id,'exit_code':0 if tr.status in {'PASS','FAIL'} else 1})
        return _apply_to_state(task,tr)
    except Exception as exc:
        return _apply_to_state(task,TesterResult('ERROR',getattr(exc,'code','HERMES_TESTER_RUNTIME_ERROR')))
    finally:
        task.official_tester_execution.update({'finished_at':time.time(),'status':'ENDED'})


def _execute_deterministic_receipts(task, working_dir, timeout=TESTER_TIMEOUT):
    """One actual subprocess per exact frozen command, shared only within this attempt.

    No cross-attempt cache, no Build reuse, no model-created receipts. A failed
    assertion at exit=0 is a test failure, not infrastructure success.
    """
    import shlex
    from deterministic_test_policy import (assert_frozen_selection, assertions_for,
                                           fingerprint, check_output)
    assert_frozen_selection(task)
    workspace = Path(working_dir).resolve()
    if workspace != Path(task.working_dir).resolve() or not workspace.is_dir():
        raise ValueError("DETERMINISTIC_TESTER_WORKSPACE_MISMATCH")
    import math
    from bounded_test_command import run as run_bounded, RECEIPT_POLICY
    if type(timeout) not in {int, float} or not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("TEST_COMMAND_LIMITS_INVALID")
    deadline = time.monotonic() + timeout
    groups = {}
    for test, command in task.test_plan["test_commands"].items():
        groups.setdefault(command, []).append(test)
    receipts = []
    cancelled = getattr(task, "_tester_cancelled", None)
    for command, tests in groups.items():
        labels = tuple(sorted(tests))
        checks = assertions_for(task.test_plan, labels)
        started = time.time()
        stdout = stderr = ""
        code, status, tool, assertion_pass = -1, "ERROR", "authorized-host-boundary", False
        observation = None
        reason = "COMMAND_NOT_OBSERVED"
        try:
            remaining = deadline-time.monotonic()
            if remaining <= 0:
                raise TimeoutError("TEST_STAGE_TIMEOUT")
            argv = shlex.split(command)
            if not argv:
                raise ValueError("EMPTY_TEST_COMMAND")
            argv[0] = trusted_executable(argv[0], forbidden_root=workspace,
                source={**os.environ, "KKM_ENFORCE_TRUSTED_EXECUTABLES": "1"})
            child_env = isolated_subprocess_env("tester", task.profile_dir)
            assert_secret_free_argv(argv, env=child_env)
            executable = Path(argv[0])
            tool = str(executable) + ":" + hashlib.sha256(executable.read_bytes()).hexdigest()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("TEST_STAGE_TIMEOUT")
            observation = run_bounded(argv, cwd=workspace, env=child_env,
                                      timeout=remaining, cancelled=cancelled)
            code = observation.returncode
            stdout = observation.stdout.decode("utf-8", errors="replace")
            stderr = observation.stderr.decode("utf-8", errors="replace")
            reason = observation.reason
            if observation.output_complete and observation.quiescent and not reason:
                assertion_pass = check_output(checks, stdout, stderr)
                status = "PASS" if code == 0 and assertion_pass else "FAIL"
        except (OSError, ValueError, RuntimeError, subprocess.TimeoutExpired) as exc:
            # Controlled identifiers only: exception messages may echo secret argv.
            reason = "TEST_STAGE_TIMEOUT" if isinstance(exc, TimeoutError) else "COMMAND_EXECUTION_ERROR"
        receipt = TestExecutionReceipt(
            task.job_id, task.execution_id, task.tester_attempt_id, task.candidate_id,
            task.test_scope_hash, command, labels, code, status, tool, str(workspace),
            started, max(started, time.time()), fingerprint(checks), assertion_pass,
            hashlib.sha256(stdout.encode()).hexdigest(), hashlib.sha256(stderr.encode()).hexdigest(),
            observation_version=RECEIPT_POLICY, termination_reason=reason,
            output_complete=observation.output_complete if observation else False,
            quiescent=observation.quiescent if observation else False,
            process_boundary=observation.boundary if observation else "NOT_OBSERVED",
            observed_process_id=observation.pid if observation else 0)
        receipts.append(receipt)
        # A technical boundary failure must not launch the next command. Valid
        # prior receipts remain immutable; missing coverage is a technical ERROR.
        if status == "ERROR":
            break
    return receipts


def _run_deterministic_tester(task, working_dir):
    from deterministic_test_policy import assert_frozen_selection, POLICY_VERSION
    task.official_tester_invoked = False
    task.official_tester_execution = {"runtime": "deterministic", "model": "none",
        "policy_version": task.test_plan.get("evaluation", {}).get("policy_version", "UNKNOWN"), "attempt_id": task.tester_attempt_id,
        "test_scope_hash": task.test_scope_hash, "started_at": time.time(),
        "execution_surface_hash": task.materialized_execution.get("execution_surface_hash", ""),
        "status": "RUNNING", "llm_calls": 0}
    try:
        assert_frozen_selection(task)
        task.official_tester_invoked = True
        receipts = execute_test_receipts(task, working_dir)
        evidence = ["AUTHORIZED_COMMAND_OBSERVED " + json.dumps({
            "tests": list(r.required_tests), "status": r.status, "exit_code": r.exit_code,
            "assertion_fingerprint": r.assertion_fingerprint,
            "stdout_sha256": r.stdout_sha256, "stderr_sha256": r.stderr_sha256}, sort_keys=True)
            for r in receipts]
        proposed = TesterResult("PASS", "DETERMINISTIC_COMMAND_ASSERTIONS", evidence=evidence)
        result = arbitrate_tester_result(task, proposed, receipts)
        task.official_tester_execution["observed_command_count"] = len(receipts)
        return _apply_to_state(task, result)
    except Exception as exc:
        return _apply_to_state(task, TesterResult("ERROR", scrub_secrets(getattr(exc, "code", str(exc)))[:200]))
    finally:
        task.official_tester_execution.update(finished_at=time.time(), status="ENDED")
