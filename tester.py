"""
Tester - 실행 전용 검증 에이전트 (코드 수정 금지, 판정 불개입)
- Worker 이후, Reviewer 이전에 실행되어 "증거"를 수집한다.
- droid exec 를 tester 모드(--auto 는 KKM_DROID_AUTO_ANALYSIS 공용, 기본 medium)로 1회 호출:
    * Playwright MCP: 프론트 UI smoke (진입 경로 → 확인할 UI)
    * DB 도구: 읽기 전용 조회만 허용 (DML/DDL 시도 금지)
    * Chrome DevTools MCP: 실패 진단 보조 (선택)
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
        "3. UI 확인은 Playwright MCP 를 사용한다(브라우저 열기→진입→요소 확인).\n"
        "4. 진단이 필요할 때만 Chrome DevTools MCP 를 보조로 사용한다.\n"
        "5. 계정/비밀번호는 MCP/환경 설정에서만 사용하고 출력하지 마라.\n\n"
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
    work_path = Path(working_dir).resolve()
    prompt = _build_tester_prompt(task, policy_root, profile, manifest_root)

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
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=selected_timeout,
            cwd=str(work_path),
            env=child_env,
        )
    except FileNotFoundError:
        tr = TesterResult(status="ERROR", summary="droid 미설치")
        return _apply_to_state(task, tr)
    except subprocess.TimeoutExpired:
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

    if result.returncode != 0:
        err = scrub_secrets(result.stderr or "")[:300]
        tr = TesterResult(
            status="ERROR", summary=f"tester 실행 실패(exit={result.returncode}): {err}"
        )
        return _apply_to_state(task, tr)

    task.official_tester_execution["exit_code"] = result.returncode
    tr = _parse_tester_output(scrub_secrets(result.stdout or ""))
    if task.test_plan.get("mandatory") and tr.status == "PASS" and not tr.evidence:
        tr = TesterResult(status="ERROR", summary="MANDATORY_TEST_EVIDENCE_MISSING")
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
    print(f"[Tester] 결과: status={tr.status} summary={task.test_summary[:120]}")
    return tr
