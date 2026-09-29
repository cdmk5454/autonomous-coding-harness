"""
Handoff - 재시도 연속성 훅 (세션/재시도 컨텍스트 전달)
- TaskState 를 대체하지 않는다. TaskState.handoff_context 필드에 이전 시도 요약을 채워
  다음 재시도 프롬프트에 주입되도록 돕는 얇은 훅.
- Manager 에 깊이 박지 않는다. run_with_retry 실패 후 1회 호출만.
- 외부 MCP 서버 불필요(로컬 함수). 설정은 mcp/handoff.config.json 참고.
"""

from __future__ import annotations

from task_state import TaskState
from runtime_safety import scrub_secrets

# 이전 시도 출력을 너무 길게 주입하지 않도록 제한
_MAX_STDOUT = 1200
_MAX_STDERR = 600
_MAX_VIOLATIONS = 8


def build_crash_recovery_handoff(task: TaskState, salvage: dict) -> str:
    """Build a compact, evidence-referenced continuation packet for a crash.

    The packet never calls an unfinalized candidate verified.  It points the
    next execution at the immutable artifact and the smallest known Job-owned
    scope so the existing retry path can continue or correct it.
    """
    if task is None or not salvage:
        return ""
    changed = list(salvage.get("task_owned_changed_files") or [])
    completed = dict(salvage.get("completed_evidence") or {})
    parts = [
        "[CRASH_SALVAGE_CANDIDATE — NOT VERIFIED]",
        f"Original Job/Task/execution: {task.job_id} / {task.task_id} / {salvage.get('execution_id', '')}",
        f"Interruption: {salvage.get('interruption_reason', '')}",
        f"Last known stage: {salvage.get('last_known_stage', '')}",
        f"Immutable artifact: {salvage.get('manifest_path', '')}",
        f"Artifact SHA-256: {salvage.get('manifest_sha256', '')}",
        "Candidate files: " + (", ".join(changed) if changed else "NO_TASK_DELTA"),
        "Completed evidence (not reusable as final proof): "
        + ", ".join(f"{key}={value}" for key, value in sorted(completed.items())),
        "Preserve inherited Batch delta. Inspect the salvage candidate first; "
        "continue or correct only the remaining affected scope. Re-run every "
        "required Build/Test/Verification/Review gate before success.",
    ]
    return scrub_secrets("\n".join(parts))


def build_retry_handoff(task: TaskState) -> str:
    """
    실패한 시도의 요약을 생성해 반환(다음 재시도 프롬프트에 부착용).
    TaskState 의 기존 필드만 읽는다(쓰지 않음). 비어 있으면 빈 문자열.
    """
    if task is None:
        return ""

    parts: list[str] = []

    if task.failure_stage:
        parts.append(f"이전 실패 단계: {task.failure_stage}")
    if task.failure_reason:
        parts.append(f"실패 사유: {task.failure_reason.strip()[:500]}")

    strategy = getattr(task, "retry_strategy", "")
    if strategy in {"DIRECT_IMPLEMENTATION", "PATCH_CORRECTION_SAME_WORKER", "ALTERNATE_WORKER_MODEL", "BOUNDED_REMEDIATION"}:
        parts.append(
            "[강제 복구 전략] task_plan.md, 계획 문서, 진행 기록, VERIFY/report를 수정하지 말고 "
            "target application source를 먼저 직접 읽고 구현하라. 계획 문서 patch 실패를 반복하지 마라."
        )
        targets = list(getattr(task, "target_resources", []) or [])
        if targets:
            parts.append("[정확한 target source]\n" + "\n".join(f"- {item}" for item in targets))
        errors = list(getattr(task, "planning_artifact_errors", []) or [])
        if errors:
            parts.append("[이전 tool 오류]\n" + "\n".join(f"- {item[:400]}" for item in errors[-2:]))
        if strategy == "ALTERNATE_WORKER_MODEL":
            parts.append(
                f"[라우팅 변경] worker={getattr(task, 'retry_worker', '')}, "
                f"model={getattr(task, 'retry_model', '')}; 같은 실행 방식을 반복하지 마라."
            )
        if strategy == "PATCH_CORRECTION_SAME_WORKER":
            parts.append(
                "[patch 보정 재시도 — 1회 한정] 실패한 대상 파일을 다시 읽는다. "
                "한 apply_patch payload에서 동일 파일을 두 번 대상으로 지정하지 않는다. "
                "파일별 모든 변경을 하나의 patch operation으로 합치거나 apply_patch 호출을 파일별로 분리한다. "
                "각 patch 성공 후 파일과 Git diff를 다시 읽는다. "
                "CSS-only 정리나 범위 외 재포맷을 하지 않는다."
            )
        if strategy == "BOUNDED_REMEDIATION":
            parts.append(
                "[마지막 bounded remediation] 앞선 실패 증거를 모두 반영해 source 구현만 최소 범위로 교정한다. "
                "이 cycle 뒤에는 자동 반복하지 않는다."
            )

    # 등급 기반 재시도 범위 지시 (retry_scope 는 Manager 가 결정해 저장)
    scope = getattr(task, "retry_scope", "") or ""
    keep_tree = bool(getattr(task, "keep_working_tree", False))
    if scope == "partial":
        parts.append(
            "[재시도 범위: 부분 수정만] 이전 시도의 워킹트리가 그대로 유지되어 있다. "
            "아래 리뷰 지적 사항만 최소 수정하고, 지적되지 않은 코드는 절대 변경하지 말 것."
        )
        if task.changed_files:
            files = ", ".join(task.changed_files[:10])
            parts.append(f"[유지된 변경 파일 힌트] {files}")
    elif scope == "full":
        parts.append(
            "[재시도 범위: 전면 재작성] 이전 시도의 워킹트리가 그대로 유지되어 있다. "
            "롤백하지 말고, 같은 요구사항을 처음부터 다시 구현할 것. "
            "기존 코드가 남아 있으므로 참고하거나 덮어쓸 수 있다."
        )
        if task.changed_files:
            files = ", ".join(task.changed_files[:10])
            parts.append(f"[유지된 변경 파일 힌트] {files}")
    elif scope == "restart":
        parts.append(
            "[재시도 범위: 처음부터] 이전 시도의 변경사항은 롤백되었다. "
            "실패 요약을 반영해 처음부터 다시 구현할 것."
        )
    if scope and getattr(task, "review_risk", ""):
        parts.append(f"리뷰 리스크 등급: {task.review_risk}")

    # 빈 diff 게이트로 인한 FAIL 이면 명시
    if task.failure_stage == "REVIEW" and not (task.git_diff or "").strip():
        parts.append("참고: 변경된 파일/diff 가 없어 자동 FAIL 되었음. 실제로 코드를 변경해야 함.")

    if task.worker_stderr:
        parts.append(f"워커 stderr(일부):\n{task.worker_stderr.strip()[:_MAX_STDERR]}")

    if getattr(task, "restored_for_retry", False):
        parts.append(
            "[RESTORED_FOR_RETRY] pre-rollback checkpoint의 task-owned 파일만 복원되었다. "
            f"복원 파일: {', '.join(task.changed_files[:20])}. 이미 구현된 부분을 전면 재작성하지 말고 "
            "이전 Review finding의 대상만 최소 수정한다. 실패 파일을 다시 읽고 동일 파일을 한 "
            "apply_patch payload에서 여러 번 대상으로 지정하지 않는다. 복원 상태는 성공 증거가 아니며 "
            "전체 diff, Build, Test/Verification, Review를 다시 통과해야 한다."
        )

    if task.review_status in ("FAIL", "REVIEW_FAIL") and task.review_result:
        parts.append(f"리뷰 의견:\n{task.review_result.strip()[:_MAX_STDOUT]}")
    elif task.review_violations:
        joined = "\n".join(f"- {v}" for v in task.review_violations[:_MAX_VIOLATIONS])
        parts.append(f"리뷰 위반/참고:\n{joined}")

    if task.build and task.build.get("status") == "FAIL":
        out = (task.build.get("output") or "").strip()
        if out:
            parts.append(f"빌드 출력(일부):\n{out[-_MAX_STDOUT:]}")

    # Tester(실행 전용) 실패/오류 — Reviewer 가 아닌 실행 증거 문제 요약
    test_status = getattr(task, "test_status", "SKIPPED")
    if test_status in ("FAIL", "ERROR"):
        t_parts = [f"테스트 실행 결과: {test_status}"]
        if task.test_summary:
            t_parts.append(task.test_summary.strip()[:300])
        for ev in (task.test_evidence or [])[:3]:
            t_parts.append(f"- {ev[:200]}")
        parts.append("\n".join(t_parts))

    text = "\n\n".join(p for p in parts if p).strip()
    if not text:
        return ""
    return scrub_secrets(
        "이전 시도에서 아래와 같이 실패했다. 이번 시도에서는 이 문제를 반드시 해결하고 "
        "동일한 실수를 반복하지 말 것.\n\n" + text
    )


def attach_handoff(task: TaskState) -> str:
    """
    Manager.run_with_retry 실패 후 호출: task.handoff_context 를 채운다.
    반환: 생성된 핸드오프 요약(참고용).
    """
    if task is None:
        return ""
    summary = build_retry_handoff(task)
    try:
        task.handoff_context = summary
    except Exception:
        # 필드가 없는 구버전 TaskState 라도 훅 자체는 조용히 통과
        pass
    return summary
