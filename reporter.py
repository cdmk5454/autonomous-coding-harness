"""
Reporter - 작업 결과를 사람이 읽기 좋게 출력
- print_final_report : 기존 콘솔 출력(LLM 미사용, 순수 파이썬)
- generate_markdown  : TaskState 의 새 필드(review/failure/rollback 등)를
                       반영한 Markdown 보고서 문자열 생성
"""

from __future__ import annotations

from pathlib import Path

from task_state import TaskState
from runtime_safety import scrub_secrets, safe_print as print
from execution_evidence import build_execution_summary


def _review_label(status: str) -> str:
    """review_status 값을 사람이 읽는 라벨로 변환 (구버전 값 포함)."""
    return {
        "REVIEW_PASS": "통과",
        "REVIEW_FAIL": "실패",
        "REVIEW_UNAVAILABLE": "검증 불완전 - 리뷰어 실행 불가(codex/droid 모두 실패)",
        "REVIEW_ERROR": "검증 불완전 - 리뷰어 응답 파싱 실패",
        "PASS": "통과(구버전 값)",
        "FAIL": "실패(구버전 값)",
        "PENDING": "대기",
    }.get(status, status or "N/A")


def _build_label(status: str, task: "TaskState | None" = None) -> str:
    """build status 값을 사람이 읽는 라벨로 변환.
    성공 작업에서 SKIPPED 인 경우 프론트엔드 전용 변경 허용임을 함께 표시."""
    label = {
        "PASS": "통과",
        "FAIL": "실패",
        "SKIPPED": "스킵 (빌드 시스템 없음)",
        "WAITING": "대기",
    }.get(status, status or "N/A")
    if (
        status == "SKIPPED"
        and task is not None
        and task.status == "SUCCESS"
        and task.task_mode == "MODIFICATION"
    ):
        label += " — 프론트엔드 전용 변경으로 성공 허용됨"
    return label


def _test_label(status: str) -> str:
    """tester(test_status) 값을 사람이 읽는 라벨로 변환."""
    return {
        "PASS": "통과",
        "FAIL": "실패 (증거 미충족)",
        "SKIPPED": "스킵",
        "ERROR": "오류 (실행/파싱 실패)",
    }.get(status, status or "N/A")


def _effort_label(effort: str, escalated: bool, reason: str) -> str:
    """실제 reasoning effort와 동적 승격 사유를 간결하게 표시한다."""
    label = effort or "medium"
    if escalated and reason:
        label += f" (승격: {reason})"
    return label


class Reporter:
    def print_console_summary(self, task: TaskState, model: str = "") -> None:
        """작업 완료 후 콘솔에 가독성 있게 내역 출력.
        변경 파일, 변경 요약, 성공/실패 사유, Review 결과 포함.
        """
        icon = "✅" if task.status == "SUCCESS" else "❌"
        print(f"\n{'━'*60}")
        print(f"  {icon} 작업 완료: {task.task_id}")
        print(f"{'━'*60}")

        # 결과
        status_text = task.status
        if task.failure_stage:
            status_text += f" (실패 단계: {task.failure_stage})"
        print(f"  결과    : {status_text}")
        print(f"  워커    : {task.worker}" + (f" / {model}" if model else ""))
        if task.worker == "codex":
            print(
                "  Worker reasoning: "
                + _effort_label(
                    task.worker_effective_reasoning_effort,
                    task.worker_effort_escalated,
                    task.worker_effort_escalation_reason,
                )
            )
        if task.task_mode:
            print(f"  모드    : {task.task_mode}")
        if task.completion_kind:
            kind_label = (
                "기존 상태 충족(독립 리뷰 확인)"
                if task.completion_kind == "ALREADY_SATISFIED" else "파일 변경"
            )
            print(f"  완료유형: {kind_label}")
        print(f"  빌드    : {task.build.get('status', 'N/A')} → "
              f"{_build_label(task.build.get('status', ''), task)}")
        if task.target_module:
            print(f"  모듈    : {', '.join(task.target_module)}")
        if task.target_resources:
            print(f"  리소스  : {', '.join(task.target_resources)}")
        if task.is_rolled_back:
            print(f"  롤백    : 수행됨 (재시도 {task.retry_count}회)")
        elif task.retry_count > 0 and getattr(task, "keep_working_tree", False):
            print(f"  롤백    : 생략(워킹트리 유지, 재시도 {task.retry_count}회)")

        # 변경된 파일
        n_files = len(task.changed_files) if task.changed_files else 0
        print(f"\n  📁 변경된 파일 ({n_files}개):")
        if task.changed_files:
            for f in task.changed_files[:10]:
                print(f"     • {f}")
            if n_files > 10:
                print(f"     ... 외 {n_files - 10}개")
        else:
            print(f"     (없음)")

        if task.artifact_files:
            print(f"\n  📄 Job 산출물 ({len(task.artifact_files)}개):")
            for f in task.artifact_files[:10]:
                print(f"     • {f}")

        # 제외된 로컬 개발 설정 (게이트/리뷰 대상 외, 롤백 보존)
        if task.excluded_files:
            print(f"\n  ⚙ 로컬 개발 설정 변경 ({len(task.excluded_files)}개, 게이트/리뷰 제외):")
            for f in task.excluded_files[:5]:
                print(f"     • {f}")
            if len(task.excluded_files) > 5:
                print(f"     ... 외 {len(task.excluded_files) - 5}개")

        # Tester(실행 전용 검증) 결과
        print(f"\n  🧪 Tester: {task.test_status} → {_test_label(task.test_status)}")
        if task.test_summary:
            print(f"     {task.test_summary[:200]}")
        for ev in (task.test_evidence or [])[:3]:
            print(f"     • {ev[:150]}")

        # Review 결과
        print(f"\n  🔍 Review: {task.review_status} → {_review_label(task.review_status)}")
        print(
            "     Reviewer reasoning: "
            + _effort_label(
                task.reviewer_effective_reasoning_effort,
                task.reviewer_effort_escalated,
                task.reviewer_effort_escalation_reason,
            )
        )
        if task.review_status == "REVIEW_FAIL" and task.review_risk:
            scope_desc = {
                "partial": "워킹트리 유지 — 부분 수정",
                "full": "워킹트리 유지 — 전면 재작성",
                "restart": "롤백 후 재시작",
            }.get(task.retry_scope, "")
            line = f"     리스크 등급: {task.review_risk}"
            if scope_desc:
                line += f" ({scope_desc})"
            print(line)
        if task.review_result:
            first_line = task.review_result.strip().splitlines()[0][:200]
            if first_line:
                print(f"     {first_line}")
        if task.review_violations:
            print(f"     주요 지적:")
            for v in task.review_violations[:5]:
                print(f"     ⚠ {v}")

        # 실패 사유
        if task.failure_reason:
            reason = task.failure_reason.strip()[:300]
            print(f"\n  ⚠ 실패 사유: {reason}")

        # 워커 응답 요약
        if task.worker_stdout:
            lines = task.worker_stdout.strip().splitlines()
            preview = lines[:5]
            print(f"\n  📝 워커 응답 (앞 {len(preview)}줄):")
            for line in preview:
                print(f"     {line[:200]}")
            if len(lines) > 5:
                print(f"     ... ({len(lines) - 5}줄 더)")

        # 리포트 파일 경로 (21차: 일자 폴더)
        rel = (TaskState.task_date_dir(task.task_id) / f"{task.task_id}.md").as_posix()
        print(f"\n  📄 상세 리포트: {rel}")
        print(f"{'━'*60}")

    def print_final_report(self, task: TaskState, model: str = "") -> None:
        print("\n" + "=" * 60)
        print("최종 리포트")
        print("=" * 60)

        print(f"task_id       : {task.task_id}")
        print(f"status        : {task.status}")
        print(f"task_mode     : {task.task_mode or '(미지정)'}")
        print(f"completion_kind: {task.completion_kind or '(미확정)'}")
        print(f"stage         : {task.stage}")
        print(f"retry_count   : {task.retry_count}")
        print(f"is_rolled_back: {task.is_rolled_back}")
        print(f"worker        : {task.worker}")
        if task.worker == "codex":
            print(
                "worker_reasoning: "
                + _effort_label(
                    task.worker_effective_reasoning_effort,
                    task.worker_effort_escalated,
                    task.worker_effort_escalation_reason,
                )
            )
        print(
            "reviewer_reasoning: "
            + _effort_label(
                task.reviewer_effective_reasoning_effort,
                task.reviewer_effort_escalated,
                task.reviewer_effort_escalation_reason,
            )
        )
        if task.target_module:
            print(f"target_module : {', '.join(task.target_module)}")
        if task.criticality != "NORMAL":
            print(f"criticality   : {task.criticality}")
        if model:
            print(f"model         : {model}")
        print(f"working_dir   : {task.working_dir}")
        print(f"changed_files : {task.changed_files if task.changed_files else '(없음)'}")
        print(f"target_resources: {task.target_resources if task.target_resources else '(모듈 단위)'}")
        print(f"eol_repairs   : {task.eol_repairs if task.eol_repairs else '(없음)'}")
        print(f"commit_eligible: {task.commit_eligible}")
        if task.commit_blockers:
            print(f"commit_blockers: {task.commit_blockers}")
        print(f"artifact_files: {task.artifact_files if task.artifact_files else '(없음)'}")
        print(f"build         : {task.build.get('status')} ({_build_label(task.build.get('status', ''), task)})")
        print(f"test          : {task.test.get('status')}")
        print(f"review_status : {task.review_status} ({_review_label(task.review_status)})")
        print(f"exit_code     : {task.exit_code}")
        if task.failure_stage:
            print(f"failure_stage : {task.failure_stage}")
        if task.failure_reason:
            print(f"failure_reason: {task.failure_reason[:200]}")

        print("\n" + "-" * 60)
        print("요구사항")
        print("-" * 60)
        print(task.requirement)

        print("\n" + "-" * 60)
        print("Git status")
        print("-" * 60)
        print(task.git_status if task.git_status else "(없음)")

        if task.git_diff_stat:
            print("\n" + "-" * 60)
            print("Git diff --stat")
            print("-" * 60)
            print(task.git_diff_stat)

        print("\n" + "-" * 60)
        print("Droid 응답")
        print("-" * 60)
        print(task.worker_stdout.strip() if task.worker_stdout else "(없음)")

        if task.worker_stderr:
            print("\n" + "-" * 60)
            print("stderr")
            print("-" * 60)
            print(task.worker_stderr.strip())

        print("\n" + "=" * 60)
        if task.status == "SUCCESS":
            print("✅ 전체 작업 성공")
        else:
            print("❌ 전체 작업 실패")
        print("=" * 60)

    def generate_markdown(self, task: TaskState, model: str = "") -> str:
        """
        TaskState 를 사람이 읽기 쉬운 Markdown 보고서로 변환(순수 파이썬, LLM 미사용).
        새 State 필드(changed_files, review_result, failure_reason,
        is_rolled_back, retry_count, target_module, criticality 등)를 반영.
        """
        icon = "✅" if task.status == "SUCCESS" else "❌"
        lines: list[str] = []
        lines.append(f"# {icon} 작업 리포트 — {task.task_id}")
        lines.append("")

        # ---- 요약 ----
        lines.append("## 요약")
        lines.append("")
        lines.append(f"- **상태**: `{task.status}` (stage: `{task.stage}`)")
        lines.append(f"- **작업 모드**: `{task.task_mode or '(미지정 → 키워드 추정)'}`")
        if task.completion_kind:
            kind_label = (
                "기존 상태 충족 — 독립 리뷰 확인"
                if task.completion_kind == "ALREADY_SATISFIED" else "파일 변경"
            )
            lines.append(f"- **완료 유형**: `{task.completion_kind}` ({kind_label})")
        lines.append(f"- **워커**: `{task.worker}`" + (f" / 모델: `{model}`" if model else ""))
        if task.worker == "codex":
            lines.append(
                "- **Worker reasoning**: `"
                + _effort_label(
                    task.worker_effective_reasoning_effort,
                    task.worker_effort_escalated,
                    task.worker_effort_escalation_reason,
                )
                + "`"
            )
        lines.append(
            "- **Reviewer reasoning**: `"
            + _effort_label(
                task.reviewer_effective_reasoning_effort,
                task.reviewer_effort_escalated,
                task.reviewer_effort_escalation_reason,
            )
            + "`"
        )
        if task.target_module:
            lines.append(f"- **타겟 모듈**: `{', '.join(task.target_module)}`")
        if task.target_resources:
            lines.append(f"- **타겟 리소스**: `{', '.join(task.target_resources)}`")
        if task.criticality != "NORMAL":
            lines.append(f"- **중요도(criticality)**: `{task.criticality}`")
        lines.append(f"- **작업 디렉토리**: `{task.working_dir}`")
        lines.append(
            f"- **빌드/테스트**: build=`{task.build.get('status')}` "
            f"({_build_label(task.build.get('status', ''), task)}), "
            f"test=`{task.test.get('status')}`"
        )
        lines.append(
            f"- **리뷰 결과**: `{task.review_status}` "
            f"({_review_label(task.review_status)})"
            + (f" | **리스크**: `{task.review_risk}`" if task.review_risk else "")
        )
        if task.review_status == "REVIEW_FAIL" and task.retry_scope:
            scope_desc = {
                "partial": "워킹트리 유지 — 부분 수정",
                "full": "워킹트리 유지 — 전면 재작성",
                "restart": "롤백 후 재시작",
            }.get(task.retry_scope, task.retry_scope)
            lines.append(f"- **재시도 전략**: {scope_desc}")
        lines.append(
            f"- **테스트(Tester)**: `{task.test_status}` ({_test_label(task.test_status)})"
            + (f" | 플래너: 사용" if task.use_planner else " | 플래너: 미사용")
        )
        lines.append(f"- **재시도**: {task.retry_count}회 | **롤백 발생**: {'예' if task.is_rolled_back else '아니오'}")
        lines.append(f"- **실행 종료 코드**: `{task.exit_code}`")
        if task.failure_stage:
            lines.append(f"- **실패 단계**: `{task.failure_stage}`")
        lines.append("")

        # Invocation/duration evidence is not otherwise present in the legacy
        # report.  Keep gate results in the existing summary instead of
        # duplicating Build/Test/Review/Verification here.
        evidence = dict(task.execution_summary or build_execution_summary(task))
        execution = dict(evidence.get("execution") or {})
        usage = dict(evidence.get("usage") or {})
        lines.append("## Execution summary")
        lines.append("")
        lines.append(f"- **Planner calls**: `{execution.get('planner_invocations', 0)}`")
        lines.append(f"- **Worker calls**: `{execution.get('worker_invocations', 0)}`")
        lines.append(f"- **Reviewer calls**: `{execution.get('reviewer_invocations', 0)}`")
        lines.append(f"- **Inner attempts**: `{execution.get('inner_attempt_count', 0)}`")
        lines.append(f"- **Reviewer recovery**: `{execution.get('reviewer_recovery_count', 0)}`")
        lines.append(f"- **Rollback count**: `{execution.get('rollback_count', 0)}`")
        duration = execution.get("duration_seconds")
        lines.append(f"- **Duration seconds**: `{duration if duration is not None else 'NOT_AVAILABLE'}`")
        lines.append(
            "- **Provider usage**: "
            f"input=`{usage.get('input_tokens', 'NOT_AVAILABLE')}`, "
            f"output=`{usage.get('output_tokens', 'NOT_AVAILABLE')}`, "
            f"cost=`{usage.get('cost', 'NOT_AVAILABLE')}`"
        )
        lines.append("")

        lines.append("## Harness 실행 증거")
        lines.append("")
        lines.append(f"- **현재 Task 변경**: `{', '.join(task.task_owned_changed_files) or '(없음)'}`")
        lines.append(f"- **상속 Batch delta**: `{', '.join(task.inherited_batch_delta_files) or '(없음)'}`")
        lines.append(f"- **예상 외 외부 dirty**: `{', '.join(task.unexpected_external_dirty_files) or '(없음)'}`")
        lines.append(f"- **pre-job snapshot**: `{task.pre_job_snapshot_id or '(없음)'}`")
        lines.append(f"- **failure fingerprint**: `{task.failure_fingerprint or '(없음)'}`")
        lines.append(f"- **retry strategy**: `{task.retry_strategy}`")
        lines.append(f"- **retry route**: `{task.retry_worker or task.worker}` / `{task.retry_model or model or 'default'}`")
        lines.append(f"- **remediation cycle**: `{task.remediation_cycle}`")
        lines.append(f"- **no-diff gate**: `{task.no_diff_gate}`")
        lines.append(f"- **scoped rollback 대상**: `{', '.join(task.scoped_rollback_targets) or '(없음)'}`")
        lines.append(f"- **보존된 선행 delta**: `{', '.join(task.preserved_predecessor_delta_files) or '(없음)'}`")
        if task.planning_artifact_errors:
            lines.append(f"- **planning artifact 오류**: `{task.planning_artifact_errors[-1]}`")
        if task.user_intervention_reason:
            lines.append(f"- **사용자 개입 필요**: `{task.user_intervention_reason}`")
        lines.append("")

        # ---- 요구사항 ----
        lines.append("## 요구사항")
        lines.append("")
        lines.append("```")
        lines.append(task.requirement.strip() or "(없음)")
        lines.append("```")
        lines.append("")

        # ---- 변경 파일 ----
        lines.append("## 변경 파일")
        lines.append("")
        if task.changed_files:
            for f in task.changed_files:
                lines.append(f"- `{f}`")
        else:
            lines.append("(변경 없음)")
        if task.excluded_files:
            lines.append("")
            lines.append(
                f"*제외된 로컬 개발 설정(게이트/리뷰 대상 외, 롤백 보존): "
                f"`{', '.join(task.excluded_files)}`*"
            )
        if task.eol_repairs:
            lines.append("")
            lines.append(f"- **Git index EOL 자동 복구**: `{', '.join(task.eol_repairs)}`")
        lines.append(f"- **로컬 커밋 가능**: `{'YES' if task.commit_eligible else 'NO'}`")
        if task.commit_blockers:
            lines.append(f"- **커밋 차단 사유**: `{', '.join(task.commit_blockers)}`")
        lines.append("")

        if task.artifact_files:
            lines.append("## Job 산출물")
            lines.append("")
            for f in task.artifact_files:
                lines.append(f"- `{f}`")
            lines.append("")

        # ---- Git diff --stat ----
        if task.git_diff_stat:
            lines.append("## Git diff --stat")
            lines.append("")
            lines.append("```")
            lines.append(task.git_diff_stat.strip())
            lines.append("```")
            lines.append("")

        # ---- Tester 결과 ----
        if task.test_status != "SKIPPED" or task.test_summary:
            lines.append("## 테스트 결과 (Tester)")
            lines.append("")
            lines.append(f"- **상태**: `{task.test_status}` ({_test_label(task.test_status)})")
            if task.test_summary:
                lines.append(f"- **요약**: {task.test_summary}")
            if task.test_frontend or task.test_db:
                lines.append(
                    f"- **세부**: frontend=`{task.test_frontend or '-'}`, "
                    f"db=`{task.test_db or '-'}`"
                )
            if task.test_evidence:
                lines.append("- **증거:**")
                lines.append("")
                for ev in task.test_evidence:
                    lines.append(f"  - {ev}")
            lines.append("")

        # ---- 플래너 브리프 ----
        if task.planner_brief:
            lines.append("## 플래너 브리프")
            lines.append("")
            for key, title in (
                ("worker_brief", "Worker 브리프"),
                ("tester_brief", "Tester 브리프"),
                ("reviewer_brief", "Reviewer 브리프"),
            ):
                if task.planner_brief.get(key):
                    lines.append(f"### {title}")
                    lines.append("")
                    lines.append(task.planner_brief[key].strip())
                    lines.append("")

        # ---- 리뷰 결과 ----
        lines.append("## 리뷰 결과")
        lines.append("")
        if task.review_result:
            body = task.review_result.strip()
            # 방어: 우연히 stream-json 조각이나 프롬프트 템플릿이 흘러든 경우 차단
            if body.lstrip().startswith('{"type":') or "[출력 형식]" in body[:200]:
                lines.append("(리뷰 본문 추출 실패 — TaskState의 redacted review detail 참조)")
            else:
                lines.append(body)
        else:
            lines.append("(리뷰 결과 없음)")
        lines.append("")
        if task.review_violations:
            lines.append("**위반/참고 사항:**")
            lines.append("")
            for v in task.review_violations:
                lines.append(f"- {v}")
            lines.append("")

        # ---- 실패 사유 ----
        if task.failure_reason:
            lines.append("## 실패 사유")
            lines.append("")
            lines.append(f"- 단계: `{task.failure_stage or 'N/A'}`")
            lines.append("```")
            lines.append(task.failure_reason.strip())
            lines.append("```")
            lines.append("")

        # ---- 롤백 이력 ----
        if task.is_rolled_back:
            lines.append("## 롤백 이력")
            lines.append("")
            lines.append(f"- 재시도 중 롤백 수행됨 (총 재시도 {task.retry_count}회)")
            lines.append("")

        return scrub_secrets("\n".join(lines))

    def save_markdown(
        self,
        task: TaskState,
        model: str = "",
        out_dir: str | Path = ".tasks",
    ) -> Path:
        """Markdown 보고서를 파일로 저장. 21차: .tasks/YYYYMMDD/{task_id}.md (일자 폴더)"""
        md = self.generate_markdown(task, model=model)
        from control_paths import (
            atomic_state_write,
            ensure_safe_state_directory,
            ensure_safe_state_root,
        )

        root = ensure_safe_state_root(out_dir, field="task_root", create=True)
        out = ensure_safe_state_directory(
            root,
            TaskState.task_date_dir(task.task_id, root),
            field="task_date_dir",
            create=True,
        )
        path = out / f"{task.task_id}.md"
        return atomic_state_write(
            path, md.encode("utf-8"), root=root, field="task_report"
        )
