"""
검증 게이트 시나리오 테스트 (외부 하네스, D:\\kkm_test)
- MODIFICATION + build SKIPPED  → FAIL
- MODIFICATION + review UNAVAILABLE/ERROR → FAIL (fail-closed)
- ANALYSIS + 빈 diff → PASS 가능 (build SKIPPED 허용)
- task_mode 명시가 키워드 추정보다 우선
- droid 단일 엔진 리뷰 동작
- 기존 정상 케이스 회귀 (MODIFICATION + build PASS + REVIEW_PASS → SUCCESS)
- 구버전 TaskState JSON 하위 호환

실행: python -m unittest test_gate_scenarios -v
(외부 프로세스 droid/codex/git/mvn 은 호출하지 않는다. 전부 목 처리)
"""

from __future__ import annotations

import io
import json
import os
import subprocess
import harness_temp as tempfile
import threading
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from build_test_runner import BuildTestResult
from git_collector import EolRepairResult, GitDiffResult
from manager import Manager, TaskScopeCoordinator
from reviewer import (
    Reviewer,
    ReviewResult,
    REVIEW_PASS,
    REVIEW_FAIL,
    REVIEW_UNAVAILABLE,
    REVIEW_ERROR,
    DIFF_TOO_LARGE_FOR_AUTOMATED_REVIEW,
    BINARY_DIFF_REQUIRES_MANUAL_REVIEW,
    REVIEW_COVERAGE_PLAN_INVALID,
    REVIEW_OUTPUT_PARSE_FAILED,
    STRUCTURED_OUTPUT_INVALID,
)
from task_state import TaskState, resolve_task_mode
from worker import WorkerResult
from project_profile import load_project_profile


E4U_PROFILE = load_project_profile(
    Path(r"D:\agents\profiles\e4u"), Path(r"D:\inha_dev")
)


_ORIGINAL_TG_ENABLED = os.environ.get("KKM_TG_ENABLED")


def setUpModule():
    """Manager 시나리오가 실제 Telegram 으로 푸시하지 않게 격리한다."""
    os.environ["KKM_TG_ENABLED"] = "0"


def tearDownModule():
    if _ORIGINAL_TG_ENABLED is None:
        os.environ.pop("KKM_TG_ENABLED", None)
    else:
        os.environ["KKM_TG_ENABLED"] = _ORIGINAL_TG_ENABLED


# ── 목(Movie) 컴포넌트 ──────────────────────────────────────────
class FakeWorker:
    def __init__(self, wr: WorkerResult):
        self.wr = wr
        self.calls = 0

    def execute(self, requirement, working_dir, task=None):
        self.calls += 1
        return self.wr


class FakeCollector:
    def __init__(self, git_result: GitDiffResult):
        self.git_result = git_result
        self.collect_calls = 0
        self.rollback_calls = 0

    def capture_baseline(self, task=None):
        return object(), ""

    def collect(self, task=None, baseline=None):
        self.collect_calls += 1
        # 실제 GitCollector.collect 의 State 매핑을 모사
        if task is not None:
            task.changed_files = self.git_result.changed_files
            task.excluded_files = list(
                getattr(self.git_result, "excluded_files", []) or []
            )
            task.git_status = self.git_result.status_short
            task.git_diff_stat = self.git_result.diff_stat
            task.git_diff = self.git_result.diff
        return self.git_result

    def rollback(self, task=None, baseline=None):
        self.rollback_calls += 1
        if task is not None:
            task.is_rolled_back = True
        return True, "fake rollback"

    def repair_eol_to_index(self, task=None, baseline=None):
        """Match the production collector contract without touching a filesystem."""
        return EolRepairResult(True, [], [], "")

    def assess_commit_safety(self, task=None, baseline=None):
        """The fake diff is already the complete deterministic task delta."""
        changed = list(getattr(task, "changed_files", []) or [])
        return True, [], {path: "0" * 64 for path in changed}


class FakeRunner:
    def __init__(self, bt: BuildTestResult):
        self.bt = bt
        self.calls: list[list[str]] = []

    def run(self, changed_files=None, target_modules=None):
        self.calls.append(list(changed_files or []))
        return self.bt


class FakeReviewer:
    def __init__(self, rr: ReviewResult):
        self.rr = rr
        self.calls = 0

    def review(self, task=None, **kwargs):
        self.calls += 1
        # 실제 Reviewer._apply_to_state 와 동일한 state 동기화만 모사
        if task is not None:
            task.review_status = self.rr.status
            task.review_result = self.rr.reason
            task.review_risk = (
                self.rr.risk if self.rr.status == "REVIEW_FAIL" else ""
            )
        return self.rr


class SequenceWorker:
    def __init__(self, results: list[WorkerResult]):
        self.results = list(results)
        self.calls = 0
        self.handoffs: list[str] = []

    def execute(self, requirement, working_dir, task=None):
        self.handoffs.append(task.handoff_context if task is not None else "")
        result = self.results[min(self.calls, len(self.results) - 1)]
        self.calls += 1
        return result


class SequenceRunner:
    def __init__(self, results: list[BuildTestResult]):
        self.results = list(results)
        self.calls = 0

    def run(self, changed_files=None, target_modules=None):
        result = self.results[min(self.calls, len(self.results) - 1)]
        self.calls += 1
        return result


class SequenceReviewer:
    def __init__(self, results: list[ReviewResult]):
        self.results = list(results)
        self.calls = 0
        self.call_args: list[dict] = []

    def review(self, task=None, **kwargs):
        self.call_args.append(dict(kwargs))
        result = self.results[min(self.calls, len(self.results) - 1)]
        self.calls += 1
        return Reviewer._apply_to_state(task, result)


def _wr_ok() -> WorkerResult:
    return WorkerResult(success=True, exit_code=0, stdout="", stderr="", command=[])


def _git(diff: str = "", files=None) -> GitDiffResult:
    return GitDiffResult(
        success=True,
        changed_files=files if files is not None else ([] if not diff else ["src/A.java"]),
        diff=diff,
    )


def _build(status: str) -> BuildTestResult:
    return BuildTestResult(
        success=(status == "PASS"),
        build_status=status,
        test_status="SKIPPED",
    )


class GateScenarioTest(unittest.TestCase):
    """Manager._run_pipeline 게이트 시나리오 (컴포넌트 목 처리)."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self._old_cwd = os.getcwd()
        os.chdir(self._tmp.name)          # .tasks/ 저장 위치 격리
        self.working_dir = self._tmp.name
        self.manager = Manager(
            working_dir=self.working_dir,
            model="",
            max_retry=1,
            timeout=5,
            worker_type="droid",
            max_workers=1,
        )

    def tearDown(self):
        self.manager.shutdown()
        os.chdir(self._old_cwd)
        self._tmp.cleanup()

    def _run(
        self,
        requirement: str,
        task_mode: str,
        worker: WorkerResult | None = None,
        git: GitDiffResult | None = None,
        build: BuildTestResult | None = None,
        review: ReviewResult | None = None,
        use_real_reviewer: bool = False,
    ):
        task = TaskState(
            task_id="TASK-TEST",
            requirement=requirement,
            working_dir=self.working_dir,
            task_mode=task_mode,
        )
        self.manager._get_worker = lambda t: (
            FakeWorker(worker or _wr_ok()), "droid"
        )
        self.manager.collector = FakeCollector(git or _git())
        self.manager.build_test_runner = FakeRunner(build or _build("PASS"))
        if not use_real_reviewer:
            self.manager.reviewer = FakeReviewer(
                review or ReviewResult(passed=True, status=REVIEW_PASS)
            )
        return self.manager._run_pipeline(task), task

    def test_configuration_drift_before_worker_blocks_all_agent_calls(self):
        task = TaskState(
            task_id="TASK-CONFIG-PRE",
            requirement="버그 수정해",
            working_dir=self.working_dir,
            task_mode="MODIFICATION",
        )
        worker = FakeWorker(_wr_ok())
        collector = FakeCollector(_git(diff="", files=[]))
        runner = FakeRunner(_build("PASS"))
        reviewer = FakeReviewer(ReviewResult(passed=True, status=REVIEW_PASS))
        self.manager._get_worker = lambda _task: (worker, "droid")
        self.manager.collector = collector
        self.manager.build_test_runner = runner
        self.manager.reviewer = reviewer

        def drift():
            raise RuntimeError("sensitive changed value")

        self.manager.configuration_guard = drift
        result = self.manager._run_pipeline(task)
        self.assertFalse(result.success)
        self.assertEqual("PROFILE_DRIFT", task.failure_code)
        self.assertEqual("NO_DELTA", task.worktree_disposition)
        self.assertEqual(0, worker.calls)
        self.assertEqual([], runner.calls)
        self.assertEqual(0, reviewer.calls)
        self.assertNotIn("sensitive", task.failure_reason)

    def test_configuration_drift_after_worker_preserves_unknown_delta(self):
        task = TaskState(
            task_id="TASK-CONFIG-POST-WORKER",
            requirement="버그 수정해",
            working_dir=self.working_dir,
            task_mode="MODIFICATION",
        )
        worker = FakeWorker(_wr_ok())
        collector = FakeCollector(_git(diff="", files=[]))
        runner = FakeRunner(_build("PASS"))
        reviewer = FakeReviewer(ReviewResult(passed=True, status=REVIEW_PASS))
        self.manager._get_worker = lambda _task: (worker, "droid")
        self.manager.collector = collector
        self.manager.build_test_runner = runner
        self.manager.reviewer = reviewer
        guard_calls = 0

        def drift_after_worker():
            nonlocal guard_calls
            guard_calls += 1
            if guard_calls >= 4:
                raise RuntimeError("changed")

        self.manager.configuration_guard = drift_after_worker
        result = self.manager.run_with_retry(task)
        self.assertFalse(result.success)
        self.assertEqual(1, worker.calls)
        self.assertEqual([], runner.calls)
        self.assertEqual(0, reviewer.calls)
        self.assertEqual(0, collector.rollback_calls)
        self.assertEqual("PROFILE_DRIFT", task.failure_code)
        self.assertEqual("UNKNOWN", task.worktree_disposition)
        self.assertEqual("AWAITING_QA", task.status)
        self.assertEqual("PARTIAL", task.verification_status)

    # 1) MODIFICATION + build SKIPPED → FAIL
    def test_modification_build_skipped_fails(self):
        result, task = self._run(
            requirement="버그 수정해",
            task_mode="MODIFICATION",
            git=_git(diff="diff --git a/src/A.java b/src/A.java"),
            build=_build("SKIPPED"),
        )
        self.assertFalse(result.success)
        self.assertEqual(task.failure_stage, "BUILD")
        self.assertEqual(task.build["status"], "SKIPPED")
        self.assertIn("build=PASS 필요", task.failure_reason)

    # 1b) build FAIL 은 모든 모드에서 실패 (ANALYSIS 포함)
    def test_analysis_build_fail_fails(self):
        result, task = self._run(
            requirement="구조 분석해줘",
            task_mode="ANALYSIS",
            git=_git(diff="", files=[]),
            build=_build("FAIL"),
        )
        self.assertFalse(result.success)
        self.assertEqual(task.failure_stage, "BUILD")

    # 2) MODIFICATION + REVIEW_ERROR / REVIEW_UNAVAILABLE → FAIL
    def test_modification_review_error_fails(self):
        for status in (REVIEW_ERROR, REVIEW_UNAVAILABLE):
            with self.subTest(status=status):
                result, task = self._run(
                    requirement="기능 추가해",
                    task_mode="MODIFICATION",
                    git=_git(diff="diff --git a/x b/x"),
                    build=_build("PASS"),
                    review=ReviewResult(
                        passed=False,
                        reason="리뷰 검증 불완전",
                        status=status,
                    ),
                )
                self.assertFalse(result.success, status)
                self.assertEqual(task.failure_stage, "REVIEW", status)
                self.assertEqual(task.review_status, status)

    # 3) ANALYSIS + 빈 diff → PASS 가능 (build SKIPPED 허용, 실제 Reviewer 사용)
    def test_analysis_empty_diff_passes(self):
        result, task = self._run(
            requirement="구조 분석해줘",
            task_mode="ANALYSIS",
            git=_git(diff="", files=[]),
            build=_build("SKIPPED"),
            use_real_reviewer=True,
        )
        self.assertTrue(result.success)
        self.assertEqual(task.review_status, REVIEW_PASS)
        self.assertEqual(task.task_mode, "ANALYSIS")

    # 3b) ANALYSIS + REVIEW_UNAVAILABLE → 자동 FAIL 아님 (검증 불완전 경고만)
    def test_analysis_review_incomplete_not_hard_fail(self):
        result, task = self._run(
            requirement="구조 분석해줘",
            task_mode="ANALYSIS",
            git=_git(diff="diff --git a/.idea/x b/.idea/x", files=[".idea/x"]),
            build=_build("SKIPPED"),
            review=ReviewResult(
                passed=False, reason="불가", status=REVIEW_UNAVAILABLE
            ),
        )
        self.assertTrue(result.success)          # 하드 FAIL 아님
        self.assertEqual(task.review_status, REVIEW_UNAVAILABLE)  # 상태는 명확히 남음
        self.assertNotEqual(task.failure_stage, "REVIEW")

    # 3c) ANALYSIS + REVIEW_FAIL → 여전히 실패
    def test_analysis_review_fail_fails(self):
        result, task = self._run(
            requirement="구조 분석해줘",
            task_mode="ANALYSIS",
            git=_git(diff="", files=[]),
            review=ReviewResult(passed=False, reason="위반", status=REVIEW_FAIL),
        )
        self.assertFalse(result.success)
        self.assertEqual(task.failure_stage, "REVIEW")

    # 4) mode 명시가 키워드보다 우선
    def test_explicit_mode_overrides_keywords(self):
        # 키워드는 MODIFICATION 을 시사하지만 명시 모드(ANALYSIS) 우선
        result, task = self._run(
            requirement="버그 수정해",          # '수정' 키워드 포함
            task_mode="ANALYSIS",
            git=_git(diff="", files=[]),
            build=_build("SKIPPED"),
            use_real_reviewer=True,
        )
        self.assertEqual(task.task_mode, "ANALYSIS")
        self.assertTrue(result.success)

        # 반대: 분석 키워드 포함 + 명시 MODIFICATION → 빈 diff면 REVIEW_FAIL
        result2, task2 = self._run(
            requirement="구조 분석해줘",        # '분석' 키워드 포함
            task_mode="MODIFICATION",
            git=_git(diff="", files=[]),
            use_real_reviewer=True,
        )
        self.assertEqual(task2.task_mode, "MODIFICATION")
        self.assertFalse(result2.success)
        self.assertEqual(task2.review_status, REVIEW_FAIL)

    # 4b) resolve_task_mode 단위 확인
    def test_resolve_task_mode(self):
        self.assertEqual(resolve_task_mode("버그 수정해", "ANALYSIS"), "ANALYSIS")
        self.assertEqual(resolve_task_mode("구조 분석해줘", "MODIFICATION"), "MODIFICATION")
        self.assertEqual(resolve_task_mode("버그 수정해", ""), "MODIFICATION")
        self.assertEqual(resolve_task_mode("수정하지 말고 분석만 해줘", ""), "ANALYSIS")
        self.assertEqual(resolve_task_mode("안녕하세요", ""), "ANALYSIS")  # 기본값

    # 5) 기존 정상 케이스 회귀: MODIFICATION + PASS/PASS → SUCCESS
    def test_modification_full_pass_regression(self):
        result, task = self._run(
            requirement="기능 추가해",
            task_mode="MODIFICATION",
            git=_git(diff="diff --git a/x b/x"),
            build=_build("PASS"),
            review=ReviewResult(passed=True, reason="ok", status=REVIEW_PASS),
        )
        self.assertTrue(result.success)
        self.assertEqual(task.review_status, REVIEW_PASS)

    def test_pipeline_preserves_worker_approval_wait_as_root_cause(self):
        """일반 무변경 리뷰/빌드 실패가 승인 대기 원인을 덮어쓰지 않는다."""
        from manager import WORKER_APPROVAL_WAIT_REASON

        worker = WorkerResult(
            success=True,
            exit_code=0,
            stdout="현재 구현을 확인했습니다. 이 설계대로 구현해도 될까요?",
            stderr="",
            command=[],
        )
        result, task = self._run(
            requirement="버그 수정",
            task_mode="MODIFICATION",
            worker=worker,
            git=_git(diff="", files=[]),
            build=_build("SKIPPED"),
            use_real_reviewer=True,
        )

        self.assertFalse(result.success)
        self.assertEqual(task.failure_stage, "WORKER")
        self.assertEqual(task.failure_reason, WORKER_APPROVAL_WAIT_REASON)

    # 6) ANALYSIS + quasar.config.cjs 만 변경 → PASS (로컬 설정은 게이트에서 무시)
    def test_analysis_local_config_only_passes(self):
        result, task = self._run(
            requirement="구조 분석해줘",
            task_mode="ANALYSIS",
            git=GitDiffResult(
                success=True,
                changed_files=[],
                diff="",
                excluded_files=["capui/quasar.config.cjs"],
            ),
            build=_build("SKIPPED"),
            use_real_reviewer=True,
        )
        self.assertTrue(result.success)
        self.assertEqual(task.review_status, REVIEW_PASS)
        self.assertEqual(task.excluded_files, ["capui/quasar.config.cjs"])

    # 7) MODIFICATION + 작업 파일 + quasar.config.cjs 혼합 → 작업 파일만 리뷰 입력
    def test_modification_mixed_local_config_ignored_in_review(self):
        captured: dict = {}

        def fake_droid(
            requirement, changed_files, git_diff, tester_context="", **kwargs
        ):
            captured["changed"] = list(changed_files)
            captured["diff"] = git_diff
            return ("PASS", "작업 파일 검토 통과", [], True, "", "")

        vue_diff = (
            "diff --git a/src/pages/A.vue b/src/pages/A.vue\n"
            "@@ -1 +1 @@\n-old\n+new\n"
        )
        with mock.patch.object(Reviewer, "_codex_review", side_effect=fake_droid):
            result, task = self._run(
                requirement="페이지 수정해",
                task_mode="MODIFICATION",
                git=GitDiffResult(
                    success=True,
                    changed_files=["src/pages/A.vue"],
                    diff_stat="src/pages/A.vue | 2 +-",
                    diff=vue_diff,
                    excluded_files=["capui/quasar.config.cjs"],
                ),
                build=_build("PASS"),
                use_real_reviewer=True,
            )
        self.assertTrue(result.success)
        self.assertEqual(task.review_status, REVIEW_PASS)
        # 리뷰 입력에서 quasar.config.cjs 는 완전히 제외되었는지 확인
        self.assertEqual(captured["changed"], ["src/pages/A.vue"])
        self.assertIn("A.vue", captured["diff"])
        self.assertNotIn("quasar.config.cjs", captured["diff"])

    # 8) MODIFICATION + quasar.config.cjs 만 변경 → '변경 없음' 정책 그대로 FAIL
    def test_modification_local_config_only_fails_as_no_change(self):
        result, task = self._run(
            requirement="페이지 수정해",
            task_mode="MODIFICATION",
            git=GitDiffResult(
                success=True,
                changed_files=[],
                diff="",
                excluded_files=["capui/quasar.config.cjs"],
            ),
            build=_build("PASS"),
            use_real_reviewer=True,
        )
        self.assertFalse(result.success)
        self.assertEqual(task.failure_stage, "REVIEW")
        self.assertEqual(task.review_status, REVIEW_FAIL)
        self.assertIn("변경된 파일이 없음", task.failure_reason)

    def test_verified_already_satisfied_can_succeed_without_task_delta(self):
        """명시 표식 + 독립 리뷰 PASS이면 기존 반영 상태를 성공으로 구분한다."""
        from task_state import ALREADY_SATISFIED_MARKER

        result, task = self._run(
            requirement=(
                "다음 파일 요구사항을 반영해\n"
                "- suc/src/main/java/suc/se/query/SsucSe0020Qry.xml\n"
                "- sucui/src/pages/admin/se/SsucSe0020List.vue"
            ),
            task_mode="MODIFICATION",
            worker=WorkerResult(
                success=True, exit_code=0,
                stdout=f"{ALREADY_SATISFIED_MARKER}\n현재 코드에서 확인 완료",
                stderr="", command=[],
            ),
            git=_git(diff="", files=[]),
            build=_build("PASS"),
            review=ReviewResult(
                passed=True, reason="현재 파일이 요구사항을 충족함", status=REVIEW_PASS,
            ),
        )

        self.assertTrue(result.success)
        self.assertEqual(task.completion_kind, "ALREADY_SATISFIED")
        self.assertEqual(
            self.manager.build_test_runner.calls[-1],
            [
                "suc/src/main/java/suc/se/query/SsucSe0020Qry.xml",
                "sucui/src/pages/admin/se/SsucSe0020List.vue",
            ],
        )

    def test_explicit_paths_trigger_current_state_review_without_marker(self):
        """실제 사고 회귀: HEAD diff를 변경으로 주장해도 task delta가 없으면 독립 검증한다."""
        result, task = self._run(
            requirement="sucui/src/pages/admin/se/SsucSe0020List.vue 수정",
            task_mode="MODIFICATION",
            worker=WorkerResult(
                success=True, exit_code=0,
                stdout="구현 완료했습니다. 실제 Git diff가 존재합니다.",
                stderr="", command=[],
            ),
            git=_git(diff="", files=[]),
            build=_build("PASS"),
            review=ReviewResult(
                passed=True, reason="현재 파일이 이미 충족함", status=REVIEW_PASS,
            ),
        )
        self.assertTrue(result.success)
        self.assertEqual(task.completion_kind, "ALREADY_SATISFIED")

    # 9) MODIFICATION + 프론트 전용 변경 + build SKIPPED → 변경 보존, 검증 미완료
    def test_modification_frontend_only_build_skipped_is_not_verified(self):
        vue_diff = (
            "diff --git a/src/pages/A.vue b/src/pages/A.vue\n"
            "@@ -1 +1 @@\n-old\n+new\n"
        )
        result, task = self._run(
            requirement="페이지 수정해",
            task_mode="MODIFICATION",
            git=GitDiffResult(
                success=True,
                changed_files=["capui/src/pages/A.vue", "capui/src/pages/B.vue"],
                diff=vue_diff,
            ),
            build=_build("SKIPPED"),
            review=ReviewResult(passed=True, reason="ok", status=REVIEW_PASS),
        )
        self.assertFalse(result.success)
        self.assertEqual(task.completion_kind, "CHANGED")
        self.assertEqual(task.failure_stage, "BUILD")
        self.assertEqual(task.failure_code, "BUILD_NOT_VERIFIED")
        self.assertIn("BUILD_NOT_VERIFIED", task.failure_reason)
        self.assertEqual(task.build["status"], "SKIPPED")
        self.assertIs(task.build["success"], False)
        self.assertNotEqual(task.verification_status, "VERIFIED")

    def test_modification_frontend_only_build_pass_still_succeeds(self):
        result, task = self._run(
            requirement="페이지 수정해",
            task_mode="MODIFICATION",
            git=GitDiffResult(
                success=True,
                changed_files=["capui/src/pages/A.vue"],
                diff="diff --git a/src/pages/A.vue b/src/pages/A.vue",
            ),
            build=_build("PASS"),
            review=ReviewResult(passed=True, reason="ok", status=REVIEW_PASS),
        )
        self.assertTrue(result.success)
        self.assertEqual(task.completion_kind, "CHANGED")
        self.assertEqual(task.build["status"], "PASS")
        self.assertIs(task.build["success"], True)
        self.assertEqual(task.build["output"], "")
        self.assertIs(task.build["details"]["structured_evidence_valid"], True)
        self.assertEqual(task.verification_status, "VERIFIED")

    def test_modification_build_pass_status_cannot_override_false_success(self):
        result, task = self._run(
            requirement="페이지 수정해",
            task_mode="MODIFICATION",
            git=GitDiffResult(
                success=True,
                changed_files=["capui/src/pages/A.vue"],
                diff="diff --git a/src/pages/A.vue b/src/pages/A.vue",
            ),
            build=BuildTestResult(
                success=False, build_status="PASS", test_status="SKIPPED",
            ),
            review=ReviewResult(passed=True, reason="ok", status=REVIEW_PASS),
        )
        self.assertFalse(result.success)
        self.assertIs(task.build["success"], False)
        self.assertNotEqual(task.verification_status, "VERIFIED")

    def test_build_skipped_stops_without_retry_or_rollback_and_handoffs(self):
        task = TaskState(
            task_id="TASK-BUILD-NOT-VERIFIED",
            requirement="capui 페이지 수정해",
            working_dir=self.working_dir,
            task_mode="MODIFICATION",
            target_module=["capui"],
        )
        worker = FakeWorker(_wr_ok())
        collector = FakeCollector(GitDiffResult(
            success=True,
            changed_files=["capui/src/pages/A.vue"],
            diff="diff --git a/src/pages/A.vue b/src/pages/A.vue",
        ))
        self.manager.max_retry = 3
        self.manager._get_worker = lambda _: (worker, "droid")
        self.manager.collector = collector
        self.manager.build_test_runner = FakeRunner(_build("SKIPPED"))
        self.manager.reviewer = FakeReviewer(
            ReviewResult(passed=True, reason="ok", status=REVIEW_PASS)
        )

        result = self.manager.run_with_retry(task)

        self.assertFalse(result.success)
        self.assertEqual(worker.calls, 1)
        self.assertEqual(collector.collect_calls, 1)
        self.assertEqual(collector.rollback_calls, 0)
        self.assertFalse(task.is_rolled_back)
        self.assertTrue(task.keep_working_tree)
        self.assertEqual(task.status, "AWAITING_QA")
        self.assertNotEqual(task.status, "SUCCESS")
        self.assertEqual(task.stage, "AWAITING_QA")
        self.assertEqual(task.completion_kind, "CHANGED")
        self.assertEqual(task.failure_code, "BUILD_NOT_VERIFIED")
        self.assertEqual(task.verification_status, "PARTIAL")
        self.assertIn("BUILD_NOT_VERIFIED", task.handoff_context)
        self.assertEqual(len(task.attempt_history), 1)

    def test_final_attempt_build_skipped_cannot_become_success(self):
        self.manager.max_retry = 3
        task = TaskState(
            task_id="TASK-FINAL-BUILD-SKIPPED",
            requirement="페이지 수정해",
            working_dir=self.working_dir,
            task_mode="MODIFICATION",
            retry_count=2,
        )
        self.manager._get_worker = lambda _: (FakeWorker(_wr_ok()), "droid")
        self.manager.collector = FakeCollector(GitDiffResult(
            success=True,
            changed_files=["capui/src/pages/A.vue"],
            diff="diff --git a/src/pages/A.vue b/src/pages/A.vue",
        ))
        self.manager.build_test_runner = FakeRunner(_build("SKIPPED"))
        self.manager.reviewer = FakeReviewer(
            ReviewResult(passed=True, reason="ok", status=REVIEW_PASS)
        )

        result = self.manager._run_pipeline(task)

        self.assertFalse(result.success)
        self.assertEqual(task.failure_code, "BUILD_NOT_VERIFIED")
        self.assertNotEqual(task.verification_status, "VERIFIED")

    # 10) MODIFICATION + 백엔드 변경 + build SKIPPED → FAIL
    def test_modification_backend_change_build_skipped_fails(self):
        java_diff = (
            "diff --git a/src/main/java/A.java b/src/main/java/A.java\n"
            "@@ -1 +1 @@\n-old\n+new\n"
        )
        result, task = self._run(
            requirement="서비스 수정해",
            task_mode="MODIFICATION",
            git=GitDiffResult(
                success=True,
                changed_files=["cap/src/main/java/A.java"],
                diff=java_diff,
            ),
            build=_build("SKIPPED"),
            review=ReviewResult(passed=True, reason="ok", status=REVIEW_PASS),
        )
        self.assertFalse(result.success)
        self.assertEqual(task.failure_stage, "BUILD")
        self.assertEqual(task.failure_code, "BUILD_NOT_VERIFIED")
        self.assertIn("BUILD_NOT_VERIFIED", task.failure_reason)

    # 10b) MODIFICATION + 프론트/백엔드 혼합 + build SKIPPED → FAIL
    def test_modification_mixed_change_build_skipped_fails(self):
        result, task = self._run(
            requirement="페이지 수정해",
            task_mode="MODIFICATION",
            git=GitDiffResult(
                success=True,
                changed_files=["capui/src/pages/A.vue", "cap/src/main/java/A.java"],
                diff="diff --git a/x b/x",
            ),
            build=_build("SKIPPED"),
            review=ReviewResult(passed=True, reason="ok", status=REVIEW_PASS),
        )
        self.assertFalse(result.success)
        self.assertEqual(task.failure_stage, "BUILD")

    # 11) MODIFICATION + 프론트 전용이어도 build FAIL → FAIL
    def test_modification_frontend_build_fail_still_fails(self):
        result, task = self._run(
            requirement="페이지 수정해",
            task_mode="MODIFICATION",
            git=GitDiffResult(
                success=True,
                changed_files=["capui/src/pages/A.vue"],
                diff="diff --git a/src/pages/A.vue b/src/pages/A.vue",
            ),
            build=BuildTestResult(
                success=False, build_status="FAIL", test_status="SKIPPED",
                error="compile failed",
            ),
            review=ReviewResult(passed=True, reason="ok", status=REVIEW_PASS),
        )
        self.assertFalse(result.success)
        self.assertEqual(task.failure_stage, "BUILD")

    # 11b) 프론트 전용 판별 헬퍼 단위 확인
    def test_is_frontend_only_changes(self):
        from task_state import is_frontend_only_changes

        self.assertTrue(is_frontend_only_changes(["capui/src/pages/A.vue"]))
        self.assertTrue(is_frontend_only_changes([
            "capui/a.vue", "itnui/b.vue", "sucui\\c.vue",
        ]))
        self.assertFalse(is_frontend_only_changes(["cap/src/A.java"]))
        self.assertFalse(is_frontend_only_changes([
            "capui/a.vue", "cap/src/A.java",
        ]))
        self.assertFalse(is_frontend_only_changes(["src/A.vue"]))  # 프리픽스 없음 → NG
        self.assertFalse(is_frontend_only_changes([]))


    # 11c) 가시성: 단계 전환 로그 + 워커 종료 중간 요약 (GIT 전)
    def test_stage_logs_and_worker_done_summary(self):
        buf = io.StringIO()
        with redirect_stdout(buf):
            result, task = self._run(
                requirement="버그 수정해",
                task_mode="MODIFICATION",
                git=_git(diff="diff --git a/src/A.java b/src/A.java"),
                build=_build("PASS"),
            )
        self.assertTrue(result.success)
        out = buf.getvalue()
        # 단계 전환 한 줄 로그
        self.assertIn("▶ WORKING", out)
        self.assertIn("▶ GIT", out)
        self.assertIn("▶ BUILD", out)
        self.assertIn("▶ TEST", out)
        self.assertIn("▶ REVIEW", out)
        # 워커 종료 중간 요약 (GIT 전)
        self.assertIn("WORKER-DONE", out)
        self.assertIn("exit_code=0", out)
        self.assertIn("timed_out=False", out)
        self.assertIn("GIT → BUILD → REVIEW", out)
        # 시도 표시 n/{max_retry} — setUp 이 max_retry=1 이므로 (1/1).
        # 실운영 기본 3회는 test_default_inactivity_900_and_retry_3 에서 검증.
        self.assertIn("(1/1)", out)

    # 11c-2) 가시성: worker_no 있으면 단계 로그에 [WorkerN] 태그
    def test_stage_logs_include_worker_tag_when_no_set(self):
        task = TaskState(
            task_id="TASK-TEST",
            requirement="버그 수정해",
            working_dir=self.working_dir,
            task_mode="MODIFICATION",
            worker_no=2,
        )
        self.manager._get_worker = lambda t: (FakeWorker(_wr_ok()), "droid")
        self.manager.collector = FakeCollector(
            _git(diff="diff --git a/src/A.java b/src/A.java")
        )
        self.manager.build_test_runner = FakeRunner(_build("PASS"))
        self.manager.reviewer = FakeReviewer(
            ReviewResult(passed=True, status=REVIEW_PASS)
        )

        buf = io.StringIO()
        with redirect_stdout(buf):
            self.manager._run_pipeline(task)
        out = buf.getvalue()
        self.assertIn("[Worker2]", out)
        self.assertIn("[Worker2][TASK-TEST]", out)

    # 11d) 동일 모듈 Job은 실패하지 않고 앞 Job이 끝날 때까지 대기
    def test_same_module_scope_waits(self):
        coordinator = TaskScopeCoordinator()
        self.assertFalse(coordinator.acquire("A", ["capui"]))
        acquired = threading.Event()
        result = []

        def acquire_b():
            result.append(coordinator.acquire("B", ["capui"]))
            acquired.set()

        thread = threading.Thread(target=acquire_b)
        thread.start()
        self.assertFalse(acquired.wait(0.1))
        coordinator.release("A")
        self.assertTrue(acquired.wait(1))
        self.assertEqual(result, [True])
        coordinator.release("B")
        thread.join(1)

    def test_different_module_scopes_do_not_wait(self):
        coordinator = TaskScopeCoordinator()
        self.assertFalse(coordinator.acquire("A", ["cap"]))
        self.assertFalse(coordinator.acquire("B", ["itn"]))
        coordinator.release("A")
        coordinator.release("B")

    def test_root_scope_blocks_every_module(self):
        coordinator = TaskScopeCoordinator()
        self.assertFalse(coordinator.acquire("A", []))
        acquired = threading.Event()

        def acquire_b():
            coordinator.acquire("B", ["capui"])
            acquired.set()

        thread = threading.Thread(target=acquire_b)
        thread.start()
        self.assertFalse(acquired.wait(0.1))
        coordinator.release("A")
        self.assertTrue(acquired.wait(1))
        coordinator.release("B")
        thread.join(1)


class ReviewerFallbackTest(unittest.TestCase):
    """Reviewer 엔진 폴백/파싱 실패 처리 (subprocess 미호출, 메서드 목 처리)."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.reviewer = Reviewer(working_dir=self._tmp.name, timeout=5)

    def tearDown(self):
        self._tmp.cleanup()

    def _task(self, mode: str) -> TaskState:
        return TaskState(
            task_id="TASK-REV",
            requirement="기능 추가해",
            working_dir=self._tmp.name,
            task_mode=mode,
        )

    def _call(self, task: TaskState):
        return self.reviewer.review(
            requirement=task.requirement,
            worker_success=True,
            exit_code=0,
            changed_files=["src/A.java"],
            worker_stdout="",
            git_diff="diff --git a/src/A.java b/src/A.java",
            task=task,
        )

    # 5a) codex 리뷰 판정 PASS → REVIEW_PASS (자동 PASS 아님, 엔진 판정)
    def test_codex_review_pass(self):
        with mock.patch.object(
            self.reviewer, "_codex_review",
            return_value=("PASS", "리뷰 통과", [], True, "", ""),
        ):
            rr = self._call(self._task("MODIFICATION"))
        self.assertTrue(rr.passed)
        self.assertEqual(rr.status, REVIEW_PASS)

    def test_already_satisfied_claim_runs_current_state_review(self):
        from task_state import ALREADY_SATISFIED_MARKER

        task = self._task("MODIFICATION")
        task.requirement = "sucui/src/pages/admin/se/SsucSe0020List.vue 수정"
        captured: dict = {}
        with mock.patch.object(
            self.reviewer, "_codex_review",
            side_effect=lambda **kw: (
                captured.update(kw) or
                ("PASS", "현재 상태 충족", [], True, "", "")
            ),
        ):
            rr = self.reviewer.review(
                requirement=task.requirement,
                worker_success=True,
                exit_code=0,
                changed_files=[],
                worker_stdout=ALREADY_SATISFIED_MARKER,
                git_diff="",
                task=task,
            )

        self.assertEqual(rr.status, REVIEW_PASS)
        self.assertTrue(captured["current_state"])
        self.assertEqual(
            captured["changed_files"],
            ["sucui/src/pages/admin/se/SsucSe0020List.vue"],
        )

    def test_explicit_path_runs_current_state_review_without_marker(self):
        task = self._task("MODIFICATION")
        task.requirement = "suc/src/main/java/suc/se/query/SsucSe0020Qry.xml 수정"
        captured: dict = {}
        with mock.patch.object(
            self.reviewer, "_codex_review",
            side_effect=lambda **kw: (
                captured.update(kw) or
                ("PASS", "현재 상태 충족", [], True, "", "")
            ),
        ):
            rr = self.reviewer.review(
                requirement=task.requirement, worker_success=True, exit_code=0,
                changed_files=[], worker_stdout="실제 Git diff가 존재합니다.",
                git_diff="", task=task,
            )
        self.assertEqual(rr.status, REVIEW_PASS)
        self.assertTrue(captured["current_state"])

    # 5b) droid 파싱 실패 → REVIEW_ERROR (fail-closed)
    def test_droid_parse_fail_review_error(self):
        with mock.patch.object(
            self.reviewer, "_codex_review",
            return_value=(None, None, ["droid 파싱 실패"], False, "error", ""),
        ):
            rr = self._call(self._task("MODIFICATION"))
        self.assertFalse(rr.passed)
        self.assertEqual(rr.status, REVIEW_ERROR)
        self.assertEqual(self._task_mode_review_status(rr), REVIEW_ERROR)

    @staticmethod
    def _task_mode_review_status(rr: ReviewResult) -> str:
        return rr.status

    # 5c) droid 실행 불가 → REVIEW_UNAVAILABLE (fail-closed)
    def test_droid_unavailable(self):
        with mock.patch.object(
            self.reviewer, "_codex_review",
            return_value=(None, None, ["droid 미설치"], False, "unavailable", ""),
        ):
            rr = self._call(self._task("MODIFICATION"))
        self.assertFalse(rr.passed)
        self.assertEqual(rr.status, REVIEW_UNAVAILABLE)

    # 5d) 리뷰어 판정 FAIL → REVIEW_FAIL
    def test_engine_fail_verdict(self):
        with mock.patch.object(
            self.reviewer, "_codex_review",
            return_value=("FAIL", "위반 발견", ["detail"], True, "", ""),
        ):
            rr = self._call(self._task("MODIFICATION"))
        self.assertFalse(rr.passed)
        self.assertEqual(rr.status, REVIEW_FAIL)

    # 5e) 파서 단위: RESULT/REASON/DETAIL/RISK 파싱
    def test_parse_review_output(self):
        v, r, d, risk = Reviewer._parse_review_output(
            "RESULT: PASS\nREASON: 문제 없음\nDETAIL: 상세 내용\nRISK: NONE"
        )
        self.assertEqual(v, "PASS")
        self.assertEqual(r, "문제 없음")
        self.assertEqual(risk, "")          # PASS → RISK 없음
        v2, _, _, _ = Reviewer._parse_review_output("이상한 응답")
        self.assertIsNone(v2)
        # FAIL + RISK 파싱
        v3, _, _, risk3 = Reviewer._parse_review_output(
            "RESULT: FAIL\nREASON: 라벨 누락\nRISK: LOW"
        )
        self.assertEqual(v3, "FAIL")
        self.assertEqual(risk3, "LOW")
        # RISK 누락은 필수 필드 불완전으로 거부한다.
        v4, _, _, risk4 = Reviewer._parse_review_output(
            "RESULT: FAIL\nREASON: 구조 과다"
        )
        self.assertIsNone(v4)
        self.assertEqual(risk4, "")


class TaskStateCompatTest(unittest.TestCase):
    """구버전 TaskState JSON 하위 호환."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self._old_cwd = os.getcwd()
        os.chdir(self._tmp.name)

    def tearDown(self):
        os.chdir(self._old_cwd)
        self._tmp.cleanup()

    def test_load_old_json_without_task_mode(self):
        base = Path(".tasks")
        base.mkdir(exist_ok=True)
        # task_mode/review_status 신규 필드가 없는 구버전 JSON
        old = {
            "task_id": "TASK-OLD",
            "requirement": "테스트",
            "status": "SUCCESS",
            "review_status": "PASS",
        }
        (base / "TASK-OLD.json").write_text(
            json.dumps(old, ensure_ascii=False), encoding="utf-8"
        )
        task = TaskState.load("TASK-OLD")
        self.assertEqual(task.task_mode, "")       # 기본값 보정
        self.assertEqual(task.review_status, "PASS")  # 구버전 값 그대로
        self.assertEqual(task.failure_code, "")

    def test_load_invalid_task_mode_normalized(self):
        base = Path(".tasks")
        base.mkdir(exist_ok=True)
        bad = {"task_id": "TASK-BAD", "requirement": "x", "task_mode": "잘못된값"}
        (base / "TASK-BAD.json").write_text(
            json.dumps(bad, ensure_ascii=False), encoding="utf-8"
        )
        task = TaskState.load("TASK-BAD")
        self.assertEqual(task.task_mode, "")

    def test_load_old_json_without_risk_fields(self):
        """review_risk/retry_scope/keep_working_tree 없는 구버전 JSON 보정."""
        base = Path(".tasks")
        base.mkdir(exist_ok=True)
        old = {
            "task_id": "TASK-OLD2",
            "requirement": "테스트",
            "review_status": "REVIEW_FAIL",
        }
        (base / "TASK-OLD2.json").write_text(
            json.dumps(old, ensure_ascii=False), encoding="utf-8"
        )
        task = TaskState.load("TASK-OLD2")
        self.assertEqual(task.review_risk, "")
        self.assertEqual(task.retry_scope, "")
        self.assertFalse(task.keep_working_tree)

    def test_load_invalid_risk_normalized(self):
        base = Path(".tasks")
        base.mkdir(exist_ok=True)
        bad = {
            "task_id": "TASK-RISKBAD", "requirement": "x",
            "review_risk": "EXTREME", "retry_scope": "nuke",
            "keep_working_tree": "yes",
        }
        (base / "TASK-RISKBAD.json").write_text(
            json.dumps(bad, ensure_ascii=False), encoding="utf-8"
        )
        task = TaskState.load("TASK-RISKBAD")
        self.assertEqual(task.review_risk, "")    # 비정상값 → 빈값
        self.assertEqual(task.retry_scope, "")
        self.assertFalse(task.keep_working_tree)

    def test_load_old_json_without_effort_history(self):
        """사용자 baseline은 보존하고 신규 실행 이력은 안전하게 보정한다."""
        base = Path(".tasks")
        base.mkdir(exist_ok=True)
        old = {
            "task_id": "TASK-EFFORT-OLD",
            "requirement": "테스트",
            "codex_reasoning_effort": "high",
        }
        (base / "TASK-EFFORT-OLD.json").write_text(
            json.dumps(old, ensure_ascii=False), encoding="utf-8"
        )

        task = TaskState.load("TASK-EFFORT-OLD")

        self.assertEqual(task.codex_reasoning_effort, "high")
        self.assertEqual(task.worker_effective_reasoning_effort, "high")
        self.assertEqual(task.reviewer_effective_reasoning_effort, "medium")
        self.assertFalse(task.worker_effort_escalated)
        self.assertFalse(task.reviewer_effort_escalated)
        self.assertEqual(task.worker_effort_escalation_reason, "")
        self.assertEqual(task.reviewer_effort_escalation_reason, "")


class DynamicReasoningEffortTest(unittest.TestCase):
    """Worker baseline 보존과 attempt/review_risk 기반 high 승격."""

    @staticmethod
    def _resolve(
        attempt: int,
        previous_risk: str = "",
        baseline: str = "medium",
        criticality: str = "NORMAL",
    ):
        from task_state import resolve_worker_reasoning_effort

        task = TaskState(
            task_id="TASK-EFFORT",
            requirement="수정",
            worker="codex",
            codex_reasoning_effort=baseline,
            review_risk=previous_risk,
            criticality=criticality,
        )
        return resolve_worker_reasoning_effort(task, attempt)

    def test_worker_attempt_policy(self):
        cases = (
            (1, "", "medium", ""),
            (2, "LOW", "medium", ""),
            (2, "MID", "high", "previous_review_mid_retry"),
            (2, "HIGH", "high", "previous_review_high"),
            (3, "LOW", "high", "attempt_three_or_later"),
        )
        for attempt, risk, effort, reason in cases:
            with self.subTest(attempt=attempt, risk=risk):
                self.assertEqual(
                    self._resolve(attempt, risk), (effort, reason)
                )

    def test_worker_critical_task_escalates(self):
        self.assertEqual(
            self._resolve(1, criticality="CRITICAL"),
            ("high", "critical_task"),
        )

    def test_worker_baseline_is_never_lowered(self):
        self.assertEqual(self._resolve(1, baseline="low"), ("low", ""))
        self.assertEqual(
            self._resolve(2, previous_risk="MID", baseline="low"),
            ("high", "previous_review_mid_retry"),
        )
        self.assertEqual(self._resolve(1, baseline="high"), ("high", ""))

    def test_manager_passes_effective_effort_to_codex_command(self):
        from manager import Manager

        manager = Manager.__new__(Manager)
        manager.timeout = 5
        manager.timeout_config = None
        manager.model = ""
        task = TaskState(
            task_id="TASK-EFFORT-CMD",
            requirement="수정",
            worker="codex",
            codex_reasoning_effort="low",
            review_risk="MID",
            retry_count=1,
        )
        worker, worker_type = manager._get_worker(task)
        completed = subprocess.CompletedProcess([], 0, stdout="ok", stderr="")

        with tempfile.TemporaryDirectory() as td, \
             mock.patch("worker.build_prompt", return_value="prompt"), \
             mock.patch("worker.codex_host_pair_matches", return_value=True), \
             mock.patch("worker.subprocess.run", return_value=completed) as run_mock:
            result = worker.execute(task.requirement, td, task=task)

        self.assertEqual(worker_type, "codex")
        self.assertTrue(result.success)
        self.assertEqual(task.worker_effective_reasoning_effort, "high")
        self.assertTrue(task.worker_effort_escalated)
        self.assertEqual(
            task.worker_effort_escalation_reason,
            "previous_review_mid_retry",
        )
        cmd = run_mock.call_args.args[0]
        self.assertIn('model_reasoning_effort="high"', cmd)
        self.assertIn("--approve-for-me", cmd)
        self.assertNotIn("-s", cmd)

    def test_reporter_shows_effective_effort_and_reason(self):
        from reporter import Reporter

        task = TaskState(
            task_id="TASK-EFFORT-REPORT",
            requirement="수정",
            status="SUCCESS",
            worker="codex",
            worker_effective_reasoning_effort="high",
            worker_effort_escalated=True,
            worker_effort_escalation_reason="previous_review_mid_retry",
            reviewer_effective_reasoning_effort="high",
            reviewer_effort_escalated=True,
            reviewer_effort_escalation_reason="semantic_high",
        )
        reporter = Reporter()
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            reporter.print_console_summary(task)
        console = stdout.getvalue()
        markdown = reporter.generate_markdown(task)

        self.assertIn(
            "Worker reasoning: high (승격: previous_review_mid_retry)",
            console,
        )
        self.assertIn(
            "Reviewer reasoning: high (승격: semantic_high)",
            console,
        )
        self.assertIn(
            "**Worker reasoning**: `high (승격: previous_review_mid_retry)`",
            markdown,
        )
        self.assertIn(
            "**Reviewer reasoning**: `high (승격: semantic_high)`",
            markdown,
        )


class ReviewerSingleInvocationTest(unittest.TestCase):
    """Reviewer effort is preselected and every semantic review invokes once."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.reviewer = Reviewer(working_dir=self._tmp.name, timeout=5)

    def tearDown(self):
        self._tmp.cleanup()

    def _task(self, criticality: str = "NORMAL", build_status: str = "PASS"):
        return TaskState(
            task_id="TASK-REVIEW-EFFORT",
            requirement="기능 수정",
            task_mode="MODIFICATION",
            working_dir=self._tmp.name,
            criticality=criticality,
            build={"status": build_status},
        )

    def _call(self, responses, task=None, attempt=1, max_attempts=3):
        calls: list[dict] = []
        queue = list(responses)

        def fake_review(**kwargs):
            calls.append(dict(kwargs))
            return queue.pop(0)

        task = task or self._task()
        with mock.patch.object(
            self.reviewer, "_codex_review", side_effect=fake_review
        ):
            result = self.reviewer.review(
                requirement=task.requirement,
                worker_success=True,
                exit_code=0,
                changed_files=["src/A.java"],
                git_diff="diff --git a/src/A.java b/src/A.java",
                task=task,
                attempt=attempt,
                max_attempts=max_attempts,
            )
        efforts = [call["reasoning_effort"] for call in calls]
        return result, task, efforts, calls

    def test_pass_calls_medium_once(self):
        rr, task, efforts, _ = self._call([
            ("PASS", "ok", [], True, "", ""),
        ])
        self.assertEqual(rr.status, REVIEW_PASS)
        self.assertEqual(efforts, ["medium"])
        self.assertEqual(task.reviewer_effective_reasoning_effort, "medium")
        self.assertFalse(task.reviewer_effort_escalated)

    def test_codex_review_command_accepts_high_and_stays_read_only(self):
        captured: dict = {}

        def fake_run(cmd, **kwargs):
            captured["cmd"] = list(cmd)
            out_file = captured["cmd"][captured["cmd"].index("-o") + 1]
            Path(out_file).write_text(
                "RESULT: PASS\nREASON: ok\nRISK: NONE", encoding="utf-8"
            )
            return subprocess.CompletedProcess(cmd, 0, "", "")

        with mock.patch("reviewer.subprocess.run", side_effect=fake_run):
            result = self.reviewer._codex_review(
                "요구사항",
                ["src/A.java"],
                "diff --git a/src/A.java b/src/A.java",
                reasoning_effort="high",
            )

        self.assertTrue(result[3])
        self.assertIn('model_reasoning_effort="high"', captured["cmd"])
        self.assertEqual(
            captured["cmd"][captured["cmd"].index("-s") + 1], "read-only"
        )
        self.assertNotIn("--approve-for-me", captured["cmd"])

    def test_low_mid_and_high_fail_call_medium_once(self):
        for risk in ("LOW", "MID", "HIGH"):
            with self.subTest(risk=risk):
                rr, _, efforts, _ = self._call([
                    ("FAIL", "지적", [], True, "", risk),
                ])
                self.assertEqual(rr.risk, risk)
                self.assertEqual(efforts, ["medium"])

    def test_semantic_high_is_not_overwritten_by_internal_re_review(self):
        rr, task, efforts, calls = self._call([
            ("FAIL", "구조 재작성 필요", [], True, "", "HIGH"),
        ])
        self.assertEqual(efforts, ["medium"])
        self.assertEqual(len(calls), 1)
        self.assertEqual(rr.status, REVIEW_FAIL)
        self.assertEqual(rr.risk, "HIGH")
        self.assertEqual(task.review_risk, "HIGH")
        self.assertFalse(task.reviewer_effort_escalated)

    def test_manager_preselects_high_for_final_attempt_then_reviewer_calls_once(self):
        from manager import Manager

        task = self._task()
        task.retry_count = 2
        manager = Manager.__new__(Manager)
        manager.max_retry = 3
        self.assertEqual(manager._prepare_reviewer_effort(task), "high")
        rr, task, efforts, _ = self._call([
            ("PASS", "high ok", [], True, "", ""),
        ], task=task, attempt=3, max_attempts=3)
        self.assertEqual(rr.status, REVIEW_PASS)
        self.assertEqual(efforts, ["high"])
        self.assertEqual(task.reviewer_effort_escalation_reason, "final_attempt")

    def test_manager_preselects_high_after_previous_semantic_high(self):
        from manager import Manager

        task = self._task()
        task.review_risk = "HIGH"
        task.retry_count = 1
        manager = Manager.__new__(Manager)
        manager.max_retry = 3
        self.assertEqual(manager._prepare_reviewer_effort(task), "high")
        rr, task, efforts, calls = self._call([
            ("PASS", "fixed", [], True, "", ""),
        ], task=task, attempt=2, max_attempts=3)
        self.assertEqual(rr.status, REVIEW_PASS)
        self.assertEqual(efforts, ["high"])
        self.assertEqual(len(calls), 1)
        self.assertEqual(
            task.reviewer_effort_escalation_reason, "previous_review_high"
        )

    def test_manager_preselects_high_for_high_and_critical_tasks(self):
        from manager import Manager

        for criticality in ("HIGH", "CRITICAL"):
            with self.subTest(criticality=criticality):
                task = self._task(criticality=criticality)
                manager = Manager.__new__(Manager)
                manager.max_retry = 3
                self.assertEqual(manager._prepare_reviewer_effort(task), "high")
                rr, task, efforts, _ = self._call([
                    ("PASS", "high ok", [], True, "", ""),
                ], task=task)
                self.assertEqual(rr.status, REVIEW_PASS)
                self.assertEqual(efforts, ["high"])
                self.assertEqual(
                    task.reviewer_effort_escalation_reason, "critical_task"
                )

    def test_missing_semantic_risk_fails_high_without_recall(self):
        rr, _, efforts, calls = self._call([
            ("FAIL", "등급 불명확", [], True, "", ""),
        ])
        self.assertEqual(rr.risk, "HIGH")
        self.assertEqual(efforts, ["medium"])
        self.assertEqual(len(calls), 1)

    def test_build_fail_and_natural_language_high_each_call_once(self):
        build_rr, _, build_efforts, _ = self._call([
            ("FAIL", "구조 재작성 필요", [], True, "", "HIGH"),
        ], task=self._task(build_status="FAIL"))
        self.assertEqual(build_rr.risk, "HIGH")
        self.assertEqual(build_efforts, ["medium"])

        hard_rr, _, hard_efforts, _ = self._call([
            (
                "FAIL",
                "비밀번호가 코드에 하드코딩되어 노출됨",
                [],
                True,
                "",
                "HIGH",
            ),
        ])
        self.assertEqual(hard_rr.risk, "HIGH")
        self.assertEqual(hard_efforts, ["medium"])

    def test_unavailable_and_parse_error_remain_fail_closed_without_recall(self):
        cases = (
            (
                (None, None, ["timeout"], False, "unavailable", ""),
                REVIEW_UNAVAILABLE,
            ),
            (
                (None, None, ["parse"], False, "error", ""),
                REVIEW_ERROR,
            ),
        )
        for high_result, expected_status in cases:
            with self.subTest(status=expected_status):
                rr, task, efforts, _ = self._call([
                    high_result,
                ])
                self.assertFalse(rr.passed)
                self.assertEqual(rr.status, expected_status)
                self.assertEqual(efforts, ["medium"])
                self.assertEqual(
                    task.reviewer_effective_reasoning_effort, "medium"
                )



class LocalDevConfigCollectorTest(unittest.TestCase):
    """GitCollector: 로컬 개발 설정(quasar.config.cjs) 게이트/리뷰 입력 제외 (실제 git)."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.repo = Path(self._tmp.name)

        def run_git(*args):
            subprocess.run(
                ["git", *args], cwd=str(self.repo),
                capture_output=True, text=True, check=True,
            )

        run_git("init")
        run_git("config", "user.email", "test@test")
        run_git("config", "user.name", "test")
        (self.repo / "quasar.config.cjs").write_text(
            "server: { port: 8080 }", encoding="utf-8"
        )
        (self.repo / "src").mkdir()
        (self.repo / "src" / "A.vue").write_text("<template/>", encoding="utf-8")
        run_git("add", "-A")
        run_git("commit", "-m", "init")

    def tearDown(self):
        self._tmp.cleanup()

    def _collect(self):
        from git_collector import GitCollector
        task = TaskState(
            task_id="TASK-EXC", requirement="x", working_dir=str(self.repo)
        )
        return GitCollector(str(self.repo), profile=E4U_PROFILE).collect(task=task), task

    def test_quasar_only_excluded_everywhere(self):
        (self.repo / "quasar.config.cjs").write_text(
            "server: { port: 9000 }", encoding="utf-8"
        )
        res, task = self._collect()
        self.assertTrue(res.success)
        self.assertEqual(res.changed_files, [])
        self.assertEqual(task.changed_files, [])
        self.assertEqual(res.diff, "")
        self.assertEqual(res.diff_stat, "")
        self.assertEqual(res.excluded_files, ["quasar.config.cjs"])
        self.assertEqual(task.excluded_files, ["quasar.config.cjs"])

    def test_mixed_keeps_work_files_only(self):
        (self.repo / "quasar.config.cjs").write_text(
            "server: { port: 9000 }", encoding="utf-8"
        )
        (self.repo / "src" / "A.vue").write_text(
            "<template>changed</template>", encoding="utf-8"
        )
        res, task = self._collect()
        self.assertTrue(res.success)
        self.assertEqual(res.changed_files, ["src/A.vue"])
        self.assertIn("src/A.vue", res.diff)
        self.assertNotIn("quasar.config.cjs", res.diff)
        self.assertNotIn("quasar.config.cjs", res.diff_stat)
        self.assertEqual(res.excluded_files, ["quasar.config.cjs"])

    def test_sucui_quasar_config_js_excluded(self):
        """sucui quasar.config.js 도 capui .cjs 와 동일하게 제외 (파일명 매칭)."""
        (self.repo / "quasar.config.js").write_text(
            "server: { port: 9000 }", encoding="utf-8"
        )
        (self.repo / "src" / "A.vue").write_text(
            "<template>changed</template>", encoding="utf-8"
        )
        res, task = self._collect()
        self.assertTrue(res.success)
        self.assertEqual(res.changed_files, ["src/A.vue"])
        self.assertNotIn("quasar.config.js", res.diff)
        self.assertEqual(res.excluded_files, ["quasar.config.js"])

    def test_env_local_variants_excluded_but_production_kept(self):
        from task_state import is_local_dev_config

        self.assertTrue(is_local_dev_config(".env", E4U_PROFILE))
        self.assertTrue(is_local_dev_config("capui/.env.development", E4U_PROFILE))
        self.assertTrue(is_local_dev_config("capui/.env.localhost", E4U_PROFILE))
        # 운영 비밀 가능 파일은 제외하지 않음 (리뷰 대상 유지)
        self.assertFalse(is_local_dev_config("capui/.env.production", E4U_PROFILE))


class RollbackExcludeTest(unittest.TestCase):
    """ROLLBACK_EXCLUDE_PATHS(quasar.config.cjs) 롤백 제외 동작 (실제 git 사용)."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.repo = Path(self._tmp.name)

        def run_git(*args):
            subprocess.run(
                ["git", *args], cwd=str(self.repo),
                capture_output=True, text=True, check=True,
            )

        run_git("init")
        run_git("config", "user.email", "test@test")
        run_git("config", "user.name", "test")
        (self.repo / "quasar.config.cjs").write_text(
            "server: { port: 8080 }", encoding="utf-8"
        )
        (self.repo / "src_A.java").write_text("base", encoding="utf-8")
        run_git("add", "-A")
        run_git("commit", "-m", "init")

    def tearDown(self):
        self._tmp.cleanup()

    def test_quasar_config_excluded_from_rollback(self):
        from git_collector import GitCollector

        collector = GitCollector(str(self.repo), profile=E4U_PROFILE)
        task = TaskState(
            task_id="TASK-RB", requirement="x", working_dir=str(self.repo)
        )
        baseline, error = collector.capture_baseline(task)
        self.assertIsNotNone(baseline, error)

        # 두 파일 모두 수정 (quasar 는 로컬 서버 세팅용, src_A 는 AI 변경분 가정)
        (self.repo / "quasar.config.cjs").write_text(
            "server: { port: 9000 }", encoding="utf-8"
        )
        (self.repo / "src_A.java").write_text("modified", encoding="utf-8")

        ok, msg = collector.rollback(task=task, baseline=baseline)

        self.assertTrue(ok, msg)
        # 일반 파일은 HEAD 기준 원복
        self.assertEqual(
            (self.repo / "src_A.java").read_text(encoding="utf-8"), "base"
        )
        # 로컬 설정 파일은 보존
        self.assertEqual(
            (self.repo / "quasar.config.cjs").read_text(encoding="utf-8"),
            "server: { port: 9000 }",
        )
        # 안내 문구에 제외 파일 표시
        self.assertIn("quasar.config.cjs", msg)


class TaskScopedRollbackTest(unittest.TestCase):
    """Job 시작 전 변경은 보존하고 해당 Job delta만 되돌린다."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.repo = Path(self._tmp.name)
        self._git("init")
        self._git("config", "user.email", "test@test")
        self._git("config", "user.name", "test")
        (self.repo / "A.txt").write_text("HEAD-A", encoding="utf-8")
        (self.repo / "B.txt").write_text("HEAD-B", encoding="utf-8")
        self._git("add", "-A")
        self._git("commit", "-m", "init")

    def tearDown(self):
        self._tmp.cleanup()

    def _git(self, *args):
        return subprocess.run(
            ["git", *args], cwd=str(self.repo), capture_output=True,
            text=True, encoding="utf-8", check=True,
        )

    def _baseline(self):
        from git_collector import GitCollector

        collector = GitCollector(str(self.repo))
        task = TaskState(task_id="TASK-B", requirement="x", working_dir=str(self.repo))
        baseline, error = collector.capture_baseline(task)
        self.assertIsNotNone(baseline, error)
        return collector, task, baseline

    def test_preserves_previous_job_and_reverts_current_job(self):
        (self.repo / "A.txt").write_text("A-SUCCESS", encoding="utf-8")
        collector, task, baseline = self._baseline()
        (self.repo / "B.txt").write_text("B-FAILED", encoding="utf-8")

        result = collector.collect(task, baseline)
        self.assertEqual(result.changed_files, ["B.txt"])
        ok, message = collector.rollback(task, baseline)
        self.assertTrue(ok, message)
        self.assertEqual((self.repo / "A.txt").read_text(encoding="utf-8"), "A-SUCCESS")
        self.assertEqual((self.repo / "B.txt").read_text(encoding="utf-8"), "HEAD-B")

    def test_same_file_returns_to_previous_job_version(self):
        (self.repo / "A.txt").write_text("A-SUCCESS", encoding="utf-8")
        collector, task, baseline = self._baseline()
        (self.repo / "A.txt").write_text("B-FAILED", encoding="utf-8")

        ok, message = collector.rollback(task, baseline)
        self.assertTrue(ok, message)
        self.assertEqual((self.repo / "A.txt").read_text(encoding="utf-8"), "A-SUCCESS")

    def test_preserves_old_untracked_and_removes_job_created_file(self):
        (self.repo / "A-new.txt").write_text("A-SUCCESS", encoding="utf-8")
        collector, task, baseline = self._baseline()
        (self.repo / "B-new.txt").write_text("B-FAILED", encoding="utf-8")

        ok, message = collector.rollback(task, baseline)
        self.assertTrue(ok, message)
        self.assertTrue((self.repo / "A-new.txt").exists())
        self.assertFalse((self.repo / "B-new.txt").exists())

    def test_preserves_baseline_staged_state(self):
        (self.repo / "A.txt").write_text("A-STAGED", encoding="utf-8")
        self._git("add", "A.txt")
        collector, task, baseline = self._baseline()
        (self.repo / "A.txt").write_text("B-FAILED", encoding="utf-8")

        ok, message = collector.rollback(task, baseline)
        self.assertTrue(ok, message)
        self.assertEqual((self.repo / "A.txt").read_text(encoding="utf-8"), "A-STAGED")
        staged = self._git("diff", "--cached", "--name-only").stdout.splitlines()
        self.assertEqual(staged, ["A.txt"])


class DroidAutoLevelTest(unittest.TestCase):
    """droid --auto 레벨 정책: 기본 모두 medium + KKM_DROID_AUTO_* env 오버라이드."""

    _ENV_KEYS = ("KKM_DROID_AUTO_ANALYSIS", "KKM_DROID_AUTO_MODIFICATION")

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.work_dir = self._tmp.name
        self._env = {k: os.environ.get(k) for k in self._ENV_KEYS}
        for k in self._ENV_KEYS:
            os.environ.pop(k, None)

    def tearDown(self):
        self._tmp.cleanup()
        for k, v in self._env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_helper_levels_default_medium(self):
        from worker import droid_auto_level

        # 기본: 모든 모드 medium
        self.assertEqual(droid_auto_level("MODIFICATION"), "medium")
        self.assertEqual(droid_auto_level("ANALYSIS"), "medium")
        # task_mode 미지정 → requirement 키워드 추정 결과 기준 분기(둘 다 medium)
        self.assertEqual(droid_auto_level("", "버그 수정해"), "medium")
        self.assertEqual(droid_auto_level("", "구조 분석해줘"), "medium")
        # 명시 mode 가 키워드보다 우선(결정되는 env 키가 달라짐)
        self.assertEqual(droid_auto_level("ANALYSIS", "버그 수정해"), "medium")

    def test_env_override(self):
        from worker import droid_auto_level

        os.environ["KKM_DROID_AUTO_ANALYSIS"] = "low"
        os.environ["KKM_DROID_AUTO_MODIFICATION"] = "high"
        self.assertEqual(droid_auto_level("ANALYSIS"), "low")
        self.assertEqual(droid_auto_level("", "구조 분석해줘"), "low")
        self.assertEqual(droid_auto_level("MODIFICATION"), "high")
        self.assertEqual(droid_auto_level("", "버그 수정해"), "high")

    def test_env_invalid_value_falls_back_to_medium(self):
        from worker import droid_auto_level

        os.environ["KKM_DROID_AUTO_ANALYSIS"] = "ultra"
        self.assertEqual(droid_auto_level("ANALYSIS"), "medium")
        del os.environ["KKM_DROID_AUTO_ANALYSIS"]
        os.environ["KKM_DROID_AUTO_MODIFICATION"] = "HIGH!!"
        self.assertEqual(droid_auto_level("MODIFICATION"), "medium")

    def test_default_inactivity_900_and_retry_3(self):
        """운영 조건 기본값: 무출력 900초 + 최대 재시도 3회."""
        import inspect

        import worker as worker_mod
        from manager import Manager

        cfg = worker_mod.TimeoutConfig.from_env()
        self.assertEqual(cfg.inactivity, 900)
        sig = inspect.signature(Manager.__init__)
        self.assertEqual(sig.parameters["max_retry"].default, 3)

    def _capture_droid_cmd(self, task_mode: str, requirement: str) -> list[str]:
        """DroidWorker.execute 가 생성한 cmd 를 캡처 (실제 프로세스 실행 없음)."""
        import worker as worker_mod

        captured: dict = {}

        def fake_activity_run(cmd, stdin_data=None, cwd=None, cfg=None, requirement=None, task_mode="", display_events=False, env=None):
            captured["cmd"] = list(cmd)
            return 0, "ok", "", None

        w = worker_mod.DroidWorker(
            timeout=5, model="test-model",
            timeout_config=worker_mod.TimeoutConfig(),
        )
        task = TaskState(
            task_id="TASK-AUTO", requirement=requirement,
            working_dir=self.work_dir, task_mode=task_mode,
        )
        with mock.patch.object(
            worker_mod, "_run_with_activity_timeout",
            side_effect=fake_activity_run,
        ):
            wr = w.execute(requirement, self.work_dir, task=task)
        self.assertTrue(wr.success)
        self.assertIn("cmd", captured)
        return captured["cmd"]

    @staticmethod
    def _auto_value(cmd: list[str]) -> str:
        i = cmd.index("--auto")
        return cmd[i + 1]

    def test_modification_uses_auto_medium(self):
        cmd = self._capture_droid_cmd("MODIFICATION", "페이지 수정해")
        self.assertEqual(self._auto_value(cmd), "medium")

    def test_analysis_uses_auto_medium(self):
        cmd = self._capture_droid_cmd("ANALYSIS", "구조 분석해줘")
        self.assertEqual(self._auto_value(cmd), "medium")

    def test_empty_mode_resolves_from_requirement(self):
        cmd = self._capture_droid_cmd("", "버그 수정해")
        self.assertEqual(self._auto_value(cmd), "medium")

    def test_worker_cmd_env_override(self):
        os.environ["KKM_DROID_AUTO_ANALYSIS"] = "low"
        cmd = self._capture_droid_cmd("ANALYSIS", "구조 분석해줘")
        self.assertEqual(self._auto_value(cmd), "low")


class DroidAutoToolsLevelTest(unittest.TestCase):
    """planner/tester/reviewer droid 폴백의 --auto 레벨 (KKM_DROID_AUTO_ANALYSIS 공용)."""

    _ENV_KEYS = ("KKM_DROID_AUTO_ANALYSIS", "KKM_DROID_AUTO_MODIFICATION")

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.work_dir = self._tmp.name
        self._env = {k: os.environ.get(k) for k in self._ENV_KEYS}
        for k in self._ENV_KEYS:
            os.environ.pop(k, None)

    def tearDown(self):
        self._tmp.cleanup()
        for k, v in self._env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    @staticmethod
    def _auto_value(cmd: list[str]) -> str:
        i = cmd.index("--auto")
        return cmd[i + 1]

    @staticmethod
    def _fake_run_result(stdout: str):
        return type("R", (), {"returncode": 0, "stdout": stdout, "stderr": ""})()

    def test_planner_uses_shared_auto_level(self):
        import planner as planner_mod

        captured: dict = {}

        def fake_run(cmd, **kw):
            captured["cmd"] = list(cmd)
            captured["env"] = dict(kw["env"])
            return self._fake_run_result(
                "WORKER_BRIEF:\na\nTESTER_BRIEF:\nb\nREVIEWER_BRIEF:\nc\n"
            )

        task = TaskState(
            task_id="TASK-PL", requirement="x",
            working_dir=self.work_dir, task_mode="ANALYSIS",
            profile_dir=str(E4U_PROFILE.profile_dir),
        )
        with mock.patch.object(planner_mod.subprocess, "run", side_effect=fake_run):
            briefs = planner_mod.run_planner(task, self.work_dir)
        self.assertTrue(briefs)
        self.assertEqual(self._auto_value(captured["cmd"]), "medium")
        self.assertEqual(
            captured["env"]["AGENT_PROFILE_DIR"], str(E4U_PROFILE.profile_dir.resolve())
        )
        self.assertNotIn("KKM_TG_BOT_TOKEN", captured["env"])

        # env 오버라이드 → low
        os.environ["KKM_DROID_AUTO_ANALYSIS"] = "low"
        with mock.patch.object(planner_mod.subprocess, "run", side_effect=fake_run):
            planner_mod.run_planner(task, self.work_dir)
        self.assertEqual(self._auto_value(captured["cmd"]), "low")

    def test_tester_uses_shared_auto_level(self):
        import tester as tester_mod

        captured: dict = {}

        def fake_run(cmd, **kw):
            captured["cmd"] = list(cmd)
            captured["env"] = dict(kw["env"])
            return self._fake_run_result(
                "TEST_RESULT: PASS\nSUMMARY: ok\nSTEPS:\n- 페이지 확인\n"
            )

        task = TaskState(
            task_id="TASK-TS", requirement="페이지 수정해",
            working_dir=self.work_dir, task_mode="MODIFICATION",
            changed_files=["capui/src/A.vue"],
        )
        with mock.patch.object(tester_mod.subprocess, "run", side_effect=fake_run):
            tr = tester_mod.run_tester(task, self.work_dir, profile=E4U_PROFILE)
        self.assertEqual(tr.status, "PASS")
        self.assertEqual(self._auto_value(captured["cmd"]), "medium")
        self.assertEqual(
            captured["env"]["AGENT_PROFILE_DIR"], str(E4U_PROFILE.profile_dir.resolve())
        )
        self.assertNotIn("KKM_TG_BOT_TOKEN", captured["env"])

    def test_reviewer_codex_cmd_readonly_medium_model(self):
        """리뷰어: codex exec + gpt-5.6-sol + reasoning medium + read-only (20차)."""
        from reviewer import Reviewer

        captured: dict = {}

        def fake_run(cmd, input=None, capture_output=True, text=True,
                     encoding=None, errors=None, timeout=None, **kw):
            captured["cmd"] = list(cmd)
            captured["input"] = input or ""
            # -o 최종 메시지 파일에 RESULT 기록됨을 모사
            o_idx = captured["cmd"].index("-o")
            with open(captured["cmd"][o_idx + 1], "w", encoding="utf-8") as f:
                f.write("RESULT: PASS\nREASON: ok\nRISK: NONE")
            return subprocess.CompletedProcess(cmd, 0, "jsonl-events", "")

        rv = Reviewer(working_dir=self.work_dir, timeout=5)
        with mock.patch("reviewer.subprocess.run", side_effect=fake_run):
            verdict, reason, _, ran, fail_kind, _risk = rv._codex_review(
                "요구사항", ["src/A.vue"], "diff --git a/x b/x"
            )
        self.assertTrue(ran)
        self.assertEqual(verdict, "PASS")
        cmd = captured["cmd"]
        # subprocess가 codex exec 다 (droid exec 아님)
        self.assertEqual(cmd[0], "codex")
        self.assertEqual(cmd[1], "exec")
        self.assertNotIn("droid", cmd)
        # 기본 모델 gpt-5.6-sol
        self.assertEqual(cmd[cmd.index("-m") + 1], "gpt-5.6-sol")
        # reasoning effort medium
        self.assertIn('model_reasoning_effort="medium"', cmd)
        # 읽기 전용 샌드박스 (workspace-write 금지)
        self.assertEqual(cmd[cmd.index("-s") + 1], "read-only")
        self.assertNotIn("--approve-for-me", cmd)
        self.assertNotIn("workspace-write", cmd)
        # stdin 프롬프트 전달
        self.assertIn("리뷰어", captured["input"])

    def test_reviewer_uses_json_events_and_activity_timeout(self):
        """리뷰어도 워커와 동일: JSONL 이벤트 + 활동 타임아웃 + ANALYSIS 기준."""
        from reviewer import Reviewer
        from worker import TimeoutConfig

        captured: dict = {}

        def fake_activity_run(cmd, stdin_data=None, cwd=None, cfg=None,
                              requirement=None, task_mode="", display_events=False,
                              log_prefix="[Worker]", heartbeat=60.0, env=None):
            captured["cmd"] = list(cmd)
            captured["task_mode"] = task_mode
            captured["log_prefix"] = log_prefix
            captured["display_events"] = display_events
            captured["cfg"] = cfg
            # -o 최종 메시지 파일에 RESULT 기록됨을 모사
            o_idx = captured["cmd"].index("-o")
            with open(captured["cmd"][o_idx + 1], "w", encoding="utf-8") as f:
                f.write("RESULT: PASS\nREASON: ok\nRISK: NONE")
            # codex --json JSONL 출력 모사
            return 0, (
                '{"type":"thread.started","thread_id":"t1"}\n'
                '{"type":"item.completed","item":{"id":"i0","type":"agent_message",'
                '"text":"RESULT: PASS\\nREASON: ok"}}\n'
                '{"type":"turn.completed","usage":{}}\n'
            ), "", None

        rv = Reviewer(
            working_dir=self.work_dir, timeout=5,
            timeout_config=TimeoutConfig(),
        )
        with mock.patch(
            "reviewer._run_with_activity_timeout",
            side_effect=fake_activity_run,
        ):
            verdict, reason, _, ran, fail_kind, _risk = rv._codex_review(
                "요구사항", ["src/A.vue"], "diff --git a/x b/x"
            )
        self.assertTrue(ran)
        self.assertEqual(verdict, "PASS")
        cmd = captured["cmd"]
        # codex JSONL 이벤트 출력 (--json)
        self.assertIn("--json", cmd)
        self.assertEqual(cmd[0], "codex")
        # 읽기 전용 리뷰 → ANALYSIS 기준 타임아웃
        self.assertEqual(captured["task_mode"], "ANALYSIS")
        # 리뷰어 로그 프리픽스 + 이벤트 표시 켜짐
        self.assertEqual(captured["log_prefix"], "[Reviewer]")
        self.assertTrue(captured["display_events"])

    def test_reviewer_activity_timeout_reports_unavailable(self):
        """활동 타임아웃 발생 시 REVIEW_UNAVAILABLE 경로 (fail-closed 유지)."""
        from reviewer import Reviewer
        from worker import TimeoutConfig

        def fake_activity_run(cmd, stdin_data=None, cwd=None, cfg=None,
                              requirement=None, task_mode="", display_events=False,
                              log_prefix="[Worker]", heartbeat=60.0, env=None):
            return -1, "partial", "", "무출력 타임아웃 (900s)"

        rv = Reviewer(
            working_dir=self.work_dir, timeout=5,
            timeout_config=TimeoutConfig(),
        )
        with mock.patch(
            "reviewer._run_with_activity_timeout",
            side_effect=fake_activity_run,
        ):
            verdict, reason, details, ran, fail_kind, _risk = rv._codex_review(
                "요구사항", ["src/A.vue"], "diff --git a/x b/x"
            )
        self.assertFalse(ran)
        self.assertEqual(fail_kind, "execution_unavailable")
        self.assertTrue(any("timeout" in d for d in details))


class ModificationOnlyUxTest(unittest.TestCase):
    """run.py 수정 전용 진입점 (모드/플래너 질문 없음, MODIFICATION 고정)."""

    def test_run_entry_has_no_mode_or_planner_prompts(self):
        import run as run_mod

        self.assertFalse(hasattr(run_mod, "select_task_mode"))
        self.assertFalse(hasattr(run_mod, "select_planner"))

    def test_create_task_always_modification_planner_off(self):
        import run as run_mod

        # 요구사항에 분석 키워드가 있어도 수정 모드로 고정된다
        task = run_mod.create_task(
            requirement="구조 분석해줘",
            worker_type="droid",
            droid_model="custom:GLM-5.3-[Z.AI-Coding]-0",
            codex_model=None,
            target_module=["capui"],
            working_dir=r"D:\fake",
            task_id="TASK-UX",
        )
        self.assertEqual(task.task_mode, "MODIFICATION")
        self.assertFalse(task.use_planner)
        self.assertEqual(task.worker, "droid")
        self.assertEqual(task.target_module, ["capui"])
        # 기본 worker_no=0 (미지정)
        self.assertEqual(task.worker_no, 0)

    def test_create_task_does_not_widen_explicit_contract_from_requirement_paths(self):
        import run as run_mod

        task = run_mod.create_task(
            requirement=(
                "- sucui/src/pages/admin/se/SsucSe0020List.vue\n"
                "- suc\\src\\main\\java\\suc\\se\\query\\SsucSe0020Qry.xml"
            ),
            worker_type="droid", droid_model="m", codex_model=None,
            target_module=["suc"], working_dir=r"D:\fake", task_id="TASK-SCOPE",
        )
        self.assertEqual(task.target_module, ["suc"])
        self.assertEqual(task.scope_authority, "JOB_CONTRACT")
        self.assertFalse(task.scope_provenance["requirement_inference_used"])

    def test_reconcile_modules_ignores_requirement_path_outside_explicit_scope(self):
        import run as run_mod

        self.assertEqual(
            run_mod.reconcile_target_modules(
                "sucui/src/pages/A.vue 수정", ["suc"], available=["suc"]
            ),
            ["suc"],
        )

    def test_create_task_worker_no(self):
        import run as run_mod

        task = run_mod.create_task(
            requirement="버그 수정해",
            worker_type="droid",
            droid_model="m",
            codex_model=None,
            target_module=[],
            working_dir=r"D:\fake",
            task_id="TASK-W2",
            worker_no=2,
        )
        self.assertEqual(task.worker_no, 2)

    def test_modification_prompt_marks_requirement_as_approved_execution(self):
        """MODIFICATION은 승인 질문 없이 실행, ANALYSIS에는 실행 승인 문구 미주입."""
        from worker import build_prompt

        mod_prompt = build_prompt("버그 수정", ["sucui"], "MODIFICATION")
        self.assertIn("[실행 단계 및 승인 범위]", mod_prompt)
        self.assertIn("별도의 설계 승인 질문 없이 바로 구현", mod_prompt)
        self.assertIn("범위 확장", mod_prompt)

        analysis_prompt = build_prompt("구조 분석", ["sucui"], "ANALYSIS")
        self.assertNotIn("[실행 단계 및 승인 범위]", analysis_prompt)

    def test_modification_prompt_defines_already_satisfied_marker(self):
        from task_state import ALREADY_SATISFIED_MARKER
        from worker import build_prompt

        prompt = build_prompt("버그 수정", ["sucui"], "MODIFICATION")
        self.assertIn(ALREADY_SATISFIED_MARKER, prompt)
        self.assertIn("실제로 수정할 필요가 없", prompt)

    def test_completion_protocol_follows_requirement_and_uses_task_artifact_dir(self):
        from worker import build_prompt

        requirement = "diff가 비어 있으면 안 된다. VERIFY.md를 작성하라."
        prompt = build_prompt(
            requirement, ["suc", "sucui"], "MODIFICATION",
            task_id="TASK-20260820-151504",
        )
        self.assertGreater(prompt.index("[하네스 완료 판정 프로토콜]"), prompt.index(requirement))
        self.assertIn("git diff HEAD", prompt)
        # 21차: 산출물은 하네스 doc/<일자>/<시간>/VERIFY_<시간>.md (task id와 1:1)
        expected_artifact = str(
            Path("doc") / "20260820" / "151504" / "VERIFY_151504.md"
        )
        self.assertIn(expected_artifact, prompt)
        # 소스 트리에 검증 문서를 심지 말라는 지시 포함
        self.assertIn("소스 트리", prompt)
        self.assertNotIn(".tasks/TASK", prompt)

    def test_collect_task_artifacts_is_separate_from_changed_files(self):
        """21차: doc/<일자>/<시간>/ 산출물 수집 (실제 하네스 경로 오염 방지 mock)."""
        from manager import collect_task_artifacts, _artifact_dir_for

        with tempfile.TemporaryDirectory() as td:
            fake_dir = Path(td) / "doc" / "20260820" / "151504"
            with mock.patch("manager._artifact_dir_for", return_value=fake_dir):
                fake_dir.mkdir(parents=True)
                (fake_dir / "VERIFY_151504.md").write_text(
                    "verification", encoding="utf-8"
                )
                task = TaskState(
                    task_id="TASK-20260820-151504", requirement="x",
                    working_dir=td, changed_files=["suc/src/A.java"],
                )
                artifacts = collect_task_artifacts(task)
            self.assertEqual(
                artifacts, [(fake_dir / "VERIFY_151504.md").as_posix()]
            )
            self.assertEqual(task.artifact_files, artifacts)
            self.assertEqual(task.changed_files, ["suc/src/A.java"])
            # 소스 changed_files 와 산출물이 분리되어 있다
            self.assertNotIn("suc/src/A.java", artifacts)

    def test_artifact_paths_layout(self):
        """21차 산출물 레이아웃: doc/YYYYMMDD/HHmmss/VERIFY_HHmmss.md (절대경로)."""
        from worker import artifact_paths, HARNESS_ROOT

        d, v = artifact_paths("TASK-20260820-151504")
        self.assertTrue(Path(d).is_absolute())
        self.assertEqual(
            d, str(HARNESS_ROOT / "doc" / "20260820" / "151504"),
        )
        self.assertEqual(
            v, str(HARNESS_ROOT / "doc" / "20260820" / "151504" / "VERIFY_151504.md"),
        )
        # 형식 밖 task_id → 폴백(오늘 일자/artifacts)
        d2, v2 = artifact_paths("WEIRD-ID")
        self.assertIn("artifacts", d2)
        self.assertTrue(v2.endswith("VERIFY.md"))

    def test_task_state_saves_into_date_dir(self):
        """.tasks/YYYYMMDD/{task_id}.json 일자 폴더 저장 + 로드(구버전 루트 폴백)."""
        with tempfile.TemporaryDirectory() as td:
            task = TaskState(
                task_id="TASK-20260820-151504", requirement="x",
                working_dir=".",
            )
            p = task.save(base_dir=td)
            self.assertEqual(
                p, Path(td) / "20260820" / "TASK-20260820-151504.json",
            )
            # 일자 폴더에서 로드
            loaded = TaskState.load("TASK-20260820-151504", base_dir=td)
            self.assertEqual(loaded.task_id, task.task_id)
            # 구버전: 루트에 있는 json 도 폴백 로드 (필수 필드 포함 최소 형식)
            legacy = Path(td) / "TASK-20200101-000001.json"
            legacy.write_text(
                '{"task_id": "TASK-20200101-000001", "requirement": "legacy", '
                '"working_dir": "."}',
                encoding="utf-8",
            )
            legacy_task = TaskState.load("TASK-20200101-000001", base_dir=td)
            self.assertEqual(legacy_task.task_id, "TASK-20200101-000001")

    def test_detect_worker_approval_wait_response(self):
        """실제 무변경 종료 문구는 승인 대기로 감지하고 일반 완료 보고는 제외."""
        from manager import is_worker_approval_wait_response

        self.assertTrue(is_worker_approval_wait_response("이 설계대로 구현해도 될까요?"))
        self.assertTrue(is_worker_approval_wait_response("Would you like me to proceed with implementation?"))
        self.assertFalse(is_worker_approval_wait_response("요구사항대로 구현과 검증을 완료했습니다."))

    def test_codex_model_and_reasoning_selection(self):
        import run as run_mod

        with mock.patch("builtins.input", side_effect=["2", "3"]):
            model, effort = run_mod.select_codex_options()
        self.assertEqual(model, "gpt-5.6-terra")
        self.assertEqual(effort, "high")

        task = run_mod.create_task(
            requirement="수정", worker_type="codex", droid_model="",
            codex_model=model, target_module=["capui"], working_dir=".",
            task_id="TASK-CODEX", codex_reasoning_effort=effort,
        )
        self.assertEqual(task.codex_model, "gpt-5.6-terra")
        self.assertEqual(task.codex_reasoning_effort, "high")

    def test_single_instance_lock_rejects_second_holder(self):
        import run as run_mod

        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "run.lock"
            first = run_mod.SingleInstanceLock(path)
            second = run_mod.SingleInstanceLock(path)
            self.assertTrue(first.acquire())
            self.assertFalse(second.acquire())
            first.release()
            self.assertTrue(second.acquire())
            second.release()

    def test_codex_worker_command_uses_selected_options(self):
        from worker import CodexWorker

        completed = subprocess.CompletedProcess([], 0, stdout="ok", stderr="")
        worker = CodexWorker(model="gpt-5.6-luna", reasoning_effort="low")
        task = TaskState(
            task_id="TASK-CODEX-CMD", requirement="수정", worker="codex",
            codex_model="gpt-5.6-luna", codex_reasoning_effort="low",
        )
        with tempfile.TemporaryDirectory() as td, \
             mock.patch("worker.build_prompt", return_value="prompt"), \
             mock.patch("worker.codex_host_pair_matches", return_value=True), \
             mock.patch("worker.subprocess.run", return_value=completed) as run_mock:
            result = worker.execute("수정", td, task=task)
        self.assertTrue(result.success)
        cmd = run_mock.call_args.args[0]
        self.assertIn("gpt-5.6-luna", cmd)
        self.assertIn('model_reasoning_effort="low"', cmd)
        self.assertIn("--approve-for-me", cmd)
        self.assertIn("--json", cmd)
        self.assertNotIn("-s", cmd)
        self.assertNotIn("--sandbox", cmd)
        self.assertNotIn("workspace-write", cmd)

    def test_codex_worker_uses_json_events_and_activity_timeout(self):
        from worker import CodexWorker, TimeoutConfig

        captured: dict = {}

        def fake_activity_run(cmd, stdin_data=None, cwd=None, cfg=None,
                              requirement=None, task_mode="", display_events=False,
                              log_prefix="[Worker]", heartbeat=60.0, env=None):
            captured.update(
                cmd=list(cmd), display_events=display_events,
                log_prefix=log_prefix, stdin_data=stdin_data,
            )
            return 0, (
                '{"type":"thread.started","thread_id":"t1"}\n'
                '{"type":"item.started","item":{"type":"command_execution",'
                '"command":"rg -n TODO ."}}\n'
                '{"type":"item.completed","item":{"type":"agent_message",'
                '"text":"작업 완료"}}\n'
                '{"type":"turn.completed","usage":{}}\n'
            ), "", None

        worker = CodexWorker(
            model="gpt-5.6-sol", reasoning_effort="medium",
            timeout_config=TimeoutConfig(),
        )
        task = TaskState(
            task_id="TASK-CODEX-EVENT", requirement="수정", worker="codex",
            worker_no=2, task_mode="MODIFICATION",
        )
        with tempfile.TemporaryDirectory() as td, \
             mock.patch("worker.build_prompt", return_value="prompt"), \
             mock.patch("worker.codex_host_pair_matches", return_value=True), \
             mock.patch("worker._run_with_activity_timeout", side_effect=fake_activity_run):
            result = worker.execute("수정", td, task=task)

        self.assertTrue(result.success)
        self.assertEqual(result.stdout, "작업 완료")
        self.assertIn("--json", captured["cmd"])
        self.assertTrue(captured["display_events"])
        self.assertEqual(captured["log_prefix"], "[Worker2]")
        self.assertEqual(captured["stdin_data"], "prompt")

    def test_alloc_worker_no_reuses_free_slot(self):
        """병렬 슬롯: 1 점유 중이면 2 반환, 1 완료 시 1 회수."""
        import run as run_mod
        from concurrent.futures import Future

        t1 = run_mod.create_task(
            requirement="a", worker_type="droid", droid_model="m",
            codex_model=None, target_module=[], working_dir=".",
            task_id="TASK-A", worker_no=1,
        )
        f1 = Future(); f1.set_running_or_notify_cancel()   # 실행 중
        futures = [(f1, t1, "m")]
        self.assertEqual(run_mod._alloc_worker_no(futures, 3), 2)

        f1.set_result(True)                                # 완료 → 슬롯 회수
        self.assertEqual(run_mod._alloc_worker_no(futures, 3), 1)

    def test_running_line_shows_worker_tag(self):
        import run as run_mod
        from concurrent.futures import Future

        t1 = run_mod.create_task(
            requirement="a", worker_type="droid", droid_model="m",
            codex_model=None, target_module=[], working_dir=".",
            task_id="TASK-20260818-11111111", worker_no=1,
        )
        f1 = Future(); f1.set_running_or_notify_cancel()
        line = run_mod._running_line([(f1, t1, "m")])
        self.assertIn("Worker1", line)
        self.assertIn("11111111", line)


class AlwaysSkillsInjectionTest(unittest.TestCase):
    """역할별 공통/profile 규칙과 always skill 주입."""

    def test_always_skills_ponytail_and_project_context(self):
        """ponytail은 자동 주입하지 않고 project-context만 유지."""
        from worker import ALWAYS_SKILLS, select_context

        self.assertNotIn("skills/ponytail/SKILL.md", ALWAYS_SKILLS)
        self.assertIn("skills/project-context/SKILL.md", ALWAYS_SKILLS)
        # 어떤 요구사항이든 (영역 미감지) 항상 포함된다
        hooks, skills = select_context("간단한 오타 수정", [], profile=E4U_PROFILE)
        self.assertNotIn("skills/ponytail/SKILL.md", skills)
        self.assertIn("skills/project-context/SKILL.md", skills)

    def test_build_prompt_contains_skill_sections(self):
        """실제 프롬프트에 AGENTS.md 규칙과 기본 스킬이 주입된다 (파일 존재 전제).
        AGENTS_DIR 이 없는 환경에서는 규칙 섹션이 스킵되더라도 스킬은 최대한 주입된다."""
        from worker import AGENTS_DIR, build_prompt

        prompt = build_prompt(
            "버그 수정해", ["capui"], profile=E4U_PROFILE, role="droid"
        )
        # AGENTS_DIR/AGENTS.md 가 실제 존재하는 환경(D:\agents)에서 검증
        if (AGENTS_DIR / "AGENTS.md").exists():
            self.assertIn("[공통 프로젝트 규칙]", prompt)
            project_ctx_path = AGENTS_DIR / "skills" / "project-context" / "SKILL.md"
            self.assertNotIn("Ponytail", prompt)
            if project_ctx_path.exists():
                self.assertIn("PROJECT_ANALYSIS.md", prompt)
            self.assertIn("[선택된 프로필 규칙: e4u]", prompt)


class WorkerHeartbeatTest(unittest.TestCase):
    """무활동 감시 중 heartbeat 생존 로그 검증 (hang vs 작업 중 구분)."""

    def test_extract_droid_text_stream_json(self):
        """stream-json stdout → 최종 텍스트 추출 (downstream 호환)."""
        import worker as worker_mod

        s = (
            '{"type":"system","subtype":"init"}\n'
            '{"type":"tool_call","toolName":"Read"}\n'
            '{"type":"message","role":"assistant","text":"중간 응답"}\n'
            '{"type":"completion","finalText":"최종 답변입니다","numTurns":3}'
        )
        self.assertEqual(
            worker_mod._extract_droid_text(s), "최종 답변입니다"
        )
        # completion 없으면 마지막 assistant message
        s2 = (
            '{"type":"message","role":"user","text":"q"}\n'
            '{"type":"message","role":"assistant","text":"a1"}'
        )
        self.assertEqual(worker_mod._extract_droid_text(s2), "a1")
        # 구형 text 포맷 하위 호환
        self.assertEqual(
            worker_mod._extract_droid_text("그냥 텍스트 응답"), "그냥 텍스트 응답"
        )
        self.assertEqual(worker_mod._extract_droid_text(""), "")

    def test_extract_falls_back_to_scrubbed_raw_tail(self):
        """스키마 변경 등 추출 실패 → raw 끝부분(마스킹) 폴백. 빈 stdout 방지."""
        import worker as worker_mod

        # finalText/assistant 모두 없는 이벤트만 존재 (스키마 파손 시나리오)
        s = (
            '{"type":"system","subtype":"init"}\n'
            '{"type":"tool_call","toolName":"Edit","parameters":{"password=abc123"}}\n'
            '{"type":"unknown_new_event"}'
        )
        out = worker_mod._extract_droid_text(s)
        self.assertIn("[응답 파싱 실패 — raw 마지막 부분]", out)
        self.assertIn("unknown_new_event", out)   # 단서 보존
        self.assertNotIn("abc123", out)           # 비밀 마스킹
        # 빈 입력은 여전히 빈 문자열
        self.assertEqual(worker_mod._extract_droid_text(""), "")

    def test_scrub_secrets_masks_common_patterns(self):
        """콘솔/로그/worker_stdout 비밀 마스킹."""
        import worker as worker_mod

        # JWT: 세그먼트별 최소 길이(8자+) 충족하는 현실적 토큰
        jwt = "Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV"
        cases = [
            ("password=************ password: ************",
             "password=**** password: ****"),
            ("api_key = abc123def api-key: xyz789",
             "api_key = **** api-key: ****"),
            ("redis://user:p@ssw0rd@host:6379/0",
             "redis://user:****@host:6379/0"),
            (f"Authorization: {jwt}",
             "Authorization: ****"),
        ]
        for raw, expected in cases:
            self.assertEqual(worker_mod._scrub_secrets(raw), expected, raw)

    def test_fmt_droid_event_minimal_display(self):
        """콘솔 표시용 라인: 도구 이름만, 파라미터/추론 본문 미표시."""
        import worker as worker_mod

        # tool_call: 이름만 (파라미터의 파일 내용/쿼리 미노출)
        line = ('{"type":"tool_call","toolName":"Read",'
                '"parameters":{"file_path":"/x/y.vue","query":"SELECT * FROM S"}}')
        shown = worker_mod._fmt_droid_event(line)
        self.assertIn("Read", shown)
        self.assertNotIn("SELECT", shown)
        self.assertNotIn("y.vue", shown)
        # reasoning: 미표시 (None)
        self.assertIsNone(worker_mod._fmt_droid_event(
            '{"type":"reasoning","text":"내부 추론 내용"}'
        ))
        # tool_result 정상: 미표시 / 실패: 표시
        self.assertIsNone(worker_mod._fmt_droid_event(
            '{"type":"tool_result","isError":false}'
        ))
        self.assertIn("도구 실패", worker_mod._fmt_droid_event(
            '{"type":"tool_result","isError":true}'
        ) or "")
        # 파손된 JSON 라인: 미표시
        self.assertIsNone(worker_mod._fmt_droid_event("not-json"))
        # assistant: 첫 줄 100자 미리보기
        shown2 = worker_mod._fmt_droid_event(
            '{"type":"message","role":"assistant","text":"요약 첫 줄입니다\\n둘째 줄"}'
        )
        self.assertIn("요약 첫 줄입니다", shown2)
        self.assertNotIn("둘째 줄", shown2)
        # codex command_execution: 명령 인자/경로 없이 작업 종류만 표시
        search = worker_mod._fmt_droid_event(
            '{"type":"item.started","item":{"type":"command_execution",'
            '"command":"rg -n password D:/secret.txt"}}'
        )
        read = worker_mod._fmt_droid_event(
            '{"type":"item.started","item":{"type":"command_execution",'
            '"command":"Get-Content D:/secret.txt"}}'
        )
        self.assertIn("검색", search or "")
        self.assertNotIn("password", search or "")
        self.assertNotIn("secret.txt", search or "")
        self.assertIn("파일 읽기", read or "")
        self.assertNotIn("secret.txt", read or "")

    def test_droid_cmd_uses_stream_json(self):
        """무활동 오판 방지: droid cmd 에 stream-json 출력 포맷 포함."""
        cmd = self._capture_cmd()
        self.assertIn("-o", cmd)
        self.assertEqual(cmd[cmd.index("-o") + 1], "stream-json")

    @staticmethod
    def _capture_cmd() -> list[str]:
        import harness_temp as tf

        import worker as worker_mod

        captured: dict = {}

        def fake_activity_run(cmd, stdin_data=None, cwd=None, cfg=None,
                              requirement=None, task_mode="", display_events=False,
                              env=None):
            captured["cmd"] = list(cmd)
            return 0, "ok", "", None

        w = worker_mod.DroidWorker(
            timeout=5, model="test-model",
            timeout_config=worker_mod.TimeoutConfig(),
        )
        with tf.TemporaryDirectory() as td:
            task = TaskState(
                task_id="TASK-SJ", requirement="버그 수정해",
                working_dir=td, task_mode="MODIFICATION",
            )
            with mock.patch.object(
                worker_mod, "_run_with_activity_timeout",
                side_effect=fake_activity_run,
            ):
                wr = w.execute("버그 수정해", td, task=task)
        assert wr.success
        return captured["cmd"]

    def test_heartbeat_prints_while_alive(self):
        import io
        from contextlib import redirect_stdout

        from worker import TimeoutConfig, _run_with_activity_timeout

        class FakeProc:
            """절대 종료되지 않는 가짜 프로세스 (타임아웃 경로 강제)."""

            def __init__(self):
                self.returncode = None
                self.stdout = iter([])
                self.stderr = iter([])
                self.stdin = None

            def poll(self):
                return None

            def kill(self):
                self.returncode = -9

            def wait(self, timeout=None):
                return self.returncode

        buf = io.StringIO()
        with mock.patch(
            "subprocess.Popen", return_value=FakeProc()
        ), redirect_stdout(buf):
            _, _, _, timeout_reason = _run_with_activity_timeout(
                cmd=["fake"],
                stdin_data=None,
                cwd=".",
                cfg=TimeoutConfig(inactivity=1),
                requirement="버그 수정해",
                task_mode="MODIFICATION",
                heartbeat=0.05,
            )
        out = buf.getvalue()
        self.assertIn("무출력 타임아웃", timeout_reason or "")
        self.assertIn("실행 중", out)          # heartbeat 생존 로그
        self.assertIn("마지막 출력", out)
        self.assertIn("무활동 기준", out)


class RiskBasedRetryTest(unittest.TestCase):
    """REVIEW_FAIL 등급(LOW/MID/HIGH) 기반 재시도 전략 — run_with_retry 목 검증."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self._old_cwd = os.getcwd()
        os.chdir(self._tmp.name)
        self.working_dir = self._tmp.name
        self.manager = Manager(
            working_dir=self.working_dir, model="", max_retry=2,
            timeout=5, worker_type="droid", max_workers=1,
        )
        self.rollback_calls: list[TaskState] = []

    def tearDown(self):
        self.manager.shutdown()
        os.chdir(self._old_cwd)
        self._tmp.cleanup()

    # ── 공용 목 구성 ──────────────────────────────────────────
    def _mock_pipeline(self, review: ReviewResult, build_status: str = "PASS"):
        """_run_pipeline 을 목 처리: 리뷰/빌드 결과만 주입하고 state 동기화 모사."""
        def fake_pipeline(task):
            task.review_status = review.status
            task.review_result = review.reason
            task.review_risk = (
                review.risk if review.status == "REVIEW_FAIL" else ""
            )
            task.review_violations = list(review.details)
            task.build = {"status": build_status, "output": ""}
            task.failure_stage = "" if review.status == REVIEW_PASS else "REVIEW"
            task.failure_reason = review.reason
            bt = BuildTestResult(
                success=(build_status == "PASS"),
                build_status=build_status,
                test_status="SKIPPED",
            )
            git = GitDiffResult(
                success=True, changed_files=["src/A.java"],
                diff="diff --git a/src/A.java b/src/A.java",
            )
            from manager import ManagerResult
            success = review.status == REVIEW_PASS and build_status == "PASS"
            return ManagerResult(
                success=success,
                git_result=git,
                build_test_result=bt,
                review_result=review,
                message="PASS" if success else "FAIL",
            )

        self.manager._run_pipeline = fake_pipeline

        def fake_rollback(task=None, baseline=None):
            self.rollback_calls.append(task)
            if task is not None:
                task.is_rolled_back = True
            return True, "fake rollback"

        self.manager.collector.rollback = fake_rollback

    def _task(self) -> TaskState:
        return TaskState(
            task_id="TASK-RISK",
            requirement="버그 수정해",
            working_dir=self.working_dir,
            task_mode="MODIFICATION",
            changed_files=["src/A.java"],
        )

    def _run_retry(self, task: TaskState):
        buf = io.StringIO()
        with redirect_stdout(buf):
            result = self.manager.run_with_retry(task)
        return result, buf.getvalue()

    def _handoff_text(self, task: TaskState) -> str:
        import handoff as handoff_mod

        return handoff_mod.build_retry_handoff(task)

    def test_worker_approval_wait_stops_without_repeating_same_attempt(self):
        """비대화형 Worker의 승인 대기는 재시도로 해결되지 않으므로 한 번에 종료."""
        from manager import ManagerResult, WORKER_APPROVAL_WAIT_REASON

        calls = 0

        def fake_pipeline(task):
            nonlocal calls
            calls += 1
            task.failure_stage = "WORKER"
            task.failure_reason = WORKER_APPROVAL_WAIT_REASON
            return ManagerResult(success=False, message="FAIL")

        self.manager._run_pipeline = fake_pipeline
        task = self._task()
        result, out = self._run_retry(task)

        self.assertFalse(result.success)
        self.assertEqual(calls, 1)
        self.assertEqual(task.status, "FAILED")
        self.assertIn("승인 대기 응답", out)
        self.assertIn("동일 재시도 중단", out)

    # a) REVIEW_FAIL + LOW → inner retry preserves, exhausted Task rolls back
    def test_fail_low_keeps_tree_partial_handoff(self):
        self._mock_pipeline(
            ReviewResult(passed=False, reason="라벨 누락",
                         status=REVIEW_FAIL, risk="LOW")
        )
        task = self._task()
        result, out = self._run_retry(task)
        self.assertFalse(result.success)
        self.assertEqual(len(self.rollback_calls), 1)
        self.assertEqual(task.retry_scope, "restart")
        self.assertFalse(task.keep_working_tree)
        self.assertTrue(task.is_rolled_back)
        self.assertEqual(task.worktree_disposition, "CLEAN_ROLLBACK")
        self.assertIn("LOW", out)
        self.assertIn("is_rolled_back=False", out)
        self.assertIn("재시도 전략: LOW", out)

    # b) REVIEW_FAIL + MID → inner retry preserves, exhausted Task rolls back
    def test_fail_mid_keeps_tree_full_handoff(self):
        self._mock_pipeline(
            ReviewResult(passed=False, reason="구조 과다 구현",
                         status=REVIEW_FAIL, risk="MID")
        )
        task = self._task()
        result, out = self._run_retry(task)
        self.assertFalse(result.success)
        self.assertEqual(len(self.rollback_calls), 1)
        self.assertEqual(task.retry_scope, "restart")
        self.assertFalse(task.keep_working_tree)
        self.assertTrue(task.is_rolled_back)
        self.assertEqual(task.worktree_disposition, "CLEAN_ROLLBACK")
        self.assertIn("MID", out)
        self.assertIn("is_rolled_back=False", out)
        self.assertIn("재시도 전략: MID", out)

    # c) REVIEW_FAIL + HIGH → rollback 호출
    def test_fail_high_rolls_back(self):
        self._mock_pipeline(
            ReviewResult(passed=False, reason="근본 재작성 필요",
                         status=REVIEW_FAIL, risk="HIGH")
        )
        task = self._task()
        result, out = self._run_retry(task)
        self.assertFalse(result.success)
        # max_retry=2 → 실패한 매 시도마다 롤백 (2회)
        self.assertEqual(len(self.rollback_calls), 2)
        self.assertEqual(task.retry_scope, "restart")
        self.assertFalse(task.keep_working_tree)
        self.assertIn("재시도 전략: 전체 롤백(restart)", out)
        self.assertIn("실패 단계:", out)          # 사유 표기 (HIGH 오판 방지)
        handoff = self._handoff_text(task)
        self.assertIn("처음부터", handoff)
        self.assertIn("롤백되었다", handoff)

    def test_high_rollback_then_low_preserve_resets_terminal_disposition(self):
        """Earlier rollback evidence must not make a later preserved diff auto-retryable."""
        from manager import ManagerResult

        reviews = iter(
            (
                ReviewResult(
                    passed=False,
                    reason="scope restart",
                    status=REVIEW_FAIL,
                    risk="HIGH",
                ),
                ReviewResult(
                    passed=False,
                    reason="local correction",
                    status=REVIEW_FAIL,
                    risk="LOW",
                ),
            )
        )

        def fake_pipeline(task):
            review = next(reviews)
            task.changed_files = ["src/A.java"]
            task.review_status = review.status
            task.review_result = review.reason
            task.review_risk = review.risk
            task.failure_stage = "REVIEW"
            task.failure_reason = review.reason
            task.failure_type = "review"
            task.fix_scope = "RETRYABLE"
            task.verification_status = "FAILED"
            return ManagerResult(
                success=False,
                git_result=GitDiffResult(
                    success=True,
                    changed_files=["src/A.java"],
                    diff="diff --git a/src/A.java b/src/A.java",
                ),
                build_test_result=BuildTestResult(
                    success=True,
                    build_status="PASS",
                    test_status="SKIPPED",
                ),
                review_result=review,
                message="FAIL",
            )

        self.manager._run_pipeline = fake_pipeline

        def fake_rollback(task=None, baseline=None):
            self.rollback_calls.append(task)
            task.is_rolled_back = True
            return True, "fake rollback"

        self.manager.collector.rollback = fake_rollback
        task = self._task()
        result, _ = self._run_retry(task)
        self.assertFalse(result.success)
        self.assertEqual(2, len(self.rollback_calls))
        self.assertTrue(task.is_rolled_back)
        self.assertFalse(task.keep_working_tree)
        self.assertEqual("CLEAN_ROLLBACK", task.worktree_disposition)

    # d) 자연어 문장은 deterministic 증거가 아니므로 risk를 승격하지 않는다.
    def test_hard_constraint_overrides_low_to_high(self):
        from reviewer import normalize_review_risk

        texts = ["비밀 노출 없음"]
        self.assertEqual(
            normalize_review_risk("LOW", True, texts), "LOW"
        )
        texts2 = ["위험한 SQL 없음"]
        self.assertEqual(
            normalize_review_risk("MID", True, texts2), "MID"
        )
        # 경계 지적(하드 제약 아님)은 LOW 유지
        self.assertEqual(
            normalize_review_risk("LOW", True, ["라벨 텍스트 누락"]), "LOW"
        )
        # 모듈 경계 문장도 실제 경로/diff 증거 없이는 승격하지 않는다.
        self.assertEqual(
            normalize_review_risk("LOW", True, ["core 수정 없음"]),
            "LOW",
        )

    # e) build=FAIL → 중간 시도는 보존하고 budget 소진 뒤 rollback
    def test_build_fail_rolls_back_once_after_budget_exhaustion(self):
        self._mock_pipeline(
            ReviewResult(passed=False, reason="국소 지적",
                         status=REVIEW_FAIL, risk="LOW"),
            build_status="FAIL",
        )
        task = self._task()
        result, out = self._run_retry(task)
        self.assertFalse(result.success)
        self.assertEqual(len(self.rollback_calls), 1)
        self.assertEqual(task.retry_scope, "restart")
        self.assertFalse(task.keep_working_tree)

    # f) RISK 파식 실패 + REVIEW_FAIL → HIGH (fail-closed)
    def test_missing_risk_defaults_to_high(self):
        from reviewer import normalize_review_risk

        self.assertEqual(normalize_review_risk("", True, ["국소 지적"]), "HIGH")
        self.assertEqual(normalize_review_risk("WEIRD", True, []), "HIGH")
        # PASS 에서는 등급 미사용
        self.assertEqual(normalize_review_risk("LOW", False, []), "")

    # g) REVIEW_UNAVAILABLE / ERROR → 등급 미사용, 수동 리뷰 대기
    def test_unavailable_error_no_risk_or_rollback(self):
        from manager import ManagerResult
        from reviewer import Reviewer

        for status in (REVIEW_UNAVAILABLE, REVIEW_ERROR):
            with self.subTest(status=status):
                review = ReviewResult(
                    passed=False, reason="불가", status=status, risk=""
                )

                def fake_pipeline(task, _r=review):
                    task.review_status = _r.status
                    task.review_result = _r.reason
                    task.review_risk = ""
                    task.build = {"status": "PASS", "output": ""}
                    task.failure_stage = "REVIEW"
                    task.failure_code = REVIEW_UNAVAILABLE
                    task.failure_reason = _r.reason
                    task.failure_type = "infrastructure"
                    task.fix_scope = "NON_RETRYABLE"
                    task.verification_status = "PARTIAL"
                    bt = BuildTestResult(
                        success=True, build_status="PASS", test_status="SKIPPED"
                    )
                    git = GitDiffResult(
                        success=True, changed_files=["src/A.java"],
                        diff="diff --git a/src/A.java b/src/A.java",
                    )
                    return ManagerResult(
                        success=False, git_result=git,
                        build_test_result=bt, review_result=_r, message="FAIL",
                    )

                self.manager._run_pipeline = fake_pipeline
                self.rollback_calls.clear()

                def fake_rollback(task=None, baseline=None):
                    self.rollback_calls.append(task)
                    if task is not None:
                        task.is_rolled_back = True
                    return True, "fake rollback"

                self.manager.collector.rollback = fake_rollback
                task = self._task()
                result, _ = self._run_retry(task)
                self.assertFalse(result.success)
                self.assertEqual(task.review_risk, "")       # 등급 미사용
                self.assertEqual(task.status, "AWAITING_QA")
                self.assertEqual(len(self.rollback_calls), 0)

    # h) REVIEW_PASS → 롤백 없음, RISK 무시
    def test_pass_no_rollback_risk_ignored(self):
        self._mock_pipeline(
            ReviewResult(passed=True, reason="PASS",
                         status=REVIEW_PASS, risk="")
        )
        task = self._task()
        result, _ = self._run_retry(task)
        self.assertTrue(result.success)
        self.assertEqual(self.rollback_calls, [])
        self.assertEqual(task.review_risk, "")
        # ReviewResult 생성자: PASS에 risk를 넣어도 ""로 강제
        rr = ReviewResult(passed=True, status=REVIEW_PASS, risk="HIGH")
        self.assertEqual(rr.risk, "")


class ReviewSanitizationTest(unittest.TestCase):
    """리뷰 산출물 정제 — stream-json raw가 리포트/TaskState로 흐르지 않게."""

    def _stream(self, final_text: str) -> str:
        """실제 droid -o stream-json 출력 모사 (프롬프트 에코 포함)."""
        prompt_echo = (
            "[출력 형식] RESULT: PASS 또는 FAIL\\nREASON: <한 줄 요약>\\n"
            "RISK: FAIL 인 경우만 LOW|MID|HIGH"
        )
        return (
            '{"type":"system","subtype":"init"}\n'
            f'{{"type":"message","role":"user","text":"{prompt_echo}"}}\n'
            '{"type":"reasoning","id":"r1","text":"내부 추론 본문"}\n'
            '{"type":"tool_call","toolName":"Read","parameters":{"file_path":"/x.vue"}}\n'
            f'{{"type":"completion","finalText":"{final_text}","numTurns":3}}'
        )

    def test_stream_json_review_extracts_final_text_result(self):
        """stream-json이어도 finalText의 RESULT/REASON/RISK를 읽는다(오판 방지)."""
        from reviewer import Reviewer

        raw = self._stream("RESULT: PASS\\nREASON: 요구사항 충족\\nRISK: NONE")
        rv = Reviewer(working_dir=".", timeout=5)
        verdict, reason, extra, risk = rv._parse_review_output(
            __import__("worker")._extract_droid_text(raw)
        )
        self.assertEqual(verdict, "PASS")
        self.assertEqual(reason, "요구사항 충족")
        # 프롬프트 에코의 'RESULT: PASS 또는 FAIL' 템플릿은 finalText 우선으로 무시됨
        self.assertNotIn("출력 형식", reason)

    def test_apply_to_state_strips_template_and_raw(self):
        """TaskState.review_result에서 템플릿 마커/stream-json 조각 제거."""
        from reviewer import Reviewer
        from task_state import TaskState

        task = TaskState(task_id="T", requirement="x", working_dir=".")
        rr = ReviewResult(
            passed=False,
            reason="[codex 리뷰] 확인 결과",
            details=[
                "[출력 형식] RESULT: PASS 또는 FAIL",
                '{"type":"reasoning","text":"..."}',
                "실제 위반: 컬럼명 불일치",
                "DETAIL: <필요 시 추가 설명>",
            ],
            status="REVIEW_FAIL",
            risk="LOW",
        )
        Reviewer._apply_to_state(task, rr)
        self.assertNotIn("[출력 형식]", task.review_result)
        self.assertNotIn('{"type":', task.review_result)
        self.assertNotIn("DETAIL: <", task.review_result)
        self.assertIn("실제 위반: 컬럼명 불일치", task.review_result)
        self.assertLess(len(task.review_result), 500)   # 수백 KB 아님

    def test_reporter_blocks_raw_in_markdown(self):
        """마크다운 리뷰 섹션에 raw stream-json이 들어가면 안내줄로 대체."""
        from reporter import Reporter
        from task_state import TaskState

        task = TaskState(task_id="T2", requirement="x", working_dir=".")
        task.status = "FAILED"
        task.review_status = "REVIEW_FAIL"
        task.review_result = '{"type":"reasoning","text":"..."}\n[출력 형식] RESULT: PASS 또는 FAIL'

        md = Reporter().generate_markdown(task)
        self.assertIn("추출 실패", md)
        self.assertNotIn('"type":"reasoning"', md)

    def test_codex_review_raw_stdout_sanitized(self):
        """_codex_review 가 raw를 저장하지 않고 최종 텍스트만 파싱."""
        from reviewer import Reviewer
        from task_state import TaskState

        task = TaskState(
            task_id="T3", requirement="버그 수정해",
            working_dir=".", task_mode="MODIFICATION",
        )
        rv = Reviewer(working_dir=".", timeout=5)
        raw = self._stream("RESULT: FAIL\\nREASON: 로직 오류\\nRISK: LOW")

        with mock.patch.object(
            rv, "_codex_review",
            return_value=("FAIL", "로직 오류", ["detail 한 줄"], True, "", "LOW"),
        ):
            rr = rv.review(
                requirement="버그 수정해", worker_success=True, exit_code=0,
                changed_files=["src/A.vue"], worker_stdout="",
                git_diff="diff --git a/src/A.vue b/src/A.vue", task=task,
            )
        self.assertEqual(task.review_status, "REVIEW_FAIL")
        self.assertEqual(task.review_risk, "LOW")
        # 리뷰 본문에 raw stream-json이 없음
        self.assertNotIn('{"type":', task.review_result)
        self.assertLess(len(task.review_result), 500)


class MultiModuleBuildTest(unittest.TestCase):
    """멀티모듈 워크스페이스 빌드 탐지 (18차) — cap/pom.xml 오판 사고 수정."""

    def _runner(self, root: Path) -> "BuildTestRunner":
        from build_test_runner import BuildTestRunner
        return BuildTestRunner(str(root))

    def test_module_extraction_from_changed_files(self):
        from build_test_runner import BuildTestRunner

        m = BuildTestRunner._module_of
        self.assertEqual(m("cap/src/main/java/cap/pcap/X.xml"), "cap")
        self.assertEqual(m("capui/src/pages/admin/X.vue"), "capui")
        self.assertEqual(m("com\\src\\main\\java\\Com.java"), "com")
        self.assertEqual(m("docs/readme.md"), "docs")
        self.assertEqual(m(""), "")

    def test_detect_targets_falls_back_to_changed_modules(self):
        """루트에 빌드 파일 없으면 changed_files 모듈(pom.xml 보유)만 탐지."""
        import harness_temp as tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "cap" / "src").mkdir(parents=True)
            (root / "cap" / "pom.xml").write_text("<project/>", encoding="utf-8")
            (root / "capui" / "src").mkdir(parents=True)
            (root / "capui" / "package.json").write_text("{}", encoding="utf-8")

            targets = self._runner(root)._detect_build_targets([
                "cap/src/main/java/cap/pcap/comm/query/PcapCommQry.xml",
                "capui/src/pages/admin/pcap/op/PcapOp0010List.vue",
            ])
            self.assertEqual(targets, [
                ("cap", root / "cap", ()),
                ("capui", root / "capui", ()),
            ])

    def test_root_build_file_takes_precedence(self):
        """루트 pom.xml 있으면 기존대로 루트 빌드만."""
        import harness_temp as tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            root.joinpath("pom.xml").write_text("<project/>", encoding="utf-8")
            root.joinpath("cap").mkdir()
            root.joinpath("cap", "pom.xml").write_text("<project/>", encoding="utf-8")

            targets = self._runner(root)._detect_build_targets(["cap/src/A.java"])
            self.assertEqual(targets, [("root", root, ())])

    def test_package_json_is_detected_as_build_target(self):
        """package.json은 실제 파일 증거로 build target이 된다."""
        import harness_temp as tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "capui" / "src").mkdir(parents=True)
            (root / "capui" / "package.json").write_text("{}", encoding="utf-8")

            targets = self._runner(root)._detect_build_targets([
                "capui/src/pages/admin/pcap/op/PcapOp0010List.vue",
            ])
            self.assertEqual(targets, [("capui", root / "capui", ())])

    def test_run_pass_when_module_build_passes(self):
        """모듈 Maven 빌드 성공 → PASS (SKIPPED 오판 회귀 방지)."""
        import harness_temp as tempfile
        from pathlib import Path
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "cap").mkdir()
            (root / "cap" / "pom.xml").write_text("<project/>", encoding="utf-8")

            r = self._runner(root)
            # mvn -v 성공, compile 성공
            with patch.object(r, "_run", side_effect=[(0, "", ""), (0, "BUILD OK", "")]):
                result = r.run(changed_files=["cap/src/A.java"])
            self.assertEqual(result.build_status, "PASS")
            self.assertTrue(result.success)
            self.assertEqual(result.details.get("modules"), ["cap"])

    def test_run_fail_when_module_build_fails(self):
        """모듈 빌드 실패 → FAIL + 실패 모듈명."""
        import harness_temp as tempfile
        from pathlib import Path
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "cap").mkdir()
            (root / "cap" / "pom.xml").write_text("<project/>", encoding="utf-8")

            r = self._runner(root)
            with patch.object(r, "_run", side_effect=[(0, "", ""), (1, "", "ERROR")]):
                result = r.run(changed_files=["cap/src/A.java"])
            self.assertEqual(result.build_status, "FAIL")
            self.assertFalse(result.success)
            self.assertEqual(result.details.get("failed_modules"), ["cap"])

    def test_run_skipped_when_no_targets(self):
        """빌드 대상 없음 → 기존대로 SKIPPED(success=False)."""
        import harness_temp as tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as td:
            result = self._runner(Path(td)).run(changed_files=["docs/a.md"])
            self.assertEqual(result.build_status, "SKIPPED")
            self.assertFalse(result.success)

    def test_e4u_u03_target_modules_select_profile_builds_without_root_pom(self):
        from build_test_runner import BuildTestRunner

        runner = BuildTestRunner(r"D:\inha_dev", profile=E4U_PROFILE)
        targets = runner._detect_build_targets(
            changed_files=[], target_modules=["suc", "sucui"]
        )
        self.assertEqual([item[0] for item in targets], ["suc", "sucui"])
        self.assertEqual(
            targets[0][2], ("mvn.cmd", "clean", "install", "-P", "dev")
        )
        self.assertEqual(
            targets[1][2], ("npm.cmd", "run", "build:dev")
        )


class UiMapInjectionTest(unittest.TestCase):
    """URL/화면 키 요구사항 → ui_map 선택 주입."""

    def test_extract_screen_keys_variants(self):
        from worker import extract_screen_keys

        self.assertEqual(
            extract_screen_keys("/inhatc/M106680 화면 수정해"), ["M106680"]
        )
        self.assertIn(
            "PCAP_OP_0210",
            extract_screen_keys("PCAP_OP_0210 평가계획서 섹션 수정"),
        )
        self.assertIn(
            "PcapOp0210",
            extract_screen_keys("PcapOp0210Detail.vue 파일 수정"),
        )
        self.assertIn(
            "OP0210",
            extract_screen_keys("OP0210 화면에서 0점 행 숨김"),
        )
        self.assertEqual(extract_screen_keys("일반 버그 수정 요청"), [])
        self.assertEqual(extract_screen_keys(""), [])

    def test_ui_map_section_matched_for_op0210(self):
        """실제 ui_map 파일에서 OP0210 섹션만 매칭(D:\\agents 존재 전제)."""
        import worker as worker_mod

        if not worker_mod._ui_map_dir(E4U_PROFILE).is_dir():
            self.skipTest("ui_map 디렉토리 없음")

        body, matched = worker_mod.load_ui_map_section(
            "PCAP_OP_0210 평가계획서 평가개요 섹션 수정", profile=E4U_PROFILE
        )
        self.assertEqual(matched, ["PCAP_OP_0210"])
        self.assertIn("OP0210", body)                 # 섹션 헤더
        self.assertIn("PcapOp0210", body)             # 화면 ID/경로
        self.assertLessEqual(len(body), worker_mod.UI_MAP_SECTION_CAP)
        # 다른 화면 섹션이 주입되지 않았는지 (인접 섹션 미포함)
        self.assertNotIn("### OP0110", body)
        self.assertNotIn("### OP0310", body)

    def test_build_prompt_contains_ui_map_section(self):
        """프롬프트에 [화면 맵] 섹션 + 매칭 키 표기(실제 ui_map 존재 전제)."""
        import worker as worker_mod

        if not worker_mod._ui_map_dir(E4U_PROFILE).is_dir():
            self.skipTest("ui_map 디렉토리 없음")

        prompt = worker_mod.build_prompt(
            "/inhatc/M106680 화면의 OP0210 평가개요 수정", ["capui"],
            profile=E4U_PROFILE, role="droid",
        )
        self.assertIn("[화면 맵]", prompt)
        self.assertIn("OP0210", prompt)
        self.assertNotIn("### OP0110", prompt)

    def test_build_prompt_no_url_no_ui_map(self):
        """URL/화면 키 없으면 [화면 맵] 없음 — 프롬프트 구성 불변."""
        import worker as worker_mod

        prompt = worker_mod.build_prompt(
            "그리드 미렌더 버그 수정해", ["capui"],
            profile=E4U_PROFILE, role="droid",
        )
        self.assertNotIn("[화면 맵]", prompt)

    def test_ui_map_missing_dir_skips_cleanly(self):
        """ui_map 디렉토리가 없어도 크래시 없이 스킵."""
        import worker as worker_mod

        with mock.patch.object(worker_mod, "_ui_map_dir", return_value=Path("Z:/nonexistent")):
            body, matched = worker_mod.load_ui_map_section("PCAP_OP_0210 수정")
        self.assertEqual(body, "")
        self.assertEqual(matched, [])

    def test_menu_id_only_no_section(self):
        """MENU_ID_INDEX 표에 없는 ID는 fail-closed 스킵(추측 금지)."""
        import worker as worker_mod

        if not worker_mod._ui_map_dir(E4U_PROFILE).is_dir():
            self.skipTest("ui_map 디렉토리 없음")
        body, matched = worker_mod.load_ui_map_section(
            "/inhatc/M999999 수정", profile=E4U_PROFILE
        )
        self.assertEqual(body, "")
        self.assertEqual(matched, [])

    def test_resolve_menu_id_from_index(self):
        """메뉴 ID → MENU_ID_INDEX §4 표 → 화면번호 해석."""
        import worker as worker_mod

        if not worker_mod._ui_map_dir(E4U_PROFILE).is_dir():
            self.skipTest("ui_map 디렉토리 없음")
        # M107920 → PcapOp0030List.vue → OP0030
        self.assertEqual(worker_mod.resolve_menu_id("M107920", E4U_PROFILE), "OP0030")
        self.assertEqual(worker_mod.resolve_menu_id("M106680", E4U_PROFILE), "OP0010")
        # 표에 없는 ID / 비메뉴 ID → '' (fail-closed)
        self.assertEqual(worker_mod.resolve_menu_id("M999999", E4U_PROFILE), "")
        self.assertEqual(worker_mod.resolve_menu_id("PCAP_OP_0210", E4U_PROFILE), "")
        self.assertEqual(worker_mod.resolve_menu_id("", E4U_PROFILE), "")

    def test_menu_url_injects_section_via_index(self):
        """URL(/inhatc/M######) 단독 → 인덱스 경유 섹션 주입 (18차 체인)."""
        import worker as worker_mod

        if not worker_mod._ui_map_dir(E4U_PROFILE).is_dir():
            self.skipTest("ui_map 디렉토리 없음")
        body, matched = worker_mod.load_ui_map_section(
            "/inhatc/M107920 교과목 프로파일 화면 수정", profile=E4U_PROFILE
        )
        self.assertEqual(matched, ["M107920"])
        self.assertIn("### OP0030", body)               # 교과목 프로파일 조회 섹션
        self.assertIn("PcapOp0030List", body)
        # 인접 섹션 미포함
        self.assertNotIn("### OP0020", body)
        self.assertNotIn("### OP0110", body)

    def test_sitn_suc_urls_and_controllers_resolve(self):
        """§1 URL 스키마(/sitn, /homepage) + P/S 컨트롤러 표기 매칭."""
        import worker as worker_mod

        if not worker_mod._ui_map_dir(E4U_PROFILE).is_dir():
            self.skipTest("ui_map 디렉토리 없음")
        # /sitn/M106709 → SitnBs0101List.vue → BS0101
        body, matched = worker_mod.load_ui_map_section(
            "/sitn/M106709 공통코드관리 수정", profile=E4U_PROFILE
        )
        self.assertEqual(matched, ["M106709"])
        self.assertIn("### BS0101", body)
        # S-접두 컨트롤러 직접 표기 → CM0060
        body2, matched2 = worker_mod.load_ui_map_section(
            "SsucCm0060List.vue 공통코드 관리 화면", profile=E4U_PROFILE
        )
        self.assertEqual(matched2, ["SsucCm0060"])
        self.assertIn("### CM0060", body2)


class FailureArbitrationContractTest(unittest.TestCase):
    """Build/Test evidence outranks Reviewer infrastructure failures."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self._old_cwd = os.getcwd()
        os.chdir(self._tmp.name)
        self.manager = Manager(
            working_dir=self._tmp.name,
            model="",
            max_retry=2,
            timeout=5,
            worker_type="droid",
            max_workers=1,
        )

    def tearDown(self):
        self.manager.shutdown()
        os.chdir(self._old_cwd)
        self._tmp.cleanup()

    @staticmethod
    def _build_result(status: str, output: str = "") -> BuildTestResult:
        return BuildTestResult(
            success=status == "PASS",
            build_status=status,
            test_status="SKIPPED",
            build_output=output,
            error="compile failed" if status == "FAIL" else "",
            details={"reason": "no build target"} if status == "SKIPPED" else {},
        )

    def _task(self, task_id: str = "TASK-FIXTURE") -> TaskState:
        return TaskState(
            task_id=task_id,
            requirement="기능 오류 수정",
            working_dir=self._tmp.name,
            task_mode="MODIFICATION",
        )

    def _configure(self, builds, reviews, max_retry: int = 2):
        self.manager.max_retry = max_retry
        worker = SequenceWorker([_wr_ok()] * max_retry)
        collector = FakeCollector(_git(
            diff=(
                "diff --git a/src/A.java b/src/A.java\n"
                "--- a/src/A.java\n+++ b/src/A.java\n"
                "@@ -1 +1 @@\n-old\n+new\n"
            ),
            files=["src/A.java"],
        ))
        runner = SequenceRunner(builds)
        reviewer = SequenceReviewer(reviews)
        self.manager._get_worker = lambda task: (worker, "droid")
        self.manager.collector = collector
        self.manager.build_test_runner = runner
        self.manager.reviewer = reviewer
        return worker, collector, runner, reviewer

    def _run(self, task):
        output = io.StringIO()
        with redirect_stdout(output):
            result = self.manager.run_with_retry(task)
        return result, output.getvalue()

    def test_actual_case_fixture_build_fail_review_error_retries_worker_without_initial_rollback(self):
        worker, collector, _, reviewer = self._configure(
            [
                self._build_result("FAIL", "password=buildsecret"),
                self._build_result("PASS"),
            ],
            [
                ReviewResult(
                    passed=False,
                    reason="token=reviewsecret",
                    details=["password=reviewsecret"],
                    status=REVIEW_ERROR,
                ),
                ReviewResult(passed=True, reason="fresh diff ok", status=REVIEW_PASS),
            ],
        )
        task = self._task("TASK-20260826-FIXTURE")

        result, _ = self._run(task)

        self.assertTrue(result.success)
        self.assertEqual(worker.calls, 2)
        self.assertEqual(reviewer.calls, 2)
        self.assertEqual(collector.rollback_calls, 0)
        first = task.attempt_history[0]
        self.assertEqual(first["failure_code"], "BUILD_FAILED")
        self.assertEqual(first["failure_type"], "build")
        self.assertEqual(first["secondary_failures"], [REVIEW_ERROR])
        self.assertIn("빌드 출력", worker.handoffs[1])
        self.assertNotIn("buildsecret", worker.handoffs[1])
        self.assertNotIn("reviewsecret", worker.handoffs[1])
        self.assertNotIn("리뷰 의견", worker.handoffs[1])

    def test_build_fail_review_unavailable_keeps_build_primary_and_retries_worker(self):
        worker, collector, _, _ = self._configure(
            [self._build_result("FAIL"), self._build_result("PASS")],
            [
                ReviewResult(passed=False, reason="offline", status=REVIEW_UNAVAILABLE),
                ReviewResult(passed=True, reason="ok", status=REVIEW_PASS),
            ],
        )
        task = self._task()

        result, _ = self._run(task)

        self.assertTrue(result.success)
        self.assertEqual(worker.calls, 2)
        self.assertEqual(collector.rollback_calls, 0)
        self.assertEqual(task.attempt_history[0]["failure_code"], "BUILD_FAILED")
        self.assertEqual(
            task.attempt_history[0]["secondary_failures"], [REVIEW_UNAVAILABLE]
        )

    def test_build_fail_review_partial_keeps_build_primary(self):
        worker, collector, _, _ = self._configure(
            [self._build_result("FAIL"), self._build_result("PASS")],
            [
                ReviewResult(
                    passed=False,
                    reason="coverage incomplete",
                    status=REVIEW_ERROR,
                    failure_code=REVIEW_COVERAGE_PLAN_INVALID,
                ),
                ReviewResult(passed=True, reason="ok", status=REVIEW_PASS),
            ],
        )
        task = self._task("TASK-BUILD-PRIMARY-PARTIAL")

        result, _ = self._run(task)

        self.assertTrue(result.success)
        self.assertEqual(worker.calls, 2)
        self.assertEqual(collector.rollback_calls, 0)
        first = task.attempt_history[0]
        self.assertEqual(first["failure_code"], "BUILD_FAILED")
        self.assertEqual(first["secondary_failures"], [REVIEW_COVERAGE_PLAN_INVALID])

    def test_build_fail_semantic_review_feedback_is_merged_into_worker_handoff(self):
        worker, collector, _, _ = self._configure(
            [
                self._build_result("FAIL", "compile symbol missing"),
                self._build_result("PASS"),
            ],
            [
                ReviewResult(
                    passed=False,
                    reason="null guard missing",
                    details=["A.java guard required"],
                    status=REVIEW_FAIL,
                    risk="LOW",
                ),
                ReviewResult(passed=True, reason="ok", status=REVIEW_PASS),
            ],
        )
        task = self._task()

        result, _ = self._run(task)

        self.assertTrue(result.success)
        self.assertEqual(collector.rollback_calls, 0)
        self.assertIn("빌드 출력", worker.handoffs[1])
        self.assertIn("null guard missing", worker.handoffs[1])
        self.assertEqual(
            task.attempt_history[0]["secondary_failures"], [REVIEW_FAIL]
        )

    def test_build_pass_review_error_then_manager_recall_passes_without_worker_retry(self):
        worker, collector, _, reviewer = self._configure(
            [self._build_result("PASS")],
            [
                ReviewResult(passed=False, reason="parse", status=REVIEW_ERROR),
                ReviewResult(passed=True, reason="ok", status=REVIEW_PASS),
            ],
        )
        task = self._task()

        result, _ = self._run(task)

        self.assertTrue(result.success)
        self.assertEqual(worker.calls, 1)
        self.assertEqual(reviewer.calls, 2)
        self.assertEqual(collector.rollback_calls, 0)
        self.assertEqual(reviewer.call_args[0]["changed_files"], reviewer.call_args[1]["changed_files"])
        self.assertEqual(reviewer.call_args[0]["git_diff"], reviewer.call_args[1]["git_diff"])

    def test_each_manager_reviewer_invocation_runs_one_model_call(self):
        worker, collector, _, _ = self._configure(
            [self._build_result("PASS")],
            [ReviewResult(passed=True, status=REVIEW_PASS)],
        )
        actual_reviewer = Reviewer(self._tmp.name)
        self.manager.reviewer = actual_reviewer
        task = self._task()
        responses = [
            (None, None, ["parse"], False, "error", ""),
            ("PASS", "ok", [], True, "", ""),
        ]

        with mock.patch("reviewer.load_review_rules", return_value="rules"), mock.patch.object(
            actual_reviewer, "_codex_review", side_effect=responses
        ) as model_call:
            result, _ = self._run(task)

        self.assertTrue(result.success)
        self.assertEqual(worker.calls, 1)
        self.assertEqual(model_call.call_count, 2)
        self.assertEqual(collector.rollback_calls, 0)

    @staticmethod
    def _two_batch_diff() -> str:
        return "".join(
            f"diff --git a/src/{name}.java b/src/{name}.java\n"
            f"--- a/src/{name}.java\n+++ b/src/{name}.java\n"
            f"@@ -1 +1 @@\n-old\n+{'x' * 180}\n"
            for name in ("A", "B")
        )

    def test_manager_sends_only_each_batch_and_aggregates_full_coverage(self):
        diff = self._two_batch_diff()
        worker = SequenceWorker([_wr_ok()])
        collector = FakeCollector(_git(diff=diff, files=["src/A.java", "src/B.java"]))
        self.manager._get_worker = lambda task: (worker, "droid")
        self.manager.collector = collector
        self.manager.build_test_runner = SequenceRunner([self._build_result("PASS")])
        actual = Reviewer(self._tmp.name)
        self.manager.reviewer = actual
        task = self._task("TASK-BATCH-PASS")

        with mock.patch.dict(os.environ, {
            "KKM_REVIEW_MAX_BATCH_CHARS": "300", "KKM_REVIEW_MAX_BATCHES": "12"
        }), mock.patch("reviewer.load_review_rules", return_value="rules"), mock.patch.object(
            actual, "_codex_review", return_value=("PASS", "ok", [], True, "", "")
        ) as model_call:
            result, console = self._run(task)

        self.assertTrue(result.success)
        self.assertEqual(model_call.call_count, 2)
        sent = [call.kwargs["git_diff"] for call in model_call.call_args_list]
        self.assertEqual(len(set(sent)), 2)
        self.assertTrue(all(part != diff for part in sent))
        self.assertEqual(result.review_result.coverage["coverage_percent"], 100)
        self.assertEqual(result.review_result.coverage["missing_atom_count"], 0)
        self.assertIn("batch 1/2", console)
        self.assertIn("batch 2/2", console)

    def test_job_wide_infrastructure_retry_is_one_for_all_batches(self):
        diff = self._two_batch_diff()
        worker = SequenceWorker([_wr_ok()])
        collector = FakeCollector(_git(diff=diff, files=["src/A.java", "src/B.java"]))
        self.manager._get_worker = lambda task: (worker, "droid")
        self.manager.collector = collector
        self.manager.build_test_runner = SequenceRunner([self._build_result("PASS")])
        actual = Reviewer(self._tmp.name)
        self.manager.reviewer = actual
        responses = [
            ("PASS", "ok", [], True, "", ""),
            (None, None, ["safe diagnostic"], False, "structured_invalid", ""),
            ("PASS", "ok", [], True, "", ""),
        ]
        task = self._task("TASK-BATCH-RETRY")

        with mock.patch.dict(os.environ, {
            "KKM_REVIEW_MAX_BATCH_CHARS": "300", "KKM_REVIEW_MAX_BATCHES": "12"
        }), mock.patch("reviewer.load_review_rules", return_value="rules"), mock.patch.object(
            actual, "_codex_review", side_effect=responses
        ) as model_call:
            result, _ = self._run(task)

        self.assertTrue(result.success)
        self.assertEqual(model_call.call_count, 3)
        self.assertEqual(worker.calls, 1)
        self.assertEqual(collector.rollback_calls, 0)

    def test_deterministic_capacity_error_has_no_retry_model_or_rollback(self):
        diff = (
            "diff --git a/src/Huge.java b/src/Huge.java\n"
            "--- a/src/Huge.java\n+++ b/src/Huge.java\n@@ -1 +1 @@\n-old\n+" +
            "x" * 13000 + "\n"
        )
        worker = SequenceWorker([_wr_ok()])
        collector = FakeCollector(_git(diff=diff, files=["src/Huge.java"]))
        self.manager._get_worker = lambda task: (worker, "droid")
        self.manager.collector = collector
        self.manager.build_test_runner = SequenceRunner([self._build_result("PASS")])
        actual = Reviewer(self._tmp.name)
        self.manager.reviewer = actual
        task = self._task("TASK-CAPACITY")

        with mock.patch.object(actual, "_codex_review") as model_call:
            result, _ = self._run(task)

        self.assertFalse(result.success)
        model_call.assert_not_called()
        self.assertEqual(task.failure_code, DIFF_TOO_LARGE_FOR_AUTOMATED_REVIEW)
        self.assertEqual(task.verification_status, "PARTIAL")
        self.assertEqual(task.status, "AWAITING_QA")
        self.assertEqual(task.stage, "AWAITING_QA")
        self.assertTrue(task.keep_working_tree)
        self.assertEqual(worker.calls, 1)
        self.assertEqual(collector.rollback_calls, 0)

    def test_structured_invalid_retry_exhaustion_preserves_specific_code(self):
        worker, collector, _, _ = self._configure(
            [self._build_result("PASS")], [ReviewResult(passed=True, status=REVIEW_PASS)]
        )
        actual = Reviewer(self._tmp.name)
        self.manager.reviewer = actual
        task = self._task("TASK-STRUCTURED-INVALID")
        responses = [
            (None, None, ["safe parser diagnostic"], False, "structured_invalid", ""),
            (None, None, ["safe parser diagnostic"], False, "structured_invalid", ""),
        ]

        with mock.patch("reviewer.load_review_rules", return_value="rules"), mock.patch.object(
            actual, "_codex_review", side_effect=responses
        ) as model_call:
            result, _ = self._run(task)

        self.assertFalse(result.success)
        self.assertEqual(model_call.call_count, 2)
        self.assertEqual(task.failure_code, STRUCTURED_OUTPUT_INVALID)
        self.assertEqual(task.status, "AWAITING_QA")
        self.assertEqual(task.verification_status, "PARTIAL")
        self.assertTrue(task.keep_working_tree)
        self.assertEqual(worker.calls, 1)
        self.assertEqual(collector.rollback_calls, 0)

    def test_binary_diff_has_no_model_retry_or_rollback(self):
        diff = (
            "diff --git a/image.png b/image.png\n"
            "index 1234567..abcdef0 100644\nBinary files a/image.png and b/image.png differ\n"
        )
        worker = SequenceWorker([_wr_ok()])
        collector = FakeCollector(_git(diff=diff, files=["image.png"]))
        self.manager._get_worker = lambda task: (worker, "droid")
        self.manager.collector = collector
        self.manager.build_test_runner = SequenceRunner([self._build_result("PASS")])
        actual = Reviewer(self._tmp.name)
        self.manager.reviewer = actual
        task = self._task("TASK-BINARY")

        with mock.patch.object(actual, "_codex_review") as model_call:
            result, _ = self._run(task)

        self.assertFalse(result.success)
        model_call.assert_not_called()
        self.assertEqual(task.failure_code, BINARY_DIFF_REQUIRES_MANUAL_REVIEW)
        self.assertEqual(task.status, "AWAITING_QA")
        self.assertEqual(collector.rollback_calls, 0)

    def test_coverage_below_100_cannot_aggregate_pass(self):
        with mock.patch.dict(os.environ, {
            "KKM_REVIEW_MAX_BATCH_CHARS": "300", "KKM_REVIEW_MAX_BATCHES": "12"
        }):
            plan = Reviewer(self._tmp.name).plan_diff_batches(self._two_batch_diff())
        first = plan.batches[0]
        result = Manager._aggregate_review_batches(
            [(first, ReviewResult(
                passed=True, status=REVIEW_PASS, reviewed_files=list(first.changed_files)
            ))],
            plan,
            ["src/A.java", "src/B.java"],
        )
        self.assertFalse(result.passed)
        self.assertEqual(result.status, REVIEW_ERROR)
        self.assertLess(result.coverage["coverage_percent"], 100)
        self.assertGreater(result.coverage["missing_atom_count"], 0)

    def test_multi_batch_semantic_fail_aggregation_deduplicates_evidence(self):
        with mock.patch.dict(os.environ, {
            "KKM_REVIEW_MAX_BATCH_CHARS": "300", "KKM_REVIEW_MAX_BATCHES": "12"
        }):
            plan = Reviewer(self._tmp.name).plan_diff_batches(self._two_batch_diff())
        results = [
            ReviewResult(
                passed=False, reason="same defect", details=["same detail", "first"],
                status=REVIEW_FAIL, risk="LOW",
                violation_codes=["SAME_CODE", "FIRST_CODE"],
                reviewed_files=list(plan.batches[0].changed_files),
            ),
            ReviewResult(
                passed=False, reason="same defect", details=["same detail", "second"],
                status=REVIEW_FAIL, risk="HIGH",
                violation_codes=["SAME_CODE", "SECOND_CODE"],
                reviewed_files=list(plan.batches[1].changed_files),
            ),
        ]

        result = Manager._aggregate_review_batches(
            list(zip(plan.batches, results)), plan, ["src/A.java", "src/B.java"]
        )

        self.assertEqual(result.status, REVIEW_FAIL)
        self.assertEqual(result.reason, "same defect")
        self.assertEqual(result.details, ["same detail", "first", "second"])
        self.assertEqual(result.violation_codes, ["SAME_CODE", "FIRST_CODE", "SECOND_CODE"])
        self.assertEqual(result.risk, "HIGH")
        self.assertEqual(result.coverage["coverage_percent"], 100)

    def test_semantic_finding_plus_incomplete_batch_remains_partial(self):
        with mock.patch.dict(os.environ, {
            "KKM_REVIEW_MAX_BATCH_CHARS": "300", "KKM_REVIEW_MAX_BATCHES": "12"
        }):
            plan = Reviewer(self._tmp.name).plan_diff_batches(self._two_batch_diff())
        first, second = plan.batches
        result = Manager._aggregate_review_batches(
            [
                (first, ReviewResult(
                    passed=False, reason="defect", details=["semantic detail"],
                    status=REVIEW_FAIL, risk="LOW", violation_codes=["SEMANTIC_DEFECT"],
                    reviewed_files=list(first.changed_files),
                )),
                (second, ReviewResult(
                    passed=False, reason="parser unavailable", details=["safe diagnostic"],
                    status=REVIEW_ERROR, failure_code=STRUCTURED_OUTPUT_INVALID,
                )),
            ],
            plan,
            ["src/A.java", "src/B.java"],
        )

        self.assertFalse(result.passed)
        self.assertEqual(result.status, REVIEW_ERROR)
        self.assertEqual(result.failure_code, STRUCTURED_OUTPUT_INVALID)
        self.assertEqual(result.violation_codes, ["SEMANTIC_DEFECT"])
        self.assertEqual(result.coverage["semantic_failure_count"], 1)
        self.assertLess(result.coverage["coverage_percent"], 100)

    def test_twelve_batches_plus_one_job_retry_caps_model_calls_at_thirteen(self):
        diff = "".join(
            f"diff --git a/src/F{i}.java b/src/F{i}.java\n"
            f"--- a/src/F{i}.java\n+++ b/src/F{i}.java\n"
            f"@@ -1 +1 @@\n-old\n+{chr(65 + i) * 180}\n"
            for i in range(12)
        )
        files = [f"src/F{i}.java" for i in range(12)]
        worker = SequenceWorker([_wr_ok()])
        collector = FakeCollector(_git(diff=diff, files=files))
        self.manager._get_worker = lambda task: (worker, "droid")
        self.manager.collector = collector
        self.manager.build_test_runner = SequenceRunner([self._build_result("PASS")])
        actual = Reviewer(self._tmp.name)
        self.manager.reviewer = actual
        responses = [(None, None, ["safe"], False, "error", "")]
        responses.extend(("PASS", "ok", [], True, "", "") for _ in range(12))
        task = self._task("TASK-MAX-CALLS")

        with mock.patch.dict(os.environ, {
            "KKM_REVIEW_MAX_BATCH_CHARS": "300", "KKM_REVIEW_MAX_BATCHES": "12"
        }), mock.patch("reviewer.load_review_rules", return_value="rules"), mock.patch.object(
            actual, "_codex_review", side_effect=responses
        ) as model_call:
            result, _ = self._run(task)

        self.assertTrue(result.success)
        self.assertEqual(result.review_result.coverage["batch_count"], 12)
        self.assertEqual(model_call.call_count, 13)
        self.assertEqual(worker.calls, 1)
        self.assertEqual(collector.rollback_calls, 0)

    def test_reviewer_recall_error_preserves_changes_for_manual_review(self):
        worker, collector, _, reviewer = self._configure(
            [self._build_result("PASS")],
            [
                ReviewResult(passed=False, reason="parse one", status=REVIEW_ERROR),
                ReviewResult(passed=False, reason="parse two", status=REVIEW_ERROR),
            ],
        )
        task = self._task()

        result, _ = self._run(task)

        self.assertFalse(result.success)
        self.assertEqual(worker.calls, 1)
        self.assertEqual(reviewer.calls, 2)
        self.assertEqual(collector.rollback_calls, 0)
        self.assertEqual(task.failure_code, REVIEW_OUTPUT_PARSE_FAILED)
        self.assertEqual(task.verification_status, "PARTIAL")
        self.assertEqual(task.status, "AWAITING_QA")
        self.assertEqual(task.stage, "AWAITING_QA")
        self.assertTrue(task.keep_working_tree)

    def test_build_pass_review_unavailable_uses_bounded_manager_retry(self):
        worker, collector, _, reviewer = self._configure(
            [self._build_result("PASS")],
            [
                ReviewResult(passed=False, reason="offline", status=REVIEW_UNAVAILABLE),
                ReviewResult(passed=True, reason="ok", status=REVIEW_PASS),
            ],
        )
        task = self._task()

        result, _ = self._run(task)

        self.assertTrue(result.success)
        self.assertEqual(worker.calls, 1)
        self.assertEqual(reviewer.calls, 2)
        self.assertEqual(collector.rollback_calls, 0)

    def test_test_fail_review_unavailable_keeps_test_primary_and_retries_worker(self):
        failed_test = BuildTestResult(
            success=False,
            build_status="PASS",
            test_status="FAIL",
            test_output="assertion failed",
        )
        worker, collector, _, _ = self._configure(
            [failed_test, self._build_result("PASS")],
            [
                ReviewResult(passed=False, reason="offline", status=REVIEW_UNAVAILABLE),
                ReviewResult(passed=True, reason="ok", status=REVIEW_PASS),
            ],
        )
        task = self._task()

        result, _ = self._run(task)

        self.assertTrue(result.success)
        self.assertEqual(worker.calls, 2)
        self.assertEqual(collector.rollback_calls, 0)
        self.assertEqual(task.attempt_history[0]["failure_code"], "TEST_FAILED")
        self.assertEqual(
            task.attempt_history[0]["secondary_failures"], [REVIEW_UNAVAILABLE]
        )

    def test_semantic_review_fail_keeps_existing_worker_handoff_retry(self):
        worker, collector, _, _ = self._configure(
            [self._build_result("PASS"), self._build_result("PASS")],
            [
                ReviewResult(
                    passed=False,
                    reason="semantic defect",
                    details=["guard missing"],
                    status=REVIEW_FAIL,
                    risk="LOW",
                ),
                ReviewResult(passed=True, reason="fixed", status=REVIEW_PASS),
            ],
        )
        task = self._task()

        result, _ = self._run(task)

        self.assertTrue(result.success)
        self.assertEqual(worker.calls, 2)
        self.assertEqual(collector.rollback_calls, 0)
        self.assertIn("semantic defect", worker.handoffs[1])
        self.assertEqual(task.attempt_history[0]["failure_code"], "REVIEW_FAILED")

    def test_build_skipped_review_error_stays_unverified_without_rollback(self):
        worker, collector, _, reviewer = self._configure(
            [self._build_result("SKIPPED")],
            [ReviewResult(passed=False, reason="parse", status=REVIEW_ERROR)],
        )
        task = self._task()

        result, _ = self._run(task)

        self.assertFalse(result.success)
        self.assertEqual(worker.calls, 1)
        self.assertEqual(reviewer.calls, 1)
        self.assertEqual(collector.rollback_calls, 0)
        self.assertEqual(task.failure_code, "BUILD_NOT_VERIFIED")
        self.assertEqual(task.verification_status, "PARTIAL")
        self.assertEqual(task.status, "AWAITING_QA")
        self.assertEqual(
            task.attempt_history[0]["secondary_failures"], [REVIEW_ERROR]
        )

    def test_build_skipped_review_partial_keeps_build_not_verified_primary(self):
        worker, collector, _, reviewer = self._configure(
            [self._build_result("SKIPPED")],
            [ReviewResult(
                passed=False,
                reason="coverage incomplete",
                status=REVIEW_ERROR,
                failure_code=REVIEW_COVERAGE_PLAN_INVALID,
            )],
        )
        task = self._task("TASK-SKIPPED-PRIMARY-PARTIAL")

        result, _ = self._run(task)

        self.assertFalse(result.success)
        self.assertEqual(worker.calls, 1)
        self.assertEqual(reviewer.calls, 1)
        self.assertEqual(collector.rollback_calls, 0)
        self.assertEqual(task.failure_code, "BUILD_NOT_VERIFIED")
        self.assertEqual(task.verification_status, "PARTIAL")
        self.assertEqual(task.status, "AWAITING_QA")
        self.assertEqual(
            task.attempt_history[0]["secondary_failures"],
            [REVIEW_COVERAGE_PLAN_INVALID],
        )

    def test_final_attempt_review_error_is_not_success_or_worker_retry(self):
        worker, collector, _, reviewer = self._configure(
            [self._build_result("PASS")],
            [
                ReviewResult(passed=False, reason="parse one", status=REVIEW_ERROR),
                ReviewResult(passed=False, reason="parse two", status=REVIEW_ERROR),
            ],
            max_retry=1,
        )
        task = self._task()

        result, _ = self._run(task)

        self.assertFalse(result.success)
        self.assertEqual(worker.calls, 1)
        self.assertEqual(reviewer.calls, 2)
        self.assertEqual(collector.rollback_calls, 0)
        self.assertNotEqual(task.status, "SUCCESS")

    def test_build_failure_rolls_back_only_after_worker_budget_is_exhausted(self):
        worker, collector, _, _ = self._configure(
            [self._build_result("FAIL"), self._build_result("FAIL")],
            [
                ReviewResult(passed=True, reason="ok", status=REVIEW_PASS),
                ReviewResult(passed=True, reason="ok", status=REVIEW_PASS),
            ],
        )
        task = self._task()

        result, _ = self._run(task)

        self.assertFalse(result.success)
        self.assertEqual(worker.calls, 2)
        self.assertEqual(collector.rollback_calls, 1)
        self.assertTrue(task.is_rolled_back)
        self.assertEqual(task.status, "FAILED")
        self.assertEqual(task.retry_scope, "restart")


if __name__ == "__main__":
    unittest.main()
