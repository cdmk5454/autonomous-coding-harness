"""
2단계(Planner/Tester/Reviewer 증거 주입) 스모크 테스트
- planner OFF → tester SKIPPED + 기존 성공 회귀 유지
- planner ON → brief 가 TaskState/Worker 프롬프트에 반영
- tester 결과 스키마(test_status/summary/evidence) 저장
- reviewer 프롬프트에 테스트 증거 포함
- 기본 정책: tester FAIL 이 전체 성공을 깨지 않음
- KKM_TEST_STRICT=1 + tester FAIL → 전체 FAIL
- ANALYSIS → tester 스킵

실행: python -m unittest test_phase2 -v
(외부 프로세스 미호출 — 전부 목 처리)
"""

from __future__ import annotations

import os
import harness_temp as tempfile
import unittest
from pathlib import Path
from unittest import mock

from build_test_runner import BuildTestResult
from git_collector import GitDiffResult
from manager import Manager
from reviewer import Reviewer, ReviewResult, REVIEW_PASS
from task_state import TaskState
from tester import should_run_tester, mark_tester_skipped
from worker import WorkerResult


_ORIGINAL_TG_ENABLED = os.environ.get("KKM_TG_ENABLED")


def setUpModule():
    """Manager 시나리오가 실제 Telegram 으로 푸시하지 않게 격리한다."""
    os.environ["KKM_TG_ENABLED"] = "0"


def tearDownModule():
    if _ORIGINAL_TG_ENABLED is None:
        os.environ.pop("KKM_TG_ENABLED", None)
    else:
        os.environ["KKM_TG_ENABLED"] = _ORIGINAL_TG_ENABLED

from test_gate_scenarios import (
    FakeWorker, FakeCollector, FakeRunner, FakeReviewer,
    _wr_ok, _build,
)

_TEST_ENV_KEYS = ("KKM_TEST_STRICT", "KKM_TEST_FRONTEND", "KKM_TEST_DB")


class Phase2PipelineTest(unittest.TestCase):
    """Manager 파이프라인 통합 (planner/tester 목 처리)."""

    def setUp(self):
        self._env = {k: os.environ.get(k) for k in _TEST_ENV_KEYS}
        for k in _TEST_ENV_KEYS:
            os.environ.pop(k, None)
        self._tmp = tempfile.TemporaryDirectory()
        self._old_cwd = os.getcwd()
        os.chdir(self._tmp.name)
        self.working_dir = self._tmp.name
        self.manager = Manager(
            working_dir=self.working_dir, model="", max_retry=1,
            timeout=5, worker_type="droid", max_workers=1,
        )

    def tearDown(self):
        self.manager.shutdown()
        os.chdir(self._old_cwd)
        self._tmp.cleanup()
        for k, v in self._env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def _run(
        self,
        task_mode: str,
        use_planner: bool = False,
        diff: str = "diff --git a/src/A.vue b/src/A.vue",
        files=None,
        build_status: str = "PASS",
        planner_side_effect=None,
        tester_side_effect=None,
    ):
        task = TaskState(
            task_id="TASK-P2",
            requirement="페이지 수정해",
            working_dir=self.working_dir,
            task_mode=task_mode,
            use_planner=use_planner,
            target_module=["capui"],
        )
        self.manager._get_worker = lambda t: (FakeWorker(_wr_ok()), "droid")
        self.manager.collector = FakeCollector(GitDiffResult(
            success=True,
            changed_files=files if files is not None else (["capui/src/A.vue"] if diff else []),
            diff=diff,
        ))
        self.manager.build_test_runner = FakeRunner(_build(build_status))
        self.manager.reviewer = FakeReviewer(
            ReviewResult(passed=True, reason="ok", status=REVIEW_PASS)
        )

        with mock.patch("manager.run_planner",
                        side_effect=planner_side_effect) as mp, \
             mock.patch("manager.run_tester",
                        side_effect=tester_side_effect) as mt:
            result = self.manager._run_pipeline(task)
        return result, task, mp, mt

    # 1) planner OFF → tester 스킵, 기존 성공 회귀 유지
    def test_planner_off_tester_skipped_success(self):
        result, task, mp, mt = self._run("MODIFICATION")
        self.assertTrue(result.success)
        self.assertEqual(task.test_status, "SKIPPED")
        self.assertIn("스킵", task.test_summary)
        mp.assert_not_called()
        mt.assert_not_called()
        self.assertEqual(task.planner_brief, {})

    # 2) planner ON → brief 생성 호출 + tester_brief 있으면 tester 실행
    def test_planner_on_brief_stored_and_tester_runs(self):
        def fake_planner(
            task, working_dir, profile=None, policy_root=None, manifest_root=None
        ):
            task.planner_brief = {
                "worker_brief": "범위: capui 검색 페이지",
                "tester_brief": "진입: /search, 확인: 그리드 렌더",
                "reviewer_brief": "필수: 파라미터 처리",
            }
            return task.planner_brief

        def fake_tester(task, working_dir, **kwargs):
            task.test_status = "PASS"
            task.test_summary = "UI smoke 통과"
            task.test_evidence = ["페이지 진입 성공", "그리드 10행 렌더"]
            task.test_frontend = "UI 확인됨"
            task.official_tester_invoked = True
            return task

        result, task, mp, mt = self._run(
            "MODIFICATION", use_planner=True,
            planner_side_effect=fake_planner,
            tester_side_effect=fake_tester,
        )
        mp.assert_called_once()
        mt.assert_called_once()
        self.assertTrue(result.success)
        self.assertEqual(task.planner_brief["worker_brief"], "범위: capui 검색 페이지")
        # 3) tester 결과 스키마 저장 확인
        self.assertEqual(task.test_status, "PASS")
        self.assertEqual(task.test_summary, "UI smoke 통과")
        self.assertEqual(len(task.test_evidence), 2)
        self.assertEqual(task.test_frontend, "UI 확인됨")

    # 5) 기본 정책: tester FAIL 이어도 전체 성공 유지 (STRICT off)
    def test_tester_fail_does_not_break_success_by_default(self):
        def fake_planner(
            task, working_dir, profile=None, policy_root=None, manifest_root=None
        ):
            task.planner_brief = {"tester_brief": "진입: /x"}
            return task.planner_brief

        def fake_tester(task, working_dir, **kwargs):
            task.test_status = "FAIL"
            task.test_summary = "그리드 미렌더"
            task.test_evidence = ["목 테스터 결과"]
            task.official_tester_invoked = True
            task.test_evidence = ["페이지 진입 성공", "그리드 0행"]
            return task

        result, task, _, _ = self._run(
            "MODIFICATION", use_planner=True,
            planner_side_effect=fake_planner, tester_side_effect=fake_tester,
        )
        self.assertTrue(result.success)          # 롤백/실패 강제 없음
        self.assertEqual(task.test_status, "FAIL")
        self.assertEqual(task.failure_stage, "")

    # 6) STRICT 모드: tester FAIL → 전체 FAIL 승격
    def test_strict_mode_promotes_tester_fail(self):
        os.environ["KKM_TEST_STRICT"] = "1"

        def fake_planner(
            task, working_dir, profile=None, policy_root=None, manifest_root=None
        ):
            task.planner_brief = {"tester_brief": "진입: /x"}
            return task.planner_brief

        def fake_tester(task, working_dir, **kwargs):
            task.test_status = "FAIL"
            task.test_summary = "그리드 미렌더"
            task.test_evidence = ["목 테스터 결과"]
            task.official_tester_invoked = True
            return task

        result, task, _, _ = self._run(
            "MODIFICATION", use_planner=True,
            planner_side_effect=fake_planner, tester_side_effect=fake_tester,
        )
        self.assertFalse(result.success)
        self.assertEqual(task.failure_stage, "TEST")
        self.assertIn("STRICT", task.failure_reason)

    # 7) ANALYSIS → tester 스킵 (planner ON 이어도)
    def test_analysis_tester_always_skipped(self):
        def fake_planner(
            task, working_dir, profile=None, policy_root=None, manifest_root=None
        ):
            task.planner_brief = {"tester_brief": "진입: /x"}
            return task.planner_brief

        result, task, _, mt = self._run(
            "ANALYSIS", use_planner=True, diff="", files=[],
            build_status="SKIPPED",
            planner_side_effect=fake_planner,
        )
        self.assertTrue(result.success)
        self.assertEqual(task.test_status, "SKIPPED")
        mt.assert_not_called()

    # 플래너 실패(예외) → 작업 계속
    def test_planner_exception_continues_pipeline(self):
        def boom(
            task, working_dir, profile=None, policy_root=None, manifest_root=None
        ):
            raise RuntimeError("planner down")

        result, task, mp, mt = self._run(
            "MODIFICATION", use_planner=True, planner_side_effect=boom,
        )
        self.assertTrue(result.success)          # 플래너 실패가 작업을 막지 않음
        self.assertEqual(task.planner_brief, {})
        self.assertEqual(task.test_status, "SKIPPED")


class WorkerBriefInjectionTest(unittest.TestCase):
    """worker_brief 가 실제 워커 프롬프트에 반영되는지 (DroidWorker, 목 실행)."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.work_dir = self._tmp.name

    def tearDown(self):
        self._tmp.cleanup()

    def _run_worker(self, task: TaskState) -> str:
        """DroidWorker 실행 후 실제 프롬프트 파일 내용 반환 (목 실행기)."""
        import worker as worker_mod
        captured: dict = {}

        def fake_run(cmd, stdin_data=None, cwd=None, cfg=None, requirement=None, task_mode="", display_events=False, env=None):
            # cmd 의 -f 인자에서 프롬프트 파일 경로 추출
            idx = cmd.index("-f")
            with open(cmd[idx + 1], encoding="utf-8") as f:
                captured["prompt"] = f.read()
            return 0, "ok", "", None

        w = worker_mod.DroidWorker(
            timeout=5, model="m",
            timeout_config=worker_mod.TimeoutConfig(),
        )
        with mock.patch.object(worker_mod, "_run_with_activity_timeout",
                               side_effect=fake_run):
            w.execute("요구사항", self.work_dir, task=task)
        return captured["prompt"]

    def test_worker_brief_injected(self):
        task = TaskState(
            task_id="TASK-WB", requirement="페이지 수정해",
            working_dir=self.work_dir, task_mode="MODIFICATION",
            planner_brief={"worker_brief": "완료 기준: 그리드 정렬 동작"},
        )
        prompt = self._run_worker(task)
        self.assertIn("[구현 브리프(플래너)]", prompt)
        self.assertIn("완료 기준: 그리드 정렬 동작", prompt)

    def test_no_brief_no_block(self):
        task = TaskState(
            task_id="TASK-NB", requirement="페이지 수정해",
            working_dir=self.work_dir, task_mode="MODIFICATION",
        )
        prompt = self._run_worker(task)
        self.assertNotIn("[구현 브리프(플래너)]", prompt)


class ReviewerEvidenceTest(unittest.TestCase):
    """reviewer 프롬프트에 테스트 증거/reviewer_brief 포함 여부."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.reviewer = Reviewer(working_dir=self._tmp.name, timeout=5)

    def tearDown(self):
        self._tmp.cleanup()

    def _task(self) -> TaskState:
        return TaskState(
            task_id="TASK-EV", requirement="페이지 수정해",
            working_dir=self._tmp.name, task_mode="MODIFICATION",
            planner_brief={"reviewer_brief": "필수: 파라미터 null 처리 확인"},
            test_status="FAIL",
            test_summary="그리드 미렌더",
            test_evidence=["진입 성공", "그리드 0행"],
            test_frontend="",
            test_db="",
        )

    def test_build_tester_context_contents(self):
        from reviewer import build_tester_context
        ctx = build_tester_context(self._task())
        self.assertIn("[리뷰 포인트(플래너)]", ctx)
        self.assertIn("파라미터 null 처리", ctx)
        self.assertIn("[테스트 증거(Tester)]", ctx)
        self.assertIn("status: FAIL", ctx)
        self.assertIn("그리드 0행", ctx)
        self.assertIn("SKIPPED/FAIL/ERROR", ctx)  # 활용 지침

    def test_context_empty_when_no_evidence(self):
        from reviewer import build_tester_context
        task = TaskState(task_id="T", requirement="x", working_dir=".")
        context = build_tester_context(task)
        self.assertIn("status: SKIPPED", context)
        self.assertIn("required: False", context)
        self.assertEqual(build_tester_context(None), "")

    def test_context_contains_job_artifacts(self):
        from reviewer import build_tester_context
        task = TaskState(task_id="T", requirement="x", working_dir=".")
        task.artifact_files = ["D:/kkm_test/doc/20260820/151504/VERIFY_151504.md"]
        ctx = build_tester_context(task)
        self.assertIn("[Job 산출물]", ctx)
        self.assertIn("VERIFY_151504.md", ctx)

    def test_review_prompt_contains_evidence(self):
        from reviewer import build_tester_context
        task = self._task()
        ctx = build_tester_context(task)
        prompt = self.reviewer._build_review_prompt(
            "요구사항", ["src/A.vue"], "diff --git a/src/A.vue b/src/A.vue", ctx
        )
        self.assertIn("[테스트 증거(Tester)]", prompt)
        self.assertIn("그리드 미렌더", prompt)
        self.assertIn("[리뷰 포인트(플래너)]", prompt)

    def test_engine_receives_tester_context(self):
        """review() → 엔진 호출로 tester_context 가 전달되는지 (경로 검증)."""
        task = self._task()
        task.git_diff = "diff --git a/src/A.vue b/src/A.vue"
        captured: dict = {}
        with mock.patch.object(
            Reviewer, "_codex_review",
            side_effect=lambda **kw: (
                captured.update(kw) or ("PASS", "ok", [], True, "", "")
            ),
        ):
            self.reviewer.review(
                requirement=task.requirement, worker_success=True,
                exit_code=0, changed_files=["src/A.vue"],
                worker_stdout="", git_diff=task.git_diff, task=task,
            )
        self.assertIn("tester_context", captured)
        self.assertIn("그리드 미렌더", captured["tester_context"])
        self.assertEqual(task.review_status, REVIEW_PASS)


class TesterUnitTest(unittest.TestCase):
    """should_run_tester 조건 + tester 파서 단위."""

    def setUp(self):
        self._env = {k: os.environ.get(k) for k in _TEST_ENV_KEYS}
        for k in _TEST_ENV_KEYS:
            os.environ.pop(k, None)

    def tearDown(self):
        for k, v in self._env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def _task(self, mode="MODIFICATION", files=None, brief=None):
        return TaskState(
            task_id="TASK-TU", requirement="페이지 수정해", working_dir=".",
            task_mode=mode, changed_files=files or [],
            planner_brief=brief or {},
        )

    def test_conditions(self):
        # ANALYSIS → 스킵
        ok, why = should_run_tester(self._task("ANALYSIS", ["a.vue"]))
        self.assertFalse(ok)
        # MODIFICATION + 파일 + brief → 실행
        ok, _ = should_run_tester(self._task(files=["a.vue"],
                                             brief={"tester_brief": "x"}))
        self.assertTrue(ok)
        # MODIFICATION + 파일 + 플래그 → 실행
        os.environ["KKM_TEST_FRONTEND"] = "1"
        ok, _ = should_run_tester(self._task(files=["a.vue"]))
        self.assertTrue(ok)
        del os.environ["KKM_TEST_FRONTEND"]
        # brief/플래그 없음 → 스킵
        ok, why = should_run_tester(self._task(files=["a.vue"]))
        self.assertFalse(ok)
        self.assertIn("스킵", why)
        # 변경 파일 없음 → 스킵
        ok, _ = should_run_tester(self._task(brief={"tester_brief": "x"}))
        self.assertFalse(ok)

    def test_mark_skipped_schema(self):
        from tester import TesterResult
        task = self._task()
        mark_tester_skipped(task, "tester_brief 없음 → 스킵")
        self.assertEqual(task.test_status, "SKIPPED")
        self.assertEqual(task.test_summary, "tester_brief 없음 → 스킵")
        self.assertEqual(task.test_evidence, [])

    def test_parse_tester_output(self):
        from tester import _parse_tester_output
        out = (
            "TEST_RESULT: FAIL\n"
            "SUMMARY: 그리드 미렌더\n"
            "STEPS:\n"
            "- 페이지 /search 진입 성공\n"
            "- grid rows=0 (기대 10)\n"
            "- SELECT COUNT 확인: 10건\n"
        )
        tr = _parse_tester_output(out)
        self.assertEqual(tr.status, "FAIL")
        self.assertEqual(tr.summary, "그리드 미렌더")
        self.assertEqual(len(tr.evidence), 3)
        self.assertEqual(tr.frontend, "UI 확인됨")
        self.assertEqual(tr.db, "SELECT 확인됨")

        # 파싱 실패 → ERROR
        tr2 = _parse_tester_output("이상한 출력")
        self.assertEqual(tr2.status, "ERROR")


class TaskStatePhase2CompatTest(unittest.TestCase):
    """구버전 JSON → 2단계 필드 기본값 보정."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self._old_cwd = os.getcwd()
        os.chdir(self._tmp.name)

    def tearDown(self):
        os.chdir(self._old_cwd)
        self._tmp.cleanup()

    def test_load_old_json(self):
        base = Path(".tasks")
        base.mkdir(exist_ok=True)
        old = {"task_id": "TASK-OLD2", "requirement": "x"}
        (base / "TASK-OLD2.json").write_text(
            __import__("json").dumps(old, ensure_ascii=False), encoding="utf-8"
        )
        task = TaskState.load("TASK-OLD2")
        self.assertFalse(task.use_planner)
        self.assertEqual(task.planner_brief, {})
        self.assertEqual(task.test_status, "SKIPPED")
        self.assertEqual(task.test_evidence, [])


if __name__ == "__main__":
    unittest.main()
