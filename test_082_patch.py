from __future__ import annotations

import json
import os
import subprocess
import harness_temp as tempfile
import unittest
from pathlib import Path
from unittest import mock

import reviewer
from job_queue import BATCH_FINAL_COMMIT, FAILED_FINAL, JobStore
from manager import Manager
from reviewer import (
    REVIEW_ERROR,
    REVIEW_EXECUTION_UNAVAILABLE,
    REVIEW_FAIL,
    REVIEW_OUTPUT_PARSE_FAILED,
    REVIEW_PASS,
    REVIEW_SCHEMA_INVALID,
    REVIEW_UNAVAILABLE,
    ReviewResult,
    ReviewSchemaValidationError,
    Reviewer,
    review_output_schema,
    validate_review_output_schema,
)
from test_081_patch import (
    PROFILE,
    SP0021,
    U02,
    _Collector,
    _Runner,
    _Worker,
    _diff,
)
from test_batch_final_commit import _active_fields, _context, _request
from task_state import TaskState


A06 = "suc/src/main/java/suc/cs/query/SsucCs0020Qry.xml"


class _SequenceReviewer:
    def __init__(self, results: list[ReviewResult]):
        self.results = list(results)
        self.calls = 0
        self.kwargs: list[dict] = []

    def review(self, task=None, **kwargs):
        self.kwargs.append(dict(kwargs))
        result = self.results[min(self.calls, len(self.results) - 1)]
        self.calls += 1
        return Reviewer._apply_to_state(task, result)

    @staticmethod
    def plan_diff_batches(git_diff: str):
        return reviewer.plan_diff_batches(git_diff)


class Patch082SchemaTest(unittest.TestCase):
    def test_all_schema_objects_are_strict_and_required_matches_properties(self):
        schema = validate_review_output_schema(review_output_schema())

        def assert_strict(node):
            if not isinstance(node, dict):
                return
            raw_type = node.get("type")
            types = raw_type if isinstance(raw_type, list) else [raw_type]
            if "object" in types:
                self.assertFalse(node["additionalProperties"])
                self.assertEqual(set(node["properties"]), set(node["required"]))
                for child in node["properties"].values():
                    assert_strict(child)
            if "array" in types:
                assert_strict(node["items"])

        assert_strict(schema)
        self.assertEqual(
            ["array", "null"],
            schema["properties"]["reviewed_task_files"]["type"],
        )

    def test_nested_objects_and_array_items_are_validated_recursively(self):
        nested = {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "meta": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {"name": {"type": "string"}},
                    "required": ["name"],
                },
                "rows": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {"value": {"type": "string"}},
                        "required": ["value"],
                    },
                },
            },
            "required": ["meta", "rows"],
        }
        self.assertIs(nested, validate_review_output_schema(nested))
        nested["properties"]["rows"]["items"]["required"] = []
        with self.assertRaises(ReviewSchemaValidationError):
            validate_review_output_schema(nested)

    def test_nullable_optional_fields_serialize_and_parse(self):
        payload = {
            "result": "PASS",
            "reason": "ok",
            "details": [],
            "risk": "NONE",
            "violation_codes": [],
            "reviewed_files": ["suc/src/A.java"],
            "reviewed_task_files": None,
            "inspected_unchanged_target_files": ["suc/src/B.java"],
        }
        parsed = Reviewer._parse_structured_review_output(json.dumps(payload))
        self.assertIsNotNone(parsed)
        payload["reviewed_task_files"] = "invalid"
        self.assertIsNone(Reviewer._parse_structured_review_output(json.dumps(payload)))

    def test_invalid_schema_is_blocked_before_codex_execution(self):
        invalid = review_output_schema()
        invalid["required"] = ["result"]
        with tempfile.TemporaryDirectory() as temp, mock.patch.object(
            reviewer, "_REVIEW_SCHEMA", invalid
        ), mock.patch("reviewer.subprocess.run") as codex:
            result = Reviewer(temp)._codex_review("review", ["src/A.java"], "diff")
        self.assertFalse(result[3])
        self.assertEqual("schema_invalid", result[4])
        codex.assert_not_called()

    def test_real_codex_cli_accepts_minimal_structured_review_schema(self):
        """Exercise the production CLI adapter with a deterministic local process."""
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            observed: dict[str, object] = {}

            def fake_codex(command, **kwargs):
                observed["command"] = list(command)
                observed["stdin"] = kwargs.get("input", "")
                schema_path = Path(command[command.index("--output-schema") + 1])
                output_path = Path(command[command.index("-o") + 1])
                schema = json.loads(schema_path.read_text(encoding="utf-8"))
                validate_review_output_schema(schema)
                output_path.write_text(
                    json.dumps({
                        "result": "PASS",
                        "reason": "ok",
                        "risk": "NONE",
                        "details": [],
                        "violation_codes": [],
                        "reviewed_files": ["src/A.java"],
                        "reviewed_task_files": ["src/A.java"],
                        "inspected_unchanged_target_files": [],
                    }),
                    encoding="utf-8",
                )
                return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

            actual = Reviewer(root)
            with mock.patch("reviewer.trusted_executable", return_value="codex"), mock.patch(
                "reviewer.subprocess.run", side_effect=fake_codex
            ):
                result = actual._codex_review(
                    "review the change", ["src/A.java"],
                    "diff --git a/src/A.java b/src/A.java"
                )

            self.assertEqual("PASS", result[0])
            self.assertTrue(result[3])
            command = observed["command"]
            self.assertIn("--output-schema", command)
            self.assertIn("read-only", command)
            self.assertEqual("-", command[-1])
            self.assertIn("review the change", observed["stdin"])


class Patch082RecoveryTest(unittest.TestCase):
    @staticmethod
    def _task(root: Path) -> TaskState:
        return TaskState(
            task_id="TASK-082-TEST",
            requirement="SP0021 application source 수정",
            task_mode="MODIFICATION",
            worker="codex",
            codex_model="gpt-5.6-sol",
            working_dir=str(root),
            target_module=["suc"],
            target_resources=["suc/src/main/java/suc/sp"],
            inherited_batch_delta_files=[U02, A06],
            pre_job_snapshot_id="SNAP-082",
        )

    @staticmethod
    def _manager(root: Path, reviews: list[ReviewResult]):
        worker = _Worker()
        collector = _Collector([SP0021], _diff())
        review_engine = _SequenceReviewer(reviews)
        manager = Manager(
            str(root), "", max_retry=2, worker_type="codex",
            codex_model="gpt-5.6-sol", profile=PROFILE,
            task_root=root / "tasks",
        )
        manager.collector = collector
        manager.build_test_runner = _Runner()
        manager.reviewer = review_engine
        manager._get_worker = lambda task: (worker, "codex")
        return manager, worker, collector, review_engine

    def test_reviewer_only_retry_preserves_delta_and_does_not_retry_worker(self):
        unavailable = ReviewResult(
            False, "transport unavailable", status=REVIEW_UNAVAILABLE,
            failure_code=REVIEW_EXECUTION_UNAVAILABLE,
        )
        passed = ReviewResult(
            True, "ok", status=REVIEW_PASS, reviewed_files=[SP0021]
        )
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            manager, worker, collector, engine = self._manager(root, [unavailable, passed])
            task = self._task(root)
            result = manager.run_with_retry(task)
            manager.shutdown()
        self.assertTrue(result.success)
        self.assertEqual(1, worker.calls)
        self.assertEqual(2, engine.calls)
        self.assertEqual(1, task.reviewer_recovery_count)
        self.assertEqual(0, task.retry_count)
        self.assertEqual(0, collector.rollbacks)
        self.assertEqual([SP0021], engine.kwargs[0]["changed_files"])
        self.assertNotIn(U02, engine.kwargs[0]["changed_files"])
        self.assertEqual([U02, A06], task.inherited_batch_delta_files)

    def test_schema_invalid_never_retries_worker_or_reviewer(self):
        invalid = ReviewResult(
            False, "invalid schema", status=REVIEW_ERROR,
            failure_code=REVIEW_SCHEMA_INVALID,
        )
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            manager, worker, collector, engine = self._manager(root, [invalid])
            task = self._task(root)
            result = manager.run_with_retry(task)
            manager.shutdown()
        self.assertFalse(result.success)
        self.assertEqual(1, worker.calls)
        self.assertEqual(1, engine.calls)
        self.assertEqual(0, task.reviewer_recovery_count)
        self.assertEqual(0, task.retry_count)
        self.assertEqual("", task.failure_fingerprint)
        self.assertEqual(0, collector.rollbacks)
        self.assertEqual("AWAITING_QA", task.status)
        self.assertEqual("PARTIAL", task.verification_status)
        self.assertTrue(task.keep_working_tree)
        self.assertEqual("PRESERVED", task.worktree_disposition)
        self.assertEqual([U02, A06], task.inherited_batch_delta_files)

    def test_reviewer_recovery_exhaustion_scoped_rolls_back_once(self):
        unavailable = ReviewResult(
            False, "offline", status=REVIEW_UNAVAILABLE,
            failure_code=REVIEW_EXECUTION_UNAVAILABLE,
        )
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            manager, worker, collector, engine = self._manager(
                root, [unavailable, unavailable]
            )
            task = self._task(root)
            result = manager.run_with_retry(task)
            manager.shutdown()
        self.assertFalse(result.success)
        self.assertEqual(1, worker.calls)
        self.assertEqual(2, engine.calls)
        self.assertEqual(1, task.reviewer_recovery_count)
        self.assertEqual(0, task.retry_count)
        self.assertEqual("infrastructure", task.failure_type)
        self.assertEqual("NON_RETRYABLE", task.fix_scope)
        self.assertEqual("REVIEWER_RECOVERY_EXHAUSTED", task.user_intervention_reason)
        self.assertEqual(0, collector.rollbacks)
        self.assertEqual("AWAITING_QA", task.status)
        self.assertEqual("PARTIAL", task.verification_status)
        self.assertTrue(task.keep_working_tree)
        self.assertEqual("PRESERVED", task.worktree_disposition)
        self.assertEqual([U02, A06], task.inherited_batch_delta_files)

    def test_review_fail_is_not_review_infrastructure(self):
        semantic = ReviewResult(
            False, "real defect", status=REVIEW_FAIL, risk="LOW"
        )
        infrastructure = ReviewResult(
            False, "parse", status=REVIEW_ERROR,
            failure_code=REVIEW_OUTPUT_PARSE_FAILED,
        )
        self.assertEqual(REVIEW_FAIL, semantic.status)
        self.assertEqual(REVIEW_ERROR, infrastructure.status)
        self.assertNotEqual(semantic.failure_code, infrastructure.failure_code)

    def test_multi_batch_infrastructure_code_never_becomes_coverage_invalid(self):
        plan = reviewer.plan_diff_batches(_diff())
        first = plan.batches[0]
        aggregated = Manager._aggregate_review_batches(
            [(first, ReviewResult(
                False,
                "transport unavailable",
                status=REVIEW_UNAVAILABLE,
                failure_code=REVIEW_EXECUTION_UNAVAILABLE,
            ))],
            plan,
            list(first.changed_files),
        )
        self.assertEqual(REVIEW_UNAVAILABLE, aggregated.status)
        self.assertEqual(REVIEW_EXECUTION_UNAVAILABLE, aggregated.failure_code)


class Patch082QueueTest(unittest.TestCase):
    def test_new_batch_head_clears_safe_failed_final_gate_without_generation_reset(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / ".control"
            root.mkdir()
            store = JobStore(root)
            fields = _active_fields()
            self.assertEqual(BATCH_FINAL_COMMIT, fields["commit_policy"])
            store.activate_cumulative_policy(1, fields)
            old, _ = store.enqueue_batch(
                "old-082", [_request("old-u03")], execution_context=_context()
            )
            with store._mutex.hold():
                old_job = store._read_job(old[0]["job_id"])
                old_job["status"] = FAILED_FINAL
                old_job["last_result"] = {
                    "worktree_disposition": "CLEAN_ROLLBACK",
                    "failure_type": "infrastructure",
                }
                store._write_job(old_job)
                queue = store._read_queue()
                store._set_gate(queue, old_job["job_id"], FAILED_FINAL)
                store._write_queue(queue)
            new, _ = store.enqueue_batch(
                "new-082", [_request("new-u03")], execution_context=_context()
            )
            self.assertEqual(1, new[0]["sequence"])
            self.assertTrue(store.clear_stale_terminal_predecessor_gate_for_batch_head())
            snapshot = store.queue_snapshot()
            self.assertEqual(1, snapshot["generation"])
            self.assertEqual("", snapshot["blocked_by_job_id"])
            self.assertEqual(1, snapshot["total"])
            self.assertEqual({"QUEUED": 1}, snapshot["counts"])
            claimed = store.claim_next("owner-082")
            self.assertEqual(new[0]["job_id"], claimed["job_id"])


if __name__ == "__main__":
    unittest.main()
