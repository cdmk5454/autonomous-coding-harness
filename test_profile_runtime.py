from __future__ import annotations

import ast
import io
import json
import os
import re
import subprocess
import sys
import harness_temp as tempfile
import unittest
from dataclasses import replace
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

import run
import planner
import reviewer
import worker
from project_profile import ModuleProfile, load_project_profile
from git_collector import GitCollector
from git_collector import _filter_diff_body
from build_test_runner import BuildTestRunner
from manager import Manager, ManagerResult
from reporter import Reporter
from reviewer import ReviewResult, Reviewer
from runtime_safety import isolated_subprocess_env, safe_print
from task_state import TaskState


PROFILE = load_project_profile(
    Path(r"D:\agents\profiles\e4u"), Path(r"D:\inha_dev")
)


class DeterministicReviewTest(unittest.TestCase):
    def test_negative_natural_language_is_not_violation_evidence(self):
        requirement = "비밀 노출 없음, core 수정 없음, 위험한 SQL 없음, 외부 API 추가 없음"
        self.assertIsNone(reviewer.is_hard_constraint_violation([requirement]))
        self.assertEqual(reviewer.normalize_review_risk("LOW", True, [requirement]), "LOW")

    def test_protected_and_out_of_scope_paths_are_detected(self):
        codes, _ = reviewer.deterministic_violations(
            ["core/src/main/java/X.java", "suc/src/main/java/Y.java"],
            "",
            PROFILE,
            ["cap"],
        )
        self.assertIn("PROTECTED_PATH_CHANGED", codes)
        self.assertIn("OUT_OF_SCOPE_CHANGE", codes)

    def test_only_added_lines_create_secret_or_sql_violations(self):
        deleted = (
            "diff --git a/cap/a.sql b/cap/a.sql\n"
            "@@ -1,2 +1,2 @@\n"
            "-password='abcdefgh'\n"
            " DROP TABLE OLD_TABLE\n"
            "+safe = value\n"
        )
        codes, _ = reviewer.deterministic_violations(
            ["cap/a.sql"], deleted, PROFILE, ["cap"]
        )
        self.assertNotIn("SECRET_ADDED", codes)
        self.assertNotIn("DANGEROUS_SQL_ADDED", codes)

        added = deleted + "+password='abcdefgh'\n+TRUNCATE TABLE LIVE_TABLE\n"
        codes, _ = reviewer.deterministic_violations(
            ["cap/a.sql"], added, PROFILE, ["cap"]
        )
        self.assertIn("SECRET_ADDED", codes)
        self.assertIn("DANGEROUS_SQL_ADDED", codes)

    def test_external_api_requires_active_endpoint_evidence(self):
        comment = "+// api docs https://example.com/reference\n"
        codes, _ = reviewer.deterministic_violations(
            ["cap/src/A.java"], comment, PROFILE, ["cap"]
        )
        self.assertNotIn("EXTERNAL_API_ADDED", codes)
        actual = '+const apiUrl = "https://example.com/v1";\n'
        codes, _ = reviewer.deterministic_violations(
            ["capui/src/A.ts"], actual, PROFILE, ["capui"]
        )
        self.assertIn("EXTERNAL_API_ADDED", codes)

    def test_deterministic_violation_overrides_semantic_pass(self):
        task = TaskState(
            task_id="TASK-POLICY", requirement="수정", task_mode="MODIFICATION",
            working_dir=r"D:\inha_dev", target_module=["cap"],
        )
        instance = Reviewer(r"D:\inha_dev", profile=PROFILE)
        with mock.patch.object(instance, "_run_semantic_review") as semantic:
            result = instance.review(
                task.requirement, True, 0, ["core/src/X.java"],
                git_diff="diff --git a/src/X.java b/src/X.java\n+value\n", task=task,
            )
        self.assertFalse(result.passed)
        self.assertIn("PROTECTED_PATH_CHANGED", result.violation_codes)
        semantic.assert_not_called()

    def test_semantic_violation_code_is_not_promoted_to_policy_evidence(self):
        task = TaskState(task_id="TASK-SEMANTIC", requirement="x")
        result = ReviewResult(
            passed=False,
            reason="model finding",
            status=reviewer.REVIEW_FAIL,
            risk="LOW",
            violation_codes=["SECRET_ADDED"],
        )
        Reviewer._apply_to_state(task, result)
        self.assertEqual(task.failure_type, "review")
        self.assertEqual(task.fix_scope, "RETRYABLE")


class DiffCoverageTest(unittest.TestCase):
    @staticmethod
    def _file_diff(name: str, fill: str) -> str:
        return (
            f"diff --git a/{name} b/{name}\n"
            "--- a/file\n+++ b/file\n@@ -1 +1 @@\n-old\n+"
            + fill
            + "\n"
        )

    def test_direct_reviewer_rejects_multi_batch_diff_before_model_call(self):
        diff = "--- [module: cap] ---\n" + self._file_diff("A.java", "a" * 7000)
        diff += self._file_diff("B.java", "b" * 7000)
        batches = reviewer.split_diff_batches(diff)
        self.assertEqual(len(batches), 2)
        self.assertEqual(reviewer.diff_files(batches[0]), ["cap/A.java"])
        self.assertEqual(reviewer.diff_files(batches[1]), ["cap/B.java"])

        instance = Reviewer(r"D:\inha_dev", profile=PROFILE)
        with mock.patch.object(
            instance, "_codex_review",
            return_value=("PASS", "ok", [], True, "", ""),
        ) as codex_review:
            result = instance._run_semantic_review(
                "수정", ["cap/A.java", "cap/B.java"], diff, "", None,
                1, 1, [], "batch",
            )
        self.assertFalse(result.passed)
        self.assertEqual(result.status, reviewer.REVIEW_ERROR)
        self.assertEqual(result.failure_code, reviewer.REVIEW_COVERAGE_PLAN_INVALID)
        codex_review.assert_not_called()
        self.assertEqual(result.coverage["missing_files"], ["cap/A.java", "cap/B.java"])
        self.assertEqual(result.coverage["batch_count"], 2)

    @staticmethod
    def _oversized_real_shape() -> str:
        body = []
        body.extend(" " + (f"context-{i:03d}-" + "c" * 32) + "\n" for i in range(37))
        body.extend("-" + (f"removed-{i:03d}-" + "r" * 32) + "\n" for i in range(130))
        body.extend("+" + (f"added-{i:03d}-" + "a" * 34) + "\n" for i in range(120))
        return (
            "--- [module: suc] ---\n"
            "diff --git a/src/main/java/suc/sp/query/SsucSp0023Qry.xml "
            "b/src/main/java/suc/sp/query/SsucSp0023Qry.xml\n"
            "--- a/src/main/java/suc/sp/query/SsucSp0023Qry.xml\n"
            "+++ b/src/main/java/suc/sp/query/SsucSp0023Qry.xml\n"
            "@@ -37,167 +37,157 @@\n" + "".join(body)
        )

    def test_oversized_single_hunk_is_losslessly_split(self):
        diff = self._oversized_real_shape()
        self.assertEqual(len(diff.splitlines()) - 5, 287)
        self.assertGreater(len(diff), 12000)

        plan = reviewer.plan_diff_batches(diff, max_chars=12000, max_batches=12)

        self.assertTrue(plan.valid, plan.reason)
        self.assertGreaterEqual(len(plan.batches), 2)
        self.assertTrue(all(len(batch.text) <= 12000 for batch in plan.batches))
        self.assertEqual(plan.missing_atoms, ())
        self.assertEqual(plan.duplicate_atoms, ())
        self.assertEqual(len(plan.assigned_atoms), len(plan.expected_atoms))
        self.assertEqual(len(plan.expected_atoms), 250)
        chunks = [chunk for batch in plan.batches for chunk in batch.chunks]
        self.assertTrue(all(chunk.original_hunk_header == "@@ -37,167 +37,157 @@" for chunk in chunks))
        self.assertEqual([chunk.chunk_ordinal for chunk in chunks], list(range(1, len(chunks) + 1)))
        self.assertTrue(all(re.match(r"^@@ -\d+,\d+ \+\d+,\d+ @@", chunk.hunk_header) for chunk in chunks))
        self.assertEqual(
            [atom for chunk in chunks for atom in chunk.changed_atoms],
            list(plan.expected_atoms),
        )
        rendered = "".join(chunk.text for chunk in chunks)
        for index in range(130):
            self.assertEqual(rendered.count(f"-removed-{index:03d}-"), 1)
        for index in range(120):
            self.assertEqual(rendered.count(f"+added-{index:03d}-"), 1)

    def test_single_diff_line_over_limit_is_manual_qa_capacity(self):
        plan = reviewer.plan_diff_batches(
            self._file_diff("Huge.java", "x" * 13000),
            max_chars=12000,
            max_batches=12,
        )
        self.assertFalse(plan.valid)
        self.assertEqual(plan.failure_code, reviewer.DIFF_TOO_LARGE_FOR_AUTOMATED_REVIEW)
        self.assertEqual(plan.batches, ())

    def test_binary_and_malformed_diff_fail_without_batches(self):
        binary = (
            "diff --git a/image.png b/image.png\n"
            "index 1234567..abcdef0 100644\nBinary files a/image.png and b/image.png differ\n"
        )
        plan = reviewer.plan_diff_batches(binary)
        self.assertEqual(plan.failure_code, reviewer.BINARY_DIFF_REQUIRES_MANUAL_REVIEW)
        self.assertEqual(plan.batches, ())

        malformed = "diff --git a/A.java b/A.java\n--- a/A.java\n+++ b/A.java\n+value\n"
        plan = reviewer.plan_diff_batches(malformed)
        self.assertEqual(plan.failure_code, reviewer.REVIEW_COVERAGE_PLAN_INVALID)
        self.assertEqual(plan.batches, ())

    def test_new_file_and_rename_metadata_are_preserved(self):
        new_file = (
            "--- [module: cap] ---\n"
            "diff --git a/new.txt b/new.txt\nnew file mode 100644\n"
            "--- /dev/null\n+++ b/new.txt\n@@ -0,0 +1,2 @@\n+one\n+two\n"
        )
        rename = (
            "--- [module: cap] ---\n"
            "diff --git a/old.txt b/newer.txt\nsimilarity index 100%\n"
            "rename from old.txt\nrename to newer.txt\n"
        )
        plan = reviewer.plan_diff_batches(new_file + rename)
        self.assertTrue(plan.valid, plan.reason)
        chunks = [chunk for batch in plan.batches for chunk in batch.chunks]
        self.assertTrue(chunks[0].new_file)
        self.assertEqual(chunks[0].change_kind, "new")
        self.assertEqual(len(chunks[0].changed_atoms), 2)
        self.assertEqual(chunks[-1].rename_from, "old.txt")
        self.assertEqual(chunks[-1].rename_to, "newer.txt")
        self.assertEqual(chunks[-1].change_kind, "rename")

    def test_batch_limit_12_accepts_and_13_rejects(self):
        diff = "--- [module: cap] ---\n" + "".join(
            self._file_diff(f"F{i}.java", chr(65 + i) * 220) for i in range(13)
        )
        accepted = reviewer.plan_diff_batches(diff, max_chars=350, max_batches=13)
        rejected = reviewer.plan_diff_batches(diff, max_chars=350, max_batches=12)
        self.assertTrue(accepted.valid, accepted.reason)
        self.assertEqual(len(accepted.batches), 13)
        self.assertEqual(rejected.failure_code, reviewer.DIFF_TOO_LARGE_FOR_AUTOMATED_REVIEW)
        self.assertEqual(rejected.batches, ())

    def test_eol_whitespace_and_full_rewrite_flags(self):
        eol = (
            "diff --git a/eol.txt b/eol.txt\n--- a/eol.txt\n+++ b/eol.txt\n"
            "@@ -1 +1 @@\n-same\r\n+same\n"
        )
        whitespace = (
            "diff --git a/ws.txt b/ws.txt\n--- a/ws.txt\n+++ b/ws.txt\n"
            "@@ -1 +1 @@\n-a b\n+ab\n"
        )
        deleted = (
            "diff --git a/gone.txt b/gone.txt\ndeleted file mode 100644\n"
            "--- a/gone.txt\n+++ /dev/null\n@@ -1,2 +0,0 @@\n-one\n-two\n"
        )
        plan = reviewer.plan_diff_batches(eol + whitespace + deleted)
        self.assertTrue(plan.valid, plan.reason)
        chunks = [chunk for batch in plan.batches for chunk in batch.chunks]
        self.assertTrue(chunks[0].eol_only)
        self.assertTrue(chunks[1].whitespace_only)
        self.assertTrue(chunks[1].full_rewrite)
        self.assertTrue(chunks[2].deleted_file)
        self.assertEqual(chunks[2].change_kind, "deleted")
        self.assertEqual(len(chunks[2].changed_atoms), 2)

    def test_invalid_environment_limits_fail_closed(self):
        diff = self._file_diff("A.java", "new")
        for name, value in (
            ("KKM_REVIEW_MAX_BATCH_CHARS", "invalid"),
            ("KKM_REVIEW_MAX_BATCHES", "0"),
        ):
            with self.subTest(name=name), mock.patch.dict(os.environ, {name: value}, clear=False):
                plan = reviewer.plan_diff_batches(diff)
                self.assertFalse(plan.valid)
                self.assertEqual(plan.failure_code, reviewer.REVIEW_COVERAGE_PLAN_INVALID)
                self.assertEqual(plan.batches, ())

    def test_114015_scale_plan_stays_under_twelve_batches(self):
        sections = []
        for index in range(10):
            body = "".join(f"+new-{index}-{line}-{'x' * 48}\n" for line in range(80))
            sections.append(
                f"diff --git a/new{index}.txt b/new{index}.txt\nnew file mode 100644\n"
                f"--- /dev/null\n+++ b/new{index}.txt\n@@ -0,0 +1,80 @@\n{body}"
            )
        for index in range(6):
            body = "".join(
                f"-old-{index}-{line}-{'o' * 42}\n+new-{index}-{line}-{'n' * 42}\n"
                for line in range(40)
            )
            sections.append(
                f"diff --git a/M{index}.java b/M{index}.java\n"
                f"--- a/M{index}.java\n+++ b/M{index}.java\n@@ -1,40 +1,40 @@\n{body}"
            )
        sections.append(
            "diff --git a/old-name.txt b/new-name.txt\nsimilarity index 100%\n"
            "rename from old-name.txt\nrename to new-name.txt\n"
        )
        diff = "--- [module: cap] ---\n" + "".join(sections)
        self.assertEqual(diff.count("diff --git "), 17)
        self.assertGreater(len(diff), 70000)
        plan = reviewer.plan_diff_batches(diff)
        self.assertTrue(plan.valid, plan.reason)
        self.assertLessEqual(len(plan.batches), 12)
        self.assertTrue(all(batch.text != diff and len(batch.text) <= 12000 for batch in plan.batches))
        self.assertEqual(plan.missing_atoms, ())
        self.assertEqual(plan.duplicate_atoms, ())


class StructuredReviewTest(unittest.TestCase):
    @staticmethod
    def _payload(**updates):
        payload = {
            "result": "FAIL", "reason": "safe", "details": [], "risk": "LOW",
            "violation_codes": ["VALIDATION_INCOMPLETE"],
            "reviewed_files": ["cap/A.java"],
            "reviewed_task_files": None,
            "inspected_unchanged_target_files": None,
        }
        payload.update(updates)
        return payload

    def test_structured_output_validates_enums_and_required_fields(self):
        valid = json.dumps({
            "result": "PASS", "reason": "ok", "details": [], "risk": "NONE",
            "violation_codes": [], "reviewed_files": ["cap/A.java"],
            "reviewed_task_files": None,
            "inspected_unchanged_target_files": None,
        })
        self.assertIsNotNone(Reviewer._parse_structured_review_output(valid))
        invalid = json.dumps({
            "result": "MAYBE", "reason": "ok", "details": [], "risk": "NONE",
            "violation_codes": [], "reviewed_files": [],
            "reviewed_task_files": None,
            "inspected_unchanged_target_files": None,
        })
        self.assertIsNone(Reviewer._parse_structured_review_output(invalid))
        self.assertIsNone(Reviewer._parse_structured_review_output('{"result":"PASS"}'))

    def test_codex_transport_schema_uses_compatibility_baseline_only(self):
        schema = reviewer.review_output_schema()
        serialized = json.dumps(schema, sort_keys=True)
        for locally_enforced in (
            "minLength", "maxLength", "uniqueItems", "minItems", "maxItems"
        ):
            with self.subTest(keyword=locally_enforced):
                self.assertNotIn(f'"{locally_enforced}"', serialized)
        self.assertFalse(schema["additionalProperties"])
        self.assertEqual(
            schema["properties"]["violation_codes"]["items"], {"type": "string"}
        )

    def test_real_semantic_code_fixtures_follow_shared_pattern_contract(self):
        fixtures = [
            ["CORRECTNESS_CAP_COURSE_MAPPING", "PROFILE_SCREEN_ID_UNVERIFIED",
             "INSUFFICIENT_DB_VERIFICATION", "VERIFICATION_EVIDENCE_MISMATCH"],
            ["DATA_INTEGRITY_PARTIAL_BATCH", "VALIDATION_INCOMPLETE",
             "PROFILE_SCREEN_ID_UNVERIFIED"],
        ]
        for codes in fixtures:
            with self.subTest(count=len(codes)):
                parsed = Reviewer._parse_structured_review_output(
                    json.dumps(self._payload(violation_codes=codes))
                )
                self.assertIsNotNone(parsed)
                self.assertEqual(parsed[4], codes)

    def test_semantic_code_boundaries_count_duplicates_and_paths(self):
        valid_64 = "A" + "_" * 63
        self.assertIsNotNone(Reviewer._parse_structured_review_output(
            json.dumps(self._payload(violation_codes=[valid_64]))
        ))
        cases = [
            (["lower_case"], "$.violation_codes[0]"),
            (["A" + "_" * 64], "$.violation_codes[0]"),
            (["DUP", "DUP"], "$.violation_codes[1]"),
            ([f"CODE_{i}" for i in range(65)], "$.violation_codes"),
        ]
        for codes, path in cases:
            with self.subTest(path=path):
                result = Reviewer._parse_structured_review_output_diagnostic(
                    json.dumps(self._payload(violation_codes=codes)),
                    source="output-last-message", candidate_count=1,
                )
                self.assertIsNone(result.value)
                self.assertTrue(result.is_json)
                self.assertEqual(result.diagnostic["invalid_path"], path)

    def test_structured_diagnostics_are_redacted_and_path_specific(self):
        cases = [
            ([], "$"),
            ({"result": "PASS"}, "$.reason"),
            (self._payload(details=[1]), "$.details[0]"),
            (self._payload(result="PASS", risk="HIGH"), "$.risk"),
        ]
        for payload, path in cases:
            with self.subTest(path=path):
                result = Reviewer._parse_structured_review_output_diagnostic(
                    json.dumps(payload), source="jsonl-final-agent-message", candidate_count=2
                )
                self.assertIsNone(result.value)
                self.assertEqual(result.diagnostic["invalid_path"], path)
                self.assertNotIn("VALIDATION_INCOMPLETE", json.dumps(result.diagnostic))

    def test_json_invalid_never_uses_legacy_text_contract(self):
        payload = self._payload(result="PASS", risk="HIGH", reason="RESULT: PASS")
        parsed = Reviewer._parse_structured_review_output_diagnostic(
            json.dumps(payload), source="output-last-message", candidate_count=1
        )
        self.assertTrue(parsed.is_json)
        self.assertIsNone(parsed.value)
        self.assertEqual(parsed.diagnostic["stage"], "semantic")

    def test_last_jsonl_agent_message_is_selected(self):
        stdout = "\n".join(json.dumps(event) for event in [
            {"type": "item.completed", "item": {"type": "agent_message", "text": "first"}},
            {"type": "item.completed", "item": {"type": "command_execution", "text": "ignored"}},
            {"type": "item.completed", "item": {"type": "agent_message", "text": "last"}},
        ])
        self.assertEqual(reviewer._extract_codex_messages(stdout), ["first", "last"])
        self.assertEqual(reviewer._extract_codex_text(stdout), "last")

    def test_nonzero_codex_exit_uses_only_redacted_explicit_error_event(self):
        stdout = "\n".join([
            json.dumps({
                "type": "item.completed",
                "item": {"type": "agent_message", "text": "private diff body"},
            }),
            json.dumps({
                "type": "error",
                "message": "invalid output schema token=supersecretvalue",
            }),
        ])
        detail = reviewer._codex_exit_failure(1, stdout, "")
        self.assertIn("source=jsonl-error", detail)
        self.assertIn("invalid output schema", detail)
        self.assertNotIn("supersecretvalue", detail)
        self.assertNotIn("private diff body", detail)

        stderr_detail = reviewer._codex_exit_failure(
            1, stdout, "request failed password=supersecretvalue"
        )
        self.assertIn("source=stderr", stderr_detail)
        self.assertNotIn("supersecretvalue", stderr_detail)

        metadata_only = reviewer._codex_exit_failure(
            1, json.dumps({"type": "thread.started"}), ""
        )
        self.assertIn("jsonl_events=1", metadata_only)
        self.assertIn("explicit_error=missing", metadata_only)

    def test_codex_review_nonzero_jsonl_error_is_reported_without_raw_output(self):
        def fake_run(cmd, **kwargs):
            stdout = "\n".join([
                json.dumps({
                    "type": "item.completed",
                    "item": {"type": "agent_message", "text": "private diff body"},
                }),
                json.dumps({
                    "type": "turn.failed",
                    "error": {"message": "schema rejected token=supersecretvalue"},
                }),
            ])
            return subprocess.CompletedProcess(cmd, 1, stdout, "")

        instance = Reviewer(r"D:\inha_dev", profile=PROFILE)
        with mock.patch("reviewer.subprocess.run", side_effect=fake_run):
            result = instance._codex_review(
                "safe requirement", ["cap/A.java"], "safe batch diff"
            )
        self.assertFalse(result[3])
        self.assertEqual(result[4], "execution_unavailable")
        detail = " ".join(result[2])
        self.assertIn("source=jsonl-error", detail)
        self.assertIn("schema rejected", detail)
        self.assertNotIn("supersecretvalue", detail)
        self.assertNotIn("private diff body", detail)

    def test_activity_timeout_runner_nonzero_jsonl_error_is_reported(self):
        stdout = "\n".join([
            json.dumps({"type": "thread.started", "thread_id": "safe-id"}),
            json.dumps({
                "type": "turn.failed",
                "error": {"message": "output schema rejected token=supersecretvalue"},
            }),
        ])
        instance = Reviewer(
            r"D:\inha_dev",
            profile=PROFILE,
            timeout_config=worker.TimeoutConfig(),
        )
        with mock.patch(
            "reviewer._run_with_activity_timeout",
            return_value=(1, stdout, "", None),
        ):
            result = instance._codex_review(
                "safe requirement", ["cap/A.java"], "safe batch diff"
            )
        self.assertFalse(result[3])
        self.assertEqual(result[4], "execution_unavailable")
        detail = " ".join(result[2])
        self.assertIn("source=jsonl-error", detail)
        self.assertIn("output schema rejected", detail)
        self.assertNotIn("supersecretvalue", detail)
        self.assertNotIn("safe batch diff", detail)

    def _run_mocked_codex(self, output_file: str, stdout: str = "", stderr: str = ""):
        def fake_run(cmd, **kwargs):
            Path(cmd[cmd.index("-o") + 1]).write_text(output_file, encoding="utf-8")
            return subprocess.CompletedProcess(cmd, 0, stdout, stderr)

        instance = Reviewer(r"D:\inha_dev", profile=PROFILE)
        with mock.patch("reviewer.subprocess.run", side_effect=fake_run):
            return instance._codex_review("safe requirement", ["cap/A.java"], "batch diff")

    def test_output_last_message_precedes_jsonl_and_empty_file_falls_back(self):
        valid = json.dumps({
            "result": "PASS", "reason": "file", "details": [], "risk": "NONE",
            "violation_codes": [], "reviewed_files": ["cap/A.java"],
            "reviewed_task_files": None,
            "inspected_unchanged_target_files": None,
        })
        other = json.dumps({
            "type": "item.completed",
            "item": {"type": "agent_message", "text": json.dumps(self._payload())},
        })
        result = self._run_mocked_codex(valid, stdout=other)
        self.assertEqual(result[:2], ("PASS", "file"))

        result = self._run_mocked_codex("", stdout=other)
        self.assertEqual(result[0], "FAIL")
        self.assertEqual(result[1], "safe")

    def test_structured_invalid_does_not_fallback_and_diagnostic_is_redacted(self):
        secret_code = "lower_secret_value"
        invalid = json.dumps(self._payload(
            reason="RESULT: PASS secret-reason", violation_codes=[secret_code]
        ))
        output = io.StringIO()
        with redirect_stdout(output):
            result = self._run_mocked_codex(invalid)
        self.assertFalse(result[3])
        self.assertEqual(result[4], "output_parse_failed")
        self.assertNotIn("text parser", output.getvalue())
        diagnostic = " ".join(result[2])
        self.assertIn("$.violation_codes[0]", diagnostic)
        self.assertNotIn(secret_code, diagnostic)
        self.assertNotIn("secret-reason", diagnostic)

    def test_only_real_legacy_text_falls_back_and_stderr_is_never_response(self):
        legacy = "RESULT: PASS\nREASON: legacy ok\nRISK: NONE"
        result = self._run_mocked_codex(legacy)
        self.assertTrue(result[3])
        self.assertEqual(result[:2], ("PASS", "legacy ok"))

        result = self._run_mocked_codex("", stdout="", stderr=legacy)
        self.assertFalse(result[3])
        self.assertEqual(result[4], "output_parse_failed")

    def test_legacy_text_requires_result_as_first_nonempty_line(self):
        valid = "\ufeff\n  RESULT: FAIL\nREASON: defect\nRISK: LOW"
        self.assertEqual(Reviewer._parse_review_output(valid)[:2], ("FAIL", "defect"))
        rejected = [
            "analysis first\nRESULT: PASS\nREASON: ok\nRISK: NONE",
            "[출력 형식] RESULT: PASS 또는 FAIL\nRESULT: PASS\nREASON: ok\nRISK: NONE",
            '{"note":"RESULT: PASS"}\nRESULT: PASS\nREASON: ok\nRISK: NONE',
            "RESULT: PASS trailing prose\nREASON: ok\nRISK: NONE",
        ]
        for response in rejected:
            with self.subTest(first=response.splitlines()[0]):
                self.assertIsNone(Reviewer._parse_review_output(response)[0])

    def test_codex_command_uses_schema_output_file_and_isolated_env(self):
        captured: dict[str, object] = {}

        def fake_run(cmd, **kwargs):
            captured["cmd"] = list(cmd)
            captured["env"] = dict(kwargs["env"])
            output = {
                "result": "PASS", "reason": "ok", "details": [], "risk": "NONE",
                "violation_codes": [], "reviewed_files": ["cap/A.java"],
                "reviewed_task_files": None,
                "inspected_unchanged_target_files": None,
            }
            Path(cmd[cmd.index("-o") + 1]).write_text(json.dumps(output), encoding="utf-8")
            return subprocess.CompletedProcess(cmd, 0, "", "")

        source = {
            "PATH": os.environ.get("PATH", ""),
            "KKM_TG_BOT_TOKEN": "manager-only-secret",
            "GITLAB_PAT": "manager-only-pat",
            "OPENAI_API_KEY": "role-auth",
        }
        instance = Reviewer(r"D:\inha_dev", profile=PROFILE)
        with mock.patch.dict(os.environ, source, clear=True), mock.patch(
            "reviewer.subprocess.run", side_effect=fake_run
        ):
            result = instance._codex_review(
                "수정", ["cap/A.java"], "diff --git a/A.java b/A.java"
            )
        self.assertTrue(result[3])
        cmd = captured["cmd"]
        self.assertIn("--output-schema", cmd)
        self.assertIn("-o", cmd)
        env = captured["env"]
        self.assertEqual(env["AGENT_PROFILE_DIR"], str(PROFILE.profile_dir.resolve()))
        self.assertIn("OPENAI_API_KEY", env)
        self.assertNotIn("KKM_TG_BOT_TOKEN", env)
        self.assertNotIn("GITLAB_PAT", env)

    def test_malformed_text_fallback_is_fail_closed(self):
        self.assertEqual(Reviewer._parse_review_output("RESULT: PASS")[0], None)
        self.assertEqual(
            Reviewer._parse_review_output("RESULT: PASS\nREASON: ok\nRISK: WEIRD")[0],
            None,
        )


class EnvironmentAndRedactionTest(unittest.TestCase):
    @staticmethod
    def _encoded_output(encoding, write):
        buffer = io.BytesIO()
        stream = io.TextIOWrapper(buffer, encoding=encoding, errors="strict")
        try:
            write(stream)
            stream.flush()
            return buffer.getvalue().decode(encoding)
        finally:
            stream.detach()

    def test_isolated_environment_is_case_insensitive_allowlist(self):
        source = {
            "Path": "bin", "systemroot": "windows", "TEMP": "tmp",
            "OPENAI_API_KEY": "auth", "KKM_TG_CHAT_ID": "42",
            "TELEGRAM_BOT_TOKEN": "telegram-secret", "SHOULD_NOT_PASS": "sentinel",
            "SSH_AUTH_SOCK": "ssh", "KUBECONFIG": "kube",
        }
        env = isolated_subprocess_env("codex", PROFILE.profile_dir, source)
        self.assertEqual(env["AGENT_PROFILE_DIR"], str(PROFILE.profile_dir.resolve()))
        self.assertEqual(env["OPENAI_API_KEY"], "auth")
        self.assertEqual(env["PATH"], "bin")
        self.assertEqual(env["SYSTEMROOT"], "windows")
        for blocked in (
            "KKM_TG_CHAT_ID", "TELEGRAM_BOT_TOKEN", "SHOULD_NOT_PASS",
            "SSH_AUTH_SOCK", "KUBECONFIG",
        ):
            self.assertNotIn(blocked, env)

    def test_role_credentials_and_settings_are_strictly_separated(self):
        source = {
            "PATH": "bin", "OPENAI_API_KEY": "openai", "DROID_API_KEY": "droid",
            "KKM_DROID_AUTO_MODIFICATION": "high", "KKM_REVIEW_MODEL": "review-model",
            "KKM_TESTER_MODEL": "tester-model", "KKM_PLANNER_MODEL": "planner-model",
            "TIBERO_PASSWORD": "db-secret", "JAVA_HOME": "java", "NVM_HOME": "nvm",
            "KKM_TG_BOT_TOKEN": "telegram", "SHOULD_NOT_PASS": "sentinel",
        }
        envs = {
            role: isolated_subprocess_env(role, PROFILE.profile_dir, source)
            for role in ("codex", "droid", "reviewer", "tester", "planner", "build")
        }
        self.assertIn("OPENAI_API_KEY", envs["codex"])
        self.assertIn("OPENAI_API_KEY", envs["reviewer"])
        self.assertNotIn("OPENAI_API_KEY", envs["droid"])
        self.assertIn("DROID_API_KEY", envs["droid"])
        self.assertIn("DROID_API_KEY", envs["tester"])
        self.assertIn("DROID_API_KEY", envs["planner"])
        self.assertNotIn("DROID_API_KEY", envs["reviewer"])
        self.assertIn("KKM_DROID_AUTO_MODIFICATION", envs["droid"])
        self.assertNotIn("KKM_DROID_AUTO_MODIFICATION", envs["tester"])
        self.assertIn("KKM_REVIEW_MODEL", envs["reviewer"])
        self.assertIn("KKM_TESTER_MODEL", envs["tester"])
        self.assertIn("KKM_PLANNER_MODEL", envs["planner"])
        self.assertIn("TIBERO_PASSWORD", envs["tester"])
        self.assertNotIn("TIBERO_PASSWORD", envs["build"])
        self.assertIn("JAVA_HOME", envs["build"])
        self.assertIn("NVM_HOME", envs["build"])
        for role, env in envs.items():
            self.assertNotIn("KKM_TG_BOT_TOKEN", env, role)
            self.assertNotIn("SHOULD_NOT_PASS", env, role)
            if role == "build":
                self.assertNotIn("AGENT_PROFILE_DIR", env)
            else:
                self.assertEqual(
                    env["AGENT_PROFILE_DIR"], str(PROFILE.profile_dir.resolve())
                )

    def test_parent_environment_is_not_copied(self):
        source = {f"UNRELATED_{index}": str(index) for index in range(50)}
        source["PATH"] = "bin"
        env = isolated_subprocess_env("build", source=source)
        self.assertEqual(env, {"PATH": "bin"})

    def test_git_environment_is_allowlist_only_with_fixed_controls(self):
        from runtime_safety import git_subprocess_env

        common = {
            "Path": "bin", "pathext": ".EXE;.CMD", "systemroot": "windows",
            "WINDIR": "windows", "COMSPEC": "cmd.exe", "SYSTEMDRIVE": "C:",
            "TEMP": "temp", "TMP": "tmp", "USERPROFILE": "user",
            "APPDATA": "app", "LOCALAPPDATA": "local", "HOMEDRIVE": "C:",
            "HOMEPATH": "\\Users\\test",
        }
        blocked_names = (
            "KKM_TG_BOT_TOKEN", "SHOULD_NOT_PASS", "SSH_AUTH_SOCK",
            "KUBECONFIG", "UNRELATED_VALUE", "GIT_DIR", "GIT_WORK_TREE",
            "GIT_OBJECT_DIRECTORY", "GIT_ALTERNATE_OBJECT_DIRECTORIES",
            "GIT_CONFIG", "GIT_CONFIG_COUNT", "GIT_SSH", "GIT_SSH_COMMAND",
            "GIT_INDEX_FILE", "GIT_ARBITRARY_CONTROL",
        )
        source = dict(common)
        source.update({name: "blocked" for name in blocked_names})

        env = git_subprocess_env(source=source)

        self.assertEqual(env["GIT_TERMINAL_PROMPT"], "0")
        self.assertEqual(env["GIT_PAGER"], "cat")
        self.assertEqual(
            set(env),
            {
                "PATH", "PATHEXT", "SYSTEMROOT", "WINDIR", "COMSPEC",
                "SYSTEMDRIVE", "TEMP", "TMP", "USERPROFILE", "APPDATA",
                "LOCALAPPDATA", "HOMEDRIVE", "HOMEPATH",
                "GIT_TERMINAL_PROMPT", "GIT_PAGER",
            },
        )
        for name in blocked_names:
            self.assertNotIn(name, env)

    def test_git_environment_adds_index_only_for_that_fresh_mapping(self):
        from runtime_safety import git_subprocess_env

        with tempfile.TemporaryDirectory() as td:
            index_path = Path(td) / "temporary index"
            regular = git_subprocess_env(source={"PATH": "bin"})
            indexed = git_subprocess_env(index_path, source={"PATH": "bin"})
            indexed["CALL_LOCAL"] = "mutation"
            next_regular = git_subprocess_env(source={"PATH": "bin"})

        self.assertNotIn("GIT_INDEX_FILE", regular)
        self.assertEqual(indexed["GIT_INDEX_FILE"], str(index_path.resolve()))
        self.assertNotIn("GIT_INDEX_FILE", next_regular)
        self.assertNotIn("CALL_LOCAL", next_regular)

    def test_git_collector_passes_explicit_fresh_environments(self):
        completed = subprocess.CompletedProcess(["git"], 0, "ok", "")
        collector = GitCollector(os.getcwd())
        with tempfile.TemporaryDirectory() as td, mock.patch.dict(os.environ, {
            "PATH": os.environ.get("PATH", ""),
            "KKM_TG_BOT_TOKEN": "blocked", "SHOULD_NOT_PASS": "blocked",
            "GIT_DIR": "blocked", "GIT_INDEX_FILE": "blocked",
        }, clear=True), mock.patch(
            "git_collector.subprocess.run", return_value=completed
        ) as run_mock:
            collector._run_in(Path(td), ["status"])
            regular_env = run_mock.call_args.kwargs["env"]
            collector._run_in(Path(td), ["write-tree"], index_file=Path(td) / "idx")
            index_env = run_mock.call_args.kwargs["env"]

        self.assertIsNotNone(regular_env)
        self.assertNotIn("GIT_INDEX_FILE", regular_env)
        self.assertNotIn("KKM_TG_BOT_TOKEN", regular_env)
        self.assertNotIn("SHOULD_NOT_PASS", regular_env)
        self.assertNotIn("GIT_DIR", regular_env)
        self.assertEqual(index_env["GIT_INDEX_FILE"], str((Path(td) / "idx").resolve()))
        self.assertIsNot(regular_env, index_env)

    def test_temporary_index_is_scoped_to_only_its_git_calls(self):
        environments = []

        def fake_run(cmd, **kwargs):
            environments.append(dict(kwargs["env"]))
            stdout = "tree-id\n" if cmd[-1] == "write-tree" else ""
            return subprocess.CompletedProcess(cmd, 0, stdout, "")

        collector = GitCollector(os.getcwd())
        with tempfile.TemporaryDirectory() as td, mock.patch(
            "git_collector.subprocess.run", side_effect=fake_run
        ):
            ok, tree, error = collector._temporary_worktree_tree(Path(td))
            collector._run_in(Path(td), ["status"])

        self.assertTrue(ok, error)
        self.assertEqual(tree, "tree-id")
        temporary = environments[:-1]
        self.assertEqual(len(temporary), 4)
        self.assertTrue(all("GIT_INDEX_FILE" in env for env in temporary))
        self.assertEqual(len({env["GIT_INDEX_FILE"] for env in temporary}), 1)
        self.assertNotIn("GIT_INDEX_FILE", environments[-1])
        self.assertEqual(len({id(env) for env in environments}), len(environments))

    def test_git_collector_read_only_command_works_in_local_repository(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            subprocess.run(["git", "init"], cwd=root, check=True, capture_output=True)
            code, out, error = GitCollector(str(root))._run_in(
                root, ["rev-parse", "--git-dir"]
            )
        self.assertEqual(code, 0, error)
        self.assertTrue(out.strip())

    def test_safe_print_cp949_preserves_korean_and_replaces_only_unencodable(self):
        output = self._encoded_output(
            "cp949",
            lambda stream: safe_print("한글", "📤", "—", sep="|", end="!", file=stream, flush=True),
        )
        self.assertEqual(output, "한글|?|?!")

    def test_safe_print_supports_stdout_stderr_and_redacts_before_encoding(self):
        secret = "민감-token-📤"
        with mock.patch.dict(os.environ, {"KKM_TG_BOT_TOKEN": secret}, clear=True):
            stdout = self._encoded_output(
                "cp949",
                lambda stream: self._print_to_stdout(stream, f"한글 {secret} —"),
            )
            stderr = self._encoded_output(
                "cp949",
                lambda stream: safe_print("경고 📤", file=stream, end="", flush=True),
            )
        self.assertEqual(stdout, "한글 **** ?" + os.linesep)
        self.assertEqual(stderr, "경고 ?")

    @staticmethod
    def _print_to_stdout(stream, value):
        with mock.patch.object(sys, "stdout", stream):
            safe_print(value, flush=True)

    def test_safe_print_utf8_and_structured_data_remain_lossless(self):
        value = "한글 — 📤"
        data = {"value": value}
        output = self._encoded_output(
            "utf-8", lambda stream: safe_print(value, file=stream, flush=True)
        )
        self.assertEqual(output, value + os.linesep)
        self.assertEqual(json.dumps(data, ensure_ascii=False), '{"value": "한글 — 📤"}')

    def test_safe_print_preserves_sep_end_and_flush(self):
        class StrictStream:
            encoding = "cp949"

            def __init__(self):
                self.parts = []
                self.flush_count = 0

            def write(self, value):
                value.encode(self.encoding, errors="strict")
                self.parts.append(value)

            def flush(self):
                self.flush_count += 1

        stream = StrictStream()
        safe_print("한글", "📤", sep=" — ", end="!", file=stream, flush=True)
        self.assertEqual("".join(stream.parts), "한글 ? ?!")
        self.assertEqual(stream.flush_count, 1)

    def test_planner_failure_console_path_is_cp949_safe(self):
        task = TaskState(
            task_id="TASK-PLANNER-CONSOLE",
            requirement="한글 분석",
            task_mode="ANALYSIS",
            working_dir=os.getcwd(),
        )
        output = self._encoded_output(
            "cp949",
            lambda stream: self._run_missing_planner(task, stream),
        )
        self.assertIn("Planner", output)
        self.assertIn("?", output)

    @staticmethod
    def _run_missing_planner(task, stream):
        with mock.patch(
            "planner.subprocess.run", side_effect=FileNotFoundError
        ), mock.patch.object(sys, "stdout", stream):
            result = planner.run_planner(task, task.working_dir)
        if result is not None:
            raise AssertionError("planner missing-executable path must return None")

    def test_codex_worker_child_env_has_profile_but_no_manager_secret(self):
        captured: dict[str, object] = {}

        def fake_run(cmd, **kwargs):
            captured["env"] = dict(kwargs["env"])
            return subprocess.CompletedProcess(cmd, 0, "done", "password=supersecret")

        task = TaskState(
            task_id="TASK-ENV", requirement="분석", task_mode="ANALYSIS",
            working_dir=r"D:\inha_dev", target_module=["cap"],
        )
        instance = worker.CodexWorker(timeout=5, profile=PROFILE)
        with mock.patch.dict(os.environ, {
            "PATH": os.environ.get("PATH", ""),
            "OPENAI_API_KEY": "auth", "KKM_TG_BOT_TOKEN": "secret-manager",
        }, clear=True), mock.patch(
            "worker.codex_host_pair_matches", return_value=True
        ), mock.patch("worker.subprocess.run", side_effect=fake_run):
            result = instance.execute(task.requirement, task.working_dir, task)
        env = captured["env"]
        self.assertEqual(env["AGENT_PROFILE_DIR"], str(PROFILE.profile_dir.resolve()))
        self.assertNotIn("KKM_TG_BOT_TOKEN", env)
        self.assertNotIn("supersecret", result.stderr)

    def test_droid_worker_child_env_has_profile_and_droid_auth_only(self):
        captured: dict[str, object] = {}

        def fake_run(cmd, **kwargs):
            captured["env"] = dict(kwargs["env"])
            return subprocess.CompletedProcess(cmd, 0, "done", "")

        task = TaskState(
            task_id="TASK-DROID-ENV", requirement="분석", task_mode="ANALYSIS",
            working_dir=r"D:\inha_dev", target_module=["cap"],
        )
        instance = worker.DroidWorker(timeout=5, model="test", profile=PROFILE)
        with mock.patch.dict(os.environ, {
            "PATH": os.environ.get("PATH", ""), "DROID_API_KEY": "droid-auth",
            "OPENAI_API_KEY": "wrong-role", "KKM_TG_BOT_TOKEN": "telegram",
        }, clear=True), mock.patch("worker.subprocess.run", side_effect=fake_run):
            result = instance.execute(task.requirement, task.working_dir, task)
        self.assertTrue(result.success)
        env = captured["env"]
        self.assertEqual(env["AGENT_PROFILE_DIR"], str(PROFILE.profile_dir.resolve()))
        self.assertEqual(env["DROID_API_KEY"], "droid-auth")
        self.assertNotIn("OPENAI_API_KEY", env)
        self.assertNotIn("KKM_TG_BOT_TOKEN", env)

    def test_build_runner_passes_only_build_environment(self):
        completed = subprocess.CompletedProcess(["fake"], 0, "ok", "")
        with tempfile.TemporaryDirectory() as td, mock.patch.dict(os.environ, {
            "PATH": "bin", "JAVA_HOME": "java", "NVM_HOME": "nvm",
            "TIBERO_PASSWORD": "db-secret", "KKM_TG_BOT_TOKEN": "telegram",
            "SHOULD_NOT_PASS": "sentinel",
        }, clear=True), mock.patch(
            "build_test_runner.subprocess.run", return_value=completed
        ) as run_mock:
            code, _, _ = BuildTestRunner(td)._run(["fake"])
        self.assertEqual(code, 0)
        env = run_mock.call_args.kwargs["env"]
        self.assertEqual(env["JAVA_HOME"], "java")
        self.assertEqual(env["NVM_HOME"], "nvm")
        self.assertNotIn("TIBERO_PASSWORD", env)
        self.assertNotIn("KKM_TG_BOT_TOKEN", env)
        self.assertNotIn("SHOULD_NOT_PASS", env)

    def test_reviewer_details_and_worker_stderr_are_masked_before_state(self):
        task = TaskState(task_id="TASK-MASK", requirement="x")
        result = ReviewResult(
            passed=False, reason="password=supersecret",
            details=["token=verysecrettoken"], status=reviewer.REVIEW_FAIL, risk="LOW",
        )
        Reviewer._apply_to_state(task, result)
        self.assertNotIn("supersecret", task.review_result)
        self.assertNotIn("verysecrettoken", "".join(task.review_violations))
        worker._apply_to_state(
            task, worker.WorkerResult(False, 1, "", "password=supersecret", [])
        )
        self.assertNotIn("supersecret", task.worker_stderr)


class ProcessAndStateTest(unittest.TestCase):
    def test_normal_exit_drains_last_jsonl_message(self):
        code = (
            "import sys; "
            "print('{\\\"type\\\":\\\"message\\\",\\\"role\\\":\\\"assistant\\\",\\\"text\\\":\\\"last\\\"}'); "
            "sys.stderr.write('tail-error\\n')"
        )
        exit_code, stdout, stderr, timeout = worker._run_with_activity_timeout(
            [sys.executable, "-c", code], None, os.getcwd(),
            worker.TimeoutConfig(10, 10, 10, 10), "분석", "ANALYSIS", heartbeat=20,
        )
        self.assertEqual(exit_code, 0)
        self.assertIsNone(timeout)
        self.assertIn('"text":"last"', stdout)
        self.assertIn("tail-error", stderr)

    def test_timeout_terminates_only_the_known_pid_tree(self):
        class FakeProcess:
            pid = 4321
            returncode = None
            stdout: list[str] = []
            stderr: list[str] = []

            def poll(self):
                return self.returncode

            def kill(self):
                self.returncode = -9

            def wait(self, timeout=None):
                self.returncode = -9
                return self.returncode

        calls: list[list[str]] = []

        def fake_run(cmd, **kwargs):
            calls.append(list(cmd))
            return subprocess.CompletedProcess(cmd, 0, "", "")

        with mock.patch("worker.subprocess.Popen", return_value=FakeProcess()), mock.patch(
            "worker.subprocess.run", side_effect=fake_run
        ):
            _, _, _, reason = worker._run_with_activity_timeout(
                ["fake-worker"], None, os.getcwd(),
                worker.TimeoutConfig(0, 0, 0, 0), "수정", "MODIFICATION",
            )
        self.assertIsNotNone(reason)
        if os.name == "nt":
            self.assertEqual(calls, [["taskkill", "/PID", "4321", "/T", "/F"]])
            self.assertNotIn("/IM", calls[0])
        else:
            self.assertEqual(calls, [])

    def test_task_state_atomic_replace_and_secret_scrub(self):
        with tempfile.TemporaryDirectory() as td:
            task = TaskState(
                task_id="TASK-20260825-120000", requirement="token=verysecrettoken"
            )
            with mock.patch("control_paths.os.replace", wraps=os.replace) as replace:
                path = task.save(td)
            replace.assert_called_once()
            data = json.loads(path.read_text(encoding="utf-8"))
            self.assertNotIn("verysecrettoken", json.dumps(data))
            self.assertFalse(list(path.parent.glob("*.tmp")))

    def test_configured_task_root_is_independent_of_project_cwd(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td).resolve()
            task_root = root / "harness" / ".tasks"
            project_root = root / "project"
            task_root.parent.mkdir()
            project_root.mkdir()
            previous_cwd = Path.cwd()
            try:
                os.chdir(project_root)
                task = TaskState(
                    task_id="TASK-20260826-171500", requirement="safe"
                )
                json_path = task.save(task_root)
                md_path = Reporter().save_markdown(task, out_dir=task_root)
            finally:
                os.chdir(previous_cwd)
            self.assertEqual(json_path.parent, task_root / "20260826")
            self.assertEqual(md_path.parent, task_root / "20260826")
            self.assertFalse((project_root / ".tasks").exists())

    def test_completed_task_writes_markdown_before_runner_shutdown(self):
        with tempfile.TemporaryDirectory() as td:
            task_root = Path(td).resolve() / ".tasks"
            task = TaskState(
                task_id="TASK-20260826-171501",
                requirement="safe",
                status="FAILED",
                stage="DONE",
            )
            future = mock.Mock()
            future.done.return_value = True
            reported: set[str] = set()
            with redirect_stdout(io.StringIO()):
                run._print_completed(
                    [(future, task, "codex")], reported, task_root
                )
            report = Path(td) / ".tasks" / "20260826" / f"{task.task_id}.md"
            self.assertTrue(report.is_file())
            self.assertIn(task.task_id, reported)

    def test_manager_persists_attempts_to_injected_task_root(self):
        with tempfile.TemporaryDirectory() as td:
            task_root = Path(td).resolve() / "harness" / ".tasks"
            task_root.parent.mkdir()
            manager = Manager.__new__(Manager)
            manager.max_retry = 1
            manager.task_root = task_root
            task = TaskState(
                task_id="TASK-20260826-171502",
                requirement="safe",
                status="RUNNING",
            )

            def successful_pipeline(current):
                current.verification_status = "VERIFIED"
                return ManagerResult(success=True, message="ok")

            manager._run_pipeline = successful_pipeline
            with redirect_stdout(io.StringIO()):
                result = manager._run_with_retry_locked(task)
            path = task_root / "20260826" / f"{task.task_id}.json"
            self.assertTrue(result.success)
            self.assertTrue(path.is_file())
            self.assertEqual(
                json.loads(path.read_text(encoding="utf-8"))["status"], "SUCCESS"
            )

    def test_task_id_collision_gets_suffix(self):
        with tempfile.TemporaryDirectory() as td, mock.patch(
            "run.datetime"
        ) as dt:
            dt.now.return_value.strftime.return_value = "20260825-120000"
            run._ISSUED_TASK_IDS.clear()
            first = run._next_task_id(td)
            second = run._next_task_id(td)
        self.assertEqual(first, "TASK-20260825-120000")
        self.assertEqual(second, "TASK-20260825-120000-01")

    def test_skipped_tester_state_is_present_in_reviewer_context(self):
        task = TaskState(task_id="TASK-SKIP", requirement="x")
        task.test_required = True
        task.test_status = "SKIPPED"
        task.test_summary = "tester disabled"
        context = reviewer.build_tester_context(task)
        self.assertIn("required: True", context)
        self.assertIn("status: SKIPPED", context)
        self.assertIn("tester disabled", context)

    def test_full_profile_baseline_exposes_sibling_module_change(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td).resolve()
            subprocess.run(["git", "init"], cwd=root, check=True, capture_output=True)
            subprocess.run(
                ["git", "config", "user.email", "test@example.com"],
                cwd=root, check=True, capture_output=True,
            )
            subprocess.run(
                ["git", "config", "user.name", "test"],
                cwd=root, check=True, capture_output=True,
            )
            (root / "api").mkdir()
            (root / "web").mkdir()
            (root / "api" / "A.txt").write_text("base", encoding="utf-8")
            (root / "web" / "B.txt").write_text("base", encoding="utf-8")
            subprocess.run(["git", "add", "-A"], cwd=root, check=True, capture_output=True)
            subprocess.run(
                ["git", "commit", "-m", "base"], cwd=root, check=True, capture_output=True
            )
            profile = replace(
                PROFILE,
                workspace_roots=(root,),
                modules=(
                    ModuleProfile("api", "api", "backend", False),
                    ModuleProfile("web", "web", "frontend", False),
                ),
                protected_paths=(),
                local_dev_config_excludes=(),
            )
            task = TaskState(
                task_id="TASK-SCOPE", requirement="api 수정", working_dir=str(root),
                task_mode="MODIFICATION", target_module=["api"],
            )
            collector = GitCollector(str(root), profile=profile)
            baseline, error = collector.capture_baseline(task)
            self.assertIsNotNone(baseline, error)
            (root / "web" / "B.txt").write_text("unexpected", encoding="utf-8")
            result = collector.collect(task, baseline)
            self.assertEqual(result.changed_files, ["web/B.txt"])
            codes, _ = reviewer.deterministic_violations(
                result.changed_files, result.diff, profile, task.target_module
            )
            self.assertIn("OUT_OF_SCOPE_CHANGE", codes)

    def test_module_prefixed_local_exclude_filters_diff(self):
        profile = replace(PROFILE, local_dev_config_excludes=("capui/config.local",))
        diff = (
            "diff --git a/config.local b/config.local\n"
            "--- a/config.local\n+++ b/config.local\n@@ -1 +1 @@\n-old\n+new\n"
        )
        self.assertEqual(_filter_diff_body(diff, profile, "capui/"), "")

    def test_root_package_without_build_script_stays_skipped(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "package.json").write_text("{}", encoding="utf-8")
            result = BuildTestRunner(str(root)).run(["src/A.ts"])
        self.assertEqual(result.build_status, "SKIPPED")
        self.assertFalse(result.success)


class BatchFileStaticTest(unittest.TestCase):
    def test_run_bat_check_mode_is_quoted_and_preserves_exit(self):
        text = Path("run_e4u.bat").read_text(encoding="utf-8")
        self.assertIn('if /I "%~1"=="--check-profile"', text)
        self.assertIn('if not exist "%USERPROFILE%\\.codex\\AGENTS.md"', text)
        self.assertIn('set "AGENT_PROFILE_DIR=%PROFILE_DIR%"', text)
        self.assertIn('pushd "%PROJECT_ROOT%"', text)
        self.assertEqual(
            text.count(
                'python "%~dp0run.py" "%PROJECT_ROOT%" %* --profile "%PROFILE_DIR%"'
            ),
            2,
        )
        self.assertIn('set "EXIT_CODE=%ERRORLEVEL%"', text)
        self.assertNotIn('--profile "%PROFILE_DIR%" --check-profile', text)
        self.assertNotIn("sync_user_config", text.casefold())
        self.assertNotIn("--approve-for-me", text)
        self.assertNotIn(" copy ", text.casefold())
        self.assertNotIn(" del ", text.casefold())
        self.assertNotIn("pause", text.casefold())

    def test_codex_bat_guards_paths_and_exports_child_profile(self):
        text = Path("codex_e4u.bat").read_text(encoding="utf-8")
        self.assertIn('if not exist "%PROJECT_ROOT%\\"', text)
        self.assertIn('if not exist "%PROFILE_DIR%\\project.json"', text)
        self.assertIn('if not exist "%USERPROFILE%\\.codex\\AGENTS.md"', text)
        self.assertIn('set "AGENT_PROFILE_DIR=%PROFILE_DIR%"', text)
        self.assertIn('pushd "%PROJECT_ROOT%"', text)
        self.assertIn("codex %*", text)
        self.assertIn('set "EXIT_CODE=%ERRORLEVEL%"', text)
        self.assertNotIn("start ", text.casefold())
        self.assertNotIn("pause", text.casefold())

    def test_droid_bat_uses_cmd_execution_probe_and_preserves_contract(self):
        text = Path("droid_e4u.bat").read_text(encoding="utf-8")
        self.assertLess(
            text.index('if /I "%~1"=="--check-profile"'),
            text.index("droid --version >nul 2>&1"),
        )
        self.assertNotIn("where droid", text.casefold())
        self.assertIn('set "AGENT_PROFILE_DIR=%PROFILE_DIR%"', text)
        self.assertIn('pushd "%PROJECT_ROOT%"', text)
        self.assertIn("droid %*", text)
        self.assertIn('set "EXIT_CODE=%ERRORLEVEL%"', text)
        self.assertNotIn("--approve-for-me", text)
        self.assertNotIn("--dangerously", text)

    def test_bat_tests_never_execute_interactive_commands(self):
        tree = ast.parse(Path(__file__).read_text(encoding="utf-8"))
        directly_executed = []
        for node in ast.walk(tree):
            if not (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "subprocess"
                and node.func.attr == "run"
                and node.args
            ):
                continue
            command = node.args[0]
            first = command.elts[0] if isinstance(command, (ast.List, ast.Tuple)) and command.elts else command
            if isinstance(first, ast.Constant) and isinstance(first.value, str):
                name = Path(first.value).name.casefold()
                if name in {"run_e4u.bat", "codex_e4u.bat"}:
                    directly_executed.append((name, node.lineno))
        self.assertEqual(directly_executed, [])


@unittest.skipUnless(os.name == "nt", "Windows batch execution test")
class RunBatchExecutionTest(unittest.TestCase):
    def _run_stub(self, args, exit_code=0):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            project = root / "project root"
            profile = root / "fixed profile"
            user = root / "user"
            project.mkdir()
            profile.mkdir()
            (profile / "project.json").write_text("{}", encoding="utf-8")
            (user / ".codex").mkdir(parents=True)
            (user / ".codex" / "AGENTS.md").write_text("rules", encoding="utf-8")

            payload_path = root / "payload.json"
            stub_path = root / "run.py"
            batch_path = root / "run_e4u.bat"
            invoke_path = root / "invoke.cmd"
            stub_path.write_text(
                "import json, os, sys\n"
                "from pathlib import Path\n"
                "payload = {\n"
                "    'argv': sys.argv[1:],\n"
                "    'cwd': os.getcwd(),\n"
                "    'profile_env': os.environ.get('AGENT_PROFILE_DIR', ''),\n"
                "}\n"
                "Path(os.environ['RUN_BAT_PAYLOAD']).write_text(\n"
                "    json.dumps(payload), encoding='utf-8'\n"
                ")\n"
                "raise SystemExit(int(os.environ['RUN_BAT_EXIT']))\n",
                encoding="utf-8",
            )
            batch_text = (Path(__file__).resolve().parent / "run_e4u.bat").read_text(
                encoding="utf-8"
            )
            batch_text = batch_text.replace(r"D:\inha_dev", str(project))
            batch_text = batch_text.replace(r"D:\agents\profiles\e4u", str(profile))
            batch_path.write_text(batch_text, encoding="utf-8")

            environment = os.environ.copy()
            environment.pop("AGENT_PROFILE_DIR", None)
            environment.update(
                {
                    "USERPROFILE": str(user),
                    "RUN_BAT_PAYLOAD": str(payload_path),
                    "RUN_BAT_EXIT": str(exit_code),
                }
            )
            invoke_path.write_text(
                f'@call "{batch_path}" {subprocess.list2cmdline(args)}\n'
                '@exit /b %ERRORLEVEL%\n',
                encoding="utf-8",
            )
            completed = subprocess.run(
                ["cmd", "/d", "/c", str(invoke_path)],
                cwd=root,
                env=environment,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                check=False,
            )
            payload = json.loads(payload_path.read_text(encoding="utf-8"))
            expected = {
                "project": str(project),
                "profile": str(profile),
            }
        return completed, payload, expected

    def test_check_profile_is_forwarded_once_with_following_arguments(self):
        completed, payload, expected = self._run_stub(
            ["--check-profile", "--flag", "value with spaces"]
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(payload["argv"].count("--check-profile"), 1)
        self.assertEqual(
            payload["argv"],
            [
                expected["project"],
                "--check-profile",
                "--flag",
                "value with spaces",
                "--profile",
                expected["profile"],
            ],
        )

    def test_single_multiple_quoted_and_option_arguments_keep_order(self):
        user_args = ["--model", "gpt-test", "value with spaces", "--flag"]
        completed, payload, expected = self._run_stub(user_args)
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(
            payload["argv"],
            [expected["project"], *user_args, "--profile", expected["profile"]],
        )

    def test_empty_arguments_still_pass_fixed_project_profile_env_and_cwd(self):
        completed, payload, expected = self._run_stub([])
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(
            payload["argv"],
            [expected["project"], "--profile", expected["profile"]],
        )
        self.assertEqual(
            os.path.normcase(payload["profile_env"]),
            os.path.normcase(expected["profile"]),
        )
        self.assertEqual(
            os.path.normcase(payload["cwd"]),
            os.path.normcase(expected["project"]),
        )

    def test_user_profile_precedes_and_cannot_override_fixed_profile(self):
        user_profile = r"C:\untrusted profile"
        completed, payload, expected = self._run_stub(
            ["--profile", user_profile, "--check-profile"]
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(payload["argv"][-2:], ["--profile", expected["profile"]])
        parsed = run.parse_args(payload["argv"])
        self.assertEqual(
            os.path.normcase(parsed.profile_dir),
            os.path.normcase(expected["profile"]),
        )
        self.assertTrue(parsed.check_profile)

    def test_stub_exit_code_is_preserved(self):
        completed, _, _ = self._run_stub(["--flag"], exit_code=37)
        self.assertEqual(completed.returncode, 37)


@unittest.skipUnless(os.name == "nt", "Windows batch execution test")
class CodexBatchExecutionTest(unittest.TestCase):
    def _run_stub(self, args, exit_code=0):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            args_path = root / "args.txt"
            cwd_path = root / "cwd.txt"
            profile_path = root / "profile.txt"
            cmd_path = root / "codex.cmd"
            invoke_path = root / "invoke.cmd"
            cmd_path.write_text(
                '@echo off\n'
                '> "%CODEX_STUB_ARGS%" echo(%~1\n'
                '>> "%CODEX_STUB_ARGS%" echo(%~2\n'
                '>> "%CODEX_STUB_ARGS%" echo(%~3\n'
                '>> "%CODEX_STUB_ARGS%" echo(%~4\n'
                '>> "%CODEX_STUB_ARGS%" echo(%~5\n'
                '> "%CODEX_STUB_CWD%" cd\n'
                '> "%CODEX_STUB_PROFILE%" echo(%AGENT_PROFILE_DIR%\n'
                'exit /b %CODEX_STUB_EXIT%\n',
                encoding="utf-8",
            )
            environment = os.environ.copy()
            environment.update({
                "PATH": str(root) + os.pathsep + environment.get("PATH", ""),
                "CODEX_STUB_ARGS": str(args_path),
                "CODEX_STUB_CWD": str(cwd_path),
                "CODEX_STUB_PROFILE": str(profile_path),
                "CODEX_STUB_EXIT": str(exit_code),
            })
            batch_path = Path(__file__).resolve().parent / "codex_e4u.bat"
            invoke_path.write_text(
                f'@call "{batch_path}" {subprocess.list2cmdline(args)}\n'
                '@exit /b %ERRORLEVEL%\n',
                encoding="utf-8",
            )
            completed = subprocess.run(
                ["cmd", "/d", "/c", str(invoke_path)],
                cwd=root,
                env=environment,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                check=False,
            )
            result = {
                "args": [line for line in args_path.read_text().splitlines() if line],
                "cwd": cwd_path.read_text().strip(),
                "profile": profile_path.read_text().strip(),
            }
        return completed, result

    def test_single_argument_is_forwarded_with_profile_and_project_cwd(self):
        completed, result = self._run_stub(["--version"])
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(result["args"], ["--version"])
        self.assertEqual(os.path.normcase(result["cwd"]), os.path.normcase(r"D:\inha_dev"))
        self.assertEqual(
            os.path.normcase(result["profile"]),
            os.path.normcase(r"D:\agents\profiles\e4u"),
        )

    def test_multiple_and_quoted_arguments_keep_order_and_exit_code(self):
        args = ["--model", "gpt-test", "value with spaces", "--flag"]
        completed, result = self._run_stub(args, exit_code=37)
        self.assertEqual(result["args"], args)
        self.assertEqual(completed.returncode, 37)


if __name__ == "__main__":
    unittest.main()
