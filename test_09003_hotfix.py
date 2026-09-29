from __future__ import annotations

import os
import harness_temp as tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from git_collector import GitCollector, GitRepoBaseline, GitTaskBaseline
from notify import terminal_notification_projection
from task_state import TaskState


class EolRetryTests(unittest.TestCase):
    def _fixture(self):
        temp = tempfile.TemporaryDirectory()
        root = Path(temp.name)
        target = root / "sample.txt"
        target.write_bytes(b"a\r\nb\r\n")
        task = TaskState(task_id="TASK-09003", requirement="x")
        task.changed_files = ["sample.txt"]
        baseline = GitTaskBaseline((GitRepoBaseline(root, None, "h", "w", "i"),))
        collector = GitCollector(str(root))
        collector._run_in = lambda *_: (0, "i/lf w/crlf attr/ sample.txt\n", "")
        return temp, target, task, baseline, collector

    def test_permission_error_is_retried_and_evidence_is_preserved(self):
        temp, target, task, baseline, collector = self._fixture()
        self.addCleanup(temp.cleanup)
        real_replace = os.replace
        calls = 0

        def transient(src, dst):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise PermissionError(13, "sharing violation")
            return real_replace(src, dst)

        with patch("git_collector.os.replace", side_effect=transient), patch(
            "git_collector.time.sleep", return_value=None
        ):
            result = collector.repair_eol_to_index(task, baseline)
        self.assertTrue(result.success)
        self.assertEqual(target.read_bytes(), b"a\nb\n")
        self.assertEqual(calls, 2)
        self.assertEqual(result.diagnostics[0]["operation"], "os.replace")
        self.assertEqual(result.diagnostics[0]["file"], "sample.txt")
        self.assertFalse(list(target.parent.glob(".sample.txt.eol-*")))

    def test_unrelated_oserror_is_not_retried_and_fails_closed(self):
        temp, _, task, baseline, collector = self._fixture()
        self.addCleanup(temp.cleanup)
        calls = 0

        def fatal(*_):
            nonlocal calls
            calls += 1
            raise OSError(22, "invalid argument")

        with patch("git_collector.os.replace", side_effect=fatal):
            result = collector.repair_eol_to_index(task, baseline)
        self.assertFalse(result.success)
        self.assertEqual(calls, 1)
        self.assertEqual(result.error, "EOL_REPAIR_ERROR:OSError")

    def test_retry_exhaustion_fails_closed_without_temp_leak(self):
        temp, target, task, baseline, collector = self._fixture()
        self.addCleanup(temp.cleanup)
        with patch(
            "git_collector.os.replace",
            side_effect=PermissionError(13, "sharing violation"),
        ), patch("git_collector.time.sleep", return_value=None):
            result = collector.repair_eol_to_index(task, baseline)
        self.assertFalse(result.success)
        self.assertEqual(len(result.diagnostics), 3)
        self.assertEqual(target.read_bytes(), b"a\r\nb\r\n")
        self.assertFalse(list(target.parent.glob(".sample.txt.eol-*")))


class ProjectionTests(unittest.TestCase):
    def test_integration_blocker_wins_over_pass(self):
        projection = terminal_notification_projection(
            {
                "job_id": "JOB-X",
                "status": "AWAITING_QA",
                "last_result": {
                    "failure_reason": "PASS",
                    "commit_blockers": ["EOL_REPAIR_ERROR:PermissionError"],
                },
            },
            {"gate_reason": "AWAITING_QA"},
        )
        self.assertIn("EOL_REPAIR_ERROR:PermissionError", projection["message"])
        self.assertNotIn("사유: PASS", projection["message"])


class TaskCompatibilityTests(unittest.TestCase):
    def test_new_history_fields_default_for_old_state(self):
        task = TaskState(task_id="TASK-OLD", requirement="x")
        self.assertEqual(task.commit_blocker_history, [])
        self.assertEqual(task.eol_repair_diagnostics, [])


if __name__ == "__main__":
    unittest.main()
