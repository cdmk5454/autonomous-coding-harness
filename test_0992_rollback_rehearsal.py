"""0.99.2 rollback / restore rehearsal against a real git fixture.

Databasus principle: ``snapshot exists != restore proven``.  Rollback code
existing is not rollback correctness — this rehearsal proves the exact
pre-job state survives a failed Job's scoped rollback on a temporary git
repository (never the Product source):

    B0 → J1 success Δ1 → J2 success Δ2 → J3 candidate Δ3
       → J3 failure → J3 rollback → exactly B0 + Δ1 + Δ2

The fixture covers: modified tracked files, a new untracked file, a deleted
file, a staged change, pre-existing dirty content, and prior accepted
(preserved) batch deltas.  Nothing here touches the real Product workspace.
"""

from __future__ import annotations

import subprocess
import harness_temp as tempfile
import unittest
from pathlib import Path

from git_collector import GitCollector


def _run(repo: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    completed = subprocess.run(
        ["git", *args],
        cwd=str(repo),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if check and completed.returncode != 0:
        raise AssertionError(
            f"git {args} failed: {completed.returncode}\n{completed.stderr}"
        )
    return completed


class RollbackRehearsalTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.repo = Path(self.temp.name) / "work" / "app"
        self.repo.mkdir(parents=True)
        _run(self.repo, "init")
        _run(self.repo, "config", "core.autocrlf", "false")
        _run(self.repo, "config", "user.email", "rehearsal@example.invalid")
        _run(self.repo, "config", "user.name", "Rehearsal")
        (self.repo / "tracked.txt").write_text("base\n", encoding="utf-8")
        (self.repo / "file2.txt").write_text("base2\n", encoding="utf-8")
        (self.repo / "delete_me.txt").write_text("keep me\n", encoding="utf-8")
        (self.repo / "unrelated_dirty.txt").write_text("clean\n", encoding="utf-8")
        _run(self.repo, "add", "-A")
        _run(self.repo, "commit", "-m", "B0")
        self.collector = GitCollector(str(self.repo), profile=None)

    def _worktree_tree(self) -> str:
        ok, tree, error = self.collector._temporary_worktree_tree(self.repo)
        self.assertTrue(ok, error)
        return tree

    def test_failed_job_rollback_restores_exact_pre_job_state(self):
        (self.repo / "tracked.txt").write_text("base\ndelta1\n", encoding="utf-8")
        (self.repo / "file2.txt").write_text("base2\ndelta2\n", encoding="utf-8")
        (self.repo / "new_delta2.txt").write_text("created by J2\n", encoding="utf-8")
        (self.repo / "unrelated_dirty.txt").write_text(
            "dirty-before-j3\n", encoding="utf-8"
        )
        (self.repo / "pre_existing_untracked.txt").write_text(
            "untracked-before-j3\n", encoding="utf-8"
        )
        baseline, error = self.collector.capture_baseline()
        self.assertIsNotNone(baseline, error)
        pre_job_tree = self._worktree_tree()
        self.assertEqual(pre_job_tree, baseline.repos[0].worktree_tree)
        (self.repo / "tracked.txt").write_text(
            "base\ndelta1\ndelta3\n", encoding="utf-8"
        )
        (self.repo / "delete_me.txt").unlink()
        (self.repo / "new_delta3.txt").write_text("created by J3\n", encoding="utf-8")
        (self.repo / "file2.txt").write_text(
            "base2\ndelta2\ndelta3-staged\n", encoding="utf-8"
        )
        _run(self.repo, "add", "file2.txt")
        staged = _run(self.repo, "status", "--short").stdout
        self.assertIn("M  file2.txt", staged)
        ok, note = self.collector.rollback(task=None, baseline=baseline)
        self.assertTrue(ok, note)
        self.assertEqual(pre_job_tree, self._worktree_tree())
        self.assertEqual(
            "base\ndelta1\n",
            (self.repo / "tracked.txt").read_text(encoding="utf-8"),
        )
        self.assertEqual(
            "base2\ndelta2\n",
            (self.repo / "file2.txt").read_text(encoding="utf-8"),
        )
        self.assertTrue((self.repo / "new_delta2.txt").exists())
        self.assertEqual(
            "keep me\n",
            (self.repo / "delete_me.txt").read_text(encoding="utf-8"),
        )
        self.assertFalse((self.repo / "new_delta3.txt").exists())
        self.assertEqual(
            "dirty-before-j3\n",
            (self.repo / "unrelated_dirty.txt").read_text(encoding="utf-8"),
        )
        self.assertTrue((self.repo / "pre_existing_untracked.txt").exists())
        _run(self.repo, "add", "-A", "--dry-run")
        status = _run(self.repo, "status", "--short").stdout
        self.assertNotIn("M  file2.txt", status)
        code, index_tree, _ = self.collector._run_in(
            self.repo, ["write-tree"]
        )
        self.assertEqual(0, code)
        self.assertEqual(baseline.repos[0].index_tree, index_tree.strip())

    def test_rollback_refuses_when_head_moved(self):
        baseline, error = self.collector.capture_baseline()
        self.assertIsNotNone(baseline, error)
        (self.repo / "tracked.txt").write_text("base\ncommitted\n", encoding="utf-8")
        _run(self.repo, "add", "-A")
        _run(self.repo, "commit", "-m", "external commit")
        ok, note = self.collector.rollback(task=None, baseline=baseline)
        self.assertFalse(ok)
        self.assertIn("HEAD", note)


if __name__ == "__main__":
    unittest.main()
