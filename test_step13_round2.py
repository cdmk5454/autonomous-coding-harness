"""Step 13 Round-2 remediation contract tests (F10-F15 + residuals).

Covers: crash-between-mutation-and-journal via a REAL disposable child
process, PENDING-path classification from actual bytes (base / intended /
foreign), workspace-wide unresolved-publication fence, recovery under another
canonical owner, unique create-only publication identity, race-checked
restore boundary, journal-backed legacy apply/restore, tester coverage
derivation and identity strictness, host build execution-closure guard,
protected-surface add/modify/delete, batch state truthfulness, and secret
separator completion.
"""
from pathlib import Path
from types import SimpleNamespace
import json
import os
import subprocess
import sys
import unittest
from unittest.mock import patch

import harness_temp
from build_test_runner import BuildTestRunner
from canonical_ownership import canonical_ownership, CanonicalBusy
from job_contract import JobRequest
from qa_quarantine import QACandidateStore, CandidateError, _sha
from runtime_safety import _SECRET_NAME_RE
from task_state import TaskState
from tester import (TestExecutionReceipt, TesterResult, arbitrate_tester_result,
                    execute_test_receipts, _authorized_test_actions)

INTEGRATION_PASS = {
    "success": True, "build_status": "PASS", "test_status": "PASS",
    "review_status": "REVIEW_PASS", "verification_status": "VERIFIED",
    "failure_code": "",
}


def passed_verifier(_changed):
    return dict(INTEGRATION_PASS)


def failed_verifier(_changed):
    return {"success": False, "build_status": "FAIL", "test_status": "FAIL",
            "review_status": "REVIEW_FAIL", "verification_status": "FAILED",
            "failure_code": "X"}


class Fixture:
    def __init__(self, root: Path, files: dict[str, bytes]):
        self.root = root
        self.workspace = root / "workspace"
        self.workspace.mkdir(parents=True)
        for name, payload in files.items():
            target = self.workspace / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(payload)
        for args in (("init", "-q"), ("add", "."),
                     ("-c", "user.name=T", "-c", "user.email=t@i", "-c",
                      "commit.gpgsign=false", "commit", "-qm", "base")):
            subprocess.run(["git", "-C", str(self.workspace), *args],
                           check=True, capture_output=True)
        self.store_root = root / "candidates"
        self.store = QACandidateStore(self.store_root, self.workspace,
                                      SimpleNamespace(id="t"))

    def snapshot_dir(self) -> Path:
        directory = self.root / "snapshots"
        directory.mkdir(exist_ok=True)
        return directory

    def base_snapshot(self, files: dict[str, bytes]) -> Path:
        directory = self.snapshot_dir() / f"snap-{len(list(self.snapshot_dir().iterdir()))}"
        (directory / "files").mkdir(parents=True)
        records = []
        for name, payload in sorted(files.items()):
            digest = _sha(payload)
            target = directory / "files" / digest
            if not target.exists():
                target.write_bytes(payload)
            records.append({"path": name, "sha256": digest,
                            "state": "PRESENT", "snapshot": "files/" + digest})
        manifest = directory / "manifest.json"
        manifest.write_text(json.dumps({
            "kind": "PRE_JOB_SNAPSHOT", "snapshot_id": directory.name,
            "files": records}), encoding="utf-8")
        return manifest

    def candidate(self, candidate_id: str, base_files: dict[str, bytes],
                  delta: dict[str, bytes | None]) -> None:
        base_manifest = str(self.base_snapshot(base_files))
        directory = self.store_root / candidate_id
        (directory / "files").mkdir(parents=True)
        records = []
        before_records = {
            item["path"]: item for item in
            json.loads(Path(base_manifest).read_text(encoding="utf-8"))["files"]}
        for name, payload in sorted(delta.items()):
            before = before_records.get(name)
            before_identity = before.get("sha256", "") if before else "DELETED"
            if payload is None:
                records.append({"path": name, "before_sha256": before_identity,
                                "candidate_sha256": "DELETED", "state": "DELETED",
                                "snapshot": ""})
                continue
            digest = _sha(payload)
            target = directory / "files" / digest
            if not target.exists():
                target.write_bytes(payload)
            records.append({"path": name, "before_sha256": before_identity,
                            "candidate_sha256": digest, "state": "PRESENT",
                            "snapshot": "files/" + digest})
        patch_bytes = b""
        (directory / "candidate.patch").write_bytes(patch_bytes)
        self.store._write_manifest(directory, {
            "schema_version": 2, "candidate_id": candidate_id,
            "state": "QUARANTINED", "created_at": "2026-09-20T00:00:00+00:00",
            "base": {"snapshot_manifest": base_manifest},
            "delta": {"changed_files": sorted(delta),
                      "files": records,
                      "patch_path": "candidate.patch",
                      "diff_sha256": _sha(patch_bytes)}})

    def journal_for(self, publication_id) -> dict:
        return json.loads(
            (self.store_root / "publications" / f"{publication_id}.json")
            .read_text(encoding="utf-8"))


CID = "CAND-AAA111111111111111111111"


class F10F11CanonicalTests(unittest.TestCase):
    def setUp(self):
        self.temp = harness_temp.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.files = {"a.txt": b"base-a\n", "b.txt": b"base-b\n"}
        self.fx = Fixture(self.root, self.files)

    def crash_child(self, store_root, workspace, candidate_id, first_path):
        """Real child process: prepare+apply first path, then hard-exit
        BEFORE the per-path APPLIED journal persistence (F10 window)."""
        script = self.root / "crash_child.py"
        source_root = Path(__file__).resolve().parent
        script.write_text(f"""
import sys, os
sys.path.insert(0, {str(source_root)!r})
from types import SimpleNamespace
from qa_quarantine import QACandidateStore
store = QACandidateStore(r"{store_root}", r"{workspace}", SimpleNamespace(id="t"))
candidate = store.load(r"{candidate_id}")
journal = store.prepare_publication([candidate], purpose="PROMOTE",
    authority={{"job_id": "J", "execution_id": "E", "contract_revision": 1}})
journal["state"] = "APPLYING"
store._persist_publication(journal)
first = [e for e in journal["paths"] if e["path"] == r"{first_path}"][0]
# Mutate the filesystem WITHOUT persisting the per-path APPLIED record.
entry = dict(first)
target = store._workspace_path(entry["path"])
if entry["intended_state"] == "DELETED":
    target.unlink()
else:
    src = store.root / entry["candidate_id"] / entry["snapshot"]
    target.write_bytes(src.read_bytes())
os._exit(9)
""", encoding="utf-8")
        completed = subprocess.run([sys.executable, "-X", "utf8", "-B", str(script)],
                                   capture_output=True, text=True, timeout=60)
        return completed

    def test_f10_crash_between_mutation_and_journal(self):
        self.fx.candidate(CID, self.files, {"a.txt": b"new-a\n", "b.txt": b"new-b\n"})
        self.crash_child(self.fx.store_root, self.fx.workspace, CID, "a.txt")
        # Actual canonical: a.txt mutated (journal still PREPARED/PENDING).
        self.assertEqual(b"new-a\n", (self.fx.workspace / "a.txt").read_bytes())
        journals = sorted((self.fx.store_root / "publications").glob("PUB-*.json"))
        self.assertEqual(1, len(journals))
        publication_id = journals[0].stem
        # Unresolved publication exists -> fence blocks new promotions (F11).
        with self.assertRaises(CandidateError) as fence:
            self.fx.store.promote([CID], passed_verifier)
        self.assertIn("CANONICAL_PUBLICATION_UNRESOLVED", str(fence.exception))
        # Recovery derives disposition from ACTUAL bytes (F10): the PENDING
        # path holding the intended postimage is discovered and restored.
        outcome = self.fx.store.recover_publication(publication_id)
        self.assertEqual("COMPLETED", outcome["state"])
        self.assertEqual(b"base-a\n", (self.fx.workspace / "a.txt").read_bytes())
        self.assertEqual(b"base-b\n", (self.fx.workspace / "b.txt").read_bytes())
        journal = self.fx.journal_for(publication_id)
        self.assertEqual("ROLLED_BACK", journal["disposition"])
        by_path = {e["path"]: e for e in journal["paths"]}
        self.assertEqual("RESTORED_FROM_OWNED", by_path["a.txt"]["mutation_state"])

    def test_f10_pending_path_with_base_and_foreign_variants(self):
        # PENDING + actual base -> treated as safely at base.
        self.fx.candidate(CID, self.files, {"a.txt": b"new-a\n"})
        journal = self.fx.store.prepare_publication([self.fx.store.load(CID)],
                                                    purpose="PROMOTE")
        journal["state"] = "APPLYING"
        self.fx.store._persist_publication(journal)
        outcome = self.fx.store.recover_publication(journal["publication_id"])
        self.assertEqual("COMPLETED", outcome["state"])
        self.assertEqual(b"base-a\n", (self.fx.workspace / "a.txt").read_bytes())
        # PENDING + foreign third version -> fenced, foreign preserved.
        self.fx.candidate("CAND-BBB222222222222222222222",
                          self.files, {"b.txt": b"new-b\n"})
        journal2 = self.fx.store.prepare_publication(
            [self.fx.store.load("CAND-BBB222222222222222222222")], purpose="PROMOTE")
        journal2["state"] = "APPLYING"
        self.fx.store._persist_publication(journal2)
        (self.fx.workspace / "b.txt").write_bytes(b"FOREIGN-THIRD\n")
        with self.assertRaises(CandidateError):
            self.fx.store.recover_publication(journal2["publication_id"])
        self.assertEqual(b"FOREIGN-THIRD\n", (self.fx.workspace / "b.txt").read_bytes())
        fenced = self.fx.journal_for(journal2["publication_id"])
        self.assertEqual("RECONCILIATION_REQUIRED", fenced["state"])
        self.assertEqual(["b.txt"], fenced.get("foreign_paths"))

    def test_f11_unresolved_journal_fences_workspace_wide(self):
        self.fx.candidate(CID, self.files, {"a.txt": b"new-a\n"})
        journal = self.fx.store.prepare_publication([self.fx.store.load(CID)],
                                                    purpose="PROMOTE")
        journal["state"] = "APPLYING"
        self.fx.store._persist_publication(journal)
        # A DIFFERENT candidate touching an UNRELATED path is still blocked.
        self.fx.candidate("CAND-BBB222222222222222222222",
                          self.files, {"b.txt": b"other-b\n"})
        with self.assertRaises(CandidateError) as fence:
            self.fx.store.apply_clean("CAND-BBB222222222222222222222")
        self.assertIn("CANONICAL_PUBLICATION_UNRESOLVED", str(fence.exception))
        with self.assertRaises(CandidateError):
            self.fx.store.restore_base(CID)

    def test_f11_recovery_requires_canonical_ownership(self):
        self.fx.candidate(CID, self.files, {"a.txt": b"new-a\n"})
        journal = self.fx.store.prepare_publication([self.fx.store.load(CID)],
                                                    purpose="PROMOTE")
        journal["state"] = "APPLYING"
        self.fx.store._persist_publication(journal)
        publication_id = journal["publication_id"]
        # A DIFFERENT canonical writer (another thread) holds the workspace:
        # recovery must not mutate anything and must report busy/fenced.
        import threading
        held = threading.Event()
        release = threading.Event()
        def owner():
            with canonical_ownership(self.fx.workspace):
                held.set()
                release.wait(timeout=10)
        thread = threading.Thread(target=owner, daemon=True)
        thread.start()
        self.assertTrue(held.wait(timeout=5))
        try:
            with self.assertRaises(CandidateError) as caught:
                self.fx.store.recover_publication(publication_id)
            self.assertIn("CANONICAL_PROMOTION_BUSY", str(caught.exception))
        finally:
            release.set()
            thread.join(timeout=5)
        self.assertEqual(b"base-a\n", (self.fx.workspace / "a.txt").read_bytes())
        # After release, the same recovery completes.
        outcome = self.fx.store.recover_publication(publication_id)
        self.assertEqual("COMPLETED", outcome["state"])

    def test_f11_publication_identity_unique_and_create_only(self):
        self.fx.candidate(CID, self.files, {"a.txt": b"new-a\n"})
        candidate = self.fx.store.load(CID)
        first = self.fx.store.prepare_publication([candidate], purpose="PROMOTE")
        second = self.fx.store.prepare_publication([candidate], purpose="PROMOTE")
        self.assertNotEqual(first["publication_id"], second["publication_id"])
        # Forced create-only collision fails instead of overwriting.
        with patch.object(QACandidateStore, "_publication_path",
                          return_value=self.fx.store_root / "publications" /
                          f"{first['publication_id']}.json"):
            with self.assertRaises(CandidateError) as caught:
                self.fx.store.prepare_publication([candidate], purpose="PROMOTE")
        self.assertIn("PUBLICATION_ID_COLLISION", str(caught.exception))

    def test_f02_restore_boundary_race_fences_not_overwrites(self):
        self.fx.candidate(CID, self.files, {"a.txt": b"new-a\n"})
        candidate = self.fx.store.load(CID)
        journal = self.fx.store.prepare_publication([candidate], purpose="PROMOTE")
        journal["state"] = "APPLYING"
        self.fx.store._persist_publication(journal)
        for entry in journal["paths"]:
            self.fx.store._apply_publication_entry(journal, entry)
        # A foreign writer replaces the owned postimage between classification
        # and restore: simulate by making the pre-boundary check see foreign
        # bytes via a race-triggered write just before restore.
        original_restore = QACandidateStore._restore_entry_base
        def racing_restore(store_self, j, entry):
            (store_self._workspace_path(entry["path"])).write_bytes(b"RACE-WRITER\n")
            return original_restore(store_self, j, entry)
        with patch.object(QACandidateStore, "_restore_entry_base", racing_restore):
            with self.assertRaises(CandidateError) as caught:
                self.fx.store.unwind_publication(journal, "integration failure")
        self.assertIn("CANONICAL_PUBLICATION_RECONCILIATION_REQUIRED",
                      str(caught.exception))
        # The racing foreign bytes were NOT overwritten.
        self.assertEqual(b"RACE-WRITER\n", (self.fx.workspace / "a.txt").read_bytes())
        fenced = self.fx.journal_for(journal["publication_id"])
        self.assertEqual("RECONCILIATION_REQUIRED", fenced["state"])

    def test_legacy_apply_and_restore_are_journal_backed(self):
        self.fx.candidate(CID, self.files, {"a.txt": b"new-a\n"})
        result = self.fx.store.apply_clean(CID)
        self.assertEqual("APPLIED_CLEAN", result["status"])
        self.assertTrue(result.get("publication_id"))
        journal = self.fx.journal_for(result["publication_id"])
        self.assertEqual("COMPLETED", journal["state"])
        self.assertEqual("APPLIED_NO_INTEGRATION", journal["disposition"])
        # Legacy restore is a reverse publication with foreign preservation.
        (self.fx.workspace / "a.txt").write_bytes(b"FOREIGN-AFTER-APPLY\n")
        with self.assertRaises(CandidateError):
            self.fx.store.restore_base(CID)
        self.assertEqual(b"FOREIGN-AFTER-APPLY\n", (self.fx.workspace / "a.txt").read_bytes())
        journals = sorted((self.fx.store_root / "publications").glob("PUB-*.json"))
        fenced = [json.loads(p.read_text(encoding="utf-8")) for p in journals
                  if json.loads(p.read_text(encoding="utf-8"))["state"]
                  == "RECONCILIATION_REQUIRED"]
        self.assertEqual(1, len(fenced))

    def test_completed_disposition_is_verified_against_actual_bytes(self):
        self.fx.candidate(CID, self.files, {"a.txt": b"new-a\n"})
        result = self.fx.store.promote([CID], passed_verifier)
        # Tamper the canonical after a clean PROMOTED completion.
        (self.fx.workspace / "a.txt").write_bytes(b"TAMPERED\n")
        outcome = self.fx.store.recover_publication(result["publication_id"])
        self.assertEqual("RECONCILIATION_REQUIRED", outcome["state"])
        self.assertEqual("DISPOSITION_MISMATCH_FENCED", outcome["action"])

    def test_delete_and_create_recovery_from_actual_bytes(self):
        files = {"keep.txt": b"keep\n", "gone.txt": b"gone\n"}
        fx = Fixture(self.root / "dc", files)
        cid = "CAND-CCC333333333333333333333"
        fx.candidate(cid, files, {"gone.txt": None, "made.txt": b"made\n"})
        candidate = fx.store.load(cid)
        journal = fx.store.prepare_publication([candidate], purpose="PROMOTE")
        journal["state"] = "APPLYING"
        fx.store._persist_publication(journal)
        # Crash after BOTH mutations, before any APPLIED persist.
        (fx.workspace / "gone.txt").unlink()
        (fx.workspace / "made.txt").write_bytes(b"made\n")
        outcome = fx.store.recover_publication(journal["publication_id"])
        self.assertEqual("COMPLETED", outcome["state"])
        self.assertEqual(b"gone\n", (fx.workspace / "gone.txt").read_bytes())
        self.assertFalse((fx.workspace / "made.txt").exists())


def receipt(task, **overrides):
    values = dict(
        job_id=task.job_id,
        execution_id=str(task.execution_id or ""),
        tester_attempt_id=str(getattr(task, "tester_attempt_id", "") or ""),
        candidate_id=str(task.candidate_id or ""),
        test_scope_hash=str(task.test_scope_hash or ""),
        command="python app/test_x.py",
        required_tests=("app/test_x.py",),
        exit_code=0,
        status="PASS",
        tool_identity="python:ab12",
        workspace="D:/fixture",
        started_at=1.0,
        finished_at=2.0,
    )
    values.update(overrides)
    return TestExecutionReceipt(**values)


def tester_task(**overrides):
    task = TaskState(task_id="T13", job_id="J13", requirement="r",
                     working_dir="D:/fixture", task_mode="MODIFICATION")
    task.execution_id = "E13"
    task.tester_attempt_id = "TA13"
    task.candidate_id = "CAND13"
    task.test_scope_hash = "scope13"
    task.profile_dir = ""
    task.test_plan = {"mandatory": True, "required_tests": ["app/test_x.py"],
                      "command": "python app/test_x.py"}
    for key, value in overrides.items():
        setattr(task, key, value)
    return task


class F12F13TesterTests(unittest.TestCase):
    def test_noop_command_cannot_claim_unrelated_required_test(self):
        task = tester_task()
        task.test_plan = {"mandatory": True,
                          "required_tests": ["app/required_test.py"],
                          "command": 'python -c "pass"'}
        actions = _authorized_test_actions(task)
        self.assertEqual(1, len(actions))
        self.assertEqual("UNBOUND", actions[0]["source"])
        self.assertEqual("", actions[0]["command"])
        # Even with a model PASS and a plausible receipt-shaped entry, the
        # required test was never bound to an authorized execution.
        tr = arbitrate_tester_result(task, TesterResult("PASS", "ok"), [])
        self.assertEqual("ERROR", tr.status)
        self.assertIn("TESTER_RECEIPTS_REQUIRED", tr.summary)

    def test_coverage_matrix(self):
        task = tester_task()
        task.test_plan = {"mandatory": True,
                          "required_tests": ["app/t1.py", "app/t2.py"],
                          "test_commands": {"app/t1.py": "python app/t1.py",
                                            "app/t2.py": "python app/t2.py"}}
        r1 = receipt(task, command="python app/t1.py", required_tests=("app/t1.py",))
        r2 = receipt(task, command="python app/t2.py", required_tests=("app/t2.py",))
        both_pass = arbitrate_tester_result(task, TesterResult("PASS", "ok"), [r1, r2])
        self.assertEqual("PASS", both_pass.status)
        # One required FAIL vetoes the model PASS (aggregate veto).
        r2_fail = receipt(task, command="python app/t2.py",
                          required_tests=("app/t2.py",), exit_code=2, status="FAIL")
        vetoed = arbitrate_tester_result(task, TesterResult("PASS", "ok"), [r1, r2_fail])
        self.assertEqual("FAIL", vetoed.status)
        # Missing one required action -> coverage incomplete ERROR.
        missing = arbitrate_tester_result(task, TesterResult("PASS", "ok"), [r1])
        self.assertEqual("ERROR", missing.status)
        self.assertEqual("TESTER_RECEIPT_COVERAGE_INCOMPLETE", missing.summary)

    def test_receipt_identity_strictness(self):
        task = tester_task()
        for overrides in ({"candidate_id": ""}, {"command": "  "},
                          {"tool_identity": ""}, {"workspace": ""},
                          {"tester_attempt_id": "OLD"},
                          {"execution_id": "PREV"},
                          {"test_scope_hash": "other"}):
            with self.subTest(overrides=overrides):
                tr = arbitrate_tester_result(
                    task, TesterResult("PASS", "ok"), [receipt(task, **overrides)])
                self.assertEqual("ERROR", tr.status, overrides)

    def test_wrong_workspace_and_command_rejected(self):
        task = tester_task()
        wrong_ws = arbitrate_tester_result(
            task, TesterResult("PASS", "ok"),
            [receipt(task, workspace="D:/other")])
        self.assertEqual("TESTER_RECEIPTS_REQUIRED", wrong_ws.summary)
        wrong_cmd = arbitrate_tester_result(
            task, TesterResult("PASS", "ok"),
            [receipt(task, command="python app/other.py")])
        # Wrong command still binds identity but covers the wrong test.
        self.assertIn(wrong_cmd.summary,
                      {"TESTER_RECEIPT_COVERAGE_INCOMPLETE", "TESTER_RECEIPTS_REQUIRED"})

    def test_genuine_command_execution_produces_receipt_with_filtered_env(self):
        with harness_temp.TemporaryDirectory() as tmp:
            work = Path(tmp)
            (work / "app").mkdir()
            (work / "app" / "test_x.py").write_text(
                "import os,sys\n"
                # Ambient secret must NOT leak into the host test boundary.
                "leaked = os.environ.get('KKM_TEST_SECRET_PROBE')\n"
                "marker = work = None\n"
                "sys.exit(0 if leaked is None else 5)\n",
                encoding="utf-8")
            task = tester_task()
            import os as _os
            _os.environ["KKM_TEST_SECRET_PROBE"] = "synthetic-secret-value"
            try:
                receipts = execute_test_receipts(task, str(work))
            finally:
                _os.environ.pop("KKM_TEST_SECRET_PROBE", None)
            self.assertEqual(1, len(receipts))
            self.assertEqual(0, receipts[0].exit_code)
            self.assertEqual("PASS", receipts[0].status)
            # Failing test produces observed FAIL.
            (work / "app" / "test_x.py").write_text("raise SystemExit(3)\n",
                                                    encoding="utf-8")
            failing = execute_test_receipts(tester_task(), str(work))
            self.assertEqual(3, failing[0].exit_code)
            self.assertEqual("FAIL", failing[0].status)


class F14F15PolicyTests(unittest.TestCase):
    def setUp(self):
        self.temp = harness_temp.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.workspace = self.root / "workspace"
        (self.workspace / "app").mkdir(parents=True)
        (self.workspace / "app" / "build.py").write_text("print('build')\n",
                                                         encoding="utf-8")

    def test_executed_project_script_in_control_closure(self):
        runner = BuildTestRunner(str(self.workspace))
        with patch.object(BuildTestRunner, "_detect_build_targets",
                          return_value=[("app", self.workspace / "app",
                                         [sys.executable, "app/build.py"])]):
            with patch.object(BuildTestRunner, "_protected_surface_snapshot",
                              return_value={"root": "x", "files": {}}):
                blocked = runner.run(changed_files=["app/build.py"],
                                     target_modules=["app"])
        self.assertEqual("FAIL", blocked.build_status)
        self.assertIn("BUILD_CONTROL_SURFACE_MODIFIED", blocked.error)
        # Authorized exact script may run.
        authorized = BuildTestRunner(str(self.workspace))
        with patch.object(BuildTestRunner, "_detect_build_targets",
                          return_value=[("app", self.workspace / "app",
                                         [sys.executable, "app/build.py"])]):
            with patch.object(BuildTestRunner, "_protected_surface_snapshot",
                              return_value={"root": "x", "files": {}}):
                allowed = authorized.run(changed_files=["app/build.py"],
                                         target_modules=["app"],
                                         evidence_context={"host_build_authorized": "True"})
        self.assertNotIn("BUILD_CONTROL_SURFACE_MODIFIED", allowed.error or "")

    def test_protected_surface_detects_add_modify_delete(self):
        guard_root = self.root / "guarded"
        guard_root.mkdir()
        (guard_root / "a.py").write_text("one\n", encoding="utf-8")
        (guard_root / "b.py").write_text("two\n", encoding="utf-8")
        runner = BuildTestRunner(str(self.workspace))
        before = runner._protected_surface_snapshot(guard_root)
        (guard_root / "a.py").write_text("one-changed\n", encoding="utf-8")  # modify
        (guard_root / "b.py").unlink()                                       # delete
        (guard_root / "c.py").write_text("new\n", encoding="utf-8")          # add
        after = runner._protected_surface_snapshot(guard_root)
        delta = BuildTestRunner.compare_protected_surfaces(before, after)
        self.assertEqual(["a.py"], delta["modified"])
        self.assertEqual(["b.py"], delta["deleted"])
        self.assertEqual(["c.py"], delta["added"])

    def test_batch_state_projection_truthful(self):
        from business_report import DISPOSITION_WORDING
        from isolated_promotion import commit_stage_for_policy
        stage = commit_stage_for_policy("BATCH_FINAL_COMMIT")
        self.assertEqual("DEFERRED_TO_BATCH_FINAL", stage["commit_status"])
        self.assertIn("BATCH_COMMIT_PENDING", DISPOSITION_WORDING)
        self.assertIn("커밋 대기", DISPOSITION_WORDING["BATCH_COMMIT_PENDING"])
        # job_queue accept-gate accepts the staged disposition only with the
        # deferred commit status.
        import job_queue
        source = pathlib_read(Path(__file__).resolve().parent / "job_queue.py")
        self.assertIn('"BATCH_COMMIT_PENDING"', source)

    def test_secret_separator_matrix(self):
        for name in ("PAT", "GITHUB_PAT", "GITHUB.PAT", "APP:API_KEY", "API.KEY",
                     "APP.CLIENT_SECRET", "MY-API-KEY", "API_KEY",
                     "ACCESS_TOKEN", "PASSWORD", "CLIENT_SECRET", "PRIVATE_KEY"):
            self.assertTrue(_SECRET_NAME_RE.search(name), name)
        for name in ("PATH", "PATHEXT", "PATCH", "FEATURE_PATCH_ENABLED"):
            self.assertFalse(_SECRET_NAME_RE.search(name), name)


def pathlib_read(path):
    return Path(path).read_text(encoding="utf-8")


if __name__ == "__main__":
    unittest.main()
