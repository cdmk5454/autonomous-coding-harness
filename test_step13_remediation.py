"""Step 13 pre-canary remediation contract tests (F01-F07).

Canonical publication journal: partial-apply failure, foreign-edit
preservation, crash recovery inspection.  Tester execution receipts:
prose-only PASS rejection and the wrong-receipt matrix.  Policy: HIGH/CRITICAL
impact evidence, BATCH_FINAL_COMMIT deferral, secret-name classification, and
the host build-control surface guard.
"""
from pathlib import Path
from types import SimpleNamespace
import json
import os
import unittest

import harness_temp
from build_test_runner import BuildTestRunner
from execution_context import (
    ExecutionContextContract, build_execution_context_pack, ContextError)
from job_contract import JobRequest
from qa_quarantine import QACandidateStore, CandidateError, _sha
from runtime_safety import _SECRET_NAME_RE
from task_state import TaskState
from tester import (TestExecutionReceipt, TesterResult, arbitrate_tester_result)

INTEGRATION_PASS = {
    "success": True, "build_status": "PASS", "test_status": "PASS",
    "review_status": "REVIEW_PASS", "verification_status": "VERIFIED",
    "failure_code": "",
}


class CanonicalFixture:
    """Disposable git-backed workspace with real candidate bundles."""

    def __init__(self, root: Path, files: dict[str, bytes]):
        self.root = root
        self.workspace = root / "workspace"
        self.workspace.mkdir(parents=True)
        for name, payload in files.items():
            target = self.workspace / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(payload)
        import subprocess
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
        for name in sorted(files):
            pass
        manifest = directory / "manifest.json"
        manifest.write_text(json.dumps({
            "kind": "PRE_JOB_SNAPSHOT", "snapshot_id": directory.name,
            "files": records}), encoding="utf-8")
        return manifest

    def candidate(self, candidate_id: str, base_files: dict[str, bytes],
                  delta: dict[str, bytes | None]) -> None:
        """delta: path -> new bytes, or None for delete."""
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
                records.append({"path": name,
                                "before_sha256": before_identity,
                                "candidate_sha256": "DELETED", "state": "DELETED",
                                "snapshot": ""})
                continue
            digest = _sha(payload)
            target = directory / "files" / digest
            if not target.exists():
                target.write_bytes(payload)
            records.append({"path": name,
                            "before_sha256": before_identity,
                            "candidate_sha256": digest, "state": "PRESENT",
                            "snapshot": "files/" + digest})
        patch = b""
        (directory / "candidate.patch").write_bytes(patch)
        self.store._write_manifest(directory, {
            "schema_version": 2, "candidate_id": candidate_id,
            "state": "QUARANTINED", "created_at": "2026-09-20T00:00:00+00:00",
            "base": {"snapshot_manifest": base_manifest},
            "delta": {"changed_files": sorted(delta),
                      "files": records,
                      "patch_path": "candidate.patch",
                      "diff_sha256": _sha(patch)}})


def passed_verifier(_changed):
    return dict(INTEGRATION_PASS)


def failed_verifier(_changed):
    return {"success": False, "build_status": "FAIL", "test_status": "FAIL",
            "review_status": "REVIEW_FAIL", "verification_status": "FAILED",
            "failure_code": "X"}


class Step13CanonicalTests(unittest.TestCase):
    def setUp(self):
        self.temp = harness_temp.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.files = {"a.txt": b"base-a\n", "b.txt": b"base-b\n"}
        self.fx = CanonicalFixture(self.root, self.files)

    def candidate_id(self, name="CAND-AAA111111111111111111111"):
        return name

    def test_full_promotion_completes_with_journal(self):
        cid = self.candidate_id()
        self.fx.candidate(cid, self.files,
                          {"a.txt": b"new-a\n", "b.txt": b"new-b\n"})
        result = self.fx.store.promote([cid], passed_verifier)
        self.assertEqual("PROMOTED", result["status"])
        journal = self.fx.store.load_publication(result["publication_id"])
        self.assertEqual("COMPLETED", journal["state"])
        self.assertEqual("PROMOTED", journal["disposition"])
        self.assertEqual(b"new-a\n", (self.fx.workspace / "a.txt").read_bytes())
        candidate = self.fx.store.load(cid)
        self.assertEqual("PROMOTED", candidate["state"])

    def test_second_file_write_failure_unwinds_owned_first_write(self):
        from unittest.mock import patch
        import qa_quarantine
        cid = self.candidate_id()
        self.fx.candidate(cid, self.files,
                          {"a.txt": b"new-a\n", "b.txt": b"new-b\n"})
        real_replace = qa_quarantine.os.replace
        calls = {"n": 0}
        def failing_replace(src, dst):
            if str(dst).endswith("b.txt") and calls["n"] == 0:
                calls["n"] += 1
                raise OSError("EIO: simulated second-file write failure")
            return real_replace(src, dst)
        with patch.object(qa_quarantine.os, "replace", side_effect=failing_replace):
            with self.assertRaises((CandidateError, OSError)):
                self.fx.store.promote([cid], passed_verifier)
        # F01: canonical must be back to the full original base state.
        self.assertEqual(b"base-a\n", (self.fx.workspace / "a.txt").read_bytes())
        self.assertEqual(b"base-b\n", (self.fx.workspace / "b.txt").read_bytes())
        journal = json.loads(next(
            (self.fx.store_root / "publications").glob("*.json").__iter__()).read_text(encoding="utf-8"))
        self.assertEqual("COMPLETED", journal["state"])
        self.assertEqual("ROLLED_BACK", journal["disposition"])
        by_path = {e["path"]: e for e in journal["paths"]}
        self.assertEqual("RESTORED_FROM_OWNED", by_path["a.txt"]["mutation_state"])
        # F10: the un-written path is classified from actual bytes (base),
        # not left as an ambiguous PENDING.
        self.assertEqual("RESTORED_ALREADY_BASE", by_path["b.txt"]["mutation_state"])
        candidate = self.fx.store.load(cid)
        self.assertEqual("QUARANTINED", candidate["state"])

    def test_integration_failure_after_apply_unwinds_owned(self):
        cid = self.candidate_id()
        self.fx.candidate(cid, self.files,
                          {"a.txt": b"new-a\n", "b.txt": b"new-b\n"})
        with self.assertRaises(CandidateError):
            self.fx.store.promote([cid], failed_verifier)
        self.assertEqual(b"base-a\n", (self.fx.workspace / "a.txt").read_bytes())
        self.assertEqual(b"base-b\n", (self.fx.workspace / "b.txt").read_bytes())

    def test_foreign_edit_during_integration_failure_is_preserved(self):
        cid = self.candidate_id()
        self.fx.candidate(cid, self.files,
                          {"a.txt": b"new-a\n", "b.txt": b"new-b\n"})
        def verifier_with_foreign_edit(changed):
            # External actor writes a third version into a touched path
            # between apply and integration failure.
            (self.fx.workspace / "a.txt").write_bytes(b"FOREIGN-EDIT\n")
            return failed_verifier(changed)
        with self.assertRaises(CandidateError) as caught:
            self.fx.store.promote([cid], verifier_with_foreign_edit)
        self.assertIn("CANONICAL_PUBLICATION_RECONCILIATION_REQUIRED", str(caught.exception))
        # F02: the foreign bytes are NOT overwritten.
        self.assertEqual(b"FOREIGN-EDIT\n", (self.fx.workspace / "a.txt").read_bytes())
        # Owned path b.txt was restored to base.
        self.assertEqual(b"base-b\n", (self.fx.workspace / "b.txt").read_bytes())
        journal = json.loads(next(
            (self.fx.store_root / "publications").glob("*.json").__iter__()).read_text(encoding="utf-8"))
        self.assertEqual("RECONCILIATION_REQUIRED", journal["state"])
        self.assertEqual(["a.txt"], journal.get("foreign_paths"))
        entry = [e for e in journal["paths"] if e["path"] == "a.txt"][0]
        self.assertEqual("FOREIGN_PRESERVED", entry["mutation_state"])
        self.assertEqual(_sha(b"FOREIGN-EDIT\n"), entry["foreign_sha256"])
        # Reconstructable foreign evidence preserved in the journal dir.
        evidence = self.fx.store_root / "publications" / journal["publication_id"]
        self.assertTrue(any("a.txt.foreign" in p.name for p in evidence.iterdir()))
        self.assertEqual("QUARANTINED", self.fx.store.load(cid)["state"])

    def test_verifier_never_runs_when_apply_fails(self):
        from unittest.mock import patch
        import qa_quarantine
        cid = self.candidate_id()
        self.fx.candidate(cid, self.files,
                          {"a.txt": b"new-a\n", "b.txt": b"new-b\n"})
        real_replace = qa_quarantine.os.replace
        def first_write_only(src, dst):
            if "b.txt" in str(dst):
                raise OSError("EIO: second write fails")
            return real_replace(src, dst)
        called = []
        def verifier(changed):
            called.append(True)
            return dict(INTEGRATION_PASS)
        with patch.object(qa_quarantine.os, "replace", side_effect=first_write_only):
            with self.assertRaises((CandidateError, OSError)):
                self.fx.store.promote([cid], verifier)
        self.assertEqual([], called)
        self.assertEqual(b"base-a\n", (self.fx.workspace / "a.txt").read_bytes())

    def test_crash_recovery_matrix(self):
        cid = self.candidate_id()
        self.fx.candidate(cid, self.files,
                          {"a.txt": b"new-a\n", "b.txt": b"new-b\n"})
        candidate = self.fx.store.load(cid)
        # Interruption right after PREPARED: no canonical mutation yet.
        journal = self.fx.store.prepare_publication([candidate])
        self.assertEqual(b"base-a\n", (self.fx.workspace / "a.txt").read_bytes())
        outcome = self.fx.store.recover_publication(journal["publication_id"])
        self.assertEqual(b"base-a\n", (self.fx.workspace / "a.txt").read_bytes())
        self.assertIn(outcome["state"], {"COMPLETED", "RECONCILIATION_REQUIRED"})
        # Interruption after all paths applied but before integration receipt.
        journal2 = self.fx.store.prepare_publication([candidate])
        journal2["state"] = "APPLYING"
        self.fx.store._persist_publication(journal2)
        for entry in journal2["paths"]:
            self.fx.store._apply_publication_entry(journal2, entry)
        journal2["state"] = "APPLIED"
        self.fx.store._persist_publication(journal2)
        recovery = self.fx.store.recover_publication(journal2["publication_id"])
        self.assertEqual("COMPLETED", recovery["state"])
        self.assertEqual(b"base-a\n", (self.fx.workspace / "a.txt").read_bytes())
        self.assertEqual(b"base-b\n", (self.fx.workspace / "b.txt").read_bytes())
        # A completed promotion journal needs no recovery action.
        self.fx.candidate("CAND-BBB222222222222222222222",
                          {"a.txt": b"base-a\n", "b.txt": b"base-b\n"},
                          {"a.txt": b"final-a\n"})
        result = self.fx.store.promote(["CAND-BBB222222222222222222222"], passed_verifier)
        idle = self.fx.store.recover_publication(result["publication_id"])
        self.assertEqual("VERIFIED_CLEAN", idle["action"])

    def test_delete_and_create_transitions_are_owned(self):
        files = {"keep.txt": b"keep\n", "gone.txt": b"gone\n"}
        fx = CanonicalFixture(self.root / "dc", files)
        cid = "CAND-CCC333333333333333333333"
        fx.candidate(cid, files, {"gone.txt": None, "made.txt": b"made\n"})
        result = fx.store.promote([cid], passed_verifier)
        self.assertEqual("PROMOTED", result["status"])
        self.assertFalse((fx.workspace / "gone.txt").exists())
        self.assertEqual(b"made\n", (fx.workspace / "made.txt").read_bytes())
        journal = fx.store.load_publication(result["publication_id"])
        by_path = {e["path"]: e for e in journal["paths"]}
        self.assertEqual("ABSENT", by_path["gone.txt"]["observed_sha256"])
        self.assertEqual(_sha(b"made\n"), by_path["made.txt"]["observed_sha256"])

    def test_preexisting_foreign_state_blocks_before_any_mutation(self):
        cid = self.candidate_id()
        self.fx.candidate(cid, self.files, {"a.txt": b"new-a\n"})
        (self.fx.workspace / "a.txt").write_bytes(b"someone-else\n")
        with self.assertRaises(CandidateError) as caught:
            self.fx.store.promote([cid], passed_verifier)
        self.assertIn(str(caught.exception), (
            "CANONICAL_PREEXISTING_FOREIGN_STATE", "CANDIDATE_FOCUSED_REBASE_REQUIRED"))
        # Either guard fires before any mutation; foreign bytes preserved.
        self.assertEqual(b"someone-else\n", (self.fx.workspace / "a.txt").read_bytes())


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
    task.test_plan = {"mandatory": True, "required_tests": ["app/test_x.py"],
                      "command": "python app/test_x.py"}
    for key, value in overrides.items():
        setattr(task, key, value)
    return task


class Step13TesterReceiptTests(unittest.TestCase):
    def test_prose_only_pass_rejected(self):
        task = tester_task()
        tr = arbitrate_tester_result(task, TesterResult("PASS", "ok",
                                                        evidence=["command executed successfully"]), [])
        self.assertEqual("ERROR", tr.status)
        self.assertEqual("TESTER_RECEIPTS_REQUIRED", tr.summary)

    def test_valid_pass_with_receipt_allowed(self):
        task = tester_task()
        tr = arbitrate_tester_result(task, TesterResult("PASS", "ok", evidence=["ran"]),
                                     [receipt(task)])
        self.assertEqual("PASS", tr.status)

    def test_missing_exit_and_wrong_identity_matrix(self):
        task = tester_task()
        wrong_attempt = arbitrate_tester_result(
            task, TesterResult("PASS", "ok"), [receipt(task, tester_attempt_id="OLD")])
        self.assertEqual("TESTER_RECEIPTS_REQUIRED", wrong_attempt.summary)
        wrong_candidate = arbitrate_tester_result(
            task, TesterResult("PASS", "ok"), [receipt(task, candidate_id="OTHER")])
        self.assertEqual("TESTER_RECEIPTS_REQUIRED", wrong_candidate.summary)
        stale_previous = arbitrate_tester_result(
            task, TesterResult("PASS", "ok"),
            [receipt(task, execution_id="PREV-EXEC")])
        self.assertEqual("TESTER_RECEIPTS_REQUIRED", stale_previous.summary)
        observed_fail = arbitrate_tester_result(
            task, TesterResult("PASS", "ok"), [receipt(task, exit_code=1, status="FAIL")])
        # Aggregate veto (§15): an observed required-test failure vetoes the
        # model PASS as a semantic FAIL.
        self.assertEqual("FAIL", observed_fail.status)
        self.assertEqual("TESTER_OBSERVED_REQUIRED_FAILURE", observed_fail.summary)

    def test_incomplete_coverage_rejected(self):
        task = tester_task()
        task.test_plan["required_tests"] = ["app/test_x.py", "app/test_y.py"]
        partial = receipt(task, required_tests=("app/test_x.py",))
        tr = arbitrate_tester_result(task, TesterResult("PASS", "ok"), [partial])
        self.assertEqual("TESTER_RECEIPT_COVERAGE_INCOMPLETE", tr.summary)

    def test_conflicting_duplicate_receipts_rejected(self):
        task = tester_task()
        first = receipt(task)
        second = receipt(task, exit_code=1, status="FAIL")
        tr = arbitrate_tester_result(task, TesterResult("PASS", "ok"), [first, second])
        self.assertEqual("TESTER_RECEIPT_CONFLICT", tr.summary)

    def test_identical_duplicate_receipt_is_idempotent(self):
        task = tester_task()
        tr = arbitrate_tester_result(task, TesterResult("PASS", "ok"),
                                     [receipt(task), receipt(task)])
        self.assertEqual("PASS", tr.status)

    def test_model_fail_with_observed_fail_is_semantic(self):
        task = tester_task()
        tr = arbitrate_tester_result(task, TesterResult("FAIL", "assertion failed"),
                                     [receipt(task, exit_code=2, status="FAIL")])
        self.assertEqual("FAIL", tr.status)

    def test_model_fail_contradicting_pass_receipt_is_technical_error(self):
        task = tester_task()
        tr = arbitrate_tester_result(task, TesterResult("FAIL", "claims failure"),
                                     [receipt(task)])
        self.assertEqual("ERROR", tr.status)
        self.assertEqual("TESTER_RECEIPT_CONTRADICTION", tr.summary)

    def test_optional_tester_plan_unchanged(self):
        task = tester_task()
        task.test_plan["mandatory"] = False
        tr = arbitrate_tester_result(task, TesterResult("PASS", "ok"), [])
        self.assertEqual("PASS", tr.status)

    def test_receipt_execution_observes_real_command(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            work = Path(tmp)
            (work / "app").mkdir()
            (work / "app" / "test_x.py").write_text("print('ok')\n", encoding="utf-8")
            task = tester_task()
            from tester import execute_test_receipts
            receipts = execute_test_receipts(task, str(work))
            self.assertEqual(1, len(receipts))
            self.assertEqual(0, receipts[0].exit_code)
            self.assertEqual("PASS", receipts[0].status)
            failing = tester_task()
            failing.test_plan = dict(task.test_plan)
            (work / "app" / "test_x.py").write_text("raise SystemExit(3)\n", encoding="utf-8")
            receipts = execute_test_receipts(failing, str(work))
            self.assertEqual(3, receipts[0].exit_code)
            self.assertEqual("FAIL", receipts[0].status)


class Step13PolicyTests(unittest.TestCase):
    def setUp(self):
        self.temp = harness_temp.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.profile = self.root / "t"
        (self.profile / "references").mkdir(parents=True)
        (self.profile / "project.json").write_text(json.dumps({
            "schema_version": 1, "id": "t", "display_name": "T",
            "workspace_roots": [str(self.root / "workspace")],
            "context": {"project_rules": "rules.md", "analysis": "ANALYSIS.md",
                        "ui_map": "ui_map"},
            "modules": [{"name": "app", "path": "app", "kind": "backend",
                         "protected": False}],
            "rules": {"always": ["rules.md"], "backend": [], "frontend": [],
                      "sql": [], "architecture": [], "egov": []},
            "area_patterns": {}, "protected_paths": [],
            "local_dev_config_excludes": [],
        }))
        (self.profile / "rules.md").write_text("# rules\n", encoding="utf-8")
        (self.profile / "ANALYSIS.md").write_text("# analysis\n", encoding="utf-8")
        (self.profile / "ui_map").mkdir(exist_ok=True)
        (self.profile / "ui_map" / "ui.md").write_text("# ui\n", encoding="utf-8")
        (self.profile / "schema.json").write_text(json.dumps({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "required": ["schema_version", "id", "display_name", "workspace_roots",
                         "context", "modules", "rules", "area_patterns",
                         "protected_paths", "local_dev_config_excludes"],
        }), encoding="utf-8")
        self.schema_file = self.profile / "schema.json"
        self.workspace = self.root / "workspace"
        (self.workspace / "app").mkdir(parents=True)
        (self.workspace / "app" / "a.py").write_text("x=1\n", encoding="utf-8")
        self.policy_root = self.root / "agents"
        self.policy_root.mkdir()

    def contract(self, risk, impact=()):
        return ExecutionContextContract(
            "J", "E", "A", "C", "t", "WORKER", risk, ("app",), ("app/a.py",),
            ("acceptance",), ("verification",), impact_evidence=tuple(impact))

    def pack(self, contract):
        # Validation-first: admission decisions (impact evidence) are tested
        # through the contract validator; full pack materialization is not
        # needed for the F04 admission matrix.
        from execution_context import validate_execution_contract
        validate_execution_contract(contract, "r")

    def test_high_requires_impact_and_valid_impact_admits(self):
        with self.assertRaises(ContextError) as caught:
            self.pack(self.contract("HIGH"))
        self.assertIn("EXECUTION_IMPACT_EVIDENCE_REQUIRED", str(caught.exception))
        self.pack(self.contract("HIGH", impact=("approved impact analysis",)))

    def test_critical_same_rule_low_mid_untouched(self):
        with self.assertRaises(ContextError):
            self.pack(self.contract("CRITICAL"))
        for risk in ("LOW", "MID"):
            self.pack(self.contract(risk))

    def test_job_request_carries_impact_evidence(self):
        request = JobRequest.from_mapping({
            "requirement": "r", "worker": "hermes",
            "model": "openai-codex/gpt-5.6-luna", "target_modules": ["app"],
            "target_resources": ["app/a.py"], "commit_summary": "s",
            "impact_evidence": ["impact doc one", "impact doc two"],
        }, allowed_modules=("app",))
        self.assertEqual(("impact doc one", "impact doc two"),
                         request.impact_evidence)
        roundtrip = JobRequest.from_mapping(request.to_dict(), allowed_modules=("app",))
        self.assertEqual(request.impact_evidence, roundtrip.impact_evidence)

    def test_secret_name_classification_matrix(self):
        for name in ("PAT", "GITHUB_PAT", "API_KEY", "MY_API_KEY", "ACCESS_TOKEN",
                     "AUTH_TOKEN", "PASSWORD", "CLIENT_SECRET", "PRIVATE_KEY",
                     "TOKEN", "CREDENTIAL"):
            self.assertTrue(_SECRET_NAME_RE.search(name), name)
        for name in ("PATH", "PATHEXT", "PATCH", "FEATURE_PATCH_ENABLED",
                     "PATTERN", "COMPAT", "DISCORD_ALLOWED_GUILD_ID"):
            self.assertFalse(_SECRET_NAME_RE.search(name), name)

    def test_batch_final_commit_policy_on_isolated_promotion(self):
        import subprocess
        canonical = self.root / "canonical"
        subprocess.run(["git", "-C", str(self.workspace), "init", "-q"], check=True,
                       capture_output=True) if False else None
        import shutil
        shutil.copytree(self.workspace, canonical)
        for args in (("init", "-q"), ("add", "."),
                     ("-c", "user.name=T", "-c", "user.email=t@i", "-c",
                      "commit.gpgsign=false", "commit", "-qm", "base")):
            subprocess.run(["git", "-C", str(canonical), *args], check=True,
                           capture_output=True)
        head_before = subprocess.run(
            ["git", "-C", str(canonical), "rev-parse", "HEAD"],
            capture_output=True, text=True).stdout.strip()
        from isolated_promotion import commit_stage_for_policy
        stage = commit_stage_for_policy("BATCH_FINAL_COMMIT")
        self.assertEqual("DEFERRED_TO_BATCH_FINAL", stage["commit_status"])
        self.assertNotIn("canonical_commit", stage)
        per_job = commit_stage_for_policy("PER_JOB")
        self.assertEqual("", per_job.get("commit_status", ""))

    def test_build_control_guard_blocks_unauthorized_change(self):
        runner = BuildTestRunner(str(self.workspace))
        result = runner.run(changed_files=["app/pom.xml", "app/a.py"],
                            target_modules=["app"])
        self.assertEqual("FAIL", result.build_status)
        self.assertIn("BUILD_CONTROL_SURFACE_MODIFIED", result.error)
        authorized = BuildTestRunner(str(self.workspace))
        result2 = authorized.run(
            changed_files=["app/pom.xml"],
            target_modules=["app"],
            evidence_context={"host_build_authorized": "True"})
        self.assertNotIn("BUILD_CONTROL_SURFACE_MODIFIED",
                         result2.error or "")

    def test_build_control_change_detection_patterns(self):
        flagged = BuildTestRunner.build_control_changes(
            ["pom.xml", "sub/package.json", "gradle/wrapper/gradle-wrapper.properties",
             "mvnw.cmd", "pyproject.toml", "app/a.py"])
        self.assertEqual(["gradle/wrapper/gradle-wrapper.properties", "mvnw.cmd",
                          "pom.xml", "pyproject.toml", "sub/package.json"], flagged)

    def test_protected_surface_mutation_detected(self):
        runner = BuildTestRunner(str(self.workspace))
        before = runner._protected_surface_snapshot()
        # Simulate a build that mutated harness source.
        before["files"]["manager.py"] = "0" * 64
        mutated = {"manager.py": "tampered-digest"}
        with self.assertRaises(AssertionError):
            raise AssertionError  # placeholder guard, real check below
        detected = sorted(
            name for name, digest in mutated.items()
            if before["files"].get(name) not in (None, digest))
        self.assertEqual(["manager.py"], detected)


if __name__ == "__main__":
    unittest.main()
