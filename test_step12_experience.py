"""Step 12 experience-projection contract tests.

Covers: successful / semantic-FIX / infrastructure / QA hold+accept+reject /
user-waiver records, technical-vs-semantic classification, human intervention,
failure fingerprints, secret exclusion, UNVERIFIED preservation, no-mutation
safety, and deterministic repeated export.
"""
from pathlib import Path
import json
import sqlite3
import unittest

import harness_temp
from control_repository import ControlRepository
from execution_experience import (classify_retry, export, records_from_run,
                                  source_identity, step13_metrics,
                                  failure_fingerprint)
from job_contract import JobRequest, EXECUTION_CONTEXT_KEYS
from job_queue import JobStore


def _env_context():
    keys = EXECUTION_CONTEXT_KEYS - {"profile_id", "profile_schema_version"}
    return {"profile_id": "t", "profile_schema_version": 1,
            **{k: "a" * 64 for k in keys}}


def _request(client_job_id):
    return JobRequest(client_job_id=client_job_id, requirement="r",
                      worker="hermes", model="openai-codex/gpt-5.6-luna",
                      reasoning_effort="medium", target_modules=("app",),
                      target_resources=("app/app.py",), commit_summary="s12",
                      max_outer_attempts=2)


class Fixture:
    """A minimal preserved-run directory in the real evidence layout."""

    def __init__(self, root: Path):
        self.root = root
        self.run = root / "run-alpha"
        self.fixture = self.run / "fixture-alpha"
        control = self.fixture / ".control"
        control.mkdir(parents=True)
        repo = ControlRepository(control / "control.sqlite3", create=True)
        repo.bootstrap(JobStore._empty_queue(), {}, {})
        (control / "sqlite-authority.json").write_text(
            json.dumps({"authority": "SQLITE", "schema_version": 6}))
        self.repo = repo
        self.store = JobStore(control, repository=repo)
        jobs, _ = self.store.enqueue_batch("s12", [_request("A")],
                                           execution_context=_env_context())
        self.job_id = jobs[0]["job_id"]
        self.job_results = []

    def add_execution(self, *, contract_revision=1, policy_sha="p" * 64):
        with self.repo.hold():
            self.repo._connection.execute(
                "INSERT INTO executions VALUES(?,?,?,?,?,?,?,?,?,?,?,?,NULL)",
                ("EXEC-1", self.job_id, "A", contract_revision, "MATERIALIZED",
                 "prof", "pol", "eff", "base", "surface",
                 "2026-09-20T00:00:00+00:00",
                 json.dumps({"execution_id": "EXEC-1", "job_id": self.job_id,
                             "contract_revision": contract_revision,
                             "execution_context": {
                                 "policy_snapshot_sha256": policy_sha,
                                 "profile_id": "t"},
                             "effective_policy": {
                                 "profile_id": "t",
                                 "effective_policy_sha256": "e" * 64,
                                 "resolved_worker": {"reasoning_effort": "medium"},
                                 "resolved_reviewer": {
                                     "reviewer_required": True,
                                     "effective_risk_level": "LOW",
                                     "risk_level": "LOW",
                                     "context_tier": "DIFF_ONLY"}},
                             "mutation_scope": {"write_scope": ["app/app.py"]}})))
            self.repo._connection.execute(
                "INSERT INTO execution_authority VALUES(?,?,?,?,?,?)",
                ("EXEC-1", "ACTIVE", 0, "MATERIALIZED",
                 "2026-09-20T00:00:00+00:00", "{}"))

    def add_attempt(self, *, role="WORKER", status="SUCCEEDED",
                    failure_code="", retry_domain="", model="gpt-5.6-luna",
                    started="2026-09-20T00:01:00+00:00",
                    finished="2026-09-20T00:02:00+00:00", worker_cycle=None,
                    outcome_status=None):
        attempt_id = f"T-1-{role}"
        with self.repo.hold():
            self.repo._connection.execute(
                "INSERT INTO attempts VALUES(?,?,?,?,?,?,?,?,?,?)",
                (attempt_id, "EXEC-1", role, model, "prof", "pol", "surface",
                 "cand", started,
                 json.dumps({"attempt_id": attempt_id, "role": role})))
            self.repo._connection.execute(
                "INSERT INTO attempt_outcomes VALUES(?,?,?,?,?,?,?)",
                (attempt_id, outcome_status or status, retry_domain, failure_code,
                 1 if retry_domain == "PRODUCT_SEMANTIC_RETRY" else 0, finished,
                 json.dumps({})))
        binding_dir = self.run / "owner-1" / f"{role.lower()}-1"
        (binding_dir / "context").mkdir(parents=True)
        manifest = {"contract": {"risk_tier": "LOW", "context_tier": "DIFF_ONLY"}}
        if worker_cycle is not None:
            manifest["implementation_context"] = {"worker_cycle": worker_cycle}
        (binding_dir / "context" / "manifest.json").write_text(
            json.dumps(manifest), encoding="utf-8")
        (binding_dir / "binding.json").write_text(json.dumps({
            "attempt_id": attempt_id, "binding_id": "B1", "job_id": self.job_id,
            "role": role, "runtime": "hermes", "provider": "openai-codex",
            "model": model, "command_id": "C1"}), encoding="utf-8")
        return attempt_id

    def set_job(self, *, status, last_result):
        job = self.store.get_job(self.job_id)
        job["status"] = status
        job["candidate_id"] = last_result.get("candidate_id", "CAND-1")
        job["last_result"] = last_result
        job["revision"] += 1
        self.repo.write_job(job, expected_revision=job["revision"] - 1)
        self.job_results = [{"job_id": self.job_id, "status": status,
                             "client_job_id": "A", "batch_id": job.get("batch_id", ""),
                             "candidate_id": job["candidate_id"],
                             "last_result": last_result}]

    def add_decision(self, resolution):
        decision = self.repo.record_decision(
            self.job_id, contract_revision=1, decision_type="CANDIDATE",
            question="qa", payload={"candidate_id": "CAND-1"})
        self.repo.resolve_decision(decision["decision_id"], resolution=resolution)
        return decision

    def finalize(self):
        (self.run / "result.json").write_text(json.dumps({
            "fixture": str(self.fixture),
            "job_results": self.job_results}), encoding="utf-8")
        return self.run


def base_result(**overrides):
    result = {"task_id": "T-1", "execution_id": "EXEC-1", "status": "SUCCESS",
              "success": True, "build_status": "PASS", "test_status": "PASS",
              "tester_status": "PASS", "review_status": "REVIEW_PASS",
              "verification_status": "VERIFIED", "failure_code": "",
              "failure_type": "", "failure_origin": "", "failure_stage": "",
              "retry_domain": "", "changed_files": ["app/app.py"],
              "candidate_id": "CAND-1", "worktree_disposition": "CANONICAL_COMMITTED",
              "product_semantic_retry_count": 0,
              "technical_execution_retry_count": 0,
              "runtime_recovery_count": 0, "is_rolled_back": False,
              "worker_invocations": 1, "reviewer_cycle_count": 1,
              "tester_cycle_count": 1}
    result.update(overrides)
    return result


class Step12ExperienceTests(unittest.TestCase):
    def setUp(self):
        self.temp = harness_temp.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def fixture(self):
        fixture = Fixture(self.root)
        fixture.add_execution()
        return fixture

    # ------------------------------------------------------------------
    def test_successful_job_record(self):
        f = self.fixture()
        f.add_attempt()
        f.set_job(status="SUCCEEDED", last_result=base_result())
        run = f.finalize()
        records = records_from_run(run)
        self.assertEqual(1, len(records))
        record = records[0]
        self.assertEqual("WORKER", record["identity"]["role"])
        self.assertEqual("SUCCEEDED", record["outcome"]["job_status"])
        self.assertEqual("PASS", record["outcome"]["build"])
        self.assertEqual("VERIFIED", record["outcome"]["verification"])
        self.assertEqual("NONE", record["classification"]["retry_class"])
        self.assertIsNone(record["failure_fingerprint"])
        self.assertEqual(60, record["outcome"]["elapsed_seconds"])
        # Decision/action/outcome stay separated.
        self.assertEqual("openai-codex", record["decision"]["provider"])
        self.assertEqual("INITIAL", record["action"]["attempt_type"])
        self.assertTrue(record["policy"]["source_fingerprint"] is not None)

    def test_semantic_fix_job_classified_as_semantic(self):
        f = self.fixture()
        f.add_attempt(role="WORKER", status="FAILED", failure_code="TEST_FAILED",
                      retry_domain="PRODUCT_SEMANTIC_RETRY", worker_cycle=2)
        f.set_job(status="SUCCEEDED",
                  last_result=base_result(status="SUCCESS",
                                          failure_type="test",
                                          failure_origin="WORKER_CAUSED",
                                          product_semantic_retry_count=1))
        run = f.finalize()
        record = records_from_run(run)[0]
        self.assertEqual("SEMANTIC", record["classification"]["retry_class"])
        self.assertTrue(record["classification"]["semantic_rework"])
        self.assertEqual("FIX", record["action"]["attempt_type"])
        self.assertEqual("TEST_FAILED", record["failure_fingerprint"]["failure_code"])

    def test_infrastructure_error_classified_as_technical(self):
        f = self.fixture()
        f.add_attempt(role="REVIEWER", status="FAILED",
                      failure_code="REVIEW_EXECUTION_UNAVAILABLE",
                      retry_domain="TECHNICAL_EXECUTION_RETRY",
                      model="gpt-5.6-luna")
        f.set_job(status="BLOCKED",
                  last_result=base_result(status="BLOCKED", success=False,
                                          failure_code="REVIEW_EXECUTION_UNAVAILABLE",
                                          failure_type="infrastructure",
                                          failure_origin="TOOL_INFRA",
                                          failure_stage="REVIEW",
                                          retry_domain="TECHNICAL_EXECUTION_RETRY",
                                          worktree_disposition="PRESERVED"))
        run = f.finalize()
        record = records_from_run(run)[0]
        self.assertEqual("TECHNICAL", record["classification"]["retry_class"])
        self.assertFalse(record["classification"]["semantic_rework"])
        self.assertEqual("REVIEW", record["failure_fingerprint"]["stage"])
        self.assertEqual("REVIEWER", record["identity"]["role"])

    def test_qa_hold_and_typed_decisions_are_human_interventions(self):
        f = self.fixture()
        f.add_attempt()
        f.set_job(status="AWAITING_QA",
                  last_result=base_result(status="AWAITING_QA", success=False,
                                          failure_code="USER_QA_REQUIRED",
                                          worktree_disposition="ISOLATED_CANDIDATE_PRESERVED"))
        run = f.finalize()
        record = records_from_run(run)[0]
        self.assertEqual(UNV := "UNVERIFIED", record["recovery"]["human_intervention"]["observed"])
        # Typed accept becomes an explicit intervention.
        f.add_decision("HUMAN_ACCEPT")
        record = records_from_run(run)[0]
        events = record["recovery"]["human_intervention"]["events"]
        self.assertTrue(any(e["kind"] == "QA_DECISION"
                            and e["resolution"] == "HUMAN_ACCEPT" for e in events))
        self.assertTrue(record["recovery"]["human_intervention"]["observed"])

    def test_user_waiver_recorded_not_as_pass(self):
        f = self.fixture()
        f.add_attempt()
        f.set_job(status="AWAITING_QA",
                  last_result=base_result(status="AWAITING_QA", success=False,
                                          failure_code="USER_QA_REQUIRED",
                                          worktree_disposition="ISOLATED_CANDIDATE_PRESERVED"))
        run = f.finalize()
        waivers = [{"item": "Native FIX re-execution",
                    "state": "USER_WAIVED / NOT_REEXECUTED"}]
        record = records_from_run(run, waivers=waivers)[0]
        events = record["recovery"]["human_intervention"]["events"]
        waiver_events = [e for e in events if e["kind"] == "USER_WAIVER"]
        self.assertEqual(1, len(waiver_events))
        self.assertEqual("USER_WAIVED / NOT_REEXECUTED", waiver_events[0]["state"])

    # ------------------------------------------------------------------
    def test_classification_matrix(self):
        self.assertEqual("SEMANTIC", classify_retry(
            retry_domain="PRODUCT_SEMANTIC_RETRY")["retry_class"])
        self.assertEqual("SEMANTIC", classify_retry(
            failure_type="review", failure_origin="REVIEWER_CODE_FINDING")["retry_class"])
        self.assertEqual("TECHNICAL", classify_retry(
            retry_domain="TECHNICAL_EXECUTION_RETRY")["retry_class"])
        self.assertEqual("TECHNICAL", classify_retry(
            failure_type="infrastructure", failure_origin="TOOL_INFRA")["retry_class"])
        mixed = classify_retry(retry_domain="TECHNICAL_EXECUTION_RETRY",
                               failure_type="test", failure_origin="WORKER_CAUSED",
                               semantic_retries=1)
        self.assertEqual("MIXED", mixed["retry_class"])
        self.assertEqual("NONE", classify_retry()["retry_class"])
        self.assertEqual("UNVERIFIED", classify_retry(failure_type="weird")["retry_class"])

    def test_failure_fingerprint_requires_code_and_never_merges_on_text(self):
        self.assertIsNone(failure_fingerprint(
            stage="", failure_code="", runtime="hermes", provider="", model="",
            retry_domain="", failure_type="", failure_origin="",
            contract_identity=""))
        first = failure_fingerprint(stage="REVIEW", failure_code="X", runtime="hermes",
                                    provider="a", model="m", retry_domain="",
                                    failure_type="infrastructure",
                                    failure_origin="TOOL_INFRA", contract_identity="c1")
        second = failure_fingerprint(stage="REVIEW", failure_code="X", runtime="hermes",
                                     provider="b", model="m", retry_domain="",
                                     failure_type="infrastructure",
                                     failure_origin="TOOL_INFRA", contract_identity="c1")
        self.assertNotEqual(first, second)  # provider differs -> distinct class

    # ------------------------------------------------------------------
    def test_secrets_excluded_from_records(self):
        secret = "sk-live-abcdef1234567890"
        f = self.fixture()
        f.add_attempt(status="FAILED", failure_code="PROVIDER_ERROR",
                      retry_domain="RUNTIME_RECOVERY")
        f.set_job(status="FAILED",
                  last_result=base_result(status="FAILED", success=False,
                                          failure_code="PROVIDER_ERROR",
                                          failure_type="infrastructure",
                                          failure_origin="TOOL_INFRA",
                                          failure_reason=f"token {secret} leaked",
                                          retry_domain="RUNTIME_RECOVERY"))
        run = f.finalize()
        record = records_from_run(run)[0]
        self.assertNotIn(secret, json.dumps(record))

    def test_projection_never_mutates_source_db(self):
        f = self.fixture()
        f.add_attempt()
        f.set_job(status="SUCCEEDED", last_result=base_result())
        run = f.finalize()
        db = f.fixture / ".control" / "control.sqlite3"
        before = db.read_bytes()
        records_from_run(run)
        records_from_run(run)
        self.assertEqual(before, db.read_bytes())

    def test_export_is_deterministic(self):
        f = self.fixture()
        f.add_attempt()
        f.set_job(status="SUCCEEDED", last_result=base_result())
        run = f.finalize()
        checkpoint = self.root / "checkpoint.json"
        checkpoint.write_text(json.dumps({"files": {"run.py": "a" * 64}}))
        first = export([run], harness_root=self.root, checkpoint_path=checkpoint,
                       waivers=[{"item": "schema5 native", "state": "USER_WAIVED / NOT_REEXECUTED"}],
                       harness_modifications=[
                           {"kind": "CANARY_DEFECT_FIX", "ref": "Step9A S9-01", "reason": "false success"}])
        second = export([run], harness_root=self.root, checkpoint_path=checkpoint,
                        waivers=[{"item": "schema5 native", "state": "USER_WAIVED / NOT_REEXECUTED"}],
                        harness_modifications=[
                            {"kind": "CANARY_DEFECT_FIX", "ref": "Step9A S9-01", "reason": "false success"}])
        self.assertEqual(json.dumps(first, sort_keys=True),
                         json.dumps(second, sort_keys=True))
        self.assertNotIn(str(self.root / "now"), json.dumps(first))

    def test_source_identity_uses_fingerprint_not_only_version(self):
        checkpoint = self.root / "cp.json"
        checkpoint.write_text(json.dumps({"files": {"a.py": "1"}, "new_files_vs_step9a": {"b.py": "2"}}))
        identity = source_identity(self.root, checkpoint)
        self.assertEqual(64, len(identity["source_fingerprint"]))
        checkpoint.write_text(json.dumps({"files": {"a.py": "changed"}}))
        self.assertNotEqual(identity["source_fingerprint"],
                            source_identity(self.root, checkpoint)["source_fingerprint"])

    def test_step13_metrics_shape(self):
        f = self.fixture()
        f.add_attempt()
        f.add_decision("HUMAN_REJECT")
        f.set_job(status="CANCELLED",
                  last_result=base_result(status="CANCELLED", success=False,
                                          failure_code="HUMAN_REJECTED",
                                          worktree_disposition="REJECTED_ISOLATED_CANDIDATE_PRESERVED"))
        run = f.finalize()
        records = records_from_run(run)
        metrics = step13_metrics(
            records,
            incidents=[{"kind": "STALE_FOREIGN_EVIDENCE_REJECTED",
                        "ref": "Step9A replay: QA_VERIFICATION_INPUT_CHANGED"}],
            harness_modifications=[
                {"kind": "CANARY_DEFECT_FIX", "ref": "Step9A S9-04"}],
            waivers=[{"item": "parallel native", "state": "USER_WAIVED / NOT_REEXECUTED"}])
        self.assertEqual({"CANCELLED": 1}, metrics["completion"]["dispositions"])
        self.assertEqual(1, metrics["human_cost"]["intervention_count"])
        self.assertEqual(1, metrics["harness_cost"]["self_modification_count"])
        self.assertEqual(1, len(metrics["safety"]["stale_foreign_rejection"]))
        self.assertEqual([], metrics["completion"]["false_success_incidents"])

    def test_export_records_deferred_experiments_verbatim(self):
        experiments = {
            "DREAM_RSI": "DEFERRED_POST_1_0_EXPERIMENT",
            "KNOWLEDGE_RAG": "DEFERRED_TO_1_X_EVALUATION",
            "JEV": "DEFERRED_EXPERIMENT",
            "OPENMAIC": "PERSONAL_LEARNING_TOOL",
            "WORKFLOW_ANALYSIS": "WORKFLOW_ANALYSIS_EXPERIMENT",
            "PI_OMP_ADVISOR": "POST_STEP13_CANDIDATES",
        }
        payload = export([], experiments=experiments)
        self.assertEqual(experiments, payload["experiments"])
        self.assertEqual("DEFERRED_POST_1_0_EXPERIMENT", payload["experiments"]["DREAM_RSI"])


if __name__ == "__main__":
    unittest.main()
