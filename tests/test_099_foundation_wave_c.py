from pathlib import Path
import unittest

import harness_temp
from control_repository import ControlRepository, RepositoryError
from verification_contract import derive_verification_contract


class VerificationContractTests(unittest.TestCase):
    def test_source_is_evidence_not_requirement_and_risk_axes_are_separate(self):
        contract = derive_verification_contract(
            {"qa_type": ""}, {"required_tests": [], "command": "", "mandatory": True}
        )
        self.assertEqual("STATIC", contract["depth"])
        self.assertFalse(contract["source_is_requirement_authority"])
        self.assertEqual("SEPARATE", contract["review_risk_axis"])
        self.assertFalse(contract["auto_e2e_generation"])

    def test_explicit_e2e_and_human_e2e_are_not_downgraded(self):
        e2e = derive_verification_contract(
            {}, {"verification_depth": "E2E", "mandatory": True}
        )
        human = derive_verification_contract(
            {"qa_type": "HUMAN_E2E"}, {"mandatory": True}
        )
        self.assertEqual("E2E", e2e["depth"])
        self.assertEqual("HUMAN_CONTROLLED_E2E", human["depth"])
        self.assertTrue(human["human_trace_readiness"])


class VerificationEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = harness_temp.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.repo = ControlRepository(Path(self.temp.name) / "control.sqlite3", create=True)
        self.job = {
            "job_id": "JOB-C", "logical_job_id": "LOGICAL-C", "client_job_id": "CLIENT-C",
            "status": "QUEUED", "revision": 0, "contract_revision": 1, "owner_id": "",
            "request": {"worker": "codex", "model": "gpt-5.6-sol",
                        "target_modules": ["sample-service"], "target_resources": ["sample-service/src"]},
            "depends_on": [],
        }
        self.repo.bootstrap(
            {"revision": 0, "generation": 1, "order": ["JOB-C"]}, {"JOB-C": self.job},
            {"JOB-C": {"path": "jobs/JOB-C.json", "sha256": "fixture"}},
        )
        contract = derive_verification_contract(
            {}, {"verification_depth": "E2E", "mandatory": True}
        )
        self.execution = self.repo.materialize(
            self.job,
            context={"profile_snapshot_sha256": "profile", "policy_snapshot_sha256": "policy",
                     "workspace_identity_sha256": "workspace"},
            policy={"effective_policy_sha256": "policy"}, baseline_hash="baseline",
            execution_surface_hash="surface",
            extra={"verification_contract": contract,
                   "verification_environment_hash": "environment"},
        )
        self.attempt = self.repo.start_attempt(
            self.execution, attempt_id="ATTEMPT-C", role="WORKER",
            model="gpt-5.6-sol", candidate_hash="candidate",
        )

    def record(self, **changes):
        values = {
            "scenario_id": "SCENARIO-C", "candidate_hash": "candidate",
            "contract_revision": 1, "environment_hash": "environment",
            "result": "PASS", "artifacts": [{"path": "evidence/e2e.json", "depth": "E2E"}],
            "machine_observed": True, "verification_depth": "E2E",
        }
        values.update(changes)
        return self.repo.record_verification_evidence(
            self.execution["execution_id"], self.attempt["attempt_id"], **values
        )

    def test_pass_is_bound_to_candidate_contract_environment_and_artifact(self):
        evidence = self.record()
        self.assertEqual("PASS", evidence["result"])
        self.assertEqual("candidate", evidence["candidate_hash"])
        self.assertFalse(evidence["auto_e2e_generation"])

    def test_agent_self_report_and_static_substitution_are_forbidden(self):
        with self.assertRaisesRegex(RepositoryError, "VERIFICATION_SELF_REPORT_FORBIDDEN"):
            self.record(artifacts=[], machine_observed=False)
        with self.assertRaisesRegex(RepositoryError, "MANDATORY_VERIFICATION_DEPTH_NOT_MET"):
            self.record(artifacts=[{"path": "review.txt", "depth": "STATIC"}])

    def test_stale_candidate_contract_or_environment_is_rejected(self):
        for changes in (
            {"candidate_hash": "stale"}, {"contract_revision": 2},
            {"environment_hash": "other"},
        ):
            with self.assertRaisesRegex(RepositoryError, "VERIFICATION_EVIDENCE_BINDING_STALE"):
                self.record(**changes)


if __name__ == "__main__":
    unittest.main()
