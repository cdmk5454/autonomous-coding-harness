from pathlib import Path
import unittest

import harness_temp
from control_repository import ControlRepository, RepositoryError
from runtime_adapter import payload_sha256
from runtime_host import runtime_host_manifest
from session_authority import fork_reference
from source_view import runtime_namespace, scope_contract, source_view_manifest, validate_write_scope


def fixture_job():
    return {
        "job_id": "JOB-B", "logical_job_id": "LOGICAL-B", "client_job_id": "CLIENT-B",
        "status": "QUEUED", "revision": 0, "contract_revision": 1, "owner_id": "",
        "request": {"worker": "codex", "model": "gpt-5.6-sol",
                    "target_modules": ["sample-service"], "target_resources": ["sample-service/src"]},
        "depends_on": [],
    }


class SourceViewContractTests(unittest.TestCase):
    def test_base_provenance_scope_host_and_namespace_are_deterministic(self):
        args = dict(
            logical_job_id="LOGICAL-B", contract_revision=1, role="WORKER",
            workspace_identity="workspace", baseline_hash="baseline",
            materialization_id="MAT-B", predecessor_delta=["sample-service/a", "sample-service/b"],
            candidate_overlay={"candidate_id": "CAND-B", "candidate_hash": "candidate"},
        )
        first = source_view_manifest(**args)
        self.assertEqual(first, source_view_manifest(**args))
        self.assertNotEqual(first["source_view_id"], source_view_manifest(
            **{**args, "baseline_hash": "other"}
        )["source_view_id"])
        scope = scope_contract(fixture_job()["request"])
        self.assertTrue(validate_write_scope(["sample-service/src/A.java"], scope)["valid"])
        self.assertEqual(
            ["sample-service/pom.xml"], validate_write_scope(["sample-service/pom.xml"], scope)["outside_write_scope"]
        )
        host = runtime_host_manifest(
            workspace_identity="workspace",
            runtime_contracts={"runtime_contract_hash": "runtime"},
            system="Windows", machine="AMD64", release="11", wsl=False,
        )
        self.assertEqual("WINDOWS", host["environment"])
        self.assertNotEqual(host["runtime_host_id"], runtime_host_manifest(
            workspace_identity="workspace",
            runtime_contracts={"runtime_contract_hash": "runtime"},
            system="Linux", machine="x86_64", release="6", wsl=True,
        )["runtime_host_id"])
        namespace = runtime_namespace("runtime-temp", source_view_id=first["source_view_id"],
                                      workspace_identity="workspace")
        self.assertEqual(Path("runtime-temp").resolve(), namespace.parent)

    def test_native_fork_has_no_writer_or_rollback_authority(self):
        reference = fork_reference(
            parent_binding_id="BIND-B", source_view_id="SV-B", materialization_id="MAT-B"
        )
        self.assertEqual("NONE", reference["writer_authority"])
        self.assertFalse(reference["canonical_rollback_evidence"])


class InboundEventAuthorityTests(unittest.TestCase):
    def setUp(self):
        self.temp = harness_temp.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.repo = ControlRepository(Path(self.temp.name) / "control.sqlite3", create=True)
        self.job = fixture_job()
        self.repo.bootstrap(
            {"revision": 0, "generation": 1, "order": ["JOB-B"]},
            {"JOB-B": self.job}, {"JOB-B": {"path": "jobs/JOB-B.json", "sha256": "fixture"}},
        )
        self.execution = self.repo.materialize(
            self.job,
            context={"profile_snapshot_sha256": "profile", "policy_snapshot_sha256": "policy",
                     "workspace_identity_sha256": "workspace"},
            policy={"effective_policy_sha256": "policy"}, baseline_hash="baseline",
            execution_surface_hash="surface",
        )
        self.attempt = self.repo.start_attempt(
            self.execution, attempt_id="ATTEMPT-B1", role="WORKER", model="gpt-5.6-sol"
        )
        self.binding = self.repo.bind_native_session(
            self.execution["execution_id"], runtime="codex", native_session_id="SESSION-B",
            adapter_revision="adapter/2", runtime_version="1", role="WORKER",
        )

    def event(self, **changes):
        values = {
            "binding_id": self.binding["binding_id"], "runtime": "codex",
            "provider_event_id": "provider-1", "native_session_id": "SESSION-B",
            "native_turn_id": "TURN-B", "event_type": "TURN_COMPLETED", "sequence": "1",
            "payload_sha256": payload_sha256({"result": "ok"}),
        }
        values.update(changes)
        return self.repo.record_inbound_event(
            self.execution["execution_id"], self.attempt["attempt_id"], **values
        )

    def test_duplicate_event_applies_once_and_ambiguous_event_holds(self):
        event = self.event()
        applied = self.repo.apply_inbound_event(
            event["event_id"], applied_revision=1, applied_state="SUCCEEDED"
        )
        self.assertEqual("APPLIED", applied["status"])
        self.assertTrue(self.event()["duplicate"])
        ambiguous = self.event(
            provider_event_id="", sequence="", cursor="",
            payload_sha256=payload_sha256({"result": "ambiguous"}),
        )
        self.assertEqual("AMBIGUOUS", ambiguous["status"])
        with self.assertRaisesRegex(RepositoryError, "INBOUND_EVENT_RECONCILIATION_REQUIRED"):
            self.repo.apply_inbound_event(
                ambiguous["event_id"], applied_revision=2, applied_state="SUCCEEDED"
            )

    def test_late_attempt_event_cannot_overwrite_current_attempt(self):
        self.repo.start_attempt(
            self.execution, attempt_id="ATTEMPT-B2", role="WORKER", model="gpt-5.6-sol"
        )
        late = self.event(provider_event_id="provider-late", sequence="2")
        self.assertEqual("LATE", late["status"])
        with self.assertRaisesRegex(RepositoryError, "INBOUND_EVENT_RECONCILIATION_REQUIRED"):
            self.repo.apply_inbound_event(late["event_id"], applied_revision=2, applied_state="FAILED")

    def test_open_decision_suspends_writer_without_destroying_session(self):
        self.repo.record_decision(
            "JOB-B", contract_revision=1, decision_type="CONTRACT", question="Choose A or B"
        )
        self.assertEqual("SUSPENDED", self.repo.writer_authority(
            self.execution["execution_id"]
        )["state"])
        self.assertEqual(self.binding["binding_id"], self.repo.session_binding(
            self.execution["execution_id"], "codex", role="WORKER"
        )["binding_id"])


if __name__ == "__main__":
    unittest.main()
