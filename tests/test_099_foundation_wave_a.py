import copy
from pathlib import Path
import unittest

import harness_temp
from control_repository import ControlRepository, RepositoryError
from droid_runtime_adapter import DroidRuntimeAdapter
from runtime_adapter import (
    CAPABILITY_KEYS,
    CapabilitySupport,
    RuntimeCapabilities,
    RuntimeDescriptor,
)
from runtime_contract import (
    RuntimeContractError,
    assert_role_contract_compatible,
    freeze_runtime_contracts,
)
from session_authority import (
    assert_harness_dispatch,
    reviewer_session_contract,
    session_reuse_allowed,
)


def job(job_id="JOB-A"):
    return {
        "job_id": job_id,
        "logical_job_id": "LOGICAL-A",
        "client_job_id": "CLIENT-A",
        "status": "QUEUED",
        "revision": 0,
        "contract_revision": 1,
        "owner_id": "",
        "request": {
            "worker": "codex",
            "model": "gpt-5.6-sol",
            "target_modules": ["sample-service"],
            "target_resources": ["sample-service/file"],
        },
        "depends_on": [],
    }


class RuntimeCapabilityContractTests(unittest.TestCase):
    def test_complete_matrix_and_contract_hash_are_deterministic(self):
        descriptor = RuntimeDescriptor(
            runtime="fixture",
            version="1.2.3",
            protocol_revision="fixture-jsonrpc/2",
            structured_output_schema_revision="fixture-schema/1",
            permission_tool_profile="fixture-readonly/1",
            native_queue_maturity="UNSUPPORTED",
            capabilities=RuntimeCapabilities(True, True, True, False, True, True),
            capability_contract={
                "session.create": "SUPPORTED",
                "session.resume": "SUPPORTED",
            },
        )
        first = descriptor.contract_snapshot()
        second = descriptor.contract_snapshot()
        self.assertEqual(set(CAPABILITY_KEYS), set(first["capability_snapshot"]))
        self.assertEqual(first, second)
        self.assertEqual(64, len(first["capability_snapshot_hash"]))
        self.assertEqual(64, len(first["runtime_contract_hash"]))
        self.assertEqual(
            CapabilitySupport.UNSUPPORTED.value,
            first["capability_snapshot"]["events.replay_cursor"],
        )
        drifted = RuntimeDescriptor(
            runtime="fixture",
            version="1.2.3",
            protocol_revision="fixture-jsonrpc/3",
            capabilities=descriptor.capabilities,
            capability_contract=descriptor.capability_contract,
        ).contract_snapshot()
        self.assertNotEqual(first["runtime_contract_hash"], drifted["runtime_contract_hash"])

    def test_worker_and_reviewer_runtime_surfaces_freeze_separately(self):
        descriptor = RuntimeDescriptor(
            runtime="codex", version="1", protocol_revision="jsonl/1",
            capabilities=RuntimeCapabilities(True, True, True, False, True, True),
        )
        frozen = freeze_runtime_contracts(worker=descriptor, reviewer=descriptor)
        self.assertNotEqual(
            frozen["roles"]["WORKER"]["runtime_contract_hash"],
            frozen["roles"]["REVIEWER"]["runtime_contract_hash"],
        )
        self.assertEqual(
            frozen["roles"]["WORKER"],
            assert_role_contract_compatible(frozen, descriptor, "WORKER"),
        )
        changed = RuntimeDescriptor(
            runtime="codex", version="2", protocol_revision="jsonl/1",
            capabilities=descriptor.capabilities,
        )
        with self.assertRaisesRegex(RuntimeContractError, "RUNTIME_CONTRACT_DRIFT"):
            assert_role_contract_compatible(frozen, changed, "WORKER")

    def test_droid_maps_installed_resume_fork_and_multiturn_without_guessing(self):
        descriptor = DroidRuntimeAdapter().preflight()
        if descriptor.validation_disposition == "UNAVAILABLE":
            self.skipTest("installed Droid CLI unavailable")
        matrix = descriptor.contract_snapshot()["capability_snapshot"]
        self.assertEqual("SUPPORTED", matrix["session.resume"])
        self.assertEqual("SUPPORTED", matrix["session.fork"])
        self.assertEqual("SUPPORTED", matrix["turn.submit"])
        self.assertIn(matrix["events.subscribe"], {"SUPPORTED", "PARTIAL"})
        self.assertEqual("UNSUPPORTED", matrix["events.replay_cursor"])
        self.assertTrue(descriptor.capabilities.explicit_resume)

    def test_droid_resume_fork_and_native_ids_use_one_adapter_contract(self):
        adapter = DroidRuntimeAdapter(executable="droid")
        resumed = adapter.resume_session(
            "session-1", "continue", cwd=".", model="model-1", reasoning_effort="high"
        )
        forked = adapter.fork_session(
            "session-1", "alternate", cwd=".", model="model-1", reasoning_effort="medium"
        )
        self.assertIn("--session-id", resumed.argv)
        self.assertIn("session-1", resumed.argv)
        self.assertIn("--fork", forked.argv)
        event = adapter.normalize_event({
            "type": "result", "session_id": "session-1", "turn_id": "turn-1",
            "subtype": "success", "result": "done",
        })[0]
        self.assertEqual(("session-1", "turn-1", True),
                         (event.session_id, event.turn_id, event.success))


class SessionAndWriterAuthorityTests(unittest.TestCase):
    def setUp(self):
        self.temp = harness_temp.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "control.sqlite3"
        self.repo = ControlRepository(self.path, create=True)
        self.job = job()
        self.repo.bootstrap(
            {"revision": 0, "generation": 1, "order": [self.job["job_id"]]},
            {self.job["job_id"]: self.job},
            {self.job["job_id"]: {"path": "jobs/JOB-A.json", "sha256": "fixture"}},
        )
        self.execution = self.repo.materialize(
            self.job,
            context={
                "profile_snapshot_sha256": "profile",
                "policy_snapshot_sha256": "policy",
                "workspace_identity_sha256": "workspace",
            },
            policy={"effective_policy_sha256": "policy"},
            baseline_hash="baseline",
            execution_surface_hash="surface",
            extra={
                "source_view_id": "SV-A",
                "runtime_contract_hash": "runtime-contract",
            },
        )

    def test_worker_and_reviewer_sessions_are_distinct_and_role_bound(self):
        worker = self.repo.bind_native_session(
            self.execution["execution_id"], runtime="codex", native_session_id="worker-session",
            adapter_revision="adapter/1", runtime_version="1", role="WORKER",
            source_view_id="SV-A", workspace_identity="workspace",
            runtime_contract_hash="runtime-contract", candidate_hash="candidate",
        )
        reviewer = self.repo.bind_native_session(
            self.execution["execution_id"], runtime="codex", native_session_id="review-session",
            adapter_revision="adapter/1", runtime_version="1", role="REVIEWER",
            source_view_id="SV-A", workspace_identity="workspace",
            runtime_contract_hash="runtime-contract", candidate_hash="candidate",
            review_contract_hash="review-contract", allow_fresh=True,
        )
        self.assertNotEqual(worker["binding_id"], reviewer["binding_id"])
        self.assertEqual("WORKER", self.repo.session_binding(
            self.execution["execution_id"], "codex", role="WORKER"
        )["role"])
        self.assertEqual("REVIEWER", self.repo.session_binding(
            self.execution["execution_id"], "codex", role="REVIEWER",
            review_contract_hash="review-contract",
        )["role"])
        with self.assertRaisesRegex(RepositoryError, "NATIVE_SESSION_ROLE_SHARE_FORBIDDEN"):
            self.repo.bind_native_session(
                self.execution["execution_id"], runtime="codex",
                native_session_id="worker-session", adapter_revision="adapter/1",
                runtime_version="1", role="REVIEWER", allow_fresh=True,
            )

    def test_qa_suspends_writer_but_preserves_native_session(self):
        attempt = self.repo.start_attempt(
            self.execution, attempt_id="ATTEMPT-W", role="WORKER", model="gpt-5.6-sol"
        )
        binding = self.repo.bind_native_session(
            self.execution["execution_id"], runtime="codex", native_session_id="worker-session",
            adapter_revision="adapter/1", runtime_version="1", role="WORKER",
        )
        self.assertEqual("ACTIVE", self.repo.writer_authority(self.execution["execution_id"])["state"])
        self.repo.suspend_writer_authority(self.execution["execution_id"], reason="PENDING_DECISION")
        self.assertEqual("SUSPENDED", self.repo.writer_authority(self.execution["execution_id"])["state"])
        self.assertEqual(binding["binding_id"], self.repo.session_binding(
            self.execution["execution_id"], "codex", role="WORKER"
        )["binding_id"])
        with self.assertRaisesRegex(RepositoryError, "WRITER_AUTHORITY_SUSPENDED"):
            self.repo.prepare_command(
                self.execution["execution_id"], attempt["attempt_id"],
                idempotency_key="queued-native-followup", payload_sha256="a" * 64,
                binding_id=binding["binding_id"],
            )
        with self.assertRaisesRegex(RepositoryError, "WRITER_AUTHORITY_SUSPENDED"):
            self.repo.record_runtime_process(
                attempt["attempt_id"], runtime="codex", pid=123,
                process_identity="123:start", binding_id=binding["binding_id"],
            )

    def test_terminalized_pending_decision_keeps_authority_suspended(self):
        self.repo.start_attempt(
            self.execution, attempt_id="ATTEMPT-QA", role="WORKER", model="gpt-5.6-sol"
        )
        current = self.repo.read_job(self.job["job_id"])
        current.update(status="AWAITING_QA", revision=current["revision"] + 1,
                       last_result={"failure_code": "CONTRACT_UNRESOLVED"})
        self.repo.write_job(current, expected_revision=current["revision"] - 1)
        authority = self.repo.writer_authority(self.execution["execution_id"])
        self.assertEqual(("SUSPENDED", "PENDING_DECISION"),
                         (authority["state"], authority["reason"]))

    def test_reviewer_reuse_requires_every_frozen_binding(self):
        contract = reviewer_session_contract(
            logical_job_id="LOGICAL-A", contract_revision=1, role="REVIEWER",
            workspace_identity="workspace", source_view_id="SV-A",
            execution_surface_hash="surface", candidate_hash="candidate",
            review_input_hash="review-input",
        )
        self.assertTrue(session_reuse_allowed(contract, copy.deepcopy(contract)))
        for key, value in (
            ("logical_job_id", "LOGICAL-B"),
            ("contract_revision", 2),
            ("role", "WORKER"),
            ("workspace_identity", "other"),
            ("source_view_id", "SV-B"),
            ("execution_surface_hash", "other-surface"),
            ("candidate_hash", "other-candidate"),
            ("review_input_hash", "other-review"),
        ):
            changed = copy.deepcopy(contract)
            changed[key] = value
            self.assertFalse(session_reuse_allowed(contract, changed), key)

    def test_native_queue_cannot_bypass_materialization_or_preload_batch(self):
        assert_harness_dispatch(materialization_id="MAT-A", queued_turn_count=1)
        with self.assertRaisesRegex(RuntimeError, "MATERIALIZATION_REQUIRED"):
            assert_harness_dispatch(materialization_id="", queued_turn_count=1)
        with self.assertRaisesRegex(RuntimeError, "NATIVE_BATCH_PRELOAD_FORBIDDEN"):
            assert_harness_dispatch(materialization_id="MAT-A", queued_turn_count=2)


if __name__ == "__main__":
    unittest.main()
