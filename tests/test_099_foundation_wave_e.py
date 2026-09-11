from pathlib import Path
import json
import sqlite3
import unittest

import harness_temp
from control_backup import create_backup, database_fingerprint, restore_backup
from control_migration import rehearse
from control_repository import ControlRepository, SCHEMA_VERSION
from chatgpt_control import ChatGPTControlService
from harness_eval import environment_manifest, run_corpus
from job_queue import JobStore
from opencode_profile import map_frozen_profile
from test_job_control import FakeLease, request, success_result, valid_enqueue
from verification_contract import derive_verification_contract


class BackupRestoreTests(unittest.TestCase):
    def test_verified_backup_restore_preserves_all_control_projections(self):
        with harness_temp.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "control.sqlite3"
            backup = root / "backup.sqlite3"
            restored = root / "restored.sqlite3"
            repo = ControlRepository(source, create=True)
            job = {
                "job_id": "JOB-E", "logical_job_id": "LOGICAL-E", "client_job_id": "CLIENT-E",
                "status": "QUEUED", "revision": 0, "contract_revision": 1, "owner_id": "",
                "request": {"worker": "codex", "model": "fixture",
                            "target_modules": ["alpha"], "target_resources": ["alpha/src"]},
                "depends_on": [],
            }
            repo.bootstrap(
                {"revision": 0, "generation": 1, "order": ["JOB-E"]}, {"JOB-E": job},
                {"JOB-E": {"path": "jobs/JOB-E.json", "sha256": "fixture"}},
            )
            repo.materialize(
                job, context={"profile_snapshot_sha256": "p", "policy_snapshot_sha256": "q",
                              "workspace_identity_sha256": "w"},
                policy={"effective_policy_sha256": "q"}, baseline_hash="b",
                execution_surface_hash="s",
            )
            backup_result = create_backup(source, backup)
            restore_result = restore_backup(backup, restored)
            self.assertEqual(backup_result["source_fingerprint"], database_fingerprint(restored))
            self.assertTrue(restore_result["artifact_references_intact"])
            self.assertEqual(SCHEMA_VERSION, restore_result["restored_fingerprint"]["schema_version"])
            self.assertEqual("PASS", restore_result["restored_fingerprint"]["integrity"])


class ProductReadinessCompatibilityTests(unittest.TestCase):
    def mixed_store(self, root: Path, *, read_only: bool):
        legacy = root / ".control"
        store = JobStore(legacy)
        jobs, _ = valid_enqueue(
            store,
            "mixed-readiness",
            [
                request(client_job_id="legacy-success"),
                request(client_job_id="legacy-unknown"),
                request(client_job_id="current"),
            ],
        )
        for index, job in enumerate(jobs[:2]):
            owner = f"owner-{index}"
            store.claim_next(owner)
            store.mark_task_started(job["job_id"], owner, f"TASK-099-{index}")
            store.record_result(job["job_id"], owner, success_result(f"TASK-099-{index}"))
            store.acknowledge_success(job["job_id"], f"ack-{index}")
        unknown_path = legacy / "jobs" / f"{jobs[1]['job_id']}.json"
        unknown = json.loads(unknown_path.read_text(encoding="utf-8"))
        unknown["status"] = "UNKNOWN_LEGACY"
        unknown_path.write_text(json.dumps(unknown, ensure_ascii=False, indent=2), encoding="utf-8")
        queue_path = legacy / "queue.json"
        queue = json.loads(queue_path.read_text(encoding="utf-8"))
        queue["active_from_index"] = 2
        queue_path.write_text(json.dumps(queue, ensure_ascii=False, indent=2), encoding="utf-8")
        database = root / "control.sqlite3"
        self.assertTrue(rehearse(legacy, database)["pass"])
        repository = ControlRepository(database, read_only=read_only, legacy_root=legacy)
        self.assertFalse(repository.has_job(jobs[0]["job_id"]))
        self.assertFalse(repository.has_job(jobs[1]["job_id"]))
        self.assertTrue(repository.has_job(jobs[2]["job_id"]))
        return legacy, jobs, repository, JobStore(legacy, read_only=read_only, repository=repository)

    def test_stored_projection_and_writable_logical_job_keep_persisted_path(self):
        with harness_temp.TemporaryDirectory() as directory:
            _, jobs, repository, _ = self.mixed_store(Path(directory), read_only=False)
            current_id = jobs[2]["job_id"]
            first = repository.product_readiness(current_id)
            second = repository.product_readiness(current_id)
            self.assertEqual("FINALIZATION_PENDING", first["readiness"])
            self.assertEqual(first, second)
            with repository.hold():
                count = repository._connection.execute(
                    "SELECT COUNT(*) FROM product_readiness WHERE job_id=?", (current_id,)
                ).fetchone()[0]
            self.assertEqual(1, count)

    def test_legacy_success_and_unknown_are_derived_without_false_complete(self):
        with harness_temp.TemporaryDirectory() as directory:
            _, jobs, repository, _ = self.mixed_store(Path(directory), read_only=False)
            success = repository.product_readiness(jobs[0]["job_id"])
            unknown = repository.product_readiness(jobs[1]["job_id"])
            self.assertEqual("COMPLETE", success["readiness"])
            self.assertEqual("FINALIZATION_PENDING", unknown["readiness"])
            with repository.hold():
                count = repository._connection.execute(
                    "SELECT COUNT(*) FROM product_readiness WHERE job_id IN (?,?)",
                    (jobs[0]["job_id"], jobs[1]["job_id"]),
                ).fetchone()[0]
            self.assertEqual(0, count)

    def test_read_only_miss_and_mixed_health_do_not_write(self):
        with harness_temp.TemporaryDirectory() as directory:
            root = Path(directory)
            legacy, jobs, repository, store = self.mixed_store(root, read_only=True)
            (root / ".tasks").mkdir()
            before = database_fingerprint(repository.path)
            self.assertEqual(
                "FINALIZATION_PENDING",
                repository.product_readiness(jobs[2]["job_id"])["readiness"],
            )
            service = ChatGPTControlService(
                "D:\\project",
                "D:\\profile",
                control_root=legacy,
                task_root=root / ".tasks",
                harness=FakeLease([]),
                store=store,
                reconcile=False,
                state_base=root,
                read_only=True,
            )
            health = service.health()
            self.assertEqual("READY", health["status"])
            self.assertEqual("FINALIZATION_PENDING", health["queue"]["product_readiness"])
            public = {item["job_id"]: item for item in service.list_jobs(limit=200)["jobs"]}
            self.assertEqual("COMPLETE", public[jobs[0]["job_id"]]["product_readiness"])
            self.assertEqual(
                "FINALIZATION_PENDING",
                public[jobs[1]["job_id"]]["product_readiness"],
            )
            self.assertEqual(
                "FINALIZATION_PENDING",
                public[jobs[2]["job_id"]]["product_readiness"],
            )
            self.assertEqual(before, database_fingerprint(repository.path))

    def test_compatibility_fixture_retains_sqlite_integrity_and_foreign_keys(self):
        with harness_temp.TemporaryDirectory() as directory:
            _, jobs, repository, _ = self.mixed_store(Path(directory), read_only=False)
            for job in jobs:
                repository.product_readiness(job["job_id"])
            fingerprint = database_fingerprint(repository.path)
            self.assertEqual("PASS", fingerprint["integrity"])
            self.assertEqual(0, fingerprint["foreign_key_errors"])


class RepresentativeSecondEnvironmentTests(unittest.TestCase):
    def test_core_contracts_reuse_without_private_profile_identity(self):
        with harness_temp.TemporaryDirectory() as directory:
            root = Path(directory)
            profile = root / "profile"
            workspace = root / "workspace"
            profile.mkdir()
            workspace.mkdir()
            for name in ("rules.md", "always.md", "analysis.md", "features.json"):
                (profile / name).write_text(name, encoding="utf-8")
            project = {
                "schema_version": 1, "id": "representative-second",
                "workspace_roots": [str(workspace)],
                "context": {"project_rules": "rules.md", "analysis": "analysis.md",
                            "feature_map": "features.json"},
                "rules": {"always": ["always.md"]},
            }
            mapping = map_frozen_profile(profile, project)
            self.assertEqual("representative-second", mapping["profile_id"])
            database = root / "second.sqlite3"
            repo = ControlRepository(database, create=True)
            job = {
                "job_id": "SECOND-JOB", "logical_job_id": "SECOND-LOGICAL",
                "client_job_id": "SECOND-CLIENT", "status": "QUEUED", "revision": 0,
                "contract_revision": 1, "owner_id": "",
                "request": {"worker": "codex", "model": "fixture",
                            "target_modules": ["alpha"], "target_resources": ["alpha/src"]},
                "depends_on": [],
            }
            repo.bootstrap(
                {"revision": 0, "generation": 1, "order": ["SECOND-JOB"]},
                {"SECOND-JOB": job},
                {"SECOND-JOB": {"path": "jobs/SECOND-JOB.json", "sha256": "fixture"}},
            )
            verification = derive_verification_contract({}, {"mandatory": True})
            execution = repo.materialize(
                job, context={"profile_snapshot_sha256": mapping["mapping_hash"],
                              "policy_snapshot_sha256": "policy",
                              "workspace_identity_sha256": "second-workspace"},
                policy={"effective_policy_sha256": "policy"}, baseline_hash="second-base",
                execution_surface_hash="second-surface",
                extra={"verification_contract": verification,
                       "verification_environment_hash": "second-env"},
            )
            self.assertTrue(execution["source_view_id"].startswith("SV-"))
            private_marker = "private" + "-profile-marker"
            self.assertNotIn(private_marker, str(execution).casefold())
            attempt = repo.start_attempt(
                execution, attempt_id="SECOND-ATTEMPT", role="WORKER",
                model="fixture", candidate_hash="second-candidate",
            )
            binding = repo.bind_native_session(
                execution["execution_id"], runtime="codex", native_session_id="second-session",
                adapter_revision="adapter/2", runtime_version="fixture", role="WORKER",
            )
            self.assertEqual("second-session", binding["native_session_id"])
            evidence = repo.record_verification_evidence(
                execution["execution_id"], attempt["attempt_id"], scenario_id="SECOND-STATIC",
                candidate_hash="second-candidate", contract_revision=1,
                environment_hash="second-env", result="PASS",
                artifacts=[{"path": "evidence/static.json", "depth": "STATIC"}],
                machine_observed=True, verification_depth="STATIC",
            )
            self.assertEqual("PASS", evidence["result"])
            decision = repo.record_decision(
                "SECOND-JOB", contract_revision=1, decision_type="DEFERRED",
                question="Choose representative option",
            )
            readiness = repo.product_readiness("SECOND-JOB")
            self.assertEqual("FINALIZATION_PENDING", readiness["readiness"])
            self.assertTrue(readiness["user_action_required"])
            self.assertEqual("OPEN", decision["status"])
            environment = environment_manifest(
                harness_version="0.99.0-dev", control_schema_revision=SCHEMA_VERSION,
                runtime_contract={"runtime": "fixture", "runtime_version": "1",
                                  "permission_tool_profile": "fixture"},
                source_view=execution["source_view_manifest"],
                execution_budget={"product_retry": 1, "runtime_recovery": 1},
            )
            eval_result = run_corpus("evals/corpus_099.json", environment=environment)
            self.assertEqual("PASS", eval_result["release_gate"])
            backup = root / "second-backup.sqlite3"
            restored = root / "second-restored.sqlite3"
            create_backup(database, backup)
            self.assertTrue(restore_backup(backup, restored)["artifact_references_intact"])


if __name__ == "__main__":
    unittest.main()
