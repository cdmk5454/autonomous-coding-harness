"""0.9.1 regressions for frozen policy, shutdown and disposable workspaces."""

from concurrent.futures import Future
import json
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest import mock

import harness_temp
from execution_lifecycle import LifecycleError, ShutdownBarrier
from harness_service import HarnessService, HarnessServiceError
from manager import Manager, ManagerResult
from policy_catalog import effective_policy_hash
from queue_supervisor import QueueSupervisor
from runtime_safety import scrub_secrets
from test_job_control import execution_context, request, MODULES


class PolicyFreezeTests(unittest.TestCase):
    def test_semantic_hash_repeatable_preserves_enforcement_and_order(self):
        first = {"schema_version": 1, "required": 2, "preferred": 3, "max": 4,
                 "exact": "sol", "safety_requirements": ["pii", "authorization"],
                 "precedence": ["system", "job"], "generated_at": "one"}
        for index in range(20):
            other = dict(reversed(list(first.items())))
            other.update(generated_at=str(index), runtime_pid=index,
                         safety_requirements=["authorization", "pii"])
            self.assertEqual(effective_policy_hash(first), effective_policy_hash(other))
        for key, value in [("required", 3), ("preferred", 2), ("max", 5), ("exact", "astra"),
                           ("safety_requirements", ["pii"]), ("precedence", ["job", "system"])]:
            self.assertNotEqual(effective_policy_hash(first), effective_policy_hash({**first, key: value}))

    def test_materialized_v1_survives_global_v2_and_next_execution_uses_v2(self):
        with harness_temp.TemporaryDirectory() as directory:
            root = Path(directory)
            context = execution_context()
            profile = SimpleNamespace(id="e4u", schema_version=1, profile_dir=root, module_names=MODULES)
            frozen = []

            class FakeManager:
                def __init__(self, **kwargs):
                    self.guard = kwargs["configuration_guard"]

                def submit_task(self, task):
                    frozen.append(task.effective_policy["policy_snapshot_sha256"])
                    context["policy_snapshot_sha256"] = "e" * 64
                    self.guard()
                    task.status = "SUCCESS"
                    task.stage = "DONE"
                    task.verification_status = "VERIFIED"
                    future = Future()
                    future.set_result(ManagerResult(success=True))
                    return future

                def shutdown(self):
                    pass

            service = HarnessService(root, root, task_root=root / ".tasks", manager_factory=FakeManager,
                                     instance_lock=SimpleNamespace(acquire=lambda: True, release=lambda: None))
            service._load_profile = lambda: (profile, list(MODULES))
            service._configuration = lambda *_: dict(context)
            service._materialize_runtime_snapshot = lambda *_: SimpleNamespace(
                profile=profile, policy_root=root, validate=lambda: None)
            self.assertTrue(service.acquire_execution_lease())
            for index in range(2):
                outcome = service.execute({"request": request().to_dict(), "execution_context": dict(context)},
                                          on_task_started=lambda _: None)
                self.assertNotEqual("PROFILE_DRIFT", outcome["failure_code"])
                self.assertEqual(index == 0, outcome["policy_changed_after_materialization"])
            service.release_execution_lease()
            self.assertEqual([execution_context()["policy_snapshot_sha256"], "e" * 64], frozen)


class LifecycleTests(unittest.TestCase):
    def test_closed_service_rejects_before_task_budget_or_manager(self):
        factory = mock.Mock()
        started = mock.Mock()
        with harness_temp.TemporaryDirectory() as directory:
            service = HarnessService(directory, directory, manager_factory=factory)
            service.close()
            service.close()
            with self.assertRaisesRegex(HarnessServiceError, "CONTROL_SHUTTING_DOWN"):
                service.execute({}, on_task_started=started)
            factory.assert_not_called()
            started.assert_not_called()
            service.restart()
            service.restart()
            self.assertEqual("ACTIVE", service._lifecycle.state)

    def test_interpreter_shutdown_rejects_executor_construction(self):
        with mock.patch("threading._SHUTTING_DOWN", True), mock.patch("execution_lifecycle.ThreadPoolExecutor") as pool:
            with self.assertRaises(LifecycleError):
                Manager(".", "", max_workers=1)
            pool.assert_not_called()

    def test_stop_and_late_start_do_not_claim_job(self):
        harness = SimpleNamespace(close=mock.Mock())
        store = mock.Mock()
        supervisor = QueueSupervisor(store, harness)
        supervisor.stop()
        supervisor.stop()
        self.assertEqual("CONTROL_SHUTTING_DOWN", supervisor.start()["code"])
        store.claim_next.assert_not_called()

    def test_manager_shutdown_is_idempotent_and_rejects_submit(self):
        with harness_temp.TemporaryDirectory() as directory:
            manager = Manager(directory, "", max_workers=1)
            manager.shutdown()
            manager.shutdown()
            with self.assertRaises(LifecycleError):
                manager.submit_task(mock.Mock())


class TempTests(unittest.TestCase):
    def test_windows_temp_references_survive_secret_redaction(self):
        path = r"C:\Users\fixture\AppData\Local\Temp\e4u-harness\test\snapshot.json"
        environment = {"HOMEPATH": r"\Users\fixture", "PAT": "credential-value"}
        self.assertEqual(path, scrub_secrets(path, environment))
        self.assertEqual("****", scrub_secrets("credential-value", environment))

    def test_cwd_scratch_goes_to_harness_root(self):
        before = {p.name for p in Path.cwd().glob("tmp*")}
        with harness_temp.TemporaryDirectory(dir=Path.cwd()) as directory:
            self.assertTrue(Path(directory).is_relative_to(harness_temp.temp_root()))
            self.assertEqual([], list(Path(directory).iterdir()))
        self.assertEqual(before, {p.name for p in Path.cwd().glob("tmp*")})

    def test_stale_cleanup_keeps_active_current_and_unowned(self):
        with harness_temp.TemporaryDirectory() as directory, mock.patch.dict(os.environ, {"HARNESS_TEMP_ROOT": directory}):
            parent = Path(directory) / "runtime"
            for name, owner, pid, session, created in [
                ("dead", "e4u-harness", 99, "old", 0), ("active", "e4u-harness", 1, "old", 0),
                ("current", "e4u-harness", 99, harness_temp._SESSION, 0),
                ("unowned", "other", 99, "old", 0), ("recent", "e4u-harness", 99, "old", 99000)]:
                target = parent / name
                target.mkdir(parents=True)
                (target / harness_temp._MARKER).write_text(json.dumps({"owner": owner, "pid": pid,
                                                                      "session": session, "created_at": created}))
            self.assertEqual([], harness_temp.cleanup_stale(pid_is_active=lambda pid: pid == 1, now=100000))
            self.assertEqual({"active", "current", "unowned", "recent"}, {p.name for p in parent.iterdir()})


if __name__ == "__main__":
    unittest.main()
