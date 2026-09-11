import unittest
from pathlib import Path

import harness_temp
from job_queue import JobStore
from test_job_control import request, valid_enqueue

from context_diet import deduplicate_repeated_lines, durable_continuation_prompt
from operator_notifications import deliver_semantic, render_compact, terminal_projection
from selective_revalidation import plan_revalidation
from retry_domains import failure_disposition


class FakeNotifications:
    def __init__(self):
        self.rows = {}

    def global_stop(self):
        return {"active": False}

    def notification(self, channel, subject):
        return self.rows.get((channel, subject))

    def record_notification(self, channel, subject, fingerprint, payload):
        self.rows[(channel, subject)] = {"fingerprint": fingerprint, "payload": payload}


class OperatorUxTests(unittest.TestCase):
    def test_duplicate_terminal_gate_is_suppressed_semantically(self):
        repository = FakeNotifications()
        sent = []
        projection = {
            "job_id": "J1", "event_class": "TERMINAL", "status": "SKELETON_READY",
            "stage": "DONE", "failure_origin": "CONTRACT_BLOCKED",
            "failure_code": "DEFERRED_CONTRACT", "gate": "", "hold_scope": "JOB",
            "blocked_by_job_id": "", "root_failure_code": "",
            "user_action_required": True, "terminal_disposition": "PRESERVED_UNVERIFIED",
            "root_blocker": "", "product_readiness": "SKELETON_READY",
            "open_decision_count": 1, "message": "one compact terminal",
        }
        sender = lambda message: sent.append(message) or {"delivered": True}
        self.assertTrue(deliver_semantic(projection, repository, sender)["delivered"])
        replay = deliver_semantic(projection, repository, sender)
        self.assertTrue(replay["duplicate_suppressed"])
        self.assertEqual(["one compact terminal"], sent)

    def test_terminal_report_exposes_product_readiness_not_full_requirement(self):
        requirement = "very long authoritative requirement that must not be repeated"
        job = {
            "job_id": "J1", "client_job_id": "C1", "status": "SKELETON_READY",
            "request": {"requirement": requirement}, "hold_scope": "JOB",
            "last_result": {
                "stage": "DONE", "failure_code": "DEFERRED_CONTRACT",
                "failure_origin": "CONTRACT_BLOCKED", "worktree_disposition": "PRESERVED",
                "product_readiness": "SKELETON_READY", "open_decision_count": 2,
            },
        }
        projection = terminal_projection(job, {"batches": {}, "gate_reason": ""})
        self.assertIn("Product: SKELETON_READY", projection["message"])
        self.assertIn("Open decisions: 2", projection["message"])
        self.assertNotIn(requirement, projection["message"])

    def test_compact_heartbeat_shows_decision_once(self):
        snapshot = {
            "job": {"operator_label": "J1", "elapsed_seconds": 65},
            "stage": {"name": "QA"}, "gates": {},
            "user_action_required": True, "open_decision_count": 1,
        }
        rendered = render_compact(snapshot, heartbeat=True)
        self.assertEqual(1, rendered.count("DECISION REQUIRED"))

    def test_durable_continuation_does_not_repeat_contract(self):
        prompt = durable_continuation_prompt()
        self.assertLess(len(prompt), 220)
        self.assertNotIn("[작업 요구사항]", prompt)

    def test_repeated_constraint_diet_is_deterministic(self):
        compact, removed = deduplicate_repeated_lines("Keep this invariant\nshort\nKeep this invariant")
        self.assertEqual(1, removed)
        self.assertEqual("Keep this invariant\nshort", compact)

    def test_selective_revalidation_skips_worker_for_runtime_or_test_evidence(self):
        runtime = plan_revalidation(
            {"runtime_process_id": "P1", "runtime_health": "LOST"},
            {"runtime_process_id": "P2", "runtime_health": "VALID"},
        )
        self.assertEqual(["RUNTIME_PREFLIGHT"], runtime.public()["gates"])
        self.assertFalse(runtime.worker_rerun)
        tests = plan_revalidation(
            {"test_scope_hash": "A", "test_evidence_hash": "A"},
            {"test_scope_hash": "A", "test_evidence_hash": "B"},
        )
        self.assertEqual(("TEST", "REVIEW", "VERIFICATION"), tests.gates)
        self.assertFalse(tests.worker_rerun)

    def test_source_change_revalidates_product_gates(self):
        planned = plan_revalidation({"candidate_hash": "A"}, {"candidate_hash": "B"})
        self.assertTrue(planned.worker_rerun)
        self.assertEqual(("WORKER", "BUILD", "TEST", "REVIEW", "VERIFICATION"), planned.gates)

    def test_runtime_recovery_exhaustion_cannot_be_product_failed_final(self):
        status, code = failure_disposition({
            "retry_domain": "RUNTIME_RECOVERY", "failure_type": "infrastructure",
            "fix_scope": "NON_RETRYABLE", "worktree_disposition": "NO_DELTA",
        }, outer_attempt=99, max_outer_attempts=1)
        self.assertEqual("BLOCKED", status)
        self.assertEqual("RUNTIME_OR_TECHNICAL_RECOVERY_REQUIRED", code)

    def test_real_review_product_defect_uses_semantic_budget(self):
        retry, _ = failure_disposition({
            "retry_domain": "PRODUCT_SEMANTIC_RETRY", "failure_type": "review",
            "fix_scope": "RETRYABLE", "worktree_disposition": "CLEAN_ROLLBACK",
        }, outer_attempt=1, max_outer_attempts=2)
        final, code = failure_disposition({
            "retry_domain": "PRODUCT_SEMANTIC_RETRY", "failure_type": "review",
            "fix_scope": "RETRYABLE", "worktree_disposition": "CLEAN_ROLLBACK",
        }, outer_attempt=2, max_outer_attempts=2)
        self.assertEqual("AWAITING_ENRICHMENT", retry)
        self.assertEqual(("FAILED_FINAL", "OUTER_RETRY_BUDGET_EXHAUSTED"), (final, code))

    def test_skeleton_allows_independent_job_and_holds_dependent_job(self):
        for dependent in (False, True):
            with self.subTest(dependent=dependent):
                temp = harness_temp.TemporaryDirectory()
                self.addCleanup(temp.cleanup)
                store = JobStore(Path(temp.name))
                first = request(client_job_id="J-SKELETON", depends_on=[], target_modules=["sample-service"])
                second = request(
                    client_job_id="J-NEXT",
                    depends_on=["J-SKELETON"] if dependent else [],
                    target_modules=["sample-service"],
                )
                jobs, _ = valid_enqueue(store, "skeleton-independent-" + str(dependent), [first, second])
                skeleton = store._read_job(jobs[0]["job_id"])
                skeleton["status"] = "SKELETON_READY"
                store._write_job(skeleton)
                queue = store._read_queue()
                queue.update(paused=False, operator_paused=False, gate_reason="", blocked_by_job_id="")
                store._write_queue(queue)
                claimed = store.claim_next("owner")
                if dependent:
                    self.assertIsNone(claimed)
                else:
                    self.assertEqual(jobs[1]["job_id"], claimed["job_id"])


if __name__ == "__main__":
    unittest.main()
