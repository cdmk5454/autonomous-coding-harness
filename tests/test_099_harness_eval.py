import json
from pathlib import Path
import unittest

import harness_temp
from control_repository import SCHEMA_VERSION
from harness_eval import GRADERS, environment_manifest, run_corpus


class HarnessEvalSuiteTests(unittest.TestCase):
    def environment(self):
        return environment_manifest(
            harness_version=Path("VERSION").read_text(encoding="utf-8").strip(),
            control_schema_revision=SCHEMA_VERSION,
            runtime_contract={"runtime": "fixture", "runtime_version": "1",
                              "protocol_revision": "fixture/1",
                              "permission_tool_profile": "fixture-readonly/1"},
            source_view={"source_view_id": "SV-EVAL", "base_manifest_hash": "base"},
            execution_budget={"product_retry": 3, "runtime_recovery": 1},
        )

    def test_meaningful_corpus_and_release_gate_pass(self):
        result = run_corpus("evals/corpus_099.json", environment=self.environment())
        self.assertGreaterEqual(result["total"], 30)
        self.assertLessEqual(result["total"], 50)
        self.assertEqual(result["total"], result["passed"])
        self.assertEqual("PASS", result["release_gate"])
        self.assertTrue(result["critical_all_pass"])
        for field in (
            "forbidden_outcome_count", "false_success_count", "false_complete_count",
            "runtime_to_product_retry_leak_count", "stale_qa_acceptance_count",
            "unsafe_writer_overlap_count", "secret_in_process_argv_count",
            "secret_persistence_leak_count",
        ):
            self.assertEqual(0, result[field], field)
        self.assertEqual(set(GRADERS), set(result["results"][0]["grader_failures"]))
        security = next(
            item for item in result["results"]
            if item["eval_id"] == "SECURITY-SECRET-IN-PROCESS-ARGV"
        )
        self.assertTrue(security["passed"])
        self.assertFalse(security["actual"]["flags"]["secret_in_process_argv"])

    def test_deterministic_grader_detects_wrong_expectation(self):
        corpus = json.loads(Path("evals/corpus_099.json").read_text(encoding="utf-8"))
        corpus["cases"] = [dict(corpus["cases"][0])]
        corpus["cases"][0]["expected"] = {"state": "SUCCEEDED"}
        with harness_temp.TemporaryDirectory() as directory:
            path = Path(directory) / "broken.json"
            path.write_text(json.dumps(corpus), encoding="utf-8")
            result = run_corpus(path, environment=self.environment())
        self.assertEqual("FAIL", result["release_gate"])
        self.assertIn("state", result["results"][0]["grader_failures"]["STATE_GRADER"])


if __name__ == "__main__":
    unittest.main()
