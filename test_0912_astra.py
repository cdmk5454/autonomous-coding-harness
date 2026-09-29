import unittest

from job_contract import JobRequest, JobContractError
from materialization_policy import freeze_routing, resolve_constraints, PolicyConflict


class AstraCompatibilityTest(unittest.TestCase):
    def test_astra_materialization(self):
        for effort in ("medium", "high"):
            for constraints in ({}, {"exact": "gpt-6-astra", "max": "HIGH"}):
                request = JobRequest.from_mapping({
                    "requirement": "Validate scratch compatibility",
                    "worker": "codex", "model": "gpt-6-astra",
                    "reasoning_effort": effort, "execution_policy": constraints,
                }, allowed_modules=[])
                route = freeze_routing(request, {})["resolved_worker"]
                self.assertEqual("gpt-6-astra", route["model"])
                self.assertEqual(effort, route["reasoning_effort"])

    def test_unknown_rejected(self):
        with self.assertRaises(JobContractError):
            JobRequest.from_mapping({"requirement": "scratch", "model": "unknown"}, allowed_modules=[])
        with self.assertRaises(PolicyConflict):
            resolve_constraints({"exact": "unknown"})

    def test_legacy_routing_preserved(self):
        for model in ("gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.6-luna"):
            self.assertEqual(model, resolve_constraints({}, default_model=model)["model"])
        self.assertEqual("gpt-5.6-sol", resolve_constraints({"preferred": "HIGH"})["model"])


if __name__ == "__main__":
    unittest.main()
