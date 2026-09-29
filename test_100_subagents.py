import unittest

from job_contract import JobContractError, JobRequest


class GenericSubagentContractTest(unittest.TestCase):
    def request(self, **extra):
        data = {"client_job_id": "A", "requirement": "Change one source file",
                "worker": "codex", "model": "gpt-5.6-sol", "reasoning_effort": "low",
                "target_modules": ["app"], "target_resources": ["app/a.py"],
                "commit_summary": "fixture", "max_outer_attempts": 1, **extra}
        return JobRequest.from_mapping(data, allowed_modules=["app"])

    def test_default_denies_without_changing_legacy_serialization(self):
        request = self.request()
        self.assertEqual({}, request.subagents)
        self.assertNotIn("subagents", request.to_dict())

    def test_generic_authorization_is_frozen_without_writer_capability(self):
        policy = {"policy": "ALLOW", "max_parallel": 2, "writer_allowed": False,
                  "roles": ["source-audit", "test-discovery"]}
        request = self.request(subagents=policy)
        self.assertEqual(policy, request.to_dict()["subagents"])

    def test_writer_and_unbounded_parallelism_are_rejected(self):
        for policy in ({"policy": "ALLOW", "max_parallel": 1, "writer_allowed": True},
                       {"policy": "ALLOW", "max_parallel": 100},
                       {"policy": "DENY", "max_parallel": 1},
                       {"policy": "RECOMMEND", "max_parallel": 1, "roles": ["a", "b"]}):
            with self.subTest(policy=policy), self.assertRaisesRegex(JobContractError, "SUBAGENT_POLICY_INVALID"):
                self.request(subagents=policy)


if __name__ == "__main__":
    unittest.main()
