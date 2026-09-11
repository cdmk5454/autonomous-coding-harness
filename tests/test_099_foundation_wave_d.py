from pathlib import Path
import unittest

import harness_temp
from codex_runtime_adapter import CodexRuntimeAdapter
from native_tooling import lsp_decisions, mcp_capability, skill_capability, tooling_decisions
from opencode_profile import (
    OpenCodeProfileError, map_frozen_profile, remote_security_contract, windows_argv,
)
from operating_metrics import NOT_CAPTURED, UNKNOWN, aggregate_metrics
from runtime_selection import RuntimeSelectionError, assert_single_migration_axis


class OpenCodeProfileAndSecurityTests(unittest.TestCase):
    def setUp(self):
        self.temp = harness_temp.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for name in ("project-rules.md", "always.md", "analysis.md", "feature.json"):
            (self.root / name).write_text(name, encoding="utf-8")
        self.project = {
            "schema_version": 1, "id": "fixture", "workspace_roots": [str(self.root / "workspace")],
            "context": {"project_rules": "project-rules.md", "analysis": "analysis.md",
                        "feature_map": "feature.json"},
            "rules": {"always": ["always.md"]},
        }

    def test_profile_mapping_keeps_mandatory_context_outside_optional_discovery(self):
        mapping = map_frozen_profile(
            self.root, self.project, optional_skills=["review-helper"],
            mcp_servers=[{"name": "db", "identity": "db-readonly", "required": False}],
        )
        self.assertEqual(2, len(mapping["mandatory_context"]))
        self.assertEqual("OPTIONAL_DISCOVERABLE_SKILL", mapping["skills"]["classification"])
        self.assertEqual("NATIVE_ONLY", mapping["mcp"][0]["loader"])
        self.assertEqual("WINDOWS_NATIVE", mapping["os_toolchain_decision"])
        self.assertFalse(mapping["permissions"]["auto"])
        self.assertEqual(mapping, map_frozen_profile(
            self.root, self.project, optional_skills=["review-helper"],
            mcp_servers=[{"name": "db", "identity": "db-readonly", "required": False}],
        ))
        self.assertEqual(("tool.exe", "D:\\work path", "--flag"),
                         windows_argv("tool.exe", "D:\\work path", "--flag"))

    def test_duplicate_mcp_injection_is_rejected(self):
        with self.assertRaisesRegex(OpenCodeProfileError, "MCP_DUPLICATE_INJECTION_FORBIDDEN"):
            map_frozen_profile(self.root, self.project, mcp_servers=[
                {"name": "one", "identity": "same"}, {"name": "two", "identity": "SAME"},
            ])

    def test_remote_boundary_requires_auth_redaction_and_no_direct_mutation(self):
        redaction = {"api_secret": True, "provider_credential": True,
                     "db_credential": True, "pii": True}
        contract = remote_security_contract(
            endpoint="https://managed.example", authenticated=True,
            password_configured=True, private_network=False,
            managed_mutation_exposed=False, redaction=redaction,
        )
        self.assertFalse(contract["cors_is_authentication"])
        self.assertFalse(contract["permission_is_os_sandbox"])
        with self.assertRaisesRegex(OpenCodeProfileError, "REMOTE_PRIVATE_OR_AUTHENTICATED_TLS_REQUIRED"):
            remote_security_contract(
                endpoint="http://managed.example", authenticated=True,
                password_configured=True, private_network=False,
                managed_mutation_exposed=False, redaction=redaction,
            )
        with self.assertRaisesRegex(OpenCodeProfileError, "MANAGED_MUTATION_ENDPOINT_EXPOSURE_FORBIDDEN"):
            remote_security_contract(
                endpoint="https://managed.example", authenticated=True,
                password_configured=True, private_network=False,
                managed_mutation_exposed=True, redaction=redaction,
            )

    def test_runtime_and_model_tier_migrations_are_separate_axes(self):
        self.assertEqual("RUNTIME", assert_single_migration_axis(
            previous_runtime="droid", next_runtime="opencode",
            previous_model_tier="low-risk-default", next_model_tier="low-risk-default",
        ))
        with self.assertRaisesRegex(RuntimeSelectionError, "RUNTIME_AND_MODEL_MIGRATION_COUPLED"):
            assert_single_migration_axis(
                previous_runtime="droid", next_runtime="opencode",
                previous_model_tier="default", next_model_tier="premium",
            )


class NativeToolingAndTelemetryTests(unittest.TestCase):
    def test_native_skills_mcp_lsp_and_optional_tooling_decisions(self):
        skills = skill_capability("opencode", [{"name": "a"}, {"name": "a"}])
        self.assertEqual(["a"], skills["discovered"])
        self.assertFalse(skills["optional_failure_blocks_mandatory_context"])
        mcp = mcp_capability("codex", [{
            "name": "context", "availability": "AVAILABLE", "version": "1",
            "capability": ["tools"], "required": False, "health": "PASS",
        }])
        self.assertEqual(0, mcp["servers"][0]["product_retry_delta"])
        self.assertEqual("TECHNICAL_EXECUTION_RETRY", mcp["servers"][0]["failure_retry_domain"])
        decisions = lsp_decisions({
            "sample-service": {"diagnostic_gain": 0, "synchronization": "PASS", "latency_ms": 5, "memory_mb": 5},
            "fixture": {"diagnostic_gain": 1, "synchronization": "PASS", "latency_ms": 5, "memory_mb": 5},
        })
        self.assertEqual({"fixture": "ON", "sample-service": "OFF"}, decisions)
        optional = tooling_decisions(repeated_investigation_bottleneck=False)
        self.assertEqual("DEFERRED", optional["SERENA"])
        self.assertEqual("DEFERRED_TO_1_1", optional["CLAUDE_MEM"])
        self.assertFalse(optional["REPOMIX_PER_ATTEMPT_REPOSITORY_INJECTION"])

    def test_usage_and_missing_metrics_are_not_silently_zero(self):
        event = CodexRuntimeAdapter().normalize_event({
            "type": "turn.completed", "thread_id": "s", "turn_id": "t",
            "usage": {"input_tokens": 10, "cached_input_tokens": 4, "output_tokens": 3},
        })[0]
        self.assertEqual(10, event.usage["input_tokens"])
        self.assertEqual(NOT_CAPTURED, event.usage["context_size"])
        empty = aggregate_metrics([])
        self.assertEqual(UNKNOWN, empty["strict_success_rate"])
        self.assertEqual(NOT_CAPTURED, empty["token_telemetry"]["input_tokens"])
        self.assertEqual(NOT_CAPTURED, empty["context_lifecycle"]["ACCESSED"])
        observed = aggregate_metrics([
            {"kind": "JOB", "strict_success": True, "autonomous_complete": True,
             "harness_patch": False, "latency_ms": 10, "product_readiness": "COMPLETE",
             "input_tokens": 10, "context_selected": 2, "context_delivered": 2},
            {"kind": "STAGE", "latency_ms": 4},
        ], coverage={"wrong_session_binding_count": True,
                     "context_selected": True, "context_delivered": True})
        self.assertEqual(1.0, observed["strict_success_rate"])
        self.assertEqual(0, observed["wrong_session_binding_count"])
        self.assertEqual(2, observed["context_lifecycle"]["SELECTED"])
        self.assertEqual(NOT_CAPTURED, observed["context_lifecycle"]["ACCESSED"])


if __name__ == "__main__":
    unittest.main()
