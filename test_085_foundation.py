from __future__ import annotations

import hashlib
import json
import harness_temp as tempfile
import unittest
from dataclasses import replace
from pathlib import Path

import context_foundation as cf
from context_foundation import (
    Authority,
    ContextError,
    ContextResource,
    ProfileFact,
    build_context_pack,
    build_task_spec,
    deduplicate_resources,
    evaluate_approval,
    manifest_has_sensitive_payload,
    persist_context_manifest,
    resolve_profile_resource,
    validate_profile_fact,
)
from golden_context_eval import run_golden_context_eval
from project_profile import ProfileError, load_project_profile, normalize_relative_path
from runtime_identity import RuntimeIdentity, RuntimeIdentityError
from task_state import TaskState
from worker import build_prompt


PROFILE_DIR = Path(r"D:\agents\profiles\e4u")
WORKSPACE = Path(r"D:\inha_dev")
POLICY_ROOT = Path(r"D:\agents")


class FoundationBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.profile = load_project_profile(PROFILE_DIR, WORKSPACE)

    def pack(self, role="WORKER", requirement="sucui 문구 수정", modules=("sucui",), changed=(), **kwargs):
        return build_context_pack(
            role=role,
            requirement=requirement,
            profile=self.profile,
            workspace=WORKSPACE,
            policy_root=POLICY_ROOT,
            target_modules=modules,
            changed_files=changed,
            task_mode=kwargs.pop("task_mode", "MODIFICATION"),
            **kwargs,
        )


class RuntimeIdentityTests(unittest.TestCase):
    def _release(self, root: Path, version: str, build_id: str = "") -> Path:
        (root / "VERSION").write_text(version + "\n", encoding="utf-8")
        directory = root / "releases" / version
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / "SHA256SUMS.json"
        path.write_text(json.dumps({"version": version, "build_id": build_id}), encoding="utf-8")
        return path

    def test_startup_loaded_identity_is_immutable_until_restart(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            first_manifest = self._release(root, "0.8.4", "BUILD-A")
            first = RuntimeIdentity.load(root, pid=101, started_at="2026-01-01T00:00:00+09:00")
            self._release(root, "0.8.5", "BUILD-B")
            self.assertEqual(first.harness_version, "0.8.4")
            self.assertEqual(first.runtime_build_id, "BUILD-A")
            self.assertEqual(first.runtime_manifest_sha256, hashlib.sha256(first_manifest.read_bytes()).hexdigest())
            restarted = RuntimeIdentity.load(root, pid=202, started_at="2026-01-02T00:00:00+09:00")
            self.assertEqual(restarted.harness_version, "0.8.5")
            self.assertEqual(restarted.runtime_pid, 202)
            self.assertNotEqual(first.runtime_started_at, restarted.runtime_started_at)

    def test_manifest_version_mismatch_fails_closed(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "VERSION").write_text("0.8.5", encoding="utf-8")
            path = root / "releases" / "0.8.5"
            path.mkdir(parents=True)
            (path / "SHA256SUMS.json").write_text('{"version":"0.8.4"}', encoding="utf-8")
            with self.assertRaisesRegex(RuntimeIdentityError, "RUNTIME_MANIFEST_VERSION_MISMATCH"):
                RuntimeIdentity.load(root)

    def test_public_runtime_identity_has_no_environment_or_secret(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            self._release(root, "0.8.5")
            keys = set(RuntimeIdentity.load(root).public())
            self.assertEqual(keys, {
                "harness_version", "runtime_build_id", "runtime_manifest_sha256",
                "runtime_started_at", "runtime_pid",
            })


class TaskSpecAndResolutionTests(FoundationBase):
    def test_task_spec_preserves_only_explicit_identifiers(self):
        spec = build_task_spec(
            "suc/src/main/java/suc/sp/query/SsucSp0021Qry.xml에서 SSUC383 SEQ=1을 반드시 사용하고 MAX(SEQ)는 금지",
            ["suc"], task_mode="MODIFICATION", workspace=WORKSPACE,
        )
        self.assertEqual(spec.intent, "MODIFICATION")
        self.assertIn("suc/src/main/java/suc/sp/query/SsucSp0021Qry.xml", spec.explicit_paths)
        self.assertEqual(spec.query_files, ("SsucSp0021Qry.xml",))
        self.assertEqual(spec.tables, ("SSUC383",))
        self.assertTrue(any("반드시" in item for item in spec.constraints))
        self.assertTrue(any("금지" in item for item in spec.exclusions))
        self.assertEqual(spec.unknowns, ())

    def test_profile_local_paths_are_canonical(self):
        analysis, warnings = resolve_profile_resource(self.profile, "analysis")
        ui_map, ui_warnings = resolve_profile_resource(self.profile, "ui_map")
        self.assertEqual(analysis, (PROFILE_DIR / "PROJECT_ANALYSIS.md").resolve())
        self.assertEqual(ui_map, (PROFILE_DIR / "ui_map").resolve())
        self.assertEqual(warnings + ui_warnings, [])

    def test_legacy_fallback_is_explicit_warning(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            (root / "PROJECT_ANALYSIS.md").write_text("# legacy", encoding="utf-8")
            missing = replace(self.profile, analysis=PROFILE_DIR / "missing.md")
            path, warnings = resolve_profile_resource(missing, "analysis", legacy_root=root)
            self.assertEqual(path, (root / "PROJECT_ANALYSIS.md").resolve())
            self.assertEqual(warnings, ["LEGACY_CONTEXT_FALLBACK:analysis"])

    def test_path_traversal_rejected_by_profile_normalizer(self):
        with self.assertRaises(ProfileError):
            normalize_relative_path("../outside.md")


class AuthorityAndBudgetTests(unittest.TestCase):
    def resource(self, path: str, authority: Authority, text: str, rule="", required=False, score=0):
        return ContextResource(
            path=path, section="s", authority=authority, text=text,
            rule_ids=(rule,) if rule else (), selection_reason="TEST",
            source_sha256=hashlib.sha256(text.encode()).hexdigest(), required=required, score=score,
        )

    def test_rule_id_deduplicates_identical_semantics(self):
        resources = [
            self.resource("a", Authority.HARD_RULE, "same rule", "COMMON.X"),
            self.resource("b", Authority.HARD_RULE, "same   rule", "COMMON.X"),
        ]
        selected, deduped, conflicts = deduplicate_resources(resources)
        self.assertEqual(len(selected), 1)
        self.assertIn("COMMON.X", deduped)
        self.assertEqual(conflicts, [])

    def test_conflicting_rule_id_fails_closed(self):
        resources = [
            self.resource("a", Authority.HARD_RULE, "allow", "COMMON.X"),
            self.resource("b", Authority.HARD_RULE, "deny", "COMMON.X"),
        ]
        with self.assertRaisesRegex(ContextError, "CONTEXT_RULE_CONFLICT"):
            deduplicate_resources(resources)

    def test_reference_cannot_override_verified_or_hard_fact(self):
        resources = [
            self.resource("fact", Authority.VERIFIED_FACT, "java=17", "FACT.JAVA"),
            self.resource("ref", Authority.REFERENCE, "java=8", "FACT.JAVA"),
        ]
        selected, deduped, conflicts = deduplicate_resources(resources)
        self.assertEqual([(item.path, item.text) for item in selected], [("fact", "java=17")])
        self.assertIn("FACT.JAVA", deduped)
        self.assertEqual(conflicts, [])

    def test_budget_omits_low_priority_and_keeps_hard_rule(self):
        hard = self.resource("hard", Authority.HARD_RULE, "H" * 20, required=True)
        ref = self.resource("ref", Authority.REFERENCE, "R" * 30)
        selected, omitted = cf._apply_budget([ref, hard], 25)
        self.assertEqual([item.path for item in selected], ["hard"])
        self.assertEqual(omitted[0]["reason"], "CONTEXT_BUDGET_LOW_PRIORITY_OMITTED")

    def test_hard_rule_budget_overflow_fails_closed(self):
        hard = self.resource("hard", Authority.HARD_RULE, "H" * 30, required=True)
        with self.assertRaisesRegex(ContextError, "CONTEXT_BUDGET_EXCEEDED"):
            cf._apply_budget([hard], 20)


class SelectionAndRolePackTests(FoundationBase):
    def test_exact_query_and_table_select_sql_contract(self):
        pack = self.pack(
            requirement="suc/src/main/java/suc/sp/query/SsucSp0021Qry.xml SSUC383 수정",
            modules=("suc",),
        )
        paths = [item["path"] for item in pack.manifest["selected_resources"]]
        self.assertIn("policy:skills/sql/SKILL.md", paths)
        self.assertTrue(any("SSUC383_CURRENT.md" in path for path in paths))
        self.assertFalse(any("ui_map" in path for path in paths))

    def test_generic_module_word_does_not_select_analysis_overview(self):
        pack = self.pack(requirement="suc module 문구 수정", modules=("suc",))
        self.assertFalse(any(
            item["path"] == "profile:PROJECT_ANALYSIS.md"
            for item in pack.manifest["selected_resources"]
        ))

    def test_changed_query_refresh_adds_sql(self):
        initial = self.pack(requirement="suc 백엔드 서비스 수정", modules=("suc",))
        refreshed = self.pack(
            requirement="suc 백엔드 서비스 수정", modules=("suc",),
            changed=("suc/src/main/java/suc/sp/query/SsucSp0021Qry.xml",),
        )
        initial_paths = [item["path"] for item in initial.manifest["selected_resources"]]
        refreshed_paths = [item["path"] for item in refreshed.manifest["selected_resources"]]
        self.assertNotIn("policy:skills/sql/SKILL.md", initial_paths)
        self.assertIn("policy:skills/sql/SKILL.md", refreshed_paths)
        self.assertNotEqual(initial.manifest["context_pack_id"], refreshed.manifest["context_pack_id"])

    def test_role_specific_packs_have_no_cross_role_skill_leakage(self):
        changed = ("suc/src/main/java/suc/sp/query/SsucSp0021Qry.xml",)
        expected = {
            "PLANNER": "policy:skills/planning/SKILL.md",
            "REVIEWER": "policy:skills/review/SKILL.md",
            "TESTER": "policy:skills/testing/SKILL.md",
        }
        for role, role_path in expected.items():
            with self.subTest(role=role):
                pack = self.pack(role=role, requirement="U03 검증", modules=("suc",), changed=changed)
                paths = [item["path"] for item in pack.manifest["selected_resources"]]
                self.assertIn(role_path, paths)
                other = set(expected.values()) - {role_path}
                self.assertTrue(other.isdisjoint(paths))
                self.assertNotIn("policy:skills/backend/SKILL.md", paths)
                self.assertNotIn("policy:skills/sql/SKILL.md", paths)

    def test_analysis_pack_does_not_create_modification_authority(self):
        pack = self.pack(requirement="현재 구조만 분석", task_mode="ANALYSIS")
        self.assertEqual(pack.task_spec.intent, "ANALYSIS")
        self.assertNotIn("승인된 MODIFICATION Job", pack.prompt)

    def test_worker_prompt_distinguishes_approved_modification(self):
        prompt = build_prompt(
            "sucui 문구 수정", ["sucui"], "MODIFICATION",
            profile=self.profile, role="droid",
        )
        self.assertIn("승인된 MODIFICATION Job", prompt)


class ApprovalTests(unittest.TestCase):
    def test_interactive_modification_requires_approval(self):
        result = evaluate_approval(task_mode="MODIFICATION", manager_approved=False)
        self.assertFalse(result.allowed)
        self.assertEqual(result.code, "INTERACTIVE_MODIFICATION_REQUIRES_APPROVAL")

    def test_approved_modification_proceeds_without_generic_question(self):
        result = evaluate_approval(task_mode="MODIFICATION", manager_approved=True)
        self.assertTrue(result.allowed)
        self.assertFalse(result.requires_human)

    def test_scope_expansion_requires_human(self):
        result = evaluate_approval(task_mode="MODIFICATION", manager_approved=True, scope_expansion=True)
        self.assertEqual(result.code, "SCOPE_EXPANSION_REQUIRES_HUMAN")

    def test_high_risk_unknown_requires_human(self):
        result = evaluate_approval(task_mode="MODIFICATION", manager_approved=True, high_risk_unknown=True)
        self.assertEqual(result.code, "HIGH_RISK_UNKNOWN_REQUIRES_HUMAN")

    def test_destructive_action_requires_human(self):
        result = evaluate_approval(task_mode="MODIFICATION", manager_approved=True, destructive_action=True)
        self.assertEqual(result.code, "DESTRUCTIVE_ACTION_REQUIRES_HUMAN")


class ManifestAndFactTests(FoundationBase):
    def test_context_pack_id_is_stable_and_manifest_has_no_prompt(self):
        first = self.pack()
        second = self.pack()
        self.assertEqual(first.manifest["context_pack_id"], second.manifest["context_pack_id"])
        self.assertNotIn("prompt", first.manifest)
        self.assertFalse(manifest_has_sensitive_payload(first.manifest))
        self.assertTrue(all(item["source_sha256"] for item in first.manifest["selected_resources"]))

    def test_changed_source_hash_changes_pack_id(self):
        first = self.pack(requirement="sucui 문구 A 수정")
        second = self.pack(requirement="sucui 문구 B 수정")
        self.assertNotEqual(first.manifest["context_pack_id"], second.manifest["context_pack_id"])

    def test_manifest_persistence_links_only_bounded_metadata(self):
        pack = self.pack()
        task = TaskState("TASK-085-MANIFEST", "sucui 문구 수정")
        with tempfile.TemporaryDirectory() as td:
            path = persist_context_manifest(pack, task, td)
            payload = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(task.context_pack_id, pack.manifest["context_pack_id"])
            self.assertEqual(task.context_manifest_path, path.as_posix())
            self.assertEqual(task.context_selected_count, len(payload["selected_resources"]))
            self.assertFalse(manifest_has_sensitive_payload(payload))

    def test_old_task_state_defaults_remain_compatible(self):
        task = TaskState("TASK-OLD", "x")
        self.assertEqual(task.context_pack_id, "")
        self.assertEqual(task.context_warnings, [])

    def test_fact_hash_status_verified_then_stale(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            path = root / "fact.txt"
            path.write_text("v1", encoding="utf-8")
            expected = hashlib.sha256(path.read_bytes()).hexdigest()
            fact = ProfileFact("FACT.X", "v1", ("x",), (str(path),), {str(path): expected})
            self.assertEqual(validate_profile_fact(fact, [root]).status, "VERIFIED")
            path.write_text("v2", encoding="utf-8")
            self.assertEqual(validate_profile_fact(fact, [root]).status, "STALE")

    def test_profile_facts_come_from_actual_build_configs(self):
        pack = self.pack(requirement="sucui build 확인", modules=("sucui",))
        facts = next(
            item for item in pack.manifest["selected_resources"]
            if item["path"] == "derived:profile-facts"
        )
        self.assertEqual(facts["authority"], "VERIFIED_FACT")
        self.assertIn("npm.cmd", pack.prompt)
        self.assertNotIn("사용자의 Java", pack.prompt)

    def test_unknown_fact_is_not_promoted_to_verified(self):
        fact = ProfileFact("FACT.UNKNOWN", "", ("suc",), (), {}, status="UNKNOWN")
        self.assertEqual(fact.status, "UNKNOWN")


class SkillPolicyAndGoldenTests(unittest.TestCase):
    def test_skill_policy_contracts(self):
        context = (POLICY_ROOT / "skills" / "project-context" / "SKILL.md").read_text(encoding="utf-8")
        testing = (POLICY_ROOT / "skills" / "testing" / "SKILL.md").read_text(encoding="utf-8")
        lock = json.loads((POLICY_ROOT / ".skill-lock.json").read_text(encoding="utf-8"))
        self.assertIn("selected profile", context)
        for status in ("PASS", "FAIL", "NOT_REQUIRED", "UNAVAILABLE", "NOT_RUN_DUE_TO_PRIOR_GATE"):
            self.assertIn(status, testing)
        self.assertIn("ponytail", lock["skills"])
        self.assertNotIn("executing-plans", lock["skills"])

    def test_golden_context_eval(self):
        result = run_golden_context_eval(PROFILE_DIR, WORKSPACE, POLICY_ROOT)
        self.assertTrue(result["passed"], result["summary"])
        self.assertEqual(result["summary"]["passed"], 8)
        self.assertEqual(result["summary"]["mandatory_rule_recall_percent"], 100.0)
        self.assertGreaterEqual(result["summary"]["simple_static_prompt_median_reduction_percent"], 30.0)
        self.assertEqual(result["model_calls"], 0)


if __name__ == "__main__":
    unittest.main()
