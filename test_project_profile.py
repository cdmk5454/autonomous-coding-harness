from __future__ import annotations

import io
import json
import os
import subprocess
import harness_temp as tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

import run
import project_profile
from project_profile import (
    ProfileError,
    load_project_profile,
    select_project_profile,
)
from worker import build_prompt, detect_areas


class ProfileFixture:
    def __init__(self, root: Path, profile_id: str = "sample"):
        self.workspace = root / f"workspace-{profile_id}"
        self.profile = root / "agents" / "profiles" / profile_id
        self.workspace.mkdir(parents=True)
        self.profile.mkdir(parents=True)
        (self.workspace / "server").mkdir()
        (self.workspace / "web").mkdir()
        (self.profile / "hooks").mkdir()
        (self.profile / "refs").mkdir()
        (self.profile / "ui").mkdir()
        (self.profile / "project-rules.md").write_text(
            f"# Rules {profile_id}\nPROFILE_RULE_{profile_id}", encoding="utf-8"
        )
        (self.profile / "analysis.md").write_text(
            f"# Analysis {profile_id}\n\n## server architecture\nANALYSIS_{profile_id}",
            encoding="utf-8",
        )
        (self.profile / "hooks" / "always.md").write_text("ALWAYS_RULE", encoding="utf-8")
        (self.profile / "refs" / "backend.md").write_text("BACKEND_REF", encoding="utf-8")
        self.data = {
            "schema_version": 1,
            "id": profile_id,
            "display_name": profile_id.title(),
            "workspace_roots": [str(self.workspace)],
            "context": {
                "project_rules": "project-rules.md",
                "analysis": "analysis.md",
                "ui_map": "ui",
            },
            "modules": [
                {"name": "server", "path": "server", "kind": "backend", "protected": False},
                {"name": "web", "path": "web", "kind": "frontend", "protected": False},
            ],
            "rules": {
                "always": ["hooks/always.md"],
                "backend": ["refs/backend.md"],
                "frontend": [],
                "sql": [],
                "architecture": [],
                "egov": [],
            },
            "area_patterns": {
                "backend": ["server/**"],
                "frontend": ["web/**"],
                "sql": ["server/**/*Qry.xml"],
                "architecture": ["server/pom.xml"],
            },
            "protected_paths": ["server/infra/**"],
            "local_dev_config_excludes": [".env.local"],
        }
        self.write()

    def write(self):
        (self.profile / "project.json").write_text(
            json.dumps(self.data, ensure_ascii=False, indent=2), encoding="utf-8"
        )


class ProjectProfileLoadTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.fx = ProfileFixture(self.root)

    def tearDown(self):
        self.tmp.cleanup()

    def load(self):
        return load_project_profile(self.fx.profile, self.fx.workspace)

    def test_loads_real_e4u_profile_with_explicit_u03_builds(self):
        profile = load_project_profile(r"D:\agents\profiles\e4u", r"D:\inha_dev")
        self.assertEqual(profile.id, "e4u")
        self.assertEqual(len(profile.modules), 8)
        modules = {module.name: module for module in profile.modules}
        self.assertEqual(
            modules["suc"].build_argv,
            ("mvn.cmd", "clean", "install", "-P", "dev"),
        )
        self.assertEqual(
            modules["sucui"].build_argv, ("npm.cmd", "run", "build:dev")
        )
        self.assertTrue(
            all(
                not module.build_argv
                for name, module in modules.items()
                if name not in {"suc", "sucui"}
            )
        )

    def test_build_is_optional(self):
        self.assertEqual(self.load().build_mode(self.fx.workspace), "unavailable")

    def test_explicit_build_argv_is_valid(self):
        self.fx.data["modules"][0]["build"] = {"cwd": "server", "argv": ["python", "-m", "compileall", "."]}
        self.fx.write()
        profile = self.load()
        self.assertEqual(profile.build_mode(self.fx.workspace), "explicit")
        self.assertEqual(profile.modules[0].build_argv[0], "python")

    def test_rejects_unsupported_schema(self):
        self.fx.data["schema_version"] = 99
        self.fx.write()
        with self.assertRaisesRegex(ProfileError, "schema_version"):
            self.load()

    def test_rejects_missing_project_json(self):
        (self.fx.profile / "project.json").unlink()
        with self.assertRaisesRegex(ProfileError, "project.json"):
            self.load()

    def test_rejects_missing_required_context_and_rule(self):
        del self.fx.data["context"]["analysis"]
        self.fx.write()
        with self.assertRaisesRegex(ProfileError, "analysis"):
            self.load()
        self.fx = ProfileFixture(self.root / "second", "second")
        self.fx.data["rules"]["always"] = ["hooks/missing.md"]
        self.fx.write()
        with self.assertRaisesRegex(ProfileError, "PROFILE_PATH_MISSING"):
            self.load()

    def test_schema_rejects_additional_top_level_field_without_value_leak(self):
        self.fx.data["unexpected"] = "TOP-SECRET-VALUE"
        self.fx.write()
        with self.assertRaises(ProfileError) as raised:
            self.load()
        self.assertEqual(raised.exception.code, "PROFILE_MANIFEST_SCHEMA_INVALID")
        self.assertEqual(raised.exception.path, "$")
        self.assertNotIn("TOP-SECRET-VALUE", str(raised.exception))

    def test_schema_requires_ui_map_and_egov(self):
        del self.fx.data["context"]["ui_map"]
        self.fx.write()
        with self.assertRaises(ProfileError) as raised:
            self.load()
        self.assertEqual(raised.exception.path, "$.context.ui_map")

        self.fx = ProfileFixture(self.root / "egov", "egov")
        del self.fx.data["rules"]["egov"]
        self.fx.write()
        with self.assertRaises(ProfileError) as raised:
            self.load()
        self.assertEqual(raised.exception.path, "$.rules.egov")

    def test_rejects_profile_id_directory_mismatch(self):
        self.fx.data["id"] = "different"
        self.fx.write()
        with self.assertRaises(ProfileError) as raised:
            self.load()
        self.assertEqual(raised.exception.code, "PROFILE_ID_DIRECTORY_MISMATCH")
        self.assertEqual(raised.exception.path, "$.id")

    def test_schema_file_missing_invalid_json_and_invalid_schema_fail_closed(self):
        missing = self.root / "missing-schema.json"
        with mock.patch.object(project_profile, "PROFILE_SCHEMA_FILE", missing):
            with self.assertRaises(ProfileError) as raised:
                self.load()
        self.assertEqual(raised.exception.code, "PROFILE_SCHEMA_MISSING")

        bad_json = self.root / "bad-schema.json"
        bad_json.write_text("{", encoding="utf-8")
        with mock.patch.object(project_profile, "PROFILE_SCHEMA_FILE", bad_json):
            with self.assertRaises(ProfileError) as raised:
                self.load()
        self.assertEqual(raised.exception.code, "PROFILE_SCHEMA_JSON_INVALID")

        bad_schema = self.root / "invalid-schema.json"
        bad_schema.write_text(json.dumps({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": 7,
        }), encoding="utf-8")
        with mock.patch.object(project_profile, "PROFILE_SCHEMA_FILE", bad_schema):
            with self.assertRaises(ProfileError) as raised:
                self.load()
        self.assertEqual(raised.exception.code, "PROFILE_SCHEMA_INVALID")

    def test_schema_engine_unavailable_fails_closed(self):
        original_import = __import__

        def blocked_import(name, *args, **kwargs):
            if name == "jsonschema" or name.startswith("jsonschema."):
                raise ImportError("blocked for test")
            return original_import(name, *args, **kwargs)

        with mock.patch("builtins.__import__", side_effect=blocked_import):
            with self.assertRaises(ProfileError) as raised:
                self.load()
        self.assertEqual(raised.exception.code, "PROFILE_SCHEMA_ENGINE_UNAVAILABLE")

    def test_rejects_relative_escape(self):
        self.fx.data["context"]["analysis"] = "../analysis.md"
        self.fx.write()
        with self.assertRaisesRegex(ProfileError, "PROFILE_MANIFEST_SCHEMA_INVALID"):
            self.load()

    def test_rejects_absolute_profile_path(self):
        self.fx.data["context"]["analysis"] = str((self.root / "outside.md").resolve())
        self.fx.write()
        with self.assertRaisesRegex(ProfileError, "PROFILE_MANIFEST_SCHEMA_INVALID"):
            self.load()

    def test_rejects_symlink_escape_after_schema_validation(self):
        outside = self.root / "outside-ui"
        outside.mkdir()
        ui_map = self.fx.profile / "ui"
        ui_map.rmdir()
        created = subprocess.run(
            ["cmd", "/d", "/c", "mklink", "/J", str(ui_map), str(outside)],
            capture_output=True,
            text=True,
        )
        self.assertEqual(created.returncode, 0, created.stderr)
        try:
            with self.assertRaises(ProfileError) as raised:
                self.load()
            self.assertEqual(raised.exception.code, "PROFILE_PATH_OUTSIDE")
            self.assertEqual(raised.exception.path, "$.context.ui_map")
        finally:
            os.rmdir(ui_map)

    def test_rejects_duplicate_module_name_and_path(self):
        self.fx.data["modules"].append(
            {"name": "server", "path": "other", "kind": "backend", "protected": False}
        )
        self.fx.write()
        with self.assertRaisesRegex(ProfileError, "PROFILE_MODULE_NAME_DUPLICATE"):
            self.load()
        self.fx = ProfileFixture(self.root / "third", "third")
        self.fx.data["modules"].append(
            {"name": "other", "path": "server", "kind": "backend", "protected": False}
        )
        self.fx.write()
        with self.assertRaisesRegex(ProfileError, "PROFILE_MODULE_PATH_DUPLICATE"):
            self.load()

    def test_rejects_workspace_mismatch(self):
        other = self.root / "other-workspace"
        other.mkdir()
        with self.assertRaisesRegex(ProfileError, "PROFILE_WORKSPACE_MISMATCH"):
            load_project_profile(self.fx.profile, other)


class ProfileSelectionTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.first = ProfileFixture(self.root, "first")

    def tearDown(self):
        self.tmp.cleanup()

    def test_cli_precedes_environment(self):
        selected = select_project_profile(
            self.first.workspace,
            self.first.profile,
            env={"AGENT_PROFILE_DIR": str(self.root / "missing")},
        )
        self.assertEqual(selected.id, "first")

    def test_environment_fallback(self):
        selected = select_project_profile(
            self.first.workspace, env={"AGENT_PROFILE_DIR": str(self.first.profile)}
        )
        self.assertEqual(selected.id, "first")

    def test_single_exact_workspace_auto_discovery(self):
        selected = select_project_profile(
            self.first.workspace, env={}, agents_dir=self.root / "agents"
        )
        self.assertEqual(selected.id, "first")

    def test_multiple_exact_profiles_are_rejected(self):
        second = ProfileFixture(self.root, "second")
        second.data["workspace_roots"] = [str(self.first.workspace)]
        second.data["modules"] = self.first.data["modules"]
        second.write()
        with self.assertRaisesRegex(ProfileError, "PROFILE_SELECTION_AMBIGUOUS"):
            select_project_profile(self.first.workspace, env={}, agents_dir=self.root / "agents")

    def test_no_implicit_project_default(self):
        empty_agents = self.root / "empty-agents"
        empty_agents.mkdir()
        with self.assertRaisesRegex(ProfileError, "PROFILE_SELECTION_MISSING"):
            select_project_profile(self.first.workspace, env={}, agents_dir=empty_agents)

    def test_profiles_do_not_mix_context(self):
        second = ProfileFixture(self.root, "second")
        first_profile = load_project_profile(self.first.profile, self.first.workspace)
        second_profile = load_project_profile(second.profile, second.workspace)
        first_prompt = build_prompt("server architecture 수정", ["server"], "MODIFICATION", profile=first_profile, role="codex")
        second_prompt = build_prompt("server architecture 수정", ["server"], "MODIFICATION", profile=second_profile, role="codex")
        self.assertIn("PROFILE_RULE_first", first_prompt)
        self.assertNotIn("PROFILE_RULE_second", first_prompt)
        self.assertIn("PROFILE_RULE_second", second_prompt)
        self.assertNotIn("PROFILE_RULE_first", second_prompt)


class ProfileContextAndCheckTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.fx = ProfileFixture(Path(self.tmp.name))
        self.profile = load_project_profile(self.fx.profile, self.fx.workspace)

    def tearDown(self):
        self.tmp.cleanup()

    def test_area_word_boundaries_avoid_build_guide_false_positive(self):
        self.assertNotIn("frontend", detect_areas("build guide", profile=self.profile))
        self.assertIn("frontend", detect_areas("UI 수정", profile=self.profile))

    def test_codex_omits_common_agents_but_droid_includes_it(self):
        codex = build_prompt("server 수정", ["server"], "MODIFICATION", profile=self.profile, role="codex")
        droid = build_prompt("server 수정", ["server"], "MODIFICATION", profile=self.profile, role="droid")
        self.assertNotIn("[공통 프로젝트 규칙]", codex)
        self.assertIn("[공통 프로젝트 규칙]", droid)
        self.assertIn("PROFILE_RULE_sample", codex)
        self.assertIn("PROFILE_RULE_sample", droid)

    def test_check_profile_returns_before_manager_or_taskstate(self):
        out = io.StringIO()
        with mock.patch("run.Manager") as manager, mock.patch.object(run.TaskState, "save") as save, redirect_stdout(out):
            code = run.main([str(self.fx.workspace), "--profile", str(self.fx.profile), "--check-profile"])
        self.assertEqual(code, 0)
        self.assertIn("VALID", out.getvalue())
        manager.assert_not_called()
        save.assert_not_called()

    def test_invalid_check_profile_is_nonzero(self):
        out = io.StringIO()
        with redirect_stdout(out):
            code = run.main([str(self.fx.workspace), "--profile", str(self.fx.profile / "missing"), "--check-profile"])
        self.assertNotEqual(code, 0)
        self.assertIn("INVALID", out.getvalue())

    def test_missing_common_rule_fails_before_manager(self):
        out = io.StringIO()
        with tempfile.TemporaryDirectory() as empty_agents, mock.patch.dict(
            os.environ, {"AGENTS_DIR": empty_agents}, clear=False
        ), mock.patch("run.Manager") as manager, redirect_stdout(out):
            code = run.main([
                str(self.fx.workspace),
                "--profile", str(self.fx.profile),
                "--check-profile",
            ])
        self.assertNotEqual(code, 0)
        self.assertIn("INVALID", out.getvalue())
        manager.assert_not_called()


if __name__ == "__main__":
    unittest.main()
