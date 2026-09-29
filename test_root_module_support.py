import importlib.util
import json
import unittest
from pathlib import Path
from unittest import mock

import harness_temp as tempfile
import jsonschema
from project_profile import ProfileError, load_project_profile
from test_project_profile import ProfileFixture


class RootModuleTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.fx = ProfileFixture(Path(self.tmp.name))
        self.root_module = dict(name="harness", path=".", kind="infrastructure", protected=False)
        self.fx.data["modules"] = [self.root_module]

    def load(self):
        self.fx.write()
        return load_project_profile(self.fx.profile, self.fx.workspace)

    def test_empty_modules_rejected_by_all_validators(self):
        self.fx.data["modules"] = []
        with self.assertRaises(ProfileError):
            self.load()
        schema = json.loads(Path(r"D:\agents\profiles\project-profile.schema.json").read_text())
        self.assertTrue(list(jsonschema.Draft202012Validator(schema).iter_errors(self.fx.data)))
        spec = importlib.util.spec_from_file_location("standalone_validator", r"D:\agents\validate_profiles.py")
        validator = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(validator)
        self.assertIn("modules must be a non-empty array", validator.validate_manifest(self.fx.profile, self.fx.data))

    def test_root_and_dot_slash_resolve_exactly_to_workspace(self):
        for path in (".", "./"):
            with self.subTest(path=path):
                self.root_module["path"] = path
                profile = self.load()
                self.assertEqual(profile.modules[0].path, ".")
                self.assertEqual((profile.selected_workspace / profile.modules[0].path).resolve(), self.fx.workspace.resolve())

    def test_root_matches_relative_and_absolute_root_and_nested_files(self):
        profile = self.load()
        for path in ("project_profile.py", "some_dir/file.py", "SOME_DIR/FILE.py"):
            self.assertEqual(profile.module_for_path(path).name, "harness")
            self.assertEqual(profile.module_for_path(str(self.fx.workspace / path)).name, "harness")

    def test_absolute_and_parent_module_paths_rejected(self):
        for path in (str(self.fx.workspace), "..", "../x", "x/../../y"):
            with self.subTest(path=path):
                self.root_module["path"] = path
                with self.assertRaises(ProfileError):
                    self.load()

    def test_outside_files_and_empty_paths_do_not_match(self):
        profile = self.load()
        for path in ("", ".", "..", "../file.py", "x/../../file.py", str(Path(self.tmp.name) / "outside.py")):
            with self.subTest(path=path):
                self.assertIsNone(profile.module_for_path(path))

    def test_selected_workspace_not_other_authorized_root(self):
        other = Path(self.tmp.name) / "other"
        other.mkdir()
        self.fx.data["workspace_roots"].append(str(other))
        self.assertIsNone(self.load().module_for_path(str(other / "file.py")))

    def test_specific_module_wins_regardless_of_order(self):
        child = dict(name="server", path="server", kind="backend", protected=False)
        for modules in ([self.root_module, child], [child, self.root_module]):
            self.fx.data["modules"] = modules
            profile = self.load()
            self.assertEqual(profile.module_for_path("server/file.py").name, "server")
            self.assertEqual(profile.module_for_path("other/file.py").name, "harness")

    def test_ordinary_subdirectory_still_valid(self):
        self.fx.data["modules"] = [dict(name="server", path="server", kind="backend", protected=False)]
        profile = self.load()
        self.assertEqual(profile.module_for_path("SERVER/file.py").name, "server")
        self.assertIsNone(profile.module_for_path("root.py"))

    def test_duplicate_names_and_normalized_paths_rejected(self):
        for duplicate in (dict(name="HARNESS", path="server", kind="backend", protected=False),
                          dict(name="second", path="./", kind="backend", protected=False)):
            self.fx.data["modules"] = [self.root_module, duplicate]
            with self.assertRaises(ProfileError):
                self.load()

    def test_missing_required_module_fields_rejected(self):
        for field in ("name", "path", "kind", "protected"):
            module = self.root_module.copy()
            del module[field]
            self.fx.data["modules"] = [module]
            with self.assertRaises(ProfileError):
                self.load()

    def test_context_root_path_still_rejected(self):
        self.fx.data["context"]["analysis"] = "."
        with self.assertRaises(ProfileError):
            self.load()

    def test_cumulative_root_paths_preserve_full_filename(self):
        from cumulative_policy import CumulativePolicyManager
        manager = object.__new__(CumulativePolicyManager)
        manager.profile = self.load()
        manager.workspace = self.fx.workspace.resolve()
        self.assertEqual(manager._repo_for_path("file.py"), (manager.workspace, "", "file.py"))
        responses = [(0, "head", ""), (0, "file.py\0", ""), (0, "new.py\0", ""), (0, "?? new.py\0", "")]
        with mock.patch.object(manager, "_run_git", side_effect=responses):
            inventory = manager._repo_inventory(manager.workspace, ".")
        self.assertEqual(inventory["tracked_files"], ["file.py"])
        self.assertEqual(inventory["untracked_files"], ["new.py"])
        self.assertEqual(inventory["dirty_files"][0]["path"], "new.py")

    def test_git_collector_uses_unprefixed_root(self):
        from git_collector import GitCollector
        collector = GitCollector(str(self.fx.workspace), profile=self.load())
        self.assertEqual(collector._git_dirs(None), [(self.fx.workspace.resolve(), None)])


if __name__ == "__main__":
    unittest.main()
