"""Public-export reproducibility checks using only synthetic inputs."""

from __future__ import annotations

import json
import shutil
import unittest
from pathlib import Path

import harness_temp
from project_profile import load_project_profile


class SyntheticProfileTests(unittest.TestCase):
    def test_sample_profile_loads_against_a_synthetic_workspace(self):
        source = Path(__file__).resolve().parents[1] / "examples" / "sample-profile"
        with harness_temp.TemporaryDirectory() as directory:
            root = Path(directory)
            profile_dir = root / "sample-profile"
            workspace = root / "sample-workspace"
            shutil.copytree(source, profile_dir)
            (workspace / "sample-service").mkdir(parents=True)
            (workspace / "sample-web").mkdir()

            manifest_path = profile_dir / "project.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["workspace_roots"] = [str(workspace)]
            manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

            profile = load_project_profile(
                profile_dir,
                workspace,
                schema_file=profile_dir / "project-profile.schema.json",
            )
            self.assertEqual("sample-profile", profile.id)
            self.assertEqual(
                {"sample-service", "sample-web"},
                {module.name for module in profile.modules},
            )


if __name__ == "__main__":
    unittest.main()
