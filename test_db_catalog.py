import json
import shutil
import harness_temp as tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import project_profile
import worker
from db_catalog import DbCatalogError, load_db_catalog
from project_profile import load_project_profile
from worker import load_db_catalog_context


ROOT = Path(__file__).resolve().parent
PROFILE = ROOT.parent / "agents" / "profiles" / "e4u"
CATALOG = PROFILE / "references" / "db" / "catalog.json"


class DbCatalogRuntimeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.catalog = load_db_catalog(CATALOG, PROFILE)

    def test_real_catalog_loads_as_immutable_snapshot(self):
        self.assertEqual(len(self.catalog.tables), 301)
        self.assertEqual(self.catalog.schema_dates["CAP"], "2026-08-07")
        self.assertEqual(self.catalog.schema_dates["ITN"], "2026-08-21")
        self.assertEqual(self.catalog.schema_dates["SUC"], "2026-08-24")
        with self.assertRaises(TypeError):
            self.catalog.tables["SUC.SSUC100"] = None

    def test_text_and_screen_lookup_are_selective(self):
        text_ids = self.catalog.table_ids_for_text("SSUC100/102와 SUC.SSUC120 확인")
        self.assertIn("SUC.SSUC100", text_ids)
        self.assertIn("SUC.SSUC102", text_ids)
        self.assertIn("SUC.SSUC120", text_ids)
        screen_ids = self.catalog.table_ids_for_screens(["NS0060"])
        self.assertIn("SUC.SSUC100", screen_ids)
        self.assertIn("SUC.SSUC102", screen_ids)
        self.assertNotIn("CAP.CAPA010", screen_ids)

    def test_rendered_context_preserves_snapshot_and_evidence_boundary(self):
        rendered = self.catalog.render_context(
            ["SUC.SSUC100", "SUC.SSUC102"],
            focus_text="OPER_TYPE CLASS_NO",
        )
        self.assertIn("SUC.SSUC100", rendered)
        self.assertIn("as-of 2026-08-24", rendered)
        self.assertIn("current DB metadata > current MyBatis SQL", rendered)
        self.assertIn("Common-code rows", rendered)
        self.assertIn("SUC_LIMITED_DB_FK", rendered)

    def test_worker_context_uses_selected_screen_tables_only(self):
        profile = SimpleNamespace(db_catalog=self.catalog)
        rendered = load_db_catalog_context(
            "NS0060 비교과 등록 화면 확인",
            "",
            ["NS0060"],
            profile=profile,
        )
        self.assertIn("SUC.SSUC100", rendered)
        self.assertNotIn("CAP.CAPA010", rendered)

    def test_large_screen_context_respects_hard_cap(self):
        table_ids = self.catalog.table_ids_for_screens(["NS0060"])
        rendered = self.catalog.render_context(table_ids, focus_text="NS0060", max_chars=4000)
        self.assertLessEqual(len(rendered), 4000)
        self.assertIn("Context cap reached", rendered)

    def test_changed_source_workbook_fails_closed(self):
        with tempfile.TemporaryDirectory() as temporary:
            copied_profile = Path(temporary) / "e4u"
            copied_db = copied_profile / "references" / "db"
            shutil.copytree(CATALOG.parent, copied_db)
            catalog_path = copied_db / "catalog.json"
            manifest = json.loads(catalog_path.read_text(encoding="utf-8"))
            source = copied_db / manifest["sources"][0]["file"]
            source.write_bytes(source.read_bytes() + b"changed")
            with self.assertRaises(DbCatalogError) as raised:
                load_db_catalog(catalog_path, copied_profile)
        self.assertEqual(raised.exception.code, "DB_CATALOG_SOURCE_HASH_MISMATCH")

    def test_project_profile_loads_and_freezes_configured_catalog(self):
        with tempfile.TemporaryDirectory() as temporary:
            temporary_root = Path(temporary)
            copied_profile = temporary_root / "profiles" / "e4u"
            shutil.copytree(PROFILE, copied_profile)
            workspace = temporary_root / "workspace"
            workspace.mkdir()
            manifest_path = copied_profile / "project.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["workspace_roots"] = [str(workspace)]
            for module in manifest["modules"]:
                (workspace / module["path"]).mkdir(parents=True)
            manifest_path.write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
            with mock.patch.object(project_profile, "_validate_manifest_schema"):
                profile = load_project_profile(copied_profile, workspace)
            with mock.patch.object(worker, "AGENTS_DIR", ROOT.parent / "agents"):
                prompt = worker.build_prompt(
                    "NS0060 비교과 등록 화면의 SSUC102 저장 확인",
                    ["suc"],
                    profile=profile,
                    role="codex",
                )
        self.assertIsNotNone(profile.db_catalog)
        self.assertEqual(len(profile.db_catalog.tables), 301)
        self.assertIn("[선택된 DB 메타데이터]", prompt)
        self.assertIn("SUC.SSUC102", prompt)
        self.assertNotIn("CAP.CAPA010", prompt)

    def test_index_unknown_column_fails_closed(self):
        with tempfile.TemporaryDirectory() as temporary:
            copied_profile = Path(temporary) / "e4u"
            copied_db = copied_profile / "references" / "db"
            shutil.copytree(CATALOG.parent, copied_db)
            catalog_path = copied_db / "catalog.json"
            manifest = json.loads(catalog_path.read_text(encoding="utf-8"))
            itn_entry = next(entry for entry in manifest["schemas"] if entry["schema"] == "ITN")
            schema_path = copied_db / itn_entry["path"]
            schema = json.loads(schema_path.read_text(encoding="utf-8"))
            schema["indexes"][0]["columns"] = ["MISSING_COLUMN"]
            schema_path.write_text(json.dumps(schema, ensure_ascii=False), encoding="utf-8")
            with self.assertRaises(DbCatalogError) as raised:
                load_db_catalog(catalog_path, copied_profile)
        self.assertEqual(raised.exception.code, "DB_CATALOG_ITEM_COLUMN_MISSING")


if __name__ == "__main__":
    unittest.main()
