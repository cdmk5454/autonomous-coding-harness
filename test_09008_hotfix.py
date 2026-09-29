import hashlib
import json
import subprocess
import harness_temp as tempfile
import unittest
from pathlib import Path

from attempt_checkpoint import AttemptCheckpointError, AttemptCheckpointStore


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class ScopedCheckpointReconstructionTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        self.repo = self.base / "project"
        self.repo.mkdir()
        subprocess.run(["git", "init"], cwd=self.repo, check=True, capture_output=True)
        subprocess.run(["git", "config", "user.email", "test@example.invalid"], cwd=self.repo, check=True)
        subprocess.run(["git", "config", "user.name", "Harness Test"], cwd=self.repo, check=True)
        for name, value in {"A.txt": b"A0\n", "B.txt": b"B0\n", "C.txt": b"C0\n", "D.txt": b"D0\n", "E.txt": b"E0\n"}.items():
            (self.repo / name).write_bytes(value)
        subprocess.run(["git", "add", "."], cwd=self.repo, check=True)
        subprocess.run(["git", "commit", "-m", "baseline"], cwd=self.repo, check=True, capture_output=True)
        (self.repo / "D.txt").write_bytes(b"D1\n")
        (self.repo / "E.txt").write_bytes(b"E2\n")
        subprocess.run(["git", "add", "D.txt", "E.txt"], cwd=self.repo, check=True)
        subprocess.run(["git", "commit", "-m", "predecessors"], cwd=self.repo, check=True, capture_output=True)
        self.tree = subprocess.run(["git", "rev-parse", "HEAD^{tree}"], cwd=self.repo, check=True, capture_output=True, text=True).stdout.strip()
        self.control = self.base / "control"
        self.store = AttemptCheckpointStore(self.repo, self.control)
        self.snapshot_path = self.control / "cumulative" / "job-snapshots" / "BASE" / "SNAP" / "manifest.json"
        self.snapshot_path.parent.mkdir(parents=True)
        snapshot = {
            "schema_version": 1, "kind": "PRE_JOB_SNAPSHOT", "snapshot_id": "SNAP",
            "baseline_id": "BASE", "job_id": "JOB", "target_resources": ["A.txt", "B.txt", "C.txt"],
            "repos": [{"module": "", "repo": str(self.repo), "worktree_tree": self.tree}],
            "files": [],
        }
        self.snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
        self.source_path = self._write_source({
            "A.txt": b"A1\n", "B.txt": b"B1\n", "C.txt": b"C1\n", "D.txt": b"D2\n"
        })

    def tearDown(self):
        self.temp.cleanup()

    def _write_source(self, after_by_path):
        files = []
        for path, after in after_by_path.items():
            before = (self.repo / path).read_bytes() if (self.repo / path).exists() else None
            files.append({
                "path": path,
                "before_state": "FILE" if before is not None else "MISSING",
                "after_state": "FILE" if after is not None else "DELETED",
                "before_sha256": sha(before) if before is not None else "MISSING",
                "after_sha256": sha(after) if after is not None else "DELETED",
                "before_blob": self.store._put_blob(before) if before is not None else "",
                "after_blob": self.store._put_blob(after) if after is not None else "",
                "before_mode": 0o644 if before is not None else 0,
                "after_mode": 0o644 if after is not None else 0,
            })
        checkpoint_id = "CHK-" + sha(json.dumps(sorted((key, None if value is None else sha(value)) for key, value in after_by_path.items())).encode())[:12].upper()
        manifest = {
            "schema_version": 2, "kind": "PRE_ROLLBACK_ATTEMPT_CHECKPOINT",
            "checkpoint_id": checkpoint_id, "generation": 5, "active_baseline_id": "BASE",
            "batch_id": "BATCH", "job_id": "JOB", "client_job_id": "CLIENT", "task_id": "TASK",
            "pre_job_snapshot_id": "SNAP", "pre_job_snapshot_manifest": str(self.snapshot_path),
            "task_owned_changed_files": list(after_by_path), "files": files, "renames": [],
            "inherited_batch_delta_files": {"D.txt": sha(b"D2\n")},
        }
        target = self.store.root / "BASE" / "JOB" / "TASK" / checkpoint_id
        target.mkdir(parents=True)
        payload = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
        (target / "manifest.json").write_bytes(payload)
        (target / "manifest.sha256").write_text(sha(payload), encoding="ascii")
        return target / "manifest.json"

    def test_three_file_projection_preserves_predecessor_admin_state(self):
        preview = self.store.preview_scoped_reconstruction(
            self.source_path, authorized_scope=["A.txt", "B.txt", "C.txt"], excluded_scope=["D.txt"]
        )
        self.assertEqual(["A.txt", "B.txt", "C.txt"], preview["resulting_changed_files"])
        self.assertEqual(sha(b"D1\n"), preview["resulting_file_sha256"]["D.txt"])
        self.assertNotEqual(sha(b"D0\n"), preview["resulting_file_sha256"]["D.txt"])

    def test_derived_manifest_is_subset_and_source_is_immutable(self):
        before = self.source_path.read_bytes()
        result = self.store.create_scoped_reconstruction(
            self.source_path, authorized_scope=["A.txt", "B.txt", "C.txt"], excluded_scope=["D.txt"],
            request_id="REQ-1",
        )
        derived = self.store.load(result["manifest_path"])
        self.assertEqual("SCOPED_DERIVED_CHECKPOINT", derived["kind"])
        self.assertEqual(["A.txt", "B.txt", "C.txt"], derived["task_owned_changed_files"])
        self.assertEqual({"A.txt", "B.txt", "C.txt"}, {item["path"] for item in derived["files"]})
        self.assertEqual(before, self.source_path.read_bytes())
        replay = self.store.create_scoped_reconstruction(
            self.source_path, authorized_scope=["A.txt", "B.txt", "C.txt"], excluded_scope=["D.txt"],
            request_id="REQ-1",
        )
        self.assertTrue(replay["replayed"])

    def test_source_manifest_hash_failure_is_closed(self):
        sidecar = self.source_path.parent / "manifest.sha256"
        sidecar.write_text("0" * 64, encoding="ascii")
        with self.assertRaisesRegex(AttemptCheckpointError, "CHECKPOINT_MANIFEST_HASH_MISMATCH"):
            self.store.preview_scoped_reconstruction(
                self.source_path, authorized_scope=["A.txt", "B.txt", "C.txt"], excluded_scope=["D.txt"]
            )

    def test_pre_job_hash_mismatch_is_closed(self):
        subprocess.run(["git", "checkout", "HEAD^", "--", "D.txt"], cwd=self.repo, check=True)
        subprocess.run(["git", "add", "D.txt"], cwd=self.repo, check=True)
        bad_tree = subprocess.run(["git", "write-tree"], cwd=self.repo, check=True, capture_output=True, text=True).stdout.strip()
        snapshot = json.loads(self.snapshot_path.read_text(encoding="utf-8"))
        snapshot["repos"][0]["worktree_tree"] = bad_tree
        self.snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
        with self.assertRaisesRegex(AttemptCheckpointError, "CHECKPOINT_PRE_JOB_FILE_MISMATCH"):
            self.store.preview_scoped_reconstruction(
                self.source_path, authorized_scope=["A.txt", "B.txt", "C.txt"], excluded_scope=["D.txt"]
            )

    def test_unattributed_extra_file_is_closed(self):
        with self.assertRaisesRegex(AttemptCheckpointError, "CHECKPOINT_SCOPE_ATTRIBUTION_AMBIGUOUS"):
            self.store.preview_scoped_reconstruction(
                self.source_path, authorized_scope=["A.txt", "B.txt"], excluded_scope=["D.txt"]
            )

    def test_multiple_predecessor_tree_is_part_of_base_identity(self):
        preview = self.store.preview_scoped_reconstruction(
            self.source_path, authorized_scope=["A.txt", "B.txt", "C.txt"], excluded_scope=["D.txt"]
        )
        self.assertTrue(preview["reconstruction_base_ref"])
        self.assertEqual(self.tree, json.loads(self.snapshot_path.read_text())["repos"][0]["worktree_tree"])
        self.assertEqual(b"E2\n", subprocess.run(["git", "show", f"{self.tree}:E.txt"], cwd=self.repo, check=True, capture_output=True).stdout)

    def test_add_delete_and_eol_post_images_are_preserved(self):
        (self.repo / "NEW.txt").unlink(missing_ok=True)
        source = self._write_source({"A.txt": b"A1\r\n", "B.txt": None, "NEW.txt": b"new\n", "D.txt": b"D2\n"})
        result = self.store.create_scoped_reconstruction(
            source, authorized_scope=["A.txt", "B.txt", "NEW.txt"], excluded_scope=["D.txt"], request_id="REQ-2"
        )
        derived = self.store.load(result["manifest_path"])
        states = {item["path"]: item["after_state"] for item in derived["files"]}
        self.assertEqual({"A.txt": "FILE", "B.txt": "DELETED", "NEW.txt": "FILE"}, states)
        self.assertEqual(sha(b"A1\r\n"), next(item for item in derived["files"] if item["path"] == "A.txt")["after_sha256"])


if __name__ == "__main__":
    unittest.main()
