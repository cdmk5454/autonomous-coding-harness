from __future__ import annotations

import hashlib
import json
import harness_temp as tempfile
import unittest
from pathlib import Path

from batch_manifest import (
    BatchManifestError,
    normalize_ab_manifest,
    prepare_ab_only,
    validate_normalized_ab_manifest,
)
from job_contract import DROID_GLM_THINKING_MODEL


CANONICAL_DROID = DROID_GLM_THINKING_MODEL
LEGACY_DROID = "custom:GLM-5.3-[Z.AI-Coding]-0"
LEGACY_CODEX = "custom:codex-5.3-[chatgpt.com]-0"
PROFILE_ID = "e4u"
PROFILE_FINGERPRINT = "a" * 64


def _job(client_id: str, model: str = CANONICAL_DROID, worker: str = "droid") -> dict:
    return {
        "client_job_id": client_id,
        "requirement": "approved fixture requirement",
        "worker": worker,
        "model": model,
        "reasoning_effort": "medium",
        "target_modules": ["suc", "sucui"],
        "max_outer_attempts": 2,
    }


class ManifestNormalization0855Tests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.source = self.root / "enqueue_batch_A.json"
        self.output = self.root / "enqueue_batch_A.normalized.json"
        self.provenance = self.root / "enqueue_batch_A.normalized.provenance.json"
        self._write_source([
            _job("WBS-A01-FIXTURE"),
            _job("WBS-A02-FIXTURE", LEGACY_CODEX),
        ])

    def tearDown(self):
        self.temp.cleanup()

    def _write_source(self, jobs: list[dict]) -> None:
        self.source.write_text(
            json.dumps({"idempotency_key": "fixture-a", "jobs": jobs}, indent=2),
            encoding="utf-8",
        )

    def _normalize(self) -> dict:
        return normalize_ab_manifest(
            self.source,
            self.output,
            phase="A",
            profile_id=PROFILE_ID,
            profile_fingerprint=PROFILE_FINGERPRINT,
            normalized_at="2026-08-30T00:00:00+09:00",
        )

    def _load_provenance(self) -> dict:
        return json.loads(self.provenance.read_text(encoding="utf-8"))

    def _write_provenance(self, value: dict) -> None:
        self.provenance.write_text(
            json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    def _rehash_normalized(self, provenance: dict) -> None:
        provenance["normalized_manifest_sha256"] = hashlib.sha256(
            self.output.read_bytes()
        ).hexdigest()

    def _validate(self) -> dict:
        return validate_normalized_ab_manifest(
            self.output,
            self.provenance,
            phase="A",
            expected_profile_id=PROFILE_ID,
            expected_profile_fingerprint=PROFILE_FINGERPRINT,
        )

    def test_01_invalid_model_is_rejected_by_job_contract(self):
        self._write_source([_job("WBS-A01-FIXTURE", "invalid-model")])
        with self.assertRaisesRegex(BatchManifestError, "AB_MANIFEST_JOB_CONTRACT_INVALID"):
            prepare_ab_only(self.source, phase="A")

    def test_02_canonical_worker_model_passes(self):
        result = self._normalize()
        self.assertTrue(result["valid"])
        jobs = json.loads(self.output.read_text(encoding="utf-8"))["jobs"]
        self.assertEqual(("droid", CANONICAL_DROID), (jobs[0]["worker"], jobs[0]["model"]))

    def test_02a_legacy_droid_alias_normalizes_to_canonical_thinking(self):
        self._write_source([
            _job("WBS-A01-FIXTURE", LEGACY_DROID),
            _job("WBS-A02-FIXTURE", LEGACY_DROID),
        ])
        self._normalize()
        jobs = json.loads(self.output.read_text(encoding="utf-8"))["jobs"]
        self.assertTrue(all(job["model"] == CANONICAL_DROID for job in jobs))
        for mapping in self._load_provenance()["model_alias_mapping"]:
            self.assertEqual("LEGACY_ALIAS", mapping["kind"])
            self.assertEqual(LEGACY_DROID, mapping["original_model"])
            self.assertEqual(CANONICAL_DROID, mapping["canonical_model"])
            self.assertEqual("droid", mapping["canonical_worker"])

    def test_03_canonical_reviewer_routing_is_recorded(self):
        self._normalize()
        route = self._load_provenance()["reviewer_routing"]
        self.assertEqual(("codex", "gpt-5.6-sol"), (route["worker"], route["model"]))
        self.assertEqual("fresh_read_only_invocation", route["isolation"])

    def test_04_legacy_alias_normalizes_deterministically(self):
        self._normalize()
        jobs = json.loads(self.output.read_text(encoding="utf-8"))["jobs"]
        self.assertEqual(("codex", "gpt-5.6-sol"), (jobs[1]["worker"], jobs[1]["model"]))
        mapping = self._load_provenance()["model_alias_mapping"][1]
        self.assertEqual("LEGACY_ALIAS", mapping["kind"])
        self.assertEqual(LEGACY_CODEX, mapping["original_model"])

    def test_05_unknown_alias_hard_fails(self):
        self._write_source([_job("WBS-A01-FIXTURE", "custom:unknown")])
        with self.assertRaisesRegex(BatchManifestError, "AB_MANIFEST_UNKNOWN_MODEL"):
            self._normalize()

    def test_06_missing_model_never_defaults(self):
        job = _job("WBS-A01-FIXTURE")
        job.pop("model")
        self._write_source([job])
        with self.assertRaisesRegex(BatchManifestError, "AB_MANIFEST_MODEL_MISSING"):
            self._normalize()

    def test_07_source_manifest_hash_change_is_detected(self):
        self._normalize()
        self.source.write_text("{}", encoding="utf-8")
        with self.assertRaisesRegex(BatchManifestError, "BATCH_MANIFEST_CONTENT_CHANGED"):
            self._validate()

    def test_08_normalized_manifest_change_is_detected(self):
        self._normalize()
        self.output.write_bytes(self.output.read_bytes() + b" ")
        with self.assertRaisesRegex(BatchManifestError, "AB_NORMALIZED_MANIFEST_CHANGED"):
            self._validate()

    def test_09_per_job_request_hash_change_is_detected(self):
        self._normalize()
        provenance = self._load_provenance()
        provenance["per_job_request_sha256"][0] = "0" * 64
        self._write_provenance(provenance)
        with self.assertRaisesRegex(BatchManifestError, "BATCH_JOB_REQUEST_CHANGED"):
            self._validate()

    def test_10_sequence_change_is_detected(self):
        self._normalize()
        provenance = self._load_provenance()
        provenance["ordered_sequence"].reverse()
        self._write_provenance(provenance)
        with self.assertRaisesRegex(BatchManifestError, "BATCH_SEQUENCE_CHANGED"):
            self._validate()

    def test_11_u03_u04_are_forbidden(self):
        self._write_source([_job("WBS-A-U03-FIXTURE")])
        with self.assertRaisesRegex(BatchManifestError, "U03_U04_FORBIDDEN_IN_AB_BATCH"):
            self._normalize()

    def test_12_duplicate_client_job_id_fails(self):
        job = _job("WBS-A01-FIXTURE")
        self._write_source([job, dict(job)])
        with self.assertRaisesRegex(BatchManifestError, "DUPLICATE_CLIENT_JOB_ID"):
            self._normalize()

    def test_13_missing_model_contract_is_explicit(self):
        job = _job("WBS-A01-FIXTURE")
        job["model"] = ""
        self._write_source([job])
        with self.assertRaisesRegex(BatchManifestError, "AB_MANIFEST_MODEL_MISSING"):
            self._normalize()

    def test_14_profile_fingerprint_mismatch_fails(self):
        self._normalize()
        with self.assertRaisesRegex(BatchManifestError, "PROFILE_FINGERPRINT_MISMATCH"):
            validate_normalized_ab_manifest(
                self.output, self.provenance, phase="A",
                expected_profile_id=PROFILE_ID,
                expected_profile_fingerprint="b" * 64,
            )

    def test_15_automatic_commit_true_fails(self):
        self._normalize()
        provenance = self._load_provenance()
        provenance["automatic_commit"] = True
        self._write_provenance(provenance)
        with self.assertRaisesRegex(BatchManifestError, "AUTOMATIC_COMMIT_FORBIDDEN"):
            self._validate()

    def test_16_automatic_push_true_fails(self):
        self._normalize()
        provenance = self._load_provenance()
        provenance["automatic_push"] = True
        self._write_provenance(provenance)
        with self.assertRaisesRegex(BatchManifestError, "AUTOMATIC_PUSH_FORBIDDEN"):
            self._validate()

    def test_17_normalized_manifest_round_trip_is_identical(self):
        self._normalize()
        first = self.output.read_bytes()
        second = self.root / "second.normalized.json"
        normalize_ab_manifest(
            self.source, second, phase="A", profile_id=PROFILE_ID,
            profile_fingerprint=PROFILE_FINGERPRINT,
            normalized_at="2026-08-30T00:00:00+09:00",
        )
        self.assertEqual(first, second.read_bytes())

    def test_18_provenance_sidecar_hash_matches_manifest(self):
        result = self._normalize()
        provenance = self._load_provenance()
        self.assertEqual(
            hashlib.sha256(self.output.read_bytes()).hexdigest(),
            provenance["normalized_manifest_sha256"],
        )
        self.assertEqual("AB_MANIFEST_JOB_CONTRACT_VALID", result["status"])


if __name__ == "__main__":
    unittest.main()
