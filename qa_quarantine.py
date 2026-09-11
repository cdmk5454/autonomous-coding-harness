"""QA/speculative candidate bundles built on the existing pre-Job snapshot.

Candidates are evidence, never strict success. Canonical source mutation is
limited to hash-guarded apply and the existing scoped rollback primitive.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, Mapping, Sequence

from control_paths import atomic_state_write, ensure_safe_state_directory, ensure_safe_state_root
from git_collector import GitCollector
from runtime_safety import scrub_secrets


QA_HUMAN_ACCEPTANCE = "HUMAN_ACCEPTANCE"
QA_VISUAL_ACCEPTANCE = "VISUAL_ACCEPTANCE"
QA_OPERATOR_ACCEPTANCE = "OPERATOR_ACCEPTANCE"
QA_CONTRACT_DECISION = "CONTRACT_DECISION"
QA_BUSINESS_DECISION = "BUSINESS_DECISION"
QA_API_DECISION = "API_DECISION"
QA_DB_DECISION = "DB_DECISION"
QA_BUSINESS_CONTRACT = "BUSINESS_CONTRACT"
QA_API_CONTRACT = "API_CONTRACT"
QA_DB_CONTRACT = "DB_CONTRACT"
QA_SAFETY_INTEGRITY = "SAFETY_INTEGRITY"
QA_TOOL_PERMISSION = "TOOL_PERMISSION"
QA_CLARIFICATION = "CLARIFICATION"
QA_CANDIDATE_DECISION = "CANDIDATE_DECISION"
QA_POLICY_DECISION = "POLICY_DECISION"
QA_DEFERRED_DECISION = "DEFERRED_DECISION"

HOLD_JOB = "JOB"
HOLD_DEPENDENCY_CHAIN = "DEPENDENCY_CHAIN"
HOLD_BATCH = "BATCH"

QA_TYPES = frozenset({
    QA_HUMAN_ACCEPTANCE, QA_VISUAL_ACCEPTANCE, QA_OPERATOR_ACCEPTANCE,
    QA_CONTRACT_DECISION, QA_BUSINESS_DECISION, QA_API_DECISION,
    QA_DB_DECISION, QA_BUSINESS_CONTRACT, QA_API_CONTRACT, QA_DB_CONTRACT,
    QA_SAFETY_INTEGRITY,
    QA_TOOL_PERMISSION, QA_CLARIFICATION, QA_CANDIDATE_DECISION,
    QA_POLICY_DECISION, QA_DEFERRED_DECISION,
})
HOLD_SCOPES = frozenset({HOLD_JOB, HOLD_DEPENDENCY_CHAIN, HOLD_BATCH})
SOURCE_MUTATING_SPECULATION_QA_TYPES = frozenset({QA_HUMAN_ACCEPTANCE})
ANALYSIS_ONLY_SPECULATION_QA_TYPES = frozenset({
    QA_CONTRACT_DECISION, QA_BUSINESS_DECISION, QA_API_DECISION, QA_DB_DECISION,
    QA_BUSINESS_CONTRACT, QA_API_CONTRACT, QA_DB_CONTRACT,
    QA_POLICY_DECISION, QA_DEFERRED_DECISION,
})


class CandidateError(RuntimeError):
    def __init__(self, code: str, detail: str = ""):
        self.code = code
        self.detail = scrub_secrets(detail)[:500]
        super().__init__(code + (f": {self.detail}" if self.detail else ""))


def validate_qa_policy(qa_type: str, hold_scope: str, machine_verified: bool) -> dict[str, Any]:
    qa = str(qa_type or "").strip().upper()
    hold = str(hold_scope or "").strip().upper()
    if not qa and not hold:
        return {"qa_type": "", "hold_scope": "", "machine_verified": False}
    if qa not in QA_TYPES:
        raise CandidateError("QA_TYPE_INVALID", qa)
    if hold not in HOLD_SCOPES:
        raise CandidateError("QA_HOLD_SCOPE_INVALID", hold)
    if not isinstance(machine_verified, bool):
        raise CandidateError("QA_MACHINE_VERIFIED_INVALID")
    if qa == QA_SAFETY_INTEGRITY and hold != HOLD_BATCH:
        raise CandidateError("QA_SAFETY_REQUIRES_BATCH_HOLD")
    return {"qa_type": qa, "hold_scope": hold, "machine_verified": machine_verified}


def speculation_mode(parent_qa: Mapping[str, Any]) -> str:
    qa = str(parent_qa.get("qa_type", ""))
    verified = parent_qa.get("machine_verified") is True
    if qa == QA_SAFETY_INTEGRITY:
        return "FORBIDDEN"
    if qa in SOURCE_MUTATING_SPECULATION_QA_TYPES and verified:
        return "SOURCE_MUTATION_ALLOWED"
    if qa in ANALYSIS_ONLY_SPECULATION_QA_TYPES:
        return "ANALYSIS_ONLY"
    return "BLOCKED"


def paths_overlap(first: Sequence[str], second: Sequence[str]) -> bool:
    left = [str(item).replace("\\", "/").strip("/") for item in first if item]
    right = [str(item).replace("\\", "/").strip("/") for item in second if item]
    if not left or not right:
        return True  # unknown scope is fail-closed
    return any(a == b or a.startswith(b + "/") or b.startswith(a + "/") for a in left for b in right)


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _now() -> str:
    return datetime.now().astimezone().isoformat()


class QACandidateStore:
    def __init__(self, root: str | Path, workspace: str | Path, profile: Any):
        self.root = ensure_safe_state_root(root, field="candidate_root", create=True)
        self.workspace = Path(workspace).resolve()
        self.profile = profile

    def _candidate_dir(self, candidate_id: str, *, create: bool = False) -> Path:
        if not re.fullmatch(r"CAND-[A-F0-9]{24}", candidate_id):
            raise CandidateError("CANDIDATE_ID_INVALID")
        return ensure_safe_state_directory(
            self.root, self.root / candidate_id, field="candidate_dir", create=create
        )

    def _workspace_path(self, relative: str) -> Path:
        normalized = str(relative).replace("\\", "/").strip("/")
        if not normalized or ".." in PurePosixPath(normalized).parts:
            raise CandidateError("CANDIDATE_PATH_INVALID", normalized)
        path = (self.workspace / Path(*PurePosixPath(normalized).parts)).resolve()
        try:
            path.relative_to(self.workspace)
        except ValueError as exc:
            raise CandidateError("CANDIDATE_PATH_ESCAPE", normalized) from exc
        return path

    def _write_manifest(self, destination: Path, manifest: Mapping[str, Any]) -> str:
        payload = json.dumps(
            dict(manifest), ensure_ascii=False, sort_keys=True, indent=2
        ).encode("utf-8")
        manifest_path = destination / "manifest.json"
        atomic_state_write(
            manifest_path, payload, root=self.root, field="candidate_manifest"
        )
        atomic_state_write(
            destination / "manifest.sha256",
            (_sha(payload) + "\n").encode("ascii"),
            root=self.root,
            field="candidate_manifest_hash",
        )
        return str(manifest_path)

    @staticmethod
    def _before_records(snapshot_manifest: str | Path) -> dict[str, dict[str, Any]]:
        try:
            data = json.loads(Path(snapshot_manifest).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CandidateError("CANDIDATE_SNAPSHOT_UNREADABLE") from exc
        if data.get("kind") != "PRE_JOB_SNAPSHOT":
            raise CandidateError("CANDIDATE_SNAPSHOT_INVALID")
        return {
            str(item.get("path", "")): dict(item)
            for item in data.get("files") or []
            if isinstance(item, Mapping) and item.get("path")
        }

    def create(
        self,
        task: Any,
        *,
        kind: str = "QA_QUARANTINE",
        parent_qa_job_id: str = "",
        parent_candidate_id: str = "",
        parent_candidate_revision: int = 0,
    ) -> dict[str, Any]:
        changed = list(dict.fromkeys(
            getattr(task, "task_owned_changed_files", [])
            or getattr(task, "changed_files", []) or []
        ))
        if not changed:
            raise CandidateError("CANDIDATE_DELTA_EMPTY")
        snapshot_manifest = str(getattr(task, "pre_job_snapshot_manifest", ""))
        before = self._before_records(snapshot_manifest)
        missing_before = [path for path in changed if path not in before]
        if missing_before:
            raise CandidateError("CANDIDATE_BEFORE_EVIDENCE_MISSING", missing_before[0])
        diff = str(getattr(task, "git_diff", ""))
        diff_sha = _sha(diff.encode("utf-8"))
        seed = json.dumps({
            "job": getattr(task, "job_id", ""),
            "task": getattr(task, "task_id", ""),
            "snapshot": getattr(task, "pre_job_snapshot_id", ""),
            "diff": diff_sha,
            "kind": kind,
            "parent": parent_candidate_id,
        }, sort_keys=True, separators=(",", ":"))
        candidate_id = "CAND-" + _sha(seed.encode("utf-8"))[:24].upper()
        destination = self._candidate_dir(candidate_id, create=True)
        files_root = ensure_safe_state_directory(
            self.root, destination / "files", field="candidate_files", create=True
        )
        candidate_records: list[dict[str, Any]] = []
        for relative in changed:
            current = self._workspace_path(relative)
            record: dict[str, Any] = {
                "path": relative,
                "before_sha256": before[relative].get("sha256", ""),
                "candidate_sha256": "DELETED",
                "state": "DELETED",
                "snapshot": "",
            }
            if current.is_file() and not current.is_symlink():
                payload = current.read_bytes()
                digest = _sha(payload)
                target = files_root / digest[:2] / digest
                target.parent.mkdir(parents=True, exist_ok=True)
                if not target.exists():
                    fd, name = tempfile.mkstemp(prefix=".candidate-", dir=str(target.parent))
                    try:
                        with os.fdopen(fd, "wb") as handle:
                            handle.write(payload)
                            handle.flush()
                            os.fsync(handle.fileno())
                        os.replace(name, target)
                    finally:
                        try:
                            os.unlink(name)
                        except FileNotFoundError:
                            pass
                record.update({
                    "candidate_sha256": digest,
                    "state": "PRESENT",
                    "snapshot": target.relative_to(destination).as_posix(),
                    "size": len(payload),
                })
            elif current.exists():
                raise CandidateError("CANDIDATE_FILE_TYPE_UNSUPPORTED", relative)
            candidate_records.append(record)
        patch_path = destination / "candidate.patch"
        atomic_state_write(
            patch_path, diff.encode("utf-8"), root=self.root, field="candidate_patch"
        )
        qa = validate_qa_policy(
            str(getattr(task, "qa_type", "")),
            str(getattr(task, "hold_scope", "")),
            bool(getattr(task, "machine_verified", False)),
        )
        qa_request = dict(getattr(task, "qa_request", {}) or {})
        manifest = {
            "schema_version": 2,
            "candidate_id": candidate_id,
            "kind": kind,
            "source_job_id": str(getattr(task, "job_id", "")),
            "source_task_id": str(getattr(task, "task_id", "")),
            "candidate_revision": 1,
            "base": {
                "baseline_id": str(getattr(task, "active_baseline_id", "")),
                "snapshot_id": str(getattr(task, "pre_job_snapshot_id", "")),
                "snapshot_manifest": snapshot_manifest,
            },
            "delta": {
                "changed_files": changed,
                "patch_path": patch_path.relative_to(destination).as_posix(),
                "diff_sha256": diff_sha,
                "files": candidate_records,
            },
            "evidence": {
                "build": dict(getattr(task, "build", {}) or {}),
                "test": {
                    "required": bool(getattr(task, "test_required", False)),
                    "status": "NOT_REQUIRED" if not bool(getattr(task, "test_required", False)) else str(getattr(task, "test_status", "")),
                },
                "review": {
                    "status": str(getattr(task, "review_status", "")),
                    "coverage": dict(getattr(task, "review_coverage", {}) or {}),
                },
                "verification": str(getattr(task, "verification_status", "")),
                "failure": {
                    "code": str(getattr(task, "failure_code", "")),
                    "origin": str(getattr(task, "failure_origin", "")),
                    "stage": str(getattr(task, "failure_stage", "")),
                    "reason": str(getattr(task, "failure_reason", "")),
                },
            },
            "qa": {
                **qa,
                "user_action_required": True,
                "reason": str(qa_request.get("reason", "")),
                "evidence": scrub_secrets(str(qa_request.get("evidence", "")))[:2000],
                "decision_independent_work_complete": (
                    qa_request.get("decision_independent_work_complete") is True
                ),
            },
            "manifest_integrity": {
                "algorithm": "sha256",
                "sidecar": "manifest.sha256",
            },
            "parent_qa_job_id": parent_qa_job_id,
            "parent_candidate_id": parent_candidate_id,
            "parent_candidate_revision": int(parent_candidate_revision),
            "state": "QUARANTINED",
            "strict_success": False,
            "created_at": _now(),
        }
        manifest_path = self._write_manifest(destination, manifest)
        return {**manifest, "manifest_path": manifest_path}

    def load(self, candidate_id: str) -> dict[str, Any]:
        path = self._candidate_dir(candidate_id) / "manifest.json"
        try:
            payload = path.read_bytes()
            data = json.loads(payload.decode("utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise CandidateError("CANDIDATE_MANIFEST_UNREADABLE") from exc
        if data.get("candidate_id") != candidate_id:
            raise CandidateError("CANDIDATE_MANIFEST_INVALID")
        root = path.parent
        if int(data.get("schema_version", 1)) >= 2:
            try:
                expected = (root / "manifest.sha256").read_text(encoding="ascii").strip()
            except OSError as exc:
                raise CandidateError("CANDIDATE_MANIFEST_HASH_MISSING") from exc
            if not re.fullmatch(r"[a-f0-9]{64}", expected) or _sha(payload) != expected:
                raise CandidateError("CANDIDATE_MANIFEST_HASH_MISMATCH")
        delta = dict(data.get("delta") or {})
        records = list(delta.get("files") or [])
        changed = list(delta.get("changed_files") or [])
        record_paths = [str(record.get("path", "")) for record in records]
        if not changed or len(changed) != len(set(changed)) or changed != record_paths:
            raise CandidateError("CANDIDATE_COVERAGE_INVALID")
        patch_relative = str(delta.get("patch_path", ""))
        patch = (root / Path(*PurePosixPath(patch_relative).parts)).resolve()
        try:
            patch.relative_to(root.resolve())
        except ValueError as exc:
            raise CandidateError("CANDIDATE_PATH_ESCAPE") from exc
        if not patch.is_file() or _sha(patch.read_bytes()) != delta.get("diff_sha256"):
            raise CandidateError("CANDIDATE_PATCH_HASH_MISMATCH")
        for record in records:
            relative = str(record.get("snapshot", ""))
            if not relative:
                continue
            source = (root / Path(*PurePosixPath(relative).parts)).resolve()
            try:
                source.relative_to(root.resolve())
            except ValueError as exc:
                raise CandidateError("CANDIDATE_PATH_ESCAPE") from exc
            if not source.is_file() or _sha(source.read_bytes()) != record.get("candidate_sha256"):
                raise CandidateError("CANDIDATE_HASH_MISMATCH", str(record.get("path", "")))
        return {**data, "manifest_path": str(path)}

    def compatibility(self, candidate_id: str) -> dict[str, Any]:
        candidate = self.load(candidate_id)
        mismatches: list[str] = []
        for record in dict(candidate.get("delta") or {}).get("files") or []:
            path = self._workspace_path(str(record.get("path", "")))
            actual = _sha(path.read_bytes()) if path.is_file() else "DELETED"
            if actual != record.get("before_sha256"):
                mismatches.append(str(record.get("path", "")))
        return {
            "candidate_id": candidate_id,
            "compatible": not mismatches,
            "decision": "CLEAN_APPLY" if not mismatches else "FOCUSED_REBASE_REQUIRED",
            "conflicting_files": mismatches,
        }

    def apply_clean(self, candidate_id: str) -> dict[str, Any]:
        candidate = self.load(candidate_id)
        decision = self.compatibility(candidate_id)
        if not decision["compatible"]:
            raise CandidateError("CANDIDATE_FOCUSED_REBASE_REQUIRED")
        root = Path(candidate["manifest_path"]).parent
        applied: list[str] = []
        for record in dict(candidate.get("delta") or {}).get("files") or []:
            target = self._workspace_path(str(record.get("path", "")))
            if record.get("state") == "DELETED":
                if target.exists() and target.is_file():
                    target.unlink()
            else:
                source = root / Path(*PurePosixPath(str(record["snapshot"])).parts)
                payload = source.read_bytes()
                if _sha(payload) != record.get("candidate_sha256"):
                    raise CandidateError("CANDIDATE_HASH_MISMATCH", str(record.get("path", "")))
                target.parent.mkdir(parents=True, exist_ok=True)
                fd, name = tempfile.mkstemp(prefix=".candidate-apply-", dir=str(target.parent))
                try:
                    with os.fdopen(fd, "wb") as handle:
                        handle.write(payload)
                        handle.flush()
                        os.fsync(handle.fileno())
                    os.replace(name, target)
                finally:
                    try:
                        os.unlink(name)
                    except FileNotFoundError:
                        pass
            applied.append(str(record.get("path", "")))
        return {"candidate_id": candidate_id, "status": "APPLIED_CLEAN", "applied_files": applied}

    def restore_base(self, candidate_id: str) -> dict[str, Any]:
        """Restore only this candidate's paths to its pre-Job snapshot."""
        candidate = self.load(candidate_id)
        manifest_path = Path(str(dict(candidate.get("base") or {}).get("snapshot_manifest", "")))
        before = self._before_records(manifest_path)
        snapshot_root = manifest_path.parent.resolve()
        restored: list[str] = []
        for candidate_record in dict(candidate.get("delta") or {}).get("files") or []:
            relative = str(candidate_record.get("path", ""))
            record = before.get(relative)
            if record is None:
                raise CandidateError("CANDIDATE_BEFORE_EVIDENCE_MISSING", relative)
            target = self._workspace_path(relative)
            if record.get("state") == "DELETED":
                if target.exists() and target.is_file():
                    target.unlink()
            else:
                source = (
                    snapshot_root
                    / Path(*PurePosixPath(str(record.get("snapshot", ""))).parts)
                ).resolve()
                try:
                    source.relative_to(snapshot_root)
                except ValueError as exc:
                    raise CandidateError("CANDIDATE_PATH_ESCAPE", relative) from exc
                payload = source.read_bytes()
                if _sha(payload) != record.get("sha256"):
                    raise CandidateError("CANDIDATE_BASE_HASH_MISMATCH", relative)
                target.parent.mkdir(parents=True, exist_ok=True)
                fd, name = tempfile.mkstemp(prefix=".candidate-restore-", dir=str(target.parent))
                try:
                    with os.fdopen(fd, "wb") as handle:
                        handle.write(payload)
                        handle.flush()
                        os.fsync(handle.fileno())
                    os.replace(name, target)
                finally:
                    try:
                        os.unlink(name)
                    except FileNotFoundError:
                        pass
            restored.append(relative)
        post = self.compatibility(candidate_id)
        if not post["compatible"]:
            raise CandidateError(
                "CANDIDATE_BASE_RESTORE_FAILED", ",".join(post["conflicting_files"])
            )
        return {
            "candidate_id": candidate_id,
            "status": "BASE_RESTORED",
            "restored_files": restored,
        }

    @staticmethod
    def _integration_passed(result: Mapping[str, Any]) -> bool:
        return (
            result.get("success") is True
            and str(result.get("build_status", "")) == "PASS"
            and str(result.get("test_status", "")) in {"PASS", "NOT_REQUIRED"}
            and str(result.get("review_status", "")) == "REVIEW_PASS"
            and str(result.get("verification_status", "")) == "VERIFIED"
            and not str(result.get("failure_code", ""))
        )

    def promote(
        self,
        candidate_ids: Sequence[str],
        integration_verifier: Any,
    ) -> dict[str, Any]:
        """Hash-guarded promotion followed by mandatory integration gates.

        ``integration_verifier`` is the existing Harness verification adapter,
        not an assertion supplied by the operator.  A mismatch never falls back
        to whole-file overwrite; callers receive the focused-rebase code.
        """
        if not candidate_ids:
            raise CandidateError("CANDIDATE_PROMOTION_EMPTY")
        if not callable(integration_verifier):
            raise CandidateError("CANDIDATE_INTEGRATION_VERIFIER_REQUIRED")
        loaded = [self.load(str(candidate_id)) for candidate_id in candidate_ids]
        for index, candidate in enumerate(loaded[1:], start=1):
            expected_parent = str(candidate.get("parent_candidate_id", ""))
            if expected_parent and expected_parent != str(loaded[index - 1]["candidate_id"]):
                raise CandidateError("CANDIDATE_PARENT_REVISION_MISMATCH")
            expected_revision = int(candidate.get("parent_candidate_revision", 0))
            if expected_revision and expected_revision != int(loaded[index - 1].get("candidate_revision", 0)):
                raise CandidateError("CANDIDATE_PARENT_REVISION_MISMATCH")
        applied: list[str] = []
        changed: list[str] = []
        try:
            for candidate in loaded:
                result = self.apply_clean(str(candidate["candidate_id"]))
                applied.append(str(candidate["candidate_id"]))
                changed.extend(result["applied_files"])
            integration = dict(integration_verifier(list(dict.fromkeys(changed))) or {})
            if not self._integration_passed(integration):
                raise CandidateError("CANDIDATE_INTEGRATION_VERIFICATION_FAILED")
        except Exception:
            for candidate_id in reversed(applied):
                self.restore_base(candidate_id)
            raise
        for candidate in loaded:
            path = Path(str(candidate["manifest_path"]))
            candidate["state"] = "PROMOTED"
            candidate["promoted_at"] = _now()
            candidate["integration_evidence"] = integration
            candidate.pop("manifest_path", None)
            self._write_manifest(path.parent, candidate)
        return {
            "status": "PROMOTED",
            "candidate_ids": [str(item["candidate_id"]) for item in loaded],
            "changed_files": list(dict.fromkeys(changed)),
            "integration_evidence": integration,
        }

    def quarantine_and_rollback(
        self,
        task: Any,
        *,
        kind: str = "QA_QUARANTINE",
        parent_qa_job_id: str = "",
        parent_candidate_id: str = "",
        parent_candidate_revision: int = 0,
    ) -> dict[str, Any]:
        candidate = self.create(
            task,
            kind=kind,
            parent_qa_job_id=parent_qa_job_id,
            parent_candidate_id=parent_candidate_id,
            parent_candidate_revision=parent_candidate_revision,
        )
        try:
            from cumulative_policy import CumulativePolicyManager
            baseline = CumulativePolicyManager(
                self.workspace, self.root.parent, self.profile, read_only=True
            ).load_job_baseline(str(getattr(task, "pre_job_snapshot_manifest", "")))
        except Exception as exc:
            raise CandidateError("CANDIDATE_ROLLBACK_BASELINE_INVALID", type(exc).__name__) from exc
        ok, message = GitCollector(str(self.workspace), self.profile).rollback(task=task, baseline=baseline)
        if not ok:
            raise CandidateError("CANDIDATE_ROLLBACK_INTEGRITY_FAILED", message)
        post = self.compatibility(candidate["candidate_id"])
        if not post["compatible"]:
            raise CandidateError("CANDIDATE_ROLLBACK_INTEGRITY_FAILED", ",".join(post["conflicting_files"]))
        return {**candidate, "rollback_status": "PASS", "rollback_message": scrub_secrets(message)}
