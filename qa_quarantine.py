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
import uuid
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any, Mapping, Sequence

from control_paths import atomic_state_write, ensure_safe_state_directory, ensure_safe_state_root
from git_collector import GitCollector
from canonical_ownership import owns_canonical
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

    @owns_canonical
    @owns_canonical
    def apply_clean(self, candidate_id: str) -> dict[str, Any]:
        """Journal-backed forward apply (legacy surface, safe semantics).

        Round-2 (F01/F11): this canonical mutation participates in the SAME
        publication lifecycle as promote() — unresolved-publication fence,
        create-only journal before the first mutation, per-path ownership
        proof, race-checked boundaries.  There is no separate unsafe apply
        route in 1.0.
        """
        self._fence_unresolved_publications()
        candidate = self.load(candidate_id)
        decision = self.compatibility(candidate_id)
        if not decision["compatible"]:
            raise CandidateError("CANDIDATE_FOCUSED_REBASE_REQUIRED")
        journal = self.prepare_publication(
            [candidate], purpose="APPLY",
            authority={"job_id": str(candidate.get("source_job_id", "")),
                       "execution_id": "", "contract_revision": None})
        applied: list[str] = []
        try:
            journal["state"] = "APPLYING"
            journal["updated_at"] = _now()
            self._persist_publication(journal)
            for entry in journal["paths"]:
                self._apply_publication_entry(journal, entry)
                applied.append(entry["path"])
        except Exception:
            self.unwind_publication(journal, "apply_clean failure")
            raise
        journal["state"] = "COMPLETED"
        journal["disposition"] = "APPLIED_NO_INTEGRATION"
        journal["updated_at"] = _now()
        self._persist_publication(journal)
        return {"candidate_id": candidate_id, "status": "APPLIED_CLEAN",
                "applied_files": applied, "publication_id": journal["publication_id"]}

    @owns_canonical
    def restore_base(self, candidate_id: str) -> dict[str, Any]:
        """Journal-backed restore-to-base (legacy surface, safe semantics).

        Round-2 (F02/F10/F11): rollback is a REVERSE publication — intended
        postimage IS the original base state — classified from actual bytes,
        with foreign versions preserved and fenced instead of overwritten.
        """
        candidate = self.load(candidate_id)
        self._fence_unresolved_publications()
        journal = self.prepare_publication(
            [candidate], purpose="RESTORE", reverse=True,
            authority={"job_id": str(candidate.get("source_job_id", "")),
                       "execution_id": "", "contract_revision": None})
        restored: list[str] = []
        try:
            journal["state"] = "APPLYING"
            journal["updated_at"] = _now()
            self._persist_publication(journal)
            for entry in journal["paths"]:
                self._apply_publication_entry(journal, entry)
                restored.append(entry["path"])
        except Exception:
            self.unwind_publication(journal, "restore_base failure")
            raise
        post = self.compatibility(candidate_id)
        if not post["compatible"]:
            journal["state"] = "RECONCILIATION_REQUIRED"
            journal["disposition"] = "BASE_RESTORE_VERIFY_FAILED"
            journal["updated_at"] = _now()
            self._persist_publication(journal)
            raise CandidateError(
                "CANDIDATE_BASE_RESTORE_FAILED", ",".join(post["conflicting_files"])
            )
        journal["state"] = "COMPLETED"
        journal["disposition"] = "BASE_RESTORED"
        journal["updated_at"] = _now()
        self._persist_publication(journal)
        return {"candidate_id": candidate_id, "status": "BASE_RESTORED",
                "restored_files": restored, "publication_id": journal["publication_id"]}

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

    # ------------------------------------------------------------------
    # Durable canonical publication journal (Round-2: F01/F02/F10/F11).
    #
    # ONE lifecycle for every canonical mutation (promote / apply / restore /
    # recovery).  Invariants:
    #   * the journal exists before the first canonical path mutation;
    #   * publication IDs are unique and journals are create-only;
    #   * PENDING never means "not written" — recovery classifies every path
    #     from ACTUAL canonical bytes (base / intended / foreign / unreadable);
    #   * owned postimages are restored through a pre/post-verified mutation
    #     boundary; any race indication becomes reconciliation, never a clean
    #     rollback claim;
    #   * any unresolved publication fences the whole canonical workspace;
    #   * recovery itself is a canonical writer under canonical_ownership.
    # ------------------------------------------------------------------
    PUBLICATION_SCHEMA = 2
    CLEAN_DISPOSITIONS = frozenset(
        {"PROMOTED", "ROLLED_BACK", "APPLIED_NO_INTEGRATION", "BASE_RESTORED"})

    def _publications_dir(self) -> Path:
        directory = self.root / "publications"
        ensure_safe_state_directory(self.root, directory, field="publications", create=True)
        return directory

    def _publication_path(self, publication_id: str) -> Path:
        return self._publications_dir() / f"{publication_id}.json"

    def _persist_publication(self, journal: dict[str, Any], *,
                             create_only: bool = False) -> None:
        path = self._publication_path(str(journal["publication_id"]))
        if create_only and path.exists():
            # Publication IDs are unique; a collision must never overwrite
            # prior evidence (F11).
            raise CandidateError("PUBLICATION_ID_COLLISION", str(journal["publication_id"]))
        atomic_state_write(
            path,
            json.dumps(journal, ensure_ascii=False, sort_keys=True, indent=1).encode("utf-8"),
            root=self.root,
            field="publication_journal",
        )

    def load_publication(self, publication_id: str) -> dict[str, Any]:
        journal = json.loads(
            self._publication_path(str(publication_id)).read_text(encoding="utf-8"))
        if int(journal.get("schema_version", 0)) != self.PUBLICATION_SCHEMA:
            # Historical/unknown journal schemas are not guessed (F11 §9).
            raise CandidateError("PUBLICATION_JOURNAL_SCHEMA_UNSUPPORTED",
                                 str(journal.get("schema_version", "")))
        return journal

    def unresolved_publications(self) -> list[str]:
        """Non-clean publication IDs for THIS canonical workspace."""
        unresolved: list[str] = []
        for path in sorted(self._publications_dir().glob("PUB-*.json")):
            try:
                journal = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                unresolved.append(path.stem)
                continue
            state = str(journal.get("state", ""))
            disposition = str(journal.get("disposition", ""))
            if state != "COMPLETED" or disposition not in self.CLEAN_DISPOSITIONS:
                unresolved.append(str(journal.get("publication_id", path.stem)))
        return unresolved

    def _fence_unresolved_publications(self, *, exclude: str = "") -> None:
        unresolved = [pid for pid in self.unresolved_publications() if pid != exclude]
        if unresolved:
            # Workspace-wide fence: an unresolved publication means canonical
            # truth is not fully trusted; a second candidate may not mutate
            # even unrelated paths (F11 §6).
            raise CandidateError(
                "CANONICAL_PUBLICATION_UNRESOLVED", ",".join(sorted(unresolved)))

    def _observe_path(self, relative: str) -> str:
        """Current content identity: sha256, ABSENT, or UNSUPPORTED."""
        target = self._workspace_path(relative)
        if not target.exists():
            return "ABSENT"
        if target.is_file():
            return _sha(target.read_bytes())
        return "UNSUPPORTED"

    def prepare_publication(self, candidates: Sequence[Mapping[str, Any]], *,
                            authority: Mapping[str, Any] | None = None,
                            purpose: str = "PROMOTE",
                            reverse: bool = False,
                            publication_id: str | None = None) -> dict[str, Any]:
        """Durable PREPARED journal: base and intended postimage per path.

        publication_id is a fresh UUID-derived identity (never clock-derived),
        and the journal file is create-only: a collision fails instead of
        overwriting prior evidence.  ``authority`` binds the publication to
        its Job/Execution/contract identity; ``reverse`` builds a
        restore-to-base publication (intended = original base state).
        """
        publication_id = publication_id or "PUB-" + uuid.uuid4().hex.upper()
        if not re.fullmatch(r"PUB-[0-9A-F]{32}", publication_id):
            raise CandidateError("PUBLICATION_ID_INVALID")
        authority = dict(authority or {})
        paths: list[dict[str, Any]] = []
        intended_roots: dict[str, str] = {}
        base_roots: dict[str, str] = {}
        for candidate in candidates:
            candidate_id = str(candidate["candidate_id"])
            snapshot_manifest = str(dict(candidate.get("base") or {}).get("snapshot_manifest", ""))
            before = self._before_records(snapshot_manifest)
            base_manifest_dir = str(Path(snapshot_manifest).parent)
            # Forward: unwind restores the original base bytes from the base
            # snapshot manifest tree; apply writes candidate bytes from the
            # candidate store.  Reverse (restore-to-base): symmetric swap.
            base_roots[candidate_id] = self.root / candidate_id if reverse else base_manifest_dir
            intended_roots[candidate_id] = base_manifest_dir if reverse else str(self.root / candidate_id)
            for record in dict(candidate.get("delta") or {}).get("files") or []:
                relative = str(record.get("path", ""))
                base_record = before.get(relative) or {}
                candidate_state = str(record.get("state", ""))
                candidate_sha = str(record.get("candidate_sha256", "") or "")
                candidate_snapshot = str(record.get("snapshot", "") or "")
                original_base_state = base_record.get("state", "DELETED")
                original_base_sha = str(base_record.get("sha256", "") or "")
                original_base_snapshot = str(base_record.get("snapshot", "") or "")
                if reverse:
                    # Restore publication: roll back FROM the candidate
                    # postimage TO the original base state.
                    entry_base_state, entry_base_sha = candidate_state, candidate_sha
                    entry_base_snapshot = candidate_snapshot
                    intended_state, intended_sha = original_base_state, original_base_sha
                    intended_snapshot = original_base_snapshot
                    allowed_current = {
                        "ABSENT" if candidate_state == "DELETED" else candidate_sha,
                        "ABSENT" if original_base_state == "DELETED" else original_base_sha,
                    }
                else:
                    entry_base_state, entry_base_sha = original_base_state, original_base_sha
                    entry_base_snapshot = original_base_snapshot
                    intended_state, intended_sha = candidate_state, candidate_sha
                    intended_snapshot = candidate_snapshot
                    allowed_current = {
                        "ABSENT" if original_base_state == "DELETED" else original_base_sha,
                        candidate_sha,
                    }
                current = self._observe_path(relative)
                if current == "UNSUPPORTED":
                    raise CandidateError("CANONICAL_PATH_TYPE_UNSUPPORTED", relative)
                if current not in allowed_current:
                    # Durable fence: canonical already holds a third version
                    # for a touched path.  Never mutate; record evidence.
                    self._fence_preexisting_foreign(
                        candidates, {relative: current}, purpose)
                    raise CandidateError("CANONICAL_PREEXISTING_FOREIGN_STATE", relative)
                paths.append({
                    "path": relative,
                    "candidate_id": candidate_id,
                    "base_state": entry_base_state,
                    "base_sha256": entry_base_sha,
                    "base_snapshot": entry_base_snapshot,
                    "intended_state": intended_state,
                    "intended_sha256": intended_sha,
                    "snapshot": intended_snapshot,
                    "pre_state": current,
                    "mutation_state": "PENDING",
                    "observed_sha256": "",
                })
        journal = {
            "schema_version": self.PUBLICATION_SCHEMA,
            "kind": "CanonicalPublicationJournal",
            "publication_id": publication_id,
            "purpose": purpose,
            "candidate_ids": [str(c["candidate_id"]) for c in candidates],
            "authority": {
                "job_id": str(authority.get("job_id", "")),
                "execution_id": str(authority.get("execution_id", "")),
                "contract_revision": authority.get("contract_revision"),
                "workspace": str(Path(self.workspace).resolve()),
                "store_root": str(Path(self.root).resolve()),
            },
            "base_snapshot_roots": {k: str(v) for k, v in base_roots.items()},
            "intended_snapshot_roots": {k: str(v) for k, v in intended_roots.items()},
            "workspace": str(self.workspace),
            "state": "PREPARED",
            "disposition": "",
            "paths": paths,
            "created_at": _now(),
            "updated_at": _now(),
            "notes": [],
        }
        self._persist_publication(journal, create_only=True)
        return journal

    def _fence_preexisting_foreign(self, candidates: Sequence[Mapping[str, Any]],
                                   foreign_by_path: dict[str, str],
                                   purpose: str) -> None:
        """Durable fence when canonical already holds a third version."""
        journal = {
            "schema_version": self.PUBLICATION_SCHEMA,
            "kind": "CanonicalPublicationJournal",
            "publication_id": "PUB-" + uuid.uuid4().hex.upper(),
            "purpose": purpose,
            "candidate_ids": [str(c["candidate_id"]) for c in candidates],
            "authority": {
                "job_id": "", "execution_id": "", "contract_revision": None,
                "workspace": str(Path(self.workspace).resolve()),
                "store_root": str(Path(self.root).resolve()),
            },
            "base_snapshot_roots": {}, "intended_snapshot_roots": {},
            "workspace": str(self.workspace),
            "state": "RECONCILIATION_REQUIRED",
            "disposition": "FOREIGN_EDITS_PRESERVED",
            "foreign_paths": sorted(foreign_by_path),
            "paths": [{"path": path, "candidate_id": "",
                       "base_state": "UNKNOWN", "base_sha256": "",
                       "base_snapshot": "", "intended_state": "UNKNOWN",
                       "intended_sha256": "", "snapshot": "",
                       "pre_state": identity, "mutation_state": "FOREIGN_PRESERVED",
                       "observed_sha256": "", "foreign_sha256": identity}
                      for path, identity in sorted(foreign_by_path.items())],
            "created_at": _now(), "updated_at": _now(),
            "notes": ["pre-existing foreign state at prepare"],
        }
        self._persist_publication(journal, create_only=True)
        evidence_dir = self._publications_dir() / journal["publication_id"]
        evidence_dir.mkdir(parents=True, exist_ok=True)
        for path in foreign_by_path:
            foreign_file = self._workspace_path(path)
            if foreign_file.is_file():
                (evidence_dir / (path.replace("/", "__") + ".foreign")).write_bytes(
                    foreign_file.read_bytes())

    def _apply_publication_entry(self, journal: dict[str, Any], entry: dict[str, Any]) -> None:
        relative = entry["path"]
        target = self._workspace_path(relative)
        roots = journal.get("intended_snapshot_roots") or {}
        intended_root = Path(str(roots.get(entry["candidate_id"], ""))) if str(
            roots.get(entry["candidate_id"], "")) else (self.root / entry["candidate_id"])
        if entry["intended_state"] == "DELETED":
            if target.exists() and target.is_file():
                target.unlink()
            observed = "ABSENT"
        else:
            source = (
                intended_root
                / Path(*PurePosixPath(entry["snapshot"]).parts)
            ).resolve()
            try:
                source.relative_to(intended_root.resolve())
            except ValueError as exc:
                raise CandidateError("CANDIDATE_PATH_ESCAPE", relative) from exc
            payload = source.read_bytes()
            if _sha(payload) != entry["intended_sha256"]:
                raise CandidateError("CANDIDATE_HASH_MISMATCH", relative)
            target.parent.mkdir(parents=True, exist_ok=True)
            fd, name = tempfile.mkstemp(prefix=".publication-", dir=str(target.parent))
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
            observed = _sha(target.read_bytes())
        if observed != (entry["intended_sha256"] if entry["intended_state"] != "DELETED" else "ABSENT"):
            raise CandidateError("PUBLICATION_POSTIMAGE_UNVERIFIED", relative)
        entry["mutation_state"] = "APPLIED"
        entry["observed_sha256"] = observed
        journal["updated_at"] = _now()
        self._persist_publication(journal)

    def _restore_entry_base(self, journal: dict[str, Any], entry: dict[str, Any]) -> None:
        """Restore one OWNED postimage path to its recorded base state.

        Mutation-boundary race contract (F02 §10): canonical ownership covers
        all cooperating Harness writers; the exact current identity is
        verified immediately before replacement and the exact postcondition is
        verified after it.  Any mismatch raises PUBLICATION_RACE_DETECTED so
        the caller fences the path for reconciliation instead of claiming a
        clean rollback.  Foreign bytes are never the input of this method.
        """
        relative = entry["path"]
        target = self._workspace_path(relative)
        base_identity = "ABSENT" if entry["base_state"] == "DELETED" else entry["base_sha256"]
        if entry["base_state"] == "DELETED":
            if target.exists() and target.is_file():
                target.unlink()
            if self._observe_path(relative) != "ABSENT":
                raise CandidateError("PUBLICATION_RACE_DETECTED", relative)
            return
        roots = journal.get("base_snapshot_roots") or {}
        snapshot_root = Path(str(roots.get(entry["candidate_id"], "")))
        source = (
            snapshot_root
            / Path(*PurePosixPath(entry["base_snapshot"]).parts)
        ).resolve()
        try:
            source.relative_to(snapshot_root.resolve())
        except ValueError as exc:
            raise CandidateError("CANDIDATE_PATH_ESCAPE", relative) from exc
        payload = source.read_bytes()
        if _sha(payload) != entry["base_sha256"]:
            raise CandidateError("CANDIDATE_BASE_HASH_MISMATCH", relative)
        # Boundary pre-check: the path must still hold the owned postimage.
        if self._observe_path(relative) != self._owned_identity(entry):
            raise CandidateError("PUBLICATION_RACE_DETECTED", relative)
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, name = tempfile.mkstemp(prefix=".publication-restore-", dir=str(target.parent))
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
        # Postcondition check: the path must now hold the exact base bytes.
        if self._observe_path(relative) != base_identity:
            raise CandidateError("PUBLICATION_RACE_DETECTED", relative)

    @staticmethod
    def _owned_identity(entry: Mapping[str, Any]) -> str:
        return "ABSENT" if entry["intended_state"] == "DELETED" else entry["intended_sha256"]

    def unwind_publication(self, journal: dict[str, Any], reason: str) -> dict[str, Any]:
        """Safe unwind classified from ACTUAL canonical bytes (F10).

        PENDING never means "not written": the process may have died between
        the filesystem mutation and the journal update, so EVERY path is
        classified from its current identity:
          current == base          -> already safely at base (skip);
          current == intended      -> owned postimage (even if journal says
                                      PENDING) -> race-checked restore to base;
          anything else            -> foreign/third version: preserved
                                      untouched with evidence, publication
                                      fenced RECONCILIATION_REQUIRED;
          unreadable/unsupported   -> fail closed reconciliation.
        A clean ROLLED_BACK is claimed only when actual bytes prove it.
        """
        journal["state"] = "RESTORING"
        journal["notes"].append(f"unwind: {reason}")
        journal["updated_at"] = _now()
        self._persist_publication(journal)
        foreign: list[str] = []
        evidence_dir = self._publications_dir() / str(journal["publication_id"])
        for entry in reversed(journal["paths"]):
            current = self._observe_path(entry["path"])
            base_identity = "ABSENT" if entry["base_state"] == "DELETED" else entry["base_sha256"]
            intended_identity = self._owned_identity(entry)
            if current == "UNSUPPORTED":
                entry["mutation_state"] = "RECONCILE_UNREADABLE"
                foreign.append(entry["path"])
            elif current == base_identity:
                entry["mutation_state"] = "RESTORED_ALREADY_BASE"
                journal["updated_at"] = _now()
                self._persist_publication(journal)
                continue
            elif current == intended_identity:
                observed = str(entry.get("observed_sha256") or "")
                if entry["mutation_state"] == "APPLIED" and observed and observed != current:
                    # Journal-proven postimage disagrees with actual bytes.
                    entry["mutation_state"] = "RECONCILE_IDENTITY_MISMATCH"
                    foreign.append(entry["path"])
                else:
                    # Owned postimage (including crash-window PENDING writes).
                    try:
                        self._restore_entry_base(journal, entry)
                        entry["mutation_state"] = "RESTORED_FROM_OWNED"
                    except CandidateError:
                        # Race at the mutation boundary: do not overwrite; the
                        # bytes may have become foreign after classification.
                        entry["mutation_state"] = "FOREIGN_PRESERVED"
                        entry["foreign_sha256"] = self._observe_path(entry["path"])
                        foreign.append(entry["path"])
            else:
                entry["mutation_state"] = "FOREIGN_PRESERVED"
                entry["foreign_sha256"] = current
                foreign.append(entry["path"])
            if entry["mutation_state"] in {"FOREIGN_PRESERVED", "RECONCILE_UNREADABLE",
                                           "RECONCILE_IDENTITY_MISMATCH"}:
                evidence_dir.mkdir(parents=True, exist_ok=True)
                foreign_file = self._workspace_path(entry["path"])
                if foreign_file.is_file():
                    (evidence_dir / (entry["path"].replace("/", "__") + ".foreign")).write_bytes(
                        foreign_file.read_bytes())
            journal["updated_at"] = _now()
            self._persist_publication(journal)
        if foreign:
            journal["state"] = "RECONCILIATION_REQUIRED"
            journal["disposition"] = "FOREIGN_EDITS_PRESERVED"
            journal["foreign_paths"] = foreign
            journal["updated_at"] = _now()
            self._persist_publication(journal)
            raise CandidateError(
                "CANONICAL_PUBLICATION_RECONCILIATION_REQUIRED", ",".join(foreign)
            )
        # Prove the rollback claim from actual bytes before declaring clean.
        for entry in journal["paths"]:
            expected = "ABSENT" if entry["base_state"] == "DELETED" else entry["base_sha256"]
            if self._observe_path(entry["path"]) != expected:
                journal["state"] = "RECONCILIATION_REQUIRED"
                journal["disposition"] = "ROLLBACK_POSTCONDITION_UNPROVEN"
                journal["updated_at"] = _now()
                self._persist_publication(journal)
                raise CandidateError(
                    "CANONICAL_PUBLICATION_RECONCILIATION_REQUIRED", entry["path"])
        journal["state"] = "COMPLETED"
        journal["disposition"] = "ROLLED_BACK"
        journal["updated_at"] = _now()
        self._persist_publication(journal)
        return {"publication_id": journal["publication_id"],
                "status": journal["disposition"],
                "restored": [e["path"] for e in journal["paths"]
                             if e["mutation_state"].startswith("RESTORED")]}

    @owns_canonical
    def recover_publication(self, publication_id: str) -> dict[str, Any]:
        """Post-interruption recovery: a canonical writer under ownership.

        Never blindly replays.  Validates journal authority (workspace/store
        identity, loadable candidate set), classifies every path from ACTUAL
        canonical bytes, and only then derives a safe disposition.  A COMPLETED
        claim is re-verified against actual bytes; a mismatch is fenced as
        reconciliation, never reported clean (F10 §4).
        """
        journal = self.load_publication(publication_id)
        authority = dict(journal.get("authority") or {})
        if (str(authority.get("workspace", "")) != str(Path(self.workspace).resolve())
                or str(authority.get("store_root", "")) != str(Path(self.root).resolve())):
            raise CandidateError("PUBLICATION_AUTHORITY_MISMATCH", publication_id)
        for candidate_id in journal.get("candidate_ids") or []:
            try:
                self.load(str(candidate_id))
            except CandidateError as exc:
                raise CandidateError("PUBLICATION_CANDIDATE_EVIDENCE_INVALID",
                                     f"{candidate_id}:{exc.code}") from exc
        state = str(journal.get("state", ""))
        if state == "RECONCILIATION_REQUIRED":
            return {"publication_id": publication_id, "state": state,
                    "disposition": journal.get("disposition", ""), "action": "FENCED"}
        if state == "COMPLETED":
            disposition = str(journal.get("disposition", ""))
            if disposition not in self.CLEAN_DISPOSITIONS:
                return {"publication_id": publication_id, "state": state,
                        "disposition": disposition, "action": "FENCED"}
            mismatches = self._completed_disposition_mismatches(journal)
            if mismatches:
                journal["state"] = "RECONCILIATION_REQUIRED"
                journal["disposition"] = "COMPLETED_DISPOSITION_UNPROVEN"
                journal["notes"].append("recovery verify: " + ",".join(mismatches))
                journal["updated_at"] = _now()
                self._persist_publication(journal)
                return {"publication_id": publication_id, "state": "RECONCILIATION_REQUIRED",
                        "disposition": journal["disposition"],
                        "action": "DISPOSITION_MISMATCH_FENCED", "mismatches": mismatches}
            return {"publication_id": publication_id, "state": state,
                    "disposition": disposition, "action": "VERIFIED_CLEAN"}
        if state == "APPLIED" and not journal.get("integration_receipt"):
            # Integration outcome unknown: conservatively unwind owned paths.
            # No blind re-apply, no promotion without a receipt.
            self.unwind_publication(journal, "recovery: integration receipt absent")
            return {"publication_id": publication_id, "state": "COMPLETED",
                    "disposition": "ROLLED_BACK", "action": "UNWOUND_OWNED_NO_RECEIPT"}
        if state in {"PREPARED", "APPLYING", "RESTORING"}:
            self.unwind_publication(journal, f"recovery from {state}")
            return {"publication_id": publication_id, "state": "COMPLETED",
                    "disposition": "ROLLED_BACK", "action": "UNWOUND"}
        return {"publication_id": publication_id, "state": state, "action": "INSPECT_ONLY"}

    def _completed_disposition_mismatches(self, journal: Mapping[str, Any]) -> list[str]:
        """Actual-byte proof for a COMPLETED claim (F10 §4 last rule)."""
        mismatches: list[str] = []
        disposition = str(journal.get("disposition", ""))
        for entry in journal.get("paths") or []:
            if entry.get("mutation_state") == "FOREIGN_PRESERVED":
                continue
            current = self._observe_path(str(entry.get("path", "")))
            if disposition == "ROLLED_BACK":
                expected = "ABSENT" if entry.get("base_state") == "DELETED" \
                    else str(entry.get("base_sha256", ""))
            else:  # PROMOTED / APPLIED_NO_INTEGRATION / BASE_RESTORED
                expected = self._owned_identity(entry)
            if current != expected:
                mismatches.append(str(entry.get("path", "")))
        return mismatches


    @owns_canonical
    def promote(
        self,
        candidate_ids: Sequence[str],
        integration_verifier: Any,
        *,
        authority: Mapping[str, Any] | None = None,
        publication_id: str | None = None,
    ) -> dict[str, Any]:
        """Journal-guarded canonical promotion with mandatory integration gates.

        ``integration_verifier`` is the existing Harness verification adapter,
        not an assertion supplied by the operator.  Every canonical mutation is
        journalled with a post-write ownership proof; any failure unwinds only
        proven-owned postimages and preserves foreign bytes untouched
        (RECONCILIATION_REQUIRED) instead of choosing a silent winner.
        """
        if not candidate_ids:
            raise CandidateError("CANDIDATE_PROMOTION_EMPTY")
        if not callable(integration_verifier):
            raise CandidateError("CANDIDATE_INTEGRATION_VERIFIER_REQUIRED")
        loaded = [self.load(str(candidate_id)) for candidate_id in candidate_ids]
        # Workspace-wide unresolved fence FIRST: when any publication for this
        # canonical workspace is unresolved, canonical truth is not trusted
        # and no new mutation may even be considered (F11 §6).
        self._fence_unresolved_publications()
        for index, candidate in enumerate(loaded[1:], start=1):
            expected_parent = str(candidate.get("parent_candidate_id", ""))
            if expected_parent and expected_parent != str(loaded[index - 1]["candidate_id"]):
                raise CandidateError("CANDIDATE_PARENT_REVISION_MISMATCH")
            expected_revision = int(candidate.get("parent_candidate_revision", 0))
            if expected_revision and expected_revision != int(loaded[index - 1].get("candidate_revision", 0)):
                raise CandidateError("CANDIDATE_PARENT_REVISION_MISMATCH")
        for candidate in loaded:
            decision = self.compatibility(str(candidate["candidate_id"]))
            if not decision["compatible"]:
                raise CandidateError("CANDIDATE_FOCUSED_REBASE_REQUIRED")
        journal = self.prepare_publication(
            loaded, purpose="PROMOTE",
            authority=authority or {"job_id": "", "execution_id": "", "contract_revision": None},
            publication_id=publication_id)
        changed: list[str] = []
        try:
            journal["state"] = "APPLYING"
            journal["updated_at"] = _now()
            self._persist_publication(journal)
            for entry in journal["paths"]:
                self._apply_publication_entry(journal, entry)
                if entry["path"] not in changed:
                    changed.append(entry["path"])
            journal["state"] = "APPLIED"
            journal["updated_at"] = _now()
            self._persist_publication(journal)
            integration = dict(integration_verifier(list(dict.fromkeys(changed))) or {})
            if not self._integration_passed(integration):
                raise CandidateError("CANDIDATE_INTEGRATION_VERIFICATION_FAILED")
            journal["integration_receipt"] = {
                "passed": True,
                "evidence": scrub_secrets(json.dumps(integration, default=str))[:4000],
            }
            journal["updated_at"] = _now()
            self._persist_publication(journal)
        except Exception:
            self.unwind_publication(journal, "apply/integration failure")
            raise
        for candidate in loaded:
            path = Path(str(candidate["manifest_path"]))
            candidate["state"] = "PROMOTED"
            candidate["promoted_at"] = _now()
            candidate["integration_evidence"] = integration
            candidate.pop("manifest_path", None)
            self._write_manifest(path.parent, candidate)
        journal["state"] = "COMPLETED"
        journal["disposition"] = "PROMOTED"
        journal["updated_at"] = _now()
        self._persist_publication(journal)
        return {
            "status": "PROMOTED",
            "publication_id": journal["publication_id"],
            "candidate_ids": [str(item["candidate_id"]) for item in loaded],
            "changed_files": list(dict.fromkeys(changed)),
            "integration_evidence": integration,
        }

    @owns_canonical
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
