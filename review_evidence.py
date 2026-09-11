"""Conservative reuse of prior review PASS evidence for unchanged diff scope.

0.99.1 evidence-proportional review.  A prior PASS review unit is reusable
only while its exact proof inputs remain valid: the same diff chunk identity
and content plus the same acceptance/contract/execution surface.  Anything
changed or ambiguous is STALE and reviewed again.  This is a logical review
frontier over the existing ``DiffBatch``/changed-atom ledger; it adds no new
lifecycle, service, or schema.

Rules:
- Only PASS units are ever recorded or reused.  FAIL/infra scopes are always
  reviewed again.
- Reuse requires an exact input match on ``acceptance_hash``,
  ``contract_revision`` and ``execution_surface_hash``.
- Reuse is disabled for HIGH/CRITICAL review depth (LOCAL_IMPACT/DOMAIN):
  cross-module semantic closure of a fix cannot be proven deterministically,
  so those tiers conservatively re-review the full diff.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from reviewer import DiffBatch, DiffBatchPlan, ReviewResult, assemble_batches


EVIDENCE_SCHEMA = "review-evidence/1"
REUSE_TIERS = frozenset({"DIFF_ONLY", "LOCAL"})


class ReviewEvidenceError(RuntimeError):
    pass


def unit_key(chunk: Any) -> str:
    """Exact content identity of one review unit.

    The key embeds the chunk content hash, so any byte change to the file's
    hunk produces a different key and the prior unit is no longer reusable.
    """
    return "|".join((
        str(getattr(chunk, "module", "")),
        str(getattr(chunk, "path", "")),
        str(getattr(chunk, "hunk_id", "")),
        str(getattr(chunk, "content_sha256", "")),
    ))


def fresh_units(
    evidence: Mapping[str, Any] | None,
    *,
    acceptance_hash: str,
    contract_revision: int,
    execution_surface_hash: str,
) -> dict[str, dict[str, Any]]:
    """Return reusable prior units only when every proof input still matches."""
    record = dict(evidence or {})
    if not record or str(record.get("schema", "")) != EVIDENCE_SCHEMA:
        return {}
    bound = record.get("proof_inputs") or {}
    if (
        str(bound.get("acceptance_hash", "")) != str(acceptance_hash)
        or str(bound.get("contract_revision", "")) != str(contract_revision)
        or str(bound.get("execution_surface_hash", "")) != str(execution_surface_hash)
    ):
        return {}
    units = record.get("units") or {}
    return dict(units) if isinstance(units, Mapping) else {}


@dataclass(frozen=True)
class ReusePartition:
    reusable_chunks: tuple[Any, ...] = ()
    review_chunks: tuple[Any, ...] = ()
    reused_files: tuple[str, ...] = ()
    stale_files: tuple[str, ...] = ()
    prior_round: int = 0

    @property
    def enabled(self) -> bool:
        return bool(self.reusable_chunks) and bool(self.review_chunks)


def partition(
    plan: DiffBatchPlan,
    units: Mapping[str, Mapping[str, Any]],
) -> ReusePartition:
    """Split plan chunks into reusable (exact prior PASS) and review sets."""
    reusable: list[Any] = []
    review: list[Any] = []
    reused_files: list[str] = []
    stale_files: list[str] = []
    if not units:
        review = [chunk for batch in plan.batches for chunk in batch.chunks]
        return ReusePartition(
            review_chunks=tuple(review),
            stale_files=tuple(_batch_files(plan.batches)),
        )
    seen_reused: set[str] = set()
    seen_stale: set[str] = set()
    for batch in plan.batches:
        for chunk in batch.chunks:
            prior = units.get(unit_key(chunk))
            if prior and str(prior.get("chunk_id", "")) == str(chunk.chunk_id):
                reusable.append(chunk)
                name = _chunk_file(chunk)
                if name not in seen_reused:
                    seen_reused.add(name)
                    reused_files.append(name)
            else:
                review.append(chunk)
                name = _chunk_file(chunk)
                if name not in seen_stale:
                    seen_stale.add(name)
                    stale_files.append(name)
    prior_round = max(
        (int(unit.get("round", 0)) for unit in units.values() if unit.get("round")),
        default=0,
    )
    return ReusePartition(
        reusable_chunks=tuple(reusable),
        review_chunks=tuple(review),
        reused_files=tuple(reused_files),
        stale_files=tuple(stale_files),
        prior_round=prior_round,
    )


def reused_batch(partition: ReusePartition, max_chars: int, max_batches: int) -> tuple[DiffBatch, str]:
    """Build the synthetic PASS carrier for reusable chunks.

    The carrier keeps the changed-atom ledger lossless: aggregation counts its
    atoms as reviewed, while its empty text means no reviewer is invoked.
    """
    chunks = list(partition.reusable_chunks)
    if not chunks:
        raise ReviewEvidenceError("REUSE_PARTITION_EMPTY")
    reused_id = "REUSED-" + hashlib.sha256(
        "|".join(chunk.chunk_id for chunk in chunks).encode("utf-8")
    ).hexdigest()[:20]
    atoms = tuple(atom for chunk in chunks for atom in chunk.changed_atoms)
    files = tuple(_chunk_files(chunks))
    batch = DiffBatch(reused_id, 0, "", tuple(chunks), atoms, files)
    reason = (
        f"REUSED_VALID_EVIDENCE: {len(chunks)} chunk(s) byte-identical to "
        f"prior PASS review (round {partition.prior_round}); "
        f"files={', '.join(files) if files else 'none'}"
    )
    return batch, reason


def residual_plan(
    plan: DiffBatchPlan,
    partition: ReusePartition,
) -> DiffBatchPlan | None:
    """Plan the review-only batches for non-reusable chunks.

    Returns ``None`` when the residual cannot be assembled losslessly under
    the same limits; the caller then conservatively reviews the full plan.
    """
    if not partition.review_chunks:
        return None
    batches, failure = assemble_batches(
        list(partition.review_chunks), plan.max_chars, plan.max_batches,
    )
    if failure or not batches:
        return None
    return DiffBatchPlan(
        batches=batches,
        expected_atoms=plan.expected_atoms,
        assigned_atoms=plan.assigned_atoms,
        max_chars=plan.max_chars,
        max_batches=plan.max_batches,
    )


def reused_result(batch: DiffBatch, reason: str) -> ReviewResult:
    return ReviewResult(
        passed=True,
        status="REVIEW_PASS",
        reason=reason,
        reviewed_files=list(batch.changed_files),
    )


def record(
    pairs: Iterable[tuple[Any, ReviewResult]],
    *,
    acceptance_hash: str,
    contract_revision: int,
    execution_surface_hash: str,
    round_no: int,
    prior_units: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Persist the reusable PASS units of one completed review round.

    Reused carrier batches re-record their units under the current round so a
    third round can keep chaining exact evidence.  FAIL and infrastructure
    scopes are intentionally not recorded: they must be reviewed again.
    """
    units: dict[str, dict[str, Any]] = {}
    for key, unit in dict(prior_units or {}).items():
        units[str(key)] = dict(unit)
    for batch, result in pairs:
        if getattr(result, "status", "") != "REVIEW_PASS":
            continue
        for chunk in getattr(batch, "chunks", ()) or ():
            units[unit_key(chunk)] = {
                "chunk_id": str(chunk.chunk_id),
                "module": str(chunk.module),
                "path": _chunk_file(chunk),
                "raw_path": str(chunk.path),
                "hunk_id": str(chunk.hunk_id),
                "content_sha256": str(chunk.content_sha256),
                "round": int(round_no),
            }
    return {
        "schema": EVIDENCE_SCHEMA,
        "proof_inputs": {
            "acceptance_hash": str(acceptance_hash),
            "contract_revision": int(contract_revision),
            "execution_surface_hash": str(execution_surface_hash),
        },
        "units": units,
    }


def _chunk_file(chunk: Any) -> str:
    module = str(getattr(chunk, "module", ""))
    path = str(getattr(chunk, "path", ""))
    return f"{module}/{path}" if module else path


def _chunk_files(chunks: Iterable[Any]) -> list[str]:
    files: list[str] = []
    for chunk in chunks:
        name = _chunk_file(chunk)
        if name not in files:
            files.append(name)
    return files


def _batch_files(batches: Iterable[DiffBatch]) -> list[str]:
    files: list[str] = []
    for batch in batches:
        for name in batch.changed_files:
            if name not in files:
                files.append(name)
    return files
