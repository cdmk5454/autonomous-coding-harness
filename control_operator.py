"""Local-only manual resolution CLI for stopped ChatGPT control Jobs.

This module is deliberately not imported by ``chatgpt_control_mcp``.  It closes a
queue gate only while holding the same OS execution lease as the harness and only
after an operator supplies the Job revision and a resolution-specific confirmation.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Callable

from control_paths import (
    ControlPathError,
    validate_control_state_roots_read_only,
)
from harness_service import HarnessService, HarnessServiceError
from chatgpt_control import ChatGPTControlService
from job_queue import (
    EXTERNAL_BASELINE_ADOPTION_CONFIRMATION,
    EXTERNAL_HANDOFF_CONFIRMATION,
    MANUAL_RESOLUTION_ACCEPT_PRESERVED,
    MANUAL_RESOLUTION_CONFIRMATIONS,
    MANUAL_RESOLUTION_VERIFIED_CLEAN,
    JobStore,
    QueueError,
)
from batch_manifest import BatchManifestError, prepare_ab_only
from run import HARNESS_ROOT
from runtime_safety import safe_print as print
from notify import send_notify_evidence


class LocalOperatorResolver:
    """Resolve a durable blocker while proving the harness is OS-idle."""

    def __init__(
        self,
        store_factory: Callable[[], JobStore],
        harness: HarnessService,
    ):
        self.store_factory = store_factory
        self.harness = harness

    def resolve(
        self,
        job_id: str,
        *,
        resolution: str,
        confirmation: str,
        expected_revision: int,
        request_id: str,
    ) -> tuple[dict[str, Any], bool]:
        if not self.harness.probe_execution_idle():
            raise QueueError("ACTIVE_HARNESS_PROCESS", job_id)
        if not self.harness.acquire_execution_lease():
            raise QueueError("ACTIVE_HARNESS_PROCESS", job_id)
        try:
            # Writable startup migration/recovery is itself a state mutation, so
            # construct the store only after the shared OS execution lease is held.
            store = self.store_factory()
            if bool(getattr(store, "read_only", False)):
                raise QueueError("CONTROL_STORE_READ_ONLY")
            return store.resolve_manually(
                job_id,
                resolution=resolution,
                confirmation=confirmation,
                expected_revision=expected_revision,
                request_id=request_id,
            )
        finally:
            self.harness.release_execution_lease()

    def supersede_batch_for_external_handoff(self, **kwargs) -> tuple[dict[str, Any], bool]:
        if not self.harness.probe_execution_idle():
            raise QueueError("ACTIVE_HARNESS_PROCESS")
        if not self.harness.acquire_execution_lease():
            raise QueueError("ACTIVE_HARNESS_PROCESS")
        try:
            store = self.store_factory()
            return store.supersede_batch_for_external_handoff(
                execution_idle=True, **kwargs
            )
        finally:
            self.harness.release_execution_lease()

    def adopt_external_baseline(self, **kwargs) -> tuple[dict[str, Any], bool]:
        if not self.harness.probe_execution_idle():
            raise QueueError("ACTIVE_HARNESS_PROCESS")
        if not self.harness.acquire_execution_lease():
            raise QueueError("ACTIVE_HARNESS_PROCESS")
        try:
            store = self.store_factory()
            queue = store.queue_snapshot()
            fields = self.harness.prepare_external_baseline_adoption(
                int(kwargs["expected_generation"]),
                str(queue.get("active_baseline_id", "")),
                kwargs.pop("source_manifest_path"),
            )
            result, replayed = store.adopt_external_baseline(
                fields=fields, execution_idle=True, **kwargs
            )
            self.harness.activate_prepared_baseline(
                str(result["baseline_id"]), int(result["generation"])
            )
            return result, replayed
        finally:
            self.harness.release_execution_lease()


def _public_resolution(job: dict[str, Any], replayed: bool) -> dict[str, Any]:
    return {
        "ok": True,
        "replayed": replayed,
        "job_id": job.get("job_id", ""),
        "status": job.get("status", ""),
        "revision": job.get("revision", 0),
        "manual_resolution": dict(job.get("manual_resolution") or {}),
        "last_result": {
            key: dict(job.get("last_result") or {}).get(key, "")
            for key in (
                "status",
                "failure_code",
                "manual_resolution",
                "manual_resolution_code",
                "resolved_from_status",
                "worktree_disposition",
            )
        },
    }


def _public_inspection(store: JobStore, job_id: str) -> dict[str, Any]:
    job = store.get_job(job_id)
    return {
        "ok": True,
        "job": {
            "job_id": job.get("job_id", ""),
            "status": job.get("status", ""),
            "revision": job.get("revision", 0),
            "outer_attempt": job.get("outer_attempt", 0),
            "task_ids": list(job.get("task_ids") or []),
            "last_result": {
                key: dict(job.get("last_result") or {}).get(key, "")
                for key in (
                    "status",
                    "failure_stage",
                    "failure_code",
                    "worktree_disposition",
                )
            },
        },
        "queue": store.queue_snapshot(),
        "confirmations": dict(MANUAL_RESOLUTION_CONFIRMATIONS),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Resolve one stopped harness Job from the local machine only"
    )
    parser.add_argument("--working-dir", required=True)
    parser.add_argument("--profile", dest="profile_dir", required=True)
    parser.add_argument(
        "--control-root", default=str(HARNESS_ROOT / ".control")
    )
    parser.add_argument("--task-root", default=str(HARNESS_ROOT / ".tasks"))
    commands = parser.add_subparsers(dest="command", required=True)

    inspect_command = commands.add_parser("inspect")
    inspect_command.add_argument("--job-id", required=True)

    resolve_command = commands.add_parser("resolve")
    resolve_command.add_argument("--job-id", required=True)
    resolve_command.add_argument("--expected-revision", required=True, type=int)
    resolve_command.add_argument(
        "--resolution",
        required=True,
        choices=(
            MANUAL_RESOLUTION_ACCEPT_PRESERVED,
            MANUAL_RESOLUTION_VERIFIED_CLEAN,
        ),
    )
    resolve_command.add_argument("--confirmation", required=True)
    resolve_command.add_argument("--request-id", required=True)
    retry_qa_command = commands.add_parser("retry-qa-technical")
    retry_qa_command.add_argument("--job-id", required=True)
    retry_qa_command.add_argument("--expected-revision", required=True, type=int)
    retry_qa_command.add_argument("--stability-evidence-sha256", required=True)
    retry_qa_command.add_argument("--recovery-context-file", required=True)
    retry_qa_command.add_argument("--request-id", required=True)
    preview_retry_command = commands.add_parser("preview-qa-technical")
    preview_retry_command.add_argument("--job-id", required=True)
    preview_retry_command.add_argument("--expected-revision", required=True, type=int)
    for name in ("preview-profile", "revalidate-profile"):
        command = commands.add_parser(name)
        command.add_argument("--job-id", required=True)
        command.add_argument("--expected-revision", required=True, type=int)
        command.add_argument("--old-execution-context-file", required=True)
        command.add_argument("--classification-evidence-file", required=True)
        command.add_argument("--classification-evidence-sha256", required=True)
        if name == "revalidate-profile":
            command.add_argument("--reason", required=True)
            command.add_argument("--request-id", required=True)
    supersede = commands.add_parser("supersede-external-handoff")
    supersede.add_argument("--batch-id", required=True)
    supersede.add_argument("--expected-generation", required=True, type=int)
    supersede.add_argument("--expected-queue-revision", required=True, type=int)
    supersede.add_argument("--expected-job-revisions-file", required=True)
    supersede.add_argument("--external-handoff-ref", required=True)
    supersede.add_argument("--request-id", required=True)
    supersede.add_argument("--confirmation", required=True)
    adopt = commands.add_parser("adopt-external-baseline")
    adopt.add_argument("--batch-id", required=True)
    adopt.add_argument("--expected-generation", required=True, type=int)
    adopt.add_argument("--expected-queue-revision", required=True, type=int)
    adopt.add_argument("--expected-job-revisions-file", required=True)
    adopt.add_argument("--source-manifest", required=True)
    adopt.add_argument("--request-id", required=True)
    adopt.add_argument(
        "--confirmation",
        required=True,
        help=f"must be {EXTERNAL_BASELINE_ADOPTION_CONFIRMATION}",
    )
    validate_manifest = commands.add_parser("validate-batch-manifest")
    validate_manifest.add_argument("--batch-id", required=True)
    prepare_ab = commands.add_parser("prepare-ab-manifest")
    prepare_ab.add_argument("--manifest", required=True)
    prepare_ab.add_argument("--phase", required=True, choices=("A", "B"))
    notification = commands.add_parser("test-notification")
    notification.add_argument("--channel", required=True, choices=("telegram",))
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        control_root, task_root = validate_control_state_roots_read_only(
            args.control_root,
            args.task_root,
            state_base=HARNESS_ROOT,
        )
        if args.command == "test-notification":
            evidence = send_notify_evidence(
                "Harness 0.9 notification health test",
                title="Harness notification test",
                channel=args.channel,
            )
            payload = {
                "ok": bool(evidence["delivered"]),
                "result": (
                    "TELEGRAM_NOTIFICATION_OK"
                    if evidence["delivered"] else "TELEGRAM_NOTIFICATION_FAILED"
                ),
                **evidence,
            }
        elif args.command == "inspect":
            store = JobStore(control_root, read_only=True)
            payload = _public_inspection(store, args.job_id)
        elif args.command == "prepare-ab-manifest":
            payload = {"ok": True, **prepare_ab_only(args.manifest, phase=args.phase)}
        elif args.command == "validate-batch-manifest":
            store = JobStore(control_root, read_only=True)
            payload = {"ok": True, **store.validate_batch_manifest(args.batch_id)}
        elif args.command == "resolve":
            harness = HarnessService(
                args.working_dir,
                args.profile_dir,
                task_root=task_root,
            )
            job, replayed = LocalOperatorResolver(
                lambda: JobStore(control_root), harness
            ).resolve(
                args.job_id,
                resolution=args.resolution,
                confirmation=args.confirmation,
                expected_revision=args.expected_revision,
                request_id=args.request_id,
            )
            payload = _public_resolution(job, replayed)
        elif args.command in {"supersede-external-handoff", "adopt-external-baseline"}:
            revisions = json.loads(
                Path(args.expected_job_revisions_file).read_text(encoding="utf-8")
            )
            if not isinstance(revisions, dict):
                raise QueueError("EXPECTED_JOB_REVISIONS_INVALID")
            harness = HarnessService(
                args.working_dir,
                args.profile_dir,
                task_root=task_root,
            )
            resolver = LocalOperatorResolver(lambda: JobStore(control_root), harness)
            common = {
                "batch_id": args.batch_id,
                "expected_generation": args.expected_generation,
                "expected_queue_revision": args.expected_queue_revision,
                "expected_job_revisions": {
                    str(k): int(v) for k, v in revisions.items()
                },
                "request_id": args.request_id,
                "confirmation": args.confirmation,
            }
            if args.command == "adopt-external-baseline":
                result, replayed = resolver.adopt_external_baseline(
                    source_manifest_path=args.source_manifest, **common
                )
            else:
                result, replayed = resolver.supersede_batch_for_external_handoff(
                    external_handoff_ref=args.external_handoff_ref, **common
                )
            payload = {"ok": True, "replayed": replayed, **result}
        elif args.command in {"retry-qa-technical", "preview-qa-technical"}:
            control = ChatGPTControlService(
                args.working_dir,
                args.profile_dir,
                control_root=control_root,
                task_root=task_root,
                state_base=HARNESS_ROOT,
            )
            if args.command == "preview-qa-technical":
                payload = {
                    "ok": True,
                    **control.preview_awaiting_qa_technical(
                        args.job_id,
                        args.expected_revision,
                    ),
                }
            else:
                payload = {
                    "ok": True,
                    **control.retry_awaiting_qa_technical(
                    args.job_id,
                    args.expected_revision,
                    args.stability_evidence_sha256,
                    Path(args.recovery_context_file).read_text(encoding="utf-8"),
                    args.request_id,
                    start_immediately=False,
                    ),
                }
        else:
            old_context = json.loads(
                Path(args.old_execution_context_file).read_text(encoding="utf-8")
            )
            evidence = json.loads(
                Path(args.classification_evidence_file).read_text(encoding="utf-8")
            )
            control = ChatGPTControlService(
                args.working_dir,
                args.profile_dir,
                control_root=control_root,
                task_root=task_root,
                state_base=HARNESS_ROOT,
            )
            if args.command == "preview-profile":
                payload = {
                    "ok": True,
                    **control.preview_blocked_job_profile_revalidation(
                        args.job_id,
                        args.expected_revision,
                        old_context,
                        evidence,
                        args.classification_evidence_sha256,
                    ),
                }
            else:
                payload = {
                    "ok": True,
                    **control.revalidate_blocked_job_profile(
                        args.job_id,
                        args.expected_revision,
                        old_context,
                        evidence,
                        args.classification_evidence_sha256,
                        args.reason,
                        args.request_id,
                        start_immediately=False,
                    ),
                }
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return 0
    except (BatchManifestError, ControlPathError, HarnessServiceError, QueueError) as exc:
        code = getattr(exc, "code", "CONTROL_OPERATOR_ERROR")
        print(json.dumps({"ok": False, "error": {"code": code}}))
        return 2


if __name__ == "__main__":
    sys.exit(main())
