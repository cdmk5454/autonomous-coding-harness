"""Canonical durable progress read model shared by local control and Telegram."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Sequence
from operator_notifications import operator_label, action_for, render_compact


PROGRESS_SCHEMA_VERSION = 1
HEARTBEAT_SECONDS = 30 * 60
RUNTIME_START_GRACE_SECONDS = 60
SOURCE_STAGE_PLAN = ("WORKER", "GIT", "BUILD", "TEST", "REVIEW", "VERIFY")
CONTRACT_STAGE_PLAN = ("ANALYZE", "CONTRACT", "REVIEW", "VERIFY")
REVIEW_RECOVERY_STAGE_PLAN = ("REVIEW", "VERIFY")


def _iso_now(clock: Callable[[], datetime] | None = None) -> str:
    value = (clock or (lambda: datetime.now(timezone.utc)))()
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone().isoformat()


def _parse_time(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value)
        # Legacy TaskState timestamps were local-naive.  Interpret them using
        # the host timezone rather than failing when 0.9 emits aware timestamps.
        return parsed.astimezone() if parsed.tzinfo is None else parsed
    except (TypeError, ValueError):
        return None


def project_execution_health(
    task: Any,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Project subprocess ownership and meaningful progress separately.

    The existing Worker/Reviewer wrapper owns the real process handle.  Its
    durable lease is canonical; Task.status alone is never liveness evidence.
    This function is read-only so a control poll can detect lease expiry even
    after the process that wrote the last heartbeat has disappeared.
    """
    current = dict(getattr(task, "execution_runtime", {}) or {})
    task_status = str(getattr(task, "status", ""))
    stage = str(getattr(task, "stage", "")).upper()
    snapshot_stage = str(
        dict(dict(getattr(task, "progress_snapshot", {}) or {}).get("stage") or {}).get(
            "name", ""
        )
    ).upper()
    external_stage = stage in {"WORKING", "WORKER", "REVIEW"} or snapshot_stage in {
        "WORKER", "REVIEW"
    }
    now_dt = now or datetime.now(timezone.utc)
    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=timezone.utc)
    role = str(current.get("execution_role", ""))
    result: dict[str, Any] = {
        "runtime_health": "NOT_RUNNING",
        "progress_health": "NOT_RUNNING",
        "execution_id": str(current.get("execution_id", "")),
        "execution_owner": str(current.get("execution_owner", "")),
        "execution_role": role,
        "pid": int(current.get("pid", 0) or 0),
        "last_runtime_heartbeat_at": str(current.get("last_runtime_heartbeat_at", "")),
        "lease_expires_at": str(current.get("lease_expires_at", "")),
        "last_progress_at": str(current.get("last_progress_at", "")),
        "process_alive_observed": current.get("process_alive"),
        "failure_code": "",
        "reconciliation_required": False,
    }
    if task_status != "RUNNING":
        return result
    if not external_stage:
        result.update(runtime_health="NOT_APPLICABLE", progress_health="RECENT")
        return result
    if not current.get("execution_id"):
        event_at = _parse_time(str(
            dict(dict(getattr(task, "progress_snapshot", {}) or {}).get(
                "last_meaningful_event"
            ) or {}).get("at", "")
        ))
        if event_at and (now_dt - event_at).total_seconds() <= RUNTIME_START_GRACE_SECONDS:
            result.update(runtime_health="STARTING", progress_health="PENDING")
            return result
        result.update(
            runtime_health="OWNER_MISSING",
            progress_health="UNKNOWN",
            failure_code=f"{role or 'EXECUTION'}_RUNTIME_OWNER_MISSING",
            reconciliation_required=True,
        )
        return result
    if current.get("ended_at"):
        ended = _parse_time(str(current.get("ended_at", "")))
        if ended and (now_dt - ended).total_seconds() <= RUNTIME_START_GRACE_SECONDS:
            result.update(
                runtime_health="STAGE_TRANSITION_PENDING",
                progress_health="TERMINAL",
            )
            return result
        result.update(
            runtime_health="OWNER_ENDED_WITH_RUNNING_STATE",
            progress_health="TERMINAL",
            failure_code=f"{role or 'EXECUTION'}_RUNTIME_OWNER_ENDED",
            reconciliation_required=True,
        )
        return result
    lease = _parse_time(str(current.get("lease_expires_at", "")))
    if lease is None:
        result.update(
            runtime_health="LEASE_UNKNOWN",
            progress_health="UNKNOWN",
            failure_code=f"{role or 'EXECUTION'}_RUNTIME_LEASE_UNKNOWN",
            reconciliation_required=True,
        )
        return result
    if now_dt > lease:
        result.update(
            runtime_health="LEASE_EXPIRED",
            progress_health="UNKNOWN",
            failure_code=f"{role or 'EXECUTION'}_RUNTIME_LEASE_EXPIRED",
            reconciliation_required=True,
        )
        return result
    progress = _parse_time(str(current.get("last_progress_at", "")))
    timeout_seconds = max(1, int(current.get("progress_timeout_seconds", 900) or 900))
    if progress is None:
        progress_health = "UNKNOWN"
    elif (now_dt - progress).total_seconds() >= timeout_seconds:
        progress_health = "STALLED"
    else:
        progress_health = "RECENT"
    result.update(runtime_health="VALID", progress_health=progress_health)
    if progress_health == "STALLED":
        result["failure_code"] = f"{role or 'EXECUTION'}_PROGRESS_TIMEOUT"
    return result


def stage_plan_for(task: Any) -> tuple[str, ...]:
    kind = str(getattr(task, "progress_plan_kind", "SOURCE_JOB"))
    if kind == "CONTRACT_JOB":
        return CONTRACT_STAGE_PLAN
    if kind == "REVIEW_RECOVERY":
        return REVIEW_RECOVERY_STAGE_PLAN
    return SOURCE_STAGE_PLAN


def meaningful_progress_update(
    task: Any,
    *,
    stage: str,
    event_type: str,
    summary: str,
    clock: Callable[[], datetime] | None = None,
    stage_plan: Sequence[str] | None = None,
) -> dict[str, Any]:
    plan = tuple(stage_plan or stage_plan_for(task))
    normalized = str(stage or "").upper()
    stage_name = "WORKER" if normalized == "WORKING" else normalized
    if stage_name == "DONE":
        stage_name = "VERIFY"
    try:
        index = plan.index(stage_name) + 1
    except ValueError:
        index = 0
    now = _iso_now(clock)
    previous = dict(getattr(task, "progress_snapshot", {}) or {})
    semantic = {
        "stage": stage_name,
        "event_type": str(event_type),
        "summary": str(summary),
        "attempt": int(getattr(task, "retry_count", 0)) + 1,
        "attempt_limit": int(getattr(task, "progress_attempt_limit", 0) or 0),
        "reviewer_attempt": int(getattr(task, "reviewer_recovery_count", 0)) + 1,
    }
    prior_semantic = dict(previous.get("semantic_key") or {})
    revision = int(getattr(task, "progress_revision", 0))
    if semantic != prior_semantic:
        revision += 1
    started = str(getattr(task, "started_at", "") or getattr(task, "created_at", ""))
    started_dt = _parse_time(started)
    now_dt = _parse_time(now)
    elapsed = max(0, int((now_dt - started_dt).total_seconds())) if started_dt and now_dt else 0
    gates = {
        "worker": "PASS" if stage_name not in {"WORKER", ""} else "RUNNING",
        "build": str(dict(getattr(task, "build", {}) or {}).get("status", "PENDING")),
        "test": str(getattr(task, "test_status", "PENDING")),
        "review": str(getattr(task, "review_status", "PENDING")),
        "verification": str(getattr(task, "verification_status", "PENDING")),
    }
    snapshot = {
        "schema_version": PROGRESS_SCHEMA_VERSION,
        "progress_revision": revision,
        "job": {
            "id": str(getattr(task, "job_id", "")),
            "client_job_id": str(getattr(task, "client_job_id", "")),
            "display_name": operator_label({"operator_label": getattr(task, "operator_label", ""),
                "client_job_id": getattr(task, "client_job_id", ""), "task_id": getattr(task, "task_id", "")}),
            "operator_label": getattr(task, "operator_label", ""),
            "status": str(getattr(task, "status", "")),
            "task_id": str(getattr(task, "task_id", "")),
            "elapsed_seconds": elapsed,
        },
        "stage": {
            "name": stage_name,
            "index": index,
            "total": len(plan),
            "attempt": semantic["attempt"],
            "attempt_limit": semantic["attempt_limit"],
            "reviewer_attempt": int(getattr(task, "reviewer_recovery_count", 0)) + 1,
            "reviewer_attempt_limit": int(
                getattr(task, "reviewer_recovery_limit", 1)
            ) + 1,
        },
        "gates": gates,
        "last_meaningful_event": {
            "type": str(event_type),
            "at": now,
            "summary": str(summary),
        },
        "next": {
            "stage": plan[index] if index and index < len(plan) else "",
            "job": "",
        },
        "product_readiness": str(getattr(task, "product_readiness", "FINALIZATION_PENDING")),
        "open_decision_count": int(getattr(task, "open_decision_count", 0)),
        "open_decisions": list(getattr(task, "open_decisions", []) or []),
        "user_action_required": bool(
            getattr(task, "user_action_required", False)
            or str(getattr(task, "status", "")) == "AWAITING_QA"
        ),
        "semantic_key": semantic,
    }
    snapshot["execution_health"] = project_execution_health(task)
    task.progress_revision = revision
    task.progress_snapshot = snapshot
    events = list(getattr(task, "progress_events", []) or [])
    if semantic != prior_semantic:
        events.append({
            "progress_revision": revision,
            # 0.99.1 stage-timing: durable stage-enter marker for the
            # execution-summary timing breakdown.
            "stage": stage_name,
            **snapshot["last_meaningful_event"],
        })
        task.progress_events = events
    return snapshot


def queue_progress_snapshot(
    queue: Mapping[str, Any],
    jobs: Sequence[Mapping[str, Any]],
    task_snapshot: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    result = dict(task_snapshot or {})
    current_batch_id = str(queue.get("current_batch_id", ""))
    candidate_ids = (
        str(queue.get("running_job_id", "")),
        str(queue.get("blocked_by_job_id", "")),
    )
    if not current_batch_id:
        current_batch_id = next(
            (
                str(job.get("batch_id", ""))
                for candidate_id in candidate_ids
                for job in jobs
                if candidate_id and job.get("job_id") == candidate_id
            ),
            "",
        )
    if not current_batch_id:
        current_generation = int(queue.get("generation", 0) or 0)
        current_batch_id = next(
            (
                str(job.get("batch_id", ""))
                for job in jobs
                if job.get("status") == "QUEUED"
                and (
                    not current_generation
                    or int(job.get("generation", current_generation) or current_generation)
                    == current_generation
                )
            ),
            "",
        )
    batch_jobs = [
        job for job in jobs
        if current_batch_id and str(job.get("batch_id", "")) == current_batch_id
    ]
    active = next(
        (
            job for candidate_id in candidate_ids for job in batch_jobs
            if candidate_id and job.get("job_id") == candidate_id
        ),
        None,
    )
    if active is None:
        active = next(
            (job for job in batch_jobs if job.get("status") == "RUNNING"), None
        )
    if active is None:
        active = next(
            (
                job for job in batch_jobs
                if job.get("status") in {
                    "AWAITING_QA", "AWAITING_DEPENDENCY_QA", "QUARANTINED"
                }
            ),
            None,
        )
    if active is None:
        active = next(
            (job for job in batch_jobs if job.get("status") == "QUEUED"), None
        )
    completed = sum(1 for job in batch_jobs if job.get("status") == "SUCCEEDED")
    qa_jobs = [
        job for job in batch_jobs
        if job.get("status") in {
            "AWAITING_QA", "AWAITING_DEPENDENCY_QA", "QUARANTINED"
        }
    ]
    quarantined_jobs = [
        job for job in batch_jobs
        if job.get("status") == "QUARANTINED"
        or bool(job.get("candidate_id"))
        or bool(job.get("speculative_candidate_ids"))
    ]
    blocked_jobs = [
        job for job in batch_jobs
        if job.get("status") in {
            "BLOCKED", "BLOCKED_BY_DEPENDENCY", "AWAITING_DEPENDENCY_QA"
        }
    ]
    categorized_ids = {
        str(job.get("job_id", ""))
        for job in batch_jobs
        if job.get("status") == "SUCCEEDED"
    } | {
        str(job.get("job_id", ""))
        for job in qa_jobs + quarantined_jobs + blocked_jobs
    }
    current = int(active.get("sequence", 0)) if active else 0
    result.setdefault("schema_version", PROGRESS_SCHEMA_VERSION)
    result.setdefault("progress_revision", 0)
    result["batch"] = {
        "id": current_batch_id,
        "label": str(active.get("batch_label", "")) if active else "",
        "current": current,
        "total": len(batch_jobs),
        "completed": completed,
        "qa": len(qa_jobs),
        "quarantined": len(quarantined_jobs),
        "blocked": len(blocked_jobs),
        "other": sum(
            1 for job in batch_jobs
            if str(job.get("job_id", "")) not in categorized_ids
        ),
    }
    if active:
        result.setdefault("job", {})
        result["job"].setdefault("id", str(active.get("job_id", "")))
        result["job"].setdefault(
            "client_job_id", str(active.get("client_job_id", ""))
        )
        result["job"]["status"] = str(active.get("status", ""))
        result["job"]["display_name"] = operator_label(active)
        result["job"]["operator_label"] = operator_label(active)
        result["job"].setdefault("task_id", str(active.get("current_claim_task_id", "")))
        result["job"].setdefault("elapsed_seconds", 0)
        terminal = dict(active.get("last_result") or {})
        if active.get("status") != "RUNNING":
            result.pop("gates", None)
            result["stage"] = {"name": terminal.get("failure_stage") or terminal.get("stage") or active.get("status")}
        result.setdefault("gates", {
            "worker": "PASS" if terminal.get("success") is True else "PENDING",
            "build": str(terminal.get("build_status", "PENDING")),
            "test": str(terminal.get("test_status", "PENDING")),
            "review": str(terminal.get("review_status", "PENDING")),
            "verification": str(terminal.get("verification_status", "PENDING")),
        })
        if not dict(result.get("stage") or {}).get("name"):
            status = str(active.get("status", ""))
            result["stage"] = {
                "name": "QA" if status in {
                    "AWAITING_QA", "AWAITING_DEPENDENCY_QA", "QUARANTINED"
                } else status,
            }
        if not dict(result.get("last_meaningful_event") or {}).get("summary"):
            result["last_meaningful_event"] = {
                "summary": str(terminal.get("failure_code", "")) or str(active.get("status", ""))
            }
        result.setdefault("next", {
            "stage": "USER_QA" if str(active.get("status", "")) in {
                "AWAITING_QA", "AWAITING_DEPENDENCY_QA", "QUARANTINED"
            } else "",
            "job": "",
        })
    result["queue"] = {
        "execution_mode": str(queue.get("execution_mode", "")),
        "paused": bool(queue.get("paused")),
        "pause_reason": str(queue.get("pause_reason", "")),
        "gate_reason": str(queue.get("gate_reason", "")),
        "running_job_id": str(queue.get("running_job_id", "")),
    }
    terminal = dict(active.get("last_result") or {}) if active else {}
    result["product_readiness"] = str(
        terminal.get("product_readiness") or (
            "COMPLETE" if active and active.get("status") == "SUCCEEDED" else
            "SKELETON_READY" if active and active.get("status") == "SKELETON_READY" else
            "FINALIZATION_PENDING"
        )
    )
    result["open_decisions"] = list(terminal.get("open_decisions") or [])
    result["open_decision_count"] = int(
        terminal.get("open_decision_count", len(result["open_decisions"]))
    )
    result["user_action_required"] = bool(
        result["open_decision_count"] or (action_for(active, queue) if active else False)
    )
    result["queue_revision"] = int(queue.get("revision", 0))
    return result


def should_emit(
    previous: Mapping[str, Any] | None,
    current: Mapping[str, Any],
    *,
    now: datetime,
    last_emitted_at: datetime | None,
    heartbeat_seconds: int = HEARTBEAT_SECONDS,
) -> tuple[bool, str]:
    if not previous or previous.get("progress_revision") != current.get("progress_revision"):
        return True, "STATE_CHANGED"
    health_keys = (
        "runtime_health", "progress_health", "failure_code",
        "reconciliation_required",
    )
    old_health = dict(previous.get("execution_health") or {})
    new_health = dict(current.get("execution_health") or {})
    if any(old_health.get(key) != new_health.get(key) for key in health_keys):
        return True, "EXECUTION_HEALTH_CHANGED"
    if (dict(current.get("job") or {}).get("status") == "RUNNING"
            and new_health.get("runtime_health") == "VALID"
            and last_emitted_at and (now - last_emitted_at).total_seconds() >= heartbeat_seconds):
        return True, "HEARTBEAT"
    return False, "SILENT"


def render_progress(snapshot: Mapping[str, Any], view: str = "USER") -> str:
    if view.upper() in {"TELEGRAM", "HEARTBEAT"}:
        return render_compact(snapshot, heartbeat=view.upper() == "HEARTBEAT")
    harness = dict(snapshot.get("harness") or {})
    batch = dict(snapshot.get("batch") or {})
    job = dict(snapshot.get("job") or {})
    stage = dict(snapshot.get("stage") or {})
    gates = dict(snapshot.get("gates") or {})
    event = dict(snapshot.get("last_meaningful_event") or {})
    next_item = dict(snapshot.get("next") or {})
    queue = dict(snapshot.get("queue") or {})
    health = dict(snapshot.get("execution_health") or {})
    runtime_health = str(health.get("runtime_health", ""))
    progress_health = str(health.get("progress_health", ""))
    progress_at = _parse_time(str(health.get("last_progress_at", "")))
    progress_age = ""
    if progress_at is not None:
        progress_age = str(max(0, int((datetime.now(timezone.utc) - progress_at).total_seconds()) // 60))
    action_required = bool(snapshot.get("user_action_required"))
    profile = str(harness.get("profile_id", "SAMPLE_PROFILE") or "SAMPLE_PROFILE").upper()
    version = str(harness.get("version", ""))
    lines: list[str] = []
    if version:
        lines.extend((
            f"{'🟡' if action_required or queue.get('paused') else '🟢'} {profile} Harness {version}",
            "",
        ))
    if batch:
        lines.append(
            f"{batch.get('label') or batch.get('id') or 'Batch'} · "
            f"{batch.get('current', 0)}/{batch.get('total', 0)}"
        )
    gate_reason = str(queue.get("gate_reason", ""))
    state_label = {
        "AWAITING_QA": "QA 대기",
        "AWAITING_DEPENDENCY_QA": "의존성 QA 대기",
        "USER_E2E_REQUIRED": "사용자 E2E 대기",
    }.get(gate_reason, "일시정지" if queue.get("paused") else "실행 중" if queue.get("running_job_id") else "대기")
    if queue:
        lines.extend((
            "",
            f"현재 상태: {state_label}",
            f"실행 중 Job: {queue.get('running_job_id') or '없음'}",
        ))
    if batch:
        lines.extend((
            "",
            "진행 현황",
            f"✅ 완료: {batch.get('completed', 0)}",
            f"🧑 QA 대기: {batch.get('qa', 0)}",
            f"🧊 격리: {batch.get('quarantined', 0)}",
            f"⛔ 차단: {batch.get('blocked', 0)}",
            f"⏸ 기타: {batch.get('other', 0)}",
        ))
    job_name = str(job.get("client_job_id") or job.get("id") or "")
    if job_name:
        lines.extend(("", job_name))
        display_name = str(job.get("display_name", ""))
        if display_name:
            lines.append(display_name)
    if gates:
        def marker(value: Any) -> str:
            normalized = str(value or "").upper()
            if normalized in {"PASS", "REVIEW_PASS", "VERIFIED", "NOT_REQUIRED"}:
                return "✓"
            if normalized in {"RUNNING", "IN_PROGRESS", "WORKING"}:
                return "●"
            if normalized in {"FAIL", "FAILED", "REVIEW_FAIL", "REJECTED"}:
                return "✗"
            return "○"
        lines.append(" ".join((
            f"[{marker(gates.get('worker'))} WORKER]",
            f"[{marker(gates.get('build'))} BUILD]",
            f"[{marker(gates.get('test'))} TEST]",
            f"[{marker(gates.get('review'))} REVIEW]",
            f"[{marker(gates.get('verification'))} VERIFY]",
        )))
    stage_name = str(stage.get("name", "")).upper()
    if stage_name == "REVIEW":
        current_label = (
            f"Reviewer {int(stage.get('reviewer_attempt', stage.get('attempt', 1)) or 1)}/"
            f"{int(stage.get('reviewer_attempt_limit', stage.get('attempt_limit', 0)) or 0)}"
        )
    else:
        current_label = {
            "VERIFY": "Verification", "USER_QA": "사용자 QA", "QA": "QA",
        }.get(stage_name, stage_name.title() if stage_name else "-")
    if job_name or stage_name:
        lines.extend((
            f"현재: {current_label}",
            f"경과: {int(job.get('elapsed_seconds', 0)) // 60}분",
        ))
    if event.get("summary"):
        lines.append(f"마지막 변화: {event.get('summary', '')}")
    next_stage = str(next_item.get("stage", "")).upper()
    next_label = {
        "VERIFY": "Verification", "USER_QA": "사용자 QA",
    }.get(next_stage, next_stage.title() if next_stage else "-")
    if job_name or next_stage:
        lines.append(f"다음: {next_label}")
    if queue:
        lines.extend((
            "",
            f"실행 모드: {queue.get('execution_mode') or '-'} · "
            f"{'PAUSED' if queue.get('paused') else 'RUNNING'}",
        ))
    lines.append(f"사용자 개입: {'필요' if action_required else '필요 없음'}")
    if action_required:
        lines.append(
            "필요한 다음 행동: "
            + ({
                "AWAITING_QA": "QA 결과 확인",
                "AWAITING_DEPENDENCY_QA": "의존성 QA 결과 확인",
                "USER_E2E_REQUIRED": "사용자 E2E 수행",
            }.get(gate_reason, "상태 확인"))
        )
    if runtime_health == "VALID" and progress_health == "RECENT":
        lines.append(f"실행 상태: 실행 중 · 최근 진척 {progress_age or '0'}분 전")
    elif runtime_health == "VALID" and progress_health == "STALLED":
        lines.append("실행 상태: process는 살아 있으나 의미 있는 진척이 없어 timeout/recovery 판단 중")
    elif runtime_health in {
        "LEASE_EXPIRED", "LEASE_UNKNOWN", "OWNER_MISSING",
        "OWNER_ENDED_WITH_RUNNING_STATE",
    }:
        lines.append("실행 상태: persisted RUNNING과 runtime 불일치 감지 · reconciliation 필요")
    elif runtime_health == "STARTING":
        lines.append("실행 상태: execution owner 시작 중")
    if view.upper() in {"OPERATOR", "DEBUG"}:
        lines.extend((
            f"Job/Task: {job.get('id', '')}/{job.get('task_id', '')}",
            f"progress_revision: {snapshot.get('progress_revision', 0)}",
            f"queue_revision: {snapshot.get('queue_revision', 0)}",
            f"runtime/progress health: {runtime_health}/{progress_health}",
            f"execution: {health.get('execution_id', '')} role={health.get('execution_role', '')} pid={health.get('pid', 0)}",
            f"owner: {health.get('execution_owner', '')}",
            f"runtime heartbeat/lease: {health.get('last_runtime_heartbeat_at', '')}/{health.get('lease_expires_at', '')}",
            f"last progress: {health.get('last_progress_at', '')}",
            f"runtime failure: {health.get('failure_code', '')}",
        ))
    if view.upper() == "DEBUG":
        lines.append(f"semantic_key: {snapshot.get('semantic_key', {})}")
    return "\n".join(lines)
