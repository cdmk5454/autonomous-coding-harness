"""
notify.py - Telegram 푸시 알림 (작업 상태 변화 안내)
- 전송: POST {KKM_TG_API_BASE}/bot<KKM_TG_BOT_TOKEN>/sendMessage (chat_id=KKM_TG_CHAT_ID)
- 22차: ntfy 완전 교체. 시그니처(send_notify/notify_task_event)와 fail-soft 계약 유지.
- 환경변수:
    KKM_TG_BOT_TOKEN   봇 토큰 (BotFather). 없으면 미전송
    KKM_TG_CHAT_ID     수신 chat id. 없으면 미전송
    KKM_TG_TITLE       제목 접두어 오버라이드 (기본 "KKM")
    KKM_TG_ENABLED     "0" 이면 알림 비활성
    KKM_TG_API_BASE    API 베이스 오버라이드 (기본 https://api.telegram.org, 테스트용)
- 전송 실패/네트워크 오류는 경고 로그만 남기고 False 반환.
  하네스의 작업 성공/실패 판정에는 절대 영향을 주지 않는다.
- 봇 토큰은 로그/예외 메시지에 노출하지 않는다.
"""

from __future__ import annotations

import json
import os
import urllib.request
from datetime import datetime
import hashlib
import time
from typing import TYPE_CHECKING
from runtime_safety import scrub_secrets, safe_print as print

if TYPE_CHECKING:
    from task_state import TaskState

DEFAULT_TITLE = "KKM"
DEFAULT_API_BASE = "https://api.telegram.org"
NOTIFY_TIMEOUT = 5.0  # 외부 API — 게이트 지연 방지용 짧은 타임아웃
MAX_TEXT_LEN = 4000   # Telegram 한계 4096 — 여유 두고 잘림


def notify_enabled() -> bool:
    """알림 활성 여부. KKM_TG_ENABLED=0 이면 False."""
    return os.environ.get("KKM_TG_ENABLED", "1").strip() != "0"


def _token() -> str:
    return (os.environ.get("KKM_TG_BOT_TOKEN") or "").strip()


def _chat_id() -> str:
    return (os.environ.get("KKM_TG_CHAT_ID") or "").strip()


def _api_base() -> str:
    return (os.environ.get("KKM_TG_API_BASE") or DEFAULT_API_BASE).rstrip("/")


def send_notify(
    message: str,
    title: str | None = None,
    priority: str | None = None,
) -> bool:
    """
    Telegram sendMessage 로 푸시 알림 전송.
    - title: 메시지 첫 줄에 합쳐진다 (Telegram 헤더 개념 없음)
    - priority: 시그니처 호환용. 전송에는 미반영 (ntfy 잔재)
    반환: 전송 성공 True. 비활성/미설정/실패는 False (예외 발생 없음).
    """
    return bool(_deliver_notify(message, title, priority)["delivered"])


def _deliver_notify(
    message: str,
    title: str | None,
    priority: str | None,
) -> dict[str, object]:
    attempted_at = datetime.now().astimezone().isoformat()
    seed = f"{attempted_at}:{title or ''}:{message}".encode("utf-8")
    evidence: dict[str, object] = {
        "notification_id": "NTF-" + hashlib.sha256(seed).hexdigest()[:20].upper(),
        "event_type": str(priority or "STATE_CHANGE").upper(),
        "channel": "telegram",
        "attempted_at": attempted_at,
        "configured": False,
        "attempted": False,
        "delivered": False,
        "delivery_status": "FAILED",
        "failure_code": "NOTIFICATION_NOT_CONFIGURED",
        "failure_origin": "NOTIFICATION",
        "http_status": None,
        "message_id": None,
        "latency_ms": None,
    }
    if not notify_enabled():
        evidence["failure_code"] = "NOTIFICATION_DISABLED"
        return evidence
    token = _token()
    chat_id = _chat_id()
    if not token or not chat_id:
        return evidence
    evidence["configured"] = True
    safe_title = scrub_secrets(title or "")
    safe_message = scrub_secrets(message)
    text = f"{safe_title}\n{safe_message}" if safe_title else safe_message
    if len(text) > MAX_TEXT_LEN:
        text = text[: MAX_TEXT_LEN - 20] + "\n…(잘림)"
    url = f"{_api_base()}/bot{token}/sendMessage"
    payload = json.dumps({"chat_id": chat_id, "text": text}).encode("utf-8")
    started = time.perf_counter()
    evidence["attempted"] = True
    try:
        req = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=NOTIFY_TIMEOUT) as resp:
            body = json.loads(resp.read().decode("utf-8", "replace") or "{}")
            evidence["http_status"] = int(resp.status)
            evidence["message_id"] = dict(body.get("result") or {}).get("message_id")
            delivered = 200 <= resp.status < 300 and bool(body.get("ok"))
            evidence["delivered"] = delivered
            evidence["delivery_status"] = "SUCCESS" if delivered else "FAILED"
            evidence["failure_code"] = "" if delivered else "TELEGRAM_API_REJECTED"
            evidence["failure_origin"] = "" if delivered else "NOTIFICATION"
    except Exception as e:  # 네트워크 오류 등 — 하네스 동작 영향 없음
        detail = scrub_secrets(str(e).replace(token, "***") if token else str(e))[:120]
        evidence["failure_code"] = f"TELEGRAM_{type(e).__name__.upper()}"
        print(f"[Notify] 전송 실패(무시됨): {type(e).__name__} {detail}")
    finally:
        evidence["latency_ms"] = int((time.perf_counter() - started) * 1000)
    return evidence


def send_notify_evidence(
    message: str,
    title: str | None = None,
    priority: str | None = None,
    *,
    channel: str = "telegram",
) -> dict[str, object]:
    """Return secret-free delivery evidence without changing Job success."""
    evidence = _deliver_notify(message, title, priority)
    evidence["channel"] = str(channel)
    return evidence


def terminal_notification_projection(job, queue):
    from operator_notifications import terminal_projection
    return terminal_projection(job, queue)


def _task_name(task):
    from operator_notifications import operator_label
    return operator_label({'operator_label': getattr(task, 'operator_label', ''),
                           'client_job_id': task.client_job_id, 'job_id': task.job_id, 'task_id': task.task_id})


def _stage_name(stage: str) -> str:
    return {
        "WORKER": "작업 실행",
        "GIT": "변경 수집",
        "BUILD": "빌드",
        "TEST": "테스트",
        "REVIEW": "리뷰",
    }.get(stage, "작업")


def _event_title(label: str) -> str:
    base = os.environ.get("KKM_TG_TITLE") or DEFAULT_TITLE
    return f"{base} · {label}"


def notify_task_event(
    event: str,
    task: "TaskState",
    detail: str = "",
    attempt: int | None = None,
    max_attempts: int = 3,
) -> bool:
    """
    작업 상태 이벤트 알림 (manager 상태 전환 지점용 포맷터).
    event:
      - "start"   : 작업 시도 시작
      - "review_start": 리뷰 시작
      - "success" : 작업 성공
      - "fail"    : 시도 실패 후 재시도 안내
      - "final"   : 최대 재시도 소진
    실패 계열은 priority=high (포맷 호환 유지).
    detail 은 구 호출부 호환을 위해 유지하되 사용자 메시지에는 노출하지 않는다.
    """
    current = attempt or getattr(task, "retry_count", 0) + 1
    total = max(1, max_attempts)
    name = _task_name(task)

    if event == "start":
        modules = ", ".join(task.target_module) if task.target_module else "전체"
        return send_notify(
            f"{name}\n대상: {modules}",
            title=_event_title(f"작업 중 {current}/{total}"),
        )
    if event == "review_start":
        return send_notify(
            name,
            title=_event_title(f"리뷰 시작 {current}/{total}"),
        )
    if event == "success":
        return send_notify(
            f"{name}\n변경 파일: {len(task.changed_files)}개",
            title=_event_title("작업 완료"),
        )
    if event == "fail":
        stage = _stage_name(task.failure_stage)
        return send_notify(
            f"{name}\n보완 후 다시 작업합니다."
            f"\n다음 작업: {min(current + 1, total)}/{total}",
            title=_event_title(f"{stage} 보완 필요"),
            priority="high",
        )
    if event == "final":
        stage = _stage_name(task.failure_stage)
        return send_notify(
            f"{name}\n{stage} 단계에서 {total}회 시도 후 종료했습니다.",
            title=_event_title("작업 실패"),
            priority="high",
        )
    return False
