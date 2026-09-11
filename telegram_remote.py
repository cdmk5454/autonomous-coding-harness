"""
telegram_remote.py - 하네스 실행기 텔레그램 원격 제어 (22차)
- run.py 와 같은 프로세스에서 Manager 인스턴스를 공유하며 백그라운드 폴링 스레드로 동작.
- Job 은 절대 워커(droid/codex/opencode)로 직행하지 않는다: 제출은 항상 on_submit 콜백(→ Manager.submit_task)
  을 경유한다. 슬롯/모듈 범위 조율·게이트(fail-closed 리뷰, RISK 재시도, 롤백)는 기존 파이프라인 유지.
- 명령 (화이트리스트 사용자 전용):
    /help              명령 도움말
    /status            실행 중 Job 목록/상태 + 리포트 경로 안내
    /submit            작업 제출. 이후 여러 줄 요구사항 → 새 줄에 END
                      (첫 줄에 옵션: worker=droid|codex|opencode, model=..., effort=low|medium|high,
                       modules=api,web — 콤마/스페이스 구분, 대소문자 무시, 순서 무관)
    /report <task_id>  완료 리포트(.tasks/YYYYMMDD/{task_id}.md) 전송
    /shutdown          graceful 종료: 신규 제출 중지 → 진행 Job 완료 대기 → manager.shutdown
- 보안:
    - 허용 Telegram user id 화이트리스트(KKM_TG_ALLOWED_USER_IDS, 콤마 구분) 필수.
      목록이 비어 있으면 어떤 명령도 거부한다(fail-closed).
    - 봇 토큰은 env(KKM_TG_BOT_TOKEN)만. 로그·응답에 토큰 미노출.
    - 콜백은 메인 루프 스레드(run.py)에서만 실행 — 제출 futures 레이스 방지.
- 폴링: getUpdates 롱폴링(timeout 25s). 오류 시 지수 백오프(최대 300s).
"""

from __future__ import annotations

import json
import os
import threading
import time
import urllib.request
from pathlib import Path
from typing import Any, Callable

from task_state import TaskState
from progress_projection import render_progress
from runtime_safety import scrub_secrets, safe_print as print

DEFAULT_API_BASE = "https://api.telegram.org"
POLL_TIMEOUT = 25          # getUpdates long poll (초)
BACKOFF_MAX = 300.0        # 오류 백오프 상한(초)
POLL_TIMEOUT_HTTP = POLL_TIMEOUT + 10   # 실제 HTTP 소켓 타임아웃

_OPTION_KEYS = ("worker", "model", "effort", "modules")
_VALID_WORKERS = ("droid", "codex", "opencode")
_VALID_EFFORTS = ("low", "medium", "high")


def allowed_ids_from_env() -> list[int]:
    """KKM_TG_ALLOWED_USER_IDS="123,456" → [123, 456]. 비정상 토큰은 무시."""
    raw = os.environ.get("KKM_TG_ALLOWED_USER_IDS", "")
    ids: list[int] = []
    for token in raw.replace(";", ",").split(","):
        token = token.strip()
        if token.isdigit():
            ids.append(int(token))
    return ids


def _lifecycle_chat_ids() -> list[int]:
    ids = list(allowed_ids_from_env())
    extra = (os.environ.get("KKM_TG_CHAT_ID") or "").strip()
    if extra.lstrip("-").isdigit():
        ids.append(int(extra))
    seen: set[int] = set()
    unique: list[int] = []
    for chat_id in ids:
        if chat_id in seen:
            continue
        seen.add(chat_id)
        unique.append(chat_id)
    return unique


def notify_job_lifecycle(event: str, task: TaskState) -> None:
    """MCP/Manager Job 시작·종료만 알린다. 진행 로그 없음. 실패해도 Job을 죽이지 않는다."""
    try:
        enabled = (os.environ.get("KKM_TG_ENABLED") or "1").strip().lower()
        if enabled in {"0", "false", "no", "off"}:
            return
        token = (os.environ.get("KKM_TG_BOT_TOKEN") or "").strip()
        chat_ids = _lifecycle_chat_ids()
        if not token or not chat_ids:
            return
        if event not in {"start", "end"}:
            return
        requirement = scrub_secrets(getattr(task, "requirement", "") or "").strip()
        if len(requirement) > 120:
            requirement = requirement[:117] + "..."
        worker = str(getattr(task, "worker", "") or "")
        model = str(
            getattr(task, "droid_model", "")
            or getattr(task, "codex_model", "")
            or ""
        )
        if event == "start":
            text = (
                f"[Job 시작] {task.task_id}\n"
                f"{worker} {model}\n"
                f"{requirement}"
            ).strip()
        else:
            status = str(getattr(task, "status", "") or "")
            code = str(getattr(task, "failure_code", "") or "")
            text = (
                f"[Job 종료] {task.task_id}\n"
                f"{status}"
                + (f" {code}" if code else "")
            )
        api_base = (
            os.environ.get("KKM_TG_API_BASE") or DEFAULT_API_BASE
        ).rstrip("/")
        for chat_id in chat_ids:
            body = json.dumps(
                {"chat_id": chat_id, "text": text[:4000]},
                ensure_ascii=False,
            ).encode("utf-8")
            req = urllib.request.Request(
                f"{api_base}/bot{token}/sendMessage",
                data=body,
                headers={"Content-Type": "application/json; charset=utf-8"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                resp.read()
    except Exception:
        return


def parse_submit_options(first_line: str) -> tuple[dict, list[str]]:
    """/submit 첫 줄의 key=value 옵션 파싱. (options, errors) 반환.
    - worker=droid|codex|opencode (기본 droid)
    - model=<model> (Codex registry id or explicit OpenCode provider/model)
    - effort=low|medium|high (기본 medium)
    - modules=api,web (콤마/스페이스 구분)
    옵션이 하나도 없으면 빈 dict — 첫 줄이 곧 요구사항의 첫 줄로 간주된다.
    """
    options: dict = {}
    errors: list[str] = []
    lowered = first_line.lower()
    has_option = any(f"{key}=" in lowered for key in _OPTION_KEYS)
    if not has_option:
        return options, errors

    import re as _re
    # modules=api,web / modules=api web 모두 지원:
    # 값은 "공백+다른옵션key=" 또는 줄 끝까지.
    _next = r"(?=\s+[a-z]+=|$)"
    for key in _OPTION_KEYS:
        m = _re.search(
            rf"(?:^|[\s,]){key}=([^\n]*?){_next}",
            first_line,
            _re.IGNORECASE,
        )
        if not m:
            continue
        value = m.group(1).strip().rstrip(",").strip()
        if key == "worker":
            v = value.lower()
            if v in _VALID_WORKERS:
                options["worker"] = v
            else:
                errors.append(f"worker는 droid|codex|opencode (받은 것: {value})")
        elif key == "effort":
            v = value.lower()
            if v in _VALID_EFFORTS:
                options["effort"] = v
            else:
                errors.append(f"effort는 low|medium|high (받은 것: {value})")
        elif key == "model":
            if value:
                options["model"] = value
        elif key == "modules":
            mods = [
                m2 for m2 in _re.split(r"[,\s]+", value) if m2
            ]
            if mods:
                options["modules"] = mods
    return options, errors


def _scrub(text: str) -> str:
    """예외 메시지 등에 토큰이 섞이는 것을 방지한다."""
    token = os.environ.get("KKM_TG_BOT_TOKEN") or ""
    return text.replace(token, "***") if token else text


class RemoteCommand:
    """폴링 스레드 → 메인 스레드로 전달되는 명령."""

    __slots__ = ("chat_id", "user_id", "name", "args", "text", "options")

    def __init__(
        self,
        chat_id: int,
        user_id: int,
        name: str,
        args: str,
        text: str,
        options: dict | None = None,
    ):
        self.chat_id = chat_id
        self.user_id = user_id
        self.name = name
        self.args = args
        self.text = text
        self.options = options


class TelegramRemote:
    """텔레그램 롱폴링 원격 제어. run.py 메인 루프가 poll_commands() 로 소비한다."""

    def __init__(
        self,
        on_submit: Callable[[str, dict, int], None],
        on_shutdown: Callable[[], None],
        status_provider: Callable[[], list[dict]],
        allowed_user_ids: list[int] | None = None,
        api_base: str | None = None,
        poll_timeout: int = POLL_TIMEOUT,
        task_root: str | Path = ".tasks",
    ):
        """
        on_submit(requirement, options, chat_id): 메인 스레드에서 Manager.submit_task 경유 제출.
        on_shutdown(): 메인 스레드에서 graceful 종료 플래그 설정.
        status_provider(): [{task_id, status, stage, worker_tag, requirement}] 반환.
        """
        self._on_submit = on_submit
        self._on_shutdown = on_shutdown
        self._status_provider = status_provider or (lambda: [])
        self._allowed = set(
            allowed_user_ids if allowed_user_ids is not None
            else allowed_ids_from_env()
        )
        self._api_base = (api_base or os.environ.get("KKM_TG_API_BASE")
                          or DEFAULT_API_BASE).rstrip("/")
        self._poll_timeout = poll_timeout
        self._task_root = Path(task_root)
        self._offset = 0
        self._thread: threading.Thread | None = None
        self._stop_flag = threading.Event()
        self._queue: list[RemoteCommand] = []
        self._queue_lock = threading.Lock()
        # chat_id → {"lines": [...], "options": {...}} (/submit 다중 줄 수집 상태)
        self._pending_submits: dict[int, dict] = {}

    # ------------------------------------------------------------------
    # 설정/권한
    # ------------------------------------------------------------------
    @property
    def allowed_user_ids(self) -> set[int]:
        return set(self._allowed)

    def is_allowed(self, user_id: int) -> bool:
        """화이트리스트 검증. 비어 있으면 무조건 거부(fail-closed)."""
        return bool(self._allowed) and user_id in self._allowed

    # ------------------------------------------------------------------
    # Bot API 저수준
    # ------------------------------------------------------------------
    def _api_url(self, method: str) -> str:
        token = (os.environ.get("KKM_TG_BOT_TOKEN") or "").strip()
        return f"{self._api_base}/bot{token}/{method}"

    def _call_api(self, method: str, params: dict, timeout: float = 15.0) -> dict:
        """Bot API 호출. 실패 시 예외 (호출부에서 fail-soft 처리)."""
        data = json.dumps(params).encode("utf-8")
        req = urllib.request.Request(
            self._api_url(method),
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", "replace") or "{}"
        result = json.loads(body)
        if not result.get("ok"):
            raise RuntimeError(f"telegram api error: {result.get('description')}")
        return result.get("result", {})

    def send_message(self, chat_id: int, text: str) -> bool:
        """fail-soft 응답 전송 (토큰 미노출)."""
        try:
            self._call_api("sendMessage", {
                "chat_id": chat_id,
                "text": scrub_secrets(text)[:4000],
            })
            return True
        except Exception as e:
            print(f"[Telegram] 응답 전송 실패(무시됨): "
                  f"{type(e).__name__} {_scrub(str(e))[:120]}")
            return False

    # ------------------------------------------------------------------
    # 폴링 스레드
    # ------------------------------------------------------------------
    def start(self) -> bool:
        """폴링 스레드 기동. 토큰/화이트리스트 미설정이면 False (원격 제어 비활성)."""
        token = (os.environ.get("KKM_TG_BOT_TOKEN") or "").strip()
        if not token:
            print("[Telegram] KKM_TG_BOT_TOKEN 미설정 — 원격 제어 비활성")
            return False
        if not self._allowed:
            print("[Telegram] KKM_TG_ALLOWED_USER_IDS 미설정 — 원격 제어 비활성 (fail-closed)")
            return False
        if self._thread and self._thread.is_alive():
            return True
        try:
            me = self._call_api("getMe", {})
        except Exception as e:
            print(f"[Telegram] 봇 확인 실패 — 원격 제어 비활성: "
                  f"{type(e).__name__} {_scrub(str(e))[:120]}")
            return False
        self._thread = threading.Thread(
            target=self._poll_loop, name="telegram-remote", daemon=True
        )
        self._thread.start()
        print(f"[Telegram] 원격 제어 시작: @{me.get('username', '?')} "
              f"(허용 사용자 {len(self._allowed)}명)")
        return True

    def stop(self) -> None:
        self._stop_flag.set()
        t = self._thread
        if t and t.is_alive():
            t.join(timeout=3.0)

    def _poll_loop(self) -> None:
        """getUpdates 롱폴링. 오류 시 지수 백오프."""
        backoff = 1.0
        while not self._stop_flag.is_set():
            try:
                updates = self._call_api("getUpdates", {
                    "offset": self._offset,
                    "timeout": self._poll_timeout,
                    "allowed_updates": json.dumps(["message"]),
                }, timeout=POLL_TIMEOUT_HTTP)
                backoff = 1.0
                for upd in updates or []:
                    self._offset = max(self._offset, int(upd.get("update_id", 0)) + 1)
                    cmd = self._parse_update(upd)
                    if cmd:
                        with self._queue_lock:
                            self._queue.append(cmd)
            except Exception as e:
                if self._stop_flag.is_set():
                    break
                print(f"[Telegram] 폴링 오류(재시도 {backoff:.0f}s): "
                      f"{type(e).__name__} {_scrub(str(e))[:120]}")
                if self._stop_flag.wait(backoff):
                    break
                backoff = min(backoff * 2, BACKOFF_MAX)

    def _parse_update(self, upd: dict) -> RemoteCommand | None:
        """update → RemoteCommand. /submit 본문 수집 상태도 여기서 관리한다."""
        msg = upd.get("message") or {}
        chat = msg.get("chat") or {}
        user = msg.get("from") or {}
        chat_id = chat.get("id")
        user_id = user.get("id")
        text = (msg.get("text") or "").strip()
        if not chat_id or not user_id or not text:
            return None

        # /submit 진행 중인 chat: 모든 일반 텍스트는 요구사항 줄로 수집
        with self._queue_lock:
            pending = self._pending_submits.get(chat_id)
        if pending is not None and not text.startswith("/"):
            pending["lines"].append(text)
            if text == "END":
                options = pending.get("options") or {}
                with self._queue_lock:
                    self._pending_submits.pop(chat_id, None)
                return RemoteCommand(
                    chat_id, user_id, "submit_body", "",
                    "\n".join(pending["lines"][:-1]),
                    options=options,
                )
            return None

        if not text.startswith("/"):
            return None
        parts = text.split(maxsplit=1)
        name = parts[0].lstrip("/").split("@")[0].lower()
        args = parts[1].strip() if len(parts) > 1 else ""
        return RemoteCommand(chat_id, user_id, name, args, text)

    # ------------------------------------------------------------------
    # 메인 스레드 소비 인터페이스
    # ------------------------------------------------------------------
    def poll_commands(self) -> list[RemoteCommand]:
        """메인 루프가 주기적으로 호출. 대기 중 명령을 꺼내 반환한다."""
        with self._queue_lock:
            cmds = self._queue
            self._queue = []
        return cmds

    # ------------------------------------------------------------------
    # 명령 실행 (메인 스레드에서만 호출 — Manager/futures 접근 안전)
    # ------------------------------------------------------------------
    def handle(self, cmd: RemoteCommand) -> None:
        """명령 1건 실행. 권한·상태 검증 후 콜백을 호출한다."""
        if not self.is_allowed(cmd.user_id):
            print(f"[Telegram] 거부(허용 목록 외 user {cmd.user_id}): "
                  f"{cmd.name}")
            self.send_message(cmd.chat_id, "허가되지 않은 사용자입니다.")
            return
        handler = getattr(self, f"_cmd_{cmd.name}", None)
        if handler is None:
            self.send_message(cmd.chat_id, f"알 수 없는 명령: /{cmd.name}\n/help 참고")
            return
        handler(cmd)

    def _cmd_help(self, cmd: RemoteCommand) -> None:
        self.send_message(cmd.chat_id, (
            "명령:\n"
            "/status — 실행 중 Job 현황\n"
            "/submit — 작업 제출 (이후 요구사항 여러 줄, END 로 종료)\n"
            "  첫 줄 옵션: worker=droid|codex|opencode model=... effort=low|medium|high "
            "modules=api,web\n"
            "/report <task_id> — 완료 리포트 전송\n"
            "/shutdown — graceful 종료(진행 Job 완료 후)"
        ))

    def _cmd_status(self, cmd: RemoteCommand) -> None:
        try:
            jobs = self._status_provider() or []
        except Exception as e:
            self.send_message(cmd.chat_id, f"상태 조회 실패: {type(e).__name__}")
            return
        if not jobs:
            self.send_message(cmd.chat_id, "실행 중인 Job 이 없습니다.")
            return
        lines = [f"Job {len(jobs)}건:"]
        for j in jobs:
            lines.append(
                f"· {j.get('worker_tag', '')}{j.get('task_id')} "
                f"[{j.get('status')}/{j.get('stage')}]"
            )
            req = (j.get("requirement") or "").strip().splitlines()
            if req and req[0].strip():
                first = req[0].strip()
                lines.append(f"  {first[:60]}")
        lines.append("리포트: /report <task_id>")
        self.send_message(cmd.chat_id, "\n".join(lines))

    def _cmd_submit(self, cmd: RemoteCommand) -> None:
        # 이미 수집 중이면 먼저 취소
        with self._queue_lock:
            had = self._pending_submits.pop(cmd.chat_id, None)
        if had:
            self.send_message(cmd.chat_id, "이전 /submit 입력을 취소합니다.")

        first_line = cmd.args
        options, errors = parse_submit_options(first_line)
        if errors:
            self.send_message(
                cmd.chat_id,
                "옵션 오류:\n" + "\n".join(errors) + "\n다시 /submit 로 시작하세요.",
            )
            return
        with self._queue_lock:
            self._pending_submits[cmd.chat_id] = {"lines": [], "options": options}
        self.send_message(
            cmd.chat_id,
            "요구사항을 입력하세요. 여러 줄 가능, 끝내려면 새 줄에 END\n"
            "(취소: /submit 다시 입력)",
        )

    def _cmd_submit_body(self, cmd: RemoteCommand) -> None:
        requirement = cmd.text.strip()
        if not requirement:
            self.send_message(cmd.chat_id, "요구사항이 비었습니다. /submit 로 다시.")
            return
        try:
            self._on_submit(requirement, cmd.options or {}, cmd.chat_id)
        except Exception as e:
            self.send_message(
                cmd.chat_id,
                f"제출 실패: {type(e).__name__}: {e}",
            )

    def _cmd_report(self, cmd: RemoteCommand) -> None:
        task_id = cmd.args.strip()
        if not task_id:
            self.send_message(cmd.chat_id, "사용법: /report <task_id>")
            return
        # 1:1 매칭 — .tasks/YYYYMMDD/{task_id}.md (21차 레이아웃)
        md = TaskState.task_date_dir(task_id, self._task_root) / f"{task_id}.md"
        if not md.exists():
            # 일부만 입력된 경우(예: 시간 접미사) 최근 파일에서 유일 매칭 시도
            matches = sorted(self._task_root.glob(f"*/{task_id}*.md"))
            if len(matches) == 1:
                md = matches[0]
        if not md.exists():
            self.send_message(cmd.chat_id, f"리포트를 찾을 수 없습니다: {task_id}")
            return
        try:
            content = md.read_text(encoding="utf-8", errors="replace")
        except OSError as e:
            self.send_message(cmd.chat_id, f"리포트 읽기 실패: {type(e).__name__}")
            return
        # 4000자 제한 — 앞부분 전송 + 경로 안내
        text = content if len(content) <= 3900 else content[:3900] + "\n…(이하 생략)"
        self.send_message(
            cmd.chat_id,
            f"📄 {md.as_posix()}\n\n{text}",
        )

    def _cmd_shutdown(self, cmd: RemoteCommand) -> None:
        self.send_message(
            cmd.chat_id,
            "graceful 종료를 시작합니다: 신규 제출 중지 → 진행 Job 완료 대기.",
        )
        self._on_shutdown()


class ControlPlaneTelegramRemote(TelegramRemote):
    """Minimal Telegram adapter for the durable Queue control plane.

    It deliberately reuses the existing polling/transport implementation.  The
    command surface is an allow-list; destructive/local-only operators never
    reach dynamic dispatch.  Mutations require a JSON payload containing the
    exact Queue/Job revision, a request id, and an explicit confirmation.
    """

    COMMANDS = frozenset({
        "help", "status", "report", "qa", "pause", "resume", "continue",
        "retry-review",
    })

    def __init__(
        self,
        control: Any,
        *,
        allowed_user_ids: list[int] | None = None,
        api_base: str | None = None,
        poll_timeout: int = POLL_TIMEOUT,
        task_root: str | Path = ".tasks",
    ):
        super().__init__(
            on_submit=lambda *_args, **_kwargs: None,
            on_shutdown=lambda: None,
            status_provider=lambda: [],
            allowed_user_ids=allowed_user_ids,
            api_base=api_base,
            poll_timeout=poll_timeout,
            task_root=task_root,
        )
        self._control = control
        self._dispatch_thread: threading.Thread | None = None

    @staticmethod
    def _payload(cmd: RemoteCommand) -> dict[str, Any]:
        try:
            value = json.loads(cmd.args or "{}")
        except json.JSONDecodeError as exc:
            raise ValueError("INVALID_JSON_PAYLOAD") from exc
        if not isinstance(value, dict):
            raise ValueError("INVALID_JSON_PAYLOAD")
        return value

    @staticmethod
    def _required(payload: dict[str, Any], *names: str) -> None:
        if any(name not in payload or payload[name] in (None, "") for name in names):
            raise ValueError("MISSING_OPERATOR_PROVENANCE")

    def start(self) -> bool:
        if (os.environ.get("KKM_TG_ENABLED") or "1").strip().lower() in {
            "0", "false", "no", "off"
        }:
            return False
        if not super().start():
            return False
        if not self._dispatch_thread or not self._dispatch_thread.is_alive():
            self._dispatch_thread = threading.Thread(
                target=self._dispatch_loop,
                name="telegram-control-dispatch",
                daemon=True,
            )
            self._dispatch_thread.start()
        return True

    def stop(self) -> None:
        super().stop()
        if self._dispatch_thread and self._dispatch_thread.is_alive():
            self._dispatch_thread.join(timeout=3.0)

    def _dispatch_loop(self) -> None:
        while not self._stop_flag.wait(0.2):
            for command in self.poll_commands():
                self.handle(command)

    def handle(self, cmd: RemoteCommand) -> None:
        if not self.is_allowed(cmd.user_id):
            self.send_message(cmd.chat_id, "허가되지 않은 사용자입니다.")
            return
        if cmd.name not in self.COMMANDS:
            self.send_message(cmd.chat_id, "이 명령은 Telegram에서 허용되지 않습니다.")
            return
        handler_name = cmd.name.replace("-", "_")
        handler = getattr(self, f"_control_{handler_name}", None)
        if not callable(handler):
            self.send_message(cmd.chat_id, "지원되지 않는 control 명령입니다.")
            return
        try:
            response = handler(cmd)
            if response is not None:
                self.send_message(
                    cmd.chat_id,
                    (
                        response
                        if isinstance(response, str)
                        else json.dumps(
                            response, ensure_ascii=False, sort_keys=True, default=str
                        )
                    )[:4000],
                )
        except Exception as exc:
            code = str(getattr(exc, "code", "") or str(exc) or type(exc).__name__)
            self.send_message(cmd.chat_id, f"CONTROL_FAILED: {_scrub(code)[:200]}")

    def _control_help(self, cmd: RemoteCommand) -> dict[str, Any]:
        return {"commands": sorted(self.COMMANDS)}

    def _control_status(self, cmd: RemoteCommand) -> str:
        queue = dict(self._control.get_queue().get("queue") or {})
        snapshot = dict(queue.get("progress") or {})
        if not snapshot:
            raise ValueError("PROGRESS_SNAPSHOT_UNAVAILABLE")
        return render_progress(snapshot, "TELEGRAM")

    def _control_report(self, cmd: RemoteCommand) -> dict[str, Any]:
        selector = cmd.args.strip()
        if not selector or selector == "current":
            queue = dict(self._control.get_queue().get("queue") or {})
            selector = str(queue.get("running_job_id") or queue.get("blocked_by_job_id") or "")
        if not selector:
            return {"report": "NO_CURRENT_JOB"}
        job = dict(self._control.get_job(selector))
        result = dict(job.get("last_result") or {})
        return {
            "job_id": job.get("job_id", selector),
            "status": job.get("status", ""),
            "task_ids": list(job.get("task_ids") or []),
            "report_path": result.get("report_path", ""),
            "progress": dict(job.get("progress") or {}),
        }

    def _control_qa(self, cmd: RemoteCommand) -> dict[str, Any]:
        snapshot = self._control.list_jobs(limit=200)
        qa_jobs = [
            job for job in list(snapshot.get("jobs") or [])
            if job.get("status") in {
                "AWAITING_QA", "AWAITING_DEPENDENCY_QA", "BLOCKED_BY_DEPENDENCY"
            }
        ]
        return {
            "qa_jobs": qa_jobs,
            "progress": dict(dict(snapshot.get("queue") or {}).get("progress") or {}),
        }

    def _control_pause(self, cmd: RemoteCommand) -> dict[str, Any]:
        payload = self._payload(cmd)
        self._required(payload, "expected_revision", "request_id", "confirmation", "reason")
        if payload["confirmation"] != "I_CONFIRM_PAUSE_QUEUE":
            raise ValueError("PAUSE_CONFIRMATION_REQUIRED")
        return dict(self._control.pause_queue(
            str(payload["reason"]),
            expected_revision=int(payload["expected_revision"]),
            request_id=str(payload["request_id"]),
            confirmation=str(payload["confirmation"]),
        ))

    def _control_resume(self, cmd: RemoteCommand) -> dict[str, Any]:
        payload = self._payload(cmd)
        self._required(payload, "expected_revision", "request_id", "confirmation")
        if payload["confirmation"] != "I_CONFIRM_RESUME_QUEUE":
            raise ValueError("RESUME_CONFIRMATION_REQUIRED")
        return dict(self._control.resume_queue(
            expected_revision=int(payload["expected_revision"]),
            request_id=str(payload["request_id"]),
            confirmation=str(payload["confirmation"]),
        ))

    def _control_continue(self, cmd: RemoteCommand) -> dict[str, Any]:
        payload = self._payload(cmd)
        self._required(
            payload, "job_id", "batch_id", "expected_revision", "request_id", "confirmation"
        )
        if payload["confirmation"] != "I_CONFIRM_CONTINUE_AFTER_SUCCESS":
            raise ValueError("CONTINUE_CONFIRMATION_REQUIRED")
        return dict(self._control.continue_after_success(
            str(payload["job_id"]),
            str(payload["request_id"]),
            expected_revision=int(payload["expected_revision"]),
            batch_id=str(payload["batch_id"]),
            start_immediately=True,
        ))

    def _control_retry_review(self, cmd: RemoteCommand) -> dict[str, Any]:
        payload = self._payload(cmd)
        self._required(payload, "job_id", "expected_revision", "request_id", "confirmation")
        if payload["confirmation"] != "I_CONFIRM_RETRY_REVIEW":
            raise ValueError("RETRY_REVIEW_CONFIRMATION_REQUIRED")
        operator = getattr(self._control, "retry_review_only", None)
        if not callable(operator):
            raise ValueError("REVIEW_ONLY_RETRY_UNAVAILABLE")
        return dict(operator(
            str(payload["job_id"]),
            expected_revision=int(payload["expected_revision"]),
            request_id=str(payload["request_id"]),
            confirmation=str(payload["confirmation"]),
        ))
