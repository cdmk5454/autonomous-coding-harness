"""Discord transport for the Harness control plane (Step 11).

Discord is a messaging/UI surface only.  Every state-changing command is
routed through ``ChatGPTControlService`` (revision-bound, request-id
idempotent) or ``job_queue`` primitives; this module never mutates Control DB
Job state directly.  Thread→Job bindings are durable Harness-side
integration state stored under the control root, never inside the Control DB.

Credentials come from the dedicated integration env file
(``D:\\agents\\discord-harness-bot\\.env``); the token is never printed,
hashed, or echoed - only its presence and auth outcome are reported.
"""
from __future__ import annotations

from pathlib import Path
import json
import os
import re
import threading
import time
import urllib.error
import urllib.request

from runtime_safety import scrub_secrets, safe_print as print

DEFAULT_ENV_PATH = Path(r"D:\agents\discord-harness-bot\.env")
DISCORD_API_BASE = "https://discord.com/api/v10"
TRUE_WORDS = {"1", "true", "yes", "on"}
FALSE_WORDS = {"0", "false", "no", "off"}

QA_CONFIRMATIONS = {
    "HUMAN_ACCEPT": "I_CONFIRM_QA_HUMAN_ACCEPT",
    "HUMAN_REJECT": "I_CONFIRM_QA_HUMAN_REJECT",
}


def _parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError:
        return values
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def _optional_channel(values, name):
    value = values.get(name, '')
    return int(value) if isinstance(value, str) and value.isdigit() and int(value)>0 else None


def _snowflake(value):
    if type(value) is int and value>0:
        return value
    if isinstance(value, str) and value.isdigit() and 0<len(value)<=25 and int(value)>0:
        return int(value)
    raise ValueError('DISCORD_SNOWFLAKE_INVALID')


class DiscordConfig:
    """Integration-local configuration; no global environment reads."""

    def __init__(self, token: str, allowed_user_ids: set[int],
                 allowed_guild_id: int | None, allowed_channel_id: int | None,
                 commands_enabled: bool, env_path: Path, *, qa_channel_id=None,
                 alerts_channel_id=None, reports_channel_id=None):
        self.token = token
        self.allowed_user_ids = allowed_user_ids
        self.allowed_guild_id = allowed_guild_id
        self.allowed_channel_id = allowed_channel_id
        self.commands_enabled = commands_enabled
        self.env_path = env_path
        self.qa_channel_id = qa_channel_id or allowed_channel_id
        self.alerts_channel_id = alerts_channel_id or allowed_channel_id
        self.reports_channel_id = reports_channel_id or allowed_channel_id

    @classmethod
    def load(cls, path: Path | str | None = None) -> "DiscordConfig":
        env_path = Path(path) if path else DEFAULT_ENV_PATH
        values = _parse_env_file(env_path)
        token = values.get("DISCORD_BOT_TOKEN", "")
        ids = set()
        for raw in re.split(r"[,\s]+", values.get("DISCORD_ALLOWED_USER_IDS", "")):
            if raw.isdigit():
                ids.add(int(raw))
        guild = int(values["DISCORD_ALLOWED_GUILD_ID"]) if values.get("DISCORD_ALLOWED_GUILD_ID", "").isdigit() else None
        channel = int(values["DISCORD_ALLOWED_CHANNEL_ID"]) if values.get("DISCORD_ALLOWED_CHANNEL_ID", "").isdigit() else None
        commands_flag = values.get("DISCORD_COMMANDS_ENABLED", "").strip().lower()
        return cls(token=token, allowed_user_ids=ids, allowed_guild_id=guild,
                   allowed_channel_id=channel,
                   commands_enabled=commands_flag in TRUE_WORDS,
                   env_path=env_path,
                   qa_channel_id=_optional_channel(values, 'DISCORD_QA_CHANNEL_ID'),
                   alerts_channel_id=_optional_channel(values, 'DISCORD_ALERTS_CHANNEL_ID'),
                   reports_channel_id=_optional_channel(values, 'DISCORD_REPORTS_CHANNEL_ID'))

    @property
    def configured(self) -> bool:
        return bool(self.token) and bool(self.allowed_user_ids) and self.allowed_channel_id is not None

    @property
    def state(self) -> str:
        if not self.token:
            return "USER_ACTION_REQUIRED"
        if not self.configured:
            return "USER_ACTION_REQUIRED"
        return "COMMANDS_ENABLED" if self.commands_enabled else "NOTIFICATION_ONLY"

    def describe(self) -> dict[str, object]:
        """Secret-free configuration summary."""
        return {
            "env_path": str(self.env_path),
            "token_present": bool(self.token),
            "allowed_user_count": len(self.allowed_user_ids),
            "allowed_guild_id": self.allowed_guild_id,
            "allowed_channel_id": self.allowed_channel_id,
            "channels": {"control":self.allowed_channel_id, "qa":self.qa_channel_id,
                         "alerts":self.alerts_channel_id, "reports":self.reports_channel_id},
            "commands_enabled": self.commands_enabled,
            "state": self.state,
        }


class DiscordTransport:
    """REST sender + Gateway (websocket) receiver.

    The gateway thread only decodes MESSAGE_CREATE events into a queue; all
    Harness semantics live in ``DiscordControlRemote``.
    """

    def __init__(self, token: str, *, api_base: str = DISCORD_API_BASE,
                 gateway_factory=None):
        self._token = token
        self._api_base = api_base.rstrip("/")
        self._gateway_factory = gateway_factory  # test seam: callable(ws_url) -> socket-like
        self._ws = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._inbox: list[dict] = []
        self._inbox_lock = threading.Lock()
        self.authenticated = False
        self.bot_user_id: int | None = None

    # ---------------------------------------------------------------- REST
    def _request(self, method: str, path: str, payload: dict | None = None,
                 timeout: float = 15.0) -> dict:
        url = f"{self._api_base}{path}"
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        req = urllib.request.Request(url, data=data, method=method, headers={
            "Authorization": f"Bot {self._token}",
            "Content-Type": "application/json",
            "User-Agent": "Harness/0.99.2.2",
        })
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8", "replace") or "{}"
        return json.loads(body)

    def verify_login(self) -> bool:
        try:
            me = self._request("GET", "/users/@me")
        except Exception:
            self.authenticated = False
            return False
        self.authenticated = True
        self.bot_user_id = int(me.get("id") or 0) or None
        return True

    def send_message(self, channel_id: int, text: str) -> dict[str, object]:
        evidence: dict[str, object] = {"channel_id": channel_id, "attempted": True}
        try:
            result = self._request("POST", f"/channels/{channel_id}/messages",
                                   {"content": scrub_secrets(text)[:2000]})
            evidence.update(delivered=True, delivery_status="SUCCESS",
                            message_id=result.get("id"))
        except Exception as exc:
            evidence.update(delivered=False, delivery_status="FAILED",
                            failure_code=f"DISCORD_{type(exc).__name__.upper()}",
                            failure_origin="NOTIFICATION")
        return evidence

    def create_thread(self, channel_id: int, name: str, content: str) -> dict[str, object] | None:
        """Create a public thread under the configured channel.

        Discord threads start from a message: post the anchor message, then
        start a thread attached to it.
        """
        anchor = self.send_message(channel_id, content)
        if not anchor.get("delivered"):
            return None
        try:
            thread = self._request(
                "POST",
                f"/channels/{channel_id}/messages/{anchor['message_id']}/threads",
                {"name": name[:100], "auto_archive_duration": 1440, "type": 11},
            )
            return {"thread_id": int(thread["id"]), "anchor_message_id": anchor["message_id"]}
        except Exception:
            return None

    # ------------------------------------------------------------ Gateway
    def _gateway_url(self) -> str:
        data = self._request("GET", "/gateway")
        return str(data.get("url") or "wss://gateway.discord.gg")

    def _connect(self):
        import websocket  # websocket-client (sync), already available on the runtime
        url = self._gateway_url() + "?v=10&encoding=json"
        if self._gateway_factory is not None:
            return self._gateway_factory(url)
        return websocket.create_connection(url, timeout=60)

    def _gateway_loop(self) -> None:
        import json as _json
        from websocket import WebSocketTimeoutException
        while not self._stop.is_set():
            ws = None
            try:
                ws = self._connect()
                self._ws = ws
                hello = _json.loads(ws.recv())
                interval = float((hello.get("d") or {}).get("heartbeat_interval", 41250)) / 1000.0
                ws.send(_json.dumps({
                    "op": 2,
                    "d": {
                        "token": self._token,
                        "intents": (1 << 9) | (1 << 15),  # GuildMessages | MessageContent
                        "properties": {"os": "windows", "browser": "harness", "device": "harness"},
                    },
                }))
                last_beat = time.monotonic()
                while not self._stop.is_set():
                    try:
                        ws.settimeout(max(1.0, min(interval / 2, 15.0)))
                        raw = ws.recv()
                    except (TimeoutError, WebSocketTimeoutException):
                        raw = None
                    except Exception:
                        break
                    now = time.monotonic()
                    if now - last_beat >= interval - 1.0:
                        ws.send(_json.dumps({"op": 1, "d": None}))
                        last_beat = now
                    if not raw:
                        continue
                    payload = _json.loads(raw)
                    op = payload.get("op")
                    if op == 11:
                        continue
                    if op == 1:
                        ws.send(_json.dumps({"op": 1, "d": payload.get("d")}))
                        last_beat = now
                        continue
                    if op == 7 or op == 9:  # reconnect / invalid session
                        break
                    if op == 0 and payload.get("t") == "MESSAGE_CREATE":
                        message = payload.get("d") or {}
                        if message.get("author", {}).get("id") != str(self.bot_user_id or ""):
                            with self._inbox_lock:
                                self._inbox.append(message)
            except Exception:
                pass  # transport-level failure domain; Harness keeps running
            finally:
                try:
                    if ws is not None:
                        ws.close()
                except Exception:
                    pass
                self._ws = None
            if not self._stop.is_set():
                self._stop.wait(5.0)

    def start(self) -> bool:
        if not self.verify_login():
            return False
        if self._thread and self._thread.is_alive():
            return True
        self._stop.clear()
        self._thread = threading.Thread(target=self._gateway_loop,
                                        name="discord-remote", daemon=True)
        self._thread.start()
        return True

    def stop(self) -> None:
        self._stop.set()
        ws = self._ws
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5.0)

    def poll_messages(self) -> list[dict]:
        with self._inbox_lock:
            messages = self._inbox
            self._inbox = []
        return messages


class ThreadBindingStore:
    """Durable thread→Job bindings (Harness-side integration state).

    A binding records the exact Job/execution/candidate/contract identity it
    was created for; commands are re-validated against current authority on
    every use so superseded candidates cannot be silently retargeted.
    """

    def __init__(self, path: Path):
        self._path = Path(path)
        self._lock = threading.Lock()
        self._bindings = self._load()

    def _load(self) -> dict[str, dict]:
        try:
            return json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._bindings, ensure_ascii=False, indent=1),
                       encoding="utf-8")
        os.replace(tmp, self._path)

    def bind(self, thread_id: int, binding: dict) -> None:
        with self._lock:
            self._bindings[str(thread_id)] = {**binding, "state": "ACTIVE", "closed_reason": ""}
            self._save()

    def get(self, thread_id: int) -> dict | None:
        return dict(self._bindings.get(str(thread_id)) or {})

    def close(self, thread_id: int, reason: str) -> None:
        with self._lock:
            record = self._bindings.get(str(thread_id))
            if record:
                record["state"] = "CLOSED"
                record["closed_reason"] = reason
                self._save()

    def mark_stale(self, thread_id: int, reason: str) -> None:
        with self._lock:
            record = self._bindings.get(str(thread_id))
            if record:
                record["state"] = "STALE"
                record["closed_reason"] = reason
                self._save()

    def active_for_job(self, job_id: str) -> dict | None:
        for thread_id, record in self._bindings.items():
            if record.get("job_id") == job_id and record.get("state") == "ACTIVE":
                return {**record, "thread_id":int(thread_id)}
        return None


class DiscordControlRemote:
    """Discord command surface bound to existing Harness authority."""

    READ_COMMANDS = ("help", "status", "jobs", "qa", "report")
    STATE_COMMANDS = ("bind", "approve", "reject", "continue")
    COMMANDS = READ_COMMANDS + STATE_COMMANDS

    def __init__(self, control, config: DiscordConfig, *,
                 transport: DiscordTransport | None = None,
                 bindings: ThreadBindingStore | None = None,
                 control_root: Path | None = None,
                 notify_interval: float = 20.0):
        self._control = control
        self._config = config
        self._transport = transport or DiscordTransport(config.token)
        self._bindings = bindings or ThreadBindingStore(
            (control_root or Path(".control")) / "discord-thread-bindings.json")
        self._notify_interval = notify_interval
        self._dispatch_thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._last_snapshot: dict | None = None
        self._last_emitted_at = None
        self._started = False
        self._command_log: dict[str, dict] = {}

    # ------------------------------------------------------------------
    def start(self) -> bool:
        if os.environ.get("KKM_DISCORD_ENABLED", "1").strip().lower() in FALSE_WORDS:
            return False
        if not self._config.configured:
            print(f"[Discord] {self._config.state} — integration disabled")
            return False
        if not self._transport.start():
            print("[Discord] bot login failed — integration disabled (no token echoed)")
            return False
        if self._dispatch_thread and self._dispatch_thread.is_alive():
            return True
        self._stop.clear()
        self._dispatch_thread = threading.Thread(target=self._loop,
                                                 name="discord-control", daemon=True)
        self._dispatch_thread.start()
        self._started = True
        return True

    def stop(self) -> None:
        self._stop.set()
        if self._dispatch_thread and self._dispatch_thread.is_alive():
            self._dispatch_thread.join(timeout=5.0)
        self._transport.stop()

    def _loop(self) -> None:
        last_poll = 0.0
        while not self._stop.wait(0.2):
            now = time.monotonic()
            if now - last_poll >= 0.5:
                last_poll = now
                for message in self._transport.poll_messages():
                    try:
                        self.handle(message)
                    except Exception as exc:
                        print(f"[Discord] command handling failed (ignored): {type(exc).__name__}")
            if now - (self._last_notify or 0.0) >= self._notify_interval:
                self._last_notify = now
                self.notify_once()

    _last_notify = 0.0

    # ------------------------------------------------------------------
    # Authorization
    # ------------------------------------------------------------------
    def _authorized(self, message: dict) -> bool:
        author = message.get('author')
        if not isinstance(author, dict) or author.get('bot') is True or message.get('webhook_id'):
            return False
        try:
            user_id = _snowflake(author.get('id'))
            if user_id not in self._config.allowed_user_ids:
                return False
            if self._config.allowed_guild_id is not None:
                return _snowflake(message.get('guild_id')) == self._config.allowed_guild_id
            return True
        except (TypeError, ValueError):
            return False

    def _allowed_location(self, message: dict) -> bool:
        try:channel_id = _snowflake(message.get('channel_id'))
        except (TypeError, ValueError):return False
        channels = {self._config.allowed_channel_id, self._config.qa_channel_id,
                    self._config.alerts_channel_id, self._config.reports_channel_id}
        if channel_id in channels:
            return True
        binding = self._bindings.get(channel_id)
        return bool(binding) and binding.get('parent_channel_id') in {
            self._config.allowed_channel_id,self._config.qa_channel_id}

    def _allowed_action_location(self, action: str, channel_id: int) -> bool:
        if channel_id == self._config.allowed_channel_id:
            return action in self.READ_COMMANDS or action in {'bind', 'continue'}
        if channel_id == self._config.qa_channel_id:
            return action in {'help', 'qa', 'bind'}
        if channel_id == self._config.reports_channel_id:
            return action in {'help', 'report'}
        if channel_id == self._config.alerts_channel_id:
            return False
        binding = self._bindings.get(channel_id)
        return bool(binding) and binding.get('parent_channel_id') == self._config.qa_channel_id and action in {'help', 'qa', 'approve', 'reject'}

    # ------------------------------------------------------------------
    # Command identity / idempotency
    # ------------------------------------------------------------------
    @staticmethod
    def _request_id(message: dict, action: str) -> str:
        """Harness-owned command identity: the Discord message snowflake.

        A redelivered or replayed interaction carries the same message id and
        therefore the same request id; the store-level idempotent actions turn
        the replay into a no-op instead of a second mutation.
        """
        return f"discord-{_snowflake(message.get('id'))}-{action}"

    def _command_seen(self, request_id: str) -> dict | None:
        return dict(self._command_log.get(request_id) or {})

    def _record_command(self, request_id: str, outcome: dict) -> None:
        if len(self._command_log) >= 1024:
            self._command_log.pop(next(iter(self._command_log)))
        self._command_log[request_id] = {
            "outcome": outcome.get("outcome", ""),
            "at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        }

    # ------------------------------------------------------------------
    # Handler
    # ------------------------------------------------------------------
    def handle(self, message: dict) -> dict:
        content = str(message.get("content") or "").strip()
        match = re.match(r"^!h\s+(\w+)(?:\s+(.*))?$", content)
        if not match:
            return {"outcome": "IGNORED"}
        action, args = match.group(1).lower(), (match.group(2) or "").strip()
        try:
            channel_id = _snowflake(message.get('channel_id'))
            _snowflake(message.get('id'))
        except (TypeError, ValueError):
            return {'outcome':'REJECTED','code':'DISCORD_MESSAGE_IDENTITY_INVALID'}
        if not self._authorized(message):
            self._reply(channel_id, "허가되지 않은 사용자입니다.")
            return {"outcome": "UNAUTHORIZED"}
        if action not in self.COMMANDS:
            self._reply(channel_id, "지원되지 않는 명령입니다. !h help 를 참고하세요.")
            return {"outcome": "UNKNOWN_COMMAND"}
        if action in self.STATE_COMMANDS and not self._config.commands_enabled:
            self._reply(channel_id, "이 명령은 현재 Discord에서 비활성 상태입니다 (알림 전용 단계).")
            return {"outcome": "COMMANDS_DISABLED"}
        if not self._allowed_location(message) or not self._allowed_action_location(action, channel_id):
            self._reply(channel_id, "허용되지 않은 채널/스레드입니다.")
            return {"outcome": "WRONG_LOCATION"}
        request_id = self._request_id(message, action)
        prior = self._command_seen(request_id)
        if prior:
            self._reply(channel_id, f"이미 처리된 명령입니다: {prior.get('outcome', '')}")
            return {"outcome": "DUPLICATE_REPLAY", "prior": prior}
        handler = getattr(self, f"_cmd_{action}", None)
        outcome = {"outcome": "OK"}
        try:
            response = handler(message, args)
            outcome.update(response if isinstance(response, dict) else {"response": response})
        except Exception as exc:
            code = str(getattr(exc, "code", "") or "").strip() or str(exc).strip()
            self._reply(channel_id, "CONTROL_READ_TIMEOUT" if code == "CONTROL_READ_TIMEOUT"
                        else f"CONTROL_FAILED: {scrub_secrets(code)[:200]}")
            outcome.update(outcome="REJECTED", code=scrub_secrets(code).split("\n")[0][:80])
        else:
            if outcome.get("outcome") in (None, "OK"):
                self._reply(channel_id, outcome.pop("message", "처리되었습니다."))
        self._record_command(request_id, outcome)
        return outcome

    def _reply(self, channel_id: int, text: str) -> None:
        self._transport.send_message(channel_id, text)

    # ------------------------------------------------------------------
    # Read-only commands (Phase A)
    # ------------------------------------------------------------------
    def _cmd_help(self, message: dict, args: str):
        phase = "명령 활성" if self._config.commands_enabled else "알림 전용(Phase A)"
        return {"message": f"Discord Harness ({phase})\n사용 가능: {', '.join(self.COMMANDS)}"}

    def _cmd_status(self, message: dict, args: str):
        from progress_projection import render_progress
        queue_payload = dict(self._control.get_queue().get("queue") or {})
        snapshot = dict(queue_payload.get("progress") or {})
        if not snapshot:
            return {"message": "PROGRESS_SNAPSHOT_UNAVAILABLE"}
        return {"message": render_progress(snapshot, "USER")}

    def _cmd_jobs(self, message: dict, args: str):
        snapshot = self._control.list_jobs(limit=20)
        jobs = list(snapshot.get("jobs") or [])
        if not jobs:
            return {"message": "표시할 Job이 없습니다."}
        lines = []
        for job in jobs:
            lines.append(f"{job.get('client_job_id') or job.get('job_id')}: {job.get('status')}")
        return {"message": "\n".join(lines[:20])}

    def _cmd_qa(self, message: dict, args: str):
        snapshot = self._control.list_actionable_qa_jobs()
        qa_jobs = list(snapshot.get("jobs") or [])
        if not qa_jobs:
            return {"message": "사용자 확인이 필요한 Job이 없습니다."}
        lines = ["사용자 확인 필요:"]
        for job in qa_jobs:
            lines.append(f"- {job.get('client_job_id') or job.get('job_id')} ({job.get('job_id')}): {job.get('status')}")
            from decision_qa import operator_projection, render_operator_projection
            lines.append(render_operator_projection(job.get("operator_decision") or operator_projection(job)))
        lines.append("최신 Control 결정과 허용 동작을 확인하세요. 상태 표시만으로 승인·재시도 권한이 생기지 않습니다.")
        return {"message": "\n".join(lines)}

    def _cmd_report(self, message: dict, args: str):
        selector = args.strip() or "current"
        queue_payload = dict(self._control.get_queue().get("queue") or {})
        if selector == "current":
            selector = str(queue_payload.get("running_job_id")
                           or queue_payload.get("blocked_by_job_id") or "")
        if not selector:
            return {"message": "NO_CURRENT_JOB"}
        job = dict(self._control.get_job(selector))
        result = dict(job.get("last_result") or {})
        report_meta = dict(result.get("developer_change_report") or {})
        lines = [
            f"Job: {job.get('job_id')}",
            f"상태: {job.get('status')}",
            f"Developer Report: {'있음' if report_meta.get('path') else '없음'}",
            f"Business Report: {'있음' if report_meta.get('business_report') else '없음'}",
        ]
        content = "\n".join(lines)
        if self._config.reports_channel_id != _snowflake(message.get('channel_id')):
            delivery = self._transport.send_message(self._config.reports_channel_id, scrub_secrets(content))
            if not isinstance(delivery, dict) or delivery.get('delivered') is not True:
                raise ValueError('DISCORD_REPORT_DELIVERY_FAILED')
            return {"message": "REPORT_SENT_TO_CONFIGURED_REPORTS_CHANNEL"}
        return {"message": content}

    # ------------------------------------------------------------------
    # State-changing commands (Phase B)
    # ------------------------------------------------------------------
    def _binding_context(self, message: dict) -> dict:
        channel_id = _snowflake(message.get('channel_id'))
        binding = self._bindings.get(channel_id)
        if not binding or binding.get('state') != 'ACTIVE':
            raise ValueError('ACTIVE_THREAD_BINDING_REQUIRED')
        if not binding.get('action_context'):
            self._bindings.mark_stale(channel_id, 'REBIND_REQUIRED')
            raise ValueError('STALE_BINDING_REBIND_REQUIRED')
        current = self._control.get_operator_action_context(str(binding['job_id']))
        if current.get('binding') != binding['action_context'].get('binding'):
            self._bindings.mark_stale(channel_id, 'AUTHORITY_CHANGED')
            raise ValueError('STALE_BINDING_AUTHORITY_CHANGED')
        job = dict(self._control.get_job(str(binding['job_id'])))
        if job.get('status')!='AWAITING_QA':
            self._bindings.close(channel_id, 'JOB_STATE_CHANGED')
            raise ValueError('JOB_NOT_AWAITING_QA')
        return {'binding':binding,'job':job,'channel_id':channel_id,'current_context':current}

    def _cmd_bind(self, message: dict, args: str):
        job_id = args.strip()
        if not job_id:raise ValueError('JOB_ID_REQUIRED')
        job = dict(self._control.get_job(job_id))
        current = self._control.get_operator_action_context(job_id)
        if job.get('status')!='AWAITING_QA' or not current.get('binding',{}).get('decision_ids'):
            raise ValueError('QA_CURRENT_DECISION_REQUIRED')
        if _snowflake(message.get('channel_id')) not in {self._config.allowed_channel_id,self._config.qa_channel_id}:
            raise ValueError('BIND_FROM_CONFIGURED_CHANNEL_ONLY')
        label = str(job.get('client_job_id') or job_id)
        parent = self._config.qa_channel_id
        created = self._transport.create_thread(parent, f'QA {label}',
            f'Job {job_id} 사용자 확인 필요. 허용 동작: '+', '.join(current['legal_next_actions']))
        if not created:raise ValueError('THREAD_CREATION_FAILED')
        self._bindings.bind(int(created['thread_id']), {
            'job_id':job_id,'execution_id':current['binding']['execution_id'],
            'candidate_id':current['binding']['candidate_id'],
            'contract_revision':current['binding']['contract_revision'],
            'queue_generation':current['binding']['queue_generation'],
            'job_revision':current['binding']['job_revision'],'action_context':current,
            'parent_channel_id':parent,'bound_by_user_id':_snowflake(message['author']['id']),
            'bound_at':time.strftime('%Y-%m-%dT%H:%M:%S%z')})
        return {'message':f'스레드 연결 완료: Job {job_id} / candidate {job.get("candidate_id")}'}

    def _cmd_approve(self, message: dict, args: str):
        return self._resolve_qa(message, "HUMAN_ACCEPT", action="approve")

    def _cmd_reject(self, message: dict, args: str):
        return self._resolve_qa(message, "HUMAN_REJECT", action="reject")

    def _resolve_qa(self, message: dict, resolution: str, *, action: str) -> dict:
        context = self._binding_context(message)
        job = context["job"]
        original = context["binding"]["action_context"]
        from operator_actions import validate
        validate(original, context["current_context"], resolution)
        confirmation = args_value(message, resolution)
        result = dict(self._control.resolve_qa_decision(
            str(job.get("job_id")),
            resolution=resolution,
            expected_revision=original["binding"]["job_revision"],
            request_id=self._request_id(message, action),
            confirmation=confirmation,
            candidate_id=original["binding"]["candidate_id"],
            decision_context=original,
        ))
        self._bindings.close(context["channel_id"], f"RESOLVED_{resolution}")
        status = result.get("status", "")
        if resolution == "HUMAN_REJECT":
            return {"message": f"반영하지 않음으로 기록되었습니다 (최종 상태: {status})."}
        return {"message": f"승인 처리 결과: {status}. 승격/커밋은 Harness 기록을 따릅니다."}

    def _cmd_continue(self, message: dict, args: str):
        if _snowflake(message.get('channel_id')) != self._config.allowed_channel_id:
            raise ValueError('CONTINUE_FROM_CONTROL_CHANNEL_ONLY')
        payload = _parse_json_args(args)
        if 'request_id' in payload and payload['request_id'] != self._request_id(message,'continue'):
            raise ValueError('DISCORD_REQUEST_ID_OVERRIDE_DENIED')
        if type(payload.get('expected_revision')) is not int:
            raise ValueError('EXPECTED_REVISION_INVALID')
        required = {"job_id", "batch_id", "expected_revision"}
        if not required <= set(payload):
            raise ValueError("MISSING_OPERATOR_PROVENANCE")
        result = dict(self._control.continue_after_success(
            str(payload["job_id"]),
            self._request_id(message, "continue"),
            expected_revision=int(payload["expected_revision"]),
            batch_id=str(payload["batch_id"]),
        ))
        return {"message": f"continue 결과: {result.get('replayed') and '재생(replay)' or '처리'}됨. Job 상태는 기록을 따릅니다."}

    # ------------------------------------------------------------------
    # Notifications (Phase A) — reuse ProgressSnapshot + should_emit
    # ------------------------------------------------------------------
    def notify_once(self) -> dict:
        try:
            queue_payload = dict(self._control.get_queue().get("queue") or {})
            snapshot = dict(queue_payload.get("progress") or {})
            if not snapshot:
                return {"emitted": False, "reason": "NO_SNAPSHOT"}
            from progress_projection import should_emit
            import datetime as _dt
            now = _dt.datetime.now(_dt.timezone.utc)
            emit, reason = should_emit(self._last_snapshot, snapshot, now=now,
                                       last_emitted_at=self._last_emitted_at)
            qa_notified = self._ensure_qa_threads(snapshot)
            if not emit and not qa_notified:
                return {"emitted": False, "reason": reason}
            from progress_projection import render_progress
            evidence = self._transport.send_message(
                self._config.alerts_channel_id,
                render_progress(snapshot, "USER"))
            self._last_snapshot = dict(snapshot)
            if evidence.get("delivered"):
                self._last_emitted_at = now
            return {"emitted": bool(evidence.get("delivered")), "reason": reason,
                    "evidence": evidence, "qa_threads": qa_notified}
        except Exception as exc:
            return {"emitted": False, "reason": f"NOTIFICATION_{type(exc).__name__}"}

    def _ensure_qa_threads(self, snapshot: dict) -> bool:
        created = False
        for job in self._control.list_actionable_qa_jobs().get('jobs') or []:
            job_id = str(job['job_id'])
            current = job.get('action_context') or self._control.get_operator_action_context(job_id)
            old = self._bindings.active_for_job(job_id)
            if old and old.get('action_context',{}).get('binding') == current.get('binding'):
                continue
            if old:self._bindings.mark_stale(old['thread_id'],'AUTHORITY_CHANGED')
            label = str(job.get('client_job_id') or job_id)
            created_thread = self._transport.create_thread(self._config.qa_channel_id,f'QA {label}',
                f'Job {job_id} 사용자 확인 필요. 허용 동작: '+', '.join(current['legal_next_actions'])+
                '. 정책·보안 결정은 candidate 승인으로 대체되지 않습니다.')
            if created_thread:
                b=current['binding']
                self._bindings.bind(int(created_thread['thread_id']), {
                    'job_id':job_id,'execution_id':b['execution_id'],'candidate_id':b['candidate_id'],
                    'contract_revision':b['contract_revision'],'queue_generation':b['queue_generation'],
                    'job_revision':b['job_revision'],'action_context':current,
                    'parent_channel_id':self._config.qa_channel_id,'bound_by_user_id':0,
                    'bound_at':time.strftime('%Y-%m-%dT%H:%M:%S%z'),'auto_created':True})
                created=True
        return created


class _DiscordOutboundSender:
    """Lazy shared REST-only sender for operator notifications.

    Never starts the Gateway and never handles commands: outbound delivery
    must not depend on the inbound websocket transport staying connected.
    Delivery failure is diagnostic evidence only and never raises.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._transport: DiscordTransport | None = None
        self._channels: dict[str, int | None] = {}

    def _resolve(self) -> tuple[DiscordTransport | None, int | None, str]:
        if os.environ.get("KKM_DISCORD_ENABLED", "1").strip().lower() in FALSE_WORDS:
            return None, None, "DISABLED"
        config = DiscordConfig.load()
        if not config.configured:
            return None, None, config.state
        with self._lock:
            if self._transport is None:
                self._transport = DiscordTransport(config.token)
                self._channels = {'control': config.allowed_channel_id,
                                  'qa': config.qa_channel_id,
                                  'alerts': config.alerts_channel_id,
                                  'reports': config.reports_channel_id}
            return self._transport, self._channels.get('control'), config.state

    def status(self) -> dict[str, object]:
        transport, channel_id, state = self._resolve()
        return {"configured": transport is not None, "state": state,
                "channel_id": channel_id}

    def send(self, message: str, title: str | None = None,
             priority: str | None = None, *, channel: str = "discord",
             surface: str = "alerts") -> dict[str, object]:
        attempted_at = time.strftime("%Y-%m-%dT%H:%M:%S%z")
        evidence: dict[str, object] = {
            "notification_id": "", "event_type": str(priority or "STATE_CHANGE").upper(),
            "channel": channel, "attempted_at": attempted_at, "configured": False,
            "attempted": False, "delivered": False, "delivery_status": "FAILED",
            "failure_code": "NOTIFICATION_DISABLED", "failure_origin": "NOTIFICATION",
            "message_id": None, "latency_ms": None,
        }
        transport, channel_id, state = self._resolve()
        if transport is None:
            evidence["failure_code"] = ("NOTIFICATION_DISABLED" if state == "DISABLED"
                                        else "NOTIFICATION_NOT_CONFIGURED")
            return evidence
        channel_id = self._channels.get(surface)
        if surface not in {'alerts', 'reports', 'qa', 'control'} or channel_id is None:
            evidence['failure_code'] = 'DISCORD_SURFACE_NOT_CONFIGURED'
            return evidence
        evidence["configured"] = True
        evidence["surface"] = surface
        text = f"{title}\n{message}" if title else str(message)
        started = time.perf_counter()
        evidence["attempted"] = True
        try:
            result = transport.send_message(int(channel_id), text)
            evidence.update(result)
            if evidence.get("delivered"):
                evidence["failure_code"] = ""
                evidence["failure_origin"] = ""
            evidence["notification_id"] = ("DCD-" + str(result.get("message_id") or
                                                         f"{attempted_at}:{hash(text) & 0xFFFFFFFF:#x}"))
        except Exception as exc:
            evidence.update(delivered=False, delivery_status="FAILED",
                            failure_code=f"DISCORD_{type(exc).__name__.upper()}",
                            failure_origin="NOTIFICATION")
        finally:
            evidence["latency_ms"] = int((time.perf_counter() - started) * 1000)
        return evidence


_shared_outbound: _DiscordOutboundSender | None = None
_outbound_lock = threading.Lock()


def discord_notify_evidence(message: str, title: str | None = None,
                            priority: str | None = None, *,
                            channel: str = "discord", surface: str = "alerts") -> dict[str, object]:
    """Secret-free Discord delivery evidence; never changes Job outcomes."""
    global _shared_outbound
    with _outbound_lock:
        if _shared_outbound is None:
            _shared_outbound = _DiscordOutboundSender()
        sender = _shared_outbound
    return sender.send(message, title, priority, channel=channel, surface=surface)


def discord_outbound_configured() -> bool:
    """True when a live Discord outbound send is possible right now."""
    global _shared_outbound
    with _outbound_lock:
        if _shared_outbound is None:
            _shared_outbound = _DiscordOutboundSender()
        sender = _shared_outbound
    return bool(sender.status()["configured"])


def args_value(message: dict, resolution: str) -> str:
    """Confirmation phrase for a typed QA decision."""
    return QA_CONFIRMATIONS[resolution]


def _parse_json_args(args: str) -> dict:
    try:
        value = json.loads(args or "{}")
    except json.JSONDecodeError as exc:
        raise ValueError("INVALID_JSON_PAYLOAD") from exc
    if not isinstance(value, dict):
        raise ValueError("INVALID_JSON_PAYLOAD")
    return value
