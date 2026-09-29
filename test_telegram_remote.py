"""
telegram_remote.py 단위 테스트 (22차 텔레그램 원격 제어)
- 화이트리스트: 허용 외 사용자 명령 거부 (fail-closed)
- /submit 다중 줄 수집 → END → on_submit 콜백 (Manager 경유 강제 구조)
- 옵션 파싱: worker/model/effort/modules
- /report: .tasks/YYYYMMDD/{task_id}.md 1:1 매칭
- /shutdown: on_shutdown 콜백
- 토큰 미노출: 오류 메시지 스크럽
- 실제 네트워크 미사용 — 로컬 HTTP 서버로 Bot API 흉내
"""

from __future__ import annotations

import json
import io
import os
import sys
import harness_temp as tempfile
import threading
import unittest
from unittest import mock
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from telegram_remote import (
    TelegramRemote,
    RemoteCommand,
    allowed_ids_from_env,
    parse_submit_options,
)
from task_state import TaskState

original_cwd = os.getcwd()


class _BotApiHandler(BaseHTTPRequestHandler):
    """로컬 Bot API 흉내: 응답 본문에 chat_id/text 기록."""

    calls: list[dict] = []

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode("utf-8") if length else ""
        try:
            params = json.loads(body)
        except Exception:
            params = {}
        _BotApiHandler.calls.append({"path": self.path, "params": params})

        if self.path.endswith("/getMe"):
            resp = {"ok": True, "result": {"id": 1, "username": "kkm_bot"}}
        elif self.path.endswith("/getUpdates"):
            resp = {"ok": True, "result": []}
        elif self.path.endswith("/sendMessage"):
            resp = {"ok": True, "result": {"message_id": 1}}
        else:
            resp = {"ok": False, "description": "not found"}
        data = json.dumps(resp).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


def _mk_cmd(name: str, args: str = "", user_id: int = 100, chat_id: int = 555,
            text: str = "", options: dict | None = None) -> RemoteCommand:
    return RemoteCommand(chat_id, user_id, name, args, text, options=options)


class TelegramRemoteTestBase(unittest.TestCase):
    def setUp(self):
        self._state_tmp = tempfile.TemporaryDirectory()
        self.state_root = Path(self._state_tmp.name) / ".tasks"
        self._env = {
            k: os.environ.get(k) for k in
            ("KKM_TG_BOT_TOKEN", "KKM_TG_ALLOWED_USER_IDS", "KKM_TG_API_BASE")
        }
        _BotApiHandler.calls = []
        self.server = HTTPServer(("127.0.0.1", 0), _BotApiHandler)
        self.thread = threading.Thread(
            target=self.server.serve_forever, daemon=True
        )
        self.thread.start()
        base = f"http://127.0.0.1:{self.server.server_address[1]}"
        os.environ["KKM_TG_BOT_TOKEN"] = "TEST-TOKEN-123"
        os.environ["KKM_TG_ALLOWED_USER_IDS"] = "100,200"
        os.environ["KKM_TG_API_BASE"] = base
        self.api_base = base
        self.submits: list[tuple] = []
        self.shutdown_calls: list[bool] = []
        self.remote = TelegramRemote(
            on_submit=lambda req, opt, cid: self.submits.append((req, opt, cid)),
            on_shutdown=lambda: self.shutdown_calls.append(True),
            status_provider=lambda: [],
            allowed_user_ids=[100, 200],
            api_base=base,
        )

    def tearDown(self):
        self.remote.stop()
        self.server.shutdown()
        self.server.server_close()
        for k, v in self._env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        self._state_tmp.cleanup()

    def _sent_texts(self) -> list[str]:
        return [
            c["params"].get("text", "") for c in _BotApiHandler.calls
            if c["path"].endswith("/sendMessage")
        ]


class WhitelistTest(TelegramRemoteTestBase):
    def test_empty_whitelist_rejects_everyone(self):
        """화이트리스트 비어 있으면 누구도 통과 못 한다 (fail-closed)."""
        r = TelegramRemote(
            on_submit=lambda *a: None,
            on_shutdown=lambda: None,
            status_provider=lambda: [],
            allowed_user_ids=[],
            api_base=self.api_base,
        )
        self.assertFalse(r.is_allowed(100))
        self.assertFalse(r.is_allowed(999))

    def test_unknown_user_command_rejected(self):
        cmd = _mk_cmd("status", user_id=999)
        self.remote.handle(cmd)
        texts = self._sent_texts()
        self.assertEqual(len(texts), 1)
        self.assertIn("허가되지 않은", texts[0])
        self.assertEqual(self.submits, [])       # 제출 없음
        self.assertEqual(self.shutdown_calls, [])  # 종료 없음

    def test_allowed_user_passes(self):
        self.assertTrue(self.remote.is_allowed(100))
        self.assertTrue(self.remote.is_allowed(200))

    def test_allowed_ids_from_env_parsing(self):
        os.environ["KKM_TG_ALLOWED_USER_IDS"] = " 100 ,abc,200 ,,300"
        self.assertEqual(allowed_ids_from_env(), [100, 200, 300])


class SubmitFlowTest(TelegramRemoteTestBase):
    def test_submit_multiline_until_end(self):
        """/submit → 여러 줄 → END → on_submit 호출 (옵션 유지)."""
        # 1) /submit 시작 (옵션 포함)
        self.remote.handle(_mk_cmd("submit", args="worker=codex modules=cap,capui"))
        # 2) 요구사항 줄들
        upd = lambda txt: self.remote._parse_update({
            "update_id": 1,
            "message": {
                "chat": {"id": 555},
                "from": {"id": 100},
                "text": txt,
            },
        })
        self.assertIsNone(upd("첫째 줄"))
        self.assertIsNone(upd("둘째 줄"))
        cmd = upd("END")
        self.assertIsNotNone(cmd)
        self.assertEqual(cmd.name, "submit_body")
        self.assertEqual(cmd.text, "첫째 줄\n둘째 줄")
        # 옵션이 유지되는지
        self.remote.handle(cmd)
        self.assertEqual(len(self.submits), 1)
        req, opt, chat_id = self.submits[0]
        self.assertEqual(req, "첫째 줄\n둘째 줄")
        self.assertEqual(opt.get("worker"), "codex")
        self.assertEqual(opt.get("modules"), ["cap", "capui"])
        self.assertEqual(chat_id, 555)

    def test_submit_reentry_cancels_previous(self):
        self.remote.handle(_mk_cmd("submit", args=""))
        self.remote._parse_update({
            "update_id": 1, "message": {"chat": {"id": 555}, "from": {"id": 100}, "text": "중간"},
        })
        # 다시 /submit → 이전 수집 취소 안내 후 새 수집 시작
        self.remote.handle(_mk_cmd("submit", args=""))
        texts = self._sent_texts()
        self.assertTrue(any("취소" in t for t in texts))
        # 이전 줄들은 사라짐 — 새 END 가 와도 빈 요구사항
        cmd = self.remote._parse_update({
            "update_id": 2, "message": {"chat": {"id": 555}, "from": {"id": 100}, "text": "END"},
        })
        self.assertEqual(cmd.name, "submit_body")
        self.assertEqual(cmd.text, "")
        self.remote.handle(cmd)
        self.assertEqual(self.submits, [])   # 빈 요구사항은 제출 안 됨

    def test_submit_options_parsing(self):
        opts, errs = parse_submit_options(
            "worker=codex, model=gpt-5.6-sol, effort=high, modules=cap capui"
        )
        self.assertEqual(errs, [])
        self.assertEqual(opts["worker"], "codex")
        self.assertEqual(opts["model"], "gpt-5.6-sol")
        self.assertEqual(opts["effort"], "high")
        self.assertEqual(opts["modules"], ["cap", "capui"])
        opencode, errors = parse_submit_options(
            "worker=opencode model=provider/model modules=suc"
        )
        self.assertEqual(errors, [])
        self.assertEqual(opencode["worker"], "opencode")
        self.assertEqual(opencode["model"], "provider/model")

    def test_submit_options_invalid_worker(self):
        opts, errs = parse_submit_options("worker=claude")
        self.assertIn("worker", errs[0])
        self.assertNotIn("worker", opts)

    def test_submit_options_absent_is_requirement(self):
        """옵션 키가 하나도 없으면 첫 줄 전체가 요구사항으로 간주(빈 options)."""
        opts, errs = parse_submit_options("cap/src/A.java 버그 고쳐줘")
        self.assertEqual(errs, [])
        self.assertEqual(opts, {})

    def test_submit_body_empty_rejected(self):
        self.remote.handle(_mk_cmd("submit_body", text="   "))
        self.assertEqual(self.submits, [])
        self.assertTrue(any("비었습니다" in t for t in self._sent_texts()))


class ReportCommandTest(TelegramRemoteTestBase):
    def test_report_exact_match(self):
        with tempfile.TemporaryDirectory() as td:
            os.chdir(td)
            try:
                md = Path(td) / ".tasks" / "20260823" / "TASK-20260823-101010.md"
                md.parent.mkdir(parents=True)
                md.write_text("# 리포트\n내용", encoding="utf-8")
                self.remote.handle(_mk_cmd("report", args="TASK-20260823-101010"))
                texts = self._sent_texts()
                self.assertEqual(len(texts), 1)
                self.assertIn("TASK-20260823-101010.md", texts[0])
                self.assertIn("# 리포트", texts[0])
            finally:
                os.chdir(original_cwd)

    def test_report_not_found(self):
        with tempfile.TemporaryDirectory() as td:
            os.chdir(td)
            try:
                self.remote.handle(_mk_cmd("report", args="TASK-XXXX-YYYY"))
                texts = self._sent_texts()
                self.assertTrue(any("찾을 수 없습니다" in t for t in texts))
            finally:
                os.chdir(original_cwd)


class ShutdownCommandTest(TelegramRemoteTestBase):
    def test_shutdown_invokes_callback(self):
        self.remote.handle(_mk_cmd("shutdown"))
        self.assertEqual(self.shutdown_calls, [True])
        self.assertTrue(any("graceful" in t for t in self._sent_texts()))

    def test_shutdown_denied_for_stranger(self):
        self.remote.handle(_mk_cmd("shutdown", user_id=999))
        self.assertEqual(self.shutdown_calls, [])
        self.assertTrue(any("허가되지 않은" in t for t in self._sent_texts()))


class StatusCommandTest(TelegramRemoteTestBase):
    def test_status_lists_running_jobs(self):
        remote = TelegramRemote(
            on_submit=lambda *a: None,
            on_shutdown=lambda: None,
            status_provider=lambda: [
                {"task_id": "TASK-20260823-1", "status": "RUNNING",
                 "stage": "REVIEW", "worker_tag": "Worker1 ",
                 "requirement": "첫째 줄 요구\n둘째"},
            ],
            allowed_user_ids=[100],
            api_base=self.api_base,
        )
        remote.handle(_mk_cmd("status"))
        texts = self._sent_texts()
        self.assertEqual(len(texts), 1)
        self.assertIn("TASK-20260823-1", texts[0])
        self.assertIn("RUNNING/REVIEW", texts[0])
        self.assertIn("Worker1", texts[0])
        self.assertIn("첫째 줄 요구", texts[0])

    def test_status_empty(self):
        self.remote.handle(_mk_cmd("status"))
        self.assertTrue(any("없습니다" in t for t in self._sent_texts()))


class ApiErrorTest(TelegramRemoteTestBase):
    def test_send_message_failsoft(self):
        """잘못된 api → 예외 전파 없이 False."""
        r = TelegramRemote(
            on_submit=lambda *a: None,
            on_shutdown=lambda: None,
            status_provider=lambda: [],
            allowed_user_ids=[100],
            api_base="http://127.0.0.1:1",   # 닫힌 포트
        )
        self.assertFalse(r.send_message(555, "x"))

    def test_token_not_leaked_in_error(self):
        """전송 실패 메시지에 토큰이 노출되지 않는다."""
        import io
        import contextlib
        r = TelegramRemote(
            on_submit=lambda *a: None,
            on_shutdown=lambda: None,
            status_provider=lambda: [],
            allowed_user_ids=[100],
            api_base="http://127.0.0.1:1",
        )
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            r.send_message(555, "x")
        self.assertNotIn("TEST-TOKEN-123", buf.getvalue())


class StartGateTest(TelegramRemoteTestBase):
    def test_start_requires_token(self):
        os.environ.pop("KKM_TG_BOT_TOKEN")
        self.assertFalse(self.remote.start())

    def test_start_requires_whitelist(self):
        r = TelegramRemote(
            on_submit=lambda *a: None,
            on_shutdown=lambda: None,
            status_provider=lambda: [],
            allowed_user_ids=[],
            api_base=self.api_base,
        )
        self.assertFalse(r.start())

    def test_start_ok_runs_getme_then_thread(self):
        self.assertTrue(self.remote.start())
        self.remote.stop()

    def test_warning_failure_and_success_console_paths_are_cp949_safe(self):
        raw = io.BytesIO()
        stream = io.TextIOWrapper(raw, encoding="cp949", errors="strict")
        try:
            with mock.patch.object(sys, "stdout", stream):
                os.environ.pop("KKM_TG_BOT_TOKEN", None)
                self.assertFalse(self.remote.start())
                os.environ["KKM_TG_BOT_TOKEN"] = "TEST-TOKEN-123"
                with mock.patch.object(
                    self.remote, "_call_api", side_effect=RuntimeError("failure — 📤")
                ):
                    self.assertFalse(self.remote.send_message(555, "x"))
                with mock.patch.object(
                    self.remote, "_call_api", return_value={"username": "한글봇📤"}
                ), mock.patch("telegram_remote.threading.Thread"):
                    self.assertTrue(self.remote.start())
            stream.flush()
            output = raw.getvalue().decode("cp949")
        finally:
            stream.detach()
        self.assertIn("Telegram", output)
        self.assertIn("한글", output)
        self.assertIn("?", output)


class SubmitViaManagerTest(TelegramRemoteTestBase):
    """run._submit_from_telegram: 제출은 Manager.submit_task 경유만 (워커 직행 금지)."""

    def test_submit_goes_through_manager(self):
        import run as run_mod
        from concurrent.futures import Future
        from types import SimpleNamespace

        submitted: list = []

        def fake_submit(task):
            submitted.append(task)
            f = Future()
            f.set_running_or_notify_cancel()
            return f

        manager = SimpleNamespace(
            max_workers=3, working_dir="D:\\inha_dev", submit_task=fake_submit
        )
        remote = self.remote
        futures: list[tuple] = []

        run_mod._submit_from_telegram(
            "suc/src/A.java 수정",
            {"worker": "codex", "model": "gpt-5.6-sol", "modules": ["suc"]},
            555,
            manager=manager,
            futures=futures,
            available_modules=["suc", "cap"],
            remote=remote,
            task_root=self.state_root,
        )

        # Manager 경유 1건, futures 등록, worker 직행 없음(fake 만 호출됨)
        self.assertEqual(len(submitted), 1)
        task = submitted[0]
        self.assertEqual(task.worker, "codex")
        self.assertEqual(task.codex_model, "gpt-5.6-sol")
        self.assertEqual(task.target_module, ["suc"])
        self.assertEqual(task.task_mode, "MODIFICATION")
        self.assertEqual(len(futures), 1)
        self.assertIn("제출 완료", self._sent_texts()[-1])

        # worker_no 슬롯 할당 확인
        self.assertEqual(task.worker_no, 1)

    def test_submit_rejected_when_slots_full(self):
        import run as run_mod
        from concurrent.futures import Future
        from types import SimpleNamespace

        submitted: list = []

        def fake_submit(task):
            submitted.append(task)
            f = Future()
            f.set_running_or_notify_cancel()
            return f

        manager = SimpleNamespace(
            max_workers=1, working_dir=".", submit_task=fake_submit
        )
        # 슬롯 1개를 이미 점유한 future
        t1 = run_mod.create_task(
            requirement="a", worker_type="droid", droid_model="",
            codex_model=None, target_module=[], working_dir=".",
            task_id="TASK-X", worker_no=1,
        )
        f1 = Future()
        f1.set_running_or_notify_cancel()
        futures = [(f1, t1, "")]

        run_mod._submit_from_telegram(
            "둘째 작업", {}, 555,
            manager=manager, futures=futures,
            available_modules=["suc"], remote=self.remote,
            task_root=self.state_root,
        )
        self.assertEqual(submitted, [])   # 거절 — 제출 없음
        self.assertTrue(any("가득" in t for t in self._sent_texts()))

    def test_submit_unknown_module_rejected(self):
        import run as run_mod
        from types import SimpleNamespace

        manager = SimpleNamespace(
            max_workers=3, working_dir=".", submit_task=lambda t: None
        )
        run_mod._submit_from_telegram(
            "cap/src/A.java 수정", {"modules": ["cap"]}, 555,
            manager=manager, futures=[],
            available_modules=["suc"],   # cap 은 작업 경로에 없음
            remote=self.remote,
            task_root=self.state_root,
        )
        self.assertTrue(any("제출 중단" in t for t in self._sent_texts()))


if __name__ == "__main__":
    unittest.main()
