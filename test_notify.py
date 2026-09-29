"""
notify.py 단위 테스트 (Telegram 알림, 22차)
- 전송 성공 → True (로컬 HTTP 서버로 sendMessage 요청 내용 검증)
- 서버 다운 → 예외 없이 False (fail-soft)
- KKM_TG_ENABLED=0 → 전송 스킵
- 토큰 미설정 → 전송 스킵 (미전송 False)
- 토큰 누출 방지: 오류 메시지에 토큰 미포함
- notify_task_event 포맷 (시그니처/포맷은 ntfy 시절과 동일)

실행: python -m unittest test_notify -v
(외부 네트워크 미사용 — 127.0.0.1 임시 서버만 사용)
"""

from __future__ import annotations

import io
import contextlib
import json
import os
import threading
import unittest
from unittest import mock
from http.server import BaseHTTPRequestHandler, HTTPServer

from notify import send_notify, send_notify_evidence, notify_task_event
from task_state import TaskState


class _CaptureHandler(BaseHTTPRequestHandler):
    requests: list[dict] = []

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode("utf-8") if length else ""
        _CaptureHandler.requests.append({
            "method": "POST",
            "path": self.path,
            "body": body,
            "json": json.loads(body) if body else {},
        })
        resp = json.dumps({"ok": True, "result": {"message_id": 1}}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(resp)))
        self.end_headers()
        self.wfile.write(resp)

    def log_message(self, *args):  # 콘솔 잡음 제거
        pass


class SendNotifyTest(unittest.TestCase):
    def setUp(self):
        self._env = {
            k: os.environ.get(k) for k in
            ("KKM_TG_BOT_TOKEN", "KKM_TG_CHAT_ID", "KKM_TG_ENABLED",
             "KKM_TG_TITLE", "KKM_TG_API_BASE")
        }
        os.environ.pop("KKM_TG_ENABLED", None)
        os.environ.pop("KKM_TG_TITLE", None)
        _CaptureHandler.requests = []
        self._server = HTTPServer(("127.0.0.1", 0), _CaptureHandler)
        self._thread = threading.Thread(
            target=self._server.serve_forever, daemon=True
        )
        self._thread.start()
        port = self._server.server_address[1]
        os.environ["KKM_TG_API_BASE"] = f"http://127.0.0.1:{port}"
        os.environ["KKM_TG_BOT_TOKEN"] = "TESTTOKEN"
        os.environ["KKM_TG_CHAT_ID"] = "424242"

    def tearDown(self):
        self._server.shutdown()
        self._server.server_close()
        for k, v in self._env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def test_send_success_returns_true(self):
        ok = send_notify("hello telegram", title="t1")
        self.assertTrue(ok)
        self.assertEqual(len(_CaptureHandler.requests), 1)
        req = _CaptureHandler.requests[0]
        self.assertEqual(req["method"], "POST")
        # URL: /bot<token>/sendMessage — 토큰이 경로에 실린다
        self.assertEqual(req["path"], "/botTESTTOKEN/sendMessage")
        payload = req["json"]
        self.assertEqual(payload["chat_id"], "424242")
        self.assertEqual(payload["text"], "t1\nhello telegram")

    def test_delivery_evidence_records_success_without_credentials(self):
        evidence = send_notify_evidence(
            "progress changed", title="Harness 0.9", priority="state_change"
        )
        self.assertTrue(evidence["configured"])
        self.assertTrue(evidence["attempted"])
        self.assertTrue(evidence["delivered"])
        self.assertEqual(evidence["delivery_status"], "SUCCESS")
        self.assertEqual(evidence["http_status"], 200)
        self.assertEqual(evidence["message_id"], 1)
        self.assertIsInstance(evidence["latency_ms"], int)
        self.assertTrue(str(evidence["notification_id"]).startswith("NTF-"))
        self.assertNotIn("TESTTOKEN", json.dumps(evidence))

    def test_disabled_delivery_evidence_is_fail_soft(self):
        os.environ["KKM_TG_ENABLED"] = "0"
        evidence = send_notify_evidence("suppressed")
        self.assertFalse(evidence["attempted"])
        self.assertFalse(evidence["delivered"])
        self.assertEqual(evidence["failure_code"], "NOTIFICATION_DISABLED")

    def test_server_down_returns_false_no_raise(self):
        # 응답하지 않는 포트 — 예외가 밖으로 새면 안 됨
        os.environ["KKM_TG_API_BASE"] = "http://127.0.0.1:1"
        try:
            ok = send_notify("down?")
        except Exception as e:
            self.fail(f"서버 다운 시 예외 발생: {e}")
            return
        self.assertFalse(ok)

    def test_disabled_skips_send(self):
        os.environ["KKM_TG_ENABLED"] = "0"
        ok = send_notify("should skip")
        self.assertFalse(ok)
        self.assertEqual(_CaptureHandler.requests, [])

    def test_missing_token_skips_send(self):
        os.environ.pop("KKM_TG_BOT_TOKEN")
        ok = send_notify("no token")
        self.assertFalse(ok)
        self.assertEqual(_CaptureHandler.requests, [])

    def test_missing_chat_id_skips_send(self):
        os.environ.pop("KKM_TG_CHAT_ID")
        ok = send_notify("no chat")
        self.assertFalse(ok)
        self.assertEqual(_CaptureHandler.requests, [])

    def test_priority_param_accepted_no_effect(self):
        """priority 는 시그니처 호환용 — 전송 payload 에 영향 없음."""
        ok = send_notify("msg", title="t", priority="high")
        self.assertTrue(ok)
        self.assertNotIn("priority", _CaptureHandler.requests[0]["json"])

    def test_token_not_leaked_in_error_output(self):
        """전송 실패 로그에 토큰이 노출되지 않는다."""
        os.environ["KKM_TG_API_BASE"] = "http://127.0.0.1:1"
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            send_notify("leak?")
        self.assertNotIn("TESTTOKEN", buf.getvalue())

    def test_failure_output_is_safe_on_strict_cp949_stream(self):
        raw = io.BytesIO()
        stream = io.TextIOWrapper(raw, encoding="cp949", errors="strict")
        try:
            with mock.patch(
                "notify.urllib.request.urlopen",
                side_effect=RuntimeError("failure — 📤"),
            ), contextlib.redirect_stdout(stream):
                self.assertFalse(send_notify("메시지"))
            stream.flush()
            output = raw.getvalue().decode("cp949")
        finally:
            stream.detach()
        self.assertIn("Notify", output)
        self.assertIn("?", output)

    def test_long_message_truncated_to_limit(self):
        ok = send_notify("가" * 5000)
        self.assertTrue(ok)
        text = _CaptureHandler.requests[0]["json"]["text"]
        self.assertLessEqual(len(text), 4000)
        self.assertTrue(text.endswith("…(잘림)"))

    def test_task_event_formats(self):
        task = TaskState(
            task_id="TASK-TEST", requirement="학생 상담 페이지 수정해",
            working_dir=".", task_mode="MODIFICATION",
            target_module=["sucui"],
            failure_stage="REVIEW",
            changed_files=["sucui/src/A.vue", "sucui/src/B.vue"],
        )
        task.operator_label = task.requirement
        sent: list[tuple] = []

        import notify as notify_mod
        orig = notify_mod.send_notify

        def fake_send(message, title=None, priority=None):
            sent.append((message, title, priority))
            return True

        notify_mod.send_notify = fake_send
        try:
            notify_task_event("start", task, attempt=1, max_attempts=3)
            notify_task_event("review_start", task, attempt=1, max_attempts=3)
            notify_task_event("fail", task, attempt=1, max_attempts=3)
            notify_task_event("success", task, attempt=2, max_attempts=3)
            notify_task_event("final", task, attempt=3, max_attempts=3)
        finally:
            notify_mod.send_notify = orig

        self.assertEqual(sent[0], (
            "학생 상담 페이지 수정해\n대상: sucui", "KKM · 작업 중 1/3", None,
        ))
        self.assertEqual(sent[1], (
            "학생 상담 페이지 수정해", "KKM · 리뷰 시작 1/3", None,
        ))
        self.assertEqual(sent[2], (
            "학생 상담 페이지 수정해\n보완 후 다시 작업합니다.\n다음 작업: 2/3",
            "KKM · 리뷰 보완 필요", "high",
        ))
        self.assertEqual(sent[3], (
            "학생 상담 페이지 수정해\n변경 파일: 2개", "KKM · 작업 완료", None,
        ))
        self.assertEqual(sent[4], (
            "학생 상담 페이지 수정해\n리뷰 단계에서 3회 시도 후 종료했습니다.",
            "KKM · 작업 실패", "high",
        ))

    def test_task_name_uses_explicit_short_label_and_never_requirement(self):
        from notify import _task_name
        task = TaskState(task_id="TASK-LONG", requirement="full requirement " * 2000, working_dir=".")
        self.assertEqual("TASK-LONG", _task_name(task))
        task.operator_label = "short title"
        self.assertEqual("short title", _task_name(task))
        task.operator_label = "X" * 200
        self.assertEqual(120, len(_task_name(task)))


if __name__ == "__main__":
    unittest.main()
