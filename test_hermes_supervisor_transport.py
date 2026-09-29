import json
from pathlib import Path
import sys
import tempfile
import unittest

from hermes_supervisor_transport import SupervisorTransportError, invoke_json, production_advisor
from supervisor_proposal import HermesSupervisorAdvisor, SupervisorGateError


class HermesUtf8TransportTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.worker = self.root / "worker.py"
        self.receipt = self.root / "receipts.jsonl"

    def invoke(self):
        return invoke_json("{}", python=Path(sys.executable), worker=self.worker,
                           home=self.root, receipt_path=self.receipt,
                           provider="zai", model="glm-5")

    def test_utf8_response_retains_session_and_provider_identity(self):
        answer = {"session_id": "SESSION-1", "reason": "정상 응답"}
        envelope = {"response": json.dumps(answer, ensure_ascii=False),
                    "session_id": "SESSION-1", "provider": "zai", "model": "glm-5",
                    "tools": 0, "api_mode": "chat_completions"}
        self.worker.write_text("import json\nprint(json.dumps(" + repr(envelope) + ", ensure_ascii=False))\n",
                               encoding="utf-8")
        self.assertEqual(answer, json.loads(self.invoke()))
        receipt = json.loads(self.receipt.read_text(encoding="utf-8"))
        self.assertEqual(("zai", "glm-5", "SESSION-1"),
                         (receipt["provider"], receipt["model"], receipt["session_id"]))

    def test_malformed_utf8_fails_closed_without_receipt(self):
        for fd in (1, 2):
            with self.subTest(fd=fd):
                self.worker.write_text(f"import os\nos.write({fd}, b'\\xff')\n", encoding="utf-8")
                with self.assertRaisesRegex(SupervisorTransportError,
                                            "SUPERVISOR_TRANSPORT_DECODE_FAILED"):
                    self.invoke()
                self.assertFalse(self.receipt.exists())

    def test_provider_failure_is_distinct_from_decode_failure(self):
        self.worker.write_text("import json\nprint(json.dumps({'error_kind':'PROVIDER'}))\nraise SystemExit(1)\n", encoding="utf-8")
        with self.assertRaisesRegex(SupervisorTransportError, "SUPERVISOR_PROVIDER_UNAVAILABLE"):
            self.invoke()
        self.assertFalse(self.receipt.exists())

    def test_runtime_failure_is_distinct_from_provider_failure(self):
        self.worker.write_text("raise SystemExit(1)\n", encoding="utf-8")
        with self.assertRaisesRegex(SupervisorTransportError, "SUPERVISOR_RUNTIME_ERROR"):
            self.invoke()

    def test_advisor_preserves_bounded_transport_failure_code(self):
        def fail(_request):
            raise SupervisorTransportError("SUPERVISOR_TRANSPORT_DECODE_FAILED")
        advisor = HermesSupervisorAdvisor(fail, "zai", "glm-5", "0.21.3", "utf8/1")
        with self.assertRaisesRegex(SupervisorGateError, "SUPERVISOR_TRANSPORT_DECODE_FAILED"):
            advisor.recommend({"request_sha256": "x"})

    def test_actual_production_constructor_binds_pinned_utf8_transport(self):
        configuration=json.loads(Path(__file__).with_name('supervisor-runtime.json').read_text(encoding='utf-8'))
        configuration['worker']=str(Path(__file__).with_name('hermes_no_tools_worker.py'))
        binding=self.root/'binding.json'
        binding.write_text(json.dumps(configuration),encoding='utf-8')
        advisor = production_advisor(Path(r'D:\kkm_test\.control'),Path(r'D:\inha_dev'),binding_path=binding)
        self.assertIsNotNone(advisor)
        identity = advisor.identity()
        self.assertEqual(('zai','glm-5'),(identity['provider'],identity['model']))
        self.assertTrue(identity['adapter_revision'].startswith('utf8-strict/'))
        self.assertIsNone(production_advisor(self.root,self.root))


if __name__ == "__main__":
    unittest.main()
