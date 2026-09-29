import base64
import copy
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch
from types import SimpleNamespace

from opencode_boundary import (IMAGE, MODEL, IsolatedOpenCodeRuntimeAdapter,
                               OpenCodeAdapterError, validate_tree)
from opencode_provider_relay import validate_request, NoRedirect


class BoundaryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.candidate = self.root / "candidate"
        self.context = self.root / "context"
        self.candidate.mkdir()
        self.context.mkdir()
        self.adapter = IsolatedOpenCodeRuntimeAdapter.__new__(IsolatedOpenCodeRuntimeAdapter)
        self.adapter.identity = dict(execution_id="execution", execution_epoch="epoch", candidate_id="candidate", job_id="job", attempt_id="attempt")
        self.adapter.cid = "a" * 64
        self.adapter.candidate = self.candidate
        self.adapter.context = self.context
        self.adapter.binary = self.root / "opencode"
        self.adapter.readonly_candidate = False
        self.adapter._started = True
        self.adapter._removed = False
        self.adapter._receipt = {"quiesced": False}
        self.adapter._relay = None
        self.item = {"Id": self.adapter.cid, "Image": IMAGE,
                     "Config": {"Labels": {"harness." + k: v for k, v in self.adapter.identity.items()}},
                     "HostConfig": {"NetworkMode": "none", "ReadonlyRootfs": True,
                                    "CapDrop": ["ALL"], "SecurityOpt": ["no-new-privileges"],
                                    "PidsLimit": 128, "Memory": 1073741824, "NanoCpus": 1000000000,
                                    "Tmpfs": {"/tmp": "rw,nosuid,size=256m", "/root": "rw,nosuid,size=512m", "/opt/data": "rw,noexec,nosuid,size=256m"}},
                     "Mounts": [{"Type": "bind", "Destination": key, "Source": str(path), "RW": rw}
                                for key, (path, rw) in self.adapter._expected_mounts().items()]}

    def tearDown(self):
        self.temp.cleanup()

    def test_exact_mounts_and_ownership(self):
        self.adapter.verify_inspection(self.item)
        for mutate in (lambda x: x["Mounts"].append({"Destination": "/canonical"}),
                       lambda x: x["Mounts"][1].update(RW=True),
                       lambda x: x["Mounts"][0].update(Source="D:/canonical"),
                       lambda x: x["Config"]["Labels"].update({"harness.execution_epoch": "other"}),
                       lambda x: x["HostConfig"].update(NetworkMode="bridge"),
                       lambda x: x.update(Image="substitute")):
            altered = copy.deepcopy(self.item)
            mutate(altered)
            with self.assertRaises(OpenCodeAdapterError):
                self.adapter.verify_inspection(altered)

    def test_secret_and_hardlink_rejected(self):
        secret = self.candidate / ".env.production"
        secret.write_text("not-a-secret")
        with self.assertRaises(OpenCodeAdapterError):
            validate_tree(self.candidate)
        secret.unlink()
        source = self.context / "source"
        source.write_text("test")
        os.link(source, self.candidate / "linked")
        with self.assertRaises(OpenCodeAdapterError):
            validate_tree(self.candidate)

    def test_route_model_body_and_redirect(self):
        request = {"method": "POST", "path": "/zen/go/v1/chat/completions",
                   "body": base64.b64encode(b'{"model":"deepseek-v4.1-flash"}').decode()}
        validate_request(request)
        for change in ({"method": "GET"}, {"path": "/zen/v1/chat/completions"},
                       {"body": base64.b64encode(b'{"model":"other"}').decode()}, {"body": "!!!"}):
            with self.assertRaises(ValueError):
                validate_request({**request, **change})
        self.assertIsNone(NoRedirect().redirect_request(None, None, 302, "", {}, "https://other"))

    def test_process_crash_is_not_quiescence(self):
        self.assertFalse(self.adapter.confirm_quiescence())
        with patch.object(self.adapter, "_inspect", return_value={**self.item, "State": {"Running": False}}):
            with self.assertRaises(OpenCodeAdapterError):
                self.adapter.request("GET", "/global/health")
        self.assertFalse(self.adapter.confirm_quiescence())

    def test_daemon_failure_never_confirms_quiescence(self):
        with patch.object(self.adapter, "_inspect", return_value=self.item), patch.object(self.adapter, "_docker", side_effect=OpenCodeAdapterError("unavailable")):
            with self.assertRaises(OpenCodeAdapterError):
                self.adapter.stop_local_server()
        self.assertFalse(self.adapter.confirm_quiescence())

    def test_cleanup_owned_container_after_mount_drift(self):
        altered = copy.deepcopy(self.item)
        altered["Mounts"] = []
        with patch.object(self.adapter, "_inspect", return_value=altered), patch.object(self.adapter, "_docker", return_value=SimpleNamespace(stdout="")), patch.object(self.adapter, "_save_receipt"):
            self.adapter.stop_local_server()
        self.assertTrue(self.adapter.confirm_quiescence())

    def test_model_cwd_binding(self):
        self.adapter._check_call(self.candidate, MODEL)
        for cwd, model in ((self.context, MODEL), (self.candidate, "other")):
            with self.assertRaises(OpenCodeAdapterError):
                self.adapter._check_call(cwd, model)

    def test_evidence_canonical_paths_rejected_before_validation_or_write(self):
        self.adapter.canonical = self.root / "canonical"
        self.adapter.canonical.mkdir()
        for destination in (self.adapter.canonical, self.adapter.canonical / "receipts",
                            self.root / "other" / ".." / "canonical" / "receipts"):
            self.adapter.evidence_dir = destination
            with self.assertRaisesRegex(OpenCodeAdapterError, "CANONICAL_EXPOSURE"):
                self.adapter._validate_paths()
            with self.assertRaisesRegex(OpenCodeAdapterError, "CANONICAL_EXPOSURE"):
                self.adapter._save_receipt()
        self.assertEqual(list(self.adapter.canonical.iterdir()), [])

    def test_evidence_unsafe_ancestor_rejected(self):
        self.adapter.canonical = self.root / "canonical"
        self.adapter.evidence_dir = self.context / "new-receipts"
        original = Path.lstat
        def junction_stat(path):
            if path == self.context:
                return SimpleNamespace(st_file_attributes=0x400, st_mode=original(path).st_mode)
            return original(path)
        with patch.object(Path, "lstat", junction_stat):
            with self.assertRaisesRegex(OpenCodeAdapterError, "EVIDENCE_LINK_REJECTED"):
                self.adapter._validate_evidence_path()
        with patch.object(Path, "is_symlink", lambda path: path == self.context):
            with self.assertRaisesRegex(OpenCodeAdapterError, "EVIDENCE_LINK_REJECTED"):
                self.adapter._validate_evidence_path()

    def test_evidence_resolved_candidate_overlap_rejected(self):
        self.adapter.canonical = self.root / "canonical"
        sibling = self.root / "sibling"
        sibling.mkdir()
        for destination in (self.candidate, self.candidate / "receipts",
                            sibling / ".." / "candidate" / "receipts"):
            self.adapter.evidence_dir = destination
            with self.assertRaisesRegex(OpenCodeAdapterError, "CONTROL_PATH_EXPOSED"):
                self.adapter._validate_paths()
            with self.assertRaisesRegex(OpenCodeAdapterError, "CONTROL_PATH_EXPOSED"):
                self.adapter._save_receipt()
        self.assertEqual(list(self.candidate.iterdir()), [])

    def test_bridge_preserves_http_404_and_transport_failures(self):
        self.adapter.timeout = 1
        self.adapter.password = ""
        self.adapter._relay = SimpleNamespace(poll=lambda: None)
        inspected = {**self.item, "State": {"Running": True}}
        for envelope, state, detail in (({"status": 404, "body": ""}, "LOST", "SESSION_NOT_FOUND"),
                                       ({"transport_error": True}, "UNKNOWN", "OPENCODE_TRANSPORT_ERROR")):
            result = SimpleNamespace(returncode=0, stdout=json.dumps(envelope))
            with patch.object(self.adapter, "_inspect", return_value=inspected), patch.object(self.adapter, "_docker", return_value=result):
                query = self.adapter.query_session("ses_missing", cwd=self.candidate)
            self.assertEqual((query.state, query.detail), (state, detail))


@unittest.skipUnless(os.environ.get("OPENCODE_BOUNDARY_DOCKER_TEST") == "1", "explicit real Docker fixture opt-in")
class DockerBoundaryTests(unittest.TestCase):
    def test_real_lifecycle(self):
        root = Path(__file__).resolve().parent.parent
        with tempfile.TemporaryDirectory(dir=root) as directory:
            fixture = Path(directory)
            candidate, context = fixture / "candidate", fixture / "context"
            candidate.mkdir()
            context.mkdir()
            (candidate / "app.py").write_text("value = 1\n")
            adapter = IsolatedOpenCodeRuntimeAdapter(
                execution_id="fixture", execution_epoch=fixture.name, candidate_id="candidate",
                job_id="job", attempt_id="attempt", candidate_dir=candidate, context_dir=context,
                runtime_binary=root / "opencode-runtime/opencode", provider_catalog=root / "provider-catalog.json",
                credential_loader=lambda: json.loads((Path.home() / ".local/share/opencode/auth.json").read_text())["opencode-go"]["key"], canonical_root=root / "absent-canonical",
                evidence_dir=root / "adapter-test-receipts", docker_executable=r"C:\Program Files\Docker\Docker\resources\bin\docker.exe")
            try:
                adapter.start_local_server(cwd=candidate)
                session = adapter.execute_invocation(adapter.create_session("", cwd=candidate))
                self.assertTrue(session["id"])
                missing = adapter.query_session("ses_missing", cwd=candidate)
                self.assertEqual((missing.state, missing.detail), ("LOST", "SESSION_NOT_FOUND"))
                self.assertFalse(adapter.confirm_quiescence())
                if os.environ.get("OPENCODE_BOUNDARY_LIVE_TEST") == "1":
                    adapter.execute_invocation(adapter.submit_turn(session["id"], "Change ONLY /workspace/app.py to contain exactly value = 2 followed by a newline. Do not add other files. Report completion.", cwd=candidate, model=MODEL))
                    deadline = time.monotonic() + 150
                    while time.monotonic() < deadline:
                        messages = adapter.request("GET", "/session/" + session["id"] + "/message?limit=100")
                        self.assertFalse(any(m.get("info", {}).get("error") for m in messages), adapter.boundary_receipt()["provider_requests"])
                        finished = any(m.get("info", {}).get("role") == "assistant" and m.get("info", {}).get("time", {}).get("completed") for m in messages)
                        if finished and (candidate / "app.py").read_text() == "value = 2\n":
                            break
                        time.sleep(1)
                    self.assertEqual((candidate / "app.py").read_text(), "value = 2\n")
                    self.assertTrue(finished)
                    self.assertTrue(any(r.get("status") == 200 for r in adapter.boundary_receipt()["provider_requests"]))
                denied = adapter._docker("exec", adapter.cid, "sh", "-c", "echo forbidden > /context/forbidden", check=False)
                self.assertNotEqual(denied.returncode, 0)
            finally:
                adapter.stop_local_server()
            self.assertTrue(adapter.confirm_quiescence())


if __name__ == "__main__":
    unittest.main()
