import hashlib
import json
import os
from contextlib import closing
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import time
import unittest
from unittest import mock

import harness_temp
from native_tooling import McpCredentialError, migrate_mcp_secret_argv_config
from runtime_safety import SecretArgvError, assert_secret_free_argv, scrub_secrets
from worker import TimeoutConfig, _run_with_activity_timeout


SYNTHETIC_SECRET = "TEST_SECRET_DO_NOT_EXPOSE_099_ARGV"


def _windows_command_line(pid: int) -> str:
    powershell = shutil.which("powershell") or shutil.which("pwsh")
    if os.name != "nt" or not powershell:
        raise unittest.SkipTest("Windows process command-line inventory unavailable")
    result = subprocess.run(
        [powershell, "-NoProfile", "-Command",
         f"(Get-CimInstance Win32_Process -Filter 'ProcessId={pid}').CommandLine"],
        capture_output=True, text=True, encoding="utf-8", errors="replace",
        timeout=15, check=True,
    )
    return result.stdout


class SecretArgvSecurityTests(unittest.TestCase):
    def setUp(self):
        self.temp = harness_temp.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_a_b_e_secret_uses_env_and_is_absent_from_parent_and_child_argv(self):
        child_pid_file = self.root / "child.pid"
        child_code = (
            "import os,time;"
            f"open({str(child_pid_file)!r},'w').write(str(os.getpid()));"
            "time.sleep(20)"
        )
        parent_code = (
            "import hashlib,os,subprocess,sys;"
            "print(hashlib.sha256(os.environ['CONTEXT7_API_KEY'].encode()).hexdigest(),flush=True);"
            f"p=subprocess.Popen([sys.executable,'-c',{child_code!r}],env=os.environ.copy());"
            "p.wait()"
        )
        env = os.environ.copy()
        env["CONTEXT7_API_KEY"] = SYNTHETIC_SECRET
        command = [sys.executable, "-c", parent_code]
        assert_secret_free_argv(command, env=env)
        proc = subprocess.Popen(
            command, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, encoding="utf-8", errors="replace",
        )
        self.addCleanup(lambda: proc.kill() if proc.poll() is None else None)
        digest = proc.stdout.readline().strip() if proc.stdout else ""
        self.assertEqual(hashlib.sha256(SYNTHETIC_SECRET.encode()).hexdigest(), digest)
        deadline = time.monotonic() + 10
        while not child_pid_file.exists() and time.monotonic() < deadline:
            time.sleep(0.05)
        self.assertTrue(child_pid_file.exists())
        child_pid = int(child_pid_file.read_text(encoding="utf-8"))
        try:
            self.assertNotIn(SYNTHETIC_SECRET, _windows_command_line(proc.pid))
            self.assertNotIn(SYNTHETIC_SECRET, _windows_command_line(child_pid))
        finally:
            subprocess.run(
                ["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                capture_output=True, check=False,
            )
            proc.wait(timeout=10)
            if proc.stdout:
                proc.stdout.close()
            if proc.stderr:
                proc.stderr.close()

    def test_c_guard_rejects_secret_values_before_spawn_and_redacts_diagnostics(self):
        with self.assertRaises(SecretArgvError) as caught:
            assert_secret_free_argv([sys.executable, "--api-key", SYNTHETIC_SECRET])
        self.assertEqual("TECHNICAL_EXECUTION_RETRY", caught.exception.retry_domain)
        self.assertEqual(0, caught.exception.product_retry_delta)
        self.assertNotIn(SYNTHETIC_SECRET, str(caught.exception))
        with self.assertRaises(SecretArgvError):
            assert_secret_free_argv(
                [sys.executable, SYNTHETIC_SECRET],
                env={"PROVIDER_TOKEN": SYNTHETIC_SECRET},
            )
        assert_secret_free_argv([sys.executable, "--api-key"])

        rendered = scrub_secrets(
            f'command --api-key "{SYNTHETIC_SECRET}" --password={SYNTHETIC_SECRET}',
            env={},
        )
        self.assertNotIn(SYNTHETIC_SECRET, rendered)
        self.assertGreaterEqual(rendered.count("****"), 2)
        context_token = "ctx7" + "sk-" + "00000000-0000-0000-0000-000000000000"
        self.assertEqual("****", scrub_secrets(context_token, env={}))

        with mock.patch("worker.subprocess.Popen") as popen:
            with self.assertRaises(SecretArgvError):
                _run_with_activity_timeout(
                    [sys.executable, "--api-key", SYNTHETIC_SECRET], None,
                    str(self.root), TimeoutConfig(), "analysis",
                )
            popen.assert_not_called()

    def test_d_secret_is_not_persisted_to_control_tasks_artifacts_or_manifest(self):
        safe = scrub_secrets(f"tool --api-key {SYNTHETIC_SECRET}", env={})
        control = self.root / "control.db"
        with closing(sqlite3.connect(control)) as conn:
            conn.execute("CREATE TABLE diagnostics(value TEXT NOT NULL)")
            conn.execute("INSERT INTO diagnostics(value) VALUES (?)", (safe,))
            conn.commit()
        for name in (".tasks", "artifacts"):
            directory = self.root / name
            directory.mkdir()
            (directory / "record.json").write_text(
                json.dumps({"diagnostic": safe}), encoding="utf-8"
            )
        (self.root / "runtime-manifest.json").write_text(
            json.dumps({"command": safe}), encoding="utf-8"
        )
        for path in self.root.rglob("*"):
            if path.is_file():
                self.assertNotIn(SYNTHETIC_SECRET.encode(), path.read_bytes(), str(path))

    def test_config_migration_moves_secret_to_supported_server_env(self):
        config = self.root / "mcp.json"
        config.write_text(json.dumps({
            "mcpServers": {
                "context7": {
                    "command": "npx",
                    "args": ["-y", "@upstash/context7-mcp", "--api-key", SYNTHETIC_SECRET],
                }
            }
        }), encoding="utf-8")
        result = migrate_mcp_secret_argv_config(
            config, "context7", "--api-key", "CONTEXT7_API_KEY"
        )
        migrated = json.loads(config.read_text(encoding="utf-8"))
        server = migrated["mcpServers"]["context7"]
        self.assertEqual(["-y", "@upstash/context7-mcp"], server["args"])
        self.assertEqual(SYNTHETIC_SECRET, server["env"]["CONTEXT7_API_KEY"])
        self.assertEqual("ENVIRONMENT", result["credential_transport"])
        self.assertNotIn(SYNTHETIC_SECRET, json.dumps(result))
        if os.name == "nt":
            acl = subprocess.run(
                ["icacls", str(config)], capture_output=True, text=True,
                encoding="utf-8", errors="replace", timeout=15, check=True,
            ).stdout
            self.assertNotIn("CodexSandboxUsers", acl)

    def test_f_missing_credential_is_explicit_and_does_not_spend_product_retry(self):
        config = self.root / "mcp.json"
        config.write_text(json.dumps({
            "mcpServers": {"context7": {"command": "npx", "args": []}}
        }), encoding="utf-8")
        with self.assertRaises(McpCredentialError) as caught:
            migrate_mcp_secret_argv_config(
                config, "context7", "--api-key", "CONTEXT7_API_KEY"
            )
        self.assertEqual("MCP_CREDENTIAL_MISSING", caught.exception.code)
        self.assertEqual("TECHNICAL_EXECUTION_RETRY", caught.exception.retry_domain)
        self.assertEqual(0, caught.exception.product_retry_delta)


if __name__ == "__main__":
    unittest.main()
