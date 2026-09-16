"""0.99.2.1 QA providers: Momentic (primary) and Stagehand v4 (fallback).

Providers produce ProviderOutcome evidence only. They never decide Job
SUCCESS, never write canonical task status, and never persist credentials.

Runtime boundaries:
- Momentic runs through integrations/momentic/run_momentic.bat (Node 24
  wrapper). The Product frontend Node 18 environment is never mutated (no
  ``nvm use``, no global Node switch).
- Stagehand runs through integrations/stagehand/run_stagehand.bat
  (Node 24 + tsx) with Browserbase + OpenCode Go / DeepSeek V4.1 Flash.
  OPENCODE_GO_SESSION_ID is generated per QA execution, never static.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any, Mapping

from runtime_safety import assert_secret_free_argv, scrub_secrets, safe_print as print
from harness_temp import scratch_root

from qa_browser_contract import (
    APPLICATION_FAILURE_CODES,
    BROWSER_BROWSERBASE,
    BROWSER_GOOGLE_CHROME,
    MODEL_DEEPSEEK_V41_FLASH,
    MODEL_PROVIDER_OPENCODE_GO,
    PROVIDER_MOMENTIC,
    PROVIDER_STAGEHAND,
    QA_ASSERTION_FAILED,
    QA_BROWSER_INFRA_FAILURE,
    QA_FLOW_FAILED,
    QA_PROVIDER_AUTH_FAILURE,
    QA_PROVIDER_COMPATIBILITY_ERROR,
    QA_PROVIDER_QUOTA_EXHAUSTED,
    QA_PROVIDER_SERVICE_UNAVAILABLE,
    QA_PROVIDER_TIMEOUT,
    QA_RESULT_CORRELATION_MISMATCH,
    QA_SCHEMA_VALIDATION_ERROR,
    QA_EXPECTED_ELEMENT_MISSING,
    ProviderOutcome,
    new_qa_execution_id,
    provider_session_id,
)

HARNESS_ROOT = Path(__file__).resolve().parent
MOMENTIC_DIR = HARNESS_ROOT / "integrations" / "momentic"
MOMENTIC_BAT = MOMENTIC_DIR / "run_momentic.bat"
MOMENTIC_CANARY = MOMENTIC_DIR / "canary" / "harness-canary-readonly.test.yaml"
STAGEHAND_DIR = HARNESS_ROOT / "integrations" / "stagehand"
STAGEHAND_BAT = STAGEHAND_DIR / "run_stagehand.bat"
STAGEHAND_RUNNER = "qa-runner.ts"

MOMENTIC_TIMEOUT = int(os.environ.get("KKM_QA_MOMENTIC_TIMEOUT", "900"))
STAGEHAND_TIMEOUT = int(os.environ.get("KKM_QA_STAGEHAND_TIMEOUT", "900"))


class QAScenarioDenied(RuntimeError):
    """Execution denied by the side-effect/action safety policy (pre-run)."""

    def __init__(self, reason: str, *, code: str = "QA_SCENARIO_FORBIDDEN"):
        self.reason = scrub_secrets(reason)[:500]
        self.code = str(code)
        super().__init__(self.code + ": " + self.reason)


# --- Shared classification -------------------------------------------------

_AUTH_MARKERS = ("auth", "not logged in", "401", "403", "unauthorized", "api key", "login required")
_QUOTA_MARKERS = ("quota", "429", "rate limit", "usage limit", "credit")
_UNAVAILABLE_MARKERS = ("502", "503", "504", "service unavailable", "server error", "econnrefused", "enotfound", "etimedout", "network")
_CONFIG_MARKERS = ("userconfigurationerror", "browser executable", "configuration", "not installed", "unsupported", "compatibility")
_TIMEOUT_MARKERS = ("timeout", "timed out")
_ASSERTION_MARKERS = ("assertionfailure", "assert", "does not contain", "expected", "element is not visible")


def classify_provider_failure(raw: str) -> str:
    text = scrub_secrets(str(raw or "")).lower()
    if any(marker in text for marker in _AUTH_MARKERS):
        return QA_PROVIDER_AUTH_FAILURE
    if any(marker in text for marker in _QUOTA_MARKERS):
        return QA_PROVIDER_QUOTA_EXHAUSTED
    if any(marker in text for marker in _TIMEOUT_MARKERS):
        return QA_PROVIDER_TIMEOUT
    if any(marker in text for marker in _CONFIG_MARKERS):
        return QA_PROVIDER_COMPATIBILITY_ERROR
    if any(marker in text for marker in _UNAVAILABLE_MARKERS):
        return QA_PROVIDER_SERVICE_UNAVAILABLE
    if any(marker in text for marker in _ASSERTION_MARKERS):
        return QA_ASSERTION_FAILED
    return QA_BROWSER_INFRA_FAILURE


def _qa_env(extra: Mapping[str, str] | None = None) -> dict[str, str]:
    env = {name: value for name, value in os.environ.items() if isinstance(value, str)}
    # Harness entry points already clear these; QA children must not inherit
    # them either (PYTHONPATH also trips the secret-name argv guard).
    for name in ("PYTHONPATH", "PYTHONHOME"):
        env.pop(name, None)
    for name, value in (extra or {}).items():
        env[str(name)] = str(value)
    return env


def _run(command: list[str], *, timeout: int, cwd: Path) -> tuple[int, str, str]:
    argv = [str(part) for part in command]
    child_env = _qa_env()
    assert_secret_free_argv(argv, env=child_env)
    completed = subprocess.run(
        argv,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
        cwd=str(cwd),
        env=child_env,
    )
    return (completed.returncode,
            scrub_secrets(completed.stdout or ""),
            scrub_secrets(completed.stderr or ""))


# --- Momentic ---------------------------------------------------------------

def _momentic_yaml_steps(scenario) -> list[dict[str, Any]]:
    steps: list[dict[str, Any]] = []
    for step in scenario.steps:
        action = str(step.get("action", "")).strip().lower()
        value = str(step.get("value", "")).strip()
        if action == "goto" and value:
            steps.append({"navigate": value})
        elif action == "click" and value:
            steps.append({"click": value})
        elif action == "type" and value:
            steps.append({"fill": {"text": str(step.get("text", ""))[:500], "into": value}})
        elif action == "assert_contains" and value:
            steps.append({"checkPageContains": value})
        elif action == "assert" and value:
            steps.append({"assert": value})
    if not steps:
        # A read-only scenario with no explicit steps asserts its acceptance.
        for item in scenario.acceptance:
            steps.append({"checkPageContains": item})
    return steps


def materialize_momentic_test(scenario, target_dir: Path) -> Path:
    """Canonical QAScenario -> provider-specific Momentic YAML materialization."""
    lines = [
        "# Generated QA materialization from the canonical QAScenario.",
        "# The canonical requirement is the vendor-neutral QAScenario contract, never this file.",
        f"# Side-effect class = {scenario.side_effect_class}",
        "fileType: momentic/test/v2",
        f"id: {scenario.scenario_id.lower()[:60]}",
        f"name: {scenario.title}",
    ]
    if scenario.start_url:
        lines.append(f"url: {scenario.start_url}")
    lines.append("steps:")
    for step in _momentic_yaml_steps(scenario):
        lines.append(f"  - {json.dumps(step, ensure_ascii=False)}")
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / f"{scenario.scenario_id.lower()[:60]}.test.yaml"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


MOMENTIC_QA_DIR = MOMENTIC_DIR / "qa"


def _execution_dir(qa_execution_id: str, provider: str) -> Path:
    """Brand-new per-execution artifact boundary.

    Every independent QA execution gets its own directory keyed by the
    canonical qa_execution_id; no two executions ever share a result file
    (the old `qa-*-adhoc` static-path reuse is forbidden).
    """
    exec_dir = scratch_root("runtime") / qa_execution_id / provider
    exec_dir.mkdir(parents=True, exist_ok=True)
    return exec_dir


def _write_execution_manifest(exec_dir: Path, *, qa_execution_id: str, scenario) -> dict:
    """Harness-side identity manifest bound to this execution's artifacts."""
    manifest = {
        "schema_revision": "qa-execution-manifest/1",
        "qa_execution_id": qa_execution_id,
        "scenario_id": scenario.scenario_id,
        "scenario_hash": scenario.scenario_hash,
        "started_epoch": time.time(),
    }
    (exec_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest


def _remove_stale(path: Path) -> None:
    """Never trust pre-existing provider output at a result path."""
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


def _parse_momentic_report(report_path: Path, scenario_id: str, *,
                           qa_execution_id: str = "",
                           started_epoch: float = 0.0) -> ProviderOutcome | None:
    if not report_path.is_file():
        return None
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ProviderOutcome(
            provider=PROVIDER_MOMENTIC, status="REPORT_INVALID", passed=False,
            failure_code=QA_SCHEMA_VALIDATION_ERROR,
            failure_message="momentic json report unreadable",
            browser=BROWSER_GOOGLE_CHROME,
        )
    if started_epoch and report_path.stat().st_mtime + 5 < started_epoch:
        return ProviderOutcome(
            provider=PROVIDER_MOMENTIC, status="STALE_REPORT", passed=False,
            failure_code=QA_RESULT_CORRELATION_MISMATCH,
            failure_message="report predates the current qa execution",
            browser=BROWSER_GOOGLE_CHROME,
        )
    summary = report.get("summary") or {}
    expected_id = scenario_id.lower()[:60]
    runs = report.get("runs") or []
    matching = [run for run in runs
                if str(run.get("testId", "")).lower() == expected_id]
    if not matching:
        # The requested scenario is not in this report. A foreign run's PASS
        # (or a stale artifact) must never satisfy the current execution.
        return ProviderOutcome(
            provider=PROVIDER_MOMENTIC, status="SCENARIO_NOT_IN_REPORT", passed=False,
            failure_code=QA_RESULT_CORRELATION_MISMATCH,
            failure_message=(
                f"report has no run for requested scenario '{expected_id}' "
                f"(qa_execution_id={qa_execution_id or 'n/a'}); stale/foreign "
                "result rejected"
            ),
            browser=BROWSER_GOOGLE_CHROME,
        )
    target = matching[0]
    status = str(target.get("status", "")).upper()
    recovered = bool(target.get("recovered", False))
    failure = target.get("failure") or {}
    raw_reason = str(failure.get("reason", "")) or str(failure.get("message", ""))
    executed = int(summary.get("executed", 0) or 0)
    passed = status == "PASSED" and executed > 0 and not summary.get("failed")
    assertions = [{
        "name": str(target.get("testName", scenario_id)),
        "passed": passed,
        "detail": "" if passed else scrub_secrets(str(failure.get("message", "")))[:1000],
    }]
    return ProviderOutcome(
        provider=PROVIDER_MOMENTIC,
        status=status or "UNKNOWN",
        passed=passed,
        recovery_used=recovered,
        failure_code="" if passed else classify_provider_failure(raw_reason or "assertion"),
        failure_message=scrub_secrets(raw_reason)[:1000],
        assertions=assertions,
        artifacts=[{"path": str(report_path)}],
        browser=BROWSER_GOOGLE_CHROME,
        usage={
            "attempt_count": int(target.get("attempts", 1) or 1),
        },
        provider_metadata={
            "run_id": scrub_secrets(str(target.get("runId", "")))[:80],
            "qa_execution_id": qa_execution_id,
        },
    )


def run_momentic(scenario, *, qa_execution_id: str = "") -> ProviderOutcome:
    """Execute one QAScenario through the Momentic CLI (read-only primary).

    One qa_execution_id correlates manifest, CLI invocation, report artifact
    and normalized outcome; the generated materialization is cleaned up after
    the run (temporary execution artifact, not canonical evidence).
    """
    if not MOMENTIC_BAT.is_file():
        return ProviderOutcome(
            provider=PROVIDER_MOMENTIC, status="RUNTIME_MISSING", passed=False,
            failure_code=QA_BROWSER_INFRA_FAILURE,
            failure_message="run_momentic.bat missing",
            browser=BROWSER_GOOGLE_CHROME,
        )
    execution_id = qa_execution_id or new_qa_execution_id()
    exec_dir = _execution_dir(execution_id, "momentic")
    report_dir = exec_dir / "reports"
    output_dir = exec_dir / "artifacts"
    report_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest = _write_execution_manifest(
        exec_dir, qa_execution_id=execution_id, scenario=scenario)
    report_path = report_dir / "app.momentic.json"
    _remove_stale(report_path)
    try:
        # Materialize inside the Momentic project so include globs, config and
        # cache resolution all stay project-scoped. The CLI positional path is
        # passed with native separators (forward slashes match 0 tests on win32).
        _remove_stale(MOMENTIC_QA_DIR / f"{scenario.scenario_id.lower()[:60]}.test.yaml")
        test_file = materialize_momentic_test(scenario, MOMENTIC_QA_DIR)
        test_arg = str(test_file.relative_to(MOMENTIC_DIR))
    except ValueError as exc:
        return ProviderOutcome(
            provider=PROVIDER_MOMENTIC, status="MATERIALIZATION_FAILED", passed=False,
            failure_code=QA_SCHEMA_VALIDATION_ERROR,
            failure_message=f"scenario materialization failed: {exc}",
            browser=BROWSER_GOOGLE_CHROME,
        )
    command = [
        str(MOMENTIC_BAT), "run", test_arg,
        "-y", "--disable-cache", "--browser", "chrome",
        "--reporter", "json", "--reporter-dir", str(report_dir),
        "--output-dir", str(output_dir),
    ]
    print(f"[QA][Momentic] run scenario={scenario.scenario_id} "
          f"qa_execution_id={execution_id}")
    try:
        code, stdout, stderr = _run(command, timeout=MOMENTIC_TIMEOUT, cwd=MOMENTIC_DIR)
    except subprocess.TimeoutExpired:
        return ProviderOutcome(
            provider=PROVIDER_MOMENTIC, status="TIMEOUT", passed=False,
            failure_code=QA_PROVIDER_TIMEOUT,
            failure_message=f"momentic exceeded {MOMENTIC_TIMEOUT}s",
            browser=BROWSER_GOOGLE_CHROME,
            provider_metadata={"qa_execution_id": execution_id},
        )
    except FileNotFoundError:
        return ProviderOutcome(
            provider=PROVIDER_MOMENTIC, status="RUNTIME_MISSING", passed=False,
            failure_code=QA_BROWSER_INFRA_FAILURE,
            failure_message="momentic runtime not found",
            browser=BROWSER_GOOGLE_CHROME,
            provider_metadata={"qa_execution_id": execution_id},
        )
    finally:
        # Generated provider input is a temporary execution artifact.
        _remove_stale(test_file)
    outcome = _parse_momentic_report(
        report_path, scenario.scenario_id,
        qa_execution_id=execution_id,
        started_epoch=float(manifest.get("started_epoch", 0.0) or 0.0))
    if outcome is None:
        # Fatal platform/config errors can exit before writing a report.
        raw = (stderr or stdout)[-2000:]
        return ProviderOutcome(
            provider=PROVIDER_MOMENTIC,
            status=f"EXIT_{code}",
            passed=False,
            failure_code=classify_provider_failure(raw) if raw else QA_BROWSER_INFRA_FAILURE,
            failure_message=(stderr or stdout).strip()[-1000:],
            browser=BROWSER_GOOGLE_CHROME,
            provider_metadata={"qa_execution_id": execution_id},
        )
    return outcome


def run_momentic_canary() -> ProviderOutcome:
    """Read-only example.com canary through the real Momentic runtime."""
    from qa_browser_contract import QAScenario
    canary = QAScenario(
        scenario_id="harness-readonly-canary",
        title="Harness Read-Only Canary (example.com)",
        acceptance=("Example Domain",),
        allowed_actions=("navigate", "assert"),
        forbidden_actions=("save", "delete", "send", "approval"),
        side_effect_class="READ_ONLY",
        start_url="https://example.com",
    )
    execution_id = new_qa_execution_id()
    exec_dir = _execution_dir(execution_id, "momentic")
    report_dir = exec_dir / "reports"
    output_dir = exec_dir / "artifacts"
    report_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest = _write_execution_manifest(
        exec_dir, qa_execution_id=execution_id, scenario=canary)
    report_path = report_dir / "app.momentic.json"
    _remove_stale(report_path)
    command = [
        str(MOMENTIC_BAT), "run", str(MOMENTIC_CANARY),
        "-y", "--disable-cache", "--browser", "chrome",
        "--reporter", "json", "--reporter-dir", str(report_dir),
        "--output-dir", str(output_dir),
    ]
    print(f"[QA][Momentic] read-only canary qa_execution_id={execution_id}")
    try:
        code, stdout, stderr = _run(command, timeout=MOMENTIC_TIMEOUT, cwd=MOMENTIC_DIR)
    except subprocess.TimeoutExpired:
        return ProviderOutcome(
            provider=PROVIDER_MOMENTIC, status="TIMEOUT", passed=False,
            failure_code=QA_PROVIDER_TIMEOUT,
            failure_message=f"canary exceeded {MOMENTIC_TIMEOUT}s",
            browser=BROWSER_GOOGLE_CHROME,
            provider_metadata={"qa_execution_id": execution_id},
        )
    except FileNotFoundError:
        return ProviderOutcome(
            provider=PROVIDER_MOMENTIC, status="RUNTIME_MISSING", passed=False,
            failure_code=QA_BROWSER_INFRA_FAILURE,
            failure_message="momentic runtime not found",
            browser=BROWSER_GOOGLE_CHROME,
            provider_metadata={"qa_execution_id": execution_id},
        )
    outcome = _parse_momentic_report(
        report_path, "harness-readonly-canary",
        qa_execution_id=execution_id,
        started_epoch=float(manifest.get("started_epoch", 0.0) or 0.0))
    if outcome is not None:
        return outcome
    raw = (stderr or stdout)[-2000:]
    return ProviderOutcome(
        provider=PROVIDER_MOMENTIC, status=f"EXIT_{code}", passed=False,
        failure_code=classify_provider_failure(raw) if raw else QA_BROWSER_INFRA_FAILURE,
        failure_message=(stderr or stdout).strip()[-1000:],
        browser=BROWSER_GOOGLE_CHROME,
        provider_metadata={"qa_execution_id": execution_id},
    )


# --- Stagehand v4 -----------------------------------------------------------

def _stagehand_base_outcome(**overrides) -> ProviderOutcome:
    base = {
        "provider": PROVIDER_STAGEHAND,
        "browser_provider": BROWSER_BROWSERBASE,
        "browser": BROWSER_BROWSERBASE,
        "model_provider": MODEL_PROVIDER_OPENCODE_GO,
        "model": MODEL_DEEPSEEK_V41_FLASH,
    }
    base.update(overrides)
    return ProviderOutcome(**base)


def _parse_stagehand_result(result_path: Path, scenario, *,
                            qa_execution_id: str) -> ProviderOutcome | None:
    """Parse + exact-correlate one Stagehand result artifact.

    ``result.scenario_id`` and ``result.qa_execution_id`` must exactly match
    the current execution; a stale ``result.json`` that survived from another
    run is rejected even when it says PASS.
    """
    if not result_path.is_file():
        return None
    try:
        payload = json.loads(result_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return _stagehand_base_outcome(
            status="REPORT_INVALID", passed=False,
            failure_code=QA_SCHEMA_VALIDATION_ERROR,
            failure_message="stagehand result json unreadable")
    if (str(payload.get("scenario_id", "")) != scenario.scenario_id
            or str(payload.get("qa_execution_id", "")) != qa_execution_id):
        return _stagehand_base_outcome(
            status="STALE_RESULT", passed=False,
            failure_code=QA_RESULT_CORRELATION_MISMATCH,
            failure_message=(
                f"result scenario/execution does not match current execution "
                f"(requested scenario='{scenario.scenario_id}' "
                f"qa_execution_id='{qa_execution_id}'); stale/foreign result "
                "rejected"
            ))
    raw_status = str(payload.get("status", ""))
    failure_code = str(payload.get("failure_code", ""))
    if not raw_status or (payload.get("passed") is None and raw_status not in {"PASS", "FAIL"}):
        return _stagehand_base_outcome(
            status="REPORT_INVALID", passed=False,
            failure_code=QA_SCHEMA_VALIDATION_ERROR,
            failure_message="stagehand result schema invalid")
    passed = bool(payload.get("passed", raw_status == "PASS"))
    if not passed and failure_code not in APPLICATION_FAILURE_CODES:
        failure_code = classify_provider_failure(payload.get("failure_message", ""))
    return _stagehand_base_outcome(
        status=raw_status,
        passed=passed,
        recovery_used=bool(payload.get("recovery_used", False)),
        failure_code="" if passed else failure_code,
        failure_message=scrub_secrets(str(payload.get("failure_message", "")))[:1000],
        assertions=list(payload.get("assertions") or [])[:40],
        artifacts=[{"path": str(result_path)}],
        browser=str(payload.get("browser", BROWSER_BROWSERBASE)) or BROWSER_BROWSERBASE,
        browser_version=str(payload.get("browser_version", "")),
        usage=dict(payload.get("usage") or {}),
        provider_metadata=dict(payload.get("provider_metadata") or {}),
    )


def run_stagehand(scenario, *, qa_execution_id: str = "") -> ProviderOutcome:
    """Execute one QAScenario through Stagehand v4 on Browserbase (fallback).

    Per-execution artifact boundary under ``<scratch>/<qa_execution_id>/stagehand``;
    the generated input is cleaned up after the run, the sanitized result is
    kept as evidence. The OpenCode Go session is
    ``harness-qa-<qa_execution_id>`` (never static).
    """
    if not STAGEHAND_BAT.is_file():
        return _stagehand_base_outcome(
            status="RUNTIME_MISSING", passed=False,
            failure_code=QA_BROWSER_INFRA_FAILURE,
            failure_message="run_stagehand.bat missing")
    execution_id = qa_execution_id or new_qa_execution_id()
    work = _execution_dir(execution_id, "stagehand")
    _write_execution_manifest(work, qa_execution_id=execution_id, scenario=scenario)
    input_path = work / "input.json"
    output_path = work / "result.json"
    # Never trust a pre-existing result at this path.
    _remove_stale(output_path)
    input_path.write_text(json.dumps({
        "qa_execution_id": execution_id,
        "session_id": provider_session_id(execution_id),
        "scenario": {
            "scenario_id": scenario.scenario_id,
            "title": scenario.title,
            "start_url": scenario.start_url,
            "acceptance": list(scenario.acceptance),
            "steps": [dict(step) for step in scenario.steps],
        },
    }, ensure_ascii=False, indent=2), encoding="utf-8")
    command = [
        str(STAGEHAND_BAT), STAGEHAND_RUNNER,
        "--input", str(input_path),
        "--output", str(output_path),
    ]
    print(f"[QA][Stagehand] run scenario={scenario.scenario_id} "
          f"session={provider_session_id(execution_id)}")
    try:
        code, stdout, stderr = _run(command, timeout=STAGEHAND_TIMEOUT, cwd=STAGEHAND_DIR)
    except subprocess.TimeoutExpired:
        return _stagehand_base_outcome(
            status="TIMEOUT", passed=False,
            failure_code=QA_PROVIDER_TIMEOUT,
            failure_message=f"stagehand exceeded {STAGEHAND_TIMEOUT}s",
            provider_metadata={"qa_execution_id": execution_id})
    except FileNotFoundError:
        return _stagehand_base_outcome(
            status="RUNTIME_MISSING", passed=False,
            failure_code=QA_BROWSER_INFRA_FAILURE,
            failure_message="stagehand runtime not found",
            provider_metadata={"qa_execution_id": execution_id})
    finally:
        # Generated provider input is a temporary execution artifact.
        _remove_stale(input_path)
    outcome = _parse_stagehand_result(output_path, scenario, qa_execution_id=execution_id)
    if outcome is not None:
        return outcome
    raw = (stderr or stdout)[-2000:]
    return _stagehand_base_outcome(
        status=f"EXIT_{code}", passed=False,
        failure_code=classify_provider_failure(raw) if raw else QA_BROWSER_INFRA_FAILURE,
        failure_message=(stderr or stdout).strip()[-1000:],
        provider_metadata={"qa_execution_id": execution_id})
