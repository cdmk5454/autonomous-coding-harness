"""UTF-8 subprocess transport for the no-tools Hermes Supervisor seam.

The caller supplies pinned runtime/worker identity and a host-owned receipt path.
Historical canary transport and receipts remain unchanged.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from bounded_transport import BoundedTransportError, run_bounded


class SupervisorTransportError(RuntimeError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


def invoke_json(request: str, *, python: Path, worker: Path, home: Path,
                receipt_path: Path, provider: str, model: str,
                api_mode: str = "chat_completions") -> str:
    if not isinstance(request, str) or len(request.encode("utf-8")) > 32768:
        raise SupervisorTransportError("SUPERVISOR_REQUEST_INVALID")
    python, worker, home, receipt_path = map(Path, (python, worker, home, receipt_path))
    if not python.is_file() or not worker.is_file() or not home.is_dir() or not receipt_path.parent.is_dir():
        raise SupervisorTransportError("SUPERVISOR_RUNTIME_UNAVAILABLE")
    allowed_env = ("PATH", "PATHEXT", "SYSTEMROOT", "WINDIR", "TEMP", "TMP",
                   "USERPROFILE", "APPDATA", "LOCALAPPDATA")
    env = {name: os.environ[name] for name in allowed_env if name in os.environ}
    env.update(HERMES_HOME=str(home), PYTHONUTF8="1", PYTHONIOENCODING="utf-8:strict")
    try:
        result = run_bounded([str(python), "-B", str(worker)],
            input_bytes=request.encode("utf-8"), cwd=worker.parent, env=env,
            timeout=120, output_limit=65536)
    except BoundedTransportError as exc:
        code = {'HOST_TRANSPORT_TIMEOUT':'SUPERVISOR_TRANSPORT_TIMEOUT',
                'HOST_TRANSPORT_OUTPUT_TOO_LARGE':'SUPERVISOR_TRANSPORT_OUTPUT_TOO_LARGE'}.get(
                    exc.code,'SUPERVISOR_RUNTIME_UNAVAILABLE')
        raise SupervisorTransportError(code) from exc
    try:
        # Decode in this thread. On Windows, text=True can lose reader-thread
        # decode exceptions and return stdout=None instead of raising them.
        stdout = result.stdout.decode("utf-8", errors="strict")
        stderr = result.stderr.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise SupervisorTransportError("SUPERVISOR_TRANSPORT_DECODE_FAILED") from exc
    if result.returncode:
        try:
            failure = json.loads(stdout)
        except ValueError:
            failure = {}
        code = ('SUPERVISOR_PROVIDER_UNAVAILABLE' if isinstance(failure,dict)
                and failure.get('error_kind') == 'PROVIDER' else 'SUPERVISOR_RUNTIME_ERROR')
        raise SupervisorTransportError(code)
    try:
        envelope = json.loads(stdout)
    except (TypeError, ValueError) as exc:
        raise SupervisorTransportError("SUPERVISOR_RESPONSE_INVALID") from exc
    if (not isinstance(envelope, dict) or envelope.get("tools") != 0
            or envelope.get("provider") != provider or envelope.get("model") != model
            or envelope.get("api_mode") != api_mode):
        raise SupervisorTransportError("SUPERVISOR_RUNTIME_IDENTITY_DRIFT")
    answer = envelope.get("response")
    if not isinstance(answer, str):
        raise SupervisorTransportError("SUPERVISOR_RESPONSE_INVALID")
    try:
        parsed = json.loads(answer)
    except ValueError as exc:
        raise SupervisorTransportError("SUPERVISOR_RESPONSE_INVALID") from exc
    if (not isinstance(parsed, dict) or not isinstance(envelope.get("session_id"), str)
            or not envelope["session_id"] or parsed.get("session_id") != envelope["session_id"]):
        raise SupervisorTransportError("SUPERVISOR_SESSION_ID_MISMATCH")
    receipt = {"timestamp_utc": datetime.now(timezone.utc).isoformat(),
               "request_sha256": hashlib.sha256(request.encode("utf-8")).hexdigest(),
               "provider": provider, "model": model, "session_id": envelope["session_id"],
               "tool_count": 0, "response_schema_keys": sorted(parsed)}
    with receipt_path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(receipt, ensure_ascii=False) + "\n")
    return answer


def production_advisor(control_root: Path, working_root: Path, *, binding_path: Path | None = None):
    """Bind the pinned local Supervisor only to its configured writable Control."""
    config_path = Path(binding_path or Path(__file__).with_name('supervisor-runtime.json'))
    if not config_path.is_file():
        return None
    try:
        config_bytes = config_path.read_bytes()
        config = json.loads(config_bytes.decode('utf-8'))
        fields = {'schema','control_root','working_root','python','worker','home',
                  'receipt_path','provider','model','runtime_version','api_mode'}
        if set(config) != fields or config['schema'] != 'supervisor-runtime-binding/1':
            raise ValueError('SUPERVISOR_BINDING_SCHEMA_INVALID')
        if (Path(config['control_root']).resolve() != Path(control_root).resolve()
                or Path(config['working_root']).resolve() != Path(working_root).resolve()):
            return None
        python, worker, home, receipt = (Path(config[key]) for key in
                                         ('python','worker','home','receipt_path'))
        if (not python.is_file() or not worker.is_file() or not home.is_dir()
                or not receipt.parent.is_dir() or worker.resolve() != Path(__file__).with_name('hermes_no_tools_worker.py').resolve()):
            raise ValueError('SUPERVISOR_BINDING_PATH_INVALID')
        from supervisor_proposal import HermesSupervisorAdvisor
        revision = 'utf8-strict/' + hashlib.sha256(
            config_bytes + worker.read_bytes()).hexdigest()[:16]
        return HermesSupervisorAdvisor(
            invoke_json=lambda request: invoke_json(request,python=python,worker=worker,home=home,
                receipt_path=receipt,provider=config['provider'],model=config['model'],
                api_mode=config['api_mode']),
            provider=config['provider'],model=config['model'],
            runtime_version=config['runtime_version'],adapter_revision=revision)
    except (OSError, UnicodeError, ValueError, TypeError, KeyError) as exc:
        raise SupervisorTransportError('SUPERVISOR_BINDING_INVALID') from exc
