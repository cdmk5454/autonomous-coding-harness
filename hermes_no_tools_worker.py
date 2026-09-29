"""Pinned no-tools Hermes Supervisor child. JSON arrives on stdin only."""

import json
import os
import sys
from pathlib import Path

RUNTIME = Path(r'D:\hermes\runtime-v2026.9.14')
sys.path.insert(0, str(RUNTIME))


def main():
    raw = sys.stdin.buffer.read(32769)
    if len(raw) > 32768:
        raise ValueError('SUPERVISOR_REQUEST_TOO_LARGE')
    request = json.loads(raw.decode('utf-8', errors='strict'))
    identity = request['runtime_identity']
    if (request.get('allowed_tools') != [] or request.get('product_write') is not False
            or request.get('control_write') is not False or request.get('publication') is not False
            or request.get('subagents') is not False):
        raise ValueError('SUPERVISOR_AUTHORITY_INVALID')
    provider, model = identity['provider'], identity['model']
    home = Path(os.environ['HERMES_HOME'])
    if not home.is_dir() or not RUNTIME.is_dir():
        raise ValueError('SUPERVISOR_RUNTIME_UNAVAILABLE')
    from run_agent import AIAgent
    agent = AIAgent(provider=provider, model=model, enabled_toolsets=[], max_iterations=1,
                    quiet_mode=True, skip_context_files=True, skip_memory=True,
                    skip_background_review=True, save_trajectories=False,
                    checkpoints_enabled=False)
    try:
        if agent.tools or agent.valid_tool_names:
            raise ValueError('SUPERVISOR_TOOLS_PRESENT')
        prompt = (
            'You are an advisory-only Harness Supervisor with no tools or Product authority. '
            'Return one JSON object with exactly: policy_version, request_sha256, '
            'recommended_action, reason, evidence_refs, session_id. Copy policy_version '
            'from input schema and request_sha256 from projection. Choose recommended_action '
            'only from legal_next_actions and cite at least one listed evidence ref. '
            'Set session_id to the current native session ID. At ADMISSION, CONTINUE only '
            'allows frozen execution to begin; it is not Product success. HOLD or REQUEST_HUMAN '
            'on missing or conflicting authority. Never claim Product success. Input JSON:\n'
            + raw.decode('utf-8', errors='strict'))
        answer = agent.chat(prompt)
        print(json.dumps({'response':answer,'session_id':str(agent.session_id),
            'provider':str(agent.provider),'model':str(agent.model),
            'tools':len(agent.tools),'api_mode':str(agent.api_mode)},ensure_ascii=False))
    finally:
        agent.close()


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(json.dumps({'error_kind':'PROVIDER' if type(exc).__name__ in {
            'APIError','APIConnectionError','APITimeoutError','AuthenticationError',
            'RateLimitError','ProviderError'} else 'RUNTIME',
            'error_type':type(exc).__name__, 'error_code':getattr(exc,'code','UNKNOWN')}))
        raise SystemExit(1)
