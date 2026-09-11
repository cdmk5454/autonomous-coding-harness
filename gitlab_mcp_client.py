"""
GitLab MCP 보조 클라이언트 (읽기 전용, 옵션)
- GitHub MCP / gh 의존 금지. GitLab 만.
- 기본 비활성(enabled=false). GITLAB_API_URL + GITLAB_PERSONAL_ACCESS_TOKEN 환경변수가
  모두 있고 config.enabled=true 일 때만 동작.
- 네트워크 호출은 지연(lazy). 모듈 import 자체는 네트워크를触지 않는다.
- 읽기 위주만(MR diff / pipeline status / issue). 쓰기 금지.
- 표준 라이브러리(urllib)만 사용 → 외부 패키지 의존 없음.
- reviewer 의 주 경로(로컬 git_collector + Codex 시맨틱)는 그대로. 본 클라이언트는 보조.

설정 우선순위: 환경변수 GITLAB_API_URL / GITLAB_PERSONAL_ACCESS_TOKEN > config.json
MCP 엔드포인트: 공식(<host>/api/v4/mcp) 우선, 미지원 시 community server 폴백 권장.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

from runtime_safety import safe_print as print

CONFIG_PATH = Path(__file__).resolve().parent / "mcp" / "gitlab_mcp.config.json"
DEFAULT_TIMEOUT = 15  # 보조 호출이 파이프라인을 오래 막지 않도록 짧게


class GitLabMCPError(RuntimeError):
    """GitLab MCP 보조 호출 관련 오류."""


def load_config() -> dict:
    """gitlab_mcp.config.json 을 읽어 gitlab_mcp 섹션 반환. 실패 시 기본 비활성 딕셔너리."""
    try:
        with open(CONFIG_PATH, encoding="utf-8") as f:
            data = json.load(f)
        return data.get("gitlab_mcp", {"enabled": False, "mode": "read_only"})
    except FileNotFoundError:
        return {"enabled": False, "mode": "read_only"}
    except Exception:
        return {"enabled": False, "mode": "read_only"}


def is_enabled() -> bool:
    """
    활성 여부. 아래 세 조건 모두 충족 시 True:
      1) config.enabled == true
      2) 환경변수 GITLAB_API_URL 존재
      3) 환경변수 GITLAB_PERSONAL_ACCESS_TOKEN 존재
    어느 하나라도 없으면 False (파이프라인은 로컬 주 경로로 계속 진행).
    """
    cfg = load_config()
    if not cfg.get("enabled", False):
        return False
    if cfg.get("mode", "read_only") != "read_only":
        return False
    return bool(os.environ.get("GITLAB_API_URL")) and bool(
        os.environ.get("GITLAB_PERSONAL_ACCESS_TOKEN")
    )


def _api_url() -> str:
    return os.environ.get("GITLAB_API_URL", "").rstrip("/")


def _headers() -> dict:
    token = os.environ.get("GITLAB_PERSONAL_ACCESS_TOKEN", "")
    return {
        "PRIVATE-TOKEN": token,
        "Accept": "application/json",
    }


def _get(path: str, params: dict | None = None, timeout: int = DEFAULT_TIMEOUT) -> dict | None:
    """
    GitLab REST API GET (읽기 전용).
    - MCP 엔드포인트(/api/v4/mcp)가 아니라 REST(/api/v4)를 직접 사용하는 단순 보조 경로.
      (MCP HTTP 사양은 인스턴스마다 다를 수 있어 1차는 안정적인 REST로 읽기만 수행.)
    - 실패 시 None 반환(예외를 파이프라인에 전파하지 않음).
    """
    if not is_enabled():
        return None
    base = _api_url()
    if not base:
        return None
    url = f"{base}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers=_headers(), method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            return json.loads(raw)
    except urllib.error.HTTPError as e:
        print(f"[GitLabMCP] HTTP {e.code}: {path} (보조 호출 스킵)")
        return None
    except (urllib.error.URLError, TimeoutError) as e:
        print(f"[GitLabMCP] 네트워크 오류: {e} (보조 호출 스킵)")
        return None
    except Exception as e:
        print(f"[GitLabMCP] 예외: {e} (보조 호출 스킵)")
        return None


def project_path_encoded(project: str) -> str:
    """'group/sub/proj' → URL 인코딩된 프로젝트 경로."""
    return urllib.parse.quote(project.strip("/"), safe="")


def get_mr_diff(project: str, mr_iid: int) -> str | None:
    """
    MR diff 본문 조회(읽기). project와 mr_iid는 호출자가 명시한다.
    실패/비활성 시 None. 본문이 매우 클 수 있으므 호출부에서 잘라 사용.
    """
    if not is_enabled():
        return None
    pid = project_path_encoded(project)
    # diffs 엔드포인트는 text/plain(diff) 반환 가능 → 헤더 Accept 조정
    base = _api_url()
    token = os.environ.get("GITLAB_PERSONAL_ACCESS_TOKEN", "")
    url = f"{base}/projects/{pid}/merge_requests/{mr_iid}/diffs"
    req = urllib.request.Request(
        url, headers={"PRIVATE-TOKEN": token, "Accept": "application/json"}, method="GET"
    )
    try:
        with urllib.request.urlopen(req, timeout=DEFAULT_TIMEOUT) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))
            # diffs 배열을 단순 텍스트로 직렬화
            lines = []
            for d in data if isinstance(data, list) else []:
                lines.append(d.get("diff", ""))
            return "\n".join(lines) if lines else None
    except Exception as e:
        print(f"[GitLabMCP] MR diff 조회 실패: {e} (보조 스킵)")
        return None


def get_pipeline_status(project: str, pipeline_id: int) -> dict | None:
    """파이프라인 상태 조회(읽기). 실패/비활성 시 None."""
    if not is_enabled():
        return None
    pid = project_path_encoded(project)
    return _get(f"/projects/{pid}/pipelines/{pipeline_id}")


def get_issue(project: str, issue_iid: int) -> dict | None:
    """이슈 조회(읽기). 실패/비활성 시 None."""
    if not is_enabled():
        return None
    pid = project_path_encoded(project)
    return _get(f"/projects/{pid}/issues/{issue_iid}")


def status() -> dict:
    """현재 GitLab MCP 보조 연동 상태 요약(진단용, 네트워크 미호출)."""
    cfg = load_config()
    return {
        "enabled": is_enabled(),
        "config_enabled": cfg.get("enabled", False),
        "mode": cfg.get("mode", "read_only"),
        "has_api_url": bool(os.environ.get("GITLAB_API_URL")),
        "has_token": bool(os.environ.get("GITLAB_PERSONAL_ACCESS_TOKEN")),
        "endpoint_strategy": cfg.get("endpoint_strategy", "auto"),
    }
