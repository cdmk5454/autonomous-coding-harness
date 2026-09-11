"""
Worker - 코드 생성/수정 실행기
- 일반 워커(DroidWorker): droid exec 호출
- 고급 워커(CodexWorker): codex exec 호출
- 작업 유형(requirement + target_module) 에 따라 내부 하네스의 영역별
  절대 제약(hooks) 과 스킬(skills) 을 선택 주입한다.
  (수정하지 않고 읽어서 주입만 함. AGENTS_DIR 환경변수로 경로 오버라이드 가능.)
"""

from __future__ import annotations

import json
import hashlib
import os
import re
import shutil
import subprocess
import uuid
import tempfile
from harness_temp import scratch_root
import time
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Sequence

from task_state import (
    TaskState,
    resolve_task_mode,
    TASK_MODE_MODIFICATION,
    ALREADY_SATISFIED_MARKER,
)
from project_profile import ProjectProfile
from feature_context import load_feature_context
from context_foundation import build_and_record_context, build_context_pack
from job_contract import DROID_DEFAULT_CODING_MODEL
from policy_catalog import (
    POLICY_REFS,
    prompt_metrics,
    repeated_constraint_count,
)
from runtime_safety import (
    assert_secret_free_argv,
    isolated_subprocess_env,
    scrub_secrets,
    safe_print as print,
    trusted_executable,
)


QA_REQUEST_PREFIX = "HARNESS_QA_REQUEST_JSON:"
QA_REQUEST_TYPES = frozenset({
    "HUMAN_ACCEPTANCE", "BUSINESS_CONTRACT", "API_CONTRACT", "DB_CONTRACT",
    "TOOL_PERMISSION", "CLARIFICATION", "CANDIDATE_DECISION",
    "CONTRACT_DECISION", "POLICY_DECISION", "DEFERRED_DECISION",
})
QA_REQUEST_HOLD_SCOPES = frozenset({"JOB", "DEPENDENCY_CHAIN", "BATCH"})
QA_REQUEST_NONE_SENTINELS = frozenset({"NONE", "NOT_REQUIRED", "없음"})


def _is_no_qa_payload(payload: str) -> bool:
    normalized = payload.strip()
    if normalized.upper() in QA_REQUEST_NONE_SENTINELS:
        return True
    return re.fullmatch(
        r"(?:NONE|NOT_REQUIRED|없음)\s*(?:—|–|-|:)\s+.+",
        normalized,
        flags=re.IGNORECASE,
    ) is not None


def parse_qa_request(stdout: str) -> dict[str, Any]:
    """Parse one explicit structured QA signal; ordinary prose never triggers QA."""
    payloads = [
        line.strip()[len(QA_REQUEST_PREFIX):].strip()
        for line in (stdout or "").splitlines()
        if line.strip().startswith(QA_REQUEST_PREFIX)
    ]
    if not payloads:
        return {}
    if len(payloads) != 1:
        raise ValueError("QA_REQUEST_MULTIPLE")
    if _is_no_qa_payload(payloads[0]):
        return {}
    try:
        value = json.loads(payloads[0])
    except json.JSONDecodeError as exc:
        raise ValueError("QA_REQUEST_JSON_INVALID") from exc
    if not isinstance(value, dict) or value.get("required") is not True:
        raise ValueError("QA_REQUEST_REQUIRED_INVALID")
    qa_type = str(value.get("qa_type", "")).strip().upper()
    hold_scope = str(value.get("hold_scope", "")).strip().upper()
    reason = scrub_secrets(str(value.get("reason", "")).strip())[:1000]
    evidence = value.get("evidence")
    complete = value.get("decision_independent_work_complete")
    if qa_type not in QA_REQUEST_TYPES:
        raise ValueError("QA_REQUEST_TYPE_INVALID")
    if hold_scope not in QA_REQUEST_HOLD_SCOPES:
        raise ValueError("QA_REQUEST_HOLD_SCOPE_INVALID")
    if not reason or not isinstance(evidence, (list, dict)) or not evidence:
        raise ValueError("QA_REQUEST_EVIDENCE_INVALID")
    if not isinstance(complete, bool):
        raise ValueError("QA_REQUEST_COMPLETION_INVALID")
    safe_evidence: list[str] | dict[str, str]
    if isinstance(evidence, list):
        safe_evidence = [scrub_secrets(str(item))[:500] for item in evidence[:20] if str(item)]
    else:
        safe_evidence = {
            scrub_secrets(str(key))[:100]: scrub_secrets(str(item))[:500]
            for key, item in list(evidence.items())[:20]
        }
    if not safe_evidence:
        raise ValueError("QA_REQUEST_EVIDENCE_INVALID")
    result = {
        "required": True,
        "qa_type": qa_type,
        "hold_scope": hold_scope,
        "reason": reason,
        "evidence": safe_evidence,
        "decision_independent_work_complete": complete,
    }
    if 'blocked_gates' in value:
        gates = value['blocked_gates']
        if not isinstance(gates, list) or not gates or any(g not in {'DB_CONNECT', 'E2E'} for g in gates):
            raise ValueError('QA_REQUEST_BLOCKED_GATES_INVALID')
        result['blocked_gates'] = gates
    if 'skeleton_safe' in value or 'deferred_contracts' in value:
        items = value.get('deferred_contracts')
        fields = {'decision', 'candidates', 'evidence', 'areas', 'verification_required'}
        if (not isinstance(value.get('skeleton_safe'), bool)
                or not isinstance(items, list) or not 1 <= len(items) <= 30
                or any(not isinstance(item, dict) or set(item) != fields
                    or any(not isinstance(v, str) or not v.strip() for v in item.values()) for item in items)):
            raise ValueError('DEFERRED_CONTRACT_RECORD_INVALID')
        result['skeleton_safe'] = value['skeleton_safe']
        result['deferred_contracts'] = [
            {k:scrub_secrets(v)[:2000] for k,v in item.items()} for item in items]
    return result


# 내부 하네스 루트 (환경변수 AGENTS_DIR 우선, 없으면 D:\\agents 폴백)
AGENTS_DIR = Path(os.environ.get("AGENTS_DIR", r"D:\agents"))

# 하네스 루트(이 파일이 속한 디렉토리) — doc/ 산출물·.tasks/ 의 기준점.
# 워커 cwd 는 작업 워크스페이스(D:\path\to\project)와 달라 절대경로로 계산한다.
HARNESS_ROOT = Path(__file__).resolve().parent

COMMON_AREA_SKILLS: dict[str, tuple[str, ...]] = {
    "sql": ("skills/sql/SKILL.md",),
    "backend": ("skills/backend/SKILL.md",),
    "frontend": ("skills/frontend/SKILL.md",),
}

# 항상 주입하는 기본 안전망 파일 (validation.md)
ALWAYS_HOOKS = ["hooks/validation.md"]
AGENTS_MD_REL = "AGENTS.md"

# Project context remains always-on; optional skills are requirement-selected.
ALWAYS_SKILLS = ["skills/project-context/SKILL.md"]

MODIFICATION_EXECUTION_AUTHORIZATION = """[실행 단계 및 승인 범위]
이 작업은 사용자가 run.py에 제출한 승인된 MODIFICATION Job이다.
명시된 요구사항과 대상 범위 안에서는 별도의 설계 승인 질문 없이 바로 구현한다.
brainstorming 지침의 설계 검토는 내부적으로 수행하되, 이미 승인된 범위에 대해 다시 사용자 승인을 요청하지 않는다.
요구사항을 벗어난 범위 확장, 위험 작업 또는 결정할 수 없는 업무 규칙이 필요한 경우에만 작업을 중단하고 사유를 보고한다.
현재 파일이 요구사항을 이미 모두 충족하여 실제로 수정할 필요가 없을 때만 최종 응답에 아래 줄을 정확히 단독으로 포함한다.
{marker}
이 표식은 독립 Reviewer가 현재 파일을 직접 검증하며, 단순 주장이나 구현 미수행 사유로 사용하면 실패 처리된다.""".format(
    marker=ALREADY_SATISFIED_MARKER
)

SOURCE_FIRST_EXECUTION_RULES = """[애플리케이션 소스 우선 실행 규칙]
대상 애플리케이션 소스를 먼저 조사하고 승인된 기능을 실제로 구현한다.
task_plan.md, 계획 문서, 진행 기록, VERIFY 문서와 Harness report는 기능 구현의 선행조건이 아니다.
프로젝트 요구사항이 명시적으로 요구하지 않는 한 계획 문서를 수정하지 않는다.
계획/검증 문서 패치가 실패해도 오류를 기록한 뒤 애플리케이션 소스 구현을 계속한다.
계획 문서와 Harness 산출물만 바꾼 결과는 MODIFICATION Job의 source change로 인정되지 않는다."""


def artifact_paths(task_id: str) -> tuple[str, str]:
    """task_id → (산출물 디렉토리 절대경로, VERIFY 파일 절대경로).
    하네스 루트 기준 doc/YYYYMMDD/HHmmss/ — 워커 cwd 는 작업
    워크스페이스라 상대경로로 쓰면 소스 트리에 심긴다(sample-service 사고).
    VERIFY 파일명은 시간(HHmmss) = task id 접미사라 TASK-YYYYMMDD-HHmmss 와 1:1.
    """
    m = re.fullmatch(r"TASK-(\d{8})-(\d{6})(?:-\d+)?", task_id or "")
    if not m:
        date_part, time_part = datetime.now().strftime("%Y%m%d"), "artifacts"
        return (
            str(HARNESS_ROOT / "doc" / date_part / time_part),
            str(HARNESS_ROOT / "doc" / date_part / time_part / "VERIFY.md"),
        )
    date_part, time_part = m.group(1), m.group(2)
    return (
        str(HARNESS_ROOT / "doc" / date_part / time_part),
        str(HARNESS_ROOT / "doc" / date_part / time_part / f"VERIFY_{time_part}.md"),
    )


def completion_protocol(task_id: str = "") -> str:
    """사용자 요구사항 뒤에 붙는 task-delta/산출물 완료 계약."""
    artifact_dir, verify_path = artifact_paths(task_id)
    return f"""[하네스 완료 판정 프로토콜]
이 섹션은 작업 요구사항을 실행 파이프라인의 Job 기준점 의미로 해석한다.
`git diff HEAD`와 `git status`에는 이 Job 시작 전 다른 Job/사용자의 변경이 포함될 수 있다.
따라서 전체 HEAD diff를 이번 Job이 만든 변경이라고 주장하지 마라. 실제 source delta 소유권은 Manager가 Job 시작 기준점으로 판정한다.
현재 코드가 요구사항을 이미 충족한다고 판단해도 diff를 만들기 위한 touch·재저장·무의미한 수정은 금지한다. MODIFICATION Job에서 의미 있는 애플리케이션 source delta가 없으면 Manager가 Build/Review 전에 기술 실패로 처리한다.
요구사항이 검증/문서 산출물(VERIFY.md, 검토용 SQL 등)을 요구하면 하네스 산출물 경로에 작성한다:
- 검증/분석 문서·검토용 쿼리 → `{verify_path}` (없으면 디렉토리를 만들어 작성)
- 앱에 배포되는 실제 소스(실행용 SQL 포함) → 대상 모듈 소스 트리 안 (예: sample-service/src/main/resources/sql/)
검증 문서를 통과 목적으로 소스 트리에 심는 것 금지. 게이트는 소스 diff 로 판정한다.
완료 보고에는 source 변경 파일과 Job 산출물(doc/ 경로)을 구분해서 적는다.
실행 중 실제 authoritative decision이 없어 business/API/DB 계약 또는 human acceptance를
판정할 수 없을 때만 마지막에 다음 한 줄 JSON 신호를 출력한다. 일반 설명이나 요구사항
키워드는 신호가 아니며, QA가 필요하지 않으면 이 줄을 출력하지 않는다.
`{QA_REQUEST_PREFIX} {{"required":true,"qa_type":"HUMAN_ACCEPTANCE|BUSINESS_CONTRACT|API_CONTRACT|DB_CONTRACT","hold_scope":"JOB|DEPENDENCY_CHAIN|BATCH","reason":"...","evidence":["..."],"decision_independent_work_complete":true|false}}`
결정 의존 값을 추측하거나 결정 의존 source를 수정하지 않는다."""

# ── 화면 맵(ui_map) 선택 주입 ────────────────────────────────────
# 선택된 profile 문서에서 URL 메뉴 ID, 화면 키, 클래스명과 관련된 절만 추출한다.

# 요구사항에서 화면 식별자 추출 정규식
URL_MENU_ID_RE = re.compile(r"/(?:[^/\s]+/)*(M\d{6})(?:\b|/)")
SCREEN_KEY_RE = re.compile(                                # 화면 키 전반
    r"\b([A-Z]{2,12})_([A-Z]{2,6})_(\d{4})(?:_([A-Z]{2,12}))?\b"
    r"|\b[A-Z][A-Za-z]{3,20}\d{4}(?![0-9])"
    r"|\b[A-Z]{2,6}\d{4}(?![0-9])",
)
UI_MAP_SECTION_CAP = 4000   # [화면 맵] 최대 길이(문자)
DB_CATALOG_CONTEXT_CAP = 10000


def extract_screen_keys(text: str) -> list[str]:
    """요구사항 텍스트에서 화면 식별자들을 추출해 고유 목록 반환.
    - URL의 M###### → 메뉴 ID (profile index로 해석)
    - PCAP_OP_0210 / PcapOp0210 / OP0210 등
    """
    if not text:
        return []
    found: list[str] = []
    m = URL_MENU_ID_RE.search(text)
    if m:
        found.append(m.group(1))
    for mm in SCREEN_KEY_RE.finditer(text):
        found.append(mm.group(0))
    # 중복 제거(순서 유지)
    seen: set[str] = set()
    uniq: list[str] = []
    for f in found:
        if f not in seen:
            seen.add(f)
            uniq.append(f)
    return uniq


def _ui_map_dir(profile: ProjectProfile | None = None) -> Path | None:
    """Return only the selected profile's UI map directory."""
    return profile.ui_map if profile is not None else None


def _ui_map_files(profile: ProjectProfile | None = None) -> list[Path]:
    """ui_map 아래 *.md 전체 (서브디렉토리 포함). 없으면 빈 목록."""
    root = _ui_map_dir(profile)
    if root is None or not root.is_dir():
        return []
    return sorted(root.rglob("*.md"))


def _screen_number_variants(key: str, profile: ProjectProfile | None = None) -> list[str]:
    """화면 식별자 → ui_map 문서 검색용 변형 목록.
    underscore/camel/menu identifiers are expanded only from observed profile data.
    """
    k = key.strip()
    if k.startswith("M") and k[1:].isdigit():
        # 메뉴 ID — MENU_ID_INDEX.md 체인으로만 해석(§3 절차)
        index_menu = resolve_menu_id(k, profile)
        return [index_menu] if index_menu else []
    variants = [k]
    m = re.fullmatch(r"([A-Z]{2,6})_([A-Z]{2})_(\d{4})(?:_([A-Z]{2,6}))?", k)
    if m:
        variants.append(f"{m.group(2)}_{m.group(3)}")            # OP_0210
        variants.append(f"{m.group(2)}{m.group(3)}")             # OP0210
    m2 = re.fullmatch(r"[A-Z]([A-Za-z]{2,20})(\d{4})", k)
    if m2:
        variants.append(f"{m2.group(1)[-2:].upper()}{m2.group(2)}")
    # 중복 제거
    seen: set[str] = set()
    return [v for v in variants if not (v in seen or seen.add(v))]


_MENU_INDEX_ROW_RE = re.compile(
    r"(?m)^\|\s*`?(M\d{6})`?\s*\|[^|]*\|[^|]*?(\S+\.vue)\S*\s*\|"
)
_VUE_SCREEN_NO_RE = re.compile(
    r"[A-Z]([A-Za-z]{2,20})(\d{4})(?:List|Detail|View|Pop)?\.vue$"
)


def resolve_menu_id(menu_id: str, profile: ProjectProfile | None = None) -> str:
    """메뉴 ID(M######) → 화면번호('OP0030' 등) 해석 (MENU_ID_INDEX §3 절차 자동화).
    ui_map/MENU_ID_INDEX.md §4 표(개발 DB 덤프)에서 menuId 행을 찾아
    PGM_PATH_NM(.vue) 파일명의 화면부(마지막 2글자+번호)를 반환.
    - 표에 없거나 .vue 파일명이 P### 패턴이 아니면 '' (fail-closed: 추측 금지 —
      표 밖 ID는 DB 조회/사용자 확인 대상, §3의 Fail Loud 방침 준수)
    """
    mid = (menu_id or "").strip()
    if not (mid.startswith("M") and mid[1:].isdigit() and len(mid) == 7):
        return ""
    root = _ui_map_dir(profile)
    if root is None:
        return ""
    index_file = root / "MENU_ID_INDEX.md"
    try:
        content = index_file.read_text(encoding="utf-8")
    except Exception:
        return ""
    for row in _MENU_INDEX_ROW_RE.finditer(content):
        if row.group(1) == mid:
            m = _VUE_SCREEN_NO_RE.search(row.group(2))
            if m:
                return f"{m.group(1)[-2:].upper()}{m.group(2)}"
            return ""
    return ""


def load_ui_map_section(
    text: str,
    cap: int = UI_MAP_SECTION_CAP,
    profile: ProjectProfile | None = None,
) -> tuple[str, list[str]]:
    """요구사항에서 화면 식별자를 찾아 ui_map 문서의 해당 화면 섹션만 추출.
    반환: (섹션_텍스트, 매칭된_키_목록). 매칭 실패/파일 없음 → ("", []).
    - '### OP0210 ...' 형태의 섹션 헤더부터 다음 '###'/'## ' 전까지.
    - 여러 키가 매칭되면 순서대로 연결(총 cap 자로 제한).
    - 파일 읽기 실패는 빈 문자열로 스킵(파이프라인 계속).
    """
    keys = extract_screen_keys(text)
    if not keys:
        return "", []
    files = _ui_map_files(profile)
    if not files:
        return "", []

    sections: list[str] = []
    matched: list[str] = []
    for key in keys:
        variants = _screen_number_variants(key, profile)
        if not variants:
            print(
                f"[Worker] ui_map: '{key}' 는 MENU_ID_INDEX 표에서도 해석 불가 "
                f"(스킵 - DB 조회/사용자 확인 필요)"
            )
            continue
        for f in files:
            try:
                content = f.read_text(encoding="utf-8")
            except Exception as e:
                print(f"[Worker] ui_map 읽기 실패({f.name}): {e} (스킵)")
                continue
            for variant in variants:
                # 섹션 헤더 매칭: '### OP0210 ...' / '### PCAP_OP_0210 ...'
                # (f-string 내 #{1,3} 이스케이프 주의 — f-string 밖에서 조립)
                pat_src = (
                    r"(?m)^#{1,3}\s[^\n]*\b" + re.escape(variant) + r"\b"
                )
                pat = re.compile(pat_src)
                header = pat.search(content)
                if header:
                    # 헤더 라인 이후부터 다음 섹션 헤더까지 (start+1 검색 시
                    # 잘린 문자열 맨 앞 '^' 이 같은 라인에 붙는 사고 방지)
                    rest = content[header.end():]
                    nxt = re.search(r"(?m)^#{1,3}\s", rest)
                    end = header.end() + nxt.start() if nxt else len(content)
                    sections.append(content[header.start():end].strip())
                    matched.append(key)
                    break
            if matched and matched[-1] == key:
                break
        if len("\n\n".join(sections)) >= cap:
            break

    if not sections:
        return "", []
    body = "\n\n---\n\n".join(sections)
    return body, matched


def load_db_catalog_context(
    requirement: str,
    ui_map_block: str,
    matched_screen_keys: Sequence[str],
    profile: ProjectProfile | None = None,
    cap: int = DB_CATALOG_CONTEXT_CAP,
) -> str:
    """Render only DB tables named by the request or selected UI-map screens."""
    if profile is None or profile.db_catalog is None:
        return ""
    catalog = profile.db_catalog
    from_text = catalog.table_ids_for_text(f"{requirement}\n{ui_map_block}")
    from_screens = catalog.table_ids_for_screens(matched_screen_keys)
    table_ids = tuple(dict.fromkeys((*from_text, *from_screens)))
    return catalog.render_context(
        table_ids,
        focus_text=f"{requirement}\n{ui_map_block}",
        max_chars=cap,
    )

# [셸 실행 제약] 항상 주입 (워커 에이전트가 셸에서 무한 대기하는 것 방지)
# PowerShell 출력 커맨드를 인자 없이 호출하면 InputObject 대기 상태로 멈춰
# 무활동 타임아웃까지 워커가 교착된다.
SHELL_SAFETY_RULES = """\
[셸 실행 제약 — 반드시 준수]
- PowerShell에서 Write-Output / Write-Host 등 출력 커맨드는 인자를 반드시 명시할 것.
- 인자 없이 호출하면 "Supply values for the following parameters: InputObject:"
  대기 상태로 무한히 멈춘다. 절대 금지.
- 출력이 필요하면 반드시 Write-Output "메시지" 형태로만 사용할 것."""

# 하위 호환: 기존 절대경로 상수 (load_project_rules 기본 인자용)
AGENTS_MD_PATH = str(AGENTS_DIR / AGENTS_MD_REL)


def _safe_read(rel_path: str, policy_root: Path | None = None) -> str:
    """Read a required common harness file and fail closed on any error."""
    path = (policy_root or AGENTS_DIR) / rel_path
    try:
        with open(path, encoding="utf-8") as f:
            return f.read().strip()
    except FileNotFoundError as exc:
        raise RuntimeError(f"required common rule missing: {path}") from exc
    except Exception as e:
        raise RuntimeError(f"required common rule unreadable: {path} ({type(e).__name__})") from e


def _read_required(path: Path, label: str) -> str:
    try:
        text = path.read_text(encoding="utf-8").strip()
    except Exception as exc:
        raise RuntimeError(f"{label} cannot be read: {path} ({type(exc).__name__})") from exc
    if not text:
        raise RuntimeError(f"{label} is empty: {path}")
    return text


def _normalize_modules(target_module: list[str] | str | None) -> list[str]:
    """target_module 정규화(list/tuple/str/None → list[str])."""
    if not target_module:
        return []
    if isinstance(target_module, str):
        return [target_module] if target_module else []
    return [m for m in target_module if m]


def detect_areas(
    requirement: str,
    target_module: list[str] | str | None = None,
    changed_files: Sequence[str] | None = None,
    profile: ProjectProfile | None = None,
) -> list[str]:
    """
    requirement 텍스트와 target_module 을 기반으로 관련 영역을 감지.
    반환: 감지된 영역 키 리스트 (AREA_KEYWORDS 선언 순서).
    - requirement 키워드 매칭(주) + target_module 기반 보조 감지
    """
    modules = _normalize_modules(target_module)
    if profile is not None:
        return list(profile.detect_areas(requirement, modules, changed_files).areas)
    text = requirement or ""
    generic = {
        "sql": ("sql", "query", "qry", "mapper"),
        "backend": ("java", "spring", "backend", "controller", "service", "svc", "ctr", "dao", "vo"),
        "frontend": ("vue", "frontend", "component", "router", "ui"),
        "architecture": ("architecture", "dependency", "boundary"),
    }
    return [
        area for area, words in generic.items()
        if any(re.search(rf"(?<![\w]){re.escape(word)}(?![\w])", text, re.IGNORECASE) for word in words)
    ]


def load_project_rules(path: str = "") -> str:
    """
    하위 호환: AGENTS.md 텍스트를 반환.
    path 가 주어지면(레거시 절대경로) 그 파일을, 아니면 AGENTS_DIR/AGENTS.md.
    실패 시 빈 문자열.
    """
    p = Path(path) if path else (AGENTS_DIR / AGENTS_MD_REL)
    try:
        with open(p, encoding="utf-8") as f:
            return f.read().strip()
    except FileNotFoundError:
        print(f"[Worker] AGENTS.md 를 찾을 수 없음: {p} (규칙 주입 생략)")
        return ""
    except Exception as e:
        print(f"[Worker] AGENTS.md 읽기 실패: {e} (규칙 주입 생략)")
        return ""


def select_context(
    requirement: str,
    target_module: list[str] | str | None = None,
    changed_files: Sequence[str] | None = None,
    profile: ProjectProfile | None = None,
) -> tuple[list[str], list[str]]:
    """
    작업 유형에 맞춰 주입할 hooks/skills 상대경로 리스트를 선택(중복 제거, 순서 유지).
    반환: (hook_rel_paths, skill_rel_paths)
    - 항상: hooks/validation.md (기본 안전망) + project-context
    - 감지된 영역별 hooks/skills 추가
    """
    areas = detect_areas(requirement, target_module, changed_files, profile)
    hook_paths: list[str] = list(ALWAYS_HOOKS)
    skill_paths: list[str] = list(ALWAYS_SKILLS)

    for area in areas:
        for s in COMMON_AREA_SKILLS.get(area, ()):
            if s not in skill_paths:
                skill_paths.append(s)
    return hook_paths, skill_paths


def _analysis_sections(
    profile: ProjectProfile,
    requirement: str,
    target_module: list[str] | str | None,
    areas: Sequence[str],
    max_sections: int = 4,
) -> str:
    """Select complete relevant Markdown sections; never slice arbitrary document ends."""
    text = _read_required(profile.analysis, "profile analysis")
    matches = list(re.finditer(r"(?m)^#{1,4}\s+.+$", text))
    if not matches:
        return ""
    modules = _normalize_modules(target_module)
    keys = extract_screen_keys(requirement)
    class_keys = re.findall(r"\b[A-Z][A-Za-z0-9_]{3,}\b", requirement or "")
    words = [*modules, *keys, *class_keys, *areas]
    selected: list[str] = []
    for index, heading in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        section = text[heading.start():end].strip()
        if words and any(re.search(re.escape(word), section, re.IGNORECASE) for word in words):
            selected.append(section)
            if len(selected) >= max_sections:
                break
    return "\n\n---\n\n".join(selected)


def build_prompt(
    requirement: str,
    target_module: list[str] | str | None = None,
    task_mode: str = "",
    task_id: str = "",
    profile: ProjectProfile | None = None,
    role: str = "droid",
    changed_files: Sequence[str] | None = None,
    policy_root: Path | None = None,
    task: TaskState | None = None,
    manifest_root: Path | None = None,
) -> str:
    """
    Droid/Codex 공용 프롬프트 빌더 (영역별 선택 주입).
    구조:
        [프로젝트 규칙]        ← AGENTS.md (항상)
        [영역별 절대 제약]     ← validation.md(항상) + 감지된 hooks
        [셸 실행 제약]         ← SHELL_SAFETY_RULES (항상, 인자 없는 출력 커맨드 금지)
        [관련 스킬 가이드]     ← project-context(항상) + 감지된 skills
        [실행 단계 및 승인 범위] ← 명시적 MODIFICATION일 때만
        [타겟 모듈]            ← target_module 있을 때만
        [작업 요구사항]        ← 사용자 요구사항 (항상)
    파일이 없으면 해당 섹션은 스킵되며, 요구사항은 항상 전달된다.
    target_module 은 list[str] (다중) 또는 str (구버전) 모두 허용.
    """
    parts: list[str] = []

    # 0.8.5 profile-aware path: reuse the existing profile assets through one
    # deterministic selector and record only the redacted manifest metadata.
    if profile is not None:
        workspace = (
            Path(task.working_dir).resolve()
            if task is not None and task.working_dir
            else profile.workspace_roots[0]
        )
        evidence_files = list(
            changed_files
            if changed_files is not None
            else (task.changed_files if task is not None else [])
        )
        evidence = None
        if task is not None:
            evidence = {
                "changed_files": evidence_files,
                "task_owned_changed_files": list(task.task_owned_changed_files),
                "inherited_batch_delta_files": list(task.inherited_batch_delta_files),
                "failure_code": task.failure_code,
                "failure_fingerprint": task.failure_fingerprint,
            }
        if task is not None:
            pack = build_and_record_context(
                task=task,
                role="WORKER",
                profile=profile,
                workspace=workspace,
                policy_root=policy_root or AGENTS_DIR,
                manifest_root=manifest_root,
                requirement=requirement,
                changed_files=evidence_files,
                task_evidence=evidence,
            )
        else:
            pack = build_context_pack(
                role="WORKER",
                requirement=requirement,
                profile=profile,
                workspace=workspace,
                policy_root=policy_root or AGENTS_DIR,
                target_modules=_normalize_modules(target_module),
                changed_files=evidence_files,
                task_mode=task_mode,
                task_id=task_id,
            )
        compatibility_markers = [f"[선택된 프로필 규칙: {profile.id}]"]
        if any(
            str(item.get("path", "")).startswith("profile:ui_map/")
            for item in pack.manifest["selected_resources"]
        ):
            compatibility_markers.append("[화면 맵]")
        rendered_pack = "\n".join(compatibility_markers) + "\n" + pack.prompt
        if task is not None:
            if not task.policy_refs:
                task.policy_refs = list(POLICY_REFS)
            task.context_refs = [
                str(item.get("path", ""))
                for item in pack.manifest.get("selected_resources", [])
                if item.get("path")
            ]
        parts.append(
            ("[공통 프로젝트 규칙]\n" + rendered_pack)
            if role == "droid"
            else rendered_pack
        )
        # The profile-aware 0.9 path references deterministic Harness policy
        # instead of repeating the same long natural-language constraints in
        # every Worker prompt.  Enforcement remains in the runtime gates.
        policy_names = list(task.policy_refs) if task is not None else list(POLICY_REFS)
        parts.append("[Harness policy references]\n" + "\n".join(policy_names))
        if task_mode == TASK_MODE_MODIFICATION:
            # Preserve the two operational decisions that a Worker must act on;
            # the long duplicated authorization/source-first prose lives in the
            # policy layer and is not repeated in every profile-aware prompt.
            parts.append(
                "[실행 권한 요약]\n"
                "이 작업은 승인된 MODIFICATION Job이며 명시 범위 안에서 즉시 실행한다.\n"
                "계획/검증 문서 패치가 실패해도 오류를 기록한 뒤 "
                "애플리케이션 소스 구현을 계속한다."
            )
        modules = _normalize_modules(target_module)
        if modules:
            parts.append(f"[타겟 모듈]\n{', '.join(modules)}")
        if task_mode == TASK_MODE_MODIFICATION:
            parts.append(completion_protocol(task_id))
        effective_prompt = "\n\n".join(parts)
        if task is not None:
            task.prompt_metrics = prompt_metrics(
                raw_job_intent=task.requirement,
                policy_text="\n".join(task.policy_refs),
                context_text=pack.prompt,
                effective_prompt=effective_prompt,
                removed_repeated_constraint_count=repeated_constraint_count(task.requirement),
                policy_refs=task.policy_refs,
            )
            task.prompt_metrics.update({
                "stable_context_sha256": pack.manifest.get("stable_context_sha256", ""),
                "dynamic_context_sha256": pack.manifest.get("dynamic_context_sha256", ""),
                "stable_resource_count": len(pack.manifest.get("context_layers", {}).get("stable", [])),
                "dynamic_resource_count": len(pack.manifest.get("context_layers", {}).get("dynamic", [])),
            })
        return effective_prompt

    # Snapshot-backed Jobs inject the common rules explicitly for both engines.
    # This prevents Codex's live global file from becoming an execution input.
    if role == "droid" or policy_root is not None:
        rules = load_project_rules(
            str((policy_root or AGENTS_DIR) / AGENTS_MD_REL)
        )
        if not rules:
            raise RuntimeError(
                f"common AGENTS.md cannot be read: "
                f"{(policy_root or AGENTS_DIR) / AGENTS_MD_REL}"
            )
        parts.append(f"[공통 프로젝트 규칙]\n{rules}")

    if profile is not None:
        parts.append(
            f"[선택된 프로필 규칙: {profile.id}]\n"
            + _read_required(profile.project_rules, "profile project rules")
        )

    # [영역별 절대 제약] 항상(validation.md) + 감지된 hooks
    areas = detect_areas(requirement, target_module, changed_files, profile)
    hook_paths, skill_paths = select_context(
        requirement, target_module, changed_files, profile
    )
    hook_blocks: list[str] = []
    for hp in hook_paths:
        content = _safe_read(hp, policy_root)
        if not content:
            raise RuntimeError(
                f"required common rule cannot be read: "
                f"{(policy_root or AGENTS_DIR) / hp}"
            )
        hook_blocks.append(content)
    profile_rule_files = list(profile.rules.get("always", ())) if profile else []
    for area in areas:
        if profile:
            profile_rule_files.extend(profile.rules.get(area, ()))
    for path in dict.fromkeys(profile_rule_files):
        hook_blocks.append(_read_required(path, "profile rule/reference"))
    if hook_blocks:
        parts.append("[영역별 절대 제약]\n" + "\n\n---\n\n".join(hook_blocks))

    # [셸 실행 제약] 항상 주입 (인자 없는 출력 커맨드로 워커 교착 방지)
    parts.append(SHELL_SAFETY_RULES)

    # [관련 스킬 가이드] project-context(always) + detected common skills.
    skill_blocks: list[str] = []
    for sp in skill_paths:
        content = _safe_read(sp, policy_root)
        if not content:
            raise RuntimeError(
                f"required common skill cannot be read: "
                f"{(policy_root or AGENTS_DIR) / sp}"
            )
        skill_blocks.append(content)
    if skill_blocks:
        parts.append("[관련 스킬 가이드]\n" + "\n\n---\n\n".join(skill_blocks))

    if task_mode == TASK_MODE_MODIFICATION:
        parts.append(MODIFICATION_EXECUTION_AUTHORIZATION)
        parts.append(SOURCE_FIRST_EXECUTION_RULES)

    # [화면 맵] 요구사항에 화면 식별자(URL/화면 키)가 있을 때만 — 매칭 섹션만 주입
    analysis_block = (
        _analysis_sections(profile, requirement, target_module, areas)
        if profile else ""
    )
    if analysis_block:
        parts.append(f"[프로젝트 분석 컨텍스트: {profile.analysis.name}]\n{analysis_block}")

    ui_map_block, matched_keys = load_ui_map_section(requirement, profile=profile)
    if ui_map_block:
        parts.append(
            f"[화면 맵]\n(요구사항에 지정된 화면의 ui_map 문서 발췌 — "
            f"matched: {', '.join(matched_keys)})\n{ui_map_block}"
        )
    db_catalog_block = load_db_catalog_context(
        requirement,
        ui_map_block,
        matched_keys,
        profile=profile,
    )
    if db_catalog_block:
        parts.append(f"[선택된 DB 메타데이터]\n{db_catalog_block}")
    feature_block = load_feature_context(
        requirement,
        changed_files=changed_files,
        profile=profile,
    )
    if feature_block:
        parts.append(f"[선택된 기능 매핑 및 AS-IS 근거]\n{feature_block}")

    # [타겟 모듈]
    modules = _normalize_modules(target_module)
    if modules:
        parts.append(f"[타겟 모듈]\n{', '.join(modules)}")

    # [작업 요구사항] (항상)
    parts.append(f"[작업 요구사항]\n{requirement}")
    # 사용자 문구의 "git diff"가 전체 HEAD diff로 오해되지 않도록 요구사항 뒤에
    # 파이프라인의 task-delta 의미와 Job 전용 산출물 경로를 확정한다.
    if task_mode == TASK_MODE_MODIFICATION:
        parts.append(completion_protocol(task_id))
    if task is not None and task.test_plan:
        parts.append("[Frozen TestPlan and approved related test envelope]\n" +
                     json.dumps(task.test_plan, ensure_ascii=False, sort_keys=True) +
                     "\nWorker-local test output is supporting evidence. The official Tester remains mandatory.")
    return "\n\n".join(parts)


@dataclass
class WorkerResult:
    success: bool
    exit_code: int
    stdout: str
    stderr: str
    command: list[str]
    timed_out: bool = False
    actual_executable: str = ""
    actual_invocation_id: str = ""
    started_at: str = ""
    finished_at: str = ""
    qa_request: dict[str, Any] = field(default_factory=dict)
    execution_failure_code: str = ""


CODEX_IPC_FAILURES = frozenset({
    "WORKER_IPC_DECODE_ERROR", "CODEX_EXECUTION_SURFACE_MISMATCH",
    "CODEX_SCHEMA_PREFLIGHT_FAILED", "CODEX_EXECUTABLE_NOT_FOUND",
    "COMMAND_ACK_RECONCILIATION_REQUIRED", "NATIVE_SESSION_RESUME_UNAVAILABLE",
    "RUNTIME_RECOVERY_EXHAUSTED", "OLD_RUNTIME_WRITER_ACTIVE",
    "OPENCODE_PROVIDER_SETUP_DEFERRED", "OPENCODE_LIVE_VALIDATION_REQUIRED",
    "OPENCODE_SCHEMA_PREFLIGHT_FAILED", "OPENCODE_TRANSPORT_ERROR",
    # OpenCode runtime-phase technical failures. Every adapter-level failure is
    # harness/runtime-caused (never a product semantic defect), so the original
    # code is preserved end-to-end and classified as infrastructure here.
    "OPENCODE_PROGRESS_TIMEOUT", "OPENCODE_ASSISTANT_ERROR",
    "OPENCODE_NATIVE_MESSAGE_ID_INVALID", "OPENCODE_RESPONSE_MESSAGE_ID_INVALID",
    "OPENCODE_SESSION_CREATE_INVALID", "OPENCODE_HTTP_ERROR",
    "OPENCODE_HEALTH_INVALID", "OPENCODE_EXECUTABLE_NOT_FOUND",
    "OPENCODE_SERVER_START_FAILED", "OPENCODE_SERVER_START_TIMEOUT",
    "OPENCODE_SERVER_STOP_FAILED", "OPENCODE_SERVER_PROCESS_OWNERSHIP_UNVERIFIED",
    "OPENCODE_SERVER_PROCESS_OWNERSHIP_MISMATCH", "OPENCODE_MODEL_ID_INVALID",
    "OPENCODE_RUNTIME_HEARTBEAT_PERSISTENCE_FAILED", "MANAGED_REMOTE_AUTH_REQUIRED",
    "RUNTIME_CONTRACT_DRIFT", "WRITER_AUTHORITY_SUSPENDED",
    "DROID_SCHEMA_PREFLIGHT_FAILED",
})


def _runtime_contract_failure(task: "TaskState | None", descriptor, role: str) -> str:
    if task is None or not task.materialized_execution:
        return ""
    try:
        from runtime_contract import assert_role_contract_compatible
        assert_role_contract_compatible(
            task.materialized_execution.get("runtime_contracts"), descriptor, role
        )
    except Exception as exc:
        return str(getattr(exc, "code", "RUNTIME_CONTRACT_DRIFT"))
    return ""


def codex_host_pair_matches(executable: str) -> bool:
    """Windows distribution siblings must match the approved installed host."""
    if os.name != "nt":
        return True
    installed = Path(os.environ.get("LOCALAPPDATA", "")) / "Programs/OpenAI/Codex/bin/codex-code-mode-host.exe"
    candidate = Path(executable)
    if not candidate.is_absolute():
        resolved = shutil.which(executable)
        if not resolved:
            return False
        candidate = Path(resolved)
    trusted = candidate.resolve().with_name("codex-code-mode-host.exe")
    try:
        return (installed.is_file() and trusted.is_file()
                and hashlib.sha256(installed.read_bytes()).digest()
                == hashlib.sha256(trusted.read_bytes()).digest())
    except OSError:
        return False


def _finalize_invocation(
    result: WorkerResult,
    invocation_id: str,
    started_at: str,
) -> WorkerResult:
    result.actual_executable = str(result.command[0]) if result.command else ""
    result.actual_invocation_id = invocation_id
    result.started_at = started_at
    result.finished_at = datetime.now().astimezone().isoformat()
    return result


# ── 타임아웃 정책 ──────────────────────────────────────────────
# 작업 유형 감지는 task_state.resolve_task_mode 사용 (키워드 단일 소스).


@dataclass
class TimeoutConfig:
    """활동 기반 타임아웃 설정. 환경변수로 오버라이드 가능."""
    analysis_max: int = 1800       # 분석 단계 최대 30분
    modification_max: int = 3600   # 수정 단계 최대 60분
    hard_limit: int = 4200         # 전체 Hard Limit 70분
    inactivity: int = 900          # 무출력 타임아웃 900초
    runtime_heartbeat: int = 15    # durable execution-owner lease refresh
    runtime_lease: int = 45        # three missed heartbeats before suspicion

    @classmethod
    def from_env(cls) -> "TimeoutConfig":
        """환경변수에서 설정 로드 (없으면 기본값)."""
        def _env_int(key: str, default: int) -> int:
            try:
                return int(os.environ.get(key, str(default)))
            except (ValueError, TypeError):
                return default
        return cls(
            analysis_max=_env_int("WORKER_ANALYSIS_MAX", 1800),
            modification_max=_env_int("WORKER_MODIFICATION_MAX", 3600),
            hard_limit=_env_int("WORKER_HARD_LIMIT", 4200),
            inactivity=_env_int("WORKER_INACTIVITY", 900),
            runtime_heartbeat=max(1, _env_int("WORKER_RUNTIME_HEARTBEAT", 15)),
            runtime_lease=max(3, _env_int("WORKER_RUNTIME_LEASE", 45)),
        )

    @property
    def summary(self) -> str:
        return (
            f"analysis_max={self.analysis_max}s, "
            f"modification_max={self.modification_max}s, "
            f"hard_limit={self.hard_limit}s, "
            f"inactivity={self.inactivity}s, "
            f"runtime_heartbeat={self.runtime_heartbeat}s, "
            f"runtime_lease={self.runtime_lease}s"
        )


def _detect_task_type(requirement: str) -> str:
    """요구사항 텍스트에서 작업 유형 감지: 'modification' 또는 'analysis'.
    task_state.resolve_task_mode 와 동일 기준(키워드 추정)을 사용한다."""
    return resolve_task_mode(requirement).lower()


# ── droid --auto 레벨 정책 (단일 결정 지점) ─────────────────────
# 기본: ANALYSIS/MODIFICATION 모두 medium.
#   - low 는 코드 탐색만으로도 "insufficient permission" 즉시 실패 발생
#   - high / --skip-permissions-unsafe 는 기본값으로 사용하지 않음
# 오버라이드: KKM_DROID_AUTO_ANALYSIS / KKM_DROID_AUTO_MODIFICATION (low|medium|high)
# planner/tester/reviewer droid 폴백 등 읽기 전용 역할은 KKM_DROID_AUTO_ANALYSIS 공용.
VALID_DROID_AUTO_LEVELS = ("low", "medium", "high")
DEFAULT_DROID_AUTO_LEVEL = "medium"
ENV_DROID_AUTO_ANALYSIS = "KKM_DROID_AUTO_ANALYSIS"
ENV_DROID_AUTO_MODIFICATION = "KKM_DROID_AUTO_MODIFICATION"


def _resolve_auto_level(env_key: str) -> str:
    """env에서 --auto 레벨 로드. 미설정 시 medium, 잘못된 값이면 medium 폴백(+경고 1줄)."""
    raw = (os.environ.get(env_key) or "").strip().lower()
    if not raw:
        return DEFAULT_DROID_AUTO_LEVEL
    if raw in VALID_DROID_AUTO_LEVELS:
        return raw
    print(f"[DroidWorker] {env_key}='{raw}' 잘못된 값 → 기본 '{DEFAULT_DROID_AUTO_LEVEL}' 폴백")
    return DEFAULT_DROID_AUTO_LEVEL


def droid_auto_level(task_mode: str, requirement: str = "") -> str:
    """
    작업 모드에 따른 droid --auto 레벨 (단일 결정 지점).
    - 기본: MODIFICATION / ANALYSIS 모두 medium
    - env 오버라이드: KKM_DROID_AUTO_MODIFICATION / KKM_DROID_AUTO_ANALYSIS
    - task_mode 가 비어 있으면 resolve_task_mode(requirement) 추정 결과 기준으로
      동일하게 분기한다.
    - planner/tester/reviewer 폴백 등 읽기 전용 역할은 droid_auto_level("ANALYSIS")
      로 호출하여 KKM_DROID_AUTO_ANALYSIS 를 공용으로 쓴다.
    """
    mode = resolve_task_mode(requirement, task_mode)
    env_key = (
        ENV_DROID_AUTO_MODIFICATION
        if mode == TASK_MODE_MODIFICATION
        else ENV_DROID_AUTO_ANALYSIS
    )
    return _resolve_auto_level(env_key)


def _run_with_activity_timeout(
    cmd: list[str],
    stdin_data: str | None,
    cwd: str,
    cfg: TimeoutConfig,
    requirement: str,
    task_mode: str = "",
    heartbeat: float = 60.0,
    display_events: bool = False,
    log_prefix: str = "[Worker]",
    env: dict[str, str] | None = None,
    runtime_event: Callable[[dict[str, Any]], None] | None = None,
    execution_id: str = "",
    execution_role: str = "WORKER",
    execution_owner: str = "",
    attempt_id: str = "",
    runtime_name: str = "",
) -> tuple[int, str, str, str | None]:
    """
    Popen + 백그라운드 출력 리더 스레드로 활동 기반 타임아웃 실행.
    반환: (exit_code, stdout, stderr, timeout_reason)
    - timeout_reason 이 None 이면 정상 종료.
    - stdout/stderr 에 출력이 있으면 무활동 타이머를 리셋.
    - 무출력이 cfg.inactivity 초 이상 지속 → 타임아웃.
    - 단계 최대 시간(stage_max) 도달 → 타임아웃.
    - Hard Limit 도달 → 타임아웃 (활동 여부 무관).
    - heartbeat 초마다 생존 로그 출력(경과 시간 / 마지막 출력 이후 초).
      장기 무출력 구간도 "죽은 것"이 아니라 "작업 중"임을 콘솔에 보여준다.
    - display_events=True 면 stream-json 이벤트를 정제된 짧은 줄로 콘솔에
      동시 출력한다 (도구 이름/완료 등 — 파라미터 본문·비밀 미표시).
      타이머용 raw 버퍼와 표시용 라인을 분리해 로그 가독성 유지.
    - log_prefix: heartbeat/이벤트 로그의 프리픽스 (워커/리뷰어 구분).
    - task_type 판정은 task_mode(명시) 우선 — 게이트와 동일 기준으로 통일.
      미지정 시에만 requirement 키워드 추정(fallback).
    """
    task_type = resolve_task_mode(requirement, task_mode).lower()
    stage_max = cfg.modification_max if task_type == "modification" else cfg.analysis_max

    assert_secret_free_argv(cmd, env=env)
    safe_cmd = list(cmd)
    safe_cmd[0] = trusted_executable(safe_cmd[0], forbidden_root=cwd)
    proc = subprocess.Popen(
        safe_cmd,
        stdin=subprocess.PIPE if stdin_data is not None else subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=cwd,
        env=env,
    )

    execution_id = execution_id or ("EXEC-" + uuid.uuid4().hex.upper())
    execution_owner = execution_owner or (
        f"pid:{os.getpid()}:thread:{threading.get_ident()}"
    )
    reader_errors: list[str] = []
    runtime_persistence_error: list[str | None] = [None]

    def emit(kind: str, **values: Any) -> None:
        if runtime_event is None:
            return
        now_dt = datetime.now().astimezone()
        event = {
            "event": kind,
            "execution_id": execution_id,
            "execution_owner": execution_owner,
            "execution_role": execution_role,
            "attempt_id": attempt_id,
            "runtime": runtime_name,
            "pid": int(getattr(proc, "pid", 0) or 0),
            "observed_at": now_dt.isoformat(),
            "last_runtime_heartbeat_at": now_dt.isoformat(),
            "lease_expires_at": (
                now_dt + timedelta(seconds=max(3, cfg.runtime_lease))
            ).isoformat(),
            "process_alive": proc.poll() is None,
            "progress_timeout_seconds": int(cfg.inactivity),
            **values,
        }
        try:
            runtime_event(event)
        except Exception as exc:
            # Heartbeat persistence is safety evidence.  Do not hide its loss
            # behind a model retry or allow it to terminate pipe draining.
            message = (
                f"runtime heartbeat error: {type(exc).__name__}: {scrub_secrets(exc)}"
            )
            reader_errors.append(message)
            runtime_persistence_error[0] = message

    # stdin 전송 (codex)
    if stdin_data is not None:
        try:
            proc.stdin.write(stdin_data)
            proc.stdin.close()
        except Exception:
            pass

    stdout_chunks: list[str] = []
    stderr_chunks: list[str] = []
    last_output = [time.monotonic()]
    last_progress_at = [datetime.now().astimezone().isoformat()]
    stop_flag = threading.Event()

    def _reader(stream, buf: list[str], echo: bool = False):
        try:
            for line in stream:
                buf.append(line)
                last_output[0] = time.monotonic()
                last_progress_at[0] = datetime.now().astimezone().isoformat()
                if echo:
                    shown = _fmt_droid_event(line)
                    if shown:
                        print(f"{log_prefix} {shown}", flush=True)
        except Exception as exc:
            reader_errors.append(f"{type(exc).__name__}: {scrub_secrets(exc)}")

    t_out = threading.Thread(
        target=_reader, args=(proc.stdout, stdout_chunks, display_events), daemon=True
    )
    t_err = threading.Thread(target=_reader, args=(proc.stderr, stderr_chunks), daemon=True)
    t_out.start()
    t_err.start()

    start = time.monotonic()
    last_heartbeat = start
    last_runtime_heartbeat = start
    last_reported_output = last_output[0]
    last_progress_emit = start
    progress_emit_interval = max(1.0, min(5.0, float(cfg.runtime_heartbeat)))
    timeout_reason: str | None = None

    emit(
        "STARTED",
        started_at=datetime.now().astimezone().isoformat(),
        last_progress_at=last_progress_at[0],
        runtime_health="VALID",
        progress_health="RECENT",
    )

    while True:
        ret = proc.poll()
        if ret is not None:
            break
        if runtime_persistence_error[0]:
            timeout_reason = "runtime heartbeat persistence failed"
            break

        now = time.monotonic()
        elapsed = now - start
        idle = now - last_output[0]

        if (
            last_output[0] > last_reported_output
            and now - last_progress_emit >= progress_emit_interval
        ):
            last_reported_output = last_output[0]
            last_progress_emit = now
            emit(
                "PROGRESS",
                last_progress_at=last_progress_at[0],
                runtime_health="VALID",
                progress_health="RECENT",
            )

        if now - last_runtime_heartbeat >= max(1, cfg.runtime_heartbeat):
            last_runtime_heartbeat = now
            emit(
                "HEARTBEAT",
                last_progress_at=last_progress_at[0],
                runtime_health="VALID",
                progress_health=(
                    "STALLED" if idle >= cfg.inactivity else "RECENT"
                ),
            )

        if elapsed >= cfg.hard_limit:
            timeout_reason = f"Hard limit 초과 ({cfg.hard_limit}s, task={task_type})"
            break
        if elapsed >= stage_max:
            timeout_reason = f"단계 최대 시간 초과 ({stage_max}s, task={task_type})"
            break
        if idle >= cfg.inactivity:
            timeout_reason = (
                f"무출력 타임아웃 ({cfg.inactivity}s, "
                f"경과={elapsed:.0f}s, task={task_type})"
            )
            break

        # 생존 heartbeat: hang 처럼 보이는 장기 무출력 구간의 가시화
        if now - last_heartbeat >= heartbeat:
            last_heartbeat = now
            print(
                f"{log_prefix} 💓 실행 중 — 경과 {elapsed:.0f}s, "
                f"마지막 출력 {idle:.0f}초 전 "
                f"(무활동 기준 {cfg.inactivity}s)",
                flush=True,
            )

        time.sleep(0.5)

    if timeout_reason:
        emit(
            "TIMEOUT",
            last_progress_at=last_progress_at[0],
            runtime_health="VALID",
            progress_health="STALLED",
            termination_reason=timeout_reason,
        )
        if os.name == "nt" and getattr(proc, "pid", None):
            try:
                subprocess.run(
                    [
                        trusted_executable("taskkill", forbidden_root=cwd),
                        "/PID",
                        str(proc.pid),
                        "/T",
                        "/F",
                    ],
                    capture_output=True,
                    text=True,
                    timeout=10,
                    env=env,
                )
            except Exception:
                proc.kill()
        else:
            proc.kill()
    try:
        proc.wait(timeout=10)
    except Exception:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)

    # Process exit first, then drain both pipes to EOF, then join readers.
    t_out.join(timeout=10)
    t_err.join(timeout=10)
    for stream in (proc.stdout, proc.stderr):
        if stream and hasattr(stream, "close") and not getattr(stream, "closed", False):
            stream.close()
    if t_out.is_alive() or t_err.is_alive():
        reader_errors.append("reader thread did not reach EOF")
    stop_flag.set()

    stdout = scrub_secrets("".join(stdout_chunks))
    stderr = scrub_secrets("".join(stderr_chunks))
    if reader_errors:
        stderr = (stderr + "\n[ReaderError] " + "; ".join(reader_errors)).strip()
        if timeout_reason is None:
            timeout_reason = "output reader error"
    exit_code = proc.returncode if proc.returncode is not None else -1

    emit(
        "ENDED",
        last_progress_at=last_progress_at[0],
        ended_at=datetime.now().astimezone().isoformat(),
        exit_code=exit_code,
        termination_reason=timeout_reason or ("EXITED" if exit_code == 0 else "NONZERO_EXIT"),
        runtime_health="ENDED",
        progress_health="TERMINAL",
        process_alive=False,
    )

    return exit_code, stdout, stderr, timeout_reason


# ── stream-json 출력 정제 (콘솔 표시 / 비밀 마스킹 / 최종 텍스트 추출) ──
# 도구 이벤트 파라미터에는 파일 내용·쿼리·자격증명이 실릴 수 있다.
# 콘솔/로그(.tasks/*.log)로 흘러가는 모든 것은 최소 정보만, 비밀은 마스킹.

def _scrub_secrets(text: str) -> str:
    """콘솔/로그로 나가는 텍스트에서 공통 비밀 패턴을 마스킹."""
    return scrub_secrets(text)


def _fmt_droid_event(line: str) -> str | None:
    """stream-json/JSONL 1줄을 콘솔 표시용 짧은 줄로 변환.
    표시하지 않을 이벤트(reasoning/파라미터 본문 등)는 None 반환.
    - tool_call: 도구 이름만 (파라미터에 파일 내용/쿼리가 실릴 수 있어 미표시)
    - reasoning: 미출력 (내부 추론 — 노이즈+노출 최소화)
    - codex --json 이벤트(thread.started/item.*)도 동일 정책으로 표시
    """
    try:
        ev = json.loads(line)
    except ValueError:
        return None
    etype = ev.get("type")
    if etype == "system":
        return "▶ 세션 시작"
    if etype == "thread.started":
        return "▶ 세션 시작"
    if etype == "tool_call":
        return f"🛠 {ev.get('toolName') or '?'}"
    if etype in ("item.started", "item.updated"):
        item = ev.get("item") or {}
        if item.get("type") == "command_execution":
            command = str(item.get("command") or "").lower()
            if re.search(r"\b(rg|grep|findstr|select-string)\b", command):
                return "🛠 검색"
            if re.search(r"\b(get-content|type|head|tail)\b", command):
                return "🛠 파일 읽기"
            if re.search(r"\bgit\s+(status|diff|log|show)\b", command):
                return "🛠 Git 확인"
            if re.search(r"\b(pytest|unittest|mvnw?|gradlew?|npm|pnpm|yarn)\b", command):
                return "🛠 테스트/빌드"
            if "apply_patch" in command:
                return "🛠 파일 수정"
            return "🛠 셸 실행"
        return None
    if etype == "item.completed":
        item = ev.get("item") or {}
        if item.get("type") in ("agent_message", "reasoning"):
            return None    # 본문/추론 미표시 — 노이즈·노출 최소화
        return "✔ 항목 완료"
    if etype == "turn.completed":
        return "✔ 완료"
    if etype == "tool_result":
        return "  ✖ 도구 실패" if ev.get("isError") else None
    if etype == "message" and ev.get("role") == "assistant":
        first = (ev.get("text") or "").strip().splitlines()
        preview = first[0][:100] if first else ""
        return f"💬 {preview}" if preview else None
    if etype == "completion":
        return "✔ 완료"
    return None


def _raw_tail(stdout: str, max_lines: int = 30, max_chars: int = 300) -> str:
    """추출 실패 시 폴백: raw stdout 끝부분(비밀 마스킹) 반환. 리뷰/handoff 단서 보존."""
    lines = [l[:max_chars] for l in stdout.splitlines() if l.strip()]
    if not lines:
        return ""
    return (
        "[응답 파싱 실패 — raw 마지막 부분]\n"
        + _scrub_secrets("\n".join(lines[-max_lines:]))
    )


def _extract_droid_text(stdout: str) -> str:
    """
    droid stream-json 또는 codex --json 출력에서 최종 응답 텍스트를 추출.
    - completion.finalText 우선, 없으면 마지막 assistant message text.
    - 스키마 변경 등으로 둘 다 없으면 raw 끝부분(마스킹)을 폴백으로 반환
      (빈 stdout 방지 — 리뷰/handoff가 최소한의 단서를 유지).
    - 비JSON(구형 text 포맷 등)이면 원본 그대로 반환 (하위 호환).
    downstream(reporter/handoff/reviewer 가 읽는 worker_stdout)은
    사람이 읽는 최종 텍스트를 기대한다. 비밀 패턴은 마스킹해 전달.
    """
    if not stdout or not stdout.lstrip().startswith("{"):
        return _scrub_secrets(stdout)
    final_text = ""
    last_assistant = ""
    for line in stdout.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            ev = json.loads(line)
        except ValueError:
            continue
        etype = ev.get("type")
        if etype == "completion" and ev.get("finalText"):
            final_text = ev["finalText"]
        elif etype == "message" and ev.get("role") == "assistant" and ev.get("text"):
            last_assistant = ev["text"]
        elif etype == "item.completed":
            item = ev.get("item") or {}
            if item.get("type") == "agent_message" and item.get("text"):
                last_assistant = item["text"]
    if final_text:
        return _scrub_secrets(final_text)
    if last_assistant:
        return _scrub_secrets(last_assistant)
    return _raw_tail(stdout)


def _print_failure_detail(wr: WorkerResult) -> None:
    """워커 실패 시 stderr/stdout 끝부분을 콘솔에 출력 (가시화).
    "Worker 실행 실패" 단독 메시지 해소: 실제 droid/codex 에러 메시지가 보이게 함.
    """
    print(f"\n{'!'*60}")
    print(f"[워커 실패 상세] exit_code={wr.exit_code}")
    if wr.stderr:
        tail = _scrub_secrets(wr.stderr.strip())
        if len(tail) > 800:
            tail = "...(앞부분 생략)...\n" + tail[-800:]
        print(f"[stderr]\n{tail}")
    if wr.stdout:
        tail = _scrub_secrets(wr.stdout.strip())
        if len(tail) > 800:
            tail = "...(앞부분 생략)...\n" + tail[-800:]
        print(f"[stdout]\n{tail}")
    print(f"{'!'*60}\n")


def _apply_to_state(task: "TaskState | None", wr: WorkerResult) -> WorkerResult:
    """
    WorkerResult → TaskState 매핑(State 동기화).
    핵심 로직(API 호출 결과 객체)은 변경하지 않고, task 필드만 채운다.
    wr 자체는 그대로 반환하여 기존 흐름 유지.
    실패 시 stderr/stdout 끝부분을 콘솔에 출력하여 원인 가시화.
    """
    if task is None:
        if not wr.success:
            _print_failure_detail(wr)
        return wr
    task.exit_code = wr.exit_code
    task.worker_stdout = _scrub_secrets(wr.stdout or "")
    task.worker_stderr = _scrub_secrets(wr.stderr or "")
    task.actual_executable = _scrub_secrets(wr.actual_executable)
    task.actual_invocation_id = wr.actual_invocation_id
    task.worker_execution_evidence = {
        "actual_executable": task.actual_executable,
        "actual_invocation_id": task.actual_invocation_id,
        "argv": [_scrub_secrets(str(item)) for item in wr.command],
        "started_at": wr.started_at,
        "finished_at": wr.finished_at,
        "exit_code": wr.exit_code,
        "timed_out": bool(wr.timed_out),
    }
    wr.qa_request = parse_qa_request(wr.stdout)
    task.qa_request = dict(wr.qa_request)
    # 실패 가시화: 실제 워커 에러 메시지를 콘솔에 출력
    if not wr.success:
        _print_failure_detail(wr)
    return wr


class DroidWorker:
    """일반 워커: droid exec 호출. 프롬프트를 임시 파일로 전달(Windows 안정성)."""

    def __init__(
        self,
        timeout: int = 180,
        model: str = DROID_DEFAULT_CODING_MODEL,
        timeout_config: TimeoutConfig | None = None,
        profile: ProjectProfile | None = None,
        policy_root: Path | None = None,
        manifest_root: Path | None = None,
    ):
        self.timeout = timeout
        self.model = model
        self.timeout_config = timeout_config
        self.profile = profile
        self.policy_root = Path(policy_root) if policy_root is not None else None
        self.manifest_root = Path(manifest_root) if manifest_root is not None else None

    def execute(self, requirement: str, working_dir: str, task: TaskState | None = None) -> WorkerResult:
        """
        task 가 전달되면 state.requirement/target_module 을 읽어 프롬프트에 반영하고,
        실행 결과를 state.worker_stdout/stderr/exit_code 에 매핑한다.
        핵심 subprocess 호출부는 변경하지 않는다.
        """
        work_path = Path(working_dir).resolve()
        if not work_path.exists():
            raise FileNotFoundError(f"작업 디렉토리가 존재하지 않음: {work_path}")

        from droid_runtime_adapter import DroidRuntimeAdapter
        from runtime_adapter import CommandDelivery, normalize_json_lines, payload_sha256

        adapter = DroidRuntimeAdapter()
        runtime_v2 = bool(
            task is not None and task.control_repository_path
            and task.execution_id and task.attempt_id
        )
        descriptor = adapter.preflight() if runtime_v2 else None
        if task is not None and descriptor is not None:
            task.runtime_adapter = descriptor.public()
        if descriptor is not None and not descriptor.schema_compatible:
            return _apply_to_state(task, WorkerResult(
                False, -1, "", descriptor.detail, [adapter.executable],
                execution_failure_code="DROID_SCHEMA_PREFLIGHT_FAILED",
            ))
        drift = _runtime_contract_failure(task, descriptor, "WORKER") if descriptor is not None else ""
        if drift:
            return _apply_to_state(task, WorkerResult(
                False, -1, "", drift, [adapter.executable], execution_failure_code=drift,
            ))
        repository = None
        binding = None
        prior_command = None
        if runtime_v2:
            from control_repository import ControlRepository
            repository = ControlRepository(task.control_repository_path)
            process_state = repository.reconcile_runtime_process(
                task.attempt_id, process_alive=lambda pid, _started="": CodexWorker._pid_alive(pid),
            )
            if process_state["action"] == "OLD_WRITER_ACTIVE":
                return _apply_to_state(task, WorkerResult(
                    False, -1, "", "OLD_RUNTIME_WRITER_ACTIVE", [adapter.executable],
                    execution_failure_code="OLD_RUNTIME_WRITER_ACTIVE",
                ))
            binding = repository.session_binding(task.execution_id, "droid", role="WORKER")
            reconciliation = repository.reconcile_command(task.execution_id)
            prior_command = reconciliation.get("command")
            if prior_command and prior_command["delivery_status"] in {
                CommandDelivery.SENT.value, CommandDelivery.UNKNOWN.value,
            }:
                if not binding or task.runtime_recovery_count <= 0:
                    task.retry_domain = "TECHNICAL_EXECUTION_RETRY"
                    return _apply_to_state(task, WorkerResult(
                        False, -1, "", "COMMAND_ACK_RECONCILIATION_REQUIRED", [adapter.executable],
                        execution_failure_code="COMMAND_ACK_RECONCILIATION_REQUIRED",
                    ))
                repository.set_command_delivery(
                    prior_command["command_id"], CommandDelivery.ACKNOWLEDGED.value,
                    native_command_id=binding["native_session_id"],
                )
                recovery = repository.record_runtime_recovery(
                    task.execution_id, task.attempt_id,
                    failure_code="DROID_PROCESS_OR_STREAM_DISCONNECT",
                    binding_id=binding["binding_id"], state="RESUMED",
                )
                task.runtime_recovery_history.append(recovery)
                task.retry_domain = "RUNTIME_RECOVERY"

        # State 우선: task 가 있으면 task.requirement/target_module 사용
        eff_requirement = task.requirement if task is not None else requirement
        target_module = task.target_module if task is not None else ""
        # Handoff: 재시도 시 이전 실패 요약을 프롬프트에 부착(subprocess 호출부는 미변경)
        handoff_ctx = getattr(task, "handoff_context", "") if task is not None else ""
        if handoff_ctx:
            eff_requirement = f"{eff_requirement}\n\n[이전 시도 피드백]\n{handoff_ctx}"
        # 플래너 구현 브리프 주입 (use_planner 작업에서만, 프롬프트에만 반영)
        worker_brief = ""
        if task is not None:
            worker_brief = (task.planner_brief or {}).get("worker_brief", "")
        if worker_brief:
            eff_requirement = (
                f"{eff_requirement}\n\n[구현 브리프(플래너)]\n{worker_brief}"
            )
        # --auto 레벨: 기본 medium(모든 모드), env(KKM_DROID_AUTO_*)로 오버라이드.
        # 핸드오프 텍스트가 아닌 순수 요구사항 기준으로 판정한다.
        eff_task_mode = task.task_mode if task is not None else ""
        prompt = build_prompt(
            eff_requirement, target_module, eff_task_mode,
            task_id=task.task_id if task is not None else "",
            profile=self.profile,
            role="droid",
            policy_root=self.policy_root,
            changed_files=task.changed_files if task is not None else None,
            task=task,
            manifest_root=self.manifest_root,
        )
        if binding:
            from context_diet import durable_continuation_prompt
            prompt = durable_continuation_prompt()
        auto_requirement = task.requirement if task is not None else requirement
        auto_level = droid_auto_level(eff_task_mode, auto_requirement)
        resolved_mode = resolve_task_mode(auto_requirement, eff_task_mode)

        # 프롬프트를 임시 파일로 저장 (여러 줄 / 길이 문제 해결)
        with tempfile.NamedTemporaryFile(
            dir=scratch_root(),
            mode="w",
            suffix=".txt",
            delete=False,
            encoding="utf-8",
        ) as f:
            f.write(prompt)
            prompt_file = f.name

        cmd = [
            "droid",
            "exec",
            "--auto", auto_level,
            "-m", self.model,
            "--cwd", str(work_path),
            "-f", prompt_file,
            # stream-json: 도구 호출/추론 이벤트마다 즉시 출력.
            # 기본 text 포맷은 작업 완료 시점에 최종 텍스트만 한 번에 출력하므로
            # 장기 작업 중 stdout 이 15분+ 침묵 → 무활동 타임아웃이 살아있는
            # 워커를 죽이는 오판이 발생한다. stream-json 은 이벤트 단위 출력으로
            # 무활동 타이머가 실제 활동마다 리셋된다.
            "-o", "stream-json",
        ]
        if binding:
            cmd.extend(["--session-id", binding["native_session_id"]])
        if repository is not None:
            command_key = (
                f"{task.attempt_id}:droid-resume:{task.runtime_recovery_count}"
                if binding else f"{task.attempt_id}:droid-initial"
            )
            prepared = repository.prepare_command(
                task.execution_id,
                task.attempt_id,
                idempotency_key=command_key,
                payload_sha256=payload_sha256({"prompt": prompt}),
                binding_id=binding["binding_id"] if binding else "",
            )
            if prepared["delivery_status"] not in {
                CommandDelivery.PREPARED.value,
                CommandDelivery.FAILED_BEFORE_SEND.value,
            }:
                return _apply_to_state(task, WorkerResult(
                    False, -1, "", "COMMAND_ALREADY_DELIVERED", cmd,
                    execution_failure_code="COMMAND_ACK_RECONCILIATION_REQUIRED",
                ))
            task.command_id = prepared["command_id"]
            repository.set_command_delivery(task.command_id, CommandDelivery.SENT.value)
        profile_dir = (
            self.profile.profile_dir
            if self.profile else getattr(task, "profile_dir", "") or None
        )
        child_env = isolated_subprocess_env("droid", profile_dir)

        # 병렬 슬롯 태그 (worker_no 있으면 Worker1/2/3 으로 콘솔 식별)
        wtag = f"[Worker{task.worker_no}]" if getattr(task, "worker_no", 0) else ""
        print(f"{wtag}[DroidWorker] 실행 명령: {' '.join(cmd)}")
        print(f"{wtag}[DroidWorker] auto={auto_level} (mode={resolved_mode})")
        print(f"{wtag}[DroidWorker] working_dir: {work_path}")
        print(f"[DroidWorker] prompt_file: {prompt_file}")
        if self.timeout_config:
            print(f"[DroidWorker] 타임아웃 정책: {self.timeout_config.summary}")
        print("-" * 60)

        try:
            invocation_id = "INV-" + uuid.uuid4().hex.upper()
            started_at = datetime.now().astimezone().isoformat()
            # Resolve once before either execution branch.  The activity-timeout
            # helper also applies this boundary, but the legacy subprocess.run
            # fallback must not be able to resolve a project-local shadow binary.
            cmd[0] = trusted_executable(cmd[0], forbidden_root=work_path)
            if self.timeout_config:
                runtime_callback = getattr(task, "_runtime_event_callback", None)
                runtime_options = ({
                    "runtime_event": runtime_callback,
                    "execution_id": invocation_id,
                    "execution_role": "WORKER",
                    "attempt_id": task.attempt_id,
                    "runtime_name": "droid",
                } if callable(runtime_callback) else {})
                exit_code, stdout, stderr, timeout_reason = _run_with_activity_timeout(
                    cmd=cmd,
                    stdin_data=None,
                    cwd=str(work_path),
                    cfg=self.timeout_config,
                    requirement=eff_requirement,
                    task_mode=eff_task_mode,
                    display_events=True,
                    env=child_env,
                    **runtime_options,
                )
                raw_stdout = stdout
                wr = WorkerResult(
                    success=(exit_code == 0 and not timeout_reason),
                    exit_code=exit_code,
                    stdout=_extract_droid_text(stdout),
                    stderr=(stderr + f"\n[Timeout] {timeout_reason}") if timeout_reason else stderr,
                    command=cmd,
                    timed_out=bool(timeout_reason),
                )
            else:
                assert_secret_free_argv(cmd, env=child_env)
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=self.timeout,
                    cwd=str(work_path),
                    env=child_env,
                )
                raw_stdout = result.stdout or ""
                wr = WorkerResult(
                    success=result.returncode == 0,
                    exit_code=result.returncode,
                    stdout=_extract_droid_text(result.stdout),
                    stderr=_scrub_secrets(result.stderr),
                    command=cmd,
                )
            if repository is not None:
                events = normalize_json_lines(raw_stdout, adapter)
                session_id = next((event.session_id for event in events if event.session_id), "")
                native_turn_id = next((event.turn_id for event in events if event.turn_id), "")
                terminal = next((event for event in reversed(events) if event.terminal), None)
                if binding and not session_id:
                    session_id = binding["native_session_id"]
                if session_id:
                    frozen = dict(task.materialized_execution or {})
                    context = dict(frozen.get("execution_context") or {})
                    binding = repository.bind_native_session(
                        task.execution_id,
                        runtime="droid",
                        native_session_id=session_id,
                        adapter_revision=descriptor.adapter_revision,
                        runtime_version=descriptor.version,
                        durable=True,
                        state="IDLE" if wr.success else "UNKNOWN",
                        role="WORKER",
                        source_view_id=str(frozen.get("source_view_id") or ""),
                        workspace_identity=str(context.get("workspace_identity_sha256") or ""),
                        runtime_contract_hash=str(frozen.get("runtime_contract_hash") or ""),
                        candidate_hash="",
                    )
                    task.native_session_binding_id = binding["binding_id"]
                    task.native_session_id = session_id
                    repository.attach_command_binding(task.command_id, binding["binding_id"])
                    repository.set_command_delivery(
                        task.command_id,
                        CommandDelivery.ACKNOWLEDGED.value,
                        native_command_id=session_id,
                        native_turn_id=native_turn_id,
                    )
                    if native_turn_id:
                        turn = repository.bind_native_turn(
                            binding["binding_id"], task.command_id, native_turn_id,
                            state="SUCCEEDED" if wr.success else "UNKNOWN",
                        )
                        task.native_turn_id = turn["turn_id"]
                current = repository.command(task.command_id)
                if wr.success:
                    if current["delivery_status"] == CommandDelivery.SENT.value:
                        repository.set_command_delivery(
                            task.command_id, CommandDelivery.ACKNOWLEDGED.value
                        )
                    repository.set_command_delivery(
                        task.command_id, CommandDelivery.COMPLETED.value,
                        native_turn_id=native_turn_id,
                    )
                elif current["delivery_status"] == CommandDelivery.SENT.value:
                    repository.set_command_delivery(
                        task.command_id, CommandDelivery.UNKNOWN.value
                    )
                abrupt = not wr.success and terminal is None
                if abrupt and binding and task.runtime_recovery_count < 1:
                    recovery = repository.record_runtime_recovery(
                        task.execution_id, task.attempt_id,
                        failure_code="DROID_PROCESS_OR_STREAM_DISCONNECT",
                        binding_id=binding["binding_id"], state="RESUMED",
                    )
                    task.runtime_recovery_history.append(recovery)
                    task.retry_domain = "RUNTIME_RECOVERY"
                    task.runtime_recovery_count += 1
                    return self.execute(requirement, working_dir, task)
                if abrupt and binding:
                    recovery = repository.record_runtime_recovery(
                        task.execution_id, task.attempt_id,
                        failure_code="DROID_RUNTIME_RECOVERY_EXHAUSTED",
                        binding_id=binding["binding_id"], state="EXHAUSTED",
                    )
                    task.runtime_recovery_history.append(recovery)
                    wr.execution_failure_code = "RUNTIME_RECOVERY_EXHAUSTED"
                    task.retry_domain = "RUNTIME_RECOVERY"
                elif abrupt and not binding:
                    wr.execution_failure_code = "NATIVE_SESSION_RESUME_UNAVAILABLE"
                    task.retry_domain = "TECHNICAL_EXECUTION_RETRY"
            return _apply_to_state(task, _finalize_invocation(wr, invocation_id, started_at))

        except subprocess.TimeoutExpired as e:
            return _apply_to_state(task, _finalize_invocation(WorkerResult(
                success=False,
                exit_code=-1,
                stdout=_scrub_secrets(e.stdout or ""),
                stderr=_scrub_secrets(f"Timeout after {self.timeout}s\n{e.stderr or ''}"),
                command=cmd,
            ), invocation_id, started_at))
        except FileNotFoundError:
            return _apply_to_state(task, _finalize_invocation(WorkerResult(
                success=False,
                exit_code=-1,
                stdout="",
                stderr="droid 명령을 찾을 수 없습니다. PATH에 droid가 있는지 확인하세요.",
                command=cmd,
            ), invocation_id, started_at))
        finally:
            # 임시 파일 정리
            try:
                Path(prompt_file).unlink(missing_ok=True)
            except Exception:
                pass


class OpenCodeWorker:
    """Managed OpenCode server adapter; live calls remain gated by finalization."""

    def __init__(self, timeout: int = 300, model: str = "", reasoning_effort: str = "medium",
                 timeout_config: TimeoutConfig | None = None,
                 profile: ProjectProfile | None = None, policy_root: Path | None = None,
                 manifest_root: Path | None = None):
        self.timeout = timeout
        self.model = model
        self.reasoning_effort = reasoning_effort
        self.timeout_config = timeout_config
        self.profile = profile
        self.policy_root = Path(policy_root) if policy_root is not None else None
        self.manifest_root = Path(manifest_root) if manifest_root is not None else None

    @staticmethod
    def _result(task, *, success=False, stdout="", stderr="", code="", command=None):
        result = WorkerResult(
            success=success, exit_code=0 if success else -1, stdout=stdout,
            stderr=stderr, command=list(command or ["opencode", "serve"]),
            execution_failure_code=code,
        )
        return _apply_to_state(task, _finalize_invocation(
            result, "INV-" + uuid.uuid4().hex.upper(), datetime.now().astimezone().isoformat()
        ))

    @staticmethod
    def _assistant_text(messages: Any) -> str:
        if not isinstance(messages, list):
            return ""
        chunks: list[str] = []
        for message in messages:
            if not isinstance(message, dict):
                continue
            info = message.get("info") if isinstance(message.get("info"), dict) else {}
            if str(info.get("role", "")).casefold() != "assistant":
                continue
            for part in message.get("parts") or []:
                if isinstance(part, dict) and part.get("type") == "text" and part.get("text"):
                    chunks.append(str(part["text"]))
        return "\n".join(chunks[-3:])

    @staticmethod
    def _correlated_assistant_response(
        messages: Any, submitted_message_id: str,
    ) -> tuple[str, str]:
        from opencode_runtime_adapter import OpenCodeAdapterError

        if not isinstance(messages, list) or not submitted_message_id:
            return "", ""
        for message in reversed(messages):
            if not isinstance(message, dict):
                continue
            info = message.get("info") if isinstance(message.get("info"), dict) else {}
            if (str(info.get("role", "")).casefold() != "assistant"
                    or str(info.get("parentID") or "") != submitted_message_id):
                continue
            response_message_id = str(info.get("id") or "")
            if not response_message_id.startswith("msg_"):
                raise OpenCodeAdapterError("OPENCODE_RESPONSE_MESSAGE_ID_INVALID")
            error = info.get("error")
            if error:
                error_name = str(error.get("name") or "") if isinstance(error, dict) else type(error).__name__
                raise OpenCodeAdapterError("OPENCODE_ASSISTANT_ERROR", error_name)
            if str(info.get("finish") or "").casefold() in {"", "tool-calls"}:
                continue
            chunks = [
                str(part.get("text") or "")
                for part in message.get("parts") or []
                if isinstance(part, dict) and part.get("type") == "text" and part.get("text")
            ]
            return response_message_id, "\n".join(chunks)
        return "", ""

    @staticmethod
    def _meaningful_progress_projection(query: Any, messages: Any, submitted_message_id: str) -> dict:
        """Project only turn-correlated semantic state for the progress watchdog.

        Heartbeats, timestamps, token/usage metadata, unrelated session
        messages, and metadata mutations of an unchanged semantic state must
        never reset the inactivity watchdog, so they are excluded here.
        """
        correlated: list[dict] = []
        if isinstance(messages, list) and submitted_message_id:
            for message in messages:
                if not isinstance(message, dict):
                    continue
                info = message.get("info") if isinstance(message.get("info"), dict) else {}
                if (str(info.get("role", "")).casefold() != "assistant"
                        or str(info.get("parentID") or "") != submitted_message_id):
                    continue
                parts: list[dict] = []
                for part in message.get("parts") or []:
                    if not isinstance(part, dict):
                        continue
                    part_type = str(part.get("type") or "")
                    if part_type == "text":
                        parts.append({"type": "text", "text": str(part.get("text") or "")})
                    elif part_type == "tool":
                        # Tool identity plus its lifecycle state transition
                        # (running -> completed/failed) is meaningful; tool
                        # metadata (timing, tokens) is not.
                        parts.append({
                            "type": "tool",
                            "id": str(part.get("id") or ""),
                            "state": str(part.get("state") or part.get("status") or ""),
                        })
                correlated.append({
                    "id": str(info.get("id") or ""),
                    "finish": str(info.get("finish") or ""),
                    "parts": parts,
                })
        return {"state": str(getattr(query, "state", "")), "correlated": correlated}

    @staticmethod
    def _activity_fingerprint(query: Any, messages: Any, submitted_message_id: str) -> str:
        return hashlib.sha256(json.dumps(
            OpenCodeWorker._meaningful_progress_projection(
                query, messages, submitted_message_id
            ),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")).hexdigest()

    @staticmethod
    def _quiesce_native_writer(
        adapter: Any, session_id: str, work_path: Path, *, started_local: bool = False,
    ) -> dict:
        """Close the actual native writer handoff after a technical failure.

        A logical runtime END or DB fencing alone does not prove that a
        persistent native session stopped writing. This performs the native
        session abort and a bounded terminal/idle/lost confirmation. An
        UNKNOWN/transport/timeout result is reported as unconfirmed so the
        caller can deny the next canonical writer. A harness-owned local
        server is instead isolated deterministically by its process-tree
        cleanup in the caller's finally block.
        """
        from runtime_adapter import SessionState

        confirming = {SessionState.IDLE.value, SessionState.LOST.value, SessionState.TERMINAL.value}
        abort_delivered = False
        abort_error = ""
        try:
            abort_delivered = bool(adapter.interrupt(session_id))
        except Exception as exc:  # transport failure / timeout / HTTP error
            abort_error = getattr(exc, "code", type(exc).__name__)
        confirmed_state = ""
        detail = abort_error
        if started_local:
            confirmed_state = SessionState.LOST.value
        else:
            # Iteration cap plus wall-clock deadline: robust even when the
            # process clock is mocked in tests.
            budget = max(1, int(min(10.0, float(getattr(adapter, "timeout", 10.0) or 10.0)) / 0.25))
            deadline = time.monotonic() + 10.0
            while budget > 0 and time.monotonic() < deadline:
                budget -= 1
                try:
                    query = adapter.query_session(session_id, cwd=work_path)
                except Exception as exc:
                    detail = getattr(exc, "code", type(exc).__name__)
                    query = None
                if query is not None:
                    state = str(getattr(query, "state", ""))
                    if state in confirming:
                        confirmed_state = state
                        break
                    if state:
                        detail = state
                time.sleep(0.25)
        evidence = {
            "abort_delivered": bool(abort_delivered),
            "abort_error": abort_error,
            "confirmed_state": confirmed_state,
            "detail": detail[:200],
            "quiesced": confirmed_state in confirming,
        }
        if started_local:
            evidence["isolation"] = "LOCAL_SERVER_PROCESS_TREE"
        return evidence

    def _wait_for_response(
        self,
        adapter: Any,
        session_id: str,
        native_message_id: str,
        work_path: Path,
        *,
        repository: Any = None,
        binding: Mapping[str, Any] | None = None,
        command_id: str = "",
        requirement: str = "",
        task_mode: str = "",
        task: TaskState | None = None,
    ) -> tuple[str, str]:
        from opencode_runtime_adapter import OpenCodeAdapterError
        from runtime_adapter import CommandDelivery, SessionState

        last_activity = last_heartbeat = time.monotonic()
        fingerprint = ""
        cfg = self.timeout_config
        activity_timeout = float(cfg.inactivity if cfg is not None else self.timeout)
        heartbeat_interval = max(
            1.0, float(cfg.runtime_heartbeat if cfg is not None else min(30, self.timeout))
        )
        lease_seconds = max(
            3.0, float(cfg.runtime_lease if cfg is not None else heartbeat_interval * 3)
        )
        runtime_event = getattr(task, "_runtime_event_callback", None)
        invocation_id = "INV-" + uuid.uuid4().hex.upper()
        started_at = datetime.now().astimezone().isoformat()
        last_progress_at = started_at
        ended = False

        def emit(kind: str, **values: Any) -> None:
            if not callable(runtime_event):
                return
            now_dt = datetime.now().astimezone()
            event = {
                "event": kind,
                "execution_id": invocation_id,
                "execution_owner": f"pid:{os.getpid()}:thread:{threading.get_ident()}",
                "execution_role": "WORKER",
                "attempt_id": getattr(task, "attempt_id", ""),
                "runtime": "opencode",
                "pid": os.getpid(),
                "observed_at": now_dt.isoformat(),
                "last_runtime_heartbeat_at": now_dt.isoformat(),
                "lease_expires_at": (
                    now_dt + timedelta(seconds=lease_seconds)
                ).isoformat(),
                "process_alive": kind != "ENDED",
                "progress_timeout_seconds": int(activity_timeout),
                **values,
            }
            try:
                runtime_event(event)
            except Exception as exc:
                raise OpenCodeAdapterError(
                    "OPENCODE_RUNTIME_HEARTBEAT_PERSISTENCE_FAILED",
                    type(exc).__name__,
                ) from exc

        emit(
            "STARTED", started_at=started_at, last_progress_at=last_progress_at,
            runtime_health="VALID", progress_health="RECENT",
        )
        try:
            while True:
                query = adapter.query_session(session_id, cwd=work_path)
                messages = adapter.request(
                    "GET", f"/session/{session_id}/message?limit=100"
                )
                observed = time.monotonic()
                current = self._activity_fingerprint(query, messages, native_message_id)
                if current != fingerprint:
                    fingerprint = current
                    last_activity = observed
                    last_progress_at = datetime.now().astimezone().isoformat()
                    if repository is not None and binding:
                        repository.update_session_state(binding["binding_id"], query.state)
                    emit(
                        "PROGRESS", last_progress_at=last_progress_at,
                        runtime_health="VALID", progress_health="RECENT",
                    )
                response_message_id, output = self._correlated_assistant_response(
                    messages, native_message_id
                )
                if query.state == SessionState.IDLE.value and response_message_id:
                    if repository is not None and command_id:
                        repository.set_command_delivery(
                            command_id,
                            CommandDelivery.COMPLETED.value,
                            native_response_message_id=response_message_id,
                        )
                    emit(
                        "ENDED", last_progress_at=last_progress_at,
                        ended_at=datetime.now().astimezone().isoformat(), exit_code=0,
                        termination_reason="EXITED", runtime_health="ENDED",
                        progress_health="TERMINAL", process_alive=False,
                    )
                    ended = True
                    return response_message_id, output
                if query.state == SessionState.WAITING_INPUT.value:
                    raise OpenCodeAdapterError(
                        "OPENCODE_INTERACTION_PENDING", query.pending_kind or ""
                    )
                idle = observed - last_activity
                if observed - last_heartbeat >= heartbeat_interval:
                    last_heartbeat = observed
                    emit(
                        "HEARTBEAT", last_progress_at=last_progress_at,
                        runtime_health="VALID",
                        progress_health="STALLED" if idle >= activity_timeout else "RECENT",
                    )
                if idle >= activity_timeout:
                    emit(
                        "TIMEOUT", last_progress_at=last_progress_at,
                        runtime_health="VALID", progress_health="STALLED",
                        termination_reason="OPENCODE_PROGRESS_TIMEOUT",
                    )
                    raise OpenCodeAdapterError("OPENCODE_PROGRESS_TIMEOUT")
                time.sleep(0.5)
        except Exception as exc:
            if not ended and not (
                isinstance(exc, OpenCodeAdapterError)
                and exc.code == "OPENCODE_RUNTIME_HEARTBEAT_PERSISTENCE_FAILED"
            ):
                emit(
                    "ENDED", last_progress_at=last_progress_at,
                    ended_at=datetime.now().astimezone().isoformat(), exit_code=-1,
                    termination_reason=getattr(exc, "code", type(exc).__name__),
                    runtime_health="ENDED", progress_health="TERMINAL",
                    process_alive=False,
                )
            raise

    def execute(self, requirement: str, working_dir: str, task: TaskState | None = None) -> WorkerResult:
        from opencode_runtime_adapter import MANAGED, OpenCodeAdapterError, OpenCodeRuntimeAdapter
        from runtime_adapter import CommandDelivery, SessionState, payload_sha256
        from runtime_selection import RuntimeBoundary, select_runtime

        work_path = Path(working_dir).resolve()
        selection = select_runtime("opencode", self.model)
        if task is not None:
            task.selection_reason = selection.selection_reason
            task.fallback_availability_evidence = (
                f"runtime={selection.fallback_runtime};activation=explicit-controller-decision"
            )
        if not selection.opencode_live_validated:
            if task is not None:
                task.retry_domain = "RUNTIME_RECOVERY"
                task.user_intervention_reason = "DEFERRED_USER_SETUP"
            return self._result(
                task, stderr="OpenCode provider/auth/live model validation is deferred user setup.",
                code="OPENCODE_PROVIDER_SETUP_DEFERRED",
            )
        base_url = str(os.environ.get("KKM_OPENCODE_SERVER_URL", "")).strip()
        password = str(os.environ.get("OPENCODE_SERVER_PASSWORD", ""))
        username = str(os.environ.get("OPENCODE_SERVER_USERNAME", "opencode"))
        try:
            boundary = RuntimeBoundary.managed(base_url, authenticated=bool(password))
            boundary.assert_operation("PRODUCT_WRITE")
        except Exception as exc:
            return self._result(task, stderr=str(exc), code=getattr(exc, "code", "MANAGED_REMOTE_BOUNDARY_INVALID"))
        adapter = OpenCodeRuntimeAdapter(
            base_url=base_url, mode=MANAGED, username=username, password=password,
            timeout=min(30.0, float(self.timeout)),
        )
        descriptor = adapter.preflight()
        if task is not None:
            task.runtime_adapter = descriptor.public()
        if not descriptor.schema_compatible:
            return self._result(task, stderr=descriptor.detail, code="OPENCODE_SCHEMA_PREFLIGHT_FAILED")
        drift = _runtime_contract_failure(task, descriptor, "WORKER")
        if drift:
            return self._result(task, stderr=drift, code=drift)

        eff_requirement = task.requirement if task is not None else requirement
        handoff = getattr(task, "handoff_context", "") if task is not None else ""
        if handoff:
            eff_requirement += f"\n\n[기술 복구 인계]\n{handoff}"
        prompt = build_prompt(
            eff_requirement, task.target_module if task else "", task.task_mode if task else "",
            task_id=task.task_id if task else "", profile=self.profile, role="opencode",
            policy_root=self.policy_root, changed_files=task.changed_files if task else None,
            task=task, manifest_root=self.manifest_root,
        )
        repository = None
        binding = None
        prior_command = None
        started_local = False
        session_id = ""
        turn_in_flight = False
        in_flight_command_id = ""
        try:
            if not base_url:
                adapter.start_local_server(cwd=work_path)
                started_local = True
            if task is not None and task.control_repository_path and task.execution_id and task.attempt_id:
                from control_repository import ControlRepository
                repository = ControlRepository(task.control_repository_path)
                binding = repository.session_binding(task.execution_id, "opencode")
                if binding:
                    prior_command = repository.reconcile_command(
                        task.execution_id
                    ).get("command")
                # A previous technical failure left the native writer's
                # quiescence unconfirmed: deny a new canonical writer unless a
                # bounded read-only query now proves the session is no longer
                # active. The current candidate/delta stays preserved and the
                # failure remains technical (zero product retry delta).
                unconfirmed = repository.unconfirmed_writer_quiescence(
                    task.execution_id,
                    binding_id=binding["binding_id"] if binding else "",
                )
                if unconfirmed:
                    from runtime_adapter import SessionState as _SessionState
                    reconfirmed_state = ""
                    if binding:
                        try:
                            query = adapter.query_session(
                                binding["native_session_id"], cwd=work_path
                            )
                        except OpenCodeAdapterError:
                            query = None
                        if query is not None:
                            reconfirmed_state = str(getattr(query, "state", ""))
                    if reconfirmed_state not in {
                        _SessionState.IDLE.value,
                        _SessionState.LOST.value,
                        _SessionState.TERMINAL.value,
                    }:
                        task.retry_domain = "TECHNICAL_EXECUTION_RETRY"
                        return self._result(
                            task,
                            stderr="OPENCODE_WRITER_QUIESCENCE_UNCONFIRMED",
                            code="OPENCODE_WRITER_QUIESCENCE_UNCONFIRMED",
                        )
                    repository.update_session_state(
                        binding["binding_id"], reconfirmed_state
                    )
                    repository.record_runtime_recovery(
                        task.execution_id, task.attempt_id,
                        failure_code="OPENCODE_WRITER_QUIESCENCE_CONFIRMED",
                        binding_id=binding["binding_id"], state="RESUMED",
                        evidence={"writer_quiescence": {
                            "abort_delivered": bool(unconfirmed.get("abort_delivered")),
                            "confirmed_state": reconfirmed_state,
                            "quiesced": True,
                            "late_confirmation": True,
                        }},
                    )
            if not binding:
                created = adapter.execute_invocation(adapter.create_session(prompt, cwd=work_path))
                session_id = str(dict(created or {}).get("id") or dict(created or {}).get("sessionID") or "")
                if not session_id:
                    raise OpenCodeAdapterError("OPENCODE_SESSION_CREATE_INVALID")
                if repository is not None:
                    binding = repository.bind_native_session(
                        task.execution_id, runtime="opencode", native_session_id=session_id,
                        adapter_revision=descriptor.adapter_revision, runtime_version=descriptor.version,
                        durable=True, state="IDLE",
                    )
            else:
                session_id = binding["native_session_id"]
            if task is not None:
                task.native_session_id = session_id
                task.native_session_binding_id = binding["binding_id"] if binding else ""
            semantic_retry = bool(task is not None and task.retry_count > 0)
            if prior_command and not semantic_retry:
                prior_status = str(prior_command.get("delivery_status", ""))
                prior_native_id = str(prior_command.get("native_command_id", ""))
                if prior_status in {
                    CommandDelivery.SENT.value,
                    CommandDelivery.UNKNOWN.value,
                } or not prior_native_id:
                    task.retry_domain = "TECHNICAL_EXECUTION_RETRY"
                    return self._result(
                        task,
                        stderr="COMMAND_ACK_RECONCILIATION_REQUIRED",
                        code="COMMAND_ACK_RECONCILIATION_REQUIRED",
                    )
                query = adapter.query_session(session_id, cwd=work_path)
                messages = adapter.request(
                    "GET", f"/session/{session_id}/message?limit=100"
                )
                response_message_id, output = self._correlated_assistant_response(
                    messages, prior_native_id
                )
                if query.state == SessionState.IDLE.value and response_message_id and not prior_command.get("aborted"):
                    # An aborted command's late assistant response is stale
                    # writer output and must never publish as the current result.
                    repository.set_command_delivery(
                        prior_command["command_id"],
                        CommandDelivery.COMPLETED.value,
                        native_response_message_id=response_message_id,
                    )
                    return self._result(
                        task, success=True, stdout=output,
                        command=["opencode", "server", "reconcile"],
                    )
                if query.state != SessionState.IDLE.value:
                    in_flight_command_id = prior_command["command_id"]
                    turn_in_flight = True
                    _, output = self._wait_for_response(
                        adapter, session_id, prior_native_id, work_path,
                        repository=repository, binding=binding,
                        command_id=prior_command["command_id"],
                        requirement=eff_requirement,
                        task_mode=task.task_mode,
                        task=task,
                    )
                    return self._result(
                        task, success=True, stdout=output,
                        command=["opencode", "server", "reconcile"],
                    )
                from context_diet import durable_continuation_prompt
                prompt = durable_continuation_prompt()
                recovery = repository.record_runtime_recovery(
                    task.execution_id,
                    task.attempt_id,
                    failure_code="OPENCODE_TURN_TIMEOUT",
                    binding_id=binding["binding_id"],
                    state="RESUMED",
                )
                task.runtime_recovery_history.append(recovery)
                task.retry_domain = "RUNTIME_RECOVERY"
            command_key = ""
            if task is not None:
                command_key = (
                    f"{task.attempt_id}:opencode:semantic:{task.retry_count + 1}:"
                    f"runtime:{task.runtime_recovery_count + 1}"
                    if semantic_retry
                    else f"{task.attempt_id}:opencode:{task.runtime_recovery_count + 1}"
                )
            canonical_command_id = ""
            if repository is not None:
                prepared = repository.prepare_command(
                    task.execution_id, task.attempt_id, idempotency_key=command_key,
                    payload_sha256=payload_sha256({"prompt": prompt}),
                    binding_id=binding["binding_id"],
                )
                task.command_id = prepared["command_id"]
                if prepared["delivery_status"] != CommandDelivery.PREPARED.value:
                    task.retry_domain = "TECHNICAL_EXECUTION_RETRY"
                    return self._result(
                        task, stderr="COMMAND_ALREADY_DELIVERED",
                        code="COMMAND_ACK_RECONCILIATION_REQUIRED",
                    )
                canonical_command_id = task.command_id
            invocation = adapter.submit_turn(
                session_id, prompt, cwd=work_path, model=selection.model,
                reasoning_effort=self.reasoning_effort,
                command_id=canonical_command_id,
            )
            native_message_id = str(invocation.body.get("messageID") or "")
            if not native_message_id.startswith("msg_"):
                raise OpenCodeAdapterError("OPENCODE_NATIVE_MESSAGE_ID_INVALID")
            if repository is not None:
                repository.set_command_delivery(task.command_id, CommandDelivery.SENT.value)
            # From this point the native command may have been delivered and an
            # actual write is possible; a technical failure must close the old
            # native writer handoff before any new canonical writer is allowed.
            turn_in_flight = True
            in_flight_command_id = task.command_id
            adapter.execute_invocation(invocation)
            if repository is not None:
                repository.set_command_delivery(
                    task.command_id, CommandDelivery.ACKNOWLEDGED.value,
                    native_command_id=native_message_id,
                )
            _, output = self._wait_for_response(
                adapter, session_id, native_message_id, work_path,
                repository=repository, binding=binding,
                command_id=task.command_id if task is not None else "",
                requirement=eff_requirement,
                task_mode=task.task_mode if task is not None else "",
                task=task,
            )
            return self._result(
                task, success=True, stdout=output,
                command=["opencode", "server", "prompt_async"],
            )
        except OpenCodeAdapterError as exc:
            if exc.code == "OPENCODE_INTERACTION_PENDING":
                qa = {
                    "required": True,
                    "qa_type": "TOOL_PERMISSION",
                    "hold_scope": "JOB",
                    "machine_verified": False,
                    "reason": exc.detail or "OpenCode interaction pending",
                }
                return self._result(
                    task,
                    stdout=QA_REQUEST_PREFIX + json.dumps(qa, ensure_ascii=False),
                    stderr="OpenCode permission/question pending",
                    code=exc.code,
                )
            if repository is not None and task.command_id:
                current = repository.command(task.command_id)
                if current and current["delivery_status"] == CommandDelivery.SENT.value:
                    repository.set_command_delivery(task.command_id, CommandDelivery.UNKNOWN.value)
                writer_quiescence = None
                if turn_in_flight and session_id:
                    # Close the actual native writer handoff: abort the native
                    # session, confirm terminal/idle/lost within a bound, and
                    # keep the stale response non-publishable. An unconfirmed
                    # result denies the next canonical writer (see admission
                    # check above) without spending product retries.
                    writer_quiescence = self._quiesce_native_writer(
                        adapter, session_id, work_path, started_local=started_local,
                    )
                    if in_flight_command_id:
                        repository.mark_command_aborted(
                            in_flight_command_id, reason=exc.code
                        )
                    if binding:
                        repository.update_session_state(
                            binding["binding_id"],
                            writer_quiescence["confirmed_state"]
                            or SessionState.UNKNOWN.value,
                        )
                recovery = repository.record_runtime_recovery(
                    task.execution_id, task.attempt_id, failure_code=exc.code,
                    binding_id=binding["binding_id"] if binding else "", state="FAILED",
                    evidence=(
                        {"writer_quiescence": writer_quiescence}
                        if writer_quiescence is not None else None
                    ),
                )
                task.runtime_recovery_count += 1
                task.runtime_recovery_history.append(recovery)
                task.retry_domain = "RUNTIME_RECOVERY"
            # Preserve the adapter's original failure code (progress timeout,
            # assistant error, transport error, ...) instead of collapsing it;
            # the classification below stays technical/infrastructure.
            return self._result(task, stderr=exc.code, code=exc.code)
        finally:
            if started_local:
                adapter.stop_local_server()


class CodexWorker:
    """Codex worker with durable native-session resume and command fencing."""

    def __init__(self, timeout: int = 300, model: str | None = None,
                 reasoning_effort: str = "medium", timeout_config: TimeoutConfig | None = None,
                 profile: ProjectProfile | None = None, policy_root: Path | None = None,
                 manifest_root: Path | None = None):
        self.timeout = timeout
        self.model = model
        self.reasoning_effort = reasoning_effort if reasoning_effort in ("low", "medium", "high") else "medium"
        self.timeout_config = timeout_config
        self.profile = profile
        self.policy_root = Path(policy_root) if policy_root is not None else None
        self.manifest_root = Path(manifest_root) if manifest_root is not None else None

    @staticmethod
    def _pid_alive(pid: int, _started_at: str = "") -> bool:
        if pid <= 0:
            return False
        try:
            os.kill(pid, 0)
            return True
        except PermissionError:
            return True
        except OSError:
            return False

    def _run(self, cmd: list[str], prompt: str, *, work_path: Path,
             requirement: str, task_mode: str, child_env: dict[str, str],
             task: TaskState | None, wtag: str) -> tuple[WorkerResult, str]:
        invocation_id = "INV-" + uuid.uuid4().hex.upper()
        started_at = datetime.now().astimezone().isoformat()
        cmd[0] = trusted_executable(cmd[0], forbidden_root=work_path)
        if not codex_host_pair_matches(cmd[0]):
            return _finalize_invocation(WorkerResult(
                False, -1, "", "CODEX_EXECUTION_SURFACE_MISMATCH", cmd,
                execution_failure_code="CODEX_EXECUTION_SURFACE_MISMATCH",
            ), invocation_id, started_at), ""
        try:
            if self.timeout_config:
                callback = getattr(task, "_runtime_event_callback", None)
                options = {"runtime_event": callback, "execution_id": invocation_id,
                           "execution_role": "WORKER"} if callable(callback) else {}
                exit_code, raw_stdout, stderr, timeout_reason = _run_with_activity_timeout(
                    cmd=cmd, stdin_data=prompt, cwd=str(work_path), cfg=self.timeout_config,
                    requirement=requirement, task_mode=task_mode, display_events=True,
                    log_prefix=wtag or "[Worker]", env=child_env, **options,
                )
                result = WorkerResult(
                    success=exit_code == 0 and not timeout_reason, exit_code=exit_code,
                    stdout=_extract_droid_text(raw_stdout),
                    stderr=(stderr + f"\n[Timeout] {timeout_reason}") if timeout_reason else stderr,
                    command=cmd, timed_out=bool(timeout_reason),
                )
            else:
                assert_secret_free_argv(cmd, env=child_env)
                completed = subprocess.run(
                    cmd, input=prompt, capture_output=True, text=True, encoding="utf-8",
                    errors="replace", timeout=self.timeout, cwd=str(work_path), env=child_env,
                )
                raw_stdout = completed.stdout
                result = WorkerResult(
                    completed.returncode == 0, completed.returncode,
                    _extract_droid_text(raw_stdout), _scrub_secrets(completed.stderr), cmd,
                )
        except subprocess.TimeoutExpired as exc:
            raw_stdout = str(exc.stdout or "")
            result = WorkerResult(False, -1, _scrub_secrets(raw_stdout),
                                  _scrub_secrets(f"Timeout after {self.timeout}s\n{exc.stderr or ''}"), cmd,
                                  timed_out=True)
        except FileNotFoundError:
            raw_stdout = ""
            result = WorkerResult(False, -1, "", "codex executable not found", cmd,
                                  execution_failure_code="CODEX_EXECUTABLE_NOT_FOUND")
        if "failed to decode code-mode IPC frame" in result.stderr:
            result.success = False
            result.execution_failure_code = "WORKER_IPC_DECODE_ERROR"
        return _finalize_invocation(result, invocation_id, started_at), raw_stdout

    def execute(self, requirement: str, working_dir: str, task: TaskState | None = None) -> WorkerResult:
        from codex_runtime_adapter import CodexRuntimeAdapter
        from runtime_adapter import CommandDelivery, SessionState, normalize_json_lines, payload_sha256

        work_path = Path(working_dir).resolve()
        if not work_path.exists():
            raise FileNotFoundError(f"작업 디렉토리가 존재하지 않음: {work_path}")
        eff_requirement = task.requirement if task is not None else requirement
        target_module = task.target_module if task is not None else ""
        task_mode = task.task_mode if task is not None else ""
        handoff = getattr(task, "handoff_context", "") if task is not None else ""
        brief = (task.planner_brief or {}).get("worker_brief", "") if task is not None else ""
        dynamic = eff_requirement
        if handoff:
            dynamic += f"\n\n[이전 시도 피드백]\n{handoff}"
        if brief:
            dynamic += f"\n\n[구현 브리프(플래너)]\n{brief}"
        original_prompt = build_prompt(
            dynamic, target_module, task_mode, task_id=task.task_id if task else "",
            profile=self.profile, role="codex", policy_root=self.policy_root,
            changed_files=task.changed_files if task else None, task=task,
            manifest_root=self.manifest_root,
        )
        adapter = CodexRuntimeAdapter()
        runtime_v2 = bool(
            task is not None
            and task.control_repository_path
            and task.execution_id
            and task.attempt_id
        )
        descriptor = adapter.preflight() if runtime_v2 else None
        if task is not None and descriptor is not None:
            task.runtime_adapter = descriptor.public()
        if descriptor is not None and not descriptor.schema_compatible:
            return _apply_to_state(task, WorkerResult(
                False, -1, "", descriptor.detail, [adapter.executable],
                execution_failure_code="CODEX_SCHEMA_PREFLIGHT_FAILED",
            ))
        drift = _runtime_contract_failure(task, descriptor, "WORKER") if descriptor is not None else ""
        if drift:
            return _apply_to_state(task, WorkerResult(
                False, -1, "", drift, [adapter.executable],
                execution_failure_code=drift,
            ))

        repository = None
        binding: dict[str, Any] | None = None
        prior_command: dict[str, Any] | None = None
        if task is not None and task.control_repository_path and task.execution_id and task.attempt_id:
            from control_repository import ControlRepository
            repository = ControlRepository(task.control_repository_path)
            process_state = repository.reconcile_runtime_process(
                task.attempt_id, process_alive=self._pid_alive,
            )
            if process_state["action"] == "OLD_WRITER_ACTIVE":
                return _apply_to_state(task, WorkerResult(
                    False, -1, "", "OLD_RUNTIME_WRITER_ACTIVE", [adapter.executable],
                    execution_failure_code="OLD_RUNTIME_WRITER_ACTIVE",
                ))
            binding = repository.session_binding(task.execution_id, "codex")
            reconciliation = repository.reconcile_command(task.execution_id)
            prior_command = reconciliation.get("command")
            if prior_command and prior_command["delivery_status"] in {
                CommandDelivery.SENT.value, CommandDelivery.UNKNOWN.value,
            }:
                if not binding:
                    task.retry_domain = "TECHNICAL_EXECUTION_RETRY"
                    return _apply_to_state(task, WorkerResult(
                        False, -1, "", "COMMAND_ACK_UNKNOWN_NO_SESSION", [adapter.executable],
                        execution_failure_code="COMMAND_ACK_RECONCILIATION_REQUIRED",
                    ))
                query = adapter.reconcile(binding["native_session_id"], cwd=work_path)
                if query.state in {SessionState.LOST.value, SessionState.UNKNOWN.value}:
                    repository.record_runtime_recovery(
                        task.execution_id, task.attempt_id,
                        failure_code="NATIVE_SESSION_RESUME_UNAVAILABLE",
                        binding_id=binding["binding_id"], state="FRESH_ATTEMPT_REQUIRED",
                    )
                    task.retry_domain = "TECHNICAL_EXECUTION_RETRY"
                    return _apply_to_state(task, WorkerResult(
                        False, -1, "", "NATIVE_SESSION_RESUME_UNAVAILABLE", [adapter.executable],
                        execution_failure_code="NATIVE_SESSION_RESUME_UNAVAILABLE",
                    ))
                repository.set_command_delivery(
                    prior_command["command_id"], CommandDelivery.ACKNOWLEDGED.value,
                    native_command_id=binding["native_session_id"],
                )
                recovery = repository.record_runtime_recovery(
                    task.execution_id, task.attempt_id, failure_code="PROCESS_OR_STREAM_DISCONNECT",
                    binding_id=binding["binding_id"], state="RESUMED",
                )
                task.runtime_recovery_count += 1
                task.runtime_recovery_history.append(recovery)
                task.retry_domain = "RUNTIME_RECOVERY"

        profile_dir = self.profile.profile_dir if self.profile else getattr(task, "profile_dir", "") or None
        child_env = isolated_subprocess_env("codex", profile_dir)
        wtag = f"[Worker{task.worker_no}]" if getattr(task, "worker_no", 0) else ""
        prompt = original_prompt
        max_runtime_runs = 2 if repository is not None else 1
        last_result: WorkerResult | None = None
        for runtime_run in range(max_runtime_runs):
            if binding:
                # The native session already owns stable contract/context. A
                # recovery turn carries only the changing continuation signal.
                from context_diet import durable_continuation_prompt
                prompt = durable_continuation_prompt()
                command_key = f"{task.attempt_id}:resume:{task.runtime_recovery_count + runtime_run + 1}" if task else ""
                invocation = adapter.resume_session(
                    binding["native_session_id"], prompt, cwd=work_path,
                    model=self.model or "", reasoning_effort=self.reasoning_effort,
                )
            else:
                command_key = f"{task.attempt_id}:initial" if task else ""
                invocation = adapter.create_session(
                    prompt, cwd=work_path, model=self.model or "",
                    reasoning_effort=self.reasoning_effort,
                )
            command = list(invocation.argv)
            if repository is not None:
                prepared = repository.prepare_command(
                    task.execution_id, task.attempt_id, idempotency_key=command_key,
                    payload_sha256=payload_sha256({"prompt": prompt}),
                    binding_id=binding["binding_id"] if binding else "",
                    command_id=invocation.command_id,
                )
                if prepared["delivery_status"] not in {CommandDelivery.PREPARED.value, CommandDelivery.FAILED_BEFORE_SEND.value}:
                    return _apply_to_state(task, WorkerResult(
                        False, -1, "", "COMMAND_ALREADY_DELIVERED", command,
                        execution_failure_code="COMMAND_ACK_RECONCILIATION_REQUIRED",
                    ))
                task.command_id = prepared["command_id"]
                repository.set_command_delivery(task.command_id, CommandDelivery.SENT.value)
            print(f"{wtag}[CodexWorker] {'resume' if binding else 'create'} session · command={task.command_id if task else invocation.command_id}")
            print(f"{wtag}[CodexWorker] working_dir: {work_path}")
            result, raw_stdout = self._run(
                command, prompt, work_path=work_path, requirement=eff_requirement,
                task_mode=task_mode, child_env=child_env, task=task, wtag=wtag,
            )
            last_result = result
            events = normalize_json_lines(raw_stdout, adapter)
            session_id = next((event.session_id for event in events if event.session_id), "")
            native_turn_id = next((event.turn_id for event in events if event.turn_id), "")
            if binding and not session_id:
                session_id = binding["native_session_id"]
            if repository is not None and session_id:
                binding = repository.bind_native_session(
                    task.execution_id, runtime="codex", native_session_id=session_id,
                    adapter_revision=descriptor.adapter_revision,
                    runtime_version=descriptor.version, durable=True,
                    state="IDLE" if result.success else "UNKNOWN",
                )
                task.native_session_binding_id = binding["binding_id"]
                task.native_session_id = session_id
                repository.attach_command_binding(task.command_id, binding["binding_id"])
                repository.set_command_delivery(
                    task.command_id, CommandDelivery.ACKNOWLEDGED.value,
                    native_command_id=session_id, native_turn_id=native_turn_id,
                )
                if native_turn_id:
                    turn = repository.bind_native_turn(
                        binding["binding_id"], task.command_id, native_turn_id,
                        state="SUCCEEDED" if result.success else "UNKNOWN",
                    )
                    task.native_turn_id = turn["turn_id"]
            if repository is not None:
                current = repository.command(task.command_id)
                if result.success:
                    if current["delivery_status"] == CommandDelivery.SENT.value:
                        repository.set_command_delivery(task.command_id, CommandDelivery.ACKNOWLEDGED.value)
                    repository.set_command_delivery(task.command_id, CommandDelivery.COMPLETED.value,
                                                    native_turn_id=native_turn_id)
                elif current["delivery_status"] == CommandDelivery.SENT.value:
                    repository.set_command_delivery(task.command_id, CommandDelivery.UNKNOWN.value)
            if result.success:
                return _apply_to_state(task, result)
            if not binding or runtime_run + 1 >= max_runtime_runs:
                break
            recovery = repository.record_runtime_recovery(
                task.execution_id, task.attempt_id,
                failure_code=result.execution_failure_code or "RUNTIME_PROCESS_FAILURE",
                binding_id=binding["binding_id"], state="RESUMED",
            )
            task.runtime_recovery_count += 1
            task.runtime_recovery_history.append(recovery)
            task.retry_domain = "RUNTIME_RECOVERY"
        assert last_result is not None
        if binding and not last_result.execution_failure_code:
            last_result.execution_failure_code = "RUNTIME_RECOVERY_EXHAUSTED"
        return _apply_to_state(task, last_result)
