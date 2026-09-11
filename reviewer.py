"""
Reviewer - Worker 결과 검증 (fail-closed)
- 빠른 결정적 규칙(exit_code / 분석 전용 위반 / diff 게이트)
- 시맨틱 리뷰: Codex(gpt-5.6-sol) 단일 엔진 (읽기 전용 샌드박스)
- 리뷰어 실행 불가 → REVIEW_UNAVAILABLE, 응답 파싱 실패 → REVIEW_ERROR.
  어느 쪽도 자동 PASS 되지 않는다.
  (MODIFICATION 작업은 Manager 가 이 상태들을 실패로 처리,
   ANALYSIS 작업은 '검증 불완전' 경고로만 기록)
"""

from __future__ import annotations

import json
import hashlib
import os
import re
import subprocess
import tempfile
from harness_temp import scratch_root
from functools import lru_cache
from dataclasses import dataclass, field
from pathlib import Path

from task_state import (
    TaskState,
    resolve_task_mode,
    is_local_dev_config,
    extract_requirement_paths,
    can_verify_current_state,
    TASK_MODE_ANALYSIS,
    TASK_MODE_MODIFICATION,
    CODE_CHANGE_KEYWORDS,       # 보조 추정용 (task_mode 미지정 시에만 사용)
    ANALYSIS_ONLY_KEYWORDS,
    REVIEW_RISK_LOW,
    REVIEW_RISK_MID,
    REVIEW_RISK_HIGH,
)
from worker import (
    TimeoutConfig,
    _run_with_activity_timeout,
    _scrub_secrets,
)
from project_profile import ProjectProfile, normalize_changed_path
from feature_context import load_feature_context
from context_foundation import build_and_record_context
from job_contract import CODEX_MODELS
from runtime_safety import (
    assert_secret_free_argv,
    isolated_subprocess_env,
    scrub_secrets,
    safe_print as print,
    trusted_executable,
)

# 하위 호환 별칭 (기존 참조 유지)
NO_MODIFY_KEYWORDS = ANALYSIS_ONLY_KEYWORDS

# 리뷰 엔진(Codex 단일) 모델:
# - KKM_REVIEW_MODEL 가 Codex 모델명(gpt-*)이면 그대로 사용
# - 기본값 gpt-5.6-sol
REVIEW_MODEL = (
    os.environ.get("KKM_REVIEW_MODEL")
    or os.environ.get("KKM_REVIEW_CODEX_MODEL")
    or "gpt-5.6-sol"
)
# Codex reasoning effort (기본 medium)
REVIEW_REASONING = (
    os.environ.get("KKM_REVIEW_REASONING") or "medium"
)
if REVIEW_REASONING not in ("low", "medium", "high"):
    REVIEW_REASONING = "medium"

# 리뷰 상태 (review_status 값)
REVIEW_PASS = "REVIEW_PASS"
REVIEW_FAIL = "REVIEW_FAIL"
REVIEW_UNAVAILABLE = "REVIEW_UNAVAILABLE"
REVIEW_ERROR = "REVIEW_ERROR"

# System failure codes drive Manager state transitions. They are deliberately
# separate from model-authored semantic violation codes.
DIFF_TOO_LARGE_FOR_AUTOMATED_REVIEW = "DIFF_TOO_LARGE_FOR_AUTOMATED_REVIEW"
BINARY_DIFF_REQUIRES_MANUAL_REVIEW = "BINARY_DIFF_REQUIRES_MANUAL_REVIEW"
REVIEW_COVERAGE_PLAN_INVALID = "REVIEW_COVERAGE_PLAN_INVALID"
STRUCTURED_OUTPUT_INVALID = "STRUCTURED_OUTPUT_INVALID"
REVIEW_SCHEMA_INVALID = "REVIEW_SCHEMA_INVALID"
REVIEW_EXECUTION_UNAVAILABLE = "REVIEW_EXECUTION_UNAVAILABLE"
REVIEW_OUTPUT_PARSE_FAILED = "REVIEW_OUTPUT_PARSE_FAILED"
RUNTIME_CONTRACT_DRIFT = "RUNTIME_CONTRACT_DRIFT"

DEFAULT_REVIEW_MAX_BATCH_CHARS = 12000
DEFAULT_REVIEW_MAX_BATCHES = 12
DEFAULT_REVIEW_CONTEXT_LINES = 3


# 내부 하네스 루트 (환경변수 AGENTS_DIR 우선, 없으면 D:\agents 폴백)
AGENTS_DIR = Path(os.environ.get("AGENTS_DIR", r"D:\agents"))

# 내부 하네스 규칙 (수정하지 않고 읽어서 주입)
AGENTS_MD_PATH = str(AGENTS_DIR / "AGENTS.md")
REVIEW_SKILL_PATH = str(AGENTS_DIR / "skills" / "review" / "SKILL.md")
VALIDATION_HOOK_PATH = str(AGENTS_DIR / "hooks" / "validation.md")


@dataclass
class ReviewResult:
    passed: bool
    reason: str = ""
    details: list[str] = field(default_factory=list)
    # REVIEW_PASS | REVIEW_FAIL | REVIEW_UNAVAILABLE | REVIEW_ERROR
    # 미지정 시 passed 값에서 파생 (passed=True→REVIEW_PASS, False→REVIEW_FAIL)
    status: str = ""
    # 재시도 전략 등급 (REVIEW_FAIL 에서만 의미): ""|LOW|MID|HIGH
    # PASS/UNAVAILABLE/ERROR 에서는 항상 ""
    risk: str = ""
    violation_codes: list[str] = field(default_factory=list)
    reviewed_files: list[str] = field(default_factory=list)
    coverage: dict[str, object] = field(default_factory=dict)
    deterministic: bool = False
    failure_code: str = ""

    def __post_init__(self):
        if not self.status:
            self.status = REVIEW_PASS if self.passed else REVIEW_FAIL
        if self.status != REVIEW_FAIL:
            self.risk = ""


@dataclass(frozen=True)
class StructuredParseResult:
    value: tuple[str, str, list[str], str, list[str], list[str]] | None
    is_json: bool
    diagnostic: dict[str, object]


@dataclass(frozen=True)
class DiffChunk:
    chunk_id: str
    sequence: int
    module: str
    path: str
    change_kind: str
    hunk_id: str
    original_hunk_header: str
    hunk_header: str
    chunk_ordinal: int
    chunk_count: int
    original_old_range: tuple[int, int]
    original_new_range: tuple[int, int]
    chunk_old_range: tuple[int, int]
    chunk_new_range: tuple[int, int]
    removed_old_ranges: tuple[tuple[int, int], ...]
    added_new_ranges: tuple[tuple[int, int], ...]
    content_sha256: str
    context_overlap: tuple[int, int]
    rename_from: str
    rename_to: str
    new_file: bool
    deleted_file: bool
    full_rewrite: bool
    eol_only: bool
    whitespace_only: bool
    binary: bool
    text: str
    changed_atoms: tuple[str, ...]


@dataclass(frozen=True)
class DiffBatch:
    batch_id: str
    index: int
    text: str
    chunks: tuple[DiffChunk, ...]
    changed_atoms: tuple[str, ...]
    changed_files: tuple[str, ...]


@dataclass(frozen=True)
class DiffBatchPlan:
    batches: tuple[DiffBatch, ...] = ()
    expected_atoms: tuple[str, ...] = ()
    assigned_atoms: tuple[str, ...] = ()
    missing_atoms: tuple[str, ...] = ()
    duplicate_atoms: tuple[str, ...] = ()
    failure_code: str = ""
    reason: str = ""
    max_chars: int = DEFAULT_REVIEW_MAX_BATCH_CHARS
    max_batches: int = DEFAULT_REVIEW_MAX_BATCHES

    @property
    def valid(self) -> bool:
        return not self.failure_code


def is_hard_constraint_violation(texts: list[str]) -> str | None:
    """Deprecated compatibility helper; natural-language verdict text is not evidence."""
    return None


def normalize_review_risk(
    parsed_risk: str,
    verdict_fail: bool,
    fail_texts: list[str],
) -> str:
    """RISK 등급 확정 (단일 결정 지점).
    - REVIEW_FAIL 에서만 LOW/MID/HIGH 의미. 그 외 "".
    - 파싱 실패/누락 → HIGH (fail-closed: 자동 LOW 금지)
    - 하드 제약 위반 감지 → 무조건 HIGH (리뷰어 LOW/MID 덮어쓰기)
    """
    if not verdict_fail:
        return ""
    r = (parsed_risk or "").strip().upper()
    if r == REVIEW_RISK_LOW:
        return REVIEW_RISK_LOW
    if r == REVIEW_RISK_MID:
        return REVIEW_RISK_MID
    return REVIEW_RISK_HIGH   # 누락/불명/파싱 실패 → 안전 쪽


_SECRET_ADDITION_RE = re.compile(
    r"(?i)\b(password|passwd|secret|token|api[_-]?key|private[_-]?key|pat)\b"
    r"\s*[=:]\s*['\"]?(?!\$\{|\$env:|os\.environ|env\(|getenv|\*{3,}|<)[^'\"\s]{8,}"
)
_DANGEROUS_SQL_RE = re.compile(
    r"(?i)\b(?:DROP\s+(?:TABLE|DATABASE|SCHEMA)|TRUNCATE\s+TABLE)\b"
)
_EXTERNAL_API_RE = re.compile(
    r"(?i)(?:\b(?:fetch|axios|url|uri|endpoint|base[_-]?url|api[_-]?url|api)\b|<property\b|<value\b)"
    r".{0,180}?https?://(?!localhost\b|127\.0\.0\.1\b)[A-Za-z0-9.-]+"
)
_DIFF_FILE_RE = re.compile(r"(?m)^diff --git a/(.+?) b/(.+?)$")
_DIFF_HUNK_RE = re.compile(r"(?m)^@@ .+? @@")
_MODULE_HEADER_RE = re.compile(r"^--- \[module: ([^\]]+)\] ---$", re.MULTILINE)
_HUNK_HEADER_RE = re.compile(
    r"^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@(?:.*)$"
)
SEMANTIC_VIOLATION_CODE_PATTERN = r"^[A-Z][A-Z0-9_]{0,63}$"
SEMANTIC_VIOLATION_CODE_MAX_ITEMS = 64
REVIEWED_FILE_MAX_ITEMS = 2048
REVIEW_TEXT_MAX_ITEMS = 128


def review_output_schema() -> dict[str, object]:
    """Build a Codex Structured Outputs-compatible transport schema.

    Codex accepts only a subset of JSON Schema. Bounds, uniqueness, and the
    semantic-code pattern therefore remain mandatory local parser checks.
    The review prompt communicates those constraints while the local parser
    enforces them after transport validation.
    """
    properties = {
        "result": {"type": "string", "enum": ["PASS", "FAIL"]},
        "reason": {"type": "string"},
        "details": {
            "type": "array",
            "items": {"type": "string"},
        },
        "risk": {"type": "string", "enum": ["NONE", "LOW", "MID", "HIGH"]},
        "violation_codes": {
            "type": "array",
            "items": {"type": "string"},
        },
        "reviewed_files": {
            "type": "array",
            "items": {"type": "string"},
        },
        # Codex strict structured output requires every property to appear in
        # ``required``. Optional transport data is represented by nullable
        # values instead of by omitting a property.
        "reviewed_task_files": {
            "type": ["array", "null"],
            "items": {"type": "string"},
        },
        "inspected_unchanged_target_files": {
            "type": ["array", "null"],
            "items": {"type": "string"},
        },
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": list(properties),
        "properties": properties,
    }


class ReviewSchemaValidationError(ValueError):
    """Raised when a response schema violates Codex strict-object rules."""


def validate_review_output_schema(
    schema: dict[str, object],
) -> dict[str, object]:
    """Recursively validate the strict JSON Schema subset used by Codex."""

    def walk(node: object, path: str) -> None:
        if not isinstance(node, dict):
            raise ReviewSchemaValidationError(f"{path}: schema node must be an object")
        raw_type = node.get("type")
        types = raw_type if isinstance(raw_type, list) else [raw_type]
        if "object" in types:
            properties = node.get("properties")
            required = node.get("required")
            if not isinstance(properties, dict):
                raise ReviewSchemaValidationError(f"{path}: object properties missing")
            if not isinstance(required, list) or any(
                not isinstance(name, str) for name in required
            ):
                raise ReviewSchemaValidationError(f"{path}: object required missing")
            if len(required) != len(set(required)) or set(required) != set(properties):
                raise ReviewSchemaValidationError(
                    f"{path}: required must exactly match properties"
                )
            if node.get("additionalProperties") is not False:
                raise ReviewSchemaValidationError(
                    f"{path}: additionalProperties must be false"
                )
            for name, child in properties.items():
                walk(child, f"{path}.properties.{name}")
        if "array" in types:
            if "items" not in node:
                raise ReviewSchemaValidationError(f"{path}: array items missing")
            walk(node["items"], f"{path}.items")
        for keyword in ("allOf", "anyOf", "oneOf"):
            branches = node.get(keyword)
            if branches is None:
                continue
            if not isinstance(branches, list):
                raise ReviewSchemaValidationError(f"{path}.{keyword}: must be an array")
            for index, child in enumerate(branches):
                walk(child, f"{path}.{keyword}[{index}]")
        definitions = node.get("$defs")
        if definitions is not None:
            if not isinstance(definitions, dict):
                raise ReviewSchemaValidationError(f"{path}.$defs: must be an object")
            for name, child in definitions.items():
                walk(child, f"{path}.$defs.{name}")

    walk(schema, "$")
    return schema


_REVIEW_SCHEMA = validate_review_output_schema(review_output_schema())


def added_diff_lines(git_diff: str) -> list[str]:
    return [
        line[1:]
        for line in (git_diff or "").splitlines()
        if line.startswith("+") and not line.startswith("+++")
    ]


def diff_files(git_diff: str) -> list[str]:
    module = ""
    files: list[str] = []
    for line in (git_diff or "").splitlines():
        header = _MODULE_HEADER_RE.match(line)
        if header:
            module = normalize_changed_path(header.group(1))
            continue
        match = re.match(r"^diff --git a/(.+?) b/(.+?)$", line)
        if match:
            path = normalize_changed_path(match.group(2))
            files.append(f"{module}/{path}" if module else path)
    return list(dict.fromkeys(files))


def _positive_limit(name: str, default: int) -> tuple[int, str]:
    raw = os.environ.get(name)
    if raw is None:
        return default, ""
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return 0, f"{name} must be a positive integer"
    return (value, "") if value > 0 else (0, f"{name} must be a positive integer")


def _line_ranges(numbers: list[int]) -> tuple[tuple[int, int], ...]:
    if not numbers:
        return ()
    ranges: list[tuple[int, int]] = []
    start = previous = numbers[0]
    for number in numbers[1:]:
        if number != previous + 1:
            ranges.append((start, previous))
            start = number
        previous = number
    ranges.append((start, previous))
    return tuple(ranges)


def _atom_key(module: str, path: str, side: str, line: int, hunk_id: str) -> str:
    return f"{module}|{path}|{side}|{line}|{hunk_id}"


def _path_with_module(module: str, path: str) -> str:
    return f"{module}/{path}" if module else path


def _chunk_flags(records: list[dict[str, object]], new_file: bool, deleted_file: bool) -> tuple[bool, bool, bool]:
    removed = [str(record["line"])[1:].rstrip("\n") for record in records if record["kind"] == "old"]
    added = [str(record["line"])[1:].rstrip("\n") for record in records if record["kind"] == "new"]
    has_context = any(record["kind"] == "context" for record in records)
    eol_only = bool(removed and added and removed != added and
                    [line.rstrip("\r") for line in removed] == [line.rstrip("\r") for line in added])
    whitespace_only = bool(removed and added and removed != added and
                           [re.sub(r"\s+", "", line) for line in removed] ==
                           [re.sub(r"\s+", "", line) for line in added])
    full_rewrite = bool(removed and added and not has_context and not new_file and not deleted_file)
    return full_rewrite, eol_only, whitespace_only


def _plan_failure(code: str, reason: str, max_chars: int, max_batches: int) -> DiffBatchPlan:
    return DiffBatchPlan(
        failure_code=code, reason=reason, max_chars=max_chars, max_batches=max_batches
    )


def assemble_batches(
    chunks: list[DiffChunk],
    max_chars: int,
    max_batches: int,
) -> tuple[tuple[DiffBatch, ...], str]:
    """Assemble size-bounded batches from ordered chunks (single source).

    Returns ``(batches, "")`` on success, or ``((), reason)`` when a single
    chunk exceeds the batch limit or the batch count exceeds ``max_batches``.
    """
    batches: list[DiffBatch] = []
    pending: list[DiffChunk] = []
    pending_text = ""

    def flush() -> None:
        nonlocal pending, pending_text
        if not pending:
            return
        batch_index = len(batches) + 1
        atom_list = tuple(atom for item in pending for atom in item.changed_atoms)
        files = tuple(dict.fromkeys(_path_with_module(item.module, item.path) for item in pending))
        batch_id = hashlib.sha256("|".join(item.chunk_id for item in pending).encode("utf-8")).hexdigest()[:20]
        batches.append(DiffBatch(batch_id, batch_index, pending_text, tuple(pending), atom_list, files))
        pending = []
        pending_text = ""

    for chunk in chunks:
        if len(chunk.text) > max_chars:
            return (), f"chunk exceeds batch limit: {_path_with_module(chunk.module, chunk.path)}"
        if pending and len(pending_text) + len(chunk.text) > max_chars:
            flush()
        pending.append(chunk)
        pending_text += chunk.text
    flush()
    if len(batches) > max_batches:
        return (), f"planned batch count {len(batches)} exceeds limit {max_batches}"
    return tuple(batches), ""


def plan_diff_batches(
    git_diff: str,
    max_chars: int | None = None,
    max_batches: int | None = None,
    context_lines: int = DEFAULT_REVIEW_CONTEXT_LINES,
) -> DiffBatchPlan:
    """Create a lossless unified-diff plan with exactly one owner per changed line."""
    if max_chars is None:
        max_chars, error = _positive_limit(
            "KKM_REVIEW_MAX_BATCH_CHARS", DEFAULT_REVIEW_MAX_BATCH_CHARS
        )
        if error:
            return _plan_failure(REVIEW_COVERAGE_PLAN_INVALID, error, 0, 0)
    if max_batches is None:
        max_batches, error = _positive_limit(
            "KKM_REVIEW_MAX_BATCHES", DEFAULT_REVIEW_MAX_BATCHES
        )
        if error:
            return _plan_failure(REVIEW_COVERAGE_PLAN_INVALID, error, max_chars, 0)
    if not isinstance(max_chars, int) or max_chars <= 0:
        return _plan_failure(REVIEW_COVERAGE_PLAN_INVALID, "invalid batch character limit", 0, max_batches)
    if not isinstance(max_batches, int) or max_batches < 1:
        return _plan_failure(REVIEW_COVERAGE_PLAN_INVALID, "invalid batch count limit", max_chars, 0)
    if not git_diff:
        return DiffBatchPlan(max_chars=max_chars, max_batches=max_batches)

    sections: list[tuple[str, list[str]]] = []
    module = ""
    current: list[str] = []

    def append_current() -> None:
        """Finish one file section without accepting transport separators.

        Older 0.7 collectors joined repository diffs with a blank line. That
        prefix-less separator is not part of a unified-diff hunk and previously
        caused ``malformed hunk body`` before Codex was invoked. Remove only
        all-whitespace transport lines at a section boundary; a real blank source
        line always has Git's context/add/remove prefix and is preserved.
        """
        nonlocal current
        while current and current[-1] in ("", "\n", "\r\n"):
            current.pop()
        if current:
            sections.append((module, current))
        current = []

    for line in git_diff.splitlines(keepends=True):
        module_match = _MODULE_HEADER_RE.match(line.rstrip("\r\n"))
        if module_match:
            append_current()
            module = normalize_changed_path(module_match.group(1))
            continue
        if line.startswith("diff --git "):
            append_current()
            current = [line]
        elif current:
            current.append(line)
        elif line.strip():
            return _plan_failure(
                REVIEW_COVERAGE_PLAN_INVALID, "content exists outside a file diff", max_chars, max_batches
            )
    append_current()
    if not sections:
        return _plan_failure(
            REVIEW_COVERAGE_PLAN_INVALID, "no unified diff file section", max_chars, max_batches
        )

    chunk_specs: list[dict[str, object]] = []
    expected_atoms: list[str] = []
    sequence = 0

    for file_index, (module, lines) in enumerate(sections, 1):
        match = re.match(r"^diff --git a/(.+?) b/(.+?)\r?\n?$", lines[0])
        if not match:
            return _plan_failure(
                REVIEW_COVERAGE_PLAN_INVALID, "malformed file header", max_chars, max_batches
            )
        old_path = normalize_changed_path(match.group(1).strip('"'))
        path = normalize_changed_path(match.group(2).strip('"'))
        joined = "".join(lines)
        if re.search(r"(?m)^(Binary files |GIT binary patch$)", joined):
            return _plan_failure(
                BINARY_DIFF_REQUIRES_MANUAL_REVIEW,
                f"binary diff requires manual review: {_path_with_module(module, path)}",
                max_chars, max_batches,
            )
        rename_from_match = re.search(r"(?m)^rename from (.+)$", joined)
        rename_to_match = re.search(r"(?m)^rename to (.+)$", joined)
        rename_from = normalize_changed_path(rename_from_match.group(1).strip('"')) if rename_from_match else ""
        rename_to = normalize_changed_path(rename_to_match.group(1).strip('"')) if rename_to_match else ""
        new_file = bool(re.search(r"(?m)^new file mode ", joined) or "--- /dev/null" in joined)
        deleted_file = bool(re.search(r"(?m)^deleted file mode ", joined) or "+++ /dev/null" in joined)
        change_kind = "rename" if (rename_from or rename_to) else "new" if new_file else "deleted" if deleted_file else "modified"
        module_header = f"--- [module: {module}] ---\n" if module else ""
        hunk_starts = [index for index, line in enumerate(lines) if line.startswith("@@ ")]

        if not hunk_starts:
            invalid_change_line = any(
                (line.startswith("+") and not line.startswith("+++")) or
                (line.startswith("-") and not line.startswith("---"))
                for line in lines[1:]
            )
            if invalid_change_line:
                return _plan_failure(
                    REVIEW_COVERAGE_PLAN_INVALID,
                    f"changed line outside a hunk: {_path_with_module(module, path)}",
                    max_chars, max_batches,
                )
            text = module_header + joined
            if len(text) > max_chars:
                return _plan_failure(
                    DIFF_TOO_LARGE_FOR_AUTOMATED_REVIEW,
                    f"metadata-only file diff exceeds batch limit: {_path_with_module(module, path)}",
                    max_chars, max_batches,
                )
            sequence += 1
            chunk_specs.append({
                "sequence": sequence, "module": module, "path": path,
                "change_kind": change_kind, "hunk_id": f"f{file_index}-metadata",
                "original_hunk_header": "", "hunk_header": "",
                "original_old_range": (0, 0), "original_new_range": (0, 0),
                "chunk_old_range": (0, 0), "chunk_new_range": (0, 0),
                "removed_old_ranges": (), "added_new_ranges": (),
                "context_overlap": (0, 0), "rename_from": rename_from,
                "rename_to": rename_to, "new_file": new_file,
                "deleted_file": deleted_file, "full_rewrite": False,
                "eol_only": False, "whitespace_only": False, "binary": False,
                "text": text, "changed_atoms": (), "ordinal_group": f"f{file_index}-metadata",
            })
            continue

        file_prefix = module_header + "".join(lines[:hunk_starts[0]])
        for hunk_index, start in enumerate(hunk_starts, 1):
            end = hunk_starts[hunk_index] if hunk_index < len(hunk_starts) else len(lines)
            raw_header = lines[start].rstrip("\r\n")
            header_match = _HUNK_HEADER_RE.match(raw_header)
            if not header_match:
                return _plan_failure(
                    REVIEW_COVERAGE_PLAN_INVALID,
                    f"malformed hunk header: {_path_with_module(module, path)}",
                    max_chars, max_batches,
                )
            old_start = int(header_match.group(1))
            old_count = int(header_match.group(2) or 1)
            new_start = int(header_match.group(3))
            new_count = int(header_match.group(4) or 1)
            hunk_id = hashlib.sha256(
                f"{module}|{path}|{hunk_index}|{raw_header}".encode("utf-8")
            ).hexdigest()[:16]
            records: list[dict[str, object]] = []
            old_line = old_start
            new_line = new_start
            for body_line in lines[start + 1:end]:
                record: dict[str, object] = {
                    "line": body_line, "old_before": old_line, "new_before": new_line,
                    "old_line": None, "new_line": None, "atom": "", "kind": "meta",
                }
                if body_line.startswith(" "):
                    record["kind"] = "context"
                    old_line += 1
                    new_line += 1
                elif body_line.startswith("-"):
                    record["kind"] = "old"
                    record["old_line"] = old_line
                    record["atom"] = _atom_key(module, path, "old", old_line, hunk_id)
                    expected_atoms.append(str(record["atom"]))
                    old_line += 1
                elif body_line.startswith("+"):
                    record["kind"] = "new"
                    record["new_line"] = new_line
                    record["atom"] = _atom_key(module, path, "new", new_line, hunk_id)
                    expected_atoms.append(str(record["atom"]))
                    new_line += 1
                elif body_line.startswith("\\ No newline at end of file"):
                    pass
                else:
                    return _plan_failure(
                        REVIEW_COVERAGE_PLAN_INVALID,
                        f"malformed hunk body: {_path_with_module(module, path)}",
                        max_chars, max_batches,
                    )
                records.append(record)
            if old_line - old_start != old_count or new_line - new_start != new_count:
                return _plan_failure(
                    REVIEW_COVERAGE_PLAN_INVALID,
                    f"hunk range count mismatch: {_path_with_module(module, path)}",
                    max_chars, max_batches,
                )
            full_rewrite, eol_only, whitespace_only = _chunk_flags(records, new_file, deleted_file)

            def render(excerpt_start: int, excerpt_end: int) -> tuple[str, str, tuple[int, int], tuple[int, int]]:
                excerpt = records[excerpt_start:excerpt_end]
                old_excerpt_start = int(excerpt[0]["old_before"]) if excerpt else old_start
                new_excerpt_start = int(excerpt[0]["new_before"]) if excerpt else new_start
                old_excerpt_count = sum(record["kind"] in ("context", "old") for record in excerpt)
                new_excerpt_count = sum(record["kind"] in ("context", "new") for record in excerpt)
                header = f"@@ -{old_excerpt_start},{old_excerpt_count} +{new_excerpt_start},{new_excerpt_count} @@"
                text = file_prefix + header + "\n" + "".join(str(record["line"]) for record in excerpt)
                return text, header, (old_excerpt_start, old_excerpt_count), (new_excerpt_start, new_excerpt_count)

            whole_text = file_prefix + lines[start] + "".join(str(record["line"]) for record in records)
            core_ranges: list[tuple[int, int, int, int, str, str, tuple[int, int], tuple[int, int]]] = []
            if len(whole_text) <= max_chars:
                core_ranges.append((0, len(records), 0, 0, whole_text, raw_header,
                                    (old_start, old_count), (new_start, new_count)))
            else:
                core_start = 0
                while core_start < len(records):
                    best = None
                    for core_end in range(core_start + 1, len(records) + 1):
                        candidate = None
                        # Overlap is context-only. Repeating an added/removed
                        # line in adjacent prompts would make semantic review
                        # ownership ambiguous even when the atom ledger has a
                        # single formal owner.
                        max_pre = 0
                        while (
                            max_pre < context_lines
                            and core_start - max_pre - 1 >= 0
                            and records[core_start - max_pre - 1]["kind"] == "context"
                        ):
                            max_pre += 1
                        max_post = 0
                        while (
                            max_post < context_lines
                            and core_end + max_post < len(records)
                            and records[core_end + max_post]["kind"] == "context"
                        ):
                            max_post += 1
                        for pre in range(max_pre, -1, -1):
                            for post in range(max_post, -1, -1):
                                text, header, old_range, new_range = render(
                                    core_start - pre, core_end + post
                                )
                                if len(text) <= max_chars:
                                    candidate = (core_start, core_end, pre, post, text, header, old_range, new_range)
                                    break
                            if candidate:
                                break
                        if candidate is None:
                            break
                        best = candidate
                    if best is None:
                        return _plan_failure(
                            DIFF_TOO_LARGE_FOR_AUTOMATED_REVIEW,
                            f"single diff line exceeds batch limit: {_path_with_module(module, path)}",
                            max_chars, max_batches,
                        )
                    core_ranges.append(best)
                    core_start = best[1]

            group_id = f"f{file_index}-h{hunk_index}"
            for core_start, core_end, pre, post, text, header, old_range, new_range in core_ranges:
                core = records[core_start:core_end]
                atoms = tuple(str(record["atom"]) for record in core if record["atom"])
                sequence += 1
                chunk_specs.append({
                    "sequence": sequence, "module": module, "path": path,
                    "change_kind": change_kind, "hunk_id": hunk_id,
                    "original_hunk_header": raw_header, "hunk_header": header,
                    "original_old_range": (old_start, old_count),
                    "original_new_range": (new_start, new_count),
                    "chunk_old_range": old_range, "chunk_new_range": new_range,
                    "removed_old_ranges": _line_ranges([
                        int(record["old_line"]) for record in core if record["old_line"] is not None
                    ]),
                    "added_new_ranges": _line_ranges([
                        int(record["new_line"]) for record in core if record["new_line"] is not None
                    ]),
                    "context_overlap": (pre, post), "rename_from": rename_from,
                    "rename_to": rename_to, "new_file": new_file,
                    "deleted_file": deleted_file, "full_rewrite": full_rewrite,
                    "eol_only": eol_only, "whitespace_only": whitespace_only,
                    "binary": False, "text": text, "changed_atoms": atoms,
                    "ordinal_group": group_id,
                })

    group_counts: dict[str, int] = {}
    for spec in chunk_specs:
        group = str(spec["ordinal_group"])
        group_counts[group] = group_counts.get(group, 0) + 1
    group_ordinals: dict[str, int] = {}
    chunks: list[DiffChunk] = []
    for spec in chunk_specs:
        group = str(spec.pop("ordinal_group"))
        group_ordinals[group] = group_ordinals.get(group, 0) + 1
        text = str(spec["text"])
        stable = "|".join((str(spec["module"]), str(spec["path"]), str(spec["hunk_id"]),
                           str(group_ordinals[group]), hashlib.sha256(text.encode("utf-8")).hexdigest()))
        chunks.append(DiffChunk(
            chunk_id=hashlib.sha256(stable.encode("utf-8")).hexdigest()[:20],
            chunk_ordinal=group_ordinals[group], chunk_count=group_counts[group],
            content_sha256=hashlib.sha256(text.encode("utf-8")).hexdigest(),
            **spec,
        ))

    batches, assembly_failure = assemble_batches(chunks, max_chars, max_batches)
    if assembly_failure:
        return _plan_failure(
            DIFF_TOO_LARGE_FOR_AUTOMATED_REVIEW, assembly_failure, max_chars, max_batches,
        )

    assigned = tuple(atom for chunk in chunks for atom in chunk.changed_atoms)
    expected = tuple(expected_atoms)
    assigned_counts: dict[str, int] = {}
    for atom in assigned:
        assigned_counts[atom] = assigned_counts.get(atom, 0) + 1
    missing = tuple(atom for atom in expected if atom not in assigned_counts)
    duplicate = tuple(atom for atom, count in assigned_counts.items() if count != 1)
    if missing or duplicate or assigned != expected:
        return DiffBatchPlan(
            expected_atoms=expected, assigned_atoms=assigned,
            missing_atoms=missing, duplicate_atoms=duplicate,
            failure_code=REVIEW_COVERAGE_PLAN_INVALID,
            reason="changed-line ownership or ordering is invalid",
            max_chars=max_chars, max_batches=max_batches,
        )
    return DiffBatchPlan(
        batches=tuple(batches), expected_atoms=expected, assigned_atoms=assigned,
        max_chars=max_chars, max_batches=max_batches,
    )


def split_diff_batches(git_diff: str, max_chars: int = DEFAULT_REVIEW_MAX_BATCH_CHARS) -> list[str]:
    """Compatibility wrapper around the lossless planner."""
    plan = plan_diff_batches(git_diff, max_chars=max_chars, max_batches=DEFAULT_REVIEW_MAX_BATCHES)
    return [batch.text for batch in plan.batches] if plan.valid else []


def deterministic_violations(
    changed_files: list[str],
    git_diff: str,
    profile: ProjectProfile,
    target_modules: list[str] | None = None,
    target_resources: list[str] | None = None,
) -> tuple[list[str], list[str]]:
    codes: list[str] = []
    details: list[str] = []

    def add(code: str, detail: str) -> None:
        if code not in codes:
            codes.append(code)
        if detail not in details:
            details.append(detail)

    targets = {name.casefold() for name in (target_modules or [])}
    resources = [
        normalize_changed_path(item).casefold().rstrip("/")
        for item in (target_resources or [])
        if normalize_changed_path(item)
    ]
    for path in changed_files:
        normalized = normalize_changed_path(path)
        if profile.is_protected_path(normalized):
            add("PROTECTED_PATH_CHANGED", f"protected path changed: {normalized}")
        module = profile.module_for_path(normalized)
        if targets and (module is None or module.name.casefold() not in targets):
            add("OUT_OF_SCOPE_CHANGE", f"outside target modules: {normalized}")
        resource_path = normalized.casefold().rstrip("/")
        if resources and not any(
            resource_path == resource or resource_path.startswith(resource + "/")
            for resource in resources
        ):
            add(
                "OUT_OF_RESOURCE_SCOPE",
                f"outside target resources: {normalized}",
            )
    additions = added_diff_lines(git_diff)
    active_additions = [
        line for line in additions
        if not line.lstrip().startswith(("//", "#", "--", "/*", "*", "<!--"))
    ]
    if any(_SECRET_ADDITION_RE.search(line) for line in active_additions):
        add("SECRET_ADDED", "secret-like literal added")
    if any(_DANGEROUS_SQL_RE.search(line) for line in active_additions):
        add("DANGEROUS_SQL_ADDED", "dangerous SQL added")
    if any(_EXTERNAL_API_RE.search(line) for line in active_additions):
        add("EXTERNAL_API_ADDED", "external API endpoint added")
    return codes, details


def _review_metadata(
    details: list[str], expected_files: list[str]
) -> tuple[list[str], list[str], list[str], bool]:
    codes: list[str] = []
    reviewed: list[str] = []
    clean: list[str] = []
    structured = False
    for detail in details:
        if detail == "OUTPUT_MODE: structured":
            structured = True
        elif detail.startswith("OUTPUT_MODE:"):
            continue
        elif detail.startswith("VIOLATION_CODE: "):
            codes.append(detail.split(": ", 1)[1])
        elif detail.startswith("REVIEWED_FILE: "):
            reviewed.append(normalize_changed_path(detail.split(": ", 1)[1]))
        else:
            clean.append(detail)
    if not structured:
        reviewed = [normalize_changed_path(path) for path in expected_files]
    return list(dict.fromkeys(codes)), list(dict.fromkeys(reviewed)), clean, structured


# 리뷰 본문에서 제거할 프롬프트 템플릿 조각 (리뷰어가 프롬프트를 에코할 때).
# 이 라인들이 review_result/details 로 흘러들면 리포트가 수백 KB 로 부풀어진다.
_REVIEW_TEMPLATE_MARKERS = (
    "[출력 형식]",
    "RESULT: PASS 또는 FAIL",
    "REASON: <한 줄 요약>",
    "DETAIL: <필요 시 추가 설명>",
    "RISK: FAIL 인 경우만",
    "PASS 면 RISK: NONE",
    "너는 코드 리뷰어야",
    "[규칙]",
    "[작업 요구사항]",
    "[변경 파일 목록]",
    "[보조 원격 MR diff",
    '"timestamp"',
    '"session_id"',
    '{"type":',
)


def _clean_review_line(text: str, max_chars: int = 2000) -> str:
    """리뷰 detail/reason 1줄 정제.
    - 프롬프트 템플릿 마커가 포함된 라인/청크는 그 라인을 통째로 제거.
    - stream-json 조각({"type":...)도 제거.
    - 남은 라인은 각 300자로 캡. 최대 max_chars.
    """
    if not text:
        return ""
    kept: list[str] = []
    for line in text.splitlines():
        s = line.strip()
        if not s:
            continue
        if any(m in line for m in _REVIEW_TEMPLATE_MARKERS):
            continue
        kept.append(s[:300])
        if sum(len(k) for k in kept) >= max_chars:
            break
    return "\n".join(kept)


def _extract_codex_messages(stdout: str) -> list[str]:
    """Return agent-message texts from a Codex JSONL stream in event order."""
    if not stdout or not stdout.lstrip().startswith("{"):
        return []
    messages: list[str] = []
    for line in stdout.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            event = json.loads(line)
        except ValueError:
            continue
        item = event.get("item") or {}
        if event.get("type") == "item.completed" and item.get("type") == "agent_message":
            text = item.get("text")
            if isinstance(text, str) and text.strip():
                messages.append(text.strip())
    return messages


def _extract_codex_text(stdout: str) -> str:
    """codex exec --json(JSONL) 출력에서 최종 에이전트 메시지 추출.
    - item.completed/agent_message 이벤트의 text 중 마지막 것.
    - JSONL 이 아니면 raw 그대로(기존 _parse_review_output 이 RESULT 를 찾음).
    - 실패 방어: 못 찾으면 ""(호출부에서 -o 파일 우선).
    """
    if not stdout or not stdout.lstrip().startswith("{"):
        return stdout or ""
    messages = _extract_codex_messages(stdout)
    return messages[-1] if messages else ""


def _extract_codex_error(stdout: str) -> str:
    """Extract one redacted message only from explicit Codex error events.

    Request and schema failures can be written to stdout JSONL while stderr is
    empty. Agent messages, prompts, diffs, commands, and arbitrary raw stdout
    are deliberately ignored so diagnostics cannot become a data leak.
    """
    if not stdout:
        return ""
    messages: list[str] = []
    for line in stdout.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if event.get("type") not in ("error", "turn.failed"):
            continue
        candidates: list[object] = [event.get("message")]
        error = event.get("error")
        if isinstance(error, dict):
            candidates.extend((error.get("message"), error.get("code")))
        elif isinstance(error, str):
            candidates.append(error)
        for candidate in candidates:
            if isinstance(candidate, str) and candidate.strip():
                cleaned = " ".join(scrub_secrets(candidate).split())[:300]
                if cleaned:
                    messages.append(cleaned)
                    break
    return messages[-1] if messages else ""


def _codex_exit_failure(exit_code: int, stdout: str, stderr: str) -> str:
    """Return bounded, redacted diagnostics for a non-zero Codex exit."""
    stderr_text = " ".join(scrub_secrets(stderr or "").split())[:300]
    if stderr_text:
        return f"codex 실행 실패(exit={exit_code}, source=stderr): {stderr_text}"
    stdout_error = _extract_codex_error(stdout or "")
    if stdout_error:
        return f"codex 실행 실패(exit={exit_code}, source=jsonl-error): {stdout_error}"
    event_count = sum(
        1 for line in (stdout or "").splitlines() if line.lstrip().startswith("{")
    )
    return (
        f"codex 실행 실패(exit={exit_code}, stderr=empty, "
        f"jsonl_events={event_count}, explicit_error=missing)"
    )


def _codex_review_failure_kind(stdout: str, stderr: str) -> str:
    """Separate local/remote schema rejection from runtime unavailability."""
    diagnostic = f"{stderr or ''}\n{_extract_codex_error(stdout or '')}".casefold()
    if "invalid_json_schema" in diagnostic or (
        "response_format" in diagnostic and "schema" in diagnostic
    ):
        return "schema_invalid"
    return "execution_unavailable"


def load_review_rules(
    profile: ProjectProfile | None = None,
    changed_files: list[str] | None = None,
    policy_root: Path | None = None,
    requirement: str = "",
) -> str:
    """Load common validation/review plus selected profile rules; never common AGENTS."""
    parts: list[str] = []
    common_root = policy_root or AGENTS_DIR
    common_paths = (
        common_root / "hooks" / "validation.md",
        common_root / "skills" / "review" / "SKILL.md",
    )
    if policy_root is not None:
        # Snapshot-backed review must not depend on the live global AGENTS file.
        # Its common rules are frozen and injected explicitly like other rules.
        common_paths = (common_root / "AGENTS.md", *common_paths)
    for p in common_paths:
        try:
            with open(p, encoding="utf-8") as f:
                t = f.read().strip()
            if t:
                parts.append(t)
        except FileNotFoundError:
            raise RuntimeError(f"required reviewer rule not found: {p}")
        except Exception as e:
            raise RuntimeError(f"required reviewer rule cannot be read: {p} ({type(e).__name__})") from e
    if profile is None:
        return "\n\n---\n\n".join(parts)
    parts.append(profile.project_rules.read_text(encoding="utf-8").strip())
    evidence = profile.detect_areas(changed_files=changed_files or [])
    selected = list(profile.rules.get("always", ()))
    for area in evidence.areas:
        selected.extend(profile.rules.get(area, ()))
    for path in dict.fromkeys(selected):
        try:
            text = path.read_text(encoding="utf-8").strip()
        except Exception as exc:
            raise RuntimeError(
                f"required profile reviewer rule cannot be read: {path} ({type(exc).__name__})"
            ) from exc
        if not text:
            raise RuntimeError(f"required profile reviewer rule is empty: {path}")
        parts.append(text)
    feature_block = load_feature_context(
        requirement,
        changed_files=changed_files,
        profile=profile,
    )
    if feature_block:
        parts.append("[선택된 기능 매핑 및 AS-IS 근거]\n" + feature_block)
    return "\n\n---\n\n".join(parts)


def build_tester_context(task: "TaskState | None") -> str:
    """
    TaskState 에서 Tester 증거 + 플래너 리뷰 포인트를 추출해 리뷰 프롬프트용 텍스트 생성.
    내용이 없으면 빈 문자열(기존 프롬프트 그대로).
    """
    if task is None:
        return ""
    parts: list[str] = []

    brief = (task.planner_brief or {}).get("reviewer_brief", "")
    if brief:
        parts.append(f"[리뷰 포인트(플래너)]\n{brief}")

    if task.artifact_files:
        artifacts = "\n".join(f"- {path}" for path in task.artifact_files[:20])
        parts.append(
            "[Job 산출물]\n"
            f"{artifacts}\n"
            "요구사항에 산출물 작성이 포함되면 실제 파일을 열어 내용까지 검증하라."
        )

    if task.test_status:
        ev = "\n".join(f"- {e}" for e in (task.test_evidence or [])[:10])
        block = (
            f"[테스트 증거(Tester)]\n"
            f"- required: {bool(getattr(task, 'test_required', False))}\n"
            f"- status: {task.test_status}\n"
            f"- summary: {task.test_summary or '(없음)'}"
        )
        if task.test_frontend or task.test_db:
            block += f"\n- 세부: frontend={task.test_frontend or '-'}, db={task.test_db or '-'}"
        if ev:
            block += f"\n{ev}"
        parts.append(block)

    if getattr(task, "build_evidence", None):
        parts.append(
            "[빌드 증거(Manager 실행 결과)]\n"
            + json.dumps(task.build_evidence, ensure_ascii=False, sort_keys=True)
        )

    if parts:
        parts.append(
            "[테스트 증거 활용 지침] 플래너가 테스트 증거를 요구했는데 "
            "status 가 SKIPPED/FAIL/ERROR 이면 그 이유를 지적하라."
        )
    return "\n\n".join(parts)


class Reviewer:
    def __init__(
        self,
        working_dir: str = ".",
        timeout: int = 180,
        timeout_config: TimeoutConfig | None = None,
        profile: ProjectProfile | None = None,
        policy_root: Path | None = None,
        manifest_root: Path | None = None,
    ):
        self.working_dir = Path(working_dir).resolve()
        self.timeout = timeout   # timeout_config 없을 때의 폴백 (subprocess.run)
        # 활동 기반 타임아웃 (워커와 동일 정책). None 이면 기존 subprocess.run 폴백.
        self.timeout_config = timeout_config
        self.profile = profile
        self.policy_root = Path(policy_root) if policy_root is not None else None
        self.manifest_root = Path(manifest_root) if manifest_root is not None else None

    def review(
        self,
        requirement: str,
        worker_success: bool,
        exit_code: int | None,
        changed_files: list[str],
        worker_stdout: str = "",
        git_diff: str = "",
        task: TaskState | None = None,
        attempt: int = 1,
        max_attempts: int = 0,
        review_batch: DiffBatch | None = None,
        all_changed_files: list[str] | None = None,
        batch_index: int = 1,
        batch_count: int = 1,
    ) -> ReviewResult:
        """
        1) 빠른 결정적 검사(exit_code, 분석 전용 위반, diff 게이트)
        2) 시맨틱 리뷰: Codex(gpt-5.6-sol) 단일 엔진 (읽기 전용 샌드박스)
        3) 리뷰어 불가/파싱 실패 → REVIEW_UNAVAILABLE / REVIEW_ERROR
           (자동 PASS 없음. MODIFICATION 은 Manager 가 실패 처리,
            ANALYSIS 는 '검증 불완전' 경고로만 기록)
        task 가 전달되면 결과를 state.review_status/review_result/
        review_violations 에 매핑(State 동기화).
        """
        details: list[str] = []

        # Manager가 호출 전에 결정한 effort를 보존한다. 직접 호출의 비정상값만
        # 환경 baseline으로 보정하며 Reviewer 내부에서는 effort를 바꾸지 않는다.
        if task is not None and task.reviewer_effective_reasoning_effort not in (
            "low", "medium", "high"
        ):
            task.reviewer_effective_reasoning_effort = REVIEW_REASONING

        # Tester 증거 + 플래너 리뷰 포인트 (task 에서 읽어 리뷰 엔진 프롬프트에 주입)
        tester_ctx = build_tester_context(task)

        # 작업 모드 확정: task.task_mode 명시 우선, 미지정 시 키워드 추정
        mode = resolve_task_mode(
            requirement, task.task_mode if task is not None else ""
        )

        if self.profile is not None:
            codes, violation_details = deterministic_violations(
                changed_files,
                git_diff,
                self.profile,
                task.target_module if task is not None else None,
                task.target_resources if task is not None else None,
            )
            if codes:
                rr = ReviewResult(
                    passed=False,
                    reason="deterministic policy violation",
                    details=violation_details,
                    status=REVIEW_FAIL,
                    risk=REVIEW_RISK_HIGH,
                    violation_codes=codes,
                    reviewed_files=list(dict.fromkeys(changed_files)),
                    coverage={
                        "total_files": list(dict.fromkeys(changed_files)),
                        "reviewed_files": list(dict.fromkeys(changed_files)),
                        "missing_files": [],
                        "batch_count": 0,
                        "truncated": False,
                    },
                    deterministic=True,
                )
                return self._apply_to_state(task, rr)

        # 1) Worker exit_code != 0 처리
        #    droid/codex 가 non-zero exit 를 반환하더라도 실제로 파일을 변경했다면
        #    작업은 수행된 것으로 보고 리뷰를 진행한다 (안전: 리뷰가 실제 품질을 판정).
        #    단, 변경사항이 없는 non-zero exit 는 명확한 실패.
        if not worker_success or (exit_code is not None and exit_code != 0):
            has_changes = bool(git_diff.strip()) or bool(changed_files)
            if not has_changes:
                # 변경사항 없이 exit_code != 0 → 진짜 실패
                stderr_tail = ""
                if task is not None and task.worker_stderr:
                    stderr_tail = task.worker_stderr.strip()[-500:]
                fail_details = [f"exit_code={exit_code}"]
                if stderr_tail:
                    fail_details.append(f"워커 stderr: {stderr_tail}")
                rr = ReviewResult(
                    passed=False,
                    reason=f"Worker 실행 실패 (exit_code={exit_code}, 변경사항 없음)",
                    details=fail_details,
                )
                return self._apply_to_state(task, rr)
            # 변경사항이 존재하면 non-zero exit 를 경고로만 기록하고 리뷰 계속 진행
            details.append(
                f"⚠ 워커가 exit_code={exit_code}(non-zero)로 종료했으나 "
                f"변경사항이 존재함({len(changed_files)}개 파일) → 리뷰 진행"
            )
            if task is not None and task.worker_stderr:
                stderr_tail = task.worker_stderr.strip()[-500:]
                if stderr_tail:
                    details.append(f"워커 stderr(일부): {stderr_tail}")

        # 2) ANALYSIS(분석 전용) 작업인데 의미 있는 파일이 변경된 경우
        #    mode 기준으로 판정 (task_mode 명시 우선, 미지정 시 키워드 추정)
        #    profile 로컬 개발 설정은 의미 있는 변경에서 제외(방어적 이중 필터).
        if mode == TASK_MODE_ANALYSIS:
            meaningful = [
                f for f in changed_files
                if not f.startswith(".idea/")
                and not f.endswith(".iml")
                and f != "target.zip"
                and not is_local_dev_config(f, self.profile)
            ]
            if meaningful:
                rr = ReviewResult(
                    passed=False,
                    reason="분석 전용(ANALYSIS) 작업인데 의미 있는 파일이 변경됨",
                    details=[f"changed: {meaningful}"],
                    status=REVIEW_FAIL,
                )
                return self._apply_to_state(task, rr)
            details.append("분석 전용(ANALYSIS) 작업 - IDE/잡파일만 변경됨 → 통과 처리")

        # 2.5) diff 게이트: MODIFICATION 작업인데 변경 사항이 없으면 FAIL
        #      단, Worker가 정확한 구조화 표식으로 "기존 상태에서 이미 충족"을
        #      주장한 경우에는 Codex가 현재 파일을 직접 읽어 독립 검증한다.
        if (
            mode == TASK_MODE_MODIFICATION
            and not git_diff.strip()
            and not changed_files
            and can_verify_current_state(requirement, worker_stdout)
        ):
            verification_files = extract_requirement_paths(
                requirement,
                self.profile.module_names if self.profile is not None else None,
            )
            if not verification_files and task is not None:
                verification_files = [f"{module}/" for module in task.target_module]
            if task is not None:
                verification_files.extend(
                    path for path in task.artifact_files
                    if path not in verification_files
                )
            rr = self._run_semantic_review(
                requirement=requirement,
                changed_files=verification_files,
                git_diff="",
                tester_context=tester_ctx,
                current_state=True,
                worker_summary=worker_stdout,
                task=task,
                attempt=attempt,
                max_attempts=max_attempts,
                base_details=details,
                label="codex 현재 상태 검증",
                all_changed_files=all_changed_files,
                batch_index=batch_index,
                batch_count=batch_count,
            )
            if rr.status == REVIEW_PASS:
                rr.reason = "기존 상태가 요구사항을 충족함 (독립 검증 완료)"
            return self._apply_to_state(task, rr)

        if mode == TASK_MODE_MODIFICATION and not git_diff.strip() and not changed_files:
            rr = ReviewResult(
                passed=False,
                reason="변경된 파일이 없음 (No changes detected)",
                details=["코드 변경(MODIFICATION) 작업이지만 diff/변경 파일이 비어 있음"],
                status=REVIEW_FAIL,
            )
            return self._apply_to_state(task, rr)

        # 3) 시맨틱 리뷰 (diff 본문이 있을 때): Codex(gpt-5.6-sol) 단일 엔진.
        #    읽기 전용 샌드박스, 실행 불가/파싱 실패는 fail-closed (자동 PASS 없음)
        if git_diff.strip():
            rr = self._run_semantic_review(
                requirement=requirement,
                changed_files=changed_files,
                git_diff=git_diff,
                tester_context=tester_ctx,
                task=task,
                attempt=attempt,
                max_attempts=max_attempts,
                base_details=details,
                label="codex 리뷰",
                review_batch=review_batch,
                all_changed_files=all_changed_files,
                batch_index=batch_index,
                batch_count=batch_count,
            )
            return self._apply_to_state(task, rr)

        # 4) diff 본문 없음 (1~2.5 통과)
        #    - ANALYSIS: 빈 diff 정상 → 규칙 기반 통과
        #    - MODIFICATION: changed_files 만 있고 diff 본문이 없으면 리뷰 불가 → REVIEW_ERROR
        if mode == TASK_MODE_MODIFICATION:
            rr = ReviewResult(
                passed=False,
                reason="diff 본문 없음(changed_files만 존재) → 시맨틱 리뷰 불가",
                details=details,
                status=REVIEW_ERROR,
            )
            return self._apply_to_state(task, rr)
        details.append("diff 본문 없음(분석 전용 작업) → 규칙 기반 검사 통과")
        rr = ReviewResult(
            passed=True, reason="PASS", details=details, status=REVIEW_PASS
        )
        return self._apply_to_state(task, rr)

    def _run_semantic_review(
        self,
        requirement: str,
        changed_files: list[str],
        git_diff: str,
        tester_context: str,
        task: TaskState | None,
        attempt: int,
        max_attempts: int,
        base_details: list[str],
        label: str,
        current_state: bool = False,
        worker_summary: str = "",
        review_batch: DiffBatch | None = None,
        all_changed_files: list[str] | None = None,
        batch_index: int = 1,
        batch_count: int = 1,
    ) -> ReviewResult:
        """Run one semantic Codex subprocess and return one structured verdict."""
        details = list(base_details)
        batches: list[str] = []
        plan: DiffBatchPlan | None = None
        if git_diff:
            if review_batch is not None:
                batches = [review_batch.text]
            else:
                plan = plan_diff_batches(git_diff)
                batches = [batch.text for batch in plan.batches]
            if plan is not None and not plan.valid:
                return ReviewResult(
                    passed=False,
                    reason=plan.reason or "diff coverage planning failed",
                    details=["diff coverage plan rejected without truncation"],
                    status=REVIEW_ERROR,
                    failure_code=plan.failure_code or REVIEW_COVERAGE_PLAN_INVALID,
                    coverage={
                        "total_files": list(dict.fromkeys(changed_files)),
                        "reviewed_files": [],
                        "missing_files": list(dict.fromkeys(changed_files)),
                        "batch_count": 0,
                        "truncated": False,
                        "expected_atom_count": len(plan.expected_atoms),
                        "assigned_atom_count": len(plan.assigned_atoms),
                        "missing_atom_count": len(plan.missing_atoms),
                        "duplicate_atom_count": len(plan.duplicate_atoms),
                    },
                )
            if plan is not None and len(plan.batches) != 1:
                return ReviewResult(
                    passed=False,
                    reason=(
                        "multi-batch diff requires Manager-owned review orchestration"
                    ),
                    details=[
                        "direct Reviewer invocation rejected before model execution"
                    ],
                    status=REVIEW_ERROR,
                    failure_code=REVIEW_COVERAGE_PLAN_INVALID,
                    coverage={
                        "total_files": list(dict.fromkeys(changed_files)),
                        "reviewed_files": [],
                        "missing_files": list(dict.fromkeys(changed_files)),
                        "batch_count": len(plan.batches),
                        "truncated": False,
                        "expected_atom_count": len(plan.expected_atoms),
                        "reviewed_atom_count": 0,
                    },
                )
            if plan is not None:
                review_batch = plan.batches[0]
                git_diff = review_batch.text
                changed_files = list(review_batch.changed_files)
        initial_effort = (
            task.reviewer_effective_reasoning_effort
            if task is not None
            and task.reviewer_effective_reasoning_effort in ("low", "medium", "high")
            else REVIEW_REASONING
        )

        verdict, reason, engine_details, ran, fail_kind, risk = self._codex_review(
            requirement=requirement,
            changed_files=changed_files,
            git_diff=git_diff,
            tester_context=tester_context,
            current_state=current_state,
            worker_summary=worker_summary,
            reasoning_effort=initial_effort,
            all_changed_files=all_changed_files or changed_files,
            review_batch=review_batch,
            batch_index=batch_index,
            batch_count=batch_count,
            review_context=(
                {
                    "current_task_changed_files": list(task.task_owned_changed_files),
                    "inherited_batch_delta_files": list(task.inherited_batch_delta_files),
                    "pre_job_snapshot_id": task.pre_job_snapshot_id,
                    "failure_fingerprint": task.failure_fingerprint,
                    "worker_tool_errors": (task.worker_stderr or "")[-1000:],
                    "profile_build_commands": list(task.profile_build_commands),
                    "build_evidence": dict(getattr(task, "build_evidence", {}) or {}),
                }
                if task is not None else {}
            ),
            task=task,
        )
        if not ran:
            details.extend(engine_details)
            failure_code = {
                "schema_invalid": REVIEW_SCHEMA_INVALID,
                "execution_unavailable": REVIEW_EXECUTION_UNAVAILABLE,
                "output_parse_failed": REVIEW_OUTPUT_PARSE_FAILED,
                "runtime_contract_drift": RUNTIME_CONTRACT_DRIFT,
                # Compatibility for injected pre-0.8.2 Reviewer fixtures. The
                # real Codex path emits only the three explicit 0.8.2 kinds.
                "unavailable": REVIEW_EXECUTION_UNAVAILABLE,
                "error": REVIEW_OUTPUT_PARSE_FAILED,
                "structured_invalid": STRUCTURED_OUTPUT_INVALID,
            }.get(fail_kind, REVIEW_EXECUTION_UNAVAILABLE)
            status = (
                REVIEW_UNAVAILABLE
                if fail_kind in ("execution_unavailable", "unavailable", "runtime_contract_drift")
                else REVIEW_ERROR
            )
            return ReviewResult(
                passed=False,
                reason=(
                    "리뷰 검증 불완전: 리뷰어 실행 불가(REVIEW_UNAVAILABLE)"
                    if status == REVIEW_UNAVAILABLE
                    else "리뷰 검증 불완전: structured review 처리 실패(REVIEW_ERROR)"
                ),
                details=details,
                status=status,
                failure_code=failure_code,
                reviewed_files=[],
                coverage={
                    "total_files": list(dict.fromkeys(changed_files)),
                    "reviewed_files": [],
                    "missing_files": list(dict.fromkeys(changed_files)),
                    "batch_count": len(batches),
                    "truncated": False,
                    "expected_atom_count": len(review_batch.changed_atoms) if review_batch else 0,
                    "reviewed_atom_count": 0,
                },
            )

        reason = reason or ("PASS" if verdict == "PASS" else "FAIL")
        details.append(f"[{label}] {reason}")
        details.extend(engine_details)

        final_risk = normalize_review_risk(
            risk,
            verdict_fail=(verdict == "FAIL"),
            fail_texts=[reason] + engine_details,
        )
        if verdict == "FAIL" and final_risk == REVIEW_RISK_HIGH and (
            risk and risk != REVIEW_RISK_HIGH
        ):
            details.append(
                f"등급 승격: 리뷰어 RISK={risk} → HIGH "
                f"(하드 제약 위반 감지)"
            )
        codes, reviewed_files, clean_engine_details, structured = _review_metadata(
            engine_details, changed_files
        )
        expected = list(dict.fromkeys(normalize_changed_path(path) for path in changed_files))
        missing = [path for path in expected if path not in reviewed_files]
        if structured and missing:
            return ReviewResult(
                passed=False,
                reason="structured review coverage incomplete",
                details=clean_engine_details + [f"missing reviewed file: {path}" for path in missing],
                status=REVIEW_ERROR,
                failure_code=REVIEW_COVERAGE_PLAN_INVALID,
                reviewed_files=reviewed_files,
                coverage={
                    "total_files": expected,
                    "reviewed_files": reviewed_files,
                    "missing_files": missing,
                    "batch_count": len(batches),
                    "truncated": False,
                    "expected_atom_count": len(review_batch.changed_atoms) if review_batch else 0,
                    "reviewed_atom_count": 0,
                },
            )
        clean_details = [
            detail for detail in details
            if not detail.startswith(("OUTPUT_MODE:", "VIOLATION_CODE: ", "REVIEWED_FILE: "))
        ]
        return ReviewResult(
            passed=(verdict == "PASS"),
            reason=reason,
            details=clean_details,
            status=REVIEW_PASS if verdict == "PASS" else REVIEW_FAIL,
            risk=final_risk,
            violation_codes=codes,
            reviewed_files=reviewed_files,
            coverage={
                "total_files": expected,
                "reviewed_files": reviewed_files,
                "missing_files": [],
                "batch_count": len(batches),
                "truncated": False,
                "expected_atom_count": len(review_batch.changed_atoms) if review_batch else 0,
                "reviewed_atom_count": len(review_batch.changed_atoms) if review_batch else 0,
            },
        )

    def plan_diff_batches(self, git_diff: str) -> DiffBatchPlan:
        return plan_diff_batches(git_diff)

    @staticmethod
    def plan_failure_result(plan: DiffBatchPlan, changed_files: list[str]) -> ReviewResult:
        return ReviewResult(
            passed=False,
            reason=plan.reason or "diff coverage planning failed",
            details=["diff coverage plan rejected without truncation"],
            status=REVIEW_ERROR,
            failure_code=plan.failure_code or REVIEW_COVERAGE_PLAN_INVALID,
            coverage={
                "total_files": list(dict.fromkeys(changed_files)),
                "reviewed_files": [],
                "missing_files": list(dict.fromkeys(changed_files)),
                "batch_count": 0,
                "truncated": False,
                "expected_atom_count": len(plan.expected_atoms),
                "assigned_atom_count": len(plan.assigned_atoms),
                "missing_atom_count": len(plan.missing_atoms),
                "duplicate_atom_count": len(plan.duplicate_atoms),
            },
        )

    @staticmethod
    def _apply_to_state(task: "TaskState | None", rr: ReviewResult) -> ReviewResult:
        """
        ReviewResult → TaskState 매핑(State 동기화).
        - review_status : REVIEW_PASS / REVIEW_FAIL / REVIEW_UNAVAILABLE / REVIEW_ERROR
        - review_result : reason + details (정제된 본문만 — raw stream-json/프롬프트
                          템플릿 저장 금지. md/json 이 수 KB 를 넘지 않게)
        - review_violations : semantic REVIEW_FAIL 위반/참고 사항만
        - review_risk : REVIEW_FAIL 인 경우 LOW/MID/HIGH (그 외 "")
        rr 자체는 그대로 반환하여 기존 흐름 유지.
        """
        if task is None:
            return rr
        rr.reason = _scrub_secrets(rr.reason)
        rr.details = [_scrub_secrets(item) for item in rr.details]
        task.review_status = rr.status
        clean_details = [_scrub_secrets(_clean_review_line(d)) for d in rr.details]
        clean_details = [d for d in clean_details if d]
        parts = [_scrub_secrets(_clean_review_line(rr.reason))] if rr.reason else []
        parts = [p for p in parts if p]
        parts.extend(clean_details)
        task.review_result = _scrub_secrets("\n".join(parts))
        # Worker retry feedback is semantic REVIEW_FAIL only. Infrastructure
        # parser/process diagnostics remain in review_result/coverage for Manager
        # arbitration and must not be injected as code-review findings.
        task.review_violations = (
            [_scrub_secrets(item) for item in (rr.violation_codes or clean_details)]
            if rr.status == REVIEW_FAIL
            else []
        )
        task.review_coverage = rr.coverage
        task.review_risk = (
            rr.risk if rr.status == REVIEW_FAIL and rr.risk in ("LOW", "MID", "HIGH") else ""
        )
        if rr.deterministic and rr.violation_codes:
            task.severity = "HIGH"
            task.fix_scope = "NON_RETRYABLE"
            task.failure_type = "policy"
            task.verification_status = "PARTIAL"
        elif rr.status in (REVIEW_ERROR, REVIEW_UNAVAILABLE):
            task.failure_type = "infrastructure"
            task.severity = "MID"
            task.fix_scope = "NON_RETRYABLE"
            task.verification_status = "FAILED"
        elif rr.status == REVIEW_FAIL:
            task.failure_type = "review"
            task.severity = rr.risk if rr.risk in ("LOW", "MID", "HIGH") else "HIGH"
            task.fix_scope = "RETRYABLE"
            task.verification_status = "FAILED"
        return rr

    def _remote_supplementary_context(self) -> tuple[str, bool]:
        """
        GitLab MCP 보조 원격 컨텍스트(옵션).
        - 활성 + GITLAB_PROJECT/GITLAB_MR_IID 환경변수가 있을 때만 원격 MR diff 조회.
        - 반환: (supplementary_text, used). used=False 면 빈 문자열.
        - 주의: 로컬 diff 가 게이트의 source of truth. 본 텍스트는 '참고용 보조'로만 프롬프트에 붙음.
        - 네트워크 오류/비활성 시 ("" , False) → 주 경로에 영향 없음.
        """
        try:
            import gitlab_mcp_client as gmcp  # 지연 import
        except Exception as e:
            print(f"[Reviewer] gitlab_mcp_client import 스킵: {e}")
            return "", False
        if not gmcp.is_enabled():
            return "", False
        project = os.environ.get("GITLAB_PROJECT", "")
        mr_iid = os.environ.get("GITLAB_MR_IID", "")
        if not project or not mr_iid:
            return "", False
        try:
            diff = gmcp.get_mr_diff(project, int(mr_iid))
        except Exception as e:
            print(f"[Reviewer] 원격 MR diff 조회 스킵: {e}")
            return "", False
        if not diff:
            return "", False
        # 너무 길면 뒤쪽 4000자로 제한(보조 컨텍스트)
        body = diff[-4000:]
        return (
            f"\n[보조 원격 MR diff (참고용, 게이트 아님)]\nproject={project} mr={mr_iid}\n{body}\n",
            True,
        )

    def _codex_review(
        self,
        requirement: str,
        changed_files: list[str],
        git_diff: str,
        tester_context: str = "",
        current_state: bool = False,
        worker_summary: str = "",
        reasoning_effort: str = REVIEW_REASONING,
        all_changed_files: list[str] | None = None,
        review_batch: DiffBatch | None = None,
        batch_index: int = 1,
        batch_count: int = 1,
        review_context: dict[str, object] | None = None,
        task: TaskState | None = None,
    ) -> tuple[str | None, str | None, list[str], bool, str, str]:
        """
        리뷰 엔진 (단일): codex exec (gpt-5.6-sol) 로 [규칙]+[Diff]+[테스트 증거] 검수.
        반환: (verdict: "PASS"|"FAIL"|None, reason|None, details, ran:bool,
               fail_kind: "" | "unavailable" | "error", risk: ""|"LOW"|"MID"|"HIGH")
        - stdin 으로 프롬프트 전달 (CodexWorker 와 동일 방식, Windows 안정성).
        - 읽기 전용: -s read-only. workspace-write / --approve-for-me /
          --dangerously-bypass 금지 (리뷰는 검증만 — 권한 상승 없음).
        - reasoning effort = 호출 인자(기본 REVIEW_REASONING=medium).
        - --json: JSONL 이벤트 단위 출력 → 무활동 타이머가 실제 활동마다 리셋.
        - -o <file>: 최종 에이전트 메시지를 파일로 확정 수신(파싱 1차 소스).
        - droid 리뷰 경로는 제거됨 (운영 정책: 리뷰는 Codex 단일 엔진).
        """
        prompt = self._build_review_prompt(
            requirement, changed_files, git_diff, tester_context,
            current_state=current_state,
            worker_summary=worker_summary,
            all_changed_files=all_changed_files,
            review_batch=review_batch,
            batch_index=batch_index,
            batch_count=batch_count,
            review_context=review_context,
            task=task,
        )

        descriptor = None
        requested_session_contract: dict[str, object] = {}
        reuse_session = False
        if task is not None and task.materialized_execution:
            from codex_runtime_adapter import CodexRuntimeAdapter
            from gate_evidence import identity as gate_identity
            from runtime_adapter import payload_sha256
            from runtime_contract import RuntimeContractError, assert_role_contract_compatible
            from session_authority import reviewer_session_contract, session_reuse_allowed

            descriptor = CodexRuntimeAdapter().preflight()
            if not descriptor.schema_compatible:
                return None, None, [descriptor.detail], False, "execution_unavailable", ""
            try:
                assert_role_contract_compatible(
                    task.materialized_execution.get("runtime_contracts"), descriptor, "REVIEWER"
                )
            except RuntimeContractError:
                return None, None, [RUNTIME_CONTRACT_DRIFT], False, "runtime_contract_drift", ""
            frozen = dict(task.materialized_execution)
            context = dict(frozen.get("execution_context") or {})
            workspace_identity = str(context.get("workspace_identity_sha256") or "")
            source_view_id = str(frozen.get("source_view_id") or "")
            if not source_view_id:
                source_view_id = "SV-LEGACY-" + payload_sha256({
                    "workspace_identity": workspace_identity,
                    "execution_id": task.execution_id,
                })[:24].upper()
            candidate_hash = str(
                gate_identity(task, self.working_dir).get("candidate_hash") or ""
            ) or payload_sha256({
                "changed_files": list(changed_files), "git_diff": git_diff,
            })
            review_input_hash = payload_sha256({
                "requirement": requirement,
                "changed_files": list(changed_files),
                "git_diff_sha256": hashlib.sha256(git_diff.encode("utf-8")).hexdigest(),
                "batch_index": int(batch_index),
                "batch_count": int(batch_count),
            })
            requested_session_contract = reviewer_session_contract(
                logical_job_id=str(frozen.get("logical_job_id") or task.logical_job_id),
                contract_revision=int(frozen.get("contract_revision") or 0),
                role="REVIEWER",
                workspace_identity=workspace_identity,
                source_view_id=source_view_id,
                execution_surface_hash=str(frozen.get("execution_surface_hash") or ""),
                candidate_hash=candidate_hash,
                review_input_hash=review_input_hash,
            )
            reuse_session = bool(
                task.reviewer_recovery_count > 0
                and task.reviewer_native_session_id
                and session_reuse_allowed(
                    task.reviewer_session_contract, requested_session_contract
                )
            )
            task.reviewer_session_reuse_mode = (
                "TECHNICAL_CONTINUATION" if reuse_session else "FRESH_LOGICAL_REVIEW"
            )
            task.reviewer_session_contract = dict(requested_session_contract)
            if not reuse_session:
                task.reviewer_native_session_binding_id = ""
                task.reviewer_native_session_id = ""

        # Reject an invalid response schema locally. This check deliberately
        # precedes temp-file creation and the Codex subprocess.
        try:
            validate_review_output_schema(_REVIEW_SCHEMA)
        except ReviewSchemaValidationError as exc:
            return (
                None,
                None,
                [f"review schema validation failed: {scrub_secrets(str(exc))}"],
                False,
                "schema_invalid",
                "",
            )

        # 최종 메시지 수신 파일 (codex -o) and validated output schema.
        out_fd, out_file = tempfile.mkstemp(suffix=".txt", prefix="kkm_review_", dir=scratch_root("reviewer"))
        os.close(out_fd)
        schema_fd, schema_file = tempfile.mkstemp(suffix=".json", prefix="kkm_review_schema_", dir=scratch_root("reviewer"))
        with os.fdopen(schema_fd, "w", encoding="utf-8") as schema_handle:
            json.dump(_REVIEW_SCHEMA, schema_handle, ensure_ascii=True)

        effective_effort = (
            reasoning_effort
            if reasoning_effort in ("low", "medium", "high")
            else "medium"
        )

        effective_model = str(
            dict(getattr(task, "review_policy", {}) or {}).get("model", REVIEW_MODEL)
        )
        if effective_model not in CODEX_MODELS:
            return (
                None, None, [f"review model unavailable: {effective_model}"], False,
                "execution_unavailable", "",
            )
        if reuse_session:
            cmd = [
                "codex", "exec", "resume",
                "--skip-git-repo-check",
                "-m", effective_model,
                "-c", f'model_reasoning_effort="{effective_effort}"',
                "-c", "project_doc_max_bytes=0",
                "--json", "--output-schema", schema_file,
                "-o", out_file,
                task.reviewer_native_session_id, "-",
            ]
        else:
            cmd = [
                "codex", "exec",
                "-C", str(self.working_dir),
                "-s", "read-only",                    # 읽기 전용 샌드박스
                "--skip-git-repo-check",
                "-m", effective_model,
                "-c", f'model_reasoning_effort="{effective_effort}"',
                "-c", "project_doc_max_bytes=0",
                "--json",                             # JSONL 이벤트 (무활동 타이머 리셋)
                "--output-schema", schema_file,
                "-o", out_file,                       # 최종 메시지 파일
                "-",                                  # 프롬프트는 stdin 으로
            ]
        print(f"[Reviewer] codex 리뷰 실행: (model={effective_model}, "
              f"reasoning={effective_effort}, sandbox=read-only)")
        if self.timeout_config:
            print(f"[Reviewer] 타임아웃 정책: {self.timeout_config.summary}")
        print("-" * 60)

        def record_native_session(raw: str, state: str) -> None:
            if (
                task is None or not task.materialized_execution
                or descriptor is None or not requested_session_contract
            ):
                return
            from codex_runtime_adapter import CodexRuntimeAdapter
            from runtime_adapter import normalize_json_lines

            events = normalize_json_lines(raw, CodexRuntimeAdapter())
            session_id = next(
                (event.session_id for event in events if event.session_id), ""
            ) or (task.reviewer_native_session_id if reuse_session else "")
            if not session_id:
                return
            task.reviewer_native_session_id = session_id
            if not task.control_repository_path:
                return
            from control_repository import ControlRepository

            frozen = dict(task.materialized_execution)
            binding = ControlRepository(task.control_repository_path).bind_native_session(
                task.execution_id,
                runtime="codex",
                native_session_id=session_id,
                adapter_revision=descriptor.adapter_revision,
                runtime_version=descriptor.version,
                durable=True,
                state=state,
                role="REVIEWER",
                source_view_id=str(requested_session_contract["source_view_id"]),
                workspace_identity=str(requested_session_contract["workspace_identity"]),
                runtime_contract_hash=str(frozen.get("runtime_contract_hash") or ""),
                candidate_hash=str(requested_session_contract["candidate_hash"]),
                review_contract_hash=str(requested_session_contract["session_contract_hash"]),
                allow_fresh=not reuse_session,
            )
            task.reviewer_native_session_binding_id = binding["binding_id"]

        try:
            cmd[0] = trusted_executable(cmd[0], forbidden_root=self.working_dir)
            child_env = isolated_subprocess_env(
                "reviewer", self.profile.profile_dir if self.profile else None
            )
            if self.timeout_config:
                # 워커와 동일한 활동 기반 타임아웃 + heartbeat.
                # 리뷰는 읽기 전용 작업이므로 ANALYSIS 기준(1800s) 적용.
                runtime_callback = getattr(task, "_runtime_event_callback", None)
                runtime_options = ({
                    "runtime_event": runtime_callback,
                    "execution_id": "INV-" + __import__("uuid").uuid4().hex.upper(),
                    "execution_role": "REVIEWER",
                    "attempt_id": task.reviewer_attempt_id,
                    "runtime_name": "codex",
                } if callable(runtime_callback) else {})
                exit_code, stdout, stderr, timeout_reason = _run_with_activity_timeout(
                    cmd=cmd,
                    stdin_data=prompt,
                    cwd=str(self.working_dir),
                    cfg=self.timeout_config,
                    requirement=requirement,
                    task_mode="ANALYSIS",
                    display_events=True,
                    log_prefix="[Reviewer]",
                    env=child_env,
                    **runtime_options,
                )
                record_native_session(
                    stdout,
                    "IDLE" if exit_code == 0 and not timeout_reason else "UNKNOWN",
                )
                if timeout_reason:
                    return None, None, [
                        f"codex timeout({timeout_reason})"
                    ], False, "execution_unavailable", ""
                if exit_code != 0:
                    failure_kind = _codex_review_failure_kind(stdout, stderr)
                    return None, None, [
                        _codex_exit_failure(exit_code, stdout, stderr)
                    ], False, failure_kind, ""
                raw_stdout = stdout
            else:
                # 폴백: 기존 subprocess.run (타임아웃 정책 미주입 시)
                assert_secret_free_argv(cmd, env=child_env)
                result = subprocess.run(
                    cmd,
                    input=prompt,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=self.timeout,
                    env=child_env,
                )
                record_native_session(
                    result.stdout or "", "IDLE" if result.returncode == 0 else "UNKNOWN"
                )
                if result.returncode != 0:
                    failure_kind = _codex_review_failure_kind(
                        result.stdout or "", result.stderr or ""
                    )
                    return None, None, [
                        _codex_exit_failure(
                            result.returncode, result.stdout or "", result.stderr or ""
                        )
                    ], False, failure_kind, ""
                raw_stdout = result.stdout or ""

            # 최종 텍스트: -o 파일 1차 소스 → JSONL agent_message → raw 폴백.
            # 프롬프트 에코의 "RESULT: PASS 또는 FAIL" 템플릿 라인이 raw 에 남아
            # 오판한 554KB 리포트 사고 방지: 최종 메시지 파일만 1차 소스.
            final_text = ""
            source = "output-last-message"
            messages = _extract_codex_messages(raw_stdout)
            try:
                final_text = Path(out_file).read_text(
                    encoding="utf-8", errors="replace"
                )
            except Exception:
                pass
            if not final_text.strip():
                source = "jsonl-final-agent-message"
                final_text = messages[-1] if messages else ""
            parsed = self._parse_structured_review_output_diagnostic(
                final_text, source=source, candidate_count=len(messages)
            )
            if parsed.value is not None:
                verdict, reason, extra, risk, codes, reviewed = parsed.value
                extra.extend(f"VIOLATION_CODE: {code}" for code in codes)
                extra.extend(f"REVIEWED_FILE: {path}" for path in reviewed)
                extra.append("OUTPUT_MODE: structured")
            elif parsed.is_json:
                diagnostic = parsed.diagnostic
                safe_diagnostic = (
                    "structured parser diagnostic: "
                    f"source={diagnostic['source']}, length={diagnostic['string_length']}, "
                    f"candidates={diagnostic['candidate_count']}, root={diagnostic['root_type']}, "
                    f"stage={diagnostic['stage']}, path={diagnostic['invalid_path']}, "
                    f"type={diagnostic['invalid_type']}, codex_exit_code=0, "
                    "timeout=false, stdout_reader=complete, stderr_reader=complete"
                )
                return None, None, [safe_diagnostic], False, "output_parse_failed", ""
            else:
                print("[Reviewer] structured output fallback: text parser")
                verdict, reason, extra, risk = self._parse_review_output(final_text)
                extra.append("OUTPUT_MODE: text-fallback")
            if verdict is None:
                # 추출 실패 → fail-closed(REVIEW_ERROR). raw response는 TaskState에 저장하지 않는다.
                return (
                    None, None,
                    ["codex response parsing failed or required fields invalid"],
                    False, "output_parse_failed", "",
                )
            return verdict, reason, extra, True, "", risk
        except FileNotFoundError:
            return None, None, ["codex 미설치"], False, "execution_unavailable", ""
        except subprocess.TimeoutExpired:
            return None, None, [
                f"codex timeout({self.timeout}s)"
            ], False, "execution_unavailable", ""
        finally:
            try:
                Path(out_file).unlink(missing_ok=True)
            except Exception:
                pass
            try:
                Path(schema_file).unlink(missing_ok=True)
            except Exception:
                pass

    def _build_review_prompt(
        self,
        requirement: str,
        changed_files: list[str],
        git_diff: str,
        tester_context: str = "",
        current_state: bool = False,
        worker_summary: str = "",
        all_changed_files: list[str] | None = None,
        review_batch: DiffBatch | None = None,
        batch_index: int = 1,
        batch_count: int = 1,
        review_context: dict[str, object] | None = None,
        task: TaskState | None = None,
    ) -> str:
        """codex/droid 공용 리뷰 프롬프트 빌더.
        tester_context(테스트 증거/리뷰 포인트)가 있으면 프롬프트에 주입한다."""
        if self.profile is not None and task is not None:
            pack = build_and_record_context(
                task=task,
                role="REVIEWER",
                profile=self.profile,
                workspace=task.working_dir or self.working_dir,
                policy_root=self.policy_root or AGENTS_DIR,
                manifest_root=self.manifest_root,
                requirement=requirement,
                changed_files=changed_files,
                task_evidence={
                    "changed_files": list(changed_files),
                    "task_owned_changed_files": list(task.task_owned_changed_files),
                    "inherited_batch_delta_files": list(task.inherited_batch_delta_files),
                    "build_evidence": dict(task.build_evidence),
                    "test_status": task.test_status,
                    "test_summary": task.test_summary,
                    "review_findings": list(task.review_violations),
                },
                context_tier=str(dict(task.review_policy or {}).get("context_tier", "DOMAIN")),
            )
            rules = pack.prompt
        else:
            rules = load_review_rules(
                self.profile,
                changed_files,
                policy_root=self.policy_root,
                requirement=requirement,
            )
        files_str = "\n".join(changed_files) if changed_files else "(없음)"
        all_files = all_changed_files or changed_files
        all_files_str = "\n".join(all_files) if all_files else "(없음)"
        diff_body = git_diff

        batch_block = ""
        if review_batch is not None:
            metadata = [
                {
                    "chunk_id": chunk.chunk_id,
                    "module": chunk.module,
                    "path": chunk.path,
                    "change_kind": chunk.change_kind,
                    "hunk_id": chunk.hunk_id,
                    "original_hunk_header": chunk.original_hunk_header,
                    "chunk_ordinal": chunk.chunk_ordinal,
                    "chunk_count": chunk.chunk_count,
                    "original_old_range": chunk.original_old_range,
                    "original_new_range": chunk.original_new_range,
                    "chunk_old_range": chunk.chunk_old_range,
                    "chunk_new_range": chunk.chunk_new_range,
                    "removed_old_ranges": chunk.removed_old_ranges,
                    "added_new_ranges": chunk.added_new_ranges,
                    "content_sha256": chunk.content_sha256,
                    "context_overlap": chunk.context_overlap,
                    "rename_from": chunk.rename_from,
                    "rename_to": chunk.rename_to,
                    "new_file": chunk.new_file,
                    "deleted_file": chunk.deleted_file,
                    "full_rewrite": chunk.full_rewrite,
                    "eol_only": chunk.eol_only,
                    "binary": chunk.binary,
                }
                for chunk in review_batch.chunks
            ]
            batch_block = (
                f"[Review batch]\n"
                f"- batch_id: {review_batch.batch_id}\n"
                f"- batch: {batch_index}/{batch_count}\n"
                f"- chunk_metadata: {json.dumps(metadata, ensure_ascii=True)}\n"
                "This input is one part of the full change. Do not claim that code "
                "outside this batch was reviewed.\n\n"
                f"[All changed files]\n{all_files_str}\n\n"
            )

        # 보조 원격 MR diff (옵션): 게이트가 아닌 참고용. 비활성 시 빈 문자열.
        remote_ctx, _remote_used = self._remote_supplementary_context()

        # 테스트 증거/리뷰 포인트 주입 (planner/tester 가 있을 때만)
        tester_block = f"{tester_context}\n\n" if tester_context.strip() else ""
        structured_context = json.dumps(
            review_context or {}, ensure_ascii=False, sort_keys=True
        )
        skeleton_evidence = ""
        if task is not None:
            from skeleton_policy import enabled
            if enabled(task):
                skeleton_evidence = (
                    "[Skeleton evidence authority]\n"
                    "Worker-authored verification.json/source-hashes.json are auxiliary, not "
                    "canonical gate evidence. Manager may normalize EOL after Worker completion. "
                    "Do not require pre-normalization auxiliary hashes or Worker-claimed final "
                    "Verification PASS as prerequisites for this Review. Inspect current code "
                    "and the fresh Manager Build/Test evidence bound to the current input. "
                    "A stale auxiliary hash alone does not invalidate current canonical gates; "
                    "missing, stale or mismatched canonical evidence must still fail. "
                    "Manager Verification follows Review; deferred DB/E2E remains NOT_RUN_BY_POLICY, "
                    "never a fabricated PASS. The parsed qa_request below uses the canonical "
                    "decision_independent_work_complete boolean; the skeleton contract's prose "
                    "'completion fields' does not introduce a field named completion or completion_kind. "
                    "Check the parsed deferred records and safe-boundary claim against current code; "
                    "missing or contradictory canonical QA data must still fail. "
                    "This does not waive any code/security finding.\n"
                    + json.dumps({"eol_repairs": list(task.eol_repairs),
                                  "qa_request": dict(task.qa_request),
                                  "gate_evidence": dict(task.gate_evidence)},
                                 ensure_ascii=False, sort_keys=True) + "\n\n"
                )

        review_target = (
            "[현재 상태 검증 모드]\n"
            "이 Job에서 새 Diff는 없고 Worker는 작업 시작 전부터 요구사항이 충족됐다고 주장한다.\n"
            "아래 검증 대상 파일을 실제 작업 디렉토리에서 직접 열어 요구사항을 항목별로 확인하라.\n"
            "Worker 요약은 증거가 아니며, 모든 요구사항이 현재 코드로 확인될 때만 PASS하라.\n"
            f"[Worker 주장]\n{worker_summary[-2000:]}\n\n"
            if current_state else ""
        )

        return (
            "너는 코드 리뷰어야. 아래 [규칙]을 기준으로 코드 상태를 검사하고, "
            "반드시 [출력 형식] 에 맞게만 답해.\n\n"
            f"[규칙]\n{rules}\n\n"
            f"[작업 요구사항]\n{requirement}\n\n"
            f"[Task-scoped review context]\n{structured_context}\n"
            f"{skeleton_evidence}"
            "current_task_changed_files만 필수 coverage 대상으로 삼고 inherited_batch_delta_files는 "
            "읽기 전용 누적 문맥으로만 사용하라. 모듈명으로 파일 coverage를 대체하지 마라.\n\n"
            "build_evidence.schema_version과 executions가 있고 각 실행의 task/baseline/restore identity, "
            "exit_code=0, status=PASS, log hash/path가 검증된 경우 그 structured evidence가 Build 실행의 "
            "권위 있는 기록이다. 자연어 Worker 주장으로 이를 부정하지 마라. 실제 structured evidence가 "
            "불완전할 때만 violation_codes에 BUILD_EVIDENCE_MISSING을 사용하라.\n\n"
            f"{review_target}"
            f"[{'검증 대상 파일' if current_state else '변경 파일 목록'}]\n{files_str}\n\n"
            f"{batch_block}"
            f"[Diff]\n{diff_body}\n\n"
            f"{remote_ctx}"
            f"{tester_block}"
            "[출력 형식] 제공된 JSON Schema를 정확히 따를 것.\n"
            "result=PASS|FAIL, reason=문자열, details=문자열 배열, "
            "risk=NONE|LOW|MID|HIGH, violation_codes=문자열 배열, "
            "reviewed_files=실제로 검토한 현재 Task 파일 배열. 가능하면 동일 배열을 "
            "reviewed_task_files에도 기록하고, 변경하지 않은 target을 직접 확인했다면 "
            "inspected_unchanged_target_files에 기록하라.\n"
            f"violation_codes 각 값은 semantic 분류이며 {SEMANTIC_VIOLATION_CODE_PATTERN} "
            f"형식, 최대 {SEMANTIC_VIOLATION_CODE_MAX_ITEMS}개를 지켜라. 시스템 상태 코드를 쓰지 마라.\n"
            "텍스트 fallback일 때만 RESULT/REASON/DETAIL/RISK 줄을 사용할 것.\n"
            "RISK: FAIL 인 경우 LOW|MID|HIGH 로 재시도 위험도를 평가\n"
            "  - LOW : 국소 지적(표시/검증/문구/국소 로직) — 부분 수정으로 해결 가능\n"
            "  - MID : 구조·범위가 요구사항 대비 과다 — 같은 트리에서 전면 수정 필요\n"
            "  - HIGH: 하드 제약 위반(비밀/위험DB/외부API 무단/core·인프라) 또는 근본 재작성 필요\n"
            "PASS 면 RISK: NONE 이라고 쓸 것.\n"
        )

    @staticmethod
    def _parse_structured_review_output(
        out: str,
    ) -> tuple[str, str, list[str], str, list[str], list[str]] | None:
        return Reviewer._parse_structured_review_output_diagnostic(out).value

    @staticmethod
    def _parse_structured_review_output_diagnostic(
        out: str,
        source: str = "unknown",
        candidate_count: int = 0,
    ) -> StructuredParseResult:
        diagnostic: dict[str, object] = {
            "source": source,
            "string_length": len(out) if isinstance(out, str) else 0,
            "candidate_count": candidate_count,
            "root_type": "unknown",
            "invalid_path": "$",
            "invalid_type": "",
            "stage": "json_syntax",
        }
        try:
            data = json.loads(out)
        except (TypeError, json.JSONDecodeError):
            return StructuredParseResult(None, False, diagnostic)
        diagnostic["root_type"] = type(data).__name__
        diagnostic["stage"] = "schema"
        if not isinstance(data, dict):
            diagnostic["invalid_type"] = type(data).__name__
            return StructuredParseResult(None, True, diagnostic)
        required = list(_REVIEW_SCHEMA["required"])
        for name in required:
            if name not in data:
                diagnostic["invalid_path"] = f"$.{name}"
                diagnostic["invalid_type"] = "missing"
                return StructuredParseResult(None, True, diagnostic)
        extra_keys = [name for name in data if name not in _REVIEW_SCHEMA["properties"]]
        if extra_keys:
            diagnostic["invalid_path"] = f"$.{extra_keys[0]}"
            diagnostic["invalid_type"] = "additional_property"
            return StructuredParseResult(None, True, diagnostic)
        verdict = data.get("result")
        reason = data.get("reason")
        details = data.get("details")
        risk = data.get("risk")
        codes = data.get("violation_codes")
        reviewed = data.get("reviewed_files")
        reviewed_task = data.get("reviewed_task_files")
        inspected_unchanged = data.get("inspected_unchanged_target_files")
        checks = (
            ("result", verdict, str), ("reason", reason, str),
            ("details", details, list), ("risk", risk, str),
            ("violation_codes", codes, list), ("reviewed_files", reviewed, list),
        )
        for name, value, expected_type in checks:
            if not isinstance(value, expected_type):
                diagnostic["invalid_path"] = f"$.{name}"
                diagnostic["invalid_type"] = type(value).__name__
                return StructuredParseResult(None, True, diagnostic)
        for name, value in (
            ("reviewed_task_files", reviewed_task),
            ("inspected_unchanged_target_files", inspected_unchanged),
        ):
            if value is not None and not isinstance(value, list):
                diagnostic["invalid_path"] = f"$.{name}"
                diagnostic["invalid_type"] = type(value).__name__
                return StructuredParseResult(None, True, diagnostic)
        if verdict not in ("PASS", "FAIL"):
            diagnostic["invalid_path"] = "$.result"
            diagnostic["invalid_type"] = "enum"
            return StructuredParseResult(None, True, diagnostic)
        if not reason.strip() or len(reason) > 4000:
            diagnostic["invalid_path"] = "$.reason"
            diagnostic["invalid_type"] = "length"
            return StructuredParseResult(None, True, diagnostic)
        if risk not in ("NONE", "LOW", "MID", "HIGH"):
            diagnostic["invalid_path"] = "$.risk"
            diagnostic["invalid_type"] = "enum"
            return StructuredParseResult(None, True, diagnostic)
        for name, values, max_items, max_length in (
            ("details", details, REVIEW_TEXT_MAX_ITEMS, 4000),
            ("violation_codes", codes, SEMANTIC_VIOLATION_CODE_MAX_ITEMS, 64),
            ("reviewed_files", reviewed, REVIEWED_FILE_MAX_ITEMS, 1024),
            (
                "reviewed_task_files",
                reviewed_task or [],
                REVIEWED_FILE_MAX_ITEMS,
                1024,
            ),
            (
                "inspected_unchanged_target_files",
                inspected_unchanged or [],
                REVIEWED_FILE_MAX_ITEMS,
                1024,
            ),
        ):
            if len(values) > max_items:
                diagnostic["invalid_path"] = f"$.{name}"
                diagnostic["invalid_type"] = "max_items"
                return StructuredParseResult(None, True, diagnostic)
            seen: set[str] = set()
            for index, item in enumerate(values):
                if not isinstance(item, str):
                    diagnostic["invalid_path"] = f"$.{name}[{index}]"
                    diagnostic["invalid_type"] = type(item).__name__
                    return StructuredParseResult(None, True, diagnostic)
                if len(item) > max_length or (name != "details" and not item):
                    diagnostic["invalid_path"] = f"$.{name}[{index}]"
                    diagnostic["invalid_type"] = "length"
                    return StructuredParseResult(None, True, diagnostic)
                if name != "details" and item in seen:
                    diagnostic["invalid_path"] = f"$.{name}[{index}]"
                    diagnostic["invalid_type"] = "duplicate"
                    return StructuredParseResult(None, True, diagnostic)
                seen.add(item)
        code_pattern = re.compile(SEMANTIC_VIOLATION_CODE_PATTERN)
        for index, code in enumerate(codes):
            if not code_pattern.fullmatch(code):
                diagnostic["invalid_path"] = f"$.violation_codes[{index}]"
                diagnostic["invalid_type"] = "pattern"
                return StructuredParseResult(None, True, diagnostic)
        diagnostic["stage"] = "semantic"
        if (verdict == "PASS" and risk != "NONE") or (verdict == "FAIL" and risk == "NONE"):
            diagnostic["invalid_path"] = "$.risk"
            diagnostic["invalid_type"] = "cross_field"
            return StructuredParseResult(None, True, diagnostic)
        diagnostic["invalid_path"] = ""
        diagnostic["invalid_type"] = ""
        diagnostic["stage"] = "valid"
        value = (verdict, reason.strip(), details, "" if risk == "NONE" else risk, codes, reviewed)
        return StructuredParseResult(value, True, diagnostic)

    @staticmethod
    def _parse_review_output(out: str) -> tuple[str | None, str | None, list[str], str]:
        """
        리뷰어 출력에서 RESULT/REASON/DETAIL/RISK 파싱 (droid 공용).
        반환: (verdict: "PASS"|"FAIL"|None, reason|None, details, risk: ""|"LOW"|"MID"|"HIGH")
        verdict 가 None 이면 파싱 실패(REVIEW_ERROR 대상).
        risk 는 RESULT=FAIL 일 때만 의미 있고, 누락 시 ""(호출부에서 HIGH로 정규화).
        """
        # Legacy fallback is intentionally narrow.  It is accepted only when
        # the first non-empty response line is the complete RESULT line; this
        # prevents prompt echoes, prose, JSON fragments, or embedded templates
        # from being mistaken for a verdict.
        normalized = out.lstrip("\ufeff \t\r\n") if isinstance(out, str) else ""
        first_line = normalized.splitlines()[0] if normalized else ""
        m = re.fullmatch(r"RESULT:[ \t]*(PASS|FAIL)[ \t]*", first_line, re.IGNORECASE)
        if not m:
            return None, None, [], ""
        legacy = re.fullmatch(
            r"RESULT:[ \t]*(PASS|FAIL)[ \t]*\r?\n"
            r"REASON:[ \t]*([^\r\n]+?)[ \t]*"
            r"(?:\r?\nDETAIL:[ \t]*(.*?))?"
            r"\r?\nRISK:[ \t]*(NONE|LOW|MID|HIGH)[ \t]*",
            normalized,
            re.IGNORECASE | re.DOTALL,
        )
        if legacy is None:
            return None, None, [], ""

        verdict = legacy.group(1).upper()
        reason = legacy.group(2).strip()
        detail = (legacy.group(3) or "").strip()
        risk_value = legacy.group(4).upper()
        if (verdict == "PASS" and risk_value != "NONE") or (verdict == "FAIL" and risk_value == "NONE"):
            return None, None, [], ""
        extra = [detail] if detail else []
        risk = "" if risk_value == "NONE" else risk_value
        return verdict, reason, extra, risk
