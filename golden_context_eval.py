"""Deterministic Golden Context Eval for Harness 0.8.5 (no model calls)."""

from __future__ import annotations

import argparse
import json
import re
import statistics
from pathlib import Path
from typing import Any, Sequence

from context_foundation import build_context_pack
from project_profile import ProjectProfile, load_project_profile


MANDATORY_RULE_IDS = {
    "COMMON.SCOPE.NO_EXPANSION",
    "COMMON.CHANGE.NO_UNRELATED_REFACTOR",
    "COMMON.SECRET.NO_EXPOSURE",
    "COMMON.VERIFY.EVIDENCE_REQUIRED",
    "COMMON.SECURITY.DESTRUCTIVE_BOUNDARY",
    "COMMON.VERIFY.COMPLETION_GATE",
    "E4U.SCOPE.PROFILE",
    "E4U.BUSINESS.UNKNOWN_REQUIRES_HUMAN",
    "E4U.ALWAYS.VALIDATION_PROJECT",
}


def _selected(pack) -> list[dict[str, Any]]:
    return list(pack.manifest["selected_resources"])


def _paths(pack) -> list[str]:
    return [item["path"] for item in _selected(pack)]


def _rule_ids(pack) -> set[str]:
    return {
        rule_id
        for item in _selected(pack)
        for rule_id in item.get("rule_ids", [])
    }


def _legacy_static_chars(
    profile: ProjectProfile,
    policy_root: Path,
    requirement: str,
    modules: Sequence[str],
    changed_files: Sequence[str] = (),
) -> int:
    """Measure the 0.8.4 resource-loading shape without invoking an agent."""
    paths = [
        policy_root / "AGENTS.md",
        policy_root / "hooks" / "validation.md",
        policy_root / "skills" / "project-context" / "SKILL.md",
        profile.project_rules,
        *profile.rules.get("always", ()),
    ]
    areas = profile.detect_areas(requirement, modules, changed_files).areas
    for area in areas:
        paths.extend(profile.rules.get(area, ()))
        if area in {"backend", "frontend", "sql"}:
            paths.append(policy_root / "skills" / area / "SKILL.md")
    unique = list(dict.fromkeys(Path(path) for path in paths))
    total = sum(len(path.read_text(encoding="utf-8")) for path in unique) + len(requirement)
    analysis = profile.analysis.read_text(encoding="utf-8")
    headings = list(re.finditer(r"(?m)^#{1,4}\s+.+$", analysis))
    words = [*modules, *areas, *re.findall(r"\b[A-Z][A-Za-z0-9_]{3,}\b", requirement)]
    selected = 0
    for index, heading in enumerate(headings):
        end = headings[index + 1].start() if index + 1 < len(headings) else len(analysis)
        section = analysis[heading.start():end]
        if words and any(re.search(re.escape(word), section, re.IGNORECASE) for word in words):
            total += len(section)
            selected += 1
            if selected >= 4:
                break
    return total


def run_golden_context_eval(
    profile_dir: str | Path = r"D:\agents\profiles\e4u",
    workspace: str | Path = r"D:\inha_dev",
    policy_root: str | Path = r"D:\agents",
) -> dict[str, Any]:
    profile = load_project_profile(profile_dir, workspace)
    policy = Path(policy_root).resolve()
    common = dict(profile=profile, workspace=workspace, policy_root=policy)
    cases: list[dict[str, Any]] = []

    def record(name: str, pack, checks: dict[str, bool], before: int | None = None) -> None:
        cases.append({
            "case": name,
            "passed": all(checks.values()),
            "checks": checks,
            "selected_resources": _selected(pack),
            "omitted_resources": list(pack.manifest["omitted_resources"]),
            "context_pack_id": pack.manifest["context_pack_id"],
            "total_chars": pack.manifest["total_chars"],
            "before_chars": before,
            "reduction_percent": (
                round((1 - pack.manifest["total_chars"] / before) * 100, 2)
                if before else None
            ),
        })

    req1 = "sucui 학생 화면의 안내 문구만 수정"
    one = build_context_pack(role="WORKER", requirement=req1, target_modules=["sucui"], task_mode="MODIFICATION", **common)
    p1 = _paths(one)
    record("1_sucui_simple_copy", one, {
        "frontend_procedure": "policy:skills/frontend/SKILL.md" in p1,
        "scope_rules": MANDATORY_RULE_IDS <= _rule_ids(one),
        "no_sql": "policy:skills/sql/SKILL.md" not in p1,
        "no_analysis_overview": "profile:PROJECT_ANALYSIS.md" not in p1,
        "no_db_catalog": not any("catalog.json" in path for path in p1),
    }, _legacy_static_chars(profile, policy, req1, ["sucui"]))

    req2 = "suc/src/main/java/suc/sp/query/SsucSp0021Qry.xml의 SSUC383 조회를 현재 계약에 맞게 수정"
    two = build_context_pack(role="WORKER", requirement=req2, target_modules=["suc"], task_mode="MODIFICATION", **common)
    p2 = _paths(two)
    record("2_ssucsp0021_query", two, {
        "backend": "policy:skills/backend/SKILL.md" in p2,
        "sql": "policy:skills/sql/SKILL.md" in p2,
        "exact_contract": any("SSUC383_CURRENT.md" in path for path in p2),
        "no_ui_map": not any("ui_map" in path for path in p2),
        "no_unrelated_modules": not any("cap/" in path or "itn/" in path for path in p2),
    })

    req3 = "itnui의 기존 Grid 컴포넌트 표시 오류 수정"
    three = build_context_pack(role="WORKER", requirement=req3, target_modules=["itnui"], task_mode="MODIFICATION", **common)
    p3 = _paths(three)
    record("3_itnui_grid", three, {
        "frontend": "policy:skills/frontend/SKILL.md" in p3,
        "grid_reference": "custom Grid" in three.prompt,
        "itnui_fact": "module.itnui" in three.prompt,
        "no_capui_or_sucui_resource": not any("ui_map/cap" in path or "ui_map/suc" in path for path in p3),
    }, _legacy_static_chars(profile, policy, req3, ["itnui"]))

    req4 = "suc/src/main/webapp/WEB-INF/web.xml session 변경 후보를 분석"
    four = build_context_pack(role="PLANNER", requirement=req4, target_modules=["suc"], task_mode="MODIFICATION", **common)
    p4 = _paths(four)
    record("4_webxml_session", four, {
        "architecture_rule": "profile:hooks/architecture.md" in p4,
        "egov_rule": "profile:hooks/egov.md" in p4,
        "protected_risk": "PROTECTED_PATH" in four.task_spec.risk_tags,
        "human_gate": "E4U.BUSINESS.UNKNOWN_REQUIRES_HUMAN" in _rule_ids(four),
        "no_auto_authority": "MODIFICATION_EXECUTION_AUTHORIZATION" not in four.prompt,
    })

    req5 = "SsucSp0021 현재 구조를 분석만 하고 파일을 수정하지 않는다"
    five = build_context_pack(role="WORKER", requirement=req5, target_modules=["suc"], task_mode="ANALYSIS", **common)
    record("5_analysis_only", five, {
        "project_context": "policy:skills/project-context/SKILL.md" in _paths(five),
        "analysis_intent": five.task_spec.intent == "ANALYSIS",
        "read_only_constraint_preserved": "수정하지 않는다" in five.prompt,
        "no_modification_authorization": "승인된 MODIFICATION Job" not in five.prompt,
        "no_source_delta_protocol": "source delta" not in five.prompt.casefold(),
    })

    req6 = "suc 백엔드 서비스 동작 수정"
    initial = build_context_pack(role="WORKER", requirement=req6, target_modules=["suc"], task_mode="MODIFICATION", **common)
    changed = ["suc/src/main/java/suc/sp/query/SsucSp0021Qry.xml"]
    refreshed = build_context_pack(role="WORKER", requirement=req6, target_modules=["suc"], changed_files=changed, task_mode="MODIFICATION", **common)
    record("6_changed_query_refresh", refreshed, {
        "initial_backend": "policy:skills/backend/SKILL.md" in _paths(initial),
        "initial_not_sql": "policy:skills/sql/SKILL.md" not in _paths(initial),
        "refresh_backend": "policy:skills/backend/SKILL.md" in _paths(refreshed),
        "refresh_sql": "policy:skills/sql/SKILL.md" in _paths(refreshed),
        "pack_changed": initial.manifest["context_pack_id"] != refreshed.manifest["context_pack_id"],
    })

    req7 = "U03 변경을 구조화된 Build/Test 증거와 함께 리뷰"
    seven = build_context_pack(
        role="REVIEWER", requirement=req7, target_modules=["suc"], changed_files=changed,
        task_mode="MODIFICATION", task_evidence={
            "changed_files": changed,
            "task_owned_changed_files": changed,
            "build_evidence": {"status": "PASS", "sha256": "a" * 64},
            "test_status": "PASS",
        }, **common
    )
    p7 = _paths(seven)
    record("7_reviewer", seven, {
        "review_skill": "policy:skills/review/SKILL.md" in p7,
        "actual_files": changed[0] in seven.prompt,
        "structured_evidence": "build_evidence" in seven.prompt,
        "hard_rules": MANDATORY_RULE_IDS <= _rule_ids(seven),
        "no_worker_patch_skill": "policy:skills/backend/SKILL.md" not in p7 and "policy:skills/sql/SKILL.md" not in p7,
        "no_planning_skill": "policy:skills/planning/SKILL.md" not in p7,
    })

    req8 = "ALREADY_SATISFIED 주장 검증: suc/src/main/java/suc/sp/query/SsucSp0021Qry.xml을 직접 확인"
    eight = build_context_pack(role="REVIEWER", requirement=req8, target_modules=["suc"], changed_files=[], task_mode="MODIFICATION", **common)
    record("8_already_satisfied", eight, {
        "exact_target": "SsucSp0021Qry.xml" in eight.prompt,
        "verification_rule": "COMMON.VERIFY.EVIDENCE_REQUIRED" in _rule_ids(eight),
        "claim_not_authority": "E4U.BUSINESS.UNKNOWN_REQUIRES_HUMAN" in _rule_ids(eight),
    })

    simple_reductions: list[float] = []
    for simple_requirement in (
        req1,
        "sucui 버튼 라벨만 수정",
        "sucui 도움말 문구만 수정",
    ):
        simple_pack = build_context_pack(
            role="WORKER", requirement=simple_requirement,
            target_modules=["sucui"], task_mode="MODIFICATION", **common
        )
        before = _legacy_static_chars(profile, policy, simple_requirement, ["sucui"])
        simple_reductions.append(
            round((1 - simple_pack.manifest["total_chars"] / before) * 100, 2)
        )
    mandatory_recalls = [
        len(MANDATORY_RULE_IDS & _rule_ids(pack)) / len(MANDATORY_RULE_IDS)
        for pack in (one, two, three, four, five, initial, refreshed, seven, eight)
    ]
    summary = {
        "cases": len(cases),
        "passed": sum(1 for item in cases if item["passed"]),
        "mandatory_rule_recall_percent": round(min(mandatory_recalls) * 100, 2),
        "simple_static_prompt_median_reduction_percent": round(statistics.median(simple_reductions), 2),
        "unrelated_context_failures": sum(
            1 for item in cases for name, passed in item["checks"].items()
            if name.startswith("no_") and not passed
        ),
        "simple_static_prompt_reductions_percent": simple_reductions,
    }
    return {
        "schema_version": 1,
        "profile_id": profile.id,
        "model_calls": 0,
        "summary": summary,
        "cases": cases,
        "passed": (
            summary["passed"] == len(cases)
            and summary["mandatory_rule_recall_percent"] == 100.0
            and summary["simple_static_prompt_median_reduction_percent"] >= 30.0
            and summary["unrelated_context_failures"] == 0
        ),
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile-dir", default=r"D:\agents\profiles\e4u")
    parser.add_argument("--workspace", default=r"D:\inha_dev")
    parser.add_argument("--policy-root", default=r"D:\agents")
    parser.add_argument("--output")
    args = parser.parse_args(argv)
    result = run_golden_context_eval(args.profile_dir, args.workspace, args.policy_root)
    body = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output:
        destination = Path(args.output)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(body, encoding="utf-8")
    print(body)
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
