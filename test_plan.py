"""Required tests plus a bounded conventional test envelope at materialization."""
from pathlib import Path, PurePosixPath
import re
from control_repository import digest

# Typed deterministic review-oracle contracts. A plan may declare that an
# explicitly approved deterministic oracle (required, frozen-scope tests run by
# the official tester) covers this acceptance; the Manager still evaluates all
# binding/freshness conditions before skipping the separate LLM Reviewer.
REVIEW_ORACLE_DETERMINISTIC = "DETERMINISTIC_ORACLE"
REVIEW_ORACLES = frozenset({REVIEW_ORACLE_DETERMINISTIC})


def validate_test_contract(value):
    if not isinstance(value, dict) or set(value) - {"required_tests", "command", "capability", "mandatory", "verification_depth", "review_oracle", "browser_qa", "evaluation", "test_commands"}:
        raise ValueError("TEST_PLAN_INVALID")
    if "mandatory" in value and not isinstance(value["mandatory"], bool):
        raise ValueError("TEST_PLAN_INVALID")
    # 0.99.2.1 additive browser QA declaration. Absent/empty -> qa_required
    # false and the pre-existing 0.99.2 path is completely unchanged.
    browser_qa = value.get("browser_qa")
    if browser_qa is not None:
        if not isinstance(browser_qa, dict) or set(browser_qa) - {
            "required", "scenario_id", "title", "acceptance", "allowed_actions",
            "forbidden_actions", "side_effect_class", "target_profile",
            "start_url", "steps",
        }:
            raise ValueError("BROWSER_QA_DECLARATION_INVALID")
        if browser_qa.get("required") is not True:
            raise ValueError("BROWSER_QA_DECLARATION_INVALID")
        from qa_browser_contract import QAScenario
        QAScenario.from_declaration(browser_qa)
    oracle = str(value.get("review_oracle", "") or "").upper()
    if oracle and oracle not in REVIEW_ORACLES:
        raise ValueError("REVIEW_ORACLE_INVALID")
    if oracle == REVIEW_ORACLE_DETERMINISTIC and value.get("mandatory") is not True:
        raise ValueError("REVIEW_ORACLE_REQUIRES_MANDATORY_TESTS")
    tests = value.get("required_tests", [])
    if not isinstance(tests, list) or any(not isinstance(p, str) or not p.strip() for p in tests):
        raise ValueError("TEST_PLAN_INVALID")
    for name in tests:
        path = PurePosixPath(name.replace("\\", "/"))
        if path.is_absolute() or ".." in path.parts or ":" in name:
            raise ValueError("TEST_SCOPE_PATH_INVALID")
    command = value.get("command", "")
    if not isinstance(command, str) or len(command) > 4000:
        raise ValueError("TEST_PLAN_INVALID")
    capability = value.get("capability", "OFFICIAL_TESTER_READ_ONLY")
    if capability != "OFFICIAL_TESTER_READ_ONLY":
        raise ValueError("TEST_CAPABILITY_UNAVAILABLE")
    depth = str(value.get("verification_depth") or "").upper()
    if depth and depth not in {"STATIC", "TARGETED", "E2E", "HUMAN_CONTROLLED_E2E"}:
        raise ValueError("VERIFICATION_DEPTH_INVALID")
    from deterministic_test_policy import validate_declaration
    validate_declaration(value)
    return dict(value)


def derive_test_plan(request, workspace):
    root = Path(workspace).resolve()
    declared = validate_test_contract(request.test_plan)
    required = set(declared.get("required_tests", []))
    # Explicit source paths in the durable requirement are evidence, never commands.
    required.update(re.findall(r"[A-Za-z0-9_./-]+(?:Test\.java|_test\.py|\.test\.[jt]sx?|\.spec\.[jt]sx?)", request.requirement))
    production = sorted(set(request.target_resources or request.target_modules))
    derived = set()
    for relative in production:
        source = root / relative
        if not source.resolve().is_relative_to(root):
            raise ValueError("TEST_SCOPE_PATH_INVALID")
        normalized = relative.replace("\\", "/")
        if "/src/main/java/" in normalized:
            test = normalized.replace("/src/main/java/", "/src/test/java/")
            conventional = str(PurePosixPath(test).with_suffix("")) + "Test.java"
            derived.add(conventional)
            parent = (root / test).parent
            stem = re.sub(r"(?:Svc|Service|Qry|Controller)$", "", source.stem)
            if parent.is_dir():
                derived.update(p.relative_to(root).as_posix() for p in parent.glob(stem + "*Test.java") if p.is_file())
        elif source.suffix == ".py":
            derived.add((PurePosixPath(relative).parent / ("test_" + source.name)).as_posix())
        elif source.suffix in {".js", ".jsx", ".ts", ".tsx", ".vue"}:
            for suffix in (".test.js", ".test.ts", ".spec.js", ".spec.ts"):
                related = source.with_name(source.stem + suffix)
                if related.is_file():
                    derived.add(related.relative_to(root).as_posix())
    for relative in required | derived:
        if not (root / relative).resolve().is_relative_to(root):
            raise ValueError("TEST_SCOPE_PATH_INVALID")
    plan = {"schema_version": 1, "production_scope": production,
            "required_tests": sorted(required), "derived_tests": sorted(derived - required),
            "command": declared.get("command", ""), "capability": "OFFICIAL_TESTER_READ_ONLY",
            "mandatory": "ANALYSIS_READONLY" not in request.policy_overlays or declared.get("mandatory", False),
            "verification_depth": str(declared.get("verification_depth") or "").upper(),
            "review_oracle": str(declared.get("review_oracle", "") or "").upper()}
    browser_qa = declared.get("browser_qa")
    if isinstance(browser_qa, dict) and browser_qa.get("required") is True:
        # The frozen canonical scenario rides with the frozen test plan; the
        # scenario hash (not this field) is the binding authority.
        plan["browser_qa"] = dict(browser_qa)
    plan["scope_hash"] = digest({key: plan[key] for key in ("production_scope", "required_tests", "derived_tests", "command", "capability", "mandatory", "verification_depth", "review_oracle")})
    if "evaluation" in declared:
        # New, explicit policy boundary. Old plan hashes remain byte-compatible.
        plan["evaluation"] = declared["evaluation"]
        plan["test_commands"] = declared["test_commands"]
        from deterministic_test_policy import validate_declaration
        validate_declaration(plan)
        plan["scope_hash"] = digest({"legacy_scope_hash": plan["scope_hash"],
                                     "evaluation": plan["evaluation"],
                                     "test_commands": plan["test_commands"]})
    plan["test_scope_hash"] = plan["scope_hash"]
    return plan
