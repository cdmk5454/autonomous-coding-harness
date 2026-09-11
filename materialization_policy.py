"""Small, explicit routing constraints; no policy language or runtime adapters."""
from policy_catalog import effective_policy_hash
from review_policy import assess_review_risk

TIERS = {"LOW": 1, "MID": 2, "HIGH": 3}
MODEL_TIERS = {"gpt-5.6-luna": "LOW", "gpt-5.6-terra": "MID", "gpt-5.6-sol": "HIGH"}
TIER_MODELS = {value: key for key, value in MODEL_TIERS.items()}
MODEL_TIERS["gpt-6-astra"] = "HIGH"


class PolicyConflict(ValueError):
    code = "POLICY_RESOLUTION_CONFLICT"


def validate_constraints(value):
    if not isinstance(value, dict) or set(value) - {"required", "preferred", "max", "exact"}:
        raise PolicyConflict("unsupported policy constraint")
    for key in ("required", "preferred", "max"):
        if key in value and value[key] not in TIERS:
            raise PolicyConflict("invalid tier: " + key)
    if value.get("exact") and value["exact"] not in MODEL_TIERS:
        raise PolicyConflict("exact model unavailable")
    return dict(value)


def resolve_constraints(value, *, safety_minimum="LOW", default_model="gpt-5.6-sol"):
    value = validate_constraints(value)
    required = max(TIERS[value.get("required", "LOW")], TIERS[safety_minimum])
    maximum = TIERS[value.get("max", "HIGH")]
    preferred = TIERS[value.get("preferred", MODEL_TIERS.get(default_model, "HIGH"))]
    exact = value.get("exact", "")
    if exact:
        selected = TIERS[MODEL_TIERS[exact]]
        if not required <= selected <= maximum:
            raise PolicyConflict("exact does not satisfy required/max")
    elif maximum < required:
        raise PolicyConflict("max below safety/required minimum")
    else:
        selected = max(required, preferred)
        if selected > maximum:
            raise PolicyConflict("preferred exceeds max")
    tier = next(name for name, rank in TIERS.items() if rank == selected)
    return {"required": next(name for name, rank in TIERS.items() if rank == required),
            "preferred": value.get("preferred", MODEL_TIERS.get(default_model, "HIGH")),
            "max": value.get("max", "HIGH"), "exact": exact,
            "tier": tier, "model": exact or (
                default_model if default_model == "gpt-6-astra" and tier == "HIGH"
                else TIER_MODELS[tier]),
            "action": "AUTO_UPGRADE" if selected > preferred else "RESOLVED"}


def freeze_routing(request, policy):
    risk = assess_review_risk(requirement=request.requirement, target_modules=request.target_modules,
        changed_files=request.target_resources, git_diff="", build_status="PASS", test_status="PASS",
        test_required=True, policy_overlays=request.policy_overlays)
    minimum = "HIGH" if risk.risk_level in {"HIGH", "CRITICAL"} else "LOW"
    routing = resolve_constraints(request.execution_policy, safety_minimum=minimum,
                                  default_model=request.model)
    if request.worker != "codex":
        if request.execution_policy:
            raise PolicyConflict("tier constraints require a registered Codex model")
        routing["model"] = request.model
    policy["resolved_worker"] = {**routing, "worker": request.worker, "reasoning_effort": request.reasoning_effort}
    policy["resolved_reviewer"] = risk.to_dict()
    policy["effective_policy_sha256"] = effective_policy_hash(policy)
    return policy
