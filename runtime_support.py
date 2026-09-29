"""Small operator-facing support table; not a registry, router or service."""
from copy import deepcopy

_TABLE = {
    "OpenCode": {"tier": "PRIMARY", "role": "WORKER", "activation": "EXISTING_EXPLICIT_ROUTING"},
    "Codex": {"tier": "SECONDARY", "role": "SPECIALIST_REVIEWER", "activation": "EXISTING_EXPLICIT_ROUTING"},
    "Hermes-native": {"tier": "CANDIDATE", "role": "SUPERVISOR_REVIEWER_MANAGED", "activation": "NOT_DEFAULT"},
    "Pi": {"tier": "CANDIDATE", "role": "WORKER", "activation": "DETACHED"},
    "OMP": {"tier": "DEFERRED", "role": "WORKER_OPTION", "activation": "NOT_IMPLEMENTED"},
    "CodeRabbit": {"tier": "DEFERRED", "role": "AUXILIARY_REVIEW_SIGNAL", "activation": "NOT_IMPLEMENTED"},
    "Droid": {"tier": "RETIRE", "role": "LEGACY_WORKER_PLANNER_TESTER", "activation": "LEGACY_DEPENDENCIES_REMAIN"},
    "Discord": {"tier": "PRIMARY", "role": "OPERATOR_SURFACE", "activation": "EXISTING_CONFIG"},
    "Telegram": {"tier": "RETIRE", "role": "OPERATOR_SURFACE", "activation": "LEGACY_CODE_RETAINED"},
    "Momentic": {"tier": "PROVISIONAL", "role": "BROWSER_QA_PRIMARY", "activation": "CONFIGURED_NOT_RUNTIME_VERIFIED"},
    "Stagehand": {"tier": "PROVISIONAL", "role": "BROWSER_QA_FALLBACK", "activation": "CONFIGURED_NOT_RUNTIME_VERIFIED"},
}

def support_matrix():
    return {"schema_revision": "runtime-support/1", "kind": "SUPPORT_POLICY_PROJECTION",
            "changes_routing": False, "entries": deepcopy(_TABLE)}


def execution_routes(execution=None, *, supervisor_advisor=None):
    """Policy tiers are not evidence of active routing or installed providers."""
    frozen = execution or {}
    contracts = (frozen.get('runtime_contracts') or {}).get('roles') or {}
    from runtime_identity import factory_identity
    return {'schema_revision': 'runtime-routing-projection/1',
            'basis': 'MATERIALIZED_EXECUTION' if frozen else 'NO_EXECUTION_SUPPLIED',
            'execution_id': frozen.get('execution_id', ''),
            'roles': deepcopy(contracts),
            'factory_identity': deepcopy(frozen.get('role_factory_identity') or {}),
            'supervisor_implementation': factory_identity(supervisor_advisor),
            'native_provider_verified': False, 'changes_routing': False,
            'note': 'Support tiers never select a Worker or prove runtime installation.'}


def native_validation_projection(operator_flag):
    """A configured admission flag is not current native/provider/canary evidence."""
    configured = operator_flag is True
    return {'live_model_call':'NOT_OBSERVED',
            'live_canary':'RUNTIME_VERIFICATION_REQUIRED' if configured else 'NOT_RUN',
            'live_validated':False, 'configured_live_validated':configured,
            'validation_evidence_source':'OPERATOR_CONFIGURATION_NOT_CURRENT_RUNTIME_RECEIPT'}
