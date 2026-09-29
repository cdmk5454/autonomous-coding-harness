"""Detached-in-process read/proposal boundary. No scheduler or mutation authority.

Pure policy/projection helpers preserve the first-pass API. Versioned enforcement
is called by HarnessService/Manager. No scheduler, Product writer or import-time model.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Callable, Mapping

READ_OPERATIONS = frozenset({"get_job", "get_queue", "get_status", "get_report"})
EXCEPTIONS = frozenset({"AMBIGUITY", "DRIFT", "RECOVERY", "PUBLICATION_ANOMALY",
                        "CROSS_JOB_AMBIGUITY", "BUSINESS_DECISION", "SECURITY_APPROVAL"})

def invocation_plan(risk: str, *, prepared: bool, boundary: str = "NORMAL", exception: str = "",
                    required: bool = False):
    if (risk not in {"LOW", "MID", "HIGH", "CRITICAL"}
            or type(prepared) is not bool or type(required) is not bool):
        raise ValueError("SUPERVISOR_INPUT_INVALID")
    if exception and exception not in EXCEPTIONS:
        raise ValueError("SUPERVISOR_EXCEPTION_UNKNOWN")
    if boundary not in {"NORMAL", "ADMISSION", "SEMANTIC_RISK"}:
        raise ValueError("SUPERVISOR_BOUNDARY_INVALID")
    needed = required or bool(exception) or risk in {"HIGH", "CRITICAL"}
    return {"kind": "INVOCATION_POLICY_PROPOSAL", "planner": "OFF" if prepared else "PLAN_PROPOSAL",
            "supervisor_required": needed, "human_gate_required": risk == "CRITICAL" or exception == "SECURITY_APPROVAL",
            "reason": exception or ("CONTRACT_REQUIRED" if required else risk + ":" + boundary),
            "enforced_by_runtime": False}

@dataclass(frozen=True)
class SupervisorReadBridge:
    """Call only a fixed read capability after binding to a fresh projection.

    The adapter supplying `read` must map operation names to existing Harness
    read APIs. It must not interpret arguments as shell, SQL, or arbitrary tools.
    """
    read: Callable[[str, Mapping[str, Any]], Any]

    def invoke(self, operation: str, arguments: Mapping[str, Any]):
        if operation not in READ_OPERATIONS:
            raise ValueError("SUPERVISOR_OPERATION_DENIED")
        expected = {"job_id"} if operation in {"get_job", "get_report"} else set()
        if set(arguments) != expected or any(not isinstance(x, str) or not x.strip() for x in arguments.values()):
            raise ValueError("SUPERVISOR_ARGUMENTS_INVALID")
        return self.read(operation, dict(arguments))


def validate_recommendation(value: Mapping[str, Any], current: Mapping[str, Any]):
    required = {"kind", "job_id", "execution_id", "revision", "recommended_action", "reason", "evidence_refs"}
    if set(value) != required or value.get("kind") != "SUPERVISOR_RECOMMENDATION":
        raise ValueError("SUPERVISOR_RECOMMENDATION_INVALID")
    if (type(value.get("revision")) is not int or not value.get("job_id") or not value.get("execution_id")
            or any(value.get(key) != current.get(key) for key in ("job_id", "execution_id", "revision"))):
        raise ValueError("SUPERVISOR_STALE_RECOMMENDATION")
    legal = current.get("legal_next_actions")
    if not isinstance(legal, list) or value.get("recommended_action") not in legal:
        raise ValueError("SUPERVISOR_ACTION_NOT_ADMISSIBLE")
    refs = value.get("evidence_refs")
    if not isinstance(refs, list) or not refs or any(not isinstance(x, str) or not x for x in refs):
        raise ValueError("SUPERVISOR_EVIDENCE_REQUIRED")
    if not isinstance(value.get("reason"), str) or not value["reason"].strip():
        raise ValueError("SUPERVISOR_REASON_REQUIRED")
    return {**dict(value), "evidence_refs": list(refs), "executable": False,
            "authority": "PROPOSAL_ONLY_REVALIDATE_THROUGH_HARNESS"}

# The projection helpers above remain compatible with first-pass consumers.
# The versioned boundary below is the actual Harness enforcement path.
POLICY_VERSION = 'supervisor-boundary/1.0x-1'
_RANK = {'LOW': 0, 'MID': 1, 'HIGH': 2, 'CRITICAL': 3}

class SupervisorGateError(RuntimeError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)

@dataclass(frozen=True)
class HermesSupervisorAdvisor:
    """Narrow source-level provider seam. The injected transport is trusted code.

    Transport receives JSON only; no repository, Product path, Writer factory or
    operation callable. It must enforce the declared no-tools execution boundary
    locally. This class validates protocol/identity, not OS or provider isolation.
    """
    invoke_json: Callable[[str], str]
    provider: str
    model: str
    runtime_version: str
    adapter_revision: str

    def identity(self):
        from control_repository import digest
        from runtime_safety import scrub_secrets
        values = (self.provider, self.model, self.runtime_version, self.adapter_revision)
        if any(not isinstance(v, str) or not v.strip() or len(v)>160 or scrub_secrets(v)!=v for v in values):
            raise SupervisorGateError('SUPERVISOR_IDENTITY_REQUIRED')
        identity = dict(runtime='hermes', provider=self.provider, model=self.model,
                        runtime_version=self.runtime_version, adapter_revision=self.adapter_revision,
                        authority='ADVISORY_ONLY', allowed_tools=[], sandbox_assurance='LOCAL_VERIFICATION_REQUIRED')
        return {**identity, 'identity_sha256': digest(identity)}

    def recommend(self, projection):
        import json
        from control_repository import canonical
        before = self.identity()
        try:
            raw = self.invoke_json(canonical({'schema': POLICY_VERSION, 'projection': projection,
                'runtime_identity': before, 'allowed_tools': [], 'product_write': False,
                'control_write': False, 'publication': False, 'subagents': False}))
        except Exception as exc:
            code = getattr(exc, 'code', '')
            if code in {'SUPERVISOR_TRANSPORT_DECODE_FAILED', 'SUPERVISOR_TRANSPORT_TIMEOUT',
                        'SUPERVISOR_TRANSPORT_OUTPUT_TOO_LARGE', 'SUPERVISOR_PROVIDER_UNAVAILABLE',
                        'SUPERVISOR_RUNTIME_UNAVAILABLE', 'SUPERVISOR_RUNTIME_IDENTITY_DRIFT',
                        'SUPERVISOR_RESPONSE_INVALID', 'SUPERVISOR_SESSION_ID_MISMATCH'}:
                raise SupervisorGateError(code) from None
            raise SupervisorGateError('SUPERVISOR_EXECUTION_UNAVAILABLE') from None
        if not isinstance(raw, str) or len(raw.encode('utf8')) > 32768:
            raise SupervisorGateError('SUPERVISOR_RESPONSE_INVALID')
        try:
            value = json.loads(raw)
        except (ValueError, TypeError):
            raise SupervisorGateError('SUPERVISOR_RESPONSE_INVALID') from None
        if self.identity() != before:
            raise SupervisorGateError('SUPERVISOR_IDENTITY_DRIFT')
        return value


def frozen_policy(effective_policy, *, prepared=True):
    # This function consumes existing Review policy; it never recalculates risk.
    risk = (effective_policy.get('resolved_reviewer') or {}).get('risk_level')
    if risk not in _RANK or type(prepared) is not bool:
        raise SupervisorGateError('SUPERVISOR_RISK_AUTHORITY_REQUIRED')
    return {'policy_version': POLICY_VERSION, 'risk_level': risk, 'prepared_contract': prepared,
            'risk_authority': 'FROZEN_RESOLVED_REVIEWER', 'planner': 'OFF' if prepared else 'PROPOSAL_ONLY',
            'supervisor_required': 'SUPERVISOR_REQUIRED' in effective_policy.get('policy_overlays', ()),
            'control_authority': 'HARNESS', 'product_authority': False}


def _validate(value, projection):
    from runtime_safety import scrub_secrets
    required = {'policy_version','request_sha256','recommended_action','reason','evidence_refs','session_id'}
    if (not isinstance(value, dict) or set(value)!=required or value.get('policy_version')!=POLICY_VERSION
            or value.get('request_sha256')!=projection['request_sha256']):
        raise SupervisorGateError('SUPERVISOR_STALE_OR_INVALID_RECOMMENDATION')
    if value['recommended_action'] not in projection['legal_next_actions']:
        raise SupervisorGateError('SUPERVISOR_ACTION_DENIED')
    refs = value['evidence_refs']
    if (not isinstance(refs,list) or not refs or len(refs)>32
            or any(not isinstance(ref,str) or ref not in projection['evidence_refs'] for ref in refs)):
        raise SupervisorGateError('SUPERVISOR_EVIDENCE_UNBOUND')
    for field, limit in (('reason',2000),('session_id',160)):
        if (not isinstance(value[field],str) or not value[field].strip() or len(value[field])>limit
                or scrub_secrets(value[field])!=value[field]):
            raise SupervisorGateError('SUPERVISOR_RESPONSE_INVALID')
    return dict(value)


def enforce(repository, job, *, policy, context, advisor, boundary='ADMISSION',
            execution=None, observed_risk='', exception='', evidence=None):
    """Enforce once per bound boundary; model latency never holds a DB transaction.

    Cache keys bind exact revision/epoch/contract/context/candidate/evidence/runtime.
    Held/missing recommendations cannot consume a Product retry or release a writer.
    Legacy executions without this frozen policy retain their previous behavior.
    """
    import json
    from control_repository import canonical, digest
    from control_epoch import current, assert_execution
    frozen = policy.get('supervisor_policy')
    if frozen is None:
        return {'disposition':'LEGACY_NOT_OPTED_IN', 'model_calls':0, 'policy_enforced':False}
    if frozen.get('policy_version')!=POLICY_VERSION:
        raise SupervisorGateError('SUPERVISOR_POLICY_UNSUPPORTED')
    original_risk = (policy.get('resolved_reviewer') or {}).get('risk_level')
    if original_risk != frozen.get('risk_level') or original_risk not in _RANK:
        raise SupervisorGateError('SUPERVISOR_RISK_AUTHORITY_MISMATCH')
    if observed_risk and observed_risk not in _RANK:
        raise SupervisorGateError('SUPERVISOR_RISK_AUTHORITY_REQUIRED')
    risk = max((original_risk, observed_risk or original_risk), key=_RANK.__getitem__)
    plan = invocation_plan(risk, prepared=frozen['prepared_contract'], boundary=boundary,
                           exception=exception, required=frozen.get('supervisor_required', False))
    if not plan['supervisor_required']:
        return {'disposition':'NOT_REQUIRED','model_calls':0,'policy_enforced':True,'risk_level':risk}
    if not isinstance(advisor, HermesSupervisorAdvisor):
        raise SupervisorGateError('SUPERVISOR_BINDING_REQUIRED')
    runtime = advisor.identity()
    def snapshot():
        with repository.hold():
            latest = repository.read_job(job['job_id'])
            if (latest.get('revision')!=job.get('revision') or latest.get('contract_revision',1)!=job.get('contract_revision',1)
                    or latest.get('request')!=job.get('request')):
                raise SupervisorGateError('SUPERVISOR_STALE_REQUEST')
            if execution:
                assert_execution(repository,execution['execution_id'])
                if repository.execution(execution['execution_id'])!=execution:
                    raise SupervisorGateError('SUPERVISOR_EXECUTION_MISMATCH')
            return {'job_id':latest['job_id'],'job_revision':latest['revision'],
                    'contract_revision':latest.get('contract_revision',1),
                    'contract_sha256':digest(latest.get('request') or {}),
                    'execution_id':execution['execution_id'] if execution else 'UNMATERIALIZED',
                    'candidate_id':str((evidence or {}).get('candidate_id') or latest.get('candidate_id','')), 'control_epoch':current(repository._connection),
                    'context_sha256':digest(context), 'policy_sha256':digest(policy),
                    'evidence_sha256':digest(evidence or {}), 'runtime_identity_sha256':runtime['identity_sha256'],
                    'risk_level':risk,'boundary':boundary,'exception':exception}
    binding = snapshot()
    refs = ['contract:'+binding['contract_sha256'], 'context:'+binding['context_sha256'],
            'evidence:'+binding['evidence_sha256']]
    from runtime_safety import scrub_secrets
    projection = {**binding,'request_sha256':digest(binding),'evidence_refs':refs,
        'legal_next_actions':['CONTINUE','HOLD','REQUEST_HUMAN'],
        'context':{'requirement':scrub_secrets(str((job.get('request') or {}).get('requirement','')))[:4000],
                   'evidence_summary':scrub_secrets(canonical(evidence or {}))[:4000]},
        'stronger_human_gate_required':plan['human_gate_required']}
    key = 'supervisor-recommendation/1/'+projection['request_sha256']
    with repository.hold():
        row=repository._connection.execute('SELECT payload FROM control_state WHERE key=?',(key,)).fetchone()
    cached=json.loads(row[0]) if row else None
    value=_validate(cached['recommendation'] if cached else advisor.recommend(projection),projection)
    if snapshot()!=binding:
        raise SupervisorGateError('SUPERVISOR_STALE_REQUEST')
    # The Harness records recommendations but does not execute proposed operations.
    human_gate_blocked = False
    with repository.hold():
        if snapshot()!=binding:
            raise SupervisorGateError('SUPERVISOR_STALE_REQUEST')
        record={'binding':binding,'runtime_identity':runtime,'recommendation':value,
                'authority':'ADVISORY_ONLY','product_mutated':False}
        prior=repository._connection.execute('SELECT payload FROM control_state WHERE key=?',(key,)).fetchone()
        if prior and json.loads(prior[0])!=record:
            raise SupervisorGateError('SUPERVISOR_RECOMMENDATION_CONFLICT')
        repository._connection.execute('INSERT OR IGNORE INTO control_state VALUES(?,?,?)',(key,1,canonical(record)))
        if plan['human_gate_required'] or value['recommended_action']=='REQUEST_HUMAN':
            authorization_binding={k:v for k,v in binding.items() if k!='job_revision'}
            fingerprint=digest({'supervisor_human_gate':authorization_binding})
            decision=repository.record_decision(job['job_id'],contract_revision=job.get('contract_revision',1),
                decision_type='POLICY',question='Explicit human/security authorization required for bound Supervisor request',
                fingerprint=fingerprint,payload={'qa_type':'SECURITY_APPROVAL','supervisor_request_sha256':projection['request_sha256'],
                                                'candidate_id':binding['candidate_id'],
                                                'supervisor_binding':authorization_binding})
            if decision.get('status')!='RESOLVED' or decision.get('resolution')!='HUMAN_ACCEPT':
                human_gate_blocked = True
    if human_gate_blocked:
        raise SupervisorGateError('SUPERVISOR_HUMAN_GATE_REQUIRED')
    if value['recommended_action']!='CONTINUE':
        raise SupervisorGateError('SUPERVISOR_HOLD_REQUIRED')
    return {'disposition':'CONTINUE','model_calls':0 if cached else 1, 'policy_enforced':True,
            'request_sha256':projection['request_sha256'], 'risk_level':risk,
            'recommendation_ref':key,'authority':'HARNESS_POLICY_NOT_MODEL_SUCCESS'}
