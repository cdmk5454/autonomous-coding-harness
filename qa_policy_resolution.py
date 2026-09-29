"""The approved DB-connect/E2E waiver, not candidate or implementation acceptance."""
POLICY = {"DB_CONNECT": "NOT_RUN_BY_POLICY", "E2E": "NOT_RUN_BY_POLICY"}
CONTRACT = """[Authoritative candidate-less QA policy resolution]
DB_CONNECT = NOT_RUN_BY_POLICY
E2E = NOT_RUN_BY_POLICY
Do not attempt DB connections (including alternate JDBC) or E2E. Live DB counts/set
equality and delivery are deferred, not actual PASS. Connection failure or absence
of this evidence must not stop implementation or trigger product retry.
Use available static source/profile evidence; never invent columns or mappings.
Build, non-connecting mandatory tests, Review and Verification remain required.
No real SMS/email/notification sends. If reporting a hold solely for exempt gates,
include blocked_gates: ["DB_CONNECT", "E2E"] in HARNESS_QA_REQUEST_JSON; genuine
business/schema ambiguity is not an exempt gate.
[/Authoritative candidate-less QA policy resolution]"""


SKELETON_POLICY = {**POLICY, 'DEVELOPMENT': 'SKELETON_READY_WITH_DEFERRED_CONTRACTS'}


def resolution_contract(policy):
    validate_resolution(policy)
    if policy == SKELETON_POLICY:
        from skeleton_policy import CONTRACT as skeleton_contract
        return CONTRACT + '\n\n' + skeleton_contract
    return CONTRACT


def validate_resolution(policy):
    if policy not in (POLICY, SKELETON_POLICY):
        raise ValueError("QA_POLICY_RESOLUTION_INVALID")


def exempt_qa_signal(task):
    """Only a frozen, explicitly gate-labelled waiver can suppress a QA signal."""
    frozen = dict(task.materialized_execution or {})
    qa = dict(task.qa_request or {})
    gates = qa.get("blocked_gates")
    return bool(frozen.get("current_requirement") == task.requirement
        and CONTRACT in task.requirement
        and qa.get("required") is True
        and qa.get("qa_type") in {"DB_CONTRACT", "HUMAN_ACCEPTANCE"}
        and qa.get("decision_independent_work_complete") is True
        and isinstance(gates, list) and gates
        and all(gate in POLICY for gate in gates))
