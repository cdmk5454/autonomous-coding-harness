"""Explicit first-pass development policy; never a final acceptance waiver."""
SKELETON_READY = 'SKELETON_READY'
CONTRACT = """[Approved deferred-contract skeleton development]
EXECUTION_SCOPE = SKELETON_DEVELOPMENT
DB_CONNECT = NOT_RUN_BY_POLICY
E2E = NOT_RUN_BY_POLICY
REAL_EXTERNAL_SEND = FORBIDDEN
This newer policy supersedes earlier instructions to stop all implementation for
missing DB/business decisions. Implement only decision-independent, safe code,
UI, service, validation and interfaces. Never invent a physical column, type,
domain, source mapping or business decision. Keep unresolved writes disabled
explicitly; do not implement a guessed fallback or claim a fake completed flow.
Build, mandatory non-DB tests and independent Review remain required. Review the
safe skeleton against this scope, not final DB delivery. No DB connections,
alternate JDBC, E2E, or real SMS/email/notification calls are permitted.
Record every unresolved contract in HARNESS_QA_REQUEST_JSON with required=true,
the existing qa_type/hold_scope/reason/evidence/completion fields, plus
skeleton_safe=true only if the unresolved boundary is safely isolated, and
deferred_contracts=[{"decision":"...","candidates":"...","evidence":"...",
"areas":"files/areas to finalize","verification_required":"DB/E2E/integration"}].
Use decision_independent_work_complete=true only after safe skeleton work is done.
If the safe skeleton is already implemented, avoid artificial edits and use the
existing "NO_CHANGE_REASON: ALREADY_SATISFIED" current-source verification protocol as well.
Destructive, security/PII, irreversible or non-isolatable ambiguity must instead
use skeleton_safe=false and stop. Do not clear a real security/product finding.
If no further business contract is missing, still report the deferred final
DB/E2E/integration verification as an item; this execution is never STRICT_SUCCESS.
For J04/J05, COURSE_CD storage column, type/domain, and CURI_GBN -> COURSE_CD
mapping remain DEFERRED. Preserve established academic-year/date/SEQ/reset scope.
J06 implements communication flow without real delivery; deliverability is deferred.
Only fresh finalization after authoritative decisions and final DB/E2E/integration
verification can earn STRICT_SUCCESS. This run can earn SKELETON_READY only.
[/Approved deferred-contract skeleton development]"""


def enabled(task):
    frozen = dict(task.materialized_execution or {})
    return bool(CONTRACT in task.requirement
                and frozen.get('current_requirement') == task.requirement)


def deferred_signal(task):
    qa = task.qa_request
    return bool(enabled(task) and qa.get('required') is True
                and qa.get('qa_type') in {'DB_CONTRACT', 'BUSINESS_CONTRACT', 'API_CONTRACT', 'HUMAN_ACCEPTANCE'}
                and qa.get('skeleton_safe') is True
                and qa.get('decision_independent_work_complete') is True
                and qa.get('deferred_contracts'))


def valid_result(result):
    frozen = result.get('materialized_execution') or {}
    gates = result.get('gate_evidence') or {}
    qa = result.get('qa_request') or {}
    return bool(result.get('status') == SKELETON_READY
        and result.get('success') is False and result.get('stage') == 'DONE'
        and CONTRACT in str(frozen.get('current_requirement', ''))
        and frozen.get('execution_id') and result.get('task_id')
        and result.get('verification_status') == 'PARTIAL'
        and result.get('build_status') == 'PASS'
        and result.get('test_status') in {'PASS', 'NOT_REQUIRED'}
        and result.get('review_status') == 'REVIEW_PASS'
        and result.get('failure_code') == 'DEFERRED_CONTRACT'
        and result.get('external_frozen_integrity') is True
        and result.get('baseline_declaration_integrity') is True
        and not result.get('unexpected_external_dirty_files')
        and not result.get('commit_blockers')
        and not result.get('secondary_failures')
        and qa.get('skeleton_safe') is True and qa.get('deferred_contracts')
        and qa.get('decision_independent_work_complete') is True
        and all(gates.get(g, {}).get('status') == 'PASS'
                and gates[g].get('freshness') == 'CURRENT' for g in ('BUILD', 'TEST', 'REVIEW'))
        and gates.get('VERIFICATION', {}).get('status') == 'PARTIAL')
