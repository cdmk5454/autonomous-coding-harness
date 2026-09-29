"""P0 developer report: exact excerpts are extracted from actual source blobs."""
from pathlib import Path
import difflib
import hashlib
import json
import subprocess
from control_paths import atomic_state_write
from control_repository import canonical, digest
from gate_evidence import identity
from runtime_safety import scrub_secrets


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def capture(task, baseline, collector, workspace, output_root):
    root = Path(workspace).resolve()
    images = []
    for name in sorted(set(task.changed_files)):
        target = (root / name).resolve()
        if not target.is_relative_to(root):
            raise ValueError("REPORT_SOURCE_PATH_ESCAPE")
        located = collector._repo_path_for_changed(baseline, name)
        if located is None:
            raise ValueError("REPORT_INPUT_BLOB_UNAVAILABLE")
        repo, relative = located
        listing = subprocess.run(['git','-C',str(repo.git_dir),'ls-tree','-z',repo.worktree_tree,'--',relative],capture_output=True,check=True).stdout
        old = subprocess.run(['git','-C',str(repo.git_dir),'show',repo.worktree_tree+':'+relative],capture_output=True,check=True).stdout if listing else b''
        new = target.read_bytes() if target.is_file() else b''
        old_text, new_text = old.decode('utf-8'), new.decode('utf-8')
        if scrub_secrets(old_text) != old_text or scrub_secrets(new_text) != new_text:
            excerpts = [{"redacted": True, "reason": "Source contains a protected value; inspect the source locally."}]
        else:
            before, after = old_text.splitlines(), new_text.splitlines()
            excerpts = []
            for group in difflib.SequenceMatcher(a=before,b=after,autojunk=False).get_grouped_opcodes(3):
                a0,a1,b0,b1=group[0][1],group[-1][2],group[0][3],group[-1][4]
                excerpts.append({"as_is_start":a0+1,"to_be_start":b0+1,
                                 "as_is":"\n".join(before[a0:a1]),"to_be":"\n".join(after[b0:b1])})
        images.append({"path":name,"input_blob":repo.worktree_tree+':'+relative,
                       "as_is_sha256":sha(old),"to_be_sha256":sha(new),"excerpts":excerpts})
    binding = {"job_id":task.job_id,"contract_revision":task.materialized_execution.get('contract_revision',1),
               "job_input_hash":digest([{'module':repo.module,'worktree_tree':repo.worktree_tree} for repo in baseline.repos]),
               **identity(task,workspace)}
    payload={"binding":binding,"files":images}
    directory=Path(output_root)/'developer-changes'/task.task_id
    directory.mkdir(parents=True,exist_ok=True)
    path=directory/(binding['candidate_hash']+'.sources.json')
    raw=canonical(payload).encode('utf-8')
    if path.exists() and path.read_bytes()!=raw:
        raise ValueError('REPORT_SOURCE_IMMUTABILITY_FAILED')
    atomic_state_write(path,raw,root=Path(output_root),field='developer_report_sources')
    task.developer_change_report={'source_ref':str(path),'source_sha256':sha(raw),'binding':binding,'freshness':'CURRENT','disposition':'CANDIDATE'}


def freshness(report, current):
    keys=('job_id','contract_revision','job_input_hash','candidate_hash','diff_hash','acceptance_hash')
    return 'CURRENT' if all(report.get('binding',{}).get(key)==current.get(key) for key in keys) else 'STALE'


INSUFFICIENT_EVIDENCE = 'INSUFFICIENT_EVIDENCE'

# Facts are a deterministic projection of recorded evidence only.  No narrative
# is invented here: unexplained entries stay INSUFFICIENT_EVIDENCE (Step 10 §27).
def _disposition(task, sources):
    if task.is_rolled_back:
        return 'ROLLED_BACK'
    if not task.changed_files and task.worktree_disposition == 'NO_DELTA':
        return 'NO_PRODUCT_CHANGE'
    verified = (task.status in {'SUCCESS', 'SUCCEEDED'} and task.verification_status == 'VERIFIED'
                and not task.qa_type and not task.qa_request.get('required'))
    if verified and task.worktree_disposition == 'CANONICAL_COMMITTED':
        return 'FINAL'
    if verified:
        return 'VERIFIED_CANDIDATE'
    return 'CANDIDATE' if sources.get('files') else 'DRAFT'

def _problem_and_root_cause(task):
    problem, cause = [], []
    prior = [a for a in (task.attempt_history or []) if a.get('failure_code')]
    for entry in prior:
        problem.append("Prior attempt failed with {code} at stage {stage}.".format(
            code=entry.get('failure_code'), stage=entry.get('failure_stage') or entry.get('stage') or 'UNKNOWN'))
    if prior and not task.failure_code:
        cause.append("Recorded prior-attempt failure classification: type={type}, origin={origin}.".format(
            type=prior[-1].get('failure_type') or INSUFFICIENT_EVIDENCE,
            origin=prior[-1].get('failure_origin') or INSUFFICIENT_EVIDENCE))
    if task.failure_code:
        problem.append("Current attempt failure: {code} at stage {stage} — {reason}".format(
            code=task.failure_code, stage=task.failure_stage or 'UNKNOWN',
            reason=task.failure_reason or 'no recorded reason'))
        if task.failure_type == 'infrastructure' or task.failure_origin == 'TOOL_INFRA':
            cause.append("Recorded classification: infrastructure failure, not a product defect.")
        else:
            cause.append("Recorded classification: type={type}, origin={origin}.".format(
                type=task.failure_type or INSUFFICIENT_EVIDENCE,
                origin=task.failure_origin or INSUFFICIENT_EVIDENCE))
    if task.review_result:
        problem.append("Reviewer findings: {findings}".format(findings=task.review_result))
    if task.status == 'AWAITING_QA':
        problem.append("Human QA decision is pending; machine gates alone cannot finalize this change.")
    if not problem:
        problem.append("Planned change under the recorded Job contract; no failure was recorded.")
    if not cause:
        cause.append(INSUFFICIENT_EVIDENCE + " — no failure exists to explain.")
    return ' '.join(problem), ' '.join(cause)

def _change_summary(files):
    summary = []
    for item in files:
        hunks = added = removed = 0
        redacted = False
        for excerpt in item.get('excerpts', []):
            if excerpt.get('redacted'):
                redacted = True
                continue
            hunks += 1
            before, after = excerpt['as_is'].splitlines(), excerpt['to_be'].splitlines()
            for op, i1, i2, j1, j2 in difflib.SequenceMatcher(a=before, b=after, autojunk=False).get_opcodes():
                if op in ('replace', 'delete'):
                    removed += i2 - i1
                if op in ('replace', 'insert'):
                    added += j2 - j1
        summary.append({'path': item['path'], 'hunks': hunks, 'added_lines': added,
                        'removed_lines': removed, 'redacted': redacted})
    return summary

def _why_changed(task):
    why = ["Recorded Job contract: {goal}".format(goal=task.requirement)]
    prior = [a.get('failure_code') for a in (task.attempt_history or []) if a.get('failure_code')]
    if prior:
        why.append("A prior attempt of this Job failed ({codes}); causal linkage to this change requires the source and verification evidence.".format(
            codes=', '.join(sorted(set(prior)))))
    return ' '.join(why)

def _expected_result(task):
    plan = dict(task.test_plan or {})
    expected = []
    if plan.get('required_tests'):
        expected.append({'item': 'Required deterministic tests execute successfully against the candidate.',
                         'ref': plan['required_tests'], 'classification': 'EXPECTED'})
    browser = dict(task.browser_qa or {})
    if browser.get('required'):
        expected.append({'item': 'Required browser QA acceptance holds on the sanctioned environment.',
                         'ref': browser.get('scenario_id') or '', 'classification': 'EXPECTED'})
    if task.qa_request.get('required') or task.qa_type:
        expected.append({'item': 'A human QA decision (accept or reject) is required before canonical promotion.',
                         'ref': '', 'classification': 'EXPECTED'})
    expected.append({'item': 'The Job reaches verification VERIFIED through the recorded gates.',
                     'ref': '', 'classification': 'EXPECTED'})
    return expected

def _actual_result(task):
    browser = dict(task.browser_qa or {})
    return [
        {'gate': 'Build', 'result': task.build.get('status') or 'NOT_RUN', 'classification': 'ACTUAL'},
        {'gate': 'Test', 'result': task.test_status or 'NOT_RUN', 'classification': 'ACTUAL'},
        {'gate': 'Tester', 'result': 'INVOKED' if task.official_tester_invoked else 'NOT_INVOKED', 'classification': 'ACTUAL'},
        {'gate': 'Review', 'result': task.review_status or 'NOT_RUN', 'classification': 'ACTUAL'},
        {'gate': 'Verification', 'result': task.verification_status or 'UNVERIFIED', 'classification': 'ACTUAL'},
        {'gate': 'BrowserQA', 'result': browser.get('latest_status') or ('NOT_REQUIRED' if not browser.get('required') else 'UNVERIFIED'), 'classification': 'ACTUAL'},
        {'gate': 'Job', 'result': task.status or 'UNVERIFIED', 'classification': 'ACTUAL'},
        {'gate': 'Canonical', 'result': task.worktree_disposition or 'UNVERIFIED', 'classification': 'ACTUAL'},
    ]

def _qa_facts(task, decision=None):
    qa = {'required': bool(task.qa_request.get('required')) or bool(task.qa_type),
          'qa_type': task.qa_type or '', 'job_status': task.status,
          'worktree_disposition': task.worktree_disposition or ''}
    if task.status == 'AWAITING_QA':
        qa['state'] = 'AWAITING_QA'
        qa['user_action'] = 'A current Harness human decision is pending; consult its legal actions. No publication is inferred.'
    elif task.status == 'CANCELLED':
        qa['state'] = 'CANCELLED'
        qa['user_action'] = 'Execution is cancelled. Canonical state must be read from the recorded publication/rollback evidence.'
    else:
        qa['state'] = task.status
        qa['user_action'] = ''
    if decision:
        qa['decision'] = {'decision_id': decision.get('decision_id', ''),
                          'resolution': decision.get('resolution', ''),
                          'candidate_id': decision.get('candidate_id', '')}
    return qa

def _retry_summary(task):
    return {'retry_domain': task.retry_domain or '',
            'runtime_recovery_count': task.runtime_recovery_count,
            'technical_execution_retry_count': task.technical_execution_retry_count,
            'product_semantic_retry_count': task.product_semantic_retry_count,
            'transitions': [{'attempt': a.get('attempt'),
                             'failure_code': a.get('failure_code') or '',
                             'status': a.get('status') or ''} for a in (task.attempt_history or [])],
            'candidate_preserved': task.worktree_disposition in
                                   {'PRESERVED', 'ISOLATED_CANDIDATE_PRESERVED',
                                    'REJECTED_ISOLATED_CANDIDATE_PRESERVED', 'CANONICAL_COMMITTED'}}

def _known_limitations(task):
    limitations = []
    browser = dict(task.browser_qa or {})
    if browser.get('required') and browser.get('latest_status') != 'PASS':
        limitations.append({'item': 'Required browser QA on the sanctioned environment',
                            'state': browser.get('latest_status') or 'UNVERIFIED'})
    if task.status == 'AWAITING_QA':
        limitations.append({'item': 'Human QA decision', 'state': 'AWAITING_QA'})
    if task.is_rolled_back:
        limitations.append({'item': 'Candidate changes were rolled back', 'state': 'ROLLED_BACK'})
    if task.user_action_required:
        limitations.append({'item': 'User action required', 'state': 'USER_ACTION_REQUIRED'})
    return limitations

def _evidence_references(task, sources):
    references = []
    report_meta = task.developer_change_report or {}
    if report_meta.get('source_ref'):
        references.append({'kind': 'source_excerpts', 'path': report_meta['source_ref'],
                           'sha256': report_meta.get('source_sha256', '')})
    references.append({'kind': 'build_evidence', 'status': task.build.get('status') or 'NOT_RUN'})
    references.append({'kind': 'test_evidence', 'records': len(task.test_evidence or [])})
    references.append({'kind': 'review', 'status': task.review_status or 'NOT_RUN'})
    references.append({'kind': 'gate_evidence', 'gates': sorted((task.gate_evidence or {}).keys())})
    references.append({'kind': 'task_state', 'task_id': task.task_id})
    return references

def build_facts(task, sources, decision=None):
    """Shared structured facts for the developer and business reports.

    Deterministic projection of recorded evidence: identical task + sources
    inputs always produce identical facts. Unexplained entries remain
    INSUFFICIENT_EVIDENCE instead of a narrative.
    """
    files = sources.get('files', [])
    problem, root_cause = _problem_and_root_cause(task)
    hunks = [{'path': f['path'],
              'as_is': [{'start': e['as_is_start'], 'text': e['as_is']} for e in f['excerpts'] if not e.get('redacted')],
              'to_be': [{'start': e['to_be_start'], 'text': e['to_be']} for e in f['excerpts'] if not e.get('redacted')],
              'redacted': any(e.get('redacted') for e in f['excerpts'])} for f in files]
    return {
        'schema_version': 1, 'kind': 'ReportFacts',
        'binding': {**sources['binding'], 'task_id': task.task_id, 'job_status': task.status,
                    'verification_status': task.verification_status,
                    'worktree_disposition': task.worktree_disposition, 'as_of': task.ended_at or ''},
        'job': {'job_id': task.job_id, 'task_id': task.task_id, 'goal': task.requirement,
                'client_job_id': task.client_job_id, 'operator_label': task.operator_label,
                'status': task.status, 'disposition': _disposition(task, sources)},
        'problem': problem, 'root_cause': root_cause,
        'requirement': task.requirement,
        'logic_delta': {'status': 'SOURCE_EXCERPTS_ONLY', 'hunks': hunks},
        'intentionally_unchanged': {'status': 'INSUFFICIENT_EVIDENCE', 'items': []},
        'regression_considerations': {
            'required_tests': list((task.test_plan or {}).get('required_tests') or []),
            'note': 'Test declarations are coverage intentions, not observed PASS.'},
        'developer_notes': {'status': 'SOURCE_DERIVED',
                            'affected_paths': sorted(set(task.changed_files or []))},
        'as_is': [{'path': h['path'], 'excerpts': h['as_is'], 'redacted': h['redacted']} for h in hunks],
        'to_be': [{'path': h['path'], 'excerpts': h['to_be'], 'redacted': h['redacted']} for h in hunks],
        'change': _change_summary(files),
        'why_changed': _why_changed(task),
        'impact': {'changed_files': sorted(set(task.changed_files or [])),
                   'modules': list(task.target_module or ()), 'commit_summary': task.commit_summary},
        'expected_result': _expected_result(task),
        'actual_result': _actual_result(task),
        'verification': {'build': task.build.get('status') or 'NOT_RUN', 'test': task.test_status or 'NOT_RUN',
                         'tester_invoked': bool(task.official_tester_invoked),
                         'review': task.review_status or 'NOT_RUN',
                         'verification': task.verification_status or 'UNVERIFIED'},
        'qa': _qa_facts(task, decision),
        'retry_recovery': _retry_summary(task),
        'known_limitations': _known_limitations(task),
        'user_action_required': {'required': bool(task.user_action_required),
                                 'open_decisions': [d.get('question', '') for d in (task.open_decisions or [])]},
        'evidence_references': _evidence_references(task, sources),
    }


# The Humanizer is an optional presentation layer for developer-report prose.
# It never sees structured facts, and its output is discarded unless every
# protected token (statuses, codes, identifiers, counts, paths) survives.
import re as _re
_PROTECTED_TOKENS = _re.compile(
    r"\b(?:PASS|FAIL|ERROR|SKIPPED|UNVERIFIED|VERIFIED|PARTIAL|AWAITING_QA|AWAITING_DEPENDENCY_QA|SUCCEEDED|SUCCESS|FAILED|FAILED_FINAL|BLOCKED|CANCELLED|INTERRUPTED|USER_WAIVED|NOT_REEXECUTED|INSUFFICIENT_EVIDENCE|REVIEW_PASS|REVIEW_FAIL)\b"
    r"|\b[A-Z][A-Z0-9_]{3,}\b|\b[\w.-]+/[\w./-]+\b|\b\d+\b")

def _protected_tokens(text):
    return set(match.group(0) for match in _PROTECTED_TOKENS.finditer(text))

def _distinct_words(text):
    return set(word.strip('.,;:()[]{}"\'').casefold()
               for word in text.split() if word.strip('.,;:()[]{}"\''))

def humanize(text, engine=None):
    """Fail-closed prose cleanup: facts, statuses and identifiers are preserved.

    Discarded output: engine failure, empty text, any dropped protected token,
    or any non-whitespace token change. This is formatting-only, not a semantic
    equivalence detector; arbitrary prose rewriting is deliberately rejected.
    """
    if engine is None:
        return text
    try:
        cleaned = engine(text)
    except Exception:
        return text
    if not isinstance(cleaned, str) or not cleaned.strip():
        return text
    if _protected_tokens(text) - _protected_tokens(cleaned):
        return text
    # A token set check allows new claims, negation reordering and numeric edits.
    # Only whitespace presentation changes are accepted for fact-bearing prose.
    if text.split() != cleaned.split():
        return text
    return scrub_secrets(cleaned)


def render(task, sources, facts=None, humanizer=None):
    binding=sources['binding']
    facts = facts or build_facts(task, sources)
    disposition=_disposition(task, sources)
    def prose(text):
        return humanize(text, humanizer)
    report={'schema_version':3,'kind':'DeveloperChangeReport','job_id':task.job_id,'task_id':task.task_id,
            'goal':task.requirement,'disposition':disposition,'binding':binding,'freshness':'CURRENT',
            'logic_delta': facts.get('logic_delta', {}),
            'intentionally_unchanged': facts.get('intentionally_unchanged', {}),
            'regression_considerations': facts.get('regression_considerations', {}),
            'developer_notes': facts.get('developer_notes', {}),
            'facts':{'problem':facts['problem'],'root_cause':facts['root_cause'],
                     'why_changed':facts['why_changed'],'expected_result':facts['expected_result'],
                     'actual_result':facts['actual_result'],'known_limitations':facts['known_limitations'],
                     'qa':facts['qa'],'retry_recovery':facts['retry_recovery']},
            'changed_files':sources.get('files',[]),'build':dict(task.build),'test':{'status':task.test_status,'official':task.official_tester_invoked,'evidence':task.test_evidence},
            'review':{'status':task.review_status,'findings':task.review_result},'verification':task.verification_status,
            'existing_behavior':'The AS-IS excerpts are the actual Job input implementation.',
            'new_behavior':'The TO-BE excerpts are the captured candidate implementation.',
            'why_changed':facts['why_changed'],'human_qa_points':dict(task.qa_request or {}) or {'required':False,'note':'Check the acceptance criteria and Reviewer findings; no QA acceptance is implied.'},
            'execution_identity':{'logical_job_id':task.logical_job_id,'materialization_id':task.materialization_id,
                'execution_id':task.execution_id,'attempt_id':task.attempt_id,'runtime_process_id':task.runtime_process_id,
                'native_session_binding_id':task.native_session_binding_id,'native_session_id':task.native_session_id,
                'native_turn_id':task.native_turn_id,'command_id':task.command_id},
            'runtime':{'adapter':dict(task.runtime_adapter or {}),'retry_domain':task.retry_domain,
                'runtime_recovery_count':task.runtime_recovery_count,
                'technical_execution_retry_count':task.technical_execution_retry_count,
                'product_semantic_retry_count':task.product_semantic_retry_count,
                'recoveries':list(task.runtime_recovery_history or [])},
            'product_readiness':{'status':task.product_readiness,'user_action_required':task.user_action_required,
                'open_decision_count':task.open_decision_count,'open_decisions':list(task.open_decisions or [])},
            'validation_dispositions':list(task.validation_dispositions or []),
            'troubleshooting_evidence':{'execution_runtime':dict(task.execution_runtime or {}),
                'execution_runtime_history':list(task.execution_runtime_history or []),
                'failure_code':task.failure_code,'failure_origin':task.failure_origin,
                'failure_stage':task.failure_stage,'report_diagnostics':list(task.report_diagnostics or []),
                'bundle':dict(task.troubleshooting_bundle or {})}}
    changed=', '.join(sorted(set(task.changed_files or []))) or 'none'
    verification_line='Build: {b} · Test: {t} · Review: {r} · Verification: {v}'.format(
        b=task.build.get('status','NOT_RUN'),t=task.test_status or 'NOT_RUN',
        r=task.review_status or 'NOT_RUN',v=task.verification_status or 'UNVERIFIED')
    lines=['# DeveloperChangeReport',
           '', '## Summary',
           f"Job: {task.job_id}", f"Task: {task.task_id}", f"Disposition: {disposition}",
           f"Goal: {task.requirement}", f"Changed files: {changed}", f"Verification: {verification_line}",
           '', '## Problem', prose(facts['problem']),
           '', '## Root Cause', prose(facts['root_cause']),
           '', '## AS-IS / TO-BE source excerpts',
           'The following AS-IS and TO-BE excerpts are extracted from the actual input and candidate source.']
    for item in sources.get('files',[]):
        lines.extend(['', '### '+item['path'],f"Input blob: {item['input_blob']}"])
        for excerpt in item['excerpts']:
            if excerpt.get('redacted'):
                lines.append(excerpt['reason']); continue
            lines.extend(['AS-IS (line '+str(excerpt['as_is_start'])+')','````',excerpt['as_is'],'````',
                          'TO-BE (line '+str(excerpt['to_be_start'])+')','````',excerpt['to_be'],'````'])
    lines.extend(['', '## Change'])
    for entry in facts['change']:
        if entry['redacted']:
            lines.append(f"- {entry['path']}: content redacted (protected value in source)")
        else:
            lines.append(f"- {entry['path']}: {entry['hunks']} hunk(s), +{entry['added_lines']}/-{entry['removed_lines']} lines")
    lines.extend(['', '## Why This Change', prose(facts['why_changed']),
                  '', '## Impact',
                  f"Modules: {', '.join(task.target_module or ()) or 'unspecified'}",
                  f"Commit summary: {task.commit_summary or 'unspecified'}",
                  '', '## Verification (recorded evidence)',
                  f"Build: {task.build.get('status','NOT_RUN')}",
                  f"Test: {task.test_status} · official Tester invoked={task.official_tester_invoked}",f"Review: {task.review_status}",
                  f"Verification: {task.verification_status}",
                  '', '## Reviewer findings',task.review_result or 'No findings available.',
                  '', '## Expected Result'])
    for entry in facts['expected_result']:
        lines.append(f"- [EXPECTED] {entry['item']}" + (f" (ref: {entry['ref']})" if entry.get('ref') else ''))
    lines.append('')
    lines.append('## Actual Result')
    for entry in facts['actual_result']:
        lines.append(f"- [ACTUAL] {entry['gate']}: {entry['result']}")
    lines.extend(['', '## Remaining Checks / Limitations'])
    if facts['known_limitations']:
        for entry in facts['known_limitations']:
            lines.append(f"- {entry['item']}: {entry['state']}")
    else:
        lines.append('- None recorded for this attempt.')
    lines.extend(['', '## Human QA', json.dumps(facts['qa'], ensure_ascii=False, indent=2),
                  '', '## Runtime / product state',
           f"Retry domain: {task.retry_domain or 'NONE'} · runtime/product retries: {task.runtime_recovery_count}/{task.product_semantic_retry_count}",
           f"Technical execution retries: {task.technical_execution_retry_count} · candidate preserved: {facts['retry_recovery']['candidate_preserved']}",
           f"Product readiness: {task.product_readiness} · open decisions: {task.open_decision_count}",
           '', '## Human QA points',json.dumps(report['human_qa_points'],ensure_ascii=False,indent=2),
           '', '## Evidence References'])
    for reference in facts['evidence_references']:
        lines.append(f"- {reference['kind']}: {json.dumps({k: v for k, v in reference.items() if k != 'kind'}, ensure_ascii=False)}")
    lines.extend(['', '## Intentionally Unchanged', 'INSUFFICIENT_EVIDENCE: no explicit unchanged-scope claim was recorded.',
                  '', '## Regression / Developer Notes',
                  json.dumps(facts.get('regression_considerations', {}), ensure_ascii=False),
                  json.dumps(facts.get('developer_notes', {}), ensure_ascii=False)])
    lines.extend(['', '## Binding','```json',json.dumps(binding,ensure_ascii=False,indent=2),'```'])
    return report,scrub_secrets('\n'.join(lines))+'\n'


def save(task, output_root, humanizer=None, decision=None):
    reference=task.developer_change_report.get('source_ref')
    if reference:
        raw=Path(reference).read_bytes()
        if sha(raw)!=task.developer_change_report['source_sha256']:
            raise ValueError('REPORT_SOURCE_INTEGRITY_FAILED')
        sources=json.loads(raw)
    else:
        sources={'binding':{'job_id':task.job_id,'contract_revision':task.materialized_execution.get('contract_revision',1)},'files':[]}
    facts=build_facts(task,sources,decision=decision)
    report,markdown=render(task,sources,facts=facts,humanizer=humanizer)
    if reference and not task.is_rolled_back:
        current={**sources['binding'],**identity(task,task.working_dir),
                 'contract_revision':task.materialized_execution.get('contract_revision',1)}
        report['freshness']=freshness(report,current)
        markdown=markdown.replace('# DeveloperChangeReport\n','# DeveloperChangeReport\nFreshness: '+report['freshness']+'\n',1)
    directory=Path(output_root)/'developer-changes'/task.task_id
    directory.mkdir(parents=True,exist_ok=True)
    facts_path=directory/'ReportFacts.json'
    facts_raw=canonical(facts).encode('utf-8')
    atomic_state_write(facts_path,facts_raw,root=Path(output_root),field='report_facts')
    path=directory/'DeveloperChangeReport.md'
    atomic_state_write(path,markdown.encode('utf-8'),root=Path(output_root),field='developer_report')
    atomic_state_write(directory/'DeveloperChangeReport.json',canonical(report).encode('utf-8'),root=Path(output_root),field='developer_report')
    task.developer_change_report.update({'path':str(path),'sha256':sha(path.read_bytes()),'disposition':report['disposition'],'binding':report['binding'],'freshness':report['freshness'],
        'facts_path':str(facts_path),'facts_sha256':sha(facts_raw)})
    return path
