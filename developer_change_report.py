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


def render(task, sources):
    binding=sources['binding']
    disposition=('ROLLED_BACK' if task.is_rolled_back else 'NO_PRODUCT_CHANGE' if not task.changed_files and task.worktree_disposition=='NO_DELTA'
                 else 'FINAL' if task.status=='SUCCESS' and task.verification_status=='VERIFIED' and not task.qa_type and not task.qa_request.get('required')
                 else 'CANDIDATE' if sources.get('files') else 'DRAFT')
    report={'schema_version':2,'kind':'DeveloperChangeReport','job_id':task.job_id,'task_id':task.task_id,
            'goal':task.requirement,'disposition':disposition,'binding':binding,'freshness':'CURRENT',
            'changed_files':sources.get('files',[]),'build':dict(task.build),'test':{'status':task.test_status,'official':task.official_tester_invoked,'evidence':task.test_evidence},
            'review':{'status':task.review_status,'findings':task.review_result},'verification':task.verification_status,
            'existing_behavior':'The AS-IS excerpts are the actual Job input implementation.',
            'new_behavior':'The TO-BE excerpts are the captured candidate implementation.',
            'why_changed':'Approved Job contract; see goal above.','human_qa_points':dict(task.qa_request or {}) or {'required':False,'note':'Check the acceptance criteria and Reviewer findings; no QA acceptance is implied.'},
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
    lines=['# DeveloperChangeReport',f"Job: {task.job_id}",f"Task: {task.task_id}",f"Disposition: {disposition}",
           '', '## Job goal',task.requirement,'','## Evidence',f"Build: {task.build.get('status','NOT_RUN')}",
           f"Test: {task.test_status} · official Tester invoked={task.official_tester_invoked}",f"Review: {task.review_status}",
           f"Verification: {task.verification_status}",'','## Reviewer findings',task.review_result or 'No findings available.',
           '', '## Runtime / product state',
           f"Retry domain: {task.retry_domain or 'NONE'} · runtime/product retries: {task.runtime_recovery_count}/{task.product_semantic_retry_count}",
           f"Product readiness: {task.product_readiness} · open decisions: {task.open_decision_count}",
           '', '## Human QA points',json.dumps(report['human_qa_points'],ensure_ascii=False,indent=2),
           '', '## Why changed','Approved Job contract; see goal above.','','## Existing behavior / New behavior',
           'The following AS-IS and TO-BE excerpts are extracted from the actual input and candidate source.']
    for item in sources.get('files',[]):
        lines.extend(['', '### '+item['path'],f"Input blob: {item['input_blob']}"])
        for excerpt in item['excerpts']:
            if excerpt.get('redacted'):
                lines.append(excerpt['reason']); continue
            lines.extend(['AS-IS (line '+str(excerpt['as_is_start'])+')','````',excerpt['as_is'],'````',
                          'TO-BE (line '+str(excerpt['to_be_start'])+')','````',excerpt['to_be'],'````'])
    lines.extend(['','## Binding','```json',json.dumps(binding,ensure_ascii=False,indent=2),'```'])
    return report,'\n'.join(lines)+'\n'


def save(task, output_root):
    reference=task.developer_change_report.get('source_ref')
    if reference:
        raw=Path(reference).read_bytes()
        if sha(raw)!=task.developer_change_report['source_sha256']:
            raise ValueError('REPORT_SOURCE_INTEGRITY_FAILED')
        sources=json.loads(raw)
    else:
        sources={'binding':{'job_id':task.job_id,'contract_revision':task.materialized_execution.get('contract_revision',1)},'files':[]}
    report,markdown=render(task,sources)
    if reference and not task.is_rolled_back:
        current={**sources['binding'],**identity(task,task.working_dir),
                 'contract_revision':task.materialized_execution.get('contract_revision',1)}
        report['freshness']=freshness(report,current)
        markdown=markdown.replace('# DeveloperChangeReport\n','# DeveloperChangeReport\nFreshness: '+report['freshness']+'\n',1)
    directory=Path(output_root)/'developer-changes'/task.task_id
    directory.mkdir(parents=True,exist_ok=True)
    path=directory/'DeveloperChangeReport.md'
    atomic_state_write(path,markdown.encode('utf-8'),root=Path(output_root),field='developer_report')
    atomic_state_write(directory/'DeveloperChangeReport.json',canonical(report).encode('utf-8'),root=Path(output_root),field='developer_report')
    task.developer_change_report.update({'path':str(path),'sha256':sha(path.read_bytes()),'disposition':report['disposition'],'binding':report['binding'],'freshness':report['freshness']})
    return path
