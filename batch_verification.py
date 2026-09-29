"""Proposal only: freeze the union of two already-published approved contracts.

This pure function grants no execution, publication, or Product PASS authority.
The existing host must load authoritative records, hold canonical ownership,
admit a fresh gate-only execution, and run its normal four gates separately.
"""
import copy
from pathlib import PurePosixPath

from control_repository import digest
from deterministic_test_policy import validate_declaration
from job_contract import validate_idempotency_key


def _require(value, code):
    if not value:
        raise ValueError(code)


def _safe_paths(values):
    _require(isinstance(values, list) and values, 'BATCH_SCOPE_REQUIRED')
    for value in values:
        _require(isinstance(value, str) and value, 'BATCH_SCOPE_INVALID')
        path = PurePosixPath(value)
        _require(isinstance(value, str) and value and not path.is_absolute()
                 and '..' not in path.parts and ':' not in value and '\\' not in value
                 and path.as_posix() == value, 'BATCH_SCOPE_INVALID')
    return sorted(set(values))


def freeze_batch_contract(parents, *, request_id, canonical_content, canonical_heads):
    """Return a non-authoritative recipe bound to exact post-B canonical bytes.

    Parents are ordered A then B and contain authoritative Job, frozen
    Execution, publication intent/journal, and the finished Task projection.
    They must have compatible frozen native or deterministic test policies.
    Commands and assertions are preserved; no shell command is synthesized.
    """
    validate_idempotency_key(request_id, 'request_id')
    _require(isinstance(parents, list) and len(parents) == 2,
             'BATCH_EXACT_TWO_PARENTS_REQUIRED')
    _require(isinstance(canonical_content, dict) and canonical_content
             and isinstance(canonical_heads, dict) and canonical_heads,
             'BATCH_CANONICAL_IDENTITY_REQUIRED')
    jobs, executions, publications, tests, commands, assertions = [], [], [], [], {}, {}
    modules, resources, changed, production, derived, requirements, lineage = [], [], [], [], [], [], []
    policy = evaluation = reference_execution = None
    deterministic = None
    integration_contracts = []
    for parent in parents:
        job, execution, task = parent['job'], parent['execution'], parent['task']
        intent, journal = parent['intent'], parent['journal']
        job_id, execution_id = job['job_id'], execution['execution_id']
        revision = int(execution['contract_revision'])
        result = job.get('last_result') or {}
        promotion = result.get('canonical_promotion') or {}
        pub_id, candidate_id = promotion.get('publication_id'), result.get('candidate_id')
        _require(job.get('status') == 'SUCCEEDED' and result.get('success') is True
                 and result.get('verification_status') == 'VERIFIED',
                 'BATCH_PARENT_NOT_VERIFIED')
        _require(execution.get('job_id') == job_id and task.get('job_id') == job_id
                 and task.get('execution_id') == execution_id
                 and revision == job.get('contract_revision', 1), 'BATCH_PARENT_LINEAGE_MISMATCH')
        _require(job_id not in jobs and execution_id not in executions
                 and pub_id and pub_id not in publications and candidate_id,
                 'BATCH_PARENT_IDENTITY_DUPLICATED')
        _require(promotion.get('status') == 'PROMOTED'
                 and candidate_id in promotion.get('candidate_ids', [])
                 and intent.get('state') == 'PUBLISHED'
                 and (intent.get('job_id'), intent.get('execution_id'), intent.get('candidate_id'), intent.get('publication_id'))
                     == (job_id, execution_id, candidate_id, pub_id)
                 and journal.get('state') == 'COMPLETED' and journal.get('disposition') == 'PROMOTED'
                 and journal.get('publication_id') == pub_id
                 and candidate_id in journal.get('candidate_ids', [])
                 and journal.get('authority', {}).get('job_id') == job_id
                 and journal.get('authority', {}).get('execution_id') == execution_id
                 and journal.get('authority', {}).get('contract_revision') == revision,
                 'BATCH_PARENT_PUBLICATION_NOT_BOUND')
        parent_changed = _safe_paths(promotion.get('changed_files'))
        _require(not set(changed).intersection(parent_changed), 'BATCH_PARENT_WRITE_OVERLAP')
        postimages = {}
        for entry in journal.get('paths', []):
            _require(entry.get('candidate_id') == candidate_id, 'BATCH_JOURNAL_CANDIDATE_MISMATCH')
            relative = _safe_paths([entry.get('path')])[0]
            intended = 'ABSENT' if entry.get('intended_state') == 'DELETED' else entry.get('intended_sha256')
            _require(canonical_content.get(relative, 'ABSENT') == intended,
                     'BATCH_PUBLISHED_BYTES_CHANGED')
            postimages[relative] = intended
        _require(set(postimages) == set(parent_changed), 'BATCH_PUBLICATION_SCOPE_MISMATCH')
        plan = copy.deepcopy(execution.get('test_plan') or {})
        validate_declaration(plan)
        _require(not plan.get('browser_qa') and plan.get('mandatory') is True
                 and plan.get('verification_depth') in {'', 'STATIC', 'TARGETED'},
                 'BATCH_TEST_CONTRACT_MERGE_UNSUPPORTED')
        declared_deterministic = plan.get('evaluation') is not None
        declaration = ({k:v for k,v in plan['evaluation'].items() if k != 'assertions'}
                       if declared_deterministic else None)
        inherited = {k:copy.deepcopy(execution.get('effective_policy', {}).get(k))
                     for k in ('resolved_tester', 'resolved_reviewer', 'supervisor_policy')}
        # Evaluation hashes legitimately differ by approved test scope. Its
        # runtime/kind/model/version must still be exactly the same.
        if isinstance(inherited['resolved_tester'], dict):
            inherited['resolved_tester'].pop('evaluation_sha256', None)
        if reference_execution is None:
            reference_execution = execution
            policy, evaluation, deterministic = inherited, declaration, declared_deterministic
        _require(inherited == policy and declaration == evaluation
                 and declared_deterministic == deterministic,
                 'BATCH_PARENT_POLICY_MISMATCH')
        _require(all(execution.get(k) == reference_execution.get(k) for k in
                     ('policy_revision', 'harness_source_identity'))
                 and parent.get('profile_contract_sha256')
                 and parent['profile_contract_sha256'] == parents[0].get('profile_contract_sha256'),
                 'BATCH_PARENT_SOURCE_PROFILE_MISMATCH')
        request = execution.get('request') or {}
        requirement = execution.get('current_requirement') or request.get('requirement')
        _require(requirement and requirement == job.get('current_requirement', job['request']['requirement']),
                 'BATCH_PARENT_REQUIREMENT_MISMATCH')
        required = _safe_paths(plan.get('required_tests'))
        if deterministic:
            actions = [{'test':test,'command':plan['test_commands'][test]} for test in required]
        else:
            from types import SimpleNamespace
            from tester import _authorized_test_actions
            actions = _authorized_test_actions(SimpleNamespace(test_plan=plan, materialized_execution={}))
        _require({action['test'] for action in actions} == set(required)
                 and all(action['command'] for action in actions), 'BATCH_PARENT_TEST_COMMAND_UNBOUND')
        for action in actions:
            test, command = action['test'], action['command']
            assertion = plan['evaluation']['assertions'][test] if deterministic else None
            _require(test not in commands or (commands[test] == command and assertions[test] == assertion),
                     'BATCH_TEST_COMMAND_CONFLICT')
            commands[test], assertions[test] = command, assertion
        integration_contracts.append({'job_id':job_id,'execution_id':execution_id,
            'execution_sha256':digest(execution),'test_plan':copy.deepcopy(plan)})
        tests.extend(required)
        modules.extend(_safe_paths(list(request.get('target_modules') or [])))
        resources.extend(_safe_paths(list(request.get('target_resources') or [])))
        production.extend(plan.get('production_scope') or request['target_resources'])
        derived.extend(plan.get('derived_tests') or [])
        changed.extend(parent_changed)
        requirements.append(requirement)
        jobs.append(job_id); executions.append(execution_id); publications.append(pub_id)
        lineage.append({'job_id':job_id,'job_revision':job['revision'],'execution_id':execution_id,
                        'contract_revision':revision,'candidate_id':candidate_id,'publication_id':pub_id,
                        'job_sha256':digest(job),'execution_sha256':digest(execution),
                        'task_sha256':digest(task),'intent_sha256':digest(intent),'journal_sha256':digest(journal)})
    merged = {'schema_version':1,'production_scope':sorted(set(production)),
              'required_tests':sorted(set(tests)),'derived_tests':sorted(set(derived)-set(tests)),
              'command':'','capability':'OFFICIAL_TESTER_READ_ONLY','mandatory':True,
              'verification_depth':'TARGETED','review_oracle':''}
    if deterministic:
        merged.update(evaluation={**evaluation,'assertions':assertions},test_commands=commands)
    validate_declaration(merged)
    merged['scope_hash'] = digest(merged)
    recipe = {'kind':'BATCH_VERIFICATION','request_id':request_id,'parent_lineage':lineage,
              'requirement':'\n\n'.join(requirements),'target_modules':sorted(set(modules)),
              'target_resources':sorted(set(resources)),'changed_files':sorted(set(changed)),
              'test_plan':merged,'canonical_content':copy.deepcopy(canonical_content),
              'integration_contracts':integration_contracts,
              'canonical_heads':copy.deepcopy(canonical_heads),
              'canonical_identity_sha256':digest({'content':canonical_content,'heads':canonical_heads}),
              'harness_source_identity':copy.deepcopy(reference_execution.get('harness_source_identity')),
              'execution_context':copy.deepcopy(reference_execution.get('execution_context')),
              'required_fresh_gates':['BUILD','TEST','REVIEW','VERIFICATION'],
              'worker_invoked':False,'publication_authorized':False,'commit_authorized':False,
              'product_pass_authority':False,'requires_gate_execution_admission':True}
    recipe['recipe_sha256'] = digest(recipe)
    return recipe


def profile_contract(profile_dir):
    """Preserve every profile byte except the owned candidate root binding."""
    from hermes_boundary import read
    from production_isolation import inventory
    from pathlib import Path
    root = Path(profile_dir)
    files = inventory(root, include_git=True)
    project = read(root / 'project.json')
    _require(isinstance(project.get('workspace_roots'), list), 'BATCH_PROFILE_ROOTS_REQUIRED')
    project.pop('workspace_roots')
    files['project.json'] = digest(project)
    return digest(files)


def verify_batch(control, job_ids, *, expected_revisions, request_id, confirmation):
    """Verify fresh combined input through existing gates; no Worker/publication/Git."""
    from dataclasses import asdict, replace
    from pathlib import Path
    import hashlib, shutil, uuid
    from canonical_ownership import canonical_ownership
    from canonical_revalidation import (assert_quiescent, source_job_binding, successor_task,
        admit_gate_execution, run_fresh_gates, task_scope, _key, _receipt, _write_receipt)
    from gate_evidence import identity, validate
    from git_collector import GitCollector
    from harness_service import HarnessService, EXECUTION_CONTEXT_KEYS
    from hermes_boundary import atomic, read, write_once
    from isolated_promotion import content, module_heads
    from production_isolation import inventory, validate_git_ownership
    from qa_quarantine import QACandidateStore, CandidateError
    from run import SingleInstanceLock
    from runtime_identity import source_process_identity
    from runtime_snapshot import load_frozen_runtime_snapshot
    control._assert_writable()
    _require(confirmation == 'I_CONFIRM_BATCH_CURRENT_INPUT_VERIFICATION'
             and len(job_ids) == 2 and len(set(job_ids)) == 2
             and set(expected_revisions) == set(job_ids), 'BATCH_CONFIRMATION_REQUIRED')
    validate_idempotency_key(request_id, 'request_id')
    request = dict(job_ids=list(job_ids), expected_revisions=expected_revisions, request_id=request_id)
    repo, promoter = control.store.repository, control.isolation.promoter
    with repo.hold():
        _require(repo._connection.execute('PRAGMA user_version').fetchone()[0]==6,
                 'BATCH_REVALIDATION_REQUIRES_SCHEMA6')
        prior = _receipt(repo, _key(request_id))
        if prior:
            _require(prior['binding'].get('operator_request') == request, 'BATCH_IDEMPOTENCY_CONFLICT')
            _require(prior['state'] == 'VERIFIED' and prior.get('batch_completed') is True,
                     'BATCH_RECONCILIATION_REQUIRED')
            return dict(replayed=True, historical_only=True, result=prior['result'])
    _require(not control.supervisor.is_running() and control.harness.acquire_execution_lease(),
             'ACTIVE_HARNESS_PROCESS')
    child = None
    try:
        with canonical_ownership(control.harness.working_dir):
            with repo.hold():
                assert_quiescent(repo, job_ids)
                _require(not repo.global_stop().get('active'), 'GLOBAL_STOP')
            profile, _ = control.harness._load_profile()
            _require(profile.control_policy.commit_policy == 'BATCH_FINAL_COMMIT', 'BATCH_COMMIT_POLICY_REQUIRED')
            origin = inventory(control.harness.profile_dir, include_git=True)
            semantic_profile = profile_contract(control.harness.profile_dir)
            parents, tasks, immutables, preserve = [], [], {}, []
            store = QACandidateStore(promoter.root/'candidates',promoter.canonical,None)
            _require(not store.unresolved_publications(), 'BATCH_UNRESOLVED_PUBLICATION')
            for job_id in job_ids:
                job = repo.read_job(job_id)
                _require(job['revision'] == expected_revisions[job_id], 'JOB_REVISION_CONFLICT')
                result = job.get('last_result') or {}
                execution = repo.execution(result.get('execution_id',''))
                _require(execution and execution['job_id']==job_id, 'BATCH_PARENT_EXECUTION_REQUIRED')
                allocation = promoter.allocations.get(job_id)
                _require(allocation is not None and allocation.get('held_verification'), 'BATCH_PARENT_CUSTODY_REQUIRED')
                bound = {**job,'materialized_execution':execution,'execution_context':execution['execution_context']}
                task, _ = promoter._task(bound,allocation['service'],allocation['held_verification']['machine_result'])
                _require(task.review_policy.get('risk_level') == 'LOW', 'BATCH_POLICY_UNSUPPORTED')
                _require(task.effective_policy == execution.get('effective_policy'), 'BATCH_EFFECTIVE_POLICY_CHANGED')
                _require(not validate(task,identity(task,task.working_dir),require_all=True), 'BATCH_PARENT_GATES_STALE')
                frozen = load_frozen_runtime_snapshot(control.harness.snapshot_root,Path(task.working_dir),execution['execution_context'])
                _require(profile_contract(frozen.profile_dir) == semantic_profile, 'BATCH_PROFILE_CHANGED')
                pub = result.get('canonical_promotion') or {}
                intent_path=promoter.root/(execution['execution_id']+'-intent.json')
                journal=store.load_publication(pub.get('publication_id',''))
                parents.append(dict(job=job,execution=execution,task=asdict(task),intent=read(intent_path),
                                    journal=journal,profile_contract_sha256=semantic_profile))
                tasks.append(task)
                preserve.extend(task.target_resources)
                preserve.extend(task.test_plan.get('required_tests',[]))
                for path in [intent_path,store._publication_path(pub['publication_id']),Path(task.candidate_manifest),Path(task.candidate_manifest).parent/'cleanup.json',
                             promoter.root/(execution['execution_id']+'-qa-verification.json')]:
                    immutables[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
            preserve=sorted(set(preserve))
            current=content(promoter.canonical,profile,preserve);heads=module_heads(promoter.canonical,profile)
            last_allocation=promoter.allocations[job_ids[-1]]
            _require(content(promoter.canonical,last_allocation['profile'],last_allocation['preserve'])
                     ==last_allocation['held_verification']['verified_input']
                     and current==content(last_allocation['service'].working_dir,profile,preserve)
                     and heads==last_allocation['canonical_heads'],'BATCH_UNOWNED_POST_PUBLICATION_DRIFT')
            recipe=freeze_batch_contract(parents,request_id=request_id,canonical_content=current,canonical_heads=heads)
            root=control.isolation.root/('batch-verify-'+uuid.uuid4().hex[:16]);root.mkdir(parents=True,exist_ok=False)
            candidate=root/'candidate';full=inventory(promoter.canonical,include_git=True)
            from production_isolation import copy_workspace_tree
            copy_workspace_tree(promoter.canonical,candidate)
            _require(inventory(promoter.canonical,include_git=True)==full
                     and inventory(candidate,include_git=True)==full,'BATCH_COPY_RACE')
            validate_git_ownership(candidate,profile)
            profile_dir=root/'profiles'/profile.id
            shutil.copytree(control.harness.profile_dir,profile_dir)
            _require(inventory(control.harness.profile_dir,include_git=True)==origin
                     and inventory(profile_dir,include_git=True)==origin,'BATCH_PROFILE_COPY_RACE')
            definition=read(profile_dir/'project.json')
            definition['workspace_roots']=[*definition['workspace_roots'],str(candidate)]
            atomic(profile_dir/'project.json',definition)
            child=HarnessService(candidate,profile_dir,task_root=control.harness.task_root,
                snapshot_root=control.harness.snapshot_root,manager_factory=control.harness.manager_factory,
                instance_lock=SingleInstanceLock(root/'execution.lock'),
                supervisor_advisor=getattr(control.harness,'supervisor_advisor',None))
            task=successor_task(tasks[-1],candidate=candidate,profile_dir=profile_dir,candidate_id='BCAND-'+uuid.uuid4().hex)
            task.requirement=recipe['requirement'];task.target_module=recipe['target_modules']
            task.target_resources=recipe['target_resources'];task.test_plan=recipe['test_plan']
            task.selective_revalidation=dict(reason='COMBINED_POST_PUBLICATION_BATCH_INPUT',worker_rerun=False,
                gates=['BUILD','TEST','REVIEW','VERIFICATION'])
            child_profile,available=child._load_profile();collector=GitCollector(str(candidate),profile=child_profile)
            # Each journal supplies exact preimages of its disjoint accepted
            # delta. The existing collector computes the combined review diff.
            records=[];postimages={};preimage_input=dict(current)
            for parent in parents:
                for entry in parent['journal']['paths']:
                    state=entry['base_state'];_require(state in {'PRESENT','DELETED'},'BATCH_BASE_STATE_INVALID')
                    snap='';before_sha='DELETED'
                    if state=='PRESENT':
                        base_root=Path(parent['journal']['base_snapshot_roots'][entry['candidate_id']]).resolve()
                        rel=PurePosixPath(entry['base_snapshot'])
                        _require(not rel.is_absolute() and '..' not in rel.parts and ':' not in str(rel)
                                 and '\\' not in str(rel),'BATCH_BASE_PATH_INVALID')
                        original=base_root.joinpath(*rel.parts)
                        _require(original.resolve().is_relative_to(base_root) and not original.is_symlink(),
                                 'BATCH_BASE_PATH_ESCAPE')
                        data=original.read_bytes();before_sha=hashlib.sha256(data).hexdigest()
                        _require(before_sha==entry['base_sha256'],'BATCH_BASE_BYTES_CHANGED')
                        snap='base/'+before_sha;destination=root/snap;destination.parent.mkdir(exist_ok=True)
                        if not destination.exists():
                            with destination.open('xb') as stream:stream.write(data)
                        _require(hashlib.sha256(destination.read_bytes()).hexdigest()==before_sha,'BATCH_BASE_COPY_CHANGED')
                    records.append(dict(path=entry['path'],state=state,sha256=before_sha,snapshot=snap))
                    path=candidate/entry['path']
                    postimages[entry['path']]=path.read_bytes() if path.is_file() else None
                    if state=='PRESENT':
                        path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes((root/snap).read_bytes())
                        preimage_input[entry['path']]=before_sha
                    else:
                        if path.is_file():path.unlink()
                        preimage_input.pop(entry['path'],None)
            _require(content(candidate,child_profile,preserve)==preimage_input,'BATCH_PREIMAGE_SCOPE_MISMATCH')
            baseline,error=collector.capture_baseline(task)
            _require(baseline is not None,'BATCH_BASELINE_FAILED:'+str(error))
            for rel,data in postimages.items():
                path=candidate/rel
                if data is None:
                    if path.is_file():path.unlink()
                else:path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(data)
            _require(content(candidate,child_profile,preserve)==current,'BATCH_POSTIMAGE_RESTORE_MISMATCH')
            snapshot=root/'pre-job.json'
            write_once(snapshot,dict(kind='PRE_JOB_SNAPSHOT',job_id=task.job_id,snapshot_id=task.task_id,files=records))
            baseline=replace(baseline,snapshot_manifest=snapshot)
            diff=collector.collect_selected(task,baseline,recipe['changed_files'])
            _require(diff.success and sorted(task.changed_files)==recipe['changed_files'] and task.git_diff.strip(),
                     'BATCH_DIFF_MISMATCH')
            task.task_owned_changed_files=recipe['changed_files'];task.changed_file_sha256={p:current.get(p,'DELETED') for p in task.changed_files}
            task.pre_job_snapshot_manifest=str(snapshot);task.pre_job_snapshot_id=task.task_id
            task.candidate_manifest=str(root/'admission.json')
            proof=dict(job_id=task.job_id,candidate_id=task.candidate_id,input_content=current,canonical_base=current,
                canonical_heads=heads,changed_files=task.changed_files,immutable_predecessor_files=immutables,
                recipe=recipe,worker_invoked=False,publication_authorized=False,
                profile_origin=str(control.harness.profile_dir),profile_origin_inventory=origin,
                profile_copy_inventory=inventory(profile_dir,include_git=True))
            write_once(root/'admission.json',proof)
            binding=dict(kind='BATCH_VERIFICATION',job_id=task.job_id,request_id=request_id,operator_request=request,
                predecessor_execution_id=tasks[-1].execution_id,candidate_id=task.candidate_id,workspace=str(candidate),
                admission_path=str(root/'admission.json'),admission_sha256=hashlib.sha256((root/'admission.json').read_bytes()).hexdigest(),
                input_content=current,canonical_base=current,canonical_heads=heads,preserve=preserve,
                source_jobs=[source_job_binding(repo,jid) for jid in job_ids],integration_contracts=recipe['integration_contracts'],
                **task_scope(task))
            configuration=child._configuration(child_profile,available)
            context={key:configuration[key] for key in EXECUTION_CONTEXT_KEYS}
            _require(context['policy_snapshot_sha256']==parents[0]['execution']['policy_revision'], 'BATCH_POLICY_DRIFT')
            runtime=child._materialize_runtime_snapshot(child_profile,context)
            _require(runtime is not None,'MATERIALIZATION_SNAPSHOT_REQUIRED')
            task.materialized_execution.update(execution_context=context,profile_revision=context['profile_snapshot_sha256'],runtime_snapshot_ref=str(runtime.root))
            provenance=source_process_identity(Path(__file__).resolve().parent)
            _require(provenance['source']==recipe['harness_source_identity'],'BATCH_HARNESS_SOURCE_CHANGED')
            task.materialized_execution.update(harness_source_identity=provenance['source'],harness_materializer_identity=provenance)
            admit_gate_execution(repo,task,binding=binding,expected_revision=expected_revisions[task.job_id])
            _require(child.acquire_execution_lease(),'CHILD_LEASE_BUSY')
            try:result=run_fresh_gates(child,task,baseline=baseline)
            finally:child.release_execution_lease()
            _require(content(promoter.canonical,profile,preserve)==current and module_heads(promoter.canonical,profile)==heads,
                     'BATCH_CANONICAL_CHANGED')
            with repo.hold():
                receipt=_receipt(repo,_key(request_id))
                receipt.update(batch_input_sha256=recipe['canonical_identity_sha256'],
                    batch_completed=result.get('success') is True,publication_performed=False,commit_performed=False)
                _write_receipt(repo,_key(request_id),receipt)
            return dict(replayed=False,result=result,batch_input_sha256=recipe['canonical_identity_sha256'],
                        publication_performed=False,commit_performed=False,worker_invoked=False)
    finally:
        if child is not None:child.close()
        control.harness.release_execution_lease()


def authorized_batch_test_actions(task):
    """Use individual original approved commands after durable Batch admission."""
    from types import SimpleNamespace
    from control_repository import ControlRepository, RepositoryError
    frozen=task.materialized_execution
    if frozen.get('role')!='BATCH_VERIFICATION':raise RepositoryError('BATCH_TEST_OWNER_REQUIRED')
    binding=frozen.get('revalidation_binding') or {}
    if binding.get('kind')!='BATCH_VERIFICATION' or task.test_plan.get('evaluation') is not None:
        raise RepositoryError('BATCH_NATIVE_TEST_CONTRACT_REQUIRED')
    contracts=binding.get('integration_contracts')
    if not isinstance(contracts,list) or len(contracts)!=2:raise RepositoryError('BATCH_PARENT_CONTRACTS_REQUIRED')
    # Read-only: this authority check runs inside existing gate transactions
    # (e.g. reviewer admission) that already hold the repository write lock.
    repo=ControlRepository(task.control_repository_path, read_only=True)
    with repo.hold():
        from canonical_revalidation import current_contract
        current_contract(repo,task)
        if [item.get('job_id') for item in contracts]!=[entry['job_id'] for entry in binding['source_jobs']]:
            raise RepositoryError('BATCH_TEST_PARENT_SET_CHANGED')
        by_test={}
        for contract in contracts:
            parent=repo.execution(contract.get('execution_id',''))
            if (not parent or parent.get('job_id')!=contract.get('job_id')
                    or digest(parent)!=contract.get('execution_sha256')
                    or parent.get('test_plan')!=contract.get('test_plan')
                    or parent['test_plan'].get('evaluation') is not None):
                raise RepositoryError('BATCH_TEST_PARENT_CONTRACT_CHANGED')
            from tester import _authorized_test_actions
            actions=_authorized_test_actions(SimpleNamespace(test_plan=copy.deepcopy(parent['test_plan']),materialized_execution={}))
            for action in actions:
                test,command=action['test'],action['command']
                if not command or test in by_test and by_test[test]['command']!=command:
                    raise RepositoryError('BATCH_TEST_COMMAND_UNBOUND_OR_CONFLICT')
                by_test[test]={**action,'source':'BOUND_PARENT_EXECUTION'}
        if set(by_test)!=set(task.test_plan.get('required_tests') or []):raise RepositoryError('BATCH_TEST_COVERAGE_CHANGED')
        return [by_test[name] for name in sorted(by_test)]
