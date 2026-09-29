"""Opt-in canonical finalization of fully isolated, quiesced executions.

Conservative integration rule: any canonical base drift requires revalidation.
No merge/rebase, gate synthesis, model choice or runtime authority lives here.
"""
from pathlib import Path
import uuid
import hashlib,json,threading
from canonical_ownership import canonical_ownership,CanonicalBusy
from hermes_boundary import snapshot,read,atomic
from qa_quarantine import QACandidateStore,CandidateError
from task_state import TaskState
from gate_evidence import identity,validate


def content(root, profile=None, preserve=()):
    from production_isolation import inventory
    return inventory(root, profile=profile, preserve=preserve)


def module_heads(root, profile):
    """Exact Git authority, separate from profile-filtered working bytes."""
    import subprocess
    from runtime_safety import git_subprocess_env, trusted_executable
    root = Path(root).resolve()
    result = {}
    for module in profile.modules:
        repo = root/module.path
        intended = next((parent for parent in (repo, *repo.parents)
                         if parent.is_relative_to(root) and (parent/'.git').exists()), None)
        if intended is None:
            raise CandidateError('CANDIDATE_GIT_OWNERSHIP_INVALID')
        command = [trusted_executable('git', forbidden_root=root), '-C', str(repo)]
        state = subprocess.run([*command, 'rev-parse', '--show-toplevel', '--absolute-git-dir',
                                '--git-common-dir', 'HEAD', '--symbolic-full-name', 'HEAD'],
            capture_output=True, timeout=30, env=git_subprocess_env())
        values = state.stdout.decode('utf-8').splitlines()
        if len(values) < 3 or any((repo/value).resolve() != expected.resolve()
                for value, expected in zip(values[:3], (intended, intended/'.git', intended/'.git'))):
            raise CandidateError('CANDIDATE_GIT_OWNERSHIP_INVALID')
        if state.returncode == 128 and values[3:] == ['HEAD']:
            # An initialized repository may have an unborn branch. Distinguish
            # it from a broken/detached HEAD instead of accepting a Git error.
            branch = subprocess.run([*command, 'symbolic-ref', '--quiet', 'HEAD'],
                capture_output=True, timeout=30, env=git_subprocess_env())
            ref = branch.stdout.decode('utf-8').strip()
            absent = subprocess.run([*command, 'show-ref', '--verify', '--quiet', ref],
                capture_output=True, timeout=30, env=git_subprocess_env())
            if branch.returncode or not ref.startswith('refs/heads/') or absent.returncode != 1:
                raise CandidateError('CANDIDATE_GIT_HEAD_INVALID')
            values = [*values[:3], '', ref]
        elif state.returncode or len(values) != 5:
            raise CandidateError('CANDIDATE_GIT_HEAD_INVALID')
        result[module.name] = {'head': values[3], 'branch': values[4]}
    return result

def commit_stage_for_policy(commit_policy: str) -> dict:
    """F05: the stage receipt for a promoted candidate under a commit policy.

    BATCH_FINAL_COMMIT defers the single authorized Git commit to the existing
    Batch finalization point; the isolated promotion must not advance HEAD.
    """
    if commit_policy == 'BATCH_FINAL_COMMIT':
        return {'commit_policy': commit_policy,
                'commit_status': 'DEFERRED_TO_BATCH_FINAL'}
    if commit_policy == 'PER_JOB':
        return {'commit_policy': commit_policy, 'commit_status': ''}
    raise CandidateError('COMMIT_POLICY_INVALID', commit_policy)


class IsolatedCanonicalPromotion:
    def __init__(self,canonical,root,repository):
        self.canonical=Path(canonical).resolve();self.root=Path(root).resolve()
        self.root.mkdir(parents=True,exist_ok=True);self.repository=repository
        self.allocations={};self.guard=threading.Lock()
    def _resolve_rolled_back_intent(self, store, path):
        intent = read(path)
        if intent.get('state') != 'PUBLISHING':
            return intent.get('state') == 'ROLLED_BACK'
        publication_id = intent.get('publication_id')
        if not publication_id:
            # Pre-hotfix intents have no journal ID. Admit only one exact
            # execution-bound journal; ambiguity keeps publication fenced.
            matches = []
            for journal_path in store._publications_dir().glob('PUB-*.json'):
                journal = store.load_publication(journal_path.stem)
                authority = journal.get('authority') or {}
                if (authority.get('job_id') == intent.get('job_id')
                        and authority.get('execution_id') == intent.get('execution_id')):
                    matches.append(journal_path.stem)
            if len(matches) != 1:
                return False
            publication_id = matches[0]
        try:
            journal = store.load_publication(publication_id)
            authority = journal.get('authority') or {}
            if (authority.get('job_id') != intent.get('job_id')
                    or authority.get('execution_id') != intent.get('execution_id')
                    or (intent.get('candidate_id') and journal.get('candidate_ids') != [intent['candidate_id']])):
                return False
            proof = store.recover_publication(publication_id)
        except (OSError, ValueError, CandidateError):
            return False
        if proof.get('state') != 'COMPLETED' or proof.get('disposition') != 'ROLLED_BACK':
            return False
        atomic(path, {**intent, 'publication_id': publication_id, 'state': 'ROLLED_BACK'})
        return True
    def recover(self):
        # Called under the dispatch execution lease; recovery owns the same
        # canonical lock as promotion and never synthesizes a Job success.
        with canonical_ownership(self.canonical):
            store=QACandidateStore(self.root/'candidates',self.canonical,None)
            unresolved=store.unresolved_publications()
            for publication_id in unresolved:
                store.recover_publication(publication_id)
            uncertain=[]
            for path in self.root.glob('*-intent.json'):
                state = read(path).get('state')
                if state == 'PUBLISHING' and self._resolve_rolled_back_intent(store, path):
                    continue
                if state not in {'PUBLISHED', 'ROLLED_BACK'}:
                    uncertain.append(path)
            if store.unresolved_publications() or uncertain:
                self.repository.set_global_stop('CANONICAL_PUBLICATION_RECOVERY_REQUIRED')
                raise CandidateError('CANONICAL_PUBLICATION_RECOVERY_REQUIRED')

    def register(self,job,service,source):
        source=Path(source).resolve();candidate=Path(service.working_dir).resolve()
        if candidate==self.canonical or source==self.canonical or source==candidate:
            raise CandidateError('ISOLATED_SOURCE_VIEW_REQUIRED')
        profile,_=service._load_profile()
        preserve=[*job.get("request",{}).get("target_resources",[]), *job.get("request",{}).get("test_plan",{}).get("required_tests",[])]
        base=content(source,profile,preserve)
        if content(candidate,profile,preserve)!=base:raise CandidateError('CANDIDATE_BASE_MISMATCH')
        heads=module_heads(source,profile)
        if module_heads(candidate,profile)!=heads:raise CandidateError('CANDIDATE_BASE_MISMATCH')
        with self.guard:
            if job['job_id'] in self.allocations:raise CandidateError('CANDIDATE_ALREADY_REGISTERED')
            self.allocations[job['job_id']]={'service':service,'source':source,'base':base,'profile':profile,'preserve':preserve,
                'full_base': base, 'source_heads': heads, 'canonical_heads': heads}
        atomic(self.root/(job['job_id']+'-base.json'),{'job_id':job['job_id'],'candidate':str(candidate),'source':str(source),'base':base,'source_heads':heads})

    def register_preserved(self, job, service, source, *, authorized_head_refresh=False):
        """Rebind a closed, preserved allocation without accepting its bytes."""
        from production_isolation import normalized_preserved_inventory
        source = Path(source).resolve()
        candidate = Path(service.working_dir).resolve()
        persisted = read(self.root / (job["job_id"] + "-base.json"))
        if (
            persisted.get("job_id") != job["job_id"]
            or Path(str(persisted.get("candidate", ""))).resolve() != candidate
            or Path(str(persisted.get("source", ""))).resolve() != source
            or source == candidate
            or candidate == self.canonical
        ):
            raise CandidateError("CANDIDATE_OWNERSHIP_MISMATCH")
        profile, _ = service._load_profile()
        preserve = [
            *job.get("request", {}).get("target_resources", []),
            *job.get("request", {}).get("test_plan", {}).get("required_tests", []),
        ]
        base = content(source, profile, preserve)
        persisted_base = normalized_preserved_inventory(
            source, persisted.get("base") or {}, profile=profile, preserve=preserve
        )
        if base != persisted_base:
            raise CandidateError("CANDIDATE_BASE_MISMATCH")
        canonical_base = content(self.canonical, profile, preserve)
        if canonical_base != base:
            if not authorized_head_refresh:
                raise CandidateError("CANDIDATE_BASE_STALE")
            changed = list(dict(job.get("last_result") or {}).get("changed_files") or [])
            if not changed or any(canonical_base.get(rel, "DELETED") != base.get(rel, "DELETED")
                                  for rel in changed):
                raise CandidateError("CANDIDATE_BASE_STALE")
        source_heads = module_heads(source, profile)
        canonical_heads = module_heads(self.canonical, profile)
        if (persisted.get('source_heads', source_heads) != source_heads
                or module_heads(candidate, profile) != source_heads):
            raise CandidateError('CANDIDATE_BASE_MISMATCH')
        if not authorized_head_refresh and canonical_heads != source_heads:
            raise CandidateError('CANDIDATE_BASE_STALE')
        with self.guard:
            if job["job_id"] in self.allocations:
                raise CandidateError("CANDIDATE_ALREADY_REGISTERED")
            self.allocations[job["job_id"]] = {
                "service": service,
                "source": source,
                "base": base,
                "profile": profile,
                "preserve": preserve,
                "source_full_base": base,
                "source_heads": source_heads,
                "canonical_heads": canonical_heads,
                "full_base": canonical_base,
            }
    def _task(self,job,service,result):
        a=self.allocations[job['job_id']]
        current_source = content(a['source'], a['profile'], a['preserve'])
        if (a['service'] is not service
                or current_source != a.get('source_full_base', a['full_base'])
                or module_heads(a['source'], a['profile']) != a['source_heads']):
            raise CandidateError('CANDIDATE_OWNERSHIP_MISMATCH')
        task=TaskState.load(result['task_id'],service.task_root)
        if task.job_id!=job['job_id'] or task.execution_id!=job['materialized_execution']['execution_id']:
            raise CandidateError('CANDIDATE_EXECUTION_MISMATCH')
        held = a.get('held_verification')
        if held:
            if (task.candidate_id != held['quarantine_candidate_id']
                    or content(service.working_dir,a['profile'],a['preserve']) != held['verified_input']
                    or task.gate_evidence != held['gate_evidence']):
                raise CandidateError('QA_VERIFICATION_INPUT_CHANGED')
            # Explicit Harness alias: frozen runtime candidate -> quarantine bundle.
            # Keep the original proof authority; bytes and bundle hashes are checked.
            QACandidateStore(self.root/'candidates',service.working_dir,None).load(task.candidate_id)
            task.candidate_id = held['runtime_candidate_id']
            task.candidate_manifest = held['runtime_manifest']
        allocation=Path(task.candidate_manifest)
        record=read(allocation);cleanup=read(allocation.parent/'cleanup.json')
        if record['execution_id']!=task.execution_id or record['job_id']!=task.job_id or cleanup.get('state')!='QUIESCED':
            raise CandidateError('CANDIDATE_WRITER_NOT_QUIESCED')
        if record.get('candidate_id') != task.candidate_id:
            raise CandidateError('CANDIDATE_OWNERSHIP_MISMATCH')
        custody=getattr(service,'isolated_execution',None)
        if custody is not None:
            custody.verify_origin()
        if custody is not None and (allocation.resolve()!=custody.root/'allocation.json'
                or cleanup.get('boundary') not in {'docker-opencode/1', 'harness-current-input/1'}):
            raise CandidateError('CANDIDATE_CUSTODY_PROOF_INVALID')
        if cleanup.get('boundary') == 'harness-current-input/1':
            from canonical_revalidation import verify_custody
            verify_custody(task, service, record, cleanup)
        if cleanup.get('boundary') == 'docker-opencode/1':
            expected = {'execution_id':task.execution_id, 'job_id':task.job_id,
                'candidate_id':task.candidate_id, 'attempt_id':task.attempt_id, 'execution_epoch':task.execution_epoch}
            if (any(cleanup.get(k) != v for k,v in expected.items())
                    or cleanup.get('quiesced') is not True or cleanup.get('imported') is not True
                    or cleanup.get('host_input') != content(service.working_dir,a['profile'],a['preserve'])):
                raise CandidateError('CANDIDATE_CUSTODY_PROOF_INVALID')
        # This is a new isolated-execution snapshot, never the historical Harness baseline.
        dest=self.root/(task.execution_id+'-base');dest.mkdir(exist_ok=True)
        records=[]
        for rel in task.task_owned_changed_files or task.changed_files:
            src=(a['source']/rel).resolve()
            if not src.is_relative_to(a['source']):raise CandidateError('CANDIDATE_PATH_ESCAPE')
            sha=a['base'].get(rel,'DELETED');rec={'path':rel,'sha256':sha,'state':'DELETED','snapshot':''}
            if sha!='DELETED':
                target=dest/'files'/sha;target.parent.mkdir(exist_ok=True)
                if not target.exists():target.write_bytes(src.read_bytes())
                rec.update(state='PRESENT',snapshot='files/'+sha)
            records.append(rec)
        manifest=dest/'manifest.json'
        atomic(manifest,{'kind':'PRE_JOB_SNAPSHOT','snapshot_id':task.execution_id,'job_id':task.job_id,'files':records,'provenance':'ISOLATED_EXECUTION_SOURCE_VIEW'})
        task.pre_job_snapshot_id=task.execution_id;task.pre_job_snapshot_manifest=str(manifest)
        return task,a
    def _bundle(self,task,service):
        profile,_=service._load_profile()
        return QACandidateStore(self.root/'candidates',service.working_dir,profile).create(task)
    def _hold(self,task,service,result,candidate,code,qa_type='CANDIDATE_DECISION'):
        if code == 'USER_QA_REQUIRED':
            from copy import deepcopy
            held = {'runtime_candidate_id': task.candidate_id,
                    'runtime_manifest': task.candidate_manifest,
                    'quarantine_candidate_id': candidate['candidate_id'],
                    'verified_input': content(service.working_dir,self.allocations[task.job_id]['profile'],self.allocations[task.job_id]['preserve']),
                    'gate_evidence': deepcopy(task.gate_evidence),
                    'machine_result': deepcopy(result)}
            self.allocations[task.job_id]['held_verification'] = held
            from hermes_boundary import write_once
            write_once(self.root/(task.execution_id+'-qa-verification.json'),held)
        task.status='AWAITING_QA';task.stage='AWAITING_QA';task.qa_type=qa_type;task.hold_scope='JOB' 
        task.machine_verified=True;task.failure_code=code;task.failure_origin='USER_QA'
        task.candidate_id=candidate['candidate_id'];task.candidate_manifest=candidate['manifest_path']
        task.worktree_disposition='ISOLATED_CANDIDATE_PRESERVED';task.save(service.task_root)
        return {**result,'success':False,'status':'AWAITING_QA','stage':'AWAITING_QA','failure_code':code,
            'failure_origin':'USER_QA','qa_type':qa_type,'hold_scope':'JOB','machine_verified':True,
            'candidate_id':candidate['candidate_id'],'candidate_manifest':candidate['manifest_path'],
            'worktree_disposition':task.worktree_disposition,'strict_machine_verification':True}
    def quarantine(self,job,service,result):
        task,a=self._task(job,service,result)
        if not QACandidateStore._integration_passed({**result,'failure_code':''}) or validate(task,identity(task,service.working_dir),require_all=True):
            raise CandidateError('QA_MACHINE_GATES_REQUIRED')
        qa=dict(result.get('qa_request') or {})
        task.qa_type=qa.get('qa_type') or job.get('qa_type') or 'HUMAN_ACCEPTANCE'
        task.hold_scope=qa.get('hold_scope') or job.get('hold_scope') or 'JOB';task.machine_verified=True
        candidate=self._bundle(task,service)
        return self._hold(task,service,result,candidate,'USER_QA_REQUIRED',task.qa_type)
    def __call__(self,job,service,result):
        task,a=self._task(job,service,result)
        previous_intent = self.root/(task.execution_id+'-intent.json')
        if previous_intent.exists() and read(previous_intent).get('state') != 'ROLLED_BACK':
            raise CandidateError('CANONICAL_PUBLICATION_REPLAY_DENIED')
        if not QACandidateStore._integration_passed(result) or validate(task,identity(task,service.working_dir),require_all=True):
            raise CandidateError('CANONICAL_MACHINE_GATES_REQUIRED')
        from reviewer_successor import verify_accepted_review
        # _task already checked this existing host-owned quarantine alias,
        # identical bytes, bundle custody and the frozen gate receipts.
        verify_accepted_review(self.repository, task,
            candidate_alias=(a.get('held_verification') or {}).get('quarantine_candidate_id', ''))
        task.qa_type='CANDIDATE_DECISION';task.hold_scope='JOB';task.machine_verified=True
        held = a.get('held_verification')
        candidate = (QACandidateStore(self.root/'candidates',service.working_dir,None).load(held['quarantine_candidate_id'])
                     if held else self._bundle(task,service))
        from runtime_snapshot import load_frozen_runtime_snapshot
        frozen=load_frozen_runtime_snapshot(service.snapshot_root,service.working_dir,job['execution_context'])
        store=QACandidateStore(self.root/'candidates',self.canonical,frozen.profile)
        # F05: the user-selected batch policy is authoritative.  Under
        # BATCH_FINAL_COMMIT the isolated promotion stages the accepted
        # canonical delta but must not advance Git HEAD per Job; the one
        # authorized commit happens at the existing Batch finalization point.
        control_policy=getattr(frozen.profile,'control_policy',None)
        commit_policy=str(getattr(control_policy,'commit_policy','PER_JOB') or 'PER_JOB')
        # Fail closed on unknown policies (never silently commit per Job).
        commit_stage_for_policy(commit_policy)
        def commit_accepted(job,result):
            stage=commit_stage_for_policy(commit_policy)
            if commit_policy=='BATCH_FINAL_COMMIT':
                return {**result,**stage}
            from git_committer import JobCommitter
            try:
                return {**result,**stage,'canonical_commit':JobCommitter(self.canonical,frozen.profile).commit(job)}
            except Exception:
                # Git may have committed before losing its receipt. Never rollback/guess that state.
                self.repository.set_global_stop('CANONICAL_COMMIT_DISPOSITION_UNCERTAIN')
                raise
        try:
            with canonical_ownership(self.canonical):
                current_canonical = content(self.canonical, a['profile'], a['preserve'])
                if (current_canonical != a['full_base']
                        or module_heads(self.canonical, a['profile']) != a['canonical_heads']):
                    return self._hold(task,service,result,candidate,'CANDIDATE_FOCUSED_REBASE_REQUIRED')
                verified=content(service.working_dir,a['profile'],a['preserve'])
                def integration(changed):
                    if content(self.canonical,a['profile'],a['preserve'])!=verified or validate(task,identity(task,self.canonical),require_all=True):
                        raise CandidateError('CANONICAL_GATE_INPUT_CHANGED')
                    from build_test_runner import BuildTestRunner
                    build=BuildTestRunner(str(self.canonical),profile=frozen.profile,evidence_root=self.root/'integration').run(
                        changed_files=changed,target_modules=task.target_module,authoritative_scope=True)
                    if not build.success or build.build_status!='PASS' or content(self.canonical,a['profile'],a['preserve'])!=verified:
                        raise CandidateError('CANONICAL_INTEGRATION_BUILD_FAILED')
                    # Native Tester/Reviewer evidence is reused only for byte-identical complete inputs.
                    return {**result,'integration_build':build.details,'gate_reuse':'IDENTICAL_FULL_PRODUCT_INPUT',
                        'gate_evidence':task.gate_evidence,'canonical_content':verified}
                from hermes_boundary import write_once
                intent=self.root/(task.execution_id+'-intent.json')
                publication_id='PUB-'+uuid.uuid4().hex.upper()
                pending = {'job_id':task.job_id,'execution_id':task.execution_id,
                           'candidate_id':candidate['candidate_id'],
                           'publication_id':publication_id,'state':'PUBLISHING'}
                if intent.exists():
                    prior = read(intent)
                    if (prior.get('state') != 'ROLLED_BACK' or prior.get('job_id') != task.job_id
                            or prior.get('execution_id') != task.execution_id):
                        raise CandidateError('CANONICAL_PUBLICATION_REPLAY_DENIED')
                    pending['rolled_back_publications'] = [
                        *prior.get('rolled_back_publications', []), prior['publication_id']]
                    atomic(intent, pending)
                else:
                    try:
                        write_once(intent, pending)
                    except FileExistsError:
                        raise CandidateError('CANONICAL_PUBLICATION_REPLAY_DENIED') from None
                try:
                    promotion=store.promote([candidate['candidate_id']],integration,
                        authority={'job_id':job['job_id'],'execution_id':task.execution_id,
                                   'contract_revision':(task.test_plan_amendment or task.materialized_execution).get('contract_revision')},
                        publication_id=publication_id)
                except Exception:
                    self._resolve_rolled_back_intent(store, intent)
                    raise
                commit=commit_accepted({**job,'status':'SUCCEEDED','last_result':result},result)
                staged = commit.get('commit_status')=='DEFERRED_TO_BATCH_FINAL'
                atomic(self.root/(task.execution_id+'-promotion.json'),{'promotion':promotion,'commit':commit})
                atomic(intent, {**read(intent), 'state':'PUBLISHED'})
                task.candidate_id=candidate['candidate_id'];task.candidate_manifest=candidate['manifest_path']
                task.qa_type='';task.hold_scope=''
                # F15: the worktree disposition must not imply a Git commit
                # that has not happened; batch mode reports a truthful staged
                # state and keeps the single authorized commit at the Batch
                # finalization point.
                task.worktree_disposition='BATCH_COMMIT_PENDING' if staged else 'CANONICAL_COMMITTED';task.save(service.task_root)
                return {**result,'canonical_promotion':promotion,'canonical_commit':commit.get('canonical_commit'),
                    'commit_status':commit.get('commit_status',''),
                    'worktree_disposition':task.worktree_disposition,'candidate_id':candidate['candidate_id']}
        except CanonicalBusy:
            return self._hold(task,service,result,candidate,'CANONICAL_PROMOTION_BUSY')

    def resolve_human(self, job_store, job_id, *, decision_id, expected_revision):
        job = job_store.get_job(job_id)
        if job['status'] != 'AWAITING_QA' or job['revision'] != expected_revision:
            raise CandidateError('QA_DECISION_STATE_CHANGED')
        choices = [d for d in self.repository.decisions(job_id, status='RESOLVED')
                   if d['decision_id'] == decision_id]
        if len(choices) != 1:
            raise CandidateError('QA_DECISION_REQUIRED')
        decision = choices[0]
        authority = next(row for row in self.repository.worklist() if row['job_id'] == job_id)
        if (decision.get('decision_type') != 'CANDIDATE'
                or decision.get('candidate_id') != job.get('candidate_id')
                or decision.get('contract_revision') != authority['contract_revision']
                or decision.get('resolution') not in {'HUMAN_ACCEPT', 'HUMAN_REJECT'}):
            raise CandidateError('QA_DECISION_BINDING_INVALID')
        allocation = self.allocations[job_id]
        held = allocation.get('held_verification')
        if not held or held['quarantine_candidate_id'] != job['candidate_id']:
            raise CandidateError('QA_VERIFICATION_RECEIPT_REQUIRED')
        service = allocation['service']
        frozen_task = TaskState.load(held['machine_result']['task_id'], service.task_root)
        with self.repository.hold():
            row = self.repository._connection.execute(
                'SELECT payload FROM executions WHERE execution_id=? AND job_id=?',
                (frozen_task.execution_id, job_id)).fetchone()
            frozen = json.loads(row[0]) if row else None
        overlay = job.get('test_plan_amendment') or {}
        approved_revision = overlay.get('contract_revision') if overlay else (frozen or {}).get('contract_revision')
        if (not frozen or frozen.get('job_id') != job_id or approved_revision != authority['contract_revision']
                or (overlay and (overlay != frozen_task.test_plan_amendment
                    or overlay.get('execution_id') != frozen['execution_id']
                    or overlay.get('candidate_id') != held['runtime_candidate_id']))):
            raise CandidateError('QA_MATERIALIZATION_MISMATCH')
        # Same frozen-contract enrichment the dispatch supervisor applies before
        # promotion: the materialized execution context is the snapshot authority.
        job = {**job, 'materialized_execution': frozen, 'execution_context': frozen['execution_context'],
               'request': job['request'] if overlay else frozen['request'], 'current_requirement': frozen['current_requirement'],
               'contract_revision': authority['contract_revision']}
        # One canonical owner spans decision, revalidation, apply, and Job receipt.
        try:
            with canonical_ownership(self.canonical), self.repository.hold():
                current = job_store.get_job(job_id)
                if current['revision'] != expected_revision or current['status'] != 'AWAITING_QA':
                    raise CandidateError('QA_DECISION_STATE_CHANGED')
                if decision['resolution'] == 'HUMAN_REJECT':
                    candidates = QACandidateStore(self.root/'candidates',self.canonical,None)
                    candidate = candidates.load(job['candidate_id'])
                    if candidate['state'] != 'QUARANTINED':
                        raise CandidateError('QA_CANDIDATE_NOT_QUARANTINED')
                    candidate.update(state='REJECTED', human_decision_id=decision_id)
                    path = Path(candidate.pop('manifest_path'))
                    candidates._write_manifest(path.parent,candidate)
                    result = {**job['last_result'], 'success':False, 'status':'CANCELLED',
                              'failure_code':'HUMAN_REJECTED', 'worktree_disposition':'REJECTED_ISOLATED_CANDIDATE_PRESERVED'}
                else:
                    # Original result is a frozen machine receipt, never a human PASS.
                    result = self(job,service,held['machine_result'])
                    if result.get('worktree_disposition') not in {'CANONICAL_COMMITTED','BATCH_COMMIT_PENDING'}:
                        return {'status':'AWAITING_QA','success':False,'result':result}
                    task = TaskState.load(result['task_id'],service.task_root)
                    task.status='SUCCESS';task.stage='DONE';task.failure_code='';task.failure_origin=''
                    task.save(service.task_root)
                result = {**result,'qa_decision_id':decision_id,'candidate_id':job['candidate_id']}
                return job_store.record_human_candidate_result(job_id, expected_revision=expected_revision,
                                                              decision_id=decision_id, result=result)

        except Exception:
            self.repository.set_global_stop('QA_FINALIZATION_UNCERTAIN')
            raise
