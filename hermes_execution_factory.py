"""Explicit, opt-in Service factory for one isolated Hermes execution.

No runtime discovery, global defaults, canonical writes or Job terminal authority.
Each role invocation owns a separate admitted binding/epoch and quiescence proof.
"""
import atexit
import json
import os
import secrets
import subprocess
import threading
import time
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path

from control_repository import ControlRepository
from execution_context import ExecutionContextContract, build_execution_context_pack, materialize_execution_context
from hermes_boundary import HermesBoundaryOwner, DOCKER, atomic, read, sha, snapshot, write_once
from hermes_identity import ProviderModelIdentity
# Native process creation is invocation-local, not an import-time requirement.
def OwnedProcess(*args, **kwargs):
    if os.name != 'nt':
        raise HermesError('LOCAL_WINDOWS_REQUIRED')
    from hermes_process_boundary import OwnedProcess as NativeOwnedProcess
    return NativeOwnedProcess(*args, **kwargs)

def identity(pid):
    if os.name != 'nt':
        raise HermesError('LOCAL_WINDOWS_REQUIRED')
    from hermes_process_boundary import identity as native_identity
    return native_identity(pid)
from hermes_runtime_adapter import HermesBinding, HermesRuntimeAdapter, HermesError
from hermes_worker import HermesWorker, HermesReviewer

RUNTIME = Path(r'D:\hermes\runtime-v2026.9.14')
HOME = Path(r'D:\hermes\home')

def candidate_criteria(task,requirement):
    return json.dumps({'candidate_worker_attempt_id':task.attempt_id,
        'worker_cycle':task.retry_count+1,'acceptance':requirement},ensure_ascii=False,sort_keys=True)

@dataclass(frozen=True)
class HermesExecutionConfig:
    allocation_root: Path
    source: Path
    proxy: Path
    image: str
    fingerprints: dict
    port: int = 18646
    tester_model: str = ""
    tester_reasoning_effort: str = "low"
    tester_technical_retries: int = 1

class HermesExecutionFactory:
    def __init__(self, config, *, approval_authorizer=None, shared_gateway=None):
        self.config = config
        self.shared_gateway = shared_gateway
        self.approval_authorizer = approval_authorizer
        self._guard = threading.Lock()
        self._scope = None

    def tester_policy(self):
        model=ProviderModelIdentity.parse(self.config.tester_model)
        cap=self.config.tester_technical_retries
        if type(cap) is not int or not 0 <= cap <= 1:
            raise HermesError('HERMES_TESTER_RETRY_POLICY_INVALID')
        return {'runtime':'hermes','model':model.qualified,
            'reasoning_effort':self.config.tester_reasoning_effort,'technical_retries':cap}

    def bind(self, task, *, candidate, profile, policy_root):
        if task.worker != 'hermes':
            raise HermesError('HERMES_RUNTIME_SELECTION_MISMATCH')
        with self._guard:
            if self._scope is not None:
                raise HermesError('HERMES_EXECUTION_OWNER_BUSY')
            scope = ExecutionScope(self, task, candidate, profile, policy_root)
            self._scope = scope
            return scope

class ExecutionScope:
    def __init__(self, factory, task, candidate, profile, policy_root):
        self.factory, self.config, self.task = factory, factory.config, task
        self.candidate = Path(candidate).resolve()
        self.profile, self.policy_root = profile, Path(policy_root) if policy_root else None
        self.root = Path(self.config.allocation_root).resolve()
        self.source = Path(self.config.source).resolve()
        self.owners, self.logs = [], []
        self.closed = False
        self._config_bytes = None
        self._attempts = set()
        if (not self.candidate.is_dir() or not self.source.is_dir()
                or self.source.is_relative_to(self.candidate) or self.candidate.is_relative_to(self.source)):
            raise HermesError('HERMES_CANDIDATE_ALLOCATION_REQUIRED')
        for p in (self.candidate, self.source, Path(task.control_repository_path).resolve()):
            if not p.is_relative_to(self.root) or p == self.root:
                raise HermesError('HERMES_ISOLATED_ALLOCATION_REQUIRED')
        if not self.policy_root or not task.materialized_execution or not task.attempt_id:
            raise HermesError('HERMES_EXECUTION_CONTEXT_REQUIRED')
        self.repository = ControlRepository(task.control_repository_path)
        ex = self.repository.active_execution(task.job_id)
        if not ex or ex != task.materialized_execution or ex['execution_id'] != task.execution_id:
            raise HermesError('HERMES_MATERIALIZATION_MISMATCH')
        self.provider_model = ProviderModelIdentity.parse(task.hermes_model)
        self.source_before = snapshot(self.source)
        snapshot(self.candidate)  # rejects links/secret files before any mount
        self.path = self.root / ('owner-' + uuid.uuid4().hex)
        self.path.mkdir()
        write_once(self.path/'allocation.json', {'job_id':task.job_id,'execution_id':task.execution_id,
            'candidate_id':'HCAND-'+uuid.uuid4().hex,'candidate':str(self.candidate),'source':str(self.source)})
        self.candidate_id = read(self.path/'allocation.json')['candidate_id']
        task.candidate_id = self.candidate_id
        task.candidate_manifest = str(self.path/'allocation.json')
        atexit.register(self.close)

    def _check_runtime(self):
        required={str(RUNTIME/rel) for rel in ('tools/environments/docker.py','tools/credential_files.py','tools/terminal_tool_backends.py','tools/code_execution_tool.py','tools/file_tools.py','pyproject.toml','hermes_cli/__init__.py')} | {DOCKER}
        if set(self.config.fingerprints)!=required or not str(self.config.image).startswith('sha256:'):
            raise HermesError('HERMES_PINNED_RUNTIME_REQUIRED')
        for path, expected in self.config.fingerprints.items():
            p = Path(path).resolve()
            if not (p.is_relative_to(RUNTIME) or p == Path(DOCKER)) or p.name in {'.env','auth.json','config.yaml'}:
                raise HermesError('HERMES_FINGERPRINT_PATH_DENIED')
            if not p.is_file():
                raise HermesError('HERMES_PINNED_RUNTIME_UNAVAILABLE')
            if sha(p) != expected:
                raise HermesError('RUNTIME_FINGERPRINT_MISMATCH')
        if not Path(self.config.proxy).is_file():
            raise HermesError('HERMES_PROXY_UNAVAILABLE')
        try:
            subprocess.run([DOCKER,'image','inspect',self.config.image],check=True,capture_output=True,timeout=15)
        except (OSError,subprocess.SubprocessError):
            raise HermesError('HERMES_DOCKER_UNAVAILABLE') from None

    def _gateway_configuration(self):
        # Existing user configuration is never read or overwritten. Installation
        # currently has no config.yaml; this exclusive temporary, model-free
        # configuration is removed only if its own bytes remain unchanged.
        if self._config_bytes is not None:
            return
        platforms='local telegram discord whatsapp whatsapp_cloud slack signal mattermost matrix homeassistant email sms dingtalk webhook msgraph_webhook feishu wecom wecom_callback weixin bluebubbles qqbot yuanbao relay'.split()
        cfg='platforms:\n'+''.join('  '+p+':\n    enabled: false\n' for p in platforms)
        cfg+='  api_server:\n    enabled: true\n    extra:\n      host: 127.0.0.1\n      port: '+str(self.config.port)+'\n'
        cfg+='gateway:\n  api_server:\n    max_concurrent_runs: 1\napprovals:\n  mode: manual\nplatform_toolsets:\n  api_server: [terminal, file]\nagent:\n  max_turns: 12\nfallback_models: []\n'
        try:
            with (HOME/'config.yaml').open('x',encoding='utf8',newline='') as f:
                f.write(cfg)
        except FileExistsError:
            raise HermesError('HERMES_EXISTING_CONFIG_REQUIRES_EXPLICIT_INTEGRATION') from None
        self._config_bytes=cfg.encode('utf8')
        write_once(self.path/'gateway-config.json',{'temporary':True,'config':cfg,'global_model':False})

    def authorize_rollback_handoff(self,task):
        # Called by trusted Manager only after its checkpoint rollback succeeded.
        if task is not self.task or not task.is_rolled_back or not self.owners:
            raise HermesError('HERMES_ROLLBACK_HANDOFF_DENIED')
        previous=self.owners[-1]
        proof=read(previous.root/'quiescence.json')
        old=read(previous.lock)
        if (proof.get('state')!='QUIESCED' or old.get('state')!='QUIESCED'
                or proof.get('binding')!=asdict(previous.binding)
                or old.get('binding')!=asdict(previous.binding)):
            raise HermesError('WRITER_QUIESCENCE_UNKNOWN')
        current=snapshot(self.candidate)
        write_once(self.path/('rollback-handoff-'+uuid.uuid4().hex+'.json'),{
            'previous_binding':asdict(previous.binding),'reason':'HARNESS_VERIFIED_CHECKPOINT_ROLLBACK',
            'checkpoint_id':task.checkpoint_id,'restored_checkpoint_id':task.restored_checkpoint_id,
            'snapshot':current})
        atomic(previous.lock,{**old,'candidate_snapshot':current})

    def _handoff(self):
        if not self.owners:
            return
        previous=self.owners[-1]
        proof=read(previous.root/'quiescence.json')
        if proof.get('state')!='QUIESCED' or proof.get('binding')!=asdict(previous.binding):
            raise HermesError('WRITER_QUIESCENCE_UNKNOWN')
        if read(previous.root/'state.json').get('state')!='FENCING':
            raise HermesError('STALE_EPOCH')
        old=read(previous.lock)
        if old['state']!='QUIESCED' or old['binding']!=asdict(previous.binding):
            raise HermesError('STALE_EPOCH')
        current=snapshot(self.candidate)
        def content(rows):return {k:v['sha256'] for k,v in rows.items() if not k.startswith('.git/')}
        if content(current)!=content(old['candidate_snapshot']):
            raise HermesError('CANDIDATE_CHANGED_AFTER_QUIESCENCE')
        # Service's serial Manager may stage/read Git between gates. Preserve
        # product bytes and record this explicit Harness metadata handoff.
        if current!=old['candidate_snapshot']:
            write_once(self.path/('handoff-'+uuid.uuid4().hex+'.json'),{'previous_binding':asdict(previous.binding),'reason':'HARNESS_GIT_METADATA_ONLY','snapshot':current})
            atomic(previous.lock,{**old,'candidate_snapshot':current})

    def _create(self, task, role, *, requirement, changed_files=(), git_diff='', tester_context=''):
        if self.closed or task is not self.task:
            raise HermesError('HERMES_FOREIGN_TASK')
        self._check_runtime()
        self._handoff()
        aid=getattr(task,{'WORKER':'attempt_id','REVIEWER':'reviewer_attempt_id','TESTER':'tester_attempt_id'}[role])
        if not aid or aid in self._attempts:
            raise HermesError('HERMES_FRESH_ATTEMPT_REQUIRED')
        role_execution_id = (
            (getattr(task, 'reviewer_execution', None) or {}).get('execution_id')
            if role == 'REVIEWER' else None
        ) or task.execution_id
        latest=self.repository.latest_attempt(role_execution_id)
        if not latest or latest['attempt_id']!=aid or latest['role']!=role:
            raise HermesError('FOREIGN_ATTEMPT')
        model=ProviderModelIdentity.parse(latest['model'])
        policy=dict(task.effective_policy.get('resolved_reviewer',{}))
        if role=='REVIEWER':policy=dict(task.review_policy or policy)
        if role=='TESTER':policy=dict(task.effective_policy['resolved_tester'])
        risk=str(policy.get('risk_level','LOW'))
        scope=tuple(task.materialized_execution['request'].get('target_resources') or ())
        if not scope:
            raise HermesError('HERMES_EXPLICIT_WRITE_SCOPE_REQUIRED')
        approved_requirement=task.materialized_execution['request']['requirement']
        # F04: approved frozen impact evidence comes only from the immutable
        # Job contract (operator-declared), never from model prose.  HIGH or
        # CRITICAL contexts without it fail admission right here, before any
        # runtime allocation, with the existing explicit error.
        impact=tuple(str(item) for item in (task.materialized_execution['request'].get('impact_evidence') or []) if str(item).strip())
        phase=self.path/(role.lower()+'-'+uuid.uuid4().hex);phase.mkdir()
        contract=ExecutionContextContract(task.job_id,role_execution_id,aid,self.candidate_id,
            self.profile.id,role,risk,tuple(task.target_module),scope,(approved_requirement,),
            (json.dumps(task.materialized_execution.get('verification_contract',{}),sort_keys=True),),
            impact_evidence=impact,
            context_tier=str(policy.get('context_tier','DIFF_ONLY')))
        from execution_context import _portable_document
        verification_text=_portable_document(tester_context or json.dumps(task.build_evidence,default=str))
        review={'candidate_id':self.candidate_id,'diff':git_diff,'verification':verification_text,'criteria':candidate_criteria(task,approved_requirement)} if role=='REVIEWER' else None
        tester={'candidate_id':self.candidate_id,'build':verification_text,'test_plan':_portable_document(json.dumps(task.test_plan,sort_keys=True)),'criteria':candidate_criteria(task,approved_requirement)} if role=='TESTER' else None
        pack=build_execution_context_pack(contract=contract,requirement=approved_requirement,
            profile_dir=self.profile.profile_dir,workspace=self.candidate,policy_root=self.policy_root,
            changed_files=tuple(changed_files),review_evidence=review,tester_evidence=tester,
            implementation_context=_portable_document(json.dumps({'worker_cycle':task.retry_count+1,
                'previous_failure':task.failure_reason,'tester_evidence':task.test_evidence,
                'review_findings':task.review_result},default=str)) if role=='WORKER' else '', 
            schema_file=self.policy_root.parent.parent/'schema'/'project-profile.schema.json')
        materialize_execution_context(pack,phase/'context')
        task.context_manifest_path=str(phase/'context'/'manifest.json')
        task.context_pack_id=pack.manifest['pack_sha256']
        binding=HermesBinding(task.job_id,role_execution_id,aid,'HB-'+uuid.uuid4().hex,'HC-'+uuid.uuid4().hex,
            self.candidate_id,pack.manifest['pack_sha256'],'EPOCH-'+uuid.uuid4().hex,'INC-'+uuid.uuid4().hex,
            model.provider,model.model,role,json.dumps(identity(os.getpid()),sort_keys=True))
        scratch=phase/'scratch';scratch.mkdir()
        boundary=HermesBoundaryOwner(root=phase/'boundary',candidate=self.candidate,context_pack=phase/'context',
            binding=binding,image=self.config.image,fingerprints=self.config.fingerprints,proxy=self.config.proxy,
            allocation_root=self.root,source=self.source if role in {'REVIEWER','TESTER'} else None,scratch=scratch if role in {'REVIEWER','TESTER'} else None)
        boundary.repository=self.repository
        self.owners.append(boundary);self._attempts.add(aid)
        write_once(phase/'binding.json',asdict(binding))
        key=self.factory.shared_gateway.key if self.factory.shared_gateway is not None else secrets.token_hex(32)
        adapter=HermesRuntimeAdapter(binding=binding,repository=self.repository,boundary=boundary,
            base_url='http://127.0.0.1:'+str(self.config.port),api_key=key)
        try:
            boundary.admit(binding)
            if self.factory.shared_gateway is not None:
                self.factory.shared_gateway.attach(self,boundary,adapter)
            else:
                self._gateway_configuration()
                env=self._environment(boundary,key)
                log=open(phase/'gateway-private.log','ab');self.logs.append(log)
                py=str(RUNTIME/'venv/Scripts/python.exe')
                process=OwnedProcess(py,[py,'-m','hermes_cli.main','gateway','run','--no-supervise','--quiet'],env,str(RUNTIME),log)
                boundary.process=process
                for _ in range(40):
                    if process.poll() is not None:raise HermesError('HERMES_GATEWAY_EXITED')
                    try:
                        health=adapter.request('GET','/health/detailed')
                        if health.get('pid') not in process.members():raise HermesError('HERMES_FOREIGN_GATEWAY')
                        break
                    except HermesError:time.sleep(.5)
                else:raise HermesError('HERMES_GATEWAY_UNAVAILABLE')
                boundary.bind_server(process,health['pid'])
                self.repository.record_runtime_process(aid,runtime='hermes',pid=process.pid,process_identity=json.dumps(identity(process.pid),sort_keys=True))
            runner=({'WORKER':OwnedWorker,'REVIEWER':OwnedReviewer,'TESTER':OwnedTester}[role])(adapter,approval_authorizer=self.factory.approval_authorizer,
                reasoning_effort=str(policy.get('reasoning_effort','low')) if role!='WORKER' else str(task.effective_policy['resolved_worker'].get('reasoning_effort','medium')))
            runner.scope=self
            return runner
        except BaseException:
            self.release(boundary)
            raise

    def _environment(self,boundary,key):
        env=os.environ.copy()
        for k in list(env):
            if any(s in k.upper() for s in ('TOKEN','API_KEY','AUTH_TOKEN')):env.pop(k,None)
        env.update(boundary.environment())
        env.update(HERMES_HOME=str(HOME),CODEX_HOME=r'D:\hermes\isolation\codex-empty',HOME=r'D:\hermes\isolation\user',USERPROFILE=r'D:\hermes\isolation\user',PYTHONUTF8='1',PYTHONIOENCODING='utf-8',PYTHONDONTWRITEBYTECODE='1',GIT_CONFIG_GLOBAL=r'D:\hermes\isolation\gitconfig',GIT_CONFIG_NOSYSTEM='1',HERMES_GIT_BASH_PATH=r'C:\Program Files\Git\bin\bash.exe',TERMINAL_ENV='docker',TERMINAL_CWD='/workspace',TERMINAL_DOCKER_IMAGE=self.config.image,TERMINAL_CONTAINER_PERSISTENT='false',TERMINAL_DOCKER_PERSIST_ACROSS_PROCESSES='false',TERMINAL_DOCKER_MOUNT_CWD_TO_WORKSPACE='false',TERMINAL_DOCKER_NETWORK='false',TERMINAL_DOCKER_ORPHAN_REAPER='false',TERMINAL_DOCKER_FORWARD_ENV='[]',TERMINAL_DOCKER_ENV='{}',TERMINAL_CONTAINER_CPU='0',TERMINAL_CONTAINER_MEMORY='0',TERMINAL_CONTAINER_DISK='0',TERMINAL_DOCKER_SHM_SIZE='64m',TERMINAL_DOCKER_EXTRA_ARGS=json.dumps(['--cpus','1','--memory','256m','--pids-limit','64']),TERMINAL_DOCKER_VOLUMES=json.dumps([x['source']+':'+x['destination']+':'+x['mode'] for x in boundary.policy['allowed']]),API_SERVER_KEY=key,API_SERVER_ENABLED='true',API_SERVER_HOST='127.0.0.1',API_SERVER_PORT=str(self.config.port),GATEWAY_MULTIPLEX_PROFILES='false')
        env.pop('HERMES_WRITE_SAFE_ROOT',None)
        return env

    def worker(self,task):
        return self._create(task,'WORKER',requirement=task.requirement)

    def reviewer(self,task,requirement,changed_files,git_diff,tester_context):
        return self._create(task,'REVIEWER',requirement=requirement,changed_files=changed_files,git_diff=git_diff,tester_context=tester_context)

    def tester(self,task):
        return self._create(task,'TESTER',requirement=task.requirement,changed_files=task.changed_files)

    def record_runtime_result(self,runner):
        status=runner.last_status or {}
        output=status.get('output','')
        try:
            parsed=runner.adapter.structured_outcome(output)
            outcome=parsed.get('outcome')
        except HermesError:
            outcome='INVALID_OR_ABSENT'
        atomic(runner.adapter.boundary.root/'runtime-result.json',{
            'binding':asdict(runner.adapter.binding),'native_status':status.get('status'),
            'structured_outcome':outcome,'output_length':len(output) if isinstance(output,str) else None,
            'job_success_authority':False})

    def release(self,boundary):
        p=boundary.root/'quiescence.json'
        proof=read(p) if p.exists() else boundary.fence(stable_seconds=3)
        if proof.get('state')!='QUIESCED' or proof.get('binding')!=asdict(boundary.binding):
            raise HermesError('HERMES_CLEANUP_QUIESCENCE_UNKNOWN')
        active=self.repository.active_runtime_process(boundary.binding.attempt_id)
        if active:self.repository.finish_runtime_process(active['process_id'],state='KILLED')
        if snapshot(self.source)!=self.source_before:
            raise HermesError('HERMES_SOURCE_MUTATED')
        atomic(boundary.root/'candidate-handoff.json',{'binding':asdict(boundary.binding),'disposition':'HARNESS_VERIFICATION_REQUIRED','quiescence':str(p),'candidate':str(self.candidate)})

    def close(self):
        if self.closed:return
        errors=[]
        for owner in self.owners:
            try:self.release(owner)
            except Exception as exc:errors.append(type(exc).__name__+':'+getattr(exc,'code','CLEANUP_FAILED'))
        for log in self.logs:log.close()
        if self._config_bytes is not None:
            config=HOME/'config.yaml'
            if config.is_file() and config.read_bytes()==self._config_bytes:config.unlink()
            else:errors.append('HERMES_TEMP_CONFIG_CHANGED')
        atomic(self.path/'cleanup.json',{'state':'FAILED' if errors else 'QUIESCED','errors':errors,'owners':len(self.owners)})
        if errors:raise HermesError('HERMES_CLEANUP_FAILED')
        self.closed=True
        atexit.unregister(self.close)
        with self.factory._guard:self.factory._scope=None

class OwnedWorker(HermesWorker):
    def execute(self,requirement,working_dir,task=None):
        cycle=getattr(task,'retry_count',0)+1
        operation='INITIAL' if cycle==1 else 'FIX'
        prompt=f'Current Harness Worker cycle {cycle}: {operation}. Current Attempt: {getattr(task,"attempt_id","")}. Apply only requirements for this current cycle. A FIX must address the previous independent Tester/Reviewer findings in implementation_context; do not replay instructions conditional on an earlier cycle. Read /context/context.md and obey its frozen contract. Work only in /workspace. Do not commit. Verify changes without pyc/cache files. After the edit and verification, end immediately with exactly this JSON object and no prose or markdown fences: {{"outcome":"EVIDENCE"}}. '
        try:return super().execute(prompt,working_dir,task)
        finally:
            self.scope.record_runtime_result(self)
            self.scope.release(self.adapter.boundary)

class OwnedReviewer(HermesReviewer):
    def review(self,requirement,working_dir,task):
        prompt=f'Current candidate producer is Worker cycle {task.retry_count+1}, Attempt {task.attempt_id}. Read /context/context.md, including the bound diff and verification evidence. Independently read and verify /workspace against the acceptance. /workspace and /source are read-only; only /scratch is writable. Do not change source. Return JSON only: {{"outcome":"REVIEW_PASS","reason":"...","details":[]}} or REVIEW_FAIL with concrete findings.'
        try:return super().review(prompt,working_dir,task)
        finally:
            self.scope.record_runtime_result(self)
            self.scope.release(self.adapter.boundary)


class OwnedTester(HermesWorker):
    def test(self,working_dir,task):
        from tester import TesterResult
        if self.adapter.binding.role!='TESTER':raise HermesError('TESTER_ROLE_MISMATCH')
        prompt='Read /context/context.md. Independently execute every frozen required/applicable test against /workspace and acceptance. Do not substitute Build or Worker claims for execution. Candidate/source/context are RO; only /scratch is writable. Disable bytecode/caches in source. Report actual commands, exits and observations. Return strict JSON only: {"outcome":"TEST_RESULT","status":"PASS or FAIL or ERROR","summary":"...","evidence":["command, exit, observation"]}. FAIL means a demonstrated product defect; unavailable tools/provider or unexecuted mandatory tests mean ERROR. Never implement a fix.'
        prompt=f'Current candidate producer is Worker cycle {task.retry_count+1}, Attempt {task.attempt_id}. ' + prompt
        try:
            result=self._bounded_execute(prompt,working_dir,task)
            if not result.success:
                native=(self.last_status or {}).get('status')
                return TesterResult('ERROR','TESTER_CANCELLED' if native in {'cancelled','interrupted'} else result.execution_failure_code)
            out=self.adapter.structured_outcome(result.stdout)
            if out['outcome']!='TEST_RESULT':raise HermesError('TEST_OUTCOME_REQUIRED')
            return TesterResult(out['status'],out['summary'],out['evidence'])
        finally:
            self.scope.record_runtime_result(self)
            self.scope.release(self.adapter.boundary)
