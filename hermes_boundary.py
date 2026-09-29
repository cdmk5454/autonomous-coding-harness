"""Harness-owned Docker boundary; per-binding immutable policy and actual receipts.

Only explicitly allocated isolated execution roots are admitted. The proxy never
mounts Harness/profile/control paths. Docker daemon containers are fenced apart
from the Windows Job Object; host process death is not quiescence evidence.
"""
from __future__ import annotations
from dataclasses import asdict
import hashlib, json, os, subprocess, time, uuid, re
from pathlib import Path
from runtime_adapter import payload_sha256
from hermes_runtime_adapter import HermesError

DOCKER = r'C:\Program Files\Docker\Docker\resources\bin\docker.exe'
CAPS = ['DAC_OVERRIDE','CHOWN','FOWNER','SETUID','SETGID']
TMPFS = ['/tmp:rw,nosuid,size=512m','/var/tmp:rw,noexec,nosuid,size=256m',
         '/run:rw,noexec,nosuid,size=64m','/home:rw,exec,size=1g','/root:rw,exec,size=1g','/opt/data:rw,noexec,nosuid,size=1g']

def norm(path):
    return str(Path(path).resolve()).replace('\\','/').rstrip('/').lower()

def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8-sig'))

def write_once(path, value):
    with Path(path).open('x',encoding='utf8') as f:
        json.dump(value,f,ensure_ascii=False,sort_keys=True)

def atomic(path, value):
    target = Path(path)
    temp = target.with_name(target.name+'.'+uuid.uuid4().hex+'.tmp')
    write_once(temp,value)
    # Windows observers can briefly hold a file without FILE_SHARE_DELETE.
    # Preserve atomic replace; never downgrade to an in-place/partial write.
    for attempt in range(8):
        try:
            os.replace(temp,target)
            return
        except PermissionError:
            if os.name != 'nt' or attempt == 7:
                raise
            time.sleep(min(.025 * (attempt + 1), .1))

def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def snapshot(root, *, exclude_git=False):
    root = Path(root)
    result = {}
    for f in root.rglob('*'):
        if exclude_git and '.git' in f.relative_to(root).parts:continue
        if f.is_symlink() or f.resolve() != f.absolute():
            raise HermesError('CANDIDATE_LINK_DENIED')
        if f.is_file():
            if f.stat().st_nlink != 1:
                raise HermesError('CANDIDATE_HARDLINK_DENIED')
            # The candidate allocator must reject credentials before admission.
            if f.name.lower() in {'.env','auth.json','credentials.json'} or f.name.lower().startswith('.env.'):
                raise HermesError('CANDIDATE_SECRET_FILE_DENIED')
            result[f.relative_to(root).as_posix()] = {'sha256':sha(f), 'mtime_ns':f.stat().st_mtime_ns}
    return result

def inspect_receipt(obj, policy):
    hc = obj['HostConfig']
    actual = [{'source':norm(m['Source']), 'destination':m['Destination'],
               'mode':'rw' if m['RW'] else 'ro'} for m in obj['Mounts'] if m['Type']=='bind']
    expected = policy['allowed']
    checks = {
        'mounts':sorted(actual,key=lambda x:x['destination'])==sorted(expected,key=lambda x:x['destination']),
        'other_mounts':all(m['Type'] in {'bind','tmpfs'} for m in obj['Mounts']),
        'privileged':hc.get('Privileged') is False,
        'cap_add':sorted(x.removeprefix('CAP_') for x in (hc.get('CapAdd') or []))==sorted(CAPS),
        'cap_drop':hc.get('CapDrop')==['ALL'],
        'security':hc.get('SecurityOpt')==['no-new-privileges'],
        'network':hc.get('NetworkMode')=='none',
        'pid':hc.get('PidMode','')=='',
        'ipc':hc.get('IpcMode')=='private',
        'devices':not hc.get('Devices') and not hc.get('DeviceRequests') and not hc.get('DeviceCgroupRules'),
        'resources':hc.get('PidsLimit')==64 and hc.get('Memory')==268435456 and hc.get('NanoCpus')==1000000000 and hc.get('ShmSize')==67108864,
        'tmpfs':hc.get('Tmpfs')==dict(x.split(':',1) for x in TMPFS),
        'image':obj.get('Image')==policy['image'],
        'label':obj.get('Config',{}).get('Labels',{}).get('harness-binding')==policy['binding']['binding_id'],
    }
    return {'container_id':obj['Id'],'binding':policy['binding'],
            'mounts':obj['Mounts'],'host_config':{k:hc.get(k) for k in (
                'Privileged','CapAdd','CapDrop','SecurityOpt','NetworkMode','PidMode','IpcMode',
                'Devices','DeviceRequests','DeviceCgroupRules','PidsLimit','Memory','NanoCpus','ShmSize','Tmpfs')},
            'checks':checks,'state':'BOUNDARY_VERIFIED' if all(checks.values()) else 'INSPECT_MISMATCH'}

class HermesBoundaryOwner:
    def __init__(self, *, root, candidate, context_pack, binding, image, fingerprints,
                 proxy, allocation_root, source=None, scratch=None):
        self.root, self.candidate, self.context_pack = map(Path,(root,candidate,context_pack))
        self.binding, self.proxy = binding, Path(proxy)
        self.allocation_root=Path(allocation_root).resolve()
        self.source=Path(source) if source else None
        self.scratch=Path(scratch) if scratch else None
        self.process=None
        self.server_identity=None
        self.repository=None
        for p in [self.root,self.candidate,self.context_pack,*([self.source,self.scratch] if source and scratch else [])]:
            if p.absolute()!=p.resolve() or not p.resolve().is_relative_to(self.allocation_root) or p.resolve()==self.allocation_root:
                raise HermesError('EXECUTION_PATH_ALLOCATION_DENIED')
        for denied in (Path(r'D:\kkm_test'),Path(r'D:\agents'),Path(r'D:\hermes\home'),Path(r'D:\harness_restore_verify')):
            if self.allocation_root.is_relative_to(denied) or denied.is_relative_to(self.allocation_root):
                raise HermesError('PROTECTED_ALLOCATION_DENIED')
        if binding.role in {'REVIEWER','TESTER'} and (not self.source or not self.scratch):
            raise HermesError('REVIEWER_BOUNDARY_INCOMPLETE')
        self.root.mkdir(parents=True,exist_ok=False)
        self.lock=self.candidate.parent/(self.candidate.name+'.harness-writer-lock.json')
        if self.lock.exists():
            old=read(self.lock)
            if old['state']!='QUIESCED':
                raise HermesError('REPLACEMENT_ADMISSION_DENIED')
            # The recorded stable snapshot must still match at replacement time.
            if not old.get('candidate_snapshot') or snapshot(self.candidate)!=old['candidate_snapshot']:
                raise HermesError('CANDIDATE_CHANGED_AFTER_QUIESCENCE')
        # Exclusive file creation protects single-owner allocation. Concurrency
        # is deliberately unsupported; a surviving allocation blocks replacement.
        if self.lock.exists():
            self.lock.rename(self.lock.with_name(self.lock.name+'.'+uuid.uuid4().hex+'.history'))
        write_once(self.lock,{'state':'PREPARED','binding':asdict(binding)})
        def mount(p,d,m):return {'source':norm(p),'destination':d,'mode':m}
        allowed=[mount(self.candidate,'/workspace','rw' if binding.role=='WORKER' else 'ro'),mount(self.context_pack,'/context','ro')]
        if binding.role in {'REVIEWER','TESTER'}:
            allowed += [mount(self.source,'/source','ro'),mount(self.scratch,'/scratch','rw')]
        h=Path(r'D:\hermes\home')
        drop=[mount(h/'skills','/root/.hermes/skills','ro')]
        for new,old in [('cache/documents','document_cache'),('cache/images','image_cache'),('cache/audio','audio_cache'),('cache/videos','video_cache'),('cache/screenshots','browser_screenshots'),('cache/web','web_cache'),('cache/delegation','delegation_cache'),('cache/spillover','cache/spillover'),('images','images'),('attachments','attachments')]:
            for s in {new,old}:drop.append(mount(h/s,'/root/.hermes/'+new,'ro'))
        from hermes_process_boundary import identity
        self.policy={'binding':asdict(binding),'image':image,'allowed':allowed,'drop':drop,
                     'fingerprints':dict(fingerprints),'owner':identity(os.getpid())}
        for name in ('hermes_boundary.py','hermes_process_boundary.py','hermes_runtime_adapter.py','hermes_worker.py','execution_liveness.py','hermes_supervision.py','hermes_execution_factory.py','hermes_identity.py','execution_context.py','hermes_shared_gateway.py'):
            source_path=Path(__file__).with_name(name)
            self.policy['fingerprints'][str(source_path)]=sha(source_path)
        self.policy['fingerprints'][str(self.proxy)]=sha(self.proxy)
        self.policy['fingerprints'][str(Path(__file__).with_name('hermes_mount_proxy.py'))]=sha(Path(__file__).with_name('hermes_mount_proxy.py'))
        write_once(self.root/'policy.json',self.policy)
        self.policy_digest=sha(self.root/'policy.json')
        atomic(self.root/'state.json',{'state':'ACTIVE','epoch':binding.execution_epoch})
        self.before=snapshot(self.candidate)
        self.validate_binding(binding)

    def validate_binding(self,b):
        if b != self.binding or read(self.root/'policy.json')['binding'] != asdict(b):
            raise HermesError('FOREIGN_ATTEMPT')
        state=read(self.root/'state.json')
        if state['epoch']!=b.execution_epoch or state['state']!='ACTIVE':
            raise HermesError('STALE_EPOCH')
        if (self.root/'admission-lease.json').exists() and not (self.root/'liveness.json').exists():
            from datetime import datetime,timezone
            lease=read(self.root/'admission-lease.json')
            if lease.get('identity')!=asdict(b) or datetime.now(timezone.utc)>=datetime.fromisoformat(lease['lease_expires_at']):
                raise HermesError('ADMISSION_LEASE_EXPIRED')
        if sha(self.root/'policy.json')!=self.policy_digest:
            raise HermesError('MOUNT_POLICY_TAMPERED')
        for path,digest in self.policy['fingerprints'].items():
            if not Path(path).is_file() or sha(path)!=digest:
                raise HermesError('RUNTIME_POLICY_REVALIDATION_REQUIRED')
        if set(p.name for p in self.context_pack.iterdir())!={'context.md','manifest.json'}:
            raise HermesError('CONTEXT_CONTENT_UNEXPECTED')
        for p in self.context_pack.iterdir():
            if not p.is_file() or p.resolve()!=p.absolute():
                raise HermesError('CONTEXT_LINK_DENIED')
        manifest=read(self.context_pack/'manifest.json')
        prompt=(self.context_pack/'context.md').read_text(encoding='utf8')
        digest=manifest.pop('pack_sha256',None)
        actual=hashlib.sha256((prompt+json.dumps(manifest,ensure_ascii=False,sort_keys=True,separators=(',',':'))).encode()).hexdigest()
        if digest!=b.context_pack_digest or digest!=actual or hashlib.sha256(prompt.encode()).hexdigest()!=manifest['content_sha256']:
            raise HermesError('CONTEXT_DIGEST_MISMATCH')
        contract=manifest['contract']
        for k in ('job_id','execution_id','attempt_id','candidate_id','role'):
            if contract.get(k)!=getattr(b,k):raise HermesError('CONTEXT_BINDING_MISMATCH')
        if contract.get('runtime_admission')!='NOT_GRANTED' or not contract.get('profile_id') or not contract.get('allowed_write_scope') or contract.get('risk_tier') not in {'LOW','MID','HIGH','CRITICAL'}:
            raise HermesError('EXECUTION_CONTRACT_UNRESOLVED')
        return manifest

    def bind_server(self, process, gateway_pid):
        from hermes_process_boundary import identity
        if self.server_identity is not None or gateway_pid not in process.members():
            raise HermesError('FOREIGN_SERVER_INCARNATION')
        observed=identity(gateway_pid)
        if not observed.get('alive') or not observed.get('creation_time_100ns'):
            raise HermesError('SERVER_IDENTITY_UNAVAILABLE')
        self.process=process
        self.server_identity=observed
        atomic(self.root/'host-members.json',[identity(pid) for pid in process.members()])
        write_once(self.root/'server-binding.json',{'binding':asdict(self.binding),
            'server':observed,'launcher':identity(process.pid)})
    def assert_server(self,b):
        self.validate_binding(b)
        from hermes_process_boundary import identity
        if self.server_identity is None or self.process is None:
            raise HermesError('HERMES_OWNED_SERVER_REQUIRED')
        current=identity(self.server_identity['pid'])
        if not current.get('alive') or current.get('creation_time_100ns')!=self.server_identity['creation_time_100ns'] or current['pid'] not in self.process.members():
            atomic(self.root/'state.json',{'state':'FENCING','epoch':b.execution_epoch})
            atomic(self.lock,{'state':'FENCING','binding':asdict(b)})
            raise HermesError('SERVER_INCARNATION_LOST')

    def environment(self):
        return {'HARNESS_HERMES_POLICY':str(self.root/'policy.json'),
                'HARNESS_HERMES_POLICY_SHA256':self.policy_digest,
                'HARNESS_HERMES_EPOCH':self.binding.execution_epoch,
                'HERMES_DOCKER_BINARY':str(self.proxy)}

    def bootstrap_args(self):
        args=['run','-d','--init','-w','/workspace','--cap-drop','ALL']
        for c in CAPS:args+=['--cap-add',c]
        args+=['--security-opt','no-new-privileges','--network','none','--cpus','1','--memory','256m','--pids-limit','64','--shm-size','64m']
        for tmp in TMPFS:args+=['--tmpfs',tmp]
        for m in self.policy['allowed']:args+=['-v',m['source']+':'+m['destination']+':'+m['mode']]
        return args+[self.policy['image'],'sleep','infinity']

    def admit(self,b):
        manifest=self.validate_binding(b)
        if self.repository is None:
            raise HermesError('CONTROL_AUTHORITY_REQUIRED')
        execution=self.repository.active_execution(b.job_id)
        attempt=self.repository.attempt_identity(b.attempt_id)
        latest=self.repository.latest_attempt(b.execution_id)
        if not execution or execution['execution_id']!=b.execution_id or not attempt or attempt['execution_id']!=b.execution_id or attempt['role']!=b.role or attempt['status']!='RUNNING' or not latest or latest['attempt_id']!=b.attempt_id:
            raise HermesError('FOREIGN_ATTEMPT')
        if self.repository.global_stop().get('active') or self.repository.open_decisions(b.job_id):
            raise HermesError('HARNESS_AUTHORIZATION_BLOCKED')
        if b.role=='WORKER' and self.repository.writer_authority(b.execution_id)['state']!='ACTIVE':
            raise HermesError('WRITER_AUTHORITY_SUSPENDED')
        if read(self.lock)['binding']!=asdict(b):
            raise HermesError('STALE_EPOCH')
        if not (self.root/'admission-lease.json').exists():
            from datetime import datetime,timezone,timedelta
            from worker import TimeoutConfig
            cfg=TimeoutConfig.from_env()
            atomic(self.root/'admission-lease.json',{'identity':asdict(b),'state':'ACTIVE',
                'lease_expires_at':(datetime.now(timezone.utc)+timedelta(seconds=cfg.runtime_lease)).isoformat()})
        if not list(self.root.glob('inspect-*.json')):
            env=os.environ.copy();env.update(self.environment())
            result=subprocess.run([str(self.proxy),*self.bootstrap_args()],env=env,capture_output=True,text=True,timeout=45)
            if result.returncode:raise HermesError('DOCKER_BOUNDARY_ADMISSION_FAILED')
        receipts=[read(p) for p in self.root.glob('inspect-*.json')]
        if not receipts or any(r['binding']!=asdict(b) or r['state']!='BOUNDARY_VERIFIED' for r in receipts):
            raise HermesError('ACTUAL_INSPECT_MISMATCH')
        from execution_context import ADMISSION_FIELDS
        evidence={key:{'binding_fingerprint':b.fingerprint,'verified':True} for key in ADMISSION_FIELDS}
        atomic(self.root/'admission.json',{'binding':asdict(b),'state':'BOUNDARY_VERIFIED',
            'evidence':evidence,'context_pack_digest':b.context_pack_digest,
            'profile_id':manifest['profile_id'],'scope':manifest['contract']['allowed_write_scope'],
            'risk_tier':manifest['risk_tier'],'policy_sha256':self.policy_digest,
            'container_receipts':[r['container_id'] for r in receipts]})
        atomic(self.lock,{'state':'ADMITTED','binding':asdict(b)})
        return receipts

    def receipt(self):
        return {'binding':asdict(self.binding),'policy_sha256':self.policy_digest,
                'containers':[read(p) for p in self.root.glob('inspect-*.json')]}

    def authorize_interaction(self,b,request_id,pending,response):
        self.validate_binding(b)
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,128}',request_id):raise HermesError('INVALID_APPROVAL_REQUEST')
        grant=read(self.root/('approval-'+request_id+'.json'))
        if grant!={'binding':asdict(b),'request_id':request_id,'action_sha256':payload_sha256(pending),'choice':response['choice']}:
            raise HermesError('APPROVAL_AUTHORIZATION_MISMATCH')
        (self.root/('approval-'+request_id+'.json')).rename(self.root/('consumed-'+request_id+'.json'))

    def authorize_resume(self,b,session_id,prompt):
        self.validate_binding(b)
        grant=read(self.root/'human-answer-grant.json')
        if grant.get('binding')!=asdict(b) or grant.get('session_id')!=session_id or grant.get('authorized_human') is not True or grant.get('previous_quiesced') is not True:
            raise HermesError('QA_AUTHORIZATION_REQUIRED')
        decisions=self.repository.decisions(b.job_id,'RESOLVED') if self.repository else []
        decision=next((d for d in decisions if d['decision_id']==grant.get('decision_id')),None)
        if not decision or decision.get('native_session_id')!=session_id or grant.get('prompt_sha256')!=hashlib.sha256(prompt.encode()).hexdigest() or grant.get('answer_sha256')!=hashlib.sha256(decision['resolution'].encode()).hexdigest():
            raise HermesError('QA_AUTHORIZATION_REQUIRED')

    def fence(self, *, stable_seconds=3):
        atomic(self.root/'state.json',{'state':'FENCING','epoch':self.binding.execution_epoch})
        atomic(self.lock,{'state':'FENCING','binding':asdict(self.binding)})
        members=read(self.root/'host-members.json') if (self.root/'host-members.json').exists() else []
        if self.process:
            members=self.process.close_boundary()
            atomic(self.root/'host-members.json',members)
        ids=[read(p)['container_id'] for p in self.root.glob('inspect-*.json')]
        # Include containers created before a receipt survived: immutable label
        # is set at creation, before inspect, so crash windows remain fenceable.
        found=subprocess.run([DOCKER,'ps','-aq','--no-trunc','--filter','label=harness-binding='+self.binding.binding_id],capture_output=True,text=True,timeout=15,check=True)
        ids=sorted(set(ids+found.stdout.split()))
        for cid in ids:
            subprocess.run([DOCKER,'rm','-f',cid],capture_output=True,timeout=20)
        from hermes_process_boundary import identity
        start=time.monotonic();samples=[]
        while True:
            alive=[identity(x['pid']) for x in members]
            same=[x for x in alive if x.get('error_code')==5 or (x.get('alive') and any(x.get('creation_time_100ns')==m.get('creation_time_100ns') and x['pid']==m['pid'] for m in members))]
            samples.append({'elapsed':time.monotonic()-start,'candidate':snapshot(self.candidate),'alive':same})
            if time.monotonic()-start>=stable_seconds:break
            time.sleep(.2)
        remaining=subprocess.run([DOCKER,'ps','-aq','--filter','label=harness-binding='+self.binding.binding_id],capture_output=True,text=True,timeout=15,check=True).stdout.strip()
        good=not remaining and all(not s['alive'] and s['candidate']==samples[0]['candidate'] for s in samples)
        proof={'binding':asdict(self.binding),'state':'QUIESCED' if good else 'UNKNOWN','containers':ids,'samples':samples}
        atomic(self.root/'quiescence.json',proof)
        if good:atomic(self.lock,{'state':'QUIESCED','binding':asdict(self.binding),'candidate_snapshot':samples[-1]['candidate']})
        if self.repository:
            self.repository.record_runtime_recovery(self.binding.execution_id,self.binding.attempt_id,
                failure_code='HERMES_FENCING',evidence={'binding':asdict(self.binding),'writer_quiescence':{'quiesced':good,'proof':str(self.root/'quiescence.json')}})
        return proof


def recover_orphan_boundary(policy_path, *, expected_policy_sha256, allocation_root, stable_seconds=30, repository=None):
    """External Harness recovery: validate trusted receipt, never revive epoch.

    The caller supplies the independently persisted policy fingerprint. An alive
    or uninspectable owner is not taken over. All labeled containers must stop
    before the durable candidate lock can become QUIESCED.
    """
    policy_path=Path(policy_path)
    allocation_root=Path(allocation_root).resolve()
    if not policy_path.resolve().is_relative_to(allocation_root) or sha(policy_path)!=expected_policy_sha256:
        raise HermesError('RECOVERY_POLICY_IDENTITY_MISMATCH')
    p=read(policy_path)
    from hermes_process_boundary import identity
    from hermes_runtime_adapter import HermesBinding
    owner=identity(p['owner']['pid'])
    if owner.get('error_code')==5 or (owner.get('alive') and owner.get('creation_time_100ns')==p['owner']['creation_time_100ns']):
        raise HermesError('BOUNDARY_OWNER_STILL_ACTIVE')
    candidate=Path(next(m['source'] for m in p['allowed'] if m['destination']=='/workspace'))
    if not candidate.resolve().is_relative_to(allocation_root):
        raise HermesError('RECOVERY_CANDIDATE_DENIED')
    boundary=HermesBoundaryOwner.__new__(HermesBoundaryOwner)
    boundary.root=policy_path.parent;boundary.candidate=candidate
    boundary.binding=HermesBinding(**p['binding']);boundary.process=None;boundary.repository=None
    boundary.lock=candidate.parent/(candidate.name+'.harness-writer-lock.json')
    if read(boundary.lock)['binding']!=p['binding']:
        raise HermesError('RECOVERY_LOCK_BINDING_MISMATCH')
    if not (boundary.root/'host-members.json').is_file():
        raise HermesError('HOST_OWNERSHIP_EVIDENCE_MISSING')
    boundary.repository=repository
    if repository:
        repository.suspend_writer_authority(boundary.binding.execution_id,reason='HERMES_BOUNDARY_OWNER_LOST')
    proof=boundary.fence(stable_seconds=stable_seconds)
    if repository:
        repository.record_runtime_recovery(boundary.binding.execution_id,boundary.binding.attempt_id,
            failure_code='HERMES_BOUNDARY_OWNER_LOST',state='FRESH_ATTEMPT_REQUIRED',
            evidence={'binding':asdict(boundary.binding),'writer_quiescence':{'quiesced':proof['state']=='QUIESCED'},
              'candidate_digest':payload_sha256({p:r['sha256'] for p,r in snapshot(candidate).items() if not p.startswith('.git/')}),
              'candidate_disposition_required':True,'command_reconciliation_required':True})
    return proof
