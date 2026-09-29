"""Execution-scoped Docker admission. No model-facing host paths or secrets."""
import sys,os,json,subprocess,pathlib,hashlib,datetime,uuid,re
from hermes_boundary import inspect_receipt,atomic
from hermes_process_boundary import identity
ROOT=pathlib.Path(os.environ.get('HARNESS_HERMES_POLICY','')).parent
REAL=r'C:\Program Files\Docker\Docker\resources\bin\docker.exe'
def norm(s): return str(pathlib.Path(s).resolve()).replace('\\','/').rstrip('/').lower()
def log(x):
 x['timestamp']=datetime.datetime.now(datetime.timezone.utc).isoformat()
 (ROOT/('audit-'+uuid.uuid4().hex+'.json')).write_text(json.dumps(x,indent=2),encoding='utf8')
def main(argv):
 policy_path=ROOT/'policy.json'
 if hashlib.sha256(policy_path.read_bytes()).hexdigest()!=os.environ.get('HARNESS_HERMES_POLICY_SHA256'): raise ValueError('policy fingerprint')
 p=json.loads(policy_path.read_text()); epoch=p['binding']['execution_epoch']; audit={'command_type':argv[0] if argv else '', 'execution_epoch':epoch,'allowed_mounts':[],'dropped_implicit_mounts':[],'denied_mounts':[]}
 def deny(reason):
  audit['decision']=reason; log(audit); print(reason,file=sys.stderr); return 125
 for file,digest in p['fingerprints'].items():
  if hashlib.sha256(pathlib.Path(file).read_bytes()).hexdigest()!=digest: return deny('RUNTIME_POLICY_REVALIDATION_REQUIRED')
 if not argv: return deny('COMMAND_POLICY_UNKNOWN')
 cmd=argv[0]
 state=json.loads((ROOT/'state.json').read_text())
 owner=identity(p['owner']['pid'])
 if not owner.get('alive') or owner.get('creation_time_100ns')!=p['owner']['creation_time_100ns']: return deny('BOUNDARY_OWNER_LOST')
 p['active']=state.get('state')=='ACTIVE' and state.get('epoch')==epoch
 server_path=ROOT/'server-binding.json'
 if server_path.exists():
  expected=json.loads(server_path.read_text())['server']; observed=identity(expected['pid'])
  if not observed.get('alive') or observed.get('creation_time_100ns')!=expected['creation_time_100ns']: return deny('SERVER_INCARNATION_LOST')

 lease_path=ROOT/'liveness.json'
 if not lease_path.exists():lease_path=ROOT/'admission-lease.json'
 if cmd in ('run','create','exec') and lease_path.exists():
  lease=json.loads(lease_path.read_text())
  if lease.get('identity',{}).get('execution_epoch')!=epoch or lease.get('state')!='ACTIVE': return deny('LEASE_NOT_ACTIVE')
  try: expires=datetime.datetime.fromisoformat(lease['lease_expires_at'])
  except (KeyError,ValueError): return deny('LEASE_UNKNOWN')
  if datetime.datetime.now(datetime.timezone.utc)>=expires: return deny('LEASE_EXPIRED')
 if cmd not in ('run','create','version','info','inspect','image','exec','stop','kill','rm','ps'): return deny('COMMAND_POLICY_UNKNOWN')
 if cmd in ('run','create'):
  if not p['active']: return deny('EXECUTION_EPOCH_INVALID')
  if os.environ.get('HARNESS_HERMES_EPOCH')!=epoch: return deny('EXECUTION_EPOCH_BINDING_MISMATCH')
  out=[cmd]; flags={}; mounts=[]; i=1
  booleans={'-d','--init','--rm'}
  values={'--name','--label','-w','--workdir','--cap-drop','--cap-add','--security-opt','--tmpfs','--cpus','--memory','--pids-limit','--shm-size','--network','--ipc','--pid','--privileged'}
  while i<len(argv) and argv[i].startswith('-'):
   token=argv[i]; k,sep,v=token.partition('=')
   if k in booleans and not sep: out.append(k); flags.setdefault(k,[]).append(True); i+=1; continue
   if k not in values|{'-v','--volume','--mount'}: return deny('SECURITY_ARGUMENT_UNKNOWN')
   if not sep:
    i+=1
    if i>=len(argv): return deny('ARGUMENT_MISSING')
    v=argv[i]
   if k in ('-v','--volume','--mount'):
    if k=='--mount': return deny('MOUNT_SYNTAX_UNSUPPORTED')
    m=re.fullmatch(r'([A-Za-z]:[\\/].*?):(/[^:]*)(?::(ro|rw))?',v)
    if not m: return deny('MOUNT_POLICY_UNKNOWN')
    source,dest,mode=m.groups(); mode=mode or 'rw'; entry={'source':norm(source),'destination':dest,'mode':mode}
    if entry in p['allowed']:
     # Reject junctions/symlinks and duplicate mount destinations.
     if any(x['destination']==dest for x in mounts): return deny('MOUNT_DUPLICATE')
     mounts.append(entry); audit['allowed_mounts'].append(entry); out+=['-v',v]
    elif entry in p['drop'] and mode=='ro': audit['dropped_implicit_mounts'].append(entry)
    else: audit['denied_mounts'].append(entry); return deny('MOUNT_POLICY_UNKNOWN')
   else:
    flags.setdefault(k,[]).append(v); out += [k,v]
   i+=1
  audit['security_flags']={k:v for k,v in flags.items() if k not in ('--label','--name')}
  if i>=len(argv) or argv[i]!=p['image']: return deny('IMAGE_POLICY_UNKNOWN')
  if argv[i+1:]!=['sleep','infinity']: return deny('CONTAINER_COMMAND_UNKNOWN')
  required={'--cap-drop':['ALL'],'--security-opt':['no-new-privileges'],'--network':['none'],'--cpus':['1'],'--memory':['256m'],'--pids-limit':['64']}
  if any(flags.get(k)!=v for k,v in required.items()): return deny('SECURITY_POLICY_MISMATCH')
  if flags.get('--cap-add',[])!=['DAC_OVERRIDE','CHOWN','FOWNER','SETUID','SETGID']: return deny('CAPABILITY_POLICY_MISMATCH')
  if '--privileged' in flags or '--pid' in flags or '--ipc' in flags: return deny('NAMESPACE_POLICY_DENIED')
  tmps={'/tmp:rw,nosuid,size=512m','/var/tmp:rw,noexec,nosuid,size=256m','/run:rw,noexec,nosuid,size=64m','/home:rw,exec,size=1g','/root:rw,exec,size=1g','/opt/data:rw,noexec,nosuid,size=1g'}
  if set(flags.get('--tmpfs',[]))!=tmps or len(flags.get('--tmpfs',[]))!=6: return deny('TMPFS_POLICY_UNKNOWN')
  if sorted(mounts,key=lambda x:x['destination'])!=sorted(p['allowed'],key=lambda x:x['destination']): return deny('REQUIRED_MOUNT_MISSING')
  if flags.get('-w',flags.get('--workdir'))not in (['/workspace'],['/root']): return deny('WORKDIR_POLICY_UNKNOWN')
  if flags.get('--shm-size')!=['64m']: return deny('RESOURCE_POLICY_UNKNOWN')
  out+=['--label','harness-binding='+p['binding']['binding_id']]+argv[i:]
  result=subprocess.run([REAL,*out],capture_output=True,text=True)
  if result.returncode==0:
   cid=result.stdout.strip(); audit['resulting_container_id']=cid
   ins=subprocess.run([REAL,'inspect',cid],capture_output=True,text=True,check=True)
   obj=json.loads(ins.stdout)[0]; hc=obj['HostConfig']; actual=[{'source':norm(m['Source']),'destination':m['Destination'],'mode':'rw' if m['RW'] else 'ro'} for m in obj['Mounts'] if m['Type']=='bind']
   record=inspect_receipt(obj,p)
   atomic(ROOT/('inspect-'+cid+'.json'),record)
   if record['state']!='BOUNDARY_VERIFIED':
    subprocess.run([REAL,'rm','-f',cid],capture_output=True); return deny('ACTUAL_INSPECT_MISMATCH')
   atomic(ROOT/('owned-'+cid+'.json'),{'epoch':epoch,'id':cid})
  audit['decision']='ALLOW' if result.returncode==0 else 'DOCKER_CREATION_FAILED'; log(audit)
  sys.stdout.write(result.stdout); sys.stderr.write(result.stderr); return result.returncode
 if cmd=='exec':
  if not p['active']: return deny('EXECUTION_EPOCH_INVALID')
  if os.environ.get('HARNESS_HERMES_EPOCH')!=epoch: return deny('EXECUTION_EPOCH_BINDING_MISMATCH')
  i=1
  while i<len(argv) and argv[i].startswith('-'):
   # No exec env forwarding, user change, privileged exec or unrecognized flags.
   if argv[i] not in ('-i','-t'): return deny('EXEC_ARGUMENT_DENIED')
   i+=1
  if i>=len(argv): return deny('EXEC_TARGET_MISSING')
  if not re.fullmatch(r'[a-f0-9]{64}',argv[i]): return deny('EXEC_TARGET_DENIED')
  f=ROOT/('owned-'+argv[i]+'.json')
  if not f.exists() or json.loads(f.read_text())['epoch']!=epoch: return deny('EXEC_TARGET_DENIED')
  observed=subprocess.run([REAL,'inspect',argv[i]],capture_output=True,text=True)
  if observed.returncode: return deny('EXEC_TARGET_UNAVAILABLE')
  receipt=inspect_receipt(json.loads(observed.stdout)[0],p)
  if receipt['state']!='BOUNDARY_VERIFIED':
   subprocess.run([REAL,'rm','-f',argv[i]],capture_output=True)
   return deny('ACTUAL_INSPECT_MISMATCH')

 if cmd=='image' and (len(argv)<2 or argv[1]!='inspect'): return deny('COMMAND_POLICY_UNKNOWN')
 if cmd in ('kill','rm','stop'):
  for target in [x for x in argv[1:] if not x.startswith('-') and not (cmd=='stop' and x=='10')]:
   if not re.fullmatch(r'[a-f0-9]{64}',target) or not (ROOT/('owned-'+target+'.json')).exists(): return deny('LIFECYCLE_TARGET_DENIED')
 lifecycle_targets=[]
 if cmd in ('kill','rm','stop'):
  lifecycle_targets=[x for x in argv[1:] if re.fullmatch(r'[a-f0-9]{64}',x)]
  for cid in lifecycle_targets:
   atomic(ROOT/('lifecycle-'+cid+'.json'),{'binding':p['binding'],'command':cmd,'state':'REQUESTED'})
 audit['decision']='PASSTHROUGH'; log(audit)
 code=subprocess.call([REAL,*argv])
 for cid in lifecycle_targets:
  atomic(ROOT/('lifecycle-'+cid+'.json'),{'binding':p['binding'],'command':cmd,'state':'ACKNOWLEDGED' if code==0 else 'UNKNOWN'})
 if cmd=='exec':
  atomic(ROOT/('exec-completed-'+uuid.uuid4().hex+'.json'),{'binding':p['binding'],'container_id':argv[i],'exit_code':code,'origin':'HARNESS_PROXY_COMMAND_RECEIPT'})
 return code
def shared_dispatch(argv):
 global ROOT
 registry=pathlib.Path(os.environ['HARNESS_HERMES_REGISTRY']).resolve()
 if not argv: return 125
 cmd=argv[0]
 if cmd in ('version','info') or (cmd=='image' and argv[1:2]==['inspect']):
  return subprocess.call([REAL,*argv])
 record=None
 if cmd in ('run','create'):
  labels=[]
  for i,arg in enumerate(argv):
   if arg=='--label' and i+1<len(argv): labels.append(argv[i+1])
   elif arg.startswith('--label='): labels.append(arg.split('=',1)[1])
  sessions=[x.split('=',1)[1] for x in labels if x.startswith('hermes-task-id=')]
  if len(sessions)!=1 or not re.fullmatch(r'HS-[a-f0-9]{32}',sessions[0]): return 125
  file=registry/'sessions'/(sessions[0]+'.json')
  if not file.is_file(): return 125
  record=json.loads(file.read_text(encoding='utf8'))
 else:
  ids=[x for x in argv[1:] if re.fullmatch(r'[a-f0-9]{64}',x)]
  if len(ids)!=1:return 125
  receipts=[]
  for file in (registry/'sessions').glob('HS-*.json'):
   item=json.loads(file.read_text(encoding='utf8'))
   if (pathlib.Path(item['boundary'])/('owned-'+ids[0]+'.json')).exists():receipts.append(item)
  if len(receipts)!=1:return 125
  record=receipts[0]
 ROOT=pathlib.Path(record['boundary']).resolve()
 if not ROOT.is_relative_to(registry.parent):return 125
 policy=json.loads((ROOT/'policy.json').read_text(encoding='utf8'))
 if policy['binding']!=record['binding']:return 125
 os.environ['HARNESS_HERMES_POLICY_SHA256']=record['policy_sha256']
 os.environ['HARNESS_HERMES_POLICY']=str(ROOT/'policy.json')
 os.environ['HARNESS_HERMES_EPOCH']=policy['binding']['execution_epoch']
 pending=ROOT/('shared-proxy-'+uuid.uuid4().hex+'.json')
 atomic(pending,{'process':identity(os.getpid()),'binding':policy['binding']})
 try:
  if cmd in ('run','create'):
   # Pinned shared gateway has NO user bind mounts and ephemeral /workspace.
   # Reject all supplied binds; replace only that known tmpfs with bound mounts.
   out=[];i=0;removed=0
   while i<len(argv):
    if argv[i] in ('-v','--volume') and i+1<len(argv):
     m=re.fullmatch(r'([A-Za-z]:[\\/].*?):(/[^:]*)(?::(ro|rw))?',argv[i+1])
     if not m:return 125
     source,dest,mode=m.groups()
     if {'source':norm(source),'destination':dest,'mode':mode or 'rw'} not in policy['drop']:return 125
     i+=2;continue
    if argv[i].startswith(('--mount','--volume=','-v=')):return 125
    if argv[i]=='--tmpfs' and i+1<len(argv) and argv[i+1]=='/workspace:rw,exec,size=10g':
     removed+=1;i+=2;continue
    out.append(argv[i]);i+=1
   if removed!=1:return 125
   pos=out.index(policy['image'])
   mounts=[]
   for m in policy['allowed']:mounts+=['-v',m['source']+':'+m['destination']+':'+m['mode']]
   argv=out[:pos]+mounts+out[pos:]
  return main(argv)
 finally:
  pending.unlink(missing_ok=True)

if __name__=='__main__':
 try: sys.exit(shared_dispatch(sys.argv[1:]) if os.environ.get('HARNESS_HERMES_REGISTRY') else main(sys.argv[1:]))
 except Exception:
  print('PROXY_INTERNAL_FAIL_CLOSED',file=sys.stderr); sys.exit(125)
