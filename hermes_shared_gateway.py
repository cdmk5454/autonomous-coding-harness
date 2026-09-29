"""One pinned-home gateway; run-scoped mounts/fencing via the public Docker CLI.

No Hermes imports, private DB access or model configuration. Gateway lifetime is
shared; a run's close revokes that binding and drains only its proxy/container IO.
"""
import json,os,secrets,threading,time,subprocess,uuid
from pathlib import Path
from dataclasses import asdict
from urllib.parse import quote
from hermes_boundary import atomic,read,write_once,DOCKER
from hermes_process_boundary import OwnedProcess,identity
from hermes_runtime_adapter import HermesError

class SharedRunProcess:
    def __init__(self,gateway,boundary,adapter):
        self.gateway,self.boundary,self.adapter=gateway,boundary,adapter
        self.pid=gateway.process.pid
    def members(self):return self.gateway.process.members()
    def poll(self):return self.gateway.process.poll()
    def close_boundary(self):
        # Boundary.fence has already durably revoked the execution epoch.
        if read(self.boundary.root/'state.json').get('state')!='FENCING':
            raise HermesError('SHARED_RUN_REVOKE_REQUIRED')
        if self.adapter.run_id:
            try:self.adapter.request('POST','/v1/runs/'+quote(self.adapter.run_id,safe='')+'/stop',{})
            except HermesError:pass # Proxy revocation + OS writer quiescence remain mandatory.
        deadline=time.monotonic()+30
        while True:
            ids=subprocess.check_output([DOCKER,'ps','-aq','--no-trunc','--filter','label=harness-binding='+self.boundary.binding.binding_id],text=True,timeout=10).split()
            for cid in ids:subprocess.run([DOCKER,'rm','-f',cid],capture_output=True,timeout=10,check=False)
            pending=list(self.boundary.root.glob('shared-proxy-*.json'))
            live=[]
            for p in pending:
                record=read(p);old=record['process'];current=identity(old['pid'])
                if current.get('error_code')==5 or (current.get('alive') and current.get('creation_time_100ns')==old.get('creation_time_100ns')):live.append(old)
            if not live and not ids:break
            if time.monotonic()>=deadline:raise HermesError('SHARED_RUN_IO_QUIESCENCE_UNKNOWN')
            time.sleep(.1)
        atomic(self.boundary.root/'shared-run-detached.json',{'binding':asdict(self.boundary.binding),'gateway_pid':self.pid,'gateway_terminated':False,'pending_io':0,'native_run_id':self.adapter.run_id,'epoch_revoked':True})
        # No per-run ownership of gateway members. Their lifetime belongs to SharedGateway.
        return []

class SharedGateway:
    def __init__(self,root,repository):
        from bounded_execution import enabled,capacity
        if not enabled(repository):raise HermesError('BOUNDED_SCHEMA_REQUIRED')
        self.root=Path(root).resolve();self.root.mkdir(exist_ok=False)
        (self.root/'sessions').mkdir()
        self.repository=repository;self.cap=capacity(repository)
        self.guard=threading.Lock();self.process=None;self.log=None;self.config_bytes=None
        self.key=secrets.token_hex(32);self.health=None;self.closed=False
    def attach(self,scope,boundary,adapter):
        from hermes_execution_factory import HOME,RUNTIME
        session='HS-'+uuid.uuid4().hex
        adapter.initial_session_id=session
        write_once(self.root/'sessions'/(session+'.json'),{'boundary':str(boundary.root),'binding':asdict(boundary.binding),'policy_sha256':boundary.policy_digest})
        with self.guard:
            if self.closed:raise HermesError('SHARED_GATEWAY_CLOSED')
            if self.process is not None and self.process.poll() is not None:
                self.repository.set_global_stop('SHARED_RUNTIME_INFRASTRUCTURE_UNAVAILABLE')
                raise HermesError('SHARED_GATEWAY_INFRASTRUCTURE_FAILURE')
            if self.process is None:
                platforms='local telegram discord whatsapp whatsapp_cloud slack signal mattermost matrix homeassistant email sms dingtalk webhook msgraph_webhook feishu wecom wecom_callback weixin bluebubbles qqbot yuanbao relay'.split()
                cfg='platforms:\n'+''.join('  '+p+':\n    enabled: false\n' for p in platforms)
                cfg+='  api_server:\n    enabled: true\n    extra:\n      host: 127.0.0.1\n      port: '+str(scope.config.port)+'\n'
                cfg+='gateway:\n  api_server:\n    max_concurrent_runs: '+str(self.cap)+'\napprovals:\n  mode: manual\nplatform_toolsets:\n  api_server: [terminal, file]\nagent:\n  max_turns: 12\nfallback_models: []\n'
                try:
                    with (HOME/'config.yaml').open('x',encoding='utf8',newline='') as f:f.write(cfg)
                except FileExistsError:raise HermesError('HERMES_EXISTING_CONFIG_REQUIRES_EXPLICIT_INTEGRATION') from None
                self.config_bytes=cfg.encode('utf8')
                write_once(self.root/'gateway-config.json',{'config':cfg,'temporary':True,'cap_source':'execution_capacity','global_model':False})
                env=scope._environment(boundary,self.key)
                for k in ('HARNESS_HERMES_POLICY','HARNESS_HERMES_POLICY_SHA256','HARNESS_HERMES_EPOCH'):env.pop(k,None)
                env.update(HARNESS_HERMES_REGISTRY=str(self.root),TERMINAL_DOCKER_VOLUMES='[]',TERMINAL_CONTAINER_PERSISTENT='false')
                self.log=open(self.root/'gateway-private.log','ab')
                py=str(RUNTIME/'venv/Scripts/python.exe')
                self.process=OwnedProcess(py,[py,'-m','hermes_cli.main','gateway','run','--no-supervise','--quiet'],env,str(RUNTIME),self.log)
                for _ in range(60):
                    if self.process.poll() is not None:raise HermesError('SHARED_GATEWAY_INFRASTRUCTURE_FAILURE')
                    try:
                        health=adapter.request('GET','/health/detailed')
                        if health.get('pid') not in self.process.members():raise HermesError('HERMES_FOREIGN_GATEWAY')
                        self.health=health;break
                    except HermesError:time.sleep(.5)
                else:raise HermesError('HERMES_GATEWAY_UNAVAILABLE')
                write_once(self.root/'process.json',{'process':identity(self.process.pid),'server':identity(self.health['pid']),'runtime':str(RUNTIME),'home':str(HOME),'max_concurrent_runs':self.cap,'ownership':'SHARED_INFRASTRUCTURE_NOT_JOB_WRITER'})
            view=SharedRunProcess(self,boundary,adapter)
            boundary.bind_server(view,self.health['pid'])
    def close(self):
        from hermes_execution_factory import HOME
        with self.guard:
            if self.closed:return
            for p in (self.root/'sessions').glob('HS-*.json'):
                root=Path(read(p)['boundary']);proof=root/'quiescence.json'
                if not proof.is_file() or read(proof).get('state')!='QUIESCED':
                    raise HermesError('SHARED_GATEWAY_ACTIVE_BOUNDARIES')
            if self.process is not None:
                self.process.close_boundary();self.process.wait()
            if self.log:self.log.close()
            if self.config_bytes is not None:
                config=HOME/'config.yaml'
                if not config.is_file() or config.read_bytes()!=self.config_bytes:raise HermesError('HERMES_TEMP_CONFIG_CHANGED')
                config.unlink()
            self.closed=True
            atomic(self.root/'cleanup.json',{'state':'CLOSED','all_boundaries_quiesced':True})
