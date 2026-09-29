"""Check actual inspect receipts against adversarial security field changes."""
import copy,json,pathlib,unittest
from hermes_boundary import inspect_receipt,read,TMPFS
from runtime_selection import select_runtime,RuntimeSelectionError
from hermes_runtime_adapter import HermesError
from types import SimpleNamespace
from manager import Manager

class HermesBoundaryContractTests(unittest.TestCase):
    def test_unbound_manager_never_falls_back(self):
        manager=Manager.__new__(Manager);manager.worker_type='hermes'
        task=SimpleNamespace(worker='hermes',retry_worker='')
        with self.assertRaisesRegex(HermesError,'OWNER_REQUIRED'):manager._get_worker(task)
    def test_selection_requires_invocation_model(self):
        with self.assertRaises(RuntimeSelectionError):select_runtime('hermes','')
        s=select_runtime('hermes','caller-model');self.assertEqual('none',s.fallback_runtime)
    def test_job_contract_and_frozen_role_catalogue(self):
        from test_job_control import request
        from job_contract import JobRequest,JobContractError
        from runtime_contract import descriptor_for,freeze_runtime_contracts
        for route in ('zai/glm-5','opencode-go/glm-5.3-flash','openai-codex/gpt-5.6-luna'):
            j=request(worker='hermes',model=route)
            self.assertEqual(route,j.model)
        with self.assertRaises(JobContractError):
            request(worker='hermes',model='')
        d=descriptor_for('hermes');c=freeze_runtime_contracts(worker=d,reviewer=d)
        self.assertNotEqual(c['roles']['WORKER']['permission_tool_profile'],c['roles']['REVIEWER']['permission_tool_profile'])

    def test_actual_receipt_and_security_mutations(self):
        root=pathlib.Path(r'D:\hermes\step6')
        receipts=[p for p in root.glob('fixture-*/work/boundary/inspect-*.json') if read(p)['state']=='BOUNDARY_VERIFIED']
        self.assertTrue(receipts,'actual Docker inspect evidence required')
        r=read(receipts[0]);p=read(receipts[0].parent/'policy.json')
        obj={'Id':r['container_id'],'HostConfig':r['host_config'],'Mounts':r['mounts'],'Image':p['image'],'Config':{'Labels':{'harness-binding':p['binding']['binding_id']}}}
        # Historical Step6 receipt predates the explicit /opt/data tmpfs.
        # Preserve that evidence unchanged and require the new contract to reject it.
        self.assertEqual('INSPECT_MISMATCH',inspect_receipt(obj,p)['state'])
        obj=copy.deepcopy(obj)
        obj['HostConfig']['Tmpfs']=dict(x.split(':',1) for x in TMPFS)
        self.assertEqual('BOUNDARY_VERIFIED',inspect_receipt(obj,p)['state'])
        for key,value in [('Privileged',True),('CapAdd',['CAP_SYS_ADMIN']),('CapDrop',[]),('SecurityOpt',[]),('NetworkMode','host'),('PidMode','host'),('IpcMode','host'),('Devices',[{'PathOnHost':'/dev/sda'}]),('DeviceRequests',[{'Count':-1}]),('DeviceCgroupRules',['a *:* rwm']),('PidsLimit',0),('Memory',0),('NanoCpus',0),('ShmSize',0),('Tmpfs',{})]:
            with self.subTest(field=key):
                altered=copy.deepcopy(obj);altered['HostConfig'][key]=value
                self.assertEqual('INSPECT_MISMATCH',inspect_receipt(altered,p)['state'])
        for change in ('rw','source','destination','extra'):
            with self.subTest(mount=change):
                altered=copy.deepcopy(obj)
                if change=='rw':altered['Mounts'][0]['RW']=not altered['Mounts'][0]['RW']
                if change=='source':altered['Mounts'][0]['Source']='D:/agents'
                if change=='destination':altered['Mounts'][0]['Destination']='/unknown'
                if change=='extra':altered['Mounts'].append({'Type':'volume','Source':'x','Destination':'/x','RW':True})
                self.assertEqual('INSPECT_MISMATCH',inspect_receipt(altered,p)['state'])
if __name__=='__main__':unittest.main()
