import copy
import hashlib
import json
import unittest
from test_0914_reconciliation import CorrectiveGateTests
from control_repository import digest, CANDIDATE_HANDOFF_CONFIRMATION


class TechnicalTakeoverTests(CorrectiveGateTests):
    def chain(self):
        self.takeover()
        f = self.f
        parent = f.edit_job(f.target,status='FAILED_FINAL',last_result={
            'failure_origin':'CONTROL','failure_code':'PROFILE_DRIFT','task_id':'TASK-OLD'})
        folder = f.control_root/'cumulative/job-snapshots'/f.baseline/'INPUT'
        folder.mkdir(parents=True)
        entries = []
        for index,(name,value) in enumerate(f.hashes.items()):
            blob = folder/str(index)
            blob.write_bytes((f.product/name).read_bytes())
            entries.append({'path':name,'sha256':value,'state':'PRESENT','snapshot':str(index)})
        manifest = folder/'manifest.json'
        manifest.write_text(json.dumps({'kind':'PRE_JOB_SNAPSHOT','job_id':f.target,
            'snapshot_id':'INPUT','baseline_id':f.baseline,'files':entries}))
        fresh = {'kind':'VERIFIED_NO_TASK_DELTA_FRESH_INPUT','parent_job_id':f.target,
            'parent_revision':parent['revision'],'snapshot_id':'INPUT','manifest_path':str(manifest),
            'manifest_sha256':hashlib.sha256(manifest.read_bytes()).hexdigest(),
            'active_baseline_id':f.baseline,'generation':f.repo.read_queue()['generation']}
        target = f.edit_job(f.other,corrects_job_id=f.target,replaces_job_id=f.target,fresh_input=fresh)
        q = f.repo.read_queue()
        rev = q['revision']
        q['qa_corrective_successors'][f.source]['state'] = 'FAILED'
        q['replacement_anchors'] = {f.target:{'replacement_job_id':f.other,'kind':'FAILED_FINAL_CORRECTIVE'}}
        q['corrective_handoff'] = {'failed_job_id':f.target,'corrective_job_id':f.other,'state':'ACTIVE'}
        f.repo.write_queue(q,expected_revision=rev)
        f.control.handoff_candidate_ownership(source_job_id=f.target,target_job_id=f.other,
            expected_source_revision=parent['revision'],expected_target_revision=target['revision'],
            candidate_scope=sorted(f.hashes),expected_candidate_hash=digest(f.hashes),
            confirmation=CANDIDATE_HANDOFF_CONFIRMATION,request_id='technical-takeover')

    def test_valid_technical_takeover(self):
        self.chain()
        self.reconcile(False)
        self.assertEqual('QUEUED',self.f.repo.read_job(self.f.other)['status'])

    def test_missing_successor_blocks(self):
        self.chain()
        self.set_link(corrective_job_id='')
        self.reconcile(True)

    def test_invalid_handoff_blocks(self):
        self.chain()
        self.f.edit_job(self.f.other,request={'target_resources':['foreign/file']})
        self.reconcile(True)

    def test_history_and_owner_immutable(self):
        self.chain()
        before = [copy.deepcopy(self.f.repo.read_job(x)) for x in (self.f.source,self.f.target)]
        links = copy.deepcopy(self.f.repo.read_queue()['qa_corrective_successors'])
        owners = self.f.repo.preserved_candidate_records(self.f.target)
        self.reconcile(False)
        self.assertEqual(before,[self.f.repo.read_job(x) for x in (self.f.source,self.f.target)])
        self.assertEqual(links,self.f.repo.read_queue()['qa_corrective_successors'])
        self.assertEqual(owners,self.f.repo.preserved_candidate_records(self.f.target))

    def test_independent_pause_blocks(self):
        self.chain()
        q = self.f.repo.read_queue()
        self.f.store.pause('Independent operator hold',expected_revision=q['revision'],
                          request_id='independent-operator-hold',confirmation='I_CONFIRM_PAUSE_QUEUE')
        self.supervisor._reconcile_interrupted()
        after = self.f.repo.read_queue()
        self.assertTrue(after['paused'])
        self.assertTrue(after['operator_paused'])
        self.assertEqual('',after['blocked_by_job_id'])

    def test_historical_batch_hold_excluded(self):
        self.chain()
        self.assertEqual('BATCH',self.f.repo.read_job(self.f.source)['hold_scope'])
        self.reconcile(False)
        self.assertEqual('BATCH',self.f.repo.read_job(self.f.source)['hold_scope'])
