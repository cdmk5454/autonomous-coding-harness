"""Ownership-only hotfix: disposable SQLite and candidate bytes, no Worker."""
import asyncio
import copy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import harness_temp
from chatgpt_control import ChatGPTControlService
from chatgpt_control_mcp import build_mcp_server, _safe_call
from control_repository import ControlRepository, RepositoryError, digest, CANDIDATE_HANDOFF_CONFIRMATION
from harness_service import HarnessService, HarnessServiceError
from job_queue import JobStore, QueueError, QA_CORRECTIVE_CONFIRMATION
from operator_notifications import terminal_projection
from test_job_control import request, valid_enqueue

FILES = [
    'suc/src/main/java/suc/sp/query/SsucSp0020Qry.xml',
    'suc/src/main/java/suc/sp/service/SsucSp0020Svc.java',
    'suc/src/test/java/suc/sp/service/SsucSp0020SurveyBaseTest.java',
    'sucui/src/pages/admin/sp/SsucSp0020StatSettingPop.vue',
]


class CandidateHandoffTests(unittest.TestCase):
    def setUp(self):
        self.temp = harness_temp.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.product = self.root/'product'
        self.control_root = self.root/'.control'
        legacy = JobStore(self.control_root)
        contracts = [request(client_job_id=name, target_modules=['suc','sucui'], target_resources=FILES)
                     for name in ('HISTORICAL','CORRECTIVE','UNRELATED')]
        jobs, _ = valid_enqueue(legacy, 'handoff-fixture', contracts)
        self.source, self.target, self.other = [job['job_id'] for job in jobs]
        self.baseline = 'BASELINE-FIXTURE'
        queue = legacy._read_queue()
        queue.update(active_baseline_id=self.baseline, running_job_id='',
                     blocked_by_job_id=self.source, gate_reason='AWAITING_QA', paused=True)
        for job in jobs:
            job['active_baseline_id'] = self.baseline
        jobs[0].update(status='AWAITING_QA', revision=14,
            last_result={'failure_code':'PROFILE_DRIFT','failure_stage':'CONTROL',
                         'success':False,'worktree_disposition':'UNKNOWN'}, hold_scope='BATCH')
        if getattr(self, 'canonical_authority', False):
            from control_migration import rehearse, cutover
            for job in jobs:
                legacy._write_job(job)
            legacy._write_queue(queue)
            database = self.root/'fixture-rehearsed.sqlite3'
            rehearsal = rehearse(self.control_root,database)
            self.assertTrue(rehearsal['pass'])
            self.assertEqual('VALID',cutover(self.control_root,database,rehearsal)['status'])
            self.repo = ControlRepository(self.control_root/'control.sqlite3')
        else:
            self.repo = ControlRepository(self.control_root/'control.sqlite3',create=True)
            self.repo.bootstrap(queue, {j['job_id']:j for j in jobs},
                                {j['job_id']:{'path':'jobs/'+j['job_id']+'.json'} for j in jobs})
            (self.control_root/'sqlite-authority.json').write_text('{}')
        self.hashes = {}
        for index, name in enumerate(FILES):
            path = self.product/name
            path.parent.mkdir(parents=True,exist_ok=True)
            path.write_bytes(('preserved fixture '+str(index)+'\r\n').encode())
            self.hashes[name] = hashlib.sha256(path.read_bytes()).hexdigest()
        self.repo.preserve_candidate_ownership(self.source,self.hashes,workspace=self.product,evidence_ref='fixture')
        self.store = JobStore(self.control_root, repository=self.repo)
        self.link()
        lock = SimpleNamespace(acquire=lambda:True,release=lambda:None)
        self.harness = HarnessService(self.product,self.root/'profile',task_root=self.root/'.tasks',instance_lock=lock)
        self.control = object.__new__(ChatGPTControlService)
        self.control.read_only = False
        self.control.store = self.store
        self.control.harness = self.harness
        self.control.supervisor = SimpleNamespace(is_running=lambda:False)
        self.control._assert_configuration_current = lambda:None
        self.integrity = {'active_baseline_id':self.baseline,'baseline_declaration_integrity':True,
                          'external_frozen_integrity':True,'unexpected_runtime_dirty_files':[]}
        self.control._canonical_integrity_snapshot = lambda queue:dict(self.integrity)
        self.args = dict(source_job_id=self.source,target_job_id=self.target,
            expected_source_revision=self.repo.read_job(self.source)['revision'],
            expected_target_revision=self.repo.read_job(self.target)['revision'],
            candidate_scope=sorted(FILES),expected_candidate_hash=digest(self.hashes),
            confirmation=CANDIDATE_HANDOFF_CONFIRMATION,request_id='handoff-0911')

    def link(self):
        self.store.record_qa_corrective_successor(self.source,self.target,
            expected_queue_revision=self.repo.read_queue()['revision'],
            expected_job_revision=self.repo.read_job(self.target)['revision'],
            request_id='explicit-link',reason='ownership handoff fixture',confirmation=QA_CORRECTIVE_CONFIRMATION)

    def snapshot(self):
        with self.repo.hold():
            return '\n'.join(self.repo._connection.iterdump())

    def bytes(self):
        return {name:(self.product/name).read_bytes() for name in FILES}

    def edit_job(self, job_id, **values):
        job = self.repo.read_job(job_id)
        revision = job['revision']
        job.update(values,revision=revision+1)
        self.repo.write_job(job,expected_revision=revision)
        return job

    def rejected(self, code, **changes):
        before, files = self.snapshot(),self.bytes()
        result = _safe_call(self.control.handoff_candidate_ownership,**dict(self.args,**changes))
        self.assertFalse(result['ok'],result)
        self.assertEqual(code,result['error']['code'])
        self.assertEqual(before,self.snapshot())
        self.assertEqual(files,self.bytes())
        self.assertFalse(self.harness._lease_held)

    def test_a_eligible_explicit_successor_transfer(self):
        preview = self.control.preview_candidate_ownership_handoff(self.source,self.target)
        self.assertTrue(preview['eligible'],preview)
        self.assertEqual(self.hashes,preview['candidate_hashes'])
        result = self.control.handoff_candidate_ownership(**self.args)
        self.assertFalse(result['replayed'])
        self.assertFalse(result['dispatched'])
        self.assertEqual([],self.repo.preserved_candidate_records(self.source))
        self.assertEqual(4,len(self.repo.preserved_candidate_records(self.target)))
        self.assertEqual(self.args['expected_source_revision']+1,
                         self.repo.read_job(self.source)['revision'])
        self.assertEqual(self.args['expected_target_revision']+1,self.repo.read_job(self.target)['revision'])

    def test_b_stale_source_revision(self):
        self.rejected('CONTROL_REVISION_CONFLICT',expected_source_revision=13)

    def test_c_stale_target_revision(self):
        self.rejected('CONTROL_REVISION_CONFLICT',expected_target_revision=0)

    def test_d_unlinked_target(self):
        self.rejected('TARGET_NOT_LINKED_CORRECTIVE',target_job_id=self.other,
                      expected_target_revision=self.repo.read_job(self.other)['revision'])

    def test_e_running_target(self):
        target = self.edit_job(self.target,status='RUNNING')
        self.rejected('CANDIDATE_TARGET_ALREADY_STARTED',expected_target_revision=target['revision'])

    def test_f_candidate_bytes_changed(self):
        (self.product/FILES[0]).write_bytes(b'changed')
        self.rejected('CANDIDATE_OWNERSHIP_INTEGRITY_FAILED')

    def test_g_third_party_case_alias_overlap(self):
        with self.repo.hold():
            self.repo._connection.execute('INSERT INTO ownership VALUES(?,?,?,?,?)',
                (FILES[0].upper(),self.other,self.hashes[FILES[0]],'PRE_EXISTING_UNVERIFIED_CANDIDATE','fixture'))
        self.rejected('THIRD_PARTY_CANDIDATE_SCOPE_CONFLICT')

    def test_h_historical_product_result_unchanged(self):
        before = self.repo.read_job(self.source)
        self.control.handoff_candidate_ownership(**self.args)
        after = self.repo.read_job(self.source)
        for key in set(before)-{'revision','updated_at','history'}:
            self.assertEqual(before[key],after[key],key)
        self.assertEqual(before['history'],after['history'][:-1])
        self.assertEqual('CANDIDATE_OWNERSHIP_HANDED_OFF',after['history'][-1]['event'])

    def test_i_candidate_bytes_unchanged(self):
        before = self.bytes()
        self.control.handoff_candidate_ownership(**self.args)
        self.assertEqual(before,self.bytes())

    def test_j_idempotent_original_cas_replay(self):
        first = self.control.handoff_candidate_ownership(**self.args)
        before = self.snapshot()
        repeated = self.control.handoff_candidate_ownership(**self.args)
        self.assertTrue(repeated['replayed'])
        self.assertEqual(first['event'],repeated['event'])
        self.assertEqual(before,self.snapshot())

    def test_k_l_m_scope_guard_before_after_and_unrelated(self):
        manager = Mock()
        manager.prepare_job_snapshot.return_value = {'snapshot_id':'FIXTURE'}
        self.harness._cumulative_manager = lambda:manager
        with self.assertRaisesRegex(HarnessServiceError,'QUARANTINED_CANDIDATE_SCOPE_CONFLICT'):
            self.harness.prepare_job_snapshot(self.repo.read_job(self.target),self.repo.read_queue())
        manager.prepare_job_snapshot.assert_not_called()
        self.control.handoff_candidate_ownership(**self.args)
        self.assertEqual({'snapshot_id':'FIXTURE'},self.harness.prepare_job_snapshot(
            self.repo.read_job(self.target),self.repo.read_queue()))
        with self.assertRaisesRegex(HarnessServiceError,'QUARANTINED_CANDIDATE_SCOPE_CONFLICT'):
            self.harness.prepare_job_snapshot(self.repo.read_job(self.other),self.repo.read_queue())
        self.assertEqual('',self.store._quarantined_scope_conflict(self.repo.read_queue(),self.repo.read_job(self.target)))
        self.assertEqual(self.source,self.store._quarantined_scope_conflict(self.repo.read_queue(),self.repo.read_job(self.other)))

    def test_n_restart_preserves_ownership_and_provenance(self):
        result = self.control.handoff_candidate_ownership(**self.args)
        reopened = ControlRepository(self.repo.path,read_only=True)
        self.assertEqual(self.repo.worklist(),reopened.worklist())
        self.assertEqual(result['event'],reopened.read_job(self.source)['candidate_ownership_handoff'])
        self.assertEqual(result['event'],reopened.read_job(self.target)['candidate_ownership_received'])
        self.assertTrue(reopened.candidate_handoff_allows(self.source,self.target))
        self.assertEqual('VALID',reopened.health()['status'])

    def test_o_control_reporting_has_no_fake_product_success_or_notification(self):
        before = terminal_projection(self.repo.read_job(self.source),self.repo.read_queue())
        with patch('queue_supervisor.send_notify_evidence') as notify, patch('worker.CodexWorker.execute') as worker:
            result = _safe_call(self.control.handoff_candidate_ownership,**self.args)
            notify.assert_not_called()
            worker.assert_not_called()
        self.assertTrue(result['ok'])
        self.assertFalse(result['dispatched'])
        self.assertEqual(before,terminal_projection(self.repo.read_job(self.source),self.repo.read_queue()))

    def test_atomic_failure_after_ownership_update_rolls_back_everything(self):
        with self.repo.hold():
            self.repo._connection.execute("CREATE TRIGGER injected_failure AFTER UPDATE ON ownership BEGIN SELECT RAISE(ABORT,'injected'); END")
        self.rejected('CONTROL_INTERNAL_ERROR')

    def test_second_job_write_failure_rolls_back_first(self):
        original = self.repo.write_job
        def write(job, **kwargs):
            if job['job_id']==self.target:
                raise RepositoryError('CONTROL_REVISION_CONFLICT')
            return original(job,**kwargs)
        with patch.object(self.repo,'write_job',side_effect=write):
            self.rejected('CONTROL_REVISION_CONFLICT')

    def test_other_connection_never_sees_intermediate_transfer(self):
        original = self.repo.write_job
        before = self.repo.candidate_ownership(self.product)
        observed = []
        def write(job,**kwargs):
            original(job,**kwargs)
            observed.append(ControlRepository(self.repo.path,read_only=True).candidate_ownership(self.product))
        with patch.object(self.repo,'write_job',side_effect=write):
            self.control.handoff_candidate_ownership(**self.args)
        self.assertEqual([before,before],observed)

    def test_wrong_scope_hash_and_confirmation(self):
        self.rejected('CANDIDATE_SCOPE_MISMATCH',candidate_scope=FILES[:-1])
        self.rejected('CANDIDATE_HASH_MISMATCH',expected_candidate_hash='0'*64)
        self.rejected('CANDIDATE_HANDOFF_CONFIRMATION_REQUIRED',confirmation='')

    def test_integrity_and_execution_lease_fail_closed(self):
        for field, value in [('baseline_declaration_integrity',False),('external_frozen_integrity',False),
                             ('unexpected_runtime_dirty_files',['unknown'])]:
            with self.subTest(field=field),patch.dict(self.integrity,{field:value}):
                self.rejected('CANDIDATE_HANDOFF_INTEGRITY_INVALID')
        with patch.object(self.harness,'acquire_execution_lease',return_value=False):
            self.rejected('ACTIVE_EXECUTION')

    def test_missing_target_preview_is_read_only_and_reports_link_requirements(self):
        before = self.snapshot()
        preview = self.control.preview_candidate_ownership_handoff(self.source)
        self.assertFalse(preview['eligible'])
        self.assertEqual(['LINKED_CORRECTIVE_TARGET_REQUIRED'],preview['conflicts'])
        self.assertEqual('record_qa_corrective_successor',preview['target_requirements']['link_operation'])
        self.assertEqual(before,self.snapshot())

    def test_different_target_after_success_is_rejected(self):
        self.control.handoff_candidate_ownership(**self.args)
        self.rejected('CANDIDATE_ALREADY_HANDED_OFF',target_job_id=self.other)

    def test_source_running_and_ineligible_classification(self):
        source = self.edit_job(self.source,status='RUNNING')
        self.rejected('CANDIDATE_SOURCE_NOT_HISTORICAL',expected_source_revision=source['revision'])

    def test_target_already_owns_candidate(self):
        target = self.edit_job(self.target,candidate_id='EXISTING')
        self.rejected('CANDIDATE_TARGET_ALREADY_OWNS_CANDIDATE',expected_target_revision=target['revision'])

    def test_ineligible_disposition(self):
        with self.repo.hold():
            self.repo._connection.execute("UPDATE ownership SET classification='VERIFIED_CLEAN'")
        self.rejected('CANDIDATE_DISPOSITION_INELIGIBLE')

    def test_missing_source_target_and_invalid_revisions(self):
        self.rejected('JOB_NOT_FOUND',source_job_id='JOB-MISSING')
        self.rejected('JOB_NOT_FOUND',target_job_id='JOB-MISSING')
        self.rejected('CANDIDATE_HANDOFF_INPUT_REQUIRED',expected_source_revision=True)
        self.rejected('CANDIDATE_HANDOFF_INPUT_REQUIRED',expected_target_revision=None)

    def test_baseline_identity_mismatch_and_target_scope(self):
        target = self.edit_job(self.target,active_baseline_id='OTHER')
        self.rejected('CANDIDATE_HANDOFF_INTEGRITY_INVALID',expected_target_revision=target['revision'])
        target = self.edit_job(self.target,active_baseline_id=self.baseline,
            request=dict(target['request'],target_resources=[FILES[0]]))
        self.rejected('CANDIDATE_OUTSIDE_TARGET_SCOPE',expected_target_revision=target['revision'])

    def test_previously_started_target_and_third_running_job(self):
        target = self.edit_job(self.target,task_ids=['OLD-TASK'])
        self.rejected('CANDIDATE_TARGET_ALREADY_STARTED',expected_target_revision=target['revision'])
        target = self.edit_job(self.target,task_ids=[])
        self.edit_job(self.other,status='RUNNING')
        self.rejected('ACTIVE_EXECUTION',expected_target_revision=target['revision'])

    def test_target_previously_materialized(self):
        from test_job_control import execution_context
        target = self.repo.read_job(self.target)
        self.repo.materialize(target,context=execution_context(),policy={'effective_policy_sha256':'fixture'},
                              baseline_hash='baseline',execution_surface_hash='surface')
        self.rejected('CANDIDATE_TARGET_ALREADY_STARTED')

    def test_read_only_surface_refuses_mutation(self):
        self.control.read_only = True
        self.rejected('CONTROL_SURFACE_READ_ONLY')

    def test_mcp_preview_write_registration_and_call(self):
        async def check():
            writable = build_mcp_server(self.control)
            inspector = build_mcp_server(self.control,read_only_surface=True)
            writable_names = {tool.name for tool in await writable.list_tools()}
            inspector_names = {tool.name for tool in await inspector.list_tools()}
            self.assertIn('handoff_candidate_ownership',writable_names)
            self.assertNotIn('handoff_candidate_ownership',inspector_names)
            self.assertIn('preview_candidate_ownership_handoff',inspector_names)
            await writable.call_tool('handoff_candidate_ownership',self.args)
            self.assertTrue(self.repo.candidate_handoff_allows(self.source,self.target))
        asyncio.run(check())


if __name__=='__main__':
    unittest.main()
