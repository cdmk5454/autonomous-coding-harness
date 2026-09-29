"""Hermes adapter failure/identity tests using the real schema-5 ledger."""
import json, unittest, uuid
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock
import test_099_execution_runtime as fixtures
from hermes_runtime_adapter import (HermesBinding,HermesRuntimeAdapter,HermesError,
    CancellationResult,CancellationState)

class HermesAdapterTests(unittest.TestCase):
    def setUp(self):
        self.f=fixtures.RuntimeRepositoryTests();self.f.setUp();self.addCleanup(self.f.doCleanups)
        self.repo=self.f.repo
        self.b=HermesBinding(self.f.job['job_id'],self.f.execution['execution_id'],self.f.attempt['attempt_id'],
            'binding','command','candidate','d'*64,'epoch','incarnation','zai',self.f.attempt['model'],'WORKER','host:creation')
        self.boundary=Mock();self.boundary.assert_server=Mock();self.boundary.candidate=Path(self.f.temp.name)
        self.boundary.receipt.return_value={'binding':'fixture'}
        self.a=HermesRuntimeAdapter(binding=self.b,repository=self.repo,boundary=self.boundary,
            base_url='http://127.0.0.1:18646',api_key='fixture-only')
        self.calls=[]
        self.status={'run_id':'run1','session_id':'session1','status':'running','model':self.b.model}
        def request(method,path,body=None,**kwargs):
            self.calls.append((method,path,body,kwargs))
            return {'run_id':'run1'} if method=='POST' and path=='/v1/runs' else dict(self.status)
        self.a.request=request
    def invocation(self,prompt='fixture'):
        return self.a.create_session(prompt,cwd=self.boundary.candidate,model=self.b.model,command_id=self.b.command_id,reasoning_effort='low')
    def start(self):
        i=self.invocation();self.a.dispatch(i);return i
    def test_invocation_model_and_provider_not_global(self):
        i=self.invocation();self.assertEqual('zai',i.body['provider']);self.assertEqual(self.b.model,i.body['model']);self.assertEqual('low',i.body['model_options']['reasoning_effort'])
    def test_wrong_model_rejected(self):
        with self.assertRaisesRegex(HermesError,'INVOCATION_BINDING'):self.a.create_session('x',cwd=self.boundary.candidate,model='other',command_id=self.b.command_id)
    def test_wrong_candidate_rejected(self):
        with self.assertRaisesRegex(HermesError,'CANDIDATE'):self.a.create_session('x',cwd='Z:/foreign',model=self.b.model,command_id=self.b.command_id)
    def test_wrong_attempt_rejected(self):
        self.a.binding=replace(self.b,attempt_id='foreign')
        with self.assertRaisesRegex(HermesError,'FOREIGN_ATTEMPT'):self.invocation()
    def test_finished_attempt_rejected(self):
        self.repo.finish_attempt(self.b.attempt_id,status='FAILED')
        with self.assertRaisesRegex(HermesError,'LATE'):self.invocation()
    def test_ledger_replay_does_not_post_again(self):
        i=self.start();n=len(self.calls);r=self.a.dispatch(i)
        self.assertTrue(r['replayed']);self.assertEqual(n,len(self.calls))
    def test_changed_payload_conflict(self):
        self.start()
        with self.assertRaisesRegex(HermesError,'IDEMPOTENCY_CONFLICT'):self.a.dispatch(self.invocation('changed'))
    def test_unknown_delivery_never_resends(self):
        def fail(*args,**kwargs):raise HermesError('HERMES_TRANSPORT_UNKNOWN')
        self.a.request=fail;i=self.invocation()
        with self.assertRaises(HermesError):self.a.dispatch(i)
        self.assertEqual('UNKNOWN',self.repo.command(self.b.command_id)['delivery_status'])
        with self.assertRaisesRegex(HermesError,'RECONCILIATION_REQUIRED'):self.a.dispatch(i)
    def test_admission_failure_precedes_send(self):
        self.boundary.admit.side_effect=HermesError('ADMISSION_DENIED')
        with self.assertRaises(HermesError):self.a.dispatch(self.invocation())
        self.assertEqual([],self.calls);self.assertIsNone(self.repo.command(self.b.command_id))
    def test_stale_epoch_rejected(self):
        self.boundary.validate_binding.side_effect=HermesError('STALE_EPOCH')
        with self.assertRaisesRegex(HermesError,'STALE_EPOCH'):self.invocation()
    def test_foreign_status_run_rejected(self):
        self.start();self.status['run_id']='foreign'
        with self.assertRaisesRegex(HermesError,'FOREIGN_RUN'):self.a.observe_status()
    def test_foreign_status_session_rejected(self):
        self.start();self.status['session_id']='foreign'
        with self.assertRaisesRegex(HermesError,'FOREIGN_SESSION'):self.a.observe_status()
    def test_completed_not_job_or_attempt_success(self):
        self.start();self.status['status']='completed';self.a.observe_status()
        self.assertEqual('RUNNING',self.repo.attempt_outcome(self.b.attempt_id)['status'])
    def test_completed_event_not_success(self):
        self.start();events=self.a.normalize_event({'run_id':'run1','session_id':'session1','type':'completed'})
        self.assertIsNone(events[0].success);self.assertFalse(events[0].terminal)
    def test_duplicate_event_rejected(self):
        self.start();ev={'run_id':'run1','session_id':'session1','type':'completed'};self.a.normalize_event(ev)
        with self.assertRaisesRegex(HermesError,'DUPLICATE'):self.a.normalize_event(ev)
    def test_event_foreign_session_rejected(self):
        self.start()
        with self.assertRaisesRegex(HermesError,'FOREIGN'):self.a.normalize_event({'run_id':'run1','session_id':'other'})
    def test_no_native_sequence_in_receipt(self):
        self.start();r=self.a._record({'type':'progress'},'TRANSIENT_SSE')
        self.assertEqual('',r['sequence']);self.assertEqual('',r['provider_event_id']);self.assertEqual('AMBIGUOUS',r['status'])
    def test_sse_gap_is_recovery_domain(self):
        self.start();self.a.gap()
        self.assertEqual('RUNNING',self.repo.attempt_outcome(self.b.attempt_id)['status'])
    def test_stop_ack_not_quiescence(self):
        self.start();r=self.a.interrupt('session1','run1');self.assertEqual(CancellationState.ACKNOWLEDGED,r.state)
        with self.assertRaises(TypeError):bool(r)
    def test_stop_transport_error_is_unknown(self):
        self.start();self.a.request=Mock(side_effect=HermesError('HERMES_TRANSPORT_UNKNOWN'))
        self.assertEqual(CancellationState.UNKNOWN,self.a.interrupt('session1').state)
    def test_invalid_approval_rejected(self):
        self.start()
        with self.assertRaisesRegex(HermesError,'INVALID_APPROVAL'):self.a.respond_interaction('session1','old',{'choice':'once'})
    def test_broad_approval_rejected(self):
        self.start();self.status.update(status='waiting_for_approval',pending_approval={'request_id':'req'})
        with self.assertRaisesRegex(HermesError,'INVALID_APPROVAL'):self.a.respond_interaction('session1','req',{'choice':'always'})
    def test_approval_exact_action_authorized(self):
        self.start();self.status.update(status='waiting_for_approval',pending_approval={'request_id':'req','command':'fixture'})
        self.assertTrue(self.a.respond_interaction('session1','req',{'choice':'deny'}));self.boundary.authorize_interaction.assert_called_once()
    def test_terminal_approval_rejected(self):
        self.start();self.status['status']='completed';self.a.observe_status()
        with self.assertRaisesRegex(HermesError,'TERMINAL_INTERACTION'):self.a.respond_interaction('session1','req',{'choice':'deny'})
    def test_question_keeps_human_input(self):
        q=self.a.structured_outcome('{"outcome":"NEEDS_INPUT","question":"Which meaning?"}')
        self.assertEqual('Which meaning?',q['question'])
    def test_native_success_is_not_structured_product_success(self):
        with self.assertRaises(HermesError):self.a.structured_outcome('{"outcome":"SUCCEEDED"}')
    def test_replay_and_fork_unsupported(self):
        for call in (lambda:self.a.replay_events('s',cursor='1'),lambda:self.a.fork_session('s')):
            with self.assertRaises(HermesError):call()
    def test_key_includes_attempt(self):
        self.assertNotEqual(self.b.idempotency_key,replace(self.b,attempt_id='other').idempotency_key)
    def test_three_provider_routes_are_invocation_scoped(self):
        for provider,model in [('zai','glm-5'),('opencode-go','glm-5.3-flash'),('openai-codex','gpt-5.6-luna')]:
            aid='route-'+provider
            self.repo.start_attempt(self.f.execution,attempt_id=aid,role='WORKER',model=model)
            self.a.binding=replace(self.b,provider=provider,model=model,attempt_id=aid,command_id='cmd-'+provider)
            i=self.a.create_session('fixture',cwd=self.boundary.candidate,model=model,command_id='cmd-'+provider)
            self.assertEqual(provider,i.body['provider']);self.assertEqual(model,i.body['model'])
    def decision(self,quiesced):
        from dataclasses import asdict
        self.repo.record_runtime_recovery(self.b.execution_id,self.b.attempt_id,failure_code='HERMES_FENCING',
            evidence={'binding':asdict(self.b),'writer_quiescence':{'quiesced':quiesced}})
        return self.repo.record_decision(self.b.job_id,contract_revision=1,decision_type='CONTRACT',question='Fixture question',
            payload={'runtime_binding':self.b.fingerprint,'writer_quiesced':True,'native_session_id':'s'})
    def test_human_answer_does_not_imply_quiescence(self):
        d=self.decision(False);self.repo.resolve_decision(d['decision_id'],resolution='fixture answer')
        with self.assertRaisesRegex(Exception,'WRITER_QUIESCENCE_REQUIRED'):
            self.repo.resume_writer_after_decision(self.b.execution_id,d['decision_id'],contract_revision=1)
    def test_unanswered_question_cannot_resume(self):
        d=self.decision(True)
        with self.assertRaisesRegex(Exception,'NOT_AUTHORIZED'):
            self.repo.resume_writer_after_decision(self.b.execution_id,d['decision_id'],contract_revision=1)
    def test_changed_contract_cannot_resume_session(self):
        d=self.decision(True);self.repo.resolve_decision(d['decision_id'],resolution='fixture answer')
        with self.assertRaisesRegex(Exception,'CONTRACT_MISMATCH'):
            self.repo.resume_writer_after_decision(self.b.execution_id,d['decision_id'],contract_revision=2)
    def test_resolved_same_contract_and_fencing_resume_only_authority(self):
        d=self.decision(True);self.repo.resolve_decision(d['decision_id'],resolution='fixture answer')
        a=self.repo.resume_writer_after_decision(self.b.execution_id,d['decision_id'],contract_revision=1)
        self.assertEqual('ACTIVE',a['state']);self.assertEqual('RUNNING',self.repo.attempt_outcome(self.b.attempt_id)['status'])
    def test_native_sse_without_session_field_uses_bound_transport(self):
        self.start();e=self.a.normalize_event({'run_id':'run1','event':'tool.started'})[0]
        self.assertEqual('session1',e.session_id);self.assertEqual(self.b.execution_epoch,e.execution_epoch)
        self.assertEqual('',e.native_event_id);self.assertTrue(e.local_receipt_id)

    def test_remote_and_embedded_credentials_denied(self):
        for u in ('https://example.com','http://user:pass@127.0.0.1','http://localhost:1'):
            with self.assertRaises(HermesError):HermesRuntimeAdapter(binding=self.b,repository=self.repo,boundary=self.boundary,base_url=u,api_key='x')

if __name__=='__main__':unittest.main()

