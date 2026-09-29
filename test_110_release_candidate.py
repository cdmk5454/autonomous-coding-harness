"""Disposable candidate manifest -> process -> Execution -> Attempt provenance."""
from pathlib import Path
import json
import os
import unittest

from runtime_identity import RuntimeIdentityError, source_process_identity


class CandidateReleaseIdentityTest(unittest.TestCase):
    def test_candidate_manifest_process_execution_attempt_binding(self):
        root=Path(__file__).resolve().parent
        try:
            process=source_process_identity(root)
        except RuntimeIdentityError as exc:
            if exc.code in {'RUNTIME_SOURCE_DRIFT','RUNTIME_SOURCE_INVENTORY_DRIFT'}:
                self.skipTest('Live edited source intentionally awaits release promotion')
            raise
        from test_110_nworker_dispatch import NWorkerDispatchTest
        fixture=NWorkerDispatchTest('test_explicit_2_to_3_migration_and_three_real_claims')
        fixture.setUp();self.addCleanup(fixture.doCleanups)
        job=fixture.enqueue([fixture.request(1)])[0]
        claimed=fixture.store.claim_next('candidate-process')
        execution=fixture.repo.materialize(claimed,context=claimed['execution_context'],
            policy={'effective_policy_sha256':'f'*64},baseline_hash='e'*64,
            execution_surface_hash='d'*64,extra={
                'harness_source_identity':process['source'],
                'harness_materializer_identity':process})
        attempt=fixture.repo.start_attempt(execution,attempt_id='CANDIDATE-ATTEMPT',
            role='WORKER',model='gpt-5.6-sol')
        provenance=fixture.repo.attempt_provenance(attempt['attempt_id'])
        self.assertEqual(process['source'],provenance['source'])
        self.assertEqual(process['process_incarnation'],provenance['process_incarnation'])
        self.assertEqual(execution['execution_id'],provenance['execution_id'])
        self.assertEqual(attempt['attempt_id'],provenance['attempt_id'])
        output = os.environ.get('H11_RELEASE_EVIDENCE_PATH','')
        if output:
            destination=Path(output)
            destination.write_text(json.dumps({
                'schema':'candidate-release-process-execution-attempt/1',
                'source_fingerprint':process['source']['source_fingerprint'],
                'release_manifest_sha256':process['source']['release_manifest_sha256'],
                'process_incarnation':process['process_incarnation'],
                'execution_id':execution['execution_id'],
                'attempt_id':attempt['attempt_id'],
                'verified_binding':True,
                'candidate_only':True,
                'live_control_migrated':False,
            },indent=2)+'\n',encoding='utf-8')


if __name__=='__main__': unittest.main()
