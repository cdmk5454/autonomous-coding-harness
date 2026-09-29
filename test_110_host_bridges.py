"""Disposable executable and MCP boundary tests for optional host hooks."""
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from host_bridges import HostBridges, JsonCommand, MCPReadOnly
from harness_11.context import ContextBoundary, ToolIdentity
from harness_11.core import Binding, Mode, Resource, Risk, Scope
from harness_11.review_support import security_activation
from task_state import TaskState


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class HostBridgeIntegration(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='h11-host-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.candidate = self.root/'candidate'; self.canonical = self.root/'canonical'
        self.candidate.mkdir(); self.canonical.mkdir()
        (self.candidate/'a.py').write_text('def sample(): pass\n',encoding='utf-8')
        self.scope = Scope(str(self.candidate),str(self.canonical),('a.py',),(Resource('repo/source','READ'),))
        self.binding = Binding('J','E','A','C','a'*64,'b'*64,'c'*64,'TEST')
        self.identity = ToolIdentity('serena','mock','1','d'*64,'e'*64,'TEST')

    def script(self, body):
        path = self.root / f'worker-{len(list(self.root.glob("worker-*")))}.py'
        path.write_text(body,encoding='utf-8')
        return path

    def command(self, body, name='advisor'):
        path=self.script(body)
        return JsonCommand(name,{'path':sys.executable,'sha256':sha(sys.executable),
            'script':str(path),'script_sha256':sha(path),'version':'mock/1'})

    def mcp(self, *, readonly=True, drift=False, response='ok'):
        script=self.script('''
import json,sys
for line in sys.stdin:
    value=json.loads(line)
    method=value.get('method')
    if method=='initialize': result={'protocolVersion':'2025-03-26','capabilities':{},'serverInfo':{'name':'mock','version':'1'}}
    elif method=='tools/list': result={'tools':[{'name':'get_symbols_overview','inputSchema':{'type':'object','properties':{%s:{'type':'string'}}},'annotations':{'readOnlyHint':%s}}]}
    elif method=='tools/call': result={'isError':False,'content':%s}
    else: continue
    sys.stdout.write(json.dumps({'jsonrpc':'2.0','id':value['id'],'result':result})+'\\n');sys.stdout.flush()
''' % (repr('bad_arg' if drift else 'relative_path'), repr(readonly), repr(response)))
        return MCPReadOnly({'path':sys.executable,'sha256':sha(sys.executable),
            'script':str(script),'script_sha256':sha(script),'version':'mock/1','read_only':True},self.candidate)

    def test_json_command_real_process_and_failure_modes(self):
        good=self.command('import json,sys\njson.load(sys.stdin)\nprint(json.dumps({"analysis":"ok","suggestions":[],"cost_usd":None}))\n')
        self.assertEqual('ok',good.call({'case':'semantic'})['analysis'])
        malformed=self.command('print("{not-json")\n')
        with self.assertRaises(Exception): malformed.call({'case':'x'})
        oversized=self.command('print("x"*5000)\n')
        with self.assertRaises(Exception): oversized.call({'case':'x'},max_bytes=256)
        stalled=self.command('import time\ntime.sleep(3)\n')
        with self.assertRaises(Exception): stalled.call({'case':'x'},timeout=.1)
        with self.assertRaises(ValueError):
            JsonCommand('advisor',{'path':str(self.root/'missing.exe'),'sha256':'0'*64,'version':'1'})

    def test_mcp_handshake_tool_schema_and_fallback(self):
        boundary=ContextBoundary('serena',mode=Mode.SHADOW,identity=self.identity,
            transport=self.mcp(),timeout_seconds=3)
        result=boundary.query(self.binding,self.scope,'get_symbols_overview',
            {'relative_path':'a.py'},external_data_approved=True)
        self.assertEqual('READ_ONLY_OBSERVATION',result['status'])
        self.assertEqual('ok',result['content'])
        forbidden=boundary.query(self.binding,self.scope,'replace_symbol_body',{},
            external_data_approved=True,fallback=lambda:{'native':'ok'})
        self.assertEqual('SAFE_FALLBACK',forbidden['status'])
        self.assertIn('fallback_source_search',forbidden)
        for transport in (self.mcp(readonly=False),self.mcp(drift=True)):
            failing=ContextBoundary('serena',mode=Mode.SHADOW,identity=self.identity,
                transport=transport,timeout_seconds=3)
            observation=failing.query(self.binding,self.scope,'get_symbols_overview',
                {'relative_path':'a.py'},external_data_approved=True,
                fallback=lambda:{'native':'ok'})
            self.assertEqual('SAFE_FALLBACK',observation['status'])
            self.assertIn('fallback_source_search',observation)

    def test_semantic_host_hook_and_optional_review_disposition(self):
        advisor=self.command('import json,sys\njson.load(sys.stdin)\nprint(json.dumps({"analysis":"inspect","suggestions":[],"cost_usd":None}))\n')
        host=HostBridges(self.root,commands={'advisor':advisor})
        task=TaskState(task_id='T',requirement='Change a.py',worker='codex')
        task.job_id='J';task.execution_id='E';task.attempt_id='A';task.candidate_id='C'
        task.materialized_execution={'execution_id':'E'};task.changed_file_sha256={'a.py':'d'*64}
        task.retry_domain='TECHNICAL_EXECUTION_RETRY'
        ignored=host.semantic_failure(task,reason='TIMEOUT',event_id='1')
        self.assertEqual('TECHNICAL_EVENT_IGNORED',ignored['status'])
        self.assertFalse(list((self.root/'host-bridge-receipts').glob('*.json')))
        task.retry_domain='PRODUCT_SEMANTIC_RETRY'
        observed=host.semantic_failure(task,reason='REVIEW_FAILED',event_id='2')
        self.assertEqual('ADVISORY_EVIDENCE',observed['advisor']['status'])
        task.review_policy={'risk_level':'LOW','risk_tags':['AUTH']}
        review=host.review_shadow(task,SimpleNamespace(status='PASS',risk='LOW'))
        self.assertEqual('CREDENTIAL_REQUIRED_OPTIONAL_FEATURE',review['ocr_status'])
        self.assertEqual('OFF',review['security_status'])
        self.assertFalse(review['product_pass'])
        self.assertFalse(security_activation(risk=Risk.LOW,tags=frozenset({'AUTH'}),
            mode=Mode.SHADOW)['would_trigger'])

    def test_security_audit_only_for_explicit_or_relevant_high(self):
        audit=self.command('import json,sys\njson.load(sys.stdin)\nprint(json.dumps({"status":"OBSERVED"}))\n',name='security_audit')
        host=HostBridges(self.root,commands={'security_audit':audit})
        task=TaskState(task_id='T',requirement='Change a.py',worker='codex')
        task.job_id='J';task.execution_id='E';task.attempt_id='A';task.candidate_id='C'
        task.materialized_execution={'execution_id':'E'};task.changed_file_sha256={'a.py':'d'*64}
        task.changed_files=['a.py'];task.security_tags=['AUTH']
        low=host.review_shadow(task,SimpleNamespace(status='PASS',risk='LOW'))
        self.assertEqual('NOT_APPLICABLE',low['security_status'])
        high=host.review_shadow(task,SimpleNamespace(status='PASS',risk='HIGH'))
        self.assertEqual('RISK_GATED_SHADOW',high['security_status'])
        task.security_audit_requested=True
        explicit=host.review_shadow(task,SimpleNamespace(status='PASS',risk='LOW'))
        self.assertEqual('RISK_GATED_SHADOW',explicit['security_status'])

    def test_ocr_shadow_normalizes_findings_and_rejects_scope_drift(self):
        body='import json,sys\njson.load(sys.stdin)\nprint(json.dumps({"findings":[{"finding_id":"F1","rule_id":"R1","path":"a.py","line":1,"severity":"LOW","summary":"Review hint"}],"cost_usd":None}))\n'
        command=self.command(body,name='alibaba_review')
        host=HostBridges(self.root,commands={'alibaba_review':command})
        task=TaskState(task_id='T',requirement='Change a.py',worker='codex')
        task.job_id='J';task.execution_id='E';task.attempt_id='A';task.candidate_id='C'
        task.materialized_execution={'execution_id':'E'};task.changed_file_sha256={'a.py':'d'*64}
        task.changed_files=['a.py']
        observed=host.review_shadow(task,SimpleNamespace(status='PASS',risk='LOW'))
        self.assertEqual('SHADOW_EVIDENCE',observed['ocr_status'])
        self.assertEqual('F1',observed['ocr_findings'][0]['finding_id'])
        self.assertFalse(observed['product_pass'])
        task.changed_files=['other.py']
        rejected=host.review_shadow(task,SimpleNamespace(status='PASS',risk='LOW'))
        self.assertEqual('SAFE_FALLBACK',rejected['ocr_status'])

    def test_context_pack_fallback_preserves_original(self):
        host=HostBridges(self.root,commands={'repomix':self.command('print("{bad")\n',name='repomix')})
        task=TaskState(task_id='T',requirement='Change a.py',worker='codex')
        task.job_id='J';task.execution_id='E';task.attempt_id='A'
        task.materialized_execution={'execution_id':'E'}
        pack=SimpleNamespace(prompt='native valid context',manifest={'context_pack_id':'CTX-1',
            'selected_resources':[],'profile_snapshot_sha256':'a'*64,'profile_id':'p'})
        observed=host.context_observation(task,pack,workspace=self.candidate)
        self.assertEqual('SAFE_FALLBACK_OR_BUDGET',observed['repomix_status'])
        self.assertEqual('native valid context',pack.prompt)
        self.assertFalse(observed['context_replaced'])

    def test_host_bridge_requires_explicit_control_root_configuration(self):
        control = self.root/'inactive-control'
        control.mkdir()
        # A source-adjacent template and a credential are insufficient.  The
        # selected Control root must contain a valid explicit configuration.
        with patch.dict(os.environ, {'jev_key':'synthetic-disposable-key'}):
            self.assertIsNone(HostBridges.discover(control))
        from harness_service import HarnessService
        service = HarnessService(self.candidate, self.root/'profile',
            task_root=self.root/'tasks', snapshot_root=control/'snapshots')
        self.assertIsNone(service.host_bridges)
        self.assertFalse((control/'host-bridge-receipts').exists())

        configured = service.control_root/'host-bridges.json'
        configured.parent.mkdir(parents=True, exist_ok=True)
        configured.write_text(json.dumps({'schema':'h11-host-bridge/1',
            'control_root':str(service.control_root), 'jev_shadow_enabled':False,
            'commands':{}}), encoding='utf-8')
        discovered = HostBridges.discover(service.control_root)
        self.assertIsInstance(discovered, HostBridges)

    def test_context_foundation_does_not_add_bridge_observation_without_opt_in(self):
        from context_foundation import build_and_record_context
        pack=SimpleNamespace(prompt='native valid context',manifest={'context_pack_id':'CTX-1'})
        task=SimpleNamespace(task_id='T',job_id='J',execution_id='E',attempt_id='A',
            requirement='Change a.py',target_module=[],changed_files=[],task_mode='',
            control_repository_path=str(self.root/'inactive-control'/'control.sqlite3'),
            host_bridge_evidence=[])
        with patch('context_foundation.build_context_pack',return_value=pack):
            observed=build_and_record_context(task=task,role='WORKER',profile=object(),
                workspace=self.candidate,policy_root=self.root,manifest_root=None)
        self.assertIs(observed,pack)
        self.assertNotIn('host_bridge_observation',pack.manifest)
        self.assertEqual([],task.host_bridge_evidence)

    def test_jev_network_error_records_actual_decision_and_fallback(self):
        from harness_11.jev import JevAdapter, JevConfig
        def network_error(*args,**kwargs): raise OSError('synthetic offline')
        adapter=JevAdapter(JevConfig(model='jev-latest',mode=Mode.SHADOW,
            external_calls_authorized=True),transport=network_error,
            env={'JEV_API_KEY':'synthetic-disposable-key'})
        host=HostBridges(self.root,jev=adapter)
        result=host.materialized({'job_id':'J','request':{'worker':'codex'}},
            {'execution_id':'E','supervisor_admission':{'disposition':'NOT_REQUIRED'}},
            {'resolved_reviewer':{'risk_level':'LOW'},
             'supervisor_policy':{'supervisor_required':False}})
        self.assertEqual('codex',result['actual']['route'])
        self.assertFalse(result['actual']['needs_supervisor'])
        self.assertEqual('SAFE_FALLBACK',result['jev']['status'])
        self.assertFalse(result['jev']['applied'])

    def test_actual_harness_materialization_records_shadow_decision(self):
        from runtime_identity import RuntimeIdentityError, verify_source_manifest
        try: verify_source_manifest(Path(__file__).resolve().parent)
        except RuntimeIdentityError as exc:
            if exc.code in {'RUNTIME_SOURCE_DRIFT','RUNTIME_SOURCE_INVENTORY_DRIFT'}:
                self.skipTest('Live edited source awaits candidate manifest')
            raise
        from test_production_isolation import CompositionTests
        fixture=CompositionTests('test_wrong_route_rejects_without_allocation')
        fixture.setUp();self.addCleanup(fixture.doCleanups)
        fixture.schema6()
        fixture.control.harness.host_bridges=HostBridges(fixture.root/'.control')
        queued=fixture.control.enqueue_jobs('HOST-DISPOSABLE',[{
            'client_job_id':'HOST-ONE','requirement':'MODIFICATION: Change app/a.py',
            'worker':'opencode','model':'opencode-go/deepseek-v4.1-flash',
            'target_modules':['app'],'target_resources':['app/a.py']}])
        claimed=fixture.control.store.claim_next('host-owner')
        child=fixture.control.isolation(claimed,fixture.control.store.repository)
        frozen=child.materialize_execution(claimed,fixture.control.store.repository)
        receipts=list((fixture.root/'.control'/'host-bridge-receipts').glob('*.json'))
        self.assertTrue(frozen['execution_id'])
        self.assertEqual(1,len(receipts))
        observation=json.loads(receipts[0].read_text(encoding='utf-8'))['payload']
        self.assertEqual('opencode',observation['actual']['route'])
        self.assertEqual('ADMISSION',observation['case'])


if __name__=='__main__': unittest.main()
