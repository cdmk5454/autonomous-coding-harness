"""Deterministic profile/context tests; no runtimes or real product repositories."""
import json
import tempfile
import unittest
from pathlib import Path
from dataclasses import replace
from unittest.mock import patch

from execution_context import (ExecutionContextContract, build_execution_context_pack,
    materialize_execution_context, unresolved_admission, ADMISSION_FIELDS)
from context_foundation import ContextError
from project_profile import ProfileError


class ExecutionContextTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.policy = self.root / 'agents'
        self.profile = self.policy / 'profiles' / 'e4u'
        self.workspace = self.root / 'product'
        for d in (self.profile/'references', self.profile/'hooks', self.profile/'ui_map',
                  self.policy/'hooks', self.workspace/'sucui', self.workspace/'suc'):
            d.mkdir(parents=True)
        for name in ('project-context','review','frontend','backend','sql'):
            d=self.policy/'skills'/name;d.mkdir(parents=True)
            (d/'SKILL.md').write_text('# Procedure\n'+name+' scoped procedure')
        (self.policy/'AGENTS.md').write_text('# Rules\nKeep scope and verify evidence.')
        (self.policy/'hooks'/'validation.md').write_text('# Validation\n## Rules\n- No external effects without permission.')
        (self.profile/'project-rules.md').write_text('# Rules\n## Scope and Automation\n- Keep business meaning.\n## Frontend Conventions\n- Preserve labels.')
        (self.profile/'PROJECT_ANALYSIS.md').write_text('# Analysis\n## ScreenAlpha.vue\nIMPACT_ALPHA\n## ScreenOther.vue\nUNRELATED_ANALYSIS')
        (self.profile/'hooks'/'always.md').write_text('# Always\n- No canonical writes.')
        (self.profile/'references'/'frontend.md').write_text('# Frontend\nRELEVANT_FRONTEND')
        (self.profile/'references'/'backend.md').write_text('# Backend\nUNRELATED_BACKEND')
        (self.profile/'ui_map'/'ui.md').write_text('# UI\n## ScreenAlpha.vue\nALPHA_UI_MAP\n## Other.vue\nUNRELATED_UI_MAP')
        self.data={'schema_version':1,'id':'e4u','display_name':'E4U fixture',
          'workspace_roots':[str(self.workspace)], 'context':{'project_rules':'project-rules.md','analysis':'PROJECT_ANALYSIS.md','ui_map':'ui_map'},
          'modules':[{'name':n,'path':n,'kind':k,'protected':False} for n,k in [('sucui','frontend'),('suc','backend')]],
          'rules':{'always':['hooks/always.md'],'frontend':['references/frontend.md'],'backend':['references/backend.md'],'sql':[],'architecture':[],'egov':[]},
          'area_patterns':{'frontend':['sucui/**'],'backend':['suc/**'],'sql':['suc/**/*Qry.xml'],'architecture':['suc/pom.xml']},'protected_paths':['sucui/protected/**'],'local_dev_config_excludes':['.env']}
        schema = {'$schema':'https://json-schema.org/draft/2020-12/schema', 'type':'object', 'required':list(self.data)}
        (self.policy/'profiles'/'project-profile.schema.json').write_text(json.dumps(schema))
        self.write_profile()
        self.c=ExecutionContextContract('J1','E1','A1','C1','e4u','WORKER','LOW',('sucui',),('sucui/ScreenAlpha.vue',),('Label matches contract',),('Build','Test'))
        self.requirement='Change sucui/ScreenAlpha.vue label. Y means enabled; N means disabled. count DISTINCT counts students; date is enrollment date. Preserve API status codes.'
    def write_profile(self):
        (self.profile/'project.json').write_text(json.dumps(self.data),encoding='utf8')
    def build(self,c=None,**kw):
        return build_execution_context_pack(contract=c or self.c,requirement=kw.pop('requirement',self.requirement),profile_dir=kw.pop('profile_dir',self.profile),workspace=kw.pop('workspace',self.workspace),policy_root=self.policy,**kw)
    def test_low_diet_and_complete_business_contract(self):
        p=self.build(implementation_context='IMPLEMENT_LABEL')
        self.assertIn(self.requirement,p.prompt)
        self.assertIn('IMPLEMENT_LABEL',p.prompt)
        for text in ('UNRELATED_ANALYSIS','UNRELATED_BACKEND','UNRELATED_UI_MAP','IMPACT_ALPHA','RELEVANT_FRONTEND'):
            self.assertNotIn(text,p.prompt)
        self.assertEqual(p.manifest['contract']['profile_modules'][0]['name'],'sucui')
    def test_mid_high_relevant_growth_not_full_documents(self):
        mid=self.build(replace(self.c,risk_tier='MID',context_tier='LOCAL_IMPACT'))
        high=self.build(replace(self.c,risk_tier='HIGH',context_tier='DOMAIN',impact_evidence=('API impact checked',)))
        self.assertIn('IMPACT_ALPHA',mid.prompt)
        self.assertIn('RELEVANT_FRONTEND',high.prompt)
        for p in (mid,high):
            self.assertNotIn('UNRELATED_ANALYSIS',p.prompt)
            self.assertNotIn('UNRELATED_BACKEND',p.prompt)
    def test_risk_does_not_widen_permission_or_context(self):
        low=self.build();high=self.build(replace(self.c,risk_tier='HIGH',impact_evidence=('Reviewed impact',)))
        self.assertEqual(low.manifest['contract']['boundary'],high.manifest['contract']['boundary'])
        self.assertEqual(low.manifest['selected_resources'],high.manifest['selected_resources'])
    def test_explicit_module_overrides_discovery(self):
        p=self.build(requirement='frontend label only')
        self.assertEqual([m['name'] for m in p.manifest['contract']['profile_modules']],['sucui'])
        self.assertNotIn('UNRELATED_BACKEND',p.prompt)
    def test_changed_file_outside_selected_module(self):
        with self.assertRaises(ContextError):self.build(changed_files=('suc/Other.java',))
    def test_changed_file_outside_write_scope(self):
        with self.assertRaises(ContextError):self.build(changed_files=('sucui/Other.vue',))
    def test_explicit_contract_scope_mismatch(self):
        with self.assertRaises(ContextError):self.build(requirement='Edit suc/Other.java')
    def test_reviewer_separation_and_evidence(self):
        c=replace(self.c,role='REVIEWER')
        ev={'candidate_id':'C1','diff':'candidate diff','verification':'build PASS','criteria':'acceptance review'}
        p=self.build(c,review_evidence=ev)
        self.assertIn('candidate diff',p.prompt)
        self.assertNotIn('Approved external-harness MODIFICATION may proceed',p.prompt)
        self.assertIn('HARNESS_MATERIALIZED',p.prompt)
        self.assertEqual(p.manifest['contract']['boundary']['candidate'],'RO')
        self.assertEqual(p.manifest['contract']['boundary']['scratch'],'RW')
        self.assertNotIn('IMPLEMENT_LABEL',p.prompt)
        with self.assertRaises(ContextError):self.build(c,review_evidence=ev,implementation_context='history')
        with self.assertRaises(ContextError):self.build(c,review_evidence={**ev,'candidate_id':'old'})
    def test_unknown_profile(self):
        with self.assertRaises(ContextError):self.build(replace(self.c,profile_id='unknown'))
    def test_missing_profile(self):
        with self.assertRaises(ContextError):self.build(profile_dir=self.root/'missing')
    def test_invalid_workspace(self):
        with self.assertRaises(ContextError):self.build(workspace=self.root/'missing')
    def test_workspace_profile_mismatch(self):
        other=self.root/'other';other.mkdir()
        with self.assertRaises((ContextError,ProfileError)):self.build(workspace=other)
    def test_unknown_module(self):
        with self.assertRaises(ContextError):self.build(replace(self.c,target_modules=('unknown',)))
    def test_missing_required_context(self):
        with self.assertRaises(ContextError):self.build(replace(self.c,required_context=(('profile:missing.md','X'),)))
    def test_required_section_survives_low_diet(self):
        p=self.build(replace(self.c,required_context=(('profile:PROJECT_ANALYSIS.md','ScreenAlpha.vue'),)))
        self.assertIn('IMPACT_ALPHA',p.prompt)
    def test_budget_fails_instead_of_truncating_contract(self):
        with self.assertRaises(ContextError):self.build(budget=20)
    def test_unreferenced_secret_file_never_read_or_hashed(self):
        secret=self.profile/'.env';secret.write_text('API_KEY=synthetic-not-real')
        original_bytes,original_text=Path.read_bytes,Path.read_text
        def rb(path,*a,**k):
            if path==secret:self.fail('Secret read_bytes')
            return original_bytes(path,*a,**k)
        def rt(path,*a,**k):
            if path==secret:self.fail('Secret read_text')
            return original_text(path,*a,**k)
        with patch.object(Path,'read_bytes',rb),patch.object(Path,'read_text',rt):p=self.build()
        self.assertNotIn('synthetic-not-real',p.prompt+json.dumps(p.manifest))
        self.assertEqual(p.manifest['snapshot_scope'],'DELIVERED_CONTEXT_ONLY')
    def test_declared_secret_file_rejected_before_read(self):
        self.data['rules']['frontend']=['.env'];self.write_profile()
        (self.profile/'.env').write_text('synthetic')
        with self.assertRaises(ContextError):self.build()
    def test_embedded_secret_rejected(self):
        with self.assertRaises(ContextError):self.build(implementation_context='api_key=synthetic-sensitive')
        with self.assertRaises(ContextError):self.build(implementation_context='{"api_key":"synthetic-sensitive"}')
    def test_document_secret_rejected(self):
        (self.profile/'hooks'/'always.md').write_text('# Rule\n- api_key=synthetic-sensitive')
        with self.assertRaises(ContextError):self.build()
    def test_local_provenance_is_not_runtime_dependency(self):
        (self.profile/'hooks'/'always.md').write_text('# Rule\n- Historical D:\\agents\\old\\report.md is provenance.')
        p=self.build()
        self.assertNotIn('D:\\agents',p.prompt)
        self.assertNotIn(str(self.policy),p.prompt)
        self.assertIn('LOCAL_PROVENANCE_OMITTED',p.prompt)
    def test_contract_host_path_is_not_silently_rewritten(self):
        with self.assertRaises(ContextError):self.build(requirement=r'Edit D:\agents\private.txt')
    def test_no_implicit_external_permission(self):
        p=self.build();self.assertEqual(p.manifest['contract']['external_permissions'],())
    def test_high_without_impact_is_closed(self):
        with self.assertRaises(ContextError):self.build(replace(self.c,risk_tier='HIGH'))
    def test_protected_and_wildcard_scope_closed(self):
        for scope in ('sucui/protected/file.vue','sucui/**','sucui/.env'):
            with self.assertRaises(ContextError):self.build(replace(self.c,allowed_write_scope=(scope,)))
    def test_determinism_and_new_snapshot_only(self):
        a,b=self.build(),self.build();self.assertEqual(a.manifest,b.manifest)
        out=self.root/'pack';materialize_execution_context(a,out)
        self.assertEqual((out/'context.md').read_text(encoding='utf8'),a.prompt)
        with self.assertRaises(FileExistsError):materialize_execution_context(a,out)
    def test_tampered_content_or_manifest_rejected(self):
        p=self.build();p.prompt+='tampered'
        with self.assertRaises(ContextError):materialize_execution_context(p,self.root/'bad')
        p=self.build();p.manifest['contract']['boundary']['candidate']='unrestricted'
        with self.assertRaises(ContextError):materialize_execution_context(p,self.root/'bad2')
    def test_admission_all_ten_required_and_no_truthy_strings(self):
        self.assertEqual(len(unresolved_admission({})),10)
        ready={key:True for key in ADMISSION_FIELDS}
        self.assertEqual(unresolved_admission(ready),())
        for key in ADMISSION_FIELDS:
            self.assertIn(key,unresolved_admission({**ready,key:'true'}))

    def test_schema_is_policy_relative(self):
        (self.policy/'profiles'/'project-profile.schema.json').unlink()
        with self.assertRaises(ContextError):self.build()
    def test_review_empty_evidence_is_closed(self):
        with self.assertRaises(ContextError):
            self.build(replace(self.c,role='REVIEWER'),review_evidence={'candidate_id':'C1','diff':'','verification':'','criteria':''})
    def test_role_independent_shared_required_context(self):
        key=('profile:PROJECT_ANALYSIS.md','ScreenAlpha.vue')
        c=replace(self.c,role='REVIEWER',required_context=(key,))
        p=self.build(c,review_evidence={'candidate_id':'C1','diff':'diff','verification':'NOT_RUN','criteria':'review'})
        self.assertIn('IMPACT_ALPHA',p.prompt)
    def test_invalid_contract_field_types(self):
        with self.assertRaises(ContextError):self.build(replace(self.c,external_permissions=(True,)))

    def test_existing_diff_only_review_policy_reusable(self):
        p=self.build(replace(self.c,role='REVIEWER',context_tier='DIFF_ONLY'),review_evidence={'candidate_id':'C1','diff':'diff','verification':'PASS','criteria':'scope'})
        self.assertNotIn('review scoped procedure',p.prompt)
        self.assertIn(self.requirement,p.prompt)
        self.assertEqual(p.manifest['contract']['boundary']['candidate'],'RO')

    def test_symlink_document_cannot_disguise_secret(self):
        secret=self.profile/'.env';secret.write_text('synthetic-private')
        doc=self.profile/'references'/'frontend.md';doc.unlink()
        try:doc.symlink_to(secret)
        except OSError:self.skipTest('Host symlink privilege unavailable')
        with self.assertRaises(ContextError):self.build()
    def test_profile_reference_traversal_closed(self):
        outside=self.root/'outside.md';outside.write_text('outside')
        self.data['rules']['frontend']=[str(outside)];self.write_profile()
        with self.assertRaises(ContextError):self.build()

if __name__=='__main__':unittest.main()
