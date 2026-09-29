"""Step 10 reporting contract: ReportFacts, developer report, business ELI5 HTML.

Fixtures cover: successful job, FIX job, QA hold + typed accept/reject,
infrastructure failure, blocked job, humanizer preservation, ELI5 truth
preservation, secret exclusion, deterministic regeneration and identity
binding. All prose is a deterministic projection of recorded task evidence.
"""
from html.parser import HTMLParser
from pathlib import Path
import json
import re
import subprocess
import unittest

import harness_temp
from business_report import render as render_business, save as save_business, status_wording
from developer_change_report import build_facts, capture, humanize, save
from git_collector import GitCollector
from task_state import TaskState


class _VisibleText(HTMLParser):
    """Visible text only: comments and tags are dropped."""
    def __init__(self):
        super().__init__()
        self.chunks = []
        self.ids = []

    def handle_data(self, data):
        self.chunks.append(data)

    def handle_starttag(self, tag, attrs):
        for key, value in attrs:
            if key == 'id' and value:
                self.ids.append(value)


def visible_text(html_document):
    parser = _VisibleText()
    parser.feed(html_document)
    return ' '.join(parser.chunks), parser.ids


class Step10ReportingTests(unittest.TestCase):
    def setUp(self):
        self.temp = harness_temp.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.project = self.root / 'project'
        self.project.mkdir()
        self.git('init', '-q')
        (self.project / 'A.py').write_text('def number():\n    return 1\n', encoding='utf-8')
        self.git('add', 'A.py')
        self.git('-c', 'user.name=Harness Test', '-c', 'user.email=harness-test@example.invalid',
                 'commit', '-qm', 'input')
        self.tasks = self.root / '.tasks'
        self.sources = None

    def git(self, *args):
        return subprocess.run(['git', '-C', str(self.project), *args],
                              capture_output=True, check=True)

    def task(self, **overrides):
        task = TaskState(task_id='TASK-S10', job_id='JOB-S10',
                         requirement='return the new number', working_dir=str(self.project),
                         task_mode='MODIFICATION',
                         materialized_execution={'contract_revision': 1,
                                                 'execution_surface_hash': 'surface',
                                                 'materialized_at': '2026-09-20T03:00:00+09:00'},
                         test_plan={'production_scope': ['A.py'], 'scope_hash': 'scope',
                                    'required_tests': ['A.py'], 'command': 'python A.py',
                                    'mandatory': True},
                         changed_files=['A.py'])
        for key, value in overrides.items():
            setattr(task, key, value)
        return task

    def capture_sources(self, task, new_source='def number():\n    return 2\n'):
        collector = GitCollector(str(self.project))
        baseline, error = collector.capture_baseline(task)
        self.assertFalse(error)
        (self.project / 'A.py').write_text(new_source, encoding='utf-8')
        collector.collect(task, baseline)
        capture(task, baseline, collector, self.project, self.tasks)
        return json.loads(Path(task.developer_change_report['source_ref']).read_text(encoding='utf-8'))

    def reports(self, task, **save_kwargs):
        developer_path = save(task, self.tasks, **save_kwargs)
        business_path = save_business(task, self.tasks)
        facts = json.loads((self.tasks / 'developer-changes' / task.task_id / 'ReportFacts.json')
                           .read_text(encoding='utf-8'))
        markdown = developer_path.read_text(encoding='utf-8')
        document = business_path.read_text(encoding='utf-8')
        return facts, markdown, document

    def gates_pass(self, task):
        task.build = {'status': 'PASS'}
        task.test_status = 'PASS'
        task.official_tester_invoked = True
        task.review_status = 'REVIEW_PASS'
        task.verification_status = 'VERIFIED'
        task.status = 'SUCCESS'
        task.stage = 'DONE'

    # ------------------------------------------------------------------
    # 1. Successful job
    # ------------------------------------------------------------------
    def test_successful_job_expected_actual_separation_and_hunks(self):
        task = self.task()
        self.gates_pass(task)
        self.capture_sources(task)
        facts, markdown, document = self.reports(task)
        self.assertIn('    return 1', markdown)
        self.assertIn('    return 2', markdown)
        for heading in ('## Problem', '## Root Cause', '## Change', '## Why This Change',
                        '## Expected Result', '## Actual Result', '## Remaining Checks / Limitations',
                        '## Evidence References', '## Impact'):
            self.assertIn(heading, markdown)
        expected = [row for row in facts['expected_result'] if row['classification'] == 'EXPECTED']
        actual = [row for row in facts['actual_result'] if row['classification'] == 'ACTUAL']
        self.assertTrue(expected and actual)
        # Expected items never claim a verified status.
        for row in expected:
            self.assertNotIn('VERIFIED', row['item'].upper().replace('VERIFIED THROUGH', 'REACHES'))
        gates = {row['gate']: row['result'] for row in actual}
        self.assertEqual('PASS', gates['Build'])
        self.assertEqual('VERIFIED', gates['Verification'])
        self.assertEqual('SUCCESS', gates['Job'])
        self.assertIn('change', facts)
        self.assertEqual(1, facts['change'][0]['hunks'])
        self.assertEqual(1, facts['change'][0]['added_lines'])
        self.assertEqual(1, facts['change'][0]['removed_lines'])
        text, ids = visible_text(document)
        for section in ('summary', 'problem', 'how', 'difference', 'verified', 'additional', 'status'):
            self.assertIn(section, ids)
        self.assertIn('변경 및 시스템 검증 완료', text)

    # ------------------------------------------------------------------
    # 2. FIX job
    # ------------------------------------------------------------------
    def test_fix_job_problem_and_root_cause_come_from_evidence(self):
        task = self.task(
            attempt_history=[{'attempt': 1, 'failure_code': 'TEST_FAILED', 'failure_stage': 'TEST',
                              'failure_type': 'test', 'failure_origin': 'WORKER_CAUSED',
                              'status': 'FAILED'}],
            remediation_cycle=2)
        self.gates_pass(task)
        self.capture_sources(task)
        facts, markdown, document = self.reports(task)
        self.assertIn('Prior attempt failed with TEST_FAILED', facts['problem'])
        self.assertIn('WORKER_CAUSED', facts['root_cause'])
        self.assertIn('prior attempt of this Job failed (TEST_FAILED)'.upper(),
                      facts['why_changed'].upper())
        self.assertIn('TEST_FAILED', markdown)
        self.assertIn('transitions', facts['retry_recovery'])
        self.assertEqual(1, len(facts['retry_recovery']['transitions']))

    def test_no_failure_root_cause_is_insufficient_evidence_not_invented(self):
        task = self.task()
        self.gates_pass(task)
        self.capture_sources(task)
        facts = build_facts(task, self.capture_sources_reload(task))
        self.assertIn('INSUFFICIENT_EVIDENCE', facts['root_cause'])

    def capture_sources_reload(self, task):
        return json.loads(Path(task.developer_change_report['source_ref']).read_text(encoding='utf-8'))

    # ------------------------------------------------------------------
    # 3. QA jobs
    # ------------------------------------------------------------------
    def test_qa_hold_requires_user_action_and_never_claims_applied(self):
        task = self.task(qa_request={'required': True}, qa_type='HUMAN_ACCEPTANCE',
                         status='AWAITING_QA', worktree_disposition='ISOLATED_CANDIDATE_PRESERVED',
                         user_action_required=True)
        task.build = {'status': 'PASS'}
        task.test_status = 'PASS'
        task.official_tester_invoked = True
        task.review_status = 'REVIEW_PASS'
        task.verification_status = 'VERIFIED'
        self.capture_sources(task)
        facts, markdown, document = self.reports(task)
        self.assertEqual('AWAITING_QA', facts['qa']['state'])
        self.assertIn('Human QA decision', facts['problem'])
        self.assertIn({'item': 'Human QA decision', 'state': 'AWAITING_QA'}, facts['known_limitations'])
        text, _ = visible_text(document)
        self.assertIn('사용자 확인 필요', text)
        self.assertIn('사람이 직접 확인해야', text)
        self.assertIn('최종 반영 전', text)
        self.assertNotIn('최종 시스템에 반영 완료', text)

    def test_qa_accept_decision_reflected_in_both_reports(self):
        task = self.task(qa_request={'required': True}, qa_type='HUMAN_ACCEPTANCE',
                         status='SUCCEEDED', worktree_disposition='CANONICAL_COMMITTED',
                         verification_status='VERIFIED')
        task.build = {'status': 'PASS'}
        task.test_status = 'PASS'
        task.official_tester_invoked = True
        task.review_status = 'REVIEW_PASS'
        self.capture_sources(task)
        decision = {'decision_id': 'DEC-S10', 'resolution': 'HUMAN_ACCEPT', 'candidate_id': 'CAND-S10'}
        facts, markdown, document = self.reports(task, decision=decision)
        self.assertEqual('HUMAN_ACCEPT', facts['qa']['decision']['resolution'])
        self.assertIn('DEC-S10', markdown)
        text, _ = visible_text(document)
        self.assertIn('최종 시스템에 반영 완료', text)
        self.assertIn('사용자 확인 후 최종 반영되었습니다', text)

    def test_qa_reject_candidate_not_applied(self):
        task = self.task(status='CANCELLED', worktree_disposition='REJECTED_ISOLATED_CANDIDATE_PRESERVED',
                         failure_code='HUMAN_REJECTED')
        self.capture_sources(task)
        facts, markdown, document = self.reports(task)
        self.assertEqual('CANCELLED', facts['qa']['state'])
        text, _ = visible_text(document)
        self.assertIn('사용자 확인 결과 반영하지 않음', text)
        self.assertNotIn('반영 완료', text)

    # ------------------------------------------------------------------
    # 4. Infrastructure failure is not a product defect
    # ------------------------------------------------------------------
    def test_infra_failure_separated_from_product_failure(self):
        task = self.task(status='FAILED', failure_code='HERMES_HTTP_404',
                         failure_stage='WORKER', failure_type='infrastructure',
                         failure_origin='TOOL_INFRA', failure_reason='게이트웨이 일시 오류',
                         retry_domain='TECHNICAL_EXECUTION_RETRY',
                         technical_execution_retry_count=1, product_semantic_retry_count=0,
                         worktree_disposition='PRESERVED', is_rolled_back=True)
        self.capture_sources(task)
        facts, markdown, document = self.reports(task)
        self.assertIn('infrastructure failure, not a product defect', facts['root_cause'])
        self.assertEqual(0, facts['retry_recovery']['product_semantic_retry_count'])
        self.assertEqual(1, facts['retry_recovery']['technical_execution_retry_count'])
        self.assertTrue(facts['retry_recovery']['candidate_preserved'])
        self.assertIn({'item': 'Candidate changes were rolled back', 'state': 'ROLLED_BACK'},
                      facts['known_limitations'])
        text, _ = visible_text(document)
        self.assertIn('실행 환경 문제', text)
        self.assertNotIn('반영 완료', text)

    # ------------------------------------------------------------------
    # 5. Blocked job
    # ------------------------------------------------------------------
    def test_blocked_job_reports_hold_and_action(self):
        task = self.task(status='BLOCKED', failure_code='HERMES_HTTP_404',
                         failure_stage='WORKER', failure_type='infrastructure',
                         failure_origin='TOOL_INFIA' if False else 'TOOL_INFRA',
                         retry_domain='TECHNICAL_EXECUTION_RETRY',
                         worktree_disposition='PRESERVED')
        self.capture_sources(task)
        facts, markdown, document = self.reports(task)
        self.assertEqual('BLOCKED', facts['job']['status'])
        text, _ = visible_text(document)
        self.assertIn('적용 보류', text)
        self.assertIn('최종 시스템에 반영되지 않음', text)

    # ------------------------------------------------------------------
    # 6. Humanizer preservation (fail-closed presentation layer)
    # ------------------------------------------------------------------
    def test_humanizer_never_changes_protected_facts(self):
        task = self.task()
        self.gates_pass(task)
        self.capture_sources(task)
        facts_plain, markdown_plain, _ = self.reports(task)
        good = lambda text: re.sub(r'\s{2,}', ' ', text.replace('. ', '.  '))
        facts_humanized, markdown_humanized, _ = self.reports(task, humanizer=good)
        self.assertEqual(facts_plain, facts_humanized)
        # A destructive engine on evidence-bearing prose (protected tokens) is discarded.
        infra = self.task(task_id='TASK-S10-INFRA', status='FAILED', failure_code='HERMES_HTTP_404',
                          failure_stage='WORKER', failure_type='infrastructure',
                          failure_origin='TOOL_INFRA', failure_reason='게이트웨이 일시 오류',
                          retry_domain='TECHNICAL_EXECUTION_RETRY')
        self.capture_sources(infra)
        _, infra_plain, _ = self.reports(infra)
        bad = lambda text: '안정성을 위해 수정했습니다.'
        _, infra_bad, _ = self.reports(infra, humanizer=bad)
        self.assertEqual(infra_plain, infra_bad)
        self.assertIn('HERMES_HTTP_404', infra_bad)
        self.assertIn('infrastructure failure, not a product defect', infra_bad)
        dropped = lambda text: text.replace('PASS', 'PASS ')
        self.assertEqual('Build: PASS', humanize('Build: PASS', engine=dropped).strip())
        destroyer = lambda text: 'PASS FAIL'
        self.assertEqual('Build PASS and Test PASS', humanize('Build PASS and Test PASS', engine=destroyer))

    def test_humanizer_failure_falls_back_to_deterministic(self):
        def explode(text):
            raise TimeoutError('humanizer timeout')
        self.assertEqual('deterministic text PASS', humanize('deterministic text PASS', engine=explode))
        self.assertEqual(None if False else 'plain', humanize('plain', engine=lambda t: None))

    # ------------------------------------------------------------------
    # 7. ELI5 / business truth preservation
    # ------------------------------------------------------------------
    def test_business_html_hides_internal_ids_and_keeps_truth(self):
        task = self.task(qa_request={'required': True}, qa_type='HUMAN_ACCEPTANCE',
                         status='AWAITING_QA', worktree_disposition='ISOLATED_CANDIDATE_PRESERVED',
                         browser_qa={'required': True, 'latest_status': ''},
                         user_action_required=True)
        task.build = {'status': 'PASS'}
        task.test_status = 'PASS'
        task.official_tester_invoked = True
        task.review_status = 'REVIEW_PASS'
        task.verification_status = 'VERIFIED'
        self.capture_sources(task)
        facts, markdown, document = self.reports(task)
        text, _ = visible_text(document)
        for internal in ('JOB-S10', 'TASK-S10', 'HCAND', 'CAND-', 'surface',
                         re.escape(self.project.name), 'A.py:'):
            self.assertNotIn(internal, text, internal)
        self.assertIn('UNVERIFIED', text)  # browser QA stays unverified
        self.assertNotIn('브라우저 자동 검증 통과', text)
        self.assertIn('사용자 확인 필요', text)
        # Internal identity is preserved as inert metadata for binding.
        meta = re.search(r'<!-- report-metadata: (.*) -->', document)
        self.assertTrue(meta)
        self.assertEqual('JOB-S10', json.loads(meta.group(1))['binding']['job_id'])

    def test_business_never_converts_expected_into_actual(self):
        task = self.task()
        self.gates_pass(task)
        self.capture_sources(task)
        facts, markdown, document = self.reports(task)
        text, _ = visible_text(document)
        self.assertIn('목표', text)
        self.assertIn('실제 확인 결과', text)
        self.assertNotIn('정상 동작합니다', text)

    def test_waived_items_are_never_worded_as_pass(self):
        facts = {'job': {'status': 'SUCCEEDED', 'goal': 'g'},
                 'verification': {'build': 'PASS', 'test': 'PASS', 'review': 'REVIEW_PASS',
                                  'verification': 'VERIFIED'},
                 'actual_result': [{'gate': 'BrowserQA', 'result': 'UNVERIFIED'}],
                 'change': [], 'expected_result': [], 'impact': {},
                 'qa': {'state': 'SUCCEEDED', 'worktree_disposition': 'CANONICAL_COMMITTED'},
                 'retry_recovery': {}, 'known_limitations': [
                     {'item': 'Native FIX re-execution', 'state': 'USER_WAIVED / NOT_REEXECUTED'},
                     {'item': 'parallel native re-execution', 'state': 'USER_WAIVED / NOT_REEXECUTED'},
                     {'item': 'schema5 native re-execution', 'state': 'USER_WAIVED / NOT_REEXECUTED'}],
                 'user_action_required': {'required': False}, 'binding': {}}
        document = render_business(facts)
        text, _ = visible_text(document)
        self.assertIn('이번 검증에서는 재실행하지 않기로 결정한 항목', text)
        self.assertIn('USER_WAIVED / NOT_REEXECUTED', text)
        self.assertNotIn('재검증 완료', text)
        self.assertNotIn('Native FIX re-execution: PASS', text)

    # ------------------------------------------------------------------
    # 8. Secret exclusion
    # ------------------------------------------------------------------
    def test_secrets_are_excluded_from_both_reports(self):
        secret = 'sk-live-abcdef1234567890abcdef'
        (self.project / 'A.py').write_text(
            'TOKEN = "%s"\ndef number():\n    return 1\n' % secret, encoding='utf-8')
        self.git('add', 'A.py')
        self.git('-c', 'user.name=Harness Test', '-c', 'user.email=harness-test@example.invalid',
                 'commit', '-qm', 'input with token')
        task = self.task()
        self.gates_pass(task)
        collector = GitCollector(str(self.project))
        baseline, error = collector.capture_baseline(task)
        self.assertFalse(error)
        (self.project / 'A.py').write_text(
            'TOKEN = "%s"\ndef number():\n    return 2\n' % secret, encoding='utf-8')
        collector.collect(task, baseline)
        capture(task, baseline, collector, self.project, self.tasks)
        facts, markdown, document = self.reports(task)
        for artifact in (markdown, document, json.dumps(facts)):
            self.assertNotIn(secret, artifact)

    # ------------------------------------------------------------------
    # 9. Deterministic regeneration + identity binding
    # ------------------------------------------------------------------
    def test_facts_regenerate_identically_and_reports_share_identity(self):
        task = self.task()
        self.gates_pass(task)
        self.capture_sources(task)
        first_facts_path = self.tasks / 'developer-changes' / task.task_id / 'ReportFacts.json'
        self.reports(task)
        first = first_facts_path.read_bytes()
        second_task = self.task()
        self.gates_pass(second_task)
        second_task.developer_change_report = dict(task.developer_change_report)
        self.capture_sources_reload(second_task)
        save(second_task, self.tasks)
        second = (self.tasks / 'developer-changes' / second_task.task_id / 'ReportFacts.json').read_bytes()
        self.assertEqual(first, second)
        document = (self.tasks / 'developer-changes' / task.task_id / 'BusinessReport.html').read_text(
            encoding='utf-8')
        meta = json.loads(re.search(r'<!-- report-metadata: (.*) -->', document).group(1))
        facts = json.loads(first)
        self.assertEqual(facts['binding']['job_id'], meta['binding']['job_id'])
        self.assertEqual(facts['binding']['candidate_hash'], meta['binding']['candidate_hash'])
        self.assertEqual(facts['binding']['job_id'], task.developer_change_report['binding']['job_id'])


if __name__ == '__main__':
    unittest.main()
