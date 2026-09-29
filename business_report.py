"""Business (ELI5) HTML report: a deterministic projection of ReportFacts.

This report explains a recorded change for non-developer users. It is a
projection only: it never reads or writes Control state, and every status
wording is a deterministic mapping of the underlying recorded value. Internal
identifiers (Job/Attempt IDs, hashes, epochs) stay out of the visible body and
are kept only in a trailing metadata comment so the file remains bound to the
same result the developer report describes.
"""
from pathlib import Path
import html
import json

from control_paths import atomic_state_write
from control_repository import canonical
from runtime_safety import scrub_secrets

STATUS_WORDING = {
    'SUCCEEDED': '변경 및 시스템 검증 완료',
    'SUCCESS': '변경 및 시스템 검증 완료',
    'AWAITING_QA': '사용자 확인 필요',
    'AWAITING_DEPENDENCY_QA': '사용자 확인 필요',
    'BLOCKED': '추가 확인/환경 문제로 적용 보류',
    'FAILED': '검증 중 문제 발견',
    'FAILED_FINAL': '검증 중 문제 발견(재시도 소진)',
    'INTERRUPTED': '실행 중 중단됨',
    'CANCELLED': '사용자 확인 결과 반영하지 않음',
    'SKELETON_READY': '기본 구조 준비됨(추가 작업 필요)',
    'UNVERIFIED': '아직 실제 확인하지 않은 항목',
    'SKIPPED': '이번 실행에서 건너뜀(계약상 허용됨)',
    'USER_WAIVED': '이번 검증에서는 재실행하지 않기로 결정한 항목',
    'NOT_REEXECUTED': '이번 검증에서는 재실행하지 않기로 결정한 항목',
}

DISPOSITION_WORDING = {
    'CANONICAL_COMMITTED': '최종 시스템에 반영 완료',
    'BATCH_COMMIT_PENDING': '변경 검증 완료 — 최종 반영은 묶음(batch) 커밋 대기 중',
    'ISOLATED_CANDIDATE_PRESERVED': '작업 결과는 별도로 보관 중(아직 최종 반영되지 않음)',
    'REJECTED_ISOLATED_CANDIDATE_PRESERVED': '사용자 확인 결과 반영하지 않음(작업본은 보관 중)',
    'PRESERVED': '최종 시스템에 반영되지 않음(작업본 보관 중)',
    'ROLLED_BACK': '작업 내용 되돌림',
}

GATE_PASS_VALUES = {'PASS', 'REVIEW_PASS', 'VERIFIED', 'SUCCESS'}


def status_wording(status):
    return STATUS_WORDING.get(str(status or ''), str(status or 'UNVERIFIED'))


def compound_state_wording(state):
    """Map 'USER_WAIVED / NOT_REEXECUTED'-style compound states token by token."""
    text = str(state or '')
    if not text:
        return status_wording(text)
    return ' / '.join(status_wording(part.strip()) for part in text.split('/'))


def disposition_wording(disposition):
    return DISPOSITION_WORDING.get(str(disposition or ''), '상태 미확인')


def _problem_wording(facts):
    retry = facts.get('retry_recovery') or {}
    qa = facts.get('qa') or {}
    verification = facts.get('verification') or {}
    if qa.get('state') == 'AWAITING_QA':
        return ('자동 검증은 통과했지만, 사람이 직접 확인해야 하는 항목이 있어 최종 반영 전입니다.')
    if verification.get('build') == 'FAIL':
        return '자동 검사(빌드)에서 문제가 발견되었습니다.'
    if verification.get('test') == 'FAIL':
        return '자동 테스트에서 문제가 발견되었습니다.'
    if verification.get('review') == 'REVIEW_FAIL':
        return '내용 검토에서 문제가 발견되었습니다.'
    if verification.get('verification') == 'PARTIAL':
        return '일부 자동 검증이 완료되지 않아 확정할 수 없습니다.'
    if retry.get('retry_domain') == 'TECHNICAL_EXECUTION_RETRY':
        return ('실행 장비/도구에 문제가 생겨 작업이 끝까지 진행되지 못했습니다. '
                '프로그램 자체의 오류가 아니라 실행 환경 문제로 구분되어 있습니다.')
    if facts.get('job', {}).get('status') in {'FAILED', 'FAILED_FINAL', 'BLOCKED', 'INTERRUPTED'}:
        return '작업 진행 중 문제가 발생하여 완료되지 못했습니다.'
    return '이번 요청 사항을 반영하기 위한 계획된 변경입니다.'


def _changed_wording(facts):
    impact = facts.get('impact') or {}
    files = impact.get('changed_files') or []
    modules = [m for m in (impact.get('modules') or []) if m]
    parts = []
    if files:
        parts.append('파일 {count}개가 수정되었습니다.'.format(count=len(files)))
    else:
        parts.append('이번에는 실제 파일 수정이 없었습니다.')
    if modules:
        parts.append('대상 영역: {modules}'.format(modules=', '.join(modules)))
    return ' '.join(parts)


def _verified_wording(facts):
    verification = facts.get('verification') or {}
    checked = []
    if verification.get('build') in GATE_PASS_VALUES:
        checked.append('기본 동작 점검(빌드) 통과')
    if verification.get('test') in GATE_PASS_VALUES:
        checked.append('자동 테스트 통과')
    if verification.get('review') in GATE_PASS_VALUES:
        checked.append('내용 검토 통과')
    if verification.get('verification') in GATE_PASS_VALUES:
        checked.append('종합 자동 검증 통과')
    actual = {row.get('gate'): row.get('result') for row in facts.get('actual_result') or []}
    if actual.get('BrowserQA') in GATE_PASS_VALUES:
        checked.append('브라우저 자동 검증 통과')
    return checked


def _user_actions(facts):
    actions = []
    qa = facts.get('qa') or {}
    if qa.get('state') == 'AWAITING_QA':
        actions.append('이 변경을 최종 반영할지 사용자 확인이 필요합니다. (확인 후 반영 또는 보류됩니다)')
    if (facts.get('user_action_required') or {}).get('required'):
        actions.append('안내된 항목에 대해 사용자 확인/조치가 필요합니다.')
    for entry in facts.get('known_limitations') or []:
        state = str(entry.get('state') or 'UNVERIFIED')
        if 'USER_WAIVED' in state or 'NOT_REEXECUTED' in state:
            actions.append('{item}: {state} ({wording})'.format(
                item=entry.get('item', ''), state=state, wording=compound_state_wording(state)))
        elif state in {'AWAITING_QA', 'UNVERIFIED', 'USER_ACTION_REQUIRED'}:
            actions.append('{item}: {state} ({wording})'.format(
                item=entry.get('item', ''), state=state, wording=status_wording(state)))
    return actions


def _badge(status):
    wording = status_wording(status)
    tone = 'ok' if status in {'SUCCEEDED', 'SUCCESS'} else \
           'warn' if status in {'AWAITING_QA', 'AWAITING_DEPENDENCY_QA', 'SKELETON_READY'} else \
           'bad' if status in {'FAILED', 'FAILED_FINAL', 'BLOCKED', 'INTERRUPTED'} else \
           'neutral' if status in {'CANCELLED'} else 'info'
    label = {'ok': '완료', 'warn': '확인 필요', 'bad': '보류/문제', 'neutral': '반영 안 됨', 'info': '정보'}[tone]
    return wording, tone, label


_CSS = """body{font-family:'Malgun Gothic','Apple SD Gothic Neo',sans-serif;line-height:1.6;color:#1f2933;max-width:760px;margin:0 auto;padding:16px}
h1{font-size:1.5rem;border-bottom:3px solid #2684ff;padding-bottom:8px}
h2{font-size:1.15rem;margin-top:1.6em;border-left:5px solid #2684ff;padding-left:8px}
.badge{display:inline-block;padding:4px 12px;border-radius:14px;font-weight:bold;border:1px solid currentColor}
.badge.ok{color:#1b7f4d;background:#eaf7f0}.badge.warn{color:#8a5a00;background:#fdf3e0}
.badge.bad{color:#a11c2b;background:#fdecee}.badge.neutral{color:#4b5563;background:#eef0f3}.badge.info{color:#1d5eb8;background:#e8f1fd}
ul{padding-left:1.2em}li{margin:.25em 0}
.asis,.tobe{border:1px solid #cbd2d9;border-radius:6px;padding:8px;margin:6px 0;white-space:pre-wrap;word-break:break-word}
.asis{background:#f5f7fa}.tobe{background:#eefaf2}
table{border-collapse:collapse;width:100%}td,th{border:1px solid #cbd2d9;padding:6px 8px;text-align:left}
footer{margin-top:2em;font-size:.85rem;color:#52606d;border-top:1px solid #cbd2d9;padding-top:8px}
@media print{body{max-width:none}}"""


def render(facts):
    """Render self-contained Korean business HTML from ReportFacts."""
    job = facts.get('job') or {}
    status = job.get('status', 'UNVERIFIED')
    wording, tone, label = _badge(status)
    as_of = (facts.get('binding') or {}).get('as_of') or ''
    checked = _verified_wording(facts)
    actions = _user_actions(facts)

    def esc(value):
        return html.escape(scrub_secrets(str(value)), quote=True)

    parts = []
    parts.append('<!DOCTYPE html>')
    parts.append('<html lang="ko"><head><meta charset="utf-8">')
    parts.append('<meta name="viewport" content="width=device-width, initial-scale=1">')
    parts.append('<title>{title}</title><style>{css}</style></head><body>'.format(
        title=esc('변경 보고 — ' + label), css=_CSS))
    parts.append('<h1>변경 보고서</h1>')
    parts.append('<p><span class="badge {tone}">{wording}</span> <small>({label})</small>'.format(
        tone=tone, wording=esc(wording), label=esc(label)))
    if as_of:
        parts.append(' <small>기준 시각: {at}</small>'.format(at=esc(as_of)))
    parts.append('</p>')

    parts.append('<h2 id="summary">변경 내용</h2>')
    parts.append('<p>{goal}</p>'.format(goal=esc(job.get('goal') or '목표 설명 없음')))
    parts.append('<p>{changed}</p>'.format(changed=esc(_changed_wording(facts))))

    parts.append('<h2 id="problem">기존 문제</h2>')
    parts.append('<p>{problem}</p>'.format(problem=esc(_problem_wording(facts))))

    parts.append('<h2 id="how">어떻게 바뀌었나</h2>')
    changes = facts.get('change') or []
    if changes:
        parts.append('<table><tr><th>구분</th><th>수정 규모</th></tr>')
        for entry in changes:
            if entry.get('redacted'):
                parts.append('<tr><td>{path}</td><td>보안상 내용을 표시하지 않습니다</td></tr>'.format(
                    path=esc(entry.get('path', ''))))
            else:
                parts.append('<tr><td>{path}</td><td>{hunks}개 구간, {add}줄 추가 / {remove}줄 삭제</td></tr>'.format(
                    path=esc(entry.get('path', '')), hunks=entry.get('hunks', 0),
                    add=entry.get('added_lines', 0), remove=entry.get('removed_lines', 0)))
        parts.append('</table>')
    else:
        parts.append('<p>이번에는 실제 파일 수정이 없었습니다.</p>')
    asis = [entry for entry in (facts.get('as_is') or []) if entry.get('excerpts') and not entry.get('redacted')]
    if asis:
        parts.append('<h3>변경 전 / 변경 후 (발췌)</h3>')
        for entry in asis[:3]:
            parts.append('<p><strong>{path}</strong></p>'.format(path=esc(entry['path'])))
            to_be = next((t for t in facts.get('to_be') or [] if t['path'] == entry['path']), None)
            for index, excerpt in enumerate(entry['excerpts'][:2]):
                parts.append('<div class="asis">변경 전: {text}</div>'.format(text=esc(excerpt.get('text', ''))))
                if to_be and index < len(to_be['excerpts']):
                    parts.append('<div class="tobe">변경 후: {text}</div>'.format(
                        text=esc(to_be['excerpts'][index].get('text', ''))))

    parts.append('<h2 id="difference">사용 시 달라지는 점</h2>')
    expected = [e for e in facts.get('expected_result') or [] if e.get('classification') == 'EXPECTED']
    parts.append('<p>이 변경이 완료되면 다음과 같아지는 것을 목표로 합니다:</p><ul>')
    for entry in expected:
        parts.append('<li>{item}</li>'.format(item=esc(entry.get('item', ''))))
    parts.append('</ul>')
    parts.append('<p><small>위 항목은 목표(예상)이며, 실제 확인 결과는 아래와 같습니다.</small></p>')

    parts.append('<h2 id="verified">확인한 내용</h2>')
    if checked:
        parts.append('<ul>')
        for item in checked:
            parts.append('<li>{item}</li>'.format(item=esc(item)))
        parts.append('</ul>')
    else:
        parts.append('<p>이번에는 자동 검증을 통과한 항목이 없습니다.</p>')

    parts.append('<h2 id="additional">추가 확인이 필요한 내용</h2>')
    limitations = facts.get('known_limitations') or []
    if actions or limitations:
        parts.append('<ul>')
        for action in _user_actions(facts):
            parts.append('<li>{action}</li>'.format(action=esc(action)))
        for entry in limitations:
            state = str(entry.get('state') or '')
            if 'USER_WAIVED' in state or 'NOT_REEXECUTED' in state:
                parts_item = '<li>{item}: {state} ({wording})</li>'.format(
                    item=esc(entry.get('item', '')), state=esc(state),
                    wording=esc(compound_state_wording(state)))
                parts.append(parts_item)
        parts.append('</ul>')
    else:
        parts.append('<p>없습니다.</p>')

    parts.append('<h2 id="status">현재 상태</h2>')
    disposition = (facts.get('qa') or {}).get('worktree_disposition') or ''
    parts.append('<p><strong>{wording}</strong></p>'.format(wording=esc(disposition_wording(disposition))))
    qa = facts.get('qa') or {}
    if qa.get('state') == 'AWAITING_QA':
        parts.append('<p>사용자 확인 후 최종 반영 여부가 결정됩니다.</p>')
    elif qa.get('state') == 'CANCELLED':
        parts.append('<p>사용자 확인 결과 이 변경은 반영하지 않았습니다.</p>')
    elif qa.get('decision', {}).get('resolution') == 'HUMAN_ACCEPT' and disposition == 'CANONICAL_COMMITTED':
        parts.append('<p>사용자 확인 후 최종 반영되었습니다.</p>')

    parts.append('<footer>이 보고서는 기록된 검증 결과를 바탕으로 자동 생성되었습니다. '
                 '상세 기술 자료는 개발자용 보고서를 참고하세요.</footer>')
    parts.append('</body></html>')
    # Identity binding kept as inert metadata: never rendered, still verifiable.
    metadata = {'kind': 'BusinessReport', 'binding': facts.get('binding') or {},
                'facts_digest_fields': ['job_id', 'candidate_hash', 'contract_revision']}
    parts.append('<!-- report-metadata: {meta} -->'.format(
        meta=json.dumps(metadata, ensure_ascii=False, sort_keys=True)))
    return '\n'.join(parts)


def save(task, output_root):
    """Write BusinessReport.html from the ReportFacts the developer report saved."""
    facts_path = Path(output_root) / 'developer-changes' / task.task_id / 'ReportFacts.json'
    if not facts_path.is_file():
        raise FileNotFoundError('REPORT_FACTS_REQUIRED')
    facts = json.loads(facts_path.read_text(encoding='utf-8'))
    document = render(facts)
    directory = facts_path.parent
    path = directory / 'BusinessReport.html'
    atomic_state_write(path, document.encode('utf-8'), root=Path(output_root), field='business_report')
    task.developer_change_report['business_report'] = {
        'path': str(path),
        'facts_sha256': task.developer_change_report.get('facts_sha256', ''),
        'binding': facts.get('binding') or {},
    }
    return path
