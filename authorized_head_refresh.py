"""Explicit, CAS-bound HEAD-only authority refresh. Never adopts working bytes."""
import fnmatch
import hashlib
import subprocess
from pathlib import Path

from canonical_ownership import canonical_ownership
from control_repository import ControlRepository, RepositoryError, now
from cumulative_policy import CumulativePolicyManager
from job_queue import JobStore
from runtime_safety import trusted_executable, git_subprocess_env
from task_state import TaskState


CONFIRMATION = 'I_CONFIRM_EXACT_DISJOINT_HEAD_REFRESH'


def overlaps(path, scope):
    return any(path == p.rstrip('/') or path.startswith(p.rstrip('/') + '/')
               or p.startswith(path.rstrip('/') + '/') or fnmatch.fnmatchcase(path, p)
               for p in scope)


def refresh(harness, *, job_id, candidate_id, expected_revision, expected_job_revision,
            modules, request_id, confirmation):
    if confirmation != CONFIRMATION or not request_id or not modules:
        raise RepositoryError('HEAD_REFRESH_CONFIRMATION_REQUIRED')
    if not harness.acquire_execution_lease():
        raise RepositoryError('HEAD_REFRESH_ACTIVE_WRITER')
    try:
        with canonical_ownership(harness.working_dir):
            repo = ControlRepository(harness.control_root / 'control.sqlite3')
            with repo.hold():
                queue = repo.read_queue()
                request = dict(job_id=job_id, candidate_id=candidate_id,
                    expected_revision=expected_revision, expected_job_revision=expected_job_revision,
                    modules=modules, request_id=request_id, confirmation=confirmation)
                for old in queue.get('authorized_head_refreshes', []):
                    if old['request']['request_id'] == request_id:
                        if old['request'] != request:
                            raise RepositoryError('HEAD_REFRESH_IDEMPOTENCY_CONFLICT')
                        return dict(old, replayed=True)
                if queue['revision'] != expected_revision:
                    raise RepositoryError('CONTROL_REVISION_CONFLICT')
                job = repo.read_job(job_id)
                if job['revision'] != expected_job_revision or job.get('candidate_id') != candidate_id:
                    raise RepositoryError('HEAD_REFRESH_CANDIDATE_BINDING_INVALID')
                if queue.get('running_job_id') or repo.global_stop().get('active'):
                    raise RepositoryError('HEAD_REFRESH_ACTIVE_WRITER')
                if repo.health()['status'] != 'VALID':
                    raise RepositoryError('HEAD_REFRESH_CONTROL_INVALID')
                if repo._connection.execute("SELECT 1 FROM execution_authority WHERE state='ACTIVE' LIMIT 1").fetchone():
                    raise RepositoryError('HEAD_REFRESH_ACTIVE_WRITER')
                if repo._connection.execute("SELECT 1 FROM executions WHERE phase!='TERMINAL' LIMIT 1").fetchone():
                    raise RepositoryError('HEAD_REFRESH_ACTIVE_EXECUTION')
                profile, _ = harness._load_profile()
                manager = CumulativePolicyManager(harness.working_dir, harness.control_root, profile)
                baseline = manager.load_batch_manifest(queue['active_baseline_id'])
                heads = manager.repository_head_authority(baseline)
                status = harness.cumulative_policy_status(active_baseline_id=queue['active_baseline_id'],
                    batch_delta_files=queue.get('batch_delta_files', []), read_only=True)
                if (any(status.get(k) is not True for k in ('candidate_preservation_integrity',
                        'baseline_declaration_integrity','dirty_overlay_path_set_match','dirty_overlay_hash_match'))
                        or status.get('unexpected_runtime_dirty_files')):
                    raise RepositoryError('HEAD_REFRESH_DIRTY_BYTES_CHANGED')
                task = TaskState.load(job['last_result']['task_id'], harness.task_root)
                expected_hashes = task.build_evidence['details']['source_sha256']
                if task.candidate_id != candidate_id or not expected_hashes:
                    raise RepositoryError('HEAD_REFRESH_CANDIDATE_BINDING_INVALID')
                candidate = Path(task.working_dir)
                for path, sha in expected_hashes.items():
                    if hashlib.sha256((candidate/path).read_bytes()).hexdigest() != sha:
                        raise RepositoryError('HEAD_REFRESH_CANDIDATE_CHANGED')
                scope = []
                for jid in queue.get('order', [])[queue.get('active_from_index', 0):]:
                    active = repo.read_job(jid)
                    if active.get('candidate_id') and active['status'] not in ('CANCELLED','SKIPPED','SUCCEEDED'):
                        scope += active.get('request', {}).get('target_resources', [])
                        scope += active.get('last_result', {}).get('changed_files', [])
                scope += [r['path'] for r in baseline.get('files', [])]
                verified = {}
                def git(root, *args):
                    return subprocess.run([trusted_executable('git', forbidden_root=harness.working_dir),
                        '-C',str(root),*args],capture_output=True,check=True,timeout=15,
                        env=git_subprocess_env()).stdout.decode('utf-8').strip()
                for module in profile.modules:
                    root = Path(harness.working_dir)/module.path
                    current = git(root,'rev-parse','HEAD')
                    old = heads[module.name]
                    supplied = modules.get(module.name)
                    if supplied is None:
                        if current != old:
                            raise RepositoryError('HEAD_REFRESH_UNREQUESTED_MODULE')
                        continue
                    if supplied.get('old') != old or supplied.get('new') != current or current == old:
                        raise RepositoryError('HEAD_REFRESH_HEAD_MISMATCH')
                    try:
                        git(root,'merge-base','--is-ancestor',old,current)
                    except subprocess.CalledProcessError:
                        raise RepositoryError('HEAD_REFRESH_NOT_FAST_FORWARD') from None
                    paths = sorted(f'{module.path}/{p}' for p in git(root,'diff','--name-only',old,current).splitlines())
                    if paths != supplied.get('changed_files'):
                        raise RepositoryError('HEAD_REFRESH_PATH_INVENTORY_MISMATCH')
                    if any(overlaps(p,scope) for p in paths):
                        raise RepositoryError('HEAD_REFRESH_CANDIDATE_OVERLAP')
                    verified[module.name] = dict(old=old,new=current,changed_files=paths,
                        fast_forward=True,overlap='DISJOINT')
                if set(verified) != set(modules):
                    raise RepositoryError('HEAD_REFRESH_MODULE_MISMATCH')
                receipt = dict(request=request,baseline_id=queue['active_baseline_id'],
                    generation=queue['generation'],modules=verified,candidate_source_hashes=expected_hashes,
                    at=now(),queue_revision=expected_revision+1)
                queue.setdefault('authorized_head_refreshes', []).append(receipt)
                queue['revision'] += 1
                queue.setdefault('history', []).append(dict(event='AUTHORIZED_DISJOINT_HEAD_REFRESH',
                    at=receipt['at'],request_id=request_id))
                repo.write_queue(queue,expected_revision=expected_revision)
                return dict(receipt,replayed=False)
    finally:
        harness.release_execution_lease()
