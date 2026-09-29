"""Production OpenCode custody: host candidate, ephemeral Worker view, sealed handoff.

The host is trusted. Runtime output cannot write the allocation, authority, source
snapshot or publication receipts. Unknown custody is fenced, never reconstructed.
"""
from pathlib import Path
from collections import deque
from concurrent.futures import ThreadPoolExecutor
import fnmatch
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import uuid

from canonical_ownership import canonical_ownership
from hermes_boundary import atomic, read, write_once
from opencode_boundary import IsolatedOpenCodeRuntimeAdapter, MODEL, SECRET_NAMES
from qa_quarantine import CandidateError

RUNTIME = Path(r'D:\hermes\isolation\opencode-v1.18.30')
ISOLATION_GENERATED_DIRS = {".quasar"}
ENVIRONMENTAL_DIRS = {".idea", ".vscode"}
# REGENERABLE_DEPENDENCY classification: directory trees that are pure
# dependency materialization, reproducible from the custodied package
# manifest and lockfile (npm/yarn/pnpm install). They carry no authoritative
# Product source, so raw full-tree parity inventories (profile-less
# include_git mode) and isolated workspace copies skip the whole subtree
# instead of stat-walking tens of thousands of installed files whose OS-level
# metadata (directory index/size) can drift under indexer/defender activity
# without any entry or byte change. Profiled custody inventories still
# traverse dependency trees: tracked, local-dev-config, protected, and
# preserved files inside them keep full fail-closed custody. package.json,
# lockfiles, and every path outside these directories keep full custody in
# all modes.
REGENERABLE_DEPENDENCY_DIRS = frozenset({"node_modules"})


def validate_git_ownership(root, profile):
    """Resolve effective Git configuration read-only before any Git mutation."""
    from runtime_safety import git_subprocess_env, trusted_executable
    root = Path(root).resolve()
    repos = [*([root] if (root/'.git').exists() else []),
             *(root/module.path for module in profile.modules)]
    for repo in dict.fromkeys(repos):
        # Query every module: absence of local metadata must not let Git discover
        # a repository outside custody. Monorepo modules may legitimately use
        # the nearest in-custody parent repository; independent nested repos
        # must still be validated even when the workspace root has its own Git.
        intended = next((parent for parent in (repo, *repo.parents)
                         if parent.is_relative_to(root) and (parent/'.git').exists()), None)
        if intended is None:
            raise CandidateError('CANDIDATE_GIT_OWNERSHIP_INVALID')
        configured = subprocess.run([trusted_executable('git', forbidden_root=root), '-C', str(repo),
            'config', '--includes', '--path', '--get', 'core.worktree'], capture_output=True,
            timeout=30, env=git_subprocess_env())
        value = configured.stdout.decode('utf-8', errors='replace').strip()
        if configured.returncode not in {0,1} or (configured.returncode == 0 and (
                not value or (intended/'.git'/Path(value)).resolve() != intended.resolve())):
            raise CandidateError('CANDIDATE_GIT_OWNERSHIP_INVALID')
        for option, expected in (('--show-toplevel', intended), ('--absolute-git-dir', intended/'.git'),
                                 ('--git-common-dir', intended/'.git')):
            process = subprocess.run([trusted_executable('git', forbidden_root=root), '-C', str(repo),
                'rev-parse', option], capture_output=True, timeout=30, env=git_subprocess_env())
            value = process.stdout.decode('utf-8', errors='replace').strip()
            actual = (repo/Path(value)).resolve() if value else None
            if process.returncode or actual != expected.resolve():
                raise CandidateError('CANDIDATE_GIT_OWNERSHIP_INVALID')


def tracked_paths(root, profile):
    from runtime_safety import git_subprocess_env, trusted_executable
    root = Path(root).absolute()
    result = set()
    repos = {parent for module in profile.modules
             for parent in (root/module.path, *(root/module.path).parents)
             if parent.is_relative_to(root) and (parent/'.git').exists()}
    for repo in sorted(repos):
        process = subprocess.run([trusted_executable('git', forbidden_root=root), '-C', str(repo),
            'ls-files', '-z', '--cached'], capture_output=True, check=True, timeout=30,
            env=git_subprocess_env())
        for rel in process.stdout.decode('utf-8').split('\0'):
            if rel:
                result.add((repo/rel).relative_to(root).as_posix())
    return result


def _file_identity(st):
    return (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns,
            st.st_nlink, getattr(st, 'st_file_attributes', 0))


def _same_handle_identity(handle, path_stat):
    actual, expected = _file_identity(handle), _file_identity(path_stat)
    # Windows CRT fstat reports st_ctime as last-write time for an open handle,
    # while path lstat reports creation time. Path-to-path checks keep ctime.
    return (actual[:4] == expected[:4] and actual[5:] == expected[5:]) if os.name == 'nt' else actual == expected


def _open_inventory_fd(path):
    if os.name != 'nt':
        return os.open(path, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0))
    import ctypes
    import msvcrt
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    create = kernel.CreateFileW
    create.argtypes = (ctypes.c_wchar_p, ctypes.c_uint32, ctypes.c_uint32,
                       ctypes.c_void_p, ctypes.c_uint32, ctypes.c_uint32, ctypes.c_void_p)
    create.restype = ctypes.c_void_p
    # FILE_SHARE_READ excludes an existing writer and prevents a new writer
    # while this handle is hashed. OPEN_REPARSE_POINT does not follow a link.
    handle = create(str(path), 0x80000000, 1, None, 3, 0x00200000, None)
    if handle == ctypes.c_void_p(-1).value:
        raise OSError(ctypes.get_last_error(), 'inventory file cannot be opened read-only')
    try:
        return msvcrt.open_osfhandle(handle, os.O_RDONLY | os.O_BINARY)
    except BaseException:
        kernel.CloseHandle(ctypes.c_void_p(handle))
        raise


def _hash_inventory_file(path, expected):
    """Hash through one verified handle; reject replacement and concurrent writes."""
    try:
        fd = _open_inventory_fd(path)
        try:
            opened = os.fstat(fd)
            if (not stat.S_ISREG(opened.st_mode) or opened.st_nlink != 1
                    or not _same_handle_identity(opened, expected)):
                raise CandidateError('CANDIDATE_FILE_CHANGED_DURING_INVENTORY')
            digest = hashlib.sha256()
            with os.fdopen(fd, 'rb', closefd=False) as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                    digest.update(chunk)
            after_handle = os.fstat(fd)
        finally:
            os.close(fd)
        after_path = path.lstat()
    except OSError as exc:
        raise CandidateError('CANDIDATE_FILE_UNREADABLE') from exc
    if (not stat.S_ISREG(after_path.st_mode)
            or stat.S_ISLNK(after_path.st_mode)
            or getattr(after_path, 'st_file_attributes', 0) & 0x400
            or after_path.st_nlink != 1
            or not _same_handle_identity(after_handle, expected)
            or _file_identity(after_path) != _file_identity(expected)):
        raise CandidateError('CANDIDATE_FILE_CHANGED_DURING_INVENTORY')
    return digest.hexdigest()


def inventory(root, *, include_git=False, profile=None, preserve=(), workers=None):
    """Trusted host inventory. Sensitive local config is hashed, never exported."""
    root = Path(root).absolute()
    result = {}
    safe_cap = min(32, max(1, (os.cpu_count() or 1) + 4))
    if workers is None:
        configured = os.environ.get('KKM_INVENTORY_WORKERS', '')
        try:
            workers = int(configured) if configured else safe_cap
        except ValueError as exc:
            raise CandidateError('CANDIDATE_INVENTORY_WORKERS_INVALID') from exc
    if type(workers) is not int or not 1 <= workers <= safe_cap:
        raise CandidateError('CANDIDATE_INVENTORY_WORKERS_INVALID')
    from gate_evidence import IGNORED_DIRS
    generated_dirs = IGNORED_DIRS | ISOLATION_GENERATED_DIRS
    tracked = tracked_paths(root, profile) if profile and not include_git else set()
    for ancestor in (root, *root.parents):
        st = ancestor.lstat()
        if stat.S_ISLNK(st.st_mode) or getattr(st, 'st_file_attributes', 0) & 0x400:
            raise CandidateError('CANDIDATE_LINK_DENIED')
    directories = [(root, root.lstat())]
    pending = deque()
    executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix='inventory')
    try:
      while directories:
        directory, expected_directory = directories.pop()
        current_directory = directory.lstat()
        if (not stat.S_ISDIR(current_directory.st_mode)
                or stat.S_ISLNK(current_directory.st_mode)
                or getattr(current_directory, 'st_file_attributes', 0) & 0x400):
            raise CandidateError('CANDIDATE_DIRECTORY_CHANGED_DURING_INVENTORY')
        with os.scandir(directory) as entries:
            entries = list(entries)
        # Content custody, not raw stat stability: a directory whose identity
        # drifted while listed (OS metadata/index churn, indexer activity) is
        # tolerated exactly when the entry-name set is unchanged. Entries that
        # appear or disappear still fail closed, and every included file keeps
        # its own open-handle/identity/hash checks.
        if _file_identity(directory.lstat()) != _file_identity(current_directory):
            if sorted(entry.name for entry in entries) != sorted(os.listdir(directory)):
                raise CandidateError('CANDIDATE_DIRECTORY_CHANGED_DURING_INVENTORY')
        for entry in entries:
            name = entry.name
            path = Path(entry.path)
            rel = path.relative_to(root).as_posix()
            st = entry.stat(follow_symlinks=False)
            if stat.S_ISLNK(st.st_mode) or getattr(st, 'st_file_attributes', 0) & 0x400:
                raise CandidateError('CANDIDATE_LINK_DENIED')
            if not (stat.S_ISDIR(st.st_mode) or stat.S_ISREG(st.st_mode)):
                raise CandidateError('CANDIDATE_SPECIAL_FILE_DENIED')
            # Windows DirEntry metadata omits link counts. Still inspect every
            # regular file, including excluded output, for hardlink custody.
            if os.name == 'nt' and (stat.S_ISREG(st.st_mode) or stat.S_ISDIR(st.st_mode)):
                st = path.lstat()
                if stat.S_ISLNK(st.st_mode) or getattr(st, 'st_file_attributes', 0) & 0x400:
                    raise CandidateError('CANDIDATE_LINK_DENIED')
                if not (stat.S_ISREG(st.st_mode) or stat.S_ISDIR(st.st_mode)):
                    raise CandidateError('CANDIDATE_SPECIAL_FILE_DENIED')
            if stat.S_ISREG(st.st_mode) and st.st_nlink != 1:
                raise CandidateError('CANDIDATE_HARDLINK_DENIED')
            # REGENERABLE_DEPENDENCY: in raw full-tree parity inventories
            # (profile-less include_git mode used by isolation/revalidation/
            # batch copies) installed dependency trees are skipped whole. In
            # profiled custody mode the traversal stays: tracked, local-dev,
            # protected, and preserved files under a dependency directory keep
            # their identity exactly as under any generated directory.
            if (profile is None and stat.S_ISDIR(st.st_mode)
                    and name in REGENERABLE_DEPENDENCY_DIRS):
                continue
            git_metadata = not include_git and '.git' in Path(rel).parts
            # Profiled custody replaces the old full inventory: retain its
            # metadata link checks without hashing Git objects or payloads.
            excluded = git_metadata and (not profile or stat.S_ISREG(st.st_mode))
            if profile and not include_git:
                # Established gate output directories are excluded only for
                # untracked, undeclared artifacts. Source and frozen surfaces
                # keep their identity even under a conventional output name.
                explicit = (rel in tracked or profile.is_local_dev_config(rel)
                    or profile.is_protected_path(rel)
                    or any(rel == p.rstrip('/') or rel.startswith(p.rstrip('/')+'/')
                           or fnmatch.fnmatchcase(rel, p) for p in preserve))
                generated = any(part in generated_dirs | ENVIRONMENTAL_DIRS for part in Path(rel).parts)
                if not explicit and (generated or profile.is_non_source_artifact(rel)):
                    # Still traverse/stat generated trees: links cannot hide in
                    # an excluded directory and later reach an included path.
                    if stat.S_ISREG(st.st_mode):
                        excluded = True
            if excluded:
                continue
            if stat.S_ISDIR(st.st_mode):
                directories.append((path, st))
            if stat.S_ISREG(st.st_mode):
                pending.append((rel, executor.submit(_hash_inventory_file, path, st)))
                if len(pending) >= workers * 2:
                    name, future = pending.popleft()
                    result[name] = future.result()
      while pending:
          name, future = pending.popleft()
          result[name] = future.result()
    finally:
        for _, future in pending:
            future.cancel()
        executor.shutdown(wait=True, cancel_futures=True)
    return dict(sorted(result.items()))


def copy_workspace_tree(source, dest):
    """Copy a workspace for isolated custody with the REGENERABLE_DEPENDENCY
    classification applied: installed dependency trees are not copied because
    they are reproducible from the custodied package manifest/lockfile and
    dominate copy time without carrying authoritative source. Every other
    path is copied byte-for-byte with copytree semantics; symlinks/reparse
    points inside the skipped trees are still denied."""
    source, dest = Path(source), Path(dest)

    def _ignore(directory, names):
        ignored = []
        for name in names:
            if name in REGENERABLE_DEPENDENCY_DIRS:
                target = Path(directory) / name
                if target.is_symlink():
                    raise CandidateError('CANDIDATE_LINK_DENIED')
                if target.is_dir():
                    ignored.append(name)
        return ignored

    return shutil.copytree(source, dest, ignore=_ignore, copy_function=shutil.copy2)


def normalized_preserved_inventory(root, recorded, *, profile, preserve=()):
    """Apply current generated-output rules to a recorded candidate inventory.

    This permits custody receipts captured before a Build to be compared with
    the same candidate after known generated output has been materialized.
    Tracked, configured, protected, and explicitly preserved paths remain
    authoritative even when nested below a generated directory.
    """
    root = Path(root)
    expected = dict(recorded or {})
    from gate_evidence import IGNORED_DIRS
    generated_dirs = IGNORED_DIRS | ISOLATION_GENERATED_DIRS
    tracked = tracked_paths(root, profile)
    for rel in list(expected):
        explicit = (
            rel in tracked
            or profile.is_local_dev_config(rel)
            or profile.is_protected_path(rel)
            or any(
                rel == item.rstrip("/")
                or rel.startswith(item.rstrip("/") + "/")
                or fnmatch.fnmatchcase(rel, item)
                for item in preserve
            )
        )
        if not explicit and (any(part in generated_dirs | ENVIRONMENTAL_DIRS for part in Path(rel).parts)
                             or profile.is_non_source_artifact(rel)):
            expected.pop(rel)
    return expected


def secret_path(rel):
    return any(part.casefold() in SECRET_NAMES | {'credentials.json', '.opencode', '.config', '.vscode', '.idea', '.mcp.json', 'opencode.json', 'opencode.jsonc', '.venv', 'node_modules'}
               or part.casefold().startswith('.env.')
               or part.casefold().endswith(('.pem', '.key', '.pfx', '.p12'))
               for part in Path(rel).parts)


def credential_loader():
    # Deliberately not environment driven; never serialize this return value.
    try:
        value = read(Path.home()/'.local/share/opencode/auth.json')['opencode-go']['key']
        if isinstance(value, str) and value.strip():
            return value
    except (OSError, ValueError, KeyError, TypeError):
        pass
    raise CandidateError('OPENCODE_PROVIDER_CREDENTIAL_UNAVAILABLE')


class ProductionIsolation:
    def __init__(self, harness, repository, *, root=None, runtime_root=RUNTIME):
        self.harness, self.repository = harness, repository
        self.canonical = Path(harness.working_dir).resolve()
        key = hashlib.sha256(str(harness.control_root).casefold().encode()).hexdigest()[:16]
        self.root = Path(root or Path(r'D:\hermes\isolation')/('control-'+key)).absolute()
        for ancestor in (self.root, *self.root.parents):
            if ancestor.exists() and (ancestor.is_symlink() or getattr(ancestor.lstat(), 'st_file_attributes', 0) & 0x400):
                raise CandidateError('CANDIDATE_LINK_DENIED')
        self.runtime_root = Path(runtime_root)
        for protected in (self.canonical, harness.profile_dir, harness.control_root,
                          harness.task_root, self.runtime_root):
            protected = Path(protected).resolve()
            if (self.root.resolve().is_relative_to(protected)
                    or protected.is_relative_to(self.root.resolve())):
                raise CandidateError('ISOLATED_ALLOCATION_OVERLAP')
        from isolated_promotion import IsolatedCanonicalPromotion
        self.promoter = IsolatedCanonicalPromotion(self.canonical, self.root/'publication', repository)

    def preflight(self):
        from bounded_execution import enabled
        if not enabled(self.repository):
            raise CandidateError('BOUNDED_SCHEMA_REQUIRED')
        self.promoter.recover()
        # An unfinished allocation from another process is evidence of uncertain
        # ownership. Never auto-allocate a replacement view or resume its session.
        for manifest in self.root.glob('run-*/allocation.json'):
            if read(manifest).get('state') not in {'CLOSED', 'PUBLISHED'}:
                raise CandidateError('ISOLATED_CUSTODY_RECOVERY_REQUIRED')
        self.descriptor()

    def descriptor(self):
        check = self.root/'preflight'
        for name in ('candidate', 'context', 'evidence'):
            (check/name).mkdir(parents=True, exist_ok=True)
        adapter = self.adapter(check, 'PREFLIGHT', 'PREFLIGHT', 'PREFLIGHT', 'PREFLIGHT', 'PREFLIGHT')
        return adapter.preflight()

    def adapter(self, root, execution, epoch, candidate, job, attempt):
        return IsolatedOpenCodeRuntimeAdapter(execution_id=execution, execution_epoch=epoch,
            candidate_id=candidate, job_id=job, attempt_id=attempt,
            candidate_dir=root/'candidate', context_dir=root/'context',
            runtime_binary=self.runtime_root/'opencode', provider_catalog=self.runtime_root/'provider-catalog.json',
            credential_loader=credential_loader, canonical_root=self.canonical, evidence_dir=root/'evidence')

    def __call__(self, job, repository):
        from harness_service import HarnessService
        from run import SingleInstanceLock
        from bounded_execution import enabled
        if repository is not self.repository or not enabled(repository):
            raise CandidateError('BOUNDED_SCHEMA_REQUIRED')
        request = job['request']
        if request.get('worker') != 'opencode' or request.get('model') != MODEL:
            raise CandidateError('ISOLATED_WORKER_ROUTE_REQUIRED')
        if repository.active_execution(job['job_id']):
            raise CandidateError('ISOLATED_EXECUTION_RECOVERY_REQUIRED')
        path = self.root/('run-'+uuid.uuid4().hex[:16])
        path.mkdir(parents=True, exist_ok=False)
        candidate, source = path/'candidate', path/'source'
        profile, _ = self.harness._load_profile()
        with canonical_ownership(self.canonical):
            before = inventory(self.canonical, include_git=True)
            if any(rel.endswith('/.git') or rel == '.git' or ('/'+rel).endswith('/.git/commondir')
                   or ('/'+rel).endswith('/.git/objects/info/alternates') for rel in before):
                raise CandidateError('CANDIDATE_SHARED_GIT_METADATA_DENIED')
            copy_workspace_tree(self.canonical, candidate)
            copy_workspace_tree(self.canonical, source)
            if (inventory(self.canonical, include_git=True) != before
                    or inventory(candidate, include_git=True) != before
                    or inventory(source, include_git=True) != before):
                raise CandidateError('CANDIDATE_BASE_MISMATCH')
            # Git includes/core.worktree are interpreted by Git itself. A
            # byte-identical .git/config can still point the copy at canonical.
            validate_git_ownership(candidate, profile)
            validate_git_ownership(source, profile)
        profile_origin = inventory(self.harness.profile_dir, include_git=True)
        shutil.copytree(self.harness.profile_dir, path/'profiles'/profile.id)
        if inventory(self.harness.profile_dir, include_git=True) != profile_origin:
            raise CandidateError('PROFILE_DRIFT')
        manifest = path/'profiles'/profile.id/'project.json'
        original = read(manifest)
        original['workspace_roots'] = [*original['workspace_roots'], str(candidate)]
        atomic(manifest, original)
        record = {'job_id': job['job_id'], 'candidate_id': 'OCAND-'+uuid.uuid4().hex,
            'candidate': str(candidate), 'source': str(source), 'state': 'ALLOCATED',
            'profile_origin': str(self.harness.profile_dir), 'profile_origin_inventory': profile_origin,
            'profile_origin_sha256': hashlib.sha256((self.harness.profile_dir/'project.json').read_bytes()).hexdigest()}
        write_once(path/'allocation.json', record)
        child = HarnessService(candidate, path/'profiles'/profile.id, task_root=self.harness.task_root,
            snapshot_root=self.harness.snapshot_root, manager_factory=self.harness.manager_factory,
            instance_lock=SingleInstanceLock(path/"execution.lock"),
            supervisor_advisor=getattr(self.harness, 'supervisor_advisor', None))
        child.isolated_execution = ExecutionCustody(self, path, record, profile)
        self.promoter.register(job, child, source)
        return child

    def reopen_preserved(self, job, *, authorized_head_refresh=False):
        """Reopen one closed preserved candidate for Worker-free gate recovery."""
        from harness_service import HarnessService
        from run import SingleInstanceLock
        allocation = Path(str(job.get("candidate_manifest", ""))).resolve()
        if (
            allocation.name != "allocation.json"
            or not allocation.is_relative_to(self.root.resolve())
            or not allocation.is_file()
        ):
            raise CandidateError("PRESERVED_CANDIDATE_MANIFEST_INVALID")
        record = read(allocation)
        cleanup = read(allocation.parent / "cleanup.json")
        expected = {
            "job_id": str(job.get("job_id", "")),
            "candidate_id": str(job.get("candidate_id", "")),
            "execution_id": str(dict(job.get("last_result") or {}).get("execution_id", "")),
        }
        if (
            record.get("state") != "CLOSED"
            or any(record.get(key) != value for key, value in expected.items())
            or cleanup.get("state") != "QUIESCED"
            or cleanup.get("quiesced") is not True
            or cleanup.get("imported") is not True
        ):
            raise CandidateError("PRESERVED_CANDIDATE_CUSTODY_INVALID")
        profile_dir = allocation.parent / "profiles" / self.harness._load_profile()[0].id
        child = HarnessService(
            record["candidate"],
            profile_dir,
            task_root=self.harness.task_root,
            snapshot_root=self.harness.snapshot_root,
            manager_factory=self.harness.manager_factory,
            instance_lock=SingleInstanceLock(allocation.parent / "recovery.lock"),
            supervisor_advisor=getattr(self.harness, 'supervisor_advisor', None),
        )
        profile, _ = child._load_profile()
        preserve = [
            *job.get("request", {}).get("target_resources", []),
            *job.get("request", {}).get("test_plan", {}).get("required_tests", []),
        ]
        expected = normalized_preserved_inventory(
            Path(record["candidate"]), cleanup.get("host_input") or {},
            profile=profile, preserve=preserve,
        )
        if inventory(Path(record["candidate"]), profile=profile, preserve=preserve) != expected:
            raise CandidateError("PRESERVED_CANDIDATE_CONTENT_CHANGED")
        child.isolated_execution = ExecutionCustody(self, allocation.parent, record, profile)
        self.promoter.register_preserved(
            job, child, record["source"],
            authorized_head_refresh=authorized_head_refresh,
        )
        return child


class ExecutionCustody:
    def __init__(self, owner, root, record, profile):
        self.owner, self.root, self.record, self.profile = owner, root, record, profile
        self.candidate = Path(record['candidate'])
        self.task = None
        self.adapters = []

    def descriptor(self):
        return self.owner.descriptor()

    def bind(self, task):
        self.verify_origin()
        if self.task is not None or task.job_id != self.record['job_id']:
            raise CandidateError('CANDIDATE_OWNERSHIP_MISMATCH')
        if task.worker != 'opencode' or task.opencode_model != MODEL:
            raise CandidateError('ISOLATED_WORKER_ROUTE_REQUIRED')
        self.task = task
        self.record.update(execution_id=task.execution_id, state='RUNNING')
        atomic(self.root/'allocation.json', self.record)
        task.candidate_id = self.record['candidate_id']
        task.candidate_manifest = str(self.root/'allocation.json')
        task._isolated_opencode = True
        task._isolated_eol_repair = self.repair_eol

    def verify_origin(self):
        if inventory(self.record['profile_origin'], include_git=True) != self.record['profile_origin_inventory']:
            raise CandidateError('PROFILE_DRIFT')

    def repair_eol(self, task, collector, baseline):
        """One trusted normalization handoff before the existing gates capture bytes."""
        if task is not self.task or Path(collector.working_dir).resolve() != self.candidate:
            raise CandidateError('CANDIDATE_OWNERSHIP_MISMATCH')
        receipt = read(self.root/'cleanup.json')
        expected = dict(job_id=task.job_id, execution_id=task.execution_id,
            candidate_id=task.candidate_id, attempt_id=task.attempt_id, execution_epoch=task.execution_epoch)
        preserve = [*task.target_resources, *task.test_plan.get('required_tests', [])]
        before = inventory(self.candidate, profile=self.profile, preserve=preserve)
        if (any(receipt.get(key) != value for key,value in expected.items())
                or receipt.get('quiesced') is not True or receipt.get('imported') is not True
                or receipt.get('host_input') != before
                or any(not adapter.confirm_quiescence() for adapter in self.adapters)):
            raise CandidateError('CANDIDATE_CUSTODY_PROOF_INVALID')
        inputs = {rel:(self.candidate/rel).read_bytes() for rel in task.changed_files if rel in before}
        result = collector.repair_eol_to_index(task, baseline)
        after = inventory(self.candidate, profile=self.profile, preserve=preserve)
        changed = {rel for rel in set(before)|set(after) if before.get(rel)!=after.get(rel)}
        if not result.success or not changed.issubset(set(result.repaired_files)):
            raise CandidateError('CANDIDATE_EOL_HANDOFF_INVALID')
        for rel in changed:
            if rel not in inputs or rel not in after or re.sub(br'\r\n|\r|\n', b'\n', inputs[rel]) != re.sub(
                    br'\r\n|\r|\n', b'\n', (self.candidate/rel).read_bytes()):
                raise CandidateError('CANDIDATE_EOL_HANDOFF_INVALID')
        if changed:
            normalization = {'policy':'EXISTING_REPAIR_EOL_TO_INDEX', 'files':sorted(changed),
                'before':{rel:before[rel] for rel in changed}, 'after':{rel:after[rel] for rel in changed}}
            atomic(self.root/'cleanup.json', {**receipt, 'host_input':after,
                'normalizations':[*receipt.get('normalizations',[]), normalization]})
        return result

    def worker(self, **kwargs):
        from worker import OpenCodeWorker
        custody = self
        class OwnedWorker:
            def execute(self, requirement, working_dir, task=None):
                if task is not custody.task or Path(working_dir).resolve() != custody.candidate:
                    raise CandidateError('CANDIDATE_OWNERSHIP_MISMATCH')
                if kwargs['model'] != MODEL or task.retry_model not in {'', MODEL}:
                    raise CandidateError('ISOLATED_WORKER_ROUTE_REQUIRED')
                return custody.work(OpenCodeWorker, kwargs, requirement, task)
        return OwnedWorker()

    def work(self, worker_type, kwargs, requirement, task):
        self.verify_origin()
        if any(not adapter.confirm_quiescence() for adapter in self.adapters):
            raise CandidateError('CANDIDATE_WRITER_NOT_QUIESCED')
        epoch = uuid.uuid4().hex
        task.execution_epoch = epoch
        root = self.root/('work-'+epoch[:12])
        (root/'candidate').mkdir(parents=True)
        (root/'context').mkdir()
        before = inventory(self.candidate, include_git=True)
        view_before = {}
        for rel, digest in before.items():
            if secret_path(rel) or self.profile.is_local_dev_config(rel) or self.profile.is_non_source_artifact(rel):
                continue
            dest = root/'candidate'/rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(self.candidate/rel, dest)
            view_before[rel] = digest
        adapter = self.owner.adapter(root, task.execution_id, epoch, task.candidate_id, task.job_id, task.attempt_id)
        self.adapters.append(adapter)
        def prompt_transform(text):
            for host, mounted in ((root/'candidate', '/workspace'), (self.candidate, '/workspace'),
                    (self.owner.canonical, '/workspace'),
                    (self.owner.harness.profile_dir, '/context/profile')):
                text = text.replace(str(host), mounted).replace(str(host).replace('\\','/'), mounted)
            (root/'context/context.md').write_text(text, encoding='utf-8')
            return text
        worker = worker_type(**kwargs, adapter_factory=lambda: adapter,
            prompt_transform=prompt_transform,
            ephemeral_session=True)
        result = None
        try:
            result = worker.execute(requirement, str(root/'candidate'), task=task)
        finally:
            adapter.stop_local_server()
            if not adapter.confirm_quiescence():
                raise CandidateError('CANDIDATE_WRITER_NOT_QUIESCED')
            if task.native_session_binding_id:
                from control_repository import ControlRepository
                ControlRepository(task.control_repository_path).update_session_state(task.native_session_binding_id, 'TERMINAL')
        after = inventory(root/'candidate', include_git=True)
        if inventory(self.candidate, include_git=True) != before:
            raise CandidateError('CANDIDATE_HOST_INPUT_CHANGED')
        changed = sorted(rel for rel in set(view_before)|set(after) if view_before.get(rel) != after.get(rel))
        scopes = list(task.target_resources or [])
        if not scopes:
            raise CandidateError("CANDIDATE_DECLARED_SCOPE_REQUIRED")
        for rel in changed:
            if (secret_path(rel) or self.profile.is_local_dev_config(rel) or self.profile.is_non_source_artifact(rel)
                    or self.profile.is_protected_path(rel)
                    or not any(rel == p.rstrip('/') or rel.startswith(p.rstrip('/')+'/')
                               or fnmatch.fnmatchcase(rel, p) for p in scopes)):
                raise CandidateError('CANDIDATE_UNDECLARED_WRITE')
        if result.success:
            if not changed:
                raise CandidateError('CANDIDATE_NO_MEANINGFUL_DELTA')
            for rel in changed:
                target = self.candidate/rel
                if rel in after:
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(root/'candidate'/rel, target)
                else:
                    target.unlink()
        atomic(self.root/'cleanup.json', {**adapter.boundary_receipt(), 'state': 'QUIESCED',
            'imported': bool(result.success), 'host_input': inventory(self.candidate, profile=self.profile,
                preserve=[*task.target_resources, *task.test_plan.get('required_tests', [])])})
        return result

    def close(self):
        for adapter in self.adapters:
            adapter.stop_local_server()
            if not adapter.confirm_quiescence():
                raise CandidateError('CANDIDATE_WRITER_NOT_QUIESCED')
        self.record['state'] = 'CLOSED'
        atomic(self.root/'allocation.json', self.record)
