"""
Git Diff 수집기 (모듈별 개별 저장소 대응)
- working_dir 가 모듈별 개별 git 저장소를 가질 때
  TaskState.target_module 리스트를 참조해 각 모듈 디렉토리에서 Git 명령어 실행.
- 모듈 미지정([]) 시 working_dir 루트 자체를 저장소로 간주.
- 여러 모듈의 diff/changed_files 를 병합(merge)하여 TaskState 에 기록.
- 롤백 역시 모듈별 작업 기준점과 delta로 수행한다.
- profile의 로컬 개발 설정 제외 목록은 게이트/리뷰 입력(changed_files/
  diff/diff_stat)에서 제외하고 excluded_files 로 별도 기록한다.
  원본은 지우지 않는다(git 에서 항상 재조회 가능). 롤백에서도 보존.
"""

from __future__ import annotations

import re
import os
import hashlib
import json
import subprocess
import tempfile
from harness_temp import scratch_root
import time
from dataclasses import dataclass, field
from pathlib import Path

from task_state import (
    TaskState,
    split_local_dev_configs,
)
from project_profile import ProjectProfile
from runtime_safety import git_subprocess_env, scrub_secrets, trusted_executable


# diff 본문에서 파일별 섹션을 나누는 헤더(b-side 경로 추출용).
# 경로에 " b/" 가 포함되는 극단적 케이스는 미지원(제외 매칭 판정에만 사용).
_DIFF_HEADER_RE = re.compile(r"^diff --git a/.+? b/(.+?)\s*$")
_STAT_SUMMARY_MARKS = ("file changed", "files changed")


def _trim_diff_transport(value: str) -> str:
    """Remove transport newlines without deleting unified-diff context bytes."""
    return value.strip("\r\n")


def _filter_diff_body(
    diff: str,
    profile: ProjectProfile | None = None,
    prefix: str = "",
) -> str:
    """diff 본문에서 로컬 개발 설정 파일 섹션(diff --git 단위)을 제외하고 반환.
    모듈 헤더(--- [module: x] ---) 라인은 유지한다."""
    if not diff.strip():
        return diff
    kept: list[str] = []
    current: list[str] = []
    target = ""
    for line in diff.splitlines(keepends=True):
        m = _DIFF_HEADER_RE.match(line)
        if m:
            if current and not (
                profile and profile.is_local_dev_config(prefix + target)
            ):
                kept.append("".join(current))
            current = [line]
            target = m.group(1).strip('"')
        else:
            current.append(line)
    if current and not (profile and profile.is_local_dev_config(prefix + target)):
        kept.append("".join(current))
    return "".join(kept)


def _filter_stat_body(
    stat: str,
    profile: ProjectProfile | None = None,
    prefix: str = "",
) -> str:
    """diff --stat 출력에서 로컬 개발 설정 파일 라인을 제외하고 반환.
    제외가 발생한 경우 총계 요약 라인도 함께 제거(건수가 어긋나므로)."""
    if not stat.strip():
        return stat
    out: list[str] = []
    dropped = False
    for ln in stat.splitlines(keepends=True):
        path = ln.split(" | ", 1)[0].strip() if " | " in ln else ""
        if path and profile and profile.is_local_dev_config(prefix + path):
            dropped = True
            continue
        if dropped and any(mk in ln for mk in _STAT_SUMMARY_MARKS):
            continue
        out.append(ln)
    return "".join(out)


@dataclass
class GitDiffResult:
    success: bool
    changed_files: list[str] = field(default_factory=list)
    # 게이트/리뷰에서 제외된 로컬 개발 설정 파일(기록/표시용)
    excluded_files: list[str] = field(default_factory=list)
    status_short: str = ""
    diff_stat: str = ""
    diff: str = ""
    error: str = ""


@dataclass
class EolRepairResult:
    success: bool
    repaired_files: list[str] = field(default_factory=list)
    skipped_files: list[str] = field(default_factory=list)
    error: str = ""
    diagnostics: list[dict[str, object]] = field(default_factory=list)


@dataclass(frozen=True)
class GitRepoBaseline:
    git_dir: Path
    module: str | None
    head_oid: str
    worktree_tree: str
    index_tree: str


@dataclass(frozen=True)
class GitTaskBaseline:
    repos: tuple[GitRepoBaseline, ...]
    snapshot_manifest: Path | None = None


class GitCollector:
    def __init__(self, working_dir: str, profile: ProjectProfile | None = None):
        self.working_dir = Path(working_dir).resolve()
        self.profile = profile

    def _run_in(
        self,
        git_dir: Path,
        args: list[str],
        index_file: str | Path | None = None,
    ) -> tuple[int, str, str]:
        """지정 디렉토리에서 git 명령 실행."""
        try:
            result = subprocess.run(
                [trusted_executable("git", forbidden_root=self.working_dir)] + args,
                cwd=str(git_dir),
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                env=git_subprocess_env(index_file),
            )
            return result.returncode, result.stdout, result.stderr
        except Exception as e:
            return -1, "", str(e)

    def _git_dirs(self, task: TaskState | None) -> list[tuple[Path, str | None]]:
        profile_workspace = bool(
            self.profile
            and any(
                os.path.normcase(str(root.resolve()))
                == os.path.normcase(str(self.working_dir))
                for root in self.profile.workspace_roots
            )
        )
        if profile_workspace:
            # A root repository already covers every profile module path. Otherwise
            # capture every independent module repository so sibling changes cannot
            # escape the Job baseline and deterministic scope review.
            if (self.working_dir / ".git").exists():
                return [(self.working_dir, None)]
            return [
                (
                    self.working_dir / Path(*module.path.split("/")),
                    module.path,
                )
                for module in self.profile.modules
            ]
        modules = task.target_module if task is not None else []
        if modules:
            return [(self.working_dir / module, module) for module in modules]
        return [(self.working_dir, None)]

    def _temporary_worktree_tree(self, git_dir: Path) -> tuple[bool, str, str]:
        """현재 worktree를 임시 index로 tree화한다. 실제 index는 건드리지 않는다."""
        fd, index_path = tempfile.mkstemp(prefix="kkm-index-", dir=scratch_root())
        os.close(fd)
        os.unlink(index_path)  # 빈 파일은 유효한 Git index가 아니므로 read-tree가 만들게 한다.
        try:
            code, _, _ = self._run_in(
                git_dir, ["rev-parse", "--verify", "HEAD"], index_file=index_path
            )
            read_args = ["read-tree", "HEAD"] if code == 0 else ["read-tree", "--empty"]
            code, out, err = self._run_in(git_dir, read_args, index_file=index_path)
            if code != 0:
                return False, "", (err or out).strip()
            code, out, err = self._run_in(
                git_dir, ["add", "-A"], index_file=index_path
            )
            if code != 0:
                return False, "", (err or out).strip()
            code, out, err = self._run_in(
                git_dir, ["write-tree"], index_file=index_path
            )
            if code != 0:
                return False, "", (err or out).strip()
            return True, out.strip(), ""
        finally:
            try:
                os.unlink(index_path)
            except FileNotFoundError:
                pass

    def capture_baseline(
        self, task: TaskState | None = None
    ) -> tuple[GitTaskBaseline | None, str]:
        """Job 시작 시 모듈별 worktree/index 기준점을 캡처한다."""
        repos: list[GitRepoBaseline] = []
        for git_dir, module in self._git_dirs(task):
            if not git_dir.exists() or not git_dir.is_dir():
                return None, f"디렉토리 없음: {git_dir}"
            code, top, err = self._run_in(git_dir, ["rev-parse", "--show-toplevel"])
            if code != 0:
                return None, f"git 저장소 아님({git_dir.name}): {(err or top).strip()}"
            repo_dir = Path(top.strip()).resolve()
            code, head, _ = self._run_in(repo_dir, ["rev-parse", "--verify", "HEAD"])
            head_oid = head.strip() if code == 0 else ""
            code, index_tree, err = self._run_in(repo_dir, ["write-tree"])
            if code != 0:
                return None, f"index 기준점 생성 실패({repo_dir.name}): {err.strip()}"
            ok, worktree_tree, error = self._temporary_worktree_tree(repo_dir)
            if not ok:
                return None, f"worktree 기준점 생성 실패({repo_dir.name}): {error}"
            repos.append(GitRepoBaseline(
                git_dir=repo_dir,
                module=module,
                head_oid=head_oid,
                worktree_tree=worktree_tree,
                index_tree=index_tree.strip(),
            ))
        return GitTaskBaseline(tuple(repos)), ""

    def verify_baseline_current(self, baseline: GitTaskBaseline) -> tuple[bool, str]:
        """Confirm HEAD, worktree tree, and index still equal a stored snapshot."""
        for repo in baseline.repos:
            code, head, error = self._run_in(
                repo.git_dir, ["rev-parse", "--verify", "HEAD"]
            )
            if code != 0 or head.strip() != repo.head_oid:
                return False, f"PRE_JOB_HEAD_MISMATCH:{repo.module or repo.git_dir.name}"
            ok, worktree_tree, error = self._temporary_worktree_tree(repo.git_dir)
            if not ok or worktree_tree != repo.worktree_tree:
                return False, f"PRE_JOB_WORKTREE_HASH_MISMATCH:{repo.module or repo.git_dir.name}"
            code, index_tree, error = self._run_in(repo.git_dir, ["write-tree"])
            if code != 0 or index_tree.strip() != repo.index_tree:
                return False, f"PRE_JOB_INDEX_HASH_MISMATCH:{repo.module or repo.git_dir.name}"
        return True, ""

    def _default_baseline(self, git_dir: Path, module: str | None) -> GitRepoBaseline | None:
        """직접 collect 호출 호환용: HEAD를 작업 시작점으로 간주한다."""
        code, head, _ = self._run_in(git_dir, ["rev-parse", "--verify", "HEAD"])
        head_oid = head.strip() if code == 0 else ""
        if head_oid:
            code, tree, _ = self._run_in(git_dir, ["rev-parse", "HEAD^{tree}"])
            if code != 0:
                return None
            base_tree = tree.strip()
        else:
            fd, index_path = tempfile.mkstemp(prefix="kkm-empty-index-", dir=scratch_root())
            os.close(fd)
            os.unlink(index_path)
            try:
                self._run_in(
                    git_dir, ["read-tree", "--empty"], index_file=index_path
                )
                code, tree, _ = self._run_in(
                    git_dir, ["write-tree"], index_file=index_path
                )
                if code != 0:
                    return None
                base_tree = tree.strip()
            finally:
                try:
                    os.unlink(index_path)
                except FileNotFoundError:
                    pass
        code, index_tree, _ = self._run_in(git_dir, ["write-tree"])
        if code != 0:
            return None
        return GitRepoBaseline(git_dir, module, head_oid, base_tree, index_tree.strip())

    def _delta_paths(self, git_dir: Path, before: str, after: str) -> tuple[bool, list[str], str]:
        code, out, err = self._run_in(
            git_dir, ["diff", "--name-only", "-z", "--no-renames", before, after]
        )
        if code != 0:
            return False, [], (err or out).strip()
        return True, [p for p in out.split("\0") if p], ""

    def _collect_one(
        self, baseline: GitRepoBaseline
    ) -> tuple[bool, str, list[str], list[str], str, str]:
        """
        단일 디렉토리에서 git 수집.
        반환: (success, error, changed_files, excluded_files, diff_stat, diff)
        profile의 로컬 개발 설정은 changed_files/diff/diff_stat 에서
        제외되고 excluded_files 로 별도 기록된다(원본은 git 에서 재조회 가능).
        module 은 다중 모듈 병합 시 changed_files 경로 프리픽스/diff 헤더용(None=루트).
        """
        git_dir = baseline.git_dir
        module = baseline.module
        if not git_dir.exists() or not git_dir.is_dir():
            return False, f"디렉토리 없음: {git_dir}", [], [], "", ""
        prefix = f"{module}/" if module else ""
        code, head, _ = self._run_in(git_dir, ["rev-parse", "--verify", "HEAD"])
        current_head = head.strip() if code == 0 else ""
        if current_head != baseline.head_oid:
            return False, f"작업 중 HEAD 변경 감지({git_dir.name})", [], [], "", ""

        ok, current_tree, error = self._temporary_worktree_tree(git_dir)
        if not ok:
            return False, f"현재 tree 생성 실패({git_dir.name}): {error}", [], [], "", ""
        ok, paths, error = self._delta_paths(
            git_dir, baseline.worktree_tree, current_tree
        )
        if not ok:
            return False, f"작업 delta 수집 실패({git_dir.name}): {error}", [], [], "", ""
        raw_changed = [prefix + path for path in paths]

        # 2.5 로컬 개발 설정 분리 (게이트/리뷰 입력에서 제외, 기록용으로 보존)
        changed_files, excluded_files = split_local_dev_configs(raw_changed, self.profile)

        # 3. staged diff (로컬 개발 설정 섹션 제외)
        _, diff_stat, _ = self._run_in(
            git_dir, ["diff", "--stat", baseline.worktree_tree, current_tree]
        )
        _, diff, _ = self._run_in(
            git_dir, ["diff", "--binary", baseline.worktree_tree, current_tree]
        )
        diff_stat = _filter_stat_body(diff_stat.strip(), self.profile, prefix)
        diff = _filter_diff_body(_trim_diff_transport(diff), self.profile, prefix)

        # 다중 모듈일 때 diff 에 출처 헤더 추가
        if module and diff.strip():
            diff = f"--- [module: {module}] ---\n{diff}"
        if module and diff_stat.strip():
            diff_stat = f"--- [module: {module}] ---\n{diff_stat}"

        return True, "", changed_files, excluded_files, diff_stat, diff

    def collect(
        self,
        task: TaskState | None = None,
        baseline: GitTaskBaseline | None = None,
    ) -> GitDiffResult:
        """
        task.target_module 리스트를 참조해 모듈별 git 수집 후 병합.
        - 모듈 지정 O: working_dir/{모듈} 각각에서 수집
        - 모듈 지정 X: working_dir 루트에서 수집
        - 하나라도 실패하면 success=False (엄격 게이트).
        - 로컬 개발 설정 파일은 changed_files/diff/diff_stat 에서 제외되고
          excluded_files 로 별도 기록된다.
        """
        if baseline is None:
            defaults: list[GitRepoBaseline] = []
            for git_dir, module in self._git_dirs(task):
                item = self._default_baseline(git_dir, module)
                if item is None:
                    return GitDiffResult(success=False, error=f"기준점 생성 실패: {git_dir}")
                defaults.append(item)
            baseline = GitTaskBaseline(tuple(defaults))

        all_changed: list[str] = []
        all_excluded: list[str] = []
        all_status: list[str] = []
        all_diff_stat: list[str] = []
        all_diff: list[str] = []
        first_error = ""
        all_success = True

        for repo in baseline.repos:
            module = repo.module
            ok, err, changed, excluded, diff_stat, diff = self._collect_one(repo)
            if not ok:
                all_success = False
                if not first_error:
                    first_error = err
                continue
            all_changed.extend(changed)
            all_excluded.extend(excluded)
            if module:
                all_status.append(f"[{module}] {len(changed)} file(s)")
            elif changed:
                all_status.append(f"{len(changed)} file(s)")
            if diff_stat:
                all_diff_stat.append(diff_stat)
            if diff:
                all_diff.append(diff)

        res = GitDiffResult(
            success=all_success,
            changed_files=all_changed,
            excluded_files=all_excluded,
            status_short="\n".join(all_status).strip(),
            diff_stat="\n\n".join(all_diff_stat).strip(),
            # Do not insert a blank, prefix-less line between unified-diff file
            # sections.  The lossless Reviewer correctly treats such a line as an
            # invalid hunk body. One newline is the structural separator Git uses.
            diff=_trim_diff_transport("\n".join(all_diff)),
            error=first_error,
        )

        # ===== State 매핑 =====
        if task is not None:
            task.changed_files = res.changed_files
            task.excluded_files = res.excluded_files
            task.git_status = res.status_short
            task.git_diff_stat = res.diff_stat
            task.git_diff = res.diff

        return res

    def prove_no_task_delta(self, baseline: GitTaskBaseline | None) -> dict[str, object]:
        """Prove that the current worktree/index exactly match the pre-Job snapshot.

        An empty reported ``changed_files`` list is not evidence: profile filters,
        collection failures, and a stale stat cache can all produce that shape.
        This proof compares the synthetic worktree tree and index tree twice, so
        tracked, untracked, deleted, renamed, binary, and mode deltas are covered
        by Git object identity without rewriting user files.
        """
        evidence: dict[str, object] = {
            "schema_version": 1,
            "kind": "VERIFIED_NO_TASK_DELTA",
            "passed": False,
            "raw_task_delta_files": [],
            "task_owned_source_delta_files": [],
            "untracked_new_deleted_renamed_delta_files": [],
            "stable_hash_comparison": False,
            "repositories": [],
            "failure_code": "",
        }
        if baseline is None:
            evidence["failure_code"] = "NO_TASK_DELTA_BASELINE_MISSING"
            return evidence
        all_paths: list[str] = []
        repositories: list[dict[str, object]] = []
        for repo in baseline.repos:
            prefix = f"{repo.module}/" if repo.module else ""
            code, head, error = self._run_in(repo.git_dir, ["rev-parse", "--verify", "HEAD"])
            if code != 0 or head.strip() != repo.head_oid:
                evidence["failure_code"] = "NO_TASK_DELTA_HEAD_MISMATCH"
                evidence["repositories"] = repositories
                return evidence
            ok1, tree1, error1 = self._temporary_worktree_tree(repo.git_dir)
            ok2, tree2, error2 = self._temporary_worktree_tree(repo.git_dir)
            code1, index1, index_error1 = self._run_in(repo.git_dir, ["write-tree"])
            code2, index2, index_error2 = self._run_in(repo.git_dir, ["write-tree"])
            if not ok1 or not ok2 or code1 != 0 or code2 != 0:
                evidence["failure_code"] = "NO_TASK_DELTA_HASH_CAPTURE_FAILED"
                evidence["repositories"] = repositories
                return evidence
            ok_paths, paths, path_error = self._delta_paths(
                repo.git_dir, repo.worktree_tree, tree2
            )
            if not ok_paths:
                evidence["failure_code"] = "NO_TASK_DELTA_COMPARE_FAILED"
                evidence["repositories"] = repositories
                return evidence
            prefixed = [prefix + path for path in paths]
            all_paths.extend(prefixed)
            stable = tree1 == tree2 and index1.strip() == index2.strip()
            matches = (
                tree2 == repo.worktree_tree
                and index2.strip() == repo.index_tree
            )
            repositories.append({
                "module": repo.module or "",
                "head_oid": repo.head_oid,
                "pre_job_worktree_tree": repo.worktree_tree,
                "current_worktree_tree_first": tree1,
                "current_worktree_tree_second": tree2,
                "pre_job_index_tree": repo.index_tree,
                "current_index_tree_first": index1.strip(),
                "current_index_tree_second": index2.strip(),
                "raw_delta_files": prefixed,
                "stable": stable,
                "matches_pre_job": matches,
            })
            if not stable:
                evidence["failure_code"] = "NO_TASK_DELTA_UNSTABLE_HASH"
                evidence["repositories"] = repositories
                evidence["raw_task_delta_files"] = all_paths
                return evidence
            if prefixed or not matches:
                evidence["failure_code"] = "TASK_DELTA_PRESENT"
                evidence["repositories"] = repositories
                evidence["raw_task_delta_files"] = all_paths
                evidence["untracked_new_deleted_renamed_delta_files"] = all_paths
                return evidence
        evidence.update({
            "passed": True,
            "raw_task_delta_files": [],
            "task_owned_source_delta_files": [],
            "untracked_new_deleted_renamed_delta_files": [],
            "stable_hash_comparison": True,
            "repositories": repositories,
            "failure_code": "",
        })
        return evidence

    def collect_selected(
        self,
        task: TaskState,
        baseline: GitTaskBaseline,
        selected_files: list[str],
    ) -> GitDiffResult:
        """Rebuild review/build evidence for only the current Task source paths."""
        selected = list(dict.fromkeys(
            path.replace("\\", "/").strip("/") for path in selected_files if path
        ))
        stats: list[str] = []
        diffs: list[str] = []
        first_error = ""
        success = True
        for repo in baseline.repos:
            prefix = (repo.module or "").replace("\\", "/").strip("/")
            relatives: list[str] = []
            for path in selected:
                if prefix:
                    if path.startswith(prefix + "/"):
                        relatives.append(path[len(prefix) + 1:])
                else:
                    relatives.append(path)
            if not relatives:
                continue
            ok, current_tree, error = self._temporary_worktree_tree(repo.git_dir)
            if not ok:
                success = False
                first_error = first_error or error
                continue
            args = [repo.worktree_tree, current_tree, "--", *relatives]
            code, stat, error = self._run_in(repo.git_dir, ["diff", "--stat", *args])
            if code != 0:
                success = False
                first_error = first_error or (error or stat).strip()
                continue
            code, diff, error = self._run_in(repo.git_dir, ["diff", "--binary", *args])
            if code != 0:
                success = False
                first_error = first_error or (error or diff).strip()
                continue
            stat = stat.strip()
            diff = _trim_diff_transport(diff)
            if prefix and stat:
                stat = f"--- [module: {prefix}] ---\n{stat}"
            if prefix and diff:
                diff = f"--- [module: {prefix}] ---\n{diff}"
            if stat:
                stats.append(stat)
            if diff:
                diffs.append(diff)
        result = GitDiffResult(
            success=success,
            changed_files=selected,
            excluded_files=list(task.excluded_files),
            status_short=f"{len(selected)} task source file(s)" if selected else "",
            diff_stat="\n\n".join(stats).strip(),
            diff=_trim_diff_transport("\n".join(diffs)),
            error=first_error,
        )
        task.changed_files = list(result.changed_files)
        task.task_owned_changed_files = list(result.changed_files)
        task.git_status = result.status_short
        task.git_diff_stat = result.diff_stat
        task.git_diff = result.diff
        return result

    def _repo_path_for_changed(
        self,
        baseline: GitTaskBaseline,
        changed_path: str,
    ) -> tuple[GitRepoBaseline, str] | None:
        normalized = changed_path.replace("\\", "/").strip("/")
        candidates: list[tuple[int, GitRepoBaseline, str]] = []
        for repo in baseline.repos:
            prefix = (repo.module or "").replace("\\", "/").strip("/")
            if prefix:
                if normalized == prefix:
                    relative = ""
                elif normalized.startswith(prefix + "/"):
                    relative = normalized[len(prefix) + 1:]
                else:
                    continue
                candidates.append((len(prefix), repo, relative))
            else:
                candidates.append((0, repo, normalized))
        if not candidates:
            return None
        _, repo, relative = max(candidates, key=lambda item: item[0])
        return repo, relative

    def repair_eol_to_index(
        self,
        task: TaskState,
        baseline: GitTaskBaseline,
    ) -> EolRepairResult:
        """Normalize only Job-changed tracked text files to their Git index EOL.

        This is a byte-preserving line-separator repair: UTF-8 BOM and all
        non-newline bytes are retained. Binary/untracked/unspecified files are
        skipped. The operation is idempotent and never runs Git checkout/reset.
        """
        repaired: list[str] = []
        skipped: list[str] = []
        diagnostics: list[dict[str, object]] = []
        current_file = ""
        operation = ""
        try:
            for changed in list(dict.fromkeys(task.changed_files or [])):
                current_file = changed
                located = self._repo_path_for_changed(baseline, changed)
                if located is None:
                    skipped.append(changed)
                    continue
                repo, relative = located
                if not relative:
                    skipped.append(changed)
                    continue
                code, output, _ = self._run_in(
                    repo.git_dir, ["ls-files", "--eol", "--", relative]
                )
                line = next((item for item in output.splitlines() if item.strip()), "")
                match = re.match(r"^i/(lf|crlf)\s+w/([^\s]+)\s+attr/", line)
                if code != 0 or not match:
                    skipped.append(changed)
                    continue
                index_eol, worktree_eol = match.groups()
                if worktree_eol == index_eol:
                    continue
                file_path = (repo.git_dir / Path(relative)).resolve()
                try:
                    file_path.relative_to(repo.git_dir.resolve())
                except ValueError:
                    return EolRepairResult(False, repaired, skipped, "EOL_PATH_ESCAPE")
                if not file_path.is_file() or file_path.is_symlink():
                    skipped.append(changed)
                    continue
                operation = "read_bytes"
                data = file_path.read_bytes()
                if b"\0" in data:
                    skipped.append(changed)
                    continue
                target = b"\n" if index_eol == "lf" else b"\r\n"
                normalized = re.sub(br"\r\n|\r|\n", target, data)
                if normalized == data:
                    continue
                operation = "stat"
                mode = file_path.stat().st_mode
                operation = "mkstemp"
                fd, temp_name = tempfile.mkstemp(
                    prefix=f".{file_path.name}.eol-", dir=str(file_path.parent)
                )
                try:
                    operation = "write_temp"
                    with os.fdopen(fd, "wb") as handle:
                        handle.write(normalized)
                        handle.flush()
                        os.fsync(handle.fileno())
                    operation = "chmod_temp"
                    os.chmod(temp_name, mode)
                    attempt_limit = 3
                    for replace_attempt in range(1, attempt_limit + 1):
                        operation = "os.replace"
                        try:
                            os.replace(temp_name, file_path)
                            break
                        except OSError as exc:
                            winerror = getattr(exc, "winerror", None)
                            transient = isinstance(exc, PermissionError) or winerror in {5, 32}
                            diagnostics.append({
                                "kind": "EOL_REPAIR_ERROR",
                                "file": changed,
                                "operation": operation,
                                "exception_type": type(exc).__name__,
                                "winerror": winerror,
                                "message": scrub_secrets(str(exc))[:500],
                                "attempt": replace_attempt,
                                "attempt_limit": attempt_limit,
                                "transient": transient,
                            })
                            if not transient or replace_attempt >= attempt_limit:
                                raise
                            time.sleep(0.05 * replace_attempt)
                finally:
                    try:
                        os.unlink(temp_name)
                    except FileNotFoundError:
                        pass
                repaired.append(changed)
            return EolRepairResult(True, repaired, skipped, "", diagnostics)
        except (OSError, ValueError) as exc:
            winerror = getattr(exc, "winerror", None)
            if not diagnostics or diagnostics[-1].get("exception_type") != type(exc).__name__:
                diagnostics.append({
                    "kind": "EOL_REPAIR_ERROR",
                    "file": current_file,
                    "operation": operation,
                    "exception_type": type(exc).__name__,
                    "winerror": winerror,
                    "message": scrub_secrets(str(exc))[:500],
                    "attempt": 1,
                    "attempt_limit": 1,
                    "transient": False,
                })
            return EolRepairResult(
                False, repaired, skipped, f"EOL_REPAIR_ERROR:{type(exc).__name__}", diagnostics
            )

    def inspect_eol_to_index(
        self, task: TaskState, baseline: GitTaskBaseline
    ) -> dict[str, object]:
        """Return read-only EOL proof for tracked Task files."""
        checked: dict[str, str] = {}
        mismatched: list[str] = []
        for changed in list(dict.fromkeys(task.changed_files or [])):
            located = self._repo_path_for_changed(baseline, changed)
            if located is None or not located[1]:
                continue
            repo, relative = located
            code, output, _ = self._run_in(repo.git_dir, ["ls-files", "--eol", "--", relative])
            line = next((item for item in output.splitlines() if item.strip()), "")
            match = re.match(r"^i/(lf|crlf)\s+w/([^\s]+)\s+attr/", line)
            if code != 0 or not match:
                continue
            index_eol, worktree_eol = match.groups()
            checked[changed] = f"i/{index_eol} w/{worktree_eol}"
            if index_eol != worktree_eol:
                mismatched.append(changed)
        return {"success": not mismatched, "checked": checked, "mismatched": mismatched}

    def assess_commit_safety(
        self,
        task: TaskState,
        baseline: GitTaskBaseline,
    ) -> tuple[bool, list[str], dict[str, str]]:
        """Prove Job paths are safe and hash their post-images.

        Under ``BATCH_FINAL_COMMIT`` the baseline is intentionally cumulative:
        the changed-file set is already measured from the pre-Job tree, so a
        same-path delta inherited from an earlier successful Job is not a blocker.
        """
        blockers: list[str] = []
        hashes: dict[str, str] = {}
        control_policy = getattr(self.profile, "control_policy", None)
        cumulative = bool(
            control_policy
            and control_policy.cumulative_worktree
            and baseline.snapshot_manifest
        )
        for changed in list(dict.fromkeys(task.changed_files or [])):
            located = self._repo_path_for_changed(baseline, changed)
            if located is None:
                blockers.append(f"UNMAPPED_PATH:{changed}")
                continue
            repo, relative = located
            if not relative:
                blockers.append(f"INVALID_PATH:{changed}")
                continue
            code, _, _ = self._run_in(
                repo.git_dir,
                ["diff", "--quiet", "HEAD", repo.worktree_tree, "--", relative],
            )
            if code == 1 and not cumulative:
                blockers.append(f"PREEXISTING_DELTA:{changed}")
            elif code != 0 and not cumulative:
                blockers.append(f"BASELINE_CHECK_ERROR:{changed}")
            file_path = (repo.git_dir / Path(relative)).resolve()
            try:
                file_path.relative_to(repo.git_dir.resolve())
            except ValueError:
                blockers.append(f"PATH_ESCAPE:{changed}")
                continue
            if file_path.is_file() and not file_path.is_symlink():
                try:
                    hashes[changed] = hashlib.sha256(file_path.read_bytes()).hexdigest()
                except OSError:
                    blockers.append(f"HASH_ERROR:{changed}")
            elif file_path.exists():
                blockers.append(f"UNSUPPORTED_FILE_TYPE:{changed}")
            else:
                # Deleted files are commit-safe; the explicit marker is verified
                # again by the committer before creating the commit.
                hashes[changed] = "DELETED"
        return not blockers, blockers, hashes

    def rollback(
        self,
        task: TaskState | None = None,
        baseline: GitTaskBaseline | None = None,
    ) -> tuple[bool, str]:
        """
        작업 기준점 기반의 모듈별 롤백.
        - task.target_module 리스트가 있으면 각 모듈 디렉토리에서 개별 롤백.
        - 없으면 working_dir 루트에서 롤백.
        - 작업이 만든 delta만 작업 시작 기준점으로 원복한다.
        - profile의 로컬 개발 설정 제외 목록은 원복에서 제외.
        - 기준점에 이미 있던 신규 파일은 보존하고, 이 작업이 만든 신규 파일만 삭제한다.
        - git clean 및 디렉토리 재귀 삭제는 사용하지 않는다.
        """
        if baseline is None:
            return False, "작업 기준점 없음 - 안전을 위해 롤백하지 않음"

        all_notes: list[str] = []
        any_fail = False
        unrelated_drift: list[str] = []

        snapshots = self._snapshot_file_records(baseline)
        persistent_cumulative = bool(
            baseline.snapshot_manifest
            and self.profile
            and getattr(self.profile, "control_policy", None)
            and self.profile.control_policy.cumulative_worktree
        )
        explicit_task_targets = [
            str(path).replace("\\", "/").strip("/")
            for path in list(
                (
                    task.scoped_rollback_targets
                    or task.task_owned_changed_files
                    if task is not None
                    else []
                )
                or []
            )
        ]
        for repo in baseline.repos:
            prefix = f"{repo.module}/" if repo.module else ""
            repo_targets = [
                path for path in explicit_task_targets
                if (prefix and path.startswith(prefix)) or (not prefix)
            ]
            if explicit_task_targets and not repo_targets:
                code, head, _ = self._run_in(
                    repo.git_dir, ["rev-parse", "--verify", "HEAD"]
                )
                if code != 0 or head.strip() != repo.head_oid:
                    unrelated_drift.append(repo.module or repo.git_dir.name)
                    all_notes.append(
                        f"[{repo.git_dir.name}] unrelated repository drift preserved"
                    )
                continue
            note, ok = self._rollback_one(
                repo,
                snapshots,
                persistent_cumulative,
                allowed_paths=(
                    [
                        path[len(prefix):]
                        if prefix and path.startswith(prefix)
                        else path
                        for path in repo_targets
                    ]
                    if explicit_task_targets
                    else None
                ),
            )
            all_notes.append(f"[{repo.git_dir.name}] {note}")
            if not ok:
                any_fail = True

        all_notes.append("다른 Job/기존 변경 보존됨 (작업 delta만 원복, clean 미실행)")
        excludes = self.profile.local_dev_config_excludes if self.profile else ()
        if excludes:
            all_notes.append(
                f"롤백 제외(로컬 개발 설정 보존): {', '.join(excludes)}"
            )

        # ===== State 매핑 =====
        if task is not None:
            task.is_rolled_back = not any_fail
            task.rollback_scope_result = {
                "task_owned_rollback": "PASS" if not any_fail else "FAIL",
                "inherited_delta_preservation": "PRESERVED",
                "external_frozen_integrity": bool(
                    getattr(task, "external_frozen_integrity", True)
                ),
                "unrelated_repository_drift": unrelated_drift,
            }

        return (not any_fail), " | ".join(all_notes)

    def _tree_has_path(self, git_dir: Path, tree: str, path: str) -> bool:
        code, _, _ = self._run_in(git_dir, ["cat-file", "-e", f"{tree}:{path}"])
        return code == 0

    @staticmethod
    def _safe_repo_path(git_dir: Path, path: str) -> Path | None:
        candidate = (git_dir / Path(path)).resolve()
        try:
            candidate.relative_to(git_dir.resolve())
        except ValueError:
            return None
        return candidate

    def _restore_index_path(
        self, baseline: GitRepoBaseline, path: str
    ) -> tuple[bool, str]:
        if self._tree_has_path(baseline.git_dir, baseline.index_tree, path):
            code, out, err = self._run_in(
                baseline.git_dir,
                ["restore", f"--source={baseline.index_tree}", "--staged", "--", path],
            )
        else:
            code, out, err = self._run_in(
                baseline.git_dir,
                ["rm", "--cached", "-f", "--ignore-unmatch", "--", path],
            )
        return code == 0, (err or out).strip()

    def _snapshot_file_records(
        self, baseline: GitTaskBaseline
    ) -> dict[str, tuple[Path | None, str]]:
        manifest_path = baseline.snapshot_manifest
        if manifest_path is None:
            return {}
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        records: dict[str, tuple[Path | None, str]] = {}
        for item in manifest.get("files") or []:
            path = str(item.get("path", "")).replace("\\", "/").strip("/")
            if not path:
                continue
            if item.get("state") == "DELETED":
                records[path] = (None, "DELETED")
                continue
            relative = str(item.get("snapshot", ""))
            snapshot = (manifest_path.parent / Path(relative)).resolve()
            try:
                snapshot.relative_to(manifest_path.parent.resolve())
            except ValueError:
                continue
            records[path] = (snapshot, str(item.get("sha256", "")))
        return records

    def _read_tree_path_bytes(
        self, baseline: GitRepoBaseline, path: str
    ) -> tuple[bytes | None, str]:
        try:
            result = subprocess.run(
                [
                    trusted_executable("git", forbidden_root=self.working_dir),
                    "show",
                    f"{baseline.worktree_tree}:{path}",
                ],
                cwd=str(baseline.git_dir),
                capture_output=True,
                env=git_subprocess_env(),
            )
        except Exception as exc:
            return None, scrub_secrets(str(exc))
        if result.returncode != 0:
            return None, result.stderr.decode("utf-8", errors="replace").strip()
        return result.stdout, ""

    def _restore_worktree_path(
        self,
        baseline: GitRepoBaseline,
        path: str,
        snapshots: dict[str, tuple[Path | None, str]],
    ) -> tuple[bool, str]:
        target = self._safe_repo_path(baseline.git_dir, path)
        if target is None:
            return False, f"저장소 밖 경로 감지: {path}"
        if target.exists() and (target.is_dir() or target.is_symlink()):
            return False, f"지원하지 않는 복원 대상({path})"
        workspace_path = (
            f"{baseline.module}/{path}" if baseline.module else path
        ).replace("\\", "/").strip("/")
        payload: bytes | None = None
        snapshot = snapshots.get(workspace_path)
        if snapshot and snapshot[0] is not None:
            try:
                payload = snapshot[0].read_bytes()
            except OSError as exc:
                return False, f"snapshot 읽기 실패({path}): {type(exc).__name__}"
            if snapshot[1] and hashlib.sha256(payload).hexdigest() != snapshot[1]:
                return False, f"snapshot hash 불일치({path})"
        if payload is None:
            payload, error = self._read_tree_path_bytes(baseline, path)
            if payload is None:
                return False, f"snapshot/tree 내용 읽기 실패({path}): {error}"
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            fd, temp_name = tempfile.mkstemp(prefix=f".{target.name}.rollback-", dir=str(target.parent))
            try:
                with os.fdopen(fd, "wb") as handle:
                    handle.write(payload)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temp_name, target)
            finally:
                try:
                    os.unlink(temp_name)
                except FileNotFoundError:
                    pass
        except OSError as exc:
            return False, f"snapshot 복원 실패({path}): {type(exc).__name__}"
        return True, ""

    def _rollback_one(
        self,
        baseline: GitRepoBaseline,
        snapshots: dict[str, tuple[Path | None, str]],
        persistent_cumulative: bool,
        allowed_paths: list[str] | None = None,
    ) -> tuple[str, bool]:
        """
        단일 저장소의 작업 delta를 시작 기준점으로 되돌린다.
        LOCAL_DEV_CONFIG_EXCLUDES는 원복에서 제외한다.
        """
        git_dir = baseline.git_dir
        code, head, _ = self._run_in(git_dir, ["rev-parse", "--verify", "HEAD"])
        current_head = head.strip() if code == 0 else ""
        if current_head != baseline.head_oid:
            return ("작업 중 HEAD 변경 감지 - 롤백 거부", False)
        ok, current_tree, error = self._temporary_worktree_tree(git_dir)
        if not ok:
            return (f"현재 tree 생성 실패: {error}", False)
        ok, paths, error = self._delta_paths(
            git_dir, baseline.worktree_tree, current_tree
        )
        if not ok:
            return (f"작업 delta 계산 실패: {error}", False)
        if allowed_paths is not None:
            allowed = {
                str(path).replace("\\", "/").strip("/") for path in allowed_paths
            }
            paths = [path for path in paths if path in allowed]
        if not persistent_cumulative:
            paths = [
                path for path in paths
                if not (self.profile and self.profile.is_local_dev_config(path))
            ]

        created_paths = [
            path
            for path in paths
            if not self._tree_has_path(git_dir, baseline.worktree_tree, path)
        ]
        existing_paths = [path for path in paths if path not in created_paths]

        # 작업이 만든 Git 경로부터 깊은 순서로 제거한다. ignored 파일이나 다른
        # 파일이 남은 디렉토리는 재귀 삭제하지 않고 안전하게 롤백을 거부한다.
        for path in sorted(
            created_paths, key=lambda value: len(Path(value).parts), reverse=True
        ):
            target = self._safe_repo_path(git_dir, path)
            if target is None:
                return (f"저장소 밖 경로 감지: {path}", False)
            if target.exists() or target.is_symlink():
                if target.is_dir() and not target.is_symlink():
                    return (f"디렉토리 재귀 삭제 거부({path})", False)
                target.unlink()
            index_ok, index_error = self._restore_index_path(baseline, path)
            if not index_ok:
                return (f"index 복원 실패({path}): {index_error}", False)

        for path in sorted(existing_paths):
            target = self._safe_repo_path(git_dir, path)
            if target is None:
                return (f"저장소 밖 경로 감지: {path}", False)
            restored, restore_error = self._restore_worktree_path(
                baseline, path, snapshots
            )
            if not restored:
                return (restore_error, False)
            index_ok, index_error = self._restore_index_path(baseline, path)
            if not index_ok:
                return (f"index 복원 실패({path}): {index_error}", False)
        if persistent_cumulative:
            ok, restored_tree, error = self._temporary_worktree_tree(git_dir)
            if not ok or restored_tree != baseline.worktree_tree:
                return (
                    f"rollback 후 worktree hash 불일치: {error or restored_tree}",
                    False,
                )
        code, restored_index, error = self._run_in(git_dir, ["write-tree"])
        if code != 0 or restored_index.strip() != baseline.index_tree:
            return (
                f"rollback 후 index hash 불일치: {(error or restored_index).strip()}",
                False,
            )
        return (f"작업 변경 {len(paths)}개 경로를 시작 기준점으로 원복", True)
