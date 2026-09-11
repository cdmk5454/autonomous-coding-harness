"""Safe, path-scoped Git commits for verified queue Job results."""

from __future__ import annotations

import hashlib
import re
import subprocess
from pathlib import Path
from typing import Any, Mapping

from project_profile import ProjectProfile, normalize_changed_path
from runtime_safety import git_subprocess_env, scrub_secrets, trusted_executable


class GitCommitError(RuntimeError):
    def __init__(self, code: str, detail: str = ""):
        self.code = code
        self.detail = scrub_secrets(detail)[:500]
        super().__init__(code + (f": {self.detail}" if self.detail else ""))


class JobCommitter:
    """Commit exactly the files attributed to one verified Job, never push."""

    def __init__(self, working_dir: str | Path, profile: ProjectProfile):
        self.working_dir = Path(working_dir).resolve()
        self.profile = profile

    def _run(self, repo: Path, args: list[str]) -> tuple[int, str, str]:
        result = subprocess.run(
            [trusted_executable("git", forbidden_root=self.working_dir)] + args,
            cwd=str(repo),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=git_subprocess_env(),
        )
        return result.returncode, result.stdout, result.stderr

    def _locate(self, changed: str) -> tuple[Path, str]:
        normalized = normalize_changed_path(changed)
        if not normalized:
            raise GitCommitError("COMMIT_PATH_INVALID")
        if (self.working_dir / ".git").exists():
            return self.working_dir, normalized
        module = self.profile.module_for_path(normalized)
        if module is None:
            raise GitCommitError("COMMIT_PATH_OUTSIDE_PROFILE", normalized)
        prefix = module.path.rstrip("/")
        relative = normalized[len(prefix):].lstrip("/")
        repo = (self.working_dir / Path(*prefix.split("/"))).resolve()
        if not relative or not (repo / ".git").exists():
            raise GitCommitError("COMMIT_REPOSITORY_NOT_FOUND", normalized)
        return repo, relative

    @staticmethod
    def _message(job: Mapping[str, Any], files: list[str]) -> tuple[str, str]:
        request = dict(job.get("request") or {})
        summary = str(request.get("commit_summary") or "").strip()
        requirement = str(request.get("requirement") or job.get("current_requirement") or "").strip()
        source = summary or requirement or str(job.get("client_job_id") or "작업 결과 반영")
        first = next((part.strip() for part in re.split(r"[\r\n。]", source) if part.strip()), "작업 결과 반영")
        title = first[:68].rstrip(" .") or "작업 결과 반영"
        description = summary or requirement
        if len(description) > 500:
            description = description[:497].rstrip() + "..."
        file_block = "\n".join(f"- {path}" for path in files)
        body = (description + "\n\n" if description else "") + "변경 파일:\n" + file_block
        return title, body

    def commit(self, job: Mapping[str, Any]) -> dict[str, Any]:
        if str(job.get("status")) != "SUCCEEDED":
            raise GitCommitError("JOB_NOT_SUCCEEDED")
        result = dict(job.get("last_result") or {})
        if not (
            result.get("success") is True
            and result.get("verification_status") == "VERIFIED"
            and result.get("build_status") == "PASS"
            and result.get("review_status") == "REVIEW_PASS"
        ):
            raise GitCommitError("JOB_NOT_FULLY_VERIFIED")
        changed = list(dict.fromkeys(
            normalize_changed_path(path)
            for path in (result.get("changed_files") or [])
            if normalize_changed_path(path)
        ))
        if not changed:
            return {"status": "NO_CHANGES", "commits": [], "changed_files": []}
        if not bool(result.get("commit_eligible")):
            blockers = ",".join(str(item) for item in result.get("commit_blockers") or [])
            raise GitCommitError("JOB_COMMIT_NOT_ELIGIBLE", blockers)
        expected_hashes = dict(result.get("changed_file_sha256") or {})

        groups: dict[Path, list[tuple[str, str]]] = {}
        for workspace_path in changed:
            repo, relative = self._locate(workspace_path)
            groups.setdefault(repo, []).append((workspace_path, relative))

        prepared: list[tuple[Path, list[tuple[str, str]], str, str, str]] = []
        for repo, entries in groups.items():
            workspace_paths = [item[0] for item in entries]
            relative_paths = [item[1] for item in entries]
            for workspace_path, relative in entries:
                path = (repo / Path(relative)).resolve()
                try:
                    path.relative_to(repo)
                except ValueError as exc:
                    raise GitCommitError("COMMIT_PATH_ESCAPE", workspace_path) from exc
                expected = str(expected_hashes.get(workspace_path, ""))
                actual = (
                    hashlib.sha256(path.read_bytes()).hexdigest()
                    if path.is_file() and not path.is_symlink()
                    else "DELETED" if not path.exists() else "UNSUPPORTED"
                )
                if not expected or actual != expected:
                    raise GitCommitError("JOB_POSTIMAGE_CHANGED", workspace_path)

            for marker in ("MERGE_HEAD", "CHERRY_PICK_HEAD", "REVERT_HEAD"):
                code, _, _ = self._run(repo, ["rev-parse", "-q", "--verify", marker])
                if code == 0:
                    raise GitCommitError("GIT_OPERATION_IN_PROGRESS", marker)
            git_dir_code, git_dir_out, git_dir_err = self._run(
                repo, ["rev-parse", "--absolute-git-dir"]
            )
            if git_dir_code != 0:
                raise GitCommitError("GIT_DIRECTORY_UNAVAILABLE", git_dir_err)
            git_dir = Path(git_dir_out.strip())
            for marker_dir in ("rebase-apply", "rebase-merge"):
                if (git_dir / marker_dir).exists():
                    raise GitCommitError("GIT_OPERATION_IN_PROGRESS", marker_dir)
            code, out, err = self._run(
                repo,
                ["-c", "core.whitespace=cr-at-eol", "diff", "--check", "--"] + relative_paths,
            )
            if code != 0:
                raise GitCommitError("COMMIT_DIFF_CHECK_FAILED", err or out)
            code, out, err = self._run(repo, ["status", "--porcelain=v1", "--"] + relative_paths)
            title, body = self._message(job, workspace_paths)
            if code != 0:
                raise GitCommitError("COMMIT_STATUS_FAILED", err)
            existing_commit = ""
            if not out.strip():
                # Recovery boundary: the process may have created the exact
                # commit and died before persisting the queue receipt. Accept
                # only when HEAD changed precisely this repository's Job paths;
                # a later/unrelated commit remains fail-closed.
                code, head_paths, head_err = self._run(
                    repo,
                    ["diff-tree", "--no-commit-id", "--name-only", "-r", "HEAD"],
                )
                actual_paths = {
                    item.replace("\\", "/").strip()
                    for item in head_paths.splitlines()
                    if item.strip()
                }
                if code != 0 or actual_paths != set(relative_paths):
                    raise GitCommitError(
                        "COMMIT_PATHS_NOT_DIRTY",
                        head_err or ",".join(workspace_paths),
                    )
                code, existing_commit, head_err = self._run(repo, ["rev-parse", "HEAD"])
                if code != 0:
                    raise GitCommitError("COMMIT_HASH_UNAVAILABLE", head_err)
                existing_commit = existing_commit.strip()
            prepared.append((repo, entries, title, body, existing_commit))

        # Preflight every repository before creating the first commit. This does
        # not make multiple repositories transactional, but it prevents a known
        # failure in a later repository from leaving a needless partial batch.
        commits: list[dict[str, Any]] = []
        for repo, entries, title, body, existing_commit in prepared:
            workspace_paths = [item[0] for item in entries]
            relative_paths = [item[1] for item in entries]
            if existing_commit:
                commits.append({
                    "repository": repo.name,
                    "commit": existing_commit,
                    "changed_files": workspace_paths,
                    "recovered": True,
                })
                continue
            # Stage only these paths so new files are known to Git. ``commit
            # --only`` prevents unrelated pre-staged files from entering the Job
            # commit and leaves their index state untouched.
            code, out, err = self._run(repo, ["add", "-A", "--"] + relative_paths)
            if code != 0:
                raise GitCommitError("COMMIT_STAGE_FAILED", err or out)
            code, out, err = self._run(
                repo,
                ["commit", "--only", "-m", title, "-m", body, "--"] + relative_paths,
            )
            if code != 0:
                raise GitCommitError("COMMIT_CREATE_FAILED", err or out)
            code, commit_hash, err = self._run(repo, ["rev-parse", "HEAD"])
            if code != 0:
                raise GitCommitError("COMMIT_HASH_UNAVAILABLE", err)
            code, remaining, err = self._run(
                repo, ["status", "--porcelain=v1", "--"] + relative_paths
            )
            if code != 0 or remaining.strip():
                raise GitCommitError("COMMIT_POSTCHECK_FAILED", err or remaining)
            commits.append({
                "repository": repo.name,
                "commit": commit_hash.strip(),
                "changed_files": workspace_paths,
            })
        return {"status": "COMMITTED", "commits": commits, "changed_files": changed}
