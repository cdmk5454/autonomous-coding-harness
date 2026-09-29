"""
Build / Test Runner
- MVP: 컴파일/빌드 성공 여부만 확인
- Maven / Gradle 자동 감지
- 멀티모듈 워크스페이스: 루트에 빌드 파일이 없으면 changed_files 에서
  profile 모듈을 추출해 각 모듈 디렉토리에서 빌드한다.
  (18차: 루트만 보다가 cap/pom.xml 빌드를 SKIPPED 오판한 사고 수정)
"""

from __future__ import annotations

import subprocess
import json
import hashlib
import os
import re
import tempfile
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
from project_profile import ProjectProfile
from runtime_safety import (
    assert_secret_free_argv, isolated_subprocess_env, scrub_secrets,
    trusted_executable,
)

@dataclass
class BuildTestResult:
    """
    build_status: PASS | FAIL | SKIPPED | WAITING
      - PASS    : 빌드 실행 후 성공
      - FAIL    : 빌드 실행 후 실패 (무조건 실패)
      - SKIPPED : 빌드 시스템 없음 등으로 실행 안 함
                  (MODIFICATION 작업은 SKIPPED로 성공 불가 — Manager 게이트가 판정)
    success: build_status == "PASS" 와 일치. SKIPPED 는 success=False.
    """
    success: bool
    build_status: str = "WAITING"      # PASS | FAIL | SKIPPED | WAITING
    test_status: str = "WAITING"
    build_output: str = ""
    test_output: str = ""
    error: str = ""
    details: dict[str, Any] = field(default_factory=dict)


class BuildTestRunner:

    # F06: the file names this runner itself treats as build-system control
    # surfaces (maven/node/gradle detection set + wrappers/locks + python
    # packaging).  Derived from the runner's own detection logic, not a
    # universal guess.  A candidate that modifies one of these without an
    # explicit elevated host-build authorization fails closed BEFORE any host
    # build execution.
    BUILD_CONTROL_NAMES = frozenset({
        "pom.xml", "package.json", "package-lock.json", "yarn.lock",
        "pnpm-lock.yaml", "build.gradle", "build.gradle.kts",
        "settings.gradle", "settings.gradle.kts", "gradle-wrapper.properties",
        "mvnw", "mvnw.cmd", "gradlew", "gradlew.bat", "gradle.properties",
        "maven.config", ".mvn", "requirements.txt", "pyproject.toml",
        "setup.py", "setup.cfg", "tox.ini",
    })

    def __init__(
        self,
        working_dir: str,
        timeout: int = 600,
        profile: ProjectProfile | None = None,
        evidence_root: str | Path | None = None,
    ):
        self.working_dir = Path(working_dir).resolve()
        self.timeout = timeout
        self.profile = profile
        self.evidence_root = Path(evidence_root).resolve() if evidence_root else None
        self._last_execution: dict[str, Any] = {}
        self._evidence_context: dict[str, str] = {}

    @classmethod
    def build_control_changes(cls, changed_files) -> list[str]:
        """Changed paths that alter the host build-control surface."""
        flagged: list[str] = []
        for raw in changed_files or []:
            path = str(raw).replace("\\", "/").strip("/")
            parts = path.split("/")
            if path in cls.BUILD_CONTROL_NAMES or parts[-1] in cls.BUILD_CONTROL_NAMES \
                    or ".mvn/" in path or "gradle/wrapper/" in path:
                flagged.append(path)
        return sorted(set(flagged))

    def _protected_surface_snapshot(self, root: Path | None = None) -> dict[str, Any]:
        """Full-set manifest of the protected host surface (F14 §22).

        Host build/test execution is NOT container-isolated (explicit trust
        boundary); this DETECTS — not prevents — add/modify/delete of Harness
        source during a build.  ``root`` defaults to the Harness source tree;
        tests may point it at a disposable root.  Control/.tasks state stays
        excluded: the Harness itself writes there concurrently.
        """
        module_root = Path(root) if root else Path(__file__).resolve().parent
        entries = {item.name: hashlib.sha256(item.read_bytes()).hexdigest()
                   for item in sorted(module_root.glob("*.py"))}
        version_file = module_root / "VERSION"
        if version_file.is_file():
            entries["VERSION"] = hashlib.sha256(version_file.read_bytes()).hexdigest()
        return {"root": str(module_root), "files": entries}

    @staticmethod
    def compare_protected_surfaces(before: dict[str, Any],
                                   after: dict[str, Any]) -> dict[str, list[str]]:
        """Full set diff: modified / deleted / added protected files."""
        before_files = dict(before.get("files") or {})
        after_files = dict(after.get("files") or {})
        return {
            "modified": sorted(name for name in before_files.keys() & after_files.keys()
                               if before_files[name] != after_files[name]),
            "deleted": sorted(before_files.keys() - after_files.keys()),
            "added": sorted(after_files.keys() - before_files.keys()),
        }

    def _build_control_closure(self, targets) -> list[str]:
        """Execution closure of the build-control surface (F14 §20).

        Beyond the declared control filenames, every project-local file the
        frozen profile command actually executes (e.g. ``python app/build.py``)
        is part of the surface.  Derived from the selected targets' argv, not
        from a generic list.
        """
        closure: set[str] = set()
        workspace_root = self.working_dir.resolve()
        for _module, mod_dir, argv in targets:
            if not argv:
                continue
            for token in argv[1:]:
                text = str(token).replace("\\", "/").strip()
                if not text or text.startswith("-"):
                    continue
                # argv tokens are relative to the module cwd (how _run
                # executes them); also try the workspace root.
                for base in (Path(mod_dir).resolve(), workspace_root):
                    candidate = (base / text).resolve()
                    try:
                        relative = candidate.relative_to(workspace_root)
                    except ValueError:
                        continue
                    if candidate.is_file():
                        closure.add(relative.as_posix())
                        break
        return sorted(closure)

    def _run(
        self,
        cmd: list[str],
        cwd: Path | None = None,
        *,
        allow_project_executable: bool = False,
    ) -> tuple[int, str, str]:
        started_at = datetime.now().astimezone().isoformat()
        actual_cwd = (cwd or self.working_dir).resolve()
        safe_cmd = list(cmd)
        try:
            child_env = isolated_subprocess_env("build")
            assert_secret_free_argv(safe_cmd, env=child_env)
            safe_cmd[0] = trusted_executable(
                safe_cmd[0],
                forbidden_root=self.working_dir,
                allow_project_absolute=allow_project_executable,
            )
            result = subprocess.run(
                safe_cmd,
                cwd=str(actual_cwd),
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=self.timeout,
                env=child_env,
            )
            code, out, err = (
                result.returncode,
                scrub_secrets(result.stdout),
                scrub_secrets(result.stderr),
            )
        except subprocess.TimeoutExpired as e:
            code, out, err = (
                -1,
                scrub_secrets(e.stdout or ""),
                scrub_secrets(f"Timeout after {self.timeout}s\n{e.stderr or ''}"),
            )
        except FileNotFoundError:
            code, out, err = -1, "", f"명령어를 찾을 수 없음: {cmd[0]}"
        except Exception as e:
            code, out, err = -1, "", scrub_secrets(e)
        finished_at = datetime.now().astimezone().isoformat()
        combined = (out or "") + ("\n" + err if err else "")
        product_execution_id = str(self._evidence_context.get("execution_id", ""))
        self._last_execution = {
            **self._evidence_context,
            "execution_id": "BUILD-" + uuid.uuid4().hex.upper(),
            "product_execution_id": product_execution_id,
            "cwd": str(actual_cwd),
            "argv": [str(item) for item in safe_cmd],
            "started_at": started_at,
            "finished_at": finished_at,
            "exit_code": int(code),
            "status": "PASS" if code == 0 else "FAIL",
            "log_sha256": hashlib.sha256(combined.encode("utf-8")).hexdigest(),
        }
        return code, out, err

    def _execution_details(
        self,
        module: str,
        command_source: str,
        output: str,
    ) -> dict[str, Any]:
        evidence = dict(self._last_execution)
        evidence.update({"module": module, "command_source": command_source})
        payload = output.encode("utf-8")
        evidence["log_sha256"] = hashlib.sha256(payload).hexdigest()
        evidence["log_path"] = ""
        if self.evidence_root is not None:
            safe_task = re.sub(
                r"[^A-Za-z0-9_.-]+", "_", self._evidence_context.get("task_id", "TASK")
            )
            destination = (
                self.evidence_root / "build-evidence" / safe_task /
                f"{evidence.get('execution_id', 'BUILD')}.log"
            )
            destination.parent.mkdir(parents=True, exist_ok=True)
            fd, name = tempfile.mkstemp(prefix=".build-", dir=str(destination.parent))
            try:
                with os.fdopen(fd, "wb") as handle:
                    handle.write(payload)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(name, destination)
            finally:
                try:
                    os.unlink(name)
                except FileNotFoundError:
                    pass
            evidence["log_path"] = str(destination)
        return evidence

    # ── 모듈 탐지 (멀티모듈 워크스페이스) ──────────────────────

    @staticmethod
    def _module_of(changed_file: str) -> str:
        """변경 파일 경로에서 워크스페이스 모듈명 추출.
        'api/src/main/java/...' → 'api', 'web/src/pages/...' → 'web'.
        알 수 없으면 ''(무시).
        """
        return (changed_file or "").replace("\\", "/").strip("/").split("/", 1)[0]

    def _detect_build_targets(
        self,
        changed_files: list[str] | None = None,
        target_modules: list[str] | None = None,
        authoritative_scope: bool = False,
    ) -> list[tuple[str, Path, tuple[str, ...]]]:
        """빌드할 (모듈명, 디렉토리) 목록 결정.
        1) 루트에 pom.xml/build.gradle → [("root", 루트)] (기존 동작 유지)
        2) changed_files 의 모듈 중 빌드 스크립트(pom.xml/build.gradle)를
           가진 것 → 각 모듈 디렉토리에서 빌드.
           프론트 모듈은 package.json으로 확인하며,
           프론트 전용 변경의 빌드 면책은 Manager 의 frontend-only SKIPPED
           허용 규칙이 그대로 담당한다.
        3) 둘 다 없으면 [] (SKIPPED)
        """
        if (self.working_dir / "pom.xml").exists() or \
           (self.working_dir / "build.gradle").exists() or \
           (self.working_dir / "build.gradle.kts").exists() or \
           (self.working_dir / "package.json").exists():
            return [("root", self.working_dir, ())]

        targets: list[tuple[str, Path, tuple[str, ...]]] = []
        seen: set[str] = set()
        candidates: list[tuple[str, bool]] = [
            (name, True) for name in (target_modules or [])
        ]
        if not authoritative_scope or not target_modules:
            candidates.extend((path, False) for path in (changed_files or []))
        for candidate, is_module_name in candidates:
            if self.profile:
                matched = next(
                    (
                        item
                        for item in self.profile.modules
                        if is_module_name
                        and item.name.casefold() == candidate.casefold()
                    ),
                    None,
                )
                if matched is None:
                    matched = self.profile.module_for_path(candidate)
                mod = matched.name if matched else ""
            else:
                mod = candidate if is_module_name else self._module_of(candidate)
            if not mod or mod in seen:
                continue
            module_profile = next(
                (item for item in self.profile.modules if item.name == mod), None
            ) if self.profile else None
            module_path = module_profile.path if module_profile else mod
            mod_dir = self.working_dir / Path(*module_path.split("/"))
            if module_profile and module_profile.build_argv:
                explicit_dir = self.working_dir / Path(*module_profile.build_cwd.split("/"))
                seen.add(mod)
                targets.append((mod, explicit_dir, module_profile.build_argv))
                continue
            if not mod_dir.is_dir():
                continue
            if (mod_dir / "pom.xml").exists() or \
               (mod_dir / "build.gradle").exists() or \
               (mod_dir / "build.gradle.kts").exists():
                seen.add(mod)
                targets.append((mod, mod_dir, ()))
            elif (mod_dir / "package.json").is_file():
                seen.add(mod)
                targets.append((mod, mod_dir, ()))
        return targets

    def _run_explicit(
        self, module: str, mod_dir: Path, argv: tuple[str, ...]
    ) -> BuildTestResult:
        code, out, err = self._run(list(argv), cwd=mod_dir)
        output = (out or "") + ("\n" + err if err else "")
        execution = self._execution_details(module, "profile", output)
        return BuildTestResult(
            success=code == 0,
            build_status="PASS" if code == 0 else "FAIL",
            test_status="SKIPPED",
            build_output=output[-4000:],
            error="" if code == 0 else f"[{module}] explicit build failed",
            details={"tool": "profile", "module": module, "argv": list(argv), "exit_code": code,
                     "executions": [execution]},
        )

    def _run_node(self, module: str, mod_dir: Path) -> BuildTestResult:
        try:
            package = json.loads((mod_dir / "package.json").read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            return BuildTestResult(
                success=False,
                build_status="FAIL",
                test_status="SKIPPED",
                error=f"[{module}] package.json parse failed: {type(exc).__name__}",
                details={"tool": "node", "module": module},
            )
        scripts = package.get("scripts", {}) if isinstance(package, dict) else {}
        if not isinstance(scripts, dict) or not scripts.get("build"):
            return BuildTestResult(
                success=False,
                build_status="SKIPPED",
                test_status="SKIPPED",
                details={"tool": "node", "module": module, "reason": "package.json build script 없음"},
            )
        npm = "npm.cmd" if __import__("os").name == "nt" else "npm"
        code, out, err = self._run([npm, "run", "build"], cwd=mod_dir)
        output = (out or "") + ("\n" + err if err else "")
        execution = self._execution_details(module, "package.json", output)
        return BuildTestResult(
            success=code == 0,
            build_status="PASS" if code == 0 else "FAIL",
            test_status="SKIPPED",
            build_output=output[-4000:],
            error="" if code == 0 else f"[{module}] npm build failed",
            details={"tool": "node", "module": module, "exit_code": code,
                     "executions": [execution]},
        )

    def _run_maven(self, module: str, mod_dir: Path) -> BuildTestResult:
        """단일 Maven 모듈 컴파일 (MVP: compile 만)."""
        # Windows면 mvn.cmd 우선 시도
        mvn_cmd = "mvn.cmd"
        code_check, _, _ = self._run([mvn_cmd, "-v"])
        if code_check != 0:
            mvn_cmd = "mvn"

        # MVP: 컴파일만 확인 (테스트 스킵)
        cmd = [mvn_cmd, "-q", "compile", "-DskipTests"]
        code, out, err = self._run(cmd, cwd=mod_dir)
        output = (out or "") + ("\n" + err if err else "")
        execution = self._execution_details(module, "autodetect:maven", output)
        label = f"[{module}] " if module != "root" else ""
        if code == 0:
            return BuildTestResult(
                success=True,
                build_status="PASS",
                test_status="SKIPPED",
                build_output=(label + output)[-4000:],  # 너무 길면 뒤쪽만
                details={"tool": "maven", "module": module,
                         "command": f"cd {mod_dir} && " + " ".join(cmd),
                         "executions": [execution]},
            )
        return BuildTestResult(
            success=False,
            build_status="FAIL",
            test_status="SKIPPED",
            build_output=(label + output)[-4000:],
            error=f"{label}maven compile failed",
            details={"tool": "maven", "module": module,
                     "command": f"cd {mod_dir} && " + " ".join(cmd),
                     "exit_code": code, "executions": [execution]},
        )

    def _run_gradle(self, module: str, mod_dir: Path) -> BuildTestResult:
        """단일 Gradle 모듈 컴파일."""
        gradlew = mod_dir / "gradlew.bat"
        if gradlew.exists():
            cmd = [str(gradlew.resolve()), "compileJava", "-x", "test"]
        else:
            cmd = ["gradle", "compileJava", "-x", "test"]

        code, out, err = self._run(
            cmd,
            cwd=mod_dir,
            allow_project_executable=gradlew.exists(),
        )
        output = (out or "") + ("\n" + err if err else "")
        execution = self._execution_details(module, "autodetect:gradle", output)
        label = f"[{module}] " if module != "root" else ""
        if code == 0:
            return BuildTestResult(
                success=True,
                build_status="PASS",
                test_status="SKIPPED",
                build_output=(label + output)[-4000:],
                details={"tool": "gradle", "module": module,
                         "command": f"cd {mod_dir} && " + " ".join(cmd),
                         "executions": [execution]},
            )
        return BuildTestResult(
            success=False,
            build_status="FAIL",
            test_status="SKIPPED",
            build_output=(label + output)[-4000:],
            error=f"{label}gradle compile failed",
            details={"tool": "gradle", "module": module,
                     "command": f"cd {mod_dir} && " + " ".join(cmd),
                     "exit_code": code, "executions": [execution]},
        )

    def run(
        self,
        changed_files: list[str] | None = None,
        target_modules: list[str] | None = None,
        evidence_context: dict[str, str] | None = None,
        authoritative_scope: bool = False,
    ) -> BuildTestResult:
        """빌드 실행. changed_files 는 Manager 가 git 수집 결과를 전달.
        - 루트 빌드 파일 → 기존과 동일하게 루트 빌드
        - 없으면 changed_files 의 모듈 중 빌드 스크립트 있는 모듈만 각각 빌드
        - 모두 성공해야 PASS (하나라도 FAIL → FAIL)
        - 빌드 대상 없음 → SKIPPED (기존 동작)
        """
        self._evidence_context = {
            str(key): str(value) for key, value in (evidence_context or {}).items()
        }
        # F06: fail closed BEFORE host execution when the candidate modifies a
        # build-control file without an explicit elevated host-build
        # authorization from the frozen Job contract.
        build_control_changed = self.build_control_changes(changed_files)
        if build_control_changed and self._evidence_context.get("host_build_authorized") != "True":
            return BuildTestResult(
                success=False,
                build_status="FAIL",
                test_status="NOT_RUN",
                build_output="",
                error="BUILD_CONTROL_SURFACE_MODIFIED: " + ",".join(build_control_changed),
                details={"tool": "guard",
                         "build_control_changed": build_control_changed,
                         "guard": "HOST_BUILD_AUTHORIZATION_REQUIRED",
                         "scope_authority": "JOB_CONTRACT" if authoritative_scope else "LEGACY_INFERRED_SCOPE",
                         "declared_modules": list(target_modules or []),
                         "ignored_changed_file_modules": []},
            )
        protected_before = self._protected_surface_snapshot()
        targets = self._detect_build_targets(
            changed_files, target_modules, authoritative_scope=authoritative_scope
        )
        # F14: the actually-executed project-local scripts are part of the
        # control surface; an unauthorized change to them blocks before host
        # execution exactly like a declared control file.
        executed_closure = self._build_control_closure(targets)
        closure_changed = sorted(
            set(path for path in executed_closure
                if path in {str(c).replace("\\", "/") for c in changed_files or []}))
        if closure_changed and self._evidence_context.get("host_build_authorized") != "True":
            return BuildTestResult(
                success=False,
                build_status="FAIL",
                test_status="NOT_RUN",
                build_output="",
                error="BUILD_CONTROL_SURFACE_MODIFIED: " + ",".join(closure_changed),
                details={"tool": "guard",
                         "build_control_changed": closure_changed,
                         "executed_closure": executed_closure,
                         "guard": "HOST_BUILD_AUTHORIZATION_REQUIRED"},
            )
        declared = list(target_modules or [])
        ignored_modules = sorted({
            self._module_of(path)
            for path in (changed_files or [])
            if self._module_of(path) and self._module_of(path) not in declared
        }) if authoritative_scope and declared else []
        scope_details = {
            "scope_authority": "JOB_CONTRACT" if authoritative_scope else "LEGACY_INFERRED_SCOPE",
            "declared_modules": declared,
            "ignored_changed_file_modules": ignored_modules,
        }

        # ----- 빌드 시스템 없음 -----
        if not targets:
            # 루트/모듈 어디에도 빌드 스크립트가 없으면 건너뛴다.
            # success=False: SKIPPED 는 "빌드 게이트 통과"가 아니다.
            # (ANALYSIS 작업은 Manager 에서 SKIPPED 를 허용한다)
            return BuildTestResult(
                success=False,
                build_status="SKIPPED",
                test_status="SKIPPED",
                build_output="",
                error="",
                details={"tool": "none", "reason": "pom.xml/build.gradle 없음", **scope_details},
            )

        results: list[BuildTestResult] = []
        for module, mod_dir, argv in targets:
            if argv:
                results.append(self._run_explicit(module, mod_dir, argv))
            elif (mod_dir / "pom.xml").exists():
                results.append(self._run_maven(module, mod_dir))
            elif (mod_dir / "package.json").exists():
                results.append(self._run_node(module, mod_dir))
            else:
                results.append(self._run_gradle(module, mod_dir))

        outputs = [r.build_output for r in results if r.build_output]
        protected_after = self._protected_surface_snapshot()
        surface_delta = self.compare_protected_surfaces(protected_before, protected_after)
        protected_mutation = sorted({*surface_delta["modified"],
                                     *surface_delta["deleted"],
                                     *surface_delta["added"]})
        if protected_mutation:
            return BuildTestResult(
                success=False,
                build_status="FAIL",
                test_status="NOT_RUN",
                build_output="\n".join(outputs)[-4000:],
                error="BUILD_PROTECTED_SURFACE_MUTATED: " + ",".join(protected_mutation),
                details={"tool": "guard",
                         "protected_mutation": protected_mutation,
                         "surface_delta": surface_delta,
                         **scope_details},
            )
        if all(r.build_status == "PASS" for r in results):
            return BuildTestResult(
                success=True,
                build_status="PASS",
                test_status="SKIPPED",
                build_output="\n".join(outputs)[-4000:],
                details={"tool": "multi", **scope_details,
                         "modules": [m for m, _, _ in targets],
                         "results": [r.details for r in results]},
            )
        actual_failures = [r for r in results if r.build_status == "FAIL"]
        if not actual_failures:
            skipped = [str(r.details.get("module", "?")) for r in results]
            return BuildTestResult(
                success=False,
                build_status="SKIPPED",
                test_status="SKIPPED",
                build_output="\n".join(outputs)[-4000:],
                details={
                    "tool": "multi", **scope_details,
                    "modules": [m for m, _, _ in targets],
                    "reason": f"build command unavailable: {', '.join(skipped)}",
                    "results": [r.details for r in results],
                },
            )
        failed = [str(r.details.get("module", "?")) for r in actual_failures]
        first_fail = actual_failures[0]
        return BuildTestResult(
            success=False,
            build_status="FAIL",
            test_status="SKIPPED",
            build_output="\n".join(outputs)[-4000:],
            error=f"빌드 실패 모듈: {', '.join(failed)}",
            details={"tool": "multi", **scope_details,
                     "modules": [m for m, _, _ in targets],
                     "failed_modules": failed,
                     "error_detail": first_fail.error,
                     "results": [r.details for r in results]},
        )
