"""
Planner - 작업 전 분석 전용 에이전트 (코드 수정 금지)
- use_planner=True 인 작업에서 Worker 실행 전에 1회 호출된다.
- droid exec 를 분석 전용(--auto 는 KKM_DROID_AUTO_ANALYSIS, 기본 medium)으로
  호출하여 3종 브리프를 생성:
    worker_brief  : 구현 범위 / 금지사항 / 완료 기준
    tester_brief  : 프론트 진입 경로 / 확인할 UI / DB SELECT 검증 포인트 / 스킵 조건
    reviewer_brief: 반드시 볼 포인트 / 테스트 증거 필수 여부 / 범위 밖 항목
- 결과는 TaskState.planner_brief 에 저장되며 각 단계 프롬프트에 주입된다.
- Planner 실패/타임아웃/파싱 실패는 None 반환 + 경고 로그. 작업 파이프라인은 계속된다.
- 엔진: droid 고정 (모델은 KKM_PLANNER_MODEL env 로 교체 가능).
"""

from __future__ import annotations

import os
import re
import subprocess
import tempfile
from harness_temp import scratch_root
from pathlib import Path

from task_state import TaskState
from job_contract import DROID_DEFAULT_CODING_MODEL
from worker import droid_auto_level
from project_profile import ProjectProfile
from feature_context import load_feature_context
from context_foundation import build_and_record_context
from runtime_safety import (
    assert_secret_free_argv, isolated_subprocess_env,
    safe_print as print, trusted_executable,
)

# 플래너용 droid 모델 (기본: canonical registry primary coding model)
PLANNER_MODEL = os.environ.get(
    "KKM_PLANNER_MODEL", DROID_DEFAULT_CODING_MODEL
)
PLANNER_TIMEOUT = int(os.environ.get("KKM_PLANNER_TIMEOUT", "300"))

BRIEF_KEYS = ("worker_brief", "tester_brief", "reviewer_brief")


def _build_planner_prompt(
    task: TaskState,
    profile: ProjectProfile | None = None,
    policy_root: Path | None = None,
    manifest_root: Path | None = None,
) -> str:
    if manifest_root is None:
        manifest_root = getattr(task, "_context_manifest_root", None)
    mods = ", ".join(task.target_module) if task.target_module else "(미지정)"
    profile_context = ""
    if profile is not None:
        pack = build_and_record_context(
            task=task,
            role="PLANNER",
            profile=profile,
            workspace=task.working_dir or profile.workspace_roots[0],
            policy_root=policy_root or Path(os.environ.get("AGENTS_DIR", r"D:\agents")),
            manifest_root=manifest_root,
            changed_files=task.changed_files,
            task_evidence={
                "changed_files": list(task.changed_files),
                "inherited_batch_delta_files": list(task.inherited_batch_delta_files),
            },
        )
        profile_context = pack.prompt + "\n\n"
    common_context = ""
    if policy_root is not None and profile is None:
        blocks: list[str] = []
        required = (
            policy_root / "AGENTS.md",
            policy_root / "hooks" / "validation.md",
            policy_root / "skills" / "project-context" / "SKILL.md",
        )
        optional = (policy_root / "skills" / "planning" / "SKILL.md",)
        try:
            for path in required:
                text = path.read_text(encoding="utf-8").strip()
                if not text:
                    raise OSError("empty policy")
                blocks.append(text)
            for path in optional:
                if path.is_file():
                    text = path.read_text(encoding="utf-8").strip()
                    if text:
                        blocks.append(text)
        except Exception as exc:
            raise RuntimeError("planner policy snapshot unreadable") from exc
        common_context = "[공통 하네스 규칙]\n" + "\n\n---\n\n".join(blocks) + "\n\n"
    return (
        "너는 작업 플래너다. 아래 [작업 정보]를 분석해서 3종 브리프를 생성해라.\n"
        "절대 규칙: 코드/파일을 수정하지 마라. 읽기와 분석만 한다.\n\n"
        f"{common_context}"
        f"{profile_context}"
        f"[작업 정보]\n"
        f"- 요구사항: {task.requirement}\n"
        f"- 작업 모드: {task.task_mode}\n"
        f"- 타겟 모듈: {mods}\n"
        f"- 작업 디렉토리: {task.working_dir}\n\n"
        "프로젝트 코드를 실제로 읽고(필요 시 검색) 아래 형식으로만 출력하라.\n"
        "[출력 형식] (오직 아래 형태만)\n"
        "WORKER_BRIEF:\n<구현 범위 / 금지사항 / 완료 기준. 각 항목 간결히>\n"
        "TESTER_BRIEF:\n<프론트 진입 경로(menu/URL) / 확인할 UI 요소 / "
        "DB SELECT 검증 포인트(쿼리 초안, SELECT만) / 스킵 조건>\n"
        "REVIEWER_BRIEF:\n<반드시 볼 포인트 / 테스트 증거 필수 여부(필수|선택) / "
        "범위 밖 항목(리뷰에서 무시할 것)>\n"
    )


def _parse_briefs(out: str) -> dict[str, str]:
    """planner 출력에서 3종 브리프 파싱. 최소 1개라도 있으면 반환."""
    briefs: dict[str, str] = {}
    for i, key in enumerate(BRIEF_KEYS):
        pattern = rf"{key}:\s*\n?(.*?)(?=\n(?:WORKER_BRIEF|TESTER_BRIEF|REVIEWER_BRIEF):|\Z)"
        m = re.search(pattern, out, re.DOTALL | re.IGNORECASE)
        if m:
            text = m.group(1).strip()
            if text:
                briefs[key] = text[:3000]
    return briefs


def run_planner(
    task: TaskState,
    working_dir: str,
    profile: ProjectProfile | None = None,
    policy_root: Path | None = None,
    manifest_root: Path | None = None,
) -> dict[str, str] | None:
    """
    플래너 실행 → 브리프 dict 반환. 실패 시 None (작업 계속).
    반환 dict 는 worker_brief/tester_brief/reviewer_brief 키(최소 1개 이상).
    """
    work_path = Path(working_dir).resolve()
    prompt = _build_planner_prompt(task, profile, policy_root, manifest_root)

    with tempfile.NamedTemporaryFile(
            dir=scratch_root(),
        mode="w", suffix=".txt", delete=False, encoding="utf-8"
    ) as f:
        f.write(prompt)
        prompt_file = f.name

    auto_level = droid_auto_level("ANALYSIS")
    cmd = [
        "droid", "exec",
        "--auto", auto_level,
        "-m", PLANNER_MODEL,
        "--cwd", str(work_path),
        "-f", prompt_file,
    ]
    print(f"[Planner] 실행: droid exec (model={PLANNER_MODEL}, "
          f"timeout={PLANNER_TIMEOUT}s, auto={auto_level})")
    try:
        cmd[0] = trusted_executable(cmd[0], forbidden_root=work_path)
        child_env = isolated_subprocess_env(
            "planner",
            profile.profile_dir if profile is not None else task.profile_dir or None,
        )
        assert_secret_free_argv(cmd, env=child_env)
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=PLANNER_TIMEOUT,
            env=child_env,
        )
    except FileNotFoundError:
        print("[Planner] droid 미설치 — 플래너 생략(작업 계속)")
        return None
    except subprocess.TimeoutExpired:
        print(f"[Planner] 타임아웃({PLANNER_TIMEOUT}s) — 플래너 생략(작업 계속)")
        return None
    finally:
        try:
            Path(prompt_file).unlink(missing_ok=True)
        except Exception:
            pass

    if result.returncode != 0:
        err = (result.stderr or "")[:300]
        print(f"[Planner] 실행 실패(exit={result.returncode}): {err} — 생략(작업 계속)")
        return None

    briefs = _parse_briefs(result.stdout or "")
    if not briefs:
        print("[Planner] 브리프 파싱 실패 — 플래너 생략(작업 계속)")
        return None

    task.planner_brief = briefs
    print(f"[Planner] 브리프 생성 완료: {', '.join(sorted(briefs))}")
    return briefs
