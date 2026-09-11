"""
Harness 실행 진입점 (수정 전용 파이프라인, 연속 작업 백그라운드 병렬 실행)
- 실행: python run.py <path> --profile <profile-dir>
  검증된 profile의 모듈을 스캔하여 타겟 모듈 선택지를 구성한다.
- 모든 작업은 MODIFICATION(코드 수정 + 검증)으로 실행된다.
  분석/탐색은 CLI(droid/codex 대화형)로 직접 수행한다 (하네스 비대상).
- 플래너/테스터는 기본 OFF (use_planner=False 고정, 질문 없음).
- 사용자가 연속적으로 작업을 입력할 때마다 Manager 의 스레드 풀에 백그라운드 제출.
  메인 콘솔은 즉시 "추가 작업? (Y/N)" 를 출력하고 다음 입력을 받는다.
- N 입력 시 모든 백그라운드 작업이 끝날 때까지 대기 후 최종 리포트 출력.
- 텔레그램 원격 제어 (22차, env 게이트: KKM_TG_BOT_TOKEN + KKM_TG_ALLOWED_USER_IDS):
  두 env 가 모두 설정돼 있으면 콘솔 입력을 피더 스레드로 우회해 원격 명령(/submit
  /status /report /shutdown)도 받는다. 미설정 시 기존 콘솔 전용 동작을 유지한다.
  Job 제출은 항상 Manager.submit_task 를 경유한다 (워커 직행 금지).
"""

from __future__ import annotations

import sys
import os
import argparse
import stat
import tempfile
import threading
from pathlib import Path
from datetime import datetime
from concurrent.futures import wait as fut_wait, FIRST_COMPLETED

from task_state import TaskState, extract_requirement_modules
from project_profile import (
    ProfileError,
    ProjectProfile,
    profile_summary,
    select_project_profile,
)
from reporter import Reporter
from manager import Manager
from runtime_safety import safe_print as print
from job_contract import (
    DROID_MODELS,
    CODEX_MODELS,
    CODEX_REASONING_EFFORTS as CONTROL_CODEX_REASONING_EFFORTS,
    WORKER_TYPES as CONTROL_WORKER_TYPES,
)


# 일반 워커(Droid)용 모델 목록
AVAILABLE_MODELS = list(DROID_MODELS)

WORKER_TYPES = list(CONTROL_WORKER_TYPES)

AVAILABLE_CODEX_MODELS = list(CODEX_MODELS)
CODEX_REASONING_EFFORTS = list(CONTROL_CODEX_REASONING_EFFORTS)
_TASK_ID_LOCK = threading.Lock()
_ISSUED_TASK_IDS: set[str] = set()
HARNESS_ROOT = Path(__file__).resolve().parent


def runtime_rule_paths() -> tuple[tuple[Path, str], ...]:
    """Return the fixed common/global rule surface used by every harness Job."""
    agents_root = Path(os.environ.get("AGENTS_DIR", r"D:\agents")).absolute()
    user_root = Path(os.environ.get("USERPROFILE") or Path.home()).absolute()
    codex_root = Path(
        os.environ.get("CODEX_HOME")
        or (user_root / ".codex")
    ).absolute()
    factory_root = (user_root / ".factory").absolute()
    shared_skills_root = user_root / ".agents" / "skills"
    return (
        (agents_root / "AGENTS.md", "common_agents"),
        (agents_root / "hooks" / "validation.md", "common_validation"),
        (agents_root / "skills" / "project-context" / "SKILL.md", "project_context_skill"),
        (agents_root / "skills" / "backend" / "SKILL.md", "backend_skill"),
        (agents_root / "skills" / "frontend" / "SKILL.md", "frontend_skill"),
        (agents_root / "skills" / "sql" / "SKILL.md", "sql_skill"),
        (agents_root / "skills" / "review" / "SKILL.md", "review_skill"),
        (codex_root / "AGENTS.md", "codex_global_agents"),
        (factory_root / "AGENTS.md", "factory_global_agents"),
        (
            shared_skills_root / "project-context" / "SKILL.md",
            "global_project_context_skill",
        ),
        (shared_skills_root / "backend" / "SKILL.md", "global_backend_skill"),
        (shared_skills_root / "frontend" / "SKILL.md", "global_frontend_skill"),
        (shared_skills_root / "sql" / "SKILL.md", "global_sql_skill"),
        (shared_skills_root / "review" / "SKILL.md", "global_review_skill"),
    )


def validate_runtime_rules() -> None:
    """Validate common/global rule files before check mode or Job creation."""
    contents: dict[str, bytes] = {}
    for path, label in runtime_rule_paths():
        try:
            current = path
            reparse_flag = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
            while current != current.parent:
                info = current.lstat()
                if current.is_symlink() or int(
                    getattr(info, "st_file_attributes", 0)
                ) & reparse_flag:
                    raise OSError("runtime rule reparse point")
                current = current.parent
            content = path.read_bytes()
            content.decode("utf-8")
        except Exception as exc:
            raise ProfileError("RUNTIME_RULE_UNREADABLE", f"runtime_rules.{label}") from exc
        if not content.strip():
            raise ProfileError("RUNTIME_RULE_EMPTY", f"runtime_rules.{label}")
        contents[label] = content
    source = contents.get("common_agents")
    for label in ("codex_global_agents", "factory_global_agents"):
        if source is not None and contents.get(label) != source:
            raise ProfileError("RUNTIME_RULE_SYNC_MISMATCH", f"runtime_rules.{label}")
    skill_pairs = (
        ("project_context_skill", "global_project_context_skill"),
        ("backend_skill", "global_backend_skill"),
        ("frontend_skill", "global_frontend_skill"),
        ("sql_skill", "global_sql_skill"),
        ("review_skill", "global_review_skill"),
    )
    for source_label, global_label in skill_pairs:
        if source_label in contents and contents.get(global_label) != contents[source_label]:
            raise ProfileError(
                "RUNTIME_RULE_SYNC_MISMATCH", f"runtime_rules.{global_label}"
            )

def reconcile_target_modules(
    requirement: str,
    selected: list[str],
    available: list[str] | None = None,
    profile: ProjectProfile | None = None,
) -> list[str]:
    """Return canonical scope without widening an explicit Job contract.

    Requirement-path inference is a backward-compatible fallback only when a
    legacy caller supplied no target modules.
    """
    order = list(profile.module_names) if profile is not None else list(available or [])
    if selected:
        if available is not None:
            missing = [module for module in selected if module not in available]
            if missing:
                raise ValueError(
                    "작업 계약에 지정된 모듈을 작업 경로에서 찾을 수 없습니다: "
                    + ", ".join(missing)
                )
        if not order:
            return list(dict.fromkeys(selected))
        wanted = set(selected)
        return [module for module in order if module in wanted]

    explicit = extract_requirement_modules(
        requirement, profile.module_names if profile is not None else None
    )
    if not order:
        order = list(dict.fromkeys([*selected, *explicit]))
    if available is not None:
        missing = [module for module in explicit if module not in available]
        if missing:
            raise ValueError(
                "요구사항의 파일 경로에 포함된 모듈을 작업 경로에서 찾을 수 없습니다: "
                + ", ".join(missing)
            )
    wanted = set(explicit)
    return [module for module in order if module in wanted]


def discover_modules(work_path: Path, profile: ProjectProfile | None = None) -> list[str]:
    """
    프로필 모듈 순서를 유지하며 실제 디렉터리가 존재하는 것만 반환.
    직접 helper 호출의 profile 미지정 경로는 모든 직계 디렉터리를 이름순으로 반환한다.
    """
    found: list[str] = []
    if profile is None:
        return sorted(path.name for path in work_path.iterdir() if path.is_dir())
    for module in profile.modules:
        if (work_path / Path(*module.path.split("/"))).is_dir():
            found.append(module.name)
    return found


def input_requirement() -> str:
    """요구사항 여러 줄 입력. END 로 종료."""
    print("\n" + "=" * 60)
    print("요구사항을 입력하세요. (여러 줄 가능, 끝내려면 새 줄에 END 입력)")
    print("=" * 60)
    lines = []
    while True:
        line = input()
        if line.strip() == "END":
            break
        lines.append(line)
    return "\n".join(lines).strip()


def select_target_module(available: list[str]) -> list[str]:
    """
    타겟 모듈 다중 선택.
    - 번호: "1,3"
    - 이름: "api,web"
    - 혼합: "api,3"
    - 빈 입력: [] (미지정)
    available 에 없는 이름/번호는 무시된다.
    """
    if not available:
        print("타겟 모듈: (사용 가능한 모듈 없음 - 미지정)")
        return []

    print("\n타겟 모듈 선택 (선택, 다중 가능):")
    for i, m in enumerate(available, 1):
        print(f"  {i}. {m}")
    print("예: '1,3' 또는 'api,web'  (Enter = 미지정)")

    choice = input("모듈 번호 또는 이름 (쉼표로 다중): ").strip()
    if choice == "":
        print("타겟 모듈: (미지정)")
        return []

    selected: list[str] = []
    for token in choice.replace(";", ",").split(","):
        token = token.strip()
        if not token:
            continue
        if token.isdigit():
            idx = int(token)
            if 1 <= idx <= len(available):
                mname = available[idx - 1]
                if mname not in selected:
                    selected.append(mname)
            else:
                print(f"  무시(범위 초과): {token}")
        elif token in available:
            if token not in selected:
                selected.append(token)
        else:
            print(f"  무시(탐색되지 않은 모듈): {token}")

    if selected:
        print(f"타겟 모듈: {', '.join(selected)}")
    else:
        print("타겟 모듈: (미지정)")
    return selected


def select_model() -> str:
    print("\n사용 가능한 모델:")
    for i, model in enumerate(AVAILABLE_MODELS, 1):
        print(f"  {i}. {model}")

    while True:
        choice = input(f"\n모델 번호 선택 (1-{len(AVAILABLE_MODELS)}): ").strip()
        if choice.isdigit():
            idx = int(choice)
            if 1 <= idx <= len(AVAILABLE_MODELS):
                selected = AVAILABLE_MODELS[idx - 1]
                print(f"선택된 모델: {selected}")
                return selected
        print("잘못된 입력입니다. 다시 선택하세요.")


def select_worker() -> str:
    """
    워커 선택 → worker_type
    - droid: fallback 워커(droid exec)
    - codex: native-session 워커(codex exec)
    - opencode: provider finalization 뒤에만 사용할 managed server 워커
    """
    print("\n워커 선택:")
    print("  1. droid  (일반 워커 - droid exec 호출)")
    print("  2. codex  (고급 워커 - codex exec 호출)")
    print("  3. opencode (provider finalization required)")
    while True:
        choice = input("워커 번호 선택 (1-3) [기본 1]: ").strip()
        if choice == "":
            print("선택된 워커: droid")
            return "droid"
        if choice in ("1", "2", "3"):
            worker_type = WORKER_TYPES[int(choice) - 1]
            print(f"선택된 워커: {worker_type}")
            return worker_type
        print("잘못된 입력입니다. 다시 선택하세요.")


def select_codex_options() -> tuple[str, str]:
    """Codex 모델과 reasoning effort를 번호로 선택한다."""
    print("\n사용 가능한 Codex 모델:")
    for i, model in enumerate(AVAILABLE_CODEX_MODELS, 1):
        print(f"  {i}. {model}")
    while True:
        choice = input(f"모델 번호 선택 (1-{len(AVAILABLE_CODEX_MODELS)}) [기본 1]: ").strip()
        if choice == "":
            model = AVAILABLE_CODEX_MODELS[0]
            break
        if choice.isdigit() and 1 <= int(choice) <= len(AVAILABLE_CODEX_MODELS):
            model = AVAILABLE_CODEX_MODELS[int(choice) - 1]
            break
        print("잘못된 입력입니다. 다시 선택하세요.")

    print("\nCodex reasoning effort:")
    for i, effort in enumerate(CODEX_REASONING_EFFORTS, 1):
        print(f"  {i}. {effort}")
    while True:
        choice = input("reasoning 번호 선택 (1-3) [기본 2=medium]: ").strip()
        if choice == "":
            effort = "medium"
            break
        if choice.isdigit() and 1 <= int(choice) <= len(CODEX_REASONING_EFFORTS):
            effort = CODEX_REASONING_EFFORTS[int(choice) - 1]
            break
        print("잘못된 입력입니다. 다시 선택하세요.")
    print(f"선택된 Codex: model={model}, reasoning={effort}")
    return model, effort


def create_task(
    requirement: str,
    worker_type: str,
    droid_model: str,
    codex_model: str | None,
    target_module: list[str],
    working_dir: str,
    task_id: str,
    target_resources: list[str] | None = None,
    commit_summary: str = "",
    worker_no: int = 0,
    codex_reasoning_effort: str = "medium",
    opencode_model: str = "",
    profile: ProjectProfile | None = None,
) -> TaskState:
    """수정 전용 TaskState 생성.
    - task_mode 는 항상 MODIFICATION 고정 (하네스는 수정+검증 전용).
    - 플래너는 기본 OFF (use_planner=False).
    - worker_no: 병렬 작업 식별용 슬롯 번호(1~max_workers). 0이면 미지정.
    """
    declared_target_modules = list(target_module)
    target_module = reconcile_target_modules(
        requirement,
        target_module,
        list(profile.module_names) if profile else None,
        profile,
    )
    scope_authority = "JOB_CONTRACT" if declared_target_modules else "LEGACY_INFERRED_SCOPE"
    return TaskState(
        task_id=task_id,
        requirement=requirement,
        status="RUNNING",
        stage="WORKING",
        worker=worker_type,
        droid_model=droid_model,
        codex_model=codex_model,
        opencode_model=opencode_model,
        codex_reasoning_effort=codex_reasoning_effort,
        working_dir=working_dir,
        target_module=target_module,
        scope_authority=scope_authority,
        scope_provenance={
            "authority": scope_authority,
            "declared_target_modules": declared_target_modules,
            "effective_target_modules": list(target_module),
            "requirement_inference_used": not bool(declared_target_modules),
        },
        started_at=datetime.now().astimezone().isoformat(),
        target_resources=list(target_resources or []),
        commit_summary=commit_summary,
        task_mode="MODIFICATION",
        use_planner=False,
        worker_no=max(0, worker_no),
        profile_id=profile.id if profile else "",
        profile_dir=str(profile.profile_dir) if profile else "",
        profile_schema_version=profile.schema_version if profile else 0,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="External modification harness")
    parser.add_argument("working_dir", help="project workspace root")
    parser.add_argument("--profile", dest="profile_dir", help="profile directory containing project.json")
    parser.add_argument(
        "--check-profile",
        action="store_true",
        help="validate and summarize the selected profile without starting the harness",
    )
    return parser.parse_args(argv)


def _next_task_id(base_dir: str | Path = ".tasks") -> str:
    """Return a collision-free second-based id while preserving legacy id prefixes."""
    root = Path(base_dir)
    with _TASK_ID_LOCK:
        stem = f"TASK-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        candidate = stem
        suffix = 1
        while (
            candidate in _ISSUED_TASK_IDS
            or TaskState.task_date_dir(candidate, root).joinpath(f"{candidate}.json").exists()
            or root.joinpath(f"{candidate}.json").exists()
        ):
            candidate = f"{stem}-{suffix:02d}"
            suffix += 1
        _ISSUED_TASK_IDS.add(candidate)
        return candidate


def _count_status(futures: list[tuple]) -> tuple[int, int, int]:
    """(running, done, failed) 집계."""
    running = done = failed = 0
    for f, t, _ in futures:
        if not f.done():
            running += 1
        elif t.status == "SUCCESS":
            done += 1
        else:
            failed += 1
    return running, done, failed


def _wait_one(futures: list[tuple]) -> None:
    """완료 대기 중인 future 중 최소 1개가 끝날 때까지 대기."""
    active = [f for f, _, _ in futures if not f.done()]
    if active:
        fut_wait(active, return_when=FIRST_COMPLETED)


def _print_completed(
    futures: list[tuple],
    reported: set[str],
    task_root: str | Path = ".tasks",
) -> None:
    """완료된 작업의 Markdown과 콘솔 요약을 즉시 한 번 확정한다."""
    from reporter import Reporter
    reporter = Reporter()
    for f, t, model_label in futures:
        if f.done() and t.task_id not in reported:
            md_path = reporter.save_markdown(
                t, model=model_label, out_dir=task_root
            )
            reporter.print_console_summary(t, model=model_label)
            print(f"  리포트 저장: {md_path}")
            reported.add(t.task_id)


def _running_line(futures: list[tuple]) -> str:
    """현재 실행 중인 작업들을 한 줄로 표시. worker_no 가 있으면 Worker N 으로 식별."""
    running_tasks = [t for f, t, _ in futures if not f.done()]
    if not running_tasks:
        return ""
    names = "  ".join(
        f"🔄 {_worker_tag(t)} {t.task_id[-8:]}" for t in running_tasks
    )
    return f"  작업 중: {names}"


def _worker_tag(task: TaskState) -> str:
    """병렬 슬롯 식별 라벨. worker_no>0 이면 'Worker1', 아니면 빈 태그."""
    n = getattr(task, "worker_no", 0) or 0
    return f"Worker{n}" if n > 0 else ""


def _alloc_worker_no(futures: list[tuple], max_workers: int) -> int:
    """빈 병렬 슬롯 번호(1..max_workers) 중 최소값 할당.
    완료된 future 의 슬롯은 즉시 회수한다."""
    used = {
        t.worker_no
        for f, t, _ in futures
        if not f.done() and getattr(t, "worker_no", 0)
    }
    for n in range(1, max_workers + 1):
        if n not in used:
            return n
    return max_workers  # 풀 가득 참(이론상 run 루프가 미리 차단함)


# ======================================================================
# 텔레그램 원격 제어 (22차)
# ======================================================================
TELEGRAM_POLL_INTERVAL = 0.5   # 메인 루프가 원격 명령을 확인하는 주기(초)


class ConsoleInputFeeder:
    """콘솔 input() 을 백그라운드 스레드에서 수행해 블로킹을 우회한다.
    - start() 후 readline() 은 (line, eof) 반환. 즉시 값이 없으면 (None, False).
    - 텔레그램 원격 제어 활성 시에만 사용한다 (미활성 시 기존 input() 직접 호출).
    """

    def __init__(self):
        import queue as _queue
        self._queue: "_queue.Queue[str]" = _queue.Queue()
        self._thread: threading.Thread | None = None
        self._eof = threading.Event()

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._feed, name="console-input", daemon=True
        )
        self._thread.start()

    def _feed(self):
        try:
            while True:
                line = input()
                self._queue.put(line)
        except EOFError:
            self._eof.set()
        except Exception:
            self._eof.set()

    def readline(self, timeout: float = 0.0) -> tuple[str | None, bool]:
        """(line, eof). line 은 개행 제거. 대기 없으면 (None, False)."""
        if self._eof.is_set() and self._queue.empty():
            return None, True
        try:
            line = self._queue.get(timeout=timeout) if timeout > 0 \
                else self._queue.get_nowait()
            return line, False
        except Exception:
            return None, False

    def push(self, line: str):
        """원격 제어가 콘솔 입력을 시뮬레이션할 때 사용 (예: N 입력 → 종료)."""
        self._queue.put(line)


def _submit_from_telegram(
    requirement: str,
    options: dict,
    chat_id: int,
    *,
    manager: "Manager",
    futures: list[tuple],
    available_modules: list[str],
    remote: "TelegramRemote",
    profile: ProjectProfile | None = None,
    task_root: str | Path | None = None,
) -> None:
    """/submit 처리(메인 스레드): Manager.submit_task 경유 제출. 워커 직행 없음.
    옵션 → worker/model/effort/modules. 없으면 droid 기본값.
    """
    # 슬롯 가득 차면 대기 없이 거절(원격에서 대기 폴링은 하지 않는다)
    running = sum(1 for f, _, _ in futures if not f.done())
    if running >= manager.max_workers:
        remote.send_message(
            chat_id,
            f"실행 중 {running}/{manager.max_workers} — 슬롯 가득 참. "
            "잠시 후 다시 /submit 하세요.",
        )
        return

    worker_type = options.get("worker", "droid")
    codex_model = options.get("model") if worker_type == "codex" else None
    opencode_model = options.get("model", "") if worker_type == "opencode" else ""
    if worker_type == "opencode" and (
        not opencode_model or os.environ.get("KKM_OPENCODE_LIVE_VALIDATED") != "1"
    ):
        remote.send_message(
            chat_id,
            "OpenCode provider/auth/live model validation is DEFERRED_USER_SETUP; Job was not submitted.",
        )
        return
    effort = options.get("effort", "medium")
    modules = [m for m in options.get("modules", []) if m]

    # 모듈 범위 조율: 명시 경로 모듈 합침 + 존재하지 않는 모듈 거부 (콘솔과 동일 규칙)
    try:
        target_module = reconcile_target_modules(
            requirement, modules, available_modules, profile
        )
    except ValueError as exc:
        remote.send_message(chat_id, f"[제출 중단] {exc}")
        return

    state_root = Path(task_root) if task_root is not None else Path(".tasks")
    task_id = _next_task_id(state_root)
    worker_no = _alloc_worker_no(futures, manager.max_workers)
    task = create_task(
        requirement=requirement,
        worker_type=worker_type,
        droid_model="",
        codex_model=codex_model,
        target_module=target_module,
        working_dir=manager.working_dir,
        task_id=task_id,
        worker_no=worker_no,
        codex_reasoning_effort=effort,
        opencode_model=opencode_model,
        profile=profile,
    )
    task.save(state_root)

    future = manager.submit_task(task)
    futures.append((future, task, "" ))
    date_dir = TaskState.task_date_dir(task_id, state_root).as_posix()
    print(f"  📤 [Telegram] 제출: {task_id} [Worker{worker_no}] → {date_dir}/{task_id}.json")
    remote.send_message(
        chat_id,
        f"제출 완료: {task_id} [Worker{worker_no}]\n"
        f"worker={worker_type}"
        + (f" model={codex_model}" if codex_model else "")
        + (f" effort={effort}" if worker_type == "codex" else "")
        + (f"\n모듈: {', '.join(target_module)}" if target_module else "\n모듈: (미지정)")
        + f"\n리포트: /report {task_id}",
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    task_root = (HARNESS_ROOT / ".tasks").resolve()
    working_dir = args.working_dir
    work_path = Path(working_dir).resolve()

    if not work_path.is_dir():
        print(f"[PROFILE INVALID] project root not found: {work_path}")
        return 2
    try:
        profile = select_project_profile(work_path, args.profile_dir)
        validate_runtime_rules()
    except ProfileError as exc:
        print(f"[PROFILE INVALID] {exc}")
        print("INVALID")
        return 2

    if args.check_profile:
        print("[PROFILE CHECK]")
        for line in profile_summary(profile, work_path):
            print(line)
        print("VALID")
        return 0

    # ===== 모듈 탐색 (path 기반) =====
    available_modules = discover_modules(work_path, profile)
    if available_modules:
        print(f"발견된 프로필 모듈: {', '.join(available_modules)}")
    else:
        print("타겟 모듈을 찾을 수 없어 지정 없이 진행합니다.")

    # ===== 매니저 생성 (공유 스레드 풀) =====
    manager = Manager(
        working_dir=str(work_path),
        model="",
        max_retry=3,
        timeout=300,
        worker_type="droid",
        max_workers=3,
        profile=profile,
        task_root=task_root,
    )

    futures: list[tuple] = []   # [(future, task, model_label), ...]
    reported: set[str] = set()  # 요약 출력 완료한 task_id
    accepting = True            # 새 작업 접수 중인가?

    # ===== 텔레그램 원격 제어 (22차, env 게이트) =====
    from telegram_remote import TelegramRemote

    class _RemoteShutdown(Exception):
        """원격 /shutdown — 현재 콘솔 프롬프트를 즉시 해제하고 배수 단계로 간다."""

    remote: "TelegramRemote | None" = None
    feeder: "ConsoleInputFeeder | None" = None
    remote_state = {"shutdown": False, "chat_id": 0}

    def _status_provider() -> list[dict]:
        return [
            {
                "task_id": t.task_id,
                "status": t.status,
                "stage": t.stage,
                "worker_tag": (
                    f"Worker{t.worker_no} " if getattr(t, "worker_no", 0) else ""
                ),
                "requirement": t.requirement,
            }
            for f, t, _ in futures if not f.done()
        ]

    def _on_submit(requirement: str, options: dict, chat_id: int) -> None:
        assert remote is not None
        _submit_from_telegram(
            requirement, options, chat_id,
            manager=manager, futures=futures,
            available_modules=available_modules, remote=remote,
            profile=profile,
            task_root=task_root,
        )
        _print_completed(futures, reported, task_root)

    def _on_shutdown() -> None:
        remote_state["shutdown"] = True

    def _service_remote() -> None:
        """원격 명령 소비(메인 스레드). /shutdown 은 콘솔 프롬프트를 해제한다."""
        if remote is None:
            return
        for cmd in remote.poll_commands():
            try:
                remote_state["chat_id"] = cmd.chat_id
                remote.handle(cmd)
            except Exception as e:
                print(f"[Telegram] 명령 처리 오류(무시됨): {type(e).__name__} {e}")
        if remote_state["shutdown"]:
            raise _RemoteShutdown()

    try:
        candidate = TelegramRemote(
            on_submit=_on_submit,
            on_shutdown=_on_shutdown,
            status_provider=_status_provider,
            task_root=task_root,
        )
        if candidate.start():
            remote = candidate
    except Exception as e:
        print(f"[Telegram] 원격 제어 초기화 실패(콘솔 전용으로 진행): {type(e).__name__}")

    _orig_input = None
    if remote is not None:
        feeder = ConsoleInputFeeder()
        feeder.start()
        import builtins
        _orig_input = builtins.input

        def _remote_input(prompt=""):
            """콘솔 입력 대기 중 0.5초 주기로 원격 명령을 처리한다."""
            print(prompt, end="", flush=True)
            while True:
                assert feeder is not None
                line, eof = feeder.readline(timeout=TELEGRAM_POLL_INTERVAL)
                if line is not None:
                    # 터미널 에코는 피더 스레드의 input() 이 담당 — 중복 출력 금지
                    return line
                if eof:
                    raise EOFError
                _service_remote()

        builtins.input = _remote_input
        print("[Telegram] 콘솔 입력과 병행하여 원격 명령을 받습니다 (/help).")

    # ===== 연속 작업 입력 + 백그라운드 제출 루프 =====
    first_task = True
    try:
        while accepting:
            # 완료된 작업 표시 (각 루프 시작 시)
            _print_completed(futures, reported, task_root)

            if not first_task:
                # 슬롯 확인: 실행 중이 max_workers 이면 하나 끝날 때까지 대기
                running, done_n, fail_n = _count_status(futures)
                if running >= manager.max_workers:
                    rline = _running_line(futures)
                    print(f"\n[대기] 실행 중 {running}/{manager.max_workers} — "
                          f"완료 대기 중...\n{rline}")
                    while True:
                        if remote is not None:
                            import concurrent.futures as _cf
                            active = [f for f, _, _ in futures if not f.done()]
                            _cf.wait(active, timeout=TELEGRAM_POLL_INTERVAL * 10)
                            try:
                                _service_remote()
                            except _RemoteShutdown:
                                accepting = False
                                break
                        else:
                            _wait_one(futures)
                        if sum(1 for f, _, _ in futures if not f.done()) < manager.max_workers:
                            break
                    if not accepting:
                        break
                    _print_completed(futures, reported, task_root)
                    running, done_n, fail_n = _count_status(futures)

                # 추가 작업 여부
                rline = _running_line(futures)
                prompt = f"\n추가 작업? (Y/N) [실행중:{running} 완료:{done_n} 실패:{fail_n}]"
                if rline:
                    prompt += f"\n{rline}"
                prompt += "\n> "
                more = input(prompt).strip().upper()
                if more != "Y":
                    accepting = False
                    break

            first_task = False

            # 요구사항 입력
            requirement = input_requirement()
            if not requirement:
                print("요구사항이 비어 있습니다. 다시 입력하세요.")
                first_task = True   # Y/N 없이 재입력 허용
                continue

            print("\n입력된 요구사항:")
            print("-" * 40)
            print(requirement)
            print("-" * 40)

            # 워커 → 타겟 모듈 → (droid면) 모델 선택 (수정 전용 — 모드/플래너 질문 없음)
            selected_worker = select_worker()
            selected_modules = select_target_module(available_modules)
            try:
                target_module = reconcile_target_modules(
                    requirement, selected_modules, available_modules, profile
                )
            except ValueError as exc:
                print(f"[제출 중단] {exc}")
                first_task = True
                continue
            auto_added = [module for module in target_module if module not in selected_modules]
            if auto_added:
                print(f"명시 파일 경로 기준 모듈 자동 추가: {', '.join(auto_added)}")
            if selected_worker == "droid":
                selected_model = select_model()
                codex_model = None
                codex_reasoning_effort = "medium"
                opencode_model = ""
            elif selected_worker == "codex":
                selected_model = ""
                codex_model, codex_reasoning_effort = select_codex_options()
                opencode_model = ""
            else:
                selected_model = ""
                codex_model = None
                codex_reasoning_effort = "medium"
                opencode_model = os.environ.get("KKM_OPENCODE_MODEL", "").strip()
                if not opencode_model or os.environ.get("KKM_OPENCODE_LIVE_VALIDATED") != "1":
                    print("OpenCode provider/auth/live model validation is DEFERRED_USER_SETUP; 제출하지 않습니다.")
                    continue

            # ===== TaskState 생성 (MODIFICATION 고정, 플래너 OFF) =====
            task_id = _next_task_id(task_root)
            worker_no = _alloc_worker_no(futures, manager.max_workers)
            task = create_task(
                requirement=requirement,
                worker_type=selected_worker,
                droid_model=selected_model,
                codex_model=codex_model,
                target_module=target_module,
                working_dir=str(work_path),
                task_id=task_id,
                worker_no=worker_no,
                codex_reasoning_effort=codex_reasoning_effort,
                opencode_model=opencode_model,
                profile=profile,
            )
            task.save(task_root)

            # 백그라운드 스레드 풀에 제출. 상태 JSON은 Manager가 같은 Task root에 갱신한다.
            future = manager.submit_task(task)
            model_label = selected_model or "(codex)"
            futures.append((future, task, model_label))

            running, done_n, fail_n = _count_status(futures)
            wtag = f"Worker{worker_no}"
            date_dir = TaskState.task_date_dir(task_id, task_root).as_posix()
            print(f"  📤 제출: {task_id} [{wtag}] (실행 중:{running}/{manager.max_workers}) "
                  f"→ TaskState: {date_dir}/{task_id}.json")
    except _RemoteShutdown:
        print("\n[Telegram] 원격 종료 요청 — 신규 제출을 중지하고 진행 Job 완료 후 종료합니다.")
    finally:
        if _orig_input is not None:
            import builtins
            builtins.input = _orig_input

    # ===== 모든 백그라운드 작업 완료 대기 =====
    while True:
        _print_completed(futures, reported, task_root)
        running, _, _ = _count_status(futures)
        if running == 0:
            break
        rline = _running_line(futures)
        print(f"\n남은 작업 {running}개 대기 중... {rline}")
        if remote is not None:
            # 배수 중에도 /status · /report 응답 (짧은 주기로 폴링)
            import concurrent.futures as _cf
            active = [f for f, _, _ in futures if not f.done()]
            _cf.wait(active, timeout=TELEGRAM_POLL_INTERVAL * 10)
            for cmd in remote.poll_commands():
                try:
                    if cmd.name == "shutdown":
                        continue   # 이미 종료 절차 진행 중
                    remote.handle(cmd)
                except Exception as e:
                    print(f"[Telegram] 명령 처리 오류(무시됨): {type(e).__name__}")
        else:
            _wait_one(futures)
    print("\n" + "=" * 60)

    reporter = Reporter()
    all_success = True
    for future, task, model_label in futures:
        result = future.result()   # 완료까지 대기
        # Markdown 리포트 저장 + 경로 안내
        md_path = reporter.save_markdown(task, model=model_label, out_dir=task_root)
        icon = "✅" if result.success else "❌"
        print(f"  {icon} {task.task_id} → {md_path}")
        if not result.success:
            all_success = False

    # 최종 요약
    _, done_n, fail_n = _count_status(futures)
    print("\n" + "=" * 60)
    print(f"전체 완료: 성공 {done_n}건, 실패 {fail_n}건 (총 {len(futures)}건)")
    print(f"TaskState/리포트 루트: {task_root}")
    print("리포트 확인 후 종료합니다.")
    print("=" * 60)

    if remote is not None:
        if remote_state["chat_id"]:
            remote.send_message(remote_state["chat_id"], "실행기를 종료합니다.")
        remote.stop()
    manager.shutdown()

    if not all_success:
        return 1
    return 0


class SingleInstanceLock:
    """동일 사용자 세션에서 두 번째 run.py 프로세스를 비차단 방식으로 거부한다."""

    def __init__(self, path: str | Path | None = None):
        self.path = Path(path or (Path(tempfile.gettempdir()) / "kkm-test-run.lock"))
        self._file = None

    def acquire(self) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self.path, "a+b")
        if self.path.stat().st_size == 0:
            self._file.write(b"\0")
            self._file.flush()
        self._file.seek(0)
        try:
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self._file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self._file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except OSError:
            self._file.close()
            self._file = None
            return False

    def release(self) -> None:
        if self._file is None:
            return
        try:
            self._file.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self._file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
        finally:
            self._file.close()
            self._file = None


if __name__ == "__main__":
    if "--check-profile" in sys.argv[1:]:
        sys.exit(main())
    instance_lock = SingleInstanceLock()
    if not instance_lock.acquire():
        print("다른 KKM 하네스가 이미 실행 중입니다. 기존 창에서 Y로 작업을 추가하세요.")
        sys.exit(2)
    try:
        sys.exit(main())
    finally:
        instance_lock.release()
