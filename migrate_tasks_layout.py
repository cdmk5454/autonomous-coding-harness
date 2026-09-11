# -*- coding: utf-8 -*-
"""21차 레이아웃 마이그레이션: .tasks 루트의 기존 TASK-* 파일을 일자 폴더로 이동.
- TASK-YYYYMMDD-HHmmss.{json,md,log} → .tasks/YYYYMMDD/ 로
- 구 산출물 디렉토리 .tasks/TASK-*/ → doc/YYYYMMDD/HHmmss/ 로(VERIFY 등)
- 이미 대상이 있으면 건너뛴다(덮어쓰지 않음). 실행: python migrate_tasks_layout.py [--dry]
"""
import re
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
TASKS = ROOT / ".tasks"
DOC = ROOT / "doc"
DRY = "--dry" in sys.argv

ID_RE = re.compile(r"^(TASK-(\d{8})-(\d{6}))")

moved = skipped = 0

# 1) 루트 TASK-*.* 파일 → .tasks/YYYYMMDD/
for f in sorted(TASKS.glob("TASK-*.*")) if TASKS.is_dir() else []:
    m = ID_RE.match(f.name)
    if not m:
        continue
    dest_dir = TASKS / m.group(2)
    dest = dest_dir / f.name
    if dest.exists():
        skipped += 1
        continue
    if DRY:
        print(f"[dry] {f.name} -> {dest.relative_to(ROOT)}")
    else:
        dest_dir.mkdir(exist_ok=True)
        f.rename(dest)
    moved += 1

# 2) 구 산출물 디렉토리 .tasks/TASK-*/ → doc/<일자>/<시간>/
for d in sorted(TASKS.glob("TASK-*/")) if TASKS.is_dir() else []:
    m = ID_RE.match(d.name)
    if not m:
        continue
    dest = DOC / m.group(2) / m.group(3)
    if dest.exists():
        skipped += 1
        continue
    if DRY:
        print(f"[dry] {d.name}/ -> {dest.relative_to(ROOT)}/")
    else:
        dest.mkdir(parents=True, exist_ok=True)
        for f in d.iterdir():
            shutil.move(str(f), str(dest / f.name))
        d.rmdir()
    moved += 1

print(f"\n이동 {moved}건, 건너뜀 {skipped}건" + (" (dry-run)" if DRY else ""))
