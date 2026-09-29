"""Fail-closed validation for persistent control-plane state roots."""

from __future__ import annotations

import os
import stat
import tempfile
import threading
import time
from contextlib import contextmanager
from pathlib import Path


class ControlPathError(RuntimeError):
    def __init__(self, code: str, field: str):
        self.code = code
        self.field = field
        super().__init__(f"{code}: {field}")


class _PathWriteEntry:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.users = 0


_PATH_WRITE_GUARD = threading.Lock()
_PATH_WRITE_ENTRIES: dict[str, _PathWriteEntry] = {}
_WINDOWS_TRANSIENT_REPLACE_WINERRORS = frozenset({5, 32})
_WINDOWS_REPLACE_BACKOFF_SECONDS = (0.01, 0.02, 0.04)


@contextmanager
def _serialized_state_path(path: Path):
    """Serialize writers to one state path without blocking other paths."""
    key = os.path.normcase(os.path.abspath(str(path)))
    with _PATH_WRITE_GUARD:
        entry = _PATH_WRITE_ENTRIES.get(key)
        if entry is None:
            entry = _PathWriteEntry()
            _PATH_WRITE_ENTRIES[key] = entry
        entry.users += 1
    entry.lock.acquire()
    try:
        yield
    finally:
        entry.lock.release()
        with _PATH_WRITE_GUARD:
            entry.users -= 1
            if entry.users == 0 and _PATH_WRITE_ENTRIES.get(key) is entry:
                _PATH_WRITE_ENTRIES.pop(key, None)


def _is_transient_windows_replace_error(exc: OSError) -> bool:
    return (
        os.name == "nt"
        and isinstance(exc, PermissionError)
        and getattr(exc, "winerror", None) in _WINDOWS_TRANSIENT_REPLACE_WINERRORS
    )


def _same_path(left: Path, right: Path) -> bool:
    return os.path.normcase(os.path.abspath(str(left))) == os.path.normcase(
        os.path.abspath(str(right))
    )


def _is_reparse(path: Path) -> bool:
    try:
        info = path.lstat()
    except FileNotFoundError:
        return False
    attributes = int(getattr(info, "st_file_attributes", 0))
    reparse_flag = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    return path.is_symlink() or bool(attributes & reparse_flag)


def _absolute(path: str | Path) -> Path:
    """Return a lexical absolute path without resolving a possible reparse point."""
    return Path(os.path.abspath(str(path)))


def _relative_parts(root: Path, candidate: Path, field: str) -> tuple[str, ...]:
    try:
        relative = candidate.relative_to(root)
    except ValueError as exc:
        raise ControlPathError("CONTROL_STATE_PATH_ESCAPE", field) from exc
    return relative.parts


def _assert_existing_entry(path: Path, field: str, *, directory: bool | None = None) -> None:
    if _is_reparse(path):
        raise ControlPathError("CONTROL_STATE_PATH_REPARSE", field)
    if directory is True and not path.is_dir():
        raise ControlPathError("CONTROL_STATE_PATH_NOT_DIRECTORY", field)
    if directory is False and not path.is_file():
        raise ControlPathError("CONTROL_STATE_PATH_NOT_FILE", field)


def validate_no_reparse_tree(root: str | Path, field: str) -> None:
    """Reject every existing symlink/junction/reparse entry under a state root."""
    base = _absolute(root)
    _assert_existing_entry(base, field, directory=True)

    def _raise_walk_error(error: OSError) -> None:
        # os.walk otherwise suppresses scandir failures and silently returns an
        # incomplete inventory, which is unsafe for persistent control state.
        raise error

    try:
        for current, directories, files in os.walk(
            base,
            topdown=True,
            followlinks=False,
            onerror=_raise_walk_error,
        ):
            current_path = Path(current)
            _assert_existing_entry(current_path, field, directory=True)
            for name in tuple(directories) + tuple(files):
                _assert_existing_entry(current_path / name, field)
    except ControlPathError:
        raise
    except OSError as exc:
        raise ControlPathError("CONTROL_STATE_TREE_UNREADABLE", field) from exc


def ensure_safe_state_root(
    root: str | Path, *, field: str, create: bool = False
) -> Path:
    """Ensure a state root itself is a regular directory, without resolving it first."""
    base = _absolute(root)
    if _is_reparse(base):
        raise ControlPathError("CONTROL_STATE_ROOT_REPARSE", field)
    if not base.exists():
        if not create:
            raise ControlPathError("CONTROL_STATE_ROOT_MISSING", field)
        parent = base.parent
        _assert_existing_entry(parent, field, directory=True)
        try:
            base.mkdir()
        except OSError as exc:
            raise ControlPathError("CONTROL_STATE_ROOT_CREATE_FAILED", field) from exc
    _assert_existing_entry(base, field, directory=True)
    return base


def ensure_safe_state_directory(
    root: str | Path,
    directory: str | Path,
    *,
    field: str,
    create: bool = False,
) -> Path:
    """Validate/create a directory one component at a time without following reparses."""
    base = ensure_safe_state_root(root, field=field)
    candidate = _absolute(directory)
    parts = _relative_parts(base, candidate, field)
    _assert_existing_entry(base, field, directory=True)
    try:
        resolved_base = base.resolve(strict=True)
    except OSError as exc:
        raise ControlPathError("CONTROL_STATE_ROOT_UNRESOLVED", field) from exc

    current = base
    for part in parts:
        current = current / part
        if _is_reparse(current):
            raise ControlPathError("CONTROL_STATE_PATH_REPARSE", field)
        if current.exists():
            _assert_existing_entry(current, field, directory=True)
        elif create:
            try:
                current.mkdir()
            except OSError as exc:
                raise ControlPathError("CONTROL_STATE_DIRECTORY_CREATE_FAILED", field) from exc
            _assert_existing_entry(current, field, directory=True)
        else:
            raise ControlPathError("CONTROL_STATE_PATH_MISSING", field)
        try:
            resolved = current.resolve(strict=True)
            resolved.relative_to(resolved_base)
        except (OSError, ValueError) as exc:
            raise ControlPathError("CONTROL_STATE_PATH_ESCAPE", field) from exc
    return candidate


def ensure_safe_state_file(
    root: str | Path,
    path: str | Path,
    *,
    field: str,
    allow_missing: bool = False,
) -> Path:
    """Validate a state file and its parent without following a reparse leaf."""
    base = ensure_safe_state_root(root, field=field)
    candidate = _absolute(path)
    _relative_parts(base, candidate, field)
    ensure_safe_state_directory(base, candidate.parent, field=field, create=False)
    if _is_reparse(candidate):
        raise ControlPathError("CONTROL_STATE_PATH_REPARSE", field)
    if candidate.exists():
        _assert_existing_entry(candidate, field, directory=False)
    elif not allow_missing:
        raise ControlPathError("CONTROL_STATE_PATH_MISSING", field)
    return candidate


def atomic_state_write(
    path: str | Path,
    data: bytes,
    *,
    root: str | Path,
    field: str,
) -> Path:
    """Atomically replace one regular state file under a reparse-free directory."""
    base = _absolute(root)
    candidate = _absolute(path)
    with _serialized_state_path(candidate):
        parent = ensure_safe_state_directory(
            base, candidate.parent, field=field, create=True
        )
        ensure_safe_state_file(base, candidate, field=field, allow_missing=True)
        fd = -1
        temp_name = ""
        try:
            fd, temp_name = tempfile.mkstemp(
                prefix=f".{candidate.name}.", suffix=".tmp", dir=parent
            )
            with os.fdopen(fd, "wb") as stream:
                fd = -1
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            for retry_no in range(len(_WINDOWS_REPLACE_BACKOFF_SECONDS) + 1):
                # Revalidate immediately before every replace attempt. os.replace
                # replaces a reparse leaf itself rather than opening its target,
                # but an unsafe parent must never be accepted.
                ensure_safe_state_directory(base, parent, field=field, create=False)
                ensure_safe_state_file(
                    base, candidate, field=field, allow_missing=True
                )
                try:
                    os.replace(temp_name, candidate)
                    break
                except OSError as exc:
                    if (
                        not _is_transient_windows_replace_error(exc)
                        or retry_no >= len(_WINDOWS_REPLACE_BACKOFF_SECONDS)
                    ):
                        raise
                    time.sleep(_WINDOWS_REPLACE_BACKOFF_SECONDS[retry_no])
            temp_name = ""
            ensure_safe_state_file(base, candidate, field=field)
            return candidate
        except ControlPathError:
            raise
        except OSError as exc:
            raise ControlPathError("CONTROL_STATE_WRITE_FAILED", field) from exc
        finally:
            if fd >= 0:
                os.close(fd)
            if temp_name:
                try:
                    os.unlink(temp_name)
                except FileNotFoundError:
                    pass


def _probe_atomic_io(path: Path, field: str) -> None:
    first_fd = -1
    first_name = ""
    second_name = ""
    try:
        first_fd, first_name = tempfile.mkstemp(
            prefix=".control-path-probe.", suffix=".tmp", dir=path
        )
        with os.fdopen(first_fd, "wb") as stream:
            first_fd = -1
            stream.write(b"control-path-probe")
            stream.flush()
            os.fsync(stream.fileno())
        second_name = first_name + ".replace"
        os.replace(first_name, second_name)
        first_name = ""
        if Path(second_name).read_bytes() != b"control-path-probe":
            raise OSError("probe content mismatch")
    except OSError as exc:
        raise ControlPathError("CONTROL_STATE_ROOT_NOT_WRITABLE", field) from exc
    finally:
        if first_fd >= 0:
            os.close(first_fd)
        for candidate in (first_name, second_name):
            if candidate:
                try:
                    os.unlink(candidate)
                except FileNotFoundError:
                    pass


def validate_state_root(
    value: str | Path,
    *,
    state_base: str | Path,
    expected_name: str,
    field: str,
) -> Path:
    """Validate one direct, non-reparse child and prove atomic local I/O.

    The probe is deliberately removed again. If validation created an otherwise
    empty root, the root is removed as well so ``--check-control`` leaves no state.
    """

    base = Path(state_base).absolute()
    if not base.exists() or not base.is_dir():
        raise ControlPathError("CONTROL_STATE_BASE_INVALID", field)
    if _is_reparse(base):
        raise ControlPathError("CONTROL_STATE_BASE_REPARSE", field)

    raw = Path(value)
    candidate = (base / raw if not raw.is_absolute() else raw).absolute()
    if candidate.name != expected_name or not _same_path(candidate.parent, base):
        raise ControlPathError("CONTROL_STATE_ROOT_ESCAPE", field)
    if _is_reparse(candidate):
        raise ControlPathError("CONTROL_STATE_ROOT_REPARSE", field)

    created = False
    try:
        if candidate.exists():
            if not candidate.is_dir():
                raise ControlPathError("CONTROL_STATE_ROOT_NOT_DIRECTORY", field)
        else:
            candidate.mkdir()
            created = True

        if _is_reparse(candidate):
            raise ControlPathError("CONTROL_STATE_ROOT_REPARSE", field)
        try:
            resolved_base = base.resolve(strict=True)
            resolved_candidate = candidate.resolve(strict=True)
        except OSError as exc:
            raise ControlPathError("CONTROL_STATE_ROOT_UNRESOLVED", field) from exc
        if not _same_path(resolved_candidate.parent, resolved_base):
            raise ControlPathError("CONTROL_STATE_ROOT_ESCAPE", field)
        validate_no_reparse_tree(candidate, field)
        _probe_atomic_io(candidate, field)
        return resolved_candidate
    finally:
        if created:
            try:
                candidate.rmdir()
            except OSError:
                # A concurrent legitimate creator may have added state after the
                # probe. Never delete a non-empty directory here.
                pass


def validate_control_state_roots(
    control_root: str | Path,
    task_root: str | Path,
    *,
    state_base: str | Path,
) -> tuple[Path, Path]:
    control = validate_state_root(
        control_root,
        state_base=state_base,
        expected_name=".control",
        field="control_root",
    )
    tasks = validate_state_root(
        task_root,
        state_base=state_base,
        expected_name=".tasks",
        field="task_root",
    )
    if _same_path(control, tasks):
        raise ControlPathError("CONTROL_STATE_ROOT_COLLISION", "task_root")
    return control, tasks


def validate_existing_state_root_read_only(
    value: str | Path,
    *,
    state_base: str | Path,
    expected_name: str,
    field: str,
) -> Path:
    """Validate one existing fixed state root without creating or probing it.

    The writable validator deliberately proves atomic I/O by creating a temporary
    file.  A read-only Inspector must not do that, nor may it create a missing
    ``.control``/``.tasks`` root.  This variant performs only lexical containment,
    resolved containment, type, readability and reparse-tree checks.
    """

    base = _absolute(state_base)
    if not base.exists() or not base.is_dir():
        raise ControlPathError("CONTROL_STATE_BASE_INVALID", field)
    if _is_reparse(base):
        raise ControlPathError("CONTROL_STATE_BASE_REPARSE", field)

    raw = Path(value)
    candidate = _absolute(base / raw if not raw.is_absolute() else raw)
    if candidate.name != expected_name or not _same_path(candidate.parent, base):
        raise ControlPathError("CONTROL_STATE_ROOT_ESCAPE", field)
    if _is_reparse(candidate):
        raise ControlPathError("CONTROL_STATE_ROOT_REPARSE", field)
    if not candidate.exists():
        raise ControlPathError("CONTROL_STATE_ROOT_MISSING", field)
    if not candidate.is_dir():
        raise ControlPathError("CONTROL_STATE_ROOT_NOT_DIRECTORY", field)
    try:
        resolved_base = base.resolve(strict=True)
        resolved_candidate = candidate.resolve(strict=True)
    except OSError as exc:
        raise ControlPathError("CONTROL_STATE_ROOT_UNRESOLVED", field) from exc
    if not _same_path(resolved_candidate.parent, resolved_base):
        raise ControlPathError("CONTROL_STATE_ROOT_ESCAPE", field)
    validate_no_reparse_tree(candidate, field)
    return resolved_candidate


def validate_control_state_roots_read_only(
    control_root: str | Path,
    task_root: str | Path,
    *,
    state_base: str | Path,
) -> tuple[Path, Path]:
    """Validate fixed existing control roots with zero filesystem writes."""

    control = validate_existing_state_root_read_only(
        control_root,
        state_base=state_base,
        expected_name=".control",
        field="control_root",
    )
    tasks = validate_existing_state_root_read_only(
        task_root,
        state_base=state_base,
        expected_name=".tasks",
        field="task_root",
    )
    if _same_path(control, tasks):
        raise ControlPathError("CONTROL_STATE_ROOT_COLLISION", "task_root")
    return control, tasks
