"""Shared redaction and child-process environment isolation."""

from __future__ import annotations

import os
import re
import builtins
import sys
import stat
from pathlib import Path
from typing import Mapping


_AUTH_HEADER_PATTERN = re.compile(
    r"(?i)(\b(?:proxy-)?authorization\s*[:=]\s*)[^\r\n]*"
)

_SECRET_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(
        r"(?i)(--(?:api[_-]?key|provider[_-]?token|bearer[_-]?token|access[_-]?token|"
        r"client[_-]?secret|password|passwd|secret|token)(?:\s*=\s*|\s+))"
        r"(?:\"[^\"]*\"|'[^']*'|\S+)"
    ), r"\1****"),
    (re.compile(
        r"(?i)(password|passwd|secret|token|api[_-]?key|authorization|private[_-]?key|pat)"
        r"(\s*[=:]\s*)\S+"
    ), r"\1\2****"),
    (re.compile(
        r"(?i)\b((?:redis|https?|jdbc[a-z0-9:]*)://[^\s/:@]+:)[^\s]+(@)"
    ), r"\1****\2"),
    (re.compile(r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b"), "****"),
    (re.compile(
        r"\b(?:gh[pousr]_[A-Za-z0-9]{12,}|github_pat_[A-Za-z0-9_]{12,}|"
        r"glpat-[A-Za-z0-9_-]{12,}|sk-[A-Za-z0-9_-]{12,}|"
        r"ctx7sk-[A-Za-z0-9_-]{12,})\b",
        re.IGNORECASE,
    ), "****"),
    (re.compile(
        r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----[\s\S]*?"
        r"-----END (?:RSA |EC |OPENSSH )?PRIVATE KEY-----",
        re.IGNORECASE,
    ), "[PRIVATE KEY REDACTED]"),
)

_SECRET_NAME_RE = re.compile(
    r"(?i)(password|passwd|secret|token|private|credential|api[_-]?key|pat|chat[_-]?id)"
)
_NON_SECRET_ENV_NAMES = frozenset({"PATH", "HOMEPATH", "PATHEXT"})
_COMMON_WINDOWS_ENV = (
    "PATH",
    "PATHEXT",
    "SYSTEMROOT",
    "WINDIR",
    "COMSPEC",
    "SYSTEMDRIVE",
    "TEMP",
    "TMP",
    "USERPROFILE",
    "APPDATA",
    "LOCALAPPDATA",
    "HOMEDRIVE",
    "HOMEPATH",
)
_DROID_AUTH_ENV = (
    "ANTHROPIC_API_KEY",
    "DROID_API_KEY",
    "FACTORY_API_KEY",
    "Z_AI_API_KEY",
)
ROLE_ENV_ALLOWLISTS: dict[str, tuple[str, ...]] = {
    "git": (),
    "codex": (
        "CODEX_HOME", "OPENAI_API_KEY", "CODEX_API_KEY",
    ),
    "droid": _DROID_AUTH_ENV + (
        "KKM_DROID_AUTO_ANALYSIS", "KKM_DROID_AUTO_MODIFICATION",
    ),
    "reviewer": (
        "CODEX_HOME", "OPENAI_API_KEY", "CODEX_API_KEY",
        "KKM_REVIEW_MODEL", "KKM_REVIEW_CODEX_MODEL", "KKM_REVIEW_REASONING",
    ),
    "tester": _DROID_AUTH_ENV + (
        "KKM_DROID_AUTO_ANALYSIS", "KKM_TESTER_MODEL", "KKM_TESTER_TIMEOUT",
        "KKM_TEST_FRONTEND", "KKM_TEST_DB", "CLASSPATH", "DATABASE_HOST",
        "DATABASE_PORT", "DATABASE_NAME", "DATABASE_USER", "DATABASE_PASSWORD",
    ),
    "planner": _DROID_AUTH_ENV + (
        "KKM_DROID_AUTO_ANALYSIS", "KKM_PLANNER_MODEL", "KKM_PLANNER_TIMEOUT",
    ),
    "build": (
        "JAVA_HOME", "MAVEN_HOME", "M2_HOME", "MAVEN_OPTS", "GRADLE_HOME",
        "GRADLE_USER_HOME", "GRADLE_OPTS", "NVM_HOME", "NVM_SYMLINK",
        "NODE_HOME", "NPM_CONFIG_CACHE", "PNPM_HOME", "YARN_CACHE_FOLDER",
    ),
}
_AGENT_ROLES = frozenset(("codex", "droid", "reviewer", "tester", "planner"))
_SECRET_ARG_FLAGS = frozenset({
    "--api-key", "--api_key", "--provider-token", "--provider_token",
    "--bearer-token", "--bearer_token", "--access-token", "--access_token",
    "--client-secret", "--client_secret", "--password", "--passwd",
    "--secret", "--token",
})


class SecretArgvError(RuntimeError):
    code = "SECRET_IN_PROCESS_ARGV"
    retry_domain = "TECHNICAL_EXECUTION_RETRY"
    product_retry_delta = 0

    def __init__(self) -> None:
        super().__init__(self.code)


def assert_secret_free_argv(
    argv: object,
    *,
    env: Mapping[str, str] | None = None,
    known_secrets: tuple[str, ...] = (),
) -> None:
    """Reject raw credentials before spawn without rendering the offending argv."""
    if isinstance(argv, (str, bytes)):
        parts = [str(argv)]
    else:
        try:
            parts = [str(item) for item in argv]  # type: ignore[arg-type]
        except TypeError as exc:
            raise TypeError("argv must be a string or iterable") from exc

    for index, part in enumerate(parts):
        flag, separator, inline_value = part.partition("=")
        if flag.casefold() in _SECRET_ARG_FLAGS:
            if (separator and inline_value) or (not separator and index + 1 < len(parts)):
                raise SecretArgvError()
        if scrub_secrets(part, env={}) != part:
            raise SecretArgvError()

    environment = os.environ if env is None else env
    candidates = list(known_secrets)
    candidates.extend(
        value for name, value in environment.items()
        if str(name).upper() not in _NON_SECRET_ENV_NAMES
        and _SECRET_NAME_RE.search(str(name)) and isinstance(value, str)
    )
    for secret in candidates:
        if len(secret) >= 4 and any(secret in part for part in parts):
            raise SecretArgvError()


def _is_reparse_entry(path: Path) -> bool:
    try:
        info = path.lstat()
    except (FileNotFoundError, OSError):
        return False
    attributes = int(getattr(info, "st_file_attributes", 0))
    reparse_flag = int(getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))
    is_junction = getattr(path, "is_junction", None)
    return bool(
        path.is_symlink()
        or (callable(is_junction) and is_junction())
        or attributes & reparse_flag
    )


def scrub_secrets(text: object, env: Mapping[str, str] | None = None) -> str:
    value = "" if text is None else str(text)
    value = _AUTH_HEADER_PATTERN.sub(r"\1****", value)
    for pattern, replacement in _SECRET_PATTERNS:
        value = pattern.sub(replacement, value)
    environment = os.environ if env is None else env
    for name, secret in environment.items():
        # PAT is a credential name; Windows HOMEPATH/PATH/PATHEXT are paths.
        # Redacting HOMEPATH corrupts durable references under the system temp root.
        if name.upper() in _NON_SECRET_ENV_NAMES:
            continue
        if _SECRET_NAME_RE.search(name) and isinstance(secret, str) and len(secret) >= 4:
            value = value.replace(secret, "****")
    return value


def safe_print(*values: object, **kwargs: object) -> None:
    """Print redacted values representable by the selected output stream."""
    options = dict(kwargs)
    if "file" not in options and os.environ.get("KKM_MCP_STDIO") == "1":
        # MCP stdio reserves stdout for JSON-RPC frames. Harness progress remains
        # visible on stderr without corrupting the protocol stream.
        options["file"] = sys.stderr
    stream = options.get("file") or sys.stdout
    encoding = getattr(stream, "encoding", None)

    def for_stream(value: object) -> str:
        text = scrub_secrets(value)
        if not encoding:
            return text
        try:
            return text.encode(encoding, errors="replace").decode(encoding)
        except LookupError:
            return text

    for name in ("sep", "end"):
        if name in options and options[name] is not None:
            options[name] = for_stream(options[name])
    try:
        builtins.print(*(for_stream(value) for value in values), **options)
    except (OSError, UnicodeEncodeError, ValueError):
        # Windows MCP stdio pipes can reject writes with errno 22. Progress
        # logging is best-effort and must not abort a Job.
        return


def isolated_subprocess_env(
    role: str,
    profile_dir: str | Path | None = None,
    source: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Build a case-insensitive role allowlist from an empty child environment."""
    if role not in ROLE_ENV_ALLOWLISTS:
        raise ValueError(f"unsupported subprocess role: {role}")
    source_env = os.environ if source is None else source
    indexed = {name.upper(): value for name, value in source_env.items()}
    allowed = _COMMON_WINDOWS_ENV + ROLE_ENV_ALLOWLISTS[role]
    result: dict[str, str] = {}
    for name in allowed:
        value = indexed.get(name)
        if isinstance(value, str):
            result[name] = value
    if role in _AGENT_ROLES and profile_dir:
        result["AGENT_PROFILE_DIR"] = str(Path(profile_dir).resolve())
    return result


def git_subprocess_env(
    index_file: str | Path | None = None,
    source: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Build a fresh local-Git environment, optionally for one temporary index."""
    result = isolated_subprocess_env("git", source=source)
    result["GIT_TERMINAL_PROMPT"] = "0"
    result["GIT_PAGER"] = "cat"
    if index_file is not None:
        resolved = Path(index_file).resolve()
        if resolved.exists() and resolved.is_dir():
            raise ValueError("Git index path must not be a directory")
        result["GIT_INDEX_FILE"] = str(resolved)
    return result


def trusted_executable(
    command: str,
    *,
    forbidden_root: str | Path | None = None,
    source: Mapping[str, str] | None = None,
    allow_project_absolute: bool = False,
) -> str:
    """Resolve a child executable without consulting the writable working dir.

    Interactive legacy entry points keep their existing behavior. The MCP BAT
    enables this fail-closed boundary with ``KKM_ENFORCE_TRUSTED_EXECUTABLES=1``.
    """
    environment = os.environ if source is None else source
    if str(environment.get("KKM_ENFORCE_TRUSTED_EXECUTABLES", "")) != "1":
        return command
    raw = Path(command)
    forbidden = Path(forbidden_root).resolve() if forbidden_root else None

    def acceptable(candidate: Path) -> str | None:
        lexical = Path(os.path.abspath(str(candidate)))
        if forbidden is not None and not allow_project_absolute:
            try:
                lexical.relative_to(forbidden)
            except ValueError:
                pass
            else:
                return None
        current = lexical
        while True:
            if _is_reparse_entry(current):
                return None
            if current.parent == current:
                break
            current = current.parent
        try:
            resolved = lexical.resolve(strict=True)
        except OSError:
            return None
        if not resolved.is_file():
            return None
        if forbidden is not None and not allow_project_absolute:
            try:
                resolved.relative_to(forbidden)
            except ValueError:
                pass
            else:
                return None
        if os.name != "nt" and not os.access(resolved, os.X_OK):
            return None
        return str(resolved)

    if raw.is_absolute():
        resolved = acceptable(raw)
        if resolved:
            return resolved
        raise FileNotFoundError(command)
    if raw.parent != Path("."):
        raise FileNotFoundError(command)

    path_value = str(environment.get("PATH", ""))
    extensions = [""]
    if os.name == "nt" and not raw.suffix:
        extensions = [
            item.casefold()
            for item in str(environment.get("PATHEXT", ".COM;.EXE;.BAT;.CMD")).split(";")
            if item
        ]
    for entry in path_value.split(os.pathsep):
        entry = entry.strip().strip('"')
        if not entry:
            continue
        directory = Path(entry)
        if not directory.is_absolute():
            continue
        for extension in extensions:
            resolved = acceptable(directory / f"{command}{extension}")
            if resolved:
                return resolved
    raise FileNotFoundError(command)
