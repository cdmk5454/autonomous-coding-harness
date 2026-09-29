"""Immutable Harness-owned contracts. External observations are never permits."""
from __future__ import annotations
from dataclasses import dataclass, asdict
from enum import Enum
from pathlib import Path, PurePosixPath
from typing import Any
import hashlib
import json
import math
import re

class BoundaryError(ValueError):
    def __init__(self, code: str):
        self.code = code
        super().__init__(code)  # Never include request/response/environment contents.

def require(condition: bool, code: str) -> None:
    if not condition:
        raise BoundaryError(code)

def text(value: Any, code: str = 'IDENTITY_INVALID', maximum: int = 200) -> str:
    require(isinstance(value, str) and 0 < len(value) <= maximum
            and value == value.strip() and not any(ord(c) < 32 for c in value), code)
    return value

def number(value: Any, minimum: float = 0, maximum: float = float('inf'), code='MEASUREMENT_INVALID') -> float:
    require(type(value) in (int, float) and math.isfinite(value)
            and minimum <= value <= maximum, code)
    return float(value)

def integer(value: Any, minimum: int = 0, maximum: int = 2**53, code='INTEGER_INVALID') -> int:
    require(type(value) is int and minimum <= value <= maximum, code)
    return value

def canonical(value: Any) -> bytes:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False).encode('utf-8')
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise BoundaryError('JSON_VALUE_INVALID') from None

def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value)).hexdigest()

def hash64(value: Any) -> str:
    require(isinstance(value, str) and bool(re.fullmatch('[a-f0-9]{64}', value)), 'HASH_INVALID')
    return value

def strict_json(raw: bytes | str, limit: int = 1_048_576) -> Any:
    require(isinstance(raw, (bytes, str)), 'JSON_INVALID')
    integer(limit, 1, 67_108_864, 'JSON_LIMIT_INVALID')
    try:
        encoded = raw.encode('utf-8') if isinstance(raw, str) else raw
    except UnicodeError:
        raise BoundaryError('JSON_INVALID') from None
    require(len(encoded) <= limit, 'RESPONSE_TOO_LARGE')
    def pairs(rows):
        result = {}
        for key, value in rows:
            require(key not in result, 'DUPLICATE_JSON_KEY')
            result[key] = value
        return result
    def finite_float(value):
        parsed = float(value)
        require(math.isfinite(parsed), 'NONFINITE_JSON')
        return parsed
    def unicode_strings(value):
        # json.loads accepts escaped lone surrogates; wire observations must
        # remain serializable as real UTF-8 throughout hashing/evidence paths.
        if isinstance(value, str):
            value.encode('utf-8')
        elif isinstance(value, dict):
            for key, item in value.items():
                key.encode('utf-8'); unicode_strings(item)
        elif isinstance(value, list):
            for item in value: unicode_strings(item)
    try:
        result = json.loads(encoded, object_pairs_hook=pairs,
            parse_float=finite_float,
            parse_constant=lambda _: (_ for _ in ()).throw(BoundaryError('NONFINITE_JSON')))
        unicode_strings(result)
        return result
    except BoundaryError:
        raise
    except (ValueError, UnicodeError, RecursionError):
        raise BoundaryError('JSON_INVALID') from None

class Mode(str, Enum):
    OFF = 'OFF'
    SHADOW = 'SHADOW'
    EXPLICIT = 'EXPLICIT'

class Risk(str, Enum):
    LOW = 'LOW'
    MID = 'MID'
    HIGH = 'HIGH'
    CRITICAL = 'CRITICAL'

@dataclass(frozen=True)
class Binding:
    job_id: str
    execution_id: str
    attempt_id: str
    candidate_id: str
    contract_hash: str
    candidate_hash: str
    diff_hash: str
    epoch: str
    role: str = 'WORKER'
    def __post_init__(self):
        for item in (self.job_id, self.execution_id, self.attempt_id, self.candidate_id, self.epoch):
            text(item)
        for item in (self.contract_hash, self.candidate_hash, self.diff_hash):
            hash64(item)
        require(self.role in ('WORKER', 'TESTER', 'REVIEWER'), 'ROLE_INVALID')
    @property
    def key(self) -> str:
        return digest(asdict(self))

@dataclass(frozen=True)
class ProcessIdentity:
    pid: int
    birth_id: str
    boundary_id: str
    provider: str
    runtime: str
    def __post_init__(self):
        integer(self.pid, 1)
        for item in (self.birth_id, self.boundary_id, self.provider, self.runtime):
            text(item)
    @property
    def key(self) -> str:
        return digest(asdict(self))

@dataclass(frozen=True)
class FenceProof:
    binding_key: str
    process_key: str
    token: int
    tree_quiesced: bool
    boundary_revoked: bool
    observer: str
    receipt_ref: str
    def __post_init__(self):
        hash64(self.binding_key); hash64(self.process_key); integer(self.token, 0)
        require(type(self.tree_quiesced) is bool and type(self.boundary_revoked) is bool, 'FENCE_BOOLEAN_REQUIRED')
        text(self.observer); text(self.receipt_ref, maximum=1000)
    def matches(self, binding: Binding, process: ProcessIdentity, token: int) -> bool:
        return (self.binding_key == binding.key and self.process_key == process.key and self.token == token
                and self.tree_quiesced and self.boundary_revoked)

@dataclass(frozen=True)
class Resource:
    """Logical resource IDs are normalized by Harness, not by model text.

    Path-like IDs are conservatively case-folded for the Windows target. Parent
    resources conflict with children. Include repo/build/port/DB in the namespace.
    """
    key: str
    access: str = 'WRITE'
    def __post_init__(self):
        text(self.key, 'RESOURCE_INVALID', 500)
        normalized = self.key.replace('\\', '/').casefold()
        require(not normalized.startswith('/') and ':' not in normalized and
                all(part not in ('', '.', '..') for part in normalized.split('/')) and
                not any(c in normalized for c in '*?[]\x00'), 'RESOURCE_NOT_CANONICAL')
        require(self.access in ('READ', 'WRITE'), 'RESOURCE_ACCESS_INVALID')
        object.__setattr__(self, 'key', normalized)
    def conflicts(self, other: Resource) -> bool:
        overlap = (self.key == other.key or self.key.startswith(other.key + '/') or other.key.startswith(self.key + '/'))
        return overlap and ('WRITE' in (self.access, other.access))

@dataclass(frozen=True)
class Scope:
    candidate_root: str
    canonical_root: str
    write_paths: tuple[str, ...]
    resources: tuple[Resource, ...]
    def __post_init__(self):
        require(type(self.write_paths) is tuple and type(self.resources) is tuple
                and self.write_paths and self.resources, 'EXPLICIT_SCOPE_REQUIRED')
        candidate = Path(self.candidate_root).resolve()
        canonical_root = Path(self.canonical_root).resolve()
        require(candidate.is_dir() and canonical_root.is_dir(), 'SCOPE_DIRECTORY_MISSING')
        a=str(candidate).replace('\\','/').casefold().rstrip('/');b=str(canonical_root).replace('\\','/').casefold().rstrip('/')
        require(a!=b and not a.startswith(b+'/') and not b.startswith(a+'/'), 'CANONICAL_CANDIDATE_OVERLAP')
        require(len(set(p.replace('\\','/').casefold() for p in self.write_paths))==len(self.write_paths),'DUPLICATE_WRITE_PATH')
        for path in self.write_paths:
            text(path, 'WRITE_PATH_INVALID', 500)
            p = PurePosixPath(path.replace('\\', '/'))
            require(not p.is_absolute() and all(x not in ('', '.', '..') for x in path.replace('\\','/').split('/'))
                    and ':' not in path and not any(c in path for c in '*?[]'), 'WRITE_PATH_INVALID')
            require((candidate / str(p)).resolve().is_relative_to(candidate), 'WRITE_SCOPE_ESCAPE')
        require(all(isinstance(r, Resource) for r in self.resources), 'RESOURCE_INVALID')
        object.__setattr__(self, 'candidate_root', str(candidate))
        object.__setattr__(self, 'canonical_root', str(canonical_root))
    @property
    def fingerprint(self):
        return digest(asdict(self))

@dataclass(frozen=True)
class FeatureConfig:
    nworker: Mode = Mode.OFF
    pi: Mode = Mode.OFF
    advisor: Mode = Mode.OFF
    evaluation: Mode = Mode.OFF
    jev: Mode = Mode.SHADOW
    serena: Mode = Mode.OFF
    repomix: Mode = Mode.OFF
    alibaba_review: Mode = Mode.SHADOW
    security_audit: Mode = Mode.OFF
    context7: Mode = Mode.OFF
    chrome_devtools: Mode = Mode.OFF
    retrieval: Mode = Mode.OFF
    external_calls_authorized: bool = False
    def __post_init__(self):
        for key, value in asdict(self).items():
            if key == 'external_calls_authorized':
                require(type(value) is bool, 'FEATURE_CONFIG_INVALID')
            else:
                require(isinstance(value, Mode), 'FEATURE_CONFIG_INVALID')
        require(self.jev in (Mode.OFF, Mode.SHADOW) and self.alibaba_review in (Mode.OFF, Mode.SHADOW), 'ADVISORY_PROMOTION_UNSUPPORTED')
    def public(self):
        return {key: value.value if isinstance(value, Mode) else value for key, value in asdict(self).items()}

@dataclass(frozen=True)
class PolicySnapshot:
    risk: Risk
    required_controls: frozenset[str]
    approved_routes: tuple[str, ...]
    actual_route: str
    actual_needs_supervisor: bool
    policy_hash: str
    def __post_init__(self):
        require(isinstance(self.risk, Risk) and type(self.required_controls) is frozenset
                and type(self.approved_routes) is tuple, 'POLICY_INVALID')
        require(bool(self.approved_routes) and self.actual_route in self.approved_routes, 'ROUTE_NOT_APPROVED')
        for x in (*self.approved_routes, *self.required_controls): text(x)
        require(type(self.actual_needs_supervisor) is bool, 'POLICY_INVALID')
        hash64(self.policy_hash)
        # Controls must arrive from the frozen existing policy. Never synthesize a lower floor.
        require(self.risk not in (Risk.HIGH, Risk.CRITICAL) or bool(self.required_controls), 'RISK_FLOOR_MISSING')
