"""Write-once evidence, separated from historical Product receipts.

The store is not an authenticity authority. A caller's JSON does not attest
anything; records include explicit observation provenance and never Product PASS.
"""
from __future__ import annotations
from pathlib import Path
from dataclasses import asdict
from typing import Any
import hashlib
import os
import re
import tempfile
from .core import Binding, BoundaryError, canonical, require, strict_json, text

class ReceiptStore:
    def __init__(self, root: Path):
        # Explicit construction is the only I/O; do not select a path from globals.
        self.root = Path(root).resolve()
        self.root.mkdir(parents=True, exist_ok=True)
    def append(self, receipt_id: str, binding: Binding, *, kind: str, payload: dict[str, Any], provenance: str) -> dict:
        require(isinstance(receipt_id, str) and bool(re.fullmatch(r'[A-Za-z0-9_-]{1,128}', receipt_id)), 'RECEIPT_ID_INVALID')
        require(isinstance(binding,Binding),'RECEIPT_BINDING_REQUIRED')
        text(kind,'RECEIPT_KIND_INVALID',200)
        require(isinstance(payload, dict) and provenance in ('FIXTURE', 'HOST_OBSERVED', 'EXTERNAL_UNVERIFIED', 'OFFLINE_ADJUDICATED'), 'RECEIPT_PROVENANCE_INVALID')
        record = {'schema': 'harness11-receipt/1', 'receipt_id': receipt_id, 'binding': asdict(binding),
                  'kind': kind, 'payload': payload, 'provenance': provenance,
                  'product_pass': False, 'publication_authority': False}
        raw = canonical(record)
        require(len(raw) <= 2_097_152, 'RECEIPT_TOO_LARGE')
        final = self.root / (receipt_id + '.json')
        fd, name = tempfile.mkstemp(prefix='.pending-', dir=self.root)
        tmp = Path(name)
        try:
            with os.fdopen(fd, 'wb') as stream:
                stream.write(raw); stream.flush(); os.fsync(stream.fileno())
            try:
                # Hard-link publication is atomic AND never overwrites an existing receipt.
                os.link(tmp, final)
            except FileExistsError:
                require(not final.is_symlink() and final.read_bytes() == raw, 'RECEIPT_ID_COLLISION')
            if os.name != 'nt':
                dir_fd = os.open(self.root, os.O_RDONLY)
                try: os.fsync(dir_fd)
                finally: os.close(dir_fd)
        finally:
            tmp.unlink(missing_ok=True)
        return {'receipt_id': receipt_id, 'sha256': hashlib.sha256(raw).hexdigest(), 'path': str(final), 'product_pass': False}
    def read(self, receipt_id: str) -> dict:
        require(isinstance(receipt_id,str) and bool(re.fullmatch(r'[A-Za-z0-9_-]{1,128}', receipt_id)), 'RECEIPT_ID_INVALID')
        path=self.root/(receipt_id+'.json')
        require(not path.is_symlink(), 'RECEIPT_SYMLINK')
        with path.open('rb') as stream: raw=stream.read(2_097_153)
        return strict_json(raw, 2_097_152)
