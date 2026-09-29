"""Assemble the release verification bundle for independent recheck.

0.99.2.2 completion patch: the verification bundle previously stopped at the
prior release because no collector tracked the current one. This collector is
version-driven — it reads ``VERSION``, requires the matching
``releases/<version>/`` manifests, and copies the verification-critical
artifacts into a self-describing, hash-bound bundle:

    VERSION
    docs/PATCH_<version>.md
    tests/<focused hotfix test>.py
    releases/<version>/runtime-manifest.json
    releases/<version>/SHA256SUMS.json
    verification-manifest.json  (kind/title/version/build_id + per-file sha256)

Historical release artifacts are never modified. Failures (missing file,
version mismatch, hash mismatch) abort the collection loudly.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from datetime import datetime
from pathlib import Path

BUNDLE_SCHEMA_VERSION = 1


class VerificationBundleError(RuntimeError):
    def __init__(self, code: str, detail: str = ""):
        self.code = code
        self.detail = detail[:300]
        super().__init__(code + (f": {self.detail}" if detail else ""))


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def collect(
    harness_root: str | Path | None = None,
    output_root: str | Path | None = None,
) -> dict:
    root = Path(harness_root).resolve() if harness_root else Path(__file__).resolve().parent
    version_path = root / "VERSION"
    if not version_path.is_file():
        raise VerificationBundleError("VERSION_MISSING", str(version_path))
    version = version_path.read_text(encoding="utf-8").strip()
    if not version:
        raise VerificationBundleError("VERSION_EMPTY")

    release_dir = root / "releases" / version
    runtime_manifest_path = release_dir / "runtime-manifest.json"
    sums_path = release_dir / "SHA256SUMS.json"
    patch_doc_path = root / "docs" / f"PATCH_{version}.md"
    focused_test_path = root / "test_09922_realwork_hotfix.py"

    # Bundle layout mirrors the repository-external verification convention:
    # the focused test rides under tests/, release evidence under releases/.
    entries = [
        (version_path, "VERSION"),
        (patch_doc_path, f"docs/PATCH_{version}.md"),
        (focused_test_path, "tests/test_09922_realwork_hotfix.py"),
        (runtime_manifest_path, f"releases/{version}/runtime-manifest.json"),
        (sums_path, f"releases/{version}/SHA256SUMS.json"),
    ]
    for source, _ in entries:
        if not source.is_file():
            raise VerificationBundleError("BUNDLE_FILE_MISSING", str(source))

    try:
        runtime_manifest = json.loads(runtime_manifest_path.read_text(encoding="utf-8"))
        sums_manifest = json.loads(sums_path.read_text(encoding="utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise VerificationBundleError("RELEASE_MANIFEST_INVALID", version) from exc
    if str(runtime_manifest.get("version", "")) != version:
        raise VerificationBundleError("RUNTIME_MANIFEST_VERSION_MISMATCH", version)
    if str(sums_manifest.get("version", "")) != version:
        raise VerificationBundleError("SHA_MANIFEST_VERSION_MISMATCH", version)
    build_id = str(runtime_manifest.get("build_id") or "")

    destination_root = (
        Path(output_root).resolve() if output_root
        else root / "artifacts" / "verification-bundles" / version
    )
    bundle_dir = destination_root / f"verification-bundle-{version}"
    if bundle_dir.exists():
        shutil.rmtree(bundle_dir)

    records = []
    for source, relative in entries:
        target = bundle_dir / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
        copied = _sha256(target)
        if copied != _sha256(source):
            raise VerificationBundleError("BUNDLE_COPY_HASH_MISMATCH", relative)
        records.append({
            "bundle_path": relative,
            "source_path": str(source.relative_to(root)).replace("\\", "/"),
            "sha256": copied,
        })

    manifest = {
        "schema_version": BUNDLE_SCHEMA_VERSION,
        "kind": "HarnessVerificationBundle",
        "title": f"{version} Verification Bundle",
        "version": version,
        "build_id": build_id,
        "control_schema_version": int(runtime_manifest.get("control_schema_version", 0) or 0),
        "generated_at": datetime.now().astimezone().isoformat(),
        "entries": records,
    }
    manifest_path = bundle_dir / "verification-manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8",
    )
    return {
        "version": version,
        "build_id": build_id,
        "bundle_dir": str(bundle_dir),
        "manifest_path": str(manifest_path),
        "manifest_sha256": _sha256(manifest_path),
        "files": len(records),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Assemble the release verification bundle")
    parser.add_argument("--output", dest="output_root", default=None,
                        help="alternative bundle output root (default: artifacts/verification-bundles/<version>)")
    args = parser.parse_args(argv)
    try:
        result = collect(output_root=args.output_root)
    except VerificationBundleError as exc:
        print(f"[VERIFICATION-BUNDLE] {exc.code}: {exc.detail}")
        return 1
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
