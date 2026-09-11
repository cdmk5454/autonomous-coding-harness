"""Run the public deterministic evaluation corpus without an LLM or provider."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from control_repository import SCHEMA_VERSION
from harness_eval import environment_manifest, run_corpus


def main() -> int:
    environment = environment_manifest(
        harness_version=(ROOT / "VERSION").read_text(encoding="utf-8").strip(),
        control_schema_revision=SCHEMA_VERSION,
        runtime_contract={
            "runtime": "synthetic-fixture",
            "runtime_version": "1",
            "protocol_revision": "fixture/1",
            "permission_tool_profile": "fixture-readonly/1",
        },
        source_view={"source_view_id": "SV-PUBLIC-EVAL", "base_manifest_hash": "synthetic"},
        execution_budget={"product_retry": 3, "runtime_recovery": 1},
    )
    result = run_corpus(ROOT / "evals" / "corpus_099.json", environment=environment)
    print(f"{result['passed']}/{result['total']} PASS")
    print(f"release_gate={result['release_gate']}")
    return 0 if result["release_gate"] == "PASS" and result["passed"] == result["total"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
