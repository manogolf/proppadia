#!/Users/jerrystrain/Projects/proppadia/.venv/bin/python
"""Validate the frozen close-blocker temporal coverage audit without rebuilding."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path


PACKAGE = Path(__file__).resolve().parent


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    failures = []
    manifest = PACKAGE / "sha256_manifest.txt"
    for line in manifest.read_text(encoding="utf-8").splitlines():
        expected, name = line.split("  ", 1)
        path = PACKAGE / name
        actual = sha256(path) if path.is_file() else "MISSING"
        if actual != expected:
            failures.append(f"SHA256:{name}")
    with (PACKAGE / "blocker_classification.csv").open(
        "r", encoding="utf-8", newline=""
    ) as handle:
        rows = list(csv.DictReader(handle))
    summary = json.loads((PACKAGE / "audit_summary.json").read_text(encoding="utf-8"))
    expected_counts = {
        "FUTURE_SCHEDULED": 72,
        "CURRENT_DATE_NOT_TERMINAL": 16,
        "PAST_DATE_TERMINAL_EVIDENCE_FOUND_ELSEWHERE": 348,
        "PAST_DATE_ONLY_STALE_NONTERMINAL_EVIDENCE": 0,
        "PAST_DATE_NO_RETAINED_TERMINAL_EVIDENCE": 0,
        "RELATED_GAME_DISPOSITION_REQUIRES_REVIEW": 0,
    }
    if len(rows) != 436 or len({row["game_pk"] for row in rows}) != 436:
        failures.append("BLOCKER_POPULATION")
    if summary["classification_counts"] != expected_counts:
        failures.append("CLASSIFICATION_COUNTS")
    if summary["decision"] != "CLOSE_BLOCKERS_INCLUDE_LOCAL_RECOVERIES":
        failures.append("DECISION")
    result = {
        "passed": not failures,
        "manifest_entries": len(manifest.read_text(encoding="utf-8").splitlines()),
        "blocker_rows": len(rows),
        "classification_counts": summary["classification_counts"],
        "failures": failures,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
