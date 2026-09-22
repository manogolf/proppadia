#!/Users/jerrystrain/Projects/proppadia/.venv/bin/python
"""Read-only deterministic validation of the compact dry-run package."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path


PACKAGE = Path(__file__).resolve().parent
ROOT = PACKAGE.parents[2]


def digest(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def main() -> int:
    checks: list[str] = []

    def check(condition: bool, label: str) -> None:
        if not condition:
            raise AssertionError(label)
        checks.append(label)

    report = json.loads((PACKAGE / "dry_run_report.json").read_text())
    gate = report["gate_report"]
    check(report["status"] == "PASS", "dry-run status")
    check(
        (gate["input_row_count"], gate["input_distinct_game_pk_count"])
        == (600766, 2812),
        "input counts",
    )
    check(
        (gate["admitted_row_count"], gate["admitted_distinct_game_pk_count"])
        == (459604, 2341),
        "admitted counts",
    )
    check(
        gate["decision_row_counts"]["EXCLUDED_PRESEASON"] == 141162
        and len(gate["decision_game_pks"]["EXCLUDED_PRESEASON"]) == 471,
        "excluded counts",
    )
    check(gate["blocked_row_count"] == 0, "zero blocked rows")
    check(
        report["invariance"]["all_retained_hashes_identical"]
        and all(
            item["before_sha256"] == item["after_sha256"]
            for item in report["invariance"]["hashes"].values()
        ),
        "retained hashes invariant",
    )
    check(
        all(
            value == 0
            for key, value in report["safety"].items()
            if key != "review_evidence_writes"
        )
        and report["safety"]["review_evidence_writes"] == 2,
        "zero fitting and operational writes",
    )
    with (PACKAGE / "excluded_game_pks.csv").open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    check(
        len(rows) == 471
        and len({row["game_pk"] for row in rows}) == 471
        and sum(int(row["row_count"]) for row in rows) == 141162
        and {row["authoritative_phase"] for row in rows} == {"PRESEASON"},
        "exclusion ledger",
    )
    tests = json.loads((PACKAGE / "test_report.json").read_text())
    check(
        tests["status"] == "PASS"
        and tests["tests"]["passed"] == 15
        and tests["tests"]["failed"] == 0
        and tests["tests"]["skipped"] == 0
        and tests["tests"]["unexecuted"] == 0,
        "dependency-free tests",
    )
    with (PACKAGE / "source_evidence_manifest.csv").open(newline="") as handle:
        sources = list(csv.DictReader(handle))
    check(
        len(sources) == 5
        and all(
            (ROOT / row["path"]).stat().st_size == int(row["bytes"])
            and digest(ROOT / row["path"]) == row["sha256"]
            for row in sources
        ),
        "source evidence hashes",
    )
    manifest = [
        line.split("  ", 1)
        for line in (PACKAGE / "sha256_manifest.txt").read_text().splitlines()
        if line
    ]
    expected = sorted(
        path.relative_to(PACKAGE).as_posix()
        for path in PACKAGE.rglob("*")
        if path.is_file() and path.name != "sha256_manifest.txt"
    )
    check(sorted(relative for _, relative in manifest) == expected, "manifest coverage")
    check(
        all(digest(PACKAGE / relative) == expected_hash for expected_hash, relative in manifest),
        "manifest hashes",
    )
    print(
        json.dumps(
            {
                "status": "PASS",
                "checks_passed": len(checks),
                "checks_failed": 0,
                "checks": checks,
                "classification": "TRAINING_PHASE_ELIGIBILITY_DRY_RUN_READY",
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(json.dumps({"status": "FAIL", "failure": str(exc)}, indent=2, sort_keys=True))
        raise SystemExit(1)
