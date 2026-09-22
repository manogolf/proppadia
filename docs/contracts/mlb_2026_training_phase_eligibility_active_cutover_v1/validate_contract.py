#!/Users/jerrystrain/Projects/proppadia/.venv/bin/python
"""Dependency-free verification of the active-cutover contract package."""

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

    validation = json.loads((PACKAGE / "validation_report.json").read_text())
    tests = validation["tests"]
    check(validation["status"] == "PASS", "aggregate validation passed")
    check(
        (tests["intended"], tests["executed"], tests["passed"]) == (40, 40, 40)
        and tests["failed"] == tests["skipped"] == tests["unexecuted"] == 0,
        "all intended assertions executed",
    )
    check(
        tests["model_artifacts_unchanged"]
        and tests["model_artifact_state_before"]
        == tests["model_artifact_state_after"],
        "model artifacts unchanged",
    )
    check(
        validation["safety"]
        == {
            "database_connections": 0,
            "model_fit_calls": 0,
            "model_train_commands": 0,
            "network_requests": 0,
            "operational_artifact_writes": 0,
            "paid_requests": 0,
            "synthetic_temporary_artifacts_only": True,
        },
        "zero fitting and operational writes",
    )
    frozen = json.loads((PACKAGE / "frozen_reproduction.json").read_text())
    result = frozen["authority_result"]
    check(
        result["input"] == {"rows": 600766, "distinct_game_pks": 2812}
        and result["admitted_regular_season"]
        == {"rows": 459604, "distinct_game_pks": 2341}
        and result["excluded_preseason"]["rows"] == 141162
        and result["excluded_preseason"]["distinct_game_pks"] == 471
        and result["missing_unknown_conflicting_duplicate"]
        == {"rows": 0, "distinct_game_pks": 0},
        "frozen counts exact",
    )
    check(
        frozen["retained_invariance"]["all_before_after_identical"],
        "retained hashes invariant",
    )
    with (PACKAGE / "source_evidence_manifest.csv").open(newline="") as handle:
        sources = list(csv.DictReader(handle))
    check(
        len(sources) == 6
        and all(
            (ROOT / row["path"]).stat().st_size == int(row["bytes"])
            and digest(ROOT / row["path"]) == row["sha256"]
            for row in sources
        ),
        "source evidence hashes",
    )
    input_contract = json.loads((PACKAGE / "input_manifest_contract.json").read_text())
    result_contract = json.loads((PACKAGE / "result_manifest_contract.json").read_text())
    check(
        not input_contract["fit_allowed_without_complete_verified_manifest"]
        and not result_contract["registration_allowed_without_complete_binding"]
        and not result_contract["certification_allowed_without_complete_binding"]
        and not result_contract["publication_allowed_without_complete_binding"],
        "lineage gates fail closed",
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
                "classification": "TRAINING_PHASE_ACTIVE_CUTOVER_READY",
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
