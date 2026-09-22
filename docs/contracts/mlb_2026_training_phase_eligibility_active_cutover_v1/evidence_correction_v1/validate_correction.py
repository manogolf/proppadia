#!/Users/jerrystrain/Projects/proppadia/.venv/bin/python
"""Dependency-free verification of the evidence-correction package."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path


PACKAGE = Path(__file__).resolve().parent
ROOT = PACKAGE.parents[3]
PARENT = PACKAGE.parent


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

    correction = json.loads((PACKAGE / "validation_report.json").read_text())
    tests = correction["tests"]
    metadata = correction["artifact_metadata_monitor"]
    reconciliation = correction["artifact_population_reconciliation"]
    claims = correction["evidence_correction"]
    check(correction["status"] == "PASS", "correction validation passed")
    check(
        (tests["intended"], tests["executed"], tests["passed"]) == (47, 47, 47)
        and tests["failed"] == tests["skipped"] == tests["unexecuted"] == 0,
        "all assertions executed",
    )
    check(
        metadata["artifact_path_count"] == 538
        and metadata["extension_counts"] == {".joblib": 534, ".pkl": 4}
        and metadata["nhl_path_count"] == 0
        and metadata["inode_identity_count"] == 536
        and len(metadata["hard_linked_path_groups"]) == 2,
        "corrected MLB population",
    )
    check(
        reconciliation["original_monitor_path_count"] == 444
        and reconciliation["original_monitor_mlb_path_count"] == 428
        and reconciliation["original_monitor_nhl_path_count"] == 16
        and reconciliation["mlb_paths_omitted_by_original_monitor_count"] == 110,
        "original monitor reconciled",
    )
    check(
        tests["model_metadata_unchanged"]
        and metadata["identity_semantics"]
        == "METADATA_IDENTITY_ONLY_NOT_BYTE_IDENTITY"
        and metadata["model_content_hashes_computed"] == 0
        and metadata["byte_identity_proven"] is False,
        "metadata claim bounded",
    )
    check(
        claims["original_report_preserved"]
        and claims["original_444_claim_superseded"]
        and not claims["evidence_of_model_mutation"]
        and not claims["exhaustive_538_file_byte_identity_proven"]
        and not claims["eligibility_and_lineage_validation_affected"]
        and not claims["existing_model_lineage_classifications_changed"],
        "supersession claims",
    )
    supersession = json.loads((PACKAGE / "supersession_record.json").read_text())
    for key in ("original_validation_report", "original_scheduler_and_safety_audit"):
        item = supersession["superseded_evidence"][key]
        path = ROOT / item["path"]
        check(
            path.stat().st_size == item["bytes"] and digest(path) == item["sha256"],
            f"preserved {key}",
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
    manifest = [
        line.split("  ", 1)
        for line in (PACKAGE / "sha256_manifest.txt").read_text().splitlines()
        if line
    ]
    expected = sorted(
        path.relative_to(PACKAGE).as_posix()
        for path in PACKAGE.iterdir()
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
                "classification": "ACTIVE_CUTOVER_EVIDENCE_CORRECTED",
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
