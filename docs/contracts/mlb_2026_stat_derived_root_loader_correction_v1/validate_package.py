#!/usr/bin/env python3
"""Dependency-free validator for the inactive root-loader contract package."""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
from collections import Counter
from pathlib import Path


PACKAGE = Path(__file__).resolve().parent
ROOT = PACKAGE.parents[2]
MANIFEST = PACKAGE / "sha256_manifest.csv"
GENERATED = {
    "affected_key_ledger.csv",
    "contract.json",
    "legacy_quarantine_specification.csv",
    "offline_mutation_proposal.jsonl.gz",
    "summary.json",
}


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def package_files() -> list[Path]:
    return sorted(
        (path for path in PACKAGE.iterdir() if path.is_file() and path.name != MANIFEST.name),
        key=lambda path: path.name,
    )


def write_manifest() -> None:
    with MANIFEST.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(("path", "sha256"))
        for path in package_files():
            writer.writerow((path.name, sha256(path)))


def validate_manifest() -> None:
    rows = list(csv.DictReader(MANIFEST.open(newline="", encoding="utf-8")))
    expected = {path.name: sha256(path) for path in package_files()}
    actual = {row["path"]: row["sha256"] for row in rows}
    assert actual == expected, "SHA256_MANIFEST_MISMATCH"


def validate() -> dict[str, object]:
    summary = json.loads((PACKAGE / "summary.json").read_text(encoding="utf-8"))
    contract = json.loads((PACKAGE / "contract.json").read_text(encoding="utf-8"))
    classifications = json.loads((PACKAGE / "classifications.json").read_text(encoding="utf-8"))
    with gzip.open(PACKAGE / "offline_mutation_proposal.jsonl.gz", "rt", encoding="utf-8") as handle:
        proposals = [json.loads(line) for line in handle]
    assert GENERATED <= {path.name for path in package_files()}, "PACKAGE_FILE_MISSING"
    assert len(proposals) == summary["proposal_rows"] == 589
    assert Counter(row["operation"] for row in proposals) == Counter(summary["operation_counts"])
    assert Counter(row["relation"] for row in proposals) == Counter(summary["relation_counts"])
    assert Counter(str(row["authoritative_game_pk"]) for row in proposals) == Counter(summary["game_counts"])
    assert summary["operation_counts"] == {
        "INSERT_NEW_EXACT_FACT": 207,
        "QUARANTINE_CONFLICTING_LEGACY_IDENTITY": 49,
        "RELOCATE_MATCHING_MISDATED_FACT": 238,
        "UNPROVABLE_FAIL_CLOSED": 95,
    }
    assert summary["player_population"] == {
        "824784": 46,
        "824784_only": 6,
        "824785": 49,
        "824785_only": 9,
        "appeared_in_both": 40,
    }
    assert summary["database_connections"] == summary["database_writes"] == 0
    assert summary["api_requests"] == summary["pipeline_runs"] == 0
    assert summary["authorization_artifact_created"] is False
    assert contract["default_mode"] == "DRY_RUN_NO_MUTATION"
    assert contract["legacy_derived_writes"] is False
    assert classifications["OPERATIONAL_ACTIVATION_STATUS"] == "NOT_AUTHORIZED"
    assert classifications["STAT_DERIVED_RETRY_READINESS"] == "BLOCKED_PENDING_AUTHORIZED_RECONCILIATION"
    assert summary["separate_totals_defect"] == "OFFICIAL_FINAL_SOURCE_COUNT_824462_2"

    legacy = list(csv.DictReader((PACKAGE / "legacy_quarantine_specification.csv").open(newline="", encoding="utf-8")))
    assert len(legacy) == 49
    assert all(row["write_allowed"] == "false" for row in legacy)
    assert all(row["disposition"] == "QUARANTINE_CONFLICTING_LEGACY_IDENTITY" for row in legacy)

    loader = (ROOT / "backend/mlb/stat_derived_exact_game_loader_v1.py").read_text(encoding="utf-8")
    generator = (ROOT / "backend/mlb/scripts/build_mlb_stat_derived_root_loader_proposal_v1.py").read_text(encoding="utf-8")
    source = loader + generator
    for forbidden in ("MAX(game_id)", "INSERT INTO mlb.player_derived_stats", "UPDATE mlb.player_derived_stats", "DELETE FROM mlb.player_derived_stats", "requests.get", "psycopg"):
        assert forbidden not in source, f"FORBIDDEN_CORRECTED_PATH_PATTERN:{forbidden}"
    assert "BVP_INITIAL_SCHEDULE_WAKE_RETRY_V1" not in source
    validate_manifest()
    return {
        "status": "PASS",
        "proposal_rows": len(proposals),
        "manifest_files": len(package_files()),
        "database_writes": 0,
        "api_requests": 0,
        "operational_activation": "NOT_AUTHORIZED",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write-manifest", action="store_true")
    args = parser.parse_args()
    if args.write_manifest:
        write_manifest()
    print(json.dumps(validate(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
