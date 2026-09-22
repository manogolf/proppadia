#!/usr/bin/env python3
"""Dependency-free deterministic validator for the review-only audit package."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import subprocess
from collections import Counter
from pathlib import Path


OUT = Path(__file__).resolve().parent
ROOT = OUT.parents[2]
REQUIRED_ANCESTRY = (
    "d362a872bd1555bb7b197d9a233e2ab606b18301",
    "19aff40e2b316b44a8c354709a5fe8655c37c77e",
    "4d9de0ee4d0eecaec9f1b949f2c73cd1c2c9bcee",
)
ALLOWED = {
    "READY_EXACT_GAME_PK",
    "READY_REPRODUCIBLE_PROVIDER_MAPPING",
    "CONDITIONAL_IDENTITY_NOT_PROVEN",
    "BLOCKED_DATE_OR_MATCHUP_ONLY",
    "BLOCKED_MISSING_OR_AMBIGUOUS_IDENTITY",
    "NOT_APPLICABLE",
}
REQUIRED_FILES = {
    "README.md",
    "lane_readiness.csv",
    "acquisition_identity_manifest.csv",
    "provider_event_to_game_pk_audit.csv",
    "ambiguous_missing_identity_ledger.csv",
    "bvp_exactly_once_assessment.md",
    "paid_acquisition_impact_assessment.md",
    "future_2027_use_classification.csv",
    "prioritized_correction_sequence.csv",
    "source_sha256_manifest.csv",
    "audit_summary.json",
    "build_review_evidence.py",
    "validate_package.py",
    "sha256_manifest.csv",
}


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def rows(name: str) -> list[dict[str, str]]:
    with (OUT / name).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def check_ancestry(commit: str) -> bool:
    result = subprocess.run(
        ["git", "merge-base", "--is-ancestor", commit, "HEAD"],
        cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    return result.returncode == 0


def synthetic_identity_checks() -> dict[str, bool]:
    from backend.mlb.markets.pinnacle_main_market_capture_v1 import bind_event as pinnacle_bind
    from backend.mlb.scripts.build_mlb_predictions_wide import _choose_game_for_event
    from backend.mlb.shared.mlb_api_v2 import GameLite

    game1 = GameLite(101, "2026-10-05", "2026-10-05T17:00:00+00:00", 2, 1, "HOME", "AWAY", None, None, "F")
    game2 = GameLite(102, "2026-10-05", "2026-10-05T23:00:00+00:00", 2, 1, "HOME", "AWAY", None, None, "F")
    schedule = [
        {"game_pk": 101, "away_team_name": "Away", "home_team_name": "Home", "scheduled_start_utc": "2026-10-05T17:00:00Z", "game_number": 1},
        {"game_pk": 102, "away_team_name": "Away", "home_team_name": "Home", "scheduled_start_utc": "2026-10-05T23:00:00Z", "game_number": 2},
    ]
    exact, exact_status, _ = pinnacle_bind(
        {"id": "synthetic-exact", "away_team": "Away", "home_team": "Home", "commence_time": "2026-10-05T23:00:00Z"},
        schedule, "2026-10-05T12:00:00Z",
    )
    ambiguous_schedule = [dict(schedule[0]), {**schedule[0], "game_pk": 103}]
    _, ambiguous_status, candidates = pinnacle_bind(
        {"id": "synthetic-ambiguous", "away_team": "Away", "home_team": "Home", "commence_time": "2026-10-05T17:00:00Z"},
        ambiguous_schedule, "2026-10-05T12:00:00Z",
    )
    rescheduled, rescheduled_status, _ = pinnacle_bind(
        {"id": "synthetic-rescheduled", "away_team": "Away", "home_team": "Home", "commence_time": "2026-10-06T01:00:00Z"},
        [{**schedule[1], "scheduled_start_utc": "2026-10-06T01:00:00Z"}], "2026-10-05T12:00:00Z",
    )
    return {
        "pinnacle_doubleheader_exact_start": exact_status == "CERTIFIED_EXACT_OR_DETERMINISTIC" and exact and exact["game_pk"] == 102,
        "pinnacle_duplicate_candidates_fail_closed": ambiguous_status == "AMBIGUOUS" and sorted(candidates) == [101, 103],
        "pinnacle_rescheduled_start_preserves_game_pk": rescheduled_status == "CERTIFIED_EXACT_OR_DETERMINISTIC" and rescheduled and rescheduled["game_pk"] == 102,
        "wide_invalid_time_first_game_defect_reproduced": _choose_game_for_event(pair_games=[game1, game2], commence_time="invalid") == game1,
        "wide_missing_time_first_game_defect_reproduced": _choose_game_for_event(pair_games=[game1, game2], commence_time=None) == game1,
    }


def validate() -> list[dict[str, object]]:
    checks: list[dict[str, object]] = []
    add = lambda name, ok, detail: checks.append({"check": name, "status": "PASS" if ok else "FAIL", "detail": detail})
    present = {path.name for path in OUT.iterdir() if path.is_file()}
    add("required_files", REQUIRED_FILES <= present, ",".join(sorted(REQUIRED_FILES - present)) or "all present")
    for commit in REQUIRED_ANCESTRY:
        add(f"ancestry_{commit[:12]}", check_ancestry(commit), commit)

    lanes = rows("lane_readiness.csv")
    add("lane_count", len(lanes) == 10, str(len(lanes)))
    classification_fields = [
        "raw_postseason_collection_readiness", "exact_game_pk_identity_integrity",
        "canonical_phase_join_readiness", "downstream_evaluation_state",
        "operational_postseason_readiness",
    ]
    invalid = sorted({row[field] for row in lanes for field in classification_fields if row[field] not in ALLOWED})
    add("lane_classifications_allowed", not invalid, "|".join(invalid) or "allowed")

    provider = rows("provider_event_to_game_pk_audit.csv")
    by_lane = Counter(row["lane"] for row in provider)
    pinnacle = [row for row in provider if row["lane"] == "PINNACLE_MAIN_MARKETS"]
    player = [row for row in provider if row["lane"] in {"BETONLINE_PLAYER_PROPS", "GENERAL_PLAYER_PROP_SNAPSHOT"}]
    add("provider_event_population", len(provider) == 2976, str(len(provider)))
    add("pinnacle_event_population", len(pinnacle) == 636, str(len(pinnacle)))
    add("pinnacle_all_single_game_pk", all(row["exact_game_pk"].isdigit() for row in pinnacle), str(sum(bool(row["exact_game_pk"]) for row in pinnacle)))
    add("player_prop_event_population", len(player) == 2340, str(len(player)))
    add("betonline_event_population", by_lane["BETONLINE_PLAYER_PROPS"] == 2211, str(by_lane["BETONLINE_PLAYER_PROPS"]))
    add("player_prop_binding_gap_explicit", all(not row["exact_game_pk"] and row["mapping_classification"] == "CONDITIONAL_IDENTITY_NOT_PROVEN" for row in player), "all provider event IDs fail closed in audit")

    missing = rows("ambiguous_missing_identity_ledger.csv")
    add("missing_binding_ledger_complete", len(missing) == 2340, str(len(missing)))
    add("pinnacle_no_conflicting_binding", not any(row["lane"] == "PINNACLE_MAIN_MARKETS" for row in missing), "zero")

    summary = json.loads((OUT / "audit_summary.json").read_text(encoding="utf-8"))
    add("zero_network_and_paid", summary["network_requests"] == summary["paid_requests"] == summary["paid_credits"] == 0, json.dumps({key: summary[key] for key in ("network_requests", "paid_requests", "paid_credits")}, sort_keys=True))
    add("authority_population", summary["phase_authority"] == {
        "authority_records_sha256": "5a7cdc460cc42ca2b4ed328c74e978b9da6967f95d7a7c4ba888b3b8d3401a84",
        "game_pks": 2919, "postseason": 0, "preseason": 489, "regular_season": 2430, "supported_through_date": "2026-09-27",
    }, json.dumps(summary["phase_authority"], sort_keys=True))
    add("bvp_exactly_once", summary["bvp_inline"]["successes"] == summary["bvp_inline"]["success_dates"] == 4 and summary["bvp_inline"]["duplicate_success_dates"] == 0, json.dumps(summary["bvp_inline"], sort_keys=True))
    add("lineage_exact_hash_population", summary["feature_lineage"]["rows"] == summary["feature_lineage"]["unique_canonical_row_identities"] == 126962 and summary["feature_lineage"]["rows_missing_feature_or_odds_snapshot_hash"] == 0, json.dumps(summary["feature_lineage"], sort_keys=True))

    for name, ok in synthetic_identity_checks().items():
        add(f"synthetic_{name}", ok, "synthetic fixture; not operational readiness")

    source_bad = []
    for row in rows("source_sha256_manifest.csv"):
        path = ROOT / row["path"]
        if not path.is_file() or digest(path) != row["sha256"] or path.stat().st_size != int(row["bytes"]):
            source_bad.append(row["path"])
    add("source_manifest", not source_bad, "|".join(source_bad) or "all source hashes match")

    package_bad = []
    manifest_path = OUT / "sha256_manifest.csv"
    if manifest_path.exists():
        manifest_rows = rows("sha256_manifest.csv")
        expected = sorted(
            path.name for path in OUT.iterdir()
            if path.is_file() and path.name not in {"sha256_manifest.csv", "validation_report.json"}
        )
        observed = sorted(row["path"] for row in manifest_rows)
        if expected != observed:
            package_bad.append("entry_set")
        for row in manifest_rows:
            path = OUT / row["path"]
            if not path.is_file() or digest(path) != row["sha256"] or path.stat().st_size != int(row["bytes"]):
                package_bad.append(row["path"])
    else:
        package_bad.append("missing")
    add("package_manifest", not package_bad, "|".join(package_bad) or "all package hashes match")
    return checks


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write-report", action="store_true")
    args = parser.parse_args()
    checks = validate()
    report = {
        "audit": "MLB_2026_POSTSEASON_COLLECTION_IDENTITY_READINESS_AUDIT_V1",
        "validation_timestamp_utc": "2026-09-22T22:37:47Z",
        "passed": sum(row["status"] == "PASS" for row in checks),
        "failed": sum(row["status"] == "FAIL" for row in checks),
        "skipped": 0,
        "checks": checks,
    }
    if args.write_report:
        (OUT / "validation_report.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
