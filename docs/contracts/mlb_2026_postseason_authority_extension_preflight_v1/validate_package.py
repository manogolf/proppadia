#!/usr/bin/env python3
"""Dependency-free deterministic validator for the read-only preflight."""

from __future__ import annotations

import csv
import hashlib
import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
PACKAGE = Path(__file__).resolve().parent
REQUIRED_ANCESTRY = (
    "6688e398ab7ac81587502206a89f265a02337e51",
    "58f7b096b132fc4deb75a1514fad1b1c2589e363",
    "4fe80cef754ea5a790b03f3a53636ba8919395b8",
)
STATIC_PACKAGE_FILES = (
    "README.md",
    "current_authority_dependency_manifest.csv",
    "extension_alternatives.csv",
    "consumer_impact_matrix.csv",
    "ordinary_retention_assessment.json",
    "proposed_activation_and_rollback_sequence.md",
    "no_action_until_postseason_checklist.md",
    "source_sha256_manifest.csv",
    "validate_package.py",
)


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def read_csv(name: str) -> list[dict[str, str]]:
    with (PACKAGE / name).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


checks: list[dict[str, object]] = []


def check(name: str, condition: bool, detail: object) -> None:
    checks.append({"check": name, "status": "PASS" if condition else "FAIL", "detail": detail})


for commit in REQUIRED_ANCESTRY:
    result = subprocess.run(
        ["git", "merge-base", "--is-ancestor", commit, "HEAD"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    check(f"required_ancestry:{commit}", result.returncode == 0, result.returncode)

tracked = subprocess.run(["git", "diff", "--quiet"], cwd=ROOT, check=False).returncode
staged = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=ROOT, check=False).returncode
check("tracked_worktree_unchanged", tracked == 0, tracked)
check("staging_area_clean", staged == 0, staged)

missing = [name for name in STATIC_PACKAGE_FILES if not (PACKAGE / name).is_file()]
check("static_package_files_present", not missing, missing)

from backend.mlb.season_transition.game_phase_authority_v1 import HashedProposalAuthority

authority = HashedProposalAuthority()
metadata = authority.metadata.to_dict()
check("authority_population", metadata["proposal_count"] == 2919, metadata["proposal_count"])
check(
    "authority_phase_counts",
    metadata["phase_counts"] == {"PRESEASON": 489, "REGULAR_SEASON": 2430},
    metadata["phase_counts"],
)
check("authority_has_zero_postseason", metadata["phase_counts"].get("POSTSEASON", 0) == 0, metadata["phase_counts"])
check(
    "authority_hashes",
    metadata["proposal_sha256"] == "b4f04273225643f691d438b492af8c36a40f2b63f62c34f60442261abc850879"
    and metadata["source_manifest_sha256"] == "766ea3ac7c230ea149e3189cd12b2070df645c27a16b86100143517d79456100"
    and metadata["authority_records_sha256"] == "5a7cdc460cc42ca2b4ed328c74e978b9da6967f95d7a7c4ba888b3b8d3401a84",
    {
        "proposal": metadata["proposal_sha256"],
        "source_manifest": metadata["source_manifest_sha256"],
        "records": metadata["authority_records_sha256"],
    },
)
check("authority_window", metadata["supported_through_date"] == "2026-09-27", metadata["supported_through_date"])

loader = (ROOT / "backend/mlb/season_transition/game_phase_authority_v1.py").read_text(encoding="utf-8")
builder = (ROOT / "backend/mlb/scripts/build_mlb_canonical_game_phase_backfill_v1.py").read_text(encoding="utf-8")
check(
    "loader_fixed_extension_blockers_observed",
    all(token in loader for token in ("EXPECTED_PROPOSAL_COUNT = 2919", "EXPECTED_SOURCE_FILE_COUNT = 464", "SUPPORTED_THROUGH_DATE = date(2026, 9, 27)")),
    "fixed count/source/window pins",
)
check("builder_directory_discovery_is_schedule_json_only", 'rglob("schedule.json")' in builder, "observed source")

assessment = json.loads((PACKAGE / "ordinary_retention_assessment.json").read_text(encoding="utf-8"))
check("preflight_used_zero_network", assessment["network_requests_made_by_preflight"] == 0, assessment["network_requests_made_by_preflight"])
check("preflight_used_zero_database", assessment["operational_database_connections_made_by_preflight"] == 0, assessment["operational_database_connections_made_by_preflight"])
check("preflight_used_zero_paid_credits", assessment["paid_credits_used_by_preflight"] == 0, assessment["paid_credits_used_by_preflight"])
check(
    "ordinary_request_claim_is_precise",
    assessment["ordinary_retention"]["additional_dedicated_statsapi_requests_required"] == 0
    and assessment["ordinary_retention"]["ordinary_public_statsapi_request_still_occurs"] is True,
    assessment["ordinary_retention"],
)

for row in assessment["observed_ordinary_sources"]:
    path = ROOT / row["path"]
    check(f"ordinary_source_hash:{row['path']}", path.is_file() and sha256(path) == row["sha256"], row["sha256"])
    if path.is_file():
        payload = json.loads(path.read_bytes())
        games = [game for block in payload.get("dates", []) for game in block.get("games", [])]
        observed_keys = set().union(*(game.keys() for game in games)) if games else set()
        check(f"ordinary_source_game_count:{row['path']}", len(games) == row["game_count"], len(games))
        check(
            f"ordinary_source_authoritative_fields:{row['path']}",
            {"gamePk", "gameType", "season", "gameDate", "seriesDescription"}.issubset(observed_keys),
            sorted(observed_keys),
        )

source_manifest = read_csv("source_sha256_manifest.csv")
source_failures = []
for row in source_manifest:
    path = ROOT / row["source_path"]
    if not path.is_file() or path.stat().st_size != int(row["bytes"]) or sha256(path) != row["sha256"]:
        source_failures.append(row["source_path"])
check("source_sha256_manifest", not source_failures, source_failures or len(source_manifest))

alternatives = read_csv("extension_alternatives.csv")
check("four_alternatives_compared", len(alternatives) == 4, len(alternatives))
check(
    "versioned_full_snapshot_is_unique_recommendation",
    [row["alternative"] for row in alternatives if row["decision"] == "RECOMMEND"] == ["immutable versioned full snapshots"],
    [row["decision"] for row in alternatives],
)

impact = read_csv("consumer_impact_matrix.csv")
expected_lanes = {
    "Moneyline", "Full-board Hits", "RAW Totals", "Totals C", "agreement study", "BvP",
    "player-prop/BetOnline identity", "Pinnacle", "feature lineage", "Ops Brief and daily index",
    "close inventory/checker", "training eligibility",
}
check("consumer_impact_complete", {row["lane"] for row in impact} == expected_lanes, sorted(row["lane"] for row in impact))

dependencies = read_csv("current_authority_dependency_manifest.csv")
check("dependency_manifest_bounded_and_complete", len(dependencies) >= 24, len(dependencies))
check(
    "close_and_agreement_exceptions_recorded",
    any(row["component"] == "regular-season close inventory/checker" and row["required_change"] for row in dependencies)
    and any(row["component"] == "agreement V4 acquisition" and "Separate" in row["required_change"] for row in dependencies),
    "close and agreement present",
)

readme = (PACKAGE / "README.md").read_text(encoding="utf-8")
required_phrases = (
    "immutable versioned full snapshot",
    "NO_NEW_AUTHORITY_EVIDENCE",
    "never silently fall back",
    "2,919 current proposal rows copied byte-for-byte",
    "ZERO_ADDITIONAL_DEDICATED_REQUESTS_EXPECTED_ZERO_PAID_CREDITS",
)
check("readme_records_core_decision", all(value in readme for value in required_phrases), required_phrases)

manifest_rows = []
for name in STATIC_PACKAGE_FILES:
    path = PACKAGE / name
    if path.is_file():
        manifest_rows.append({"path": name, "sha256": sha256(path), "bytes": path.stat().st_size})
manifest_text = "path,sha256,bytes\n" + "".join(
    f"{row['path']},{row['sha256']},{row['bytes']}\n" for row in manifest_rows
)
(PACKAGE / "sha256_manifest.csv").write_text(manifest_text, encoding="utf-8")
check("package_sha256_manifest_entries", len(manifest_rows) == len(STATIC_PACKAGE_FILES), len(manifest_rows))

report = {
    "contract": "MLB_2026_POSTSEASON_AUTHORITY_EXTENSION_PREFLIGHT_V1",
    "passed": sum(row["status"] == "PASS" for row in checks),
    "failed": sum(row["status"] == "FAIL" for row in checks),
    "skipped": 0,
    "checks": checks,
    "deterministic_report_sha256": hashlib.sha256(canonical_json(checks).encode("utf-8")).hexdigest(),
}
(PACKAGE / "validation_report.json").write_text(canonical_json(report) + "\n", encoding="utf-8")
print(json.dumps({key: report[key] for key in ("passed", "failed", "skipped", "deterministic_report_sha256")}, sort_keys=True))
raise SystemExit(0 if report["failed"] == 0 else 1)
