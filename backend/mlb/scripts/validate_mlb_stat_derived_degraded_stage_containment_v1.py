#!/usr/bin/env python3
"""Offline validator for MLB stat-derived degraded-stage containment V1."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--wrapper", type=Path, default=Path("/Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh"))
    parser.add_argument("--expected-wrapper-sha256", required=True)
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("artifacts/analysis/mlb/operational_reconciliation/2026-09-24/stat_derived_degraded_stage_containment_v1/sha256_manifest.csv"),
    )
    args = parser.parse_args()

    contract_path = Path("backend/mlb/contracts/mlb_stat_derived_degraded_stage_containment_v1.json")
    module_path = Path("backend/mlb/scripts/contain_mlb_stat_derived_failure.py")
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    wrapper = args.wrapper.read_text(encoding="utf-8")
    module = module_path.read_text(encoding="utf-8")
    operational = wrapper[wrapper.index('MLB_STAT_DERIVED_FAILURE_BOUNDARY="'):]

    stages = contract["stages_after_failure"]
    independent = [row["name"] for row in stages if row["classification"] == "PROVEN_INDEPENDENT"]
    dependent = [row["name"] for row in stages if row["classification"] == "TRUE_DATA_DEPENDENCY"]
    unknown = [row["name"] for row in stages if row["classification"] == "UNKNOWN_REQUIRES_PROOF"]
    manifest_rows = list(csv.DictReader(args.manifest.open(encoding="utf-8")))
    manifest_failures = []
    for row in manifest_rows:
        manifest_path = Path(row["path"])
        if not manifest_path.exists() or sha256(manifest_path) != row["sha256"]:
            manifest_failures.append(row["path"])

    checks = {
        "wrapper_hash": sha256(args.wrapper) == args.expected_wrapper_sha256,
        "single_stat_refresh": operational.count("make mlb-stat-derived-refresh") == 1,
        "single_containment_entry": operational.count("backend.mlb.scripts.contain_mlb_stat_derived_failure") == 1,
        "failure_boundary_before_stat": operational.index("MLB_STAT_DERIVED_STAGE_BOUNDARY") < operational.index("make mlb-stat-derived-refresh"),
        "containment_after_stat": operational.index("make mlb-stat-derived-refresh") < operational.index("backend.mlb.scripts.contain_mlb_stat_derived_failure"),
        "original_exit_preserved": 'exit "$MLB_STAT_DERIVED_RC"' in wrapper,
        "exit_trap_unchanged": "trap 'wrapper_rc=$?; release_launchagent_locks; write_launchagent_summary \"$wrapper_rc\"; exit \"$wrapper_rc\"' EXIT" in wrapper,
        "bvp_invocation_unchanged_once": wrapper.count('bin/mlb_bvp_inline_daily_hook.sh "$MLB_DATE_ET" "$MLB_RUN_TAG"') == 1,
        "ordinary_full_game_totals_once": wrapper.count('bin/mlb_full_game_totals_daily_hook.sh "$MLB_DATE_ET" "$MLB_RUN_TAG"') == 1,
        "ordinary_totals_prospective_once": wrapper.count("bin/mlb_totals_prospective_shadow_daily_hook.sh") == 1,
        "known_defect_visible": contract["known_defect"] == "POSTPONED_AS_FINAL_ADMISSION_PLUS_UNORDERED_DELETE_INSERT_CTE",
        "two_command_allowlist": independent == ["full-game-totals-daily-hook", "totals-prospective-shadow-daily-hook", "wrapper-summary-and-lock-release"],
        "dependent_stages_present": len(dependent) == 10,
        "unknown_stages_present": len(unknown) == 2,
        "no_direct_provider_client": all(token not in module for token in ("requests.get(", "requests.post(", "urllib.request", "httpx.")),
        "no_database_client": all(token not in module for token in ("psycopg.connect", "pg_connect(", "DATABASE_URL", "SUPABASE_DB_URL")),
        "sha256_manifest": not manifest_failures,
    }
    failed = sorted(name for name, passed in checks.items() if not passed)
    result = {
        "contract": contract["contract"],
        "status": "PASS" if not failed else "FAIL",
        "checks": checks,
        "failed": failed,
        "manifest_failures": manifest_failures,
        "wrapper_sha256": sha256(args.wrapper),
        "stage_counts": {
            "true_data_dependency": len(dependent),
            "proven_independent": len(independent),
            "unknown_requires_proof": len(unknown),
        },
        "live_api_calls": 0,
        "database_connections": 0,
        "pipeline_runs": 0,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
