#!/usr/bin/env python3
"""Offline validator for MLB stat-derived degraded-stage containment V1."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path

from backend.mlb.scripts.contain_mlb_stat_derived_failure import (
    FINGERPRINT_CLASSIFIER_VERSION,
    KNOWN_FAILURE_CLASSIFICATION,
    failure_fingerprint,
)


CORRECTION_ROOT = Path(
    "artifacts/analysis/mlb/operational_reconciliation/2026-09-24/"
    "stat_derived_containment_receipt_fingerprint_correction_v1"
)
HISTORICAL_RECEIPT = Path(
    "artifacts/ops/mlb_stat_derived_degraded_containment_v1/2026-09-24/"
    "local_daily_20260924T180004Z.json"
)
HISTORICAL_RECEIPT_SHA256 = "3a6a28105d1ac93cf0c26a5472b1f7a34fd2a8c59de2609456034e9f187924ed"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--wrapper", type=Path, default=Path("/Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh"))
    parser.add_argument("--expected-wrapper-sha256", required=True)
    parser.add_argument(
        "--manifest",
        type=Path,
        default=CORRECTION_ROOT / "sha256_manifest.csv",
    )
    args = parser.parse_args()

    contract_path = Path("backend/mlb/contracts/mlb_stat_derived_degraded_stage_containment_v1.json")
    module_path = Path("backend/mlb/scripts/contain_mlb_stat_derived_failure.py")
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    wrapper = args.wrapper.read_text(encoding="utf-8")
    module = module_path.read_text(encoding="utf-8")
    operational = wrapper[wrapper.index('MLB_STAT_DERIVED_FAILURE_BOUNDARY="'):]
    fixture_root = Path("backend/tests/fixtures/mlb_stat_derived_containment_v1")
    natural_stdout_fixture = fixture_root / "2026-09-24_local_daily_20260924T180004Z.stdout.txt"
    natural_stderr_fixture = fixture_root / "2026-09-24_local_daily_20260924T180004Z.stderr.txt"
    natural = failure_fingerprint(
        natural_stdout_fixture,
        natural_stderr_fixture,
        2,
    )
    retained_stdout = Path("artifacts/ops/mlb_refresh_daily.out.log").read_text(
        encoding="utf-8", errors="replace"
    )
    retained_stderr = Path("artifacts/ops/mlb_refresh_daily.err.log").read_text(
        encoding="utf-8", errors="replace"
    )
    natural_boundary = "MLB_STAT_DERIVED_STAGE_BOUNDARY run_identity=local_daily_20260924T180004Z"
    retained_stderr_after_boundary = (
        retained_stderr.rsplit(natural_boundary, 1)[1]
        if natural_boundary in retained_stderr
        else ""
    )

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
        "per_run_stream_root": "artifacts/ops/mlb_stat_derived_stage_output_v1/${MLB_DATE_ET}/${MLB_RUN_TAG}" in operational,
        "stdout_stream_capture": '> "$MLB_STAT_DERIVED_STDOUT_LOG" >&$MLB_STAT_DERIVED_LIVE_STDOUT_FD' in operational,
        "stderr_stream_capture": '2> "$MLB_STAT_DERIVED_STDERR_LOG" 2>&$MLB_STAT_DERIVED_LIVE_STDERR_FD' in operational,
        "live_stream_fds_closed": all(
            token in operational
            for token in (
                'exec {MLB_STAT_DERIVED_LIVE_STDOUT_FD}>&-',
                'exec {MLB_STAT_DERIVED_LIVE_STDERR_FD}>&-',
            )
        ),
        "zsh_multios_explicit": "setopt MULTIOS" in operational,
        "restricted_stream_directory": 'chmod 700 "$MLB_STAT_DERIVED_STREAM_ROOT"' in operational and "umask 077" in operational,
        "classifier_receives_both_streams": all(
            token in operational
            for token in (
                '--failure-stdout-log "$MLB_STAT_DERIVED_STDOUT_LOG"',
                '--failure-stderr-log "$MLB_STAT_DERIVED_STDERR_LOG"',
            )
        ),
        "legacy_shared_log_not_used": "--failure-log" not in operational,
        "original_exit_preserved": 'exit "$MLB_STAT_DERIVED_RC"' in wrapper,
        "exit_trap_unchanged": "trap 'wrapper_rc=$?; release_launchagent_locks; write_launchagent_summary \"$wrapper_rc\"; exit \"$wrapper_rc\"' EXIT" in wrapper,
        "bvp_invocation_unchanged_once": wrapper.count('bin/mlb_bvp_inline_daily_hook.sh "$MLB_DATE_ET" "$MLB_RUN_TAG"') == 1,
        "ordinary_full_game_totals_once": wrapper.count('bin/mlb_full_game_totals_daily_hook.sh "$MLB_DATE_ET" "$MLB_RUN_TAG"') == 1,
        "ordinary_totals_prospective_once": wrapper.count("bin/mlb_totals_prospective_shadow_daily_hook.sh") == 1,
        "known_defect_visible": contract["known_defect"] == "POSTPONED_AS_FINAL_ADMISSION_PLUS_UNORDERED_DELETE_INSERT_CTE",
        "fingerprint_contract_v2": contract["receipt_fingerprint_contract"]["version"] == FINGERPRINT_CLASSIFIER_VERSION,
        "natural_fixture_classified": natural["classification"] == KNOWN_FAILURE_CLASSIFICATION,
        "natural_fixture_boundary": natural["boundary_found"] is True,
        "natural_fixture_identity": natural["player_id"] == 453286 and natural["game_id"] == 824785,
        "natural_fixture_stdout_is_retained_exact_excerpt": natural_stdout_fixture.read_text(encoding="utf-8") in retained_stdout,
        "natural_fixture_stderr_is_retained_after_run_boundary": natural_stderr_fixture.read_text(encoding="utf-8") in retained_stderr_after_boundary,
        "historical_receipt_unchanged": HISTORICAL_RECEIPT.exists() and sha256(HISTORICAL_RECEIPT) == HISTORICAL_RECEIPT_SHA256,
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
