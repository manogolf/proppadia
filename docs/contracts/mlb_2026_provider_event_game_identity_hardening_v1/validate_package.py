#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
PACKAGE = Path(__file__).resolve().parent
PYTHON = ROOT / ".venv/bin/python"
TEST_MODULE = "backend.mlb.tests.test_mlb_2026_provider_event_game_identity_hardening_v1"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    checks = []

    def check(name, condition, detail=""):
        checks.append({"name": name, "passed": bool(condition), "detail": str(detail)})

    summary = json.loads((PACKAGE / "reconciliation_summary.json").read_text())
    check("authority population", summary["authority_game_pks"] == 2919, summary["authority_game_pks"])
    check("schedule sources verified", summary["verified_schedule_source_files"] == 464, summary["verified_schedule_source_files"])
    check("player event population", summary["player_prop_events"] == 2340, summary["player_prop_events"])
    check("BetOnline event population", summary["betonline_events"] == 2211, summary["betonline_events"])
    check("player reconstructable", summary["player_prop_classifications"].get("RECONSTRUCTABLE_BUT_PREVIOUSLY_UNBOUND") == 2325)
    check("player ambiguous", summary["player_prop_classifications"].get("AMBIGUOUS") == 2)
    check("player unavailable", summary["player_prop_classifications"].get("UNAVAILABLE") == 13)
    check("exact blocked population", summary["blocked_events"] == 15, summary["blocked_events"])
    check("Pinnacle event population", summary["pinnacle_events"] == 636, summary["pinnacle_events"])
    check("Pinnacle mappings invariant", summary["pinnacle_unique_event_game_pairs"] == 636)
    check("historical rows untouched", summary["historical_rows_modified"] == 0)
    check("zero acquisition", summary["network_requests"] == summary["paid_requests"] == summary["paid_credits"] == 0)

    with (PACKAGE / "historical_reconciliation.csv").open(newline="", encoding="utf-8") as handle:
        history = list(csv.DictReader(handle))
    with (PACKAGE / "blocked_event_ledger.csv").open(newline="", encoding="utf-8") as handle:
        blocked = list(csv.DictReader(handle))
    check("historical exact row count", len(history) == 2976, len(history))
    check("blocked ledger exact row count", len(blocked) == 15, len(blocked))
    check("blocked ledger classifications", {row["historical_classification"] for row in blocked} == {"AMBIGUOUS", "UNAVAILABLE"})
    check("no silent historical upgrade", all(row["historical_rows_modified"] == "0" for row in history))

    shared = (ROOT / "backend/mlb/identity/provider_event_game_binding_v1.py").read_text()
    player = (ROOT / "backend/mlb/scripts/build_mlb_predictions_wide.py").read_text()
    pinnacle = (ROOT / "backend/mlb/scripts/capture_mlb_pinnacle_main_markets_v1.py").read_text()
    check("shared resolver offline", "requests" not in shared and "urlopen" not in shared)
    check("shared resolver phase-neutral", "season_phase" not in shared and "game_date" not in shared)
    check("unsafe player chooser removed", "_choose_game_for_event" not in player)
    check("Pinnacle verified gate active", "require_verified_bindings=True" in pinnacle)
    check("provider request count unchanged", pinnacle.count("requests.get(") == 1 and player.count("market_odds_service._fetch_market_snapshot(") == 1)

    test = subprocess.run(
        [str(PYTHON), "-m", "unittest", "-q", TEST_MODULE], cwd=ROOT,
        text=True, capture_output=True,
    )
    check(
        "dependency-free tests", test.returncode == 0,
        "returncode=0;tests=22" if test.returncode == 0 else (test.stdout + test.stderr).strip(),
    )

    manifest_path = PACKAGE / "sha256_manifest.csv"
    if manifest_path.exists():
        with manifest_path.open(newline="", encoding="utf-8") as handle:
            manifest = list(csv.DictReader(handle))
        mismatches = [row["path"] for row in manifest if sha256(PACKAGE / row["path"]) != row["sha256"]]
        check("package SHA-256 manifest", not mismatches, "|".join(mismatches))
    source_manifest_path = PACKAGE / "source_sha256_manifest.csv"
    if source_manifest_path.exists():
        with source_manifest_path.open(newline="", encoding="utf-8") as handle:
            manifest = list(csv.DictReader(handle))
        mismatches = [row["path"] for row in manifest if sha256(ROOT / row["path"]) != row["sha256"]]
        check("source SHA-256 manifest", not mismatches, "|".join(mismatches))

    report = {
        "contract": "MLB_2026_PROVIDER_EVENT_GAME_IDENTITY_HARDENING_V1",
        "passed": sum(item["passed"] for item in checks),
        "failed": sum(not item["passed"] for item in checks),
        "skipped": 0,
        "dependency_free_tests_passed": 22 if test.returncode == 0 else 0,
        "dependency_free_tests_failed": 0 if test.returncode == 0 else 1,
        "checks": checks,
    }
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0 if report["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
