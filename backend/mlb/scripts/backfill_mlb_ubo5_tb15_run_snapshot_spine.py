#!/usr/bin/env python3
"""Backfill complete-run snapshots only where exact run-tagged sources survive."""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

from backend.mlb.shared.ubo5_tb15_run_snapshot_spine import freeze_complete_run

ROOT = Path(__file__).resolve().parents[3]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--output-root", default="backend/mlb/exports/model_v2/ubo5_tb15")
    parser.add_argument("--odds-root", default="backend/mlb/exports/odds_history")
    args = parser.parse_args()
    output_root = ROOT / args.output_root
    odds_day = ROOT / args.odds_root / args.date
    frozen, unavailable = 0, []
    for audit in sorted((output_root / args.date).glob("ubo5_tb15_prelineup_confirmation_audit_*.csv")):
        match = re.search(r"(local_daily_\d{8}T\d{6}Z)", audit.name)
        if not match:
            continue
        tag = match.group(1)
        odds = odds_day / f"odds_mlb_playerprops__{tag}.json"
        wide = odds_day / f"mlb_predictions_wide_calibrated__{tag}.csv"
        route = odds_day / f"route_ledger__{tag}.csv"
        missing = [path.name for path in (odds, wide, route) if not path.is_file()]
        if missing:
            unavailable.append({"run_tag": tag, "missing": missing})
            continue
        freeze_complete_run(
            repository_root=ROOT, output_root=output_root, date=args.date, run_tag=tag,
            market_snapshot_path=odds, identity_source_path=wide,
            route_ledger_path=route, prelineup_audit_path=audit,
        )
        frozen += 1
    for package in sorted((output_root / args.date / "manual_refresh").glob("manual_ubo5_tb15_*")):
        tag = package.name
        odds = package / "betonline_tb15_snapshot.json"
        wide = package / "identity_binding.csv"
        route = package / "confirmed_route_ledger.csv"
        audit = package / "prelineup_confirmation_audit.csv"
        missing = [path.name for path in (odds, wide, route, audit) if not path.is_file()]
        if missing:
            unavailable.append({"run_tag": tag, "missing": missing})
            continue
        refresh_summary = package / "refresh_summary.json"
        identity_rejects = []
        if refresh_summary.is_file():
            identity_rejects = json.loads(refresh_summary.read_text(encoding="utf-8")).get(
                "identity_rejects", []
            )
        freeze_complete_run(
            repository_root=ROOT, output_root=output_root, date=args.date, run_tag=tag,
            market_snapshot_path=odds, identity_source_path=wide,
            route_ledger_path=route, prelineup_audit_path=audit,
            identity_rejects=identity_rejects,
        )
        frozen += 1
    print(f"complete_run_snapshots_frozen={frozen}")
    print(f"runs_not_certifiable={len(unavailable)}")
    for item in unavailable:
        print(f"{item['run_tag']} missing={','.join(item['missing'])}")
    manifest_path = output_root / args.date / f"ubo5_tb15_run_population_manifest_{args.date}.json"
    if manifest_path.is_file():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["historical_backfill_status"] = (
            "COMPLETE_EXACT_RUN_TAGGED_SOURCE_COVERAGE"
            if not unavailable else "PARTIAL_EXACT_RUN_TAGGED_SOURCE_COVERAGE"
        )
        manifest["uncertifiable_runs"] = unavailable
        if unavailable:
            manifest["spine_status"] = "PARTIAL_CERTIFIED_RUN_SNAPSHOTS"
            manifest["final_pregame_population_status"] = (
                "NOT_CERTIFIABLE_INCOMPLETE_RUN_SPINE"
            )
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
