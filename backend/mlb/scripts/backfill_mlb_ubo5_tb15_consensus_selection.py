#!/usr/bin/env python3
"""Freeze a previously certified same-run consensus audit (no board inference)."""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

from backend.mlb.scripts.build_mlb_ubo5_tb15_human_board import (
    canonical_game, canonical_player_name, price_index, read_csv, team_name_map,
)
from backend.mlb.shared.ubo5_tb15_consensus_selection import freeze

ROOT = Path(__file__).resolve().parents[3]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--run-tag", required=True)
    ap.add_argument("--output-root", default="backend/mlb/exports/model_v2/ubo5_tb15")
    args = ap.parse_args()
    day = ROOT / args.output_root / args.date
    audit_path = day / f"ubo5_tb15_consensus_audit__{args.run_tag}.csv"
    route_path = ROOT / f"backend/mlb/exports/odds_history/{args.date}/route_ledger__{args.run_tag}.csv"
    odds_path = ROOT / f"backend/mlb/exports/odds_history/{args.date}/odds_mlb_playerprops__{args.run_tag}.json"
    wide_path = ROOT / f"backend/mlb/exports/odds_history/{args.date}/mlb_predictions_wide_calibrated__{args.run_tag}.csv"
    for path in (audit_path, route_path, odds_path, wide_path):
        if not path.is_file():
            raise SystemExit(f"missing certified same-run source: {path}")
    audit = read_csv(audit_path)
    routes = {(r["game_pk"], r["batter_mlb_id"]): r for r in read_csv(route_path)}
    snapshot = json.loads(odds_path.read_text(encoding="utf-8"))
    prices = price_index(snapshot, team_name_map(read_csv(wide_path)))
    rows = []
    for item in audit:
        if item.get("consensus_positive_flag", "").lower() != "true":
            continue
        source = routes[(item["game_pk"], item["batter_mlb_id"])]
        market = prices[(canonical_game(item["game"]), canonical_player_name(item["player_name"]), "1.5")]
        rows.append({
            "slate_date": args.date, "run_tag": args.run_tag,
            "snapshot_timestamp_utc": item["snapshot_timestamp_utc"],
            "selection_timestamp_utc": source["prediction_timestamp_utc"],
            "game_pk": item["game_pk"], "batter_mlb_id": item["batter_mlb_id"],
            "player_name": item["player_name"], "team": source["team"],
            "opponent": source["opponent"], "game": canonical_game(item["game"]),
            "scheduled_start_utc": source["scheduled_start_utc"],
            "batting_order": source["batting_order_position"], "prop_type": "total_bases",
            "line": "1.5", "side": "OVER",
            "ubo5_probability_over": item["ubo5_probability_over"],
            "counterfactual_incumbent_probability": item["counterfactual_incumbent_probability"],
            "betonline_over_price": market["over"], "betonline_under_price": market["under"],
            "no_vig_over_probability": item["no_vig_over_probability"],
            "ubo5_over_edge_pp": float(item["ubo5_over_edge"]) * 100,
            "incumbent_over_edge_pp": float(item["incumbent_over_edge"]) * 100,
            "consensus_positive_flag": True,
            "ubo5_artifact_hash": source.get("active_artifact_sha256", ""),
            "counterfactual_incumbent_artifact_hash": item["counterfactual_incumbent_artifact_hash"],
            "counterfactual_lineage_status": "CERTIFIED_SAME_RUN_INDEPENDENT",
            "feature_vector_sha256": source["feature_vector_sha256"],
            "market_snapshot_path": str(odds_path.relative_to(ROOT)),
            "route_ledger_path": str(route_path.relative_to(ROOT)),
        })
    manifest = freeze(ROOT / args.output_root, args.date, args.run_tag, rows)
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
