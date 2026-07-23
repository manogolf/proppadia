#!/usr/bin/env python3
"""Build the certified current-slate UBO-5 TB1.5 candidate ledger."""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

COLUMNS = [
    "slate_date", "game_pk", "batter_mlb_id", "team", "opponent", "home_away",
    "prediction_timestamp_utc", "scheduled_start_utc", "lineup_certified",
    "lineup_certified_at_utc", "batting_order_position", "line", "run_tag",
    "opposing_starter_id", "batter_identity_certified", "identity_ambiguous",
    "source_lineage_pointer", "market_row_certified",
]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--slate-date", required=True)
    ap.add_argument("--wide-csv", required=True, type=Path)
    ap.add_argument("--lineup-csv", required=True, type=Path)
    ap.add_argument("--output", required=True, type=Path)
    ap.add_argument("--status-json", required=True, type=Path)
    ap.add_argument("--run-tag", required=True)
    args = ap.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)
    status = "PRODUCER_ERROR"
    detail = ""
    candidates = pd.DataFrame(columns=COLUMNS)
    try:
        wide = pd.read_csv(args.wide_csv)
        lineup = pd.read_csv(args.lineup_csv)
        required_wide = {"game_id", "player_id", "prop_type", "p_over_1_5", "team", "opponent", "is_home"}
        legacy = "pregame_validity_state" in lineup
        required_lineup = (
            {"game_id", "player_id", "team", "opponent", "lineup_slot", "opposing_starter_id",
             "lineup_status", "source_timestamp", "first_pitch_timestamp", "pregame_validity_state"}
            if legacy else
            {"game_id", "player_id", "team", "opponent", "lineup_slot", "confirmed_lineup_starter_flag",
             "team_lineup_status", "source_fetched_at_utc", "game_start_time_utc", "statsapi_game_status"}
        )
        missing = sorted((required_wide - set(wide)) | (required_lineup - set(lineup)))
        if missing:
            raise ValueError("missing columns: " + "|".join(missing))
        market = wide[
            wide["prop_type"].eq("total_bases")
            & pd.to_numeric(wide["p_over_1_5"], errors="coerce").notna()
        ].copy()
        market = market.drop_duplicates(["game_id", "player_id"], keep=False)
        if legacy:
            confirmed = lineup[
                lineup["lineup_status"].eq("CONFIRMED_LINEUP")
                & lineup["pregame_validity_state"].eq("VALID_PREGAME")
            ].copy()
            confirmed["_lineup_certified_at"] = confirmed["source_timestamp"]
            confirmed["_game_start"] = confirmed["first_pitch_timestamp"]
        else:
            confirmed = lineup[
                lineup["confirmed_lineup_starter_flag"].fillna(False).astype(bool)
                & lineup["team_lineup_status"].eq("confirmed_full")
                & lineup["statsapi_game_status"].eq("Pre-Game")
            ].copy()
            confirmed["_lineup_certified_at"] = confirmed["source_fetched_at_utc"]
            confirmed["_game_start"] = confirmed["game_start_time_utc"]
        joined = market.merge(
            confirmed, on=["game_id", "player_id"], how="inner", suffixes=("_wide", "_lineup"),
            validate="one_to_one",
        )
        pitcher_rows = wide[wide["prop_type"].isin(["strikeouts_pitching", "outs_recorded", "earned_runs", "hits_allowed"])].copy()
        pitcher_rows = pitcher_rows.sort_values("prop_type").drop_duplicates(["game_id", "team"])
        pitcher_map = dict(zip(zip(pitcher_rows["game_id"], pitcher_rows["team"]), pitcher_rows["player_id"]))
        opposing = (
            joined["opposing_starter_id"] if "opposing_starter_id" in joined
            else [pitcher_map.get((game, opponent)) for game, opponent in zip(joined["game_id"], joined["opponent_lineup"])]
        )
        candidates = pd.DataFrame({
            "slate_date": args.slate_date,
            "game_pk": joined["game_id"],
            "batter_mlb_id": joined["player_id"],
            "team": joined["team_lineup"],
            "opponent": joined["opponent_lineup"],
            "home_away": joined["is_home"].map({True: "home", False: "away"}),
            "prediction_timestamp_utc": now.isoformat(),
            "scheduled_start_utc": joined["_game_start"],
            "lineup_certified": True,
            "lineup_certified_at_utc": joined["_lineup_certified_at"],
            "batting_order_position": pd.to_numeric(joined["lineup_slot"], errors="coerce"),
            "line": 1.5,
            "run_tag": args.run_tag,
            "opposing_starter_id": opposing,
            "batter_identity_certified": True,
            "identity_ambiguous": False,
            "source_lineage_pointer": str(args.lineup_csv),
            "market_row_certified": True,
        })
        starts = pd.to_datetime(candidates["scheduled_start_utc"], utc=True, errors="coerce")
        candidates = candidates[starts.gt(pd.Timestamp(now))].copy()
        candidates = candidates[COLUMNS]
        status = "POPULATED" if len(candidates) else "NO_CURRENT_CANDIDATES"
    except Exception as exc:
        detail = f"{type(exc).__name__}:{exc}"
    candidates.to_csv(args.output, index=False)
    payload = {
        "generated_at_utc": now.isoformat(), "slate_date": args.slate_date,
        "producer_status": status, "candidate_rows": len(candidates),
        "candidate_ledger_path": str(args.output), "lineup_path": str(args.lineup_csv),
        "detail": detail,
        "candidate_ledger_sha256": hashlib.sha256(args.output.read_bytes()).hexdigest(),
    }
    args.status_json.write_text(json.dumps(payload, indent=2) + "\n")
    print(json.dumps(payload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
