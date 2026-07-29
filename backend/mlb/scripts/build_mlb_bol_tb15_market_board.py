#!/usr/bin/env python3
"""Build the model-independent, identity-bound BetOnline TB 1.5 ledger."""
from __future__ import annotations

import argparse
import csv
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
TEAM_NAMES = {
    "ARI":"Arizona Diamondbacks","ATH":"Athletics","OAK":"Athletics","ATL":"Atlanta Braves",
    "BAL":"Baltimore Orioles","BOS":"Boston Red Sox","CHC":"Chicago Cubs","CWS":"Chicago White Sox",
    "CIN":"Cincinnati Reds","CLE":"Cleveland Guardians","COL":"Colorado Rockies","DET":"Detroit Tigers",
    "HOU":"Houston Astros","KC":"Kansas City Royals","LAA":"Los Angeles Angels","LAD":"Los Angeles Dodgers",
    "MIA":"Miami Marlins","MIL":"Milwaukee Brewers","MIN":"Minnesota Twins","NYM":"New York Mets",
    "NYY":"New York Yankees","PHI":"Philadelphia Phillies","PIT":"Pittsburgh Pirates","SD":"San Diego Padres",
    "SEA":"Seattle Mariners","SF":"San Francisco Giants","STL":"St. Louis Cardinals",
    "TB":"Tampa Bay Rays","TEX":"Texas Rangers","TOR":"Toronto Blue Jays","WSH":"Washington Nationals",
}


def norm_name(value: object) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").casefold())


def parse_prices(snapshot: dict) -> dict[tuple[str, str, str], dict]:
    reverse = {v: k for k, v in TEAM_NAMES.items()}
    result: dict[tuple[str, str, str], dict] = {}
    for event in snapshot.get("events", []):
        home, away = reverse.get(event.get("home_team")), reverse.get(event.get("away_team"))
        if not home or not away:
            continue
        game = f"{'ATH' if away == 'OAK' else away} @ {'ATH' if home == 'OAK' else home}"
        for book in event.get("bookmakers", []):
            if book.get("key") != "betonlineag":
                continue
            for market in book.get("markets", []):
                if market.get("key") != "batter_total_bases":
                    continue
                stamp = market.get("last_update") or snapshot.get("captured_at_utc")
                for outcome in market.get("outcomes", []):
                    if float(outcome.get("point") or 0) != 1.5:
                        continue
                    side = str(outcome.get("name") or "").lower()
                    if side not in {"over", "under"}:
                        continue
                    key = (game, norm_name(outcome.get("description")), "1.5")
                    result.setdefault(key, {"over": None, "under": None, "source_timestamp": stamp})
                    result[key][side] = outcome.get("price")
    return result


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--wide-csv", type=Path, required=True)
    ap.add_argument("--odds-json", type=Path, required=True)
    ap.add_argument("--run-tag", default="")
    ap.add_argument("--output-root", type=Path, default=Path("backend/mlb/exports/model_v2/bol_tb15"))
    args = ap.parse_args()
    wide = pd.read_csv(args.wide_csv)
    snapshot = json.loads(args.odds_json.read_text())
    prices = parse_prices(snapshot)
    tb = wide[(wide["prop_type"] == "total_bases") & pd.to_numeric(wide["p_over_1_5"], errors="coerce").notna()].copy()
    rows = []
    for row in tb.itertuples(index=False):
        away = "ATH" if str(row.away_team_code) == "OAK" else str(row.away_team_code)
        home = "ATH" if str(row.home_team_code) == "OAK" else str(row.home_team_code)
        game = f"{away} @ {home}"
        price = prices.get((game, norm_name(row.player_name), "1.5"))
        if not price or price["over"] is None or price["under"] is None:
            continue
        rows.append({
            "slate_date": args.date, "game_pk": int(row.game_id), "batter_mlb_id": int(row.player_id),
            "player": row.player_name, "game": game, "team": "ATH" if str(row.team) == "OAK" else row.team,
            "opponent": "ATH" if str(row.opponent) == "OAK" else row.opponent,
            "line": 1.5, "over_odds": int(price["over"]), "under_odds": int(price["under"]),
            "source_timestamp": price["source_timestamp"], "lineup_status": "UNKNOWN",
            "batting_order": "", "participation_state": "PREGAME",
        })
    frame = pd.DataFrame(rows).drop_duplicates(["slate_date","game_pk","batter_mlb_id","line"])
    day = args.output_root / args.date
    run_tag = args.run_tag or f"neutral_{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
    snap = day / "snapshots" / run_tag
    snap.mkdir(parents=True, exist_ok=True)
    day.mkdir(parents=True, exist_ok=True)
    csv_path = day / f"bol_tb15_market_board_{args.date}.csv"
    md_path = day / f"bol_tb15_market_board_{args.date}.md"
    frame.to_csv(csv_path, index=False, quoting=csv.QUOTE_MINIMAL)
    frame.to_csv(snap / "bol_tb15_market_rows.csv", index=False)
    cols = ["Player","Game","Over odds","Under odds","Lineup","Batting order"]
    lines = [f"# BetOnline Total Bases 1.5 Market Board — {args.date}", "",
             f"Run tag: `{run_tag}`  ", f"Source timestamp: `{snapshot.get('captured_at_utc','')}`", "",
             "| " + " | ".join(cols) + " |", "| " + " | ".join(["---"]*len(cols)) + " |"]
    for r in frame.itertuples(index=False):
        lines.append(f"| {r.player} | {r.game} | {r.over_odds:+d} | {r.under_odds:+d} | {r.lineup_status} | {r.batting_order} |")
    if frame.empty:
        lines.append("| *None* |  |  |  |  |  |")
    md_path.write_text("\n".join(lines) + "\n")
    manifest = {
        "status":"ACTIVE_MODEL_INDEPENDENT_MARKET_LEDGER","slate_date":args.date,"run_tag":run_tag,
        "generated_at_utc":datetime.now(timezone.utc).isoformat(),"source_timestamp":snapshot.get("captured_at_utc"),
        "valid_identity_bound_two_sided_rows":len(frame),"model_filter_applied":False,
        "identity":["slate_date","game_pk","batter_mlb_id","line"],
    }
    (day/"population_manifest.json").write_text(json.dumps(manifest,indent=2,sort_keys=True)+"\n")
    print(json.dumps(manifest,sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
