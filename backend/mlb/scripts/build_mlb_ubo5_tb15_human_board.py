#!/usr/bin/env python3
"""Render the authoritative UBO-5 TB1.5 production route as an operator board."""

from __future__ import annotations

import argparse
import csv
import json
import math
import shutil
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

UTC = ZoneInfo("UTC")
PT = ZoneInfo("America/Los_Angeles")
ROOT = Path(__file__).resolve().parents[3]
FIELDS = [
    "player_name",
    "game",
    "line",
    "ubo5_over_probability",
    "no_vig_over_probability",
    "over_edge_percentage_points",
]
TEAM_NAMES = {
    "ARI": "Arizona Diamondbacks", "ATH": "Athletics", "ATL": "Atlanta Braves",
    "BAL": "Baltimore Orioles", "BOS": "Boston Red Sox", "CHC": "Chicago Cubs",
    "CIN": "Cincinnati Reds", "CLE": "Cleveland Guardians", "COL": "Colorado Rockies",
    "CWS": "Chicago White Sox", "DET": "Detroit Tigers", "HOU": "Houston Astros",
    "KC": "Kansas City Royals", "LAA": "Los Angeles Angels", "LAD": "Los Angeles Dodgers",
    "MIA": "Miami Marlins", "MIL": "Milwaukee Brewers", "MIN": "Minnesota Twins",
    "NYM": "New York Mets", "NYY": "New York Yankees", "PHI": "Philadelphia Phillies",
    "PIT": "Pittsburgh Pirates", "SD": "San Diego Padres", "SEA": "Seattle Mariners",
    "SF": "San Francisco Giants", "STL": "St. Louis Cardinals", "TB": "Tampa Bay Rays",
    "TEX": "Texas Rangers", "TOR": "Toronto Blue Jays", "WSH": "Washington Nationals",
}


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def number(value: object) -> float | None:
    try:
        parsed = float(str(value))
        return parsed if math.isfinite(parsed) else None
    except (TypeError, ValueError):
        return None


def integer(value: object) -> int | None:
    parsed = number(value)
    return int(parsed) if parsed is not None else None


def implied(american: int | None) -> float | None:
    if american is None or american == 0:
        return None
    return 100 / (american + 100) if american > 0 else -american / (-american + 100)


def pct(value: float | None) -> str:
    return "" if value is None else f"{value * 100:.2f}"


def signed_pct(value: float | None) -> str:
    return "" if value is None else f"{value * 100:+.2f}"


def display_pct(value: str) -> str:
    return "—" if value == "" else f"{value}%"


def display_odds(value: object) -> str:
    parsed = integer(value)
    return "PRICE UNAVAILABLE" if parsed is None else f"{parsed:+d}"


def parse_time(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except (ValueError, AttributeError):
        return None


def team_name_map(wide: list[dict[str, str]]) -> dict[str, str]:
    mapping: dict[str, str] = dict(TEAM_NAMES)
    for row in wide:
        for code_key, name_key in (("home_team_code", "home_team"), ("away_team_code", "away_team")):
            if row.get(code_key) and row.get(name_key):
                mapping[row[code_key]] = row[name_key]
    return mapping


def price_index(snapshot: dict, team_names: dict[str, str]) -> dict[tuple[str, str, str], dict]:
    """Index exact BetOnline event/name/line rows after the certified ID-to-name bridge."""
    index: dict[tuple[str, str, str], dict] = {}
    reverse = {v: k for k, v in team_names.items()}
    captured = str(snapshot.get("captured_at_utc") or "")
    for event in snapshot.get("events") or []:
        home = reverse.get(str(event.get("home_team") or ""))
        away = reverse.get(str(event.get("away_team") or ""))
        if not home or not away:
            continue
        matchup = f"{away} @ {home}"
        for book in event.get("bookmakers") or []:
            if book.get("key") != "betonlineag":
                continue
            for market in book.get("markets") or []:
                if market.get("key") != "batter_total_bases":
                    continue
                updated = str(market.get("last_update") or captured)
                for outcome in market.get("outcomes") or []:
                    line = number(outcome.get("point"))
                    name = str(outcome.get("description") or "").strip()
                    side = str(outcome.get("name") or "").lower()
                    if line is None or not name or side not in {"over", "under"}:
                        continue
                    key = (matchup, name, f"{line:.1f}")
                    entry = index.setdefault(key, {"over": None, "under": None, "timestamp": updated})
                    entry[side] = integer(outcome.get("price"))
                    if updated > entry["timestamp"]:
                        entry["timestamp"] = updated
    return index


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--route-ledger", default="")
    ap.add_argument("--route-health", default="")
    ap.add_argument("--wide-csv", default="backend/mlb/data/processed/mlb_predictions_wide_calibrated.csv")
    ap.add_argument("--slate-csv", default="backend/mlb/data/processed/mlb_slate_output.csv")
    ap.add_argument("--odds-json", default="")
    ap.add_argument("--snapshot-run-tag", default="")
    ap.add_argument("--output-root", default="backend/mlb/exports/model_v2/ubo5_tb15")
    ap.add_argument("--archive-root", default="backend/mlb/exports/odds_history")
    args = ap.parse_args()

    date = args.date
    route = ROOT / (args.route_ledger or f"artifacts/analysis/mlb/production_routes/ubo5_tb15/{date}/route_ledger.csv")
    health_path = ROOT / (args.route_health or f"artifacts/analysis/mlb/production_routes/ubo5_tb15/{date}/route_health.json")
    wide_path = ROOT / args.wide_csv
    slate_path = ROOT / args.slate_csv
    odds_path = ROOT / (args.odds_json or f"backend/mlb/exports/odds_history/{date}/odds_mlb_playerprops.json")
    for path in (route, health_path, wide_path, slate_path, odds_path):
        if not path.exists():
            raise SystemExit(f"missing authoritative input: {path}")

    routes, wide, slate = read_csv(route), read_csv(wide_path), read_csv(slate_path)
    health = json.loads(health_path.read_text())
    snapshot = json.loads(odds_path.read_text())
    wide_idx = {
        (r.get("game_id"), r.get("player_id"), f"{number(r.get('line')) or 0:.1f}", r.get("prop_type")): r
        for r in wide
    }
    slate_idx = {
        (r.get("game_id"), r.get("player_id"), f"{number(r.get('line')) or 0:.1f}", r.get("prop_type")): r
        for r in slate
    }
    names = team_name_map(wide)
    prices = price_index(snapshot, names)
    generated = datetime.now(UTC).isoformat()
    rows: list[dict[str, object]] = []
    excluded: list[dict[str, str]] = []

    for source in routes:
        line = number(source.get("line")) or 1.5
        key = (source.get("game_pk"), source.get("batter_mlb_id"), f"{line:.1f}", "total_bases")
        identity = slate_idx.get(key) or wide_idx.get(key)
        if not identity or not identity.get("player_name"):
            excluded.append({
                "game_pk": source.get("game_pk", ""), "batter_mlb_id": source.get("batter_mlb_id", ""),
                "reason": "UNCERTIFIED_PLAYER_IDENTITY",
            })
            continue
        name = identity["player_name"]
        home = identity.get("home_team_code") or (source.get("team") if source.get("home_away") == "home" else source.get("opponent"))
        away = identity.get("away_team_code") or (source.get("team") if source.get("home_away") == "away" else source.get("opponent"))
        matchup = f"{away} @ {home}"
        event_time = parse_time(source.get("scheduled_start_utc", "") or identity.get("game_time", ""))
        active_over = number(source.get("active_probability"))
        incumbent_over = number(source.get("existing_production_probability") or source.get("production_prob_over"))
        ubo5_over = number(source.get("ubo5_probability_over"))
        routed = str(source.get("model_source") or "").startswith("UBO5") and ubo5_over is not None
        displayed_over = active_over is not None and active_over >= 0.5
        side = "OVER" if displayed_over else "UNDER"
        displayed = active_over if displayed_over else (1 - active_over if active_over is not None else None)
        incumbent = incumbent_over if displayed_over else (1 - incumbent_over if incumbent_over is not None else None)
        market = prices.get((matchup, name, f"{line:.1f}"), {})
        over_odds, under_odds = market.get("over"), market.get("under")
        raw_over, raw_under = implied(over_odds), implied(under_odds)
        denom = (raw_over or 0) + (raw_under or 0)
        nv_over = raw_over / denom if raw_over is not None and raw_under is not None and denom else None
        nv_under = 1 - nv_over if nv_over is not None else None
        side_nv = nv_over if displayed_over else nv_under
        side_odds = over_odds if displayed_over else under_odds
        raw_side = raw_over if displayed_over else raw_under
        route_status = "ROUTED" if routed else "INCUMBENT_FALLBACK"
        row = {
            "slate_date": date, "player_name": name, "batter_mlb_id": source.get("batter_mlb_id"),
            "team": source.get("team"), "opponent": source.get("opponent"), "matchup": matchup,
            "game_time_pt": event_time.astimezone(PT).strftime("%-I:%M %p") if event_time else "",
            "game_time_utc": event_time.strftime("%Y-%m-%dT%H:%M:%SZ") if event_time else "",
            "batting_order": source.get("batting_order_position"), "line": f"{line:.1f}",
            "prediction_side": side, "prediction_label": f"{side} 1.5 TOTAL BASES",
            "ubo5_over_probability_pct": pct(ubo5_over),
            "ubo5_under_probability_pct": pct(1 - ubo5_over if ubo5_over is not None else None),
            "displayed_side_probability_pct": pct(displayed),
            "incumbent_displayed_side_probability_pct": pct(incumbent),
            "probability_delta_pct_points": signed_pct(displayed - incumbent if displayed is not None and incumbent is not None else None),
            "betonline_over_odds": over_odds, "betonline_under_odds": under_odds,
            "displayed_side_odds": side_odds, "raw_break_even_pct": pct(raw_side),
            "no_vig_market_probability_pct": pct(side_nv),
            "model_edge_pct_points": signed_pct(displayed - side_nv if displayed is not None and side_nv is not None else None),
            "over_model_edge_pct_points": signed_pct(active_over - nv_over if active_over is not None and nv_over is not None else None),
            "under_model_edge_pct_points": signed_pct((1-active_over) - nv_under if active_over is not None and nv_under is not None else None),
            "market_hold_pct": pct(denom - 1 if raw_over is not None and raw_under is not None else None),
            "plus_money_over_flag": bool(displayed_over and over_odds is not None and over_odds > 0),
            "price_source_status": "BETONLINE_TWO_SIDED" if over_odds is not None and under_odds is not None else "PRICE UNAVAILABLE",
            "price_snapshot_timestamp_utc": market.get("timestamp", ""),
            "price_snapshot_run_tag": args.snapshot_run_tag or snapshot.get("run_tag") or snapshot.get("snapshot_run_tag") or "",
            "route_status": route_status,
            "fallback_reason": source.get("primary_fallback_category") or source.get("secondary_fallback_details") or "",
            "missing_requirement": source.get("exact_missing_features") or "",
            "model_source": source.get("model_source"), "prediction_timestamp_utc": source.get("prediction_timestamp_utc"),
            "_sort_time": event_time.timestamp() if event_time else float("inf"),
        }
        rows.append(row)

    routed = [r for r in rows if r["route_status"] == "ROUTED"]
    fallbacks = [r for r in rows if r["route_status"] != "ROUTED"]
    priced = [r for r in rows if r["price_source_status"] == "BETONLINE_TWO_SIDED"]
    positive_over = [
        r for r in routed
        if number(r["over_model_edge_pct_points"]) is not None
        and number(r["over_model_edge_pct_points"]) > 0
        and r["price_source_status"] == "BETONLINE_TWO_SIDED"
    ]
    price_unavailable = [r for r in routed if r["price_source_status"] != "BETONLINE_TWO_SIDED"]
    def edge_sort(row: dict) -> tuple:
        edge = number(row.get("model_edge_pct_points"))
        prob = number(row.get("displayed_side_probability_pct")) or 0
        return (-(edge if edge is not None else -999), -prob, row["_sort_time"])
    routed.sort(key=edge_sort)
    positive_over.sort(
        key=lambda r: (
            -(number(r["over_model_edge_pct_points"]) or -999),
            -(number(r["ubo5_over_probability_pct"]) or 0),
            r["_sort_time"],
        )
    )

    out_dir = ROOT / args.output_root / date
    latest = ROOT / args.output_root / "latest"
    archive = ROOT / args.archive_root / date
    out_dir.mkdir(parents=True, exist_ok=True)
    latest.mkdir(parents=True, exist_ok=True)
    archive.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"ubo5_tb15_board_{date}.csv"
    md_path = out_dir / f"ubo5_tb15_board_{date}.md"
    excluded_path = out_dir / f"ubo5_tb15_board_excluded_{date}.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows([
            {
                "player_name": r["player_name"],
                "game": r["matchup"],
                "line": "Over 1.5 TB",
                "ubo5_over_probability": f"{number(r['ubo5_over_probability_pct']):.2f}%",
                "no_vig_over_probability": f"{number(r['ubo5_over_probability_pct']) - number(r['over_model_edge_pct_points']):.2f}%",
                "over_edge_percentage_points": f"{number(r['over_model_edge_pct_points']):+.2f} pp",
            }
            for r in positive_over
        ])
    with excluded_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["game_pk", "batter_mlb_id", "reason"])
        writer.writeheader(); writer.writerows(excluded)

    columns = ["Player", "Game", "Line", "UBO-5 Over", "No-vig Over", "Over edge"]
    def table(items: list[dict]) -> list[str]:
        lines = [
            "| " + " | ".join(columns) + " |",
            "|---|---|---|---:|---:|---:|",
        ]
        for r in items:
            over = number(r["ubo5_over_probability_pct"])
            edge = number(r["over_model_edge_pct_points"])
            lines.append(
                f"| {r['player_name']} | {r['matchup']} | Over 1.5 TB | "
                f"{over:.2f}% | {over - edge:.2f}% | {edge:+.2f} pp |"
            )
        return lines + ([] if items else ["", "*None*"])
    md = [
        f"# UBO-5 Total Bases 1.5 Board — {date}", "",
        f"- Generated: `{generated}`",
        f"- Route enabled: `{health.get('route_enabled', 'unknown')}`",
        f"- Candidates: `{health.get('feature_ledger_rows', len(routes))}` | routed: `{len(routed)}` | incumbent fallbacks: `{len(fallbacks)}`",
        f"- Artifact hash: `{health.get('artifact_hash_status')}` | feature schema: `{health.get('feature_schema_status')}` | temporal integrity: `{health.get('temporal_integrity_status')}`",
        f"- BetOnline two-sided price coverage: `{len(priced)}/{len(rows)} ({(100*len(priced)/len(rows) if rows else 0):.2f}%)`", "",
        "## Positive UBO-5 Over Edge", "", *table(positive_over), "",
        "## Price-unavailable diagnostic", "",
        "| Player | Game | Line | Status |",
        "|---|---|---|---|",
    ]
    md += [
        f"| {r['player_name']} | {r['matchup']} | Over 1.5 TB | PRICE UNAVAILABLE |"
        for r in price_unavailable
    ]
    if not price_unavailable:
        md.append("| _None_ | | | |")
    md += [
        "", "## Notes", "",
        "- UBO-5 applies only to certified Total Bases 1.5 established starters.",
        "- Inclusion is `UBO-5 Over probability > current two-sided BetOnline no-vig Over probability`.",
        "- The inclusion rule is independent of whether Over exceeds 50%.",
        "- This board presents production outputs; it does not rescore, route, select, upload, or wager.",
        "- Rows without current two-sided BetOnline prices are excluded from the main table.",
        "- `route_ledger.csv` remains the audit authority.", "",
    ]
    md_path.write_text("\n".join(md), encoding="utf-8")
    summary = {
        "slate_date": date, "generated_at_utc": generated, "route_enabled": health.get("route_enabled"),
        "candidate_rows": len(routes), "routed_rows": len(routed), "fallback_rows": len(fallbacks),
        "identity_excluded_rows": len(excluded), "priced_rows": len(priced),
        "betonline_price_coverage_pct": round(100*len(priced)/len(rows), 2) if rows else 0,
        "positive_over_edge_rows": len(positive_over),
        "positive_no_vig_edge_rows": len(positive_over),
        "price_unavailable_rows": len(price_unavailable),
        "markdown_path": str(md_path.relative_to(ROOT)), "csv_path": str(csv_path.relative_to(ROOT)),
        "latest_markdown_path": str((latest / "ubo5_tb15_board.md").relative_to(ROOT)),
        "latest_csv_path": str((latest / "ubo5_tb15_board.csv").relative_to(ROOT)),
    }
    summary_path = out_dir / f"ubo5_tb15_board_summary_{date}.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    for source, destination in (
        (md_path, latest / "ubo5_tb15_board.md"), (csv_path, latest / "ubo5_tb15_board.csv"),
        (health_path, latest / "route_health.json"), (md_path, archive / md_path.name),
        (csv_path, archive / csv_path.name), (health_path, archive / f"ubo5_tb15_route_health_{date}.json"),
    ):
        shutil.copy2(source, destination)
    print(f"markdown={md_path.relative_to(ROOT)}")
    print(f"csv={csv_path.relative_to(ROOT)}")
    print(f"latest={latest.relative_to(ROOT)}")
    print("PLAYER                 GAME          LINE          UBO5 OVER  NO-VIG OVER  OVER EDGE")
    for r in positive_over[:10]:
        over = number(r["ubo5_over_probability_pct"])
        edge = number(r["over_model_edge_pct_points"])
        print(
            f"{str(r['player_name'])[:21]:21} {str(r['matchup']):13} "
            f"{'Over 1.5 TB':13} {over:8.2f}%  {over-edge:10.2f}%  {edge:+8.2f} pp"
        )
    if not positive_over:
        print("None")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
