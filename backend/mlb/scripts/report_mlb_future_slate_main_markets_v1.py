"""Report first-price and descriptive movement for retained future-slate markets."""
from __future__ import annotations

import argparse
import csv
import json
import sqlite3
from pathlib import Path
from typing import Any

from backend.mlb.markets.bookmaker_eu_supplemental_v1 import utc

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_LEDGER = ROOT / "backend/mlb/exports/market_history/full_game_totals/full_game_totals_v1.sqlite3"


def _snapshot_rows(conn: sqlite3.Connection, game_date: str) -> list[dict[str, Any]]:
    generic = [json.loads(row[0]) for row in conn.execute(
        "SELECT market_payload_json FROM supplemental_main_market_snapshots WHERE game_date=?", (game_date,))]
    has_totals = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='full_game_total_market_snapshots'"
    ).fetchone()
    totals = ([json.loads(row[0]) for row in conn.execute(
        "SELECT market_payload_json FROM full_game_total_market_snapshots WHERE game_date=?", (game_date,))]
        if has_totals else [])
    seen, output = set(), []
    for row in generic + totals:
        identity = row["canonical_market_identity"]
        if identity not in seen:
            seen.add(identity); output.append(row)
    return output


def report_rows(conn: sqlite3.Connection, game_date: str) -> list[dict[str, Any]]:
    discoveries = [json.loads(row[0]) for row in conn.execute(
        "SELECT discovery_payload_json FROM main_market_event_discoveries WHERE game_date=? ORDER BY game_id,captured_at_utc",
        (game_date,),
    )]
    snapshots = _snapshot_rows(conn, game_date)
    first_discovery = {}
    for item in discoveries:
        key = (item["provider"], int(item["game_id"]))
        if key not in first_discovery or utc(item["captured_at_utc"]) < utc(first_discovery[key]["captured_at_utc"]):
            first_discovery[key] = item
    groups: dict[tuple[str, str, int], list[dict[str, Any]]] = {}
    for row in snapshots:
        groups.setdefault((str(row.get("provider") or "THE_ODDS_API"), str(row["bookmaker_key"]), int(row["game_id"])), []).append(row)
    output = []
    discovery_keys_with_prices = {(provider, game) for provider, _, game in groups}
    for (provider, book, game_id), values in sorted(groups.items()):
        values.sort(key=lambda row: row["captured_at_utc"])
        discovery = first_discovery.get((provider, game_id), {})
        first = values[0]
        by_market = {market: [row for row in values if row["market_type"] == market]
                     for market in ("MONEYLINE", "FULL_GAME_TOTAL", "RUN_LINE")}
        output.append(_report_row(game_date, provider, book, game_id, discovery, first, by_market))
    for (provider, game_id), discovery in sorted(first_discovery.items()):
        if (provider, game_id) in discovery_keys_with_prices:
            continue
        output.append(_report_row(game_date, provider, discovery.get("bookmaker_scope", "UNPRICED_PROVIDER_EVENT"),
                                  game_id, discovery, None, {}))
    return sorted(output, key=lambda row: (row["game_pk"], row["provider"], row["bookmaker"]))


def _report_row(game_date: str, provider: str, book: str, game_id: int, discovery: dict[str, Any],
                first: dict[str, Any] | None, by_market: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    market_status = lambda name: "PRICED" if by_market.get(name) else "NOT_PRICED"
    rows = [row for values in by_market.values() for row in values]
    first_time = min((row["captured_at_utc"] for row in rows), default=None)
    lead = ((utc(first["scheduled_start_utc"]) - utc(first_time)).total_seconds() / 60.0) if first and first_time else None
    result = {"game_date": game_date, "matchup": (
        f"{first['away_team']} @ {first['home_team']}" if first else discovery.get("matchup")),
        "game_pk": game_id, "provider": provider, "bookmaker": book,
        "first_event_discovery_utc": discovery.get("captured_at_utc"),
        "first_priced_observation_utc": first_time, "moneyline_status": market_status("MONEYLINE"),
        "total_status": market_status("FULL_GAME_TOTAL"), "run_line_status": market_status("RUN_LINE"),
        "first_price_lead_time_minutes": lead, "retained_snapshots": len(rows)}
    for market, values in by_market.items():
        if not values:
            continue
        values.sort(key=lambda row: row["captured_at_utc"]); earliest, latest = values[0], values[-1]
        prefix = {"MONEYLINE": "moneyline", "FULL_GAME_TOTAL": "total", "RUN_LINE": "run_line"}[market]
        line_field = "total_line" if market == "FULL_GAME_TOTAL" else "home_spread" if market == "RUN_LINE" else None
        probability = "no_vig_over_probability" if market == "FULL_GAME_TOTAL" else "no_vig_home_probability"
        result[f"{prefix}_snapshot_count"] = len(values)
        if line_field:
            result[f"{prefix}_first_line"] = earliest.get(line_field); result[f"{prefix}_latest_line"] = latest.get(line_field)
            result[f"{prefix}_line_movement"] = float(latest[line_field]) - float(earliest[line_field])
        same_line = not line_field or float(latest[line_field]) == float(earliest[line_field])
        if same_line and earliest.get(probability) is not None and latest.get(probability) is not None:
            result[f"{prefix}_probability_movement"] = float(latest[probability]) - float(earliest[probability])
        side_names = ("away", "home") if market in {"MONEYLINE", "RUN_LINE"} else ("over", "under")
        for side in side_names:
            result[f"{prefix}_first_{side}_price"] = earliest.get(f"{side}_american_price", earliest.get(f"{side}_price"))
            result[f"{prefix}_latest_{side}_price"] = latest.get(f"{side}_american_price", latest.get(f"{side}_price"))
    return result


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--date", required=True)
    parser.add_argument("--ledger-path", type=Path, default=DEFAULT_LEDGER); parser.add_argument("--output", type=Path)
    args = parser.parse_args(); conn = sqlite3.connect(args.ledger_path); rows = report_rows(conn, args.date)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        fields = sorted({key for row in rows for key in row})
        with args.output.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields); writer.writeheader(); writer.writerows(rows)
    print(json.dumps(rows, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
