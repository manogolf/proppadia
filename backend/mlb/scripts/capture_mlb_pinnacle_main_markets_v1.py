"""Capture explicit Pinnacle MLB main markets in the regular shared market estate."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from backend.mlb.markets.bookmaker_eu_supplemental_v1 import (
    append_attachment, append_consensus, append_market, build_consensus, connect_ledger,
    market_rows, no_vig,
)
from backend.mlb.markets.full_game_total_capture_v1 import (
    append_market as append_total_market, attach_all_markets,
    connect_ledger as connect_total_ledger, market_rows as total_market_rows,
)
from backend.mlb.markets.pinnacle_main_market_capture_v1 import (
    BOOKMAKER_KEY, MARKETS, PROVIDER, REQUEST_CLASS, RUN_LINE_MODEL_STATUS, eastern_date, parse_events,
)
from backend.mlb.public_game_predictions.durable_store_v1 import fetch_prediction_rows
from backend.mlb.scripts.run_mlb_totals_prospective_shadow_v1 import probability_fields
from backend.mlb.totals_predictions.live_context_bridge_v1 import fetch_hydrated_schedule, normalize_schedule
from backend.mlb.totals_predictions.prospective_shadow_v1 import (
    connect_ledger as connect_prediction_ledger, rows_for_date,
)

ROOT = Path(__file__).resolve().parents[3]
URL = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds"
RAW_ROOT = ROOT / "backend/mlb/exports/odds_history"
DEFAULT_LEDGER = ROOT / "backend/mlb/exports/market_history/full_game_totals/full_game_totals_v1.sqlite3"
TOTALS_PREDICTIONS = ROOT / "backend/mlb/exports/model_v2/totals_shadow_v1/totals_shadow_v1.sqlite3"
MODEL_ALPHA = 0.12944479977012996
QUOTA_HEADERS = ("x-requests-remaining", "x-requests-used", "x-requests-last")


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _display_path(path: Path) -> str:
    return str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else str(path)


def fetch(game_date: str, run_tag: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    key = os.getenv("ODDS_API_KEY", "").strip()
    if not key:
        raise RuntimeError("ODDS_API_KEY missing")
    params = {"apiKey": key, "bookmakers": BOOKMAKER_KEY, "markets": ",".join(MARKETS),
              "oddsFormat": "american", "dateFormat": "iso"}
    requested = now_utc()
    response = requests.get(URL, params=params, timeout=30)
    fetched = now_utc()
    content = response.content
    raw_path = RAW_ROOT / game_date / f"odds_mlb_pinnacle_main_markets__{run_tag}.json"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    # Retain even a provider error body; callers fail visibly after evidence exists.
    with raw_path.open("xb") as handle:
        handle.write(content)
    raw_sha = hashlib.sha256(content).hexdigest()
    headers = {name: response.headers.get(name) for name in QUOTA_HEADERS}
    manifest = {
        "provider": PROVIDER, "bookmaker_key": BOOKMAKER_KEY, "sport_key": "baseball_mlb",
        "game_date": game_date, "requested_at_utc": requested, "fetch_timestamp_utc": fetched,
        "source_run_tag": run_tag, "request_class": REQUEST_CLASS,
        "request_parameters_without_secret": {k: v for k, v in params.items() if k != "apiKey"},
        "authentication": "apiKey query parameter; value not retained", "http_status": response.status_code,
        "request_cost_headers": headers, "raw_response_path": _display_path(raw_path),
        "raw_response_sha256": raw_sha, "request_count": 1,
    }
    manifest_path = raw_path.with_suffix(".manifest.json")
    with manifest_path.open("x") as handle:
        handle.write(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    response.raise_for_status()
    events = response.json()
    if not isinstance(events, list):
        raise RuntimeError("unexpected OddsAPI Pinnacle payload")
    return events, {**manifest, "manifest_path": _display_path(manifest_path)}


def _to_total(row: dict[str, Any]) -> dict[str, Any]:
    return {**row, "market_type": "FULL_GAME_TOTAL", "over_price": row["over_american_price"],
            "under_price": row["under_american_price"],
            "provider_market_timestamp_utc": row["provider_market_updated_at_utc"],
            "market_status": "TOTAL_MARKET_CERTIFIED_PAIRED"}


def _generic_totals(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{**row, "provider": row.get("provider") or PROVIDER,
             "timing_status": row.get("timing_status") or "PREGAME_CERTIFIED",
             "no_vig_over_probability": no_vig(row.get("over_price"), row.get("under_price"))}
            for row in rows if row.get("market_type") == "FULL_GAME_TOTAL"]


def _attach(conn: Any, game_date: str, rows: list[dict[str, Any]], created: str):
    totals_models = rows_for_date(connect_prediction_ledger(TOTALS_PREDICTIONS), game_date)
    money_models = []
    money_status = "AVAILABLE"
    try:
        money_models = fetch_prediction_rows(game_date)
    except Exception as exc:
        money_status = f"UNAVAILABLE:{type(exc).__name__}"
    by_key = {(int(row["game_id"]), row["market_type"]): row for row in rows}
    totals_out, money_out = [], []
    for prediction in totals_models:
        market = by_key.get((int(prediction["game_pk"]), "FULL_GAME_TOTAL"))
        if not market:
            continue
        probs = probability_fields(float(prediction["expected_total"]), MODEL_ALPHA, float(market["total_line"]))
        identity = f"{prediction['game_date']}|{prediction['game_pk']}|{prediction['model_version']}|{prediction['prediction_snapshot_class']}"
        payload = {"prediction_identity": identity, "market_identity": market["canonical_market_identity"],
                   "timing_relationship": "AT_OR_BEFORE_PREDICTION" if market["captured_at_utc"] <= prediction["prediction_timestamp_utc"] else "POST_PREDICTION_MARKET_OBSERVATION",
                   "game_pk": int(prediction["game_pk"]), "model_expected_total": float(prediction["expected_total"]),
                   "pinnacle_total": float(market["total_line"]),
                   "model_minus_pinnacle": float(prediction["expected_total"]) - float(market["total_line"]), **probs}
        payload["ledger_action"] = append_attachment(conn, table="pinnacle_totals_shadow_attachments",
            prediction_identity=identity, market_identity=market["canonical_market_identity"], payload=payload, created_at_utc=created)
        totals_out.append(payload)
    for prediction in money_models:
        game_id = int(prediction.get("game_id", prediction.get("game_pk")))
        market = by_key.get((game_id, "MONEYLINE"))
        if not market:
            continue
        identity = f"{prediction['game_date']}|{game_id}|{prediction['winner_model_version']}|{prediction['prediction_snapshot_class']}"
        model_home, book_home = float(prediction["home_win_probability"]), float(market["no_vig_home_probability"])
        payload = {"prediction_identity": identity, "market_identity": market["canonical_market_identity"],
                   "timing_relationship": "AT_OR_BEFORE_PREDICTION" if market["captured_at_utc"] <= prediction["prediction_timestamp_utc"] else "POST_PREDICTION_MARKET_OBSERVATION",
                   "game_pk": game_id, "model_home_probability": model_home,
                   "pinnacle_no_vig_home_probability": book_home, "model_minus_pinnacle_probability": model_home-book_home,
                   "model_pick": prediction["predicted_winner"],
                   "pinnacle_favorite": market["home_team"] if book_home > .5 else market["away_team"]}
        payload["ledger_action"] = append_attachment(conn, table="pinnacle_moneyline_shadow_attachments",
            prediction_identity=identity, market_identity=market["canonical_market_identity"], payload=payload, created_at_utc=created)
        money_out.append(payload)
    return totals_out, money_out, money_status


def run(game_date: str, run_tag: str, output_dir: Path, ledger_path: Path = DEFAULT_LEDGER) -> dict[str, Any]:
    events, source = fetch(game_date, run_tag)
    schedule = []
    schedule_dates = {game_date} | {
        eastern_date(event["commence_time"]) for event in events if event.get("commence_time")
        and eastern_date(event["commence_time"]) >= game_date
    }
    for schedule_date in sorted(schedule_dates):
        schedule_payload, observed, schedule_sha = fetch_hydrated_schedule(schedule_date)
        schedule.extend(normalize_schedule(schedule_payload, observed, schedule_sha))
    rows, audit = parse_events(events=events, schedule=schedule, game_date=game_date,
        fetched_at_utc=source["fetch_timestamp_utc"], run_tag=run_tag,
        raw_source_path=source["raw_response_path"], raw_source_sha256=source["raw_response_sha256"])
    conn = connect_ledger(ledger_path); total_conn = connect_total_ledger(ledger_path)
    actions = [{**row, "ledger_action": append_market(conn, row)} for row in rows]
    current_rows = [row for row in rows if row["game_date"] == game_date]
    future_rows = [row for row in rows if row["event_classification"] == "FUTURE_SLATE_PREGAME"]
    total_rows = [_to_total(row) for row in rows if row["market_type"] == "FULL_GAME_TOTAL"]
    for row in total_rows:
        append_total_market(total_conn, row)
    predictions = rows_for_date(connect_prediction_ledger(TOTALS_PREDICTIONS), game_date)
    for prediction in predictions:
        matching = [row for row in total_rows if int(row["game_id"]) == int(prediction["game_pk"])]
        if matching:
            attach_all_markets(total_conn, prediction, matching, source["fetch_timestamp_utc"])
    all_rows = market_rows(conn, game_date) + _generic_totals(total_market_rows(total_conn, game_date))
    consensus = []
    for game_id in sorted({int(row["game_id"]) for row in current_rows}):
        for market_type in ("MONEYLINE", "FULL_GAME_TOTAL", "RUN_LINE"):
            value = build_consensus(rows=all_rows, game_date=game_date, game_id=game_id,
                                    market_type=market_type, captured_at_utc=source["fetch_timestamp_utc"])
            if value:
                value["ledger_action"] = append_consensus(conn, value); consensus.append(value)
    totals_attach, money_attach, money_status = _attach(conn, game_date, current_rows, source["fetch_timestamp_utc"])
    ages = [(datetime.fromisoformat(source["fetch_timestamp_utc"].replace("Z", "+00:00")) -
             datetime.fromisoformat(row["provider_market_updated_at_utc"].replace("Z", "+00:00"))).total_seconds() for row in rows]
    summary = {**source, "events_returned": len(events), "games_mapped": len({row["game_id"] for row in rows}),
        "moneyline_coverage": len({row["game_id"] for row in rows if row["market_type"] == "MONEYLINE"}),
        "totals_coverage": len({row["game_id"] for row in rows if row["market_type"] == "FULL_GAME_TOTAL"}),
        "run_line_coverage": len({row["game_id"] for row in rows if row["market_type"] == "RUN_LINE"}),
        "post_start_rejects": sum(row["certification_status"] == "POST_START" for row in audit),
        "identity_rejects": sum(row["certification_status"] in {"AMBIGUOUS", "GAME_NOT_FOUND", "TIMING_UNRESOLVED"} for row in audit),
        "observation_age_seconds": {"minimum": min(ages) if ages else None, "median": statistics.median(ages) if ages else None, "maximum": max(ages) if ages else None},
        "market_rows": len(rows), "consensus_rows": len(consensus), "totals_attachments": len(totals_attach),
        "moneyline_attachments": len(money_attach), "moneyline_prediction_source_status": money_status,
        "run_line_model_status": RUN_LINE_MODEL_STATUS, "duplicate_identities": conn.execute(
            "SELECT COUNT(*) FROM (SELECT canonical_market_identity FROM supplemental_main_market_snapshots GROUP BY 1 HAVING COUNT(*)>1)").fetchone()[0],
        "future_slate_events": sum(row["event_classification"] == "FUTURE_SLATE_PREGAME" for row in audit),
        "future_moneyline_rows": sum(row["market_type"] == "MONEYLINE" for row in future_rows),
        "future_totals_rows": sum(row["market_type"] == "FULL_GAME_TOTAL" for row in future_rows),
        "future_run_line_rows": sum(row["market_type"] == "RUN_LINE" for row in future_rows),
        "outcomes_accessed": 0}
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "pinnacle_capture_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
    (output_dir / "pinnacle_identity_audit.json").write_text(json.dumps(audit, indent=2, sort_keys=True) + "\n")
    (output_dir / "pinnacle_model_attachments.json").write_text(json.dumps({"totals": totals_attach, "moneyline": money_attach,
        "run_line": RUN_LINE_MODEL_STATUS}, indent=2, sort_keys=True) + "\n")
    print(json.dumps(summary, indent=2, sort_keys=True)); return summary


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--date", required=True); parser.add_argument("--run-tag", required=True)
    parser.add_argument("--output-dir", type=Path, required=True); parser.add_argument("--ledger-path", type=Path, default=DEFAULT_LEDGER)
    args = parser.parse_args(); run(args.date, args.run_tag, args.output_dir, args.ledger_path)


if __name__ == "__main__":
    main()
