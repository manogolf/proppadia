"""One bounded, non-production The Odds API Pinnacle MLB main-market probe."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from backend.mlb.markets.main_market_provider_replacement_trial_v1 import parse_provider_events
from backend.mlb.public_game_predictions.durable_store_v1 import fetch_prediction_rows
from backend.mlb.scripts.run_mlb_totals_prospective_shadow_v1 import probability_fields
from backend.mlb.totals_predictions.live_context_bridge_v1 import fetch_hydrated_schedule, normalize_schedule
from backend.mlb.totals_predictions.prospective_shadow_v1 import connect_ledger, rows_for_date

ROOT = Path(__file__).resolve().parents[3]
URL = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds"
MARKETS = ("h2h", "totals", "spreads")
MODEL_ALPHA = 0.12944479977012996


def utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def implied(price: float) -> float:
    return 100 / (price + 100) if price > 0 else -price / (-price + 100)


def no_vig(first: float, second: float) -> tuple[float, float]:
    a, b = implied(first), implied(second)
    return a / (a + b), b / (a + b)


def team(value: str) -> str:
    return " ".join(value.casefold().replace(".", "").split())


def bind(event: dict[str, Any], schedule: list[dict[str, Any]]) -> tuple[dict[str, Any] | None, str]:
    candidates = [row for row in schedule if team(row["away_team_name"]) == team(event["away_team"])
                  and team(row["home_team_name"]) == team(event["home_team"])
                  and abs((utc(row["scheduled_start_utc"]) - utc(event["commence_time"])).total_seconds()) <= 600]
    return (candidates[0], "CERTIFIED_EXACT_TEAMS_START_WITHIN_10_MINUTES") if len(candidates) == 1 else (None, "AMBIGUOUS_OR_UNMAPPED")


def outcomes(market: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(row.get("name")): row for row in market.get("outcomes", [])}


def parse_pinnacle(events: list[dict[str, Any]], schedule: list[dict[str, Any]], fetched: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows, audit = [], []
    for event in events:
        game, status = bind(event, schedule)
        pregame = utc(event["commence_time"]) > utc(fetched)
        books = event.get("bookmakers", [])
        audit.append({"provider_event_id": event.get("id"), "game_pk": game.get("game_pk") if game else None,
                      "identity_status": status, "pregame": pregame, "bookmaker_keys": [b.get("key") for b in books]})
        if not game or not pregame:
            continue
        for book in books:
            if book.get("key") != "pinnacle":
                continue
            for market in book.get("markets", []):
                key, by_name = market.get("key"), outcomes(market)
                base = {"game_pk": int(game["game_pk"]), "away_team": event["away_team"], "home_team": event["home_team"],
                        "commence_time": event["commence_time"], "bookmaker_key": book.get("key"),
                        "bookmaker_title": book.get("title"), "market_key": key, "last_update": market.get("last_update")}
                if key == "h2h" and event["away_team"] in by_name and event["home_team"] in by_name:
                    away, home = float(by_name[event["away_team"]]["price"]), float(by_name[event["home_team"]]["price"])
                    away_nv, home_nv = no_vig(away, home)
                    rows.append({**base, "away_price": away, "home_price": home,
                                 "away_no_vig_probability": away_nv, "home_no_vig_probability": home_nv})
                elif key == "totals" and "Over" in by_name and "Under" in by_name and by_name["Over"].get("point") == by_name["Under"].get("point"):
                    over, under = float(by_name["Over"]["price"]), float(by_name["Under"]["price"])
                    over_nv, under_nv = no_vig(over, under)
                    rows.append({**base, "total_line": float(by_name["Over"]["point"]), "over_price": over,
                                 "under_price": under, "over_no_vig_probability": over_nv, "under_no_vig_probability": under_nv})
                elif key == "spreads" and event["away_team"] in by_name and event["home_team"] in by_name:
                    away, home = by_name[event["away_team"]], by_name[event["home_team"]]
                    if float(away["point"]) + float(home["point"]) != 0:
                        continue
                    away_nv, home_nv = no_vig(float(away["price"]), float(home["price"]))
                    rows.append({**base, "away_spread": float(away["point"]), "away_price": float(away["price"]),
                                 "home_spread": float(home["point"]), "home_price": float(home["price"]),
                                 "away_no_vig_probability": away_nv, "home_no_vig_probability": home_nv})
    return rows, audit


def consensus(rows: list[dict[str, Any]], game_pk: int, market: str) -> dict[str, Any] | None:
    values = [row for row in rows if int(row["game_id"]) == game_pk and row["market_type"] == market]
    if not values:
        return None
    if market == "MONEYLINE":
        return {"home_probability": statistics.median(float(x["no_vig_home_probability"]) for x in values)}
    line_name = "total_line" if market == "FULL_GAME_TOTAL" else "home_spread"
    line = statistics.median(float(x[line_name]) for x in values)
    same = [x for x in values if float(x[line_name]) == line]
    return {"line": line, "probability": statistics.median(float(x["no_vig_over_probability"] if market == "FULL_GAME_TOTAL" else x["no_vig_home_probability"]) for x in same)}


def latest_sgo(game_date: str, schedule: list[dict[str, Any]]) -> list[dict[str, Any]]:
    paths = sorted((ROOT / "backend/mlb/exports/market_history/sportsgameodds_main_market/raw" / game_date).glob("*/sportsgameodds_response.json"))
    if not paths:
        return []
    path = paths[-1]; payload = json.loads(path.read_text()); fetched = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat()
    rows, _ = parse_provider_events(events=payload["data"], schedule=schedule, game_date=game_date, fetched_at_utc=fetched,
                                    run_tag=path.parent.name, raw_source_path=str(path.relative_to(ROOT)),
                                    raw_source_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
    return rows


def existing_oddsapi_totals(game_date: str, schedule: list[dict[str, Any]]) -> list[dict[str, Any]]:
    paths = sorted((ROOT / "backend/mlb/exports/odds_history" / game_date).glob("odds_mlb_full_game_totals__*.json"))
    if not paths:
        return []
    data = json.loads(paths[-1].read_text()); result = []
    for event in data.get("events", []):
        game, status = bind(event, schedule)
        if not game or status.startswith("AMBIGUOUS"):
            continue
        for book in event.get("bookmakers", []):
            for market in book.get("markets", []):
                by_name = outcomes(market)
                if market.get("key") == "totals" and "Over" in by_name and "Under" in by_name and by_name["Over"].get("point") == by_name["Under"].get("point"):
                    over, under = float(by_name["Over"]["price"]), float(by_name["Under"]["price"])
                    result.append({"game_pk": int(game["game_pk"]), "line": float(by_name["Over"]["point"]), "probability": no_vig(over, under)[0]})
    return result


def run(game_date: str, output: Path, reuse_retained_response: bool = False) -> dict[str, Any]:
    api_key = os.getenv("ODDS_API_KEY", "").strip()
    if not api_key and not reuse_retained_response:
        raise RuntimeError("ODDS_API_KEY missing")
    output.mkdir(parents=True, exist_ok=reuse_retained_response)
    raw_path = output / "pinnacle_response.json"
    params = {"apiKey": api_key, "bookmakers": "pinnacle", "markets": ",".join(MARKETS), "oddsFormat": "american", "dateFormat": "iso"}
    if reuse_retained_response:
        if not raw_path.is_file():
            raise RuntimeError("retained Pinnacle response missing")
        content = raw_path.read_bytes(); events = json.loads(content)
        if not isinstance(events, list):
            raise RuntimeError("retained response is not a successful odds list")
        requested_at = fetched = iso(datetime.fromtimestamp(raw_path.stat().st_mtime, timezone.utc))
        latency = None; status = 200
        headers = {key: None for key in ("x-requests-remaining", "x-requests-used", "x-requests-last")}
        header_status = "UNAVAILABLE_LOCAL_MANIFEST_FAILURE_AFTER_SINGLE_REQUEST_NO_RETRY_PERMITTED"
    else:
        requested_at = iso(datetime.now(timezone.utc)); started = time.monotonic()
        response = requests.get(URL, params=params, timeout=45)
        fetched = iso(datetime.now(timezone.utc)); latency = time.monotonic() - started
        headers = {key: response.headers.get(key) for key in ("x-requests-remaining", "x-requests-used", "x-requests-last")}
        content = response.content; raw_path.write_bytes(content); response.raise_for_status(); events = response.json(); status = response.status_code
        header_status = "PRESERVED"
    manifest = {"endpoint": "/v4/sports/baseball_mlb/odds", "sport_key": "baseball_mlb",
                "request_parameters_without_secret": {k: v for k, v in params.items() if k != "apiKey"},
                "requested_at_utc": requested_at, "fetch_timestamp_utc": fetched, "http_status": status,
                "response_latency_seconds": latency, "request_headers": headers, "request_header_retention_status": header_status,
                "raw_response_path": str(raw_path.resolve().relative_to(ROOT)), "raw_response_sha256": hashlib.sha256(content).hexdigest()}
    (output / "request_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    schedule_payload, observed, schedule_hash = fetch_hydrated_schedule(game_date)
    schedule = normalize_schedule(schedule_payload, observed, schedule_hash)
    rows, audit = parse_pinnacle(events, schedule, fetched)
    sgo = latest_sgo(game_date, schedule); existing = existing_oddsapi_totals(game_date, schedule)
    totals_models = {int(x["game_pk"]): x for x in rows_for_date(connect_ledger(ROOT / "backend/mlb/exports/model_v2/totals_shadow_v1/totals_shadow_v1.sqlite3"), game_date)}
    try:
        money_models = {int(x["game_id"]): x for x in fetch_prediction_rows(game_date)}; money_status = "AVAILABLE"
    except Exception as exc:
        money_models = {}; money_status = f"UNAVAILABLE:{type(exc).__name__}"
    comparisons = []
    for row in rows:
        game_pk, key = row["game_pk"], row["market_key"]
        market_type = {"h2h": "MONEYLINE", "totals": "FULL_GAME_TOTAL", "spreads": "RUN_LINE"}[key]
        con = consensus(sgo, game_pk, market_type)
        bookmaker = next((x for x in sgo if int(x["game_id"]) == game_pk and x["market_type"] == market_type and x["bookmaker_provider_id"] == "bookmakereu"), None)
        item = {**row, "sportsgameodds_consensus": con, "sportsgameodds_bookmaker_eu": bookmaker}
        if key == "totals":
            existing_rows = [x for x in existing if x["game_pk"] == game_pk]
            item["existing_the_odds_api_consensus"] = ({"line": statistics.median(x["line"] for x in existing_rows),
                "probability": statistics.median(x["probability"] for x in existing_rows if x["line"] == statistics.median(y["line"] for y in existing_rows))} if existing_rows else None)
            model = totals_models.get(game_pk)
            if model:
                probs = probability_fields(float(model["expected_total"]), MODEL_ALPHA, float(row["total_line"]))
                item["model_comparison"] = {"expected_total": model["expected_total"], "model_minus_pinnacle": float(model["expected_total"])-float(row["total_line"]), **probs}
        elif key == "h2h":
            model = money_models.get(game_pk)
            if model:
                item["model_comparison"] = {"home_win_probability": model["home_win_probability"], "away_win_probability": model["away_win_probability"],
                    "home_probability_difference": float(model["home_win_probability"])-float(row["home_no_vig_probability"]),
                    "predicted_winner": model["predicted_winner"], "pinnacle_favorite": row["home_team"] if row["home_no_vig_probability"] > .5 else row["away_team"]}
        else:
            item["model_comparison"] = "MODEL_COMPARISON_UNAVAILABLE_NO_QUALIFIED_RUN_LINE_MODEL"
        comparisons.append(item)
    ages = [(utc(fetched)-utc(x["last_update"])).total_seconds() for x in rows if x.get("last_update")]
    summary = {"decision": "THE_ODDS_API_PINNACLE_CAPTURE_READY_WITH_DELAY_CAVEAT" if rows else "THE_ODDS_API_PINNACLE_NOT_RETURNED",
               "request": manifest, "events_returned": len(events), "bookmaker_keys": sorted({b.get("key") for e in events for b in e.get("bookmakers", [])}),
               "bookmaker_titles": sorted({b.get("title") for e in events for b in e.get("bookmakers", [])}),
               "pregame_games_covered": len({x["game_pk"] for x in rows}),
               "coverage": {key: len({x["game_pk"] for x in rows if x["market_key"] == key}) for key in MARKETS},
               "observation_age_seconds": {"minimum": min(ages) if ages else None, "median": statistics.median(ages) if ages else None, "maximum": max(ages) if ages else None,
                                           "label": "THE_ODDS_API_PINNACLE_OBSERVATION_AGE"},
               "moneyline_model_source_status": money_status, "identity_audit": audit, "comparisons": comparisons,
               "historical_access_status": "THE_ODDS_API_PINNACLE_HISTORY_AVAILABLE_CURRENT_PLAN",
               "request_cost_interpretation": {"explicit_pinnacle_three_markets": headers["x-requests-last"],
                   "broad_us_plus_eu_three_markets_documented_cost": 6, "separate_pinnacle_three_markets_documented_cost": 3,
                   "eu_region_effect": "returns unwanted EU books; cost is one region unit per market, not per returned bookmaker"}}
    (output / "probe_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n")
    with (output / "pinnacle_main_markets.csv").open("w", newline="") as handle:
        names = sorted({k for row in rows for k in row}); writer = csv.DictWriter(handle, names); writer.writeheader(); writer.writerows(rows)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--date", required=True); parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--reuse-retained-response", action="store_true")
    args = parser.parse_args(); result = run(args.date, args.output_dir, args.reuse_retained_response)
    print(json.dumps({k: result[k] for k in ("decision", "events_returned", "bookmaker_keys", "bookmaker_titles", "pregame_games_covered", "coverage", "observation_age_seconds", "historical_access_status")}, indent=2))


if __name__ == "__main__":
    main()
