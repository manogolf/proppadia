"""Run one SportsGameOdds/The Odds API MLB main-market shadow comparison."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import statistics
import time as time_module
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests

from backend.mlb.markets.bookmaker_eu_supplemental_v1 import (
    append_event_discovery, connect_ledger, mark_first_observed_prices,
)
from backend.mlb.markets.full_game_total_capture_v1 import (
    market_rows as total_market_rows,
)
from backend.mlb.markets.main_market_provider_replacement_trial_v1 import (
    BOOK_DISPLAY_NAMES, EXPERIMENT, MARKETS, PRIORITY_BOOKS, PROVIDER,
    append_reliability, append_shadow_attachment, canonical_book_id, compare_provider_rows, consensus_metrics,
    freshness_metrics, parse_provider_events, reliability_rows, sha256_json, utc,
)
from backend.mlb.markets.pinnacle_main_market_capture_v1 import eastern_date
from backend.mlb.public_game_predictions.durable_store_v1 import fetch_prediction_rows as fetch_moneyline_predictions
from backend.mlb.scripts.capture_mlb_bookmaker_eu_supplemental_v1 import _bounded_read
from backend.mlb.scripts.run_mlb_totals_prospective_shadow_v1 import probability_fields
from backend.mlb.totals_predictions.live_context_bridge_v1 import fetch_hydrated_schedule, normalize_schedule
from backend.mlb.totals_predictions.prospective_shadow_v1 import (
    connect_ledger as connect_prediction_ledger,
    rows_for_date as totals_predictions_for_date,
)

ROOT = Path(__file__).resolve().parents[3]
API_URL = "https://api.sportsgameodds.com/v2/events"
DEFAULT_LEDGER = ROOT / "backend/mlb/exports/market_history/full_game_totals/full_game_totals_v1.sqlite3"
RAW_ROOT = ROOT / "backend/mlb/exports/market_history/sportsgameodds_main_market/raw"
DEFAULT_PACKAGE = ROOT / "artifacts/analysis/model_development/mlb_main_market_provider_replacement_trial_v1"
TOTALS_PREDICTION_LEDGER = ROOT / "backend/mlb/exports/model_v2/totals_shadow_v1/totals_shadow_v1.sqlite3"
TOTALS_MODEL_ALPHA = 0.12944479977012996
DECISION = "PROVIDER_TRIAL_EVIDENCE_INSUFFICIENT"


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    names = fields or sorted({key for row in rows for key in row})
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=names, extrasaction="ignore")
        writer.writeheader(); writer.writerows(rows)


def _day_bounds(game_date: str) -> tuple[str, str]:
    pacific = ZoneInfo("America/Los_Angeles")
    value = datetime.fromisoformat(game_date).date()
    start = datetime.combine(value, time.min, tzinfo=pacific).astimezone(timezone.utc)
    end = datetime.combine(value + timedelta(days=1), time.max, tzinfo=pacific).astimezone(timezone.utc)
    return start.isoformat().replace("+00:00", "Z"), end.isoformat().replace("+00:00", "Z")


def fetch_current(game_date: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    key = os.getenv("SPORTSGAMEODDSAPI", "").strip()
    if not key:
        raise RuntimeError("SPORTSGAMEODDS_AUTH_MISSING:SPORTSGAMEODDSAPI")
    _, day_end = _day_bounds(game_date)
    started = now_utc()
    params = {
        "leagueID": "MLB", "oddsAvailable": "true",
        "oddID": ",".join(odd_id for market in MARKETS.values() for odd_id in market.values()),
        "startsAfter": started, "startsBefore": day_end, "limit": "100",
    }
    monotonic = time_module.monotonic()
    response = requests.get(API_URL, params=params, headers={"x-api-key": key}, timeout=45)
    latency = time_module.monotonic() - monotonic
    fetched = now_utc(); response.raise_for_status(); payload = response.json()
    if not isinstance(payload, dict) or payload.get("success") is not True or not isinstance(payload.get("data"), list):
        raise RuntimeError("SPORTSGAMEODDS_UNEXPECTED_RESPONSE")
    run_tag = "sgo_main_market_" + utc(fetched).strftime("%Y%m%dT%H%M%SZ")
    raw_dir = RAW_ROOT / game_date / run_tag; raw_dir.mkdir(parents=True, exist_ok=False)
    raw_path = raw_dir / "sportsgameodds_response.json"; raw_path.write_bytes(response.content)
    raw_sha = hashlib.sha256(raw_path.read_bytes()).hexdigest()
    raw_display = raw_path.relative_to(ROOT) if raw_path.is_relative_to(ROOT) else raw_path
    source = {
        "experiment": EXPERIMENT, "provider": PROVIDER, "game_date": game_date,
        "fetch_timestamp_utc": fetched, "run_tag": run_tag,
        "request_class": "CURRENT_MLB_PREGAME_PROVIDER_WIDE_CANONICAL_MAIN_MARKETS",
        "request_parameters_without_secret": params, "authentication_transport": "x-api-key header; value not retained",
        "http_status": response.status_code, "provider_event_count": len(payload["data"]),
        "provider_notice": payload.get("notice"), "raw_response_path": str(raw_display),
        "raw_response_sha256": raw_sha, "request_count": 1, "response_latency_seconds": latency,
        "quota_requests_used": response.headers.get("x-requests-used"),
        "quota_requests_remaining": response.headers.get("x-requests-remaining"),
        "quota_request_cost": response.headers.get("x-requests-last"), "provider_counted_entities": None,
    }
    manifest = raw_dir / "run_manifest.json"; manifest.write_text(json.dumps(source, indent=2, sort_keys=True) + "\n")
    source["run_manifest_path"] = str(manifest.relative_to(ROOT) if manifest.is_relative_to(ROOT) else manifest)
    return payload["data"], source


def load_probe(raw_path: Path, game_date: str, captured_at_utc: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    raw_path = raw_path.resolve(); payload = json.loads(raw_path.read_text())
    if payload.get("success") is not True or not isinstance(payload.get("data"), list):
        raise RuntimeError("INVALID_PROVIDER_WIDE_PROBE")
    raw_sha = hashlib.sha256(raw_path.read_bytes()).hexdigest()
    display = raw_path.relative_to(ROOT) if raw_path.is_relative_to(ROOT) else raw_path
    return payload["data"], {
        "experiment": EXPERIMENT, "provider": PROVIDER, "game_date": game_date,
        "fetch_timestamp_utc": captured_at_utc,
        "run_tag": "sgo_probe_" + utc(captured_at_utc).strftime("%Y%m%dT%H%M%SZ"),
        "request_class": "AUTHENTIC_CURRENT_PROVIDER_WIDE_PROBE_REPLAY",
        "http_status": 200, "provider_event_count": len(payload["data"]), "provider_notice": payload.get("notice"),
        "raw_response_path": str(display), "raw_response_sha256": raw_sha,
        "request_count": 0, "original_provider_request_count": 1, "response_latency_seconds": None,
        "quota_requests_used": None, "quota_requests_remaining": None, "quota_request_cost": None,
        "provider_counted_entities": None, "run_manifest_path": None,
    }


def odds_api_rows(conn: Any, game_date: str, reference_utc: str, maximum_age_minutes: int,
                  source_succeeded: bool) -> list[dict[str, Any]]:
    if not source_succeeded:
        return []
    values = [row for row in total_market_rows(conn, game_date) if not str(row["bookmaker_key"]).startswith("sportsgameodds:")]
    if not values:
        return []
    latest = max(str(row["captured_at_utc"]) for row in values)
    if abs((utc(reference_utc) - utc(latest)).total_seconds()) > maximum_age_minutes * 60:
        return []
    return [{**row, "provider": "THE_ODDS_API", "market_type": "FULL_GAME_TOTAL"}
            for row in values if str(row["captured_at_utc"]) == latest]


def to_total_row(row: dict[str, Any]) -> dict[str, Any]:
    return {**row, "total_line": float(row["total_line"]), "over_price": int(row["over_american_price"]),
            "under_price": int(row["under_american_price"]),
            "provider_market_timestamp_utc": row["provider_market_updated_at_utc"],
            "market_status": "TOTAL_MARKET_CERTIFIED_PAIRED"}


def append_provider_rows_batch(conn: Any, rows: list[dict[str, Any]]) -> list[str]:
    """Batch the large provider-wide population into one SQLite transaction."""
    identities = {row[0]: row[1] for row in conn.execute(
        "SELECT canonical_market_identity,market_payload_sha256 FROM supplemental_main_market_snapshots"
    )}
    actions, inserts = [], []
    for row in rows:
        identity = row["canonical_market_identity"]; digest = sha256_json(row)
        if identity in identities:
            actions.append("EXISTING_IMMUTABLE" if identities[identity] == digest else "EXISTING_CONFLICT_PRESERVED")
            continue
        inserts.append((identity, row["provider"], row["bookmaker_key"], row["game_date"], row["game_id"],
                        row["market_type"], row["line_key"], row["captured_at_utc"], row["scheduled_start_utc"],
                        row["timing_status"], json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False),
                        digest, row["raw_source_path"], row["raw_source_sha256"]))
        actions.append("APPENDED_NEW")
    with conn:
        conn.executemany("INSERT INTO supplemental_main_market_snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", inserts)
    return actions


def append_total_rows_batch(conn: Any, rows: list[dict[str, Any]]) -> list[str]:
    identities = {row[0]: row[1] for row in conn.execute(
        "SELECT canonical_market_identity,market_payload_sha256 FROM full_game_total_market_snapshots"
    )}
    actions, inserts = [], []
    for source in rows:
        if source["market_type"] != "FULL_GAME_TOTAL":
            continue
        row = to_total_row(source); identity = row["canonical_market_identity"]; digest = sha256_json(row)
        if identity in identities:
            actions.append("EXISTING_IMMUTABLE" if identities[identity] == digest else "EXISTING_CONFLICT_PRESERVED")
            continue
        inserts.append((identity, row["game_date"], row["game_id"], row["bookmaker_key"], row["market_type"],
                        row["captured_at_utc"], row["total_line"],
                        json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=False), digest,
                        row["raw_source_path"], row["raw_source_sha256"]))
        actions.append("APPENDED_NEW")
    with conn:
        conn.executemany("INSERT INTO full_game_total_market_snapshots VALUES (?,?,?,?,?,?,?,?,?,?,?)", inserts)
    return actions


def bookmaker_coverage(rows: list[dict[str, Any]], events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    accessible = sorted({book for event in events for book in _event_books(event)})
    output = []
    for book in accessible:
        values = [row for row in rows if row["bookmaker_provider_id"] == book]
        by_market = {market: [row for row in values if row["market_type"] == market]
                     for market in ("MONEYLINE", "FULL_GAME_TOTAL", "RUN_LINE")}
        output.append({"provider": PROVIDER, "bookmaker_id": book,
                       "bookmaker_display_name": BOOK_DISPLAY_NAMES.get(book, book),
                       "games_with_any_paired_market": len({row["game_id"] for row in values}),
                       "moneyline_games": len({row["game_id"] for row in by_market["MONEYLINE"]}),
                       "full_game_total_games": len({row["game_id"] for row in by_market["FULL_GAME_TOTAL"]}),
                       "run_line_games": len({row["game_id"] for row in by_market["RUN_LINE"]}),
                       "paired_market_rows": len(values),
                       "update_timestamp_rows": sum(bool(row.get("provider_market_updated_at_utc")) for row in values),
                       "live_feed_availability": "AVAILABLE_PAIRED" if values else "SIDES_PRESENT_NO_VALID_PAIR"})
    return output


def _event_books(event: dict[str, Any]) -> set[str]:
    values = set()
    for market in MARKETS.values():
        for odd_id in market.values():
            for book, detail in (((event.get("odds") or {}).get(odd_id) or {}).get("byBookmaker") or {}).items():
                if detail.get("available"):
                    values.add(book)
    return values


def priority_coverage(coverage: list[dict[str, Any]], odds_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    live = {row["bookmaker_id"]: row for row in coverage}
    odds_books = {canonical_book_id("THE_ODDS_API", row["bookmaker_key"]) for row in odds_rows}
    output = []
    for name, contract in PRIORITY_BOOKS.items():
        ids = contract["live_ids"]
        found = next((book for book in ids if book in live), None)
        row = live.get(found or "", {})
        output.append({"priority_book": name, "display_name": contract["display_name"],
                       "candidate_live_ids": "|".join(ids), "exact_live_bookmaker_id": found,
                       "sportsgameodds_live_availability": "AVAILABLE" if found else "NOT_RETURNED_LIVE",
                       "games_covered": row.get("games_with_any_paired_market", 0),
                       "moneyline_games": row.get("moneyline_games", 0),
                       "full_game_total_games": row.get("full_game_total_games", 0),
                       "run_line_games": row.get("run_line_games", 0),
                       "the_odds_api_same_book_present": any(book in odds_books for book in ids)})
    return output


def common_book_consensus(sgo: list[dict[str, Any]], odds: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sgo_books = {canonical_book_id(PROVIDER, row["bookmaker_key"]) for row in sgo}
    odds_books = {canonical_book_id("THE_ODDS_API", row["bookmaker_key"]) for row in odds}
    common = sgo_books & odds_books
    sgo_common = [row for row in sgo if canonical_book_id(PROVIDER, row["bookmaker_key"]) in common]
    odds_common = [row for row in odds if canonical_book_id("THE_ODDS_API", row["bookmaker_key"]) in common]
    return (consensus_metrics(sgo_common, PROVIDER, "COMMON_BOOKS_SPORTSGAMEODDS") +
            consensus_metrics(odds_common, "THE_ODDS_API", "COMMON_BOOKS_THE_ODDS_API"))


def attach_model_context(conn: Any, game_date: str, rows: list[dict[str, Any]],
                         consensus: list[dict[str, Any]], captured_at: str) -> dict[str, Any]:
    """Attach immutable observations without changing or creating predictions."""
    total_predictions = totals_predictions_for_date(connect_prediction_ledger(TOTALS_PREDICTION_LEDGER), game_date)
    total_by_game = {int(row["game_pk"]): row for row in total_predictions}
    try:
        with _bounded_read(20):
            money_predictions = fetch_moneyline_predictions(game_date)
        money_status = "AVAILABLE"
    except Exception as exc:
        money_predictions = []
        money_status = f"UNAVAILABLE:{type(exc).__name__}"
    money_by_game = {int(row.get("game_id", row.get("game_pk"))): row for row in money_predictions}
    priority_ids = {book for contract in PRIORITY_BOOKS.values() for book in contract["live_ids"]}
    actions = []
    for market in rows:
        book = market["bookmaker_provider_id"]
        if book not in priority_ids:
            continue
        game_id = int(market["game_id"])
        if market["market_type"] == "FULL_GAME_TOTAL" and game_id in total_by_game:
            prediction = total_by_game[game_id]
            prediction_identity = (f"{prediction['game_date']}|{game_id}|{prediction['model_version']}|"
                                   f"{prediction['prediction_snapshot_class']}")
            payload = {"prediction_identity": prediction_identity,
                       "market_identity": market["canonical_market_identity"], "provider_view": f"SPORTSGAMEODDS:{book}",
                       "market_type": "FULL_GAME_TOTAL", "created_at_utc": captured_at,
                       "timing_relationship": ("AT_OR_BEFORE_PREDICTION" if utc(captured_at) <= utc(prediction["prediction_timestamp_utc"])
                                               else "POST_PREDICTION_MARKET_OBSERVATION"),
                       "model_expected_total": float(prediction["expected_total"]), "market_total_line": float(market["total_line"]),
                       **probability_fields(float(prediction["expected_total"]), TOTALS_MODEL_ALPHA, float(market["total_line"]))}
            actions.append(append_shadow_attachment(conn, payload))
        elif market["market_type"] == "MONEYLINE" and game_id in money_by_game:
            prediction = money_by_game[game_id]
            prediction_identity = (f"{prediction['game_date']}|{game_id}|{prediction['winner_model_version']}|"
                                   f"{prediction['prediction_snapshot_class']}")
            payload = {"prediction_identity": prediction_identity,
                       "market_identity": market["canonical_market_identity"], "provider_view": f"SPORTSGAMEODDS:{book}",
                       "market_type": "MONEYLINE", "created_at_utc": captured_at,
                       "timing_relationship": ("AT_OR_BEFORE_PREDICTION" if utc(captured_at) <= utc(prediction["prediction_timestamp_utc"])
                                               else "POST_PREDICTION_MARKET_OBSERVATION"),
                       "model_home_probability": float(prediction["home_win_probability"]),
                       "market_no_vig_home_probability": float(market["no_vig_home_probability"])}
            actions.append(append_shadow_attachment(conn, payload))
    # Research rows keep model, provider consensus, and named priority books separate.
    sgo_money = {(int(v["game_id"]), v["market_type"]): v for v in consensus if v["consensus_scope"] == "SPORTSGAMEODDS_ALL_BOOKS"}
    for value in consensus:
        game_id = int(value["game_id"]); market = value["market_type"]
        if market == "FULL_GAME_TOTAL" and game_id in total_by_game:
            value["model_expected_total"] = float(total_by_game[game_id]["expected_total"])
        if market == "MONEYLINE" and game_id in money_by_game:
            value["model_home_probability"] = float(money_by_game[game_id]["home_win_probability"])
        source_rows = [row for row in rows if int(row["game_id"]) == game_id and row["market_type"] == market]
        for label, book in (("bookmaker_eu", "bookmakereu"), ("pinnacle", "pinnacle")):
            observation = next((row for row in source_rows if row["bookmaker_provider_id"] == book), None)
            value[f"{label}_observation"] = (
                observation.get("total_line", observation.get("no_vig_home_probability")) if observation else None
            )
        provider_consensus = sgo_money.get((game_id, market))
        value["sportsgameodds_consensus_value"] = (
            provider_consensus.get("median_line", provider_consensus.get("median_no_vig_home_probability"))
            if provider_consensus else None
        )
    return {"totals_predictions_read": len(total_predictions), "moneyline_predictions_read": len(money_predictions),
            "moneyline_prediction_source_status": money_status, "attachment_actions": len(actions),
            "attachments_appended": actions.count("APPENDED_NEW")}


def reliability_payloads(source: dict[str, Any], schedule: list[dict[str, Any]], rows: list[dict[str, Any]],
                         audit: list[dict[str, Any]], odds: list[dict[str, Any]], odds_api_run_status: int) -> list[dict[str, Any]]:
    priority = {name: any(row["bookmaker_provider_id"] in contract["live_ids"] for row in rows)
                for name, contract in PRIORITY_BOOKS.items()}
    sgo = {"provider": PROVIDER, "game_date": source["game_date"], "captured_at_utc": source["fetch_timestamp_utc"],
           "source_run_tag": source["run_tag"], "request_success": True, "http_status": source["http_status"],
           "response_latency_seconds": source.get("response_latency_seconds"), "games_expected": len(schedule),
           "games_returned": source["provider_event_count"], "games_mapped": len({row["game_id"] for row in rows}),
           "markets_parsed": len(rows), "priority_books_present": "|".join(name for name, yes in priority.items() if yes),
           "malformed_rows": sum(row.get("malformed_market_rows", 0) for row in audit),
           "identity_failures": sum(row["certification_status"] in {"AMBIGUOUS", "GAME_NOT_FOUND", "TIMING_UNRESOLVED"} for row in audit),
           "post_start_rejections": sum(row["certification_status"] == "POST_START" for row in audit),
           "http_errors": 0, "quota_rate_limit_errors": 0, "provider_notice": source.get("provider_notice")}
    if odds:
        captured = max(row["captured_at_utc"] for row in odds)
        odds_payload = {"provider": "THE_ODDS_API", "game_date": source["game_date"], "captured_at_utc": captured,
                        "source_run_tag": max(row["source_run_tag"] for row in odds), "request_success": True,
                        "http_status": 200, "response_latency_seconds": None, "games_expected": len(schedule),
                        "games_returned": len({row["game_id"] for row in odds}),
                        "games_mapped": len({row["game_id"] for row in odds}), "markets_parsed": len(odds),
                        "priority_books_present": "", "malformed_rows": 0, "identity_failures": 0,
                        "post_start_rejections": 0, "http_errors": 0, "quota_rate_limit_errors": 0,
                        "provider_notice": "CURRENT_PROJECT_CAPTURE_IS_FULL_GAME_TOTAL_ONLY"}
        return [sgo, odds_payload]
    return [sgo, {"provider": "THE_ODDS_API", "game_date": source["game_date"],
                  "captured_at_utc": source["fetch_timestamp_utc"],
                  "source_run_tag": f"same_refresh_status_{source['run_tag']}",
                  "request_success": odds_api_run_status == 0, "http_status": None,
                  "response_latency_seconds": None, "games_expected": len(schedule), "games_returned": 0,
                  "games_mapped": 0, "markets_parsed": 0, "priority_books_present": "",
                  "malformed_rows": 0, "identity_failures": 0, "post_start_rejections": 0,
                  "http_errors": int(odds_api_run_status != 0), "quota_rate_limit_errors": 0,
                  "provider_notice": ("SOURCE_FAILURE" if odds_api_run_status != 0
                                      else "SUCCESSFUL_CAPTURE_NO_FRESH_COMPARISON_ROWS")}]


def write_package(output: Path, *, source: dict[str, Any], coverage: list[dict[str, Any]], overlap: list[dict[str, Any]],
                  consensus: list[dict[str, Any]], freshness: list[dict[str, Any]], reliability: list[dict[str, Any]],
                  priority: list[dict[str, Any]], rows: list[dict[str, Any]], odds: list[dict[str, Any]],
                  schedule: list[dict[str, Any]], audit: list[dict[str, Any]]) -> None:
    output.mkdir(parents=True, exist_ok=True)
    write_csv(output / "sportsgameodds_bookmaker_coverage.csv", coverage)
    write_csv(output / "provider_overlap_comparison.csv", overlap)
    write_csv(output / "provider_market_consensus_comparison.csv", consensus)
    write_csv(output / "provider_freshness_comparison.csv", freshness)
    write_csv(output / "provider_reliability_ledger.csv", reliability)
    quota = [
        {"provider": PROVIDER, "capture_requests": source.get("original_provider_request_count", source.get("request_count", 1)),
         "projected_requests_at_five_daily_30_day_month": 150, "entities_or_units_exposed": source.get("provider_counted_entities"),
         "quota_remaining": source.get("quota_requests_remaining"), "request_cost_exposed": source.get("quota_request_cost"),
         "locally_observed_account_tier": "rookie", "locally_observed_monthly_entity_limit": 100000,
         "locally_observed_daily_request_limit": 500000,
         "locally_observed_counter_after_initial_three_request_probe": "3 requests / 6 entities daily; 6 entities monthly",
         "subscription_price_local_evidence": "NOT_ESTABLISHED", "rate_limit_failures": 0,
         "notes": "One provider-wide event request per refresh; per-capture entity cost and remaining quota were not exposed in the event response and are not inferred."},
        {"provider": "THE_ODDS_API", "capture_requests": 1 if odds else 0,
         "projected_requests_at_five_daily_30_day_month": 150, "entities_or_units_exposed": "NOT_RETAINED_IN_CURRENT_CAPTURE",
         "quota_remaining": "NOT_RETAINED_IN_CURRENT_CAPTURE", "request_cost_exposed": "NOT_RETAINED_IN_CURRENT_CAPTURE",
         "locally_observed_account_tier": "NOT_ESTABLISHED", "locally_observed_monthly_entity_limit": "NOT_ESTABLISHED",
         "locally_observed_daily_request_limit": "NOT_ESTABLISHED",
         "locally_observed_counter_after_initial_three_request_probe": "NOT_APPLICABLE",
         "subscription_price_local_evidence": "NOT_ESTABLISHED", "rate_limit_failures": 0,
         "notes": "No cost claim inferred; current project capture is totals-only."},
    ]
    write_csv(output / "provider_quota_cost_audit.csv", quota)
    write_csv(output / "priority_book_coverage.csv", priority)
    historical = """# Historical capability audit

- Bounded SportsGameOdds August 5 availability request: zero currently-available events.
- Bounded August 4 control: one completed event was accessible, but its bookmaker observations were post-start/unavailable for authentic pregame certification.
- BookMaker.eu historical pregame moneyline/total/run-line capability: `NOT_CERTIFIED`.
- Pinnacle historical pregame moneyline/total/run-line capability: `NOT_CERTIFIED`.
- No bulk history was acquired. Completed-event and post-start observations remain distinct from authentic pregame history.
- Historical usefulness decision: `BOUNDED_CHARACTERIZATION_INCONCLUSIVE`.
"""
    (output / "historical_capability_audit.md").write_text(historical)
    priority_by_name = {row["priority_book"]: row for row in priority}
    # The overlap ledger may contain an explicit SOURCE_FAILURE diagnostic row.
    # Such rows deliberately do not claim provider-presence fields.
    common = {
        row["canonical_bookmaker_id"] for row in overlap
        if row.get("sportsgameodds_present") and row.get("the_odds_api_present")
    }
    unique = sorted({row["bookmaker_provider_id"] for row in rows} - {canonical_book_id("THE_ODDS_API", row["bookmaker_key"]) for row in odds})
    meaningful_unique = [book for book in ("bookmakereu", "caesars", "espnbet", "hardrockbet") if book in unique]
    rates = {}
    for provider in {row["provider"] for row in reliability}:
        provider_rows = [row for row in reliability if row["provider"] == provider]
        rates[provider] = 100.0 * sum(bool(row.get("request_success")) for row in provider_rows) / len(provider_rows)
    progress = f"""# MLB Main-Market Provider Replacement Trial v1 progress

Decision: `{DECISION}`

- Trial capture: `{source['run_tag']}` at `{source['fetch_timestamp_utc']}`
- Official games / SportsGameOdds games mapped: {len(schedule)} / {len({row['game_id'] for row in rows})}
- Accessible live bookmaker IDs: {len(coverage)}
- Paired market rows: {len(rows)}
- The Odds API comparison rows: {len(odds)} (full-game totals only in the current project capture)
- Exact overlapping canonical books: {len(common)} (`{'|'.join(sorted(common)) or 'NONE'}`)
- Provider notices: `{source.get('provider_notice') or 'NONE'}`
- Identity failures / malformed rows: {sum(row['certification_status'] in {'AMBIGUOUS','GAME_NOT_FOUND','TIMING_UNRESOLVED'} for row in audit)} / {sum(row.get('malformed_market_rows',0) for row in audit)}
- Currently meaningful conventional/reference books unique to this captured comparison: `{'|'.join(meaningful_unique) or 'NONE'}`. This is availability evidence, not a retention decision.
- All currently unique IDs (including exchanges/international/trial labels): `{'|'.join(unique) or 'NONE'}`.
- Successful-refresh rate in the living ledger: SportsGameOdds {rates.get(PROVIDER, 0):.2f}%; The Odds API {rates.get('THE_ODDS_API', 0):.2f}%.
- The Odds API remains active and unchanged. No replacement decision is authorized from this capture.

## Priority books

| Book | SportsGameOdds live | Games | ML | Total | Run line | The Odds API same book |
| --- | --- | ---: | ---: | ---: | ---: | --- |
"""
    for key in ("BOOKMAKER_EU", "PINNACLE", "CIRCA", "PRIME_SPORTS"):
        row = priority_by_name[key]
        progress += f"| {row['display_name']} | {row['sportsgameodds_live_availability']} | {row['games_covered']} | {row['moneyline_games']} | {row['full_game_total_games']} | {row['run_line_games']} | {row['the_odds_api_same_book_present']} |\n"
    progress += "\n## Readiness\n\nThe living ledgers, same-run provider comparison, provider-specific/common-book consensus, freshness audit, quota audit, and existing-cadence hook are initialized. Multiple natural refreshes are still required before replacement readiness can be decided.\n"
    (output / "provider_replacement_progress.md").write_text(progress)
    concise = f"""# Concise provider replacement trial

`{DECISION}`

- SportsGameOdds live bookmakers: {len(coverage)}
- Games with paired SportsGameOdds main markets: {len({row['game_id'] for row in rows})}
- BookMaker.eu: {priority_by_name['BOOKMAKER_EU']['sportsgameodds_live_availability']}
- Pinnacle: {priority_by_name['PINNACLE']['sportsgameodds_live_availability']}
- Circa: {priority_by_name['CIRCA']['sportsgameodds_live_availability']}
- Prime Sports: {priority_by_name['PRIME_SPORTS']['sportsgameodds_live_availability']}
- Provider-wide request cost: one `/v2/events` request per refresh; provider entity/cost counters unavailable in this response.
- Schema: two append-only trial tables added to the existing shadow SQLite ledger; existing provider tables are unchanged.
- Scheduling: existing market-refresh cadence reused; no scheduler added.
- Predictions/public/model behavior: unchanged.
"""
    (output / "concise_provider_replacement_trial.md").write_text(concise)
    hash_path = output / "reproducibility_hashes.sha256"
    files = sorted(path for path in output.iterdir() if path.is_file() and path != hash_path)
    raw = ROOT / source["raw_response_path"]
    lines = [f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(ROOT)}\n" for path in files]
    lines.append(f"{hashlib.sha256(raw.read_bytes()).hexdigest()}  {raw.relative_to(ROOT)}\n")
    hash_path.write_text("".join(lines))


def run(game_date: str, output: Path, ledger_path: Path, raw_in: Path | None, captured_at_utc: str | None,
        odds_api_run_status: int = 0, same_refresh_max_age_minutes: int = 90) -> dict[str, Any]:
    if raw_in and not captured_at_utc:
        raise ValueError("--captured-at-utc is required with --raw-in")
    events, source = load_probe(raw_in, game_date, captured_at_utc) if raw_in else fetch_current(game_date)  # type: ignore[arg-type]
    schedule = []
    schedule_dates = {game_date} | {
        eastern_date((event.get("status") or {})["startsAt"]) for event in events
        if (event.get("status") or {}).get("startsAt")
        and eastern_date((event.get("status") or {})["startsAt"]) >= game_date
    }
    for schedule_date in sorted(schedule_dates):
        schedule_payload, observed, schedule_sha = fetch_hydrated_schedule(schedule_date)
        schedule.extend(normalize_schedule(schedule_payload, observed, schedule_sha))
    rows, audit = parse_provider_events(events=events, schedule=schedule, game_date=game_date,
        fetched_at_utc=source["fetch_timestamp_utc"], run_tag=source["run_tag"],
        raw_source_path=source["raw_response_path"], raw_source_sha256=source["raw_response_sha256"])
    conn = connect_ledger(ledger_path)
    discovery_actions = []
    for item in audit:
        if not item.get("game_pk") or item["event_classification"] not in {"CURRENT_SLATE", "FUTURE_SLATE_PREGAME"}:
            continue
        discovery_actions.append(append_event_discovery(conn, {
            "provider": PROVIDER, "provider_event_id": item["provider_event_id"],
            "game_date": item["scheduled_start_eastern_date"], "game_id": int(item["game_pk"]),
            "captured_at_utc": source["fetch_timestamp_utc"], "scheduled_start_utc": item["scheduled_start_utc"],
            "event_classification": item["event_classification"], "raw_source_path": source["raw_response_path"],
            "raw_source_sha256": source["raw_response_sha256"], "main_market_prices_present": item["admitted_market_rows"] > 0,
            "bookmaker_scope": "SPORTSGAMEODDS_PROVIDER_WIDE",
            "matchup": f"{item['away_team']} @ {item['home_team']}",
        }))
    rows = mark_first_observed_prices(conn, rows)
    actions = append_provider_rows_batch(conn, rows)
    append_total_rows_batch(conn, rows)
    current_rows = [row for row in rows if row["game_date"] == game_date]
    odds = odds_api_rows(conn, game_date, source["fetch_timestamp_utc"], same_refresh_max_age_minutes,
                         odds_api_run_status == 0)
    coverage = bookmaker_coverage(current_rows, events); priority = priority_coverage(coverage, odds)
    overlap = compare_provider_rows(current_rows, odds)
    overlap.extend({"classification": "IDENTITY_FAILURE", "provider_event_id": row["provider_event_id"],
                    "game_id": row.get("game_pk"), "identity_status": row["certification_status"]}
                   for row in audit if row["certification_status"] in {"AMBIGUOUS", "GAME_NOT_FOUND", "TIMING_UNRESOLVED"})
    if odds_api_run_status != 0:
        overlap.append({"classification": "SOURCE_FAILURE", "provider": "THE_ODDS_API",
                        "source_exit_status": odds_api_run_status})
    consensus = (consensus_metrics(current_rows, PROVIDER, "SPORTSGAMEODDS_ALL_BOOKS") +
                 consensus_metrics(odds, "THE_ODDS_API", "THE_ODDS_API_ALL_BOOKS") +
                 common_book_consensus(current_rows, odds))
    attachment_summary = attach_model_context(conn, game_date, current_rows, consensus, source["fetch_timestamp_utc"])
    freshness = freshness_metrics(current_rows, PROVIDER) + freshness_metrics(odds, "THE_ODDS_API")
    current_schedule = [game for game in schedule if eastern_date(game["scheduled_start_utc"]) == game_date]
    for payload in reliability_payloads(source, current_schedule, current_rows, audit, odds, odds_api_run_status):
        payload["ledger_action"] = append_reliability(conn, payload)
    reliability = reliability_rows(conn)
    write_package(output, source=source, coverage=coverage, overlap=overlap, consensus=consensus,
                  freshness=freshness, reliability=reliability, priority=priority, rows=rows, odds=odds,
                  schedule=schedule, audit=audit)
    summary = {"decision": DECISION, "run_tag": source["run_tag"], "captured_at_utc": source["fetch_timestamp_utc"],
               "official_games": len(schedule), "provider_events": len(events),
               "games_mapped": len({row["game_id"] for row in rows}), "accessible_bookmakers": len(coverage),
               "paired_market_rows": len(rows), "ledger_appended": actions.count("APPENDED_NEW"),
               "future_market_rows": len(rows) - len(current_rows), "event_discoveries": len(discovery_actions),
               "overlap_rows": len(overlap), "package": str(output), "outcomes_accessed": 0,
               **attachment_summary}
    print(json.dumps(summary, indent=2, sort_keys=True)); return summary


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--date", required=True)
    parser.add_argument("--output-dir", type=Path); parser.add_argument("--ledger-path", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--raw-in", type=Path); parser.add_argument("--captured-at-utc")
    parser.add_argument("--the-odds-api-run-status", type=int, default=0)
    parser.add_argument("--same-refresh-max-age-minutes", type=int, default=90)
    args = parser.parse_args(); output = args.output_dir or DEFAULT_PACKAGE / args.date
    run(args.date, output.resolve(), args.ledger_path.resolve(), args.raw_in, args.captured_at_utc,
        args.the_odds_api_run_status, args.same_refresh_max_age_minutes)


if __name__ == "__main__":
    main()
