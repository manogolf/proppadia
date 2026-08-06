"""Capture SportsGameOdds Bookmaker.eu as a supplemental MLB main-market source."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import signal
from contextlib import contextmanager
from datetime import datetime, time, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests

from backend.mlb.markets.bookmaker_eu_supplemental_v1 import (
    BOOKMAKER_ID, BOOKMAKER_NAME, CONSENSUS_POLICY, EXPERIMENT, LEDGER_BOOKMAKER_KEY,
    MARKETS, PROVIDER, RUN_LINE_MODEL_STATUS, append_attachment, append_consensus,
    append_market, american_implied, build_consensus, connect_ledger, ledger_counts,
    market_rows, no_vig, parse_events, sha256_json, utc,
)
from backend.mlb.markets.full_game_total_capture_v1 import (
    append_market as append_total_market,
    attach_all_markets,
    connect_ledger as connect_total_ledger,
    market_rows as total_market_rows,
)
from backend.mlb.public_game_predictions.durable_store_v1 import fetch_prediction_rows as fetch_moneyline_predictions
from backend.mlb.scripts.run_mlb_totals_prospective_shadow_v1 import probability_fields
from backend.mlb.totals_predictions.live_context_bridge_v1 import fetch_hydrated_schedule, normalize_schedule
from backend.mlb.totals_predictions.prospective_shadow_v1 import (
    connect_ledger as connect_prediction_ledger,
    rows_for_date as totals_predictions_for_date,
)

ROOT = Path(__file__).resolve().parents[3]
API_URL = "https://api.sportsgameodds.com/v2/events"
DEFAULT_LEDGER = ROOT / "backend/mlb/exports/market_history/full_game_totals/full_game_totals_v1.sqlite3"
RAW_ROOT = ROOT / "backend/mlb/exports/market_history/bookmaker_eu/raw"
TOTALS_PREDICTION_LEDGER = ROOT / "backend/mlb/exports/model_v2/totals_shadow_v1/totals_shadow_v1.sqlite3"
MODEL_ALPHA = 0.12944479977012996
MONEYLINE_READ_TIMEOUT_SECONDS = 20

TEST_NAMES = [
    "authentication_without_secret_exposure", "bookmaker_filtering", "mlb_game_identity",
    "doubleheader_identity", "moneyline_parsing", "total_parsing", "run_line_parsing",
    "american_odds_normalization", "provider_update_timestamp", "fetch_timestamp",
    "post_start_rejection", "raw_response_hashing", "immutable_ledger_append",
    "duplicate_protection", "the_odds_api_rows_unchanged", "bookmaker_independent_identity",
    "totals_consensus_inclusion", "moneyline_consensus_inclusion", "differing_total_line_handling",
    "differing_run_line_handling", "totals_shadow_attachment", "moneyline_shadow_attachment",
    "run_line_preservation_without_model", "provider_failure_isolation", "no_ev_or_wager_output",
]


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    names = fields or sorted({key for row in rows for key in row})
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=names, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _raw_hash_snapshot(conn: Any) -> dict[str, str]:
    return dict(conn.execute(
        "SELECT canonical_market_identity,market_payload_sha256 FROM full_game_total_market_snapshots WHERE bookmaker_key<>?",
        (LEDGER_BOOKMAKER_KEY,),
    ).fetchall())


def _day_bounds(game_date: str) -> tuple[str, str]:
    pacific = ZoneInfo("America/Los_Angeles")
    requested = datetime.fromisoformat(game_date).date()
    start = datetime.combine(requested, time.min, tzinfo=pacific).astimezone(timezone.utc)
    end = datetime.combine(requested, time.max, tzinfo=pacific).astimezone(timezone.utc)
    return start.isoformat().replace("+00:00", "Z"), end.isoformat().replace("+00:00", "Z")


def fetch_current(game_date: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    key = os.getenv("SPORTSGAMEODDSAPI", "").strip()
    if not key:
        raise RuntimeError("SPORTSGAMEODDS_AUTH_MISSING:SPORTSGAMEODDSAPI")
    _, day_end = _day_bounds(game_date)
    request_started = now_utc()
    params = {
        "leagueID": "MLB",
        "oddsAvailable": "true",
        "bookmakerID": BOOKMAKER_ID,
        "oddID": ",".join(odd_id for market in MARKETS.values() for odd_id in market.values()),
        "startsAfter": request_started,
        "startsBefore": day_end,
        "limit": "20",
    }
    response = requests.get(API_URL, params=params, headers={"x-api-key": key}, timeout=45)
    fetched = now_utc()
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict) or payload.get("success") is not True or not isinstance(payload.get("data"), list):
        raise RuntimeError("SPORTSGAMEODDS_UNEXPECTED_RESPONSE")
    run_tag = "bookmaker_eu_" + utc(fetched).strftime("%Y%m%dT%H%M%SZ")
    raw_dir = RAW_ROOT / game_date / run_tag
    raw_dir.mkdir(parents=True, exist_ok=False)
    raw_path = raw_dir / "sportsgameodds_response.json"
    raw_path.write_bytes(response.content)
    raw_sha = hashlib.sha256(raw_path.read_bytes()).hexdigest()
    raw_display_path = raw_path.relative_to(ROOT) if raw_path.is_relative_to(ROOT) else raw_path
    manifest = {
        "experiment": EXPERIMENT,
        "provider": PROVIDER,
        "bookmaker_id": BOOKMAKER_ID,
        "game_date": game_date,
        "fetch_timestamp_utc": fetched,
        "run_tag": run_tag,
        "request_class": "CURRENT_MLB_PREGAME_BOOKMAKER_EU_CANONICAL_MAIN_MARKETS",
        "request_parameters_without_secret": params,
        "authentication_transport": "x-api-key header; value not retained",
        "http_status": response.status_code,
        "provider_event_count": len(payload["data"]),
        "provider_notice": payload.get("notice"),
        "raw_response_path": str(raw_display_path),
        "raw_response_sha256": raw_sha,
        "request_count": 1,
        "provider_counted_entities": None,
        "remaining_quota": None,
    }
    manifest_path = raw_dir / "run_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    manifest_display_path = manifest_path.relative_to(ROOT) if manifest_path.is_relative_to(ROOT) else manifest_path
    return payload["data"], {**manifest, "run_manifest_path": str(manifest_display_path)}


def load_immutable_capture(manifest_path: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Replay one already acquired response without making another provider request."""
    manifest_path = manifest_path.resolve()
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("experiment") != EXPERIMENT or manifest.get("provider") != PROVIDER:
        raise RuntimeError("INVALID_BOOKMAKER_EU_RUN_MANIFEST")
    raw_path = ROOT / manifest["raw_response_path"]
    digest = hashlib.sha256(raw_path.read_bytes()).hexdigest()
    if digest != manifest.get("raw_response_sha256"):
        raise RuntimeError("BOOKMAKER_EU_RAW_RESPONSE_HASH_MISMATCH")
    payload = json.loads(raw_path.read_text())
    if payload.get("success") is not True or not isinstance(payload.get("data"), list):
        raise RuntimeError("INVALID_BOOKMAKER_EU_RAW_RESPONSE")
    display = manifest_path.relative_to(ROOT) if manifest_path.is_relative_to(ROOT) else manifest_path
    return payload["data"], {**manifest, "run_manifest_path": str(display), "request_count": 0,
                             "replay_of_request_count": manifest.get("request_count", 1)}


def _to_total_row(row: dict[str, Any]) -> dict[str, Any]:
    converted = {
        **row,
        "market_type": "FULL_GAME_TOTAL",
        "total_line": float(row["total_line"]),
        "over_price": int(row["over_american_price"]),
        "under_price": int(row["under_american_price"]),
        "provider_market_timestamp_utc": row["provider_market_updated_at_utc"],
        "market_status": "TOTAL_MARKET_CERTIFIED_PAIRED",
    }
    converted["canonical_market_identity"] = row["canonical_market_identity"]
    return converted


def _generic_total_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        if row.get("market_type") != "FULL_GAME_TOTAL":
            continue
        out.append({
            **row,
            "provider": row.get("provider") or "THE_ODDS_API",
            "bookmaker_key": row["bookmaker_key"],
            "market_type": "FULL_GAME_TOTAL",
            "timing_status": row.get("timing_status") or "PREGAME_CERTIFIED",
            "no_vig_over_probability": no_vig(row.get("over_price"), row.get("under_price")),
        })
    return out


def _timing(prediction_time: str, captured_at: str) -> str:
    return "AT_OR_BEFORE_PREDICTION" if utc(captured_at) <= utc(prediction_time) else "POST_PREDICTION_MARKET_OBSERVATION"


@contextmanager
def _bounded_read(seconds: int):
    """Bound an optional read-only comparison so capture cannot hang the refresh."""
    if not hasattr(signal, "SIGALRM"):
        yield
        return
    previous = signal.getsignal(signal.SIGALRM)
    def expired(signum, frame):  # noqa: ARG001
        raise TimeoutError("BOUNDED_READ_TIMEOUT")
    signal.signal(signal.SIGALRM, expired)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


def totals_attachments(conn: Any, game_date: str, current_rows: list[dict[str, Any]], created_at: str) -> list[dict[str, Any]]:
    prediction_conn = connect_prediction_ledger(TOTALS_PREDICTION_LEDGER)
    predictions = totals_predictions_for_date(prediction_conn, game_date)
    totals = {int(row["game_id"]): row for row in current_rows if row["market_type"] == "FULL_GAME_TOTAL"}
    output = []
    for prediction in predictions:
        market = totals.get(int(prediction["game_pk"]))
        if not market:
            continue
        prediction_identity = (
            f"{prediction['game_date']}|{prediction['game_pk']}|{prediction['model_version']}|"
            f"{prediction['prediction_snapshot_class']}"
        )
        timing = _timing(prediction["prediction_timestamp_utc"], market["captured_at_utc"])
        probabilities = probability_fields(float(prediction["expected_total"]), MODEL_ALPHA, float(market["total_line"]))
        payload = {
            "prediction_identity": prediction_identity,
            "market_identity": market["canonical_market_identity"],
            "game_date": game_date,
            "game_pk": int(prediction["game_pk"]),
            "away_team": prediction["away_team"],
            "home_team": prediction["home_team"],
            "model_expected_total": float(prediction["expected_total"]),
            "bookmaker_eu_total": float(market["total_line"]),
            "model_minus_bookmaker_eu": float(prediction["expected_total"]) - float(market["total_line"]),
            "prediction_timestamp_utc": prediction["prediction_timestamp_utc"],
            "bookmaker_eu_market_timestamp_utc": market["provider_market_updated_at_utc"],
            "bookmaker_eu_fetch_timestamp_utc": market["captured_at_utc"],
            "timing_relationship": timing,
            **probabilities,
        }
        payload["ledger_action"] = append_attachment(
            conn, table="bookmaker_eu_totals_shadow_attachments",
            prediction_identity=prediction_identity, market_identity=market["canonical_market_identity"],
            payload=payload, created_at_utc=created_at,
        )
        output.append(payload)
    return output


def moneyline_attachments(
    conn: Any, game_date: str, current_rows: list[dict[str, Any]], consensus: list[dict[str, Any]], created_at: str,
) -> tuple[list[dict[str, Any]], str]:
    try:
        with _bounded_read(MONEYLINE_READ_TIMEOUT_SECONDS):
            predictions = fetch_moneyline_predictions(game_date)
    except Exception as exc:
        return [], f"MONEYLINE_PREDICTION_SOURCE_UNAVAILABLE:{type(exc).__name__}"
    markets = {int(row["game_id"]): row for row in current_rows if row["market_type"] == "MONEYLINE"}
    consensus_by_game = {
        int(row["game_id"]): row for row in consensus
        if row["market_type"] == "MONEYLINE" and row.get("consensus_status") == "MULTI_BOOK_CONSENSUS"
    }
    output = []
    for prediction in predictions:
        game_id = int(prediction.get("game_id", prediction.get("game_pk")))
        market = markets.get(game_id)
        if not market:
            continue
        model_home = float(prediction["home_win_probability"])
        book_home = float(market["no_vig_home_probability"])
        multi = consensus_by_game.get(game_id)
        consensus_home = multi.get("median_no_vig_home_probability") if multi else None
        prediction_identity = (
            f"{prediction['game_date']}|{game_id}|{prediction['winner_model_version']}|"
            f"{prediction['prediction_snapshot_class']}"
        )
        timing = _timing(prediction["prediction_timestamp_utc"], market["captured_at_utc"])
        payload = {
            "prediction_identity": prediction_identity,
            "market_identity": market["canonical_market_identity"],
            "game_date": game_date,
            "game_pk": game_id,
            "away_team": prediction["away_team"],
            "home_team": prediction["home_team"],
            "model_home_probability": model_home,
            "bookmaker_eu_no_vig_home_probability": book_home,
            "consensus_no_vig_home_probability": consensus_home,
            "model_minus_bookmaker_eu_probability": model_home - book_home,
            "model_minus_consensus_probability": model_home - float(consensus_home) if consensus_home is not None else None,
            "predicted_winner": prediction["predicted_winner"],
            "bookmaker_eu_favorite": market["home_team"] if book_home > .5 else market["away_team"],
            "consensus_favorite": (market["home_team"] if float(consensus_home) > .5 else market["away_team"]) if consensus_home is not None else "UNAVAILABLE",
            "prediction_timestamp_utc": prediction["prediction_timestamp_utc"],
            "bookmaker_eu_market_timestamp_utc": market["provider_market_updated_at_utc"],
            "bookmaker_eu_fetch_timestamp_utc": market["captured_at_utc"],
            "timing_relationship": timing,
        }
        payload["ledger_action"] = append_attachment(
            conn, table="bookmaker_eu_moneyline_shadow_attachments",
            prediction_identity=prediction_identity, market_identity=market["canonical_market_identity"],
            payload=payload, created_at_utc=created_at,
        )
        output.append(payload)
    return output, "AVAILABLE"


def _consensus_rows_for_current(
    conn: Any, total_conn: Any, game_date: str, current_rows: list[dict[str, Any]], captured_at: str,
) -> list[dict[str, Any]]:
    generic = market_rows(conn, game_date)
    generic += _generic_total_rows(total_market_rows(total_conn, game_date))
    output = []
    for game_id in sorted({int(row["game_id"]) for row in current_rows}):
        for market_type in ("MONEYLINE", "FULL_GAME_TOTAL", "RUN_LINE"):
            value = build_consensus(
                rows=generic, game_date=game_date, game_id=game_id,
                market_type=market_type, captured_at_utc=captured_at,
            )
            if value:
                value["ledger_action"] = append_consensus(conn, value)
                output.append(value)
    return output


def _latest_old_totals_consensus(total_conn: Any, game_date: str) -> dict[int, dict[str, Any]]:
    result = {}
    rows = total_conn.execute(
        "SELECT consensus_payload_json FROM totals_shadow_market_consensus WHERE json_extract(consensus_payload_json,'$.game_date')=? ORDER BY captured_at_utc",
        (game_date,),
    ).fetchall()
    for row in rows:
        payload = json.loads(row[0])
        result[int(payload["game_id"])] = payload
    return result


def write_evidence(
    *, output_dir: Path, summary: dict[str, Any], rows: list[dict[str, Any]], audit: list[dict[str, Any]],
    actions: list[dict[str, Any]], consensus: list[dict[str, Any]], total_attach: list[dict[str, Any]],
    money_attach: list[dict[str, Any]], old_consensus: dict[int, dict[str, Any]], validated_test_count: int,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    contract = {
        "experiment": EXPERIMENT,
        "provider": PROVIDER,
        "provider_endpoint": "/v2/events",
        "bookmaker_id": BOOKMAKER_ID,
        "bookmaker_display_name": BOOKMAKER_NAME,
        "credential_environment_variable": "SPORTSGAMEODDSAPI",
        "credential_persisted": False,
        "market_scope": ["FULL_GAME_MONEYLINE", "FULL_GAME_TOTAL", "FULL_GAME_RUN_LINE"],
        "canonical_odd_ids": MARKETS,
        "event_identity": "date + exact teams + scheduled start within 10 minutes + game number when required; one-to-one",
        "timing_contract": "fetch and bookmaker update must precede official first pitch",
        "canonical_market_identity": "provider + bookmaker + game_pk + market_type + line + fetch timestamp",
        "consensus_policy": CONSENSUS_POLICY,
        "provider_role": "SUPPLEMENTAL_ONLY",
        "the_odds_api_role": "UNCHANGED",
        "public_behavior": "UNCHANGED",
    }
    (output_dir / "bookmaker_eu_adapter_contract.json").write_text(json.dumps(contract, indent=2, sort_keys=True) + "\n")
    write_csv(output_dir / "bookmaker_eu_current_capture.csv", rows)
    write_csv(output_dir / "bookmaker_eu_identity_audit.csv", audit)
    write_csv(output_dir / "bookmaker_eu_main_market_ledger_audit.csv", actions)

    consensus_audit = []
    current_by_key = {(int(row["game_id"]), row["market_type"]): row for row in rows}
    for value in consensus:
        current = current_by_key.get((int(value["game_id"]), value["market_type"]), {})
        old = old_consensus.get(int(value["game_id"])) if value["market_type"] == "FULL_GAME_TOTAL" else None
        consensus_audit.append({
            **value,
            "bookmaker_eu_line": current.get("total_line", current.get("home_spread")),
            "bookmaker_eu_no_vig_probability": current.get("no_vig_over_probability", current.get("no_vig_home_probability")),
            "existing_consensus_line": old.get("median_total_line") if old else None,
            "existing_consensus_no_vig_probability": old.get("median_no_vig_over_probability_at_consensus_line") if old else None,
            "bookmaker_minus_existing_line": (float(current["total_line"]) - float(old["median_total_line"])) if old and current.get("total_line") is not None else None,
            "existing_comparison_status": (
                "COMPARABLE_SAME_TOTAL_LINE" if old and float(current["total_line"]) == float(old["median_total_line"])
                else "DIFFERENT_TOTAL_LINE_NOT_PROBABILITY_COMPARABLE" if old
                else "EXISTING_CONSENSUS_UNAVAILABLE"
            ),
        })
    write_csv(output_dir / "bookmaker_eu_consensus_integration.csv", consensus_audit)
    write_csv(output_dir / "bookmaker_eu_totals_shadow_attachment.csv", total_attach)
    write_csv(output_dir / "bookmaker_eu_moneyline_shadow_attachment.csv", money_attach)
    run_lines = [{**row, "model_comparison_status": RUN_LINE_MODEL_STATUS} for row in rows if row["market_type"] == "RUN_LINE"]
    write_csv(output_dir / "bookmaker_eu_run_line_capture.csv", run_lines)
    tests = [{
        "test": name,
        "status": "PASS" if validated_test_count == len(TEST_NAMES) else "NOT_RUN_IN_THIS_INVOCATION",
        "validation_source": "pytest backend/mlb/tests/test_bookmaker_eu_supplemental_market_adapter_v1.py",
    } for name in TEST_NAMES]
    write_csv(output_dir / "bookmaker_eu_adapter_test_results.csv", tests, ["test", "status", "validation_source"])

    workflow = f"""# Bookmaker.eu daily workflow integration

- Existing scheduler reused: `com.proppadia.mlb.refresh.daily`; no scheduler was added.
- Existing market stage: `bin/mlb_full_game_totals_daily_hook.sh`.
- The Odds API command and SportsGameOdds command run independently on every applicable existing refresh.
- SportsGameOdds request count per capture: 1.
- Provider failure behavior: visible warning and source-specific nonzero status; the other source still runs.
- Combined hook behavior: success when either source succeeds; nonzero only when both source captures fail.
- Source health: independent exit codes and log markers.
- Quota counters: request count and HTTP status logged; entity/remaining-quota values remain unavailable when response headers omit them.
- Public prediction feature flags, model authority, schedules, and deployment: unchanged.
- Raw response: `{summary['raw_response_path']}` (`{summary['raw_response_sha256']}`).
"""
    (output_dir / "bookmaker_eu_daily_workflow_integration.md").write_text(workflow)

    total_by_game = {int(row["game_pk"]): row for row in total_attach}
    money_by_game = {int(row["game_pk"]): row for row in money_attach}
    combined_total_by_game = {
        int(row["game_id"]): row for row in consensus
        if row["market_type"] == "FULL_GAME_TOTAL" and row.get("consensus_status") == "MULTI_BOOK_CONSENSUS"
    }
    row_by_market = {(int(row["game_id"]), row["market_type"]): row for row in rows}
    game_ids = sorted({int(row["game_id"]) for row in rows})
    report = [
        "# Current model / consensus / Bookmaker.eu report", "",
        "No value, edge, wager, ranking, or staking calculation is included.", "",
        "| Matchup | Model ML home | Bookmaker.eu no-vig home | Consensus ML home | Totals model | Bookmaker.eu total | Multi-book total | Bookmaker.eu run line | Consensus run line | Market update / fetch |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |",
    ]
    for game_id in game_ids:
        market = next(row for row in rows if int(row["game_id"]) == game_id)
        money = money_by_game.get(game_id)
        total = total_by_game.get(game_id)
        total_market = row_by_market.get((game_id, "FULL_GAME_TOTAL"))
        run_line = row_by_market.get((game_id, "RUN_LINE"))
        combined_total = combined_total_by_game.get(game_id)
        fmt = lambda value, places=3: "UNAVAILABLE" if value is None else f"{float(value):.{places}f}"
        report.append(
            f"| {market['away_team']} @ {market['home_team']} | {fmt(money.get('model_home_probability') if money else None)} | "
            f"{fmt(money.get('bookmaker_eu_no_vig_home_probability') if money else row_by_market.get((game_id,'MONEYLINE'),{}).get('no_vig_home_probability'))} | "
            f"{fmt(money.get('consensus_no_vig_home_probability') if money else None)} | "
            f"{fmt(total.get('model_expected_total') if total else None)} | {fmt(total_market.get('total_line') if total_market else None,1)} | "
            f"{fmt(combined_total.get('median_line') if combined_total else None,1)} | "
            f"{('away ' + str(run_line['away_spread']) + ' / home ' + str(run_line['home_spread'])) if run_line else 'UNAVAILABLE'} | "
            f"UNAVAILABLE | {market['provider_market_updated_at_utc']} / {market['captured_at_utc']} |"
        )
    (output_dir / "current_model_consensus_bookmaker_eu_report.md").write_text("\n".join(report) + "\n")

    decision = "BOOKMAKER_EU_SUPPLEMENTAL_CAPTURE_INTEGRATED" if rows else "BOOKMAKER_EU_SUPPLEMENTAL_CAPTURE_PARTIAL"
    concise = f"""# MLB Bookmaker.eu Supplemental Market Adapter v1

`{decision}`

- Current provider events: {summary['provider_events']}
- Certified games: {summary['current_games_captured']}
- Moneyline rows: {summary['moneyline_rows']}
- Total rows: {summary['total_rows']}
- Run-line rows: {summary['run_line_rows']}
- Pregame-certified market rows: {summary['pregame_certified_rows']}
- Post-start identity rejections: {summary['post_start_rejected_rows']}
- Totals shadow attachments: {len(total_attach)}
- Moneyline shadow attachments: {len(money_attach)} ({summary['moneyline_prediction_source_status']})
- Consensus records appended/idempotently preserved: {len(consensus)}
- Existing The Odds API payload hashes unchanged: {summary['the_odds_api_rows_unchanged']}
- SportsGameOdds HTTP status / request count: {summary['http_status']} / {summary['request_count']}
- Provider-counted entities / remaining quota: UNAVAILABLE_IN_EVENT_RESPONSE / UNAVAILABLE_IN_EVENT_RESPONSE
- Run-line model comparison: `{RUN_LINE_MODEL_STATUS}`
- Tests certified: {validated_test_count}/{len(TEST_NAMES)}
- Public/model/deployment behavior: unchanged
"""
    (output_dir / "concise_mlb_bookmaker_eu_supplemental_adapter_v1.md").write_text(concise)

    hash_path = output_dir / "reproducibility_hashes.sha256"
    files = sorted(path for path in output_dir.iterdir() if path.is_file() and path != hash_path)
    raw_path = ROOT / summary["raw_response_path"]
    run_manifest_path = ROOT / summary["run_manifest_path"]
    files.extend([raw_path, run_manifest_path])
    hash_path.write_text("".join(
        f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.relative_to(ROOT)}\n" for path in files
    ))


def run(
    game_date: str, output_dir: Path, ledger_path: Path, validated_test_count: int = 0,
    run_manifest_in: Path | None = None,
) -> dict[str, Any]:
    output_dir = output_dir.resolve()
    events, source = load_immutable_capture(run_manifest_in) if run_manifest_in else fetch_current(game_date)
    schedule_payload, schedule_observed, schedule_sha = fetch_hydrated_schedule(game_date)
    schedule = normalize_schedule(schedule_payload, schedule_observed, schedule_sha)
    rows, audit = parse_events(
        events=events, schedule=schedule, game_date=game_date,
        fetched_at_utc=source["fetch_timestamp_utc"], run_tag=source["run_tag"],
        raw_source_path=source["raw_response_path"], raw_source_sha256=source["raw_response_sha256"],
    )
    total_conn = connect_total_ledger(ledger_path)
    before_odds_hashes = _raw_hash_snapshot(total_conn)
    conn = connect_ledger(ledger_path)
    before = ledger_counts(conn)
    actions = [{**row, "ledger_action": append_market(conn, row)} for row in rows]
    total_rows = [_to_total_row(row) for row in rows if row["market_type"] == "FULL_GAME_TOTAL"]
    for row in total_rows:
        append_total_market(total_conn, row)
    # Preserve every Bookmaker total in the existing all-book attachment ledger.
    total_predictions = totals_predictions_for_date(connect_prediction_ledger(TOTALS_PREDICTION_LEDGER), game_date)
    for prediction in total_predictions:
        matching = [row for row in total_rows if int(row["game_id"]) == int(prediction["game_pk"])]
        if matching:
            attach_all_markets(total_conn, prediction, matching, source["fetch_timestamp_utc"])
    consensus = _consensus_rows_for_current(conn, total_conn, game_date, rows, source["fetch_timestamp_utc"])
    total_attach = totals_attachments(conn, game_date, rows, source["fetch_timestamp_utc"])
    money_attach, money_status = moneyline_attachments(conn, game_date, rows, consensus, source["fetch_timestamp_utc"])
    after_odds_hashes = _raw_hash_snapshot(total_conn)
    after = ledger_counts(conn)
    old_consensus = _latest_old_totals_consensus(total_conn, game_date)
    summary = {
        "experiment": EXPERIMENT,
        **source,
        "provider_events": len(events),
        "official_games": len(schedule),
        "current_games_captured": len({row["game_id"] for row in rows}),
        "moneyline_rows": sum(row["market_type"] == "MONEYLINE" for row in rows),
        "total_rows": sum(row["market_type"] == "FULL_GAME_TOTAL" for row in rows),
        "run_line_rows": sum(row["market_type"] == "RUN_LINE" for row in rows),
        "pregame_certified_rows": sum(row["timing_status"] == "PREGAME_CERTIFIED" for row in rows),
        "post_start_rejected_rows": sum(row["certification_status"] == "POST_START" for row in audit),
        "identity_rejected_rows": sum(row["certification_status"] in {"AMBIGUOUS", "GAME_NOT_FOUND", "TIMING_UNRESOLVED"} for row in audit),
        "consensus_records": len(consensus),
        "totals_shadow_attachments": len(total_attach),
        "moneyline_shadow_attachments": len(money_attach),
        "moneyline_prediction_source_status": money_status,
        "request_count": int(source.get("replay_of_request_count", source.get("request_count", 1))),
        "evidence_replay_request_count": int(source.get("request_count", 1)) if run_manifest_in else 0,
        "ledger_before": before,
        "ledger_after": after,
        "the_odds_api_rows_unchanged": before_odds_hashes == after_odds_hashes,
        "outcomes_accessed": 0,
    }
    write_evidence(
        output_dir=output_dir, summary=summary, rows=rows, audit=audit, actions=actions,
        consensus=consensus, total_attach=total_attach, money_attach=money_attach,
        old_consensus=old_consensus, validated_test_count=validated_test_count,
    )
    # The required package intentionally has only the 13 named outputs. Runtime
    # summary remains stdout; daily runs place their own evidence in run-tagged dirs.
    print(json.dumps(summary, indent=2, sort_keys=True))
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--ledger-path", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--validated-test-count", type=int, default=0)
    parser.add_argument("--run-manifest-in", type=Path)
    args = parser.parse_args()
    run(args.date, args.output_dir, args.ledger_path, args.validated_test_count, args.run_manifest_in)


if __name__ == "__main__":
    main()
