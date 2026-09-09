#!/usr/bin/env python3
"""Freeze the pre-outcome live-timing amendment without calling the Odds API."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import plistlib
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.mlb.scripts import run_mlb_market_strong_agreement_separation_prospective_v1 as v1


ROOT = v1.ROOT
OUT = ROOT / "artifacts/analysis/model_development/mlb_market_strong_agreement_separation_live_timing_v3/2026-09-09"
V1_FREEZE = v1.OUT / "pre_outcome_freeze.json"
V2_FREEZE = ROOT / ("artifacts/analysis/model_development/"
    "mlb_market_strong_agreement_separation_feasibility_v2/2026-09-09/pre_outcome_freeze_v2.json")
V2_SCHEDULE = V2_FREEZE.parent / "remaining_regular_season_schedule.csv"
LEDGER = v1.LEDGER
INSTALLED_PLIST = Path("/Users/jerrystrain/Library/LaunchAgents/com.proppadia.mlb.refresh.daily.plist")
INSTALLED_WRAPPER = Path("/Users/jerrystrain/bin/proppadia_mlb_refresh_daily.sh")
HOOK = ROOT / "bin/mlb_full_game_totals_daily_hook.sh"
CLIENT = ROOT / "backend/mlb/scripts/capture_mlb_pinnacle_main_markets_v1.py"
PARSER = ROOT / "backend/mlb/markets/pinnacle_main_market_capture_v1.py"
SAMPLE_MANIFEST = ROOT / ("backend/mlb/exports/odds_history/2026-09-09/"
    "odds_mlb_pinnacle_main_markets__local_daily_20260909T123000Z.manifest.json")
STUDY_ID = "MLB_MARKET_STRONG_AGREEMENT_SEPARATION_PROSPECTIVE_V3"
FROZEN_BOOKS = tuple(v1.BOOKS)
REMAINING_DATES = 18
IMMEDIATE_MAX_LAG_SECONDS = 300
ODDS_DOC = "https://the-odds-api.com/liveapi/guides/v4/"

PREDICTION_WRITE_EVIDENCE = {
    "source": "read-only query of mlb.public_game_moneyline_predictions",
    "query_observed_utc_date": "2026-09-09",
    "game_date": "2026-09-09",
    "prediction_timestamp_utc": "2026-09-09T12:32:39.520656Z",
    "first_created_at_utc": "2026-09-09T12:32:42.569548Z",
    "last_created_at_utc": "2026-09-09T12:32:42.569548Z",
    "rows": 15,
    "credential_or_connection_string_preserved": False,
}


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def line_number(text: str, needle: str) -> int:
    matches = [number for number, line in enumerate(text.splitlines(), 1) if needle in line]
    if len(matches) != 1:
        raise RuntimeError(f"Expected one workflow line for {needle!r}; found {len(matches)}")
    return matches[0]


def workflow_evidence() -> dict[str, Any]:
    wrapper = INSTALLED_WRAPPER.read_text()
    hook = HOOK.read_text()
    client = CLIENT.read_text()
    parser = PARSER.read_text()
    plist = plistlib.loads(INSTALLED_PLIST.read_bytes())
    intervals = plist["StartCalendarInterval"]
    manifest = json.loads(SAMPLE_MANIFEST.read_text())
    raw_path = ROOT / manifest["raw_response_path"]
    events = json.loads(raw_path.read_text())
    updates = []
    pregame = []
    for event in events:
        for book in event.get("bookmakers", []):
            if book.get("key") != "pinnacle":
                continue
            for market in book.get("markets", []):
                if market.get("key") == "h2h":
                    updates.append(market.get("last_update"))
                    pregame.append(market.get("last_update") < event.get("commence_time"))
    prediction_commit = datetime.fromisoformat(
        PREDICTION_WRITE_EVIDENCE["last_created_at_utc"].replace("Z", "+00:00"))
    request_start = datetime.fromisoformat(manifest["requested_at_utc"].replace("Z", "+00:00"))
    lag = (request_start - prediction_commit).total_seconds()
    lifecycle_line = line_number(wrapper, 'bin/mlb_public_game_moneyline_daily_hook.sh "$MLB_DATE_ET"')
    market_hook_line = line_number(wrapper, 'bin/mlb_full_game_totals_daily_hook.sh "$MLB_DATE_ET" "$MLB_RUN_TAG"')
    pinnacle_line = line_number(hook, ".scripts.capture_mlb_pinnacle_main_markets_v1")
    return {
        "launchagent": {"path": str(INSTALLED_PLIST), "sha256": sha(INSTALLED_PLIST),
            "calendar_intervals_local_time": intervals,
            "has_0530_local_interval": {"Hour": 5, "Minute": 30} in intervals},
        "installed_wrapper": {"path": str(INSTALLED_WRAPPER), "sha256": sha(INSTALLED_WRAPPER),
            "moneyline_lifecycle_line": lifecycle_line, "main_market_hook_line": market_hook_line,
            "moneyline_precedes_market_capture": lifecycle_line < market_hook_line,
            "intervening_workflow_stages": ["roster refresh", "stat-derived refresh", "predictions-wide",
                "Hits 0.5 research scoring", "slate-output generation", "odds snapshot preservation"]},
        "market_hook": {"path": str(HOOK.relative_to(ROOT)), "sha256": sha(HOOK),
            "pinnacle_capture_line": pinnacle_line},
        "current_client": {"path": str(CLIENT.relative_to(ROOT)), "sha256": sha(CLIENT),
            "parser_path": str(PARSER.relative_to(ROOT)), "parser_sha256": sha(PARSER),
            "endpoint": "GET /v4/sports/baseball_mlb/odds", "uses_bookmakers_pinnacle": '"bookmakers": BOOKMAKER_KEY' in client,
            "uses_region_parameter": '"regions"' in client[client.index("def fetch"):client.index("def _to_total")],
            "markets": ["h2h", "totals", "spreads"], "raw_response_preserved_before_status_check":
                client.index('with raw_path.open("xb")') < client.index("response.raise_for_status()"),
            "downstream_parser_retains_only_pinnacle_rows": 'if book.get("key") != BOOKMAKER_KEY:' in parser,
            "prediction_attachment_occurs_after_immutable_prediction_fetch": "fetch_prediction_rows(game_date)" in client},
        "sample_2026_09_09": {**PREDICTION_WRITE_EVIDENCE,
            "request_manifest_path": str(SAMPLE_MANIFEST.relative_to(ROOT)),
            "request_manifest_sha256": sha(SAMPLE_MANIFEST),
            "request_started_at_utc": manifest["requested_at_utc"],
            "response_received_at_utc": manifest["fetch_timestamp_utc"],
            "request_parameters_without_secret": manifest["request_parameters_without_secret"],
            "x_requests_last": int(manifest["request_cost_headers"]["x-requests-last"]),
            "prediction_commit_to_request_lag_seconds": lag,
            "meets_post_commit_order": lag >= 0,
            "meets_immediate_max_lag": lag <= IMMEDIATE_MAX_LAG_SECONDS,
            "pinnacle_h2h_rows": len(updates), "earliest_bookmaker_market_update_utc": min(updates),
            "latest_bookmaker_market_update_utc": max(updates), "all_h2h_updates_pregame": all(pregame),
            "all_h2h_updates_not_after_response": all(
                value <= manifest["fetch_timestamp_utc"] for value in updates)},
        "bookmaker_group_rule": {"source": ODDS_DOC, "accessed_utc_date": "2026-09-09",
            "rule": "up to ten explicit bookmakers count as one region-equivalent bookmaker group",
            "frozen_bookmaker_count": len(FROZEN_BOOKS), "remains_one_group": len(FROZEN_BOOKS) <= 10},
        "odds_api_requests_by_this_review": 0,
    }


def acquisition_options() -> list[dict[str, Any]]:
    return [
        {"option": "EXPAND_AND_MOVE_EXISTING_LIVE_REQUEST", "dates": REMAINING_DATES,
         "markets": "h2h,totals,spreads", "credits_per_request": 3,
         "gross_credits_for_designated_0530_requests": 54, "incremental_study_credits_no_failures": 0,
         "incremental_if_historical_recovery_every_date": 180,
         "timing": "move existing request immediately after durable prediction commit; no intervening workflow stage",
         "status": "RECOMMENDED_PENDING_SEPARATE_SCHEDULER_AUTHORIZATION"},
        {"option": "ADD_SEPARATE_LIVE_TEN_BOOK_H2H_REQUEST", "dates": REMAINING_DATES,
         "markets": "h2h", "credits_per_request": 1,
         "gross_credits_for_designated_0530_requests": 18, "incremental_study_credits_no_failures": 18,
         "incremental_if_historical_recovery_every_date": 198,
         "timing": "new request immediately after durable prediction commit; existing three-market request remains later",
         "status": "VALID_LOW_RISK_ALTERNATIVE"},
        {"option": "HISTORICAL_SNAPSHOT_FOR_EVERY_DATE", "dates": REMAINING_DATES,
         "markets": "h2h", "credits_per_request": 10,
         "gross_credits_for_designated_0530_requests": 180, "incremental_study_credits_no_failures": 180,
         "incremental_if_historical_recovery_every_date": 180,
         "timing": "retrieve nearest historical snapshot at or before the frozen target after the live window",
         "status": "UNNECESSARY_AS_PRIMARY; FALLBACK_ONLY"},
    ]


def freeze_payload(frozen_at: str) -> dict[str, Any]:
    return {
        "study_id": STUDY_ID, "amendment_version": 3, "frozen_at_utc": frozen_at,
        "pre_outcome": True, "prospective_rows_at_amendment": 0,
        "preserved_prior_freezes": [
            {"version": 1, "path": str(V1_FREEZE.relative_to(ROOT)), "sha256": sha(V1_FREEZE)},
            {"version": 2, "path": str(V2_FREEZE.relative_to(ROOT)), "sha256": sha(V2_FREEZE)}],
        "superseded_timing_term_only": "historical snapshot requested at immutable prediction timestamp",
        "unchanged_contract": ("model, model threshold, market-strength threshold, agreement indicator, bookmaker set, "
            "risk-set admission, exclusions, grading, outcomes, uncertainty, and decision layers"),
        "designated_sequence": [
            "durably commit and verify all immutable market-independent prediction rows",
            "immediately initiate the first current sport-odds request for the frozen ten-book group",
            "preserve the complete raw response, redacted request parameters, capture timestamps, and quota headers",
            "construct market-strength and agreement states outside prediction generation",
            "grade independently after official outcomes"],
        "prediction_barrier": {
            "authority": "max(created_at) for the date's immutable DESIGNATED_DAILY_PUBLIC_SNAPSHOT rows",
            "all_expected_rows_must_be_committed_or_the_date_is_missing_model": True,
            "market_data_available_to_prediction_generation": False},
        "designated_live_capture": {
            "endpoint": "GET /v4/sports/baseball_mlb/odds", "bookmakers": list(FROZEN_BOOKS),
            "regions_parameter": "OMITTED", "bookmaker_groups": 1,
            "request_markets": ["h2h", "totals", "spreads"], "study_market_consumed": "h2h",
            "request_must_begin_after_prediction_barrier": True,
            "maximum_prediction_commit_to_first_request_seconds": IMMEDIATE_MAX_LAG_SECONDS,
            "designated_price_timestamp": "first qualifying live request's requested_at_utc",
            "capture_timestamp": "response_received_at_utc", "first_attempt_controls_no_price_based_retry": True,
            "bookmaker_and_market_last_update_preserved": True,
            "last_update_must_be_before_game_start": True,
            "last_update_must_not_exceed_response_received_at": True,
            "complete_raw_response_preserved_before_parsing": True},
        "failure_recovery": {
            "preserve_every_attempt_and_quota_headers": True,
            "immediate_live_retry": ("only when the first attempt is demonstrably uncharged and retry remains within "
                "the five-minute timing window; an unknown charge state is not retryable"),
            "charged_failure_or_missed_window": "do not substitute a later live price; use fixed-time historical recovery or classify missing",
            "historical_target_if_attempt_exists": "first live attempt requested_at_utc",
            "historical_target_if_no_attempt_exists": "prediction barrier plus five minutes",
            "historical_acceptance": ("nearest returned snapshot at or before the fixed target; preserve returned and bookmaker "
                "timestamps; require demonstrably pregame prices; label HISTORICAL_RECOVERY"),
            "bookmaker_absence_or_one_sided_market_in_successful_live_response": "classify that cell; do not backfill it",
            "outcomes_or_prices_may_not_control_recovery": True,
            "prospective_status": ("preserved when the target and automatic request-level recovery rule were frozen before outcomes; "
                "report recovery rows separately as an operational sensitivity")},
        "acquisition_budget_comparison": acquisition_options(),
        "recommended_primary": "EXPAND_AND_MOVE_EXISTING_LIVE_REQUEST",
        "activation_status": "FROZEN_BUT_NOT_SCHEDULED; SEPARATE_AUTHORIZATION_REQUIRED",
        "odds_api_called_by_review": False,
        "constraints": {"scheduler_changed": False, "model_changed": False, "threshold_changed": False,
            "prediction_generation_changed": False, "wagering": False, "production_rule": False,
            "promotion_authorized": False},
    }


def render_report(evidence: dict[str, Any], options: list[dict[str, Any]]) -> str:
    sample = evidence["sample_2026_09_09"]
    rows = "\n".join(
        f"| {x['option']} | {x['incremental_study_credits_no_failures']} | "
        f"{x['gross_credits_for_designated_0530_requests']} | {x['incremental_if_historical_recovery_every_date']} |"
        for x in options)
    return f"""# MLB agreement-separation live-timing review v3

## Finding

The 10-credit historical endpoint is not justified as the routine acquisition path for this prospective study. The current sport-odds endpoint can return the frozen ten books as one bookmaker group, and h2h costs one credit per group. The least expensive valid design is to expand and move the existing three-market Pinnacle request: its gross cost remains three credits per date and its incremental study cost is zero.

The installed LaunchAgent includes 05:30 local time. The wrapper calls the immutable moneyline lifecycle before the full-game market hook, but roster/stat refresh, predictions-wide, Hits 0.5 research scoring, slate generation, and artifact preservation intervene. On 2026-09-09 the model timestamp was `{sample['prediction_timestamp_utc']}`, all 15 rows committed at `{sample['last_created_at_utc']}`, and the current Pinnacle request began at `{sample['request_started_at_utc']}`—a {sample['prediction_commit_to_request_lag_seconds']:.3f}-second delay. The order is safe, but the delay is not “immediate” under the new five-minute ceiling.

The existing call is `GET /v4/sports/baseball_mlb/odds` with `bookmakers=pinnacle`, no `regions`, and `markets=h2h,totals,spreads`. Its recorded `x-requests-last` is {sample['x_requests_last']}. The complete raw response is written before HTTP status handling. The parser deliberately retains only Pinnacle rows for its existing downstream tables, so adding nine books to the raw request does not make those books model inputs or change existing Pinnacle attachments. The sample preserved {sample['pinnacle_h2h_rows']} Pinnacle h2h updates; all were pregame and no later than response receipt.

## Acquisition comparison through September 27

| Option | Incremental credits, no failures | Gross designated-request credits | Incremental credits if every date needs historical recovery |
|---|---:|---:|---:|
{rows}

The 54-credit gross figure for the expanded option is the already-existing 18 × 3-credit request, not new study usage. A separate one-market live request costs 18 additional credits. Historical acquisition on every date costs 180 additional credits. If separate live attempts were charged and every date also required fallback, the combined incremental ceiling would be 198.

## Timing amendment and recovery

Version 3 replaces only the designated price timing term. The primary price is now the first qualifying live capture initiated after durable prediction commit and within five minutes, with no intervening workflow stage. The ten-book raw payload cannot enter prediction generation because the database commit is the barrier and parsing/state construction happens afterward.

Every attempt must preserve the complete body, redacted parameters, request/receipt times, and quota headers. Retry live only after a demonstrably uncharged failure and only inside the five-minute window. A charged or uncertain failure cannot be replaced by a later live price. It may be recovered with the historical endpoint at the already-fixed first-attempt timestamp—or at commit plus five minutes if the request never started. Recovery is automatic for request-level failure, never driven by prices or outcomes, and is separately labeled. Individual absent or one-sided books in a successful response are classified rather than backfilled.

This rule preserves prospective status because prediction, target time, and recovery rule are frozen before outcomes. Historical recovery is operationally less exact and must remain a separately reported sensitivity. No API call or scheduler change was made.
"""


def write_manifest(output: Path) -> None:
    files = sorted(path for path in output.iterdir() if path.is_file() and path.name != "sha256_manifest.txt")
    (output / "sha256_manifest.txt").write_text("".join(f"{sha(path)}  {path.name}\n" for path in files))


def run(output: Path) -> dict[str, Any]:
    output.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(LEDGER) as conn:
        counts = {table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                  for table in ("risk_set", "bookmaker_prices", "outcomes")}
    if any(counts.values()):
        raise RuntimeError("Live-timing amendment is permitted only before the first prospective row")
    for source, name in ((V1_FREEZE, "original_v1_freeze.json"), (V2_FREEZE, "original_v2_freeze.json")):
        target = output / name
        if target.exists() and target.read_bytes() != source.read_bytes():
            raise RuntimeError(f"Preserved {name} changed")
        if not target.exists():
            target.write_bytes(source.read_bytes())
    freeze_path = output / "pre_outcome_freeze_v3.json"
    if freeze_path.exists():
        freeze = json.loads(freeze_path.read_text())
        if freeze != freeze_payload(freeze["frozen_at_utc"]):
            raise RuntimeError("V3 timing freeze changed")
    else:
        freeze = freeze_payload(utc_now())
        write_json(freeze_path, freeze)
    with sqlite3.connect(LEDGER) as conn:
        row = conn.execute("SELECT freeze_sha256 FROM study_metadata WHERE study_id=?", (STUDY_ID,)).fetchone()
        if row and row[0] != sha(freeze_path):
            raise RuntimeError("V3 ledger binding changed")
        conn.execute("INSERT OR IGNORE INTO study_metadata VALUES (?,?,?,?,?)",
            (STUDY_ID, sha(freeze_path), "2026-09-10", "2026-09-27", freeze["frozen_at_utc"]))
        conn.commit()
    evidence = workflow_evidence()
    options = acquisition_options()
    write_json(output / "workflow_evidence.json", evidence)
    write_json(output / "prediction_write_evidence.json", PREDICTION_WRITE_EVIDENCE)
    write_json(output / "acquisition_options.json", options)
    with (output / "acquisition_options.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=options[0].keys(), lineterminator="\n")
        writer.writeheader(); writer.writerows(options)
    (output / "live_timing_review.md").write_text(render_report(evidence, options))
    write_manifest(output)
    return {"freeze": freeze, "evidence": evidence, "options": options, "prospective_counts": counts,
            "odds_api_requests": 0, "scheduler_changed": False}


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--output", type=Path, default=OUT)
    args = parser.parse_args()
    print(json.dumps(run(args.output), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
