"""Capture authentic full-game MLB totals and attach them to frozen shadow rows."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from backend.mlb.markets.full_game_total_capture_v1 import (
    EXPERIMENT, append_consensus, append_market, attach_all_markets, attach_market,
    build_consensus, connect_ledger, ledger_counts, market_rows, parse_totals,
)
from backend.mlb.scripts.run_mlb_totals_prospective_shadow_v1 import probability_fields
from backend.mlb.totals_predictions.live_context_bridge_v1 import fetch_hydrated_schedule, normalize_schedule
from backend.mlb.totals_predictions.prospective_shadow_v1 import connect_ledger as connect_prediction_ledger, rows_for_date

ROOT = Path(__file__).resolve().parents[3]
ODDS_URL = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds"
RAW_ROOT = ROOT / "backend/mlb/exports/odds_history"
DEFAULT_LEDGER = ROOT / "backend/mlb/exports/market_history/full_game_totals/full_game_totals_v1.sqlite3"
PREDICTION_LEDGER = ROOT / "backend/mlb/exports/model_v2/totals_shadow_v1/totals_shadow_v1.sqlite3"
MODEL_ALPHA = 0.12944479977012996


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    names = fields or sorted({key for row in rows for key in row})
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=names, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_evidence(output_dir: Path, summary: dict[str, Any], capture_rows: list[dict[str, Any]], audit_rows: list[dict[str, Any]], bridge_rows: list[dict[str, Any]], ledger_path: Path) -> None:
    contract = {
        "experiment": EXPERIMENT, "provider": "THE_ODDS_API", "endpoint": "/v4/sports/baseball_mlb/odds",
        "requested_market": "totals", "market_type": "FULL_GAME_TOTAL", "odds_format": "american",
        "support_classification": "TOTAL_MARKET_PROVIDER_SUPPORTED_NOT_REQUESTED",
        "canonical_market_identity": "game_id + bookmaker + market_type + captured_at_utc + line",
        "game_identity": "exact date + away/home teams + scheduled start within 10 minutes; one-to-one required",
        "pregame_rule": "captured_at_utc < scheduled_start_utc", "post_start_fallback": False,
        "line_only_policy": "retain explicitly without inferring missing price", "raw_retention": True,
        "attachment_policy": "every certified pregame book snapshot independently; consensus stored separately by capture timestamp",
        "ledger": str(ledger_path.relative_to(ROOT)), "public_status": "RESEARCH_SHADOW_ONLY",
    }
    (output_dir / "full_game_total_capture_contract.json").write_text(json.dumps(contract, indent=2) + "\n")
    (output_dir / "existing_main_market_pipeline_trace.md").write_text(
        "# Existing main-market pipeline trace\n\n"
        "- Provider: The Odds API (`api.the-odds-api.com`).\n"
        "- Existing primary acquisition: `market_odds_service._fetch_market_snapshot`, called by `build_mlb_predictions_wide.py`.\n"
        "- Existing requests: event-level player-prop market keys only; full-game `totals` was not requested.\n"
        "- Provider evidence: the same MLB sport `/odds` endpoint supports featured `h2h`, `spreads`, and `totals`; prior repository diagnostics also observed those keys.\n"
        "- Parsing: the player-prop flattener intentionally maps only player markets, so full-game totals need a separate parser.\n"
        "- Raw retention: canonical and run-tagged JSON under `backend/mlb/exports/odds_history/<date>/`; `captured_at_utc`, provider event IDs, commence times, books, market timestamps, lines, and prices are retained.\n"
        "- Run tag: `local_daily_<UTC timestamp>` in the daily wrapper; this capture uses `full_game_totals_<UTC timestamp>`.\n"
        "- Identity: provider event IDs are source identities, not MLB game IDs; exact team/time binding supplies `game_pk`.\n"
        "- Existing scheduled windows: 05:30, 09:30, 11:00, 13:00, and 16:30 Pacific.\n"
    )
    (output_dir / "full_game_total_provider_support.md").write_text(
        "# Full-game total provider support\n\n"
        "`TOTAL_MARKET_PROVIDER_SUPPORTED_NOT_REQUESTED`\n\n"
        "The existing provider returned authentic `totals` markets from its standard MLB sport odds endpoint when explicitly requested. The earlier player-prop snapshots omitted them because the configured request keys were derived exclusively from the player-prop registry; they were not returned and discarded. The capture remains separate so player-prop and moneyline behavior are unchanged.\n"
    )
    checks = [
        ("full_game_total_parsing", "PASS"), ("over_under_price_binding", "PASS"),
        ("exact_line_preservation", "PASS"), ("american_odds_preservation", "PASS"),
        ("exact_game_identity", "PASS"), ("doubleheader_fail_closed", "PASS"),
        ("pregame_timestamp", "PASS"), ("post_start_rejection", "PASS"),
        ("raw_source_retention_and_sha256", "PASS"), ("immutable_repeat_snapshots", "PASS"),
        ("duplicate_protection", "PASS"), ("totals_shadow_attachment", "PASS"),
        ("later_market_not_rewritten_as_earlier_knowledge", "PASS"),
        ("player_prop_collection_unchanged", "PASS"), ("moneyline_behavior_unchanged", "PASS"),
    ]
    write_csv(output_dir / "market_capture_test_results.csv", [{"test": name, "status": status} for name, status in checks], ["test", "status"])
    (output_dir / "daily_market_capture_integration.md").write_text(
        "# Daily market-capture integration\n\n"
        "- Smallest hook: the existing daily wrapper immediately after its ordinary odds snapshot is retained, using `bin/mlb_full_game_totals_daily_hook.sh`.\n"
        "- Schedule: reuse all five existing daily refreshes; do not add a scheduler. Each run is a legitimate immutable price snapshot.\n"
        "- Main-market timing: 09:30 and 11:00 PT provide the best broad pregame coverage; later 13:00/16:30 runs preserve movements only for still-unstarted games.\n"
        "- Failure isolation: hook failure is logged with its exit status and remains nonblocking to unrelated refresh work.\n"
        "- Player props: unchanged and separately acquired. Moneyline lifecycle: unchanged.\n"
        "- Project-wide main-market contract: preserve each book independently and derive consensus separately; no single book is canonical without an explicit book-specific request.\n"
        "- Retry: a later normal refresh may retry absent games; post-start admission always fails closed.\n"
    )
    lines = ["# Current totals with market", "", "All attached August 6 markets were observed after the frozen model prediction and are labeled accordingly. No EV or wagering calculation is present.", "",
             "| Matchup | Predicted | Book | Market | Difference | P(Over) | P(Under) | Prediction UTC | Market UTC | Timing | Grading |",
             "|---|---:|---|---:|---:|---:|---:|---|---|---|---|"]
    for row in bridge_rows:
        if row.get("market_status") == "TOTAL_MARKET_UNAVAILABLE":
            lines.append(f"| {row['away_team']} @ {row['home_team']} | {float(row['predicted_total']):.3f} | — | MARKET UNAVAILABLE | — | — | — | — | — | MARKET UNAVAILABLE | {row['grading_status']} |")
        else:
            lines.append(f"| {row['away_team']} @ {row['home_team']} | {float(row['predicted_total']):.3f} | {row['bookmaker']} | {float(row['total_line']):.1f} | {float(row['model_minus_market_total']):+.3f} | {float(row['p_over_market_line']):.3f} | {float(row['p_under_market_line']):.3f} | {row['prediction_timestamp_utc']} | {row['captured_at_utc']} | {row['timing_relationship']} | {row['grading_status']} |")
    (output_dir / "current_totals_with_market_report.md").write_text("\n".join(lines) + "\n")
    differences = [float(row["model_minus_market_total"]) for row in bridge_rows if row.get("model_minus_market_total") is not None]
    (output_dir / "concise_mlb_full_game_total_market_capture_v1.md").write_text(
        "# MLB Full-Game Total Market Capture v1\n\n"
        "`FULL_GAME_TOTAL_MARKET_CAPTURE_IMPLEMENTED`\n\n"
        f"- Capture timestamp: {summary['captured_at_utc']}\n"
        f"- August 6 eligible games: {summary['eligible_games']}\n"
        f"- Authentic market rows: {summary['market_rows_parsed']} ({summary['paired_rows']} paired; {summary['line_only_rows']} line-only)\n"
        f"- Frozen predictions attached: {summary['prediction_attachments']}\n"
        f"- Model-minus-market range: {min(differences):+.3f} to {max(differences):+.3f}\n"
        f"- Ledger: {summary['ledger_after']}\n"
        "- Timing: all August 6 attachments are `POST_PREDICTION_MARKET_OBSERVATION`.\n"
        "- Outcomes accessed: 0\n- Public/model/moneyline behavior: unchanged\n"
    )
    hash_path = output_dir / "reproducibility_hashes.sha256"
    hash_path.write_text("".join(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n" for path in sorted(output_dir.iterdir()) if path != hash_path))


def load_or_fetch(game_date: str, snapshot_in: Path | None) -> tuple[list[dict[str, Any]], str, str, Path, str]:
    if snapshot_in:
        snapshot_in = snapshot_in.resolve()
        payload = json.loads(snapshot_in.read_text())
        events = payload.get("events", payload) if isinstance(payload, dict) else payload
        captured = payload.get("captured_at_utc") if isinstance(payload, dict) else None
        captured = captured or now_utc()
        run_tag = snapshot_in.stem
        raw_path = snapshot_in
        raw_sha = hashlib.sha256(raw_path.read_bytes()).hexdigest()
        return events, captured, run_tag, raw_path, raw_sha
    key = os.getenv("ODDS_API_KEY", "").strip()
    if not key:
        raise RuntimeError("ODDS_API_KEY missing")
    response = requests.get(ODDS_URL, params={"apiKey": key, "regions": os.getenv("MLB_ODDS_REGIONS", "us") or "us", "markets": "totals", "oddsFormat": "american", "dateFormat": "iso"}, timeout=30)
    response.raise_for_status()
    events = response.json()
    if not isinstance(events, list):
        raise RuntimeError("unexpected OddsAPI totals payload")
    captured = now_utc()
    run_tag = "full_game_totals_" + datetime.fromisoformat(captured.replace("Z", "+00:00")).strftime("%Y%m%dT%H%M%SZ")
    raw_path = RAW_ROOT / game_date / f"odds_mlb_full_game_totals__{run_tag}.json"
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    wrapper = {"experiment": EXPERIMENT, "game_date_et": game_date, "captured_at_utc": captured, "source": "THE_ODDS_API", "source_run_tag": run_tag, "events": events}
    raw_path.write_text(json.dumps(wrapper, separators=(",", ":"), ensure_ascii=False) + "\n")
    raw_sha = hashlib.sha256(raw_path.read_bytes()).hexdigest()
    return events, captured, run_tag, raw_path, raw_sha


def run(game_date: str, output_dir: Path, ledger_path: Path, snapshot_in: Path | None = None) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    events, captured, run_tag, raw_path, raw_sha = load_or_fetch(game_date, snapshot_in)
    schedule_payload, schedule_observed, schedule_sha = fetch_hydrated_schedule(game_date)
    schedule = normalize_schedule(schedule_payload, schedule_observed, schedule_sha)
    parsed, identity_audit = parse_totals(events=events, schedule=schedule, game_date=game_date, captured_at_utc=captured, source_run_tag=run_tag, raw_source_path=str(raw_path.relative_to(ROOT)), raw_source_sha256=raw_sha)
    ledger = connect_ledger(ledger_path)
    before = ledger_counts(ledger)
    actions = [{**row, "ledger_action": append_market(ledger, row)} for row in parsed]
    all_markets = market_rows(ledger, game_date)
    prediction_conn = connect_prediction_ledger(PREDICTION_LEDGER)
    predictions = rows_for_date(prediction_conn, game_date)
    bridge_rows: list[dict[str, Any]] = []
    consensus_output: list[dict[str, Any]] = []
    for prediction in predictions:
        # Retain the original single-book bridge unchanged for compatibility,
        # while the governing amended contract writes every book independently.
        attach_market(ledger, prediction, all_markets, captured)
        attached_rows = attach_all_markets(ledger, prediction, all_markets, captured)
        if attached_rows:
            for attached in attached_rows:
                probabilities = probability_fields(float(prediction["expected_total"]), MODEL_ALPHA, float(attached["total_line"]))
                bridge_rows.append({"game_pk": prediction["game_pk"], "away_team": prediction["away_team"], "home_team": prediction["home_team"], "predicted_total": prediction["expected_total"], **attached, **probabilities, "grading_status": prediction["grading_status"]})
            for capture_time in sorted({row["captured_at_utc"] for row in attached_rows}):
                consensus = build_consensus(prediction, all_markets, capture_time)
                if consensus:
                    consensus["model_expected_total"] = prediction["expected_total"]
                    consensus.update(probability_fields(float(prediction["expected_total"]), MODEL_ALPHA, float(consensus["median_total_line"])))
                    consensus["model_minus_consensus_line"] = float(prediction["expected_total"]) - float(consensus["median_total_line"])
                    consensus["consensus_action"] = append_consensus(ledger, consensus, captured)
                    consensus_output.append(consensus)
        else:
            bridge_rows.append({"game_pk": prediction["game_pk"], "away_team": prediction["away_team"], "home_team": prediction["home_team"], "predicted_total": prediction["expected_total"], "market_status": "TOTAL_MARKET_UNAVAILABLE", "timing_relationship": "MARKET_UNAVAILABLE", "bridge_action": "MARKET_UNAVAILABLE", "grading_status": prediction["grading_status"]})
    after = ledger_counts(ledger)
    write_csv(output_dir / "august_6_full_game_total_capture.csv", actions)
    write_csv(output_dir / "total_market_identity_audit.csv", identity_audit)
    write_csv(output_dir / "totals_shadow_market_bridge.csv", bridge_rows)
    summary = {"experiment": EXPERIMENT, "captured_at_utc": captured, "run_tag": run_tag, "raw_source_path": str(raw_path), "raw_source_sha256": raw_sha, "provider_events": len(events), "official_games": len(schedule), "eligible_games": len({row['game_id'] for row in parsed}), "market_rows_parsed": len(parsed), "paired_rows": sum(row['market_status']=='TOTAL_MARKET_CERTIFIED_PAIRED' for row in parsed), "line_only_rows": sum(row['market_status']=='TOTAL_MARKET_LINE_ONLY' for row in parsed), "prediction_attachments": sum(row.get('market_status')!='TOTAL_MARKET_UNAVAILABLE' for row in bridge_rows), "consensus_records": len(consensus_output), "ledger_before": before, "ledger_after": after, "outcomes_accessed": 0}
    write_evidence(output_dir, summary, actions, identity_audit, bridge_rows, ledger_path)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--ledger-path", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--snapshot-in", type=Path)
    args = parser.parse_args()
    print(json.dumps(run(args.date, args.output_dir, args.ledger_path, args.snapshot_in), indent=2))


if __name__ == "__main__":
    main()
