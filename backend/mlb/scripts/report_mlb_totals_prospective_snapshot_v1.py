"""Certify a frozen totals snapshot against already captured multi-book markets."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sqlite3
import statistics
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.mlb.markets.main_market_provider_replacement_trial_v1 import canonical_book_id, display_name
from backend.mlb.scripts.run_mlb_totals_prospective_shadow_v1 import probability_fields
from backend.mlb.totals_predictions.live_context_bridge_v1 import (
    GOVERNED_STARTER_HISTORY_TIERS, attach_context, build_history, canonical_hash, fetch_hydrated_schedule, load_candidate, normalize_schedule,
)
from backend.mlb.totals_predictions.prospective_shadow_v1 import (
    append_context, append_prediction, canonical_identity, connect_ledger, contexts_for_date, counts, rows_for_date,
)

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_LEDGER = ROOT / "backend/mlb/exports/model_v2/totals_shadow_v1/totals_shadow_v1.sqlite3"
DEFAULT_MARKET_LEDGER = ROOT / "backend/mlb/exports/market_history/full_game_totals/full_game_totals_v1.sqlite3"
PRIORITY = {"bookmakereu": "BookMaker.eu", "pinnacle": "Pinnacle", "circa": "Circa",
            "primesports": "Prime Sports", "prime": "Prime Sports"}
MODEL_ALPHA = 0.12944479977012996


def utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = sorted({key for row in rows for key in row})
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader(); writer.writerows(rows)


def implied(price: float) -> float:
    return 100/(price+100) if price > 0 else abs(price)/(abs(price)+100)


def no_vig_over(over: float | None, under: float | None) -> float | None:
    if over is None or under is None: return None
    a, b = implied(float(over)), implied(float(under))
    return a/(a+b)


def normalized_market(row: dict[str, Any]) -> dict[str, Any]:
    provider = str(row.get("provider") or "THE_ODDS_API")
    canonical = canonical_book_id(provider, str(row["bookmaker_key"]))
    if provider == "SPORTSGAMEODDS":
        over, under = row.get("over_american_price"), row.get("under_american_price")
        updated = row.get("provider_market_updated_at_utc")
        title = display_name(canonical)
    else:
        over, under = row.get("over_price"), row.get("under_price")
        updated = row.get("provider_market_timestamp_utc")
        title = row.get("bookmaker") or display_name(canonical)
    fetch = row["captured_at_utc"]
    return {"game_pk": int(row["game_id"]), "away_team": row.get("away_team"), "home_team": row.get("home_team"),
        "provider": provider, "provider_bookmaker_key": row["bookmaker_key"], "canonical_bookmaker_id": canonical,
        "sportsbook": title, "total_line": float(row["total_line"]), "over_price": over, "under_price": under,
        "no_vig_over_probability": no_vig_over(over, under), "bookmaker_update_timestamp_utc": updated,
        "fetch_timestamp_utc": fetch,
        "update_age_minutes": ((utc(fetch)-utc(updated)).total_seconds()/60 if updated else None),
        "raw_source_path": row["raw_source_path"], "raw_source_sha256": row["raw_source_sha256"],
        "canonical_market_identity": row["canonical_market_identity"], "timing_status": row.get("timing_status")}


def select_canonical_observations(observations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Choose the freshest provider observation once per exact game/canonical book."""
    groups: dict[tuple[int,str], list[dict[str, Any]]] = {}
    for row in observations: groups.setdefault((row["game_pk"], row["canonical_bookmaker_id"]), []).append(row)
    selected = []
    for values in groups.values():
        choice = max(values, key=lambda row: (row.get("bookmaker_update_timestamp_utc") or row["fetch_timestamp_utc"], row["fetch_timestamp_utc"], row["provider"]))
        selected.append({**choice, "consensus_selection_provider": choice["provider"],
            "preserved_source_observation_count": len(values),
            "alternate_provider_observations": "|".join(sorted({row["provider"] for row in values if row is not choice}))})
    return selected


def context_reasons(context: dict[str, Any]) -> list[str]:
    reasons = []
    for side in ("away", "home"):
        status = context.get(f"{side}_probable_pitcher_status")
        if status != "PROBABLE_PITCHER_CERTIFIED": reasons.append(f"{side.upper()}_{status}")
        tier = context[f"{side}_starter_state"]["fallback_tier"]
        if status == "PROBABLE_PITCHER_CERTIFIED" and tier not in GOVERNED_STARTER_HISTORY_TIERS:
            reasons.append(f"{side.upper()}_STARTER_UNGOVERNED_{tier}")
    park = context["park_state"]["fallback_status"]
    if park != "DIRECT_REGRESSED_PARK_HISTORY": reasons.append(f"PARK_{park}")
    return reasons


def run(game_date: str, output_dir: Path, ledger_path: Path, market_ledger_path: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    slug = game_date.replace("-", "_")
    ledger = connect_ledger(ledger_path); before = counts(ledger); predictions = rows_for_date(ledger, game_date)
    if not predictions: raise RuntimeError("NO_FROZEN_TOTALS_PREDICTIONS")
    contexts = contexts_for_date(ledger, game_date); prediction_by_game = {int(row["game_pk"]): row for row in predictions}
    payload, schedule_observed, schedule_hash = fetch_hydrated_schedule(game_date)
    schedule = normalize_schedule(payload, schedule_observed, schedule_hash); history = build_history()
    frozen_cutoff = min(utc(row["prediction_timestamp_utc"]) for row in predictions)
    discovery, prediction_rows = [], []
    for schedule_row in schedule:
        game_id = int(schedule_row["game_pk"]); context = attach_context(schedule_row, history, schedule_observed)
        started = utc(schedule_row["scheduled_start_utc"]) <= frozen_cutoff or schedule_row["official_game_status"] not in {"Scheduled", "Pre-Game", "Warmup"}
        frozen = prediction_by_game.get(game_id); reasons = context_reasons(context)
        status = "REJECTED_GAME_ALREADY_STARTED" if started else ("CAPTURED_CONTEXT_COMPLETE" if frozen else "REJECTED_CONTEXT_NOT_COMPLETE")
        discovery.append({**{key: schedule_row.get(key) for key in ("game_pk","away_team_name","home_team_name","scheduled_start_utc",
            "away_probable_pitcher_id","away_probable_pitcher_name","away_probable_pitcher_status","home_probable_pitcher_id",
            "home_probable_pitcher_name","home_probable_pitcher_status","venue_id","venue_name","game_number","doubleheader_state","official_game_status")},
            "context_quality_state": context["data_quality_status"], "discovery_status": status,
            "rejection_reason": "|".join(reasons) if not frozen else ""})
        if not frozen: continue
        identity = canonical_identity(game_date, game_id); component = contexts[identity]
        prediction_rows.append({**{key: value for key,value in frozen.items() if not isinstance(value, dict)},
            "away_starter_state_hash": component["away_starter_state"]["state_hash"],
            "home_starter_state_hash": component["home_starter_state"]["state_hash"],
            "park_state_hash": component["park_state"]["state_hash"],
            "feature_cutoff_utc": frozen["dynamic_league_environment"]["feature_cutoff_utc"]})
    if any(row["context_quality_state"] != "TOTALS_CONTEXT_COMPLETE" for row in prediction_rows):
        raise RuntimeError("NONCOMPLETE_CONTEXT_IN_FROZEN_LEDGER")

    market_db = sqlite3.connect(market_ledger_path)
    raw_rows = [json.loads(row[0]) for row in market_db.execute(
        "SELECT market_payload_json FROM full_game_total_market_snapshots WHERE game_date=?", (game_date,))]
    observations = []
    for raw in raw_rows:
        game_id = int(raw["game_id"]); prediction = prediction_by_game.get(game_id)
        if not prediction or utc(raw["captured_at_utc"]) > utc(prediction["prediction_timestamp_utc"]): continue
        if utc(raw["captured_at_utc"]) >= utc(prediction["scheduled_start_utc"]): continue
        observations.append(normalized_market(raw))
    selected = select_canonical_observations(observations)

    consensus, priority_rows = [], []
    for game_id, prediction in sorted(prediction_by_game.items()):
        books = [row for row in selected if row["game_pk"] == game_id]; lines = [row["total_line"] for row in books]
        if not books: continue
        median_line = float(statistics.median(lines)); modes = statistics.multimode(lines); max_count = max(lines.count(line) for line in set(lines))
        same = [row for row in books if row["total_line"] == median_line]
        probs = [row["no_vig_over_probability"] for row in same if row["no_vig_over_probability"] is not None]
        probability = probability_fields(float(prediction["expected_total"]), MODEL_ALPHA, median_line)
        composition = Counter(row["consensus_selection_provider"] for row in books)
        consensus.append({"game_pk": game_id, "away_team": prediction["away_team"], "home_team": prediction["home_team"],
            "canonical_books_represented": len(books), "distinct_total_lines": len(set(lines)), "minimum_line": min(lines), "maximum_line": max(lines),
            "median_consensus_line": median_line, "modal_line": modes[0] if len(modes) == 1 else None,
            "modal_share": max_count/len(lines), "same_line_book_count": len(same),
            "same_line_median_no_vig_over_probability": statistics.median(probs) if probs else None,
            "consensus_source_composition": "|".join(f"{key}:{composition[key]}" for key in sorted(composition)),
            "model_expected_total": prediction["expected_total"], "model_minus_consensus_line": probability["model_minus_market_total"],
            "model_p_over_consensus_line": probability["p_over_market_line"], "model_p_under_consensus_line": probability["p_under_market_line"],
            "context_quality_state": prediction["context_quality_state"], "prediction_timestamp_utc": prediction["prediction_timestamp_utc"]})
        for row in books:
            if row["canonical_bookmaker_id"] not in PRIORITY: continue
            p = probability_fields(float(prediction["expected_total"]), MODEL_ALPHA, row["total_line"])
            priority_rows.append({**row, "priority_book": PRIORITY[row["canonical_bookmaker_id"]],
                "model_expected_total": prediction["expected_total"], "model_minus_line": p["model_minus_market_total"],
                "model_p_over_line": p["p_over_market_line"], "model_p_under_line": p["p_under_market_line"]})

    prediction_hashes_before = {row["game_pk"]: canonical_hash(row) for row in predictions}
    repeat_prediction_actions = [append_prediction(ledger, row) for row in predictions]
    repeat_context_actions = [append_context(ledger, canonical_identity(game_date, int(row["game_pk"])),
        contexts[canonical_identity(game_date, int(row["game_pk"]))], row["feature_state_hash"], row["prediction_timestamp_utc"]) for row in predictions]
    prediction_hashes_after = {row["game_pk"]: canonical_hash(row) for row in rows_for_date(ledger, game_date)}
    after = counts(ledger)
    idempotent = (all(action == "EXISTING_IMMUTABLE" for action in repeat_prediction_actions+repeat_context_actions)
                  and prediction_hashes_before == prediction_hashes_after and after == before)
    all_bridge_count = market_db.execute("SELECT COUNT(*) FROM totals_shadow_all_book_market_bridge WHERE prediction_identity LIKE ?", (f"{game_date}|%",)).fetchone()[0]
    outcome_rows_for_date = ledger.execute("""SELECT COUNT(*) FROM totals_shadow_outcomes o JOIN totals_shadow_predictions p
        USING(canonical_identity) WHERE p.game_date=?""", (game_date,)).fetchone()[0]
    owner = sorted(consensus, key=lambda row: abs(float(row["model_minus_consensus_line"])), reverse=True)
    bookmaker_by_game = {row["game_pk"]: row for row in priority_rows if row["priority_book"] == "BookMaker.eu"}
    lines = ["# August 7 MLB totals prospective snapshot", "", "Descriptive frozen shadow only. No EV, ranking, staking, wager, deployment, or public output.", "",
        "| Matchup | Expected | Consensus | Model − consensus | P(Over) | P(Under) | BookMaker.eu | Context | Books | Prediction UTC |",
        "|---|---:|---:|---:|---:|---:|---:|---|---:|---|"]
    for row in owner:
        book = bookmaker_by_game.get(row["game_pk"]); book_line = f"{book['total_line']:.1f}" if book else "—"
        lines.append(f"| {row['away_team']} @ {row['home_team']} | {row['model_expected_total']:.3f} | {row['median_consensus_line']:.1f} | {row['model_minus_consensus_line']:+.3f} | {row['model_p_over_consensus_line']:.3f} | {row['model_p_under_consensus_line']:.3f} | {book_line} | {row['context_quality_state']} | {row['canonical_books_represented']} | {row['prediction_timestamp_utc']} |")
    (output_dir/f"{slug}_totals_owner_report.md").write_text("\n".join(lines)+"\n")
    write_csv(output_dir/f"{slug}_game_discovery.csv", discovery); write_csv(output_dir/f"{slug}_frozen_predictions.csv", prediction_rows)
    write_csv(output_dir/f"{slug}_all_provider_market_observations.csv", observations)
    write_csv(output_dir/f"{slug}_canonical_book_consensus_selections.csv", selected)
    write_csv(output_dir/f"{slug}_multibook_consensus.csv", consensus); write_csv(output_dir/f"{slug}_priority_books.csv", priority_rows)
    schedule_record = {"game_date": game_date, "observed_at_utc": schedule_observed, "source_sha256": schedule_hash,
                       "outcome_fields_requested": False, "payload": payload}
    (output_dir/f"{slug}_official_schedule_source.json").write_text(json.dumps(schedule_record, separators=(",", ":"))+"\n")
    candidate = load_candidate(); largest = [{"game_pk": row["game_pk"], "matchup": f"{row['away_team']} @ {row['home_team']}",
        "model_minus_consensus_line": row["model_minus_consensus_line"]} for row in owner[:5]]
    summary = {"declaration": "AUGUST_7_TOTALS_SNAPSHOT_PARTIAL_CONTEXT" if len(predictions) < len(schedule) else "AUGUST_7_TOTALS_PROSPECTIVE_SNAPSHOT_FROZEN",
        "game_date": game_date, "games_discovered": len(schedule), "pregame_eligible": sum(row["discovery_status"] != "REJECTED_GAME_ALREADY_STARTED" for row in discovery),
        "games_captured": len(predictions), "context_complete_games": len(predictions), "rejected_games": len(schedule)-len(predictions),
        "rejection_reasons": dict(Counter(row["rejection_reason"] for row in discovery if row["rejection_reason"])),
        "prediction_min": min(float(row["expected_total"]) for row in predictions), "prediction_max": max(float(row["expected_total"]) for row in predictions),
        "model_version": predictions[0]["model_version"], "model_hash": candidate["canonical_model_hash"],
        "consensus_games": len(consensus), "canonical_consensus_book_range": [min(row["canonical_books_represented"] for row in consensus), max(row["canonical_books_represented"] for row in consensus)],
        "raw_provider_observations_attached": all_bridge_count, "selected_canonical_book_observations": len(selected),
        "bookmaker_eu_coverage": sum(row["priority_book"] == "BookMaker.eu" for row in priority_rows),
        "priority_book_coverage": {name: sum(row["priority_book"] == name for row in priority_rows) for name in sorted(set(PRIORITY.values()))},
        "largest_model_consensus_disagreements": largest, "ledger_row_count": after["prediction_rows"], "date_ledger_row_count": len(predictions),
        "duplicate_prediction_identities": after["duplicate_prediction_identities"], "idempotency_result": "PASS_ZERO_NEW_ROWS_STABLE_HASHES" if idempotent else "FAIL",
        "august_7_outcome_rows": outcome_rows_for_date, "outcomes_accessed_for_scoring_or_reporting": 0,
        "august_6_reference": {"games":5,"model_mae":5.1674,"model_bias":-3.5883,"regulation_nine_mae":3.3674,"consensus_mae":5.0,"consensus_bias":-3.2},
        "august_8_automation_readiness": "READY_FOR_EXISTING_0930_HOOK_NOT_INSTALLED"}
    (output_dir/f"{slug}_totals_snapshot_summary.json").write_text(json.dumps(summary, indent=2)+"\n")
    hash_path = output_dir/"reproducibility_hashes.sha256"; files = sorted(path for path in output_dir.iterdir() if path.is_file() and path != hash_path)
    hash_path.write_text("".join(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n" for path in files))
    return summary


def main() -> None:
    parser=argparse.ArgumentParser();parser.add_argument("--date",required=True);parser.add_argument("--output-dir",type=Path,required=True)
    parser.add_argument("--ledger-path",type=Path,default=DEFAULT_LEDGER);parser.add_argument("--market-ledger-path",type=Path,default=DEFAULT_MARKET_LEDGER)
    args=parser.parse_args();print(json.dumps(run(args.date,args.output_dir,args.ledger_path,args.market_ledger_path),indent=2))


if __name__=="__main__":main()
