"""Grade the frozen totals shadow and compare it with captured pregame markets."""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

import numpy as np

from backend.mlb.totals_predictions.live_context_bridge_v1 import distribution
from backend.mlb.totals_predictions.prospective_shadow_v1 import (
    append_outcome, canonical_identity, connect_ledger, counts, outcomes_for_date, rows_for_date,
)

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_LEDGER = ROOT / "backend/mlb/exports/model_v2/totals_shadow_v1/totals_shadow_v1.sqlite3"
DEFAULT_MARKET_LEDGER = ROOT / "backend/mlb/exports/market_history/full_game_totals/full_game_totals_v1.sqlite3"
OFFICIAL_ROOT = ROOT / "artifacts/analysis/mlb/player_stats_completeness"
ALPHA = 0.12944479977012996
THRESHOLDS = (6.5, 7.5, 8.5, 9.5, 10.5, 11.5)


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = sorted({key for row in rows for key in row})
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader(); writer.writerows(rows)


def official_final(game_date: str, game_id: int) -> dict[str, Any]:
    paths = sorted((OFFICIAL_ROOT / game_date / f"game_{game_id}" / "sources").glob(f"game_{game_id}_live_feed_*.json"))
    if len(paths) != 1:
        raise RuntimeError(f"OFFICIAL_FINAL_SOURCE_COUNT_{game_id}_{len(paths)}")
    path = paths[0]; raw = path.read_bytes(); payload = json.loads(raw)
    if payload.get("gameData", {}).get("status", {}).get("abstractGameState") != "Final":
        raise RuntimeError(f"GAME_NOT_OFFICIALLY_FINAL_{game_id}")
    linescore = payload["liveData"]["linescore"]
    final_total = int(linescore["teams"]["away"]["runs"]) + int(linescore["teams"]["home"]["runs"])
    regulation = sum(int(inn.get("away", {}).get("runs") or 0) + int(inn.get("home", {}).get("runs") or 0)
                     for inn in linescore.get("innings", []) if int(inn.get("num", 0)) <= 9)
    return {"official_final_total": final_total, "regulation_nine_total": regulation,
            "official_source_path": str(path.relative_to(ROOT)), "official_source_hash": hashlib.sha256(raw).hexdigest(),
            "official_status": payload["gameData"]["status"].get("detailedState")}


def crps(expected_total: float, actual: int) -> float:
    mass = distribution(expected_total, ALPHA); support = np.arange(len(mass))
    return float(np.sum((np.cumsum(mass) - (support >= actual).astype(float)) ** 2))


def threshold_scores(expected_total: float, actual: int) -> dict[str, float]:
    mass = distribution(expected_total, ALPHA); support = np.arange(len(mass)); output = {}
    for line in THRESHOLDS:
        probability = float(mass[support > line].sum()); observed = float(actual > line); key = str(line).replace(".", "_")
        output[f"brier_over_{key}"] = (probability-observed)**2
        output[f"log_loss_over_{key}"] = -(observed*math.log(max(probability, 1e-15)) + (1-observed)*math.log(max(1-probability, 1e-15)))
    return output


def line_probabilities(expected_total: float, line: float) -> dict[str, float]:
    mass = distribution(expected_total, ALPHA); support = np.arange(len(mass))
    return {"p_over_sportsbook_line": float(mass[support > line].sum()),
            "p_under_sportsbook_line": float(mass[support < line].sum()),
            "p_push_sportsbook_line": float(mass[support == line].sum())}


def american_implied(price: float) -> float:
    return 100.0/(price+100.0) if price > 0 else abs(price)/(abs(price)+100.0)


def no_vig_over(row: dict[str, Any]) -> float | None:
    if row.get("over_price") is None or row.get("under_price") is None: return None
    over = american_implied(float(row["over_price"])); under = american_implied(float(row["under_price"]))
    return over/(over+under)


def selected_market_rows(connection: sqlite3.Connection, game_date: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    records = [json.loads(row[0]) for row in connection.execute(
        "SELECT market_payload_json FROM full_game_total_market_snapshots WHERE game_date=?", (game_date,))]
    primary = []
    game_ids = sorted({int(row["game_id"]) for row in records if not str(row["bookmaker_key"]).startswith("sportsgameodds:")})
    for game_id in game_ids:
        rows = [row for row in records if int(row["game_id"]) == game_id and
                not str(row["bookmaker_key"]).startswith("sportsgameodds:") and row.get("timing_status") == "PREGAME_CERTIFIED"]
        if rows:
            first = min(row["captured_at_utc"] for row in rows); primary.extend(row for row in rows if row["captured_at_utc"] == first)
    bookmaker = []
    game_ids = sorted({int(row["game_id"]) for row in records if row["bookmaker_key"] == "sportsgameodds:bookmakereu"})
    for game_id in game_ids:
        rows = [row for row in records if int(row["game_id"]) == game_id and row["bookmaker_key"] == "sportsgameodds:bookmakereu" and row.get("timing_status") == "PREGAME_CERTIFIED"]
        if rows: bookmaker.append(min(rows, key=lambda row: row["captured_at_utc"]))
    return primary, bookmaker


def run(game_date: str, output_dir: Path, ledger_path: Path, market_ledger_path: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True); connection = connect_ledger(ledger_path); before = counts(connection)
    predictions = rows_for_date(connection, game_date)
    if not predictions: raise RuntimeError("NO_FROZEN_TOTALS_PREDICTIONS")
    hashes_before = {row["game_pk"]: hashlib.sha256(json.dumps(row, sort_keys=True, separators=(",", ":")).encode()).hexdigest() for row in predictions}
    graded_at = now_utc(); actions = []
    for row in predictions:
        result = official_final(game_date, int(row["game_pk"])); expected = float(row["expected_total"])
        payload = {"game_date": game_date, "game_pk": int(row["game_pk"]), "away_team": row["away_team"], "home_team": row["home_team"],
            "model_version": row["model_version"], "model_hash": row["model_hash"], "prediction_timestamp_utc": row["prediction_timestamp_utc"],
            "expected_total": expected, **result, "signed_residual_final": expected-result["official_final_total"],
            "absolute_error_final": abs(expected-result["official_final_total"]),
            "signed_residual_regulation_nine": expected-result["regulation_nine_total"],
            "absolute_error_regulation_nine": abs(expected-result["regulation_nine_total"]),
            "crps_final": crps(expected, result["official_final_total"]), **threshold_scores(expected, result["official_final_total"])}
        action = append_outcome(connection, canonical_identity(game_date, int(row["game_pk"])), payload, graded_at)
        actions.append({**payload, "ledger_action": action})
    outcomes = outcomes_for_date(connection, game_date); after = counts(connection)
    hashes_after = {row["game_pk"]: hashlib.sha256(json.dumps(row, sort_keys=True, separators=(",", ":")).encode()).hexdigest() for row in rows_for_date(connection, game_date)}
    if hashes_before != hashes_after: raise RuntimeError("PREDICTION_LEDGER_MUTATED")
    by_game = {int(row["game_pk"]): row for row in outcomes}
    market = sqlite3.connect(market_ledger_path); primary, bookmaker = selected_market_rows(market, game_date)
    market_results = []
    for source_class, rows in (("MULTIBOOK_PRIMARY", primary), ("BOOKMAKER_EU_SUPPLEMENTAL", bookmaker)):
        for row in rows:
            outcome = by_game[int(row["game_id"])]; actual = int(outcome["official_final_total"]); line = float(row["total_line"])
            market_results.append({"source_class": source_class, "game_pk": int(row["game_id"]), "away_team": row["away_team"], "home_team": row["home_team"],
                "sportsbook": row.get("bookmaker") or row["bookmaker_key"], "bookmaker_key": row["bookmaker_key"], "total_line": line,
                "over_price": row.get("over_price"), "under_price": row.get("under_price"), "capture_timestamp_utc": row["captured_at_utc"],
                "lead_time_minutes": row.get("lead_time_minutes"), "timing_relationship": "POST_PREDICTION_MARKET_OBSERVATION",
                "raw_source_path": row["raw_source_path"], "raw_source_sha256": row["raw_source_sha256"], "model_expected_total": outcome["expected_total"],
                "model_minus_sportsbook_line": float(outcome["expected_total"])-line, **line_probabilities(float(outcome["expected_total"]), line),
                "sportsbook_no_vig_over_probability": no_vig_over(row), "official_final_total": actual,
                "market_result": "OVER" if actual > line else ("UNDER" if actual < line else "PUSH"),
                "model_absolute_error": abs(float(outcome["expected_total"])-actual), "sportsbook_line_absolute_error": abs(line-actual)})
    consensus_rows = []
    for game_id in sorted(by_game):
        rows = [row for row in primary if int(row["game_id"]) == game_id]; lines = [float(row["total_line"]) for row in rows]
        if not lines: continue
        consensus_line = float(median(lines)); modes = Counter(lines); max_count = max(modes.values()); modal = sorted(k for k,v in modes.items() if v == max_count)
        outcome = by_game[game_id]; actual = int(outcome["official_final_total"]); expected = float(outcome["expected_total"])
        same_line = [row for row in rows if float(row["total_line"]) == consensus_line]
        no_vig = [value for value in (no_vig_over(row) for row in same_line) if value is not None]
        over_prices = [float(row["over_price"]) for row in same_line if row.get("over_price") is not None]
        under_prices = [float(row["under_price"]) for row in same_line if row.get("under_price") is not None]
        consensus_rows.append({"game_pk": game_id, "away_team": outcome["away_team"], "home_team": outcome["home_team"], "books": len(rows),
            "minimum_line": min(lines), "maximum_line": max(lines), "median_consensus_line": consensus_line,
            "modal_line": modal[0] if len(modal) == 1 else None, "modal_line_share": max_count/len(lines), "line_dispersion": max(lines)-min(lines),
            "books_at_consensus_line": len(same_line), "median_over_price_at_consensus_line": median(over_prices) if over_prices else None,
            "median_under_price_at_consensus_line": median(under_prices) if under_prices else None,
            "median_no_vig_over_probability_at_consensus_line": median(no_vig) if no_vig else None,
            **{key.replace("sportsbook", "consensus"): value for key, value in line_probabilities(expected, consensus_line).items()},
            "model_expected_total": expected, "official_final_total": actual, "model_absolute_error": abs(expected-actual),
            "consensus_absolute_error": abs(consensus_line-actual), "model_minus_consensus_line": expected-consensus_line,
            "consensus_signed_residual": consensus_line-actual})
    model_mae = mean(float(row["absolute_error_final"]) for row in outcomes); model_bias = mean(float(row["signed_residual_final"]) for row in outcomes)
    reg_mae = mean(float(row["absolute_error_regulation_nine"]) for row in outcomes); model_crps = mean(float(row["crps_final"]) for row in outcomes)
    consensus_mae = mean(float(row["consensus_absolute_error"]) for row in consensus_rows); consensus_bias = mean(float(row["consensus_signed_residual"]) for row in consensus_rows)
    model_closer = sum(row["model_absolute_error"] < row["consensus_absolute_error"] for row in consensus_rows)
    consensus_closer = sum(row["consensus_absolute_error"] < row["model_absolute_error"] for row in consensus_rows); ties = len(consensus_rows)-model_closer-consensus_closer
    book_metrics = []
    for book in sorted({row["bookmaker_key"] for row in market_results}):
        rows = [row for row in market_results if row["bookmaker_key"] == book]
        book_metrics.append({"bookmaker_key": book, "sportsbook": rows[0]["sportsbook"], "games": len(rows),
            "mae": mean(float(row["sportsbook_line_absolute_error"]) for row in rows),
            "signed_bias": mean(float(row["total_line"])-float(row["official_final_total"]) for row in rows), "source_class": rows[0]["source_class"]})
    summary = {"decision": "AUGUST_6_TOTALS_PROSPECTIVE_GRADE_COMPLETE", "game_date": game_date, "frozen_predictions": len(predictions),
        "official_finals": len(outcomes), "new_outcome_rows": sum(a["ledger_action"] == "APPENDED_NEW" for a in actions),
        "model_mae_final": model_mae, "model_signed_bias_final": model_bias, "model_mae_regulation_nine": reg_mae, "model_crps_final": model_crps,
        "consensus_market_mae": consensus_mae, "consensus_market_signed_bias": consensus_bias,
        "model_closer_than_consensus": model_closer, "consensus_closer_than_model": consensus_closer, "ties": ties,
        "primary_multibook_rows": len(primary), "primary_books": len({row["bookmaker_key"] for row in primary}), "bookmaker_eu_rows": len(bookmaker),
        "ledger_before": before, "ledger_after": after, "duplicate_outcome_identities": after["duplicate_outcome_identities"],
        "prediction_rows_unchanged": True, "market_timing": "POST_PREDICTION_MARKET_OBSERVATION", "public_status": "SHADOW_ONLY_NOT_PUBLIC"}
    write_csv(output_dir/"august_6_totals_grading.csv", outcomes); write_csv(output_dir/"august_6_multibook_market_results.csv", market_results)
    write_csv(output_dir/"august_6_multibook_consensus.csv", consensus_rows); write_csv(output_dir/"august_6_book_specific_metrics.csv", book_metrics)
    (output_dir/"august_6_totals_grade_summary.json").write_text(json.dumps(summary, indent=2)+"\n")
    (output_dir/"august_6_totals_grade_report.md").write_text(
        "# August 6 totals prospective grade\n\n" f"`{summary['decision']}`\n\n- Frozen games/finals: {len(predictions)}/{len(outcomes)}\n"
        f"- Model final-score MAE / signed bias / CRPS: {model_mae:.6f} / {model_bias:+.6f} / {model_crps:.6f}\n"
        f"- Model regulation-nine MAE: {reg_mae:.6f}\n- 11-book consensus MAE / signed bias: {consensus_mae:.6f} / {consensus_bias:+.6f}\n"
        f"- Model closer / consensus closer / ties: {model_closer}/{consensus_closer}/{ties}\n"
        f"- Book-specific rows: {len(market_results)} ({len(primary)} accepted 11-book observations; {len(bookmaker)} separate BookMaker.eu observations)\n"
        "- Timing: every comparison is `POST_PREDICTION_MARKET_OBSERVATION`. No EV, ROI, ranking, or wager calculation is present.\n")
    hash_path = output_dir/"reproducibility_hashes.sha256"; files = sorted(path for path in output_dir.iterdir() if path.is_file() and path != hash_path)
    hash_path.write_text("".join(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n" for path in files))
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--date", required=True); parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--ledger-path", type=Path, default=DEFAULT_LEDGER); parser.add_argument("--market-ledger-path", type=Path, default=DEFAULT_MARKET_LEDGER)
    args = parser.parse_args(); print(json.dumps(run(args.date, args.output_dir, args.ledger_path, args.market_ledger_path), indent=2))


if __name__ == "__main__": main()
