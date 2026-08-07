"""Attach already captured full-game totals markets to immutable shadow rows."""
from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.mlb.markets.full_game_total_capture_v1 import (
    append_consensus,
    attach_all_markets,
    attach_market,
    build_consensus,
    connect_ledger,
    ledger_counts,
    market_rows,
)
from backend.mlb.scripts.run_mlb_totals_prospective_shadow_v1 import probability_fields
from backend.mlb.scripts.report_mlb_totals_prospective_snapshot_v1 import normalized_market, select_canonical_observations
from backend.mlb.totals_predictions.prospective_shadow_v1 import (
    connect_ledger as connect_prediction_ledger,
    rows_for_date,
)

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PREDICTION_LEDGER = ROOT / "backend/mlb/exports/model_v2/totals_shadow_v1/totals_shadow_v1.sqlite3"
DEFAULT_MARKET_LEDGER = ROOT / "backend/mlb/exports/market_history/full_game_totals/full_game_totals_v1.sqlite3"
MODEL_ALPHA = 0.12944479977012996


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fields = sorted({key for row in rows for key in row})
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader(); writer.writerows(rows)


def run(game_date: str, output_dir: Path, prediction_ledger_path: Path, market_ledger_path: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    created_at = now_utc()
    market_ledger = connect_ledger(market_ledger_path)
    predictions = rows_for_date(connect_prediction_ledger(prediction_ledger_path), game_date)
    markets = market_rows(market_ledger, game_date)
    before = ledger_counts(market_ledger)
    attachment_rows: list[dict[str, Any]] = []
    consensus_rows: list[dict[str, Any]] = []
    unavailable: list[dict[str, Any]] = []
    for prediction in predictions:
        selected, selected_action = attach_market(market_ledger, prediction, markets, created_at)
        attached = attach_all_markets(market_ledger, prediction, markets, created_at)
        if not attached:
            unavailable.append({"game_pk": prediction["game_pk"], "away_team": prediction["away_team"],
                                "home_team": prediction["home_team"], "market_status": "MARKET_UNAVAILABLE"})
            continue
        for row in attached:
            probabilities = probability_fields(float(prediction["expected_total"]), MODEL_ALPHA, float(row["total_line"]))
            attachment_rows.append({"game_pk": prediction["game_pk"], "away_team": prediction["away_team"],
                "home_team": prediction["home_team"], "model_expected_total": prediction["expected_total"],
                "selected_market_action": selected_action, "selected_market_identity": selected.get("canonical_market_identity") if selected else None,
                **row, **probabilities})
        for captured_at in sorted({row["captured_at_utc"] for row in attached}):
            consensus = build_consensus(prediction, markets, captured_at)
            if consensus:
                consensus["model_expected_total"] = prediction["expected_total"]
                consensus.update(probability_fields(float(prediction["expected_total"]), MODEL_ALPHA, float(consensus["median_total_line"])))
                consensus["model_minus_consensus_line"] = float(prediction["expected_total"]) - float(consensus["median_total_line"])
                consensus["consensus_action"] = append_consensus(market_ledger, consensus, created_at)
                consensus_rows.append(consensus)
    canonical_observations = select_canonical_observations([normalized_market(row) for row in markets])
    canonical_consensus = []
    for prediction in predictions:
        books = [row for row in canonical_observations if int(row["game_pk"]) == int(prediction["game_pk"])]
        if not books: continue
        lines = [float(row["total_line"]) for row in books]; median_line = float(statistics.median(lines))
        modes = statistics.multimode(lines); max_count = max(Counter(lines).values())
        same_line = [row for row in books if float(row["total_line"]) == median_line]
        no_vig = [row["no_vig_over_probability"] for row in same_line if row.get("no_vig_over_probability") is not None]
        probabilities = probability_fields(float(prediction["expected_total"]), MODEL_ALPHA, median_line)
        canonical_consensus.append({"game_pk": prediction["game_pk"], "away_team": prediction["away_team"], "home_team": prediction["home_team"],
            "canonical_books_represented": len(books), "distinct_total_lines": len(set(lines)), "minimum_line": min(lines), "maximum_line": max(lines),
            "median_consensus_line": median_line, "modal_line": modes[0] if len(modes) == 1 else None,
            "modal_share": max_count/len(lines), "same_line_book_count": len(same_line),
            "same_line_median_no_vig_over_probability": statistics.median(no_vig) if no_vig else None,
            "model_expected_total": prediction["expected_total"], "model_minus_consensus_line": probabilities["model_minus_market_total"],
            "model_p_over_consensus_line": probabilities["p_over_market_line"], "model_p_under_consensus_line": probabilities["p_under_market_line"]})
    after = ledger_counts(market_ledger)
    summary = {
        "game_date": game_date, "prediction_rows": len(predictions), "captured_market_rows_available": len(markets),
        "predictions_with_market": len({int(row["game_pk"]) for row in attachment_rows}),
        "market_unavailable_predictions": len(unavailable),
        "new_all_book_bridge_rows": after["all_book_bridge_rows"] - before["all_book_bridge_rows"],
        "new_consensus_rows": after["consensus_rows"] - before["consensus_rows"],
        "bookmaker_eu_prediction_coverage": len({int(row["game_pk"]) for row in attachment_rows
            if row.get("bookmaker_key") == "sportsgameodds:bookmakereu"}),
        "canonical_consensus_predictions": len(canonical_consensus),
        "ledger_before": before, "ledger_after": after, "outcomes_accessed": 0,
    }
    slug = game_date.replace("-", "_")
    write_csv(output_dir/f"{slug}_existing_market_attachments.csv", attachment_rows)
    write_csv(output_dir/f"{slug}_existing_market_consensus.csv", consensus_rows)
    write_csv(output_dir/f"{slug}_canonical_deduplicated_consensus.csv", canonical_consensus)
    write_csv(output_dir/f"{slug}_market_unavailable.csv", unavailable)
    (output_dir/f"{slug}_existing_market_attachment_summary.json").write_text(json.dumps(summary, indent=2)+"\n")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True); parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--prediction-ledger-path", type=Path, default=DEFAULT_PREDICTION_LEDGER)
    parser.add_argument("--market-ledger-path", type=Path, default=DEFAULT_MARKET_LEDGER)
    args = parser.parse_args()
    print(json.dumps(run(args.date, args.output_dir, args.prediction_ledger_path, args.market_ledger_path), indent=2))


if __name__ == "__main__":
    main()
