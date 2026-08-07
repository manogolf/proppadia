"""Non-public daily lifecycle for the immutable MLB totals prospective shadow."""
from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from backend.mlb.scripts.attach_mlb_totals_shadow_existing_markets_v1 import run as attach_markets
from backend.mlb.scripts.grade_mlb_totals_prospective_shadow_v1 import run as grade
from backend.mlb.scripts.run_mlb_totals_prospective_shadow_v1 import run as score
from backend.mlb.totals_predictions.live_context_bridge_v1 import load_candidate
from backend.mlb.totals_predictions.prospective_shadow_v1 import connect_ledger, counts

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_LEDGER = ROOT / "backend/mlb/exports/model_v2/totals_shadow_v1/totals_shadow_v1.sqlite3"
DEFAULT_MARKET_LEDGER = ROOT / "backend/mlb/exports/market_history/full_game_totals/full_game_totals_v1.sqlite3"
DEFAULT_OUTPUT_ROOT = ROOT / "artifacts/analysis/model_development/mlb_totals_prospective_shadow_v1"
MODEL_HASH = "fb1c730d295ce28d90436ec95cb71d1a81813679de8364e838255111917498ac"
GRADE_ONLY = "GRADE_ONLY"
PRIMARY_SCORE = "PRIMARY_SCORE"
SCORE_MISSING = "SCORE_MISSING"


def resolve_mode(mode: str, wrapper_started_at_utc: str) -> str:
    if mode == "grade-only": return GRADE_ONLY
    if mode == "score-missing": return SCORE_MISSING
    started = datetime.fromisoformat(wrapper_started_at_utc.replace("Z", "+00:00"))
    pacific = started.astimezone(ZoneInfo("America/Los_Angeles"))
    return PRIMARY_SCORE if (pacific.hour, pacific.minute) < (8, 30) else SCORE_MISSING


def pending_grade_dates(connection: sqlite3.Connection, completed_through: str) -> list[str]:
    return [row[0] for row in connection.execute(
        """SELECT DISTINCT p.game_date FROM totals_shadow_predictions p
           LEFT JOIN totals_shadow_outcomes o USING(canonical_identity)
           WHERE p.game_date<=? AND o.canonical_identity IS NULL ORDER BY p.game_date""", (completed_through,)
    ).fetchall()]


def run(
    slate_date: str, completed_through: str, mode: str, wrapper_started_at_utc: str,
    output_root: Path, ledger_path: Path, market_ledger_path: Path,
) -> dict[str, Any]:
    candidate = load_candidate()
    if candidate["canonical_model_hash"] != MODEL_HASH:
        raise RuntimeError("TOTALS_MODEL_HASH_MISMATCH")
    resolved_mode = resolve_mode(mode, wrapper_started_at_utc)
    if resolved_mode in (PRIMARY_SCORE, SCORE_MISSING):
        current_et = datetime.now(ZoneInfo("America/New_York")).date().isoformat()
        if slate_date != current_et:
            raise RuntimeError(f"TOTALS_NONCURRENT_SCORING_BLOCKED requested={slate_date} current_et={current_et}")
    connection = connect_ledger(ledger_path); before = counts(connection)
    grading = []
    for date_value in pending_grade_dates(connection, completed_through):
        grading.append(grade(date_value, output_root/date_value, ledger_path, market_ledger_path, allow_partial=True))
    scoring = None; markets = None
    if resolved_mode in (PRIMARY_SCORE, SCORE_MISSING):
        scoring = score(slate_date, output_root/slate_date, ledger_path)
        markets = attach_markets(slate_date, output_root/slate_date, ledger_path, market_ledger_path)
    after = counts(connect_ledger(ledger_path))
    return {
        "status": "TOTALS_SHADOW_DAILY_LIFECYCLE_COMPLETE",
        "slate_date": slate_date, "completed_through": completed_through,
        "requested_mode": mode, "resolved_mode": resolved_mode,
        "wrapper_started_at_utc": wrapper_started_at_utc,
        "grading_dates_attempted": [row["game_date"] for row in grading],
        "new_outcome_rows": sum(int(row["new_outcome_rows"]) for row in grading),
        "grading": grading, "scoring": scoring, "market_attachment": markets,
        "ledger_before": before, "ledger_after": after,
        "model_version": "DIRECT_NEGATIVE_BINOMIAL", "model_hash": MODEL_HASH,
        "public_totals_status": "UNAVAILABLE_SHADOW_ONLY", "ev_or_wager_outputs": 0,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--slate-date", required=True); parser.add_argument("--completed-through", required=True)
    parser.add_argument("--mode", choices=("auto", "grade-only", "score-missing"), default="auto")
    parser.add_argument("--wrapper-started-at-utc", required=True)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--ledger-path", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--market-ledger-path", type=Path, default=DEFAULT_MARKET_LEDGER)
    parser.add_argument("--output-json", type=Path)
    args = parser.parse_args()
    result = run(args.slate_date, args.completed_through, args.mode, args.wrapper_started_at_utc,
                 args.output_root, args.ledger_path, args.market_ledger_path)
    text = json.dumps(result, indent=2, default=str)
    if args.output_json:
        args.output_json.parent.mkdir(parents=True, exist_ok=True); args.output_json.write_text(text+"\n")
    print(text)


if __name__ == "__main__":
    main()
