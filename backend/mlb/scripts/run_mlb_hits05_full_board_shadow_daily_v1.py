#!/usr/bin/env python3
"""Run one fail-closed, shadow-only full-board score/attach/grade/report cycle."""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from backend.mlb.hits05_full_board_shadow import ledger_v1 as ledger
from backend.mlb.scripts.attach_mlb_hits05_full_board_markets_v1 import attach_date
from backend.mlb.scripts.grade_mlb_hits05_full_board_shadow_v1 import COMPLETENESS_ROOT, grade_date
from backend.mlb.scripts.report_mlb_hits05_full_board_shadow_v1 import build_report
from backend.mlb.scripts.score_mlb_hits05_full_board_shadow_v1 import (
    DEFAULT_LEDGER,
    DEFAULT_SUMMARY_ROOT,
    latest_parent_dir,
    now_utc,
    parse_dt,
    rel,
    score_board,
)
from backend.mlb.scripts.validate_mlb_hits05_full_board_shadow_v1 import PACKAGE, validate


ROOT = Path(__file__).resolve().parents[3]


def _missed_run(slate_date: str, run_tag: str, capture_time: datetime, ledger_path: Path, reason: str) -> dict[str, Any]:
    connection = ledger.connect_ledger(ledger_path)
    payload = {
        "run_tag": run_tag,
        "slate_date": slate_date,
        "capture_timestamp_utc": capture_time.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "evidence_mode": "PROSPECTIVE",
        "run_status": "MISSED_SLATE_OR_RETRY_PENDING_NO_RETROSPECTIVE_RECONSTRUCTION",
        "eligible_rows": 0,
        "new_prediction_rows": 0,
        "existing_prediction_rows": 0,
        "excluded_rows": 0,
        "outcomes_accessed": 0,
        "failure_reason": reason,
        "retrospective_manufacture_authorized": False,
    }
    ledger.append_run(connection, payload)
    return payload


def run_daily(slate_date: str, run_tag: str, ledger_path: Path, capture_time: datetime) -> dict[str, Any]:
    if slate_date < ledger.EXPERIMENT_START_DATE:
        return {"status": "FULL_BOARD_SHADOW_NOT_STARTED", "slate_date": slate_date, "experiment_start_date": ledger.EXPERIMENT_START_DATE}
    parent = latest_parent_dir(slate_date)
    if parent is None:
        scoring = _missed_run(slate_date, run_tag, capture_time, ledger_path, "GOVERNED_NONMARKET_PARENT_NOT_AVAILABLE")
    else:
        scoring = score_board(
            slate_date=slate_date,
            run_tag=run_tag,
            parent_dir=parent,
            capture_time=capture_time,
            ledger_path=ledger_path,
        )
    markets = attach_date(slate_date, ledger_path)
    prior = (date.fromisoformat(slate_date) - timedelta(days=1)).isoformat()
    completeness = COMPLETENESS_ROOT / prior / f"player_stats_date_completeness_{prior}.csv"
    if completeness.exists():
        try:
            grading = grade_date(prior, ledger_path)
        except Exception as exc:
            grading = {"status": "FAIL_CLOSED_GRADING_PENDING", "date": prior, "reason": f"{type(exc).__name__}:{exc}"}
    else:
        grading = {"status": "PENDING_CANONICAL_COMPLETENESS", "date": prior}
    progress = build_report(ledger_path)
    validation = validate(ledger_path, PACKAGE / "sha256_manifest.json")
    result = {
        "status": "PASS" if validation["status"] == "PASS" else "FAIL_CLOSED_VALIDATION",
        "slate_date": slate_date,
        "run_tag": run_tag,
        "capture_timestamp_utc": capture_time.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "scoring": scoring,
        "market_attachment": markets,
        "prior_date_grading": grading,
        "progress": {"decision_category": progress["decision_category"], "counts": progress["counts"], "evidence_horizon": progress["evidence_horizon"]},
        "validation": validation,
        "public_or_wagering_output_written": False,
    }
    out = DEFAULT_SUMMARY_ROOT / slate_date / f"daily_integrity_{run_tag}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    progress_path = DEFAULT_SUMMARY_ROOT / "progress_latest.json"
    progress_path.parent.mkdir(parents=True, exist_ok=True)
    progress_path.write_text(json.dumps(progress, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return {"daily_integrity_summary": rel(out), **result}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    parser.add_argument("--run-tag", required=True)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--capture-timestamp", default="")
    args = parser.parse_args()
    result = run_daily(args.date, args.run_tag, args.ledger, parse_dt(args.capture_timestamp) or now_utc())
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0 if result.get("status") in {"PASS", "FULL_BOARD_SHADOW_NOT_STARTED"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
