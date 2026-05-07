#!/usr/bin/env python3
"""Backfill MLB V1 candidate result CSVs across a date range.

Uses only outcome-backed full-slate reconcile artifacts:
  artifacts/analysis/mlb/execution_vs_model/<date>/reconcile_rows.csv

CSV only. No database reads/writes.
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Optional, Sequence

import numpy as np
import pandas as pd

from backend.mlb.scripts.reconcile_mlb_v1_results import _summary, reconcile


SUMMARY_COLUMNS = [
    "date",
    "candidate_exists",
    "reconcile_exists",
    "bets",
    "wins",
    "losses",
    "profit",
    "roi",
    "status",
]


def _date_range(start: date, end: date) -> list[date]:
    if end < start:
        raise SystemExit(f"--to-date {end.isoformat()} is before --from-date {start.isoformat()}")
    out: list[date] = []
    cur = start
    while cur <= end:
        out.append(cur)
        cur += timedelta(days=1)
    return out


def _parse_date(value: str, flag: str) -> date:
    try:
        return date.fromisoformat(str(value).strip())
    except Exception as exc:
        raise SystemExit(f"{flag} must be YYYY-MM-DD, got {value!r}") from exc


def _empty_row(day: str, *, candidate_exists: bool, reconcile_exists: bool, status: str) -> dict[str, Any]:
    return {
        "date": day,
        "candidate_exists": bool(candidate_exists),
        "reconcile_exists": bool(reconcile_exists),
        "bets": 0,
        "wins": 0,
        "losses": 0,
        "profit": 0.0,
        "roi": np.nan,
        "status": status,
    }


def _print_row(row: dict[str, Any]) -> None:
    roi = row.get("roi")
    roi_s = "nan" if pd.isna(roi) else f"{float(roi):.3f}"
    print(
        "[mlb-v1-results-backfill] "
        f"date={row['date']} candidate_exists={row['candidate_exists']} "
        f"reconcile_exists={row['reconcile_exists']} bets={int(row['bets'])} "
        f"wins={int(row['wins'])} losses={int(row['losses'])} "
        f"profit={float(row['profit']):.3f} roi={roi_s} status={row['status']}"
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Backfill MLB V1 result CSVs across a date range.")
    ap.add_argument("--from-date", required=True)
    ap.add_argument("--to-date", required=True)
    ap.add_argument(
        "--skip-missing",
        action="store_true",
        help="Continue and summarize dates missing candidates or outcome-backed reconcile artifacts.",
    )
    args = ap.parse_args(list(argv) if argv is not None else None)

    start = _parse_date(args.from_date, "--from-date")
    end = _parse_date(args.to_date, "--to-date")

    rows: list[dict[str, Any]] = []
    for day in _date_range(start, end):
        day_s = day.isoformat()
        candidates = Path(f"backend/mlb/exports/v1_wagers/{day_s}/wagers.csv")
        rec = Path(f"artifacts/analysis/mlb/execution_vs_model/{day_s}/reconcile_rows.csv")
        out_csv = Path(f"backend/mlb/exports/v1_results/{day_s}/results.csv")
        candidate_exists = candidates.exists()
        reconcile_exists = rec.exists()

        missing = []
        if not candidate_exists:
            missing.append("missing_candidates")
        if not reconcile_exists:
            missing.append("missing_reconcile")
        if missing:
            status = ",".join(missing)
            if not args.skip_missing:
                raise SystemExit(f"{day_s}: {status}. Use --skip-missing to continue.")
            row = _empty_row(
                day_s,
                candidate_exists=candidate_exists,
                reconcile_exists=reconcile_exists,
                status=status,
            )
            rows.append(row)
            _print_row(row)
            continue

        try:
            result_rows = reconcile(candidates, rec)
            out_csv.parent.mkdir(parents=True, exist_ok=True)
            result_rows.to_csv(out_csv, index=False)
            summary = _summary(result_rows)
            row = {
                "date": day_s,
                "candidate_exists": True,
                "reconcile_exists": True,
                "bets": int(summary["bets"]),
                "wins": int(summary["wins"]),
                "losses": int(summary["losses"]),
                "profit": float(summary["profit"]),
                "roi": summary["roi"],
                "status": "ok",
            }
        except Exception as exc:
            if not args.skip_missing:
                raise
            row = _empty_row(
                day_s,
                candidate_exists=True,
                reconcile_exists=True,
                status=f"error:{type(exc).__name__}",
            )
        rows.append(row)
        _print_row(row)

    summary_path = Path("backend/mlb/exports/v1_results/v1_results_backfill_summary.csv")
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows, columns=SUMMARY_COLUMNS).to_csv(summary_path, index=False)
    print(f"[mlb-v1-results-backfill] summary_csv={summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
