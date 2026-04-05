#!/usr/bin/env python3
"""Validate that MLB rolling features are populated and moving forward daily."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta
from typing import Any, Dict, Sequence

from backend.shared.db.pg import pg_fetchone


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def _resolve_bounds(
    *,
    days: int,
    from_date: str | None,
    to_date: str | None,
) -> tuple[str, str]:
    parsed_from = _parse_date(from_date)
    parsed_to = _parse_date(to_date)
    today = date.today()
    max_days = max(1, int(days))

    if parsed_to is None:
        parsed_to = today
    if parsed_from is None:
        parsed_from = parsed_to - timedelta(days=max_days - 1)
    if parsed_from > parsed_to:
        raise ValueError(f"from-date {parsed_from.isoformat()} is after to-date {parsed_to.isoformat()}")
    return parsed_from.isoformat(), parsed_to.isoformat()


def _to_int(row: Dict[str, Any], key: str) -> int:
    return int(row.get(key) or 0)


def _pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((100.0 * numerator) / float(denominator), 2)


def _fetch_pds_coverage(from_date: str, to_date: str) -> Dict[str, Any]:
    row = pg_fetchone(
        """
SELECT
  COUNT(*)::int AS rows_total,
  COUNT(*) FILTER (WHERE d7_hits IS NOT NULL)::int AS d7_nonnull,
  COUNT(*) FILTER (WHERE d15_hits IS NOT NULL)::int AS d15_nonnull,
  COUNT(*) FILTER (WHERE d30_hits IS NOT NULL)::int AS d30_nonnull
FROM mlb.player_derived_stats
WHERE game_date >= %s::date
  AND game_date <= %s::date
""",
        (from_date, to_date),
    ) or {}
    rows_total = _to_int(row, "rows_total")
    d7_nonnull = _to_int(row, "d7_nonnull")
    d15_nonnull = _to_int(row, "d15_nonnull")
    d30_nonnull = _to_int(row, "d30_nonnull")
    return {
        "rows_total": rows_total,
        "d7_nonnull": d7_nonnull,
        "d15_nonnull": d15_nonnull,
        "d30_nonnull": d30_nonnull,
        "d7_pct": _pct(d7_nonnull, rows_total),
        "d15_pct": _pct(d15_nonnull, rows_total),
        "d30_pct": _pct(d30_nonnull, rows_total),
    }


def _fetch_pds_movement(from_date: str, to_date: str) -> Dict[str, Any]:
    row = pg_fetchone(
        """
WITH seq AS (
  SELECT
    player_id,
    game_date::date AS game_date,
    game_id,
    d7_hits,
    d15_hits,
    d30_hits,
    LAG(d7_hits) OVER (PARTITION BY player_id ORDER BY game_date::date, game_id) AS prev_d7,
    LAG(d15_hits) OVER (PARTITION BY player_id ORDER BY game_date::date, game_id) AS prev_d15,
    LAG(d30_hits) OVER (PARTITION BY player_id ORDER BY game_date::date, game_id) AS prev_d30
  FROM mlb.player_derived_stats
  WHERE game_date >= %s::date
    AND game_date <= %s::date
)
SELECT
  COUNT(*) FILTER (WHERE prev_d7 IS NOT NULL)::int AS comparable_rows,
  COUNT(*) FILTER (WHERE prev_d7 IS NOT NULL AND d7_hits IS DISTINCT FROM prev_d7)::int AS changed_d7,
  COUNT(*) FILTER (WHERE prev_d15 IS NOT NULL AND d15_hits IS DISTINCT FROM prev_d15)::int AS changed_d15,
  COUNT(*) FILTER (WHERE prev_d30 IS NOT NULL AND d30_hits IS DISTINCT FROM prev_d30)::int AS changed_d30
FROM seq
""",
        (from_date, to_date),
    ) or {}
    comparable_rows = _to_int(row, "comparable_rows")
    changed_d7 = _to_int(row, "changed_d7")
    changed_d15 = _to_int(row, "changed_d15")
    changed_d30 = _to_int(row, "changed_d30")
    return {
        "comparable_rows": comparable_rows,
        "changed_d7": changed_d7,
        "changed_d15": changed_d15,
        "changed_d30": changed_d30,
        "changed_d7_pct": _pct(changed_d7, comparable_rows),
        "changed_d15_pct": _pct(changed_d15, comparable_rows),
        "changed_d30_pct": _pct(changed_d30, comparable_rows),
    }


def _fetch_mtp_coverage(from_date: str, to_date: str) -> Dict[str, Any]:
    row = pg_fetchone(
        """
SELECT
  COUNT(*)::int AS rows_total,
  COUNT(*) FILTER (WHERE rolling_result_avg_7 IS NOT NULL)::int AS d7_nonnull
FROM mlb.model_training_props
WHERE prop_source = 'mlb_api'
  AND game_date >= %s::date
  AND game_date <= %s::date
""",
        (from_date, to_date),
    ) or {}
    rows_total = _to_int(row, "rows_total")
    d7_nonnull = _to_int(row, "d7_nonnull")
    return {
        "rows_total": rows_total,
        "d7_nonnull": d7_nonnull,
        "d7_pct": _pct(d7_nonnull, rows_total),
    }


def _fetch_mtp_movement(from_date: str, to_date: str) -> Dict[str, Any]:
    row = pg_fetchone(
        """
WITH seq AS (
  SELECT
    player_id,
    prop_type,
    game_date::date AS game_date,
    game_id,
    rolling_result_avg_7,
    LAG(rolling_result_avg_7) OVER (
      PARTITION BY player_id, prop_type
      ORDER BY game_date::date, game_id
    ) AS prev_val
  FROM mlb.model_training_props
  WHERE prop_source = 'mlb_api'
    AND game_date >= %s::date
    AND game_date <= %s::date
)
SELECT
  COUNT(*) FILTER (WHERE prev_val IS NOT NULL)::int AS comparable_rows,
  COUNT(*) FILTER (
    WHERE prev_val IS NOT NULL
      AND rolling_result_avg_7 IS DISTINCT FROM prev_val
  )::int AS changed_rows
FROM seq
""",
        (from_date, to_date),
    ) or {}
    comparable_rows = _to_int(row, "comparable_rows")
    changed_rows = _to_int(row, "changed_rows")
    return {
        "comparable_rows": comparable_rows,
        "changed_rows": changed_rows,
        "changed_pct": _pct(changed_rows, comparable_rows),
    }


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Check MLB rolling-feature integrity and movement.")
    ap.add_argument("--days", type=int, default=10, help="Window size when explicit dates are not supplied.")
    ap.add_argument("--from-date", default=None, help="Optional lower bound YYYY-MM-DD.")
    ap.add_argument("--to-date", default=None, help="Optional upper bound YYYY-MM-DD.")
    ap.add_argument(
        "--min-coverage-pct",
        type=float,
        default=99.0,
        help="Minimum acceptable non-null coverage percentage.",
    )
    ap.add_argument(
        "--min-comparable",
        type=int,
        default=100,
        help="Minimum comparable sequential rows required for movement checks.",
    )
    ap.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    args = ap.parse_args(list(argv) if argv is not None else None)

    from_date, to_date = _resolve_bounds(
        days=max(1, int(args.days)),
        from_date=args.from_date,
        to_date=args.to_date,
    )
    min_cov = max(0.0, float(args.min_coverage_pct))
    min_comp = max(1, int(args.min_comparable))

    pds_cov = _fetch_pds_coverage(from_date, to_date)
    pds_move = _fetch_pds_movement(from_date, to_date)
    mtp_cov = _fetch_mtp_coverage(from_date, to_date)
    mtp_move = _fetch_mtp_movement(from_date, to_date)

    failures: list[str] = []
    if int(pds_cov["rows_total"]) <= 0:
        failures.append("player_derived_stats:no_rows")
    if float(pds_cov["d7_pct"]) < min_cov:
        failures.append(f"player_derived_stats:d7_hits_coverage<{min_cov}")
    if float(pds_cov["d15_pct"]) < min_cov:
        failures.append(f"player_derived_stats:d15_hits_coverage<{min_cov}")
    if float(pds_cov["d30_pct"]) < min_cov:
        failures.append(f"player_derived_stats:d30_hits_coverage<{min_cov}")

    if int(mtp_cov["rows_total"]) <= 0:
        failures.append("model_training_props:no_rows")
    if float(mtp_cov["d7_pct"]) < min_cov:
        failures.append(f"model_training_props:rolling_result_avg_7_coverage<{min_cov}")

    if int(pds_move["comparable_rows"]) < min_comp:
        failures.append(f"player_derived_stats:comparable_rows<{min_comp}")
    else:
        if int(pds_move["changed_d7"]) <= 0:
            failures.append("player_derived_stats:d7_not_changing")
        if int(pds_move["changed_d15"]) <= 0:
            failures.append("player_derived_stats:d15_not_changing")
        if int(pds_move["changed_d30"]) <= 0:
            failures.append("player_derived_stats:d30_not_changing")

    if int(mtp_move["comparable_rows"]) < min_comp:
        failures.append(f"model_training_props:comparable_rows<{min_comp}")
    elif int(mtp_move["changed_rows"]) <= 0:
        failures.append("model_training_props:rolling_result_avg_7_not_changing")

    ok = len(failures) == 0
    payload = {
        "status": "pass" if ok else "fail",
        "ok": ok,
        "window": {"from_date": from_date, "to_date": to_date},
        "thresholds": {"min_coverage_pct": min_cov, "min_comparable": min_comp},
        "player_derived_stats": {
            "coverage": pds_cov,
            "movement": pds_move,
        },
        "model_training_props": {
            "coverage": mtp_cov,
            "movement": mtp_move,
        },
        "failures": failures,
    }

    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print(
            f"MLB rolling integrity window={from_date}..{to_date} "
            f"pds_rows={pds_cov['rows_total']} "
            f"pds_cov(d7/d15/d30)={pds_cov['d7_pct']:.2f}/{pds_cov['d15_pct']:.2f}/{pds_cov['d30_pct']:.2f}% "
            f"mtp_rows={mtp_cov['rows_total']} mtp_cov_d7={mtp_cov['d7_pct']:.2f}% "
            f"pds_changed(d7/d15/d30)={pds_move['changed_d7']}/{pds_move['changed_d15']}/{pds_move['changed_d30']} "
            f"mtp_changed_d7={mtp_move['changed_rows']}"
        )
        if ok:
            print(f"PASS mlb rolling integrity window={from_date}..{to_date}")
        else:
            print(f"FAIL mlb rolling integrity window={from_date}..{to_date} failures={';'.join(failures)}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
