#!/usr/bin/env python3
"""Validate that MLB rolling features are populated and moving forward daily."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta
from typing import Any, Dict, Sequence

from backend.shared.db.pg import pg_fetchone


_ROLLING_SOURCE_BY_PROP = {
    "hits": "d7_hits", "total_bases": "d7_total_bases",
    "strikeouts_batting": "d7_strikeouts_batting", "earned_runs": "d7_earned_runs",
    "doubles": "d7_doubles", "triples": "d7_triples", "singles": "d7_singles",
    "stolen_bases": "d7_stolen_bases", "home_runs": "d7_home_runs",
    "hits_allowed": "d7_hits_allowed", "strikeouts_pitching": "d7_strikeouts_pitching",
    "outs_recorded": "d7_outs_recorded", "walks": "d7_walks",
    "hits_runs_rbis": "d7_hits_runs_rbis", "runs_scored": "d7_runs_scored",
    "walks_allowed": "d7_walks_allowed", "runs_rbis": "d7_runs_rbis", "rbis": "d7_rbis",
}


def _mtp_rolling_source_ctes() -> str:
    prop_case = "CASE mt.prop_type\n" + "\n".join(
        f"  WHEN '{prop}' THEN pds.{column}"
        for prop, column in sorted(_ROLLING_SOURCE_BY_PROP.items())
    ) + "\n  ELSE NULL::numeric\nEND"
    pds_columns = ", ".join(
        f"MAX({column}) AS {column}"
        for column in sorted(set(_ROLLING_SOURCE_BY_PROP.values()))
    )
    return f"""
WITH pds_daily AS (
  SELECT player_id, game_date::date AS game_date, COUNT(*)::int AS pds_rows,
         {pds_columns}
  FROM mlb.player_derived_stats
  WHERE game_date >= %s::date AND game_date <= %s::date
  GROUP BY player_id, game_date::date
), player_game_days AS (
  SELECT player_id, game_date::date AS game_date,
         COUNT(DISTINCT game_id)::int AS game_count
  FROM mlb.player_stats
  WHERE game_date >= %s::date AND game_date <= %s::date
  GROUP BY player_id, game_date::date
), mtp_source AS (
  SELECT mt.id, mt.player_id, mt.prop_type, mt.game_date::date AS game_date,
         mt.game_id, mt.rolling_result_avg_7,
         pds.player_id AS pds_player_id, COALESCE(pds.pds_rows, 0) AS pds_rows,
         COALESCE(pgd.game_count, 0) AS game_count,
         {prop_case} AS source_value
  FROM mlb.model_training_props mt
  LEFT JOIN pds_daily pds
    ON pds.player_id = mt.player_id AND pds.game_date = mt.game_date::date
  LEFT JOIN player_game_days pgd
    ON pgd.player_id = mt.player_id AND pgd.game_date = mt.game_date::date
  WHERE mt.prop_source = 'mlb_api'
    AND mt.game_date >= %s::date AND mt.game_date <= %s::date
), classified_mtp AS (
  SELECT *, CASE
    WHEN prop_type NOT IN ({", ".join(repr(prop) for prop in sorted(_ROLLING_SOURCE_BY_PROP))}) THEN 'UNMAPPED_PROP_TYPE'
    WHEN pds_player_id IS NULL THEN 'NO_PLAYER_DAILY_FEATURE_ROW'
    WHEN pds_rows <> 1 THEN 'AMBIGUOUS_PLAYER_DAILY_FEATURE_ROWS'
    WHEN game_count = 0 THEN 'NO_EXACT_PLAYER_GAME_DAY'
    WHEN game_count > 1 THEN 'AMBIGUOUS_EXACT_GAME_DAY'
    WHEN source_value IS NULL THEN 'PROP_SPECIFIC_D7_VALUE_UNAVAILABLE'
    ELSE 'FEATURE_REQUIRED'
  END AS feature_class
  FROM mtp_source
)
"""


def _classify_mtp_feature_requirement(
    prop_type: str, *, pds_rows: int, player_game_count: int, source_value: Any
) -> str:
    """Classify source-backed rolling coverage without treating unknowns as required."""
    if prop_type not in _ROLLING_SOURCE_BY_PROP:
        return "UNMAPPED_PROP_TYPE"
    if pds_rows == 0:
        return "NO_PLAYER_DAILY_FEATURE_ROW"
    if pds_rows != 1:
        return "AMBIGUOUS_PLAYER_DAILY_FEATURE_ROWS"
    if player_game_count == 0:
        return "NO_EXACT_PLAYER_GAME_DAY"
    if player_game_count > 1:
        return "AMBIGUOUS_EXACT_GAME_DAY"
    if source_value is None:
        return "PROP_SPECIFIC_D7_VALUE_UNAVAILABLE"
    return "FEATURE_REQUIRED"


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
        _mtp_rolling_source_ctes() + """
SELECT
  COUNT(*)::int AS rows_total,
  COUNT(*) FILTER (WHERE feature_class = 'FEATURE_REQUIRED')::int AS required_rows,
  COUNT(*) FILTER (WHERE feature_class = 'FEATURE_REQUIRED' AND rolling_result_avg_7 IS NOT NULL)::int AS d7_nonnull,
  COUNT(*) FILTER (WHERE feature_class = 'FEATURE_REQUIRED' AND rolling_result_avg_7 IS NULL)::int AS required_null,
  COUNT(*) FILTER (WHERE feature_class <> 'FEATURE_REQUIRED')::int AS not_required_rows,
  COUNT(*) FILTER (WHERE feature_class = 'NO_PLAYER_DAILY_FEATURE_ROW')::int AS no_player_daily_feature_row,
  COUNT(*) FILTER (WHERE feature_class = 'AMBIGUOUS_PLAYER_DAILY_FEATURE_ROWS')::int AS ambiguous_daily_feature_rows,
  COUNT(*) FILTER (WHERE feature_class = 'NO_EXACT_PLAYER_GAME_DAY')::int AS no_exact_player_game_day,
  COUNT(*) FILTER (WHERE feature_class = 'AMBIGUOUS_EXACT_GAME_DAY')::int AS ambiguous_exact_game_day,
  COUNT(*) FILTER (WHERE feature_class = 'PROP_SPECIFIC_D7_VALUE_UNAVAILABLE')::int AS prop_specific_d7_unavailable,
  COUNT(*) FILTER (WHERE feature_class = 'UNMAPPED_PROP_TYPE')::int AS unmapped_prop_type
FROM classified_mtp
""",
        (from_date, to_date, from_date, to_date, from_date, to_date),
    ) or {}
    rows_total = _to_int(row, "rows_total")
    required_rows = _to_int(row, "required_rows")
    d7_nonnull = _to_int(row, "d7_nonnull")
    required_null = _to_int(row, "required_null")
    exclusions = {
        "no_player_daily_feature_row": _to_int(row, "no_player_daily_feature_row"),
        "ambiguous_daily_feature_rows": _to_int(row, "ambiguous_daily_feature_rows"),
        "no_exact_player_game_day": _to_int(row, "no_exact_player_game_day"),
        "ambiguous_exact_game_day": _to_int(row, "ambiguous_exact_game_day"),
        "prop_specific_d7_unavailable": _to_int(row, "prop_specific_d7_unavailable"),
        "unmapped_prop_type": _to_int(row, "unmapped_prop_type"),
    }
    not_required_rows = _to_int(row, "not_required_rows")
    if required_rows + not_required_rows != rows_total:
        raise RuntimeError("ROLLING_COVERAGE_CLASSIFICATION_COUNT_MISMATCH")
    if sum(exclusions.values()) != not_required_rows:
        raise RuntimeError("ROLLING_COVERAGE_EXCLUSION_COUNT_MISMATCH")
    if d7_nonnull + required_null != required_rows:
        raise RuntimeError("ROLLING_COVERAGE_REQUIRED_ROW_COUNT_MISMATCH")
    return {
        "rows_total": rows_total,
        "required_rows": required_rows,
        "d7_nonnull": d7_nonnull,
        "required_null": required_null,
        "not_required_rows": not_required_rows,
        "not_required_by_reason": exclusions,
        "d7_pct": _pct(d7_nonnull, required_rows),
    }


def _fetch_mtp_movement(from_date: str, to_date: str) -> Dict[str, Any]:
    row = pg_fetchone(
        _mtp_rolling_source_ctes() + """, seq AS (
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
  FROM classified_mtp
  WHERE feature_class = 'FEATURE_REQUIRED'
)
SELECT
  COUNT(*) FILTER (WHERE prev_val IS NOT NULL)::int AS comparable_rows,
  COUNT(*) FILTER (
    WHERE prev_val IS NOT NULL
      AND rolling_result_avg_7 IS DISTINCT FROM prev_val
  )::int AS changed_rows
FROM seq
""",
        (from_date, to_date, from_date, to_date, from_date, to_date),
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
    if mtp_cov["not_required_by_reason"]["ambiguous_daily_feature_rows"]:
        failures.append("model_training_props:ambiguous_player_daily_feature_source")
    if mtp_cov["not_required_by_reason"]["no_exact_player_game_day"]:
        failures.append("model_training_props:missing_exact_player_game_day")
    if mtp_cov["not_required_by_reason"]["unmapped_prop_type"]:
        failures.append("model_training_props:unmapped_rolling_prop_type")

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
            f"mtp_rows={mtp_cov['rows_total']} mtp_required={mtp_cov['required_rows']} "
            f"mtp_cov_d7={mtp_cov['d7_pct']:.2f}% mtp_required_null={mtp_cov['required_null']} "
            f"mtp_not_required={mtp_cov['not_required_rows']} "
            f"mtp_excluded(no_pds/ambig_pds/no_game/multi_game/no_prop_d7/unmapped)="
            f"{mtp_cov['not_required_by_reason']['no_player_daily_feature_row']}/"
            f"{mtp_cov['not_required_by_reason']['ambiguous_daily_feature_rows']}/"
            f"{mtp_cov['not_required_by_reason']['no_exact_player_game_day']}/"
            f"{mtp_cov['not_required_by_reason']['ambiguous_exact_game_day']}/"
            f"{mtp_cov['not_required_by_reason']['prop_specific_d7_value_unavailable']}/"
            f"{mtp_cov['not_required_by_reason']['unmapped_prop_type']} "
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
