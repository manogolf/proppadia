#!/usr/bin/env python3
"""Analyze MLB prediction quality from historical MLB prop rows."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict, Sequence, Tuple

from backend.shared.db.pg import pg_fetchall

_ALLOWED_SOURCE_TABLES = {"player_props", "model_training_props"}
_DEFAULT_SOURCE_TABLE = "model_training_props"


def _normalize_source_table(source_table: str) -> str:
    table = str(source_table or "").strip().lower()
    if table not in _ALLOWED_SOURCE_TABLES:
        raise ValueError(f"source_table must be one of: {sorted(_ALLOWED_SOURCE_TABLES)}")
    return table


def _common_cte(source_table: str, prop_types: Sequence[str] | None = None) -> Tuple[str, Tuple[Any, ...]]:
    prop_types = [str(p).strip() for p in (prop_types or []) if str(p).strip()]
    filter_sql = ""
    params: Tuple[Any, ...] = ()
    if prop_types:
        placeholders = ", ".join(["%s"] * len(prop_types))
        filter_sql = f" AND prop_type IN ({placeholders})"
        params = tuple(prop_types)
    sql = (
        """
WITH src AS (
  SELECT
    prop_type,
    game_date::date AS game_day,
    lower(trim(over_under)) AS over_under_norm,
    lower(trim(outcome)) AS outcome_norm,
    lower(trim(predicted_outcome)) AS predicted_norm,
    was_correct,
    confidence_score
  FROM """
        + _normalize_source_table(source_table)
        + """
  WHERE game_date IS NOT NULL
"""
        + filter_sql
        + """
),
norm AS (
  SELECT
    prop_type,
    game_day,
    outcome_norm,
    confidence_score,
    CASE
      WHEN outcome_norm = 'win' THEN over_under_norm
      WHEN outcome_norm = 'loss' THEN CASE WHEN over_under_norm = 'over' THEN 'under' WHEN over_under_norm = 'under' THEN 'over' END
      ELSE NULL
    END AS actual_side,
    CASE
      WHEN predicted_norm IN ('over', 'under') THEN predicted_norm
      WHEN predicted_norm = 'win' THEN over_under_norm
      WHEN predicted_norm = 'loss' THEN CASE WHEN over_under_norm = 'over' THEN 'under' WHEN over_under_norm = 'under' THEN 'over' END
      ELSE NULL
    END AS predicted_side,
    CASE
      WHEN was_correct IS TRUE THEN 1
      WHEN was_correct IS FALSE THEN 0
      ELSE NULL
    END AS was_correct_i
  FROM src
),
labeled AS (
  SELECT
    prop_type,
    game_day,
    confidence_score,
    CASE
      WHEN confidence_score IS NULL THEN 'unknown'
      WHEN abs(confidence_score - 0.5) < 0.05 THEN 'low'
      WHEN abs(confidence_score - 0.5) < 0.10 THEN 'medium'
      ELSE 'high'
    END AS confidence_bucket,
    CASE
      WHEN outcome_norm IN ('win','loss') AND (predicted_side IS NOT NULL OR was_correct_i IS NOT NULL)
      THEN COALESCE(was_correct_i, CASE WHEN predicted_side = actual_side THEN 1 ELSE 0 END, 0)
      ELSE NULL
    END AS model_correct_i
  FROM norm
)
"""
    )
    return sql, params


def _window_clause(window_mode: str) -> str:
    if window_mode == "games":
        return """
WHERE game_day IN (
  SELECT DISTINCT game_day
  FROM labeled
  WHERE model_correct_i IS NOT NULL
  ORDER BY game_day DESC
  LIMIT %s::int
)
"""
    return "WHERE game_day >= (CURRENT_DATE - (%s::int || ' days')::interval)::date"


def _overall(window_value: int, window_mode: str, source_table: str, prop_types: Sequence[str]) -> Dict[str, Any]:
    common_cte, cte_params = _common_cte(source_table, prop_types)
    rows = pg_fetchall(
        common_cte
        + """
SELECT
  COUNT(*) FILTER (WHERE model_correct_i IS NOT NULL)::int AS total,
  COALESCE(SUM(model_correct_i), 0)::int AS correct
FROM labeled
"""
        + _window_clause(window_mode),
        cte_params + (int(window_value),),
    )
    row = (rows or [{}])[0]
    total = int(row.get("total") or 0)
    correct = int(row.get("correct") or 0)
    return {
        "window_mode": window_mode,
        "window_value": int(window_value),
        "total": total,
        "correct": correct,
        "accuracy_pct": round((100.0 * correct / total), 2) if total > 0 else None,
    }


def _by_prop(window_value: int, window_mode: str, source_table: str, prop_types: Sequence[str]) -> list[Dict[str, Any]]:
    common_cte, cte_params = _common_cte(source_table, prop_types)
    rows = pg_fetchall(
        common_cte
        + """
SELECT
  prop_type,
  COUNT(*) FILTER (WHERE model_correct_i IS NOT NULL)::int AS total,
  COALESCE(SUM(model_correct_i), 0)::int AS correct
FROM labeled
"""
        + _window_clause(window_mode)
        + """
GROUP BY prop_type
ORDER BY total DESC, prop_type
""",
        cte_params + (int(window_value),),
    )
    out = []
    for row in rows:
        total = int(row.get("total") or 0)
        correct = int(row.get("correct") or 0)
        out.append(
            {
                "prop_type": row.get("prop_type"),
                "total": total,
                "correct": correct,
                "accuracy_pct": round((100.0 * correct / total), 2) if total > 0 else None,
            }
        )
    return out


def _by_confidence_bucket(
    window_value: int, window_mode: str, source_table: str, prop_types: Sequence[str]
) -> list[Dict[str, Any]]:
    common_cte, cte_params = _common_cte(source_table, prop_types)
    rows = pg_fetchall(
        common_cte
        + """
SELECT
  confidence_bucket,
  COUNT(*) FILTER (WHERE model_correct_i IS NOT NULL)::int AS total,
  COALESCE(SUM(model_correct_i), 0)::int AS correct
FROM labeled
"""
        + _window_clause(window_mode)
        + """
GROUP BY confidence_bucket
ORDER BY
  CASE confidence_bucket
    WHEN 'high' THEN 1
    WHEN 'medium' THEN 2
    WHEN 'low' THEN 3
    ELSE 4
  END
""",
        cte_params + (int(window_value),),
    )
    out = []
    for row in rows:
        total = int(row.get("total") or 0)
        correct = int(row.get("correct") or 0)
        out.append(
            {
                "confidence_bucket": row.get("confidence_bucket"),
                "total": total,
                "correct": correct,
                "accuracy_pct": round((100.0 * correct / total), 2) if total > 0 else None,
            }
        )
    return out


def _drift(source_table: str, prop_types: Sequence[str]) -> Dict[str, Any]:
    common_cte, cte_params = _common_cte(source_table, prop_types)
    rows = pg_fetchall(
        common_cte
        + """
SELECT
  CASE
    WHEN game_day >= (CURRENT_DATE - interval '14 days')::date THEN 'last_14d'
    WHEN game_day >= (CURRENT_DATE - interval '28 days')::date THEN 'prev_14d'
    ELSE 'other'
  END AS bucket,
  COUNT(*) FILTER (WHERE model_correct_i IS NOT NULL)::int AS total,
  COALESCE(SUM(model_correct_i), 0)::int AS correct
FROM labeled
WHERE game_day >= (CURRENT_DATE - interval '28 days')::date
GROUP BY 1
""",
        cte_params,
    )
    by_bucket: Dict[str, Dict[str, Any]] = {
        "last_14d": {"total": 0, "correct": 0, "accuracy_pct": None},
        "prev_14d": {"total": 0, "correct": 0, "accuracy_pct": None},
    }
    for row in rows:
        bucket = row.get("bucket")
        if bucket not in by_bucket:
            continue
        total = int(row.get("total") or 0)
        correct = int(row.get("correct") or 0)
        by_bucket[bucket] = {
            "total": total,
            "correct": correct,
            "accuracy_pct": round((100.0 * correct / total), 2) if total > 0 else None,
        }
    last_acc = by_bucket["last_14d"]["accuracy_pct"]
    prev_acc = by_bucket["prev_14d"]["accuracy_pct"]
    delta = None
    if last_acc is not None and prev_acc is not None:
        delta = round(float(last_acc) - float(prev_acc), 2)
    return {"last_14d": by_bucket["last_14d"], "prev_14d": by_bucket["prev_14d"], "delta_pct": delta}


def collect_quality(
    window_mode: str,
    window_value: int,
    prop_types: Sequence[str] | None = None,
    source_table: str = _DEFAULT_SOURCE_TABLE,
) -> Dict[str, Any]:
    normalized_mode = "games" if str(window_mode).lower() == "games" else "days"
    normalized_source = _normalize_source_table(source_table)
    window_value = max(1, int(window_value))
    filtered_prop_types = [str(p).strip() for p in (prop_types or []) if str(p).strip()]
    overall = _overall(window_value, normalized_mode, normalized_source, filtered_prop_types)
    by_prop = _by_prop(window_value, normalized_mode, normalized_source, filtered_prop_types)
    by_bucket = _by_confidence_bucket(window_value, normalized_mode, normalized_source, filtered_prop_types)
    drift = _drift(normalized_source, filtered_prop_types)
    return {
        "source_table": normalized_source,
        "window_mode": normalized_mode,
        "window_value": window_value,
        "prop_types": filtered_prop_types,
        "overall": overall,
        "by_prop": by_prop,
        "by_confidence_bucket": by_bucket,
        "drift_14d": drift,
    }


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Analyze MLB prediction quality (JSON).")
    ap.add_argument("--window-mode", choices=["days", "games"], default="days")
    ap.add_argument("--window-days", type=int, default=120)
    ap.add_argument("--games-back", type=int, default=30)
    ap.add_argument("--min-total", type=int, default=1, help="Fail when overall total is below this threshold.")
    ap.add_argument("--prop-types", default="", help="Optional comma-separated prop types to filter quality scope.")
    ap.add_argument(
        "--source-table",
        choices=sorted(_ALLOWED_SOURCE_TABLES),
        default=_DEFAULT_SOURCE_TABLE,
        help="Source table for quality metrics (default: model_training_props).",
    )
    args = ap.parse_args(list(argv) if argv is not None else sys.argv[1:])

    window_value = int(args.games_back) if args.window_mode == "games" else int(args.window_days)
    prop_types = [p.strip() for p in str(args.prop_types).split(",") if p.strip()]
    quality = collect_quality(args.window_mode, window_value, prop_types=prop_types, source_table=args.source_table)
    overall = quality["overall"]

    min_total = max(0, int(args.min_total))
    ok = int(overall.get("total") or 0) >= min_total
    payload = {
        "ok": ok,
        "status": "pass" if ok else "fail",
        "overall": quality["overall"],
        "by_prop": quality["by_prop"],
        "by_confidence_bucket": quality["by_confidence_bucket"],
        "drift_14d": quality["drift_14d"],
        "min_total": min_total,
    }
    print(json.dumps(payload, indent=2, default=str))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
