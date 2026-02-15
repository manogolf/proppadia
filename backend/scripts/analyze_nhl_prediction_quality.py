#!/usr/bin/env python3
"""Analyze NHL prediction quality from graded nhl_* rows in player_props."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from typing import Any, Dict, Sequence

from backend.shared.db.pg import pg_fetchall


COMMON_CTE = """
WITH src AS (
  SELECT
    prop_type,
    prop_source,
    game_date::date AS game_day,
    lower(trim(over_under)) AS over_under_norm,
    lower(trim(outcome)) AS outcome_norm,
    lower(trim(predicted_outcome)) AS predicted_norm,
    was_correct,
    confidence_score
  FROM player_props
  WHERE prop_source LIKE 'nhl_%'
    AND game_date IS NOT NULL
    AND game_date::date BETWEEN %s::date AND %s::date
),
norm AS (
  SELECT
    prop_type,
    prop_source,
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
    prop_source,
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


def _overall(from_date: str, to_date: str) -> Dict[str, Any]:
    rows = pg_fetchall(
        COMMON_CTE
        + """
SELECT
  COUNT(*) FILTER (WHERE model_correct_i IS NOT NULL)::int AS total,
  COALESCE(SUM(model_correct_i), 0)::int AS correct
FROM labeled
""",
        (from_date, to_date),
    )
    row = (rows or [{}])[0]
    total = int(row.get("total") or 0)
    correct = int(row.get("correct") or 0)
    return {
        "from_date": from_date,
        "to_date": to_date,
        "total": total,
        "correct": correct,
        "accuracy_pct": round((100.0 * correct / total), 2) if total > 0 else None,
    }


def _by_prop(from_date: str, to_date: str) -> list[Dict[str, Any]]:
    rows = pg_fetchall(
        COMMON_CTE
        + """
SELECT
  prop_type,
  COUNT(*) FILTER (WHERE model_correct_i IS NOT NULL)::int AS total,
  COALESCE(SUM(model_correct_i), 0)::int AS correct
FROM labeled
GROUP BY prop_type
ORDER BY total DESC, prop_type
""",
        (from_date, to_date),
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


def _by_source(from_date: str, to_date: str) -> list[Dict[str, Any]]:
    rows = pg_fetchall(
        COMMON_CTE
        + """
SELECT
  prop_source,
  COUNT(*) FILTER (WHERE model_correct_i IS NOT NULL)::int AS total,
  COALESCE(SUM(model_correct_i), 0)::int AS correct
FROM labeled
GROUP BY prop_source
ORDER BY total DESC, prop_source
""",
        (from_date, to_date),
    )
    out = []
    for row in rows:
        total = int(row.get("total") or 0)
        correct = int(row.get("correct") or 0)
        out.append(
            {
                "prop_source": row.get("prop_source"),
                "total": total,
                "correct": correct,
                "accuracy_pct": round((100.0 * correct / total), 2) if total > 0 else None,
            }
        )
    return out


def _validate_iso(raw: str, label: str) -> str:
    value = str(raw or "").strip()
    try:
        date.fromisoformat(value)
    except Exception as e:
        raise ValueError(f"{label} must be YYYY-MM-DD") from e
    return value


def collect_quality(from_date: str, to_date: str) -> Dict[str, Any]:
    overall = _overall(from_date, to_date)
    by_prop = _by_prop(from_date, to_date)
    by_source = _by_source(from_date, to_date)
    return {
        "overall": overall,
        "by_prop": by_prop,
        "by_source": by_source,
        "caveats": [
            "Includes only rows with prop_source like nhl_%.",
            "Accuracy is evaluated only on graded outcomes (win/loss).",
            "Sparse/offseason windows can be valid but may fail min_total guard.",
        ],
    }


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Analyze NHL prediction quality (JSON).")
    ap.add_argument("--from-date", required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--to-date", required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--min-total", type=int, default=1, help="Fail when graded total is below this threshold.")
    args = ap.parse_args(list(argv) if argv is not None else sys.argv[1:])

    try:
        from_date = _validate_iso(args.from_date, "from-date")
        to_date = _validate_iso(args.to_date, "to-date")
    except ValueError as e:
        print(json.dumps({"ok": False, "status": "fail", "error": str(e)}, indent=2))
        return 2
    if from_date > to_date:
        print(json.dumps({"ok": False, "status": "fail", "error": "from-date must be <= to-date"}, indent=2))
        return 2

    quality = collect_quality(from_date, to_date)
    overall = quality["overall"]
    min_total = max(0, int(args.min_total))
    ok = int(overall.get("total") or 0) >= min_total
    payload = {
        "ok": ok,
        "status": "pass" if ok else "fail",
        "overall": quality["overall"],
        "by_prop": quality["by_prop"],
        "by_source": quality["by_source"],
        "caveats": quality["caveats"],
        "min_total": min_total,
    }
    print(json.dumps(payload, indent=2, default=str))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
