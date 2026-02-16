#!/usr/bin/env python3
"""Segmented MLB quality report: preseason vs regular-season date windows."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from typing import Any, Dict, Sequence, Tuple

from backend.shared.db.pg import pg_fetchall

_ALLOWED_SOURCE_TABLES = {"player_props", "model_training_props"}
_DEFAULT_SOURCE_TABLE = "model_training_props"


def _normalize_source_table(source_table: str) -> str:
    table = str(source_table or "").strip().lower()
    if table not in _ALLOWED_SOURCE_TABLES:
        raise ValueError(f"source_table must be one of: {sorted(_ALLOWED_SOURCE_TABLES)}")
    return table


def _validate_iso(raw: str, label: str) -> str:
    value = str(raw or "").strip()
    try:
        date.fromisoformat(value)
    except Exception as e:
        raise ValueError(f"{label} must be YYYY-MM-DD") from e
    return value


def _common_cte(
    source_table: str, from_date: str, to_date: str, prop_types: Sequence[str] | None = None
) -> Tuple[str, Tuple[Any, ...]]:
    prop_types = [str(p).strip() for p in (prop_types or []) if str(p).strip()]
    filter_sql = ""
    filter_params: Tuple[Any, ...] = ()
    if prop_types:
        placeholders = ", ".join(["%s"] * len(prop_types))
        filter_sql = f" AND prop_type IN ({placeholders})"
        filter_params = tuple(prop_types)
    sql = (
        """
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
  FROM """
        + _normalize_source_table(source_table)
        + """
  WHERE game_date IS NOT NULL
    AND game_date::date BETWEEN %s::date AND %s::date
"""
        + filter_sql
        + """
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
    )
    return sql, (from_date, to_date) + filter_params


def _overall(source_table: str, from_date: str, to_date: str, prop_types: Sequence[str]) -> Dict[str, Any]:
    common_cte, params = _common_cte(source_table, from_date, to_date, prop_types)
    rows = pg_fetchall(
        common_cte
        + """
SELECT
  COUNT(*) FILTER (WHERE model_correct_i IS NOT NULL)::int AS total,
  COALESCE(SUM(model_correct_i), 0)::int AS correct
FROM labeled
""",
        params,
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


def _by_prop(source_table: str, from_date: str, to_date: str, prop_types: Sequence[str]) -> list[Dict[str, Any]]:
    common_cte, params = _common_cte(source_table, from_date, to_date, prop_types)
    rows = pg_fetchall(
        common_cte
        + """
SELECT
  prop_type,
  COUNT(*) FILTER (WHERE model_correct_i IS NOT NULL)::int AS total,
  COALESCE(SUM(model_correct_i), 0)::int AS correct
FROM labeled
GROUP BY prop_type
ORDER BY total DESC, prop_type
""",
        params,
    )
    out: list[Dict[str, Any]] = []
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


def _by_source(source_table: str, from_date: str, to_date: str, prop_types: Sequence[str]) -> list[Dict[str, Any]]:
    common_cte, params = _common_cte(source_table, from_date, to_date, prop_types)
    rows = pg_fetchall(
        common_cte
        + """
SELECT
  prop_source,
  COUNT(*) FILTER (WHERE model_correct_i IS NOT NULL)::int AS total,
  COALESCE(SUM(model_correct_i), 0)::int AS correct
FROM labeled
GROUP BY prop_source
ORDER BY total DESC, prop_source
""",
        params,
    )
    out: list[Dict[str, Any]] = []
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


def _segment(
    name: str,
    *,
    source_table: str,
    from_date: str,
    to_date: str,
    prop_types: Sequence[str],
) -> Dict[str, Any]:
    return {
        "segment": name,
        "overall": _overall(source_table, from_date, to_date, prop_types),
        "by_prop": _by_prop(source_table, from_date, to_date, prop_types),
        "by_source": _by_source(source_table, from_date, to_date, prop_types),
    }


def _comparison(pre: Dict[str, Any], reg: Dict[str, Any]) -> Dict[str, Any]:
    pre_overall = pre.get("overall") or {}
    reg_overall = reg.get("overall") or {}
    pre_total = int(pre_overall.get("total") or 0)
    reg_total = int(reg_overall.get("total") or 0)
    pre_acc = pre_overall.get("accuracy_pct")
    reg_acc = reg_overall.get("accuracy_pct")

    by_prop_pre = {str(r.get("prop_type")): r for r in (pre.get("by_prop") or []) if r.get("prop_type")}
    by_prop_reg = {str(r.get("prop_type")): r for r in (reg.get("by_prop") or []) if r.get("prop_type")}
    prop_union = sorted(set(by_prop_pre.keys()) | set(by_prop_reg.keys()))

    per_prop: list[Dict[str, Any]] = []
    for prop in prop_union:
        p = by_prop_pre.get(prop) or {}
        r = by_prop_reg.get(prop) or {}
        p_acc = p.get("accuracy_pct")
        r_acc = r.get("accuracy_pct")
        delta_acc = None
        if p_acc is not None and r_acc is not None:
            delta_acc = round(float(r_acc) - float(p_acc), 2)
        per_prop.append(
            {
                "prop_type": prop,
                "preseason_total": int(p.get("total") or 0),
                "regular_total": int(r.get("total") or 0),
                "preseason_accuracy_pct": p_acc,
                "regular_accuracy_pct": r_acc,
                "regular_minus_preseason_accuracy_pct": delta_acc,
            }
        )

    delta_acc = None
    if pre_acc is not None and reg_acc is not None:
        delta_acc = round(float(reg_acc) - float(pre_acc), 2)

    return {
        "overall": {
            "preseason_total": pre_total,
            "regular_total": reg_total,
            "regular_minus_preseason_total": reg_total - pre_total,
            "preseason_accuracy_pct": pre_acc,
            "regular_accuracy_pct": reg_acc,
            "regular_minus_preseason_accuracy_pct": delta_acc,
        },
        "by_prop": per_prop,
    }


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Segmented MLB quality report (preseason vs regular-season windows).")
    ap.add_argument("--preseason-from-date", required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--preseason-to-date", required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--regular-from-date", required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--regular-to-date", required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--prop-types", default="", help="Optional comma-separated prop types filter.")
    ap.add_argument(
        "--source-table",
        choices=sorted(_ALLOWED_SOURCE_TABLES),
        default=_DEFAULT_SOURCE_TABLE,
        help="Source table for quality metrics (default: model_training_props).",
    )
    ap.add_argument("--min-preseason-total", type=int, default=1)
    ap.add_argument("--min-regular-total", type=int, default=1)
    args = ap.parse_args(list(argv) if argv is not None else sys.argv[1:])

    try:
        preseason_from = _validate_iso(args.preseason_from_date, "preseason-from-date")
        preseason_to = _validate_iso(args.preseason_to_date, "preseason-to-date")
        regular_from = _validate_iso(args.regular_from_date, "regular-from-date")
        regular_to = _validate_iso(args.regular_to_date, "regular-to-date")
    except ValueError as e:
        print(json.dumps({"ok": False, "status": "fail", "error": str(e)}, indent=2))
        return 2
    if preseason_from > preseason_to:
        print(json.dumps({"ok": False, "status": "fail", "error": "preseason date range invalid"}, indent=2))
        return 2
    if regular_from > regular_to:
        print(json.dumps({"ok": False, "status": "fail", "error": "regular date range invalid"}, indent=2))
        return 2

    source_table = _normalize_source_table(args.source_table)
    prop_types = [p.strip() for p in str(args.prop_types).split(",") if p.strip()]

    preseason = _segment(
        "preseason",
        source_table=source_table,
        from_date=preseason_from,
        to_date=preseason_to,
        prop_types=prop_types,
    )
    regular = _segment(
        "regular",
        source_table=source_table,
        from_date=regular_from,
        to_date=regular_to,
        prop_types=prop_types,
    )

    pre_total = int(((preseason.get("overall") or {}).get("total") or 0))
    reg_total = int(((regular.get("overall") or {}).get("total") or 0))
    min_pre = max(0, int(args.min_preseason_total))
    min_reg = max(0, int(args.min_regular_total))
    ok = pre_total >= min_pre and reg_total >= min_reg

    payload = {
        "ok": ok,
        "status": "pass" if ok else "fail",
        "source_table": source_table,
        "prop_types": prop_types,
        "min_preseason_total": min_pre,
        "min_regular_total": min_reg,
        "segments": {
            "preseason": preseason,
            "regular": regular,
        },
        "comparison": _comparison(preseason, regular),
        "caveats": [
            "Segmentation is date-window based; game_type is not stored in current MLB tables.",
            "Accuracy uses graded outcomes only (win/loss).",
        ],
    }
    print(json.dumps(payload, indent=2, default=str))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
