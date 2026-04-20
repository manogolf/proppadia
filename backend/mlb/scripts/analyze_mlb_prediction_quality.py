#!/usr/bin/env python3
"""Analyze MLB prediction quality from historical MLB prop rows."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, Sequence, Tuple

from backend.shared.db.pg import pg_fetchall

_ALLOWED_SOURCE_TABLES = {"player_props", "model_training_props", "reconcile_rows"}
_DEFAULT_SOURCE_TABLE = "model_training_props"


def _normalize_source_table(source_table: str) -> str:
    table = str(source_table or "").strip().lower()
    if table not in _ALLOWED_SOURCE_TABLES:
        raise ValueError(f"source_table must be one of: {sorted(_ALLOWED_SOURCE_TABLES)}")
    return table


def _source_table_sql(source_table: str) -> str:
    table = _normalize_source_table(source_table)
    if table == "reconcile_rows":
        raise ValueError("reconcile_rows source_table is csv-only and does not map to SQL table")
    return f"mlb.{table}"


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _confidence_bucket_from_prob(model_pick_prob: float | None) -> str:
    if model_pick_prob is None:
        return "unknown"
    diff = abs(float(model_pick_prob) - 0.5)
    if diff < 0.05:
        return "low"
    if diff < 0.10:
        return "medium"
    return "high"


def _window_filter_rows(
    rows: list[dict[str, Any]],
    *,
    window_mode: str,
    window_value: int,
) -> list[dict[str, Any]]:
    if not rows:
        return []
    if window_mode == "games":
        ordered_days = sorted({r["game_day"] for r in rows if r.get("game_day") is not None}, reverse=True)
        selected_days = set(ordered_days[: int(window_value)])
        return [r for r in rows if r.get("game_day") in selected_days]
    min_day = date.today() - timedelta(days=int(window_value))
    return [r for r in rows if (r.get("game_day") is not None and r["game_day"] >= min_day)]


def _collect_quality_from_rows_csv(
    *,
    rows_csv: str,
    window_mode: str,
    window_value: int,
    prop_types: Sequence[str],
    require_two_sided: bool,
) -> Dict[str, Any]:
    try:
        import pandas as pd
    except Exception as exc:  # pragma: no cover
        raise RuntimeError(f"pandas is required for reconcile_rows source_table: {exc}") from exc

    csv_path = Path(rows_csv).expanduser()
    if not csv_path.exists():
        raise FileNotFoundError(f"reconcile rows csv not found: {csv_path}")

    df = pd.read_csv(csv_path, low_memory=False)
    if df.empty:
        return {
            "source_table": "reconcile_rows",
            "window_mode": window_mode,
            "window_value": int(window_value),
            "prop_types": list(prop_types),
            "prop_sources": [],
            "overall": {"window_mode": window_mode, "window_value": int(window_value), "total": 0, "correct": 0, "accuracy_pct": None},
            "by_prop": [],
            "by_confidence_bucket": [],
            "drift_14d": {
                "last_14d": {"total": 0, "correct": 0, "accuracy_pct": None},
                "prev_14d": {"total": 0, "correct": 0, "accuracy_pct": None},
                "delta_pct": None,
            },
        }

    for col in ("game_date", "prop_type", "actual_model_pick_outcome", "model_pick_prob"):
        if col not in df.columns:
            df[col] = pd.NA
    if require_two_sided:
        for col in ("price_over_american", "price_under_american"):
            if col not in df.columns:
                df[col] = pd.NA

    if prop_types:
        prop_set = {str(p).strip() for p in prop_types if str(p).strip()}
        df = df[df["prop_type"].astype(str).str.strip().isin(prop_set)]

    if require_two_sided:
        two_sided_mask = (
            pd.to_numeric(df["price_over_american"], errors="coerce").notna()
            & pd.to_numeric(df["price_under_american"], errors="coerce").notna()
        )
        df = df.loc[two_sided_mask].copy()

    df["game_day"] = pd.to_datetime(df["game_date"], errors="coerce").dt.date
    df["actual_model_pick_outcome"] = df["actual_model_pick_outcome"].astype(str).str.lower().str.strip()
    df["model_pick_prob"] = pd.to_numeric(df["model_pick_prob"], errors="coerce")

    normalized_rows: list[dict[str, Any]] = []
    for row in df.itertuples(index=False):
        outcome = str(getattr(row, "actual_model_pick_outcome", "") or "").strip().lower()
        model_correct_i: int | None
        if outcome == "win":
            model_correct_i = 1
        elif outcome == "loss":
            model_correct_i = 0
        else:
            model_correct_i = None

        model_pick_prob_val = _safe_float(getattr(row, "model_pick_prob", None))
        normalized_rows.append(
            {
                "prop_type": str(getattr(row, "prop_type", "") or "").strip(),
                "game_day": getattr(row, "game_day", None),
                "model_correct_i": model_correct_i,
                "confidence_bucket": _confidence_bucket_from_prob(model_pick_prob_val),
            }
        )

    scoped_rows = _window_filter_rows(normalized_rows, window_mode=window_mode, window_value=int(window_value))

    scored_rows = [r for r in scoped_rows if r.get("model_correct_i") is not None]
    total = len(scored_rows)
    correct = sum(_safe_int(r.get("model_correct_i")) for r in scored_rows)
    overall = {
        "window_mode": window_mode,
        "window_value": int(window_value),
        "total": int(total),
        "correct": int(correct),
        "accuracy_pct": round((100.0 * float(correct) / float(total)), 2) if total > 0 else None,
    }

    by_prop_map: dict[str, dict[str, int]] = {}
    for row in scored_rows:
        prop = str(row.get("prop_type") or "").strip()
        if not prop:
            continue
        bucket = by_prop_map.setdefault(prop, {"total": 0, "correct": 0})
        bucket["total"] += 1
        bucket["correct"] += _safe_int(row.get("model_correct_i"))
    by_prop = []
    for prop in sorted(by_prop_map.keys(), key=lambda p: (-by_prop_map[p]["total"], p)):
        p_total = by_prop_map[prop]["total"]
        p_correct = by_prop_map[prop]["correct"]
        by_prop.append(
            {
                "prop_type": prop,
                "total": int(p_total),
                "correct": int(p_correct),
                "accuracy_pct": round((100.0 * float(p_correct) / float(p_total)), 2) if p_total > 0 else None,
            }
        )

    by_conf_map: dict[str, dict[str, int]] = {}
    for row in scored_rows:
        bucket_name = str(row.get("confidence_bucket") or "unknown")
        bucket = by_conf_map.setdefault(bucket_name, {"total": 0, "correct": 0})
        bucket["total"] += 1
        bucket["correct"] += _safe_int(row.get("model_correct_i"))
    confidence_order = {"high": 1, "medium": 2, "low": 3, "unknown": 4}
    by_confidence_bucket = []
    for bucket_name in sorted(by_conf_map.keys(), key=lambda b: (confidence_order.get(b, 99), b)):
        b_total = by_conf_map[bucket_name]["total"]
        b_correct = by_conf_map[bucket_name]["correct"]
        by_confidence_bucket.append(
            {
                "confidence_bucket": bucket_name,
                "total": int(b_total),
                "correct": int(b_correct),
                "accuracy_pct": round((100.0 * float(b_correct) / float(b_total)), 2) if b_total > 0 else None,
            }
        )

    today = date.today()
    last_14_start = today - timedelta(days=14)
    prev_14_start = today - timedelta(days=28)

    drift_rows = [r for r in normalized_rows if r.get("model_correct_i") is not None and r.get("game_day") is not None and r["game_day"] >= prev_14_start]

    def _bucket_stats(bucket_rows: list[dict[str, Any]]) -> dict[str, Any]:
        b_total = len(bucket_rows)
        b_correct = sum(_safe_int(r.get("model_correct_i")) for r in bucket_rows)
        return {
            "total": int(b_total),
            "correct": int(b_correct),
            "accuracy_pct": round((100.0 * float(b_correct) / float(b_total)), 2) if b_total > 0 else None,
        }

    last_14_rows = [r for r in drift_rows if r["game_day"] >= last_14_start]
    prev_14_rows = [r for r in drift_rows if prev_14_start <= r["game_day"] < last_14_start]
    last_14 = _bucket_stats(last_14_rows)
    prev_14 = _bucket_stats(prev_14_rows)
    delta = None
    if last_14.get("accuracy_pct") is not None and prev_14.get("accuracy_pct") is not None:
        delta = round(float(last_14["accuracy_pct"]) - float(prev_14["accuracy_pct"]), 2)

    return {
        "source_table": "reconcile_rows",
        "window_mode": window_mode,
        "window_value": int(window_value),
        "prop_types": list(prop_types),
        "prop_sources": [],
        "overall": overall,
        "by_prop": by_prop,
        "by_confidence_bucket": by_confidence_bucket,
        "drift_14d": {"last_14d": last_14, "prev_14d": prev_14, "delta_pct": delta},
    }


def _common_cte(
    source_table: str,
    prop_types: Sequence[str] | None = None,
    prop_sources: Sequence[str] | None = None,
) -> Tuple[str, Tuple[Any, ...]]:
    prop_types = [str(p).strip() for p in (prop_types or []) if str(p).strip()]
    prop_sources = [str(s).strip().lower() for s in (prop_sources or []) if str(s).strip()]
    filter_sql = ""
    params: Tuple[Any, ...] = ()
    if prop_types:
        placeholders = ", ".join(["%s"] * len(prop_types))
        filter_sql = f" AND prop_type IN ({placeholders})"
        params = tuple(prop_types)
    if prop_sources:
        placeholders = ", ".join(["%s"] * len(prop_sources))
        filter_sql += f" AND lower(trim(coalesce(prop_source, ''))) IN ({placeholders})"
        params = params + tuple(prop_sources)
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
        + _source_table_sql(source_table)
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


def _overall(
    window_value: int,
    window_mode: str,
    source_table: str,
    prop_types: Sequence[str],
    prop_sources: Sequence[str],
) -> Dict[str, Any]:
    common_cte, cte_params = _common_cte(source_table, prop_types, prop_sources)
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


def _by_prop(
    window_value: int,
    window_mode: str,
    source_table: str,
    prop_types: Sequence[str],
    prop_sources: Sequence[str],
) -> list[Dict[str, Any]]:
    common_cte, cte_params = _common_cte(source_table, prop_types, prop_sources)
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
    window_value: int,
    window_mode: str,
    source_table: str,
    prop_types: Sequence[str],
    prop_sources: Sequence[str],
) -> list[Dict[str, Any]]:
    common_cte, cte_params = _common_cte(source_table, prop_types, prop_sources)
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


def _drift(source_table: str, prop_types: Sequence[str], prop_sources: Sequence[str]) -> Dict[str, Any]:
    common_cte, cte_params = _common_cte(source_table, prop_types, prop_sources)
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
    prop_sources: Sequence[str] | None = None,
    source_table: str = _DEFAULT_SOURCE_TABLE,
    rows_csv: str | None = None,
    require_two_sided_reconcile_rows: bool = (
        str(os.environ.get("MLB_QUALITY_RECONCILE_REQUIRE_TWO_SIDED", "1")).strip().lower()
        in {"1", "true", "yes", "on"}
    ),
) -> Dict[str, Any]:
    normalized_mode = "games" if str(window_mode).lower() == "games" else "days"
    normalized_source = _normalize_source_table(source_table)
    window_value = max(1, int(window_value))
    filtered_prop_types = [str(p).strip() for p in (prop_types or []) if str(p).strip()]
    filtered_prop_sources = [str(s).strip().lower() for s in (prop_sources or []) if str(s).strip()]
    if normalized_source == "reconcile_rows":
        return _collect_quality_from_rows_csv(
            rows_csv=str(rows_csv or "").strip(),
            window_mode=normalized_mode,
            window_value=window_value,
            prop_types=filtered_prop_types,
            require_two_sided=bool(require_two_sided_reconcile_rows),
        )
    overall = _overall(window_value, normalized_mode, normalized_source, filtered_prop_types, filtered_prop_sources)
    by_prop = _by_prop(window_value, normalized_mode, normalized_source, filtered_prop_types, filtered_prop_sources)
    by_bucket = _by_confidence_bucket(
        window_value, normalized_mode, normalized_source, filtered_prop_types, filtered_prop_sources
    )
    drift = _drift(normalized_source, filtered_prop_types, filtered_prop_sources)
    return {
        "source_table": normalized_source,
        "window_mode": normalized_mode,
        "window_value": window_value,
        "prop_types": filtered_prop_types,
        "prop_sources": filtered_prop_sources,
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
    ap.add_argument("--prop-sources", default="", help="Optional comma-separated prop sources to filter quality scope.")
    ap.add_argument(
        "--source-table",
        choices=sorted(_ALLOWED_SOURCE_TABLES),
        default=_DEFAULT_SOURCE_TABLE,
        help="Source table for quality metrics.",
    )
    ap.add_argument(
        "--rows-csv",
        default="",
        help="Required when --source-table reconcile_rows; path to reconcile rows csv.",
    )
    ap.add_argument(
        "--reconcile-require-two-sided",
        action="store_true",
        default=str(os.environ.get("MLB_QUALITY_RECONCILE_REQUIRE_TWO_SIDED", "1")).strip().lower()
        in {"1", "true", "yes", "on"},
        help="When using --source-table reconcile_rows, keep only two-sided market rows.",
    )
    args = ap.parse_args(list(argv) if argv is not None else sys.argv[1:])

    window_value = int(args.games_back) if args.window_mode == "games" else int(args.window_days)
    prop_types = [p.strip() for p in str(args.prop_types).split(",") if p.strip()]
    prop_sources = [s.strip().lower() for s in str(args.prop_sources).split(",") if s.strip()]
    quality = collect_quality(
        args.window_mode,
        window_value,
        prop_types=prop_types,
        prop_sources=prop_sources,
        source_table=args.source_table,
        rows_csv=args.rows_csv,
        require_two_sided_reconcile_rows=bool(args.reconcile_require_two_sided),
    )
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
