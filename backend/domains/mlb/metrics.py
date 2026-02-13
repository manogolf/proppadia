"""MLB metrics domain queries."""

from __future__ import annotations

from typing import Any, Dict, List, Sequence

try:
    import psycopg
    import psycopg.rows
except Exception:  # pragma: no cover - environment-dependent import
    psycopg = None  # type: ignore

from backend.supabase.supabase_utils import get_database_url


def _db_url() -> str:
    url = get_database_url()
    if not url:
        raise RuntimeError("DATABASE_URL/SUPABASE_DB_URL not configured")
    return url


def _fetchall(sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    if psycopg is None:
        raise RuntimeError("psycopg not installed")
    with psycopg.connect(_db_url(), row_factory=psycopg.rows.dict_row, prepare_threshold=None) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return list(cur.fetchall() or [])


BASE_METRICS_CTE = """
WITH base AS (
  SELECT
    prop_type,
    game_date::date AS game_day,
    lower(trim(over_under)) AS over_under_norm,
    lower(trim(outcome)) AS outcome_norm,
    lower(trim(predicted_outcome)) AS predicted_norm,
    was_correct
  FROM player_props
  WHERE game_date IS NOT NULL
    AND lower(trim(status)) IN ('win', 'loss', 'resolved', 'pending', 'expired', 'dnp')
),
resolved AS (
  SELECT
    prop_type,
    game_day,
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
    END AS was_correct_i,
    outcome_norm
  FROM base
)
"""


def get_user_vs_model_accuracy() -> List[Dict[str, Any]]:
    sql = BASE_METRICS_CTE + """
SELECT
  prop_type,
  COUNT(*) FILTER (WHERE outcome_norm IN ('win', 'loss'))::int AS total,
  COUNT(*) FILTER (WHERE outcome_norm IN ('win', 'loss'))::int AS user_total,
  COUNT(*) FILTER (WHERE outcome_norm = 'win')::int AS user_correct,
  COUNT(*) FILTER (
    WHERE outcome_norm IN ('win', 'loss')
      AND (predicted_side IS NOT NULL OR was_correct_i IS NOT NULL)
  )::int AS model_total,
  SUM(
    CASE
      WHEN outcome_norm IN ('win', 'loss')
       AND (predicted_side IS NOT NULL OR was_correct_i IS NOT NULL)
      THEN COALESCE(was_correct_i, CASE WHEN predicted_side = actual_side THEN 1 ELSE 0 END, 0)
      ELSE 0
    END
  )::int AS model_correct
FROM resolved
GROUP BY prop_type
ORDER BY prop_type;
"""
    return _fetchall(sql)


def get_model_accuracy_metrics() -> List[Dict[str, Any]]:
    sql = BASE_METRICS_CTE + """
SELECT
  prop_type,
  COUNT(*) FILTER (
    WHERE outcome_norm IN ('win', 'loss')
      AND (predicted_side IS NOT NULL OR was_correct_i IS NOT NULL)
  )::int AS total,
  SUM(
    CASE
      WHEN outcome_norm IN ('win', 'loss')
       AND (predicted_side IS NOT NULL OR was_correct_i IS NOT NULL)
      THEN COALESCE(was_correct_i, CASE WHEN predicted_side = actual_side THEN 1 ELSE 0 END, 0)
      ELSE 0
    END
  )::int AS correct
FROM resolved
GROUP BY prop_type
ORDER BY prop_type;
"""
    return _fetchall(sql)


def get_user_vs_model_accuracy_weekly() -> List[Dict[str, Any]]:
    sql = BASE_METRICS_CTE + """
SELECT
  date_trunc('week', game_day)::date AS week_start,
  prop_type,
  COUNT(*) FILTER (WHERE outcome_norm IN ('win', 'loss'))::int AS total,
  COUNT(*) FILTER (WHERE outcome_norm IN ('win', 'loss'))::int AS user_total,
  COUNT(*) FILTER (WHERE outcome_norm = 'win')::int AS user_correct,
  COUNT(*) FILTER (
    WHERE outcome_norm IN ('win', 'loss')
      AND (predicted_side IS NOT NULL OR was_correct_i IS NOT NULL)
  )::int AS model_total,
  SUM(
    CASE
      WHEN outcome_norm IN ('win', 'loss')
       AND (predicted_side IS NOT NULL OR was_correct_i IS NOT NULL)
      THEN COALESCE(was_correct_i, CASE WHEN predicted_side = actual_side THEN 1 ELSE 0 END, 0)
      ELSE 0
    END
  )::int AS model_correct
FROM resolved
GROUP BY date_trunc('week', game_day)::date, prop_type
ORDER BY week_start DESC, prop_type;
"""
    return _fetchall(sql)


def get_model_accuracy_weekly() -> List[Dict[str, Any]]:
    sql = BASE_METRICS_CTE + """
SELECT
  date_trunc('week', game_day)::date AS week_start,
  prop_type,
  COUNT(*) FILTER (
    WHERE outcome_norm IN ('win', 'loss')
      AND (predicted_side IS NOT NULL OR was_correct_i IS NOT NULL)
  )::int AS total,
  SUM(
    CASE
      WHEN outcome_norm IN ('win', 'loss')
       AND (predicted_side IS NOT NULL OR was_correct_i IS NOT NULL)
      THEN COALESCE(was_correct_i, CASE WHEN predicted_side = actual_side THEN 1 ELSE 0 END, 0)
      ELSE 0
    END
  )::int AS correct
FROM resolved
GROUP BY date_trunc('week', game_day)::date, prop_type
ORDER BY week_start DESC, prop_type;
"""
    rows = _fetchall(sql)
    for row in rows:
        total = row.get("total") or 0
        correct = row.get("correct") or 0
        row["accuracy"] = (100.0 * correct / total) if total > 0 else None
    return rows

