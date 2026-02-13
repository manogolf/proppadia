#!/usr/bin/env python3
"""
Validate MLB metrics endpoints against independent SQL aggregates.

By default this runs in-process (FastAPI TestClient) and compares:
- /api/model-metrics
- /api/user-vs-model-accuracy
- /api/user-vs-model-accuracy-weekly
- /api/model-accuracy-weekly

against direct SQL over player_props.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from backend.scripts.api_client_utils import ClientAdapter, HttpClient, InProcessClient
from backend.shared.db import pg_fetchall


def _k_prop(row: Dict[str, Any]) -> str:
    return str(row.get("prop_type") or "")


def _k_week_prop(row: Dict[str, Any]) -> Tuple[str, str]:
    return (str(row.get("week_start") or ""), str(row.get("prop_type") or ""))


def _to_int(v: Any) -> int:
    try:
        return int(v)
    except Exception:
        return 0


def _to_float(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


@dataclass
class Diff:
    key: Any
    field: str
    api_value: Any
    db_value: Any


def _fetch_api_rows(client: ClientAdapter, path: str) -> List[Dict[str, Any]]:
    status, body = client.get_json(path)
    if status != 200:
        raise RuntimeError(f"{path} returned {status}: {body}")
    if not isinstance(body, list):
        raise RuntimeError(f"{path} expected list response, got: {type(body).__name__}")
    return body


def _fetchall(sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    return pg_fetchall(sql, params)


# Independent SQL for validation (deliberately not importing backend/domains/mlb/metrics.py)
COMMON_CTE = """
WITH src AS (
  SELECT
    prop_type,
    game_date::date AS game_day,
    lower(trim(over_under)) AS over_under_norm,
    lower(trim(outcome)) AS outcome_norm,
    lower(trim(predicted_outcome)) AS predicted_norm,
    was_correct
  FROM player_props
  WHERE game_date IS NOT NULL
),
norm AS (
  SELECT
    prop_type,
    game_day,
    outcome_norm,
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
)
"""


def db_user_vs_model() -> List[Dict[str, Any]]:
    sql = COMMON_CTE + """
SELECT
  prop_type,
  COUNT(*) FILTER (WHERE outcome_norm IN ('win','loss'))::int AS total,
  COUNT(*) FILTER (WHERE outcome_norm IN ('win','loss'))::int AS user_total,
  COUNT(*) FILTER (WHERE outcome_norm = 'win')::int AS user_correct,
  COUNT(*) FILTER (WHERE outcome_norm IN ('win','loss') AND (predicted_side IS NOT NULL OR was_correct_i IS NOT NULL))::int AS model_total,
  SUM(
    CASE
      WHEN outcome_norm IN ('win','loss') AND (predicted_side IS NOT NULL OR was_correct_i IS NOT NULL)
      THEN COALESCE(was_correct_i, CASE WHEN predicted_side = actual_side THEN 1 ELSE 0 END, 0)
      ELSE 0
    END
  )::int AS model_correct
FROM norm
GROUP BY prop_type
ORDER BY prop_type
"""
    return _fetchall(sql)


def db_model_metrics() -> List[Dict[str, Any]]:
    sql = COMMON_CTE + """
SELECT
  prop_type,
  COUNT(*) FILTER (WHERE outcome_norm IN ('win','loss') AND (predicted_side IS NOT NULL OR was_correct_i IS NOT NULL))::int AS total,
  SUM(
    CASE
      WHEN outcome_norm IN ('win','loss') AND (predicted_side IS NOT NULL OR was_correct_i IS NOT NULL)
      THEN COALESCE(was_correct_i, CASE WHEN predicted_side = actual_side THEN 1 ELSE 0 END, 0)
      ELSE 0
    END
  )::int AS correct
FROM norm
GROUP BY prop_type
ORDER BY prop_type
"""
    return _fetchall(sql)


def db_user_vs_model_weekly() -> List[Dict[str, Any]]:
    sql = COMMON_CTE + """
SELECT
  date_trunc('week', game_day)::date AS week_start,
  prop_type,
  COUNT(*) FILTER (WHERE outcome_norm IN ('win','loss'))::int AS total,
  COUNT(*) FILTER (WHERE outcome_norm IN ('win','loss'))::int AS user_total,
  COUNT(*) FILTER (WHERE outcome_norm = 'win')::int AS user_correct,
  COUNT(*) FILTER (WHERE outcome_norm IN ('win','loss') AND (predicted_side IS NOT NULL OR was_correct_i IS NOT NULL))::int AS model_total,
  SUM(
    CASE
      WHEN outcome_norm IN ('win','loss') AND (predicted_side IS NOT NULL OR was_correct_i IS NOT NULL)
      THEN COALESCE(was_correct_i, CASE WHEN predicted_side = actual_side THEN 1 ELSE 0 END, 0)
      ELSE 0
    END
  )::int AS model_correct
FROM norm
GROUP BY date_trunc('week', game_day)::date, prop_type
ORDER BY week_start DESC, prop_type
"""
    return _fetchall(sql)


def db_model_weekly() -> List[Dict[str, Any]]:
    rows = []
    for row in db_user_vs_model_weekly():
        total = _to_int(row.get("model_total"))
        correct = _to_int(row.get("model_correct"))
        rows.append(
            {
                "week_start": row.get("week_start"),
                "prop_type": row.get("prop_type"),
                "total": total,
                "correct": correct,
                "accuracy": (100.0 * correct / total) if total > 0 else None,
            }
        )
    return rows


def compare_rows(
    *,
    api_rows: Iterable[Dict[str, Any]],
    db_rows: Iterable[Dict[str, Any]],
    key_fn,
    fields: Sequence[str],
    float_fields: Sequence[str] = (),
    tol: float = 1e-6,
) -> List[Diff]:
    api_map = {key_fn(r): r for r in api_rows}
    db_map = {key_fn(r): r for r in db_rows}
    keys = sorted(set(api_map.keys()) | set(db_map.keys()), key=str)
    diffs: List[Diff] = []
    for k in keys:
        a = api_map.get(k, {})
        b = db_map.get(k, {})
        for f in fields:
            av = a.get(f)
            bv = b.get(f)
            if f in float_fields:
                if av is None and bv is None:
                    continue
                if abs(_to_float(av) - _to_float(bv)) > tol:
                    diffs.append(Diff(k, f, av, bv))
            else:
                if _to_int(av) != _to_int(bv):
                    diffs.append(Diff(k, f, av, bv))
    return diffs


def print_diffs(name: str, diffs: List[Diff], max_rows: int):
    if not diffs:
        print(f"PASS {name}: no differences")
        return
    print(f"FAIL {name}: {len(diffs)} differences")
    for d in diffs[:max_rows]:
        print(
            f"  key={d.key} field={d.field} api={json.dumps(d.api_value, default=str)} db={json.dumps(d.db_value, default=str)}"
        )


def main():
    ap = argparse.ArgumentParser(description="Validate MLB metrics endpoints against independent SQL")
    ap.add_argument("--base-url", default="", help="Optional base URL, e.g. http://127.0.0.1:8001")
    ap.add_argument("--api-only", action="store_true", help="Only assert API endpoints return list payloads")
    ap.add_argument("--max-diff-rows", type=int, default=30)
    args = ap.parse_args()

    client: ClientAdapter = HttpClient(args.base_url, timeout=25) if args.base_url else InProcessClient()

    try:
        api_user_vs = _fetch_api_rows(client, "/api/user-vs-model-accuracy")
        api_model = _fetch_api_rows(client, "/api/model-metrics")
        api_user_vs_weekly = _fetch_api_rows(client, "/api/user-vs-model-accuracy-weekly")
        api_model_weekly = _fetch_api_rows(client, "/api/model-accuracy-weekly")
    except RuntimeError as e:
        print(f"FAIL metrics endpoint check: {e}")
        print("Hint: ensure backend can reach DB and DATABASE_URL/SUPABASE_DB_URL is configured.")
        raise SystemExit(1)

    print(
        "API rows:"
        f" user_vs={len(api_user_vs)}"
        f" model={len(api_model)}"
        f" user_vs_weekly={len(api_user_vs_weekly)}"
        f" model_weekly={len(api_model_weekly)}"
    )

    if args.api_only:
        print("PASS api-only validation")
        return

    db_user = db_user_vs_model()
    db_model = db_model_metrics()
    db_user_weekly = db_user_vs_model_weekly()
    db_model_weekly_rows = db_model_weekly()

    diffs = []
    diffs_user = compare_rows(
        api_rows=api_user_vs,
        db_rows=db_user,
        key_fn=_k_prop,
        fields=["total", "user_total", "user_correct", "model_total", "model_correct"],
    )
    print_diffs("user-vs-model", diffs_user, args.max_diff_rows)
    diffs.extend(diffs_user)

    diffs_model = compare_rows(
        api_rows=api_model,
        db_rows=db_model,
        key_fn=_k_prop,
        fields=["total", "correct"],
    )
    print_diffs("model-metrics", diffs_model, args.max_diff_rows)
    diffs.extend(diffs_model)

    diffs_user_week = compare_rows(
        api_rows=api_user_vs_weekly,
        db_rows=db_user_weekly,
        key_fn=_k_week_prop,
        fields=["total", "user_total", "user_correct", "model_total", "model_correct"],
    )
    print_diffs("user-vs-model-weekly", diffs_user_week, args.max_diff_rows)
    diffs.extend(diffs_user_week)

    diffs_model_week = compare_rows(
        api_rows=api_model_weekly,
        db_rows=db_model_weekly_rows,
        key_fn=_k_week_prop,
        fields=["total", "correct", "accuracy"],
        float_fields=["accuracy"],
        tol=1e-4,
    )
    print_diffs("model-weekly", diffs_model_week, args.max_diff_rows)
    diffs.extend(diffs_model_week)

    if diffs:
        raise SystemExit(1)
    print("PASS all metric comparisons")


if __name__ == "__main__":
    main()
