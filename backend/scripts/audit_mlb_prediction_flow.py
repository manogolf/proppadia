#!/usr/bin/env python3
"""Audit MLB prediction flow integrity for prepare->predict->add->grade path."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict, Sequence

from backend.shared.db.pg import pg_fetchall


def _date_filter(date_column: str, source_table: str, window_mode: str) -> str:
    if window_mode == "games":
        return f"""
WHERE {date_column} IN (
  SELECT DISTINCT game_date
  FROM {source_table}
  WHERE game_date IS NOT NULL
  ORDER BY game_date DESC
  LIMIT %s::int
)
"""
    return f"WHERE {date_column} >= (CURRENT_DATE - (%s::int || ' days')::interval)::date"


def _has_user_id_column() -> bool:
    rows = pg_fetchall(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema='public'
          AND table_name='player_props'
          AND column_name='user_id'
        LIMIT 1
        """
    )
    return bool(rows)


def _flow_summary(window_value: int, window_mode: str) -> Dict[str, int]:
    rows = pg_fetchall(
        """
        SELECT
          COUNT(*)::int AS total_rows,
          COUNT(*) FILTER (WHERE prop_source = 'user_added')::int AS user_added_rows,
          COUNT(*) FILTER (WHERE prop_source = 'mlb_api')::int AS mlb_api_rows,
          COUNT(*) FILTER (WHERE lower(trim(coalesce(outcome, ''))) IN ('win','loss','push','dnp'))::int AS resolved_rows,
          COUNT(*) FILTER (WHERE lower(trim(coalesce(outcome, ''))) IN ('win','loss'))::int AS graded_rows
        FROM player_props
        """
        + _date_filter("game_date", "player_props", window_mode),
        (int(window_value),),
    )
    row = (rows or [{}])[0]
    return {
        "total_rows": int(row.get("total_rows") or 0),
        "user_added_rows": int(row.get("user_added_rows") or 0),
        "mlb_api_rows": int(row.get("mlb_api_rows") or 0),
        "resolved_rows": int(row.get("resolved_rows") or 0),
        "graded_rows": int(row.get("graded_rows") or 0),
    }


def _integrity_checks(window_value: int, window_mode: str, max_drift_days: int) -> Dict[str, int]:
    rows = pg_fetchall(
        """
        SELECT
          COUNT(*) FILTER (
            WHERE prop_source = 'user_added'
              AND (
                game_id IS NULL
                OR trim(cast(game_id as text)) = ''
                OR trim(cast(game_id as text)) !~ '^[0-9]+$'
                OR cast(game_id as bigint) <= 0
              )
          )::int AS user_added_missing_game_id,
          COUNT(*) FILTER (
            WHERE prop_source = 'user_added'
              AND (
                game_date IS NULL
                OR trim(cast(game_date as text)) = ''
                OR cast(game_date as text) !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
              )
          )::int AS user_added_invalid_game_date,
          COUNT(*) FILTER (
            WHERE prop_source = 'user_added'
              AND game_date IS NOT NULL
              AND ABS((created_at::date - game_date::date)) > %s::int
          )::int AS user_added_created_game_date_drift,
          COUNT(*) FILTER (
            WHERE lower(trim(coalesce(status, ''))) = 'resolved'
              AND lower(trim(coalesce(outcome, ''))) NOT IN ('win','loss','push','dnp')
          )::int AS resolved_rows_with_invalid_outcome
        FROM player_props
        """
        + _date_filter("game_date", "player_props", window_mode),
        (int(max_drift_days), int(window_value)),
    )
    row = (rows or [{}])[0]
    return {
        "user_added_missing_game_id": int(row.get("user_added_missing_game_id") or 0),
        "user_added_invalid_game_date": int(row.get("user_added_invalid_game_date") or 0),
        "user_added_created_game_date_drift": int(row.get("user_added_created_game_date_drift") or 0),
        "resolved_rows_with_invalid_outcome": int(row.get("resolved_rows_with_invalid_outcome") or 0),
    }


def _duplicate_rows(window_value: int, window_mode: str, include_user_id: bool) -> Dict[str, int]:
    user_expr = ", COALESCE(cast(user_id as text), '')" if include_user_id else ""
    user_where = _date_filter("game_date", "player_props", window_mode).replace("WHERE", "WHERE prop_source = 'user_added' AND ", 1)
    user_rows = pg_fetchall(
        f"""
        SELECT
          COUNT(*)::int AS duplicate_groups,
          COALESCE(SUM(dup_count - 1), 0)::int AS duplicate_extra_rows
        FROM (
          SELECT COUNT(*)::int AS dup_count
          FROM player_props
          {user_where}
          GROUP BY player_id, game_id, prop_type, over_under, prop_value{user_expr}
          HAVING COUNT(*) > 1
        ) d
        """,
        (int(window_value),),
    )

    mt_rows = pg_fetchall(
        """
        SELECT
          COUNT(*)::int AS duplicate_groups,
          COALESCE(SUM(dup_count - 1), 0)::int AS duplicate_extra_rows
        FROM (
          SELECT COUNT(*)::int AS dup_count
          FROM model_training_props
          WHERE prop_source = 'mlb_api'
        """
        + _date_filter("game_date", "model_training_props", window_mode).replace("WHERE", "AND ", 1)
        + """
          GROUP BY player_id, game_id, prop_type, prop_source
          HAVING COUNT(*) > 1
        ) d
        """,
        (int(window_value),),
    )
    a = (user_rows or [{}])[0]
    b = (mt_rows or [{}])[0]
    return {
        "player_props_duplicate_groups": int(a.get("duplicate_groups") or 0),
        "player_props_duplicate_extra_rows": int(a.get("duplicate_extra_rows") or 0),
        "model_training_duplicate_groups": int(b.get("duplicate_groups") or 0),
        "model_training_duplicate_extra_rows": int(b.get("duplicate_extra_rows") or 0),
    }


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Audit MLB prediction flow integrity.")
    ap.add_argument("--window-mode", choices=["days", "games"], default="days")
    ap.add_argument("--window-days", type=int, default=30)
    ap.add_argument("--games-back", type=int, default=30)
    ap.add_argument("--max-created-date-drift-days", type=int, default=1)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(list(argv) if argv is not None else sys.argv[1:])

    window_mode = "games" if str(args.window_mode).lower() == "games" else "days"
    window_value = max(1, int(args.games_back if window_mode == "games" else args.window_days))
    max_drift_days = max(0, int(args.max_created_date_drift_days))

    include_user_id = _has_user_id_column()
    summary = _flow_summary(window_value, window_mode)
    checks = _integrity_checks(window_value, window_mode, max_drift_days)
    dupes = _duplicate_rows(window_value, window_mode, include_user_id=include_user_id)

    failures: list[str] = []
    if checks["user_added_missing_game_id"] > 0:
        failures.append("user_added_missing_game_id")
    if checks["user_added_invalid_game_date"] > 0:
        failures.append("user_added_invalid_game_date")
    if checks["user_added_created_game_date_drift"] > 0:
        failures.append("user_added_created_game_date_drift")
    if checks["resolved_rows_with_invalid_outcome"] > 0:
        failures.append("resolved_rows_with_invalid_outcome")
    if dupes["player_props_duplicate_groups"] > 0:
        failures.append("player_props_duplicate_groups")
    if dupes["model_training_duplicate_groups"] > 0:
        failures.append("model_training_duplicate_groups")

    ok = len(failures) == 0
    payload = {
        "ok": ok,
        "status": "pass" if ok else "fail",
        "window_mode": window_mode,
        "window_value": window_value,
        "max_created_date_drift_days": max_drift_days,
        "source_of_truth": {
            "prediction_write_table": "public.player_props",
            "training_table": "public.model_training_props",
            "grade_fields": ["status", "outcome", "result"],
            "join_keys": ["player_id", "game_id", "prop_type"],
            "user_scope_key": "user_id" if include_user_id else None,
        },
        "summary": summary,
        "checks": checks,
        "duplicates": dupes,
        "failures": failures,
    }

    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print(f"MLB prediction flow audit: {payload['status']}")
        print(f"window: {window_mode}={window_value}")
        print(
            "summary:",
            f"total={summary['total_rows']}",
            f"user_added={summary['user_added_rows']}",
            f"mlb_api={summary['mlb_api_rows']}",
            f"graded={summary['graded_rows']}",
        )
        print("checks:", json.dumps(checks, sort_keys=True))
        print("dupes:", json.dumps(dupes, sort_keys=True))
        if failures:
            print("FAIL:", ",".join(failures))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
