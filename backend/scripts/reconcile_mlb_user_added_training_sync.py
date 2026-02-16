#!/usr/bin/env python3
"""Reconcile missing graded user_added MLB rows from player_props into model_training_props."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Sequence

from backend.shared.db.pg import pg_fetchone


def _window_where(from_date: str | None, to_date: str | None) -> tuple[str, tuple[str, ...]]:
    clauses: list[str] = []
    params: list[str] = []
    if from_date:
        clauses.append("pp.game_date >= %s::date")
        params.append(from_date)
    if to_date:
        clauses.append("pp.game_date <= %s::date")
        params.append(to_date)
    if not clauses:
        return "", tuple()
    return " AND " + " AND ".join(clauses), tuple(params)


def _preview(from_date: str | None, to_date: str | None) -> dict[str, int]:
    window_where, params = _window_where(from_date, to_date)
    row = pg_fetchone(
        f"""
WITH missing AS (
  SELECT
    pp.id,
    pp.player_id,
    pp.game_id,
    pp.prop_type,
    pp.game_date,
    COALESCE(pp.team_id, pi.team_id)::bigint AS team_id_resolved
  FROM player_props pp
  LEFT JOIN player_ids pi
    ON CAST(pi.player_id AS TEXT) = CAST(pp.player_id AS TEXT)
  WHERE pp.prop_source = 'user_added'
    AND lower(trim(coalesce(pp.status, ''))) IN ('win','loss')
    AND lower(trim(coalesce(pp.outcome, ''))) IN ('win','loss')
    AND pp.game_id IS NOT NULL
    AND trim(cast(pp.game_id as text)) ~ '^[0-9]+$'
    AND cast(pp.game_id as bigint) > 0
    {window_where}
    AND NOT EXISTS (
      SELECT 1
      FROM model_training_props mt
      WHERE mt.prop_source = 'user_added'
        AND CAST(mt.player_id AS TEXT) = CAST(pp.player_id AS TEXT)
        AND CAST(mt.game_id AS TEXT) = CAST(pp.game_id AS TEXT)
        AND mt.prop_type = pp.prop_type
    )
)
SELECT
  COUNT(*)::int AS missing_total,
  COUNT(*) FILTER (WHERE team_id_resolved IS NOT NULL)::int AS eligible_total,
  COUNT(*) FILTER (WHERE team_id_resolved IS NULL)::int AS skipped_missing_team_id
FROM missing
        """,
        params,
    ) or {}
    return {
        "missing_total": int(row.get("missing_total") or 0),
        "eligible_total": int(row.get("eligible_total") or 0),
        "skipped_missing_team_id": int(row.get("skipped_missing_team_id") or 0),
    }


def _apply(from_date: str | None, to_date: str | None) -> dict[str, int]:
    window_where, params = _window_where(from_date, to_date)
    row = pg_fetchone(
        f"""
WITH missing AS (
  SELECT
    pp.player_id,
    pp.player_name,
    COALESCE(pp.team_id, pi.team_id)::bigint AS team_id_resolved,
    pp.game_id,
    pp.game_date,
    pp.prop_type,
    pp.prop_value,
    pp.over_under,
    pp.status,
    pp.outcome,
    pp.predicted_outcome,
    pp.confidence_score,
    pp.prediction_timestamp,
    pp.was_correct,
    pp.position,
    pp.is_home,
    pp.is_pitcher,
    pp.opponent,
    pp.opponent_encoded,
    pp.opponent_team_id,
    pp.game_time,
    pp.home_away,
    CASE
      WHEN pp.result IS NULL OR trim(cast(pp.result as text)) = '' THEN NULL
      WHEN trim(cast(pp.result as text)) ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN trim(cast(pp.result as text))::double precision
      ELSE NULL
    END AS result_numeric,
    pp.created_at
  FROM player_props pp
  LEFT JOIN player_ids pi
    ON CAST(pi.player_id AS TEXT) = CAST(pp.player_id AS TEXT)
  WHERE pp.prop_source = 'user_added'
    AND lower(trim(coalesce(pp.status, ''))) IN ('win','loss')
    AND lower(trim(coalesce(pp.outcome, ''))) IN ('win','loss')
    AND pp.game_id IS NOT NULL
    AND trim(cast(pp.game_id as text)) ~ '^[0-9]+$'
    AND cast(pp.game_id as bigint) > 0
    {window_where}
    AND NOT EXISTS (
      SELECT 1
      FROM model_training_props mt
      WHERE mt.prop_source = 'user_added'
        AND CAST(mt.player_id AS TEXT) = CAST(pp.player_id AS TEXT)
        AND CAST(mt.game_id AS TEXT) = CAST(pp.game_id AS TEXT)
        AND mt.prop_type = pp.prop_type
    )
),
eligible AS (
  SELECT *
  FROM missing
  WHERE team_id_resolved IS NOT NULL
),
upserted AS (
  INSERT INTO model_training_props (
    player_id,
    player_name,
    team,
    team_id,
    game_id,
    game_date,
    prop_type,
    prop_value,
    over_under,
    status,
    outcome,
    predicted_outcome,
    confidence_score,
    prediction_timestamp,
    was_correct,
    prop_source,
    created_at,
    updated_at,
    position,
    is_home,
    is_pitcher,
    opponent,
    opponent_encoded,
    opponent_team_id,
    line,
    game_time,
    home_away,
    result
  )
  SELECT
    CAST(player_id AS bigint),
    player_name,
    CAST(team_id_resolved AS TEXT),
    team_id_resolved,
    CAST(game_id AS bigint),
    game_date::date,
    prop_type,
    prop_value,
    over_under,
    status,
    outcome,
    predicted_outcome,
    confidence_score,
    prediction_timestamp,
    was_correct,
    'user_added',
    created_at,
    NOW(),
    position,
    is_home,
    is_pitcher,
    opponent,
    opponent_encoded,
    opponent_team_id,
    prop_value,
    game_time,
    home_away,
    result_numeric
  FROM eligible
  ON CONFLICT (player_id, game_id, prop_type, prop_source)
  DO UPDATE SET
    player_name = EXCLUDED.player_name,
    team = EXCLUDED.team,
    team_id = EXCLUDED.team_id,
    game_date = EXCLUDED.game_date,
    prop_value = EXCLUDED.prop_value,
    over_under = EXCLUDED.over_under,
    status = EXCLUDED.status,
    outcome = EXCLUDED.outcome,
    predicted_outcome = EXCLUDED.predicted_outcome,
    confidence_score = EXCLUDED.confidence_score,
    prediction_timestamp = EXCLUDED.prediction_timestamp,
    was_correct = EXCLUDED.was_correct,
    updated_at = NOW(),
    position = EXCLUDED.position,
    is_home = EXCLUDED.is_home,
    is_pitcher = EXCLUDED.is_pitcher,
    opponent = EXCLUDED.opponent,
    opponent_encoded = EXCLUDED.opponent_encoded,
    opponent_team_id = EXCLUDED.opponent_team_id,
    line = EXCLUDED.line,
    game_time = EXCLUDED.game_time,
    home_away = EXCLUDED.home_away,
    result = EXCLUDED.result
  RETURNING 1
)
SELECT
  (SELECT COUNT(*)::int FROM missing) AS missing_total,
  (SELECT COUNT(*)::int FROM eligible) AS eligible_total,
  (SELECT COUNT(*)::int FROM missing WHERE team_id_resolved IS NULL) AS skipped_missing_team_id,
  (SELECT COUNT(*)::int FROM upserted) AS upserted_total
        """,
        params,
    ) or {}
    return {
        "missing_total": int(row.get("missing_total") or 0),
        "eligible_total": int(row.get("eligible_total") or 0),
        "skipped_missing_team_id": int(row.get("skipped_missing_team_id") or 0),
        "upserted_total": int(row.get("upserted_total") or 0),
    }


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Backfill graded user_added rows missing in model_training_props."
    )
    ap.add_argument("--from-date", default=None, help="Optional YYYY-MM-DD lower bound on player_props.game_date.")
    ap.add_argument("--to-date", default=None, help="Optional YYYY-MM-DD upper bound on player_props.game_date.")
    ap.add_argument("--apply", action="store_true", help="Apply changes. Default is preview-only.")
    args = ap.parse_args(list(argv) if argv is not None else sys.argv[1:])

    preview = _preview(args.from_date, args.to_date)
    payload: dict[str, object] = {
        "ok": True,
        "status": "preview",
        "from_date": args.from_date,
        "to_date": args.to_date,
        "preview": preview,
    }

    if args.apply:
        applied = _apply(args.from_date, args.to_date)
        payload["status"] = "applied"
        payload["applied"] = applied

    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
