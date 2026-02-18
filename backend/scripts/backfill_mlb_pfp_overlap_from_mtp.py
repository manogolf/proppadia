#!/usr/bin/env python3
"""Backfill missing prop_features_precomputed rows from reconciled training rows.

This script closes overlap gaps between model_training_props and
prop_features_precomputed by creating missing PFP rows keyed by:
  (prop_type, player_id, game_id, feature_set_tag)

Feature payloads are sourced from player_derived_stats rolling columns
(d7_*, d15_*, d30_*), which are the primary non-leaky rolling features used
in training diagnostics.
"""

from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from typing import Any, Dict, List, Sequence

from backend.shared.db.pg import pg_connect, pg_fetchall

_DEFAULT_PROP_TYPES = (
    "hits,total_bases,strikeouts_batting,earned_runs,doubles,hits_allowed,"
    "strikeouts_pitching,walks,hits_runs_rbis,runs_scored,walks_allowed,runs_rbis"
)

_ROLLING_FEATURE_COLUMNS = [
    "d7_doubles",
    "d7_earned_runs",
    "d7_hits",
    "d7_hits_allowed",
    "d7_hits_runs_rbis",
    "d7_home_runs",
    "d7_outs_recorded",
    "d7_rbis",
    "d7_runs_rbis",
    "d7_runs_scored",
    "d7_singles",
    "d7_stolen_bases",
    "d7_strikeouts_batting",
    "d7_strikeouts_pitching",
    "d7_total_bases",
    "d7_triples",
    "d7_walks",
    "d7_walks_allowed",
    "d15_doubles",
    "d15_earned_runs",
    "d15_hits",
    "d15_hits_allowed",
    "d15_hits_runs_rbis",
    "d15_home_runs",
    "d15_outs_recorded",
    "d15_rbis",
    "d15_runs_rbis",
    "d15_runs_scored",
    "d15_singles",
    "d15_stolen_bases",
    "d15_strikeouts_batting",
    "d15_strikeouts_pitching",
    "d15_total_bases",
    "d15_triples",
    "d15_walks",
    "d15_walks_allowed",
    "d30_doubles",
    "d30_earned_runs",
    "d30_hits",
    "d30_hits_allowed",
    "d30_hits_runs_rbis",
    "d30_home_runs",
    "d30_outs_recorded",
    "d30_rbis",
    "d30_runs_rbis",
    "d30_runs_scored",
    "d30_singles",
    "d30_stolen_bases",
    "d30_strikeouts_batting",
    "d30_strikeouts_pitching",
    "d30_total_bases",
    "d30_triples",
    "d30_walks",
    "d30_walks_allowed",
]


def _csv(v: str) -> list[str]:
    return [x.strip() for x in str(v or "").split(",") if x.strip()]


def _window_clause(mode: str) -> str:
    if mode == "games":
        return """
  WHERE game_day IN (
    SELECT DISTINCT game_day
    FROM mt_base
    ORDER BY game_day DESC
    LIMIT %s
  )
"""
    return "  WHERE game_day >= (CURRENT_DATE - (%s || ' days')::interval)::date\n"


def _fetch_missing_rows(
    *,
    prop_types: Sequence[str],
    prop_source: str,
    feature_set_tag: str,
    window_mode: str,
    window_value: int,
    from_date: str | None,
    to_date: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    placeholders = ", ".join(["%s"] * len(prop_types))
    extra_filters: list[str] = []
    params: list[Any] = [str(prop_source), *[str(p) for p in prop_types]]
    if from_date:
        extra_filters.append("AND game_date::date >= %s::date")
        params.append(str(from_date))
    if to_date:
        extra_filters.append("AND game_date::date <= %s::date")
        params.append(str(to_date))
    extra_filter_sql = "\n    " + "\n    ".join(extra_filters) if extra_filters else ""

    sql = f"""
WITH mt_base AS (
  SELECT
    prop_type,
    player_id,
    game_id,
    game_date::date AS game_day
  FROM model_training_props
  WHERE prop_source = %s
    AND prop_type IN ({placeholders})
    AND game_date IS NOT NULL
    AND lower(trim(outcome)) IN ('win','loss')
    {extra_filter_sql}
),
mt_win AS (
  SELECT *
  FROM mt_base
{_window_clause(window_mode)}
),
missing AS (
  SELECT
    m.prop_type,
    m.player_id,
    m.game_id,
    m.game_day
  FROM mt_win m
  LEFT JOIN prop_features_precomputed p
    ON p.prop_type = m.prop_type
   AND p.player_id = m.player_id
   AND p.game_id = m.game_id
   AND p.feature_set_tag = %s
  WHERE p.player_id IS NULL
),
joined AS (
  SELECT
    m.prop_type,
    m.player_id,
    m.game_id,
    m.game_day,
    pds.{", pds.".join(_ROLLING_FEATURE_COLUMNS)}
  FROM missing m
  LEFT JOIN player_derived_stats pds
    ON pds.player_id = m.player_id
   AND pds.game_id = m.game_id
)
SELECT *
FROM joined
ORDER BY game_day DESC, prop_type, player_id, game_id
{"LIMIT %s" if limit > 0 else ""}
"""

    params.append(int(window_value))
    params.append(str(feature_set_tag))
    if limit > 0:
        params.append(int(limit))
    return pg_fetchall(sql, tuple(params))


def _to_feature_dict(row: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for col in _ROLLING_FEATURE_COLUMNS:
        value = row.get(col)
        if value is None:
            continue
        try:
            f = float(value)
        except Exception:
            continue
        if not math.isfinite(f):
            continue
        out[col] = f
    return out


def _upsert_rows(
    *,
    rows: Sequence[dict[str, Any]],
    feature_set_tag: str,
    model_tag: str,
    batch_size: int,
) -> int:
    if not rows:
        return 0
    sql = """
INSERT INTO prop_features_precomputed (
  prop_type,
  player_id,
  game_id,
  game_date,
  features,
  feature_set_tag,
  model_tag,
  computed_at
)
VALUES (%s, %s, %s, %s::date, %s::jsonb, %s, %s, NOW())
ON CONFLICT (prop_type, player_id, game_id, feature_set_tag)
DO UPDATE SET
  game_date = EXCLUDED.game_date,
  features = EXCLUDED.features,
  model_tag = EXCLUDED.model_tag,
  computed_at = NOW()
"""
    total = 0
    with pg_connect() as conn:
        with conn.cursor() as cur:
            buf: list[tuple[Any, ...]] = []
            for row in rows:
                features = _to_feature_dict(row)
                buf.append(
                    (
                        str(row["prop_type"]),
                        int(row["player_id"]),
                        int(row["game_id"]),
                        str(row["game_day"]),
                        json.dumps(features, separators=(",", ":")),
                        str(feature_set_tag),
                        str(model_tag),
                    )
                )
                if len(buf) >= batch_size:
                    cur.executemany(sql, buf)
                    total += len(buf)
                    buf = []
            if buf:
                cur.executemany(sql, buf)
                total += len(buf)
        conn.commit()
    return total


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Backfill missing PFP rows from reconciled MTP rows.")
    ap.add_argument("--prop-types", default=_DEFAULT_PROP_TYPES)
    ap.add_argument("--prop-source", default="mlb_api")
    ap.add_argument("--feature-set-tag", default="v1")
    ap.add_argument("--model-tag", default="mtp_overlap_backfill_v1")
    ap.add_argument("--window-mode", choices=["games", "days"], default="games")
    ap.add_argument("--games-back", type=int, default=30)
    ap.add_argument("--window-days", type=int, default=120)
    ap.add_argument("--from-date")
    ap.add_argument("--to-date")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--batch-size", type=int, default=1000)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args(list(argv) if argv is not None else None)

    prop_types = _csv(args.prop_types)
    if not prop_types:
        payload = {"ok": False, "status": "fail", "failures": ["no_prop_types"]}
        print(json.dumps(payload, indent=2))
        return 2

    window_value = int(args.games_back if args.window_mode == "games" else args.window_days)
    rows = _fetch_missing_rows(
        prop_types=prop_types,
        prop_source=str(args.prop_source),
        feature_set_tag=str(args.feature_set_tag),
        window_mode=str(args.window_mode),
        window_value=window_value,
        from_date=args.from_date,
        to_date=args.to_date,
        limit=int(args.limit),
    )

    by_prop = defaultdict(int)
    with_rolling = 0
    for r in rows:
        p = str(r["prop_type"])
        by_prop[p] += 1
        if _to_feature_dict(r):
            with_rolling += 1

    written = 0
    if args.apply:
        written = _upsert_rows(
            rows=rows,
            feature_set_tag=str(args.feature_set_tag),
            model_tag=str(args.model_tag),
            batch_size=max(1, int(args.batch_size)),
        )

    payload = {
        "ok": True,
        "status": "pass",
        "apply": bool(args.apply),
        "window_mode": str(args.window_mode),
        "window_value": int(window_value),
        "prop_types": prop_types,
        "prop_source": str(args.prop_source),
        "feature_set_tag": str(args.feature_set_tag),
        "model_tag": str(args.model_tag),
        "from_date": args.from_date,
        "to_date": args.to_date,
        "limit": int(args.limit),
        "missing_rows_found": len(rows),
        "missing_rows_with_rolling_features": int(with_rolling),
        "written_rows": int(written),
        "by_prop": dict(sorted(by_prop.items())),
    }
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
