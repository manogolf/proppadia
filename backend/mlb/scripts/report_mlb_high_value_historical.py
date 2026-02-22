#!/usr/bin/env python3
"""Historical multi-season diagnostics for underserved MLB prop lanes."""

from __future__ import annotations

import argparse
import json
import sys
from decimal import Decimal
from typing import Any, Dict, Sequence

from backend.shared.db.pg import pg_fetchall


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    return value


def _line_expr(prop_type: str) -> str:
    if prop_type in {"outs_recorded"}:
        return (
            "CASE WHEN exp_val < 12.0 THEN 11.5 WHEN exp_val < 15.0 THEN 14.5 "
            "WHEN exp_val < 18.0 THEN 17.5 ELSE 20.5 END"
        )
    if prop_type in {"runs_scored", "home_runs", "stolen_bases"}:
        return "0.5"
    return "CASE WHEN exp_val < 1.0 THEN 0.5 WHEN exp_val < 2.0 THEN 1.5 WHEN exp_val < 3.0 THEN 2.5 ELSE 3.5 END"


def _expectation_expr(prop_type: str, default_expectation: float) -> str:
    return (
        f"COALESCE(NULLIF(pf.features->>'d7_{prop_type}','')::numeric, "
        f"pds.d7_{prop_type}::numeric, {float(default_expectation)})"
    )


def _candidate_grid(prop_type: str) -> list[Dict[str, Any]]:
    if prop_type in {"runs_scored", "home_runs", "stolen_bases"}:
        return [
            {"candidate": "m0.00", "margin": 0.00},
            {"candidate": "m0.10", "margin": 0.10},
            {"candidate": "m0.20", "margin": 0.20},
            {"candidate": "m0.30", "margin": 0.30},
        ]
    return [
        {"candidate": "m0.00", "margin": 0.00},
        {"candidate": "m0.25", "margin": 0.25},
        {"candidate": "m0.50", "margin": 0.50},
        {"candidate": "m0.75", "margin": 0.75},
    ]


def _load_seasons(prop_types: Sequence[str], prop_sources: Sequence[str], limit: int) -> list[int]:
    if not prop_types:
        return []
    prop_placeholders = ", ".join(["%s"] * len(prop_types))
    source_placeholders = ", ".join(["%s"] * len(prop_sources))
    rows = pg_fetchall(
        f"""
        SELECT DISTINCT EXTRACT(YEAR FROM game_date)::int AS season
        FROM mlb.model_training_props
        WHERE game_date IS NOT NULL
          AND prop_type IN ({prop_placeholders})
          AND prop_source IN ({source_placeholders})
          AND lower(trim(outcome)) IN ('win','loss')
        ORDER BY season DESC
        LIMIT %s::int
        """,
        tuple(prop_types) + tuple(prop_sources) + (int(limit),),
    )
    return [int(r.get("season")) for r in (rows or []) if r.get("season") is not None]


def _current_stats(
    *,
    prop_type: str,
    season: int,
    prop_sources: Sequence[str],
) -> Dict[str, Any]:
    source_placeholders = ", ".join(["%s"] * len(prop_sources))
    rows = pg_fetchall(
        f"""
        SELECT
          COUNT(*)::int AS total,
          COUNT(*) FILTER (WHERE lower(trim(outcome))='win')::int AS correct,
          COUNT(*) FILTER (WHERE lower(trim(over_under))='over')::int AS over_n,
          COUNT(*) FILTER (WHERE lower(trim(over_under))='under')::int AS under_n
        FROM mlb.model_training_props
        WHERE prop_type = %s
          AND EXTRACT(YEAR FROM game_date)::int = %s::int
          AND prop_source IN ({source_placeholders})
          AND lower(trim(outcome)) IN ('win','loss')
        """,
        (str(prop_type), int(season), *prop_sources),
    )
    row = (rows or [{}])[0]
    total = int(row.get("total") or 0)
    correct = int(row.get("correct") or 0)
    over_n = int(row.get("over_n") or 0)
    under_n = int(row.get("under_n") or 0)
    accuracy_pct = round((100.0 * correct / total), 2) if total > 0 else None
    over_pct = round((100.0 * over_n / total), 2) if total > 0 else None
    return {
        "total": total,
        "correct": correct,
        "accuracy_pct": accuracy_pct,
        "over_n": over_n,
        "under_n": under_n,
        "over_pct": over_pct,
    }


def _candidate_scan(
    *,
    prop_type: str,
    season: int,
    default_expectation: float,
    prop_sources: Sequence[str],
) -> list[Dict[str, Any]]:
    line_expr = _line_expr(prop_type)
    exp_expr = _expectation_expr(prop_type, default_expectation)
    candidates = _candidate_grid(prop_type)
    source_placeholders = ", ".join(["%s"] * len(prop_sources))
    candidate_rows_sql = ",\n".join(
        [f"('{row['candidate']}', {float(row['margin'])}::numeric)" for row in candidates]
    )
    rows = pg_fetchall(
        f"""
WITH base AS (
  SELECT
    mt.id,
    mt.prop_value::numeric AS actual,
    {exp_expr} AS exp_val
  FROM mlb.model_training_props mt
  LEFT JOIN mlb.prop_features_precomputed pf
    ON pf.player_id = mt.player_id
   AND pf.game_id = mt.game_id
   AND pf.prop_type = %s
  LEFT JOIN mlb.player_derived_stats pds
    ON pds.player_id = mt.player_id
   AND pds.game_id = mt.game_id
  WHERE mt.prop_type = %s
    AND EXTRACT(YEAR FROM mt.game_date)::int = %s::int
    AND mt.prop_source IN ({source_placeholders})
    AND lower(trim(mt.outcome)) IN ('win','loss')
),
candidates AS (
  SELECT * FROM (VALUES
{candidate_rows_sql}
  ) v(candidate, margin)
),
scored AS (
  SELECT
    c.candidate,
    b.actual,
    ({line_expr})::numeric AS line_new,
    CASE WHEN b.exp_val >= ({line_expr})::numeric + c.margin THEN 'over' ELSE 'under' END AS side_new
  FROM base b
  CROSS JOIN candidates c
),
agg AS (
  SELECT
    candidate,
    COUNT(*)::int AS total,
    COUNT(*) FILTER (
      WHERE (side_new='over' AND actual > line_new)
         OR (side_new='under' AND actual < line_new)
    )::int AS correct,
    COUNT(*) FILTER (WHERE side_new='over')::int AS over_n,
    COUNT(*) FILTER (WHERE side_new='under')::int AS under_n
  FROM scored
  GROUP BY candidate
)
SELECT
  candidate,
  total,
  correct,
  ROUND(100.0 * correct::numeric / NULLIF(total,0), 2) AS accuracy_pct,
  over_n,
  under_n,
  ROUND(100.0 * over_n::numeric / NULLIF(total,0), 2) AS over_pct
FROM agg
ORDER BY accuracy_pct DESC, candidate
""",
        (str(prop_type), str(prop_type), int(season), *prop_sources),
    )
    return list(rows or [])


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Historical diagnostics for underserved MLB prop lanes.")
    ap.add_argument(
        "--prop-types",
        default="runs_scored,walks_allowed,outs_recorded,home_runs,runs_rbis",
        help="Comma-separated underserved props to analyze.",
    )
    ap.add_argument(
        "--prop-sources",
        default="mlb_api",
        help="Comma-separated model_training_props.prop_source values to include.",
    )
    ap.add_argument(
        "--seasons",
        default="",
        help="Comma-separated season years (YYYY). Empty = latest --season-count seasons.",
    )
    ap.add_argument("--season-count", type=int, default=3, help="Used when --seasons is empty.")
    ap.add_argument(
        "--balance-floor-pct",
        type=float,
        default=20.0,
        help="Minimum over percentage to mark candidate as balanced.",
    )
    args = ap.parse_args(list(argv) if argv is not None else sys.argv[1:])

    prop_types = [p.strip() for p in str(args.prop_types).split(",") if p.strip()]
    prop_sources = [s.strip() for s in str(args.prop_sources).split(",") if s.strip()]
    if not prop_types:
        print(json.dumps({"ok": False, "status": "fail", "error": "no_prop_types"}, indent=2))
        return 1
    if not prop_sources:
        print(json.dumps({"ok": False, "status": "fail", "error": "no_prop_sources"}, indent=2))
        return 1

    seasons = [int(s.strip()) for s in str(args.seasons).split(",") if s.strip()]
    if not seasons:
        seasons = _load_seasons(prop_types, prop_sources, max(1, int(args.season_count)))
    seasons = sorted({int(s) for s in seasons}, reverse=True)

    defaults = {
        "runs_scored": 0.6,
        "walks_allowed": 1.5,
        "outs_recorded": 15.5,
        "home_runs": 0.2,
        "runs_rbis": 1.2,
    }

    rows: list[Dict[str, Any]] = []
    by_prop_rollup: dict[str, dict[str, Any]] = {
        p: {"prop_type": p, "seasons_total": len(seasons), "seasons_balanced": 0} for p in prop_types
    }
    for season in seasons:
        season_rows: list[Dict[str, Any]] = []
        for prop in prop_types:
            current = _current_stats(prop_type=prop, season=season, prop_sources=prop_sources)
            candidates = _candidate_scan(
                prop_type=prop,
                season=season,
                default_expectation=float(defaults.get(prop, 1.0)),
                prop_sources=prop_sources,
            )
            best_any = candidates[0] if candidates else None
            best_balanced = None
            for cand in candidates:
                over_pct = float(cand.get("over_pct") or 0.0)
                if over_pct >= float(args.balance_floor_pct):
                    best_balanced = cand
                    break
            season_rows.append(
                {
                    "prop_type": prop,
                    "current": current,
                    "best_candidate_any": best_any,
                    "best_candidate_balanced": best_balanced,
                    "status": "pass" if best_balanced is not None else "degenerate",
                }
            )
            if best_balanced is not None:
                by_prop_rollup[prop]["seasons_balanced"] = int(by_prop_rollup[prop]["seasons_balanced"]) + 1
        rows.append({"season": int(season), "props": season_rows})

    prop_rollup = []
    for prop in prop_types:
        item = by_prop_rollup[prop]
        seasons_total = int(item["seasons_total"])
        seasons_balanced = int(item["seasons_balanced"])
        prop_rollup.append(
            {
                "prop_type": prop,
                "seasons_total": seasons_total,
                "seasons_balanced": seasons_balanced,
                "promotable_balanced_all_seasons": seasons_total > 0 and seasons_balanced == seasons_total,
            }
        )

    payload = _json_safe(
        {
            "ok": True,
            "status": "pass",
            "lane_group": "underserved_props",
            "prop_types": prop_types,
            "prop_sources": prop_sources,
            "seasons": seasons,
            "balance_floor_pct": float(args.balance_floor_pct),
            "prop_rollup": prop_rollup,
            "rows": rows,
        }
    )
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
