#!/usr/bin/env python3
"""Report balance-vs-accuracy diagnostics for degenerate MLB prop lanes."""

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


def _window_clause(window_mode: str) -> str:
    if window_mode == "games":
        return """
  AND mt.game_date::date IN (
    SELECT DISTINCT game_date::date
    FROM model_training_props
    WHERE game_date IS NOT NULL
      AND lower(trim(outcome)) IN ('win','loss')
    ORDER BY game_date::date DESC
    LIMIT %s::int
  )
"""
    return "  AND mt.game_date::date >= (CURRENT_DATE - (%s::int || ' days')::interval)::date\n"


def _candidate_grid(prop_type: str, default_expectation: float) -> list[Dict[str, Any]]:
    rows = [
        {"candidate": "m0.00", "margin": 0.00},
        {"candidate": "m0.25", "margin": 0.25},
        {"candidate": "m0.50", "margin": 0.50},
        {"candidate": "m0.75", "margin": 0.75},
    ]
    if prop_type in {"runs_scored", "home_runs", "stolen_bases"}:
        rows = [
            {"candidate": "m0.00", "margin": 0.00},
            {"candidate": "m0.10", "margin": 0.10},
            {"candidate": "m0.20", "margin": 0.20},
            {"candidate": "m0.30", "margin": 0.30},
        ]
    return rows


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


def _scan_prop(
    *,
    prop_type: str,
    window_mode: str,
    window_value: int,
    balance_floor_pct: float,
    default_expectation: float,
) -> Dict[str, Any]:
    line_expr = _line_expr(prop_type)
    exp_expr = _expectation_expr(prop_type, default_expectation)
    candidates = _candidate_grid(prop_type, default_expectation)
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
  FROM model_training_props mt
  LEFT JOIN prop_features_precomputed pf
    ON pf.player_id = mt.player_id
   AND pf.game_id = mt.game_id
   AND pf.prop_type = %s
  LEFT JOIN player_derived_stats pds
    ON pds.player_id = mt.player_id
   AND pds.game_id = mt.game_id
  WHERE mt.prop_type = %s
    AND mt.prop_source = 'mlb_api'
    AND lower(trim(mt.outcome)) IN ('win','loss')
{_window_clause(window_mode)}
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
        (str(prop_type), str(prop_type), int(window_value)),
    )
    best_balanced = None
    for row in rows:
        over_pct = float(row.get("over_pct") or 0.0)
        if over_pct >= float(balance_floor_pct):
            best_balanced = row
            break
    return _json_safe(
        {
        "prop_type": prop_type,
        "window_mode": window_mode,
        "window_value": int(window_value),
        "balance_floor_pct": float(balance_floor_pct),
        "best_candidate_any": rows[0] if rows else None,
        "best_candidate_balanced": best_balanced,
        "candidates": rows,
        "status": "pass" if best_balanced is not None else "degenerate",
        }
    )


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Report MLB degenerate-lane balance vs accuracy diagnostics.")
    ap.add_argument("--window-mode", choices=["days", "games"], default="games")
    ap.add_argument("--window-days", type=int, default=30)
    ap.add_argument("--games-back", type=int, default=30)
    ap.add_argument(
        "--prop-types",
        default="runs_scored,walks_allowed,outs_recorded,home_runs,runs_rbis",
        help="Comma-separated MLB prop types to diagnose.",
    )
    ap.add_argument(
        "--balance-floor-pct",
        type=float,
        default=20.0,
        help="Minimum over-side percent required to treat a candidate as balanced.",
    )
    args = ap.parse_args(list(argv) if argv is not None else sys.argv[1:])

    window_mode = "games" if str(args.window_mode).lower() == "games" else "days"
    window_value = int(args.games_back) if window_mode == "games" else int(args.window_days)
    props = [p.strip() for p in str(args.prop_types).split(",") if p.strip()]
    defaults = {
        "runs_scored": 0.6,
        "walks_allowed": 1.5,
        "outs_recorded": 15.5,
        "home_runs": 0.2,
        "runs_rbis": 1.2,
    }

    rows = []
    for prop in props:
        rows.append(
            _scan_prop(
                prop_type=prop,
                window_mode=window_mode,
                window_value=max(1, int(window_value)),
                balance_floor_pct=float(args.balance_floor_pct),
                default_expectation=float(defaults.get(prop, 1.0)),
            )
        )
    degenerate_props = [r["prop_type"] for r in rows if r.get("status") != "pass"]
    payload = _json_safe(
        {
        "ok": len(degenerate_props) == 0,
        "status": "pass" if len(degenerate_props) == 0 else "degraded",
        "window_mode": window_mode,
        "window_value": int(window_value),
        "balance_floor_pct": float(args.balance_floor_pct),
        "degenerate_props": degenerate_props,
        "rows": rows,
        }
    )
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
