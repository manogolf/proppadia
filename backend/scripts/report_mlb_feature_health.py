#!/usr/bin/env python3
"""Report feature-source health for MLB prop lanes."""

from __future__ import annotations

import argparse
import json
import re
from typing import Any, Dict, Sequence

from backend.shared.db.pg import pg_fetchall, pg_fetchone

_PROP_RE = re.compile(r"^[a-z0-9_]+$")
_NUMERIC_RE = r"^-?[0-9]+(\.[0-9]+)?$"


def _window_clause(window_mode: str, window_value: int) -> str:
    if window_mode == "games":
        return """
WHERE game_day IN (
  SELECT DISTINCT game_day
  FROM mt
  ORDER BY game_day DESC
  LIMIT """ + str(int(window_value)) + """
)
"""
    return (
        "WHERE game_day >= (CURRENT_DATE - ("
        + str(int(window_value))
        + " || ' days')::interval)::date"
    )


def _load_pds_d7_columns() -> set[str]:
    rows = pg_fetchall(
        """
SELECT column_name
FROM information_schema.columns
WHERE table_schema='public'
  AND table_name='player_derived_stats'
  AND column_name LIKE 'd7_%%'
"""
    )
    return {str(r.get("column_name") or "") for r in rows}


def _pf_candidate_keys(prop_type: str) -> list[str]:
    direct = f"d7_{prop_type}"
    fallback = {
        "hits_runs_rbis": [direct, "d7_hits", "d7_rbis"],
        "runs_rbis": [direct, "d7_rbis", "d7_hits"],
        "runs_scored": [direct, "d7_hits", "d7_walks", "d7_home_runs"],
    }
    return fallback.get(prop_type, [direct])


def _source_mix_for_prop(
    *,
    prop_type: str,
    window_mode: str,
    window_value: int,
    prop_sources: Sequence[str],
    pds_columns: set[str],
) -> Dict[str, Any]:
    placeholders = ", ".join(["%s"] * len(prop_sources))
    pds_col = f"d7_{prop_type}"
    pds_has_column = pds_col in pds_columns
    pds_expr = "NULL::numeric AS pds_d7_stat"
    if pds_has_column:
        pds_expr = f"MAX({pds_col})::numeric AS pds_d7_stat"
    pf_keys = _pf_candidate_keys(prop_type)
    pf_raw_expr = "COALESCE(" + ", ".join([f"NULLIF(features->>'{k}', '')" for k in pf_keys]) + ")"

    row = pg_fetchone(
        f"""
WITH mt AS (
  SELECT
    player_id,
    game_id,
    game_date::date AS game_day
  FROM model_training_props
  WHERE prop_type = %s
    AND prop_source IN ({placeholders})
    AND game_date IS NOT NULL
    AND lower(trim(outcome)) IN ('win','loss')
),
win AS (
  SELECT *
  FROM mt
"""
        + _window_clause(window_mode, int(window_value))
        + f"""
),
pf AS (
  SELECT
    player_id,
    game_id,
    MAX(
      CASE
        WHEN {pf_raw_expr} ~ %s THEN ({pf_raw_expr})::numeric
        ELSE NULL
      END
    ) AS pf_d7_stat
  FROM prop_features_precomputed
  WHERE prop_type=%s
  GROUP BY player_id, game_id
),
pds AS (
  SELECT
    player_id,
    game_id,
    """
        + pds_expr
        + """
  FROM player_derived_stats
  GROUP BY player_id, game_id
),
labeled AS (
  SELECT
    w.player_id,
    w.game_id,
    pf.pf_d7_stat,
    pds.pds_d7_stat,
    CASE
      WHEN pf.pf_d7_stat IS NOT NULL THEN 'pf_d7_stat'
      WHEN pds.pds_d7_stat IS NOT NULL THEN 'pds_d7_stat'
      ELSE 'default_1_0'
    END AS expectation_source
  FROM win w
  LEFT JOIN pf USING (player_id, game_id)
  LEFT JOIN pds USING (player_id, game_id)
),
agg AS (
  SELECT
    COUNT(*)::int AS total_rows,
    COUNT(*) FILTER (WHERE expectation_source='pf_d7_stat')::int AS pf_rows,
    COUNT(*) FILTER (WHERE expectation_source='pds_d7_stat')::int AS pds_rows,
    COUNT(*) FILTER (WHERE expectation_source='default_1_0')::int AS default_rows,
    COUNT(*) FILTER (WHERE pf_d7_stat IS NOT NULL AND pds_d7_stat IS NOT NULL)::int AS both_rows
  FROM labeled
)
SELECT
  total_rows,
  pf_rows,
  pds_rows,
  default_rows,
  both_rows,
  ROUND(100.0 * pf_rows::numeric / NULLIF(total_rows,0), 2) AS pf_pct,
  ROUND(100.0 * pds_rows::numeric / NULLIF(total_rows,0), 2) AS pds_pct,
  ROUND(100.0 * default_rows::numeric / NULLIF(total_rows,0), 2) AS default_pct,
  ROUND(100.0 * both_rows::numeric / NULLIF(total_rows,0), 2) AS both_pct
FROM agg
""",
        (
            str(prop_type),
            *prop_sources,
            _NUMERIC_RE,
            str(prop_type),
        ),
    ) or {}

    return {
        "prop_type": prop_type,
        "window_mode": window_mode,
        "window_value": int(window_value),
        "pds_has_column": bool(pds_has_column),
        "pf_candidate_keys": pf_keys,
        "total_rows": int(row.get("total_rows") or 0),
        "pf_rows": int(row.get("pf_rows") or 0),
        "pds_rows": int(row.get("pds_rows") or 0),
        "default_rows": int(row.get("default_rows") or 0),
        "both_rows": int(row.get("both_rows") or 0),
        "pf_pct": float(row.get("pf_pct")) if row.get("pf_pct") is not None else None,
        "pds_pct": float(row.get("pds_pct")) if row.get("pds_pct") is not None else None,
        "default_pct": float(row.get("default_pct")) if row.get("default_pct") is not None else None,
        "both_pct": float(row.get("both_pct")) if row.get("both_pct") is not None else None,
    }


def collect(
    *,
    window_mode: str,
    window_value: int,
    prop_types: Sequence[str],
    prop_sources: Sequence[str],
    warn_default_pct: float,
    warn_min_rows: int,
    fail_on_warn: bool,
) -> Dict[str, Any]:
    pds_cols = _load_pds_d7_columns()
    rows: list[Dict[str, Any]] = []
    warnings: list[str] = []

    for prop_type in prop_types:
        mix = _source_mix_for_prop(
            prop_type=prop_type,
            window_mode=window_mode,
            window_value=window_value,
            prop_sources=prop_sources,
            pds_columns=pds_cols,
        )
        rows.append(mix)
        total_rows = int(mix.get("total_rows") or 0)
        default_pct = mix.get("default_pct")
        if total_rows == 0:
            warnings.append(f"{prop_type}:no_rows_in_window")
            continue
        if total_rows >= warn_min_rows and default_pct is not None and float(default_pct) > warn_default_pct:
            warnings.append(
                f"{prop_type}:high_default_pct:{float(default_pct):.2f}>{float(warn_default_pct):.2f}"
            )
        if not bool(mix.get("pds_has_column")):
            warnings.append(f"{prop_type}:missing_pds_column:d7_{prop_type}")

    total_rows_all = sum(int(r.get("total_rows") or 0) for r in rows)
    pf_rows_all = sum(int(r.get("pf_rows") or 0) for r in rows)
    pds_rows_all = sum(int(r.get("pds_rows") or 0) for r in rows)
    default_rows_all = sum(int(r.get("default_rows") or 0) for r in rows)
    ok = (not warnings) or (not fail_on_warn)

    return {
        "ok": bool(ok),
        "status": "pass" if ok else "fail",
        "window_mode": window_mode,
        "window_value": int(window_value),
        "prop_sources": list(prop_sources),
        "warn_default_pct": float(warn_default_pct),
        "warn_min_rows": int(warn_min_rows),
        "fail_on_warn": bool(fail_on_warn),
        "summary": {
            "prop_types": len(rows),
            "total_rows": total_rows_all,
            "pf_rows": pf_rows_all,
            "pds_rows": pds_rows_all,
            "default_rows": default_rows_all,
            "pf_pct": round(100.0 * pf_rows_all / total_rows_all, 2) if total_rows_all else None,
            "pds_pct": round(100.0 * pds_rows_all / total_rows_all, 2) if total_rows_all else None,
            "default_pct": round(100.0 * default_rows_all / total_rows_all, 2) if total_rows_all else None,
        },
        "rows": rows,
        "warnings": warnings,
    }


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Report MLB feature-source health for prop lanes.")
    ap.add_argument("--window-mode", choices=["days", "games"], default="games")
    ap.add_argument("--window-days", type=int, default=120)
    ap.add_argument("--games-back", type=int, default=30)
    ap.add_argument("--prop-types", default="hits,total_bases,strikeouts_batting")
    ap.add_argument("--prop-sources", default="mlb_api")
    ap.add_argument("--warn-default-pct", type=float, default=35.0)
    ap.add_argument("--warn-min-rows", type=int, default=200)
    ap.add_argument("--fail-on-warn", action="store_true")
    args = ap.parse_args(list(argv) if argv is not None else None)

    prop_types = [p.strip() for p in str(args.prop_types).split(",") if p.strip()]
    bad_props = [p for p in prop_types if not _PROP_RE.match(p)]
    if bad_props:
        payload = {
            "ok": False,
            "status": "fail",
            "warnings": [],
            "failures": [f"invalid_prop_type:{p}" for p in bad_props],
        }
        print(json.dumps(payload, indent=2))
        return 2
    prop_sources = [s.strip() for s in str(args.prop_sources).split(",") if s.strip()]
    mode = "games" if str(args.window_mode).lower() == "games" else "days"
    window_value = int(args.games_back if mode == "games" else args.window_days)

    payload = collect(
        window_mode=mode,
        window_value=max(1, window_value),
        prop_types=prop_types,
        prop_sources=prop_sources,
        warn_default_pct=float(args.warn_default_pct),
        warn_min_rows=max(1, int(args.warn_min_rows)),
        fail_on_warn=bool(args.fail_on_warn),
    )
    print(json.dumps(payload, indent=2))
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
