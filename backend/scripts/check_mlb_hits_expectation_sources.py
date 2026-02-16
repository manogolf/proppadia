#!/usr/bin/env python3
"""Guardrails for MLB hits expectation source policy and source-mix reporting."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Sequence

from backend.shared.db.pg import pg_fetchone

_TARGET_FILES = [
    "backend/mlb/v2_write_mlb_api_labels_to_mtp.py",
    "backend/mlb/v2_backfill_mlb_api_training.py",
    "backend/mlb/v2_write_training_from_pfp.py",
]
_FORBIDDEN_TOKEN = "rolling_result_avg_7"


def _window_clause(window_mode: str) -> str:
    if window_mode == "games":
        return """
WHERE game_day IN (
  SELECT DISTINCT game_day
  FROM mt
  WHERE lower(trim(outcome)) IN ('win','loss')
  ORDER BY game_day DESC
  LIMIT %s::int
)
"""
    return "WHERE game_day >= (CURRENT_DATE - (%s::int || ' days')::interval)::date"


def _scan_forbidden_token(root: Path) -> dict[str, Any]:
    files_with_token: list[dict[str, Any]] = []
    for rel in _TARGET_FILES:
        p = root / rel
        if not p.exists():
            files_with_token.append({"file": rel, "error": "missing_file"})
            continue
        text = p.read_text(encoding="utf-8")
        if _FORBIDDEN_TOKEN not in text:
            continue
        lines: list[int] = []
        for i, line in enumerate(text.splitlines(), start=1):
            if _FORBIDDEN_TOKEN in line:
                lines.append(i)
        files_with_token.append({"file": rel, "lines": lines})
    return {
        "forbidden_token": _FORBIDDEN_TOKEN,
        "violations": files_with_token,
        "ok": len(files_with_token) == 0,
    }


def _source_mix(window_mode: str, window_value: int) -> dict[str, Any]:
    row = pg_fetchone(
        """
WITH mt AS (
  SELECT
    player_id,
    game_id,
    game_date::date AS game_day,
    outcome
  FROM model_training_props
  WHERE prop_source='mlb_api'
    AND prop_type='hits'
    AND game_date IS NOT NULL
),
win AS (
  SELECT *
  FROM mt
"""
        + _window_clause(window_mode)
        + """
),
pf AS (
  SELECT
    player_id,
    game_id,
    MAX(NULLIF(features->>'d7_hits','')::numeric) AS pf_d7_hits
  FROM prop_features_precomputed
  WHERE prop_type='hits'
  GROUP BY player_id, game_id
),
pds AS (
  SELECT
    player_id,
    game_id,
    MAX(d7_hits)::numeric AS pds_d7_hits
  FROM player_derived_stats
  GROUP BY player_id, game_id
),
labeled AS (
  SELECT
    w.player_id,
    w.game_id,
    CASE
      WHEN pf.pf_d7_hits IS NOT NULL THEN 'pf_d7_hits'
      WHEN pds.pds_d7_hits IS NOT NULL THEN 'pds_d7_hits'
      ELSE 'default_1_0'
    END AS expectation_source
  FROM win w
  LEFT JOIN pf USING (player_id, game_id)
  LEFT JOIN pds USING (player_id, game_id)
),
agg AS (
  SELECT
    COUNT(*)::int AS total_rows,
    COUNT(*) FILTER (WHERE expectation_source='pf_d7_hits')::int AS pf_d7_hits_rows,
    COUNT(*) FILTER (WHERE expectation_source='pds_d7_hits')::int AS pds_d7_hits_rows,
    COUNT(*) FILTER (WHERE expectation_source='default_1_0')::int AS default_rows
  FROM labeled
)
SELECT
  total_rows,
  pf_d7_hits_rows,
  pds_d7_hits_rows,
  default_rows,
  ROUND(100.0 * pf_d7_hits_rows::numeric / NULLIF(total_rows,0), 2) AS pf_d7_hits_pct,
  ROUND(100.0 * pds_d7_hits_rows::numeric / NULLIF(total_rows,0), 2) AS pds_d7_hits_pct,
  ROUND(100.0 * default_rows::numeric / NULLIF(total_rows,0), 2) AS default_pct
FROM agg
""",
        (int(window_value),),
    ) or {}

    total = int(row.get("total_rows") or 0)
    return {
        "window_mode": window_mode,
        "window_value": int(window_value),
        "total_rows": total,
        "pf_d7_hits_rows": int(row.get("pf_d7_hits_rows") or 0),
        "pds_d7_hits_rows": int(row.get("pds_d7_hits_rows") or 0),
        "default_rows": int(row.get("default_rows") or 0),
        "pf_d7_hits_pct": float(row.get("pf_d7_hits_pct")) if row.get("pf_d7_hits_pct") is not None else None,
        "pds_d7_hits_pct": float(row.get("pds_d7_hits_pct")) if row.get("pds_d7_hits_pct") is not None else None,
        "default_pct": float(row.get("default_pct")) if row.get("default_pct") is not None else None,
    }


def collect(window_mode: str, window_value: int) -> dict[str, Any]:
    root = Path(__file__).resolve().parents[2]
    forbidden = _scan_forbidden_token(root)
    source_mix = _source_mix(window_mode, window_value)
    warnings: list[str] = []
    if int(source_mix.get("total_rows") or 0) == 0:
        warnings.append("no_hits_rows_in_window")

    ok = bool(forbidden.get("ok"))
    return {
        "ok": ok,
        "status": "pass" if ok else "fail",
        "policy": forbidden,
        "source_mix": source_mix,
        "warnings": warnings,
    }


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="MLB hits expectation source guard + source mix report.")
    ap.add_argument("--window-mode", choices=["days", "games"], default="games")
    ap.add_argument("--window-days", type=int, default=30)
    ap.add_argument("--games-back", type=int, default=30)
    args = ap.parse_args(list(argv) if argv is not None else None)

    mode = "games" if str(args.window_mode).lower() == "games" else "days"
    value = int(args.games_back if mode == "games" else args.window_days)
    payload = collect(mode, value)
    print(json.dumps(payload, indent=2))
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())

