#!/usr/bin/env python3
"""Guard one MLB prop lane against one-sided drift and quality drop."""

from __future__ import annotations

import argparse
import json
from typing import Any, Dict, Sequence

from backend.shared.db.pg import pg_fetchall


def _window_clause(window_mode: str) -> str:
    if window_mode == "games":
        return """
  AND game_date::date IN (
    SELECT DISTINCT game_date::date
    FROM model_training_props
    WHERE game_date IS NOT NULL
      AND lower(trim(outcome)) IN ('win','loss')
    ORDER BY game_date::date DESC
    LIMIT %s::int
  )
"""
    return "  AND game_date::date >= (CURRENT_DATE - (%s::int || ' days')::interval)::date\n"


def _collect(
    *,
    prop_type: str,
    prop_sources: Sequence[str],
    window_mode: str,
    window_value: int,
) -> Dict[str, Any]:
    placeholders = ", ".join(["%s"] * len(prop_sources))
    rows = pg_fetchall(
        f"""
SELECT
  COUNT(*)::int AS total,
  COUNT(*) FILTER (WHERE lower(trim(outcome))='win')::int AS correct,
  COUNT(*) FILTER (WHERE lower(trim(over_under))='over')::int AS over_n,
  COUNT(*) FILTER (WHERE lower(trim(over_under))='under')::int AS under_n
FROM model_training_props
WHERE prop_type = %s
  AND prop_source IN ({placeholders})
  AND lower(trim(outcome)) IN ('win','loss')
{_window_clause(window_mode)}
""",
        (str(prop_type), *prop_sources, int(window_value)),
    )
    row = (rows or [{}])[0]
    total = int(row.get("total") or 0)
    correct = int(row.get("correct") or 0)
    over_n = int(row.get("over_n") or 0)
    under_n = int(row.get("under_n") or 0)
    return {
        "total": total,
        "correct": correct,
        "accuracy_pct": round((100.0 * correct / total), 2) if total > 0 else None,
        "over_n": over_n,
        "under_n": under_n,
        "over_pct": round((100.0 * over_n / total), 2) if total > 0 else None,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Guard one MLB prop lane for balance and quality.")
    ap.add_argument("--prop-type", default="runs_scored")
    ap.add_argument("--prop-sources", default="mlb_api")
    ap.add_argument("--window-mode", choices=["days", "games"], default="games")
    ap.add_argument("--window-days", type=int, default=30)
    ap.add_argument("--games-back", type=int, default=30)
    ap.add_argument("--min-total", type=int, default=1000)
    ap.add_argument("--min-accuracy-pct", type=float, default=48.0)
    ap.add_argument("--min-over-pct", type=float, default=10.0)
    args = ap.parse_args()

    prop_sources = [s.strip() for s in str(args.prop_sources).split(",") if s.strip()]
    window_mode = "games" if str(args.window_mode).lower() == "games" else "days"
    window_value = int(args.games_back) if window_mode == "games" else int(args.window_days)
    metrics = _collect(
        prop_type=str(args.prop_type),
        prop_sources=prop_sources,
        window_mode=window_mode,
        window_value=max(1, int(window_value)),
    )

    failures = []
    if int(metrics["total"]) < int(args.min_total):
        failures.append(
            {
                "reason": "min_total_not_met",
                "total": metrics["total"],
                "min_total": int(args.min_total),
            }
        )
    if (metrics.get("accuracy_pct") or 0.0) < float(args.min_accuracy_pct):
        failures.append(
            {
                "reason": "accuracy_below_floor",
                "accuracy_pct": metrics.get("accuracy_pct"),
                "min_accuracy_pct": float(args.min_accuracy_pct),
            }
        )
    if (metrics.get("over_pct") or 0.0) < float(args.min_over_pct):
        failures.append(
            {
                "reason": "over_pct_below_floor",
                "over_pct": metrics.get("over_pct"),
                "min_over_pct": float(args.min_over_pct),
            }
        )

    payload = {
        "ok": len(failures) == 0,
        "status": "pass" if len(failures) == 0 else "degraded",
        "prop_type": str(args.prop_type),
        "prop_sources": prop_sources,
        "window_mode": window_mode,
        "window_value": int(window_value),
        "min_total": int(args.min_total),
        "min_accuracy_pct": float(args.min_accuracy_pct),
        "min_over_pct": float(args.min_over_pct),
        "metrics": metrics,
        "failures": failures,
    }
    print(json.dumps(payload, indent=2))
    return 0 if payload["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
