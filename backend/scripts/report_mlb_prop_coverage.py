#!/usr/bin/env python3
"""Report recent MLB prop coverage across prediction and training tables."""

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


def _player_props(window_value: int, window_mode: str) -> list[Dict[str, Any]]:
    rows = pg_fetchall(
        """
        SELECT
          prop_type,
          COUNT(*)::int AS total_predictions,
          COUNT(*) FILTER (WHERE lower(trim(outcome)) IN ('win','loss','push','dnp'))::int AS resolved_count,
          COUNT(*) FILTER (WHERE lower(trim(outcome)) IN ('win','loss'))::int AS graded_count,
          COUNT(*) FILTER (WHERE lower(trim(outcome)) = 'win')::int AS wins,
          COUNT(*) FILTER (WHERE lower(trim(outcome)) = 'loss')::int AS losses,
          COUNT(*) FILTER (WHERE lower(trim(outcome)) = 'push')::int AS pushes,
          COUNT(*) FILTER (WHERE lower(trim(outcome)) = 'dnp')::int AS dnps
        FROM player_props
        """
        + _date_filter("game_date", "player_props", window_mode)
        + """
        GROUP BY prop_type
        ORDER BY total_predictions DESC, prop_type
        """,
        (int(window_value),),
    )
    return list(rows or [])


def _training_source_counts(window_value: int, window_mode: str, prop_sources: Sequence[str]) -> dict[str, int]:
    sources = [s.strip() for s in prop_sources if str(s).strip()]
    if not sources:
        return {}
    source_placeholders = ", ".join(["%s"] * len(sources))
    rows = pg_fetchall(
        f"""
        SELECT
          prop_type,
          COUNT(*)::int AS training_source_count
        FROM model_training_props
        WHERE prop_source IN ({source_placeholders})
        """
        + _date_filter("game_date", "model_training_props", window_mode).replace("WHERE", "AND", 1)
        + """
        GROUP BY prop_type
        """,
        tuple(sources) + (int(window_value),),
    )
    out: dict[str, int] = {}
    for row in rows or []:
        prop = str(row.get("prop_type") or "").strip()
        if not prop:
            continue
        out[prop] = int(row.get("training_source_count") or 0)
    return out


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Report recent MLB prop coverage.")
    ap.add_argument("--window-mode", choices=["days", "games"], default="days")
    ap.add_argument("--window-days", type=int, default=30)
    ap.add_argument("--games-back", type=int, default=30)
    ap.add_argument(
        "--required-props",
        default="",
        help="Comma-separated prop types expected to be present.",
    )
    ap.add_argument(
        "--min-graded-per-prop",
        type=int,
        default=0,
        help="Fail when a required prop has fewer graded rows than this threshold.",
    )
    ap.add_argument(
        "--gate-metric",
        choices=["graded", "training_source", "stat_derived"],
        default="graded",
        help="Metric used for required-prop threshold checks.",
    )
    ap.add_argument(
        "--training-prop-sources",
        default="mlb_api",
        help="Comma-separated model_training_props.prop_source values used for training-depth counts.",
    )
    args = ap.parse_args(list(argv) if argv is not None else sys.argv[1:])

    window_mode = "games" if str(args.window_mode).lower() == "games" else "days"
    window_value = max(1, int(args.games_back if window_mode == "games" else args.window_days))
    required_props = [p.strip() for p in str(args.required_props).split(",") if p.strip()]
    min_graded = max(0, int(args.min_graded_per_prop))
    gate_metric_raw = str(args.gate_metric).strip().lower()
    gate_metric = "training_source" if gate_metric_raw == "stat_derived" else gate_metric_raw
    training_prop_sources = [s.strip() for s in str(args.training_prop_sources).split(",") if s.strip()]

    pred_rows = _player_props(window_value, window_mode)
    stat_counts = _training_source_counts(window_value, window_mode, training_prop_sources)

    by_prop: dict[str, Dict[str, Any]] = {}
    for row in pred_rows:
        prop = str(row.get("prop_type") or "").strip()
        if not prop:
            continue
        by_prop[prop] = {
            "prop_type": prop,
            "total_predictions": int(row.get("total_predictions") or 0),
            "resolved_count": int(row.get("resolved_count") or 0),
            "graded_count": int(row.get("graded_count") or 0),
            "wins": int(row.get("wins") or 0),
            "losses": int(row.get("losses") or 0),
            "pushes": int(row.get("pushes") or 0),
            "dnps": int(row.get("dnps") or 0),
            "training_source_count": int(stat_counts.get(prop, 0)),
        }

    # Include training-source-only props that might not yet be in player_props window.
    for prop, n in stat_counts.items():
        by_prop.setdefault(
            prop,
            {
                "prop_type": prop,
                "total_predictions": 0,
                "resolved_count": 0,
                "graded_count": 0,
                "wins": 0,
                "losses": 0,
                "pushes": 0,
                "dnps": 0,
                "training_source_count": int(n),
            },
        )

    rows = sorted(
        by_prop.values(),
        key=lambda r: (int(r["graded_count"]), int(r["total_predictions"]), r["prop_type"]),
        reverse=True,
    )

    missing_required = [p for p in required_props if p not in by_prop]
    under_min_required = []
    for p in required_props:
        if p not in by_prop:
            continue
        row = by_prop[p]
        threshold_value = int(row.get("graded_count") or 0)
        if gate_metric == "training_source":
            threshold_value = int(row.get("training_source_count") or 0)
        if threshold_value < min_graded:
            under_min_required.append(p)

    total_predictions = sum(int(r["total_predictions"]) for r in rows)
    total_graded = sum(int(r["graded_count"]) for r in rows)
    total_training_source = sum(int(r["training_source_count"]) for r in rows)

    ok = not missing_required and not under_min_required
    payload = {
        "ok": ok,
        "status": "pass" if ok else "fail",
        "window_mode": window_mode,
        "window_value": window_value,
        "required_props": required_props,
        "min_graded_per_prop": min_graded,
        "gate_metric": gate_metric,
        "training_prop_sources": training_prop_sources,
        "missing_required_props": missing_required,
        "under_min_required_props": under_min_required,
        "summary": {
            "prop_types": len(rows),
            "total_predictions": total_predictions,
            "total_graded": total_graded,
            "total_training_source": total_training_source,
        },
        "rows": rows,
    }
    print(json.dumps(payload, indent=2))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
