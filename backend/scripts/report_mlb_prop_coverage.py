#!/usr/bin/env python3
"""Report recent MLB prop coverage across prediction and training tables."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Dict, Sequence

from backend.shared.db.pg import pg_fetchall


def _player_props(window_days: int) -> list[Dict[str, Any]]:
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
        WHERE game_date >= (CURRENT_DATE - (%s::int || ' days')::interval)::date
        GROUP BY prop_type
        ORDER BY total_predictions DESC, prop_type
        """,
        (int(window_days),),
    )
    return list(rows or [])


def _stat_derived(window_days: int) -> dict[str, int]:
    rows = pg_fetchall(
        """
        SELECT
          prop_type,
          COUNT(*)::int AS stat_derived_count
        FROM model_training_props
        WHERE prop_source = 'stat_derived'
          AND game_date >= (CURRENT_DATE - (%s::int || ' days')::interval)::date
        GROUP BY prop_type
        """,
        (int(window_days),),
    )
    out: dict[str, int] = {}
    for row in rows or []:
        prop = str(row.get("prop_type") or "").strip()
        if not prop:
            continue
        out[prop] = int(row.get("stat_derived_count") or 0)
    return out


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Report recent MLB prop coverage.")
    ap.add_argument("--window-days", type=int, default=30)
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
    args = ap.parse_args(list(argv) if argv is not None else sys.argv[1:])

    window_days = max(1, int(args.window_days))
    required_props = [p.strip() for p in str(args.required_props).split(",") if p.strip()]
    min_graded = max(0, int(args.min_graded_per_prop))

    pred_rows = _player_props(window_days)
    stat_counts = _stat_derived(window_days)

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
            "stat_derived_count": int(stat_counts.get(prop, 0)),
        }

    # Include stat-derived-only props that might not yet be in player_props window.
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
                "stat_derived_count": int(n),
            },
        )

    rows = sorted(
        by_prop.values(),
        key=lambda r: (int(r["graded_count"]), int(r["total_predictions"]), r["prop_type"]),
        reverse=True,
    )

    missing_required = [p for p in required_props if p not in by_prop]
    under_min_required = [
        p for p in required_props if p in by_prop and int(by_prop[p].get("graded_count") or 0) < min_graded
    ]

    total_predictions = sum(int(r["total_predictions"]) for r in rows)
    total_graded = sum(int(r["graded_count"]) for r in rows)
    total_stat_derived = sum(int(r["stat_derived_count"]) for r in rows)

    ok = not missing_required and not under_min_required
    payload = {
        "ok": ok,
        "status": "pass" if ok else "fail",
        "window_days": window_days,
        "required_props": required_props,
        "min_graded_per_prop": min_graded,
        "missing_required_props": missing_required,
        "under_min_required_props": under_min_required,
        "summary": {
            "prop_types": len(rows),
            "total_predictions": total_predictions,
            "total_graded": total_graded,
            "total_stat_derived": total_stat_derived,
        },
        "rows": rows,
    }
    print(json.dumps(payload, indent=2))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
