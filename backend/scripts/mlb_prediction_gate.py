#!/usr/bin/env python3
"""Combined MLB prediction gate: operability + quality thresholds."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Sequence

from backend.scripts.analyze_mlb_prediction_quality import collect_quality
from backend.scripts.api_client_utils import ClientAdapter, HttpClient, InProcessClient
from backend.scripts.probe_mlb_prediction_readiness import collect_probe

DEFAULT_PROP_TYPES = "hits,total_bases,strikeouts_batting"


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Run MLB prediction gate (operability + quality).")
    ap.add_argument("--base-url", default=None, help="Use running backend URL instead of in-process app.")
    ap.add_argument("--date", default="2025-08-15")
    ap.add_argument("--sample-size", type=int, default=10)
    ap.add_argument("--require-min-success", type=int, default=1)
    ap.add_argument("--prop-types", default=DEFAULT_PROP_TYPES)
    ap.add_argument("--quality-window-days", type=int, default=120)
    ap.add_argument("--quality-window-mode", choices=["days", "games"], default="days")
    ap.add_argument("--quality-games-back", type=int, default=30)
    ap.add_argument("--quality-min-total", type=int, default=1)
    ap.add_argument("--quality-min-accuracy", type=float, default=0.0)
    ap.add_argument("--quality-prop-sources", default="mlb_api")
    args = ap.parse_args(list(argv) if argv is not None else sys.argv[1:])

    prop_types = [p.strip() for p in str(args.prop_types).split(",") if p.strip()]
    client: ClientAdapter = HttpClient(args.base_url) if args.base_url else InProcessClient()

    operability = collect_probe(
        client,
        game_date=args.date,
        sample_size=int(args.sample_size),
        require_min_success=int(args.require_min_success),
        prop_types=prop_types,
    )
    quality_window_value = (
        int(args.quality_games_back)
        if str(args.quality_window_mode) == "games"
        else int(args.quality_window_days)
    )
    quality_prop_sources = [s.strip().lower() for s in str(args.quality_prop_sources).split(",") if s.strip()]
    quality = collect_quality(
        str(args.quality_window_mode),
        quality_window_value,
        prop_types=prop_types,
        prop_sources=quality_prop_sources,
    )
    overall = quality.get("overall") or {}
    q_total = int(overall.get("total") or 0)
    q_acc = overall.get("accuracy_pct")

    quality_ok = q_total >= int(args.quality_min_total)
    if q_acc is not None:
        quality_ok = quality_ok and float(q_acc) >= float(args.quality_min_accuracy)
    else:
        quality_ok = False if float(args.quality_min_accuracy) > 0 else quality_ok

    ok = bool(operability.get("ok")) and quality_ok
    operability_degraded = []
    per_prop = operability.get("per_prop") or {}
    for prop, stats in per_prop.items():
        attempts = int((stats or {}).get("attempts") or 0)
        success = int((stats or {}).get("predict_success") or 0)
        failures = int((stats or {}).get("failure_count") or 0)
        if failures > 0 or (attempts > 0 and success < attempts):
            operability_degraded.append(
                {
                    "prop_type": str(prop),
                    "attempts": attempts,
                    "predict_success": success,
                    "failure_count": failures,
                    "reason": "operability_failures",
                }
            )

    quality_degraded = []
    for row in (quality.get("by_prop") or []):
        prop = str(row.get("prop_type") or "").strip()
        if not prop:
            continue
        total = int(row.get("total") or 0)
        acc = row.get("accuracy_pct")
        if total <= 0:
            quality_degraded.append({"prop_type": prop, "reason": "quality_no_volume", "total": total})
            continue
        if acc is not None and float(acc) < float(args.quality_min_accuracy):
            quality_degraded.append(
                {
                    "prop_type": prop,
                    "reason": "quality_accuracy_below_threshold",
                    "accuracy_pct": float(acc),
                    "min_accuracy_pct": float(args.quality_min_accuracy),
                    "total": total,
                }
            )

    degraded_prop_lanes = operability_degraded + quality_degraded
    payload = {
        "ok": ok,
        "status": "pass" if ok else "fail",
        "degraded_prop_lanes": degraded_prop_lanes,
        "operability": operability,
        "quality": {
            "ok": quality_ok,
            "min_total": int(args.quality_min_total),
            "min_accuracy_pct": float(args.quality_min_accuracy),
            "summary": quality,
        },
    }
    print(json.dumps(payload, indent=2, default=str))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
