#!/usr/bin/env python3
"""Append MLB pipeline check snapshots to JSONL history."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from backend.scripts import mlb_pipeline_check


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Log MLB pipeline check result to JSONL history.")
    ap.add_argument("--output", default="artifacts/mlb_pipeline_history.jsonl")
    ap.add_argument("--base-url", default=None)
    ap.add_argument("--date", default="2025-08-15")
    ap.add_argument("--sample-size", type=int, default=10)
    ap.add_argument("--require-min-success", type=int, default=1)
    ap.add_argument("--prop-types", default=mlb_pipeline_check.DEFAULT_PROP_TYPES)
    ap.add_argument("--quality-window-mode", choices=["days", "games"], default="days")
    ap.add_argument("--quality-window-days", type=int, default=120)
    ap.add_argument("--quality-games-back", type=int, default=30)
    ap.add_argument("--quality-min-total", type=int, default=1)
    ap.add_argument("--quality-min-accuracy", type=float, default=0.0)
    ap.add_argument("--quality-prop-sources", default="mlb_api")
    ap.add_argument("--coverage-window-mode", choices=["days", "games"], default="days")
    ap.add_argument("--coverage-window-days", type=int, default=30)
    ap.add_argument("--coverage-games-back", type=int, default=30)
    ap.add_argument("--coverage-required-props", default="")
    ap.add_argument("--coverage-min-graded-per-prop", type=int, default=0)
    ap.add_argument(
        "--coverage-gate-metric",
        choices=["graded", "training_source", "stat_derived"],
        default="graded",
    )
    ap.add_argument("--coverage-training-prop-sources", default="mlb_api")
    args = ap.parse_args(list(argv) if argv is not None else sys.argv[1:])

    payload = mlb_pipeline_check.collect_pipeline_check(
        base_url=args.base_url,
        date=args.date,
        sample_size=args.sample_size,
        require_min_success=args.require_min_success,
        prop_types=args.prop_types,
        quality_window_mode=args.quality_window_mode,
        quality_window_days=args.quality_window_days,
        quality_games_back=args.quality_games_back,
        quality_min_total=args.quality_min_total,
        quality_min_accuracy=args.quality_min_accuracy,
        quality_prop_sources=args.quality_prop_sources,
        coverage_window_mode=args.coverage_window_mode,
        coverage_window_days=args.coverage_window_days,
        coverage_games_back=args.coverage_games_back,
        coverage_required_props=args.coverage_required_props,
        coverage_min_graded_per_prop=args.coverage_min_graded_per_prop,
        coverage_gate_metric=args.coverage_gate_metric,
        coverage_training_prop_sources=args.coverage_training_prop_sources,
    )

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, default=str))
        fh.write("\n")

    summary = {
        "captured_at": payload.get("captured_at"),
        "status": payload.get("status"),
        "ok": payload.get("ok"),
        "output": str(out_path),
        "failures": payload.get("failures") or [],
    }
    print(json.dumps(summary, indent=2))
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
