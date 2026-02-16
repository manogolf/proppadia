#!/usr/bin/env python3
"""Run MLB prediction gate + flow audit + prop coverage as one JSON check."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from typing import Any, Sequence

from backend.scripts import audit_mlb_prediction_flow
from backend.scripts import check_mlb_hits_expectation_sources
from backend.scripts import mlb_prediction_gate
from backend.scripts import report_mlb_prop_coverage
from backend.scripts.json_check_runner import run_json_check

DEFAULT_PROP_TYPES = "hits,total_bases,strikeouts_batting"


def _degraded_prop_lanes(
    gate_payload: dict[str, Any], coverage_payload: dict[str, Any]
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in gate_payload.get("degraded_prop_lanes") or []:
        if not isinstance(item, dict):
            continue
        prop = str(item.get("prop_type") or "").strip()
        reason = str(item.get("reason") or "").strip() or "unknown"
        if not prop:
            continue
        key = (prop, reason)
        if key in seen:
            continue
        seen.add(key)
        out.append(item)

    for prop in coverage_payload.get("missing_required_props") or []:
        p = str(prop).strip()
        if not p:
            continue
        key = (p, "coverage_missing_required")
        if key in seen:
            continue
        seen.add(key)
        out.append({"prop_type": p, "reason": "coverage_missing_required"})

    for prop in coverage_payload.get("under_min_required_props") or []:
        p = str(prop).strip()
        if not p:
            continue
        key = (p, "coverage_under_min_threshold")
        if key in seen:
            continue
        seen.add(key)
        out.append({"prop_type": p, "reason": "coverage_under_min_threshold"})

    return out


def collect_pipeline_check(
    *,
    base_url: str | None,
    date: str,
    sample_size: int,
    require_min_success: int,
    prop_types: str,
    quality_window_mode: str,
    quality_window_days: int,
    quality_games_back: int,
    quality_min_total: int,
    quality_min_accuracy: float,
    quality_prop_sources: str,
    coverage_window_mode: str,
    coverage_window_days: int,
    coverage_games_back: int,
    coverage_required_props: str,
    coverage_min_graded_per_prop: int,
    coverage_gate_metric: str,
    coverage_training_prop_sources: str,
) -> dict[str, Any]:
    gate_args: list[str] = [
        "--date",
        str(date),
        "--sample-size",
        str(int(sample_size)),
        "--require-min-success",
        str(int(require_min_success)),
        "--prop-types",
        str(prop_types),
        "--quality-window-mode",
        str(quality_window_mode),
        "--quality-window-days",
        str(int(quality_window_days)),
        "--quality-games-back",
        str(int(quality_games_back)),
        "--quality-min-total",
        str(int(quality_min_total)),
        "--quality-min-accuracy",
        str(float(quality_min_accuracy)),
        "--quality-prop-sources",
        str(quality_prop_sources),
    ]
    if base_url:
        gate_args.extend(["--base-url", str(base_url)])

    flow_args = [
        "--window-mode",
        str(quality_window_mode),
        "--window-days",
        str(int(quality_window_days)),
        "--games-back",
        str(int(quality_games_back)),
        "--json",
    ]
    coverage_args = [
        "--window-mode",
        str(coverage_window_mode),
        "--window-days",
        str(int(coverage_window_days)),
        "--games-back",
        str(int(coverage_games_back)),
        "--required-props",
        str(coverage_required_props),
        "--min-graded-per-prop",
        str(int(coverage_min_graded_per_prop)),
        "--gate-metric",
        str(coverage_gate_metric),
        "--training-prop-sources",
        str(coverage_training_prop_sources),
    ]
    hits_expectation_guard_args = [
        "--window-mode",
        str(quality_window_mode),
        "--window-days",
        str(int(quality_window_days)),
        "--games-back",
        str(int(quality_games_back)),
    ]

    gate_rc, gate_payload = run_json_check(mlb_prediction_gate.main, gate_args)
    flow_rc, flow_payload = run_json_check(audit_mlb_prediction_flow.main, flow_args)
    coverage_rc, coverage_payload = run_json_check(report_mlb_prop_coverage.main, coverage_args)
    hits_expectation_guard_rc, hits_expectation_guard_payload = run_json_check(
        check_mlb_hits_expectation_sources.main, hits_expectation_guard_args
    )

    checks = [
        {
            "name": "prediction_gate",
            "ok": bool(gate_payload.get("ok")),
            "status": gate_payload.get("status"),
            "exit_code": int(gate_rc),
            "payload": gate_payload,
        },
        {
            "name": "prediction_flow_audit",
            "ok": bool(flow_payload.get("ok")),
            "status": flow_payload.get("status"),
            "exit_code": int(flow_rc),
            "payload": flow_payload,
        },
        {
            "name": "prop_coverage",
            "ok": bool(coverage_payload.get("ok")),
            "status": coverage_payload.get("status"),
            "exit_code": int(coverage_rc),
            "payload": coverage_payload,
        },
        {
            "name": "hits_expectation_sources",
            "ok": bool(hits_expectation_guard_payload.get("ok")),
            "status": hits_expectation_guard_payload.get("status"),
            "exit_code": int(hits_expectation_guard_rc),
            "payload": hits_expectation_guard_payload,
        },
    ]
    failures = [item["name"] for item in checks if (item["exit_code"] != 0) or (not item["ok"])]
    degraded_prop_lanes = _degraded_prop_lanes(gate_payload, coverage_payload)
    ok = len(failures) == 0
    return {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "ok": ok,
        "status": "pass" if ok else "fail",
        "failures": failures,
        "degraded_prop_lanes": degraded_prop_lanes,
        "checks": checks,
    }


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Run MLB gate + flow + coverage as one JSON check.")
    ap.add_argument("--base-url", default=None, help="Use running backend URL for prediction gate probe calls.")
    ap.add_argument("--date", default="2025-08-15")
    ap.add_argument("--sample-size", type=int, default=10)
    ap.add_argument("--require-min-success", type=int, default=1)
    ap.add_argument("--prop-types", default=DEFAULT_PROP_TYPES)
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

    payload = collect_pipeline_check(
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
    print(json.dumps(payload, indent=2))
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
