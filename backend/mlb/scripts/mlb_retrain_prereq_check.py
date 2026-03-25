#!/usr/bin/env python3
"""Run MLB retrain prerequisites as one checklist payload."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from backend.mlb.scripts import analyze_mlb_prediction_quality
from backend.shared.scripts import json_check_runner
from backend.mlb.scripts import report_mlb_prop_coverage
from backend.scripts import season_baseline_last
from backend.mlb.scripts import validate_mlb_stat_derived_recent


def _safe_run_json_check(fn: Any, args: list[str], check_name: str) -> tuple[int, dict[str, Any]]:
    try:
        return json_check_runner.run_json_check(fn, args)
    except Exception as exc:
        return (
            1,
            {
                "ok": False,
                "status": "fail",
                "error": f"{check_name}: {type(exc).__name__}: {exc}",
            },
        )


def _mlb_baseline_check(*, baseline_dir: str, max_age_hours: int) -> dict[str, Any]:
    payload = season_baseline_last.build_payload(Path(baseline_dir))
    latest_mlb = ((payload.get("latest") or {}).get("mlb") or {})
    exists = bool(latest_mlb.get("exists"))
    age_hours = latest_mlb.get("age_hours")
    path = latest_mlb.get("path")

    errors: list[str] = []
    if not exists:
        errors.append("missing_mlb_baseline")
    if exists and int(max_age_hours) > 0:
        age = float(age_hours) if age_hours is not None else None
        if age is None or age > float(max_age_hours):
            errors.append("mlb_baseline_stale")

    ok = len(errors) == 0
    return {
        "ok": ok,
        "status": "pass" if ok else "fail",
        "baseline_dir": baseline_dir,
        "max_age_hours": int(max_age_hours),
        "mlb_exists": exists,
        "mlb_path": path,
        "mlb_age_hours": age_hours,
        "errors": errors,
    }


def collect_retrain_prereq_checklist(
    *,
    freshness_days: int,
    freshness_min_rows: int,
    coverage_window_mode: str,
    coverage_window_days: int,
    coverage_games_back: int,
    coverage_required_props: str,
    coverage_min_training_source_per_prop: int,
    coverage_training_prop_sources: str,
    grading_window_mode: str,
    grading_window_days: int,
    grading_games_back: int,
    grading_prop_types: str,
    grading_min_total: int,
    baseline_dir: str,
    baseline_max_age_hours: int,
) -> dict[str, Any]:
    freshness_rc, freshness_payload = _safe_run_json_check(
        validate_mlb_stat_derived_recent.main,
        [
            "--days",
            str(max(1, int(freshness_days))),
            "--require-min",
            str(max(0, int(freshness_min_rows))),
            "--json",
        ],
        "data_freshness",
    )
    coverage_rc, coverage_payload = _safe_run_json_check(
        report_mlb_prop_coverage.main,
        [
            "--window-mode",
            str(coverage_window_mode),
            "--window-days",
            str(max(1, int(coverage_window_days))),
            "--games-back",
            str(max(1, int(coverage_games_back))),
            "--required-props",
            str(coverage_required_props),
            "--min-graded-per-prop",
            str(max(0, int(coverage_min_training_source_per_prop))),
            "--gate-metric",
            "training_source",
            "--training-prop-sources",
            str(coverage_training_prop_sources),
        ],
        "prop_coverage",
    )
    grading_rc, grading_payload = _safe_run_json_check(
        analyze_mlb_prediction_quality.main,
        [
            "--window-mode",
            str(grading_window_mode),
            "--window-days",
            str(max(1, int(grading_window_days))),
            "--games-back",
            str(max(1, int(grading_games_back))),
            "--prop-types",
            str(grading_prop_types),
            "--source-table",
            "model_training_props",
            "--min-total",
            str(max(0, int(grading_min_total))),
        ],
        "grading_completeness",
    )
    baseline_payload = _mlb_baseline_check(
        baseline_dir=baseline_dir,
        max_age_hours=max(0, int(baseline_max_age_hours)),
    )
    baseline_rc = 0 if baseline_payload.get("ok") else 1

    checks = [
        {
            "name": "data_freshness",
            "ok": bool(freshness_payload.get("status") == "pass"),
            "status": freshness_payload.get("status"),
            "exit_code": int(freshness_rc),
            "payload": freshness_payload,
        },
        {
            "name": "prop_coverage",
            "ok": bool(coverage_payload.get("ok")),
            "status": coverage_payload.get("status"),
            "exit_code": int(coverage_rc),
            "payload": coverage_payload,
        },
        {
            "name": "grading_completeness",
            "ok": bool(grading_payload.get("ok")),
            "status": grading_payload.get("status"),
            "exit_code": int(grading_rc),
            "payload": grading_payload,
        },
        {
            "name": "baseline_comparison_availability",
            "ok": bool(baseline_payload.get("ok")),
            "status": baseline_payload.get("status"),
            "exit_code": int(baseline_rc),
            "payload": baseline_payload,
        },
    ]

    failures = [item["name"] for item in checks if (item["exit_code"] != 0) or (not item["ok"])]
    ok = len(failures) == 0
    return {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "ok": ok,
        "status": "pass" if ok else "fail",
        "failures": failures,
        "checks": checks,
    }


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Run MLB retrain prerequisites checklist (JSON).")
    ap.add_argument("--freshness-days", type=int, default=7)
    ap.add_argument("--freshness-min-rows", type=int, default=1)
    ap.add_argument("--coverage-window-mode", choices=["days", "games"], default="games")
    ap.add_argument("--coverage-window-days", type=int, default=30)
    ap.add_argument("--coverage-games-back", type=int, default=30)
    ap.add_argument("--coverage-required-props", default="")
    ap.add_argument("--coverage-min-training-source-per-prop", type=int, default=20)
    ap.add_argument("--coverage-training-prop-sources", default="mlb_api,user_added")
    ap.add_argument("--grading-window-mode", choices=["days", "games"], default="games")
    ap.add_argument("--grading-window-days", type=int, default=30)
    ap.add_argument("--grading-games-back", type=int, default=30)
    ap.add_argument("--grading-prop-types", default="")
    ap.add_argument("--grading-min-total", type=int, default=1000)
    ap.add_argument("--baseline-dir", default="artifacts/season_baselines")
    ap.add_argument("--baseline-max-age-hours", type=int, default=0, help="0 disables staleness check.")
    args = ap.parse_args(list(argv) if argv is not None else sys.argv[1:])

    payload = collect_retrain_prereq_checklist(
        freshness_days=args.freshness_days,
        freshness_min_rows=args.freshness_min_rows,
        coverage_window_mode=args.coverage_window_mode,
        coverage_window_days=args.coverage_window_days,
        coverage_games_back=args.coverage_games_back,
        coverage_required_props=args.coverage_required_props,
        coverage_min_training_source_per_prop=args.coverage_min_training_source_per_prop,
        coverage_training_prop_sources=args.coverage_training_prop_sources,
        grading_window_mode=args.grading_window_mode,
        grading_window_days=args.grading_window_days,
        grading_games_back=args.grading_games_back,
        grading_prop_types=args.grading_prop_types,
        grading_min_total=args.grading_min_total,
        baseline_dir=args.baseline_dir,
        baseline_max_age_hours=args.baseline_max_age_hours,
    )
    print(json.dumps(payload, indent=2))
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
