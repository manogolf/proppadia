#!/usr/bin/env python3
"""Compare candidate MLB quality vs baseline artifact and emit promotion recommendation."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from backend.mlb.scripts import analyze_mlb_prediction_quality


def _latest_baseline_file(root: Path) -> Path | None:
    files = sorted(root.glob("mlb_quality_*.json"), key=lambda p: p.stat().st_mtime)
    return files[-1] if files else None


def _load_json(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("baseline payload must be a JSON object")
    return raw


def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _to_int(v: Any) -> int:
    try:
        return int(v)
    except Exception:
        return 0


def _pick_baseline(path_arg: str | None, baseline_dir: str) -> Path:
    if path_arg:
        path = Path(path_arg)
        if not path.exists():
            raise FileNotFoundError(f"baseline file not found: {path}")
        return path
    latest = _latest_baseline_file(Path(baseline_dir))
    if latest is None:
        raise FileNotFoundError(f"no mlb baseline artifacts found in: {baseline_dir}")
    return latest


def evaluate_candidate(
    *,
    baseline_payload: dict[str, Any],
    candidate_payload: dict[str, Any],
    required_props: Sequence[str],
    min_overall_lift_pct: float,
    max_prop_drop_pct: float,
    min_candidate_total: int,
    min_baseline_prop_total_for_drop: int,
) -> dict[str, Any]:
    base_overall = baseline_payload.get("overall") or {}
    cand_overall = candidate_payload.get("overall") or {}
    base_total = _to_int(base_overall.get("total"))
    cand_total = _to_int(cand_overall.get("total"))
    base_acc = _to_float(base_overall.get("accuracy_pct"))
    cand_acc = _to_float(cand_overall.get("accuracy_pct"))
    overall_lift = None if base_acc is None or cand_acc is None else round(cand_acc - base_acc, 2)

    required = [str(p).strip() for p in required_props if str(p).strip()]
    base_by_prop = {
        str(r.get("prop_type")): r for r in (baseline_payload.get("by_prop") or []) if r.get("prop_type")
    }
    cand_by_prop = {
        str(r.get("prop_type")): r for r in (candidate_payload.get("by_prop") or []) if r.get("prop_type")
    }
    if not required:
        required = sorted(set(base_by_prop.keys()))

    missing_required_props: list[str] = []
    degraded_required_props: list[dict[str, Any]] = []
    insufficient_baseline_sample_props: list[dict[str, Any]] = []
    for prop in required:
        c = cand_by_prop.get(prop)
        if not c:
            missing_required_props.append(prop)
            continue
        b = base_by_prop.get(prop) or {}
        b_total = _to_int(b.get("total"))
        c_total = _to_int(c.get("total"))
        b_acc = _to_float(b.get("accuracy_pct"))
        c_acc = _to_float(c.get("accuracy_pct"))
        if b_total < int(min_baseline_prop_total_for_drop):
            insufficient_baseline_sample_props.append(
                {
                    "prop_type": prop,
                    "baseline_total": b_total,
                    "candidate_total": c_total,
                    "baseline_accuracy_pct": b_acc,
                    "candidate_accuracy_pct": c_acc,
                }
            )
            continue
        delta = None if b_acc is None or c_acc is None else round(c_acc - b_acc, 2)
        if delta is not None and delta < (0.0 - float(max_prop_drop_pct)):
            degraded_required_props.append(
                {
                    "prop_type": prop,
                    "baseline_accuracy_pct": b_acc,
                    "candidate_accuracy_pct": c_acc,
                    "candidate_minus_baseline_accuracy_pct": delta,
                }
            )

    checks = {
        "candidate_sample_size": {
            "ok": cand_total >= int(min_candidate_total),
            "candidate_total": cand_total,
            "required_min_total": int(min_candidate_total),
        },
        "overall_lift": {
            "ok": overall_lift is not None and overall_lift >= float(min_overall_lift_pct),
            "baseline_accuracy_pct": base_acc,
            "candidate_accuracy_pct": cand_acc,
            "candidate_minus_baseline_accuracy_pct": overall_lift,
            "required_min_lift_pct": float(min_overall_lift_pct),
        },
        "required_prop_stability": {
            "ok": not missing_required_props and not degraded_required_props,
            "required_props": required,
            "missing_required_props": missing_required_props,
            "degraded_required_props": degraded_required_props,
            "insufficient_baseline_sample_props": insufficient_baseline_sample_props,
            "max_allowed_drop_pct": float(max_prop_drop_pct),
            "min_baseline_prop_total_for_drop": int(min_baseline_prop_total_for_drop),
        },
    }

    failures = [name for name, c in checks.items() if not bool(c.get("ok"))]
    promote = len(failures) == 0
    return {
        "ok": promote,
        "status": "pass" if promote else "fail",
        "recommendation": "promote" if promote else "hold",
        "promotion_rule": (
            "promote when candidate total >= min_total, overall lift >= min_lift, "
            "and required props do not drop beyond max_prop_drop"
        ),
        "failures": failures,
        "summary": {
            "baseline_total": base_total,
            "candidate_total": cand_total,
            "baseline_accuracy_pct": base_acc,
            "candidate_accuracy_pct": cand_acc,
            "candidate_minus_baseline_accuracy_pct": overall_lift,
        },
        "checks": checks,
    }


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Evaluate MLB candidate quality against baseline artifact.")
    ap.add_argument("--baseline-path", default="", help="Optional path to baseline JSON artifact.")
    ap.add_argument("--baseline-dir", default="artifacts/season_baselines", help="Used when --baseline-path is empty.")
    ap.add_argument(
        "--source-table",
        choices=["player_props", "model_training_props", "reconcile_rows"],
        default="model_training_props",
    )
    ap.add_argument(
        "--rows-csv",
        default="",
        help="Required when --source-table reconcile_rows; path to reconcile rows csv.",
    )
    ap.add_argument(
        "--reconcile-require-two-sided",
        action="store_true",
        default=str(os.environ.get("MLB_QUALITY_RECONCILE_REQUIRE_TWO_SIDED", "1")).strip().lower()
        in {"1", "true", "yes", "on"},
        help="When using --source-table reconcile_rows, keep only two-sided market rows.",
    )
    ap.add_argument("--window-mode", choices=["days", "games"], default="")
    ap.add_argument("--window-days", type=int, default=120)
    ap.add_argument("--games-back", type=int, default=30)
    ap.add_argument("--prop-types", default="", help="Candidate scope; defaults to baseline by_prop prop types.")
    ap.add_argument("--required-props", default="", help="Required props for per-prop stability checks.")
    ap.add_argument("--min-candidate-total", type=int, default=-1, help="-1 uses baseline overall total.")
    ap.add_argument("--min-overall-lift-pct", type=float, default=0.25)
    ap.add_argument("--max-prop-drop-pct", type=float, default=0.5)
    ap.add_argument("--min-baseline-prop-total-for-drop", type=int, default=300)
    args = ap.parse_args(list(argv) if argv is not None else sys.argv[1:])

    try:
        baseline_path = _pick_baseline(str(args.baseline_path).strip() or None, args.baseline_dir)
        baseline_payload = _load_json(baseline_path)
    except Exception as exc:
        print(
            json.dumps(
                {"ok": False, "status": "fail", "recommendation": "hold", "error": f"baseline_load_failed: {exc}"},
                indent=2,
            )
        )
        return 2

    baseline_overall = baseline_payload.get("overall") or {}
    baseline_window_mode = str((baseline_overall.get("window_mode") or "")).strip()
    baseline_window_value = _to_int(baseline_overall.get("window_value"))
    baseline_props = [
        str(r.get("prop_type")).strip()
        for r in (baseline_payload.get("by_prop") or [])
        if str(r.get("prop_type") or "").strip()
    ]

    window_mode = str(args.window_mode).strip() or baseline_window_mode or "games"
    if window_mode == "games":
        window_value = max(1, baseline_window_value or int(args.games_back))
        window_days = int(args.window_days)
        games_back = int(window_value)
    else:
        window_value = max(1, baseline_window_value or int(args.window_days))
        window_days = int(window_value)
        games_back = int(args.games_back)

    prop_types = [p.strip() for p in str(args.prop_types).split(",") if p.strip()] or baseline_props
    required_props = [p.strip() for p in str(args.required_props).split(",") if p.strip()] or prop_types

    try:
        candidate_payload = analyze_mlb_prediction_quality.collect_quality(
            window_mode=window_mode,
            window_value=window_value,
            prop_types=prop_types,
            source_table=args.source_table,
            rows_csv=args.rows_csv,
            require_two_sided_reconcile_rows=bool(args.reconcile_require_two_sided),
        )
    except Exception as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "status": "fail",
                    "recommendation": "hold",
                    "error": f"candidate_collect_failed: {type(exc).__name__}: {exc}",
                },
                indent=2,
            )
        )
        return 1

    min_candidate_total = int(args.min_candidate_total)
    if min_candidate_total < 0:
        min_candidate_total = _to_int((baseline_payload.get("overall") or {}).get("total"))

    result = evaluate_candidate(
        baseline_payload=baseline_payload,
        candidate_payload=candidate_payload,
        required_props=required_props,
        min_overall_lift_pct=float(args.min_overall_lift_pct),
        max_prop_drop_pct=max(0.0, float(args.max_prop_drop_pct)),
        min_candidate_total=max(0, int(min_candidate_total)),
        min_baseline_prop_total_for_drop=max(0, int(args.min_baseline_prop_total_for_drop)),
    )
    payload = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "baseline_path": str(baseline_path),
        "source_table": args.source_table,
        "rows_csv": str(args.rows_csv or ""),
        "reconcile_require_two_sided": bool(args.reconcile_require_two_sided),
        "window_mode": window_mode,
        "window_value": window_value,
        "prop_types": prop_types,
        "required_props": required_props,
        "baseline": {
            "overall": baseline_payload.get("overall"),
            "by_prop": baseline_payload.get("by_prop"),
        },
        "candidate": {
            "overall": candidate_payload.get("overall"),
            "by_prop": candidate_payload.get("by_prop"),
        },
        **result,
    }
    print(json.dumps(payload, indent=2))
    return 0 if payload.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
