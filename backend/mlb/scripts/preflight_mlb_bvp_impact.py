#!/usr/bin/env python3
"""Cheap preflight for the MLB BvP/PvB impact report runtime."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Optional, Sequence


def _count_slate(path: Path) -> tuple[int, Counter[str]]:
    props: Counter[str] = Counter()
    rows = 0
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows += 1
            prop = str(row.get("prop_type") or "").strip().lower() or "unknown"
            props[prop] += 1
    return rows, props


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        if not path.exists():
            return {}
        obj = json.loads(path.read_text(encoding="utf-8"))
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _risk(rows: int, medium_rows: int, high_rows: int) -> str:
    if rows >= high_rows:
        return "HIGH"
    if rows >= medium_rows:
        return "MEDIUM"
    return "LOW"


def _fmt_pct(v: Optional[float]) -> str:
    try:
        if v is None:
            return "n/a"
        return f"{float(v) * 100:.2f}%"
    except Exception:
        return "n/a"


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Estimate MLB BvP impact report cost before running it.")
    ap.add_argument("--slate-csv", default="backend/mlb/data/processed/mlb_slate_output.csv")
    ap.add_argument("--wide-csv", default="backend/mlb/data/processed/mlb_predictions_wide_calibrated.csv")
    ap.add_argument("--impact-json", default="artifacts/analysis/mlb/mlb_bvp_impact_latest.json")
    ap.add_argument("--label-date", default="")
    ap.add_argument("--medium-rows", type=int, default=700)
    ap.add_argument("--high-rows", type=int, default=1500)
    ap.add_argument("--fail-high", type=int, default=0)
    args = ap.parse_args(list(argv) if argv is not None else None)

    slate_path = Path(args.slate_csv)
    wide_path = Path(args.wide_csv)
    impact_path = Path(args.impact_json)

    if not slate_path.exists():
        print(f"[bvp-impact-preflight] ERROR missing slate_csv={slate_path}")
        return 2
    if not wide_path.exists():
        print(f"[bvp-impact-preflight] ERROR missing wide_csv={wide_path}")
        return 2

    rows, props = _count_slate(slate_path)
    prediction_passes = rows * 2
    risk = _risk(rows, int(args.medium_rows), int(args.high_rows))
    latest = _load_json(impact_path)
    latest_label = str(latest.get("label_date") or "missing")
    latest_rows = latest.get("rows_evaluated")
    latest_mean_delta = latest.get("mean_abs_delta_prob")
    label_date = str(args.label_date or "").strip() or "unspecified"

    print(f"[bvp-impact-preflight] label_date={label_date}")
    print(f"[bvp-impact-preflight] slate_csv={slate_path}")
    print(f"[bvp-impact-preflight] wide_csv={wide_path}")
    print(f"[bvp-impact-preflight] slate_rows={rows}")
    print(f"[bvp-impact-preflight] estimated_prediction_passes={prediction_passes}")
    print(f"[bvp-impact-preflight] runtime_risk={risk} medium_rows={args.medium_rows} high_rows={args.high_rows}")
    print(
        "[bvp-impact-preflight] latest_artifact="
        f"label_date={latest_label} rows_evaluated={latest_rows if latest_rows is not None else 'n/a'} "
        f"mean_abs_delta={_fmt_pct(latest_mean_delta)}"
    )
    print("[bvp-impact-preflight] prop_distribution:")
    for prop, count in props.most_common():
        print(f"  {prop}: {count}")

    print("[bvp-impact-preflight] local_command:")
    print(f"  make mlb-bvp-impact-report MLB_BVP_IMPACT_LABEL_DATE={label_date}")
    print("[bvp-impact-preflight] brief_after_local_command:")
    print(f"  make mlb-daily-ops-brief MLB_DAILY_BRIEF_REFRESH_BVP_IMPACT=0 MLB_DAILY_BRIEF_REPORT_DATE={label_date}")

    if risk == "HIGH":
        print(
            "[bvp-impact-preflight] recommendation=run locally or explicitly approve Codex to wait; "
            "this job scores every slate row twice and may take several minutes."
        )
        if int(args.fail_high) == 1:
            return 3
    elif risk == "MEDIUM":
        print("[bvp-impact-preflight] recommendation=ask before running in Codex; local is safer if latency matters.")
    else:
        print("[bvp-impact-preflight] recommendation=reasonable for Codex to run.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
