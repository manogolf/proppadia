#!/usr/bin/env python3
"""Append NHL SOG segmented calibration experiment summary to JSONL history."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from backend.scripts import experiment_nhl_sog_segmented_calibration
from backend.shared.scripts.json_check_runner import run_json_check


def _parse_lines(raw: str) -> list[float]:
    vals: list[float] = []
    for tok in (raw or "").split(","):
        tok = tok.strip()
        if not tok:
            continue
        vals.append(float(tok))
    return sorted(set(vals))


def collect_snapshot(
    *,
    model_family: str,
    model_version: str,
    lines: str,
    from_date: str,
    to_date: str,
    lookback_days: int,
    holdout_days: int,
    segment_min_rows: int,
    blend_alpha: float,
    decay_half_life_days: float,
    required_lines: str,
    max_delta_brier_vs_raw: float,
    max_delta_logloss_vs_raw: float,
) -> dict[str, Any]:
    exp_args: list[str] = [
        "--model-family",
        str(model_family),
        "--model-version",
        str(model_version),
        "--lines",
        str(lines),
        "--lookback-days",
        str(int(lookback_days)),
        "--holdout-days",
        str(int(holdout_days)),
        "--segment-min-rows",
        str(int(segment_min_rows)),
        "--blend-alpha",
        str(float(blend_alpha)),
        "--decay-half-life-days",
        str(float(decay_half_life_days)),
    ]
    if str(from_date).strip():
        exp_args.extend(["--from-date", str(from_date).strip()])
    if str(to_date).strip():
        exp_args.extend(["--to-date", str(to_date).strip()])

    exp_rc, exp_payload = run_json_check(experiment_nhl_sog_segmented_calibration.main, exp_args)
    exp_ok = int(exp_rc) == 0 and bool(exp_payload.get("ok"))

    required = _parse_lines(required_lines)
    delta_rows = (
        (exp_payload.get("holdout_deltas_vs_raw") or {}).get("segmented_iso")
        if isinstance(exp_payload, dict)
        else None
    ) or []
    delta_map: dict[float, dict[str, Any]] = {}
    for row in delta_rows:
        try:
            key = float(row.get("line"))
        except Exception:
            continue
        delta_map[key] = row

    line_checks: list[dict[str, Any]] = []
    failures: list[str] = []
    for line in required:
        row = delta_map.get(float(line))
        if row is None:
            failures.append(f"missing_line_delta:{line}")
            line_checks.append({"line": line, "ok": False, "reason": "missing_delta"})
            continue
        delta_brier = row.get("delta_brier_vs_raw")
        delta_logloss = row.get("delta_logloss_vs_raw")
        brier_ok = delta_brier is not None and float(delta_brier) <= float(max_delta_brier_vs_raw)
        logloss_ok = delta_logloss is None or float(delta_logloss) <= float(max_delta_logloss_vs_raw)
        ok = bool(brier_ok and logloss_ok)
        if not ok:
            failures.append(f"line_failed:{line}")
        line_checks.append(
            {
                "line": float(line),
                "ok": ok,
                "delta_brier_vs_raw": delta_brier,
                "delta_logloss_vs_raw": delta_logloss,
                "max_delta_brier_vs_raw": float(max_delta_brier_vs_raw),
                "max_delta_logloss_vs_raw": float(max_delta_logloss_vs_raw),
            }
        )

    if not exp_ok:
        failures.append("experiment_failed")

    ok = len(failures) == 0
    return {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "ok": ok,
        "status": "pass" if ok else "fail",
        "failures": failures,
        "checks": {
            "experiment": {
                "ok": exp_ok,
                "status": exp_payload.get("status"),
                "exit_code": int(exp_rc),
                "error": exp_payload.get("error"),
            },
            "line_deltas": {
                "ok": len([x for x in line_checks if not bool(x.get("ok"))]) == 0,
                "required_lines": required,
                "rows": line_checks,
            },
        },
        "experiment_summary": {
            "config": exp_payload.get("config"),
            "counts": exp_payload.get("counts"),
            "holdout_by_method_line": exp_payload.get("holdout_by_method_line"),
            "holdout_deltas_vs_raw": exp_payload.get("holdout_deltas_vs_raw"),
        },
        "experiment_payload": exp_payload if not exp_ok else None,
    }


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Append NHL SOG calibration monitor snapshot to JSONL history.")
    ap.add_argument("--output", default="artifacts/nhl_sog_calibration_history.jsonl")
    ap.add_argument("--model-family", default="denali_blend")
    ap.add_argument("--model-version", default="phoenix_v2")
    ap.add_argument("--lines", default="1.5,2.5,3.5")
    ap.add_argument("--from-date", default="")
    ap.add_argument("--to-date", default="")
    ap.add_argument("--lookback-days", type=int, default=120)
    ap.add_argument("--holdout-days", type=int, default=14)
    ap.add_argument("--segment-min-rows", type=int, default=120)
    ap.add_argument("--blend-alpha", type=float, default=0.65)
    ap.add_argument("--decay-half-life-days", type=float, default=21.0)
    ap.add_argument("--required-lines", default="1.5,2.5,3.5")
    ap.add_argument("--max-delta-brier-vs-raw", type=float, default=0.0)
    ap.add_argument("--max-delta-logloss-vs-raw", type=float, default=0.0)
    args = ap.parse_args(list(argv) if argv is not None else sys.argv[1:])

    payload = collect_snapshot(
        model_family=str(args.model_family),
        model_version=str(args.model_version),
        lines=str(args.lines),
        from_date=str(args.from_date),
        to_date=str(args.to_date),
        lookback_days=int(args.lookback_days),
        holdout_days=int(args.holdout_days),
        segment_min_rows=int(args.segment_min_rows),
        blend_alpha=float(args.blend_alpha),
        decay_half_life_days=float(args.decay_half_life_days),
        required_lines=str(args.required_lines),
        max_delta_brier_vs_raw=float(args.max_delta_brier_vs_raw),
        max_delta_logloss_vs_raw=float(args.max_delta_logloss_vs_raw),
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
