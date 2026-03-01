#!/usr/bin/env python3
"""Sweep blend weights between NHL ge_4 model output and a Poisson baseline."""

from __future__ import annotations

import argparse
import json
from typing import Any, Dict, List

from backend.nhl.scripts.benchmark_sog_ge4_vs_poisson import (
    _expected_bucket,
    _fetch_rows,
    _metric_rows,
    _poisson_tail,
    _resolve_window,
    _round,
    _to_float,
)


def _weight_grid(step: float) -> List[float]:
    if step <= 0 or step > 1:
        raise ValueError("--step must be > 0 and <= 1")
    out: List[float] = []
    w = 0.0
    while w < 1.0:
        out.append(round(w, 4))
        w += step
    out.append(1.0)
    return sorted(set(out))


def _bucket_rows(rows: List[Dict[str, Any]], bucket: str) -> List[Dict[str, Any]]:
    return [r for r in rows if _expected_bucket(_to_float(r.get("expected_sog"))) == bucket]


def _eval_weight(rows: List[Dict[str, Any]], threshold: int, model_weight: float) -> Dict[str, Any]:
    blend_rows: List[Dict[str, Any]] = []
    for row in rows:
        p_model = _to_float(row.get("model_p_over"))
        p_poisson = _to_float(row.get("poisson_p_over"))
        if p_model is not None and p_poisson is not None:
            p_blend = (model_weight * p_model) + ((1.0 - model_weight) * p_poisson)
        else:
            p_blend = None
        blend_rows.append({**row, "blend_p_over": p_blend})

    overall = _metric_rows(blend_rows, "blend_p_over", threshold)
    buckets = {
        "2.5-3.5": _metric_rows(_bucket_rows(blend_rows, "2.5-3.5"), "blend_p_over", threshold),
        "3.5+": _metric_rows(_bucket_rows(blend_rows, "3.5+"), "blend_p_over", threshold),
    }
    return {
        "model_weight": _round(model_weight, 4),
        "poisson_weight": _round(1.0 - model_weight, 4),
        "overall": overall,
        "buckets": buckets,
    }


def _pick_best(results: List[Dict[str, Any]], bucket: str | None, metric: str) -> Dict[str, Any] | None:
    scored = []
    for row in results:
        target = row["overall"] if bucket is None else row["buckets"].get(bucket, {})
        value = _to_float(target.get(metric))
        if value is None:
            continue
        if metric == "gap":
            score = abs(value)
        else:
            score = value
        scored.append((score, row))
    if not scored:
        return None
    scored.sort(key=lambda item: item[0])
    best = scored[0][1]
    target = best["overall"] if bucket is None else best["buckets"][bucket]
    return {
        "model_weight": best["model_weight"],
        "poisson_weight": best["poisson_weight"],
        "metric": metric,
        "bucket": bucket or "overall",
        "value": target.get(metric),
        "avg_p": target.get("avg_p"),
        "hit_rate": target.get("hit_rate"),
        "gap": target.get("gap"),
        "brier": target.get("brier"),
        "n": target.get("n"),
    }


def analyze(args: argparse.Namespace) -> Dict[str, Any]:
    line = float(args.line)
    threshold = int(line // 1 + 1)
    window = _resolve_window(
        from_date_raw=args.from_date,
        to_date_raw=args.to_date,
        lookback_days=max(1, int(args.lookback_days)),
        model_family=args.model_family,
        model_version=args.model_version,
        line=line,
    )

    rows = _fetch_rows(args.model_family, args.model_version, line, window)
    for row in rows:
        row["poisson_p_over"] = _poisson_tail(_to_float(row.get("expected_sog")), threshold)

    weights = _weight_grid(float(args.step))
    results = [_eval_weight(rows, threshold, w) for w in weights]

    selected_weights = {0.0, 0.25, 0.5, 0.75, 1.0}
    snapshots = [r for r in results if float(r["model_weight"]) in selected_weights]

    return {
        "ok": True,
        "config": {
            "model_family": args.model_family,
            "model_version": args.model_version,
            "line": line,
            "threshold": threshold,
            "from_date": window.from_date,
            "to_date": window.to_date,
            "lookback_days": int(args.lookback_days),
            "step": float(args.step),
        },
        "best_overall_brier": _pick_best(results, None, "brier"),
        "best_overall_gap": _pick_best(results, None, "gap"),
        "best_bucket_2p5_3p5_brier": _pick_best(results, "2.5-3.5", "brier"),
        "best_bucket_2p5_3p5_gap": _pick_best(results, "2.5-3.5", "gap"),
        "best_bucket_3p5p_brier": _pick_best(results, "3.5+", "brier"),
        "best_bucket_3p5p_gap": _pick_best(results, "3.5+", "gap"),
        "snapshots": snapshots,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Blend sweep between NHL 3.5 model output and Poisson baseline.")
    ap.add_argument("--model-family", default="denali_blend")
    ap.add_argument("--model-version", default="phoenix_v2")
    ap.add_argument("--line", type=float, default=3.5)
    ap.add_argument("--from-date", default=None)
    ap.add_argument("--to-date", default=None)
    ap.add_argument("--lookback-days", type=int, default=120)
    ap.add_argument("--step", type=float, default=0.05)
    args = ap.parse_args()

    print(json.dumps(analyze(args), indent=2))


if __name__ == "__main__":
    main()
