#!/usr/bin/env python3
"""Replay a configurable multi-line NHL SOG policy offline."""

from __future__ import annotations

import argparse
import json
import math
from typing import Any, Dict, List, Sequence

from backend.nhl.scripts.benchmark_sog_ge4_vs_poisson import (
    _expected_bucket,
    _fetch_rows,
    _metric_rows,
    _poisson_tail,
    _resolve_window,
    _round,
    _to_float,
)


def _line_key(line: float) -> str:
    return str(line).replace('.', 'p')


def _weights_for_line(args: argparse.Namespace, line: float) -> Dict[str, float]:
    key = _line_key(line)
    default = float(getattr(args, f"line_{key}_default_model_weight"))
    return {
        "default": default,
        "1.5-2.5": float(getattr(args, f"line_{key}_bucket_1p5_2p5_model_weight", default)),
        "2.5-3.5": float(getattr(args, f"line_{key}_bucket_2p5_3p5_model_weight", default)),
        "3.5+": float(getattr(args, f"line_{key}_bucket_3p5p_model_weight", default)),
    }


def _apply_policy(rows: Sequence[Dict[str, Any]], threshold: int, weights: Dict[str, float]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        bucket = _expected_bucket(_to_float(row.get("expected_sog")))
        model_weight = weights.get(bucket, weights["default"])
        p_model = _to_float(row.get("model_p_over"))
        p_poisson = _to_float(row.get("poisson_p_over"))
        if p_model is not None and p_poisson is not None:
            p_policy = (model_weight * p_model) + ((1.0 - model_weight) * p_poisson)
        else:
            p_policy = None
        out.append({**row, "expected_bucket": bucket, "policy_model_weight": model_weight, "policy_p_over": p_policy})
    return out


def _bucket_stats(rows: Sequence[Dict[str, Any]], threshold: int) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get("expected_bucket") or "missing"), []).append(row)
    out: List[Dict[str, Any]] = []
    for key, group in sorted(grouped.items(), key=lambda item: item[0]):
        out.append(
            {
                "segment_value": key,
                "n": len(group),
                "model": _metric_rows(group, "model_p_over", threshold),
                "poisson": _metric_rows(group, "poisson_p_over", threshold),
                "policy": _metric_rows(group, "policy_p_over", threshold),
            }
        )
    return out


def _fetch_line_rows(args: argparse.Namespace, line: float, from_date: str, to_date: str) -> List[Dict[str, Any]]:
    window = _resolve_window(
        from_date_raw=from_date,
        to_date_raw=to_date,
        lookback_days=max(1, int(args.lookback_days)),
        model_family=args.model_family,
        model_version=args.model_version,
        line=line,
    )
    rows = _fetch_rows(args.model_family, args.model_version, line, window)
    threshold = int(math.floor(line) + 1)
    for row in rows:
        row["poisson_p_over"] = _poisson_tail(_to_float(row.get("expected_sog")), threshold)
        row["line"] = line
    return rows


def analyze(args: argparse.Namespace) -> Dict[str, Any]:
    lines = [1.5, 2.5, 3.5]
    per_line: Dict[str, Any] = {}
    combined_policy_rows: List[Dict[str, Any]] = []
    combined_model_rows: List[Dict[str, Any]] = []
    combined_poisson_rows: List[Dict[str, Any]] = []

    for line in lines:
        rows = _fetch_line_rows(args, line, args.from_date, args.to_date)
        threshold = int(math.floor(line) + 1)
        weights = _weights_for_line(args, line)
        policy_rows = _apply_policy(rows, threshold, weights)

        per_line[str(line)] = {
            "weights": weights,
            "n": len(policy_rows),
            "model": _metric_rows(policy_rows, "model_p_over", threshold),
            "poisson": _metric_rows(policy_rows, "poisson_p_over", threshold),
            "policy": _metric_rows(policy_rows, "policy_p_over", threshold),
            "by_expected_sog_bucket": _bucket_stats(policy_rows, threshold),
        }

        for row in policy_rows:
            y = 1 if (_to_float(row.get("shots_on_goal")) or 0.0) >= threshold else 0
            base = {"line": line, "y": y}
            if _to_float(row.get("model_p_over")) is not None:
                combined_model_rows.append({**base, "p": _to_float(row.get("model_p_over"))})
            if _to_float(row.get("poisson_p_over")) is not None:
                combined_poisson_rows.append({**base, "p": _to_float(row.get("poisson_p_over"))})
            if _to_float(row.get("policy_p_over")) is not None:
                combined_policy_rows.append({**base, "p": _to_float(row.get("policy_p_over"))})

    def _aggregate(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
        n = len(rows)
        if n == 0:
            return {"n": 0, "avg_p": None, "hit_rate": None, "gap": None, "brier": None}
        avg_p = sum(float(r["p"]) for r in rows) / n
        hit_rate = sum(int(r["y"]) for r in rows) / n
        brier = sum((float(r["p"]) - int(r["y"])) ** 2 for r in rows) / n
        return {
            "n": n,
            "avg_p": _round(avg_p),
            "hit_rate": _round(hit_rate),
            "gap": _round(avg_p - hit_rate),
            "brier": _round(brier),
        }

    return {
        "ok": True,
        "config": {
            "model_family": args.model_family,
            "model_version": args.model_version,
            "from_date": args.from_date,
            "to_date": args.to_date,
            "lookback_days": int(args.lookback_days),
        },
        "combined": {
            "model": _aggregate(combined_model_rows),
            "poisson": _aggregate(combined_poisson_rows),
            "policy": _aggregate(combined_policy_rows),
        },
        "per_line": per_line,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Replay a configurable multi-line NHL SOG policy offline.")
    ap.add_argument("--model-family", default="denali_blend")
    ap.add_argument("--model-version", default="phoenix_v2")
    ap.add_argument("--from-date", required=True)
    ap.add_argument("--to-date", required=True)
    ap.add_argument("--lookback-days", type=int, default=120)

    # 1.5
    ap.add_argument("--line-1p5-default-model-weight", type=float, default=0.0)
    ap.add_argument("--line-1p5-bucket-1p5-2p5-model-weight", dest="line_1p5_bucket_1p5_2p5_model_weight", type=float, default=0.0)
    ap.add_argument("--line-1p5-bucket-2p5-3p5-model-weight", dest="line_1p5_bucket_2p5_3p5_model_weight", type=float, default=0.0)
    ap.add_argument("--line-1p5-bucket-3p5p-model-weight", dest="line_1p5_bucket_3p5p_model_weight", type=float, default=0.0)

    # 2.5
    ap.add_argument("--line-2p5-default-model-weight", type=float, default=0.0)
    ap.add_argument("--line-2p5-bucket-1p5-2p5-model-weight", dest="line_2p5_bucket_1p5_2p5_model_weight", type=float, default=0.0)
    ap.add_argument("--line-2p5-bucket-2p5-3p5-model-weight", dest="line_2p5_bucket_2p5_3p5_model_weight", type=float, default=0.0)
    ap.add_argument("--line-2p5-bucket-3p5p-model-weight", dest="line_2p5_bucket_3p5p_model_weight", type=float, default=0.60)

    # 3.5
    ap.add_argument("--line-3p5-default-model-weight", type=float, default=1.0)
    ap.add_argument("--line-3p5-bucket-1p5-2p5-model-weight", dest="line_3p5_bucket_1p5_2p5_model_weight", type=float, default=1.0)
    ap.add_argument("--line-3p5-bucket-2p5-3p5-model-weight", dest="line_3p5_bucket_2p5_3p5_model_weight", type=float, default=0.10)
    ap.add_argument("--line-3p5-bucket-3p5p-model-weight", dest="line_3p5_bucket_3p5p_model_weight", type=float, default=0.35)

    args = ap.parse_args()
    print(json.dumps(analyze(args), indent=2))


if __name__ == "__main__":
    main()
