#!/usr/bin/env python3
"""Replay a bucket-gated NHL SOG 3.5 blend policy offline."""

from __future__ import annotations

import argparse
import json
import math
from typing import Any, Dict, List

from backend.nhl.scripts.benchmark_sog_ge4_vs_poisson import (
    _expected_bucket,
    _fetch_rows,
    _metric_rows,
    _poisson_tail,
    _resolve_window,
    _role_bucket,
    _round,
    _to_float,
    _toi_bucket,
)


def _weights_from_args(args: argparse.Namespace) -> Dict[str, float]:
    return {
        "default": float(args.default_model_weight),
        "2.5-3.5": float(args.bucket_2p5_3p5_model_weight),
        "3.5+": float(args.bucket_3p5p_model_weight),
    }


def _apply_policy(rows: List[Dict[str, Any]], threshold: int, weights: Dict[str, float]) -> List[Dict[str, Any]]:
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
        out.append(
            {
                **row,
                "expected_bucket": bucket,
                "policy_model_weight": model_weight,
                "policy_p_over": p_policy,
            }
        )
    return out


def _segment_stats(rows: List[Dict[str, Any]], key_fn, threshold: int) -> List[Dict[str, Any]]:
    buckets: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        buckets.setdefault(key_fn(row), []).append(row)

    out: List[Dict[str, Any]] = []
    for key, group in sorted(buckets.items(), key=lambda item: item[0]):
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


def _player_extremes(rows: List[Dict[str, Any]], threshold: int, min_n: int, top_n: int) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[int, List[Dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(int(row["player_id"]), []).append(row)

    scored: List[Dict[str, Any]] = []
    for _, group in grouped.items():
        if len(group) < min_n:
            continue
        model_stats = _metric_rows(group, "model_p_over", threshold)
        poisson_stats = _metric_rows(group, "poisson_p_over", threshold)
        policy_stats = _metric_rows(group, "policy_p_over", threshold)
        scored.append(
            {
                "player_id": int(group[0]["player_id"]),
                "player_name": group[0]["player_name"],
                "role": _role_bucket(group[0].get("position_raw")),
                "avg_expected_sog": _round(sum(_to_float(r.get("expected_sog")) or 0.0 for r in group) / len(group)),
                "n": len(group),
                "hit_rate": model_stats.get("hit_rate"),
                "model_gap": model_stats.get("gap"),
                "poisson_gap": poisson_stats.get("gap"),
                "policy_gap": policy_stats.get("gap"),
                "model_avg_p": model_stats.get("avg_p"),
                "poisson_avg_p": poisson_stats.get("avg_p"),
                "policy_avg_p": policy_stats.get("avg_p"),
            }
        )

    def _gap_abs_improvement(row: Dict[str, Any]) -> float:
        model_gap = abs(_to_float(row.get("model_gap")) or 0.0)
        policy_gap = abs(_to_float(row.get("policy_gap")) or 0.0)
        return model_gap - policy_gap

    under = sorted(scored, key=lambda r: _to_float(r.get("policy_gap")) if _to_float(r.get("policy_gap")) is not None else 999.0)
    improved = sorted(scored, key=_gap_abs_improvement, reverse=True)
    return {
        "most_policy_underpredicted": under[:top_n],
        "largest_policy_improvement": improved[:top_n],
    }


def analyze(args: argparse.Namespace) -> Dict[str, Any]:
    line = float(args.line)
    threshold = int(math.floor(line) + 1)
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

    weights = _weights_from_args(args)
    policy_rows = _apply_policy(rows, threshold, weights)

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
            "weights": weights,
        },
        "overall": {
            "model": _metric_rows(policy_rows, "model_p_over", threshold),
            "poisson": _metric_rows(policy_rows, "poisson_p_over", threshold),
            "policy": _metric_rows(policy_rows, "policy_p_over", threshold),
        },
        "by_expected_sog_bucket": _segment_stats(policy_rows, lambda r: r["expected_bucket"], threshold),
        "by_role": _segment_stats(policy_rows, lambda r: _role_bucket(r.get("position_raw")), threshold),
        "by_toi": _segment_stats(policy_rows, lambda r: _toi_bucket(_to_float(r.get("d10_toi_min_avg"))), threshold),
        "player_extremes": _player_extremes(
            policy_rows,
            threshold=threshold,
            min_n=max(1, int(args.player_min_n)),
            top_n=max(1, int(args.player_top_n)),
        ),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Replay a bucket-gated blend policy for NHL SOG 3.5.")
    ap.add_argument("--model-family", default="denali_blend")
    ap.add_argument("--model-version", default="phoenix_v2")
    ap.add_argument("--line", type=float, default=3.5)
    ap.add_argument("--from-date", default=None)
    ap.add_argument("--to-date", default=None)
    ap.add_argument("--lookback-days", type=int, default=120)
    ap.add_argument("--default-model-weight", type=float, default=1.0)
    ap.add_argument("--bucket-2p5-3p5-model-weight", type=float, default=0.10)
    ap.add_argument("--bucket-3p5p-model-weight", type=float, default=0.35)
    ap.add_argument("--player-min-n", type=int, default=5)
    ap.add_argument("--player-top-n", type=int, default=10)
    args = ap.parse_args()
    print(json.dumps(analyze(args), indent=2))


if __name__ == "__main__":
    main()
