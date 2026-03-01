#!/usr/bin/env python3
"""Compare NHL SOG top-N ranked overs for current model vs Poisson baseline."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Sequence

from backend.nhl.scripts.benchmark_sog_ge4_vs_poisson import (
    _fetch_rows,
    _poisson_tail,
    _resolve_window,
    _round,
    _to_float,
)


LINES = (1.5, 2.5, 3.5)


def _group_by_game_date(rows: Sequence[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["game_date"])].append(row)
    return grouped


def _rank_top_n(rows: Sequence[Dict[str, Any]], prob_key: str, top_n: int, threshold: int) -> List[Dict[str, Any]]:
    usable: List[Dict[str, Any]] = []
    for row in rows:
        p = _to_float(row.get(prob_key))
        sog = _to_float(row.get("shots_on_goal"))
        if p is None or sog is None:
            continue
        usable.append(
            {
                **row,
                "_p": p,
                "_hit": 1 if sog >= threshold else 0,
            }
        )
    usable.sort(key=lambda r: (r["_p"], -(r.get("player_id") or 0)), reverse=True)
    return usable[:top_n]


def _metric_rows(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    n = len(rows)
    if n == 0:
        return {"n": 0, "avg_p": None, "hit_rate": None, "gap": None, "brier": None}
    avg_p = sum(float(r["_p"]) for r in rows) / n
    hit_rate = sum(int(r["_hit"]) for r in rows) / n
    brier = sum((float(r["_p"]) - int(r["_hit"])) ** 2 for r in rows) / n
    return {
        "n": n,
        "avg_p": _round(avg_p),
        "hit_rate": _round(hit_rate),
        "gap": _round(avg_p - hit_rate),
        "brier": _round(brier),
    }


def _analyze_grouped(rows: Sequence[Dict[str, Any]], prob_key: str, top_n: int, threshold: int) -> Dict[str, Any]:
    grouped = _group_by_game_date(rows)
    picked_rows: List[Dict[str, Any]] = []
    for _, day_rows in sorted(grouped.items()):
        picked_rows.extend(_rank_top_n(day_rows, prob_key, top_n, threshold))
    return _metric_rows(picked_rows)


def _fetch_line_rows(args: argparse.Namespace, line: float) -> List[Dict[str, Any]]:
    window = _resolve_window(
        from_date_raw=args.from_date,
        to_date_raw=args.to_date,
        lookback_days=max(1, int(args.lookback_days)),
        model_family=args.model_family,
        model_version=args.model_version,
        line=line,
    )
    threshold = int(line) + 1
    rows = _fetch_rows(args.model_family, args.model_version, line, window)
    for row in rows:
        row["poisson_p_over"] = _poisson_tail(_to_float(row.get("expected_sog")), threshold)
        row["line"] = line
        row["threshold"] = threshold
    return rows


def analyze(args: argparse.Namespace) -> Dict[str, Any]:
    top_ns = [int(v) for v in args.top_n]
    all_rows: List[Dict[str, Any]] = []
    per_line_rows: Dict[str, List[Dict[str, Any]]] = {}
    for line in LINES:
        rows = _fetch_line_rows(args, line)
        per_line_rows[str(line)] = rows
        all_rows.extend(rows)

    out: Dict[str, Any] = {
        "ok": True,
        "config": {
            "model_family": args.model_family,
            "model_version": args.model_version,
            "from_date": args.from_date,
            "to_date": args.to_date,
            "lookback_days": int(args.lookback_days),
            "top_n": top_ns,
        },
        "per_line": {},
        "combined": {},
    }

    for line_key, rows in per_line_rows.items():
        threshold = int(float(line_key)) + 1
        line_result: Dict[str, Any] = {"row_count": len(rows), "top_n": {}}
        for top_n in top_ns:
            line_result["top_n"][str(top_n)] = {
                "model": _analyze_grouped(rows, "model_p_over", top_n, threshold),
                "poisson": _analyze_grouped(rows, "poisson_p_over", top_n, threshold),
            }
        out["per_line"][line_key] = line_result

    def _combined_rank(prob_key: str, top_n: int) -> Dict[str, Any]:
        grouped = _group_by_game_date(all_rows)
        picked_rows: List[Dict[str, Any]] = []
        for _, day_rows in sorted(grouped.items()):
            usable: List[Dict[str, Any]] = []
            for row in day_rows:
                p = _to_float(row.get(prob_key))
                sog = _to_float(row.get("shots_on_goal"))
                threshold = int(_to_float(row.get("line")) or 0) + 1
                if p is None or sog is None:
                    continue
                usable.append({**row, "_p": p, "_hit": 1 if sog >= threshold else 0})
            usable.sort(key=lambda r: (r["_p"], -(r.get("player_id") or 0)), reverse=True)
            picked_rows.extend(usable[:top_n])
        return _metric_rows(picked_rows)

    out["combined"]["row_count"] = len(all_rows)
    out["combined"]["top_n"] = {
        str(top_n): {
            "model": _combined_rank("model_p_over", top_n),
            "poisson": _combined_rank("poisson_p_over", top_n),
        }
        for top_n in top_ns
    }
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Compare NHL SOG model vs Poisson on top-N ranked overs.")
    ap.add_argument("--model-family", default="denali_blend")
    ap.add_argument("--model-version", default="phoenix_v2")
    ap.add_argument("--from-date", required=True)
    ap.add_argument("--to-date", required=True)
    ap.add_argument("--lookback-days", type=int, default=120)
    ap.add_argument("--top-n", nargs="+", default=["5", "10", "20"])
    args = ap.parse_args()
    print(json.dumps(analyze(args), indent=2))


if __name__ == "__main__":
    main()
