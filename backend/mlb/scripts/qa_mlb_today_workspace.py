#!/usr/bin/env python3
"""QA helper for /api/mlb/today/workspace trust validation."""

from __future__ import annotations

import argparse
import json
import math
import sys
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


def _to_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        x = float(v)
        if not math.isfinite(x):
            return None
        return x
    except Exception:
        return None


def _missing(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str):
        s = v.strip().lower()
        return s in {"", "nan", "null", "undefined"}
    if isinstance(v, (int, float)):
        return not math.isfinite(float(v))
    return False


def _eq(a: Optional[float], b: Optional[float], tol: float = 1e-8) -> bool:
    if a is None or b is None:
        return False
    return abs(a - b) <= tol


def _sign(x: Optional[float]) -> Optional[int]:
    if x is None:
        return None
    if x > 0:
        return 1
    if x < 0:
        return -1
    return 0


def _row_brief(row: Dict[str, Any]) -> Dict[str, Any]:
    keep = [
        "player_id",
        "player_name",
        "team",
        "opponent",
        "game_id",
        "prop_type",
        "line",
        "best_price",
        "market_median",
        "market_range",
        "value_vs_market",
        "timing_signal",
        "timing_reason",
        "streak_context_label",
        "streak_count",
        "baseline_delta",
        "consistency_score",
        "hit_rate_last_10",
        "hit_rate_season",
        "open_over_price",
        "latest_over_price",
        "over_price_change_from_open",
        "num_snapshots",
    ]
    return {k: row.get(k) for k in keep}


def _sample(rows: List[Dict[str, Any]], n: int = 5) -> List[Dict[str, Any]]:
    return [_row_brief(r) for r in rows[:n]]


def _fetch_rows_http(base_url: str, limit: int) -> Dict[str, Any]:
    params = urllib.parse.urlencode({"limit": limit, "offset": 0})
    url = f"{base_url.rstrip('/')}/api/mlb/today/workspace?{params}"
    with urllib.request.urlopen(url, timeout=30) as resp:  # noqa: S310
        return json.loads(resp.read().decode("utf-8"))


def _fetch_rows_inprocess(limit: int) -> Dict[str, Any]:
    from fastapi.testclient import TestClient
    from backend.app.api_server import app

    client = TestClient(app)
    resp = client.get("/api/mlb/today/workspace", params={"limit": limit, "offset": 0})
    if resp.status_code != 200:
        raise RuntimeError(f"inprocess GET failed: status={resp.status_code} body={resp.text}")
    return resp.json()


def _bucket_counts(rows: Iterable[Dict[str, Any]], field: str) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for r in rows:
        key = str(r.get(field) or "UNKNOWN").strip() or "UNKNOWN"
        out[key] = out.get(key, 0) + 1
    return dict(sorted(out.items(), key=lambda kv: (-kv[1], kv[0])))


def _group_samples(rows: List[Dict[str, Any]], field: str, n: int = 5) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        key = str(r.get(field) or "UNKNOWN").strip() or "UNKNOWN"
        grouped.setdefault(key, [])
        if len(grouped[key]) < n:
            grouped[key].append(_row_brief(r))
    return dict(sorted(grouped.items(), key=lambda kv: kv[0]))


def _validate_rows(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    checks = {
        "value_vs_market_exact_pass": 0,
        "value_vs_market_exact_fail": 0,
        "value_sign_pass": 0,
        "value_sign_fail": 0,
        "over_price_delta_field_pass": 0,
        "over_price_delta_field_fail": 0,
        "timing_signal_rule_pass": 0,
        "timing_signal_rule_fail": 0,
        "timing_reason_mapping_pass": 0,
        "timing_reason_mapping_fail": 0,
        "streak_label_rule_pass": 0,
        "streak_label_rule_fail": 0,
        "baseline_delta_formula_pass": 0,
        "baseline_delta_formula_fail": 0,
        "consistency_range_pass": 0,
        "consistency_range_fail": 0,
        "invalid_american_odds_field_pass": 0,
        "invalid_american_odds_field_fail": 0,
    }
    issues: List[Dict[str, Any]] = []

    reason_map = {
        "VOLATILE": "Large intraday movement",
        "WAIT": "Current price better than open",
        "EARLY": "Current price worse than open",
        "STABLE": "Little intraday movement",
    }

    for r in rows:
        best = _to_float(r.get("best_price"))
        median = _to_float(r.get("market_median"))
        value = _to_float(r.get("value_vs_market"))
        computed_value = None if (best is None or median is None) else (best - median)
        if (_missing(value) and computed_value is None) or _eq(value, computed_value):
            checks["value_vs_market_exact_pass"] += 1
        else:
            checks["value_vs_market_exact_fail"] += 1
            issues.append(
                {
                    "type": "value_vs_market_mismatch",
                    "row": _row_brief(r),
                    "expected": computed_value,
                    "actual": value,
                }
            )

        if _sign(value) == _sign(computed_value):
            checks["value_sign_pass"] += 1
        else:
            checks["value_sign_fail"] += 1
            issues.append(
                {
                    "type": "value_sign_mismatch",
                    "row": _row_brief(r),
                    "expected_sign": _sign(computed_value),
                    "actual_sign": _sign(value),
                }
            )

        open_over = _to_float(r.get("open_over_price"))
        latest_over = _to_float(r.get("latest_over_price"))
        delta_field = _to_float(r.get("over_price_change_from_open"))
        delta_calc = None if (open_over is None or latest_over is None) else (latest_over - open_over)
        if (_missing(delta_field) and delta_calc is None) or _eq(delta_field, delta_calc):
            checks["over_price_delta_field_pass"] += 1
        else:
            checks["over_price_delta_field_fail"] += 1
            issues.append(
                {
                    "type": "over_price_delta_mismatch",
                    "row": _row_brief(r),
                    "expected": delta_calc,
                    "actual": delta_field,
                }
            )

        signal = str(r.get("timing_signal") or "").strip().upper()
        timing_ok = True
        if signal == "WAIT":
            timing_ok = delta_calc is not None and delta_calc >= 10
        elif signal == "EARLY":
            timing_ok = delta_calc is not None and delta_calc <= -10
        elif signal == "STABLE":
            timing_ok = (delta_calc is None) or (abs(delta_calc) < 10)
        elif signal == "VOLATILE":
            # Span is not present in endpoint payload; validate reason mapping for volatile.
            timing_ok = True
        else:
            timing_ok = False

        if timing_ok:
            checks["timing_signal_rule_pass"] += 1
        else:
            checks["timing_signal_rule_fail"] += 1
            issues.append(
                {
                    "type": "timing_signal_rule_mismatch",
                    "row": _row_brief(r),
                    "delta_calc": delta_calc,
                }
            )

        reason = str(r.get("timing_reason") or "").strip()
        reason_ok = reason == reason_map.get(signal, "")
        if reason_ok:
            checks["timing_reason_mapping_pass"] += 1
        else:
            checks["timing_reason_mapping_fail"] += 1
            issues.append(
                {
                    "type": "timing_reason_mismatch",
                    "row": _row_brief(r),
                    "expected": reason_map.get(signal),
                    "actual": reason,
                }
            )

        baseline_delta = _to_float(r.get("baseline_delta"))
        streak = str(r.get("streak_context_label") or "").strip().upper()
        streak_count = _to_float(r.get("streak_count"))
        streak_ok = True
        if streak in {"", "UNKNOWN", "NONE", "NULL"}:
            streak_ok = True
        elif streak == "ABOVE_BASELINE":
            streak_ok = baseline_delta is not None and baseline_delta >= 0.10
        elif streak == "BELOW_BASELINE":
            streak_ok = baseline_delta is not None and baseline_delta <= -0.10
        elif streak in {"HOT", "COLD"}:
            streak_ok = streak_count is not None and streak_count >= 3
        elif streak == "NEUTRAL":
            streak_ok = True
        else:
            streak_ok = False

        if streak_ok:
            checks["streak_label_rule_pass"] += 1
        else:
            checks["streak_label_rule_fail"] += 1
            issues.append(
                {
                    "type": "streak_label_rule_mismatch",
                    "row": _row_brief(r),
                }
            )

        h10 = _to_float(r.get("hit_rate_last_10"))
        hs = _to_float(r.get("hit_rate_season"))
        baseline_calc = None if (h10 is None or hs is None) else (h10 - hs)
        baseline_ok = (_missing(baseline_delta) and baseline_calc is None) or _eq(baseline_delta, baseline_calc)
        if baseline_ok:
            checks["baseline_delta_formula_pass"] += 1
        else:
            checks["baseline_delta_formula_fail"] += 1
            issues.append(
                {
                    "type": "baseline_delta_formula_mismatch",
                    "row": _row_brief(r),
                    "expected": baseline_calc,
                    "actual": baseline_delta,
                }
            )

        consistency = _to_float(r.get("consistency_score"))
        consistency_ok = consistency is None or (0 <= consistency <= 100)
        if consistency_ok:
            checks["consistency_range_pass"] += 1
        else:
            checks["consistency_range_fail"] += 1
            issues.append(
                {
                    "type": "consistency_out_of_range",
                    "row": _row_brief(r),
                    "actual": consistency,
                }
            )

        price_fields = ("best_price", "market_median", "open_over_price", "latest_over_price")
        invalid_fields = []
        for pf in price_fields:
            pv = _to_float(r.get(pf))
            if pv is not None and abs(pv) < 100:
                invalid_fields.append({pf: pv})
        if invalid_fields:
            checks["invalid_american_odds_field_fail"] += 1
            issues.append(
                {
                    "type": "invalid_american_odds_field",
                    "row": _row_brief(r),
                    "invalid_fields": invalid_fields,
                }
            )
        else:
            checks["invalid_american_odds_field_pass"] += 1

    return {"checks": checks, "issues": issues}


def _missing_counts(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    fields = ["timing_reason", "streak_count", "baseline_delta", "consistency_score"]
    return {f: sum(1 for r in rows if _missing(r.get(f))) for f in fields}


def _build_report(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_value_desc = sorted(rows, key=lambda r: _to_float(r.get("value_vs_market")) or float("-inf"), reverse=True)
    by_value_asc = sorted(rows, key=lambda r: _to_float(r.get("value_vs_market")) or float("inf"))
    by_cons_desc = sorted(rows, key=lambda r: _to_float(r.get("consistency_score")) or float("-inf"), reverse=True)
    by_cons_asc = sorted(rows, key=lambda r: _to_float(r.get("consistency_score")) or float("inf"))

    validations = _validate_rows(rows)
    missing = _missing_counts(rows)
    value_series = [_to_float(r.get("value_vs_market")) for r in rows]
    value_clean = [v for v in value_series if v is not None]
    value_distribution = {
        "count_non_null": len(value_clean),
        "count_null": len(rows) - len(value_clean),
        "count_positive": sum(1 for v in value_clean if v > 0),
        "count_zero": sum(1 for v in value_clean if v == 0),
        "count_negative": sum(1 for v in value_clean if v < 0),
        "min": (min(value_clean) if value_clean else None),
        "max": (max(value_clean) if value_clean else None),
        "median": (sorted(value_clean)[len(value_clean) // 2] if value_clean else None),
    }

    samples = {
        "highest_positive_value_vs_market": _sample(by_value_desc, 5),
        "most_negative_value_vs_market": _sample(by_value_asc, 5),
        "highest_consistency_score": _sample(by_cons_desc, 5),
        "lowest_consistency_score": _sample(by_cons_asc, 5),
        "timing_signal_buckets": _group_samples(rows, "timing_signal", 5),
        "streak_context_buckets": _group_samples(rows, "streak_context_label", 5),
        "null_field_rows": _sample(
            [
                r
                for r in rows
                if _missing(r.get("timing_reason"))
                or _missing(r.get("streak_count"))
                or _missing(r.get("baseline_delta"))
                or _missing(r.get("consistency_score"))
            ],
            8,
        ),
    }

    return {
        "row_count": len(rows),
        "bucket_counts": {
            "timing_signal": _bucket_counts(rows, "timing_signal"),
            "streak_context_label": _bucket_counts(rows, "streak_context_label"),
        },
        "missing_counts": missing,
        "value_vs_market_distribution": value_distribution,
        "validation": {
            "checks": validations["checks"],
            "issue_count": len(validations["issues"]),
            "issue_samples": validations["issues"][:30],
        },
        "samples": samples,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="QA / trust checklist for /api/mlb/today/workspace")
    ap.add_argument("--mode", choices=["inprocess", "http"], default="inprocess")
    ap.add_argument("--base-url", default="http://localhost:8000", help="Used only with --mode http")
    ap.add_argument("--limit", type=int, default=5000)
    ap.add_argument(
        "--out-json",
        default="tmp/analysis/mlb_today_workspace_qa_report.json",
        help="Output JSON report path",
    )
    args = ap.parse_args()

    if args.mode == "http":
        payload = _fetch_rows_http(args.base_url, args.limit)
    else:
        payload = _fetch_rows_inprocess(args.limit)

    rows = payload.get("rows") or []
    if not isinstance(rows, list):
        raise RuntimeError("endpoint returned non-list rows")

    report = _build_report(rows)
    out_path = Path(args.out_json).expanduser()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(json.dumps(
        {
            "status": "ok",
            "row_count": report["row_count"],
            "out_json": str(out_path),
            "missing_counts": report["missing_counts"],
            "validation_checks": report["validation"]["checks"],
            "issue_count": report["validation"]["issue_count"],
        },
        indent=2,
    ))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
