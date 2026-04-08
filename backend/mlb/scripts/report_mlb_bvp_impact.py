#!/usr/bin/env python3
"""Report MLB prediction sensitivity to BvP/PvB features.

Compares per-row model probability with:
  1) BvP/PvB hydration enabled
  2) BvP/PvB hydration disabled

Input rows come from today's slate CSV, joined to wide predictions CSV for
team_id/team context needed by prepare_prop.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from collections import defaultdict
from datetime import datetime
from statistics import mean, median
from typing import Any, Dict, List, Optional, Sequence, Tuple

from backend.domains.mlb import prop_workflow
from backend.mlb.prediction import make_prediction as prediction_runtime
from backend.shared.db.pg import pg_fetchone


def _to_int(v: Any) -> Optional[int]:
    try:
        if v is None or v == "":
            return None
        return int(v)
    except Exception:
        return None


def _to_float(v: Any) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None


def _key(game_id: Any, player_id: Any, prop_type: Any) -> Tuple[str, str, str]:
    return (str(game_id or "").strip(), str(player_id or "").strip(), str(prop_type or "").strip().lower())


def _load_wide_context(path: str) -> Dict[Tuple[str, str, str], Dict[str, Any]]:
    out: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    with open(path, "r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            k = _key(row.get("game_id"), row.get("player_id"), row.get("prop_type"))
            if not all(k):
                continue
            out[k] = row
    return out


def _predict_with_toggle(payload: Dict[str, Any], *, bvp_enabled: bool) -> Tuple[Optional[float], Dict[str, Any], str]:
    prev = os.getenv("MLB_BVP_FEATURES_ENABLED")
    os.environ["MLB_BVP_FEATURES_ENABLED"] = "1" if bvp_enabled else "0"
    try:
        prepared = prop_workflow.prepare_prop(payload)
        pred = prop_workflow.predict_prop(str(payload.get("prop_type") or ""), prepared)
        p = _to_float(pred.get("probability_over"))
        strategy = str(((pred.get("model_meta") or {}).get("strategy") or "unknown")).strip().lower() or "unknown"
        return p, prepared, strategy
    except Exception:
        return None, {}, "error"
    finally:
        if prev is None:
            os.environ.pop("MLB_BVP_FEATURES_ENABLED", None)
        else:
            os.environ["MLB_BVP_FEATURES_ENABLED"] = prev


def _ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def _append_history(path: str, summary: Dict[str, Any]) -> None:
    if not path or path.strip() in {"-", "none", "null"}:
        return
    _ensure_parent_dir(path)
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(summary, ensure_ascii=True))
        fh.write("\n")


def _model_bvp_feature_inventory(prop_types: Sequence[str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for prop in sorted({str(p).strip().lower() for p in prop_types if str(p).strip()}):
        cols = prediction_runtime._input_columns_for(prop) or []  # noqa: SLF001
        bvp_cols = [str(c) for c in cols if str(c).startswith("bvp_")]
        out.append(
            {
                "prop_type": prop,
                "feature_count": len(cols),
                "bvp_feature_count": len(bvp_cols),
                "bvp_features": bvp_cols,
                "uses_bvp": len(bvp_cols) > 0,
            }
        )
    return out


def _db_available() -> bool:
    try:
        row = pg_fetchone("SELECT 1 AS ok") or {}
        return int(row.get("ok") or 0) == 1
    except Exception:
        return False


def main(argv: Optional[Sequence[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Compare MLB probabilities with vs without BvP/PvB feature hydration.")
    ap.add_argument("--slate-csv", default="backend/mlb/data/processed/mlb_slate_output.csv")
    ap.add_argument("--wide-csv", default="backend/mlb/data/processed/mlb_predictions_wide_calibrated.csv")
    ap.add_argument("--out-json", default="tmp/analysis/mlb_bvp_impact_summary.json")
    ap.add_argument("--out-csv", default="tmp/analysis/mlb_bvp_impact_rows.csv")
    ap.add_argument("--history-jsonl", default="artifacts/analysis/mlb/mlb_bvp_impact_history.jsonl")
    ap.add_argument("--label-date", default="", help="Optional label date (YYYY-MM-DD) for runbook-style tracking.")
    ap.add_argument("--max-rows", type=int, default=0, help="Optional cap for quick checks (0 = all rows).")
    ap.add_argument(
        "--require-db",
        type=int,
        default=1,
        help="Fail fast when DB is unavailable (default: 1). Set 0 to allow best-effort mode.",
    )
    args = ap.parse_args(list(argv) if argv is not None else None)

    if not _db_available():
        msg = "[bvp-impact] DB unavailable (check backend/.env / DB connectivity)"
        if int(args.require_db) == 1:
            print(f"{msg}; exiting")
            return 2
        print(f"{msg}; continuing in best-effort mode")

    wide_ctx = _load_wide_context(args.wide_csv)

    rows_out: List[Dict[str, Any]] = []
    counters: Dict[str, int] = defaultdict(int)
    strategy_counts_with: Dict[str, int] = defaultdict(int)
    strategy_counts_without: Dict[str, int] = defaultdict(int)
    abs_deltas: List[float] = []
    props_seen: List[str] = []
    by_prop_abs: Dict[str, List[float]] = defaultdict(list)
    by_prop_nonzero: Dict[str, int] = defaultdict(int)
    by_prop_rows: Dict[str, int] = defaultdict(int)
    rows_with_bvp_payload = 0
    label_dates = set()

    with open(args.slate_csv, "r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            counters["slate_rows"] += 1
            if args.max_rows > 0 and counters["evaluated"] >= args.max_rows:
                break

            prop_type = str(row.get("prop_type") or "").strip().lower()
            props_seen.append(prop_type)
            game_date = str(row.get("game_date") or "").strip()
            if game_date:
                label_dates.add(game_date)

            line = _to_float(row.get("line"))
            if line is None:
                counters["skip_missing_line"] += 1
                continue

            k = _key(row.get("game_id"), row.get("player_id"), prop_type)
            wide = wide_ctx.get(k) or {}
            team_id = _to_int(wide.get("team_id"))
            team_abbr = str(wide.get("team") or "").strip() or None
            if team_id is None and not team_abbr:
                counters["skip_missing_team_context"] += 1
                continue

            payload = {
                "player_id": _to_int(row.get("player_id")),
                "player_name": row.get("player_name"),
                "team_id": team_id,
                "team_abbr": team_abbr,
                "game_date": row.get("game_date"),
                "game_id": _to_int(row.get("game_id")),
                "game_type": row.get("game_type"),
                "prop_type": prop_type,
                "prop_value": line,
                "over_under": "over",
            }

            p_with, prepared_with, strategy_with = _predict_with_toggle(payload, bvp_enabled=True)
            p_without, _, strategy_without = _predict_with_toggle(payload, bvp_enabled=False)
            if p_with is None or p_without is None:
                counters["skip_predict_error"] += 1
                continue
            strategy_counts_with[strategy_with] += 1
            strategy_counts_without[strategy_without] += 1

            bvp_keys = [k2 for k2 in prepared_with.keys() if str(k2).startswith("bvp_")]
            if bvp_keys:
                rows_with_bvp_payload += 1

            delta = float(p_with - p_without)
            abs_delta = abs(delta)
            nonzero = abs_delta > 1e-9

            counters["evaluated"] += 1
            if nonzero:
                counters["nonzero_delta_rows"] += 1
            abs_deltas.append(abs_delta)
            by_prop_abs[prop_type].append(abs_delta)
            by_prop_rows[prop_type] += 1
            if nonzero:
                by_prop_nonzero[prop_type] += 1

            rows_out.append(
                {
                    "game_date": game_date,
                    "game_id": row.get("game_id"),
                    "player_id": row.get("player_id"),
                    "player_name": row.get("player_name"),
                    "prop_type": prop_type,
                    "line": line,
                    "prob_with_bvp": round(p_with, 8),
                    "prob_without_bvp": round(p_without, 8),
                    "delta_prob": round(delta, 8),
                    "abs_delta_prob": round(abs_delta, 8),
                    "bvp_feature_keys_count": len(bvp_keys),
                }
            )

    rows_out.sort(key=lambda r: float(r.get("abs_delta_prob") or 0.0), reverse=True)

    _ensure_parent_dir(args.out_json)
    if rows_out:
        _ensure_parent_dir(args.out_csv)
        with open(args.out_csv, "w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=list(rows_out[0].keys()))
            writer.writeheader()
            writer.writerows(rows_out)

    prop_summary = []
    for prop, vals in sorted(by_prop_abs.items()):
        n = len(vals)
        prop_summary.append(
            {
                "prop_type": prop,
                "rows": n,
                "rows_nonzero_delta": int(by_prop_nonzero.get(prop, 0)),
                "pct_nonzero_delta": round((by_prop_nonzero.get(prop, 0) / n) * 100.0, 2) if n else 0.0,
                "mean_abs_delta_prob": round(mean(vals), 8) if vals else 0.0,
                "median_abs_delta_prob": round(median(vals), 8) if vals else 0.0,
                "max_abs_delta_prob": round(max(vals), 8) if vals else 0.0,
            }
        )

    model_inventory = _model_bvp_feature_inventory(props_seen)
    props_using_bvp = sorted(
        str(row.get("prop_type"))
        for row in model_inventory
        if bool(row.get("uses_bvp"))
    )
    generated_at_utc = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    label_date = str(args.label_date or "").strip()
    if not label_date:
        if len(label_dates) == 1:
            label_date = next(iter(label_dates))
        elif len(label_dates) > 1:
            label_date = f"{min(label_dates)}..{max(label_dates)}"
        else:
            label_date = "unknown"

    summary = {
        "generated_at_utc": generated_at_utc,
        "label_date": label_date,
        "slate_csv": args.slate_csv,
        "wide_csv": args.wide_csv,
        "out_csv": args.out_csv,
        "history_jsonl": args.history_jsonl,
        "rows_total_slate": int(counters.get("slate_rows", 0)),
        "rows_evaluated": int(counters.get("evaluated", 0)),
        "rows_nonzero_delta": int(counters.get("nonzero_delta_rows", 0)),
        "pct_nonzero_delta": round(
            (counters.get("nonzero_delta_rows", 0) / counters.get("evaluated", 1)) * 100.0
            if counters.get("evaluated", 0) > 0
            else 0.0,
            2,
        ),
        "rows_with_bvp_payload": int(rows_with_bvp_payload),
        "pct_rows_with_bvp_payload": round(
            (rows_with_bvp_payload / counters.get("evaluated", 1)) * 100.0
            if counters.get("evaluated", 0) > 0
            else 0.0,
            2,
        ),
        "mean_abs_delta_prob": round(mean(abs_deltas), 8) if abs_deltas else 0.0,
        "median_abs_delta_prob": round(median(abs_deltas), 8) if abs_deltas else 0.0,
        "max_abs_delta_prob": round(max(abs_deltas), 8) if abs_deltas else 0.0,
        "skips": {
            "missing_line": int(counters.get("skip_missing_line", 0)),
            "missing_team_context": int(counters.get("skip_missing_team_context", 0)),
            "predict_error": int(counters.get("skip_predict_error", 0)),
        },
        "strategy_counts_with_bvp": {k: int(v) for k, v in sorted(strategy_counts_with.items())},
        "strategy_counts_without_bvp": {k: int(v) for k, v in sorted(strategy_counts_without.items())},
        "props_using_bvp": props_using_bvp,
        "prop_impact": prop_summary,
        "model_bvp_feature_inventory": model_inventory,
    }

    with open(args.out_json, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2)
    _append_history(args.history_jsonl, summary)

    print(
        f"[bvp-impact] date={label_date} evaluated={summary['rows_evaluated']} "
        f"nonzero={summary['rows_nonzero_delta']} ({summary['pct_nonzero_delta']}%) "
        f"rows_with_bvp_payload={summary['rows_with_bvp_payload']} ({summary['pct_rows_with_bvp_payload']}%) "
        f"mean_abs_delta={summary['mean_abs_delta_prob']}"
    )
    if props_using_bvp:
        print("[bvp-impact] model_props_using_bvp=" + ",".join(props_using_bvp))
    else:
        print("[bvp-impact] model_props_using_bvp=<none>")
    if strategy_counts_with:
        print(
            "[bvp-impact] strategy_with_bvp="
            + ",".join(f"{k}:{v}" for k, v in sorted(strategy_counts_with.items()))
        )
    if strategy_counts_without:
        print(
            "[bvp-impact] strategy_without_bvp="
            + ",".join(f"{k}:{v}" for k, v in sorted(strategy_counts_without.items()))
        )
    top_props = sorted(
        prop_summary,
        key=lambda row: float(row.get("mean_abs_delta_prob") or 0.0),
        reverse=True,
    )[:8]
    for row in top_props:
        print(
            "[bvp-impact] prop="
            + str(row.get("prop_type"))
            + f" rows={int(row.get('rows') or 0)} "
            + f"nonzero={float(row.get('pct_nonzero_delta') or 0.0):.2f}% "
            + f"mean_abs_delta={float(row.get('mean_abs_delta_prob') or 0.0):.6f} "
            + f"max_abs_delta={float(row.get('max_abs_delta_prob') or 0.0):.6f}"
        )
    print(f"[bvp-impact] summary_json={args.out_json}")
    if rows_out:
        print(f"[bvp-impact] rows_csv={args.out_csv}")
    if args.history_jsonl and args.history_jsonl.strip() not in {"-", "none", "null"}:
        print(f"[bvp-impact] history_jsonl={args.history_jsonl}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
