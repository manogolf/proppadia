#!/usr/bin/env python3
"""Build a next-slate NHL SOG segment policy from a recency training window.

This script learns per-segment (min_ev, min_gap) thresholds from recent
reconcile rows and writes a policy JSON compatible with
`select_sog_candidates_live.py`.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


DEFAULT_THRESHOLDS: dict[str, tuple[float, float]] = {
    "over:1.5": (0.03, 0.04),
    "over:2.5": (0.15, 0.07),
    "over:3.5": (0.03, 0.00),
    "under:1.5": (0.03, 0.02),
    "under:2.5": (0.19, 0.10),
    "under:3.5": (0.03, 0.04),
}

DEFAULT_EV_GRID = [0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.10, 0.12, 0.15, 0.19, 0.22]
DEFAULT_GAP_GRID = [0.00, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.10, 0.12]


@dataclass(frozen=True)
class SegmentResult:
    min_ev: float
    min_gap: float
    train_rows: int
    train_selected_rows: int
    train_roi5: float | None
    train_pnl5_units: float
    fallback_used: bool


def _parse_csv_floats(raw: str) -> list[float]:
    out: list[float] = []
    for tok in str(raw).split(","):
        tok = tok.strip()
        if not tok:
            continue
        out.append(float(tok))
    if not out:
        raise SystemExit("empty numeric grid")
    return sorted(set(out))


def _as_bool(v: Any) -> bool:
    s = str(v).strip().lower()
    return s in {"1", "true", "t", "yes", "y"}


def _to_line_key(v: Any) -> str:
    x = pd.to_numeric(pd.Series([v]), errors="coerce").iloc[0]
    if pd.isna(x):
        return str(v)
    return f"{float(x):.1f}"


def _prob_to_american(p: float) -> int | None:
    if not (0.0 < p < 1.0):
        return None
    if p >= 0.5:
        return int(-round(100.0 * p / (1.0 - p)))
    return int(round(100.0 * (1.0 - p) / p))


def _win_profit_units(price: float) -> float:
    if price > 0:
        return price / 100.0
    return 100.0 / abs(price)


def _load_fallback_thresholds(path: Path | None) -> dict[str, tuple[float, float]]:
    if path is None:
        return dict(DEFAULT_THRESHOLDS)
    if not path.exists():
        raise SystemExit(f"fallback policy json not found: {path}")

    data = json.loads(path.read_text())
    if "thresholds_for_next_slate" in data:
        data = data["thresholds_for_next_slate"]

    out: dict[str, tuple[float, float]] = {}
    for seg, vals in data.items():
        if not isinstance(vals, dict):
            continue
        if "min_ev" not in vals or "min_gap" not in vals:
            continue
        out[str(seg)] = (float(vals["min_ev"]), float(vals["min_gap"]))

    if not out:
        return dict(DEFAULT_THRESHOLDS)
    return out


def _prepare_training_rows(
    rows_csv: Path,
    train_from: date,
    train_to_exclusive: date,
    *,
    segments: set[str],
    under15_min_model_prob: float,
    under15_max_price: float,
    over35_max_price: float,
    max_fair_favorite: int,
) -> tuple[pd.DataFrame, int]:
    df = pd.read_csv(rows_csv)
    raw_rows = int(len(df))

    required = ["game_date", "line", "model_pick", "model_wl", "p_base", "p_mkt", "price_over", "publishable"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise SystemExit(f"rows csv missing required columns: {missing}")

    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce").dt.date
    df = df[(df["game_date"] >= train_from) & (df["game_date"] < train_to_exclusive)].copy()
    df = df[df["publishable"].map(_as_bool)].copy()

    for c in ["line", "p_base", "p_mkt", "price_over"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["model_pick"] = df["model_pick"].astype(str).str.strip().str.lower()
    df = df[df["model_pick"].isin({"over", "under"})].copy()

    df["model_wl"] = df["model_wl"].astype(str).str.strip().str.upper()
    df = df[df["model_wl"].isin({"W", "L"})].copy()

    df["line_key"] = df["line"].map(_to_line_key)
    df["segment"] = df["model_pick"] + ":" + df["line_key"]
    df = df[df["segment"].isin(segments)].copy()

    df["model_side_prob"] = np.where(df["model_pick"].eq("over"), df["p_base"], 1.0 - df["p_base"])
    df["market_side_prob"] = np.where(df["model_pick"].eq("over"), df["p_mkt"], 1.0 - df["p_mkt"])
    df = df[
        df["model_side_prob"].between(0.0, 1.0, inclusive="neither")
        & df["market_side_prob"].between(0.0, 1.0, inclusive="neither")
    ].copy()

    # Reconstruct side price for under rows from market-side probability when needed.
    df["price_side"] = np.where(
        df["model_pick"].eq("over"),
        pd.to_numeric(df["price_over"], errors="coerce"),
        df["market_side_prob"].map(_prob_to_american),
    )
    df["price_side"] = pd.to_numeric(df["price_side"], errors="coerce")
    df = df[df["price_side"].notna()].copy()

    df["ev_side"] = (df["model_side_prob"] / df["market_side_prob"]) - 1.0
    df["edge_side"] = df["model_side_prob"] - df["market_side_prob"]
    df["profit_units"] = np.where(
        df["model_wl"].eq("W"),
        df["price_side"].map(_win_profit_units),
        -1.0,
    )

    # Mirror live-policy gates so threshold learning and live selection align.
    df = df[~((df["segment"] == "under:1.5") & (df["model_side_prob"] < float(under15_min_model_prob)))].copy()
    df = df[~((df["segment"] == "under:1.5") & (df["price_side"] > float(under15_max_price)))].copy()
    df = df[~((df["segment"] == "over:3.5") & (df["price_side"] > float(over35_max_price)))].copy()

    fair = df["model_side_prob"].map(_prob_to_american)
    df = df[(fair > 0) | (fair >= int(max_fair_favorite))].copy()

    return df.reset_index(drop=True), raw_rows


def _choose_threshold_for_segment(
    seg_rows: pd.DataFrame,
    *,
    fallback_min_ev: float,
    fallback_min_gap: float,
    ev_grid: list[float],
    gap_grid: list[float],
    min_train_rows_per_segment: int,
) -> SegmentResult:
    best: tuple[float, float, int, float, float] | None = None
    # tuple format: (pnl, roi, n, min_ev, min_gap)

    for min_ev in ev_grid:
        for min_gap in gap_grid:
            sel = seg_rows[(seg_rows["ev_side"] >= float(min_ev)) & (seg_rows["edge_side"] >= float(min_gap))]
            n = int(len(sel))
            if n < int(min_train_rows_per_segment):
                continue
            pnl = float(sel["profit_units"].sum())
            roi = float(sel["profit_units"].mean()) if n else -999.0
            cand = (pnl, roi, n, float(min_ev), float(min_gap))
            if best is None or cand > best:
                best = cand

    if best is None:
        return SegmentResult(
            min_ev=float(fallback_min_ev),
            min_gap=float(fallback_min_gap),
            train_rows=int(len(seg_rows)),
            train_selected_rows=0,
            train_roi5=None,
            train_pnl5_units=0.0,
            fallback_used=True,
        )

    best_pnl, best_roi, best_n, best_min_ev, best_min_gap = best
    return SegmentResult(
        min_ev=float(best_min_ev),
        min_gap=float(best_min_gap),
        train_rows=int(len(seg_rows)),
        train_selected_rows=int(best_n),
        train_roi5=float(best_roi),
        train_pnl5_units=float(best_pnl),
        fallback_used=False,
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="Build next-slate recency policy JSON for NHL SOG selection.")
    ap.add_argument("--rows-csv", default="tmp/nhl_sog_base_vs_betonline_rows.csv")
    ap.add_argument("--as-of-date", required=True, help="Slate date YYYY-MM-DD. Training uses rows strictly before this date.")
    ap.add_argument("--window-days", type=int, default=30)
    ap.add_argument("--min-train-rows-per-segment", type=int, default=25)
    ap.add_argument("--fallback-policy-json", default="tmp/nhl_sog_walkforward_summary.json")
    ap.add_argument("--ev-grid", default="0.03,0.04,0.05,0.06,0.07,0.08,0.10,0.12,0.15,0.19,0.22")
    ap.add_argument("--gap-grid", default="0.00,0.01,0.02,0.03,0.04,0.05,0.06,0.07,0.08,0.10,0.12")
    ap.add_argument("--under15-min-model-prob", type=float, default=0.65)
    ap.add_argument("--under15-max-price", type=float, default=100.0)
    ap.add_argument("--over35-max-price", type=float, default=130.0)
    ap.add_argument("--max-fair-favorite", type=int, default=-300)
    ap.add_argument("--out-json", required=True)
    args = ap.parse_args()

    rows_csv = Path(args.rows_csv)
    if not rows_csv.exists():
        raise SystemExit(f"rows csv not found: {rows_csv}")

    as_of_date = date.fromisoformat(str(args.as_of_date))
    if int(args.window_days) <= 0:
        raise SystemExit("--window-days must be > 0")
    train_from = as_of_date - timedelta(days=int(args.window_days))

    fallback_path = Path(args.fallback_policy_json) if str(args.fallback_policy_json).strip() else None
    fallback_thresholds = _load_fallback_thresholds(fallback_path)
    segments = set(fallback_thresholds.keys())
    if not segments:
        segments = set(DEFAULT_THRESHOLDS.keys())

    ev_grid = _parse_csv_floats(args.ev_grid) if str(args.ev_grid).strip() else list(DEFAULT_EV_GRID)
    gap_grid = _parse_csv_floats(args.gap_grid) if str(args.gap_grid).strip() else list(DEFAULT_GAP_GRID)

    train_rows, raw_rows = _prepare_training_rows(
        rows_csv=rows_csv,
        train_from=train_from,
        train_to_exclusive=as_of_date,
        segments=segments,
        under15_min_model_prob=float(args.under15_min_model_prob),
        under15_max_price=float(args.under15_max_price),
        over35_max_price=float(args.over35_max_price),
        max_fair_favorite=int(args.max_fair_favorite),
    )

    if train_rows.empty:
        raise SystemExit(
            f"no usable training rows in window [{train_from.isoformat()}, {as_of_date.isoformat()})"
        )

    thresholds_for_next_slate: dict[str, dict[str, Any]] = {}
    segment_diagnostics: dict[str, dict[str, Any]] = {}
    for seg in sorted(segments):
        default_ev, default_gap = fallback_thresholds.get(seg, DEFAULT_THRESHOLDS.get(seg, (0.03, 0.04)))
        seg_rows = train_rows[train_rows["segment"] == seg].copy()
        result = _choose_threshold_for_segment(
            seg_rows,
            fallback_min_ev=float(default_ev),
            fallback_min_gap=float(default_gap),
            ev_grid=ev_grid,
            gap_grid=gap_grid,
            min_train_rows_per_segment=int(args.min_train_rows_per_segment),
        )
        thresholds_for_next_slate[seg] = {
            "min_ev": float(result.min_ev),
            "min_gap": float(result.min_gap),
            "train_wilson_lb": None,
        }
        segment_diagnostics[seg] = {
            "train_rows": int(result.train_rows),
            "train_selected_rows": int(result.train_selected_rows),
            "train_roi5": (None if result.train_roi5 is None else float(result.train_roi5)),
            "train_pnl5_units": float(result.train_pnl5_units),
            "fallback_used": bool(result.fallback_used),
            "fallback_min_ev": float(default_ev),
            "fallback_min_gap": float(default_gap),
        }

    payload: dict[str, Any] = {
        "ok": True,
        "generated_at_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "as_of_date": as_of_date.isoformat(),
        "window_days": int(args.window_days),
        "train_window": {
            "from_date_inclusive": train_from.isoformat(),
            "to_date_exclusive": as_of_date.isoformat(),
        },
        "input_rows_csv": str(rows_csv),
        "input_rows_raw": int(raw_rows),
        "input_rows_usable": int(len(train_rows)),
        "min_train_rows_per_segment": int(args.min_train_rows_per_segment),
        "ev_grid": [float(x) for x in ev_grid],
        "gap_grid": [float(x) for x in gap_grid],
        "thresholds_for_next_slate": thresholds_for_next_slate,
        "segment_diagnostics": segment_diagnostics,
    }

    out_json = Path(args.out_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(payload, indent=2))
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()

