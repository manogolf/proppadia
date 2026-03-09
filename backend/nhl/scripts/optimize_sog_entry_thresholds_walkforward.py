#!/usr/bin/env python3
"""Walk-forward optimizer for NHL SOG entry thresholds by line x side.

Learns per-segment thresholds on prior dates only:
  segment = "<side>:<line>" (e.g., "over:2.5", "under:1.5")
  policy  = ev_side >= min_ev AND edge_side >= min_gap

Inputs:
  - row CSV from reconcile_sog_base_vs_betonline_by_month.py

Outputs:
  - selected picks (out-of-time)
  - threshold history
  - summary JSON including thresholds for next slate
"""

from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class SegmentThreshold:
    min_ev: float
    min_gap: float
    train_rows: int
    train_wins: int
    train_losses: int
    train_win_pct: float | None
    train_wilson_lb: float | None
    train_expected_roi: float | None
    train_expected_pnl: float | None
    train_realized_roi: float | None
    train_realized_pnl: float | None


def _to_bool(v: Any) -> bool:
    return str(v).strip().lower() in {"1", "true", "t", "yes", "y"}


def _to_line(v: Any) -> str:
    x = pd.to_numeric(pd.Series([v]), errors="coerce").iloc[0]
    if pd.isna(x):
        return str(v)
    return f"{float(x):.1f}"


def _parse_grid(s: str) -> list[float]:
    vals = [x.strip() for x in str(s).split(",") if x.strip()]
    out = sorted({round(float(v), 6) for v in vals})
    if not out:
        raise ValueError("empty grid")
    return out


def _wilson_lower_bound(wins: int, n: int, z: float = 1.96) -> float:
    if n <= 0:
        return 0.0
    p = wins / n
    den = 1 + (z * z) / n
    center = p + (z * z) / (2 * n)
    spread = z * math.sqrt((p * (1 - p) + (z * z) / (4 * n)) / n)
    return (center - spread) / den


def _prob_to_american(p: float) -> float | None:
    if not (0.0 < float(p) < 1.0):
        return None
    if p >= 0.5:
        return -100.0 * float(p) / (1.0 - float(p))
    return 100.0 * (1.0 - float(p)) / float(p)


def _apply_slippage(price: float, slippage_cents: float) -> float:
    p = float(price)
    s = max(0.0, float(slippage_cents))
    if p > 0:
        return max(100.0, p - s)
    return p - s


def _win_profit_units(american_price: float) -> float:
    p = float(american_price)
    return p / 100.0 if p > 0 else 100.0 / abs(p)


def _prepare(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["publishable"] = out["publishable"].map(_to_bool)
    out = out[out["publishable"]].copy()

    out["game_date"] = out["game_date"].astype(str)
    out["line"] = pd.to_numeric(out["line"], errors="coerce")
    out["p_base"] = pd.to_numeric(out["p_base"], errors="coerce")
    out["p_mkt"] = pd.to_numeric(out["p_mkt"], errors="coerce")
    out["model_pick"] = out["model_pick"].astype(str).str.lower().str.strip()
    out["model_wl"] = out["model_wl"].astype(str).str.upper().str.strip()

    out = out[out["model_pick"].isin(["over", "under"])].copy()
    out = out[out["model_wl"].isin(["W", "L"])].copy()

    over_mask = out["model_pick"] == "over"
    under_mask = out["model_pick"] == "under"

    out["model_side_prob"] = pd.NA
    out["market_side_prob"] = pd.NA
    out.loc[over_mask, "model_side_prob"] = out.loc[over_mask, "p_base"]
    out.loc[over_mask, "market_side_prob"] = out.loc[over_mask, "p_mkt"]
    out.loc[under_mask, "model_side_prob"] = 1.0 - out.loc[under_mask, "p_base"]
    out.loc[under_mask, "market_side_prob"] = 1.0 - out.loc[under_mask, "p_mkt"]

    out["model_side_prob"] = pd.to_numeric(out["model_side_prob"], errors="coerce")
    out["market_side_prob"] = pd.to_numeric(out["market_side_prob"], errors="coerce")
    out = out[
        out["model_side_prob"].between(0.0, 1.0, inclusive="neither")
        & out["market_side_prob"].between(0.0, 1.0, inclusive="neither")
    ].copy()

    out["ev_side"] = (out["model_side_prob"] / out["market_side_prob"]) - 1.0
    out["edge_side"] = out["model_side_prob"] - out["market_side_prob"]
    out["line_key"] = out["line"].map(_to_line)
    out["segment"] = out["model_pick"] + ":" + out["line_key"]
    out["is_win"] = (out["model_wl"] == "W").astype(int)
    out["market_side_price"] = out["market_side_prob"].map(_prob_to_american)
    out["market_side_price"] = pd.to_numeric(out["market_side_price"], errors="coerce")
    out = out[out["market_side_price"].notna()].copy()
    return out


def _segment_perf(
    df: pd.DataFrame,
    min_ev: float,
    min_gap: float,
    objective_slippage_cents: float,
) -> dict[str, Any]:
    sel = df[(df["ev_side"] >= float(min_ev)) & (df["edge_side"] >= float(min_gap))]
    n = int(len(sel))
    if n == 0:
        return {
            "rows": 0,
            "wins": 0,
            "losses": 0,
            "win_pct": None,
            "lb": None,
            "expected_roi": None,
            "expected_pnl": None,
            "realized_roi": None,
            "realized_pnl": None,
        }
    wins = int(sel["is_win"].sum())
    losses = n - wins
    win_pct = wins / n if n else None
    lb = _wilson_lower_bound(wins, n) if n else None

    prices = pd.to_numeric(sel["market_side_price"], errors="coerce").map(
        lambda p: _apply_slippage(float(p), objective_slippage_cents)
    )
    win_profit = prices.map(_win_profit_units)
    model_prob = pd.to_numeric(sel["model_side_prob"], errors="coerce")
    is_win = pd.to_numeric(sel["is_win"], errors="coerce")

    expected = (model_prob * win_profit) - (1.0 - model_prob)
    realized = (is_win * win_profit) - (1.0 - is_win)
    expected_roi = float(expected.mean()) if not expected.empty else None
    realized_roi = float(realized.mean()) if not realized.empty else None

    return {
        "rows": n,
        "wins": wins,
        "losses": losses,
        "win_pct": win_pct,
        "lb": lb,
        "expected_roi": expected_roi,
        "expected_pnl": (None if expected_roi is None else float(expected_roi * n)),
        "realized_roi": realized_roi,
        "realized_pnl": (None if realized_roi is None else float(realized_roi * n)),
    }


def _candidate_score(perf: dict[str, Any], objective: str) -> tuple[float, float, float, float]:
    rows = float(perf.get("rows") or 0.0)
    lb = float(perf.get("lb") or 0.0)
    wp = float(perf.get("win_pct") or 0.0)
    expected_roi = float(perf.get("expected_roi") or -1e9)
    expected_pnl = float(perf.get("expected_pnl") or -1e9)
    realized_roi = float(perf.get("realized_roi") or -1e9)
    realized_pnl = float(perf.get("realized_pnl") or -1e9)
    if objective == "expected_pnl":
        return (expected_pnl, expected_roi, rows, lb)
    if objective == "expected_roi":
        return (expected_roi, expected_pnl, rows, lb)
    if objective == "realized_pnl":
        return (realized_pnl, realized_roi, rows, lb)
    if objective == "realized_roi":
        return (realized_roi, realized_pnl, rows, lb)
    # objective == "wilson_lb"
    return (lb, rows, wp, expected_roi)


def _choose_threshold(
    seg_df: pd.DataFrame,
    ev_grid: list[float],
    gap_grid: list[float],
    min_train_rows: int,
    fallback_min_ev: float,
    fallback_min_gap: float,
    objective: str,
    objective_slippage_cents: float,
) -> SegmentThreshold:
    best_score: tuple[float, float, float, float] | None = None
    best_payload: dict[str, Any] | None = None

    for ev in ev_grid:
        for gap in gap_grid:
            perf = _segment_perf(seg_df, ev, gap, objective_slippage_cents)
            rows = int(perf["rows"])
            if rows < int(min_train_rows):
                continue
            score = _candidate_score(perf, objective)
            if best_score is None or score > best_score:
                best_score = score
                best_payload = {"ev": ev, "gap": gap, **perf}

    if best_payload is None:
        perf = _segment_perf(seg_df, fallback_min_ev, fallback_min_gap, objective_slippage_cents)
        return SegmentThreshold(
            min_ev=float(fallback_min_ev),
            min_gap=float(fallback_min_gap),
            train_rows=int(perf["rows"]),
            train_wins=int(perf["wins"]),
            train_losses=int(perf["losses"]),
            train_win_pct=(None if perf["win_pct"] is None else float(perf["win_pct"])),
            train_wilson_lb=(None if perf["lb"] is None else float(perf["lb"])),
            train_expected_roi=(None if perf["expected_roi"] is None else float(perf["expected_roi"])),
            train_expected_pnl=(None if perf["expected_pnl"] is None else float(perf["expected_pnl"])),
            train_realized_roi=(None if perf["realized_roi"] is None else float(perf["realized_roi"])),
            train_realized_pnl=(None if perf["realized_pnl"] is None else float(perf["realized_pnl"])),
        )

    payload = best_payload
    return SegmentThreshold(
        min_ev=float(payload["ev"]),
        min_gap=float(payload["gap"]),
        train_rows=int(payload["rows"]),
        train_wins=int(payload["wins"]),
        train_losses=int(payload["losses"]),
        train_win_pct=(None if payload["win_pct"] is None else float(payload["win_pct"])),
        train_wilson_lb=(None if payload["lb"] is None else float(payload["lb"])),
        train_expected_roi=(None if payload["expected_roi"] is None else float(payload["expected_roi"])),
        train_expected_pnl=(None if payload["expected_pnl"] is None else float(payload["expected_pnl"])),
        train_realized_roi=(None if payload["realized_roi"] is None else float(payload["realized_roi"])),
        train_realized_pnl=(None if payload["realized_pnl"] is None else float(payload["realized_pnl"])),
    )


def _summarize(df: pd.DataFrame) -> dict[str, Any]:
    def _wl(frame: pd.DataFrame) -> dict[str, Any]:
        n = int(len(frame))
        w = int((frame["model_wl"] == "W").sum())
        l = int((frame["model_wl"] == "L").sum())
        return {
            "rows": n,
            "wins": w,
            "losses": l,
            "win_pct": (w / (w + l) if (w + l) else None),
        }

    out: dict[str, Any] = {"overall": _wl(df)}

    by_seg: dict[str, Any] = {}
    for seg, sub in df.groupby("segment", dropna=False):
        by_seg[str(seg)] = _wl(sub)
    out["by_segment"] = by_seg

    by_month: dict[str, Any] = {}
    tmp = df.copy()
    tmp["month"] = tmp["game_date"].str.slice(0, 7)
    for month, sub in tmp.groupby("month", dropna=False):
        by_month[str(month)] = _wl(sub)
    out["by_month"] = by_month

    return out


def _summarize_threshold_history(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {}
    out: dict[str, Any] = {}
    for seg, sub in df.groupby("segment", dropna=False):
        row: dict[str, Any] = {
            "reopt_points": int(len(sub)),
            "median_min_ev": float(median(sub["min_ev"].astype(float).tolist())),
            "median_min_gap": float(median(sub["min_gap"].astype(float).tolist())),
            "last_min_ev": float(sub.iloc[-1]["min_ev"]),
            "last_min_gap": float(sub.iloc[-1]["min_gap"]),
        }
        for col in (
            "train_expected_roi",
            "train_expected_pnl",
            "train_realized_roi",
            "train_realized_pnl",
        ):
            if col in sub.columns and sub[col].notna().any():
                vals = pd.to_numeric(sub[col], errors="coerce").dropna()
                if not vals.empty:
                    row[f"median_{col}"] = float(median(vals.astype(float).tolist()))
                    row[f"last_{col}"] = float(pd.to_numeric(sub.iloc[-1][col], errors="coerce"))
        out[str(seg)] = row
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Walk-forward threshold optimizer (line x side) for NHL SOG.")
    ap.add_argument("--rows-csv", required=True, help="Row-level CSV from reconcile_sog_base_vs_betonline_by_month.py")
    ap.add_argument("--from-date", default="", help="Optional inclusive lower date YYYY-MM-DD")
    ap.add_argument("--to-date", default="", help="Optional inclusive upper date YYYY-MM-DD")
    ap.add_argument("--warmup-days", type=int, default=30, help="Distinct game dates before first OOT test day.")
    ap.add_argument("--reopt-every-days", type=int, default=1, help="How frequently to re-optimize thresholds.")
    ap.add_argument("--min-train-rows", type=int, default=80, help="Minimum selected train rows required per segment.")
    ap.add_argument("--ev-grid", default="0.03,0.04,0.05,0.06,0.07,0.08,0.09,0.10")
    ap.add_argument("--gap-grid", default="0.00,0.01,0.02,0.03,0.04,0.05,0.06,0.07,0.08")
    ap.add_argument(
        "--objective",
        default="expected_roi",
        choices=["expected_roi", "expected_pnl", "realized_roi", "realized_pnl", "wilson_lb"],
        help="Threshold ranking objective on train rows.",
    )
    ap.add_argument(
        "--objective-slippage-cents",
        type=float,
        default=0.0,
        help="Apply cents slippage to market-side prices while scoring objective.",
    )
    ap.add_argument("--fallback-min-ev", type=float, default=0.05)
    ap.add_argument("--fallback-min-gap", type=float, default=0.04)
    ap.add_argument("--out-picks-csv", default="tmp/nhl_sog_walkforward_selected.csv")
    ap.add_argument("--out-threshold-history-csv", default="tmp/nhl_sog_walkforward_threshold_history.csv")
    ap.add_argument("--out-summary-json", default="tmp/nhl_sog_walkforward_summary.json")
    args = ap.parse_args()

    rows_csv = Path(args.rows_csv)
    if not rows_csv.exists():
        raise SystemExit(f"rows csv not found: {rows_csv}")

    ev_grid = _parse_grid(args.ev_grid)
    gap_grid = _parse_grid(args.gap_grid)

    raw = pd.read_csv(rows_csv)
    df = _prepare(raw)

    if args.from_date:
        df = df[df["game_date"] >= str(args.from_date)].copy()
    if args.to_date:
        df = df[df["game_date"] <= str(args.to_date)].copy()

    dates = sorted(df["game_date"].dropna().astype(str).unique().tolist())
    if len(dates) <= int(args.warmup_days):
        raise SystemExit(
            f"not enough distinct dates: have={len(dates)} warmup_days={int(args.warmup_days)}"
        )

    segments = sorted(df["segment"].dropna().astype(str).unique().tolist())
    current_thr: dict[str, SegmentThreshold] = {}

    picks_rows: list[pd.DataFrame] = []
    threshold_rows: list[dict[str, Any]] = []

    for i, day in enumerate(dates):
        if i < int(args.warmup_days):
            continue

        need_reopt = (not current_thr) or ((i - int(args.warmup_days)) % int(args.reopt_every_days) == 0)
        if need_reopt:
            train = df[df["game_date"] < day].copy()
            for seg in segments:
                seg_train = train[train["segment"] == seg].copy()
                chosen = _choose_threshold(
                    seg_df=seg_train,
                    ev_grid=ev_grid,
                    gap_grid=gap_grid,
                    min_train_rows=int(args.min_train_rows),
                    fallback_min_ev=float(args.fallback_min_ev),
                    fallback_min_gap=float(args.fallback_min_gap),
                    objective=str(args.objective),
                    objective_slippage_cents=float(args.objective_slippage_cents),
                )
                current_thr[seg] = chosen
                threshold_rows.append(
                    {
                        "reopt_date": day,
                        "segment": seg,
                        "min_ev": chosen.min_ev,
                        "min_gap": chosen.min_gap,
                        "train_rows": chosen.train_rows,
                        "train_wins": chosen.train_wins,
                        "train_losses": chosen.train_losses,
                        "train_win_pct": chosen.train_win_pct,
                        "train_wilson_lb": chosen.train_wilson_lb,
                        "train_expected_roi": chosen.train_expected_roi,
                        "train_expected_pnl": chosen.train_expected_pnl,
                        "train_realized_roi": chosen.train_realized_roi,
                        "train_realized_pnl": chosen.train_realized_pnl,
                    }
                )

        test = df[df["game_date"] == day].copy()
        if test.empty:
            continue

        test["applied_min_ev"] = test["segment"].map(lambda s: current_thr[s].min_ev if s in current_thr else args.fallback_min_ev)
        test["applied_min_gap"] = test["segment"].map(
            lambda s: current_thr[s].min_gap if s in current_thr else args.fallback_min_gap
        )
        test["selected"] = (test["ev_side"] >= test["applied_min_ev"]) & (test["edge_side"] >= test["applied_min_gap"])
        day_sel = test[test["selected"]].copy()
        if not day_sel.empty:
            picks_rows.append(day_sel)

    selected = pd.concat(picks_rows, ignore_index=True) if picks_rows else pd.DataFrame(columns=df.columns.tolist())
    thr_hist = pd.DataFrame(threshold_rows)

    out_picks = Path(args.out_picks_csv)
    out_picks.parent.mkdir(parents=True, exist_ok=True)
    keep_cols = [
        "game_date",
        "game_id",
        "player_id",
        "player_name",
        "line",
        "segment",
        "model_pick",
        "model_wl",
        "actual_sog",
        "p_base",
        "p_mkt",
        "model_side_prob",
        "market_side_prob",
        "edge_side",
        "ev_side",
        "applied_min_ev",
        "applied_min_gap",
    ]
    keep_cols = [c for c in keep_cols if c in selected.columns]
    selected[keep_cols].to_csv(out_picks, index=False)

    out_thr = Path(args.out_threshold_history_csv)
    out_thr.parent.mkdir(parents=True, exist_ok=True)
    thr_hist.to_csv(out_thr, index=False)

    # Final thresholds represent "next slate" settings after the last reopt.
    final_thresholds = {
        seg: {
            "min_ev": thr.min_ev,
            "min_gap": thr.min_gap,
            "train_rows": thr.train_rows,
            "train_wins": thr.train_wins,
            "train_losses": thr.train_losses,
            "train_win_pct": thr.train_win_pct,
            "train_wilson_lb": thr.train_wilson_lb,
            "train_expected_roi": thr.train_expected_roi,
            "train_expected_pnl": thr.train_expected_pnl,
            "train_realized_roi": thr.train_realized_roi,
            "train_realized_pnl": thr.train_realized_pnl,
        }
        for seg, thr in sorted(current_thr.items())
    }

    summary = {
        "config": {
            "rows_csv": str(rows_csv),
            "from_date": args.from_date or None,
            "to_date": args.to_date or None,
            "warmup_days": int(args.warmup_days),
            "reopt_every_days": int(args.reopt_every_days),
            "min_train_rows": int(args.min_train_rows),
            "ev_grid": ev_grid,
            "gap_grid": gap_grid,
            "objective": str(args.objective),
            "objective_slippage_cents": float(args.objective_slippage_cents),
            "fallback_min_ev": float(args.fallback_min_ev),
            "fallback_min_gap": float(args.fallback_min_gap),
        },
        "coverage": {
            "distinct_dates_total": int(len(dates)),
            "distinct_dates_oot": int(max(0, len(dates) - int(args.warmup_days))),
            "segments": segments,
        },
        "selected": _summarize(selected),
        "threshold_history": _summarize_threshold_history(thr_hist),
        "thresholds_for_next_slate": final_thresholds,
        "outputs": {
            "picks_csv": str(out_picks),
            "threshold_history_csv": str(out_thr),
            "summary_json": str(Path(args.out_summary_json)),
        },
    }

    out_json = Path(args.out_summary_json)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
