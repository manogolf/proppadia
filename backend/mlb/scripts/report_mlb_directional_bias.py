#!/usr/bin/env python3
"""Measure per-prop directional probability bias and test a simple correction.

CSV-only reporting. This does not alter model artifacts or production logic.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Iterable, Sequence

import numpy as np
import pandas as pd


DEFAULT_ROOT = Path("artifacts/analysis/mlb/execution_vs_model")
DEFAULT_OUT_CSV = Path("backend/mlb/exports/model_performance/directional_bias.csv")


def _date_key(value: Any) -> str:
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return ""
    return dt.date().isoformat()


def _clean(value: Any) -> str:
    text = str(value if value is not None else "").strip()
    if text.lower() in {"", "nan", "none", "null", "<na>"}:
        return ""
    return text


def _num(series: pd.Series | Any, index: pd.Index | None = None) -> pd.Series:
    if isinstance(series, pd.Series):
        return pd.to_numeric(series, errors="coerce")
    if index is None:
        raise ValueError("index is required when converting a scalar")
    return pd.Series(np.nan, index=index, dtype="float64")


def _discover_reconcile_files(root: Path, from_date: str = "", to_date: str = "") -> list[Path]:
    files: list[tuple[str, Path]] = []
    for path in root.glob("*/reconcile_rows.csv"):
        date = path.parent.name
        if not pd.notna(pd.to_datetime(date, errors="coerce")):
            continue
        if from_date and date < from_date:
            continue
        if to_date and date > to_date:
            continue
        files.append((date, path))
    return [path for _, path in sorted(files)]


def _load_rows(paths: Iterable[Path]) -> pd.DataFrame:
    frames = []
    required = {
        "game_date",
        "prop_type",
        "model_prob_over",
        "model_prob_under",
        "actual_over_outcome",
        "actual_under_outcome",
        "pnl_over_1u",
        "pnl_under_1u",
    }
    for path in paths:
        df = pd.read_csv(path, low_memory=False)
        missing = sorted(required - set(df.columns))
        if missing:
            print(f"[directional-bias] skip {path}: missing {missing}")
            continue
        df = df.copy()
        df["source_date"] = path.parent.name
        frames.append(df)
    if not frames:
        raise SystemExit("No compatible reconcile_rows.csv files found.")
    return pd.concat(frames, ignore_index=True)


def _resolved_rows(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work["date"] = work["game_date"].map(_date_key)
    work["prop_type"] = work["prop_type"].map(lambda v: _clean(v).lower())
    work["model_prob_over"] = _num(work["model_prob_over"])
    work["model_prob_under"] = _num(work["model_prob_under"])
    work["pnl_over_1u"] = _num(work["pnl_over_1u"])
    work["pnl_under_1u"] = _num(work["pnl_under_1u"])
    work["actual_over_outcome"] = work["actual_over_outcome"].map(lambda v: _clean(v).lower())
    work["actual_under_outcome"] = work["actual_under_outcome"].map(lambda v: _clean(v).lower())
    work = work[
        work["actual_over_outcome"].isin({"win", "loss"})
        & work["actual_under_outcome"].isin({"win", "loss"})
        & work["model_prob_over"].notna()
        & work["model_prob_under"].notna()
    ].copy()
    work["actual_over"] = work["actual_over_outcome"].eq("win").astype(float)
    work["actual_under"] = work["actual_under_outcome"].eq("win").astype(float)
    return work


def _roi_summary(df: pd.DataFrame, side_col: str, pnl_col: str) -> dict[str, Any]:
    bets = int(len(df))
    if bets == 0:
        return {"bets": 0, "wins": 0, "losses": 0, "profit_units": 0.0, "win_rate": np.nan, "roi": np.nan}
    wins = int(
        np.where(
            df[side_col].eq("over"),
            df["actual_over_outcome"].eq("win"),
            df["actual_under_outcome"].eq("win"),
        ).sum()
    )
    losses = int(bets - wins)
    profit = float(pd.to_numeric(df[pnl_col], errors="coerce").fillna(0.0).sum())
    return {
        "bets": bets,
        "wins": wins,
        "losses": losses,
        "profit_units": profit,
        "win_rate": wins / bets,
        "roi": profit / bets,
    }


def build_report(rows: pd.DataFrame) -> pd.DataFrame:
    resolved = _resolved_rows(rows)
    if resolved.empty:
        raise SystemExit("No resolved non-push rows found in reconcile inputs.")

    summaries = []
    for prop_type, group in resolved.groupby("prop_type", dropna=False):
        model_over = float(group["model_prob_over"].mean())
        model_under = float(group["model_prob_under"].mean())
        actual_over = float(group["actual_over"].mean())
        actual_under = float(group["actual_under"].mean())
        over_bias = model_over - actual_over
        under_bias = model_under - actual_under

        scored = group.copy()
        scored["adjusted_model_prob_over"] = (scored["model_prob_over"] - over_bias).clip(0.001, 0.999)
        scored["adjusted_model_prob_under"] = 1.0 - scored["adjusted_model_prob_over"]
        scored["adjusted_pick_side"] = np.where(
            scored["adjusted_model_prob_over"].ge(scored["adjusted_model_prob_under"]), "over", "under"
        )
        scored["adjusted_pnl"] = np.where(
            scored["adjusted_pick_side"].eq("over"), scored["pnl_over_1u"], scored["pnl_under_1u"]
        )
        scored["raw_pick_side"] = np.where(scored["model_prob_over"].ge(scored["model_prob_under"]), "over", "under")
        scored["raw_pnl"] = np.where(scored["raw_pick_side"].eq("over"), scored["pnl_over_1u"], scored["pnl_under_1u"])

        raw = _roi_summary(scored, "raw_pick_side", "raw_pnl")
        adjusted = _roi_summary(scored, "adjusted_pick_side", "adjusted_pnl")
        summaries.append(
            {
                "prop_type": prop_type,
                "rows": int(len(group)),
                "p_actual_over": actual_over,
                "p_actual_under": actual_under,
                "model_predicted_over": model_over,
                "model_predicted_under": model_under,
                "over_bias": over_bias,
                "under_bias": under_bias,
                "raw_bets": raw["bets"],
                "raw_wins": raw["wins"],
                "raw_losses": raw["losses"],
                "raw_profit_units": raw["profit_units"],
                "raw_win_rate": raw["win_rate"],
                "raw_roi": raw["roi"],
                "adjusted_bets": adjusted["bets"],
                "adjusted_wins": adjusted["wins"],
                "adjusted_losses": adjusted["losses"],
                "adjusted_profit_units": adjusted["profit_units"],
                "adjusted_win_rate": adjusted["win_rate"],
                "adjusted_roi": adjusted["roi"],
                "roi_delta": adjusted["roi"] - raw["roi"],
            }
        )

    out = pd.DataFrame(summaries).sort_values(["over_bias", "prop_type"], ascending=[False, True])

    # Add an overall row using each row's prop-specific correction.
    bias_map = out.set_index("prop_type")["over_bias"].to_dict()
    scored = resolved.copy()
    scored["over_bias"] = scored["prop_type"].map(bias_map)
    scored["adjusted_model_prob_over"] = (scored["model_prob_over"] - scored["over_bias"]).clip(0.001, 0.999)
    scored["adjusted_model_prob_under"] = 1.0 - scored["adjusted_model_prob_over"]
    scored["adjusted_pick_side"] = np.where(
        scored["adjusted_model_prob_over"].ge(scored["adjusted_model_prob_under"]), "over", "under"
    )
    scored["adjusted_pnl"] = np.where(
        scored["adjusted_pick_side"].eq("over"), scored["pnl_over_1u"], scored["pnl_under_1u"]
    )
    scored["raw_pick_side"] = np.where(scored["model_prob_over"].ge(scored["model_prob_under"]), "over", "under")
    scored["raw_pnl"] = np.where(scored["raw_pick_side"].eq("over"), scored["pnl_over_1u"], scored["pnl_under_1u"])
    raw = _roi_summary(scored, "raw_pick_side", "raw_pnl")
    adjusted = _roi_summary(scored, "adjusted_pick_side", "adjusted_pnl")
    overall = {
        "prop_type": "ALL",
        "rows": int(len(scored)),
        "p_actual_over": float(scored["actual_over"].mean()),
        "p_actual_under": float(scored["actual_under"].mean()),
        "model_predicted_over": float(scored["model_prob_over"].mean()),
        "model_predicted_under": float(scored["model_prob_under"].mean()),
        "over_bias": float(scored["model_prob_over"].mean() - scored["actual_over"].mean()),
        "under_bias": float(scored["model_prob_under"].mean() - scored["actual_under"].mean()),
        "raw_bets": raw["bets"],
        "raw_wins": raw["wins"],
        "raw_losses": raw["losses"],
        "raw_profit_units": raw["profit_units"],
        "raw_win_rate": raw["win_rate"],
        "raw_roi": raw["roi"],
        "adjusted_bets": adjusted["bets"],
        "adjusted_wins": adjusted["wins"],
        "adjusted_losses": adjusted["losses"],
        "adjusted_profit_units": adjusted["profit_units"],
        "adjusted_win_rate": adjusted["win_rate"],
        "adjusted_roi": adjusted["roi"],
        "roi_delta": adjusted["roi"] - raw["roi"],
    }
    return pd.concat([pd.DataFrame([overall]), out], ignore_index=True)


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Measure MLB model directional bias and adjusted-pick ROI.")
    ap.add_argument("--reconcile-root", default=str(DEFAULT_ROOT))
    ap.add_argument("--from-date", default="")
    ap.add_argument("--to-date", default="")
    ap.add_argument("--out-csv", default=str(DEFAULT_OUT_CSV))
    args = ap.parse_args(list(argv) if argv is not None else None)

    paths = _discover_reconcile_files(Path(args.reconcile_root), args.from_date, args.to_date)
    rows = _load_rows(paths)
    out = build_report(rows)
    out_path = Path(args.out_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)

    overall = out[out["prop_type"].eq("ALL")].iloc[0]
    print(
        "[directional-bias] "
        f"files={len(paths)} rows={int(overall['rows'])} "
        f"raw_roi={overall['raw_roi']:.3f} adjusted_roi={overall['adjusted_roi']:.3f} "
        f"out_csv={out_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
