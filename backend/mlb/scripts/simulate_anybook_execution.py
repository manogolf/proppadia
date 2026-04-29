#!/usr/bin/env python3
"""Simulate MLB execution with full anybook market access.

This does not use executed tool bets. It reads reconcile rows, chooses the best
available over/under price per player/prop/line, then evaluates model picks.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.mlb.shared.probability_calibration import calibrate_probability, load_calibrator


def _american_to_implied_probability(odds: Any) -> Optional[float]:
    try:
        if odds is None or pd.isna(odds):
            return None
        x = float(odds)
        if x == 0:
            return None
        if x > 0:
            return 100.0 / (x + 100.0)
        return abs(x) / (abs(x) + 100.0)
    except Exception:
        return None


def _profit_per_1u(*, outcome: Any, odds: Any) -> Optional[float]:
    result = str(outcome or "").strip().lower()
    try:
        price = float(odds)
    except Exception:
        return None
    if result == "push":
        return 0.0
    if result == "loss":
        return -1.0
    if result != "win" or price == 0:
        return None
    if price > 0:
        return price / 100.0
    return 100.0 / abs(price)


def _edge_bucket(edge: Any) -> str:
    try:
        if edge is None or pd.isna(edge):
            return "unknown"
        pp = float(edge) * 100.0
    except Exception:
        return "unknown"
    if pp < 0:
        return "< 0pp"
    if pp < 5:
        return "0-5pp"
    if pp < 10:
        return "5-10pp"
    if pp < 15:
        return "10-15pp"
    if pp < 20:
        return "15-20pp"
    return "> 20pp"


def _spearman(df: pd.DataFrame) -> Optional[float]:
    work = df[df["pick_outcome"].isin(["win", "loss"]) & df["edge"].notna()].copy()
    if len(work) < 2:
        return None
    work["actual_win_i"] = work["pick_outcome"].eq("win").astype(int)
    if work["edge"].nunique(dropna=True) <= 1 or work["actual_win_i"].nunique(dropna=True) <= 1:
        return None
    val = work[["edge", "actual_win_i"]].corr(method="spearman").iloc[0, 1]
    return None if pd.isna(val) else float(val)


def _monotonicity(bucket_df: pd.DataFrame) -> str:
    order = ["< 0pp", "0-5pp", "5-10pp", "10-15pp", "15-20pp", "> 20pp"]
    rates: list[float] = []
    for bucket in order:
        row = bucket_df[bucket_df["edge_bucket"].eq(bucket)]
        if not row.empty and pd.notna(row.iloc[0].get("win_rate")):
            rates.append(float(row.iloc[0]["win_rate"]))
    if len(rates) < 2:
        return "insufficient_data"
    if all(a <= b for a, b in zip(rates, rates[1:])):
        return "monotonic"
    if all(a >= b for a, b in zip(rates, rates[1:])):
        return "inverted"
    return "flat_or_mixed"


def _best_price_rows(rows: pd.DataFrame) -> pd.DataFrame:
    idx_cols = [
        "game_date",
        "slate_date",
        "game_id",
        "home_team_code",
        "away_team_code",
        "player_id",
        "player_name",
        "prop_type",
        "market_key",
        "line",
    ]
    work = rows.copy()
    work["price_over_american"] = pd.to_numeric(work["price_over_american"], errors="coerce")
    work["price_under_american"] = pd.to_numeric(work["price_under_american"], errors="coerce")

    over_idx = work.dropna(subset=["price_over_american"]).groupby(idx_cols, dropna=False)["price_over_american"].idxmax()
    under_idx = work.dropna(subset=["price_under_american"]).groupby(idx_cols, dropna=False)["price_under_american"].idxmax()
    over = work.loc[over_idx].copy()
    under = work.loc[under_idx].copy()
    over_keep = idx_cols + ["bookmaker_key", "price_over_american"]
    under_keep = idx_cols + ["bookmaker_key", "price_under_american"]
    over = over[over_keep].rename(columns={"bookmaker_key": "best_over_bookmaker_key", "price_over_american": "best_over_price"})
    under = under[under_keep].rename(columns={"bookmaker_key": "best_under_bookmaker_key", "price_under_american": "best_under_price"})
    best = over.merge(under, on=idx_cols, how="outer")

    model_cols = idx_cols + [
        "model_prob_over",
        "model_prob_under",
        "model_pick_side",
        "model_pick_prob",
        "actual_value",
        "actual_over_outcome",
        "actual_under_outcome",
        "actual_model_pick_outcome",
    ]
    model = (
        work[model_cols]
        .drop_duplicates(subset=idx_cols, keep="first")
        .reset_index(drop=True)
    )
    return best.merge(model, on=idx_cols, how="left")


def _simulate(rows: pd.DataFrame, *, calibration_json: str) -> pd.DataFrame:
    calibrator = load_calibrator(calibration_json) if str(calibration_json or "").strip() else None
    min_prop_samples = int((calibrator or {}).get("min_prop_samples") or 200)
    out = _best_price_rows(rows)
    out["raw_model_prob_over"] = pd.to_numeric(out["model_prob_over"], errors="coerce")
    out["raw_model_prob_under"] = pd.to_numeric(out["model_prob_under"], errors="coerce")
    out["calibrated_prob_over"] = [
        calibrate_probability(calibrator, prop_type=prop, raw_prob=prob, min_prop_samples=min_prop_samples)
        for prop, prob in zip(out["prop_type"], out["raw_model_prob_over"])
    ]
    out["calibrated_prob_under"] = [
        calibrate_probability(calibrator, prop_type=prop, raw_prob=prob, min_prop_samples=min_prop_samples)
        for prop, prob in zip(out["prop_type"], out["raw_model_prob_under"])
    ]
    out["pick_side"] = np.where(
        pd.to_numeric(out["calibrated_prob_over"], errors="coerce") >= pd.to_numeric(out["calibrated_prob_under"], errors="coerce"),
        "over",
        "under",
    )
    out["calibrated_pick_prob"] = np.where(out["pick_side"].eq("over"), out["calibrated_prob_over"], out["calibrated_prob_under"])
    out["best_price"] = np.where(out["pick_side"].eq("over"), out["best_over_price"], out["best_under_price"])
    out["best_bookmaker_key"] = np.where(
        out["pick_side"].eq("over"), out["best_over_bookmaker_key"], out["best_under_bookmaker_key"]
    )
    out["best_implied_probability"] = out["best_price"].map(_american_to_implied_probability)
    out["edge"] = pd.to_numeric(out["calibrated_pick_prob"], errors="coerce") - pd.to_numeric(
        out["best_implied_probability"], errors="coerce"
    )
    out["edge_bucket"] = out["edge"].map(_edge_bucket)
    out["pick_outcome"] = np.where(
        out["pick_side"].eq("over"),
        out["actual_over_outcome"].astype(str).str.lower().str.strip(),
        out["actual_under_outcome"].astype(str).str.lower().str.strip(),
    )
    out["pnl_1u"] = [
        _profit_per_1u(outcome=outcome, odds=price)
        for outcome, price in zip(out["pick_outcome"], out["best_price"])
    ]
    out["selection_baseline"] = True
    out["selection_prob_ge_0_52"] = pd.to_numeric(out["calibrated_pick_prob"], errors="coerce") >= 0.52
    return out


def _summary(df: pd.DataFrame, *, selection_col: str) -> Dict[str, Any]:
    work = df[df[selection_col].eq(True)].copy()
    executable = work[work["best_price"].notna() & work["edge"].notna()].copy()
    resolved = executable[executable["pick_outcome"].isin(["win", "loss", "push"])].copy()
    wins = int(resolved["pick_outcome"].eq("win").sum())
    losses = int(resolved["pick_outcome"].eq("loss").sum())
    pushes = int(resolved["pick_outcome"].eq("push").sum())
    pnl = float(pd.to_numeric(resolved["pnl_1u"], errors="coerce").fillna(0).sum())
    bets_with_pnl = int(pd.to_numeric(resolved["pnl_1u"], errors="coerce").notna().sum())
    return {
        "selection": selection_col,
        "rows": int(len(work)),
        "executable_rows": int(len(executable)),
        "resolved_rows": int(len(resolved)),
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "win_rate": wins / (wins + losses) if (wins + losses) > 0 else None,
        "pnl": pnl,
        "roi": pnl / bets_with_pnl if bets_with_pnl > 0 else None,
        "avg_best_price": float(pd.to_numeric(resolved["best_price"], errors="coerce").mean()),
        "avg_calibrated_pick_prob": float(pd.to_numeric(resolved["calibrated_pick_prob"], errors="coerce").mean()),
        "avg_edge": float(pd.to_numeric(resolved["edge"], errors="coerce").mean()),
        "spearman": _spearman(resolved),
    }


def _bucket_summary(df: pd.DataFrame, *, selection_col: str) -> tuple[pd.DataFrame, str]:
    work = df[
        df[selection_col].eq(True)
        & df["pick_outcome"].isin(["win", "loss"])
        & df["best_price"].notna()
        & df["edge"].notna()
    ].copy()
    if work.empty:
        return pd.DataFrame(), "insufficient_data"
    g = (
        work.groupby("edge_bucket", dropna=False)
        .agg(
            bets=("pick_outcome", "size"),
            wins=("pick_outcome", lambda s: int((s == "win").sum())),
            losses=("pick_outcome", lambda s: int((s == "loss").sum())),
            pnl=("pnl_1u", lambda s: float(pd.to_numeric(s, errors="coerce").fillna(0).sum())),
            avg_calibrated_edge=("edge", "mean"),
            avg_calibrated_pick_prob=("calibrated_pick_prob", "mean"),
            avg_best_price=("best_price", "mean"),
        )
        .reset_index()
    )
    g["win_rate"] = np.where((g["wins"] + g["losses"]) > 0, g["wins"] / (g["wins"] + g["losses"]), np.nan)
    g["roi"] = np.where(g["bets"] > 0, g["pnl"] / g["bets"], np.nan)
    order = {"< 0pp": 0, "0-5pp": 1, "5-10pp": 2, "10-15pp": 3, "15-20pp": 4, "> 20pp": 5, "unknown": 6}
    g["__order"] = g["edge_bucket"].map(lambda x: order.get(x, 99))
    g = g.sort_values(["__order", "edge_bucket"]).drop(columns=["__order"])
    g.insert(0, "selection", selection_col)
    return g, _monotonicity(g)


def main() -> int:
    ap = argparse.ArgumentParser(description="Simulate MLB anybook execution from reconcile rows only.")
    ap.add_argument("--reconcile-csv", default="tmp/mlb_base_vs_market_rows_anybook_full.csv")
    ap.add_argument("--calibration-json", default="artifacts/analysis/mlb/calibration/mlb_probability_calibrator.json")
    ap.add_argument("--out-dir", default="artifacts/analysis/mlb/execution_vs_model/anybook_sim")
    args = ap.parse_args()

    rows = pd.read_csv(Path(args.reconcile_csv), low_memory=False)
    sim = _simulate(rows, calibration_json=str(args.calibration_json))
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    sim.to_csv(out_dir / "anybook_simulated_execution.csv", index=False)

    summaries: list[Dict[str, Any]] = []
    bucket_frames = []
    monotonicities: Dict[str, str] = {}
    for selection_col in ("selection_baseline", "selection_prob_ge_0_52"):
        s = _summary(sim, selection_col=selection_col)
        b, mono = _bucket_summary(sim, selection_col=selection_col)
        s["monotonicity"] = mono
        summaries.append(s)
        if not b.empty:
            bucket_frames.append(b)
        monotonicities[selection_col] = mono

    summary_df = pd.DataFrame(summaries)
    bucket_df = pd.concat(bucket_frames, ignore_index=True) if bucket_frames else pd.DataFrame()
    summary_df.to_csv(out_dir / "anybook_execution_summary.csv", index=False)
    bucket_df.to_csv(out_dir / "anybook_edge_bucket_summary.csv", index=False)

    def pct(v: Any) -> str:
        return "n/a" if v is None or pd.isna(v) else f"{100.0 * float(v):.2f}%"

    lines = [
        "# Anybook Simulated Execution",
        "",
        "Input uses reconcile rows only. Tool/executed-bet CSVs are not used.",
        "",
        f"- Reconcile CSV: `{args.reconcile_csv}`",
        f"- Calibration JSON: `{args.calibration_json}`",
        f"- Simulated rows: {len(sim)}",
        "",
        "## Summary",
        "",
    ]
    for row in summaries:
        lines.extend(
            [
                f"### {row['selection']}",
                f"- Candidate rows: {row['rows']}",
                f"- Executable rows with best price: {row['executable_rows']}",
                f"- Resolved rows: {row['resolved_rows']}",
                f"- Wins/losses/pushes: {row['wins']}-{row['losses']}-{row['pushes']}",
                f"- Win rate: {pct(row['win_rate'])}",
                f"- ROI: {pct(row['roi'])} (pnl={row['pnl']})",
                f"- Average calibrated pick probability: {pct(row['avg_calibrated_pick_prob'])}",
                f"- Average edge: {pct(row['avg_edge'])}",
                f"- Spearman(edge vs outcome): {row['spearman']}",
                f"- Monotonicity: {row['monotonicity']}",
                "",
            ]
        )
    (out_dir / "anybook_summary.md").write_text("\n".join(lines), encoding="utf-8")
    (out_dir / "anybook_summary.json").write_text(json.dumps(summaries, indent=2, default=str), encoding="utf-8")

    print(f"[anybook-sim] rows={len(sim)} out_dir={out_dir}")
    for row in summaries:
        print(
            "[anybook-sim] "
            f"{row['selection']} resolved={row['resolved_rows']} win_rate={row['win_rate']} "
            f"roi={row['roi']} spearman={row['spearman']} monotonicity={row['monotonicity']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
