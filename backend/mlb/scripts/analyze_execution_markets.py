#!/usr/bin/env python3
"""Market-specific execution-vs-model diagnostics for MLB."""

from __future__ import annotations

import argparse
import json
import re
import unicodedata
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import numpy as np
import pandas as pd


OFFSHORE_BOOK_ALIASES = {
    "betonline",
    "betonlineag",
    "betonline ag",
    "propkingz",
    "prop kingz",
    "propking",
    "betdsi",
    "betdsi superbook",
    "bookmaker",
    "bookmaker eu",
    "bovada",
}


def _norm_text(v: Any) -> str:
    return str(v if v is not None else "").strip()


def _norm_name(v: Any) -> str:
    text = unicodedata.normalize("NFKD", _norm_text(v))
    text = "".join(ch for ch in text if not unicodedata.combining(ch)).lower()
    text = re.sub(r"[^a-z0-9 ]+", "", text)
    return " ".join(text.split())


def _norm_book(v: Any) -> str:
    text = unicodedata.normalize("NFKD", _norm_text(v))
    text = "".join(ch for ch in text if not unicodedata.combining(ch)).lower()
    text = re.sub(r"[^a-z0-9 ]+", " ", text)
    return " ".join(text.split())


def _date_norm(v: Any) -> str:
    dt = pd.to_datetime(v, errors="coerce")
    if pd.isna(dt):
        return ""
    return pd.Timestamp(dt).date().isoformat()


def _edge_bucket(edge: Any) -> str:
    x = pd.to_numeric(pd.Series([edge]), errors="coerce").iloc[0]
    if pd.isna(x):
        return "unknown"
    pp = float(x) * 100.0
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


def _monotonicity(bucket_df: pd.DataFrame) -> str:
    order = ["< 0pp", "0-5pp", "5-10pp", "10-15pp", "15-20pp", "> 20pp"]
    rates: list[float] = []
    for bucket in order:
        row = bucket_df[bucket_df["edge_bucket"].eq(bucket)]
        if not row.empty and pd.notna(row.iloc[0].get("bet_win_rate")):
            rates.append(float(row.iloc[0]["bet_win_rate"]))
    if len(rates) < 2:
        return "insufficient_data"
    if all(a <= b for a, b in zip(rates, rates[1:])):
        return "monotonic"
    if all(a >= b for a, b in zip(rates, rates[1:])):
        return "inverted"
    return "flat_or_mixed"


def _spearman(df: pd.DataFrame, *, edge_col: str) -> Optional[float]:
    corr_df = df[df["bet_result"].isin(["win", "loss"]) & pd.to_numeric(df[edge_col], errors="coerce").notna()].copy()
    if len(corr_df) < 2:
        return None
    corr_df["actual_win_i"] = corr_df["bet_result"].eq("win").astype(int)
    if corr_df[edge_col].nunique(dropna=True) <= 1 or corr_df["actual_win_i"].nunique(dropna=True) <= 1:
        return None
    val = corr_df[[edge_col, "actual_win_i"]].corr(method="spearman").iloc[0, 1]
    return None if pd.isna(val) else float(val)


def _rate(num: Any, den: Any) -> float:
    den = float(den)
    return float(num) / den if den else float("nan")


def _summarize_group(g: pd.DataFrame, *, edge_col: str) -> Dict[str, Any]:
    wins = int(g["bet_result"].eq("win").sum())
    losses = int(g["bet_result"].eq("loss").sum())
    pushes = int(g["bet_result"].eq("push").sum())
    bets = int(len(g))
    pnl = float(pd.to_numeric(g["pnl"], errors="coerce").fillna(0).sum())
    model_correct = int(g["model_pick_outcome"].eq("win").sum()) if "model_pick_outcome" in g.columns else 0
    model_wrong = int(g["model_pick_outcome"].eq("loss").sum()) if "model_pick_outcome" in g.columns else 0
    model_wl = model_correct + model_wrong
    return {
        "bets": bets,
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "bet_win_rate": _rate(wins, wins + losses),
        "pnl": pnl,
        "roi": _rate(pnl, bets),
        "model_correct": model_correct,
        "model_wrong": model_wrong,
        "model_accuracy": _rate(model_correct, model_wl),
        "avg_calibrated_edge": float(pd.to_numeric(g[edge_col], errors="coerce").mean()),
        "spearman": _spearman(g, edge_col=edge_col),
    }


def _load_anybook_reconcile(path: Path) -> pd.DataFrame:
    rec = pd.read_csv(path, low_memory=False)
    for col in ["game_date", "player_name", "prop_type", "line", "implied_over", "implied_under", "bookmaker_key"]:
        if col not in rec.columns:
            rec[col] = pd.NA
    out = rec.copy()
    out["date_norm"] = out["game_date"].map(_date_norm)
    out["player_name_key"] = out["player_name"].map(_norm_name)
    out["prop_type_norm"] = out["prop_type"].astype(str).str.strip().str.lower()
    out["line_norm"] = pd.to_numeric(out["line"], errors="coerce").round(1)
    frames = []
    for side, implied_col in (("over", "implied_over"), ("under", "implied_under")):
        s = out[["date_norm", "player_name_key", "prop_type_norm", "line_norm", "bookmaker_key", implied_col]].copy()
        s["side_norm"] = side
        s["anybook_implied_probability"] = pd.to_numeric(s[implied_col], errors="coerce")
        s = s.drop(columns=[implied_col])
        frames.append(s)
    side = pd.concat(frames, ignore_index=True)
    side = side[side["anybook_implied_probability"].notna()].copy()
    return side.drop_duplicates(
        subset=["date_norm", "player_name_key", "prop_type_norm", "line_norm", "side_norm"],
        keep="first",
    )


def _prepare_market_views(execution: pd.DataFrame, reconcile: pd.DataFrame) -> pd.DataFrame:
    df = execution.copy()
    df["book_norm"] = df.get("tool__Book", "").map(_norm_book)
    df["is_offshore_book"] = df["book_norm"].isin(OFFSHORE_BOOK_ALIASES)
    df["player_name_key"] = df["player_name_norm"].map(_norm_name)
    df["line_norm"] = pd.to_numeric(df["line_norm"], errors="coerce").round(1)
    df["calibrated_model_prob"] = pd.to_numeric(df["calibrated_model_prob"], errors="coerce")
    df["implied_prob_from_bet_odds"] = pd.to_numeric(df["implied_prob_from_bet_odds"], errors="coerce")

    keys = ["date_norm", "player_name_key", "prop_type_norm", "line_norm", "side_norm"]
    df = df.merge(reconcile[keys + ["anybook_implied_probability", "bookmaker_key"]], on=keys, how="left")
    df["anybook_edge"] = df["calibrated_model_prob"] - df["anybook_implied_probability"]
    df["offshore_edge"] = df["calibrated_model_prob"] - df["implied_prob_from_bet_odds"]

    anybook = df[df["matched_reconcile"].eq(True)].copy()
    anybook["market_type"] = "anybook"
    anybook["market_edge"] = anybook["anybook_edge"].where(anybook["anybook_edge"].notna(), anybook["offshore_edge"])
    anybook["market_implied_probability"] = anybook["anybook_implied_probability"].where(
        anybook["anybook_implied_probability"].notna(), anybook["implied_prob_from_bet_odds"]
    )

    offshore = df[df["matched_reconcile"].eq(True) & df["is_offshore_book"].eq(True)].copy()
    offshore["market_type"] = "offshore"
    offshore["market_edge"] = offshore["offshore_edge"]
    offshore["market_implied_probability"] = offshore["implied_prob_from_bet_odds"]

    return pd.concat([anybook, offshore], ignore_index=True)


def _summaries(views: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    resolved = views[views["bet_result"].isin(["win", "loss", "push"]) & views["market_edge"].notna()].copy()
    market_rows = []
    market_prop_rows = []
    bucket_rows = []
    bucket_prop_rows = []

    def add_bucket_rows(g: pd.DataFrame, *, market_type: str, prop_type: Optional[str] = None) -> str:
        tmp = g[g["bet_result"].isin(["win", "loss"])].copy()
        tmp["edge_bucket"] = tmp["market_edge"].map(_edge_bucket)
        bg = (
            tmp.groupby("edge_bucket", dropna=False)
            .agg(
                bets=("bet_result", "size"),
                wins=("bet_result", lambda s: int((s == "win").sum())),
                losses=("bet_result", lambda s: int((s == "loss").sum())),
                pnl=("pnl", lambda s: float(pd.to_numeric(s, errors="coerce").fillna(0).sum())),
                avg_calibrated_edge=("market_edge", "mean"),
            )
            .reset_index()
        )
        bg["bet_win_rate"] = np.where((bg["wins"] + bg["losses"]) > 0, bg["wins"] / (bg["wins"] + bg["losses"]), np.nan)
        bg["roi"] = np.where(bg["bets"] > 0, bg["pnl"] / bg["bets"], np.nan)
        mono = _monotonicity(bg)
        sp = _spearman(g, edge_col="market_edge")
        order = {"< 0pp": 0, "0-5pp": 1, "5-10pp": 2, "10-15pp": 3, "15-20pp": 4, "> 20pp": 5, "unknown": 6}
        for _, row in bg.assign(__order=bg["edge_bucket"].map(lambda x: order.get(x, 99))).sort_values("__order").iterrows():
            payload = {
                "market_type": market_type,
                "edge_bucket": row["edge_bucket"],
                "bets": int(row["bets"]),
                "wins": int(row["wins"]),
                "losses": int(row["losses"]),
                "bet_win_rate": float(row["bet_win_rate"]) if pd.notna(row["bet_win_rate"]) else np.nan,
                "pnl": float(row["pnl"]),
                "roi": float(row["roi"]) if pd.notna(row["roi"]) else np.nan,
                "avg_calibrated_edge": float(row["avg_calibrated_edge"]) if pd.notna(row["avg_calibrated_edge"]) else np.nan,
                "monotonicity": mono,
                "spearman": sp,
            }
            if prop_type is None:
                bucket_rows.append(payload)
            else:
                payload["prop_type"] = prop_type
                bucket_prop_rows.append(payload)
        return mono

    for market_type, g in resolved.groupby("market_type"):
        summary = _summarize_group(g, edge_col="market_edge")
        summary["market_type"] = market_type
        summary["edge_monotonicity"] = add_bucket_rows(g, market_type=market_type)
        market_rows.append(summary)
        for prop_type, pg in g.groupby("prop_type_norm"):
            ps = _summarize_group(pg, edge_col="market_edge")
            ps["market_type"] = market_type
            ps["prop_type"] = prop_type
            ps["edge_monotonicity"] = add_bucket_rows(pg, market_type=market_type, prop_type=str(prop_type))
            market_prop_rows.append(ps)

    return (
        pd.DataFrame(market_rows),
        pd.DataFrame(bucket_rows),
        pd.DataFrame(market_prop_rows),
        pd.DataFrame(bucket_prop_rows),
    )


def _signal_flags(market_prop: pd.DataFrame) -> Dict[str, Any]:
    if market_prop.empty:
        return {}
    work = market_prop.copy()
    work["positive_spearman"] = pd.to_numeric(work["spearman"], errors="coerce") > 0
    work["positive_roi"] = pd.to_numeric(work["roi"], errors="coerce") > 0
    work["monotonic"] = work["edge_monotonicity"].eq("monotonic")
    pivot = work.pivot_table(index="prop_type", columns="market_type", values="positive_spearman", aggfunc="max", fill_value=False)
    anybook_pos = set(pivot.index[pivot.get("anybook", False).astype(bool)]) if "anybook" in pivot else set()
    offshore_pos = set(pivot.index[pivot.get("offshore", False).astype(bool)]) if "offshore" in pivot else set()
    offshore_fail = work[(work["market_type"].eq("offshore")) & (~work["positive_spearman"]) & (~work["positive_roi"])]["prop_type"].tolist()
    return {
        "props_that_work_in_both_markets": sorted(anybook_pos & offshore_pos),
        "props_that_only_work_in_anybook": sorted(anybook_pos - offshore_pos),
        "props_that_fail_in_offshore": sorted(set(offshore_fail)),
        "positive_roi_by_market": {
            market: sorted(g.loc[g["positive_roi"], "prop_type"].tolist()) for market, g in work.groupby("market_type")
        },
        "monotonic_by_market": {
            market: sorted(g.loc[g["monotonic"], "prop_type"].tolist()) for market, g in work.groupby("market_type")
        },
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Build MLB execution diagnostics by market type.")
    ap.add_argument("--execution-csv", default="artifacts/analysis/mlb/execution_vs_model/extended_clean/execution_vs_model.csv")
    ap.add_argument("--reconcile-csv", default="tmp/mlb_base_vs_market_rows_anybook_full.csv")
    ap.add_argument("--out-dir", default="artifacts/analysis/mlb/execution_vs_model/extended_clean")
    args = ap.parse_args()

    execution = pd.read_csv(Path(args.execution_csv), low_memory=False)
    reconcile = _load_anybook_reconcile(Path(args.reconcile_csv))
    views = _prepare_market_views(execution, reconcile)
    market_summary, bucket_summary, market_prop, bucket_prop = _summaries(views)
    flags = _signal_flags(market_prop)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    views.to_csv(out_dir / "execution_with_market_type.csv", index=False)
    market_summary.to_csv(out_dir / "execution_by_market_summary.csv", index=False)
    bucket_summary.to_csv(out_dir / "edge_bucket_by_market.csv", index=False)
    market_prop.to_csv(out_dir / "execution_by_market_prop_summary.csv", index=False)
    bucket_prop.to_csv(out_dir / "edge_bucket_by_market_prop.csv", index=False)
    (out_dir / "market_signal_flags.json").write_text(json.dumps(flags, indent=2), encoding="utf-8")

    lines = [
        "# Execution Market Breakdown",
        "",
        f"- Execution CSV: `{args.execution_csv}`",
        f"- Reconcile CSV: `{args.reconcile_csv}`",
        f"- Props that work in both markets: {', '.join(flags.get('props_that_work_in_both_markets', [])) or 'none'}",
        f"- Props that only work in anybook: {', '.join(flags.get('props_that_only_work_in_anybook', [])) or 'none'}",
        f"- Props that fail in offshore: {', '.join(flags.get('props_that_fail_in_offshore', [])) or 'none'}",
        "",
    ]
    (out_dir / "market_breakdown.md").write_text("\n".join(lines), encoding="utf-8")

    print(f"[market-analysis] views={len(views)}")
    print(f"[market-analysis] out_dir={out_dir}")
    print(f"[market-analysis] flags={json.dumps(flags, sort_keys=True)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
