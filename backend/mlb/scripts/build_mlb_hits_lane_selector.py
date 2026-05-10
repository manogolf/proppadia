#!/usr/bin/env python3
"""Build a validation-only hits lane selector from ranked lane outputs.

This is a selection/reporting layer, not a model change. Initial lane rules are
fixed and intentionally simple:
- UNDER 0.5: direct hitless model, top decile only.
- OVER: residual ranker, rank bucket 9 only.
- Optional Quick Card hits rows as a separate lane.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


DEFAULT_RESIDUAL_OVER = Path("backend/mlb/exports/model_v2/ranking/validation/hits_rank_mapper_validation.csv")
DEFAULT_DIRECT_UNDER = Path("backend/mlb/exports/model_v2/ranking/validation/hits_05_under_direct_target_audit.csv")
DEFAULT_RECONCILE_ROOT = Path("artifacts/analysis/mlb/execution_vs_model")
DEFAULT_QUICK_CARD_ROOT = Path("backend/mlb/exports/quick_card")
DEFAULT_OUT_CSV = Path("backend/mlb/exports/model_v2/lanes/hits_lane_selector_validation.csv")
DEFAULT_SUMMARY_JSON = Path("backend/mlb/exports/model_v2/lanes/hits_lane_selector_summary.json")
DEFAULT_FROM_DATE = "2026-04-09"
DEFAULT_TO_DATE = "2026-05-08"


def _clean(value: Any) -> str:
    text = str(value if value is not None else "").strip()
    if text.lower() in {"", "nan", "none", "null", "<na>"}:
        return ""
    return text


def _date_key(value: Any) -> str:
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return ""
    return dt.date().isoformat()


def _norm_name(value: Any) -> str:
    return " ".join(_clean(value).lower().split())


def _line_key(value: Any) -> float | None:
    val = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(val):
        return None
    return float(round(float(val), 3))


def _bool_win(series: pd.Series) -> pd.Series:
    if series.dtype == bool:
        return series
    return series.astype(str).str.strip().str.lower().isin({"true", "1", "yes", "win", "won"})


def _profit_from_price(price: Any, win: Any) -> float | None:
    if pd.isna(win):
        return None
    px = pd.to_numeric(pd.Series([price]), errors="coerce").iloc[0]
    if pd.isna(px):
        return None
    if not bool(win):
        return -1.0
    if px > 0:
        return float(px / 100.0)
    if px < 0:
        return float(100.0 / abs(px))
    return None


def _discover_reconcile_files(root: Path, from_date: str, to_date: str) -> list[Path]:
    files: list[tuple[str, Path]] = []
    for path in root.glob("*/reconcile_rows.csv"):
        date = path.parent.name
        parsed = pd.to_datetime(date, errors="coerce")
        if pd.isna(parsed):
            continue
        if from_date <= date <= to_date:
            files.append((date, path))
    return [path for _, path in sorted(files)]


def _load_reconcile(root: Path, from_date: str, to_date: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for path in _discover_reconcile_files(root, from_date, to_date):
        df = pd.read_csv(path, low_memory=False)
        df["source_reconcile_file"] = str(path)
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out["date_norm"] = out["game_date"].map(_date_key)
    out["player_name_norm"] = out["player_name"].map(_norm_name)
    out["prop_type_norm"] = out["prop_type"].astype(str).str.strip().str.lower()
    out["line_norm"] = out["line"].map(_line_key)
    return out


def _selector_cols(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "date",
        "player",
        "prop_type",
        "side",
        "line",
        "source_lane",
        "rank_bucket",
        "win_rate_estimate",
        "odds",
        "actual_win",
        "pnl",
    ]
    for col in cols:
        if col not in df.columns:
            df[col] = np.nan
    return df[cols].copy()


def _load_under(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, low_memory=False)
    needed = {"game_date", "player_name", "prop_type", "line", "under_win", "price_under_american", "pnl_under_1u", "rank_bucket"}
    if not needed.issubset(df.columns):
        return pd.DataFrame()
    out = df[
        df["prop_type"].astype(str).str.strip().str.lower().eq("hits")
        & pd.to_numeric(df["line"], errors="coerce").eq(0.5)
        & pd.to_numeric(df["rank_bucket"], errors="coerce").eq(10)
    ].copy()
    out["date"] = out["game_date"].map(_date_key)
    out["player"] = out["player_name"]
    out["side"] = "under"
    out["source_lane"] = "direct_hitless_under_05_top_decile"
    out["win_rate_estimate"] = pd.to_numeric(out.get("under_win_score"), errors="coerce")
    out["odds"] = pd.to_numeric(out["price_under_american"], errors="coerce")
    out["actual_win"] = _bool_win(out["under_win"])
    out["pnl"] = pd.to_numeric(out["pnl_under_1u"], errors="coerce")
    return _selector_cols(out)


def _load_over(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, low_memory=False)
    needed = {"game_date", "player_name", "prop_type", "side", "line", "actual_win", "pnl_side_1u", "price", "rank_bucket"}
    if not needed.issubset(df.columns):
        return pd.DataFrame()
    out = df[
        df["prop_type"].astype(str).str.strip().str.lower().eq("hits")
        & df["side"].astype(str).str.strip().str.lower().eq("over")
        & pd.to_numeric(df["rank_bucket"], errors="coerce").eq(9)
    ].copy()
    out["date"] = out["game_date"].map(_date_key)
    out["player"] = out["player_name"]
    out["source_lane"] = "residual_ranker_over_bucket_9"
    out["win_rate_estimate"] = pd.to_numeric(out.get("empirical_win_rate"), errors="coerce")
    out["odds"] = pd.to_numeric(out["price"], errors="coerce")
    out["actual_win"] = _bool_win(out["actual_win"])
    out["pnl"] = pd.to_numeric(out["pnl_side_1u"], errors="coerce")
    return _selector_cols(out)


def _load_quick_card(root: Path, reconcile: pd.DataFrame, from_date: str, to_date: str) -> pd.DataFrame:
    if reconcile.empty or not root.exists():
        return pd.DataFrame()
    frames: list[pd.DataFrame] = []
    for child in sorted(root.iterdir()):
        if not child.is_dir() or not (from_date <= child.name <= to_date):
            continue
        path = child / "quick_card.csv"
        if not path.exists():
            continue
        qc = pd.read_csv(path, low_memory=False)
        required = {"date", "player_name", "prop_type", "side", "line"}
        if not required.issubset(qc.columns):
            continue
        qc = qc[qc["prop_type"].astype(str).str.strip().str.lower().eq("hits")].copy()
        if qc.empty:
            continue
        qc["date_norm"] = qc["date"].map(_date_key)
        qc["player_name_norm"] = qc["player_name"].map(_norm_name)
        qc["prop_type_norm"] = qc["prop_type"].astype(str).str.strip().str.lower()
        qc["side_norm"] = qc["side"].astype(str).str.strip().str.lower()
        qc["line_norm"] = qc["line"].map(_line_key)
        frames.append(qc)
    if not frames:
        return pd.DataFrame()
    quick = pd.concat(frames, ignore_index=True)
    rec = reconcile[reconcile["prop_type_norm"].eq("hits")].copy()
    rec = rec.drop_duplicates(["date_norm", "player_name_norm", "prop_type_norm", "line_norm"], keep="first")
    merged = quick.merge(
        rec,
        on=["date_norm", "player_name_norm", "prop_type_norm", "line_norm"],
        how="left",
        suffixes=("_quick", "_rec"),
    )
    if merged.empty:
        return pd.DataFrame()
    merged = merged[merged["side_norm"].isin({"over", "under"})].copy()
    # `side` from quick card is `side_norm`; only count rows that actually
    # joined to a resolved reconcile outcome.
    side = merged["side_norm"]
    resolved = np.where(
        side.eq("over"),
        merged.get("actual_over_outcome"),
        merged.get("actual_under_outcome"),
    )
    resolved_series = pd.Series(resolved, index=merged.index).astype(str).str.strip().str.lower()
    merged = merged[resolved_series.isin({"win", "loss"})].copy()
    if merged.empty:
        return pd.DataFrame()
    merged["date"] = merged["date_norm"]
    merged["player"] = merged.get("player_name_quick", merged.get("player_name"))
    merged["prop_type"] = "hits"
    merged["side"] = merged["side_norm"]
    merged["line"] = merged["line_norm"]
    merged["source_lane"] = "quick_card_hits"
    merged["rank_bucket"] = pd.NA
    merged["win_rate_estimate"] = pd.to_numeric(merged.get("model_prob"), errors="coerce")
    merged["odds"] = np.where(
        merged["side"].eq("over"),
        pd.to_numeric(merged.get("price_over_american"), errors="coerce"),
        pd.to_numeric(merged.get("price_under_american"), errors="coerce"),
    )
    merged["actual_win"] = np.where(
        merged["side"].eq("over"),
        merged["actual_over_outcome"].astype(str).str.lower().eq("win"),
        merged["actual_under_outcome"].astype(str).str.lower().eq("win"),
    )
    merged["pnl"] = np.where(
        merged["side"].eq("over"),
        pd.to_numeric(merged.get("pnl_over_1u"), errors="coerce"),
        pd.to_numeric(merged.get("pnl_under_1u"), errors="coerce"),
    )
    missing_pnl = pd.isna(merged["pnl"])
    if missing_pnl.any():
        merged.loc[missing_pnl, "pnl"] = pd.Series([
            _profit_from_price(px, win)
            for px, win in zip(merged.loc[missing_pnl, "odds"], merged.loc[missing_pnl, "actual_win"])
        ], index=merged.loc[missing_pnl].index, dtype="float64")
    return _selector_cols(merged)


def _metrics(df: pd.DataFrame, group: str, value: str) -> dict[str, Any]:
    bets = int(len(df))
    wins = int(df["actual_win"].sum()) if bets else 0
    profit = float(pd.to_numeric(df["pnl"], errors="coerce").fillna(0.0).sum()) if bets else 0.0
    return {
        "group": group,
        "value": value,
        "bets": bets,
        "wins": wins,
        "win_rate": float(wins / bets) if bets else None,
        "profit_units": profit,
        "roi": float(profit / bets) if bets else None,
        "avg_odds": float(pd.to_numeric(df["odds"], errors="coerce").mean(skipna=True)) if bets else None,
    }


def _summary_rows(selected: pd.DataFrame) -> list[dict[str, Any]]:
    rows = [_metrics(selected, "overall", "all")]
    for lane, part in selected.groupby("source_lane", dropna=False):
        rows.append(_metrics(part, "by_lane", str(lane)))
    for date, part in selected.groupby("date", dropna=False):
        rows.append(_metrics(part, "by_day", str(date)))
    for (lane, date), part in selected.groupby(["source_lane", "date"], dropna=False):
        rows.append(_metrics(part, "by_lane_day", f"{lane}|{date}"))
    return rows


def run(args: argparse.Namespace) -> dict[str, Any]:
    reconcile = _load_reconcile(Path(args.reconcile_root), args.from_date, args.to_date)
    frames = [
        _load_under(Path(args.direct_under_csv)),
        _load_over(Path(args.residual_over_csv)),
    ]
    if args.include_quick_card:
        frames.append(_load_quick_card(Path(args.quick_card_root), reconcile, args.from_date, args.to_date))
    selected = pd.concat([f for f in frames if not f.empty], ignore_index=True) if any(not f.empty for f in frames) else pd.DataFrame()
    if selected.empty:
        raise SystemExit("No selected lane rows found.")
    selected = selected[(selected["date"] >= args.from_date) & (selected["date"] <= args.to_date)].copy()
    selected = selected.sort_values(["date", "source_lane", "prop_type", "player", "side", "line"])

    out_csv = Path(args.out_csv)
    summary_json = Path(args.summary_json)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_json.parent.mkdir(parents=True, exist_ok=True)
    selected.to_csv(out_csv, index=False)
    summary = {
        "residual_over_csv": str(args.residual_over_csv),
        "direct_under_csv": str(args.direct_under_csv),
        "reconcile_root": str(args.reconcile_root),
        "quick_card_root": str(args.quick_card_root),
        "from_date": args.from_date,
        "to_date": args.to_date,
        "include_quick_card": bool(args.include_quick_card),
        "out_csv": str(out_csv),
        "summary_json": str(summary_json),
        "rules": {
            "under_0_5": "direct_hitless_under_05 rank_bucket == 10",
            "over": "residual_ranker_over rank_bucket == 9",
            "quick_card": "all hits quick_card rows when available",
        },
        "metrics": _summary_rows(selected),
    }
    summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build validation hits lane selector.")
    parser.add_argument("--residual-over-csv", default=str(DEFAULT_RESIDUAL_OVER))
    parser.add_argument("--direct-under-csv", default=str(DEFAULT_DIRECT_UNDER))
    parser.add_argument("--reconcile-root", default=str(DEFAULT_RECONCILE_ROOT))
    parser.add_argument("--quick-card-root", default=str(DEFAULT_QUICK_CARD_ROOT))
    parser.add_argument("--from-date", default=DEFAULT_FROM_DATE)
    parser.add_argument("--to-date", default=DEFAULT_TO_DATE)
    parser.add_argument("--out-csv", default=str(DEFAULT_OUT_CSV))
    parser.add_argument("--summary-json", default=str(DEFAULT_SUMMARY_JSON))
    parser.add_argument("--no-quick-card", action="store_true")
    args = parser.parse_args()
    args.include_quick_card = not args.no_quick_card
    return args


def main() -> None:
    summary = run(parse_args())
    overall = next((m for m in summary["metrics"] if m["group"] == "overall"), {})
    print(f"Wrote {summary['out_csv']}")
    print(f"Wrote {summary['summary_json']}")
    print(
        "bets={bets} win_rate={win_rate:.4f} roi={roi:.4f}".format(
            bets=overall.get("bets", 0),
            win_rate=overall.get("win_rate") or 0.0,
            roi=overall.get("roi") or 0.0,
        )
    )


if __name__ == "__main__":
    main()
