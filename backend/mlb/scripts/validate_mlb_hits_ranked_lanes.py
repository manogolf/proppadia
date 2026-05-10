#!/usr/bin/env python3
"""Compare hits ranked lanes against current model and Quick Card lanes.

Validation/reporting only. No deployment, no model changes, no DB writes.
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
DEFAULT_OUT_CSV = Path("backend/mlb/exports/model_v2/ranking/validation/hits_ranked_lanes_validation.csv")
DEFAULT_SUMMARY_JSON = Path("backend/mlb/exports/model_v2/ranking/validation/hits_ranked_lanes_summary.json")
DEFAULT_FROM_DATE = "2026-04-09"
DEFAULT_TO_DATE = "2026-05-08"


def _clean(value: Any) -> str:
    text = str(value if value is not None else "").strip()
    if text.lower() in {"", "nan", "none", "null", "<na>"}:
        return ""
    return text


def _bool_win(series: pd.Series) -> pd.Series:
    if series.dtype == bool:
        return series
    return series.astype(str).str.strip().str.lower().isin({"true", "1", "yes", "win", "won"})


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
    out["side_norm"] = out.get("model_pick_side", pd.Series("", index=out.index)).astype(str).str.strip().str.lower()
    out["line_norm"] = out["line"].map(_line_key)
    return out


def _load_residual_over(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, low_memory=False)
    needed = {"game_date", "prop_type", "side", "line", "actual_win", "pnl_side_1u", "price", "rank_bucket"}
    if not needed.issubset(df.columns):
        return pd.DataFrame()
    out = df[
        df["prop_type"].astype(str).str.strip().str.lower().eq("hits")
        & df["side"].astype(str).str.strip().str.lower().eq("over")
    ].copy()
    out["lane"] = "residual_ranker_over"
    out["date"] = out["game_date"].map(_date_key)
    out["side"] = "over"
    out["actual_win"] = _bool_win(out["actual_win"])
    out["profit_units"] = pd.to_numeric(out["pnl_side_1u"], errors="coerce")
    out["price"] = pd.to_numeric(out["price"], errors="coerce")
    out["rank_bucket"] = pd.to_numeric(out["rank_bucket"], errors="coerce").astype("Int64")
    out["rank_score"] = pd.to_numeric(out.get("rank_score"), errors="coerce")
    out["source"] = str(path)
    return _lane_cols(out)


def _load_direct_under(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, low_memory=False)
    needed = {"game_date", "prop_type", "line", "under_win", "pnl_under_1u", "price_under_american", "rank_bucket"}
    if not needed.issubset(df.columns):
        return pd.DataFrame()
    out = df[df["prop_type"].astype(str).str.strip().str.lower().eq("hits") & pd.to_numeric(df["line"], errors="coerce").eq(0.5)].copy()
    out["lane"] = "direct_hitless_under_05"
    out["date"] = out["game_date"].map(_date_key)
    out["side"] = "under"
    out["actual_win"] = _bool_win(out["under_win"])
    out["profit_units"] = pd.to_numeric(out["pnl_under_1u"], errors="coerce")
    out["price"] = pd.to_numeric(out["price_under_american"], errors="coerce")
    out["rank_bucket"] = pd.to_numeric(out["rank_bucket"], errors="coerce").astype("Int64")
    out["rank_score"] = pd.to_numeric(out.get("under_win_score"), errors="coerce")
    out["source"] = str(path)
    return _lane_cols(out)


def _load_current_model(reconcile: pd.DataFrame) -> pd.DataFrame:
    if reconcile.empty:
        return pd.DataFrame()
    needed = {"prop_type", "model_pick_side", "actual_model_pick_outcome", "pnl_model_pick_1u"}
    if not needed.issubset(reconcile.columns):
        return pd.DataFrame()
    out = reconcile[
        reconcile["prop_type_norm"].eq("hits")
        & reconcile["actual_model_pick_outcome"].astype(str).str.strip().str.lower().isin({"win", "loss"})
    ].copy()
    out["lane"] = "current_model_hits"
    out["date"] = out["date_norm"]
    out["side"] = out["model_pick_side"].astype(str).str.strip().str.lower()
    out["actual_win"] = out["actual_model_pick_outcome"].astype(str).str.strip().str.lower().eq("win")
    out["profit_units"] = pd.to_numeric(out["pnl_model_pick_1u"], errors="coerce")
    out["price"] = np.where(
        out["side"].eq("over"),
        pd.to_numeric(out.get("price_over_american"), errors="coerce"),
        pd.to_numeric(out.get("price_under_american"), errors="coerce"),
    )
    out["rank_bucket"] = pd.NA
    out["rank_score"] = pd.to_numeric(out.get("model_pick_prob"), errors="coerce")
    out["source"] = "reconcile_rows/current_model"
    return _lane_cols(out)


def _load_quick_card(root: Path, reconcile: pd.DataFrame, from_date: str, to_date: str) -> pd.DataFrame:
    if reconcile.empty or not root.exists():
        return pd.DataFrame()
    frames: list[pd.DataFrame] = []
    dates = sorted({p.name for p in root.iterdir() if p.is_dir() and from_date <= p.name <= to_date})
    for date in dates:
        path = root / date / "quick_card.csv"
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
        qc["source_quick_card_file"] = str(path)
        frames.append(qc)
    if not frames:
        return pd.DataFrame()
    quick = pd.concat(frames, ignore_index=True)
    rec = reconcile[reconcile["prop_type_norm"].eq("hits")].copy()
    merged = quick.merge(
        rec,
        on=["date_norm", "player_name_norm", "prop_type_norm", "side_norm", "line_norm"],
        how="left",
        suffixes=("_quick", "_rec"),
    )
    merged = merged[merged["actual_over_outcome"].notna() | merged["actual_under_outcome"].notna()].copy()
    if merged.empty:
        return pd.DataFrame()
    merged["lane"] = "quick_card_hits"
    merged["date"] = merged["date_norm"]
    merged["side"] = merged["side_norm"]
    merged["actual_win"] = np.where(
        merged["side"].eq("over"),
        merged["actual_over_outcome"].astype(str).str.lower().eq("win"),
        merged["actual_under_outcome"].astype(str).str.lower().eq("win"),
    )
    merged["profit_units"] = np.where(
        merged["side"].eq("over"),
        pd.to_numeric(merged.get("pnl_over_1u"), errors="coerce"),
        pd.to_numeric(merged.get("pnl_under_1u"), errors="coerce"),
    )
    merged["price"] = np.where(
        merged["side"].eq("over"),
        pd.to_numeric(merged.get("price_over_american"), errors="coerce"),
        pd.to_numeric(merged.get("price_under_american"), errors="coerce"),
    )
    merged["rank_bucket"] = pd.NA
    merged["rank_score"] = pd.to_numeric(merged.get("model_prob"), errors="coerce")
    merged["source"] = merged["source_quick_card_file"]
    merged["player_name"] = merged.get("player_name_quick", merged.get("player_name"))
    merged["prop_type"] = "hits"
    merged["line"] = merged["line_norm"]
    return _lane_cols(merged)


def _lane_cols(df: pd.DataFrame) -> pd.DataFrame:
    for col in ("player_name", "prop_type", "line"):
        if col not in df.columns:
            df[col] = None
    cols = [
        "lane",
        "date",
        "player_name",
        "prop_type",
        "side",
        "line",
        "price",
        "actual_win",
        "profit_units",
        "rank_bucket",
        "rank_score",
        "source",
    ]
    out = df[[c for c in cols if c in df.columns]].copy()
    out["line"] = pd.to_numeric(out["line"], errors="coerce")
    out["price"] = pd.to_numeric(out["price"], errors="coerce")
    out["profit_units"] = pd.to_numeric(out["profit_units"], errors="coerce")
    return out


def _metrics(df: pd.DataFrame, group: str, value: str) -> dict[str, Any]:
    bets = int(len(df))
    wins = int(df["actual_win"].sum()) if bets else 0
    profit = float(df["profit_units"].fillna(0.0).sum()) if bets else 0.0
    return {
        "group": group,
        "value": value,
        "bets": bets,
        "wins": wins,
        "win_rate": float(wins / bets) if bets else None,
        "profit_units": profit,
        "roi": float(profit / bets) if bets else None,
        "avg_odds": float(df["price"].mean(skipna=True)) if bets else None,
        "date_min": df["date"].min() if bets else None,
        "date_max": df["date"].max() if bets else None,
    }


def _aggregate(lanes: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    rows.append(_metrics(lanes, "overall", "all"))
    for lane, group in lanes.groupby("lane", dropna=False):
        rows.append(_metrics(group, "by_lane", str(lane)))
    for date, group in lanes.groupby("date", dropna=False):
        rows.append(_metrics(group, "by_date", str(date)))
    for (lane, date), group in lanes.groupby(["lane", "date"], dropna=False):
        rows.append(_metrics(group, "by_lane_date", f"{lane}|{date}"))
    ranked = lanes[lanes["rank_bucket"].notna()].copy()
    if not ranked.empty:
        ranked["rank_bucket"] = pd.to_numeric(ranked["rank_bucket"], errors="coerce").astype("Int64")
        for (lane, bucket), group in ranked.groupby(["lane", "rank_bucket"], dropna=False):
            rows.append(_metrics(group, "by_lane_rank_bucket", f"{lane}|rank_bucket={bucket}"))
        top = ranked[ranked["rank_bucket"].ge(8)]
        if not top.empty:
            for lane, group in top.groupby("lane", dropna=False):
                rows.append(_metrics(group, "by_lane_top_deciles_8_10", str(lane)))
        top10 = ranked[ranked["rank_bucket"].eq(10)]
        if not top10.empty:
            for lane, group in top10.groupby("lane", dropna=False):
                rows.append(_metrics(group, "by_lane_top_decile_10", str(lane)))
    return pd.DataFrame(rows)


def run(args: argparse.Namespace) -> dict[str, Any]:
    reconcile = _load_reconcile(Path(args.reconcile_root), args.from_date, args.to_date)
    lane_frames = [
        _load_residual_over(Path(args.residual_over_csv)),
        _load_direct_under(Path(args.direct_under_csv)),
        _load_current_model(reconcile),
        _load_quick_card(Path(args.quick_card_root), reconcile, args.from_date, args.to_date),
    ]
    lanes = pd.concat([f for f in lane_frames if not f.empty], ignore_index=True) if any(not f.empty for f in lane_frames) else pd.DataFrame()
    if lanes.empty:
        raise SystemExit("No lane rows available.")
    lanes = lanes[(lanes["date"] >= args.from_date) & (lanes["date"] <= args.to_date)].copy()
    aggregate = _aggregate(lanes)
    out_csv = Path(args.out_csv)
    summary_json = Path(args.summary_json)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_json.parent.mkdir(parents=True, exist_ok=True)
    aggregate.to_csv(out_csv, index=False)

    lane_counts = lanes.groupby("lane").size().sort_index().to_dict()
    summary = {
        "residual_over_csv": str(args.residual_over_csv),
        "direct_under_csv": str(args.direct_under_csv),
        "reconcile_root": str(args.reconcile_root),
        "quick_card_root": str(args.quick_card_root),
        "from_date": args.from_date,
        "to_date": args.to_date,
        "out_csv": str(out_csv),
        "summary_json": str(summary_json),
        "lane_counts": {str(k): int(v) for k, v in lane_counts.items()},
        "metrics": aggregate.to_dict(orient="records"),
    }
    summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate combined hits ranked lanes.")
    parser.add_argument("--residual-over-csv", default=str(DEFAULT_RESIDUAL_OVER))
    parser.add_argument("--direct-under-csv", default=str(DEFAULT_DIRECT_UNDER))
    parser.add_argument("--reconcile-root", default=str(DEFAULT_RECONCILE_ROOT))
    parser.add_argument("--quick-card-root", default=str(DEFAULT_QUICK_CARD_ROOT))
    parser.add_argument("--from-date", default=DEFAULT_FROM_DATE)
    parser.add_argument("--to-date", default=DEFAULT_TO_DATE)
    parser.add_argument("--out-csv", default=str(DEFAULT_OUT_CSV))
    parser.add_argument("--summary-json", default=str(DEFAULT_SUMMARY_JSON))
    return parser.parse_args()


def main() -> None:
    summary = run(parse_args())
    print(f"Wrote {summary['out_csv']}")
    print(f"Wrote {summary['summary_json']}")
    print("lane_counts=" + json.dumps(summary["lane_counts"], sort_keys=True))


if __name__ == "__main__":
    main()
