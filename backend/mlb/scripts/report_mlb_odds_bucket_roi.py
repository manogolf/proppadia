#!/usr/bin/env python3
"""Report MLB model-picked ROI by American-odds bucket from reconcile rows."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

BUCKET_ORDER = [
    ">=+201",
    "+151..+200",
    "+131..+150",
    "+121..+130",
    "+111..+120",
    "+101..+110",
    "-99..+100",
    "-109..-100",
    "-119..-110",
    "-139..-120",
    "-159..-140",
    "-179..-160",
    "-199..-180",
    "-219..-200",
    "-249..-220",
    "-299..-250",
    "<=-300",
]


def _bucket_from_american(odds: float) -> str:
    o = int(round(float(odds)))
    if o >= 201:
        return ">=+201"
    if 151 <= o <= 200:
        return "+151..+200"
    if 131 <= o <= 150:
        return "+131..+150"
    if 121 <= o <= 130:
        return "+121..+130"
    if 111 <= o <= 120:
        return "+111..+120"
    if 101 <= o <= 110:
        return "+101..+110"
    if -99 <= o <= 100:
        return "-99..+100"
    if -109 <= o <= -100:
        return "-109..-100"
    if -119 <= o <= -110:
        return "-119..-110"
    if -139 <= o <= -120:
        return "-139..-120"
    if -159 <= o <= -140:
        return "-159..-140"
    if -179 <= o <= -160:
        return "-179..-160"
    if -199 <= o <= -180:
        return "-199..-180"
    if -219 <= o <= -200:
        return "-219..-200"
    if -249 <= o <= -220:
        return "-249..-220"
    if -299 <= o <= -250:
        return "-299..-250"
    return "<=-300"


def _pnl_1u(odds: float, outcome: str) -> float:
    if outcome != "win":
        return -1.0
    return (odds / 100.0) if odds > 0 else (100.0 / abs(odds))


def main() -> int:
    ap = argparse.ArgumentParser(description="Report MLB model-picked ROI by American-odds bucket.")
    ap.add_argument("--rows-csv", default="tmp/mlb_base_vs_market_rows.csv")
    ap.add_argument("--out-json", default="tmp/analysis/mlb_red_mode_odds_bucket_summary.json")
    ap.add_argument("--out-csv", default="tmp/analysis/mlb_red_mode_odds_bucket_by_bucket.csv")
    ap.add_argument("--out-focus-csv", default="tmp/analysis/mlb_red_mode_odds_bucket_focus.csv")
    ap.add_argument(
        "--focus-buckets",
        default="+111..+120,+131..+150,+151..+200,-299..-250,-249..-220,<=-300",
    )
    ap.add_argument("--label-from-date", default="")
    ap.add_argument("--label-to-date", default="")
    args = ap.parse_args()

    rows_csv = Path(args.rows_csv).expanduser()
    out_json = Path(args.out_json).expanduser()
    out_csv = Path(args.out_csv).expanduser()
    out_focus_csv = Path(args.out_focus_csv).expanduser()
    focus_buckets = [x.strip() for x in str(args.focus_buckets).split(",") if str(x).strip()]

    if not rows_csv.exists():
        raise FileNotFoundError(f"rows csv not found: {rows_csv}")

    df = pd.read_csv(rows_csv, low_memory=False)
    required = {"actual_model_pick_outcome", "model_pick_side", "price_over_american", "price_under_american"}
    missing = sorted(required - set(df.columns))
    if missing:
        raise RuntimeError(f"missing required columns in rows csv: {missing}")

    mask = (
        df["actual_model_pick_outcome"].isin(["win", "loss"])
        & df["model_pick_side"].isin(["over", "under"])
    )
    picked_odds = np.where(
        df["model_pick_side"].eq("over"),
        pd.to_numeric(df["price_over_american"], errors="coerce"),
        pd.to_numeric(df["price_under_american"], errors="coerce"),
    )
    work = df.loc[mask].copy()
    work["picked_american_odds"] = pd.to_numeric(pd.Series(picked_odds, index=df.index), errors="coerce").loc[mask]
    work = work[work["picked_american_odds"].notna()].copy()

    if work.empty:
        raise RuntimeError("no resolved model-pick rows with valid picked-side American odds")

    work["odds_bucket"] = work["picked_american_odds"].map(_bucket_from_american)
    work["pnl_1u"] = [
        _pnl_1u(float(o), str(r))
        for o, r in zip(work["picked_american_odds"].tolist(), work["actual_model_pick_outcome"].tolist())
    ]

    grouped = (
        work.groupby("odds_bucket", dropna=False)
        .agg(
            rows=("actual_model_pick_outcome", "size"),
            wins=("actual_model_pick_outcome", lambda s: int((s == "win").sum())),
            losses=("actual_model_pick_outcome", lambda s: int((s == "loss").sum())),
            pnl_sum=("pnl_1u", "sum"),
        )
        .reset_index()
    )
    grouped["win_rate_pct"] = grouped["wins"] / grouped["rows"] * 100.0
    grouped["roi_pct"] = grouped["pnl_sum"] / grouped["rows"] * 100.0
    grouped["row_share_pct"] = grouped["rows"] / grouped["rows"].sum() * 100.0
    grouped["odds_bucket"] = pd.Categorical(grouped["odds_bucket"], categories=BUCKET_ORDER, ordered=True)
    grouped = grouped.sort_values("odds_bucket").reset_index(drop=True)

    out_table = grouped[
        ["odds_bucket", "rows", "wins", "losses", "win_rate_pct", "roi_pct", "row_share_pct"]
    ].copy()
    for col in ("win_rate_pct", "roi_pct", "row_share_pct"):
        out_table[col] = out_table[col].round(2)
    out_table["odds_bucket"] = out_table["odds_bucket"].astype(str)

    # Per-bucket prop contributor:
    # - positive bucket ROI: prop with highest positive pnl contribution
    # - negative bucket ROI: prop with largest negative pnl contribution (main drag)
    bucket_prop = (
        work.groupby(["odds_bucket", "prop_type"], dropna=False)
        .agg(
            top_prop_rows=("actual_model_pick_outcome", "size"),
            top_prop_pnl_1u=("pnl_1u", "sum"),
        )
        .reset_index()
    )
    bucket_prop["top_prop_roi_pct"] = bucket_prop["top_prop_pnl_1u"] / bucket_prop["top_prop_rows"] * 100.0
    bucket_prop = bucket_prop.merge(
        out_table[["odds_bucket", "roi_pct"]].rename(columns={"roi_pct": "bucket_roi_pct"}),
        on="odds_bucket",
        how="left",
    )
    bucket_prop["contrib_rank_key"] = np.where(
        bucket_prop["bucket_roi_pct"] >= 0.0,
        bucket_prop["top_prop_pnl_1u"],
        -bucket_prop["top_prop_pnl_1u"],
    )
    top_contrib = (
        bucket_prop.sort_values(
            ["odds_bucket", "contrib_rank_key", "top_prop_rows"],
            ascending=[True, False, False],
        )
        .groupby("odds_bucket", as_index=False)
        .head(1)[
            ["odds_bucket", "prop_type", "top_prop_rows", "top_prop_pnl_1u", "top_prop_roi_pct"]
        ]
        .rename(columns={"prop_type": "top_prop_type"})
    )

    out_table = out_table.merge(top_contrib, on="odds_bucket", how="left")
    for col in ("top_prop_pnl_1u", "top_prop_roi_pct"):
        out_table[col] = pd.to_numeric(out_table[col], errors="coerce").round(2)

    focus_df = out_table[out_table["odds_bucket"].isin(focus_buckets)].copy()
    focus_df["focus_rank"] = focus_df["odds_bucket"].map({b: i for i, b in enumerate(focus_buckets)})
    focus_df = focus_df.sort_values("focus_rank").drop(columns=["focus_rank"]).reset_index(drop=True)
    extra_positive_df = out_table[
        (~out_table["odds_bucket"].isin(focus_buckets)) & (out_table["roi_pct"] > 0.0)
    ].copy()
    extra_positive_df["bucket_rank"] = extra_positive_df["odds_bucket"].map(
        {b: i for i, b in enumerate(BUCKET_ORDER)}
    )
    extra_positive_df = (
        extra_positive_df.sort_values(["bucket_rank", "rows"], ascending=[True, False])
        .drop(columns=["bucket_rank"])
        .reset_index(drop=True)
    )

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_focus_csv.parent.mkdir(parents=True, exist_ok=True)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_table.to_csv(out_csv, index=False)
    focus_df.to_csv(out_focus_csv, index=False)

    game_dates = (
        pd.to_datetime(work["game_date"], errors="coerce").dropna().dt.date
        if "game_date" in work.columns
        else pd.Series([], dtype="object")
    )
    window_min = str(game_dates.min()) if len(game_dates) else ""
    window_max = str(game_dates.max()) if len(game_dates) else ""

    payload = {
        "status": "ok",
        "rows_csv": str(rows_csv),
        "window": {"game_date_min": window_min, "game_date_max": window_max},
        "counts": {
            "rows_input": int(len(df)),
            "resolved_model_pick_rows": int(mask.sum()),
            "resolved_rows_with_picked_odds": int(len(work)),
        },
        "focus_buckets": focus_buckets,
        "focus_results": focus_df.to_dict(orient="records"),
        "extra_positive_results": extra_positive_df.to_dict(orient="records"),
        "outputs": {
            "by_bucket_csv": str(out_csv),
            "focus_csv": str(out_focus_csv),
            "summary_json": str(out_json),
        },
    }
    out_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))
    label_from = str(args.label_from_date or "").strip()
    label_to = str(args.label_to_date or "").strip()
    if not label_from:
        label_from = window_min
    if not label_to:
        label_to = window_max
    label_from = label_from.replace("-", ".")
    label_to = label_to.replace("-", ".")
    if label_from and label_to:
        print(f"Model results {label_from} to {label_to}")
    elif label_from:
        print(f"Model results from {label_from}")
    elif label_to:
        print(f"Model results through {label_to}")
    resolved_from = (window_min or "").replace("-", ".")
    resolved_to = (window_max or "").replace("-", ".")
    if resolved_from and resolved_to and (resolved_from != label_from or resolved_to != label_to):
        print(f"Resolved outcomes {resolved_from} to {resolved_to}")

    for row in focus_df.to_dict(orient="records"):
        bucket = str(row.get("odds_bucket") or "")
        rows = int(row.get("rows") or 0)
        roi_pct = float(row.get("roi_pct") or 0.0)
        top_prop = str(row.get("top_prop_type") or "").strip()
        top_pnl = float(row.get("top_prop_pnl_1u") or 0.0)
        if top_prop:
            print(f"* {bucket}: {rows} rows, {roi_pct:+.2f}% ({top_prop} {top_pnl:+.2f}u)")
        else:
            print(f"* {bucket}: {rows} rows, {roi_pct:+.2f}%")
    for row in extra_positive_df.to_dict(orient="records"):
        bucket = str(row.get("odds_bucket") or "")
        rows = int(row.get("rows") or 0)
        roi_pct = float(row.get("roi_pct") or 0.0)
        top_prop = str(row.get("top_prop_type") or "").strip()
        top_pnl = float(row.get("top_prop_pnl_1u") or 0.0)
        if top_prop:
            print(f"* {bucket}: {rows} rows, {roi_pct:+.2f}% ({top_prop} {top_pnl:+.2f}u)")
        else:
            print(f"* {bucket}: {rows} rows, {roi_pct:+.2f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
