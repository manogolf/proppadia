#!/usr/bin/env python3
"""Report MLB selected-side ROI by American-odds bucket from reconcile rows."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import numpy as np
import pandas as pd

LEGACY_BUCKET_ORDER = [
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


TEN_BUCKET_ORDER = [
    ">=+201",
    "+191..+200",
    "+181..+190",
    "+171..+180",
    "+161..+170",
    "+151..+160",
    "+141..+150",
    "+131..+140",
    "+121..+130",
    "+111..+120",
    "+101..+110",
    "-99..+100",
    "-109..-100",
    "-119..-110",
    "-129..-120",
    "-139..-130",
    "-149..-140",
    "-159..-150",
    "-169..-160",
    "-179..-170",
    "-189..-180",
    "-199..-190",
    "-209..-200",
    "-219..-210",
    "-229..-220",
    "-239..-230",
    "-249..-240",
    "-259..-250",
    "-269..-260",
    "-279..-270",
    "-289..-280",
    "-299..-290",
    "<=-300",
]


def _prop_abbrev(prop_type: str) -> str:
    key = str(prop_type or "").strip().lower()
    mapping = {
        "hits": "H",
        "total_bases": "TB",
        "hits_runs_rbis": "H+R+RBI",
        "runs_rbis": "R+RBI",
        "runs_scored": "R",
        "rbis": "RBI",
        "hits_allowed": "HA",
        "earned_runs": "ER",
        "walks": "BB",
        "walks_allowed": "BBA",
        "strikeouts_pitching": "K",
        "strikeouts_batting": "K_B",
        "outs_recorded": "OUTS",
        "home_runs": "HR",
        "doubles": "2B",
        "triples": "3B",
        "singles": "1B",
        "stolen_bases": "SB",
    }
    return mapping.get(key, str(prop_type or "").strip())


def _bucket_from_american(odds: float, layout: str = "legacy") -> str:
    o = int(round(float(odds)))
    if layout == "ten":
        if o >= 201:
            return ">=+201"
        if 101 <= o <= 200:
            low = ((o - 101) // 10) * 10 + 101
            high = low + 9
            return f"+{low}..+{high}"
        if -99 <= o <= 100:
            return "-99..+100"
        if -299 <= o <= -100:
            abs_o = abs(o)
            low_abs = ((abs_o - 100) // 10) * 10 + 100
            high_abs = low_abs + 9
            return f"-{high_abs}..-{low_abs}"
        return "<=-300"

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


def _build_selected_columns(df: pd.DataFrame, selection: str) -> pd.DataFrame:
    out = df.copy()
    pick = out["model_pick_side"].astype(str).str.lower().str.strip()
    model_over = pick.eq("over")
    model_under = pick.eq("under")

    if selection == "model":
        selected_side = np.where(model_over, "over", np.where(model_under, "under", None))
        selected_outcome = out["actual_model_pick_outcome"].astype(str).str.lower().str.strip()
        selected_odds = np.where(
            model_over,
            pd.to_numeric(out["price_over_american"], errors="coerce"),
            np.where(model_under, pd.to_numeric(out["price_under_american"], errors="coerce"), np.nan),
        )
    else:
        selected_side = np.where(model_over, "under", np.where(model_under, "over", None))
        selected_outcome = np.where(
            model_over,
            out["actual_under_outcome"].astype(str).str.lower().str.strip(),
            np.where(model_under, out["actual_over_outcome"].astype(str).str.lower().str.strip(), None),
        )
        selected_odds = np.where(
            model_over,
            pd.to_numeric(out["price_under_american"], errors="coerce"),
            np.where(model_under, pd.to_numeric(out["price_over_american"], errors="coerce"), np.nan),
        )

    out["selected_side"] = selected_side
    out["selected_outcome"] = selected_outcome
    out["selected_american_odds"] = pd.to_numeric(selected_odds, errors="coerce")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Report MLB selected-side ROI by American-odds bucket.")
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
    ap.add_argument(
        "--selection",
        choices=["model", "fade"],
        default="model",
        help="Which side to evaluate per row. model=model-picked side; fade=opposite side.",
    )
    ap.add_argument(
        "--require-two-sided",
        action="store_true",
        default=str(os.environ.get("MLB_ODDS_BUCKET_REQUIRE_TWO_SIDED", "1")).strip().lower() in {"1", "true", "yes", "on"},
        help="Require both over and under prices on each source row before bucket evaluation.",
    )
    ap.add_argument(
        "--print-positive-only",
        action="store_true",
        help="Print only buckets with ROI above --min-print-roi-pct.",
    )
    ap.add_argument(
        "--compact-print",
        action="store_true",
        help="Print compact lines: '* bucket: +X.XX%%'.",
    )
    ap.add_argument(
        "--min-print-roi-pct",
        type=float,
        default=0.0,
        help="Minimum ROI threshold used with --print-positive-only.",
    )
    ap.add_argument(
        "--bucket-layout",
        choices=["legacy", "ten"],
        default="legacy",
        help="Bucket layout: legacy mixed widths or ten-point (10-odds) buckets.",
    )
    ap.add_argument(
        "--print-both-contributors",
        action="store_true",
        help="Include both winner and drag contributors in printed bucket lines.",
    )
    ap.add_argument(
        "--output-positive-only",
        action="store_true",
        help="Filter output files to only buckets with ROI above --min-print-roi-pct.",
    )
    args = ap.parse_args()

    rows_csv = Path(args.rows_csv).expanduser()
    out_json = Path(args.out_json).expanduser()
    out_csv = Path(args.out_csv).expanduser()
    out_focus_csv = Path(args.out_focus_csv).expanduser()
    selection = str(args.selection).strip().lower()
    bucket_layout = str(args.bucket_layout).strip().lower()
    bucket_order = TEN_BUCKET_ORDER if bucket_layout == "ten" else LEGACY_BUCKET_ORDER
    focus_buckets = [x.strip() for x in str(args.focus_buckets).split(",") if str(x).strip()]

    if not rows_csv.exists():
        raise FileNotFoundError(f"rows csv not found: {rows_csv}")

    df = pd.read_csv(rows_csv, low_memory=False)
    required = {
        "actual_model_pick_outcome",
        "actual_over_outcome",
        "actual_under_outcome",
        "model_pick_side",
        "price_over_american",
        "price_under_american",
        "prop_type",
    }
    missing = sorted(required - set(df.columns))
    if missing:
        raise RuntimeError(f"missing required columns in rows csv: {missing}")

    selected = _build_selected_columns(df, selection)
    mask = selected["selected_outcome"].isin(["win", "loss"]) & selected["selected_side"].isin(["over", "under"])
    if bool(args.require_two_sided):
        mask = mask & selected["price_over_american"].notna() & selected["price_under_american"].notna()
    work = selected.loc[mask].copy()
    work = work[work["selected_american_odds"].notna()].copy()

    if work.empty:
        raise RuntimeError("no resolved selected-side rows with valid selected-side American odds")

    work["odds_bucket"] = work["selected_american_odds"].map(lambda o: _bucket_from_american(o, bucket_layout))
    work["pnl_1u"] = [
        _pnl_1u(float(o), str(r))
        for o, r in zip(work["selected_american_odds"].tolist(), work["selected_outcome"].tolist())
    ]

    grouped = (
        work.groupby("odds_bucket", dropna=False)
        .agg(
            rows=("selected_outcome", "size"),
            wins=("selected_outcome", lambda s: int((s == "win").sum())),
            losses=("selected_outcome", lambda s: int((s == "loss").sum())),
            pnl_sum=("pnl_1u", "sum"),
        )
        .reset_index()
    )
    grouped["win_rate_pct"] = grouped["wins"] / grouped["rows"] * 100.0
    grouped["roi_pct"] = grouped["pnl_sum"] / grouped["rows"] * 100.0
    grouped["row_share_pct"] = grouped["rows"] / grouped["rows"].sum() * 100.0
    grouped["odds_bucket"] = pd.Categorical(grouped["odds_bucket"], categories=bucket_order, ordered=True)
    grouped = grouped.sort_values("odds_bucket").reset_index(drop=True)

    out_table = grouped[
        ["odds_bucket", "rows", "wins", "losses", "win_rate_pct", "roi_pct", "row_share_pct"]
    ].copy()
    for col in ("win_rate_pct", "roi_pct", "row_share_pct"):
        out_table[col] = out_table[col].round(2)
    out_table["odds_bucket"] = out_table["odds_bucket"].astype(str)

    # Per-bucket prop contributors (always compute both):
    # - positive contributor: prop with highest pnl contribution
    # - drag contributor: prop with lowest pnl contribution (main drag)
    bucket_prop = (
        work.groupby(["odds_bucket", "prop_type"], dropna=False)
        .agg(
            top_prop_rows=("selected_outcome", "size"),
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

    top_positive = (
        bucket_prop.sort_values(
            ["odds_bucket", "top_prop_pnl_1u", "top_prop_rows"],
            ascending=[True, False, False],
        )
        .groupby("odds_bucket", as_index=False)
        .head(1)[
            ["odds_bucket", "prop_type", "top_prop_rows", "top_prop_pnl_1u", "top_prop_roi_pct"]
        ]
        .rename(
            columns={
                "prop_type": "positive_prop_type",
                "top_prop_rows": "positive_prop_rows",
                "top_prop_pnl_1u": "positive_prop_pnl_1u",
                "top_prop_roi_pct": "positive_prop_roi_pct",
            }
        )
    )
    top_drag = (
        bucket_prop.sort_values(
            ["odds_bucket", "top_prop_pnl_1u", "top_prop_rows"],
            ascending=[True, True, False],
        )
        .groupby("odds_bucket", as_index=False)
        .head(1)[
            ["odds_bucket", "prop_type", "top_prop_rows", "top_prop_pnl_1u", "top_prop_roi_pct"]
        ]
        .rename(
            columns={
                "prop_type": "drag_prop_type",
                "top_prop_rows": "drag_prop_rows",
                "top_prop_pnl_1u": "drag_prop_pnl_1u",
                "top_prop_roi_pct": "drag_prop_roi_pct",
            }
        )
    )

    out_table = out_table.merge(top_positive, on="odds_bucket", how="left")
    out_table = out_table.merge(top_drag, on="odds_bucket", how="left")

    # Backward-compatible top contributor fields:
    # positive bucket -> winner contributor, negative bucket -> drag contributor.
    out_table["top_prop_type"] = np.where(
        out_table["roi_pct"] >= 0.0,
        out_table["positive_prop_type"],
        out_table["drag_prop_type"],
    )
    out_table["top_prop_rows"] = np.where(
        out_table["roi_pct"] >= 0.0,
        out_table["positive_prop_rows"],
        out_table["drag_prop_rows"],
    )
    out_table["top_prop_pnl_1u"] = np.where(
        out_table["roi_pct"] >= 0.0,
        out_table["positive_prop_pnl_1u"],
        out_table["drag_prop_pnl_1u"],
    )
    out_table["top_prop_roi_pct"] = np.where(
        out_table["roi_pct"] >= 0.0,
        out_table["positive_prop_roi_pct"],
        out_table["drag_prop_roi_pct"],
    )
    out_table["positive_prop_abbr"] = out_table["positive_prop_type"].map(_prop_abbrev)
    out_table["drag_prop_abbr"] = out_table["drag_prop_type"].map(_prop_abbrev)
    out_table["top_prop_abbr"] = out_table["top_prop_type"].map(_prop_abbrev)

    for col in (
        "positive_prop_pnl_1u",
        "positive_prop_roi_pct",
        "drag_prop_pnl_1u",
        "drag_prop_roi_pct",
        "top_prop_pnl_1u",
        "top_prop_roi_pct",
    ):
        out_table[col] = pd.to_numeric(out_table[col], errors="coerce").round(2)

    report_table = out_table.copy()
    if args.output_positive_only:
        min_roi = float(args.min_print_roi_pct or 0.0)
        report_table = report_table[report_table["roi_pct"] > min_roi].copy()

    focus_df = report_table[report_table["odds_bucket"].isin(focus_buckets)].copy()
    focus_df["focus_rank"] = focus_df["odds_bucket"].map({b: i for i, b in enumerate(focus_buckets)})
    focus_df = focus_df.sort_values("focus_rank").drop(columns=["focus_rank"]).reset_index(drop=True)
    extra_positive_df = report_table[
        (~report_table["odds_bucket"].isin(focus_buckets)) & (report_table["roi_pct"] > 0.0)
    ].copy()
    extra_positive_df["bucket_rank"] = extra_positive_df["odds_bucket"].map(
        {b: i for i, b in enumerate(bucket_order)}
    )
    extra_positive_df = (
        extra_positive_df.sort_values(["bucket_rank", "rows"], ascending=[True, False])
        .drop(columns=["bucket_rank"])
        .reset_index(drop=True)
    )

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_focus_csv.parent.mkdir(parents=True, exist_ok=True)
    out_json.parent.mkdir(parents=True, exist_ok=True)
    report_table.to_csv(out_csv, index=False)
    focus_df.to_csv(out_focus_csv, index=False)

    game_dates = (
        pd.to_datetime(work["game_date"], errors="coerce").dropna().dt.date
        if "game_date" in work.columns
        else pd.Series([], dtype="object")
    )
    window_min = str(game_dates.min()) if len(game_dates) else ""
    window_max = str(game_dates.max()) if len(game_dates) else ""

    counts: dict[str, int] = {
        "rows_input": int(len(df)),
        "resolved_selected_rows": int(mask.sum()),
        "resolved_rows_with_selected_odds": int(len(work)),
    }
    if selection == "model":
        # Backward-compatible keys used by existing workflows.
        counts["resolved_model_pick_rows"] = counts["resolved_selected_rows"]
        counts["resolved_rows_with_picked_odds"] = counts["resolved_rows_with_selected_odds"]

    payload = {
        "status": "ok",
        "selection": selection,
        "bucket_layout": bucket_layout,
        "output_positive_only": bool(args.output_positive_only),
        "rows_csv": str(rows_csv),
        "window": {"game_date_min": window_min, "game_date_max": window_max},
        "counts": counts,
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

    label_from = str(args.label_from_date or "").strip() or window_min
    label_to = str(args.label_to_date or "").strip() or window_max
    label_from_print = label_from.replace("-", ".") if label_from else ""
    label_to_print = label_to.replace("-", ".") if label_to else ""
    title = "Model" if selection == "model" else "Fade"

    if label_from_print and label_to_print:
        print(f"{title} results {label_from_print} to {label_to_print}")
    elif label_from_print:
        print(f"{title} results from {label_from_print}")
    elif label_to_print:
        print(f"{title} results through {label_to_print}")

    resolved_from = (window_min or "").replace("-", ".")
    resolved_to = (window_max or "").replace("-", ".")
    if resolved_from and resolved_to and (resolved_from != label_from_print or resolved_to != label_to_print):
        print(f"Resolved outcomes {resolved_from} to {resolved_to}")

    if args.print_positive_only:
        min_roi = float(args.min_print_roi_pct or 0.0)
        print_df = report_table[report_table["roi_pct"] > min_roi].copy()
    else:
        if focus_buckets:
            print_df = pd.concat([focus_df, extra_positive_df], ignore_index=True)
        else:
            print_df = report_table.copy()

    for row in print_df.to_dict(orient="records"):
        bucket = str(row.get("odds_bucket") or "")
        rows = int(row.get("rows") or 0)
        roi_pct = float(row.get("roi_pct") or 0.0)
        top_prop = str(row.get("top_prop_type") or "").strip()
        top_prop_abbr = str(row.get("top_prop_abbr") or top_prop).strip()
        top_pnl = float(row.get("top_prop_pnl_1u") or 0.0)
        pos_prop = str(row.get("positive_prop_type") or "").strip()
        pos_prop_abbr = str(row.get("positive_prop_abbr") or pos_prop).strip()
        pos_pnl = float(row.get("positive_prop_pnl_1u") or 0.0)
        drag_prop = str(row.get("drag_prop_type") or "").strip()
        drag_prop_abbr = str(row.get("drag_prop_abbr") or drag_prop).strip()
        drag_pnl = float(row.get("drag_prop_pnl_1u") or 0.0)

        if args.compact_print:
            print(f"* {bucket}: {roi_pct:+.2f}%")
        elif args.print_both_contributors and (pos_prop or drag_prop):
            parts = []
            if pos_prop:
                parts.append(f"{pos_prop_abbr} {pos_pnl:+.2f}u")
            if drag_prop:
                parts.append(f"{drag_prop_abbr} {drag_pnl:+.2f}u")
            detail = " | ".join(parts)
            print(f"* {bucket}: {rows} rows, {roi_pct:+.2f}% ({detail})")
        elif top_prop:
            print(f"* {bucket}: {rows} rows, {roi_pct:+.2f}% ({top_prop_abbr} {top_pnl:+.2f}u)")
        else:
            print(f"* {bucket}: {rows} rows, {roi_pct:+.2f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
