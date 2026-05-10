#!/usr/bin/env python3
"""Audit TRAIN-vs-VALIDATION mapper behavior for hits 0.5 UNDER.

Diagnosis only. This does not tune filters, change model logic, or write
production artifacts.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_INPUT = Path("backend/mlb/exports/model_v2/ranking/validation/hits_rank_mapper_validation.csv")
DEFAULT_OUT_CSV = Path("backend/mlb/exports/model_v2/ranking/validation/hits_05_under_mapper_audit.csv")
DEFAULT_SUMMARY_JSON = Path("backend/mlb/exports/model_v2/ranking/validation/hits_05_under_mapper_audit_summary.json")


def _metric_float(value: Any) -> float | None:
    val = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(val):
        return None
    return float(val)


def _safe_bool(series: pd.Series) -> pd.Series:
    if series.dtype == bool:
        return series
    return series.astype(str).str.strip().str.lower().isin({"true", "1", "yes", "win"})


def _corr(frame: pd.DataFrame, a: str, b: str, method: str = "spearman") -> float | None:
    if a not in frame.columns or b not in frame.columns or frame.empty:
        return None
    val = pd.to_numeric(frame[a], errors="coerce").corr(pd.to_numeric(frame[b], errors="coerce"), method=method)
    if pd.isna(val):
        return None
    return float(val)


def run(args: argparse.Namespace) -> dict[str, Any]:
    input_csv = Path(args.input_csv)
    out_csv = Path(args.out_csv)
    summary_json = Path(args.summary_json)
    if not input_csv.exists():
        raise SystemExit(f"Missing mapper validation CSV: {input_csv}")
    df = pd.read_csv(input_csv, low_memory=False)
    required = {
        "prop_type",
        "side",
        "line_bucket",
        "rank_bucket",
        "empirical_win_rate",
        "sample_size",
        "actual_win",
        "pnl_side_1u",
        "predicted_residual",
        "actual_residual",
        "line",
        "price_under_american",
        "price_over_american",
    }
    missing = sorted(required - set(df.columns))
    if missing:
        raise SystemExit(f"{input_csv} missing required columns: {missing}")

    work = df.copy()
    work["prop_type"] = work["prop_type"].astype(str).str.strip().str.lower()
    work["side"] = work["side"].astype(str).str.strip().str.lower()
    work["line_bucket"] = pd.to_numeric(work["line_bucket"], errors="coerce")
    work["rank_bucket"] = pd.to_numeric(work["rank_bucket"], errors="coerce").astype("Int64")
    work["actual_win"] = _safe_bool(work["actual_win"])
    numeric_cols = [
        "empirical_win_rate",
        "sample_size",
        "pnl_side_1u",
        "predicted_residual",
        "actual_residual",
        "line",
        "price_under_american",
        "price_over_american",
        "train_avg_rank_score",
        "train_avg_actual_residual",
    ]
    for col in numeric_cols:
        if col in work.columns:
            work[col] = pd.to_numeric(work[col], errors="coerce")

    focus = work[
        work["prop_type"].eq("hits")
        & work["side"].eq("under")
        & work["line_bucket"].eq(0.5)
        & work["mapped"].astype(str).str.strip().str.lower().isin({"true", "1", "yes"})
        & work["passes_sample_size"].astype(str).str.strip().str.lower().isin({"true", "1", "yes"})
    ].copy()
    if focus.empty:
        raise SystemExit("No mapped hits 0.5 UNDER rows found.")

    bucket_rows: list[dict[str, Any]] = []
    for rank_bucket, group in focus.groupby("rank_bucket", dropna=False):
        bets = int(len(group))
        wins = int(group["actual_win"].sum())
        profit = float(group["pnl_side_1u"].fillna(0.0).sum())
        bucket_rows.append(
            {
                "rank_bucket": int(rank_bucket) if pd.notna(rank_bucket) else None,
                "train_empirical_win_rate": _metric_float(group["empirical_win_rate"].dropna().iloc[0])
                if group["empirical_win_rate"].notna().any()
                else None,
                "train_sample_size": int(group["sample_size"].dropna().iloc[0])
                if group["sample_size"].notna().any()
                else None,
                "train_avg_rank_score": float(group["train_avg_rank_score"].mean(skipna=True))
                if "train_avg_rank_score" in group.columns
                else None,
                "train_avg_actual_residual": float(group["train_avg_actual_residual"].mean(skipna=True))
                if "train_avg_actual_residual" in group.columns
                else None,
                "validation_win_rate": float(wins / bets) if bets else None,
                "validation_sample_size": bets,
                "validation_roi": float(profit / bets) if bets else None,
                "validation_profit_units": profit,
                "avg_predicted_residual": float(group["predicted_residual"].mean(skipna=True)),
                "avg_actual_residual": float(group["actual_residual"].mean(skipna=True)),
                "avg_line": float(group["line"].mean(skipna=True)),
                "avg_price_under_american": float(group["price_under_american"].mean(skipna=True)),
                "avg_price_over_american": float(group["price_over_american"].mean(skipna=True)),
                "win_rate_gap_validation_minus_train": (
                    float(wins / bets) - float(group["empirical_win_rate"].dropna().iloc[0])
                    if bets and group["empirical_win_rate"].notna().any()
                    else None
                ),
            }
        )

    audit = pd.DataFrame(bucket_rows).sort_values("rank_bucket")
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    summary_json.parent.mkdir(parents=True, exist_ok=True)
    audit.to_csv(out_csv, index=False)

    total_bets = int(len(focus))
    total_wins = int(focus["actual_win"].sum())
    total_profit = float(focus["pnl_side_1u"].fillna(0.0).sum())
    # For UNDER, higher rank_score means lower predicted residual. If this is
    # working, rank_bucket should generally move with UNDER wins.
    rank_win_corr = _corr(focus, "rank_bucket", "actual_win", method="spearman")
    pred_actual_corr = _corr(focus, "predicted_residual", "actual_residual", method="spearman")
    under_rank_actual_corr = _corr(focus, "rank_score", "actual_residual", method="spearman")
    train_val_gap = audit["win_rate_gap_validation_minus_train"].dropna()
    price_under = pd.to_numeric(focus["price_under_american"], errors="coerce")
    price_summary = {
        "avg_price_under_american": float(price_under.mean(skipna=True)),
        "pct_plus_money_under": float((price_under > 0).mean()),
        "pct_under_price_le_minus_200": float((price_under <= -200).mean()),
    }
    summary = {
        "input_csv": str(input_csv),
        "out_csv": str(out_csv),
        "summary_json": str(summary_json),
        "focus": "prop_type=hits,line=0.5,side=under,mapped,passes_sample_size",
        "validation_bets": total_bets,
        "validation_wins": total_wins,
        "validation_win_rate": float(total_wins / total_bets) if total_bets else None,
        "validation_profit_units": total_profit,
        "validation_roi": float(total_profit / total_bets) if total_bets else None,
        "rank_bucket_count": int(audit["rank_bucket"].nunique()),
        "rank_bucket_vs_actual_win_spearman": rank_win_corr,
        "predicted_residual_vs_actual_residual_spearman": pred_actual_corr,
        "under_rank_score_vs_actual_residual_spearman": under_rank_actual_corr,
        "avg_train_validation_win_rate_gap": float(train_val_gap.mean()) if not train_val_gap.empty else None,
        "max_abs_train_validation_win_rate_gap": float(train_val_gap.abs().max()) if not train_val_gap.empty else None,
        "price_summary": price_summary,
        "diagnosis_hints": {
            "train_validation_base_rate_shift": "Large negative validation-minus-train gaps indicate base-rate shift.",
            "rank_direction_wrong_for_under": "Positive rank_bucket-vs-win correlation supports direction; negative suggests inversion.",
            "price_trap": "Check avg_price_under_american and ROI by bucket; plus-money or poor payoff can dominate.",
            "bucket_too_coarse": "Large within-bucket train/validation gaps with broad sample sizes suggest coarse buckets.",
            "residual_target_unsuitable": "Weak predicted-vs-actual residual correlation in focus group suggests residual ranking is not stable for UNDER.",
        },
        "bucket_rows": audit.to_dict(orient="records"),
    }
    summary_json.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit hits 0.5 UNDER mapper failure.")
    parser.add_argument("--input-csv", default=str(DEFAULT_INPUT))
    parser.add_argument("--out-csv", default=str(DEFAULT_OUT_CSV))
    parser.add_argument("--summary-json", default=str(DEFAULT_SUMMARY_JSON))
    return parser.parse_args()


def main() -> None:
    summary = run(parse_args())
    print(f"Wrote {summary['out_csv']}")
    print(f"Wrote {summary['summary_json']}")
    print(
        "bets={validation_bets} win_rate={validation_win_rate:.4f} roi={validation_roi:.4f} "
        "rank_win_corr={rank_bucket_vs_actual_win_spearman:.4f}".format(**summary)
    )


if __name__ == "__main__":
    main()
