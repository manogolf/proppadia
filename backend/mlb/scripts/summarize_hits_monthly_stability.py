#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


def _winner_by_month(df_month: pd.DataFrame) -> Dict[str, str]:
    decision = (
        df_month.sort_values(
            ["accuracy_pct", "false_over", "false_under"],
            ascending=[False, True, True],
        )
        .iloc[0]["model_name"]
    )
    ranking = df_month.sort_values(["auc_p_over"], ascending=[False]).iloc[0]["model_name"]
    probability = (
        df_month.sort_values(
            ["brier_score", "log_loss", "calibration_decile_error_pp"],
            ascending=[True, True, True],
        )
        .iloc[0]["model_name"]
    )
    return {
        "decision_winner": str(decision),
        "ranking_winner": str(ranking),
        "probability_winner": str(probability),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Summarize monthly hits model comparison stability and winners.")
    ap.add_argument("--leaderboard-csv", required=True)
    ap.add_argument("--out-dir", required=True)
    args = ap.parse_args()

    lb_path = Path(args.leaderboard_csv).resolve()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    leaderboard = pd.read_csv(lb_path)
    monthly = leaderboard[leaderboard["cohort_type"] == "monthly"].copy()
    if monthly.empty:
        raise RuntimeError("no monthly rows found in leaderboard")

    monthly["cohort_label"] = monthly["cohort_label"].astype(str)
    monthly = monthly.sort_values(["cohort_label", "model_name"]).reset_index(drop=True)
    monthly_details_path = out_dir / "monthly_model_performance.csv"
    monthly.to_csv(monthly_details_path, index=False)

    winner_rows: List[Dict[str, Any]] = []
    for month, g in monthly.groupby("cohort_label", sort=True):
        w = _winner_by_month(g)
        winner_rows.append({"cohort_label": str(month), **w})
    winners = pd.DataFrame(winner_rows)
    winners_path = out_dir / "monthly_axis_winners.csv"
    winners.to_csv(winners_path, index=False)

    winner_counts: List[Dict[str, Any]] = []
    model_names = sorted(monthly["model_name"].unique().tolist())
    for model_name in model_names:
        winner_counts.append(
            {
                "model_name": str(model_name),
                "decision_month_wins": int((winners["decision_winner"] == model_name).sum()),
                "ranking_month_wins": int((winners["ranking_winner"] == model_name).sum()),
                "probability_month_wins": int((winners["probability_winner"] == model_name).sum()),
                "months_evaluated": int(winners["cohort_label"].nunique()),
            }
        )
    win_counts_df = pd.DataFrame(winner_counts)

    agg = (
        monthly.groupby("model_name", as_index=False)
        .agg(
            months_evaluated=("cohort_label", "nunique"),
            attempted_mean=("attempted", "mean"),
            scored_mean=("scored", "mean"),
            accuracy_mean=("accuracy_pct", "mean"),
            accuracy_std=("accuracy_pct", "std"),
            auc_mean=("auc_p_over", "mean"),
            auc_std=("auc_p_over", "std"),
            brier_mean=("brier_score", "mean"),
            brier_std=("brier_score", "std"),
            log_loss_mean=("log_loss", "mean"),
            log_loss_std=("log_loss", "std"),
            pred_over_mean=("pred_over_pct", "mean"),
            pred_over_std=("pred_over_pct", "std"),
            actual_over_mean=("actual_over_pct", "mean"),
            actual_over_std=("actual_over_pct", "std"),
            false_over_mean=("false_over", "mean"),
            false_over_std=("false_over", "std"),
            false_under_mean=("false_under", "mean"),
            false_under_std=("false_under", "std"),
            calibration_error_mean=("calibration_decile_error_pp", "mean"),
            calibration_error_std=("calibration_decile_error_pp", "std"),
        )
        .merge(win_counts_df, on="model_name", how="left")
    )
    numeric_cols = [c for c in agg.columns if c != "model_name"]
    agg[numeric_cols] = agg[numeric_cols].apply(pd.to_numeric, errors="coerce")
    agg[numeric_cols] = agg[numeric_cols].round(6)

    stability_summary_path = out_dir / "monthly_model_stability_summary.csv"
    agg.to_csv(stability_summary_path, index=False)

    manifest = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "source_leaderboard_csv": str(lb_path),
        "files": {
            "monthly_model_performance_csv": str(monthly_details_path),
            "monthly_axis_winners_csv": str(winners_path),
            "monthly_model_stability_summary_csv": str(stability_summary_path),
        },
        "months_evaluated": int(winners["cohort_label"].nunique()),
        "models_evaluated": model_names,
    }
    manifest_path = out_dir / "monthly_stability_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"wrote {monthly_details_path}")
    print(f"wrote {winners_path}")
    print(f"wrote {stability_summary_path}")
    print(f"wrote {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
