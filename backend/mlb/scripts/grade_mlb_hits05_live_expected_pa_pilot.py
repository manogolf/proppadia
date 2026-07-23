#!/usr/bin/env python3
"""Append-only grader for the Hits 0.5 live expected-PA research shadow."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, brier_score_loss, log_loss, roc_auc_score


ROOT = Path(__file__).resolve().parents[3]
PILOT = ROOT / "artifacts/analysis/model_development/mlb_hits05_live_expected_pa_parent_pilot/2026-07-21"
KEY = ["slate_date", "game_id", "player_id"]


def load_predictions(date: str = "", run_tag: str = "") -> pd.DataFrame:
    files = sorted((PILOT / "live_parent_runs").glob("*/*/live_expected_pa_parent_*.csv"))
    frames = []
    for path in files:
        x = pd.read_csv(path, low_memory=False)
        if date and not x["slate_date"].astype(str).eq(date).any():
            continue
        if run_tag and not x["governing_run_tag"].astype(str).eq(run_tag).any():
            continue
        x["immutable_prediction_path"] = str(path.relative_to(ROOT))
        frames.append(x)
    return pd.concat(frames, ignore_index=True, sort=False) if frames else pd.DataFrame()


def normalize_outcomes(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=KEY + ["actual_pa", "actual_hits", "game_completion_status"])
    x = pd.read_csv(path, low_memory=False)
    aliases = {
        "game_date": "slate_date", "date": "slate_date", "actual_plate_appearances": "actual_pa",
        "plate_appearances": "actual_pa", "hits": "actual_hits", "actual_value": "actual_hits",
    }
    for old, new in aliases.items():
        if new not in x and old in x:
            x[new] = x[old]
    required = KEY + ["actual_pa", "actual_hits"]
    if any(c not in x for c in required):
        return pd.DataFrame(columns=KEY + ["actual_pa", "actual_hits", "game_completion_status"])
    x["game_completion_status"] = x.get("game_completion_status", "OFFICIAL_COMPLETED")
    return x[KEY + ["actual_pa", "actual_hits", "game_completion_status"]].copy()


def safe_metric(fn, y: pd.Series, p: pd.Series) -> float:
    try:
        return float(fn(y, p))
    except Exception:
        return float("nan")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="")
    ap.add_argument("--run-tag", default="")
    ap.add_argument("--all-ungraded", action="store_true")
    ap.add_argument("--retry-unresolved-only", action="store_true")
    ap.add_argument("--outcomes-csv", default="")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--output-root", default=str(PILOT / "prospective_grading"))
    args = ap.parse_args()

    pred = load_predictions(args.date, args.run_tag)
    if pred.empty:
        print(json.dumps({"status": "NO_PREDICTIONS", "rows": 0}, indent=2))
        return 0
    pred["prediction_timestamp"] = pd.to_datetime(pred["prediction_timestamp"], utc=True, errors="coerce")
    pred["game_start_time"] = pd.to_datetime(pred["game_start_time"], utc=True, errors="coerce")
    pred = pred[pred["temporal_integrity_status"].eq("PASS_SOURCE_LT_PREDICTION_LT_GAME_START") & (pred["prediction_timestamp"] < pred["game_start_time"])].copy()

    # Freeze latest valid strict-pregame prediction; never consult outcomes for selection.
    pred = pred.sort_values(KEY + ["prediction_timestamp"])
    first = pred.drop_duplicates(KEY, keep="first")[KEY + ["governing_run_tag", "expected_plate_appearances"]].rename(columns={"governing_run_tag": "first_valid_run_tag", "expected_plate_appearances": "first_expected_pa"})
    governing = pred.drop_duplicates(KEY, keep="last").merge(first, on=KEY, how="left")
    if governing.duplicated(KEY).any():
        raise ValueError("duplicate governing prediction identity")

    outcomes = normalize_outcomes(Path(args.outcomes_csv)) if args.outcomes_csv else pd.DataFrame(columns=KEY + ["actual_pa", "actual_hits", "game_completion_status"])
    duplicate_outcomes = int(outcomes.duplicated(KEY, keep=False).sum())
    outcomes = outcomes.drop_duplicates(KEY, keep=False)
    graded = governing.merge(outcomes, on=KEY, how="left", indicator=True)
    graded["outcome_status"] = np.where(graded["actual_pa"].notna() & graded["actual_hits"].notna(), "RESOLVED_OFFICIAL", "UNRESOLVED_RETRY")
    graded["hitless"] = np.where(graded["actual_hits"].notna(), (pd.to_numeric(graded["actual_hits"], errors="coerce") == 0).astype(float), np.nan)
    graded["low_pa"] = np.where(graded["actual_pa"].notna(), (pd.to_numeric(graded["actual_pa"], errors="coerce") <= 2).astype(float), np.nan)
    graded["pa_error"] = pd.to_numeric(graded["expected_plate_appearances"], errors="coerce") - pd.to_numeric(graded["actual_pa"], errors="coerce")
    graded["absolute_pa_error"] = graded["pa_error"].abs()

    out_root = Path(args.output_root)
    dates = sorted(graded["slate_date"].astype(str).unique())
    resolved = graded[graded["outcome_status"].eq("RESOLVED_OFFICIAL")].copy()
    summary = {
        "status": "DRY_RUN" if args.dry_run else "WRITTEN",
        "dates": dates, "frozen_prediction_rows": len(governing), "resolved_rows": len(resolved),
        "unresolved_rows": int((graded["outcome_status"] == "UNRESOLVED_RETRY").sum()),
        "duplicate_outcome_rows_rejected": duplicate_outcomes,
        "governing_rule": "latest valid strict-pregame prediction before first pitch",
    }
    if len(resolved):
        y_low = resolved["low_pa"].astype(int); p_low = pd.to_numeric(resolved["probability_pa_le_2"], errors="coerce")
        y_hit = resolved["hitless"].astype(int)
        summary.update({
            "pa_mae": float(resolved["absolute_pa_error"].mean()),
            "pa_rmse": float(np.sqrt((resolved["pa_error"] ** 2).mean())),
            "low_pa_pr_auc": safe_metric(average_precision_score, y_low, p_low),
            "low_pa_roc_auc": safe_metric(roc_auc_score, y_low, p_low),
        })
        for label, col in [("opportunity_only", "hitless_probability_opportunity_only"), ("opportunity_plus_hitter", "hitless_probability_opportunity_hitter")]:
            p = pd.to_numeric(resolved[col], errors="coerce").clip(1e-6, 1 - 1e-6)
            summary[f"{label}_pr_auc"] = safe_metric(average_precision_score, y_hit, p)
            summary[f"{label}_roc_auc"] = safe_metric(roc_auc_score, y_hit, p)
            summary[f"{label}_brier"] = safe_metric(brier_score_loss, y_hit, p)
            summary[f"{label}_log_loss"] = safe_metric(log_loss, y_hit, p)
    if args.dry_run:
        print(json.dumps(summary, indent=2, sort_keys=True, default=str))
        return 0

    out_root.mkdir(parents=True, exist_ok=True)
    for date in dates:
        daily = out_root / date
        daily.mkdir(parents=True, exist_ok=True)
        target = daily / "governing_official_grading_ledger.csv"
        if target.exists():
            old = pd.read_csv(target, low_memory=False)
            immutable = [c for c in governing.columns if c in old.columns]
            check = old.merge(graded, on=KEY, suffixes=("_old", "_new"))
            for col in immutable:
                if col in KEY:
                    continue
                if f"{col}_old" in check and not check[f"{col}_old"].astype(str).eq(check[f"{col}_new"].astype(str)).all():
                    raise ValueError(f"immutable prediction mutation rejected: {col}")
            if args.retry_unresolved_only:
                old = old[~old["outcome_status"].eq("UNRESOLVED_RETRY")]
            graded_date = graded[graded["slate_date"].astype(str).eq(date)]
            merged = pd.concat([old, graded_date], ignore_index=True, sort=False).drop_duplicates(KEY, keep="last")
        else:
            merged = graded[graded["slate_date"].astype(str).eq(date)]
        merged.to_csv(target, index=False)
        summary_tag = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        (daily / f"daily_grading_summary_{summary_tag}.json").write_text(json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
