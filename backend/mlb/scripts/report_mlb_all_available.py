#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
from pandas.errors import EmptyDataError


RESOLVED_OUTCOMES = {"win", "loss", "push"}
MODEL_OUTCOMES = {"win", "loss"}


def _normalize_text(series: pd.Series) -> pd.Series:
    return series.astype(str).str.lower().str.strip()


def _safe_pct(numer: int, denom: int) -> float | None:
    if denom <= 0:
        return None
    return round((100.0 * float(numer)) / float(denom), 2)


def main() -> int:
    ap = argparse.ArgumentParser(description="Report MLB all-available resolved slate outcomes from reconcile rows.")
    ap.add_argument("--rows-csv", required=True, help="Reconcile rows CSV from build_mlb_reconcile_rows.py")
    ap.add_argument("--out-json", default="tmp/analysis/mlb_all_available_summary.json")
    ap.add_argument("--out-csv", default="tmp/analysis/mlb_all_available_by_prop.csv")
    args = ap.parse_args()

    rows_csv = Path(args.rows_csv).expanduser()
    if not rows_csv.exists():
        raise FileNotFoundError(f"rows csv not found: {rows_csv}")

    out_json = Path(args.out_json).expanduser()
    out_csv = Path(args.out_csv).expanduser()
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    try:
        df = pd.read_csv(rows_csv, low_memory=False)
    except EmptyDataError:
        payload = {
            "rows_csv": str(rows_csv),
            "window": {"game_date_min": None, "game_date_max": None},
            "counts": {
                "rows_input": 0,
                "rows_resolved_any": 0,
                "rows_resolved_two_sided": 0,
                "rows_with_model_pick_result": 0,
            },
            "overall": {
                "model_win_rate_pct": None,
                "status": "no_data",
            },
            "outputs": {
                "by_prop_csv": str(out_csv),
                "summary_json": str(out_json),
            },
        }
        pd.DataFrame().to_csv(out_csv, index=False)
        out_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(json.dumps(payload, indent=2))
        return 0

    for col in (
        "prop_type",
        "game_date",
        "actual_over_outcome",
        "actual_under_outcome",
        "actual_model_pick_outcome",
    ):
        if col not in df.columns:
            df[col] = pd.NA

    df["actual_over_outcome"] = _normalize_text(df["actual_over_outcome"])
    df["actual_under_outcome"] = _normalize_text(df["actual_under_outcome"])
    df["actual_model_pick_outcome"] = _normalize_text(df["actual_model_pick_outcome"])
    df["prop_type"] = _normalize_text(df["prop_type"])

    resolved_any_mask = df["actual_over_outcome"].isin(RESOLVED_OUTCOMES) | df["actual_under_outcome"].isin(RESOLVED_OUTCOMES)
    resolved_two_mask = df["actual_over_outcome"].isin(RESOLVED_OUTCOMES) & df["actual_under_outcome"].isin(RESOLVED_OUTCOMES)
    model_mask = resolved_any_mask & df["actual_model_pick_outcome"].isin(MODEL_OUTCOMES)

    resolved_any = df.loc[resolved_any_mask].copy()
    resolved_two = df.loc[resolved_two_mask].copy()
    model_rows = df.loc[model_mask].copy()

    resolved_two["over_hit"] = resolved_two["actual_over_outcome"].eq("win")
    resolved_two["under_hit"] = resolved_two["actual_under_outcome"].eq("win")
    model_rows["model_win"] = model_rows["actual_model_pick_outcome"].eq("win")

    by_prop = (
        resolved_two.groupby("prop_type", dropna=False)
        .agg(
            rows=("prop_type", "size"),
            over_hit_rate=("over_hit", "mean"),
            under_hit_rate=("under_hit", "mean"),
        )
        .reset_index()
    )

    by_prop_model = (
        model_rows.groupby("prop_type", dropna=False)
        .agg(
            model_rows=("prop_type", "size"),
            model_win_rate=("model_win", "mean"),
        )
        .reset_index()
    )

    out = by_prop.merge(by_prop_model, on="prop_type", how="left")
    if out.empty:
        out = pd.DataFrame(
            columns=[
                "prop_type",
                "rows",
                "over_hit_rate",
                "under_hit_rate",
                "model_rows",
                "model_win_rate",
                "over_hit_rate_pct",
                "under_hit_rate_pct",
                "model_win_rate_pct",
            ]
        )
    else:
        out = out.sort_values("rows", ascending=False).reset_index(drop=True)
        out["over_hit_rate_pct"] = (out["over_hit_rate"] * 100.0).round(2)
        out["under_hit_rate_pct"] = (out["under_hit_rate"] * 100.0).round(2)
        out["model_win_rate_pct"] = (out["model_win_rate"] * 100.0).round(2)
    out.to_csv(out_csv, index=False)

    game_date = pd.to_datetime(df["game_date"], errors="coerce")
    game_date_min = game_date.min()
    game_date_max = game_date.max()
    model_wins = int(model_rows["model_win"].sum()) if not model_rows.empty else 0
    model_total = int(model_rows.shape[0])

    payload = {
        "rows_csv": str(rows_csv),
        "window": {
            "game_date_min": None if pd.isna(game_date_min) else str(game_date_min.date()),
            "game_date_max": None if pd.isna(game_date_max) else str(game_date_max.date()),
        },
        "counts": {
            "rows_input": int(df.shape[0]),
            "rows_resolved_any": int(resolved_any.shape[0]),
            "rows_resolved_two_sided": int(resolved_two.shape[0]),
            "rows_with_model_pick_result": model_total,
        },
        "overall": {
            "model_win_rate_pct": _safe_pct(model_wins, model_total),
        },
        "outputs": {
            "by_prop_csv": str(out_csv),
            "summary_json": str(out_json),
        },
    }

    out_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

