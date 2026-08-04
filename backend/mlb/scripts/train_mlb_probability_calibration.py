#!/usr/bin/env python3
"""Train an MLB side-probability calibration layer from reconcile rows."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Sequence

import pandas as pd

from backend.mlb.shared.probability_calibration import (
    GLOBAL_PROP,
    brier_score,
    build_calibrator,
    calibrate_probability,
    calibration_curve,
)


DEFAULT_CORE_PROPS = "hits,singles,total_bases,hits_runs_rbis,strikeouts_pitching,outs_recorded"


def _parse_date(value: Any) -> str:
    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return ""
    return pd.Timestamp(dt).date().isoformat()


def _side_training_frame(rows: pd.DataFrame, *, prop_types: Sequence[str], training_scope: str) -> pd.DataFrame:
    df = rows.copy()
    for col in (
        "game_date",
        "prop_type",
        "model_prob_over",
        "model_prob_under",
        "model_pick_prob",
        "model_pick_side",
        "actual_model_pick_outcome",
        "actual_over_outcome",
        "actual_under_outcome",
    ):
        if col not in df.columns:
            df[col] = pd.NA
    df["game_date_norm"] = df["game_date"].map(_parse_date)
    df["prop_type"] = df["prop_type"].astype(str).str.strip().str.lower()
    prop_set = {str(p).strip().lower() for p in prop_types if str(p).strip()}
    if prop_set:
        df = df[df["prop_type"].isin(prop_set)].copy()

    out_rows: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        prop = str(row.get("prop_type") or "").strip().lower()
        if str(training_scope or "").strip().lower() != "all_sides":
            prob = pd.to_numeric(pd.Series([row.get("model_pick_prob")]), errors="coerce").iloc[0]
            outcome_norm = str(row.get("actual_model_pick_outcome") or "").strip().lower()
            if pd.isna(prob) or outcome_norm not in {"win", "loss"}:
                continue
            out_rows.append(
                {
                    "game_date": row.get("game_date_norm"),
                    "prop_type": prop,
                    "side": str(row.get("model_pick_side") or "").strip().lower(),
                    "raw_prob": float(prob),
                    "actual_win": 1 if outcome_norm == "win" else 0,
                }
            )
            continue
        over_prob = pd.to_numeric(pd.Series([row.get("model_prob_over")]), errors="coerce").iloc[0]
        under_prob = pd.to_numeric(pd.Series([row.get("model_prob_under")]), errors="coerce").iloc[0]
        if pd.isna(under_prob) and not pd.isna(over_prob):
            under_prob = 1.0 - float(over_prob)
        for side, prob, outcome in (
            ("over", over_prob, row.get("actual_over_outcome")),
            ("under", under_prob, row.get("actual_under_outcome")),
        ):
            outcome_norm = str(outcome or "").strip().lower()
            if pd.isna(prob) or outcome_norm not in {"win", "loss"}:
                continue
            out_rows.append(
                {
                    "game_date": row.get("game_date_norm"),
                    "prop_type": prop,
                    "side": side,
                    "raw_prob": float(prob),
                    "actual_win": 1 if outcome_norm == "win" else 0,
                }
            )
    return pd.DataFrame(out_rows)


def _comparison(side_df: pd.DataFrame, calibrator: dict[str, Any]) -> pd.DataFrame:
    if side_df.empty:
        return pd.DataFrame(
            columns=[
                "prop_type",
                "rows",
                "actual_win_rate",
                "avg_raw_prob",
                "avg_calibrated_prob",
                "raw_brier",
                "calibrated_brier",
                "brier_improvement",
                "calibration_model",
            ]
        )
    work = side_df.copy()
    min_prop_samples = int(calibrator.get("min_prop_samples") or 200)
    work["calibrated_prob"] = [
        calibrate_probability(calibrator, prop_type=prop, raw_prob=prob, min_prop_samples=min_prop_samples)
        for prop, prob in zip(work["prop_type"], work["raw_prob"])
    ]
    rows: list[dict[str, Any]] = []
    for prop, group in work.groupby("prop_type", dropna=False):
        prop_key = str(prop)
        model = calibrator.get("models", {}).get(prop_key)
        model_name = prop_key if isinstance(model, dict) and int(model.get("n") or 0) >= min_prop_samples else GLOBAL_PROP
        raw_brier = brier_score(group["raw_prob"], group["actual_win"])
        cal_brier = brier_score(group["calibrated_prob"], group["actual_win"])
        rows.append(
            {
                "prop_type": prop_key,
                "rows": int(len(group)),
                "actual_win_rate": float(group["actual_win"].mean()),
                "avg_raw_prob": float(group["raw_prob"].mean()),
                "avg_calibrated_prob": float(pd.to_numeric(group["calibrated_prob"], errors="coerce").mean()),
                "raw_brier": raw_brier,
                "calibrated_brier": cal_brier,
                "brier_improvement": (raw_brier - cal_brier) if raw_brier is not None and cal_brier is not None else None,
                "calibration_model": model_name,
            }
        )
    global_raw_brier = brier_score(work["raw_prob"], work["actual_win"])
    global_cal_brier = brier_score(work["calibrated_prob"], work["actual_win"])
    rows.append(
        {
            "prop_type": GLOBAL_PROP,
            "rows": int(len(work)),
            "actual_win_rate": float(work["actual_win"].mean()),
            "avg_raw_prob": float(work["raw_prob"].mean()),
            "avg_calibrated_prob": float(pd.to_numeric(work["calibrated_prob"], errors="coerce").mean()),
            "raw_brier": global_raw_brier,
            "calibrated_brier": global_cal_brier,
            "brier_improvement": (
                global_raw_brier - global_cal_brier
                if global_raw_brier is not None and global_cal_brier is not None
                else None
            ),
            "calibration_model": GLOBAL_PROP,
        }
    )
    return pd.DataFrame(rows).sort_values(["prop_type"], kind="stable").reset_index(drop=True)


def main(argv: Sequence[str] | None = None) -> int:
    from backend.mlb.shared.model_authority import assert_predictive_model_qualified

    assert_predictive_model_qualified("retired_model_recalibration")
    ap = argparse.ArgumentParser(description="Train MLB probability calibration from resolved reconcile rows.")
    ap.add_argument("--rows-csv", default="tmp/mlb_base_vs_market_rows_anybook_window.csv")
    ap.add_argument("--out-json", default="artifacts/analysis/mlb/calibration/mlb_probability_calibrator.json")
    ap.add_argument("--comparison-csv", default="artifacts/analysis/mlb/calibration/raw_vs_calibrated.csv")
    ap.add_argument("--curve-csv", default="artifacts/analysis/mlb/calibration/calibration_curve.csv")
    ap.add_argument("--prop-types", default=DEFAULT_CORE_PROPS)
    ap.add_argument("--from-date", default="")
    ap.add_argument("--to-date", default="")
    ap.add_argument("--min-prop-samples", type=int, default=200)
    ap.add_argument(
        "--training-scope",
        choices=["model_picks", "all_sides"],
        default="model_picks",
        help="model_picks calibrates model_pick_prob vs actual_model_pick_outcome; all_sides calibrates over/under side rows.",
    )
    args = ap.parse_args(list(argv) if argv is not None else None)

    rows_csv = Path(str(args.rows_csv)).expanduser()
    if not rows_csv.exists():
        raise FileNotFoundError(f"missing --rows-csv: {rows_csv}")
    raw = pd.read_csv(rows_csv, low_memory=False)
    if "game_date" in raw.columns:
        raw["game_date_norm"] = raw["game_date"].map(_parse_date)
        if args.from_date:
            raw = raw[raw["game_date_norm"] >= str(args.from_date)].copy()
        if args.to_date:
            raw = raw[raw["game_date_norm"] <= str(args.to_date)].copy()

    prop_types = [p.strip().lower() for p in str(args.prop_types or "").split(",") if p.strip()]
    calibrator = build_calibrator(
        raw,
        prop_types=prop_types,
        min_prop_samples=int(args.min_prop_samples),
        training_scope=str(args.training_scope),
    )
    side_df = _side_training_frame(raw, prop_types=prop_types, training_scope=str(args.training_scope))
    comparison = _comparison(side_df, calibrator)

    side_curve = side_df.copy()
    if not side_curve.empty:
        side_curve["calibrated_prob"] = [
            calibrate_probability(calibrator, prop_type=prop, raw_prob=prob, min_prop_samples=int(args.min_prop_samples))
            for prop, prob in zip(side_curve["prop_type"], side_curve["raw_prob"])
        ]
        raw_curve = calibration_curve(side_curve, prob_col="raw_prob", actual_col="actual_win", group_cols=["prop_type"])
        raw_curve["probability_kind"] = "raw"
        cal_curve = calibration_curve(side_curve, prob_col="calibrated_prob", actual_col="actual_win", group_cols=["prop_type"])
        cal_curve["probability_kind"] = "calibrated"
        curve = pd.concat([raw_curve, cal_curve], ignore_index=True)
    else:
        curve = pd.DataFrame()

    out_json = Path(str(args.out_json)).expanduser()
    comparison_csv = Path(str(args.comparison_csv)).expanduser()
    curve_csv = Path(str(args.curve_csv)).expanduser()
    out_json.parent.mkdir(parents=True, exist_ok=True)
    comparison_csv.parent.mkdir(parents=True, exist_ok=True)
    curve_csv.parent.mkdir(parents=True, exist_ok=True)

    out_json.write_text(json.dumps(calibrator, indent=2, sort_keys=True), encoding="utf-8")
    comparison.to_csv(comparison_csv, index=False)
    curve.to_csv(curve_csv, index=False)

    global_model = calibrator.get("models", {}).get(GLOBAL_PROP, {})
    print(f"[mlb-calibration] rows_csv={rows_csv}")
    print(
        "[mlb-calibration] training "
        f"rows={calibrator.get('training_rows')} side_rows={calibrator.get('training_side_rows')} "
        f"scope={calibrator.get('training_scope')} props={calibrator.get('prop_types')}"
    )
    print(
        "[mlb-calibration] global "
        f"n={global_model.get('n')} raw_brier={global_model.get('brier_raw')} "
        f"calibrated_brier={global_model.get('brier_calibrated')}"
    )
    print(f"[mlb-calibration] out_json={out_json}")
    print(f"[mlb-calibration] comparison_csv={comparison_csv}")
    print(f"[mlb-calibration] curve_csv={curve_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
