#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd


CATEGORY_MAP: Dict[str, List[str]] = {
    "decision_quality": ["accuracy_pct", "false_over", "false_under"],
    "ranking_quality": ["auc_p_over"],
    "probability_quality": ["brier_score", "log_loss", "calibration_decile_error_pp"],
}


def _norm(series: pd.Series, *, higher_is_better: bool) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce").astype(float)
    if s.notna().sum() == 0:
        return pd.Series(np.full(len(s), 0.5), index=s.index)
    lo = float(s.min(skipna=True))
    hi = float(s.max(skipna=True))
    if np.isclose(hi, lo):
        return pd.Series(np.full(len(s), 0.5), index=s.index)
    out = (s - lo) / (hi - lo)
    if not higher_is_better:
        out = 1.0 - out
    return out.clip(0.0, 1.0).fillna(0.5)


def _model_type(row: pd.Series) -> str:
    decision = float(row["decision_quality_score"])
    probability = float(row["probability_quality_score"])
    margin = 0.08
    minimum_axis_strength = 0.50
    if decision >= probability + margin and decision >= minimum_axis_strength:
        return "decision_optimized"
    if probability >= decision + margin and probability >= minimum_axis_strength:
        return "probability_optimized"
    return "balanced"


def _strength_text(df: pd.DataFrame, row: pd.Series) -> str:
    checks = [
        ("accuracy_pct", "top accuracy", "max"),
        ("auc_p_over", "top AUC", "max"),
        ("false_over", "lowest false_over", "min"),
        ("false_under", "lowest false_under", "min"),
        ("brier_score", "best Brier", "min"),
        ("log_loss", "best log loss", "min"),
        ("calibration_decile_error_pp", "best calibration", "min"),
    ]
    tags: List[str] = []
    for col, label, mode in checks:
        values = pd.to_numeric(df[col], errors="coerce")
        value = pd.to_numeric(pd.Series([row[col]]), errors="coerce").iloc[0]
        if pd.isna(value) or values.notna().sum() == 0:
            continue
        target = values.max(skipna=True) if mode == "max" else values.min(skipna=True)
        if np.isclose(float(value), float(target), atol=1e-12):
            tags.append(label)
    if not tags:
        tags = ["mixed strengths"]
    head = str(row["model_type"]).replace("_", "-")
    return f"{head}: {', '.join(tags[:3])}"


def _build_cards(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["decision_quality_score"] = (
        _norm(out["accuracy_pct"], higher_is_better=True)
        + _norm(out["false_over"], higher_is_better=False)
        + _norm(out["false_under"], higher_is_better=False)
    ) / 3.0
    out["ranking_quality_score"] = _norm(out["auc_p_over"], higher_is_better=True)
    out["probability_quality_score"] = (
        _norm(out["brier_score"], higher_is_better=False)
        + _norm(out["log_loss"], higher_is_better=False)
        + _norm(out["calibration_decile_error_pp"], higher_is_better=False)
    ) / 3.0
    out["model_type"] = out.apply(_model_type, axis=1)
    out["strengths"] = out.apply(lambda r: _strength_text(out, r), axis=1)
    for col in ("decision_quality_score", "ranking_quality_score", "probability_quality_score"):
        out[col] = out[col].round(6)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Build standardized hits model cards from leaderboard output.")
    ap.add_argument("--leaderboard-csv", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--cohort-type", default="fixed_gate")
    ap.add_argument("--cohort-label", default=None)
    args = ap.parse_args()

    lb_path = Path(args.leaderboard_csv).resolve()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    leaderboard = pd.read_csv(lb_path)
    scoped = leaderboard[leaderboard["cohort_type"] == str(args.cohort_type)].copy()
    if args.cohort_label:
        scoped = scoped[scoped["cohort_label"] == str(args.cohort_label)].copy()
    if scoped.empty:
        raise RuntimeError("no rows available for requested cohort scope")

    cards = _build_cards(scoped)
    ordered_cols = [
        "model_name",
        "model_type",
        "model_root",
        "cohort_type",
        "cohort_label",
        "attempted",
        "scored",
        "correct",
        "accuracy_pct",
        "auc_p_over",
        "brier_score",
        "log_loss",
        "pred_over_pct",
        "actual_over_pct",
        "false_over",
        "false_under",
        "calibration_decile_error_pp",
        "decision_quality_score",
        "ranking_quality_score",
        "probability_quality_score",
        "strengths",
    ]
    cards = cards[ordered_cols].copy()

    axis_table = cards[
        [
            "model_name",
            "model_type",
            "decision_quality_score",
            "accuracy_pct",
            "false_over",
            "false_under",
            "ranking_quality_score",
            "auc_p_over",
            "probability_quality_score",
            "brier_score",
            "log_loss",
            "calibration_decile_error_pp",
            "pred_over_pct",
            "actual_over_pct",
            "strengths",
        ]
    ].copy()

    cards_csv = out_dir / "model_cards.csv"
    cards_json = out_dir / "model_cards.json"
    axis_csv = out_dir / "multi_axis_comparison.csv"
    manifest_json = out_dir / "model_cards_manifest.json"

    cards.to_csv(cards_csv, index=False)
    axis_table.to_csv(axis_csv, index=False)

    payload: Dict[str, Any] = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "source_leaderboard_csv": str(lb_path),
        "cohort_scope": {
            "cohort_type": str(args.cohort_type),
            "cohort_label": str(args.cohort_label) if args.cohort_label else None,
        },
        "category_metric_map": CATEGORY_MAP,
        "models": cards.to_dict(orient="records"),
    }
    cards_json.write_text(json.dumps(payload, indent=2))

    manifest: Dict[str, Any] = {
        "files": {
            "model_cards_csv": str(cards_csv),
            "model_cards_json": str(cards_json),
            "multi_axis_comparison_csv": str(axis_csv),
        },
        "category_metric_map": CATEGORY_MAP,
    }
    manifest_json.write_text(json.dumps(manifest, indent=2))

    print(f"wrote {cards_csv}")
    print(f"wrote {cards_json}")
    print(f"wrote {axis_csv}")
    print(f"wrote {manifest_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
