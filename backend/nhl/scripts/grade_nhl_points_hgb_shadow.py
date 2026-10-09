#!/usr/bin/env python3
"""Grade retained NHL Points HGB shadows against official goals plus assists."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import poisson
from sklearn.metrics import average_precision_score, brier_score_loss, log_loss, roc_auc_score


def grade(predictions: pd.DataFrame, outcomes: pd.DataFrame) -> dict:
    required_pred = {"game_id", "player_id", "expected_points", "prob_over_0_5", "prob_over_1_5", "prob_over_2_5"}
    required_out = {"game_id", "player_id", "goals", "assists"}
    if not required_pred.issubset(predictions) or not required_out.issubset(outcomes):
        raise ValueError("MISSING_GRADE_COLUMNS")
    if predictions.duplicated(["game_id", "player_id"]).any() or outcomes.duplicated(["game_id", "player_id"]).any():
        raise ValueError("DUPLICATE_GRADE_KEY")
    df = predictions.merge(outcomes, on=["game_id", "player_id"], how="inner", validate="one_to_one")
    if len(df) != len(predictions):
        raise ValueError("UNSETTLED_OR_MISSING_OUTCOMES")
    y = (df.goals + df.assists).astype(int).to_numpy()
    mu = np.maximum(df.expected_points.astype(float).to_numpy(), 1e-8)
    out = {"n": len(df), "mean_predicted": float(mu.mean()), "mean_observed": float(y.mean()),
           "count_nll": float(-poisson.logpmf(y, mu).mean()), "crossing_count": int(((df.prob_over_0_5 < df.prob_over_1_5) | (df.prob_over_1_5 < df.prob_over_2_5)).sum()),
           "thresholds": {}}
    for line, col in zip((1, 2, 3), ("prob_over_0_5", "prob_over_1_5", "prob_over_2_5")):
        target = (y >= line).astype(int)
        p = np.clip(df[col].astype(float).to_numpy(), 1e-12, 1 - 1e-12)
        out["thresholds"][col] = {"base_rate": float(target.mean()), "log_loss": float(log_loss(target, p, labels=[0, 1])),
            "brier": float(brier_score_loss(target, p)),
            "auc": float(roc_auc_score(target, p)) if len(np.unique(target)) == 2 else None,
            "average_precision": float(average_precision_score(target, p)) if target.sum() else None}
    return out


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--predictions", type=Path, required=True)
    p.add_argument("--official-outcomes", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    a = p.parse_args()
    result = grade(pd.read_csv(a.predictions), pd.read_csv(a.official_outcomes))
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
