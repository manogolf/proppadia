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
    required_out = {"game_id", "player_id"}
    if not required_pred.issubset(predictions) or not required_out.issubset(outcomes):
        raise ValueError("MISSING_GRADE_COLUMNS")
    if "official_points" not in outcomes and not {"goals", "assists"}.issubset(outcomes):
        raise ValueError("MISSING_OFFICIAL_POINTS")
    if predictions.duplicated(["game_id", "player_id"]).any() or outcomes.duplicated(["game_id", "player_id"]).any():
        raise ValueError("DUPLICATE_GRADE_KEY")
    df = predictions.merge(outcomes, on=["game_id", "player_id"], how="left", validate="one_to_one", indicator=True)
    if "participation_state" in df:
        state = df.participation_state
    else:
        state = pd.Series("PARTICIPATED", index=df.index)
    if "official_points" in df:
        points = pd.to_numeric(df.official_points, errors="coerce")
    else:
        points = pd.to_numeric(df.goals, errors="coerce") + pd.to_numeric(df.assists, errors="coerce")
    if "official_final" in df:
        final = df.official_final.astype(str).str.lower().isin({"true", "t", "1"})
    else:
        final = pd.Series(True, index=df.index)
    participated = state.eq("PARTICIPATED") & points.notna() & final & df._merge.eq("both")
    nonparticipant = state.isin({"SCRATCHED", "NONPARTICIPANT", "POSTPONED"})
    unresolved = ~(participated | nonparticipant)
    if participated.sum() == 0:
        raise ValueError("NO_SETTLED_PARTICIPANTS")
    graded = df.loc[participated]
    y = points.loc[participated].astype(int).to_numpy()
    mu = np.maximum(graded.expected_points.astype(float).to_numpy(), 1e-8)
    selected_side = selected_side_record(
        graded, y, ("prob_over_0_5", "prob_over_1_5", "prob_over_2_5"),
        unresolved=int(unresolved.sum()))
    out = {"n": len(graded), "identity_reconciled_count": int(df._merge.eq("both").sum()),
           "missing_outcome_count": int(df._merge.eq("left_only").sum()),
           "participated_graded_count": int(participated.sum()), "nonparticipant_count": int(nonparticipant.sum()),
           "unresolved_count": int(unresolved.sum()),
           "mean_predicted": float(mu.mean()), "mean_observed": float(y.mean()),
           "count_nll": float(-poisson.logpmf(y, mu).mean()), "crossing_count": int(((df.prob_over_0_5 < df.prob_over_1_5) | (df.prob_over_1_5 < df.prob_over_2_5)).sum()),
           "thresholds": {}, "selected_side_record": selected_side}
    threshold_losses = []
    for line, col in zip((1, 2, 3), ("prob_over_0_5", "prob_over_1_5", "prob_over_2_5")):
        target = (y >= line).astype(int)
        p = np.clip(graded[col].astype(float).to_numpy(), 1e-12, 1 - 1e-12)
        threshold_log_loss = float(log_loss(target, p, labels=[0, 1]))
        base_rate = float(target.mean())
        average_precision = float(average_precision_score(target, p)) if target.sum() else None
        threshold_losses.append(threshold_log_loss)
        out["thresholds"][col] = {"base_rate": base_rate, "log_loss": threshold_log_loss,
            "brier": float(brier_score_loss(target, p)),
            "auc": float(roc_auc_score(target, p)) if len(np.unique(target)) == 2 else None,
            "average_precision": average_precision,
            "lift": average_precision / base_rate if average_precision is not None and base_rate > 0 else None}
    out["average_threshold_log_loss"] = float(np.mean(threshold_losses))
    return out


def selected_side_record(graded: pd.DataFrame, points: np.ndarray,
                         probability_columns: tuple[str, str, str], *,
                         unresolved: int = 0) -> dict:
    """Grade over/under at the established 0.5 probability decision threshold."""
    lines = {}
    side_totals = {side: {"wins": 0, "losses": 0, "settled": 0}
                   for side in ("OVER", "UNDER")}
    total_wins = total_losses = 0
    for line, threshold, column in zip((0.5, 1.5, 2.5), (1, 2, 3), probability_columns):
        p = pd.to_numeric(graded[column], errors="raise").to_numpy(dtype=float)
        predicted_over = p >= 0.5
        realized_over = points >= threshold
        wins = predicted_over == realized_over
        losses = ~wins
        over_mask, under_mask = predicted_over, ~predicted_over
        line_wins, line_losses = int(wins.sum()), int(losses.sum())
        lines[str(line)] = {
            "wins": line_wins, "losses": line_losses, "settled": len(graded),
            "win_rate": line_wins / len(graded) if len(graded) else None,
            "predicted_over_count": int(over_mask.sum()),
            "predicted_under_count": int(under_mask.sum()),
            "realized_over_count": int(realized_over.sum()),
            "realized_under_count": int((~realized_over).sum()),
        }
        for side, mask in (("OVER", over_mask), ("UNDER", under_mask)):
            side_wins = int((wins & mask).sum())
            side_losses = int((losses & mask).sum())
            side_totals[side]["wins"] += side_wins
            side_totals[side]["losses"] += side_losses
            side_totals[side]["settled"] += int(mask.sum())
        total_wins += line_wins
        total_losses += line_losses
    for totals in side_totals.values():
        totals["win_rate"] = totals["wins"] / totals["settled"] if totals["settled"] else None
    settled = total_wins + total_losses
    return {
        "wins": total_wins, "losses": total_losses, "settled": settled,
        "win_rate": total_wins / settled if settled else None, "pushes": 0,
        "unresolved": int(unresolved),
        "denominator_definition": "participated identities with final official points, across lines 0.5/1.5/2.5",
        "by_line": lines, "by_selected_side": side_totals,
    }


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
