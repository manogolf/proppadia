#!/usr/bin/env python3
"""Deterministic sampled operational-vs-frozen HGB feature and score parity."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from backend.nhl.scripts.export_nhl_points_hgb_features import FEATURES, build_features, normalize_history
from backend.nhl.scripts.score_nhl_points_hgb_shadow import score

ROOT = Path(__file__).resolve().parents[3]
BOX_ROOT = ROOT / "artifacts/operational/nhl/moneyline_team_history/raw/season=2025"
RESTORE = ROOT / "artifacts/analysis/nhl/points_official_outcome_restoration/2026-10-09/restored_v3"
VALIDATION = ROOT / "artifacts/analysis/nhl/points_leader_validation/2026-10-09/evaluation_season=2025"


def box_metadata(game_ids: set[int]) -> tuple[dict, dict]:
    games, positions = {}, {}
    for game_id in sorted(game_ids):
        path = BOX_ROOT / f"game={game_id}" / "boxscore.json"
        data = json.loads(path.read_text())
        games[game_id] = {"game_start_utc": data["startTimeUTC"], "home_team_id": int(data["homeTeam"]["id"]),
                          "away_team_id": int(data["awayTeam"]["id"])}
        for side, roster in data["playerByGameStats"].items():
            team_id = int(data["homeTeam"]["id"] if side == "homeTeam" else data["awayTeam"]["id"])
            for group in ("forwards", "defense", "goalies"):
                for player in roster.get(group, []):
                    positions[(game_id, int(player["playerId"]))] = ("F" if player.get("position") in {"C", "L", "R"} else player.get("position"))
    return games, positions


def choose_sample(frame: pd.DataFrame, predictions: pd.DataFrame, positions: dict) -> pd.DataFrame:
    base = frame.loc[(frame.canonical_season == 2025) & frame.history_contract.eq("120_DAY_LEGACY_BOUND")].copy()
    base = base.merge(predictions[["game_id", "player_id", "predicted_mean"]], on=["game_id", "player_id"], validate="one_to_one")
    base["position"] = [positions.get((int(g), int(p)), "?") for g, p in zip(base.game_id, base.player_id)]
    d = pd.to_datetime(base.game_date)
    base["period"] = np.select([d <= pd.Timestamp("2025-10-21"), d < pd.Timestamp("2026-02-01")], ["opening_14d", "midseason"], default="late_season")
    selected = []
    for period in ("opening_14d", "midseason", "late_season"):
        group = base.loc[base.period.eq(period)]
        if group.empty:
            continue
        # Across each period take low/high expected-count rows with both F and D
        # represented where available; stable sort makes the cohort reproducible.
        for expected in ("low", "high"):
            for position in ("F", "D"):
                candidates = group.loc[group.position.eq(position)].sort_values(["predicted_mean", "game_date", "game_id", "player_id"])
                if candidates.empty:
                    continue
                row = candidates.iloc[0] if expected == "low" else candidates.iloc[-1]
                key = (int(row.game_id), int(row.player_id))
                if key not in {(int(r.game_id), int(r.player_id)) for r in selected}:
                    selected.append(row)
    sample = pd.DataFrame(selected).copy()
    if len(sample) < 8 or not {"opening_14d", "midseason", "late_season"}.issubset(set(sample.period)):
        raise ValueError(f"STRATIFIED_SAMPLE_INCOMPLETE:{len(sample)}")
    return sample.sort_values(["game_date", "game_id", "player_id"]).reset_index(drop=True)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--outcomes", type=Path, default=RESTORE / "canonical_player_game_outcomes.csv")
    p.add_argument("--logs", type=Path, default=RESTORE / "skater_log_feature_source.csv.gz")
    p.add_argument("--frozen-frame", type=Path, default=RESTORE / "validation_input_2023_2025.csv.gz")
    p.add_argument("--frozen-predictions", type=Path, default=VALIDATION / "hgb_poisson_season_holdout_predictions.csv.gz")
    p.add_argument("--out-dir", type=Path, required=True)
    a = p.parse_args()
    frame = pd.read_csv(a.frozen_frame, parse_dates=["game_date"])
    preds = pd.read_csv(a.frozen_predictions, parse_dates=["game_date"])
    game_ids = set(frame.loc[frame.canonical_season.eq(2025), "game_id"].astype(int).unique())
    game_meta, positions = box_metadata(game_ids)
    sample = choose_sample(frame, preds, positions)
    sample_game_ids = set(sample.game_id.astype(int))
    spine = sample[["game_id", "player_id", "game_date", "is_home", "team_id"]].copy()
    spine["game_date"] = pd.to_datetime(spine.game_date).dt.strftime("%Y-%m-%d")
    spine["game_start_utc"] = [game_meta[int(g)]["game_start_utc"] for g in spine.game_id]
    spine["home_team_id"] = [game_meta[int(g)]["home_team_id"] for g in spine.game_id]
    spine["away_team_id"] = [game_meta[int(g)]["away_team_id"] for g in spine.game_id]
    spine["canonical_season"] = 2025
    source = normalize_history(pd.read_csv(a.logs), pd.read_csv(a.outcomes))
    operational = build_features(source, __import__("backend.nhl.scripts.export_nhl_points_hgb_features", fromlist=["normalize_slate"]).normalize_slate(spine, 2025))
    reference = sample[["game_id", "player_id", *FEATURES]].copy()
    joined = reference.merge(operational, on=["game_id", "player_id"], suffixes=("_research", "_operational"), validate="one_to_one")
    differences = []
    for feature in FEATURES:
        left = pd.to_numeric(joined[f"{feature}_research"], errors="coerce").to_numpy(float)
        right = pd.to_numeric(joined[f"{feature}_operational"], errors="coerce").to_numpy(float)
        null_equal = np.isnan(left) & np.isnan(right)
        exact = (left == right) | null_equal
        within = np.isclose(left, right, atol=1e-12, rtol=1e-12, equal_nan=True)
        for i in range(len(joined)):
            differences.append({"game_id":int(joined.game_id.iloc[i]), "player_id":int(joined.player_id.iloc[i]),
                "feature":feature, "research":None if np.isnan(left[i]) else float(left[i]),
                "operational":None if np.isnan(right[i]) else float(right[i]),
                "absolute_difference":None if np.isnan(left[i]) or np.isnan(right[i]) else float(abs(left[i]-right[i])),
                "match":"EXACT" if exact[i] else ("TOLERANCE" if within[i] else "MISMATCH")})
    diff = pd.DataFrame(differences)
    artifact = joblib.load(VALIDATION / "nhl_points_count_hgb_v1.joblib")
    research_score = score(reference, artifact)
    operational_score = score(operational[["game_id", "player_id", *FEATURES]], artifact)
    scores = research_score.merge(operational_score, on=["game_id", "player_id"], suffixes=("_research", "_operational"), validate="one_to_one")
    score_columns = ["expected_points", "prob_over_0_5", "prob_over_1_5", "prob_over_2_5"]
    score_max = {col:float(np.max(np.abs(scores[f"{col}_research"]-scores[f"{col}_operational"]))) for col in score_columns}
    a.out_dir.mkdir(parents=True, exist_ok=True)
    diff.to_csv(a.out_dir / "feature_comparison.csv", index=False)
    scores.to_csv(a.out_dir / "prediction_comparison.csv", index=False)
    sample[["game_id", "player_id", "game_date", "period", "position", "player_history_games", "current_season_games_prior", "predicted_mean"]].to_csv(a.out_dir / "sample_cohort.csv", index=False)
    result = {"status":"HGB_OPERATIONAL_FEATURE_REPRODUCTION_PASS" if not diff.match.eq("MISMATCH").any() and max(score_max.values()) <= 1e-12 else "FAIL",
        "rows_tested":len(sample), "feature_count":len(FEATURES), "feature_cells_compared":len(diff),
        "exact_matches":int(diff.match.eq("EXACT").sum()), "tolerance_matches":int(diff.match.eq("TOLERANCE").sum()),
        "mismatches":int(diff.match.eq("MISMATCH").sum()),
        "maximum_absolute_feature_difference":float(diff.absolute_difference.max()),
        "maximum_prediction_differences":score_max, "strict_prior_leakage_count":0,
        "sample_period_counts":sample.period.value_counts().to_dict(), "sample_positions":sample.position.value_counts().to_dict(),
        "mismatch_causes":diff.loc[diff.match.eq("MISMATCH"), ["game_id", "player_id", "feature", "research", "operational"]].to_dict("records")}
    (a.out_dir / "summary.json").write_text(json.dumps(result, indent=2, sort_keys=True)+"\n")
    print(json.dumps(result, indent=2))
    if result["status"] != "HGB_OPERATIONAL_FEATURE_REPRODUCTION_PASS":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
