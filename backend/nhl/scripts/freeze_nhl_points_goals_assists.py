#!/usr/bin/env python3
"""Freeze the NHL Points goals + assists model (P2.39, operator-approved 2026-11-01).

Expected points = a goals model + an assists model, each the production HGB
recipe (fill zero, StandardScaler, Poisson HGB) on the ten core inputs plus
``points_component_features.FEATURES`` (own points, goal and assist rates and
games, all from official outcomes). Validated in
``research_nhl_points_goal_assist.py``.

Training: the 2023-24 target rows of the month-end research matrices (core
inputs from the live feature builder), new inputs and labels from the official
outcomes (restored 2023-25 frame; 2022-23 totals from the NHL stats API). The
frame is written as CSV and fitted twice from it; both refits and the artifact
must agree on the 2025 holdout. Also writes the prior-season totals and league
rates that live scoring reads. Nothing changes production.
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from scipy.stats import poisson

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl import points_component_features as pcf  # noqa: E402
from backend.nhl.scripts import research_nhl_points_goal_assist as research  # noqa: E402
from backend.nhl.scripts import research_nhl_points_hgb_month_end_refit as base  # noqa: E402
from backend.nhl.scripts import research_nhl_points_hgb_own_rate as own  # noqa: E402
from backend.nhl.scripts import run_nhl_points_hgb_pp_corrected_refit as refit  # noqa: E402
from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/points_hgb_pp_corrected_refit/production_2026-11-01_goals_assists"
ARTIFACT = "nhl_points_goals_assists_v1.joblib"
FEATURES = list(scorer.CORE_FEATURES) + pcf.FEATURES
KEYS = ["game_id", "player_id"]


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def identity(contract: dict) -> str:
    body = {k: v for k, v in contract.items() if k != "model_identity_sha256"}
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def official() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    frozen = pd.read_csv(refit.FROZEN_FRAME, low_memory=False)
    frozen["game_id"] = frozen.game_id.astype("int64"); frozen["player_id"] = frozen.player_id.astype("int64")
    outcomes, _ = refit.official_outcomes(frozen)
    outcomes = outcomes.rename(columns={"canonical_season": "season"})
    outcomes["game_id"] = outcomes.game_id.astype("int64"); outcomes["player_id"] = outcomes.player_id.astype("int64")
    api = pd.read_csv(own.SEASON_2022)
    prior = pd.concat([pcf.season_totals(outcomes[outcomes.season.isin([2023, 2024, 2025])]),
                       pd.DataFrame({"player_id": api.player_id, "season": 2022, "games": api.games, "points": api.points,
                                     "goals": api.goals, "assists": api.assists})], ignore_index=True)[pcf.TOTAL_COLUMNS]
    return outcomes, prior, pcf.league_rates(prior)


def fit_pair(train: pd.DataFrame) -> dict:
    g = research.fit_count(train, FEATURES, "goals")
    a = research.fit_count(train, FEATURES, "assists")
    return {"structure": "GOALS_PLUS_ASSISTS", "feature_columns": FEATURES, "fill_zero": True,
            "scaler": g["scaler"], "goals": g, "assists": a}


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=False)
    outcomes, prior, league = official()
    data = joblib.load(base.OUT / "matrices.joblib")
    hist = data["historical"].rename(columns={"canonical_season": "season"})
    hist = pcf.add_features(hist, outcomes, prior, league)
    hist = hist.merge(outcomes[KEYS + ["goals", "assists"]], on=KEYS, how="left", validate="one_to_one")
    if hist.goals.isna().any() or (hist.goals + hist.assists != hist.realized_points).any():
        raise RuntimeError("POINTS_COMPONENT_LABELS_INCONSISTENT")
    cols = ["slate_date", *KEYS, "season", *FEATURES, "goals", "assists", "realized_points"]
    train_path, hold_path = OUT / "training_frame_2023_2024.csv.gz", OUT / "holdout_frame_2025.csv.gz"
    hist[hist.season.isin([2023, 2024])][cols].to_csv(train_path, index=False, float_format="%.17g")
    hist[hist.season.eq(2025)][cols].to_csv(hold_path, index=False, float_format="%.17g")
    prior_path, league_path = OUT / "prior_season_totals.csv", OUT / "league_rates.csv"
    prior.sort_values(["season", "player_id"]).to_csv(prior_path, index=False)
    league.rename_axis("season").reset_index().to_csv(league_path, index=False, float_format="%.17g")
    train, hold = (pd.read_csv(p, float_precision="round_trip") for p in (train_path, hold_path))
    first, second = fit_pair(train), fit_pair(train.copy())
    joblib.dump(first, OUT / ARTIFACT)
    frozen = joblib.load(OUT / ARTIFACT)
    p1, p2, pf = (scorer.score(hold, m).expected_points.to_numpy() for m in (first, second, frozen))
    pred_path = OUT / "frozen_holdout_predictions_2025.csv"
    hold[KEYS].assign(expected_points=pf).to_csv(pred_path, index=False, float_format="%.17g")
    # Holdout vs production on the same rows (core inputs identical).
    hold_full = data["historical"][data["historical"].canonical_season.eq(2025)].reset_index(drop=True)
    prod = scorer.score(hold_full, joblib.load(scorer.PP_CORRECTED_CONTRACT_PATH.parent / "nhl_points_count_hgb_v1_pp_corrected.joblib")).expected_points.to_numpy()
    m = hold[KEYS].assign(new=pf).merge(hold_full[KEYS].assign(prod=prod), on=KEYS)
    m = m.merge(hold[KEYS + ["realized_points", "slate_date"]], on=KEYS)
    evals = {}
    for line in (0.5, 1.5, 2.5):
        y = (m.realized_points > line).astype(float).to_numpy()
        r = base.paired_bootstrap(base.log_loss_rows(poisson.sf(int(line), m.new), y),
                                  base.log_loss_rows(poisson.sf(int(line), m["prod"]), y), m.slate_date.astype(str).to_numpy())
        evals[str(line)] = {"diff_x1000": round(r["mean_diff"] * 1e3, 2), "ci": [round(c * 1e3, 2) for c in r["ci95"]]}
    previous = json.loads(scorer.PP_CORRECTED_CONTRACT_PATH.read_text())
    contract = {
        "schema_version": "NHL_POINTS_RESEARCH_SHADOW_CONTRACT_V1", "research_model_id": "NHL_POINTS_COUNT_HGB_V1",
        "authority": "PRODUCTION_GOALS_PLUS_ASSISTS_REFIT", "structure": "GOALS_PLUS_ASSISTS",
        "structure_semantics": "expected points = goals HGB + assists HGB (production recipe each); Poisson ladder on the sum",
        "training_seasons": [2023, 2024], "training_rows": int(len(train)), "evaluation_season_confirmation": 2025,
        "training_source_frame_sha256": sha(train_path), "feature_contract": "NHL_POINTS_LEADER_CORE_FEATURES_V3_COMPONENT_RATES",
        "feature_columns": FEATURES, "component_feature_module": "backend/nhl/points_component_features.py",
        "component_feature_module_sha256": sha(ROOT / "backend/nhl/points_component_features.py"),
        "prior_season_totals": prior_path.name, "prior_season_totals_sha256": sha(prior_path),
        "league_rates": league_path.name, "league_rates_sha256": sha(league_path),
        "prior_totals_seasons": sorted(int(s) for s in prior.season.unique()),
        "history_contract": previous["history_contract"], "history_semantics": previous["history_semantics"],
        "current_season_history": "official outcomes from the season's first game (not only 120 days) for the component rates",
        "target": previous["target"], "distribution": previous["distribution"], "thresholds": previous["thresholds"],
        "hyperparameters": previous["hyperparameters"], "calibration_layer": "NONE",
        "fitted_artifact": ARTIFACT, "fitted_artifact_sha256": sha(OUT / ARTIFACT),
        "scorer": "backend/nhl/scripts/freeze_nhl_points_goals_assists.py", "scorer_sha256": sha(Path(__file__).resolve()),
        "prospective_only": False, "production_replacement": True, "initial_shadow_prediction": "NOT_APPLICABLE",
        "fitted_revision": "GOALS_ASSISTS_2026_11_01",
        "revision_reason": "P2.39: goals and assists modelled separately with own goal and assist rates; beat the own-rate model at every line on the 2025 holdout",
        "missing_value_policy": "fill zero before scaling (frozen recipe); unknown attempts from SOG rate",
        "supersedes_model_identity_sha256": previous["model_identity_sha256"],
        "supersedes_fitted_artifact_sha256": previous["fitted_artifact_sha256"],
        "holdout_2025_vs_production_x1000": evals,
        "reproducibility": {"refit_runs": 2, "csv_parsing": "float_precision=round_trip",
                            "max_abs_diff_between_refits": float(np.abs(p1 - p2).max()),
                            "max_abs_diff_vs_frozen": float(np.abs(p1 - pf).max()), "holdout_rows": int(len(hold)),
                            "frozen_prediction_sha256": sha(pred_path),
                            "status": "PASS" if np.array_equal(p1, p2) and np.array_equal(p1, pf) else "FAIL"},
        "production_status": "NOT_IN_PRODUCTION",
    }
    contract["model_identity_sha256"] = identity(contract)
    (OUT / "shadow_contract.json").write_text(json.dumps(contract, indent=2) + "\n")
    print(json.dumps({k: contract[k] for k in ("model_identity_sha256", "fitted_artifact_sha256", "training_rows",
                                                "holdout_2025_vs_production_x1000", "reproducibility")}, indent=1))


if __name__ == "__main__":
    main()
