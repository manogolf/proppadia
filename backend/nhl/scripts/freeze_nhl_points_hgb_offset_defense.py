#!/usr/bin/env python3
"""Freeze the NHL Points OFFSET+DEFENSE model (P2.39, operator-approved 2026-11-01).

Structure: Poisson GLM on the standardized features, times the production HGB
recipe fitted as a multiplier on it (target y / m, weight m), so top scorers
can go above the tree ceiling. Features: the ten production inputs plus
``is_defense`` (``nhl.players`` position, as the SOG HGB).

Input: the month-end research matrices
(``research_nhl_points_hgb_month_end_refit.py``, research login, read-only).
The 2023-24 training frame is written once as CSV and every fit reads it back,
so two refits and the frozen artifact must agree exactly. Local files only.
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl.scripts import research_nhl_points_hgb_month_end_refit as research  # noqa: E402
from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/points_hgb_pp_corrected_refit/production_2026-11-01"
ARTIFACT = "nhl_points_count_hgb_offset_defense.joblib"
FEATURES = research.CANDIDATES["OFFSET+DEFENSE"]
KEYS = ["game_id", "player_id"]


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def identity(contract: dict) -> str:
    body = {k: v for k, v in contract.items() if k != "model_identity_sha256"}
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def fit(frame: pd.DataFrame) -> dict:
    m = research.fit_offset(frame, FEATURES)
    return {"structure": "GLM_TIMES_HGB_OFFSET", "feature_columns": FEATURES, "fill_zero": True,
            "scaler": m["scaler"], "glm": m["glm"], "model": m["offset"]}


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=False)
    data = joblib.load(research.OUT / "matrices.joblib")
    hist = data["historical"]
    cols = ["slate_date", *KEYS, "canonical_season", *FEATURES, "realized_points"]
    train_path = OUT / "training_frame_2023_2024.csv.gz"
    hist[hist.canonical_season.isin([2023, 2024])][cols].to_csv(train_path, index=False, float_format="%.17g")
    hold_path = OUT / "holdout_frame_2025.csv.gz"
    hist[hist.canonical_season.eq(2025)][cols].to_csv(hold_path, index=False, float_format="%.17g")
    train = pd.read_csv(train_path, float_precision="round_trip")
    hold = pd.read_csv(hold_path, float_precision="round_trip")
    first, second = fit(train), fit(train.copy())
    joblib.dump(first, OUT / ARTIFACT)
    frozen = joblib.load(OUT / ARTIFACT)
    p1, p2, pf = (scorer.score(hold, m).expected_points.to_numpy() for m in (first, second, frozen))
    pred_path = OUT / "frozen_holdout_predictions_2025.csv"
    hold[KEYS].assign(expected_points=pf).to_csv(pred_path, index=False, float_format="%.17g")
    previous = json.loads(scorer.PP_CORRECTED_CONTRACT_PATH.read_text())
    contract = {
        "schema_version": "NHL_POINTS_RESEARCH_SHADOW_CONTRACT_V1",
        "research_model_id": "NHL_POINTS_COUNT_HGB_V1",
        "authority": "PRODUCTION_OFFSET_DEFENSE_REFIT",
        "training_seasons": [2023, 2024],
        "training_rows": int(len(train)),
        "evaluation_season_confirmation": 2025,
        "training_source_frame_sha256": sha(train_path),
        "feature_contract": "NHL_POINTS_LEADER_CORE_FEATURES_V2_DEFENSE",
        "feature_columns": FEATURES,
        "history_contract": previous["history_contract"],
        "history_semantics": previous["history_semantics"],
        "target": previous["target"],
        "distribution": previous["distribution"],
        "thresholds": previous["thresholds"],
        "structure": "GLM_TIMES_HGB_OFFSET",
        "structure_semantics": ("expected points = PoissonRegressor(alpha=1e-4) on standardized features "
                                "x max(HGB(standardized features), 1e-8); HGB fitted to y / glm with weight glm"),
        "hyperparameters": previous["hyperparameters"],
        "glm_hyperparameters": {"alpha": 1e-4, "max_iter": 1000},
        "fitted_artifact": ARTIFACT,
        "fitted_artifact_sha256": sha(OUT / ARTIFACT),
        "scorer": "backend/nhl/scripts/freeze_nhl_points_hgb_offset_defense.py",
        "scorer_sha256": sha(Path(__file__).resolve()),
        "prospective_only": False,
        "production_replacement": True,
        "initial_shadow_prediction": "NOT_APPLICABLE",
        "fitted_revision": "OFFSET_DEFENSE_2026_11_01",
        "revision_reason": ("P2.39 month-end review: GLM offset lifts the HGB star ceiling; is_defense corrects "
                            "the defensemen level. Assist and games-played shot-share inputs tested and dropped."),
        "training_source_frame": "training_frame_2023_2024.csv.gz (built by research_nhl_points_hgb_month_end_refit.py)",
        "missing_value_policy": "fill zero before scaling (frozen V1 recipe); unknown attempts from SOG rate; is_defense required",
        "position_source": "nhl.players.position; D = defense, all else forward (sog_hgb_features.position_groups)",
        "supersedes_model_identity_sha256": previous["model_identity_sha256"],
        "supersedes_fitted_artifact_sha256": previous["fitted_artifact_sha256"],
        "reproducibility": {
            "refit_runs": 2, "csv_parsing": "float_precision=round_trip",
            "max_abs_diff_between_refits": float(np.abs(p1 - p2).max()),
            "max_abs_diff_vs_frozen": float(np.abs(p1 - pf).max()),
            "holdout_rows": int(len(hold)), "frozen_prediction_sha256": sha(pred_path),
            "status": "PASS" if np.array_equal(p1, p2) and np.array_equal(p1, pf) else "FAIL",
        },
    }
    contract["model_identity_sha256"] = identity(contract)
    (OUT / "shadow_contract.json").write_text(json.dumps(contract, indent=2) + "\n")
    print(json.dumps({k: contract[k] for k in ("model_identity_sha256", "fitted_artifact_sha256",
                                                "training_rows", "reproducibility")}, indent=1))


if __name__ == "__main__":
    main()
