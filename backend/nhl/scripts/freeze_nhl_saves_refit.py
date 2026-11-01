#!/usr/bin/env python3
"""Freeze the NHL Saves refit NHL_SAVES_SHOTS_X_SAVE_RATE_V1 (P2.41, operator-approved 2026-11-01).

Builds the 2023-24 training frames with ``backend/nhl/saves_refit_model.py``
from read-only history (skater logs bound to canonical teams by
``run_nhl_points_hgb_pp_corrected_refit.pull_history``; goalie logs with
2023-24 starters and teams from the retained boxscores, as in
``research_nhl_saves_refit.py``; 2022-23 team totals from the NHL stats API).
Writes the frames once as CSV and fits from them twice; the frozen artifact,
both refits and the 2025 holdout predictions must agree exactly. Also writes
2025 and 2026 holdout scores. No calibration layer. Nothing changes production.
"""
from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from scipy.stats import nbinom

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl import saves_refit_model as srm  # noqa: E402
from backend.nhl.scripts import research_nhl_saves_refit as research  # noqa: E402
from backend.nhl.scripts.run_nhl_points_hgb_pp_corrected_refit import pull_history  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/saves_refit_2026-11-01/frozen_2026-11-01"
TEAM_2022 = (ROOT / "artifacts/analysis/nhl/points_official_outcome_restoration/2026-11-01_skater_season_totals"
             / "team_totals_season=20222023.csv")
ARTIFACT = "nhl_saves_shots_x_save_rate_v1.joblib"


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def identity(contract: dict) -> str:
    body = {k: v for k, v in contract.items() if k != "model_identity_sha256"}
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def extra_prev() -> pd.DataFrame:
    api = pd.read_csv(TEAM_2022)
    return pd.DataFrame({"team_id": api.team_id, "season": 2022, "games": api.games,
                         "sf": api.shots_for, "sa": api.shots_against})


def frames() -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    logs, audit = pull_history()
    logs = logs.rename(columns={"canonical_season": "season"})
    g = srm.team_features(srm.team_games(logs), extra_prev())
    goalies = research.goalie_logs().rename(columns={})
    rates = srm.save_rates(goalies)
    starters = rates[rates.start_flag.eq(True)].merge(
        g.drop(columns=["season", "game_date"]), on=["game_id", "team_id"], how="inner")
    return g, starters, {"login": audit["login"], "snapshot_utc": audit["transaction_timestamp_utc"]}


def nb_ll(y: np.ndarray, mu: np.ndarray, ratio: float) -> float:
    p = 1.0 / ratio
    return float(-nbinom.logpmf(y, mu * p / (1 - p), p).mean())


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=False)
    g, starters, audit = frames()
    team_cols = ["season", "game_date", "game_id", "team_id", "opp_id", "sa", "league_spg_to_date", *srm.SHOT_FEATURES]
    st_cols = ["season", "game_date", "game_id", "player_id", "team_id", "saves", "save_rate", "goalie_shots_prior",
               "league_spg_to_date", *srm.SHOT_FEATURES]
    team_path, st_path = OUT / "training_team_games_2023_2024.csv.gz", OUT / "training_starters_2023_2024.csv.gz"
    g[g.season.isin([2023, 2024]) & g.sa.notna()][team_cols].to_csv(team_path, index=False, float_format="%.17g")
    starters[starters.season.isin([2023, 2024])][st_cols].to_csv(st_path, index=False, float_format="%.17g")
    hold_path = OUT / "holdout_starters_2025_2026.csv.gz"
    starters[starters.season.isin([2025, 2026])][st_cols].to_csv(hold_path, index=False, float_format="%.17g")
    tr, ts, ho = (pd.read_csv(p, float_precision="round_trip") for p in (team_path, st_path, hold_path))
    first, second = srm.fit(tr, ts), srm.fit(tr.copy(), ts.copy())
    joblib.dump(first, OUT / ARTIFACT)
    frozen = joblib.load(OUT / ARTIFACT)
    m1, m2, mf = (srm.expected_saves(m, ho) for m in (first, second, frozen))
    pred_path = OUT / "frozen_holdout_expected_saves.csv"
    ho[["season", "game_id", "player_id"]].assign(expected_saves=mf).to_csv(pred_path, index=False, float_format="%.17g")
    evals = {}
    for s in (2025, 2026):
        m = ho.season.eq(s).to_numpy()
        y = ho.saves.to_numpy(float)[m]
        evals[s] = {"starters": int(m.sum()), "corr": float(np.corrcoef(mf[m], y)[0, 1]),
                    "mae": float(np.abs(mf[m] - y).mean()), "negbin_ll": nb_ll(y, mf[m], frozen["variance_ratio"]),
                    "flat_negbin_ll": nb_ll(y, np.full(m.sum(), ts.saves.mean()), frozen["variance_ratio"]),
                    "mean_expected": float(mf[m].mean()), "mean_actual": float(y.mean())}
    contract = {
        "schema_version": "NHL_SAVES_REFIT_CONTRACT_V1", "model_id": srm.MODEL_ID,
        "structure": "expected shots against (Poisson GLM, league-level offset) x shrunk goalie save rate x scale; negative binomial counts",
        "target": "official saves of the named starting goalie (P(saves > line | named goalie starts))",
        "training_seasons": [2023, 2024], "training_team_games": int(len(tr)), "training_starters": int(len(ts)),
        "training_team_games_sha256": sha(team_path), "training_starters_sha256": sha(st_path),
        "shot_features": srm.SHOT_FEATURES, "rate_columns_relative_to_league": list(srm.RATE_COLS),
        "k_team_games": srm.K_TEAM, "k_save_shots": srm.K_SAVE, "scale": frozen["scale"],
        "variance_ratio": frozen["variance_ratio"], "lines": list(srm.LINES), "calibration_layer": "NONE",
        "feature_module": "backend/nhl/saves_refit_model.py", "feature_module_sha256": sha(ROOT / "backend/nhl/saves_refit_model.py"),
        "freeze_script": "backend/nhl/scripts/freeze_nhl_saves_refit.py", "freeze_script_sha256": sha(Path(__file__).resolve()),
        "fitted_artifact": ARTIFACT, "fitted_artifact_sha256": sha(OUT / ARTIFACT),
        "starter_source": "boxscore starter flag (2023-24 retained boxscores; goalie_game_logs_raw.start_flag 2025+)",
        "history_sources": {"skater_logs": "nhl.skater_game_logs_raw (research login, canonical team binding)",
                            "goalie_logs": "nhl.goalie_game_logs_raw (2025 completed by the P2.41 backfill)",
                            "team_2022_23": str(TEAM_2022.relative_to(ROOT))},
        "database_login": audit["login"], "snapshot_utc": audit["snapshot_utc"],
        "holdout_evaluation": evals,
        "reproducibility": {"refit_runs": 2, "csv_parsing": "float_precision=round_trip",
                            "max_abs_diff_between_refits": float(np.abs(m1 - m2).max()),
                            "max_abs_diff_vs_frozen": float(np.abs(m1 - mf).max()),
                            "holdout_rows": int(len(ho)), "frozen_prediction_sha256": sha(pred_path),
                            "status": "PASS" if np.array_equal(m1, m2) and np.array_equal(m1, mf) else "FAIL"},
        "production_status": "NOT_IN_PRODUCTION",
        "supersedes_on_approval": "NHL_GOALIE_SAVES_PHOENIX_V2 (models/latest/goalie_saves; P2.41 review)",
    }
    contract["model_identity_sha256"] = identity(contract)
    (OUT / "contract.json").write_text(json.dumps(contract, indent=2, default=str) + "\n")
    print(json.dumps({k: contract[k] for k in ("model_identity_sha256", "fitted_artifact_sha256", "training_starters",
                                                "scale", "variance_ratio", "holdout_evaluation", "reproducibility")},
                     indent=1, default=str))


if __name__ == "__main__":
    main()
