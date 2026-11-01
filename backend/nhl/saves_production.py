"""Production NHL Saves scorer selection (P2.41 cutover, operator-approved 2026-11-01).

``PRODUCTION`` picks the Saves model for both scoring paths: the daily run's
Saves lane (``scripts/score_nhl_saves_with_lineage.py``) and the prediction-only
packages that grading reads (``prediction_only.core.publish_saves``).

- ``SHOTS_X_SAVE_RATE_V1`` (production from 2026-11-01): expected shots against
  x the goalie's shrunk save rate, negative binomial counts, no calibration
  layer (``saves_refit_model.py``; frozen under
  ``artifacts/analysis/nhl/saves_refit_2026-11-01/frozen_2026-11-01/``).
- ``PHOENIX_V2``: the previous Poisson GLM with its isotonic 24.5 map
  (``saves_shadow.core.score_frozen``). One-step rollback: set
  ``PRODUCTION = PHOENIX_V2``.

Live history for the refit is read strictly before the slate date from the
database (regular-season skater and goalie logs, the slate season and the one
before) in one read-only snapshot. Lines are unchanged (18.5-30.5); the line
ladder itself has no research behind it (operator, 2026-11-01).
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import psycopg

from backend.nhl import saves_refit_model as srm

ROOT = Path(__file__).resolve().parents[2]
SHOTS_X_SAVE_RATE_V1 = "SHOTS_X_SAVE_RATE_V1"
PHOENIX_V2 = "PHOENIX_V2"
PRODUCTION = SHOTS_X_SAVE_RATE_V1
FROZEN_DIR = ROOT / "artifacts/analysis/nhl/saves_refit_2026-11-01/frozen_2026-11-01"
CONTRACT_PATH = FROZEN_DIR / "contract.json"
LABELS = {SHOTS_X_SAVE_RATE_V1: {"model": srm.MODEL_ID, "model_family": "saves_refit",
                                 "model_version": "shots_x_save_rate_v1"},
          PHOENIX_V2: {"model": "NHL_GOALIE_SAVES_PHOENIX_V2", "model_family": "phoenix",
                       "model_version": "phoenix_v2"}}


def sha256_file(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def labels() -> dict:
    return dict(LABELS[PRODUCTION])


def load_refit() -> tuple[dict, dict]:
    """Frozen refit model and contract, after checking the artifact and module hashes."""
    contract = json.loads(CONTRACT_PATH.read_text())
    artifact = FROZEN_DIR / contract["fitted_artifact"]
    if sha256_file(artifact) != contract["fitted_artifact_sha256"]:
        raise RuntimeError("SAVES_REFIT_ARTIFACT_HASH_DRIFT")
    body = {k: v for k, v in contract.items() if k != "model_identity_sha256"}
    if hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest() \
            != contract["model_identity_sha256"]:
        raise RuntimeError("SAVES_REFIT_CONTRACT_IDENTITY_DRIFT")
    model = joblib.load(artifact)
    if model.get("calibration_layer") != "NONE" or model.get("model_id") != srm.MODEL_ID:
        raise RuntimeError("SAVES_REFIT_ARTIFACT_CONTRACT_MISMATCH")
    return model, contract


def components() -> list[tuple[str, Path]]:
    """Files whose hashes form the fitted-model identity of a refit prediction."""
    return [("scorer_selection", Path(__file__)), ("feature_and_scoring_module", ROOT / "backend/nhl/saves_refit_model.py"),
            ("frozen_contract", CONTRACT_PATH), ("frozen_model", FROZEN_DIR / json.loads(CONTRACT_PATH.read_text())["fitted_artifact"])]


def history(dsn: str, slate_date: str, season: int) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    with psycopg.connect(dsn, prepare_threshold=None) as c, c.cursor() as cur:
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        cur.execute("SELECT transaction_timestamp()")
        snapshot = cur.fetchone()[0].isoformat()
        cur.execute("""SELECT g.season, g.game_date, l.game_id::bigint, l.team_id, g.home_team_id, g.away_team_id,
                              l.shots_on_goal
                       FROM nhl.skater_game_logs_raw l JOIN nhl.games g USING(game_id)
                       WHERE g.season IN (%s, %s) AND substring(g.game_id::text,5,2) = '02' AND g.game_date < %s""",
                    (season - 1, season, slate_date))
        logs = pd.DataFrame(cur.fetchall(), columns=["season", "game_date", "game_id", "team_id", "home_team_id",
                                                     "away_team_id", "shots_on_goal"])
        cur.execute("""SELECT g.season, l.game_id::bigint, l.player_id::bigint, l.team_id, g.game_date,
                              l.toi_minutes, l.saves, l.shots_faced, l.goals_allowed
                       FROM nhl.goalie_game_logs_raw l JOIN nhl.games g USING(game_id)
                       WHERE g.season IN (%s, %s) AND substring(g.game_id::text,5,2) = '02' AND g.game_date < %s""",
                    (season - 1, season, slate_date))
        goalies = pd.DataFrame(cur.fetchall(), columns=["season", "game_id", "player_id", "team_id", "game_date", "toi",
                                                        "saves", "shots", "goals_allowed"])
    if logs.empty or goalies.empty:
        raise RuntimeError("SAVES_REFIT_HISTORY_UNAVAILABLE")
    logs["shots_on_goal"] = pd.to_numeric(logs.shots_on_goal, errors="coerce")
    for col in ("toi", "saves", "shots", "goals_allowed"):
        goalies[col] = pd.to_numeric(goalies[col], errors="coerce")
    # Shots faced from saves + goals allowed where only those are stored (the training loader's rule).
    goalies["shots"] = goalies.shots.fillna(goalies.saves + goalies.goals_allowed).where(goalies.toi > 0)
    return logs, goalies, snapshot


def score_refit(targets: pd.DataFrame, *, slate_date: str, season: int, dsn: str,
                model: dict | None = None) -> tuple[pd.DataFrame, dict]:
    """Wide refit predictions for ``targets`` (player_id, game_id, team_id, home_team_id,
    away_team_id): expected_saves and p_over_<line> per goalie-game."""
    model = model or load_refit()[0]
    targets = targets[["player_id", "game_id", "team_id", "home_team_id", "away_team_id"]].astype("int64")
    if targets.duplicated(["player_id", "game_id"]).any():
        raise RuntimeError("SAVES_REFIT_TARGET_DUPLICATE")
    logs, goalies, snapshot = history(dsn, slate_date, season)
    games = targets[["game_id", "home_team_id", "away_team_id"]].drop_duplicates().assign(game_date=slate_date)
    g = srm.team_features(srm.add_targets(srm.team_games(logs), games, season))
    pending = targets[["player_id", "game_id"]].assign(season=season, game_date=slate_date, toi=np.nan,
                                                       saves=np.nan, shots=np.nan)
    rates = srm.save_rates(pd.concat([goalies, pending], ignore_index=True))
    rows = targets.merge(rates[["player_id", "game_id", "save_rate", "goalie_shots_prior"]],
                         on=["player_id", "game_id"], how="left", validate="one_to_one").merge(
        g.drop(columns=["season", "game_date"]), on=["game_id", "team_id"], how="left", validate="many_to_one")
    needed = ["save_rate", "league_spg_to_date", *srm.SHOT_FEATURES]
    if rows[needed].isna().any().any():
        raise RuntimeError("SAVES_REFIT_FEATURE_MISSING")
    rows["expected_saves"] = srm.expected_saves(model, rows)
    if not np.isfinite(rows.expected_saves).all() or (rows.expected_saves <= 0).any():
        raise RuntimeError("SAVES_REFIT_INVALID_EXPECTATION")
    wide = pd.concat([rows.reset_index(drop=True), srm.prob_over(model, rows.expected_saves.to_numpy())], axis=1)
    return wide, {"history_snapshot_utc": snapshot, "skater_log_rows": int(len(logs)), "goalie_log_rows": int(len(goalies))}


def long_predictions(wide: pd.DataFrame, lines, start_prob: float | None) -> pd.DataFrame:
    """The prediction-only package's long rows (same columns as ``score_frozen``)."""
    label = labels()
    rows = []
    for r in wide.itertuples(index=False):
        for line in lines:
            p = float(getattr(r, f"p_over_{str(float(line)).replace('.', '_')}"))
            rows.append({"game_id": int(r.game_id), "goalie_id": int(r.player_id), "line": float(line),
                         "expected_saves": float(r.expected_saves), "raw_prob_over": p, "prob_over": p,
                         "prob_under": 1 - p, "start_prob_operational_input": start_prob,
                         "model": label["model"], "model_version": label["model_version"]})
    return pd.DataFrame(rows)
