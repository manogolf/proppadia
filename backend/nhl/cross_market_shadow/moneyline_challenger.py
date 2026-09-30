"""Isolated NHL Moneyline prior-strength V3 shadow challenger."""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

CHALLENGER_NAME = "NHL_MONEYLINE_SHOT_FINISHING_CHALLENGER_V3"
POLICY_VERSION = "NHL_MONEYLINE_PRIOR_STRENGTH_POLICY_V1"
FINISHING_WEIGHT = 0.50
MODEL_PATH = Path(__file__).resolve().parent / "moneyline_shot_finishing_v3.json"
CHALLENGER_FEATURES = [
    "diff_std_goal_diff_pg", "diff_r10_goal_diff_pg",
    "diff_prior_blended_shot_diff_pg", "diff_days_rest",
    "home_back_to_back", "away_back_to_back",
    "shrunk_prior_finishing_residual_gap",
]
FRANCHISE = {"ARI": "UTA", "UTA": "UTA"}


def _code(value: Any) -> str:
    return FRANCHISE.get(str(value).strip().upper(), str(value).strip().upper())


def _sha256(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def load_model(path: Path = MODEL_PATH) -> dict[str, Any]:
    model = json.loads(Path(path).read_text())
    if (model.get("model_version") != CHALLENGER_NAME
            or model.get("feature_policy_version") != POLICY_VERSION
            or model.get("feature_order") != CHALLENGER_FEATURES
            or model.get("finishing_weight") != FINISHING_WEIGHT):
        raise RuntimeError("MONEYLINE_CHALLENGER_MODEL_IDENTITY_MISMATCH")
    unsigned = dict(model)
    expected = unsigned.pop("parameter_content_sha256", None)
    actual_content = hashlib.sha256(_canonical_json(unsigned).encode()).hexdigest()
    if expected != actual_content:
        raise RuntimeError("MONEYLINE_CHALLENGER_PARAMETER_HASH_MISMATCH")
    model["parameter_file_sha256"] = _sha256(Path(path))
    return model


def prior_strength_states(prior_games: pd.DataFrame) -> tuple[dict[str, dict[str, float]], float]:
    """Derive regular-season team shot and finishing priors from qualified games."""
    prior = prior_games.copy()
    game_type_col = "game_type_code" if "game_type_code" in prior else "game_type"
    prior[game_type_col] = pd.to_numeric(prior[game_type_col], errors="coerce")
    prior = prior[prior[game_type_col].eq(2)].copy()
    if "database_game_status" in prior:
        prior = prior[prior.database_game_status.astype(str).str.lower().isin(
            {"final", "off", "completed"})]
    elif "game_status" in prior:
        prior = prior[prior.game_status.astype(str).str.upper().isin(
            {"FINAL", "OFF", "COMPLETED"})]
    required = {"home_team_code", "away_team_code", "home_shots", "away_shots",
                "final_home_goals", "final_away_goals"}
    if required - set(prior):
        raise ValueError("MONEYLINE_PRIOR_SOURCE_SCHEMA_INCOMPLETE")
    for col in ["home_shots", "away_shots", "final_home_goals", "final_away_goals"]:
        prior[col] = pd.to_numeric(prior[col], errors="coerce")
    prior = prior.dropna(subset=list(required - {"home_team_code", "away_team_code"}))
    if prior.game_id.duplicated().any() or prior.empty:
        raise ValueError("MONEYLINE_PRIOR_SOURCE_GRAIN_INVALID")
    league_rate = float((prior.final_home_goals.sum() + prior.final_away_goals.sum()) /
                        (prior.home_shots.sum() + prior.away_shots.sum()))
    sides = []
    for side, team_col, goals_for, goals_against, sf, sa in (
        ("home", "home_team_code", "final_home_goals", "final_away_goals", "home_shots", "away_shots"),
        ("away", "away_team_code", "final_away_goals", "final_home_goals", "away_shots", "home_shots"),
    ):
        temp = pd.DataFrame({
            "team": prior[team_col].map(_code), "gf": prior[goals_for], "ga": prior[goals_against],
            "sf": prior[sf], "sa": prior[sa],
        })
        sides.append(temp)
    all_sides = pd.concat(sides, ignore_index=True)
    states = {}
    for team, group in all_sides.groupby("team"):
        count = len(group)
        states[team] = {
            "games": float(count),
            "shot_diff_pg": float((group.sf - group.sa).mean()),
            "finishing_residual_pg": float((group.gf - league_rate * group.sf).sum() / count),
        }
    return states, league_rate


def shot_prior_weight(games: int) -> float:
    if games <= 3:
        return 1.0
    if games <= 5:
        return 0.75
    if games <= 10:
        return 0.50
    return 0.0


def build_challenger_predictions(
    v2_predictions: pd.DataFrame,
    shot_prior_predictions: pd.DataFrame,
    shot_provenance: pd.DataFrame,
    prior_games: pd.DataFrame,
    *,
    prior_team_source_sha256: str,
    prior_outcome_source_sha256: str,
    schedule_source_sha256: str,
    history_source_sha256: str,
    odds_source_sha256: str,
    model_path: Path = MODEL_PATH,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Score challenger alongside immutable V2 using same slate and market capture."""
    model = load_model(model_path)
    states, league_rate = prior_strength_states(prior_games)
    v2 = v2_predictions.set_index("game_id", drop=False)
    shot = shot_prior_predictions.set_index("game_id", drop=False)
    provenance = shot_provenance.set_index("game_id", drop=False)
    means, scales, medians = (model["standardization_means"], model["standardization_scales"],
                              model["imputation_medians"])
    coefficients = model["coefficients"]
    rows, prov_rows = [], []
    for game_id, base in v2.iterrows():
        prior_row = provenance.loc[game_id]
        shot_row = shot.loc[game_id]
        home_code, away_code = _code(base.home_team), _code(base.away_team)
        home_prior, away_prior = states.get(home_code), states.get(away_code)
        raw_shot = float(base.diff_std_shot_diff_pg) if pd.notna(base.diff_std_shot_diff_pg) else np.nan
        blended_shot = float(shot_row.diff_std_shot_diff_pg)
        raw_finish_gap = (
            home_prior["finishing_residual_pg"] - away_prior["finishing_residual_pg"]
            if home_prior is not None and away_prior is not None else np.nan
        )
        shrunk_finish_gap = FINISHING_WEIGHT * raw_finish_gap if pd.notna(raw_finish_gap) else np.nan
        raw_features = {
            "diff_std_goal_diff_pg": base.diff_std_goal_diff_pg,
            "diff_r10_goal_diff_pg": base.diff_r10_goal_diff_pg,
            "diff_prior_blended_shot_diff_pg": blended_shot,
            "diff_days_rest": base.diff_days_rest,
            "home_back_to_back": base.home_back_to_back,
            "away_back_to_back": base.away_back_to_back,
            "shrunk_prior_finishing_residual_gap": shrunk_finish_gap,
        }
        imputed, scaled = {}, {}
        for feature in CHALLENGER_FEATURES:
            value = raw_features[feature]
            imputed[feature] = float(medians[feature]) if pd.isna(value) else float(value)
            scaled[feature] = (imputed[feature] - float(means[feature])) / float(scales[feature])
        logit = float(model["intercept"] + sum(scaled[f] * coefficients[f] for f in CHALLENGER_FEATURES))
        prob = float(1 / (1 + math.exp(-max(min(logit, 709), -709))))
        favored = base.home_team if prob >= .5 else base.away_team
        rows.append({
            "canonical_season": int(base.canonical_season), "slate_date": base.slate_date,
            "game_id": int(game_id), "game_date": base.game_date,
            "scheduled_start_time_utc": str(base.scheduled_start_time_utc),
            "home_team_id": int(base.home_team_id), "home_team": base.home_team,
            "away_team_id": int(base.away_team_id), "away_team": base.away_team,
            **raw_features,
            **{f"imputed__{k}": value for k, value in imputed.items()},
            **{f"scaled__{k}": value for k, value in scaled.items()},
            "challenger_logit": logit, "challenger_home_win_probability": prob,
            "challenger_away_win_probability": 1 - prob, "challenger_favored_team": favored,
            "v2_home_win_probability": float(base.v2_home_win_probability),
            "probability_delta_challenger_minus_v2": prob - float(base.v2_home_win_probability),
            "side_changed": favored != base.model_favored_team,
            "model_version": CHALLENGER_NAME, "model_parameter_sha256": model["parameter_file_sha256"],
            "feature_policy_version": POLICY_VERSION, "feature_policy_sha256": model["policy_sha256"],
            "finishing_weight": FINISHING_WEIGHT, "source_prior_season": int(model["prior_season"]),
            "wager_recommendation": "NONE_SHADOW_ONLY", "production_write": False,
        })
        prov_rows.append({
            "canonical_season": int(base.canonical_season), "slate_date": base.slate_date,
            "game_id": int(game_id), "scheduled_start_time_utc": str(base.scheduled_start_time_utc),
            "prior_season": int(model["prior_season"]), "home_team": home_code, "away_team": away_code,
            "prior_home_full_shot_diff_pg": home_prior["shot_diff_pg"] if home_prior else np.nan,
            "prior_away_full_shot_diff_pg": away_prior["shot_diff_pg"] if away_prior else np.nan,
            "current_home_shot_diff_pg": prior_row.current_home_shot_diff_pg,
            "current_away_shot_diff_pg": prior_row.current_away_shot_diff_pg,
            "current_home_games": int(prior_row.current_home_games),
            "current_away_games": int(prior_row.current_away_games),
            "home_shot_prior_weight": float(prior_row.home_prior_weight),
            "away_shot_prior_weight": float(prior_row.away_prior_weight),
            "blended_home_shot_diff_pg": float(prior_row.blended_home_shot_diff_pg),
            "blended_away_shot_diff_pg": float(prior_row.blended_away_shot_diff_pg),
            "v2_shot_feature_control": raw_shot, "challenger_shot_gap": blended_shot,
            "prior_home_finishing_residual": home_prior["finishing_residual_pg"] if home_prior else np.nan,
            "prior_away_finishing_residual": away_prior["finishing_residual_pg"] if away_prior else np.nan,
            "raw_finishing_residual_gap": raw_finish_gap,
            "finishing_weight": FINISHING_WEIGHT, "shrunk_finishing_residual_gap": shrunk_finish_gap,
            "league_goals_per_shot": league_rate,
            "prior_team_source_sha256": prior_team_source_sha256,
            "prior_outcome_source_sha256": prior_outcome_source_sha256,
            "schedule_source_sha256": schedule_source_sha256,
            "history_source_sha256": history_source_sha256,
            "market_snapshot_sha256": odds_source_sha256,
            "model_parameter_sha256": model["parameter_file_sha256"],
            "feature_policy_version": POLICY_VERSION,
            "feature_vector_json": _canonical_json(raw_features),
        })
    output = pd.DataFrame(rows)
    provenance_output = pd.DataFrame(prov_rows)
    if not output.challenger_home_win_probability.between(0, 1, inclusive="neither").all():
        raise RuntimeError("MONEYLINE_CHALLENGER_PROBABILITY_INVALID")
    return output, provenance_output, make_ledger(output, pd.DataFrame())


def make_ledger(predictions: pd.DataFrame, market_references: pd.DataFrame) -> pd.DataFrame:
    """Create the moneyline shadow ledger using this run's normalized market rows."""
    ledger = predictions.copy()
    if len(market_references):
        cols = [c for c in ["game_id", "side_orientation", "american_price", "decimal_price",
                            "no_vig_probability", "source_update_timestamp_utc", "sportsbook_key",
                            "qualification_status"] if c in market_references]
        prices = market_references[market_references.qualification_status.eq("PREGAME_QUALIFIED")
                                  & market_references.market_type.eq("FULL_GAME_MONEYLINE")]
        ledger = ledger.merge(prices[cols], on="game_id", how="left", validate="one_to_many")
    if "observation_timestamp_utc" not in ledger:
        ledger["observation_timestamp_utc"] = pd.NA
    ledger["financial_claim"] = "NONE_SHADOW_ONLY"
    return ledger
