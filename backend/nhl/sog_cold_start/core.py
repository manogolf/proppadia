"""Frozen, odds-independent NHL SOG cold-start feature and prediction lane."""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

CONTRACT_PATH = Path(__file__).with_name("frozen_contract_v1.json")
CONTRACT_VERSION = "NHL_SOG_COLD_START_PREDICTION_V1"
LINES = (1.5, 2.5, 3.5)
PHASES = {"MIDDAY", "FINAL_PREGAME"}
FORBIDDEN_RETROSPECTIVE_THROUGH = "2026-09-19"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def digest(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode()).hexdigest()


def load_contract(path: Path = CONTRACT_PATH) -> dict[str, Any]:
    contract = json.loads(path.read_text())
    if contract.get("contract_version") != CONTRACT_VERSION:
        raise RuntimeError("COLD_START_CONTRACT_VERSION_MISMATCH")
    return contract


def poisson_tail(lam: float, line: float) -> float:
    if not math.isfinite(lam) or lam < 0 or line not in LINES:
        raise ValueError("invalid Poisson input")
    threshold = int(line + 0.5)
    cdf = sum(math.exp(-lam) * lam**k / math.factorial(k) for k in range(threshold))
    return min(1.0, max(0.0, 1.0 - cdf))


def _number(value: Any) -> float | None:
    value = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    return None if pd.isna(value) or not math.isfinite(float(value)) else float(value)


def _profile(row: pd.Series, contract: dict[str, Any]) -> tuple[float | None, float | None, str, str]:
    """Return selected SOG/60, TOI minutes, class, transition state.

    Input rows are immutable, precomputed strict-prior summaries. The function never
    consults a same-game outcome and never replaces missing history with zero.
    """
    prior_minutes = _number(row.get("prior_minutes")) or 0.0
    prior_rate = _number(row.get("prior_sog_per60"))
    prior_toi = _number(row.get("prior_toi_per_game"))
    older_minutes = _number(row.get("older_minutes")) or 0.0
    older_rate = _number(row.get("older_sog_per60"))
    older_toi = _number(row.get("older_toi_per_game"))
    position_rate = _number(row.get("position_sog_per60"))
    position_toi = _number(row.get("position_toi_per_game"))
    pos_equiv = float(contract["minimum_history"]["position_prior_equivalent_minutes"])
    old_weight = float(contract["weights"]["older_season"])

    weighted_minutes = prior_minutes + old_weight * older_minutes
    if prior_rate is not None or older_rate is not None:
        rate_num = (prior_rate or 0.0) * prior_minutes
        if older_rate is not None:
            rate_num += older_rate * older_minutes * old_weight
        base_rate = rate_num / weighted_minutes if weighted_minutes else None
        toi_num = (prior_toi or 0.0) * prior_minutes
        if older_toi is not None:
            toi_num += older_toi * older_minutes * old_weight
        base_toi = toi_num / weighted_minutes if weighted_minutes else None
    else:
        base_rate = base_toi = None

    if base_rate is not None and base_toi is not None and position_rate is not None and position_toi is not None:
        rate = (base_rate * weighted_minutes + position_rate * pos_equiv) / (weighted_minutes + pos_equiv)
        toi = (base_toi * weighted_minutes + position_toi * pos_equiv) / (weighted_minutes + pos_equiv)
    elif base_rate is not None and base_toi is not None:
        rate, toi = base_rate, base_toi
    elif position_rate is not None and position_toi is not None:
        rate, toi = position_rate, position_toi
    else:
        return None, None, "UNREPRESENTABLE", "EXCLUDED_MISSING_CERTIFIED_HISTORY"

    if prior_minutes >= float(contract["minimum_history"]["adequate_prior_minutes"]):
        player_class = "RETURNING_ADEQUATE_HISTORY"
    elif prior_minutes > 0:
        player_class = "RETURNING_SPARSE_HISTORY"
    elif older_minutes > 0:
        player_class = "RETURNING_AFTER_ABSENCE_OR_OLD_HISTORY"
    else:
        player_class = "ROOKIE_POSITION_PRIOR"
    if bool(row.get("team_changed", False)):
        player_class += "_TEAM_CHANGE"

    return rate, toi, player_class, "COLD_START_SELECTED_PRIOR_ONLY"


def _arm_profiles(row: pd.Series, contract: dict[str, Any]) -> tuple[dict[str, tuple[float,float,str]], str]:
    d_rate,d_toi,player_class,_=_profile(row,contract)
    if d_rate is None or d_toi is None:return {},player_class
    arms={"D_PLAYER_ROLE_HIERARCHICAL":(d_rate,d_toi,"COLD_START_SELECTED_PRIOR_ONLY")}
    prior_rate=_number(row.get("prior_sog_per60"));prior_toi=_number(row.get("prior_toi_per_game"))
    if prior_rate is not None and prior_toi is not None:arms["A_PRIOR_SEASON_CARRY_FORWARD"]=(prior_rate,prior_toi,"PRIOR_SEASON_ONLY")
    recent_rate=_number(row.get("prior_recency_sog_per60"));recent_toi=_number(row.get("prior_recency_toi_per_game"))
    if recent_rate is not None and recent_toi is not None:arms["B_PRIOR_SEASON_RECENCY_WEIGHTED"]=(recent_rate,recent_toi,"PRIOR_SEASON_FINAL20_DECAY_0.9")
    prior_minutes=_number(row.get("prior_minutes")) or 0.;older_minutes=_number(row.get("older_minutes")) or 0.;older_rate=_number(row.get("older_sog_per60"));older_toi=_number(row.get("older_toi_per_game"));league_rate=_number(row.get("league_sog_per60"));league_toi=_number(row.get("league_toi_per_game"));total=prior_minutes+.5*older_minutes
    if total>0 and league_rate is not None and league_toi is not None:
        br=((prior_rate or 0)*prior_minutes+(older_rate or 0)*older_minutes*.5)/total;bt=((prior_toi or 0)*prior_minutes+(older_toi or 0)*older_minutes*.5)/total
        arms["C_MULTISEASON_SHRUNK_PLAYER"]=((br*total+league_rate*300)/(total+300),(bt*total+league_toi*300)/(total+300),"MULTISEASON_LEAGUE_SHRINK_300_MIN")
    pg=_number(row.get("current_preseason_games")) or 0.;pr=_number(row.get("current_preseason_sog_per60"));pt=_number(row.get("current_preseason_toi_per_game"))
    if pg>0 and pr is not None and pt is not None:
        w=min(pg/float(contract["weights"]["current_preseason_equivalent_games"]),1.);arms["F_CURRENT_PRESEASON_UPDATE"]=((1-w)*d_rate+w*pr,(1-w)*d_toi+w*pt,f"STRICT_PRIOR_PRESEASON_BLEND_{w:.3f}_UNQUALIFIED")
    cg=_number(row.get("current_regular_games")) or 0.;cr=_number(row.get("current_regular_sog_per60"));ct=_number(row.get("current_regular_toi_per_game"))
    if cg>0 and cr is not None and ct is not None:
        w=min(cg/float(contract["minimum_history"]["current_transition_appearances"]),1.);arms["G_COLD_START_TO_CURRENT_SEASON_BLEND"]=((1-w)*d_rate+w*cr,(1-w)*d_toi+w*ct,f"CURRENT_SEASON_BLEND_{w:.3f}_CHALLENGER")
    else:arms["G_COLD_START_TO_CURRENT_SEASON_BLEND"]=(d_rate,d_toi,"CURRENT_SEASON_BLEND_0.000_CHALLENGER")
    return arms,player_class


def build_predictions(
    features: pd.DataFrame,
    *,
    slate_date: str,
    phase: str,
    prediction_timestamp_utc: str,
    input_cutoff_utc: str,
    contract_path: Path = CONTRACT_PATH,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if slate_date <= FORBIDDEN_RETROSPECTIVE_THROUGH:
        raise RuntimeError("SEPTEMBER_19_RETROSPECTIVE_PREDICTION_FORBIDDEN")
    if phase not in PHASES:
        raise ValueError("INVALID_PREDICTION_PHASE")
    contract = load_contract(contract_path)
    required = {
        "canonical_season", "slate_date", "game_id", "player_id", "player_name",
        "team_id", "opponent_id", "position", "roster_status", "lineup_status",
        "scheduled_start_time_utc", "feature_cutoff_utc",
    }
    missing = required - set(features)
    if missing:
        raise ValueError(f"COLD_START_FEATURE_SCHEMA_INCOMPLETE:{sorted(missing)}")
    if features.duplicated(["game_id", "player_id"]).any():
        raise RuntimeError("DUPLICATE_PLAYER_GAME_FEATURE_IDENTITY")
    if not features.slate_date.astype(str).eq(slate_date).all():
        raise RuntimeError("SLATE_DATE_MISMATCH")
    pred_ts = pd.Timestamp(prediction_timestamp_utc)
    cutoff = pd.Timestamp(input_cutoff_utc)
    starts = pd.to_datetime(features.scheduled_start_time_utc, utc=True, errors="coerce")
    feature_cutoffs = pd.to_datetime(features.feature_cutoff_utc, utc=True, errors="coerce")
    if pred_ts.tzinfo is None or cutoff.tzinfo is None:
        raise ValueError("TIMESTAMP_MUST_BE_TIMEZONE_AWARE")
    if starts.isna().any() or feature_cutoffs.isna().any() or (pred_ts >= starts).any() or (cutoff > pred_ts):
        raise RuntimeError("PREGAME_TIMING_GATE_FAILED")
    if (feature_cutoffs > cutoff).any():
        raise RuntimeError("POST_CUTOFF_FEATURE_INPUT")

    prediction_rows: list[dict[str, Any]] = []
    input_rows: list[dict[str, Any]] = []
    exclusion_rows: list[dict[str, Any]] = []
    contract_hash = sha256_file(contract_path)
    allowed_roster = {"ACTIVE", "ACTIVE_ROSTER"}
    allowed_lineup = set(contract["lineup_policy"]["included"])
    for _, row in features.sort_values(["game_id", "player_id"]).iterrows():
        base = {k: row.get(k) for k in ["canonical_season", "slate_date", "game_id", "player_id", "player_name", "team_id", "opponent_id", "position", "roster_status", "lineup_status", "scheduled_start_time_utc", "feature_cutoff_utc"]}
        reason = None
        if str(row.roster_status) not in allowed_roster:
            reason = f"ROSTER_{row.roster_status}"
        elif str(row.lineup_status) not in allowed_lineup:
            reason = f"LINEUP_{row.lineup_status}"
        elif not str(row.position).strip() or str(row.position).upper() in {"NAN", "NONE", "UNK", "UNKNOWN", "G"}:
            reason = "CERTIFIED_SKATER_POSITION_UNAVAILABLE"
        arms, player_class = _arm_profiles(row, contract)
        if reason is None and not arms:
            reason = "CERTIFIED_COLD_START_HISTORY_UNAVAILABLE"
        if reason is not None:
            exclusion_rows.append({**base, "phase": phase, "exclusion_reason": reason, "contract_version": CONTRACT_VERSION})
            continue
        for arm,(rate,toi,transition) in arms.items():
          if rate < 0 or toi <= 0:continue
          expected = rate * toi / 60.0
          feature_identity = digest({"contract":contract_hash,"arm":arm,"game_id":int(row.game_id),"player_id":int(row.player_id),"feature_cutoff_utc":str(row.feature_cutoff_utc),"rate":rate,"toi":toi,"class":player_class,"transition":transition})
          input_rows.append({**base,"phase":phase,"contract_arm":arm,"arm_status":"SELECTED" if arm==contract["selected_arm"] else ("UNQUALIFIED_SHADOW" if arm.startswith("F_") else "SHADOW_CHALLENGER"),"selected_sog_per60":rate,"selected_toi_minutes":toi,"expected_sog":expected,"cold_start_class":player_class,"transition_state":transition,"feature_identity":feature_identity,"contract_version":CONTRACT_VERSION,"contract_sha256":contract_hash})
          for line in LINES:
            p_over = poisson_tail(expected, line)
            mass = [math.exp(-expected) * expected**k / math.factorial(k) for k in range(7)]
            prediction_identity = digest({"contract": contract_hash, "arm":arm,"phase": phase,
                                          "slate_date": slate_date, "game_id": int(row.game_id),
                                          "player_id": int(row.player_id), "line": line})
            prediction_rows.append({
                **base, "phase": phase, "prediction_timestamp_utc": pred_ts.isoformat(),
                "input_cutoff_utc": cutoff.isoformat(), "prediction_identity": prediction_identity,
                "feature_identity": feature_identity, "cold_start_class": player_class,
                "contract_arm":arm,"arm_status":"SELECTED" if arm==contract["selected_arm"] else ("UNQUALIFIED_SHADOW" if arm.startswith("F_") else "SHADOW_CHALLENGER"),"transition_state": transition, "prop_type": "shots_on_goal", "line": line,
                "expected_sog": expected, "p_over": p_over, "p_under": 1.0 - p_over,
                **{f"p_sog_{k}": mass[k] for k in range(7)},
                "p_sog_7_plus": max(0.0, 1.0 - sum(mass)),
                "selected_side": "OVER" if p_over >= 0.5 else "UNDER",
                "model_family": contract["model_family"], "model_version": contract["model_version"],
                "contract_version": CONTRACT_VERSION, "contract_sha256": contract_hash,
                "market_attachment_status": "UNAVAILABLE_NOT_REQUIRED",
                "bookmaker": None, "price": None, "market_probability": None,
                "evidence_status": "IMMUTABLE_PREDICTION_ONLY_SHADOW",
            })
    predictions = pd.DataFrame(prediction_rows)
    inputs = pd.DataFrame(input_rows)
    exclusions = pd.DataFrame(exclusion_rows)
    if not predictions.empty and predictions.prediction_identity.duplicated().any():
        raise RuntimeError("DUPLICATE_IMMUTABLE_PREDICTION_IDENTITY")
    return predictions, inputs, exclusions


def grade_predictions(predictions: pd.DataFrame, outcomes: pd.DataFrame, *, grading_timestamp_utc: str) -> pd.DataFrame:
    required = {"canonical_season", "slate_date", "game_id", "player_id", "official_final", "official_sog", "participation_status", "outcome_source", "outcome_source_timestamp_utc"}
    if required - set(outcomes):
        raise ValueError(f"OUTCOME_SCHEMA_INCOMPLETE:{sorted(required-set(outcomes))}")
    if outcomes.duplicated(["canonical_season", "slate_date", "game_id", "player_id"]).any():
        raise RuntimeError("DUPLICATE_CANONICAL_OUTCOME_IDENTITY")
    joined = predictions.merge(outcomes, on=["canonical_season", "slate_date", "game_id", "player_id"], how="left", validate="many_to_one")
    states: list[str] = []
    for row in joined.itertuples(index=False):
        if pd.isna(row.participation_status) or not bool(row.official_final):
            state = "UNRESOLVED_UNGRADED"
        elif row.participation_status == "LATE_SCRATCH":
            state = "LATE_SCRATCH_UNGRADED"
        elif row.participation_status in {"NO_APPEARANCE", "NONPARTICIPANT"}:
            state = "NO_APPEARANCE_UNGRADED"
        elif row.participation_status == "POSTPONED":
            state = "POSTPONED_UNGRADED"
        elif row.participation_status == "APPEARED" and pd.notna(row.official_sog):
            actual_over = float(row.official_sog) > float(row.line)
            state = "WIN" if (actual_over and row.selected_side == "OVER") or (not actual_over and row.selected_side == "UNDER") else "LOSS"
        else:
            state = "UNRESOLVED_UNGRADED"
        states.append(state)
    joined["grading_state"] = states
    joined["grading_timestamp_utc"] = pd.Timestamp(grading_timestamp_utc).isoformat()
    return joined
