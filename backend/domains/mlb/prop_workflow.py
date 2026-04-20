"""MLB prop workflow domain logic: prepare, predict, persist."""

from __future__ import annotations

import math
import os
from datetime import date, datetime
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

from backend.app.services.mlb.commit_tokens import sign_commit_payload, verify_commit_token
from backend.domains.mlb.game_context import build_game_context
from backend.domains.mlb.repository.prop_repository import (
    DuplicatePropError,
    find_duplicate_prop_id,
    insert_prop_row,
)
from backend.domains.mlb.player_resolver import resolve_player_candidate
from backend.mlb.shared.team_name_map import (
    getFullTeamAbbreviationFromID,
    getTeamIdFromAbbr,
    normalizeTeamAbbreviation,
)
from backend.shared.db.pg import pg_fetchone

ET = ZoneInfo("America/New_York")
_HORIZONS = ("d7", "d15", "d30")
_DERIVED_BASE_PROPS = {
    "hits",
    "total_bases",
    "strikeouts_batting",
    "earned_runs",
    "doubles",
    "hits_allowed",
    "strikeouts_pitching",
    "walks",
    "hits_runs_rbis",
    "runs_scored",
    "walks_allowed",
    "runs_rbis",
    "rbis",
}
_BVP_ALIAS_TO_CANONICAL = {
    "bvp_pa_prior": "bvp_plate_appearances",
    "bvp_ab_prior": "bvp_at_bats",
    "bvp_hits_prior": "bvp_hits",
    "bvp_hr_prior": "bvp_home_runs",
    "bvp_bb_prior": "bvp_walks",
    "bvp_so_prior": "bvp_strikeouts",
    "bvp_tb_prior": "bvp_total_bases",
}
_BVP_CANONICAL_KEYS = {
    "bvp_plate_appearances",
    "bvp_at_bats",
    "bvp_hits",
    "bvp_home_runs",
    "bvp_rbi",
    "bvp_strikeouts",
    "bvp_walks",
    "bvp_total_bases",
}


def _env_enabled(name: str, default: bool = True) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() not in {"0", "false", "no", "off"}


def normalize_prop_type(prop_type: str) -> str:
    return (
        (prop_type or "")
        .strip()
        .lower()
        .replace("(", "")
        .replace(")", "")
        .replace(" + ", "_")
        .replace(" ", "_")
        .strip("_")
    )


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _is_missing(v: Any) -> bool:
    return v is None or v == ""


def _n(v: Any) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None


def _load_latest_derived_stats(player_id: int, game_id: Optional[int], game_date: str) -> Dict[str, Any]:
    row = pg_fetchone(
        """
SELECT row_to_json(pds)::jsonb AS stats
FROM mlb.player_derived_stats pds
WHERE pds.player_id = %s
  AND (
    (%s::int IS NOT NULL AND pds.game_id = %s::int)
    OR pds.game_date::date <= %s::date
  )
ORDER BY
  CASE WHEN %s::int IS NOT NULL AND pds.game_id = %s::int THEN 1 ELSE 0 END DESC,
  pds.game_date DESC NULLS LAST
LIMIT 1
""",
        (int(player_id), game_id, game_id, str(game_date), game_id, game_id),
    ) or {}
    stats = row.get("stats")
    return stats if isinstance(stats, dict) else {}


def _load_latest_pfp_features(
    *,
    prop_type: str,
    player_id: int,
    game_id: Optional[int],
    game_date: str,
    feature_set_tag: str,
) -> Dict[str, Any]:
    row = pg_fetchone(
        """
SELECT pfp.features
FROM mlb.prop_features_precomputed pfp
WHERE pfp.prop_type = %s
  AND pfp.player_id = %s
  AND pfp.feature_set_tag = %s
  AND (
    (%s::int IS NOT NULL AND pfp.game_id = %s::int)
    OR pfp.game_date::date <= %s::date
  )
ORDER BY
  CASE WHEN %s::int IS NOT NULL AND pfp.game_id = %s::int THEN 1 ELSE 0 END DESC,
  pfp.game_date DESC NULLS LAST
LIMIT 1
""",
        (
            str(prop_type),
            int(player_id),
            str(feature_set_tag),
            game_id,
            game_id,
            str(game_date),
            game_id,
            game_id,
        ),
    ) or {}
    features = row.get("features")
    return features if isinstance(features, dict) else {}


def _hydrate_bvp_feature_snapshot(
    *,
    prop_type: str,
    player_id: int,
    game_id: Optional[int],
    game_date: str,
) -> Dict[str, Any]:
    if not _env_enabled("MLB_BVP_FEATURES_ENABLED", True):
        return {}

    feature_set_tag = (
        os.getenv("MLB_BVP_FEATURE_SET_TAG")
        or os.getenv("MLB_PFP_OVERLAP_FEATURE_SET_TAG")
        or "v1"
    )
    try:
        features = _load_latest_pfp_features(
            prop_type=prop_type,
            player_id=player_id,
            game_id=game_id,
            game_date=game_date,
            feature_set_tag=feature_set_tag,
        )
    except Exception:
        return {}
    if not features:
        return {}

    out: Dict[str, Any] = {}

    # Keep direct bvp_* payloads from prop_features_precomputed.
    for k, v in features.items():
        key = str(k)
        if not key.startswith("bvp_"):
            continue
        if _is_missing(v):
            continue
        out[key] = _to_float(v, 0.0)

    # Normalize legacy aliases into canonical keys expected by feature metadata.
    for alias, canonical in _BVP_ALIAS_TO_CANONICAL.items():
        if _is_missing(out.get(canonical)) and not _is_missing(out.get(alias)):
            out[canonical] = _to_float(out.get(alias), 0.0)

    # Mirror canonical keys back to aliases for backward compatibility.
    for alias, canonical in _BVP_ALIAS_TO_CANONICAL.items():
        if _is_missing(out.get(alias)) and not _is_missing(out.get(canonical)):
            out[alias] = _to_float(out.get(canonical), 0.0)

    # Ensure all canonical keys are present when we have any BvP payload.
    for canonical in _BVP_CANONICAL_KEYS:
        if canonical not in out:
            out[canonical] = 0.0

    return out


def _derive_combo_stat(stats: Dict[str, Any], horizon: str, target_prop: str) -> Optional[float]:
    runs = _n(stats.get(f"{horizon}_runs_scored"))
    rbis = _n(stats.get(f"{horizon}_rbis"))
    hits = _n(stats.get(f"{horizon}_hits"))
    if target_prop == "runs_rbis":
        if runs is None or rbis is None:
            return None
        return runs + rbis
    if target_prop == "hits_runs_rbis":
        if hits is None or runs is None or rbis is None:
            return None
        return hits + runs + rbis
    return None


def _hydrate_derived_feature_snapshot(
    *,
    player_id: int,
    game_id: Optional[int],
    game_date: str,
    prop_type: str,
) -> Dict[str, Any]:
    try:
        stats = _load_latest_derived_stats(player_id, game_id, game_date)
    except Exception:
        return {}
    if not stats:
        return {}

    wanted = set(_DERIVED_BASE_PROPS)
    wanted.add(str(prop_type))
    out: Dict[str, Any] = {}
    for horizon in _HORIZONS:
        for base in wanted:
            key = f"{horizon}_{base}"
            if key in stats and not _is_missing(stats.get(key)):
                out[key] = _to_float(stats.get(key), 0.0)
        for combo_prop in ("runs_rbis", "hits_runs_rbis"):
            combo_key = f"{horizon}_{combo_prop}"
            if _is_missing(out.get(combo_key)):
                derived = _derive_combo_stat(stats, horizon, combo_prop)
                if derived is not None:
                    out[combo_key] = float(derived)
    return out


def prepare_prop(payload: Dict[str, Any]) -> Dict[str, Any]:
    game_date = str(payload.get("game_date") or "").strip()
    if not game_date:
        game_date = datetime.now(ET).date().isoformat()
    date.fromisoformat(game_date)  # raises on bad format

    prop_type = normalize_prop_type(str(payload.get("prop_type") or ""))
    if not prop_type:
        raise ValueError("prop_type is required")

    prop_value = _to_float(payload.get("prop_value"), 0.0)
    over_under = str(payload.get("over_under") or "over").strip().lower()
    if over_under not in {"over", "under"}:
        over_under = "over"

    player_id = payload.get("player_id")
    team_id = payload.get("team_id")
    team_abbr = normalizeTeamAbbreviation(payload.get("team_abbr") or payload.get("team"))
    player_name = str(payload.get("player_name") or payload.get("name") or "").strip() or None

    warnings = []

    # Resolve identity/team from inputs and tables.
    resolved = resolve_player_candidate(
        player_id=int(player_id) if player_id is not None else None,
        name=player_name,
        team_abbr=team_abbr,
    )
    if resolved:
        resolved_team_id = resolved.get("team_id")
        resolved_team_abbr = resolved.get("team_abbr")
        if (
            team_id is not None
            and resolved_team_id is not None
            and int(team_id) != int(resolved_team_id)
        ):
            warnings.append("input team_id mismatched resolved player team; using resolved team")
        if (
            team_abbr
            and resolved_team_abbr
            and normalizeTeamAbbreviation(team_abbr) != normalizeTeamAbbreviation(resolved_team_abbr)
        ):
            warnings.append("input team_abbr mismatched resolved player team; using resolved team")
        player_id = resolved.get("player_id")
        player_name = player_name or resolved.get("player_name")
        if resolved_team_id is not None:
            team_id = resolved_team_id
        if resolved_team_abbr:
            team_abbr = normalizeTeamAbbreviation(resolved_team_abbr)

    if team_id is None and team_abbr:
        team_id = getTeamIdFromAbbr(team_abbr)
    if team_abbr is None and team_id is not None:
        team_abbr = normalizeTeamAbbreviation(getFullTeamAbbreviationFromID(int(team_id)))

    if player_id is None:
        raise ValueError("unable to resolve player_id")
    if team_id is None:
        raise ValueError("unable to resolve team_id/team_abbr")

    try:
        context = build_game_context(team_id=int(team_id), game_date=game_date)
    except Exception:
        context = None
    if not context:
        # Offseason/no-network fallback: keep pipeline alive with neutral defaults.
        warnings.append("game context unavailable; using fallback context")
        game_id_fallback = payload.get("game_id")
        try:
            game_id_fallback = int(game_id_fallback) if game_id_fallback is not None else None
        except Exception:
            game_id_fallback = None
        context = {
            "team_id": int(team_id),
            "team_abbr": team_abbr,
            "for_date": game_date,
            "game_id": game_id_fallback,
            "game_type": (str(payload.get("game_type") or "").strip().upper() or None),
            "game_time": None,
            "is_home": bool(payload.get("is_home", False)),
            "home_team_code": payload.get("home_team_code"),
            "away_team_code": payload.get("away_team_code"),
            "opponent_team_id": payload.get("opponent_team_id"),
            "opponent": payload.get("opponent"),
            "opponent_encoded": payload.get("opponent_encoded"),
            "game_day_of_week": payload.get("game_day_of_week"),
            "time_of_day_bucket": payload.get("time_of_day_bucket"),
            "starting_pitcher_id": payload.get("starting_pitcher_id"),
        }

    rolling_raw = payload.get("rolling_result_avg_7")
    if _is_missing(rolling_raw):
        rolling_result_avg_7 = None
    else:
        rolling_result_avg_7 = _to_float(rolling_raw, None)

    line_diff_raw = payload.get("line_diff")
    if _is_missing(line_diff_raw):
        line_diff = (
            (float(rolling_result_avg_7) - prop_value)
            if rolling_result_avg_7 is not None
            else None
        )
    else:
        line_diff = _to_float(line_diff_raw, None)
    market_odds_american = payload.get("market_odds_american")
    market_implied_probability = payload.get("market_implied_probability")
    market_odds_american = _to_float(market_odds_american, None) if market_odds_american not in (None, "") else None
    market_implied_probability = (
        _to_float(market_implied_probability, None)
        if market_implied_probability not in (None, "")
        else None
    )
    # Two-sided market fields (OddsAPI snapshot native).
    price_over_american = payload.get("price_over_american")
    price_under_american = payload.get("price_under_american")
    implied_over = payload.get("implied_over")
    implied_under = payload.get("implied_under")
    implied_over_novig = payload.get("implied_over_novig")
    implied_under_novig = payload.get("implied_under_novig")
    market_hold = payload.get("market_hold")

    price_over_american = _to_float(price_over_american, None) if price_over_american not in (None, "") else None
    price_under_american = _to_float(price_under_american, None) if price_under_american not in (None, "") else None
    implied_over = _to_float(implied_over, None) if implied_over not in (None, "") else None
    implied_under = _to_float(implied_under, None) if implied_under not in (None, "") else None
    implied_over_novig = _to_float(implied_over_novig, None) if implied_over_novig not in (None, "") else None
    implied_under_novig = _to_float(implied_under_novig, None) if implied_under_novig not in (None, "") else None
    market_hold = _to_float(market_hold, None) if market_hold not in (None, "") else None

    features = {
        "player_id": int(player_id),
        "player_name": player_name,
        "team_id": int(team_id),
        "team": team_abbr,
        "game_date": game_date,
        "prop_type": prop_type,
        "line": prop_value,
        "prop_value": prop_value,
        "over_under": over_under,
        "rolling_result_avg_7": rolling_result_avg_7,
        "hit_streak": _to_float(payload.get("hit_streak"), 0.0),
        "win_streak": _to_float(payload.get("win_streak"), 0.0),
        "line_diff": line_diff,
        "price_over_american": price_over_american,
        "price_under_american": price_under_american,
        "implied_over": implied_over,
        "implied_under": implied_under,
        "implied_over_novig": implied_over_novig,
        "implied_under_novig": implied_under_novig,
        "market_hold": market_hold,
        "market_odds_american": market_odds_american,
        "market_implied_probability": market_implied_probability,
        **context,
    }
    if _is_missing(features.get("home_team_code")) and not _is_missing(payload.get("home_team_code")):
        features["home_team_code"] = payload.get("home_team_code")
    if _is_missing(features.get("away_team_code")) and not _is_missing(payload.get("away_team_code")):
        features["away_team_code"] = payload.get("away_team_code")

    # Hydrate derived rolling stats so prediction has signal even when caller omits them.
    game_id_context = context.get("game_id")
    try:
        resolved_game_id = int(game_id_context) if game_id_context is not None else None
    except Exception:
        resolved_game_id = None
    derived = _hydrate_derived_feature_snapshot(
        player_id=int(player_id),
        game_id=resolved_game_id,
        game_date=game_date,
        prop_type=prop_type,
    )
    for k, v in derived.items():
        if _is_missing(features.get(k)):
            features[k] = v

    # Hydrate BvP/PvB precomputed payloads from prop_features_precomputed.
    bvp_features = _hydrate_bvp_feature_snapshot(
        prop_type=prop_type,
        player_id=int(player_id),
        game_id=resolved_game_id,
        game_date=game_date,
    )
    for k, v in bvp_features.items():
        if _is_missing(features.get(k)):
            features[k] = v

    # If rolling/line fields were absent from input, derive them from d7_<prop>.
    d7_prop = _n(features.get(f"d7_{prop_type}"))
    if d7_prop is None and prop_type in {"runs_rbis", "hits_runs_rbis"}:
        d7_prop = _n(features.get(f"d7_{prop_type}"))
    if _is_missing(payload.get("rolling_result_avg_7")) and d7_prop is not None:
        features["rolling_result_avg_7"] = float(d7_prop)
    if _is_missing(payload.get("line_diff")) and d7_prop is not None:
        features["line_diff"] = float(d7_prop) - prop_value

    if warnings:
        features["_warnings"] = warnings
    return features


def _heuristic_probability(features: Dict[str, Any]) -> float:
    line_diff = _to_float(features.get("line_diff"), 0.0)
    hit_streak = _to_float(features.get("hit_streak"), 0.0)
    win_streak = _to_float(features.get("win_streak"), 0.0)
    is_home = 1.0 if bool(features.get("is_home")) else 0.0

    z = 0.0
    z += 0.9 * math.tanh(line_diff)
    z += 0.07 * max(-8.0, min(8.0, hit_streak))
    z += 0.05 * max(-8.0, min(8.0, win_streak))
    z += 0.10 * (1.0 if is_home else -1.0)
    return 1.0 / (1.0 + math.exp(-z))


def predict_prop(prop_type: str, features: Dict[str, Any]) -> Dict[str, Any]:
    normalized = normalize_prop_type(prop_type)
    if not normalized:
        raise ValueError("prop_type is required")

    probability: Optional[float] = None
    decision_threshold = 0.5
    model_name = "heuristic_fallback_v1"
    model_meta: Dict[str, Any] = {"strategy": "heuristic"}

    # Try canonical model pipeline first; fallback if unavailable.
    try:
        from backend.mlb.prediction.make_prediction import predict as model_predict

        result = model_predict(prop_type=normalized, features=features)
        probability = _to_float(result.get("probability"), None)  # type: ignore[arg-type]
        if probability is None:
            probability = _to_float(result.get("probability_over"), None)  # type: ignore[arg-type]
        decision_threshold = _to_float(result.get("decision_threshold"), 0.5)
        decision_threshold = max(0.0, min(1.0, float(decision_threshold)))
        model_name = str(result.get("blend", {}).get("strategy") or "model_pipeline")
        model_meta = {
            "strategy": "model_pipeline",
            "components": result.get("components"),
            "blend": result.get("blend"),
            "decision_threshold": decision_threshold,
        }
    except Exception:
        probability = None

    if probability is None:
        probability = _heuristic_probability(features)

    probability = max(0.0, min(1.0, float(probability)))
    recommendation = "over" if probability >= decision_threshold else "under"
    commit_payload = {
        "flow": "mlb_prop_v1",
        "prop_type": normalized,
        "features": features,
        "probability": probability,
        "decision_threshold": decision_threshold,
        "recommendation": recommendation,
    }
    commit_token = sign_commit_payload(commit_payload)

    return {
        "prop_type": normalized,
        "probability": probability,
        "probability_over": probability,
        "probability_under": 1.0 - probability,
        "decision_threshold": decision_threshold,
        "recommendation": recommendation,
        "predicted_outcome": recommendation,
        "commit_token": commit_token,
        "model": model_name,
        "model_meta": model_meta,
    }


def add_prop_from_commit(
    *,
    commit_token: str,
    prop_source: str = "user_added",
    user_id: Optional[str] = None,
) -> Dict[str, Any]:
    payload = verify_commit_token(commit_token)
    features = payload.get("features") or {}
    prop_type = normalize_prop_type(payload.get("prop_type") or "")
    probability = _to_float(payload.get("probability"), 0.5)
    decision_threshold = max(0.0, min(1.0, float(_to_float(payload.get("decision_threshold"), 0.5))))
    recommendation = str(payload.get("recommendation") or ("over" if probability >= decision_threshold else "under"))

    if features.get("player_id") is None:
        raise ValueError("player_id missing in committed features")
    if features.get("game_id") is None:
        raise ValueError("game_id missing in committed features")
    game_date_raw = str(features.get("game_date") or "").strip()
    if not game_date_raw:
        raise ValueError("game_date missing in committed features")
    try:
        date.fromisoformat(game_date_raw)
    except ValueError as e:
        raise ValueError("game_date must be YYYY-MM-DD") from e
    context_date = str(features.get("for_date") or "").strip()
    if context_date and context_date != game_date_raw:
        raise ValueError("game_date mismatch with context for_date")

    player_id = int(features.get("player_id"))
    game_id = int(features.get("game_id"))
    if game_id <= 0:
        raise ValueError("game_id must be a positive integer")
    over_under = str(features.get("over_under") or "over").lower()
    prop_value = _to_float(features.get("prop_value"), 0.0)
    game_date = game_date_raw
    team = normalizeTeamAbbreviation(features.get("team") or features.get("team_abbr"))
    team_id = features.get("team_id")
    game_type = str(features.get("game_type") or "").strip().upper() or None
    player_name = features.get("player_name")

    dup_id = find_duplicate_prop_id(
        player_id=player_id,
        game_id=game_id,
        prop_type=prop_type,
        over_under=over_under,
        prop_value=prop_value,
        prop_source=prop_source,
    )
    if dup_id:
        return {"ok": True, "saved": False, "duplicate": True, "id": dup_id}

    try:
        insert_prop_row(
            player_id=player_id,
            player_name=player_name,
            team=team,
            team_id=int(team_id) if team_id is not None else None,
            game_id=game_id,
            game_date=game_date,
            prop_type=prop_type,
            prop_value=prop_value,
            over_under=over_under,
            prop_source=prop_source,
            recommendation=recommendation,
            probability=probability,
            game_type=game_type,
            user_id=user_id,
        )
    except DuplicatePropError:
        dup_id = find_duplicate_prop_id(
            player_id=player_id,
            game_id=game_id,
            prop_type=prop_type,
            over_under=over_under,
            prop_value=prop_value,
            prop_source=prop_source,
        )
        return {"ok": True, "saved": False, "duplicate": True, "id": dup_id}
    return {"ok": True, "saved": True, "duplicate": False}
