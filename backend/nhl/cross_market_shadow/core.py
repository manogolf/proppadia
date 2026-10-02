"""Pure runtime for the NHL 2026 V2 cross-market shadow lane."""
from __future__ import annotations

import hashlib
import json
import math
import re
import fcntl
import unicodedata
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from backend.nhl.game_phase import GAME_TYPE_PHASE, phase_for_game_type, regular_season_evaluation_eligible
from sklearn.metrics import roc_auc_score
from .shot_prior import (
    CHALLENGER_NAME, POLICY_VERSION, build_shot_prior_challenger,
)
from .moneyline_challenger import (
    CHALLENGER_NAME as MONEYLINE_CHALLENGER_NAME,
    POLICY_VERSION as MONEYLINE_FEATURE_POLICY_VERSION,
    build_challenger_predictions as build_moneyline_challenger,
    make_ledger as make_moneyline_challenger_ledger,
)


HERE = Path(__file__).resolve().parent
PARAMETER_PATH = HERE / "frozen_control_v2.json"
PUCK_PARAMETER_PATH = HERE / "frozen_puck_line_control_v1.json"
ACTIVATION_PATH = HERE / "activation_v1.json"
CONTROL_NAME = "NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V2"
PUCK_CONTROL_NAME = "NHL_STANDARD_PUCK_LINE_SIMPLE_BASELINE_V1"
V1_DISPOSITION = "HISTORICAL_REFERENCE_NOT_FORWARD_REPLAYABLE"
SEASON = 2026
PRESEASON_START = "2026-09-19"
REGULAR_SEASON_START = "2026-09-29"
FEATURES = [
    "diff_std_goal_diff_pg", "diff_r10_goal_diff_pg", "diff_std_shot_diff_pg",
    "diff_days_rest", "home_back_to_back", "away_back_to_back",
]
SCHEDULE_COLUMNS = [
    "canonical_season", "slate_date", "game_id", "game_date", "scheduled_start_time_utc",
    "home_team_id", "home_team", "away_team_id", "away_team", "game_status",
]
GAME_TYPES = GAME_TYPE_PHASE
MAX_REQUESTS_PER_RUN = 1
MAX_ESTIMATED_CREDITS_PER_RUN = 4
MARKETS = ("h2h", "spreads")
NHL_OPERATIONAL_TZ = ZoneInfo("America/New_York")
TIME_WARNING_MINUTES = 15
REGIONS = ("us", "us2")
PRIOR_SOURCE_DIR = Path(__file__).resolve().parents[3] / "artifacts" / "analysis" / "model_development"
PRIOR_TEAM_SOURCE = PRIOR_SOURCE_DIR / "nhl_season_2025_frozen_moneyline_replay_and_sog_novelty_v1" / "2026-09-15" / "season_2025_team_game_source.csv"
PRIOR_TEAM_MANIFEST = PRIOR_TEAM_SOURCE.parent / "SHA256SUMS"
PRIOR_OUTCOME_SOURCE = PRIOR_SOURCE_DIR / "nhl_cross_market_game_state_bridge_v1" / "2026-09-15" / "season_2025_outcome_spine.parquet"
PRIOR_OUTCOME_MANIFEST = PRIOR_OUTCOME_SOURCE.parent / "SHA256SUMS"

TEAM_NAMES = {
    "anaheimducks": "ANA", "bostonbruins": "BOS", "buffalosabres": "BUF",
    "calgaryflames": "CGY", "carolinahurricanes": "CAR", "chicagoblackhawks": "CHI",
    "coloradoavalanche": "COL", "columbusbluejackets": "CBJ", "dallasstars": "DAL",
    "detroitredwings": "DET", "edmontonoilers": "EDM", "floridapanthers": "FLA",
    "losangeleskings": "LAK", "minnesotawild": "MIN", "montrealcanadiens": "MTL",
    "montréalcanadiens": "MTL", "nashvillepredators": "NSH", "newjerseydevils": "NJD",
    "newyorkislanders": "NYI", "newyorkrangers": "NYR", "ottawasenators": "OTT",
    "philadelphiaflyers": "PHI", "pittsburghpenguins": "PIT", "sanjosesharks": "SJS",
    "seattlekraken": "SEA", "stlouisblues": "STL", "tampabaylightning": "TBL",
    "torontomapleleafs": "TOR", "utahmammoth": "UTA", "utahhockeyclub": "UTA",
    "vancouvercanucks": "VAN", "vegasgoldenknights": "VGK", "washingtoncapitals": "WSH",
    "winnipegjets": "WPG",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _binary_ece(actual: pd.Series, probability: pd.Series) -> float:
    frame = pd.DataFrame({"actual": pd.to_numeric(actual), "probability": pd.to_numeric(probability)})
    frame["bin"] = pd.cut(frame.probability, bins=np.linspace(0, 1, 11), include_lowest=True)
    return float(sum(
        len(group) / len(frame) * abs(group.actual.mean() - group.probability.mean())
        for _, group in frame.groupby("bin", observed=False) if len(group)
    )) if len(frame) else float("nan")


def parse_utc(value: Any) -> pd.Timestamp:
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"INVALID_UTC_TIMESTAMP:{value!r}")
    return parsed


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def digest_value(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def load_parameters() -> dict[str, Any]:
    parameters = json.loads(PARAMETER_PATH.read_text())
    if parameters.get("control_name") != CONTROL_NAME or parameters.get("feature_order") != FEATURES:
        raise RuntimeError("V2_PARAMETER_IDENTITY_MISMATCH")
    return parameters


def load_puck_parameters() -> dict[str, Any]:
    parameters = json.loads(PUCK_PARAMETER_PATH.read_text())
    if parameters.get("model_name") != PUCK_CONTROL_NAME or parameters.get("feature_order") != FEATURES:
        raise RuntimeError("PUCK_LINE_PARAMETER_IDENTITY_MISMATCH")
    if parameters.get("classes") != ["AWAY_BY_2_PLUS", "ONE_GOAL_GAME", "HOME_BY_2_PLUS"]:
        raise RuntimeError("PUCK_LINE_CLASS_IDENTITY_MISMATCH")
    return parameters


def _verified_manifest_entry(manifest: Path, artifact: Path, expected: str) -> None:
    if not manifest.is_file() or sha256(manifest) == "":
        raise RuntimeError("SHOT_PRIOR_MANIFEST_MISSING")
    entries = {name: digest for digest, name in (
        line.split("  ", 1) for line in manifest.read_text().splitlines() if "  " in line
    )}
    actual = entries.get(artifact.name)
    if actual != expected or sha256(artifact) != expected:
        raise RuntimeError(f"SHOT_PRIOR_PINNED_SOURCE_MISMATCH:{artifact.name}")


def load_verified_2025_shot_prior() -> tuple[pd.DataFrame, str, str]:
    """Load the certified, pinned 2025 regular-season shot/outcome spine."""
    source_hash = "99fa6f7ab39a00f6aca2b6f2a11b22932a261fac3d8584703732615d38277af7"
    outcome_hash = "b8b51274837c413ae8d957da93180ba6528a530f1b5e3670503011411133ef43"
    _verified_manifest_entry(PRIOR_TEAM_MANIFEST, PRIOR_TEAM_SOURCE, source_hash)
    _verified_manifest_entry(PRIOR_OUTCOME_MANIFEST, PRIOR_OUTCOME_SOURCE, outcome_hash)
    shots = pd.read_csv(PRIOR_TEAM_SOURCE)
    outcomes = pd.read_parquet(PRIOR_OUTCOME_SOURCE)
    qualified = outcomes[
        outcomes.identity_match.astype(str).str.lower().eq("true")
        & outcomes.final_state_qualified.astype(str).str.lower().eq("true")
        & outcomes.decisive_score_qualified.astype(str).str.lower().eq("true")
        & outcomes.outcome_qualified.astype(str).str.lower().eq("true")
    ]
    joined = shots.merge(
        qualified[["game_id", "canonical_season", "game_date", "start_time_utc",
                   "home_team", "away_team", "final_home_goals", "final_away_goals"]],
        on=["game_id", "canonical_season"],
        how="inner", validate="one_to_one", suffixes=("", "_outcome"),
    )
    starts_match = pd.to_datetime(joined.start_time_utc, utc=True).eq(
        pd.to_datetime(joined.start_time_utc_outcome, utc=True)
    )
    dates_match = pd.to_datetime(joined.game_date).dt.date.eq(
        pd.to_datetime(joined.game_date_outcome).dt.date
    )
    identity_ok = (
        joined.home_team_code.astype(str).str.upper().eq(joined.home_team.astype(str).str.upper())
        & joined.away_team_code.astype(str).str.upper().eq(joined.away_team.astype(str).str.upper())
        & starts_match & dates_match
    )
    if len(shots) != 1312 or len(joined) != 1312 or not identity_ok.all():
        raise RuntimeError("SHOT_PRIOR_2025_OUTCOME_JOIN_INCOMPLETE_OR_IDENTITY_CONFLICT")
    if not pd.to_numeric(joined.game_type, errors="coerce").eq(2).all():
        raise RuntimeError("SHOT_PRIOR_2025_NON_REGULAR_SEASON_SOURCE")
    joined["database_game_status"] = "FINAL"
    return joined, source_hash, outcome_hash


def load_activation() -> dict[str, Any]:
    activation = json.loads(ACTIVATION_PATH.read_text())
    if activation.get("control_name") != CONTROL_NAME or activation.get("season") != SEASON:
        raise RuntimeError("ACTIVATION_IDENTITY_MISMATCH")
    return activation


def normalize_game_types(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    source = next((name for name in ["game_type_code", "game_type", "gameType"] if name in result), None)
    if source is None:
        raise ValueError("GAME_TYPE_REQUIRED")
    result["game_type_code"] = pd.to_numeric(result[source], errors="coerce").astype("Int64")
    result["game_type_label"] = result.game_type_code.map(phase_for_game_type)
    return result


def _team_history(history: pd.DataFrame, team_id: int, target: pd.Series) -> pd.DataFrame:
    target_type = int(target.game_type_code)
    allowed = {1: set(), 2: {2}, 3: {2, 3}}[target_type]
    if not allowed:
        return history.iloc[0:0].copy()
    subset = history[
        history.canonical_season.eq(int(target.canonical_season))
        & history.game_type_code.isin(allowed)
        & history.game_status.astype(str).str.upper().isin(["FINAL", "OFF", "COMPLETED"])
        & history.scheduled_start_time_utc.lt(target.scheduled_start_time_utc)
    ]
    home = subset[subset.home_team_id.eq(team_id)].copy().assign(
        gf=lambda x: x.final_home_goals, ga=lambda x: x.final_away_goals,
        sf=lambda x: x.final_home_shots, sa=lambda x: x.final_away_shots,
    )
    away = subset[subset.away_team_id.eq(team_id)].copy().assign(
        gf=lambda x: x.final_away_goals, ga=lambda x: x.final_home_goals,
        sf=lambda x: x.final_away_shots, sa=lambda x: x.final_home_shots,
    )
    return pd.concat([home, away], ignore_index=True).sort_values(
        ["scheduled_start_time_utc", "game_id"], kind="mergesort"
    )


def build_v2_predictions(schedule: pd.DataFrame, history: pd.DataFrame, prediction_time_utc: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    parameters = load_parameters()
    created = parse_utc(prediction_time_utc)
    schedule = normalize_game_types(schedule)
    history = normalize_game_types(history)
    required_schedule = set(SCHEDULE_COLUMNS)
    required_history = required_schedule | {
        "final_home_goals", "final_away_goals", "final_home_shots", "final_away_shots",
    }
    if required_schedule - set(schedule) or required_history - set(history):
        raise ValueError("V2_INPUT_SCHEMA_INCOMPLETE")
    schedule["scheduled_start_time_utc"] = pd.to_datetime(schedule.scheduled_start_time_utc, utc=True, format="mixed")
    history["scheduled_start_time_utc"] = pd.to_datetime(history.scheduled_start_time_utc, utc=True, format="mixed")
    if schedule.duplicated(["canonical_season", "game_id"]).any():
        raise ValueError("DUPLICATE_GAME_IDENTITY")
    if not schedule.canonical_season.eq(SEASON).all():
        raise ValueError("CANONICAL_SEASON_MUST_EQUAL_2026")
    rows, timing = [], []
    for _, target in schedule.sort_values(["scheduled_start_time_utc", "game_id"]).iterrows():
        if target.game_type_label == "UNKNOWN_GAME_TYPE":
            raise ValueError(f"UNKNOWN_GAME_TYPE:{target.game_id}")
        prestart = created < target.scheduled_start_time_utc
        values: dict[str, dict[str, Any]] = {}
        for side, team_id in [("home", int(target.home_team_id)), ("away", int(target.away_team_id))]:
            prior = _team_history(history, team_id, target)
            if len(prior):
                date_delta = (pd.Timestamp(target.game_date) - pd.Timestamp(prior.game_date.iloc[-1])).days
                values[side] = {
                    "std_goal_diff_pg": float((prior.gf - prior.ga).mean()),
                    "r10_goal_diff_pg": float((prior.tail(10).gf - prior.tail(10).ga).mean()),
                    "std_shot_diff_pg": float((prior.sf - prior.sa).mean()),
                    "days_rest": float(max(date_delta - 1, 0)),
                    "back_to_back": float(date_delta == 1), "prior_games": len(prior),
                    "latest_prior_game_id": int(prior.game_id.iloc[-1]),
                    "latest_prior_start_utc": prior.scheduled_start_time_utc.iloc[-1],
                }
            else:
                values[side] = {
                    "std_goal_diff_pg": np.nan, "r10_goal_diff_pg": np.nan,
                    "std_shot_diff_pg": np.nan, "days_rest": np.nan, "back_to_back": np.nan,
                    "prior_games": 0, "latest_prior_game_id": pd.NA, "latest_prior_start_utc": pd.NaT,
                }
        home, away = values["home"], values["away"]
        feature_values = {
            "diff_std_goal_diff_pg": home["std_goal_diff_pg"] - away["std_goal_diff_pg"],
            "diff_r10_goal_diff_pg": home["r10_goal_diff_pg"] - away["r10_goal_diff_pg"],
            "diff_std_shot_diff_pg": home["std_shot_diff_pg"] - away["std_shot_diff_pg"],
            "diff_days_rest": home["days_rest"] - away["days_rest"],
            "home_back_to_back": home["back_to_back"], "away_back_to_back": away["back_to_back"],
        }
        imputed, scaled = {}, {}
        for feature in FEATURES:
            raw = feature_values[feature]
            imputed[feature] = parameters["imputation_medians"][feature] if pd.isna(raw) else float(raw)
            scaled[feature] = (
                imputed[feature] - parameters["standardization_means"][feature]
            ) / parameters["standardization_scales"][feature]
        logit = parameters["intercept"] + sum(
            scaled[feature] * parameters["coefficients"][feature] for feature in FEATURES
        )
        probability = 1 / (1 + math.exp(-logit))
        substantive = {
            "canonical_season": int(target.canonical_season), "game_id": int(target.game_id),
            "scheduled_start_time_utc": target.scheduled_start_time_utc.isoformat(),
            "home_team": target.home_team, "away_team": target.away_team,
            "features": feature_values, "parameter_sha256": sha256(PARAMETER_PATH),
        }
        row = {name: target[name] for name in SCHEDULE_COLUMNS}
        row.update(feature_values)
        for feature in FEATURES:
            row[f"imputed__{feature}"] = imputed[feature]
            row[f"scaled__{feature}"] = scaled[feature]
        row.update({
            "home_prior_games": home["prior_games"], "away_prior_games": away["prior_games"],
            "home_latest_prior_game_id": home["latest_prior_game_id"],
            "away_latest_prior_game_id": away["latest_prior_game_id"],
            "home_latest_prior_start_utc": home["latest_prior_start_utc"],
            "away_latest_prior_start_utc": away["latest_prior_start_utc"],
            "raw_missing_count": sum(pd.isna(feature_values[name]) for name in FEATURES),
            "missingness_state": "FIT_MEDIAN_IMPUTED" if any(pd.isna(feature_values[name]) for name in FEATURES) else "FULLY_OBSERVED",
            "v2_logit": logit, "v2_home_win_probability": probability,
            "v2_away_win_probability": 1 - probability,
            "model_favored_team": target.home_team if probability >= .5 else target.away_team,
            "prediction_creation_time_utc": created.isoformat(),
            "game_type_code": int(target.game_type_code),
            "game_type_label": phase_for_game_type(target.game_type_code),
            "substantive_prediction_sha256": digest_value(substantive),
            "control_name": CONTROL_NAME, "control_artifact_sha256": sha256(PARAMETER_PATH),
            "prediction_status": (
                "POST_START_REJECTED" if not prestart else
                "PRESEASON_REHEARSAL_EXCLUDED" if int(target.game_type_code) == 1 else
                "REGULAR_SEASON_SHADOW_ELIGIBLE" if int(target.game_type_code) == 2 else
                "POSTSEASON_NON_REGULAR_EVALUATION"
            ),
            "regular_season_evaluation_eligible": bool(prestart and regular_season_evaluation_eligible(target.game_type_code)),
            "wager_recommendation": "NONE_SHADOW_ONLY",
        })
        rows.append(row)
        prior_starts = [home["latest_prior_start_utc"], away["latest_prior_start_utc"]]
        timing.append({
            "game_id": int(target.game_id), "game_type_code": int(target.game_type_code),
            "target_start_utc": target.scheduled_start_time_utc.isoformat(),
            "prediction_time_utc": created.isoformat(), "prediction_before_start": prestart,
            "latest_prior_start_utc": max([x for x in prior_starts if pd.notna(x)], default=pd.NaT),
            "all_contributors_strictly_prior": all(
                pd.isna(x) or x < target.scheduled_start_time_utc for x in prior_starts
            ),
            "preseason_history_rows": 0 if int(target.game_type_code) in {1, 2} else pd.NA,
            "regular_season_evaluation_eligible": bool(prestart and regular_season_evaluation_eligible(target.game_type_code)),
        })
    return pd.DataFrame(rows), pd.DataFrame(timing)


def build_puck_line_predictions(moneyline_predictions: pd.DataFrame) -> pd.DataFrame:
    """Score the frozen three-class margin control from the same strict-prior raw features."""
    parameters = load_puck_parameters()
    created_rows: list[dict[str, Any]] = []
    for row in moneyline_predictions.itertuples(index=False):
        raw = {feature: getattr(row, feature) for feature in FEATURES}
        imputed, scaled = {}, {}
        for feature in FEATURES:
            value = raw[feature]
            imputed[feature] = parameters["imputation_medians"][feature] if pd.isna(value) else float(value)
            scaled[feature] = (
                imputed[feature] - parameters["standardization_means"][feature]
            ) / parameters["standardization_scales"][feature]
        logits = {
            label: parameters["intercepts_by_class"][label] + sum(
                scaled[feature] * parameters["coefficients_by_class"][label][feature]
                for feature in FEATURES
            )
            for label in parameters["classes"]
        }
        maximum = max(logits.values())
        exponentials = {label: math.exp(value - maximum) for label, value in logits.items()}
        denominator = sum(exponentials.values())
        probabilities = {label: exponentials[label] / denominator for label in parameters["classes"]}
        substantive = {
            "canonical_season": int(row.canonical_season), "game_id": int(row.game_id),
            "scheduled_start_time_utc": str(row.scheduled_start_time_utc),
            "home_team": row.home_team, "away_team": row.away_team,
            "features": raw, "control_artifact_sha256": sha256(PUCK_PARAMETER_PATH),
        }
        output = {
            "canonical_season": int(row.canonical_season), "slate_date": row.slate_date,
            "game_id": int(row.game_id), "game_date": row.game_date,
            "scheduled_start_time_utc": row.scheduled_start_time_utc,
            "home_team_id": int(row.home_team_id), "home_team": row.home_team,
            "away_team_id": int(row.away_team_id), "away_team": row.away_team,
            **raw,
        }
        for feature in FEATURES:
            output[f"imputed__{feature}"] = imputed[feature]
            output[f"scaled__{feature}"] = scaled[feature]
        output.update({
            "raw_missing_count": sum(pd.isna(raw[name]) for name in FEATURES),
            "missingness_state": "FIT_MEDIAN_IMPUTED" if any(pd.isna(raw[name]) for name in FEATURES) else "FULLY_OBSERVED",
            "away_by_2_plus_probability": probabilities["AWAY_BY_2_PLUS"],
            "one_goal_game_probability": probabilities["ONE_GOAL_GAME"],
            "home_by_2_plus_probability": probabilities["HOME_BY_2_PLUS"],
            "home_minus_1_5_cover_probability": probabilities["HOME_BY_2_PLUS"],
            "away_plus_1_5_cover_probability": 1 - probabilities["HOME_BY_2_PLUS"],
            "away_minus_1_5_cover_probability": probabilities["AWAY_BY_2_PLUS"],
            "home_plus_1_5_cover_probability": 1 - probabilities["AWAY_BY_2_PLUS"],
            "probability_sum": sum(probabilities.values()),
            "prediction_creation_time_utc": row.prediction_creation_time_utc,
            "prediction_status": row.prediction_status,
            "regular_season_evaluation_eligible": row.regular_season_evaluation_eligible,
            "control_name": PUCK_CONTROL_NAME,
            "control_artifact_sha256": sha256(PUCK_PARAMETER_PATH),
            "control_model_sha256": parameters["model_sha256"],
            "substantive_prediction_sha256": digest_value(substantive),
            "wager_recommendation": "NONE_SHADOW_ONLY",
        })
        created_rows.append(output)
    return pd.DataFrame(created_rows)


def normalize_name(value: Any) -> str:
    folded = unicodedata.normalize("NFKD", str(value).lower())
    unaccented = "".join(character for character in folded if not unicodedata.combining(character))
    return re.sub(r"[^a-z0-9]", "", unaccented)


def team_name_normalization_flag(value: Any) -> str:
    raw = str(value)
    return (
        "UNICODE_DIACRITICS_FOLDED"
        if unicodedata.normalize("NFKD", raw) != raw else "NONE"
    )


def team_code(value: Any) -> str | None:
    normalized = normalize_name(value)
    return TEAM_NAMES.get(normalized, str(value).upper() if len(str(value)) == 3 else None)


def american_to_decimal(price: float) -> float:
    price = float(price)
    if price == 0:
        raise ValueError("ZERO_AMERICAN_PRICE")
    return 1 + price / 100 if price > 0 else 1 + 100 / abs(price)


def normalize_markets(envelope: dict[str, Any], schedule: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    capture = parse_utc(envelope["capture_timestamp_utc"])
    schedule = normalize_game_types(schedule)
    schedule["scheduled_start_time_utc"] = pd.to_datetime(schedule.scheduled_start_time_utc, utc=True, format="mixed")
    schedule["_official_et_date"] = schedule.scheduled_start_time_utc.map(
        lambda value: (
            value.to_pydatetime().astimezone(NHL_OPERATIONAL_TZ).date().isoformat()
            if pd.notna(value) else None
        )
    )
    quote_rows, binding_rows, raw_rows = [], [], []
    events = envelope.get("provider_response") or []
    # Provider event identity is the ordered team pair on its NHL business
    # date (ET). Start time is diagnostic for a unique identity and only helps
    # disambiguate when the canonical schedule itself has duplicate pairs.
    event_pairs: dict[tuple[str, str, str], list[tuple[int, pd.Timestamp]]] = {}
    for index, item in enumerate(events):
        commence_value = pd.to_datetime(item.get("commence_time"), utc=True, errors="coerce")
        home_value, away_value = team_code(item.get("home_team")), team_code(item.get("away_team"))
        if pd.notna(commence_value) and home_value and away_value:
            et_date = commence_value.to_pydatetime().astimezone(NHL_OPERATIONAL_TZ).date().isoformat()
            key = (et_date, away_value, home_value)
            event_pairs.setdefault(key, []).append((index, commence_value))
    for event_index, event in enumerate(events):
        event_id = str(event.get("id") or "")
        commence = pd.to_datetime(event.get("commence_time"), utc=True, errors="coerce")
        home_code, away_code = team_code(event.get("home_team")), team_code(event.get("away_team"))
        normalization_flags = sorted({
            flag for flag in (
                team_name_normalization_flag(event.get("home_team", "")),
                team_name_normalization_flag(event.get("away_team", "")),
            ) if flag != "NONE"
        })
        provider_et_date = (
            commence.to_pydatetime().astimezone(NHL_OPERATIONAL_TZ).date().isoformat()
            if pd.notna(commence) else None
        )
        pair_candidates = schedule[
            schedule.home_team.eq(home_code) & schedule.away_team.eq(away_code)
        ] if home_code and away_code else schedule.iloc[0:0]
        same_day_candidates = pair_candidates[
            pair_candidates._official_et_date.eq(provider_et_date)
            & pair_candidates.slate_date.astype(str).eq(provider_et_date)
        ] if provider_et_date else schedule.iloc[0:0]
        reversed_candidates = schedule[
            schedule.home_team.eq(away_code) & schedule.away_team.eq(home_code)
        ] if home_code and away_code else schedule.iloc[0:0]
        game = None
        binding_classification = "NO_PROVIDER_EVENT"
        binding_reason = "NO_PROVIDER_EVENT"
        if not pd.notna(commence):
            binding_classification = "MALFORMED_PROVIDER_EVENT_DATE"
            binding_reason = "MALFORMED_PROVIDER_EVENT_DATE"
        elif len(same_day_candidates) == 1:
            pair_key = (provider_et_date, away_code, home_code)
            pair_events = event_pairs.get(pair_key, [])
            if len(pair_events) > 1:
                distances = [
                    abs((candidate_time - same_day_candidates.iloc[0].scheduled_start_time_utc).total_seconds())
                    for _, candidate_time in pair_events
                ]
                minimum_distance = min(distances)
                nearest_indices = [
                    pair_events[position][0] for position, distance in enumerate(distances)
                    if distance == minimum_distance
                ]
                if len(nearest_indices) == 1 and event_index == nearest_indices[0]:
                    game = same_day_candidates.iloc[0]
                    binding_classification = "EXACT_IDENTITY_MATCH"
                    binding_reason = "ET_TEAM_PAIR_DISAMBIGUATED_BY_NEAREST_PROVIDER_START"
                else:
                    binding_classification = "AMBIGUOUS_TEAM_PAIR"
                    binding_reason = "MULTIPLE_PROVIDER_EVENTS_NOT_UNIQUELY_DISAMBIGUATED"
            else:
                game = same_day_candidates.iloc[0]
                binding_classification = "EXACT_IDENTITY_MATCH"
                binding_reason = "ET_SLATE_DATE_AND_ORDERED_TEAM_PAIR_MATCH"
        elif len(same_day_candidates) > 1:
            deltas = (same_day_candidates.scheduled_start_time_utc - commence).abs()
            nearest = deltas.min()
            nearest_candidates = same_day_candidates.loc[deltas.eq(nearest)]
            if len(nearest_candidates) == 1 and len(event_pairs.get((provider_et_date, away_code, home_code), [])) == 1:
                game = nearest_candidates.iloc[0]
                binding_classification = "EXACT_IDENTITY_MATCH"
                binding_reason = "ET_TEAM_PAIR_DISAMBIGUATED_BY_NEAREST_START"
            else:
                binding_classification = "AMBIGUOUS_TEAM_PAIR"
                binding_reason = "CANONICAL_TEAM_PAIR_NOT_UNIQUELY_DISAMBIGUATED"
        elif len(pair_candidates):
            binding_classification = "DATE_MISMATCH"
            binding_reason = "PROVIDER_ET_DATE_DOES_NOT_MATCH_CANONICAL_SLATE_DATE"
        elif len(reversed_candidates):
            binding_classification = "ORIENTATION_MISMATCH"
            binding_reason = "REVERSED_ORDERED_TEAM_PAIR"
        signed_delta_minutes = (
            float((commence - game.scheduled_start_time_utc).total_seconds() / 60)
            if game is not None else pd.NA
        )
        delta_minutes = abs(signed_delta_minutes) if game is not None else pd.NA
        time_warning = bool(game is not None and delta_minutes > TIME_WARNING_MINUTES)
        if game is not None and time_warning:
            binding_classification = "EXACT_IDENTITY_MATCH_WITH_TIME_WARNING"
            binding_reason += ";START_TIME_DELTA_EXCEEDS_WARNING_LEVEL"
        status = "BOUND" if game is not None else "UNMATCHED_OR_AMBIGUOUS"
        game_start_utc = game.scheduled_start_time_utc if game is not None else pd.NaT
        provider_start_et = commence.tz_convert("America/New_York") if pd.notna(commence) else pd.NaT
        official_start_et = game_start_utc.tz_convert("America/New_York") if pd.notna(game_start_utc) else pd.NaT
        binding_rows.append({
            "provider_event_id": event_id, "participant_home_raw": event.get("home_team"),
            "participant_away_raw": event.get("away_team"), "normalized_home_team": home_code,
            "normalized_away_team": away_code,
            "team_name_normalization": ",".join(normalization_flags) or "NONE",
            "provider_commence_time_utc": event.get("commence_time"),
            "provider_start_et": provider_start_et.isoformat() if pd.notna(provider_start_et) else None,
            "provider_et_slate_date": provider_et_date,
            "official_start_utc": game_start_utc.isoformat() if pd.notna(game_start_utc) else None,
            "official_start_et": official_start_et.isoformat() if pd.notna(official_start_et) else None,
            "start_delta_minutes": delta_minutes,
            "provider_minus_official_start_minutes": signed_delta_minutes,
            "time_warning": time_warning,
            "binding_classification": binding_classification,
            "candidate_games": len(same_day_candidates), "binding_status": status,
            "binding_reason": binding_reason,
            "canonical_game_id": int(game.game_id) if game is not None else pd.NA,
        })
        for book in event.get("bookmakers") or []:
            for market in book.get("markets") or []:
                market_key = market.get("key")
                if market_key not in MARKETS:
                    continue
                update = pd.to_datetime(market.get("last_update") or book.get("last_update"), utc=True, errors="coerce")
                raw_rows.append({
                    "provider_event_id": event_id, "sportsbook_key": book.get("key"),
                    "sportsbook_name": book.get("title"), "provider_market_key": market_key,
                    "provider_last_update_utc": update.isoformat() if pd.notna(update) else None,
                    "raw_outcomes_json": canonical_json(market.get("outcomes") or []),
                    "raw_event_sha256": digest_value(event),
                })
                for outcome in market.get("outcomes") or []:
                    side_code = team_code(outcome.get("name"))
                    point = pd.to_numeric(outcome.get("point"), errors="coerce")
                    standard_spread = market_key == "h2h" or abs(point) == 1.5
                    if not standard_spread:
                        continue
                    price = pd.to_numeric(outcome.get("price"), errors="coerce")
                    timing_ok = bool(
                        game is not None and pd.notna(update) and pd.notna(price)
                        and update < game.scheduled_start_time_utc and capture < game.scheduled_start_time_utc
                    )
                    qualification = "PREGAME_QUALIFIED" if timing_ok else (
                        "GAME_BINDING_FAILED" if game is None else
                        "TIMESTAMP_MISSING" if pd.isna(update) else "POST_START_INVALID"
                    )
                    quote_rows.append({
                        "provider_event_id": event_id,
                        "provider_commence_time_utc": event.get("commence_time"),
                        "canonical_season": int(game.canonical_season) if game is not None else pd.NA,
                        "game_id": int(game.game_id) if game is not None else pd.NA,
                        "game_type_code": int(game.game_type_code) if game is not None else pd.NA,
                        "scheduled_start_time_utc": game.scheduled_start_time_utc.isoformat() if game is not None else None,
                        "participant_home_raw": event.get("home_team"), "participant_away_raw": event.get("away_team"),
                        "home_team": home_code, "away_team": away_code,
                        "sportsbook_key": book.get("key"), "sportsbook_name": book.get("title"),
                        "betonline_available": book.get("key") == "betonlineag",
                        "market_type": "FULL_GAME_MONEYLINE" if market_key == "h2h" else "STANDARD_PUCK_LINE",
                        "provider_market_key": market_key, "side_team": side_code,
                        "side_orientation": "HOME" if side_code == home_code else "AWAY" if side_code == away_code else "UNBOUND",
                        "point": point if pd.notna(point) else pd.NA, "american_price": float(price) if pd.notna(price) else pd.NA,
                        "decimal_price": american_to_decimal(float(price)) if pd.notna(price) else pd.NA,
                        "source_update_timestamp_utc": update.isoformat() if pd.notna(update) else None,
                        "observation_timestamp_utc": capture.isoformat(), "qualification_status": qualification,
                        "shadow_only": True, "wager_recommendation": "NONE",
                    })
    quote_columns = [
        "provider_event_id", "provider_commence_time_utc", "canonical_season", "game_id", "game_type_code",
        "scheduled_start_time_utc", "participant_home_raw", "participant_away_raw",
        "home_team", "away_team", "sportsbook_key", "sportsbook_name",
        "betonline_available", "market_type", "provider_market_key", "side_team",
        "side_orientation", "point", "american_price", "decimal_price",
        "source_update_timestamp_utc", "observation_timestamp_utc",
        "qualification_status", "shadow_only", "wager_recommendation",
    ]
    binding_columns = [
        "provider_event_id", "participant_home_raw", "participant_away_raw",
        "normalized_home_team", "normalized_away_team", "team_name_normalization",
        "provider_commence_time_utc", "provider_start_et", "provider_et_slate_date",
        "official_start_utc", "official_start_et", "start_delta_minutes",
        "provider_minus_official_start_minutes", "time_warning",
        "binding_classification", "candidate_games", "binding_status", "binding_reason",
        "canonical_game_id",
    ]
    raw_columns = [
        "provider_event_id", "sportsbook_key", "sportsbook_name", "provider_market_key",
        "provider_last_update_utc", "raw_outcomes_json", "raw_event_sha256",
    ]
    quotes = pd.DataFrame(quote_rows, columns=quote_columns)
    if len(quotes):
        quotes["raw_implied_probability"] = 1 / quotes.decimal_price
        quotes["no_vig_probability"] = np.nan
        eligible = quotes[quotes.qualification_status.eq("PREGAME_QUALIFIED") & quotes.market_type.eq("FULL_GAME_MONEYLINE")]
        for _, group in eligible.groupby(["game_id", "sportsbook_key", "source_update_timestamp_utc"]):
            if len(group) == 2 and set(group.side_orientation) == {"HOME", "AWAY"}:
                total = group.raw_implied_probability.sum()
                quotes.loc[group.index, "no_vig_probability"] = group.raw_implied_probability / total
        quotes["observation_identity_sha256"] = quotes.apply(lambda row: digest_value({
            "provider_event_id": row.provider_event_id,
            "game_id": row.game_id,
            "sportsbook_key": row.sportsbook_key,
            "market_type": row.market_type,
            "side_orientation": row.side_orientation,
            "point": row.point,
            "american_price": row.american_price,
            "source_update_timestamp_utc": row.source_update_timestamp_utc,
            "qualification_status": row.qualification_status,
        }), axis=1)
    else:
        quotes["raw_implied_probability"] = pd.Series(dtype=float)
        quotes["no_vig_probability"] = pd.Series(dtype=float)
        quotes["observation_identity_sha256"] = pd.Series(dtype=str)
    return (
        quotes,
        pd.DataFrame(binding_rows, columns=binding_columns),
        pd.DataFrame(raw_rows, columns=raw_columns),
    )


def build_price_references(root: Path, current_quotes: pd.DataFrame, season: int, slate_date: str) -> pd.DataFrame:
    """Derive truthful first-seen/latest-prestart references without mutating observations."""
    frames: list[pd.DataFrame] = []
    slate_root = root / f"season={season}" / f"slate_date={slate_date}"
    for path in sorted(slate_root.glob("run_type=*/state=*/normalized_market_observations.csv")):
        frame = pd.read_csv(path)
        if len(frame):
            frames.append(frame)
    if len(current_quotes):
        frames.append(current_quotes.copy())
    columns = list(current_quotes.columns) + ["reference_label"]
    if not frames:
        return pd.DataFrame(columns=columns)
    combined = pd.concat(frames, ignore_index=True)
    combined = combined[combined.qualification_status.eq("PREGAME_QUALIFIED")].copy()
    if not len(combined):
        return pd.DataFrame(columns=columns)
    identity = ["game_id", "sportsbook_key", "market_type", "side_orientation", "point"]
    combined = combined.drop_duplicates("observation_identity_sha256", keep="first")
    combined["_source_time"] = pd.to_datetime(combined.source_update_timestamp_utc, utc=True)
    combined["_observed_time"] = pd.to_datetime(combined.observation_timestamp_utc, utc=True)
    combined = combined.sort_values(identity + ["_source_time", "_observed_time"], kind="mergesort")
    first = combined.groupby(identity, dropna=False, sort=False).head(1).assign(reference_label="FIRST_SEEN")
    latest = combined.groupby(identity, dropna=False, sort=False).tail(1).assign(reference_label="LATEST_PRESTART")
    return pd.concat([first, latest], ignore_index=True).drop(columns=["_source_time", "_observed_time"])


def build_coverage_history(root: Path, current: pd.DataFrame, season: int, slate_date: str) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    slate_root = root / f"season={season}" / f"slate_date={slate_date}"
    for path in sorted(slate_root.glob("run_type=*/state=*/cross_market_game_state_observations.csv")):
        frame = pd.read_csv(path)
        if len(frame):
            frames.append(frame)
    frames.append(current.copy())
    history = pd.concat(frames, ignore_index=True)
    return history.drop_duplicates(
        ["canonical_season", "game_id", "observation_timestamp_utc"], keep="first"
    ).sort_values(["game_id", "observation_timestamp_utc"], kind="mergesort")


def quota_estimate(regions: tuple[str, ...] = REGIONS, markets: tuple[str, ...] = MARKETS) -> dict[str, Any]:
    estimate = len(regions) * len(markets)
    return {
        "http_request_count": 1, "market_count": len(markets), "region_count": len(regions),
        "estimated_credit_upper_bound": estimate,
        "within_request_bound": 1 <= MAX_REQUESTS_PER_RUN,
        "within_credit_bound": estimate <= MAX_ESTIMATED_CREDITS_PER_RUN,
    }


def fetch_markets(api_key: str, output: Path, regions: tuple[str, ...] = REGIONS) -> Path:
    if not api_key:
        raise RuntimeError("ODDS_API_CREDENTIAL_MISSING_FAIL_CLOSED")
    if output.exists():
        raise FileExistsError("OVERWRITE_ATTEMPT_BLOCKED")
    estimate = quota_estimate(regions=regions)
    if not estimate["within_request_bound"] or not estimate["within_credit_bound"]:
        raise RuntimeError("QUOTA_ESTIMATE_EXCEEDS_BOUND")
    query = urllib.parse.urlencode({
        "apiKey": api_key, "regions": ",".join(regions), "markets": ",".join(MARKETS),
        "oddsFormat": "american", "dateFormat": "iso",
    })
    request = urllib.request.Request(
        "https://api.the-odds-api.com/v4/sports/icehockey_nhl/odds?" + query,
        headers={"User-Agent": "proppadia-nhl-cross-market-shadow/1.0"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        body = response.read()
        headers = {key.lower(): value for key, value in response.headers.items()}
        if response.status != 200:
            raise RuntimeError(f"ODDS_API_HTTP_{response.status}")
    captured = utc_now()
    actual = pd.to_numeric(headers.get("x-requests-last"), errors="coerce")
    envelope = {
        "capture_timestamp_utc": captured, "provider": "THE_ODDS_API",
        "request_metadata": {
            "sport": "icehockey_nhl", "regions": list(regions), "markets": list(MARKETS),
            "credential_present": True, "credential_persisted": False,
            "request_url_redacted": "https://api.the-odds-api.com/v4/sports/icehockey_nhl/odds?apiKey=<REDACTED>",
        },
        "quota": {
            **estimate, "credits_consumed": int(actual) if pd.notna(actual) else estimate["estimated_credit_upper_bound"],
            "credits_consumed_source": "x-requests-last" if pd.notna(actual) else "CONSERVATIVE_ESTIMATE",
            "requests_remaining": headers.get("x-requests-remaining"),
            "requests_used": headers.get("x-requests-used"),
        },
        "provider_response": json.loads(body),
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(canonical_json(envelope) + "\n")
    return output


def _coverage(schedule: pd.DataFrame, predictions: pd.DataFrame, quotes: pd.DataFrame,
              sog: pd.DataFrame | None, points: pd.DataFrame | None, saves: pd.DataFrame | None,
              observation_time: str) -> pd.DataFrame:
    base = schedule[["canonical_season", "slate_date", "game_id", "game_type_code", "game_type_label", "home_team", "away_team"]].copy()
    base["observation_timestamp_utc"] = observation_time
    base["v2_prediction_available"] = base.game_id.isin(predictions.game_id)
    qualified = quotes[quotes.qualification_status.eq("PREGAME_QUALIFIED")] if len(quotes) else quotes
    base["moneyline_market_available"] = base.game_id.isin(qualified.loc[qualified.market_type.eq("FULL_GAME_MONEYLINE"), "game_id"]) if len(qualified) else False
    base["puck_line_market_available"] = base.game_id.isin(qualified.loc[qualified.market_type.eq("STANDARD_PUCK_LINE"), "game_id"]) if len(qualified) else False
    for family, frame in [("sog", sog), ("points", points), ("saves", saves)]:
        if frame is None or not len(frame) or "game_id" not in frame:
            base[f"{family}_strict_prior_available"] = False
            base[f"{family}_coverage_state"] = "NOT_OBSERVED"
            continue
        available = frame.groupby("game_id").size()
        base[f"{family}_strict_prior_available"] = base.game_id.isin(available.index)
        base[f"{family}_coverage_state"] = np.where(base[f"{family}_strict_prior_available"], "OBSERVED", "NOT_OBSERVED")
        keep = [column for column in frame.columns if column != "game_id" and (
            column.startswith(family + "_") or column in {"snapshot_timestamp_utc", "coverage_quality_state"}
        )]
        if keep and not frame.game_id.duplicated().any():
            renamed = {column: f"{family}_{column}" if not column.startswith(family + "_") else column for column in keep}
            base = base.merge(frame[["game_id"] + keep].rename(columns=renamed), on="game_id", how="left", validate="one_to_one")
    base["points_classification"] = "PARTIAL_NO_STABLE_ORDERED_RELATIONSHIP"
    base["saves_classification"] = "PARTIAL_FRAGILE_STARTER_UNCONFIRMED"
    base["confirmed_goalie_inferred"] = False
    return base


def write_manifest(path: Path) -> None:
    files = sorted(item for item in path.iterdir() if item.is_file() and item.name != "SHA256SUMS")
    (path / "SHA256SUMS").write_text("".join(f"{sha256(item)}  {item.name}\n" for item in files))


def verify_manifest(path: Path) -> None:
    for line in (path / "SHA256SUMS").read_text().splitlines():
        expected, name = line.split("  ", 1)
        if sha256(path / name) != expected:
            raise RuntimeError("MANIFEST_MISMATCH")


def _run_nonblocking_challenger(callback):
    """Keep optional challenger failures out of the frozen control capture."""
    try:
        return callback(), {"status": "COMPLETE", "failure": None}
    except Exception as error:
        return None, {
            "status": "FAILED_NONBLOCKING",
            "failure": f"{type(error).__name__}:{error}",
        }


def _run_capture_unlocked(schedule_csv: Path, history_csv: Path, odds_json: Path | None, root: Path,
                          slate_date: str, run_timestamp_utc: str, run_type: str,
                          sog_csv: Path | None = None, points_csv: Path | None = None,
                          saves_csv: Path | None = None, canary_mode: bool = False) -> Path:
    activation = load_activation()
    if not canary_mode and not activation.get("capture_enabled"):
        raise RuntimeError("SHADOW_CAPTURE_READY_NOT_ACTIVATED")
    schedule = normalize_game_types(pd.read_csv(schedule_csv))
    history = normalize_game_types(pd.read_csv(history_csv))
    if not schedule.slate_date.astype(str).eq(slate_date).all():
        raise ValueError("SLATE_DATE_MISMATCH")
    if run_type not in {"MIDDAY", "FINAL_PREGAME", "REFRESH"}:
        raise ValueError("RUN_TYPE_INVALID")
    if canary_mode and (
        not schedule.game_type_code.eq(1).all()
        or not (PRESEASON_START <= slate_date < REGULAR_SEASON_START)
    ):
        raise RuntimeError("PRESEASON_CANARY_SCOPE_VIOLATION")
    predictions, timing = build_v2_predictions(schedule, history, run_timestamp_utc)
    puck_predictions = build_puck_line_predictions(predictions)
    puck_v2_predictions = pd.DataFrame()
    puck_v2_provenance = pd.DataFrame()
    puck_v2_lane = {"status": "NOT_APPLICABLE", "failure": None}
    moneyline_challenger_lane = {"status": "SKIPPED_DEPENDENCY_OR_NOT_APPLICABLE", "failure": None}
    if schedule.game_type_code.eq(2).any():
        def build_puck_line_challenger():
            prior = load_verified_2025_shot_prior()
            candidate_input, provenance = build_shot_prior_challenger(
                schedule, history, predictions, prior[0],
                prior_source_sha256=prior[1], prior_outcome_sha256=prior[2],
            )
            candidate_predictions = build_puck_line_predictions(candidate_input)
            candidate_predictions["control_name"] = CHALLENGER_NAME
            candidate_predictions["feature_policy_version"] = POLICY_VERSION
            candidate_predictions["control_model_sha256"] = load_puck_parameters()["model_sha256"]
            candidate_predictions["substantive_prediction_sha256"] = candidate_predictions.apply(
                lambda row: digest_value({
                    "game_id": int(row.game_id), "canonical_season": int(row.canonical_season),
                    "feature_policy_version": POLICY_VERSION,
                    "features": {feature: row[feature] for feature in FEATURES},
                    "control_model_sha256": row.control_model_sha256,
                }), axis=1,
            )
            return prior, candidate_input, provenance, candidate_predictions

        puck_result, puck_v2_lane = _run_nonblocking_challenger(build_puck_line_challenger)
        if puck_result is not None:
            (prior_result, challenger_input, puck_v2_provenance,
             puck_v2_predictions) = puck_result
            prior_source, prior_source_hash, prior_outcome_hash = prior_result
    if odds_json:
        envelope = json.loads(odds_json.read_text())
    else:
        envelope = {
            "capture_timestamp_utc": run_timestamp_utc, "provider": "NO_MARKET_FIXTURE",
            "quota": {**quota_estimate(), "credits_consumed": 0, "requests_remaining": None},
            "provider_response": [],
        }
    quotes, bindings, raw_observations = normalize_markets(envelope, schedule)
    moneyline_challenger_predictions = pd.DataFrame()
    moneyline_challenger_provenance = pd.DataFrame()
    moneyline_challenger_ledger = pd.DataFrame(columns=["game_id", "outcome_status"])
    if len(puck_v2_predictions):
        def build_moneyline_candidate():
            candidate, provenance, _ = build_moneyline_challenger(
                predictions, challenger_input, puck_v2_provenance, prior_source,
                prior_team_source_sha256=prior_source_hash,
                prior_outcome_source_sha256=prior_outcome_hash,
                schedule_source_sha256=sha256(schedule_csv),
                history_source_sha256=sha256(history_csv),
                odds_source_sha256=sha256(odds_json) if odds_json else "NO_MARKET",
            )
            candidate_ledger = make_moneyline_challenger_ledger(candidate, quotes)
            candidate_ledger["slate_date"] = slate_date
            candidate_ledger["market_observation_timestamp_utc"] = envelope.get("capture_timestamp_utc")
            v2_by_game = predictions.set_index("game_id")
            candidate_ledger["v2_side"] = candidate_ledger.game_id.map(v2_by_game.model_favored_team)
            candidate_ledger["v2_home_win_probability"] = candidate_ledger.game_id.map(v2_by_game.v2_home_win_probability)
            candidate_ledger["v2_probability"] = candidate_ledger.v2_home_win_probability
            candidate_ledger["challenger_side"] = candidate_ledger.challenger_favored_team
            candidate_ledger["probability_delta"] = candidate_ledger.probability_delta_challenger_minus_v2
            candidate_ledger["current_history_depth_home"] = candidate_ledger.game_id.map(
                puck_v2_provenance.set_index("game_id").current_home_games
            )
            candidate_ledger["current_history_depth_away"] = candidate_ledger.game_id.map(
                puck_v2_provenance.set_index("game_id").current_away_games
            )
            candidate_ledger["outcome_status"] = "PENDING_OFFICIAL_RECONCILIATION"
            return candidate, provenance, candidate_ledger

        moneyline_result, moneyline_challenger_lane = _run_nonblocking_challenger(
            build_moneyline_candidate)
        if moneyline_result is not None:
            (moneyline_challenger_predictions, moneyline_challenger_provenance,
             moneyline_challenger_ledger) = moneyline_result
    if len(puck_v2_predictions):
        standard = quotes[
            quotes.qualification_status.eq("PREGAME_QUALIFIED")
            & quotes.market_type.eq("STANDARD_PUCK_LINE")
        ].copy()
        market_columns = [c for c in ["game_id", "side_orientation", "point", "american_price", "source_update_timestamp_utc", "sportsbook_key"] if c in standard]
        ledger = puck_v2_predictions.merge(
            puck_predictions[["game_id", "away_by_2_plus_probability", "one_goal_game_probability", "home_by_2_plus_probability"]].rename(columns={
                "away_by_2_plus_probability": "v1_away_by_2_plus_probability",
                "one_goal_game_probability": "v1_one_goal_game_probability",
                "home_by_2_plus_probability": "v1_home_by_2_plus_probability",
            }), on="game_id", validate="one_to_one",
        )
        ledger = ledger.merge(standard[market_columns], on="game_id", how="left", validate="one_to_many")
        ledger["market_observation_timestamp_utc"] = envelope.get("capture_timestamp_utc")
        ledger["v1_selected_class"] = ledger[[
            "v1_away_by_2_plus_probability", "v1_one_goal_game_probability", "v1_home_by_2_plus_probability"
        ]].idxmax(axis=1).map({
            "v1_away_by_2_plus_probability": "AWAY_BY_2_PLUS",
            "v1_one_goal_game_probability": "ONE_GOAL_GAME",
            "v1_home_by_2_plus_probability": "HOME_BY_2_PLUS",
        })
        ledger["v2_selected_class"] = ledger[[
            "away_by_2_plus_probability", "one_goal_game_probability", "home_by_2_plus_probability"
        ]].idxmax(axis=1).map({
            "away_by_2_plus_probability": "AWAY_BY_2_PLUS",
            "one_goal_game_probability": "ONE_GOAL_GAME",
            "home_by_2_plus_probability": "HOME_BY_2_PLUS",
        })
        ledger["class_changed"] = ledger.v1_selected_class.ne(ledger.v2_selected_class)
        provenance_columns = ["game_id", "v1_diff_std_shot_diff_pg", "diff_std_shot_diff_pg"]
        ledger = ledger.merge(
            puck_v2_provenance[provenance_columns].rename(columns={
                "v1_diff_std_shot_diff_pg": "v1_shot_control_value",
                "diff_std_shot_diff_pg": "v2_shot_challenger_value",
            }), on="game_id", validate="many_to_one",
        )
        for label in ("away_by_2_plus", "one_goal_game", "home_by_2_plus"):
            ledger[f"{label}_probability_delta_v2_minus_v1"] = (
                ledger[f"{label}_probability"] - ledger[f"v1_{label}_probability"]
            )
        ledger["outcome_status"] = "PENDING_OFFICIAL_RECONCILIATION"
        ledger["wager_recommendation"] = "NONE_SHADOW_ONLY"
    else:
        ledger = pd.DataFrame(columns=["game_id", "outcome_status"])
    price_references = build_price_references(root, quotes, SEASON, slate_date)
    read_optional = lambda path: pd.read_csv(path) if path else None
    coverage = _coverage(
        schedule, predictions, quotes, read_optional(sog_csv), read_optional(points_csv),
        read_optional(saves_csv), envelope["capture_timestamp_utc"],
    )
    coverage_history = build_coverage_history(root, coverage, SEASON, slate_date)
    substantive = {
        "schedule": schedule.sort_values("game_id")[["game_id", "scheduled_start_time_utc", "home_team", "away_team", "game_type_code"]].astype(str).to_dict("records"),
        "predictions": sorted(predictions.substantive_prediction_sha256.astype(str)),
        "puck_predictions": sorted(puck_predictions.substantive_prediction_sha256.astype(str)),
        "puck_v2_predictions": sorted(puck_v2_predictions.substantive_prediction_sha256.astype(str)) if len(puck_v2_predictions) else [],
        "moneyline_challenger_predictions": sorted(
            moneyline_challenger_predictions.apply(lambda row: digest_value({
                "game_id": int(row.game_id), "model_version": MONEYLINE_CHALLENGER_NAME,
                "feature_policy_version": MONEYLINE_FEATURE_POLICY_VERSION,
                "features": {feature: row[feature] for feature in [
                    "diff_std_goal_diff_pg", "diff_r10_goal_diff_pg",
                    "diff_prior_blended_shot_diff_pg", "diff_days_rest",
                    "home_back_to_back", "away_back_to_back",
                    "shrunk_prior_finishing_residual_gap",
                ]}, "probability": float(row.challenger_home_win_probability),
                "parameter_sha256": row.model_parameter_sha256,
            }), axis=1).astype(str)
        ) if len(moneyline_challenger_predictions) else [],
        "markets": quotes.sort_values(["game_id", "sportsbook_key", "market_type", "side_orientation"], na_position="last")[[
            "provider_event_id", "game_id", "sportsbook_key", "market_type", "side_orientation", "point",
            "american_price", "source_update_timestamp_utc", "qualification_status",
        ]].astype(str).to_dict("records") if len(quotes) else [],
        "coverage": coverage.drop(columns=["observation_timestamp_utc"], errors="ignore").astype(str).to_dict("records"),
        "run_type": run_type,
    }
    # Manual intraday refreshes are observations, not idempotent reruns. Keep
    # identical market payloads captured at different times as distinct states.
    if run_type == "REFRESH":
        substantive["capture_identity_timestamp_utc"] = parse_utc(run_timestamp_utc).isoformat()
    substantive_hash = digest_value(substantive)
    destination = root / f"season={SEASON}" / f"slate_date={slate_date}" / f"run_type={run_type}" / f"state={substantive_hash}"
    if destination.is_dir():
        verify_manifest(destination)
        return destination
    destination.mkdir(parents=True, exist_ok=False)
    schedule.to_csv(destination / "schedule_event_identity.csv", index=False)
    predictions.to_csv(destination / "v2_immutable_predictions.csv", index=False)
    puck_predictions.to_csv(destination / "puck_line_v1_immutable_predictions.csv", index=False)
    if len(puck_v2_predictions):
        puck_v2_predictions.to_csv(destination / "puck_line_v2_shot_prior_challenger_predictions.csv", index=False)
        puck_v2_provenance.to_csv(destination / "puck_line_v2_shot_prior_feature_provenance.csv", index=False)
        ledger.to_csv(destination / "puck_line_v2_shot_prior_prospective_ledger.csv", index=False)
    if len(moneyline_challenger_predictions):
        moneyline_challenger_predictions.to_csv(destination / "moneyline_shot_finishing_challenger_v3_predictions.csv", index=False)
        moneyline_challenger_provenance.to_csv(destination / "moneyline_shot_finishing_challenger_v3_feature_provenance.csv", index=False)
        moneyline_challenger_ledger.to_csv(destination / "moneyline_shot_finishing_challenger_v3_ledger.csv", index=False)
    timing.to_csv(destination / "strict_prior_timing_audit.csv", index=False)
    (destination / "raw_market_response.json").write_text(canonical_json(envelope) + "\n")
    raw_observations.to_csv(destination / "raw_market_observations.csv", index=False)
    quotes.to_csv(destination / "normalized_market_observations.csv", index=False)
    price_references.to_csv(destination / "market_price_references.csv", index=False)
    bindings.to_csv(destination / "market_event_binding.csv", index=False)
    coverage.to_csv(destination / "cross_market_game_state_observations.csv", index=False)
    coverage_history.to_csv(destination / "coverage_history.csv", index=False)
    pd.DataFrame(columns=["game_id", "outcome_status"]).to_csv(destination / "canonical_outcomes.csv", index=False)
    pd.DataFrame(columns=["game_id", "grading_status"]).to_csv(destination / "graded_moneyline_shadow_results.csv", index=False)
    pd.DataFrame(columns=["game_id", "grading_status", "hypothetical_result"]).to_csv(destination / "graded_puck_line_market_results.csv", index=False)
    qualified = quotes[quotes.qualification_status.eq("PREGAME_QUALIFIED")] if len(quotes) else quotes
    status = {
        "mode": "SHADOW_RESEARCH_ONLY", "slate_date": slate_date, "run_type": run_type,
        "run_timestamp_utc": parse_utc(run_timestamp_utc).isoformat(), "substantive_state_sha256": substantive_hash,
        "scheduled_games": len(schedule), "v2_predictions_created": len(predictions),
        "puck_line_v1_predictions_created": len(puck_predictions),
        "puck_line_v2_shot_prior_predictions_created": len(puck_v2_predictions),
        "puck_line_v2_shot_prior_feature_policy": POLICY_VERSION if len(puck_v2_predictions) else None,
        "moneyline_shot_finishing_challenger_v3_predictions_created": len(moneyline_challenger_predictions),
        "moneyline_shot_finishing_challenger_v3_model": MONEYLINE_CHALLENGER_NAME if len(moneyline_challenger_predictions) else None,
        "moneyline_shot_finishing_challenger_v3_feature_policy": MONEYLINE_FEATURE_POLICY_VERSION if len(moneyline_challenger_predictions) else None,
        "puck_line_v2_challenger_lane": puck_v2_lane,
        "moneyline_challenger_lane": moneyline_challenger_lane,
        "puck_line_v1_v2_class_changes": int(ledger.drop_duplicates("game_id").class_changed.sum()) if len(ledger) else 0,
        "puck_line_v1_v2_mean_absolute_probability_delta": float(pd.concat([
            ledger.drop_duplicates("game_id")[f"{label}_probability_delta_v2_minus_v1"].abs()
            for label in ("away_by_2_plus", "one_goal_game", "home_by_2_plus")
        ]).mean()) if len(ledger) else None,
        "moneyline_games_captured": int(qualified.loc[qualified.market_type.eq("FULL_GAME_MONEYLINE"), "game_id"].nunique()) if len(qualified) else 0,
        "moneyline_books_captured": int(qualified.loc[qualified.market_type.eq("FULL_GAME_MONEYLINE"), "sportsbook_key"].nunique()) if len(qualified) else 0,
        "puck_line_games_captured": int(qualified.loc[qualified.market_type.eq("STANDARD_PUCK_LINE"), "game_id"].nunique()) if len(qualified) else 0,
        "puck_line_books_captured": int(qualified.loc[qualified.market_type.eq("STANDARD_PUCK_LINE"), "sportsbook_key"].nunique()) if len(qualified) else 0,
        "unmatched_events": int(bindings.binding_status.ne("BOUND").sum()) if len(bindings) else 0,
        "remaining_api_credits": (envelope.get("quota") or {}).get("requests_remaining"),
        "credits_consumed": (envelope.get("quota") or {}).get("credits_consumed", 0),
        "preseason_records": int(schedule.game_type_code.eq(1).sum()),
        "regular_season_evaluation_records": int(predictions.regular_season_evaluation_eligible.sum()),
        "wagers_selected": 0, "production_writes": 0,
    }
    (destination / "daily_execution_status.json").write_text(json.dumps(status, indent=2, sort_keys=True) + "\n")
    write_manifest(destination)
    return destination


def run_capture(schedule_csv: Path, history_csv: Path, odds_json: Path | None, root: Path,
                slate_date: str, run_timestamp_utc: str, run_type: str,
                sog_csv: Path | None = None, points_csv: Path | None = None,
                saves_csv: Path | None = None, canary_mode: bool = False) -> Path:
    """Serialize a slate capture and release its lock on every return/exception path."""
    lock_path = root / ".locks" / f"{slate_date}.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError("CROSS_MARKET_SHADOW_CAPTURE_ALREADY_RUNNING") from error
        return _run_capture_unlocked(
            schedule_csv, history_csv, odds_json, root, slate_date, run_timestamp_utc, run_type,
            sog_csv, points_csv, saves_csv, canary_mode,
        )


def grade_capture(run_dir: Path, outcomes_csv: Path, grade_root: Path, grading_time_utc: str) -> Path:
    verify_manifest(run_dir)
    predictions = pd.read_csv(run_dir / "v2_immutable_predictions.csv")
    puck_predictions = pd.read_csv(run_dir / "puck_line_v1_immutable_predictions.csv")
    quotes = pd.read_csv(run_dir / "normalized_market_observations.csv")
    references = pd.read_csv(run_dir / "market_price_references.csv")
    outcomes = pd.read_csv(outcomes_csv)
    required = {"canonical_season", "game_id", "final_home_goals", "final_away_goals", "game_status", "outcome_source", "outcome_source_timestamp_utc"}
    if required - set(outcomes) or outcomes.duplicated(["canonical_season", "game_id"]).any():
        raise ValueError("OUTCOME_SCHEMA_OR_GRAIN_INVALID")
    outcomes = outcomes[outcomes.game_status.astype(str).str.upper().isin(["FINAL", "OFF", "COMPLETED"])].copy()
    outcomes["actual_winner"] = np.where(outcomes.final_home_goals > outcomes.final_away_goals, "HOME", "AWAY")
    moneyline = predictions.merge(outcomes, on=["canonical_season", "game_id"], how="inner", validate="one_to_one")
    moneyline["correct"] = moneyline.model_favored_team.eq(np.where(moneyline.actual_winner.eq("HOME"), moneyline.home_team, moneyline.away_team))
    moneyline["probability_assigned_to_outcome"] = np.where(moneyline.actual_winner.eq("HOME"), moneyline.v2_home_win_probability, moneyline.v2_away_win_probability)
    moneyline["brier_contribution"] = (moneyline.v2_home_win_probability - moneyline.actual_winner.eq("HOME").astype(int)) ** 2
    moneyline["log_loss_contribution"] = -np.log(moneyline.probability_assigned_to_outcome.clip(1e-15, 1 - 1e-15))
    moneyline["grading_timestamp_utc"] = parse_utc(grading_time_utc).isoformat()
    moneyline["financial_claim"] = "NONE_SHADOW_ONLY"
    moneyline_challenger = pd.DataFrame()
    moneyline_paired = pd.DataFrame()
    moneyline_challenger_path = run_dir / "moneyline_shot_finishing_challenger_v3_predictions.csv"
    if moneyline_challenger_path.is_file():
        challenger_predictions = pd.read_csv(moneyline_challenger_path)
        moneyline_challenger = challenger_predictions.merge(
            outcomes, on=["canonical_season", "game_id"], how="inner", validate="one_to_one"
        )
        challenger_provenance_path = run_dir / "moneyline_shot_finishing_challenger_v3_feature_provenance.csv"
        if challenger_provenance_path.is_file():
            challenger_provenance = pd.read_csv(challenger_provenance_path)
            keep = ["game_id", "current_home_games", "current_away_games", "shrunk_finishing_residual_gap"]
            moneyline_challenger = moneyline_challenger.merge(
                challenger_provenance[keep], on="game_id", how="left", validate="one_to_one"
            )
        moneyline_challenger["actual_home_win"] = moneyline_challenger.actual_winner.eq("HOME").astype(int)
        moneyline_challenger["challenger_favored_side"] = np.where(
            moneyline_challenger.challenger_favored_team.eq(moneyline_challenger.home_team), "HOME", "AWAY"
        )
        moneyline_challenger["correct"] = moneyline_challenger.challenger_favored_side.eq(
            moneyline_challenger.actual_winner
        )
        moneyline_challenger["probability_assigned_to_outcome"] = np.where(
            moneyline_challenger.actual_home_win.eq(1),
            moneyline_challenger.challenger_home_win_probability,
            moneyline_challenger.challenger_away_win_probability,
        )
        moneyline_challenger["brier_contribution"] = (
            moneyline_challenger.challenger_home_win_probability - moneyline_challenger.actual_home_win
        ) ** 2
        moneyline_challenger["log_loss_contribution"] = -np.log(
            moneyline_challenger.probability_assigned_to_outcome.clip(1e-15, 1 - 1e-15)
        )
        moneyline_challenger["grading_timestamp_utc"] = parse_utc(grading_time_utc).isoformat()
        moneyline_challenger["financial_claim"] = "NONE_SHADOW_ONLY"
        moneyline_paired = moneyline[[
            "canonical_season", "game_id", "actual_winner", "correct",
            "brier_contribution", "log_loss_contribution", "v2_home_win_probability",
        ]].merge(moneyline_challenger[[
            "canonical_season", "game_id", "correct", "brier_contribution",
            "log_loss_contribution", "challenger_home_win_probability", "side_changed",
        ]], on=["canonical_season", "game_id"], how="inner", validate="one_to_one",
           suffixes=("_v2", "_challenger"))
        moneyline_paired["side_changed"] = moneyline_paired.side_changed.astype(bool)
        moneyline_paired["same_official_outcome_evidence"] = True
        moneyline_challenger["accuracy_contribution"] = moneyline_challenger.correct.astype(int)
        moneyline_challenger["history_depth_games"] = moneyline_challenger[[
            "current_home_games", "current_away_games",
        ]].max(axis=1)
        moneyline_challenger["history_depth_bucket"] = pd.cut(
            moneyline_challenger.history_depth_games, bins=[-1, 1, 3, 5, 10, float("inf")],
            labels=["0_1", "2_3", "4_5", "6_10", "OVER_10"],
        ).astype("string")
        moneyline_challenger["finishing_gap_bucket"] = pd.cut(
            moneyline_challenger.shrunk_prior_finishing_residual_gap,
            bins=[-float("inf"), -.1, 0, .1, float("inf")],
            labels=["BELOW_MINUS_0_1", "MINUS_0_1_TO_0", "0_TO_0_1", "ABOVE_0_1"],
        ).astype("string")
        moneyline_segment_rows = []
        for dimension in ("history_depth_bucket", "finishing_gap_bucket"):
            for segment, sub in moneyline_challenger.groupby(dimension, dropna=False, observed=False):
                if not len(sub):
                    continue
                y = sub.actual_home_win.astype(int)
                p = sub.challenger_home_win_probability.astype(float)
                moneyline_segment_rows.append({
                    "segment_dimension": dimension, "segment": str(segment), "games": len(sub),
                    "brier": float(sub.brier_contribution.mean()),
                    "log_loss": float(sub.log_loss_contribution.mean()),
                    "accuracy": float(sub.correct.mean()),
                    "auc": float(roc_auc_score(y, p)) if y.nunique() == 2 else None,
                    "ece": _binary_ece(sub.actual_home_win, sub.challenger_home_win_probability),
                })
        moneyline_segments = pd.DataFrame(moneyline_segment_rows)
    comparison = references[
        references.qualification_status.eq("PREGAME_QUALIFIED")
        & references.market_type.eq("FULL_GAME_MONEYLINE")
        & references.no_vig_probability.notna()
    ].merge(
        predictions[["canonical_season", "game_id", "v2_home_win_probability", "v2_away_win_probability"]],
        on=["canonical_season", "game_id"], how="inner", validate="many_to_one",
    ).merge(outcomes, on=["canonical_season", "game_id"], how="inner", validate="many_to_one")
    comparison["v2_side_probability"] = np.where(
        comparison.side_orientation.eq("HOME"),
        comparison.v2_home_win_probability,
        comparison.v2_away_win_probability,
    )
    comparison["v2_minus_market_probability_gap"] = (
        comparison.v2_side_probability - comparison.no_vig_probability
    )
    comparison["edge_or_wager_claim"] = "NONE_SHADOW_COMPARISON_ONLY"
    puck = quotes[
        quotes.qualification_status.eq("PREGAME_QUALIFIED") & quotes.market_type.eq("STANDARD_PUCK_LINE")
    ].merge(outcomes, on=["canonical_season", "game_id"], how="inner", validate="many_to_one").merge(
        puck_predictions[[
            "canonical_season", "game_id", "home_minus_1_5_cover_probability",
            "away_plus_1_5_cover_probability", "away_minus_1_5_cover_probability",
            "home_plus_1_5_cover_probability",
        ]], on=["canonical_season", "game_id"], how="inner", validate="many_to_one",
    )
    margin = puck.final_home_goals - puck.final_away_goals
    puck["standard_puck_line_result"] = np.where(
        puck.side_orientation.eq("HOME"), np.where(margin + puck.point > 0, "WIN", "LOSS"),
        np.where(-margin + puck.point > 0, "WIN", "LOSS"),
    )
    puck["model_cover_probability"] = np.select(
        [
            puck.side_orientation.eq("HOME") & puck.point.eq(-1.5),
            puck.side_orientation.eq("AWAY") & puck.point.eq(1.5),
            puck.side_orientation.eq("AWAY") & puck.point.eq(-1.5),
            puck.side_orientation.eq("HOME") & puck.point.eq(1.5),
        ],
        [
            puck.home_minus_1_5_cover_probability, puck.away_plus_1_5_cover_probability,
            puck.away_minus_1_5_cover_probability, puck.home_plus_1_5_cover_probability,
        ], default=np.nan,
    )
    puck["cover_actual"] = puck.standard_puck_line_result.eq("WIN").astype(int)
    puck["brier_contribution"] = (puck.model_cover_probability - puck.cover_actual) ** 2
    puck["log_loss_contribution"] = -np.log(np.where(
        puck.cover_actual.eq(1), puck.model_cover_probability, 1 - puck.model_cover_probability
    ).clip(1e-15, 1 - 1e-15))
    puck["hypothetical_one_unit_result"] = np.where(
        puck.standard_puck_line_result.eq("WIN"), puck.decimal_price - 1, -1.0
    )
    puck["financial_result_label"] = "HYPOTHETICAL_NO_WAGER_PLACED"
    puck_model = puck_predictions.merge(outcomes, on=["canonical_season", "game_id"], how="inner", validate="one_to_one")
    model_margin = puck_model.final_home_goals - puck_model.final_away_goals
    puck_model["actual_margin_class"] = np.select(
        [model_margin <= -2, model_margin >= 2], ["AWAY_BY_2_PLUS", "HOME_BY_2_PLUS"],
        default="ONE_GOAL_GAME",
    )
    class_probability = {
        "AWAY_BY_2_PLUS": "away_by_2_plus_probability",
        "ONE_GOAL_GAME": "one_goal_game_probability",
        "HOME_BY_2_PLUS": "home_by_2_plus_probability",
    }
    puck_model["probability_assigned_to_outcome"] = [
        row[class_probability[row.actual_margin_class]] for _, row in puck_model.iterrows()
    ]
    puck_model["predicted_margin_class"] = puck_model[[
        "away_by_2_plus_probability", "one_goal_game_probability", "home_by_2_plus_probability",
    ]].idxmax(axis=1).map({
        "away_by_2_plus_probability": "AWAY_BY_2_PLUS",
        "one_goal_game_probability": "ONE_GOAL_GAME",
        "home_by_2_plus_probability": "HOME_BY_2_PLUS",
    })
    puck_model["correct"] = puck_model.predicted_margin_class.eq(puck_model.actual_margin_class)
    puck_model["home_minus_1_5_cover_actual"] = model_margin > 1.5
    puck_model["away_plus_1_5_cover_actual"] = model_margin < 1.5
    puck_model["away_minus_1_5_cover_actual"] = model_margin < -1.5
    puck_model["home_plus_1_5_cover_actual"] = model_margin > -1.5
    puck_model["brier_contribution"] = [
        sum((float(row[column]) - int(label == row.actual_margin_class)) ** 2 for label, column in class_probability.items())
        for _, row in puck_model.iterrows()
    ]
    puck_model["log_loss_contribution"] = -np.log(puck_model.probability_assigned_to_outcome.clip(1e-15, 1 - 1e-15))
    puck_model["financial_claim"] = "NONE_SHADOW_ONLY"
    puck_v2_model = pd.DataFrame()
    paired = pd.DataFrame()
    challenger_path = run_dir / "puck_line_v2_shot_prior_challenger_predictions.csv"
    if challenger_path.is_file():
        candidate_predictions = pd.read_csv(challenger_path)
        puck_v2_model = candidate_predictions.merge(
            outcomes, on=["canonical_season", "game_id"], how="inner", validate="one_to_one"
        )
        provenance_path = run_dir / "puck_line_v2_shot_prior_feature_provenance.csv"
        provenance = pd.read_csv(provenance_path)
        puck_v2_model = puck_v2_model.merge(
            provenance[["game_id", "current_home_games", "current_away_games", "home_prior_weight", "away_prior_weight"]],
            on="game_id", how="left", validate="one_to_one",
        )
        v2_margin = puck_v2_model.final_home_goals - puck_v2_model.final_away_goals
        puck_v2_model["actual_margin_class"] = np.select(
            [v2_margin <= -2, v2_margin >= 2],
            ["AWAY_BY_2_PLUS", "HOME_BY_2_PLUS"], default="ONE_GOAL_GAME",
        )
        puck_v2_model["probability_assigned_to_outcome"] = [
            row[class_probability[row.actual_margin_class]] for _, row in puck_v2_model.iterrows()
        ]
        puck_v2_model["predicted_margin_class"] = puck_v2_model[[
            "away_by_2_plus_probability", "one_goal_game_probability", "home_by_2_plus_probability",
        ]].idxmax(axis=1).map({
            "away_by_2_plus_probability": "AWAY_BY_2_PLUS",
            "one_goal_game_probability": "ONE_GOAL_GAME",
            "home_by_2_plus_probability": "HOME_BY_2_PLUS",
        })
        puck_v2_model["correct"] = puck_v2_model.predicted_margin_class.eq(puck_v2_model.actual_margin_class)
        puck_v2_model["brier_contribution"] = [
            sum((float(row[column]) - int(label == row.actual_margin_class)) ** 2
                for label, column in class_probability.items())
            for _, row in puck_v2_model.iterrows()
        ]
        puck_v2_model["log_loss_contribution"] = -np.log(
            puck_v2_model.probability_assigned_to_outcome.clip(1e-15, 1 - 1e-15)
        )
        puck_v2_model["financial_claim"] = "NONE_SHADOW_ONLY"
        paired = puck_model[["canonical_season", "game_id", "actual_margin_class", "predicted_margin_class", "brier_contribution", "log_loss_contribution", "correct"]].merge(
            puck_v2_model[["canonical_season", "game_id", "predicted_margin_class", "brier_contribution", "log_loss_contribution", "correct"]],
            on=["canonical_season", "game_id"], how="inner", validate="one_to_one", suffixes=("_v1", "_v2"),
        )
        paired["class_changed"] = paired.predicted_margin_class_v1.ne(paired.predicted_margin_class_v2)
        side_depth = pd.concat([
            provenance[["game_id", "current_home_games", "home_prior_weight"]].rename(columns={"current_home_games": "current_games", "home_prior_weight": "prior_weight"}),
            provenance[["game_id", "current_away_games", "away_prior_weight"]].rename(columns={"current_away_games": "current_games", "away_prior_weight": "prior_weight"}),
        ], ignore_index=True).merge(
            puck_v2_model[["game_id", "brier_contribution", "log_loss_contribution", "correct"]],
            on="game_id", how="inner", validate="many_to_one",
        )
        side_depth["history_depth_bucket"] = pd.cut(
            side_depth.current_games, bins=[-1, 3, 5, 10, float("inf")],
            labels=["0_3", "4_5", "6_10", "OVER_10"],
        ).astype("string")
        side_depth["prior_weight_bucket"] = side_depth.prior_weight.map({1.0: "1.00", .75: "0.75", .5: "0.50", 0.0: "0.00"})
        segment_rows = []
        for dimension in ("history_depth_bucket", "prior_weight_bucket"):
            for key, group in side_depth.groupby(dimension, dropna=False):
                confidence = puck_v2_model.set_index("game_id").loc[group.game_id].copy()
                if isinstance(confidence, pd.Series):
                    confidence = confidence.to_frame().T
                confidence["confidence"] = confidence[[
                    "away_by_2_plus_probability", "one_goal_game_probability", "home_by_2_plus_probability",
                ]].max(axis=1)
                confidence["top_label_correct"] = confidence.predicted_margin_class.eq(confidence.actual_margin_class)
                bins = pd.cut(confidence.confidence, bins=np.linspace(0, 1, 11), include_lowest=True)
                ece = 0.0
                for _, calibration_bin in confidence.groupby(bins, observed=False):
                    if len(calibration_bin):
                        ece += len(calibration_bin) / len(confidence) * abs(
                            calibration_bin.top_label_correct.mean() - calibration_bin.confidence.mean()
                        )
                segment_rows.append({
                    "segment_dimension": dimension, "segment": str(key),
                    "side_observations": len(group), "games": group.game_id.nunique(),
                    "multiclass_brier": group.brier_contribution.mean(),
                    "log_loss": group.log_loss_contribution.mean(),
                    "class_accuracy": group.correct.mean(), "top_label_ece": ece,
                })
        challenger_segments = pd.DataFrame(segment_rows)
    # Retain every matched outcome row while limiting evaluation metrics to
    # regular-season official game types. Preseason remains observable but is
    # never included in evaluation summaries.
    schedule_path = run_dir / "schedule_event_identity.csv"
    if not schedule_path.is_file():
        raise RuntimeError("CANONICAL_SCHEDULE_PHASE_EVIDENCE_MISSING")
    phase_schedule = pd.read_csv(schedule_path)
    if phase_schedule.duplicated("game_id").any() or not {"game_id", "game_type_code"}.issubset(phase_schedule):
        raise RuntimeError("CANONICAL_SCHEDULE_PHASE_EVIDENCE_INVALID")
    game_type_by_id = phase_schedule.set_index("game_id").game_type_code.to_dict()
    evaluated_frames = {
        "moneyline": moneyline, "moneyline_challenger": moneyline_challenger,
        "moneyline_market": comparison, "puck_market": puck,
        "puck_model": puck_model, "puck_challenger": puck_v2_model,
    }
    for frame in evaluated_frames.values():
        if frame.empty:
            continue
        if "game_type_code" not in frame and "game_id" in frame:
            frame["game_type_code"] = frame.game_id.map(game_type_by_id)
        type_column = next((name for name in ("game_type_code", "canonical_game_type_code") if name in frame), None)
        if type_column is None:
            continue
        frame["canonical_phase"] = frame[type_column].map(phase_for_game_type)
        eligible = frame[type_column].map(regular_season_evaluation_eligible)
        frame["evaluation_status"] = "REGULAR_SEASON_GRADED"
        frame.loc[~eligible, "evaluation_status"] = frame.loc[~eligible, "canonical_phase"].map({
            "PRESEASON": "PRESEASON_NON_EVALUATION",
            "POSTSEASON": "POSTSEASON_NON_REGULAR_SEASON_EVALUATION",
            "UNKNOWN_GAME_TYPE": "GAME_TYPE_UNRESOLVED",
        }).fillna("GAME_TYPE_UNRESOLVED")
        for metric in ("correct", "brier_contribution", "log_loss_contribution",
                       "accuracy_contribution", "probability_assigned_to_outcome",
                       "model_cover_probability", "cover_actual",
                       "v2_minus_market_probability_gap", "standard_puck_line_result"):
            if metric in frame:
                if pd.api.types.is_bool_dtype(frame[metric].dtype):
                    frame[metric] = frame[metric].astype("boolean")
                elif pd.api.types.is_numeric_dtype(frame[metric].dtype):
                    frame[metric] = pd.to_numeric(frame[metric], errors="coerce").astype("Float64")
                frame.loc[~eligible, metric] = pd.NA
    substantive = {
        "run_manifest": sha256(run_dir / "SHA256SUMS"),
        "outcomes": outcomes.sort_values("game_id")[sorted(required)].astype(str).to_dict("records"),
        "grading_contract_version": "MONEYLINE_V3_BRIER_LOGLOSS_AUC_ACCURACY_ECE_V5_PHASE_AWARE",
    }
    grade_hash = digest_value(substantive)
    destination = grade_root / run_dir.name / f"grade={grade_hash}"
    if destination.is_dir():
        verify_manifest(destination)
        return destination
    destination.mkdir(parents=True, exist_ok=False)
    outcomes.to_csv(destination / "canonical_outcomes.csv", index=False)
    moneyline.to_csv(destination / "graded_moneyline_shadow_results.csv", index=False)
    if len(moneyline_challenger):
        moneyline_challenger.to_csv(destination / "graded_moneyline_shot_finishing_challenger_v3_results.csv", index=False)
        moneyline_paired.to_csv(destination / "graded_moneyline_v2_vs_shot_finishing_challenger_v3_paired.csv", index=False)
        moneyline_segments.to_csv(destination / "graded_moneyline_shot_finishing_challenger_v3_segments.csv", index=False)
    comparison.to_csv(destination / "graded_moneyline_market_comparison.csv", index=False)
    puck.to_csv(destination / "graded_puck_line_market_results.csv", index=False)
    puck_model.to_csv(destination / "graded_puck_line_model_results.csv", index=False)
    if len(puck_v2_model):
        puck_v2_model.to_csv(destination / "graded_puck_line_v2_shot_prior_model_results.csv", index=False)
        paired.to_csv(destination / "graded_puck_line_v1_v2_paired_results.csv", index=False)
        challenger_segments.to_csv(destination / "graded_puck_line_v2_shot_prior_segments.csv", index=False)
    def evaluation_count(frame: pd.DataFrame) -> int:
        if frame.empty or "evaluation_status" not in frame:
            return 0
        return int(frame.evaluation_status.eq("REGULAR_SEASON_GRADED").sum())

    status_payload = {
        "mode": "SHADOW_RESEARCH_ONLY", "graded_games": len(moneyline),
        "outcomes_observed": len(moneyline),
        "moneyline_regular_season_games_graded": evaluation_count(moneyline),
        "source_run_dir": str(run_dir),
        "slate_date": str(predictions.slate_date.iloc[0]) if len(predictions) else None,
        "moneyline_correct": int(moneyline.loc[moneyline.evaluation_status.eq("REGULAR_SEASON_GRADED"), "correct"].fillna(False).sum()) if evaluation_count(moneyline) else 0,
        "moneyline_brier": moneyline.brier_contribution.mean() if len(moneyline) else None,
        "moneyline_log_loss": moneyline.log_loss_contribution.mean() if len(moneyline) else None,
        "moneyline_challenger_v3_games_graded": len(moneyline_challenger),
        "moneyline_challenger_v3_correct": int(moneyline_challenger.correct.fillna(False).sum()) if len(moneyline_challenger) else 0,
        "moneyline_challenger_v3_brier": moneyline_challenger.brier_contribution.mean() if len(moneyline_challenger) else None,
        "moneyline_challenger_v3_log_loss": moneyline_challenger.log_loss_contribution.mean() if len(moneyline_challenger) else None,
        "moneyline_challenger_v3_auc": float(roc_auc_score(
            moneyline_challenger.actual_home_win, moneyline_challenger.challenger_home_win_probability
        )) if len(moneyline_challenger) and moneyline_challenger.actual_home_win.nunique() == 2 else None,
        "moneyline_challenger_v3_accuracy": float(moneyline_challenger.correct.mean()) if len(moneyline_challenger) else None,
        "moneyline_challenger_v3_ece": _binary_ece(
            moneyline_challenger.actual_home_win, moneyline_challenger.challenger_home_win_probability
        ) if len(moneyline_challenger) else None,
        "moneyline_v2_v3_side_changes": int(moneyline_paired.side_changed.sum()) if len(moneyline_paired) else 0,
        "moneyline_v2_v3_paired_games": len(moneyline_paired),
        "puck_line_model_games_graded": evaluation_count(puck_model),
        "puck_line_model_correct": int(puck_model.loc[puck_model.evaluation_status.eq("REGULAR_SEASON_GRADED"), "correct"].fillna(False).sum()) if evaluation_count(puck_model) else 0,
        "puck_line_model_brier": puck_model.brier_contribution.mean() if len(puck_model) else None,
        "puck_line_model_log_loss": puck_model.log_loss_contribution.mean() if len(puck_model) else None,
        "puck_line_v2_shot_prior_games_graded": len(puck_v2_model),
        "puck_line_v2_shot_prior_correct": int(puck_v2_model.correct.fillna(False).sum()) if len(puck_v2_model) else 0,
        "puck_line_v2_shot_prior_brier": puck_v2_model.brier_contribution.mean() if len(puck_v2_model) else None,
        "puck_line_v2_shot_prior_log_loss": puck_v2_model.log_loss_contribution.mean() if len(puck_v2_model) else None,
        "puck_line_v1_v2_class_changes": int(paired.class_changed.sum()) if len(paired) else 0,
        "puck_line_market_observations_graded": evaluation_count(puck), "wagers_placed": 0,
    }
    def json_default(value: Any) -> Any:
        if value is pd.NA or value is pd.NaT:
            return None
        if isinstance(value, np.generic):
            return value.item()
        if pd.api.types.is_scalar(value) and pd.isna(value):
            return None
        raise TypeError(f"NOT_JSON_SERIALIZABLE:{type(value).__name__}")
    (destination / "grading_status.json").write_text(
        json.dumps(status_payload, indent=2, sort_keys=True, default=json_default) + "\n")
    write_manifest(destination)
    return destination


def daily_status(root: Path, slate_date: str) -> dict[str, Any]:
    runs = sorted((root / f"season={SEASON}" / f"slate_date={slate_date}").glob("run_type=*/state=*"))
    statuses = [json.loads((run / "daily_execution_status.json").read_text()) for run in runs]
    if not statuses:
        return {
            "slate_date": slate_date, "scheduled_games": 0, "v2_predictions_created": 0,
            "moneyline_games_books_captured": "0/0", "puck_line_games_books_captured": "0/0",
            "sog_games_covered": 0, "points_games_covered": 0, "saves_games_covered": 0,
            "missing_or_unmatched_events": 0, "remaining_api_credits": None,
            "final_games_awaiting_grading": 0, "graded_moneyline_record": "0-0",
            "graded_moneyline_brier": None, "graded_moneyline_log_loss": None,
            "graded_puck_line_market_observations": 0,
            "puck_line_v2_shot_prior_predictions_created": 0,
            "puck_line_v1_v2_class_changes": 0,
            "puck_line_v1_v2_mean_absolute_probability_delta": None,
            "graded_puck_line_v2_shot_prior_games": 0,
            "graded_puck_line_v2_shot_prior_brier": None,
            "graded_puck_line_v2_shot_prior_log_loss": None,
            "moneyline_shot_finishing_challenger_v3_predictions_created": 0,
            "graded_moneyline_shot_finishing_challenger_v3_games": 0,
            "graded_moneyline_shot_finishing_challenger_v3_brier": None,
            "graded_moneyline_shot_finishing_challenger_v3_log_loss": None,
            "integrity_warnings": ["NO_CAPTURE_RUN_FOR_SLATE"], "edge_claim": "NONE",
        }
    latest = statuses[-1]
    coverage = pd.read_csv(runs[-1] / "cross_market_game_state_observations.csv")
    grading = []
    for path in sorted((root / "grades").glob("state=*/grade=*/grading_status.json")):
        item = json.loads(path.read_text())
        if item.get("slate_date") == slate_date:
            grading.append(item)
    graded_games = max((item.get("outcomes_observed", item["graded_games"]) for item in grading), default=0)
    best = max(grading, key=lambda item: item.get("outcomes_observed", item["graded_games"]), default=None)
    return {
        "slate_date": slate_date, "scheduled_games": latest["scheduled_games"],
        "v2_predictions_created": latest["v2_predictions_created"],
        "moneyline_games_books_captured": f"{latest['moneyline_games_captured']}/{latest['moneyline_books_captured']}",
        "puck_line_games_books_captured": f"{latest['puck_line_games_captured']}/{latest['puck_line_books_captured']}",
        "puck_line_v2_shot_prior_predictions_created": latest.get("puck_line_v2_shot_prior_predictions_created", 0),
        "moneyline_shot_finishing_challenger_v3_predictions_created": latest.get(
            "moneyline_shot_finishing_challenger_v3_predictions_created", 0),
        "puck_line_v1_v2_class_changes": latest.get("puck_line_v1_v2_class_changes", 0),
        "puck_line_v1_v2_mean_absolute_probability_delta": latest.get("puck_line_v1_v2_mean_absolute_probability_delta"),
        "sog_games_covered": int(coverage.sog_strict_prior_available.sum()),
        "points_games_covered": int(coverage.points_strict_prior_available.sum()),
        "saves_games_covered": int(coverage.saves_strict_prior_available.sum()),
        "missing_or_unmatched_events": latest["unmatched_events"],
        "remaining_api_credits": latest["remaining_api_credits"],
        "final_games_awaiting_grading": max(latest["scheduled_games"] - max(
            graded_games, max((item.get("outcomes_observed", item["graded_games"]) for item in grading), default=0)), 0),
        "graded_moneyline_record": (
            f"{best['moneyline_correct']}-{best.get('moneyline_regular_season_games_graded', best['graded_games']) - best['moneyline_correct']}" if best else "0-0"
        ),
        "graded_moneyline_brier": best["moneyline_brier"] if best else None,
        "graded_moneyline_log_loss": best["moneyline_log_loss"] if best else None,
        "graded_puck_line_market_observations": best["puck_line_market_observations_graded"] if best else 0,
        "graded_puck_line_v2_shot_prior_games": best.get("puck_line_v2_shot_prior_games_graded", 0) if best else 0,
        "graded_puck_line_v2_shot_prior_brier": best.get("puck_line_v2_shot_prior_brier") if best else None,
        "graded_puck_line_v2_shot_prior_log_loss": best.get("puck_line_v2_shot_prior_log_loss") if best else None,
        "graded_moneyline_shot_finishing_challenger_v3_games": best.get(
            "moneyline_challenger_v3_games_graded", 0) if best else 0,
        "graded_moneyline_shot_finishing_challenger_v3_brier": best.get(
            "moneyline_challenger_v3_brier") if best else None,
        "graded_moneyline_shot_finishing_challenger_v3_log_loss": best.get(
            "moneyline_challenger_v3_log_loss") if best else None,
        "integrity_warnings": [], "edge_claim": "NONE_SMALL_SAMPLE_SHADOW_ONLY",
    }
