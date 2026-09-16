"""Pure runtime for the NHL 2026 V2 cross-market shadow lane."""
from __future__ import annotations

import hashlib
import json
import math
import re
import fcntl
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


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
GAME_TYPES = {1: "PRESEASON", 2: "REGULAR_SEASON", 3: "POSTSEASON"}
MAX_REQUESTS_PER_RUN = 1
MAX_ESTIMATED_CREDITS_PER_RUN = 4
MARKETS = ("h2h", "spreads")
REGIONS = ("us", "us2")

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
    result["game_type_label"] = result.game_type_code.map(GAME_TYPES).fillna("UNKNOWN_GAME_TYPE")
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
            "substantive_prediction_sha256": digest_value(substantive),
            "control_name": CONTROL_NAME, "control_artifact_sha256": sha256(PARAMETER_PATH),
            "prediction_status": (
                "POST_START_REJECTED" if not prestart else
                "PRESEASON_REHEARSAL_EXCLUDED" if int(target.game_type_code) == 1 else
                "REGULAR_SEASON_SHADOW_ELIGIBLE" if int(target.game_type_code) == 2 else
                "POSTSEASON_NON_REGULAR_EVALUATION"
            ),
            "regular_season_evaluation_eligible": bool(prestart and int(target.game_type_code) == 2),
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
            "regular_season_evaluation_eligible": bool(prestart and int(target.game_type_code) == 2),
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
    return re.sub(r"[^a-z0-9]", "", str(value).lower())


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
    quote_rows, binding_rows, raw_rows = [], [], []
    for event in envelope.get("provider_response") or []:
        event_id = str(event.get("id") or "")
        commence = pd.to_datetime(event.get("commence_time"), utc=True, errors="coerce")
        home_code, away_code = team_code(event.get("home_team")), team_code(event.get("away_team"))
        candidates = schedule[
            schedule.home_team.eq(home_code) & schedule.away_team.eq(away_code)
            & schedule.scheduled_start_time_utc.sub(commence).abs().le(pd.Timedelta(minutes=15))
        ] if pd.notna(commence) else schedule.iloc[0:0]
        status = "BOUND" if len(candidates) == 1 else "UNMATCHED_OR_AMBIGUOUS"
        game = candidates.iloc[0] if len(candidates) == 1 else None
        binding_rows.append({
            "provider_event_id": event_id, "participant_home_raw": event.get("home_team"),
            "participant_away_raw": event.get("away_team"), "normalized_home_team": home_code,
            "normalized_away_team": away_code, "provider_commence_time_utc": event.get("commence_time"),
            "candidate_games": len(candidates), "binding_status": status,
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
        "normalized_home_team", "normalized_away_team", "provider_commence_time_utc",
        "candidate_games", "binding_status", "canonical_game_id",
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
    if run_type not in {"MIDDAY", "FINAL_PREGAME"}:
        raise ValueError("RUN_TYPE_INVALID")
    if canary_mode and (
        not schedule.game_type_code.eq(1).all()
        or not (PRESEASON_START <= slate_date < REGULAR_SEASON_START)
    ):
        raise RuntimeError("PRESEASON_CANARY_SCOPE_VIOLATION")
    predictions, timing = build_v2_predictions(schedule, history, run_timestamp_utc)
    puck_predictions = build_puck_line_predictions(predictions)
    if odds_json:
        envelope = json.loads(odds_json.read_text())
    else:
        envelope = {
            "capture_timestamp_utc": run_timestamp_utc, "provider": "NO_MARKET_FIXTURE",
            "quota": {**quota_estimate(), "credits_consumed": 0, "requests_remaining": None},
            "provider_response": [],
        }
    quotes, bindings, raw_observations = normalize_markets(envelope, schedule)
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
        "markets": quotes.sort_values(["game_id", "sportsbook_key", "market_type", "side_orientation"], na_position="last")[[
            "provider_event_id", "game_id", "sportsbook_key", "market_type", "side_orientation", "point",
            "american_price", "source_update_timestamp_utc", "qualification_status",
        ]].astype(str).to_dict("records") if len(quotes) else [],
        "coverage": coverage.drop(columns=["observation_timestamp_utc"], errors="ignore").astype(str).to_dict("records"),
        "run_type": run_type,
    }
    substantive_hash = digest_value(substantive)
    destination = root / f"season={SEASON}" / f"slate_date={slate_date}" / f"run_type={run_type}" / f"state={substantive_hash}"
    if destination.is_dir():
        verify_manifest(destination)
        return destination
    destination.mkdir(parents=True, exist_ok=False)
    schedule.to_csv(destination / "schedule_event_identity.csv", index=False)
    predictions.to_csv(destination / "v2_immutable_predictions.csv", index=False)
    puck_predictions.to_csv(destination / "puck_line_v1_immutable_predictions.csv", index=False)
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
    substantive = {
        "run_manifest": sha256(run_dir / "SHA256SUMS"),
        "outcomes": outcomes.sort_values("game_id")[sorted(required)].astype(str).to_dict("records"),
    }
    grade_hash = digest_value(substantive)
    destination = grade_root / run_dir.name / f"grade={grade_hash}"
    if destination.is_dir():
        verify_manifest(destination)
        return destination
    destination.mkdir(parents=True, exist_ok=False)
    outcomes.to_csv(destination / "canonical_outcomes.csv", index=False)
    moneyline.to_csv(destination / "graded_moneyline_shadow_results.csv", index=False)
    comparison.to_csv(destination / "graded_moneyline_market_comparison.csv", index=False)
    puck.to_csv(destination / "graded_puck_line_market_results.csv", index=False)
    puck_model.to_csv(destination / "graded_puck_line_model_results.csv", index=False)
    (destination / "grading_status.json").write_text(json.dumps({
        "mode": "SHADOW_RESEARCH_ONLY", "graded_games": len(moneyline),
        "source_run_dir": str(run_dir),
        "slate_date": str(predictions.slate_date.iloc[0]) if len(predictions) else None,
        "moneyline_correct": int(moneyline.correct.sum()),
        "moneyline_brier": moneyline.brier_contribution.mean() if len(moneyline) else None,
        "moneyline_log_loss": moneyline.log_loss_contribution.mean() if len(moneyline) else None,
        "puck_line_model_games_graded": len(puck_model),
        "puck_line_model_correct": int(puck_model.correct.sum()),
        "puck_line_model_brier": puck_model.brier_contribution.mean() if len(puck_model) else None,
        "puck_line_model_log_loss": puck_model.log_loss_contribution.mean() if len(puck_model) else None,
        "puck_line_market_observations_graded": len(puck), "wagers_placed": 0,
    }, indent=2, sort_keys=True) + "\n")
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
            "integrity_warnings": ["NO_CAPTURE_RUN_FOR_SLATE"], "edge_claim": "NONE",
        }
    latest = statuses[-1]
    coverage = pd.read_csv(runs[-1] / "cross_market_game_state_observations.csv")
    grading = []
    for path in sorted((root / "grades").glob("state=*/grade=*/grading_status.json")):
        item = json.loads(path.read_text())
        if item.get("slate_date") == slate_date:
            grading.append(item)
    graded_games = max((item["graded_games"] for item in grading), default=0)
    best = max(grading, key=lambda item: item["graded_games"], default=None)
    return {
        "slate_date": slate_date, "scheduled_games": latest["scheduled_games"],
        "v2_predictions_created": latest["v2_predictions_created"],
        "moneyline_games_books_captured": f"{latest['moneyline_games_captured']}/{latest['moneyline_books_captured']}",
        "puck_line_games_books_captured": f"{latest['puck_line_games_captured']}/{latest['puck_line_books_captured']}",
        "sog_games_covered": int(coverage.sog_strict_prior_available.sum()),
        "points_games_covered": int(coverage.points_strict_prior_available.sum()),
        "saves_games_covered": int(coverage.saves_strict_prior_available.sum()),
        "missing_or_unmatched_events": latest["unmatched_events"],
        "remaining_api_credits": latest["remaining_api_credits"],
        "final_games_awaiting_grading": max(latest["scheduled_games"] - graded_games, 0),
        "graded_moneyline_record": (
            f"{best['moneyline_correct']}-{best['graded_games'] - best['moneyline_correct']}" if best else "0-0"
        ),
        "graded_moneyline_brier": best["moneyline_brier"] if best else None,
        "graded_moneyline_log_loss": best["moneyline_log_loss"] if best else None,
        "graded_puck_line_market_observations": best["puck_line_market_observations_graded"] if best else 0,
        "integrity_warnings": [], "edge_claim": "NONE_SMALL_SAMPLE_SHADOW_ONLY",
    }
