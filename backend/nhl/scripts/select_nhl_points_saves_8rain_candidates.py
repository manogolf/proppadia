#!/usr/bin/env python3
"""Select current-slate Points/Saves 8rain candidates under explicit JSON policies."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from backend.nhl.eightrain_adapter import (
    ambiguous_player_bindings, ambiguous_player_names, load_catalogs,
    normalize_player_name, unique_player_codes_by_name,
)


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_POLICIES = {
    "points": ROOT / "backend/nhl/config/nhl_points_active_candidate_policy_v1.json",
    "saves": ROOT / "backend/nhl/config/nhl_saves_active_candidate_policy_v1.json",
}


def american_implied_probability(value: Any) -> float:
    try:
        price = float(value)
    except (TypeError, ValueError):
        return math.nan
    if not math.isfinite(price) or price == 0 or (-100 < price < 100):
        return math.nan
    return 100.0 / (price + 100.0) if price > 0 else -price / (-price + 100.0)


def decimal_odds(value: Any) -> float:
    price = float(value)
    return 1.0 + price / 100.0 if price > 0 else 1.0 + 100.0 / abs(price)


def load_active_policy(path: Path, *, lane: str) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text())
    if payload.get("schema_version") != "NHL_8RAIN_PROP_CANDIDATE_POLICY_V1":
        raise ValueError("PROP_POLICY_SCHEMA_INVALID")
    if payload.get("status") != "ACTIVE_REFERENCE" or payload.get("lane") != lane.upper():
        raise ValueError("PROP_POLICY_NOT_ACTIVE_FOR_LANE")
    selection = payload.get("selection") or {}
    required = {
        "allowed_sides", "minimum_ev_exclusive", "minimum_model_market_gap_exclusive",
        "allowed_lines", "minimum_price_american", "maximum_price_american",
        "minimum_model_probability_exclusive", "maximum_model_probability_exclusive",
        "require_side_specific_market_price",
    }
    if required - set(selection):
        raise ValueError("PROP_POLICY_SELECTION_INCOMPLETE")
    if payload.get("identity", {}).get("per_slate_cap") is not None:
        raise ValueError("PROP_POLICY_CANDIDATE_CAP_NOT_AUTHORIZED")
    return payload


def require_current_slate(slate_date: str, *, current_date: str) -> None:
    try:
        slate = datetime.strptime(str(slate_date), "%Y-%m-%d").date()
        current = datetime.strptime(str(current_date), "%Y-%m-%d").date()
    except ValueError as error:
        raise ValueError("SLATE_DATE_INVALID") from error
    if slate != current:
        raise ValueError("CANDIDATE_SELECTION_MUST_USE_CURRENT_SLATE")


def _sha256(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def select_lane(
    source: pd.DataFrame,
    names: pd.DataFrame,
    schedule: pd.DataFrame,
    *,
    lane: str,
    slate_date: str,
    policy: dict[str, Any],
    policy_sha256: str,
    source_artifact_sha256: str,
    odds_observation_id: str,
    odds_observation_manifest_sha256: str,
    capture_timestamp_utc: str,
    team_map: dict[str, str],
    player_map: dict[tuple[str, str], str],
    ambiguous_player_keys: set[tuple[str, str]],
    allowed_bets: dict[str, set[str]],
    model_family: str = "phoenix",
    model_version: str = "phoenix_v2",
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    lane = lane.lower()
    if lane not in {"points", "saves"} or policy.get("lane") != lane.upper():
        raise ValueError("PROP_LANE_POLICY_MISMATCH")
    if lane not in allowed_bets or not {"over", "under"}.issubset(allowed_bets[lane]):
        raise ValueError(f"PROP_MARKET_NOT_SUPPORTED_BY_CATALOG:{lane}")
    if source.empty:
        raise ValueError(f"{lane.upper()}_SOURCE_ARTIFACT_EMPTY")
    required = {"full_name", "player_id", "game_id", "team_id", "line", "p_over", "game_date", "parent_daily_run_id"}
    if required - set(source.columns):
        raise ValueError(f"{lane.upper()}_SOURCE_SCHEMA_MISSING:{','.join(sorted(required-set(source.columns)))}")
    dates = sorted(source.game_date.dropna().astype(str).unique())
    if dates != [slate_date]:
        raise ValueError(f"{lane.upper()}_SOURCE_SLATE_MISMATCH:{dates}")
    if source.parent_daily_run_id.dropna().astype(str).nunique() != 1:
        raise ValueError(f"{lane.upper()}_SOURCE_RUN_ID_NOT_UNIQUE")

    frame = source.copy()
    frame["player_id"] = pd.to_numeric(frame.player_id, errors="coerce")
    frame["game_id"] = pd.to_numeric(frame.game_id, errors="coerce")
    frame["line"] = pd.to_numeric(frame.line, errors="coerce")
    frame["p_over"] = pd.to_numeric(frame.p_over, errors="coerce")
    if frame[["player_id", "game_id", "line"]].isna().any().any():
        raise ValueError(f"{lane.upper()}_SOURCE_PREDICTION_IDENTITY_INVALID")
    duplicate_key = ["player_id", "game_id", "line"]
    if "feature_hash" in frame.columns:
        duplicate_key.append("feature_hash")
    if frame.duplicated(duplicate_key, keep=False).any():
        raise ValueError(f"{lane.upper()}_DUPLICATE_SOURCE_PREDICTION_GRAIN")

    names = names.copy()
    name_columns = {"player_id", "game_id", "team_code"}
    if name_columns - set(names.columns):
        raise ValueError("CANONICAL_NAMES_SCHEMA_MISSING")
    names["player_id"] = pd.to_numeric(names.player_id, errors="coerce")
    names["game_id"] = pd.to_numeric(names.game_id, errors="coerce")
    identity_names = names[["player_id", "game_id", "team_code"]].drop_duplicates()
    if identity_names.duplicated(["player_id", "game_id"], keep=False).any():
        raise ValueError("CANONICAL_PLAYER_GAME_TEAM_NOT_UNIQUE")
    frame = frame.merge(identity_names, on=["player_id", "game_id"], how="left", validate="many_to_one")

    schedule_rows = schedule.copy()
    if {"game_id", "game_date", "home_team", "away_team"} - set(schedule_rows.columns):
        raise ValueError("CANONICAL_SCHEDULE_SCHEMA_MISSING")
    if schedule_rows.game_id.duplicated().any():
        raise ValueError("CANONICAL_GAME_ID_NOT_UNIQUE")
    if set(schedule_rows.game_date.astype(str)) != {slate_date}:
        raise ValueError("CANONICAL_SCHEDULE_SLATE_MISMATCH")
    game_identity = {
        int(row.game_id): (str(row.game_date), str(row.home_team).upper(), str(row.away_team).upper())
        for row in schedule_rows.itertuples(index=False)
    }

    selection = policy["selection"]
    allowed_sides = {str(side).upper() for side in selection["allowed_sides"]}
    allowed_lines = selection["allowed_lines"]
    rows: list[dict[str, Any]] = []
    run_id = str(frame.parent_daily_run_id.iloc[0])
    model_identity = f"NHL_{lane.upper()}_{model_family.upper()}_{model_version.upper()}_REFERENCE"
    for source_row in frame.to_dict("records"):
        game_id, player_id = int(source_row["game_id"]), int(source_row["player_id"])
        if game_id not in game_identity:
            raise ValueError(f"{lane.upper()}_GAME_NOT_IN_CANONICAL_SLATE:{game_id}")
        date_value, home, away = game_identity[game_id]
        team = str(source_row.get("team_code") or "").upper()
        if team not in {home, away}:
            mapping_issue = "PLAYER_TEAM_NOT_IN_GAME" if team else "CANONICAL_TEAM_MAPPING_MISSING"
            opponent = ""
        else:
            mapping_issue = ""
            opponent = away if team == home else home
        p_over = float(source_row["p_over"]) if pd.notna(source_row["p_over"]) else math.nan
        for side in ("OVER", "UNDER"):
            model_prob = p_over if side == "OVER" else 1.0 - p_over
            price_column = "price_over" if side == "OVER" else "price_under"
            price_value = source_row.get(price_column)
            price_prob = american_implied_probability(price_value)
            market_column = "p_over_mkt" if side == "OVER" else "p_under_mkt"
            market_value = source_row.get(market_column)
            market_prob = float(market_value) if pd.notna(market_value) else price_prob
            gap = model_prob - market_prob if math.isfinite(model_prob) and math.isfinite(market_prob) else math.nan
            ev = model_prob / price_prob - 1.0 if math.isfinite(model_prob) and price_prob > 0 else math.nan
            reason: list[str] = []
            if side not in allowed_sides:
                reason.append("SIDE_NOT_ALLOWED")
            if mapping_issue:
                reason.append(mapping_issue)
            attachment_value = source_row.get("attachment_status", "")
            attachment = str(attachment_value).upper() if pd.notna(attachment_value) else ""
            policy_reasons: list[str] = []
            if attachment and attachment != "MATCHED":
                policy_reasons.append("MARKET_ATTACHMENT_NOT_MATCHED")
            if not math.isfinite(price_prob):
                policy_reasons.append("SIDE_PRICE_MISSING_OR_INVALID")
            if not math.isfinite(market_prob) or not 0 < market_prob < 1:
                policy_reasons.append("MARKET_PROBABILITY_MISSING_OR_INVALID")
            if not math.isfinite(model_prob) or not (
                float(selection["minimum_model_probability_exclusive"]) < model_prob
                < float(selection["maximum_model_probability_exclusive"])
            ):
                policy_reasons.append("MODEL_PROBABILITY_OUT_OF_BOUNDS")
            if allowed_lines is not None and float(source_row["line"]) not in {float(x) for x in allowed_lines}:
                policy_reasons.append("LINE_NOT_ALLOWED")
            try:
                numeric_price = float(price_value)
            except (TypeError, ValueError):
                numeric_price = math.nan
            if selection["minimum_price_american"] is not None and numeric_price < float(selection["minimum_price_american"]):
                policy_reasons.append("PRICE_BELOW_POLICY_BOUND")
            if selection["maximum_price_american"] is not None and numeric_price > float(selection["maximum_price_american"]):
                policy_reasons.append("PRICE_ABOVE_POLICY_BOUND")
            if not math.isfinite(ev) or ev <= float(selection["minimum_ev_exclusive"]):
                policy_reasons.append("EV_NOT_ABOVE_POLICY_MINIMUM")
            if not math.isfinite(gap) or gap <= float(selection["minimum_model_market_gap_exclusive"]):
                policy_reasons.append("GAP_NOT_ABOVE_POLICY_MINIMUM")
            normalized_name = normalize_player_name(source_row.get("full_name"))
            team_code = team_map.get(team, "")
            player_key = (normalized_name, team_code)
            eight_rain_code = player_map.get(player_key, "") if team_code else ""
            map_status = (
                "AMBIGUOUS" if player_key in ambiguous_player_keys else
                "MAPPED" if eight_rain_code else "UNMAPPED"
            ) if team_code and normalized_name else "UNMAPPED"
            policy_pass = not policy_reasons and not mapping_issue and side in allowed_sides
            reason.extend(policy_reasons)
            if policy_pass and map_status != "MAPPED":
                reason.append("PLAYER_CODE_" + map_status)
            status = "FINAL_CANDIDATE" if policy_pass and map_status == "MAPPED" else (
                "POLICY_QUALIFIED_PLAYER_UNMAPPED" if policy_pass else "EXCLUDED"
            )
            feature_hash = str(source_row.get("feature_hash") or "")
            prediction_identity = hashlib.sha256(json.dumps({
                "prediction_artifact_sha256": source_row.get("prediction_artifact_sha256"),
                "player_id": player_id, "game_id": game_id, "prop": lane,
                "line": float(source_row["line"]), "feature_hash": feature_hash,
            }, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
            odds_manifest = str(source_row.get("odds_observation_manifest_sha256") or odds_observation_manifest_sha256)
            rows.append({
                **source_row,
                "date": date_value, "game_id": game_id, "player_id": player_id,
                "player_name": str(source_row["full_name"]), "team": team,
                "opponent": opponent, "market": lane, "side": side,
                "model_pick": side.lower(), "model_side_prob": model_prob,
                "market_side_prob": market_prob, "sportsbook_price": price_value,
                "price_implied_probability": price_prob, "ev": ev,
                "model_market_gap": gap, "model_identity": model_identity,
                "model_family": model_family, "model_version": model_version,
                "run_id": run_id, "parent_daily_run_id": run_id,
                "odds_observation_id": odds_observation_id,
                "odds_observation_manifest_sha256": odds_manifest,
                "capture_timestamp_utc": capture_timestamp_utc,
                "market_attachment_artifact_sha256": source_artifact_sha256,
                "prediction_artifact_sha256": source_row.get("prediction_artifact_sha256", ""),
                "prediction_identity": prediction_identity,
                "feature_hash": feature_hash,
                "candidate_policy_name": policy["policy_name"],
                "candidate_policy_version": policy["policy_version"],
                "candidate_policy_sha256": policy_sha256,
                "catalog_player_code": eight_rain_code,
                "player_mapping_status": map_status,
                "policy_qualified": bool(policy_pass),
                "decision_status": status,
                "decision_reason": ";".join(reason),
            })
    decisions = pd.DataFrame(rows)
    candidate_key = ["game_id", "player_id", "market", "line", "side"]
    if decisions[decisions.decision_status.eq("FINAL_CANDIDATE")].duplicated(candidate_key, keep=False).any():
        raise ValueError(f"{lane.upper()}_DUPLICATE_CANDIDATE_IDENTITY")
    selected = decisions[decisions.decision_status.eq("FINAL_CANDIDATE")].copy()
    qualified = decisions[decisions.policy_qualified].copy()
    summary = {
        "lane": lane.upper(), "market_code": lane, "slate_date": slate_date,
        "policy_name": policy["policy_name"], "policy_version": policy["policy_version"],
        "policy_sha256": policy_sha256, "source_rows": int(len(frame)),
        "source_priced_rows": int(pd.to_numeric(frame.get("price_over"), errors="coerce").notna().sum()),
        "decision_rows": int(len(decisions)), "policy_qualified_before_mapping": int(len(qualified)),
        "selected_mapped_candidates": int(len(selected)),
        "qualified_side_counts": {str(k): int(v) for k, v in qualified.side.value_counts().items()},
        "qualified_line_counts": {str(k): int(v) for k, v in qualified.line.value_counts().sort_index().items()},
        "qualified_ev_range": [float(qualified.ev.min()), float(qualified.ev.max())] if len(qualified) else None,
        "qualified_gap_range": [float(qualified.model_market_gap.min()), float(qualified.model_market_gap.max())] if len(qualified) else None,
        "mapped_policy_qualified": int(qualified.player_mapping_status.eq("MAPPED").sum()),
        "unmapped_policy_qualified": int(qualified.player_mapping_status.eq("UNMAPPED").sum()),
        "ambiguous_policy_qualified": int(qualified.player_mapping_status.eq("AMBIGUOUS").sum()),
        "policy_qualified_unique_player_game_identities": int(
            qualified.loc[:, ["player_id", "game_id"]].drop_duplicates().shape[0]),
        "selected_unique_player_game_line_identities": int(
            selected.loc[:, ["player_id", "game_id", "line"]].drop_duplicates().shape[0]),
        "selected_unique_players": int(selected.player_id.nunique()),
        "challengers_included": False,
    }
    return decisions, selected, summary


def _load_observation(path: Path) -> tuple[str, str, str]:
    path = Path(path)
    complete = json.loads((path / "RUN_COMPLETE.json").read_text())
    if complete.get("classification") != "CAPTURED_NONEMPTY":
        raise ValueError("ODDS_OBSERVATION_NOT_CAPTURED_NONEMPTY")
    summary = json.loads((path / "observation_summary.json").read_text())
    if summary.get("classification") != "CAPTURED_NONEMPTY":
        raise ValueError("ODDS_OBSERVATION_SUMMARY_NOT_CAPTURED")
    return path.name.removeprefix("observation="), _sha256(path / "SHA256SUMS"), str(summary["observation_timestamp_utc"])


def _prepare_sog(path: Path | None, *, slate_date: str, names: pd.DataFrame) -> pd.DataFrame:
    if path is None:
        return pd.DataFrame()
    sog = pd.read_csv(path)
    if sog.empty:
        return sog
    if "game_date" not in sog:
        raise ValueError("SOG_CANDIDATE_DATE_MISSING")
    if set(sog.game_date.astype(str)) != {slate_date}:
        raise ValueError("SOG_CANDIDATE_SLATE_MISMATCH")
    if "player_name" not in sog and "full_name" in sog:
        sog = sog.rename(columns={"full_name": "player_name"})
    if "model_side_prob" not in sog or "model_pick" not in sog:
        raise ValueError("SOG_CANDIDATE_SIDE_SCHEMA_MISSING")
    names_key = names[["player_id", "game_id", "team_code"]].drop_duplicates()
    if "team" not in sog:
        sog = sog.merge(names_key, on=["player_id", "game_id"], how="left", validate="many_to_one")
        sog = sog.rename(columns={"team_code": "team"})
    sog["market"] = "shots_on_goal"
    sog["model_identity"] = "NHL_SOG_POLICY_SELECTED_REFERENCE"
    sog["run_id"] = sog.get("parent_daily_run_id", "")
    sog["player_mapping_status"] = "PENDING_8RAIN_CATALOG_VALIDATION"
    return sog


def select_raw_lane(
    source: pd.DataFrame, names: pd.DataFrame, schedule: pd.DataFrame, *,
    lane: str, slate_date: str, source_sha256: str, observation_id: str,
    observation_manifest_sha256: str, capture_timestamp_utc: str,
    team_map: dict[str, str], player_map: dict[tuple[str, str], str],
    unique_name_map: dict[str, str], ambiguous_team_keys: set[tuple[str, str]],
    ambiguous_names: set[str], allowed_bets: dict[str, set[str]],
    parent_run_id: str,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Preserve every valid raw model line; mapping only controls thin CSV admission."""
    lane = lane.lower()
    if lane not in {"points", "saves", "shots_on_goal"}:
        raise ValueError("RAW_PROP_LANE_INVALID")
    if lane not in allowed_bets or not {"over", "under"}.issubset(allowed_bets[lane]):
        raise ValueError(f"PROP_MARKET_NOT_SUPPORTED_BY_CATALOG:{lane}")
    required = {"player_id", "game_id", "line", "p_over", "game_date"}
    if required - set(source.columns):
        raise ValueError(f"{lane.upper()}_RAW_SOURCE_SCHEMA_MISSING:{','.join(sorted(required-set(source.columns)))}")
    frame = source.copy()
    for column in ("player_id", "game_id", "line", "p_over"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["game_date"] = frame.game_date.astype(str)
    frame["_prediction_valid"] = (
        frame.player_id.notna() & frame.game_id.notna()
        & frame.line.notna() & frame.line.gt(0)
        & frame.p_over.notna() & frame.p_over.ge(0) & frame.p_over.le(1)
    )
    if set(frame.game_date) != {slate_date}:
        raise ValueError(f"{lane.upper()}_RAW_SOURCE_SLATE_MISMATCH")
    if frame.duplicated(["player_id", "game_id", "line"], keep=False).any():
        raise ValueError(f"{lane.upper()}_RAW_DUPLICATE_PREDICTION_GRAIN")

    name_cols = [c for c in ("player_id", "game_id", "full_name", "team_code") if c in names]
    name_frame = names[name_cols].copy()
    name_frame["player_id"] = pd.to_numeric(name_frame.player_id, errors="coerce")
    name_frame["game_id"] = pd.to_numeric(name_frame.game_id, errors="coerce")
    if name_frame.duplicated(["player_id", "game_id"], keep=False).any():
        conflicts = name_frame.groupby(["player_id", "game_id"], dropna=False).agg(
            teams=("team_code", lambda values: values.dropna().astype(str).nunique()),
            names=("full_name", lambda values: values.dropna().astype(str).nunique()),
        )
        if ((conflicts.teams > 1) | (conflicts.names > 1)).any():
            raise ValueError("CANONICAL_PLAYER_GAME_IDENTITY_CONFLICT")
        name_frame = name_frame.drop_duplicates(["player_id", "game_id"])
    frame = frame.merge(name_frame, on=["player_id", "game_id"], how="left", suffixes=("", "_canonical"), validate="many_to_one")
    if "full_name" not in frame:
        frame["full_name"] = frame.get("full_name_canonical", "")
    elif "full_name_canonical" in frame:
        frame["full_name"] = frame["full_name"].where(frame["full_name"].notna() & frame["full_name"].astype(str).str.strip().ne(""), frame["full_name_canonical"])

    schedule_frame = schedule.copy()
    schedule_frame["game_id"] = pd.to_numeric(schedule_frame.game_id, errors="coerce")
    games = {
        int(r.game_id): (str(r.game_date), str(r.home_team).upper(), str(r.away_team).upper())
        for r in schedule_frame.itertuples(index=False)
    }
    rows: list[dict[str, Any]] = []
    identity_hash = str(source_sha256)
    for source_row in frame.to_dict("records"):
        game_id = int(source_row["game_id"]) if pd.notna(source_row["game_id"]) else -1
        player_id = int(source_row["player_id"]) if pd.notna(source_row["player_id"]) else -1
        line = float(source_row["line"]) if pd.notna(source_row["line"]) else math.nan
        p_over = float(source_row["p_over"]) if pd.notna(source_row["p_over"]) else math.nan
        valid = bool(source_row["_prediction_valid"])
        reason = "" if valid else "INVALID_MODEL_PREDICTION_OR_LINE"
        win_percent_representable = bool(valid and 0 < p_over < 1)
        if game_id not in games:
            valid, reason = False, "GAME_NOT_IN_CANONICAL_SLATE"
            game_date, home, away = slate_date, "", ""
        else:
            game_date, home, away = games[game_id]
        team = str(source_row.get("team_code") or "").upper()
        if team not in {home, away}:
            valid, reason = False, "CANONICAL_PLAYER_TEAM_GAME_IDENTITY_INVALID"
        opponent = away if team == home else home if team == away else ""
        normalized_name = normalize_player_name(source_row.get("full_name"))
        provider_team = team_map.get(team, "")
        team_key = (normalized_name, provider_team)
        code = player_map.get(team_key, "") if provider_team else ""
        mapping_attempts = ["CANONICAL_NHL_NAME_AND_TEAM"]
        mapping_status = "MAPPED" if code else ""
        if not code and normalized_name and normalized_name in unique_name_map:
            code = unique_name_map[normalized_name]
            mapping_status = "MAPPED_UNIQUE_NAME_CATALOG_TEAM_LAG"
            mapping_attempts.append("UNIQUE_NORMALIZED_NAME_GLOBAL")
        if not code:
            if (team_key in ambiguous_team_keys) or (normalized_name in ambiguous_names):
                mapping_status = "AMBIGUOUS"
            elif not provider_team or not normalized_name:
                mapping_status = "OTHER"
            else:
                mapping_status = "CATALOG_MISSING"
        model_probability = p_over if valid else math.nan
        price_over = source_row.get("price_over")
        price_under = source_row.get("price_under")
        market_prob_over = source_row.get("p_over_mkt")
        market_prob_under = source_row.get("p_under_mkt")
        try:
            market_prob_over = float(market_prob_over)
        except (TypeError, ValueError):
            market_prob_over = american_implied_probability(price_over)
        try:
            market_prob_under = float(market_prob_under)
        except (TypeError, ValueError):
            market_prob_under = american_implied_probability(price_under)
        implied_over = american_implied_probability(price_over)
        implied_under = american_implied_probability(price_under)
        ev_over = model_probability / implied_over - 1 if math.isfinite(model_probability) and math.isfinite(implied_over) else math.nan
        ev_under = (1.0 - model_probability) / implied_under - 1 if math.isfinite(model_probability) and math.isfinite(implied_under) else math.nan
        edge_over = model_probability - market_prob_over if math.isfinite(model_probability) and math.isfinite(market_prob_over) else math.nan
        edge_under = 1.0 - model_probability - market_prob_under if math.isfinite(model_probability) and math.isfinite(market_prob_under) else math.nan
        model_identity = str(source_row.get("model_identity") or f"NHL_{lane.upper()}_PHOENIX_PHOENIX_V2_REFERENCE")
        prediction_identity = hashlib.sha256(json.dumps({
            "source_sha256": identity_hash, "player_id": player_id,
            "game_id": game_id, "market": lane, "line": line,
            "model_identity": model_identity,
        }, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        decision = {
            **source_row, "date": game_date, "game_id": game_id,
            "player_id": player_id, "player_name": str(source_row.get("full_name") or ""),
            "team": team, "opponent": opponent, "market": lane, "line": line,
            "side": "OVER", "model_pick": "over", "model_side_prob": model_probability,
            "model_identity": model_identity, "model_family": "phoenix",
            "model_version": "phoenix_v2", "run_id": parent_run_id,
            "parent_daily_run_id": parent_run_id,
            "odds_observation_id": observation_id,
            "odds_observation_manifest_sha256": observation_manifest_sha256,
            "capture_timestamp_utc": capture_timestamp_utc,
            "prediction_artifact_sha256": str(source_row.get("prediction_artifact_sha256") or identity_hash),
            "market_attachment_artifact_sha256": identity_hash,
            "prediction_identity": prediction_identity,
            "feature_hash": str(source_row.get("feature_hash") or ""),
            "catalog_player_code": code, "player_mapping_status": mapping_status,
            "_win_percent_representable": win_percent_representable,
            "mapping_lookup_attempts": "|".join(mapping_attempts),
            "raw_export_status": (
                "INVALID_MODEL_PREDICTION" if not valid else
                "WIN_PERCENT_UNREPRESENTABLE" if not win_percent_representable else
                "RAW_EXPORT_CANDIDATE" if code else mapping_status
            ),
            "exclusion_class": "" if valid and code and win_percent_representable else (
                "MODEL_ELIGIBILITY_REQUIRED" if not bool(source_row["_prediction_valid"]) else
                "SCHEMA_REQUIRED" if not win_percent_representable else "IDENTITY_REQUIRED"
            ),
            "decision_reason": reason or ("AMERICAN_WIN_PERCENT_REQUIRES_OPEN_PROBABILITY" if not win_percent_representable else ""),
            "p_under": 1.0 - model_probability if valid else math.nan,
            "market_probability_over": market_prob_over,
            "market_probability_under": market_prob_under,
            "ev_over": ev_over, "ev_under": ev_under,
            "edge_over": edge_over, "edge_under": edge_under,
            "candidate_policy_name": "RAW_PREDICTION_COLLECTION",
            "candidate_policy_version": "NO_FILTERS",
        }
        rows.append(decision)
    decisions = pd.DataFrame(rows)
    valid_rows = decisions[decisions._prediction_valid].copy()
    mapping_failed = valid_rows.player_mapping_status.isin({"CATALOG_MISSING", "AMBIGUOUS", "OTHER"})
    upload_rows = decisions[decisions.raw_export_status.eq("RAW_EXPORT_CANDIDATE") | decisions.raw_export_status.eq("MAPPED_UNIQUE_NAME_CATALOG_TEAM_LAG")].copy()
    summary = {
        "lane": lane.upper(), "mode": "RAW_PREDICTION_COLLECTION", "slate_date": slate_date,
        "source_rows": int(len(source)), "valid_model_predictions": int(len(valid_rows)),
        "mapped_upload_predictions": int(len(upload_rows)),
        "mapping_status_counts": valid_rows.player_mapping_status.value_counts().to_dict(),
        "mapping_status_unique_players": valid_rows.groupby("player_mapping_status").player_id.nunique().to_dict(),
        "mapping_excluded_predictions": int(mapping_failed.sum()),
        "mapping_only_excluded_predictions": int((mapping_failed & valid_rows._win_percent_representable).sum()),
        "mapping_exclusions_also_unrepresentable": int((mapping_failed & ~valid_rows._win_percent_representable).sum()),
        "invalid_model_predictions": int((~decisions._prediction_valid).sum()),
        "win_percent_unrepresentable": int(decisions.raw_export_status.eq("WIN_PERCENT_UNREPRESENTABLE").sum()),
        "integrity_or_identity_excluded": int((decisions.raw_export_status.eq("INVALID_MODEL_PREDICTION") & decisions._prediction_valid).sum()),
        "valid_prediction_rows_excluded_from_thin_csv_for_mapping": int((mapping_failed & valid_rows._win_percent_representable).sum()),
        "rows_by_line": {str(k): int(v) for k, v in upload_rows.line.value_counts().sort_index().items()},
        "sportsbook_over_quotes": int(pd.to_numeric(valid_rows.get("price_over", pd.Series(index=valid_rows.index, dtype=float)), errors="coerce").notna().sum()),
        "sportsbook_under_quotes": int(pd.to_numeric(valid_rows.get("price_under", pd.Series(index=valid_rows.index, dtype=float)), errors="coerce").notna().sum()),
        "challengers_included": False,
    }
    return decisions, upload_rows, summary


def select_raw_sog(
    prediction_path: Path, market_path: Path, names: pd.DataFrame, schedule: pd.DataFrame, *,
    slate_date: str, source_sha256: str, observation_id: str,
    observation_manifest_sha256: str, capture_timestamp_utc: str,
    team_map: dict[str, str], player_map: dict[tuple[str, str], str],
    unique_name_map: dict[str, str], ambiguous_team_keys: set[tuple[str, str]],
    ambiguous_names: set[str], allowed_bets: dict[str, set[str]], parent_run_id: str,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    wide = pd.read_csv(prediction_path)
    if "game_date" not in wide or set(wide.game_date.astype(str)) != {slate_date}:
        raise ValueError("SOG_RAW_PREDICTION_SLATE_MISMATCH")
    import re
    probability_columns = {}
    for column in wide.columns:
        match = re.fullmatch(r"p_over_(\d+)(?:[._](\d+))?", str(column))
        if match:
            probability_columns[column] = float(match.group(1) + ("." + match.group(2) if match.group(2) else ""))
    if not probability_columns:
        raise ValueError("SOG_RAW_PROBABILITY_COLUMNS_MISSING")
    market = pd.read_csv(market_path)
    market_required = {"player_id", "game_id", "line", "p_over"}
    if market_required - set(market.columns):
        raise ValueError("SOG_MARKET_ATTACHMENT_SCHEMA_MISSING")
    market = market.drop_duplicates(["player_id", "game_id", "line"])
    long = wide.melt(
        id_vars=[c for c in wide.columns if c not in probability_columns],
        value_vars=list(probability_columns), var_name="_probability_column", value_name="p_over",
    )
    long["line"] = long._probability_column.map(probability_columns)
    long = long.drop(columns=["_probability_column"])
    market_extras = [c for c in market.columns if c not in long.columns and c not in {"full_name", "team_id"}]
    long = long.merge(
        market[["player_id", "game_id", "line"] + market_extras],
        on=["player_id", "game_id", "line"], how="left", validate="one_to_one",
    )
    if "poisson_source" in long:
        long["model_identity"] = "NHL_SOG_POISSON_BASELINE_V1_REFERENCE"
    else:
        long["model_identity"] = "NHL_SOG_REFERENCE_PREDICTION"
    return select_raw_lane(
        long, names, schedule, lane="shots_on_goal", slate_date=slate_date,
        source_sha256=source_sha256, observation_id=observation_id,
        observation_manifest_sha256=observation_manifest_sha256,
        capture_timestamp_utc=capture_timestamp_utc, team_map=team_map,
        player_map=player_map, unique_name_map=unique_name_map,
        ambiguous_team_keys=ambiguous_team_keys, ambiguous_names=ambiguous_names,
        allowed_bets=allowed_bets, parent_run_id=parent_run_id,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--slate-date", required=True)
    parser.add_argument("--points-csv", type=Path, default=Path("nhl/site/data/points_with_market.csv"))
    parser.add_argument("--saves-csv", type=Path, default=Path("nhl/site/data/saves_with_market.csv"))
    parser.add_argument("--names-csv", type=Path, required=True)
    parser.add_argument("--package-dir", type=Path, required=True)
    parser.add_argument("--catalog-dir", type=Path, required=True)
    parser.add_argument("--odds-observation-dir", type=Path, required=True)
    parser.add_argument("--sog-candidates-csv", type=Path)
    parser.add_argument("--sog-predictions-csv", type=Path)
    parser.add_argument("--sog-market-csv", type=Path, default=Path("nhl/site/data/sog_with_market.csv"))
    parser.add_argument("--mode", choices=("raw", "filtered"), default="raw",
                        help="Raw valid reference predictions by default; filtered applies the optional legacy policies.")
    parser.add_argument("--out-dir", type=Path, default=Path("tmp/cards"))
    parser.add_argument("--combined-props-csv", type=Path, required=True)
    parser.add_argument("--points-policy", type=Path, default=DEFAULT_POLICIES["points"])
    parser.add_argument("--saves-policy", type=Path, default=DEFAULT_POLICIES["saves"])
    args = parser.parse_args()

    today = datetime.now(ZoneInfo("America/Los_Angeles")).date().isoformat()
    require_current_slate(args.slate_date, current_date=today)
    catalog_dir = Path(args.catalog_dir)
    spec, team_map, player_map, allowed_bets = load_catalogs(catalog_dir)
    ambiguous = ambiguous_player_bindings(catalog_dir)
    ambiguous_names = ambiguous_player_names(catalog_dir)
    unique_name_map = unique_player_codes_by_name(catalog_dir)
    names = pd.read_csv(args.names_csv)
    schedule = pd.read_csv(Path(args.package_dir) / "schedule_event_identity.csv")
    if set(schedule.game_date.astype(str)) != {args.slate_date}:
        raise ValueError("PACKAGE_SLATE_MISMATCH")
    observation_id, observation_manifest_sha256, capture_timestamp = _load_observation(args.odds_observation_dir)
    lane_sources = [(lane, path) for lane, path in (("points", args.points_csv), ("saves", args.saves_csv))]
    run_ids = set()
    for _, source_path in lane_sources:
        source_frame = pd.read_csv(source_path)
        if "parent_daily_run_id" in source_frame:
            run_ids.update(source_frame.parent_daily_run_id.dropna().astype(str).unique())
    if len(run_ids) != 1:
        raise ValueError(f"CURRENT_DAILY_RUN_ID_NOT_UNIQUE:{sorted(run_ids)}")
    parent_run_id = next(iter(run_ids))
    output_dir = Path(args.out_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    prop_outputs: list[pd.DataFrame] = []
    lane_summaries = {}
    mapping_exceptions: list[pd.DataFrame] = []
    for lane, source_path, policy_path in (
        ("points", args.points_csv, args.points_policy),
        ("saves", args.saves_csv, args.saves_policy),
    ):
        if args.mode == "raw":
            decisions, selected, summary = select_raw_lane(
                pd.read_csv(source_path), names, schedule, lane=lane,
                slate_date=args.slate_date, source_sha256=_sha256(source_path),
                observation_id=observation_id,
                observation_manifest_sha256=observation_manifest_sha256,
                capture_timestamp_utc=capture_timestamp,
                team_map=team_map, player_map=player_map,
                unique_name_map=unique_name_map, ambiguous_team_keys=ambiguous,
                ambiguous_names=ambiguous_names, allowed_bets=allowed_bets,
                parent_run_id=parent_run_id,
            )
            decisions_path = output_dir / f"nhl_{lane}_raw_prediction_decisions_{args.slate_date}.csv"
            selected_path = output_dir / f"nhl_{lane}_raw_mapped_predictions_{args.slate_date}.csv"
            summary_path = output_dir / f"nhl_{lane}_raw_prediction_summary_{args.slate_date}.json"
            mapping_exceptions.append(decisions[
                decisions._prediction_valid & decisions.player_mapping_status.isin({"CATALOG_MISSING", "AMBIGUOUS", "OTHER"})
            ].copy())
        else:
            policy = load_active_policy(policy_path, lane=lane)
            decisions, selected, summary = select_lane(
                pd.read_csv(source_path), names, schedule, lane=lane,
                slate_date=args.slate_date, policy=policy,
                policy_sha256=_sha256(policy_path),
                source_artifact_sha256=_sha256(source_path),
                odds_observation_id=observation_id,
                odds_observation_manifest_sha256=observation_manifest_sha256,
                capture_timestamp_utc=capture_timestamp,
                team_map=team_map, player_map=player_map,
                ambiguous_player_keys=ambiguous, allowed_bets=allowed_bets,
            )
            decisions_path = output_dir / f"nhl_{lane}_candidate_decisions_{args.slate_date}.csv"
            selected_path = output_dir / f"nhl_{lane}_candidates_{args.slate_date}.csv"
            summary_path = output_dir / f"nhl_{lane}_candidate_summary_{args.slate_date}.json"
        decisions.to_csv(decisions_path, index=False)
        selected.to_csv(selected_path, index=False)
        summary.update({
            "decision_ledger_csv": str(decisions_path), "selected_candidates_csv": str(selected_path),
            "source_csv": str(source_path), "source_artifact_sha256": _sha256(source_path),
            "odds_observation_id": observation_id,
            "odds_observation_manifest_sha256": observation_manifest_sha256,
            "capture_timestamp_utc": capture_timestamp,
        })
        summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
        lane_summaries[lane] = summary
        prop_outputs.append(selected)

    sog_summary: dict[str, Any] = {}
    if args.mode == "raw":
        sog_path = args.sog_predictions_csv or (
            ROOT / "backend/nhl/data/processed/daily_runs" / parent_run_id / "sog_predictions_wide_calibrated.csv"
        )
        if not Path(sog_path).is_file():
            raise FileNotFoundError(f"CURRENT_RUN_SOG_PREDICTIONS_MISSING:{sog_path}")
        sog, sog_mapped, sog_summary = select_raw_sog(
            Path(sog_path), args.sog_market_csv, names, schedule,
            slate_date=args.slate_date, source_sha256=_sha256(Path(sog_path)),
            observation_id=observation_id,
            observation_manifest_sha256=observation_manifest_sha256,
            capture_timestamp_utc=capture_timestamp,
            team_map=team_map, player_map=player_map,
            unique_name_map=unique_name_map, ambiguous_team_keys=ambiguous,
            ambiguous_names=ambiguous_names, allowed_bets=allowed_bets,
            parent_run_id=parent_run_id,
        )
        sog_decisions_path = output_dir / f"nhl_sog_raw_prediction_decisions_{args.slate_date}.csv"
        sog_selected_path = output_dir / f"nhl_sog_raw_mapped_predictions_{args.slate_date}.csv"
        sog_summary_path = output_dir / f"nhl_sog_raw_prediction_summary_{args.slate_date}.json"
        sog.to_csv(sog_decisions_path, index=False)
        sog_mapped.to_csv(sog_selected_path, index=False)
        sog_summary.update({
            "source_csv": str(sog_path), "source_artifact_sha256": _sha256(Path(sog_path)),
            "market_attachment_csv": str(args.sog_market_csv),
            "market_attachment_sha256": _sha256(args.sog_market_csv),
            "decision_ledger_csv": str(sog_decisions_path),
            "mapped_predictions_csv": str(sog_selected_path),
            "odds_observation_id": observation_id,
            "capture_timestamp_utc": capture_timestamp,
        })
        sog_summary_path.write_text(json.dumps(sog_summary, indent=2, sort_keys=True) + "\n")
        mapping_exceptions.append(sog[
            sog._prediction_valid & sog.player_mapping_status.isin({"CATALOG_MISSING", "AMBIGUOUS", "OTHER"})
        ].copy())
        prop_outputs.append(sog_mapped)
    else:
        sog = _prepare_sog(args.sog_candidates_csv, slate_date=args.slate_date, names=names)
        if not sog.empty:
            prop_outputs.append(sog)
    combined = pd.concat(prop_outputs, ignore_index=True, sort=False) if prop_outputs else pd.DataFrame()
    if len(combined):
        for required in ("game_date", "game_id", "player_id", "player_name", "team", "market", "line", "model_pick", "model_side_prob"):
            if required not in combined:
                raise ValueError(f"COMBINED_PROP_INPUT_MISSING:{required}")
        combined.to_csv(args.combined_props_csv, index=False)
    else:
        combined.to_csv(args.combined_props_csv, index=False)
    exception_path = output_dir / f"nhl_raw_8rain_mapping_exceptions_{args.slate_date}.csv"
    exception_frame = pd.concat(mapping_exceptions, ignore_index=True, sort=False) if mapping_exceptions else pd.DataFrame()
    if len(exception_frame):
        exception_frame["NHL_player_ID"] = exception_frame.get("player_id")
        exception_frame["NHL_player_name"] = exception_frame.get("player_name")
        exception_frame["current_NHL_team"] = exception_frame.get("team")
        exception_frame["8rain_lookup_attempts"] = exception_frame.get("mapping_lookup_attempts")
        exception_frame["status"] = exception_frame.get("player_mapping_status")
        exception_frame.to_csv(exception_path, index=False)
    else:
        pd.DataFrame(columns=["NHL_player_ID", "NHL_player_name", "current_NHL_team", "8rain_lookup_attempts", "status"]).to_csv(exception_path, index=False)
    report = {
        "slate_date": args.slate_date, "mode": "RAW_PREDICTION_COLLECTION" if args.mode == "raw" else "OPTIONAL_FILTERED_POLICY",
        "catalog_dir": str(catalog_dir),
        "catalog_market_codes": sorted(allowed_bets), "challengers_included": False,
        "sog_selected_rows": int(len(sog_mapped) if args.mode == "raw" else len(sog)),
        "combined_props_csv": str(args.combined_props_csv), "mapping_exception_csv": str(exception_path),
        "sog": sog_summary, "lanes": lane_summaries,
    }
    report_path = output_dir / f"nhl_raw_8rain_export_summary_{args.slate_date}.json"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
