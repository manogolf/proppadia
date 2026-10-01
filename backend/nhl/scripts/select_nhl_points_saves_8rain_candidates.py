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
    ambiguous_player_bindings, load_catalogs, normalize_player_name,
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
    names = pd.read_csv(args.names_csv)
    schedule = pd.read_csv(Path(args.package_dir) / "schedule_event_identity.csv")
    if set(schedule.game_date.astype(str)) != {args.slate_date}:
        raise ValueError("PACKAGE_SLATE_MISMATCH")
    observation_id, observation_manifest_sha256, capture_timestamp = _load_observation(args.odds_observation_dir)
    output_dir = Path(args.out_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    prop_outputs: list[pd.DataFrame] = []
    lane_summaries = {}
    for lane, source_path, policy_path in (
        ("points", args.points_csv, args.points_policy),
        ("saves", args.saves_csv, args.saves_policy),
    ):
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
    report = {
        "slate_date": args.slate_date, "catalog_dir": str(catalog_dir),
        "catalog_market_codes": sorted(allowed_bets), "challengers_included": False,
        "sog_selected_rows": int(len(sog)), "combined_policy_selected_rows": int(len(combined)),
        "combined_props_csv": str(args.combined_props_csv), "lanes": lane_summaries,
    }
    report_path = output_dir / f"nhl_points_saves_8rain_candidate_summary_{args.slate_date}.json"
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
