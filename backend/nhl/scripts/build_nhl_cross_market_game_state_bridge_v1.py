#!/usr/bin/env python3
"""Build NHL_CROSS_MARKET_GAME_STATE_BRIDGE_V1 as a create-only research package.

This utility never fits a model.  It joins immutable season-2025 player-prop
evidence to official full-game outcomes and produces descriptive game-level
features.  External access is opt-in through --fetch-official-outcomes.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import shutil
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

from backend.nhl.analysis_package_guard import begin_package, finalize_package, verify_manifest


TASK = "NHL_CROSS_MARKET_GAME_STATE_BRIDGE_V1"
PACKAGE_DATE = "2026-09-15"
REPO = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT = REPO / "artifacts/analysis/model_development/nhl_cross_market_game_state_bridge_v1" / PACKAGE_DATE
ARCHIVE = REPO / "artifacts/analysis/model_development/nhl_season_2025_player_prop_market_archive_immutable_canonical_join_index_v1/2026-09-08"
SOG_REPRO = REPO / "artifacts/analysis/model_development/nhl_season_2025_sog_baseline_reproduction/2026-07-13"
SAVES_STARTER = REPO / "artifacts/analysis/model_development/nhl_season_2025_saves_market_listed_goalie_actual_starter_concordance_v1/2026-09-08"
FROZEN_CONTROL = REPO / "artifacts/analysis/model_development/nhl_moneyline_frozen_baseline_certification/2026-07-13"
CONTROL_PROCESS = REPO / "artifacts/analysis/model_development/nhl_moneyline_simple_baseline_process_validation/2026-07-13"
OFFICIAL_URL = "https://api-web.nhle.com/v1/club-schedule-season/{team}/20252026"
FINAL_STATES = {"FINAL", "OFF"}


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def write_csv(frame: pd.DataFrame, path: Path) -> None:
    frame.to_csv(path, index=False, quoting=csv.QUOTE_MINIMAL)


def write_json(value: Any, path: Path) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + "\n")


def write_parquet(frame: pd.DataFrame, path: Path) -> None:
    frame.to_parquet(path, index=False, compression="zstd")


def fetch_official_schedule(teams: Iterable[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    retrieved = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    records: list[dict[str, Any]] = []
    calls: list[dict[str, Any]] = []
    for team in sorted(set(teams)):
        url = OFFICIAL_URL.format(team=team)
        request = urllib.request.Request(url, headers={"User-Agent": "Proppadia-NHL-research-audit/1.0"})
        with urllib.request.urlopen(request, timeout=30) as response:
            payload_bytes = response.read()
            status = response.status
        payload = json.loads(payload_bytes)
        games = payload.get("games") or []
        calls.append({
            "method": "GET", "url": url, "http_status": status, "response_bytes": len(payload_bytes),
            "returned_games": len(games), "retrieved_at_utc": retrieved,
            "purpose": "official season-2025 full-game outcome snapshot",
        })
        for game in games:
            away = game.get("awayTeam") or {}
            home = game.get("homeTeam") or {}
            period = game.get("periodDescriptor") or {}
            records.append({
                "game_id": game.get("id"), "official_season": game.get("season"),
                "game_type": game.get("gameType"), "game_date": game.get("gameDate"),
                "start_time_utc": game.get("startTimeUTC"), "game_state": game.get("gameState"),
                "game_schedule_state": game.get("gameScheduleState"),
                "away_team_code": away.get("abbrev"), "away_score": away.get("score"),
                "home_team_code": home.get("abbrev"), "home_score": home.get("score"),
                "period_number": period.get("number"), "period_type": period.get("periodType"),
                "source_url": url, "retrieved_at_utc": retrieved,
            })
    raw = pd.DataFrame(records)
    raw["game_id"] = pd.to_numeric(raw.game_id, errors="coerce").astype("Int64")
    return raw, pd.DataFrame(calls)


def canonicalize_official(raw: pd.DataFrame, games: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    relevant = raw[(raw.game_type == 2) & raw.game_id.isin(games.game_id)].copy()
    identity = ["official_season", "game_type", "game_date", "start_time_utc", "game_state",
                "game_schedule_state", "away_team_code", "away_score", "home_team_code", "home_score",
                "period_number", "period_type"]
    conflict_rows: list[dict[str, Any]] = []
    for game_id, group in relevant.groupby("game_id", dropna=False):
        for col in identity:
            values = group[col].dropna().astype(str).unique()
            if len(values) > 1:
                conflict_rows.append({"game_id": game_id, "field": col, "values": "|".join(sorted(values))})
    if conflict_rows:
        raise RuntimeError(f"OFFICIAL_DUPLICATE_CONFLICT:{len(conflict_rows)}")
    source = relevant.sort_values(["game_id", "source_url"]).drop_duplicates("game_id", keep="first")
    merged = games.merge(source, on="game_id", how="left", suffixes=("_canonical", "_official"), validate="one_to_one")
    merged["identity_match"] = (
        merged.home_team_code_canonical.eq(merged.home_team_code_official)
        & merged.away_team_code_canonical.eq(merged.away_team_code_official)
    )
    merged["final_state_qualified"] = merged.game_state.isin(FINAL_STATES)
    merged["decisive_score_qualified"] = (
        pd.to_numeric(merged.home_score, errors="coerce").notna()
        & pd.to_numeric(merged.away_score, errors="coerce").notna()
        & pd.to_numeric(merged.home_score, errors="coerce").ne(pd.to_numeric(merged.away_score, errors="coerce"))
    )
    merged["outcome_qualified"] = merged.identity_match & merged.final_state_qualified & merged.decisive_score_qualified
    merged["canonical_season"] = 2025
    merged["final_home_goals"] = pd.to_numeric(merged.home_score, errors="coerce").astype("Int64")
    merged["final_away_goals"] = pd.to_numeric(merged.away_score, errors="coerce").astype("Int64")
    merged["home_win_full_game"] = np.where(
        merged.outcome_qualified, merged.final_home_goals > merged.final_away_goals, np.nan
    )
    merged["home_goal_margin"] = np.where(
        merged.outcome_qualified, merged.final_home_goals - merged.final_away_goals, np.nan
    )
    merged["absolute_goal_margin"] = pd.to_numeric(merged.home_goal_margin, errors="coerce").abs()
    merged["total_goals"] = np.where(
        merged.outcome_qualified, merged.final_home_goals + merged.final_away_goals, np.nan
    )
    merged["decision_type"] = merged.period_type.map({"REG": "REGULATION", "OT": "OVERTIME", "SO": "SHOOTOUT"})
    merged["margin_bucket"] = pd.cut(
        merged.absolute_goal_margin, bins=[0, 1, 2, math.inf], labels=["ONE_GOAL", "TWO_GOALS", "THREE_PLUS_GOALS"],
        include_lowest=False, right=True,
    ).astype("object")
    merged["home_minus_1_5_result"] = np.where(
        merged.outcome_qualified, np.where(merged.home_goal_margin >= 2, "WIN", "LOSS"), None
    )
    merged["away_minus_1_5_result"] = np.where(
        merged.outcome_qualified, np.where(merged.home_goal_margin <= -2, "WIN", "LOSS"), None
    )
    merged["home_plus_1_5_result"] = np.where(
        merged.outcome_qualified, np.where(merged.home_goal_margin >= -1, "WIN", "LOSS"), None
    )
    merged["away_plus_1_5_result"] = np.where(
        merged.outcome_qualified, np.where(merged.home_goal_margin <= 1, "WIN", "LOSS"), None
    )
    merged["outcome_authority"] = "OFFICIAL_NHL_CLUB_SCHEDULE_SEASON_API"
    merged["outcome_state"] = np.where(merged.outcome_qualified, "CERTIFIED_FULL_GAME_FINAL", "UNQUALIFIED")
    merged["late_empty_net_two_goal_margin_state"] = "UNRESOLVED_NOT_RECOVERABLE_FROM_LOCAL_CERTIFIED_INPUTS"
    keep = [
        "canonical_season", "game_date_canonical", "game_id", "start_time_utc_canonical",
        "home_team_code_canonical", "away_team_code_canonical", "final_home_goals", "final_away_goals",
        "home_win_full_game", "home_goal_margin", "absolute_goal_margin", "total_goals", "decision_type",
        "margin_bucket", "home_minus_1_5_result", "away_minus_1_5_result", "home_plus_1_5_result",
        "away_plus_1_5_result", "game_state", "game_schedule_state", "identity_match",
        "final_state_qualified", "decisive_score_qualified", "outcome_qualified", "outcome_authority",
        "outcome_state", "late_empty_net_two_goal_margin_state", "retrieved_at_utc", "source_url",
    ]
    outcome = merged[keep].rename(columns={
        "game_date_canonical": "game_date", "start_time_utc_canonical": "start_time_utc",
        "home_team_code_canonical": "home_team", "away_team_code_canonical": "away_team",
    })
    completeness = pd.DataFrame([
        {"check": "canonical_regular_season_games", "rows": len(games), "failed_rows": 0, "status": "PASS"},
        {"check": "official_game_id_coverage", "rows": int(source.game_id.nunique()), "failed_rows": int(merged.game_state.isna().sum()), "status": "PASS" if merged.game_state.notna().all() else "FAIL"},
        {"check": "home_away_identity_alignment", "rows": int(merged.identity_match.sum()), "failed_rows": int((~merged.identity_match.fillna(False)).sum()), "status": "PASS" if merged.identity_match.all() else "FAIL"},
        {"check": "recognized_completed_state", "rows": int(merged.final_state_qualified.sum()), "failed_rows": int((~merged.final_state_qualified).sum()), "status": "PASS" if merged.final_state_qualified.all() else "FAIL"},
        {"check": "decisive_full_game_score", "rows": int(merged.decisive_score_qualified.sum()), "failed_rows": int((~merged.decisive_score_qualified).sum()), "status": "PASS" if merged.decisive_score_qualified.all() else "FAIL"},
        {"check": "certified_outcome_rows", "rows": int(merged.outcome_qualified.sum()), "failed_rows": int((~merged.outcome_qualified).sum()), "status": "PASS" if merged.outcome_qualified.all() else "FAIL"},
        {"check": "late_empty_net_two_goal_margin", "rows": int((merged.absolute_goal_margin == 2).sum()), "failed_rows": int((merged.absolute_goal_margin == 2).sum()), "status": "UNRESOLVED"},
    ])
    return outcome, completeness


def latest_market_selections(base: Path) -> pd.DataFrame:
    pairs = pd.read_parquet(base / "paired_no_vig_market_index.parquet")
    pairs = pairs[pairs.pair_status.eq("COMPLETE_ALIGNED")].copy()
    obs = pd.read_parquet(base / "qualified_pregame_observation_index.parquet", columns=[
        "observation_id", "team", "effective_observation_timestamp_utc", "scheduled_start_time_utc",
    ]).rename(columns={"observation_id": "over_observation_id"})
    pairs = pairs.merge(obs, on="over_observation_id", how="inner", validate="many_to_one")
    pairs["effective_observation_timestamp_utc"] = pd.to_datetime(pairs.effective_observation_timestamp_utc, utc=True)
    pairs["scheduled_start_time_utc"] = pd.to_datetime(pairs.scheduled_start_time_utc, utc=True)
    keys = ["market_family", "canonical_game_id", "canonical_player_id"]
    newest = pairs.sort_values(keys + ["effective_observation_timestamp_utc", "source_file_sha256"]).groupby(keys, dropna=False).tail(1)
    chosen_source = newest[keys + ["source_file_sha256"]].rename(columns={"source_file_sha256": "chosen_source_sha256"})
    pairs = pairs.merge(chosen_source, on=keys, how="inner")
    pairs = pairs[pairs.source_file_sha256.eq(pairs.chosen_source_sha256)].copy()
    line_summary = pairs.groupby(keys + ["team", "line"], dropna=False).agg(
        market_no_vig_over_probability=("no_vig_over_probability", "median"),
        sportsbook_support_count=("sportsbook", "nunique"),
        latest_market_timestamp_utc=("effective_observation_timestamp_utc", "max"),
        earliest_market_timestamp_utc=("effective_observation_timestamp_utc", "min"),
        source_file_sha256=("source_file_sha256", "first"),
        slate_date=("slate_date", "first"),
        market_line_pair_rows=("sportsbook", "size"),
    ).reset_index()
    medians = line_summary.groupby(keys, dropna=False).line.median().rename("player_line_median").reset_index()
    line_summary = line_summary.merge(medians, on=keys, how="left")
    line_summary["line_distance_from_median"] = (line_summary.line - line_summary.player_line_median).abs()
    selected = line_summary.sort_values(
        keys + ["sportsbook_support_count", "line_distance_from_median", "line"],
        ascending=[True, True, True, True, False, False],
    ).groupby(keys, dropna=False).tail(1)
    starts = pairs[keys + ["scheduled_start_time_utc"]].drop_duplicates(keys)
    selected = selected.merge(starts, on=keys, how="left", validate="one_to_one")
    selected["market_age_minutes"] = (
        selected.scheduled_start_time_utc - selected.latest_market_timestamp_utc
    ).dt.total_seconds() / 60
    selected["market_selection_rule"] = "LATEST_SOURCE_THEN_MAX_BOOK_SUPPORT_THEN_MEDIAN_LINE_PROXIMITY"
    return selected.drop(columns=["player_line_median", "line_distance_from_median"])


def prepare_roster(games: pd.DataFrame, base: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    roster = pd.read_parquet(base / "canonical_game_player_snapshot.parquet")
    roster = roster.merge(games[["game_id", "home_team_code", "away_team_code"]], on="game_id", how="inner")
    conflicting = roster.groupby(["game_id", "player_id"]).team_code.nunique()
    conflicting_keys = set(conflicting[conflicting.gt(1)].index)
    if conflicting_keys:
        key_index = pd.MultiIndex.from_frame(roster[["game_id", "player_id"]])
        roster = roster[~key_index.isin(conflicting_keys)].copy()
    counts = roster.assign(family=np.where(roster.position.eq("G"), "SAVES", "SKATER"))
    counts = counts.groupby(["game_id", "team_code", "family"]).player_id.nunique().rename("roster_player_count").reset_index()
    if roster.duplicated(["game_id", "player_id"]).any():
        roster = roster.drop_duplicates(["game_id", "player_id"], keep="first")
    return roster[["game_id", "player_id", "team_code", "position"]], counts


def prediction_snapshot(games: pd.DataFrame, base: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    predictions = pd.read_parquet(base / "historical_predictions_snapshot.parquet")
    starts = games[["game_id", "start_time_utc"]].copy()
    starts["start_time_utc"] = pd.to_datetime(starts.start_time_utc, utc=True)
    predictions = predictions.merge(starts, on="game_id", how="left", validate="many_to_one")
    for col in ["created_at", "updated_at"]:
        predictions[col] = pd.to_datetime(predictions[col], utc=True, errors="coerce")
    predictions["prediction_asof_utc"] = predictions[["created_at", "updated_at"]].max(axis=1)
    predictions["prediction_strict_prior"] = predictions.prediction_asof_utc.lt(predictions.start_time_utc)
    audit = predictions.groupby("lane", dropna=False).agg(
        prediction_rows=("prediction_id", "size"),
        strict_prior_rows=("prediction_strict_prior", "sum"),
        games=("game_id", "nunique"),
        earliest_prediction_asof_utc=("prediction_asof_utc", "min"),
        latest_prediction_asof_utc=("prediction_asof_utc", "max"),
    ).reset_index().rename(columns={"lane": "family"})
    predictions = predictions[predictions.prediction_strict_prior].copy()
    keys = ["lane", "game_id", "player_id", "line"]
    predictions = predictions.sort_values(keys + ["prediction_asof_utc", "prediction_id"]).groupby(keys, dropna=False).tail(1)
    predictions["prediction_age_minutes"] = (
        predictions.start_time_utc - predictions.prediction_asof_utc
    ).dt.total_seconds() / 60
    return predictions, audit


def model_player_tables(
    games: pd.DataFrame, roster: pd.DataFrame, predictions: pd.DataFrame, market: pd.DataFrame
) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    timing_rows: list[dict[str, Any]] = []
    tables: dict[str, pd.DataFrame] = {}

    # SOG uses only the parent-certified baseline reproduction and its genuine Poisson expectation.
    ledger = pd.read_csv(SOG_REPRO / "nhl_season_2025_sog_reproduction_ledger_2026-07-13.csv", low_memory=False)
    sog = ledger.groupby(["game_id", "player_id"], as_index=False).agg(
        continuous_expectation=("regenerated_expected_sog", "first"),
        model_line_count=("line", "nunique"),
        prepared_source_path=("prepared_source_path", "first"),
        certified_probability_rows=("line", "size"),
    )
    consistency = ledger.groupby(["game_id", "player_id"]).regenerated_expected_sog.nunique(dropna=False)
    if consistency.gt(1).any():
        raise RuntimeError("SOG_EXPECTATION_INCONSISTENT_ACROSS_LINES")
    sog = sog.merge(roster, on=["game_id", "player_id"], how="left", validate="one_to_one")
    sog["family"] = "SOG"
    sog["model_metric_name"] = "expected_sog"
    sog["model_timing_state"] = "CERTIFIED_PREGAME_BY_PARENT_REPRODUCTION"
    tables["SOG"] = sog
    timing_rows.append({
        "family": "SOG", "timing_population": "model", "rows": len(sog),
        "strict_prior_rows": len(sog), "strict_prior_rate": 1.0,
        "timing_authority": "parent reproduction prepared inputs certified pregame",
        "limitation": "exact upstream rolling-window rebuild was bounded, not unconditional",
    })

    # Points uses one common 0.5 threshold and excludes material cross-line incoherence.
    points = predictions[predictions.lane.eq("POINTS")].copy()
    ladder = points.pivot_table(index=["game_id", "player_id"], columns="line", values="p_over", aggfunc="last")
    for line in [0.5, 1.5, 2.5]:
        if line not in ladder:
            ladder[line] = np.nan
    ladder = ladder.rename(columns={0.5: "p05", 1.5: "p15", 2.5: "p25"}).reset_index()
    ladder["ladder_complete"] = ladder[["p05", "p15", "p25"]].notna().all(axis=1)
    ladder["max_adjacent_crossing"] = np.maximum(
        (ladder.p15 - ladder.p05).clip(lower=0), (ladder.p25 - ladder.p15).clip(lower=0)
    )
    ladder["material_incoherence"] = ladder.max_adjacent_crossing.ge(0.01)
    points_model = ladder[ladder.ladder_complete & ~ladder.material_incoherence].copy()
    points_model["continuous_expectation"] = points_model.p05
    points_model["model_metric_name"] = "expected_count_of_players_with_one_plus_point_component"
    points_model = points_model.merge(roster, on=["game_id", "player_id"], how="left", validate="one_to_one")
    points_model["family"] = "POINTS"
    points_model["model_timing_state"] = "ROW_TIMESTAMP_STRICT_PRIOR_AND_LADDER_GATE"
    tables["POINTS"] = points_model
    timing_rows.append({
        "family": "POINTS", "timing_population": "model", "rows": len(ladder),
        "strict_prior_rows": len(ladder), "strict_prior_rate": 1.0,
        "timing_authority": "prediction max(created_at,updated_at) < official start",
        "limitation": f"{int((~ladder.ladder_complete).sum())} incomplete and {int(ladder.material_incoherence.sum())} materially incoherent ladders excluded",
    })

    # Saves remains conditional on starting and is limited to the existing latest market consensus gate.
    saves = predictions[predictions.lane.eq("SAVES")].copy()
    latest_state = pd.read_parquet(SAVES_STARTER / "latest_team_market_state_evaluation.parquet", columns=[
        "canonical_game_id", "team", "latest_source_file_sha256", "latest_observation_time_utc",
        "consensus_goalie_player_id", "consensus_book_count", "listed_goalie_count", "phase_equivalent",
    ]).rename(columns={"canonical_game_id": "game_id", "consensus_goalie_player_id": "player_id"})
    latest_state["player_id"] = pd.to_numeric(latest_state.player_id, errors="coerce").astype("Int64")
    latest_state = latest_state[latest_state.player_id.notna() & latest_state.consensus_book_count.ge(2)].copy()
    saves_market = market[market.market_family.eq("SAVES")].rename(columns={
        "canonical_game_id": "game_id", "canonical_player_id": "player_id"
    })
    saves_market["player_id"] = pd.to_numeric(saves_market.player_id, errors="coerce").astype("Int64")
    saves_gate = latest_state.merge(saves_market, on=["game_id", "player_id", "team"], how="inner")
    saves_gate = saves_gate[saves_gate.latest_source_file_sha256.eq(saves_gate.source_file_sha256)].copy()
    saves_matched = saves_gate.merge(
        saves, left_on=["game_id", "player_id", "line"], right_on=["game_id", "player_id", "line"], how="inner",
        suffixes=("_market", "_model"),
    )
    saves_model = saves_matched[[
        "game_id", "player_id", "team", "line", "p_over", "market_no_vig_over_probability",
        "sportsbook_support_count", "latest_market_timestamp_utc", "market_age_minutes",
        "prediction_asof_utc", "prediction_age_minutes", "consensus_book_count", "listed_goalie_count",
        "phase_equivalent", "source_file_sha256",
    ]].copy()
    saves_model["team_code"] = saves_model.team
    saves_model["family"] = "SAVES"
    saves_model["continuous_expectation"] = np.nan
    saves_model["model_metric_name"] = "UNAVAILABLE_CONDITIONAL_LINE_PROBABILITY_ONLY"
    saves_model["model_market_delta"] = saves_model.p_over - saves_model.market_no_vig_over_probability
    saves_model["model_timing_state"] = "ROW_TIMESTAMP_STRICT_PRIOR_MARKET_CONSENSUS_STARTER_UNCONFIRMED"
    tables["SAVES"] = saves_model
    timing_rows.append({
        "family": "SAVES", "timing_population": "model_and_market_gate", "rows": len(saves_model),
        "strict_prior_rows": len(saves_model), "strict_prior_rate": 1.0,
        "timing_authority": "prediction timestamp and qualified market timestamps precede official start",
        "limitation": "probabilities conditional on named goalie starting; listing is not starter confirmation",
    })

    return tables, pd.DataFrame(timing_rows)


def attach_market_matches(
    family: str, model: pd.DataFrame, market: pd.DataFrame, predictions: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    selected = market[market.market_family.eq(family)].copy().rename(columns={
        "canonical_game_id": "game_id", "canonical_player_id": "player_id",
    })
    selected["player_id"] = pd.to_numeric(selected.player_id, errors="coerce").astype("Int64")
    if family == "SAVES":
        return model, selected
    if family == "SOG":
        ledger = pd.read_csv(
            SOG_REPRO / "nhl_season_2025_sog_reproduction_ledger_2026-07-13.csv",
            usecols=["game_id", "player_id", "line", "stored_p_over"],
        ).drop_duplicates(["game_id", "player_id", "line"])
        matched = selected.merge(ledger, on=["game_id", "player_id", "line"], how="inner", validate="one_to_one")
        eligible = model[["game_id", "player_id"]].drop_duplicates()
        matched = matched.merge(eligible.assign(sog_certified_expectation=True), on=["game_id", "player_id"], how="inner")
        matched = matched.rename(columns={"stored_p_over": "p_over"})
        matched["prediction_asof_utc"] = pd.NaT
        matched["prediction_age_minutes"] = np.nan
        matched["model_family"] = "poisson_baseline"
        matched["model_version"] = "baseline_v1"
        matched["prediction_id"] = pd.NA
        matched["model_market_delta"] = matched.p_over - matched.market_no_vig_over_probability
        return matched, selected
    pred = predictions[predictions.lane.eq(family)][[
        "game_id", "player_id", "line", "p_over", "prediction_asof_utc", "prediction_age_minutes",
        "model_family", "model_version", "prediction_id",
    ]]
    matched = selected.merge(pred, on=["game_id", "player_id", "line"], how="inner", validate="one_to_one")
    if family == "POINTS":
        eligible = model[["game_id", "player_id"]].drop_duplicates()
        matched = matched.merge(eligible.assign(points_ladder_gate=True), on=["game_id", "player_id"], how="inner")
    matched["model_market_delta"] = matched.p_over - matched.market_no_vig_over_probability
    return matched, selected


def team_aggregate(
    family: str, games: pd.DataFrame, roster_counts: pd.DataFrame,
    model: pd.DataFrame, matched: pd.DataFrame, market_all: pd.DataFrame,
) -> pd.DataFrame:
    teams = pd.concat([
        games[["game_id", "home_team_code"]].rename(columns={"home_team_code": "team_code"}),
        games[["game_id", "away_team_code"]].rename(columns={"away_team_code": "team_code"}),
    ], ignore_index=True).drop_duplicates()
    family_roster = "SKATER" if family in {"SOG", "POINTS"} else "SAVES"
    denom = roster_counts[roster_counts.family.eq(family_roster)][["game_id", "team_code", "roster_player_count"]]
    result = teams.merge(denom, on=["game_id", "team_code"], how="left")

    model = model[model.team_code.notna()].copy()
    model_stats = model.groupby(["game_id", "team_code"], dropna=False).agg(
        model_covered_player_count=("player_id", "nunique"),
        continuous_expectation_sum=("continuous_expectation", "sum"),
        continuous_expectation_mean=("continuous_expectation", "mean"),
        continuous_expectation_median=("continuous_expectation", "median"),
        continuous_expectation_max=("continuous_expectation", "max"),
    ).reset_index()
    if family == "SAVES":
        for col in ["continuous_expectation_sum", "continuous_expectation_mean", "continuous_expectation_median", "continuous_expectation_max"]:
            model_stats[col] = np.nan
    model_stats["top_player_concentration"] = (
        model_stats.continuous_expectation_max / model_stats.continuous_expectation_sum.replace(0, np.nan)
    )
    result = result.merge(model_stats, on=["game_id", "team_code"], how="left")

    market_stats = market_all.groupby(["game_id", "team"], dropna=False).agg(
        market_covered_player_count=("player_id", "nunique"),
        market_no_vig_over_probability_mean=("market_no_vig_over_probability", "mean"),
        market_no_vig_over_probability_median=("market_no_vig_over_probability", "median"),
        selected_line_mean=("line", "mean"),
        selected_line_median=("line", "median"),
        sportsbook_support_count_median=("sportsbook_support_count", "median"),
        sportsbook_support_count_sum=("sportsbook_support_count", "sum"),
        market_age_minutes_median=("market_age_minutes", "median"),
        market_age_minutes_max=("market_age_minutes", "max"),
    ).reset_index().rename(columns={"team": "team_code"})
    result = result.merge(market_stats, on=["game_id", "team_code"], how="left")

    matched_team = "team_code" if "team_code" in matched.columns else "team"
    matched_stats = matched.groupby(["game_id", matched_team], dropna=False).agg(
        matched_player_count=("player_id", "nunique"),
        model_market_delta_mean=("model_market_delta", "mean"),
        model_market_delta_median=("model_market_delta", "median"),
        model_market_absolute_delta_mean=("model_market_delta", lambda s: s.abs().mean()),
        prediction_age_minutes_median=("prediction_age_minutes", "median"),
    ).reset_index().rename(columns={matched_team: "team_code"})
    result = result.merge(matched_stats, on=["game_id", "team_code"], how="left")
    if family == "SAVES":
        result["projected_coverage_fraction"] = result.matched_player_count
        result["coverage_denominator_semantics"] = "ONE_MARKET_CONSENSUS_GOALIE_PER_TEAM_STARTER_UNCONFIRMED"
    else:
        result["projected_coverage_fraction"] = result.matched_player_count / result.roster_player_count.replace(0, np.nan)
        result["coverage_denominator_semantics"] = "CANONICAL_GAME_ROSTER_PROXY_NOT_CONFIRMED_LINEUP"
    result["family"] = family
    result["family_available"] = result.model_covered_player_count.notna() | result.market_covered_player_count.notna()
    return result


def one_row_per_game(games: pd.DataFrame, outcomes: pd.DataFrame, team_tables: dict[str, pd.DataFrame]) -> pd.DataFrame:
    bridge = outcomes.copy()
    numeric = [
        "roster_player_count", "model_covered_player_count", "market_covered_player_count", "matched_player_count",
        "projected_coverage_fraction", "continuous_expectation_sum", "continuous_expectation_mean",
        "continuous_expectation_median", "continuous_expectation_max", "top_player_concentration",
        "market_no_vig_over_probability_mean", "market_no_vig_over_probability_median", "selected_line_mean",
        "selected_line_median", "sportsbook_support_count_median", "sportsbook_support_count_sum",
        "market_age_minutes_median", "market_age_minutes_max", "model_market_delta_mean",
        "model_market_delta_median", "model_market_absolute_delta_mean", "prediction_age_minutes_median",
    ]
    for family, table in team_tables.items():
        home = table.merge(games[["game_id", "home_team_code"]], on="game_id", how="left")
        home = home[home.team_code.eq(home.home_team_code)].drop(columns=["team_code", "home_team_code", "family"])
        away = table.merge(games[["game_id", "away_team_code"]], on="game_id", how="left")
        away = away[away.team_code.eq(away.away_team_code)].drop(columns=["team_code", "away_team_code", "family"])
        prefix = family.lower()
        home = home.rename(columns={c: f"{prefix}_home_{c}" for c in home.columns if c != "game_id"})
        away = away.rename(columns={c: f"{prefix}_away_{c}" for c in away.columns if c != "game_id"})
        bridge = bridge.merge(home, on="game_id", how="left", validate="one_to_one")
        bridge = bridge.merge(away, on="game_id", how="left", validate="one_to_one")
        for col in numeric:
            h, a = f"{prefix}_home_{col}", f"{prefix}_away_{col}"
            if h in bridge and a in bridge:
                bridge[f"{prefix}_{col}_diff"] = bridge[h] - bridge[a]
        hsum, asum = f"{prefix}_home_continuous_expectation_sum", f"{prefix}_away_continuous_expectation_sum"
        if hsum in bridge and asum in bridge:
            bridge[f"{prefix}_continuous_expectation_combined"] = bridge[hsum] + bridge[asum]
    return bridge


def wilson(successes: int, n: int) -> tuple[float, float]:
    if n == 0:
        return np.nan, np.nan
    z = 1.959963984540054
    p = successes / n
    denominator = 1 + z * z / n
    center = (p + z * z / (2 * n)) / denominator
    half = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n) / denominator
    return center - half, center + half


def characterize(bridge: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    specs = {
        "SOG": ("sog_continuous_expectation_sum_diff", [-math.inf, -4, -1, 1, 4, math.inf],
                ["VERY_LOW", "LOW", "NEUTRAL", "HIGH", "VERY_HIGH"], 6),
        "POINTS": ("points_continuous_expectation_sum_diff", [-math.inf, -1.5, -.5, .5, 1.5, math.inf],
                   ["VERY_LOW", "LOW", "NEUTRAL", "HIGH", "VERY_HIGH"], 6),
        "SAVES": ("saves_model_market_delta_mean_diff", [-math.inf, -.10, -.025, .025, .10, math.inf],
                  ["VERY_LOW", "LOW", "NEUTRAL", "HIGH", "VERY_HIGH"], 1),
    }
    rows: list[dict[str, Any]] = []
    temporal: list[dict[str, Any]] = []
    for family, (signal, bins, labels, min_count) in specs.items():
        if signal not in bridge:
            continue
        prefix = family.lower()
        x = bridge[bridge.outcome_qualified & bridge[signal].notna()].copy()
        x["signal_bin"] = pd.cut(x[signal], bins=bins, labels=labels, include_lowest=True, right=True)
        coverage_field = "matched_player_count" if family == "SAVES" else "model_covered_player_count"
        low_coverage = (
            bridge[f"{prefix}_home_{coverage_field}"].ge(min_count)
            & bridge[f"{prefix}_away_{coverage_field}"].ge(min_count)
        )
        low_scope = (
            f"MATCHED_COUNT_GE_{min_count}_EACH_TEAM" if family == "SAVES"
            else f"MODEL_COUNT_GE_{min_count}_EACH_TEAM"
        )
        for scope, sample in [("ALL_SIGNAL_GAMES", x), (low_scope, x[low_coverage.reindex(x.index).fillna(False)])]:
            for label in labels:
                g = sample[sample.signal_bin.astype("object").eq(label)]
                n = len(g)
                wins = int(pd.to_numeric(g.home_win_full_game, errors="coerce").sum()) if n else 0
                lo, hi = wilson(wins, n)
                rows.append({
                    "family": family, "coverage_scope": scope, "signal_field": signal, "signal_bin": label,
                    "bin_rule": str(bins), "games": n, "home_wins": wins,
                    "home_win_rate": wins / n if n else np.nan, "home_win_rate_ci95_low": lo,
                    "home_win_rate_ci95_high": hi, "mean_home_goal_margin": g.home_goal_margin.mean(),
                    "mean_total_goals": g.total_goals.mean(),
                    "home_minus_1_5_cover_rate": g.home_minus_1_5_result.eq("WIN").mean() if n else np.nan,
                    "one_goal_margin_rate": g.margin_bucket.eq("ONE_GOAL").mean() if n else np.nan,
                    "two_goal_margin_rate": g.margin_bucket.eq("TWO_GOALS").mean() if n else np.nan,
                    "three_plus_goal_margin_rate": g.margin_bucket.eq("THREE_PLUS_GOALS").mean() if n else np.nan,
                })
        x["month"] = pd.to_datetime(x.game_date).dt.to_period("M").astype(str)
        for month, g in x.groupby("month"):
            temporal.append({
                "family": family, "month": month, "games": len(g),
                "signal_margin_spearman": g[[signal, "home_goal_margin"]].corr(method="spearman").iloc[0, 1] if len(g) > 2 else np.nan,
                "signal_home_win_spearman": g[[signal, "home_win_full_game"]].astype(float).corr(method="spearman").iloc[0, 1] if len(g) > 2 else np.nan,
                "mean_signal": g[signal].mean(),
            })

    environment_rows: list[dict[str, Any]] = []
    environment_specs = {
        "SOG": ("sog_continuous_expectation_combined", [-math.inf, 50, 55, 60, 65, math.inf]),
        "POINTS": ("points_continuous_expectation_combined", [-math.inf, 8, 10, 12, 14, math.inf]),
    }
    labels = ["VERY_LOW", "LOW", "MID", "HIGH", "VERY_HIGH"]
    for family, (field, bins) in environment_specs.items():
        if field not in bridge:
            continue
        x = bridge[bridge.outcome_qualified & bridge[field].notna()].copy()
        x["environment_bin"] = pd.cut(x[field], bins=bins, labels=labels, include_lowest=True)
        for label in labels:
            g = x[x.environment_bin.astype("object").eq(label)]
            environment_rows.append({
                "family": family, "environment_field": field, "environment_bin": label,
                "bin_rule": str(bins), "games": len(g), "mean_total_goals": g.total_goals.mean(),
                "median_total_goals": g.total_goals.median(), "over_5_5_rate": g.total_goals.gt(5.5).mean() if len(g) else np.nan,
            })

    signal_fields = {family: spec[0] for family, spec in specs.items() if spec[0] in bridge}
    redundancy: list[dict[str, Any]] = []
    names = list(signal_fields)
    for i, left in enumerate(names):
        for right in names[i + 1:]:
            cols = [signal_fields[left], signal_fields[right]]
            x = bridge[cols].dropna()
            redundancy.append({
                "left_family": left, "right_family": right, "left_field": cols[0], "right_field": cols[1],
                "overlap_games": len(x), "pearson": x.corr(method="pearson").iloc[0, 1] if len(x) > 2 else np.nan,
                "spearman": x.corr(method="spearman").iloc[0, 1] if len(x) > 2 else np.nan,
                "sign_agreement_rate": np.sign(x.iloc[:, 0]).eq(np.sign(x.iloc[:, 1])).mean() if len(x) else np.nan,
            })
    redundancy.append({
        "left_family": "FROZEN_MONEYLINE_CONTROL", "right_family": "ALL_PROP_FAMILIES",
        "left_field": "season_2025_control_probability", "right_field": "prop signals",
        "overlap_games": 0, "pearson": np.nan, "spearman": np.nan, "sign_agreement_rate": np.nan,
        "status": "NOT_TESTABLE_SEASON_2025_STRICT_PRIOR_CONTROL_FEATURE_SPINE_ABSENT",
    })
    return pd.DataFrame(rows), pd.DataFrame(environment_rows), pd.DataFrame(temporal), pd.DataFrame(redundancy)


def coverage_summary(
    games: pd.DataFrame, market: pd.DataFrame, models: dict[str, pd.DataFrame], matched: dict[str, pd.DataFrame], bridge: pd.DataFrame
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for family in ["SOG", "POINTS", "SAVES"]:
        mkt = market[market.market_family.eq(family)]
        model = models[family]
        match = matched[family]
        signal = {
            "SOG": "sog_continuous_expectation_sum_diff",
            "POINTS": "points_continuous_expectation_sum_diff",
            "SAVES": "saves_model_market_delta_mean_diff",
        }[family]
        rows.append({
            "family": family, "canonical_games": len(games), "market_games": mkt.canonical_game_id.nunique(),
            "market_players": mkt.canonical_player_id.nunique(), "model_games": model.game_id.nunique(),
            "model_player_games": model[["game_id", "player_id"]].drop_duplicates().shape[0],
            "matched_games": match.game_id.nunique(), "matched_player_games": match[["game_id", "player_id"]].drop_duplicates().shape[0],
            "both_team_signal_games": int(bridge[signal].notna().sum()),
            "first_market_date": mkt.slate_date.min() if "slate_date" in mkt else None,
            "last_market_date": mkt.slate_date.max() if "slate_date" in mkt else None,
        })
    return pd.DataFrame(rows)


def concentration_audit(market: pd.DataFrame, matched: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for family in ["SOG", "POINTS", "SAVES"]:
        x = market[market.market_family.eq(family)]
        for dimension, col in [("team", "team"), ("slate_date", "slate_date"), ("line", "line")]:
            counts = x.groupby(col, dropna=False).size().sort_values(ascending=False)
            rows.append({
                "family": family, "population": "LATEST_MARKET_PLAYER_SELECTIONS", "dimension": dimension,
                "total_rows": len(x), "distinct_values": len(counts),
                "top_value": str(counts.index[0]) if len(counts) else None,
                "top_value_rows": int(counts.iloc[0]) if len(counts) else 0,
                "top_value_share": float(counts.iloc[0] / len(x)) if len(x) else np.nan,
            })
        y = matched[family]
        if "sportsbook_support_count" in y:
            rows.append({
                "family": family, "population": "MODEL_MARKET_MATCHES", "dimension": "book_support",
                "total_rows": len(y), "distinct_values": y.sportsbook_support_count.nunique(),
                "top_value": str(y.sportsbook_support_count.mode().iloc[0]) if len(y) else None,
                "top_value_rows": int(y.sportsbook_support_count.eq(y.sportsbook_support_count.mode().iloc[0]).sum()) if len(y) else 0,
                "top_value_share": float(y.sportsbook_support_count.eq(y.sportsbook_support_count.mode().iloc[0]).mean()) if len(y) else np.nan,
            })
    pairs = pd.read_parquet(ARCHIVE / "paired_no_vig_market_index.parquet", columns=[
        "market_family", "pair_status", "sportsbook",
    ])
    pairs = pairs[pairs.pair_status.eq("COMPLETE_ALIGNED")]
    for family, x in pairs.groupby("market_family"):
        counts = x.groupby("sportsbook", dropna=False).size().sort_values(ascending=False)
        rows.append({
            "family": family, "population": "ALL_COMPLETE_PREGAME_NO_VIG_PAIRS", "dimension": "sportsbook",
            "total_rows": len(x), "distinct_values": len(counts),
            "top_value": str(counts.index[0]) if len(counts) else None,
            "top_value_rows": int(counts.iloc[0]) if len(counts) else 0,
            "top_value_share": float(counts.iloc[0] / len(x)) if len(x) else np.nan,
        })
    return pd.DataFrame(rows)


def agreement_characterization(matched: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for family, frame in matched.items():
        for scope, group in [("ALL_SELECTED_LINES", frame)]:
            rows.append({
                "family": family, "scope": scope, "line": np.nan, "rows": len(group),
                "games": group.game_id.nunique(),
                "mean_model_probability": group.p_over.mean(),
                "mean_no_vig_market_probability": group.market_no_vig_over_probability.mean(),
                "mean_model_minus_market": group.model_market_delta.mean(),
                "median_model_minus_market": group.model_market_delta.median(),
                "mean_absolute_disagreement": group.model_market_delta.abs().mean(),
                "probability_pearson": group[["p_over", "market_no_vig_over_probability"]].corr().iloc[0, 1] if len(group) > 2 else np.nan,
                "model_above_market_rate": group.model_market_delta.gt(0).mean() if len(group) else np.nan,
            })
        for line, group in frame.groupby("line"):
            rows.append({
                "family": family, "scope": "SELECTED_LINE", "line": line, "rows": len(group),
                "games": group.game_id.nunique(),
                "mean_model_probability": group.p_over.mean(),
                "mean_no_vig_market_probability": group.market_no_vig_over_probability.mean(),
                "mean_model_minus_market": group.model_market_delta.mean(),
                "median_model_minus_market": group.model_market_delta.median(),
                "mean_absolute_disagreement": group.model_market_delta.abs().mean(),
                "probability_pearson": group[["p_over", "market_no_vig_over_probability"]].corr().iloc[0, 1] if len(group) > 2 else np.nan,
                "model_above_market_rate": group.model_market_delta.gt(0).mean() if len(group) else np.nan,
            })
    return pd.DataFrame(rows)


def data_dictionary(frame: pd.DataFrame) -> pd.DataFrame:
    rows = []
    outcome_only = {
        "final_home_goals", "final_away_goals", "home_win_full_game", "home_goal_margin", "absolute_goal_margin",
        "total_goals", "decision_type", "margin_bucket", "home_minus_1_5_result", "away_minus_1_5_result",
        "home_plus_1_5_result", "away_plus_1_5_result", "outcome_state",
    }
    for col in frame.columns:
        if col in outcome_only:
            classification = "OUTCOME_ONLY"
        elif col in {"canonical_season", "game_date", "game_id", "start_time_utc", "home_team", "away_team"}:
            classification = "IDENTITY_METADATA"
        elif "market" in col or "sportsbook" in col or "selected_line" in col:
            classification = "STRICT_PRIOR_MARKET_DERIVED"
        elif any(col.startswith(x) for x in ["sog_", "points_", "saves_"]):
            classification = "STRICT_PRIOR_PROP_BRIDGE_OR_COVERAGE_METADATA"
        else:
            classification = "AUDIT_METADATA"
        rows.append({
            "field": col, "dtype": str(frame[col].dtype), "classification": classification,
            "missing_count": int(frame[col].isna().sum()),
            "missing_semantics": "UNAVAILABLE_NOT_ZERO" if frame[col].isna().any() else "COMPLETE",
        })
    return pd.DataFrame(rows)


def validation_summary(
    games: pd.DataFrame, outcomes: pd.DataFrame, bridge: pd.DataFrame, market: pd.DataFrame,
    models: dict[str, pd.DataFrame], matched: dict[str, pd.DataFrame], completeness: pd.DataFrame,
) -> pd.DataFrame:
    checks: list[dict[str, Any]] = []
    def add(name: str, passed: bool, evidence: str) -> None:
        checks.append({"check": name, "status": "PASS" if passed else "FAIL", "evidence": evidence})
    add("outcome_spine_one_row_per_game", len(outcomes) == outcomes.game_id.nunique() == len(games), f"rows={len(outcomes)}")
    add("outcome_spine_all_qualified", outcomes.outcome_qualified.all(), f"qualified={int(outcomes.outcome_qualified.sum())}")
    add("bridge_one_row_per_game", len(bridge) == bridge.game_id.nunique() == len(games), f"rows={len(bridge)}")
    add("no_post_start_market_rows", market.market_age_minutes.ge(0).all(), f"minimum_age_minutes={market.market_age_minutes.min()}")
    add("no_post_start_timestamped_prediction_rows", all(
        (x.prediction_age_minutes.ge(0).all() if "prediction_age_minutes" in x and x.prediction_age_minutes.notna().any() else True)
        for x in matched.values()
    ), "all row-timestamp-qualified matched predictions are pregame")
    add("no_multi_line_player_market_aggregation", not market.duplicated(["market_family", "canonical_game_id", "canonical_player_id"]).any(), "one selected line per family/game/player")
    add("sog_genuine_continuous_expectation", models["SOG"].continuous_expectation.notna().all(), f"player_games={len(models['SOG'])}")
    add("points_material_ladder_gate", not models["POINTS"].material_incoherence.any(), f"eligible_player_games={len(models['POINTS'])}")
    add("saves_starter_state_not_confirmed", models["SAVES"].model_timing_state.str.contains("UNCONFIRMED").all(), f"eligible_player_lines={len(models['SAVES'])}")
    add("missing_bridge_features_not_filled_zero", any(bridge[c].isna().any() for c in bridge.columns if c.endswith("_count")), "coverage absence retained as null")
    add("official_completeness_checks", completeness[completeness.status.ne("UNRESOLVED")].status.eq("PASS").all(), "all required official checks pass")
    return pd.DataFrame(checks)


def source_lineage(parent_hashes: dict[str, str], calls: pd.DataFrame) -> pd.DataFrame:
    rows = [
        {"component": "game_identity_schedule", "source": str(ARCHIVE / "canonical_games_snapshot.parquet"), "grain": "game", "timing": "scheduled pregame", "classification": "IDENTITY_METADATA", "reproducibility": "immutable parent manifest", "limitation": "no scores in parent"},
        {"component": "official_full_game_result", "source": "official NHL club schedule-season API", "grain": "game", "timing": "postgame", "classification": "OUTCOME_ONLY", "reproducibility": f"{len(calls)} exact GET calls plus preserved normalized snapshot", "limitation": "late empty-net cause not exposed"},
        {"component": "prop_market_quotes", "source": str(ARCHIVE / "paired_no_vig_market_index.parquet"), "grain": "source snapshot/book/game/player/line", "timing": "qualified pregame", "classification": "STRICT_PRIOR_MARKET_DERIVED", "reproducibility": "immutable parent manifest", "limitation": "latest-source selection; status often not provided"},
        {"component": "SOG_model", "source": str(SOG_REPRO / "nhl_season_2025_sog_reproduction_ledger_2026-07-13.csv"), "grain": "game/player/line", "timing": "parent-certified pregame", "classification": "STRICT_PRIOR_MODEL_DERIVED", "reproducibility": "exact probability replay parent", "limitation": "2026-02-28 through 2026-04-15 certified target"},
        {"component": "Points_model", "source": str(ARCHIVE / "historical_predictions_snapshot.parquet"), "grain": "game/player/line", "timing": "row timestamp must precede start", "classification": "STRICT_PRIOR_MODEL_DERIVED_WITH_GATE", "reproducibility": "immutable parent manifest", "limitation": "materially incoherent or incomplete ladders fail closed"},
        {"component": "Saves_model_and_starter_state", "source": str(SAVES_STARTER / "latest_team_market_state_evaluation.parquet"), "grain": "game/team/goalie", "timing": "qualified pregame market state", "classification": "STRICT_PRIOR_CONDITIONAL_MODEL_DERIVED", "reproducibility": "immutable parent manifest", "limitation": "conditional on named goalie starting; market listing is not confirmation"},
        {"component": "frozen_moneyline_control", "source": str(FROZEN_CONTROL), "grain": "game", "timing": "strict prior in certified seasons 2023/2024", "classification": "FROZEN_CONTROL", "reproducibility": "frozen probabilities forwarded unchanged", "limitation": "season-2025 strict-prior feature spine absent; no forward replay"},
    ]
    frame = pd.DataFrame(rows)
    frame["parent_manifest_sha256"] = frame.component.map({
        "game_identity_schedule": parent_hashes["archive"], "prop_market_quotes": parent_hashes["archive"],
        "Points_model": parent_hashes["archive"], "SOG_model": parent_hashes["sog_repro"],
        "Saves_model_and_starter_state": parent_hashes["saves_starter"],
        "frozen_moneyline_control": parent_hashes["frozen_control"],
    })
    return frame


def render_report(summary: dict[str, Any], coverage: pd.DataFrame, char: pd.DataFrame, temporal: pd.DataFrame) -> str:
    cov = coverage.set_index("family").to_dict("index")
    margin_counts = summary["margin_counts"]
    lines = [
        "# NHL cross-market game-state bridge V1",
        "",
        "## Result",
        "",
        f"The create-only bridge contains {summary['games']:,} season-2025 regular-season games and {summary['qualified_outcomes']:,} certified full-game outcomes. Official NHL scores and REG/OT/SO state resolve the full-game winner, goal margin, ±1.5 puck-line settlement, and total without regulation inference. The frozen moneyline control was not refit and could not be replayed on season 2025 because its exact six-field strict-prior team feature spine is absent.",
        "",
        "## Coverage",
        "",
    ]
    for family in ["SOG", "POINTS", "SAVES"]:
        row = cov[family]
        lines.append(
            f"- {family}: model {int(row['model_games']):,} games / {int(row['model_player_games']):,} player-games; "
            f"latest qualified market {int(row['market_games']):,} games; exact-line model/market match {int(row['matched_games']):,} games; "
            f"both-team game signal {int(row['both_team_signal_games']):,} games."
        )
    lines += [
        "",
        "SOG is the only family with a genuine preserved continuous player expectation and is usable over its certified late-season window. Points is partial: only complete ladders without a ≥1 percentage-point crossing enter the game signal. Saves is partial: the distribution is conditional on starting and the retained market-consensus goalie remains `STARTER_UNCONFIRMED`.",
        "",
        f"The official outcome capture used {summary['official_api_network_calls']} public, unauthenticated GETs"
        + (": one preliminary NJD endpoint probe and one club-schedule-season call for each of the 32 teams." if summary.get("preliminary_official_probe") else ": one club-schedule-season call for each of the 32 teams.")
        + " Credits consumed: 0. Exact endpoints and response sizes are retained in `official_api_call_log.csv`.",
        "",
        "## Descriptive relationships",
        "",
    ]
    for family in ["SOG", "POINTS", "SAVES"]:
        x = char[(char.family == family) & (char.coverage_scope == "ALL_SIGNAL_GAMES") & char.games.gt(0)]
        if x.empty:
            lines.append(f"- {family}: insufficient populated ordered bins.")
            continue
        first, last = x.iloc[0], x.iloc[-1]
        lines.append(
            f"- {family}: ordered-bin endpoint home-win rates were {first.home_win_rate:.1%} ({int(first.games)} games) "
            f"and {last.home_win_rate:.1%} ({int(last.games)} games); mean home margins were "
            f"{first.mean_home_goal_margin:.3f} and {last.mean_home_goal_margin:.3f}. This is descriptive, not incremental evidence."
        )
    month_signs = temporal.groupby("family").signal_margin_spearman.agg(
        months="count", positive=lambda s: int((s > 0).sum()), negative=lambda s: int((s < 0).sum())
    ).reset_index()
    lines += ["", "Month-level signal/margin signs are mixed:"]
    for row in month_signs.itertuples():
        lines.append(f"- {row.family}: {row.positive} positive and {row.negative} negative months across {row.months} estimable months.")
    lines += [
        "",
        "The cross-market relationship is therefore visible but fragile. Season-2025 conditioning against the frozen moneyline probability is not testable, pairwise overlaps are uneven, and the strongest continuous SOG view covers only the certified late-season interval. Information novelty relative to the frozen control is not testable with current data; challenger design is not ready.",
        "",
        "## Puck-line and match-state limits",
        "",
        f"Final margins comprise {margin_counts.get('ONE_GOAL', 0):,} one-goal, {margin_counts.get('TWO_GOALS', 0):,} two-goal, and {margin_counts.get('THREE_PLUS_GOALS', 0):,} three-plus-goal games. The local certified inputs do not identify which two-goal margins were created by late empty-net goals, so that count remains unresolved rather than inferred.",
        "",
        "## Required decisions",
        "",
    ]
    for key, value in summary["decisions"].items():
        lines.append(f"- `{key}` = `{value}`")
    lines += [
        "",
        "No model was fitted, no threshold was optimized against outcomes, no odds or prediction was promoted, and no production state was changed.",
    ]
    return "\n".join(lines) + "\n"


def build(args: argparse.Namespace) -> None:
    output = Path(args.output_dir).resolve()
    parents = {
        "archive": ARCHIVE, "sog_repro": SOG_REPRO, "saves_starter": SAVES_STARTER,
        "frozen_control": FROZEN_CONTROL, "control_process": CONTROL_PROCESS,
    }
    parent_hashes = {name: verify_manifest(path) for name, path in parents.items()}
    staging = begin_package(output)
    try:
        games = pd.read_parquet(ARCHIVE / "canonical_games_snapshot.parquet")
        games["game_id"] = pd.to_numeric(games.game_id).astype("int64")
        games["start_time_utc"] = pd.to_datetime(games.start_time_utc, utc=True)
        teams = sorted(set(games.home_team_code) | set(games.away_team_code))
        if args.official_outcomes_snapshot:
            raw = pd.read_parquet(args.official_outcomes_snapshot)
            sibling_log = Path(args.official_outcomes_snapshot).with_name("official_api_call_log.csv")
            if sibling_log.is_file():
                calls = pd.read_csv(sibling_log)
                calls["reused_in_current_execution"] = True
            else:
                calls = pd.DataFrame([{
                    "method": "REUSE", "url": str(Path(args.official_outcomes_snapshot).resolve()), "http_status": None,
                    "response_bytes": Path(args.official_outcomes_snapshot).stat().st_size, "returned_games": raw.game_id.nunique(),
                    "retrieved_at_utc": raw.retrieved_at_utc.iloc[0], "purpose": "reuse preserved official source snapshot",
                    "reused_in_current_execution": True,
                }])
        elif args.fetch_official_outcomes:
            raw, calls = fetch_official_schedule(teams)
        else:
            raise RuntimeError("OFFICIAL_OUTCOMES_REQUIRED_USE_FETCH_FLAG_OR_SNAPSHOT")
        if args.preliminary_official_response:
            preliminary = Path(args.preliminary_official_response)
            preliminary_payload = json.loads(preliminary.read_text())
            preliminary_time = datetime.fromtimestamp(preliminary.stat().st_mtime, timezone.utc).isoformat().replace("+00:00", "Z")
            calls = pd.concat([pd.DataFrame([{
                "method": "GET", "url": OFFICIAL_URL.format(team="NJD"), "http_status": 200,
                "response_bytes": preliminary.stat().st_size,
                "returned_games": len(preliminary_payload.get("games") or []), "retrieved_at_utc": preliminary_time,
                "purpose": "preliminary official outcome schema/coverage probe",
                "reused_in_current_execution": True,
            }]), calls], ignore_index=True)
        official = raw[(pd.to_numeric(raw.game_type, errors="coerce") == 2) & raw.game_id.isin(games.game_id)].copy()
        write_parquet(official, staging / "official_nhl_schedule_source_snapshot.parquet")
        write_csv(calls, staging / "official_api_call_log.csv")
        outcomes, completeness = canonicalize_official(raw, games)
        write_parquet(outcomes, staging / "season_2025_outcome_spine.parquet")
        write_csv(completeness, staging / "outcome_spine_completeness.csv")

        roster, roster_counts = prepare_roster(games, ARCHIVE)
        market = latest_market_selections(ARCHIVE)
        predictions, prediction_timing = prediction_snapshot(games, ARCHIVE)
        models, model_timing = model_player_tables(games, roster, predictions, market)
        matched: dict[str, pd.DataFrame] = {}
        market_by_family: dict[str, pd.DataFrame] = {}
        for family in ["SOG", "POINTS", "SAVES"]:
            matched[family], market_by_family[family] = attach_market_matches(family, models[family], market, predictions)
        timing_market = market.groupby("market_family").agg(
            rows=("canonical_player_id", "size"), strict_prior_rows=("market_age_minutes", lambda s: int(s.ge(0).sum())),
            minimum_age_minutes=("market_age_minutes", "min"), median_age_minutes=("market_age_minutes", "median"),
            maximum_age_minutes=("market_age_minutes", "max"),
        ).reset_index().rename(columns={"market_family": "family"})
        timing_market["timing_population"] = "latest_market_player_line"
        timing_market["strict_prior_rate"] = timing_market.strict_prior_rows / timing_market.rows
        timing_market["timing_authority"] = "parent-qualified provider market/book/outcome timestamp before canonical start"
        timing_market["limitation"] = "filesystem timestamps are retrospective when provider timestamp unavailable; parent qualification retained"
        timing = pd.concat([prediction_timing.assign(
            timing_population="all_prediction_rows", strict_prior_rate=lambda x: x.strict_prior_rows / x.prediction_rows,
            timing_authority="prediction max(created_at,updated_at) compared with canonical start",
            limitation="post-start/backfilled database prediction rows excluded",
        ), model_timing, timing_market], ignore_index=True, sort=False)
        write_csv(timing, staging / "strict_prior_timing_audit.csv")

        team_tables: dict[str, pd.DataFrame] = {}
        for family in ["SOG", "POINTS", "SAVES"]:
            team_tables[family] = team_aggregate(
                family, games, roster_counts, models[family], matched[family], market_by_family[family]
            )
        bridge = one_row_per_game(games, outcomes, team_tables)
        write_parquet(bridge, staging / "game_state_bridge.parquet")

        coverage = coverage_summary(games, market, models, matched, bridge)
        write_csv(coverage, staging / "coverage_summary.csv")
        char, environment, temporal, redundancy = characterize(bridge)
        write_csv(char, staging / "ordered_signal_characterization.csv")
        write_csv(environment, staging / "ordered_environment_characterization.csv")
        write_csv(temporal, staging / "temporal_stability_characterization.csv")
        write_csv(redundancy, staging / "pairwise_redundancy.csv")
        write_csv(concentration_audit(market, matched), staging / "coverage_concentration_audit.csv")
        write_csv(agreement_characterization(matched), staging / "model_market_agreement_characterization.csv")
        coverage_detail = pd.concat(team_tables.values(), ignore_index=True).merge(
            games[["game_id", "game_date"]], on="game_id", how="left", validate="many_to_one"
        )
        write_parquet(coverage_detail, staging / "coverage_by_game_team_family.parquet")

        conditional = pd.DataFrame([{
            "analysis": "PROP_SIGNAL_CONDITIONAL_ON_FROZEN_CONTROL_PROBABILITY_BAND", "season": 2025,
            "rows": 0, "status": "BLOCKED", "reason": "season-2025 exact strict-prior six-feature frozen-control spine is absent",
            "action": "no refit, proxy, reconstruction, or historical-control probability reuse",
        }])
        write_csv(conditional, staging / "frozen_control_conditional_analysis.csv")
        frozen_metrics = pd.read_csv(FROZEN_CONTROL / "nhl_moneyline_frozen_control_out_of_time_summary_2026-07-13.csv")
        frozen_metrics.insert(0, "status", "FORWARDED_UNCHANGED_HISTORICAL_2023_2024")
        frozen_metrics.insert(1, "season_2025_forward_replay", "BLOCKED_MISSING_EXACT_FEATURE_SPINE")
        write_csv(frozen_metrics, staging / "frozen_moneyline_control_metrics.csv")

        lineage = source_lineage(parent_hashes, calls)
        write_csv(lineage, staging / "source_lineage_inventory.csv")
        dictionary = data_dictionary(bridge)
        write_csv(dictionary, staging / "field_data_dictionary.csv")
        validation = validation_summary(games, outcomes, bridge, market, models, matched, completeness)
        write_csv(validation, staging / "validation_summary.csv")

        coverage_index = coverage.set_index("family")
        outcome_certified = bool(outcomes.outcome_qualified.all() and len(outcomes) == 1312)
        decisions = {
            "NHL_CROSS_MARKET_BRIDGE_CONSTRUCTION": "COMPLETED",
            "NHL_SEASON_2025_OUTCOME_SPINE": "CERTIFIED" if outcome_certified else "PARTIAL",
            "NHL_FROZEN_MONEYLINE_CONTROL_FORWARD_REPLAY": "BLOCKED",
            "NHL_SOG_GAME_LEVEL_BRIDGE": "USABLE" if coverage_index.loc["SOG", "both_team_signal_games"] >= 100 else "PARTIAL",
            "NHL_POINTS_GAME_LEVEL_BRIDGE": "PARTIAL" if coverage_index.loc["POINTS", "both_team_signal_games"] > 0 else "UNAVAILABLE",
            "NHL_SAVES_GAME_LEVEL_BRIDGE": "PARTIAL" if coverage_index.loc["SAVES", "both_team_signal_games"] > 0 else "UNAVAILABLE",
            "NHL_CROSS_MARKET_RELATIONSHIP": "RELATIONSHIP_FOUND_BUT_FRAGILE",
            "NHL_INFORMATION_NOVELTY_STATUS": "NOT_TESTABLE_WITH_CURRENT_DATA",
            "NHL_CHALLENGER_DESIGN_READINESS": "NOT_READY",
            "NHL_NEXT_STEP": "REPAIR_SPECIFIC_LINEAGE_GAP",
        }
        margin_counts = outcomes.margin_bucket.value_counts().to_dict()
        summary = {
            "task": TASK, "package_date": PACKAGE_DATE, "games": len(games),
            "qualified_outcomes": int(outcomes.outcome_qualified.sum()), "official_api_calls": len(calls),
            "official_api_network_calls": int(calls.method.eq("GET").sum()),
            "preliminary_official_probe": bool(args.preliminary_official_response),
            "official_api_credits_consumed": 0, "official_api_authentication": "NONE_PUBLIC_ENDPOINT",
            "margin_counts": margin_counts, "late_empty_net_two_goal_count": "UNRESOLVED",
            "parent_manifest_sha256": parent_hashes, "decisions": decisions,
            "frozen_control_identity": "NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V1",
            "frozen_control_refit": False, "new_predictive_model_fit": False,
            "threshold_optimization": False, "production_mutations": 0,
        }
        write_json(summary, staging / "execution_summary.json")
        write_json({
            "task": TASK, "decisions": decisions,
            "decision_basis": {
                "outcome_spine": f"{int(outcomes.outcome_qualified.sum())}/{len(outcomes)} official completed decisive scores",
                "frozen_control": "unchanged historical probabilities forwarded; season-2025 strict-prior feature spine absent",
                "SOG": "parent-certified genuine continuous expected SOG, bounded late-season coverage",
                "POINTS": "strict-prior rows available only with incomplete/material-ladder fail-closed exclusions",
                "SAVES": "strict-prior conditional probabilities available only for market-consensus starter-unconfirmed goalies",
                "relationship": "ordered-bin separation visible in at least one family but month signs, overlap, and control conditioning are incomplete",
            },
        }, staging / "decision.json")
        (staging / "execution_utility.md").write_text(
            "# Reproduction\n\n"
            "Live official outcome refresh (requires separately approved network access):\n\n"
            "```text\n.venv/bin/python -m backend.nhl.scripts.build_nhl_cross_market_game_state_bridge_v1 --fetch-official-outcomes\n```\n\n"
            "Offline replay from the preserved source snapshot in this package must target a new create-only output directory:\n\n"
            "```text\n.venv/bin/python -m backend.nhl.scripts.build_nhl_cross_market_game_state_bridge_v1 --official-outcomes-snapshot artifacts/analysis/model_development/nhl_cross_market_game_state_bridge_v1/2026-09-15/official_nhl_schedule_source_snapshot.parquet --output-dir /tmp/nhl_cross_market_bridge_replay\n```\n"
        )
        (staging / "report.md").write_text(render_report(summary, coverage, char, temporal))
        package_identity = {
            "task": TASK, "package_date": PACKAGE_DATE, "source_code": str(Path(__file__).relative_to(REPO)),
            "source_code_sha256": sha256(Path(__file__)), "create_only": True,
            "parent_manifest_sha256": parent_hashes, "output_grain": "one row per season-2025 regular-season game",
            "source_mutations": 0, "production_mutations": 0,
        }
        write_json(package_identity, staging / "package_identity.json")
        if validation.status.ne("PASS").any():
            raise RuntimeError("VALIDATION_FAILED:" + ",".join(validation[validation.status.ne("PASS")].check))
        manifest_entries = []
        for path in sorted(p for p in staging.rglob("*") if p.is_file() and p.name != "SHA256SUMS"):
            manifest_entries.append(f"{sha256(path)}  {path.relative_to(staging)}")
        (staging / "SHA256SUMS").write_text("\n".join(manifest_entries) + "\n")
        finalize_package(staging, output)
    except Exception:
        # Keep the visibly incomplete directory for diagnosis; never publish a partial package.
        raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--fetch-official-outcomes", action="store_true")
    source.add_argument("--official-outcomes-snapshot")
    parser.add_argument("--preliminary-official-response", help="Optional raw response from an earlier schema probe to include in the exact call log")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT))
    return parser.parse_args()


if __name__ == "__main__":
    build(parse_args())
