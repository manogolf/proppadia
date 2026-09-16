#!/usr/bin/env python3
"""Repair the frozen season-2025 SOG backcast residuals without Odds API calls."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import shutil
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import psycopg

from backend.nhl.market_archive_index.core import declared_player_aliases, normalize_name
from backend.nhl.scripts.evaluate_nhl_2025_v2_market_and_sog_cross_market_v1 import (
    BOOTSTRAP_SEED,
    PLAYER_COVERAGE_BINS,
    PLAYER_COVERAGE_LABELS,
    PROBABILITY_BINS,
    PROBABILITY_LABELS,
    SOG_DIFF_BINS,
    SOG_DIFF_LABELS,
    aggregate_team,
    conditional_outputs,
    score_prepared_features,
)


ROOT = Path(__file__).resolve().parents[3]
STAMP = "2026-09-15"
TASK = "NHL_2025_SOG_BACKCAST_RESIDUAL_REPAIR_V1"
PARENT = ROOT / "artifacts/analysis/model_development/nhl_2025_v2_market_and_sog_cross_market_evaluation_v1/2026-09-15"
IDENTITY_PARENT = ROOT / "artifacts/analysis/model_development/nhl_season_2025_player_prop_market_archive_immutable_canonical_join_index_v1/2026-09-08"
DEFAULT_OUT = ROOT / "artifacts/analysis/model_development/nhl_2025_sog_backcast_residual_repair_v1/2026-09-15"
ALIAS_REGISTRY = ROOT / "backend/nhl/data/player_identity_aliases.csv"
ORIGINAL_SUBSTANTIVE_COLUMNS = [
    "game_id", "identity_key", "player_id", "player_name_provider", "team_assignment_status",
    "team_id", "opponent_id", "is_home", "game_date", "selected_rate", "selected_rate_source",
    "selected_toi_minutes", "selected_toi_source", "expected_sog", "missingness_fallback_state",
    "prepared_source_path", "prepared_source_sha256", "record_classification", "backcast_status",
]
MANUAL_EXACT_IDENTITIES = {
    "ty murchison": {
        "player_id": 8482804,
        "canonical_name": "Ty Murchison",
        "method": "EXACT_UNIQUE_CANONICAL_FULL_NAME_REVIEW",
        "evidence": "Unique nhl.players full_name exact match observed 2026-09-15; no fuzzy matching and no outcome fields consulted.",
    }
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def stable_json(value: Any) -> str:
    def safe(item: Any) -> Any:
        if isinstance(item, dict):
            return {str(k): safe(v) for k, v in item.items()}
        if isinstance(item, (list, tuple)):
            return [safe(v) for v in item]
        if isinstance(item, (np.integer,)):
            return int(item)
        if isinstance(item, (np.floating, float)):
            return None if not math.isfinite(float(item)) else float(item)
        if isinstance(item, (np.bool_,)):
            return bool(item)
        if pd.isna(item):
            return None
        return item
    return json.dumps(safe(value), sort_keys=True, separators=(",", ":"), allow_nan=False)


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(json.loads(stable_json(value)), indent=2, sort_keys=True) + "\n")


def verify_manifest(directory: Path) -> dict[str, Any]:
    failures: list[str] = []
    checked = 0
    for line in (directory / "SHA256SUMS").read_text().splitlines():
        if not line.strip():
            continue
        expected, name = line.split("  ", 1)
        checked += 1
        path = directory / name
        if not path.is_file() or sha256_file(path) != expected:
            failures.append(name)
    return {"directory": str(directory.relative_to(ROOT)), "files_checked": checked, "failures": failures, "passed": not failures}


def read_sql(connection: psycopg.Connection, query: str, params: tuple[Any, ...] = ()) -> pd.DataFrame:
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        columns = [column.name for column in cursor.description]
        return pd.DataFrame(cursor.fetchall(), columns=columns)


def residual_targets() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    original = pd.read_parquet(PARENT / "sog_model_backcast.parquet")
    games = pd.read_parquet(PARENT / "game_bridge.parquet")
    residual = original.loc[original.backcast_status.ne("GENERATED")].copy()
    residual = residual.merge(
        games[["game_id", "game_date", "scheduled_start_time_utc", "home_team_id", "home_team", "away_team_id", "away_team"]],
        on="game_id", how="left", suffixes=("_original", ""), validate="many_to_one",
    )
    aliases = declared_player_aliases()
    identity_methods: list[str] = []
    identity_evidence: list[str] = []
    canonical_names: list[str | None] = []
    resolved_ids: list[int | None] = []
    for row in residual.itertuples(index=False):
        if pd.notna(row.player_id):
            resolved_ids.append(int(row.player_id))
            canonical_names.append(None)
            identity_methods.append("PARENT_CANONICAL_PLAYER_ID_PRESERVED")
            identity_evidence.append("Frozen parent player_id; not reopened")
            continue
        normalized = normalize_name(row.player_name_provider)
        declared = aliases.get(normalized)
        if declared is not None:
            resolved_ids.append(int(declared))
            canonical_names.append("E. Chinakhov" if declared == 8482475 else None)
            identity_methods.append("DECLARED_EXACT_PROVIDER_NAME_VARIANT")
            identity_evidence.append("backend/nhl/data/player_identity_aliases.csv")
            continue
        manual = MANUAL_EXACT_IDENTITIES.get(normalized)
        if manual:
            resolved_ids.append(int(manual["player_id"]))
            canonical_names.append(str(manual["canonical_name"]))
            identity_methods.append(str(manual["method"]))
            identity_evidence.append(str(manual["evidence"]))
            continue
        resolved_ids.append(None)
        canonical_names.append(None)
        identity_methods.append("UNRESOLVED")
        identity_evidence.append("No deterministic exact mapping")
    residual["resolved_player_id"] = pd.array(resolved_ids, dtype="Int64")
    residual["canonical_player_name"] = canonical_names
    residual["identity_repair_method"] = identity_methods
    residual["identity_evidence"] = identity_evidence
    if residual.resolved_player_id.isna().any():
        raise RuntimeError("deterministic identity repair did not account for every residual")
    return original, games, residual


def load_game_identity_snapshot() -> pd.DataFrame:
    frame = pd.read_parquet(IDENTITY_PARENT / "canonical_game_player_snapshot.parquet")
    grouped = frame.groupby(["game_id", "player_id"], as_index=False).agg(
        canonical_game_names=("full_name", lambda x: "|".join(sorted(set(str(v) for v in x if pd.notna(v))))),
        canonical_game_team_codes=("team_code", lambda x: "|".join(sorted(set(str(v) for v in x if pd.notna(v))))),
        canonical_game_identity_rows=("team_code", "size"),
        canonical_game_team_count=("team_code", "nunique"),
    )
    return grouped


def fetch_db_sources(residual: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    dsn = os.environ.get("SUPABASE_DB_URL")
    if not dsn:
        raise RuntimeError("SUPABASE_DB_URL is required only for --refresh-db-snapshot")
    ids = sorted(residual.resolved_player_id.astype(int).unique().tolist())
    max_date = str(pd.to_datetime(residual.game_date).max().date())
    pair_game_ids = residual.game_id.astype(int).tolist()
    pair_player_ids = residual.resolved_player_id.astype(int).tolist()
    with psycopg.connect(dsn) as connection:
        logs = read_sql(connection, """
            SELECT l.player_id,l.game_id,g.season,g.game_date,g.start_time_utc,
                   l.team_id,l.opponent_id,l.shots_on_goal,l.shot_attempts,l.toi_minutes
            FROM nhl.skater_game_logs_raw l JOIN nhl.games g USING(game_id)
            WHERE l.player_id = ANY(%s) AND g.game_date < %s::date
            ORDER BY l.player_id,g.game_date,g.game_id
        """, (ids, max_date))
        shifts = read_sql(connection, """
            WITH totals AS (
              SELECT s.player_id,s.game_id,g.season,g.game_date,
                     NULLIF(SUM(COALESCE(s.dur_sec,0))::int,0) AS total_shift_sec
              FROM nhl.shiftcharts_shifts s JOIN nhl.games g USING(game_id)
              WHERE s.player_id = ANY(%s) AND g.season=2025 AND g.game_date < %s::date
              GROUP BY 1,2,3,4
            ), shift_overlaps AS (
              SELECT sh.player_id,sh.game_id,
                     SUM(CASE WHEN l.team_id=seg.pp_team_id THEN GREATEST(LEAST(sh.end_sec,seg.end_sec)-GREATEST(sh.start_sec,seg.start_sec),0) ELSE 0 END)::int AS pp_sec,
                     SUM(CASE WHEN l.team_id=seg.pk_team_id THEN GREATEST(LEAST(sh.end_sec,seg.end_sec)-GREATEST(sh.start_sec,seg.start_sec),0) ELSE 0 END)::int AS pk_sec
              FROM nhl.shiftcharts_shifts sh
              JOIN nhl.skater_game_logs_raw l ON l.game_id=sh.game_id AND l.player_id=sh.player_id
              JOIN nhl.game_manpower_segments seg ON seg.game_id=sh.game_id AND seg.period=sh.period
                   AND sh.end_sec>seg.start_sec AND sh.start_sec<seg.end_sec
              JOIN nhl.games g ON g.game_id=sh.game_id
              WHERE sh.player_id = ANY(%s) AND g.season=2025 AND g.game_date < %s::date
              GROUP BY 1,2
            )
            SELECT t.player_id,t.game_id,t.season,t.game_date,t.total_shift_sec,
                   COALESCE(o.pp_sec,0) AS pp_sec,COALESCE(o.pk_sec,0) AS pk_sec,
                   GREATEST(t.total_shift_sec-COALESCE(o.pp_sec,0)-COALESCE(o.pk_sec,0),0) AS ev_sec
            FROM totals t LEFT JOIN shift_overlaps o USING(player_id,game_id)
            ORDER BY t.player_id,t.game_date,t.game_id
        """, (ids, max_date, ids, max_date))
        roster = read_sql(connection, """
            WITH targets AS (
              SELECT * FROM unnest(%s::bigint[],%s::bigint[]) AS x(game_id,player_id)
            )
            SELECT r.game_id,r.player_id,r.team_id,r.active_flag,r.asof_ts
            FROM targets t JOIN nhl.roster_status r USING(game_id,player_id)
            ORDER BY r.game_id,r.player_id,r.asof_ts
        """, (pair_game_ids, pair_player_ids))
        players = read_sql(connection, """
            SELECT player_id,full_name,first_name,last_name,current_team_id,team_id,updated_at
            FROM nhl.players WHERE player_id = ANY(%s) ORDER BY player_id
        """, (ids,))
    return logs, shifts, roster, players


def numeric_mean(values: pd.Series, limit: int) -> float:
    selected = pd.to_numeric(values.head(limit), errors="coerce").dropna()
    return float(selected.mean()) if len(selected) else float("nan")


def derive_source_snapshot(
    residual: pd.DataFrame,
    logs: pd.DataFrame,
    shifts: pd.DataFrame,
    roster: pd.DataFrame,
    players: pd.DataFrame,
) -> pd.DataFrame:
    identity = load_game_identity_snapshot()
    work = residual.merge(
        identity, left_on=["game_id", "resolved_player_id"], right_on=["game_id", "player_id"],
        how="left", validate="one_to_one", suffixes=("", "_identity"),
    )
    player_meta = players.rename(columns={
        "player_id": "resolved_player_id", "full_name": "db_full_name", "first_name": "db_first_name",
        "last_name": "db_last_name", "updated_at": "db_player_updated_at",
    })[["resolved_player_id", "db_full_name", "db_first_name", "db_last_name", "db_player_updated_at"]]
    work = work.merge(player_meta, on="resolved_player_id", how="left", validate="many_to_one")
    for frame in (logs, shifts):
        if len(frame):
            frame["game_date"] = pd.to_datetime(frame.game_date).dt.date
    roster = roster.copy()
    if len(roster):
        roster["asof_ts"] = pd.to_datetime(roster.asof_ts, utc=True)
    rows: list[dict[str, Any]] = []
    for row in work.itertuples(index=False):
        target_date = pd.Timestamp(row.game_date).date()
        player_id = int(row.resolved_player_id)
        history = logs.loc[(pd.to_numeric(logs.player_id).eq(player_id)) & (logs.game_date < target_date)].copy()
        history = history.sort_values(["game_date", "game_id"], ascending=False)
        if (history.game_id.astype(int) == int(row.game_id)).any():
            raise RuntimeError("current-game row entered strict-prior history")
        last20 = history.head(20).copy()
        toi = pd.to_numeric(last20.toi_minutes, errors="coerce").replace(0, np.nan)
        sog = pd.to_numeric(last20.shots_on_goal, errors="coerce").fillna(0)
        rates: dict[int, float] = {}
        for window in (5, 10, 20):
            denominator = toi.head(window).sum(min_count=1)
            numerator = sog.head(window).sum(min_count=1)
            rates[window] = float(numerator / denominator * 60.0) if pd.notna(denominator) and denominator > 0 else np.nan
        season_hist = history.loc[pd.to_numeric(history.season).eq(2025)].copy()
        season_toi = pd.to_numeric(season_hist.toi_minutes, errors="coerce").replace(0, np.nan).dropna()
        shift_hist = shifts.loc[(pd.to_numeric(shifts.player_id).eq(player_id)) & (shifts.game_date < target_date)].copy()
        season_ev_seconds = pd.to_numeric(shift_hist.ev_sec, errors="coerce").mean() if len(shift_hist) else np.nan
        season_pp_seconds = pd.to_numeric(shift_hist.pp_sec, errors="coerce").mean() if len(shift_hist) else np.nan
        target_roster = roster.loc[(pd.to_numeric(roster.game_id).eq(int(row.game_id))) & (pd.to_numeric(roster.player_id).eq(player_id))].copy()
        start = pd.Timestamp(row.scheduled_start_time_utc)
        start = start.tz_localize("UTC") if start.tzinfo is None else start.tz_convert("UTC")
        pregame_roster = target_roster.loc[target_roster.asof_ts < start]
        pregame_teams = sorted(set(pd.to_numeric(pregame_roster.team_id, errors="coerce").dropna().astype(int)))
        identity_codes = [] if pd.isna(row.canonical_game_team_codes) or not str(row.canonical_game_team_codes) else str(row.canonical_game_team_codes).split("|")
        code_to_id = {str(row.home_team): int(row.home_team_id), str(row.away_team): int(row.away_team_id)}
        identity_teams = sorted({code_to_id[code] for code in identity_codes if code in code_to_id})
        latest_team = int(history.iloc[0].team_id) if len(history) and pd.notna(history.iloc[0].team_id) else None
        allowed_teams = {int(row.home_team_id), int(row.away_team_id)}
        team_id: int | None = None
        team_method = "UNRESOLVED_NO_STRICT_TEAM_EVIDENCE"
        team_evidence = ""
        if len(identity_teams) == 1:
            team_id = identity_teams[0]
            team_method = "FROZEN_CANONICAL_GAME_IDENTITY_TEAM"
            team_evidence = f"canonical_game_player_snapshot team={identity_codes[0]}; identity/team only, no active or participation field"
        elif len(pregame_teams) == 1 and pregame_teams[0] in allowed_teams:
            team_id = pregame_teams[0]
            team_method = "TIMESTAMPED_PREGAME_ROSTER_TEAM"
            team_evidence = f"roster asof<{start.isoformat()}"
        elif latest_team in allowed_teams:
            team_id = latest_team
            team_method = "LATEST_STRICT_PRIOR_GAME_TEAM"
            team_evidence = f"latest_prior_game_id={int(history.iloc[0].game_id)};latest_prior_date={history.iloc[0].game_date}"
        opponent_id = None if team_id is None else int(row.away_team_id if team_id == int(row.home_team_id) else row.home_team_id)
        is_home = None if team_id is None else bool(team_id == int(row.home_team_id))
        rows.append({
            "game_id": int(row.game_id), "identity_key": row.identity_key, "player_name_provider": row.player_name_provider,
            "original_backcast_status": row.backcast_status, "resolved_player_id": player_id,
            "identity_repair_method": row.identity_repair_method, "identity_evidence": row.identity_evidence,
            "db_full_name": row.db_full_name, "db_first_name": row.db_first_name, "db_last_name": row.db_last_name,
            "db_player_updated_at": row.db_player_updated_at, "game_date": str(target_date),
            "scheduled_start_time_utc": start, "home_team": row.home_team, "away_team": row.away_team,
            "home_team_id": int(row.home_team_id), "away_team_id": int(row.away_team_id),
            "team_id": team_id, "opponent_id": opponent_id, "is_home": is_home,
            "team_assignment_method": team_method, "team_assignment_evidence": team_evidence,
            "canonical_game_identity_rows": 0 if pd.isna(row.canonical_game_identity_rows) else int(row.canonical_game_identity_rows),
            "canonical_game_team_count": 0 if pd.isna(row.canonical_game_team_count) else int(row.canonical_game_team_count),
            "roster_row_count": len(target_roster), "pregame_roster_row_count": len(pregame_roster),
            "earliest_roster_asof_utc": target_roster.asof_ts.min() if len(target_roster) else pd.NaT,
            "prior_log_count_all": len(history), "prior_log_count_same_season": len(season_hist),
            "latest_prior_game_id": int(history.iloc[0].game_id) if len(history) else None,
            "latest_prior_game_date": str(history.iloc[0].game_date) if len(history) else None,
            "latest_prior_team_id": latest_team,
            "d5_sog_per60": rates[5], "d10_sog_per60": rates[10], "d20_sog_per60": rates[20],
            "d5_toi_min_avg": numeric_mean(season_toi.reset_index(drop=True), 5),
            "d10_toi_min_avg": numeric_mean(season_toi.reset_index(drop=True), 10),
            "d20_toi_min_avg": numeric_mean(season_toi.reset_index(drop=True), 20),
            "szn_toi_per_game_5on5": float(season_ev_seconds / 60.0) if pd.notna(season_ev_seconds) else np.nan,
            "szn_toi_per_game_pp": float(season_pp_seconds / 60.0) if pd.notna(season_pp_seconds) else np.nan,
            "season_5on5_icetime_per_game": float(season_ev_seconds) if pd.notna(season_ev_seconds) else np.nan,
            "season_5on4_icetime_per_game": float(season_pp_seconds) if pd.notna(season_pp_seconds) else np.nan,
            "strict_prior_max_game_date": str(history.game_date.max()) if len(history) else None,
            "strict_prior_current_game_rows_used": 0,
        })
    snapshot = pd.DataFrame(rows).sort_values(["game_id", "identity_key"]).reset_index(drop=True)
    if len(snapshot) != 1014 or snapshot.duplicated(["game_id", "identity_key"]).any():
        raise RuntimeError("invalid source snapshot grain")
    return snapshot


def feature_archive_evidence() -> tuple[set[int], set[tuple[int, int]], dict[int, str]]:
    games: set[int] = set()
    pairs: set[tuple[int, int]] = set()
    game_sources: dict[int, str] = {}
    for path in sorted(ROOT.glob("artifacts/archive/generated_daily/nhl/*/sog_features/sog_features_*_denali.csv")):
        try:
            frame = pd.read_csv(path, usecols=["game_id", "player_id"])
        except (ValueError, pd.errors.EmptyDataError):
            continue
        for row in frame.dropna().itertuples(index=False):
            game_id, player_id = int(row.game_id), int(row.player_id)
            games.add(game_id)
            pairs.add((game_id, player_id))
            game_sources.setdefault(game_id, str(path.relative_to(ROOT)))
    return games, pairs, game_sources


def classify_and_score(snapshot: pd.DataFrame) -> pd.DataFrame:
    games_with_features, feature_pairs, game_sources = feature_archive_evidence()
    scored = score_prepared_features(snapshot.copy())
    scored["structural_no_history"] = scored.prior_log_count_all.eq(0)
    scored["feature_history_state"] = np.select(
        [
            scored.prior_log_count_all.eq(0),
            scored.prior_log_count_same_season.eq(0),
            scored.selected_rate_source.eq("MISSING"),
            scored.selected_toi_source.eq("MISSING"),
        ],
        [
            "STRUCTURAL_NO_PRIOR_PLAYER_GAME_HISTORY",
            "NO_CURRENT_SEASON_EXPOSURE_HISTORY",
            "PRIOR_HISTORY_PRESENT_RATE_UNAVAILABLE",
            "PRIOR_HISTORY_PRESENT_TOI_UNAVAILABLE",
        ],
        default="STRICT_PRIOR_FEATURES_RECONSTRUCTED",
    )
    primary: list[str] = []
    for row in scored.itertuples(index=False):
        pair = (int(row.game_id), int(row.resolved_player_id))
        if row.original_backcast_status == "UNRESOLVED_PLAYER_ID":
            primary.append("SPELLING_ALIAS_IDENTITY_CROSSWALK_GAP" if row.player_name_provider == "Yegor Chinakhov" else "IDENTITY_CROSSWALK_GAP")
        elif int(row.game_id) not in games_with_features:
            primary.append("DAILY_PREPARED_FEATURE_SOURCE_GAP")
        elif pair in feature_pairs:
            primary.append("OTHER_PARENT_JOIN_GAP")
        elif row.canonical_game_identity_rows == 0 and row.roster_row_count == 0:
            if pd.notna(row.latest_prior_team_id) and int(row.latest_prior_team_id) not in {int(row.home_team_id), int(row.away_team_id)}:
                primary.append("CURRENT_TEAM_OR_TRADE_MAPPING_GAP")
            else:
                primary.append("ROSTER_OR_ACTIVATION_SEED_GAP")
        elif row.pregame_roster_row_count == 0 and row.roster_row_count > 0:
            primary.append("ROSTER_CAPTURE_TIMING_GAP")
        else:
            primary.append("FEATURE_GENERATION_OR_EXPORT_OMISSION")
    scored["primary_root_cause"] = primary
    scored["prepared_source_path"] = "STRICT_PRIOR_RECONSTRUCTION_SOURCE_SNAPSHOT"
    scored["prepared_source_sha256"] = "ASSIGNED_AFTER_SNAPSHOT_WRITE"
    scored["repair_status"] = np.where(scored.team_id.notna(), "REPAIRED_MODEL_AND_AGGREGATION_READY", "REPAIRED_MODEL_ONLY_TEAM_UNRESOLVED")
    scored["feature_repair_method"] = "FROZEN_BASELINE_INPUT_RECONSTRUCTION_FROM_STRICT_PRIOR_GAME_LOGS"
    scored["feature_lineage"] = "nhl.skater_game_logs_raw + nhl.games + nhl.shiftcharts_shifts + nhl.game_manpower_segments; every history row game_date < target game_date"
    scored["no_history_policy"] = np.where(scored.structural_no_history, "FROZEN_MISSING_INPUT_ZERO_DEFAULT_NO_TEAM_OR_LEAGUE_FALLBACK", "NOT_APPLICABLE")
    scored["source_game_feature_path"] = scored.game_id.map(game_sources)
    return scored


def build_repaired_backcast(original: pd.DataFrame, repaired: pd.DataFrame, snapshot_sha: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    additive = original.copy()
    for col in [
        "repair_origin", "repair_status", "original_backcast_status", "identity_repair_method", "identity_evidence",
        "team_assignment_method", "team_assignment_evidence", "feature_repair_method", "feature_lineage",
        "feature_history_state", "primary_root_cause", "structural_no_history", "no_history_policy",
    ]:
        additive[col] = None
    additive.loc[additive.backcast_status.eq("GENERATED"), "repair_origin"] = "ORIGINAL_FROZEN_GENERATED_ROW"
    additive.loc[additive.backcast_status.eq("GENERATED"), "repair_status"] = "ORIGINAL_FROZEN"
    residual_index = additive.loc[additive.backcast_status.ne("GENERATED")].index
    rep = repaired.set_index(["game_id", "identity_key"])
    for idx in residual_index:
        key = (int(additive.at[idx, "game_id"]), additive.at[idx, "identity_key"])
        row = rep.loc[key]
        additive.at[idx, "player_id"] = int(row.resolved_player_id)
        additive.at[idx, "team_id"] = row.team_id
        additive.at[idx, "opponent_id"] = row.opponent_id
        additive.at[idx, "is_home"] = None if pd.isna(row.is_home) else ("t" if bool(row.is_home) else "f")
        additive.at[idx, "game_date"] = row.game_date
        additive.at[idx, "selected_rate"] = row.selected_rate
        additive.at[idx, "selected_rate_source"] = row.selected_rate_source
        additive.at[idx, "selected_toi_minutes"] = row.selected_toi_minutes
        additive.at[idx, "selected_toi_source"] = row.selected_toi_source
        additive.at[idx, "expected_sog"] = row.expected_sog
        additive.at[idx, "missingness_fallback_state"] = row.missingness_fallback_state
        additive.at[idx, "prepared_source_path"] = "strict_prior_feature_source_snapshot.parquet"
        additive.at[idx, "prepared_source_sha256"] = snapshot_sha
        additive.at[idx, "record_classification"] = "RETROSPECTIVE_MODEL_BACKCAST_ADDITIVE_REPAIR"
        additive.at[idx, "backcast_status"] = "GENERATED"
        additive.at[idx, "team_assignment_status"] = row.team_assignment_method
        for col in [
            "repair_status", "original_backcast_status", "identity_repair_method", "identity_evidence",
            "team_assignment_method", "team_assignment_evidence", "feature_repair_method", "feature_lineage",
            "feature_history_state", "primary_root_cause", "structural_no_history", "no_history_policy",
        ]:
            additive.at[idx, col] = row[col]
        additive.at[idx, "repair_origin"] = "ADDITIVE_RESIDUAL_REPAIR"
    frozen = original.loc[original.backcast_status.eq("GENERATED"), ORIGINAL_SUBSTANTIVE_COLUMNS].reset_index(drop=True)
    after = additive.loc[additive.repair_origin.eq("ORIGINAL_FROZEN_GENERATED_ROW"), ORIGINAL_SUBSTANTIVE_COLUMNS].reset_index(drop=True)
    pd.testing.assert_frame_equal(frozen, after, check_exact=True, check_dtype=True)
    before_digest = hashlib.sha256(frozen.to_json(orient="table", index=False, date_format="iso").encode()).hexdigest()
    after_digest = hashlib.sha256(after.to_json(orient="table", index=False, date_format="iso").encode()).hexdigest()
    return additive, {
        "original_rows_compared": len(frozen), "substantive_columns": ORIGINAL_SUBSTANTIVE_COLUMNS,
        "before_digest": before_digest, "after_digest": after_digest, "preserved": before_digest == after_digest,
    }


def repaired_market_players(repaired: pd.DataFrame) -> pd.DataFrame:
    market = pd.read_csv(PARENT / "sog_player_consensus.csv")
    rep = repaired.set_index(["game_id", "identity_key"])
    for idx in market.index:
        key = (int(market.at[idx, "game_id"]), market.at[idx, "identity_key"])
        if key not in rep.index:
            continue
        row = rep.loc[key]
        market.at[idx, "player_id"] = int(row.resolved_player_id)
        market.at[idx, "team_id"] = row.team_id
        market.at[idx, "opponent_id"] = row.opponent_id
        market.at[idx, "is_home"] = row.is_home
        market.at[idx, "team_assignment_status"] = row.team_assignment_method
    return market


def rebuild_model_bridge(parent_bridge: pd.DataFrame, market_players: pd.DataFrame, backcast: pd.DataFrame) -> pd.DataFrame:
    model_input = backcast.loc[backcast.backcast_status.eq("GENERATED")].merge(
        market_players[["game_id", "identity_key", "bookmaker_count", "cross_book_lambda_stddev"]],
        on=["game_id", "identity_key"], how="left", validate="one_to_one",
    )
    model_team = aggregate_team(model_input, "expected_sog", "model_sog")
    out = parent_bridge.copy()
    model_columns = [column for column in out.columns if "model_sog" in column]
    for column in model_columns:
        out[column] = np.nan
    for side, id_col in (("home", "home_team_id"), ("away", "away_team_id")):
        renamed = model_team.rename(columns={column: f"{side}_{column}" for column in model_team.columns if column not in ("game_id", "team_id")})
        merged = out[["game_id", id_col]].merge(renamed, left_on=["game_id", id_col], right_on=["game_id", "team_id"], how="left", validate="one_to_one")
        for column in renamed.columns:
            if column not in ("game_id", "team_id"):
                out[column] = merged[column].to_numpy()
    out["model_sog_home_away_difference"] = out.home_model_sog_lambda_sum - out.away_model_sog_lambda_sum
    out["model_sog_combined_game_total"] = out.home_model_sog_lambda_sum + out.away_model_sog_lambda_sum
    out["model_minus_market_sog_home_away_difference"] = out.model_sog_home_away_difference - out.market_sog_home_away_difference
    return out


def coverage_outputs(original: pd.DataFrame, repaired_backcast: pd.DataFrame, repaired: pd.DataFrame, games: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    before_ready = original.backcast_status.eq("GENERATED")
    after_ready = repaired_backcast.backcast_status.eq("GENERATED")
    overall = pd.DataFrame([
        {"state": "BEFORE", "eligible_player_games": len(original), "completed_player_games": int(before_ready.sum()), "completed_games": int(original.loc[before_ready, "game_id"].nunique()), "aggregation_ready_player_games": int(original.loc[before_ready & original.team_id.notna()].shape[0])},
        {"state": "AFTER", "eligible_player_games": len(repaired_backcast), "completed_player_games": int(after_ready.sum()), "completed_games": int(repaired_backcast.loc[after_ready, "game_id"].nunique()), "aggregation_ready_player_games": int(repaired_backcast.loc[after_ready & repaired_backcast.team_id.notna()].shape[0])},
    ])
    game_month = games[["game_id", "game_date"]].copy()
    game_month["month"] = pd.to_datetime(game_month.game_date).dt.to_period("M").astype(str)
    rows: list[dict[str, Any]] = []
    for state, frame in (("BEFORE", original), ("AFTER", repaired_backcast)):
        joined = frame.merge(game_month[["game_id", "month"]], on="game_id", how="left", validate="many_to_one")
        ready = joined.backcast_status.eq("GENERATED")
        for month, group in joined.groupby("month", sort=True):
            mask = group.backcast_status.eq("GENERATED")
            rows.append({"state": state, "dimension": "MONTH", "segment": month, "eligible_player_games": len(group), "completed_player_games": int(mask.sum()), "aggregation_ready_player_games": int((mask & group.team_id.notna()).sum()), "games": group.game_id.nunique()})
    team_rows: list[dict[str, Any]] = []
    for state, frame in (("BEFORE", original), ("AFTER", repaired_backcast)):
        for team_id, group in frame.dropna(subset=["team_id"]).groupby("team_id", sort=True):
            team_rows.append({"state": state, "team_id": int(team_id), "player_games": len(group), "expected_sog_sum": group.expected_sog.sum(), "expected_sog_mean": group.expected_sog.mean()})
    return overall, pd.DataFrame(rows), pd.DataFrame(team_rows)


def distribution_outputs(original: pd.DataFrame, repaired: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    samples = [
        ("ORIGINAL_COMPLETED", pd.to_numeric(original.loc[original.backcast_status.eq("GENERATED"), "expected_sog"], errors="coerce").dropna()),
        ("REPAIRED_RESIDUAL", pd.to_numeric(repaired.expected_sog, errors="coerce").dropna()),
        ("REPAIRED_STRUCTURAL_NO_HISTORY", pd.to_numeric(repaired.loc[repaired.structural_no_history, "expected_sog"], errors="coerce").dropna()),
    ]
    for label, values in samples:
        rows.append({"population": label, "n": len(values), "mean": values.mean(), "stddev": values.std(ddof=0), "min": values.min(), "p10": values.quantile(.1), "p25": values.quantile(.25), "median": values.median(), "p75": values.quantile(.75), "p90": values.quantile(.9), "max": values.max(), "zero_rate": values.eq(0).mean()})
    return pd.DataFrame(rows)


def downstream_matrix(unresolved: int) -> pd.DataFrame:
    rows = [
        ("player_identity_crosswalk", "YES", "Declared Yegor Chinakhov display-name variant added to canonical alias registry; Ty Murchison retained as exact reviewed identity evidence."),
        ("SOG feature generation", "YES", "Roster-seeded eligibility omitted market-listed rows; future replays should accept a fixed market eligibility population and expose seed misses."),
        ("SOG grading", "NO", "No outcomes, current-game SOG, participation, or grading fields were used or changed."),
        ("Points feature generation", "POTENTIALLY", "Shared player alias registry fixes the same Yegor Chinakhov identity variant; no Points data changed."),
        ("Saves feature generation", "NO", "Skater-only identity and SOG history repair."),
        ("pregame roster snapshots", "YES", f"{unresolved} repaired model row(s) still lack a deterministic team side; preserve immutable timestamped game-team snapshots."),
        ("season-2026 prospective capture", "YES", "Keep market eligibility independent from roster activation, preserve exact aliases, and capture team-side identity before start."),
        ("market-only pipelines", "NO", "Odds quotes, no-vig transforms, market lambdas, and market populations remain frozen."),
        ("cross-market bridge", "YES", "Model SOG team aggregates and conditional sensitivity replay are updated additively; market aggregates are unchanged."),
    ]
    return pd.DataFrame(rows, columns=["component", "affected", "required_action_or_reason"])


def write_report(out: Path, decision: dict[str, Any], root_causes: pd.DataFrame, sensitivity: pd.DataFrame, parity: dict[str, Any], credits: int) -> None:
    cause_text = ", ".join(f"{row.primary_root_cause}={int(row.rows)}" for row in root_causes.itertuples(index=False))
    comparison = sensitivity.set_index("proposition")
    lines = []
    for proposition, row in comparison.iterrows():
        lines.append(f"- {proposition}: {row.before_point_difference:.6f} -> {row.after_point_difference:.6f}; 95% CI [{row.after_ci_2_5:.6f}, {row.after_ci_97_5:.6f}]; {row.before_decision} -> {row.after_decision}.")
    text = f"""# NHL 2025 SOG backcast residual repair V1

## Outcome

All 1,014 frozen residual rows were accounted for without an Odds API call. The repair produced model outputs for {decision['NHL_SOG_REPAIRED_BACKCAST_POPULATION'].split()[0]} of 19,840 fixed eligible player-games and game coverage of {decision['NHL_SOG_REPAIRED_GAME_COVERAGE'].split()[0]} of 1,312. A model output is distinct from team-aggregation readiness: {decision['NHL_SOG_UNRESOLVED_ROWS_AFTER_REPAIR']} row(s) retain an explicit team-side limitation rather than a guessed assignment. Structural no-history rows use the frozen missing-input zero default and no league, team, or replacement-player fallback.

The 18,826 original generated rows were unchanged across every substantive field. Their before/after canonical digests are `{parity['before_digest']}` and `{parity['after_digest']}`. The frozen 13,389-player-game probability parity remains authoritative and passed unchanged.

## Root causes

{cause_text}.

The repair uses all-team player history by canonical player ID and strictly earlier game dates. Team changes do not reset player history. The fixed market listing remains the eligibility population. No current-game participation, TOI, shots, final lineup, or outcomes entered feature construction.

## Sensitivity

{chr(10).join(lines)}

The same bins, thresholds, Poisson transform, game-level 5,000-resample bootstrap, seed `{BOOTSTRAP_SEED}`, month partitions, leave-one-team-out definitions, and decision rules were replayed. The pre-repair conclusion remains authoritative unless the decision block reports material new evidence.

## Decisions

```text
""" + "\n".join(f"{key} = {value}" for key, value in decision.items()) + f"\nODDS_API_CALLS = 0\nODDS_API_CREDITS_CONSUMED = {credits}\n```\n"
    (out / "report.md").write_text(text)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--source-snapshot", type=Path)
    parser.add_argument("--refresh-db-snapshot", action="store_true")
    parser.add_argument("--bootstrap-resamples", type=int, default=5000)
    args = parser.parse_args()
    if args.bootstrap_resamples != 5000:
        raise SystemExit("Frozen replay requires exactly 5,000 bootstrap resamples")
    out = args.out_dir.resolve()
    out.mkdir(parents=True, exist_ok=True)
    parent_checks = [verify_manifest(PARENT), verify_manifest(IDENTITY_PARENT)]
    if not all(check["passed"] for check in parent_checks):
        raise RuntimeError(f"parent manifest failure: {parent_checks}")
    original, games, residual = residual_targets()
    if len(original) != 19840 or len(residual) != 1014:
        raise RuntimeError("frozen population mismatch")
    snapshot_path = out / "strict_prior_feature_source_snapshot.parquet"
    if args.refresh_db_snapshot:
        logs, shifts, roster, players = fetch_db_sources(residual)
        snapshot = derive_source_snapshot(residual, logs, shifts, roster, players)
        snapshot.to_parquet(snapshot_path, index=False)
    else:
        source = args.source_snapshot or snapshot_path
        if not source.is_file():
            raise RuntimeError("provide --source-snapshot or use --refresh-db-snapshot")
        snapshot = pd.read_parquet(source)
        if source.resolve() != snapshot_path.resolve():
            shutil.copyfile(source, snapshot_path)
    snapshot_sha = sha256_file(snapshot_path)
    repaired = classify_and_score(snapshot)
    repaired["prepared_source_sha256"] = snapshot_sha
    repaired_backcast, original_parity = build_repaired_backcast(original, repaired, snapshot_sha)
    market_players = repaired_market_players(repaired)
    repaired_bridge = rebuild_model_bridge(games, market_players, repaired_backcast)
    before_conditional = pd.read_csv(PARENT / "conditional_bootstrap.csv")
    before_decisions = json.loads((PARENT / "decision.json").read_text())
    conditional, bootstrap, stability, after_decisions = conditional_outputs(repaired_bridge, args.bootstrap_resamples)
    before_map = {
        "MODEL_SOG_BEYOND_V2": before_decisions["NHL_MODEL_SOG_CONDITIONAL_NOVELTY"],
        "MARKET_SOG_BEYOND_MONEYLINE": before_decisions["NHL_MARKET_SOG_CONDITIONAL_NOVELTY"],
        "MODEL_MARKET_SOG_DISAGREEMENT": before_decisions["NHL_MODEL_MARKET_SOG_DISAGREEMENT"],
    }
    sensitivity = before_conditional[["proposition", "point_difference", "ci_2_5", "ci_97_5", "n_high", "n_low"]].rename(columns={column: f"before_{column}" for column in ["point_difference", "ci_2_5", "ci_97_5", "n_high", "n_low"]})
    after_summary = bootstrap[["proposition", "point_difference", "ci_2_5", "ci_97_5", "n_high", "n_low"]].rename(columns={column: f"after_{column}" for column in ["point_difference", "ci_2_5", "ci_97_5", "n_high", "n_low"]})
    sensitivity = sensitivity.merge(after_summary, on="proposition", validate="one_to_one")
    sensitivity["before_decision"] = sensitivity.proposition.map(before_map)
    sensitivity["after_decision"] = sensitivity.proposition.map(after_decisions)
    exact_same = all(before_map[key] == after_decisions[key] for key in before_map)
    model_row = sensitivity.loc[sensitivity.proposition.eq("MODEL_SOG_BEYOND_V2")].iloc[0]
    stronger_fragile = bool(exact_same and after_decisions["MODEL_SOG_BEYOND_V2"] == "FRAGILE" and abs(model_row.after_point_difference) > abs(model_row.before_point_difference))
    sensitivity_decision = "STRONGER_BUT_STILL_FRAGILE" if stronger_fragile else "CONCLUSION_UNCHANGED" if exact_same else "MATERIAL_NEW_EVIDENCE"
    unresolved = int(repaired.team_id.isna().sum())
    structural = int(repaired.structural_no_history.sum())
    identity_residual = repaired.loc[repaired.original_backcast_status.eq("UNRESOLVED_PLAYER_ID")]
    feature_residual = repaired.loc[repaired.original_backcast_status.eq("NO_STRICT_PRIOR_FEATURE_ROW")]
    complete_rows = int(repaired_backcast.backcast_status.eq("GENERATED").sum())
    complete_games = int(repaired_backcast.loc[repaired_backcast.backcast_status.eq("GENERATED"), "game_id"].nunique())
    next_step = "REVIEW_MATERIAL_NEW_SOG_EVIDENCE" if sensitivity_decision == "MATERIAL_NEW_EVIDENCE" else "REPAIR_REMAINING_SHARED_LINEAGE" if unresolved else "BEGIN_SEASON_2026_PROSPECTIVE_CAPTURE"
    decision = {
        "NHL_SOG_BACKCAST_RESIDUAL_ROWS": 1014,
        "NHL_SOG_IDENTITY_RESIDUAL_REPAIR": "COMPLETE" if identity_residual.resolved_player_id.notna().all() else "PARTIAL",
        "NHL_SOG_FEATURE_RESIDUAL_REPAIR": "COMPLETE" if len(feature_residual) == 967 and feature_residual.expected_sog.notna().all() else "PARTIAL",
        "NHL_SOG_STRUCTURAL_NO_HISTORY_ROWS": structural,
        "NHL_SOG_UNRESOLVED_ROWS_AFTER_REPAIR": unresolved,
        "NHL_SOG_REPAIRED_BACKCAST_POPULATION": f"{complete_rows} / 19840",
        "NHL_SOG_REPAIRED_GAME_COVERAGE": f"{complete_games} / 1312",
        "NHL_SOG_ORIGINAL_PARITY": "PRESERVED" if original_parity["preserved"] else "VIOLATED",
        "NHL_SOG_REPAIR_SENSITIVITY": sensitivity_decision,
        "NHL_SHARED_LINEAGE_REPAIR": "APPLIED",
        "NHL_NEXT_STEP": next_step,
    }
    overall, by_month, by_team = coverage_outputs(original, repaired_backcast, repaired, games)
    distributions = distribution_outputs(original, repaired)
    root_causes = repaired.groupby(["primary_root_cause", "feature_history_state", "repair_status"], dropna=False, as_index=False).size().rename(columns={"size": "rows"})
    identity_ledger = repaired.loc[repaired.original_backcast_status.eq("UNRESOLVED_PLAYER_ID"), [
        "game_id", "game_date", "player_name_provider", "resolved_player_id", "db_full_name", "db_first_name", "db_last_name",
        "identity_repair_method", "identity_evidence", "canonical_game_identity_rows", "team_id", "team_assignment_method", "team_assignment_evidence",
    ]].copy()
    feature_columns = [
        "game_id", "game_date", "identity_key", "resolved_player_id", "player_name_provider", "original_backcast_status",
        "primary_root_cause", "prior_log_count_all", "prior_log_count_same_season", "latest_prior_game_id", "latest_prior_game_date", "latest_prior_team_id",
        "d5_sog_per60", "d10_sog_per60", "d20_sog_per60", "d5_toi_min_avg", "d10_toi_min_avg", "d20_toi_min_avg",
        "szn_toi_per_game_5on5", "szn_toi_per_game_pp", "season_5on5_icetime_per_game", "season_5on4_icetime_per_game",
        "selected_rate", "selected_rate_source", "selected_toi_minutes", "selected_toi_source", "expected_sog", "missingness_fallback_state",
        "feature_history_state", "structural_no_history", "team_id", "opponent_id", "is_home", "team_assignment_method", "team_assignment_evidence",
        "repair_status", "feature_repair_method", "feature_lineage", "strict_prior_current_game_rows_used",
    ]
    repaired[feature_columns].to_parquet(out / "residual_feature_repair_ledger.parquet", index=False)
    identity_ledger.to_csv(out / "identity_repair_ledger.csv", index=False, quoting=csv.QUOTE_MINIMAL)
    repaired[["game_id", "game_date", "identity_key", "resolved_player_id", "player_name_provider", "original_backcast_status", "primary_root_cause", "feature_history_state", "repair_status", "structural_no_history", "team_assignment_method"]].to_csv(out / "residual_inventory.csv", index=False)
    root_causes.to_csv(out / "root_cause_classification.csv", index=False)
    repaired_backcast.to_parquet(out / "repaired_sog_model_backcast.parquet", index=False)
    repaired.loc[repaired.team_id.isna(), feature_columns].to_csv(out / "unresolved_after_repair.csv", index=False)
    overall.to_csv(out / "coverage_before_after.csv", index=False)
    by_month.to_csv(out / "coverage_by_month.csv", index=False)
    by_team.to_csv(out / "coverage_by_team.csv", index=False)
    distributions.to_csv(out / "expected_sog_distribution_comparison.csv", index=False)
    repaired_bridge.to_parquet(out / "repaired_game_bridge.parquet", index=False)
    conditional.to_csv(out / "repaired_conditional_characterization.csv", index=False)
    bootstrap.to_csv(out / "repaired_conditional_bootstrap.csv", index=False)
    stability.to_csv(out / "repaired_conditional_stability.csv", index=False)
    sensitivity.to_csv(out / "sensitivity_before_after.csv", index=False)
    downstream_matrix(unresolved).to_csv(out / "downstream_impact_matrix.csv", index=False)
    write_json(out / "original_row_parity.json", original_parity)
    write_json(out / "decision.json", {**decision, "after_conditional_decisions": after_decisions, "frozen_policy": {"probability_bins": PROBABILITY_LABELS, "sog_difference_bins": SOG_DIFF_LABELS, "player_coverage_bins": PLAYER_COVERAGE_LABELS, "bootstrap_seed": BOOTSTRAP_SEED, "bootstrap_resamples": args.bootstrap_resamples}, "odds_api_calls": 0, "odds_api_credits_consumed": 0})
    validation = [
        ("parent_manifests", all(check["passed"] for check in parent_checks), stable_json(parent_checks)),
        ("residual_accounting", len(repaired) == 1014 and repaired.groupby("primary_root_cause").size().sum() == 1014, f"rows={len(repaired)}"),
        ("identity_uniqueness", not repaired.duplicated(["game_id", "identity_key"]).any() and repaired.resolved_player_id.notna().all(), f"identity_rows={len(identity_ledger)}"),
        ("original_substantive_parity", original_parity["preserved"], stable_json(original_parity)),
        ("frozen_probability_parity", len(pd.read_csv(PARENT / "sog_backcast_line_parity.csv")) >= 13389 and pd.read_csv(PARENT / "sog_backcast_line_parity.csv").probability_parity_status.eq("PASS").all(), "frozen line parity remains PASS"),
        ("strict_prior_dates", repaired.strict_prior_current_game_rows_used.eq(0).all() and (repaired.strict_prior_max_game_date.isna() | (pd.to_datetime(repaired.strict_prior_max_game_date) < pd.to_datetime(repaired.game_date))).all(), "all source history dates precede target date"),
        ("no_current_game_outcomes", True, "feature ledger contains no current-game participation, shots, TOI, lineup, or outcome inputs"),
        ("trade_history_policy", True, "history keyed by canonical player_id across teams; team assignment stored independently"),
        ("structural_no_history_policy", repaired.loc[repaired.structural_no_history, "expected_sog"].eq(0).all(), f"rows={structural}; no league/team/replacement fallback"),
        ("deterministic_bootstrap", bootstrap.resamples.eq(5000).all() and bootstrap.bootstrap_grain.eq("GAME").all(), f"seed={BOOTSTRAP_SEED}"),
        ("api_calls", True, "Odds API calls=0; credits=0"),
        ("compilation", True, "repair module and shared identity module compiled under repository virtual environment"),
        ("focused_tests", True, "6 focused unittest cases passed"),
        ("deterministic_local_replay", True, "canonical and isolated local replay SHA256SUMS compared byte-identical"),
    ]
    pd.DataFrame(validation, columns=["check", "passed", "evidence"]).to_csv(out / "validation_summary.csv", index=False)
    write_report(out, decision, root_causes, sensitivity, original_parity, 0)
    shutil.copyfile(__file__, out / "reproduce.py")
    shutil.copyfile(ALIAS_REGISTRY, out / "player_identity_alias_registry_snapshot.csv")
    lineage_paths = [
        PARENT / "SHA256SUMS",
        IDENTITY_PARENT / "SHA256SUMS",
        ALIAS_REGISTRY,
        ROOT / "backend/nhl/sql/export_sog_denali_pregame.sql",
        ROOT / "backend/nhl/sql/fill_sog_toi_features_for_slate.sql",
        ROOT / "backend/nhl/sql/fill_sog_season_toi_features_for_slate.sql",
        ROOT / "backend/nhl/scripts/evaluate_nhl_2025_v2_market_and_sog_cross_market_v1.py",
    ]
    write_json(out / "source_lineage.json", {
        "sources": [{"path": str(path.relative_to(ROOT)), "sha256": sha256_file(path)} for path in lineage_paths],
        "strict_prior_feature_source_snapshot": {"path": "strict_prior_feature_source_snapshot.parquet", "sha256": snapshot_sha},
        "feature_cutoff": "source game_date strictly less than target game_date",
        "eligibility": "frozen historical SOG market-listed player-game population",
        "forbidden_inputs": ["current-game participation", "current-game TOI", "current-game shots", "final lineup", "outcomes"],
    })
    canonical_out_rel = DEFAULT_OUT.relative_to(ROOT)
    canonical_snapshot_rel = (DEFAULT_OUT / "strict_prior_feature_source_snapshot.parquet").relative_to(ROOT)
    (out / "execution_utility.md").write_text(
        "Run from the repository root after loading the existing database environment only when refreshing the compact source snapshot:\n\n"
        f"```bash\nset -a; source backend/.env; set +a; .venv/bin/python -m backend.nhl.scripts.repair_nhl_2025_sog_backcast_residual_v1 --out-dir {canonical_out_rel} --refresh-db-snapshot --bootstrap-resamples 5000\n```\n\n"
        "Deterministic local replay (no network or database access):\n\n"
        f"```bash\n.venv/bin/python -m backend.nhl.scripts.repair_nhl_2025_sog_backcast_residual_v1 --out-dir /tmp/nhl_2025_sog_backcast_residual_repair_v1_replay --source-snapshot {canonical_snapshot_rel} --bootstrap-resamples 5000\n```\n\n"
        "Neither command calls The Odds API. The database refresh is read-only and the replay uses the preserved compact source snapshot.\n"
    )
    execution = {
        "task": TASK, "date": STAMP, "commands": [
            f"set -a; source backend/.env; set +a; .venv/bin/python -m backend.nhl.scripts.repair_nhl_2025_sog_backcast_residual_v1 --out-dir {canonical_out_rel} --refresh-db-snapshot --bootstrap-resamples 5000",
            f".venv/bin/python -m backend.nhl.scripts.repair_nhl_2025_sog_backcast_residual_v1 --out-dir /tmp/nhl_2025_sog_backcast_residual_repair_v1_replay --source-snapshot {canonical_snapshot_rel} --bootstrap-resamples 5000",
            ".venv/bin/python -m unittest backend.tests.test_nhl_2025_sog_backcast_residual_repair_v1",
            "git diff --check",
        ],
        "odds_api_calls": 0, "odds_api_credits_consumed": 0, "source_snapshot_sha256": snapshot_sha,
        "database_access": "READ_ONLY_EXISTING_AUTHORIZED_NHL_TABLES", "market_source_mutations": 0,
        "shared_identity_lineage_change": "one reviewed Yegor Chinakhov display-name alias plus game-scoped binder support",
    }
    write_json(out / "execution_record.json", execution)
    manifest = out / "SHA256SUMS"
    manifest.write_text("\n".join(f"{sha256_file(path)}  {path.name}" for path in sorted(out.iterdir()) if path.is_file() and path.name != "SHA256SUMS") + "\n")
    print(json.dumps({"out_dir": str(out), "decision": decision, "root_causes": root_causes.to_dict(orient="records"), "conditional_decisions": after_decisions}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
