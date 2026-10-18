"""Database export and observer plumbing for market-free Points/Saves snapshots."""
from __future__ import annotations

import argparse
import io
import json
import os
import subprocess
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg
from backend.nhl.database_access import (
    database_url,
    psql_command_without_password,
    restricted_mode,
)

from backend.nhl.prediction_only.core import (
    PROSPECTIVE_NOT_BEFORE,
    game_level_eligibility,
    publish_points,
    publish_saves,
)
from backend.nhl.reconciliation_certificate import (
    RosterObservationNotReady,
    require_slate_history_certificate,
    require_slate_roster_observation,
)
from backend.nhl.scripts.nhl_observer_provenance import observer_provenance
from backend.nhl.scripts.run_nhl_sog_prediction_only_warn_only import phase_for


ROOT = Path(__file__).resolve().parents[3]
SQL = ROOT / "backend/nhl/sql"
POINTS_OBSERVER_FEATURE_CONTRACT = (
    ROOT / "backend/nhl/points_shadow/phoenix_shadow_attempt_missingness_v1.json"
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def load_database_env(path: Path) -> str:
    """Read only database settings; never inspect or load bookmaker credentials."""
    if restricted_mode():
        return database_url("research")
    values = {key: os.environ.get(key, "").strip() for key in ("SUPABASE_DB_URL", "DATABASE_URL")}
    if path.is_file() and not any(values.values()):
        for raw in path.read_text().splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            if key.strip() in values and not values[key.strip()]:
                values[key.strip()] = value.strip().strip("'\"")
    return values["SUPABASE_DB_URL"] or values["DATABASE_URL"]


def durable_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, raw = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(raw)
    try:
        with os.fdopen(fd, "w") as handle:
            handle.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _psql_export(dsn: str, sql_path: Path, slate_date: str) -> pd.DataFrame:
    command, env = psql_command_without_password(
        ["psql", dsn, "--no-psqlrc", "-q", "-v", "ON_ERROR_STOP=1",
         "-v", f"slate_date={slate_date}", "-f", str(sql_path)]
    )
    result = subprocess.run(
        command, cwd=ROOT, check=True, capture_output=True, text=True, env=env,
    )
    return pd.read_csv(io.StringIO(result.stdout))


def _query(connection: psycopg.Connection, sql: str, params: tuple) -> pd.DataFrame:
    with connection.cursor() as cursor:
        cursor.execute(sql, params)
        return pd.DataFrame(cursor.fetchall(), columns=[item.name for item in cursor.description])


def _points_observer_feature_identity() -> dict:
    """Load the reviewed shadow-only feature identity used by Points cards."""
    identity = json.loads(POINTS_OBSERVER_FEATURE_CONTRACT.read_text())
    identity["_identity_path"] = str(POINTS_OBSERVER_FEATURE_CONTRACT.resolve())
    return identity


def _canonical_and_identity(dsn: str, season: int, slate_date: str,
                            cutoff: datetime, lane: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    history_table = "nhl.skater_game_logs_raw" if lane == "POINTS" else "nhl.goalie_game_logs_raw"
    with psycopg.connect(dsn) as connection:
        connection.execute("BEGIN READ ONLY")
        games = _query(connection, """
            SELECT season AS canonical_season,game_date::text AS slate_date,game_id,
                   home_team_id,home_team_code AS home_team,away_team_id,
                   away_team_code AS away_team,start_time_utc AS scheduled_start_time_utc,
                   game_type AS game_type_code,upper(coalesce(status,'SCHEDULED')) AS game_status
            FROM nhl.games WHERE season=%s AND game_date=%s::date
            ORDER BY start_time_utc,game_id
        """, (season, slate_date))
        if games.empty:
            connection.rollback()
            return games, pd.DataFrame()
        identity = _query(connection, f"""
            WITH latest AS (
              SELECT DISTINCT ON (r.game_id,r.player_id)
                     r.game_id,r.player_id,r.team_id,r.active_flag,r.asof_ts
              FROM nhl.roster_status r
              WHERE r.game_id=ANY(%s) AND r.asof_ts<=%s
              ORDER BY r.game_id,r.player_id,r.asof_ts DESC
            )
            SELECT g.season AS canonical_season,g.game_date::text AS slate_date,g.game_id,
                   l.player_id,p.full_name,p.position,l.team_id,l.active_flag,l.asof_ts,
                   g.home_team_id,g.home_team_code,g.away_team_id,g.away_team_code,
                   g.start_time_utc AS scheduled_start_time_utc,g.game_type AS game_type_code,
                   (SELECT max(hg.start_time_utc) FROM {history_table} h
                    JOIN nhl.games hg USING(game_id)
                    WHERE h.player_id=l.player_id AND hg.start_time_utc<%s) AS feature_history_max_timestamp_utc
            FROM latest l JOIN nhl.games g USING(game_id) JOIN nhl.players p USING(player_id)
            WHERE l.active_flag IS TRUE
            ORDER BY g.start_time_utc,g.game_id,l.player_id
        """, (games.game_id.astype(int).tolist(), cutoff, cutoff))
        connection.rollback()
    games.attrs["roster_identity_scope_known"] = True
    games.attrs["roster_identity_game_ids"] = sorted(
        identity.game_id.astype(int).unique().tolist()) if not identity.empty else []
    return games, identity


def export_lane_inputs(dsn: str, lane: str, season: int, slate_date: str,
                       cutoff: datetime) -> tuple[pd.DataFrame, pd.DataFrame]:
    games, identity = _canonical_and_identity(dsn, season, slate_date, cutoff, lane)
    if games.empty:
        return games, identity
    sql_path = SQL / ("export_points.sql" if lane == "POINTS" else "export_saves_from_denali.sql")
    features = _psql_export(dsn, sql_path, slate_date)
    key = ["game_id", "player_id"]
    if not features.empty and features.duplicated(key).any():
        raise RuntimeError(f"{lane}_FEATURE_EXPORT_DUPLICATE_IDENTITY")
    # Canonical identity and orientation always come from the cutoff-bounded
    # roster/game query, never from convenience columns in the model export.
    overlap = sorted((set(features) & set(identity)) - set(key))
    if overlap:
        features = features.drop(columns=overlap)
    identity_keys = set(map(tuple, identity[key].astype(int).to_numpy()))
    feature_keys = set(map(tuple, features[key].astype(int).to_numpy()))
    unexpected = sorted(feature_keys - identity_keys)
    if unexpected:
        raise RuntimeError(f"{lane}_FEATURE_IDENTITY_SOURCE_CONFLICT:{unexpected[:20]}")
    merged = features.merge(identity, on=key, how="inner", validate="one_to_one")
    missing_feature_keys = sorted(identity_keys - feature_keys)
    epoch = "1970-01-01T00:00:00Z"
    merged["feature_history_max_timestamp_utc"] = merged.feature_history_max_timestamp_utc.fillna(epoch)
    merged["feature_cutoff_timestamp_utc"] = cutoff.isoformat()
    merged["roster_source_timestamp_utc"] = pd.to_datetime(merged.asof_ts, utc=True).map(lambda value: value.isoformat())
    merged["scheduled_start_time_utc"] = pd.to_datetime(merged.scheduled_start_time_utc, utc=True).map(lambda value: value.isoformat())
    merged["team"] = merged.apply(
        lambda row: row.home_team_code if int(row.team_id) == int(row.home_team_id) else row.away_team_code, axis=1)
    merged["opponent"] = merged.apply(
        lambda row: row.away_team_code if int(row.team_id) == int(row.home_team_id) else row.home_team_code, axis=1)
    if lane == "POINTS":
        merged["player_name"] = merged.full_name
        merged["pregame_participation_state"] = "ACTIVE"
    else:
        merged = merged.rename(columns={"player_id": "goalie_id", "full_name": "goalie_name"})
        merged["goalie_eligibility_state"] = "ACTIVE_ROSTER_STARTER_UNKNOWN"
        merged["population_contract"] = "COMPLETE_SCORER_ELIGIBLE"
        merged["scorer_eligible"] = True
        merged["expected_complete_population_rows"] = len(merged)
    merged.attrs["input_exclusions"] = [
        {"game_id": int(game_id), "player_id": int(player_id),
         "reason": "NO_QUALIFIED_LANE_FEATURE_HISTORY"}
        for game_id, player_id in missing_feature_keys
    ]
    return games, merged


def observe(*, lane: str, season: int, slate_date: str, requested_phase: str,
            observation_timestamp: datetime, canonical_run_identifier: str | None,
            dsn: str, output_root: Path) -> Path:
    lane = lane.upper()
    stamp = observation_timestamp.strftime("%Y%m%dT%H%M%S.%fZ") + "_" + uuid.uuid4().hex[:8]
    status_path = output_root / "status" / slate_date / f"prediction_{stamp}.json"
    result = {
        "contract_version": "NHL_GENERAL_PREDICTION_ONLY_OBSERVER_V1", "lane": lane,
        "canonical_season": season, "slate_date": slate_date, "status": "RUNNING",
        "observation_timestamp_utc": observation_timestamp.isoformat(),
        "market_directory_dependency": "NONE", "provider_event_dependency": "NONE",
        "bookmaker_credential_access": False, "market_requests": 0, "paid_credits": 0,
        "warning_only": True, **observer_provenance(Path(__file__), observation_timestamp),
    }
    try:
        if slate_date < PROSPECTIVE_NOT_BEFORE:
            raise RuntimeError(f"RETROSPECTIVE_PREDICTION_FORBIDDEN:NOT_BEFORE_{PROSPECTIVE_NOT_BEFORE}")
        if not dsn:
            raise RuntimeError("DATABASE_URL_MISSING")
        certificate = require_slate_history_certificate(slate_date)
        roster = require_slate_roster_observation(slate_date)
        result["prior_day_reconciliation_certificate"] = certificate
        result["current_slate_roster_observation"] = roster
        games, features = export_lane_inputs(dsn, lane, season, slate_date, observation_timestamp)
        if games.empty:
            result.update(status="VALID_EMPTY_SLATE", phase=None, prediction_rows=0)
            durable_json(status_path, result)
            return status_path
        eligible_game_ids, started_exclusions = game_level_eligibility(
            games, observation_timestamp.isoformat())
        started_ids = {int(row["game_id"]) for row in started_exclusions}
        feature_game_ids = (set(features.game_id.astype(int)) if not features.empty else set())
        roster_ids = set(map(int, games.attrs.get("roster_identity_game_ids", [])))
        game_exclusions = list(started_exclusions)
        for game_id in eligible_game_ids:
            if game_id not in feature_game_ids:
                reason = ("NO_ACTIVE_CUTOFF_BOUNDED_ROSTER_IDENTITIES"
                          if games.attrs.get("roster_identity_scope_known") is True
                          and game_id not in roster_ids
                          else f"NO_QUALIFIED_{lane}_FEATURE_INPUTS")
                game_exclusions.append({"game_id": game_id, "reason": reason})
        result.update(
            canonical_game_ids=sorted(games.game_id.astype(int).tolist()),
            eligible_game_ids=eligible_game_ids,
            started_excluded_games=started_exclusions,
            game_exclusions=game_exclusions,
            feature_input_exclusions=features.attrs.get("input_exclusions", []),
        )
        if not eligible_game_ids:
            result.update(status="NOOP_NO_ELIGIBLE_PREGAME_GAMES",
                          phase=None, prediction_rows=0)
            durable_json(status_path, result)
            return status_path
        if features.empty:
            result.update(status="NOOP_NO_QUALIFIED_FEATURE_INPUTS",
                          phase=None, prediction_rows=0,
                          feature_input_exclusions=features.attrs.get("input_exclusions", []))
            durable_json(status_path, result)
            return status_path
        if requested_phase == "AUTO":
            phase, gate = phase_for(games, observation_timestamp, requested_phase)
        else:
            # Explicit governed commands must reach the whole-slate timing gate;
            # they may not silently turn a post-start attempt into a no-op.
            phase, gate = requested_phase, "EXPLICIT_PHASE"
        result.update(phase=phase, phase_gate=gate, canonical_games=len(games))
        if phase is None:
            result["status"] = "NOOP_OUTSIDE_PREDICTION_WINDOW"
        else:
            run_identifier = canonical_run_identifier or f"NHL_{lane}_{season}_{slate_date}_{phase}"
            publisher = publish_points if lane == "POINTS" else publish_saves
            destination, disposition = publisher(
                games=games, players=features, output_root=output_root, season=season,
                slate_date=slate_date, phase=phase,
                observation_timestamp_utc=observation_timestamp.isoformat(),
                input_cutoff_timestamp_utc=observation_timestamp.isoformat(),
                canonical_run_identifier=run_identifier,
                feature_contract_identity=_points_observer_feature_identity(),
                readiness_evidence={"prior_day_reconciliation_certificate": certificate,
                                    "current_slate_roster_observation": roster},
            ) if lane == "POINTS" else publisher(
                games=games, goalies=features, output_root=output_root, season=season,
                slate_date=slate_date, phase=phase,
                observation_timestamp_utc=observation_timestamp.isoformat(),
                input_cutoff_timestamp_utc=observation_timestamp.isoformat(),
                canonical_run_identifier=run_identifier,
                readiness_evidence={"prior_day_reconciliation_certificate": certificate,
                                    "current_slate_roster_observation": roster},
            )
            metadata = json.loads((destination / "run_metadata.json").read_text())
            result.update(status="CAPTURED" if disposition == "COMPLETE_NEW_APPEND_ONLY" else "NOOP_ALREADY_CAPTURED",
                          disposition=disposition, run_dir=str(destination),
                          prediction_rows=metadata["prediction_rows"],
                          coverage_status=metadata.get("coverage_status"),
                          prediction_game_ids=metadata.get("prediction_game_ids", []),
                          game_exclusions_path=str(destination / "game_exclusions.csv"))
            if disposition == "COMPLETE_NEW_APPEND_ONLY":
                # Recorded only after the immutable package has been atomically published.
                result["durable_publication_timestamp_utc"] = (
                    utc_now().isoformat().replace("+00:00", "Z"))
    except RosterObservationNotReady as error:
        result.update(
            status="ROSTER_NOT_READY",
            readiness_reason=str(error),
            retry_eligible_on_later_poll=True,
            feature_export_started=False,
            prediction_attempt_created=False,
        )
    except Exception as error:
        result.update(status="FAILED_WARN_ONLY", failure=f"{type(error).__name__}:{error}")
    durable_json(status_path, result)
    return status_path


def cli(lane: str) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--slate-date", required=True)
    parser.add_argument("--phase", choices=["MIDDAY", "FINAL_PREGAME"], required=True)
    parser.add_argument("--observation-timestamp-utc", required=True)
    parser.add_argument("--canonical-run-identifier", required=True)
    parser.add_argument("--env-file", type=Path, default=ROOT / "backend/.env")
    parser.add_argument("--output-root", type=Path, default=ROOT / "artifacts/operational/nhl" / f"{lane.lower()}_prediction_only")
    args = parser.parse_args()
    observation = parse_timestamp(args.observation_timestamp_utc)
    status = observe(
        lane=lane, season=args.season, slate_date=args.slate_date,
        requested_phase=args.phase, observation_timestamp=observation,
        canonical_run_identifier=args.canonical_run_identifier,
        dsn=load_database_env(args.env_file), output_root=args.output_root,
    )
    payload = json.loads(status.read_text())
    print(status)
    return 0 if payload["status"] in {
        "CAPTURED", "NOOP_ALREADY_CAPTURED", "VALID_EMPTY_SLATE", "ROSTER_NOT_READY",
    } else 1


def parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("OBSERVATION_TIMESTAMP_MUST_BE_TIMEZONE_AWARE")
    return parsed.astimezone(timezone.utc)
