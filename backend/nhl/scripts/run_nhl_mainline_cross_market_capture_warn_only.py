#!/usr/bin/env python3
"""Bounded WARN-only live runner for the armed NHL mainline cross-market shadow."""
from __future__ import annotations

import argparse
import contextlib
import fcntl
import json
import os
import subprocess
import sys
import tempfile
import uuid
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg
from backend.nhl.game_phase import authorized_season_for_date, season_for_date
from backend.nhl.database_access import database_url, owner_free_child_environment

from backend.nhl.cross_market_shadow.core import PRESEASON_START, REGULAR_SEASON_START, fetch_markets, run_capture
from backend.nhl.cross_market_shadow.canonical_slate_adapter import adapt_canonical_slate
from backend.nhl.cross_market_shadow.official_outcomes import load_official_outcomes
from backend.nhl.reconciliation_certificate import (
    RosterObservationNotReady,
    require_full_roster_coverage,
    require_prior_day_certificate,
    require_slate_history_certificate,
    require_slate_roster_observation,
    verified_all_games_void,
    verify_reconciliation_certificate,
)
from backend.nhl.daily_capture import load_canonical_slate
from backend.nhl.daily_orchestration import redact_sensitive_text
from backend.nhl.scripts.nhl_prediction_only_common import observe as observe_independent_prediction_only
from backend.nhl.scripts.run_nhl_sog_prediction_only_warn_only import observe as observe_sog_prediction_only
from backend.nhl.scripts.nhl_observer_provenance import observer_provenance


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ROOT = ROOT / "artifacts/operational/nhl/cross_market_shadow"
DEFAULT_OUTCOME_ROOT = ROOT / "artifacts/operational/nhl/postgame_reconciliation"


class PriorDayChildFailure(RuntimeError):
    """Bounded, redacted child evidence for the parent observer receipt."""

    def __init__(self, stage: str, exit_status: int | None,
                 stdout: object = "", stderr: object = "", reason: str | None = None):
        self.stage = stage
        self.exit_status = exit_status
        self.diagnostics = {
            "stage": stage,
            "exit_status": exit_status,
            "reason": (redact_sensitive_text(reason)[:500] if reason else None),
            "stdout_tail": redact_sensitive_text(stdout)[-1200:],
            "stderr_tail": redact_sensitive_text(stderr)[-1200:],
        }
        super().__init__(f"PRIOR_DAY_CHILD_FAILED:{stage}:exit={exit_status}")


def _run_prior_day_child(stage: str, command: list[str]) -> subprocess.CompletedProcess:
    try:
        result = subprocess.run(
            command, cwd=str(ROOT), check=False, text=True, capture_output=True,
            env=owner_free_child_environment(),
        )
    except Exception as error:
        raise PriorDayChildFailure(
            stage, None, stderr=redact_sensitive_text(error),
            reason=f"{type(error).__name__}:{error}",
        ) from error
    if result.returncode:
        raise PriorDayChildFailure(
            stage, result.returncode, result.stdout, result.stderr,
        )
    return result


def _verify_prior_day_package(outcome_root: Path, prior_slate: str,
                              expected_game_ids: list[int]) -> dict[str, object]:
    day_root = outcome_root / prior_slate
    packages = sorted(day_root.glob("reconciliation=*") if day_root.is_dir() else [])
    if len(packages) != 1:
        raise RuntimeError(
            f"CROSS_MARKET_PRIOR_DAY_PACKAGE_CARDINALITY:{len(packages)}"
        )
    certificate = verify_reconciliation_certificate(packages[0], prior_slate)
    # Postponed, suspended or cancelled games are void: reconciliation leaves
    # them out of the certified (played) game set.
    expected = (set(map(int, expected_game_ids))
                - set(map(int, certificate.get("voided_game_ids") or [])))
    certified = set(map(int, certificate.get("game_ids") or []))
    if certified != expected:
        raise RuntimeError(
            "CROSS_MARKET_PRIOR_DAY_CERTIFIED_GAME_SET_MISMATCH:"
            f"expected={len(expected)}:actual={len(certified)}"
        )
    outcomes = load_official_outcomes(outcome_root)
    observed = pd.to_numeric(outcomes.game_id, errors="coerce").dropna().astype(int)
    if not expected.issubset(set(observed)):
        raise RuntimeError("CROSS_MARKET_PRIOR_DAY_RECONCILIATION_OUTPUT_INCOMPLETE")
    return certificate


def _player_prop_observer_link(path: Path) -> dict[str, object]:
    """Read only the child status needed to link its authoritative receipt."""
    link: dict[str, object] = {"receipt_path": str(path)}
    try:
        payload = json.loads(path.read_text())
        if not isinstance(payload, dict) or not isinstance(payload.get("status"), str):
            raise ValueError("CHILD_STATUS_RECEIPT_INVALID_SHAPE")
        link["status"] = payload["status"]
    except Exception as error:
        link.update(status="CHILD_RECEIPT_UNREADABLE", failure={
            "exception_type": type(error).__name__,
            "message": redact_sensitive_text(error)[:500],
        })
    return link


def _player_prop_observer_failure(error: Exception) -> dict[str, object]:
    return {
        "status": "FAILED_BEFORE_RECEIPT",
        "receipt_path": None,
        "failure": {
            "exception_type": type(error).__name__,
            "message": redact_sensitive_text(error)[:500],
        },
    }


def observe_prediction_only_lanes(slate: str, requested: str, dsn: str,
                                  now: datetime) -> dict[str, dict[str, object]]:
    """Run and link independent lane statuses; none gate mainline capture."""
    results: dict[str, dict[str, object]] = {}
    for lane in ("POINTS", "SAVES"):
        try:
            path = observe_independent_prediction_only(
                lane=lane, season=authorized_season_for_date(slate), slate_date=slate, requested_phase=requested,
                observation_timestamp=now, canonical_run_identifier=None, dsn=dsn,
                output_root=ROOT / "artifacts/operational/nhl" / f"{lane.lower()}_prediction_only",
            )
            results[lane] = _player_prop_observer_link(Path(path))
        except Exception as error:
            # Each publisher normally emits its own FAILED_WARN_ONLY status. This
            # boundary additionally prevents an unexpected lane defect from
            # suppressing another prediction lane or changing paid-market flow.
            results[lane] = _player_prop_observer_failure(error)
    try:
        path = observe_sog_prediction_only(
            slate=slate, requested=requested, dsn=dsn,
            root=ROOT / "artifacts/operational/nhl/sog_prediction_only", now=now,
        )
        results["SOG"] = _player_prop_observer_link(Path(path))
    except Exception as error:
        results["SOG"] = _player_prop_observer_failure(error)
    return results


def load_env(path: Path) -> None:
    if not path.is_file():
        return
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def validate_history_score_source(
    history: pd.DataFrame,
    schedule: pd.DataFrame,
) -> None:
    """Fail closed unless history goals match the frozen final-score contract."""
    if history.empty:
        return

    required = {
        "canonical_season", "game_id", "home_team_id", "away_team_id",
        "scheduled_start_time_utc", "game_status", "final_home_goals",
        "final_away_goals", "score_source", "score_status",
        "score_identity_qualified", "score_observed_at_utc",
    }
    missing = sorted(required - set(history.columns))
    if missing:
        raise ValueError(f"CROSS_MARKET_OFFICIAL_FINAL_SCORE_FIELDS_MISSING:{','.join(missing)}")
    if history.duplicated(["canonical_season", "game_id"]).any():
        raise ValueError("CROSS_MARKET_DUPLICATE_FINAL_SCORE_IDENTITY")
    if not history.score_source.astype(str).eq("OFFICIAL_NHL_FINAL_SCORE").all():
        raise ValueError("CROSS_MARKET_TRAINING_COMPATIBLE_FINAL_SCORE_SOURCE_REQUIRED")
    if not history.score_status.astype(str).eq("QUALIFIED").all():
        raise ValueError("CROSS_MARKET_UNQUALIFIED_FINAL_SCORE")
    identity_qualified = history.score_identity_qualified.astype("string").str.lower().eq("true")
    if not identity_qualified.all():
        raise ValueError("CROSS_MARKET_FINAL_SCORE_IDENTITY_CONFLICT")
    if not history.game_status.astype(str).str.upper().isin({"FINAL", "OFF"}).all():
        raise ValueError("CROSS_MARKET_GAME_NOT_OFFICIAL_FINAL")

    home = pd.to_numeric(history.final_home_goals, errors="coerce")
    away = pd.to_numeric(history.final_away_goals, errors="coerce")
    if (home.isna() | away.isna() | home.lt(0) | away.lt(0)).any():
        raise ValueError("CROSS_MARKET_FINAL_SCORE_MISSING_OR_INVALID")
    if home.eq(away).any():
        raise ValueError("CROSS_MARKET_FINAL_SCORE_NOT_DECISIVE")

    observed = pd.to_datetime(history.score_observed_at_utc, utc=True, errors="coerce")
    target_starts = pd.to_datetime(schedule.scheduled_start_time_utc, utc=True, errors="coerce")
    if observed.isna().any() or target_starts.isna().any() or target_starts.empty:
        raise ValueError("CROSS_MARKET_SCORE_OR_TARGET_TIMING_MISSING")
    scheduled = pd.to_datetime(history.scheduled_start_time_utc, utc=True, errors="coerce")
    # A retained final can be usable for a later target even when it was
    # observed after an earlier game on the same slate had already started.
    # Downstream feature construction applies this pairwise rule per target.
    pairwise_prior = scheduled.to_numpy()[:, None] < target_starts.to_numpy()[None, :]
    pairwise_observed = observed.to_numpy()[:, None] < target_starts.to_numpy()[None, :]
    if scheduled.isna().any() or not pairwise_prior.any(axis=1).all():
        raise ValueError("CROSS_MARKET_FINAL_SCORE_NOT_STRICT_PRIOR")
    if not (pairwise_prior & pairwise_observed).any(axis=1).all():
        raise ValueError("CROSS_MARKET_FINAL_SCORE_NOT_OBSERVED_BEFORE_TARGET")
    for side in ("home", "away"):
        expected = pd.to_numeric(history[f"{side}_team_id"], errors="coerce")
        outcome = pd.to_numeric(history[f"score_{side}_team_id"], errors="coerce")
        if expected.isna().any() or outcome.isna().any() or not expected.eq(outcome).all():
            raise ValueError(f"CROSS_MARKET_FINAL_SCORE_{side.upper()}_IDENTITY_MISMATCH")


def _prior_day_regular_season_game_ids(dsn: str, prior_slate: str) -> list[int]:
    """Return the prior day's canonical game IDs, as reconciliation admits them.

    Every game type (preseason, regular season, postseason) and the season of
    the date, matching run_nhl_postgame_reconciliation.canonical_slate, so the
    certified package's game set compares equal on playoff and new-season days.
    """
    season = season_for_date(prior_slate)
    with psycopg.connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT game_id
            FROM nhl.games
            WHERE season = %s AND game_date = %s::date
            ORDER BY game_id
            """,
            (season, prior_slate),
        )
        return [int(row[0]) for row in cursor.fetchall()]


def _parse_json_output(output: str, *, expected_status: str) -> dict:
    """Parse a governed preflight's JSON-only output and require its status."""
    try:
        payload = json.loads(output)
    except json.JSONDecodeError as error:
        raise RuntimeError("CROSS_MARKET_RECONCILIATION_PREFLIGHT_INVALID_JSON") from error
    if not isinstance(payload, dict):
        raise RuntimeError("CROSS_MARKET_RECONCILIATION_PREFLIGHT_INVALID_SHAPE")
    if payload.get("status") != expected_status:
        raise RuntimeError(
            "CROSS_MARKET_PRIOR_DAY_RECONCILIATION_BLOCKED:"
            f"{payload.get('failure', payload.get('status', 'INVALID_STATUS'))}"
        )
    return payload


def _all_games_void_certificate(outcome_root: Path, prior_slate: str,
                                game_ids: list[int]) -> dict[str, object] | None:
    """The all-void certificate when it covers exactly the date's games, else None."""
    try:
        certificate = verified_all_games_void(prior_slate, reconciliation_root=outcome_root)
    except RuntimeError as error:
        if "PRIOR_DATE_ALL_GAMES_VOID_PROBE_MISSING" in str(error):
            return None
        raise
    if set(certificate["voided_game_ids"]) != set(map(int, game_ids)):
        raise RuntimeError("CROSS_MARKET_PRIOR_DAY_ALL_VOID_GAME_SET_MISMATCH")
    return {"status": "PRIOR_DAY_ALL_GAMES_VOID", "slate_date": prior_slate,
            "game_ids": [], "certificate": certificate}


def ensure_prior_day_official_outcomes(
    dsn: str, slate: str, *, outcome_root: Path = DEFAULT_OUTCOME_ROOT,
    now: datetime | None = None,
) -> dict[str, object]:
    """Ensure yesterday's regular-season finals have the canonical governed package.

    This runs only for today's operational slate and only for the immediately
    prior date. Existing evidence is validated by the same immutable reader
    used by cross-market history. It never substitutes database player stats
    for official scores.
    """
    today = (now or datetime.now(ZoneInfo("America/New_York"))).astimezone(
        ZoneInfo("America/New_York")).date().isoformat()
    if slate != today:
        return {"status": "NOT_CURRENT_OPERATIONAL_SLATE"}
    prior_slate = (date.fromisoformat(slate) - timedelta(days=1)).isoformat()
    game_ids = _prior_day_regular_season_game_ids(dsn, prior_slate)
    if not game_ids:
        certificate = require_prior_day_certificate(
            prior_slate, reconciliation_root=outcome_root,
            slate_root=ROOT / "artifacts/operational/nhl/slates",
        )
        if certificate.get("status") != "VERIFIED_EMPTY_SCHEDULE":
            raise RuntimeError("CROSS_MARKET_PRIOR_DAY_DATABASE_SCHEDULE_MISMATCH")
        return {"status": "NO_PRIOR_DAY_REGULAR_SEASON_GAMES",
                "slate_date": prior_slate, "certificate": certificate}

    day_root = outcome_root / prior_slate
    packages = sorted(day_root.glob("reconciliation=*") if day_root.is_dir() else [])
    if not packages:
        all_void = _all_games_void_certificate(outcome_root, prior_slate, game_ids)
        if all_void is not None:
            return all_void
    if packages:
        certificate = _verify_prior_day_package(outcome_root, prior_slate, game_ids)
        return {"status": "EXISTING_GOVERNED_PACKAGE_VALID", "slate_date": prior_slate,
                "game_ids": game_ids, "certificate": certificate}
    if day_root.is_dir() and list(day_root.glob(".reconciliation.incomplete.*")):
        raise RuntimeError("PARTIAL_RECONCILIATION_PACKAGE_REQUIRES_REVIEW")

    base = [sys.executable, "-m", "backend.nhl.scripts.run_nhl_postgame_reconciliation"]
    acquisition = _run_prior_day_child(
        "authority_roster_acquisition",
        [*base, "--authority-roster-acquisition", "--prior-learning", prior_slate],
    )
    try:
        candidate = json.loads(acquisition.stdout)
    except (TypeError, ValueError):
        candidate = {}
    if isinstance(candidate, dict) and candidate.get("status") == "ALL_SCHEDULED_GAMES_VOID":
        all_void = _all_games_void_certificate(outcome_root, prior_slate, game_ids)
        if all_void is None:
            raise RuntimeError("CROSS_MARKET_PRIOR_DAY_ALL_GAMES_VOID_NOT_CERTIFIED")
        return all_void
    try:
        acquisition_payload = _parse_json_output(
            acquisition.stdout, expected_status="COMPLETE")
    except Exception as error:
        raise PriorDayChildFailure(
            "authority_roster_acquisition_receipt", acquisition.returncode,
            acquisition.stdout, acquisition.stderr,
            reason=f"{type(error).__name__}:{error}",
        ) from error
    source_id = str(acquisition_payload.get("run_id") or "")
    voided_ids = {int(row["game_id"])
                  for row in acquisition_payload.get("schedule_voided_games") or []}
    if (not source_id or set(map(int, acquisition_payload.get("game_ids") or []))
            != set(game_ids) - voided_ids):
        raise RuntimeError("CROSS_MARKET_PRIOR_DAY_ACQUISITION_GAME_SET_MISMATCH")
    sources = [
        "--response-source", f"AUTHORITY_RESPONSE_SOURCE={source_id}",
        "--response-source", f"ROSTER_RESPONSE_SOURCE={source_id}",
    ]
    local = _run_prior_day_child(
        "local_input_preflight",
        [*base, "--local-input-preflight", "--prior-learning", prior_slate, *sources],
    )
    try:
        _parse_json_output(local.stdout, expected_status="LOCAL_INPUTS_VALID")
    except Exception as error:
        raise PriorDayChildFailure(
            "local_input_preflight_receipt", local.returncode,
            local.stdout, local.stderr, reason=f"{type(error).__name__}:{error}",
        ) from error
    identity = _run_prior_day_child(
        "database_identity_preflight",
        [*base, "--database-identity-preflight", "--prior-learning", prior_slate, *sources],
    )
    try:
        identity_payload = _parse_json_output(
            identity.stdout, expected_status="DATABASE_IDENTITY_PREFLIGHT_VALID")
    except Exception as error:
        raise PriorDayChildFailure(
            "database_identity_preflight_receipt", identity.returncode,
            identity.stdout, identity.stderr, reason=f"{type(error).__name__}:{error}",
        ) from error
    partition = identity_payload.get("database_preflight") or {}
    classification = partition.get("classification") or {}
    if (partition.get("authorized_new_official_lookup_ids")
            or classification.get("new_official_lookup")
            or classification.get("conflict")):
        raise RuntimeError(
            "CROSS_MARKET_PRIOR_DAY_PLAYER_IDENTITY_AUTHORIZATION_REQUIRED"
        )

    execution = _run_prior_day_child(
        "reconciliation_execution",
        [*base, "--execute", "--prior-learning", prior_slate, *sources],
    )
    certificate = _verify_prior_day_package(outcome_root, prior_slate, game_ids)
    return {"status": "GOVERNED_RECONCILIATION_COMPLETE", "slate_date": prior_slate,
            "game_ids": game_ids, "acquisition_run_id": source_id,
            "execution_exit_status": execution.returncode,
            "certificate": certificate}


def export_inputs(dsn: str, slate_date: str, directory: Path, *,
                  canonical_schedule: pd.DataFrame | None = None,
                  outcome_root: Path = DEFAULT_OUTCOME_ROOT) -> tuple[Path, Path, pd.DataFrame]:
    season = authorized_season_for_date(slate_date)
    with psycopg.connect(dsn) as connection:
        if canonical_schedule is None:
            schedule = pd.read_sql_query("""
                SELECT season AS canonical_season, game_date::text AS slate_date, game_id,
                       game_date::text AS game_date, start_time_utc AS scheduled_start_time_utc,
                       home_team_id, home_team_code AS home_team, away_team_id,
                       away_team_code AS away_team, upper(coalesce(status,'SCHEDULED')) AS game_status,
                       game_type AS game_type_code
                FROM nhl.games WHERE season=%s AND game_date=%s::date
                ORDER BY start_time_utc,game_id
            """, connection, params=(season, slate_date))
        else:
            schedule = canonical_schedule.copy()
            if not schedule.slate_date.astype(str).eq(slate_date).all():
                raise ValueError("CANONICAL_SCHEDULE_SLATE_DATE_MISMATCH")
        if schedule.empty:
            history_cutoff = None
        else:
            history_cutoff = pd.to_datetime(
                schedule.scheduled_start_time_utc, utc=True, errors="raise"
            ).min()
        history = pd.read_sql_query("""
            WITH team_totals AS (
              SELECT g.season AS canonical_season,g.game_id,g.game_date,g.start_time_utc,
                     g.home_team_id,g.home_team_code,g.away_team_id,g.away_team_code,
                     g.status,g.game_type,l.team_id,
                     count(*)::int AS skater_rows,
                     count(DISTINCT l.player_id)::int AS distinct_skater_players,
                     sum(coalesce(l.shots_on_goal,0))::int AS shots
              FROM nhl.games g JOIN nhl.skater_game_logs_raw l USING(game_id)
              WHERE g.season=%s AND g.game_type=2 AND g.start_time_utc < %s::timestamptz
              GROUP BY g.season,g.game_id,g.game_date,g.start_time_utc,g.home_team_id,
                       g.home_team_code,g.away_team_id,g.away_team_code,g.status,g.game_type,l.team_id
            )
            SELECT h.canonical_season,h.game_date::text AS slate_date,h.game_id,
                   h.game_date::text AS game_date,h.start_time_utc AS scheduled_start_time_utc,
                   h.home_team_id,h.home_team_code AS home_team,h.away_team_id,
                   h.away_team_code AS away_team,upper(h.status) AS game_status,h.game_type AS game_type_code,
                   h.shots AS final_home_shots,a.shots AS final_away_shots,
                   h.skater_rows AS final_home_skater_rows,
                   h.distinct_skater_players AS final_home_distinct_skater_players,
                   a.skater_rows AS final_away_skater_rows,
                   a.distinct_skater_players AS final_away_distinct_skater_players
            FROM team_totals h JOIN team_totals a USING(game_id)
            WHERE h.team_id=h.home_team_id AND a.team_id=a.away_team_id
            ORDER BY h.start_time_utc,h.game_id
        """, connection, params=(season, history_cutoff))
    if not history.empty:
        for side in ("home", "away"):
            rows = pd.to_numeric(history[f"final_{side}_skater_rows"], errors="coerce")
            players = pd.to_numeric(
                history[f"final_{side}_distinct_skater_players"], errors="coerce"
            )
            if rows.isna().any() or players.isna().any() or not players.eq(rows).all():
                raise ValueError(f"CROSS_MARKET_SKATER_SHOTS_INCOMPLETE:{side.upper()}")
        source = load_official_outcomes(outcome_root)
        if source.empty:
            raise ValueError("CROSS_MARKET_OFFICIAL_OUTCOME_EVIDENCE_MISSING")
        else:
            candidate_ids = set(history.game_id.astype(int))
            source_ids = set(source.game_id.astype(int))
            missing_outcomes = sorted(candidate_ids - source_ids)
            if missing_outcomes:
                raise ValueError(
                    "CROSS_MARKET_OFFICIAL_OUTCOME_EVIDENCE_MISSING:"
                    + ",".join(map(str, missing_outcomes[:20]))
                )
            history["scheduled_start_time_utc"] = pd.to_datetime(
                history.scheduled_start_time_utc, utc=True, errors="raise"
            )
            history = history.drop(columns=["game_status"], errors="ignore")
            source["scheduled_start_time_utc"] = pd.to_datetime(
                source.scheduled_start_time_utc, utc=True, errors="raise"
            )
            history = history.merge(
                source,
                on=["canonical_season", "game_id", "game_type_code",
                    "scheduled_start_time_utc", "home_team_id", "away_team_id"],
                how="inner", validate="one_to_one",
            )
            if history.empty:
                raise ValueError("CROSS_MARKET_OFFICIAL_OUTCOME_EVIDENCE_MISSING")
        if not history.empty:
            history["score_home_team_id"] = history.home_team_id
            history["score_away_team_id"] = history.away_team_id
            validate_history_score_source(history, schedule)
    else:
        # Before the first regular-season final, the strict-prior query is
        # legitimately empty. Keep its typed schema compatible with the V2
        # predictor instead of treating an empty history as malformed input.
        for column in ("final_home_goals", "final_away_goals"):
            if column not in history:
                history[column] = pd.Series(dtype="float64")
    directory.mkdir(parents=True, exist_ok=False)
    schedule_path, history_path = directory / "schedule.csv", directory / "history.csv"
    schedule.to_csv(schedule_path, index=False)
    history.to_csv(history_path, index=False)
    return schedule_path, history_path, schedule


def phase_for(schedule: pd.DataFrame, now: datetime, requested: str, force: bool) -> tuple[str | None, str]:
    if schedule.empty:
        return None, "NO_CANONICAL_2026_GAMES"
    timing = first_start_diagnostics(schedule, now)
    if timing["first_start_time"] is None:
        return None, "NO_PRESTART_GAME"
    minutes = float(timing["minutes_until_first_start"])
    if requested != "AUTO":
        return (requested, "EXPLICIT_FORCE" if force else "EXPLICIT_PHASE")
    if 20 <= minutes <= 75:
        return "FINAL_PREGAME", f"AUTO_ALLOWED_FINAL_PREGAME_FIRST_START_IN_{minutes:.1f}_MINUTES"
    return "MIDDAY", f"AUTO_ALLOWED_NONBLOCKING_FIRST_START_IN_{minutes:.1f}_MINUTES"


def first_start_diagnostics(schedule: pd.DataFrame, now: datetime) -> dict[str, object]:
    """Return the next scheduled start and lead time for audit metadata only."""
    if schedule.empty or "scheduled_start_time_utc" not in schedule:
        return {"first_start_time": None, "minutes_until_first_start": None}
    starts = pd.to_datetime(schedule.scheduled_start_time_utc, utc=True, errors="coerce")
    future = starts[starts > pd.Timestamp(now)]
    if future.empty:
        return {"first_start_time": None, "minutes_until_first_start": None}
    first = future.min()
    return {
        "first_start_time": first.isoformat().replace("+00:00", "Z"),
        "minutes_until_first_start": round((first - pd.Timestamp(now)).total_seconds() / 60, 1),
    }


def auto_capture_readiness_decision(
    slate: str, schedule: pd.DataFrame, now: datetime,
) -> dict[str, object]:
    """Report schedule-window eligibility without claiming source readiness.

    Mainline source, history, model, and market checks run in ``observe`` and
    ``run_capture``. A daily parent receipt is not an input to this producer.
    """
    decision: dict[str, object] = {
        "slate_date": slate, "requested_phase": "AUTO",
        "mainline_input_validation": "DEFERRED_TO_OBSERVER",
        "capture_window": "NONBLOCKING",
        "canonical_games": int(len(schedule)),
        **first_start_diagnostics(schedule, now),
        "decision": "BLOCKED",
    }
    if schedule.empty:
        decision["gate_reason"] = "NO_CANONICAL_2026_GAMES"
        return decision
    phase, reason = phase_for(schedule, now, "AUTO", False)
    if phase is None:
        decision["gate_reason"] = reason
        return decision
    decision.update(decision="SCHEDULE_WINDOW_ELIGIBLE", resolved_phase=phase, gate_reason=reason)
    return decision


def prior_capture_suppression(
    phase: str, *, existing: bool, prior_claim: bool, prior_paid: bool, force: bool,
) -> str | None:
    """Scheduler phases are idempotent; explicit REFRESH is append-only."""
    if phase == "REFRESH" or force:
        return None
    if existing:
        return "NOOP_ALREADY_CAPTURED"
    if prior_claim or prior_paid:
        return "NOOP_PAID_ATTEMPT_ALREADY_EXISTS"
    return None


@contextlib.contextmanager
def acquisition_lock(root: Path, slate: str):
    """Cover eligibility, durable claims, HTTP and publication, not just scoring."""
    path = root / ".locks" / f"{slate}.acquisition.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError("CROSS_MARKET_ACQUISITION_ALREADY_RUNNING") from error
        yield


def durable_json(path: Path, payload: dict, *, create_only: bool = False) -> None:
    """Fsync both bytes and directory before permitting a paid request."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        if create_only:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        else:
            fd, raw = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
            temporary = Path(raw)
        with os.fdopen(fd, "w") as handle:
            handle.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        if temporary is not None:
            os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def resolve_slate_date(requested: str, now: datetime) -> str:
    """Resolve the operator's `today` against the NHL ET calendar boundary."""
    if requested == "today":
        return now.astimezone(ZoneInfo("America/New_York")).date().isoformat()
    return requested


def observe(root: Path, slate: str, requested: str, force: bool, dsn: str,
            canonical_raw_path: Path | None = None,
            canonical_health_path: Path | None = None,
            player_prop_observers: dict[str, dict[str, object]] | None = None) -> Path:
    """One WARN-only entry; --force is the existing explicit operator override.

    Claims are immutable request intents, not assertions that a charge occurred.
    A crash even before HTTP deliberately requires operator review before retry.
    """
    stamp = utc_now().strftime("%Y%m%dT%H%M%S.%fZ") + "_" + uuid.uuid4().hex
    status_dir = root / "orchestration_status" / slate
    status_path = status_dir / f"capture_{stamp}.json"
    result = {"slate_date": slate, "warning_only": True,
              "requested_phase": requested,
              "capture_window": "NONBLOCKING" if requested == "AUTO" else "EXPLICIT_PHASE",
              "player_prop_observers": {
                  "schema_version": "NHL_PLAYER_PROP_OBSERVER_LINKS_V1",
                  **(player_prop_observers or {}),
              },
              "player_prop_execution_allowed": True, "historical_odds_calls": 0,
              "live_calls": 0, "live_credits_consumed": 0,
              **observer_provenance(Path(__file__))}
    with acquisition_lock(root, slate):
        claim = None
        try:
            if not dsn:
                raise RuntimeError("SUPABASE_DB_URL_MISSING")
            result["prior_day_outcome_handoff"] = ensure_prior_day_official_outcomes(
                dsn, slate,
            )
            result["prior_day_reconciliation_certificate"] = (
                require_slate_history_certificate(slate)
            )
            result["current_slate_roster_observation"] = require_slate_roster_observation(slate)
            # Moneyline/puck line have no per-game roster exclusion surface, so a
            # partial roster package blocks them with a distinct reason (P2.13).
            require_full_roster_coverage(result["current_slate_roster_observation"])
            input_dir = root / "runtime_inputs" / slate / stamp
            if (canonical_raw_path is None) != (canonical_health_path is None):
                raise ValueError("BOTH_CANONICAL_SLATE_PATHS_REQUIRED")
            canonical_schedule = None
            if canonical_raw_path is not None and canonical_health_path is not None:
                health = json.loads(canonical_health_path.read_text())
                canonical_games = load_canonical_slate(
                    slate_date=slate, raw_schedule_path=canonical_raw_path,
                    slate_health_path=canonical_health_path,
                )
                canonical_schedule = adapt_canonical_slate(
                    canonical_games, json.loads(canonical_raw_path.read_text()),
                    slate_date=slate, canonical_season=int(health["canonical_season"]),
                )
            schedule_path, history_path, schedule = export_inputs(
                dsn, slate, input_dir, canonical_schedule=canonical_schedule,
            )
            timing = first_start_diagnostics(schedule, utc_now())
            phase, reason = phase_for(schedule, utc_now(), requested, force)
            result.update(canonical_games=len(schedule), phase=phase, gate_reason=reason, **timing)
            claims_dir = root / "paid_attempt_claims" / slate
            if phase is None:
                result["status"] = "NOOP_READY"
            else:
                existing = list((root / f"season={authorized_season_for_date(slate)}" / f"slate_date={slate}" /
                                 f"run_type={phase}").glob("state=*"))
                # File existence is sufficient: malformed/partial claims fail closed.
                prior_claims = list(claims_dir.glob(f"{phase}_*.claim.json"))
                prior_paid = []
                for path in sorted(status_dir.glob("capture_*.json")):
                    prior = json.loads(path.read_text())
                    if prior.get("phase") == phase and int(prior.get("live_calls", 0)) > 0:
                        prior_paid.append(path)
                suppression = prior_capture_suppression(
                    phase, existing=bool(existing), prior_claim=bool(prior_claims),
                    prior_paid=bool(prior_paid), force=force,
                )
                if suppression == "NOOP_ALREADY_CAPTURED":
                    result.update(status=suppression, existing_states=len(existing))
                elif suppression == "NOOP_PAID_ATTEMPT_ALREADY_EXISTS":
                    result.update(status=suppression, operator_review_required_for_retry=True)
                else:
                    claim = claims_dir / f"{phase}_{stamp}.claim.json"
                    durable_json(claim, {"contract_version": "NHL_PAID_ATTEMPT_CLAIM_V1",
                                 "slate_date": slate, "phase": phase,
                                 "claimed_at_utc": utc_now().isoformat(),
                                 "explicit_operator_override": force,
                                 "status": "REQUEST_INTENT_RETRY_BLOCKED",
                                 "status_path": str(status_path)}, create_only=True)
                    result.update(paid_attempt_claim=str(claim), request_attempted=True)
                    # Persist intent before HTTP; actual charged credits may be unknown.
                    result.update(status="REQUEST_ATTEMPT_IN_PROGRESS", charge_state="UNKNOWN")
                    durable_json(status_path, result)
                    result["live_calls"] = 1
                    odds_path = input_dir / "the_odds_api_live_envelope.json"
                    fetch_markets(os.environ.get("ODDS_API_KEY", "").strip(), odds_path)
                    envelope = json.loads(odds_path.read_text())
                    result.update(status="REQUEST_RESPONSE_PRESERVED", charge_state="RESPONSE_RECORDED",
                                  capture_timestamp_utc=envelope.get("capture_timestamp_utc"),
                                  live_credits_consumed=int(envelope["quota"]["credits_consumed"]),
                                  ending_requests_remaining=envelope["quota"].get("requests_remaining"))
                    durable_json(status_path, result)
                    run = run_capture(schedule_path, history_path, odds_path, root, slate,
                                      envelope["capture_timestamp_utc"], phase,
                                      canary_mode=PRESEASON_START <= slate < REGULAR_SEASON_START,
                                      readiness_evidence={
                                          "prior_day_reconciliation_certificate": result[
                                              "prior_day_reconciliation_certificate"],
                                          "current_slate_roster_observation": result[
                                              "current_slate_roster_observation"],
                                      })
                    result.update(status="CAPTURED", run_dir=str(run))
        except PriorDayChildFailure as error:
            result.update(
                status="FAILED_WARN_ONLY",
                failure=f"{type(error).__name__}:{error}",
                prior_day_child_failure=error.diagnostics,
            )
            if claim is not None:
                result["operator_review_required_for_retry"] = True
        except RosterObservationNotReady as error:
            result.update(
                status="ROSTER_NOT_READY",
                readiness_reason=str(error),
                retry_eligible_on_later_poll=True,
                feature_export_started=False,
                request_attempted=False,
            )
        except Exception as error:
            result.update(status="FAILED_WARN_ONLY", failure=f"{type(error).__name__}:{error}")
            if claim is not None:
                result["operator_review_required_for_retry"] = True
        durable_json(status_path, result)
    return status_path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--slate-date", default="today")
    parser.add_argument("--phase", choices=["AUTO", "MIDDAY", "FINAL_PREGAME", "REFRESH"], default="AUTO")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--env-file", type=Path, default=ROOT / "backend/.env")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--canonical-slate-raw", type=Path)
    parser.add_argument("--canonical-slate-health", type=Path)
    args = parser.parse_args()
    load_env(args.env_file)
    now = utc_now()
    slate = resolve_slate_date(args.slate_date, now)
    try:
        # Odds-independent prediction lanes remain isolated from mainline source readiness.
        # Their immutable status records remain lane-local and never authorize a
        # request, candidate, upload, execution, or retry.
        # Prediction-only observers retain their scheduler phases; REFRESH is
        # a market-snapshot mode, not a new scoring/prediction phase.
        observer_phase = "AUTO" if args.phase == "REFRESH" else args.phase
        player_prop_observers = observe_prediction_only_lanes(
            slate, observer_phase, database_url("research"), now,
        )
        status_path = observe(args.output_root, slate, args.phase, args.force,
                              database_url("research"),
                              args.canonical_slate_raw, args.canonical_slate_health,
                              player_prop_observers=player_prop_observers)
        print(status_path)
    except Exception as error:
        # Lock/claim/status failures never permit acquisition; remain WARN-only.
        print(json.dumps({"status": "FAILED_WARN_ONLY", "failure":
                          f"{type(error).__name__}:{error}",
                          "live_calls": None, "charge_state": "CONSULT_DURABLE_ATTEMPT_CLAIMS"}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
