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

from backend.nhl.cross_market_shadow.core import PRESEASON_START, REGULAR_SEASON_START, fetch_markets, run_capture
from backend.nhl.cross_market_shadow.canonical_slate_adapter import adapt_canonical_slate
from backend.nhl.cross_market_shadow.official_outcomes import load_official_outcomes
from backend.nhl.daily_capture import load_canonical_slate, sha256_file, verify_package
from backend.nhl.scripts.nhl_prediction_only_common import observe as observe_independent_prediction_only
from backend.nhl.scripts.run_nhl_sog_prediction_only_warn_only import observe as observe_sog_prediction_only
from backend.nhl.scripts.nhl_observer_provenance import observer_provenance


ROOT = Path(__file__).resolve().parents[3]
DEFAULT_ROOT = ROOT / "artifacts/operational/nhl/cross_market_shadow"
DEFAULT_OUTCOME_ROOT = ROOT / "artifacts/operational/nhl/postgame_reconciliation"
MORNING_ROOT = ROOT / "artifacts/operational/nhl/morning"
DAILY_RUN_ROOT = ROOT / "artifacts/operational/nhl/daily_runs"


def observe_prediction_only_lanes(slate: str, requested: str, dsn: str,
                                  now: datetime) -> dict[str, str]:
    """Run independent lane snapshots before consulting any paid-market gate."""
    results: dict[str, str] = {}
    for lane in ("POINTS", "SAVES"):
        try:
            path = observe_independent_prediction_only(
                lane=lane, season=2026, slate_date=slate, requested_phase=requested,
                observation_timestamp=now, canonical_run_identifier=None, dsn=dsn,
                output_root=ROOT / "artifacts/operational/nhl" / f"{lane.lower()}_prediction_only",
            )
            results[lane] = str(path)
        except Exception as error:
            # Each publisher normally emits its own FAILED_WARN_ONLY status. This
            # boundary additionally prevents an unexpected lane defect from
            # suppressing another prediction lane or changing paid-market flow.
            results[lane] = f"FAILED_WARN_ONLY:{type(error).__name__}:{error}"
    try:
        path = observe_sog_prediction_only(
            slate=slate, requested=requested, dsn=dsn,
            root=ROOT / "artifacts/operational/nhl/sog_prediction_only", now=now,
        )
        results["SOG"] = str(path)
    except Exception as error:
        results["SOG"] = f"FAILED_WARN_ONLY:{type(error).__name__}:{error}"
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
    if observed.max() >= target_starts.min():
        raise ValueError("CROSS_MARKET_FINAL_SCORE_NOT_OBSERVED_BEFORE_TARGET")
    for side in ("home", "away"):
        expected = pd.to_numeric(history[f"{side}_team_id"], errors="coerce")
        outcome = pd.to_numeric(history[f"score_{side}_team_id"], errors="coerce")
        if expected.isna().any() or outcome.isna().any() or not expected.eq(outcome).all():
            raise ValueError(f"CROSS_MARKET_FINAL_SCORE_{side.upper()}_IDENTITY_MISMATCH")
    scheduled = pd.to_datetime(history.scheduled_start_time_utc, utc=True, errors="coerce")
    target_start = pd.to_datetime(schedule.scheduled_start_time_utc, utc=True, errors="coerce").min()
    if scheduled.isna().any() or not scheduled.lt(target_start).all():
        raise ValueError("CROSS_MARKET_FINAL_SCORE_NOT_STRICT_PRIOR")


def _prior_day_regular_season_game_ids(dsn: str, prior_slate: str) -> list[int]:
    """Return canonical regular-season game IDs for one operational prior day."""
    with psycopg.connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT game_id
            FROM nhl.games
            WHERE season = 2026 AND game_type = 2 AND game_date = %s::date
            ORDER BY game_id
            """,
            (prior_slate,),
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
        return {"status": "NO_PRIOR_DAY_REGULAR_SEASON_GAMES", "slate_date": prior_slate}

    day_root = outcome_root / prior_slate
    packages = sorted(day_root.glob("reconciliation=*") if day_root.is_dir() else [])
    if packages:
        outcomes = load_official_outcomes(outcome_root)
        observed = set(pd.to_numeric(outcomes.game_id, errors="coerce").dropna().astype(int))
        missing = sorted(set(game_ids) - observed)
        if missing:
            raise ValueError(
                "CROSS_MARKET_PRIOR_DAY_RECONCILIATION_INCOMPLETE:"
                + ",".join(map(str, missing))
            )
        return {"status": "EXISTING_GOVERNED_PACKAGE_VALID", "slate_date": prior_slate,
                "game_ids": game_ids}

    base = [sys.executable, "-m", "backend.nhl.scripts.run_nhl_postgame_reconciliation"]
    acquisition = subprocess.run(
        [*base, "--authority-roster-acquisition", prior_slate], cwd=str(ROOT),
        check=True, text=True, capture_output=True, env=os.environ.copy(),
    )
    acquisition_payload = _parse_json_output(
        acquisition.stdout, expected_status="COMPLETE")
    source_id = str(acquisition_payload.get("run_id") or "")
    if not source_id or set(map(int, acquisition_payload.get("game_ids") or [])) != set(game_ids):
        raise RuntimeError("CROSS_MARKET_PRIOR_DAY_ACQUISITION_GAME_SET_MISMATCH")
    sources = [
        "--response-source", f"AUTHORITY_RESPONSE_SOURCE={source_id}",
        "--response-source", f"ROSTER_RESPONSE_SOURCE={source_id}",
    ]
    local = subprocess.run(
        [*base, "--local-input-preflight", prior_slate, *sources], cwd=str(ROOT),
        check=True, text=True, capture_output=True, env=os.environ.copy(),
    )
    _parse_json_output(local.stdout, expected_status="LOCAL_INPUTS_VALID")
    identity = subprocess.run(
        [*base, "--database-identity-preflight", prior_slate, *sources], cwd=str(ROOT),
        check=True, text=True, capture_output=True, env=os.environ.copy(),
    )
    identity_payload = _parse_json_output(
        identity.stdout, expected_status="DATABASE_IDENTITY_PREFLIGHT_VALID")
    partition = identity_payload.get("database_preflight") or {}
    classification = partition.get("classification") or {}
    if (partition.get("authorized_new_official_lookup_ids")
            or classification.get("new_official_lookup")
            or classification.get("conflict")):
        raise RuntimeError(
            "CROSS_MARKET_PRIOR_DAY_PLAYER_IDENTITY_AUTHORIZATION_REQUIRED"
        )

    subprocess.run(
        [*base, "--execute", prior_slate, *sources], cwd=str(ROOT),
        check=True, text=True, capture_output=True, env=os.environ.copy(),
    )
    outcomes = load_official_outcomes(outcome_root)
    observed = set(pd.to_numeric(outcomes.game_id, errors="coerce").dropna().astype(int))
    missing = sorted(set(game_ids) - observed)
    if missing:
        raise RuntimeError(
            "CROSS_MARKET_PRIOR_DAY_RECONCILIATION_OUTPUT_INCOMPLETE:"
            + ",".join(map(str, missing))
        )
    return {"status": "GOVERNED_RECONCILIATION_COMPLETE", "slate_date": prior_slate,
            "game_ids": game_ids, "acquisition_run_id": source_id}


def export_inputs(dsn: str, slate_date: str, directory: Path, *,
                  canonical_schedule: pd.DataFrame | None = None,
                  outcome_root: Path = DEFAULT_OUTCOME_ROOT) -> tuple[Path, Path, pd.DataFrame]:
    with psycopg.connect(dsn) as connection:
        if canonical_schedule is None:
            schedule = pd.read_sql_query("""
                SELECT season AS canonical_season, game_date::text AS slate_date, game_id,
                       game_date::text AS game_date, start_time_utc AS scheduled_start_time_utc,
                       home_team_id, home_team_code AS home_team, away_team_id,
                       away_team_code AS away_team, upper(coalesce(status,'SCHEDULED')) AS game_status,
                       game_type AS game_type_code
                FROM nhl.games WHERE season=2026 AND game_date=%s::date
                ORDER BY start_time_utc,game_id
            """, connection, params=(slate_date,))
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
              WHERE g.season=2026 AND g.game_type=2 AND g.start_time_utc < %s::timestamptz
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
        """, connection, params=(history_cutoff,))
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
    *, daily_run_root: Path = DAILY_RUN_ROOT,
) -> dict[str, object]:
    """Read-only readiness rehearsal for an AUTO capture; never acquires markets."""
    ready, receipt_reason = morning_capture_allowed(slate, daily_run_root=daily_run_root)
    decision: dict[str, object] = {
        "slate_date": slate, "requested_phase": "AUTO",
        "daily_readiness": "PASS" if ready else "BLOCKED",
        "prior_day_learning": "BLOCKED", "capture_window": "NONBLOCKING",
        "canonical_games": int(len(schedule)),
        **first_start_diagnostics(schedule, now),
        "decision": "BLOCKED",
    }
    if not ready:
        decision["gate_reason"] = receipt_reason
        return decision
    if schedule.empty:
        decision["gate_reason"] = "NO_CANONICAL_2026_GAMES"
        return decision
    run_id = receipt_reason.removeprefix("DAILY_READY_RECEIPT:")
    try:
        receipt = json.loads((Path(daily_run_root) / f"run_id={run_id}" / "parent_receipt.json").read_text())
        learning = receipt.get("prior_day_learning") or {}
        reconciliation = Path(str(learning.get("reconciliation_package", "")))
        learning_ok = (
            learning.get("official_outcomes_status") == "FINAL"
            and learning.get("reconciliation_status") in {"CREATED", "EXISTING_GOVERNED_PACKAGE_VALID"}
            and learning.get("moneyline_grade_status") == "COMPLETE"
            and learning.get("puck_line_grade_status") == "COMPLETE"
            and reconciliation.is_dir()
            and (reconciliation / "RUN_COMPLETE.json").is_file()
            and (reconciliation / "SHA256SUMS").is_file()
        )
        if learning_ok:
            from backend.nhl.postgame_reconcile.core import _verify_manifest
            _verify_manifest(reconciliation)
            learning_ok = json.loads(
                (reconciliation / "RUN_COMPLETE.json").read_text()
            ).get("status") == "COMPLETE"
    except (OSError, ValueError, TypeError, RuntimeError):
        learning_ok = False
    decision["prior_day_learning"] = "PASS" if learning_ok else "BLOCKED"
    if not learning_ok:
        decision["gate_reason"] = "PRIOR_DAY_LEARNING_HANDOFF_INVALID"
        return decision
    phase, reason = phase_for(schedule, now, "AUTO", False)
    if phase is None:
        decision["gate_reason"] = reason
        return decision
    decision.update(decision="LIVE_CAPTURE_ALLOWED", resolved_phase=phase, gate_reason=reason)
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


def _daily_receipt_is_capture_ready(package: Path, slate: str) -> tuple[bool, str, str | None]:
    """Validate a completed daily receipt as the current-slate market readiness authority."""
    try:
        verify_package(package)
        receipt_path = package / "parent_receipt.json"
        marker_path = package / "RUN_COMPLETE.json"
        receipt = json.loads(receipt_path.read_text())
        marker = json.loads(marker_path.read_text())
        run_id = str(receipt.get("parent_daily_run_id") or "")
        if (receipt.get("schema_version") != "NHL_COMPREHENSIVE_DAILY_RUN_RECEIPT_V3"
                or marker.get("schema_version") != receipt.get("schema_version")
                or marker.get("parent_daily_run_id") != run_id
                or marker.get("final_classification") != "READY"
                or receipt.get("final_classification") != "READY"
                or receipt.get("operational_timezone") != "America/New_York"
                or receipt.get("slate_date") != slate
                or not run_id
                or package.name != f"run_id={run_id}"):
            return False, "DAILY_RECEIPT_IDENTITY_OR_CLASSIFICATION_INVALID", None
        lanes = receipt.get("lanes") or {}
        required_lanes = {
            "shared_prerequisites": {"COMPLETE"},
            "roster": {"COMPLETE", "REUSED"},
            "legacy_sog": {"COMPLETE"},
            "points": {"COMPLETE"},
            "saves": {"COMPLETE"},
        }
        for lane, statuses in required_lanes.items():
            if (not isinstance(lanes.get(lane), dict)
                    or lanes[lane].get("status") not in statuses):
                return False, f"DAILY_REQUIRED_LANE_NOT_READY:{lane}", None
        predictions = {
            "legacy_sog": "sog_predictions_wide_calibrated.csv",
            "points": "points_predictions.csv",
            "saves": "saves_predictions.csv",
        }
        for lane, filename in predictions.items():
            lane_outputs = lanes[lane].get("outputs") or []
            candidates = [row for row in lane_outputs if isinstance(row, dict)
                          and Path(str(row.get("path", ""))).name == filename]
            if not candidates:
                return False, f"DAILY_REQUIRED_PREDICTION_MISSING:{lane}", None
            artifact = candidates[0]
            artifact_path = Path(str(artifact.get("path", "")))
            if (not artifact_path.is_file()
                    or artifact_path.stat().st_size != int(artifact.get("bytes", -1))
                    or sha256_file(artifact_path) != artifact.get("sha256")):
                return False, f"DAILY_REQUIRED_PREDICTION_INVALID:{lane}", None
            if lane in {"points", "saves"} and (
                    artifact.get("parent_daily_run_id") != run_id
                    or artifact.get("canonical_game_set_hash") != receipt.get("canonical_game_set_hash")
                    or int(artifact.get("canonical_game_count", -1)) != len(receipt.get("canonical_game_ids") or [])):
                return False, f"DAILY_REQUIRED_PREDICTION_LINEAGE_INVALID:{lane}", None
        odds = receipt.get("odds_observation") or {}
        odds_dir = Path(str(odds.get("path", "")))
        if (odds.get("classification") not in {"CAPTURED_EMPTY", "CAPTURED_NONEMPTY"}
                or not odds_dir.is_dir()
                or verify_package(odds_dir) != odds.get("manifest_sha256")):
            return False, "DAILY_ODDS_OBSERVATION_INVALID", None
        if marker.get("completed_at_utc") is None:
            return False, "DAILY_COMPLETION_TIMESTAMP_MISSING", None
        return True, f"DAILY_READY_RECEIPT:{run_id}", marker["completed_at_utc"]
    except (OSError, ValueError, TypeError, KeyError, RuntimeError):
        return False, "DAILY_RECEIPT_INTEGRITY_INVALID", None


def morning_capture_allowed(
    slate: str, morning_root: Path | None = None, *, daily_run_root: Path = DAILY_RUN_ROOT,
) -> tuple[bool, str]:
    """Use newest valid, completed same-ET-slate daily receipt for readiness."""
    # morning_root remains an accepted argument for call compatibility. The
    # comprehensive daily receipt supersedes the redundant legacy health file.
    del morning_root
    candidates: list[tuple[str, Path]] = []
    for package in Path(daily_run_root).glob("run_id=*"):
        ready, reason, completed = _daily_receipt_is_capture_ready(package, slate)
        if ready and completed:
            candidates.append((completed, package))
    if candidates:
        _, selected = max(candidates, key=lambda item: item[0])
        receipt = json.loads((selected / "parent_receipt.json").read_text())
        return True, f"DAILY_READY_RECEIPT:{receipt['parent_daily_run_id']}"
    return False, "MORNING_READINESS_RECEIPT_ABSENT_OR_BLOCKED"


def resolve_slate_date(requested: str, now: datetime) -> str:
    """Resolve the operator's `today` against the NHL ET calendar boundary."""
    if requested == "today":
        return now.astimezone(ZoneInfo("America/New_York")).date().isoformat()
    return requested


def record_morning_not_ready(root: Path, slate: str, reason: str) -> Path:
    stamp = utc_now().strftime("%Y%m%dT%H%M%S.%fZ") + "_" + uuid.uuid4().hex
    status = root / "orchestration_status" / slate / f"capture_{stamp}.json"
    with acquisition_lock(root, slate):
        durable_json(status, {
            "slate_date": slate, "warning_only": True, "phase": None,
            "status": "NOOP_MORNING_NOT_READY", "gate_reason": reason,
            "historical_odds_calls": 0, "live_calls": 0,
            "live_credits_consumed": 0, "player_prop_execution_allowed": False,
            **observer_provenance(Path(__file__)),
        })
    return status


def observe(root: Path, slate: str, requested: str, force: bool, dsn: str,
            canonical_raw_path: Path | None = None,
            canonical_health_path: Path | None = None) -> Path:
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
                existing = list((root / "season=2026" / f"slate_date={slate}" /
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
                                      canary_mode=PRESEASON_START <= slate < REGULAR_SEASON_START)
                    result.update(status="CAPTURED", run_dir=str(run))
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
        # All odds-independent predictions run before the market-readiness gate.
        # Their immutable status records remain lane-local and never authorize a
        # request, candidate, upload, execution, or retry.
        # Prediction-only observers retain their scheduler phases; REFRESH is
        # a market-snapshot mode, not a new scoring/prediction phase.
        observer_phase = "AUTO" if args.phase == "REFRESH" else args.phase
        observe_prediction_only_lanes(
            slate, observer_phase, os.environ.get("SUPABASE_DB_URL", "").strip(), now,
        )
        ready, reason = morning_capture_allowed(slate)
        if not ready:
            status_path = record_morning_not_ready(args.output_root, slate, reason)
        else:
            status_path = observe(args.output_root, slate, args.phase, args.force,
                                  os.environ.get("SUPABASE_DB_URL", "").strip(),
                                  args.canonical_slate_raw, args.canonical_slate_health)
        print(status_path)
    except Exception as error:
        # Lock/claim/status failures never permit acquisition; remain WARN-only.
        print(json.dumps({"status": "FAILED_WARN_ONLY", "failure":
                          f"{type(error).__name__}:{error}",
                          "live_calls": None, "charge_state": "CONSULT_DURABLE_ATTEMPT_CLAIMS"}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
