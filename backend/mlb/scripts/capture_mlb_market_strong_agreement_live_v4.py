#!/usr/bin/env python3
"""Isolated live h2h capture for the frozen MLB agreement-separation study v4."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from backend.app.deps import pg_connect
from backend.mlb.markets import agreement_phase_gating_v1 as phase_gate
from backend.mlb.season_transition.game_phase_authority_v1 import CanonicalGamePhaseAuthority
from backend.mlb.scripts import acquire_and_audit_mlb_oddsapi_historical_joint_strength_transfer_v1 as hardened
from backend.mlb.scripts import run_mlb_market_strong_agreement_separation_prospective_v1 as v1


ROOT = Path(__file__).resolve().parents[3]
STUDY_ID = "MLB_MARKET_STRONG_AGREEMENT_SEPARATION_PROSPECTIVE_V4"
FREEZE = ROOT / ("artifacts/analysis/model_development/"
    "mlb_market_strong_agreement_separation_live_capture_v4/2026-09-09/pre_outcome_freeze_v4.json")
LEDGER = v1.LEDGER
RUNTIME = ROOT / "backend/mlb/exports/model_v2/mlb_market_strong_agreement_separation_live_capture_v4"
LIVE_URL = "https://api.the-odds-api.com/v4/sports/baseball_mlb/odds"
HISTORICAL_URL = hardened.URL
BOOKS = v1.BOOKS
START_DATE = "2026-09-10"
END_DATE = "2026-09-27"
FIRST_ELIGIBLE_LIVE_DATE = "2026-09-11"
MAX_START_LAG_SECONDS = 300
ROUTINE_CEILING = 18
RECOVERY_COUNT_CEILING = 3
RECOVERY_EXPECTED_COST = 10
TOTAL_CEILING = 48
QUOTA_HEADERS = hardened.QUOTA_HEADERS
RECOVERY_ELIGIBLE = frozenset({"TRANSPORT_ERROR", "HTTP_ERROR", "INVALID_RESPONSE"})


V4_SCHEMA = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS live_capture_authorization_v4 (
  study_id TEXT PRIMARY KEY, freeze_sha256 TEXT NOT NULL, start_date TEXT NOT NULL, end_date TEXT NOT NULL,
  routine_credit_ceiling INTEGER NOT NULL, recovery_count_ceiling INTEGER NOT NULL,
  recovery_expected_cost INTEGER NOT NULL, total_credit_ceiling INTEGER NOT NULL,
  frozen_bookmakers_json TEXT NOT NULL, created_at_utc TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS live_capture_claims_v4 (
  study_id TEXT NOT NULL, game_date TEXT NOT NULL, capture_mode TEXT NOT NULL,
  run_identity TEXT NOT NULL, prediction_barrier_utc TEXT NOT NULL,
  request_started_at_utc TEXT NOT NULL, claim_sha256 TEXT NOT NULL,
  PRIMARY KEY(study_id,game_date,capture_mode)
);
CREATE TABLE IF NOT EXISTS live_capture_events_v4 (
  event_id TEXT PRIMARY KEY, study_id TEXT NOT NULL, game_date TEXT NOT NULL,
  run_identity TEXT NOT NULL, capture_mode TEXT NOT NULL, status TEXT NOT NULL,
  recorded_at_utc TEXT NOT NULL, prediction_barrier_utc TEXT NOT NULL,
  request_started_at_utc TEXT, response_received_at_utc TEXT, requested_snapshot_utc TEXT,
  returned_snapshot_utc TEXT, http_status INTEGER, x_requests_last TEXT,
  x_requests_used TEXT, x_requests_remaining TEXT, raw_response_path TEXT,
  request_parameters_path TEXT, response_headers_path TEXT, raw_sha256 TEXT,
  error TEXT NOT NULL, event_sha256 TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS capture_provenance_v4 (
  game_key TEXT PRIMARY KEY, study_id TEXT NOT NULL, game_date TEXT NOT NULL,
  capture_mode TEXT NOT NULL, request_event_id TEXT NOT NULL,
  raw_response_sha256 TEXT NOT NULL, row_sha256 TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS live_capture_authorization_v4_no_update BEFORE UPDATE ON live_capture_authorization_v4 BEGIN SELECT RAISE(ABORT,'immutable authorization'); END;
CREATE TRIGGER IF NOT EXISTS live_capture_authorization_v4_no_delete BEFORE DELETE ON live_capture_authorization_v4 BEGIN SELECT RAISE(ABORT,'immutable authorization'); END;
CREATE TRIGGER IF NOT EXISTS live_capture_claims_v4_no_update BEFORE UPDATE ON live_capture_claims_v4 BEGIN SELECT RAISE(ABORT,'append-only claims'); END;
CREATE TRIGGER IF NOT EXISTS live_capture_claims_v4_no_delete BEFORE DELETE ON live_capture_claims_v4 BEGIN SELECT RAISE(ABORT,'append-only claims'); END;
CREATE TRIGGER IF NOT EXISTS live_capture_events_v4_no_update BEFORE UPDATE ON live_capture_events_v4 BEGIN SELECT RAISE(ABORT,'append-only events'); END;
CREATE TRIGGER IF NOT EXISTS live_capture_events_v4_no_delete BEFORE DELETE ON live_capture_events_v4 BEGIN SELECT RAISE(ABORT,'append-only events'); END;
CREATE TRIGGER IF NOT EXISTS capture_provenance_v4_no_update BEFORE UPDATE ON capture_provenance_v4 BEGIN SELECT RAISE(ABORT,'append-only provenance'); END;
CREATE TRIGGER IF NOT EXISTS capture_provenance_v4_no_delete BEFORE DELETE ON capture_provenance_v4 BEGIN SELECT RAISE(ABORT,'append-only provenance'); END;
"""


@dataclass(frozen=True)
class PredictionRow:
    game_date: str
    game_id: int
    scheduled_start_utc: str
    prediction_timestamp_utc: str
    prediction_cutoff_utc: str
    home_team: str
    away_team: str
    home_win_probability: float
    away_win_probability: float
    payload_sha256: str
    model_version: str
    prediction_snapshot_class: str
    model_hash: str
    admission_status: str
    created_at_utc: str


@dataclass(frozen=True)
class PredictionSnapshot:
    game_date: str
    barrier_utc: str
    expected_rows: int
    rows: tuple[PredictionRow, ...]


class PredictionSnapshotSchemaError(RuntimeError):
    """The durable prediction query did not satisfy its frozen named-field contract."""


PREDICTION_ROW_FIELDS = frozenset({
    "game_date", "game_id", "scheduled_start_utc", "prediction_timestamp_utc",
    "prediction_cutoff_utc", "home_team", "away_team", "home_win_probability",
    "away_win_probability", "payload_sha256", "model_version",
    "prediction_snapshot_class", "model_hash", "admission_status", "created_at",
})
SHA256 = re.compile(r"^[0-9a-f]{64}$")


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def digest(value: Any) -> str:
    return hashlib.sha256(stable_json(value).encode()).hexdigest()


def file_sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def atomic_new_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def schema(conn: sqlite3.Connection) -> None:
    v1.schema(conn)
    conn.executescript(V4_SCHEMA)


def establish_authorization(conn: sqlite3.Connection, freeze_path: Path = FREEZE) -> None:
    freeze_hash = file_sha(freeze_path)
    frozen = stable_json(list(BOOKS))
    row = (STUDY_ID, freeze_hash, START_DATE, END_DATE, ROUTINE_CEILING,
           RECOVERY_COUNT_CEILING, RECOVERY_EXPECTED_COST, TOTAL_CEILING, frozen, now_utc())
    conn.execute("INSERT OR IGNORE INTO live_capture_authorization_v4 VALUES (?,?,?,?,?,?,?,?,?,?)", row)
    existing = conn.execute("""SELECT freeze_sha256,start_date,end_date,routine_credit_ceiling,
        recovery_count_ceiling,recovery_expected_cost,total_credit_ceiling,frozen_bookmakers_json
        FROM live_capture_authorization_v4 WHERE study_id=?""", (STUDY_ID,)).fetchone()
    if not existing or tuple(existing) != row[1:-1]:
        raise RuntimeError("Frozen v4 authorization does not match the immutable ledger authority")


def read_expected_rows(lifecycle_result: Path, game_date: str) -> int:
    payload = json.loads(lifecycle_result.read_text())
    if payload.get("mlb_date") != game_date:
        raise RuntimeError("Moneyline lifecycle result date does not match capture date")
    expected = payload.get("games_discovered")
    if not isinstance(expected, int) or expected < 0:
        raise RuntimeError("Moneyline lifecycle result lacks an exact games_discovered count")
    return expected


def _schema_error(row_number: int, detail: str) -> PredictionSnapshotSchemaError:
    return PredictionSnapshotSchemaError(
        f"Durable prediction snapshot schema error at row {row_number}: {detail}"
    )


def _timestamp_field(row: dict[str, Any], key: str, row_number: int) -> str:
    value = row[key]
    if value is None:
        raise _schema_error(row_number, f"required field {key!r} is null")
    if not isinstance(value, (str, datetime, pd.Timestamp)):
        raise _schema_error(row_number, f"field {key!r} is not a timestamp")
    try:
        parsed = pd.Timestamp(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise _schema_error(row_number, f"field {key!r} is malformed: {exc}") from None
    if pd.isna(parsed) or parsed.tzinfo is None:
        raise _schema_error(row_number, f"field {key!r} must be a timezone-aware timestamp")
    return parsed.tz_convert("UTC").isoformat().replace("+00:00", "Z")


def _string_field(row: dict[str, Any], key: str, row_number: int) -> str:
    value = row[key]
    if value is None:
        raise _schema_error(row_number, f"required field {key!r} is null")
    if not isinstance(value, str) or not value.strip():
        raise _schema_error(row_number, f"field {key!r} must be a non-empty string")
    return value


def _probability_field(row: dict[str, Any], key: str, row_number: int) -> float:
    value = row[key]
    if value is None:
        raise _schema_error(row_number, f"required field {key!r} is null")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _schema_error(row_number, f"field {key!r} must be numeric")
    result = float(value)
    if not math.isfinite(result) or not 0 < result < 1:
        raise _schema_error(row_number, f"field {key!r} must be finite and strictly between 0 and 1")
    return result


def prediction_snapshot_from_rows(
    game_date: str, expected_rows: int, raw_rows: tuple[Any, ...]
) -> PredictionSnapshot:
    """Validate the exact psycopg ``dict_row`` contract and normalize its values."""
    if not isinstance(expected_rows, int) or isinstance(expected_rows, bool) or expected_rows < 0:
        raise PredictionSnapshotSchemaError("Expected durable prediction row count must be a non-negative integer")
    if len(raw_rows) != expected_rows:
        raise RuntimeError(
            f"Durable commit barrier incomplete: expected {expected_rows} immutable rows; found {len(raw_rows)}"
        )
    if expected_rows == 0:
        return PredictionSnapshot(game_date, now_utc(), 0, tuple())

    parsed_rows: list[PredictionRow] = []
    identities: set[tuple[str, int, str, str]] = set()
    for row_number, raw in enumerate(raw_rows, 1):
        if not isinstance(raw, dict):
            raise _schema_error(
                row_number,
                f"expected a dictionary row from psycopg dict_row; got {type(raw).__name__}",
            )
        actual_fields = set(raw)
        missing = sorted(PREDICTION_ROW_FIELDS - actual_fields)
        unexpected = sorted(actual_fields - PREDICTION_ROW_FIELDS)
        if missing:
            raise _schema_error(row_number, f"missing required fields: {', '.join(missing)}")
        if unexpected:
            raise _schema_error(row_number, f"unexpected fields: {', '.join(unexpected)}")
        null_fields = sorted(key for key in PREDICTION_ROW_FIELDS if raw[key] is None)
        if null_fields:
            raise _schema_error(row_number, f"required field {null_fields[0]!r} is null")

        row_date = _string_field(raw, "game_date", row_number)
        try:
            parsed_date = datetime.strptime(row_date, "%Y-%m-%d").date().isoformat()
        except ValueError:
            raise _schema_error(row_number, "field 'game_date' must use YYYY-MM-DD") from None
        if parsed_date != row_date or row_date != game_date:
            raise _schema_error(row_number, f"unexpected game_date {row_date!r}; expected {game_date!r}")

        game_id = raw["game_id"]
        if type(game_id) is not int or game_id <= 0:
            raise _schema_error(row_number, "field 'game_id' must be a positive integer")
        model_version = _string_field(raw, "model_version", row_number)
        snapshot_class = _string_field(raw, "prediction_snapshot_class", row_number)
        model_hash = _string_field(raw, "model_hash", row_number)
        admission_status = _string_field(raw, "admission_status", row_number)
        if model_version != v1.MODEL:
            raise _schema_error(row_number, f"unexpected model_version {model_version!r}")
        if snapshot_class != v1.SNAPSHOT:
            raise _schema_error(row_number, f"unexpected prediction_snapshot_class {snapshot_class!r}")
        if model_hash != v1.MODEL_HASH:
            raise _schema_error(row_number, f"unexpected frozen model_hash {model_hash!r}")
        if admission_status != v1.ADMISSION:
            raise _schema_error(row_number, f"unexpected admission_status {admission_status!r}")

        payload_sha = _string_field(raw, "payload_sha256", row_number)
        if not SHA256.fullmatch(payload_sha):
            raise _schema_error(row_number, "field 'payload_sha256' must be a lowercase SHA-256 digest")
        home_team = _string_field(raw, "home_team", row_number)
        away_team = _string_field(raw, "away_team", row_number)
        if home_team == away_team:
            raise _schema_error(row_number, "home_team and away_team must differ")
        home_probability = _probability_field(raw, "home_win_probability", row_number)
        away_probability = _probability_field(raw, "away_win_probability", row_number)
        if not math.isclose(home_probability + away_probability, 1.0, abs_tol=1e-9):
            raise _schema_error(row_number, "home and away probabilities must sum to 1")

        scheduled = _timestamp_field(raw, "scheduled_start_utc", row_number)
        predicted = _timestamp_field(raw, "prediction_timestamp_utc", row_number)
        cutoff = _timestamp_field(raw, "prediction_cutoff_utc", row_number)
        created = _timestamp_field(raw, "created_at", row_number)
        if pd.Timestamp(predicted) >= pd.Timestamp(scheduled):
            raise _schema_error(row_number, "prediction_timestamp_utc must precede scheduled_start_utc")
        if pd.Timestamp(cutoff) > pd.Timestamp(predicted):
            raise _schema_error(row_number, "prediction_cutoff_utc must not follow prediction_timestamp_utc")
        if pd.Timestamp(created) < pd.Timestamp(predicted):
            raise _schema_error(row_number, "created_at must not precede prediction_timestamp_utc")

        identity = (row_date, game_id, model_version, snapshot_class)
        if identity in identities:
            raise _schema_error(row_number, f"duplicate immutable prediction key {identity!r}")
        identities.add(identity)
        parsed_rows.append(PredictionRow(
            game_date=row_date, game_id=game_id, scheduled_start_utc=scheduled,
            prediction_timestamp_utc=predicted, prediction_cutoff_utc=cutoff,
            home_team=home_team, away_team=away_team,
            home_win_probability=home_probability, away_win_probability=away_probability,
            payload_sha256=payload_sha, model_version=model_version,
            prediction_snapshot_class=snapshot_class, model_hash=model_hash,
            admission_status=admission_status, created_at_utc=created,
        ))
    barrier = max(row.created_at_utc for row in parsed_rows)
    return PredictionSnapshot(game_date, barrier, expected_rows, tuple(parsed_rows))


def snapshot_phase_rows(snapshot: PredictionSnapshot) -> tuple[dict[str, Any], ...]:
    """Expose only exact identity/date fields needed by the shared phase gate."""
    return tuple({"game_id": row.game_id, "game_date": row.game_date} for row in snapshot.rows)


def load_prediction_snapshot(game_date: str, expected_rows: int) -> PredictionSnapshot:
    sql = """
      SELECT game_date::text,game_id,scheduled_start_utc,prediction_timestamp_utc,prediction_cutoff_utc,
             home_team,away_team,home_win_probability,away_win_probability,payload_sha256,model_version,
             prediction_snapshot_class,model_hash,admission_status,created_at
      FROM mlb.public_game_moneyline_predictions
      WHERE game_date=%s AND model_version=%s AND prediction_snapshot_class=%s
      ORDER BY game_id,model_version,prediction_snapshot_class
    """
    with pg_connect() as source, source.cursor() as cursor:
        cursor.execute(sql, (game_date, v1.MODEL, v1.SNAPSHOT))
        rows = tuple(cursor.fetchall())
    return prediction_snapshot_from_rows(game_date, expected_rows, rows)


def ingest_snapshot_predictions(conn: sqlite3.Connection, snapshot: PredictionSnapshot) -> int:
    inserted = 0
    for row in snapshot.rows:
        if row.admission_status != v1.ADMISSION:
            continue
        home, away = row.home_win_probability, row.away_win_probability
        strong = "HOME" if home > v1.BOUNDARY else "AWAY" if away > v1.BOUNDARY else "NONE"
        values = {"game_key": f"MLB|{row.game_date}|{row.game_id}", "game_date": row.game_date,
                  "game_id": row.game_id, "scheduled_start_utc": row.scheduled_start_utc,
                  "prediction_timestamp_utc": row.prediction_timestamp_utc,
                  "prediction_cutoff_utc": row.prediction_cutoff_utc,
                  "home_team": row.home_team, "away_team": row.away_team, "home_model_probability": home,
                  "away_model_probability": away, "model_strong_side": strong,
                  "prediction_payload_sha256": row.payload_sha256}
        values["row_sha256"] = digest(values)
        inserted += v1.immutable_insert(conn, "predictions", "game_key", values)
    return inserted


def terminal_events(conn: sqlite3.Connection, game_date: str, mode: str | None = None) -> list[sqlite3.Row]:
    where = "study_id=? AND game_date=?"
    params: list[Any] = [STUDY_ID, game_date]
    if mode:
        where += " AND capture_mode=?"
        params.append(mode)
    return conn.execute(f"SELECT * FROM live_capture_events_v4 WHERE {where} ORDER BY recorded_at_utc,event_id", params).fetchall()


def observed_cost(conn: sqlite3.Connection) -> int:
    total = 0
    claims = conn.execute("SELECT game_date,capture_mode FROM live_capture_claims_v4 WHERE study_id=?",
                          (STUDY_ID,)).fetchall()
    for game_date, mode in claims:
        row = conn.execute("""SELECT x_requests_last FROM live_capture_events_v4
            WHERE study_id=? AND game_date=? AND capture_mode=? AND x_requests_last<>''
            ORDER BY recorded_at_utc DESC,event_id DESC LIMIT 1""", (STUDY_ID, game_date, mode)).fetchone()
        if row:
            try:
                total += int(row[0])
                continue
            except (TypeError, ValueError):
                pass
        # Unknown transport charge states reserve the full frozen expected cost.
        total += 1 if mode == "LIVE" else RECOVERY_EXPECTED_COST
    return total


def request_spec(mode: str, target: str = "") -> tuple[str, dict[str, str], int]:
    """Build the secret-free request specification shared by live execution and preflight."""
    if mode not in {"LIVE", "HISTORICAL_RECOVERY"}:
        raise ValueError("unsupported capture mode")
    params = {"bookmakers": ",".join(BOOKS), "markets": "h2h",
              "oddsFormat": "american", "dateFormat": "iso"}
    if mode == "HISTORICAL_RECOVERY":
        if not target:
            raise ValueError("historical recovery requires an immutable target timestamp")
        params["date"] = target
        return HISTORICAL_URL, params, RECOVERY_EXPECTED_COST
    return LIVE_URL, params, 1


def validate_authorization_read_only(conn: sqlite3.Connection, freeze_path: Path) -> None:
    row = conn.execute("""SELECT freeze_sha256,start_date,end_date,routine_credit_ceiling,
        recovery_count_ceiling,recovery_expected_cost,total_credit_ceiling,frozen_bookmakers_json
        FROM live_capture_authorization_v4 WHERE study_id=?""", (STUDY_ID,)).fetchone()
    expected = (file_sha(freeze_path), START_DATE, END_DATE, ROUTINE_CEILING,
                RECOVERY_COUNT_CEILING, RECOVERY_EXPECTED_COST, TOTAL_CEILING,
                stable_json(list(BOOKS)))
    if not row or tuple(row) != expected:
        raise RuntimeError("Frozen v4 authorization does not match the immutable ledger authority")


def preflight_capture(*, game_date: str, run_identity: str, snapshot: PredictionSnapshot,
                      ledger: Path = LEDGER, runtime: Path = RUNTIME,
                      freeze_path: Path = FREEZE,
                      evaluation_phase: str = phase_gate.REGULAR_SEASON,
                      phase_authority: CanonicalGamePhaseAuthority | None = None) -> dict[str, Any]:
    """Reach the live request boundary without writes, claims, credentials, or network access."""
    if not START_DATE <= game_date <= END_DATE:
        raise ValueError("game date outside frozen regular-season capture horizon")
    if snapshot.game_date != game_date:
        raise ValueError("prediction snapshot date mismatch")
    if snapshot.expected_rows <= 0 or len(snapshot.rows) != snapshot.expected_rows:
        raise RuntimeError("Preflight requires a complete non-empty durable prediction snapshot")
    decisions = phase_gate.require_snapshot_membership(
        snapshot_phase_rows(snapshot), evaluation_phase,
        authority=phase_authority, require_freshness=True,
    )
    if not ledger.is_file():
        raise RuntimeError("V4 ledger is absent; read-only preflight cannot initialize it")

    uri = ledger.resolve().as_uri() + "?mode=ro"
    with sqlite3.connect(uri, uri=True, timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        validate_authorization_read_only(conn, freeze_path)
        prior_claims = int(conn.execute("""SELECT COUNT(*) FROM live_capture_claims_v4
            WHERE study_id=? AND game_date=? AND capture_mode='LIVE'""",
            (STUDY_ID, game_date)).fetchone()[0])
        prior_events = int(conn.execute("""SELECT COUNT(*) FROM live_capture_events_v4
            WHERE study_id=? AND game_date=? AND capture_mode='LIVE'""",
            (STUDY_ID, game_date)).fetchone()[0])
        if prior_claims or prior_events:
            raise RuntimeError("Read-only preflight found a prior live claim or event for the date")
        routine_claims = int(conn.execute("""SELECT COUNT(*) FROM live_capture_claims_v4
            WHERE study_id=? AND capture_mode='LIVE'""", (STUDY_ID,)).fetchone()[0])
        if routine_claims >= ROUTINE_CEILING:
            raise RuntimeError("Read-only preflight found the routine claim ceiling exhausted")
        unknown_cost = conn.execute("""SELECT 1 FROM live_capture_events_v4
            WHERE study_id=? AND status IN ('SUCCESS_RESPONSE_PRESERVED','HTTP_ERROR','INVALID_RESPONSE')
              AND (x_requests_last='' OR x_requests_last IS NULL) LIMIT 1""", (STUDY_ID,)).fetchone()
        if unknown_cost:
            raise RuntimeError("Read-only preflight found an unresolved request cost")
        prior_cost = observed_cost(conn)
        if prior_cost + 1 > TOTAL_CEILING:
            raise RuntimeError("Read-only preflight found the total credit ceiling exhausted")
        study_rows = {table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                      for table in ("predictions", "risk_set", "bookmaker_prices")}

    url, params, expected_cost = request_spec("LIVE")
    latest = (pd.Timestamp(snapshot.barrier_utc) + timedelta(seconds=MAX_START_LAG_SECONDS))
    raw_path, params_path, headers_path = paths(runtime, game_date, run_identity, "LIVE")
    prepared_paths = (raw_path, params_path, headers_path)
    if any(path.exists() for path in prepared_paths):
        raise RuntimeError("Read-only preflight found a pre-existing runtime request path")
    return {
        "status": "PREFLIGHT_REQUEST_BOUNDARY_REACHED",
        "diagnostic_only": True,
        "game_date": game_date,
        "first_eligible_prospective_date": FIRST_ELIGIBLE_LIVE_DATE,
        "live_date_is_operationally_eligible": game_date >= FIRST_ELIGIBLE_LIVE_DATE,
        "durable_prediction_rows": snapshot.expected_rows,
        "durable_prediction_barrier_utc": snapshot.barrier_utc,
        "validated_prediction_identities": [
            {"game_date": row.game_date, "game_id": row.game_id,
             "model_version": row.model_version,
             "prediction_snapshot_class": row.prediction_snapshot_class}
            for row in snapshot.rows
        ],
        "validated_frozen_model_hash": v1.MODEL_HASH,
        "evaluation_phase": evaluation_phase,
        "authoritative_game_types": sorted({decision.source_game_type for decision in decisions}),
        "postseason_rounds": sorted({decision.postseason_round for decision in decisions
                                      if decision.postseason_round}),
        "request_start_window_utc": [snapshot.barrier_utc,
            latest.isoformat().replace("+00:00", "Z")],
        "endpoint": url,
        "request_parameters_excluding_secret": params,
        "expected_request_cost": expected_cost,
        "observed_or_reserved_study_credits": prior_cost,
        "remaining_authorized_study_credits": TOTAL_CEILING - prior_cost,
        "prior_live_claims_for_date": prior_claims,
        "prior_live_events_for_date": prior_events,
        "prospective_ledger_rows": study_rows,
        "prepared_paths_not_created": [relative(path) for path in prepared_paths],
        "live_claim_created": False,
        "credits_reserved": False,
        "api_credential_read": False,
        "network_requests": 0,
        "outcome_data_accessed": False,
    }


def claim(conn: sqlite3.Connection, game_date: str, mode: str, run_identity: str,
          barrier: str, started: str) -> bool:
    payload = {"study_id": STUDY_ID, "game_date": game_date, "capture_mode": mode,
               "run_identity": run_identity, "prediction_barrier_utc": barrier,
               "request_started_at_utc": started}
    try:
        conn.execute("INSERT INTO live_capture_claims_v4 VALUES (?,?,?,?,?,?,?)",
                     (*payload.values(), digest(payload)))
        return True
    except sqlite3.IntegrityError:
        return False


def append_event(conn: sqlite3.Connection, **values: Any) -> str:
    defaults = {"event_id": str(uuid.uuid4()), "study_id": STUDY_ID, "game_date": "",
        "run_identity": "", "capture_mode": "", "status": "", "recorded_at_utc": now_utc(),
        "prediction_barrier_utc": "", "request_started_at_utc": "", "response_received_at_utc": "",
        "requested_snapshot_utc": "", "returned_snapshot_utc": "", "http_status": None,
        "x_requests_last": "", "x_requests_used": "", "x_requests_remaining": "",
        "raw_response_path": "", "request_parameters_path": "", "response_headers_path": "",
        "raw_sha256": "", "error": ""}
    defaults.update(values)
    defaults["error"] = hardened.redact_sensitive(defaults["error"])
    defaults["event_sha256"] = digest(defaults)
    columns = list(defaults)
    conn.execute(f"INSERT INTO live_capture_events_v4 ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
                 [defaults[column] for column in columns])
    return str(defaults["event_id"])


def relative(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:  # Test-only/custom runtime roots retain an explicit absolute path.
        return str(path)


def paths(runtime: Path, game_date: str, run_identity: str, mode: str) -> tuple[Path, Path, Path]:
    token = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in run_identity)
    base = runtime / "raw" / game_date / f"{mode.lower()}__{token}"
    return base.with_suffix(".json"), base.with_name(base.name + "__request.json"), base.with_name(base.name + "__headers.json")


def response_parts(response: Any) -> tuple[dict[str, str], int | None]:
    headers = hardened.header_dict(response)
    return headers, hardened.header_int(headers, "x-requests-last")


def live_classify(prediction: pd.Series | None, event: dict[str, Any] | None, request_started: str,
                  received: str, raw_sha: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    risk, prices = v1.classify_risk(prediction, event, received, received, raw_sha)
    risk["requested_timestamp_utc"] = request_started
    risk["row_sha256"] = digest({key: value for key, value in risk.items() if key != "row_sha256"})
    return risk, prices


def ingest_events(conn: sqlite3.Connection, game_date: str, events: list[dict[str, Any]],
                  request_started: str, returned: str, raw_sha: str, mode: str,
                  request_event_id: str, *,
                  evaluation_phase: str = phase_gate.REGULAR_SEASON,
                  phase_authority: CanonicalGamePhaseAuthority | None = None) -> dict[str, int]:
    predictions = pd.read_sql_query("SELECT * FROM predictions WHERE game_date=? ORDER BY game_id", conn,
                                    params=(game_date,))
    phase_gate.require_snapshot_membership(
        predictions.to_dict("records"), evaluation_phase,
        authority=phase_authority, require_freshness=True,
    )
    risk_count = price_count = 0
    historical = mode == "HISTORICAL_RECOVERY"
    for _, prediction in predictions.iterrows():
        event = v1.select_event(events, prediction)
        if historical:
            risk, prices = v1.classify_risk(prediction, event, request_started, returned, raw_sha)
        else:
            risk, prices = live_classify(prediction, event, request_started, returned, raw_sha)
        risk_count += v1.immutable_insert(conn, "risk_set", "game_key", risk)
        for price in prices:
            price_count += v1.immutable_insert(conn, "bookmaker_prices", ("game_key", "bookmaker_key"), price)
        provenance = {"game_key": risk["game_key"], "study_id": STUDY_ID, "game_date": game_date,
                      "capture_mode": mode, "request_event_id": request_event_id,
                      "raw_response_sha256": raw_sha}
        provenance["row_sha256"] = digest(provenance)
        v1.immutable_insert(conn, "capture_provenance_v4", "game_key", provenance)
    # Unmatched provider events have no exact MLB gamePk and are not study rows.
    return {"risk_rows_inserted": risk_count, "price_rows_inserted": price_count}


def _response_capture(conn: sqlite3.Connection, response: Any, *, game_date: str, mode: str,
                      run_identity: str, barrier: str, started: str, received: str,
                      target: str, raw_path: Path, params_path: Path, headers_path: Path,
                      evaluation_phase: str = phase_gate.REGULAR_SEASON,
                      phase_authority: CanonicalGamePhaseAuthority | None = None) -> dict[str, Any]:
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    with raw_path.open("xb") as handle:
        handle.write(hardened.redact_sensitive_bytes(response.content)); handle.flush(); os.fsync(handle.fileno())
    headers, last = response_parts(response)
    atomic_new_json(headers_path, {key: headers[key] for key in QUOTA_HEADERS})
    common = dict(game_date=game_date, run_identity=run_identity, capture_mode=mode,
                  prediction_barrier_utc=barrier, request_started_at_utc=started,
                  response_received_at_utc=received, requested_snapshot_utc=target,
                  http_status=int(response.status_code), x_requests_last=headers["x-requests-last"],
                  x_requests_used=headers["x-requests-used"], x_requests_remaining=headers["x-requests-remaining"],
                  raw_response_path=relative(raw_path), request_parameters_path=relative(params_path),
                  response_headers_path=relative(headers_path), raw_sha256=file_sha(raw_path))
    if not response.ok:
        event_id = append_event(conn, **common, status="HTTP_ERROR",
            error=hardened.http_error_message(mode.lower(), response.status_code, response.content))
        conn.commit()
        return {"status": "HTTP_ERROR", "event_id": event_id, "x_requests_last": last}
    try:
        payload = json.loads(raw_path.read_text())
        if mode == "LIVE":
            if not isinstance(payload, list):
                raise ValueError("live response is not an event list")
            returned, events = received, payload
        else:
            if not isinstance(payload, dict) or not isinstance(payload.get("data"), list) or not payload.get("timestamp"):
                raise ValueError("historical response lacks timestamp/data")
            returned, events = v1.iso(payload["timestamp"]), payload["data"]
            if pd.to_datetime(returned, utc=True) > pd.to_datetime(target, utc=True):
                raise ValueError("historical snapshot is after frozen live request timestamp")
    except (ValueError, json.JSONDecodeError, TypeError) as exc:
        event_id = append_event(conn, **common, status="INVALID_RESPONSE", error=str(exc))
        conn.commit()
        return {"status": "INVALID_RESPONSE", "event_id": event_id, "x_requests_last": last}
    event_id = append_event(conn, **common, status="SUCCESS_RESPONSE_PRESERVED", returned_snapshot_utc=returned)
    counts = ingest_events(conn, game_date, events, started if mode == "LIVE" else target,
                           returned, common["raw_sha256"], mode, event_id,
                           evaluation_phase=evaluation_phase, phase_authority=phase_authority)
    conn.commit()
    status = "SUCCESS_VALID_CAPTURE" if last == (1 if mode == "LIVE" else 10) else "SUCCESS_COST_MISMATCH"
    return {"status": status, "event_id": event_id, "x_requests_last": last, **counts}


def execute_capture(*, game_date: str, run_identity: str, mode: str, snapshot: PredictionSnapshot,
                    ledger: Path = LEDGER, runtime: Path = RUNTIME, freeze_path: Path = FREEZE,
                    getter: Callable[..., Any] = hardened.safe_get,
                    clock: Callable[[], str] = now_utc, timeout: int = 30,
                    evaluation_phase: str = phase_gate.REGULAR_SEASON,
                    phase_authority: CanonicalGamePhaseAuthority | None = None) -> dict[str, Any]:
    if not START_DATE <= game_date <= END_DATE:
        raise ValueError("game date outside frozen regular-season capture horizon")
    if snapshot.game_date != game_date:
        raise ValueError("prediction snapshot date mismatch")
    phase_gate.require_snapshot_membership(
        snapshot_phase_rows(snapshot), evaluation_phase,
        authority=phase_authority, require_freshness=True,
    )
    ledger.parent.mkdir(parents=True, exist_ok=True)
    mode = mode.upper()
    if mode not in {"LIVE", "HISTORICAL_RECOVERY"}:
        raise ValueError("unsupported capture mode")
    with sqlite3.connect(ledger, timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        schema(conn); establish_authorization(conn, freeze_path)
        ingest_snapshot_predictions(conn, snapshot)
        existing = terminal_events(conn, game_date, mode)
        if existing or conn.execute("SELECT 1 FROM live_capture_claims_v4 WHERE study_id=? AND game_date=? AND capture_mode=?",
                                    (STUDY_ID, game_date, mode)).fetchone():
            return {"status": "SKIPPED_PRIOR_ATTEMPT", "charged_request": False}
        if snapshot.expected_rows == 0:
            append_event(conn, game_date=game_date, run_identity=run_identity, capture_mode=mode,
                         status="NO_ELIGIBLE_SLATE", prediction_barrier_utc=snapshot.barrier_utc)
            conn.commit(); return {"status": "NO_ELIGIBLE_SLATE", "charged_request": False}
        prior_cost = observed_cost(conn)
        expected_cost = 1 if mode == "LIVE" else RECOVERY_EXPECTED_COST
        unknown_cost = conn.execute("""SELECT 1 FROM live_capture_events_v4
            WHERE study_id=? AND status IN ('SUCCESS_RESPONSE_PRESERVED','HTTP_ERROR','INVALID_RESPONSE')
              AND (x_requests_last='' OR x_requests_last IS NULL) LIMIT 1""", (STUDY_ID,)).fetchone()
        if unknown_cost:
            return {"status": "QUOTA_COST_UNKNOWN_STOP", "charged_request": False}
        if prior_cost + expected_cost > TOTAL_CEILING:
            append_event(conn, game_date=game_date, run_identity=run_identity, capture_mode=mode,
                         status="AUTHORIZATION_CEILING_STOP", prediction_barrier_utc=snapshot.barrier_utc,
                         error="Next request would exceed the frozen 48-credit total ceiling")
            conn.commit(); return {"status": "AUTHORIZATION_CEILING_STOP", "charged_request": False}
        if mode == "LIVE":
            routine_claims = conn.execute("SELECT COUNT(*) FROM live_capture_claims_v4 WHERE study_id=? AND capture_mode='LIVE'",
                                          (STUDY_ID,)).fetchone()[0]
            if routine_claims >= ROUTINE_CEILING:
                return {"status": "ROUTINE_CEILING_STOP", "charged_request": False}
        else:
            live = terminal_events(conn, game_date, "LIVE")
            if not live or live[-1]["status"] not in RECOVERY_ELIGIBLE:
                return {"status": "RECOVERY_NOT_ELIGIBLE", "charged_request": False}
            recoveries = conn.execute("SELECT COUNT(*) FROM live_capture_claims_v4 WHERE study_id=? AND capture_mode='HISTORICAL_RECOVERY'",
                                      (STUDY_ID,)).fetchone()[0]
            if recoveries >= RECOVERY_COUNT_CEILING:
                return {"status": "RECOVERY_COUNT_CEILING_STOP", "charged_request": False}
        started = clock()
        lag = (pd.to_datetime(started, utc=True) - pd.to_datetime(snapshot.barrier_utc, utc=True)).total_seconds()
        if mode == "LIVE" and (lag < 0 or lag > MAX_START_LAG_SECONDS):
            append_event(conn, game_date=game_date, run_identity=run_identity, capture_mode=mode,
                         status="LIVE_TIMING_FAIL_CLOSED", prediction_barrier_utc=snapshot.barrier_utc,
                         request_started_at_utc=started, error=f"start lag {lag:.6f}s outside [0,300]")
            conn.commit(); return {"status": "LIVE_TIMING_FAIL_CLOSED", "charged_request": False, "lag_seconds": lag}
        if not claim(conn, game_date, mode, run_identity, snapshot.barrier_utc, started):
            conn.rollback(); return {"status": "SKIPPED_CONCURRENT_CLAIM", "charged_request": False}
        target = started
        if mode == "HISTORICAL_RECOVERY":
            live_claim = conn.execute("SELECT request_started_at_utc FROM live_capture_claims_v4 WHERE study_id=? AND game_date=? AND capture_mode='LIVE'",
                                      (STUDY_ID, game_date)).fetchone()
            target = live_claim[0]
        raw_path, params_path, headers_path = paths(runtime, game_date, run_identity, mode)
        url, params, expected_cost = request_spec(mode, target)
        atomic_new_json(params_path, {"endpoint": url, "parameters_excluding_secret": params,
            "capture_mode": mode, "expected_cost": expected_cost, "total_authorized_ceiling": TOTAL_CEILING})
        append_event(conn, game_date=game_date, run_identity=run_identity, capture_mode=mode,
                     status="REQUEST_STARTED", prediction_barrier_utc=snapshot.barrier_utc,
                     request_started_at_utc=started, requested_snapshot_utc=target,
                     request_parameters_path=relative(params_path))
        conn.commit()
    key = os.getenv("ODDS_API_KEY", "").strip()
    if not key:
        with sqlite3.connect(ledger) as conn:
            schema(conn); append_event(conn, game_date=game_date, run_identity=run_identity, capture_mode=mode,
                status="LOCAL_CONFIGURATION_ERROR", prediction_barrier_utc=snapshot.barrier_utc,
                request_started_at_utc=started, requested_snapshot_utc=target,
                request_parameters_path=relative(params_path), error="ODDS_API_KEY missing"); conn.commit()
        return {"status": "LOCAL_CONFIGURATION_ERROR", "charged_request": False}
    try:
        response = getter(url, {**params, "apiKey": key}, timeout)
    except BaseException as exc:
        with sqlite3.connect(ledger) as conn:
            schema(conn); event_id = append_event(conn, game_date=game_date, run_identity=run_identity,
                capture_mode=mode, status="TRANSPORT_ERROR", prediction_barrier_utc=snapshot.barrier_utc,
                request_started_at_utc=started, requested_snapshot_utc=target,
                request_parameters_path=relative(params_path), error=hardened.render_exception(exc)); conn.commit()
        return {"status": "TRANSPORT_ERROR", "event_id": event_id, "charged_request": "UNKNOWN"}
    received = clock()
    with sqlite3.connect(ledger, timeout=30) as conn:
        conn.row_factory = sqlite3.Row; schema(conn)
        result = _response_capture(conn, response, game_date=game_date, mode=mode, run_identity=run_identity,
            barrier=snapshot.barrier_utc, started=started, received=received, target=target,
            raw_path=raw_path, params_path=params_path, headers_path=headers_path,
            evaluation_phase=evaluation_phase, phase_authority=phase_authority)
    return {**result, "charged_request": True,
            "authorization_stop_required": result.get("x_requests_last") is None or
                prior_cost + int(result.get("x_requests_last") or 0) > TOTAL_CEILING}


def run_live(game_date: str, run_identity: str, lifecycle_result: Path, **kwargs: Any) -> dict[str, Any]:
    if not START_DATE <= game_date <= END_DATE:
        raise ValueError("game date outside frozen regular-season capture horizon")
    if game_date < FIRST_ELIGIBLE_LIVE_DATE:
        return {"status": "SKIPPED_PRE_REQUEST_CLIENT_FAILURE_DATE",
                "failure_classification": "PRE_REQUEST_CLIENT_FAILURE",
                "game_date": game_date, "first_eligible_prospective_date": FIRST_ELIGIBLE_LIVE_DATE,
                "charged_request": False}
    expected = read_expected_rows(lifecycle_result, game_date)
    snapshot = load_prediction_snapshot(game_date, expected)
    return execute_capture(game_date=game_date, run_identity=run_identity, mode="LIVE", snapshot=snapshot, **kwargs)


def run_preflight(game_date: str, run_identity: str, lifecycle_result: Path, **kwargs: Any) -> dict[str, Any]:
    expected = read_expected_rows(lifecycle_result, game_date)
    snapshot = load_prediction_snapshot(game_date, expected)
    return preflight_capture(game_date=game_date, run_identity=run_identity,
                             snapshot=snapshot, **kwargs)


def run_recovery(game_date: str, run_identity: str, **kwargs: Any) -> dict[str, Any]:
    # Recovery re-verifies the already durable prediction set without consulting outcomes.
    claim_ledger = kwargs.get("ledger", LEDGER)
    with sqlite3.connect(claim_ledger) as conn:
        claim_row = conn.execute("""SELECT prediction_barrier_utc FROM live_capture_claims_v4
            WHERE study_id=? AND game_date=? AND capture_mode='LIVE'""", (STUDY_ID, game_date)).fetchone()
    if not claim_row:
        raise RuntimeError("No frozen live request claim exists for the requested recovery date")
    with pg_connect() as source, source.cursor() as cursor:
        cursor.execute("""SELECT COUNT(*) FROM mlb.public_game_moneyline_predictions
            WHERE game_date=%s AND model_version=%s AND prediction_snapshot_class=%s""",
            (game_date, v1.MODEL, v1.SNAPSHOT))
        count_row = cursor.fetchone()
    if not isinstance(count_row, dict) or set(count_row) != {"count"} or type(count_row["count"]) is not int:
        raise PredictionSnapshotSchemaError(
            "Durable prediction count query must return exactly one integer 'count' dictionary field"
        )
    expected = count_row["count"]
    snapshot = load_prediction_snapshot(game_date, expected)
    if snapshot.barrier_utc != claim_row[0]:
        raise RuntimeError("Durable prediction barrier changed after the frozen live request claim")
    return execute_capture(game_date=game_date, run_identity=run_identity, mode="HISTORICAL_RECOVERY", snapshot=snapshot, **kwargs)


def main() -> None:
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--live-date")
    group.add_argument("--preflight-date")
    group.add_argument("--recover-date")
    parser.add_argument("--run-identity", required=True)
    parser.add_argument("--lifecycle-result", type=Path)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    if args.live_date or args.preflight_date:
        if not args.lifecycle_result:
            parser.error("--lifecycle-result is required with --live-date or --preflight-date")
    if args.live_date:
        result = run_live(args.live_date, args.run_identity, args.lifecycle_result, timeout=args.timeout)
    elif args.preflight_date:
        result = run_preflight(args.preflight_date, args.run_identity, args.lifecycle_result)
    else:
        result = run_recovery(args.recover_date, args.run_identity, timeout=args.timeout)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
