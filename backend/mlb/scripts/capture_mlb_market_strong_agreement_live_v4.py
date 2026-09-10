#!/usr/bin/env python3
"""Isolated live h2h capture for the frozen MLB agreement-separation study v4."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from backend.app.deps import pg_connect
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
class PredictionSnapshot:
    game_date: str
    barrier_utc: str
    expected_rows: int
    rows: tuple[tuple[Any, ...], ...]


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


def load_prediction_snapshot(game_date: str, expected_rows: int) -> PredictionSnapshot:
    sql = """
      SELECT game_date::text,game_id,scheduled_start_utc,prediction_timestamp_utc,prediction_cutoff_utc,
             home_team,away_team,home_win_probability,away_win_probability,payload_sha256,model_hash,
             admission_status,created_at
      FROM mlb.public_game_moneyline_predictions
      WHERE game_date=%s AND model_version=%s AND prediction_snapshot_class=%s
      ORDER BY game_id
    """
    with pg_connect() as source, source.cursor() as cursor:
        cursor.execute(sql, (game_date, v1.MODEL, v1.SNAPSHOT))
        rows = tuple(cursor.fetchall())
    if len(rows) != expected_rows:
        raise RuntimeError(f"Durable commit barrier incomplete: expected {expected_rows} immutable rows; found {len(rows)}")
    if expected_rows == 0:
        return PredictionSnapshot(game_date, now_utc(), 0, rows)
    if any(row[10] != v1.MODEL_HASH for row in rows):
        raise RuntimeError("Immutable prediction row has a non-frozen model hash")
    barrier = max(v1.iso(row[12]) for row in rows)
    return PredictionSnapshot(game_date, barrier, expected_rows, rows)


def ingest_snapshot_predictions(conn: sqlite3.Connection, snapshot: PredictionSnapshot) -> int:
    inserted = 0
    for row in snapshot.rows:
        if row[11] != v1.ADMISSION:
            continue
        home, away = float(row[7]), float(row[8])
        strong = "HOME" if home > v1.BOUNDARY else "AWAY" if away > v1.BOUNDARY else "NONE"
        values = {"game_key": f"MLB|{row[0]}|{int(row[1])}", "game_date": row[0],
                  "game_id": int(row[1]), "scheduled_start_utc": v1.iso(row[2]),
                  "prediction_timestamp_utc": v1.iso(row[3]), "prediction_cutoff_utc": v1.iso(row[4]),
                  "home_team": row[5], "away_team": row[6], "home_model_probability": home,
                  "away_model_probability": away, "model_strong_side": strong,
                  "prediction_payload_sha256": row[9]}
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
                  request_event_id: str) -> dict[str, int]:
    predictions = pd.read_sql_query("SELECT * FROM predictions WHERE game_date=? ORDER BY game_id", conn,
                                    params=(game_date,))
    matched: set[str] = set()
    risk_count = price_count = 0
    historical = mode == "HISTORICAL_RECOVERY"
    for _, prediction in predictions.iterrows():
        event = v1.select_event(events, prediction)
        if event:
            matched.add(str(event.get("id")))
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
    for event in events:
        if str(event.get("id")) in matched:
            continue
        start = pd.to_datetime(event.get("commence_time"), utc=True, errors="coerce")
        if pd.isna(start) or start.tz_convert("America/New_York").date().isoformat() != game_date:
            continue
        if historical:
            risk, prices = v1.classify_risk(None, event, request_started, returned, raw_sha)
        else:
            risk, prices = live_classify(None, event, request_started, returned, raw_sha)
        # v1 derives provider-only dates in Pacific time; retain the frozen MLB slate date here.
        old_key = risk["game_key"]
        risk["game_date"] = game_date
        risk["game_key"] = f"MLB_PROVIDER|{game_date}|{event.get('id')}"
        risk["row_sha256"] = digest({key: value for key, value in risk.items() if key != "row_sha256"})
        for price in prices:
            price["game_key"] = risk["game_key"]
            price["row_sha256"] = digest({key: value for key, value in price.items() if key != "row_sha256"})
        del old_key
        risk_count += v1.immutable_insert(conn, "risk_set", "game_key", risk)
        for price in prices:
            price_count += v1.immutable_insert(conn, "bookmaker_prices", ("game_key", "bookmaker_key"), price)
        provenance = {"game_key": risk["game_key"], "study_id": STUDY_ID, "game_date": game_date,
                      "capture_mode": mode, "request_event_id": request_event_id,
                      "raw_response_sha256": raw_sha}
        provenance["row_sha256"] = digest(provenance)
        v1.immutable_insert(conn, "capture_provenance_v4", "game_key", provenance)
    return {"risk_rows_inserted": risk_count, "price_rows_inserted": price_count}


def _response_capture(conn: sqlite3.Connection, response: Any, *, game_date: str, mode: str,
                      run_identity: str, barrier: str, started: str, received: str,
                      target: str, raw_path: Path, params_path: Path, headers_path: Path) -> dict[str, Any]:
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
                           returned, common["raw_sha256"], mode, event_id)
    conn.commit()
    status = "SUCCESS_VALID_CAPTURE" if last == (1 if mode == "LIVE" else 10) else "SUCCESS_COST_MISMATCH"
    return {"status": status, "event_id": event_id, "x_requests_last": last, **counts}


def execute_capture(*, game_date: str, run_identity: str, mode: str, snapshot: PredictionSnapshot,
                    ledger: Path = LEDGER, runtime: Path = RUNTIME, freeze_path: Path = FREEZE,
                    getter: Callable[..., Any] = hardened.safe_get,
                    clock: Callable[[], str] = now_utc, timeout: int = 30) -> dict[str, Any]:
    if not START_DATE <= game_date <= END_DATE:
        raise ValueError("game date outside frozen regular-season capture horizon")
    if snapshot.game_date != game_date:
        raise ValueError("prediction snapshot date mismatch")
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
        params = {"bookmakers": ",".join(BOOKS), "markets": "h2h", "oddsFormat": "american", "dateFormat": "iso"}
        if mode == "HISTORICAL_RECOVERY":
            params["date"] = target
        url = LIVE_URL if mode == "LIVE" else HISTORICAL_URL
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
            raw_path=raw_path, params_path=params_path, headers_path=headers_path)
    return {**result, "charged_request": True,
            "authorization_stop_required": result.get("x_requests_last") is None or
                prior_cost + int(result.get("x_requests_last") or 0) > TOTAL_CEILING}


def run_live(game_date: str, run_identity: str, lifecycle_result: Path, **kwargs: Any) -> dict[str, Any]:
    expected = read_expected_rows(lifecycle_result, game_date)
    snapshot = load_prediction_snapshot(game_date, expected)
    return execute_capture(game_date=game_date, run_identity=run_identity, mode="LIVE", snapshot=snapshot, **kwargs)


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
        expected = int(cursor.fetchone()[0])
    snapshot = load_prediction_snapshot(game_date, expected)
    if snapshot.barrier_utc != claim_row[0]:
        raise RuntimeError("Durable prediction barrier changed after the frozen live request claim")
    return execute_capture(game_date=game_date, run_identity=run_identity, mode="HISTORICAL_RECOVERY", snapshot=snapshot, **kwargs)


def main() -> None:
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--live-date")
    group.add_argument("--recover-date")
    parser.add_argument("--run-identity", required=True)
    parser.add_argument("--lifecycle-result", type=Path)
    parser.add_argument("--timeout", type=int, default=30)
    args = parser.parse_args()
    if args.live_date:
        if not args.lifecycle_result:
            parser.error("--lifecycle-result is required with --live-date")
        result = run_live(args.live_date, args.run_identity, args.lifecycle_result, timeout=args.timeout)
    else:
        result = run_recovery(args.recover_date, args.run_identity, timeout=args.timeout)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
