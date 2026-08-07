"""Append-only local lifecycle for the frozen V1 totals research shadow."""
from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any

MODEL_VERSION = "DIRECT_NEGATIVE_BINOMIAL"
SNAPSHOT_CLASS = "DAILY_DESIGNATED_PREGAME"


def canonical_identity(game_date: str, game_id: int) -> str:
    return f"{game_date}|{int(game_id)}|{MODEL_VERSION}|{SNAPSHOT_CLASS}"


def connect_ledger(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.executescript("""
    CREATE TABLE IF NOT EXISTS totals_shadow_predictions (
      canonical_identity TEXT PRIMARY KEY,
      game_date TEXT NOT NULL,
      game_id INTEGER NOT NULL,
      totals_model_version TEXT NOT NULL,
      prediction_snapshot_class TEXT NOT NULL,
      scheduled_start_utc TEXT NOT NULL,
      prediction_timestamp_utc TEXT NOT NULL,
      model_hash TEXT NOT NULL,
      feature_state_hash TEXT NOT NULL,
      schedule_source_hash TEXT NOT NULL,
      market_source_hash TEXT,
      prediction_payload_json TEXT NOT NULL,
      prediction_payload_sha256 TEXT NOT NULL,
      created_at_utc TEXT NOT NULL,
      UNIQUE(game_date, game_id, totals_model_version, prediction_snapshot_class)
    );
    CREATE TABLE IF NOT EXISTS totals_shadow_outcomes (
      canonical_identity TEXT PRIMARY KEY REFERENCES totals_shadow_predictions(canonical_identity),
      official_final_total INTEGER NOT NULL,
      regulation_nine_total INTEGER NOT NULL,
      official_source_hash TEXT NOT NULL,
      grading_payload_json TEXT NOT NULL,
      grading_payload_sha256 TEXT NOT NULL,
      graded_at_utc TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS totals_shadow_prediction_context (
      canonical_identity TEXT PRIMARY KEY REFERENCES totals_shadow_predictions(canonical_identity),
      context_payload_json TEXT NOT NULL,
      context_payload_sha256 TEXT NOT NULL,
      created_at_utc TEXT NOT NULL
    );
    CREATE TRIGGER IF NOT EXISTS totals_shadow_predictions_no_update BEFORE UPDATE ON totals_shadow_predictions BEGIN SELECT RAISE(ABORT, 'APPEND_ONLY_PREDICTION_LEDGER'); END;
    CREATE TRIGGER IF NOT EXISTS totals_shadow_predictions_no_delete BEFORE DELETE ON totals_shadow_predictions BEGIN SELECT RAISE(ABORT, 'APPEND_ONLY_PREDICTION_LEDGER'); END;
    CREATE TRIGGER IF NOT EXISTS totals_shadow_outcomes_no_update BEFORE UPDATE ON totals_shadow_outcomes BEGIN SELECT RAISE(ABORT, 'APPEND_ONLY_OUTCOME_LEDGER'); END;
    CREATE TRIGGER IF NOT EXISTS totals_shadow_outcomes_no_delete BEFORE DELETE ON totals_shadow_outcomes BEGIN SELECT RAISE(ABORT, 'APPEND_ONLY_OUTCOME_LEDGER'); END;
    CREATE TRIGGER IF NOT EXISTS totals_shadow_context_no_update BEFORE UPDATE ON totals_shadow_prediction_context BEGIN SELECT RAISE(ABORT, 'APPEND_ONLY_CONTEXT_LEDGER'); END;
    CREATE TRIGGER IF NOT EXISTS totals_shadow_context_no_delete BEFORE DELETE ON totals_shadow_prediction_context BEGIN SELECT RAISE(ABORT, 'APPEND_ONLY_CONTEXT_LEDGER'); END;
    """)
    connection.commit(); return connection


def payload_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def append_prediction(connection: sqlite3.Connection, payload: dict[str, Any]) -> str:
    forbidden = {"final_total", "regulation_nine_total", "outcome", "result", "official_final_total"}
    if forbidden & set(payload):
        raise ValueError("OUTCOME_FIELD_FORBIDDEN_IN_PREDICTION_LEDGER")
    identity = canonical_identity(payload["game_date"], payload["game_pk"]); encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False); digest = payload_hash(payload)
    existing = connection.execute("SELECT prediction_payload_sha256 FROM totals_shadow_predictions WHERE canonical_identity=?", (identity,)).fetchone()
    if existing:
        return "EXISTING_IMMUTABLE" if existing[0] == digest else "EXISTING_IDENTITY_DIFFERENT_CAPTURE_PRESERVED"
    connection.execute("""INSERT INTO totals_shadow_predictions
      (canonical_identity,game_date,game_id,totals_model_version,prediction_snapshot_class,scheduled_start_utc,prediction_timestamp_utc,
       model_hash,feature_state_hash,schedule_source_hash,market_source_hash,prediction_payload_json,prediction_payload_sha256,created_at_utc)
      VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (identity, payload["game_date"], payload["game_pk"], MODEL_VERSION, SNAPSHOT_CLASS,
       payload["scheduled_start_utc"], payload["prediction_timestamp_utc"], payload["model_hash"], payload["feature_state_hash"],
       payload["schedule_source_sha256"], payload.get("market_source_sha256"), encoded, digest, payload["prediction_timestamp_utc"]))
    connection.commit(); return "APPENDED_NEW"


def rows_for_date(connection: sqlite3.Connection, game_date: str) -> list[dict[str, Any]]:
    rows = connection.execute("""SELECT prediction_payload_json FROM totals_shadow_predictions
      WHERE game_date=? AND totals_model_version=? AND prediction_snapshot_class=? ORDER BY scheduled_start_utc,game_id""",
      (game_date, MODEL_VERSION, SNAPSHOT_CLASS)).fetchall()
    return [json.loads(row[0]) for row in rows]


def append_outcome(connection: sqlite3.Connection, identity: str, payload: dict[str, Any], graded_at_utc: str) -> str:
    """Append one immutable official-final grade for an existing prediction."""
    if not connection.execute("SELECT 1 FROM totals_shadow_predictions WHERE canonical_identity=?", (identity,)).fetchone():
        raise ValueError("PREDICTION_IDENTITY_NOT_FOUND")
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    digest = payload_hash(payload)
    existing = connection.execute(
        "SELECT grading_payload_sha256 FROM totals_shadow_outcomes WHERE canonical_identity=?", (identity,)
    ).fetchone()
    if existing:
        return "EXISTING_IMMUTABLE" if existing[0] == digest else "EXISTING_OUTCOME_CONFLICT_PRESERVED"
    connection.execute(
        """INSERT INTO totals_shadow_outcomes
        (canonical_identity,official_final_total,regulation_nine_total,official_source_hash,
         grading_payload_json,grading_payload_sha256,graded_at_utc) VALUES (?,?,?,?,?,?,?)""",
        (identity, int(payload["official_final_total"]), int(payload["regulation_nine_total"]),
         payload["official_source_hash"], encoded, digest, graded_at_utc),
    )
    connection.commit()
    return "APPENDED_NEW"


def outcomes_for_date(connection: sqlite3.Connection, game_date: str) -> list[dict[str, Any]]:
    rows = connection.execute(
        """SELECT o.canonical_identity,o.grading_payload_json,o.grading_payload_sha256,o.graded_at_utc
        FROM totals_shadow_outcomes o JOIN totals_shadow_predictions p USING(canonical_identity)
        WHERE p.game_date=? ORDER BY p.scheduled_start_utc,p.game_id""", (game_date,)
    ).fetchall()
    return [{"canonical_identity": identity, **json.loads(payload),
             "grading_payload_sha256": digest, "graded_at_utc": graded_at}
            for identity, payload, digest, graded_at in rows]


def append_context(connection: sqlite3.Connection, identity: str, payload: dict[str, Any], expected_hash: str, created_at_utc: str) -> str:
    digest = payload_hash(payload)
    if digest != expected_hash:
        raise ValueError("FEATURE_STATE_HASH_MISMATCH")
    existing = connection.execute("SELECT context_payload_sha256 FROM totals_shadow_prediction_context WHERE canonical_identity=?", (identity,)).fetchone()
    if existing:
        return "EXISTING_IMMUTABLE" if existing[0] == digest else "EXISTING_CONTEXT_CONFLICT_PRESERVED"
    connection.execute("INSERT INTO totals_shadow_prediction_context VALUES (?,?,?,?)",
        (identity, json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False), digest, created_at_utc))
    connection.commit(); return "APPENDED_NEW"


def contexts_for_date(connection: sqlite3.Connection, game_date: str) -> dict[str, dict[str, Any]]:
    rows = connection.execute("""SELECT c.canonical_identity,c.context_payload_json FROM totals_shadow_prediction_context c
      JOIN totals_shadow_predictions p USING(canonical_identity) WHERE p.game_date=?""", (game_date,)).fetchall()
    return {identity: json.loads(payload) for identity, payload in rows}


def counts(connection: sqlite3.Connection) -> dict[str, int]:
    return {"prediction_rows": connection.execute("SELECT COUNT(*) FROM totals_shadow_predictions").fetchone()[0],
            "context_rows": connection.execute("SELECT COUNT(*) FROM totals_shadow_prediction_context").fetchone()[0],
            "outcome_rows": connection.execute("SELECT COUNT(*) FROM totals_shadow_outcomes").fetchone()[0],
            "duplicate_prediction_identities": connection.execute("SELECT COUNT(*) FROM (SELECT canonical_identity FROM totals_shadow_predictions GROUP BY canonical_identity HAVING COUNT(*)>1)").fetchone()[0],
            "duplicate_outcome_identities": connection.execute("SELECT COUNT(*) FROM (SELECT canonical_identity FROM totals_shadow_outcomes GROUP BY canonical_identity HAVING COUNT(*)>1)").fetchone()[0]}
