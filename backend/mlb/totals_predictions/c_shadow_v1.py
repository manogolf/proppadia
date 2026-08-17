"""Append-only lifecycle for the frozen MLB totals C live shadow."""
from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any


MODEL_NAME = "DIRECT_NEGATIVE_BINOMIAL_COUNT_CONFIDENCE_ONLY_V1"
MODEL_HASH = "21828319efc421661f833484246b81e48721282ccd65f84ccc4f94222d7dd1cd"
SNAPSHOT_CLASS = "C_LIVE_SHADOW_CANONICAL_PREGAME"


def canonical_identity(game_date: str, game_pk: int) -> str:
    return f"{game_date}|{int(game_pk)}|{MODEL_NAME}|{SNAPSHOT_CLASS}"


def payload_hash(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def _contains_outcome(value: Any) -> bool:
    forbidden = {"outcome", "result", "final_total", "official_final_total", "regulation_nine_total"}
    if isinstance(value, dict):
        return bool(forbidden & {str(key).lower() for key in value}) or any(_contains_outcome(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_outcome(item) for item in value)
    return False


def connect_ledger(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA journal_mode=WAL")
    connection.executescript("""
    CREATE TABLE IF NOT EXISTS totals_c_shadow_predictions (
      canonical_identity TEXT PRIMARY KEY,
      game_date TEXT NOT NULL,
      game_pk INTEGER NOT NULL,
      model_name TEXT NOT NULL,
      model_hash TEXT NOT NULL,
      artifact_sha256 TEXT NOT NULL,
      snapshot_class TEXT NOT NULL,
      scheduled_start_utc TEXT NOT NULL,
      prediction_timestamp_utc TEXT NOT NULL,
      source_raw_identity TEXT NOT NULL,
      feature_state_hash TEXT NOT NULL,
      prediction_payload_json TEXT NOT NULL,
      prediction_payload_sha256 TEXT NOT NULL,
      created_at_utc TEXT NOT NULL,
      UNIQUE(game_date,game_pk,model_hash,snapshot_class)
    );
    CREATE TABLE IF NOT EXISTS totals_c_shadow_contexts (
      canonical_identity TEXT PRIMARY KEY REFERENCES totals_c_shadow_predictions(canonical_identity),
      context_payload_json TEXT NOT NULL,
      context_payload_sha256 TEXT NOT NULL,
      created_at_utc TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS totals_c_shadow_outcomes (
      canonical_identity TEXT PRIMARY KEY REFERENCES totals_c_shadow_predictions(canonical_identity),
      official_final_total INTEGER NOT NULL,
      regulation_nine_total INTEGER NOT NULL,
      official_source_hash TEXT NOT NULL,
      outcome_payload_json TEXT NOT NULL,
      outcome_payload_sha256 TEXT NOT NULL,
      graded_at_utc TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS totals_c_shadow_watch_observations (
      observation_identity TEXT PRIMARY KEY,
      game_date TEXT NOT NULL,
      scoring_run_tag TEXT NOT NULL,
      deployment_watch_status TEXT NOT NULL,
      regime_classification TEXT NOT NULL,
      watch_payload_json TEXT NOT NULL,
      watch_payload_sha256 TEXT NOT NULL,
      observed_at_utc TEXT NOT NULL,
      UNIQUE(game_date,scoring_run_tag)
    );
    CREATE TRIGGER IF NOT EXISTS totals_c_predictions_no_update BEFORE UPDATE ON totals_c_shadow_predictions BEGIN SELECT RAISE(ABORT,'APPEND_ONLY_C_PREDICTION_LEDGER'); END;
    CREATE TRIGGER IF NOT EXISTS totals_c_predictions_no_delete BEFORE DELETE ON totals_c_shadow_predictions BEGIN SELECT RAISE(ABORT,'APPEND_ONLY_C_PREDICTION_LEDGER'); END;
    CREATE TRIGGER IF NOT EXISTS totals_c_contexts_no_update BEFORE UPDATE ON totals_c_shadow_contexts BEGIN SELECT RAISE(ABORT,'APPEND_ONLY_C_CONTEXT_LEDGER'); END;
    CREATE TRIGGER IF NOT EXISTS totals_c_contexts_no_delete BEFORE DELETE ON totals_c_shadow_contexts BEGIN SELECT RAISE(ABORT,'APPEND_ONLY_C_CONTEXT_LEDGER'); END;
    CREATE TRIGGER IF NOT EXISTS totals_c_outcomes_no_update BEFORE UPDATE ON totals_c_shadow_outcomes BEGIN SELECT RAISE(ABORT,'APPEND_ONLY_C_OUTCOME_LEDGER'); END;
    CREATE TRIGGER IF NOT EXISTS totals_c_outcomes_no_delete BEFORE DELETE ON totals_c_shadow_outcomes BEGIN SELECT RAISE(ABORT,'APPEND_ONLY_C_OUTCOME_LEDGER'); END;
    CREATE TRIGGER IF NOT EXISTS totals_c_watch_no_update BEFORE UPDATE ON totals_c_shadow_watch_observations BEGIN SELECT RAISE(ABORT,'APPEND_ONLY_C_WATCH_LEDGER'); END;
    CREATE TRIGGER IF NOT EXISTS totals_c_watch_no_delete BEFORE DELETE ON totals_c_shadow_watch_observations BEGIN SELECT RAISE(ABORT,'APPEND_ONLY_C_WATCH_LEDGER'); END;
    """)
    connection.commit()
    return connection


def append_prediction_with_context(connection: sqlite3.Connection, prediction: dict[str, Any], context: dict[str, Any]) -> tuple[str, str]:
    if _contains_outcome(prediction) or _contains_outcome(context):
        raise ValueError("OUTCOME_FIELD_FORBIDDEN_IN_C_PREDICTION_LEDGER")
    identity = canonical_identity(prediction["game_date"], prediction["game_pk"])
    prediction_digest = payload_hash(prediction)
    context_digest = payload_hash(context)
    if context_digest != prediction["feature_state_hash"]:
        raise ValueError("C_FEATURE_STATE_HASH_MISMATCH")
    existing = connection.execute(
        "SELECT prediction_payload_sha256 FROM totals_c_shadow_predictions WHERE canonical_identity=?", (identity,)
    ).fetchone()
    if existing:
        context_existing = connection.execute(
            "SELECT context_payload_sha256 FROM totals_c_shadow_contexts WHERE canonical_identity=?", (identity,)
        ).fetchone()
        return (
            "EXISTING_IMMUTABLE" if existing[0] == prediction_digest else "EXISTING_IDENTITY_DIFFERENT_CAPTURE_PRESERVED",
            "EXISTING_IMMUTABLE" if context_existing and context_existing[0] == context_digest else "EXISTING_CONTEXT_CONFLICT_PRESERVED",
        )
    encoded = json.dumps(prediction, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    context_encoded = json.dumps(context, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    try:
        connection.execute("""INSERT INTO totals_c_shadow_predictions
          (canonical_identity,game_date,game_pk,model_name,model_hash,artifact_sha256,snapshot_class,
           scheduled_start_utc,prediction_timestamp_utc,source_raw_identity,feature_state_hash,
           prediction_payload_json,prediction_payload_sha256,created_at_utc)
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
            identity, prediction["game_date"], int(prediction["game_pk"]), MODEL_NAME, MODEL_HASH,
            prediction["artifact_sha256"], SNAPSHOT_CLASS, prediction["scheduled_start_utc"],
            prediction["prediction_timestamp_utc"], prediction["source_raw_identity"],
            prediction["feature_state_hash"], encoded, prediction_digest, prediction["prediction_timestamp_utc"],
        ))
        connection.execute("INSERT INTO totals_c_shadow_contexts VALUES (?,?,?,?)", (
            identity, context_encoded, context_digest, prediction["prediction_timestamp_utc"],
        ))
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return "APPENDED_NEW", "APPENDED_NEW"


def append_outcome(connection: sqlite3.Connection, identity: str, payload: dict[str, Any], graded_at_utc: str) -> str:
    if not connection.execute("SELECT 1 FROM totals_c_shadow_predictions WHERE canonical_identity=?", (identity,)).fetchone():
        raise ValueError("C_PREDICTION_IDENTITY_NOT_FOUND")
    digest = payload_hash(payload)
    existing = connection.execute(
        "SELECT outcome_payload_sha256 FROM totals_c_shadow_outcomes WHERE canonical_identity=?", (identity,)
    ).fetchone()
    if existing:
        return "EXISTING_IMMUTABLE" if existing[0] == digest else "EXISTING_OUTCOME_CONFLICT_PRESERVED"
    connection.execute("""INSERT INTO totals_c_shadow_outcomes
      (canonical_identity,official_final_total,regulation_nine_total,official_source_hash,
       outcome_payload_json,outcome_payload_sha256,graded_at_utc) VALUES (?,?,?,?,?,?,?)""", (
        identity, int(payload["official_final_total"]), int(payload["regulation_nine_total"]),
        payload["official_source_hash"], json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False),
        digest, graded_at_utc,
    ))
    connection.commit()
    return "APPENDED_NEW"


def append_watch_observation(connection: sqlite3.Connection, payload: dict[str, Any]) -> str:
    identity = f"{payload['game_date']}|{payload['scoring_run_tag']}"
    digest = payload_hash(payload)
    existing = connection.execute(
        "SELECT watch_payload_sha256 FROM totals_c_shadow_watch_observations WHERE observation_identity=?", (identity,)
    ).fetchone()
    if existing:
        return "EXISTING_IMMUTABLE" if existing[0] == digest else "EXISTING_WATCH_CONFLICT_PRESERVED"
    connection.execute("INSERT INTO totals_c_shadow_watch_observations VALUES (?,?,?,?,?,?,?,?)", (
        identity, payload["game_date"], payload["scoring_run_tag"], payload["deployment_watch_status"],
        payload["regime_classification"], json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False),
        digest, payload["observed_at_utc"],
    ))
    connection.commit()
    return "APPENDED_NEW"


def predictions_for_date(connection: sqlite3.Connection, game_date: str) -> list[dict[str, Any]]:
    rows = connection.execute(
        "SELECT prediction_payload_json FROM totals_c_shadow_predictions WHERE game_date=? ORDER BY scheduled_start_utc,game_pk",
        (game_date,),
    ).fetchall()
    return [json.loads(row[0]) for row in rows]


def outcomes_for_date(connection: sqlite3.Connection, game_date: str) -> list[dict[str, Any]]:
    rows = connection.execute("""SELECT o.canonical_identity,o.outcome_payload_json,o.outcome_payload_sha256,o.graded_at_utc
      FROM totals_c_shadow_outcomes o JOIN totals_c_shadow_predictions p USING(canonical_identity)
      WHERE p.game_date=? ORDER BY p.scheduled_start_utc,p.game_pk""", (game_date,)).fetchall()
    return [{"canonical_identity": identity, **json.loads(payload), "outcome_payload_sha256": digest, "graded_at_utc": graded}
            for identity, payload, digest, graded in rows]


def counts(connection: sqlite3.Connection) -> dict[str, int]:
    return {
        "prediction_rows": connection.execute("SELECT COUNT(*) FROM totals_c_shadow_predictions").fetchone()[0],
        "context_rows": connection.execute("SELECT COUNT(*) FROM totals_c_shadow_contexts").fetchone()[0],
        "outcome_rows": connection.execute("SELECT COUNT(*) FROM totals_c_shadow_outcomes").fetchone()[0],
        "watch_observation_rows": connection.execute("SELECT COUNT(*) FROM totals_c_shadow_watch_observations").fetchone()[0],
        "duplicate_prediction_identities": connection.execute("SELECT COUNT(*) FROM (SELECT canonical_identity FROM totals_c_shadow_predictions GROUP BY canonical_identity HAVING COUNT(*)>1)").fetchone()[0],
        "duplicate_outcome_identities": connection.execute("SELECT COUNT(*) FROM (SELECT canonical_identity FROM totals_c_shadow_outcomes GROUP BY canonical_identity HAVING COUNT(*)>1)").fetchone()[0],
    }
