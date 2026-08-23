"""Append-only storage for the Hits 0.5 sportsbook-independent shadow.

Prediction, market, and outcome state are deliberately separate.  SQLite
triggers make every recorded observation immutable; a later capture can append
new market/rank observations but cannot revise a frozen prediction.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any, Iterable


EXPERIMENT_ID = "MLB_HITS05_SPORTSBOOK_INDEPENDENT_FULL_BOARD_SHADOW_V1"
MODEL_ID = "MLB_HITS_SEMANTIC_V1_2e7377b2cdcb"
MODEL_HASH = "2e7377b2cdcb836b110e4b1a1a6acccff3afc78f7f82652e3b490048087ddadf"
TARGET_LINE = 0.5
EXPERIMENT_START_DATE = "2026-08-24"

OUTCOME_FORBIDDEN = {
    "actual_hits",
    "actual_value",
    "appeared_in_game",
    "outcome",
    "outcome_status",
    "result",
    "won",
}
MARKET_FORBIDDEN = {
    "bookmaker_key",
    "market_probability",
    "odds",
    "price_over_american",
    "price_under_american",
    "sportsbook",
}


def canonical_identity(slate_date: str, game_id: int, player_id: int) -> str:
    return f"{slate_date}|{int(game_id)}|{int(player_id)}|hits|0.5|{MODEL_ID}"


def canonical_json(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def payload_hash(payload: Any) -> str:
    return hashlib.sha256(canonical_json(payload).encode()).hexdigest()


def connect_ledger(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS hits05_full_board_predictions (
          canonical_identity TEXT PRIMARY KEY,
          experiment_id TEXT NOT NULL,
          slate_date TEXT NOT NULL,
          game_id INTEGER NOT NULL,
          player_id INTEGER NOT NULL,
          model_semantic_id TEXT NOT NULL,
          model_artifact_sha256 TEXT NOT NULL,
          scheduled_start_utc TEXT NOT NULL,
          prediction_timestamp_utc TEXT NOT NULL,
          run_tag TEXT NOT NULL,
          probability_over REAL NOT NULL CHECK(probability_over >= 0 AND probability_over <= 1),
          score_board_rank INTEGER NOT NULL CHECK(score_board_rank >= 1),
          score_board_percentile REAL NOT NULL CHECK(score_board_percentile >= 0 AND score_board_percentile <= 1),
          baseline_population_probability REAL NOT NULL,
          baseline_hitter_shrunk_probability REAL NOT NULL,
          feature_state_sha256 TEXT NOT NULL,
          replay_references_sha256 TEXT NOT NULL,
          input_artifacts_sha256 TEXT NOT NULL,
          prestart_integrity_result TEXT NOT NULL,
          prediction_payload_json TEXT NOT NULL,
          prediction_payload_sha256 TEXT NOT NULL,
          created_at_utc TEXT NOT NULL,
          UNIQUE(slate_date, game_id, player_id, model_semantic_id)
        );
        CREATE TABLE IF NOT EXISTS hits05_full_board_feature_context (
          canonical_identity TEXT PRIMARY KEY REFERENCES hits05_full_board_predictions(canonical_identity),
          feature_payload_json TEXT NOT NULL,
          feature_payload_sha256 TEXT NOT NULL,
          replay_references_json TEXT NOT NULL,
          replay_references_sha256 TEXT NOT NULL,
          input_artifacts_json TEXT NOT NULL,
          input_artifacts_sha256 TEXT NOT NULL,
          created_at_utc TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS hits05_full_board_outcomes (
          canonical_identity TEXT PRIMARY KEY REFERENCES hits05_full_board_predictions(canonical_identity),
          slate_date TEXT NOT NULL,
          game_id INTEGER NOT NULL,
          player_id INTEGER NOT NULL,
          actual_hits REAL,
          appearance_status TEXT NOT NULL,
          outcome_status TEXT NOT NULL,
          grading_timestamp_utc TEXT NOT NULL,
          grading_source TEXT NOT NULL,
          grading_source_sha256 TEXT NOT NULL,
          outcome_payload_json TEXT NOT NULL,
          outcome_payload_sha256 TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS hits05_full_board_market_observations (
          observation_identity TEXT PRIMARY KEY,
          canonical_identity TEXT NOT NULL REFERENCES hits05_full_board_predictions(canonical_identity),
          bookmaker_key TEXT NOT NULL,
          market_line REAL NOT NULL,
          price_over_american REAL,
          price_under_american REAL,
          no_vig_probability_over REAL,
          observation_timestamp_utc TEXT NOT NULL,
          scheduled_start_utc TEXT NOT NULL,
          snapshot_path TEXT NOT NULL,
          snapshot_sha256 TEXT NOT NULL,
          market_payload_json TEXT NOT NULL,
          market_payload_sha256 TEXT NOT NULL,
          created_at_utc TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS hits05_full_board_eligibility_observations (
          observation_identity TEXT PRIMARY KEY,
          slate_date TEXT NOT NULL,
          game_id INTEGER,
          player_id INTEGER,
          run_tag TEXT NOT NULL,
          eligibility_state TEXT NOT NULL,
          exclusion_reason TEXT NOT NULL,
          capture_timestamp_utc TEXT NOT NULL,
          scheduled_start_utc TEXT,
          payload_json TEXT NOT NULL,
          payload_sha256 TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS hits05_full_board_rank_snapshots (
          observation_identity TEXT PRIMARY KEY,
          canonical_identity TEXT NOT NULL REFERENCES hits05_full_board_predictions(canonical_identity),
          slate_date TEXT NOT NULL,
          snapshot_timestamp_utc TEXT NOT NULL,
          board_rows INTEGER NOT NULL,
          board_rank INTEGER NOT NULL,
          board_percentile REAL NOT NULL,
          snapshot_class TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          payload_sha256 TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS hits05_full_board_runs (
          run_tag TEXT PRIMARY KEY,
          slate_date TEXT NOT NULL,
          capture_timestamp_utc TEXT NOT NULL,
          evidence_mode TEXT NOT NULL,
          run_status TEXT NOT NULL,
          eligible_rows INTEGER NOT NULL,
          new_prediction_rows INTEGER NOT NULL,
          existing_prediction_rows INTEGER NOT NULL,
          excluded_rows INTEGER NOT NULL,
          outcomes_accessed INTEGER NOT NULL CHECK(outcomes_accessed = 0),
          run_payload_json TEXT NOT NULL,
          run_payload_sha256 TEXT NOT NULL
        );

        CREATE TRIGGER IF NOT EXISTS hits05_fb_predictions_no_update BEFORE UPDATE ON hits05_full_board_predictions BEGIN SELECT RAISE(ABORT, 'APPEND_ONLY_HITS05_FULL_BOARD_PREDICTIONS'); END;
        CREATE TRIGGER IF NOT EXISTS hits05_fb_predictions_no_delete BEFORE DELETE ON hits05_full_board_predictions BEGIN SELECT RAISE(ABORT, 'APPEND_ONLY_HITS05_FULL_BOARD_PREDICTIONS'); END;
        CREATE TRIGGER IF NOT EXISTS hits05_fb_context_no_update BEFORE UPDATE ON hits05_full_board_feature_context BEGIN SELECT RAISE(ABORT, 'APPEND_ONLY_HITS05_FULL_BOARD_CONTEXT'); END;
        CREATE TRIGGER IF NOT EXISTS hits05_fb_context_no_delete BEFORE DELETE ON hits05_full_board_feature_context BEGIN SELECT RAISE(ABORT, 'APPEND_ONLY_HITS05_FULL_BOARD_CONTEXT'); END;
        CREATE TRIGGER IF NOT EXISTS hits05_fb_outcomes_no_update BEFORE UPDATE ON hits05_full_board_outcomes BEGIN SELECT RAISE(ABORT, 'APPEND_ONLY_HITS05_FULL_BOARD_OUTCOMES'); END;
        CREATE TRIGGER IF NOT EXISTS hits05_fb_outcomes_no_delete BEFORE DELETE ON hits05_full_board_outcomes BEGIN SELECT RAISE(ABORT, 'APPEND_ONLY_HITS05_FULL_BOARD_OUTCOMES'); END;
        CREATE TRIGGER IF NOT EXISTS hits05_fb_markets_no_update BEFORE UPDATE ON hits05_full_board_market_observations BEGIN SELECT RAISE(ABORT, 'APPEND_ONLY_HITS05_FULL_BOARD_MARKETS'); END;
        CREATE TRIGGER IF NOT EXISTS hits05_fb_markets_no_delete BEFORE DELETE ON hits05_full_board_market_observations BEGIN SELECT RAISE(ABORT, 'APPEND_ONLY_HITS05_FULL_BOARD_MARKETS'); END;
        CREATE TRIGGER IF NOT EXISTS hits05_fb_eligibility_no_update BEFORE UPDATE ON hits05_full_board_eligibility_observations BEGIN SELECT RAISE(ABORT, 'APPEND_ONLY_HITS05_FULL_BOARD_ELIGIBILITY'); END;
        CREATE TRIGGER IF NOT EXISTS hits05_fb_eligibility_no_delete BEFORE DELETE ON hits05_full_board_eligibility_observations BEGIN SELECT RAISE(ABORT, 'APPEND_ONLY_HITS05_FULL_BOARD_ELIGIBILITY'); END;
        CREATE TRIGGER IF NOT EXISTS hits05_fb_ranks_no_update BEFORE UPDATE ON hits05_full_board_rank_snapshots BEGIN SELECT RAISE(ABORT, 'APPEND_ONLY_HITS05_FULL_BOARD_RANKS'); END;
        CREATE TRIGGER IF NOT EXISTS hits05_fb_ranks_no_delete BEFORE DELETE ON hits05_full_board_rank_snapshots BEGIN SELECT RAISE(ABORT, 'APPEND_ONLY_HITS05_FULL_BOARD_RANKS'); END;
        CREATE TRIGGER IF NOT EXISTS hits05_fb_runs_no_update BEFORE UPDATE ON hits05_full_board_runs BEGIN SELECT RAISE(ABORT, 'APPEND_ONLY_HITS05_FULL_BOARD_RUNS'); END;
        CREATE TRIGGER IF NOT EXISTS hits05_fb_runs_no_delete BEFORE DELETE ON hits05_full_board_runs BEGIN SELECT RAISE(ABORT, 'APPEND_ONLY_HITS05_FULL_BOARD_RUNS'); END;
        """
    )
    connection.commit()
    return connection


def _reject_prediction_contamination(prediction: dict[str, Any], context: dict[str, Any]) -> None:
    prediction_keys = set(prediction)
    if prediction_keys & OUTCOME_FORBIDDEN:
        raise ValueError("OUTCOME_FIELD_FORBIDDEN_IN_FULL_BOARD_PREDICTION")
    if prediction_keys & MARKET_FORBIDDEN:
        raise ValueError("MARKET_FIELD_FORBIDDEN_IN_FULL_BOARD_PREDICTION")
    serialized = canonical_json(context).lower()
    forbidden_tokens = ('"actual_hits"', '"outcome_status"', '"bookmaker_key"', '"price_over_american"', '"price_under_american"')
    if any(token in serialized for token in forbidden_tokens):
        raise ValueError("OUTCOME_OR_MARKET_FIELD_FORBIDDEN_IN_FEATURE_CONTEXT")


def append_prediction_with_context(
    connection: sqlite3.Connection,
    prediction: dict[str, Any],
    feature_payload: dict[str, Any],
    replay_references: dict[str, Any],
    input_artifacts: list[dict[str, Any]],
) -> str:
    _reject_prediction_contamination(prediction, feature_payload)
    identity = canonical_identity(prediction["slate_date"], prediction["game_id"], prediction["player_id"])
    feature_digest = payload_hash(feature_payload)
    replay_digest = payload_hash(replay_references)
    input_digest = payload_hash(input_artifacts)
    if feature_digest != prediction["feature_state_sha256"]:
        raise ValueError("FEATURE_STATE_HASH_MISMATCH")
    if replay_digest != prediction["replay_references_sha256"]:
        raise ValueError("REPLAY_REFERENCE_HASH_MISMATCH")
    if input_digest != prediction["input_artifacts_sha256"]:
        raise ValueError("INPUT_ARTIFACT_HASH_MISMATCH")
    encoded = canonical_json(prediction)
    digest = payload_hash(prediction)
    existing = connection.execute(
        "SELECT prediction_payload_sha256 FROM hits05_full_board_predictions WHERE canonical_identity=?",
        (identity,),
    ).fetchone()
    if existing:
        return "EXISTING_IMMUTABLE" if existing[0] == digest else "EXISTING_IDENTITY_DIFFERENT_CAPTURE_PRESERVED"
    try:
        connection.execute(
            """INSERT INTO hits05_full_board_predictions
            (canonical_identity,experiment_id,slate_date,game_id,player_id,model_semantic_id,model_artifact_sha256,
             scheduled_start_utc,prediction_timestamp_utc,run_tag,probability_over,score_board_rank,score_board_percentile,
             baseline_population_probability,baseline_hitter_shrunk_probability,feature_state_sha256,replay_references_sha256,
             input_artifacts_sha256,prestart_integrity_result,prediction_payload_json,prediction_payload_sha256,created_at_utc)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                identity,
                EXPERIMENT_ID,
                prediction["slate_date"],
                int(prediction["game_id"]),
                int(prediction["player_id"]),
                prediction["model_semantic_id"],
                prediction["model_artifact_sha256"],
                prediction["scheduled_start_utc"],
                prediction["prediction_timestamp_utc"],
                prediction["run_tag"],
                float(prediction["probability_over"]),
                int(prediction["score_board_rank"]),
                float(prediction["score_board_percentile"]),
                float(prediction["baseline_population_probability"]),
                float(prediction["baseline_hitter_shrunk_probability"]),
                feature_digest,
                replay_digest,
                input_digest,
                prediction["prestart_integrity_result"],
                encoded,
                digest,
                prediction["prediction_timestamp_utc"],
            ),
        )
        connection.execute(
            """INSERT INTO hits05_full_board_feature_context
            (canonical_identity,feature_payload_json,feature_payload_sha256,replay_references_json,replay_references_sha256,
             input_artifacts_json,input_artifacts_sha256,created_at_utc) VALUES (?,?,?,?,?,?,?,?)""",
            (
                identity,
                canonical_json(feature_payload),
                feature_digest,
                canonical_json(replay_references),
                replay_digest,
                canonical_json(input_artifacts),
                input_digest,
                prediction["prediction_timestamp_utc"],
            ),
        )
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    return "APPENDED_NEW"


def append_eligibility_observation(connection: sqlite3.Connection, payload: dict[str, Any]) -> str:
    if set(payload) & OUTCOME_FORBIDDEN or set(payload) & MARKET_FORBIDDEN:
        raise ValueError("CONTAMINATED_ELIGIBILITY_PAYLOAD")
    digest = payload_hash(payload)
    identity = hashlib.sha256(
        f"{payload['run_tag']}|{payload.get('game_id')}|{payload.get('player_id')}|{digest}".encode()
    ).hexdigest()
    existing = connection.execute(
        "SELECT payload_sha256 FROM hits05_full_board_eligibility_observations WHERE observation_identity=?",
        (identity,),
    ).fetchone()
    if existing:
        return "EXISTING_IMMUTABLE"
    connection.execute(
        """INSERT INTO hits05_full_board_eligibility_observations
        (observation_identity,slate_date,game_id,player_id,run_tag,eligibility_state,exclusion_reason,
         capture_timestamp_utc,scheduled_start_utc,payload_json,payload_sha256) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (
            identity,
            payload["slate_date"],
            payload.get("game_id"),
            payload.get("player_id"),
            payload["run_tag"],
            payload["eligibility_state"],
            payload.get("exclusion_reason") or "",
            payload["capture_timestamp_utc"],
            payload.get("scheduled_start_utc"),
            canonical_json(payload),
            digest,
        ),
    )
    connection.commit()
    return "APPENDED_NEW"


def append_market_observation(connection: sqlite3.Connection, identity: str, payload: dict[str, Any]) -> str:
    if not connection.execute(
        "SELECT 1 FROM hits05_full_board_predictions WHERE canonical_identity=?", (identity,)
    ).fetchone():
        return "PREDICTION_NOT_YET_AVAILABLE"
    digest = payload_hash(payload)
    observation_identity = hashlib.sha256(
        canonical_json({
            "canonical_identity": identity,
            "bookmaker_key": payload["bookmaker_key"],
            "market_line": payload["market_line"],
            "price_over_american": payload.get("price_over_american"),
            "price_under_american": payload.get("price_under_american"),
            "observation_timestamp_utc": payload["observation_timestamp_utc"],
            "snapshot_path": payload["snapshot_path"],
            "snapshot_sha256": payload["snapshot_sha256"],
        }).encode()
    ).hexdigest()
    existing = connection.execute(
        "SELECT market_payload_sha256 FROM hits05_full_board_market_observations WHERE observation_identity=?",
        (observation_identity,),
    ).fetchone()
    if existing:
        return "EXISTING_IMMUTABLE"
    connection.execute(
        """INSERT INTO hits05_full_board_market_observations
        (observation_identity,canonical_identity,bookmaker_key,market_line,price_over_american,price_under_american,
         no_vig_probability_over,observation_timestamp_utc,scheduled_start_utc,snapshot_path,snapshot_sha256,
         market_payload_json,market_payload_sha256,created_at_utc) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            observation_identity,
            identity,
            payload["bookmaker_key"],
            float(payload["market_line"]),
            payload.get("price_over_american"),
            payload.get("price_under_american"),
            payload.get("no_vig_probability_over"),
            payload["observation_timestamp_utc"],
            payload["scheduled_start_utc"],
            payload["snapshot_path"],
            payload["snapshot_sha256"],
            canonical_json(payload),
            digest,
            payload["created_at_utc"],
        ),
    )
    connection.commit()
    return "APPENDED_NEW"


def append_outcome(connection: sqlite3.Connection, identity: str, payload: dict[str, Any]) -> str:
    if not connection.execute(
        "SELECT 1 FROM hits05_full_board_predictions WHERE canonical_identity=?", (identity,)
    ).fetchone():
        raise ValueError("PREDICTION_IDENTITY_NOT_FOUND")
    digest = payload_hash(payload)
    existing = connection.execute(
        "SELECT outcome_payload_sha256 FROM hits05_full_board_outcomes WHERE canonical_identity=?", (identity,)
    ).fetchone()
    if existing:
        return "EXISTING_IMMUTABLE" if existing[0] == digest else "EXISTING_OUTCOME_CONFLICT_PRESERVED"
    connection.execute(
        """INSERT INTO hits05_full_board_outcomes
        (canonical_identity,slate_date,game_id,player_id,actual_hits,appearance_status,outcome_status,
         grading_timestamp_utc,grading_source,grading_source_sha256,outcome_payload_json,outcome_payload_sha256)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            identity,
            payload["slate_date"],
            int(payload["game_id"]),
            int(payload["player_id"]),
            payload.get("actual_hits"),
            payload["appearance_status"],
            payload["outcome_status"],
            payload["grading_timestamp_utc"],
            payload["grading_source"],
            payload["grading_source_sha256"],
            canonical_json(payload),
            digest,
        ),
    )
    connection.commit()
    return "APPENDED_NEW"


def append_rank_snapshot(connection: sqlite3.Connection, identity: str, payload: dict[str, Any]) -> str:
    if not connection.execute(
        "SELECT 1 FROM hits05_full_board_predictions WHERE canonical_identity=?", (identity,)
    ).fetchone():
        raise ValueError("PREDICTION_IDENTITY_NOT_FOUND")
    digest = payload_hash(payload)
    observation_identity = hashlib.sha256(
        f"{identity}|{payload['snapshot_timestamp_utc']}|{payload['snapshot_class']}".encode()
    ).hexdigest()
    if connection.execute(
        "SELECT 1 FROM hits05_full_board_rank_snapshots WHERE observation_identity=?", (observation_identity,)
    ).fetchone():
        return "EXISTING_IMMUTABLE"
    connection.execute(
        """INSERT INTO hits05_full_board_rank_snapshots
        (observation_identity,canonical_identity,slate_date,snapshot_timestamp_utc,board_rows,board_rank,
         board_percentile,snapshot_class,payload_json,payload_sha256) VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (
            observation_identity,
            identity,
            payload["slate_date"],
            payload["snapshot_timestamp_utc"],
            int(payload["board_rows"]),
            int(payload["board_rank"]),
            float(payload["board_percentile"]),
            payload["snapshot_class"],
            canonical_json(payload),
            digest,
        ),
    )
    connection.commit()
    return "APPENDED_NEW"


def append_run(connection: sqlite3.Connection, payload: dict[str, Any]) -> str:
    if int(payload.get("outcomes_accessed", -1)) != 0:
        raise ValueError("SCORING_RUN_MUST_NOT_ACCESS_OUTCOMES")
    digest = payload_hash(payload)
    existing = connection.execute(
        "SELECT run_payload_sha256 FROM hits05_full_board_runs WHERE run_tag=?", (payload["run_tag"],)
    ).fetchone()
    if existing:
        return "EXISTING_IMMUTABLE" if existing[0] == digest else "EXISTING_RUN_TAG_CONFLICT_PRESERVED"
    connection.execute(
        """INSERT INTO hits05_full_board_runs
        (run_tag,slate_date,capture_timestamp_utc,evidence_mode,run_status,eligible_rows,new_prediction_rows,
         existing_prediction_rows,excluded_rows,outcomes_accessed,run_payload_json,run_payload_sha256)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            payload["run_tag"],
            payload["slate_date"],
            payload["capture_timestamp_utc"],
            payload["evidence_mode"],
            payload["run_status"],
            int(payload["eligible_rows"]),
            int(payload["new_prediction_rows"]),
            int(payload["existing_prediction_rows"]),
            int(payload["excluded_rows"]),
            0,
            canonical_json(payload),
            digest,
        ),
    )
    connection.commit()
    return "APPENDED_NEW"


def predictions_for_date(connection: sqlite3.Connection, slate_date: str) -> list[dict[str, Any]]:
    rows = connection.execute(
        """SELECT canonical_identity,prediction_payload_json,prediction_payload_sha256
        FROM hits05_full_board_predictions WHERE slate_date=? ORDER BY scheduled_start_utc,game_id,player_id""",
        (slate_date,),
    ).fetchall()
    return [
        {"canonical_identity": row["canonical_identity"], **json.loads(row["prediction_payload_json"]),
         "prediction_payload_sha256": row["prediction_payload_sha256"]}
        for row in rows
    ]


def outcomes_for_date(connection: sqlite3.Connection, slate_date: str) -> list[dict[str, Any]]:
    rows = connection.execute(
        """SELECT canonical_identity,outcome_payload_json,outcome_payload_sha256
        FROM hits05_full_board_outcomes WHERE slate_date=? ORDER BY game_id,player_id""", (slate_date,)
    ).fetchall()
    return [
        {"canonical_identity": row["canonical_identity"], **json.loads(row["outcome_payload_json"]),
         "outcome_payload_sha256": row["outcome_payload_sha256"]}
        for row in rows
    ]


def prediction_identities(connection: sqlite3.Connection, slate_date: str) -> set[str]:
    return {
        str(row[0])
        for row in connection.execute(
            "SELECT canonical_identity FROM hits05_full_board_predictions WHERE slate_date=?", (slate_date,)
        ).fetchall()
    }


def prediction_payloads(connection: sqlite3.Connection, identities: Iterable[str]) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for identity in identities:
        row = connection.execute(
            "SELECT prediction_payload_json FROM hits05_full_board_predictions WHERE canonical_identity=?", (identity,)
        ).fetchone()
        if row:
            output[identity] = json.loads(row[0])
    return output


def counts(connection: sqlite3.Connection) -> dict[str, int]:
    tables = {
        "prediction_rows": "hits05_full_board_predictions",
        "feature_context_rows": "hits05_full_board_feature_context",
        "outcome_rows": "hits05_full_board_outcomes",
        "market_observation_rows": "hits05_full_board_market_observations",
        "eligibility_observation_rows": "hits05_full_board_eligibility_observations",
        "rank_snapshot_rows": "hits05_full_board_rank_snapshots",
        "run_rows": "hits05_full_board_runs",
    }
    result = {name: int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]) for name, table in tables.items()}
    result["duplicate_prediction_identities"] = int(
        connection.execute(
            "SELECT COUNT(*) FROM (SELECT canonical_identity FROM hits05_full_board_predictions GROUP BY canonical_identity HAVING COUNT(*)>1)"
        ).fetchone()[0]
    )
    result["duplicate_outcome_identities"] = int(
        connection.execute(
            "SELECT COUNT(*) FROM (SELECT canonical_identity FROM hits05_full_board_outcomes GROUP BY canonical_identity HAVING COUNT(*)>1)"
        ).fetchone()[0]
    )
    return result
