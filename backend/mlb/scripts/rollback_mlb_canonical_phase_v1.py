#!/usr/bin/env python3
"""Prepared deterministic rollback executor for canonical phase activation."""

from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row

from backend.mlb.scripts.activate_mlb_canonical_phase_v1 import (
    CONTRACT_NAME,
    _table_state,
    atomic_json,
    canonical_json,
    sha256_path,
    target_identity,
)


AUTHORIZATION_PHRASE = "EXECUTE_MLB_2026_CANONICAL_PHASE_ROLLBACK_V1"
ROOT = Path(__file__).resolve().parents[3]


def rollback(
    dsn: str,
    *,
    rollback_sql: str,
    expected_target_identity_sha256: str,
    activation_evidence: dict[str, object],
) -> dict[str, object]:
    if activation_evidence.get("status") != "COMMITTED":
        raise RuntimeError("COMMITTED_ACTIVATION_EVIDENCE_REQUIRED")
    identity = target_identity(dsn)
    if identity["target_identity_sha256"] != expected_target_identity_sha256:
        raise RuntimeError("DATABASE_TARGET_IDENTITY_MISMATCH")
    if re.search(r"(?im)^\s*(BEGIN|COMMIT|ROLLBACK)\s*;", rollback_sql):
        raise RuntimeError("ROLLBACK_SQL_MUST_BE_TRANSACTION_NEUTRAL")
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as connection:
        with connection.transaction():
            with connection.cursor() as cursor:
                cursor.execute("SET LOCAL application_name = 'mlb_canonical_phase_rollback_v1'")
                cursor.execute("SET LOCAL lock_timeout = '3s'")
                cursor.execute("SET LOCAL statement_timeout = '120s'")
                cursor.execute(
                    "SELECT pg_try_advisory_xact_lock(hashtext(%s)) AS acquired",
                    (CONTRACT_NAME,),
                )
                if not cursor.fetchone()["acquired"]:
                    raise RuntimeError("ACTIVATION_ADVISORY_LOCK_UNAVAILABLE")
                cursor.execute(
                    "LOCK TABLE mlb.game_info, mlb_cleanroom_v1.games IN ACCESS EXCLUSIVE MODE NOWAIT"
                )
                before = {
                    "mlb.game_info": _table_state(cursor, "mlb.game_info", "game_id"),
                    "mlb_cleanroom_v1.games": _table_state(
                        cursor, "mlb_cleanroom_v1.games", "game_pk"
                    ),
                }
                cursor.execute("SELECT to_regclass('mlb.canonical_game_phase_v1') IS NOT NULL AS present")
                if not cursor.fetchone()["present"]:
                    raise RuntimeError("CANONICAL_PHASE_VIEW_NOT_PRESENT")
                cursor.execute(rollback_sql)
                after = {
                    "mlb.game_info": _table_state(cursor, "mlb.game_info", "game_id"),
                    "mlb_cleanroom_v1.games": _table_state(
                        cursor, "mlb_cleanroom_v1.games", "game_pk"
                    ),
                }
                if before != after:
                    raise RuntimeError("ROLLBACK_CHANGED_BASE_ROWS_OR_IDENTITIES")
                cursor.execute("SELECT to_regclass('mlb.canonical_game_phase_v1') IS NULL AS absent")
                if not cursor.fetchone()["absent"]:
                    raise RuntimeError("CANONICAL_PHASE_VIEW_STILL_PRESENT")
                cursor.execute(
                    """
                    SELECT COUNT(*)::integer AS count
                    FROM information_schema.columns
                    WHERE (table_schema, table_name) IN (
                      ('mlb', 'game_info'), ('mlb_cleanroom_v1', 'games')
                    ) AND column_name IN (
                      'source_season','source_game_type','season_phase','postseason_round',
                      'season_name','source_round','schedule_relationships','game_type_source_sha256'
                    )
                    """
                )
                if cursor.fetchone()["count"] != 0:
                    raise RuntimeError("CANONICAL_PHASE_COLUMNS_STILL_PRESENT")
                return {
                    "contract_name": CONTRACT_NAME,
                    "status": "ROLLED_BACK_COMMITTED",
                    "target": identity,
                    "before": before,
                    "after": after,
                    "zero_row_creation_or_deletion": True,
                    "base_identity_hashes_unchanged": True,
                    "activation_evidence_sha256": activation_evidence.get("proposal_sha256"),
                }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--authorization-phrase")
    parser.add_argument("--env-file", type=Path, default=ROOT / "backend/.env")
    parser.add_argument("--rollback", required=True, type=Path)
    parser.add_argument("--activation-evidence", required=True, type=Path)
    parser.add_argument("--evidence-output", required=True, type=Path)
    parser.add_argument("--expected-target-identity-sha256", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.execute or args.authorization_phrase != AUTHORIZATION_PHRASE:
        raise SystemExit("ROLLBACK_NOT_AUTHORIZED")
    rollback_path = args.rollback.resolve()
    activation_path = args.activation_evidence.resolve()
    evidence_path = args.evidence_output.resolve()
    started = {
        "contract_name": CONTRACT_NAME,
        "status": "ROLLBACK_STARTED_NOT_COMMITTED",
        "started_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "rollback_path": str(rollback_path),
        "rollback_sha256": sha256_path(rollback_path),
        "activation_evidence_path": str(activation_path),
    }
    atomic_json(evidence_path, started)
    try:
        activation_evidence = json.loads(activation_path.read_text())
        load_dotenv(args.env_file, override=False)
        dsn = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
        if not dsn:
            raise RuntimeError("DATABASE_URL_OR_SUPABASE_DB_URL_REQUIRED")
        result = rollback(
            dsn,
            rollback_sql=rollback_path.read_text(),
            expected_target_identity_sha256=args.expected_target_identity_sha256,
            activation_evidence=activation_evidence,
        )
    except Exception as exc:
        failed = {
            **started,
            "status": "ROLLBACK_FAILED_ROLLED_BACK_OR_NOT_STARTED",
            "completed_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "error_type": type(exc).__name__,
            "error": str(exc),
        }
        atomic_json(evidence_path, failed)
        print(canonical_json(failed))
        return 1
    completed = {
        **started,
        **result,
        "completed_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    atomic_json(evidence_path, completed)
    print(canonical_json(completed))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
