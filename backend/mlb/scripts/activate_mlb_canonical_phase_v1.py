#!/usr/bin/env python3
"""Prepared transactional canonical-phase migration/backfill loader.

This module is inert unless ``--execute`` and the exact authorization phrase
are both supplied. It is intended for a separately authorized maintenance
window; the migration activation preflight must never invoke ``main`` with
those flags against the operational target.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse

import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from backend.mlb.season_transition.contract_v1 import normalize_source_game_type


CONTRACT_NAME = "MLB_2026_CANONICAL_PHASE_MIGRATION_ACTIVATION_PREFLIGHT_V1"
AUTHORIZATION_PHRASE = "EXECUTE_MLB_2026_CANONICAL_PHASE_ACTIVATION_V1"
EXPECTED_PROPOSAL_COUNT = 2919
EXPECTED_TYPE_COUNTS = {"E": 38, "R": 2430, "S": 451}
EXPECTED_PHASE_COUNTS = {"PRESEASON": 489, "REGULAR_SEASON": 2430}
ROOT = Path(__file__).resolve().parents[3]


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_path(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    data = (json.dumps(value, sort_keys=True, indent=2, default=str) + "\n").encode()
    with temporary.open("wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def target_identity(dsn: str) -> dict[str, Any]:
    parsed = urlparse(dsn)
    identity = {
        "engine": "PostgreSQL",
        "host": parsed.hostname or "",
        "port": parsed.port or 5432,
        "database": parsed.path.lstrip("/"),
    }
    return {
        **identity,
        "target_identity_sha256": sha256_bytes(canonical_json(identity).encode()),
    }


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def validate_inputs(
    proposal_path: Path,
    source_manifest_path: Path,
    expected_proposal_sha256: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    proposal_sha = sha256_path(proposal_path)
    if proposal_sha != expected_proposal_sha256:
        raise RuntimeError(f"PROPOSAL_SHA256_MISMATCH:{proposal_sha}")
    if not re.fullmatch(r"[0-9a-f]{64}", expected_proposal_sha256):
        raise RuntimeError("EXPECTED_PROPOSAL_SHA256_INVALID")
    source_rows = read_jsonl(source_manifest_path)
    source_hashes: dict[str, str] = {}
    for source in source_rows:
        path = (ROOT / source["source_path"]).resolve()
        digest = sha256_path(path)
        if digest != source["source_sha256"]:
            raise RuntimeError(f"SOURCE_HASH_MISMATCH:{source['source_path']}")
        source_hashes[str(source["source_sha256"])] = str(source["source_path"])
    rows = read_jsonl(proposal_path)
    ids = [int(row["game_pk"]) for row in rows]
    if len(rows) != EXPECTED_PROPOSAL_COUNT or len(set(ids)) != EXPECTED_PROPOSAL_COUNT:
        raise RuntimeError(f"PROPOSAL_IDENTITY_COUNT_MISMATCH:{len(rows)}:{len(set(ids))}")
    type_counts: Counter[str] = Counter()
    phase_counts: Counter[str] = Counter()
    for row in rows:
        classification = normalize_source_game_type(
            row.get("source_game_type"),
            season=int(row.get("source_season")),
            source_round=row.get("source_round"),
        )
        if classification.phase != row.get("season_phase"):
            raise RuntimeError(f"PROPOSAL_PHASE_CONFLICT:{row['game_pk']}")
        if classification.postseason_round != row.get("postseason_round"):
            raise RuntimeError(f"PROPOSAL_ROUND_CONFLICT:{row['game_pk']}")
        if classification.season_name != row.get("season_name"):
            raise RuntimeError(f"PROPOSAL_SEASON_NAME_CONFLICT:{row['game_pk']}")
        hashes = set(row.get("source_hashes") or [])
        if not hashes or not hashes.issubset(source_hashes):
            raise RuntimeError(f"PROPOSAL_SOURCE_HASH_UNVERIFIED:{row['game_pk']}")
        if row.get("game_type_source_sha256") not in hashes:
            raise RuntimeError(f"PROPOSAL_PRIMARY_SOURCE_HASH_UNVERIFIED:{row['game_pk']}")
        type_counts[str(row["source_game_type"])] += 1
        phase_counts[str(row["season_phase"])] += 1
    if dict(sorted(type_counts.items())) != EXPECTED_TYPE_COUNTS:
        raise RuntimeError(f"PROPOSAL_TYPE_COUNTS_MISMATCH:{dict(type_counts)}")
    if dict(sorted(phase_counts.items())) != EXPECTED_PHASE_COUNTS:
        raise RuntimeError(f"PROPOSAL_PHASE_COUNTS_MISMATCH:{dict(phase_counts)}")
    return rows, {
        "proposal_sha256": proposal_sha,
        "proposal_count": len(rows),
        "source_manifest_sha256": sha256_path(source_manifest_path),
        "source_file_count": len(source_rows),
        "source_type_counts": dict(sorted(type_counts.items())),
        "phase_counts": dict(sorted(phase_counts.items())),
        "missing_count": 0,
        "unknown_count": 0,
        "conflicting_count": 0,
        "duplicate_identity_count": 0,
    }


def _rowset_hash(rows: Iterable[dict[str, Any]]) -> str:
    return sha256_bytes(canonical_json(list(rows)).encode())


def _table_state(cursor: psycopg.Cursor[Any], relation: str, key: str) -> dict[str, Any]:
    cursor.execute(
        f"SELECT COUNT(*)::bigint AS rows, COUNT(DISTINCT {key})::bigint AS distinct_game_pks FROM {relation}"
    )
    counts = dict(cursor.fetchone())
    cursor.execute(f"SELECT {key}::bigint AS game_pk FROM {relation} ORDER BY {key}")
    identities = [dict(row) for row in cursor.fetchall()]
    return {**counts, "identity_sha256": _rowset_hash(identities)}


def _phase_state(cursor: psycopg.Cursor[Any], relation: str, key: str) -> dict[str, Any]:
    cursor.execute(
        f"""
        SELECT {key}::bigint AS game_pk, source_season, source_game_type,
               season_phase, postseason_round, season_name, source_round,
               schedule_relationships
        FROM {relation}
        WHERE source_game_type IS NOT NULL
        ORDER BY {key}, source_game_type, season_phase, postseason_round, season_name,
                 source_round, schedule_relationships::text
        """
    )
    rows = [dict(row) for row in cursor.fetchall()]
    return {"typed_row_count": len(rows), "typed_rowset_sha256": _rowset_hash(rows)}


def activate(
    dsn: str,
    *,
    rows: list[dict[str, Any]],
    migration_sql: str,
    input_evidence: dict[str, Any],
    expected_target_identity_sha256: str,
    expected_game_info_matches: int,
    expected_cleanroom_rows: int,
    expected_game_info_changes: int,
    expected_cleanroom_changes: int,
) -> dict[str, Any]:
    if re.search(r"(?im)^\s*(BEGIN|COMMIT|ROLLBACK)\s*;", migration_sql):
        raise RuntimeError("MIGRATION_MUST_BE_TRANSACTION_NEUTRAL")
    identity = target_identity(dsn)
    if identity["target_identity_sha256"] != expected_target_identity_sha256:
        raise RuntimeError("DATABASE_TARGET_IDENTITY_MISMATCH")
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as connection:
        with connection.transaction():
            with connection.cursor() as cursor:
                cursor.execute("SET LOCAL application_name = 'mlb_canonical_phase_activation_v1'")
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
                cursor.execute(migration_sql)
                cursor.execute(
                    """
                    SELECT t.tgenabled, pg_get_triggerdef(t.oid, true) AS definition,
                           pg_get_functiondef(t.tgfoid) AS function_definition
                    FROM pg_trigger t
                    JOIN pg_class c ON c.oid = t.tgrelid
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = 'mlb_cleanroom_v1' AND c.relname = 'games'
                      AND t.tgname = 'reject_mutation' AND NOT t.tgisinternal
                    """
                )
                trigger = cursor.fetchone()
                if (
                    trigger is None
                    or trigger["tgenabled"] != "O"
                    or "BEFORE DELETE OR UPDATE" not in trigger["definition"]
                    or "source tables are append-only" not in trigger["function_definition"]
                ):
                    raise RuntimeError("CLEANROOM_IMMUTABILITY_TRIGGER_UNEXPECTED")
                cursor.execute(
                    """
                    CREATE TEMP TABLE canonical_phase_stage_v1 (
                      game_pk bigint PRIMARY KEY,
                      source_season integer NOT NULL,
                      source_game_type text NOT NULL,
                      season_phase text,
                      postseason_round text,
                      season_name text,
                      source_round text,
                      schedule_relationships jsonb NOT NULL,
                      game_type_source_sha256 text NOT NULL
                    ) ON COMMIT DROP
                    """
                )
                with cursor.copy(
                    """
                    COPY canonical_phase_stage_v1 (
                      game_pk, source_season, source_game_type, season_phase,
                      postseason_round, season_name, source_round,
                      schedule_relationships, game_type_source_sha256
                    ) FROM STDIN
                    """
                ) as copy:
                    for row in rows:
                        copy.write_row(
                            (
                                int(row["game_pk"]),
                                int(row["source_season"]),
                                row["source_game_type"],
                                row["season_phase"],
                                row.get("postseason_round"),
                                row.get("season_name"),
                                row.get("source_round"),
                                Jsonb(row.get("schedule_relationships") or {}),
                                row["game_type_source_sha256"],
                            )
                        )
                cursor.execute(
                    "SELECT COUNT(*)::bigint AS count, COUNT(DISTINCT game_pk)::bigint AS distinct_count FROM canonical_phase_stage_v1"
                )
                staged = dict(cursor.fetchone())
                if staged != {"count": EXPECTED_PROPOSAL_COUNT, "distinct_count": EXPECTED_PROPOSAL_COUNT}:
                    raise RuntimeError(f"STAGED_MANIFEST_COUNT_MISMATCH:{staged}")
                cursor.execute(
                    "SELECT COUNT(*)::bigint AS count FROM mlb.game_info g JOIN canonical_phase_stage_v1 s ON s.game_pk = g.game_id"
                )
                game_info_matches = int(cursor.fetchone()["count"])
                cursor.execute(
                    "SELECT COUNT(*)::bigint AS count FROM mlb_cleanroom_v1.games g JOIN canonical_phase_stage_v1 s USING (game_pk)"
                )
                cleanroom_matches = int(cursor.fetchone()["count"])
                if game_info_matches != expected_game_info_matches:
                    raise RuntimeError(
                        f"GAME_INFO_MATCH_COUNT_MISMATCH:{game_info_matches}:{expected_game_info_matches}"
                    )
                if cleanroom_matches != expected_cleanroom_rows:
                    raise RuntimeError(
                        f"CLEANROOM_MATCH_COUNT_MISMATCH:{cleanroom_matches}:{expected_cleanroom_rows}"
                    )
                cursor.execute(
                    """
                    SELECT COUNT(*)::bigint AS count
                    FROM mlb.game_info g JOIN canonical_phase_stage_v1 s ON s.game_pk = g.game_id
                    WHERE g.source_game_type IS NOT NULL
                      AND ROW(g.source_season, g.source_game_type, g.season_phase,
                              g.postseason_round, g.season_name)
                          IS DISTINCT FROM
                          ROW(s.source_season, s.source_game_type, s.season_phase,
                              s.postseason_round, s.season_name)
                    """
                )
                if cursor.fetchone()["count"]:
                    raise RuntimeError("EXISTING_GAME_INFO_PHASE_CONFLICT")
                cursor.execute(
                    """
                    SELECT COUNT(*)::bigint AS count
                    FROM mlb_cleanroom_v1.games g JOIN canonical_phase_stage_v1 s USING (game_pk)
                    WHERE g.source_game_type IS NOT NULL
                      AND ROW(g.source_season, g.source_game_type, g.season_phase,
                              g.postseason_round, g.season_name)
                          IS DISTINCT FROM
                          ROW(s.source_season, s.source_game_type, s.season_phase,
                              s.postseason_round, s.season_name)
                    """
                )
                if cursor.fetchone()["count"]:
                    raise RuntimeError("EXISTING_CLEANROOM_PHASE_CONFLICT")

                cursor.execute(
                    """
                    UPDATE mlb.game_info AS g SET
                      source_season = s.source_season,
                      source_game_type = s.source_game_type,
                      season_phase = s.season_phase,
                      postseason_round = s.postseason_round,
                      season_name = s.season_name,
                      source_round = s.source_round,
                      schedule_relationships = s.schedule_relationships,
                      game_type_source_sha256 = s.game_type_source_sha256
                    FROM canonical_phase_stage_v1 AS s
                    WHERE g.game_id = s.game_pk
                      AND ROW(g.source_season, g.source_game_type, g.season_phase,
                              g.postseason_round, g.season_name, g.source_round,
                              g.schedule_relationships, g.game_type_source_sha256)
                          IS DISTINCT FROM
                          ROW(s.source_season, s.source_game_type, s.season_phase,
                              s.postseason_round, s.season_name, s.source_round,
                              s.schedule_relationships, s.game_type_source_sha256)
                    """
                )
                game_info_changes = int(cursor.rowcount)

                cursor.execute(
                    "ALTER TABLE mlb_cleanroom_v1.games DISABLE TRIGGER reject_mutation"
                )
                cursor.execute(
                    """
                    UPDATE mlb_cleanroom_v1.games AS g SET
                      source_season = s.source_season,
                      source_game_type = s.source_game_type,
                      season_phase = s.season_phase,
                      postseason_round = s.postseason_round,
                      season_name = s.season_name,
                      source_round = s.source_round,
                      schedule_relationships = s.schedule_relationships
                    FROM canonical_phase_stage_v1 AS s
                    WHERE g.game_pk = s.game_pk
                      AND ROW(g.source_season, g.source_game_type, g.season_phase,
                              g.postseason_round, g.season_name, g.source_round,
                              g.schedule_relationships)
                          IS DISTINCT FROM
                          ROW(s.source_season, s.source_game_type, s.season_phase,
                              s.postseason_round, s.season_name, s.source_round,
                              s.schedule_relationships)
                    """
                )
                cleanroom_changes = int(cursor.rowcount)
                cursor.execute(
                    "ALTER TABLE mlb_cleanroom_v1.games ENABLE TRIGGER reject_mutation"
                )
                if game_info_changes != expected_game_info_changes:
                    raise RuntimeError(
                        f"GAME_INFO_MUTATION_COUNT_MISMATCH:{game_info_changes}:{expected_game_info_changes}"
                    )
                if cleanroom_changes != expected_cleanroom_changes:
                    raise RuntimeError(
                        f"CLEANROOM_MUTATION_COUNT_MISMATCH:{cleanroom_changes}:{expected_cleanroom_changes}"
                    )
                after = {
                    "mlb.game_info": {
                        **_table_state(cursor, "mlb.game_info", "game_id"),
                        **_phase_state(cursor, "mlb.game_info", "game_id"),
                    },
                    "mlb_cleanroom_v1.games": {
                        **_table_state(cursor, "mlb_cleanroom_v1.games", "game_pk"),
                        **_phase_state(cursor, "mlb_cleanroom_v1.games", "game_pk"),
                    },
                }
                for relation in before:
                    if before[relation]["rows"] != after[relation]["rows"]:
                        raise RuntimeError(f"UNINTENDED_ROW_COUNT_CHANGE:{relation}")
                    if before[relation]["identity_sha256"] != after[relation]["identity_sha256"]:
                        raise RuntimeError(f"UNINTENDED_IDENTITY_CHANGE:{relation}")
                cursor.execute("SELECT COUNT(*)::bigint AS count FROM mlb.canonical_game_phase_v1")
                canonical_view_rows = int(cursor.fetchone()["count"])
                cursor.execute(
                    """
                    SELECT COUNT(*)::bigint AS count
                    FROM (
                      SELECT game_id AS game_pk FROM mlb.game_info WHERE source_game_type IS NOT NULL
                      UNION
                      SELECT game_pk FROM mlb_cleanroom_v1.games WHERE source_game_type IS NOT NULL
                    ) q
                    """
                )
                expected_view_rows = int(cursor.fetchone()["count"])
                if canonical_view_rows != expected_view_rows:
                    raise RuntimeError(
                        f"CANONICAL_VIEW_CONFLICT_OR_OMISSION:{canonical_view_rows}:{expected_view_rows}"
                    )
                cursor.execute(
                    """
                    SELECT tgenabled FROM pg_trigger t
                    JOIN pg_class c ON c.oid = t.tgrelid
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = 'mlb_cleanroom_v1' AND c.relname = 'games'
                      AND t.tgname = 'reject_mutation' AND NOT t.tgisinternal
                    """
                )
                if cursor.fetchone()["tgenabled"] != "O":
                    raise RuntimeError("CLEANROOM_IMMUTABILITY_TRIGGER_NOT_REENABLED")
                return {
                    "contract_name": CONTRACT_NAME,
                    "status": "COMMITTED",
                    "target": identity,
                    "input_evidence": input_evidence,
                    "before": before,
                    "after": after,
                    "proposed_mutations": {
                        "mlb.game_info": game_info_changes,
                        "mlb_cleanroom_v1.games": cleanroom_changes,
                    },
                    "target_matches": {
                        "mlb.game_info": game_info_matches,
                        "mlb_cleanroom_v1.games": cleanroom_matches,
                    },
                    "canonical_view_rows": canonical_view_rows,
                    "zero_row_creation_or_deletion": True,
                    "cleanroom_immutability_trigger_reenabled": True,
                    "atomic_transaction": True,
                }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--authorization-phrase")
    parser.add_argument("--env-file", type=Path, default=ROOT / "backend/.env")
    parser.add_argument("--proposal", required=True, type=Path)
    parser.add_argument("--source-manifest", required=True, type=Path)
    parser.add_argument("--migration", required=True, type=Path)
    parser.add_argument("--evidence-output", required=True, type=Path)
    parser.add_argument("--expected-proposal-sha256", required=True)
    parser.add_argument("--expected-target-identity-sha256", required=True)
    parser.add_argument("--expected-game-info-matches", required=True, type=int)
    parser.add_argument("--expected-cleanroom-rows", required=True, type=int)
    parser.add_argument("--expected-game-info-changes", required=True, type=int)
    parser.add_argument("--expected-cleanroom-changes", required=True, type=int)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.execute or args.authorization_phrase != AUTHORIZATION_PHRASE:
        raise SystemExit("ACTIVATION_NOT_AUTHORIZED")
    proposal = args.proposal.resolve()
    source_manifest = args.source_manifest.resolve()
    migration = args.migration.resolve()
    evidence = args.evidence_output.resolve()
    started = {
        "contract_name": CONTRACT_NAME,
        "status": "STARTED_NOT_COMMITTED",
        "started_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "proposal_path": str(proposal),
        "proposal_sha256": sha256_path(proposal),
        "migration_path": str(migration),
        "migration_sha256": sha256_path(migration),
    }
    atomic_json(evidence, started)
    try:
        rows, input_evidence = validate_inputs(
            proposal, source_manifest, args.expected_proposal_sha256
        )
        load_dotenv(args.env_file, override=False)
        dsn = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
        if not dsn:
            raise RuntimeError("DATABASE_URL_OR_SUPABASE_DB_URL_REQUIRED")
        result = activate(
            dsn,
            rows=rows,
            migration_sql=migration.read_text(),
            input_evidence=input_evidence,
            expected_target_identity_sha256=args.expected_target_identity_sha256,
            expected_game_info_matches=args.expected_game_info_matches,
            expected_cleanroom_rows=args.expected_cleanroom_rows,
            expected_game_info_changes=args.expected_game_info_changes,
            expected_cleanroom_changes=args.expected_cleanroom_changes,
        )
    except Exception as exc:
        failed = {
            **started,
            "status": "FAILED_ROLLED_BACK_OR_NOT_STARTED",
            "completed_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "error_type": type(exc).__name__,
            "error": str(exc),
        }
        atomic_json(evidence, failed)
        print(canonical_json(failed))
        return 1
    completed = {
        **started,
        **result,
        "completed_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    atomic_json(evidence, completed)
    print(canonical_json(completed))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
