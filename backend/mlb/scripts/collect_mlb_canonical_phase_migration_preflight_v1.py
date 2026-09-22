#!/usr/bin/env python3
"""Collect a read-only operational DB preflight for canonical MLB phase activation."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import psycopg
from dotenv import load_dotenv
from psycopg import sql
from psycopg.rows import dict_row


CONTRACT_NAME = "MLB_2026_CANONICAL_PHASE_MIGRATION_ACTIVATION_PREFLIGHT_V1"
ROOT = Path(__file__).resolve().parents[3]
TABLES = (("mlb", "game_info", "game_id"), ("mlb_cleanroom_v1", "games", "game_pk"))
PHASE_COLUMNS = (
    "source_season",
    "source_game_type",
    "season_phase",
    "postseason_round",
    "season_name",
    "source_round",
    "schedule_relationships",
)


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _target_identity(dsn: str) -> dict[str, Any]:
    parsed = urlparse(dsn)
    host = parsed.hostname or ""
    port = parsed.port or 5432
    database = parsed.path.lstrip("/")
    identity = {"engine": "PostgreSQL", "host": host, "port": port, "database": database}
    return {
        **identity,
        "target_identity_sha256": hashlib.sha256(canonical_json(identity).encode()).hexdigest(),
        "credentials_recorded": False,
    }


def _fetch_all(cursor: psycopg.Cursor[Any], query: Any, parameters: Any = None) -> list[dict[str, Any]]:
    cursor.execute(query, parameters)
    return [dict(row) for row in cursor.fetchall()]


def _table_inventory(cursor: psycopg.Cursor[Any], schema: str, table: str, game_key: str) -> dict[str, Any]:
    columns = _fetch_all(
        cursor,
        """
        SELECT ordinal_position, column_name, data_type, udt_name, is_nullable,
               column_default, is_identity, identity_generation, is_generated,
               generation_expression
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema, table),
    )
    constraints = _fetch_all(
        cursor,
        """
        SELECT c.conname AS constraint_name, c.contype AS constraint_type,
               c.condeferrable AS deferrable, c.condeferred AS initially_deferred,
               c.convalidated AS validated, pg_get_constraintdef(c.oid, true) AS definition
        FROM pg_constraint c
        JOIN pg_class r ON r.oid = c.conrelid
        JOIN pg_namespace n ON n.oid = r.relnamespace
        WHERE n.nspname = %s AND r.relname = %s
        ORDER BY c.conname
        """,
        (schema, table),
    )
    indexes = _fetch_all(
        cursor,
        """
        SELECT indexname AS index_name, indexdef AS definition
        FROM pg_indexes
        WHERE schemaname = %s AND tablename = %s
        ORDER BY indexname
        """,
        (schema, table),
    )
    triggers = _fetch_all(
        cursor,
        """
        SELECT t.tgname AS trigger_name, t.tgenabled AS enabled,
               pg_get_triggerdef(t.oid, true) AS definition,
               pg_get_functiondef(t.tgfoid) AS function_definition
        FROM pg_trigger t
        JOIN pg_class r ON r.oid = t.tgrelid
        JOIN pg_namespace n ON n.oid = r.relnamespace
        WHERE n.nspname = %s AND r.relname = %s AND NOT t.tgisinternal
        ORDER BY t.tgname
        """,
        (schema, table),
    )
    policies = _fetch_all(
        cursor,
        """
        SELECT policyname AS policy_name, permissive, roles, cmd, qual, with_check
        FROM pg_policies
        WHERE schemaname = %s AND tablename = %s
        ORDER BY policyname
        """,
        (schema, table),
    )
    relation = sql.Identifier(schema, table)
    key = sql.Identifier(game_key)
    cursor.execute(
        sql.SQL(
            """
            SELECT COUNT(*)::bigint AS row_count,
                   COUNT(DISTINCT {key})::bigint AS distinct_game_pk_count,
                   COUNT(*) FILTER (WHERE {key} IS NULL)::bigint AS null_game_pk_count
            FROM {relation}
            """
        ).format(key=key, relation=relation)
    )
    counts = dict(cursor.fetchone())
    cursor.execute(
        sql.SQL(
            """
            SELECT COUNT(*)::bigint AS duplicate_game_pk_group_count,
                   COALESCE(SUM(row_count - 1), 0)::bigint AS duplicate_game_pk_extra_row_count,
                   COALESCE(MAX(row_count), 0)::bigint AS maximum_rows_per_game_pk
            FROM (
              SELECT {key}, COUNT(*)::bigint AS row_count
              FROM {relation}
              WHERE {key} IS NOT NULL
              GROUP BY {key}
              HAVING COUNT(*) > 1
            ) d
            """
        ).format(key=key, relation=relation)
    )
    duplicate_counts = dict(cursor.fetchone())
    cursor.execute(
        sql.SQL("SELECT {key} AS game_pk FROM {relation} WHERE {key} IS NOT NULL ORDER BY {key}").format(
            key=key, relation=relation
        )
    )
    game_pks = [int(row["game_pk"]) for row in cursor.fetchall()]
    column_names = {str(row["column_name"]) for row in columns}
    phase_present = sorted(
        column_names.intersection(set(PHASE_COLUMNS) | {"game_type_source_sha256"})
    )
    phase_coverage: dict[str, Any] = {
        "phase_columns_present": phase_present,
        "migration_state": "NOT_APPLIED" if not phase_present else "PARTIAL_OR_APPLIED_REQUIRES_REVIEW",
    }
    if set(PHASE_COLUMNS).issubset(column_names):
        cursor.execute(
            sql.SQL(
                """
                SELECT COUNT(*) FILTER (WHERE source_game_type IS NOT NULL)::bigint AS typed_rows,
                       COUNT(*) FILTER (WHERE source_game_type IS NULL)::bigint AS untyped_rows,
                       COUNT(DISTINCT {key}) FILTER (WHERE source_game_type IS NOT NULL)::bigint AS typed_game_pks,
                       COUNT(DISTINCT {key}) FILTER (WHERE source_game_type IS NULL)::bigint AS untyped_game_pks
                FROM {relation}
                """
            ).format(key=key, relation=relation)
        )
        phase_coverage.update(dict(cursor.fetchone()))
    return {
        "schema": schema,
        "table": table,
        "game_pk_column": game_key,
        "owner": None,
        "columns": columns,
        "constraints": constraints,
        "indexes": indexes,
        "triggers": triggers,
        "row_level_security_policies": policies,
        **counts,
        **duplicate_counts,
        "game_pks": game_pks,
        "phase_coverage": phase_coverage,
    }


def collect(dsn: str) -> dict[str, Any]:
    target = _target_identity(dsn)
    with psycopg.connect(dsn, autocommit=False, row_factory=dict_row) as connection:
        with connection.cursor() as cursor:
            cursor.execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            cursor.execute("SET LOCAL statement_timeout = '30s'")
            cursor.execute("SET LOCAL lock_timeout = '2s'")
            cursor.execute(
                """
                SELECT version() AS version, current_database() AS database,
                       current_user AS current_user, session_user AS session_user,
                       current_setting('server_version_num') AS server_version_num,
                       pg_is_in_recovery() AS is_in_recovery,
                       current_setting('transaction_read_only') AS transaction_read_only,
                       current_setting('transaction_isolation') AS transaction_isolation,
                       transaction_timestamp() AS transaction_snapshot_utc
                """
            )
            engine = dict(cursor.fetchone())
            schema_rows = _fetch_all(
                cursor,
                """
                SELECT n.nspname AS schema, r.rolname AS owner
                FROM pg_namespace n JOIN pg_roles r ON r.oid = n.nspowner
                WHERE n.nspname IN ('mlb', 'mlb_cleanroom_v1')
                ORDER BY n.nspname
                """,
            )
            table_owner_rows = _fetch_all(
                cursor,
                """
                SELECT n.nspname AS schema, c.relname AS table, r.rolname AS owner,
                       c.relrowsecurity AS row_level_security_enabled,
                       c.relforcerowsecurity AS row_level_security_forced,
                       pg_total_relation_size(c.oid)::bigint AS total_bytes
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_roles r ON r.oid = c.relowner
                WHERE (n.nspname, c.relname) IN (
                  ('mlb', 'game_info'), ('mlb_cleanroom_v1', 'games')
                )
                ORDER BY n.nspname, c.relname
                """,
            )
            owners = {(row["schema"], row["table"]): row for row in table_owner_rows}
            tables = []
            for schema, table, key in TABLES:
                inventory = _table_inventory(cursor, schema, table, key)
                inventory.update(owners[(schema, table)])
                tables.append(inventory)

            migration_relations = _fetch_all(
                cursor,
                """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_name ILIKE '%migration%'
                ORDER BY table_schema, table_name
                """,
            )
            migration_versions: list[dict[str, Any]] = []
            if any(
                row["table_schema"] == "supabase_migrations"
                and row["table_name"] == "schema_migrations"
                for row in migration_relations
            ):
                migration_versions = _fetch_all(
                    cursor,
                    """
                    SELECT version, name
                    FROM supabase_migrations.schema_migrations
                    WHERE version LIKE '20260921%' OR name ILIKE '%phase%'
                    ORDER BY version, name
                    """,
                )
            cursor.execute("SELECT to_regclass('mlb.canonical_game_phase_v1')::text AS relation")
            canonical_view = dict(cursor.fetchone())

            sessions = _fetch_all(
                cursor,
                """
                SELECT pid, usename, application_name, state, wait_event_type, wait_event,
                       backend_type, xact_start, query_start
                FROM pg_stat_activity
                WHERE datname = current_database() AND pid <> pg_backend_pid()
                ORDER BY pid
                """,
            )
            relation_locks = _fetch_all(
                cursor,
                """
                SELECT l.pid, n.nspname AS schema, c.relname AS relation,
                       l.mode, l.granted, a.application_name, a.state,
                       a.wait_event_type, a.wait_event
                FROM pg_locks l
                JOIN pg_class c ON c.oid = l.relation
                JOIN pg_namespace n ON n.oid = c.relnamespace
                LEFT JOIN pg_stat_activity a ON a.pid = l.pid
                WHERE (n.nspname, c.relname) IN (
                  ('mlb', 'game_info'), ('mlb_cleanroom_v1', 'games')
                )
                  AND l.pid <> pg_backend_pid()
                ORDER BY n.nspname, c.relname, l.pid, l.mode
                """,
            )
            advisory_locks = _fetch_all(
                cursor,
                """
                SELECT pid, mode, granted
                FROM pg_locks
                WHERE locktype = 'advisory' AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
                ORDER BY pid, mode
                """,
            )
            privileges = []
            for schema, table, _key in TABLES:
                cursor.execute(
                    """
                    SELECT %s AS schema, %s AS table,
                           has_schema_privilege(current_user, %s, 'USAGE') AS schema_usage,
                           has_table_privilege(current_user, %s, 'SELECT') AS can_select,
                           has_table_privilege(current_user, %s, 'INSERT') AS can_insert,
                           has_table_privilege(current_user, %s, 'UPDATE') AS can_update,
                           pg_has_role(current_user, c.relowner, 'USAGE') AS can_assume_owner_role
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = %s AND c.relname = %s
                    """,
                    (
                        schema,
                        table,
                        schema,
                        f"{schema}.{table}",
                        f"{schema}.{table}",
                        f"{schema}.{table}",
                        schema,
                        table,
                    ),
                )
                privileges.append(dict(cursor.fetchone()))
            connection.rollback()
    return {
        "contract_name": CONTRACT_NAME,
        "evidence_mode": "OPERATIONAL_DATABASE_REPEATABLE_READ_READ_ONLY",
        "target": target,
        "engine": engine,
        "schemas": schema_rows,
        "tables": tables,
        "migration_state": {
            "canonical_view": canonical_view["relation"],
            "migration_tracking_relations": migration_relations,
            "matching_migration_versions": migration_versions,
        },
        "sessions": sessions,
        "relation_locks": relation_locks,
        "advisory_locks": advisory_locks,
        "current_user_privileges": privileges,
        "database_writes": 0,
        "blocking_locks_acquired": 0,
        "credentials_recorded": False,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--env-file", type=Path, default=ROOT / "backend/.env")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_dotenv(args.env_file, override=False)
    dsn = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL_OR_SUPABASE_DB_URL_REQUIRED")
    report = collect(dsn)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, sort_keys=True, indent=2, default=str) + "\n")
    print(
        canonical_json(
            {
                "contract_name": CONTRACT_NAME,
                "target": report["target"],
                "engine": report["engine"],
                "table_counts": {
                    f"{row['schema']}.{row['table']}": {
                        "rows": row["row_count"],
                        "distinct_game_pks": row["distinct_game_pk_count"],
                        "duplicate_groups": row["duplicate_game_pk_group_count"],
                    }
                    for row in report["tables"]
                },
                "migration_state": report["migration_state"],
                "session_count": len(report["sessions"]),
                "relation_lock_count": len(report["relation_locks"]),
                "database_writes": 0,
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
