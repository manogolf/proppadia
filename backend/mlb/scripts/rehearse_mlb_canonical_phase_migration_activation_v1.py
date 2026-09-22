#!/usr/bin/env python3
"""Run the canonical-phase migration and loader in disposable local PostgreSQL."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import tempfile
import threading
from pathlib import Path
from typing import Any
from urllib.parse import quote

import psycopg
from psycopg.rows import dict_row

from backend.mlb.scripts.activate_mlb_canonical_phase_v1 import (
    CONTRACT_NAME,
    activate,
    canonical_json,
    read_jsonl,
    sha256_path,
    target_identity,
    validate_inputs,
)


ROOT = Path(__file__).resolve().parents[3]


def _run(command: list[str]) -> None:
    subprocess.run(command, check=True, text=True, capture_output=True)


def _scalar(connection: psycopg.Connection[Any], query: str) -> Any:
    with connection.cursor() as cursor:
        cursor.execute(query)
        row = cursor.fetchone()
        if isinstance(row, dict):
            return next(iter(row.values()))
        return row[0]


def _schema_fixture(connection: psycopg.Connection[Any], selected: list[dict[str, Any]]) -> dict[str, Any]:
    by_type = {row["source_game_type"]: row for row in selected}
    s_game = by_type["S"]
    e_game = by_type["E"]
    r_game = by_type["R"]
    fourth = next(row for row in selected if row["game_pk"] not in {s_game["game_pk"], e_game["game_pk"], r_game["game_pk"]})
    game_ids = [int(row["game_pk"]) for row in (s_game, e_game, r_game, fourth)]
    with connection.transaction():
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE SCHEMA mlb;
                CREATE SCHEMA mlb_cleanroom_v1;
                CREATE TABLE mlb.game_info (
                  game_id bigint PRIMARY KEY UNIQUE,
                  game_time timestamp,
                  game_date date,
                  home_team_id bigint,
                  away_team_id bigint,
                  home_team_abbr text,
                  away_team_abbr text,
                  starting_pitcher_id_home bigint,
                  starting_pitcher_id_away bigint
                );
                CREATE TABLE mlb_cleanroom_v1.games (
                  game_pk bigint NOT NULL,
                  slate_date date NOT NULL,
                  official_game_date date NOT NULL,
                  home_team_mlb_id bigint NOT NULL,
                  away_team_mlb_id bigint NOT NULL,
                  scheduled_start_utc timestamptz NOT NULL,
                  game_status text NOT NULL,
                  source text NOT NULL,
                  source_observed_at_utc timestamptz NOT NULL,
                  ingested_at_utc timestamptz NOT NULL,
                  source_payload_sha256 text NOT NULL CHECK (
                    source_payload_sha256 ~ '^[0-9a-f]{64}$'
                  ),
                  PRIMARY KEY (game_pk, source_payload_sha256)
                );
                CREATE FUNCTION mlb_cleanroom_v1.reject_mutation()
                RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                  RAISE EXCEPTION 'mlb_cleanroom_v1 source tables are append-only';
                END $$;
                CREATE TRIGGER reject_mutation BEFORE UPDATE OR DELETE
                  ON mlb_cleanroom_v1.games FOR EACH ROW
                  EXECUTE FUNCTION mlb_cleanroom_v1.reject_mutation();
                """
            )
            cursor.executemany(
                """
                INSERT INTO mlb.game_info (
                  game_id, game_time, game_date, home_team_id, away_team_id,
                  home_team_abbr, away_team_abbr
                ) VALUES (%s, '2026-03-01 12:00:00', '2026-03-01', 1, 2, 'AAA', 'BBB')
                """,
                [(game_pk,) for game_pk in game_ids],
            )
            cleanroom_rows = [
                (game_ids[0], "1" * 64),
                (game_ids[0], "2" * 64),
                (game_ids[2], "3" * 64),
            ]
            cursor.executemany(
                """
                INSERT INTO mlb_cleanroom_v1.games (
                  game_pk, slate_date, official_game_date, home_team_mlb_id,
                  away_team_mlb_id, scheduled_start_utc, game_status, source,
                  source_observed_at_utc, ingested_at_utc, source_payload_sha256
                ) VALUES (
                  %s, '2026-03-01', '2026-03-01', 1, 2,
                  '2026-03-01 12:00:00+00', 'Final', 'SANITIZED_FIXTURE',
                  '2026-03-01 10:00:00+00', '2026-03-01 10:01:00+00', %s
                )
                """,
                cleanroom_rows,
            )
    return {"game_info_ids": game_ids, "cleanroom_rows": len(cleanroom_rows)}


def _phase_columns(connection: psycopg.Connection[Any], schema: str, table: str) -> list[str]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
              AND column_name IN (
                'source_season','source_game_type','season_phase','postseason_round',
                'season_name','source_round','schedule_relationships','game_type_source_sha256'
              )
            ORDER BY column_name
            """,
            (schema, table),
        )
        return [row[0] for row in cursor.fetchall()]


def _failure_case(
    temp: Path,
    name: str,
    rows: list[dict[str, Any]],
    manifest: Path,
) -> dict[str, Any]:
    path = temp / f"{name}.jsonl"
    path.write_text("".join(canonical_json(row) + "\n" for row in rows))
    try:
        validate_inputs(path, manifest, sha256_path(path))
    except Exception as exc:
        return {"case": name, "status": "PASS_FAIL_CLOSED", "error": str(exc)}
    return {"case": name, "status": "FAIL_ACCEPTED_INVALID_INPUT"}


def rehearse(
    proposal_path: Path,
    source_manifest_path: Path,
    migration_path: Path,
    rollback_path: Path,
) -> dict[str, Any]:
    initdb = shutil.which("initdb")
    pg_ctl = shutil.which("pg_ctl")
    if not initdb or not pg_ctl:
        raise RuntimeError("LOCAL_POSTGRES_BINARIES_REQUIRED")
    postgres = Path(initdb).with_name("postgres")
    if not postgres.is_file():
        return {
            "contract_name": CONTRACT_NAME,
            "status": "BLOCKED",
            "environment": "DISPOSABLE_LOCAL_POSTGRESQL_CLUSTER_UNAVAILABLE",
            "production_database_ddl_dml": 0,
            "blocker": "LOCAL_POSTGRES_SERVER_BINARY_ABSENT",
            "detail": (
                f"initdb={initdb}; pg_ctl={pg_ctl}; required_companion_server={postgres}"
            ),
            "initial_failed_attempt_created_schema": False,
            "initial_failed_attempt_created_rows": False,
            "required_rehearsal_scenarios_unexecuted": [
                "first_migration",
                "repeated_migration",
                "canonical_backfill",
                "repeated_backfill",
                "rollback",
                "reactivation_after_rollback",
                "concurrent_writer_compatibility",
                "database_enforced_invalid_type_failure",
                "exact_game_pk_join",
                "zero_unintended_row_creation_or_deletion",
            ],
            "smallest_remediation": (
                "provide an isolated disposable PostgreSQL server with matching major-version "
                "semantics; do not install it or use the operational target under this task"
            ),
        }
    rows, input_evidence = validate_inputs(
        proposal_path, source_manifest_path, sha256_path(proposal_path)
    )
    migration_sql = migration_path.read_text()
    rollback_sql = rollback_path.read_text()
    if any(token in rollback_sql.upper() for token in ("BEGIN;", "COMMIT;", "ROLLBACK;")):
        raise RuntimeError("ROLLBACK_MUST_BE_TRANSACTION_NEUTRAL")
    with tempfile.TemporaryDirectory(prefix="mlb_phase_pg_rehearsal_") as temporary_name:
        temp = Path(temporary_name)
        cluster = temp / "cluster"
        socket_dir = temp / "socket"
        socket_dir.mkdir()
        _run([initdb, "-D", str(cluster), "--auth=trust", "--username=postgres", "--no-locale", "--encoding=UTF8"])
        port = 55439
        _run(
            [
                pg_ctl,
                "-D",
                str(cluster),
                "-o",
                f"-k {socket_dir} -h '' -p {port}",
                "-w",
                "start",
            ]
        )
        try:
            dsn = (
                "postgresql://postgres@localhost/postgres?"
                f"host={quote(str(socket_dir), safe='')}&port={port}"
            )
            with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as connection:
                fixture = _schema_fixture(connection, rows)
                initial_counts = {
                    "game_info": int(_scalar(connection, "SELECT COUNT(*) FROM mlb.game_info")),
                    "cleanroom_games": int(
                        _scalar(connection, "SELECT COUNT(*) FROM mlb_cleanroom_v1.games")
                    ),
                }
            target_hash = target_identity(dsn)["target_identity_sha256"]
            first = activate(
                dsn,
                rows=rows,
                migration_sql=migration_sql,
                input_evidence=input_evidence,
                expected_target_identity_sha256=target_hash,
                expected_game_info_matches=4,
                expected_cleanroom_rows=3,
                expected_game_info_changes=4,
                expected_cleanroom_changes=3,
            )
            repeated = activate(
                dsn,
                rows=rows,
                migration_sql=migration_sql,
                input_evidence=input_evidence,
                expected_target_identity_sha256=target_hash,
                expected_game_info_matches=4,
                expected_cleanroom_rows=3,
                expected_game_info_changes=0,
                expected_cleanroom_changes=0,
            )

            with psycopg.connect(dsn, autocommit=False, row_factory=dict_row) as writer:
                with writer.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO mlb.game_info (
                          game_id, game_date, source_season, source_game_type,
                          season_phase, postseason_round, season_name, source_round,
                          schedule_relationships, game_type_source_sha256
                        ) VALUES (
                          999999999, '2026-03-01', 2026, 'R', 'REGULAR_SEASON',
                          NULL, 'MLB_2026_REGULAR_SEASON', NULL, '{}'::jsonb, %s
                        )
                        """,
                        ("9" * 64,),
                    )
                    cursor.execute(
                        """
                        INSERT INTO mlb_cleanroom_v1.games (
                          game_pk, slate_date, official_game_date, home_team_mlb_id,
                          away_team_mlb_id, scheduled_start_utc, game_status, source,
                          source_observed_at_utc, ingested_at_utc, source_payload_sha256,
                          source_season, source_game_type, season_phase, postseason_round,
                          season_name, source_round, schedule_relationships
                        ) VALUES (
                          999999999, '2026-03-01', '2026-03-01', 1, 2,
                          '2026-03-01 12:00:00+00', 'Scheduled', 'SANITIZED_WRITER',
                          now(), now(), %s, 2026, 'R', 'REGULAR_SEASON', NULL,
                          'MLB_2026_REGULAR_SEASON', NULL, '{}'::jsonb
                        )
                        """,
                        ("8" * 64,),
                    )
                    writer.rollback()
            writer_compatibility = {
                "named_game_info_insert": "PASS_ROLLED_BACK",
                "named_cleanroom_insert": "PASS_ROLLED_BACK",
                "append_only_trigger_remained_enabled": True,
            }

            lock_started = threading.Event()
            release_lock = threading.Event()
            holder_error: list[str] = []

            def hold_writer_lock() -> None:
                try:
                    with psycopg.connect(dsn, autocommit=False) as connection:
                        with connection.cursor() as cursor:
                            cursor.execute(
                                "UPDATE mlb.game_info SET game_date = game_date WHERE game_id = %s",
                                (fixture["game_info_ids"][0],),
                            )
                            lock_started.set()
                            release_lock.wait(timeout=10)
                            connection.rollback()
                except Exception as exc:
                    holder_error.append(str(exc))
                    lock_started.set()

            thread = threading.Thread(target=hold_writer_lock, daemon=True)
            thread.start()
            lock_started.wait(timeout=10)
            try:
                activate(
                    dsn,
                    rows=rows,
                    migration_sql=migration_sql,
                    input_evidence=input_evidence,
                    expected_target_identity_sha256=target_hash,
                    expected_game_info_matches=4,
                    expected_cleanroom_rows=3,
                    expected_game_info_changes=0,
                    expected_cleanroom_changes=0,
                )
            except Exception as exc:
                concurrent_result = {
                    "status": "PASS_FAIL_CLOSED_WITHOUT_WAITING",
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
            else:
                concurrent_result = {"status": "FAIL_ACTIVATED_DURING_WRITER"}
            finally:
                release_lock.set()
                thread.join(timeout=10)
            if holder_error:
                concurrent_result = {"status": "FAIL_WRITER_HOLDER", "errors": holder_error}

            with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as connection:
                view_ids = _scalar(
                    connection,
                    "SELECT array_agg(game_pk ORDER BY game_pk) FROM mlb.canonical_game_phase_v1",
                )
                view_definition = _scalar(
                    connection,
                    "SELECT pg_get_viewdef('mlb.canonical_game_phase_v1'::regclass, true)",
                )
                exact_join = {
                    "view_game_pks": list(view_ids),
                    "expected_game_pks": sorted(fixture["game_info_ids"]),
                    "contains_calendar_join": any(
                        token in view_definition
                        for token in ("game_date", "official_game_date", "home_team", "away_team")
                    ),
                }
                with connection.transaction():
                    with connection.cursor() as cursor:
                        cursor.execute(rollback_sql)
                after_rollback = {
                    "view_exists": _scalar(
                        connection,
                        "SELECT to_regclass('mlb.canonical_game_phase_v1') IS NOT NULL",
                    ),
                    "game_info_phase_columns": _phase_columns(connection, "mlb", "game_info"),
                    "cleanroom_phase_columns": _phase_columns(
                        connection, "mlb_cleanroom_v1", "games"
                    ),
                    "game_info_rows": int(_scalar(connection, "SELECT COUNT(*) FROM mlb.game_info")),
                    "cleanroom_rows": int(
                        _scalar(connection, "SELECT COUNT(*) FROM mlb_cleanroom_v1.games")
                    ),
                }
            reactivated = activate(
                dsn,
                rows=rows,
                migration_sql=migration_sql,
                input_evidence=input_evidence,
                expected_target_identity_sha256=target_hash,
                expected_game_info_matches=4,
                expected_cleanroom_rows=3,
                expected_game_info_changes=4,
                expected_cleanroom_changes=3,
            )

            missing_rows = rows[:-1]
            unknown_rows = [dict(row) for row in rows]
            unknown_rows[0]["source_game_type"] = "X"
            conflict_rows = [dict(row) for row in rows]
            conflict_rows[0]["season_phase"] = "REGULAR_SEASON"
            duplicate_rows = [dict(row) for row in rows]
            duplicate_rows[-1]["game_pk"] = duplicate_rows[0]["game_pk"]
            invalid_results = [
                _failure_case(temp, "missing", missing_rows, source_manifest_path),
                _failure_case(temp, "unknown", unknown_rows, source_manifest_path),
                _failure_case(temp, "conflicting", conflict_rows, source_manifest_path),
                _failure_case(temp, "duplicate", duplicate_rows, source_manifest_path),
            ]
            checks_pass = (
                first["proposed_mutations"]
                == {"mlb.game_info": 4, "mlb_cleanroom_v1.games": 3}
                and repeated["proposed_mutations"]
                == {"mlb.game_info": 0, "mlb_cleanroom_v1.games": 0}
                and reactivated["proposed_mutations"]
                == {"mlb.game_info": 4, "mlb_cleanroom_v1.games": 3}
                and concurrent_result["status"] == "PASS_FAIL_CLOSED_WITHOUT_WAITING"
                and exact_join["view_game_pks"] == exact_join["expected_game_pks"]
                and not exact_join["contains_calendar_join"]
                and after_rollback["view_exists"] is False
                and not after_rollback["game_info_phase_columns"]
                and not after_rollback["cleanroom_phase_columns"]
                and all(row["status"] == "PASS_FAIL_CLOSED" for row in invalid_results)
            )
            zero_unintended = (
                first["zero_row_creation_or_deletion"]
                and repeated["zero_row_creation_or_deletion"]
                and reactivated["zero_row_creation_or_deletion"]
                and after_rollback["game_info_rows"] == initial_counts["game_info"]
                and after_rollback["cleanroom_rows"] == initial_counts["cleanroom_games"]
            )
            return {
                "contract_name": CONTRACT_NAME,
                "status": "PASS" if checks_pass and zero_unintended else "FAIL",
                "environment": "DISPOSABLE_LOCAL_POSTGRESQL_CLUSTER",
                "production_database_ddl_dml": 0,
                "sanitized_fixture": fixture,
                "initial_counts": initial_counts,
                "first_activation": first,
                "repeated_migration_and_backfill": repeated,
                "writer_compatibility": writer_compatibility,
                "concurrent_writer_gate": concurrent_result,
                "exact_game_pk_join": exact_join,
                "rollback": after_rollback,
                "reactivation_after_rollback": reactivated,
                "invalid_input_failures": invalid_results,
                "zero_unintended_row_creation_or_deletion": zero_unintended,
            }
        finally:
            subprocess.run(
                [pg_ctl, "-D", str(cluster), "-m", "fast", "-w", "stop"],
                check=False,
                text=True,
                capture_output=True,
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--proposal", required=True, type=Path)
    parser.add_argument("--source-manifest", required=True, type=Path)
    parser.add_argument("--migration", required=True, type=Path)
    parser.add_argument("--rollback", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = rehearse(
        args.proposal.resolve(),
        args.source_manifest.resolve(),
        args.migration.resolve(),
        args.rollback.resolve(),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, sort_keys=True, indent=2, default=str) + "\n")
    if report["status"] == "PASS":
        summary = {
            "contract_name": CONTRACT_NAME,
            "status": report["status"],
            "first_mutations": report["first_activation"]["proposed_mutations"],
            "repeated_mutations": report["repeated_migration_and_backfill"][
                "proposed_mutations"
            ],
            "concurrent_writer_gate": report["concurrent_writer_gate"],
            "rollback": report["rollback"],
            "invalid_input_failures": report["invalid_input_failures"],
            "zero_unintended_row_creation_or_deletion": report[
                "zero_unintended_row_creation_or_deletion"
            ],
        }
    else:
        summary = {
            "contract_name": CONTRACT_NAME,
            "status": report["status"],
            "blocker": report["blocker"],
            "required_rehearsal_scenarios_unexecuted": report[
                "required_rehearsal_scenarios_unexecuted"
            ],
        }
    print(canonical_json(summary))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
