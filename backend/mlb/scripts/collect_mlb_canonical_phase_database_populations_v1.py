#!/usr/bin/env python3
"""Collect deterministic, read-only MLB 2026 database game populations."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row


CONTRACT_NAME = "MLB_2026_CANONICAL_PHASE_COVERAGE_AND_TEST_GATE_V1"
TABLES = {
    "mlb.game_info": (
        "game_id", "game_date", ("home_team_id", "away_team_id")
    ),
    "mlb_cleanroom_v1.games": (
        "game_pk", "official_game_date", ("home_team_mlb_id", "away_team_mlb_id")
    ),
    "mlb.public_game_moneyline_predictions": (
        "game_id", "game_date", ("home_team", "away_team")
    ),
    "mlb.public_game_moneyline_outcomes": ("game_id", "game_date", ()),
}
PHASE_COLUMNS = (
    "source_season",
    "source_game_type",
    "season_phase",
    "postseason_round",
    "season_name",
)


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def collect(database_url: str) -> dict[str, Any]:
    output: dict[str, Any] = {
        "contract_name": CONTRACT_NAME,
        "database_transaction": "READ_ONLY",
        "selection_rule": "calendar_year_2026_identity_inventory_only_not_phase_inference",
        "populations": {},
    }
    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        with connection.cursor() as cursor:
            cursor.execute("BEGIN READ ONLY")
            cursor.execute(
                """
                SELECT table_schema, table_name, column_name, data_type
                FROM information_schema.columns
                WHERE (table_schema, table_name) IN (
                    ('mlb','game_info'),
                    ('mlb_cleanroom_v1','games'),
                    ('mlb','public_game_moneyline_predictions'),
                    ('mlb','public_game_moneyline_outcomes')
                )
                ORDER BY table_schema, table_name, ordinal_position
                """
            )
            columns = [dict(row) for row in cursor.fetchall()]
            column_names: dict[str, list[str]] = {}
            for row in columns:
                key = f"{row['table_schema']}.{row['table_name']}"
                column_names.setdefault(key, []).append(str(row["column_name"]))
            output["schema_columns_sha256"] = hashlib.sha256(
                canonical_json(columns).encode()
            ).hexdigest()

            for table, (id_column, date_column, identity_columns) in TABLES.items():
                identity_select = "".join(
                    f", {column}::text AS {column}" for column in identity_columns
                )
                cursor.execute(
                    f"""
                    SELECT {id_column} AS game_pk, {date_column}::text AS game_date
                           {identity_select}
                    FROM {table}
                    WHERE {date_column} >= DATE '2026-01-01'
                      AND {date_column} < DATE '2027-01-01'
                    ORDER BY {id_column}, {date_column}
                    """
                )
                raw_rows = [dict(row) for row in cursor.fetchall()]
                dates_by_game: dict[int, set[str]] = {}
                variants_by_game: dict[int, set[str]] = {}
                for row in raw_rows:
                    game_pk = int(row["game_pk"])
                    dates_by_game.setdefault(game_pk, set()).add(str(row["game_date"]))
                    variant = {
                        "game_date": str(row["game_date"]),
                        **{column: row.get(column) for column in identity_columns},
                    }
                    variants_by_game.setdefault(game_pk, set()).add(canonical_json(variant))
                rows = [
                    {
                        "game_pk": game_pk,
                        "game_dates": sorted(dates),
                        "identity_variants": [
                            json.loads(value)
                            for value in sorted(variants_by_game[game_pk])
                        ],
                    }
                    for game_pk, dates in sorted(dates_by_game.items())
                ]
                output["populations"][table] = {
                    "raw_row_count": len(raw_rows),
                    "distinct_game_pk_count": len(rows),
                    "earliest_date": min(
                        (date for row in rows for date in row["game_dates"]),
                        default=None,
                    ),
                    "latest_date": max(
                        (date for row in rows for date in row["game_dates"]),
                        default=None,
                    ),
                    "duplicate_date_conflicts": [
                        row for row in rows if len(row["game_dates"]) > 1
                    ],
                    "duplicate_identity_conflicts": [
                        row for row in rows if len(row["identity_variants"]) > 1
                    ],
                    "phase_columns_present": sorted(
                        set(PHASE_COLUMNS).intersection(column_names.get(table, []))
                    ),
                    "rowset_sha256": hashlib.sha256(
                        canonical_json(raw_rows).encode()
                    ).hexdigest(),
                    "rows": rows,
                }
            connection.rollback()
    return output


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    database_url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if not database_url:
        raise SystemExit("DATABASE_URL_OR_SUPABASE_DB_URL_REQUIRED")
    report = collect(database_url)
    rendered = json.dumps(report, sort_keys=True, indent=2) + "\n"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(rendered, encoding="utf-8")
    print(
        canonical_json(
            {
                "contract_name": CONTRACT_NAME,
                "database_transaction": "READ_ONLY",
                "populations": {
                    name: {
                        "raw_row_count": row["raw_row_count"],
                        "distinct_game_pk_count": row["distinct_game_pk_count"],
                        "earliest_date": row["earliest_date"],
                        "latest_date": row["latest_date"],
                    }
                    for name, row in report["populations"].items()
                },
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
