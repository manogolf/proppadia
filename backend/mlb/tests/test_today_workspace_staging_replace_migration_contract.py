from __future__ import annotations

import re
from pathlib import Path

MIGRATIONS = Path(__file__).resolve().parents[1] / "sql" / "migrations"
FORWARD = MIGRATIONS / "20261019_mlb_today_workspace_staging_replace.sql"
ROLLBACK = MIGRATIONS / "20261019_mlb_today_workspace_staging_replace_rollback.sql"
SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "build_mlb_today_workspace.py"


def _insert_columns(sql: str, table: str) -> list[str]:
    match = re.search(rf"INSERT INTO mlb\.{table} \((.*?)\)", sql, flags=re.DOTALL)
    assert match, table
    return [column.strip() for column in match.group(1).split(",")]


def test_function_is_fixed_definer_with_ingest_only_execute():
    sql = FORWARD.read_text()
    assert "NOT APPLIED" in sql
    assert "SECURITY DEFINER" in sql
    assert "SET search_path = pg_catalog, pg_temp" in sql
    assert "REVOKE ALL ON FUNCTION mlb.replace_today_workspace_staging(date, jsonb, jsonb, jsonb) FROM PUBLIC;" in sql
    grants = re.findall(r"GRANT [^;]+;", sql)
    assert grants == [
        "GRANT EXECUTE ON FUNCTION mlb.replace_today_workspace_staging(date, jsonb, jsonb, jsonb) TO proppadia_mlb_ingest;"
    ]
    assert "EXECUTE format" not in sql and "EXECUTE '" not in sql
    deletes = re.findall(r"DELETE FROM (mlb\.\w+) WHERE slate_date = p_slate_date;", sql)
    assert sorted(deletes) == ["mlb.today_odds_book_rows", "mlb.today_slate_rows", "mlb.today_wide_rows"]
    assert sql.count("DELETE FROM") == 3
    assert "(e ->> 'slate_date')::date <> p_slate_date" in sql


def test_function_inserts_exactly_the_columns_the_daily_build_writes():
    from backend.mlb.scripts import build_mlb_today_workspace as workspace

    sql = FORWARD.read_text()
    expected = {
        "today_slate_rows": workspace.TODAY_SLATE_ROW_COLUMNS,
        "today_wide_rows": workspace.TODAY_WIDE_ROW_COLUMNS,
        "today_odds_book_rows": workspace.TODAY_ODDS_BOOK_ROW_COLUMNS,
    }
    for table, columns in expected.items():
        assert _insert_columns(sql, table) == list(columns), table


def test_rollback_removes_only_the_function():
    sql = ROLLBACK.read_text()
    assert "DROP FUNCTION mlb.replace_today_workspace_staging(date, jsonb, jsonb, jsonb);" in sql
    assert "DELETE" not in sql and "TRUNCATE" not in sql and "DROP TABLE" not in sql
