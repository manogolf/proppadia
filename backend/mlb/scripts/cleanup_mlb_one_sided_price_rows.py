#!/usr/bin/env python3
"""Preview/apply cleanup for one-sided MLB price rows in DB tables."""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Sequence

import psycopg

from backend.shared.db.pg import pg_connect


_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _parse_csv(raw: str) -> List[str]:
    return [tok.strip() for tok in str(raw or "").split(",") if tok.strip()]


def _assert_ident(value: str, *, label: str) -> str:
    text = str(value or "").strip()
    if not text or not _IDENT_RE.fullmatch(text):
        raise ValueError(f"invalid {label}: {value!r}")
    return text


def _table_has_column(conn, schema: str, table: str, column: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
              AND column_name = %s
            LIMIT 1
            """,
            (schema, table, column),
        )
        return cur.fetchone() is not None


def _discover_tables(conn, schema: str) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.table_name
            FROM information_schema.tables t
            WHERE t.table_schema = %s
              AND t.table_type = 'BASE TABLE'
              AND EXISTS (
                SELECT 1
                FROM information_schema.columns c
                WHERE c.table_schema = t.table_schema
                  AND c.table_name = t.table_name
                  AND c.column_name = 'price_over_american'
              )
              AND EXISTS (
                SELECT 1
                FROM information_schema.columns c
                WHERE c.table_schema = t.table_schema
                  AND c.table_name = t.table_name
                  AND c.column_name = 'price_under_american'
              )
            ORDER BY t.table_name
            """,
            (schema,),
        )
        rows = cur.fetchall() or []
    out: List[str] = []
    for row in rows:
        value = row["table_name"] if isinstance(row, dict) else row[0]
        if value:
            out.append(str(value))
    return out


def _table_preview(conn, schema: str, table: str) -> Dict[str, Any]:
    q = psycopg.sql.SQL(
        """
        SELECT
          COUNT(*)::bigint AS total_rows,
          COALESCE(SUM(CASE WHEN (price_over_american IS NULL) <> (price_under_american IS NULL) THEN 1 ELSE 0 END), 0)::bigint AS one_sided_rows,
          COALESCE(SUM(CASE WHEN price_over_american IS NOT NULL AND price_under_american IS NOT NULL THEN 1 ELSE 0 END), 0)::bigint AS two_sided_rows,
          COALESCE(SUM(CASE WHEN price_over_american IS NULL AND price_under_american IS NULL THEN 1 ELSE 0 END), 0)::bigint AS no_price_rows
        FROM {}.{}
        """
    ).format(psycopg.sql.Identifier(schema), psycopg.sql.Identifier(table))
    with conn.cursor() as cur:
        cur.execute(q)
        row = cur.fetchone() or {}

    total_rows = int((row.get("total_rows") if isinstance(row, dict) else row[0]) or 0)
    one_sided_rows = int((row.get("one_sided_rows") if isinstance(row, dict) else row[1]) or 0)
    two_sided_rows = int((row.get("two_sided_rows") if isinstance(row, dict) else row[2]) or 0)
    no_price_rows = int((row.get("no_price_rows") if isinstance(row, dict) else row[3]) or 0)

    payload: Dict[str, Any] = {
        "table": table,
        "total_rows": total_rows,
        "one_sided_rows": one_sided_rows,
        "two_sided_rows": two_sided_rows,
        "no_price_rows": no_price_rows,
    }

    if _table_has_column(conn, schema, table, "prop_source") and one_sided_rows > 0:
        by_source_q = psycopg.sql.SQL(
            """
            SELECT COALESCE(CAST(prop_source AS text), '<null>') AS prop_source, COUNT(*)::bigint AS rows
            FROM {}.{}
            WHERE (price_over_american IS NULL) <> (price_under_american IS NULL)
            GROUP BY 1
            ORDER BY rows DESC, prop_source
            """
        ).format(psycopg.sql.Identifier(schema), psycopg.sql.Identifier(table))
        with conn.cursor() as cur:
            cur.execute(by_source_q)
            rows = cur.fetchall() or []
        payload["one_sided_by_prop_source"] = [
            {
                "prop_source": str(r.get("prop_source") if isinstance(r, dict) else r[0]),
                "rows": int((r.get("rows") if isinstance(r, dict) else r[1]) or 0),
            }
            for r in rows
        ]

    return payload


def _delete_one_sided(conn, schema: str, table: str) -> int:
    q = psycopg.sql.SQL(
        """
        DELETE FROM {}.{}
        WHERE (price_over_american IS NULL) <> (price_under_american IS NULL)
        """
    ).format(psycopg.sql.Identifier(schema), psycopg.sql.Identifier(table))
    with conn.cursor() as cur:
        cur.execute(q)
        return int(cur.rowcount or 0)


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Cleanup one-sided MLB price rows in DB tables.")
    ap.add_argument("--schema", default="mlb", help="Schema to scan (default: mlb).")
    ap.add_argument("--tables", default="", help="Optional comma-separated table allowlist.")
    ap.add_argument("--apply", action="store_true", help="Apply deletes. Default is preview-only.")
    ap.add_argument("--out-json", default="", help="Optional path to write JSON payload.")
    args = ap.parse_args(list(argv) if argv is not None else sys.argv[1:])

    try:
        schema = _assert_ident(args.schema, label="schema")
        table_filter = [_assert_ident(t, label="table") for t in _parse_csv(args.tables)]
    except ValueError as exc:
        payload = {"ok": False, "status": "fail", "error": str(exc)}
        print(json.dumps(payload, indent=2))
        return 2

    with pg_connect() as conn:
        discovered = _discover_tables(conn, schema)
        if table_filter:
            selected = [t for t in discovered if t in set(table_filter)]
            missing = sorted(set(table_filter) - set(selected))
        else:
            selected = discovered
            missing = []

        preview = [_table_preview(conn, schema, table) for table in selected]
        totals = {
            "tables_scanned": int(len(selected)),
            "rows_total": int(sum(int(x.get("total_rows") or 0) for x in preview)),
            "rows_one_sided": int(sum(int(x.get("one_sided_rows") or 0) for x in preview)),
            "rows_two_sided": int(sum(int(x.get("two_sided_rows") or 0) for x in preview)),
            "rows_no_price": int(sum(int(x.get("no_price_rows") or 0) for x in preview)),
        }

        payload: Dict[str, Any] = {
            "ok": True,
            "status": "pass",
            "mode": "apply" if bool(args.apply) else "dry-run",
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "schema": schema,
            "tables_discovered": discovered,
            "tables_selected": selected,
            "tables_missing_from_filter": missing,
            "totals_before": totals,
            "by_table_before": preview,
        }

        if bool(args.apply):
            deleted_by_table: List[Dict[str, Any]] = []
            deleted_total = 0
            for table in selected:
                deleted = _delete_one_sided(conn, schema, table)
                deleted_total += int(deleted)
                deleted_by_table.append({"table": table, "deleted_one_sided_rows": int(deleted)})
            conn.commit()
            payload["deleted"] = {
                "rows_one_sided_deleted": int(deleted_total),
                "by_table": deleted_by_table,
            }
            payload["by_table_after"] = [_table_preview(conn, schema, table) for table in selected]

    out_json_raw = str(args.out_json or "").strip()
    if out_json_raw:
        out_path = Path(out_json_raw).expanduser()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        payload["out_json"] = str(out_path)

    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

