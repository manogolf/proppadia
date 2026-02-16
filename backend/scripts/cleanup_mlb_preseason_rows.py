#!/usr/bin/env python3
"""Delete preseason MLB rows from prediction/training tables for a date window."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from typing import Dict, Sequence

from backend.shared.db.pg import pg_connect


def _row_int(row, key: str) -> int:
    if not row:
        return 0
    if isinstance(row, dict):
        return int(row.get(key) or 0)
    return int(row[0] or 0)


def _validate_iso(raw: str, label: str) -> str:
    value = str(raw or "").strip()
    try:
        date.fromisoformat(value)
    except Exception as e:
        raise ValueError(f"{label} must be YYYY-MM-DD") from e
    return value


def _count_rows(
    from_date: str,
    to_date: str,
    *,
    include_user_added: bool,
) -> Dict[str, int]:
    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)::int AS n
            FROM model_training_props
            WHERE game_date BETWEEN %s::date AND %s::date
              AND prop_source = 'stat_derived'
            """,
            (from_date, to_date),
        )
        mtp = _row_int(cur.fetchone(), "n")

        if include_user_added:
            cur.execute(
                """
                SELECT COUNT(*)::int AS n
                FROM player_props
                WHERE game_date BETWEEN %s::date AND %s::date
                  AND prop_source IN ('stat_derived', 'user_added')
                """,
                (from_date, to_date),
            )
        else:
            cur.execute(
                """
                SELECT COUNT(*)::int AS n
                FROM player_props
                WHERE game_date BETWEEN %s::date AND %s::date
                  AND prop_source = 'stat_derived'
                """,
                (from_date, to_date),
            )
        pp = _row_int(cur.fetchone(), "n")
    return {
        "model_training_props": mtp,
        "player_props": pp,
    }


def _delete_rows(
    from_date: str,
    to_date: str,
    *,
    include_user_added: bool,
) -> Dict[str, int]:
    with pg_connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM model_training_props
            WHERE game_date BETWEEN %s::date AND %s::date
              AND prop_source = 'stat_derived'
            """,
            (from_date, to_date),
        )
        mtp_deleted = int(cur.rowcount or 0)

        if include_user_added:
            cur.execute(
                """
                DELETE FROM player_props
                WHERE game_date BETWEEN %s::date AND %s::date
                  AND prop_source IN ('stat_derived', 'user_added')
                """,
                (from_date, to_date),
            )
        else:
            cur.execute(
                """
                DELETE FROM player_props
                WHERE game_date BETWEEN %s::date AND %s::date
                  AND prop_source = 'stat_derived'
                """,
                (from_date, to_date),
            )
        pp_deleted = int(cur.rowcount or 0)
        conn.commit()
    return {
        "model_training_props": mtp_deleted,
        "player_props": pp_deleted,
    }


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Cleanup preseason MLB rows by date range.")
    ap.add_argument("--from-date", required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--to-date", required=True, help="YYYY-MM-DD (inclusive)")
    ap.add_argument(
        "--include-user-added",
        action="store_true",
        help="Also delete player_props rows where prop_source='user_added' in the date window.",
    )
    ap.add_argument(
        "--apply",
        action="store_true",
        help="Apply deletes. Without this flag the command runs as dry-run only.",
    )
    args = ap.parse_args(list(argv) if argv is not None else sys.argv[1:])

    try:
        from_date = _validate_iso(args.from_date, "from-date")
        to_date = _validate_iso(args.to_date, "to-date")
    except ValueError as e:
        print(json.dumps({"ok": False, "status": "fail", "error": str(e)}, indent=2))
        return 2
    if from_date > to_date:
        print(json.dumps({"ok": False, "status": "fail", "error": "from-date must be <= to-date"}, indent=2))
        return 2

    include_user_added = bool(args.include_user_added)
    dry_run = not bool(args.apply)
    preview = _count_rows(from_date, to_date, include_user_added=include_user_added)

    payload = {
        "ok": True,
        "status": "pass",
        "mode": "dry-run" if dry_run else "apply",
        "from_date": from_date,
        "to_date": to_date,
        "include_user_added": include_user_added,
        "preview_counts": preview,
    }

    if dry_run:
        print(json.dumps(payload, indent=2))
        return 0

    deleted = _delete_rows(from_date, to_date, include_user_added=include_user_added)
    payload["deleted_counts"] = deleted
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
