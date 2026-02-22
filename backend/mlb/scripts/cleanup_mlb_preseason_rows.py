#!/usr/bin/env python3
"""Delete preseason MLB rows from prediction/training tables for a date window."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from typing import Dict, List, Sequence

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


def _parse_game_types(raw: str) -> List[str]:
    if not str(raw or "").strip():
        return []
    out: List[str] = []
    for token in str(raw).split(","):
        value = token.strip().upper()
        if not value:
            continue
        if len(value) > 3:
            raise ValueError("game-types must be comma-separated MLB gameType codes (for example: S,R)")
        out.append(value)
    if not out:
        return []
    return sorted(set(out))


def _table_has_column(conn, table_name: str, column_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'mlb'
              AND table_name = %s
              AND column_name = %s
            LIMIT 1
            """,
            (table_name, column_name),
        )
        return cur.fetchone() is not None


def _count_rows(
    from_date: str,
    to_date: str,
    *,
    include_user_added: bool,
    game_types: Sequence[str],
) -> Dict[str, object]:
    with pg_connect() as conn, conn.cursor() as cur:
        mtp_has_game_type = _table_has_column(conn, "model_training_props", "game_type")
        pp_has_game_type = _table_has_column(conn, "player_props", "game_type")

        mtp_sql = """
            SELECT COUNT(*)::int AS n
            FROM mlb.model_training_props
            WHERE game_date BETWEEN %s::date AND %s::date
              AND prop_source = 'mlb_api'
            """
        mtp_params: List[object] = [from_date, to_date]
        if game_types and mtp_has_game_type:
            mtp_sql += " AND UPPER(COALESCE(game_type, '')) = ANY(%s)"
            mtp_params.append(list(game_types))

        cur.execute(mtp_sql, tuple(mtp_params))
        mtp = _row_int(cur.fetchone(), "n")

        pp_sql = """
            SELECT COUNT(*)::int AS n
            FROM mlb.player_props
            WHERE game_date BETWEEN %s::date AND %s::date
              AND prop_source = %s
        """
        pp_params: List[object]
        if include_user_added:
            pp_sql = pp_sql.replace("prop_source = %s", "prop_source IN ('mlb_api', 'user_added')")
            pp_params = [from_date, to_date]
        else:
            pp_params = [from_date, to_date, "mlb_api"]

        if game_types and pp_has_game_type:
            pp_sql += " AND UPPER(COALESCE(game_type, '')) = ANY(%s)"
            pp_params.append(list(game_types))

        cur.execute(pp_sql, tuple(pp_params))
        pp = _row_int(cur.fetchone(), "n")
    return {
        "model_training_props": mtp,
        "player_props": pp,
        "type_filter_applied": {
            "model_training_props": bool(game_types) and mtp_has_game_type,
            "player_props": bool(game_types) and pp_has_game_type,
        },
    }


def _delete_rows(
    from_date: str,
    to_date: str,
    *,
    include_user_added: bool,
    game_types: Sequence[str],
) -> Dict[str, object]:
    with pg_connect() as conn, conn.cursor() as cur:
        mtp_has_game_type = _table_has_column(conn, "model_training_props", "game_type")
        pp_has_game_type = _table_has_column(conn, "player_props", "game_type")

        mtp_sql = """
            DELETE FROM mlb.model_training_props
            WHERE game_date BETWEEN %s::date AND %s::date
              AND prop_source = 'mlb_api'
            """
        mtp_params: List[object] = [from_date, to_date]
        if game_types and mtp_has_game_type:
            mtp_sql += " AND UPPER(COALESCE(game_type, '')) = ANY(%s)"
            mtp_params.append(list(game_types))

        cur.execute(mtp_sql, tuple(mtp_params))
        mtp_deleted = int(cur.rowcount or 0)

        pp_sql = """
            DELETE FROM mlb.player_props
            WHERE game_date BETWEEN %s::date AND %s::date
              AND prop_source = %s
        """
        pp_params: List[object]
        if include_user_added:
            pp_sql = pp_sql.replace("prop_source = %s", "prop_source IN ('mlb_api', 'user_added')")
            pp_params = [from_date, to_date]
        else:
            pp_params = [from_date, to_date, "mlb_api"]

        if game_types and pp_has_game_type:
            pp_sql += " AND UPPER(COALESCE(game_type, '')) = ANY(%s)"
            pp_params.append(list(game_types))

        cur.execute(pp_sql, tuple(pp_params))
        pp_deleted = int(cur.rowcount or 0)
        conn.commit()
    return {
        "model_training_props": mtp_deleted,
        "player_props": pp_deleted,
        "type_filter_applied": {
            "model_training_props": bool(game_types) and mtp_has_game_type,
            "player_props": bool(game_types) and pp_has_game_type,
        },
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
        "--game-types",
        default="",
        help="Optional comma-separated MLB game types to target (example: S,R).",
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
        game_types = _parse_game_types(args.game_types)
    except ValueError as e:
        print(json.dumps({"ok": False, "status": "fail", "error": str(e)}, indent=2))
        return 2
    if from_date > to_date:
        print(json.dumps({"ok": False, "status": "fail", "error": "from-date must be <= to-date"}, indent=2))
        return 2

    include_user_added = bool(args.include_user_added)
    dry_run = not bool(args.apply)
    preview = _count_rows(
        from_date,
        to_date,
        include_user_added=include_user_added,
        game_types=game_types,
    )
    preview_type_filter = preview.get("type_filter_applied") if isinstance(preview, dict) else {}
    warnings: List[str] = []
    if game_types and not bool((preview_type_filter or {}).get("model_training_props")):
        warnings.append("model_training_props.game_type missing; game-type filter not applied there")
    if game_types and not bool((preview_type_filter or {}).get("player_props")):
        warnings.append("player_props.game_type missing; game-type filter not applied there")

    payload = {
        "ok": True,
        "status": "pass",
        "mode": "dry-run" if dry_run else "apply",
        "from_date": from_date,
        "to_date": to_date,
        "game_types": game_types,
        "include_user_added": include_user_added,
        "preview_counts": preview,
        "warnings": warnings,
    }

    if dry_run:
        print(json.dumps(payload, indent=2))
        return 0

    deleted = _delete_rows(
        from_date,
        to_date,
        include_user_added=include_user_added,
        game_types=game_types,
    )
    payload["deleted_counts"] = deleted
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
