#!/usr/bin/env python3
"""
Validate DB contract for frontend MLB props table.

Checks:
- required columns exist on public.player_props
- sample rows can be parsed for key UI fields
"""

from __future__ import annotations

import argparse
from datetime import date
from typing import Any, Dict, List, Sequence


REQUIRED_COLUMNS = {
    "id",
    "player_name",
    "team",
    "prop_type",
    "over_under",
    "prop_value",
    "game_date",
    "status",
    "outcome",
    "prop_source",
    "created_at",
}


def _db_url() -> str:
    from backend.supabase.supabase_utils import get_database_url

    url = get_database_url()
    if not url:
        raise RuntimeError("DATABASE_URL/SUPABASE_DB_URL not configured")
    return url


def _fetchall(sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
    import psycopg
    import psycopg.rows

    with psycopg.connect(_db_url(), row_factory=psycopg.rows.dict_row, prepare_threshold=0) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return list(cur.fetchall() or [])


def _columns() -> List[str]:
    rows = _fetchall(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name='player_props'
        ORDER BY ordinal_position
        """
    )
    return [str(r.get("column_name")) for r in rows]


def _sample_rows(limit: int, cols: Sequence[str]) -> List[Dict[str, Any]]:
    select_fields = [
        "id",
        "player_name",
        "team",
        "prop_type",
        "over_under",
        "prop_value",
        "game_date",
        "status",
        "outcome",
        "prop_source",
        "created_at",
    ]
    # Optional timestamp fields are included only when present in DB schema.
    if "updated_at" in cols:
        select_fields.append("updated_at")
    if "prediction_timestamp" in cols:
        select_fields.append("prediction_timestamp")

    sql = f"""
        SELECT {", ".join(select_fields)}
        FROM player_props
        ORDER BY created_at DESC NULLS LAST
        LIMIT %s
    """

    return _fetchall(
        sql,
        (int(limit),),
    )


def _parse_date(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, date):
        return True
    try:
        date.fromisoformat(str(v))
        return True
    except Exception:
        return False


def main() -> int:
    ap = argparse.ArgumentParser(description="Validate MLB props table DB contract")
    ap.add_argument("--sample-limit", type=int, default=50)
    args = ap.parse_args()

    try:
        cols = set(_columns())
    except Exception as e:
        print(f"FAIL columns query: {type(e).__name__}: {e}")
        return 1

    missing = sorted(REQUIRED_COLUMNS - cols)
    if missing:
        print(f"FAIL missing required columns: {missing}")
        return 1

    print("PASS required columns present")

    try:
        rows = _sample_rows(args.sample_limit, list(cols))
    except Exception as e:
        print(f"FAIL sample rows query: {type(e).__name__}: {e}")
        return 1

    if not rows:
        print("PASS sample rows: table currently empty (contract columns validated)")
        return 0

    bad_rows = 0
    for r in rows:
        if not _parse_date(r.get("game_date")):
            bad_rows += 1
            continue
        try:
            float(r.get("prop_value"))
        except Exception:
            bad_rows += 1
            continue
        if not str(r.get("prop_type") or "").strip():
            bad_rows += 1
            continue
        if str(r.get("over_under") or "").lower() not in {"over", "under"}:
            bad_rows += 1
            continue

    if bad_rows:
        print(f"FAIL sample row parsing: {bad_rows}/{len(rows)} rows violated UI assumptions")
        return 1

    print(f"PASS sample rows: {len(rows)} rows valid for props-table contract")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
