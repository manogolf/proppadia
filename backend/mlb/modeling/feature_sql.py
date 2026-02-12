# backend/scripts/modeling/feature_sql.py
from __future__ import annotations
from textwrap import dedent
from typing import Optional

def _mv_name(prop_type: str) -> str:
    if not prop_type:
        raise ValueError("prop_type is required")
    return f"public.training_features_{prop_type}_enriched"

def build_training_sql(
    *args,
    **kwargs,
) -> str:
    """
    Return SQL to pull training rows from the per-prop MV.

    Parameters (by name or position):
      - prop_type: str (required)
      - days_back: Optional[int] = None
      - limit: Optional[int] = 50000
      - resolved_only: bool = True   # keep it strict per your data
    """
    # Back-compat: if someone accidentally passed an engine first, ignore it.
    if args and not isinstance(args[0], str) and "prop_type" not in kwargs:
        args = args[1:]

    prop_type: Optional[str] = kwargs.get("prop_type") or (args[0] if args else None)
    if not prop_type:
        raise ValueError("prop_type is required")

    days_back: Optional[int] = kwargs.get("days_back")
    limit: Optional[int] = kwargs.get("limit", 50000)
    resolved_only: bool = kwargs.get("resolved_only", True)

    table = _mv_name(prop_type)

    where = []
    #if resolved_only:
        # Your MT/MV pipeline yields resolved-only rows; keep it strict.
        #where.append("status = 'resolved'")
    if days_back:
        where.append(f"game_date >= CURRENT_DATE - INTERVAL '{int(days_back)} days'")
    where.append("btrim(prop_value::text) <> ''")
    where.append("btrim(result::text) <> ''")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    limit_sql = f"LIMIT {int(limit)}" if limit else ""

    # NOTE:
    # - No ORDER BY (avoids full sort & timeouts).
    # - Casts via NULLIF(btrim(..),'')::numeric to dodge bad text.
    # - Compute y_over inline; training code drops NULL labels anyway.
    sql = dedent(f"""
        SELECT
          game_date,
          game_id,
          player_id,
          team_id,
          prop_type,
          prop_source,
          NULLIF(btrim(prop_value::text), '')::numeric AS prop_value,
          NULLIF(btrim(result::text), '')::numeric     AS result,
          CASE
            WHEN NULLIF(btrim(result::text), '')::numeric > NULLIF(btrim(prop_value::text), '')::numeric THEN 1
            WHEN NULLIF(btrim(result::text), '')::numeric < NULLIF(btrim(prop_value::text), '')::numeric THEN 0
            ELSE NULL
          END AS y_over
        FROM {table}
        {where_sql}
        {limit_sql}
    """).strip()

    return sql
