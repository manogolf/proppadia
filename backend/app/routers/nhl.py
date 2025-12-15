# backend/app/routers/nhl.py
from __future__ import annotations

from datetime import datetime
from typing import Optional, List, Dict, Any
from zoneinfo import ZoneInfo

import psycopg
import psycopg.rows
from fastapi import APIRouter, Query

from backend.app.deps import pg_fetchone
from backend.supabase.supabase_utils import get_database_url

router = APIRouter(prefix="/api/nhl", tags=["nhl"])


# ---- small helper to get a dict-row connection ----
def _conn():
    url = get_database_url()
    if not url:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg.connect(url, row_factory=psycopg.rows.dict_row)


@router.get("/ping", summary="Ping Nhl")
def ping_nhl():
    return {"sport": "nhl", "ok": True}


@router.get("/ping-db", summary="Nhl Ping Db")
def nhl_ping_db():
    ok, row, err = pg_fetchone("SELECT 1 AS ok")
    return {"ok": bool(row), "err": err}


@router.get("/games/today", summary="Nhl Games Today",
            description="Return today's NHL games with team names/abbrs (schema: nhl.games + nhl.teams).")
def nhl_games_today(
    date: Optional[str] = Query(None, description="YYYY-MM-DD (defaults to today in America/Los_Angeles)"),
    limit: int = Query(25, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> Dict[str, Any]:
    # Resolve date in America/Los_Angeles unless overridden
    if date:
        try:
            target_date = datetime.fromisoformat(date).date()
        except ValueError:
            return {"ok": False, "error": "invalid date format; expected YYYY-MM-DD"}
    else:
        target_date = datetime.now(ZoneInfo("America/Los_Angeles")).date()

    sql = """
        SELECT
            g.game_id, g.game_date, g.start_time_utc, g.start_time, g.status, g.venue,
            g.season, g.game_type,
            g.home_team_id, ht.abbr AS home_abbr, ht.name AS home_name,
            g.away_team_id, at.abbr AS away_abbr, at.name AS away_name
        FROM nhl.games g
        LEFT JOIN nhl.teams ht ON ht.team_id = g.home_team_id
        LEFT JOIN nhl.teams at ON at.team_id = g.away_team_id
        WHERE g.game_date = %s
        ORDER BY COALESCE(g.start_time_utc, g.start_time) NULLS LAST, g.game_id
        LIMIT %s OFFSET %s
    """
    try:
        with _conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (target_date, limit, offset))
            rows = cur.fetchall()
        return {"ok": True, "date": str(target_date), "count": len(rows), "rows": rows}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


@router.get("/props/today", summary="Nhl Props Today",
            description="Return a small page of predictions for today's games.")
def nhl_props_today(
    date: Optional[str] = Query(None, description="YYYY-MM-DD (defaults to today in America/Los_Angeles)"),
    limit: int = Query(25, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    if date:
        try:
            target_date = datetime.fromisoformat(date).date()
        except ValueError:
            return {"ok": False, "error": "invalid date format; expected YYYY-MM-DD"}
    else:
        target_date = datetime.now(ZoneInfo("America/Los_Angeles")).date()

    sql = """
        SELECT
            p.prediction_id, p.player_id, p.game_id, p.prop, p.line, p.p_over,
            p.model_version, p.created_at
        FROM nhl.predictions p
        JOIN nhl.games g ON g.game_id = p.game_id
        WHERE g.game_date = %s
        ORDER BY p.created_at DESC
        LIMIT %s OFFSET %s
    """
    try:
        with _conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (target_date, limit, offset))
            rows = cur.fetchall()
        return {"ok": True, "date": str(target_date), "count": len(rows), "rows": rows}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


# --- SOG (wide) ---
# Canonical route (new)
@router.get("/sog", summary="Skater SOG predictions (wide)")
def sog(
    date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    limit: int = Query(25, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    sql = """
    WITH base AS (
      SELECT
        player_id,
        game_id,
        game_date,
        line,
        p_over
      FROM nhl.predictions
      WHERE prop = 'shots_on_goal'
        AND (%s::date IS NULL OR game_date = %s::date)
    )
    SELECT
      player_id,
      game_id,
      game_date,
      MAX(p_over) FILTER (WHERE line = 0.5) AS p_over_0_5,
      MAX(p_over) FILTER (WHERE line = 1.5) AS p_over_1_5,
      MAX(p_over) FILTER (WHERE line = 2.5) AS p_over_2_5,
      MAX(p_over) FILTER (WHERE line = 3.5) AS p_over_3_5
    FROM base
    GROUP BY player_id, game_id, game_date
    ORDER BY game_id, player_id
    LIMIT %s OFFSET %s
    """
    try:
        with _conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (date, date, limit, offset))
            return cur.fetchall()
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}

# Back-compat alias (old route name)
@router.get("/sog_stage", summary="(legacy) Skater SOG predictions (wide)")
def sog_stage(
    date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    limit: int = Query(25, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    return sog(date=date, limit=limit, offset=offset)


# --- Saves (wide) ---
# Canonical route (new)
@router.get("/saves", summary="Goalie Saves predictions (wide)")
def saves(
    date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    limit: int = Query(25, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    sql = """
    WITH base AS (
      SELECT
        player_id,
        game_id,
        game_date,
        line,
        p_over
      FROM nhl.predictions
      WHERE prop = 'goalie_saves'
        AND (%s::date IS NULL OR game_date = %s::date)
    )
    SELECT
      player_id,
      game_id,
      game_date,
      MAX(p_over) FILTER (WHERE line = 24.5) AS p_over_24_5,
      MAX(p_over) FILTER (WHERE line = 28.5) AS p_over_28_5
    FROM base
    GROUP BY player_id, game_id, game_date
    ORDER BY game_id, player_id
    LIMIT %s OFFSET %s
    """
    try:
        with _conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (date, date, limit, offset))
            return cur.fetchall()
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}

# Back-compat alias (old route name)
@router.get("/saves_stage", summary="(legacy) Goalie Saves predictions (wide)")
def saves_stage(
    date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    limit: int = Query(25, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    return saves(date=date, limit=limit, offset=offset)
