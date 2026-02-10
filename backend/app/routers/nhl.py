# backend/app/routers/nhl.py
from __future__ import annotations

from datetime import datetime
from typing import Optional, Dict, Any
from zoneinfo import ZoneInfo

import psycopg
import psycopg.rows
from fastapi import APIRouter, Query
import httpx

from backend.app.deps import pg_fetchone
from backend.supabase.supabase_utils import get_database_url

router = APIRouter(prefix="/api/nhl", tags=["nhl"])


@router.get("/gamecenter/{game_id}/landing", summary="NHL GameCenter landing (proxy)")
async def nhl_gamecenter_landing(game_id: int):
    url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/landing"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.get(url, headers={"User-Agent": "proppadia/1.0"})
        r.raise_for_status()
        return {"ok": True, "game_id": game_id, "data": r.json()}
    except Exception as e:
        return {"ok": False, "game_id": game_id, "error": str(e)}


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


@router.get(
    "/games/today",
    summary="Nhl Games Today",
    description="Return today's NHL games with team names/abbrs (schema: nhl.games + nhl.teams).",
)
def nhl_games_today(
    date: Optional[str] = Query(
        None, description="YYYY-MM-DD (defaults to today in America/New_York)"
    ),
    limit: int = Query(25, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> Dict[str, Any]:
    # Resolve date in America/New_York unless overridden
    if date:
        try:
            target_date = datetime.fromisoformat(date).date()
        except ValueError:
            return {"ok": False, "error": "invalid date format; expected YYYY-MM-DD"}
    else:
        target_date = datetime.now(ZoneInfo("America/New_York")).date()

    sql = """
        SELECT
            g.game_id,
            g.game_date,
            g.start_time_utc,
            g.status,
            g.season,
            g.game_type,
            g.home_team_id,
            g.away_team_id,
            g.home_team_code AS home_abbr,
            g.away_team_code AS away_abbr
        FROM nhl.games g
        WHERE g.game_date = %s
        ORDER BY g.start_time_utc NULLS LAST, g.game_id
        LIMIT %s OFFSET %s
    """
    try:
        with _conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (target_date, limit, offset))
            rows = cur.fetchall()
        return {"ok": True, "date": str(target_date), "count": len(rows), "rows": rows}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


@router.get(
    "/props/today",
    summary="Nhl Props Today",
    description="Return a small page of predictions for today's games.",
)
def nhl_props_today(
    date: Optional[str] = Query(
        None, description="YYYY-MM-DD (defaults to today in America/New_York)"
    ),
    limit: int = Query(25, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    if date:
        try:
            target_date = datetime.fromisoformat(date).date()
        except ValueError:
            return {"ok": False, "error": "invalid date format; expected YYYY-MM-DD"}
    else:
        target_date = datetime.now(ZoneInfo("America/New_York")).date()

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
@router.get("/sog", summary="Skater SOG predictions (wide)")
def sog(
    date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    limit: int = Query(25, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    sql = """
    WITH base AS (
    SELECT
        p.player_id,
        p.game_id,
        p.line,
        p.p_over
    FROM nhl.predictions p
    WHERE p.prop = 'shots_on_goal'
        AND p.model_family = 'denali_blend'
        AND p.model_version = 'phoenix_v2'
        AND p.feature_hash = 'phoenix_v2'
    ),
    joined AS (
    SELECT
        b.player_id,
        pl.full_name AS player_name,
        b.game_id,
        gm.game_date,
        COALESCE(s.team_id, pl.current_team_id) AS team_id,
        t.team AS team_abbr,
        t.full_team_name,
        b.line,
        b.p_over
    FROM base b
    JOIN nhl.games gm
        ON gm.game_id = b.game_id
    LEFT JOIN nhl.skater_game_logs_raw s
        ON s.player_id = b.player_id
    AND s.game_id   = b.game_id
    LEFT JOIN nhl.players pl
        ON pl.player_id = b.player_id
    LEFT JOIN nhl.teams t
        ON t.team_id = COALESCE(s.team_id, pl.current_team_id)
    WHERE (%s::date IS NULL OR gm.game_date = %s::date)
    )
    SELECT
    player_id,
    player_name,
    game_id,
    game_date,
    team_id,
    team_abbr,
    full_team_name,
    MAX(p_over) FILTER (WHERE line = 1.5) AS p_over_1_5,
    MAX(p_over) FILTER (WHERE line = 2.5) AS p_over_2_5,
    MAX(p_over) FILTER (WHERE line = 3.5) AS p_over_3_5
    FROM joined
    GROUP BY player_id, player_name, game_id, game_date, team_id, team_abbr, full_team_name
    ORDER BY game_id, team_abbr NULLS LAST, player_name NULLS LAST
    LIMIT %s OFFSET %s;
    """
    try:
        with _conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (date, date, limit, offset))
            return cur.fetchall()
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


@router.get("/sog_stage", summary="(legacy) Skater SOG predictions (wide)")
def sog_stage(
    date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    limit: int = Query(25, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    return sog(date=date, limit=limit, offset=offset)


# --- Saves (wide) ---
@router.get("/saves", summary="Goalie Saves predictions (wide)")
def saves(
    date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    limit: int = Query(25, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    sql = """
    WITH base AS (
      SELECT
        p.player_id,
        p.game_id,
        p.line,
        p.p_over
      FROM nhl.predictions p
      WHERE p.prop = 'goalie_saves'
        AND p.line IN (18.5, 19.5, 20.5, 21.5, 22.5, 23.5)
    ),
    joined AS (
      SELECT
        b.player_id,
        pl.full_name AS player_name,
        b.game_id,
        gm.game_date,
        COALESCE(gl.team_id, pl.current_team_id) AS team_id,
        t.team AS team_abbr,
        t.full_team_name,
        b.line,
        b.p_over
      FROM base b
      JOIN nhl.games gm
        ON gm.game_id = b.game_id
      LEFT JOIN nhl.goalie_game_logs_raw gl
        ON gl.player_id = b.player_id
       AND gl.game_id   = b.game_id
      LEFT JOIN nhl.players pl
        ON pl.player_id = b.player_id
      LEFT JOIN nhl.teams t
        ON t.team_id = COALESCE(gl.team_id, pl.current_team_id)
      WHERE (%s::date IS NULL OR gm.game_date = %s::date)
    )
    SELECT
      player_id,
      player_name,
      game_id,
      game_date,
      team_id,
      team_abbr,
      full_team_name,
      MAX(p_over) FILTER (WHERE line = 18.5) AS p_over_18_5,
      MAX(p_over) FILTER (WHERE line = 19.5) AS p_over_19_5,
      MAX(p_over) FILTER (WHERE line = 20.5) AS p_over_20_5,
      MAX(p_over) FILTER (WHERE line = 21.5) AS p_over_21_5,
      MAX(p_over) FILTER (WHERE line = 22.5) AS p_over_22_5,
      MAX(p_over) FILTER (WHERE line = 23.5) AS p_over_23_5
    FROM joined
    GROUP BY player_id, player_name, game_id, game_date, team_id, team_abbr, full_team_name
    ORDER BY game_id, team_abbr NULLS LAST, player_name NULLS LAST
    LIMIT %s OFFSET %s;
    """
    try:
        with _conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (date, date, limit, offset))
            return cur.fetchall()
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}

@router.get("/saves_stage", summary="(legacy) Goalie Saves predictions (wide)")
def saves_stage(
    date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    limit: int = Query(25, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    return saves(date=date, limit=limit, offset=offset)