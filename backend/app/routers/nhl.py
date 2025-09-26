from fastapi import APIRouter
from backend.app.deps import pg_fetchone
from fastapi import APIRouter, Query
from datetime import datetime
from zoneinfo import ZoneInfo
import psycopg  # already installed for ping-db
from backend.app.deps import _db_url
from typing import Optional, List, Dict, Any

router = APIRouter(tags=["nhl"])

@router.get("/nhl/ping")
def ping_nhl():
    return {"sport": "nhl", "ok": True}

@router.get("/nhl/ping-db")
def nhl_ping_db():
    sql = """
        SELECT player_id, game_id, team_id, opponent_id, is_home, game_date
        FROM nhl.training_features_nhl_sog_v2
        ORDER BY game_date DESC
        LIMIT 1
    """
    ok, row, err = pg_fetchone(sql)
    if ok:
        return {
            "ok": True,
            "source": "postgres",
            "table": "nhl.training_features_nhl_sog_v2",
            "sample": row,
        }
    return {"ok": False, "error": err}

@router.get("/nhl/games/today")
def nhl_games_today(
    date: Optional[str] = Query(None, description="YYYY-MM-DD (defaults to today in America/Los_Angeles)"),
    limit: int = Query(25, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> Dict[str, Any]:
    """Return today's NHL games with team names/abbrs (schema: nhl.games + nhl.teams)."""
    # Resolve target date in America/Los_Angeles unless user overrides
    if date:
        try:
            target_date = datetime.fromisoformat(date).date()
        except ValueError:
            return {"ok": False, "error": "invalid date format; expected YYYY-MM-DD"}
    else:
        target_date = datetime.now(ZoneInfo("America/Los_Angeles")).date()

    url = _db_url()
    if not url:
        return {"ok": False, "error": "DATABASE_URL not set"}

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
        with psycopg.connect(url) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (target_date, limit, offset))
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
        data: List[Dict[str, Any]] = [dict(zip(cols, r)) for r in rows]
        return {"ok": True, "date": str(target_date), "count": len(data), "rows": data}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}

@router.get("/nhl/props/today")
def nhl_props_today(
    date: Optional[str] = Query(None, description="YYYY-MM-DD (defaults to today in America/Los_Angeles)"),
    limit: int = Query(25, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """Return a small page of predictions for today's games."""
    # Resolve target date in America/Los_Angeles unless user overrides
    if date:
        try:
            target_date = datetime.fromisoformat(date).date()
        except ValueError:
            return {"ok": False, "error": "invalid date format; expected YYYY-MM-DD"}
    else:
        target_date = datetime.now(ZoneInfo("America/Los_Angeles")).date()

    url = _db_url()
    if not url:
        return {"ok": False, "error": "DATABASE_URL not set"}

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
        with psycopg.connect(url) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (target_date, limit, offset))
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
        data = [dict(zip(cols, r)) for r in rows]
        return {"ok": True, "date": str(target_date), "count": len(data), "rows": data}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}
    