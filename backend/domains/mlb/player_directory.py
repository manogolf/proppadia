"""MLB player directory/profile query helpers."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

try:
    import psycopg
    import psycopg.rows
except Exception:  # pragma: no cover - environment-dependent import
    psycopg = None  # type: ignore

from backend.mlb.shared.team_name_map import (
    getFullTeamAbbreviationFromID,
    getTeamIdFromAbbr,
    normalizeTeamAbbreviation,
)
from backend.supabase.supabase_utils import get_database_url


def _db_url() -> str:
    url = get_database_url()
    if not url:
        raise RuntimeError("DATABASE_URL/SUPABASE_DB_URL not configured")
    return url


def _fetchall(sql: str, params: Sequence[Any]) -> List[Dict[str, Any]]:
    if psycopg is None:
        raise RuntimeError("psycopg not installed")
    with psycopg.connect(_db_url(), row_factory=psycopg.rows.dict_row, prepare_threshold=None) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return list(cur.fetchall() or [])


def _normalize_team(team_abbr: Optional[str]) -> Optional[str]:
    if not team_abbr:
        return None
    s = str(team_abbr).strip()
    if not s:
        return None
    if s.isdigit():
        return normalizeTeamAbbreviation(getFullTeamAbbreviationFromID(int(s)))
    return normalizeTeamAbbreviation(s)


def _to_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return None


def _decorate(row: Dict[str, Any], source: str) -> Optional[Dict[str, Any]]:
    pid = _to_int(row.get("player_id"))
    if pid is None:
        return None
    team = _normalize_team(row.get("team"))
    return {
        "player_id": pid,
        "player_name": row.get("player_name"),
        "team_abbr": team,
        "team_id": (_to_int(row.get("team")) if str(row.get("team", "")).strip().isdigit() else None)
        or (getTeamIdFromAbbr(team) if team else None),
        "source": source,
    }


def lookup_player(player_id: int) -> Optional[Dict[str, Any]]:
    queries = [
        (
            """
            SELECT player_id, player_name, team
            FROM player_ids
            WHERE CAST(player_id AS TEXT) = %s
            LIMIT 1
            """,
            "player_ids",
        ),
        (
            """
            SELECT player_id, player_name, team
            FROM model_training_props
            WHERE CAST(player_id AS TEXT) = %s
            ORDER BY game_date DESC NULLS LAST
            LIMIT 1
            """,
            "model_training_props",
        ),
    ]
    for sql, source in queries:
        try:
            rows = _fetchall(sql, (str(player_id),))
        except Exception:
            continue
        if not rows:
            continue
        out = _decorate(rows[0], source)
        if out:
            return out
    return None


def search_players(q: str, limit: int = 10) -> List[Dict[str, Any]]:
    query = (q or "").strip()
    if not query:
        return []
    lim = max(1, min(int(limit), 100))
    sql = """
        SELECT
          CAST(player_id AS TEXT) AS player_id,
          MIN(player_name) AS player_name,
          MIN(team) AS team
        FROM player_ids
        WHERE player_name ILIKE %s
        GROUP BY CAST(player_id AS TEXT)
        ORDER BY MIN(player_name) ASC
        LIMIT %s
    """
    try:
        rows = _fetchall(sql, (f"%{query}%", lim))
    except Exception:
        return []
    out: List[Dict[str, Any]] = []
    for row in rows:
        d = _decorate(row, "player_ids")
        if d:
            out.append(d)
    return out


def list_players(limit: int = 2000) -> List[Dict[str, Any]]:
    lim = max(1, min(int(limit), 5000))
    sql = """
        WITH players AS (
          SELECT
            CAST(player_id AS TEXT) AS player_id,
            MIN(player_name) AS player_name,
            MIN(team) AS team
          FROM player_ids
          GROUP BY CAST(player_id AS TEXT)
        ),
        recent AS (
          SELECT
            CAST(player_id AS TEXT) AS player_id,
            MAX(game_date)::date AS last_prop_date
          FROM player_props
          GROUP BY CAST(player_id AS TEXT)
        )
        SELECT
          p.player_id,
          p.player_name,
          p.team,
          r.last_prop_date
        FROM players p
        LEFT JOIN recent r
          ON r.player_id = p.player_id
        ORDER BY p.team ASC NULLS LAST, p.player_name ASC
        LIMIT %s
    """
    try:
        rows = _fetchall(sql, (lim,))
    except Exception:
        return []

    out: List[Dict[str, Any]] = []
    for row in rows:
        pid = _to_int(row.get("player_id"))
        if pid is None:
            continue
        out.append(
            {
                "player_id": pid,
                "player_name": row.get("player_name"),
                "team": _normalize_team(row.get("team")),
                "last_prop_date": (
                    row.get("last_prop_date").isoformat()
                    if hasattr(row.get("last_prop_date"), "isoformat")
                    else (str(row.get("last_prop_date")) if row.get("last_prop_date") else None)
                ),
            }
        )
    return out


def player_profile(player_id: int) -> Dict[str, Any]:
    pid_txt = str(player_id)
    info = lookup_player(player_id) or {"player_id": player_id}

    recent_props_sql = """
        SELECT game_date, prop_type, result, outcome, over_under, prop_value, confidence_score
        FROM player_props
        WHERE CAST(player_id AS TEXT) = %s
        ORDER BY game_date DESC NULLS LAST
        LIMIT 14
    """
    streaks_sql = """
        SELECT prop_type, streak_type, streak_count
        FROM player_streak_profiles
        WHERE CAST(player_id AS TEXT) = %s
        ORDER BY streak_count DESC NULLS LAST
        LIMIT 10
    """
    stat_derived_sql = """
        SELECT game_date, prop_type, result, outcome
        FROM model_training_props
        WHERE CAST(player_id AS TEXT) = %s
          AND prop_source = 'stat_derived'
        ORDER BY game_date DESC NULLS LAST
        LIMIT 20
    """
    training_summary_sql = """
        SELECT prop_type, COUNT(*)::int AS count
        FROM model_training_props
        WHERE CAST(player_id AS TEXT) = %s
        GROUP BY prop_type
        ORDER BY count DESC
        LIMIT 20
    """

    def run_or_empty(sql: str) -> List[Dict[str, Any]]:
        try:
            return _fetchall(sql, (pid_txt,))
        except Exception:
            return []

    recent_props = run_or_empty(recent_props_sql)
    streaks = run_or_empty(streaks_sql)
    stat_derived = run_or_empty(stat_derived_sql)
    training_summary = run_or_empty(training_summary_sql)

    return {
        "player_info": {
            "player_id": info.get("player_id"),
            "player_name": info.get("player_name"),
            "team": info.get("team_abbr"),
            "team_id": info.get("team_id"),
        },
        "streaks": streaks,
        "recent_props": recent_props,
        "stat_derived": stat_derived,
        "training_summary": training_summary,
        # Kept for frontend shape compatibility; can be filled in later.
        "season_stats": {},
        "career_stats": {},
    }
