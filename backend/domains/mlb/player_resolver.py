"""MLB player resolution domain logic."""

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
    with psycopg.connect(_db_url(), row_factory=psycopg.rows.dict_row, prepare_threshold=0) as conn:
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


def _decorate(row: Dict[str, Any], *, source: str) -> Optional[Dict[str, Any]]:
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


def _resolve_by_player_id(player_id: int) -> Optional[Dict[str, Any]]:
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
        cand = _decorate(rows[0], source=source)
        if cand:
            cand["matched_on"] = "player_id"
            return cand
    return None


def _resolve_by_name(name: str, team_abbr: Optional[str]) -> Optional[Dict[str, Any]]:
    team = _normalize_team(team_abbr)
    exact_name_sql = """
        SELECT player_id, player_name, team
        FROM player_ids
        WHERE lower(player_name) = lower(%s)
          AND (%s IS NULL OR upper(team) = upper(%s))
        LIMIT 1
    """
    fuzzy_name_sql = """
        SELECT player_id, player_name, team
        FROM player_ids
        WHERE player_name ILIKE %s
          AND (%s IS NULL OR upper(team) = upper(%s))
        LIMIT 5
    """
    mtp_fallback_sql = """
        SELECT player_id, player_name, team
        FROM model_training_props
        WHERE player_name ILIKE %s
          AND (%s IS NULL OR upper(team) = upper(%s))
        ORDER BY game_date DESC NULLS LAST
        LIMIT 5
    """

    search_steps = [
        (exact_name_sql, (name, team, team), "player_ids", "exact_name"),
        (fuzzy_name_sql, (f"%{name}%", team, team), "player_ids", "fuzzy_name"),
        (mtp_fallback_sql, (f"%{name}%", team, team), "model_training_props", "fuzzy_name"),
    ]

    for sql, params, source, matched_on in search_steps:
        try:
            rows = _fetchall(sql, params)
        except Exception:
            continue
        for row in rows:
            cand = _decorate(row, source=source)
            if not cand:
                continue
            cand["matched_on"] = matched_on
            return cand
    return None


def resolve_player_candidate(
    *,
    player_id: Optional[int],
    name: Optional[str],
    team_abbr: Optional[str],
) -> Optional[Dict[str, Any]]:
    """Resolve best-match MLB player identity from current tables."""
    if player_id is not None:
        by_id = _resolve_by_player_id(player_id)
        if by_id:
            return by_id

    query_name = (name or "").strip()
    if query_name:
        return _resolve_by_name(query_name, team_abbr)
    return None
