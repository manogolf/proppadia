"""MLB player repository queries."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from backend.mlb.shared.team_name_map import (
    getFullTeamAbbreviationFromID,
    getTeamIdFromAbbr,
    normalizeTeamAbbreviation,
)
from backend.shared.db import pg_fetchall


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


def team_abbr_to_team_id(team_abbr: Optional[str]) -> Optional[int]:
    if not team_abbr:
        return None
    return getTeamIdFromAbbr(team_abbr)


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
        or team_abbr_to_team_id(team),
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
            rows = pg_fetchall(sql, (str(player_id),))
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
        rows = pg_fetchall(sql, (f"%{query}%", lim))
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
        latest_team AS (
          SELECT DISTINCT ON (CAST(player_id AS TEXT))
            CAST(player_id AS TEXT) AS player_id,
            team
          FROM model_training_props
          WHERE team IS NOT NULL
            AND BTRIM(CAST(team AS TEXT)) <> ''
          ORDER BY CAST(player_id AS TEXT), game_date DESC NULLS LAST
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
          COALESCE(NULLIF(BTRIM(CAST(p.team AS TEXT)), ''), lt.team) AS team,
          r.last_prop_date
        FROM players p
        LEFT JOIN latest_team lt
          ON lt.player_id = p.player_id
        LEFT JOIN recent r
          ON r.player_id = p.player_id
        ORDER BY COALESCE(NULLIF(BTRIM(CAST(p.team AS TEXT)), ''), lt.team) ASC NULLS LAST, p.player_name ASC
        LIMIT %s
    """
    try:
        rows = pg_fetchall(sql, (lim,))
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


def list_players_mlb(limit: int = 2000) -> List[Dict[str, Any]]:
    """
    MLB-scoped cumulative players directory.

    Keeps compatibility with list_players output shape, but computes recency from
    non-NHL rows to avoid cross-sport date bleed when player ids overlap.
    """
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
        latest_team AS (
          SELECT DISTINCT ON (CAST(player_id AS TEXT))
            CAST(player_id AS TEXT) AS player_id,
            team
          FROM model_training_props
          WHERE team IS NOT NULL
            AND BTRIM(CAST(team AS TEXT)) <> ''
          ORDER BY CAST(player_id AS TEXT), game_date DESC NULLS LAST
        ),
        latest_prop_team AS (
          SELECT DISTINCT ON (CAST(player_id AS TEXT))
            CAST(player_id AS TEXT) AS player_id,
            team
          FROM player_props
          WHERE team IS NOT NULL
            AND BTRIM(CAST(team AS TEXT)) <> ''
            AND (prop_source IS NULL OR prop_source NOT ILIKE 'nhl_%')
          ORDER BY CAST(player_id AS TEXT), game_date DESC NULLS LAST
        ),
        recent AS (
          SELECT
            CAST(player_id AS TEXT) AS player_id,
            MAX(game_date)::date AS last_prop_date
          FROM player_props
          WHERE prop_source IS NULL OR prop_source NOT ILIKE 'nhl_%'
          GROUP BY CAST(player_id AS TEXT)
        )
        SELECT
          p.player_id,
          p.player_name,
          COALESCE(NULLIF(BTRIM(CAST(p.team AS TEXT)), ''), lt.team, lpt.team) AS team,
          r.last_prop_date
        FROM players p
        LEFT JOIN latest_team lt
          ON lt.player_id = p.player_id
        LEFT JOIN latest_prop_team lpt
          ON lpt.player_id = p.player_id
        LEFT JOIN recent r
          ON r.player_id = p.player_id
        ORDER BY COALESCE(NULLIF(BTRIM(CAST(p.team AS TEXT)), ''), lt.team, lpt.team) ASC NULLS LAST, p.player_name ASC
        LIMIT %s
    """
    try:
        rows = pg_fetchall(sql, (lim,))
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


def resolve_by_player_id(player_id: int) -> Optional[Dict[str, Any]]:
    resolved = lookup_player(player_id)
    if resolved:
        resolved["matched_on"] = "player_id"
    return resolved


def resolve_by_name(name: str, team_abbr: Optional[str]) -> Optional[Dict[str, Any]]:
    team = _normalize_team(team_abbr)
    team_id = team_abbr_to_team_id(team) if team else None
    team_id_txt = str(team_id) if team_id is not None else None
    exact_name_sql = """
        SELECT player_id, player_name, team
        FROM player_ids
        WHERE lower(player_name) = lower(%s)
          AND (
            %s IS NULL
            OR upper(CAST(team AS TEXT)) = upper(%s)
            OR CAST(team AS TEXT) = %s
          )
        LIMIT 1
    """
    fuzzy_name_sql = """
        SELECT player_id, player_name, team
        FROM player_ids
        WHERE player_name ILIKE %s
          AND (
            %s IS NULL
            OR upper(CAST(team AS TEXT)) = upper(%s)
            OR CAST(team AS TEXT) = %s
          )
        LIMIT 5
    """
    mtp_fallback_sql = """
        SELECT player_id, player_name, team
        FROM model_training_props
        WHERE player_name ILIKE %s
          AND (
            %s IS NULL
            OR upper(CAST(team AS TEXT)) = upper(%s)
            OR CAST(team AS TEXT) = %s
          )
        ORDER BY game_date DESC NULLS LAST
        LIMIT 5
    """

    search_steps = [
        (exact_name_sql, (name, team, team, team_id_txt), "player_ids", "exact_name"),
        (fuzzy_name_sql, (f"%{name}%", team, team, team_id_txt), "player_ids", "fuzzy_name"),
        (mtp_fallback_sql, (f"%{name}%", team, team, team_id_txt), "model_training_props", "fuzzy_name"),
    ]

    for sql, params, source, matched_on in search_steps:
        try:
            rows = pg_fetchall(sql, params)
        except Exception:
            continue
        for row in rows:
            cand = _decorate(row, source)
            if not cand:
                continue
            cand["matched_on"] = matched_on
            return cand
    return None


def fetch_player_profile_rows(player_id: int) -> Dict[str, List[Dict[str, Any]]]:
    pid_txt = str(player_id)
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
          AND prop_source = 'mlb_api'
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
            return pg_fetchall(sql, (pid_txt,))
        except Exception:
            return []

    return {
        "recent_props": run_or_empty(recent_props_sql),
        "streaks": run_or_empty(streaks_sql),
        "stat_derived": run_or_empty(stat_derived_sql),
        "training_summary": run_or_empty(training_summary_sql),
    }
