"""MLB player repository queries."""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional, Set

from backend.mlb.shared.team_name_map import (
    getFullTeamAbbreviationFromID,
    getTeamIdFromAbbr,
    isValidMLBTeam,
    normalizeTeamAbbreviation,
)
from backend.shared.db import pg_fetchall

LOGGER = logging.getLogger(__name__)


def _normalize_team(team_abbr: Optional[str]) -> Optional[str]:
    if not team_abbr:
        return None
    s = str(team_abbr).strip()
    if not s:
        return None
    if s.isdigit():
        return normalizeTeamAbbreviation(getFullTeamAbbreviationFromID(int(s)))
    return normalizeTeamAbbreviation(s)


def _is_known_player_name(value: Any) -> bool:
    name = str(value or "").strip()
    return bool(name) and name.lower() != "unknown"


def _first_known_name(*values: Any) -> Optional[str]:
    for value in values:
        if _is_known_player_name(value):
            return str(value).strip()
    return None


def _first_valid_mlb_team(*values: Any) -> Optional[str]:
    for value in values:
        team = _normalize_team(value)
        if team and (isValidMLBTeam(team) or team_abbr_to_team_id(team) is not None):
            return team
    return None


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
    pid = _to_int(player_id)
    if pid is None:
        return None
    fallback: Optional[Dict[str, Any]] = None
    queries = [
        (
            """
            SELECT player_id, player_name, team
            FROM mlb.player_ids
            WHERE player_id = %s
            LIMIT 1
            """,
            "player_ids",
        ),
        (
            """
            SELECT player_id, player_name, team
            FROM mlb.model_training_props
            WHERE player_id = %s
            ORDER BY game_date DESC NULLS LAST
            LIMIT 1
            """,
            "model_training_props",
        ),
    ]
    for sql, source in queries:
        try:
            rows = pg_fetchall(sql, (pid,))
        except Exception:
            continue
        if not rows:
            continue
        out = _decorate(rows[0], source)
        if not out:
            continue
        if _is_known_player_name(out.get("player_name")):
            return out
        if fallback is None:
            fallback = out
    return fallback


def search_players(q: str, limit: int = 10) -> List[Dict[str, Any]]:
    query = (q or "").strip()
    if not query:
        return []
    lim = max(1, min(int(limit), 100))
    player_ids_sql = """
        SELECT
          CAST(player_id AS TEXT) AS player_id,
          MIN(player_name) AS player_name,
          MIN(team) AS team
        FROM mlb.player_ids
        WHERE player_name ILIKE %s
        GROUP BY CAST(player_id AS TEXT)
        ORDER BY MIN(player_name) ASC
        LIMIT %s
    """
    mtp_sql = """
        SELECT DISTINCT ON (CAST(player_id AS TEXT))
          CAST(player_id AS TEXT) AS player_id,
          player_name,
          team
        FROM mlb.model_training_props
        WHERE player_name ILIKE %s
        ORDER BY CAST(player_id AS TEXT), game_date DESC NULLS LAST
        LIMIT %s
    """
    try:
        player_rows = pg_fetchall(player_ids_sql, (f"%{query}%", lim))
    except Exception:
        player_rows = []

    out: List[Dict[str, Any]] = []
    seen: set[int] = set()
    for row in player_rows:
        d = _decorate(row, "player_ids")
        if d:
            seen.add(int(d["player_id"]))
            out.append(d)

    if len(out) >= lim:
        return out[:lim]

    try:
        mtp_rows = pg_fetchall(mtp_sql, (f"%{query}%", lim * 3))
    except Exception:
        return out[:lim]
    for row in mtp_rows:
        d = _decorate(row, "model_training_props")
        if not d:
            continue
        pid = int(d["player_id"])
        if pid in seen:
            continue
        seen.add(pid)
        out.append(d)
        if len(out) >= lim:
            break
    return out


def list_players(limit: int = 2000) -> List[Dict[str, Any]]:
    lim = max(1, min(int(limit), 5000))
    sql = """
        WITH players AS (
          SELECT
            CAST(player_id AS TEXT) AS player_id,
            MIN(player_name) AS player_name,
            MIN(team) AS team
          FROM mlb.player_ids
          GROUP BY CAST(player_id AS TEXT)
        ),
        latest_team AS (
          SELECT DISTINCT ON (CAST(player_id AS TEXT))
            CAST(player_id AS TEXT) AS player_id,
            team
          FROM mlb.model_training_props
          WHERE team IS NOT NULL
            AND BTRIM(CAST(team AS TEXT)) <> ''
          ORDER BY CAST(player_id AS TEXT), game_date DESC NULLS LAST
        ),
        recent AS (
          SELECT
            CAST(player_id AS TEXT) AS player_id,
            MAX(game_date)::date AS last_prop_date
          FROM mlb.player_props
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
        WITH player_ids_rows AS (
          SELECT
            CAST(player_id AS TEXT) AS player_id,
            MIN(player_name) FILTER (
              WHERE player_name IS NOT NULL
                AND BTRIM(CAST(player_name AS TEXT)) <> ''
                AND lower(BTRIM(CAST(player_name AS TEXT))) <> 'unknown'
            ) AS player_ids_name,
            MIN(player_name) AS fallback_player_ids_name,
            MIN(team) AS player_ids_team
          FROM mlb.player_ids
          GROUP BY CAST(player_id AS TEXT)
        ),
        latest_team AS (
          SELECT DISTINCT ON (CAST(player_id AS TEXT))
            CAST(player_id AS TEXT) AS player_id,
            player_name AS latest_training_name,
            team
          FROM mlb.model_training_props
          WHERE (
              team IS NOT NULL
              AND BTRIM(CAST(team AS TEXT)) <> ''
            )
            OR (
              player_name IS NOT NULL
              AND BTRIM(CAST(player_name AS TEXT)) <> ''
              AND lower(BTRIM(CAST(player_name AS TEXT))) <> 'unknown'
            )
          ORDER BY CAST(player_id AS TEXT), game_date DESC NULLS LAST
        ),
        latest_prop_team AS (
          SELECT DISTINCT ON (CAST(player_id AS TEXT))
            CAST(player_id AS TEXT) AS player_id,
            player_name AS latest_prop_name,
            team
          FROM mlb.player_props
          WHERE (
              team IS NOT NULL
              AND BTRIM(CAST(team AS TEXT)) <> ''
            )
            OR (
              player_name IS NOT NULL
              AND BTRIM(CAST(player_name AS TEXT)) <> ''
              AND lower(BTRIM(CAST(player_name AS TEXT))) <> 'unknown'
            )
            AND (prop_source IS NULL OR prop_source NOT ILIKE 'nhl_%%')
          ORDER BY CAST(player_id AS TEXT), game_date DESC NULLS LAST
        ),
        recent AS (
          SELECT
            CAST(player_id AS TEXT) AS player_id,
            MAX(game_date)::date AS last_prop_date
          FROM mlb.model_training_props
          WHERE prop_source = 'mlb_api'
          GROUP BY CAST(player_id AS TEXT)
        )
        SELECT
          p.player_id,
          p.player_ids_name,
          p.fallback_player_ids_name,
          p.player_ids_team,
          lt.latest_training_name,
          lt.team AS latest_training_team,
          lpt.latest_prop_name,
          lpt.team AS latest_prop_team,
          r.last_prop_date
        FROM player_ids_rows p
        LEFT JOIN latest_team lt
          ON lt.player_id = p.player_id
        LEFT JOIN latest_prop_team lpt
          ON lpt.player_id = p.player_id
        LEFT JOIN recent r
          ON r.player_id = p.player_id
        ORDER BY r.last_prop_date DESC NULLS LAST, p.player_ids_name ASC NULLS LAST, p.player_id ASC
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
        team = _first_valid_mlb_team(
            row.get("latest_training_team"),
            row.get("player_ids_team"),
            row.get("latest_prop_team"),
        )
        name = _first_known_name(
            row.get("player_ids_name"),
            row.get("latest_training_name"),
            row.get("latest_prop_name"),
            row.get("fallback_player_ids_name"),
        )
        last_prop_date_raw = row.get("last_prop_date")
        if team:
            player_status = "recent_mlb" if last_prop_date_raw else "active_mlb"
        elif last_prop_date_raw:
            player_status = "unknown_team"
        else:
            player_status = "minor_affiliate"
        out.append(
            {
                "player_id": pid,
                "player_name": name,
                "team": team,
                "player_status": player_status,
                "last_prop_date": (
                    last_prop_date_raw.isoformat()
                    if hasattr(last_prop_date_raw, "isoformat")
                    else (str(last_prop_date_raw) if last_prop_date_raw else None)
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
        FROM mlb.player_ids
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
        FROM mlb.player_ids
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
        FROM mlb.model_training_props
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


def _normalize_profile_sections(sections: Optional[Set[str]]) -> Set[str]:
    allowed = {"recent_props", "streaks", "stat_derived", "training_summary", "freshness_metadata"}
    if not sections:
        return allowed
    normalized = {str(section or "").strip().lower() for section in sections}
    if "summary" in normalized:
        normalized.add("freshness_metadata")
    if "history" in normalized:
        normalized.update({"recent_props", "stat_derived", "training_summary"})
    if "all" in normalized:
        return allowed
    return {section for section in normalized if section in allowed}


def fetch_player_profile_rows(player_id: int, sections: Optional[Set[str]] = None) -> Dict[str, Any]:
    pid = _to_int(player_id)
    if pid is None:
        return {
            "recent_props": [],
            "streaks": [],
            "stat_derived": [],
            "training_summary": [],
            "freshness_metadata": {},
        }
    selected_sections = _normalize_profile_sections(sections)
    recent_props_sql = """
        SELECT
          game_date,
          prop_type,
          result,
          outcome,
          over_under,
          COALESCE(prop_value, line) AS prop_value,
          confidence_score,
          prop_source,
          'model_training_props'::text AS source
        FROM mlb.model_training_props
        WHERE player_id = %s
          AND prop_source = 'mlb_api'
        ORDER BY game_date DESC NULLS LAST
        LIMIT 14
    """
    streaks_sql = """
        WITH hist AS (
          SELECT
            lower(trim(prop_type)) AS prop_type,
            lower(trim(outcome)) AS outcome,
            game_date::date AS game_date,
            game_id,
            row_number() OVER (
              PARTITION BY lower(trim(prop_type))
              ORDER BY game_date DESC NULLS LAST, game_id DESC NULLS LAST
            ) AS rn
          FROM mlb.model_training_props
          WHERE player_id = %s
            AND prop_source = 'mlb_api'
            AND lower(trim(coalesce(outcome, ''))) IN ('win', 'loss')
            AND prop_type IS NOT NULL
            AND game_date IS NOT NULL
        ),
        latest AS (
          SELECT prop_type, outcome AS latest_outcome
          FROM hist
          WHERE rn = 1
        ),
        breaks AS (
          SELECT h.prop_type, min(h.rn) AS break_rn
          FROM hist h
          JOIN latest l
            ON l.prop_type = h.prop_type
          WHERE h.outcome <> l.latest_outcome
          GROUP BY h.prop_type
        )
        SELECT
          l.prop_type,
          CASE
            WHEN l.latest_outcome = 'win' THEN 'hot'
            WHEN l.latest_outcome = 'loss' THEN 'cold'
            ELSE 'neutral'
          END AS streak_type,
          count(*)::int AS streak_count
        FROM hist h
        JOIN latest l
          ON l.prop_type = h.prop_type
        LEFT JOIN breaks b
          ON b.prop_type = h.prop_type
        WHERE h.outcome = l.latest_outcome
          AND (b.break_rn IS NULL OR h.rn < b.break_rn)
        GROUP BY l.prop_type, l.latest_outcome
        ORDER BY count(*) DESC NULLS LAST
        LIMIT 10
    """
    stat_derived_sql = """
        SELECT game_date, prop_type, result, outcome
        FROM mlb.model_training_props
        WHERE player_id = %s
          AND prop_source = 'mlb_api'
        ORDER BY game_date DESC NULLS LAST
        LIMIT 20
    """
    training_summary_sql = """
        SELECT prop_type, COUNT(*)::int AS count
        FROM mlb.model_training_props
        WHERE player_id = %s
        GROUP BY prop_type
        ORDER BY count DESC
        LIMIT 20
    """
    freshness_sql = """
        SELECT
          'model_training_props'::text AS source,
          COUNT(*)::int AS rows,
          MAX(game_date)::date AS max_game_date,
          MAX(created_at) AS max_created_at
        FROM mlb.model_training_props
        WHERE player_id = %s
          AND prop_source = 'mlb_api'
    """

    timings: List[tuple[str, float, int]] = []

    def run_or_empty(name: str, sql: str) -> List[Dict[str, Any]]:
        start = time.perf_counter()
        rows: List[Dict[str, Any]] = []
        try:
            rows = pg_fetchall(sql, (pid,))
        except Exception:
            pass
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        timings.append((name, elapsed_ms, len(rows)))
        return rows

    def run_one_or_empty(name: str, sql: str) -> Dict[str, Any]:
        rows = run_or_empty(name, sql)
        return rows[0] if rows else {}

    payload = {
        "recent_props": run_or_empty("recent_props", recent_props_sql)
        if "recent_props" in selected_sections
        else [],
        "streaks": run_or_empty("streaks", streaks_sql) if "streaks" in selected_sections else [],
        "stat_derived": run_or_empty("stat_derived", stat_derived_sql)
        if "stat_derived" in selected_sections
        else [],
        "training_summary": run_or_empty("training_summary", training_summary_sql)
        if "training_summary" in selected_sections
        else [],
        "freshness_metadata": run_one_or_empty("freshness_metadata", freshness_sql)
        if "freshness_metadata" in selected_sections
        else {},
    }
    if timings:
        LOGGER.info(
            "MLB player profile timings player_id=%s sections=%s %s",
            pid,
            ",".join(sorted(selected_sections)),
            " ".join(f"{name}={elapsed_ms:.1f}ms/{rows}" for name, elapsed_ms, rows in timings),
        )
    return payload
