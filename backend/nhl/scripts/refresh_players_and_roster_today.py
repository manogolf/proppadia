#!/usr/bin/env python3
"""
refresh_players_and_roster_today.py

Resilient daily "ensure" step:
- ONLINE: fetch roster per team from api-web.nhle.com, stage players → run
          upsert_players_from_stage.sql; stage roster rows → merge into
          nhl.roster_status (temp table).
- OFFLINE (API down or disabled): derive (game_id, team_id, player_id) from
          v_slate_* feature views and UPSERT nhl.roster_status directly
          (no temp tables). We DO NOT fabricate placeholder player names
          (to avoid CHECK constraints); we only write roster rows whose
          players already exist (or that we can name via API lookup).

Env:
  SLATE_DATE=YYYY-MM-DD (defaults to Pacific today)
  SUPABASE_DB_URL / DATABASE_URL
  NHL_FETCH_DISABLE=1  # force offline path
"""

import json
import os, sys, datetime as dt, re
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo
import datetime as dt, os, sys


# ---- absolutely disable server-side prepares (must run before importing psycopg) ----
import os
os.environ.setdefault("PSYCOPG_DISABLE_PREPARES", "1")

import psycopg
from psycopg.rows import dict_row

# Force every cursor.execute(...) to use simple execution (no PREPARE)
_ORIG_EXECUTE = psycopg.Cursor.execute
def _no_prep_execute(self, query, params=None, **kw):
    kw["prepare"] = False
    return _ORIG_EXECUTE(self, query, params, **kw)
psycopg.Cursor.execute = _no_prep_execute

# Force executemany(...) to avoid PREPARE by delegating to execute() per item
_ORIG_EXECUTEMANY = psycopg.Cursor.executemany
def _no_prep_executemany(self, query, params_seq=None, **kw):
    # Don’t pass prepare=… here; executemany() doesn’t accept it in psycopg3.
    # Instead, call our patched execute() which already forces prepare=False.
    for params in (params_seq or []):
        _no_prep_execute(self, query, params, **kw)
    return None
psycopg.Cursor.executemany = _no_prep_executemany

# Optional: load .env locally
try:
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv())
except Exception:
    pass

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

from backend.nhl.official_request_journal import (
    ENV_REQUIRED,
    ROSTER_REDIRECT_POLICY,
    RequestContext,
    official_get,
    official_season_id,
)
from backend.nhl.player_external_identity import localized_text
from backend.nhl.player_stage_normalizer import (
    PlayerStageConflict,
    normalize_player_stage_rows,
    normalize_position as _normalize_pos,
)
from backend.nhl.daily_capture import (
    CanonicalGame, canonical_game_set_hash, iso_utc, sha256_file, utc_now,
    write_roster_observation,
)
from backend.nhl.daily_orchestration import verify_roster_observation_reuse

# ---------------- Config ----------------
PACIFIC = ZoneInfo("America/Los_Angeles")
SLATE_DATE = os.environ.get("SLATE_DATE") or dt.datetime.now(PACIFIC).date().isoformat()

DB_URL = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
if not DB_URL:
    sys.exit("Missing SUPABASE_DB_URL / DATABASE_URL")
if "?sslmode=" not in DB_URL and "&sslmode=" not in DB_URL:
    DB_URL += ("&" if "?" in DB_URL else "?") + "sslmode=require"
if "?gssencmode=" not in DB_URL and "&gssencmode=" not in DB_URL:
    DB_URL += ("&" if "?" in DB_URL else "?") + "gssencmode=disable"

BASE = "https://api-web.nhle.com/v1"
FETCH_DISABLED = os.environ.get("NHL_FETCH_DISABLE", "0") == "1"
REUSE_ROSTER_OBSERVATION = os.environ.get("NHL_REUSE_ROSTER_OBSERVATION", "").strip()

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
SQL_DIR = os.path.join(ROOT, "backend", "nhl", "sql")
UPsertPlayersSQL = os.path.join(SQL_DIR, "upsert_players_from_stage.sql")

# ---------------- HTTP session ----------------
def _session() -> requests.Session:
    r = Retry(
        total=6, connect=6, read=4,
        backoff_factor=0.75,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    s = requests.Session()
    s.headers.update({"User-Agent": "proppadia/refresh-players-roster (requests)"})
    s.mount("https://", HTTPAdapter(max_retries=r))
    s.mount("http://", HTTPAdapter(max_retries=r))
    return s

S = _session()
ROSTER_RESPONSE_VARIANT_BY_TEAM: dict[str, tuple[str, str]] = {}
ROSTER_SOURCE_RESPONSES: list[dict] = []
DATABASE_TRANSACTION_ENTERED = False

# ---------------- Helpers ----------------
PLACEHOLDER_RE = re.compile(r"^\s*(?:player|unknown)\s+\d+\s*$", re.IGNORECASE)

def is_placeholder(name: str | None) -> bool:
    if not name or not str(name).strip():
        return True
    return PLACEHOLDER_RE.match(str(name)) is not None

def season_start_year_from_date(iso_date: str) -> int:
    """
    Project rule: NHL season is the 4-digit season start year.
    Examples:
      2025-12-23 -> 2025
      2026-01-15 -> 2025
    """
    y, m, _ = map(int, iso_date.split("-"))
    start = y if m >= 7 else y - 1
    return int(start)

def _safe_str(v):
    return localized_text(v)

def fetch_player_name_strict(nhl_pid: int | str) -> str | None:
    try:
        resp = official_get(
            f"{BASE}/player/{nhl_pid}/landing", timeout=8, session=S,
            stage="ROSTER_COLLECTION", endpoint_family="PLAYER_LANDING",
            identity={"slate_date": SLATE_DATE, "player_id": int(nhl_pid)},
            max_attempts=7, retry_statuses={429, 500, 502, 503, 504},
            backoff_seconds=0.75, preserve_response=True,
        )
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        j = resp.json() or {}
        for k in ("fullName", "playerName", "name"):
            v = _safe_str(j.get(k))
            if v and not is_placeholder(v):
                return v
        returned_id = j.get("playerId") or j.get("id")
        if returned_id is not None and int(returned_id) != int(nhl_pid):
            raise RuntimeError("PLAYER_LANDING_IDENTITY_MISMATCH")
        first, last = _safe_str(j.get("firstName")), _safe_str(j.get("lastName"))
        if first or last:
            nm = f"{first or ''} {last or ''}".strip()
            if nm and not is_placeholder(nm):
                return nm
    except Exception:
        if os.environ.get(ENV_REQUIRED) == "1":
            raise
        pass
    return None

def _validate_roster_names(players_stage: list[dict]) -> None:
    need = [row for row in players_stage if not row.get("first_name") and not row.get("last_name")]
    if need:
        identities = sorted({int(row["player_id"]) for row in need})
        raise RuntimeError(
            f"VERIFIED_ROSTER_NAMES_MISSING:{len(identities)}:{identities[:20]}")

def upsert_players_from_stage(cur) -> None:
    with open(UPsertPlayersSQL, "r") as f:
        cur.execute(f.read())


def stage_and_upsert_players(cur, players_stage: list[dict]) -> dict[str, int]:
    """Normalize the full player batch before performing any player DML."""
    try:
        normalized, counts = normalize_player_stage_rows(players_stage)
    except PlayerStageConflict as error:
        print("[player-upsert-normalization] " + json.dumps(
            error.counts, sort_keys=True, separators=(",", ":")))
        raise

    _validate_roster_names(normalized)
    print("[player-upsert-normalization] " + json.dumps(
        counts, sort_keys=True, separators=(",", ":")))
    cur.execute("TRUNCATE nhl.import_players_stage;")
    cur.executemany("""
        INSERT INTO nhl.import_players_stage
            (player_id, team_id, first_name, last_name, "position", shoots_catches, active)
        VALUES (%(player_id)s, %(team_id)s, %(first_name)s, %(last_name)s,
                %(position)s, %(shoots_catches)s, %(active)s)
    """, normalized)
    upsert_players_from_stage(cur)
    return counts

ROSTER_STATUS_KEY_COLUMNS = ("game_id", "team_id", "player_id")
ROSTER_STATUS_OPTIONAL_COLUMNS = ("active_flag", "line_role", "pp_unit", "asof_ts")


class RosterStatusConflict(RuntimeError):
    """One roster natural key has contradictory protected source values."""

    def __init__(self, *, conflicts: list[dict[str, Any]], counts: dict[str, int]):
        self.conflicts = conflicts
        self.counts = counts
        detail = ";".join(
            f"{'/'.join(map(str, item['key']))}:{','.join(item['fields'])}"
            for item in conflicts
        )
        super().__init__(f"ROSTER_STATUS_PROTECTED_CONFLICT:{detail}")


def _nullable_roster_text(value: Any, *, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"ROSTER_STATUS_{field.upper()}_NOT_TEXT")
    value = value.strip()
    return value or None


def normalize_roster_status_rows(
    rows: Iterable[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Collapse a roster batch before staging it for database mutation.

    API/reuse rows do not yet have a game id, so their pre-expansion natural
    key is ``(game_date, team_id, player_id)``. Rows that already carry a game
    id use the destination key ``(game_id, team_id, player_id)``. Nullable role
    values can complement one another; different non-null values are protected
    conflicts and reject the complete batch.
    """
    grouped: dict[tuple[Any, int, int], list[dict[str, Any]]] = {}
    source_count = 0
    for source in rows:
        date_or_game = source.get("game_id")
        key_name = "game_id"
        if date_or_game is None:
            date_or_game = source.get("game_date")
            key_name = "game_date"
        if date_or_game is None:
            raise ValueError("ROSTER_STATUS_GAME_IDENTITY_MISSING")
        if key_name == "game_id":
            try:
                date_or_game = int(date_or_game)
            except (TypeError, ValueError) as error:
                raise ValueError("ROSTER_STATUS_GAME_ID_INVALID") from error
        else:
            try:
                date_or_game = dt.date.fromisoformat(str(date_or_game)).isoformat()
            except ValueError as error:
                raise ValueError("ROSTER_STATUS_GAME_DATE_INVALID") from error
        try:
            team_id = int(source["team_id"])
            player_id = int(source["player_id"])
        except (KeyError, TypeError, ValueError) as error:
            raise ValueError("ROSTER_STATUS_NATURAL_KEY_INVALID") from error

        active = source.get("active_flag")
        if not isinstance(active, bool):
            raise ValueError(
                f"ROSTER_STATUS_ACTIVE_FLAG_NOT_BOOLEAN:{date_or_game}/{team_id}/{player_id}")
        normalized = {
            key_name: date_or_game,
            "team_id": team_id,
            "player_id": player_id,
            "active_flag": active,
            "line_role": _nullable_roster_text(source.get("line_role"), field="line_role"),
            "pp_unit": _nullable_roster_text(source.get("pp_unit"), field="pp_unit"),
        }
        grouped.setdefault((date_or_game, team_id, player_id), []).append(normalized)
        source_count += 1

    counts = {
        "source_rows": source_count,
        "unique_identities": len(grouped),
        "duplicate_source_rows": source_count - len(grouped),
        "exact_rows_collapsed": 0,
        "complementary_groups_merged": 0,
        "conflicting_groups_rejected": 0,
    }
    output: list[dict[str, Any]] = []
    conflicts: list[dict[str, Any]] = []
    for key in sorted(grouped):
        group = grouped[key]
        signatures = {
            (row["active_flag"], row["line_role"], row["pp_unit"])
            for row in group
        }
        counts["exact_rows_collapsed"] += len(group) - len(signatures)
        fields: list[str] = []
        merged: dict[str, Any] = {
            ("game_id" if "game_id" in group[0] else "game_date"): key[0],
            "team_id": key[1],
            "player_id": key[2],
        }
        for field in ("active_flag", "line_role", "pp_unit"):
            values = {row[field] for row in group if row[field] is not None}
            if len(values) > 1:
                fields.append(field)
            else:
                merged[field] = next(iter(values), None)
        if fields:
            conflicts.append({"key": key, "fields": sorted(fields)})
            continue
        if len(signatures) > 1:
            counts["complementary_groups_merged"] += 1
        output.append(merged)

    counts["conflicting_groups_rejected"] = len(conflicts)
    if conflicts:
        raise RosterStatusConflict(conflicts=conflicts, counts=counts)
    return output, counts


def roster_status_columns(cur) -> frozenset[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'nhl'
          AND table_name = 'roster_status'
        ORDER BY ordinal_position
        """,
    )
    columns = frozenset(
        str(row["column_name"] if isinstance(row, dict) else row[0])
        for row in cur.fetchall()
    )
    missing = sorted(set(ROSTER_STATUS_KEY_COLUMNS) - columns)
    if missing:
        raise RuntimeError(f"ROSTER_STATUS_REQUIRED_COLUMNS_MISSING:{','.join(missing)}")
    return columns


def _roster_status_upsert_parts(
    *, target_columns: Iterable[str], source_alias: str,
) -> tuple[str, str, str]:
    """
    Build INSERT cols / SELECT cols / UPDATE SET fragments for nhl.roster_status
    while tolerating optional columns on older DB schemas.
    """
    target_columns = frozenset(target_columns)
    missing = sorted(set(ROSTER_STATUS_KEY_COLUMNS) - target_columns)
    if missing:
        raise RuntimeError(f"ROSTER_STATUS_REQUIRED_COLUMNS_MISSING:{','.join(missing)}")
    insert_cols = list(ROSTER_STATUS_KEY_COLUMNS)
    select_cols = [f"{source_alias}.{column}" for column in ROSTER_STATUS_KEY_COLUMNS]
    update_set: list[str] = []

    if "active_flag" in target_columns:
        insert_cols.append("active_flag")
        select_cols.append(f"{source_alias}.active_flag")
        update_set.append("active_flag = EXCLUDED.active_flag")

    for column in ("line_role", "pp_unit"):
        if column in target_columns:
            insert_cols.append(column)
            select_cols.append(f"{source_alias}.{column}")
            update_set.append(
                f"{column} = COALESCE(EXCLUDED.{column}, nhl.roster_status.{column})")

    if "asof_ts" in target_columns:
        insert_cols.append("asof_ts")
        select_cols.append("now()")
        update_set.append("asof_ts = now()")

    return ", ".join(insert_cols), ", ".join(select_cols), ",\n              ".join(update_set)


def _roster_conflict_predicate(target_columns: Iterable[str], source_alias: str) -> str:
    protected = [column for column in ("line_role", "pp_unit") if column in target_columns]
    if not protected:
        return "FALSE"
    return " OR ".join(
        f"({source_alias}.{column} IS NOT NULL "
        f"AND existing.{column} IS NOT NULL "
        f"AND {source_alias}.{column} IS DISTINCT FROM existing.{column})"
        for column in protected
    )

def _on_conflict_sql(update_set: str) -> str:
    if not update_set:
        return "ON CONFLICT (game_id, team_id, player_id) DO NOTHING"
    return (
        "ON CONFLICT (game_id, team_id, player_id) DO UPDATE\n"
        f"          SET {update_set}"
    )


def merge_roster_status_from_temp(
    cur, slate_date: str, *, target_columns: Iterable[str] | None = None,
) -> int | None:
    """
    Merge roster rows either from tmp_import_roster (if present) or,
    as a fallback, from slate feature views for the given slate_date.
    Enforces FK to nhl.players to avoid violations.
    """
    cur.execute("""
        SELECT
          (to_regclass('pg_temp.tmp_import_roster') IS NOT NULL)
          OR (to_regclass('tmp_import_roster') IS NOT NULL) AS has_tmp
    """)
    row = cur.fetchone()
    has_tmp = bool(row["has_tmp"] if isinstance(row, dict) else row[0])

    target_columns = (
        frozenset(target_columns) if target_columns is not None else roster_status_columns(cur)
    )

    if has_tmp:
        # NOTE: tmp_import_roster has (game_date, team_id, player_id, active_flag, pp_unit).
        # Resolve the target game_id from date+team, then enforce player FK.
        insert_cols, select_cols, update_set = _roster_status_upsert_parts(
            target_columns=target_columns,
            source_alias="s",
        )
        conflict_predicate = _roster_conflict_predicate(target_columns, "checked")
        on_conflict = _on_conflict_sql(update_set)
        cur.execute(f"""
        WITH src AS (
          SELECT
            g.game_id,
            r.team_id,
            r.player_id,
            COALESCE(r.active_flag, TRUE)::boolean AS active_flag,
            NULL::text                           AS line_role,
            r.pp_unit::text                      AS pp_unit
          FROM tmp_import_roster r
          JOIN nhl.games g
            ON g.game_date = r.game_date::date
           AND (g.home_team_id = r.team_id OR g.away_team_id = r.team_id)
          WHERE g.game_date = %s::date
        ),
        src_checked AS (
          SELECT DISTINCT
            s.game_id, s.team_id, s.player_id,
            s.active_flag, s.line_role, s.pp_unit
          FROM src s
          JOIN nhl.players p ON p.player_id = s.player_id  -- FK guard
        ),
        protected_conflicts AS (
          SELECT checked.game_id, checked.team_id, checked.player_id
          FROM src_checked checked
          JOIN nhl.roster_status existing
            USING (game_id, team_id, player_id)
          WHERE {conflict_predicate}
        ),
        conflict_guard AS (
          SELECT CAST(
            CASE WHEN COUNT(*) = 0 THEN '1' ELSE 'ROSTER_STATUS_PROTECTED_CONFLICT' END
            AS integer
          ) AS ok
          FROM protected_conflicts
        ),
        guarded_source AS (
          SELECT checked.game_id, checked.team_id, checked.player_id,
                 checked.active_flag, checked.line_role, checked.pp_unit
          FROM src_checked checked
          CROSS JOIN conflict_guard guard
          WHERE guard.ok = 1
        )
        INSERT INTO nhl.roster_status (
          {insert_cols}
        )
        SELECT
          {select_cols}
        FROM guarded_source s
        {on_conflict};
        """, (slate_date,))
        affected = getattr(cur, "rowcount", None)
        return int(affected) if isinstance(affected, int) and affected >= 0 else None
    else:
        return upsert_roster_status_from_features(
            cur, slate_date, target_columns=target_columns)["roster_status_upsert_rows"]

def ensure_players_exist(cur, player_ids: list[int]) -> None:
    """
    Ensure players exist WITHOUT violating any "no placeholder" CHECKs.
    Only insert when a real, non-placeholder name can be fetched.
    """
    if not player_ids:
        return
    cur.execute("SELECT player_id FROM nhl.players WHERE player_id = ANY(%s);", (player_ids,))
    have_rows = cur.fetchall()
    have = { (r["player_id"] if isinstance(r, dict) else r[0]) for r in have_rows }
    missing = [int(pid) for pid in set(player_ids) if pid not in have]
    if not missing:
        return

    to_insert = []
    for pid in missing:
        nm = fetch_player_name_strict(pid)
        if nm:
            to_insert.append({"pid": pid, "full_name": nm})

    if to_insert:
        cur.executemany(
            """
            INSERT INTO nhl.players (player_id, full_name, position, status, active, updated_at)
            VALUES (%(pid)s, %(full_name)s, 'F', 'active', TRUE, now())
            ON CONFLICT (player_id) DO UPDATE
              SET active = TRUE,
                  updated_at = now();
            """,
            to_insert,
        )
        print(f"[info] players: inserted/activated {len(to_insert)} with real names")
    unresolved = len(missing) - len(to_insert)
    if unresolved > 0:
        print(f"[warn] players: {unresolved} missing player_ids had no safe name; they will be skipped by FK guard")

def _dedupe_roster_rows(rows: list[dict]) -> list[dict]:
    """Compatibility wrapper for callers that only need normalized rows."""
    return normalize_roster_status_rows(rows)[0]

def _feature_roster_source_sql() -> str:
    """Explicit governed fallback source with a stable, typed column contract."""
    return """
      SELECT
        f.game_id::bigint AS game_id,
        f.team_id::bigint AS team_id,
        f.player_id::bigint AS player_id,
        TRUE::boolean AS active_flag,
        NULL::text AS line_role,
        NULL::text AS pp_unit
      FROM nhl.v_slate_sog_features f
      JOIN nhl.games g
        ON g.game_id = f.game_id
       AND g.game_date = %s::date
       AND (g.home_team_id = f.team_id OR g.away_team_id = f.team_id)
      WHERE f.game_date = %s::date
      UNION ALL
      SELECT
        f.game_id::bigint AS game_id,
        f.team_id::bigint AS team_id,
        f.player_id::bigint AS player_id,
        TRUE::boolean AS active_flag,
        NULL::text AS line_role,
        NULL::text AS pp_unit
      FROM nhl.v_slate_saves_features f
      JOIN nhl.games g
        ON g.game_id = f.game_id
       AND g.game_date = %s::date
       AND (g.home_team_id = f.team_id OR g.away_team_id = f.team_id)
      WHERE f.game_date = %s::date
    """


def upsert_roster_status_from_features(
    cur, slate_date: str, *, target_columns: Iterable[str] | None = None,
) -> dict[str, int]:
    """Offline UPSERT directly from feature views (no temp table), FK-safe."""
    target_columns = (
        frozenset(target_columns) if target_columns is not None else roster_status_columns(cur)
    )
    insert_cols, select_cols, update_set = _roster_status_upsert_parts(
        target_columns=target_columns,
        source_alias="sc",
    )
    source_sql = _feature_roster_source_sql()
    source_params = (slate_date, slate_date, slate_date, slate_date)
    conflict_predicate = _roster_conflict_predicate(target_columns, "checked")
    on_conflict = _on_conflict_sql(update_set)
    cur.execute(f"""
    WITH source_rows AS ({source_sql}),
    source_validation AS (
      SELECT CAST(
        CASE WHEN COUNT(*) = 0 THEN '1' ELSE 'ROSTER_STATUS_NATURAL_KEY_INVALID' END
        AS integer
      ) AS ok
      FROM source_rows
      WHERE game_id IS NULL OR team_id IS NULL OR player_id IS NULL
    ),
    normalized AS (
      SELECT game_id, team_id, player_id,
             TRUE::boolean AS active_flag,
             NULL::text AS line_role,
             NULL::text AS pp_unit
      FROM source_rows
      CROSS JOIN source_validation validation
      WHERE validation.ok = 1
      GROUP BY game_id, team_id, player_id
    ),
    src_checked AS (
      SELECT n.game_id, n.team_id, n.player_id,
             n.active_flag, n.line_role, n.pp_unit
      FROM normalized n
      JOIN nhl.players p ON p.player_id = n.player_id  -- FK guard
    ),
    protected_conflicts AS (
      SELECT checked.game_id, checked.team_id, checked.player_id
      FROM src_checked checked
      JOIN nhl.roster_status existing USING (game_id, team_id, player_id)
      WHERE {conflict_predicate}
    ),
    conflict_guard AS (
      SELECT CAST(
        CASE WHEN COUNT(*) = 0 THEN '1' ELSE 'ROSTER_STATUS_PROTECTED_CONFLICT' END
        AS integer
      ) AS ok
      FROM protected_conflicts
    ),
    guarded_source AS (
      SELECT checked.game_id, checked.team_id, checked.player_id,
             checked.active_flag, checked.line_role, checked.pp_unit
      FROM src_checked checked
      CROSS JOIN conflict_guard guard
      WHERE guard.ok = 1
    )
    upserted AS (
      INSERT INTO nhl.roster_status (
        {insert_cols}
      )
      SELECT
        {select_cols}
      FROM guarded_source sc
      {on_conflict}
      RETURNING game_id
    )
    SELECT
      (SELECT COUNT(*)::int FROM source_rows) AS source_rows,
      (SELECT COUNT(*)::int FROM normalized) AS unique_identities,
      (
        (SELECT COUNT(*)::int FROM source_rows)
        - (SELECT COUNT(*)::int FROM normalized)
      ) AS exact_rows_collapsed,
      0::int AS complementary_groups_merged,
      0::int AS conflicting_groups_rejected,
      (SELECT COUNT(*)::int FROM upserted) AS roster_status_upsert_rows;
    """, source_params)
    row = cur.fetchone()
    if row is None:
        raise RuntimeError("ROSTER_FEATURE_NORMALIZATION_COUNTS_MISSING")
    names = (
        "source_rows", "unique_identities", "exact_rows_collapsed",
        "complementary_groups_merged", "conflicting_groups_rejected",
        "roster_status_upsert_rows",
    )
    counts = {
        name: int(row[name] if isinstance(row, dict) else row[index])
        for index, name in enumerate(names)
    }
    return counts

def fetch_roster(team_tri: str, when_iso: str) -> list[dict]:
    """
    Fetch roster using team tri-code (e.g., 'LAK'). Try /current then explicit season.
    Returns items with person.id, position.code, firstName/lastName when present.
    """
    tri = str(team_tri).upper()
    season = season_start_year_from_date(when_iso)
    official_season = official_season_id(season)
    urls = [
        f"{BASE}/roster/{tri}/current",
        f"{BASE}/roster/{tri}/{official_season}",
    ]

    reused = ROSTER_RESPONSE_VARIANT_BY_TEAM.get(tri)
    context = RequestContext.from_env()
    current_identity = {"slate_date": when_iso, "team": tri, "roster_variant": "current"}
    if reused is None and context is not None and context.has_declared_response(
            "ROSTER", current_identity):
        reused = (urls[0], "current")
    if reused is not None:
        url, variant = reused
        resp = official_get(
            url, timeout=20, session=S, stage="ROSTER_COLLECTION",
            endpoint_family="ROSTER",
            identity={"slate_date": when_iso, "team": tri, "roster_variant": variant},
            request_class="PRIMARY" if variant == "current" else "FALLBACK",
            reuse_preserved=True,
        )
        resp.raise_for_status()
        j = resp.json() or {}
        ROSTER_SOURCE_RESPONSES.append({
            "team": tri,
            "requested_source_url": url,
            "resolved_source_url": str(getattr(resp, "url", url)),
            "observed_at_utc": iso_utc(utc_now()),
            "payload": j,
        })
        out: list[dict] = []
        _append_from_section(out, j.get("forwards"), "F")
        _append_from_section(out, j.get("defensemen") or j.get("defense"), "D")
        _append_from_section(out, j.get("goalies"), "G")
        if not out and isinstance(j.get("roster"), dict):
            roster = j["roster"]
            _append_from_section(out, roster.get("forwards"), "F")
            _append_from_section(out, roster.get("defensemen") or roster.get("defense"), "D")
            _append_from_section(out, roster.get("goalies"), "G")
        if not out:
            raise RuntimeError("PRESERVED_ROSTER_RESPONSE_BECAME_EMPTY")
        return out

    for index, url in enumerate(urls):
        variant = "current" if index == 0 else official_season
        resp = official_get(
            url, timeout=20, session=S, stage="ROSTER_COLLECTION",
            endpoint_family="ROSTER", identity={"slate_date": when_iso, "team": tri,
                                                  "roster_variant": variant},
            max_attempts=7, retry_statuses={429, 500, 502, 503, 504},
            backoff_seconds=0.75, request_class="PRIMARY" if index == 0 else "FALLBACK",
            preserve_response=True,
            redirect_policy=({"policy": ROSTER_REDIRECT_POLICY, "team": tri,
                              "repository_season": season} if index == 0 else None),
        )
        if resp.status_code == 404:
            continue
        resp.raise_for_status()
        j = resp.json() or {}
        ROSTER_SOURCE_RESPONSES.append({
            "team": tri,
            "requested_source_url": url,
            "resolved_source_url": str(getattr(resp, "url", url)),
            "observed_at_utc": iso_utc(utc_now()),
            "payload": j,
        })

        out: list[dict] = []

        # api-web uses "defensemen" (not "defense") for the roster endpoint
        _append_from_section(out, j.get("forwards"), "F")
        _append_from_section(out, j.get("defensemen") or j.get("defense"), "D")
        _append_from_section(out, j.get("goalies"), "G")

        # some variants wrap under {"roster": {...}}
        if not out and isinstance(j.get("roster"), dict):
            r = j["roster"]
            _append_from_section(out, r.get("forwards"), "F")
            _append_from_section(out, r.get("defensemen") or r.get("defense"), "D")
            _append_from_section(out, r.get("goalies"), "G")

        if out:
            if RequestContext.from_env() is not None:
                ROSTER_RESPONSE_VARIANT_BY_TEAM[tri] = (url, variant)
            return out

    return []

def _append_from_section(out: list[dict], section: list | None, default_pos: str) -> None:
    """
    Normalizes a roster "section" list into the api-web style items:
      {"person":{"id":...}, "position":{"code":...}, "firstName":..., "lastName":...}
    Works with both shapes:
      - keys like id/playerId, firstName/lastName (strings)
      - keys like player:{id:...}
    """
    for p in (section or []):
        pid = p.get("id") or p.get("playerId") or (p.get("player") or {}).get("id")
        if not pid:
            continue

        pos = _normalize_pos(p.get("positionCode") or p.get("position") or default_pos) or default_pos

        first = _safe_str(p.get("firstName"))
        last  = _safe_str(p.get("lastName"))

        out.append({
            "person":    {"id": int(pid)},
            "position":  {"code": pos},
            "firstName": first,
            "lastName":  last,
        })

# ---------------- Main ----------------
def main():
    global DATABASE_TRANSACTION_ENTERED
    DATABASE_TRANSACTION_ENTERED = False
    source = "features-fallback"
    normalizer_counts: dict[str, int] = {}
    roster_normalizer_counts: dict[str, int] = {}
    roster_status_upsert_rows = -1
    roster_observation_reference: dict | None = None
    with psycopg.connect(DB_URL, prepare_threshold=None, row_factory=dict_row) as conn:
        try:
            conn.prepare_threshold = None  # type: ignore[attr-defined]
        except Exception:
            pass

        # 1) Fetch slate games
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                  g.game_id,
                  g.start_time_utc,
                  g.home_team_id,
                  g.away_team_id,
                  ht.team AS home_tri,
                  at.team AS away_tri
                FROM nhl.games g
                JOIN nhl.teams ht ON ht.team_id = g.home_team_id
                JOIN nhl.teams at ON at.team_id = g.away_team_id
               WHERE g.game_date = %s::date
               ORDER BY g.game_id
            """, (SLATE_DATE,))
            games = cur.fetchall()

        if not games:
            print(f"ℹ️ No games for {SLATE_DATE} in nhl.games; run import_schedule_today.py first.")
            return

        players_stage = []
        roster_rows   = []

        canonical_games = [CanonicalGame(
            game_id=int(game["game_id"]),
            start_time_utc=iso_utc(game["start_time_utc"]),
            home_team=str(game["home_tri"]),
            away_team=str(game["away_tri"]),
            home_aliases=(str(game["home_tri"]),),
            away_aliases=(str(game["away_tri"]),),
        ) for game in games]

        # 2a) Explicit immutable reuse path. It validates and loads before any
        # roster DML, performs no HTTP, and creates no replacement observation.
        if REUSE_ROSTER_OBSERVATION:
            season = season_start_year_from_date(SLATE_DATE)
            game_hash = canonical_game_set_hash(game.game_id for game in canonical_games)
            roster_observation_reference = verify_roster_observation_reuse(
                Path(REUSE_ROSTER_OBSERVATION), season=season, slate_date=SLATE_DATE,
                phase=os.environ.get("NHL_DAILY_PHASE", "EARLY"),
                canonical_game_ids=[game.game_id for game in canonical_games],
                canonical_game_set_hash=game_hash,
            )
            team_ids = {
                str(game["home_tri"]).upper(): int(game["home_team_id"])
                for game in games
            }
            team_ids.update({
                str(game["away_tri"]).upper(): int(game["away_team_id"])
                for game in games
            })
            for raw in (Path(REUSE_ROSTER_OBSERVATION) / "roster_snapshot.jsonl").read_text().splitlines():
                row = json.loads(raw)
                team = str(row["team"]).upper()
                if team not in team_ids:
                    raise RuntimeError(f"ROSTER_REUSE_TEAM_ID_UNRESOLVED:{team}")
                players_stage.append({
                    "player_id": int(row["player_id"]), "team_id": team_ids[team],
                    "first_name": row.get("first_name"), "last_name": row.get("last_name"),
                    "position": row.get("position"), "shoots_catches": None, "active": True,
                })
                roster_rows.append({
                    "game_date": SLATE_DATE, "team_id": team_ids[team],
                    "player_id": int(row["player_id"]), "active_flag": True, "pp_unit": None,
                })
            if not players_stage or not roster_rows:
                raise RuntimeError("ROSTER_REUSE_SNAPSHOT_EMPTY")
            source = "REUSED_IMMUTABLE_OBSERVATION"

        # 2b) ONLINE PATH
        elif not FETCH_DISABLED:
            try:
                for g in games:
                    for tri, team_id in ((g["home_tri"], g["home_team_id"]),
                                         (g["away_tri"], g["away_team_id"])):
                        try:
                            roster = fetch_roster(tri, SLATE_DATE) or []
                        except Exception as e:
                            if os.environ.get(ENV_REQUIRED) == "1":
                                raise
                            print(f"[warn] roster fetch failed for {tri}: {e}")
                            roster = []

                        for item in roster:
                            person = item.get("person") or {}
                            pid = person.get("id")
                            if pid is None:
                                continue
                            pid = int(pid)
                            pos   = _normalize_pos(((item.get("position") or {}) or {}).get("code")) or "F"
                            first = item.get("firstName") or None
                            last  = item.get("lastName")  or None

                            players_stage.append({
                                "player_id": pid,
                                "team_id": int(team_id),
                                "first_name": first,
                                "last_name": last,
                                "position": pos,
                                "shoots_catches": None,
                                "active": True,
                            })
                            roster_rows.append({
                                "game_date": SLATE_DATE,
                                "team_id": int(team_id),
                                "player_id": pid,
                                "active_flag": True,
                                "pp_unit": None,
                            })
                if players_stage or roster_rows:
                    source = "API"
            except Exception as e:
                if os.environ.get(ENV_REQUIRED) == "1":
                    raise
                print(f"[warn] NHL API fetch failed: {e}")

        # The immutable observation is source evidence, not a side effect of
        # database persistence.  Verify and durably bind it before any roster
        # or player DML so an evidence failure cannot follow a committed write.
        observation_root = os.environ.get("NHL_ROSTER_OBSERVATION_ROOT", "").strip()
        if observation_root and source == "API":
            roster_observation = write_roster_observation(
                root=Path(observation_root),
                season=season_start_year_from_date(SLATE_DATE),
                slate_date=SLATE_DATE,
                phase=os.environ.get("NHL_DAILY_PHASE", "EARLY"),
                parent_daily_run_id=os.environ.get("NHL_PARENT_DAILY_RUN_ID", "UNBOUND_DAILY_RUN"),
                canonical_games=canonical_games,
                source_responses=ROSTER_SOURCE_RESPONSES,
            )
            print(f"ROSTER_OBSERVATION={roster_observation}")
            roster_observation_reference = {
                "path": str(roster_observation.resolve()),
                "manifest_sha256": sha256_file(roster_observation / "SHA256SUMS"),
                "reuse_mode": "NEW_IMMUTABLE_OBSERVATION",
            }
        elif observation_root and source != "REUSED_IMMUTABLE_OBSERVATION":
            raise RuntimeError("ROSTER_OBSERVATION_REQUIRES_OFFICIAL_RESPONSES")

        # Normalize the complete roster batch before any player, stage, or
        # roster-status DML. Source inclusion establishes active membership;
        # neither official roster payload supplies line/PP roles.
        if source in {"API", "REUSED_IMMUTABLE_OBSERVATION"} and roster_rows:
            try:
                roster_rows, roster_normalizer_counts = normalize_roster_status_rows(roster_rows)
            except RosterStatusConflict as error:
                print("[roster-status-normalization] " + json.dumps(
                    error.counts, sort_keys=True, separators=(",", ":")))
                raise
            print("[roster-status-normalization] " + json.dumps(
                roster_normalizer_counts, sort_keys=True, separators=(",", ":")))

        # 3) Decide path & write
        with conn.transaction():
            DATABASE_TRANSACTION_ENTERED = True
            with conn.cursor() as cur:
                if source in {"API", "REUSED_IMMUTABLE_OBSERVATION"} and players_stage and roster_rows:
                    # Validate the destination contract before any stage/player
                    # mutation in this transaction.
                    target_columns = roster_status_columns(cur)
                    normalizer_counts = stage_and_upsert_players(cur, players_stage)

                    cur.execute("""
                        CREATE TEMP TABLE tmp_import_roster (
                          game_date date,
                          team_id bigint,
                          player_id bigint,
                          active_flag boolean,
                          pp_unit text
                        ) ON COMMIT DROP;
                    """)
                    cur.executemany("""
                        INSERT INTO tmp_import_roster (game_date, team_id, player_id, active_flag, pp_unit)
                        VALUES (%(game_date)s, %(team_id)s, %(player_id)s, %(active_flag)s, %(pp_unit)s)
                    """, roster_rows)

                    merged_rows = merge_roster_status_from_temp(
                        cur, SLATE_DATE, target_columns=target_columns)
                    roster_status_upsert_rows = -1 if merged_rows is None else merged_rows

                else:
                    # The offline contract is feature-view-only. Missing player
                    # dimensions are excluded by the FK guard; no provider or
                    # identity manufacturing is allowed in this path.
                    roster_normalizer_counts = upsert_roster_status_from_features(
                        cur, SLATE_DATE)
                    roster_status_upsert_rows = roster_normalizer_counts[
                        "roster_status_upsert_rows"]

                cur.execute("""
                    SELECT COUNT(*) AS cnt
                    FROM nhl.roster_status rs
                    JOIN nhl.games g USING (game_id)
                    WHERE g.game_date = %s::date
                """, (SLATE_DATE,))
                row = cur.fetchone()
                total_rs = (row["cnt"] if isinstance(row, dict) else row[0])
                print(f"Refreshed players & roster_status for {SLATE_DATE} (source={source})")
                print(f"✅ roster_status rows present for {SLATE_DATE}: {total_rs}")

        child_summary = {
            "schema_version": "NHL_ROSTER_CHILD_SUMMARY_V2",
            "status": "COMPLETE",
            "normalizer_counts": normalizer_counts,
            "roster_observation": (
                roster_observation_reference.get("path")
                if roster_observation_reference else None
            ),
            "roster_manifest_sha256": (
                roster_observation_reference.get("manifest_sha256")
                if roster_observation_reference else None
            ),
            "database_stage_summary": {
                "source": source,
                "roster_status_rows_present": int(total_rs),
                "players_stage_rows": len(players_stage),
                "roster_stage_rows": len(roster_rows),
                "roster_normalizer_counts": roster_normalizer_counts,
            },
            "database_write_capable": True,
            "database_write_status": "COMMITTED",
            "transaction_disposition": "COMMITTED",
            "database_row_counts": {
                "roster_status_upsert_rows": int(roster_status_upsert_rows),
                "players_stage_rows": len(players_stage),
                "roster_stage_rows": len(roster_rows),
            },
            "database_row_counts_complete": (
                source == "features-fallback" and roster_status_upsert_rows >= 0
            ),
        }
    # The connection context has committed successfully before COMMITTED is
    # emitted for the parent recorder.
    print("NHL_CHILD_SUMMARY_JSON=" + json.dumps(
        child_summary, sort_keys=True, separators=(",", ":")))

if __name__ == "__main__":
    try:
        main()
    except BaseException as error:
        rolled_back = DATABASE_TRANSACTION_ENTERED
        print("NHL_CHILD_SUMMARY_JSON=" + json.dumps({
            "schema_version": "NHL_ROSTER_CHILD_SUMMARY_V2",
            "status": "FAILED",
            "database_write_capable": True,
            "database_write_status": "ROLLED_BACK" if rolled_back else "UNKNOWN",
            "transaction_disposition": "ROLLED_BACK" if rolled_back else "UNKNOWN",
            "database_row_counts": {},
            "database_row_counts_complete": False,
            "error_type": type(error).__name__,
        }, sort_keys=True, separators=(",", ":")))
        raise
