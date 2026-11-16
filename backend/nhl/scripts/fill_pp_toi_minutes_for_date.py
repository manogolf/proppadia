#!/usr/bin/env python3
"""
fill_pp_toi_minutes_for_date.py

Canonical daily fixer for nhl.skater_game_logs_raw.pp_toi_minutes.

- Reads SLATE_DATE (ET) or --date
- Finds regular-season games (game_type=2) on that date
- For each game, once the official-fill migration is applied (P2.45), fetches
  the NHL stats API `skater/timeonice` `ppTimeOnIce` (source of record since
  2026-11-15) and fills NULL rows through nhl.fill_pp_toi_minutes_from_official,
  zeros included. Otherwise, or when the stats API has no rows for the game,
  reconstructs positive PP overlap from official-derived manpower segments and
  player shiftcharts (the earlier method)
- Fills skater_game_logs_raw.pp_toi_minutes only where current value is NULL
- Once the verified-zero migration is applied, also writes 0.00 for NULL rows
  whose complete shifts overlap no own-team PP window (P2.31)
- Preserves populated values, including verified zero, and records disagreements
- Then enforces an invariant:
    For each (game_id, team_id) on that date, if SUM(toi_minutes)>0 and SUM(pp_toi_minutes)=0 -> FAIL

This prevents silent regressions and makes PP role/share auditable.

Usage:
  SLATE_DATE=2026-01-08 python backend/nhl/scripts/fill_pp_toi_minutes_for_date.py --commit
  python backend/nhl/scripts/fill_pp_toi_minutes_for_date.py --date 2026-01-08 --commit
  ... omit --commit to dry-run (no DB writes)

Env:
  SUPABASE_DB_URL or DATABASE_URL
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
import uuid
from typing import Any, Dict, Iterable, Optional, Tuple, List

import psycopg
import requests
from backend.nhl.database_access import database_url

from backend.nhl.pp_toi_source import pp_toi_conflict, write_pp_toi_conflict_diagnostic

from backend.nhl.official_request_journal import RequestContext, official_get, skip_voided_games


BOX_URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"
STATS_TOI_URL = "https://api.nhle.com/stats/rest/en/skater/timeonice"
OFFICIAL_FILL_FUNCTION = "nhl.fill_pp_toi_minutes_from_official(bigint,jsonb)"
UA = "proppadia-pp-toi-canonical/1.0"


def die(msg: str, code: int = 1) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(code)


def parse_mmss_to_minutes(val: Any) -> Optional[float]:
    if val is None:
        return None

    if isinstance(val, (int, float)):
        # If a large number, treat as seconds; otherwise minutes
        if val > 60:
            return float(val) / 60.0
        return float(val)

    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        m = re.match(r"^(\d+):(\d{2})$", s)
        if m:
            mm = int(m.group(1))
            ss = int(m.group(2))
            return mm + ss / 60.0

    return None


def walk_json(obj: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from walk_json(v)
    elif isinstance(obj, list):
        for it in obj:
            yield from walk_json(it)


def _pid_from_item(it: Dict[str, Any]) -> Optional[int]:
    # common direct forms
    for k in ("playerId", "player_id"):
        if k in it:
            try:
                return int(it[k])
            except Exception:
                pass

    # nested player object
    p = it.get("player")
    if isinstance(p, dict):
        for k in ("playerId", "id", "player_id"):
            if k in p:
                try:
                    return int(p[k])
                except Exception:
                    pass

    # sometimes "id" is player id in skater dicts, but often it's not — keep last.
    if "id" in it:
        try:
            return int(it["id"])
        except Exception:
            pass

    return None


def _pp_minutes_from_item(it: Dict[str, Any]) -> Optional[float]:
    # try all plausible keys (minutes or seconds)
    pp_keys = (
        "powerPlayTimeOnIce",
        "ppTimeOnIce",
        "ppToi",
        "powerPlayToi",
        "powerPlayTimeOnIceSeconds",
        "ppTimeOnIceSeconds",
        "ppToiSeconds",
    )
    for k in pp_keys:
        if k in it:
            mins = parse_mmss_to_minutes(it.get(k))
            if mins is not None:
                return float(mins)

    # nested stat dict variants
    for nest_key in ("stats", "skaterStats", "playerStats"):
        v = it.get(nest_key)
        if isinstance(v, dict):
            for k in pp_keys:
                if k in v:
                    mins = parse_mmss_to_minutes(v.get(k))
                    if mins is not None:
                        return float(mins)

    return None


def extract_pp_toi_minutes_from_db(cur, game_id: int) -> Dict[int, float]:
    """
    Build {player_id: pp_minutes} from overlap between:
      - nhl.game_manpower_segments (PP windows)
      - nhl.shiftcharts_raw (player shifts)
    """
    cur.execute(
        """
        WITH pp AS (
          SELECT DISTINCT game_id, period, start_sec, end_sec, pp_team_id AS team_id
          FROM nhl.game_manpower_segments
          WHERE game_id = %s
        ),
        shifts AS (
          -- The official Stats API can repeat an identical player interval
          -- under separate shift IDs. Those are one observed interval, not
          -- additive ice time.
          SELECT DISTINCT game_id, player_id, team_id, period, start_sec, end_sec
          FROM nhl.shiftcharts_raw
          WHERE game_id = %s
            AND player_id IS NOT NULL AND team_id IS NOT NULL
            AND period IS NOT NULL AND start_sec IS NOT NULL AND end_sec IS NOT NULL
            AND end_sec > start_sec
        ),
        ov AS (
          SELECT
            s.player_id::bigint AS player_id,
            SUM(
              GREATEST(
                0,
                LEAST(s.end_sec, pp.end_sec) - GREATEST(s.start_sec, pp.start_sec)
              )
            )::bigint AS pp_seconds
          FROM shifts s
          JOIN pp
            ON pp.game_id = s.game_id
           AND pp.period  = s.period
           AND pp.team_id = s.team_id
          WHERE s.game_id = %s
          GROUP BY 1
        )
        SELECT player_id, (pp_seconds / 60.0)::float AS pp_minutes
        FROM ov
        WHERE pp_seconds > 0;
        """,
        (game_id, game_id, game_id),
    )
    out: Dict[int, float] = {}
    for pid, mins in cur.fetchall():
        out[int(pid)] = float(mins)
    return out

def fetch_boxscore(game_id: int, timeout: int = 30) -> Dict[str, Any]:
    url = BOX_URL.format(game_id=game_id)
    context = RequestContext.from_env()
    session = requests.Session()
    session.headers.update({"User-Agent": UA})
    r = official_get(
        url, timeout=timeout, session=session, stage="PP_TOI_COLLECTION",
        endpoint_family="BOXSCORE",
        identity={"slate_date": os.environ.get("SLATE_DATE"), "game_id": int(game_id)},
        reuse_preserved=context is not None,
    )
    r.raise_for_status()
    return r.json()


def get_games_for_date(cur, slate_date: str) -> List[int]:
    cur.execute(
        """
        SELECT game_id::bigint
        FROM nhl.games
        WHERE game_date = %s::date
          AND game_type = 2;
        """,
        (slate_date,),
    )
    return [int(r[0]) for r in cur.fetchall()]


def update_game(
    cur, game_id: int, pp_map: Dict[int, float], *,
    target_relation: str = "nhl.skater_game_logs_raw",
) -> Tuple[int, int, int]:
    """
    Returns:
      (db_rows, overlap_players, rows_updated)
    Only fills NULL values. Populated values, including zero, are preserved.
    """
    parts = target_relation.split(".")
    if len(parts) not in (1, 2) or not all(part.isidentifier() for part in parts):
        raise ValueError("INVALID_PP_TOI_FILL_TARGET")
    if parts[-1] != "skater_game_logs_raw":
        raise ValueError("INVALID_PP_TOI_FILL_TARGET")
    schema = parts[-2] if len(parts) == 2 else "nhl"
    target = target_relation
    cur.execute(
        f"SELECT player_id::bigint FROM {target} WHERE game_id = %s;",
        (game_id,),
    )
    db_players = {int(r[0]) for r in cur.fetchall()}
    overlap = sorted(db_players.intersection(pp_map.keys()))
    if not overlap:
        return (len(db_players), 0, 0)

    # The database function recomputes positive PP/shift overlap from the
    # canonical source tables and can only fill currently NULL raw values.
    # Schema identifiers were checked above; no values enter SQL text.
    cur.execute(
        f'SELECT "{schema}".fill_pp_toi_minutes_from_shift_overlap(%s)',
        (game_id,),
    )
    result = cur.fetchone()
    updated = int(result[0]) if result else 0
    return (len(db_players), len(overlap), updated)


def official_fill_available(cur) -> bool:
    """True once the operator has applied the official-fill migration (P2.45)."""
    cur.execute("SELECT to_regprocedure(%s) IS NOT NULL;", (OFFICIAL_FILL_FUNCTION,))
    row = cur.fetchone()
    return bool(row and row[0])


def fetch_official_pp(game_id: int, timeout: int = 30) -> List[Dict[str, int]]:
    """Official per-player PP and total TOI seconds for one game from the stats API."""
    session = requests.Session()
    session.headers.update({"User-Agent": UA})
    r = official_get(
        STATS_TOI_URL, timeout=timeout, session=session, stage="PP_TOI_COLLECTION",
        endpoint_family="SKATER_TIME_ON_ICE",
        identity={"slate_date": os.environ.get("SLATE_DATE"), "game_id": int(game_id)},
        params={"isAggregate": "false", "isGame": "true", "limit": "-1",
                "cayenneExp": f"gameId={int(game_id)}"},
    )
    r.raise_for_status()
    payload = r.json()
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, list) or len(data) != payload.get("total", len(data)):
        raise RuntimeError("OFFICIAL_SKATER_TOI_RESPONSE_INCOMPLETE")
    rows = []
    for item in data:
        if int(item.get("gameId", -1)) != int(game_id):
            raise RuntimeError("OFFICIAL_SKATER_TOI_GAME_MISMATCH")
        rows.append({"player_id": int(item["playerId"]), "pp_seconds": int(item["ppTimeOnIce"]),
                     "toi_seconds": int(item["timeOnIce"])})
    return rows


def fill_from_official(cur, game_id: int, rows: List[Dict[str, int]]) -> int:
    """NULL-only fill through the fixed database function; never updates directly."""
    cur.execute("SELECT nhl.fill_pp_toi_minutes_from_official(%s, %s::jsonb)",
                (game_id, json.dumps(rows, separators=(",", ":"))))
    row = cur.fetchone()
    return int(row[0]) if row else 0


def collect_conflicts(cur, game_id: int, values: Dict[int, float], *, source_kind: str,
                      source_reference: str) -> List[Dict[str, Any]]:
    """Populated rows whose stored value differs from the incoming source (kept, reported)."""
    cur.execute(
        """
        SELECT l.player_id::bigint,
               COALESCE(r.team_id, l.team_id)::bigint AS team_id,
               team_map.official_team_id, team_map.mapping_count,
               l.pp_toi_minutes
        FROM nhl.skater_game_logs_raw l
        LEFT JOIN LATERAL (
          SELECT team_id FROM nhl.roster_status rs
          WHERE rs.game_id=l.game_id AND rs.player_id=l.player_id
          ORDER BY rs.asof_ts DESC LIMIT 1
        ) r ON TRUE
        LEFT JOIN LATERAL (
          SELECT
            CASE WHEN COUNT(DISTINCT e.provider_team_id::bigint) = 1
                 THEN MIN(e.provider_team_id::bigint) END AS official_team_id,
            COUNT(DISTINCT e.provider_team_id::bigint) AS mapping_count
          FROM nhl.team_external_ids e
          WHERE e.team_id = COALESCE(r.team_id, l.team_id)
            AND e.provider = 'nhl'
        ) team_map ON TRUE
        WHERE l.game_id = %s AND l.player_id = ANY(%s)
          AND l.pp_toi_minutes IS NOT NULL
        ORDER BY l.player_id;
        """,
        (game_id, list(values)),
    )
    out = []
    for player_id, team_id, official_team_id, mapping_count, current_value in cur.fetchall():
        conflict = pp_toi_conflict(
            game_id=game_id, player_id=player_id, team_id=team_id,
            official_team_id=official_team_id, official_team_mapping_count=mapping_count,
            existing_value=current_value, incoming_value=values.get(int(player_id)),
            source_kind=source_kind, source_reference=source_reference,
        )
        if conflict:
            out.append(conflict)
    return out


ZERO_FILL_FUNCTION = "nhl.fill_pp_toi_zero_from_shift_overlap(bigint)"


def zero_fill_available(cur) -> bool:
    """True once the operator has applied the verified-zero migration."""
    cur.execute("SELECT to_regprocedure(%s) IS NOT NULL;", (ZERO_FILL_FUNCTION,))
    row = cur.fetchone()
    return bool(row and row[0])


def fill_verified_zero(cur, game_id: int) -> int:
    """Write 0.00 for NULL rows whose shifts prove no PP time (P2.31).

    The database function checks complete shift coverage, ingested manpower
    windows and zero own-team PP overlap, and only fills NULL values.
    """
    cur.execute("SELECT nhl.fill_pp_toi_zero_from_shift_overlap(%s)", (game_id,))
    row = cur.fetchone()
    return int(row[0]) if row else 0


def assert_no_bad_team_games(cur, slate_date: str, pending_game_ids=()) -> int:
    """
    Returns count of (game_id, team_id) where:
      - team TOI > 0
      - team PP TOI sum = 0
      - BUT the team actually had PP windows (>0 seconds) in game_manpower_segments
        (i.e., appears as pp_team_id with any segment duration > 0)
    Regular season only (game_type = 2).
    """
    cur.execute(
        """
        WITH team_games AS (
          SELECT
            g.game_id::bigint AS game_id,
            l.team_id::bigint AS team_id,
            SUM(COALESCE(l.toi_minutes,0))    AS team_toi_sum,
            SUM(COALESCE(l.pp_toi_minutes,0)) AS team_pp_toi_sum
          FROM nhl.skater_game_logs_raw l
          JOIN nhl.games g ON g.game_id = l.game_id
          WHERE g.game_date = %s::date
            AND g.game_type = 2
            AND l.team_id IS NOT NULL
            AND NOT (g.game_id = ANY(%s::bigint[]))
          GROUP BY 1,2
        ),
        pp_windows AS (
          SELECT
            s.game_id::bigint      AS game_id,
            s.pp_team_id::bigint   AS team_id,
            SUM(GREATEST(0, s.end_sec - s.start_sec))::bigint AS pp_window_sec
          FROM nhl.game_manpower_segments s
          JOIN nhl.games g ON g.game_id = s.game_id
          WHERE g.game_date = %s::date
            AND g.game_type = 2
            AND s.pp_team_id IS NOT NULL
          GROUP BY 1,2
        ),
        bad AS (
          SELECT
            tg.game_id,
            tg.team_id,
            tg.team_toi_sum,
            tg.team_pp_toi_sum,
            COALESCE(pw.pp_window_sec,0) AS pp_window_sec
          FROM team_games tg
          LEFT JOIN pp_windows pw
            ON pw.game_id = tg.game_id
           AND pw.team_id = tg.team_id
          WHERE tg.team_toi_sum > 0
            AND tg.team_pp_toi_sum = 0
            AND COALESCE(pw.pp_window_sec,0) > 0
        )
        SELECT COUNT(*) FROM bad;
        """,
        (slate_date, [int(g) for g in pending_game_ids], slate_date),
    )
    return int(cur.fetchone()[0])

def print_bad_team_games(cur, slate_date: str, pending_game_ids=(), limit: int = 20) -> None:
    cur.execute(
        """
        WITH team_games AS (
          SELECT
            g.game_id::bigint AS game_id,
            l.team_id::bigint AS team_id,
            SUM(COALESCE(l.toi_minutes,0))    AS team_toi_sum,
            SUM(COALESCE(l.pp_toi_minutes,0)) AS team_pp_toi_sum
          FROM nhl.skater_game_logs_raw l
          JOIN nhl.games g ON g.game_id = l.game_id
          WHERE g.game_date = %s::date
            AND g.game_type = 2
            AND l.team_id IS NOT NULL
            AND NOT (g.game_id = ANY(%s::bigint[]))
          GROUP BY 1,2
        ),
        pp_windows AS (
          SELECT
            s.game_id::bigint      AS game_id,
            s.pp_team_id::bigint   AS team_id,
            SUM(GREATEST(0, s.end_sec - s.start_sec))::bigint AS pp_window_sec
          FROM nhl.game_manpower_segments s
          JOIN nhl.games g ON g.game_id = s.game_id
          WHERE g.game_date = %s::date
            AND g.game_type = 2
            AND s.pp_team_id IS NOT NULL
          GROUP BY 1,2
        )
        SELECT
          tg.game_id,
          tg.team_id,
          tg.team_toi_sum,
          tg.team_pp_toi_sum,
          COALESCE(pw.pp_window_sec,0) AS pp_window_sec,
          g.home_team_code,
          g.away_team_code
        FROM team_games tg
        JOIN nhl.games g ON g.game_id = tg.game_id
        LEFT JOIN pp_windows pw
          ON pw.game_id = tg.game_id AND pw.team_id = tg.team_id
        WHERE tg.team_toi_sum > 0
          AND tg.team_pp_toi_sum = 0
          AND COALESCE(pw.pp_window_sec,0) > 0
        ORDER BY pp_window_sec DESC, tg.team_toi_sum DESC
        LIMIT %s;
        """,
        (slate_date, [int(g) for g in pending_game_ids], slate_date, limit),
    )
    for r in cur.fetchall():
        print(f"BAD team-game: {r}", file=sys.stderr)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=None, help="YYYY-MM-DD (ET). If omitted, uses SLATE_DATE env.")
    ap.add_argument("--commit", action="store_true", help="Write updates to DB (default: dry-run rollback).")
    ap.add_argument("--sleep", type=float, default=0.2, help="Sleep between API calls.")
    ap.add_argument("--db-url", default=None, help="Override DB url (else SUPABASE_DB_URL or DATABASE_URL).")
    args = ap.parse_args()

    slate_date = args.date or os.environ.get("SLATE_DATE")
    if not slate_date:
        die("Provide --date YYYY-MM-DD or set SLATE_DATE.")

    try:
        db_url = database_url("ingestion", legacy_url=args.db_url)
    except RuntimeError:
        die("Set SUPABASE_DB_URL (or DATABASE_URL) or pass --db-url")

    commit = bool(args.commit)

    conn = psycopg.connect(db_url, prepare_threshold=None)
    try:
        conn.prepare_threshold = None  # type: ignore[attr-defined]
    except Exception:
        pass
    conn.autocommit = False
    cur = conn.cursor()

    game_ids = skip_voided_games(get_games_for_date(cur, slate_date))
    if not game_ids:
        print(f"No regular-season games found for {slate_date} (game_type=2).")
        conn.rollback()
        conn.close()
        return

    print(f"PP TOI canonical fill for {slate_date}: games={len(game_ids)} mode={'COMMIT' if commit else 'DRY-RUN'}")

    total_updated_rows = 0
    total_zero_rows = 0
    zero_available = zero_fill_available(cur)
    if not zero_available:
        print("verified-zero fill function not installed; positive fill only", file=sys.stderr)
    conflicts: list[dict[str, Any]] = []
    source_values: list[tuple[int, int, float]] = []
    official_available = official_fill_available(cur)
    if not official_available:
        print("official PP fill function not installed; shift-overlap fill only", file=sys.stderr)
    total_official_rows = 0
    official_games = 0
    overlap_games = 0
    pending_games: list[int] = []
    for game_id in game_ids:
        # --- Source of record: the NHL stats API (P2.45), once its fill function exists ---
        if official_available:
            try:
                official_rows = fetch_official_pp(game_id)
            except Exception as e:
                print(f"game_id={game_id}: stats API PP fetch failed: {e}", file=sys.stderr)
                official_rows = []
            if official_rows:
                values = {r["player_id"]: r["pp_seconds"] / 60.0 for r in official_rows}
                source_values.extend((int(game_id), int(pid), float(v)) for pid, v in sorted(values.items()))
                try:
                    conflicts.extend(collect_conflicts(
                        cur, game_id, values, source_kind="OFFICIAL_STATS_API_SKATER_TOI",
                        source_reference=STATS_TOI_URL))
                    filled = fill_from_official(cur, game_id, official_rows)
                except Exception as e:
                    print(f"game_id={game_id}: official PP fill failed: {e}", file=sys.stderr)
                    conn.rollback()
                    continue
                total_official_rows += filled
                official_games += 1
                print(f"game_id={game_id}: source=stats_api official_rows={len(official_rows)} filled={filled}")
                if commit:
                    conn.commit()
                else:
                    conn.rollback()
                time.sleep(args.sleep)
                continue
            # Not published yet (the stats API lags the final horn) or unreachable: leave the
            # game NULL for a later run. A shift-overlap value written now could never be
            # replaced, because every fill is NULL-only (2026-11-15 VAN-VGK, P2.45).
            pending_games.append(int(game_id))
            print(f"game_id={game_id}: stats API has no rows yet; left NULL for a later run", file=sys.stderr)
            conn.rollback()
            time.sleep(args.sleep)
            continue

        # --- Fallback: DB overlap (segments + shiftcharts), not boxscore JSON ---
        overlap_games += 1
        try:
            pp_map = extract_pp_toi_minutes_from_db(cur, game_id)
        except Exception as e:
            print(f"game_id={game_id}: pp overlap query failed: {e}", file=sys.stderr)
            conn.rollback()
            continue

        # If we can’t compute PP overlap for this game, do not write anything.
        # Let the end-of-run invariant catch the systemic failure (if any).
        if not pp_map:
            print(
                f"game_id={game_id}: extracted pp_map empty (no PP overlap found); skipping updates for this game",
                file=sys.stderr,
            )
            conn.rollback()
            time.sleep(args.sleep)
            continue

        source_values.extend((int(game_id), int(pid), float(value))
                             for pid, value in sorted(pp_map.items()))
        conflicts.extend(collect_conflicts(
            cur, game_id, pp_map, source_kind="RECONSTRUCTED_SHIFT_OVERLAP",
            source_reference=(
                "nhl.game_manpower_segments + nhl.shiftcharts_raw; "
                "extract_pp_toi_minutes_from_db"
            ),
        ))

        # Apply updates (only where pp_toi_minutes is NULL or 0)
        try:
            db_rows, overlap, upd = update_game(cur, game_id, pp_map)
        except Exception as e:
            print(f"game_id={game_id}: update_game failed: {e}", file=sys.stderr)
            conn.rollback()
            continue

        total_updated_rows += upd
        zero = 0
        if zero_available:
            try:
                zero = fill_verified_zero(cur, game_id)
            except Exception as e:
                print(f"game_id={game_id}: verified-zero fill failed: {e}", file=sys.stderr)
                conn.rollback()
                continue
        total_zero_rows += zero
        print(f"game_id={game_id}: db_rows={db_rows} overlap={overlap} update_statements={upd} verified_zero={zero}")

        if commit:
            conn.commit()
        else:
            conn.rollback()

        time.sleep(args.sleep)

    # invariant check (run in a transaction that matches mode)
    bad = assert_no_bad_team_games(cur, slate_date, pending_games)
    if bad > 0:
        print_bad_team_games(cur, slate_date, pending_games)
        if commit:
            conn.rollback()
        conn.close()
        die(f"Invariant failed: {bad} team-games on {slate_date} have TOI>0 but PP TOI sum=0. PP role is unsafe.", 2)

    if commit:
        conn.commit()
    else:
        conn.rollback()

    conn.close()
    source_body = json.dumps(source_values, separators=(",", ":")).encode()
    run_id = (os.environ.get("NHL_DAILY_RUN_ID")
              or os.environ.get("NHL_RECONCILIATION_RUN_ID")
              or f"nhl_pp_toi_fill_{slate_date}_{uuid.uuid4().hex[:12]}")
    diagnostic = write_pp_toi_conflict_diagnostic(
        run_id=run_id, slate_date=slate_date, stage="fill",
        conflicts=conflicts,
        source_reference=("NHL stats API skater/timeonice ppTimeOnIce; fallback "
                          "nhl.game_manpower_segments + nhl.shiftcharts_raw"),
        source_observation_set_sha256=hashlib.sha256(source_body).hexdigest(),
    )
    print("NHL_CHILD_SUMMARY_JSON=" + json.dumps({
        "schema_version": "NHL_PP_TOI_SOURCE_SUMMARY_V1",
        "run_id": run_id,
        "slate_date": slate_date,
        "source_reconstructed_positive_count": len(source_values),
        "official_fill_function_available": official_available,
        "official_fills": total_official_rows,
        "official_source_games": official_games,
        "shift_overlap_games": overlap_games,
        "official_pending_games": len(pending_games),
        "filled_nulls": total_updated_rows,
        "verified_zero_fills": total_zero_rows,
        "zero_fill_function_available": zero_available,
        "confirmed_discrepancy_count": len(conflicts),
        "diagnostic_artifact": diagnostic,
        "unknown_source_remains_null": True,
        "populated_zero_preserved": True,
    }, sort_keys=True))
    print(f"DONE. official_rows={total_official_rows} official_games={official_games} "
          f"overlap_games={overlap_games} pending_games={len(pending_games)} total_updated_rows={total_updated_rows} verified_zero_rows={total_zero_rows} bad_team_games={bad} mode={'COMMIT' if commit else 'DRY-RUN'}")


if __name__ == "__main__":
    main()
