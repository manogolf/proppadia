#!/usr/bin/env python3
"""DB-URL-native MLB stat-derived backfill.

Recreates core behavior of the retired legacy JS stat-derived job while
using psycopg + DATABASE_URL/SUPABASE_DB_URL (no Supabase JS credentials).
"""

from __future__ import annotations

import argparse
import hashlib
import random
import uuid
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

from backend.mlb.shared.team_name_map import (
    getTeamIdFromAbbr,
    normalizeTeamAbbreviation,
)
from backend.shared.db.pg import pg_connect


BATTER_PROP_TYPES = [
    "hits",
    "strikeouts_batting",
    "home_runs",
    "rbis",
    "runs_rbis",
    "runs_scored",
    "total_bases",
    "walks",
    "stolen_bases",
    "singles",
    "doubles",
    "triples",
    "hits_runs_rbis",
]

PITCHER_PROP_TYPES = [
    "strikeouts_pitching",
    "outs_recorded",
    "earned_runs",
    "hits_allowed",
    "walks_allowed",
]

# MLB StatsAPI gameType codes considered "in-season" for collection gates.
# Keeps postseason rows when season mode excludes preseason.
IN_SEASON_GAME_TYPES = {"R", "P", "F", "D", "L", "W"}


def _parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _daterange(start: date, end: date) -> Iterable[date]:
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def _hash01(s: str) -> float:
    h = hashlib.sha256(s.encode("utf-8")).hexdigest()
    return int(h[:8], 16) / 0xFFFFFFFF


def _should_include(player_id: int, game_id: int, prop_type: str, ratio: float = 0.2) -> bool:
    return _hash01(f"{player_id}-{game_id}-{prop_type}") < ratio


def _determine_outcome(result: float, line: float, over_under: str) -> Optional[str]:
    if result == line:
        return None
    if over_under == "over":
        return "win" if result > line else "loss"
    if over_under == "under":
        return "win" if result < line else "loss"
    return None


def _time_bucket(hour: int) -> str:
    if hour < 12:
        return "morning"
    if hour < 17:
        return "afternoon"
    if hour < 21:
        return "evening"
    return "night"


def _ip_to_outs(ip: Any) -> Optional[int]:
    if ip is None:
        return None
    s = str(ip)
    parts = s.split(".")
    try:
        whole = int(parts[0])
    except Exception:
        return None
    frac = parts[1] if len(parts) > 1 else "0"
    extra = 1 if frac == "1" else 2 if frac == "2" else 0
    return whole * 3 + extra


def _num(x: Any) -> Optional[float]:
    try:
        v = float(x)
        return v
    except Exception:
        return None


def _to_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(x)
    except Exception:
        return None


def _extract_stat_for_prop(stats: Dict[str, Any], prop_type: str) -> Optional[float]:
    b = stats.get("batting") or {}
    p = stats.get("pitching") or {}

    if prop_type == "hits":
        return _num(b.get("hits"))
    if prop_type == "strikeouts_batting":
        return _num(b.get("strikeOuts") or b.get("strikeouts"))
    if prop_type == "home_runs":
        return _num(b.get("homeRuns") or b.get("home_runs"))
    if prop_type == "rbis":
        return _num(b.get("rbi") or b.get("rbis"))
    if prop_type == "runs_scored":
        return _num(b.get("runs"))
    if prop_type == "runs_rbis":
        r = _num(b.get("runs")) or 0.0
        i = _num(b.get("rbi") or b.get("rbis")) or 0.0
        return r + i
    if prop_type == "walks":
        return _num(b.get("baseOnBalls") or b.get("walks"))
    if prop_type == "stolen_bases":
        return _num(b.get("stolenBases") or b.get("stolen_bases"))
    if prop_type == "doubles":
        return _num(b.get("doubles"))
    if prop_type == "triples":
        return _num(b.get("triples"))
    if prop_type == "total_bases":
        return _num(b.get("totalBases") or b.get("total_bases"))
    if prop_type == "singles":
        h = _num(b.get("hits")) or 0.0
        d = _num(b.get("doubles")) or 0.0
        t = _num(b.get("triples")) or 0.0
        hr = _num(b.get("homeRuns") or b.get("home_runs")) or 0.0
        return h - d - t - hr
    if prop_type == "hits_runs_rbis":
        h = _num(b.get("hits")) or 0.0
        r = _num(b.get("runs")) or 0.0
        i = _num(b.get("rbi") or b.get("rbis")) or 0.0
        return h + r + i

    if prop_type == "strikeouts_pitching":
        return _num(p.get("strikeOuts") or p.get("strikeouts"))
    if prop_type == "outs_recorded":
        return _num(p.get("outs")) or _ip_to_outs(p.get("inningsPitched"))
    if prop_type == "earned_runs":
        return _num(p.get("earnedRuns") or p.get("earned_runs"))
    if prop_type == "hits_allowed":
        return _num(p.get("hits") or p.get("hits_allowed"))
    if prop_type == "walks_allowed":
        return _num(p.get("baseOnBalls") or p.get("walks_allowed") or p.get("walks"))
    return None


def _fetch_json(url: str) -> Dict[str, Any]:
    r = requests.get(url, timeout=25)
    r.raise_for_status()
    return r.json()


def _fetch_schedule(date_iso: str) -> List[Dict[str, Any]]:
    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date_iso}"
    js = _fetch_json(url)
    dates = js.get("dates") or []
    if not dates:
        return []
    return (dates[0] or {}).get("games", []) or []


def _fetch_live_feed(game_id: int) -> Dict[str, Any]:
    return _fetch_json(f"https://statsapi.mlb.com/api/v1.1/game/{game_id}/feed/live")


def _fetch_boxscore(game_id: int) -> Dict[str, Any]:
    return _fetch_json(f"https://statsapi.mlb.com/api/v1/game/{game_id}/boxscore")


def _get_positions_by_date(conn, game_date: str) -> Dict[int, str]:
    out: Dict[int, str] = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT player_id, position
            FROM mlb.player_stats
            WHERE game_date = %s::date
            """,
            (game_date,),
        )
        for pid, pos in cur.fetchall() or []:
            try:
                pid_i = int(pid)
            except Exception:
                continue
            if pid_i not in out and pos:
                out[pid_i] = str(pos)
    return out


def _get_streak(conn, player_id: int, prop_type: str) -> Tuple[Optional[str], Optional[int]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT streak_type, streak_count
            FROM mlb.player_streak_profiles
            WHERE CAST(player_id AS TEXT) = %s
              AND prop_type = %s
              AND prop_source = 'mlb_api'
            LIMIT 1
            """,
            (str(player_id), prop_type),
        )
        row = cur.fetchone()
        if not row:
            return None, None
        if isinstance(row, dict):
            st = row.get("streak_type")
            cnt = row.get("streak_count")
        else:
            st, cnt = row
        return (str(st) if st is not None else None, int(cnt) if cnt is not None else None)


def _date_has_mlb_api_rows(conn, game_date: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM mlb.model_training_props
            WHERE game_date = %s::date
              AND prop_source = 'mlb_api'
            LIMIT 1
            """,
            (game_date,),
        )
        return cur.fetchone() is not None


def _date_has_negative_lines(conn, game_date: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM mlb.model_training_props
            WHERE game_date = %s::date
              AND prop_source = 'mlb_api'
              AND line < 0
            LIMIT 1
            """,
            (game_date,),
        )
        return cur.fetchone() is not None


def _existing_game_ids(conn, game_ids: List[int]) -> set[int]:
    ids = [int(g) for g in game_ids if _to_int(g) is not None]
    if not ids:
        return set()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT game_id
            FROM mlb.game_info
            WHERE game_id = ANY(%s)
            """,
            (ids,),
        )
        out: set[int] = set()
        for r in cur.fetchall():
            v = (r or {}).get("game_id")
            if v is not None:
                out.add(int(v))
        return out


def _upsert_game_info_min(conn, game: Dict[str, Any], fallback_date_iso: str) -> int:
    game_id = _to_int(game.get("gamePk"))
    if game_id is None:
        return 0

    teams = (game.get("teams") or {})
    home_team = ((teams.get("home") or {}).get("team") or {})
    away_team = ((teams.get("away") or {}).get("team") or {})

    game_time: Optional[datetime] = None
    game_date_val: Optional[str] = None
    raw_game_date = game.get("gameDate")
    if raw_game_date:
        try:
            parsed = datetime.fromisoformat(str(raw_game_date).replace("Z", "+00:00"))
            game_time = parsed.replace(tzinfo=None)
            game_date_val = parsed.date().isoformat()
        except Exception:
            game_time = None
            game_date_val = None
    if not game_date_val:
        game_date_val = fallback_date_iso

    row = {
        "game_id": game_id,
        "game_time": game_time,
        "game_date": game_date_val,
        "home_team_id": _to_int(home_team.get("id")),
        "away_team_id": _to_int(away_team.get("id")),
        "home_team_abbr": normalizeTeamAbbreviation(home_team.get("abbreviation")),
        "away_team_abbr": normalizeTeamAbbreviation(away_team.get("abbreviation")),
    }

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO mlb.game_info (
                game_id,
                game_time,
                game_date,
                home_team_id,
                away_team_id,
                home_team_abbr,
                away_team_abbr
            ) VALUES (
                %(game_id)s,
                %(game_time)s,
                %(game_date)s,
                %(home_team_id)s,
                %(away_team_id)s,
                %(home_team_abbr)s,
                %(away_team_abbr)s
            )
            ON CONFLICT (game_id)
            DO UPDATE SET
                game_time = COALESCE(game_info.game_time, EXCLUDED.game_time),
                game_date = COALESCE(game_info.game_date, EXCLUDED.game_date),
                home_team_id = COALESCE(game_info.home_team_id, EXCLUDED.home_team_id),
                away_team_id = COALESCE(game_info.away_team_id, EXCLUDED.away_team_id),
                home_team_abbr = COALESCE(game_info.home_team_abbr, EXCLUDED.home_team_abbr),
                away_team_abbr = COALESCE(game_info.away_team_abbr, EXCLUDED.away_team_abbr)
            WHERE (
                game_info.game_time IS NULL
                OR game_info.game_date IS NULL
                OR game_info.home_team_id IS NULL
                OR game_info.away_team_id IS NULL
                OR game_info.home_team_abbr IS NULL
                OR game_info.away_team_abbr IS NULL
            )
            """,
            row,
        )
        return int(cur.rowcount or 0)


def _table_has_column(conn, table_name: str, column_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'mlb'
              AND table_name = %s
              AND column_name = %s
            LIMIT 1
            """,
            (table_name, column_name),
        )
        return cur.fetchone() is not None


def _upsert_player_id_min(
    conn,
    *,
    player_id: int,
    player_name: str,
    team_abbr: Optional[str],
    team_id: Optional[int],
    has_team_col: bool,
    has_team_id_col: bool,
    has_placeholder_col: bool,
) -> int:
    row: Dict[str, Any] = {
        "player_id": int(player_id),
        "player_name": str(player_name) if player_name else f"player_{int(player_id)}",
    }
    cols = ["player_id", "player_name"]
    vals = ["%(player_id)s", "%(player_name)s"]
    if has_team_col:
        cols.append("team")
        vals.append("%(team)s")
        row["team"] = normalizeTeamAbbreviation(team_abbr)
    if has_team_id_col:
        cols.append("team_id")
        vals.append("%(team_id)s")
        row["team_id"] = _to_int(team_id)
    if has_placeholder_col:
        cols.append("is_placeholder")
        vals.append("%(is_placeholder)s")
        row["is_placeholder"] = True

    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO mlb.player_ids ({", ".join(cols)})
            VALUES ({", ".join(vals)})
            ON CONFLICT (player_id) DO NOTHING
            """,
            row,
        )
        return int(cur.rowcount or 0)


def _upsert_training_row(conn, row: Dict[str, Any], *, include_game_type: bool = False) -> int:
    extra_insert_col = ", game_type" if include_game_type else ""
    extra_insert_val = ", %(game_type)s" if include_game_type else ""
    extra_update_set = ", game_type = EXCLUDED.game_type" if include_game_type else ""
    extra_current_tuple = ", model_training_props.game_type" if include_game_type else ""
    extra_excluded_tuple = ", EXCLUDED.game_type" if include_game_type else ""
    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO mlb.model_training_props (
                id, game_id, player_id, player_name, team, opponent,
                team_id, opponent_team_id, opponent_encoded, is_home,
                prop_type, prop_value, line, over_under, outcome, status,
                created_at, updated_at, prop_source, was_correct, game_date,
                game_time, game_day_of_week, time_of_day_bucket, streak_type, streak_count{extra_insert_col}
            ) VALUES (
                %(id)s, %(game_id)s, %(player_id)s, %(player_name)s, %(team)s, %(opponent)s,
                %(team_id)s, %(opponent_team_id)s, %(opponent_encoded)s, %(is_home)s,
                %(prop_type)s, %(prop_value)s, %(line)s, %(over_under)s, %(outcome)s, %(status)s,
                %(created_at)s, %(updated_at)s, %(prop_source)s, %(was_correct)s, %(game_date)s,
                %(game_time)s, %(game_day_of_week)s, %(time_of_day_bucket)s, %(streak_type)s, %(streak_count)s{extra_insert_val}
            )
            ON CONFLICT (player_id, game_id, prop_type, prop_source)
            DO UPDATE SET
                team = EXCLUDED.team,
                opponent = EXCLUDED.opponent,
                team_id = EXCLUDED.team_id,
                opponent_team_id = EXCLUDED.opponent_team_id,
                opponent_encoded = EXCLUDED.opponent_encoded,
                is_home = EXCLUDED.is_home,
                prop_value = EXCLUDED.prop_value,
                line = EXCLUDED.line,
                over_under = EXCLUDED.over_under,
                outcome = EXCLUDED.outcome,
                status = EXCLUDED.status,
                updated_at = EXCLUDED.updated_at,
                was_correct = EXCLUDED.was_correct,
                game_time = EXCLUDED.game_time,
                game_day_of_week = EXCLUDED.game_day_of_week,
                time_of_day_bucket = EXCLUDED.time_of_day_bucket,
                streak_type = EXCLUDED.streak_type,
                streak_count = EXCLUDED.streak_count{extra_update_set}
            WHERE (
                model_training_props.prop_value,
                model_training_props.line,
                model_training_props.over_under,
                model_training_props.outcome,
                model_training_props.status,
                model_training_props.was_correct,
                model_training_props.game_time,
                model_training_props.game_day_of_week,
                model_training_props.time_of_day_bucket,
                model_training_props.streak_type,
                model_training_props.streak_count,
                model_training_props.team,
                model_training_props.opponent,
                model_training_props.team_id,
                model_training_props.opponent_team_id,
                model_training_props.opponent_encoded,
                model_training_props.is_home{extra_current_tuple}
            ) IS DISTINCT FROM (
                EXCLUDED.prop_value,
                EXCLUDED.line,
                EXCLUDED.over_under,
                EXCLUDED.outcome,
                EXCLUDED.status,
                EXCLUDED.was_correct,
                EXCLUDED.game_time,
                EXCLUDED.game_day_of_week,
                EXCLUDED.time_of_day_bucket,
                EXCLUDED.streak_type,
                EXCLUDED.streak_count,
                EXCLUDED.team,
                EXCLUDED.opponent,
                EXCLUDED.team_id,
                EXCLUDED.opponent_team_id,
                EXCLUDED.opponent_encoded,
                EXCLUDED.is_home{extra_excluded_tuple}
            )
            """,
            row,
        )
        # rowcount is 1 for insert/update, 0 when ON CONFLICT DO UPDATE ... WHERE skips.
        return int(cur.rowcount or 0)


def _is_pitcher(position: Optional[str], has_pitch: bool) -> bool:
    p = (position or "").upper()
    return has_pitch or p in {"P", "SP", "RP"}


def _is_starter(position: Optional[str], stats: Dict[str, Any]) -> bool:
    p = (position or "").upper()
    gs = _num((stats.get("pitching") or {}).get("gamesStarted")) or 0.0
    return gs > 0 or p == "SP"


def _final_games(
    schedule: List[Dict[str, Any]],
    *,
    require_regular_season: bool,
) -> List[Tuple[int, str]]:
    out: List[Tuple[int, str]] = []
    for g in schedule:
        if (g.get("status", {}) or {}).get("detailedState") != "Final":
            continue
        game_type = str((g.get("gameType") or "")).strip().upper()
        if require_regular_season and game_type not in IN_SEASON_GAME_TYPES:
            continue
        try:
            out.append((int(g["gamePk"]), game_type))
        except Exception:
            continue
    return out


def run(
    from_date: str,
    to_date: str,
    batter_sample_ratio: float = 0.2,
    quiet: bool = False,
    max_games_per_date: int = 0,
    skip_existing_dates: bool = False,
    require_regular_season: bool = False,
) -> int:
    start = _parse_date(from_date)
    end = _parse_date(to_date)
    if start > end:
        raise ValueError(f"from-date must be <= to-date ({from_date} > {to_date})")

    attempted_upserts = 0
    applied_upserts = 0
    failed_dates = 0
    skipped_dates = 0
    skipped_games_missing_info = 0
    game_info_upserts = 0
    player_id_upserts = 0
    over_count = 0
    under_count = 0

    with pg_connect() as conn:
        mtp_has_game_type = _table_has_column(conn, "model_training_props", "game_type")
        player_ids_has_team = _table_has_column(conn, "player_ids", "team")
        player_ids_has_team_id = _table_has_column(conn, "player_ids", "team_id")
        player_ids_has_placeholder = _table_has_column(conn, "player_ids", "is_placeholder")
        if not quiet:
            print(f"ℹ️ model_training_props.game_type column detected: {mtp_has_game_type}")

        for d in _daterange(start, end):
            d_iso = d.isoformat()
            print(f"\n📅 Processing {d_iso} ...")
            try:
                if skip_existing_dates and _date_has_mlb_api_rows(conn, d_iso):
                    if _date_has_negative_lines(conn, d_iso):
                        if not quiet:
                            print(f"🔧 {d_iso} reprocessing | repairing negative mlb_api lines")
                    else:
                        skipped_dates += 1
                        print(f"⏭️  {d_iso} skipped | mlb_api rows already present")
                        continue

                schedule = _fetch_schedule(d_iso)
                final_games_meta = _final_games(
                    schedule,
                    require_regular_season=require_regular_season,
                )
                final_games = [gid for gid, _ in final_games_meta]
                game_type_by_game_id = {gid: gtype for gid, gtype in final_games_meta}
                schedule_by_game_id = {
                    _to_int(g.get("gamePk")): g for g in schedule if _to_int(g.get("gamePk")) is not None
                }
                if max_games_per_date > 0:
                    final_games = final_games[:max_games_per_date]
                existing_games = _existing_game_ids(conn, final_games)

                missing_game_ids = [gid for gid in final_games if gid not in existing_games]
                if missing_game_ids:
                    for gid in missing_game_ids:
                        sg = schedule_by_game_id.get(gid)
                        if sg is None:
                            continue
                        game_info_upserts += _upsert_game_info_min(conn, sg, d_iso)
                    existing_games = _existing_game_ids(conn, final_games)

                missing_for_date = len(final_games) - len(existing_games)
                if missing_for_date > 0:
                    skipped_games_missing_info += missing_for_date
                    if not quiet:
                        print(f"   skipped games missing game_info: {missing_for_date}")
                if not quiet:
                    print(f"   final games: {len(existing_games)}")
                pos_map = _get_positions_by_date(conn, d_iso)
                before_attempted = attempted_upserts
                before_applied = applied_upserts

                for game_id in final_games:
                    if game_id not in existing_games:
                        continue
                    game_type = game_type_by_game_id.get(game_id) or None
                    live = _fetch_live_feed(game_id)
                    box = _fetch_boxscore(game_id)

                    home_team = (live.get("gameData", {}).get("teams", {}) or {}).get("home", {}) or {}
                    away_team = (live.get("gameData", {}).get("teams", {}) or {}).get("away", {}) or {}
                    home_abbr = home_team.get("abbreviation") or home_team.get("teamCode")
                    away_abbr = away_team.get("abbreviation") or away_team.get("teamCode")
                    home_id = home_team.get("id") or home_team.get("teamId")
                    away_id = away_team.get("id") or away_team.get("teamId")

                    game_date_iso = (
                        (live.get("gameData", {}).get("datetime", {}) or {}).get("dateTime")
                        or (box.get("gameData", {}).get("datetime", {}) or {}).get("dateTime")
                    )
                    game_dt = None
                    if game_date_iso:
                        try:
                            game_dt = datetime.fromisoformat(
                                str(game_date_iso).replace("Z", "+00:00")
                            ).astimezone()
                        except Exception:
                            game_dt = None
                    game_time = game_dt.isoformat() if game_dt else None
                    dow = game_dt.weekday() if game_dt else None
                    tod = _time_bucket(game_dt.hour) if game_dt else None

                    for side in ("home", "away"):
                        side_box = (box.get("teams", {}) or {}).get(side, {}) or {}
                        players_map = side_box.get("players") or {}
                        is_home = side == "home"
                        team_abbr = normalizeTeamAbbreviation(home_abbr if is_home else away_abbr)
                        opp_abbr = normalizeTeamAbbreviation(away_abbr if is_home else home_abbr)
                        team_id = _to_int(home_id if is_home else away_id)
                        opp_id = _to_int(away_id if is_home else home_id)
                        opp_encoded = str(opp_id) if opp_id is not None else str(getTeamIdFromAbbr(opp_abbr) or "")

                        for _, p in players_map.items():
                            person = p.get("person") or {}
                            stats = p.get("stats") or {}
                            pid_raw = person.get("id")
                            if pid_raw is None:
                                continue
                            try:
                                pid = int(pid_raw)
                            except Exception:
                                continue
                            pname = person.get("fullName") or f"player_{pid}"

                            bat = stats.get("batting") or {}
                            pitch = stats.get("pitching") or {}
                            has_bat = len(bat.keys()) > 0
                            has_pitch = len(pitch.keys()) > 0
                            position = pos_map.get(pid)
                            is_pitch = _is_pitcher(position, has_pitch)
                            is_starter = _is_starter(position, stats)

                            if not (has_bat or is_pitch):
                                continue

                            prop_types: List[str] = []
                            if has_bat:
                                prop_types.extend(BATTER_PROP_TYPES)
                            if is_pitch and is_starter:
                                prop_types.extend(PITCHER_PROP_TYPES)
                            prop_types = sorted(set(prop_types))

                            for prop_type in prop_types:
                                is_batter_prop = prop_type in BATTER_PROP_TYPES
                                if (not is_starter) and (not is_batter_prop):
                                    continue
                                if is_batter_prop and not _should_include(pid, game_id, prop_type, batter_sample_ratio):
                                    continue

                                result = _extract_stat_for_prop(stats, prop_type)
                                if result is None:
                                    continue

                                seed = _hash01(f"line-{pid}-{game_id}-{prop_type}")
                                line = (result - 0.5) if seed < 0.5 else (result + 0.5)
                                line = round(line * 2) / 2
                                # Synthetic count-market lines must be non-negative half-steps.
                                if line < 0.5:
                                    line = 0.5

                                over_under = "over" if _hash01(f"ou-{pid}-{game_id}-{prop_type}") < 0.5 else "under"
                                outcome = _determine_outcome(float(result), float(line), over_under)
                                if outcome not in {"win", "loss"}:
                                    continue

                                if over_under == "over":
                                    over_count += 1
                                else:
                                    under_count += 1

                                streak_type, streak_count = _get_streak(conn, pid, prop_type)
                                now_iso = datetime.utcnow().isoformat()
                                row = {
                                    "id": str(uuid.uuid4()),
                                    "game_id": game_id,
                                    "player_id": str(pid),
                                    "player_name": pname,
                                    # Current DB constraint mtp_team_text_numeric expects numeric text.
                                    "team": str(team_id) if team_id is not None else None,
                                    "opponent": str(opp_id) if opp_id is not None else None,
                                    "team_id": team_id,
                                    "opponent_team_id": opp_id,
                                    "opponent_encoded": opp_encoded or None,
                                    "is_home": bool(is_home),
                                    "prop_type": prop_type,
                                    "prop_value": float(result),
                                    "line": float(line),
                                    "over_under": over_under,
                                    "outcome": outcome,
                                    "status": "resolved",
                                    "created_at": now_iso,
                                    "updated_at": now_iso,
                                    "prop_source": "mlb_api",
                                    "was_correct": outcome == "win",
                                    "game_date": d_iso,
                                    "game_time": game_time,
                                    "game_day_of_week": dow,
                                    "time_of_day_bucket": tod,
                                    "streak_type": streak_type,
                                    "streak_count": streak_count,
                                    "game_type": game_type,
                                }
                                player_id_upserts += _upsert_player_id_min(
                                    conn,
                                    player_id=pid,
                                    player_name=pname,
                                    team_abbr=team_abbr,
                                    team_id=team_id,
                                    has_team_col=player_ids_has_team,
                                    has_team_id_col=player_ids_has_team_id,
                                    has_placeholder_col=player_ids_has_placeholder,
                                )
                                applied_upserts += _upsert_training_row(
                                    conn,
                                    row,
                                    include_game_type=mtp_has_game_type,
                                )
                                attempted_upserts += 1

                conn.commit()
                print(
                    f"✅ {d_iso} done | attempted: {attempted_upserts - before_attempted} "
                    f"| applied: {applied_upserts - before_applied}"
                )
            except Exception as e:
                failed_dates += 1
                conn.rollback()
                print(f"❌ Crash during processDate({d_iso}): {type(e).__name__}: {e}")

    print("\n🎯 Over/Under Pick Distribution:")
    print(f"   ➕ Over:  {over_count}")
    print(f"   ➖ Under: {under_count}")
    print(f"\n📥 Upserts attempted: {attempted_upserts}")
    print(f"🧩 Upserts applied:   {applied_upserts}")
    print(f"🗂️ game_info upserts: {game_info_upserts}")
    print(f"👤 player_ids upserts: {player_id_upserts}")
    if skipped_dates:
        print(f"⏭️  Dates skipped:      {skipped_dates}")
    if skipped_games_missing_info:
        print(f"⏭️  Games skipped (missing game_info): {skipped_games_missing_info}")
    if failed_dates:
        print(f"⚠️ Script finished with {failed_dates} date failure(s).")
        return 1
    print("🏁 Script finished successfully.")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Insert MLB stat-derived rows via DB URL.")
    ap.add_argument("--from-date", default=None, help="YYYY-MM-DD")
    ap.add_argument("--to-date", default=None, help="YYYY-MM-DD")
    ap.add_argument("--days-ago", type=int, default=2, help="Default rolling range if no explicit dates.")
    ap.add_argument("--batter-sample-ratio", type=float, default=0.2, help="Sampling ratio for batter props.")
    ap.add_argument("--quiet", action="store_true")
    ap.add_argument(
        "--max-games-per-date",
        type=int,
        default=0,
        help="Optional cap for quick smoke runs (0 = no cap).",
    )
    ap.add_argument(
        "--skip-existing-dates",
        action="store_true",
        help="Skip any date that already has mlb_api rows from this loader.",
    )
    ap.add_argument(
        "--require-regular-season",
        action="store_true",
        help="Only process final in-season games (R + postseason gameType codes).",
    )
    args = ap.parse_args()

    if args.from_date or args.to_date:
        if not args.from_date or not args.to_date:
            raise SystemExit("both --from-date and --to-date are required when using explicit range")
        from_date = args.from_date
        to_date = args.to_date
    else:
        end_d = date.today() - timedelta(days=1)
        start_d = date.today() - timedelta(days=max(1, int(args.days_ago)))
        from_date, to_date = start_d.isoformat(), end_d.isoformat()

    return run(
        from_date=from_date,
        to_date=to_date,
        batter_sample_ratio=max(0.0, min(float(args.batter_sample_ratio), 1.0)),
        quiet=bool(args.quiet),
        max_games_per_date=max(0, int(args.max_games_per_date)),
        skip_existing_dates=bool(args.skip_existing_dates),
        require_regular_season=bool(args.require_regular_season),
    )


if __name__ == "__main__":
    raise SystemExit(main())
