#  fetch_nhl_to_csv.py
"""
Patched NHL fetcher: prefers api-web.nhle.com, falls back to statsapi only if resolvable.

Creates CSVs under data/import_templates/:

- teams.csv
- team_external_ids.csv
- players.csv
- player_external_ids.csv
- games.csv
- game_external_ids.csv
- skater_game_logs_raw.csv
- goalie_game_logs_raw.csv

Usage:
  python scripts/fetch_nhl_to_csv.py --start 2023-10-01 --end 2024-07-01
  python scripts/fetch_nhl_to_csv.py --start 2024-10-01 --end 2025-07-01
"""

from __future__ import annotations
import argparse, csv, datetime as dt, os, sys, time, socket
from typing import Dict, List, Iterable, Tuple, Any, Optional

import requests

API_WEB = "https://api-web.nhle.com"
STATSAPI = "https://statsapi.web.nhl.com"

# Stable internal team_id map (32 teams), keyed by NHL team abbreviation.
TEAM_ID_MAP = {
  "ANA": 1,  "ARI": 2,  "BOS": 3,  "BUF": 4,  "CAR": 5,  "CBJ": 6,  "CGY": 7,  "CHI": 8,
  "COL": 9,  "DAL":10,  "DET":11,  "EDM":12,  "FLA":13,  "LAK":14,  "MIN":15,  "MTL":16,
  "NJD":17,  "NSH":18,  "NYI":19,  "NYR":20,  "OTT":21,  "PHI":22,  "PIT":23,  "SEA":24,
  "SJS":25,  "STL":26,  "TBL":27,  "TOR":28,  "VAN":29,  "VGK":30,  "WPG":31,  "WSH":32
}

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def write_csv(path: str, headers: List[str], rows: Iterable[Dict[str, Any]]) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow({h: r.get(h, "") for h in headers})

def can_resolve(host: str) -> bool:
    try:
        socket.getaddrinfo(host, 443)
        return True
    except Exception:
        return False

def season_from_date(d: dt.date) -> str:
    y = d.year
    return f"{y}{y+1}" if d.month >= 7 else f"{y-1}{y}"

def jget(url: str, params: Dict[str, Any] | None = None, tries: int = 3, sleep: float = 0.5) -> Dict[str, Any]:
    last = None
    for _ in range(tries):
        try:
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            time.sleep(sleep)
    raise RuntimeError(f"GET failed {url} {params}: {last}")

# ---------- API-WEB helpers ----------
def api_web_standings(date_str: str) -> List[Dict[str, Any]]:
    return jget(f"{API_WEB}/v1/standings/{date_str}")

def api_web_roster(team_abbr: str, season: str) -> Dict[str, Any]:
    return jget(f"{API_WEB}/v1/roster/{team_abbr}/{season}")

def api_web_schedule_season(team_abbr: str, season: str) -> Dict[str, Any]:
    return jget(f"{API_WEB}/v1/club-schedule-season/{team_abbr}/{season}")

def api_web_boxscore(game_id: int) -> Dict[str, Any]:
    return jget(f"{API_WEB}/v1/gamecenter/{game_id}/boxscore")

# ---------- Optional StatsAPI helpers ----------
def statsapi_boxscore(game_id: int) -> Dict[str, Any]:
    return jget(f"{STATSAPI}/api/v1/game/{game_id}/boxscore")

# ---------- Builders ----------
def build_teams_from_standings(date_for_teams: dt.date) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Robustly parse api-web standings for team abbrev/name data.
    Handles cases where the endpoint returns:
      - a flat list of dicts
      - a dict with lists under keys like 'standings', 'records', etc.
      - a dict of divisions/conferences containing 'teams' arrays
    Falls back to TEAM_ID_MAP if nothing usable is returned.
    """
    data = api_web_standings(date_for_teams.isoformat())

    # 1) Collect candidate team rows (dicts only)
    candidates: List[Dict[str, Any]] = []
    if isinstance(data, list):
        candidates = [r for r in data if isinstance(r, dict)]
    elif isinstance(data, dict):
        # Try common list holders first
        for key in ("standings", "teamStandings", "records", "data"):
            val = data.get(key)
            if isinstance(val, list):
                candidates = [r for r in val if isinstance(r, dict)]
                break
        # If still empty, scan for division/conference objects containing 'teams' arrays
        if not candidates:
            for v in data.values():
                if isinstance(v, list):
                    for item in v:
                        if isinstance(item, dict):
                            teams_list = item.get("teams") or item.get("teamRecords")
                            if isinstance(teams_list, list):
                                candidates.extend([t for t in teams_list if isinstance(t, dict)])

    # 2) Helper to extract fields from heterogeneous rows
    def get_abbr(row: Dict[str, Any]) -> Optional[str]:
        # direct
        abbr = row.get("teamAbbrev") or row.get("abbrev")
        if abbr:
            return str(abbr).upper()
        # nested
        team = row.get("team") or row.get("teamCommonName") or {}
        if isinstance(team, dict):
            ab = team.get("abbrev") or team.get("teamAbbrev")
            if ab:
                return str(ab).upper()
        return None

    def get_name(row: Dict[str, Any]) -> str:
        return (
            row.get("teamName")
            or row.get("teamCommonName")
            or ( (row.get("team") or {}).get("name") if isinstance(row.get("team"), dict) else None )
            or row.get("name")
            or ""
        )

    def get_city(row: Dict[str, Any]) -> str:
        return row.get("teamPlaceName") or row.get("city") or ""

    def get_conf(row: Dict[str, Any]) -> str:
        return row.get("conferenceName") or row.get("conference") or ""

    def get_div(row: Dict[str, Any]) -> str:
        return row.get("divisionName") or row.get("division") or ""

    # 3) Build outputs
    teams: List[Dict[str, Any]] = []
    team_xids: List[Dict[str, Any]] = []
    seen_abbr: set[str] = set()

    for row in candidates:
        if not isinstance(row, dict):
            continue
        abbr = get_abbr(row)
        if not abbr or abbr not in TEAM_ID_MAP or abbr in seen_abbr:
            continue
        seen_abbr.add(abbr)
        team_id = TEAM_ID_MAP[abbr]
        teams.append({
            "team_id": team_id,
            "abbr": abbr,
            "name": get_name(row) or abbr,
            "city": get_city(row),
            "conference": get_conf(row),
            "division": get_div(row),
            "active": True,
        })
        team_xids.append({
            "team_id": team_id,
            "provider": "nhl",
            "provider_team_id": abbr,  # using abbrev as provider ID (text)
        })

    # 4) Fallback: no parseable rows → emit all 32 with minimal fields
    if not teams:
        for abbr, team_id in TEAM_ID_MAP.items():
            teams.append({
                "team_id": team_id, "abbr": abbr,
                "name": abbr, "city": "", "conference": "", "division": "", "active": True
            })
            team_xids.append({"team_id": team_id, "provider": "nhl", "provider_team_id": abbr})

    return teams, team_xids

def build_players_from_rosters(season: str, team_abbrs: List[str]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    players: List[Dict[str, Any]] = []
    xids: List[Dict[str, Any]] = []
    for abbr in team_abbrs:
        if abbr not in TEAM_ID_MAP:
            continue
        team_id = TEAM_ID_MAP[abbr]
        try:
            r = api_web_roster(abbr, season)
        except Exception:
            try:
                r = jget(f"{API_WEB}/v1/roster/{abbr}/current")
            except Exception:
                continue
        for group_key, pos in (("forwards", "F"), ("defensemen", "D"), ("goalies", "G")):
            for p in (r.get(group_key) or []):
                pid = p.get("id") or p.get("playerId")
                full = (p.get("firstName", {}) or {}).get("default", "")
                lastn = (p.get("lastName", {}) or {}).get("default", "")
                first = full or (p.get("firstName") if isinstance(p.get("firstName"), str) else "")
                last  = lastn or (p.get("lastName") if isinstance(p.get("lastName"), str) else "")
                shoots = p.get("shootsCatches") or p.get("shoots") or p.get("catches")
                if not pid:
                    continue
                players.append({
                    "player_id": int(pid),
                    "team_id": team_id,
                    "first_name": first or "",
                    "last_name": last or "",
                    "position": pos,
                    "shoots_catches": shoots or "",
                    "active": True,
                })
                xids.append({
                    "player_id": int(pid),
                    "provider": "nhl",
                    "provider_player_id": str(pid),
                })
        time.sleep(0.15)
    return players, xids

def build_schedule_and_maps(start: dt.date, end: dt.date, season: str, team_abbrs: List[str]) -> Tuple[List[Dict[str, Any]], Dict[int, str], Dict[int, str], Dict[int, str], Dict[int, str]]:
    games: List[Dict[str, Any]] = []
    gid_to_home: Dict[int, str] = {}
    gid_to_away: Dict[int, str] = {}
    gid_to_date: Dict[int, str] = {}
    gid_to_start: Dict[int, str] = {}
    for abbr in team_abbrs:
        if abbr not in TEAM_ID_MAP:
            continue
        try:
            sched = api_web_schedule_season(abbr, season)
        except Exception:
            continue
        for g in (sched.get("games") or []):
            try:
                gid = int(g.get("id"))
            except Exception:
                continue
            gdate = (g.get("gameDate") or "").split("T")[0]
            start_utc = g.get("startTimeUTC") or ""
            home_abbr = g.get("homeTeamAbbrev") or (g.get("homeTeam") or {}).get("abbrev")
            away_abbr = g.get("awayTeamAbbrev") or (g.get("awayTeam") or {}).get("abbrev")
            if not home_abbr or not away_abbr or home_abbr not in TEAM_ID_MAP or away_abbr not in TEAM_ID_MAP:
                continue
            if gdate:
                d = dt.date.fromisoformat(gdate)
                if d < start or d > end:
                    continue
            if gid in gid_to_date:
                continue
            gid_to_home[gid] = home_abbr
            gid_to_away[gid] = away_abbr
            gid_to_date[gid] = gdate
            gid_to_start[gid] = start_utc or ""
            games.append({
                "game_id": gid,
                "game_date": gdate,
                "start_time_utc": start_utc or "",
                "season": season,
                "game_type": g.get("gameType") or g.get("gameState") or "",
                "home_team_id": TEAM_ID_MAP[home_abbr],
                "away_team_id": TEAM_ID_MAP[away_abbr],
            })
        time.sleep(0.1)
    return games, gid_to_home, gid_to_away, gid_to_date, gid_to_start

def mmss_to_minutes(s: Optional[str]) -> Optional[float]:
    if not s or ":" not in s:
        return None
    try:
        m, sec = s.split(":")
        return round(int(m) + int(sec)/60.0, 2)
    except Exception:
        return None

# --- ADD this helper above build_logs_from_boxscores ---
# --- DROP-IN REPLACEMENT: paste both defs below ---

def find_team_buckets(bx: Dict[str, Any],
                      gid: int,
                      gid_to_home: Dict[int, str],
                      gid_to_away: Dict[int, str]) -> List[Dict[str, Any]]:
    """
    Return buckets like:
      { 'is_home': bool, 'team_abbr': 'BOS', 'players': [ {...}, ... ] }
    Skaters can live under 'players' or 'skaters'; goalies often under 'goalies'.
    We tag goalie-sourced rows via '__is_goalie_source': True.
    """
    buckets: List[Dict[str, Any]] = []

    def as_abbr(d: Dict[str, Any]) -> Optional[str]:
        if not isinstance(d, dict):
            return None
        ab = d.get("teamAbbrev") or d.get("abbrev")
        if ab:
            return str(ab).upper()
        t = d.get("team") if isinstance(d.get("team"), dict) else None
        if t:
            ab = t.get("abbrev") or t.get("teamAbbrev")
            if ab:
                return str(ab).upper()
        return None

    def merged_players_block(side: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        """
        Merge 'players'/'skaters' (tag as skaters) and 'goalies' (tag as goalies).
        If none present, fall back to any list of dicts with playerId/id.
        """
        if not isinstance(side, dict):
            return None
        out: List[Dict[str, Any]] = []
        for key in ("players", "skaters"):
            val = side.get(key)
            if isinstance(val, list):
                for x in val:
                    if isinstance(x, dict):
                        y = dict(x)
                        y["__is_goalie_source"] = False
                        out.append(y)
        gval = side.get("goalies")
        if isinstance(gval, list):
            for x in gval:
                if isinstance(x, dict):
                    y = dict(x)
                    y["__is_goalie_source"] = True
                    out.append(y)

        if out:
            return out

        # last resort: any list of dicts containing playerId/id
        for v in side.values():
            if isinstance(v, list) and v and isinstance(v[0], dict) and any(("playerId" in vv or "id" in vv) for vv in v):
                tmp = []
                for x in v:
                    if isinstance(x, dict):
                        y = dict(x)
                        y["__is_goalie_source"] = False
                        tmp.append(y)
                return tmp or None
        return None

    # A) playerByGameStats.homeTeam/awayTeam
    pg = bx.get("playerByGameStats") or (bx.get("boxscore") or {}).get("playerByGameStats")
    if isinstance(pg, dict):
        for key, is_home in (("homeTeam", True), ("awayTeam", False)):
            side = pg.get(key)
            if isinstance(side, dict):
                players = merged_players_block(side)
                if players:
                    abbr = as_abbr(side) or (gid_to_home.get(gid) if is_home else gid_to_away.get(gid))
                    if abbr:
                        buckets.append({"is_home": is_home, "team_abbr": abbr, "players": players})
        if buckets:
            return buckets

    # B) boxscore.homeTeam / boxscore.awayTeam
    bs = bx.get("boxscore")
    if isinstance(bs, dict):
        for key, is_home in (("homeTeam", True), ("awayTeam", False)):
            side = bs.get(key)
            if isinstance(side, dict):
                players = merged_players_block(side)
                if players:
                    abbr = as_abbr(side) or (gid_to_home.get(gid) if is_home else gid_to_away.get(gid))
                    if abbr:
                        buckets.append({"is_home": is_home, "team_abbr": abbr, "players": players})
        if buckets:
            return buckets

        # C) boxscore.teams.home / boxscore.teams.away
        teams = bs.get("teams")
        if isinstance(teams, dict):
            for key, is_home in (("home", True), ("away", False)):
                side = teams.get(key)
                if isinstance(side, dict):
                    players = merged_players_block(side)
                    if players:
                        abbr = as_abbr(side) or (gid_to_home.get(gid) if is_home else gid_to_away.get(gid))
                        if abbr:
                            buckets.append({"is_home": is_home, "team_abbr": abbr, "players": players})
            if buckets:
                return buckets

    # D) Recursive fallback
    def walk(obj):
        if isinstance(obj, dict):
            players = merged_players_block(obj)
            if players:
                abbr = as_abbr(obj)
                if abbr:
                    buckets.append({
                        "is_home": (abbr == gid_to_home.get(gid)),
                        "team_abbr": abbr,
                        "players": players
                    })
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for it in obj:
                walk(it)

    walk(bx)
    # De-dupe on team_abbr
    out, seen = [], set()
    for b in buckets:
        ab = b.get("team_abbr")
        if not ab or ab in seen:
            continue
        seen.add(ab)
        b["is_home"] = (ab == gid_to_home.get(gid))
        out.append(b)
    return out


def build_logs_from_boxscores(game_ids: List[int],
                              gid_to_home: Dict[int,str],
                              gid_to_away: Dict[int,str],
                              gid_to_date: Dict[int,str]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    sk_rows: List[Dict[str, Any]] = []
    gk_rows: List[Dict[str, Any]] = []

    def mmss_to_minutes(s: Optional[str]) -> Optional[float]:
        if not s or ":" not in s:
            return None
        try:
            m, sec = s.split(":")
            return round(int(m) + int(sec)/60.0, 2)
        except Exception:
            return None

    for gid in game_ids:
        bx = None
        try:
            bx = api_web_boxscore(gid)
        except Exception:
            if can_resolve("statsapi.web.nhl.com"):
                # ... your existing statsapi fallback ...
                pass
            time.sleep(0.05)
            continue  # next game

        # >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
        # INSERTED HERE — SKATER-ONLY ADDON (does not touch goalie logic)
        try:
            bx_root = bx if isinstance(bx, dict) else {}
            bs = bx_root.get("boxscore", {}) if isinstance(bx_root.get("boxscore"), dict) else {}

            def _abbr(sd):
                if not isinstance(sd, dict):
                    return None
                t = sd.get("team") if isinstance(sd.get("team"), dict) else {}
                ab = sd.get("teamAbbrev") or sd.get("abbrev") or t.get("abbrev") or t.get("teamAbbrev")
                return (ab or "").upper() if ab else None

            def _mm(s):
                if not s or ":" not in s: return None
                try:
                    m, sec = s.split(":"); return round(int(m) + int(sec)/60.0, 2)
                except Exception:
                    return None

            def _emit_skaters(side_dict, is_home_flag, arr):
                team_abbr = _abbr(side_dict) or (gid_to_home.get(gid) if is_home_flag else gid_to_away.get(gid))
                opp_abbr  = (gid_to_away.get(gid) if is_home_flag else gid_to_home.get(gid))
                if not team_abbr or not opp_abbr:
                    return
                team_id = TEAM_ID_MAP.get(team_abbr, 0)
                opp_id  = TEAM_ID_MAP.get(opp_abbr, 0)
                for p in arr:
                    if not isinstance(p, dict):
                        continue
                    pid = p.get("playerId") or p.get("id")
                    if not pid:
                        continue
                    # Exclude explicit goalies; goalies handled elsewhere
                    pos_raw = p.get("position") or p.get("positionCode") or ""
                    if isinstance(pos_raw, dict):
                        pos_code = (pos_raw.get("code") or pos_raw.get("abbrev") or pos_raw.get("name") or "").upper()
                    else:
                        pos_code = str(pos_raw).upper()
                    if pos_code == "G":
                        continue

                    sog = p.get("shots") or p.get("sog") or p.get("shotsOnGoal")
                    toi = _mm(p.get("timeOnIce") or p.get("toi"))
                    pp  = _mm(p.get("powerPlayTimeOnIce") or p.get("ppTimeOnIce"))

                    sk_rows.append({
                        "player_id": int(pid),
                        "game_id": gid,
                        "team_id": team_id,
                        "opponent_id": opp_id,
                        "is_home": bool(is_home_flag),
                        "shots_on_goal": sog if sog is not None else "",
                        "shot_attempts": "",  # api-web boxscore doesn’t expose attempts here
                        "toi_minutes": toi if toi is not None else "",
                        "pp_toi_minutes": pp if pp is not None else "",
                        "game_date": gid_to_date.get(gid, ""),
                    })
                # 1) boxscore.homeTeam/awayTeam …
            for key, flag in (("homeTeam", True), ("awayTeam", False)):
                sd = bs.get(key)
                if isinstance(sd, dict):
                    for pk in ("players", "skaters", "forwards", "defense"):   # <-- add these
                        arr = sd.get(pk)
                        if isinstance(arr, list) and arr:
                            _emit_skaters(sd, flag, arr)

            # 2) boxscore.teams.home/away …
            teams = bs.get("teams")
            if isinstance(teams, dict):
                for key, flag in (("home", True), ("away", False)):
                    sd = teams.get(key)
                    if isinstance(sd, dict):
                        for pk in ("players", "skaters", "forwards", "defense"):  # <-- add these
                            arr = sd.get(pk)
                            if isinstance(arr, list) and arr:
                                _emit_skaters(sd, flag, arr)

            # 3) playerByGameStats.homeTeam/awayTeam …
            pg = bx_root.get("playerByGameStats") or (bs.get("playerByGameStats") if isinstance(bs, dict) else None)
            if isinstance(pg, dict):
                for key, flag in (("homeTeam", True), ("awayTeam", False)):
                    sd = pg.get(key)
                    if isinstance(sd, dict):
                        # some shapes use forwards/defense here as well
                        for pk in ("players", "skaters", "forwards", "defense"):  # <-- add these
                            arr = sd.get(pk)
                            if isinstance(arr, list) and arr:
                                _emit_skaters(sd, flag, arr)

        except Exception:
            pass
       
        buckets = find_team_buckets(bx, gid, gid_to_home, gid_to_away)
        if not buckets:
            continue

        for b in buckets:
            is_home = bool(b["is_home"])
            team_abbr = b.get("team_abbr") or (gid_to_home.get(gid) if is_home else gid_to_away.get(gid))
            opp_abbr  = (gid_to_away.get(gid) if is_home else gid_to_home.get(gid))
            if not team_abbr or not opp_abbr:
                continue
            team_id = TEAM_ID_MAP.get(team_abbr, 0)
            opp_id  = TEAM_ID_MAP.get(opp_abbr, 0)

            for p in (b.get("players") or []):
                if not isinstance(p, dict):
                    continue
                pid = p.get("playerId") or p.get("id")
                if not pid:
                    continue

                # source tag from merged_players_block
                goalie_source = bool(p.pop("__is_goalie_source", False))

                # decode position, when present
                pos_raw = p.get("position") or p.get("positionCode") or ""
                if isinstance(pos_raw, dict):
                    pos_code = (pos_raw.get("code") or pos_raw.get("abbrev") or pos_raw.get("name") or "").upper()
                else:
                    pos_code = str(pos_raw).upper()

                # common fields
                sog = p.get("shots") or p.get("sog") or p.get("shotsOnGoal")
                toi = mmss_to_minutes(p.get("timeOnIce") or p.get("toi"))
                pp  = mmss_to_minutes(p.get("powerPlayTimeOnIce") or p.get("ppTimeOnIce"))

                # goalie-ish stats
                saves         = p.get("saves")
                shots_against = p.get("shotsAgainst")
                goals_against = p.get("goalsAgainst") or p.get("goalsAllowed")

                # final goalie decision:
                # - if it came from 'goalies', it's a goalie
                # - else if explicit position == G, it's a goalie
                # - else if no position, require strong evidence (≥2 positive goalie stats)
                is_goalie = goalie_source or (pos_code == "G")
                if not is_goalie and not pos_code:
                    evidence = sum(1 for v in (saves, shots_against, goals_against) if isinstance(v, (int, float)) and v > 0)
                    is_goalie = (evidence >= 2)

                if not is_goalie:
                    sk_rows.append({
                        "player_id": int(pid), "game_id": gid, "team_id": team_id,
                        "opponent_id": opp_id, "is_home": is_home,
                        "shots_on_goal": sog if sog is not None else "",
                        "shot_attempts": "",
                        "toi_minutes": toi if toi is not None else "",
                        "pp_toi_minutes": pp if pp is not None else "",
                        "game_date": gid_to_date.get(gid, ""),
                    })
                else:
                    shots_val = (shots_against if shots_against is not None
                                 else ((saves or 0) + (goals_against or 0)
                                       if (saves is not None and goals_against is not None) else ""))
                    gk_rows.append({
                        "player_id": int(pid), "game_id": gid, "team_id": team_id,
                        "opponent_id": opp_id, "is_home": is_home,
                        "shots_faced": shots_val,
                        "saves": saves if saves is not None else "",
                        "goals_allowed": goals_against if goals_against is not None else "",
                        "toi_minutes": toi if toi is not None else "",
                        "start_prob": "",
                        "game_date": gid_to_date.get(gid, ""),
                    })

        time.sleep(0.06)

    return sk_rows, gk_rows

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD")
    ap.add_argument("--season", default=None, help="YYYYYYYY (e.g., 20232024). If omitted, guessed from --start.")
    args = ap.parse_args()

    start = dt.date.fromisoformat(args.start)
    end = dt.date.fromisoformat(args.end)
    if start > end:
        print("Error: --start must be <= --end", file=sys.stderr)
        sys.exit(2)
    season = args.season or season_from_date(start)
    outdir = os.path.join(os.getcwd(), "data", "import_templates")
    ensure_dir(outdir)

    teams, team_xids = build_teams_from_standings(end)
    team_abbrs = [t["abbr"] for t in teams if t.get("abbr") in TEAM_ID_MAP]

    players, player_xids = build_players_from_rosters(season, team_abbrs)

    games, gid_to_home, gid_to_away, gid_to_date, gid_to_start = build_schedule_and_maps(start, end, season, team_abbrs)

    game_ids = [g["game_id"] for g in games]
    sk_rows, gk_rows = build_logs_from_boxscores(game_ids, gid_to_home, gid_to_away, gid_to_date)

    write_csv(os.path.join(outdir, "teams.csv"),
              ["team_id","abbr","name","city","conference","division","active"],
              teams)

    write_csv(os.path.join(outdir, "team_external_ids.csv"),
              ["team_id","provider","provider_team_id"],
              team_xids)

    write_csv(os.path.join(outdir, "players.csv"),
              ["player_id","team_id","first_name","last_name","position","shoots_catches","active"],
              players)

    write_csv(os.path.join(outdir, "player_external_ids.csv"),
              ["player_id","provider","provider_player_id"],
              player_xids)

    write_csv(os.path.join(outdir, "games.csv"),
              ["game_id","game_date","start_time_utc","season","game_type","home_team_id","away_team_id"],
              games)

    write_csv(os.path.join(outdir, "game_external_ids.csv"),
              ["game_id","provider","provider_game_id"],
              [{"game_id": g["game_id"], "provider": "nhl", "provider_game_id": str(g["game_id"]) } for g in games])

    write_csv(os.path.join(outdir, "skater_game_logs_raw.csv"),
              ["player_id","game_id","team_id","opponent_id","is_home","shots_on_goal","shot_attempts","toi_minutes","pp_toi_minutes","game_date"],
              sk_rows)

    write_csv(os.path.join(outdir, "goalie_game_logs_raw.csv"),
              ["player_id","game_id","team_id","opponent_id","is_home","shots_faced","saves","goals_allowed","toi_minutes","start_prob","game_date"],
              gk_rows)

    print("✅ Wrote CSVs to:", outdir)
    print("   Import into nhl.import_*_stage tables → run your saved upsert query.")

if __name__ == "__main__":
    main()
