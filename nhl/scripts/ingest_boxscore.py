#!/usr/bin/env python3
from __future__ import annotations
import argparse, os, re
from typing import Any, Dict, List
import requests
import psycopg


# ───────────────── helpers ─────────────────

def env_db_url() -> str:
    db = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if not db:
        raise SystemExit("Missing SUPABASE_DB_URL / DATABASE_URL")
    return db

def fetch_json(url: str, timeout: int = 12) -> dict | list:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()

def parse_mmss_to_minutes(mmss: str | None) -> float | None:
    if not mmss: return None
    s = str(mmss).strip()
    if ":" not in s: return None
    try:
        m, s = s.split(":")
        return int(m) + int(s)/60.0
    except Exception:
        return None

def to_int(x) -> int | None:
    try:
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return None

# If API doesn’t surface numeric team ids, map by abbrev
TEAM_ID_BY_ABBR = {
    "ANA":24,"ARI":53,"BOS":6,"BUF":7,"CGY":20,"CAR":12,"CHI":16,"COL":21,"CBJ":29,"DAL":25,
    "DET":17,"EDM":22,"FLA":13,"LAK":26,"MIN":30,"MTL":8,"NSH":18,"NJD":1,"NYI":2,"NYR":3,
    "OTT":9,"PHI":4,"PIT":5,"SEA":55,"SJS":28,"STL":19,"TBL":14,"TOR":10,"UTA":41,
    "VAN":23,"VGK":54,"WPG":52,"WSH":15
}

# ─────────── PBP utilities ───────────

def plays_list(pbp_obj) -> list:
    """Return a flat list of plays across several payload shapes."""
    if isinstance(pbp_obj, list):
        return pbp_obj
    if isinstance(pbp_obj, dict):
        if isinstance(pbp_obj.get("plays"), list):
            return pbp_obj["plays"]
        pby = pbp_obj.get("playByPlay")
        if isinstance(pby, dict):
            if isinstance(pby.get("allPlays"), list):
                return pby["allPlays"]
            if isinstance(pby.get("plays"), list):
                return pby["plays"]
        live = pbp_obj.get("liveData")
        if isinstance(live, dict):
            pl = live.get("plays")
            if isinstance(pl, dict) and isinstance(pl.get("allPlays"), list):
                return pl["allPlays"]
    return []

def event_type(play: dict) -> str:
    """Normalize an event label (string preferred; fall back to CODE_###)."""
    v = play.get("typeDescKey")
    if isinstance(v, str) and v.strip():
        return v.upper()

    d = play.get("details")
    if isinstance(d, dict):
        v = d.get("typeDescKey")
        if isinstance(v, str) and v.strip():
            return v.upper()

    r = play.get("result")
    if isinstance(r, dict):
        v = r.get("eventTypeId")
        if isinstance(v, str) and v.strip():
            return v.upper()

    for k in ("typeCode", "eventCode", "eventTypeId"):
        v = play.get(k)
        if isinstance(v, int):
            return f"CODE_{v}"
        if isinstance(v, str) and v.strip():
            return v.upper()

    return ""

def aggregate_attempts_from_pbp(pbp: dict | list) -> Dict[int, Dict[str, int]]:
    """
    Return { playerId: {"sog": n, "missed": n, "blocked": n} } from PBP.
    Treat GOAL as SOG.
    """
    out: Dict[int, Dict[str,int]] = {}
    plays = plays_list(pbp)

    def bump(pid: int, key: str):
        if pid not in out:
            out[pid] = {"sog":0, "missed":0, "blocked":0}
        out[pid][key] += 1

    for p in plays:
        if not isinstance(p, dict):
            continue
        typ = event_type(p)

        # shooter id
        shooter = (p.get("details") or {}).get("playerId")
        if shooter is None:
            for pl in p.get("players", []) or []:
                if (pl.get("playerType") or "").lower() in ("shooter","scorer"):
                    shooter = (pl.get("player",{}) or {}).get("id") or pl.get("playerId")
                    break
        if shooter is None:
            continue
        try:
            shooter = int(shooter)
        except Exception:
            continue

        if typ in ("SHOT","SHOT-ON-GOAL","SHOT_ON_GOAL","GOAL"):
            bump(shooter, "sog")
        elif typ in ("MISSED_SHOT","MISSED-SHOT","MISS"):
            bump(shooter, "missed")
        elif typ in ("BLOCKED_SHOT","BLOCKED-SHOT","BLOCK"):
            bump(shooter, "blocked")

    return out

def _parse_situation(play: dict) -> tuple[int|None, int|None]:
    sit = play.get("situationCode")
    if isinstance(sit, str) and "v" in sit:
        try:
            a, b = sit.split("v", 1)
            return int(a), int(b)
        except Exception:
            pass
    hs = play.get("homeSkaters") or (play.get("about", {}) or {}).get("homeSkaters")
    as_ = play.get("awaySkaters") or (play.get("about", {}) or {}).get("awaySkaters")
    try:
        return (int(hs) if hs is not None else None, int(as_) if as_ is not None else None)
    except Exception:
        return (None, None)

def _play_team_abbr(play: dict) -> str|None:
    d = play.get("details") or {}
    t = (d.get("teamAbbrev") or d.get("team") or {}).get("abbrev") if isinstance(d.get("team"), dict) else d.get("teamAbbrev")
    if t:
        return str(t).upper()
    t2 = play.get("team", {})
    if isinstance(t2, dict):
        ab = t2.get("triCode") or t2.get("abbrev")
        if ab:
            return str(ab).upper()
    return None

def _play_shooter_id(play: dict) -> int|None:
    d = play.get("details") or {}
    pid = d.get("playerId")
    if pid is not None:
        try: return int(pid)
        except: return None
    for pl in play.get("players", []) or []:
        if (pl.get("playerType") or "").lower() in ("shooter","scorer"):
            pid = (pl.get("player",{}) or {}).get("id") or pl.get("playerId")
            try: return int(pid)
            except: return None
    return None

def aggregate_splits_from_pbp(pbp: dict | list, home_abbr: str, away_abbr: str, home_team_id: int, away_team_id: int):
    """
    Returns:
      sk_splits: {player_id: {"EV":e, "PP":p, "SH":s}}
      team_sf:   {team_id:   {"EV":e, "PP":p, "SH":s}}  (shots faced by defending team)
    """
    sk_splits: dict[int, dict[str,int]] = {}
    team_sf: dict[int, dict[str,int]] = {}
    plays = plays_list(pbp)

    def bump_sk(pid: int, lab: str):
        sk_splits.setdefault(pid, {"EV":0,"PP":0,"SH":0})
        sk_splits[pid][lab] += 1

    def bump_team(team_id: int, lab: str):
        team_sf.setdefault(team_id, {"EV":0,"PP":0,"SH":0})
        team_sf[team_id][lab] += 1

    for p in plays:
        if not isinstance(p, dict):
            continue
        typ = event_type(p)
        if typ not in ("SHOT","SHOT-ON-GOAL","SHOT_ON_GOAL","GOAL"):
            continue
        shooter_id = _play_shooter_id(p)
        team_abbr  = _play_team_abbr(p)
        if team_abbr is None:
            continue
        shoot_home = (team_abbr == home_abbr)
        hs, as_ = _parse_situation(p)
        if hs is None or as_ is None:
            lab = "EV"
        else:
            lab = "EV" if hs == as_ else ("PP" if (hs > as_) == shoot_home else "SH")
        if shooter_id:
            bump_sk(shooter_id, lab)
        def_team_id = away_team_id if shoot_home else home_team_id
        bump_team(def_team_id, lab)

    return sk_splits, team_sf

# ───────────────── main ingest ─────────────────

def ingest_game(game_id: int) -> None:
    box_url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"
    pbp_url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"

    box = fetch_json(box_url)
    try:
        pbp = fetch_json(pbp_url)
    except Exception:
        pbp = {}

    home = (box.get("homeTeam") or {})
    away = (box.get("awayTeam") or {})
    home_abbr = (home.get("abbrev") or home.get("teamAbbrev") or "").upper()
    away_abbr = (away.get("abbrev") or away.get("teamAbbrev") or "").upper()
    home_id = int(home.get("id") or TEAM_ID_BY_ABBR.get(home_abbr))
    away_id = int(away.get("id") or TEAM_ID_BY_ABBR.get(away_abbr))
    if not home_id or not away_id:
        raise SystemExit("Could not resolve team IDs; extend TEAM_ID_BY_ABBR.")

    game_date = box.get("gameDate") or box.get("startTimeUTC") or None
    if isinstance(game_date, str) and "T" in game_date:
        game_date = game_date.split("T",1)[0]

    # name map from players{}
    name_by_pid: Dict[int,str] = {}
    for side in ("homeTeam","awayTeam"):
        roster = (box.get(side) or {}).get("players") or {}
        if isinstance(roster, dict):
            for pid_s, p in roster.items():
                m = re.search(r"\d+", str(pid_s))
                if not m: 
                    continue
                pid = int(m.group())
                fn = ((p.get("firstName") or {}) or {}).get("default") or p.get("firstName") or ""
                ln = ((p.get("lastName")  or {}) or {}).get("default") or p.get("lastName")  or ""
                nm = (str(fn).strip()+" "+str(ln).strip()).strip() or f"Player {pid}"
                name_by_pid[pid] = nm

    # PBP aggregations
    attempts = aggregate_attempts_from_pbp(pbp)
    sk_splits, team_sf = aggregate_splits_from_pbp(pbp, home_abbr, away_abbr, home_id, away_id)

    # nested iterators so they can see `box`
    def _team_players(section: str):
        team = box.get(section) or {}
        players = team.get("players")
        if isinstance(players, dict):
            for pid_s, pdata in players.items():
                m = re.search(r"\d+", str(pid_s))
                if not m:
                    continue
                yield int(m.group()), (pdata or {}), section

    def iter_skaters(section: str):
        seen = False
        for pid, p, sect in _team_players(section):
            seen = True
            pos = (p.get("positionCode") or p.get("position") or "").upper()
            if pos != "G":
                yield pid, p, sect
        if not seen:
            team = box.get(section) or {}
            for key in ("skaters","forwards","defense"):
                for p in team.get(key, []) or []:
                    pid = int(p.get("playerId") or p.get("id"))
                    yield pid, p, section

    def iter_goalies(section: str):
        seen = False
        for pid, p, sect in _team_players(section):
            seen = True
            pos = (p.get("positionCode") or p.get("position") or "").upper()
            if pos == "G":
                yield pid, p, sect
        if not seen:
            team = box.get(section) or {}
            for p in team.get("goalies", []) or []:
                pid = int(p.get("playerId") or p.get("id"))
                yield pid, p, section

    # materialize goalie lists once so we can reuse counts + rows
    home_goalies = list(iter_goalies("homeTeam"))
    away_goalies = list(iter_goalies("awayTeam"))
    home_single = (len(home_goalies) == 1)
    away_single = (len(away_goalies) == 1)

    def pos_code(raw: dict, default: str) -> str:
        code = (raw.get("positionCode") or raw.get("position") or "").upper()
        if code in ("G","D","F"): return code
        if code in ("LW","RW","C"): return "F"
        return default

    DB = env_db_url()
    with psycopg.connect(DB) as conn, conn.cursor() as cur:
        # Teams
        for tid, abbr, name in (
            (home_id, home_abbr, (home.get("name") or home.get("teamName") or home_abbr)),
            (away_id, away_abbr, (away.get("name") or away.get("teamName") or away_abbr)),
        ):
            cur.execute("""
                INSERT INTO nhl.teams (team_id, name, abbr, active)
                VALUES (%s, %s, %s, true)
                ON CONFLICT (team_id) DO UPDATE
                  SET name = EXCLUDED.name, abbr = EXCLUDED.abbr, active = true;
            """, (tid, str(name), abbr))

        # Game
        cur.execute("""
            INSERT INTO nhl.games (game_id, game_date, home_team_id, away_team_id, status)
            VALUES (%s, %s, %s, %s, 'final')
            ON CONFLICT (game_id) DO UPDATE
              SET game_date = EXCLUDED.game_date,
                  home_team_id = EXCLUDED.home_team_id,
                  away_team_id = EXCLUDED.away_team_id,
                  status = EXCLUDED.status;
        """, (game_id, game_date, home_id, away_id))

        # Skaters
        sk_batch: List[tuple] = []
        for pid, raw, sect in list(iter_skaters("homeTeam")) + list(iter_skaters("awayTeam")):
            team_id = home_id if sect=="homeTeam" else away_id
            opp_id  = away_id if sect=="homeTeam" else home_id
            is_home = (sect == "homeTeam")
            nm = name_by_pid.get(pid) or f"Player {pid}"

            cur.execute("""
                INSERT INTO nhl.players (player_id, full_name, current_team_id, position, status)
                VALUES (%s, %s, %s, %s, 'active')
                ON CONFLICT (player_id) DO UPDATE
                  SET full_name = EXCLUDED.full_name,
                      current_team_id = COALESCE(EXCLUDED.current_team_id, nhl.players.current_team_id),
                      position = EXCLUDED.position,
                      status = 'active';
            """, (pid, nm, team_id, pos_code(raw, "F")))

            agg = attempts.get(pid, {"sog":0,"missed":0,"blocked":0})
            sog = int(agg["sog"])
            attempts_total = int(agg["sog"] + agg["missed"] + agg["blocked"])

            splits = sk_splits.get(pid, {"EV":0,"PP":0,"SH":0})
            ev_sog = int(splits["EV"])
            pp_sog = int(splits["PP"])
            sh_sog = int(splits["SH"])

            toi = parse_mmss_to_minutes(raw.get("toi") or raw.get("timeOnIce"))
            pp_toi = parse_mmss_to_minutes(raw.get("ppToi") or raw.get("powerPlayToi"))

            sk_batch.append((
                pid, game_id, team_id, opp_id, is_home, game_date,
                sog, attempts_total, toi, pp_toi,
                ev_sog, pp_sog, sh_sog
            ))

        if sk_batch:
            cur.executemany("""
                INSERT INTO nhl.skater_game_logs_raw
                  (player_id, game_id, team_id, opponent_id, is_home, game_date,
                   shots_on_goal, shot_attempts, toi_minutes, pp_toi_minutes,
                   ev_sog, pp_sog, sh_sog)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (player_id, game_id) DO UPDATE SET
                  team_id=EXCLUDED.team_id,
                  opponent_id=EXCLUDED.opponent_id,
                  is_home=EXCLUDED.is_home,
                  game_date=EXCLUDED.game_date,
                  shots_on_goal=EXCLUDED.shots_on_goal,
                  shot_attempts=EXCLUDED.shot_attempts,
                  toi_minutes=EXCLUDED.toi_minutes,
                  pp_toi_minutes=EXCLUDED.pp_toi_minutes,
                  ev_sog=EXCLUDED.ev_sog,
                  pp_sog=EXCLUDED.pp_sog,
                  sh_sog=EXCLUDED.sh_sog;
            """, sk_batch)

        # Goalies
        def team_splits(team_id: int) -> dict[str,int]:
            return team_sf.get(team_id, {"EV":0,"PP":0,"SH":0})

        gl_batch: List[tuple] = []
        for pid, raw, sect in home_goalies + away_goalies:
            team_id = home_id if sect=="homeTeam" else away_id
            opp_id  = away_id if sect=="homeTeam" else home_id
            is_home = (sect == "homeTeam")
            nm = name_by_pid.get(pid) or f"Player {pid}"

            cur.execute("""
                INSERT INTO nhl.players (player_id, full_name, current_team_id, position, status)
                VALUES (%s, %s, %s, %s, 'active')
                ON CONFLICT (player_id) DO UPDATE
                  SET full_name = EXCLUDED.full_name,
                      current_team_id = COALESCE(EXCLUDED.current_team_id, nhl.players.current_team_id),
                      position = EXCLUDED.position,
                      status = 'active';
            """, (pid, nm, team_id, "G"))

            toi = parse_mmss_to_minutes(raw.get("toi") or raw.get("timeOnIce"))
            shots_faced = to_int(raw.get("shotsAgainst") or raw.get("shotsFaced"))
            saves = to_int(raw.get("saves"))
            goals_allowed = to_int(raw.get("goalsAgainst"))
            start_flag = bool(raw.get("starter") or raw.get("isStarter") or False)
            pulled_flag = bool(raw.get("pulled") or False)

            ev_sf = pp_sf = sh_sf = None
            if (is_home and home_single) or ((not is_home) and away_single):
                ts = team_splits(team_id)
                ev_sf, pp_sf, sh_sf = int(ts["EV"]), int(ts["PP"]), int(ts["SH"])

            gl_batch.append((
                pid, game_id, team_id, opp_id, is_home, game_date,
                toi, shots_faced, saves, goals_allowed,
                start_flag, pulled_flag,
                ev_sf, pp_sf, sh_sf, None  # rebounds_allowed unknown
            ))

        if gl_batch:
            cur.executemany("""
                INSERT INTO nhl.goalie_game_logs_raw
                  (player_id, game_id, team_id, opponent_id, is_home, game_date,
                   toi_minutes, shots_faced, saves, goals_allowed,
                   start_flag, pulled_flag,
                   ev_shots_faced, pp_shots_faced, sh_shots_faced, rebounds_allowed)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (game_id, player_id) DO UPDATE SET
                   team_id=EXCLUDED.team_id,
                   opponent_id=EXCLUDED.opponent_id,
                   is_home=EXCLUDED.is_home,
                   game_date=EXCLUDED.game_date,
                   toi_minutes=EXCLUDED.toi_minutes,
                   shots_faced=EXCLUDED.shots_faced,
                   saves=EXCLUDED.saves,
                   goals_allowed=EXCLUDED.goals_allowed,
                   start_flag=EXCLUDED.start_flag,
                   pulled_flag=EXCLUDED.pulled_flag,
                   ev_shots_faced=COALESCE(EXCLUDED.ev_shots_faced, nhl.goalie_game_logs_raw.ev_shots_faced),
                   pp_shots_faced=COALESCE(EXCLUDED.pp_shots_faced, nhl.goalie_game_logs_raw.pp_shots_faced),
                   sh_shots_faced=COALESCE(EXCLUDED.sh_shots_faced, nhl.goalie_game_logs_raw.sh_shots_faced);
            """, gl_batch)

        conn.commit()

    print(f"✅ Ingested game {game_id}: skaters={len(sk_batch)} goalies={len(gl_batch)}")

# ─────────── CLI ───────────

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Ingest NHL boxscore + PBP into nhl.* tables")
    ap.add_argument("--game-id", type=int, required=True, help="NHL gamePk, e.g., 2025010041")
    args = ap.parse_args()
    ingest_game(args.game_id)
