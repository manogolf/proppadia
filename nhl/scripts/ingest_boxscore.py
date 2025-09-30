#!/usr/bin/env python3
from __future__ import annotations
import argparse, os, re
from typing import Any, Dict, List, Tuple, Iterable, Optional
import requests
import psycopg
import sys

# ───────────────────────── helpers ─────────────────────────

def env_db_url() -> str:
    db = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if not db:
        raise SystemExit("Missing SUPABASE_DB_URL / DATABASE_URL")
    return db

def fetch_json(url: str, timeout: int = 15) -> dict:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()

def parse_mmss_to_minutes(mmss: Optional[str]) -> Optional[float]:
    if not mmss:
        return None
    s = str(mmss).strip()
    if ":" not in s:
        return None
    try:
        m, ss = s.split(":", 1)
        return int(m) + int(ss)/60.0
    except Exception:
        return None

def to_int(x) -> Optional[int]:
    try:
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return None

# optional: map abbr → official team_id if API doesn’t surface it
TEAM_ID_BY_ABBR = {
    "ANA":24,"ARI":53,"BOS":6,"BUF":7,"CGY":20,"CAR":12,"CHI":16,"COL":21,"CBJ":29,"DAL":25,
    "DET":17,"EDM":22,"FLA":13,"LAK":26,"MIN":30,"MTL":8,"NSH":18,"NJD":1,"NYI":2,"NYR":3,
    "OTT":9,"PHI":4,"PIT":5,"SEA":55,"SJS":28,"STL":19,"TBL":14,"TOR":10,"UTA":41,
    "VAN":23,"VGK":54,"WPG":52,"WSH":15
}

# ───────────── PBP utilities (best-effort; safe if empty) ─────────────

def plays_list(pbp_obj) -> list:
    """Return a flat list of plays across several NHL payload shapes."""
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
    # prefer human-readable labels if present
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
    # fallbacks (numeric codes)
    for k in ("typeCode", "eventCode", "eventTypeId"):
        v = play.get(k)
        if isinstance(v, int):
            return f"CODE_{v}"
        if isinstance(v, str) and v.strip():
            return v.upper()
    return ""

def _play_team_abbr(p):
    d = p.get("details") or {}
    t = d.get("teamAbbrev")
    if t: return str(t).upper()
    t2 = p.get("team") or {}
    if isinstance(t2, dict):
        t = t2.get("triCode") or t2.get("abbrev")
        if t: return str(t).upper()
    return None

def event_bucket(et: str) -> Optional[str]:
    # treat GOAL as SOG
    if et in ("SHOT", "SHOT-ON-GOAL", "SHOT_ON_GOAL", "GOAL"):
        return "sog"
    if et in ("MISSED_SHOT", "MISSED-SHOT", "MISS"):
        return "missed"
    if et in ("BLOCKED_SHOT", "BLOCKED-SHOT", "BLOCK"):
        return "blocked"
    return None

# --- Add this helper (above aggregate_* functions) -----------------
def _is_sog_like(play: dict) -> bool:
    """
    Heuristic SOG detector that works when typeDescKey is missing and only numeric codes are present.
    - True  if details.isGoal is True
    - True  if details.shotOnGoal is True (some payloads use this)
    - True  if result.eventTypeId is 'GOAL' or 'SHOT' (legacy statsapi shape)
    - Else  fall back to the stringy event_type() check
    """
    d = play.get("details") or {}
    if d.get("isGoal") is True:
        return True
    if d.get("shotOnGoal") is True:  # seen in some api-web payloads
        return True
    res = play.get("result") or {}
    if isinstance(res.get("eventTypeId"), str) and res["eventTypeId"] in ("GOAL","SHOT"):
        return True
    # final fallback to our texty event_type()
    et = event_type(play)
    return et in ("SHOT","SHOT-ON-GOAL","SHOT_ON_GOAL","GOAL")

def shooter_id_from_play(play: dict):
    """
    Robust shooter id resolver for api-web & statsapi shapes.
    """
    d = play.get("details") or {}

    # api-web keys we've now observed
    for k in ("playerId", "shooterId", "shootingPlayerId", "scoringPlayerId"):
        pid = d.get(k)
        if pid is not None:
            try:
                return int(pid)
            except Exception:
                try:
                    return int(float(pid))
                except Exception:
                    pass

    # participants-style list (rare but seen)
    for part in (d.get("participants") or play.get("participants") or []):
        role = (part.get("type") or part.get("role") or "").lower()
        if role in ("shooter", "scorer"):
            pid = part.get("playerId") or part.get("id")
            if pid is not None:
                try:
                    return int(pid)
                except Exception:
                    try:
                        return int(float(pid))
                    except Exception:
                        pass

    # legacy players[] with role names
    for pl in play.get("players") or []:
        role = (pl.get("playerType") or pl.get("type") or "").lower()
        if role in ("shooter", "scorer"):
            pid = (pl.get("player") or {}).get("id") or pl.get("playerId") or pl.get("id")
            if pid is not None:
                try:
                    return int(pid)
                except Exception:
                    try:
                        return int(float(pid))
                    except Exception:
                        pass

    return None

# keep this if other code still calls _play_shooter_id(...)
_play_shooter_id = shooter_id_from_play

def _goalie_ids_from_box(box: dict) -> tuple[set[int], set[int]]:
    """Return (home_goalie_ids, away_goalie_ids) from playerByGameStats."""
    def collect(side: str) -> set[int]:
        out = set()
        pbg = (box.get("playerByGameStats") or {}).get(side) or {}
        for g in pbg.get("goalies", []) or []:
            pid = to_int(g.get("playerId"))
            if pid: out.add(pid)
        return out
    return collect("homeTeam"), collect("awayTeam")

def _ppsh_for_goalie(goalie_is_home: bool, home_skaters: int | None, away_skaters: int | None) -> str:
    """Label from the goalie’s perspective: opponent advantage == PP."""
    if home_skaters is None or away_skaters is None:
        return "EV"
    if goalie_is_home:
        # opponent is away
        if away_skaters == home_skaters: return "EV"
        return "PP" if away_skaters > home_skaters else "SH"
    else:
        # opponent is home
        if home_skaters == away_skaters: return "EV"
        return "PP" if home_skaters > away_skaters else "SH"

def compute_goalie_splits_from_pbp(pbp_obj, home_goalie_ids: list[int], away_goalie_ids: list[int]):
    """
    Attribute SOG-like plays to the goalie in net if details.goalieInNetId is present.
    Falls back to side if needed. Returns: { goalie_pid: {"EV":e,"PP":p,"SH":s} }
    """
    home_set = set(home_goalie_ids or [])
    away_set = set(away_goalie_ids or [])

    out = {}
    plays = plays_list(pbp_obj)
    used = 0

    # NEW: debug tallies
    label_counts = {"EV": 0, "PP": 0, "SH": 0}
    sit_counts = {}

    for p in plays:
        et = event_type(p)
        if et not in ("SHOT", "SHOT-ON-GOAL", "SHOT_ON_GOAL", "GOAL"):
            continue

        # which side shot?
        d = p.get("details") or {}
        gid = to_int(d.get("goalieInNetId"))
        hs, aw = _sit_counts(p)

        # shot was taken by HOME if the goalie in net belongs to AWAY, and vice-versa.
        # if we don't have goalieInNetId, we can't reliably pinpoint a single goalie.
        lab = _strength(True, hs, aw)  # temporary init; will be re-set right away below

        if gid is not None:
            # Determine which side is shooting by goalie side
            if gid in home_set:
                # shot taken by AWAY → shooter is away, defender is home
                lab = _strength(False, hs, aw)  # shooter is AWAY ⇒ shoot_home=False
                out.setdefault(gid, {"EV": 0, "PP": 0, "SH": 0})
                out[gid][lab] += 1
            elif gid in away_set:
                # shot taken by HOME → shooter is home, defender is away
                lab = _strength(True, hs, aw)   # shooter is HOME ⇒ shoot_home=True
                out.setdefault(gid, {"EV": 0, "PP": 0, "SH": 0})
                out[gid][lab] += 1
            else:
                # unknown goalie id; skip
                continue

            # debug tallies
            label_counts[lab] += 1
            sc = p.get("situationCode")
            if isinstance(sc, str) and sc:
                sit_counts[sc] = sit_counts.get(sc, 0) + 1

            used += 1

    print(
        "[dbg] goalie split sog-faced used="
        f"{used} goalies={len(out)} "
        f"labels EV={label_counts['EV']} PP={label_counts['PP']} SH={label_counts['SH']} "
        f"sits={dict(sorted(sit_counts.items()))}"
    )
    return out

def aggregate_attempts_from_pbp(pbp_obj):
    """
    Return { player_id: { 'sog': n, 'missed': n, 'blocked': n } }.
    For now we only count SOG (incl. GOAL) from PBP; 'missed'/'blocked' can be
    added later if/when those event labels are stable in this feed.
    """
    out = {}
    plays = plays_list(pbp_obj)
    sog_like = 0
    shooters = set()

    for p in plays:
        et = event_type(p)
        if et not in ("SHOT", "SHOT-ON-GOAL", "SHOT_ON_GOAL", "GOAL"):
            continue
        sog_like += 1

        pid = shooter_id_from_play(p)
        if pid is None:
            continue

        shooters.add(pid)
        d = out.setdefault(pid, {"sog": 0, "missed": 0, "blocked": 0})
        d["sog"] += 1

    print(f"[dbg] pbp plays={len(plays)} sog-like={sog_like} shooters={len(shooters)}")
    return out

# AFTER (parse situationCode like "5v4", with fallbacks)
def _sit_counts(p):
    """
    Return (home_skaters, away_skaters), decoding situationCode if present.
    situationCode is 4 digits: Hg Hs As Ag
      Hg = 1 if home goalie on, 0 if off
      Hs = home skaters (excluding goalie)
      As = away skaters (excluding goalie)
      Ag = 1 if away goalie on, 0 if off
    Examples:
      1551 -> home: goalie on, 5 skaters; away: 5 skaters, goalie on  -> 5v5
      1541 -> 5v4 (home PP)
      1451 -> 4v5 (home SH)
      1560 -> 5v6 (away goalie pulled)
    """
    sc = p.get("situationCode")
    if isinstance(sc, str) and len(sc) == 4 and sc.isdigit():
        # Hg, Hs, As, Ag (we only need Hs/As here)
        _hg, hs, as_, _ag = sc
        try:
            return int(hs), int(as_)
        except Exception:
            pass

    # fallback fields if present in other payload shapes
    hs = p.get("homeSkaters") or (p.get("about") or {}).get("homeSkaters")
    as_ = p.get("awaySkaters") or (p.get("about") or {}).get("awaySkaters")
    try:
        return (int(hs) if hs is not None else None,
                int(as_) if as_ is not None else None)
    except Exception:
        return (None, None)

def _strength(shoot_home, hs, aw):
    if hs is None or aw is None: return "EV"
    if hs == aw: return "EV"
    return "PP" if ((shoot_home and hs>aw) or ((not shoot_home) and aw>hs)) else "SH"

def _play_team_side(play: dict, home_id: int, away_id: int, home_abbr: str, away_abbr: str) -> str | None:
    d = play.get("details") or {}
    # Best source: numeric owner id on api-web payloads
    owner = d.get("eventOwnerTeamId")
    if isinstance(owner, int):
        if owner == home_id: return "HOME"
        if owner == away_id: return "AWAY"

    # Fallback to abbrev if present
    abbr = (_play_team_abbr(play) or "").upper()
    if abbr == home_abbr: return "HOME"
    if abbr == away_abbr: return "AWAY"
    return None

def compute_splits_from_pbp(pbp_obj, home_id: int, away_id: int, home_abbr: str, away_abbr: str):
    """
    Return { pid: {'EV': e, 'PP': p, 'SH': s} } using shot-like events.
    Uses details.eventOwnerTeamId / team abbrev to determine which side took the shot.
    """
    sk = {}
    plays = plays_list(pbp_obj)
    used = 0
    shooters = set()

    # NEW: debug tallies
    label_counts = {"EV": 0, "PP": 0, "SH": 0}
    sit_counts = {}  # situationCode -> count

    for p in plays:
        et = event_type(p)
        if et not in ("SHOT", "SHOT-ON-GOAL", "SHOT_ON_GOAL", "GOAL"):
            continue

        pid  = shooter_id_from_play(p)
        side = _play_team_side(p, home_id, away_id, home_abbr, away_abbr)  # "HOME"/"AWAY"
        if pid is None or side is None:
            continue

        hs, aw = _sit_counts(p)
        lab = _strength(side == "HOME", hs, aw)

        row = sk.setdefault(pid, {"EV": 0, "PP": 0, "SH": 0})
        row[lab] += 1

        # debug tallies
        label_counts[lab] += 1
        sc = p.get("situationCode")
        if isinstance(sc, str) and sc:
            sit_counts[sc] = sit_counts.get(sc, 0) + 1

        used += 1
        shooters.add(pid)

    # richer debug so we can see what's happening
    print(
        "[dbg] split sog-like used="
        f"{used} shooters={len(shooters)} "
        f"labels EV={label_counts['EV']} PP={label_counts['PP']} SH={label_counts['SH']} "
        f"sits={dict(sorted(sit_counts.items()))}"
    )
    return sk

def compute_team_sf_splits_from_pbp(pbp, home_abbr: str, away_abbr: str,
                                    home_team_id: int, away_team_id: int) -> dict[int, dict[str, int]]:
    """
    Count SOG-like events (SOG + GOAL) by strength for the DEFENDING team.
    Returns: { team_id: {"EV": e, "PP": p, "SH": s} }
    """
    team_sf = {
        home_team_id: {"EV": 0, "PP": 0, "SH": 0},
        away_team_id: {"EV": 0, "PP": 0, "SH": 0},
    }
    plays = plays_list(pbp)
    for p in plays:
        if not isinstance(p, dict):
            continue
        if not _is_sog_like(p):
            continue
        t = _play_team_abbr(p)
        if t not in (home_abbr, away_abbr):
            continue
        hs, aw = _sit_counts(p)
        lab = _strength(t == home_abbr, hs, aw)  # strength from the SHOOTING side
        # defending team is the opposite of the shooting team
        def_team_id = away_team_id if t == home_abbr else home_team_id
        team_sf[def_team_id][lab] += 1
    return team_sf

def _dbg_probe_pbp(pbp):
    plays = plays_list(pbp)
    print(f"[dbg] PBP root={type(pbp).__name__} plays={len(plays)}")
    for i, p in enumerate(plays[:5]):
        if not isinstance(p, dict):
            print(f"[dbg] play[{i}] is {type(p).__name__}")
            continue
        det = p.get("details") or {}
        res = p.get("result") or {}
        print(f"[dbg] play[{i}] keys={list(p.keys())}")
        print("       typeCode=", p.get("typeCode"),
              " typeDescKey=", p.get("typeDescKey"),
              " result.eventTypeId=", res.get("eventTypeId"))
        print("       details:",
              "isGoal=", det.get("isGoal"),
              "shotOnGoal=", det.get("shotOnGoal"),
              "teamAbbrev=", det.get("teamAbbrev"),
              "playerId=", det.get("playerId"),
              "situationCode=", det.get("situationCode"))

def parse_sa(s):  # "9/10" -> 10
    if isinstance(s, str) and "/" in s:
        try: return int(s.split("/",1)[1])
        except: return None
    return None
# ───────────────────────── main ingest ─────────────────────────

def ingest_game(game_id: int) -> None:
    box_url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"
    pbp_url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"

    # Fetch payloads
    box = fetch_json(box_url)
    try:
        pbp = fetch_json(pbp_url)
        _dbg_probe_pbp(pbp)  # << add this line

    except Exception:
        pbp = {}

        # --- DEBUG: inspect first 2 shot-like plays to find shooter id fields ---
    try:
        shown = 0
        for _p in plays_list(pbp):
            if isinstance(_p, dict) and _is_sog_like(_p):
                shown += 1
                print(f"[probe] shot-like #{shown}  event_type={event_type(_p)}")
                print("  top keys:", sorted(list(_p.keys())))
                det = _p.get("details") or {}
                print("  details keys:", sorted(list(det.keys())))
                # show likely shooter fields if present
                for k in ("playerId", "shooterId", "shootingPlayerId", "scoringPlayerId"):
                    if k in det:
                        print(f"  details.{k} =", det[k])
                # show players[] roles / ids (first 4)
                pls = _p.get("players") or []
                snippet = []
                for pl in pls[:4]:
                    if isinstance(pl, dict):
                        snippet.append({
                            "playerType": pl.get("playerType") or pl.get("type") or pl.get("role"),
                            "id": (pl.get("player") or {}).get("id") or pl.get("playerId")
                        })
                print("  players snippet:", snippet)
                if shown >= 2:
                    break
    except Exception as _e:
        print("[probe error]", _e)

    # Teams / game meta
    home = (box.get("homeTeam") or {})
    away = (box.get("awayTeam") or {})
    home_abbr = (home.get("abbrev") or "").upper()
    away_abbr = (away.get("abbrev") or "").upper()
    home_id = to_int(home.get("id")) or TEAM_ID_BY_ABBR.get(home_abbr)
    away_id = to_int(away.get("id")) or TEAM_ID_BY_ABBR.get(away_abbr)
    if not home_id or not away_id:
        raise SystemExit("Could not resolve team IDs from API; extend TEAM_ID_BY_ABBR.")

    game_date = box.get("gameDate") or box.get("startTimeUTC")
    if game_date and "T" in game_date:
        game_date = game_date.split("T",1)[0]

    attempts   = aggregate_attempts_from_pbp(pbp)                   # pid -> {sog,...}
    sk_splits  = compute_splits_from_pbp(pbp, home_id, away_id, home_abbr, away_abbr)
    team_sf = compute_team_sf_splits_from_pbp(pbp, home_abbr, away_abbr, home_id, away_id)
    home_goalie_ids, away_goalie_ids = _goalie_ids_from_box(box)
    goalie_splits = compute_goalie_splits_from_pbp(pbp, home_goalie_ids, away_goalie_ids)


            # ---- Build name_by_pid (shared by skaters & goalies) ----
    name_by_pid: Dict[int, str] = {}

    def add_names_from_pbg(side_key: str):
        pbg = (box.get("playerByGameStats") or {}).get(side_key) or {}
        for key in ("forwards", "defense", "goalies"):
            for p in pbg.get(key, []) or []:
                pid = to_int(p.get("playerId"))
                if not pid:
                    continue
                nm_node = p.get("name") or {}
                nm = (nm_node.get("default") or "").strip()
                if nm:
                    name_by_pid[pid] = nm

    add_names_from_pbg("homeTeam")
    add_names_from_pbg("awayTeam")

    # Fallback: derive names from box.homeTeam/awayTeam.players{} if still missing
    for side in ("homeTeam", "awayTeam"):
        team = box.get(side) or {}
        players = team.get("players") or {}
        if isinstance(players, dict):
            for pid_s, pdata in players.items():
                pid = to_int(pid_s)
                if not pid or pid in name_by_pid:
                    continue
                fn = ((pdata.get("firstName") or {}).get("default")
                      or pdata.get("firstName") or "").strip()
                ln = ((pdata.get("lastName") or {}).get("default")
                      or pdata.get("lastName") or "").strip()
                nm = (fn + " " + ln).strip()
                if nm:
                    name_by_pid[pid] = nm

    # Build name map from playerByGameStats (preferred)
    name_by_pid: Dict[int, str] = {}

    def add_names_from_pbg(side_key: str):
        pbg = (box.get("playerByGameStats") or {}).get(side_key) or {}
        for key in ("forwards","defense","goalies"):
            for p in pbg.get(key, []) or []:
                pid = to_int(p.get("playerId"))
                if not pid:
                    continue
                nm_node = p.get("name") or {}
                nm = nm_node.get("default") or ""
                name_by_pid[pid] = str(nm).strip() or f"Player {pid}"

    add_names_from_pbg("homeTeam")
    add_names_from_pbg("awayTeam")

    # Fallback name collection (rarely needed)
    def add_names_from_players_dict(section: str):
        team = box.get(section) or {}
        players = team.get("players")
        if isinstance(players, dict):
            for pid_s, pdata in players.items():
                m = re.search(r"\d+", str(pid_s))
                if not m: 
                    continue
                pid = int(m.group())
                fn = ((pdata.get("firstName") or {}).get("default") 
                      or pdata.get("firstName") or "")
                ln = ((pdata.get("lastName")  or {}).get("default") 
                      or pdata.get("lastName")  or "")
                nm = (f"{fn} {ln}").strip() or f"Player {pid}"
                name_by_pid.setdefault(pid, nm)

    add_names_from_players_dict("homeTeam")
    add_names_from_players_dict("awayTeam")

    # Iterators (prefer playerByGameStats)
    def iter_skaters(section: str) -> Iterable[Tuple[int, dict, str]]:
        pbg = (box.get("playerByGameStats") or {}).get(section) or {}
        had_any = False
        for key in ("forwards","defense"):
            for p in pbg.get(key, []) or []:
                pid = to_int(p.get("playerId"))
                if not pid: 
                    continue
                had_any = True
                yield pid, p, section
        if had_any:
            return
        # fallback: players dict (exclude goalies) or legacy arrays
        team = box.get(section) or {}
        players = team.get("players")
        if isinstance(players, dict):
            for pid_s, pdata in players.items():
                m = re.search(r"\d+", str(pid_s))
                if not m: 
                    continue
                pid = int(m.group())
                pos = (pdata.get("positionCode") or pdata.get("position") or "").upper()
                if pos != "G":
                    yield pid, pdata, section
        for key in ("skaters", "forwards", "defense"):
            for p in team.get(key, []) or []:
                pid = to_int(p.get("playerId") or p.get("id"))
                if pid:
                    yield pid, p, section

    def iter_goalies(section: str) -> Iterable[Tuple[int, dict, str]]:
        pbg = (box.get("playerByGameStats") or {}).get(section) or {}
        had_any = False
        for p in pbg.get("goalies", []) or []:
            pid = to_int(p.get("playerId"))
            if not pid: 
                continue
            had_any = True
            yield pid, p, section
        if had_any:
            return
        team = box.get(section) or {}
        players = team.get("players")
        if isinstance(players, dict):
            for pid_s, pdata in players.items():
                m = re.search(r"\d+", str(pid_s))
                if not m: 
                    continue
                pid = int(m.group())
                pos = (pdata.get("positionCode") or pdata.get("position") or "").upper()
                if pos == "G":
                    yield pid, pdata, section
        for p in team.get("goalies", []) or []:
            pid = to_int(p.get("playerId") or p.get("id"))
            if pid:
                yield pid, p, section

    # Aggregations from PBP (safe if empty)
    attempts = aggregate_attempts_from_pbp(pbp)  # {pid: {sog, missed, blocked}}

    # ───────────── DB upserts ─────────────
    DB = env_db_url()
    with psycopg.connect(DB) as conn, conn.cursor() as cur:
        try:
            # Teams
            for tid, abbr, name in (
                (home_id, home_abbr, (home.get("commonName") or {}).get("default") or home_abbr),
                (away_id, away_abbr, (away.get("commonName") or {}).get("default") or away_abbr),
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
                  SET game_date     = EXCLUDED.game_date,
                      home_team_id  = EXCLUDED.home_team_id,
                      away_team_id  = EXCLUDED.away_team_id,
                      status        = EXCLUDED.status;
            """, (game_id, game_date, home_id, away_id))

            def pos_code(raw: dict, default: str) -> str:
                code = (raw.get("positionCode") or raw.get("position") or "").upper()
                if code in ("G","D","F"): return code
                if code in ("LW","RW","C"): return "F"
                return default

            print(f"[dbg] attempts has {len(attempts)} shooters")
            print(f"[dbg] sk_splits has {len(sk_splits)} shooters")

            # ── Skaters (use boxscore SOG; attempts from PBP if available) ──
            did_log = False
            sk_batch: List[tuple] = []

            for pid, raw, sect in list(iter_skaters("homeTeam")) + list(iter_skaters("awayTeam")):
                team_id = home_id if sect == "homeTeam" else away_id
                opp_id  = away_id if sect == "homeTeam" else home_id
                is_home = (sect == "homeTeam")
                nm = name_by_pid.get(pid) or f"Player {pid}"

                cur.execute("""
                    INSERT INTO nhl.players (player_id, full_name, current_team_id, position, status)
                    VALUES (%s, %s, %s, %s, 'active')
                    ON CONFLICT (player_id) DO UPDATE
                      SET full_name      = EXCLUDED.full_name,
                          current_team_id = COALESCE(EXCLUDED.current_team_id, nhl.players.current_team_id),
                          position        = EXCLUDED.position,
                          status          = 'active';
                """, (pid, nm, team_id, pos_code(raw, "F")))

                # SOG from boxscore; attempts via PBP if present
                sog_box = to_int(raw.get("sog") or raw.get("shotsOnGoal") or raw.get("shots"))

                agg = attempts.get(pid)
                if agg:
                    sog_pbp = to_int(agg.get("sog"))
                    miss    = to_int(agg.get("missed"))
                    blk     = to_int(agg.get("blocked"))
                    attempts_total = (sog_pbp or 0) + (miss or 0) + (blk or 0)
                    # prefer boxscore SOG if present
                    sog = sog_box if sog_box is not None else sog_pbp
                else:
                    sog = sog_box
                    attempts_total = None  # unknown without PBP

                toi    = parse_mmss_to_minutes(raw.get("toi") or raw.get("timeOnIce"))
                pp_toi = parse_mmss_to_minutes(raw.get("ppToi") or raw.get("powerPlayToi"))

                # SOG strength splits from PBP (EV/PP/SH)
                spl = sk_splits.get(pid, {"EV": 0, "PP": 0, "SH": 0})
                ev_sog = int(spl["EV"])
                pp_sog = int(spl["PP"])
                sh_sog = int(spl["SH"])

                if not did_log:
                    print(f"[dbg] first skater pid={pid} sog_pbp={attempts.get(pid,{}).get('sog')} box_sog={sog_box} splits={sk_splits.get(pid)}")
                    did_log = True

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
                    team_id        = EXCLUDED.team_id,
                    opponent_id    = EXCLUDED.opponent_id,
                    is_home        = EXCLUDED.is_home,
                    game_date      = EXCLUDED.game_date,
                    shots_on_goal  = EXCLUDED.shots_on_goal,
                    shot_attempts  = EXCLUDED.shot_attempts,
                    toi_minutes    = EXCLUDED.toi_minutes,
                    pp_toi_minutes = EXCLUDED.pp_toi_minutes,
                    ev_sog         = EXCLUDED.ev_sog,
                    pp_sog         = EXCLUDED.pp_sog,
                    sh_sog         = EXCLUDED.sh_sog;
                """, sk_batch)

            # ---- DEBUG & collect goalie rows (place here) ----------------------------
            home_goalie_rows = list(iter_goalies("homeTeam"))
            away_goalie_rows = list(iter_goalies("awayTeam"))
            print(
                f"[dbg] iter_goalies: home={len(home_goalie_rows)} "
                f"away={len(away_goalie_rows)} "
                f"idsH={[pid for pid,_,_ in home_goalie_rows]} "
                f"idsA={[pid for pid,_,_ in away_goalie_rows]}"
            )

            # ── Goalies (totals from boxscore; add splits if available) ──
            gl_batch: List[tuple] = []
            for pid, raw, sect in home_goalie_rows + away_goalie_rows:
                team_id = home_id if sect == "homeTeam" else away_id
                opp_id  = away_id if sect == "homeTeam" else home_id
                is_home = (sect == "homeTeam")
                nm = name_by_pid.get(pid) or f"Player {pid}"

                cur.execute("""
                    INSERT INTO nhl.players (player_id, full_name, current_team_id, position, status)
                    VALUES (%s, %s, %s, %s, 'active')
                    ON CONFLICT (player_id) DO UPDATE
                    SET full_name      = EXCLUDED.full_name,
                        current_team_id = COALESCE(EXCLUDED.current_team_id, nhl.players.current_team_id),
                        position        = EXCLUDED.position,
                        status          = 'active';
                """, (pid, nm, team_id, "G"))

                toi = parse_mmss_to_minutes(raw.get("toi") or raw.get("timeOnIce"))
                shots_faced   = to_int(raw.get("shotsAgainst") or raw.get("shotsFaced"))
                saves         = to_int(raw.get("saves"))
                goals_allowed = to_int(raw.get("goalsAgainst"))
                start_flag    = bool(raw.get("starter") or raw.get("isStarter") or False)
                pulled_flag   = bool(raw.get("pulled") or False)

                # Strength-faced splits from precomputed goalie_splits (if present)
                ev_sf = pp_sf = sh_sf = None
                if 'goalie_splits' in locals() and isinstance(goalie_splits, dict):
                    gs = goalie_splits.get(pid)
                    if gs:
                        ev_sf = to_int(gs.get("EV"))
                        pp_sf = to_int(gs.get("PP"))
                        sh_sf = to_int(gs.get("SH"))

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
                    team_id         = EXCLUDED.team_id,
                    opponent_id     = EXCLUDED.opponent_id,
                    is_home         = EXCLUDED.is_home,
                    game_date       = EXCLUDED.game_date,
                    toi_minutes     = EXCLUDED.toi_minutes,
                    shots_faced     = EXCLUDED.shots_faced,
                    saves           = EXCLUDED.saves,
                    goals_allowed   = EXCLUDED.goals_allowed,
                    start_flag      = EXCLUDED.start_flag,
                    pulled_flag     = EXCLUDED.pulled_flag,
                    ev_shots_faced  = COALESCE(EXCLUDED.ev_shots_faced, nhl.goalie_game_logs_raw.ev_shots_faced),
                    pp_shots_faced  = COALESCE(EXCLUDED.pp_shots_faced, nhl.goalie_game_logs_raw.pp_shots_faced),
                    sh_shots_faced  = COALESCE(EXCLUDED.sh_shots_faced, nhl.goalie_game_logs_raw.sh_shots_faced);
                """, gl_batch)

            conn.commit()
            print(f"✅ Ingested game {game_id}: skaters={len(sk_batch)} goalies={len(gl_batch)}")

        except Exception as e:
            conn.rollback()
            print("[DB ERROR]", type(e).__name__, e, file=sys.stderr)
            raise

# ───────────────────────── CLI ─────────────────────────

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Ingest NHL boxscore (+PBP attempts if available) into nhl.* tables")
    ap.add_argument("--game-id", type=int, required=True, help="NHL gamePk, e.g., 2025010041")
    args = ap.parse_args()
    ingest_game(args.game_id)
