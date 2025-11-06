#!/usr/bin/env python3
from __future__ import annotations

import argparse, os, re, sys
import datetime as dt
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Tuple, Iterable, Optional

import requests
import psycopg
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from pathlib import Path

# ───────────────────────── env / DB ─────────────────────────
try:
    from dotenv import load_dotenv
    ROOT = Path(__file__).resolve().parents[2]   # nhl/scripts -> nhl -> <repo root>
    load_dotenv(ROOT / ".env", override=True)
except Exception:
    pass

os.environ.setdefault("PGSSLMODE", "require")
os.environ.setdefault("PGGSSENCMODE", "disable")

DB = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
if not DB:
    raise SystemExit("Missing SUPABASE_DB_URL / DATABASE_URL")

# Debug print helper (enabled when DEBUG_PBP is truthy)
DEBUG_PBP = os.getenv("DEBUG_PBP") not in (None, "", "0", "false", "False")
def dprint(*args, **kwargs):
    if DEBUG_PBP:
        print(*args, **kwargs)

# ───────────────────────── helpers ─────────────────────────
def _http() -> requests.Session:
    r = Retry(
        total=5, connect=5, read=5, backoff_factor=0.4,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset({"GET"}),
    )
    s = requests.Session()
    s.headers.update({"User-Agent": "proppadia-nhl-boxscore/1.0"})
    s.mount("https://", HTTPAdapter(max_retries=r))
    return s

S = _http()

def fetch_json(url: str, timeout: int = 15) -> dict:
    r = S.get(url, timeout=timeout)
    if r.status_code == 404:
        return {}
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
    "OTT":9,"PHI":4,"PIT":5,"SEA":55,"SJS":28,"STL":19,"TBL":14,"TOR":10,"UTA":68,
    "VAN":23,"VGK":54,"WPG":52,"WSH":15
}

def _to_secs_mmss(val) -> Optional[int]:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        try:
            v = int(val)
            return v if v >= 0 else None
        except Exception:
            return None
    s = str(val)
    if ":" not in s:
        return None
    try:
        m, ss = s.split(":", 1)
        return int(m) * 60 + int(ss)
    except Exception:
        return None

def fetch_statsapi_shifts(game_id: int) -> List[dict]:
    """
    Fetch shift chart rows from the public StatsAPI and normalize to:
      [{"pid": player_id, "tid": team_id, "start": abs_sec, "end": abs_sec}, ...]
    Absolute seconds are measured from game start (period blocks of 20min).
    """
    url = f"https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={game_id}"
    try:
        js = fetch_json(url, timeout=25)
    except Exception as e:
        dprint("[dbg] statsapi shifts fetch failed:", e)
        return []

    rows = (js.get("data") or js.get("shifts") or []) if isinstance(js, dict) else []
    out: List[dict] = []

    for r in rows:
        pid = to_int(r.get("playerId") or r.get("playerId_x") or r.get("playerId_y") or r.get("playerIdNumeric"))
        tid = to_int(r.get("teamId")) or TEAM_ID_BY_ABBR.get(str(r.get("teamAbbrev") or "").upper())
        per = to_int(r.get("period"))
        # times may come as "MM:SS" or already-in-seconds
        st_raw = r.get("startTimeInSeconds") or r.get("startTime") or r.get("startTimeSeconds")
        et_raw = r.get("endTimeInSeconds")   or r.get("endTime")   or r.get("endTimeSeconds")
        dur    = to_int(r.get("duration"))

        st = _to_secs_mmss(st_raw) if isinstance(st_raw, str) else to_int(st_raw)
        et = _to_secs_mmss(et_raw) if isinstance(et_raw, str) else to_int(et_raw)

        if st is None and et is not None and dur is not None:
            st = et - dur
        if et is None and st is not None and dur is not None:
            et = st + dur

        if not (pid and tid and per and st is not None and et is not None):
            continue
        if et <= st:
            continue

        abs_start = (per - 1) * 20 * 60 + st
        abs_end   = (per - 1) * 20 * 60 + et
        out.append({"pid": pid, "tid": tid, "start": abs_start, "end": abs_end})

    dprint(f"[dbg] statsapi shifts: normalized {len(out)} intervals")
    return out


# ───────────── PBP utilities (best-effort; safe if empty) ─────────────
def iso_to_et_date(iso_str) -> Optional[str]:
    if not iso_str:
        return None
    s = str(iso_str).strip()
    try:
        if s.endswith("Z"):
            dt_utc = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            dt_utc = dt.datetime.fromisoformat(s)
        return dt_utc.astimezone(ZoneInfo("America/New_York")).date().isoformat()
    except Exception:
        return s.split("T",1)[0] if "T" in s else None

def map_api_state_to_db(state: str) -> str:
    m = {
        "FUT":"scheduled", "PRE":"scheduled",
        "LIVE":"live", "CRIT":"live",
        "FINAL":"final", "OFF":"final",
        "POSTPONED":"postponed", "CANCELED":"canceled",
    }
    return m.get((state or "").upper(), "scheduled")

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

def _is_sog_like(play: dict) -> bool:
    """
    Heuristic SOG detector:
    - details.isGoal True
    - details.shotOnGoal True
    - result.eventTypeId in {'GOAL','SHOT'}
    - or texty event_type() in {'SHOT','SHOT-ON-GOAL','SHOT_ON_GOAL','GOAL'}
    """
    d = play.get("details") or {}
    if d.get("isGoal") is True:
        return True
    if d.get("shotOnGoal") is True:
        return True
    res = play.get("result") or {}
    if isinstance(res.get("eventTypeId"), str) and res["eventTypeId"] in ("GOAL","SHOT"):
        return True
    et = event_type(play)
    return et in ("SHOT","SHOT-ON-GOAL","SHOT_ON_GOAL","GOAL")

def shooter_id_from_play(play: dict):
    """Robust shooter id resolver for api-web & statsapi shapes."""
    d = play.get("details") or {}

    # api-web keys we've seen
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

    # participants-style list
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

def get_pp_toi_minutes_from_box(raw: dict) -> Optional[float]:
    """
    Return PP TOI minutes from a boxscore skater node, robust to key changes.
    """
    # flat keys first
    for k in ("ppToi", "powerPlayToi", "powerPlayTimeOnIce", "ppTimeOnIce", "pp_time_on_ice", "pp_toi"):
        v = raw.get(k)
        if v:
            m = parse_mmss_to_minutes(v)
            if m is not None:
                return m

    # nested variants (rare)
    for nest in ("powerPlay", "specialTeams", "pp", "situational"):
        sub = raw.get(nest)
        if isinstance(sub, dict):
            for k in ("toi", "timeOnIce", "ppToi", "pp_time_on_ice"):
                v = sub.get(k)
                if v:
                    m = parse_mmss_to_minutes(v)
                    if m is not None:
                        return m
    # sometimes under stats.skaterStats
    stats_node = ((raw.get("stats") or {}).get("skaterStats") or {})
    for k in ("ppToi", "ppTimeOnIce", "powerPlayTimeOnIce"):
        v = stats_node.get(k)
        if v:
            m = parse_mmss_to_minutes(v)
            if m is not None:
                return m

    return None

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

def _sit_counts(p):
    """
    Return (home_skaters, away_skaters) from a play dict `p`.

    Tries, in order:
      1) NHL numeric situationCode "HgHsAsAg" (e.g., "1551" -> 5v5, "1541" -> 5v4)
      2) Textual "5v4"
      3) Fallback fields: p['homeSkaters'] / p['awaySkaters'] or p['about'][…]
    On failure returns (None, None).
    """
    hs = as_ = None
    sc = p.get("situationCode")

    # Normalize to string if an int slipped through
    if isinstance(sc, (int, float)):
        sc = str(sc)

    # Case 1: canonical 4-digit numeric "HgHsAsAg"
    if isinstance(sc, str) and sc:
        if len(sc) == 4 and sc.isdigit():
            try:
                # Hg, Hs, As, Ag (we only need Hs/As here)
                _hg, hs_ch, as_ch, _ag = sc
                hs, as_ = int(hs_ch), int(as_ch)
            except Exception:
                hs = as_ = None
        # Case 2: textual like "5v4"
        elif "v" in sc:
            try:
                left, right = sc.split("v", 1)
                hs, as_ = int(left), int(right)
            except Exception:
                hs = as_ = None

    # Case 3: fallback to explicit fields if present
    if hs is None or as_ is None:
        about = p.get("about") or {}
        hs2 = p.get("homeSkaters", about.get("homeSkaters"))
        as2 = p.get("awaySkaters", about.get("awaySkaters"))
        try:
            if hs is None and hs2 is not None:
                hs = int(hs2)
            if as_ is None and as2 is not None:
                as_ = int(as2)
        except Exception:
            hs = as_ = None

    return hs, as_

# --- PP window builder from PBP (step 1) ------------------------------------

def _clock_to_sec(mmss: str) -> Optional[int]:
    if not isinstance(mmss, str) or ":" not in mmss:
        return None
    try:
        m, s = mmss.split(":", 1)
        return int(m) * 60 + int(s)
    except Exception:
        return None

def _play_abs_time(p: dict) -> Optional[tuple[int, int]]:
    """
    Return (period_number, absolute_seconds_from_game_start) for a play.
    Uses periodDescriptor.number and timeInPeriod (both present in api-web PBP).
    """
    per_obj = p.get("periodDescriptor") or p.get("about") or {}
    per = per_obj.get("number") or per_obj.get("period")
    t = p.get("timeInPeriod") or per_obj.get("periodTime")
    if not per or not t:
        return None
    ts = _clock_to_sec(str(t))
    if ts is None:
        return None
    try:
        per_i = int(per)
        return per_i, (per_i - 1) * 20 * 60 + ts
    except Exception:
        return None

def build_pp_windows_from_pbp(pbp_obj) -> list[tuple[str, int, int]]:
    """
    Build continuous PP windows: list of (advantage_side, start_sec, end_sec),
    where advantage_side is "HOME" or "AWAY".
    We detect advantage via situationCode (e.g., 1541 => HOME has 5 vs AWAY 4).
    """
    plays = [p for p in plays_list(pbp_obj) if isinstance(p, dict)]
    rows = []
    for p in plays:
        tinfo = _play_abs_time(p)
        if not tinfo:
            continue
        per, abs_s = tinfo
        hs, as_ = _sit_counts(p)
        rows.append((per, abs_s, hs, as_))

    if not rows:
        return []

    # sort by time
    rows.sort(key=lambda r: (r[0], r[1]))

    windows: list[tuple[str, int, int]] = []
    # iterate contiguous segments between plays
    for i, (per, abs_s, hs, as_) in enumerate(rows):
        if hs is None or as_ is None or hs == as_:
            continue  # EV or unknown
        adv = "HOME" if hs > as_ else "AWAY"

        # end at next play in same period, or period end if this is the last
        if i + 1 < len(rows) and rows[i + 1][0] == per:
            end_abs = rows[i + 1][1]
        else:
            # period end is 20:00 of that period
            end_abs = per * 20 * 60

        if end_abs > abs_s:
            windows.append((adv, abs_s, end_abs))

    return windows


def _strength(shoot_home: bool, hs: Optional[int], aw: Optional[int]) -> str:
    if hs is None or aw is None:
        return "EV"
    if hs == aw:
        return "EV"
    return "PP" if ((shoot_home and hs > aw) or ((not shoot_home) and aw > hs)) else "SH"

def compute_goalie_splits_from_pbp(pbp_obj, home_goalie_ids: list[int], away_goalie_ids: list[int]) -> Dict[int, Dict[str, int]]:
    """
    Attribute SOG-like plays to the goalie in net if details.goalieInNetId is present.
    Returns: { goalie_pid: {"EV":e,"PP":p,"SH":s} }
    """
    home_set = set(home_goalie_ids or [])
    away_set = set(away_goalie_ids or [])

    out: Dict[int, Dict[str, int]] = {}
    plays = plays_list(pbp_obj)
    used = 0

    # debug tallies
    label_counts = {"EV": 0, "PP": 0, "SH": 0}
    sit_counts: Dict[str, int] = {}

    for p in plays:
        if not isinstance(p, dict) or not _is_sog_like(p):
            continue

        d = p.get("details") or {}
        gid = to_int(d.get("goalieInNetId"))
        if gid is None:
            continue

        hs, aw = _sit_counts(p)
        if gid in home_set:
            lab = _strength(False, hs, aw)  # shooter is AWAY
        elif gid in away_set:
            lab = _strength(True, hs, aw)   # shooter is HOME
        else:
            continue

        out.setdefault(gid, {"EV": 0, "PP": 0, "SH": 0})
        out[gid][lab] += 1

        label_counts[lab] += 1
        sc = p.get("situationCode")
        if isinstance(sc, str) and sc:
            sit_counts[sc] = sit_counts.get(sc, 0) + 1
        used += 1

    dprint("[dbg] goalie split sog-faced used="
           f"{used} goalies={len(out)} "
           f"labels EV={label_counts['EV']} PP={label_counts['PP']} SH={label_counts['SH']} "
           f"sits={dict(sorted(sit_counts.items()))}")
    return out

def aggregate_attempts_from_pbp(pbp_obj):
    """
    Return { player_id: { 'sog': n, 'missed': n, 'blocked': n } }.
    For now we only count SOG (incl. GOAL) from PBP; 'missed'/'blocked' left 0.
    """
    out: Dict[int, Dict[str, int]] = {}
    plays = plays_list(pbp_obj)
    sog_like = 0
    shooters = set()

    for p in plays:
        if not isinstance(p, dict) or not _is_sog_like(p):
            continue
        sog_like += 1

        pid = shooter_id_from_play(p)
        if pid is None:
            continue

        shooters.add(pid)
        d = out.setdefault(pid, {"sog": 0, "missed": 0, "blocked": 0})
        d["sog"] += 1

    dprint(f"[dbg] pbp plays={len(plays)} sog-like={sog_like} shooters={len(shooters)}")
    return out

def compute_splits_from_pbp(pbp_obj, home_id: int, away_id: int, home_abbr: str, away_abbr: str) -> Dict[int, Dict[str, int]]:
    """
    Return { pid: {'EV': e, 'PP': p, 'SH': s} } using SOG-like events.
    Uses eventOwnerTeamId (or team abbrev) to identify the SHOOTING side,
    and situationCode to derive EV/PP/SH from the shooter's perspective.
    """
    sk: Dict[int, Dict[str, int]] = {}
    plays = plays_list(pbp_obj)

    # Debug tallies
    label_counts = {"EV": 0, "PP": 0, "SH": 0}
    sit_counts: Dict[str, int] = {}
    shooters = set()
    used = 0

    for p in plays:
        if not isinstance(p, dict) or not _is_sog_like(p):
            continue

        pid = shooter_id_from_play(p)
        if pid is None:
            continue

        # Determine if the shooter is home or away
        details = p.get("details") or {}
        owner_tid = details.get("eventOwnerTeamId")
        if owner_tid is None:
            team_abbr = (_play_team_abbr(p) or "").upper()
            if team_abbr == home_abbr:
                owner_is_home = True
            elif team_abbr == away_abbr:
                owner_is_home = False
            else:
                owner_is_home = None
        else:
            owner_is_home = (owner_tid == home_id)

        # Strength label from situationCode (e.g., 1541 => 5v4)
        hs, as_ = _sit_counts(p)
        if hs is not None and as_ is not None and owner_is_home is not None:
            diff = (hs - as_) if owner_is_home else (as_ - hs)
            lab = "PP" if diff > 0 else ("SH" if diff < 0 else "EV")
        else:
            lab = "EV"

        sk.setdefault(pid, {"EV": 0, "PP": 0, "SH": 0})
        sk[pid][lab] += 1

        label_counts[lab] += 1
        sc = p.get("situationCode")
        if isinstance(sc, str) and sc:
            sit_counts[sc] = sit_counts.get(sc, 0) + 1

        used += 1
        shooters.add(pid)

    dprint("[dbg] split sog-like used="
           f"{used} shooters={len(shooters)} "
           f"labels EV={label_counts['EV']} PP={label_counts['PP']} SH={label_counts['SH']} "
           f"sits={dict(sorted(sit_counts.items()))}")
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
        # Label from the SHOOTING side
        lab = _strength(t == home_abbr, hs, aw)
        # DEFENDING team is the opposite of the shooter
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
        
def compute_pp_toi_from_shiftcharts(game_id: int, pbp_obj: dict, box: dict,
                                    home_id: int, away_id: int) -> Dict[int, float]:
    """
    Derive per-skater PP TOI in minutes by intersecting team power-play windows (from PBP)
    with each player's shifts (from shiftcharts).
    Returns: { player_id: minutes_float }
    """

    # ---- utilities ----
    def _mmss_to_sec(s: Optional[str]) -> Optional[int]:
        if not s or not isinstance(s, str) or ":" not in s: return None
        try:
            m, ss = s.split(":", 1)
            return int(m) * 60 + int(ss)
        except Exception:
            return None

    def _abs_time(period: Optional[int], mmss: Optional[str]) -> Optional[int]:
        try:
            p = int(period or 0)
        except Exception:
            return None
        t = _mmss_to_sec(mmss)
        if p <= 0 or t is None: return None
        return (p - 1) * 20 * 60 + t  # assume 20:00 periods

    def _sit_counts_from_play(p: dict) -> Tuple[Optional[int], Optional[int]]:
        sc = p.get("situationCode")
        if isinstance(sc, (int, float)): sc = str(sc)
        if isinstance(sc, str) and sc:
            if len(sc) == 4 and sc.isdigit():
                try:
                    _hg, hs, a, _ag = sc
                    return int(hs), int(a)
                except Exception:
                    pass
            if "v" in sc:
                try:
                    L, R = sc.split("v", 1)
                    return int(L), int(R)
                except Exception:
                    pass
        about = p.get("about") or {}
        hs = p.get("homeSkaters", about.get("homeSkaters"))
        a  = p.get("awaySkaters", about.get("awaySkaters"))
        try:
            return (int(hs) if hs is not None else None,
                    int(a)  if a  is not None else None)
        except Exception:
            return None, None

    # goalies set (so we don't award them PP skater minutes)
    home_goalie_ids, away_goalie_ids = _goalie_ids_from_box(box)
    _goalies: set[int] = set(home_goalie_ids) | set(away_goalie_ids)

    pp_toi_by_pid: Dict[int, float] = {}

    # ---- 1) Build PP windows (absolute seconds) from PBP ----
    plays = plays_list(pbp_obj)
    pp_windows_home: List[Tuple[int,int]] = []
    pp_windows_away: List[Tuple[int,int]] = []
    cur_side: Optional[str] = None   # "HOME" / "AWAY" / None
    cur_start: Optional[int] = None

    last_abs = 0
    for p in plays:
        if not isinstance(p, dict):
            continue
        # time
        per = (p.get("periodDescriptor") or {}).get("number") \
              or (p.get("about") or {}).get("period")
        mmss = p.get("timeInPeriod") or (p.get("about") or {}).get("periodTime")
        t_abs = _abs_time(per, mmss)
        if t_abs is None:
            continue
        last_abs = max(last_abs, t_abs)

       # situation → who has advantage? (ignore EN/pulled-goalie; keep classic PP only)
        sc = str(p.get("situationCode") or "")
        hg = hs = as_ = ag = None

        if len(sc) == 4 and sc.isdigit():
            # HgHsAsAg (e.g., "1551"=5v5, "1541"=5v4, "0651"=6v5 EN)
            hg, hs, as_, ag = map(int, sc)
        else:
            # fallback: you already have this helper
            hs, as_ = _sit_counts_from_play(p)

        allowed = {(5, 4), (5, 3), (4, 3)}  # “classic only”

        if hg is not None and ag is not None and (hg != 1 or ag != 1):
            # any empty-net / pulled-goalie → not a PP window
            side = None
        elif hs is None or as_ is None or hs == as_:
            side = None
        elif (hs, as_) in allowed:
            side = "HOME"
        elif (as_, hs) in allowed:
            side = "AWAY"
        else:
            side = None

        # Ignore delayed-penalty 6-on-5 segments in PP windows
        sc = p.get("situationCode")
        if isinstance(sc, (int, float)):
            sc = str(sc)
        if isinstance(sc, str) and sc.startswith("06"):
            continue

        # --- removed stale on_ice_* gating here ---

        # state machine for PP window segmentation
        if cur_side is None and side is not None:
            cur_side = side
            cur_start = t_abs
        elif cur_side is not None and side != cur_side:
            # close current window
            if cur_start is not None and t_abs > cur_start:
                if cur_side == "HOME":
                    pp_windows_home.append((cur_start, t_abs))
                else:
                    pp_windows_away.append((cur_start, t_abs))
            cur_side = side
            cur_start = t_abs if side is not None else None

    # close any trailing window at last_abs
    if cur_side and cur_start is not None and last_abs > cur_start:
        if cur_side == "HOME":
            pp_windows_home.append((cur_start, last_abs))
        else:
            pp_windows_away.append((cur_start, last_abs))

    # ---- 2) Fetch & parse shiftcharts → shifts per player (absolute seconds) ----
    shift_url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/shiftcharts"
    shift = fetch_json(shift_url)

    # map pid -> list[(start_abs, end_abs)]
    shifts: Dict[int, List[Tuple[int,int]]] = {}

    def _maybe_add_shift(node: dict):
        pid = to_int(node.get("playerId") or node.get("id"))
        if not pid:
            return
        per = node.get("period") or node.get("periodNumber") \
              or (node.get("periodDescriptor") or {}).get("number")
        st = node.get("startTime") or node.get("start") or node.get("begin")
        en = node.get("endTime")   or node.get("end")   or node.get("finish")
        dur = node.get("duration") or node.get("shiftDuration")
        t0 = _abs_time(per, st)
        t1 = _abs_time(per, en) if en else None
        if t1 is None and t0 is not None and isinstance(dur, str):
            ds = _mmss_to_sec(dur)
            if ds is not None:
                t1 = t0 + ds
        if t0 is not None and t1 is not None and t1 > t0:
            shifts.setdefault(pid, []).append((t0, t1))

    def _walk(obj):
        if isinstance(obj, dict):
            if ("playerId" in obj) and any(k in obj for k in ("startTime","endTime","duration","shiftDuration","start","end")):
                _maybe_add_shift(obj)
            for v in obj.values():
                _walk(v)
        elif isinstance(obj, list):
            for it in obj:
                _walk(it)

    _walk(shift)

    # ---- 3) Build pid→team map from box (to choose HOME/AWAY windows) ----
    pid_team: Dict[int, int] = {}
    def _add_pid_team(section: str, tid: int):
        pbg = (box.get("playerByGameStats") or {}).get(section) or {}
        for key in ("forwards","defense","goalies"):
            for p in pbg.get(key, []) or []:
                pid = to_int(p.get("playerId"))
                if pid:
                    pid_team[pid] = tid
        team = box.get(section) or {}
        players = team.get("players")
        if isinstance(players, dict):
            for pid_s, pdata in players.items():
                m = re.search(r"\d+", str(pid_s))
                if m:
                    pid_team.setdefault(int(m.group()), tid)
        for key in ("skaters","forwards","defense","goalies"):
            for p in team.get(key, []) or []:
                pid = to_int(p.get("playerId") or p.get("id"))
                if pid:
                    pid_team.setdefault(pid, tid)

    _add_pid_team("homeTeam", home_id)
    _add_pid_team("awayTeam", away_id)

    # ---- 4) Intersect shifts with windows → per-player PP seconds ----
    def _sum_intersection(intervals: List[Tuple[int,int]], windows: List[Tuple[int,int]]) -> int:
        if not intervals or not windows: return 0
        total = 0
        wi = 0
        windows = sorted(windows)
        for (a0, a1) in sorted(intervals):
            while wi < len(windows) and windows[wi][1] <= a0:
                wi += 1
            wj = wi
            while wj < len(windows) and windows[wj][0] < a1:
                b0, b1 = windows[wj]
                overlap = max(0, min(a1, b1) - max(a0, b0))
                total += overlap
                if b1 <= a1:
                    wj += 1
                else:
                    break
        return total

    pid_pp_minutes: Dict[int, float] = {}
    for pid, ivals in shifts.items():
        if pid in _goalies:
            continue  # don't award PP skater minutes to goalies
        tid = pid_team.get(pid)
        if not tid:
            continue
        windows = pp_windows_home if tid == home_id else pp_windows_away
        secs = _sum_intersection(ivals, windows)
        if secs > 0:
            pid_pp_minutes[pid] = secs / 60.0

    # quick debug summary
    if DEBUG_PBP:
        home_total = sum(max(0, e - s) for s, e in pp_windows_home) / 60.0
        away_total = sum(max(0, e - s) for s, e in pp_windows_away) / 60.0
        dprint(f"[dbg] PP windows (derived): HOME={home_total:.1f} min  AWAY={away_total:.1f} min  "
               f"players_with_pp={len(pid_pp_minutes)}")

    return pid_pp_minutes

def _abs_sec_from_play(p) -> Optional[int]:
    """Absolute seconds from game start using period + timeInPeriod."""
    per = (p.get("periodDescriptor") or {}).get("number") \
          or (p.get("about") or {}).get("period")
    per = to_int(per)
    t = p.get("timeInPeriod") or (p.get("about") or {}).get("periodTime")
    sec = _to_secs_mmss(t)
    if per is None or sec is None:
        return None
    return (per - 1) * 20 * 60 + sec

def _pp_windows_from_pbp_for_stats(pbp, home_id: int, away_id: int) -> list[dict]:
    """
    Build power-play windows from PBP using situationCode.
    Returns [{side:'HOME'|'AWAY', start:abs_sec, end:abs_sec}, ...]
    """
    wins: list[dict] = []
    plays = plays_list(pbp)

    cur_side: Optional[str] = None  # 'HOME' | 'AWAY' | None
    cur_start: Optional[int] = None

    for p in plays:
        ts = _abs_sec_from_play(p)
        if ts is None:
            continue
        hs, as_ = _sit_counts(p)
        side = None
        if hs is not None and as_ is not None:
            if hs > as_:
                side = "HOME"
            elif as_ > hs:
                side = "AWAY"

        # open a new window
        if cur_side is None and side is not None:
            cur_side, cur_start = side, ts
            continue

        # close/switch window
        if cur_side is not None and side != cur_side:
            if cur_start is not None and ts > cur_start:
                wins.append({"side": cur_side, "start": cur_start, "end": ts})
            cur_start = ts if side is not None else None
            cur_side = side

    # close trailing window to last timestamp (or 60:00 if unknown)
    last_ts = None
    for p in reversed(plays):
        ts = _abs_sec_from_play(p)
        if ts is not None:
            last_ts = ts
            break
    if last_ts is None:
        last_ts = 60 * 60

    if cur_side is not None and cur_start is not None and last_ts > cur_start:
        wins.append({"side": cur_side, "start": cur_start, "end": last_ts})

    # tiny cleanup: drop sub-second or zero windows
    return [w for w in wins if w["end"] > w["start"]]

def compute_pp_toi_from_statsapi_shifts(game_id: int, pbp, home_id: int, away_id: int) -> Dict[int, float]:
    """
    Fallback: intersect StatsAPI shift intervals with PBP-derived PP windows.
    Returns {player_id: minutes_on_PP}.
    """
    wins = _pp_windows_from_pbp_for_stats(pbp, home_id, away_id)
    if not wins:
        dprint("[dbg] statsapi PP: no PP windows found")
        return {}

    # Group windows by advantaged team id
    by_tid: Dict[int, list[tuple[int,int]]] = {home_id: [], away_id: []}
    for w in wins:
        tid = home_id if w["side"] == "HOME" else away_id
        by_tid.setdefault(tid, []).append((w["start"], w["end"]))

    # Fetch normalized shifts
    shifts = fetch_statsapi_shifts(game_id)
    if not shifts:
        return {}

    out: Dict[int, float] = {}

    # fast lookup: nothing to do if team has no PP windows
    valid_tids = {tid for tid, arr in by_tid.items() if arr}

    for s in shifts:
        pid = s.get("pid"); tid = s.get("tid")
        st  = s.get("start"); en = s.get("end")
        if not (pid and tid and st is not None and en is not None):
            continue
        if tid not in valid_tids:
            continue

        total_overlap = 0
        for ws, we in by_tid[tid]:
            a = max(st, ws); b = min(en, we)
            if b > a:
                total_overlap += (b - a)

        if total_overlap > 0:
            out[pid] = out.get(pid, 0.0) + (total_overlap / 60.0)

    if DEBUG_PBP:
        hmin = sum(max(0, e - s) for s, e in by_tid.get(home_id, [])) / 60.0
        amin = sum(max(0, e - s) for s, e in by_tid.get(away_id, [])) / 60.0
        dprint(f"[dbg] PP windows (derived): HOME={hmin:.1f} min  AWAY={amin:.1f} min  players_with_pp={len(out)}")

    return {pid: round(m, 2) for pid, m in out.items() if m > 0}


# ───────────────────────── main ingest ─────────────────────────
def ingest_game(game_id: int) -> None:
    box_url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"
    pbp_url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"

    # Fetch payloads
    box = fetch_json(box_url)
    try:
        pbp = fetch_json(pbp_url)
        if DEBUG_PBP:
            _dbg_probe_pbp(pbp)
    except Exception:
        pbp = {}

        # After: pbp = fetch_json(pbp_url) and optional _dbg_probe_pbp(pbp)
    pp_windows = build_pp_windows_from_pbp(pbp)
    if DEBUG_PBP:
        home_pp_sec = sum(e - s for side, s, e in pp_windows if side == "HOME")
        away_pp_sec = sum(e - s for side, s, e in pp_windows if side == "AWAY")

    # --- quick probe of first 2 SOG-like plays (optional debug) ---
    if DEBUG_PBP:
        try:
            shown = 0
            for _p in plays_list(pbp):
                if isinstance(_p, dict) and _is_sog_like(_p):
                    shown += 1
                    print(f"[probe] shot-like #{shown}  event_type={event_type(_p)}")
                    print("  top keys:", sorted(list(_p.keys())))
                    det = _p.get("details") or {}
                    print("  details keys:", sorted(list(det.keys())))
                    for k in ("playerId", "shooterId", "shootingPlayerId", "scoringPlayerId"):
                        if k in det:
                            print(f"  details.{k} =", det[k])
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

    # Game date and status
    start_iso = box.get("startTimeUTC") or box.get("gameDate") or ""
    game_date = iso_to_et_date(start_iso) if start_iso else None

    state_raw = (box.get("gameState")
                 or box.get("gameScheduleState")
                 or (box.get("game") or {}).get("gameState")
                 or "")
    status = map_api_state_to_db(state_raw)

    # ───────────── PBP-derived aggregations (safe if PBP is empty) ─────────────
    attempts: Dict[int, Dict[str, int]] = aggregate_attempts_from_pbp(pbp)               # {pid: {sog, missed, blocked}}
    sk_splits: Dict[int, Dict[str, int]] = compute_splits_from_pbp(                      # {pid: EV/PP/SH}
        pbp, home_id, away_id, home_abbr, away_abbr
    )
    _team_sf = compute_team_sf_splits_from_pbp(                                          # unused aggregate
        pbp, home_abbr, away_abbr, home_id, away_id
    )
    # Derive PP TOI from shift charts (fallback when box has none)
    pp_toi_by_pid: Dict[int, float] = compute_pp_toi_from_statsapi_shifts(game_id, pbp, home_id, away_id)
    if DEBUG_PBP:
        dprint(f"[dbg] shiftchart PP TOI recovered for {len(pp_toi_by_pid)} skaters")

        # Build box_pids without calling iter_skaters (avoid def-order scoping issues)
        def _collect_box_pids(side_key: str) -> set[int]:
            pids: set[int] = set()
            pbg = (box.get("playerByGameStats") or {}).get(side_key) or {}
            # primary: forwards/defense under playerByGameStats
            for key in ("forwards", "defense"):
                for p in pbg.get(key, []) or []:
                    pid = to_int(p.get("playerId"))
                    if pid:
                        pids.add(pid)
            if pids:
                return pids  # found structured list

            # fallbacks if playerByGameStats missing/empty
            team = box.get(side_key) or {}
            players = team.get("players")
            if isinstance(players, dict):
                for pid_s, pdata in players.items():
                    m = re.search(r"\d+", str(pid_s))
                    if m:
                        pids.add(int(m.group()))
            for key in ("skaters", "forwards", "defense"):
                for p in team.get(key, []) or []:
                    pid = to_int(p.get("playerId") or p.get("id"))
                    if pid:
                        pids.add(pid)
            return pids

    home_goalie_ids, away_goalie_ids = _goalie_ids_from_box(box)
    goalie_splits: Dict[int, Dict[str, int]] = compute_goalie_splits_from_pbp(
        pbp, list(home_goalie_ids), list(away_goalie_ids)
    )
    if DEBUG_PBP:
        print(f"[dbg] attempts has {sum(1 for v in attempts.values() if (v or {}).get('sog'))} shooters")
        print(f"[dbg] sk_splits has {len(sk_splits)} shooters")

    # Build name map (prefer playerByGameStats)
    name_by_pid: Dict[int, str] = {}

    def add_names_from_pbg(side_key: str) -> None:
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

    # Fallback name collection (rare)
    def add_names_from_players_dict(section: str) -> None:
        team = box.get(section) or {}
        players = team.get("players")
        if isinstance(players, dict):
            for pid_s, pdata in players.items():
                m = re.search(r"\d+", str(pid_s))
                if not m:
                    continue
                pid = int(m.group())
                fn = ((pdata.get("firstName") or {}).get("default") or pdata.get("firstName") or "").strip()
                ln = ((pdata.get("lastName")  or {}).get("default") or pdata.get("lastName")  or "").strip()
                nm = (f"{fn} {ln}").strip()
                if nm:
                    name_by_pid.setdefault(pid, nm)

    add_names_from_players_dict("homeTeam")
    add_names_from_players_dict("awayTeam")

    # Iterators (prefer playerByGameStats)
    def iter_skaters(section: str) -> Iterable[Tuple[int, dict, str]]:
        pbg = (box.get("playerByGameStats") or {}).get(section) or {}
        had_any = False
        for key in ("forwards", "defense"):
            for p in pbg.get(key, []) or []:
                pid = to_int(p.get("playerId"))
                if not pid:
                    continue
                had_any = True
                yield pid, p, section
        if had_any:
            return

        # Fallback: team.<players>/skaters lists — avoid duplicates
        team = box.get(section) or {}
        seen: set[int] = set()

        players = team.get("players")
        if isinstance(players, dict):
            for pid_s, pdata in players.items():
                m = re.search(r"\d+", str(pid_s))
                if not m:
                    continue
                pid = int(m.group())
                pos = (pdata.get("positionCode") or pdata.get("position") or "").upper()
                if pos == "G":
                    continue
                if pid in seen:
                    continue
                seen.add(pid)
                yield pid, pdata, section

        for key in ("skaters", "forwards", "defense"):
            for p in team.get(key, []) or []:
                pid = to_int(p.get("playerId") or p.get("id"))
                if not pid or pid in seen:
                    continue
                seen.add(pid)
                yield pid, p, section

    def iter_goalies(section: str) -> Iterable[Tuple[int, dict, str]]:
        """Yield (player_id, node, section) for goalies on `section` ('homeTeam'/'awayTeam')."""
        pbg = (box.get("playerByGameStats") or {}).get(section) or {}
        had_any = False

        # Preferred: playerByGameStats.goalies
        for p in pbg.get("goalies", []) or []:
            pid = to_int(p.get("playerId"))
            if not pid:
                continue
            had_any = True
            yield pid, p, section

        # If we had any from PBG, stop—don’t fall through and risk duplicates
        if had_any:
            return

        # Fallback: team dicts/arrays, but dedupe across sources
        team = box.get(section) or {}
        seen: set[int] = set()

        # 1) team.players{} (filter to position G)
        players = team.get("players")
        if isinstance(players, dict):
            for pid_s, pdata in players.items():
                m = re.search(r"\d+", str(pid_s))
                if not m:
                    continue
                pid = int(m.group())
                pos = (pdata.get("positionCode") or pdata.get("position") or "").upper()
                if pos != "G":
                    continue
                if pid in seen:
                    continue
                seen.add(pid)
                yield pid, pdata, section

        # 2) team.goalies[] array (some payloads)
        for p in team.get("goalies", []) or []:
            pid = to_int(p.get("playerId") or p.get("id"))
            if not pid or pid in seen:
                continue
            seen.add(pid)
            yield pid, p, section
       
    # ───────────── DB upserts ─────────────
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
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (game_id) DO UPDATE
                  SET game_date     = EXCLUDED.game_date,
                      home_team_id  = EXCLUDED.home_team_id,
                      away_team_id  = EXCLUDED.away_team_id,
                      status        = EXCLUDED.status;
            """, (game_id, game_date, home_id, away_id, status))

            def pos_code(raw: dict, default: str) -> str:
                code = (raw.get("positionCode") or raw.get("position") or "").upper()
                if code in ("G", "D", "F"): return code
                if code in ("LW", "RW", "C"): return "F"
                return default

            # ───── Skaters
            did_log = False
            sk_batch: List[tuple] = []
            pp_seen, pp_total, recovered_pp = 0, 0, 0

            for pid, raw, sect in list(iter_skaters("homeTeam")) + list(iter_skaters("awayTeam")):
                team_id = home_id if sect == "homeTeam" else away_id
                opp_id  = away_id if sect == "homeTeam" else home_id
                is_home = (sect == "homeTeam")
                nm = name_by_pid.get(pid) or f"Player {pid}"

                cur.execute("""
                    INSERT INTO nhl.players (player_id, full_name, current_team_id, position, status)
                    VALUES (%s, %s, %s, %s, 'active')
                    ON CONFLICT (player_id) DO UPDATE
                      SET full_name       = EXCLUDED.full_name,
                          current_team_id = COALESCE(EXCLUDED.current_team_id, nhl.players.current_team_id),
                          position        = EXCLUDED.position,
                          status          = 'active';
                """, (pid, nm, team_id, pos_code(raw, "F")))

                sog_box = to_int(raw.get("sog") or raw.get("shotsOnGoal") or raw.get("shots"))

                agg = attempts.get(pid)
                if agg:
                    sog_pbp = to_int(agg.get("sog"))
                    miss    = to_int(agg.get("missed"))
                    blk     = to_int(agg.get("blocked"))
                    attempts_total = (sog_pbp or 0) + (miss or 0) + (blk or 0)
                    sog = sog_box if sog_box is not None else sog_pbp
                else:
                    sog = sog_box
                    attempts_total = None

                toi = parse_mmss_to_minutes(raw.get("toi") or raw.get("timeOnIce"))

                # PP TOI (robust finder + shift fallback + zero-fill when windows exist)
                pp_toi = get_pp_toi_minutes_from_box(raw)
                if pp_toi is None:
                    pp_toi = pp_toi_by_pid.get(pid)  # from shift charts
                if pp_toi is None and pp_toi_by_pid:
                    # We successfully computed PP windows this game; player simply never overlapped one
                    pp_toi = 0.0


                pp_total += 1
                if pp_toi is not None:
                    pp_seen += 1

                # one-time PP key probe (first skater only when DEBUG_PBP)
                if DEBUG_PBP and not did_log:
                    stats_node = ((raw.get("stats") or {}).get("skaterStats") or {})
                    dprint("[dbg] PP keys sample:", {
                        "ppToi": raw.get("ppToi"),
                        "powerPlayToi": raw.get("powerPlayToi"),
                        "ppTimeOnIce": raw.get("ppTimeOnIce"),
                        "powerPlayTimeOnIce": raw.get("powerPlayTimeOnIce"),
                        "stats.skaterStats.ppToi": stats_node.get("ppToi"),
                        "stats.skaterStats.ppTimeOnIce": stats_node.get("ppTimeOnIce"),
                        "stats.skaterStats.powerPlayTimeOnIce": stats_node.get("powerPlayTimeOnIce"),
                    })

                # Split counts from PBP
                spl = sk_splits.get(pid, {"EV": 0, "PP": 0, "SH": 0})
                ev_sog = int(spl.get("EV", 0)); pp_sog = int(spl.get("PP", 0)); sh_sog = int(spl.get("SH", 0))

                # your existing one-time debug
                if not did_log and DEBUG_PBP:
                    print(f"[dbg] first skater pid={pid} sog_pbp={attempts.get(pid,{}).get('sog')} "
                          f"box_sog={sog_box} splits={sk_splits.get(pid)}")
                    did_log = True

                sk_batch.append((
                    pid, game_id, team_id, opp_id, is_home, game_date,
                    sog, attempts_total, toi, pp_toi, ev_sog, pp_sog, sh_sog
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

            # ───── Goalies
            home_goalie_rows = list(iter_goalies("homeTeam"))
            away_goalie_rows = list(iter_goalies("awayTeam"))
            if DEBUG_PBP:
                print(f"[dbg] iter_goalies: home={len(home_goalie_rows)} away={len(away_goalie_rows)} "
                    f"idsH={[pid for pid,_,_ in home_goalie_rows]} idsA={[pid for pid,_,_ in away_goalie_rows]}")

            gl_batch: List[tuple] = []
            for pid, raw, sect in home_goalie_rows + away_goalie_rows:
                team_id = home_id if sect == "homeTeam" else away_id
                opp_id  = away_id if sect == "homeTeam" else home_id
                is_home = (sect == "homeTeam")
                nm = name_by_pid.get(pid) or f"Player {pid}"

                cur.execute("""
                    INSERT INTO nhl.players (player_id, full_name, current_team_id, position, status)
                    VALUES (%s, %s, %s, %s, 'active')
                    ON CONFLICT (player_id) DO UPDATE SET
                    full_name       = EXCLUDED.full_name,
                    current_team_id = COALESCE(EXCLUDED.current_team_id, nhl.players.current_team_id),
                    position        = EXCLUDED.position,
                    status          = 'active';
                """, (pid, nm, team_id, "G"))

                toi = parse_mmss_to_minutes(raw.get("toi") or raw.get("timeOnIce"))

                # splits from PBP (may be None)
                ev_sf = pp_sf = sh_sf = None
                gs = goalie_splits.get(pid)
                if gs:
                    ev_sf = to_int(gs.get("EV")); pp_sf = to_int(gs.get("PP")); sh_sf = to_int(gs.get("SH"))

                shots_faced   = to_int(raw.get("shotsAgainst") or raw.get("shotsFaced"))
                saves         = to_int(raw.get("saves"))
                goals_allowed = to_int(raw.get("goalsAgainst"))
                start_flag    = bool(raw.get("starter") or raw.get("isStarter") or False)
                pulled_flag   = bool(raw.get("pulled") or False)

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
