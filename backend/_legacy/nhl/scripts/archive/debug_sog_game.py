#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
import requests

API_WEB_PBP   = "https://api-web.nhle.com/v1/gamecenter/{gid}/play-by-play"
API_WEB_BOX   = "https://api-web.nhle.com/v1/gamecenter/{gid}/boxscore"
STATSAPI_FEED = "https://statsapi.web.nhl.com/api/v1/game/{gid}/feed/live"

def fetch_json(url: str, timeout=12):
    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[warn] fetch failed {url} -> {e}")
        return None

def plays_list(pbp_obj):
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
    for path in (("typeDescKey",), ("details","typeDescKey"), ("result","eventTypeId")):
        cur = play
        ok = True
        for k in path:
            if not isinstance(cur, dict) or k not in cur:
                ok = False; break
            cur = cur[k]
        if ok and isinstance(cur, str) and cur.strip():
            return cur.strip().upper()
    for k in ("typeCode","eventCode","eventTypeId"):
        v = play.get(k)
        if isinstance(v, int): return f"CODE_{v}"
        if isinstance(v, str) and v.strip(): return v.strip().upper()
    return ""

def _strength(shoot_home: bool, hs: int | None, as_: int | None) -> str:
    # Unknown counts → best guess is EV
    if hs is None or as_ is None:
        return "EV"
    if hs == as_:
        return "EV"
    # If the shooting side has more skaters => PP, else SH
    return "PP" if (shoot_home and hs > as_) or ((not shoot_home) and as_ > hs) else "SH"

def is_sog_like(play: dict) -> bool:
    d = play.get("details") or {}
    if d.get("isGoal") is True: return True
    if d.get("shotOnGoal") is True: return True
    et = event_type(play)
    return et in ("SHOT","SHOT-ON-GOAL","SHOT_ON_GOAL","GOAL")

def shooter_id(play: dict):
    d = play.get("details") or {}
    pid = d.get("shootingPlayerId") or d.get("playerId")
    if pid is not None:
        try: return int(pid)
        except: return None
    for pl in play.get("players",[]) or []:
        if (pl.get("playerType") or "").lower() in ("shooter","scorer"):
            pid = (pl.get("player") or {}).get("id") or pl.get("playerId")
            try: return int(pid)
            except: return None
    return None

def play_team_side(play: dict, home_id: int, away_id: int, home_abbr: str, away_abbr: str):
    d = play.get("details") or {}
    owner_id = d.get("eventOwnerTeamId")
    try: owner_id = int(owner_id) if owner_id is not None else None
    except: owner_id = None
    if owner_id == home_id: return "HOME"
    if owner_id == away_id: return "AWAY"
    ab = d.get("teamAbbrev")
    if isinstance(ab, str):
        ab = ab.strip().upper()
        if ab == home_abbr: return "HOME"
        if ab == away_abbr: return "AWAY"
    t = play.get("team") or {}
    tri = str(t.get("triCode") or t.get("abbrev") or "").upper()
    if tri == home_abbr: return "HOME"
    if tri == away_abbr: return "AWAY"
    return None

def _sit_counts(play: dict):
    """
    Return (home_skaters, away_skaters) if we can infer counts.
    Priority:
      1) StatsAPI explicit strength code
      2) api-web 'situationCode' like '5v4' or '1551'
    """
    # 1) StatsAPI explicit strength (most reliable)
    # e.g. play['result']['strength']['code'] in {'EVEN','PP','SH','PPG','SHG'}
    res = play.get("result") or {}
    str_node = res.get("strength") or {}
    code = (str_node.get("code") or "").upper()
    if code:
        if code == "EVEN":
            return 5, 5
        # For PP/SH we don't know exact counts, but treat as 5v4 (common case)
        if code in {"PP", "PPG"}:
            # shooting team has more skaters; actual EV/PP decision happens later
            return 5, 4
        if code in {"SH", "SHG"}:
            return 4, 5
        # fall through if unknown

    # 2) api-web: situationCode like "5v4"
    d = play.get("details") or {}
    sc = d.get("situationCode")
    if isinstance(sc, str) and "v" in sc:
        try:
            a, b = sc.split("v", 1)
            return int(a), int(b)
        except Exception:
            pass

    # 2b) api-web: situationCode like "1551" (heuristic: middle digits are home, last-but-one is away)
    if isinstance(sc, str) and len(sc) == 4 and sc.isdigit():
        # Observed formats suggest positions [1] and [2] are the skater counts
        # e.g., "1551" => home=5, away=5 ; "1541" => home=5, away=4 ; "1451" => home=4, away=5
        try:
            home = int(sc[1])
            away = int(sc[2])
            return home, away
        except Exception:
            pass

    return None, None

def strength(shoot_home: bool, hs: int|None, as_: int|None) -> str:
    if hs is None or as_ is None: return "EV"
    if hs == as_: return "EV"
    return "PP" if (shoot_home and hs > as_) or ((not shoot_home) and as_ > hs) else "SH"

def compute_splits(pbp_obj, home_id: int, away_id: int, home_abbr: str, away_abbr: str):
    out = {}
    used = 0
    for p in plays_list(pbp_obj):
        if not is_sog_like(p): continue
        pid = shooter_id(p)
        side = play_team_side(p, home_id, away_id, home_abbr, away_abbr)
        if pid is None or side is None: continue
        hs, aw = _sit_counts(p)
        lab = strength(side=="HOME", hs, aw)
        d = out.setdefault(pid, {"EV":0,"PP":0,"SH":0})
        d[lab] += 1; used += 1
    return out, used

def _sog_strength_debug(plays):
    """
    Inspect only SOG-like plays and print how strength is represented.
    """
    from collections import Counter

    def sog_like(p):
        d = p.get("details") or {}
        if d.get("isGoal") is True or d.get("shotOnGoal") is True:
            return True
        et = (d.get("typeDescKey") or p.get("typeDescKey") or "").upper()
        if not et:
            et = ((p.get("result") or {}).get("eventTypeId") or "").upper()
        return et in {"SHOT", "SHOT-ON-GOAL", "SHOT_ON_GOAL", "GOAL"}

    c_situation = Counter()
    c_strength_code = Counter()
    c_strength_name = Counter()
    c_counts = Counter()

    for p in plays:
        if not sog_like(p):
            continue
        d = p.get("details") or {}
        r = p.get("result") or {}
        s = r.get("strength") or {}

        sc = d.get("situationCode")
        c_situation[str(sc)] += 1

        c_strength_code[str(s.get("code"))] += 1
        c_strength_name[str(s.get("name"))] += 1

        # What our current heuristic reads:
        hs, as_ = _sit_counts(p)
        c_counts[f"{hs}v{as_}"] += 1

    print("\n[debug] SOG-like strength summary:")
    print("  details.situationCode -> count:", dict(c_situation.most_common()))
    print("  result.strength.code  -> count:", dict(c_strength_code.most_common()))
    print("  result.strength.name  -> count:", dict(c_strength_name.most_common()))
    print("  _sit_counts()         -> count:", dict(c_counts.most_common()))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--gid", required=True, type=int, help="NHL game id (e.g., 2023010067)")
    ap.add_argument("--save-json", action="store_true", help="Write pbp.json and box.json beside script")
    args = ap.parse_args()

    gid = args.gid
    print(f"== Game {gid} ==")

    box = fetch_json(API_WEB_BOX.format(gid=gid))
    if not isinstance(box, dict):
        print("boxscore fetch failed"); return

    home = (box.get("homeTeam") or {})
    away = (box.get("awayTeam") or {})
    home_abbr = (home.get("abbrev") or home.get("teamAbbrev") or "").upper()
    away_abbr = (away.get("abbrev") or away.get("teamAbbrev") or "").upper()
    home_id = home.get("id"); away_id = away.get("id")

    print(f"home={home_abbr}({home_id}) away={away_abbr}({away_id})")

    src = "api-web"
    pbp = fetch_json(API_WEB_PBP.format(gid=gid))
    if not pbp:
        src = "statsapi"
        pbp = fetch_json(STATSAPI_FEED.format(gid=gid))
    plays = plays_list(pbp)
    print(f"pbp source={src} plays={len(plays)}")
    _sog_strength_debug(plays)

    splits, used = compute_splits(pbp, int(home_id), int(away_id), home_abbr, away_abbr)
    shooters = sorted(splits.items(), key=lambda kv: -(kv[1]["EV"]+kv[1]["PP"]+kv[1]["SH"]))

    print(f"shooters={len(shooters)} sog_like_used={used}")
    print("player_id,EV,PP,SH,total")
    for pid, d in shooters:
        ev,pp,sh = d["EV"], d["PP"], d["SH"]
        print(f"{pid},{ev},{pp},{sh},{ev+pp+sh}")

    if args.save_json:
        with open("pbp.json","w") as f: json.dump(pbp, f)
        with open("box.json","w") as f: json.dump(box, f)
        print("wrote pbp.json, box.json")

if __name__ == "__main__":
    main()
