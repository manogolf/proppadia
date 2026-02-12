#!/usr/bin/env python3
import sys, requests, json

API_WEB_PBP   = "https://api-web.nhle.com/v1/gamecenter/{gid}/play-by-play"
API_WEB_BOX   = "https://api-web.nhle.com/v1/gamecenter/{gid}/boxscore"
STATSAPI_FEED = "https://statsapi.web.nhl.com/api/v1/game/{gid}/feed/live"

def j(url):
    r = requests.get(url, timeout=12, headers={"User-Agent":"curl/8"})
    r.raise_for_status()
    return r.json()

def plays_list(obj):
    if isinstance(obj, dict):
        pby = obj.get("playByPlay") or {}
        if isinstance(pby.get("allPlays"), list):
            return pby["allPlays"]
        if isinstance(pby.get("plays"), list):
            return pby["plays"]
        live = obj.get("liveData") or {}
        pl = live.get("plays") or {}
        if isinstance(pl.get("allPlays"), list):
            return pl["allPlays"]
    return []

def run(gid: str):
    box   = j(API_WEB_BOX.format(gid=gid))
    stats = j(STATSAPI_FEED.format(gid=gid))

    home = (box.get("homeTeam") or {})
    away = (box.get("awayTeam") or {})
    H = (home.get("abbrev") or home.get("teamAbbrev") or "").upper()
    A = (away.get("abbrev") or away.get("teamAbbrev") or "").upper()

    ps = plays_list(stats)
    web = j(API_WEB_PBP.format(gid=gid))
    pw = plays_list(web)

    # quick strength availability probe (StatsAPI)
    have_strength = 0
    for p in ps:
        r = p.get("result") or {}
        et = (r.get("eventTypeId") or "").upper()
        if et in ("SHOT","GOAL") and (r.get("strength") or {}).get("code"):
            have_strength += 1
            break

    print(f"== Game {gid} ==")
    print(f"home={H} away={A}")
    print(f"api-web plays={len(pw)} statsapi plays={len(ps)} stats_has_strength_any={bool(have_strength)}")

    # per-player EV/PP/SH from StatsAPI (preferred)
    splits = {}
    used = 0
    for p in ps:
        r = p.get("result") or {}
        et = (r.get("eventTypeId") or "").upper()
        if et not in ("SHOT","GOAL"):
            continue
        pid = None
        for pl in (p.get("players") or []):
            if (pl.get("playerType") or "").lower() in ("shooter","scorer"):
                pid = (pl.get("player") or {}).get("id") or pl.get("playerId")
                break
        if not pid:
            continue
        t = p.get("team") or {}
        tri = (t.get("triCode") or t.get("abbrev") or "").upper()
        if tri not in (H, A):
            continue
        code = ((r.get("strength") or {}).get("code") or "").upper()
        if code in {"EVEN","EV"}: lab = "EV"
        elif code in {"PP","PPG"}: lab = "PP"
        elif code in {"SH","SHG"}: lab = "SH"
        else: lab = "EV"
        d = splits.setdefault(int(pid), {"EV":0,"PP":0,"SH":0})
        d[lab] += 1
        used += 1

    print("player_id,EV,PP,SH,total")
    for pid, d in sorted(splits.items()):
        print(f"{pid},{d['EV']},{d['PP']},{d['SH']},{d['EV']+d['PP']+d['SH']}")

if __name__ == "__main__":
    if len(sys.argv) != 2: 
        print("usage: python nhl_dbg_strength.py <GAME_ID>"); sys.exit(2)
    run(sys.argv[1])
