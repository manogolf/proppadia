#!/usr/bin/env python3
from __future__ import annotations
import sys, json, requests

API_PBP = "https://api-web.nhle.com/v1/gamecenter/{gid}/play-by-play"
S = requests.Session(); S.headers.update({"User-Agent":"proppadia-schema-check"})

def inspect(gid: int):
    r = S.get(API_PBP.format(gid=gid), timeout=20)
    r.raise_for_status()
    j = r.json() or {}
    plays = list(j.get("plays") or [])
    n = len(plays)
    has_counts = sum(1 for ev in plays if ev.get("homeTeamOnIceCount") is not None and ev.get("awayTeamOnIceCount") is not None)
    pen_like = [ev for ev in plays if "penal" in (ev.get("typeDescKey","")+ev.get("typeDesc","")).lower()]
    pen_team_keys = 0
    for ev in pen_like:
        d = ev.get("details") or {}
        # anything that could identify the penalized team?
        cand = [d.get(k) for k in (
            "committedByTeamAbbrev","againstTeamAbbrev","offendingTeamAbbrev","eventOwnerTeamAbbrev",
            "teamAbbrev","byTeamAbbrev"
        )]
        if any(isinstance(x,str) and x for x in cand):
            pen_team_keys += 1
    goals = [ev for ev in plays if "goal" in (ev.get("typeDescKey","")+ev.get("typeDesc","")).lower()]
    goal_strengths = sum(1 for ev in goals if ((ev.get("details") or {}).get("strength") or ev.get("homeTeamDefendingStrength") or ev.get("homeTeamStrength")))
    print(json.dumps({
        "gid": gid, "plays": n,
        "counts_present": has_counts,
        "penalties": len(pen_like),
        "penalties_with_team_keys": pen_team_keys,
        "goals": len(goals),
        "goals_with_strength": goal_strengths,
    }, indent=2))

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: python nhl_inspect_pbp_schema.py <gameId> [<gameId> ...]")
        sys.exit(2)
    for arg in sys.argv[1:]:
        inspect(int(arg))
