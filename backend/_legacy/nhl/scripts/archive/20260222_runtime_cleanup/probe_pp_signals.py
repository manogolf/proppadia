#!/usr/bin/env python3
# backend/nhl/scripts/probe_pp_signals.py
from __future__ import annotations
import argparse, json
from typing import Dict, Any, List, Tuple, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_PBP    = "https://api-web.nhle.com/v1/gamecenter/{gid}/play-by-play"
API_SHIFTS = "https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId={gid}"

def _session() -> requests.Session:
    retry = Retry(total=4, connect=4, read=4, backoff_factor=0.3,
                  status_forcelist=[429,500,502,503,504],
                  allowed_methods=frozenset({"GET"}), raise_on_status=False)
    s = requests.Session()
    s.headers.update({"User-Agent": "proppadia-pp-probe"})
    s.mount("https://", HTTPAdapter(max_retries=retry))
    s.mount("http://", HTTPAdapter(max_retries=retry))
    return s

S = _session()

def _norm_abbr(s: Optional[str]) -> Optional[str]:
    if not isinstance(s, str): return None
    s = s.strip().upper()
    if not s: return None
    aliases = {
        "LA":"LAK","L.A.":"LAK",
        "NJ":"NJD",
        "MON":"MTL","MTL.":"MTL",
        "TB":"TBL",
        "SJ":"SJS","S.J.":"SJS",
        "WSH":"WSH","WAS":"WSH",
        "CLB":"CBJ","COLS":"CBJ",
        "PHX":"ARI",
    }
    return aliases.get(s, s)

def _parse_mmss(s: str) -> int:
    try:
        m, sec = s.split(":",1); return int(m)*60+int(sec)
    except Exception:
        return 0

def _abs_sec(period: int, time_in_period: str) -> int:
    # 20-minute periods
    return (int(period or 0)-1)*20*60 + _parse_mmss(time_in_period or "0:00")

def _strength_pair(ev: Dict[str,Any]) -> Tuple[int,int]:
    st = ev.get("homeTeamDefendingStrength") or ev.get("homeTeamStrength")
    if isinstance(st,str) and "x" in st:
        try:
            a,b = st.split("x",1); return (int(a),int(b))
        except Exception:
            pass
    d = ev.get("details") or {}
    s = d.get("strength")
    if isinstance(s,str) and "v" in s:
        try:
            a,b = s.split("v",1); return (int(a),int(b))
        except Exception:
            pass
    h = ev.get("homeTeamOnIceCount") or 0
    a = ev.get("awayTeamOnIceCount") or 0
    return (int(h or 0), int(a or 0))

def _build_shift_counts(shifts: List[Dict[str,Any]]):
    per_team_player_intervals: Dict[str,List[Tuple[int,int]]] = {}
    boundaries: List[int] = []
    for s in shifts or []:
        try:
            per = int(s.get("period") or 0)
            a = _abs_sec(per, s.get("startTime") or "0:00")
            b = _abs_sec(per, s.get("endTime") or "0:00")
            if b <= a: continue
            tab = _norm_abbr(s.get("teamAbbrev") or "")
            if not tab: continue
            pos = (s.get("playerPositionCode") or s.get("positionCode") or "").strip().upper()
            if pos == "G":  # exclude explicit goalies
                continue
            per_team_player_intervals.setdefault(tab, []).append((a,b))
            boundaries.append(a); boundaries.append(b)
        except Exception:
            continue
    if len(per_team_player_intervals) < 2 or not boundaries:
        return None, None, None
    def team_load(iv): return sum(max(0,e-s) for s,e in iv)
    team_sorted = sorted(per_team_player_intervals.items(), key=lambda kv: team_load(kv[1]), reverse=True)
    if len(team_sorted) < 2: return None, None, None
    T1, iv1 = team_sorted[0]; T2, iv2 = team_sorted[1]
    xs = sorted(set(boundaries))
    if len(xs) < 2: return None, None, None

    def count_at_one(iv_list, t0):
        c = 0
        for s,e in iv_list:
            if s <= t0 < e: c += 1
        return c

    # precompute counts at segment start
    seg_counts = {}
    for i in range(len(xs)-1):
        t0 = xs[i]
        seg_counts[t0] = (count_at_one(iv1,t0), count_at_one(iv2,t0))

    def count_at(team: str, t: int) -> int:
        import bisect
        j = max(0, bisect.bisect_right(xs, t)-1)
        t0 = xs[j]
        c1,c2 = seg_counts.get(t0,(0,0))
        return c1 if team == T1 else c2 if team == T2 else 0

    return (T1,T2), xs, count_at

def probe_game(gid: int) -> Dict[str,Any]:
    out = {"game_id": gid}

    # PBP
    rp = S.get(API_PBP.format(gid=gid), timeout=25)
    if rp.status_code == 404:
        out["pbp_status"] = "404"
        return out
    rp.raise_for_status()
    pbp = rp.json() or {}
    plays = list(pbp.get("plays") or [])
    out["pbp_status"] = "ok"
    out["plays"] = len(plays)

    # Shiftcharts
    rs = S.get(API_SHIFTS.format(gid=gid), timeout=30)
    if rs.status_code == 404:
        out["shifts_status"] = "404"
        return out
    rs.raise_for_status()
    shifts = (rs.json() or {}).get("data") or []
    out["shifts_status"] = "ok"
    out["shifts_rows"] = len(shifts)

    # --- Check 1: any PBP strength signal? ---
    strength_fmt = 0
    onice_pairs = 0
    for ev in plays:
        h,a = _strength_pair(ev)
        # formats:
        st1 = ev.get("homeTeamDefendingStrength") or ev.get("homeTeamStrength")
        st2 = (ev.get("details") or {}).get("strength")
        if isinstance(st1,str) and "x" in st1: strength_fmt += 1
        if isinstance(st2,str) and "v" in st2: strength_fmt += 1
        if (h or 0) > 0 and (a or 0) > 0: onice_pairs += 1
    out["check1_strength_present"] = (strength_fmt > 0) or (onice_pairs > 0)
    out["pbp_strength_tokens"] = strength_fmt
    out["pbp_onice_pairs"] = onice_pairs

    # --- Check 2: penalty has team or player id we can map? ---
    penalties = [ev for ev in plays if "penal" in (ev.get("typeDescKey","")+ev.get("typeDesc","")).lower()]
    pen_team_keys = 0
    pen_player_keys = 0
    for ev in penalties:
        d = ev.get("details") or {}
        for k in ("committedByTeamAbbrev","againstTeamAbbrev","offendingTeamAbbrev","eventOwnerTeamAbbrev","teamAbbrev","byTeamAbbrev"):
            if isinstance(d.get(k), str) and d.get(k).strip():
                pen_team_keys += 1; break
        for k in ("committedByPlayerId","penaltyToPlayerId","playerId","offendingPlayerId","againstPlayerId","byPlayerId"):
            v = d.get(k)
            if isinstance(v,(int,str)) and str(v).isdigit():
                pen_player_keys += 1; break
    out["check2_penalty_team_or_player"] = (pen_team_keys > 0) or (pen_player_keys > 0)
    out["penalties"] = len(penalties)
    out["penalties_with_team_key"] = pen_team_keys
    out["penalties_with_player_id"] = pen_player_keys

    # --- Check 3: shiftcharts have two teams after normalization? ---
    teams = []
    for s in shifts:
        t = _norm_abbr(s.get("teamAbbrev") or "")
        if t: teams.append(t)
    uniq = sorted(set(teams))
    out["shift_unique_teams_norm"] = uniq
    out["check3_two_shift_teams"] = (len(uniq) >= 2)

    # --- Check 4: boundary density ---
    bounds = []
    for s in shifts:
        try:
            per = int(s.get("period") or 0)
            a = _abs_sec(per, s.get("startTime") or "0:00")
            b = _abs_sec(per, s.get("endTime") or "0:00")
            if b > a:
                bounds.append(a); bounds.append(b)
        except Exception:
            pass
    distinct_bounds = len(set(bounds))
    out["boundary_count"] = distinct_bounds
    out["check4_dense_boundaries"] = (distinct_bounds >= 60)  # heuristic

    # --- Check 5: goalie labeling sanity ---
    pos_counts = {}
    for s in shifts:
        pos = (s.get("playerPositionCode") or s.get("positionCode") or "").strip().upper() or "(blank)"
        pos_counts[pos] = pos_counts.get(pos,0)+1
    goalie_rows = pos_counts.get("G", 0)
    skaterish_rows = sum(v for k,v in pos_counts.items() if k != "G")
    # ok if goalies are not overwhelming
    out["positions"] = pos_counts
    out["check5_goalie_label_ok"] = (goalie_rows < skaterish_rows)

    # --- Check 6: shift-only advantage ever occurs? ---
    sc = _build_shift_counts(shifts)
    adv_seen = False
    if sc[0] and sc[1]:
        (T1,T2), xs, count_at = sc
        for i in range(len(xs)-1):
            t0 = xs[i]
            c1,c2 = count_at(T1,t0), count_at(T2,t0)
            if c1 > 0 and c2 > 0 and c1 != c2:
                adv_seen = True; break
    out["check6_shift_only_advantage"] = adv_seen
    out["shift_count_keys"] = sc[0] if sc[0] else None

    # --- Check 7: external id coverage proxy (just raw playerId presence here) ---
    nhl_ids = set()
    for s in shifts:
        v = s.get("playerId")
        if isinstance(v,(int,str)) and str(v).isdigit():
            nhl_ids.add(int(v))
    out["nhl_ids_in_shifts"] = len(nhl_ids)
    # DB mapping rate not checked here (no DB); treat as informational.

    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--gid", type=int, nargs="+", required=True, help="NHL game id(s) to probe, e.g. 2023020099")
    args = ap.parse_args()
    results = []
    for gid in args.gid:
        try:
            results.append(probe_game(gid))
        except Exception as e:
            results.append({"game_id": gid, "error": str(e)})
    print(json.dumps(results, indent=2))

if __name__ == "__main__":
    main()
