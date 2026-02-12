from __future__ import annotations

import os, sys, json, math
import datetime as dt
import requests
import logging

from zoneinfo import ZoneInfo
from typing import Dict, Any, List, Tuple, Optional
from scripts.shared.supabase_utils import get_supabase
from scripts.shared.team_name_map import get_team_info_by_id
from ml.feature_utils import load_feature_names

log = logging.getLogger("precompute")

PROCESSED_KEYS: set[str] = set()
def _work_key(prop_type: str, player_id: str | int, game_id: str | int) -> str:
    return f"{prop_type}:{player_id}:{game_id}"


BATTER_PROPS = [
    "doubles","hits","hits_runs_rbis","home_runs","rbis","runs_rbis",
    "runs_scored","singles","stolen_bases","strikeouts_batting",
    "total_bases","triples","walks",
]
PITCHER_PROPS = [
    "strikeouts_pitching","outs_recorded","earned_runs","hits_allowed","walks_allowed",
]

MLB_API = "https://statsapi.mlb.com"

STATS_BASE = "https://statsapi.mlb.com/api/v1"

def _get(url: str, timeout: int = 15) -> Dict[str, Any]:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()

def schedule(date_yyyy_mm_dd: str) -> Dict[str, Any]:
    return _get(f"{STATS_BASE}/schedule?sportId=1&date={date_yyyy_mm_dd}")

def boxscore(game_pk: int) -> Dict[str, Any]:
    return _get(f"{STATS_BASE}/game/{game_pk}/boxscore")

def iso_to_et(utc_iso: str | None) -> str | None:
    """
    Convert an MLB API UTC ISO timestamp to ISO string in America/New_York.
    Returns None if input is falsey.
    """
    if not utc_iso:
        return None
    dt_utc = dt.datetime.fromisoformat(utc_iso.replace("Z", "+00:00")).astimezone(dt.timezone.utc)
    dt_et = dt_utc.astimezone(ZoneInfo("America/New_York"))
    return dt_et.replace(microsecond=0).isoformat()

def extract_games(sched: Dict[str, Any]) -> List[Dict[str, Any]]:
    out = []
    for day in (sched.get("dates") or []):
        for g in (day.get("games") or []):
            teams = g.get("teams", {})
            home_t = teams.get("home", {}).get("team", {}) or {}
            away_t = teams.get("away", {}).get("team", {}) or {}
            home_id = int(home_t.get("id", 0))
            away_id = int(away_t.get("id", 0))
            pk = int(g.get("gamePk"))

            game_time_et = iso_to_et(g.get("gameDate"))
            game_date_et = (game_time_et or "")[:10]  # <-- ET, not UTC

            # probable pitchers
            sp_home = (teams.get("home", {}).get("probablePitcher", {}) or {}).get("id")
            sp_away = (teams.get("away", {}).get("probablePitcher", {}) or {}).get("id")

            # abbr fallback from map
            home_abbr = home_t.get("abbreviation") or (get_team_info_by_id(home_id) or {}).get("abbr")
            away_abbr = away_t.get("abbreviation") or (get_team_info_by_id(away_id) or {}).get("abbr")

            out.append({
                "game_id": pk,
                "game_date": game_date_et,   # <-- ET date here
                "home_team_id": home_id,
                "away_team_id": away_id,
                "home_abbr": home_abbr,
                "away_abbr": away_abbr,
                "game_time_et": game_time_et,
                "prob_sp_home": int(sp_home) if sp_home else None,
                "prob_sp_away": int(sp_away) if sp_away else None,
            })
    return out

def extract_lineup_ids(box: Dict[str, Any]) -> Tuple[List[int], List[int]]:
    """Return (home_lineup_ids, away_lineup_ids). Empty until lineups post."""
    def lineup(team_obj: Dict[str, Any]) -> List[int]:
        out = []
        for pid_key, pdata in (team_obj.get("players") or {}).items():
            bo = str(pdata.get("battingOrder") or "")
            if not bo: continue
            try:
                slot = int(bo) // 100
            except Exception:
                continue
            pid = (pdata.get("person") or {}).get("id")
            if slot and pid: out.append((slot, int(pid)))
        out.sort(key=lambda t: t[0])
        return [pid for _, pid in out[:9]]

    teams = box.get("teams", {}) or {}
    return lineup(teams.get("home", {}) or {}), lineup(teams.get("away", {}) or {})

# ---------- Stats helpers ----------
def _people_stats(player_id: int, group: str, types: List[str]) -> Dict[str, Any]:
    # Example: type=last7,last15,last30 for hitters; for pitchers, same types
    qtypes = ",".join(types)
    url = f"{STATS_BASE}/people/{player_id}?hydrate=stats(group={group},type={qtypes})"
    return _get(url)

def _extract_last(stats_data: Dict[str, Any], want_type: str, field: str) -> float:
    # navigate MLB hydrate payload; return value or 0
    try:
        people = stats_data["people"][0]
        for s in people["stats"]:
            if s.get("type", {}).get("displayName","").lower() == want_type.lower():
                splits = s.get("splits") or []
                if not splits: return 0.0
                stat = splits[0].get("stat") or {}
                v = stat.get(field)
                if v is None: return 0.0
                if isinstance(v, str):
                    # innings like "12.2" -> 12 + 2/3
                    if field == "inningsPitched":
                        try:
                            parts = v.split(".")
                            whole = int(parts[0])
                            frac = int(parts[1]) if len(parts) > 1 else 0
                            return float(whole) + (frac / 3.0)
                        except Exception:
                            return 0.0
                    try:
                        return float(v)
                    except Exception:
                        return 0.0
                return float(v)
    except Exception:
        return 0.0
    return 0.0

def _team_active_hitters(team_id: int, date_iso: str) -> list[int]:
    if not team_id:
        return []
    try:
        url = f"{MLB_API}/api/v1/teams/{team_id}/roster?rosterType=active&date={date_iso}"
        data = _get(url) or {}
        hitters: list[int] = []
        for r in data.get("roster", []):
            person = (r or {}).get("person") or {}
            pos     = (r or {}).get("position") or {}
            pid = person.get("id")
            if not pid:
                continue
            abbr = (pos.get("abbreviation") or "").upper()
            code = str(pos.get("code") or "").upper()   # can be "1" for P
            name = (pos.get("name") or "").upper()
            if abbr == "P" or code == "1" or "PITCHER" in name:
                continue
            hitters.append(int(pid))
        return hitters
    except Exception:
        return []

def _bvp_vs_pitcher(hitter_id: int, pitcher_id: int) -> Dict[str, float]:
    # career vs pitcher
    try:
        url = f"{STATS_BASE}/people/{hitter_id}/stats?group=hitting&stats=vsPlayer&opposingPlayerId={pitcher_id}"
        data = _get(url)
        splits = data.get("stats", [])[0].get("splits", [])
        if not splits: return {}
        stat = splits[0].get("stat", {})
        # keys present: atBats, hits, homeRuns, baseOnBalls, strikeOuts, totalBases, plateAppearances, avg
        out = {}
        def g(name): 
            v = stat.get(name)
            try: return float(v)
            except: return 0.0
        out["bvp_pa_prior"] = g("plateAppearances")
        out["bvp_ab_prior"] = g("atBats")
        out["bvp_hits_prior"] = g("hits")
        out["bvp_tb_prior"] = g("totalBases")
        out["bvp_hr_prior"] = g("homeRuns")
        out["bvp_bb_prior"] = g("baseOnBalls")
        out["bvp_so_prior"] = g("strikeOuts")
        # smoothed rates
        ab = max(0.0, out["bvp_ab_prior"])
        pa = max(0.0, out["bvp_pa_prior"])
        hits = out["bvp_hits_prior"]; tb = out["bvp_tb_prior"]
        bb = out["bvp_bb_prior"]; so = out["bvp_so_prior"]
        # Laplace smoothing (add-1 where sensible)
        out["bvp_avg_prior_sm"] = (hits + 1.0) / (ab + 2.0) if ab > 0 else 0.0
        out["bvp_tb_per_ab_prior_sm"] = (tb + 1.0) / (ab + 2.0) if ab > 0 else 0.0
        out["bvp_bb_rate_prior_sm"] = (bb + 1.0) / (pa + 2.0) if pa > 0 else 0.0
        out["bvp_so_rate_prior_sm"] = (so + 1.0) / (pa + 2.0) if pa > 0 else 0.0
        return out
    except Exception:
        return {}

# ---------- Build per-prop feature dict (matches your JSON names) ----------
def build_features_for_batter(prop: str,
                              hitter_id: int,
                              team_id: int,
                              opp_team_id: int,
                              opp_prob_sp: Optional[int],
                              game_id: int,
                              game_date: str) -> Dict[str, Any]:
    names = load_feature_names(prop)
    feats = {k: 0 for k in names}

    # team/opponent labels (if present in schema)
    if "team" in feats:
        info = get_team_info_by_id(team_id) or {}
        if info.get("abbr"): feats["team"] = info["abbr"]
    if "opponent" in feats:
        info = get_team_info_by_id(opp_team_id) or {}
        if info.get("abbr"): feats["opponent"] = info["abbr"]
    if "opponent_encoded" in feats:
        feats["opponent_encoded"] = opp_team_id

    # last7/15/30 hitting
    try:
        hit_stats = _people_stats(hitter_id, group="hitting", types=["last7","last15","last30"])
        # fields: hits, totalBases, homeRuns, rbi, baseOnBalls, strikeOuts
        for horizon in ("7","15","30"):
            if f"d{horizon}_hits" in feats:
                feats[f"d{horizon}_hits"] = _extract_last(hit_stats, f"last{horizon}", "hits")
            if f"d{horizon}_total_bases" in feats:
                feats[f"d{horizon}_total_bases"] = _extract_last(hit_stats, f"last{horizon}", "totalBases")
            if f"d{horizon}_home_runs" in feats:
                feats[f"d{horizon}_home_runs"] = _extract_last(hit_stats, f"last{horizon}", "homeRuns")
            if f"d{horizon}_rbis" in feats:
                feats[f"d{horizon}_rbis"] = _extract_last(hit_stats, f"last{horizon}", "rbi")
            if f"d{horizon}_walks" in feats:
                feats[f"d{horizon}_walks"] = _extract_last(hit_stats, f"last{horizon}", "baseOnBalls")
            if f"d{horizon}_strikeouts_batting" in feats:
                feats[f"d{horizon}_strikeouts_batting"] = _extract_last(hit_stats, f"last{horizon}", "strikeOuts")
    except Exception:
        pass

    # pitcher form (last15/30): k/9, bb/9, era – from opposing probable SP
    if opp_prob_sp:
        try:
            pit_stats = _people_stats(opp_prob_sp, group="pitching", types=["last15","last30"])
            def per9(type_name: str, num_field: str) -> float:
                num = _extract_last(pit_stats, type_name, num_field)  # K or BB
                ip  = _extract_last(pit_stats, type_name, "inningsPitched")
                return (num * 9.0 / ip) if ip > 0 else 0.0
            if "d15_k_per9" in feats: feats["d15_k_per9"] = per9("last15","strikeOuts")
            if "d30_k_per9" in feats: feats["d30_k_per9"] = per9("last30","strikeOuts")
            if "d15_bb_per9" in feats: feats["d15_bb_per9"] = per9("last15","baseOnBalls")
            if "d30_bb_per9" in feats: feats["d30_bb_per9"] = per9("last30","baseOnBalls")
            if "d15_era" in feats: feats["d15_era"] = _extract_last(pit_stats, "last15", "era")
            if "d30_era" in feats: feats["d30_era"] = _extract_last(pit_stats, "last30", "era")
        except Exception:
            pass

        # BvP vs opposing probable SP
        try:
            bvp = _bvp_vs_pitcher(hitter_id, opp_prob_sp)
            for k, v in bvp.items():
                if k in feats: feats[k] = v
        except Exception:
            pass

    return feats

def build_features_for_pitcher(prop: str,
                               pitcher_id: int,
                               team_id: int,
                               opp_team_id: int,
                               game_id: int,
                               game_date: str) -> Dict[str, Any]:
    names = load_feature_names(prop)
    feats = {k: 0 for k in names}

    if "team" in feats:
        info = get_team_info_by_id(team_id) or {}
        if info.get("abbr"): feats["team"] = info["abbr"]
    if "opponent" in feats:
        info = get_team_info_by_id(opp_team_id) or {}
        if info.get("abbr"): feats["opponent"] = info["abbr"]
    if "opponent_encoded" in feats:
        feats["opponent_encoded"] = opp_team_id

    # last7/15/30 pitching core (you can extend with more if your schemas include them)
    try:
        pit_stats = _people_stats(pitcher_id, group="pitching", types=["last15","last30"])
        def per9(type_name: str, num_field: str) -> float:
            num = _extract_last(pit_stats, type_name, num_field)
            ip  = _extract_last(pit_stats, type_name, "inningsPitched")
            return (num * 9.0 / ip) if ip > 0 else 0.0
        if "d15_k_per9" in feats: feats["d15_k_per9"] = per9("last15","strikeOuts")
        if "d30_k_per9" in feats: feats["d30_k_per9"] = per9("last30","strikeOuts")
        if "d15_bb_per9" in feats: feats["d15_bb_per9"] = per9("last15","baseOnBalls")
        if "d30_bb_per9" in feats: feats["d30_bb_per9"] = per9("last30","baseOnBalls")
        if "d15_era" in feats: feats["d15_era"] = _extract_last(pit_stats, "last15", "era")
        if "d30_era" in feats: feats["d30_era"] = _extract_last(pit_stats, "last30", "era")
    except Exception:
        pass

    return feats


def upsert_row(prop: str,
               player_id: int,
               game_id: int,
               game_date: str,
               feature_tag: str,
               features: Dict[str, Any],
               lineup_slot: Optional[int] = None,
               is_prob_sp: Optional[bool] = None,
               model_tag: Optional[str] = None):
    # idempotency: skip if we've already handled this (prop, player, game)
    k = _work_key(prop, player_id, game_id)
    if k in PROCESSED_KEYS:
        return False  # already processed in this run

    row = {
        "prop_type": prop,
        "player_id": int(player_id),
        "game_id": int(game_id),
        "game_date": game_date,
        "features": features,
        "feature_set_tag": feature_tag,
        "lineup_slot": lineup_slot,
        "is_probable_sp": bool(is_prob_sp) if is_prob_sp is not None else None,
        "model_tag": model_tag,
    }

    # drop nulls
    row = {k2: v for k2, v in row.items() if v is not None}

    # single cached client; raises cleanly if env missing
    get_supabase().from_("prop_features_precomputed").upsert(
        row,
        on_conflict="prop_type,player_id,game_id,feature_set_tag"
    ).execute()

    PROCESSED_KEYS.add(k)
    return True
   
def run_for_date(game_date: str, feature_tag: str = "v1"):
    PROCESSED_KEYS.clear()

    sched = schedule(game_date)
    games = extract_games(sched)

    for g in games:
        gid = g["game_id"]
        home = g["home_team_id"]
        away = g["away_team_id"]

        # probable SPs
        sp_home = g.get("prob_sp_home")
        sp_away = g.get("prob_sp_away")

        # =========================================================
        # BATTERS: use ACTIVE ROSTERS (no lineup dependency)
        # =========================================================
        home_hitters = _team_active_hitters(home, game_date)
        away_hitters = _team_active_hitters(away, game_date)

        # preserve order: all home hitters, then away hitters not already in list
        home_set = set(home_hitters)
        batters = home_hitters + [pid for pid in away_hitters if pid not in home_set]

        for pid in batters:
            is_home = pid in home_set
            team_id     = home if is_home else away
            opp_team_id = away if is_home else home
            opp_prob_sp = sp_away if is_home else sp_home  # opposing probable SP (may be None)

            for prop in BATTER_PROPS:
                try:
                    feats = build_features_for_batter(
                        prop=prop,
                        hitter_id=pid,
                        team_id=team_id,
                        opp_team_id=opp_team_id,
                        opp_prob_sp=opp_prob_sp,
                        game_id=gid,
                        game_date=game_date,
                    )
                    upsert_row(
                        prop=prop,
                        player_id=pid,
                        game_id=gid,
                        game_date=game_date,
                        feature_tag=feature_tag,
                        features=feats,
                        lineup_slot=None,      # no lineup dependency
                        is_prob_sp=False,      # batter rows
                        model_tag="poisson_v1" # keep your existing tag
                    )
                except Exception:
                    # keep going even if a single player/prop fails
                    pass

        # =========================================================
        # PITCHERS: PROBABLE STARTERS ONLY
        # =========================================================
        for sp, team_id, opp_id in ((sp_home, home, away), (sp_away, away, home)):
            if not sp:
                continue
            for pprop in PITCHER_PROPS:
                try:
                    pfeats = build_features_for_pitcher(
                        prop=pprop,
                        pitcher_id=sp,
                        team_id=team_id,
                        opp_team_id=opp_id,
                        game_id=gid,
                        game_date=game_date,
                    )
                    upsert_row(
                        prop=pprop,
                        player_id=sp,
                        game_id=gid,
                        game_date=game_date,
                        feature_tag=feature_tag,
                        features=pfeats,
                        lineup_slot=None,
                        is_prob_sp=True,       # tag as probable starter
                        model_tag="poisson_v1"
                    )
                except Exception:
                    pass

if __name__ == "__main__":
    # Use ET as the canonical baseball date when no arg is given
    if len(sys.argv) >= 2 and sys.argv[1]:
        date_arg = sys.argv[1]
    else:
        now_et = dt.datetime.now(ZoneInfo("America/New_York"))
        date_arg = now_et.date().isoformat()

    feature_tag = os.getenv("FEATURE_SET_TAG", "v1")
    print(f"[precompute] running for date={date_arg} tag={feature_tag}")

    run_for_date(date_arg, feature_tag=feature_tag)
