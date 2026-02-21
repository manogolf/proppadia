"""
v2_write_training_from_pfp.py
==============================
Purpose
- Join/copy feature blobs from `prop_features_precomputed` (PFP) into
  `model_training_props` (MTP) rows for the same (prop_type, player_id, game_id).
- This co-locates features with labels in MTP for faster training exports.

What it DOES
- For each MTP row (prop_source usually 'mlb_api'), looks up the matching PFP row
  (same prop_type, player_id, game_id, feature_set_tag).
- Copies the features JSON into MTP (configure the target column in the script),
  or updates selected feature columns if your MTP has them.
- Skips rows without a matching PFP entry.

What it DOES NOT do
- Does not compute labels or read boxscores (that’s the labels script).
- Does not generate features (that’s the backfill script).

Reads
- `mlb.model_training_props` (source of labeled rows).
- `mlb.prop_features_precomputed` (source of feature blobs).

Writes
- `mlb.model_training_props` (adds/updates a features snapshot per row).
  NOTE: If your MTP schema lacks a JSON features column, adjust the mapping
  or add one (e.g., `features jsonb` or `features_json jsonb`).

Run Order
- After v2_backfill_mlb_api_training.py (to ensure features exist).
- After v2_write_mlb_api_labels_to_mtp.py (so there are labeled targets to enrich).

Idempotency
- Updates are idempotent: repeated runs simply refresh the same rows.

Env / Flags
- FEATURE_SET_TAG (default: "v1") — must match PFP rows you want to pull.
- QUIET / VERBOSE / DEBUG — optional logging controls.

Typical cron
- Nightly after labels job, or ad-hoc during backfills.
"""


from __future__ import annotations

import os, sys, json, argparse
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
import requests
_DOUBLES_OVER_THRESHOLD = 0.15
_HITS_ALLOWED_OVER_MARGIN = 1.0

try:
    from backend.mlb.shared.supabase_utils import supabase
except Exception:
    from backend.supabase.supabase_utils import supabase  # type: ignore


# ------------- helpers ---------------

BATTER_PROPS = [
    "singles", "hits", "total_bases", "hits_runs_rbis", "runs_rbis",
    "rbis", "runs_scored", "home_runs", "doubles", "triples",
    "walks", "strikeouts_batting", "stolen_bases",
]

PITCHER_PROPS = [
    "strikeouts_pitching", "walks_allowed", "hits_allowed",
    "earned_runs", "outs_recorded",
]

def _canon_prop(p: str) -> str:
    p = (p or "").strip().lower()
    if p in {"hitsrundrbis", "h+r+rbi", "hrr"}:
        return "hits_runs_rbis"
    if p in {"runsrbis", "r+rbi", "runs_rbi"}:
        return "runs_rbis"
    if p in {"so_bat", "k_bat"}:
        return "strikeouts_batting"
    if p in {"so_pit", "k_pit"}:
        return "strikeouts_pitching"
    return p

def _determine_outcome(actual: float, line: float, ou: str) -> str:
    ou = (ou or "over").strip().lower()
    if ou not in ("over", "under"): ou = "over"
    if actual is None or line is None:
        return "dnp"
    if ou == "over":
        if actual > line: return "win"
        if actual < line: return "loss"
    else:
        if actual < line: return "win"
        if actual > line: return "loss"
    return "push"  # only possible if caller ever uses integer lines

def _schedule(date_str: str) -> Dict[str, Any]:
    url = f"https://statsapi.mlb.com/api/v1/schedule?sportId=1&date={date_str}"
    r = requests.get(url, timeout=12)
    return r.json() if r.ok else {}

def _boxscore(game_pk: int) -> Optional[Dict[str, Any]]:
    url = f"https://statsapi.mlb.com/api/v1/game/{game_pk}/boxscore"
    r = requests.get(url, timeout=15)
    return r.json() if r.ok else None

def _game_feed(game_pk: int) -> Optional[Dict[str, Any]]:
    url = f"https://statsapi.mlb.com/api/v1/game/{game_pk}/feed/live"
    r = requests.get(url, timeout=15)
    return r.json() if r.ok else None

def _iso_naive(iso_or_z: Optional[str]) -> Optional[str]:
    if not iso_or_z: return None
    try:
        return datetime.fromisoformat(iso_or_z.replace("Z","+00:00")).replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

def _extract_batter_actual(bat: Dict[str, Any], ptype: str) -> Optional[float]:
    H  = float(bat.get("hits") or 0)
    _2 = float(bat.get("doubles") or 0)
    _3 = float(bat.get("triples") or 0)
    HR = float(bat.get("homeRuns") or 0)
    R  = float(bat.get("runs") or 0)
    RBI= float(bat.get("rbi") or 0)
    BB = float(bat.get("baseOnBalls") or 0)
    SO = float(bat.get("strikeOuts") or 0)
    SB = float(bat.get("stolenBases") or 0)

    singles = max(0.0, H - _2 - _3 - HR)
    tb = singles + 2*_2 + 3*_3 + 4*HR
    hrr = H + R + RBI

    match ptype:
        case "singles":             return singles
        case "hits":                return H
        case "total_bases":         return tb
        case "hits_runs_rbis":      return hrr
        case "runs_rbis":           return R + RBI
        case "rbis":                return RBI
        case "runs_scored":         return R
        case "home_runs":           return HR
        case "doubles":             return _2
        case "triples":             return _3
        case "walks":               return BB
        case "strikeouts_batting":  return SO
        case "stolen_bases":        return SB
    return None

def _extract_pitcher_actual(pit: Dict[str, Any], ptype: str) -> Optional[float]:
    SO = float(pit.get("strikeOuts") or 0)
    BB = float(pit.get("baseOnBalls") or 0)
    H  = float(pit.get("hits") or 0)
    ER = float(pit.get("earnedRuns") or 0)

    # outs_recorded: inningsPitched is like "5.2" (5 and 2/3). MLB also gives "outs".
    outs = pit.get("outs")
    if outs is None:
        ip = str(pit.get("inningsPitched") or "0")
        try:
            if "." in ip:
                whole, frac = ip.split(".", 1)
                outs = int(whole) * 3 + int(frac)
            else:
                outs = int(ip) * 3
        except Exception:
            outs = 0
    outs = float(outs or 0)

    match ptype:
        case "strikeouts_pitching": return SO
        case "walks_allowed":       return BB
        case "hits_allowed":        return H
        case "earned_runs":         return ER
        case "outs_recorded":       return outs
    return None

def _player_box_nodes(box: Dict[str, Any], pid: int) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Return (batting_node, pitching_node) for player_id in boxscore JSON."""
    if not box: return None, None
    for side in ("home", "away"):
        players = (box.get("teams", {}).get(side, {}) or {}).get("players", {}) or {}
        for _, node in players.items():
            person = node.get("person") or {}
            if int(person.get("id") or -1) == pid:
                return node.get("stats", {}).get("batting") or {}, node.get("stats", {}).get("pitching") or {}
    return None, None


def _hits_line_from_anchor(anchor: float) -> float:
    x = max(0.0, float(anchor))
    if x < 1.0:
        return 0.5
    if x < 2.0:
        return 1.5
    if x < 3.0:
        return 2.5
    return 3.5


def _doubles_line_from_anchor(anchor: float) -> float:
    x = max(0.0, float(anchor))
    if x < 1.0:
        return 0.5
    return 1.5


def _hits_allowed_line_from_anchor(anchor: float) -> float:
    x = max(0.0, float(anchor))
    if x < 4.0:
        return 3.5
    if x < 5.0:
        return 4.5
    if x < 6.0:
        return 5.5
    if x < 7.0:
        return 6.5
    return 7.5


def _runs_scored_line_from_anchor(anchor: float) -> float:
    x = max(0.0, float(anchor))
    if x < 1.0:
        return 0.5
    if x < 2.0:
        return 1.5
    return 2.5


def _runs_rbis_line_from_anchor(anchor: float) -> float:
    x = max(0.0, float(anchor))
    if x < 1.0:
        return 0.5
    if x < 2.0:
        return 1.5
    if x < 3.0:
        return 2.5
    return 3.5


def _hits_runs_rbis_line_from_anchor(anchor: float) -> float:
    x = max(0.0, float(anchor))
    if x < 2.0:
        return 1.5
    if x < 3.0:
        return 2.5
    if x < 4.0:
        return 3.5
    if x < 5.0:
        return 4.5
    return 5.5


def _feat_nonneg(features: Dict[str, Any], key: str) -> Optional[float]:
    try:
        v = float((features or {}).get(key))
        if v >= 0:
            return v
    except Exception:
        return None
    return None


def _expected_runs_scored_from_features(features: Dict[str, Any]) -> float:
    d7_hits = _feat_nonneg(features, "d7_hits") or 0.0
    d7_walks = _feat_nonneg(features, "d7_walks") or 0.0
    d7_home_runs = _feat_nonneg(features, "d7_home_runs") or 0.0
    # Proxy for runs_scored when explicit d7_runs_scored is unavailable in PFP.
    return max(0.0, (0.45 * d7_hits) + (0.35 * d7_walks) + (0.20 * d7_home_runs))


def _expected_runs_rbis_from_features(features: Dict[str, Any]) -> float:
    d7_rbis = _feat_nonneg(features, "d7_rbis")
    if d7_rbis is not None:
        return d7_rbis
    d7_hits = _feat_nonneg(features, "d7_hits") or 0.0
    return max(0.0, 0.7 * d7_hits)


def _expected_hits_runs_rbis_from_features(features: Dict[str, Any]) -> float:
    d7_hits = _feat_nonneg(features, "d7_hits") or 0.0
    d7_rbis = _feat_nonneg(features, "d7_rbis") or 0.0
    d7_runs_scored = _expected_runs_scored_from_features(features)
    return max(0.0, d7_hits + d7_rbis + d7_runs_scored)


def _decide_line(actual: float) -> float:
    # half-step around actual to avoid pushes, jitter slightly for realism
    if actual <= 0:
        return 0.5
    # n +/- 0.5, rounded to nearest .5
    import random
    line = actual + (0.5 if random.random() < 0.5 else -0.5)
    return round(line * 2) / 2

# ------------- main ---------------

def iter_dates(start: str, end: str):
    d0 = datetime.strptime(start, "%Y-%m-%d").date()
    d1 = datetime.strptime(end, "%Y-%m-%d").date()
    cur = d0
    while cur <= d1:
        yield cur.strftime("%Y-%m-%d")
        cur += timedelta(days=1)

def _final_game_ids(date_str: str) -> set[int]:
    js = _schedule(date_str)
    out: set[int] = set()
    for day in js.get("dates", []):
        for g in day.get("games", []):
            st = (g.get("status", {}) or {}).get("detailedState", "")
            if st.lower() == "final":
                out.add(int(g.get("gamePk")))
    return out

def _pfp_rows_for_date(date_str: str) -> List[Dict[str, Any]]:
    # pull a minimal set from mlb.prop_features_precomputed (spine)
    res = (
        supabase
        .schema("mlb")
        .from_("prop_features_precomputed")
        .select("prop_type, player_id, game_id, game_date, features")
        .eq("game_date", date_str)
        .limit(20000)
        .execute()
    )
    return getattr(res, "data", []) or []

def upsert_mtp(row: Dict[str, Any]) -> None:
    supabase.schema("mlb").from_("model_training_props").upsert(
        row,
        on_conflict="player_id,game_id,prop_type,prop_source",
    ).execute()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()

    total = inserted = skipped = 0

    for d in iter_dates(args.start, args.end):
        finals = _final_game_ids(d)
        if not finals:
            continue

        # cache boxscores per game
        box_by_gid: Dict[int, Dict[str, Any]] = {}

        rows = _pfp_rows_for_date(d)
        for r in rows:
            total += 1
            try:
                gid = int(r["game_id"])
                pid = int(r["player_id"])
                ptype_raw = str(r.get("prop_type","")).strip()
                ptype = _canon_prop(ptype_raw)
            except Exception:
                skipped += 1
                continue

            if gid not in finals:
                skipped += 1
                continue

            if gid not in box_by_gid:
                box = _boxscore(gid)
                box_by_gid[gid] = box or {}
            box = box_by_gid.get(gid) or {}
            if not box:
                skipped += 1
                continue

            bat, pit = _player_box_nodes(box, pid)
            actual: Optional[float] = None
            if ptype in BATTER_PROPS:
                actual = _extract_batter_actual(bat or {}, ptype)
            elif ptype in PITCHER_PROPS:
                actual = _extract_pitcher_actual(pit or {}, ptype)
            else:
                skipped += 1
                continue

            if actual is None:
                skipped += 1
                continue

            # derive line & OU
            if ptype == "hits":
                expected_hits = None
                d7_hits = (r.get("features") or {}).get("d7_hits")
                try:
                    expected_hits = float(d7_hits)
                    if not (expected_hits >= 0):
                        expected_hits = None
                except Exception:
                    expected_hits = None
                anchor = expected_hits if expected_hits is not None else 1.0
                line = _hits_line_from_anchor(anchor)
                over_under = "over" if anchor > line else "under"
            elif ptype == "doubles":
                expected_doubles = None
                d7_doubles = (r.get("features") or {}).get("d7_doubles")
                try:
                    expected_doubles = float(d7_doubles)
                    if not (expected_doubles >= 0):
                        expected_doubles = None
                except Exception:
                    expected_doubles = None
                anchor = expected_doubles if expected_doubles is not None else 0.35
                line = _doubles_line_from_anchor(anchor)
                over_under = "over" if anchor >= _DOUBLES_OVER_THRESHOLD else "under"
            elif ptype == "hits_allowed":
                expected_ha = None
                d7_hits_allowed = (r.get("features") or {}).get("d7_hits_allowed")
                try:
                    expected_ha = float(d7_hits_allowed)
                    if not (expected_ha >= 0):
                        expected_ha = None
                except Exception:
                    expected_ha = None
                anchor = expected_ha if expected_ha is not None else 4.5
                line = _hits_allowed_line_from_anchor(anchor)
                over_under = "over" if anchor >= (line + _HITS_ALLOWED_OVER_MARGIN) else "under"
            elif ptype == "runs_scored":
                anchor = _expected_runs_scored_from_features(r.get("features") or {})
                line = _runs_scored_line_from_anchor(anchor)
                over_under = "over" if anchor > line else "under"
            elif ptype == "runs_rbis":
                anchor = _expected_runs_rbis_from_features(r.get("features") or {})
                line = _runs_rbis_line_from_anchor(anchor)
                over_under = "over" if anchor > line else "under"
            elif ptype == "hits_runs_rbis":
                anchor = _expected_hits_runs_rbis_from_features(r.get("features") or {})
                line = _hits_runs_rbis_line_from_anchor(anchor)
                over_under = "over" if anchor > line else "under"
            else:
                line = _decide_line(actual)
                over_under = "over"  # arbitrary; we only need a consistent label target
            outcome = _determine_outcome(actual, line, over_under)

            # enrich from features (ids/abbrs if present)
            feats = r.get("features") or {}
            team_id = feats.get("team_id")
            opp_id  = feats.get("opponent_team_id") or feats.get("opponent_encoded")
            is_home = feats.get("is_home")
            team_abbr = feats.get("team") or feats.get("team_abbr")
            opp_abbr  = feats.get("opponent")

            # try to fetch game_time from live feed (nice-to-have)
            gfeed = _game_feed(gid)
            game_time_naive = None
            try:
                dtz = (((gfeed or {}).get("gameData") or {}).get("datetime") or {}).get("dateTime")
                game_time_naive = _iso_naive(dtz)
            except Exception:
                pass

            mtp = {
                "game_date": d,
                "game_id": gid,
                "player_id": pid,
                "player_name": None,     # optional; leave null
                "prop_type": ptype,
                "prop_value": actual,    # actual result as value (training target context)
                "line": line,
                "over_under": over_under,
                "outcome": outcome,
                "status": "resolved",
                "prop_source": "mlb_api",
                "team_id": int(team_id) if team_id is not None else None,
                "opponent_team_id": int(opp_id) if opp_id is not None else None,
                "team": team_abbr,
                "opponent": opp_abbr,
                "is_home": bool(is_home) if isinstance(is_home, (bool, int)) else None,
                "game_time": game_time_naive,  # naive ts column
            }

            # drop Nones to avoid NOT NULL/constraint surprises
            mtp = {k: v for k, v in mtp.items() if v is not None}
            upsert_mtp(mtp)
            inserted += 1

    print(f"Done. considered={total} inserted={inserted} skipped={skipped}")

if __name__ == "__main__":
    main()
