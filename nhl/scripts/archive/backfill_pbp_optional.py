#!/usr/bin/env python3
"""
Backfill optional PBP-derived columns into NHL stage tables.

- Skaters (nhl.import_skater_logs_stage):
  shot_attempts, fenwick_for, missed_shots, blocked_shots_taken, rebounds_for,
  hits, takeaways, giveaways, ev_shot_attempts, pp_shot_attempts, sh_shot_attempts

- Goalies (nhl.import_goalie_logs_stage):
  ev_shots_faced, pp_shots_faced, sh_shots_faced, high_danger_shots_faced, rebounds_allowed

Data source: api-web.nhle.com play-by-play
Endpoint (observed): https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play

Usage:
  python scripts/backfill_pbp_optional.py --db-url "<POOLER_DSN>"
"""

import argparse
import json
import time
import re
from typing import Any, Dict, List, Optional, Tuple
import psycopg2
import psycopg2.extras
import requests

API_BASE = "https://api-web.nhle.com/v1"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "proppadia-pbp/1.0"})

# --------------------------- helpers ---------------------------

def get_json(url: str, timeout: float = 20.0) -> Optional[Dict[str, Any]]:
    r = SESSION.get(url, timeout=timeout)
    if r.status_code != 200:
        return None
    try:
        return r.json()
    except Exception:
        try:
            return json.loads(r.text)
        except Exception:
            return None

def fetch_pbp(gid: int):
    """
    Fetch api-web PBP (single authoritative shape for 2023+).
    Returns a dict with a 'plays' list or None on failure.
    """
    import requests, time
    url = f"https://api-web.nhle.com/v1/gamecenter/{gid}/play-by-play"
    for _ in range(3):
        try:
            r = requests.get(url, timeout=15)
            r.raise_for_status()
            pbp = r.json()
            # api-web returns {"plays":[...], "homeTeam":{...}, "awayTeam":{...}}
            if isinstance(pbp, dict) and isinstance(pbp.get("plays"), list):
                return pbp
        except Exception:
            time.sleep(0.4)
    return None
def find_plays(pbp: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Return the canonical play list.
    Prefer the root pbp['plays'] the API provides to avoid duplicates.
    Fall back to a conservative recursive search only if root list is missing.
    """
    plays = pbp.get("plays")
    if isinstance(plays, list):
        return plays

    # Fallback (older / weird games only): conservative recursive pull
    out: List[Dict[str, Any]] = []

    def looks_like_play(x: Any) -> bool:
        return isinstance(x, dict) and (
            "typeDescKey" in x or "eventId" in x or "typeCode" in x
        )

    def walk(obj: Any):
        if isinstance(obj, list):
            if obj and looks_like_play(obj[0]):
                out.extend([it for it in obj if looks_like_play(it)])
            else:
                for it in obj:
                    walk(it)
        elif isinstance(obj, dict):
            for v in obj.values():
                walk(v)

    walk(pbp)
    return out

def play_type_key(pl) -> str:
    """
    Normalize api-web types to our internal keys.
    """
    t = (pl.get("typeDescKey") or pl.get("eventType") or "").lower()
    if t in ("shot-on-goal", "shotongoal", "shot"):
        return "SHOT"
    if t in ("goal",):
        return "GOAL"
    if t in ("missed-shot", "missed_shot", "missed"):
        return "MISSED_SHOT"
    if t in ("blocked-shot", "blocked_shot", "blocked"):
        return "BLOCKED_SHOT"
    if t in ("hit",):
        return "HIT"
    if t in ("takeaway",):
        return "TAKEAWAY"
    if t in ("giveaway",):
        return "GIVEAWAY"
    if t in ("penalty",):
        return "PENALTY"
    return t.upper() or "OTHER"

def event_team_abbr(ev: dict, id_to_abbr: dict) -> str | None:
    """
    Resolve the event owner's team abbreviation.
    Prefer details.eventOwnerTeamId → id_to_abbr.
    Fall back to embedded abbrev fields.
    """
    d = ev.get("details") or {}
    tid = d.get("eventOwnerTeamId")
    if isinstance(tid, int):
        ab = id_to_abbr.get(tid)
        if isinstance(ab, str) and ab:
            return ab

    t = ev.get("team")
    if isinstance(t, dict):
        ab = t.get("abbrev")
        if isinstance(ab, str) and ab:
            return ab

    ab = d.get("eventOwnerTeamAbbrev")
    return ab if isinstance(ab, str) and ab else None


def players_by_role(play):
    """
    Return a role->player_id map. Accepts both api-web shapes and legacy shapes.
    Prioritizes explicit IDs in `details` (shootingPlayerId, goalieInNetId, etc.).
    """
    out = {}

    # 1) From players[] array (if present)
    for pl in (play.get("players") or []):
        if not isinstance(pl, dict):
            continue
        pid = pl.get("playerId") or pl.get("id")
        role = (pl.get("playerType") or pl.get("role") or pl.get("type") or "Player")
        if not isinstance(pid, int) or not isinstance(role, str):
            continue
        role = role.strip().title()
        if role == "Scorer":
            role = "Shooter"
        out.setdefault(role, pid)

    # 2) From details{} (api-web puts the key actors here)
    d = play.get("details")
    if isinstance(d, dict):
        # Shooter on shots/goals
        shooter = d.get("shootingPlayerId") or d.get("scoringPlayerId") or d.get("shooterPlayerId")
        if isinstance(shooter, int):
            out.setdefault("Shooter", shooter)

        # Goalie on shots/goals
        goalie = d.get("goalieInNetId") or d.get("goalieId")
        if isinstance(goalie, int):
            out.setdefault("Goalie", goalie)

        # Other roles we sometimes use
        hitter = d.get("hittingPlayerId")
        if isinstance(hitter, int):
            out.setdefault("Hitter", hitter)

        taker = d.get("takeawayPlayerId")
        if isinstance(taker, int):
            out.setdefault("Player", taker)  # TAKEAWAY uses generic 'Player' in our code

        giver = d.get("giveawayPlayerId")
        if isinstance(giver, int):
            out.setdefault("Player", giver)  # GIVEAWAY uses generic 'Player'

        # Penalties
        pen_on = d.get("penalizedPlayerId")
        drew_by = d.get("drawnByPlayerId") or d.get("drawnBy")
        if isinstance(pen_on, int):
            out.setdefault("PenaltyOn", pen_on)
        if isinstance(drew_by, int):
            out.setdefault("DrewBy", drew_by)

        # Blocked shots sometimes expose the original shooter here
        b_shooter = d.get("shootingPlayerIdBeforeBlock") or d.get("shooterPlayerId")
        if isinstance(b_shooter, int):
            out.setdefault("Shooter", b_shooter)

    return out

def shooter_from_event(ev: dict, roles: dict) -> int | None:
    """Return a shooter/scorer id from roles or event details."""
    # try roles first (handles "Shooter" or "Scorer")
    shooter = roles.get("Shooter") or roles.get("Scorer")
    if isinstance(shooter, int):
        return shooter
    # fall back to details keys used by api-web
    d = ev.get("details") or {}
    for k in ("shootingPlayerId", "scoringPlayerId", "shooterId", "playerId"):
        v = d.get(k)
        if isinstance(v, int):
            return v
    return None


def situation_code(play: Dict[str, Any]) -> Optional[str]:
    s = play.get("situationCode") or (play.get("details") or {}).get("situationCode")
    if isinstance(s, str) and "V" in s.upper():
        s = s.upper().replace("V", "v")
    return s


def strength_bucket(ev: dict, shooter_team_abbr: str) -> str:
    """
    Return "EV", "PP", or "SH" for the *shooter's* team on this event.
    Uses situationCode if present; falls back to on-ice counts.
    Requires caller to have stamped ev["_home_abbr"] / ev["_away_abbr"] once per game.
    """
    d = ev.get("details") or {}
    home_abbr = ev.get("_home_abbr")
    away_abbr = ev.get("_away_abbr")

    # 1) Prefer situationCode like "1551" (H-goalie, H-skaters, A-skaters, A-goalie)
    sc = d.get("situationCode") or ev.get("situationCode")
    if isinstance(sc, str) and len(sc) == 4 and sc.isdigit():
        home_skaters = int(sc[1])
        away_skaters = int(sc[2])
        if shooter_team_abbr == home_abbr:
            diff = home_skaters - away_skaters
        elif shooter_team_abbr == away_abbr:
            diff = away_skaters - home_skaters
        else:
            diff = 0
        return "PP" if diff > 0 else ("SH" if diff < 0 else "EV")

    # 2) Fallback: explicit on-ice counts if present
    h = ev.get("homeTeamOnIceCount")
    a = ev.get("awayTeamOnIceCount")
    if isinstance(h, int) and isinstance(a, int):
        if shooter_team_abbr == home_abbr:
            diff = h - a
        elif shooter_team_abbr == away_abbr:
            diff = a - h
        else:
            diff = 0
        return "PP" if diff > 0 else ("SH" if diff < 0 else "EV")

    # 3) Last resort
    return "EV"


def is_high_danger(play: Dict[str, Any]) -> bool:
    """
    Simple heuristic: certain shot types are more dangerous.
    (Refine later with coordinates if present.)
    """
    shot_type = (play.get("details") or {}).get("shotType") or ""
    shot_type = str(shot_type).lower()
    return any(k in shot_type for k in ("tip", "deflect", "wrap", "backhand", "slot"))

def game_clock_seconds(play: Dict[str, Any]) -> int:
    # Approx timeline for rebound detection
    period = 0
    try:
        period = int(play.get("period", 0))
    except Exception:
        pass
    t = play.get("timeInPeriod") or play.get("time") or play.get("timeRemaining")
    sec = 0
    if isinstance(t, str) and ":" in t:
        mm, ss = t.split(":")
        sec = int(mm) * 60 + int(ss)
    elif isinstance(t, (int, float)):
        sec = int(t)
    return period * 2000 + sec

# --------------------------- backfill core ---------------------------

def backfill_games(conn) -> Tuple[int, int, int]:
    """
    For every game_id present in either stage table, fetch PBP once,
    aggregate per-player tallies, and UPDATE stage rows.
    Returns: (games_seen, skater_rows_updated, goalie_rows_updated)
    """
    with conn.cursor() as cur:
        cur.execute("""
          SELECT DISTINCT game_id FROM (
            SELECT game_id FROM nhl.import_skater_logs_stage
            UNION
            SELECT game_id FROM nhl.import_goalie_logs_stage
          ) s
          ORDER BY game_id
        """)
        game_ids = [r[0] for r in cur.fetchall()]

    games_seen = 0
    sk_updates_total = 0
    gk_updates_total = 0

    for idx, gid in enumerate(game_ids, start=1):
        pbp = fetch_pbp(gid)
        games_seen += 1
        if not isinstance(pbp, dict):
            time.sleep(0.05)
            continue
        plays = find_plays(pbp)
        if not plays:
            time.sleep(0.05)
            continue

        # per-player aggregations
        sk = {}  # skater tallies: pid -> dict
        gk = {}  # goalie tallies: pid -> dict

        # rebound detection (skater-side)
        last_shot_by_team = {}  # team_abbr -> (clock, was_goal, saved_or_blocked)
        REBOUND_WINDOW = 3  # seconds

        # Map numeric team IDs in api-web events to abbrs we already use
        home = pbp.get("homeTeam") or {}
        away = pbp.get("awayTeam") or {}
        id_to_abbr = {}
        hid = home.get("id") or home.get("teamId")
        aid = away.get("id") or away.get("teamId")
        hab = home.get("abbrev")
        aab = away.get("abbrev")
        if isinstance(hid, int) and isinstance(hab, str):
            id_to_abbr[hid] = hab
        if isinstance(aid, int) and isinstance(aab, str):
            id_to_abbr[aid] = aab

        # Stamp home/away abbr on each play once so strength_bucket can use them
        home_abbr = (pbp.get("homeTeam") or {}).get("abbrev")
        away_abbr = (pbp.get("awayTeam") or {}).get("abbrev")
        for _p in plays:
            _p["_home_abbr"] = home_abbr
            _p["_away_abbr"] = away_abbr

        # -------------------- per-play aggregation --------------------
        for pl in plays:
            ptype = play_type_key(pl)
            roles = players_by_role(pl)

            # shooter + team must be known before strength
            shooter = shooter_from_event(pl, roles)
            team = event_team_abbr(pl, id_to_abbr)  # prefers details.eventOwnerTeamId → id_to_abbr
            if shooter is None or team is None:
                # still allow non-shot events below that don't need shooter (hits/takeaways/etc.)
                pass

            strength = strength_bucket(pl, team) if team else "EV"
            clock = game_clock_seconds(pl)

            # --- Skater SHOT/GOAL/MISSED/BLOCKED ---
            if shooter is not None and ptype in ("SHOT", "GOAL", "MISSED_SHOT", "BLOCKED_SHOT"):
                s = sk.setdefault(shooter, {
                    "shot_attempts": 0, "fenwick_for": 0,
                    "missed_shots": 0, "blocked_shots_taken": 0,
                    "rebounds_for": 0,
                    "hits": 0, "takeaways": 0, "giveaways": 0,
                    "ev_shot_attempts": 0, "pp_shot_attempts": 0, "sh_shot_attempts": 0,
                    "ev_sog": 0, "pp_sog": 0, "sh_sog": 0,
                })

                # Every attempt increments attempts + the strength split
                s["shot_attempts"] += 1
                if strength == "EV":
                    s["ev_shot_attempts"] += 1
                elif strength == "PP":
                    s["pp_shot_attempts"] += 1
                else:
                    s["sh_shot_attempts"] += 1

                if ptype in ("SHOT", "GOAL"):
                    # Fenwick and SOG-by-strength
                    s["fenwick_for"] += 1
                    if strength == "EV":
                        s["ev_sog"] += 1
                    elif strength == "PP":
                        s["pp_sog"] += 1
                    else:
                        s["sh_sog"] += 1

                    # Rebound-for detection: previous saved shot by same team within window
                    prev = last_shot_by_team.get(team)
                    if prev:
                        prev_clock, prev_goal, prev_saved_or_blocked = prev
                        if (clock - prev_clock) <= REBOUND_WINDOW and (not prev_goal) and prev_saved_or_blocked:
                            s["rebounds_for"] += 1

                    # Update last shot memory: mark if this was a goal or a saved shot
                    last_shot_by_team[team] = (clock, ptype == "GOAL", ptype == "SHOT")

                elif ptype == "MISSED_SHOT":
                    s["missed_shots"] += 1
                    s["fenwick_for"] += 1
                    last_shot_by_team[team] = (clock, False, False)

                elif ptype == "BLOCKED_SHOT":
                    s["blocked_shots_taken"] += 1
                    last_shot_by_team[team] = (clock, False, True)

            # --- Skater HIT ---
            elif ptype == "HIT":
                hitter = roles.get("Hitter") or roles.get("Player")
                if isinstance(hitter, int):
                    s = sk.setdefault(hitter, {
                        "shot_attempts": 0, "fenwick_for": 0,
                        "missed_shots": 0, "blocked_shots_taken": 0,
                        "rebounds_for": 0,
                        "hits": 0, "takeaways": 0, "giveaways": 0,
                        "ev_shot_attempts": 0, "pp_shot_attempts": 0, "sh_shot_attempts": 0,
                        "ev_sog": 0, "pp_sog": 0, "sh_sog": 0,
                    })
                    s["hits"] += 1

            # --- Skater TAKEAWAY ---
            elif ptype == "TAKEAWAY":
                taker = roles.get("Player")
                if isinstance(taker, int):
                    s = sk.setdefault(taker, {
                        "shot_attempts": 0, "fenwick_for": 0,
                        "missed_shots": 0, "blocked_shots_taken": 0,
                        "rebounds_for": 0,
                        "hits": 0, "takeaways": 0, "giveaways": 0,
                        "ev_shot_attempts": 0, "pp_shot_attempts": 0, "sh_shot_attempts": 0,
                        "ev_sog": 0, "pp_sog": 0, "sh_sog": 0,
                    })
                    s["takeaways"] += 1

            # --- Skater GIVEAWAY ---
            elif ptype == "GIVEAWAY":
                giver = roles.get("Player")
                if isinstance(giver, int):
                    s = sk.setdefault(giver, {
                        "shot_attempts": 0, "fenwick_for": 0,
                        "missed_shots": 0, "blocked_shots_taken": 0,
                        "rebounds_for": 0,
                        "hits": 0, "takeaways": 0, "giveaways": 0,
                        "ev_shot_attempts": 0, "pp_shot_attempts": 0, "sh_shot_attempts": 0,
                        "ev_sog": 0, "pp_sog": 0, "sh_sog": 0,
                    })
                    s["giveaways"] += 1

            # --- Skater PENALTY (optional tallies) ---
            elif ptype == "PENALTY":
                po = roles.get("PenaltyOn")
                drew = roles.get("DrewBy")
                if isinstance(po, int):
                    s = sk.setdefault(po, {
                        "shot_attempts": 0, "fenwick_for": 0,
                        "missed_shots": 0, "blocked_shots_taken": 0,
                        "rebounds_for": 0,
                        "hits": 0, "takeaways": 0, "giveaways": 0,
                        "ev_shot_attempts": 0, "pp_shot_attempts": 0, "sh_shot_attempts": 0,
                        "ev_sog": 0, "pp_sog": 0, "sh_sog": 0,
                    })
                    s["penalties_taken"] = s.get("penalties_taken", 0) + 1
                if isinstance(drew, int):
                    s = sk.setdefault(drew, {
                        "shot_attempts": 0, "fenwick_for": 0,
                        "missed_shots": 0, "blocked_shots_taken": 0,
                        "rebounds_for": 0,
                        "hits": 0, "takeaways": 0, "giveaways": 0,
                        "ev_shot_attempts": 0, "pp_shot_attempts": 0, "sh_shot_attempts": 0,
                        "ev_sog": 0, "pp_sog": 0, "sh_sog": 0,
                    })
                    s["penalties_drawn"] = s.get("penalties_drawn", 0) + 1

            # --- Goalie shots faced (count on SHOT/GOAL only) ---
            if ptype in ("SHOT", "GOAL"):
                goalie = roles.get("Goalie")
                if isinstance(goalie, int) and team:
                    t = gk.setdefault(goalie, {
                        "ev": 0, "pp": 0, "sh": 0,
                        "hd": 0, "rebounds_allowed": 0,
                        "last_save_clock": None, "last_save_team": None
                    })
                    if strength == "PP":
                        t["pp"] += 1
                    elif strength == "SH":
                        t["sh"] += 1
                    else:
                        t["ev"] += 1
                    if is_high_danger(pl):
                        t["hd"] += 1

            # Track last shot outcome per team for skater rebound_for (already handled above)
            # No extra code needed here; we updated last_shot_by_team inside the shot block.

        # ---- Second pass for goalie rebounds_allowed (approx) ----
        last_saved_shot = {}  # team_abbr -> (clock, goalie_id)
        for pl in plays:
            ptype = play_type_key(pl)
            team = event_team_abbr(pl, id_to_abbr)  # FIX: pass id_to_abbr
            roles = players_by_role(pl)
            clock = game_clock_seconds(pl)
            if ptype == "SHOT":
                prev = last_saved_shot.get(team)
                if prev:
                    prev_clock, prev_goalie = prev
                    if (clock - prev_clock) <= 3 and isinstance(prev_goalie, int) and (prev_goalie in gk):
                        gk[prev_goalie]["rebounds_allowed"] += 1
                goalie = roles.get("Goalie")
                if isinstance(goalie, int):
                    last_saved_shot[team] = (clock, goalie)
            elif ptype == "GOAL":
                if team in last_saved_shot:
                    del last_saved_shot[team]

        # ---- Idempotent reset for this game (do this BEFORE building sk_updates/gk_updates) ----
        with conn.cursor() as cur:
            # Skaters
            cur.execute("""
                UPDATE nhl.import_skater_logs_stage
                SET
                  shot_attempts = 0,
                  fenwick_for = 0,
                  missed_shots = 0,
                  blocked_shots_taken = 0,
                  rebounds_for = 0,
                  hits = 0,
                  takeaways = 0,
                  giveaways = 0,
                  ev_shot_attempts = 0,
                  pp_shot_attempts = 0,
                  sh_shot_attempts = 0,
                  ev_sog = 0,
                  pp_sog = 0,
                  sh_sog = 0
                WHERE game_id = %s
            """, (gid,))

            # Goalies
            cur.execute("""
                UPDATE nhl.import_goalie_logs_stage
                SET
                  ev_shots_faced = 0,
                  pp_shots_faced = 0,
                  sh_shots_faced = 0,
                  high_danger_shots_faced = 0,
                  rebounds_allowed = 0
                WHERE game_id = %s
            """, (gid,))
        conn.commit()

        # -------------------- write updates --------------------
        sk_updates = []
        for pid, agg in sk.items():
            sk_updates.append((
                agg.get("shot_attempts", 0) or 0,
                agg.get("fenwick_for", 0) or 0,
                agg.get("missed_shots", 0) or 0,
                agg.get("blocked_shots_taken", 0) or 0,
                agg.get("rebounds_for", 0) or 0,
                agg.get("hits", 0) or 0,
                agg.get("takeaways", 0) or 0,
                agg.get("giveaways", 0) or 0,
                agg.get("ev_shot_attempts", 0) or 0,
                agg.get("pp_shot_attempts", 0) or 0,
                agg.get("sh_shot_attempts", 0) or 0,
                agg.get("ev_sog", 0) or 0,
                agg.get("pp_sog", 0) or 0,
                agg.get("sh_sog", 0) or 0,
                int(pid), int(gid)
            ))

        gk_updates = []
        for pid, agg in gk.items():
            gk_updates.append((
                agg.get("ev", 0) or 0,
                agg.get("pp", 0) or 0,
                agg.get("sh", 0) or 0,
                agg.get("hd", 0) or 0,
                agg.get("rebounds_allowed", 0) or 0,
                int(pid), int(gid)
            ))

        with conn.cursor() as cur:
            if sk_updates:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    UPDATE nhl.import_skater_logs_stage AS s
                    SET
                    shot_attempts       = data.shot_attempts,
                    fenwick_for         = data.fenwick_for,
                    missed_shots        = data.missed_shots,
                    blocked_shots_taken = data.blocked_shots_taken,
                    rebounds_for        = data.rebounds_for,
                    hits                = data.hits,
                    takeaways           = data.takeaways,
                    giveaways           = data.giveaways,
                    ev_shot_attempts    = data.ev_shot_attempts,
                    pp_shot_attempts    = data.pp_shot_attempts,
                    sh_shot_attempts    = data.shot_attempts_sh,
                    ev_sog              = data.ev_sog,
                    pp_sog              = data.pp_sog,
                    sh_sog              = data.sh_sog
                    FROM (VALUES %s) AS data(
                        shot_attempts, fenwick_for, missed_shots, blocked_shots_taken, rebounds_for,
                        hits, takeaways, giveaways, ev_shot_attempts, pp_shot_attempts, shot_attempts_sh,
                        ev_sog, pp_sog, sh_sog, player_id, game_id
                    )
                    WHERE s.player_id = data.player_id AND s.game_id = data.game_id
                    """,
                    sk_updates,
                    page_size=500
                )
                sk_updates_total += cur.rowcount

            if gk_updates:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    UPDATE nhl.import_goalie_logs_stage AS g
                    SET
                    ev_shots_faced          = data.ev,
                    pp_shots_faced          = data.pp,
                    sh_shots_faced          = data.sh,
                    high_danger_shots_faced = data.hd,
                    rebounds_allowed        = data.rebounds_allowed
                    FROM (VALUES %s) AS data(
                        ev, pp, sh, hd, rebounds_allowed, player_id, game_id
                    )
                    WHERE g.player_id = data.player_id AND g.game_id = data.game_id
                    """,
                    gk_updates,
                    page_size=500
                )
                gk_updates_total += cur.rowcount

        conn.commit()

        # progress heartbeat every 50 games
        if idx % 50 == 0:
            print(
                f"[{idx}/{len(game_ids)}] game_id={gid} ✓ "
                f"sk_updates_total={sk_updates_total} gk_updates_total={gk_updates_total}",
                flush=True
            )

        time.sleep(0.06)  # be nice to API

    return games_seen, sk_updates_total, gk_updates_total

# --------------------------- main ---------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db-url", required=True, help="Postgres DSN (use Supabase Session Pooler + sslmode=require)")
    args = ap.parse_args()

    print("🔌 Connecting to DB…")
    conn = psycopg2.connect(args.db_url)
    try:
        g, s, k = backfill_games(conn)
    finally:
        conn.close()
    print(f"✅ Done. Games processed: {g}, skater rows updated: {s}, goalie rows updated: {k}")

if __name__ == "__main__":
    main()
