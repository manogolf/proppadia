#!/usr/bin/env python3
from __future__ import annotations

import os, sys, time, argparse
from typing import Dict, List, Tuple, Optional
import requests
import psycopg
import bootstrap_env



# ───────────────────────── config / helpers ─────────────────────────

API_WEB_PBP   = "https://api-web.nhle.com/v1/gamecenter/{gid}/play-by-play"
API_WEB_BOX   = "https://api-web.nhle.com/v1/gamecenter/{gid}/boxscore"
STATSAPI_FEED = "https://statsapi.web.nhl.com/api/v1/game/{gid}/feed/live"

TASK_KEY = "sog_splits_v1"

def env_db_url() -> str:
    db = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if not db:
        raise SystemExit("Missing SUPABASE_DB_URL / DATABASE_URL")
    # Supabase pooler expects SSL; avoid GSS negotiation
    if "?sslmode=" not in db and "&sslmode=" not in db:
        db += ("&" if "?" in db else "?") + "sslmode=require"
    if "?gssencmode=" not in db and "&gssencmode=" not in db:
        db += ("&" if "?" in db else "?") + "gssencmode=disable"
    return db

def to_int(x) -> Optional[int]:
    try:
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return None

def fetch_json(url: str, timeout=12) -> Optional[dict]:
    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None
    
def offenders_count(cur) -> int:
    cur.execute(f"SELECT COUNT(*) FROM nhl.skater_game_logs_raw WHERE {OFFENDER_PRED}")
    return cur.fetchone()[0]

# ───────────────────────── offenders & progress ─────────────────────────

OFFENDER_PRED = """
shots_on_goal > 0 AND (
  ev_sog IS NULL OR pp_sog IS NULL OR sh_sog IS NULL
  OR (COALESCE(ev_sog,0)+COALESCE(pp_sog,0)+COALESCE(sh_sog,0)) <> shots_on_goal
)
"""

def ensure_progress_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS nhl.backfill_progress (
          task text PRIMARY KEY,
          last_game_id bigint,
          updated_at timestamptz DEFAULT now()
        );
    """)

def get_last_game_id(cur) -> Optional[int]:
    cur.execute("SELECT last_game_id FROM nhl.backfill_progress WHERE task=%s", (TASK_KEY,))
    row = cur.fetchone()
    return row[0] if row else None

def set_last_game_id(cur, gid: Optional[int]):
    cur.execute("""
        INSERT INTO nhl.backfill_progress(task, last_game_id)
        VALUES (%s, %s)
        ON CONFLICT (task) DO UPDATE SET last_game_id = EXCLUDED.last_game_id, updated_at = now()
    """, (TASK_KEY, gid))

def find_offender_games(cur, limit: int, min_game_id: Optional[int]) -> List[int]:
    if min_game_id is None:
        cur.execute(f"""
            SELECT DISTINCT game_id
            FROM nhl.skater_game_logs_raw
            WHERE {OFFENDER_PRED}
            ORDER BY game_id
            LIMIT %s
        """, (limit,))
    else:
        cur.execute(f"""
            SELECT DISTINCT game_id
            FROM nhl.skater_game_logs_raw
            WHERE game_id > %s AND {OFFENDER_PRED}
            ORDER BY game_id
            LIMIT %s
        """, (min_game_id, limit))
    return [r[0] for r in cur.fetchall()]

# --- PBP parsing for backfill (api-web + statsapi) -----------------

def _plays_list(pbp_obj) -> list:
    if isinstance(pbp_obj, list):
        return pbp_obj
    if isinstance(pbp_obj, dict):
        # api-web
        if isinstance(pbp_obj.get("plays"), list):
            return pbp_obj["plays"]
        pby = pbp_obj.get("playByPlay")
        if isinstance(pby, dict):
            if isinstance(pby.get("allPlays"), list):
                return pby["allPlays"]
            if isinstance(pby.get("plays"), list):
                return pby["plays"]
        # statsapi
        live = pbp_obj.get("liveData")
        if isinstance(live, dict):
            pl = live.get("plays")
            if isinstance(pl, dict) and isinstance(pl.get("allPlays"), list):
                return pl["allPlays"]
    return []

def _event_type(play: dict) -> str:
    v = play.get("typeDescKey")
    if isinstance(v, str) and v.strip():
        return v.strip().upper()
    d = play.get("details")
    if isinstance(d, dict):
        v = d.get("typeDescKey")
        if isinstance(v, str) and v.strip():
            return v.strip().upper()
    r = play.get("result")
    if isinstance(r, dict):
        v = r.get("eventTypeId")
        if isinstance(v, str) and v.strip():
            return v.strip().upper()
    # numeric fallback
    for k in ("typeCode", "eventCode", "eventTypeId"):
        v = play.get(k)
        if isinstance(v, int):
            return f"CODE_{v}"
        if isinstance(v, str) and v.strip():
            return v.strip().upper()
    return ""

def _is_sog_like(play: dict) -> bool:
    d = play.get("details") or {}
    if d.get("isGoal") is True:
        return True
    if d.get("shotOnGoal") is True:
        return True
    et = _event_type(play)
    return et in ("SHOT", "SHOT-ON-GOAL", "SHOT_ON_GOAL", "GOAL")

def _shooter_id(play: dict):
    # api-web primary
    d = play.get("details") or {}
    pid = d.get("shootingPlayerId")  # <-- important for api-web
    if pid is None:
        pid = d.get("playerId")       # some payloads use this
    if pid is not None:
        try:
            return int(pid)
        except Exception:
            return None
    # statsapi fallback
    for pl in play.get("players", []) or []:
        if (pl.get("playerType") or "").lower() in ("shooter", "scorer"):
            pid = (pl.get("player") or {}).get("id") or pl.get("playerId")
            try:
                return int(pid)
            except Exception:
                return None
    return None

def _play_team_side(play: dict, home_id: int, away_id: int, home_abbr: str, away_abbr: str):
    """
    Return 'HOME' or 'AWAY' for the shooting team.
    Works for api-web (eventOwnerTeamId/teamAbbrev) and statsapi (team.triCode).
    """
    d = play.get("details") or {}
    # ids first
    owner_id = d.get("eventOwnerTeamId")
    try:
        owner_id = int(owner_id) if owner_id is not None else None
    except Exception:
        owner_id = None
    if owner_id == home_id:
        return "HOME"
    if owner_id == away_id:
        return "AWAY"

    # abbrs (api-web)
    ab = d.get("teamAbbrev")
    if isinstance(ab, str):
        ab = ab.strip().upper()
        if ab == home_abbr:
            return "HOME"
        if ab == away_abbr:
            return "AWAY"

    # statsapi team node
    t = play.get("team") or {}
    tri = (t.get("triCode") or t.get("abbrev") or "")
    tri = str(tri).upper()
    if tri == home_abbr:
        return "HOME"
    if tri == away_abbr:
        return "AWAY"

    return None

def _sit_counts(play: dict):
    # api-web: situationCode like "1551" (home v away skaters)
    code = play.get("situationCode")
    if isinstance(code, str) and "v" in code:
        try:
            a, b = code.split("v", 1)
            return int(a), int(b)
        except Exception:
            pass
    if isinstance(code, str) and len(code) == 4 and code.isdigit():
        try:
            # heuristic: last two digits are away, middle two are home
            # e.g., 1551 => 5v5 ; 1541 => 5v4 ; 1451 => 4v5 ; 1560 => 5v6 (goalie pulled)
            h = int(code[1:3])
            a = int(code[2:4])
            return h, a
        except Exception:
            pass
    # statsapi doesn't carry counts on SHOT; default to EV if unknown
    return None, None

def _strength(shoot_home: bool, hs: int | None, as_: int | None) -> str:
    if hs is None or as_ is None:
        return "EV"
    if hs == as_:
        return "EV"
    # PP if shoot team has more skaters, else SH
    return "PP" if (shoot_home and hs > as_) or ((not shoot_home) and as_ > hs) else "SH"

def compute_skater_sog_splits(pbp_obj, home_id: int, away_id: int, home_abbr: str, away_abbr: str):
    """
    Return { player_id: {'EV': e, 'PP': p, 'SH': s} } for SOG-like events.
    """
    out: dict[int, dict[str, int]] = {}
    used = 0
    plays = _plays_list(pbp_obj)
    for p in plays:
        if not _is_sog_like(p):
            continue
        pid = _shooter_id(p)
        side = _play_team_side(p, home_id, away_id, home_abbr, away_abbr)
        if pid is None or side is None:
            continue
        hs, aw = _sit_counts(p)
        lab = _strength(side == "HOME", hs, aw)
        d = out.setdefault(pid, {"EV": 0, "PP": 0, "SH": 0})
        d[lab] += 1
        used += 1
    # debug (optional)
    # print(f"[bf] splits sog-like used={used} shooters={len(out)}")
    return out

# Backward-compat aliases (avoid duplicate logic)
plays_list = _plays_list
shooter_id_from_play = _shooter_id

# --- Robust: normalize & merge SOG plays from api-web + statsapi ---------

def _period_num(p: dict) -> Optional[int]:
    pd = p.get("periodDescriptor") or (p.get("about") or {}).get("period")
    if isinstance(pd, dict):
        v = pd.get("number")
    else:
        v = pd
    try:
        return int(v)
    except Exception:
        return None

def _time_in_period(p: dict) -> Optional[str]:
    # Prefer "timeInPeriod" (api-web), else statsapi "about.periodTime"
    v = p.get("timeInPeriod")
    if isinstance(v, str) and v:
        return v
    about = p.get("about") or {}
    v = about.get("periodTime")
    if isinstance(v, str) and v:
        return v
    return None

def _xy_tuple(p: dict) -> Tuple[Optional[int], Optional[int]]:
    d = p.get("details") or {}
    x = d.get("xCoord"); y = d.get("yCoord")
    try: x = int(x) if x is not None else None
    except: x = None
    try: y = int(y) if y is not None else None
    except: y = None
    return x, y

def _shot_type(p: dict) -> str:
    d = p.get("details") or {}
    st = d.get("shotType") or ""
    return str(st).strip().upper()

def _merge_key(p: dict, home_id:int, away_id:int, home_abbr:str, away_abbr:str) -> tuple:
    """
    Build a stable key across sources:
      (period, time, side, x, y, shotType)
    """
    per  = _period_num(p) or -1
    t    = _time_in_period(p) or ""
    side = _play_team_side(p, home_id, away_id, home_abbr, away_abbr) or "UNK"
    x, y = _xy_tuple(p)
    st   = _shot_type(p)
    return (per, t, side, x, y, st)

def _normalize_sog_plays(pbp_obj, home_id, away_id, home_abbr, away_abbr) -> List[dict]:
    """Return list of dicts: {'key', 'pid', 'side', 'hs', 'aw'} for SOG-like events."""
    out = []
    for p in _plays_list(pbp_obj):
        if not _is_sog_like(p):
            continue
        pid  = _shooter_id(p)
        side = _play_team_side(p, home_id, away_id, home_abbr, away_abbr)
        if side is None:
            continue
        hs, aw = _sit_counts(p)
        out.append({
            "key": _merge_key(p, home_id, away_id, home_abbr, away_abbr),
            "pid": pid,
            "side": side,
            "hs": hs, "aw": aw
        })
    return out

def sog_map_for_game(cur, gid: int) -> Dict[int, int]:
    cur.execute("""
        SELECT player_id, shots_on_goal
        FROM nhl.skater_game_logs_raw
        WHERE game_id = %s AND shots_on_goal IS NOT NULL AND shots_on_goal > 0
    """, (gid,))
    return {int(pid): int(sog) for pid, sog in cur.fetchall()}

def merge_pbp_for_sog(pbp_web, pbp_stats, home_id, away_id, home_abbr, away_abbr) -> List[dict]:
    """
    Merge api-web + statsapi SOG-like plays by composite key; fill missing shooter from the other source.
    """
    web_norm   = _normalize_sog_plays(pbp_web,   home_id, away_id, home_abbr, away_abbr) if pbp_web else []
    stats_norm = _normalize_sog_plays(pbp_stats, home_id, away_id, home_abbr, away_abbr) if pbp_stats else []

    merged = { r["key"]: r for r in web_norm }
    for r in stats_norm:
        k = r["key"]
        if k in merged:
            if merged[k]["pid"] is None and r["pid"] is not None:
                merged[k]["pid"] = r["pid"]
            # keep existing side/hs/aw from either; they should agree
        else:
            merged[k] = r
    return list(merged.values())

def compute_skater_sog_splits_from_norm(norm_plays: List[dict]) -> Dict[int, Dict[str, int]]:
    out: Dict[int, Dict[str, int]] = {}
    for r in norm_plays:
        pid = r.get("pid")
        if pid is None:
            continue  # cannot attribute to a player
        hs, aw = r.get("hs"), r.get("aw")
        shoot_home = (r.get("side") == "HOME")
        lab = _strength(shoot_home, hs, aw)
        d = out.setdefault(int(pid), {"EV": 0, "PP": 0, "SH": 0})
        d[lab] += 1
    return out

# ───────────────────────── main backfill loop ─────────────────────────

def backfill(batch_size: int, delay: float, commit: bool = False, resume: bool = False):
    DB = env_db_url()
    with psycopg.connect(DB) as conn, conn.cursor() as cur:
        start_remaining = offenders_count(cur)
        print(f"[bf] starting remaining={start_remaining}")

        ensure_progress_table(cur)
        # If not resuming, start from the beginning each run
        if not resume:
            set_last_game_id(cur, None)
        conn.commit()

    # loop until no offenders remain
    while True:
        with psycopg.connect(DB) as conn, conn.cursor() as cur:
            ensure_progress_table(cur)
            last_gid = get_last_game_id(cur)

            # page of offender games
            game_ids = find_offender_games(cur, batch_size, last_gid)
            if not game_ids:
                if last_gid is None:
                    print("✅ No offending games remain. Done.")
                    return
                # wrap to the beginning once
                print("↻ Wrapped to beginning (no offenders after last checkpoint).")
                set_last_game_id(cur, None)
                conn.commit()
                continue

            print(f"[page] processing {len(game_ids)} games {min(game_ids)}–{max(game_ids)} (resume after {last_gid})")

            for gid in game_ids:
                updated_rows = 0
                # fetch boxscore
                box = fetch_json(API_WEB_BOX.format(gid=gid))
                if not isinstance(box, dict):
                    print(f"[{gid}] box fetch failed; skipping")
                    set_last_game_id(cur, gid)
                    conn.commit()
                    continue

                home = (box.get("homeTeam") or {})
                away = (box.get("awayTeam") or {})
                home_abbr = (home.get("abbrev") or home.get("teamAbbrev") or "").upper()
                away_abbr = (away.get("abbrev") or away.get("teamAbbrev") or "").upper()
                home_id = to_int(home.get("id")); away_id = to_int(away.get("id"))
                if not home_id or not away_id:
                    print(f"[{gid}] missing team ids; skipping")
                    set_last_game_id(cur, gid)
                    conn.commit()
                    continue

                # fetch PBP (api-web AND statsapi), then merge
                pbp_web   = fetch_json(API_WEB_PBP.format(gid=gid))
                pbp_stats = fetch_json(STATSAPI_FEED.format(gid=gid))

                norm = merge_pbp_for_sog(pbp_web, pbp_stats, home_id, away_id, home_abbr, away_abbr)
                if not norm:
                    print(f"[{gid}] no sog-like plays after merge; skipping")
                    set_last_game_id(cur, gid); conn.commit(); continue

                sk_splits = compute_skater_sog_splits_from_norm(norm)
                if not sk_splits:
                    print(f"[{gid}] no skater splits derived; skipping")
                    set_last_game_id(cur, gid); conn.commit(); continue

                norm_count = len(norm or [])
                shooters_count = len(sk_splits or {})

                # --- build updates only where our total matches DB SOG ---
                # --- reconcile against DB SOG: fill any shortfall into EV, skip excess ---
            sog_by_pid = sog_map_for_game(cur, gid)  # {pid: shots_on_goal} you already have this

            updates: List[tuple] = []  # (ev, pp, sh, gid, pid)
            stats_ok = stats_filled = stats_excess = 0

            for pid, sog in sog_by_pid.items():
                if not sog or sog <= 0:
                    continue
                d = sk_splits.get(pid, {"EV": 0, "PP": 0, "SH": 0})
                ev, pp, sh = int(d.get("EV", 0)), int(d.get("PP", 0)), int(d.get("SH", 0))
                total = ev + pp + sh

                if total == sog:
                    stats_ok += 1
                    # use as-is
                elif total < sog:
                    # undercount in PBP: fill remainder into EV
                    ev += (sog - total)
                    total = sog
                    stats_filled += 1
                else:
                    # overcount/noisy PBP: skip to avoid corrupting data
                    stats_excess += 1
                    continue

                updates.append((ev, pp, sh, gid, pid))

            updated_rows = 0
            if commit:
                # Update only rows that are offenders (nulls or sum mismatch)
                for ev, pp, sh, g_id, p_id in updates:
                    cur.execute(f"""
                        UPDATE nhl.skater_game_logs_raw
                        SET ev_sog = %s, pp_sog = %s, sh_sog = %s
                        WHERE game_id = %s AND player_id = %s
                        AND (
                            ev_sog IS NULL OR pp_sog IS NULL OR sh_sog IS NULL
                            OR (COALESCE(ev_sog,0)+COALESCE(pp_sog,0)+COALESCE(sh_sog,0)) <> shots_on_goal
                        )
                    """, (ev, pp, sh, g_id, p_id))
                    updated_rows += cur.rowcount
                print(f"[{gid}] COMMIT updated_rows={updated_rows} ok={stats_ok} filled_ev={stats_filled} skipped_excess={stats_excess} sog_rows={len(sog_by_pid)}")
            else:
                # Dry-run: count how many would update
                would = 0
                for ev, pp, sh, g_id, p_id in updates:
                    cur.execute(f"""
                        SELECT 1
                        FROM nhl.skater_game_logs_raw
                        WHERE game_id = %s AND player_id = %s
                        AND (
                            ev_sog IS NULL OR pp_sog IS NULL OR sh_sog IS NULL
                            OR (COALESCE(ev_sog,0)+COALESCE(pp_sog,0)+COALESCE(sh_sog,0)) <> shots_on_goal
                        )
                        LIMIT 1
                    """, (g_id, p_id))
                    if cur.fetchone():
                        would += 1
                conn.rollback()
                print(f"[{gid}] DRY-RUN would_update={would} ok={stats_ok} filled_ev={stats_filled} skipped_excess={stats_excess} sog_rows={len(sog_by_pid)}")

            # advance checkpoint & commit each game
            set_last_game_id(cur, gid)
            conn.commit()

            if delay > 0:
                time.sleep(delay)

            now_remaining = offenders_count(cur)
            print(f"[bf] remaining={now_remaining}")


        # loop continues to next page until no offenders remain

# ───────────────────────── CLI ─────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Backfill EV/PP/SH SOG splits into nhl.skater_game_logs_raw")
    ap.add_argument("--batch-size", type=int, default=200, help="Games per page (default 200)")
    ap.add_argument("--delay", type=float, default=0.0, help="Sleep seconds between games")
    ap.add_argument("--commit", action="store_true", help="Apply updates (default: dry-run)")
    ap.add_argument("--resume", action="store_true", help="(parsed only) We'll wire this in next step.")
    args = ap.parse_args()
    backfill(args.batch_size, args.delay, commit=args.commit, resume=args.resume)

if __name__ == "__main__":
    main()
