#!/usr/bin/env python3
from __future__ import annotations

import os, time, argparse
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import requests
import psycopg

# ─────────────── config ───────────────

API_WEB_PBP   = "https://api-web.nhle.com/v1/gamecenter/{gid}/play-by-play"
API_WEB_BOX   = "https://api-web.nhle.com/v1/gamecenter/{gid}/boxscore"
STATSAPI_FEED = "https://statsapi.web.nhl.com/api/v1/game/{gid}/feed/live"

TASK_KEY = "goalie_splits_v1"


def _load_env_upwards():
    """Load .env from current dir or any parent (first hit wins)."""
    if os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL"):
        return
    try:
        from dotenv import load_dotenv  # pip install python-dotenv
    except Exception:
        return
    here = Path(__file__).resolve()
    for parent in [here.parent, *here.parents]:
        dot = parent / ".env"
        if dot.exists():
            load_dotenv(dot)
            break

def env_db_url() -> str:
    _load_env_upwards()
    db = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
    if not db:
        raise SystemExit("Missing SUPABASE_DB_URL / DATABASE_URL")
    # Supabase pooler: require SSL and disable GSS
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

# ───────── offenders & progress ─────────

OFFENDER_PRED = """
shots_faced > 0 AND (
  ev_shots_faced IS NULL OR pp_shots_faced IS NULL OR sh_shots_faced IS NULL
  OR (COALESCE(ev_shots_faced,0)+COALESCE(pp_shots_faced,0)+COALESCE(sh_shots_faced,0)) <> shots_faced
)
"""

def offenders_count(cur) -> int:
    cur.execute(f"""
        SELECT COUNT(*) FROM nhl.goalie_game_logs_raw
        WHERE {OFFENDER_PRED}
    """)
    return cur.fetchone()[0]

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
        ON CONFLICT (task) DO UPDATE
          SET last_game_id = EXCLUDED.last_game_id, updated_at = now()
    """, (TASK_KEY, gid))

def find_offender_games(cur, limit: int, min_game_id: Optional[int]) -> List[int]:
    if min_game_id is None:
        cur.execute(f"""
            SELECT DISTINCT game_id
            FROM nhl.goalie_game_logs_raw
            WHERE {OFFENDER_PRED}
            ORDER BY game_id
            LIMIT %s
        """, (limit,))
    else:
        cur.execute(f"""
            SELECT DISTINCT game_id
            FROM nhl.goalie_game_logs_raw
            WHERE game_id > %s AND {OFFENDER_PRED}
            ORDER BY game_id
            LIMIT %s
        """, (min_game_id, limit))
    return [r[0] for r in cur.fetchall()]

# ───────── PBP helpers ─────────

def _plays_list(pbp_obj) -> list:
    if isinstance(pbp_obj, list):
        return pbp_obj
    if isinstance(pbp_obj, dict):
        if isinstance(pbp_obj.get("plays"), list):  # api-web
            return pbp_obj["plays"]
        pby = pbp_obj.get("playByPlay")
        if isinstance(pby, dict):
            if isinstance(pby.get("allPlays"), list):
                return pby["allPlays"]
            if isinstance(pby.get("plays"), list):
                return pby["plays"]
        live = pbp_obj.get("liveData")  # statsapi
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
    for k in ("typeCode","eventCode","eventTypeId"):
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
    return et in ("SHOT","SHOT-ON-GOAL","SHOT_ON_GOAL","GOAL")

def _sit_counts(play: dict) -> Tuple[Optional[int], Optional[int]]:
    code = play.get("situationCode")
    if isinstance(code, str) and "v" in code:
        try:
            a, b = code.split("v", 1)
            return int(a), int(b)
        except Exception:
            pass
    if isinstance(code, str) and len(code) == 4 and code.isdigit():
        try:
            h = int(code[1:3]); a = int(code[2:4])
            return h, a
        except Exception:
            pass
    return None, None

def _strength(shoot_home: bool, hs: Optional[int], aw: Optional[int]) -> str:
    if hs is None or aw is None:
        return "EV"
    if hs == aw:
        return "EV"
    return "PP" if ((shoot_home and hs > aw) or ((not shoot_home) and aw > hs)) else "SH"

def _play_team_side(play: dict, home_id: int, away_id: int, home_abbr: str, away_abbr: str) -> Optional[str]:
    d = play.get("details") or {}
    tid = to_int(d.get("eventOwnerTeamId"))
    if tid == home_id:
        return "HOME"
    if tid == away_id:
        return "AWAY"
    ta = (d.get("teamAbbrev") or "").upper()
    if ta == home_abbr:
        return "HOME"
    if ta == away_abbr:
        return "AWAY"
    t2 = play.get("team") or {}
    if isinstance(t2, dict):
        ab = (t2.get("triCode") or t2.get("abbrev") or "").upper()
        if ab == home_abbr:
            return "HOME"
        if ab == away_abbr:
            return "AWAY"
    return None

def _goalie_ids_from_box(box: dict) -> Tuple[List[int], List[int]]:
    def ids(side: str) -> List[int]:
        out: List[int] = []
        pbg = (box.get("playerByGameStats") or {}).get(side) or {}
        for p in pbg.get("goalies", []) or []:
            pid = to_int(p.get("playerId"))
            if pid: out.append(pid)
        team = box.get(side) or {}
        for g in team.get("goalies", []) or []:
            pid = to_int(g.get("playerId") or g.get("id"))
            if pid: out.append(pid)
        players = team.get("players")
        if isinstance(players, dict):
            for pid_s, pdata in players.items():
                pid = to_int(pid_s)
                pos = (pdata.get("positionCode") or pdata.get("position") or "").upper()
                if pid and pos == "G": out.append(pid)
        return list(dict.fromkeys(out))  # dedupe, preserve order
    return ids("homeTeam"), ids("awayTeam")

# ───────── derive goalie EV/PP/SH shots faced ─────────

def compute_goalie_sog_splits(
    pbp_obj, home_id: int, away_id: int, home_abbr: str, away_abbr: str,
    home_goalie_ids: List[int], away_goalie_ids: List[int]
) -> Dict[int, Dict[str,int]]:
    """
    Return { goalie_id: {'EV': e, 'PP': p, 'SH': s} }.
    Primary key is details.goalieInNetId (api-web). Fallbacks:
      • statsapi 'players' array with playerType 'goalie'
      • single-goalie-per-team fallback if we can’t read the goalie on a shot
    """
    out: Dict[int, Dict[str,int]] = {}
    used = 0
    plays = _plays_list(pbp_obj)
    for p in plays:
        if not _is_sog_like(p):
            continue

        d = p.get("details") or {}
        gid = to_int(d.get("goalieInNetId"))
        side = _play_team_side(p, home_id, away_id, home_abbr, away_abbr)  # shooter side
        if gid is None:
            # statsapi fallback: find goalie in players array
            for pl in p.get("players", []) or []:
                if (pl.get("playerType") or "").lower() == "goalie":
                    gid = to_int((pl.get("player") or {}).get("id") or pl.get("playerId"))
                    if gid: break
        if gid is None and side is not None:
            # last-ditch: if only one goalie listed for that defending team, attribute to them
            if side == "HOME" and len(away_goalie_ids) == 1:
                gid = away_goalie_ids[0]
            elif side == "AWAY" and len(home_goalie_ids) == 1:
                gid = home_goalie_ids[0]

        if gid is None or side is None:
            continue

        hs, aw = _sit_counts(p)
        lab = _strength(side == "HOME", hs, aw)   # strength from SHOOTER perspective
        row = out.setdefault(gid, {"EV": 0, "PP": 0, "SH": 0})
        row[lab] += 1
        used += 1

    # print(f"[dbg] goalie split sog-faced used={used} goalies={len(out)}")
    return out

# ───────── main backfill loop ─────────

def backfill(batch_size: int, delay: float, commit: bool = False):
    DB = env_db_url()

    with psycopg.connect(DB) as conn, conn.cursor() as cur:
        start_remaining = offenders_count(cur)
        print(f"[bf-G] starting remaining={start_remaining}")
        ensure_progress_table(cur)
        conn.commit()

    while True:
        with psycopg.connect(DB) as conn, conn.cursor() as cur:
            ensure_progress_table(cur)
            last_gid = get_last_game_id(cur)

            game_ids = find_offender_games(cur, batch_size, last_gid)
            if not game_ids:
                if last_gid is None:
                    print("✅ No offending goalie rows remain. Done.")
                    return
                print("↻ Wrapped to beginning (no offenders after last checkpoint).")
                set_last_game_id(cur, None)
                conn.commit()
                continue

            print(f"[page-G] processing {len(game_ids)} games {min(game_ids)}–{max(game_ids)} (resume after {last_gid})")

            for gid in game_ids:
                updated_rows = 0

                box = fetch_json(API_WEB_BOX.format(gid=gid))
                if not isinstance(box, dict):
                    print(f"[{gid}] box fetch failed; skip")
                    set_last_game_id(cur, gid); conn.commit(); continue

                home = (box.get("homeTeam") or {})
                away = (box.get("awayTeam") or {})
                home_abbr = (home.get("abbrev") or home.get("teamAbbrev") or "").upper()
                away_abbr = (away.get("abbrev") or away.get("teamAbbrev") or "").upper()
                home_id = to_int(home.get("id")); away_id = to_int(away.get("id"))
                if not home_id or not away_id:
                    print(f"[{gid}] missing team ids; skip")
                    set_last_game_id(cur, gid); conn.commit(); continue

                home_goalies, away_goalies = _goalie_ids_from_box(box)

                pbp = fetch_json(API_WEB_PBP.format(gid=gid)) or fetch_json(STATSAPI_FEED.format(gid=gid))
                plays = _plays_list(pbp)
                if not plays:
                    print(f"[{gid}] no plays; skip")
                    set_last_game_id(cur, gid); conn.commit(); continue

                gl_splits = compute_goalie_sog_splits(
                    pbp, home_id, away_id, home_abbr, away_abbr,
                    home_goalies, away_goalies
                )
                if not gl_splits:
                    print(f"[{gid}] no goalie splits; skip")
                    set_last_game_id(cur, gid); conn.commit(); continue

                # per-goalie updates (commit-aware)
                if commit:
                    for gpid, d in gl_splits.items():
                        ev, pp, sh = int(d.get("EV", 0)), int(d.get("PP", 0)), int(d.get("SH", 0))
                        cur.execute(f"""
                            UPDATE nhl.goalie_game_logs_raw
                            SET ev_shots_faced = %s, pp_shots_faced = %s, sh_shots_faced = %s
                            WHERE game_id = %s AND player_id = %s
                              AND {OFFENDER_PRED}
                              AND (%s + %s + %s) = shots_faced
                        """, (ev, pp, sh, gid, gpid, ev, pp, sh))
                        updated_rows += cur.rowcount
                    print(f"[{gid}] COMMIT updated_rows={updated_rows} goalies_seen={len(gl_splits)}")
                else:
                    would = 0
                    for gpid, d in gl_splits.items():
                        ev, pp, sh = int(d.get("EV", 0)), int(d.get("PP", 0)), int(d.get("SH", 0))
                        cur.execute(f"""
                            SELECT COUNT(*) FROM nhl.goalie_game_logs_raw
                            WHERE game_id = %s AND player_id = %s
                              AND {OFFENDER_PRED}
                              AND (%s + %s + %s) = shots_faced
                        """, (gid, gpid, ev, pp, sh))
                        would += cur.fetchone()[0]
                    updated_rows = would
                    conn.rollback()
                    print(f"[{gid}] DRY-RUN would_update={updated_rows} goalies_seen={len(gl_splits)}")

                set_last_game_id(cur, gid)
                conn.commit()

                if delay > 0:
                    time.sleep(delay)

                cur.execute(f"SELECT COUNT(*) FROM nhl.goalie_game_logs_raw WHERE {OFFENDER_PRED}")
                rem = cur.fetchone()[0]
                print(f"[bf-G] remaining={rem}")

# ───────── CLI ─────────

def main():
    ap = argparse.ArgumentParser(description="Backfill EV/PP/SH shots-faced splits into nhl.goalie_game_logs_raw")
    ap.add_argument("--batch-size", type=int, default=200, help="Games per page (default 200)")
    ap.add_argument("--delay", type=float, default=0.0, help="Sleep seconds between games")
    ap.add_argument("--commit", action="store_true", help="Apply updates (default: dry-run)")
    ap.add_argument("--resume", action="store_true", help="(parsed only; resume is automatic via checkpoint)")
    args = ap.parse_args()
    backfill(args.batch_size, args.delay, commit=args.commit)

if __name__ == "__main__":
    main()
