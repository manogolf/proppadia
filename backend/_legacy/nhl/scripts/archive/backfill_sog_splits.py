#!/usr/bin/env python3
from __future__ import annotations

import os, sys, time, argparse
from typing import Dict, List, Tuple, Optional
import requests
import psycopg
try:
    from dotenv import load_dotenv
    load_dotenv()  # loads variables from .env into process env if present
except Exception:
    pass

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

def fetch_json(url: str, timeout=12, tries=3, pause=0.7) -> Optional[dict]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/126.0 Safari/537.36"
        )
    }
    for attempt in range(tries):
        try:
            r = requests.get(url, timeout=timeout, headers=headers)
            r.raise_for_status()
            return r.json()
        except Exception:
            if attempt == tries - 1:
                return None
            time.sleep(pause)
    
def offenders_count(cur) -> int:
    cur.execute(f"SELECT COUNT(*) FROM nhl.skater_game_logs_raw WHERE {OFFENDER_NULLS_PRED}")
    return cur.fetchone()[0]

# ───────────────────────── offenders & progress ─────────────────────────

OFFENDER_PRED = """
shots_on_goal > 0 AND (
  ev_sog IS NULL OR pp_sog IS NULL OR sh_sog IS NULL
  OR (COALESCE(ev_sog,0)+COALESCE(pp_sog,0)+COALESCE(sh_sog,0)) <> shots_on_goal
)
"""

# rows we can actually fix: any split is NULL (regardless of current total mismatch)
OFFENDER_NULLS_PRED = """
(ev_sog IS NULL OR pp_sog IS NULL OR sh_sog IS NULL)
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
            WHERE {OFFENDER_NULLS_PRED}
            ORDER BY game_id
            LIMIT %s
        """, (limit,))
    else:
        cur.execute(f"""
            SELECT DISTINCT game_id
            FROM nhl.skater_game_logs_raw
            WHERE game_id > %s AND {OFFENDER_NULLS_PRED}
            ORDER BY game_id
            LIMIT %s
        """, (min_game_id, limit))
    return [r[0] for r in cur.fetchall()]

def map_nhl_to_internal(cur, nhl_ids: List[int]) -> dict[int, int]:
    """
    Map NHL player ids -> internal player_id.

    Strategy:
      1) nhl.player_external_ids using auto-detected external id column
         and provider in {'nhl','api-web','statsapi','web'} (or whatever exists).
      2) Fallback: nhl.players using any plausible NHL id column.
    """
    if not nhl_ids:
        return {}
    nhl_ids = list({int(i) for i in nhl_ids if i is not None})

    # ---- discover columns/providers in player_external_ids ----
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='nhl' AND table_name='player_external_ids'
    """)
    pei_cols = {r[0] for r in cur.fetchall()}

    # external id column candidates
    pei_ext_candidates = [
        "external_id",
        "provider_player_id",
        "nhl_player_id",
        "player_nhl_id",
        "ext_id",
        "source_id",
    ]
    pei_ext_col = next((c for c in pei_ext_candidates if c in pei_cols), None)
    if pei_ext_col is None:
        pei_ext_col = next((c for c in pei_cols if c.endswith("_id") and c not in {"player_id", "game_id", "team_id"}), None)

    # gather providers present; restrict to nhl-like first
    cur.execute("SELECT DISTINCT provider FROM nhl.player_external_ids")
    providers_found = [r[0] for r in cur.fetchall()]
    preferred_providers = [p for p in providers_found if str(p).lower() in {"nhl", "api-web", "statsapi", "web"}]
    use_providers = preferred_providers or providers_found  # if no preferred, try all

    mapping: dict[int, int] = {}

    # ---- query player_external_ids if usable ----
    if pei_ext_col:
        sql = f"""
            SELECT CAST({pei_ext_col} AS bigint) AS nhl_id, player_id
            FROM nhl.player_external_ids
            WHERE provider = ANY(%s) AND CAST({pei_ext_col} AS bigint) = ANY(%s)
        """
        cur.execute(sql, (use_providers, nhl_ids))
        for nhl_id, pid in cur.fetchall():
            if nhl_id is not None and pid is not None:
                mapping[int(nhl_id)] = int(pid)

    # Early exit if we covered all
    if len(mapping) == len(nhl_ids):
        return mapping

    # ---- fallback: check nhl.players for a plausible NHL id column ----
    cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='nhl' AND table_name='players'
    """)
    p_cols = {r[0] for r in cur.fetchall()}
    players_ext_candidates = [
        "nhl_player_id",
        "nhl_id",
        "player_nhl_id",
        "external_id_nhl",
        "provider_player_id",
    ]
    players_ext_col = next((c for c in players_ext_candidates if c in p_cols), None)
    if players_ext_col is None:
        players_ext_col = next((c for c in p_cols if c.endswith("_nhl_id") or c == "external_id"), None)

    if players_ext_col:
        sql = f"""
            SELECT CAST({players_ext_col} AS bigint) AS nhl_id, player_id
            FROM nhl.players
            WHERE CAST({players_ext_col} AS bigint) = ANY(%s)
        """
        cur.execute(sql, (nhl_ids,))
        for nhl_id, pid in cur.fetchall():
            if nhl_id is not None and pid is not None and nhl_id not in mapping:
                mapping[int(nhl_id)] = int(pid)

    return mapping


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
    """
    Return True for shots-on-goal and goals, handling both api-web and statsapi shapes.
    Excludes shootout by default.
    """
    d = play.get("details") or {}

    # --- exclude shootout plays (api-web often flags SO via periodDescriptor.periodType) ---
    pd = play.get("periodDescriptor") or {}
    pt = (pd.get("periodType") or "").upper()
    if pt == "SO":
        return False

    # --- explicit flags (api-web) ---
    if d.get("isGoal") is True:
        return True
    if d.get("shotOnGoal") is True:
        return True

    # --- canonical event type where available ---
    et = (_event_type(play) or "").upper()
    if et in {"SHOT", "SHOT-ON-GOAL", "SHOT_ON_GOAL", "GOAL"}:
        return True

    # --- additional fallbacks for api-web/statsapi variants ---
    # Sometimes event type id/name lives under different keys
    for key in ("eventTypeId", "eventType", "type", "event"):
        v = play.get(key) or d.get(key)
        if isinstance(v, str) and v.strip().upper() in {"SHOT", "GOAL"}:
            return True

    # Some api-web payloads only have a numeric 'typeCode' but still mark shots with
    # a 'shotType' or shooter in 'details'. If we have a shooter and not a block/miss hint,
    # allow it when there's an explicit on-goal hint.
    # (Deliberately conservative to avoid counting MISSED/BLOCKED.)
    if d.get("shootingPlayerId") or d.get("playerId"):
        on_goal_hint = d.get("shotOnGoal") is True or d.get("isGoal") is True
        if on_goal_hint:
            return True

    return False

def _shooter_id(play: dict):
    """
    Return the NHL personId for the shooter/scorer.

    api-web variants seen in the wild include (non-exhaustive):
      details.shootingPlayerId, details.scoringPlayerId, details.shooterId,
      details.scorerId, details.primaryPlayerId, details.byPlayerId,
      details.playerId

    statsapi fallback:
      players[].player.id where players[].playerType in ('Shooter','Scorer')
    """
    d = play.get("details") or {}

    # Try the common api-web keys in order
    for k in (
        "shootingPlayerId",
        "scoringPlayerId",
        "shooterId",
        "scorerId",
        "primaryPlayerId",
        "byPlayerId",
        "playerId",
    ):
        pid = d.get(k)
        if pid is not None:
            try:
                return int(pid)
            except Exception:
                pass  # keep trying other fields

    # statsapi fallback (used by /feed/live and some api-web hybrids)
    for pl in (play.get("players") or []):
        ptype = (pl.get("playerType") or "").strip().lower()
        if ptype in ("shooter", "scorer"):
            pid = (pl.get("player") or {}).get("id") or pl.get("playerId")
            if pid is not None:
                try:
                    return int(pid)
                except Exception:
                    continue

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

def _strength(shoot_home: bool, hs: int | None, as_: int | None) -> str:
    # Unknown counts → best guess is EV
    if hs is None or as_ is None:
        return "EV"
    if hs == as_:
        return "EV"
    # If the shooting side has more skaters => PP, else SH
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


# ───────────────────────── main backfill loop ─────────────────────────

def backfill(batch_size: int, delay: float, commit: bool = False, resume: bool = False):
    """
    Iterate through games that have NULL split fields (EV/PP/SH) and try to fill them by
    parsing PBP. We only write rows where our EV+PP+SH == skater_game_logs_raw.shots_on_goal.
    """
    DB = env_db_url()
    with psycopg.connect(DB) as conn, conn.cursor() as cur:
        start_remaining = offenders_count(cur)
        print(f"[bf] starting remaining={start_remaining}")

        ensure_progress_table(cur)
        if not resume:
            set_last_game_id(cur, None)
        conn.commit()

    while True:
        with psycopg.connect(DB) as conn, conn.cursor() as cur:
            ensure_progress_table(cur)
            last_gid = get_last_game_id(cur)

            game_ids = find_offender_games(cur, batch_size, last_gid)
            if not game_ids:
                if last_gid is None:
                    print("✅ No offending games remain. Done.")
                    return
                print("↻ Wrapped to beginning (no offenders after last checkpoint).")
                set_last_game_id(cur, None)
                conn.commit()
                continue

            print(f"[page] processing {len(game_ids)} games {min(game_ids)}–{max(game_ids)} (resume after {last_gid})")

            for gid in game_ids:
                # ---- boxscore (team ids/abbrs) ----
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
                home_id = to_int(home.get("id"))
                away_id = to_int(away.get("id"))
                if not home_id or not away_id:
                    print(f"[{gid}] missing team ids; skipping")
                    set_last_game_id(cur, gid)
                    conn.commit()
                    continue

                # ---- fetch both PBP sources (inside loop!) ----
                pbp_web   = fetch_json(API_WEB_PBP.format(gid=gid))
                pbp_stats = fetch_json(STATSAPI_FEED.format(gid=gid))

                plays_web   = plays_list(pbp_web)
                plays_stats = plays_list(pbp_stats)

                if not plays_web and not plays_stats:
                    print(f"[{gid}] no plays; skipping")
                    set_last_game_id(cur, gid)
                    conn.commit()
                    continue

                # Prefer StatsAPI if any SHOT/GOAL has a strength code
                use_stats = False
                if plays_stats:
                    for p in plays_stats:
                        r = p.get("result") or {}
                        et = (r.get("eventTypeId") or "").upper()
                        if et in ("SHOT", "GOAL") and (r.get("strength") or {}).get("code"):
                            use_stats = True
                            break

                if use_stats:
                    sk_splits, used = compute_skater_sog_splits_statsapi(plays_stats, home_abbr, away_abbr)
                    plays_count = len(plays_stats)
                else:
                    sk_splits = compute_skater_sog_splits(pbp_web or {}, home_id, away_id, home_abbr, away_abbr)
                    used = sum(sum(v.values()) for v in sk_splits.values())
                    plays_count = len(plays_web)

                if not sk_splits:
                    print(f"[{gid}] no skater splits derived; skipping")
                    set_last_game_id(cur, gid)
                    conn.commit()
                    continue

                # ---- map NHL ids -> internal player_id ----
                nhl_ids = list(sk_splits.keys())
                id_map = map_nhl_to_internal(cur, nhl_ids)

                missing = [x for x in nhl_ids if x not in id_map]
                if missing:
                    print(f"[{gid}] note: {len(missing)} NHL ids lack mapping to internal player_id; skipping those.")

                # ---- fetch per-game SOG targets (only rows with NULL splits & SOG > 0) ----
                cur.execute("""
                    SELECT player_id, shots_on_goal
                    FROM nhl.skater_game_logs_raw
                    WHERE game_id = %s
                      AND (ev_sog IS NULL OR pp_sog IS NULL OR sh_sog IS NULL)
                      AND shots_on_goal > 0
                """, (gid,))
                sog_target = {int(pid): int(sog) for pid, sog in cur.fetchall()}

                # ---- build params guarded by mapping AND target match ----
                from typing import List
                unmapped: list[int] = []
                mismatches: list[tuple[int, int, int]] = []  # (internal_pid, ours_total, target_sog)
                params: List[tuple] = []

                for nhl_pid, d in sk_splits.items():
                    internal_pid = id_map.get(nhl_pid)
                    if internal_pid is None:
                        unmapped.append(nhl_pid)
                        continue

                    ev = int(d.get("EV", 0)); pp = int(d.get("PP", 0)); sh = int(d.get("SH", 0))
                    total = ev + pp + sh

                    tgt = sog_target.get(internal_pid)
                    if tgt is None:
                        # no eligible row to fill (either not in game rows, or splits not NULL, or SOG=0)
                        continue

                    if total != tgt:
                        mismatches.append((internal_pid, total, tgt))
                        continue

                    # Eligible: totals match; only fill NULLs in the UPDATE
                    params.append((ev, pp, sh, gid, internal_pid))

                if unmapped:
                    print(f"[{gid}] note: {len(unmapped)} NHL ids lack mapping to internal player_id; skipping those.")

                if mismatches:
                    sample = ", ".join([f"pid={p} ours={o} tgt={t}" for p, o, t in mismatches[:6]])
                    more = f" (+{len(mismatches)-6} more)" if len(mismatches) > 6 else ""
                    print(f"[{gid}] split≠target: {len(mismatches)} rows differ; {sample}{more}")

                # ---- write or dry-run ----
                updated_rows = 0
                if commit:
                    if params:
                        cur.executemany("""
                            UPDATE nhl.skater_game_logs_raw
                            SET ev_sog = COALESCE(ev_sog, %s),
                                pp_sog = COALESCE(pp_sog, %s),
                                sh_sog = COALESCE(sh_sog, %s)
                            WHERE game_id = %s AND player_id = %s
                              AND (ev_sog IS NULL OR pp_sog IS NULL OR sh_sog IS NULL)
                        """, params)
                        updated_rows = cur.rowcount
                    print(f"[{gid}] COMMIT updated_rows={updated_rows} plays={plays_count} shooters={len(sk_splits)} used={used}")
                else:
                    updated_rows = len(params)
                    conn.rollback()
                    print(f"[{gid}] DRY-RUN would_update={updated_rows} plays={plays_count} shooters={len(sk_splits)} used={used}")

                # ---- checkpoint & progress ----
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
