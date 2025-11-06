#!/usr/bin/env python3
"""
load_skaters_season_situation.py

Loads your master Skaters CSV (season totals by player *and* situation) into Postgres,
keeping only the fields we want for the project and computing per-60 & TOI share features.

Usage:
  python backend/nhl/scripts/load_skaters_season_situation.py \
    --csv backend/nhl/data/skaters2023.csv \
    --db "$SUPABASE_DB_URL" \
    --season 2023, 2024, 2025 to-date \
    --create-table   # run once to (idempotently) create schema/table

Notes
- Expects columns shown in your sample (playerId, season, team, position, situation, icetime, etc.).
- Treats `icetime` as SECONDS. Per-60 rates are 3600 * metric / icetime_seconds.
- Primary key = (player_id, season, situation). We upsert on that key.
- TOI share fields are computed per player-season using that player's 'all' row as the denominator.
"""

from __future__ import annotations
import argparse, csv, os, sys, math
from collections import defaultdict
from typing import Dict, Any, List, Tuple, Optional

import psycopg
from psycopg.rows import dict_row

# ------------ Helpers ------------

def _f(v) -> float:
    try:
        if v is None or v == "":
            return 0.0
        return float(v)
    except Exception:
        return 0.0

def _i(v) -> int:
    try:
        if v is None or v == "":
            return 0
        return int(float(v))
    except Exception:
        return 0

def _per60(val: float, icetime_s: float) -> float:
    if icetime_s <= 0: return 0.0
    return 3600.0 * float(val) / float(icetime_s)

def _norm_situation(s: str) -> str:
    s = (s or "").strip().lower()
    if s in ("5on5", "5v5", "ev"): return "5on5"
    if s in ("5on4", "pp"): return "5on4"
    if s in ("4on5", "pk"): return "4on5"
    if s in ("all", "total", "totals"): return "all"
    return "other"

# ------------ SQL DDL ------------

DDL = """
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'nhl_ref') THEN
    EXECUTE 'CREATE SCHEMA nhl_ref';
  END IF;
END$$;

CREATE TABLE IF NOT EXISTS nhl_ref.skaters_season_situation (
  player_id      BIGINT NOT NULL,
  season         INT    NOT NULL,
  team           TEXT   NOT NULL,
  position       TEXT   NOT NULL,
  situation      TEXT   NOT NULL,  -- one of: all, 5on5, 5on4, 4on5, other
  -- raw
  games_played   INT,
  icetime_s      DOUBLE PRECISION,
  shifts         DOUBLE PRECISION,
  game_score     DOUBLE PRECISION,
  -- production
  points         DOUBLE PRECISION,
  goals          DOUBLE PRECISION,
  a1             DOUBLE PRECISION,
  a2             DOUBLE PRECISION,
  -- volume
  sog            DOUBLE PRECISION,
  cf             DOUBLE PRECISION,
  xg             DOUBLE PRECISION,
  -- danger & rebounds
  ld_shots       DOUBLE PRECISION,
  md_shots       DOUBLE PRECISION,
  hd_shots       DOUBLE PRECISION,
  ld_xg          DOUBLE PRECISION,
  md_xg          DOUBLE PRECISION,
  hd_xg          DOUBLE PRECISION,
  rebounds       DOUBLE PRECISION,
  rebound_goals  DOUBLE PRECISION,
  xg_from_rebounds DOUBLE PRECISION,
  rebound_xg     DOUBLE PRECISION,
  -- on-ice context
  on_ice_for_sog       DOUBLE PRECISION,
  on_ice_for_corsi     DOUBLE PRECISION,
  on_ice_for_xg        DOUBLE PRECISION,
  on_ice_against_sog   DOUBLE PRECISION,
  on_ice_against_corsi DOUBLE PRECISION,
  on_ice_against_xg    DOUBLE PRECISION,
  -- derived
  toi_min        DOUBLE PRECISION,
  points60       DOUBLE PRECISION,
  goals60        DOUBLE PRECISION,
  sog60          DOUBLE PRECISION,
  cf60           DOUBLE PRECISION,
  xg60           DOUBLE PRECISION,
  rebounds60     DOUBLE PRECISION,
  ld_xg60        DOUBLE PRECISION,
  md_xg60        DOUBLE PRECISION,
  hd_xg60        DOUBLE PRECISION,
  ev_toi_share   DOUBLE PRECISION,
  pp_toi_share   DOUBLE PRECISION,
  pk_toi_share   DOUBLE PRECISION,
  -- misc
  created_at     TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (player_id, season, situation)
);
"""

UPSERT_SQL = """
INSERT INTO nhl_ref.skaters_season_situation (
  player_id, season, team, position, situation,
  games_played, icetime_s, shifts, game_score,
  points, goals, a1, a2,
  sog, cf, xg,
  ld_shots, md_shots, hd_shots,
  ld_xg, md_xg, hd_xg,
  rebounds, rebound_goals, xg_from_rebounds, rebound_xg,
  on_ice_for_sog, on_ice_for_corsi, on_ice_for_xg,
  on_ice_against_sog, on_ice_against_corsi, on_ice_against_xg,
  toi_min, points60, goals60, sog60, cf60, xg60, rebounds60, ld_xg60, md_xg60, hd_xg60,
  ev_toi_share, pp_toi_share, pk_toi_share
) VALUES (
  %(player_id)s, %(season)s, %(team)s, %(position)s, %(situation)s,
  %(games_played)s, %(icetime_s)s, %(shifts)s, %(game_score)s,
  %(points)s, %(goals)s, %(a1)s, %(a2)s,
  %(sog)s, %(cf)s, %(xg)s,
  %(ld_shots)s, %(md_shots)s, %(hd_shots)s,
  %(ld_xg)s, %(md_xg)s, %(hd_xg)s,
  %(rebounds)s, %(rebound_goals)s, %(xg_from_rebounds)s, %(rebound_xg)s,
  %(on_ice_for_sog)s, %(on_ice_for_corsi)s, %(on_ice_for_xg)s,
  %(on_ice_against_sog)s, %(on_ice_against_corsi)s, %(on_ice_against_xg)s,
  %(toi_min)s, %(points60)s, %(goals60)s, %(sog60)s, %(cf60)s, %(xg60)s, %(rebounds60)s, %(ld_xg60)s, %(md_xg60)s, %(hd_xg60)s,
  %(ev_toi_share)s, %(pp_toi_share)s, %(pk_toi_share)s
)
ON CONFLICT (player_id, season, situation)
DO UPDATE SET
  team = EXCLUDED.team,
  position = EXCLUDED.position,
  games_played = EXCLUDED.games_played,
  icetime_s = EXCLUDED.icetime_s,
  shifts = EXCLUDED.shifts,
  game_score = EXCLUDED.game_score,
  points = EXCLUDED.points,
  goals = EXCLUDED.goals,
  a1 = EXCLUDED.a1,
  a2 = EXCLUDED.a2,
  sog = EXCLUDED.sog,
  cf = EXCLUDED.cf,
  xg = EXCLUDED.xg,
  ld_shots = EXCLUDED.ld_shots,
  md_shots = EXCLUDED.md_shots,
  hd_shots = EXCLUDED.hd_shots,
  ld_xg = EXCLUDED.ld_xg,
  md_xg = EXCLUDED.md_xg,
  hd_xg = EXCLUDED.hd_xg,
  rebounds = EXCLUDED.rebounds,
  rebound_goals = EXCLUDED.rebound_goals,
  xg_from_rebounds = EXCLUDED.xg_from_rebounds,
  rebound_xg = EXCLUDED.rebound_xg,
  on_ice_for_sog = EXCLUDED.on_ice_for_sog,
  on_ice_for_corsi = EXCLUDED.on_ice_for_corsi,
  on_ice_for_xg = EXCLUDED.on_ice_for_xg,
  on_ice_against_sog = EXCLUDED.on_ice_against_sog,
  on_ice_against_corsi = EXCLUDED.on_ice_against_corsi,
  on_ice_against_xg = EXCLUDED.on_ice_against_xg,
  toi_min = EXCLUDED.toi_min,
  points60 = EXCLUDED.points60,
  goals60 = EXCLUDED.goals60,
  sog60 = EXCLUDED.sog60,
  cf60 = EXCLUDED.cf60,
  xg60 = EXCLUDED.xg60,
  rebounds60 = EXCLUDED.rebounds60,
  ld_xg60 = EXCLUDED.ld_xg60,
  md_xg60 = EXCLUDED.md_xg60,
  hd_xg60 = EXCLUDED.hd_xg60,
  ev_toi_share = EXCLUDED.ev_toi_share,
  pp_toi_share = EXCLUDED.pp_toi_share,
  pk_toi_share = EXCLUDED.pk_toi_share
;
"""

# ------------ Core loader ------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Path to Skaters CSV (season totals; with situation)")
    ap.add_argument("--db", "--db-url", dest="db", default=os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL"),
                    help="Postgres connection URL")
    ap.add_argument("--season", type=int, default=0, help="If provided, only load this season")
    ap.add_argument("--create-table", action="store_true", help="Create schema/table if missing (idempotent)")
    ap.add_argument("--batch", type=int, default=5000, help="Upsert batch size")
    args = ap.parse_args()

    if not args.db:
        print("Set --db or SUPABASE_DB_URL", file=sys.stderr); sys.exit(2)

    # Read CSV -> stash by (player_id, season, situation)
    rows_by_key: Dict[Tuple[int,int,str], Dict[str,Any]] = {}
    all_icetime: Dict[Tuple[int,int], float] = defaultdict(float)  # (player,season) -> icetime in 'all'
    ev_icetime: Dict[Tuple[int,int], float]  = defaultdict(float)  # 5on5
    pp_icetime: Dict[Tuple[int,int], float]  = defaultdict(float)  # 5on4
    pk_icetime: Dict[Tuple[int,int], float]  = defaultdict(float)  # 4on5

    kept = 0
    with open(args.csv, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t" if "\t" in f.readline() else ",")
        f.seek(0); reader = csv.DictReader(f)  # re-init after sniff

        for r in reader:
            try:
                season = _i(r.get("season"))
                if args.season and season != args.season:
                    continue

                situation = _norm_situation(r.get("situation"))
                if situation not in ("all","5on5","5on4","4on5"):
                    # keep the row, but it won't be used for shares; still valuable for completeness if wanted later
                    pass

                player_id = _i(r.get("playerId"))
                team      = (r.get("team") or "").strip().upper()
                position  = (r.get("position") or "").strip().upper()

                icetime_s = _f(r.get("icetime"))
                games     = _i(r.get("games_played"))
                shifts    = _f(r.get("shifts"))
                game_score= _f(r.get("gameScore"))

                points = _f(r.get("I_F_points"))
                goals  = _f(r.get("I_F_goals"))
                a1     = _f(r.get("I_F_primaryAssists"))
                a2     = _f(r.get("I_F_secondaryAssists"))

                sog    = _f(r.get("I_F_shotsOnGoal"))
                cf     = _f(r.get("I_F_shotAttempts"))
                xg     = _f(r.get("I_F_xGoals"))

                ld_sh  = _f(r.get("I_F_lowDangerShots"))
                md_sh  = _f(r.get("I_F_mediumDangerShots"))
                hd_sh  = _f(r.get("I_F_highDangerShots"))

                ld_xg  = _f(r.get("I_F_lowDangerxGoals"))
                md_xg  = _f(r.get("I_F_mediumDangerxGoals"))
                hd_xg  = _f(r.get("I_F_highDangerxGoals"))

                rebounds       = _f(r.get("I_F_rebounds"))
                rebound_goals  = _f(r.get("I_F_reboundGoals"))
                xg_from_reb    = _f(r.get("I_F_xGoalsFromxReboundsOfShots"))
                rebound_xg     = _f(r.get("I_F_reboundxGoals"))

                oi_for_sog     = _f(r.get("OnIce_F_shotsOnGoal"))
                oi_for_corsi   = _f(r.get("OnIce_F_shotAttempts"))
                oi_for_xg      = _f(r.get("OnIce_F_xGoals"))

                oi_against_sog   = _f(r.get("OnIce_A_shotsOnGoal"))
                oi_against_corsi = _f(r.get("OnIce_A_shotAttempts"))
                oi_against_xg    = _f(r.get("OnIce_A_xGoals"))

                # Derived per-60
                toi_min = icetime_s / 60.0 if icetime_s > 0 else 0.0
                row = {
                    "player_id": player_id,
                    "season": season,
                    "team": team,
                    "position": position,
                    "situation": situation,
                    "games_played": games,
                    "icetime_s": icetime_s,
                    "shifts": shifts,
                    "game_score": game_score,

                    "points": points, "goals": goals, "a1": a1, "a2": a2,
                    "sog": sog, "cf": cf, "xg": xg,

                    "ld_shots": ld_sh, "md_shots": md_sh, "hd_shots": hd_sh,
                    "ld_xg": ld_xg, "md_xg": md_xg, "hd_xg": hd_xg,

                    "rebounds": rebounds, "rebound_goals": rebound_goals,
                    "xg_from_rebounds": xg_from_reb, "rebound_xg": rebound_xg,

                    "on_ice_for_sog": oi_for_sog, "on_ice_for_corsi": oi_for_corsi, "on_ice_for_xg": oi_for_xg,
                    "on_ice_against_sog": oi_against_sog, "on_ice_against_corsi": oi_against_corsi, "on_ice_against_xg": oi_against_xg,

                    "toi_min": toi_min,
                    "points60": _per60(points, icetime_s),
                    "goals60":  _per60(goals, icetime_s),
                    "sog60":    _per60(sog, icetime_s),
                    "cf60":     _per60(cf,  icetime_s),
                    "xg60":     _per60(xg,  icetime_s),
                    "rebounds60": _per60(rebounds, icetime_s),
                    "ld_xg60":  _per60(ld_xg, icetime_s),
                    "md_xg60":  _per60(md_xg, icetime_s),
                    "hd_xg60":  _per60(hd_xg, icetime_s),

                    "ev_toi_share": 0.0,
                    "pp_toi_share": 0.0,
                    "pk_toi_share": 0.0,
                }

                key = (player_id, season, situation)
                rows_by_key[key] = row
                kept += 1

                # accumulate TOI for shares
                base = (player_id, season)
                if situation == "all":
                    all_icetime[base] = icetime_s
                elif situation == "5on5":
                    ev_icetime[base] += icetime_s
                elif situation == "5on4":
                    pp_icetime[base] += icetime_s
                elif situation == "4on5":
                    pk_icetime[base] += icetime_s

            except Exception as e:
                # tolerate occasional bad rows
                print(f"warn: skipping row due to error: {e}", file=sys.stderr)
                continue

    # Compute shares per player-season (ratios to 'all' icetime)
    for base, all_s in all_icetime.items():
        if all_s <= 0: continue
        ev = ev_icetime.get(base, 0.0)
        pp = pp_icetime.get(base, 0.0)
        pk = pk_icetime.get(base, 0.0)
        # set on all 4 rows if present; else we at least set on the 'all' row
        for sit in ("all","5on5","5on4","4on5"):
            k = (base[0], base[1], sit)
            if k in rows_by_key:
                rows_by_key[k]["ev_toi_share"] = round(ev / all_s, 6)
                rows_by_key[k]["pp_toi_share"] = round(pp / all_s, 6)
                rows_by_key[k]["pk_toi_share"] = round(pk / all_s, 6)

    # Connect and upsert
    conn = psycopg.connect(args.db, row_factory=dict_row)
    try:
        if args.create_table:
            with conn.cursor() as cur:
                cur.execute(DDL)
                conn.commit()
                print("✓ ensured nhl_ref.skaters_season_situation exists")

        to_upsert = list(rows_by_key.values())
        total = len(to_upsert)
        if total == 0:
            print("No rows to upsert. Exiting.")
            return

        batch = max(1000, int(args.batch))
        done = 0
        with conn.cursor() as cur:
            for i in range(0, total, batch):
                chunk = to_upsert[i:i+batch]
                cur.executemany(UPSERT_SQL, chunk)
                conn.commit()
                done += len(chunk)
                print(f"… upserted {done}/{total}")

        # Coverage report
        players = { (r["player_id"], r["season"]) for r in to_upsert }
        by_sit = defaultdict(int)
        for r in to_upsert:
            by_sit[r["situation"]] += 1
        print("=== Load summary ===")
        print(f"players (unique by season): {len(players)}")
        print(f"rows upserted: {total}")
        for k in ("all","5on5","5on4","4on5","other"):
            if by_sit.get(k): print(f"  {k:>4}: {by_sit[k]}")

    finally:
        try: conn.close()
        except Exception: pass


if __name__ == "__main__":
    main()
