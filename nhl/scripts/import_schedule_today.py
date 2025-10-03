#!/usr/bin/env python3
import os, sys, json, datetime, requests, psycopg
from urllib.parse import urlencode

DATE = os.getenv("SLATE_DATE") or datetime.date.today().isoformat()
DB   = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
if not DB:
    sys.exit("Missing SUPABASE_DB_URL / DATABASE_URL")
if "?sslmode=" not in DB and "&sslmode=" not in DB:
    DB += ("&" if "?" in DB else "?") + "sslmode=require"
if "?gssencmode=" not in DB and "&gssencmode=" not in DB:
    DB += ("&" if "?" in DB else "?") + "gssencmode=disable"

# NHL schedule API (statsapi)
URL = f"https://statsapi.web.nhl.com/api/v1/schedule?{urlencode({'date': DATE})}"

def fetch_schedule():
    r = requests.get(URL, timeout=20)
    r.raise_for_status()
    return r.json()

def main():
    data = fetch_schedule()
    dates = data.get("dates", [])
    games = dates[0]["games"] if dates else []
    if not games:
        print(f"ℹ️ No NHL games for {DATE}")
        return

    with psycopg.connect(DB) as conn, conn.cursor() as cur:
        # Map provider team ids -> internal team_id
        cur.execute("""
            select team_id, provider_team_id
            from nhl.team_external_ids
            where provider = 'nhl'
        """)
        team_map = {str(pid): tid for (tid, pid) in cur.fetchall()}

        # Stage games
        cur.execute("truncate nhl.import_games_stage")
        cur.execute("truncate nhl.import_game_external_ids_stage")
        rows = 0
        for g in games:
            gid = int(g["gamePk"])
            season = g.get("season")
            game_type = g.get("gameType")
            # ISO start time may be absent for some games
            start_iso = g.get("gameDate")
            start_ts = start_iso  # pass raw ISO; DB column is timestamptz

            teams = g["teams"]
            home_pid = str(teams["home"]["team"]["id"])
            away_pid = str(teams["away"]["team"]["id"])
            home_team_id = team_map.get(home_pid)
            away_team_id = team_map.get(away_pid)
            if home_team_id is None or away_team_id is None:
                print(f"⚠️ Missing team mapping for game {gid}: home {home_pid}→{home_team_id}, away {away_pid}→{away_team_id}")
                continue

            cur.execute("""
                insert into nhl.import_games_stage
                  (game_id, game_date, start_time_utc, season, game_type, home_team_id, away_team_id)
                values (%s, %s, %s, %s, %s, %s, %s)
            """, (gid, DATE, start_ts, season, game_type, home_team_id, away_team_id))

            cur.execute("""
                insert into nhl.import_game_external_ids_stage
                  (game_id, provider, provider_game_id)
                values (%s, 'nhl', %s)
            """, (gid, str(gid)))

            rows += 1

        # Merge to base
        cur.execute("""
            insert into nhl.games (game_id, game_date, start_time_utc, season, game_type, home_team_id, away_team_id)
            select distinct game_id, game_date, start_time_utc, season, game_type, home_team_id, away_team_id
            from nhl.import_games_stage
            on conflict (game_id) do update
              set game_date      = excluded.game_date,
                  start_time_utc = excluded.start_time_utc,
                  season         = excluded.season,
                  game_type      = excluded.game_type,
                  home_team_id   = excluded.home_team_id,
                  away_team_id   = excluded.away_team_id
        """)
        cur.execute("""
            insert into nhl.game_external_ids (game_id, provider, provider_game_id)
            select distinct game_id, provider, provider_game_id
            from nhl.import_game_external_ids_stage
            on conflict (game_id, provider) do update
              set provider_game_id = excluded.provider_game_id
        """)
        conn.commit()
        print(f"✅ Staged & merged {rows} games for {DATE}")

if __name__ == "__main__":
    main()
