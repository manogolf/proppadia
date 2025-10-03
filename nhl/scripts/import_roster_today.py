#!/usr/bin/env python3
import os, sys, datetime, requests, psycopg

DATE = os.getenv("SLATE_DATE") or datetime.date.today().isoformat()
DB   = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
if not DB:
    sys.exit("Missing SUPABASE_DB_URL / DATABASE_URL")
if "?sslmode=" not in DB and "&sslmode=" not in DB:
    DB += ("&" if "?" in DB else "?") + "sslmode=require"
if "?gssencmode=" not in DB and "&gssencmode=" not in DB:
    DB += ("&" if "?" in DB else "?") + "gssencmode=disable"

TEAMS_URL = "https://statsapi.web.nhl.com/api/v1/teams/{tid}?expand=team.roster"

def fetch_roster(nhl_team_id: str):
    r = requests.get(TEAMS_URL.format(tid=nhl_team_id), timeout=20)
    r.raise_for_status()
    j = r.json()
    ro = j.get("teams", [{}])[0].get("roster", {}).get("roster", []) or []
    # each: person.id, position.code
    return ro

def main():
    with psycopg.connect(DB) as conn, conn.cursor() as cur:
        # map NHL team ids -> internal team_id
        cur.execute("""
            select team_id, provider_team_id::text
            from nhl.team_external_ids
            where provider = 'nhl'
        """)
        team_map = {pid: tid for (tid, pid) in cur.fetchall()}

        # get today's games + external NHL ids for both teams
        cur.execute("""
            select g.game_id, g.home_team_id, g.away_team_id,
                   th.provider_team_id::text as home_nhl_id,
                   ta.provider_team_id::text as away_nhl_id
            from nhl.games g
            join nhl.team_external_ids th on th.team_id = g.home_team_id and th.provider = 'nhl'
            join nhl.team_external_ids ta on ta.team_id = g.away_team_id and ta.provider = 'nhl'
            where g.game_date = current_date
              and g.status in ('scheduled','live')
        """)
        games = cur.fetchall()
        if not games:
            print(f"ℹ️ No games today in nhl.games; run import_schedule_today.py first.")
            return

        # player external id map
        cur.execute("""
            select player_id, provider_player_id::text
            from nhl.player_external_ids
            where provider = 'nhl'
        """)
        player_map = {pid: pl for (pl, pid) in cur.fetchall()}

        inserted = 0
        for game_id, home_tid, away_tid, home_nhl, away_nhl in games:
            for nhl_tid, tid in [(home_nhl, home_tid), (away_nhl, away_tid)]:
                roster = fetch_roster(nhl_tid)
                for item in roster:
                    nhl_pid = str(item["person"]["id"])
                    pos = item.get("position", {}).get("code")  # 'G','D','C','L','R'
                    player_id = player_map.get(nhl_pid)
                    if not player_id:
                        # skip if we don't have the player id mapped yet
                        continue
                    cur.execute("""
                        insert into nhl.roster_status
                          (game_id, team_id, player_id, active_flag, line_role, pp_unit, asof_ts)
                        values (%s, %s, %s, true, null, 'None', now())
                        on conflict (team_id, asof_ts, game_id, player_id) do nothing
                    """, (game_id, tid, player_id))
                    inserted += 1

        conn.commit()
        print(f"✅ Inserted {inserted} roster_status rows for {DATE}")

if __name__ == "__main__":
    main()
