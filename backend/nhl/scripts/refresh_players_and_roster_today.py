#!/usr/bin/env python3
import os, sys, json, datetime as dt
import requests
import psycopg
from psycopg.rows import dict_row

SLATE_DATE = os.environ.get("SLATE_DATE") or dt.date.today().isoformat()
DB_URL = os.environ["SUPABASE_DB_URL"]

STATS_TEAMS = "https://statsapi.web.nhl.com/api/v1/teams?expand=team.roster"
STATS_PEOPLE = "https://statsapi.web.nhl.com/api/v1/people/{}"

def fetch_day_roster():
    # teams -> roster (active NHL roster)
    r = requests.get(STATS_TEAMS, timeout=30)
    r.raise_for_status()
    data = r.json().get("teams", [])
    players = []
    roster_rows = []  # (game_date, team_id, player_id, active_flag, pp_unit)
    for t in data:
        tm = t.get("id")
        for p in (t.get("roster", {}) or {}).get("roster", []):
            person = p.get("person", {}) or {}
            pid = int(person.get("id"))
            pos = (p.get("position", {}) or {}).get("abbreviation")
            # People endpoint to get handedness/shoots if needed
            shoots = None
            try:
                pr = requests.get(STATS_PEOPLE.format(pid), timeout=10)
                if pr.ok:
                    shoots = ((pr.json().get("people") or [{}])[0]
                              .get("shootsCatches"))
            except Exception:
                pass
            full_name = person.get("fullName", "")
            first, last = (full_name.split(" ", 1) + [""])[:2]
            players.append({
                "player_id": pid, "team_id": tm, "first_name": first, "last_name": last,
                "position": ("G" if pos == "G" else ("D" if pos == "D" else "F")),
                "shoots_catches": shoots, "active": True,
            })
            roster_rows.append({
                "game_date": SLATE_DATE, "team_id": tm, "player_id": pid,
                "active_flag": True, "pp_unit": "None",
            })
    return players, roster_rows

def main():
    players, roster_rows = fetch_day_roster()
    if not players:
        print("No players fetched; aborting.", file=sys.stderr)
        sys.exit(0)

    with psycopg.connect(DB_URL) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            # Clear/import stage for players
            cur.execute("TRUNCATE nhl.import_players_stage;")
            cur.executemany("""
                INSERT INTO nhl.import_players_stage
                  (player_id, team_id, first_name, last_name, "position", shoots_catches, active)
                VALUES (%(player_id)s, %(team_id)s, %(first_name)s, %(last_name)s,
                        %(position)s, %(shoots_catches)s, %(active)s)
            """, players)

            # Upsert players
            with open("backend/nhl/sql/upsert_players_from_stage.sql", "r") as f:
                cur.execute(f.read())

            # Stage roster_status in a TEMP table and merge
            cur.execute("""
                CREATE TEMP TABLE tmp_roster_stage (
                  game_date date, team_id bigint, player_id bigint, active_flag boolean, pp_unit text
                ) ON COMMIT DROP;
            """)
            cur.executemany("""
                INSERT INTO tmp_roster_stage (game_date, team_id, player_id, active_flag, pp_unit)
                VALUES (%(game_date)s, %(team_id)s, %(player_id)s, %(active_flag)s, %(pp_unit)s)
            """, roster_rows)

            with open("backend/nhl/sql/upsert_roster_status_from_stage.sql", "r") as f:
                cur.execute(f.read())

    print(f"Refreshed players & roster_status for {SLATE_DATE}")

if __name__ == "__main__":
    main()
