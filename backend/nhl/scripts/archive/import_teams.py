#!/usr/bin/env python3
import os, sys, requests, psycopg
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

try:
    from dotenv import load_dotenv
    load_dotenv()  # loads variables from .env into process env if present
except Exception:
    pass

DB = os.getenv("SUPABASE_DB_URL") or os.getenv("DATABASE_URL")
if not DB:
    sys.exit("Missing SUPABASE_DB_URL / DATABASE_URL")
if "?sslmode=" not in DB and "&sslmode=" not in DB:
    DB += ("&" if "?" in DB else "?") + "sslmode=require"
if "?gssencmode=" not in DB and "&gssencmode=" not in DB:
    DB += ("&" if "?" in DB else "?") + "gssencmode=disable"

API_WEB = "https://api-web.nhle.com/v1"
STATS_REST = "https://api.nhle.com/stats/rest/en"

def _session() -> requests.Session:
    r = Retry(total=5, connect=5, read=5, backoff_factor=0.5,
              status_forcelist=[429,500,502,503,504],
              allowed_methods=frozenset({"GET"}))
    s = requests.Session()
    s.headers.update({"User-Agent": "proppadia-nhl-teams/1.0"})
    s.mount("https://", HTTPAdapter(max_retries=r))
    return s

S = _session()

def _g(d, *keys, default=None):
    v = d
    for k in keys:
        v = (v or {}).get(k)
    if isinstance(v, dict) and "default" in v:
        v = v["default"]
    return v if (v is not None and v != "") else default

def fetch_from_standings_now():
    """Primary: api-web standings/now → list of active teams (in-season)."""
    try:
        r = S.get(f"{API_WEB}/standings/now", timeout=12)
        r.raise_for_status()
        data = r.json()
        out = []
        for row in data.get("standings", []):
            tid = _g(row, "teamId") or _g(row, "team", "id")
            tri = _g(row, "teamAbbrev")
            name = _g(row, "teamCommonName") or _g(row, "teamName")
            city = _g(row, "teamCity")
            conf = _g(row, "conferenceName") or _g(row, "conferenceAbbrev")
            div  = _g(row, "divisionName")   or _g(row, "divisionAbbrev")
            if tid and tri and name:
                out.append({
                    "team_id": int(tid),
                    "abbr": tri,
                    "name": name,
                    "city": city,
                    "conference": conf,
                    "division": div,
                    "active": True,
                })
        return out
    except Exception:
        return []

def fetch_from_stats_rest():
    """
    Fallback: stats REST team list. Many installs use ?isActive=true; if that
    param isn’t supported, we’ll just filter locally by presence of triCode.
    """
    out = []
    # First try the common active filter; if it 4xx’s we retry without it.
    for url in (f"{STATS_REST}/team?isActive=true", f"{STATS_REST}/team"):
        try:
            r = S.get(url, timeout=12)
            if r.status_code >= 400:
                continue
            j = r.json()
            for t in j.get("data", []):
                # fields seen in the wild: teamId, triCode/teamAbbrev, fullName/teamName, city
                tid = t.get("teamId") or t.get("id")
                tri = t.get("triCode") or t.get("teamAbbrev")
                name = t.get("fullName") or t.get("teamName")
                city = t.get("city")
                conf = t.get("conferenceName") or t.get("conferenceAbbrev")
                div  = t.get("divisionName")   or t.get("divisionAbbrev")
                # treat records with a tri code as “active” enough for our purposes
                if tid and tri and name:
                    out.append({
                        "team_id": int(tid),
                        "abbr": tri,
                        "name": name,
                        "city": city,
                        "conference": conf,
                        "division": div,
                        "active": True,
                    })
            if out:
                break
        except Exception:
            continue
    return out

def main():
    teams = fetch_from_standings_now()
    if not teams:
        print("ℹ️ standings/now empty — falling back to stats REST …")
        teams = fetch_from_stats_rest()

    if not teams:
        print("⚠️ No teams returned from either source.")
        return

    with psycopg.connect(DB) as conn, conn.cursor() as cur:
        up, map_up = 0, 0

        for t in teams:
            provider_tid = int(t["team_id"])   # NHL numeric id from API
            tri = t["abbr"]

            # 1) If a team with this abbr already exists, use its team_id as canonical
            cur.execute("select team_id from nhl.teams where abbr = %s", (tri,))
            row = cur.fetchone()
            if row:
                canonical_tid = int(row[0])
                # Update metadata on the existing row
                cur.execute("""
                    update nhl.teams
                    set name = %s,
                        city = %s,
                        conference = %s,
                        division = %s,
                        active = true
                    where team_id = %s
                """, (t["name"], t["city"], t["conference"], t["division"], canonical_tid))
                up += 1
            else:
                # 2) No existing abbr row → insert by provider id as PK (if free), else update
                canonical_tid = provider_tid
                cur.execute("""
                    insert into nhl.teams (team_id, abbr, name, city, conference, division, active)
                    values (%s, %s, %s, %s, %s, %s, %s)
                    on conflict (team_id) do update
                      set abbr       = excluded.abbr,
                          name       = excluded.name,
                          city       = excluded.city,
                          conference = excluded.conference,
                          division   = excluded.division,
                          active     = true
                """, (canonical_tid, tri, t["name"], t["city"], t["conference"], t["division"], True))
                up += 1

            # 3) Ensure provider→internal mapping points at the canonical team_id
            cur.execute("""
                insert into nhl.team_external_ids (team_id, provider, provider_team_id)
                values (%s, 'nhl', %s)
                on conflict (provider, provider_team_id)
                do update set team_id = excluded.team_id
            """, (canonical_tid, str(provider_tid)))
            map_up += 1

        # Optionally inactivate obvious placeholders
        cur.execute("""
            update nhl.teams
            set active = false
            where abbr ~ '^T[0-9]+$'
              and team_id not in (select team_id from nhl.team_external_ids where provider='nhl')
        """)

        conn.commit()
        print(f"🔧 Upserted/confirmed {up} teams in nhl.teams")
        print(f"🔄 Seeded {map_up} team mappings in nhl.team_external_ids (provider='nhl')")

if __name__ == "__main__":
    main()
