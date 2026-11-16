#!/usr/bin/env python3
"""Restore the 82 missing 2024-25 Utah games: step 1, read-only manifest (NHL Backlog P2.57).

No database writes. The NHL gave the Utah Hockey Club team id 59 in 2024-25 (Utah
Mammoth 68 from 2025-26); `nhl.team_external_ids` maps only 68, so these games have no
date in `nhl.games` and no player-game rows. This script:

  --fetch     official boxscore per game (public api-web gamecenter/{id}/boxscore);
              play-by-play is already fetched under points_onice_xg_2026-11-16/.
  --manifest  expected `nhl.games` dates and start times, skater and goalie rows
              (team 59 written as 68), PP TOI from the stats API evidence
              (pp_toi_stats_api_correction_2026-11-15/official, the P2.45 source of
              record), shot attempts and SOG by strength from the play-by-play. Checks:
              keys vs official_without_stored_row.csv, players missing from
              nhl.players, rows already present, and a reproduction check that
              rebuilds sample stored 2024-25 games with the same code and compares
              them with the stored values.

Output: artifacts/analysis/nhl/utah_2024_restore_2026-11-16/.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
OUT = ROOT / "artifacts/analysis/nhl/utah_2024_restore_2026-11-16"
PBP_FILL = ROOT / "artifacts/analysis/nhl/points_onice_xg_2026-11-16/pbp_2024_missing"
PBP_STORED = ROOT / "artifacts/analysis/nhl/pp_toi_historical_recovery_2026-10-16/official_sources"
PP_EVIDENCE = ROOT / "artifacts/analysis/nhl/pp_toi_stats_api_correction_2026-11-15"
TEAM_TRANSLATION = {59: 68}  # Utah Hockey Club 2024-25 -> stored team 68 (UTA)
KEYS = ["game_id", "player_id"]


def sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def connect():
    import psycopg
    from dotenv import dotenv_values
    from backend.nhl.database_access import database_url
    env = {**os.environ, **{k: v for k, v in dotenv_values(ROOT / "backend/.env").items() if v is not None}}
    c = psycopg.connect(database_url("research", environ=env), prepare_threshold=None)
    c.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
    return c


def utah_games() -> list[int]:
    with connect() as c, c.cursor() as cur:
        cur.execute("""SELECT game_id::bigint FROM nhl.games WHERE season = 2024 AND game_type = 2
                        AND game_date IS NULL AND 'UTA' IN (home_team_code, away_team_code) ORDER BY 1""")
        return [r[0] for r in cur.fetchall()]


def fetch(games: list[int]) -> dict:
    import requests
    d = OUT / "boxscore"
    d.mkdir(parents=True, exist_ok=True)
    s = requests.Session(); s.headers["User-Agent"] = "research-client/1.0"
    man = {}
    for g in games:
        f = d / f"game={g}.boxscore.json"
        if not f.is_file():
            for attempt in range(5):
                try:
                    r = s.get(f"https://api-web.nhle.com/v1/gamecenter/{g}/boxscore", timeout=30)
                    r.raise_for_status(); json.loads(r.content); f.write_bytes(r.content); break
                except Exception:
                    if attempt == 4:
                        raise
                    time.sleep(2 * (attempt + 1))
            time.sleep(0.2)
        b = f.read_bytes()
        man[str(g)] = {"sha256": sha(b), "state": json.loads(b).get("gameState")}
    (OUT / "boxscore_manifest.json").write_text(json.dumps(man, indent=1, sort_keys=True) + "\n")
    return man


def toi_min(v) -> float | None:
    if not v or ":" not in str(v):
        return None
    m, s = str(v).split(":")
    return round((int(m) * 60 + int(s)) / 60, 2)


def pbp_counts(p: dict) -> pd.DataFrame:
    """Per shooter: attempts (SOG + missed + blocked + goals) and SOG by strength, shootout excluded."""
    home = int(p["homeTeam"]["id"])
    rows = []
    for e in p.get("plays", []):
        if (e.get("periodDescriptor") or {}).get("periodType") == "SO":
            continue
        kind = e.get("typeDescKey")
        if kind not in ("shot-on-goal", "missed-shot", "blocked-shot", "goal"):
            continue
        d = e.get("details") or {}
        shooter = d.get("shootingPlayerId") or d.get("scoringPlayerId")
        team = d.get("eventOwnerTeamId")
        if shooter is None or team is None:
            continue
        code = str(e.get("situationCode") or "1551").zfill(4)
        own_home = int(team) == home
        own, opp = (int(code[2]), int(code[1])) if own_home else (int(code[1]), int(code[2]))
        strength = "pp" if own > opp else "sh" if own < opp else "ev"
        on_net = kind in ("shot-on-goal", "goal")
        rows.append({"player_id": int(shooter), "attempt": 1, f"{strength}_sog": int(on_net)})
    if not rows:
        return pd.DataFrame(columns=["player_id", "shot_attempts", "ev_sog", "pp_sog", "sh_sog"])
    t = pd.DataFrame(rows).fillna(0).groupby("player_id").sum().reset_index()
    for c in ("ev_sog", "pp_sog", "sh_sog"):
        if c not in t:
            t[c] = 0
    return t.rename(columns={"attempt": "shot_attempts"})


def pp_official() -> dict[tuple[int, int], float]:
    man = json.loads((PP_EVIDENCE / "official_manifest.json").read_text())
    out = {}
    for day in man:
        if not day.startswith(("2024-1", "2025-0")):
            continue
        for r in json.loads((PP_EVIDENCE / "official" / f"{day}.json").read_text())["data"]:
            out[(int(r["gameId"]), int(r["playerId"]))] = round(int(r["ppTimeOnIce"]) / 60, 2)
    return out


def pp_for_uncovered(games: list[int], pp: dict) -> dict:
    """Per-game stats API PP TOI for games the P2.45 evidence does not cover (a date with only a
    Utah game had no stored rows, so it was never fetched). Saved under pp_extra/."""
    import requests
    d = OUT / "pp_extra"
    d.mkdir(exist_ok=True)
    covered = {g for g, _ in pp}
    for g in games:
        if g in covered:
            continue
        f = d / f"game={g}.timeonice.json"
        if not f.is_file():
            r = requests.get("https://api.nhle.com/stats/rest/en/skater/timeonice",
                             params={"isAggregate": "false", "isGame": "true", "limit": "-1", "cayenneExp": f"gameId={g}"},
                             headers={"User-Agent": "research-client/1.0"}, timeout=30)
            r.raise_for_status(); f.write_bytes(r.content)
        body = json.loads(f.read_text())
        if len(body["data"]) != body["total"]:
            raise RuntimeError(f"OFFICIAL_RESPONSE_TRUNCATED:{g}")
        for x in body["data"]:
            pp[(int(x["gameId"]), int(x["playerId"]))] = round(int(x["ppTimeOnIce"]) / 60, 2)
    return pp


def build(game: int, box: dict, pbp: dict, pp: dict) -> tuple[dict, list[dict], list[dict]]:
    tr = lambda t: TEAM_TRANSLATION.get(int(t), int(t))  # noqa: E731
    home, away = tr(box["homeTeam"]["id"]), tr(box["awayTeam"]["id"])
    date = box["gameDate"]
    game_row = {"game_id": game, "game_date": date, "start_time_utc": box.get("startTimeUTC"), "state": box.get("gameState"),
                "home_team_id": home, "away_team_id": away}
    counts = pbp_counts(pbp).set_index("player_id") if pbp else pd.DataFrame()
    skaters, goalies = [], []
    for side, team, opp, is_home in (("homeTeam", home, away, True), ("awayTeam", away, home, False)):
        st = box["playerByGameStats"][side]
        for p in (st.get("forwards") or []) + (st.get("defense") or []):
            pid = int(p["playerId"])
            c = counts.loc[pid] if pid in counts.index else None
            skaters.append({"player_id": pid, "game_id": game, "team_id": team, "opponent_id": opp, "is_home": is_home,
                            "game_date": date, "toi_minutes": toi_min(p.get("toi")), "pp_toi_minutes": pp.get((game, pid)),
                            "shots_on_goal": int(p.get("sog") or 0),
                            "shot_attempts": int(c.shot_attempts) if c is not None else 0,
                            "ev_sog": int(c.ev_sog) if c is not None else 0, "pp_sog": int(c.pp_sog) if c is not None else 0,
                            "sh_sog": int(c.sh_sog) if c is not None else 0,
                            "blocks": p.get("blockedShots"), "hits": p.get("hits"),
                            "goals": int(p.get("goals") or 0), "assists": int(p.get("assists") or 0),
                            "points": int(p.get("points") or 0), "name": (p.get("name") or {}).get("default"),
                            "position": p.get("position")})
        for p in st.get("goalies") or []:
            toi = toi_min(p.get("toi"))
            if not toi:
                continue
            goalies.append({"player_id": int(p["playerId"]), "game_id": game, "team_id": team, "opponent_id": opp,
                            "is_home": is_home, "game_date": date, "toi_minutes": toi,
                            "shots_faced": p.get("shotsAgainst"), "saves": p.get("saves"),
                            "goals_allowed": p.get("goalsAgainst"), "start_flag": bool(p.get("starter")),
                            "name": (p.get("name") or {}).get("default")})
    return game_row, skaters, goalies


def reproduction_check(pp: dict, n: int = 6) -> pd.DataFrame:
    """Rebuild stored non-Utah 2024-25 games with the same code; compare with stored rows."""
    import requests
    games = sorted(int(p.name.split("=")[1]) for p in PBP_STORED.glob("game=2024*"))[100:100 + n]
    s = requests.Session(); s.headers["User-Agent"] = "research-client/1.0"
    import gzip
    rows = []
    for g in games:
        box = json.loads(s.get(f"https://api-web.nhle.com/v1/gamecenter/{g}/boxscore", timeout=30).content)
        pbp = json.load(gzip.open(PBP_STORED / f"game={g}" / "pbp.json.gz"))
        _, sk, _ = build(g, box, pbp, pp)
        rows += sk
        time.sleep(0.2)
    built = pd.DataFrame(rows)
    with connect() as c, c.cursor() as cur:
        cur.execute("""SELECT player_id::bigint, game_id::bigint, team_id::bigint, toi_minutes, pp_toi_minutes,
                              shots_on_goal, shot_attempts, ev_sog, pp_sog, sh_sog, points, game_date::text
                         FROM nhl.skater_game_logs_raw WHERE game_id = ANY(%s)""", (games,))
        stored = pd.DataFrame(cur.fetchall(), columns=["player_id", "game_id", "team_id", "toi_minutes", "pp_toi_minutes",
                                                       "shots_on_goal", "shot_attempts", "ev_sog", "pp_sog", "sh_sog",
                                                       "points", "game_date"])
    m = built.merge(stored, on=KEYS, how="outer", suffixes=("_built", "_stored"), indicator=True)
    out = {"rows_built": int(len(built)), "rows_stored": int(len(stored)), "match_keys": int((m._merge == "both").sum())}
    both = m[m._merge == "both"]
    for col in ("team_id", "toi_minutes", "pp_toi_minutes", "shots_on_goal", "shot_attempts", "ev_sog", "pp_sog", "sh_sog",
                "points", "game_date"):
        a, b = both[f"{col}_built"], both[f"{col}_stored"]
        if col == "game_date":
            eq = a.astype(str) == b.astype(str)
        else:
            eq = np.isclose(pd.to_numeric(a, errors="coerce").astype(float), pd.to_numeric(b, errors="coerce").astype(float),
                            atol=0.011, equal_nan=True)
        out[f"{col}_equal_share"] = float(eq.mean())
        out[f"{col}_stored_null_share"] = float(b.isna().mean())
    return pd.DataFrame([out])


def manifest() -> dict:
    games = utah_games()
    pp = pp_for_uncovered(games, pp_official())
    game_rows, sk, gl = [], [], []
    for g in games:
        box = json.loads((OUT / "boxscore" / f"game={g}.boxscore.json").read_text())
        pf = PBP_FILL / f"game={g}.json"
        pbp = json.loads(pf.read_text()) if pf.is_file() else None
        gr, s, gg = build(g, box, pbp, pp)
        gr["pbp"] = pbp is not None
        game_rows.append(gr); sk += s; gl += gg
    games_df, sk_df, gl_df = pd.DataFrame(game_rows), pd.DataFrame(sk), pd.DataFrame(gl)
    missing = pd.read_csv(PP_EVIDENCE / "official_without_stored_row.csv")
    miss_keys = set(zip(missing.game_id.astype(int), missing.player_id.astype(int)))
    sk_keys = set(zip(sk_df.game_id, sk_df.player_id))
    with connect() as c, c.cursor() as cur:
        cur.execute("SELECT player_id::bigint FROM nhl.players WHERE player_id = ANY(%s)",
                    (sorted(set(sk_df.player_id) | set(gl_df.player_id)),))
        known = {r[0] for r in cur.fetchall()}
        cur.execute("SELECT count(*) FROM nhl.skater_game_logs_raw WHERE game_id = ANY(%s)", (games,))
        sk_present = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM nhl.goalie_game_logs_raw WHERE game_id = ANY(%s)", (games,))
        gl_present = cur.fetchone()[0]
        cur.execute("SELECT team_id FROM nhl.teams WHERE team_id = ANY(%s)",
                    (sorted(set(games_df.home_team_id) | set(games_df.away_team_id)),))
        teams_known = {r[0] for r in cur.fetchall()}
    new_players = sorted((set(sk_df.player_id) | set(gl_df.player_id)) - known)
    games_df.to_csv(OUT / "games.csv", index=False)
    sk_df.to_csv(OUT / "skater_rows.csv", index=False)
    gl_df.to_csv(OUT / "goalie_rows.csv", index=False)
    repro = reproduction_check(pp)
    repro.to_csv(OUT / "reproduction_check.csv", index=False)
    summary = {
        "games": len(games), "games_final": int(games_df.state.isin(["OFF", "FINAL"]).sum()),
        "games_with_pbp": int(games_df.pbp.sum()),
        "date_range": [games_df.game_date.min(), games_df.game_date.max()],
        "team_translation": {str(k): v for k, v in TEAM_TRANSLATION.items()},
        "teams_known_in_nhl_teams": sorted(int(t) for t in teams_known),
        "skater_rows": int(len(sk_df)), "goalie_rows": int(len(gl_df)),
        "skater_rows_already_present": int(sk_present), "goalie_rows_already_present": int(gl_present),
        "skater_keys_in_missing_list": int(len(sk_keys & miss_keys)),
        "skater_keys_not_in_missing_list": int(len(sk_keys - miss_keys)),
        "missing_list_2024_keys_in_these_games": int(sum(1 for g, p in miss_keys if g in set(games))),
        "pp_toi_known_share": float(sk_df.pp_toi_minutes.notna().mean()),
        "players_not_in_nhl_players": len(new_players), "players_not_in_nhl_players_ids": new_players[:50],
        "reproduction_check": repro.iloc[0].to_dict(),
        "database_writes": 0,
    }
    (OUT / "summary.json").write_text(json.dumps(summary, indent=1, default=str) + "\n")
    return summary


# --------------------------------------------------------------------------- SQL

SKATER_COLS = ["player_id", "game_id", "team_id", "opponent_id", "is_home", "game_date", "toi_minutes",
               "pp_toi_minutes", "shots_on_goal", "shot_attempts", "ev_sog", "pp_sog", "sh_sog", "blocks", "hits"]
GOALIE_COLS = ["player_id", "game_id", "team_id", "opponent_id", "is_home", "game_date", "toi_minutes",
               "shots_faced", "saves", "goals_allowed", "start_flag"]
ROLE = "proppadia_nhl_ingest"


def lit(v) -> str:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "NULL"
    if isinstance(v, (bool, np.bool_)):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, np.integer)):
        return str(int(v))
    if isinstance(v, (float, np.floating)):
        return f"{float(v):.2f}"
    return "'" + str(v).replace("'", "''") + "'"


def values(frame: pd.DataFrame, cols: list[str], casts: dict[str, str]) -> str:
    out = []
    for i, r in enumerate(frame[cols].itertuples(index=False)):
        vals = [lit(None if pd.isna(v) else v) for v in r]
        if i == 0:
            vals = [f"{v}::{casts[c]}" if c in casts else v for v, c in zip(vals, cols)]
        out.append("    (" + ", ".join(vals) + ")")
    return ",\n".join(out)


def wrap(body: str, title: str, *, role: str | None = ROLE) -> str:
    head = f"""-- PREPARED ONLY. Owner authorization required. NHL Backlog P2.57: {title}
-- Paste this whole block into the Supabase SQL editor for the production project and run it once.
-- One transaction; any failure aborts it and nothing is committed{', including the grant' if role else ''}.
-- Afterwards run backend/nhl/sql/repairs/verify_nhl_utah_2024_restore.sql (read-only).

BEGIN;

DO $$
BEGIN
  IF session_user <> 'postgres' THEN
    RAISE EXCEPTION 'RUN_AS_POSTGRES_IN_SQL_EDITOR:session_user=%', session_user;
  END IF;
"""
    if role is None:
        return head + "END $$;\n\n" + body.strip() + "\n\nCOMMIT;\n"
    return head + f"""  IF pg_has_role(session_user, '{role}', 'MEMBER') THEN
    RAISE EXCEPTION 'INGEST_ROLE_ALREADY_GRANTED_REFUSING_TO_PROCEED';
  END IF;
END $$;

GRANT {role} TO postgres;
SET LOCAL ROLE {role};

DO $$
BEGIN
  IF current_user <> '{role}' THEN
    RAISE EXCEPTION 'INGEST_SET_ROLE_VERIFICATION_FAILED:current_user=%', current_user;
  END IF;
END $$;

{body.strip()}

RESET ROLE;
REVOKE {role} FROM postgres;

DO $$
BEGIN
  IF current_user <> 'postgres' OR pg_has_role('postgres', '{role}', 'MEMBER') THEN
    RAISE EXCEPTION 'INGEST_ROLE_RESET_OR_REVOKE_FAILED';
  END IF;
END $$;

COMMIT;

SELECT now() AS checked_at_utc, current_user AS checked_as,
       pg_has_role('postgres', '{role}', 'MEMBER') AS ingest_role_still_granted;
"""


def games_sql(games: pd.DataFrame, *, reverse: bool = False) -> str:
    g = games.assign(start=games.start_time_utc.str.replace("Z", "+00:00"))
    n = len(g)
    rows = values(g, ["game_id", "game_date", "start", "home_team_id", "away_team_id"],
                  {"game_id": "bigint", "game_date": "date", "start": "timestamptz",
                   "home_team_id": "bigint", "away_team_id": "bigint"})
    if reverse:
        cond, setter, err = ("g.game_date = v.d AND g.start_time_utc = v.t", "game_date = NULL, start_time_utc = NULL",
                             "UTAH_2024_GAMES_ROLLBACK_GUARD_FAILED")
    else:
        cond, setter, err = ("g.game_date IS NULL AND g.start_time_utc IS NULL", "game_date = c.d, start_time_utc = c.t",
                             "UTAH_2024_GAMES_GUARD_FAILED")
    return f"""-- {'Rollback: clear' if reverse else 'Fill'} game_date and start_time_utc for the {n} 2024-25 Utah games (team 59 unmapped),
-- only where every game is in its pinned state with its pinned teams; otherwise the cast raises.
UPDATE nhl.games AS g
SET {setter}
FROM (
  SELECT v.*, count(x.game_id) OVER () AS matching_rows
  FROM (VALUES
{rows}
  ) AS v(game_id, d, t, home, away)
  LEFT JOIN nhl.games AS x
    ON x.game_id = v.game_id AND x.home_team_id = v.home AND x.away_team_id = v.away
   AND {cond.replace('g.', 'x.')}
) AS c
WHERE g.game_id = c.game_id
  AND (CASE WHEN c.matching_rows = {n} THEN TRUE
       ELSE ('{err}:matching_rows=' || c.matching_rows)::boolean END);
"""


def logs_sql(games: pd.DataFrame, sk: pd.DataFrame, gl: pd.DataFrame, player: dict) -> str:
    ids = ", ".join(str(int(g)) for g in games.game_id)
    return f"""-- Guards: the games block ran first, and no player-game row exists yet for these games.
DO $$
BEGIN
  IF (SELECT count(*) FROM nhl.games WHERE game_id IN ({ids}) AND game_date IS NOT NULL) <> {len(games)} THEN
    RAISE EXCEPTION 'RUN_THE_GAMES_BLOCK_FIRST';
  END IF;
  IF EXISTS (SELECT 1 FROM nhl.skater_game_logs_raw WHERE game_id IN ({ids}))
     OR EXISTS (SELECT 1 FROM nhl.goalie_game_logs_raw WHERE game_id IN ({ids})) THEN
    RAISE EXCEPTION 'UTAH_2024_ROWS_ALREADY_PRESENT_REFUSING';
  END IF;
END $$;

-- The one player not yet in nhl.players (foreign key for his goalie row).
INSERT INTO nhl.players (player_id, full_name, first_name, last_name, team_id, position)
SELECT {player['player_id']}, {lit(player['full_name'])}, {lit(player['first_name'])}, {lit(player['last_name'])}, {player['team_id']}, 'G'
WHERE NOT EXISTS (SELECT 1 FROM nhl.players WHERE player_id = {player['player_id']});

INSERT INTO nhl.skater_game_logs_raw ({", ".join(SKATER_COLS)})
SELECT * FROM (VALUES
{values(sk, SKATER_COLS, {"player_id": "bigint", "game_id": "bigint", "team_id": "bigint", "opponent_id": "bigint",
                          "is_home": "boolean", "game_date": "date", "toi_minutes": "numeric", "pp_toi_minutes": "numeric",
                          "shots_on_goal": "smallint", "shot_attempts": "smallint", "ev_sog": "integer", "pp_sog": "integer",
                          "sh_sog": "integer", "blocks": "smallint", "hits": "smallint"})}
) AS v({", ".join(SKATER_COLS)});

INSERT INTO nhl.goalie_game_logs_raw ({", ".join(GOALIE_COLS)})
SELECT * FROM (VALUES
{values(gl, GOALIE_COLS, {"player_id": "bigint", "game_id": "bigint", "team_id": "bigint", "opponent_id": "bigint",
                          "is_home": "boolean", "game_date": "date", "toi_minutes": "numeric", "shots_faced": "smallint",
                          "saves": "smallint", "goals_allowed": "smallint", "start_flag": "boolean"})}
) AS v({", ".join(GOALIE_COLS)});

-- All-or-nothing: exact counts, every team one of the game's two teams.
DO $$
BEGIN
  IF (SELECT count(*) FROM nhl.skater_game_logs_raw WHERE game_id IN ({ids})) <> {len(sk)}
     OR (SELECT count(*) FROM nhl.goalie_game_logs_raw WHERE game_id IN ({ids})) <> {len(gl)} THEN
    RAISE EXCEPTION 'UTAH_2024_ROW_COUNT_GUARD_FAILED';
  END IF;
  IF EXISTS (SELECT 1 FROM nhl.skater_game_logs_raw l JOIN nhl.games g USING (game_id)
              WHERE l.game_id IN ({ids}) AND l.team_id NOT IN (g.home_team_id, g.away_team_id))
     OR EXISTS (SELECT 1 FROM nhl.goalie_game_logs_raw l JOIN nhl.games g USING (game_id)
              WHERE l.game_id IN ({ids}) AND l.team_id NOT IN (g.home_team_id, g.away_team_id)) THEN
    RAISE EXCEPTION 'UTAH_2024_TEAM_GUARD_FAILED';
  END IF;
END $$;
"""


def rollback_logs_sql(games: pd.DataFrame, sk: pd.DataFrame, gl: pd.DataFrame, player: dict) -> str:
    ids = ", ".join(str(int(g)) for g in games.game_id)
    return f"""-- OWNER-LEVEL ROLLBACK. No restricted role holds DELETE on these tables, so removing the
-- restored rows needs the postgres owner; run only on the operator's decision to undo P2.57 step 3.
-- Removes exactly the restored rows (counts pinned) and the one added player if nothing else uses him.
DO $$
BEGIN
  IF (SELECT count(*) FROM nhl.skater_game_logs_raw WHERE game_id IN ({ids})) <> {len(sk)}
     OR (SELECT count(*) FROM nhl.goalie_game_logs_raw WHERE game_id IN ({ids})) <> {len(gl)} THEN
    RAISE EXCEPTION 'UTAH_2024_ROLLBACK_COUNT_GUARD_FAILED';
  END IF;
END $$;
DELETE FROM nhl.skater_game_logs_raw WHERE game_id IN ({ids});
DELETE FROM nhl.goalie_game_logs_raw WHERE game_id IN ({ids});
DELETE FROM nhl.players p WHERE p.player_id = {player['player_id']}
  AND NOT EXISTS (SELECT 1 FROM nhl.goalie_game_logs_raw WHERE player_id = p.player_id)
  AND NOT EXISTS (SELECT 1 FROM nhl.skater_game_logs_raw WHERE player_id = p.player_id);
"""


def build_sql() -> dict:
    games = pd.read_csv(OUT / "games.csv")
    sk = pd.read_csv(OUT / "skater_rows.csv")
    gl = pd.read_csv(OUT / "goalie_rows.csv")
    for f in (sk, gl):
        f["game_date"] = f.game_date.astype(str)
    gl["goals_allowed"] = gl.goals_allowed.astype("Int64")
    with connect() as c, c.cursor() as cur:
        cur.execute("SELECT player_id FROM nhl.players WHERE player_id = ANY(%s)",
                    (sorted(set(sk.player_id) | set(gl.player_id)),))
        known = {r[0] for r in cur.fetchall()}
    new = sorted((set(sk.player_id) | set(gl.player_id)) - known)
    if len(new) != 1:
        raise SystemExit(f"EXPECTED_ONE_NEW_PLAYER:{new}")
    pid = new[0]
    row = gl[gl.player_id.eq(pid)].iloc[0]
    pbp = json.loads((PBP_FILL / f"game={int(row.game_id)}.json").read_text())
    spot = next(r for r in pbp["rosterSpots"] if int(r["playerId"]) == pid)
    player = {"player_id": pid, "first_name": spot["firstName"]["default"], "last_name": spot["lastName"]["default"],
              "full_name": f"{spot['firstName']['default']} {spot['lastName']['default']}", "team_id": int(row.team_id)}
    sql_dir = OUT / "sql"
    sql_dir.mkdir(exist_ok=True)
    files = {
        "editor_1_games.sql": wrap(games_sql(games), f"fill dates for {len(games)} 2024-25 Utah games"),
        "editor_2_player_logs.sql": wrap(logs_sql(games, sk, gl, player),
                                         f"add {len(sk)} skater rows, {len(gl)} goalie rows and 1 player"),
        "rollback_1_games.sql": wrap(games_sql(games, reverse=True), "ROLLBACK of the games block"),
        "rollback_2_player_logs.sql": wrap(rollback_logs_sql(games, sk, gl, player),
                                           "ROLLBACK of the player and log rows (owner-level)", role=None),
    }
    out = {"player": player, "skater_rows": int(len(sk)), "goalie_rows": int(len(gl)), "games": int(len(games)), "files": {}}
    for name, text in files.items():
        (sql_dir / name).write_text(text)
        out["files"][name] = {"sha256": sha(text.encode()), "bytes": len(text.encode())}
    (OUT / "sql_summary.json").write_text(json.dumps(out, indent=1) + "\n")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--fetch", action="store_true")
    mode.add_argument("--manifest", action="store_true")
    mode.add_argument("--build", action="store_true", help="write the editor blocks and rollbacks under sql/")
    args = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    if args.fetch:
        man = fetch(utah_games())
        print(f"UTAH 2024 BOXSCORES: {len(man)} games, states {pd.Series([v['state'] for v in man.values()]).value_counts().to_dict()}")
    elif args.build:
        print(json.dumps(build_sql(), indent=1))
    else:
        print(json.dumps(manifest(), indent=1, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
