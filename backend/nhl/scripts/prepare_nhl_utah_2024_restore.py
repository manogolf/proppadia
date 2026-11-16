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


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = ap.add_mutually_exclusive_group(required=True)
    mode.add_argument("--fetch", action="store_true")
    mode.add_argument("--manifest", action="store_true")
    args = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    if args.fetch:
        man = fetch(utah_games())
        print(f"UTAH 2024 BOXSCORES: {len(man)} games, states {pd.Series([v['state'] for v in man.values()]).value_counts().to_dict()}")
    else:
        print(json.dumps(manifest(), indent=1, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
