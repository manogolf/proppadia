#!/usr/bin/env python3
"""Fill missing regular-season goalie game logs from official boxscores (P2.41).

``nhl.goalie_game_logs_raw`` has 354 of 1,312 2025 regular-season games. This
fills only games with no goalie row, from the official gamecenter boxscore
(``api-web.nhle.com/v1/gamecenter/{id}/boxscore``), the source the daily
goalie seed (``seed_goalie_logs_for_date.py``) and postgame reconciliation
use. Rows follow the existing table convention, proven first on every game
that already has rows (``--validate``): goalie team from the boxscore side,
``start_flag`` = the boxscore's ``starter`` (it matches stored rows 1,414 of
1,416; most-ice-time mislabels pulled starters), ``goals_allowed`` = shots -
saves, and a goalie with no ice time keeps NULL shots faced. The strength
split (``ev/pp/sh_shots_faced``) is left NULL: stored rows use a different
derivation that moves a shot between even strength and short-handed in about
28% of rows, and 693 stored 2025 rows already leave it NULL.

Writes use the ingestion login with ``ON CONFLICT DO NOTHING``, so no populated
value can change; a goalie not bound in ``nhl.player_external_ids`` is reported
and not written (the seed's identity rule). Steps: ``--fetch`` (boxscores kept
gzip with SHA-256), ``--validate``, ``--apply`` (one transaction, receipt,
exact rollback SQL).
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl.database_access import database_url  # noqa: E402
from backend.nhl.scripts.fetch_nhl_skater_season_totals import session  # noqa: E402

BOX_URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"
COLUMNS = ["player_id", "game_id", "team_id", "opponent_id", "is_home", "game_date", "saves", "shots_faced",
           "goals_allowed", "toi_minutes", "start_flag"]
COMPARE = [c for c in COLUMNS if c not in ("player_id", "game_id")]


def research_env() -> dict:
    from backend.nhl.scripts.run_nhl_points_hgb_pp_corrected_refit import research_env as env
    return env()


def season_games(season: int) -> pd.DataFrame:
    with psycopg.connect(database_url("research", environ=research_env()), prepare_threshold=None) as c, c.cursor() as cur:
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        cur.execute("""SELECT g.game_id::bigint, g.game_date, g.home_team_id, g.away_team_id,
                              count(l.player_id) AS goalie_rows
                       FROM nhl.games g LEFT JOIN nhl.goalie_game_logs_raw l USING(game_id)
                       WHERE g.season = %s AND substring(g.game_id::text,5,2) = '02'
                       GROUP BY 1,2,3,4 ORDER BY 1""", (season,))
        return pd.DataFrame(cur.fetchall(), columns=["game_id", "game_date", "home_team_id", "away_team_id", "goalie_rows"])


def fetch(games: pd.DataFrame, out: Path) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    s, fetched, reused = session(), 0, 0
    for gid in games.game_id:
        path = out / f"game={gid}_boxscore.json.gz"
        if path.is_file():
            reused += 1
            continue
        r = s.get(BOX_URL.format(game_id=gid), timeout=30)
        r.raise_for_status()
        path.write_bytes(gzip.compress(r.content))
        fetched += 1
        time.sleep(0.2)
    return {"fetched": fetched, "reused": reused}


def _minutes(toi) -> float | None:
    try:
        m, s = str(toi).split(":")
        return round(int(m) + int(s) / 60, 2)
    except (ValueError, AttributeError):
        return None


def rows_from_box(box: dict, game_date) -> list[dict]:
    home, away = int(box["homeTeam"]["id"]), int(box["awayTeam"]["id"])
    out = []
    for side, team, opp in (("homeTeam", home, away), ("awayTeam", away, home)):
        goalies = (box.get("playerByGameStats") or {}).get(side, {}).get("goalies") or []
        team_rows = []
        for g in goalies:
            toi = _minutes(g.get("toi"))
            played = bool(toi and toi > 0)
            shots, saves = g.get("shotsAgainst"), g.get("saves")
            team_rows.append({
                "player_id": int(g["playerId"]), "game_id": int(box["id"]), "team_id": team, "opponent_id": opp,
                "is_home": side == "homeTeam", "game_date": game_date,
                "saves": None if saves is None else int(saves),
                "shots_faced": int(shots) if (played and shots is not None) else None,
                "goals_allowed": (int(shots) - int(saves)) if (played and shots is not None and saves is not None) else 0,
                "toi_minutes": toi,
                "start_flag": bool(g.get("starter")),
            })
        out.extend(team_rows)
    return out


def build(games: pd.DataFrame, box_dir: Path) -> pd.DataFrame:
    rows = []
    for g in games.itertuples(index=False):
        box = json.loads(gzip.open(box_dir / f"game={g.game_id}_boxscore.json.gz").read())
        rows.extend(rows_from_box(box, g.game_date))
    return pd.DataFrame(rows)


def existing(game_ids: list[int]) -> pd.DataFrame:
    with psycopg.connect(database_url("research", environ=research_env()), prepare_threshold=None) as c, c.cursor() as cur:
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        cur.execute(f"SELECT {', '.join(COLUMNS)} FROM nhl.goalie_game_logs_raw WHERE game_id = ANY(%s)", (game_ids,))
        return pd.DataFrame(cur.fetchall(), columns=COLUMNS)


def bound_ids(player_ids: list[int]) -> set[int]:
    with psycopg.connect(database_url("research", environ=research_env()), prepare_threshold=None) as c, c.cursor() as cur:
        cur.execute("SET TRANSACTION READ ONLY")
        cur.execute("""SELECT x.provider_player_id::bigint FROM nhl.player_external_ids x JOIN nhl.players p
                       ON p.player_id = x.player_id
                       WHERE x.provider = 'nhl' AND x.provider_player_id = ANY(%s) AND x.player_id::text = x.provider_player_id""",
                    (list(map(str, player_ids)),))
        return {int(r[0]) for r in cur.fetchall()}


def compare(built: pd.DataFrame, stored: pd.DataFrame) -> dict:
    m = built.merge(stored, on=["player_id", "game_id"], how="outer", suffixes=("_box", "_db"), indicator=True)
    both = m[m._merge == "both"]
    diffs, nulls = {}, {}
    for c in COMPARE:
        a, b = both[f"{c}_box"], both[f"{c}_db"]
        known = a.notna() & b.notna()
        if c == "game_date":
            same = pd.to_datetime(a[known]) == pd.to_datetime(b[known])
        elif c in ("is_home", "start_flag"):
            same = a[known].astype(bool) == b[known].astype(bool)
        else:
            same = (a[known].astype(float) - b[known].astype(float)).abs().le(0.011)
        diffs[c] = int((~same).sum())
        nulls[c] = int(b.isna().sum())
    return {"rows_both": int(len(both)), "only_box": int((m._merge == "left_only").sum()),
            "only_db": int((m._merge == "right_only").sum()), "value_mismatches_where_both_known": diffs,
            "stored_nulls": nulls}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--season", type=int, default=2025)
    ap.add_argument("--out", type=Path, default=ROOT / "artifacts/analysis/nhl/goalie_log_backfill_2026-11-01")
    ap.add_argument("--fetch", action="store_true")
    ap.add_argument("--validate", action="store_true")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    box_dir = a.out / f"season={a.season}" / "boxscores"
    games = season_games(a.season)
    print(json.dumps({"games": len(games), "with_goalie_rows": int((games.goalie_rows > 0).sum()),
                      "without": int((games.goalie_rows == 0).sum())}))
    if a.fetch:
        print(json.dumps(fetch(games, box_dir)))
    built = build(games, box_dir)
    present = games[games.goalie_rows > 0].game_id.astype(int).tolist()
    missing = games[games.goalie_rows == 0].game_id.astype(int).tolist()
    if a.validate or a.apply:
        check = compare(built[built.game_id.isin(present)], existing(present))
        print(json.dumps(check, indent=1))
        # Stored values may carry later NHL corrections or known defects; allow at most 1% per field.
        worst = max(check["value_mismatches_where_both_known"].values())
        if check["only_box"] or check["only_db"] or worst > 0.01 * check["rows_both"]:
            if a.apply:
                raise SystemExit("VALIDATION_MISMATCH: not applying")
    if not a.apply:
        return
    todo = built[built.game_id.isin(missing)].copy()
    bound = bound_ids(sorted(todo.player_id.unique().tolist()))
    unbound = sorted(set(todo.player_id) - bound)
    todo = todo[todo.player_id.isin(bound)]
    games_written = todo.game_id.nunique()
    started = datetime.now(timezone.utc).isoformat()
    with psycopg.connect(database_url("ingestion", environ=research_env()), prepare_threshold=None) as c, c.cursor() as cur:
        cur.execute("SELECT current_user"); login = cur.fetchone()[0]
        inserted = []
        for r in todo[COLUMNS].itertuples(index=False):
            cur.execute(f"""INSERT INTO nhl.goalie_game_logs_raw ({', '.join(COLUMNS)})
                            VALUES ({', '.join(['%s'] * len(COLUMNS))})
                            ON CONFLICT (player_id, game_id) DO NOTHING RETURNING player_id, game_id""",
                        tuple(None if pd.isna(v) else (bool(v) if isinstance(v, (bool,)) else v) for v in r))
            got = cur.fetchone()
            if got:
                inserted.append([int(got[0]), int(got[1])])
        c.commit()
    run = a.out / f"season={a.season}" / f"apply_{started[:19].replace(':', '')}Z"
    run.mkdir(parents=True, exist_ok=False)
    pairs = ",\n".join(f"  ({p}, {g})" for p, g in inserted)
    (run / "rollback_inserted_goalie_rows.sql").write_text(
        "-- Removes exactly the rows this backfill inserted (P2.41).\nBEGIN;\nDELETE FROM nhl.goalie_game_logs_raw\n"
        f"WHERE (player_id, game_id) IN (\n{pairs}\n);\nCOMMIT;\n")
    todo.to_csv(run / "inserted_rows_source.csv", index=False)
    receipt = {"started_utc": started, "season": a.season, "login": login, "games_missing_before": len(missing),
               "games_written": int(games_written), "rows_attempted": int(len(todo)), "rows_inserted": len(inserted),
               "rows_skipped_on_conflict": int(len(todo) - len(inserted)), "unbound_goalie_ids": unbound,
               "boxscore_dir": str(box_dir.relative_to(ROOT)),
               "boxscore_sha256": {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                                   for p in sorted(box_dir.glob("*.json.gz")) if int(p.name.split("=")[1].split("_")[0]) in set(missing)}}
    (run / "receipt.json").write_text(json.dumps(receipt, indent=1) + "\n")
    print(json.dumps({k: v for k, v in receipt.items() if k != "boxscore_sha256"}, indent=1))


if __name__ == "__main__":
    main()
