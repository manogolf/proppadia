"""Build the per-player usage table the daily input watch compares against.

One row per player and completed regular season: games, ice time and
power-play ice time per game, from ``nhl.skater_game_logs_raw`` (regular
season = NHL game type 02 in the game ID). Finished seasons do not change, so
this runs once per new season (read-only, research login):

    ./bin/python backend/nhl/scripts/build_nhl_prior_season_usage.py --seasons 2024 2025

Output: ``artifacts/operational/nhl/input_watch/prior_season_usage.csv``
(git-ignored; rerun the command above to regenerate).
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd
import psycopg
from dotenv import dotenv_values

from backend.nhl.database_access import database_url
from backend.nhl.input_watch import PRIOR_USAGE_PATH, ROOT

QUERY = """
SELECT (game_id / 1000000)::int AS season, player_id::bigint AS player_id,
       count(*)::int AS games,
       avg(toi_minutes)::float AS toi_per_game,
       avg(pp_toi_minutes)::float AS pp_toi_per_game,
       count(pp_toi_minutes)::int AS pp_games
FROM nhl.skater_game_logs_raw
WHERE (game_id / 10000) %% 100 = 2 AND (game_id / 1000000) = ANY(%s)
GROUP BY 1, 2
ORDER BY 1, 2
"""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--seasons", type=int, nargs="+", required=True,
                        help="completed seasons by start year, e.g. 2024 2025")
    parser.add_argument("--env-file", type=Path, default=ROOT / "backend/.env")
    parser.add_argument("--output", type=Path, default=PRIOR_USAGE_PATH)
    args = parser.parse_args()
    env = {**os.environ, **{k: v for k, v in dotenv_values(args.env_file).items() if v}}
    with psycopg.connect(database_url("research", environ=env), prepare_threshold=None) as conn, \
            conn.cursor() as cur:
        cur.execute(QUERY, (sorted(args.seasons),))
        columns = [c.name for c in cur.description]
        frame = pd.DataFrame(cur.fetchall(), columns=columns)
    missing = sorted(set(args.seasons) - set(frame.season.unique()))
    if missing:
        raise SystemExit(f"PRIOR_SEASON_USAGE_EMPTY:{missing}")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    frame.round(3).to_csv(args.output, index=False)
    counts = frame.groupby("season").size().to_dict()
    print(f"PRIOR_SEASON_USAGE={args.output} players_by_season={counts}")


if __name__ == "__main__":
    main()
