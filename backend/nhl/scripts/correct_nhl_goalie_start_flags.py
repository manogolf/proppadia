#!/usr/bin/env python3
"""Correct stored goalie start flags to the official boxscore starter (P2.41).

``nhl.goalie_game_logs_raw.start_flag`` was written by most ice time (2026,
before the 2026-11-01 rule fix) or left false for every goalie (2023-24). The
official boxscore's ``starter`` flag is the source (a pulled starter keeps the
start). Boxscores: the 2023-24 files retained by the 2026-10-16 PP-TOI
recovery, and the 2025 and 2026 files fetched for the P2.41 backfill. Only
``start_flag`` changes, and only where the stored value still equals the value
read for the preview (operator-approved 2026-11-01: "correct existing starter
data"). ``--apply`` writes one transaction through the ingestion login, a
receipt, the exact rollback SQL and a verify SQL.
"""
from __future__ import annotations

import argparse
import gzip
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psycopg

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from backend.nhl.database_access import database_url  # noqa: E402
from backend.nhl.scripts.run_nhl_points_hgb_pp_corrected_refit import research_env  # noqa: E402

OUT = ROOT / "artifacts/analysis/nhl/goalie_log_backfill_2026-11-01/start_flag_correction"
BOX_DIRS = {2023: ROOT / "artifacts/analysis/nhl/pp_toi_historical_recovery_2026-10-16/official_sources",
            2024: ROOT / "artifacts/analysis/nhl/pp_toi_historical_recovery_2026-10-16/official_sources",
            2025: ROOT / "artifacts/analysis/nhl/goalie_log_backfill_2026-11-01/season=2025/boxscores",
            2026: ROOT / "artifacts/analysis/nhl/goalie_log_backfill_2026-11-01/season=2026/boxscores"}


def box_starters() -> pd.DataFrame:
    rows = []
    paths = list(BOX_DIRS[2023].glob("game=*/boxscore.json.gz"))
    paths += list(BOX_DIRS[2025].glob("game=*_boxscore.json.gz")) + list(BOX_DIRS[2026].glob("game=*_boxscore.json.gz"))
    for path in paths:
        box = json.loads(gzip.open(path).read())
        for side in ("homeTeam", "awayTeam"):
            team = [g for g in (box.get("playerByGameStats") or {}).get(side, {}).get("goalies") or []]
            flags = [g.get("starter") for g in team]
            if not team or not all(isinstance(f, bool) for f in flags) or sum(flags) != 1:
                continue  # no usable flag for this team-game: leave stored values as they are
            for g in team:
                rows.append((int(box["id"]), int(g["playerId"]), bool(g["starter"])))
    return pd.DataFrame(rows, columns=["game_id", "player_id", "box_starter"]).drop_duplicates(["game_id", "player_id"])


def stored() -> pd.DataFrame:
    with psycopg.connect(database_url("research", environ=research_env()), prepare_threshold=None) as c, c.cursor() as cur:
        cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        cur.execute("""SELECT g.season, l.game_id::bigint, l.player_id::bigint, l.start_flag
                       FROM nhl.goalie_game_logs_raw l JOIN nhl.games g USING(game_id)
                       WHERE g.season IN (2023, 2024, 2025, 2026) AND substring(g.game_id::text,5,2) = '02'""")
        return pd.DataFrame(cur.fetchall(), columns=["season", "game_id", "player_id", "start_flag"])


def plan() -> tuple[pd.DataFrame, dict]:
    db, bx = stored(), box_starters()
    m = db.merge(bx, on=["game_id", "player_id"], how="left")
    change = m[m.box_starter.notna() & (m.start_flag.isna() | (m.start_flag.astype("boolean") != m.box_starter.astype("boolean")))]
    summary = {"stored_rows": int(len(db)), "rows_with_boxscore_flag": int(m.box_starter.notna().sum()),
               "rows_to_change": int(len(change)),
               "by_season": {int(s): {"rows": int(len(g)), "games": int(g.game_id.nunique()),
                                      "false_to_true": int((g.box_starter.astype(bool) & g.start_flag.eq(False)).sum()),
                                      "true_to_false": int((~g.box_starter.astype(bool) & g.start_flag.eq(True)).sum()),
                                      "null_to_value": int(g.start_flag.isna().sum())}
                             for s, g in change.groupby("season")},
               "rows_without_boxscore_flag_by_season": {int(s): int(n) for s, n in
                                                         m[m.box_starter.isna()].groupby("season").size().items()}}
    return change, summary


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    change, summary = plan()
    print(json.dumps(summary, indent=1))
    if not a.apply:
        return
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")
    run = OUT / f"apply_{stamp}"
    run.mkdir(parents=True, exist_ok=False)
    change.to_csv(run / "changes_before_after.csv", index=False)
    updated = []
    with psycopg.connect(database_url("ingestion", environ=research_env()), prepare_threshold=None) as c, c.cursor() as cur:
        cur.execute("SELECT current_user"); login = cur.fetchone()[0]
        for r in change.itertuples(index=False):
            old = None if pd.isna(r.start_flag) else bool(r.start_flag)
            cur.execute("""UPDATE nhl.goalie_game_logs_raw SET start_flag = %s
                           WHERE player_id = %s AND game_id = %s AND start_flag IS NOT DISTINCT FROM %s
                           RETURNING player_id""", (bool(r.box_starter), int(r.player_id), int(r.game_id), old))
            if cur.fetchone():
                updated.append((int(r.game_id), int(r.player_id), old, bool(r.box_starter)))
        if len(updated) != len(change):
            c.rollback()
            raise SystemExit(f"STORED_VALUES_CHANGED_SINCE_PREVIEW:{len(updated)}/{len(change)}; rolled back")
        c.commit()
    sql = lambda v: "NULL" if v is None else ("TRUE" if v else "FALSE")
    (run / "rollback_start_flags.sql").write_text(
        "-- Restores start_flag exactly as before this correction (P2.41).\nBEGIN;\n" + "".join(
            f"UPDATE nhl.goalie_game_logs_raw SET start_flag = {sql(o)} WHERE player_id = {p} AND game_id = {g};\n"
            for g, p, o, unused in updated) + "COMMIT;\n")
    (run / "start_flag_verify.sql").write_text("""-- P2.41 verify: start flags after the 2026-11-01 correction.
-- Expected: every regular-season game 2023-2026 with goalie rows has exactly two starters,
-- except team-games without a usable boxscore flag (see receipt).
SELECT g.season, count(DISTINCT g.game_id) AS games,
       count(*) FILTER (WHERE l.start_flag) AS starters,
       count(*) FILTER (WHERE l.start_flag IS NULL) AS null_flags
FROM nhl.goalie_game_logs_raw l JOIN nhl.games g USING (game_id)
WHERE g.season IN (2023, 2024, 2025, 2026) AND substring(g.game_id::text, 5, 2) = '02'
GROUP BY 1 ORDER BY 1;

SELECT g.season, l.game_id, count(*) FILTER (WHERE l.start_flag) AS starters
FROM nhl.goalie_game_logs_raw l JOIN nhl.games g USING (game_id)
WHERE g.season IN (2023, 2024, 2025, 2026) AND substring(g.game_id::text, 5, 2) = '02'
GROUP BY 1, 2 HAVING count(*) FILTER (WHERE l.start_flag) <> 2 ORDER BY 1, 2;
""")
    receipt = {"applied_utc": stamp, "login": login, "rows_updated": len(updated), **summary}
    (run / "receipt.json").write_text(json.dumps(receipt, indent=1) + "\n")
    print(json.dumps({"run": str(run.relative_to(ROOT)), "rows_updated": len(updated), "login": login}, indent=1))


if __name__ == "__main__":
    main()
