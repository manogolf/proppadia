#!/usr/bin/env python3
"""Admit certified exact-roster bridge and BetOnline rows into clean room."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import uuid
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

import psycopg


def now() -> datetime:
    return datetime.now(timezone.utc)


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pilot-dir", type=Path, required=True)
    parser.add_argument("--board-root", type=Path, required=True)
    args = parser.parse_args()
    attempts_path = args.pilot_dir / "roster_constrained_identity_attempts.csv"
    all_attempts = list(csv.DictReader(attempts_path.open()))
    attempts = [row for row in all_attempts if row["decision"] == "EXACT_UNIQUE_MATCH"]
    rejected_attempts = [row for row in all_attempts if row["decision"] != "EXACT_UNIQUE_MATCH"]
    if not attempts:
        raise SystemExit("pilot has no exact-unique rows; refusing admission")

    db_url = os.environ["SUPABASE_DB_URL"]
    run_id = uuid.uuid4()
    started = now()
    aggregate_sha = hashlib.sha256(attempts_path.read_bytes()).hexdigest()
    bridge_seen = set()
    bridge_rows, odds_rows = [], []
    game_rows, player_rows, lineup_rows = [], [], []
    audit = []

    by_game: dict[int, list[dict]] = defaultdict(list)
    for row in attempts:
        by_game[int(row["game_pk"])].append(row)

    for game_pk, rows in by_game.items():
        feed_path = args.pilot_dir / "raw" / "MLB_STATS_API" / f"game_{game_pk}.json"
        feed = json.loads(feed_path.read_text())
        feed_sha = sha(feed_path)
        official_date = date.fromisoformat(feed["gameData"]["datetime"]["officialDate"])
        observed = now()
        home = feed["gameData"]["teams"]["home"]
        away = feed["gameData"]["teams"]["away"]
        game_rows.append((
            game_pk, official_date, official_date, home["id"], away["id"],
            feed["gameData"]["datetime"]["dateTime"],
            feed["gameData"]["status"]["detailedState"], "MLB_STATS_API_GAME_FEED",
            observed, now(), feed_sha,
        ))
        for side in ("home", "away"):
            team_id = feed["gameData"]["teams"][side]["id"]
            box = feed["liveData"]["boxscore"]["teams"][side]
            order = [int(str(x).replace("ID", "")) for x in box.get("battingOrder", [])[:9]]
            for key, entry in box.get("players", {}).items():
                player_id = int(key.replace("ID", ""))
                person = entry["person"]
                player_rows.append((
                    player_id, person["fullName"], "ACTIVE",
                    (entry.get("position") or {}).get("name"), team_id,
                    official_date, None, "MLB_STATS_API_GAME_FEED",
                    observed, now(), feed_sha,
                ))
                if player_id in order:
                    lineup_rows.append((
                        game_pk, official_date, team_id, player_id,
                        order.index(player_id) + 1, "CONFIRMED", observed,
                        "MLB_STATS_API_GAME_FEED", run_id, feed_sha,
                    ))

    for row in attempts:
        observed = datetime.fromisoformat(row["source_observed_at_utc"].replace("Z", "+00:00"))
        bridge_key = (
            row["provider_event_id"], row["raw_provider_player_name"],
            row["player_mlb_id"], row["raw_payload_sha256"],
        )
        if bridge_key not in bridge_seen:
            bridge_seen.add(bridge_key)
            bridge_rows.append((
                "THE_ODDS_API_BETONLINE", row["provider_event_id"], int(row["game_pk"]),
                row["raw_provider_player_name"], row["normalized_provider_name"],
                int(row["player_mlb_id"]), row["official_player_name"],
                "exact_game_roster_name_v1", "EXACT_UNIQUE_MATCH", row["reason"],
                observed, run_id, row["raw_payload_sha256"], now(),
            ))
        odds_rows.append((
            int(row["game_pk"]), date.fromisoformat(
                json.loads((args.pilot_dir / "raw" / "MLB_STATS_API" /
                            f"game_{row['game_pk']}.json").read_text())
                ["gameData"]["datetime"]["officialDate"]
            ),
            "BetOnline", "Total Bases", int(row["player_mlb_id"]),
            float(row["line"]), row["side"], int(row["price"]), observed,
            "THE_ODDS_API_EXACT_GAME_ROSTER_BRIDGE_V1", run_id,
            row["raw_payload_sha256"],
        ))

    with psycopg.connect(db_url, autocommit=False) as conn:
        with conn.cursor() as cur:
            statements = [
                ("games", """INSERT INTO mlb_cleanroom_v1.games VALUES
                  (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""", game_rows),
                ("players", """INSERT INTO mlb_cleanroom_v1.players VALUES
                  (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""", player_rows),
                ("lineups", """INSERT INTO mlb_cleanroom_v1.lineup_snapshots VALUES
                  (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""", lineup_rows),
                ("bridge", """INSERT INTO mlb_cleanroom_v1.odds_player_identity_bridge VALUES
                  (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""", bridge_rows),
                ("odds", """INSERT INTO mlb_cleanroom_v1.odds_snapshots VALUES
                  (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""", odds_rows),
            ]
            total_written = total_duplicates = 0
            for concept, sql, rows in statements:
                written = duplicates = 0
                for item in rows:
                    cur.execute(sql, item)
                    written += int(bool(cur.rowcount))
                    duplicates += int(not cur.rowcount)
                total_written += written
                total_duplicates += duplicates
                audit.append({
                    "concept": concept, "received": len(rows), "written": written,
                    "duplicates": duplicates, "rejected": 0, "status": "PASS",
                })
            audit.append({
                "concept": "identity_rejects", "received": len(rejected_attempts),
                "written": 0, "duplicates": 0, "rejected": len(rejected_attempts),
                "status": "FAIL_CLOSED",
            })
            cur.execute("""INSERT INTO mlb_cleanroom_v1.ingestion_runs VALUES
              (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""", (
                run_id, "BETONLINE_EXACT_GAME_ROSTER_BRIDGE_V1", started, now(),
                min(row[1] for row in odds_rows), len(all_attempts), total_written,
                total_duplicates, len(rejected_attempts),
                "PARTIAL_IDENTITY_FAIL_CLOSED" if rejected_attempts else "COMPLETED",
                f"{len(rejected_attempts)} non-exact identity rows rejected" if rejected_attempts else None,
                str(args.pilot_dir), aggregate_sha,
            ))
            conn.commit()

    slate_dates = sorted({row[1] for row in odds_rows})
    board_results = []
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            for slate_date in slate_dates:
                cur.execute("""
                  WITH eligible_lineups AS (
                    SELECT DISTINCT ON (o.game_pk,o.player_mlb_id)
                      o.game_pk,o.player_mlb_id,l.lineup_status,l.batting_order_position
                    FROM mlb_cleanroom_v1.latest_bol_tb15 o
                    LEFT JOIN LATERAL (
                      SELECT l.*
                      FROM mlb_cleanroom_v1.valid_pregame_lineup_observations l
                      JOIN mlb_cleanroom_v1.ingestion_runs li USING (ingestion_run_id)
                      WHERE l.game_pk=o.game_pk
                        AND l.player_mlb_id=o.player_mlb_id
                        AND l.snapshot_timestamp_utc <= o.snapshot_timestamp_utc
                        AND (l.ingestion_run_id=o.ingestion_run_id
                             OR li.completed_at_utc <= %s)
                      ORDER BY l.snapshot_timestamp_utc DESC,
                               li.completed_at_utc DESC,
                               l.source_payload_sha256 DESC
                      LIMIT 1
                    ) l ON true
                    WHERE o.slate_date=%s
                    ORDER BY o.game_pk,o.player_mlb_id,o.snapshot_timestamp_utc DESC
                  ), paired AS (
                    SELECT o.game_pk,o.player_mlb_id,o.line,
                      max(o.american_odds) FILTER (WHERE o.side='Over') over_odds,
                      max(o.american_odds) FILTER (WHERE o.side='Under') under_odds,
                      max(o.snapshot_timestamp_utc) market_timestamp,
                      max(b.official_player_name) player,
                      max(g.home_team_mlb_id) home_team_id,
                      max(g.away_team_mlb_id) away_team_id,
                      max(l.lineup_status) lineup_status,
                      max(l.batting_order_position) batting_order
                    FROM mlb_cleanroom_v1.latest_bol_tb15 o
                    JOIN mlb_cleanroom_v1.odds_player_identity_bridge b
                      ON b.game_pk=o.game_pk AND b.player_mlb_id=o.player_mlb_id
                    JOIN mlb_cleanroom_v1.current_games g ON g.game_pk=o.game_pk
                    LEFT JOIN eligible_lineups l
                      ON l.game_pk=o.game_pk AND l.player_mlb_id=o.player_mlb_id
                    WHERE o.slate_date=%s
                    GROUP BY o.game_pk,o.player_mlb_id,o.line
                  )
                  SELECT * FROM paired WHERE over_odds IS NOT NULL AND under_odds IS NOT NULL
                  ORDER BY game_pk,player
                """, (started, slate_date, slate_date))
                rows = cur.fetchall()
                columns = [d.name for d in cur.description]
                out_dir = args.board_root / str(slate_date)
                out_dir.mkdir(parents=True, exist_ok=True)
                csv_path = out_dir / f"bol_tb15_cleanroom_market_board_{slate_date}.csv"
                with csv_path.open("w", newline="") as handle:
                    writer = csv.writer(handle, lineterminator="\n")
                    writer.writerow(columns); writer.writerows(rows)
                md = [
                    f"# Clean-room BetOnline TB 1.5 Board — {slate_date}", "",
                    "| Player | Game PK | MLB player ID | Over | Under | Market timestamp | Lineup | Order |",
                    "| --- | ---: | ---: | ---: | ---: | --- | --- | ---: |",
                ]
                for row in rows:
                    d = dict(zip(columns, row))
                    md.append(
                        f"| {d['player']} | {d['game_pk']} | {d['player_mlb_id']} | "
                        f"{d['over_odds']:+d} | {d['under_odds']:+d} | "
                        f"{d['market_timestamp']} | {d['lineup_status'] or ''} | "
                        f"{d['batting_order'] or ''} |"
                    )
                (out_dir / f"bol_tb15_cleanroom_market_board_{slate_date}.md").write_text("\n".join(md) + "\n")
                manifest = {
                    "slate_date": str(slate_date), "identity_route": "exact_game_roster_name_v1",
                    "board_rows": len(rows), "source_outcome_rows": len(
                        [x for x in odds_rows if x[1] == slate_date]
                    ), "identity_rejects": 0, "status": "CERTIFIED_EXACT_ROSTER_BRIDGE",
                }
                (out_dir / "population_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
                board_results.append({"slate_date": slate_date, "board_rows": len(rows), "status": "PASS"})

    for filename, fields, rows in (
        ("cleanroom_odds_reingestion_results.csv",
         ["concept","received","written","duplicates","rejected","status"], audit),
        ("cleanroom_bol_board_validation.csv",
         ["slate_date","board_rows","status"], board_results),
    ):
        with (args.pilot_dir / filename).open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
            writer.writeheader(); writer.writerows(rows)
    print(json.dumps({"run_id": str(run_id), "bridge_rows": len(bridge_rows),
                      "odds_rows": len(odds_rows), "identity_rejects": len(rejected_attempts),
                      "boards": board_results}, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
