#!/usr/bin/env python3
"""Forward-only authoritative MLB clean-room source acquisition."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import uuid
from datetime import date, datetime, timezone
from pathlib import Path

import psycopg
import requests

MLB = "https://statsapi.mlb.com/api"
ODDS = "https://api.the-odds-api.com/v4"


def now() -> datetime:
    return datetime.now(timezone.utc)


def raw_get(url: str, params: dict, path: Path) -> tuple[dict | list, str, datetime]:
    observed = now()
    response = requests.get(url, params=params, timeout=45)
    response.raise_for_status()
    payload = response.content
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    return response.json(), hashlib.sha256(payload).hexdigest(), observed


def insert_many(cur, sql: str, rows: list[tuple]) -> tuple[int, int]:
    written = duplicates = 0
    for row in rows:
        cur.execute(sql, row)
        if cur.rowcount:
            written += 1
        else:
            duplicates += 1
    return written, duplicates


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--completed-date", required=True)
    parser.add_argument("--raw-root", type=Path, default=Path("backend/mlb/exports/cleanroom_v1/raw"))
    parser.add_argument("--evidence-dir", type=Path, required=True)
    args = parser.parse_args()
    slate = date.fromisoformat(args.date)
    completed = date.fromisoformat(args.completed_date)
    run_tag = f"cleanroom_{now():%Y%m%dT%H%M%SZ}"
    raw_manifest, cycle, identity = [], [], []
    db_url = os.environ["SUPABASE_DB_URL"]
    odds_key = os.environ.get("ODDS_API_KEY", "")
    totals = {"received": 0, "written": 0, "duplicates": 0, "rejects": 0}

    with psycopg.connect(db_url, autocommit=False) as conn:
        with conn.cursor() as cur:
            run_id = uuid.uuid4()
            run_started = now()
            raw_dir = args.raw_root / "MLB_STATS_API" / args.date / run_tag
            schedule, schedule_sha, observed = raw_get(
                f"{MLB}/v1/schedule",
                {"sportId": 1, "date": args.date, "hydrate": "team,linescore"},
                raw_dir / "schedule.json",
            )
            raw_manifest.append(("MLB_STATS_API", args.date, run_tag, str(raw_dir / "schedule.json"), observed.isoformat(), schedule_sha, "PRESERVED"))
            games, team_ids = [], set()
            for block in schedule.get("dates", []):
                for game in block.get("games", []):
                    home = game["teams"]["home"]["team"]["id"]
                    away = game["teams"]["away"]["team"]["id"]
                    team_ids.update((home, away))
                    games.append((game["gamePk"], slate, game["officialDate"], home, away,
                                  game["gameDate"], game["status"]["detailedState"],
                                  "MLB_STATS_API", observed, now(), schedule_sha))
            w, d = insert_many(cur, """
                INSERT INTO mlb_cleanroom_v1.games VALUES
                (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING
            """, games)
            totals["received"] += len(games); totals["written"] += w; totals["duplicates"] += d

            teams_path = raw_dir / "teams.json"
            teams_payload, teams_sha, teams_observed = raw_get(
                f"{MLB}/v1/teams", {"sportId": 1, "hydrate": "league,division"}, teams_path
            )
            raw_manifest.append(("MLB_STATS_API", args.date, run_tag, str(teams_path), teams_observed.isoformat(), teams_sha, "PRESERVED"))
            team_rows = []
            for t in teams_payload.get("teams", []):
                if t["id"] not in team_ids:
                    continue
                team_rows.append((t["id"], t.get("abbreviation", ""), t["name"],
                                  (t.get("league") or {}).get("name"),
                                  (t.get("division") or {}).get("name"), slate, None,
                                  "MLB_STATS_API", teams_observed, now(), teams_sha))
            w, d = insert_many(cur, """
                INSERT INTO mlb_cleanroom_v1.teams VALUES
                (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING
            """, team_rows)
            totals["received"] += len(team_rows); totals["written"] += w; totals["duplicates"] += d

            player_rows, lineup_rows, result_rows, fresh_players = [], [], [], {}
            all_game_pks = [g[0] for g in games]
            # Include the latest completed slate directly from MLB.
            completed_dir = args.raw_root / "MLB_STATS_API" / args.completed_date / run_tag
            completed_schedule, completed_sha, completed_observed = raw_get(
                f"{MLB}/v1/schedule", {"sportId": 1, "date": args.completed_date},
                completed_dir / "schedule.json",
            )
            raw_manifest.append(("MLB_STATS_API", args.completed_date, run_tag, str(completed_dir / "schedule.json"), completed_observed.isoformat(), completed_sha, "PRESERVED"))
            completed_pks = [g["gamePk"] for b in completed_schedule.get("dates", []) for g in b.get("games", [])]
            for game_pk, game_date in [(x, slate) for x in all_game_pks] + [(x, completed) for x in completed_pks]:
                game_dir = args.raw_root / "MLB_STATS_API" / str(game_date) / run_tag
                feed, feed_sha, feed_observed = raw_get(
                    f"{MLB}/v1.1/game/{game_pk}/feed/live", {}, game_dir / f"game_{game_pk}.json"
                )
                raw_manifest.append(("MLB_STATS_API", str(game_date), run_tag, str(game_dir / f"game_{game_pk}.json"), feed_observed.isoformat(), feed_sha, "PRESERVED"))
                status = feed["gameData"]["status"]["detailedState"]
                for side in ("home", "away"):
                    team_id = feed["gameData"]["teams"][side]["id"]
                    box = feed["liveData"]["boxscore"]["teams"][side]
                    batting_order = box.get("battingOrder", [])
                    for idx, raw_id in enumerate(batting_order[:9], 1):
                        player_id = int(str(raw_id).replace("ID", ""))
                        lineup_rows.append((game_pk, game_date, team_id, player_id, idx,
                                            "CONFIRMED" if batting_order else "UNCONFIRMED",
                                            feed_observed, "MLB_STATS_API", run_id, feed_sha))
                    for key, entry in box.get("players", {}).items():
                        player_id = int(key.replace("ID", ""))
                        person = entry["person"]
                        fresh_players[player_id] = (
                            person.get("fullName", ""), team_id,
                            (entry.get("position") or {}).get("name"),
                            feed_observed, feed_sha,
                        )
                        if status == "Final":
                            stats = (entry.get("stats") or {}).get("batting") or {}
                            if stats:
                                result_rows.append((game_pk, game_date, player_id, team_id,
                                    stats.get("plateAppearances"), stats.get("atBats"), stats.get("hits"),
                                    stats.get("hits", 0) - stats.get("doubles", 0) - stats.get("triples", 0) - stats.get("homeRuns", 0),
                                    stats.get("doubles"), stats.get("triples"), stats.get("homeRuns"),
                                    stats.get("totalBases"), "Final", "MLB_STATS_API",
                                    feed_observed, now(), feed_sha))
            for player_id, (name, team_id, position, player_observed, player_sha) in fresh_players.items():
                player_rows.append((player_id, name, "ACTIVE", position, team_id, slate, None,
                                    "MLB_STATS_API_GAME_FEED", player_observed, now(), player_sha))
            for table, sql, rows in (
                ("players", "INSERT INTO mlb_cleanroom_v1.players VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", player_rows),
                ("lineups", "INSERT INTO mlb_cleanroom_v1.lineup_snapshots VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", lineup_rows),
                ("results", "INSERT INTO mlb_cleanroom_v1.player_game_results VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING", result_rows),
            ):
                w, d = insert_many(cur, sql, rows)
                totals["received"] += len(rows); totals["written"] += w; totals["duplicates"] += d
                cycle.append((table, len(rows), w, d, 0, "PASS"))

            odds_rows, odds_received, odds_rejects = [], 0, 0
            if odds_key:
                odds_dir = args.raw_root / "THE_ODDS_API" / args.date / run_tag
                events, event_sha, odds_observed = raw_get(
                    f"{ODDS}/sports/baseball_mlb/events", {"apiKey": odds_key},
                    odds_dir / "events.json",
                )
                raw_manifest.append(("THE_ODDS_API", args.date, run_tag, str(odds_dir / "events.json"), odds_observed.isoformat(), event_sha, "PRESERVED"))
                # The provider's player-prop payload exposes player names, not MLB IDs.
                # Preserve payloads but fail closed: no name-based identity binding.
                for event in events:
                    event_odds, event_odds_sha, event_obs = raw_get(
                        f"{ODDS}/sports/baseball_mlb/events/{event['id']}/odds",
                        {"apiKey": odds_key, "regions": "us", "markets": "batter_total_bases", "oddsFormat": "american"},
                        odds_dir / f"event_{event['id']}.json",
                    )
                    raw_manifest.append(("THE_ODDS_API", args.date, run_tag, str(odds_dir / f"event_{event['id']}.json"), event_obs.isoformat(), event_odds_sha, "PRESERVED"))
                    for book in event_odds.get("bookmakers", []):
                        if book.get("key") not in {"betonlineag", "betonline"}:
                            continue
                        for market in book.get("markets", []):
                            odds_received += len(market.get("outcomes", []))
                            odds_rejects += len(market.get("outcomes", []))
            totals["rejects"] += odds_rejects
            identity.append(("BetOnline TB 1.5", "game_pk|player_mlb_id", odds_received, 0, odds_rejects,
                             "FAIL_CLOSED_PROVIDER_HAS_NO_MLB_PLAYER_ID"))
            cycle.append(("odds", odds_received, 0, 0, odds_rejects,
                          "IDENTITY_REJECTED" if odds_rejects else "NO_CURRENT_BOL_OFFERS"))

            cur.execute("""
              INSERT INTO mlb_cleanroom_v1.ingestion_runs VALUES
              (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (run_id, "MLB_STATS_API_AND_THE_ODDS_API", run_started, now(), slate,
                  totals["received"] + odds_received, totals["written"], totals["duplicates"],
                  totals["rejects"], "PARTIAL_IDENTITY_FAIL_CLOSED" if odds_rejects else "COMPLETED",
                  "BetOnline player props lack MLB player IDs; no name join attempted" if odds_rejects else None,
                  str(args.raw_root), hashlib.sha256("".join(r[5] for r in raw_manifest).encode()).hexdigest()))
            conn.commit()

    args.evidence_dir.mkdir(parents=True, exist_ok=True)
    for name, fields, rows in (
        ("cleanroom_raw_payload_manifest.csv", ["source","slate_date","run_tag","raw_payload_path","source_observed_at_utc","sha256","status"], raw_manifest),
        ("cleanroom_identity_audit.csv", ["concept","exact_key","rows_received","rows_admitted","identity_rejects","status"], identity),
        ("cleanroom_source_cycle_results.csv", ["concept","rows_received","rows_written","duplicates","identity_rejects","status"], cycle),
    ):
        with (args.evidence_dir / name).open("w", newline="") as handle:
            writer = csv.writer(handle, lineterminator="\n")
            writer.writerow(fields); writer.writerows(rows)
    print(json.dumps({"run_tag": run_tag, **totals}))
    return 2 if odds_rejects else 0


if __name__ == "__main__":
    raise SystemExit(main())
