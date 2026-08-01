#!/usr/bin/env python3
"""Materialize an immutable clean-room BetOnline capture snapshot."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
from datetime import datetime
from pathlib import Path

import psycopg


def write_csv(path: Path, fields: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--run-tag", required=True)
    parser.add_argument("--pilot-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--run-index", type=Path, required=True)
    parser.add_argument("--board-dir", type=Path, required=True)
    args = parser.parse_args()
    db_url = os.environ["SUPABASE_DB_URL"]

    with psycopg.connect(db_url) as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                """SELECT * FROM mlb_cleanroom_v1.ingestion_runs
                   WHERE ingestion_run_id=%s""", (args.run_id,)
            )
            run = dict(cur.fetchone())
            cur.execute(
                """SELECT ingestion_run_id,started_at_utc FROM mlb_cleanroom_v1.ingestion_runs
                   WHERE source_name='BETONLINE_EXACT_GAME_ROSTER_BRIDGE_V1'
                     AND started_at_utc < %s
                   ORDER BY started_at_utc DESC LIMIT 1""",
                (run["started_at_utc"],),
            )
            prior = cur.fetchone()

            cur.execute(
                """SELECT o.game_pk,o.player_mlb_id,o.line,o.side,o.american_odds,
                          o.snapshot_timestamp_utc,b.official_player_name AS player,
                          g.home_team_mlb_id,g.away_team_mlb_id,
                          coalesce(l.lineup_status,'UNCONFIRMED') lineup_status,
                          l.batting_order_position,
                          l.snapshot_timestamp_utc lineup_observed_at_utc,
                          l.ingestion_run_id lineup_ingestion_run_id,
                          CASE WHEN l.game_pk IS NOT NULL
                               THEN 'LINEUP_VALID_PREGAME'
                               ELSE 'LINEUP_NOT_RUN_VISIBLE' END lineup_temporal_classification,
                          o.source_payload_sha256
                   FROM mlb_cleanroom_v1.odds_snapshots o
                   JOIN mlb_cleanroom_v1.odds_player_identity_bridge b
                     ON b.ingestion_run_id=o.ingestion_run_id
                    AND b.game_pk=o.game_pk AND b.player_mlb_id=o.player_mlb_id
                    AND b.raw_payload_sha256=o.source_payload_sha256
                   JOIN mlb_cleanroom_v1.current_games g ON g.game_pk=o.game_pk
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
                   WHERE o.ingestion_run_id=%s AND o.line=1.5
                     AND o.snapshot_timestamp_utc < g.scheduled_start_utc
                   ORDER BY o.game_pk,o.player_mlb_id,o.line,o.side""",
                (run["started_at_utc"], args.run_id),
            )
            sides = [dict(row) for row in cur.fetchall()]
            cur.execute(
                """WITH governing_game AS (
                     SELECT game_pk,max(snapshot_timestamp_utc) governing_market_at
                     FROM mlb_cleanroom_v1.odds_snapshots o
                     WHERE o.ingestion_run_id=%s AND o.line=1.5
                       AND EXISTS (
                         SELECT 1 FROM mlb_cleanroom_v1.current_games g
                         WHERE g.game_pk=o.game_pk
                           AND o.snapshot_timestamp_utc < g.scheduled_start_utc
                       )
                     GROUP BY game_pk
                   )
                   SELECT DISTINCT ON (l.game_pk,l.team_mlb_id,l.player_mlb_id)
                          l.game_pk,l.team_mlb_id,l.player_mlb_id,l.lineup_status,
                          l.batting_order_position,l.snapshot_timestamp_utc,
                          l.source,l.source_payload_sha256,l.ingestion_run_id
                   FROM governing_game m
                   JOIN mlb_cleanroom_v1.valid_pregame_lineup_observations l
                     ON l.game_pk=m.game_pk
                    AND l.snapshot_timestamp_utc <= m.governing_market_at
                   JOIN mlb_cleanroom_v1.ingestion_runs li USING (ingestion_run_id)
                   WHERE l.ingestion_run_id=%s OR li.completed_at_utc <= %s
                   ORDER BY l.game_pk,l.team_mlb_id,l.player_mlb_id,
                            l.snapshot_timestamp_utc DESC,li.completed_at_utc DESC,
                            l.source_payload_sha256 DESC""",
                (args.run_id, args.run_id, run["started_at_utc"]),
            )
            lineups = [dict(row) for row in cur.fetchall()]
            cur.execute(
                """SELECT provider_event_id,game_pk,raw_player_name,normalized_player_name,
                          player_mlb_id,official_player_name,normalization_version,
                          decision,decision_reason,source_observed_at_utc,
                          raw_payload_sha256
                   FROM mlb_cleanroom_v1.odds_player_identity_bridge
                   WHERE ingestion_run_id=%s
                   ORDER BY provider_event_id,player_mlb_id,raw_payload_sha256""",
                (args.run_id,),
            )
            identities = [dict(row) for row in cur.fetchall()]
            cur.execute(
                """SELECT count(*) official_games,
                          count(*) FILTER (WHERE game_status IN ('Postponed','Cancelled')) unavailable,
                          min(scheduled_start_utc) first_pitch_utc
                   FROM mlb_cleanroom_v1.current_games WHERE slate_date=%s""",
                (args.date,),
            )
            coverage = dict(cur.fetchone())

            prior_sides = []
            prior_lineups = []
            if prior:
                cur.execute(
                    """SELECT game_pk,player_mlb_id,line,side,american_odds
                       FROM mlb_cleanroom_v1.odds_snapshots
                       WHERE ingestion_run_id=%s AND line=1.5""",
                    (prior["ingestion_run_id"],),
                )
                prior_sides = [dict(row) for row in cur.fetchall()]
                cur.execute(
                    """SELECT game_pk,player_mlb_id,batting_order_position
                       FROM mlb_cleanroom_v1.lineup_snapshots WHERE ingestion_run_id=%s""",
                    (prior["ingestion_run_id"],),
                )
                prior_lineups = [dict(row) for row in cur.fetchall()]

    paired = {}
    tb15_identity_keys = {
        (row["game_pk"], row["player_mlb_id"], row["source_payload_sha256"])
        for row in sides
    }
    identities = [
        row for row in identities
        if (row["game_pk"], row["player_mlb_id"], row["raw_payload_sha256"])
        in tb15_identity_keys
    ]
    for row in sides:
        key = (row["game_pk"], row["player_mlb_id"], str(row["line"]))
        pair = paired.setdefault(key, {
            "slate_date": args.date, "game_pk": row["game_pk"],
            "player_mlb_id": row["player_mlb_id"], "player": row["player"],
            "team_mlb_id": "", "opponent_mlb_id": "",
            "over_odds": "", "under_odds": "", "market_timestamp_utc": "",
            "lineup_status": row["lineup_status"],
            "batting_order": row["batting_order_position"] or "",
            "lineup_observed_at_utc": row["lineup_observed_at_utc"] or "",
            "lineup_ingestion_run_id": row["lineup_ingestion_run_id"] or "",
            "lineup_temporal_classification": row["lineup_temporal_classification"],
            "identity_decision": "EXACT_UNIQUE_MATCH",
        })
        pair["over_odds" if row["side"] == "Over" else "under_odds"] = row["american_odds"]
        pair["market_timestamp_utc"] = max(
            str(pair["market_timestamp_utc"]), str(row["snapshot_timestamp_utc"])
        )
    two_sided = [row for row in paired.values() if row["over_odds"] != "" and row["under_odds"] != ""]

    current_keys = {(r["game_pk"], r["player_mlb_id"], str(r["line"]), r["side"]): r for r in sides}
    previous_keys = {
        (r["game_pk"], r["player_mlb_id"], str(r["line"]), r["side"]): r for r in prior_sides
    }
    new_ids = set(current_keys) - set(previous_keys)
    removed = set(previous_keys) - set(current_keys)
    changed_side_keys = {
        key
        for key in set(current_keys) & set(previous_keys)
        if current_keys[key]["american_odds"] != previous_keys[key]["american_odds"]
    }
    price_changes = len(changed_side_keys)
    new_markets = {key[:3] for key in new_ids}
    removed_markets = {key[:3] for key in removed}
    changed_markets = {key[:3] for key in changed_side_keys}
    old_lineups = {(r["game_pk"], r["player_mlb_id"]): r["batting_order_position"] for r in prior_lineups}
    new_lineups = {(r["game_pk"], r["player_mlb_id"]): r["batting_order_position"] for r in lineups}
    newly_confirmed = len(set(new_lineups) - set(old_lineups))
    lineup_changes = sum(
        new_lineups[key] != old_lineups[key] for key in set(new_lineups) & set(old_lineups)
    )
    confirmed_games = len({r["game_pk"] for r in lineups if r["batting_order_position"]})
    phase = (
        "PRELINEUP" if confirmed_games == 0
        else "PARTIAL_LINEUPS" if confirmed_games < max(1, coverage["official_games"] * 0.75)
        else "MOST_LINEUPS_CONFIRMED"
    )

    args.output_dir.mkdir(parents=True, exist_ok=False)
    write_csv(args.output_dir / "bol_tb15_market_sides.csv", list(sides[0]), sides)
    write_csv(args.output_dir / "bol_tb15_two_sided_markets.csv", list(two_sided[0]), two_sided)
    lineup_fields = list(lineups[0]) if lineups else [
        "game_pk","team_mlb_id","player_mlb_id","lineup_status",
        "batting_order_position","snapshot_timestamp_utc","source","source_payload_sha256",
    ]
    write_csv(args.output_dir / "lineup_snapshot.csv", lineup_fields, lineups)
    write_csv(args.output_dir / "identity_audit.csv", list(identities[0]), identities)

    referenced = {}
    for row in sides + lineups:
        referenced[row["source_payload_sha256"]] = True
    hash_rows = []
    for path in sorted(args.pilot_dir.rglob("*.json")):
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        hash_rows.append({
            "source": "MLB_STATS_API", "raw_payload_path": str(path),
            "sha256": digest, "referenced_by_admitted_row": digest in referenced,
        })
    for path_text in sorted({row["raw_payload_path"] for row in csv.DictReader(
        (args.pilot_dir / "roster_constrained_identity_attempts.csv").open()
    )}):
        path = Path(path_text)
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        hash_rows.append({
            "source": "THE_ODDS_API", "raw_payload_path": str(path),
            "sha256": digest, "referenced_by_admitted_row": digest in referenced,
        })
    write_csv(args.output_dir / "source_hash_manifest.csv", list(hash_rows[0]), hash_rows)

    pilot_attempts = list(csv.DictReader(
        (args.pilot_dir / "roster_constrained_identity_attempts.csv").open()
    ))
    tb15_attempts = [r for r in pilot_attempts if str(r["line"]) == "1.5"]
    tb15_rejects = sum(r["decision"] != "EXACT_UNIQUE_MATCH" for r in tb15_attempts)
    manifest = {
        "run_tag": args.run_tag, "ingestion_run_id": args.run_id,
        "slate_date": args.date, "capture_timestamp_utc": run["started_at_utc"].isoformat(),
        "phase": phase, "official_games": coverage["official_games"],
        "provider_events": len({r["provider_event_id"] for r in identities}),
        "raw_odds_sides": len(tb15_attempts),
        "exact_id_admitted_sides": len(sides), "identity_rejects": tb15_rejects,
        "two_sided_markets": len(two_sided), "games_with_confirmed_lineups": confirmed_games,
        "confirmed_lineup_players": len(lineups), "batting_order_rows": len(lineups),
        "new_market_side_identities": len(new_ids), "market_sides_no_longer_present": len(removed),
        "new_market_identities": len(new_markets),
        "markets_no_longer_present": len(removed_markets),
        "markets_with_price_changes": len(changed_markets),
        "price_changes": price_changes, "newly_confirmed_starters": newly_confirmed,
        "lineup_changes": lineup_changes,
        "earliest_first_pitch_utc": coverage["first_pitch_utc"].isoformat(),
        "final_population_frozen": False,
        "source_allowlist": ["mlb_cleanroom_v1", "MLB_STATS_API", "THE_ODDS_API"],
        "identity_normalization_version": "exact_game_roster_name_v1",
    }
    (args.output_dir / "run_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")

    args.board_dir.mkdir(parents=True, exist_ok=True)
    board_csv = args.board_dir / f"bol_tb15_cleanroom_market_board_{args.date}.csv"
    write_csv(board_csv, list(two_sided[0]), two_sided)
    md = [
        f"# Clean-room BetOnline TB 1.5 Board — {args.date}", "",
        f"Run: `{args.run_tag}`", f"Phase: `{phase}`", "",
        "| Player | Game PK | MLB player ID | Over | Under | Market timestamp | Lineup | Order |",
        "| --- | ---: | ---: | ---: | ---: | --- | --- | ---: |",
    ]
    for row in two_sided:
        md.append(
            f"| {row['player']} | {row['game_pk']} | {row['player_mlb_id']} | "
            f"{row['over_odds']:+d} | {row['under_odds']:+d} | "
            f"{row['market_timestamp_utc']} | {row['lineup_status']} | "
            f"{row['batting_order']} |"
        )
    (args.board_dir / f"bol_tb15_cleanroom_market_board_{args.date}.md").write_text(
        "\n".join(md) + "\n"
    )
    (args.board_dir / "population_manifest.json").write_text(json.dumps({
        "slate_date": args.date, "run_tag": args.run_tag, "phase": phase,
        "board_rows": len(two_sided), "raw_tb15_sides": len(tb15_attempts),
        "admitted_tb15_sides": len(sides), "identity_rejects": tb15_rejects,
        "final_population_frozen": False, "status": "CURRENT_CAPTURE_NOT_FINAL",
    }, indent=2) + "\n")

    existing = []
    if args.run_index.exists():
        existing = list(csv.DictReader(args.run_index.open()))
    existing.append({
        "run_tag": args.run_tag, "ingestion_run_id": args.run_id,
        "capture_timestamp_utc": run["started_at_utc"].isoformat(), "phase": phase,
        "provider_events": manifest["provider_events"], "raw_odds_sides": len(tb15_attempts),
        "admitted_sides": len(sides), "identity_rejects": tb15_rejects,
        "two_sided_markets": len(two_sided), "confirmed_lineup_games": confirmed_games,
        "snapshot_path": str(args.output_dir),
    })
    write_csv(args.run_index, list(existing[0]), existing)
    print(json.dumps(manifest))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
