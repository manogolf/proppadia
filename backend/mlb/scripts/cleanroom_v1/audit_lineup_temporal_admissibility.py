#!/usr/bin/env python3
"""Read-only clean-room lineup temporal-admissibility audit."""

from __future__ import annotations

import argparse
import csv
import json
import os
import statistics
from collections import Counter
from datetime import datetime
from pathlib import Path

import psycopg

from backend.mlb.scripts.cleanroom_v1.closeout_cleanroom_bol_tb15 import (
    EVIDENCE_ROOT,
    EXPORT_ROOT,
    latest_schedule,
    load_runs,
    parse_timestamp,
)
from backend.mlb.scripts.cleanroom_v1.manage_cleanroom_bol_tb15_under_hypotheses import (
    ROOT,
    write_csv,
)

FAILED_RUN_ID = "14951a25-57cb-49f1-88c1-15424cac4f94"
OUTPUT = (
    ROOT / "artifacts/analysis/model_development/"
    "mlb_cleanroom_lineup_temporal_admissibility_audit/2026-08-01"
)


def source_rows(ingestion_run_id: str | None, slate: str | None) -> list[dict]:
    clauses, values = [], []
    if ingestion_run_id:
        clauses.append("l.ingestion_run_id=%s")
        values.append(ingestion_run_id)
    if slate:
        clauses.append("l.slate_date=%s")
        values.append(slate)
    where = " AND ".join(clauses) or "TRUE"
    sql = f"""SELECT l.game_pk,l.slate_date source_derived_slate_date,
      l.player_mlb_id,l.batting_order_position batting_order,
      g.scheduled_start_utc,l.snapshot_timestamp_utc source_observed_at_utc,
      ir.completed_at_utc ingested_at_utc,l.ingestion_run_id,
      l.source_payload_sha256,
      CASE
        WHEN g.scheduled_start_utc IS NULL THEN 'MISSING_SCHEDULE_TIME'
        WHEN l.snapshot_timestamp_utc IS NULL THEN 'MISSING_OBSERVATION_TIME'
        WHEN l.snapshot_timestamp_utc >= g.scheduled_start_utc
          THEN 'POST_FIRST_PITCH_OBSERVATION'
        WHEN l.snapshot_timestamp_utc < g.scheduled_start_utc
          THEN 'VALID_PREGAME_OBSERVATION'
        ELSE 'TEMPORALLY_AMBIGUOUS'
      END temporal_classification,
      extract(epoch FROM (l.snapshot_timestamp_utc-g.scheduled_start_utc))/60.0
        minutes_after_first_pitch
      FROM mlb_cleanroom_v1.lineup_snapshots l
      LEFT JOIN mlb_cleanroom_v1.current_games g USING (game_pk)
      JOIN mlb_cleanroom_v1.ingestion_runs ir USING (ingestion_run_id)
      WHERE {where}
      ORDER BY l.slate_date,l.game_pk,l.player_mlb_id,l.snapshot_timestamp_utc"""
    with psycopg.connect(os.environ["SUPABASE_DB_URL"]) as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(sql, values)
            return [dict(row) for row in cur.fetchall()]


def temporal_view_audit(slate: str | None) -> list[dict]:
    where = "WHERE slate_date=%s" if slate else ""
    values = (slate,) if slate else ()
    with psycopg.connect(os.environ["SUPABASE_DB_URL"]) as conn:
        with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            cur.execute(
                f"""SELECT temporal_classification,count(*) observation_rows,
                  count(DISTINCT game_pk) games,
                  count(DISTINCT ingestion_run_id) ingestion_runs
                  FROM mlb_cleanroom_v1.lineup_temporal_observation_audit
                  {where}
                  GROUP BY temporal_classification
                  ORDER BY temporal_classification""",
                values,
            )
            return [dict(row) for row in cur.fetchall()]


def classify_snapshot_market(
    market: dict, lineups: list[dict], pitch: datetime, run_id: str,
    ingestion_times: dict[str, tuple[datetime, datetime | None]],
) -> dict:
    market_at = parse_timestamp(market["market_timestamp_utc"])
    candidates = [
        row for row in lineups
        if row["game_pk"] == market["game_pk"]
        and row["player_mlb_id"] == market["player_mlb_id"]
        and str(row.get("batting_order_position", "")) == str(market.get("batting_order", ""))
    ]
    if market.get("lineup_status") != "CONFIRMED" or not market.get("batting_order"):
        return {"classification": "ORDER_NOT_CONFIRMED", "lineup_observed_at_utc": "", "lineup_ingestion_run_id": ""}
    if not candidates:
        return {"classification": "LINEUP_NOT_RUN_VISIBLE", "lineup_observed_at_utc": "", "lineup_ingestion_run_id": ""}
    candidate = max(candidates, key=lambda row: parse_timestamp(row["snapshot_timestamp_utc"]))
    observed = parse_timestamp(candidate["snapshot_timestamp_utc"])
    candidate_run = candidate.get("ingestion_run_id", run_id)
    if observed >= pitch:
        classification = "LINEUP_POST_FIRST_PITCH"
    elif observed > market_at:
        classification = "LINEUP_AFTER_GOVERNING_CAPTURE"
    elif candidate_run != run_id and not (
        candidate_run in ingestion_times and run_id in ingestion_times
        and ingestion_times[candidate_run][1] is not None
        and ingestion_times[candidate_run][1] <= ingestion_times[run_id][0]
    ):
        classification = "LINEUP_NOT_RUN_VISIBLE"
    else:
        classification = "LINEUP_VALID_PREGAME"
    return {
        "classification": classification,
        "lineup_observed_at_utc": observed.isoformat(),
        "lineup_ingestion_run_id": candidate_run,
    }


def frozen_artifact_audit() -> list[dict]:
    with psycopg.connect(os.environ["SUPABASE_DB_URL"]) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ingestion_run_id,started_at_utc,completed_at_utc "
                "FROM mlb_cleanroom_v1.ingestion_runs"
            )
            ingestion_times = {
                str(run_id): (started, completed)
                for run_id, started, completed in cur.fetchall()
            }
    output = []
    for slate in ("2026-07-29", "2026-07-30", "2026-07-31"):
        runs, failures = load_runs(slate)
        if failures or not runs:
            output.append({"slate_date": slate, "artifact": "CAPTURE_SPINE", "temporal_classification": "INSUFFICIENT_LINEAGE"})
            continue
        games, _ = latest_schedule(slate, runs)
        pitches = {str(game["gamePk"]): parse_timestamp(game["gameDate"]) for game in games}
        audit_by_identity = {}
        for run in runs:
            snapshot = run["snapshot"]
            lineups = list(csv.DictReader((snapshot / "lineup_snapshot.csv").open()))
            markets = list(csv.DictReader((snapshot / "bol_tb15_two_sided_markets.csv").open()))
            for market in markets:
                result = classify_snapshot_market(
                    market, lineups, pitches[market["game_pk"]],
                    run["manifest"]["ingestion_run_id"],
                    ingestion_times,
                )
                key = (run["run_tag"], market["game_pk"], market["player_mlb_id"])
                audit_by_identity[key] = {
                    "slate_date": slate, "artifact": "IMMUTABLE_SNAPSHOT",
                    "governing_run_tag": run["run_tag"],
                    "game_pk": market["game_pk"],
                    "player_mlb_id": market["player_mlb_id"],
                    "market_timestamp_utc": market["market_timestamp_utc"],
                    **result,
                    "failed_ingestion_referenced": result["lineup_ingestion_run_id"] == FAILED_RUN_ID,
                }
        output.extend(audit_by_identity.values())

        population_files = []
        if slate == "2026-07-29":
            population_files.append((
                "FINAL_PREGAME_POPULATION",
                EXPORT_ROOT / slate / f"bol_tb15_final_pregame_actionable_{slate}.csv",
            ))
        if slate == "2026-07-30":
            population_files.append((
                "FROZEN_UNDER_HYPOTHESES",
                EXPORT_ROOT / slate / "under_hypotheses/baseline_all_under.csv",
            ))
        for artifact, path in population_files:
            if not path.exists():
                continue
            for row in csv.DictReader(path.open()):
                key = (row["governing_run_tag"], row["game_pk"], row["player_mlb_id"])
                snapshot_result = audit_by_identity.get(key)
                output.append({
                    "slate_date": slate, "artifact": artifact,
                    "governing_run_tag": row["governing_run_tag"],
                    "game_pk": row["game_pk"], "player_mlb_id": row["player_mlb_id"],
                    "market_timestamp_utc": row["market_timestamp_utc"],
                    "classification": (
                        snapshot_result["classification"] if snapshot_result
                        else "LINEUP_NOT_RUN_VISIBLE"
                    ),
                    "lineup_observed_at_utc": (
                        snapshot_result["lineup_observed_at_utc"] if snapshot_result else ""
                    ),
                    "lineup_ingestion_run_id": (
                        snapshot_result["lineup_ingestion_run_id"] if snapshot_result else ""
                    ),
                    "failed_ingestion_referenced": (
                        snapshot_result["failed_ingestion_referenced"] if snapshot_result else False
                    ),
                })
    return output


def read_path_inventory() -> list[dict]:
    return [
        {"use": "provisional market board", "file_or_object": "admit_exact_roster_bridge.py", "function_or_query": "eligible_lineups lateral query", "lineup_source": "valid_pregame_lineup_observations", "identity_predicate": "game_pk + player_mlb_id", "run_scope": "same ingestion or completed strict-prior", "observation_predicate": "lineup <= market", "first_pitch_predicate": "enforced by view", "ordering": "observation DESC, ingestion completion DESC, SHA DESC", "post_first_pitch_reachable": "NO"},
        {"use": "capture market sides", "file_or_object": "materialize_capture_snapshot.py", "function_or_query": "sides lateral query", "lineup_source": "valid_pregame_lineup_observations", "identity_predicate": "game_pk + player_mlb_id", "run_scope": "same ingestion or completed strict-prior", "observation_predicate": "lineup <= market", "first_pitch_predicate": "enforced by view", "ordering": "observation DESC, ingestion completion DESC, SHA DESC", "post_first_pitch_reachable": "NO"},
        {"use": "capture lineup snapshot", "file_or_object": "materialize_capture_snapshot.py", "function_or_query": "governing_game query", "lineup_source": "valid_pregame_lineup_observations", "identity_predicate": "exact game/player", "run_scope": "same ingestion or completed strict-prior", "observation_predicate": "lineup <= game market capture", "first_pitch_predicate": "enforced by view", "ordering": "observation DESC, ingestion completion DESC, SHA DESC", "post_first_pitch_reachable": "NO"},
        {"use": "final-pregame freeze", "file_or_object": "closeout_cleanroom_bol_tb15.py", "function_or_query": "audit_and_freeze", "lineup_source": "immutable two-sided market snapshot", "identity_predicate": "game_pk + player_mlb_id", "run_scope": "governing immutable run", "observation_predicate": "certified during materialization", "first_pitch_predicate": "capture must predate pitch", "ordering": "latest market before pitch", "post_first_pitch_reachable": "NO_AFTER_HARDENING"},
        {"use": "H1 top-order freeze", "file_or_object": "manage_cleanroom_bol_tb15_under_hypotheses.py", "function_or_query": "build_final_population", "lineup_source": "immutable two-sided market snapshot", "identity_predicate": "game_pk + player_mlb_id", "run_scope": "governing immutable run", "observation_predicate": "LINEUP_VALID_PREGAME required", "first_pitch_predicate": "certified during materialization", "ordering": "latest market before pitch", "post_first_pitch_reachable": "NO_AFTER_HARDENING"},
        {"use": "status and closeout", "file_or_object": "closeout_cleanroom_bol_tb15.py / toporder manager", "function_or_query": "status / confirmed_starters_before_pitch", "lineup_source": "immutable lineup_snapshot.csv", "identity_predicate": "game_pk + player_mlb_id", "run_scope": "immutable capture run", "observation_predicate": "pre-certified", "first_pitch_predicate": "pre-certified", "ordering": "latest eligible capture", "post_first_pitch_reachable": "NO_AFTER_HARDENING"},
        {"use": "historical research", "file_or_object": "audit_cleanroom_bol_tb15_under_fail_fast.py", "function_or_query": "build_canonical", "lineup_source": "frozen population", "identity_predicate": "game_pk + player_mlb_id", "run_scope": "frozen governing tag", "observation_predicate": "historical artifact lineage audited separately", "first_pitch_predicate": "historical artifact lineage audited separately", "ordering": "frozen", "post_first_pitch_reachable": "HISTORICAL_DEFECT_VISIBLE_NOT_REWRITTEN"},
    ]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date")
    parser.add_argument("--ingestion-run-id")
    parser.add_argument("--output-dir", type=Path, default=OUTPUT)
    args = parser.parse_args()
    rows = source_rows(args.ingestion_run_id, args.date)
    frozen = frozen_artifact_audit()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    if args.ingestion_run_id == FAILED_RUN_ID:
        write_csv(args.output_dir / "affected_lineup_temporal_classification.csv", rows)
    write_csv(args.output_dir / "lineup_read_path_inventory.csv", read_path_inventory())
    write_csv(args.output_dir / "prior_frozen_population_temporal_audit.csv", frozen)
    view_audit = temporal_view_audit(args.date)
    write_csv(args.output_dir / "lineup_temporal_view_audit.csv", view_audit)
    contract = {
        "only_eligible_classification": "LINEUP_VALID_PREGAME",
        "requirements": ["exact game_pk", "exact player_mlb_id", "official confirmed order", "observed before scheduled first pitch", "observed no later than governing market", "same-run or completed strict-prior ingestion visibility", "preserved source payload SHA-256"],
        "fail_closed_classifications": ["LINEUP_POST_FIRST_PITCH", "LINEUP_AFTER_GOVERNING_CAPTURE", "LINEUP_NOT_RUN_VISIBLE", "LINEUP_TIME_MISSING", "LINEUP_SCHEDULE_TIME_MISSING", "LINEUP_IDENTITY_UNRESOLVED"],
    }
    (args.output_dir / "lineup_temporal_contract.json").write_text(json.dumps(contract, indent=2) + "\n")
    class_counts = Counter(row["temporal_classification"] for row in rows)
    minutes = sorted(float(row["minutes_after_first_pitch"]) for row in rows if row["minutes_after_first_pitch"] is not None)
    defects = [row for row in frozen if row.get("classification") not in {"LINEUP_VALID_PREGAME", "ORDER_NOT_CONFIRMED"}]
    selected_frozen = [
        row for row in frozen
        if row.get("artifact") in {"FINAL_PREGAME_POPULATION", "FROZEN_UNDER_HYPOTHESES"}
    ]
    frozen_defects = [
        row for row in selected_frozen
        if row.get("classification") not in {"LINEUP_VALID_PREGAME", "ORDER_NOT_CONFIRMED"}
    ]
    scoped_frozen = [row for row in frozen if not args.date or row.get("slate_date") == args.date]
    scoped_counts = Counter(row.get("classification") for row in scoped_frozen)
    official_games = set()
    valid_games = set()
    if args.date:
        runs, failures = load_runs(args.date)
        if runs and not failures:
            games, _ = latest_schedule(args.date, runs)
            official_games = {str(game["gamePk"]) for game in games}
        valid_games = {
            str(row["game_pk"]) for row in rows
            if row["temporal_classification"] == "VALID_PREGAME_OBSERVATION"
        }
    summary = {
        "total_lineup_observations": len(rows),
        "valid_pregame_observations": class_counts["VALID_PREGAME_OBSERVATION"],
        "post_first_pitch_observations": class_counts["POST_FIRST_PITCH_OBSERVATION"],
        "rows_by_slate_date": dict(Counter(str(row["source_derived_slate_date"]) for row in rows)),
        "rows_by_temporal_classification": dict(class_counts),
        "minimum_minutes_after_first_pitch": min(minutes) if minutes else None,
        "median_minutes_after_first_pitch": statistics.median(minutes) if minutes else None,
        "maximum_minutes_after_first_pitch": max(minutes) if minutes else None,
        "failed_ingestion_references": sum(row.get("failed_ingestion_referenced") in {True, "True"} for row in frozen),
        "temporal_defects_in_prior_artifacts": len(defects),
        "temporal_defects_in_frozen_populations": len(frozen_defects),
        "after_governing_capture_observations": scoped_counts["LINEUP_AFTER_GOVERNING_CAPTURE"],
        "run_invisible_observations": scoped_counts["LINEUP_NOT_RUN_VISIBLE"],
        "missing_time_observations": scoped_counts["LINEUP_TIME_MISSING"] + scoped_counts["LINEUP_SCHEDULE_TIME_MISSING"],
        "games_with_valid_confirmed_lineups": len(valid_games),
        "games_without_valid_confirmed_lineups": len(official_games - valid_games),
        "ingestion_runs_referenced_by_frozen_populations": sorted({
            row.get("lineup_ingestion_run_id", "") for row in selected_frozen
            if row.get("lineup_ingestion_run_id")
        }),
    }
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
