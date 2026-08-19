"""Natural daily lifecycle for the frozen totals C live shadow."""
from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from backend.mlb.scripts.run_mlb_totals_c_shadow_v1 import (
    ARTIFACT_SHA256, C_LEDGER, EXPERIMENT, MODEL_HASH, MODEL_NAME, OUTPUT_ROOT, RAW_LEDGER, START_DATE,
    score_from_raw,
)
from backend.mlb.scripts.run_mlb_totals_prospective_shadow_daily_v1 import GRADE_ONLY, PRIMARY_SCORE, SCORE_MISSING, resolve_mode
from backend.mlb.totals_predictions.c_shadow_v1 import (
    append_outcome, canonical_identity, connect_ledger, counts, outcomes_for_date, payload_hash, predictions_for_date,
)
from backend.mlb.totals_predictions.prospective_shadow_v1 import payload_hash as raw_payload_hash


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def pending_dates(connection: sqlite3.Connection, completed_through: str) -> list[str]:
    return [row[0] for row in connection.execute("""SELECT DISTINCT p.game_date
      FROM totals_c_shadow_predictions p LEFT JOIN totals_c_shadow_outcomes o USING(canonical_identity)
      WHERE p.game_date<=? AND o.canonical_identity IS NULL ORDER BY p.game_date""", (completed_through,)).fetchall()]


def raw_outcomes(game_date: str, raw_ledger_path: Path) -> dict[int, dict[str, Any]]:
    connection = sqlite3.connect(raw_ledger_path)
    rows = connection.execute("""SELECT p.game_id,p.canonical_identity,o.grading_payload_json,o.grading_payload_sha256,o.graded_at_utc
      FROM totals_shadow_predictions p JOIN totals_shadow_outcomes o USING(canonical_identity)
      WHERE p.game_date=? ORDER BY p.game_id""", (game_date,)).fetchall()
    connection.close()
    output = {}
    for game_pk, identity, payload_json, digest, graded in rows:
        payload = json.loads(payload_json)
        if raw_payload_hash(payload) != digest:
            raise RuntimeError(f"RAW_OUTCOME_PAYLOAD_HASH_MISMATCH_{identity}")
        output[int(game_pk)] = {"raw_identity": identity, "payload": payload, "payload_sha256": digest, "raw_graded_at_utc": graded}
    return output


def grade_date(game_date: str, c_ledger_path: Path, raw_ledger_path: Path) -> dict[str, Any]:
    connection = connect_ledger(c_ledger_path)
    predictions = predictions_for_date(connection, game_date)
    hashes_before = {int(row["game_pk"]): payload_hash(row) for row in predictions}
    sources = raw_outcomes(game_date, raw_ledger_path)
    graded_at = now_utc()
    actions, deferred = [], []
    for prediction in predictions:
        game_pk = int(prediction["game_pk"])
        source = sources.get(game_pk)
        if source is None:
            deferred.append({"game_pk": game_pk, "reason": "RAW_CANONICAL_OFFICIAL_FINAL_NOT_YET_ATTACHED"})
            continue
        raw_grade = source["payload"]
        payload = {
            "experiment": EXPERIMENT, "game_date": game_date, "game_pk": game_pk,
            "model_name": MODEL_NAME, "model_hash": MODEL_HASH,
            "prediction_payload_sha256": payload_hash(prediction),
            "official_final_total": int(raw_grade["official_final_total"]),
            "regulation_nine_total": int(raw_grade["regulation_nine_total"]),
            "completion_state": raw_grade.get("official_status", "Final"),
            "official_source_path": raw_grade["official_source_path"],
            "official_source_hash": raw_grade["official_source_hash"],
            "source_raw_outcome_identity": source["raw_identity"],
            "source_raw_grading_payload_sha256": source["payload_sha256"],
            "source_raw_graded_at_utc": source["raw_graded_at_utc"],
        }
        identity = canonical_identity(game_date, game_pk)
        action = append_outcome(connection, identity, payload, graded_at)
        actions.append({"game_pk": game_pk, "ledger_action": action})
    hashes_after = {int(row["game_pk"]): payload_hash(row) for row in predictions_for_date(connection, game_date)}
    if hashes_before != hashes_after:
        raise RuntimeError("C_PREDICTION_LEDGER_MUTATED_DURING_GRADING")
    return {
        "game_date": game_date, "predictions": len(predictions), "official_outcomes": len(outcomes_for_date(connection, game_date)),
        "new_outcome_rows": sum(row["ledger_action"] == "APPENDED_NEW" for row in actions),
        "deferred_rows": len(deferred), "deferred": deferred, "prediction_rows_unchanged": True,
    }


def cluster_counts(connection: sqlite3.Connection) -> dict[str, Any]:
    rows = connection.execute("""SELECT p.game_date,COUNT(DISTINCT p.canonical_identity),COUNT(DISTINCT o.canonical_identity)
      FROM totals_c_shadow_predictions p LEFT JOIN totals_c_shadow_outcomes o USING(canonical_identity)
      GROUP BY p.game_date ORDER BY p.game_date""").fetchall()
    completed = {game_date for game_date, predictions, outcomes in rows if predictions > 0 and predictions == outcomes}
    pending = {game_date for game_date, predictions, outcomes in rows if predictions > 0 and predictions != outcomes}
    latest_watch = {}
    for game_date, classification in connection.execute("""SELECT w.game_date,w.regime_classification
      FROM totals_c_shadow_watch_observations w JOIN (
        SELECT game_date,MAX(observed_at_utc) AS latest FROM totals_c_shadow_watch_observations GROUP BY game_date
      ) x ON x.game_date=w.game_date AND x.latest=w.observed_at_utc"""):
        latest_watch[game_date] = classification
    primary = sorted(day for day in completed if latest_watch.get(day) == "NORMAL_COMPETITIVE_REGIME")
    transition = sorted(day for day in completed if latest_watch.get(day) == "LATE_SEASON_TRANSITION_WATCH")
    late = sorted(day for day in completed if latest_watch.get(day) == "LATE_SEASON_DISTINCT_REGIME")
    pending_primary = sorted(day for day in pending if latest_watch.get(day) == "NORMAL_COMPETITIVE_REGIME")
    pending_transition = sorted(day for day in pending if latest_watch.get(day) == "LATE_SEASON_TRANSITION_WATCH")
    pending_late = sorted(day for day in pending if latest_watch.get(day) == "LATE_SEASON_DISTINCT_REGIME")
    return {
        "completed_date_clusters": len(completed), "completed_primary_regime_clusters": len(primary),
        "completed_transition_watch_clusters": len(transition), "completed_late_season_clusters": len(late),
        "primary_dates": primary, "transition_watch_dates": transition, "late_season_dates": late,
        "pending_date_clusters": len(pending), "pending_primary_regime_clusters": len(pending_primary),
        "pending_transition_watch_clusters": len(pending_transition), "pending_late_season_clusters": len(pending_late),
        "pending_primary_dates": pending_primary, "pending_transition_watch_dates": pending_transition,
        "pending_late_season_dates": pending_late,
        "first_formal_checkpoint": 8, "second_conditional_checkpoint": 12,
        "next_primary_checkpoint": 8 if len(primary) < 8 else (12 if len(primary) < 12 else None),
        "completed_primary_clusters_to_next_checkpoint": (
            max(0, 8 - len(primary)) if len(primary) < 8 else (max(0, 12 - len(primary)) if len(primary) < 12 else 0)
        ),
    }


def run(slate_date: str, completed_through: str, mode: str, wrapper_started_at_utc: str, run_tag: str,
        output_root: Path = OUTPUT_ROOT, c_ledger_path: Path = C_LEDGER, raw_ledger_path: Path = RAW_LEDGER,
        raw_lifecycle_json: Path | None = None) -> dict[str, Any]:
    resolved = resolve_mode(mode, wrapper_started_at_utc)
    if resolved in (PRIMARY_SCORE, SCORE_MISSING):
        current_et = datetime.now(ZoneInfo("America/New_York")).date().isoformat()
        if slate_date != current_et:
            raise RuntimeError(f"C_SHADOW_NONCURRENT_SCORING_BLOCKED requested={slate_date} current_et={current_et}")
    raw_lifecycle = json.loads(raw_lifecycle_json.read_text()) if raw_lifecycle_json else None
    if raw_lifecycle and raw_lifecycle.get("resolved_mode") != resolved:
        raise RuntimeError("C_RAW_LIFECYCLE_MODE_MISMATCH")
    connection = connect_ledger(c_ledger_path)
    before = counts(connection)
    grading = [grade_date(day, c_ledger_path, raw_ledger_path) for day in pending_dates(connection, completed_through)]
    scoring = None
    if resolved in (PRIMARY_SCORE, SCORE_MISSING) and slate_date >= START_DATE:
        raw_attempts = ((raw_lifecycle.get("scoring") or {}).get("attempts") or []) if raw_lifecycle else []
        scoring = score_from_raw(slate_date, resolved, run_tag, raw_ledger_path, c_ledger_path, raw_attempts=raw_attempts)
    after_connection = connect_ledger(c_ledger_path)
    after = counts(after_connection)
    clusters = cluster_counts(after_connection)
    output_root.mkdir(parents=True, exist_ok=True)
    status = "TOTALS_C_SHADOW_DAILY_LIFECYCLE_COMPLETE" if slate_date >= START_DATE else "TOTALS_C_SHADOW_ARMED_AWAITING_START_DATE"
    return {
        "status": status, "slate_date": slate_date, "completed_through": completed_through,
        "requested_mode": mode, "resolved_mode": resolved, "scoring_run_tag": run_tag,
        "wrapper_started_at_utc": wrapper_started_at_utc, "shadow_start_date": START_DATE,
        "grading_dates_attempted": [row["game_date"] for row in grading],
        "new_outcome_rows": sum(row["new_outcome_rows"] for row in grading), "grading": grading, "scoring": scoring,
        "ledger_before": before, "ledger_after": after, "cluster_status": clusters,
        "model_name": MODEL_NAME, "model_hash": MODEL_HASH, "artifact_sha256": ARTIFACT_SHA256,
        "raw_control_unchanged": True, "v1_intercept_policy": "DO_NOT_APPLY_RAW_INTERCEPT_TO_C",
        "public_status": "PRIVATE_SHADOW_ONLY_NOT_PUBLIC", "ev_roi_wager_outputs": 0,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--slate-date", required=True); parser.add_argument("--completed-through", required=True)
    parser.add_argument("--mode", choices=("auto", "grade-only", "score-missing"), default="auto")
    parser.add_argument("--wrapper-started-at-utc", required=True); parser.add_argument("--run-tag", required=True)
    parser.add_argument("--output-root", type=Path, default=OUTPUT_ROOT); parser.add_argument("--c-ledger-path", type=Path, default=C_LEDGER)
    parser.add_argument("--raw-ledger-path", type=Path, default=RAW_LEDGER); parser.add_argument("--raw-lifecycle-json", type=Path)
    parser.add_argument("--output-json", type=Path)
    args = parser.parse_args()
    result = run(args.slate_date, args.completed_through, args.mode, args.wrapper_started_at_utc, args.run_tag,
                 args.output_root, args.c_ledger_path, args.raw_ledger_path, args.raw_lifecycle_json)
    text = json.dumps(result, indent=2, default=str)
    if args.output_json:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(text + "\n")
    print(text)


if __name__ == "__main__":
    main()
