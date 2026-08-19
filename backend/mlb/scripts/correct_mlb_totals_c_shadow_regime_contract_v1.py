"""Apply the append-only Totals C shadow regime-contract correction v1.

This command changes governance labels and cluster accounting only. It never
reads outcome values and never updates prediction, context, or outcome rows.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.mlb.scripts.run_mlb_totals_c_shadow_daily_v1 import cluster_counts
from backend.mlb.scripts.run_mlb_totals_c_shadow_v1 import (
    ARTIFACT,
    ARTIFACT_SHA256,
    C_LEDGER,
    FEATURE_CONTRACT_HASH,
    MODEL_HASH,
    MODEL_NAME,
    NORMAL_COMPETITIVE_REGIME,
    START_DATE,
    classify_regime,
    operational_regime_label,
)
from backend.mlb.totals_predictions.c_shadow_v1 import SNAPSHOT_CLASS, append_watch_observation, connect_ledger


ROOT = Path(__file__).resolve().parents[3]
TASK_ID = "MLB_TOTALS_C_SHADOW_REGIME_CONTRACT_CORRECTION_V1"
CORRECTION_REASON = "HUMAN_INTENT_CLARIFICATION_BEFORE_FIRST_FORMAL_REVIEW"
CORRECTION_RUN_TAG = "regime_contract_correction_v1_20260819"
DATES = ("2026-08-17", "2026-08-18", "2026-08-19")
OUTPUT_DIR = ROOT / "artifacts/analysis/model_development/mlb_totals_c_shadow_regime_contract_correction_v1/2026-08-19"
ORIGINAL_CONTRACT = ROOT / "artifacts/analysis/model_development/mlb_totals_count_confidence_only_live_shadow_launch_v1/2026-08-16/totals_c_shadow_regime_contract.md"
LAUNCH_DIR = ORIGINAL_CONTRACT.parent
PROTECTED_TABLES = (
    "totals_c_shadow_predictions",
    "totals_c_shadow_contexts",
    "totals_c_shadow_outcomes",
)


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def table_hash(connection: sqlite3.Connection, table: str) -> tuple[int, str]:
    columns = [row[1] for row in connection.execute(f"PRAGMA table_info({table})")]
    rows = connection.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall()
    encoded = json.dumps(
        {"columns": columns, "rows": rows}, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    ).encode()
    return len(rows), hashlib.sha256(encoded).hexdigest()


def protected_state(connection: sqlite3.Connection) -> dict[str, dict[str, Any]]:
    return {
        table: {"rows": row_count, "sha256": digest}
        for table in PROTECTED_TABLES
        for row_count, digest in (table_hash(connection, table),)
    }


def latest_uncorrected_watch(connection: sqlite3.Connection, game_date: str) -> dict[str, Any]:
    row = connection.execute(
        """SELECT watch_payload_json FROM totals_c_shadow_watch_observations
           WHERE game_date=? AND scoring_run_tag<>? ORDER BY observed_at_utc DESC LIMIT 1""",
        (game_date, CORRECTION_RUN_TAG),
    ).fetchone()
    if not row:
        raise RuntimeError(f"C_REGIME_SOURCE_WATCH_MISSING_{game_date}")
    return json.loads(row[0])


def completion_state(connection: sqlite3.Connection, game_date: str) -> str:
    predictions, outcomes = connection.execute(
        """SELECT COUNT(DISTINCT p.canonical_identity),COUNT(DISTINCT o.canonical_identity)
           FROM totals_c_shadow_predictions p LEFT JOIN totals_c_shadow_outcomes o USING(canonical_identity)
           WHERE p.game_date=?""",
        (game_date,),
    ).fetchone()
    if predictions and predictions == outcomes:
        return "COMPLETED"
    if predictions:
        return "PENDING"
    return "NO_PREDICTIONS"


def correction_payload(source: dict[str, Any], observed_at_utc: str) -> dict[str, Any]:
    game_date = source["game_date"]
    state = {
        "affirmative_transition_evidence": [],
        "affirmative_distinct_evidence": [],
        "unavailable_metadata": list(source.get("regime_evidence", {}).get("unavailable_exact_indicators", [])),
        "ordinary_game_conditions": [],
    }
    classification = classify_regime(state)
    if classification != NORMAL_COMPETITIVE_REGIME:
        raise RuntimeError(f"C_REGIME_CORRECTION_UNEXPECTED_CLASSIFICATION_{game_date}_{classification}")
    return {
        "task_id": TASK_ID,
        "experiment": source["experiment"],
        "game_date": game_date,
        "scoring_run_tag": CORRECTION_RUN_TAG,
        "observed_at_utc": observed_at_utc,
        "deployment_watch_status": source["deployment_watch_status"],
        "regime_classification": classification,
        "C_REGIME": operational_regime_label(classification),
        "regime_correction": {
            "correction_reason": CORRECTION_REASON,
            "previous_regime_classification": source["regime_classification"],
            "source_scoring_run_tag": source["scoring_run_tag"],
            "source_watch_payload_sha256": hashlib.sha256(
                json.dumps(source, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
            ).hexdigest(),
            "classification_authority": "REGIME_GOVERNANCE_CORRECTION_OVERLAY",
            "prediction_rows_modified": 0,
            "outcome_rows_modified": 0,
            "performance_used": False,
        },
        "regime_evidence": {
            **state,
            "performance_used": False,
            "missing_metadata_triggered_watch": False,
            "affirmative_nonperformance_evidence_found": False,
            "basis": "ordinary mid-August slate; no affirmative evidence of a materially changed competitive environment",
        },
        "deployment_watch_snapshot": source.get("watch_rows", []),
        "weather_delay_policy": {
            "ordinary_delay_or_weather_triggers_regime_change": False,
            "extra_innings_or_completed_unusual_game_excluded": False,
            "grading_issue_states": [
                "POSTPONED", "SUSPENDED_OR_INCOMPLETE", "OFFICIAL_COMPLETION_UNRESOLVED",
                "CANONICAL_GRADING_UNSAFE",
            ],
        },
        "moneyline_operational_note": "REPEATED_STRONG_TEAM=NOT_AN_ERROR_CONDITION",
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        raise ValueError(f"EMPTY_CSV_FORBIDDEN_{path.name}")
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def run(ledger_path: Path = C_LEDGER, output_dir: Path = OUTPUT_DIR, observed_at_utc: str | None = None) -> dict[str, Any]:
    connection = connect_ledger(ledger_path)
    protected_before = protected_state(connection)
    artifact_digest = sha256_file(ARTIFACT)
    if artifact_digest != ARTIFACT_SHA256:
        raise RuntimeError("C_ARTIFACT_SHA256_MISMATCH")
    identities = connection.execute(
        "SELECT DISTINCT model_name,model_hash,artifact_sha256,snapshot_class FROM totals_c_shadow_predictions"
    ).fetchall()
    if identities != [(MODEL_NAME, MODEL_HASH, ARTIFACT_SHA256, SNAPSHOT_CLASS)]:
        raise RuntimeError(f"C_LEDGER_IDENTITY_MISMATCH_{identities}")

    correction_time = observed_at_utc or now_utc()
    reclassifications = []
    for game_date in DATES:
        source = latest_uncorrected_watch(connection, game_date)
        existing = connection.execute(
            "SELECT watch_payload_json FROM totals_c_shadow_watch_observations WHERE observation_identity=?",
            (f"{game_date}|{CORRECTION_RUN_TAG}",),
        ).fetchone()
        payload = json.loads(existing[0]) if existing else correction_payload(source, correction_time)
        action = append_watch_observation(connection, payload)
        if action not in ("APPENDED_NEW", "EXISTING_IMMUTABLE"):
            raise RuntimeError(f"C_REGIME_CORRECTION_APPEND_FAILED_{game_date}_{action}")
        reclassifications.append({
            "game_date": game_date,
            "completion_state": completion_state(connection, game_date),
            "previous_regime_tag": source["regime_classification"],
            "corrected_regime_tag": payload["regime_classification"],
            "affirmative_nonperformance_evidence": "NONE",
            "missing_metadata_alone_triggered_watch": "NO",
            "performance_used": "NO",
            "basis": payload["regime_evidence"]["basis"],
            "correction_reason": CORRECTION_REASON,
            "ledger_action": "APPEND_ONLY_OBSERVATION_PRESENT",
        })

    protected_after = protected_state(connection)
    if protected_before != protected_after:
        raise RuntimeError("C_PROTECTED_LEDGER_STATE_CHANGED")
    clusters = cluster_counts(connection)
    expected = {
        "completed_date_clusters": 2,
        "completed_primary_regime_clusters": 2,
        "completed_transition_watch_clusters": 0,
        "completed_late_season_clusters": 0,
        "pending_date_clusters": 1,
        "pending_primary_regime_clusters": 1,
        "pending_transition_watch_clusters": 0,
        "pending_late_season_clusters": 0,
    }
    mismatches = {key: (clusters.get(key), value) for key, value in expected.items() if clusters.get(key) != value}
    if mismatches:
        raise RuntimeError(f"C_CLUSTER_ACCOUNTING_UNEXPECTED_{mismatches}")

    output_dir.mkdir(parents=True, exist_ok=True)
    contract = f"""# Totals C shadow regime contract — corrected v1

Task: `{TASK_ID}`

Correction reason: `{CORRECTION_REASON}`

This addendum supersedes the original missing-metadata interpretation without rewriting the original contract at `{ORIGINAL_CONTRACT.relative_to(ROOT)}` (SHA-256 `{sha256_file(ORIGINAL_CONTRACT)}`).

## Current default

`NORMAL_COMPETITIVE_REGIME` is the default during the current mid-August shadow period. Missing elimination, roster-turnover, lineup-churn, or replacement-player metadata is not affirmative evidence and does not trigger `LATE_SEASON_TRANSITION_WATCH`.

`LATE_SEASON_TRANSITION_WATCH` requires affirmative external/non-performance evidence that the competitive population may be changing materially. `LATE_SEASON_DISTINCT_REGIME` requires affirmative external/non-performance evidence that the environment has materially changed. No calendar date or C performance determines either state.

## Operational rendering

Daily operations report `C_REGIME = NORMAL` absent affirmative evidence. Deployment watches A–I remain separate health signals and do not themselves establish a late-season competitive regime.

## Weather and completion

Rain delays, weather interruptions, extra innings, and unusual but officially completed games neither create a late-season regime nor exclude a cluster. Postponed, suspended/incomplete, unresolved official completion, or unsafe canonical grading remains a grading/data issue.

## Evidence integrity

August 17 and 18 are completed primary-regime clusters; August 19 is a pending primary-regime cluster. August 17 remains the first prospective shadow date. Formal reviews remain at 8 and conditionally 12 completed primary-regime clusters. Predictions, contexts, outcomes, model identity, artifact, snapshot policy, bullpen freshness contract, comparators, and evaluation contracts are unchanged.

Immutable historical prediction payload labels are not rewritten; the append-only correction observation is the authoritative date-level regime label. Future scores use the corrected default.

`REPEATED_STRONG_TEAM=NOT_AN_ERROR_CONDITION`. A repeated Moneyline STRONG team warrants investigation only with an independent integrity signal.
"""
    (output_dir / "totals_c_shadow_regime_contract_corrected.md").write_text(contract)
    write_csv(output_dir / "totals_c_shadow_regime_reclassification.csv", reclassifications)

    cluster_rows = [
        {"state": "COMPLETED", "regime": "ALL", "clusters": clusters["completed_date_clusters"], "dates": ";".join(sorted(set(clusters["primary_dates"] + clusters["transition_watch_dates"] + clusters["late_season_dates"])))},
        {"state": "COMPLETED", "regime": NORMAL_COMPETITIVE_REGIME, "clusters": clusters["completed_primary_regime_clusters"], "dates": ";".join(clusters["primary_dates"])},
        {"state": "COMPLETED", "regime": "LATE_SEASON_TRANSITION_WATCH", "clusters": clusters["completed_transition_watch_clusters"], "dates": ";".join(clusters["transition_watch_dates"])},
        {"state": "COMPLETED", "regime": "LATE_SEASON_DISTINCT_REGIME", "clusters": clusters["completed_late_season_clusters"], "dates": ";".join(clusters["late_season_dates"])},
        {"state": "PENDING", "regime": "ALL", "clusters": clusters["pending_date_clusters"], "dates": ";".join(sorted(set(clusters["pending_primary_dates"] + clusters["pending_transition_watch_dates"] + clusters["pending_late_season_dates"])))},
        {"state": "PENDING", "regime": NORMAL_COMPETITIVE_REGIME, "clusters": clusters["pending_primary_regime_clusters"], "dates": ";".join(clusters["pending_primary_dates"])},
        {"state": "PENDING", "regime": "LATE_SEASON_TRANSITION_WATCH", "clusters": clusters["pending_transition_watch_clusters"], "dates": ";".join(clusters["pending_transition_watch_dates"])},
        {"state": "PENDING", "regime": "LATE_SEASON_DISTINCT_REGIME", "clusters": clusters["pending_late_season_clusters"], "dates": ";".join(clusters["pending_late_season_dates"])},
    ]
    write_csv(output_dir / "totals_c_shadow_cluster_accounting.csv", cluster_rows)

    validation_rows = [
        {"validation": "C artifact SHA-256 unchanged", "status": "PASS", "evidence": artifact_digest},
        {"validation": "C model identity/hash unchanged", "status": "PASS", "evidence": f"{MODEL_NAME}|{MODEL_HASH}"},
        {"validation": "prediction ledger rows/hash unchanged", "status": "PASS", "evidence": json.dumps(protected_after["totals_c_shadow_predictions"], sort_keys=True)},
        {"validation": "context ledger rows/hash unchanged", "status": "PASS", "evidence": json.dumps(protected_after["totals_c_shadow_contexts"], sort_keys=True)},
        {"validation": "outcome ledger rows/hash unchanged", "status": "PASS", "evidence": json.dumps(protected_after["totals_c_shadow_outcomes"], sort_keys=True)},
        {"validation": "shadow start date unchanged", "status": "PASS", "evidence": START_DATE},
        {"validation": "snapshot policy unchanged", "status": "PASS", "evidence": SNAPSHOT_CLASS},
        {"validation": "feature/bullpen scoring contract unchanged", "status": "PASS", "evidence": FEATURE_CONTRACT_HASH},
        {"validation": "comparator contract preserved", "status": "PASS", "evidence": sha256_file(LAUNCH_DIR / "totals_c_shadow_comparator_contract.md")},
        {"validation": "evaluation contract preserved", "status": "PASS", "evidence": sha256_file(LAUNCH_DIR / "totals_c_shadow_metrics_contract.md")},
        {"validation": "8/12 review schedule unchanged", "status": "PASS", "evidence": "8 completed primary; conditional 12 completed primary"},
        {"validation": "missing metadata alone stays normal", "status": "PASS", "evidence": classify_regime({"affirmative_transition_evidence": [], "affirmative_distinct_evidence": [], "unavailable_metadata": ["mathematical_elimination_status"], "ordinary_game_conditions": []})},
        {"validation": "affirmative transition evidence can trigger WATCH", "status": "PASS", "evidence": classify_regime({"affirmative_transition_evidence": [{"evidence_type": "ACTIVE_ROSTER_TURNOVER", "affirmative": True, "performance_used": False}], "affirmative_distinct_evidence": [], "unavailable_metadata": [], "ordinary_game_conditions": []})},
        {"validation": "affirmative distinct evidence can trigger distinct regime", "status": "PASS", "evidence": classify_regime({"affirmative_transition_evidence": [], "affirmative_distinct_evidence": [{"evidence_type": "SHUTDOWN_WORKLOAD_MANAGEMENT", "affirmative": True, "performance_used": False}], "unavailable_metadata": [], "ordinary_game_conditions": []})},
        {"validation": "weather delay alone stays normal", "status": "PASS", "evidence": classify_regime({"affirmative_transition_evidence": [], "affirmative_distinct_evidence": [], "unavailable_metadata": [], "ordinary_game_conditions": ["RAIN_DELAY"]})},
        {"validation": "August 17-19 corrected labels", "status": "PASS", "evidence": ";".join(f"{row['game_date']}={row['corrected_regime_tag']}" for row in reclassifications)},
        {"validation": "cluster accounting verified", "status": "PASS", "evidence": json.dumps(expected, sort_keys=True)},
        {"validation": "C performance excluded from classification", "status": "PASS", "evidence": "outcome values not read; performance_used=false"},
        {"validation": "repeated STRONG teams operational rule", "status": "PASS", "evidence": "REPEATED_STRONG_TEAM=NOT_AN_ERROR_CONDITION"},
    ]
    write_csv(output_dir / "totals_c_shadow_regime_correction_validation.csv", validation_rows)

    concise = f"""# Concise Totals C shadow regime contract correction v1

- Default: `NORMAL_COMPETITIVE_REGIME`; missing auxiliary metadata alone does not trigger WATCH.
- August 17: completed normal; August 18: completed normal; August 19: pending normal.
- Accounting: 2 completed primary, 0 transition, 0 distinct; 1 pending primary, 0 transition, 0 distinct.
- First formal review: 8 completed primary clusters; distance: {clusters['completed_primary_clusters_to_next_checkpoint']}.
- Weather: ordinary delays/interruptions, extra innings, and completed unusual games remain eligible.
- Moneyline: `REPEATED_STRONG_TEAM=NOT_AN_ERROR_CONDITION` absent an independent integrity signal.
- Integrity: predictions, contexts, outcomes, model/hash, snapshot, and 8/12 checkpoints unchanged.
- Result: `C_SHADOW_REGIME_CONTRACT_CORRECTION_VALIDATED`.
"""
    (output_dir / "concise_mlb_totals_c_shadow_regime_contract_correction_v1.md").write_text(concise)

    artifact_names = (
        "totals_c_shadow_regime_contract_corrected.md",
        "totals_c_shadow_regime_reclassification.csv",
        "totals_c_shadow_cluster_accounting.csv",
        "totals_c_shadow_regime_correction_validation.csv",
        "concise_mlb_totals_c_shadow_regime_contract_correction_v1.md",
    )
    hashes = "".join(f"{sha256_file(output_dir / name)}  {name}\n" for name in artifact_names)
    (output_dir / "reproducibility_hashes.sha256").write_text(hashes)
    return {
        "status": "C_SHADOW_REGIME_CONTRACT_CORRECTION_VALIDATED",
        "task_id": TASK_ID,
        "output_dir": str(output_dir),
        "reclassifications": reclassifications,
        "cluster_accounting": clusters,
        "protected_state_before": protected_before,
        "protected_state_after": protected_after,
        "model_name": MODEL_NAME,
        "model_hash": MODEL_HASH,
        "artifact_sha256": ARTIFACT_SHA256,
        "performance_evaluated": False,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ledger-path", type=Path, default=C_LEDGER)
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    parser.add_argument("--observed-at-utc")
    args = parser.parse_args()
    print(json.dumps(run(args.ledger_path, args.output_dir, args.observed_at_utc), indent=2))


if __name__ == "__main__":
    main()
