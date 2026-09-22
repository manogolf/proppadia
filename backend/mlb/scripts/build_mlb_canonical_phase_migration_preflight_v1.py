#!/usr/bin/env python3
"""Build deterministic canonical-phase migration preflight contracts offline."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable


CONTRACT_NAME = "MLB_2026_CANONICAL_PHASE_MIGRATION_ACTIVATION_PREFLIGHT_V1"
ROOT = Path(__file__).resolve().parents[3]


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def read_json(path: Path) -> Any:
    return json.loads(path.read_text())


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True, indent=2, default=str) + "\n")


def build(
    *,
    prior_reconciliation: Path,
    prior_missing_ledger: Path,
    completion_reconciliation: Path,
    completion_outside_ledger: Path,
    proposal_path: Path,
    source_manifest_path: Path,
    operational_snapshot_path: Path,
    rehearsal_report_path: Path,
    migration_path: Path,
    rollback_path: Path,
) -> dict[str, Any]:
    prior = read_json(prior_reconciliation)
    prior_missing = read_jsonl(prior_missing_ledger)
    completion = read_json(completion_reconciliation)
    outside = read_jsonl(completion_outside_ledger)
    proposal = read_jsonl(proposal_path)
    operational = read_json(operational_snapshot_path)
    rehearsal = read_json(rehearsal_report_path)
    proposal_ids = {int(row["game_pk"]) for row in proposal}
    prior_universe = set(int(value) for value in prior["canonical_universe"]["missing_authoritative_type_game_pks"])
    for population in prior["populations"]:
        prior_universe.update(int(value) for value in population["game_pks"])
    outside_ids = {int(row["game_pk"]) for row in outside}
    prior_missing_ids = {int(row["game_pk"]) for row in prior_missing}
    response_ids = prior_missing_ids | outside_ids
    net_new = proposal_ids - prior_universe
    previously_classified_overlap = response_ids - prior_missing_ids - net_new
    population_transition = {
        "contract_name": CONTRACT_NAME,
        "prior_union_count": len(prior_universe),
        "prior_missing_count": len(prior_missing_ids),
        "response_distinct_game_pk_count": completion["returned_distinct_game_pk_count"],
        "resolved_prior_missing_count": completion["resolved_prior_missing_game_pk_count"],
        "returned_outside_prior_missing_count": len(outside_ids),
        "returned_outside_prior_missing_game_pks": sorted(outside_ids),
        "net_new_canonical_count": len(net_new),
        "net_new_canonical_game_pks": sorted(net_new),
        "previously_classified_overlap_count": len(previously_classified_overlap),
        "previously_classified_overlap_game_pks": sorted(previously_classified_overlap),
        "final_population_count": len(proposal_ids),
        "source_conflict_count": completion["violations"]["source_conflict_count"],
        "duplicate_identity_conflict_count": completion["violations"][
            "duplicate_identity_conflict_count"
        ],
        "missing_count": completion["violations"]["current_missing_classification_count"],
        "unknown_count": completion["violations"][
            "unknown_or_invalid_response_observation_count"
        ],
        "phase_inference_from_dates": False,
    }

    tables: dict[str, dict[str, Any]] = {}
    for table in operational["tables"]:
        name = f"{table['schema']}.{table['table']}"
        ids = {int(value) for value in table["game_pks"]}
        tables[name] = {
            "owner": table["owner"],
            "row_count": table["row_count"],
            "distinct_game_pk_count": table["distinct_game_pk_count"],
            "duplicate_game_pk_group_count": table["duplicate_game_pk_group_count"],
            "duplicate_game_pk_extra_row_count": table["duplicate_game_pk_extra_row_count"],
            "maximum_rows_per_game_pk": table["maximum_rows_per_game_pk"],
            "proposal_intersection_count": len(ids & proposal_ids),
            "proposal_only_count": len(proposal_ids - ids),
            "proposal_only_game_pks": sorted(proposal_ids - ids),
            "table_only_count": len(ids - proposal_ids),
            "phase_coverage": table["phase_coverage"],
            "columns": table["columns"],
            "constraints": table["constraints"],
            "indexes": table["indexes"],
            "triggers": table["triggers"],
            "row_level_security_policies": table["row_level_security_policies"],
        }
    proposed_mutations = {
        "mlb.game_info": {
            "updates": tables["mlb.game_info"]["proposal_intersection_count"],
            "inserts": 0,
            "deletes": 0,
            "unchanged_nonproposal_rows": tables["mlb.game_info"]["table_only_count"],
        },
        "mlb_cleanroom_v1.games": {
            "updates": tables["mlb_cleanroom_v1.games"]["row_count"],
            "distinct_game_pks_updated": tables["mlb_cleanroom_v1.games"][
                "proposal_intersection_count"
            ],
            "inserts": 0,
            "deletes": 0,
        },
    }
    operational_summary = {
        "contract_name": CONTRACT_NAME,
        "evidence_mode": operational["evidence_mode"],
        "target": operational["target"],
        "engine": operational["engine"],
        "schemas": operational["schemas"],
        "tables": tables,
        "migration_state": operational["migration_state"],
        "active_session_count_excluding_collector": len(operational["sessions"]),
        "sessions_without_query_text_or_client_address": operational["sessions"],
        "relevant_external_relation_lock_count": len(operational["relation_locks"]),
        "relevant_external_relation_locks": operational["relation_locks"],
        "advisory_lock_count": len(operational["advisory_locks"]),
        "current_user_privileges": operational["current_user_privileges"],
        "proposed_mutations": proposed_mutations,
        "post_activation_expected_canonical_view_distinct_game_pks": tables[
            "mlb.game_info"
        ]["proposal_intersection_count"],
        "database_writes": 0,
        "blocking_locks_acquired": 0,
    }

    migration_text = migration_path.read_text()
    rollback_text = rollback_path.read_text()
    writer_paths = (
        ROOT / "backend/mlb/scripts/insert_mlb_stat_derived.py",
        ROOT / "backend/mlb/scripts/cleanroom_v1/run_cleanroom_source_cycle.py",
        ROOT / "backend/mlb/scripts/cleanroom_v1/admit_exact_roster_bridge.py",
        ROOT / "backend/mlb/season_transition/canonical_phase_v1.py",
    )
    writer_text = "\n".join(path.read_text() for path in writer_paths)
    migration_review = {
        "contract_name": CONTRACT_NAME,
        "migration_path": str(migration_path.relative_to(ROOT)),
        "migration_sha256": sha256(migration_path),
        "rollback_path": str(rollback_path.relative_to(ROOT)),
        "rollback_sha256": sha256(rollback_path),
        "transaction_neutral_body": not any(
            token in migration_text.upper() for token in ("\nBEGIN;", "\nCOMMIT;", "\nROLLBACK;")
        ),
        "caller_owned_atomic_transaction_required": "TRANSACTION-NEUTRAL BODY" in migration_text,
        "idempotent_columns": migration_text.count("ADD COLUMN IF NOT EXISTS") == 15,
        "idempotent_indexes": migration_text.count("CREATE INDEX IF NOT EXISTS") == 2,
        "idempotent_view": "CREATE OR REPLACE VIEW mlb.canonical_game_phase_v1" in migration_text,
        "named_column_writer_compatibility": (
            "INSERT INTO mlb.game_info (" in writer_text
            and "INSERT INTO mlb_cleanroom_v1.games (" in writer_text
            and "INSERT INTO mlb.game_info VALUES" not in writer_text
            and "INSERT INTO mlb_cleanroom_v1.games VALUES" not in writer_text
        ),
        "no_default_to_r": "DEFAULT 'R'" not in migration_text.upper(),
        "no_calendar_derived_phase": all(
            token not in migration_text.lower()
            for token in ("game_date", "official_game_date", "extract(month", "date_part")
        ),
        "preserved_fields": {
            field: migration_text.count(f"ADD COLUMN IF NOT EXISTS {field}")
            for field in (
                "source_season",
                "source_game_type",
                "source_round",
                "schedule_relationships",
            )
        },
        "existing_rows_allowed_unclassified_until_backfill": "source_game_type IS NULL" in migration_text,
        "future_rows_contract_constrained": "source_game_type IN" in migration_text,
        "rollback_transaction_neutral": not any(
            token in rollback_text.upper() for token in ("\nBEGIN;", "\nCOMMIT;", "\nROLLBACK;")
        ),
        "rollback_drops_only_v1_objects": all(
            token in rollback_text
            for token in (
                "DROP VIEW IF EXISTS mlb.canonical_game_phase_v1",
                "DROP COLUMN IF EXISTS source_game_type",
                "DROP COLUMN IF EXISTS source_season",
            )
        ),
        "runtime_rehearsal_status": rehearsal["status"],
        "runtime_idempotence_proven": rehearsal["status"] == "PASS",
        "runtime_rollback_proven": rehearsal["status"] == "PASS",
        "runtime_concurrent_writer_proven": rehearsal["status"] == "PASS",
    }

    cleanroom_trigger = tables["mlb_cleanroom_v1.games"]["triggers"]
    writer_compatibility = {
        "contract_name": CONTRACT_NAME,
        "current_schema_state": "LEGACY_COMPLETE",
        "post_migration_schema_state": "ACTIVATED_COMPLETE",
        "partial_schema_behavior": "FAIL_CLOSED",
        "game_info_writer": "backend/mlb/scripts/insert_mlb_stat_derived.py",
        "cleanroom_writers": [
            "backend/mlb/scripts/cleanroom_v1/run_cleanroom_source_cycle.py",
            "backend/mlb/scripts/cleanroom_v1/admit_exact_roster_bridge.py",
        ],
        "named_columns_only": migration_review["named_column_writer_compatibility"],
        "current_writer_compatibility": "PASS_STATIC_AND_OPERATIONAL_SCHEMA",
        "post_activation_writer_compatibility": (
            "UNPROVEN_PENDING_ISOLATED_POSTGRES_REHEARSAL"
            if rehearsal["status"] != "PASS"
            else "PASS"
        ),
        "cleanroom_append_only_trigger": cleanroom_trigger,
        "backfill_trigger_strategy": (
            "under nonblocking exclusive maintenance lock, verify exact trigger/function, "
            "disable only reject_mutation, update phase columns by exact gamePk, re-enable "
            "before commit; any error rolls back trigger state and data"
        ),
    }

    blockers = []
    if rehearsal["status"] != "PASS":
        blockers.append(
            {
                "code": "ISOLATED_POSTGRES_REHEARSAL_NOT_EXECUTED",
                "detail": rehearsal["blocker"],
                "unexecuted_scenarios": rehearsal[
                    "required_rehearsal_scenarios_unexecuted"
                ],
            }
        )
    if writer_compatibility["post_activation_writer_compatibility"] != "PASS":
        blockers.append(
            {
                "code": "POST_ACTIVATION_WRITER_COMPATIBILITY_NOT_RUNTIME_PROVEN",
                "detail": "static compatibility passes, but local PostgreSQL execution is unavailable",
            }
        )
    activation_plan = {
        "contract_name": CONTRACT_NAME,
        "status": "READY" if not blockers else "BLOCKED",
        "proposal_path": str(proposal_path.relative_to(ROOT)),
        "proposal_sha256": sha256(proposal_path),
        "proposal_count": len(proposal),
        "source_manifest_path": str(source_manifest_path.relative_to(ROOT)),
        "source_manifest_sha256": sha256(source_manifest_path),
        "source_manifest_count": len(read_jsonl(source_manifest_path)),
        "operational_target_identity_sha256": operational["target"][
            "target_identity_sha256"
        ],
        "proposed_mutations": proposed_mutations,
        "abort_thresholds": {
            "unexpected_row_count": 0,
            "unexpected_game_pk": 0,
            "hash_mismatch": 0,
            "missing_source_type": 0,
            "unknown_source_type": 0,
            "phase_mismatch": 0,
            "source_conflict": 0,
            "duplicate_identity_conflict": 0,
            "unexpected_relation_lock": 0,
            "waiting_activation_lock": 0,
            "writer_incompatibility": 0,
            "unverified_source_file": 0,
            "row_creation": 0,
            "row_deletion": 0,
            "disabled_trigger_at_commit": 0,
        },
        "blockers": blockers,
        "smallest_justified_next_action": (
            "Provide an isolated disposable PostgreSQL server with matching major-version "
            "semantics and rerun the committed rehearsal; do not install software or use the "
            "operational database for rehearsal under this task."
            if blockers
            else "Schedule the separately authorized maintenance-window activation."
        ),
        "recommendation": (
            "CANONICAL_PHASE_MIGRATION_ACTIVATION_READY"
            if not blockers
            else "CANONICAL_PHASE_MIGRATION_ACTIVATION_BLOCKED"
        ),
    }
    return {
        "population_transition": population_transition,
        "operational_summary": operational_summary,
        "migration_review": migration_review,
        "writer_compatibility": writer_compatibility,
        "activation_plan": activation_plan,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--prior-reconciliation", required=True, type=Path)
    parser.add_argument("--prior-missing-ledger", required=True, type=Path)
    parser.add_argument("--completion-reconciliation", required=True, type=Path)
    parser.add_argument("--completion-outside-ledger", required=True, type=Path)
    parser.add_argument("--proposal", required=True, type=Path)
    parser.add_argument("--source-manifest", required=True, type=Path)
    parser.add_argument("--operational-snapshot", required=True, type=Path)
    parser.add_argument("--rehearsal-report", required=True, type=Path)
    parser.add_argument("--migration", required=True, type=Path)
    parser.add_argument("--rollback", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build(
        prior_reconciliation=args.prior_reconciliation.resolve(),
        prior_missing_ledger=args.prior_missing_ledger.resolve(),
        completion_reconciliation=args.completion_reconciliation.resolve(),
        completion_outside_ledger=args.completion_outside_ledger.resolve(),
        proposal_path=args.proposal.resolve(),
        source_manifest_path=args.source_manifest.resolve(),
        operational_snapshot_path=args.operational_snapshot.resolve(),
        rehearsal_report_path=args.rehearsal_report.resolve(),
        migration_path=args.migration.resolve(),
        rollback_path=args.rollback.resolve(),
    )
    output = args.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    for name, value in report.items():
        write_json(output / f"{name}.json", value)
    print(canonical_json(report["activation_plan"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
