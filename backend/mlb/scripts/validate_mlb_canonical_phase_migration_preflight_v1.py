#!/usr/bin/env python3
"""Validate the canonical-phase migration activation preflight offline."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

from backend.mlb.scripts.activate_mlb_canonical_phase_v1 import validate_inputs
from backend.mlb.scripts.run_mlb_canonical_phase_migration_preflight_tests_v1 import (
    execute as execute_preflight_tests,
)
from backend.mlb.scripts.run_mlb_canonical_phase_stdlib_tests_v1 import (
    execute_target as execute_phase_tests,
)
from backend.mlb.scripts.validate_mlb_canonical_game_phase_activation_v1 import (
    validate as validate_phase_contract,
)
from backend.mlb.scripts.validate_mlb_canonical_phase_source_completion_v1 import (
    validate as validate_source_completion,
)


CONTRACT_NAME = "MLB_2026_CANONICAL_PHASE_MIGRATION_ACTIVATION_PREFLIGHT_V1"
ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PACKAGE = ROOT / "docs/contracts/mlb_2026_canonical_phase_migration_activation_preflight_v1"
SOURCE_PACKAGE = ROOT / "docs/contracts/mlb_2026_canonical_phase_source_completion_v1"
EXPECTED_RAW = ROOT / "backend/mlb/data/external/statsapi/raw/2026/schedule_2026-02-20_2026-03-25.json"
EXPECTED_RAW_SHA = "3ca8bff9e4361676ad3b34c5166f23f4d180d809083557ad8498062a9febd091"
EXPECTED_PROPOSAL_SHA = "b4f04273225643f691d438b492af8c36a40f2b63f62c34f60442261abc850879"
EXPECTED_TARGET_SHA = "5fe30ccd585f3ccb9781999afafab6d42d793aee336998421e395cb5e9c5fb7d"


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_json(path: Path) -> Any:
    return json.loads(path.read_text())


def manifest_failures(package: Path) -> list[str]:
    failures: list[str] = []
    for line in (package / "sha256_manifest.txt").read_text().splitlines():
        expected, relative = line.split("  ", 1)
        path = (package / relative).resolve()
        if not path.is_file() or sha256(path) != expected:
            failures.append(relative)
    return failures


def validate(package: Path) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []

    def check(name: str, passed: bool, detail: Any) -> None:
        checks.append({"check": name, "status": "PASS" if passed else "FAIL", "detail": detail})

    raw = EXPECTED_RAW.read_bytes()
    check(
        "ignored_authoritative_response_identity",
        len(raw) == 642447 and hashlib.sha256(raw).hexdigest() == EXPECTED_RAW_SHA,
        {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()},
    )

    transition = read_json(package / "population_transition.json")
    check(
        "population_transition_exact",
        all(
            transition[key] == expected
            for key, expected in {
                "prior_union_count": 2901,
                "prior_missing_count": 471,
                "response_distinct_game_pk_count": 490,
                "resolved_prior_missing_count": 471,
                "returned_outside_prior_missing_count": 19,
                "net_new_canonical_count": 18,
                "previously_classified_overlap_count": 1,
                "final_population_count": 2919,
                "source_conflict_count": 0,
                "duplicate_identity_conflict_count": 0,
                "missing_count": 0,
                "unknown_count": 0,
            }.items()
        )
        and transition["phase_inference_from_dates"] is False,
        transition,
    )

    snapshot = read_json(package / "operational_database_read_only_snapshot.json")
    snapshot_text = (package / "operational_database_read_only_snapshot.json").read_text()
    check(
        "operational_snapshot_read_only_secret_free",
        snapshot["engine"]["transaction_read_only"] == "on"
        and snapshot["engine"]["transaction_isolation"] == "repeatable read"
        and snapshot["database_writes"] == 0
        and snapshot["blocking_locks_acquired"] == 0
        and snapshot["credentials_recorded"] is False
        and "postgresql://" not in snapshot_text
        and "password" not in snapshot_text.casefold(),
        {"engine": snapshot["engine"], "database_writes": snapshot["database_writes"]},
    )
    check(
        "operational_target_exact",
        snapshot["target"]
        == {
            "credentials_recorded": False,
            "database": "postgres",
            "engine": "PostgreSQL",
            "host": "aws-0-us-west-1.pooler.supabase.com",
            "port": 5432,
            "target_identity_sha256": EXPECTED_TARGET_SHA,
        }
        and snapshot["engine"]["server_version_num"] == "150008",
        snapshot["target"],
    )
    tables = {f"{row['schema']}.{row['table']}": row for row in snapshot["tables"]}
    check(
        "operational_schema_and_counts",
        tables["mlb.game_info"]["row_count"] == 10337
        and tables["mlb.game_info"]["distinct_game_pk_count"] == 10337
        and tables["mlb.game_info"]["duplicate_game_pk_group_count"] == 0
        and tables["mlb_cleanroom_v1.games"]["row_count"] == 590
        and tables["mlb_cleanroom_v1.games"]["distinct_game_pk_count"] == 86
        and tables["mlb_cleanroom_v1.games"]["duplicate_game_pk_group_count"] == 85
        and tables["mlb_cleanroom_v1.games"]["duplicate_game_pk_extra_row_count"] == 504
        and all(not row["phase_coverage"]["phase_columns_present"] for row in tables.values()),
        {
            name: {
                key: row[key]
                for key in (
                    "row_count",
                    "distinct_game_pk_count",
                    "duplicate_game_pk_group_count",
                    "duplicate_game_pk_extra_row_count",
                )
            }
            for name, row in tables.items()
        },
    )
    check(
        "migration_not_applied",
        snapshot["migration_state"]["canonical_view"] is None
        and not snapshot["migration_state"]["matching_migration_versions"],
        snapshot["migration_state"],
    )
    check(
        "operational_locks_nonblocking",
        not snapshot["relation_locks"] and not snapshot["advisory_locks"],
        {
            "relation_locks": snapshot["relation_locks"],
            "advisory_locks": snapshot["advisory_locks"],
        },
    )
    trigger = tables["mlb_cleanroom_v1.games"]["triggers"]
    check(
        "cleanroom_immutability_trigger_recorded",
        len(trigger) == 1
        and trigger[0]["trigger_name"] == "reject_mutation"
        and trigger[0]["enabled"] == "O"
        and "source tables are append-only" in trigger[0]["function_definition"],
        trigger,
    )

    operational = read_json(package / "operational_summary.json")
    check(
        "proposed_mutation_counts_exact",
        operational["proposed_mutations"]["mlb.game_info"]
        == {"updates": 2809, "inserts": 0, "deletes": 0, "unchanged_nonproposal_rows": 7528}
        and operational["proposed_mutations"]["mlb_cleanroom_v1.games"]
        == {"updates": 590, "distinct_game_pks_updated": 86, "inserts": 0, "deletes": 0}
        and operational["post_activation_expected_canonical_view_distinct_game_pks"] == 2809,
        operational["proposed_mutations"],
    )

    proposal = SOURCE_PACKAGE / "canonical_backfill_proposal/canonical_game_phase_backfill_proposal.jsonl"
    source_manifest = SOURCE_PACKAGE / "canonical_backfill_proposal/retained_source_manifest.jsonl"
    rows, input_evidence = validate_inputs(proposal, source_manifest, EXPECTED_PROPOSAL_SHA)
    check(
        "activation_loader_inputs_exact_and_source_hashed",
        len(rows) == 2919
        and input_evidence["source_file_count"] == 464
        and input_evidence["missing_count"] == 0
        and input_evidence["unknown_count"] == 0
        and input_evidence["conflicting_count"] == 0
        and input_evidence["duplicate_identity_count"] == 0,
        input_evidence,
    )

    migration_review = read_json(package / "migration_review.json")
    required_true = (
        "transaction_neutral_body",
        "caller_owned_atomic_transaction_required",
        "idempotent_columns",
        "idempotent_indexes",
        "idempotent_view",
        "named_column_writer_compatibility",
        "no_default_to_r",
        "no_calendar_derived_phase",
        "existing_rows_allowed_unclassified_until_backfill",
        "future_rows_contract_constrained",
        "rollback_transaction_neutral",
        "rollback_drops_only_v1_objects",
    )
    check(
        "migration_static_hardening",
        all(migration_review[key] is True for key in required_true)
        and migration_review["preserved_fields"]
        == {
            "schedule_relationships": 2,
            "source_game_type": 2,
            "source_round": 2,
            "source_season": 2,
        },
        {key: migration_review[key] for key in required_true},
    )

    rehearsal = read_json(package / "isolated_rehearsal_report.json")
    check(
        "isolated_rehearsal_blocker_exact",
        rehearsal["status"] == "BLOCKED"
        and rehearsal["blocker"] == "LOCAL_POSTGRES_SERVER_BINARY_ABSENT"
        and rehearsal["production_database_ddl_dml"] == 0
        and len(rehearsal["required_rehearsal_scenarios_unexecuted"]) == 10,
        rehearsal,
    )
    check(
        "runtime_claims_fail_closed",
        migration_review["runtime_rehearsal_status"] == "BLOCKED"
        and migration_review["runtime_idempotence_proven"] is False
        and migration_review["runtime_rollback_proven"] is False
        and migration_review["runtime_concurrent_writer_proven"] is False,
        {
            key: migration_review[key]
            for key in (
                "runtime_rehearsal_status",
                "runtime_idempotence_proven",
                "runtime_rollback_proven",
                "runtime_concurrent_writer_proven",
            )
        },
    )

    stored_tests = read_json(package / "executed_dependency_free_test_report.json")
    executed_tests = execute_preflight_tests()
    check(
        "preflight_assertions_reexecuted",
        stored_tests == executed_tests
        and executed_tests["passed"] == 15
        and executed_tests["failed"] == 0
        and executed_tests["skipped"] == 0
        and executed_tests["unexecuted"] == 0,
        {key: executed_tests[key] for key in ("passed", "failed", "skipped", "unexecuted")},
    )
    phase_tests = execute_phase_tests()
    check(
        "canonical_phase_assertions_still_pass",
        phase_tests["passed"] == 25
        and phase_tests["failed"] == 0
        and phase_tests["skipped"] == 0
        and phase_tests["unexecuted"] == 0
        and phase_tests["gate_status"] == "PASS",
        {key: phase_tests[key] for key in ("passed", "failed", "skipped", "unexecuted")},
    )
    phase_contract = validate_phase_contract()
    check(
        "existing_phase_validator",
        phase_contract["status"] == "PASS" and phase_contract["check_count"] == 13,
        {"status": phase_contract["status"], "check_count": phase_contract["check_count"]},
    )
    source_completion = validate_source_completion(SOURCE_PACKAGE)
    check(
        "source_completion_validator",
        source_completion["status"] == "PASS"
        and source_completion["check_count"] == 14
        and not source_completion["failed_checks"],
        {
            "status": source_completion["status"],
            "check_count": source_completion["check_count"],
            "failed_checks": source_completion["failed_checks"],
        },
    )

    runbook = (package / "RUNBOOK.md").read_text()
    check(
        "activation_and_rollback_commands_prepared",
        all(
            token in runbook
            for token in (
                "pg_dump",
                "activate_mlb_canonical_phase_v1",
                "--expected-game-info-changes 2809",
                "--expected-cleanroom-changes 590",
                "rollback_mlb_canonical_phase_v1",
                "EXECUTE_MLB_2026_CANONICAL_PHASE_ROLLBACK_V1",
            )
        ),
        "backup, activation, validation, smoke, decision, and rollback commands present",
    )
    plan = read_json(package / "activation_plan.json")
    check(
        "activation_gate_blocked_exactly",
        plan["status"] == "BLOCKED"
        and plan["recommendation"] == "CANONICAL_PHASE_MIGRATION_ACTIVATION_BLOCKED"
        and len(plan["blockers"]) == 2
        and all(value == 0 for value in plan["abort_thresholds"].values()),
        {"status": plan["status"], "blockers": plan["blockers"], "thresholds": plan["abort_thresholds"]},
    )

    failures = manifest_failures(package)
    check("sha256_manifest", not failures, failures)
    failed_checks = [row["check"] for row in checks if row["status"] != "PASS"]
    return {
        "contract_name": CONTRACT_NAME,
        "status": "PASS" if not failed_checks else "FAIL",
        "activation_gate": "BLOCKED",
        "check_count": len(checks),
        "passed_check_count": len(checks) - len(failed_checks),
        "failed_checks": failed_checks,
        "operational_database_writes": 0,
        "provider_requests": 0,
        "paid_credits": 0,
        "checks": checks,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--package", type=Path, default=DEFAULT_PACKAGE)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = validate(args.package.resolve())
    rendered = json.dumps(report, sort_keys=True, indent=2, default=str) + "\n"
    if args.output:
        args.output.write_text(rendered)
    print(rendered, end="")
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
