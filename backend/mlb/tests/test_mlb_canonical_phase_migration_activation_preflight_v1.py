from __future__ import annotations

import hashlib
import json
import unittest
from pathlib import Path

from backend.mlb.season_transition.canonical_phase_v1 import cleanroom_game_insert_sql


ROOT = Path(__file__).resolve().parents[3]
PACKAGE = ROOT / "docs/contracts/mlb_2026_canonical_phase_migration_activation_preflight_v1"
RAW = ROOT / "backend/mlb/data/external/statsapi/raw/2026/schedule_2026-02-20_2026-03-25.json"
MIGRATION = ROOT / "backend/mlb/sql/migrations/20260921_prepare_mlb_season_phase_contract_v1.sql"
ROLLBACK = ROOT / "backend/mlb/sql/migrations/20260921_rollback_mlb_season_phase_contract_v1.sql"
LOADER = ROOT / "backend/mlb/scripts/activate_mlb_canonical_phase_v1.py"


class CanonicalPhaseMigrationPreflightTests(unittest.TestCase):
    def test_ignored_authoritative_response_identity(self) -> None:
        raw = RAW.read_bytes()
        self.assertEqual(len(raw), 642447)
        self.assertEqual(
            hashlib.sha256(raw).hexdigest(),
            "3ca8bff9e4361676ad3b34c5166f23f4d180d809083557ad8498062a9febd091",
        )

    def test_population_transition_exact(self) -> None:
        report = json.loads((PACKAGE / "population_transition.json").read_text())
        self.assertEqual(report["prior_union_count"], 2901)
        self.assertEqual(report["prior_missing_count"], 471)
        self.assertEqual(report["response_distinct_game_pk_count"], 490)
        self.assertEqual(report["resolved_prior_missing_count"], 471)
        self.assertEqual(report["returned_outside_prior_missing_count"], 19)
        self.assertEqual(report["net_new_canonical_count"], 18)
        self.assertEqual(report["previously_classified_overlap_count"], 1)
        self.assertEqual(report["final_population_count"], 2919)

    def test_population_has_zero_classification_conflicts(self) -> None:
        report = json.loads((PACKAGE / "population_transition.json").read_text())
        for name in (
            "source_conflict_count",
            "duplicate_identity_conflict_count",
            "missing_count",
            "unknown_count",
        ):
            self.assertEqual(report[name], 0)
        self.assertFalse(report["phase_inference_from_dates"])

    def test_operational_snapshot_is_read_only_and_secret_free(self) -> None:
        report = json.loads((PACKAGE / "operational_database_read_only_snapshot.json").read_text())
        self.assertEqual(report["engine"]["transaction_read_only"], "on")
        self.assertEqual(report["database_writes"], 0)
        self.assertEqual(report["blocking_locks_acquired"], 0)
        self.assertFalse(report["credentials_recorded"])
        text = (PACKAGE / "operational_database_read_only_snapshot.json").read_text()
        self.assertNotIn("postgresql://", text)
        self.assertNotIn("SUPABASE_DB_URL", text)

    def test_operational_migration_is_not_applied(self) -> None:
        report = json.loads((PACKAGE / "operational_database_read_only_snapshot.json").read_text())
        self.assertIsNone(report["migration_state"]["canonical_view"])
        self.assertEqual(report["migration_state"]["matching_migration_versions"], [])
        for table in report["tables"]:
            self.assertEqual(table["phase_coverage"]["phase_columns_present"], [])
            self.assertEqual(table["phase_coverage"]["migration_state"], "NOT_APPLIED")

    def test_operational_counts_and_proposed_mutations_exact(self) -> None:
        report = json.loads((PACKAGE / "operational_summary.json").read_text())
        self.assertEqual(report["tables"]["mlb.game_info"]["row_count"], 10337)
        self.assertEqual(report["tables"]["mlb.game_info"]["distinct_game_pk_count"], 10337)
        self.assertEqual(report["tables"]["mlb.game_info"]["duplicate_game_pk_group_count"], 0)
        self.assertEqual(report["tables"]["mlb_cleanroom_v1.games"]["row_count"], 590)
        self.assertEqual(report["tables"]["mlb_cleanroom_v1.games"]["distinct_game_pk_count"], 86)
        self.assertEqual(report["proposed_mutations"]["mlb.game_info"]["updates"], 2809)
        self.assertEqual(report["proposed_mutations"]["mlb_cleanroom_v1.games"]["updates"], 590)

    def test_migration_is_transaction_neutral_and_idempotent_by_construction(self) -> None:
        migration = MIGRATION.read_text()
        self.assertNotRegex(migration, r"(?im)^\s*(BEGIN|COMMIT|ROLLBACK)\s*;")
        self.assertEqual(migration.count("ADD COLUMN IF NOT EXISTS"), 15)
        self.assertEqual(migration.count("CREATE INDEX IF NOT EXISTS"), 2)
        self.assertIn("CREATE OR REPLACE VIEW mlb.canonical_game_phase_v1", migration)
        self.assertIn("TRANSACTION-NEUTRAL BODY", migration)

    def test_migration_has_no_default_or_calendar_phase_inference(self) -> None:
        migration = MIGRATION.read_text()
        self.assertNotIn("DEFAULT 'R'", migration.upper())
        lower = migration.lower()
        for token in ("game_date", "official_game_date", "extract(month", "date_part"):
            self.assertNotIn(token, lower)
        self.assertIn("source_game_type IN", migration)

    def test_migration_preserves_authoritative_fields(self) -> None:
        migration = MIGRATION.read_text()
        for column in (
            "source_season",
            "source_game_type",
            "source_round",
            "schedule_relationships",
        ):
            self.assertEqual(migration.count(f"ADD COLUMN IF NOT EXISTS {column}"), 2)

    def test_writers_use_named_columns_and_fail_on_partial_schema(self) -> None:
        source = "\n".join(
            (ROOT / path).read_text()
            for path in (
                "backend/mlb/scripts/insert_mlb_stat_derived.py",
                "backend/mlb/scripts/cleanroom_v1/run_cleanroom_source_cycle.py",
                "backend/mlb/scripts/cleanroom_v1/admit_exact_roster_bridge.py",
            )
        )
        self.assertNotIn("INSERT INTO mlb.game_info VALUES", source)
        self.assertNotIn("INSERT INTO mlb_cleanroom_v1.games VALUES", source)
        legacy = {
            "game_pk", "slate_date", "official_game_date", "home_team_mlb_id",
            "away_team_mlb_id", "scheduled_start_utc", "game_status", "source",
            "source_observed_at_utc", "ingested_at_utc", "source_payload_sha256",
        }
        with self.assertRaisesRegex(RuntimeError, "CANONICAL_PHASE_SCHEMA_PARTIAL"):
            cleanroom_game_insert_sql(legacy | {"source_game_type"})

    def test_loader_is_guarded_named_and_exact_game_pk_only(self) -> None:
        loader = LOADER.read_text()
        self.assertIn("ACTIVATION_NOT_AUTHORIZED", loader)
        self.assertIn("LOCK TABLE mlb.game_info, mlb_cleanroom_v1.games", loader)
        self.assertIn("WHERE g.game_id = s.game_pk", loader)
        self.assertIn("WHERE g.game_pk = s.game_pk", loader)
        self.assertNotIn("INSERT INTO mlb.game_info VALUES", loader)
        self.assertNotIn("INSERT INTO mlb_cleanroom_v1.games VALUES", loader)
        self.assertNotIn("game_date = s.", loader)

    def test_loader_handles_append_only_trigger_transactionally(self) -> None:
        loader = LOADER.read_text()
        disable = loader.index("DISABLE TRIGGER reject_mutation")
        enable = loader.index("ENABLE TRIGGER reject_mutation")
        self.assertLess(disable, enable)
        self.assertIn("CLEANROOM_IMMUTABILITY_TRIGGER_NOT_REENABLED", loader)
        self.assertIn("zero_row_creation_or_deletion", loader)

    def test_rollback_is_transaction_neutral_and_guarded(self) -> None:
        rollback = ROLLBACK.read_text()
        self.assertNotRegex(rollback, r"(?im)^\s*(BEGIN|COMMIT|ROLLBACK)\s*;")
        self.assertIn("DROP VIEW IF EXISTS mlb.canonical_game_phase_v1", rollback)
        executor = (
            ROOT / "backend/mlb/scripts/rollback_mlb_canonical_phase_v1.py"
        ).read_text()
        self.assertIn("ROLLBACK_NOT_AUTHORIZED", executor)
        self.assertIn("ROLLBACK_CHANGED_BASE_ROWS_OR_IDENTITIES", executor)

    def test_rehearsal_blocker_is_explicit_and_no_production_mutation_occurred(self) -> None:
        report = json.loads((PACKAGE / "isolated_rehearsal_report.json").read_text())
        self.assertEqual(report["status"], "BLOCKED")
        self.assertEqual(report["blocker"], "LOCAL_POSTGRES_SERVER_BINARY_ABSENT")
        self.assertEqual(report["production_database_ddl_dml"], 0)
        self.assertFalse(report["initial_failed_attempt_created_schema"])
        self.assertFalse(report["initial_failed_attempt_created_rows"])
        self.assertEqual(len(report["required_rehearsal_scenarios_unexecuted"]), 10)

    def test_activation_plan_blocks_on_unexecuted_rehearsal(self) -> None:
        report = json.loads((PACKAGE / "activation_plan.json").read_text())
        self.assertEqual(report["status"], "BLOCKED")
        self.assertEqual(len(report["blockers"]), 2)
        self.assertEqual(report["proposal_count"], 2919)
        self.assertTrue(all(value == 0 for value in report["abort_thresholds"].values()))


if __name__ == "__main__":
    unittest.main()
