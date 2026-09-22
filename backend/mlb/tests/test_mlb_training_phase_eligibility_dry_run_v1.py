from __future__ import annotations

import io
import tempfile
import unittest
from contextlib import redirect_stdout
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from backend.mlb.scripts.build_mlb_training_phase_eligibility_dry_run_v1 import (
    EXPECTED_COUNTS,
    execute_dry_run,
)
from backend.mlb.season_transition.game_phase_authority_v1 import (
    DEFAULT_PROPOSAL_PATH,
    CanonicalGamePhaseAuthority,
    GamePhaseAuthorityError,
    GamePhaseAuthorityMetadata,
    GamePhaseAuthorityRecord,
    HashedProposalAuthority,
)
from backend.mlb.season_transition.training_phase_eligibility_v1 import (
    ADMITTED_REGULAR_SEASON,
    BLOCKED_ABSENT_AUTHORITY,
    BLOCKED_CONFLICTING_TYPE,
    BLOCKED_DUPLICATE_AUTHORITY,
    BLOCKED_MISSING_GAME_PK,
    BLOCKED_SPECIAL_TYPE,
    BLOCKED_UNKNOWN_TYPE,
    EXCLUDED_POSTSEASON,
    EXCLUDED_PRESEASON,
    EligibilityGateBlocked,
    filter_regular_season_membership,
)


class StubAuthority(CanonicalGamePhaseAuthority):
    def __init__(
        self,
        metadata: GamePhaseAuthorityMetadata,
        records: dict[int, GamePhaseAuthorityRecord] | None = None,
        errors: dict[int, str] | None = None,
    ) -> None:
        self._metadata = metadata
        self._records = records or {}
        self._errors = errors or {}

    @property
    def metadata(self) -> GamePhaseAuthorityMetadata:
        return self._metadata

    def lookup_exact(self, game_pk: object) -> GamePhaseAuthorityRecord:
        if game_pk is None or game_pk == "":
            raise GamePhaseAuthorityError("GAME_PHASE_GAME_PK_MISSING")
        exact = int(game_pk)
        if exact in self._errors:
            raise GamePhaseAuthorityError(self._errors[exact], game_pk=exact)
        if exact not in self._records:
            raise GamePhaseAuthorityError("GAME_PHASE_ABSENT", game_pk=exact)
        return self._records[exact]


class TrainingPhaseEligibilityDryRunTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.file_authority = HashedProposalAuthority()
        cls.regular = cls.file_authority.lookup_exact(822744)
        cls.preseason = cls.file_authority.lookup_exact(831427)
        cls.full_report = execute_dry_run(output_dir=None)

    def gate(
        self,
        rows: list[dict[str, object]],
        *,
        authority: CanonicalGamePhaseAuthority | None = None,
    ):
        return filter_regular_season_membership(
            rows,
            game_pk_field="game_id",
            source_type_field="source_game_type",
            authority=authority or self.file_authority,
            consumer_identity="UNIT_TEST",
            input_identity="UNIT_TEST_INPUT",
            row_identity_fields=("id", "game_id"),
            invariant_field_groups={
                "features": ("id", "feature"),
                "targets": ("id", "target"),
                "order": ("id", "game_id"),
            },
        )

    def blocked_report(
        self,
        rows: list[dict[str, object]],
        *,
        authority: CanonicalGamePhaseAuthority | None = None,
    ):
        with self.assertRaises(EligibilityGateBlocked) as caught:
            self.gate(rows, authority=authority)
        return caught.exception.report

    def test_regular_season_admission(self) -> None:
        row = {"id": "r", "game_id": 822744, "feature": 1.25, "target": 1}
        result = self.gate([row])
        self.assertEqual(result.admitted_rows, (row,))
        self.assertIs(result.admitted_rows[0], row)
        self.assertEqual(result.report["decision_row_counts"][ADMITTED_REGULAR_SEASON], 1)

    def test_preseason_exclusion(self) -> None:
        result = self.gate([{"id": "s", "game_id": 831427}])
        self.assertEqual(result.admitted_rows, ())
        self.assertEqual(result.report["decision_row_counts"][EXCLUDED_PRESEASON], 1)

    def test_postseason_exclusion(self) -> None:
        postseason = replace(
            self.regular,
            game_pk=900001,
            source_game_type="W",
            season_phase="POSTSEASON",
            postseason_round="WORLD_SERIES",
        )
        authority = StubAuthority(self.file_authority.metadata, {900001: postseason})
        result = self.gate([{"id": "p", "game_id": 900001}], authority=authority)
        self.assertEqual(result.report["decision_row_counts"][EXCLUDED_POSTSEASON], 1)

    def test_missing_game_pk_blocks(self) -> None:
        report = self.blocked_report([{"id": "missing", "game_id": None}])
        self.assertEqual(report["decision_row_counts"][BLOCKED_MISSING_GAME_PK], 1)

    def test_missing_authority_blocks(self) -> None:
        report = self.blocked_report([{"id": "absent", "game_id": 999999999}])
        self.assertEqual(report["decision_row_counts"][BLOCKED_ABSENT_AUTHORITY], 1)

    def test_unknown_and_special_type_block(self) -> None:
        unknown = self.blocked_report(
            [{"id": "unknown", "game_id": 822744, "source_game_type": "Z"}]
        )
        self.assertEqual(unknown["decision_row_counts"][BLOCKED_UNKNOWN_TYPE], 1)
        authority = StubAuthority(
            self.file_authority.metadata,
            errors={900002: "GAME_PHASE_SPECIAL_EXCLUDED"},
        )
        special = self.blocked_report(
            [{"id": "special", "game_id": 900002}], authority=authority
        )
        self.assertEqual(special["decision_row_counts"][BLOCKED_SPECIAL_TYPE], 1)

    def test_conflicting_authority_blocks(self) -> None:
        report = self.blocked_report(
            [{"id": "conflict", "game_id": 822744, "source_game_type": "W"}]
        )
        self.assertEqual(report["decision_row_counts"][BLOCKED_CONFLICTING_TYPE], 1)

    def test_duplicate_authority_blocks_before_rows(self) -> None:
        metadata = replace(self.file_authority.metadata, duplicate_identity_count=1)
        authority = StubAuthority(metadata, {822744: self.regular})
        report = self.blocked_report(
            [{"id": "r", "game_id": 822744}], authority=authority
        )
        self.assertEqual(report["decision_row_counts"][BLOCKED_DUPLICATE_AUTHORITY], 1)
        self.assertEqual(report["input_row_count"], 0)

    def test_authority_hash_mismatch_blocks(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "proposal.jsonl"
            path.write_bytes(DEFAULT_PROPOSAL_PATH.read_bytes() + b"\n")
            with self.assertRaisesRegex(
                GamePhaseAuthorityError,
                "GAME_PHASE_PROPOSAL_HASH_MISMATCH",
            ):
                HashedProposalAuthority(proposal_path=path)

    def test_deterministic_repeated_execution_and_stable_order(self) -> None:
        rows = [
            {"id": "first", "game_id": 822744, "feature": 4.5, "target": 0},
            {"id": "drop", "game_id": 831427, "feature": 9.0, "target": 1},
            {"id": "last", "game_id": 822744, "feature": 7.5, "target": 1},
        ]
        first = self.gate(rows)
        second = self.gate(rows)
        self.assertEqual(first.report, second.report)
        self.assertEqual([r["id"] for r in first.admitted_rows], ["first", "last"])

    def test_ordinary_invocation_remains_on_training_path(self) -> None:
        from backend.mlb import model_trainer

        with tempfile.TemporaryDirectory() as directory, patch(
            "backend.mlb.shared.model_authority.assert_predictive_model_qualified"
        ) as qualify, patch.object(
            model_trainer, "train_models_for_prop", return_value=None
        ) as train, patch.object(
            model_trainer, "MODELS_DIR", Path(directory) / "models"
        ), patch.object(
            model_trainer, "LATEST_DIR", Path(directory) / "models/latest"
        ), patch.object(
            model_trainer, "ARCHIVE_DIR", Path(directory) / "models/archive"
        ), redirect_stdout(io.StringIO()):
            code = model_trainer.main(["--prop", "hits", "--quiet"])
        self.assertEqual(code, 0)
        qualify.assert_called_once_with("retired_model_training")
        train.assert_called_once()

    def test_dry_run_cannot_fit_or_write_operational_artifacts(self) -> None:
        from backend.mlb import model_trainer

        forbidden = AssertionError("DRY_RUN_REACHED_FORBIDDEN_PATH")
        with patch.object(
            model_trainer, "train_models_for_prop", side_effect=forbidden
        ), patch.object(
            model_trainer, "build_pipeline", side_effect=forbidden
        ), patch.object(
            model_trainer, "_atomic_write_bytes", side_effect=forbidden
        ), patch.object(
            model_trainer.joblib, "dump", side_effect=forbidden
        ), patch.object(
            model_trainer, "pg_fetchall", side_effect=forbidden
        ), patch.object(
            model_trainer, "create_client", side_effect=forbidden
        ), redirect_stdout(io.StringIO()):
            code = model_trainer.main(["--phase-eligibility-dry-run"])
        self.assertEqual(code, 0)
        self.assertEqual(self.full_report["safety"]["model_fit_calls"], 0)
        self.assertEqual(self.full_report["safety"]["operational_artifact_writes"], 0)

    def test_exact_expected_frozen_counts(self) -> None:
        gate = self.full_report["gate_report"]
        self.assertEqual(gate["input_row_count"], EXPECTED_COUNTS["input_rows"])
        self.assertEqual(
            gate["input_distinct_game_pk_count"], EXPECTED_COUNTS["input_game_pks"]
        )
        self.assertEqual(gate["admitted_row_count"], EXPECTED_COUNTS["admitted_rows"])
        self.assertEqual(
            gate["admitted_distinct_game_pk_count"],
            EXPECTED_COUNTS["admitted_game_pks"],
        )
        self.assertEqual(
            gate["decision_row_counts"][EXCLUDED_PRESEASON],
            EXPECTED_COUNTS["excluded_preseason_rows"],
        )
        self.assertEqual(
            len(gate["decision_game_pks"][EXCLUDED_PRESEASON]),
            EXPECTED_COUNTS["excluded_preseason_game_pks"],
        )

    def test_retained_feature_target_and_order_hash_invariance(self) -> None:
        self.assertTrue(self.full_report["invariance"]["all_retained_hashes_identical"])
        hashes = self.full_report["invariance"]["hashes"]
        self.assertEqual(
            set(hashes),
            {
                "retained_feature_source_row_commitment",
                "retained_target_projection",
                "retained_row_order",
            },
        )
        for evidence in hashes.values():
            self.assertEqual(evidence["before_sha256"], evidence["after_sha256"])

    def test_existing_hits_authority_pilot_remains_valid(self) -> None:
        from backend.mlb.scripts.validate_mlb_game_phase_file_authority_hits_pilot_v1 import (
            validate,
        )

        report = validate()
        self.assertEqual(report["status"], "PASS")


if __name__ == "__main__":
    unittest.main()
