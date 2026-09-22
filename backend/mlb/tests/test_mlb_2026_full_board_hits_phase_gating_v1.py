from __future__ import annotations

import inspect
import tempfile
import unittest
from pathlib import Path

from backend.mlb.hits05_full_board_shadow import ledger_v1 as ledger
from backend.mlb.hits05_full_board_shadow.phase_gating_v1 import (
    FullBoardHitsPhaseGateError,
    canonical_rows_sha256,
    classify_full_board_hits_row,
    partition_full_board_hits_rows,
)
from backend.mlb.scripts.report_mlb_hits05_full_board_shadow_v1 import build_report
from backend.mlb.scripts.score_mlb_hits05_full_board_shadow_v1 import prepare_baseball_features
from backend.mlb.season_transition.game_phase_authority_v1 import (
    CanonicalGamePhaseAuthority,
    GamePhaseAuthorityError,
    GamePhaseAuthorityMetadata,
    GamePhaseAuthorityRecord,
)


def metadata(**changes: int | str) -> GamePhaseAuthorityMetadata:
    values = dict(
        authority_interface="SYNTHETIC_TEST_AUTHORITY",
        backend="SYNTHETIC_TEST_BACKEND",
        supported_season=2026,
        supported_from_date="2026-01-01",
        supported_through_date="2026-12-31",
        proposal_path="synthetic/proposal.jsonl",
        proposal_sha256="a" * 64,
        proposal_count=1,
        source_manifest_path="synthetic/sources.jsonl",
        source_manifest_sha256="b" * 64,
        source_file_count=1,
        source_observation_count=1,
        phase_contract_name="SYNTHETIC_TEST_CONTRACT",
        phase_contract_version="contract_v1",
        phase_contract_sha256="c" * 64,
        source_type_counts={"R": 1},
        phase_counts={"REGULAR_SEASON": 1},
        authority_records_sha256="d" * 64,
        missing_count=0,
        unknown_count=0,
        conflicting_count=0,
        duplicate_identity_count=0,
    )
    values.update(changes)
    return GamePhaseAuthorityMetadata(**values)


def record(
    game_pk: int,
    source_type: str,
    phase: str | None,
    postseason_round: str | None = None,
    relationships: dict | None = None,
) -> GamePhaseAuthorityRecord:
    return GamePhaseAuthorityRecord(
        game_pk=game_pk,
        source_season=2026,
        source_game_type=source_type,
        season_phase=phase,
        postseason_round=postseason_round,
        season_name=f"SYNTHETIC_{phase}" if phase else None,
        source_round=postseason_round,
        schedule_relationships=relationships or {},
        primary_source_path="synthetic_fixture.json",
        primary_source_sha256="e" * 64,
        source_paths=("synthetic_fixture.json",),
        source_hashes=("e" * 64,),
        phase_decision="CLASSIFIED_FROM_AUTHORITATIVE_SOURCE_TYPE",
        authority_status="AUTHORITATIVE_UNAMBIGUOUS" if phase else "SPECIAL_EXCLUDED",
    )


class StubAuthority(CanonicalGamePhaseAuthority):
    def __init__(
        self,
        records: dict[int, GamePhaseAuthorityRecord],
        *,
        meta: GamePhaseAuthorityMetadata | None = None,
        errors: dict[int, str] | None = None,
    ) -> None:
        self._records = records
        self._metadata = meta or metadata(proposal_count=len(records))
        self._errors = errors or {}

    @property
    def metadata(self) -> GamePhaseAuthorityMetadata:
        return self._metadata

    def lookup_exact(self, game_pk: object) -> GamePhaseAuthorityRecord:
        try:
            exact = int(game_pk)
        except (TypeError, ValueError):
            raise GamePhaseAuthorityError("GAME_PHASE_GAME_PK_MISSING") from None
        if exact in self._errors:
            raise GamePhaseAuthorityError(self._errors[exact], game_pk=exact)
        if exact not in self._records:
            raise GamePhaseAuthorityError("GAME_PHASE_ABSENT", game_pk=exact)
        return self._records[exact]


def row(game_pk: int = 1, slate_date: str = "2026-09-22", **changes: object) -> dict:
    value = {
        "synthetic_fixture": True,
        "canonical_identity": f"{slate_date}|{game_pk}|42|hits|0.5",
        "slate_date": slate_date,
        "game_id": game_pk,
        "probability_over": 0.6,
        "actual_hits": 1,
    }
    value.update(changes)
    return value


class FullBoardHitsPhaseGatingV1Tests(unittest.TestCase):
    def test_all_supported_postseason_rounds_use_synthetic_fixtures(self) -> None:
        rounds = {
            "F": "WILD_CARD",
            "D": "DIVISION_SERIES",
            "L": "LEAGUE_CHAMPIONSHIP_SERIES",
            "W": "WORLD_SERIES",
            "P": "PLAYOFFS_UNSPECIFIED",
            "C": "CHAMPIONSHIP_UNSPECIFIED",
        }
        records = {
            game_pk: record(game_pk, source_type, "POSTSEASON", round_name)
            for game_pk, (source_type, round_name) in enumerate(rounds.items(), 100)
        }
        decisions = [
            classify_full_board_hits_row(row(game_pk), authority=StubAuthority(records))
            for game_pk in records
        ]
        self.assertEqual({item.source_game_type for item in decisions}, set(rounds))
        self.assertEqual({item.postseason_round for item in decisions}, set(rounds.values()))
        self.assertTrue(all(item.evaluation_partition == "POSTSEASON" for item in decisions))

    def test_regular_after_nominal_close_is_not_reclassified_by_date(self) -> None:
        decision = classify_full_board_hits_row(
            row(1, "2026-10-05"),
            authority=StubAuthority({1: record(1, "R", "REGULAR_SEASON")}),
        )
        self.assertEqual(decision.evaluation_partition, "REGULAR_SEASON")

    def test_postponed_rescheduled_and_suspended_resumed_remain_regular(self) -> None:
        authority = StubAuthority({
            1: record(1, "R", "REGULAR_SEASON", relationships={"rescheduledFrom": 9}),
            2: record(2, "R", "REGULAR_SEASON", relationships={"resumedFrom": 8}),
        })
        partitions = partition_full_board_hits_rows([row(1), row(2)], authority=authority)
        self.assertEqual(partitions.counts()["REGULAR_SEASON"], 2)

    def test_preseason_and_special_enter_neither_evaluation(self) -> None:
        authority = StubAuthority(
            {1: record(1, "S", "PRESEASON")},
            errors={2: "GAME_PHASE_SPECIAL_EXCLUDED"},
        )
        partitions = partition_full_board_hits_rows([row(1), row(2)], authority=authority)
        self.assertEqual(partitions.counts(), {
            "REGULAR_SEASON": 0,
            "POSTSEASON": 0,
            "EXCLUDED_PRESEASON": 1,
            "EXCLUDED_SPECIAL": 1,
        })

    def test_missing_unknown_and_conflicting_authority_fail_closed(self) -> None:
        for code in (
            "GAME_PHASE_ABSENT",
            "GAME_PHASE_AUTHORITY_STATUS_UNKNOWN",
            "GAME_PHASE_CONFLICT_BLOCKED",
        ):
            with self.subTest(code=code), self.assertRaises(FullBoardHitsPhaseGateError):
                classify_full_board_hits_row(
                    row(9), authority=StubAuthority({}, errors={9: code})
                )

    def test_stale_authority_fails_closed(self) -> None:
        stale = metadata(supported_through_date="2026-09-21")
        with self.assertRaisesRegex(FullBoardHitsPhaseGateError, "PHASE_AUTHORITY_STALE"):
            classify_full_board_hits_row(
                row(1),
                authority=StubAuthority(
                    {1: record(1, "R", "REGULAR_SEASON")}, meta=stale
                ),
            )

    def test_source_type_conflict_and_exact_game_pk_mismatch_fail(self) -> None:
        authority = StubAuthority({1: record(1, "R", "REGULAR_SEASON")})
        with self.assertRaisesRegex(FullBoardHitsPhaseGateError, "SOURCE_TYPE_CONFLICT"):
            classify_full_board_hits_row(row(1, game_type="W"), authority=authority)
        with self.assertRaisesRegex(FullBoardHitsPhaseGateError, "EXACT_GAME_PK_MISMATCH"):
            classify_full_board_hits_row(row(1, gamePk=2), authority=authority)
        with self.assertRaisesRegex(FullBoardHitsPhaseGateError, "GAME_PK_MISSING"):
            classify_full_board_hits_row(row(1.5), authority=authority)

    def test_duplicate_authority_and_evaluation_identity_fail(self) -> None:
        invalid = metadata(duplicate_identity_count=1)
        with self.assertRaisesRegex(FullBoardHitsPhaseGateError, "POPULATION_INVALID"):
            classify_full_board_hits_row(
                row(1),
                authority=StubAuthority(
                    {1: record(1, "R", "REGULAR_SEASON")}, meta=invalid
                ),
            )
        authority = StubAuthority({1: record(1, "R", "REGULAR_SEASON")})
        with self.assertRaisesRegex(FullBoardHitsPhaseGateError, "DUPLICATE_EVALUATION_IDENTITY"):
            partition_full_board_hits_rows(
                [row(1), row(1)],
                authority=authority,
                unique_identity_fields=("canonical_identity",),
            )

    def test_missing_game_type_no_longer_defaults_to_regular(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "AUTHORITATIVE_GAME_TYPE_REQUIRED"):
            prepare_baseball_features({})
        source = inspect.getsource(prepare_baseball_features)
        self.assertNotIn('or "R"', source)
        self.assertNotIn("or 'R'", source)

    def test_regular_and_postseason_metrics_receive_disjoint_rows(self) -> None:
        authority = StubAuthority({
            1: record(1, "R", "REGULAR_SEASON"),
            2: record(2, "W", "POSTSEASON", "WORLD_SERIES"),
        })
        partitions = partition_full_board_hits_rows([row(1), row(2)], authority=authority)
        self.assertEqual([item["game_id"] for item in partitions.regular_season], [1])
        self.assertEqual([item["game_id"] for item in partitions.postseason], [2])
        self.assertEqual(
            inspect.signature(build_report).parameters["evaluation_phase"].default,
            "REGULAR_SEASON",
        )

    def test_report_builder_computes_disjoint_phase_metrics(self) -> None:
        authority = StubAuthority({
            1: record(1, "R", "REGULAR_SEASON"),
            2: record(2, "W", "POSTSEASON", "WORLD_SERIES"),
        })
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "phase_report.sqlite3"
            connection = ledger.connect_ledger(path)
            for game_pk, player_id, probability, actual_hits in (
                (1, 101, 0.7, 1),
                (2, 102, 0.3, 0),
            ):
                feature, replay, inputs = {}, {}, []
                prediction = {
                    "slate_date": "2026-09-22",
                    "game_id": game_pk,
                    "player_id": player_id,
                    "model_semantic_id": ledger.MODEL_ID,
                    "model_artifact_sha256": ledger.MODEL_HASH,
                    "scheduled_start_utc": "2026-09-22T20:00:00Z",
                    "prediction_timestamp_utc": "2026-09-22T18:00:00Z",
                    "run_tag": f"synthetic_phase_{game_pk}",
                    "probability_over": probability,
                    "score_board_rank": 1,
                    "score_board_percentile": 1.0,
                    "baseline_population_probability": 0.5,
                    "baseline_hitter_shrunk_probability": 0.5,
                    "feature_state_sha256": ledger.payload_hash(feature),
                    "replay_references_sha256": ledger.payload_hash(replay),
                    "input_artifacts_sha256": ledger.payload_hash(inputs),
                    "prestart_integrity_result": "PASS_SYNTHETIC_FIXTURE",
                    "evidence_mode": "PROSPECTIVE",
                }
                ledger.append_prediction_with_context(
                    connection, prediction, feature, replay, inputs
                )
                identity = ledger.canonical_identity("2026-09-22", game_pk, player_id)
                ledger.append_outcome(connection, identity, {
                    "slate_date": "2026-09-22",
                    "game_id": game_pk,
                    "player_id": player_id,
                    "actual_hits": actual_hits,
                    "appearance_status": "APPEARANCE_RESOLVED",
                    "outcome_status": "CANONICAL_RESOLVED_OFFICIAL_PLAYER_STAT",
                    "grading_timestamp_utc": "2026-09-23T12:00:00Z",
                    "grading_source": "SYNTHETIC_FIXTURE",
                    "grading_source_sha256": "f" * 64,
                })
            connection.close()
            regular = build_report(
                path, evaluation_phase="REGULAR_SEASON", phase_authority=authority
            )
            postseason = build_report(
                path, evaluation_phase="POSTSEASON", phase_authority=authority
            )
            self.assertEqual(regular["counts"]["prospective_predictions"], 1)
            self.assertEqual(postseason["counts"]["prospective_predictions"], 1)
            self.assertEqual(regular["phase_partition_counts"]["POSTSEASON"], 1)
            self.assertEqual(postseason["phase_partition_counts"]["REGULAR_SEASON"], 1)
            self.assertEqual(
                regular["population_evaluations"]["entire_technically_eligible_appearance_resolved"]["model"]["rows"],
                1,
            )
            self.assertEqual(
                postseason["population_evaluations"]["entire_technically_eligible_appearance_resolved"]["model"]["rows"],
                1,
            )

    def test_zero_calendar_based_phase_reconstruction(self) -> None:
        authority = StubAuthority({
            1: record(1, "W", "POSTSEASON", "WORLD_SERIES"),
            2: record(2, "R", "REGULAR_SEASON"),
        })
        self.assertEqual(
            classify_full_board_hits_row(row(1, "2026-04-01"), authority=authority).normalized_phase,
            "POSTSEASON",
        )
        self.assertEqual(
            classify_full_board_hits_row(row(2, "2026-11-01"), authority=authority).normalized_phase,
            "REGULAR_SEASON",
        )

    def test_retained_row_hash_is_invariant_and_repeated_run_is_deterministic(self) -> None:
        rows = [row(1), row(2, probability_over=0.55, actual_hits=0)]
        authority = StubAuthority({
            1: record(1, "R", "REGULAR_SEASON"),
            2: record(2, "R", "REGULAR_SEASON"),
        })
        before = canonical_rows_sha256(rows)
        first = partition_full_board_hits_rows(rows, authority=authority)
        second = partition_full_board_hits_rows(rows, authority=authority)
        self.assertEqual(first, second)
        self.assertEqual(canonical_rows_sha256(first.regular_season), before)

    def test_selector_and_quick_card_remain_outside_full_board_modules(self) -> None:
        for function in (classify_full_board_hits_row, build_report):
            source = inspect.getsource(function).lower()
            self.assertNotIn("quick_card", source)
            self.assertNotIn("lane_selector", source)


if __name__ == "__main__":
    unittest.main()
