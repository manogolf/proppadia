from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from backend.app.services.mlb.public_game_prediction_service import _public_row
from backend.mlb.public_game_predictions.phase_gating_v1 import (
    MoneylinePhaseGateError,
    canonical_rows_sha256,
    classify_moneyline_row,
    market_roi_metrics,
    partition_moneyline_rows,
    prediction_quality_metrics,
)
from backend.mlb.season_transition.game_phase_authority_v1 import (
    CanonicalGamePhaseAuthority,
    GamePhaseAuthorityError,
    GamePhaseAuthorityMetadata,
    GamePhaseAuthorityRecord,
)


def metadata(**changes: int | str) -> GamePhaseAuthorityMetadata:
    values = dict(
        authority_interface="TEST", backend="TEST", supported_season=2026,
        supported_from_date="2026-01-01", supported_through_date="2026-12-31",
        proposal_path="test.jsonl", proposal_sha256="a" * 64, proposal_count=1,
        source_manifest_path="sources.jsonl", source_manifest_sha256="b" * 64,
        source_file_count=1, source_observation_count=1,
        phase_contract_name="TEST", phase_contract_version="contract_v1",
        phase_contract_sha256="c" * 64, source_type_counts={"R": 1},
        phase_counts={"REGULAR_SEASON": 1}, authority_records_sha256="d" * 64,
        missing_count=0, unknown_count=0, conflicting_count=0,
        duplicate_identity_count=0,
    )
    values.update(changes)
    return GamePhaseAuthorityMetadata(**values)


def record(game_pk: int, raw: str, phase: str | None, round_name: str | None = None,
           relationships: dict | None = None) -> GamePhaseAuthorityRecord:
    return GamePhaseAuthorityRecord(
        game_pk=game_pk, source_season=2026, source_game_type=raw,
        season_phase=phase, postseason_round=round_name,
        season_name=f"MLB_2026_{phase}" if phase else None,
        source_round=round_name, schedule_relationships=relationships or {},
        primary_source_path="fixture.json", primary_source_sha256="e" * 64,
        source_paths=("fixture.json",), source_hashes=("e" * 64,),
        phase_decision="CLASSIFIED_FROM_AUTHORITATIVE_SOURCE_TYPE",
        authority_status="AUTHORITATIVE_UNAMBIGUOUS" if phase else "SPECIAL_EXCLUDED",
    )


class StubAuthority(CanonicalGamePhaseAuthority):
    def __init__(self, records: dict[int, GamePhaseAuthorityRecord], *, meta=None,
                 errors: dict[int, str] | None = None) -> None:
        self._records = records
        self._metadata = meta or metadata(proposal_count=len(records))
        self._errors = errors or {}

    @property
    def metadata(self) -> GamePhaseAuthorityMetadata:
        return self._metadata

    def lookup_exact(self, game_pk: object) -> GamePhaseAuthorityRecord:
        if game_pk is None:
            raise GamePhaseAuthorityError("GAME_PHASE_GAME_PK_MISSING")
        exact = int(game_pk)
        if exact in self._errors:
            raise GamePhaseAuthorityError(self._errors[exact], game_pk=exact)
        if exact not in self._records:
            raise GamePhaseAuthorityError("GAME_PHASE_ABSENT", game_pk=exact)
        return self._records[exact]


def row(game_pk: int = 1, game_date: str = "2026-09-22") -> dict:
    return {
        "game_date": game_date, "game_id": game_pk,
        "model_version": "MLB_GAME_PYTHAGOREAN_LOG5_V1",
        "winner_model_version": "MLB_GAME_PYTHAGOREAN_LOG5_V1",
        "prediction_snapshot_class": "DESIGNATED_DAILY_PUBLIC_SNAPSHOT",
        "home_team": "Home", "away_team": "Away", "predicted_winner": "Home",
        "home_win_probability": 0.60, "away_win_probability": 0.40,
        "official_home_runs": 5, "official_away_runs": 3,
    }


class MoneylinePhaseGatingV1Tests(unittest.TestCase):
    def test_every_supported_postseason_round_isolated(self) -> None:
        rounds = {
            "F": "WILD_CARD", "D": "DIVISION_SERIES",
            "L": "LEAGUE_CHAMPIONSHIP_SERIES", "W": "WORLD_SERIES",
            "P": "PLAYOFFS_UNSPECIFIED", "C": "CHAMPIONSHIP_UNSPECIFIED",
        }
        records = {n: record(n, raw, "POSTSEASON", round_name)
                   for n, (raw, round_name) in enumerate(rounds.items(), 10)}
        authority = StubAuthority(records)
        decisions = [classify_moneyline_row(row(game_pk), authority=authority)
                     for game_pk in records]
        self.assertEqual({item.source_game_type for item in decisions}, set(rounds))
        self.assertEqual({item.postseason_round for item in decisions}, set(rounds.values()))
        self.assertTrue(all(item.evaluation_partition == "POSTSEASON" for item in decisions))

    def test_regular_after_nominal_close_uses_type_not_date(self) -> None:
        decision = classify_moneyline_row(
            row(1, "2026-10-05"), authority=StubAuthority({1: record(1, "R", "REGULAR_SEASON")})
        )
        self.assertEqual(decision.evaluation_partition, "REGULAR_SEASON")

    def test_rescheduled_and_suspended_regular_relationships_do_not_change_phase(self) -> None:
        records = {
            1: record(1, "R", "REGULAR_SEASON", relationships={"rescheduledFrom": 2}),
            3: record(3, "R", "REGULAR_SEASON", relationships={"resumedFrom": 4}),
        }
        decisions = partition_moneyline_rows([row(1), row(3)], authority=StubAuthority(records))
        self.assertEqual(decisions.counts()["REGULAR_SEASON"], 2)

    def test_preseason_and_special_are_excluded(self) -> None:
        authority = StubAuthority(
            {1: record(1, "S", "PRESEASON")},
            errors={2: "GAME_PHASE_SPECIAL_EXCLUDED"},
        )
        result = partition_moneyline_rows([row(1), row(2)], authority=authority)
        self.assertEqual(result.counts(), {
            "REGULAR_SEASON": 0, "POSTSEASON": 0,
            "EXCLUDED_PRESEASON": 1, "EXCLUDED_SPECIAL": 1,
        })

    def test_missing_unknown_and_conflicting_authority_fail_closed(self) -> None:
        for code in ("GAME_PHASE_ABSENT", "GAME_PHASE_AUTHORITY_STATUS_UNKNOWN",
                     "GAME_PHASE_CONFLICT_BLOCKED"):
            with self.subTest(code=code), self.assertRaises(MoneylinePhaseGateError):
                classify_moneyline_row(row(9), authority=StubAuthority({}, errors={9: code}))

    def test_stale_authority_fails_closed(self) -> None:
        stale = metadata(supported_through_date="2026-09-21")
        with self.assertRaisesRegex(MoneylinePhaseGateError, "MONEYLINE_PHASE_AUTHORITY_STALE"):
            classify_moneyline_row(
                row(1, "2026-09-22"),
                authority=StubAuthority({1: record(1, "R", "REGULAR_SEASON")}, meta=stale),
            )

    def test_exact_game_pk_mismatch_fails(self) -> None:
        bad = row(1)
        bad["gamePk"] = 2
        with self.assertRaisesRegex(MoneylinePhaseGateError, "MONEYLINE_EXACT_GAME_PK_MISMATCH"):
            classify_moneyline_row(bad, authority=StubAuthority({1: record(1, "R", "REGULAR_SEASON")}))

    def test_source_type_conflict_fails(self) -> None:
        bad = row(1)
        bad["source_game_type"] = "W"
        with self.assertRaisesRegex(MoneylinePhaseGateError, "MONEYLINE_SOURCE_TYPE_CONFLICT"):
            classify_moneyline_row(bad, authority=StubAuthority({1: record(1, "R", "REGULAR_SEASON")}))

    def test_duplicate_authority_population_fails(self) -> None:
        invalid = metadata(duplicate_identity_count=1)
        with self.assertRaisesRegex(MoneylinePhaseGateError, "MONEYLINE_PHASE_AUTHORITY_POPULATION_INVALID"):
            classify_moneyline_row(
                row(1), authority=StubAuthority({1: record(1, "R", "REGULAR_SEASON")}, meta=invalid)
            )

    def test_duplicate_evaluation_identity_fails(self) -> None:
        authority = StubAuthority({1: record(1, "R", "REGULAR_SEASON")})
        with self.assertRaisesRegex(MoneylinePhaseGateError, "MONEYLINE_DUPLICATE_EVALUATION_IDENTITY"):
            partition_moneyline_rows(
                [row(1), row(1)], authority=authority,
                unique_identity_fields=("game_date", "game_id", "model_version"),
            )

    def test_zero_calendar_phase_reconstruction(self) -> None:
        postseason = record(1, "W", "POSTSEASON", "WORLD_SERIES")
        regular = record(2, "R", "REGULAR_SEASON")
        authority = StubAuthority({1: postseason, 2: regular})
        self.assertEqual(classify_moneyline_row(row(1, "2026-04-01"), authority=authority).normalized_phase,
                         "POSTSEASON")
        self.assertEqual(classify_moneyline_row(row(2, "2026-11-01"), authority=authority).normalized_phase,
                         "REGULAR_SEASON")

    def test_metric_partitions_are_separate(self) -> None:
        regular_row = row(1)
        postseason_row = row(2)
        authority = StubAuthority({
            1: record(1, "R", "REGULAR_SEASON"),
            2: record(2, "W", "POSTSEASON", "WORLD_SERIES"),
        })
        partitions = partition_moneyline_rows([regular_row, postseason_row], authority=authority)
        regular_metrics = prediction_quality_metrics(partitions.regular_season)
        postseason_metrics = prediction_quality_metrics(partitions.postseason)
        self.assertEqual(regular_metrics["rows"], 1)
        self.assertEqual(postseason_metrics["rows"], 1)
        self.assertNotIn("roi", regular_metrics)
        self.assertNotIn("flat_stake_return", postseason_metrics)

    def test_market_roi_is_not_prediction_quality(self) -> None:
        result = market_roi_metrics([{"selected_win": 1, "selected_decimal_price": 2.1}])
        self.assertAlmostEqual(result["roi"], 1.1)
        self.assertNotIn("brier", result)
        self.assertEqual(result["execution_status"], "HYPOTHETICAL_NO_FILL_EVIDENCE")

    def test_regular_content_hash_invariance(self) -> None:
        rows = [row(1), {**row(2), "home_win_probability": 0.55}]
        before = canonical_rows_sha256(rows)
        authority = StubAuthority({
            1: record(1, "R", "REGULAR_SEASON"),
            2: record(2, "R", "REGULAR_SEASON"),
        })
        result = partition_moneyline_rows(rows, authority=authority)
        self.assertEqual(canonical_rows_sha256(result.regular_season), before)
        self.assertEqual(result.regular_season[0], rows[0])

    def test_partition_does_not_mutate_an_immutable_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "predictions.jsonl"
            path.write_text('{"game_id":1}\n', encoding="utf-8")
            before = path.read_bytes()
            partition_moneyline_rows(
                [row(1)], authority=StubAuthority({1: record(1, "R", "REGULAR_SEASON")})
            )
            self.assertEqual(path.read_bytes(), before)

    def test_repeated_execution_is_deterministic(self) -> None:
        authority = StubAuthority({1: record(1, "R", "REGULAR_SEASON")})
        first = partition_moneyline_rows([row(1)], authority=authority)
        second = partition_moneyline_rows([row(1)], authority=authority)
        self.assertEqual(first, second)

    def test_public_projection_labels_postseason_without_changing_probability(self) -> None:
        source = row(1)
        source.update({
            "winner_model_hash": "804535afde26e09516571c7a105d8376c2607cb7abc572621e80d8a9a006acf6",
            "admission_status": "ADMITTED_SHADOW", "confidence_band": "MODERATE",
            "scheduled_start_utc": "2026-09-22T23:00:00Z",
            "prediction_timestamp_utc": "2026-09-22T20:00:00Z",
        })
        authority = StubAuthority({1: record(1, "D", "POSTSEASON", "DIVISION_SERIES")})
        with patch(
            "backend.mlb.public_game_predictions.phase_gating_v1.verified_moneyline_phase_authority",
            return_value=authority,
        ):
            projected = _public_row(source, "2026-09-22")
        self.assertEqual(projected["normalized_phase"], "POSTSEASON")
        self.assertEqual(projected["postseason_round"], "DIVISION_SERIES")
        self.assertEqual(projected["home_win_probability"], source["home_win_probability"])
        self.assertEqual(projected["away_win_probability"], source["away_win_probability"])

    def test_outcome_write_boundary_blocks_preseason_before_database_access(self) -> None:
        from backend.mlb.public_game_predictions import durable_store_v1 as durable

        grade = row(1)
        grade["official_status"] = "Final"
        authority = StubAuthority({1: record(1, "S", "PRESEASON")})
        with patch(
            "backend.mlb.public_game_predictions.phase_gating_v1.verified_moneyline_phase_authority",
            return_value=authority,
        ), patch.object(durable, "pg_connect", side_effect=AssertionError("database reached")):
            with self.assertRaisesRegex(MoneylinePhaseGateError, "MONEYLINE_ROW_NOT_EVALUATION_ELIGIBLE"):
                durable.append_outcome_grade(grade)

    def test_shared_report_loader_selects_only_requested_partition(self) -> None:
        import pandas as pd
        from backend.mlb.scripts.audit_mlb_moneyline_probability_region_premise_v1 import (
            _phase_partition_frame,
        )

        regular_row = row(1)
        postseason_row = row(2)
        authority = StubAuthority({
            1: record(1, "R", "REGULAR_SEASON"),
            2: record(2, "F", "POSTSEASON", "WILD_CARD"),
        })
        frame = pd.DataFrame([regular_row, postseason_row])
        regular = _phase_partition_frame(frame, authority=authority)
        postseason = _phase_partition_frame(
            frame, evaluation_phase="POSTSEASON", authority=authority
        )
        self.assertEqual(regular.game_id.tolist(), [1])
        self.assertEqual(postseason.game_id.tolist(), [2])


if __name__ == "__main__":
    unittest.main()
