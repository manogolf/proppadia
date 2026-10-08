from __future__ import annotations

import hashlib
import inspect
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from backend.mlb.scripts import grade_mlb_totals_prospective_shadow_v1 as raw_grade
from backend.mlb.scripts import run_mlb_totals_c_shadow_daily_v1 as c_daily
from backend.mlb.scripts import run_mlb_totals_c_shadow_v1 as c_score
from backend.mlb.scripts import run_mlb_totals_prospective_shadow_v1 as raw_score
from backend.mlb.totals_predictions.live_context_bridge_v1 import (
    TotalsLiveContextError,
    normalize_schedule,
)
from backend.mlb.totals_predictions import c_shadow_v1 as c_ledger
from backend.mlb.totals_predictions import prospective_shadow_v1 as raw_ledger
from backend.mlb.totals_predictions.phase_gating_v1 import (
    TotalsPhaseGateError,
    authority_with_retained_schedule_rows,
    canonical_rows_sha256,
    classify_totals_row,
    partition_totals_rows,
    require_raw_c_consistency,
)
from backend.mlb.season_transition.game_phase_authority_v1 import (
    CanonicalGamePhaseAuthority,
    GamePhaseAuthorityError,
    GamePhaseAuthorityMetadata,
    GamePhaseAuthorityRecord,
)
from backend.mlb.season_transition.runtime_schedule_authority_v1 import RuntimeScheduleAuthority
from backend.mlb.season_transition.phase_authority_snapshot_v1 import REPO_ROOT
from backend.mlb.scripts.validate_mlb_2026_totals_phase_gating_v1 import aggregate_status


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


def row(game_pk: int = 1, game_date: str = "2026-09-22", **changes: object) -> dict:
    value = {
        "canonical_identity": f"{game_date}|{game_pk}|TOTALS",
        "game_date": game_date,
        "game_pk": game_pk,
        "expected_total": 8.5,
        "official_final_total": 9,
        "total_line": 8.0,
    }
    value.update(changes)
    return value


class TotalsPhaseGatingV1Tests(unittest.TestCase):
    def test_aggregate_status_requires_every_check(self) -> None:
        self.assertEqual(aggregate_status({"one": True, "two": False}), "FAIL")
        self.assertEqual(aggregate_status({"one": True, "two": "INCONCLUSIVE"}), "INCONCLUSIVE")
        self.assertEqual(aggregate_status({"one": True, "two": "PASS"}), "PASS")

    def test_retained_october_6_schedule_authorizes_exact_division_series_games(self) -> None:
        source = REPO_ROOT / (
            "backend/mlb/exports/provider_event_game_bindings/schedule_sources/2026-10-06/"
            "statsapi_schedule__local_daily_20261006T123004Z.json"
        )
        source_hash = hashlib.sha256(source.read_bytes()).hexdigest()
        authority = RuntimeScheduleAuthority(
            source_path=source, expected_source_sha256=source_hash,
        )
        decisions = []
        for game_pk in (849819, 849826):
            decision = classify_totals_row(
                row(game_pk, "2026-10-06", source_game_type="D"),
                authority=authority,
            )
            record_value = authority.lookup_exact(game_pk)
            self.assertEqual(decision.evaluation_partition, "POSTSEASON")
            self.assertEqual(record_value.source_game_type, "D")
            self.assertEqual(record_value.source_round, "NL Division Series")
            self.assertEqual(record_value.primary_source_sha256, source_hash)
            decisions.append(decision)
        self.assertEqual({item.game_pk for item in decisions}, {849819, 849826})

    def test_retained_prediction_binding_rehydrates_exact_schedule_for_grading(self) -> None:
        source = REPO_ROOT / (
            "backend/mlb/exports/provider_event_game_bindings/schedule_sources/2026-10-06/"
            "statsapi_schedule__local_daily_20261006T123004Z.json"
        )
        source_hash = hashlib.sha256(source.read_bytes()).hexdigest()
        overlay = authority_with_retained_schedule_rows([
            row(849819, "2026-10-06", source_game_type="D",
                schedule_source_path=source.relative_to(REPO_ROOT).as_posix(),
                schedule_source_sha256=source_hash),
        ])
        decision = classify_totals_row(
            row(849819, "2026-10-06", source_game_type="D"), authority=overlay,
        )
        self.assertEqual(decision.evaluation_partition, "POSTSEASON")
        self.assertEqual(overlay.lookup_exact(849819).primary_source_sha256, source_hash)

    def test_runtime_overlay_requires_exact_schedule_date_and_identity(self) -> None:
        source = REPO_ROOT / (
            "backend/mlb/exports/provider_event_game_bindings/schedule_sources/2026-10-06/"
            "statsapi_schedule__local_daily_20261006T123004Z.json"
        )
        authority = RuntimeScheduleAuthority(
            source_path=source, expected_source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
        )
        with self.assertRaisesRegex(TotalsPhaseGateError, "SOURCE_TYPE_CONFLICT"):
            classify_totals_row(
                row(849819, "2026-10-06", source_game_type="R"), authority=authority,
            )
        with self.assertRaisesRegex(TotalsPhaseGateError, "PHASE_AUTHORITY_STALE"):
            classify_totals_row(row(849819, "2026-10-07", source_game_type="D"), authority=authority)
        with self.assertRaises(TotalsPhaseGateError):
            classify_totals_row(row(849820, "2026-10-06", source_game_type="D"), authority=authority)

    def test_all_supported_postseason_rounds_are_transient_labels(self) -> None:
        rounds = {
            "F": "WILD_CARD", "D": "DIVISION_SERIES",
            "L": "LEAGUE_CHAMPIONSHIP_SERIES", "W": "WORLD_SERIES",
            "P": "PLAYOFFS_UNSPECIFIED", "C": "CHAMPIONSHIP_UNSPECIFIED",
        }
        records = {
            game_pk: record(game_pk, source_type, "POSTSEASON", round_name)
            for game_pk, (source_type, round_name) in enumerate(rounds.items(), 100)
        }
        decisions = [
            classify_totals_row(row(game_pk), authority=StubAuthority(records))
            for game_pk in records
        ]
        self.assertEqual({item.source_game_type for item in decisions}, set(rounds))
        self.assertEqual({item.postseason_round for item in decisions}, set(rounds.values()))
        self.assertTrue(all(item.evaluation_partition == "POSTSEASON" for item in decisions))

    def test_regular_after_nominal_close_uses_authority_not_date(self) -> None:
        decision = classify_totals_row(
            row(1, "2026-10-10"),
            authority=StubAuthority({1: record(1, "R", "REGULAR_SEASON")}),
        )
        self.assertEqual(decision.evaluation_partition, "REGULAR_SEASON")

    def test_rescheduled_and_resumed_regular_games_stay_regular(self) -> None:
        authority = StubAuthority({
            1: record(1, "R", "REGULAR_SEASON", relationships={"rescheduledFrom": 9}),
            2: record(2, "R", "REGULAR_SEASON", relationships={"resumedFrom": 8}),
        })
        partitions = partition_totals_rows([row(1), row(2)], authority=authority)
        self.assertEqual(partitions.counts()["REGULAR_SEASON"], 2)

    def test_preseason_and_special_enter_neither_evaluation(self) -> None:
        authority = StubAuthority(
            {1: record(1, "S", "PRESEASON")},
            errors={2: "GAME_PHASE_SPECIAL_EXCLUDED"},
        )
        partitions = partition_totals_rows([row(1), row(2)], authority=authority)
        self.assertEqual(partitions.counts(), {
            "REGULAR_SEASON": 0, "POSTSEASON": 0,
            "EXCLUDED_PRESEASON": 1, "EXCLUDED_SPECIAL": 1,
        })

    def test_missing_unknown_conflicting_and_duplicate_authority_fail_closed(self) -> None:
        for code in (
            "GAME_PHASE_ABSENT", "GAME_PHASE_AUTHORITY_STATUS_UNKNOWN",
            "GAME_PHASE_CONFLICT_BLOCKED",
        ):
            with self.subTest(code=code), self.assertRaises(TotalsPhaseGateError):
                classify_totals_row(row(9), authority=StubAuthority({}, errors={9: code}))
        invalid = metadata(duplicate_identity_count=1)
        with self.assertRaisesRegex(TotalsPhaseGateError, "POPULATION_INVALID"):
            classify_totals_row(
                row(1), authority=StubAuthority(
                    {1: record(1, "R", "REGULAR_SEASON")}, meta=invalid
                ),
            )

    def test_stale_authority_fails_closed(self) -> None:
        stale = metadata(supported_through_date="2026-09-21")
        with self.assertRaisesRegex(TotalsPhaseGateError, "PHASE_AUTHORITY_STALE"):
            classify_totals_row(
                row(1), authority=StubAuthority(
                    {1: record(1, "R", "REGULAR_SEASON")}, meta=stale
                ),
            )

    def test_exact_game_pk_and_source_type_conflicts_fail_closed(self) -> None:
        authority = StubAuthority({1: record(1, "R", "REGULAR_SEASON")})
        with self.assertRaisesRegex(TotalsPhaseGateError, "EXACT_GAME_PK_MISMATCH"):
            classify_totals_row(row(1, game_id=2), authority=authority)
        with self.assertRaisesRegex(TotalsPhaseGateError, "SOURCE_TYPE_CONFLICT"):
            classify_totals_row(row(1, source_game_type="W"), authority=authority)
        with self.assertRaisesRegex(TotalsPhaseGateError, "GAME_PK_MISSING"):
            classify_totals_row(row(1.5), authority=authority)

    def test_duplicate_evaluation_identity_fails_closed(self) -> None:
        authority = StubAuthority({1: record(1, "R", "REGULAR_SEASON")})
        with self.assertRaisesRegex(TotalsPhaseGateError, "DUPLICATE_EVALUATION_IDENTITY"):
            partition_totals_rows(
                [row(1), row(1)], authority=authority,
                unique_identity_fields=("canonical_identity",),
            )

    def test_schedule_requires_exact_statsapi_game_type(self) -> None:
        payload = {"dates": [{"date": "2026-09-22", "games": [{
            "gamePk": 1, "gameDate": "2026-09-22T20:00:00Z",
            "teams": {"away": {"team": {"id": 1}}, "home": {"team": {"id": 2}}},
        }]}]}
        with self.assertRaisesRegex(TotalsLiveContextError, "AUTHORITATIVE_GAME_TYPE_REQUIRED"):
            normalize_schedule(payload, "2026-09-22T12:00:00Z", "f" * 64)
        payload["dates"][0]["games"][0]["gameType"] = "W"
        normalized = normalize_schedule(payload, "2026-09-22T12:00:00Z", "f" * 64)
        self.assertEqual(normalized[0]["source_game_type"], "W")

    def test_raw_to_c_exact_gamepk_and_phase_consistency(self) -> None:
        authority = StubAuthority({
            1: record(1, "R", "REGULAR_SEASON"),
            2: record(2, "W", "POSTSEASON", "WORLD_SERIES"),
        })
        decisions = require_raw_c_consistency(
            [row(1), row(2)],
            [row(1, source_raw_identity="raw-1"), row(2, source_raw_identity="raw-2")],
            authority=authority,
        )
        self.assertEqual(decisions[1].normalized_phase, "REGULAR_SEASON")
        self.assertEqual(decisions[2].normalized_phase, "POSTSEASON")
        with self.assertRaisesRegex(TotalsPhaseGateError, "RAW_PARENT_MISSING"):
            require_raw_c_consistency([row(1)], [row(2)], authority=authority)

    def test_regular_and_postseason_metrics_are_disjoint(self) -> None:
        authority = StubAuthority({
            1: record(1, "R", "REGULAR_SEASON"),
            2: record(2, "D", "POSTSEASON", "DIVISION_SERIES"),
        })
        partitions = partition_totals_rows([row(1), row(2)], authority=authority)
        self.assertEqual([item["game_pk"] for item in partitions.selected("REGULAR_SEASON")], [1])
        self.assertEqual([item["game_pk"] for item in partitions.selected("POSTSEASON")], [2])
        self.assertEqual(
            inspect.signature(raw_grade.run).parameters["evaluation_phase"].default,
            "REGULAR_SEASON",
        )
        self.assertEqual(
            inspect.signature(c_daily.cluster_counts).parameters["evaluation_phase"].default,
            "REGULAR_SEASON",
        )

    def test_raw_grader_metrics_and_c_clusters_are_phase_separate(self) -> None:
        authority = StubAuthority({
            1: record(1, "R", "REGULAR_SEASON"),
            2: record(2, "W", "POSTSEASON", "WORLD_SERIES"),
        })
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw_path, c_path, market_path = root / "raw.sqlite3", root / "c.sqlite3", root / "market.sqlite3"
            raw_connection = raw_ledger.connect_ledger(raw_path)
            for game_pk, expected in ((1, 8.0), (2, 12.0)):
                prediction = {
                    "game_date": "2026-09-22", "game_pk": game_pk,
                    "scheduled_start_utc": "2026-09-22T23:00:00Z",
                    "prediction_timestamp_utc": "2026-09-22T12:00:00Z",
                    "model_hash": "a" * 64, "feature_state_hash": "b" * 64,
                    "schedule_source_sha256": "c" * 64,
                    "away_team": "Away", "home_team": "Home",
                    "model_version": "SYNTHETIC", "expected_total": expected,
                }
                raw_ledger.append_prediction(raw_connection, prediction)
            with mock.patch.object(raw_grade, "official_final", side_effect=lambda _date, game_pk: {
                "official_final_total": 9 if game_pk == 1 else 10,
                "regulation_nine_total": 9 if game_pk == 1 else 10,
                "official_source_path": f"synthetic_{game_pk}.json",
                "official_source_hash": "f" * 64,
                "official_status": "Final",
            }), mock.patch.object(raw_grade, "selected_market_rows", return_value=([], [])):
                regular = raw_grade.run(
                    "2026-09-22", root / "reports", raw_path, market_path,
                    evaluation_phase="REGULAR_SEASON", phase_authority=authority,
                )
                postseason = raw_grade.run(
                    "2026-09-22", root / "reports", raw_path, market_path,
                    evaluation_phase="POSTSEASON", phase_authority=authority,
                )
            self.assertEqual(regular["frozen_predictions"], 1)
            self.assertEqual(postseason["frozen_predictions"], 1)
            self.assertEqual(regular["model_mae_final"], 1.0)
            self.assertEqual(postseason["model_mae_final"], 2.0)

            connection = c_ledger.connect_ledger(c_path)
            for game_pk in (1, 2):
                context = {"synthetic": game_pk}
                prediction = {
                    "game_date": "2026-09-22", "game_pk": game_pk,
                    "scheduled_start_utc": "2026-09-22T23:00:00Z",
                    "prediction_timestamp_utc": "2026-09-22T12:00:00Z",
                    "source_raw_identity": raw_ledger.canonical_identity("2026-09-22", game_pk),
                    "feature_state_hash": c_ledger.payload_hash(context),
                    "artifact_sha256": "d" * 64,
                }
                c_ledger.append_prediction_with_context(connection, prediction, context)
                c_ledger.append_outcome(connection, c_ledger.canonical_identity("2026-09-22", game_pk), {
                    "official_final_total": 9, "regulation_nine_total": 9,
                    "official_source_hash": "e" * 64,
                }, "2026-09-23T12:00:00Z")
            regular_clusters = c_daily.cluster_counts(
                connection, evaluation_phase="REGULAR_SEASON", phase_authority=authority
            )
            postseason_clusters = c_daily.cluster_counts(
                connection, evaluation_phase="POSTSEASON", phase_authority=authority
            )
            self.assertEqual(regular_clusters["phase_partition_counts"]["REGULAR_SEASON"], 1)
            self.assertEqual(postseason_clusters["phase_partition_counts"]["POSTSEASON"], 1)
            self.assertEqual(regular_clusters["completed_date_clusters"], 1)
            self.assertEqual(postseason_clusters["completed_date_clusters"], 1)

    def test_zero_calendar_reconstruction(self) -> None:
        authority = StubAuthority({
            1: record(1, "W", "POSTSEASON", "WORLD_SERIES"),
            2: record(2, "R", "REGULAR_SEASON"),
        })
        self.assertEqual(
            classify_totals_row(row(1, "2026-04-01"), authority=authority).normalized_phase,
            "POSTSEASON",
        )
        self.assertEqual(
            classify_totals_row(row(2, "2026-11-01"), authority=authority).normalized_phase,
            "REGULAR_SEASON",
        )

    def test_hash_invariance_and_repeated_execution(self) -> None:
        rows = [row(1), row(2, expected_total=7.75)]
        authority = StubAuthority({
            1: record(1, "R", "REGULAR_SEASON"),
            2: record(2, "R", "REGULAR_SEASON"),
        })
        before = canonical_rows_sha256(rows)
        first = partition_totals_rows(rows, authority=authority)
        second = partition_totals_rows(rows, authority=authority)
        self.assertEqual(first, second)
        self.assertEqual(canonical_rows_sha256(first.regular_season), before)

    def test_scoring_gates_precede_feature_aggregation_or_fitting(self) -> None:
        raw_source = inspect.getsource(raw_score.run)
        self.assertLess(raw_source.index("partition_totals_rows"), raw_source.index("attach_context"))
        c_source = inspect.getsource(c_score.score_from_raw)
        self.assertLess(c_source.index("partition_totals_rows"), c_source.index("structural.score"))
        self.assertNotIn(".fit(", raw_source)
        self.assertNotIn(".fit(", c_source)

    def test_no_missing_type_fallback_or_date_phase_logic_in_bounded_modules(self) -> None:
        modules = (raw_score, c_score, raw_grade, c_daily)
        forbidden = ('or "R"', "or 'R'", 'fillna("R")', "fillna('R')", "COALESCE")
        for module in modules:
            source = inspect.getsource(module)
            for token in forbidden:
                self.assertNotIn(token, source)
        helper_source = inspect.getsource(classify_totals_row)
        self.assertNotIn("month", helper_source.lower())
        self.assertNotIn("postseason_start", helper_source.lower())

    def test_governance_and_thresholds_are_unchanged(self) -> None:
        self.assertEqual(raw_score.THRESHOLDS, (6.5, 7.5, 8.5, 9.5, 10.5, 11.5))
        self.assertEqual(c_score.THRESHOLDS, (6.5, 7.5, 8.5, 9.5, 10.5, 11.5))
        self.assertIn("SHADOW_ONLY_NOT_PUBLIC", inspect.getsource(raw_score.run))
        self.assertIn("PRIVATE_SHADOW_ONLY_NOT_PUBLIC", inspect.getsource(c_score.score_from_raw))
        for function in (classify_totals_row, raw_grade.run, c_daily.cluster_counts):
            source = inspect.getsource(function).lower()
            self.assertNotIn("quick_card", source)
            self.assertNotIn("selector", source)


if __name__ == "__main__":
    unittest.main()
