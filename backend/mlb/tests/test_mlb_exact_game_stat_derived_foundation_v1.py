from __future__ import annotations

import hashlib
import json
from dataclasses import FrozenInstanceError
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.mlb.exact_game_features.adapters_v1 import (
    ExactGameFactRecordV1,
    ExactGameFeatureRecordV1,
    LegacyDailyAggregateRecordV1,
)
from backend.mlb.exact_game_features.contract_v1 import (
    CONTRACT_VERSION,
    EXACT_GAME_STRICT_PRIOR_FEATURE_STATE_PROVABLE,
    HISTORICAL_EXACT_GAME_FEATURE_STATE_UNPROVABLE,
    LEGACY_PLAYER_DERIVED_GRAIN,
    ExactGameAuthorityV1,
    ExactGameContractError,
    ExactGameFeatureStateV1,
    SourceObservationV1,
    coalesce_exact_states,
)
from backend.mlb.exact_game_features.offline_builder_v1 import (
    ExactGameTargetV1,
    HistoricalExactGameFactV1,
    OfflineExactGameCandidateBuilderV1,
)
from backend.mlb.identity import playable_terminal_v1 as shared
from backend.mlb.public_game_predictions import finality_v1 as moneyline
from backend.mlb.season_transition.game_phase_authority_v1 import GamePhaseAuthorityError


ROOT = Path(__file__).resolve().parents[3]
FIXTURE = ROOT / "backend/mlb/tests/fixtures/exact_game_stat_derived_v1/retained_and_synthetic_cases.json"
MIGRATION = ROOT / "backend/mlb/sql/migrations/20260924_prepare_player_game_feature_state_v1.sql"
ROLLBACK = ROOT / "backend/mlb/sql/migrations/20260924_rollback_player_game_feature_state_v1.sql"
CONSUMERS = ROOT / "docs/contracts/mlb_2026_exact_game_stat_derived_foundation_v1/consumer_cutover_classification.csv"
SHA = "a" * 64


class StubPhaseAuthority:
    def __init__(self, game_pks: list[int]) -> None:
        self.metadata = SimpleNamespace(
            authority_interface="MLB_CANONICAL_GAME_PHASE_AUTHORITY_V1",
            snapshot_descriptor_sha256="b" * 64,
            authority_records_sha256="c" * 64,
            snapshot_id="snapshot-v1",
            proposal_sha256="d" * 64,
        )
        self._rows = {
            game_pk: SimpleNamespace(
                game_pk=game_pk,
                source_season=2026,
                source_game_type="R",
                season_phase="REGULAR_SEASON",
                schedule_relationships={},
                source_paths=(f"fixtures/{game_pk}.json",),
                source_hashes=(SHA,),
            )
            for game_pk in game_pks
        }

    def lookup_exact(self, game_pk: int):
        if int(game_pk) not in self._rows:
            raise GamePhaseAuthorityError("GAME_PHASE_EVIDENCE_MISSING", game_pk=int(game_pk))
        return self._rows[int(game_pk)]


def fixture() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def source(observed_at: str = "2026-07-01T20:00:00Z") -> SourceObservationV1:
    return SourceObservationV1.create(
        source_path="fixtures/retained.json",
        source_sha256=SHA,
        observed_at_utc=observed_at,
        source_kind="RETAINED_TEST_FIXTURE",
    )


def authority(game_pk: int, start: str, official_date: str = "2026-07-01") -> ExactGameAuthorityV1:
    return ExactGameAuthorityV1.from_canonical_interface(
        StubPhaseAuthority([game_pk]),
        game_pk=game_pk,
        official_date=official_date,
        scheduled_start_utc=start,
    )


def fact(
    game_pk: int,
    *,
    start: str,
    terminal: str | None,
    payload: dict | None = None,
    observed: str = "2026-07-01T20:00:00Z",
) -> HistoricalExactGameFactV1:
    return HistoricalExactGameFactV1.create(
        player_id=10,
        authority=authority(game_pk, start),
        terminal_observed_at_utc=terminal,
        source_observations=[source(observed)],
        fact_payload=payload or {"hits": 1},
    )


def test_retained_fixture_hashes_are_exact_and_offline():
    for item in fixture()["retained_sources"].values():
        path = ROOT / item["path"]
        assert path.is_file()
        assert hashlib.sha256(path.read_bytes()).hexdigest() == item["sha256"]


@pytest.mark.parametrize(
    "fields",
    [
        {"abstractGameState": "Final", "detailedState": "Final", "codedGameState": "F", "statusCode": "F"},
        {"abstractGameState": "Final", "detailedState": "Game Over", "codedGameState": "O", "statusCode": "O"},
        {"abstractGameState": "Final", "detailedState": "Postponed", "codedGameState": "D", "statusCode": "DR"},
        {"abstractGameState": "Final", "detailedState": "Suspended", "codedGameState": "D", "statusCode": "D"},
        {"abstractGameState": "Preview", "detailedState": "Scheduled", "codedGameState": "S", "statusCode": "S"},
        {"abstractGameState": "Live", "detailedState": "In Progress", "codedGameState": "I", "statusCode": "I"},
        {"abstractGameState": "Final", "detailedState": "Final", "codedGameState": "O", "statusCode": "O"},
        {"abstractGameState": "Final"},
    ],
)
def test_shared_terminal_classifier_has_moneyline_parity(fields):
    legacy = moneyline.classify_playable_terminal(fields)
    candidate = shared.classify_playable_terminal(fields)
    assert (candidate.classification, candidate.reason, dict(candidate.fields)) == (
        legacy.classification,
        legacy.reason,
        legacy.fields,
    )


def test_shared_schedule_reconciliation_has_moneyline_parity_on_retained_cases():
    payload = fixture()["retained_schedule_payload"]
    legacy = moneyline.reconcile_schedule_by_game_pk(payload)
    candidate = shared.reconcile_schedule_by_game_pk(payload)
    assert [
        (row.game_pk, row.decision, row.reason, None if row.selected is None else row.selected.source_order)
        for row in candidate
    ] == [
        (row.game_pk, row.decision, row.reason, None if row.selected is None else row.selected.source_order)
        for row in legacy
    ]


def test_824785_and_824784_remain_two_exact_identities_without_date_collapse():
    decisions = shared.reconcile_schedule_by_game_pk(fixture()["retained_schedule_payload"])
    by_game = {row.game_pk: row for row in decisions}
    assert by_game[824785].decision == "FETCH_PLAYABLE_FINAL"
    assert by_game[824785].selected is not None
    assert by_game[824785].selected.operational_date == "2026-09-23"
    assert by_game[824784].decision == "FETCH_PLAYABLE_FINAL"
    assert by_game[824784].selected is not None
    assert by_game[824784].selected.game_number == 2
    assert set(by_game).issuperset({824785, 824784})


def test_ordinary_doubleheader_remains_two_exact_identities():
    decisions = shared.reconcile_schedule_by_game_pk(fixture()["retained_schedule_payload"])
    by_game = {row.game_pk: row for row in decisions}
    assert by_game[824459].decision == "FETCH_PLAYABLE_FINAL"
    assert by_game[824460].decision == "FETCH_PLAYABLE_FINAL"
    assert by_game[824459].selected is not None and by_game[824459].selected.game_number == 1
    assert by_game[824460].selected is not None and by_game[824460].selected.game_number == 2


def test_suspended_resumed_identity_remains_one_game_pk():
    decision = {
        row.game_pk: row
        for row in shared.reconcile_schedule_by_game_pk(fixture()["retained_schedule_payload"])
    }[824912]
    assert decision.decision == "FETCH_PLAYABLE_FINAL"
    assert len(decision.appearances) == 2
    assert {row.game_pk for row in decision.appearances} == {824912}
    assert decision.selected is not None and decision.selected.operational_date == "2026-06-16"


def test_conflicting_reschedule_relationship_fails_closed():
    games = [
        {
            "gamePk": 7,
            "gameDate": "2026-07-01T17:00:00Z",
            "officialDate": "2026-07-01",
            "status": {"abstractGameState": "Final", "detailedState": "Postponed", "codedGameState": "D", "statusCode": "DR"},
            "rescheduleDate": "2026-07-02T17:00:00Z",
            "teams": {"away": {"team": {"id": 1}}, "home": {"team": {"id": 2}}},
        },
        {
            "gamePk": 7,
            "gameDate": "2026-07-03T17:00:00Z",
            "officialDate": "2026-07-03",
            "status": {"abstractGameState": "Final", "detailedState": "Final", "codedGameState": "F", "statusCode": "F"},
            "rescheduledFrom": "2026-06-30T17:00:00Z",
            "teams": {"away": {"team": {"id": 1}}, "home": {"team": {"id": 2}}},
        },
    ]
    decision = shared.reconcile_schedule_by_game_pk({"dates": [{"games": games}]})[0]
    assert decision.decision == "QUARANTINED"
    assert decision.reason == "CONFLICTING_REPEATED_SCHEDULE_APPEARANCES"


def test_exact_state_rejects_missing_cutoff_post_cutoff_sources_and_date_identity():
    auth = authority(101, "2026-07-01T23:00:00Z")
    with pytest.raises(ExactGameContractError, match="FEATURE_INPUT_CUTOFF_MISSING"):
        ExactGameFeatureStateV1.create(
            player_id=10,
            authority=auth,
            feature_input_cutoff_utc=None,
            source_observations=[source()],
            feature_payload={},
        )
    with pytest.raises(ExactGameContractError, match="POST_CUTOFF_SOURCE_OBSERVATION"):
        ExactGameFeatureStateV1.create(
            player_id=10,
            authority=auth,
            feature_input_cutoff_utc="2026-07-01T19:00:00Z",
            source_observations=[source("2026-07-01T20:00:00Z")],
            feature_payload={},
        )
    with pytest.raises(ExactGameContractError, match="EXACT_GAME_PK_MISSING"):
        ExactGameAuthorityV1.from_canonical_interface(
            StubPhaseAuthority([101]),
            game_pk=None,
            official_date="2026-07-01",
            scheduled_start_utc="2026-07-01T23:00:00Z",
        )
    with pytest.raises(ExactGameContractError, match="CANONICAL_PHASE_AUTHORITY_REJECTED"):
        ExactGameAuthorityV1.from_canonical_interface(
            StubPhaseAuthority([]),
            game_pk=101,
            official_date="2026-07-01",
            scheduled_start_utc="2026-07-01T23:00:00Z",
        )

    conflicting = StubPhaseAuthority([101])
    conflicting._rows[101].game_pk = 999
    with pytest.raises(ExactGameContractError, match="CANONICAL_PHASE_AUTHORITY_CONFLICT"):
        ExactGameAuthorityV1.from_canonical_interface(
            conflicting,
            game_pk=101,
            official_date="2026-07-01",
            scheduled_start_utc="2026-07-01T23:00:00Z",
        )


def test_exact_state_is_immutable_deterministic_and_idempotent():
    kwargs = dict(
        player_id=10,
        authority=authority(101, "2026-07-01T23:00:00Z"),
        feature_input_cutoff_utc="2026-07-01T22:30:00Z",
        source_observations=[source()],
        feature_payload={"prior_games": [100], "rolling_hits": 1.0},
    )
    first = ExactGameFeatureStateV1.create(**kwargs)
    second = ExactGameFeatureStateV1.create(**kwargs)
    assert first.row_sha256 == second.row_sha256
    assert coalesce_exact_states([first, second]) == (first,)
    with pytest.raises(FrozenInstanceError):
        first.player_id = 11  # type: ignore[misc]
    conflict = ExactGameFeatureStateV1.create(**(kwargs | {"feature_payload": {"rolling_hits": 2.0}}))
    with pytest.raises(ExactGameContractError, match="DUPLICATE_EXACT_IDENTITY_CONFLICTING_PAYLOAD"):
        coalesce_exact_states([first, conflict])


def test_game_one_is_admitted_only_when_terminal_observation_predates_cutoff():
    builder = OfflineExactGameCandidateBuilderV1()
    target = ExactGameTargetV1.create(
        player_id=10,
        authority=authority(202, "2026-07-01T23:00:00Z"),
        feature_input_cutoff_utc="2026-07-01T22:30:00Z",
    )
    before = fact(201, start="2026-07-01T17:00:00Z", terminal="2026-07-01T20:00:00Z")
    proposal = builder.build(facts=[before], targets=[target])[0]
    assert proposal.classification == EXACT_GAME_STRICT_PRIOR_FEATURE_STATE_PROVABLE
    assert proposal.eligible_prior_game_pks == (201,)

    after = fact(
        201,
        start="2026-07-01T17:00:00Z",
        terminal="2026-07-01T22:45:00Z",
        observed="2026-07-01T22:45:00Z",
    )
    proposal = builder.build(facts=[after], targets=[target])[0]
    assert proposal.eligible_prior_game_pks == ()
    assert proposal.excluded_prior_games == ((201, "TERMINAL_OBSERVED_AT_OR_AFTER_CUTOFF"),)


def test_overlapping_game_and_missing_terminal_proof_are_excluded():
    builder = OfflineExactGameCandidateBuilderV1()
    target = ExactGameTargetV1.create(
        player_id=10,
        authority=authority(302, "2026-07-01T23:00:00Z"),
        feature_input_cutoff_utc="2026-07-01T22:30:00Z",
    )
    overlapping = fact(301, start="2026-07-01T23:30:00Z", terminal="2026-07-02T02:00:00Z")
    missing = fact(300, start="2026-06-30T17:00:00Z", terminal=None)
    proposal = builder.build(facts=[overlapping, missing], targets=[target])[0]
    assert proposal.eligible_prior_game_pks == ()
    assert set(proposal.excluded_prior_games) == {
        (300, "TERMINAL_OBSERVATION_TIME_UNPROVEN"),
        (301, "EVENT_CHRONOLOGY_NOT_STRICTLY_PRIOR"),
    }


def test_missing_historical_cutoff_is_never_reconstructed():
    targets = [
        ExactGameTargetV1.create(
            player_id=10,
            authority=authority(game_pk, start, "2026-09-23"),
            feature_input_cutoff_utc=None,
        )
        for game_pk, start in (
            (824785, "2026-09-23T17:35:00Z"),
            (824784, "2026-09-23T22:35:00Z"),
        )
    ]
    proposals = OfflineExactGameCandidateBuilderV1().build(facts=[], targets=targets)
    assert {row.game_pk for row in proposals} == {824785, 824784}
    assert all(
        row.classification == HISTORICAL_EXACT_GAME_FEATURE_STATE_UNPROVABLE
        and row.reason == "IMMUTABLE_FEATURE_INPUT_CUTOFF_NOT_RETAINED"
        for row in proposals
    )


def test_retained_824785_legacy_population_is_declared_zero_mutation():
    preservation = fixture()["legacy_preservation"]
    assert preservation == {
        "game_pk": 824785,
        "player_stats_rows": 49,
        "player_derived_stats_rows": 49,
        "model_training_props_rows": 188,
        "expected_mutations": 0,
    }


def test_repeated_build_and_matching_fact_are_idempotent_but_conflict_fails():
    builder = OfflineExactGameCandidateBuilderV1()
    retained = fact(401, start="2026-07-01T17:00:00Z", terminal="2026-07-01T20:00:00Z")
    target = ExactGameTargetV1.create(
        player_id=10,
        authority=authority(402, "2026-07-01T23:00:00Z"),
        feature_input_cutoff_utc="2026-07-01T22:30:00Z",
    )
    first = builder.build(facts=[retained, retained], targets=[target])
    second = builder.build(facts=[retained], targets=[target, target])
    assert first == second
    conflict = fact(
        401,
        start="2026-07-01T17:00:00Z",
        terminal="2026-07-01T20:00:00Z",
        payload={"hits": 2},
    )
    with pytest.raises(ExactGameContractError, match="DUPLICATE_EXACT_IDENTITY_CONFLICTING_PAYLOAD"):
        builder.build(facts=[retained, conflict], targets=[target])


def test_migration_is_additive_append_only_and_rollback_is_scoped():
    migration = MIGRATION.read_text(encoding="utf-8")
    rollback = ROLLBACK.read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS mlb.player_game_feature_state_v1" in migration
    assert "PRIMARY KEY (player_id, game_pk, contract_version)" in migration
    assert "BEFORE UPDATE OR DELETE" in migration
    assert "feature_input_cutoff_utc <= scheduled_start_utc" in migration
    assert "latest_source_observed_at_utc <= feature_input_cutoff_utc" in migration
    assert "ALTER TABLE mlb.player_derived_stats" not in migration
    assert "UPDATE mlb." not in migration and "DELETE FROM mlb." not in migration
    assert "DROP TABLE IF EXISTS mlb.player_game_feature_state_v1" in rollback
    assert "player_derived_stats" not in rollback
    assert "model_training_props" not in rollback
    assert "player_stats" not in rollback


def test_adapters_are_nominally_incompatible_and_no_fallback_exists():
    assert ExactGameFactRecordV1 is not ExactGameFeatureRecordV1
    assert ExactGameFeatureRecordV1 is not LegacyDailyAggregateRecordV1
    assert "game_pk" not in LegacyDailyAggregateRecordV1.__dataclass_fields__
    assert "game_date" not in ExactGameFeatureRecordV1.__dataclass_fields__
    assert LEGACY_PLAYER_DERIVED_GRAIN == "PLAYER_DATE_POSTGAME_AGGREGATE_WITH_MIXED_GAME_ID_SEMANTICS"


def test_consumer_cutover_contract_covers_exactly_55_consumers():
    import csv

    with CONSUMERS.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 55
    allowed = {
        "LEGACY_DAILY_COMPATIBLE",
        "EXACT_GAME_REQUIRED",
        "NOT_APPLICABLE",
        "BLOCKED_UNPROVEN_LINEAGE",
    }
    assert {row["cutover_classification"] for row in rows}.issubset(allowed)
    assert len({row["consumer_path"] for row in rows}) == 55


def test_foundation_contains_no_db_network_or_max_game_id_execution_path():
    package = ROOT / "backend/mlb/exact_game_features"
    text = "\n".join(path.read_text(encoding="utf-8") for path in sorted(package.glob("*.py")))
    assert "requests" not in text
    assert "psycopg" not in text
    assert "sqlalchemy" not in text
    assert "MAX(game_id)" not in text
    assert "supabase" not in text.casefold()
