from __future__ import annotations

import json
import gzip
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

import pytest

from backend.mlb.scripts.build_mlb_stat_derived_root_loader_proposal_v1 import build
from backend.mlb.stat_derived_exact_game_loader_v1 import (
    AUTHORIZATION_VERSION,
    BLOCK_CONFLICTING_PAYLOAD,
    INSERT_NEW_EXACT_FACT,
    MATCH_EXISTING_EXACT_FACT,
    NO_ACTION,
    QUARANTINE_CONFLICTING_LEGACY_IDENTITY,
    RELOCATE_MATCHING_MISDATED_FACT,
    UNPROVABLE_FAIL_CLOSED,
    AcceptedGameV1,
    ExactGameMutationPlannerV1,
    LoaderContractError,
    MutationAuthorizationV1,
    MutationIntentV1,
    OrderedMutationExecutorV1,
    SourceEvidenceV1,
    accepted_games_from_retained_schedule,
    assert_no_legacy_derived_write,
    canonical_json_bytes,
    content_sha256,
    strict_prior_admission,
    validate_terminal_feed,
)


ROOT = Path(__file__).resolve().parents[3]
FIXTURE = ROOT / "backend/mlb/tests/fixtures/exact_game_stat_derived_v1/retained_and_synthetic_cases.json"
MODULE = ROOT / "backend/mlb/stat_derived_exact_game_loader_v1.py"
SCRIPT = ROOT / "backend/mlb/scripts/build_mlb_stat_derived_root_loader_proposal_v1.py"
SHA = "a" * 64


def fixture() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def source(at: str = "2026-07-01T20:00:00Z") -> SourceEvidenceV1:
    return SourceEvidenceV1("fixtures/source.json", SHA, at, "TEST")


def accepted(game_pk: int, start: str, official_date: str = "2026-07-01") -> AcceptedGameV1:
    return AcceptedGameV1(
        game_pk=game_pk,
        operational_date=official_date,
        scheduled_start_utc=start,
        game_number=1,
        double_header="N",
        team_ids=(1, 2),
        relationship_evidence={},
        status_fields={
            "abstract_game_state": "Final",
            "detailed_state": "Final",
            "coded_game_state": "F",
            "status_code": "F",
        },
        source_evidence=(source(),),
        phase_authority_interface="MLB_CANONICAL_GAME_PHASE_AUTHORITY_V1",
        phase_authority_descriptor_sha256="b" * 64,
        phase="REGULAR_SEASON",
    )


def intent(
    *,
    relation: str = "player_stats",
    player_id: int = 10,
    game_pk: int = 1,
    current: dict | None = None,
    proposed: dict | None = None,
    force: str | None = None,
) -> MutationIntentV1:
    return MutationIntentV1(
        relation=relation,
        exact_key={"player_id": player_id, "game_id": game_pk},
        authoritative_game_pk=game_pk,
        accepted_operational_date="2026-07-01",
        source_evidence=(source(),),
        relationship_evidence={},
        reason="TEST",
        rollback_identity={"player_id": player_id, "game_id": game_pk},
        current_value=current,
        proposed_value=proposed,
        force_operation=force,
    )


def test_postponed_makeup_and_sibling_resolve_by_exact_game_pk():
    accepted_games = accepted_games_from_retained_schedule(
        fixture()["retained_schedule_payload"],
        schedule_sources=(source(),),
        phase_by_game_pk={824785: "REGULAR_SEASON", 824784: "REGULAR_SEASON", 824912: "REGULAR_SEASON", 824459: "REGULAR_SEASON", 824460: "REGULAR_SEASON"},
        phase_authority_descriptor_sha256="b" * 64,
    )
    by_game = {row.game_pk: row for row in accepted_games}
    assert by_game[824785].operational_date == "2026-09-23"
    assert by_game[824785].game_pk == 824785
    assert by_game[824784].operational_date == "2026-09-23"
    assert by_game[824784].game_pk == 824784
    assert by_game[824784].game_number == 2
    assert by_game[824912].operational_date == "2026-06-16"


def test_final_postponed_is_rejected_and_terminal_feed_must_match_exact_identity():
    payload = fixture()["retained_schedule_payload"]
    game = next(row for row in accepted_games_from_retained_schedule(
        payload,
        schedule_sources=(source(),),
        phase_by_game_pk={824785: "REGULAR_SEASON", 824784: "REGULAR_SEASON", 824912: "REGULAR_SEASON", 824459: "REGULAR_SEASON", 824460: "REGULAR_SEASON"},
        phase_authority_descriptor_sha256="b" * 64,
    ) if row.game_pk == 824785)
    feed = {
        "gamePk": 824785,
        "gameData": {
            "status": {"abstractGameState": "Final", "detailedState": "Final", "codedGameState": "F", "statusCode": "F"},
            "datetime": {"officialDate": "2026-09-23"},
        },
    }
    validate_terminal_feed(game, feed, source())
    feed["gameData"]["status"] = {"abstractGameState": "Final", "detailedState": "Postponed", "codedGameState": "D", "statusCode": "DR"}
    with pytest.raises(LoaderContractError, match="TERMINAL_FEED_NOT_PLAYABLE"):
        validate_terminal_feed(game, feed, source())


def test_relation_grain_keeps_game_one_game_two_and_same_stat_type_separate():
    intents = [
        intent(game_pk=100, proposed={"hits": 1}),
        intent(game_pk=101, proposed={"hits": 2}),
        intent(player_id=11, game_pk=100, proposed={"hits": 0}),
        intent(player_id=12, game_pk=101, proposed={"hits": 1}),
        intent(relation="model_training_props", game_pk=100, proposed={"prop_type": "hits", "value": 1}),
        intent(relation="model_training_props", game_pk=101, proposed={"prop_type": "hits", "value": 2}),
    ]
    plan = ExactGameMutationPlannerV1().build(intents)
    assert len(plan.proposals) == 6
    assert {row.authoritative_game_pk for row in plan.proposals} == {100, 101}
    assert all(row.operation == INSERT_NEW_EXACT_FACT for row in plan.proposals)


def test_retained_824785_824784_proposal_is_complete_and_keeps_player_populations_separate(tmp_path):
    summary = build(tmp_path)
    assert summary["proposal_rows"] == 589
    assert summary["operation_counts"] == {
        INSERT_NEW_EXACT_FACT: 207,
        QUARANTINE_CONFLICTING_LEGACY_IDENTITY: 49,
        RELOCATE_MATCHING_MISDATED_FACT: 238,
        UNPROVABLE_FAIL_CLOSED: 95,
    }
    assert summary["game_824785"] == {
        "accepted_operational_date": "2026-09-23",
        "player_stats_relocations": 49,
        "training_relocations": 188,
        "game_info_relocations": 1,
        "legacy_derived_quarantines": 49,
        "feature_states_blocked_missing_cutoff": 49,
    }
    assert summary["game_824784"] == {
        "accepted_operational_date": "2026-09-23",
        "game_info_inserts": 1,
        "player_stats_inserts": 46,
        "training_inserts": 160,
        "legacy_derived_writes": 0,
        "feature_states_blocked_missing_cutoff": 46,
    }
    assert summary["player_population"] == {
        "824785": 49,
        "824784": 46,
        "appeared_in_both": 40,
        "824785_only": 9,
        "824784_only": 6,
    }
    with gzip.open(tmp_path / "offline_mutation_proposal.jsonl.gz", "rt", encoding="utf-8") as handle:
        rows = [json.loads(line) for line in handle]
    assert {row["authoritative_game_pk"] for row in rows} == {824784, 824785}
    assert all(row["accepted_operational_date"] == "2026-09-23" for row in rows)
    assert not any(
        row["relation"] == "player_derived_stats"
        and row["operation"] in {INSERT_NEW_EXACT_FACT, RELOCATE_MATCHING_MISDATED_FACT}
        for row in rows
    )


def test_planner_classifies_insert_match_relocation_conflict_quarantine_and_unprovable():
    old = {"game_date": "2026-09-22", "hits": 1}
    same = dict(old)
    moved = {"game_date": "2026-09-23", "hits": 1}
    changed = {"game_date": "2026-09-23", "hits": 2}
    intents = [
        intent(player_id=1, proposed={"hits": 1}),
        intent(player_id=2, current=old, proposed=same),
        MutationIntentV1(**(intent(player_id=3, current=old, proposed=moved).__dict__ | {
            "current_substantive_hash": content_sha256({"hits": 1}),
            "proposed_substantive_hash": content_sha256({"hits": 1}),
        })),
        intent(player_id=4, current=old, proposed=changed),
        intent(relation="player_derived_stats", player_id=5, current=old, proposed=None),
        intent(relation="player_game_feature_state_v1", player_id=6, current=None, proposed=None),
    ]
    plan = ExactGameMutationPlannerV1().build(intents)
    assert {row.operation for row in plan.proposals} == {
        INSERT_NEW_EXACT_FACT,
        MATCH_EXISTING_EXACT_FACT,
        RELOCATE_MATCHING_MISDATED_FACT,
        BLOCK_CONFLICTING_PAYLOAD,
        QUARANTINE_CONFLICTING_LEGACY_IDENTITY,
        UNPROVABLE_FAIL_CLOSED,
    }
    assert_no_legacy_derived_write(plan)


@pytest.mark.parametrize(
    ("terminal", "cutoff", "source_times", "expected", "reason"),
    [
        ("2026-07-01T20:00:00Z", "2026-07-01T22:30:00Z", ["2026-07-01T20:00:00Z"], True, "STRICT_PRIOR_TERMINAL_AND_SOURCE_OBSERVATIONS_PROVEN"),
        ("2026-07-01T22:45:00Z", "2026-07-01T22:30:00Z", ["2026-07-01T22:45:00Z"], False, "TERMINAL_OBSERVED_AT_OR_AFTER_CUTOFF"),
        (None, "2026-07-01T22:30:00Z", ["2026-07-01T20:00:00Z"], False, "TERMINAL_OBSERVATION_TIME_UNPROVEN"),
        ("2026-07-01T20:00:00Z", None, ["2026-07-01T20:00:00Z"], False, "IMMUTABLE_FEATURE_INPUT_CUTOFF_NOT_RETAINED"),
        ("2026-07-01T20:00:00Z", "2026-07-01T22:30:00Z", ["2026-07-01T22:40:00Z"], False, "SOURCE_OBSERVED_POST_CUTOFF"),
    ],
)
def test_strict_prior_cutoff_for_doubleheader_and_overlap(terminal, cutoff, source_times, expected, reason):
    result = strict_prior_admission(
        prior_game=accepted(100, "2026-07-01T17:00:00Z"),
        target_game=accepted(101, "2026-07-01T23:00:00Z"),
        prior_terminal_observed_at_utc=terminal,
        target_feature_cutoff_utc=cutoff,
        prior_source_observed_at_utc=source_times,
    )
    assert result.admitted is expected
    assert result.reason == reason


class FakeBackend:
    def __init__(self, state: dict | None = None, *, fail_at: str | None = None) -> None:
        self.state = deepcopy(state or {})
        self.before = deepcopy(self.state)
        self.events: list[str] = []
        self.fail_at = fail_at
        self.completed: dict[str, str] = {}
        self.constraints = True

    @staticmethod
    def identity(relation, exact_key):
        return relation, canonical_json_bytes(dict(exact_key))

    def hit(self, name):
        self.events.append(name)
        if self.fail_at == name:
            raise RuntimeError(name)

    def begin_serializable(self): self.hit("begin_serializable")
    def acquire_protection(self, game_pks): self.hit("acquire_protection")
    def triggers_and_constraints_enabled(self): self.hit("constraints"); return self.constraints
    def current_value_hash(self, relation, exact_key): return self.state.get(self.identity(relation, exact_key))
    def completed_plan_hash(self, authorization_id): return self.completed.get(authorization_id)
    def write_attempt_claim(self, authorization, plan): self.hit("attempt_claim")
    def write_before_state_receipt(self, authorization, plan): self.hit("before_state_receipt")
    def insert_exact_fact(self, proposal): self.hit("insert"); self.state[self.identity(proposal.relation, proposal.exact_key)] = proposal.proposed_value_hash
    def relocate_exact_fact(self, proposal): self.hit("relocate"); self.state[self.identity(proposal.relation, proposal.exact_key)] = proposal.proposed_value_hash
    def record_legacy_quarantine(self, proposal): self.hit("quarantine")
    def validate_expected_state(self, plan):
        self.hit("validate")
        if self.fail_at == "expected_mismatch": return False
        for row in plan.proposals:
            expected = row.proposed_value_hash if row.operation in {INSERT_NEW_EXACT_FACT, RELOCATE_MATCHING_MISDATED_FACT} else row.current_value_hash
            if self.state.get(self.identity(row.relation, row.exact_key)) != expected:
                return False
        return True
    def write_completion_receipt(self, authorization, plan): self.hit("completion_receipt"); self.completed[authorization.authorization_id] = plan.plan_sha256
    def commit(self): self.hit("commit"); self.before = deepcopy(self.state)
    def rollback(self): self.events.append("rollback"); self.state = deepcopy(self.before)


def executable_plan():
    return ExactGameMutationPlannerV1().build([
        intent(relation="game_info", player_id=0, game_pk=10, proposed={"date": "2026-07-01"}),
        intent(game_pk=10, current={"date": "2026-06-30", "hits": 1}, proposed={"date": "2026-07-01", "hits": 1}, force=RELOCATE_MATCHING_MISDATED_FACT),
    ])


def authorization(plan, *, expires="2026-07-02T00:00:00Z"):
    return MutationAuthorizationV1.create(
        authorization_id="separate-review-1",
        plan=plan,
        issued_at_utc="2026-07-01T00:00:00Z",
        expires_at_utc=expires,
        authorized_game_pks={10},
    )


def backend_for(plan):
    state = {}
    for row in plan.proposals:
        if row.current_value_hash is not None:
            state[FakeBackend.identity(row.relation, row.exact_key)] = row.current_value_hash
    return FakeBackend(state)


def test_default_is_dry_run_and_missing_or_stale_authorization_fails():
    plan = executable_plan()
    backend = backend_for(plan)
    result = OrderedMutationExecutorV1().execute(plan, backend)
    assert result.status == "DRY_RUN_NO_MUTATION" and not backend.events
    with pytest.raises(LoaderContractError, match="AUTHORIZATION_ARTIFACT_REQUIRED"):
        OrderedMutationExecutorV1().execute(plan, backend, execute=True, now_utc="2026-07-01T12:00:00Z")
    with pytest.raises(LoaderContractError, match="AUTHORIZATION_STALE"):
        OrderedMutationExecutorV1().execute(plan, backend, execute=True, authorization=authorization(plan, expires="2026-07-01T01:00:00Z"), now_utc="2026-07-01T12:00:00Z")


def test_source_hash_plan_binding_and_concurrent_state_mismatch_fail_before_mutation():
    plan = executable_plan()
    auth = authorization(plan)
    bad = MutationAuthorizationV1(**(auth.__dict__ | {"source_set_sha256": "f" * 64}))
    with pytest.raises(LoaderContractError, match="AUTHORIZATION_ARTIFACT_HASH_MISMATCH"):
        OrderedMutationExecutorV1().execute(plan, backend_for(plan), execute=True, authorization=bad, now_utc="2026-07-01T12:00:00Z")
    backend = backend_for(plan)
    first = next(row for row in plan.proposals if row.current_value_hash is not None)
    backend.state[FakeBackend.identity(first.relation, first.exact_key)] = "0" * 64
    with pytest.raises(LoaderContractError, match="CONCURRENT_STATE_MISMATCH"):
        OrderedMutationExecutorV1().execute(plan, backend, execute=True, authorization=auth, now_utc="2026-07-01T12:00:00Z")
    assert "insert" not in backend.events and "relocate" not in backend.events


@pytest.mark.parametrize("failure", ["attempt_claim", "before_state_receipt", "insert", "relocate", "completion_receipt", "commit", "expected_mismatch"])
def test_failure_at_each_ordered_phase_rolls_back(failure):
    plan = executable_plan()
    backend = backend_for(plan)
    backend.fail_at = failure
    original = deepcopy(backend.state)
    with pytest.raises((RuntimeError, LoaderContractError)):
        OrderedMutationExecutorV1().execute(plan, backend, execute=True, authorization=authorization(plan), now_utc="2026-07-01T12:00:00Z")
    assert backend.state == original
    assert backend.events[-1] == "rollback"


def test_ordered_execution_and_repeated_execution_are_idempotent():
    plan = executable_plan()
    backend = backend_for(plan)
    auth = authorization(plan)
    result = OrderedMutationExecutorV1().execute(plan, backend, execute=True, authorization=auth, now_utc="2026-07-01T12:00:00Z")
    assert result.status == "COMMITTED"
    assert backend.events.index("attempt_claim") < backend.events.index("before_state_receipt")
    assert backend.events.index("before_state_receipt") < backend.events.index("relocate")
    assert backend.events.index("relocate") < backend.events.index("insert")
    assert backend.events.index("insert") < backend.events.index("completion_receipt")
    again = OrderedMutationExecutorV1().execute(plan, backend, execute=True, authorization=auth, now_utc="2026-07-01T12:30:00Z")
    assert again.status == "IDEMPOTENT_COMPLETED_PLAN_MATCH"
    assert again.mutation_count == 0


def test_blocking_payload_and_unproven_cutoff_never_execute():
    conflict = ExactGameMutationPlannerV1().build([intent(current={"hits": 1}, proposed={"hits": 2})])
    assert conflict.proposals[0].operation == BLOCK_CONFLICTING_PAYLOAD
    with pytest.raises(LoaderContractError, match="PLAN_CONTAINS_BLOCKING_OPERATIONS"):
        OrderedMutationExecutorV1().execute(conflict, backend_for(conflict), execute=True, authorization=MutationAuthorizationV1.create(
            authorization_id="x", plan=conflict, issued_at_utc="2026-07-01T00:00:00Z", expires_at_utc="2026-07-02T00:00:00Z", authorized_game_pks={1}), now_utc="2026-07-01T12:00:00Z")


def test_corrected_path_has_no_legacy_derived_mutation_identity_shortcuts_or_operational_import():
    text = MODULE.read_text(encoding="utf-8") + SCRIPT.read_text(encoding="utf-8")
    forbidden = ["MAX(game_id)", "requests.get", "pg_connect", "psycopg", "supabase"]
    assert not any(value in text for value in forbidden)
    assert "INSERT INTO mlb.player_derived_stats" not in text
    assert "UPDATE mlb.player_derived_stats" not in text
    assert "DELETE FROM mlb.player_derived_stats" not in text
    assert "IMMUTABLE_FEATURE_INPUT_CUTOFF_NOT_RETAINED" in text
    assert AUTHORIZATION_VERSION in text
