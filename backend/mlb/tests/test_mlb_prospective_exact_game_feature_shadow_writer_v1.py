from __future__ import annotations

import json
import inspect
from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.mlb import prospective_exact_game_feature_shadow_writer_v1 as shadow
from backend.mlb.scripts import run_mlb_exact_game_feature_shadow_v1 as runner
from backend.mlb.season_transition.game_phase_authority_v1 import GamePhaseAuthorityError


SHA = "a" * 64
SCHEDULE_SHA = "b" * 64


class StubAuthority:
    def __init__(self, rows):
        self._rows = rows
        self.metadata = SimpleNamespace(
            authority_interface="MLB_CANONICAL_GAME_PHASE_AUTHORITY_V1",
            backend="TEST_FILE_BACKEND",
            snapshot_id="fixture-v1",
            snapshot_descriptor_path="fixtures/authority.json",
            snapshot_descriptor_sha256="c" * 64,
            proposal_path="fixtures/proposal.jsonl",
            proposal_sha256="d" * 64,
            proposal_count=len(rows),
            authority_records_sha256="e" * 64,
        )

    def lookup_exact(self, game_pk):
        try:
            value = self._rows[int(game_pk)]
        except (KeyError, TypeError, ValueError):
            raise GamePhaseAuthorityError("GAME_PHASE_EVIDENCE_MISSING", game_pk=int(game_pk)) from None
        return SimpleNamespace(
            game_pk=int(game_pk),
            source_season=2026,
            source_game_type=value.get("game_type", "R"),
            season_phase=value.get("phase", "REGULAR_SEASON"),
            schedule_relationships=value.get("relationships", {}),
            source_paths=(f"fixtures/{game_pk}.json",),
            source_hashes=(SHA,),
            scheduled_start_utc=value["start"],
        )


def game(game_pk, start, away=1, home=2, **extra):
    return {
        "gamePk": game_pk,
        "gameDate": start,
        "officialDate": start[:10],
        "status": {"abstractGameState": "Preview", "detailedState": "Scheduled", "codedGameState": "S"},
        "teams": {
            "away": {"team": {"id": away, "abbreviation": "AWY"}},
            "home": {"team": {"id": home, "abbreviation": "HME"}},
        },
        **extra,
    }


def snapshot(*, cutoff="2026-09-24T17:00:00Z", duplicate_stat=False):
    stats = [
        {
            "player_id": 10,
            "game_id": 800001,
            "game_date": "2026-09-20",
            "team": "AWY",
            "opponent": "HME",
            "is_home": "false",
            "position": "OF",
            "plate_appearances": "4",
            "at_bats": "4",
            "hits": "2",
            "total_bases": "3",
            "rbis": "1",
            "runs_scored": "1",
            "strikeouts_batting": "0",
            "walks": "0",
            "home_runs": "0",
            "stolen_bases": "0",
            "strikeouts_pitching": None,
            "walks_allowed": None,
            "hits_allowed": None,
            "outs_recorded": None,
            "earned_runs": None,
            "updated_at": "2026-09-20T23:30:00Z",
        }
    ]
    if duplicate_stat:
        stats.append({**stats[0], "hits": "3"})
    return shadow.SnapshotRowsV1(
        cutoff_utc=cutoff,
        isolation_level="repeatable read",
        read_only=True,
        roster_rows=(
            {"player_id": 10, "player_name": "Away Player", "team": "AWY", "team_id": "1", "active": "true", "status": "active", "updated_at": "2026-09-24T16:59:00Z"},
            {"player_id": 20, "player_name": "Home Player", "team": "HME", "team_id": "2", "active": "true", "status": "active", "updated_at": "2026-09-24T16:59:00Z"},
        ),
        player_stat_rows=tuple(stats),
        roster_query_sha256="f" * 64,
        player_stats_query_sha256="1" * 64,
    )


def build(games, *, snap=None, authority_rows=None, prior_final=True, schedule_source=None):
    rows = authority_rows or {
        800001: {"start": "2026-09-20T20:00:00Z"},
        **{int(item["gamePk"]): {"start": item["gameDate"], "relationships": shadow._relationships(item)} for item in games if item.get("gamePk") is not None},
    }
    return shadow.build_shadow(
        slate_date="2026-09-24",
        run_identity="local_daily_fixture",
        wrapper_started_at_utc="2026-09-24T16:55:00Z",
        snapshot=snap or snapshot(),
        schedule_payload={"dates": [
            {"date": "2026-09-20", "games": [{
                **game(800001, "2026-09-20T20:00:00Z"),
                "officialDate": "2026-09-20",
                "status": (
                    {"abstractGameState": "Final", "detailedState": "Final", "codedGameState": "F", "statusCode": "F"}
                    if prior_final
                    else {"abstractGameState": "Live", "detailedState": "In Progress", "codedGameState": "I", "statusCode": "I"}
                ),
            }]},
            {"date": "2026-09-24", "games": games},
        ]},
        schedule_source=schedule_source or {"source_path": "fixtures/schedule.json", "source_sha256": SCHEDULE_SHA, "retrieved_at_utc": "2026-09-24T16:58:00Z", "selection_receipt_path": "fixtures/selection.json", "selection_receipt_sha256": SHA},
        phase_authority=StubAuthority(rows),
        code_commit="2" * 40,
        contract_sha256="3" * 64,
        interpreter="/canonical/python",
    )


def test_ordinary_game_is_exact_player_game_and_strict_prior():
    result = build([game(824900, "2026-09-24T20:00:00Z")])
    assert {(row["player_id"], row["game_pk"]) for row in result.admitted} == {(10, 824900), (20, 824900)}
    away = next(row for row in result.admitted if row["player_id"] == 10)
    assert away["eligible_prior_game_pks"] == [800001]
    assert away["feature_payload"]["prior_game_count"] == 1
    assert result.receipt["transaction"] == {"isolation_level": "REPEATABLE_READ", "read_only": True, "transaction_id_requested": False, "database_writes": 0}


def test_doubleheader_keeps_games_separate_without_max_or_date_identity():
    games = [
        game(824459, "2026-09-24T20:00:00Z", doubleHeader="Y", gameNumber=1),
        game(824460, "2026-09-24T23:00:00Z", doubleHeader="Y", gameNumber=2),
    ]
    result = build(games)
    assert len(result.admitted) == 4
    assert {row["game_pk"] for row in result.admitted} == {824459, 824460}
    assert len({row["row_sha256"] for row in result.admitted}) == 4


def test_game_one_final_before_game_two_cutoff_is_strict_prior_for_same_player():
    game_one = game(824459, "2026-09-24T14:00:00Z", doubleHeader="Y", gameNumber=1)
    game_one["status"] = {"abstractGameState": "Final", "detailedState": "Final", "codedGameState": "F", "statusCode": "F"}
    game_two = game(824460, "2026-09-24T23:00:00Z", doubleHeader="Y", gameNumber=2)
    base = snapshot()
    game_one_stat = {**base.player_stat_rows[0], "game_id": 824459, "game_date": "2026-09-24", "updated_at": "2026-09-24T16:00:00Z"}
    snap = shadow.SnapshotRowsV1(**{**base.__dict__, "player_stat_rows": base.player_stat_rows + (game_one_stat,)})
    result = build([game_one, game_two], snap=snap)
    row = next(row for row in result.admitted if row["player_id"] == 10 and row["game_pk"] == 824460)
    assert 824459 in row["eligible_prior_game_pks"]


def test_game_one_not_final_before_game_two_cutoff_is_not_a_prior_fact():
    game_one = game(824459, "2026-09-24T14:00:00Z", doubleHeader="Y", gameNumber=1)
    game_one["status"] = {"abstractGameState": "Live", "detailedState": "In Progress", "codedGameState": "I", "statusCode": "I"}
    game_two = game(824460, "2026-09-24T23:00:00Z", doubleHeader="Y", gameNumber=2)
    base = snapshot()
    partial = {**base.player_stat_rows[0], "game_id": 824459, "game_date": "2026-09-24", "updated_at": "2026-09-24T16:00:00Z"}
    snap = shadow.SnapshotRowsV1(**{**base.__dict__, "player_stat_rows": base.player_stat_rows + (partial,)})
    result = build([game_one, game_two], snap=snap)
    row = next(row for row in result.admitted if row["player_id"] == 10 and row["game_pk"] == 824460)
    assert 824459 not in row["eligible_prior_game_pks"]
    assert result.summary["historical_fact_rejections"]["HISTORICAL_GAME_NOT_PLAYABLE_TERMINAL_AT_CUTOFF"] >= 1


def test_makeup_824785_and_distinct_824784_remain_separate():
    games = [
        game(824785, "2026-09-24T20:00:00Z", rescheduledFrom="2026-09-22T20:00:00Z"),
        game(824784, "2026-09-24T23:00:00Z", doubleHeader="Y", gameNumber=2),
    ]
    result = build(games)
    assert {row["game_pk"] for row in result.admitted} == {824784, 824785}
    makeup = next(row for row in result.admitted if row["game_pk"] == 824785)
    assert makeup["authority"]["schedule_relationships"]["rescheduledFrom"] == "2026-09-22T20:00:00Z"
    assert makeup["source_observations"]


def test_suspended_resumed_824912_is_one_exact_identity():
    games = [game(824912, "2026-09-24T22:00:00Z", resumeDate="2026-09-24T22:00:00Z")]
    result = build(games)
    assert len(result.admitted) == 2
    assert {row["game_pk"] for row in result.admitted} == {824912}


def test_chronology_post_start_and_post_cutoff_fail_closed():
    post_start = build([game(824901, "2026-09-24T16:00:00Z")])
    assert post_start.admitted == ()
    assert {row["reason"] for row in post_start.rejected} == {"TARGET_GAME_STARTED_AT_OR_BEFORE_CUTOFF"}
    bad_roster = snapshot()
    bad_roster = shadow.SnapshotRowsV1(**{**bad_roster.__dict__, "roster_rows": ({**bad_roster.roster_rows[0], "updated_at": "2026-09-24T17:01:00Z"},)})
    result = build([game(824902, "2026-09-24T20:00:00Z")], snap=bad_roster)
    assert result.admitted == ()
    assert {row["reason"] for row in result.rejected} == {"SOURCE_OBSERVED_POST_CUTOFF"}


def test_phase_partition_and_missing_identity_fail_closed():
    target = game(824903, "2026-09-24T20:00:00Z")
    rows = {800001: {"start": "2026-09-20T20:00:00Z"}, 824903: {"start": target["gameDate"], "phase": "PRESEASON"}}
    result = build([target], authority_rows=rows)
    assert result.admitted == ()
    assert result.rejected[0]["reason"] == "UNSUPPORTED_TARGET_PHASE"
    missing = build([{**target, "gamePk": None}])
    assert missing.admitted == ()
    assert missing.rejected[0]["reason"] == "TARGET_GAME_IDENTITY_INVALID"


def test_regular_and_postseason_rows_are_explicitly_partitioned():
    regular = game(824907, "2026-09-24T20:00:00Z")
    postseason = game(824908, "2026-09-24T22:00:00Z")
    rows = {
        800001: {"start": "2026-09-20T20:00:00Z"},
        824907: {"start": regular["gameDate"], "phase": "REGULAR_SEASON"},
        824908: {"start": postseason["gameDate"], "phase": "POSTSEASON", "game_type": "F"},
    }
    result = build([regular, postseason], authority_rows=rows)
    phases = {(row["game_pk"], row["season_phase"]) for row in result.admitted}
    assert phases == {(824907, "REGULAR_SEASON"), (824908, "POSTSEASON")}


def test_missing_and_conflicting_game_authority_fail_closed():
    missing = game(824909, "2026-09-24T20:00:00Z")
    result = build([missing], authority_rows={800001: {"start": "2026-09-20T20:00:00Z"}})
    assert result.admitted == ()
    assert result.rejected[0]["reason"] == "CANONICAL_PHASE_AUTHORITY_REJECTED"
    conflict = game(824910, "2026-09-24T21:00:00Z")
    conflicted = build([conflict], authority_rows={800001: {"start": "2026-09-20T20:00:00Z"}, 824910: {"start": conflict["gameDate"], "phase": None}})
    assert conflicted.admitted == ()
    assert conflicted.rejected[0]["reason"] == "CANONICAL_PHASE_AUTHORITY_CONFLICT"


def test_missing_player_identity_is_rejected():
    base = snapshot()
    snap = shadow.SnapshotRowsV1(**{**base.__dict__, "roster_rows": ({**base.roster_rows[0], "player_id": None},)})
    result = build([game(824913, "2026-09-24T20:00:00Z")], snap=snap)
    assert result.admitted == ()
    assert result.rejected[0]["reason"] == "EXACT_ACTIVE_ROSTER_MISSING"


def test_missing_cutoff_fails_closed_in_offline_candidate_contract():
    authority = shadow.ExactGameAuthorityV1.from_canonical_interface(
        StubAuthority({824910: {"start": "2026-09-24T20:00:00Z"}}),
        game_pk=824910,
        official_date="2026-09-24",
        scheduled_start_utc="2026-09-24T20:00:00Z",
    )
    target = shadow.ExactGameTargetV1.create(player_id=10, authority=authority, feature_input_cutoff_utc=None)
    proposal = shadow.OfflineExactGameCandidateBuilderV1().build(facts=(), targets=(target,))[0]
    assert proposal.reason == "IMMUTABLE_FEATURE_INPUT_CUTOFF_NOT_RETAINED"


def test_duplicate_identical_target_coalesces_once():
    authority = shadow.ExactGameAuthorityV1.from_canonical_interface(
        StubAuthority({824911: {"start": "2026-09-24T20:00:00Z"}}),
        game_pk=824911,
        official_date="2026-09-24",
        scheduled_start_utc="2026-09-24T20:00:00Z",
    )
    target = shadow.ExactGameTargetV1.create(player_id=10, authority=authority, feature_input_cutoff_utc="2026-09-24T17:00:00Z")
    proposals = shadow.OfflineExactGameCandidateBuilderV1().build(facts=(), targets=(target, target))
    assert len(proposals) == 1


def test_duplicate_conflicting_target_fails_closed():
    stub = StubAuthority({824914: {"start": "2026-09-24T20:00:00Z"}})
    first = shadow.ExactGameAuthorityV1.from_canonical_interface(stub, game_pk=824914, official_date="2026-09-24", scheduled_start_utc="2026-09-24T20:00:00Z")
    second = shadow.ExactGameAuthorityV1.from_canonical_interface(stub, game_pk=824914, official_date="2026-09-24", scheduled_start_utc="2026-09-24T21:00:00Z")
    one = shadow.ExactGameTargetV1.create(player_id=10, authority=first, feature_input_cutoff_utc="2026-09-24T17:00:00Z")
    two = shadow.ExactGameTargetV1.create(player_id=10, authority=second, feature_input_cutoff_utc="2026-09-24T17:00:00Z")
    with pytest.raises(Exception, match="DUPLICATE_EXACT_TARGET_CONFLICT"):
        shadow.OfflineExactGameCandidateBuilderV1().build(facts=(), targets=(one, two))


def test_conflicting_duplicate_fact_is_rejected_not_collapsed():
    with pytest.raises(Exception, match="DUPLICATE_EXACT_IDENTITY_CONFLICTING_PAYLOAD"):
        build([game(824904, "2026-09-24T20:00:00Z")], snap=snapshot(duplicate_stat=True))


def test_create_only_backend_is_idempotent_and_conflicts_fail(tmp_path):
    result = build([game(824905, "2026-09-24T20:00:00Z")])
    first, directory = shadow.publish_create_only(root=tmp_path, slate_date="2026-09-24", run_identity="run", result=result)
    second, same = shadow.publish_create_only(root=tmp_path, slate_date="2026-09-24", run_identity="run", result=result)
    assert first == "SHADOW_SNAPSHOT_CREATED"
    assert second == "SHADOW_SNAPSHOT_ALREADY_EXISTS_IDENTICAL"
    assert same == directory
    assert shadow.verify_package(directory)["status"] == "PASS"
    changed = build([game(824906, "2026-09-24T21:00:00Z")])
    with pytest.raises(shadow.ShadowWriterError, match="IMMUTABLE_RUN_IDENTITY_CONFLICT"):
        shadow.publish_create_only(root=tmp_path, slate_date="2026-09-24", run_identity="run", result=changed)


def test_existing_exact_run_short_circuits_without_new_snapshot(tmp_path, monkeypatch):
    fixture_dir = tmp_path / "fixtures"
    fixture_dir.mkdir()
    schedule = fixture_dir / "schedule.json"
    selection = fixture_dir / "selection.json"
    schedule.write_text("schedule")
    selection.write_text("selection")
    source = {"source_path": "fixtures/schedule.json", "source_sha256": shadow.sha256_path(schedule), "retrieved_at_utc": "2026-09-24T16:58:00Z", "selection_receipt_path": "fixtures/selection.json", "selection_receipt_sha256": shadow.sha256_path(selection)}
    result = build([game(824905, "2026-09-24T20:00:00Z")], schedule_source=source)
    output_root = tmp_path / "output"
    _, directory = shadow.publish_create_only(root=output_root, slate_date="2026-09-24", run_identity="local_daily_fixture", result=result)
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    status = runner._existing_run_status(
        output=directory,
        slate_date="2026-09-24",
        run_identity="local_daily_fixture",
        wrapper_started_at_utc="2026-09-24T16:55:00Z",
        code_commit="2" * 40,
        contract_sha256="3" * 64,
    )
    assert status == "SHADOW_SNAPSHOT_ALREADY_EXISTS_IDENTICAL"


def test_current_run_schedule_discovery_uses_one_immutable_receipt_not_latest(tmp_path, monkeypatch):
    source_dir = tmp_path / "artifacts/ops/mlb_public_game_moneyline_history_schedules/2026-09-24"
    source_dir.mkdir(parents=True)
    schedule = source_dir / "immutable.json"
    schedule.write_text(json.dumps({"dates": []}))
    source = {
        "source_path": str(schedule.relative_to(tmp_path)),
        "source_sha256": shadow.sha256_path(schedule),
        "retrieved_at_utc": "2026-09-24T17:00:01Z",
        "date_horizon": {"start_date": "2026-08-05", "end_date": "2026-09-24"},
    }
    selection = source_dir / "immutable.selection.json"
    selection.write_text(json.dumps({"history_schedule": source}))
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    found, retained = runner._schedule_source("2026-09-24", "2026-09-24T17:00:00Z")
    assert found == schedule
    assert retained["selection_receipt_path"] == str(selection.relative_to(tmp_path))
    (tmp_path / "artifacts/ops/mlb_public_game_moneyline_daily_2026-09-24_latest.json").write_text("conflict")
    found_again, _ = runner._schedule_source("2026-09-24", "2026-09-24T17:00:00Z")
    assert found_again == schedule


def test_contract_and_source_forbid_legacy_grain_and_max_game_id():
    contract = json.loads(Path("backend/mlb/contracts/mlb_2026_prospective_exact_game_feature_shadow_writer_v1.json").read_text())
    source = Path(shadow.__file__).read_text()
    assert contract["admission"]["date_only_identity_forbidden"] is True
    assert contract["admission"]["max_game_id_forbidden"] is True
    assert "player_derived_stats" not in shadow.PLAYER_STATS_SQL
    assert "MAX(" not in shadow.PLAYER_STATS_SQL.upper()
    assert "txid_" not in inspect.getsource(shadow.load_read_only_snapshot).lower()


def test_hook_is_shadow_only_and_before_bvp_contract():
    hook = Path("bin/mlb_exact_game_feature_shadow_daily_hook.sh").read_text()
    contract = json.loads(Path("backend/mlb/contracts/mlb_2026_prospective_exact_game_feature_shadow_writer_v1.json").read_text())
    assert "run_mlb_exact_game_feature_shadow_v1" in hook
    assert contract["activation_boundary"] == "AFTER_SUCCESSFUL_ROSTER_REFRESH_BEFORE_BVP_AND_STAT_DERIVED"
    assert contract["production"]["consumers"] == []
    assert contract["production"]["network_requests"] == 0
