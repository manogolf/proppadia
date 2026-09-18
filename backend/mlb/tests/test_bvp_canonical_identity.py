"""Deterministic identity regressions. No HTTP, acquisition or database writes."""
from dataclasses import replace
import json
from pathlib import Path
from unittest.mock import Mock

import pytest

from backend.mlb.scripts import refresh_mlb_bvp_pvb as bvp
from backend.mlb.shared import bvp_identity as identity


@pytest.fixture(autouse=True)
def offline(monkeypatch, tmp_path):
    monkeypatch.setattr(bvp.requests, "get", Mock(side_effect=AssertionError("LIVE_NETWORK_FORBIDDEN")))
    monkeypatch.setattr(bvp, "pg_connect", Mock(side_effect=AssertionError("DATABASE_FORBIDDEN")))
    monkeypatch.setattr(identity, "ROOT", tmp_path)


def game(game_id=823977, **kwargs):
    return replace(bvp.GameRow(game_id, "2026-09-18", 108, 142, 111, 222,
                              official_game_id=game_id, official_date="2026-09-18",
                              scheduled_start_utc="2026-09-19T01:38:00Z",
                              schedule_date="2026-09-18", game_state="Scheduled",
                              source_observed_at_utc="2026-09-18T17:12:42Z"), **kwargs)


@pytest.mark.parametrize("start,official,schedule,reason", [
    ("2026-09-18T22:40:00Z", "2026-09-18", "2026-09-18", None),
    ("2026-09-18T01:38:00Z", "2026-09-17", "2026-09-18", "OFF_DATE_GAME_REJECTED"),
    ("2026-09-19T08:00:00Z", "2026-09-19", "2026-09-18", "OFF_DATE_GAME_REJECTED"),
    ("2026-09-19T02:15:00Z", "2026-09-18", "2026-09-18", None),
])
def test_local_date_boundaries(start, official, schedule, reason):
    assert identity.game_rejection(game(scheduled_start_utc=start, official_date=official,
                                        schedule_date=schedule), "2026-09-18") == reason


def test_doubleheader_preserves_two_official_ids():
    valid, rejected = identity.validate_slate([game(1), game(2, scheduled_start_utc="2026-09-19T02:15:00Z")], "2026-09-18")
    assert [g.game_id for g in valid] == [1, 2] and not rejected


@pytest.mark.parametrize("state", ["Postponed", "Suspended", "Cancelled"])
def test_unverifiable_game_states_excluded(state):
    valid, rejected = identity.validate_slate([game(game_state=state)], "2026-09-18")
    assert not valid and rejected[0][1] == "CANONICAL_IDENTITY_UNRESOLVED"


def test_rescheduled_current_game_with_independent_authority():
    assert identity.game_rejection(game(game_state="Scheduled (Rescheduled)"), "2026-09-18") is None


@pytest.mark.parametrize("field", ["official_date", "schedule_date", "scheduled_start_utc", "official_game_id"])
def test_missing_authority_fails_closed(field):
    with pytest.raises(identity.CanonicalSlateIdentityError):
        identity.validate_slate([game(**{field: None})], "2026-09-18")


def fake_db(monkeypatch, rows):
    cursor = Mock();cursor.fetchall.return_value = rows
    cursor.__enter__ = Mock(return_value=cursor);cursor.__exit__ = Mock(return_value=False)
    conn = Mock();conn.cursor.return_value=cursor
    conn.__enter__ = Mock(return_value=conn);conn.__exit__ = Mock(return_value=False)
    monkeypatch.setattr(bvp, "pg_connect", lambda: conn)
    return cursor


@pytest.mark.parametrize("local_rows", [[], [{"game_id": 823978, "home_team_id":108, "away_team_id":142}]])
def test_optional_missing_or_cross_date_local_match_cannot_substitute_id(monkeypatch, local_rows):
    fake_db(monkeypatch, local_rows)
    result, counters = bvp._map_games_to_local_game_ids([game()], "2026-09-18")
    assert result[0].game_id == 823977 and counters["local_game_id_mapped"] == 0
    assert counters["local_game_id_unmapped"] == 1
    assert counters.unmapped_game_ids == [823977]


def row(game_id=823977, player=1, slate="2026-09-18"):
    return ("hits", player, game_id, slate, {"bvp_hits": 1.0}, "v1", "bvp_pvb_refresh_v1")


def test_foreign_official_id_and_mixed_candidate_population():
    valid, rejected = identity.filter_prepared_rows([row(), row(823978, 2)], [game()], "2026-09-18")
    assert valid == [row()] and len(rejected) == 1
    assert rejected[0]["reason_code"] == "CANONICAL_IDENTITY_UNRESOLVED"


def test_duplicate_keys_fail_closed():
    with pytest.raises(identity.CanonicalSlateIdentityError, match="DUPLICATE_ROW"):
        identity.filter_prepared_rows([row(), row()], [game()], "2026-09-18")


def prepare(monkeypatch, games):
    monkeypatch.setattr(bvp, "_fetch_schedule_games", lambda *a, **k: games)
    monkeypatch.setattr(bvp, "_map_games_to_local_game_ids", lambda g, d: (g, {}))
    monkeypatch.setattr(bvp, "_augment_games_with_db_starters", lambda g, d: (g, {}))
    monkeypatch.setattr(bvp, "_active_hitters", lambda *a, **k: [123])


def records(counters):
    return [json.loads(s) for s in counters.audit_path.read_text().splitlines()]


def test_missing_opposing_starter_retains_exact_side(monkeypatch):
    prepare(monkeypatch, [game(prob_sp_home=None, prob_sp_away=None)])
    rows, counters = bvp._build_rows_for_date("2026-09-18",feature_set_tag="v1",model_tag="bvp_pvb_refresh_v1",timeout_sec=20,retries=3)
    skipped = [r for r in records(counters) if r["reason_code"] == "OPPOSING_STARTER_UNRESOLVED"]
    assert not rows and counters["skip_no_opp_sp"] == 2 and counters["starter_unresolved_games"] == 1
    assert {r["team_id"] for r in skipped} == {108,142}
    assert all(r["game_id"] == 823977 and r["pitcher_id"] is None for r in skipped)


def test_successful_empty_response_and_request_identity_preserved(monkeypatch):
    prepare(monkeypatch, [game()])
    monkeypatch.setattr(bvp, "_fetch_json", lambda *a, **k: {"stats": []})
    rows, counters = bvp._build_rows_for_date("2026-09-18",feature_set_tag="v1",model_tag="bvp_pvb_refresh_v1",timeout_sec=20,retries=3)
    empty = [r for r in records(counters) if r["reason_code"] == "EMPTY_BVP_RESPONSE"]
    assert not rows and len(empty) == 2
    assert {r["pitcher_id"] for r in empty} == {111,222}
    assert all(r["batter_id"] == 123 and r["empty_response_payload"] == {"stats": []}
               and r["successful_response"] and not r["empty_response_is_zero_history"] for r in empty)
    assert counters.audit_path.stat().st_mode & 0o777 == 0o600


def test_failed_request_is_not_a_successful_empty_response(monkeypatch):
    prepare(monkeypatch, [game()])
    monkeypatch.setattr(bvp, "_fetch_json", Mock(side_effect=ValueError("bad response")))
    rows,counters=bvp._build_rows_for_date("2026-09-18",feature_set_tag="v1",model_tag="bvp_pvb_refresh_v1",timeout_sec=20,retries=3)
    evidence=records(counters)
    assert not rows and counters["bvp_fetch_errors"]==2
    assert not any(r["reason_code"]=="EMPTY_BVP_RESPONSE" for r in evidence)


def test_identity_receipt_failure_prevents_pairing_fetch(monkeypatch):
    prepare(monkeypatch, [game()])
    monkeypatch.setattr(bvp, "IdentityAudit", Mock(side_effect=PermissionError("claim blocked")))
    fetch=Mock();monkeypatch.setattr(bvp,"_bvp_stats",fetch)
    with pytest.raises(PermissionError):
        bvp._build_rows_for_date("2026-09-18",feature_set_tag="v1",model_tag="bvp_pvb_refresh_v1",timeout_sec=20,retries=3)
    fetch.assert_not_called()


def test_september18_population_preserves_1846_and_excludes_91():
    receipt=json.loads((Path(__file__).resolve().parents[3]/"artifacts/analysis/mlb/operational_reconciliation/2026-09-18/bvp_identity_exclusions_v1.json").read_text())
    fixture=json.loads((Path(__file__).parent/'fixtures/bvp_september18_identity_v1.json').read_text())
    valid=[{"prop_type":p,"player_id":i,"game_id":g,"game_date":"2026-09-18","feature_set_tag":"v1","computed_at":"2026-09-18T17:13:42.094683Z","features":{"bvp_hits":1}}
           for g,i in fixture['families'] if g!=823978 for p in bvp.BATTER_PROPS]
    bad=[{**r["key"],"game_date":r["stored_slate_date"],"computed_at":r["computed_at"],"features":{"bvp_hits":1}} for r in receipt["excluded_rows"]]
    population=valid+bad
    authority={g['game_id']:{g['official_date']} for g in fixture['games']};authority[823978]={'2026-09-17'}
    result=identity.certified_rows(population,authority=authority,exclusions=receipt["excluded_rows"])
    assert len(population)==1937 and len(result)==1846
    assert len({(r['prop_type'],r['player_id'],r['game_id'],r['feature_set_tag']) for r in result})==1846


def test_unknown_or_ambiguous_reader_authority_fails_closed():
    r={"prop_type":"hits","player_id":1,"game_id":823331,"game_date":"2026-09-18","feature_set_tag":"v1","features":{"bvp_hits":1}}
    assert not identity.certified_rows([r],authority={},exclusions=[])
    assert not identity.certified_rows([r],authority={823331:{"2026-09-17","2026-09-18"}},exclusions=[])


def test_uncertified_empty_shape_does_not_claim_successful_empty_stats(monkeypatch):
    prepare(monkeypatch,[game()])
    monkeypatch.setattr(bvp,"_fetch_json",lambda *a,**k:{})
    _,counters=bvp._build_rows_for_date("2026-09-18",feature_set_tag="v1",model_tag="bvp_pvb_refresh_v1",timeout_sec=20,retries=3)
    empty=[r for r in records(counters) if r['reason_code']=='EMPTY_BVP_RESPONSE_UNVERIFIABLE']
    assert len(empty)==2 and all(not r['successful_empty_response_verified'] for r in empty)


def test_reader_keeps_valid_fallback_after_rejecting_bad_first_row(monkeypatch):
    from backend.domains.mlb import prop_workflow
    bad={"prop_type":"hits","player_id":1,"game_id":823978,"game_date":"2026-09-18","feature_set_tag":"v1","computed_at":"2026-09-18T17:13:42.094683Z","features":{"bvp_hits":99}}
    good={**bad,"game_id":823331,"features":{"bvp_hits":1}}
    monkeypatch.setattr(prop_workflow,"pg_fetchall",lambda *a,**k:[bad,good])
    monkeypatch.setattr(prop_workflow,"certified_rows",lambda rows:identity.certified_rows(rows,authority={823978:{"2026-09-17"},823331:{"2026-09-18"}},exclusions=[]))
    assert prop_workflow._load_latest_pfp_features(prop_type='hits',player_id=1,game_id=823978,game_date='2026-09-18',feature_set_tag='v1')=={"bvp_hits":1}


def test_forward_mixed_schedule_preserves_valid_population(monkeypatch):
    prepare(monkeypatch,[game(),game(823978,official_date='2026-09-17',scheduled_start_utc='2026-09-18T01:38:00Z')])
    monkeypatch.setattr(bvp,'_fetch_json',lambda *a,**k:{'stats':[{'splits':[{'stat':{'hits':1,'atBats':2,'plateAppearances':2}}]}]})
    # A roster normally contains each player once per game; use one per team.
    monkeypatch.setattr(bvp,'_active_hitters',lambda team,*a,**k:[team])
    rows,counters=bvp._build_rows_for_date('2026-09-18',feature_set_tag='v1',model_tag='bvp_pvb_refresh_v1',timeout_sec=20,retries=3)
    assert len(rows)==26 and {r[2] for r in rows}=={823977}
    assert counters['off_date_rejected_games']==1
    assert any(r['reason_code']=='OFF_DATE_GAME_REJECTED' and r['game_id']==823978 for r in records(counters))


def test_substantive_row_mutation_breaks_exclusion_validator():
    with pytest.raises(identity.CanonicalSlateIdentityError,match='RETAINED_BVP_ROW_STREAM_CHANGED'):
        identity.validate_exclusion_set([],{'all_1937_row_stream_sha256':'fake'})


def test_new_rows_require_committed_pitcher_and_pregame_source_receipt():
    r={'prop_type':'hits','player_id':123,'game_id':823977,'game_date':'2026-09-18',
       'feature_set_tag':'v1','model_tag':'bvp_pvb_refresh_v1','computed_at':'2026-09-18T19:00:00Z','features':{'bvp_hits':1}}
    e={'contract':identity.CONTRACT,'reason_code':'BVP_RESPONSE_NONEMPTY','game_id':823977,'official_game_id':823977,
       'batter_id':123,'pitcher_id':222,'feature_set_tag':'v1','model_tag':'bvp_pvb_refresh_v1',
       'feature_payload_sha256':identity.stable_hash(r['features']),'successful_response':True,
       'official_date':'2026-09-18','schedule_date':'2026-09-18','slate_date':'2026-09-18',
       'scheduled_start_utc':'2026-09-19T01:38:00Z','response_observed_at_utc':'2026-09-18T18:59:55Z',
       'home_team_id':108,'away_team_id':142,'team_id':108,'opponent_team_id':142}
    commit={'reason_code':'DATABASE_WRITE_COMMITTED','acquisition_timestamp_utc':'2026-09-18T19:00:02Z'}
    assert not identity.forward_source_identity_valid(r,[[e]])
    assert identity.forward_source_identity_valid(r,[[e,commit]])
    assert not identity.forward_source_identity_valid(r,[[{**e,'pitcher_id':None},commit]])
    assert not identity.forward_source_identity_valid(r,[[{**e,'response_observed_at_utc':'2026-09-19T02:00:00Z'},commit]])
    assert not identity.forward_source_identity_valid({**r,'features':{'bvp_hits':2}},[[e,commit]])
    assert not identity.certified_rows([r],authority={823977:{'2026-09-18'}},exclusions=[])


def test_unverifiable_slate_fails_acquisition_before_write(monkeypatch):
    prepare(monkeypatch,[game(official_date=None)])
    write=Mock();fetch=Mock()
    monkeypatch.setattr(bvp,'_upsert_rows',write);monkeypatch.setattr(bvp,'_bvp_stats',fetch)
    with pytest.raises(identity.CanonicalSlateIdentityError):
        bvp.main(['--date','2026-09-18'])
    write.assert_not_called();fetch.assert_not_called()
