"""Offline audit-policy tests; never connect to DB, HTTP or production runners."""
from datetime import datetime,timezone
import ast
import socket
import typing

import pytest

from backend.mlb.scripts import audit_mlb_bvp_downstream_materiality_v1 as audit


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def forbidden(*args,**kwargs):
        raise AssertionError("NETWORK_FORBIDDEN")
    monkeypatch.setattr(socket.socket,"connect",forbidden)
    monkeypatch.setattr(socket,"getaddrinfo",forbidden)
    monkeypatch.setattr(socket,"create_connection",forbidden)


def source(**changes):
    row={"prop_type":"total_bases","player_id":42,"game_id":77,"game_date":"2026-04-15",
         "feature_set_tag":"v1","model_tag":"bvp_pvb_refresh_v1","computed_at":"2026-04-15T17:06:37Z",
         "features":{"bvp_plate_appearances":5.,"bvp_at_bats":4.,"bvp_hits":2.,"bvp_total_bases":3.}}
    return {**row,**changes}


def detail(boundary=True):
    return {"classification":"PREVIOUS_LOCAL_DATE_UTC_BOUNDARY_FINGERPRINT" if boundary else "OTHER_OFF_DATE_RETAINED_AUTHORITY_NOT_PROVEN_MAPPING_CAUSE"}


def test_fingerprint_alone_never_certifies_defect():
    group,_=audit.disposition(source(),detail(),[],[],set())
    assert group==audit.CLASSES[3]


def test_mapping_run_timestamp_proves_bounded_april15():
    runs=[{"slate_date":"2026-04-15","start_utc":"2026-04-15T17:05:40Z","end_utc":"2026-04-15T17:23:07Z",
           "summary":{"local_game_id_mapped":4,"rows_written":2613}}]
    assert audit.disposition(source(),detail(),[],runs,set())[0]==audit.CLASSES[0]
    assert audit.disposition(source(computed_at="2026-04-15T17:30:00Z"),detail(),[],runs,set())[0]==audit.CLASSES[3]


def test_exact_row_quarantine_not_game_wide_blanket():
    banned={audit.stable_hash(source())}
    assert audit.disposition(source(),detail(),[],[],banned)[0]==audit.CLASSES[4]
    assert audit.disposition(source(player_id=43),detail(),[],[],banned)[0]==audit.CLASSES[3]


def test_postponement_requires_explicit_retained_requested_day():
    evidence=[{"schedule_date":"2026-04-15","start_utc":"2026-04-15T20:00:00Z","rescheduleDate":"2026-04-16T20:00:00Z"}]
    assert audit.disposition(source(),detail(False),evidence,[],set())[0]==audit.CLASSES[1]
    evidence[0].pop("rescheduleDate")
    assert audit.disposition(source(),detail(False),evidence,[],set())[0]==audit.CLASSES[3]


def test_poststart_source_not_legitimate_pregame():
    evidence=[{"schedule_date":"2026-04-15","start_utc":"2026-04-15T16:00:00Z","resumedFromDate":"2026-04-14"}]
    assert audit.disposition(source(),detail(False),evidence,[],set())[0]==audit.CLASSES[3]


def compact(**changes):
    return {"prop_type":"total_bases","bvp_source_game_id":"77.0","bvp_source_date":"2026-04-15","bvp_feature_set_tag":"v1",
            "bvp_plate_appearances":"5.0","bvp_at_bats":"4.0","bvp_hits":"2.0","bvp_total_bases":"3.0",**changes}


def test_compact_source_key_and_values_prove_substantive_not_timestamp():
    result=audit.artifact_matches(compact(),[source()])[0]
    assert result["proven_source_admission"] and not result["original_source_timestamp_proven"]


@pytest.mark.parametrize("field,value",[("bvp_source_date","2026-04-14"),("bvp_source_game_id","78"),("bvp_feature_set_tag","v2")])
def test_payload_equality_without_source_identity_is_only_possible(field,value):
    result=audit.artifact_matches(compact(**{field:value}),[source()])[0]
    assert not result["proven_source_admission"]


def test_one_value_changed_prevents_consumption_proof():
    assert audit.artifact_matches(compact(bvp_hits="3"),[source()])==[]


def test_cross_prop_identity_not_conflated():
    assert audit.artifact_matches(compact(prop_type="hits"),[source()])==[]


def test_zero_probability_is_scored_and_push_not_graded():
    m=audit.metrics([{"model_prob_over":0,"actual_over_outcome":"loss"},{"model_prob_over":1,"actual_over_outcome":"win"},
                     {"model_prob_over":.5,"actual_over_outcome":"push"}])
    assert m["scored_rows"]==2 and m["brier"]==0 and m["accuracy"]==1
    assert m["roi"] is None


def test_all_three_current_queries_preserve_identity_metadata():
    paths=("backend/domains/mlb/prop_workflow.py","backend/mlb/model_trainer.py","backend/mlb/v2_write_training_from_pfp.py")
    for path in paths:
        text=(audit.ROOT/path).read_text()
        assert "certified_rows" in text
        assert all(field in text for field in ("game_date","feature_set_tag","model_tag","computed_at"))


def test_snapshot_transactions_are_read_only_and_output_scope_bounded():
    text=audit.AUDIT_SOURCE.read_text()
    assert "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY" in text
    assert "OUTPUT_SCOPE_VIOLATION" in text and "SNAPSHOT_ALREADY_EXISTS_DO_NOT_OVERWRITE" in text
    assert "requests.get(" not in text and ".fit(" not in text


@pytest.mark.parametrize("path,name",[
    ("backend/domains/mlb/prop_workflow.py","_load_latest_pfp_features"),
    ("backend/mlb/model_trainer.py","_fetch_pfp_feature_rows"),
    ("backend/mlb/v2_write_training_from_pfp.py","_pfp_rows_for_date"),
])
def test_actual_query_functions_reject_wrong_slate_without_import_side_effects(monkeypatch,path,name):
    from backend.mlb.shared import bvp_identity
    good=source(); bad=source(game_id=78)
    gate=bvp_identity.certified_rows
    def protected(rows):
        return gate(rows,authority={77:{"2026-04-15"},78:{"2026-04-14"}},exclusions=[])
    monkeypatch.setattr(bvp_identity,"certified_rows",protected)
    class FakeQuery:
        data=[bad,good]
        def __getattr__(self,key):
            return lambda *args,**kwargs: self
    captured=[]
    def fetch(sql,params):
        captured.append(sql)
        return [bad,good]
    env={**vars(typing),"Client":object,"pg_fetchall":fetch,"supabase":FakeQuery(),
         "certified_rows":protected,"_chunked":lambda xs,n:[xs],"_pg_data":lambda r:r.data}
    tree=ast.parse((audit.ROOT/path).read_text())
    function=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name==name)
    module=ast.Module(body=[function],type_ignores=[])
    exec(compile(ast.fix_missing_locations(module),path,"exec"),env)
    if name=="_load_latest_pfp_features":
        result=env[name](prop_type="total_bases",player_id=42,game_id=77,game_date="2026-04-15",feature_set_tag="v1")
        assert result==good["features"]
    elif name=="_fetch_pfp_feature_rows":
        assert env[name](None,game_ids=[77,78],feature_set_tag="v1")==[good]
    else:
        assert env[name]("2026-04-15")==[good]
    assert all("computed_at" in q and "model_tag" in q for q in captured)
