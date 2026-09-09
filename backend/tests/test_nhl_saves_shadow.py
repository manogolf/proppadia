from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

import backend.nhl.saves_shadow.core as shadow
from backend.nhl.saves_quote_capture.core import capture_run, normalize_quotes, sha256_file


def manifest(directory: Path, *paths: Path, name: str="SHA256SUMS") -> Path:
    target=directory/name
    target.write_text("".join(f"{sha256_file(path)}  {path.name}\n" for path in paths))
    return target


def fixture(tmp_path: Path, game_type: int=1):
    base=pd.read_csv("backend/nhl/exports/history/2026-04-16/train_goalie_saves_v2.csv").iloc[:4].copy()
    base["game_id"]=[9001,9001,9002,9002];base["player_id"]=[101,102,201,202]
    base["goalie_id"]=base.player_id;base["goalie_name"]=["Alpha Goalie","Backup Alpha","Bravo Goalie","Backup Bravo"]
    base["team"]=["AAA","BBB","CCC","DDD"];base["opponent"]=["BBB","AAA","DDD","CCC"]
    base["canonical_season"]=2026;base["slate_date"]="2026-09-20";base["scheduled_start_time_utc"]="2026-09-20T23:00:00Z"
    base["game_type_code"]=game_type;base["feature_cutoff_timestamp_utc"]="2026-09-20T17:00:00Z";base["feature_history_max_timestamp_utc"]="2026-09-19T23:00:00Z"
    base["goalie_eligibility_state"]="ROSTER_ELIGIBLE";base["roster_source_timestamp_utc"]="2026-09-20T16:00:00Z"
    base["population_contract"]="COMPLETE_SCORER_ELIGIBLE";base["scorer_eligible"]=True;base["goalie_aliases"]=["A Goalie","B Alpha","B Goalie","B Bravo"]
    base["expected_complete_population_rows"]=len(base)
    games=pd.DataFrame([
        {"canonical_season":2026,"slate_date":"2026-09-20","game_id":9001,"home_team":"AAA","away_team":"BBB","scheduled_start_time_utc":"2026-09-20T23:00:00Z","game_type_code":game_type,"provider_event_id":"event1"},
        {"canonical_season":2026,"slate_date":"2026-09-20","game_id":9002,"home_team":"CCC","away_team":"DDD","scheduled_start_time_utc":"2026-09-20T23:00:00Z","game_type_code":game_type,"provider_event_id":"event2"},
    ])
    games_path=tmp_path/"games.csv";goalies_path=tmp_path/"goalies.csv";games.to_csv(games_path,index=False);base.to_csv(goalies_path,index=False)
    parent=manifest(tmp_path,games_path,goalies_path)
    def book(key,names,status="ACTIVE",stamp="2026-09-20T19:59:00Z"):
        outcomes=[]
        for name in names:
            outcomes += [{"name":"Over","description":name,"point":24.5,"price":-110,"last_update":stamp,"status":status},
                         {"name":"Under","description":name,"point":24.5,"price":-110,"last_update":stamp,"status":status}]
        return {"key":key,"title":key,"markets":[{"key":"player_total_saves","last_update":stamp,"outcomes":outcomes}]}
    payload={"capture_timestamp_utc":"2026-09-20T20:00:00Z","provider_response":[
        {"id":"event1","home_team":"AAA","away_team":"BBB","commence_time":"2026-09-20T23:00:00Z","bookmakers":[book("book1",["Alpha Goalie","Backup Alpha"]),book("book2",["Alpha Goalie"]) ]},
        {"id":"event2","home_team":"CCC","away_team":"DDD","commence_time":"2026-09-20T23:00:00Z","bookmakers":[book("book1",["Bravo Goalie"]),book("book2",["Bravo Goalie"]) ]},
    ]}
    payload_path=tmp_path/"payload.json";payload_path.write_text(json.dumps(payload))
    quote=capture_run(payload_json=payload_path,games_csv=games_path,goalies_csv=goalies_path,parent_manifest=parent,output_root=tmp_path/"quotes",slate_date="2026-09-20",run_timestamp_utc="2026-09-20T20:00:00Z",run_type="MIDDAY")
    return games_path,goalies_path,parent,quote,base


def test_historical_byte_parity_and_bounded_start_amendment():
    assert shadow.verify_historical_parity()["status"]=="EXACT_BYTE_PARITY"
    result=shadow.verify_operational_amendment()
    assert result["start_prob"]==1.0
    assert result["max_probability_delta_percentage_points"]==pytest.approx(0.002600557877119325)
    assert shadow.verify_policy_c_historical_fixture()=={"status":"EXACT_CERTIFIED_POLICY_C_POPULATION_PARITY","universe_team_games":706,"selected_team_games":544,"mismatches":2,"mismatch_rate":pytest.approx(2/544)}


def test_full_population_scores_before_exact_policy_c_gate(tmp_path):
    games,goalies,parent,quote,source=fixture(tmp_path)
    run=shadow.run_shadow(game_spine_csv=games,game_spine_manifest=parent,goalie_inputs_csv=goalies,goalie_inputs_manifest=parent,quote_run_dir=quote,output_root=tmp_path/"shadow",slate_date="2026-09-20",run_timestamp_utc="2026-09-20T20:00:00Z",run_type="MIDDAY")
    meta=json.loads((run/"run_metadata.json").read_text());assert meta["P"]==52 and meta["C"]==meta["U"]==meta["E"]==0
    p=pd.read_csv(run/"complete_prediction_population.csv");m=pd.read_csv(run/"market_qualified_population.csv")
    assert p.start_prob_operational_input.eq(1).all() and p.export_status.eq("SHADOW_PREDICTION_EXPORT").all()
    assert len(m)==2 and set(m.goalie_id)=={101,201};assert m.prob_over.equals(p.merge(m[["game_id","goalie_id","line"]],on=["game_id","goalie_id","line"]).prob_over)
    decisions=pd.read_csv(run/"policy_c_team_game_decisions.csv")
    assert decisions.loc[(decisions.game_id==9001)&(decisions.team=="AAA"),"selected_goalie_id"].iloc[0]==101
    assert decisions.loc[(decisions.game_id==9001)&(decisions.team=="BBB"),"reason"].iloc[0]=="INSUFFICIENT_MULTIBOOK_AGREEMENT"
    with pytest.raises(RuntimeError,match="PRE_SCORING_MARKET_FILTER_FORBIDDEN"):
        shadow.run_shadow(game_spine_csv=games,game_spine_manifest=parent,goalie_inputs_csv=goalies,goalie_inputs_manifest=parent,quote_run_dir=quote,output_root=tmp_path/"other",slate_date="2026-09-20",run_timestamp_utc="2026-09-20T20:00:00Z",run_type="MIDDAY",pre_scoring_population="MARKET_FILTERED")


def test_capture_rejects_alias_ambiguity_post_start_unavailable_and_unknown_type(tmp_path):
    games,goalies,_,_,base=fixture(tmp_path)
    g=pd.read_csv(games);players=base.copy();players.loc[1,"goalie_aliases"]="A Goalie"
    def payload(name,status="ACTIVE",stamp="2026-09-20T19:00:00Z"):
        return [{"id":"event1","home_team":"AAA","away_team":"BBB","commence_time":"2026-09-20T23:00:00Z","bookmakers":[{"key":"b","markets":[{"key":"player_total_saves","outcomes":[{"name":"Over","description":name,"point":24.5,"price":-110,"status":status,"last_update":stamp}]}]}]}]
    q,_=normalize_quotes(payload("A Goalie"),g,players,slate_date="2026-09-20",run_id="r",run_type="MIDDAY",run_timestamp_utc="2026-09-20T20:00:00Z",capture_timestamp_utc="2026-09-20T20:00:00Z",raw_payload_sha256="x")
    assert q.iloc[0].quote_qualification_status=="GOALIE_IDENTITY_AMBIGUOUS"
    q,_=normalize_quotes(payload("Alpha Goalie",stamp="2026-09-20T23:01:00Z"),g,base,slate_date="2026-09-20",run_id="r",run_type="MIDDAY",run_timestamp_utc="2026-09-20T23:02:00Z",capture_timestamp_utc="2026-09-20T23:02:00Z",raw_payload_sha256="x")
    assert q.iloc[0].quote_qualification_status=="POST_START_INVALID"
    q,_=normalize_quotes(payload("Alpha Goalie",status="SUSPENDED"),g,base,slate_date="2026-09-20",run_id="r",run_type="MIDDAY",run_timestamp_utc="2026-09-20T20:00:00Z",capture_timestamp_utc="2026-09-20T20:00:00Z",raw_payload_sha256="x")
    assert q.iloc[0].quote_qualification_status=="UNAVAILABLE_STATUS"
    g.loc[0,"game_type_code"]=9
    q,_=normalize_quotes(payload("Alpha Goalie"),g,base,slate_date="2026-09-20",run_id="r",run_type="MIDDAY",run_timestamp_utc="2026-09-20T20:00:00Z",capture_timestamp_utc="2026-09-20T20:00:00Z",raw_payload_sha256="x")
    assert q.iloc[0].quote_qualification_status=="WRONG_OR_UNKNOWN_GAME_TYPE"


def test_actual_state_rejected_from_prediction_and_nonstarter_not_loss(tmp_path):
    games,goalies,parent,quote,_=fixture(tmp_path,game_type=2)
    polluted=pd.read_csv(goalies);polluted["actual_start_flag"]=True;bad=tmp_path/"polluted.csv";polluted.to_csv(bad,index=False);bad_manifest=manifest(tmp_path,bad,name="BAD_SHA256SUMS")
    with pytest.raises(RuntimeError,match="ACTUAL_OR_STARTER_STATE_LEAKAGE"):
        shadow.run_shadow(game_spine_csv=games,game_spine_manifest=parent,goalie_inputs_csv=bad,goalie_inputs_manifest=bad_manifest,quote_run_dir=quote,output_root=tmp_path/"bad",slate_date="2026-09-20",run_timestamp_utc="2026-09-20T20:00:00Z",run_type="MIDDAY")
    run=shadow.run_shadow(game_spine_csv=games,game_spine_manifest=parent,goalie_inputs_csv=goalies,goalie_inputs_manifest=parent,quote_run_dir=quote,output_root=tmp_path/"good",slate_date="2026-09-20",run_timestamp_utc="2026-09-20T20:00:00Z",run_type="MIDDAY")
    outcomes=pd.DataFrame([{"canonical_season":2026,"slate_date":"2026-09-20","game_id":9001,"goalie_id":101,"actual_start_flag":False,"goalie_participation_state":"RELIEF_APPEARANCE","official_saves":7,"outcome_source_timestamp_utc":"2026-09-21T03:00:00Z"},{"canonical_season":2026,"slate_date":"2026-09-20","game_id":9002,"goalie_id":201,"actual_start_flag":True,"goalie_participation_state":"STARTED","official_saves":26,"outcome_source_timestamp_utc":"2026-09-21T03:00:00Z"}])
    op=tmp_path/"outcomes.csv";outcomes.to_csv(op,index=False);grade=shadow.grade_shadow(shadow_run_dir=run,outcomes_csv=op,output_root=tmp_path/"grades")
    scored=pd.read_csv(grade/"shadow_grades.csv")
    assert scored.loc[scored.goalie_id.eq(101),"observed_over"].isna().all()
    assert scored.loc[scored.goalie_id.eq(101),"grading_status"].eq("NONSTARTER_EXCLUDED_FROM_CONDITIONAL_EVALUATION").all()


def test_preseason_has_no_numeric_evaluation_targets(tmp_path):
    games,goalies,parent,quote,_=fixture(tmp_path,game_type=1)
    run=shadow.run_shadow(game_spine_csv=games,game_spine_manifest=parent,goalie_inputs_csv=goalies,goalie_inputs_manifest=parent,quote_run_dir=quote,output_root=tmp_path/"shadow",slate_date="2026-09-20",run_timestamp_utc="2026-09-20T20:00:00Z",run_type="MIDDAY")
    outcomes=pd.DataFrame([{"canonical_season":2026,"slate_date":"2026-09-20","game_id":9001,"goalie_id":101,"actual_start_flag":True,"goalie_participation_state":"STARTED","official_saves":30,"outcome_source_timestamp_utc":"2026-09-21T03:00:00Z"},{"canonical_season":2026,"slate_date":"2026-09-20","game_id":9002,"goalie_id":201,"actual_start_flag":True,"goalie_participation_state":"STARTED","official_saves":26,"outcome_source_timestamp_utc":"2026-09-21T03:00:00Z"}])
    op=tmp_path/"outcomes.csv";outcomes.to_csv(op,index=False);grade=shadow.grade_shadow(shadow_run_dir=run,outcomes_csv=op,output_root=tmp_path/"grades")
    metadata=json.loads((grade/"grade_metadata.json").read_text());assert metadata["preseason_numeric_targets"]==0
    assert pd.read_csv(grade/"shadow_grades.csv").grading_status.eq("PRESEASON_NON_EVALUATION").all()


def test_model_hash_drift_and_create_only(tmp_path,monkeypatch):
    identity=shadow.frozen_identity();bad=dict(identity);bad["coefficient_sha256"]="0"*64;path=tmp_path/"identity.json";path.write_text(json.dumps(bad));monkeypatch.setattr(shadow,"IDENTITY_PATH",path)
    with pytest.raises(RuntimeError,match="COEFFICIENT_HASH_DRIFT"):shadow.verify_frozen_identity()


def test_midday_final_are_distinct_and_duplicate_is_blocked(tmp_path):
    games,goalies,parent,midday,_=fixture(tmp_path)
    midday_hash=sha256_file(midday/"SHA256SUMS")
    payload=tmp_path/"payload.json"
    with pytest.raises(FileExistsError,match="OVERWRITE_ATTEMPT_BLOCKED"):
        capture_run(payload_json=payload,games_csv=games,goalies_csv=goalies,parent_manifest=parent,output_root=tmp_path/"quotes",slate_date="2026-09-20",run_timestamp_utc="2026-09-20T20:00:00Z",run_type="MIDDAY")
    final=capture_run(payload_json=payload,games_csv=games,goalies_csv=goalies,parent_manifest=parent,output_root=tmp_path/"quotes",slate_date="2026-09-20",run_timestamp_utc="2026-09-20T20:00:00Z",run_type="FINAL_PREGAME")
    assert midday!=final and midday.exists() and final.exists()
    assert sha256_file(midday/"SHA256SUMS")==midday_hash


def test_morning_adds_prerequisite_only_and_sentinel_enforces_contract(tmp_path):
    morning=subprocess.run([sys.executable,"backend/nhl/scripts/run_nhl_morning_orchestration.py","--slate-date","2026-09-20","--fixture-scenario","nonempty","--output-root",str(tmp_path/"morning")],capture_output=True,text=True)
    assert morning.returncode==0
    health=list((tmp_path/"morning").rglob("morning_health.json"));assert len(health)==1
    data=json.loads(health[0].read_text());assert data["saves_prerequisite_readiness"]=="READY_FOR_IMMUTABLE_SNAPSHOT"
    assert data["downstream"]["SAVES_MORNING_PREREQUISITES_READY"] is True
    assert not any("capture" in x["stage_id"] or "score" in x["stage_id"] for x in data["stages"])
    saves={x:True for x in ["parents_current","complete_population_scored_before_market_gate","quote_coverage_visible","multi_goalie_state_visible","multibook_gate_applied","aliases_unambiguous","strictly_pregame","single_batch_preprocessing","actual_starter_postgame_only","nonstarters_excluded_from_evaluation","run_bound_inputs_only","identity_current"]}
    saves.update(start_prob_values=[1.0],canonical_season=2026,game_type_codes=[1])
    payload={"sentinel_timestamp_utc":"2026-09-20T20:00:00Z","slate_health":{"fetch_timestamp_utc":"2026-09-20T19:00:00Z","completion_status":"READY","downstream_ready":True},"market":{"eligible":4,"covered":2,"qualified":2,"post_start_quotes":0},"populations":{"market_qualified":2},"saves":saves}
    inp=tmp_path/"sentinel.json";inp.write_text(json.dumps(payload));command=[sys.executable,"backend/nhl/scripts/run_nhl_live_failure_sentinel.py","--phase","MIDDAY","--slate-date","2026-09-20","--run-id","good","--input-json",str(inp),"--output-root",str(tmp_path/"sentinel")]
    good=subprocess.run(command,capture_output=True,text=True);assert good.returncode==0
    report=json.loads((tmp_path/"sentinel/2026-09-20/good/nhl_live_failure_sentinel.json").read_text());assert next(x for x in report["sentinels"] if x["sentinel"]=="saves_shadow_contract")["state"]=="PASS"
    payload["saves"]["start_prob_values"]=[0.0];inp.write_text(json.dumps(payload));command[command.index("good")]="bad"
    bad=subprocess.run(command,capture_output=True,text=True);assert bad.returncode==2


def test_no_usable_market_is_valid_zero_m_shadow_run(tmp_path):
    games,goalies,parent,_,_=fixture(tmp_path)
    empty=tmp_path/"empty_payload.json";empty.write_text(json.dumps({"capture_timestamp_utc":"2026-09-20T20:00:00Z","provider_response":[]}))
    quote=capture_run(payload_json=empty,games_csv=games,goalies_csv=goalies,parent_manifest=parent,output_root=tmp_path/"empty_quotes",slate_date="2026-09-20",run_timestamp_utc="2026-09-20T20:00:00Z",run_type="MIDDAY")
    run=shadow.run_shadow(game_spine_csv=games,game_spine_manifest=parent,goalie_inputs_csv=goalies,goalie_inputs_manifest=parent,quote_run_dir=quote,output_root=tmp_path/"shadow",slate_date="2026-09-20",run_timestamp_utc="2026-09-20T20:00:00Z",run_type="MIDDAY")
    metadata=json.loads((run/"run_metadata.json").read_text());assert metadata["P"]==52 and metadata["M"]==0
    decisions=pd.read_csv(run/"policy_c_team_game_decisions.csv");assert decisions.reason.eq("NO_SAVES_MARKET").all()


def test_population_collapse_fails_before_scoring(tmp_path):
    games,goalies,parent,quote,_=fixture(tmp_path);collapsed=pd.read_csv(goalies).iloc[:-1];bad=tmp_path/"collapsed.csv";collapsed.to_csv(bad,index=False);bad_manifest=manifest(tmp_path,bad,name="COLLAPSED_SHA256SUMS")
    with pytest.raises(RuntimeError,match="PRE_SCORING_POPULATION_COLLAPSE"):
        shadow.run_shadow(game_spine_csv=games,game_spine_manifest=parent,goalie_inputs_csv=bad,goalie_inputs_manifest=bad_manifest,quote_run_dir=quote,output_root=tmp_path/"blocked",slate_date="2026-09-20",run_timestamp_utc="2026-09-20T20:00:00Z",run_type="MIDDAY")
