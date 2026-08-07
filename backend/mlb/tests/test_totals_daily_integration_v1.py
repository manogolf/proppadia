from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from backend.mlb.markets.full_game_total_capture_v1 import connect_ledger as connect_market_ledger
from backend.mlb.scripts import grade_mlb_totals_prospective_shadow_v1 as grader
from backend.mlb.scripts import run_mlb_totals_prospective_shadow_daily_v1 as daily
from backend.mlb.scripts import run_mlb_totals_prospective_shadow_v1 as shadow
from backend.mlb.scripts.attach_mlb_totals_shadow_existing_markets_v1 import run as attach_markets
from backend.mlb.totals_predictions.prospective_shadow_v1 import (
    append_prediction_with_context, connect_ledger, counts, outcomes_for_date, payload_hash, rows_for_date,
)

ROOT = Path(__file__).resolve().parents[3]
MODEL_HASH = "fb1c730d295ce28d90436ec95cb71d1a81813679de8364e838255111917498ac"
MONEYLINE = ROOT / "backend/mlb/config/public_game_predictions/MLB_GAME_PYTHAGOREAN_LOG5_V1.json"


def prediction(game_date: str, game_pk: int, *, start="2026-08-07T23:00:00Z"):
    context = {"model_features": {"league_total": 8.5}, "away_starter_state": {}, "home_starter_state": {},
               "park_state": {}, "dynamic_league_environment": {}}
    row = {"experiment":"MLB_TOTALS_PROSPECTIVE_SHADOW_V1","game_date":game_date,"game_pk":game_pk,
        "prediction_snapshot_class":"DAILY_DESIGNATED_PREGAME","scheduled_start_utc":start,
        "prediction_timestamp_utc":"2026-08-07T14:00:00Z","model_hash":MODEL_HASH,
        "feature_state_hash":payload_hash(context),"schedule_source_sha256":"a"*64,
        "market_source_sha256":None,"expected_total":8.5,"context_quality_state":"TOTALS_CONTEXT_COMPLETE",
        "away_team":"Away","home_team":"Home","away_probable_starter_name":"Away Starter",
        "home_probable_starter_name":"Home Starter","venue_name":"Park","park_factor":1.0,
        "model_version":"DIRECT_NEGATIVE_BINOMIAL","grading_status":"UNGRADED_OUTCOME_SEPARATE_LEDGER",
        "dynamic_league_environment":{},"total_line":None,"p_over_market_line":None,"p_under_market_line":None}
    return row, context


def add_prediction(connection, game_date, game_pk):
    row, context = prediction(game_date, game_pk)
    assert append_prediction_with_context(connection, row, context) == ("APPENDED_NEW", "APPENDED_NEW")
    return row


def test_auto_window_0530_is_primary_and_0830_or_later_retries_missing():
    assert daily.resolve_mode("auto", "2026-08-07T12:30:00Z") == daily.PRIMARY_SCORE
    assert daily.resolve_mode("auto", "2026-08-07T15:30:00Z") == daily.SCORE_MISSING
    assert daily.resolve_mode("auto", "2026-08-07T16:30:00Z") == daily.SCORE_MISSING
    assert daily.resolve_mode("auto", "2026-08-07T18:00:00Z") == daily.SCORE_MISSING


def test_daily_0830_scores_and_later_runs_retry_missing(monkeypatch, tmp_path):
    today = datetime.now(ZoneInfo("America/New_York")).date().isoformat(); calls=[]
    monkeypatch.setattr(daily,"score",lambda *args:calls.append(("score",args[0])) or {"rows":1,"new_rows":1})
    monkeypatch.setattr(daily,"attach_markets",lambda *args:calls.append(("markets",args[0])) or {"predictions_with_market":0,"market_unavailable_predictions":1})
    monkeypatch.setattr(daily,"grade",lambda *args,**kwargs:pytest.fail("no pending grade dates expected"))
    result=daily.run(today,"2000-01-01","auto","2026-08-07T15:30:00Z",tmp_path,tmp_path/"p.sqlite3",tmp_path/"m.sqlite3")
    assert result["resolved_mode"]==daily.SCORE_MISSING and calls==[("score",today),("markets",today)]
    calls.clear()
    result=daily.run(today,"2000-01-01","auto","2026-08-07T18:00:00Z",tmp_path,tmp_path/"p2.sqlite3",tmp_path/"m2.sqlite3")
    assert result["resolved_mode"]==daily.SCORE_MISSING and calls==[("score",today),("markets",today)]


def test_daily_0530_is_primary_scoring_pass(monkeypatch, tmp_path):
    calls=[]
    monkeypatch.setattr(daily,"score",lambda *args:calls.append(("score",args[0])) or {"rows":1,"new_rows":1})
    monkeypatch.setattr(daily,"attach_markets",lambda *args:calls.append(("markets",args[0])) or {"predictions_with_market":0,"market_unavailable_predictions":1})
    result=daily.run("2026-08-07","2000-01-01","auto","2026-08-07T12:30:00Z",tmp_path,tmp_path/"p.sqlite3",tmp_path/"m.sqlite3")
    assert result["resolved_mode"]==daily.PRIMARY_SCORE and calls==[("score","2026-08-07"),("markets","2026-08-07")]


def test_existing_identity_is_bypassed_before_context_reconstruction(monkeypatch, tmp_path):
    ledger=tmp_path/"p.sqlite3";connection=connect_ledger(ledger);add_prediction(connection,"2026-08-07",10)
    monkeypatch.setattr(shadow,"fetch_hydrated_schedule",lambda *_:({},"2026-08-07T15:00:00Z","b"*64))
    monkeypatch.setattr(shadow,"normalize_schedule",lambda *_:[{"game_pk":10}])
    monkeypatch.setattr(shadow,"build_history",lambda :{})
    monkeypatch.setattr(shadow,"dynamic_environment",lambda *_:{})
    monkeypatch.setattr(shadow,"market_inventory",lambda *_:([],[]))
    monkeypatch.setattr(shadow,"attach_context",lambda *_:pytest.fail("existing identity context reconstructed"))
    result=shadow.run("2026-08-07",tmp_path/"out",ledger)
    assert result["new_rows"]==0 and result["attempts"][0]["context_action"]=="EXISTING_CONTEXT_NOT_RECONSTRUCTED"


def test_market_unavailable_does_not_block_or_mutate_prediction(tmp_path):
    pred=tmp_path/"p.sqlite3";connection=connect_ledger(pred);row=add_prediction(connection,"2026-08-07",10)
    market=tmp_path/"m.sqlite3";connect_market_ledger(market)
    before=rows_for_date(connection,"2026-08-07")
    result=attach_markets("2026-08-07",tmp_path/"out",pred,market)
    assert result["market_unavailable_predictions"]==1 and result["predictions_with_market"]==0
    assert rows_for_date(connection,"2026-08-07")==before==[row]


def test_partial_grading_appends_only_official_final(monkeypatch, tmp_path):
    pred=tmp_path/"p.sqlite3";connection=connect_ledger(pred);add_prediction(connection,"2026-08-06",1);add_prediction(connection,"2026-08-06",2)
    market=tmp_path/"m.sqlite3";connect_market_ledger(market)
    def final(_date,game):
        if game==2: raise RuntimeError("GAME_NOT_OFFICIALLY_FINAL_2")
        return {"official_final_total":9,"regulation_nine_total":9,"official_source_path":"official.json",
                "official_source_hash":"f"*64,"official_status":"Final"}
    monkeypatch.setattr(grader,"official_final",final)
    result=grader.run("2026-08-06",tmp_path/"out",pred,market,allow_partial=True)
    assert result["new_outcome_rows"]==1 and result["deferred_rows"]==1
    assert len(outcomes_for_date(connection,"2026-08-06"))==1
    repeat=grader.run("2026-08-06",tmp_path/"out",pred,market,allow_partial=True)
    assert repeat["new_outcome_rows"]==0 and len(outcomes_for_date(connection,"2026-08-06"))==1


def test_ambiguous_official_final_sources_remain_fatal(monkeypatch, tmp_path):
    pred=tmp_path/"p.sqlite3";connection=connect_ledger(pred);add_prediction(connection,"2026-08-06",1)
    market=tmp_path/"m.sqlite3";connect_market_ledger(market)
    monkeypatch.setattr(grader,"official_final",lambda *_:(_ for _ in ()).throw(RuntimeError("OFFICIAL_FINAL_SOURCE_COUNT_1_2")))
    with pytest.raises(RuntimeError,match="OFFICIAL_FINAL_SOURCE_COUNT_1_2"):
        grader.run("2026-08-06",tmp_path/"out",pred,market,allow_partial=True)


def test_shadow_contract_has_no_public_ev_or_wager_authority():
    hook=(ROOT/"bin/mlb_totals_prospective_shadow_daily_hook.sh").read_text()
    lifecycle=(ROOT/"backend/mlb/scripts/run_mlb_totals_prospective_shadow_daily_v1.py").read_text()
    assert "MLB_PUBLIC" not in hook
    assert "UNAVAILABLE_SHADOW_ONLY" in lifecycle
    forbidden=("wager recommendation","staking output","ev output")
    assert not any(value in (hook+lifecycle).casefold() for value in forbidden)
    assert hashlib.sha256(MONEYLINE.read_bytes()).hexdigest()=="afc257a5ede1c5bc352dcb1e990b710272d472cd62d2dadfb7dafb7254b35722"
    assert '"PREGAME_CUTOFF_FAILED"' in (ROOT/"backend/mlb/scripts/run_mlb_totals_prospective_shadow_v1.py").read_text()
