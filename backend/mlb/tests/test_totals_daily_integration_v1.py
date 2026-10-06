from __future__ import annotations

import hashlib
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
from backend.mlb.scripts import attach_mlb_totals_shadow_existing_markets_v1 as market_attachment
from backend.mlb.scripts.attach_mlb_totals_shadow_existing_markets_v1 import run as attach_markets
from backend.mlb.tests.test_mlb_2026_totals_phase_gating_v1 import StubAuthority, record
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


def _phase_authority(*game_pks: int) -> StubAuthority:
    return StubAuthority({game_pk: record(game_pk, "R", "REGULAR_SEASON") for game_pk in game_pks})


def test_auto_window_0530_is_primary_and_0830_or_later_retries_missing():
    assert daily.resolve_mode("auto", "2026-08-07T12:30:00Z") == daily.PRIMARY_SCORE
    assert daily.resolve_mode("auto", "2026-08-07T15:30:00Z") == daily.SCORE_MISSING
    assert daily.resolve_mode("auto", "2026-08-07T16:30:00Z") == daily.SCORE_MISSING
    assert daily.resolve_mode("auto", "2026-08-07T18:00:00Z") == daily.SCORE_MISSING


def test_daily_0830_scores_and_later_runs_retry_missing(monkeypatch, tmp_path):
    today = datetime.now(ZoneInfo("America/New_York")).date().isoformat(); calls=[]
    schedule_path=tmp_path/"schedule.json";schedule_path.write_text("{}")
    schedule_hash=hashlib.sha256(schedule_path.read_bytes()).hexdigest()
    monkeypatch.setattr(daily,"score",lambda *args,**kwargs:calls.append(("score",args[0])) or {"rows":1,"new_rows":1})
    monkeypatch.setattr(daily,"attach_markets",lambda *args,**kwargs:calls.append(("markets",args[0])) or {"predictions_with_market":0,"market_unavailable_predictions":1})
    monkeypatch.setattr(daily,"grade",lambda *args,**kwargs:pytest.fail("no pending grade dates expected"))
    result=daily.run(today,"2000-01-01","auto","2026-08-07T15:30:00Z",tmp_path,tmp_path/"p.sqlite3",tmp_path/"m.sqlite3",
        schedule_source_path=schedule_path,expected_schedule_source_sha256=schedule_hash,phase_authority=_phase_authority())
    assert result["resolved_mode"]==daily.SCORE_MISSING and calls==[("score",today),("markets",today)]
    calls.clear()
    result=daily.run(today,"2000-01-01","auto","2026-08-07T18:00:00Z",tmp_path,tmp_path/"p2.sqlite3",tmp_path/"m2.sqlite3",
        schedule_source_path=schedule_path,expected_schedule_source_sha256=schedule_hash,phase_authority=_phase_authority())
    assert result["resolved_mode"]==daily.SCORE_MISSING and calls==[("score",today),("markets",today)]


def test_daily_0530_is_primary_scoring_pass(monkeypatch, tmp_path):
    today=datetime.now(ZoneInfo("America/New_York")).date().isoformat(); calls=[]
    schedule_path=tmp_path/"schedule.json";schedule_path.write_text("{}")
    schedule_hash=hashlib.sha256(schedule_path.read_bytes()).hexdigest()
    monkeypatch.setattr(daily,"score",lambda *args,**kwargs:calls.append(("score",args[0])) or {"rows":1,"new_rows":1})
    monkeypatch.setattr(daily,"attach_markets",lambda *args,**kwargs:calls.append(("markets",args[0])) or {"predictions_with_market":0,"market_unavailable_predictions":1})
    result=daily.run(today,"2000-01-01","auto","2026-08-07T12:30:00Z",tmp_path,tmp_path/"p.sqlite3",tmp_path/"m.sqlite3",
        schedule_source_path=schedule_path,expected_schedule_source_sha256=schedule_hash,phase_authority=_phase_authority())
    assert result["resolved_mode"]==daily.PRIMARY_SCORE and calls==[("score",today),("markets",today)]


def test_existing_identity_is_bypassed_before_context_reconstruction(monkeypatch, tmp_path):
    ledger=tmp_path/"p.sqlite3";connection=connect_ledger(ledger);add_prediction(connection,"2026-08-07",10)
    monkeypatch.setattr(shadow,"verified_totals_phase_authority",lambda:_phase_authority(10))
    schedule_path=tmp_path/"schedule.json";schedule_path.write_text("{}")
    schedule_hash=hashlib.sha256(schedule_path.read_bytes()).hexdigest()
    monkeypatch.setattr(shadow,"load_retained_schedule",lambda *_:({},"2026-08-07T15:00:00Z",schedule_hash))
    monkeypatch.setattr(shadow,"normalize_schedule",lambda *_:[{"game_pk":10,"game_date":"2026-08-07"}])
    monkeypatch.setattr(shadow,"build_history",lambda :{})
    monkeypatch.setattr(shadow,"dynamic_environment",lambda *_:{})
    monkeypatch.setattr(shadow,"market_inventory",lambda *_:([],[]))
    monkeypatch.setattr(shadow,"attach_context",lambda *_:pytest.fail("existing identity context reconstructed"))
    result=shadow.run("2026-08-07",tmp_path/"out",ledger,
        phase_authority=_phase_authority(10),schedule_source_path=schedule_path,
        expected_schedule_source_sha256=schedule_hash)
    assert result["new_rows"]==0 and result["attempts"][0]["context_action"]=="EXISTING_CONTEXT_NOT_RECONSTRUCTED"


def test_market_unavailable_does_not_block_or_mutate_prediction(monkeypatch,tmp_path):
    pred=tmp_path/"p.sqlite3";connection=connect_ledger(pred);row=add_prediction(connection,"2026-08-07",10)
    monkeypatch.setattr(market_attachment,"verified_totals_phase_authority",lambda:_phase_authority(10))
    market=tmp_path/"m.sqlite3";connect_market_ledger(market)
    before=rows_for_date(connection,"2026-08-07")
    result=attach_markets("2026-08-07",tmp_path/"out",pred,market)
    assert result["market_unavailable_predictions"]==1 and result["predictions_with_market"]==0
    assert rows_for_date(connection,"2026-08-07")==before==[row]


def _odds_event(*, total=8.5):
    return {"away_team":"Away","home_team":"Home","commence_time":"2026-08-08T23:00:00Z","bookmakers":[{
        "key":"pinnacle","title":"Pinnacle","markets":[{"key":"totals","last_update":"2026-08-08T12:00:00Z",
        "outcomes":[{"name":"Over","point":total,"price":-110},{"name":"Under","point":total,"price":-104}]}]}]}


def test_market_inventory_accepts_retained_list_shaped_pinnacle(monkeypatch, tmp_path):
    retained=ROOT/"backend/mlb/exports/odds_history/2026-08-08/odds_mlb_pinnacle_main_markets__local_daily_20260808T123001Z.json"
    payload=json.loads(retained.read_text())
    events,captured=shadow._normalize_odds_events(payload,retained)
    assert len(events)==15 and captured is None and all(isinstance(event,dict) for event in events)


def test_market_inventory_accepts_wrapped_fixture_and_preserves_broad_totals(monkeypatch, tmp_path):
    root=tmp_path/"odds"; day=root/"2026-08-08"; day.mkdir(parents=True)
    (day/"broad.json").write_text(json.dumps({"captured_at_utc":"2026-08-08T12:01:00Z","events":[_odds_event(total=9.0)]}))
    monkeypatch.setattr(shadow,"ROOT",tmp_path); monkeypatch.setattr(shadow,"ODDS_ROOT",root)
    rows,files=shadow.market_inventory("2026-08-08")
    assert len(rows)==1 and rows[0]["total_line"]==9.0 and rows[0]["provider_key"]=="pinnacle"
    assert files[0]["captured_at_utc"]=="2026-08-08T12:01:00Z" and files[0]["game_totals_markets"]==1


@pytest.mark.parametrize("payload", [[], {"events":[]}])
def test_market_inventory_accepts_empty_event_list(payload, tmp_path):
    events,captured=shadow._normalize_odds_events(payload,tmp_path/"empty.json")
    assert events==[] and captured is None


def test_market_inventory_rejects_malformed_list_member(tmp_path):
    with pytest.raises(ValueError,match="ODDS_EVENT_NOT_OBJECT.*index=1"):
        shadow._normalize_odds_events([_odds_event(),"bad"],tmp_path/"bad.json")


@pytest.mark.parametrize("payload", [None,"bad",7])
def test_market_inventory_rejects_unexpected_scalar_root(payload, tmp_path):
    with pytest.raises(ValueError,match="UNEXPECTED_ODDS_JSON_ROOT"):
        shadow._normalize_odds_events(payload,tmp_path/"bad.json")


def test_list_inventory_is_unique_and_does_not_access_outcomes_or_public_surface(monkeypatch, tmp_path):
    root=tmp_path/"odds"; day=root/"2026-08-08"; day.mkdir(parents=True)
    (day/"pinnacle.json").write_text(json.dumps([_odds_event()]))
    monkeypatch.setattr(shadow,"ROOT",tmp_path); monkeypatch.setattr(shadow,"ODDS_ROOT",root)
    rows,files=shadow.market_inventory("2026-08-08")
    identities={(r["away_team"],r["home_team"],r["provider_key"],r["total_line"],r["snapshot_timestamp_utc"]) for r in rows}
    assert len(rows)==len(identities)==1 and files[0]["event_count"]==1
    assert "outcome" not in rows[0] and "result" not in rows[0]
    assert "public" not in shadow.market_inventory.__name__ and MONEYLINE.exists()


def test_partial_grading_appends_only_official_final(monkeypatch, tmp_path):
    pred=tmp_path/"p.sqlite3";connection=connect_ledger(pred);add_prediction(connection,"2026-08-06",1);add_prediction(connection,"2026-08-06",2)
    market=tmp_path/"m.sqlite3";connect_market_ledger(market)
    monkeypatch.setattr(grader,"verified_totals_phase_authority",lambda:_phase_authority(1,2))
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


def test_official_final_rejects_conflicting_final_score_sources(monkeypatch, tmp_path):
    source_dir=tmp_path/"artifacts/analysis/mlb/player_stats_completeness/2026-09-20/game_824462/sources"
    source_dir.mkdir(parents=True)
    for index,score in enumerate((1,2)):
        (source_dir/f"game_824462_live_feed_{index}.json").write_text(
            json.dumps(_official_feed(824462,home_runs=score)))
    monkeypatch.setattr(grader,"ROOT",tmp_path)
    monkeypatch.setattr(grader,"OFFICIAL_ROOT",tmp_path/"artifacts/analysis/mlb/player_stats_completeness")
    with pytest.raises(RuntimeError,match="OFFICIAL_FINAL_SOURCE_CONFLICT_824462_2"):
        grader.official_final("2026-09-20",824462)


def _official_feed(game_pk: int, *, away_runs: int = 9, home_runs: int = 1):
    return {
        "gamePk": game_pk,
        "gameData": {
            "datetime": {"officialDate": "2026-09-20"},
            "status": {"abstractGameState": "Final", "detailedState": "Final",
                       "codedGameState": "F", "statusCode": "F"},
        },
        "liveData": {
            "linescore": {
                "teams": {"away": {"runs": away_runs}, "home": {"runs": home_runs}},
                "innings": [{"num": n, "away": {"runs": 1 if n == 9 else 0},
                             "home": {"runs": 0}} for n in range(1, 10)],
            },
            "plays": {"allPlays": [{"playEvents": [{"pitchData": {"breaks": {
                "spinDirection": 216, "spinRate": 2059,
            }}}]}]},
        },
    }


def test_official_final_reconciles_pitch_tracking_revisions_at_game_identity(monkeypatch, tmp_path):
    source_dir=tmp_path/"artifacts/analysis/mlb/player_stats_completeness/2026-09-20/game_824462/sources"
    source_dir.mkdir(parents=True)
    first=_official_feed(824462)
    second=json.loads(json.dumps(first))
    second["liveData"]["plays"]["allPlays"][0]["playEvents"][0]["pitchData"]["breaks"].update(
        {"spinDirection":223,"spinRate":2099})
    for payload in (first,second):
        raw=json.dumps(payload,sort_keys=True).encode()
        digest=hashlib.sha256(raw).hexdigest()
        (source_dir/f"game_824462_live_feed_{digest}.json").write_bytes(raw)
    monkeypatch.setattr(grader,"ROOT",tmp_path)
    monkeypatch.setattr(grader,"OFFICIAL_ROOT",tmp_path/"artifacts/analysis/mlb/player_stats_completeness")

    result=grader.official_final("2026-09-20",824462)

    assert result["official_final_total"]==10
    assert result["regulation_nine_total"]==1
    assert result["official_source_equivalence_count"]==2
    assert len(result["official_equivalent_source_hashes"])==2
    assert result["official_source_hash"]==min(result["official_equivalent_source_hashes"])


@pytest.mark.parametrize("change", [
    lambda feed: feed["liveData"]["linescore"]["teams"]["home"].update({"runs":2}),
    lambda feed: feed["gameData"]["datetime"].update({"officialDate":"2026-09-21"}),
    lambda feed: feed["gameData"]["status"].update({"codedGameState":"D","statusCode":"D"}),
])
def test_official_final_still_fails_closed_on_conflicting_terminal_facts(monkeypatch,tmp_path,change):
    source_dir=tmp_path/"artifacts/analysis/mlb/player_stats_completeness/2026-09-20/game_824462/sources"
    source_dir.mkdir(parents=True)
    feeds=[_official_feed(824462),_official_feed(824462)]
    change(feeds[1])
    for index,payload in enumerate(feeds):
        (source_dir/f"game_824462_live_feed_{index}.json").write_text(json.dumps(payload))
    monkeypatch.setattr(grader,"ROOT",tmp_path)
    monkeypatch.setattr(grader,"OFFICIAL_ROOT",tmp_path/"artifacts/analysis/mlb/player_stats_completeness")
    with pytest.raises(RuntimeError):
        grader.official_final("2026-09-20",824462)


def test_shadow_contract_has_no_public_ev_or_wager_authority():
    hook=(ROOT/"bin/mlb_totals_prospective_shadow_daily_hook.sh").read_text()
    lifecycle=(ROOT/"backend/mlb/scripts/run_mlb_totals_prospective_shadow_daily_v1.py").read_text()
    assert "MLB_PUBLIC" not in hook
    assert "UNAVAILABLE_SHADOW_ONLY" in lifecycle
    forbidden=("wager recommendation","staking output","ev output")
    assert not any(value in (hook+lifecycle).casefold() for value in forbidden)
    assert hashlib.sha256(MONEYLINE.read_bytes()).hexdigest()=="afc257a5ede1c5bc352dcb1e990b710272d472cd62d2dadfb7dafb7254b35722"
    assert '"PREGAME_CUTOFF_FAILED"' in (ROOT/"backend/mlb/scripts/run_mlb_totals_prospective_shadow_v1.py").read_text()
