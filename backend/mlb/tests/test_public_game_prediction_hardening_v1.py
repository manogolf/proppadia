from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from backend.mlb.public_game_predictions import pythagorean_log5_v1 as model
from backend.mlb.public_game_predictions import durable_store_v1 as durable
from backend.mlb.public_game_predictions.state_v1 import (
    OfficialFinalGame, load_initialization_state, reconstruct_state, scoring_team_states, state_hash,
)
from backend.mlb.scripts.run_mlb_public_game_moneyline_daily_v1 import official_final_from_feed

ROOT=Path(__file__).resolve().parents[3]


def game(pk=1,date='2026-08-05',start='2026-08-05T17:00:00Z',number=1,
         home=110,away=111,hr=5,ar=3,status='Final',effective='2026-08-05T20:59:00Z',
         observed='2026-08-05T21:00:00Z'):
    return OfficialFinalGame(pk,date,start,number,home,away,hr,ar,status,effective,observed,
                             f'official:{pk}',hashlib.sha256(str(pk).encode()).hexdigest())


def snapshot(rows=(),cutoff='2026-08-06T00:00:00Z'):
    return reconstruct_state(rows,prediction_cutoff_utc=cutoff,state_generated_at_utc=cutoff)


def test_01_initialization_is_committed_and_hash_bound():
    state=load_initialization_state()
    assert len(state['teams'])==30 and state['state_through_game_date']=='2026-08-04'


def test_02_chronological_advancement_updates_both_teams():
    state=snapshot([game()]);teams=scoring_team_states(state)
    assert teams[110]['games']==514 and teams[111]['games']==516
    assert state['applied_game_ids']==[1]


def test_03_same_day_doubleheader_order_is_exact():
    second=game(pk=12,start='2026-08-05T20:00:00Z',number=2,observed='2026-08-05T23:30:00Z')
    first=game(pk=11,start='2026-08-05T17:00:00Z',number=1,observed='2026-08-05T20:30:00Z')
    assert snapshot([second,first],cutoff='2026-08-06T01:00:00Z')['applied_game_ids']==[11,12]


@pytest.mark.parametrize('status',['Postponed','Suspended','In Progress','Delayed'])
def test_04_nonfinal_regimes_are_explicitly_unresolved(status):
    state=snapshot([game(status=status)])
    assert not state['applied_game_ids'] and state['unresolved_games'][0]['reason']=='OFFICIAL_STATUS_NOT_FINAL'


def test_05_cutoff_uses_final_effective_time_not_later_fetch_time():
    admitted=snapshot([game(effective='2026-08-05T23:59:00Z',observed='2026-08-06T02:00:00Z')])
    excluded=snapshot([game(effective='2026-08-06T00:00:01Z',observed='2026-08-06T02:00:00Z')])
    assert admitted['applied_game_ids']==[1]
    assert not excluded['applied_game_ids'] and excluded['unresolved_games'][0]['reason']=='FINAL_EFFECTIVE_AFTER_CUTOFF'


def test_06_current_game_outcome_cannot_enter_its_prediction_state():
    state=snapshot([game(pk=99,start='2026-08-06T02:00:00Z',effective='2026-08-06T04:00:00Z',observed='2026-08-06T04:01:00Z')],cutoff='2026-08-06T01:00:00Z')
    assert 99 not in state['applied_game_ids']


def test_07_state_hash_is_deterministic_and_order_invariant():
    a=snapshot([game(pk=2),game(pk=1)]);b=snapshot([game(pk=1),game(pk=2)])
    assert a['state_hash']==b['state_hash']==state_hash(a)


def test_08_conflicting_duplicate_official_game_fails_closed():
    with pytest.raises(model.PublicGamePredictionError,match='CONFLICTING'):
        snapshot([game(pk=1,hr=5),game(pk=1,hr=6)])


def test_08a_canonical_final_hash_excludes_raw_acquisition_lineage():
    first=game(observed='2026-08-05T21:00:00Z')
    second=OfficialFinalGame(**{**first.__dict__,'observed_final_at_utc':'2026-08-06T09:00:00Z',
                                'source_identity':'different/path.json','source_sha256':'f'*64})
    assert first.content_hash==second.content_hash


def test_08b_genuine_score_correction_changes_canonical_hash():
    assert game(hr=5).content_hash!=game(hr=6).content_hash


def test_08c_state_replay_after_cutoff_is_stable():
    row=game(effective='2026-08-05T21:00:00Z',observed='2026-08-06T09:00:00Z')
    early=reconstruct_state([row],prediction_cutoff_utc='2026-08-06T00:00:00Z',state_generated_at_utc='2026-08-06T00:00:01Z')
    later=reconstruct_state([row],prediction_cutoff_utc='2026-08-06T00:00:00Z',state_generated_at_utc='2026-08-07T00:00:00Z')
    assert early['state_hash']==later['state_hash'] and early['applied_game_ids']==[1]


def test_08d_official_feed_uses_last_play_completion_time():
    feed={'gamePk':1,'gameData':{'status':{'abstractGameState':'Final'},
          'datetime':{'officialDate':'2026-08-05','dateTime':'2026-08-05T17:00:00Z'},
          'game':{'gameNumber':1},'teams':{'home':{'id':110},'away':{'id':111}}},
          'liveData':{'linescore':{'teams':{'home':{'runs':5},'away':{'runs':3}}},
          'plays':{'allPlays':[{'about':{'endTime':'2026-08-05T20:59:00.123Z'}}]}}}
    row=official_final_from_feed(feed,observed_at_utc='2026-08-06T01:00:00Z',
                                 source_identity='retained.json',source_sha256='a'*64)
    assert row.official_final_effective_utc=='2026-08-05T20:59:00.123000Z'


def test_08e_durable_final_ignores_raw_hash_change_but_rejects_score_change(monkeypatch):
    db={}
    class Cursor:
        result=None
        def __enter__(self): return self
        def __exit__(self,*args): return False
        def execute(self,sql,params):
            if 'INSERT INTO mlb.public_game_official_finals' in sql:
                key=int(params[0])
                if key in db: self.result=None
                else: db[key]=params[-1];self.result=(key,)
            elif 'SELECT content_sha256' in sql: self.result=(db.get(int(params[0])),)
            else: raise AssertionError(sql)
        def fetchone(self): return self.result
    class Connection:
        def __enter__(self): return self
        def __exit__(self,*args): return False
        def cursor(self): return Cursor()
        def commit(self): pass
    monkeypatch.setattr(durable,'pg_connect',lambda:Connection())
    first=game();second=OfficialFinalGame(**{**first.__dict__,'observed_final_at_utc':'2026-08-06T09:00:00Z',
                                            'source_identity':'new.json','source_sha256':'f'*64})
    assert durable.append_official_finals([first])==1
    assert durable.append_official_finals([second])==0
    with pytest.raises(model.PublicGamePredictionError,match='CORRECTION_REQUIRES_REPLAY'):
        durable.append_official_finals([game(hr=6)])


def test_08f_production_dict_row_final_loader(monkeypatch):
    stamp=datetime(2026,8,5,21,tzinfo=timezone.utc)
    row={'game_pk':1,'game_date':'2026-08-05','scheduled_start_utc':stamp,'game_number':1,
         'home_team_id':110,'away_team_id':111,'home_runs':5,'away_runs':3,'official_status':'Final',
         'official_final_effective_utc':stamp,'observed_final_at_utc':stamp,
         'source_identity':'retained.json','source_sha256':'a'*64}
    class Cursor:
        def __enter__(self): return self
        def __exit__(self,*args): return False
        def execute(self,sql,params):
            assert 'AS home_runs' in sql and 'AS source_identity' in sql
        def fetchall(self): return [row]
    class Connection:
        def __enter__(self): return self
        def __exit__(self,*args): return False
        def cursor(self): return Cursor()
    monkeypatch.setattr(durable,'pg_connect',lambda:Connection())
    loaded=durable.load_official_finals_before('2026-08-06T00:00:00Z')
    assert len(loaded)==1 and loaded[0].home_runs==5 and loaded[0].source_identity=='retained.json'


def test_09_tampered_state_hash_fails_closed():
    state=snapshot();state['teams']['110']['runs_scored']+=1
    with pytest.raises(model.PublicGamePredictionError,match='TEAM_STATE_HASH'):
        scoring_team_states(state)


def test_10_production_scorer_rejects_stale_initialization_fallback():
    payload={'dates':[]}
    with pytest.raises(model.PublicGamePredictionError,match='ADVANCED_TEAM_STATE'):
        model.score_schedule_payload(payload,prediction_timestamp_utc='2026-08-06T00:00:00Z',
                                     source_schedule_hash='a'*64,production_mode=True)


def test_11_migration_has_unique_append_only_and_no_outcome_prediction_columns():
    sql=(ROOT/'backend/mlb/sql/migrations/20260805_create_public_game_moneyline_lifecycle.sql').read_text()
    assert 'PRIMARY KEY (game_date,game_id,model_version,prediction_snapshot_class)' in sql
    assert 'reject_public_game_lifecycle_mutation' in sql
    prediction_block=sql.split('CREATE TABLE IF NOT EXISTS mlb.public_game_moneyline_predictions',1)[1].split('CREATE TABLE IF NOT EXISTS',1)[0]
    assert 'official_winner' not in prediction_block and 'prediction_timestamp_utc < scheduled_start_utc' in prediction_block


def test_12_compact_fixture_reproduces_exact_population_without_analysis_path():
    fixture=ROOT/'backend/mlb/tests/fixtures/public_game_predictions_v1/pythagorean_log5_exact_reproduction.csv.gz'
    assert fixture.exists() and fixture.stat().st_size < 100_000
    candidate_test=(ROOT/'backend/mlb/tests/test_public_game_prediction_pythagorean_log5_v1.py').read_text()
    baseline_test=(ROOT/'backend/mlb/tests/test_public_game_prediction_baseline_v1.py').read_text()
    assert 'BENCHMARK =' not in candidate_test and 'FOUNDATION =' not in baseline_test


def test_13_all_self_contained_fixture_hashes_are_bound():
    candidate=model.load_candidate()
    root=ROOT/'backend/mlb/tests/fixtures/public_game_predictions_v1'
    for name,expected in candidate['self_contained_reproduction_fixtures'].items():
        assert hashlib.sha256((root/name).read_bytes()).hexdigest()==expected


class _FakeCursor:
    def __init__(self,db): self.db=db;self.result=None
    def __enter__(self): return self
    def __exit__(self,*args): return False
    def execute(self,sql,params):
        normalized=' '.join(sql.split())
        if 'INSERT INTO mlb.public_game_moneyline_predictions' in normalized:
            key=tuple(params[:4]);payload_hash=params[-1]
            if key in self.db['predictions']: self.result=None
            else: self.db['predictions'][key]=payload_hash;self.result=(params[1],)
        elif 'SELECT payload_sha256 FROM mlb.public_game_moneyline_predictions' in normalized:
            self.result=(self.db['predictions'].get(tuple(params)),)
        elif 'INSERT INTO mlb.public_game_moneyline_outcomes' in normalized:
            key=tuple(params[:4]);payload_hash=params[-1]
            if key in self.db['outcomes']: self.result=None
            else: self.db['outcomes'][key]=payload_hash;self.result=(params[1],)
        elif 'SELECT payload_sha256 FROM mlb.public_game_moneyline_outcomes' in normalized:
            self.result=(self.db['outcomes'].get(tuple(params)),)
        else: raise AssertionError(normalized)
    def fetchone(self): return self.result


class _FakeConnection:
    def __init__(self,db): self.db=db
    def __enter__(self): return self
    def __exit__(self,*args): return False
    def cursor(self): return _FakeCursor(self.db)
    def commit(self): pass


def _durable_prediction():
    payload=json.loads((ROOT/'backend/mlb/tests/fixtures/public_game_predictions_v1/august6_schedule.json').read_text())
    state=snapshot(cutoff='2026-08-06T00:00:00Z')
    return model.score_schedule_payload(payload,prediction_timestamp_utc='2026-08-06T00:00:00Z',
                                        source_schedule_hash='a'*64,team_state_snapshot=state)[0]


def test_14_durable_prediction_identity_is_unique_and_idempotent(monkeypatch):
    db={'predictions':{},'outcomes':{}}
    monkeypatch.setattr(durable,'pg_connect',lambda:_FakeConnection(db))
    row=_durable_prediction()
    assert durable.append_prediction_rows([row])==1
    assert durable.append_prediction_rows([row])==0 and len(db['predictions'])==1


def test_15_durable_prediction_is_append_only_on_conflict(monkeypatch):
    db={'predictions':{},'outcomes':{}}
    monkeypatch.setattr(durable,'pg_connect',lambda:_FakeConnection(db))
    row=_durable_prediction();durable.append_prediction_rows([row])
    changed={**row,'home_win_probability':row['home_win_probability']+.01}
    with pytest.raises(model.PublicGamePredictionError,match='IMMUTABLE_PREDICTION_CONFLICT'):
        durable.append_prediction_rows([changed])


def test_16_durable_outcome_is_final_only_append_only_and_idempotent(monkeypatch):
    db={'predictions':{},'outcomes':{}}
    monkeypatch.setattr(durable,'pg_connect',lambda:_FakeConnection(db))
    prediction=_durable_prediction();durable.append_prediction_rows([prediction])
    grade=model.build_official_final_grade(prediction,official_home_runs=5,official_away_runs=3,
                                           official_source_path='official:824804',official_source_sha256='b'*64,
                                           grading_timestamp_utc='2026-08-07T12:00:00Z')
    assert durable.append_outcome_grade(grade) is True
    assert durable.append_outcome_grade(grade) is False
    changed={**grade,'official_home_runs':6}
    with pytest.raises(model.PublicGamePredictionError,match='OUTCOME_CORRECTION_REQUIRES_HISTORY'):
        durable.append_outcome_grade(changed)
