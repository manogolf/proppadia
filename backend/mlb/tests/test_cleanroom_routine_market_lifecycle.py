from datetime import datetime,timezone,timedelta
import csv,json
from pathlib import Path
import pytest
from backend.mlb.scripts.cleanroom_v1 import routine_market_lifecycle as r

def write_csv(p,fields,rows):
 p.parent.mkdir(parents=True,exist_ok=True)
 with p.open('w',newline='') as f:w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(rows)
def fixture(tmp_path,monkeypatch,rows=True,run='local_daily_20260804T120000Z'):
 slate='2026-08-04';normal=tmp_path/'normal';odds=tmp_path/'odds';out=tmp_path/'out';monkeypatch.setattr(r,'NORMAL',normal);monkeypatch.setattr(r,'ODDS',odds);monkeypatch.setattr(r,'OUT',out);monkeypatch.setattr(r,'current_date',lambda:slate)
 market=normal/slate/'snapshots'/run/'bol_tb15_market_rows.csv';source=odds/slate/f'odds_mlb_playerprops__{run}.json';roster=odds/slate/f'mlb_predictions_wide_calibrated__{run}.csv'
 m=[{'slate_date':slate,'game_pk':'1','batter_mlb_id':'10','player':'Exact Player','game':'SEA @ TEX','team':'SEA','opponent':'TEX','line':'1.5','over_odds':'120','under_odds':'-150','source_timestamp':'2026-08-04T12:00:00Z'}] if rows else []
 write_csv(market,['slate_date','game_pk','batter_mlb_id','player','game','team','opponent','line','over_odds','under_odds','source_timestamp'],m)
 write_csv(roster,['game_id','player_id','player_name','team','opponent','game_time'],[{'game_id':'1','player_id':'10','player_name':'Exact Player','team':'SEA','opponent':'TEX','game_time':'2026-08-04T20:00:00+00:00'}])
 source.parent.mkdir(parents=True,exist_ok=True);source.write_text(json.dumps({'captured_at_utc':'2026-08-04T12:00:00Z','events':[{'id':'event1','away_team':'Seattle Mariners','home_team':'Texas Rangers'}]}))
 return slate,run,out
def test_confirmed_lineup_and_batting_order_not_required(tmp_path,monkeypatch):
 slate,tag,out=fixture(tmp_path,monkeypatch);m=r.cohort(slate,tag,datetime(2026,8,4,12,tzinfo=timezone.utc));assert m['frozen_market_identities']==1;row=r.read_csv(out/slate/'routine_market_baseline.csv')[0];assert 'batting' not in row and row['player_mlb_id']=='10'
def test_same_normal_run_sources_and_hashes(tmp_path,monkeypatch):
 slate,tag,out=fixture(tmp_path,monkeypatch);r.cohort(slate,tag,datetime.now(timezone.utc));m=json.loads((out/slate/'routine_market_manifest.json').read_text());assert m['governing_normal_run']==tag and all(m['source_lineage'].get(k) for k in ('odds_sha256','roster_sha256','market_ledger_sha256'))
def test_exact_roster_required(tmp_path,monkeypatch):
 slate,tag,out=fixture(tmp_path,monkeypatch);roster=r.ODDS/slate/f'mlb_predictions_wide_calibrated__{tag}.csv';write_csv(roster,['game_id','player_id','game_time'],[{'game_id':'1','player_id':'11','game_time':'2026-08-04T20:00:00+00:00'}]);b,x,_=r.build(slate,tag,datetime.now(timezone.utc));assert not b and x[0]['reason']=='PLAYER_NOT_ON_SAME_RUN_ROSTER'
def test_one_sided_excluded(tmp_path,monkeypatch):
 slate,tag,out=fixture(tmp_path,monkeypatch);p=r.NORMAL/slate/'snapshots'/tag/'bol_tb15_market_rows.csv';rows=r.read_csv(p);rows[0]['under_odds']='';write_csv(p,list(rows[0]),rows);b,x,_=r.build(slate,tag,datetime.now(timezone.utc));assert not b and x[0]['reason']=='MARKET_NOT_TWO_SIDED'
def test_ambiguous_event_excluded(tmp_path,monkeypatch):
 slate,tag,out=fixture(tmp_path,monkeypatch);p=r.ODDS/slate/f'odds_mlb_playerprops__{tag}.json';d=json.loads(p.read_text());d['events']*=2;p.write_text(json.dumps(d));b,x,_=r.build(slate,tag,datetime.now(timezone.utc));assert not b and x[0]['reason']=='EVENT_BINDING_AMBIGUOUS'
def test_doubleheader_event_resolves_by_exact_first_pitch(tmp_path,monkeypatch):
 slate,tag,out=fixture(tmp_path,monkeypatch);p=r.ODDS/slate/f'odds_mlb_playerprops__{tag}.json';d=json.loads(p.read_text());d['events'][0]['commence_time']='2026-08-04T20:00:00Z';other={**d['events'][0],'id':'event2','commence_time':'2026-08-04T23:00:00Z'};d['events'].append(other);p.write_text(json.dumps(d));b,x,_=r.build(slate,tag,datetime.now(timezone.utc));assert len(b)==1 and b[0]['provider_event_id']=='event1'
def test_post_first_pitch_excluded(tmp_path,monkeypatch):
 slate,tag,out=fixture(tmp_path,monkeypatch);p=r.NORMAL/slate/'snapshots'/tag/'bol_tb15_market_rows.csv';rows=r.read_csv(p);rows[0]['source_timestamp']='2026-08-04T21:00:00Z';write_csv(p,list(rows[0]),rows);b,x,_=r.build(slate,tag,datetime.now(timezone.utc));assert not b and x[0]['reason']=='MARKET_POST_FIRST_PITCH'
def test_empty_run_allows_later_run(tmp_path,monkeypatch):
 slate,tag,out=fixture(tmp_path,monkeypatch,False);m=r.cohort(slate,tag);assert m['status']=='ROUTINE_RUN_NO_ELIGIBLE_MARKET' and not (out/slate/'routine_market_manifest.json').exists()
def test_later_run_cannot_replace_frozen(tmp_path,monkeypatch):
 slate,tag,out=fixture(tmp_path,monkeypatch);r.cohort(slate,tag);m=r.cohort(slate,tag);assert m['decision']=='ROUTINE_COHORT_ALREADY_FROZEN'
def test_noncurrent_manual_capture_fails(tmp_path,monkeypatch):
 slate,tag,out=fixture(tmp_path,monkeypatch);monkeypatch.setattr(r,'current_date',lambda:'2026-08-05');pytest.raises(RuntimeError,r.cohort,slate,tag)
def test_missing_result_is_unresolved_role():
 assert r.role({'stats':{'batting':{}},'gameStatus':{}})[0]=='ROLE_AMBIGUOUS_TECHNICAL_UNRESOLVED'
def test_official_bench_supports_nonappearance():
 assert r.role({'stats':{'batting':{'plateAppearances':0}},'gameStatus':{'isOnBench':True}})[0]=='DID_NOT_APPEAR'
def test_ph_and_pr_are_postgame_roles_only():
 assert r.role({'stats':{'batting':{'plateAppearances':1}},'gameStatus':{'isSubstitute':True}})[0]=='PINCH_HITTER';assert r.role({'stats':{'batting':{'plateAppearances':0,'runs':1}},'gameStatus':{'isSubstitute':True}})[0]=='PINCH_RUNNER'
def test_correction_overlay_is_append_only_empty_at_freeze(tmp_path,monkeypatch):
 slate,tag,out=fixture(tmp_path,monkeypatch);r.cohort(slate,tag);assert len(r.read_csv(out/slate/'correction_overlay.csv'))==0
