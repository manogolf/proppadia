from backend.mlb.scripts import player_stats_game_completeness as c
from backend.mlb.scripts.insert_mlb_stat_derived import (
 _extract_player_stats_row,
 _final_games,
 _upsert_player_stats_row,
)
from backend.mlb.scripts.backfill_mlb_player_pa import PaRow,_retain_boxscore_payload,_update_player_stats_pa

def off(pid=1,role='STARTED',pa=4,tb=0):
 return {'game_pk':10,'player_mlb_id':pid,'player':f'p{pid}','official_role':role,'plate_appearances':pa,'at_bats':pa,'hits':tb,'singles':tb,'doubles':0,'triples':0,'home_runs':0,'total_bases':tb}
def loc(pid=1,pa=4,tb=0):return {'game_id':10,'player_id':pid,'position':'LF','plate_appearances':pa,'at_bats':pa,'hits':tb,'singles':tb,'doubles':0,'triples':0,'home_runs':0,'total_bases':tb}
def test_complete_set_passes():assert c.decision(c.compare([off()],[loc()]))=='COMPLETE_EXACT'
def test_partial_many_rows_fails():assert c.decision(c.compare([off(1),off(2)],[loc(1)]))=='MISSING_OFFICIAL_PARTICIPANTS'
def test_missing_starter_fails():assert c.compare([off()],[])[0]['decision']=='MISSING_OFFICIAL_PARTICIPANTS'
def test_missing_pinch_hitter_fails():assert c.compare([off(role='PINCH_HITTER',pa=1)],[])[0]['decision']=='MISSING_OFFICIAL_PARTICIPANTS'
def test_missing_zero_pa_substitute_fails():assert c.compare([off(role='OTHER_SUBSTITUTE',pa=0)],[])[0]['decision']=='MISSING_OFFICIAL_PARTICIPANTS'
def test_nonparticipant_not_required():assert c.compare([],[] )==[]
def test_extra_local_fails():assert c.decision(c.compare([],[loc()]))=='EXTRA_LOCAL_ROWS'
def test_pitcher_local_not_batter_extra():
 x=loc();x['position']='P';assert c.compare([],[x])==[]
def test_duplicate_local_fails():assert c.decision(c.compare([off()],[loc(),loc()]))=='DUPLICATE_LOCAL_ROWS'
def test_stat_mismatch_fails():assert c.decision(c.compare([off(tb=2)],[loc(tb=1)]))=='STAT_MISMATCH'
def test_pa_mismatch_fails_entire_game():
 second=loc(2,pa=3);second['plate_appearances']=0
 rows=c.compare([off(1,pa=4),off(2,pa=3)],[loc(1,pa=4),second])
 assert rows[1]['mismatch_fields']=='plate_appearances'
 assert c.decision(rows)=='STAT_MISMATCH'
def test_exact_id_binding_not_name():assert c.compare([off(1)],[loc(2)])[0]['decision']=='MISSING_OFFICIAL_PARTICIPANTS'
def test_date_only_matching_refused():
 x=loc();x['game_id']=11;assert c.compare([off()],[x])[0]['decision']=='MISSING_OFFICIAL_PARTICIPANTS'
def test_terminal_rain_is_final():
 games=[{'gamePk':824807,'gameType':'R','season':2026,'status':{'abstractGameState':'Final','codedGameState':'F','detailedState':'Completed Early: Rain','statusCode':'FR'}}];assert _final_games(games,require_regular_season=True)==[(824807,'R')]
def test_nonfinal_is_not_selected():assert _final_games([{'gamePk':1,'gameType':'R','season':2026,'status':{'detailedState':'In Progress'}}],require_regular_season=False)==[]
def test_parser_includes_zero_pa_substitute(monkeypatch):
 monkeypatch.setattr(c,'role',lambda p:('OTHER_SUBSTITUTE',''));feed={'gameData':{'game':{'pk':10},'teams':{'away':{'id':1,'abbreviation':'A'},'home':{'id':2,'abbreviation':'H'}}},'liveData':{'boxscore':{'teams':{'away':{'players':{'ID1':{'person':{'id':1,'fullName':'p'},'stats':{'batting':{'plateAppearances':0}},'position':{'abbreviation':'LF'}}}},'home':{'players':{}}}}}};assert len(c.participant_rows(feed))==1
def test_parser_excludes_official_nonparticipant(monkeypatch):
 monkeypatch.setattr(c,'role',lambda p:('DID_NOT_APPEAR',''));feed={'gameData':{'game':{'pk':10},'teams':{'away':{'id':1,'abbreviation':'A'},'home':{'id':2,'abbreviation':'H'}}},'liveData':{'boxscore':{'teams':{'away':{'players':{'ID1':{'person':{'id':1},'stats':{'batting':{}}}}},'home':{'players':{}}}}}};assert c.participant_rows(feed)==[]
def test_repair_sql_is_insert_only():
 import inspect;s=inspect.getsource(c.repair);assert 'ON CONFLICT (player_id,game_id) DO NOTHING' in s and 'DO UPDATE' not in s
def test_repair_has_idempotent_no_action():
 import inspect;assert 'NO_ACTION_ALREADY_PRESENT' in inspect.getsource(c.dry_run)
def test_rollback_deletes_exact_game_player():
 import inspect;assert 'DELETE FROM mlb.player_stats WHERE game_id = {game} AND player_id = {pid}' in inspect.getsource(c.repair)
def test_failed_completeness_is_visible():
 assert c.decision([{'decision':'MISSING_OFFICIAL_PARTICIPANTS'}])=='MISSING_OFFICIAL_PARTICIPANTS'
def test_bounded_date_recovery_succeeds(monkeypatch,tmp_path):
 calls={'n':0}
 def inspect(date,out):
  calls['n']+=1;return [{'game_pk':10,'classification':'MISSING_OFFICIAL_PARTICIPANTS'}] if calls['n']==1 else [{'game_pk':10,'classification':'COMPLETE_EXACT'}]
 monkeypatch.setattr(c,'inspect_date',inspect);monkeypatch.setattr(c,'repair',lambda game,out:{'inserted_rows':1});assert c.recover_date('2026-08-02',tmp_path)['status']=='COMPLETE_EXACT'
def test_failed_recovery_remains_visible(monkeypatch,tmp_path):
 import pytest
 monkeypatch.setattr(c,'inspect_date',lambda date,out:[{'game_pk':10,'classification':'STAT_MISMATCH'}]);pytest.raises(RuntimeError,c.recover_date,'2026-08-02',tmp_path)
def test_finalized_gate_rejects_partial_game(monkeypatch):
 import sys
 from backend.mlb.scripts import check_mlb_finalized_training_data as gate
 monkeypatch.setattr(gate,'_counts',lambda date:{'model_training_props_rows':1,'player_stats_rows':10});monkeypatch.setattr(gate,'inspect_date',lambda date,out:[{'game_pk':10,'classification':'MISSING_OFFICIAL_PARTICIPANTS'}]);monkeypatch.setattr(sys,'argv',['gate','--date','2026-08-02','--check-player-stats']);assert gate.main()==2
def test_finalized_gate_accepts_complete_games(monkeypatch):
 import sys
 from backend.mlb.scripts import check_mlb_finalized_training_data as gate
 monkeypatch.setattr(gate,'_counts',lambda date:{'model_training_props_rows':1,'player_stats_rows':10});monkeypatch.setattr(gate,'inspect_date',lambda date,out:[{'game_pk':10,'classification':'COMPLETE_EXACT'}]);monkeypatch.setattr(sys,'argv',['gate','--date','2026-08-02','--check-player-stats']);assert gate.main()==0

def test_primary_importer_parses_direct_plate_appearances():
 row=_extract_player_stats_row(player_id=1,game_id=10,game_date='2026-08-14',team_abbr='NYY',opponent_abbr='BOS',is_home=True,position='LF',stats={'batting':{'plateAppearances':5,'atBats':4,'hits':2}},is_starter=True)
 assert row['plate_appearances']==5
 assert row['at_bats']==4
 assert row['hits']==2

def test_primary_importer_only_fills_blank_pa_on_conflict():
 class Cursor:
  rowcount=1
  description=()
  def __enter__(self):return self
  def __exit__(self,*args):return False
  def execute(self,sql,row):
   self.sql=sql
   if sql.lstrip().startswith('SELECT game_date'):
    self.result=None
   elif 'safe_upsert_player_stats' in sql:
    self.result=(1,)
  def fetchone(self):return getattr(self,'result',None)
 class Conn:
  def __init__(self):self.cur=Cursor()
  def cursor(self):return self.cur
 conn=Conn();row=_extract_player_stats_row(player_id=1,game_id=10,game_date='2026-08-14',team_abbr='NYY',opponent_abbr='BOS',is_home=True,position='LF',stats={'batting':{'plateAppearances':5}},is_starter=True)
 assert _upsert_player_stats_row(conn,row)==1
 assert conn.cur.sql == 'SELECT mlb.safe_upsert_player_stats(%s::jsonb)'

def test_player_stats_conflicting_populated_official_value_is_reported_and_not_upserted():
 class Cursor:
  rowcount=0
  def __init__(self):self.calls=[];self.description=[(name,) for name in ('game_date','team','opponent','is_home','position','plate_appearances','at_bats','hits','total_bases','rbis','runs_scored','strikeouts_batting','walks','singles','doubles','triples','home_runs','stolen_bases','strikeouts_pitching','walks_allowed','hits_allowed','outs_recorded','earned_runs','is_starter')]
  def __enter__(self):return self
  def __exit__(self,*args):return False
  def execute(self,sql,params):
   self.calls.append(sql)
   if 'safe_upsert_player_stats' in sql:self.result=(1,)
  def fetchone(self):return getattr(self,'result',{'game_date':'2026-08-14','team':'NYY','opponent':'BOS','is_home':True,'position':'LF','plate_appearances':5,'at_bats':4,'hits':1,'total_bases':1,'rbis':0,'runs_scored':0,'strikeouts_batting':1,'walks':0,'singles':1,'doubles':0,'triples':0,'home_runs':0,'stolen_bases':0,'strikeouts_pitching':None,'walks_allowed':None,'hits_allowed':None,'outs_recorded':None,'earned_runs':None,'is_starter':False})
 class Conn:
  def __init__(self):self.cur=Cursor()
  def cursor(self):return self.cur
 conn=Conn()
 row={'player_id':1,'game_id':10,'game_date':'2026-08-14','team':'NYY','opponent':'BOS','is_home':True,'position':'LF','plate_appearances':5,'at_bats':4,'hits':2,'total_bases':2,'rbis':0,'runs_scored':0,'strikeouts_batting':1,'walks':0,'singles':1,'doubles':0,'triples':0,'home_runs':0,'stolen_bases':0,'strikeouts_pitching':None,'walks_allowed':None,'hits_allowed':None,'outs_recorded':None,'earned_runs':None,'is_starter':False}
 result=_upsert_player_stats_row(conn,row)
 assert result==0
 assert result.conflicts[0]['conflicting_fields']==('hits','total_bases')
 assert len(conn.cur.calls)==1

def test_player_stats_verified_null_fill_and_identical_replay_are_non_destructive():
 fields=('game_date','team','opponent','is_home','position','plate_appearances','at_bats','hits','total_bases','rbis','runs_scored','strikeouts_batting','walks','singles','doubles','triples','home_runs','stolen_bases','strikeouts_pitching','walks_allowed','hits_allowed','outs_recorded','earned_runs','is_starter')
 incoming={'player_id':1,'game_id':10,'game_date':'2026-08-14','team':'NYY','opponent':'BOS','is_home':True,'position':'LF','plate_appearances':5,'at_bats':4,'hits':2,'total_bases':2,'rbis':0,'runs_scored':0,'strikeouts_batting':1,'walks':0,'singles':1,'doubles':0,'triples':0,'home_runs':0,'stolen_bases':0,'strikeouts_pitching':None,'walks_allowed':None,'hits_allowed':None,'outs_recorded':None,'earned_runs':None,'is_starter':False}
 class Cursor:
  def __init__(self,existing,rowcount):self.existing=existing;self.write_result=(rowcount,);self.calls=[];self.description=[(name,) for name in fields];self.write_called=False
  def __enter__(self):return self
  def __exit__(self,*args):return False
  def execute(self,sql,params):
   self.calls.append(sql)
   if 'safe_upsert_player_stats' in sql:self.write_called=True
  def fetchone(self):return self.write_result if self.write_called else self.existing
 class Conn:
  def __init__(self,cursor):self.cur=cursor
  def cursor(self):return self.cur
 existing={name:incoming.get(name) for name in fields};existing['hits']=None;existing['total_bases']=None
 fill=Conn(Cursor(existing,1));assert _upsert_player_stats_row(fill,incoming)==1
 assert fill.cur.calls[1]=='SELECT mlb.safe_upsert_player_stats(%s::jsonb)'
 stable={name:incoming.get(name) for name in fields}
 replay=Conn(Cursor(stable,0));assert _upsert_player_stats_row(replay,incoming)==0
 assert replay.cur.calls[1]=='SELECT mlb.safe_upsert_player_stats(%s::jsonb)'

def test_pa_backfill_uses_fixed_fill_entrypoint_not_direct_update():
 class Cursor:
  description=[(name,) for name in ('player_id','game_id','plate_appearances','hit_by_pitch','sacrifice_flies','sacrifice_hits','catcher_interference','pa_source')]
  def __enter__(self):return self
  def __exit__(self,*args):return False
  def execute(self,sql,params=None):
   self.calls=getattr(self,'calls',[])+[sql]
   self.sql=sql
   if sql.lstrip().startswith('WITH wanted'):
    self.result=[{'player_id':1,'game_id':10,'plate_appearances':None,'hit_by_pitch':None,'sacrifice_flies':None,'sacrifice_hits':None,'catcher_interference':None,'pa_source':None}]
   elif 'safe_fill_player_stats_pa' in sql:self.result=(1,)
  def fetchall(self):return getattr(self,'result',[])
  def fetchone(self):return getattr(self,'result',(0,))
 class Conn:
  def __init__(self):self.cur=Cursor()
  def cursor(self):return self.cur
 row=PaRow(game_date='2026-08-14',game_id=10,player_id=1,player_name='p',team='NYY',opponent='BOS',at_bats=4,walks=1,hit_by_pitch=0,sacrifice_flies=0,sacrifice_hits=0,catcher_interference=0,plate_appearances_direct=5,plate_appearances_formula=5,plate_appearances=5,formula_matches_direct=True,used_direct_pa=True,missing_components='')
 conn=Conn();assert _update_player_stats_pa(conn,[row],source='statsapi',only_missing_pa=True)==1
 assert 'SELECT mlb.safe_fill_player_stats_pa(%s,%s,%s,%s,%s,%s,%s,%s)' in conn.cur.calls

def test_pa_backfill_reports_populated_conflict_without_calling_entrypoint():
 class Cursor:
  description=[(name,) for name in ('player_id','game_id','plate_appearances','hit_by_pitch','sacrifice_flies','sacrifice_hits','catcher_interference','pa_source')]
  def __enter__(self):return self
  def __exit__(self,*args):return False
  def execute(self,sql,params=None):
   self.sql=sql
   if sql.lstrip().startswith('WITH wanted'):
    self.result=[{'player_id':1,'game_id':10,'plate_appearances':4,'hit_by_pitch':0,'sacrifice_flies':0,'sacrifice_hits':0,'catcher_interference':0,'pa_source':'statsapi'}]
   else:self.entrypoint_called=True
  def fetchall(self):return self.result
 class Conn:
  def __init__(self):self.cur=Cursor()
  def cursor(self):return self.cur
 row=PaRow(game_date='2026-08-14',game_id=10,player_id=1,player_name='p',team='NYY',opponent='BOS',at_bats=4,walks=1,hit_by_pitch=0,sacrifice_flies=0,sacrifice_hits=0,catcher_interference=0,plate_appearances_direct=5,plate_appearances_formula=5,plate_appearances=5,formula_matches_direct=True,used_direct_pa=True,missing_components='')
 conn=Conn();assert _update_player_stats_pa(conn,[row],source='statsapi')==0
 assert row.conflicting_fields==['plate_appearances']
 assert not getattr(conn.cur,'entrypoint_called',False)

def test_pa_backfill_retains_exact_boxscore_bytes_and_hash(monkeypatch,tmp_path):
 import hashlib
 import backend.mlb.scripts.backfill_mlb_player_pa as backfill
 monkeypatch.setattr(backfill,'ROOT',tmp_path)
 monkeypatch.setattr(backfill,'OUT_DIR',tmp_path/'artifacts'/'pa')
 raw=b'{"official":"boxscore"}\n'
 relpath,digest=_retain_boxscore_payload(123,raw)
 assert digest==hashlib.sha256(raw).hexdigest()
 assert (tmp_path/relpath).read_bytes()==raw
