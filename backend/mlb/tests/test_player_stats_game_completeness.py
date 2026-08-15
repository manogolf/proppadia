from backend.mlb.scripts import player_stats_game_completeness as c
from backend.mlb.scripts.insert_mlb_stat_derived import (
 _extract_player_stats_row,
 _final_games,
 _upsert_player_stats_row,
)
from backend.mlb.scripts.backfill_mlb_player_pa import PaRow,_update_player_stats_pa

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
 games=[{'gamePk':824807,'gameType':'R','status':{'abstractGameState':'Final','codedGameState':'F','detailedState':'Completed Early: Rain'}}];assert _final_games(games,require_regular_season=True)==[(824807,'R')]
def test_nonfinal_is_not_selected():assert _final_games([{'gamePk':1,'gameType':'R','status':{'detailedState':'In Progress'}}],require_regular_season=False)==[]
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
  def __enter__(self):return self
  def __exit__(self,*args):return False
  def execute(self,sql,row):self.sql=sql
 class Conn:
  def __init__(self):self.cur=Cursor()
  def cursor(self):return self.cur
 conn=Conn();row=_extract_player_stats_row(player_id=1,game_id=10,game_date='2026-08-14',team_abbr='NYY',opponent_abbr='BOS',is_home=True,position='LF',stats={'batting':{'plateAppearances':5}},is_starter=True)
 assert _upsert_player_stats_row(conn,row)==1
 assert 'plate_appearances = COALESCE(player_stats.plate_appearances, EXCLUDED.plate_appearances)' in conn.cur.sql

def test_blank_only_pa_backfill_does_not_overwrite_populated_pa():
 class Cursor:
  rowcount=1
  def __enter__(self):return self
  def __exit__(self,*args):return False
  def executemany(self,sql,rows):self.sql=sql;self.rows=rows
 class Conn:
  def __init__(self):self.cur=Cursor()
  def cursor(self):return self.cur
 row=PaRow(game_date='2026-08-14',game_id=10,player_id=1,player_name='p',team='NYY',opponent='BOS',at_bats=4,walks=1,hit_by_pitch=0,sacrifice_flies=0,sacrifice_hits=0,catcher_interference=0,plate_appearances_direct=5,plate_appearances_formula=5,plate_appearances=5,formula_matches_direct=True,used_direct_pa=True,missing_components='')
 conn=Conn();assert _update_player_stats_pa(conn,[row],source='statsapi',only_missing_pa=True)==1
 assert 'AND ps.plate_appearances IS NULL' in conn.cur.sql
 assert 'OR COALESCE(ps.at_bats, 0) > 0' in conn.cur.sql
 assert conn.cur.rows[0][0:5]==(5,0,0,0,0)
