import json
from backend.mlb.scripts.cleanroom_v1 import routine_outcome_reconciliation as o

def official(player):
 return {'gameData':{'status':{'abstractGameState':'Final'}},'liveData':{'boxscore':{'teams':{'away':{'players':{'ID10':player}},'home':{'players':{}}}}}}
def player(batting=None,status=None,order=''):
 return {'person':{'id':10},'stats':{'batting':batting or {}},'gameStatus':status or {},'battingOrder':order}
def local(**kw):
 d={'player_id':10,'game_id':1,'plate_appearances':4,'at_bats':4,'hits':2,'singles':1,'doubles':1,'triples':0,'home_runs':0,'total_bases':3};d.update(kw);return d
def result(feed):return o.player_result(feed,10)
def test_exact_game_player_local_result_verified():
 r=result(official(player({'plateAppearances':4,'atBats':4,'hits':2,'doubles':1},order='100')));assert o.classify_local([local()],r)=='LOCAL_ROW_VERIFIED_EXACT'
def test_official_correction_detected():
 r=result(official(player({'plateAppearances':4,'atBats':4,'hits':2,'doubles':1},order='100')));assert o.classify_local([local(total_bases=2)],r)=='LOCAL_ROW_STAT_MISMATCH'
def test_missing_local_not_nonappearance():
 r=result(official(player({'plateAppearances':1,'atBats':1,'hits':0},status={'isSubstitute':True})));assert o.classify_local([],r)=='LOCAL_ROW_MISSING' and r['official_status']=='OFFICIAL_APPEARANCE_RESULT_RECOVERED'
def test_official_nonappearance_supported():
 r=result(official(player({},status={'isOnBench':True})));assert r['official_status']=='OFFICIAL_NONAPPEARANCE_SUPPORTED'
def test_pinch_hitter_recovered():
 r=result(official(player({'plateAppearances':1,'atBats':1},status={'isSubstitute':True})));assert r['role']=='PINCH_HITTER'
def test_pinch_runner_zero_pa_preserved():
 r=result(official(player({'plateAppearances':0,'runs':1},status={'isSubstitute':True})));assert r['role']=='PINCH_RUNNER' and r['official_status']=='OFFICIAL_ZERO_PA_APPEARANCE_RECOVERED'
def test_other_substitute_zero_pa_preserved():
 r=result(official(player({'plateAppearances':0},status={'isSubstitute':True})));assert r['role']=='OTHER_SUBSTITUTE'
def test_duplicate_local_fails_closed():
 r=result(official(player({'plateAppearances':4},order='100')));assert o.classify_local([local(),local()],r)=='DUPLICATE_LOCAL_PLAYER_STATS_ROWS'
def test_name_only_matching_refused():
 assert o.player_result(official(player({'plateAppearances':1})),11)['official_status']=='TECHNICAL_UNRESOLVED_NO_EXACT_OFFICIAL_SUPPORT'
def test_game_date_metadata_not_part_of_stat_comparison():
 r=result(official(player({'plateAppearances':4,'atBats':4,'hits':2,'doubles':1},order='100')));assert o.classify_local([local(game_date='2026-08-01')],r)=='LOCAL_ROW_VERIFIED_EXACT'
def test_total_bases_independently_recomputed():
 r=result(official(player({'plateAppearances':4,'atBats':4,'hits':3,'doubles':1,'triples':1,'homeRuns':1},order='100')));assert r['singles']==0 and r['total_bases']==9
def test_official_payload_governs_conflict():
 r=result(official(player({'plateAppearances':4,'atBats':4,'hits':1},order='100')));assert o.classify_local([local(hits=2)],r)=='LOCAL_ROW_STAT_MISMATCH'
def test_missing_exact_official_player_unresolved():
 assert o.player_result(official(player()),99)['role']=='ROLE_AMBIGUOUS_TECHNICAL_UNRESOLVED'
def test_summary_counts_recovery_and_conflict():
 rows=[{'normal_player_stats_row_count':0,'local_status':'LOCAL_ROW_MISSING','final_support_decision':'OFFICIAL_APPEARANCE_RESULT_RECOVERED','official_source_status':'OFFICIAL_APPEARANCE_RESULT_RECOVERED','official_source_sha256':'x','game_pk':1,'player_mlb_id':10},{'normal_player_stats_row_count':1,'local_status':'LOCAL_ROW_STAT_MISMATCH','final_support_decision':'OFFICIAL_SOURCE_GOVERNS_LOCAL_CONFLICT','official_source_status':'OFFICIAL_APPEARANCE_RESULT_RECOVERED','official_source_sha256':'y','game_pk':1,'player_mlb_id':11}];s=o.summary(rows,[{}]);assert s['officially_recovered_missing_rows']==1 and s['stat_conflicts']==1
def test_closeout_contract_uses_certified_layer():
 import inspect
 from backend.mlb.scripts.cleanroom_v1 import routine_market_lifecycle as life
 assert 'routine_outcome_reconciliation' in inspect.getsource(life.closeout)
