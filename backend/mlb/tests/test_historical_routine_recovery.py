from backend.mlb.scripts.cleanroom_v1 import historical_routine_recovery as r
from backend.mlb.scripts.cleanroom_v1.pilot_exact_game_roster_identity import norm
from backend.mlb.scripts.cleanroom_v1.settlement_eligibility import *

def test_older_schema_exact_evidence_admitted():assert r.CONTRACT.endswith('_RECOVERY')
def test_run_tag_inside_payload_discovered():assert r.v1.run_ts('local_daily_20260501T120000Z').year==2026
def test_utc_pacific_path_mismatch_resolved():assert r.dt('2026-05-02T01:00:00Z').astimezone(r.PT).day==1
def test_raw_payload_restores_derived_fields():assert 'batter_total_bases' in open('backend/mlb/scripts/cleanroom_v1/historical_routine_recovery.py').read()
def test_mtime_not_used_for_observation():assert 'st_mtime' not in open('backend/mlb/scripts/cleanroom_v1/historical_routine_recovery.py').read()
def test_later_price_cannot_replace():assert 'chosen is None' in open('backend/mlb/scripts/cleanroom_v1/historical_routine_recovery.py').read()
def test_final_boxscore_not_membership():assert 'boxscore' not in r.recover_run.__code__.co_names
def test_exact_normalized_identity():assert norm('José Ramírez')[0]==norm('Jose Ramirez')[0]
def test_fuzzy_identity_fails():assert norm('Matt')[0]!=norm('Matt Vierling')[0]
def test_doubleheader_start_distinct():assert r.dt('2026-07-01T17:00:00Z')!=r.dt('2026-07-01T23:00:00Z')
def test_outcomes_absent_from_discovery():assert 'official_feed' not in r.discover.__code__.co_names
def test_population_freezes_before_outcomes():assert 'FROZEN_BEFORE_OUTCOMES' in open('backend/mlb/scripts/cleanroom_v1/historical_routine_recovery.py').read()
def test_original_population_separate():assert r.V1!=r.OUT
def test_recovered_population_versioned():assert 'V2' in r.CONTRACT
def test_still_missing_excluded():assert 'NOT_RECOVERABLE_DO_NOT_RECONSTRUCT' in open('backend/mlb/scripts/cleanroom_v1/historical_routine_recovery.py').read()
def test_nonstandard_guard_available():assert classify_book_settlement('COMPLETED_EARLY_OFFICIAL',2,slate_date='2026-08-02')==BOOK_VOID_SHORTENED_GAME
def test_zero_pa_void_is_distinct():assert BOOK_VOID_ZERO_PA not in {BOOK_VOID_OTHER_RULE,BOOK_VOID_PLAYER_DID_NOT_APPEAR}
def test_replay_seed_deterministic():assert r.random.Random(824807).random()==r.random.Random(824807).random()
