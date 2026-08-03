from backend.mlb.scripts.cleanroom_v1 import historical_routine_replay as h
from backend.mlb.scripts.cleanroom_v1.settlement_eligibility import *

def test_first_eligible_normal_run_is_selected(monkeypatch):
 monkeypatch.setattr(h.routine,'runs',lambda d:['local_daily_20260729T120000Z','local_daily_20260729T130000Z'])
 assert h.run_ts(h.routine.runs('x')[0])<h.run_ts(h.routine.runs('x')[1])
def test_later_run_cannot_replace_selected():
 assert h.run_ts('local_daily_20260729T130000Z')>h.run_ts('local_daily_20260729T120000Z')
def test_outcomes_not_in_membership_contract():assert 'outcome' not in h.routine.FIELDS
def test_same_run_roster_required():assert h.routine.artifacts('2026-07-29','x')[2].name.endswith('__x.csv')
def test_market_timestamp_required():assert 'market_observation_timestamp' in h.routine.FIELDS
def test_later_price_not_in_contract():assert 'later' not in ' '.join(h.routine.FIELDS)
def test_game_already_started_exclusion_is_defined():assert 'MARKET_POST_FIRST_PITCH' in open('backend/mlb/scripts/cleanroom_v1/routine_market_lifecycle.py').read()
def test_name_only_identity_refused():assert 'player_mlb_id' in h.routine.FIELDS
def test_missing_evidence_not_reconstructed():assert h.CONTRACT.endswith('_V1')
def test_official_exact_join_key():assert {'game_pk','player_mlb_id'}<=set(h.routine.FIELDS)
def test_zero_pa_is_nonappearance_void():assert classify_book_settlement('NORMAL_FINAL',0,slate_date='2026-08-02')==BOOK_VOID_PLAYER_DID_NOT_APPEAR
def test_zero_pa_substitute_classification_exists():assert BOOK_VOID_OTHER_RULE != BOOK_VOID_PLAYER_DID_NOT_APPEAR
def test_nonstandard_invokes_rule():assert classify_book_settlement('COMPLETED_EARLY_OFFICIAL',2,slate_date='2026-08-02')==BOOK_VOID_SHORTENED_GAME
def test_void_excluded_from_denominator():assert settle_side(0,'UNDER',-180,BOOK_VOID_SHORTENED_GAME)['stake_at_risk']==0
def test_replay_random_seed_is_deterministic():assert h.random.Random(824807).random()==h.random.Random(824807).random()
def test_current_database_cannot_change_membership():assert 'local_' not in ' '.join(h.routine.FIELDS)
