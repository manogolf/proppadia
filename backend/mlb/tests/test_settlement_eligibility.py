from backend.mlb.scripts.cleanroom_v1.settlement_eligibility import *

CERT={"applicability_decision":"CERTIFIED_FOR_2026-08-02","historical_applicability_through":"2026-08-03","completed_early_treatment":"VOID"}

def test_normal_final_game_settles_normally():
 assert classify_book_settlement("NORMAL_FINAL",4,slate_date="2026-08-02",contract=CERT)==BOOK_SETTLED_OFFICIAL_RESULT
def test_completed_early_invokes_rule_layer():
 assert classify_game({"abstractGameState":"Final","codedGameState":"F","detailedState":"Completed Early: Rain"},7)=="COMPLETED_EARLY_OFFICIAL"
def test_official_stats_remain_when_void():
 assert settle_side(3,"OVER",120,BOOK_VOID_SHORTENED_GAME)["official_outcome"]=="OVER_WIN"
def test_shortened_void_returns_stake_zero_net():
 r=settle_side(0,"UNDER",-180,BOOK_VOID_SHORTENED_GAME);assert (r["returned_stake"],r["net"])==(5.0,0.0)
def test_void_excluded_from_at_risk_denominator():
 assert settle_side(0,"UNDER",-180,BOOK_VOID_SHORTENED_GAME)["stake_at_risk"]==0
def test_nonappearance_distinct_from_shortened_void():
 assert classify_book_settlement("COMPLETED_EARLY_OFFICIAL",0,slate_date="2026-08-02",contract=CERT)==BOOK_VOID_PLAYER_DID_NOT_APPEAR
def test_current_rule_without_history_fails_closed():
 c={**CERT,"applicability_decision":"CURRENT_ONLY"};assert classify_book_settlement("COMPLETED_EARLY_OFFICIAL",2,slate_date="2026-08-02",contract=c)==BOOK_RULE_UNCERTIFIED
def test_actual_ticket_settlement_preserved():
 assert classify_book_settlement("NORMAL_FINAL",3,slate_date="2026-08-02",contract=CERT,actual_ticket_status="SETTLED")==BOOK_SETTLED_OFFICIAL_RESULT
def test_written_rule_ticket_conflict_visible():
 assert classify_book_settlement("COMPLETED_EARLY_OFFICIAL",3,slate_date="2026-08-02",contract=CERT,actual_ticket_status="SETTLED")==BOOK_SETTLEMENT_CONFLICT
def test_nonstandard_cannot_silently_settle():
 assert classify_book_settlement("SUSPENDED_FINAL",3,slate_date="2026-08-02",contract=CERT)==BOOK_RULE_UNCERTIFIED
def test_normal_official_classification():
 assert classify_game({"abstractGameState":"Final","codedGameState":"F","detailedState":"Final"},9)=="NORMAL_FINAL"
def test_normal_home_win_eight_completed_innings_is_normal():
 assert classify_game({"abstractGameState":"Final","codedGameState":"F","detailedState":"Final"},8)=="NORMAL_FINAL"
def test_postponed_is_other_rule_void():
 assert classify_book_settlement("POSTPONED_OR_CANCELLED",None,slate_date="2026-08-02",contract=CERT)==BOOK_VOID_OTHER_RULE
def test_settlement_is_deterministic_idempotent():
 a=settle_side(2,"OVER",140,BOOK_SETTLED_OFFICIAL_RESULT);assert a==settle_side(2,"OVER",140,BOOK_SETTLED_OFFICIAL_RESULT)
