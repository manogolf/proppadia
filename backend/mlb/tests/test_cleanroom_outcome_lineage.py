from backend.mlb.scripts.cleanroom_v1.outcome_lineage import american_profit,classify_outcome,reconstruct_stats
def test_exact_result_and_no_action():
 assert classify_outcome(1,True,4,2)=='OVER_WIN'; assert classify_outcome(1,True,0,0)=='NO_ACTION'
def test_missing_duplicate_pending_fail_closed():
 assert classify_outcome(0,True,None,None)=='MISSING_PLAYER_RESULT'; assert classify_outcome(2,True,4,1)=='DUPLICATE_PLAYER_RESULT'; assert classify_outcome(1,False,None,None)=='PENDING'
def test_tb_components():
 assert reconstruct_stats(1,0,0,0)==('TB_ARITHMETIC_CERTIFIED',1,1)
 assert reconstruct_stats(1,1,0,0)==('TB_ARITHMETIC_CERTIFIED',0,2)
 assert reconstruct_stats(1,0,1,0)==('TB_ARITHMETIC_CERTIFIED',0,3)
 assert reconstruct_stats(1,0,0,1)==('TB_ARITHMETIC_CERTIFIED',0,4)
 assert reconstruct_stats(3,1,1,0)==('TB_ARITHMETIC_CERTIFIED',1,6)
 assert reconstruct_stats(1,2,0,0)[0]=='NEGATIVE_SINGLES'
def test_authentic_price_math():
 assert american_profit(5,-200,True)==2.5; assert american_profit(5,150,True)==7.5; assert american_profit(5,150,False)==-5
