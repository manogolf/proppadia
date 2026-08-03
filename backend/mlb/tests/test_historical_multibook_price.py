from backend.mlb.scripts.cleanroom_v1 import historical_multibook_price as m

def test_odds_round_trip():
    for x in [-200,-110,100,150]:assert abs(m._american(m._decimal(x))-x)<1e-9

def test_profit():
    assert m._profit(100)==5
    assert abs(m._profit(-200)-2.5)<1e-9

def test_contract():assert m.CONTRACT=='C1_CONTEMPORANEOUS_MULTIBOOK_PRICE_ATTACHMENT_V1'
