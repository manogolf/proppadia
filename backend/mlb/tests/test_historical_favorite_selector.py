import pandas as pd
from backend.mlb.scripts.cleanroom_v1 import historical_favorite_selector as f


def test_american_implied_probability():
    got=f.implied(pd.Series([100,-200])).round(6).tolist()
    assert got==[.5,.666667]


def test_partition_contract():
    dates=[f"2026-01-{i:02d}" for i in range(1,32)]+[f"2026-02-{i:02d}" for i in range(1,32)]
    p=f.partitions(dates)
    assert [len(p[x]) for x in ['DESIGN','VALIDATION','HOLDOUT']]==[40,11,11]


def test_instruments_are_fixed():
    assert set(f.INSTRUMENTS)=={'B0_BLIND_UNDER','B1_HISTORICAL_MODEL_DIRECTION','B2_CONTEMPORANEOUS_MARKET_FAVORITE','C1_MODEL_MARKET_AGREEMENT','C2_UNDER_MARKET_FAVORITE','C3_MODEL_CONFIRMED_UNDER_FAVORITE'}


def test_contract_name():
    assert f.CONTRACT=='MARKET_FAVORITE_WITHHOLD_ATTACHMENT_V1'
