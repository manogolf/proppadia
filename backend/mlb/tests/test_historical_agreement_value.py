import pandas as pd
from backend.mlb.scripts.cleanroom_v1 import historical_agreement_value as a


def test_gap_bands_are_fixed():
    assert [a._gap_band(x) for x in [-.1,-.05,-.001,0,.001,.05,.1]]==['MODEL_AT_LEAST_10PP_BELOW','MODEL_5_TO_9_99PP_BELOW','MODEL_0_01_TO_4_99PP_BELOW','MODEL_EXACTLY_EQUAL','MODEL_0_01_TO_4_99PP_ABOVE','MODEL_5_TO_9_99PP_ABOVE','MODEL_AT_LEAST_10PP_ABOVE']


def test_price_failure_classification():
    assert a._classification({'novig_fair_roi':.01,'offered_roi':-.01,'offered_price_drag_vs_novig':-1})=='POSITIVE_FAIR_VALUE_ERASED_BY_OFFERED_PRICE'
    assert a._classification({'novig_fair_roi':-.01,'offered_roi':-.02,'offered_price_drag_vs_novig':-1})=='MIXED_SELECTION_AND_PRICE_FAILURE'
