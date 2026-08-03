from pathlib import Path
import pandas as pd

from backend.mlb.scripts.cleanroom_v1 import historical_pipeline_selection as h


def test_population_contract():
    p=h.load_population()
    assert len(p)==9267
    assert p.slate_date.nunique()==62
    assert not p[h.KEY].duplicated().any()


def test_exact_slate_path_is_run_bound():
    p=h.load_population().iloc[0]
    s=h.slate_path(p.roster_source_payload)
    assert p.normal_pipeline_run_tag in s.name
    assert s.exists()


def test_book_upload_is_not_a_selection_contract():
    inv=h.inventory(write=False)["surfaces"]
    row=next(x for x in inv if x["surface_name"]=="two-sided book upload")
    assert row["classification"]=="DESCRIPTIVE_ONLY"
    assert "both sides" in row["side"]


def test_review_aids_cannot_enter_primary_layers():
    inv=h.inventory(write=False)["surfaces"]
    review=[x for x in inv if "review/discovery" in x["surface_name"]][0]
    assert review["classification"]=="OPERATOR_REVIEW_AID_ONLY"


def test_aggregate_uses_authentic_selected_side_price():
    d=pd.DataFrame([{"slate_date":"x","game_pk":"1","player_mlb_id":"1","model_pick_side":"over","book_settlement":"BOOK_SETTLED_OFFICIAL_RESULT","over_result":"WIN","under_result":"LOSS","over_net":"5","under_net":"-5","over_odds":"100","under_odds":"-120"}])
    a=h.aggregate(d)
    assert a["wins"]==1 and a["net_dollars"]==5 and a["stake_at_5_risk"]==5


def test_metrics_known_values():
    import numpy as np
    y=np.array([0,1]); p=np.array([.1,.9])
    assert h.auc(y,p)==1
    assert h.average_precision(y,p)==1
