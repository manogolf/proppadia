import pandas as pd
import pytest

from backend.mlb.scripts import (
    report_mlb_hits_interaction_failure as interaction_failure,
    report_mlb_hits_recency_vs_baseline as recency_report,
)


@pytest.fixture
def overlapping_reconcile_and_features():
    reconcile = pd.DataFrame(
        [
            {
                "game_date": "2026-04-09",
                "game_id": 823319,
                "player_id": 686668,
                "prop_type": "hits",
                "line": 0.5,
                "price_over_american": -110,
                "price_under_american": 100,
                "model_prob_over": 0.55,
                "model_prob_under": 0.45,
                "actual_over_outcome": "win",
                "actual_under_outcome": "loss",
                "d7_hits": 0.10,
                "d30_hits": 0.80,
                "rolling_result_avg_7": 0.10,
            }
        ]
    )
    features = pd.DataFrame(
        [
            {
                "date_key": "2026-04-09",
                "game_id_key": 823319,
                "player_id_key": 686668,
                "d7_hits": 0.25,
                "d30_hits": 0.90,
                "rolling_result_avg_7": 0.30,
            }
        ]
    )
    return reconcile, features


def test_recency_report_keeps_database_feature_columns_canonical(
    overlapping_reconcile_and_features,
):
    reconcile, features = overlapping_reconcile_and_features

    sides = recency_report._side_rows(reconcile, features)

    assert set(sides["d7_hits"]) == {0.25}
    assert set(sides["d30_hits"]) == {0.90}
    assert set(sides["rolling_result_avg_7"]) == {0.30}


def test_interaction_report_keeps_database_feature_columns_canonical(
    overlapping_reconcile_and_features,
):
    reconcile, features = overlapping_reconcile_and_features

    sides = interaction_failure._side_rows(reconcile, features)

    assert set(sides["d7_hits"]) == {0.25}
    assert set(sides["d30_hits"]) == {0.90}
