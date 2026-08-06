from __future__ import annotations

import numpy as np
import pandas as pd

from backend.mlb.scripts.run_mlb_totals_structural_challenger_v2 import (
    V1_ALPHA,
    add_park_features,
    add_sparse_shrinkage,
    dynamic_environment,
    nb_mass,
)


def test_dynamic_environment_is_strict_prior_and_same_day_frozen():
    data = pd.DataFrame([
        {"game_pk": 1, "game_date": pd.Timestamp("2025-04-01"), "final_total": 10},
        {"game_pk": 2, "game_date": pd.Timestamp("2025-04-01"), "final_total": 2},
        {"game_pk": 3, "game_date": pd.Timestamp("2025-04-02"), "final_total": 20},
    ])
    result = dynamic_environment(data).set_index("game_pk")
    assert result.loc[1, "season_to_date_league_rpg"] == result.loc[2, "season_to_date_league_rpg"]
    assert result.loc[1, "run_environment_history_depth"] == 0
    assert result.loc[3, "season_to_date_league_rpg"] == 6
    assert result.loc[3, "run_environment_history_depth"] == 2


def test_sparse_starter_shrinkage_changes_only_one_or_two_start_rows():
    data = pd.DataFrame({"home_starter_ra9": [9.0, 9.0, 9.0], "away_starter_ra9": [3.0, 3.0, 3.0],
        "home_starter_prior_starts": [1, 2, 3], "away_starter_prior_starts": [1, 2, 3],
        "home_team_starter_level": [4.0, 4.0, 4.0], "away_team_starter_level": [4.0, 4.0, 4.0], "league_starter_level": [4.5, 4.5, 4.5]})
    result = add_sparse_shrinkage(data, .5)
    assert result.loc[0, "home_starter_ra9_shrunk"] < 9
    assert result.loc[1, "home_starter_ra9_shrunk"] < 9
    assert result.loc[2, "home_starter_ra9_shrunk"] == 9
    assert result.loc[2, "away_starter_ra9_shrunk"] == 3


def test_park_transformation_is_bounded_and_depth_shrunk():
    data = pd.DataFrame({"strict_prior_total_run_factor": [.5, 1.5, 1.1], "park_history_depth": [100, 100, 0], "trailing_30_league_rpg": [9, 9, 9]})
    result = add_park_features(data)
    assert np.isclose(result.loc[0, "park_shrunk_deviation"], -.2 * 100 / 150)
    assert np.isclose(result.loc[1, "park_shrunk_deviation"], .2 * 100 / 150)
    assert result.loc[2, "park_shrunk_deviation"] == 0


def test_negative_binomial_distribution_is_normalized_with_v1_alpha():
    mass = nb_mass([5.0, 9.0, 15.0], V1_ALPHA)
    assert mass.shape == (3, 31)
    assert np.allclose(mass.sum(axis=1), 1)
    assert np.all(mass >= 0)
