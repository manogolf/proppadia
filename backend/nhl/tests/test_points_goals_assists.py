"""Points goals + assists model (P2.39): component inputs, scorer structure, exporter step."""
import unittest
from unittest import mock

import numpy as np
import pandas as pd

from backend.nhl import points_component_features as pcf
from backend.nhl.scripts import freeze_nhl_points_goals_assists as freeze
from backend.nhl.scripts import score_nhl_points_hgb_shadow as scorer


def outcomes():
    rows = []
    for i, (goals, assists) in enumerate(((1, 0), (0, 2), (2, 1))):
        rows.append({"player_id": 7, "game_id": 2026020100 + i, "season": 2026,
                     "game_date": f"2026-10-0{i + 1}", "goals": goals, "assists": assists})
    return pd.DataFrame(rows)


PRIOR = pd.DataFrame([{"player_id": 7, "season": 2025, "games": 80, "points": 60, "goals": 20, "assists": 40},
                      {"player_id": 8, "season": 2025, "games": 82, "points": 20, "goals": 5, "assists": 15}])
LEAGUE = pcf.league_rates(PRIOR)


class ComponentFeatureTests(unittest.TestCase):
    def test_strict_prior_counts_and_shrinkage(self):
        targets = pd.DataFrame([{"player_id": 7, "game_id": 2026020103, "season": 2026, "game_date": "2026-10-03"}])
        f = pcf.add_features(targets, outcomes(), PRIOR, LEAGUE).iloc[0]
        # Before 10-03: games 10-01 and 10-02 (1 goal, 2 assists); the target game is excluded.
        games = 80 + 2
        self.assertEqual(f.own_games, games)
        self.assertAlmostEqual(f.goal_rate, (20 + 1 + pcf.K * LEAGUE.loc[2025, "goals"]) / (games + pcf.K))
        self.assertAlmostEqual(f.assist_rate, (40 + 2 + pcf.K * LEAGUE.loc[2025, "assists"]) / (games + pcf.K))
        self.assertAlmostEqual(f.own_rate_k30, (60 + 3 + pcf.K * LEAGUE.loc[2025, "points"]) / (games + pcf.K))

    def test_missing_league_rate_fails_closed(self):
        targets = pd.DataFrame([{"player_id": 7, "game_id": 1, "season": 2028, "game_date": "2028-10-03"}])
        with self.assertRaisesRegex(ValueError, "LEAGUE_RATE_MISSING"):
            pcf.add_features(targets, outcomes(), PRIOR, LEAGUE)


@unittest.skipUnless((freeze.OUT / freeze.ARTIFACT).is_file(), "local frozen goals + assists model not present")
class FrozenModelTests(unittest.TestCase):
    def test_structure_sums_components_and_ladder_holds(self):
        import joblib
        model = joblib.load(freeze.OUT / freeze.ARTIFACT)
        row = {name: 1.0 for name in model["feature_columns"]}
        row.update({"game_id": 1, "player_id": 1, "own_rate_k30": 1.5, "goal_rate": 0.6, "assist_rate": 0.9, "own_games": 90})
        out = scorer.score(pd.DataFrame([row]), model)
        frame = pd.DataFrame([row])
        x = frame[model["feature_columns"]].astype(float).to_numpy()
        manual = sum(np.maximum(p["model"].predict(p["scaler"].transform(x)), 1e-8) for p in (model["goals"], model["assists"]))
        self.assertAlmostEqual(out.expected_points.iloc[0], float(manual[0]))
        self.assertGreaterEqual(out.prob_over_0_5.iloc[0], out.prob_over_1_5.iloc[0])

    def test_exporter_adds_component_inputs_under_the_frozen_contract(self):
        from backend.nhl.scripts import export_nhl_points_hgb_features as exporter
        result = pd.DataFrame([{"game_id": 2026020200, "player_id": 7, "game_date": "2026-10-10"}])
        slate = pd.DataFrame([{"game_id": 2026020200, "player_id": 7, "canonical_season": 2026}])
        history = outcomes().rename(columns={"season": "canonical_season"})
        with mock.patch.object(scorer, "CONTRACT_PATH", freeze.OUT / "shadow_contract.json"):
            out = exporter.add_component_features(result, history, slate)
        self.assertTrue(set(pcf.FEATURES) <= set(out.columns))
        self.assertEqual(out.own_games.iloc[0], 3 + PRIOR_GAMES_2025(7))


def PRIOR_GAMES_2025(player_id):
    import json
    prior = pd.read_csv(freeze.OUT / json.loads((freeze.OUT / "shadow_contract.json").read_text())["prior_season_totals"])
    row = prior[(prior.player_id == player_id) & (prior.season == 2025)]
    return int(row.games.iloc[0]) if len(row) else 0


if __name__ == "__main__":
    unittest.main()
