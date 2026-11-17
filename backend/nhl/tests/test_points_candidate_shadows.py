"""Points candidate shadows (P2.58): PP unit and morning game-line inputs."""
import json
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

from backend.nhl import points_candidate_shadows as sh


def logs(rows):
    return pd.DataFrame(rows, columns=["game_id", "player_id", "game_date", "team_id", "pp_toi_minutes"])


class PpUnitTests(unittest.TestCase):
    def test_top_five_with_pp_time_are_pp1_and_ties_break_by_player_id(self):
        rows = [(1, p, "2026-11-01", 10, m) for p, m in ((7, 3.0), (6, 3.0), (5, 2.0), (4, 1.0), (3, 1.0), (2, 0.5), (1, 0.0))]
        t = sh.pp_unit_table(logs(rows)).set_index("player_id")
        self.assertEqual(t.loc[6, "pp_rank"], 1.0)        # tie at 3.0: lower player_id first
        self.assertEqual(t.loc[7, "pp_rank"], 2.0)
        self.assertEqual(t.loc[2, "pp1"], 0.0)            # sixth with PP time is not PP1
        self.assertEqual((t.loc[1, "pp1"], t.loc[1, "pp_rank"]), (0.0, 10.0))   # no PP time

    def test_inputs_are_strictly_prior_and_use_the_previous_season_tail(self):
        lg = logs([(2, 9, "2026-11-02", 10, 2.0), (3, 9, "2026-11-04", 10, 0.0)])
        prior = pd.DataFrame({"game_id": [1], "player_id": [9], "game_date": ["2026-04-10"], "pp1": [1.0], "pp_rank": [3.0]})
        targets = pd.DataFrame({"game_id": [3, 4], "player_id": [9, 9], "game_date": ["2026-11-04", "2026-11-06"]})
        out = sh.pp_unit_inputs(targets, lg, prior).set_index("game_id")
        self.assertEqual(out.loc[3, "pp1_last_game"], 1.0)                 # game 2 (2.0 min, rank 1)
        self.assertAlmostEqual(out.loc[3, "pp1_share_last5"], 1.0)         # prior-season game + game 2
        self.assertEqual((out.loc[4, "pp1_last_game"], out.loc[4, "pp_rank_last_game"]), (0.0, 10.0))
        self.assertAlmostEqual(out.loc[4, "pp1_share_last5"], 2 / 3)


class GameLineTests(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp(dir=Path(__file__).resolve().parents[3] / "tmp"))
        self.day = self.root / "season=2026" / "slate_date=2026-11-16"

    def observation(self, stamp, phase, rows):
        d = self.day / f"observation={stamp}Z_x"
        d.mkdir(parents=True)
        (d / "observation_summary.json").write_text(json.dumps({"phase": phase}))
        (d / "normalized_odds.jsonl").write_text("\n".join(json.dumps(r) for r in rows) + "\n")
        return d / "normalized_odds.jsonl"

    def rows(self):
        base = {"home_team": "Utah Mammoth", "away_team": "Vancouver Canucks", "bookmaker": "b1",
                "commence_time": "2026-11-17T01:30:00Z"}
        return [dict(base, market="h2h", outcome="Utah Mammoth", point=None, price=-150),
                dict(base, market="h2h", outcome="Vancouver Canucks", point=None, price=130),
                dict(base, market="totals", outcome="Over", point=6.5, price=-110),
                dict(base, market="totals", outcome="Under", point=6.5, price=-110)]

    def test_latest_full_slate_observation_by_0630_pt_and_never_closing(self):
        early = self.observation("20261116T134813", "EARLY", self.rows())
        self.observation("20261116T140000", "CLOSING", self.rows())        # 06:00 PT but single-game
        self.observation("20261116T163310", "EARLY", self.rows())          # after 06:30 PT
        self.assertEqual(sh.morning_observation("2026-11-16", odds_root=self.root), early)

    def test_first_full_slate_observation_when_none_by_0630(self):
        later = self.observation("20261116T153837", "EARLY", self.rows())
        self.observation("20261116T193310", "EARLY", self.rows())
        self.assertEqual(sh.morning_observation("2026-11-16", odds_root=self.root), later)
        self.assertIsNone(sh.morning_observation("2026-11-16", now=pd.Timestamp("2026-11-16T15:00Z"), odds_root=self.root))

    def test_implied_team_goals_split_the_total_by_win_probability(self):
        obs = self.observation("20261116T134813", "EARLY", self.rows())
        slate = pd.DataFrame({"game_id": [7], "home_team_id": [68], "away_team_id": [23]})
        g = sh.game_line_inputs(slate, obs).set_index("team_id")
        p_home = (150 / 250) / (150 / 250 + 100 / 230)
        self.assertAlmostEqual(g.loc[68, "team_win_p"], p_home)
        self.assertAlmostEqual(g.loc[68, "game_total"], 6.5)
        self.assertAlmostEqual(g.loc[68, "team_implied_goals"], 6.5 * (0.5 + 0.6 * (p_home - 0.5)))
        self.assertAlmostEqual(g.loc[68, "team_implied_goals"] + g.loc[23, "team_implied_goals"], 6.5)


class ArmTests(unittest.TestCase):
    def test_frozen_arms_match_their_contracts(self):
        names = [a.name for a in sh.arms()]
        self.assertEqual(names, ["arm=TOTALS_PPU_2S", "arm=TOTALS_PPU_3S"])
        for a in sh.arms():
            contract, model = sh.load_arm(a)
            self.assertEqual(contract["feature_columns"][-6:], sh.PPU + sh.TOT)
            self.assertEqual(contract["calibration_layer"], "NONE")
            self.assertEqual(contract["live_parity"]["pp_rank_last_game_equal_share"], 1.0)


if __name__ == "__main__":
    unittest.main()
