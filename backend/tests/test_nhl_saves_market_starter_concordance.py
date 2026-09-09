import unittest

import pandas as pd

from backend.nhl.scripts.assess_nhl_season_2025_saves_market_listed_goalie_actual_starter_concordance import (
    lead_bucket, policy_table,
)


class SavesMarketStarterConcordanceTests(unittest.TestCase):
    def test_policy_mismatch_counts_are_nonnegative_and_exact(self):
        latest=pd.DataFrame([
            {"canonical_game_id":1,"team":"BOS","actual_outcome_available":True,"listed_goalie_ids":"10","actual_starter_player_id":10,"listed_goalie_count":1,"uniquely_identified_actual_starter":True,"consensus_goalie_player_id":10,"consensus_matches_actual":True},
            {"canonical_game_id":2,"team":"NYR","actual_outcome_available":True,"listed_goalie_ids":"20","actual_starter_player_id":21,"listed_goalie_count":1,"uniquely_identified_actual_starter":False,"consensus_goalie_player_id":20,"consensus_matches_actual":False},
            {"canonical_game_id":3,"team":"TOR","actual_outcome_available":True,"listed_goalie_ids":"30;31","actual_starter_player_id":30,"listed_goalie_count":2,"uniquely_identified_actual_starter":False,"consensus_goalie_player_id":30,"consensus_matches_actual":True},
        ])
        result=policy_table(latest,4).set_index("policy")
        self.assertEqual(result.loc["A_EVERY_ACTIVE_MARKET_LISTED_GOALIE","mismatches"],2)
        self.assertEqual(result.loc["B_EXACTLY_ONE_LISTED_GOALIE_PER_TEAM","mismatches"],1)
        self.assertEqual(result.loc["C_MULTIPLE_BOOKS_UNIQUE_GOALIE_AGREEMENT","mismatches"],1)
        self.assertTrue((result.mismatches.dropna()>=0).all())

    def test_lead_time_buckets_include_final_boundary(self):
        self.assertEqual(lead_bucket(180),"121_TO_180_MIN")
        self.assertEqual(lead_bucket(181),"181_TO_360_MIN")


if __name__=="__main__":
    unittest.main()
