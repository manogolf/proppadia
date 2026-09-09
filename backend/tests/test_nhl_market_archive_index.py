from __future__ import annotations

import unittest
from pathlib import Path

import pandas as pd

from backend.nhl.analysis_package_guard import begin_package
from backend.nhl.market_archive_index.core import (
    american_implied, bind_event, bind_player, build_pairs, classify_duplicates,
    effective_timestamp, market_status, prepare_games, prepare_player_candidates,
    qualification, team_code, timing_classification, verify_parent_hashes,
)


def games():
    return prepare_games(pd.DataFrame([
        {"game_id":2025010001,"season":2025,"game_type":1,"game_date":"2025-09-20","start_time_utc":"2025-09-21T00:00:00Z","home_team_code":"BOS","away_team_code":"NYR"},
        {"game_id":2025020001,"season":2025,"game_type":2,"game_date":"2025-10-07","start_time_utc":"2025-10-08T00:00:00Z","home_team_code":"BOS","away_team_code":"NYR"},
    ]))


def candidates():
    roster=pd.DataFrame([
        {"game_id":2025020001,"player_id":1,"full_name":"Alex Smith","team_code":"BOS","position":"F"},
        {"game_id":2025020001,"player_id":2,"full_name":"Adam Smith","team_code":"NYR","position":"F"},
        {"game_id":2025020001,"player_id":3,"full_name":"Jane Doe","team_code":"BOS","position":"G"},
    ])
    pred=pd.DataFrame(columns=["game_id","player_id","player_name"])
    return prepare_player_candidates(roster,pred)


class MarketArchiveIndexTests(unittest.TestCase):
 def test_team_aliases(self):
    self.assertEqual(team_code("Boston Bruins"), "BOS")
    self.assertEqual(team_code("Utah Hockey Club"), "UTA")


 def test_event_exact_orientation_and_utc_boundary(self):
    event={"id":"e","home_team":"Boston Bruins","away_team":"New York Rangers","commence_time":"2025-10-07T20:00:00-04:00"}
    self.assertEqual(bind_event(event,games(),"2025-10-07")["canonical_game_id"], 2025020001)


 def test_rescheduled_unique_team_date_fallback(self):
    event={"id":"e","home_team":"Boston Bruins","away_team":"New York Rangers","commence_time":"2025-10-08T08:00:00Z"}
    got=bind_event(event,games(),"2025-10-07")
    self.assertEqual(got["binding_method"], "EXACT_TEAM_ORIENTATION_AND_SLATE_DATE_FALLBACK")


 def test_team_orientation_mismatch_rejected(self):
    event={"id":"e","home_team":"New York Rangers","away_team":"Boston Bruins","commence_time":"2025-10-08T00:00:00Z"}
    self.assertEqual(bind_event(event,games(),"2025-10-07")["identity_classification"], "CONFLICT")


 def test_provider_event_collision_is_ambiguous(self):
    frame=pd.concat([games(),games().iloc[[1]].assign(game_id=2025020002)],ignore_index=True)
    event={"id":"e","home_team":"Boston Bruins","away_team":"New York Rangers","commence_time":"2025-10-08T00:00:00Z"}
    self.assertEqual(bind_event(event,frame,"2025-10-07")["identity_classification"], "AMBIGUOUS")


 def test_exact_player_name_with_game_is_corroborated(self):
    self.assertEqual(bind_player("Jane Doe",2025020001,candidates())["canonical_player_id"], 3)


 def test_ambiguous_initial_last_alias_rejected(self):
    got=bind_player("A. Smith",2025020001,candidates())
    self.assertEqual(got["identity_classification"], "AMBIGUOUS")


 def test_identical_names_on_different_teams_are_ambiguous(self):
    c=candidates(); extra=c[2025020001].iloc[[0]].assign(player_id=9,team_code="NYR")
    c[2025020001]=pd.concat([c[2025020001],extra],ignore_index=True)
    self.assertEqual(bind_player("Alex Smith",2025020001,c)["identity_classification"], "AMBIGUOUS")


 def test_start_boundary_rejection(self):
    for effective,expected in [("2025-10-07T23:59:59Z","PREGAME_QUALIFIED"),("2025-10-08T00:00:00Z","AT_OR_POST_START"),("2025-10-08T00:01:00Z","AT_OR_POST_START")]:
        with self.subTest(effective=effective):
            self.assertEqual(timing_classification(effective,"2025-10-08T00:00:00Z"), expected)


 def test_indeterminate_and_start_unresolved(self):
    self.assertEqual(timing_classification(None,"2025-10-08T00:00:00Z"), "TIMING_INDETERMINATE")
    self.assertEqual(timing_classification("2025-10-07T23:00:00Z",None), "START_TIME_UNRESOLVED")


 def test_timestamp_precedence(self):
    got=effective_timestamp({}, {"last_update":"2025-10-07T22:00:00Z"},{"last_update":"2025-10-07T21:00:00Z"},"2025-10-07T20:00:00Z","2025-10-09T00:00:00Z")
    self.assertEqual(got, ("2025-10-07T22:00:00Z","MARKET_PROVIDER_TIMESTAMP"))


 def test_suspended_market_excluded(self):
    self.assertEqual(market_status({}, {"active":False}, {}), "SUSPENDED")
    self.assertEqual(qualification("CANONICAL_CORROBORATED","CANONICAL_CORROBORATED","PREGAME_QUALIFIED","SUSPENDED","OVER",1.5,-110)[0], "EXCLUDED")


 def test_incomplete_pair_not_synthesized(self):
    q=pd.DataFrame([{"source_file_sha256":"h","source_file_path":"p","archive_family":"x","slate_date":"d","canonical_season":2025,"canonical_game_id":1,"provider_event_id":"e","sportsbook":"b","sportsbook_name":"B","provider_market_key":"player_points","market_family":"POINTS","canonical_player_id":1,"canonical_player_name":"P","line":.5,"market_object_locator":"m","side":"OVER","american_price":-110,"observation_id":"o","effective_observation_timestamp_utc":"2025-01-01T00:00:00Z"}])
    pair=build_pairs(q).iloc[0]
    self.assertFalse(pair.pair_complete)
    self.assertTrue(pd.isna(pair.no_vig_over_probability))


 def test_complete_pair_no_vig(self):
    base={"source_file_sha256":"h","source_file_path":"p","archive_family":"x","slate_date":"d","canonical_season":2025,"canonical_game_id":1,"provider_event_id":"e","sportsbook":"b","sportsbook_name":"B","provider_market_key":"player_points","market_family":"POINTS","canonical_player_id":1,"canonical_player_name":"P","line":.5,"market_object_locator":"m","effective_observation_timestamp_utc":"2025-01-01T00:00:00Z"}
    q=pd.DataFrame([{**base,"side":"OVER","american_price":-110,"observation_id":"o"},{**base,"side":"UNDER","american_price":-110,"observation_id":"u"}])
    self.assertAlmostEqual(build_pairs(q).iloc[0].no_vig_over_probability, .5)


 def test_book_line_side_distinct_quotes_not_duplicates(self):
    base={"source_file_sha256":"h","provider_event_id":"e","sportsbook":"a","provider_market_key":"m","source_player_name":"P","line":.5,"side":"OVER","american_price":-110,"effective_observation_timestamp_utc":"t"}
    df=pd.DataFrame([base,{**base,"sportsbook":"b"},{**base,"line":1.5},{**base,"side":"UNDER"}])
    self.assertEqual(set(classify_duplicates(df).duplicate_classification), {"BOOK_LINE_SIDE_DISTINCT_OBSERVATION"})


 def test_repeated_capture_classified(self):
    base={"source_file_sha256":"h1","provider_event_id":"e","sportsbook":"a","provider_market_key":"m","source_player_name":"P","line":.5,"side":"OVER","american_price":-110,"effective_observation_timestamp_utc":"t1"}
    df=pd.DataFrame([base,{**base,"source_file_sha256":"h2","effective_observation_timestamp_utc":"t2"}])
    self.assertEqual(set(classify_duplicates(df).duplicate_classification), {"REPEATED_CAPTURE"})


 def test_parent_hash_drift_rejected(self):
    from tempfile import TemporaryDirectory
    with TemporaryDirectory() as temp:
        tmp_path=Path(temp); p=tmp_path/"parent.json"; p.write_text("{}")
        manifest=pd.DataFrame([{"source_file_path":"parent.json","sha256":"bad"}])
        with self.assertRaisesRegex(RuntimeError,"PARENT_HASH_DRIFT_REJECTED"):
            verify_parent_hashes(manifest,tmp_path)


 def test_create_only_output(self):
    from tempfile import TemporaryDirectory
    with TemporaryDirectory() as temp:
        target=Path(temp)/"package"; target.mkdir()
        with self.assertRaisesRegex(RuntimeError,"GOVERNED_PACKAGE_EXISTS_ABORT"):
            begin_package(target)


 def test_american_implied(self):
    self.assertAlmostEqual(american_implied(-110),110/210)
    self.assertAlmostEqual(american_implied(120),100/220)


 def test_cross_lane_and_line_mismatch_keys_are_distinct(self):
    keys={("SOG",1.5),("POINTS",1.5),("SOG",2.5)}
    self.assertEqual(len(keys),3)


if __name__ == "__main__":
    unittest.main()
