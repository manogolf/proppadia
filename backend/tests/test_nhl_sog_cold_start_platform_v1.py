import tempfile
import unittest
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from backend.nhl.sog_cold_start.core import build_predictions, grade_predictions
from backend.nhl.sog_cold_start.cli import main as cli_main
from backend.nhl.scripts.preflight_nhl_sog_prediction_only_v1 import run_preflight
from backend.nhl.scripts import run_nhl_sog_prediction_only_warn_only as observer


def features(slate="2026-09-20"):
    common={"canonical_season":2026,"slate_date":slate,"game_id":2026010001,"team_id":1,"opponent_id":2,"position":"C","roster_status":"ACTIVE_ROSTER","lineup_status":"ACTIVE_ROSTER_UNCONFIRMED","scheduled_start_time_utc":f"{slate}T23:00:00Z","feature_cutoff_utc":f"{slate}T18:00:00Z","position_sog_per60":7.0,"position_toi_per_game":16.0,"team_changed":False,"current_preseason_games":0,"current_preseason_sog_per60":None,"current_preseason_toi_per_game":None,"current_regular_games":0,"current_regular_sog_per60":None,"current_regular_toi_per_game":None}
    return pd.DataFrame([{**common,"player_id":1,"player_name":"Veteran","prior_minutes":1000,"prior_sog_per60":8.0,"prior_toi_per_game":18.0,"older_minutes":800,"older_sog_per60":7.0,"older_toi_per_game":17.0},{**common,"player_id":2,"player_name":"Rookie","prior_minutes":0,"prior_sog_per60":None,"prior_toi_per_game":None,"older_minutes":0,"older_sog_per60":None,"older_toi_per_game":None},{**common,"player_id":3,"player_name":"Unknown","position":"UNK","prior_minutes":0,"prior_sog_per60":None,"prior_toi_per_game":None,"older_minutes":0,"older_sog_per60":None,"older_toi_per_game":None}])


class ColdStartTest(unittest.TestCase):
    def test_grading_marks_exact_line_tie_as_push(self):
        prediction = pd.DataFrame([{
            "canonical_season": 2026, "slate_date": "2026-09-20",
            "game_id": 2026010001, "player_id": 1, "line": 2.0,
            "selected_side": "OVER",
        }])
        outcome = pd.DataFrame([{
            "canonical_season": 2026, "slate_date": "2026-09-20",
            "game_id": 2026010001, "player_id": 1, "official_final": True,
            "official_sog": 2, "participation_status": "APPEARED",
            "outcome_source": "official_fixture",
            "outcome_source_timestamp_utc": "2026-09-21T02:00:00Z",
        }])
        graded = grade_predictions(
            prediction, outcome, grading_timestamp_utc="2026-09-21T03:00:00Z")
        self.assertEqual(graded.grading_state.iloc[0], "PUSH")

    def test_no_network_market_independence_preflight(self):
        with patch("socket.socket", side_effect=AssertionError("NETWORK_FORBIDDEN")), \
             patch.dict(os.environ, {"ODDS_API_KEY": "fixture-secret"}):
            result=run_preflight("2026-09-20",datetime(2026,9,20,18,0,tzinfo=timezone.utc))
        self.assertEqual(result["network_requests"],0)
        self.assertFalse(result["credential_access"])
        self.assertFalse(result["morning_readiness_blocks_prediction"])
        self.assertEqual(result["selected_arm"],"D_PLAYER_ROLE_HIERARCHICAL")
        self.assertEqual(result["historical_e_arm_status"],"HISTORICAL_DIAGNOSTIC_SHADOW_NOT_EMITTED_BY_OPERATIONAL_CONTRACT")

    def test_prediction_observer_does_not_consult_morning_gate(self):
        with tempfile.TemporaryDirectory(prefix="sog_observer_gate_") as raw:
            root=Path(raw)
            schedule,fixture=__import__("backend.nhl.scripts.preflight_nhl_sog_prediction_only_v1",fromlist=["fixture"]).fixture("2026-09-20")
            with patch.object(observer,"export_features",return_value=(schedule,fixture)), \
                 patch("socket.socket",side_effect=AssertionError("NETWORK_FORBIDDEN")), \
                 patch.dict(os.environ,{"PROPPADIA_INVOCATION_ORIGIN":"VALIDATION_TEST","PROPPADIA_VALIDATION_ID":"sog_gate_fixture"}):
                status=observer.observe(slate="2026-09-20",requested="AUTO",dsn="fixture",root=root,now=datetime(2026,9,20,18,0,tzinfo=timezone.utc))
            payload=json.loads(status.read_text())
            self.assertEqual(payload["status"],"CAPTURED")
            self.assertEqual(payload["morning_readiness_dependency"],"NONE_PREDICTION_ONLY")
            self.assertEqual(payload["invocation_classification"],"VALIDATION_TEST")
            self.assertEqual(payload["market_requests"],0)
    def test_prediction_only_no_zero_fill_and_three_line_ladder(self):
        p,i,e=build_predictions(features(),slate_date="2026-09-20",phase="MIDDAY",prediction_timestamp_utc="2026-09-20T18:01:00Z",input_cutoff_utc="2026-09-20T18:01:00Z")
        self.assertEqual(i.player_id.nunique(),2);self.assertEqual(len(p),len(i)*3);self.assertEqual(len(e),1)
        self.assertTrue((i.expected_sog>0).all());self.assertEqual(set(p.line),{1.5,2.5,3.5});self.assertTrue(p.price.isna().all());self.assertIn("D_PLAYER_ROLE_HIERARCHICAL",set(i.contract_arm))
        dist=p[[f"p_sog_{k}" for k in range(7)]+["p_sog_7_plus"]].sum(axis=1);self.assertTrue((dist-1).abs().lt(1e-12).all())
    def test_september_19_never_retroactively_predicted(self):
        with self.assertRaisesRegex(RuntimeError,"SEPTEMBER_19_RETROSPECTIVE_PREDICTION_FORBIDDEN"):
            build_predictions(features("2026-09-19"),slate_date="2026-09-19",phase="MIDDAY",prediction_timestamp_utc="2026-09-19T18:01:00Z",input_cutoff_utc="2026-09-19T18:01:00Z")
    def test_strict_pregame_timing(self):
        with self.assertRaisesRegex(RuntimeError,"PREGAME_TIMING_GATE_FAILED"):
            build_predictions(features(),slate_date="2026-09-20",phase="MIDDAY",prediction_timestamp_utc="2026-09-21T00:00:00Z",input_cutoff_utc="2026-09-20T18:01:00Z")
    def test_current_blend_is_gradual(self):
        f=features().iloc[[0]].copy();f["current_regular_games"]=5;f["current_regular_sog_per60"]=10.;f["current_regular_toi_per_game"]=20.
        _,i,_=build_predictions(f,slate_date="2026-09-20",phase="FINAL_PREGAME",prediction_timestamp_utc="2026-09-20T21:00:00Z",input_cutoff_utc="2026-09-20T21:00:00Z")
        g=i[i.contract_arm.eq("G_COLD_START_TO_CURRENT_SEASON_BLEND")]
        self.assertEqual(g.transition_state.iloc[0],"CURRENT_SEASON_BLEND_0.500_CHALLENGER")
    def test_grading_distinguishes_appearance(self):
        p,_,_=build_predictions(features(),slate_date="2026-09-20",phase="MIDDAY",prediction_timestamp_utc="2026-09-20T18:01:00Z",input_cutoff_utc="2026-09-20T18:01:00Z")
        o=pd.DataFrame([{"canonical_season":2026,"slate_date":"2026-09-20","game_id":2026010001,"player_id":1,"official_final":True,"official_sog":3,"participation_status":"APPEARED","outcome_source":"fixture","outcome_source_timestamp_utc":"2026-09-21T02:00:00Z"},{"canonical_season":2026,"slate_date":"2026-09-20","game_id":2026010001,"player_id":2,"official_final":True,"official_sog":None,"participation_status":"LATE_SCRATCH","outcome_source":"fixture","outcome_source_timestamp_utc":"2026-09-21T02:00:00Z"}])
        g=grade_predictions(p,o,grading_timestamp_utc="2026-09-21T03:00:00Z");self.assertTrue(g.loc[g.player_id.eq(2),"grading_state"].eq("LATE_SCRATCH_UNGRADED").all())

if __name__=="__main__":unittest.main()
