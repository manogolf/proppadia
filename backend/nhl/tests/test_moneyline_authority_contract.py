from __future__ import annotations

import unittest
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from backend.nhl.cross_market_shadow.moneyline_authority import load_authority
from backend.nhl.daily_orchestration import DailyRunRecorder
from backend.nhl.cli import _cross_market_envelope_from_daily_observation
from backend.nhl.odds_markets import NHL_COMPREHENSIVE_PROP_MARKETS
from backend.nhl.scripts import run_nhl_mainline_cross_market_capture_warn_only as cross_market


class MoneylineAuthorityContractTests(unittest.TestCase):
    def test_prospective_roles_and_frozen_model_contract(self):
        contract = load_authority()
        self.assertEqual(contract["production_role"],
                         "NHL_MONEYLINE_SHOT_FINISHING_CHALLENGER_V3")
        self.assertEqual(contract["shadow_role"],
                         "NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V2")
        self.assertEqual(contract["prior_champion"], contract["shadow_role"])
        self.assertEqual(contract["prior_challenger"], contract["production_role"])
        self.assertTrue(contract["prospective_only"])
        self.assertTrue(contract["evidence_package"].endswith(
            "moneyline_prospective_performance/2026-10-13/"))
        for field in ("model_parameters_changed", "training_data_changed",
                      "feature_construction_changed", "probability_calculation_changed",
                      "calibration_changed", "market_logic_changed"):
            self.assertFalse(contract[field])

    def test_parent_daily_odds_request_includes_h2h(self):
        self.assertIn("h2h", NHL_COMPREHENSIVE_PROP_MARKETS)
        self.assertEqual(len(NHL_COMPREHENSIVE_PROP_MARKETS), 7)

    def test_parent_daily_receipt_persists_authority_identity(self):
        with tempfile.TemporaryDirectory() as temporary:
            recorder = DailyRunRecorder(
                run_id="authority-receipt-test", command=["daily", "--with-odds"],
                phase="EARLY", started_at=datetime.now(timezone.utc))
            recorder.moneyline_authority = load_authority()
            path = recorder.finalize(Path(temporary))
            receipt = json.loads((path / "parent_receipt.json").read_text())
        self.assertEqual(receipt["moneyline_authority"]["production_role"],
                         "NHL_MONEYLINE_SHOT_FINISHING_CHALLENGER_V3")
        self.assertEqual(receipt["moneyline_authority"]["shadow_role"],
                         "NHL_MONEYLINE_TEAM_SCHEDULE_LOGIT_CONTROL_V2")

    def test_cross_market_reuses_the_parent_observation_prices(self):
        with tempfile.TemporaryDirectory() as temporary:
            observation = Path(temporary)
            (observation / "observation_summary.json").write_text(
                '{"observation_timestamp_utc":"2026-10-14T14:00:00Z",'
                '"credits_consumed":21,"credits_remaining":979}')
            (observation / "normalized_odds.jsonl").write_text(
                '{"provider_event_id":"evt-1","home_team":"Montreal Canadiens",'
                '"away_team":"Ottawa Senators","commence_time":"2026-10-14T23:00:00Z",'
                '"bookmaker":"betonlineag","bookmaker_title":"BetOnline",'
                '"market":"h2h","market_last_update":"2026-10-14T13:59:00Z",'
                '"outcome":"Montreal Canadiens","price":-120,"point":null}\n')
            envelope = _cross_market_envelope_from_daily_observation(observation)
        self.assertEqual(envelope["provider"], "PARENT_DAILY_ODDS_OBSERVATION")
        self.assertEqual(envelope["quota"]["credits_consumed"], 21)
        outcome = envelope["provider_response"][0]["bookmakers"][0]["markets"][0]["outcomes"][0]
        self.assertEqual(outcome["price"], -120)
        self.assertEqual(envelope["provider_response"][0]["bookmakers"][0]["markets"][0]["key"], "h2h")

    def test_cross_market_export_owns_exclusive_input_directory_creation(self):
        schedule = pd.DataFrame([{
            "slate_date": "2026-10-14",
            "scheduled_start_time_utc": "2026-10-14T23:00:00Z",
        }])
        empty_history = pd.DataFrame()
        connection = patch.object(cross_market.psycopg, "connect")
        read_sql = patch.object(cross_market.pd, "read_sql_query", return_value=empty_history)
        with connection, read_sql:
            with tempfile.TemporaryDirectory() as temporary:
                parent_run = Path(temporary)
                owned_input_dir = parent_run / "cross_market_inputs"

                # This is the live failure shape: a caller pre-creates the
                # child directory, then export_inputs enforces create-only.
                owned_input_dir.mkdir()
                with self.assertRaises(FileExistsError):
                    cross_market.export_inputs(
                        "offline-dsn", "2026-10-14", owned_input_dir,
                        canonical_schedule=schedule,
                    )

                owned_input_dir.rmdir()
                schedule_path, history_path, _ = cross_market.export_inputs(
                    "offline-dsn", "2026-10-14", owned_input_dir,
                    canonical_schedule=schedule,
                )
                self.assertEqual(schedule_path.read_text().splitlines()[0],
                                 "slate_date,scheduled_start_time_utc")
                self.assertTrue(history_path.is_file())
                self.assertTrue(owned_input_dir.is_dir())


if __name__ == "__main__":
    unittest.main()
