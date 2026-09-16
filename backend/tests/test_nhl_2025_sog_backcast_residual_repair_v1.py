from __future__ import annotations

import json
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

from backend.nhl.market_archive_index.core import bind_player, declared_player_aliases, prepare_player_candidates
from backend.nhl.scripts.evaluate_nhl_2025_v2_market_and_sog_cross_market_v1 import score_prepared_features


ROOT = Path(__file__).resolve().parents[2]
PACKAGE = ROOT / "artifacts/analysis/model_development/nhl_2025_sog_backcast_residual_repair_v1/2026-09-15"


class TestNhl2025SogBackcastResidualRepairV1(unittest.TestCase):
    def test_declared_chinakhov_alias_is_exact_and_game_scoped(self) -> None:
        roster = pd.DataFrame([{"game_id": 1, "player_id": 8482475, "full_name": "E. Chinakhov", "team_code": "PIT", "position": "F"}])
        candidates = prepare_player_candidates(roster, pd.DataFrame(columns=["game_id", "player_id", "player_name"]))
        result = bind_player("Yegor Chinakhov", 1, candidates)
        self.assertEqual(declared_player_aliases()["yegor chinakhov"], 8482475)
        self.assertEqual(result["canonical_player_id"], 8482475)
        self.assertEqual(result["binding_method"], "DECLARED_EXACT_PROVIDER_NAME_VARIANT_WITHIN_BOUND_GAME")
        self.assertEqual(bind_player("Yegor Chinakhov", 2, candidates)["final_disposition"], "REJECTED")

    def test_no_history_uses_only_frozen_zero_default(self) -> None:
        frame = pd.DataFrame([{column: np.nan for column in [
            "d5_sog_per60", "d10_sog_per60", "d20_sog_per60", "d5_toi_min_avg", "d10_toi_min_avg", "d20_toi_min_avg",
            "szn_toi_per_game_5on5", "szn_toi_per_game_pp", "season_5on5_icetime_per_game", "season_5on4_icetime_per_game",
        ]}])
        scored = score_prepared_features(frame).iloc[0]
        self.assertEqual(scored.expected_sog, 0)
        self.assertEqual(scored.selected_rate_source, "MISSING")
        self.assertEqual(scored.selected_toi_source, "MISSING")
        self.assertEqual(scored.missingness_fallback_state, "MISSING_RATE_ZERO_DEFAULT")

    def test_residual_ledger_is_complete_unique_and_strict_prior(self) -> None:
        ledger = pd.read_parquet(PACKAGE / "residual_feature_repair_ledger.parquet")
        self.assertEqual(len(ledger), 1014)
        self.assertFalse(ledger.duplicated(["game_id", "identity_key"]).any())
        self.assertTrue(ledger.resolved_player_id.notna().all())
        self.assertTrue(ledger.expected_sog.notna().all())
        self.assertTrue(ledger.strict_prior_current_game_rows_used.eq(0).all())
        latest = pd.to_datetime(ledger.latest_prior_game_date, errors="coerce")
        target = pd.to_datetime(ledger.game_date)
        self.assertTrue((latest[latest.notna()] < target[latest.notna()]).all())

    def test_population_parity_and_explicit_team_limitations(self) -> None:
        decision = json.loads((PACKAGE / "decision.json").read_text())
        backcast = pd.read_parquet(PACKAGE / "repaired_sog_model_backcast.parquet")
        unresolved = pd.read_csv(PACKAGE / "unresolved_after_repair.csv")
        self.assertEqual(len(backcast), 19840)
        self.assertTrue(backcast.backcast_status.eq("GENERATED").all())
        self.assertEqual(backcast.game_id.nunique(), 1312)
        self.assertEqual(len(unresolved), decision["NHL_SOG_UNRESOLVED_ROWS_AFTER_REPAIR"])
        self.assertEqual(len(unresolved), 74)
        self.assertEqual(decision["NHL_SOG_ORIGINAL_PARITY"], "PRESERVED")

    def test_original_rows_are_substantively_identical(self) -> None:
        parity = json.loads((PACKAGE / "original_row_parity.json").read_text())
        self.assertTrue(parity["preserved"])
        self.assertEqual(parity["original_rows_compared"], 18826)
        self.assertEqual(parity["before_digest"], parity["after_digest"])

    def test_frozen_sensitivity_conclusion_is_unchanged(self) -> None:
        decision = json.loads((PACKAGE / "decision.json").read_text())
        self.assertEqual(decision["after_conditional_decisions"], {
            "MARKET_SOG_BEYOND_MONEYLINE": "REDUNDANT",
            "MODEL_MARKET_SOG_DISAGREEMENT": "NO_INFORMATION",
            "MODEL_SOG_BEYOND_V2": "FRAGILE",
        })
        self.assertEqual(decision["NHL_SOG_REPAIR_SENSITIVITY"], "CONCLUSION_UNCHANGED")
        self.assertEqual(decision["odds_api_calls"], 0)
        self.assertEqual(decision["odds_api_credits_consumed"], 0)


if __name__ == "__main__":
    unittest.main()
