from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from backend.nhl.daily_capture import canonical_game_set_hash
from backend.nhl.sog_attachment_integrity import audit_sog_attachment


class SogAttachmentMixedSlateTests(unittest.TestCase):
    def test_partial_prediction_population_binds_full_canonical_set(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            prediction = root / "predictions.csv"
            attachment = root / "attachment.csv"
            unmatched = root / "unmatched.csv"
            odds = root / "odds"
            odds.mkdir()
            pd.DataFrame([{
                "game_date": "2026-10-01", "game_id": 2026020002,
                "player_id": 22, "p_over_1_5": 0.6,
                "p_over_2_5": 0.4, "p_over_3_5": 0.2,
            }]).to_csv(prediction, index=False)
            attachment_rows = [
                {"game_date": "2026-10-01", "game_id": 2026020002,
                 "player_id": 22, "line": line, "p_over_mkt": market}
                for line, market in ((1.5, 0.55), (2.5, None), (3.5, None))
            ]
            pd.DataFrame(attachment_rows).to_csv(attachment, index=False)
            pd.DataFrame([row for row in attachment_rows if row["p_over_mkt"] is None]).to_csv(unmatched, index=False)
            (odds / "observation_summary.json").write_text(json.dumps({
                "observation_timestamp_utc": "2026-10-02T02:00:00Z",
            }))
            starts = {
                2026020001: "2026-10-02T01:00:00Z",
                2026020002: "2026-10-02T03:00:00Z",
            }
            result = audit_sog_attachment(
                prediction_path=prediction, attachment_path=attachment,
                unmatched_path=unmatched, slate_date="2026-10-01",
                parent_daily_run_id="mixed-run", odds_observation_path=odds,
                odds_observation_manifest_sha256="fixture-manifest",
                canonical_game_starts_utc=starts,
            )
            self.assertEqual(result["status"], "PASS")
            self.assertEqual(result["canonical_game_set_hash"], canonical_game_set_hash(starts))
            self.assertEqual(result["game_specific_prestart"]["eligible_game_count"], 1)
            self.assertEqual(result["game_specific_prestart"]["ineligible_game_count"], 0)


if __name__ == "__main__":
    unittest.main()
