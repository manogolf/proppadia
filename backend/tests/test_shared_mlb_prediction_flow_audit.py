import json
import unittest
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

from backend.scripts import audit_mlb_prediction_flow as audit


class TestSharedMlbPredictionFlowAudit(unittest.TestCase):
    def test_pass_when_integrity_and_duplicates_are_clean(self):
        side_effects = [
            [{"?column?": 1}],  # has user_id column
            [  # flow summary
                {
                    "total_rows": 100,
                    "user_added_rows": 40,
                    "stat_derived_rows": 60,
                    "resolved_rows": 90,
                    "graded_rows": 70,
                }
            ],
            [  # integrity checks
                {
                    "user_added_missing_game_id": 0,
                    "user_added_invalid_game_date": 0,
                    "user_added_created_game_date_drift": 0,
                    "resolved_rows_with_invalid_outcome": 0,
                }
            ],
            [{"duplicate_groups": 0, "duplicate_extra_rows": 0}],  # player_props dupes
            [{"duplicate_groups": 0, "duplicate_extra_rows": 0}],  # model_training dupes
        ]
        out = StringIO()
        with patch.object(audit, "pg_fetchall", side_effect=side_effects), redirect_stdout(out):
            rc = audit.main(["--window-days", "30", "--json"])
        self.assertEqual(rc, 0)
        payload = json.loads(out.getvalue())
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["failures"], [])

    def test_fail_when_missing_game_ids_and_duplicates_exist(self):
        side_effects = [
            [],  # no user_id column
            [  # flow summary
                {
                    "total_rows": 10,
                    "user_added_rows": 10,
                    "stat_derived_rows": 0,
                    "resolved_rows": 10,
                    "graded_rows": 8,
                }
            ],
            [  # integrity checks
                {
                    "user_added_missing_game_id": 2,
                    "user_added_invalid_game_date": 0,
                    "user_added_created_game_date_drift": 0,
                    "resolved_rows_with_invalid_outcome": 0,
                }
            ],
            [{"duplicate_groups": 1, "duplicate_extra_rows": 1}],  # player_props dupes
            [{"duplicate_groups": 0, "duplicate_extra_rows": 0}],  # model_training dupes
        ]
        out = StringIO()
        with patch.object(audit, "pg_fetchall", side_effect=side_effects), redirect_stdout(out):
            rc = audit.main(["--window-mode", "games", "--games-back", "30", "--json"])
        self.assertEqual(rc, 1)
        payload = json.loads(out.getvalue())
        self.assertFalse(payload["ok"])
        self.assertIn("user_added_missing_game_id", payload["failures"])
        self.assertIn("player_props_duplicate_groups", payload["failures"])


if __name__ == "__main__":
    unittest.main()
