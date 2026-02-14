import unittest
from unittest.mock import patch

from backend.app.services.nhl.prop_submission_service import add_prop
from backend.domains.mlb.repository.prop_repository import DuplicatePropError


class TestNhlPropSubmissionService(unittest.TestCase):
    @patch("backend.app.services.nhl.prop_submission_service.insert_prop_row")
    @patch("backend.app.services.nhl.prop_submission_service.find_duplicate_prop_id", return_value=None)
    def test_add_prop_happy_path(self, _mock_dup, mock_insert):
        out = add_prop(
            {
                "player_id": 8478402,
                "player_name": "Connor McDavid",
                "team": "EDM",
                "team_id": 22,
                "game_id": 2025020011,
                "game_date": "2026-02-14",
                "prop_type": "sog",
                "prop_value": 3.5,
                "over_under": "over",
                "probability": 0.64,
                "prop_source": "user_added",
                "user_id": "user-1",
            }
        )
        self.assertTrue(out["ok"])
        self.assertTrue(out["saved"])
        self.assertFalse(out["duplicate"])
        self.assertEqual(mock_insert.call_args.kwargs["prop_type"], "shots_on_goal")
        self.assertEqual(mock_insert.call_args.kwargs["prop_source"], "nhl_user_added")
        self.assertEqual(mock_insert.call_args.kwargs["user_id"], "user-1")

    @patch("backend.app.services.nhl.prop_submission_service.find_duplicate_prop_id", return_value="dup-11")
    def test_add_prop_duplicate_short_circuit(self, _mock_dup):
        out = add_prop(
            {
                "player_id": 8478402,
                "game_id": 2025020011,
                "prop_type": "goalie_saves",
                "prop_value": 22.5,
            }
        )
        self.assertTrue(out["ok"])
        self.assertFalse(out["saved"])
        self.assertTrue(out["duplicate"])
        self.assertEqual(out["id"], "dup-11")

    @patch(
        "backend.app.services.nhl.prop_submission_service.insert_prop_row",
        side_effect=DuplicatePropError("duplicate key value violates unique constraint"),
    )
    @patch("backend.app.services.nhl.prop_submission_service.find_duplicate_prop_id", side_effect=[None, "dup-22"])
    def test_add_prop_handles_unique_violation(self, _mock_dup, _mock_insert):
        out = add_prop(
            {
                "player_id": 8478402,
                "game_id": 2025020011,
                "prop_type": "sog",
                "prop_value": 3.5,
            }
        )
        self.assertTrue(out["ok"])
        self.assertFalse(out["saved"])
        self.assertTrue(out["duplicate"])
        self.assertEqual(out["id"], "dup-22")

    def test_add_prop_rejects_invalid_over_under(self):
        with self.assertRaises(ValueError) as ctx:
            add_prop(
                {
                    "player_id": 8478402,
                    "game_id": 2025020011,
                    "prop_type": "sog",
                    "prop_value": 3.5,
                    "over_under": "sideways",
                }
            )
        self.assertIn("over_under must be over or under", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
