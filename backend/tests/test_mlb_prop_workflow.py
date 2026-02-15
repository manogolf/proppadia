import unittest
from unittest.mock import patch

from backend.app.services.mlb.commit_tokens import sign_commit_payload
from backend.app.services.mlb.prop_submission_service import prepare_prop_submission
from backend.domains.mlb.prop_workflow import add_prop_from_commit
from backend.domains.mlb.repository.prop_repository import DuplicatePropError


class TestMlbPropWorkflow(unittest.TestCase):
    @patch("backend.domains.mlb.prop_workflow.build_game_context", side_effect=RuntimeError("network down"))
    @patch("backend.domains.mlb.prop_workflow.resolve_player_candidate", return_value=None)
    def test_prepare_fallback_context_when_game_context_unavailable(self, _mock_resolve, _mock_ctx):
        payload = {
            "player_id": 660271,
            "team_id": 144,
            "game_date": "2026-02-10",
            "prop_type": "hits",
            "prop_value": 1.5,
            "over_under": "over",
        }
        out = prepare_prop_submission(payload)
        self.assertTrue(out["ok"])
        self.assertIn("warnings", out)
        self.assertIn("game context unavailable", out["warnings"][0])
        self.assertEqual(out["features"]["team"], "ATL")
        self.assertIsNone(out["features"]["game_id"])

    def test_add_prop_requires_game_id(self):
        token = sign_commit_payload(
            {
                "flow": "mlb_prop_v1",
                "prop_type": "hits",
                "probability": 0.6,
                "recommendation": "over",
                "features": {
                    "player_id": 660271,
                    "team": "ATL",
                    "team_id": 144,
                    "game_date": "2026-02-10",
                    "prop_value": 1.5,
                    "over_under": "over",
                },
            }
        )
        with self.assertRaises(ValueError) as ctx:
            add_prop_from_commit(commit_token=token, prop_source="user_added")
        self.assertIn("game_id missing", str(ctx.exception))

    def test_add_prop_requires_game_date(self):
        token = sign_commit_payload(
            {
                "flow": "mlb_prop_v1",
                "prop_type": "hits",
                "probability": 0.6,
                "recommendation": "over",
                "features": {
                    "player_id": 660271,
                    "team": "ATL",
                    "team_id": 144,
                    "game_id": 12345,
                    "prop_value": 1.5,
                    "over_under": "over",
                },
            }
        )
        with self.assertRaises(ValueError) as ctx:
            add_prop_from_commit(commit_token=token, prop_source="user_added")
        self.assertIn("game_date missing", str(ctx.exception))

    def test_add_prop_rejects_bad_game_date(self):
        token = sign_commit_payload(
            {
                "flow": "mlb_prop_v1",
                "prop_type": "hits",
                "probability": 0.6,
                "recommendation": "over",
                "features": {
                    "player_id": 660271,
                    "team": "ATL",
                    "team_id": 144,
                    "game_id": 12345,
                    "game_date": "08/15/2025",
                    "prop_value": 1.5,
                    "over_under": "over",
                },
            }
        )
        with self.assertRaises(ValueError) as ctx:
            add_prop_from_commit(commit_token=token, prop_source="user_added")
        self.assertIn("game_date must be YYYY-MM-DD", str(ctx.exception))

    def test_add_prop_rejects_game_date_context_mismatch(self):
        token = sign_commit_payload(
            {
                "flow": "mlb_prop_v1",
                "prop_type": "hits",
                "probability": 0.6,
                "recommendation": "over",
                "features": {
                    "player_id": 660271,
                    "team": "ATL",
                    "team_id": 144,
                    "game_id": 12345,
                    "game_date": "2025-08-15",
                    "for_date": "2025-08-14",
                    "prop_value": 1.5,
                    "over_under": "over",
                },
            }
        )
        with self.assertRaises(ValueError) as ctx:
            add_prop_from_commit(commit_token=token, prop_source="user_added")
        self.assertIn("game_date mismatch", str(ctx.exception))

    @patch(
        "backend.domains.mlb.prop_workflow.build_game_context",
        return_value={"team_id": 119, "team_abbr": "LAD", "for_date": "2026-02-10", "game_id": 12345},
    )
    @patch(
        "backend.domains.mlb.prop_workflow.resolve_player_candidate",
        return_value={"player_id": 660271, "player_name": "Shohei Ohtani", "team_id": 119, "team_abbr": "LAD"},
    )
    def test_prepare_prefers_resolved_player_team_on_mismatch(self, _mock_resolve, _mock_ctx):
        payload = {
            "player_id": 660271,
            "team_id": 144,
            "team_abbr": "ATL",
            "game_date": "2026-02-10",
            "prop_type": "hits",
            "prop_value": 1.5,
            "over_under": "over",
        }
        out = prepare_prop_submission(payload)
        self.assertTrue(out["ok"])
        self.assertEqual(out["features"]["team_id"], 119)
        self.assertEqual(out["features"]["team"], "LAD")
        self.assertIn("warnings", out)
        warning_text = " ".join(out["warnings"])
        self.assertIn("mismatched resolved player team", warning_text)

    @patch(
        "backend.domains.mlb.prop_workflow.insert_prop_row",
        side_effect=DuplicatePropError("duplicate key value violates unique constraint"),
    )
    @patch("backend.domains.mlb.prop_workflow.find_duplicate_prop_id", side_effect=["", "dup-123"])
    def test_add_prop_handles_db_unique_violation_as_duplicate(self, _dup, _insert):
        token = sign_commit_payload(
            {
                "flow": "mlb_prop_v1",
                "prop_type": "hits",
                "probability": 0.6,
                "recommendation": "over",
                "features": {
                    "player_id": 660271,
                    "player_name": "Shohei Ohtani",
                    "team": "LAD",
                    "team_id": 119,
                    "game_date": "2026-02-10",
                    "game_id": 12345,
                    "prop_value": 1.5,
                    "over_under": "over",
                },
            }
        )
        out = add_prop_from_commit(commit_token=token, prop_source="user_added")
        self.assertTrue(out["ok"])
        self.assertFalse(out["saved"])
        self.assertTrue(out["duplicate"])
        self.assertEqual(out["id"], "dup-123")

    @patch("backend.domains.mlb.prop_workflow.insert_prop_row")
    @patch("backend.domains.mlb.prop_workflow.find_duplicate_prop_id", return_value=None)
    def test_add_prop_passes_user_id_to_insert(self, _dup, mock_insert):
        token = sign_commit_payload(
            {
                "flow": "mlb_prop_v1",
                "prop_type": "hits",
                "probability": 0.6,
                "recommendation": "over",
                "features": {
                    "player_id": 660271,
                    "player_name": "Shohei Ohtani",
                    "team": "LAD",
                    "team_id": 119,
                    "game_date": "2026-02-10",
                    "game_id": 12345,
                    "prop_value": 1.5,
                    "over_under": "over",
                },
            }
        )
        out = add_prop_from_commit(
            commit_token=token,
            prop_source="user_added",
            user_id="user-123",
        )
        self.assertTrue(out["ok"])
        self.assertTrue(out["saved"])
        self.assertFalse(out["duplicate"])
        self.assertEqual(mock_insert.call_args.kwargs["user_id"], "user-123")


if __name__ == "__main__":
    unittest.main()
