import unittest
from unittest.mock import patch

from backend.nhl.scripts import prepare_nhl_2025_attempts_repair as repair


def row(game_id, player_id, attempts, status, official="", pbp="a" * 64, box="b" * 64):
    return {
        "game_id": str(game_id), "player_id": str(player_id), "team_id": "1", "opponent_id": "2",
        "game_date": "2025-10-07", "shots_on_goal": "1", "shot_attempts": attempts,
        "toi_minutes": "10.00", "pp_toi_minutes": "", "blocks": "0", "created_at": "2025-12-14T00:00:00Z",
        "candidate_status": status, "official_attempts": official, "pbp_sha256": pbp, "boxscore_sha256": box,
    }


class PrepareNhl2025AttemptsRepairTests(unittest.TestCase):
    def setUp(self):
        self.targets = [
            row(2025020001, 11, "2", "VERIFIED_INCORRECT", "3"),
            row(2025020001, 12, "", "RECOVERABLE_NULL", "0"),
        ]
        self.unresolved = [row(2025020002, 13, "", "UNRESOLVED")]
        self.correct = [row(2025020002, 14, "0", "VERIFIED_CORRECT", "0")]
        self.candidate = self.targets + self.unresolved + self.correct
        self.snapshot = [
            {"game_id": r["game_id"], "player_id": r["player_id"], "team_id": r["team_id"],
             "opponent_id": r["opponent_id"], "game_date": r["game_date"], "shots_on_goal": r["shots_on_goal"],
             "shot_attempts": r["shot_attempts"], "toi_minutes": r["toi_minutes"],
             "pp_toi_minutes": r["pp_toi_minutes"], "blocks": r["blocks"], "created_at": r["created_at"]}
            for r in self.candidate
        ]

    def test_validates_scope_and_current_values(self):
        with patch.multiple(repair, EXPECTED_TARGETS=2, EXPECTED_UNRESOLVED=1, EXPECTED_VERIFIED_CORRECT=1):
            targets, unresolved = repair.validate(self.candidate, self.snapshot)
        self.assertEqual(len(targets), 2)
        self.assertEqual(len(unresolved), 1)

    def test_rejects_changed_target_or_unresolved_value(self):
        with patch.multiple(repair, EXPECTED_TARGETS=2, EXPECTED_UNRESOLVED=1, EXPECTED_VERIFIED_CORRECT=1):
            changed = [dict(x) for x in self.snapshot]
            changed[0]["shot_attempts"] = "4"
            with self.assertRaisesRegex(ValueError, "CURRENT_ATTEMPTS_CHANGED"):
                repair.validate(self.candidate, changed)
            changed = [dict(x) for x in self.snapshot]
            changed[2]["shot_attempts"] = "0"
            with self.assertRaisesRegex(ValueError, "CURRENT_ATTEMPTS_CHANGED"):
                repair.validate(self.candidate, changed)

    def test_generated_sql_is_prefix_scoped_idempotent_and_evidence_emitting(self):
        with patch.multiple(repair, EXPECTED_TARGETS=2, EXPECTED_UNRESOLVED=1, EXPECTED_VERIFIED_CORRECT=1):
            targets, unresolved = repair.validate(self.candidate, self.snapshot)
            forward, rollback = repair.sql_scripts(targets, unresolved, "c" * 64)
        self.assertIn("game_id >= 2025020000 AND game_id < 2025030000", forward)
        self.assertIn("TARGET_SET_NOT_ALL_ORIGINAL_OR_CORRECTED", forward)
        self.assertIn("UNRESOLVED_VALUES_CHANGED", forward)
        self.assertIn("attempt_2025_changed_evidence", forward)
        self.assertIn("NHL_2025_ATTEMPTS_UPDATED_ROWS", forward)
        self.assertIn("ROLLBACK_SET_NOT_ALL_CORRECTED_OR_ORIGINAL", rollback)
        self.assertIn("NHL_2025_ATTEMPTS_ROLLED_BACK_ROWS", rollback)
        self.assertIn("(2025020001, 12, NULL, 0", forward)


if __name__ == "__main__":
    unittest.main()
