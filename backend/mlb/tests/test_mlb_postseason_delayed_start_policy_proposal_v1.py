from __future__ import annotations

import hashlib
import json
import unittest
from pathlib import Path

from backend.mlb.season_transition.runtime_schedule_status_policy_proposal_v1 import (
    ProposedScheduleStatusError,
    validate_proposed_new_postseason_status,
)


ROOT = Path(__file__).resolve().parents[3]
DELAYED_SOURCE = ROOT / (
    "artifacts/ops/mlb_current_slate_sources/2026-10-16/"
    "local_daily_20261016T233004Z/moneyline_schedule.json"
)
DELAYED_SOURCE_SHA256 = "50e2a6d70ec32797a9cfacbda05fd549a290d49c6d753e905751cdfb7800554c"


class DelayedStartPolicyProposalTests(unittest.TestCase):
    @staticmethod
    def game(status: dict | None = None) -> dict:
        return {
            "gamePk": 849817,
            "gameType": "L",
            "season": "2026",
            "seriesDescription": "AL Championship Series",
            "gameDate": "2026-10-17T00:00:00Z",
            "status": status or {
                "abstractGameState": "Preview",
                "abstractGameCode": "P",
                "codedGameState": "P",
                "statusCode": "PI",
                "detailedState": "Delayed Start",
                "reason": "Inclement Weather",
            },
        }

    def test_source_verified_849817_delayed_start_tuple_is_accepted_by_proposal(self) -> None:
        raw = DELAYED_SOURCE.read_bytes()
        self.assertEqual(hashlib.sha256(raw).hexdigest(), DELAYED_SOURCE_SHA256)
        payload = json.loads(raw)
        matches = [
            game for day in payload["dates"] for game in day["games"]
            if int(game.get("gamePk") or 0) == 849817
        ]
        self.assertEqual(len(matches), 1)
        validate_proposed_new_postseason_status(matches[0], expected_game_pk=849817)

    def test_delayed_status_with_conflicting_exact_identity_is_rejected(self) -> None:
        wrong_pk = self.game()
        wrong_pk["gamePk"] = 849816
        with self.assertRaisesRegex(ProposedScheduleStatusError, "GAME_PK_CONFLICT"):
            validate_proposed_new_postseason_status(wrong_pk, expected_game_pk=849817)

        wrong_type = self.game()
        wrong_type["gameType"] = "R"
        wrong_type["seriesDescription"] = "Regular Season"
        with self.assertRaises(ProposedScheduleStatusError):
            validate_proposed_new_postseason_status(wrong_type, expected_game_pk=849817)

        nested_conflict = self.game()
        nested_conflict["gameData"] = {"game": {"pk": 849816, "type": "L", "season": "2026"}}
        with self.assertRaisesRegex(ProposedScheduleStatusError, "GAME_PK_CONFLICT"):
            validate_proposed_new_postseason_status(nested_conflict, expected_game_pk=849817)

    def test_unknown_status_is_rejected(self) -> None:
        game = self.game({
            "abstractGameState": "Preview", "abstractGameCode": "P",
            "codedGameState": "X", "statusCode": "XX", "detailedState": "Unknown",
        })
        with self.assertRaisesRegex(ProposedScheduleStatusError, "STATUS_CONFLICT"):
            validate_proposed_new_postseason_status(game, expected_game_pk=849817)

    def test_terminal_postponement_remains_rejected(self) -> None:
        game = self.game({
            "abstractGameState": "Final", "abstractGameCode": "F",
            "codedGameState": "D", "statusCode": "DR", "detailedState": "Postponed",
        })
        with self.assertRaisesRegex(ProposedScheduleStatusError, "STATUS_CONFLICT"):
            validate_proposed_new_postseason_status(game, expected_game_pk=849817)

    def test_conflicting_delayed_abstract_code_is_rejected(self) -> None:
        game = self.game()
        game["status"]["abstractGameCode"] = "L"
        with self.assertRaisesRegex(ProposedScheduleStatusError, "STATUS_CONFLICT"):
            validate_proposed_new_postseason_status(game, expected_game_pk=849817)


if __name__ == "__main__":
    unittest.main()
