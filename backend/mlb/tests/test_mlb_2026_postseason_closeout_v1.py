"""2026 postseason closeout package: lane classification, finals, credits, write-once."""
from __future__ import annotations

import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from backend.mlb.season_transition import postseason_closeout_v1 as closeout
from backend.mlb.season_transition.regular_season_close_inventory_v1 import REPO_ROOT, CloseInventoryError


def _game(pk, day, detailed, coded, code=None, abstract="Final"):
    return {"gamePk": pk, "officialDate": day, "gameType": "L", "seriesDescription": "AL Championship Series",
            "status": {"abstractGameState": abstract, "detailedState": detailed,
                       "codedGameState": coded, "statusCode": code or coded},
            "teams": {"away": {"score": 3}, "home": {"score": 1}}}


class LaneClassificationTests(unittest.TestCase):
    def test_postponed_date_rows_are_void_or_flagged_and_own_date_rows_must_be_graded(self):
        official = {849817: "2026-10-18", 849815: "2026-10-19"}
        predictions = [(849817, "2026-10-16", "a"), (849817, "2026-10-18", "b"),
                       (849815, "2026-10-19", "c"), (849815, "2026-10-19", "d")]
        summary = closeout.summarize_lane("X", predictions, {"b", "c"}, official)
        self.assertEqual(summary["row_counts"], {"GRADED": 2, "UNGRADED_ELIGIBLE": 1, "VOID_POSTPONED_DATE": 1})
        self.assertEqual(summary["ungraded_eligible_rows"], 1)
        self.assertEqual(summary["game_pks_by_status"]["UNGRADED_ELIGIBLE"], [849815])
        flagged = closeout.summarize_lane("X", predictions[:2], {"a", "b"}, official)
        self.assertEqual(flagged["row_counts"], {"GRADED": 1, "GRADED_ON_POSTPONED_DATE": 1})
        self.assertEqual(flagged["ungraded_eligible_rows"], 0)

    def test_one_playable_final_per_game_with_postponed_appearance_retained(self):
        schedule = {"dates": [
            {"games": [_game(849817, "2026-10-18", "Postponed", "D", "DR")]},
            {"games": [_game(849817, "2026-10-18", "Final", "F")]},
        ]}
        finals = closeout.final_appearances(schedule, [849817])
        self.assertEqual(finals[849817]["official_date"], "2026-10-18")
        self.assertEqual(finals[849817]["other_appearances"], ["2026-10-18:Postponed"])
        with self.assertRaises(CloseInventoryError):
            closeout.final_appearances({"dates": [{"games": [_game(1, "2026-10-01", "Scheduled", "S",
                                                                   abstract="Preview")]}]}, [1])

    def test_credits_sum_requests_and_flag_failures(self):
        rows = [{"game_date": "2026-10-04", "request_count": 1, "http_status": 200,
                 "request_cost_headers": {"x-requests-last": "3"}},
                {"game_date": "2026-10-05", "request_count": 1, "http_status": 429,
                 "request_cost_headers": {"x-requests-last": "0"}}]
        credits = closeout.summarize_credits(rows)
        self.assertEqual((credits["requests"], credits["credits_charged"], credits["non_200_responses"]), (2, 3, 1))


class PublicationTests(unittest.TestCase):
    def setUp(self):
        (REPO_ROOT / "tmp").mkdir(exist_ok=True)
        self.dir = Path(tempfile.mkdtemp(prefix="postseason_closeout_", dir=REPO_ROOT / "tmp"))
        self.addCleanup(shutil.rmtree, self.dir, True)

    def _package(self, decision="POSTSEASON_CLOSEOUT_READY"):
        return {"schema": closeout.SCHEMA, "decision": decision, "blockers": [] if decision.endswith("READY") else ["X"]}

    def test_write_once_self_hashed_and_refuses_blocked_or_unauthorized(self):
        out = self.dir / "postseason_closeout.json"
        with mock.patch.object(closeout, "build_package", return_value=self._package()):
            with self.assertRaises(CloseInventoryError):
                closeout.execute("  ", out)
            body = closeout.execute("operator", out)
            self.assertEqual(closeout.validate_package(out)["package_sha256"], body["package_sha256"])
            with self.assertRaises(CloseInventoryError):
                closeout.execute("operator", out)
        with mock.patch.object(closeout, "build_package", return_value=self._package("POSTSEASON_CLOSEOUT_BLOCKED")):
            with self.assertRaises(CloseInventoryError):
                closeout.execute("operator", self.dir / "other.json")
        tampered = json.loads(out.read_text())
        tampered["blockers"] = ["edited"]
        out.chmod(0o644)
        out.write_text(json.dumps(tampered))
        with self.assertRaises(CloseInventoryError):
            closeout.validate_package(out)


if __name__ == "__main__":
    unittest.main()
