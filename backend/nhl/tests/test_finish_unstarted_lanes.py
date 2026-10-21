"""Lanes a daily run never reached are labelled by why the slate had no work."""
import unittest

from backend.nhl.cli import finish_unstarted_lanes
from backend.nhl.daily_orchestration import DailyRunRecorder


class FinishUnstartedLanesTests(unittest.TestCase):
    def _recorder(self, canonical, eligible):
        recorder = DailyRunRecorder(run_id="run", command=["daily"], phase="EARLY")
        recorder.set_canonical(slate_date="2026-10-21", season=2026, game_ids=canonical, game_set_hash="hash")
        recorder.set_pregame_eligibility(eligible_game_ids=eligible,
                                         started_excluded_game_ids=sorted(set(canonical) - set(eligible)),
                                         cutoff_utc="2026-10-21T23:41:00Z")
        return recorder

    def _labels(self, recorder):
        finish_unstarted_lanes(recorder)
        return {(lane.status, lane.reason) for lane in recorder.lanes.values()}

    def test_all_started_slate_is_not_called_empty(self):
        labels = self._labels(self._recorder([2026020152, 2026020153], []))
        self.assertIn(("SKIPPED_NO_PREGAME_GAMES", "ALL_CANONICAL_GAMES_ALREADY_STARTED"), labels)
        self.assertNotIn(("SKIPPED_VALID_EMPTY_SLATE", "NO_CANONICAL_GAMES"), labels)

    def test_off_day_is_still_an_empty_slate(self):
        labels = self._labels(self._recorder([], []))
        self.assertIn(("SKIPPED_VALID_EMPTY_SLATE", "NO_CANONICAL_GAMES"), labels)


if __name__ == "__main__":
    unittest.main()
