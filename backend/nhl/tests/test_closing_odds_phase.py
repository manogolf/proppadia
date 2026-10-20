"""CLOSING is a per-game odds phase with its own claim per game."""
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from backend.nhl.daily_capture import CanonicalGame, ProviderCapture, capture_odds_observation
from backend.tests.test_nhl_comprehensive_daily_capture_v1 import FakeProvider, exchange

UTC = timezone.utc
SLATE = "2026-10-21"


def game(game_id, home, away, start="2026-10-21T23:00:00Z"):
    return CanonicalGame(game_id, start, home[0], away[0], home, away)


def provider_for(home_name, away_name, event_id, start="2026-10-21T23:00:00Z"):
    payload = [{"id": event_id, "home_team": home_name, "away_team": away_name, "commence_time": start,
                "bookmakers": [{"key": "draftkings", "title": "DraftKings", "markets": [{
                    "key": "player_shots_on_goal", "last_update": "2026-10-21T22:30:00Z",
                    "outcomes": [{"name": "Over", "description": "Sample Player", "price": -110, "point": 2.5}]}]}]}]
    event = {"id": event_id, "home_team": home_name, "away_team": away_name, "commence_time": start}
    return FakeProvider(ProviderCapture(
        None, [event], payload, [exchange(body=json.dumps(payload).encode())],
        (json.dumps(payload) + "\n").encode(), credits_consumed=21, credits_remaining=90000))


class ClosingPhaseTests(unittest.TestCase):
    NOW = datetime(2026, 10, 21, 22, 40, tzinfo=UTC)  # 20 minutes before puck

    def capture(self, root, g, provider, **kwargs):
        return capture_odds_observation(
            root=root, season=2026, slate_date=SLATE, phase="CLOSING",
            parent_daily_run_id="closing-scheduler", canonical_games=[g], provider=provider,
            authorized=True, invocation_id=f"closing-{g.game_id}", now=self.NOW, **kwargs)

    def test_closing_requires_a_game_scope(self):
        njd = game(2026020200, ("NJD", "New Jersey Devils"), ("NYR", "New York Rangers"))
        with tempfile.TemporaryDirectory() as temp, self.assertRaisesRegex(
                ValueError, "CLOSING_PHASE_REQUIRES_GAME_CLAIM_SCOPE"):
            self.capture(Path(temp), njd, provider_for("New Jersey Devils", "New York Rangers", "e1"))

    def test_each_game_on_a_slate_gets_its_own_closing_capture(self):
        njd = game(2026020200, ("NJD", "New Jersey Devils"), ("NYR", "New York Rangers"))
        bos = game(2026020201, ("BOS", "Boston Bruins"), ("TOR", "Toronto Maple Leafs"))
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            first = self.capture(root, njd, provider_for("New Jersey Devils", "New York Rangers", "e1"),
                                 claim_scope="game=2026020200")
            second = self.capture(root, bos, provider_for("Boston Bruins", "Toronto Maple Leafs", "e2"),
                                  claim_scope="game=2026020201")
            for result, game_id in ((first, 2026020200), (second, 2026020201)):
                self.assertEqual(result.classification, "CAPTURED_NONEMPTY")
                summary = json.loads((result.observation_dir / "observation_summary.json").read_text())
                self.assertEqual(summary["phase"], "CLOSING")
                self.assertIs(summary["strictly_prestart"], True)
                binding = [json.loads(line) for line in
                           (result.observation_dir / "game_binding.jsonl").read_text().splitlines()]
                self.assertEqual([b["canonical_game_id"] for b in binding if b["binding_status"] == "MATCHED"], [game_id])
            claims = sorted(p.name for p in (root / ".claims/season=2026/slate_date=2026-10-21").iterdir())
            self.assertIn("phase=CLOSING.game=2026020200.claim.json", claims)
            self.assertIn("phase=CLOSING.game=2026020201.claim.json", claims)
            self.assertFalse(any(p.is_dir() for p in (root / ".claims/season=2026/slate_date=2026-10-21").iterdir()))

    def test_existing_phases_are_unchanged_without_a_scope(self):
        njd = game(2026020200, ("NJD", "New Jersey Devils"), ("NYR", "New York Rangers"))
        with tempfile.TemporaryDirectory() as temp:
            result = capture_odds_observation(
                root=Path(temp), season=2026, slate_date=SLATE, phase="EARLY",
                parent_daily_run_id="daily", canonical_games=[njd],
                provider=provider_for("New Jersey Devils", "New York Rangers", "e1"),
                authorized=True, invocation_id="early", now=self.NOW)
            self.assertEqual(result.classification, "CAPTURED_NONEMPTY")
            self.assertTrue((Path(temp) / ".claims/season=2026/slate_date=2026-10-21/phase=EARLY.claim.json").is_file())


if __name__ == "__main__":
    unittest.main()


class ClosingReaderGuardTests(unittest.TestCase):
    """Readers of a day's latest or pooled snapshots ignore CLOSING snapshots."""

    def _obs(self, day, name, phase):
        obs = day / f"observation={name}"
        obs.mkdir(parents=True)
        (obs / "observation_summary.json").write_text(json.dumps({"phase": phase}))
        return obs

    def test_helper_reads_the_recorded_phase(self):
        from backend.nhl.daily_capture import is_closing_observation
        with tempfile.TemporaryDirectory() as temp:
            day = Path(temp)
            self.assertTrue(is_closing_observation(self._obs(day, "b", "CLOSING")))
            self.assertFalse(is_closing_observation(self._obs(day, "a", "EARLY")))
            self.assertFalse(is_closing_observation(day / "missing"))

    def test_8rain_latest_observation_skips_a_later_closing_snapshot(self):
        from unittest.mock import patch
        from backend.nhl.scripts import run_nhl_8rain_export as export
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            day = root / "season=2026/slate_date=2026-10-21"
            early = self._obs(day, "20261021T190000.000000Z_aaaa", "EARLY")
            self._obs(day, "20261021T224000.000000Z_bbbb", "CLOSING")
            with patch.object(export, "OBSERVATION_ROOT", root):
                self.assertEqual(export._latest_observation("2026-10-21"), early.resolve())
