from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd

from backend.nhl.daily_capture import canonical_game_set_hash
from backend.nhl.daily_orchestration import DailyRunRecorder
from backend.nhl import cli
from backend.nhl.performance_summary import _validate_production_sog_game_set


class DailyStartupOrderingTests(unittest.TestCase):
    def test_bootstrap_records_canonical_set_from_schedule_before_lane_checks(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            slate_dir = root / "artifacts/operational/nhl/slates/2026-10-11"
            slate_dir.mkdir(parents=True)
            health = {"slate_date": "2026-10-11", "downstream_ready": True,
                      "completion_status": "READY", "source": "NHL_API_SCHEDULE"}
            (slate_dir / "slate_health.json").write_text(json.dumps(health))
            games = [SimpleNamespace(game_id=2026020084), SimpleNamespace(game_id=2026020085)]
            recorder = DailyRunRecorder(run_id="test", command=[], phase="EARLY")
            with patch.object(cli, "ROOT", root), patch.object(cli, "run") as run, patch.object(
                cli, "load_canonical_slate", return_value=games
            ) as load:
                _, frozen, game_hash = cli._bootstrap_daily_canonical_slate(
                    slate="2026-10-11", recorder=recorder)
            run.assert_called_once()
            load.assert_called_once()
            self.assertEqual([g.game_id for g in frozen], [2026020084, 2026020085])
            self.assertEqual(recorder.canonical_game_ids, [2026020084, 2026020085])
            self.assertEqual(game_hash, canonical_game_set_hash([2026020084, 2026020085]))

    def test_missing_health_is_uninitialized_not_a_valid_empty_slate(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            recorder = DailyRunRecorder(run_id="test", command=[], phase="EARLY")
            with patch.object(cli, "ROOT", root), patch.object(cli, "run"):
                with self.assertRaisesRegex(RuntimeError, "NHL slate health missing"):
                    cli._bootstrap_daily_canonical_slate(slate="2026-10-11", recorder=recorder)
            self.assertIsNone(recorder.canonical_game_set_hash)

    def test_schedule_bootstrap_precedes_prior_day_learning(self):
        events = []
        recorder = DailyRunRecorder(run_id="test", command=[], phase="EARLY")
        with tempfile.TemporaryDirectory() as temp:
            with patch.object(cli, "ROOT", Path(temp)), patch.object(
                cli, "require_db_url", return_value="db"
            ), patch.object(cli, "run", side_effect=lambda *a, **k: events.append("db")), patch.object(
                cli, "et_today", return_value="2026-10-11"
            ), patch.object(cli, "et_yesterday", return_value="2026-10-10"), patch.object(
                cli, "prior_et_slate", return_value="2026-10-10"
            ), patch.object(cli, "_bootstrap_daily_canonical_slate",
                            side_effect=lambda **kw: (events.append("schedule"), {}, [], "empty-hash")[1:]), patch.object(
                cli, "ensure_prior_learning", side_effect=lambda _: events.append("prior-learning") or
                (_ for _ in ()).throw(RuntimeError("stop-after-order-check"))
            ):
                with patch.dict("os.environ", {}, clear=False):
                    with self.assertRaisesRegex(RuntimeError, "stop-after-order-check"):
                        cli._cmd_daily_impl(with_odds=False, morning_only=False,
                                            odds_phase="EARLY", daily_run_id="test",
                                            recorder=recorder)
        self.assertLess(events.index("schedule"), events.index("prior-learning"))

    def test_valid_initialized_empty_schedule_is_explicit(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            slate_dir = root / "artifacts/operational/nhl/slates/2026-10-11"
            slate_dir.mkdir(parents=True)
            (slate_dir / "slate_health.json").write_text(json.dumps({
                "slate_date": "2026-10-11", "downstream_ready": True,
                "completion_status": "VALID_EMPTY_SLATE",
            }))
            recorder = DailyRunRecorder(run_id="test", command=[], phase="EARLY")
            with patch.object(cli, "ROOT", root), patch.object(cli, "run") as run:
                health, games, game_hash = cli._bootstrap_daily_canonical_slate(
                    slate="2026-10-11", recorder=recorder)
            self.assertEqual(health["completion_status"], "VALID_EMPTY_SLATE")
            self.assertEqual(games, [])
            self.assertEqual(game_hash, canonical_game_set_hash([]))
            self.assertEqual(recorder.canonical_game_ids, [])
            self.assertEqual(recorder.canonical_game_set_hash, canonical_game_set_hash([]))
            run.assert_called_once()

    def test_production_sog_accepts_full_canonical_lineage_with_eligible_subset(self):
        canonical = [10, 11, 12, 13]
        receipt = {"canonical_game_ids": canonical,
                   "canonical_game_set_hash": canonical_game_set_hash(canonical),
                   "eligible_pregame_game_ids": [12, 13]}
        output = {"canonical_game_set_hash": receipt["canonical_game_set_hash"]}
        _validate_production_sog_game_set(receipt, output,
                                          pd.DataFrame({"game_id": [12, 13]}))

    def test_production_sog_rejects_a_real_canonical_mismatch(self):
        canonical = [10, 11, 12, 13]
        receipt = {"canonical_game_ids": canonical,
                   "canonical_game_set_hash": canonical_game_set_hash(canonical),
                   "eligible_pregame_game_ids": [12, 13]}
        output = {"canonical_game_set_hash": receipt["canonical_game_set_hash"]}
        with self.assertRaisesRegex(ValueError, "PRODUCTION_SOG_CANONICAL_GAME_SET_MISMATCH"):
            _validate_production_sog_game_set(receipt, output,
                                              pd.DataFrame({"game_id": [11, 13]}))

    def test_production_sog_allows_initialized_empty_full_slate(self):
        empty_hash = canonical_game_set_hash([])
        _validate_production_sog_game_set(
            {"canonical_game_ids": [], "canonical_game_set_hash": empty_hash,
             "eligible_pregame_game_ids": []},
            {"canonical_game_set_hash": empty_hash}, pd.DataFrame({"game_id": []}))


if __name__ == "__main__":
    unittest.main()
