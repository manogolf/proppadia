from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

from backend.nhl.daily_orchestration import LANE_NAMES
from backend.nhl.performance_summary import render_markdown, summarize_frames
import backend.nhl.sog_shrunk_rate_shadow as shadow

SCORER = Path(__file__).parents[1] / "scripts/score_sog_poisson_baseline.py"


def log(player, game, date, season, sog, toi, game_type="02"):
    return {"player_id": player, "game_id": game, "game_date": date, "season": season,
            "game_type": game_type, "shots_on_goal": sog, "toi_minutes": toi}


def fixture():
    rows = [
        # prior season (2025) defines the league prior: F = 60/600*60 = 6.0, D = 20/600*60 = 2.0
        log(4, 2025020001, "2026-01-01", 2025, 60, 600.0),
        log(2, 2025020002, "2026-01-01", 2025, 20, 600.0),
        # current season for player 1: two clean games plus noise that must be ignored
        log(1, 2026020001, "2026-10-10", 2026, 4, 20.0),
        log(1, 2026020002, "2026-10-12", 2026, 2, 16.0),
        log(1, 2026010001, "2026-10-01", 2026, 9, 20.0, game_type="01"),  # preseason
        log(1, 2026020003, "2026-10-13", 2026, 5, None),                   # no TOI
        log(1, 2026020004, "2026-10-14", 2026, None, 18.0),                # no SOG
    ]
    history = pd.DataFrame(rows)
    positions = pd.DataFrame({"player_id": [1, 2, 3, 4], "position": ["C", "D", "L", "R"]})
    production = pd.DataFrame({"game_id": [2026020010, 2026020010], "player_id": [1, 3],
                               "expected_sog": [1.5, 0.9]})
    return history, positions, production


def build(history=None, positions=None, production=None, **extra):
    h, p, prod = fixture()
    kwargs = dict(slate_date="2026-10-15", season=2026, run_id="r", feature_cutoff_utc="t",
                  canonical_game_ids=[2026020010], started_excluded_game_ids=[])
    kwargs.update(extra)
    return shadow.build_predictions(history if history is not None else h, positions if positions is not None else p,
                                    production if production is not None else prod, **kwargs)


class ShrunkRateShadowTest(unittest.TestCase):
    def test_recipe_uses_clean_regular_season_window_and_position_prior(self):
        frame, prior = build()
        self.assertEqual(prior, {"D": 2.0, "F": 6.0})
        row = frame[frame.player_id.eq(1)].iloc[0]
        self.assertEqual(row.clean_games_20, 2)
        self.assertEqual(row.clean_sog_20, 6)
        self.assertEqual(row.clean_toi_20, 36)
        rate = (6 + 6.0 * 180 / 60) / (36 + 180) * 60
        self.assertAlmostEqual(row.shrunk_rate_per60, rate)
        self.assertAlmostEqual(row.shadow_lambda, rate * 18.0 / 60)
        self.assertEqual(row.shadow_source, "SHRUNK_RATE")

    def test_rate_window_is_last_twenty_games_and_toi_last_ten(self):
        rows = [log(4, 2025020001, "2026-01-01", 2025, 60, 600.0), log(2, 2025020002, "2026-01-01", 2025, 20, 600.0)]
        for i in range(25):
            rows.append(log(1, 2026020100 + i, f"2026-10-{i + 1:02d}", 2026, 10 if i < 5 else 1, 10.0 if i < 15 else 20.0))
        frame, _ = build(history=pd.DataFrame(rows), slate_date="2026-10-30")
        row = frame[frame.player_id.eq(1)].iloc[0]
        self.assertEqual(row.clean_sog_20, 20)
        self.assertEqual(row.clean_toi_20, 10 * 10 + 10 * 20)
        self.assertEqual(row.clean_toi_avg_10, 20.0)

    def test_player_without_history_keeps_production_lambda(self):
        frame, _ = build()
        row = frame[frame.player_id.eq(3)].iloc[0]
        self.assertEqual(row.shadow_source, "NO_CLEAN_HISTORY_PRODUCTION_FALLBACK")
        self.assertEqual(row.shadow_lambda, 0.9)
        self.assertTrue(np.isnan(row.shrunk_rate_per60))

    def test_three_lines_and_coherent_probabilities(self):
        frame, _ = build()
        self.assertEqual(len(frame), 6)
        p = frame[frame.player_id.eq(1)].set_index("line").shadow_p_over
        self.assertGreater(p[1.5], p[2.5]); self.assertGreater(p[2.5], p[3.5])
        lam = frame[frame.player_id.eq(1)].shadow_lambda.iloc[0]
        self.assertAlmostEqual(p[1.5], 1 - np.exp(-lam) * (1 + lam))

    def test_rejects_non_prior_history_and_noncanonical_games(self):
        h, _, _ = fixture()
        late = pd.concat([h, pd.DataFrame([log(1, 2026020009, "2026-10-15", 2026, 3, 15.0)])])
        with self.assertRaisesRegex(ValueError, "NOT_STRICTLY_PRIOR"):
            build(history=late)
        with self.assertRaisesRegex(ValueError, "NONCANONICAL_GAME"):
            build(canonical_game_ids=[1])

    def test_missing_prior_season_fails_closed(self):
        h, _, _ = fixture()
        with self.assertRaisesRegex(ValueError, "LEAGUE_PRIOR_UNAVAILABLE"):
            build(history=h[h.season.eq(2026)])

    def test_capture_grade_and_summary(self):
        h, p, prod = fixture()
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            prod_path = root / "production.csv"
            prod.to_csv(prod_path, index=False)
            manifest = shadow.capture(history=h, positions=p, production_path=prod_path, output_root=root / "captures",
                                      run_id="r", slate_date="2026-10-15", season=2026, feature_cutoff_utc="t",
                                      canonical_game_ids=[2026020010], started_excluded_game_ids=[], scorer_path=SCORER)
            self.assertEqual(manifest["deterministic_replay"], "PASS")
            self.assertEqual(manifest["shrunk_player_game_count"], 1)
            self.assertEqual(manifest["production_fallback_player_game_count"], 1)
            self.assertEqual(manifest["history_sha256"], shadow.history_sha256(h, "2026-10-15"))
            with self.assertRaises(FileExistsError):
                shadow.capture(history=h, positions=p, production_path=prod_path, output_root=root / "captures",
                               run_id="r", slate_date="2026-10-15", season=2026, feature_cutoff_utc="t",
                               canonical_game_ids=[2026020010], started_excluded_game_ids=[], scorer_path=SCORER)
            recon = root / "recon"
            recon.mkdir()
            pd.DataFrame({"slate_date": ["2026-10-15"] * 2, "game_id": [2026020010] * 2, "player_id": [1, 3],
                          "participation_state": ["PARTICIPATED"] * 2, "official_final": [True, True],
                          "shots_on_goal": [3, 0]}).to_csv(recon / "canonical_skater_outcomes.csv", index=False)
            graded = shadow.grade_slate(slate_date="2026-10-15", reconciliation_package=recon,
                                        capture_root=root / "captures")
            self.assertEqual(graded["status"], "COMPLETE")
            grade = graded["grades"][0]
            self.assertEqual(grade["settled_player_games"], 2)
            self.assertEqual(grade["production_fallback_player_games"], 1)
            line = grade["by_line"]["1.5"]
            self.assertEqual(line["wins"] + line["losses"], 2)
            self.assertEqual(line["production_wins"] + line["production_losses"], 2)
            summary = summarize_frames(slate_date="2026-10-15", games=1, phase="REGULAR_SEASON",
                                       reconciliation_status="CREATED", package_identity="p", grades={},
                                       source_artifacts={}, sog_shrunk_rate_shadow_context=graded)
            self.assertEqual(summary["sog_shrunk_rate_k180_shadow"]["status"], "COMPLETE")
            text = render_markdown(summary)
            self.assertIn("## SOG Shrunk-Rate (K=180) Prospective Shadow", text)
            self.assertIn("Paired count log loss vs production", text)
            self.assertIn("Latest pregame capture per player-game", text)
            latest = graded["latest_pregame"]
            self.assertEqual(latest["settled_player_games"], 2)
            self.assertEqual(latest["overall"]["wins"] + latest["overall"]["losses"], 6)

    def test_latest_pregame_uses_latest_capture_per_player_game(self):
        h, p, prod = fixture()
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            first, second = root / "first.csv", root / "second.csv"
            prod.to_csv(first, index=False)
            prod[prod.player_id.eq(1)].assign(expected_sog=4.0).to_csv(second, index=False)
            for run_id, path in (("run_a", first), ("run_b", second)):
                shadow.capture(history=h, positions=p, production_path=path, output_root=root / "captures",
                               run_id=run_id, slate_date="2026-10-15", season=2026, feature_cutoff_utc="t",
                               canonical_game_ids=[2026020010], started_excluded_game_ids=[], scorer_path=SCORER)
            recon = root / "recon"; recon.mkdir()
            pd.DataFrame({"slate_date": ["2026-10-15"] * 2, "game_id": [2026020010] * 2, "player_id": [1, 3],
                          "shots_on_goal": [3, 0]}).to_csv(recon / "canonical_skater_outcomes.csv", index=False)
            graded = shadow.grade_slate(slate_date="2026-10-15", reconciliation_package=recon, capture_root=root / "captures")
            self.assertEqual(len(graded["grades"]), 2)
            latest = graded["latest_pregame"]
            self.assertEqual(latest["settled_player_games"], 2)
            self.assertEqual(latest["capture_runs"], ["run_a", "run_b"])
            self.assertEqual(latest["production_overall"]["wins"] + latest["production_overall"]["losses"], 6)

    def test_grade_without_capture_reports_none(self):
        with tempfile.TemporaryDirectory() as td:
            recon = Path(td) / "recon"; recon.mkdir()
            pd.DataFrame({"slate_date": ["2026-10-15"], "game_id": [1], "player_id": [1],
                          "shots_on_goal": [1]}).to_csv(recon / "canonical_skater_outcomes.csv", index=False)
            graded = shadow.grade_slate(slate_date="2026-10-15", reconciliation_package=recon, capture_root=Path(td) / "none")
            self.assertEqual(graded["status"], "NO_IMMUTABLE_SHADOW_CAPTURE")

    def test_daily_lane_is_registered(self):
        self.assertIn("sog_shrunk_rate_k180_shadow", LANE_NAMES)


if __name__ == "__main__":
    unittest.main()
